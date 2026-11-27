# Copyright (c) 2026 Workflow Controller Contributors
#
# Licensed under the MIT License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://opensource.org/licenses/MIT
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Headless runner — 无 UI，调用与 Web 相同的 CoreAPI。

输出纯 JSON lines 到 stdout。
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from workflow_controller.api.core_api import CoreAPI
    from workflow_controller.config.settings import AppConfig
    from workflow_controller.core.orchestrator import Orchestrator

log = logging.getLogger(__name__)


def _emit_stdout(message: str) -> None:
    # 入口层只能 import api 面，无法复用 logging_config 的共享实现；
    # 行为与 logging_config.emit_stdout 保持一致（独立 Logger + 不传播）
    output_logger = logging.Logger(f"{__name__}.stdout", level=logging.INFO)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter("%(message)s"))
    output_logger.addHandler(stream_handler)
    try:
        output_logger.info("%s", message)
    finally:
        output_logger.removeHandler(stream_handler)
        stream_handler.close()


class HeadlessRunner:
    """Headless 模式 runner。

    使用与 Web 相同的 boot() → CoreAPI，输出 JSON lines。
    """

    def __init__(
        self,
        config: AppConfig,
        config_path: str = "workflow-controller-config.yaml",
        settings_dir: str | None = None,
        hook_port: int | None = None,
    ) -> None:
        self._config = config
        self._config_path = config_path
        self._settings_dir = settings_dir
        self._hook_port = hook_port
        self._orchestrator: Orchestrator | None = None
        self._api: CoreAPI | None = None

    @staticmethod
    def _emit(obj: dict[str, Any]) -> None:
        _emit_stdout(json.dumps(obj, ensure_ascii=False, default=str))

    async def run(self) -> None:
        from pathlib import Path as _Path

        from workflow_controller.api.core_api import BootError, boot

        sd = _Path(self._settings_dir) if self._settings_dir else None
        try:
            self._orchestrator, self._api = await boot(
                self._config,
                _Path(self._config_path),
                settings_dir=sd,
                hook_port=self._hook_port,
            )
        except BootError as exc:
            self._emit({"event": "error", "msg": str(exc)})
            raise SystemExit(3) from None

        self._api.session.set_reviewer_mode("auto")
        self._emit_auth_status(self._orchestrator)

        project_dir = self._config.project_dir

        if not project_dir:
            self._emit({"event": "error", "msg": "project_dir not set"})
            return

        self._emit({"event": "info", "msg": f"Starting session: {project_dir}"})

        event_cursor = self._api.event.snapshot_cursor()
        sid = await self._start_session(self._api, project_dir)
        if sid is None:
            return

        try:
            await self._pump_events(self._api, event_cursor)
        except KeyboardInterrupt:
            self._emit({"event": "interrupted", "msg": "Ctrl+C received"})

        # Emit final usage stats
        self._emit_usage(self._api)

        self._emit({"event": "done", "msg": "All done"})

    async def shutdown(self) -> None:
        if self._orchestrator:
            await self._orchestrator.shutdown()

    def _emit_auth_status(self, orchestrator: Orchestrator) -> None:
        """输出当前 auth 状态行。"""
        rotator = orchestrator.auth_rotator
        if rotator is not None:
            self._emit(
                {
                    "event": "auth",
                    "mode": rotator.get_mode().value,
                    "api_keys": rotator.get_available_count(),
                }
            )

    async def _start_session(self, api: CoreAPI, project_dir: str) -> str | None:
        """启动会话并输出 session_started；失败输出 error 并返回 None。"""
        try:
            result = await api.session.start(project_dir)
            if result.get("error"):
                self._emit({"event": "error", "msg": result["error"]})
                return None
            sid = cast("str | None", result.get("session_id", str(result)))
        except Exception as exc:
            self._emit({"event": "error", "msg": f"Failed to start session: {exc}"})
            return None
        self._emit({"event": "session_started", "id": sid})
        return sid

    async def _pump_events(self, api: CoreAPI, event_cursor: str) -> None:
        """轮询事件/会话/进度并输出，直到全部会话进入终态。"""
        all_done = False
        _prev_sessions: dict[str, Any] = {}
        _prev_pipeline: str = ""
        while not all_done:
            await asyncio.sleep(0.25)
            event_cursor = self._drain_events(api, event_cursor)
            _prev_sessions, all_done = self._emit_sessions_snapshot(api, _prev_sessions)
            _prev_pipeline = self._emit_pipeline(api, _prev_pipeline)

    def _drain_events(self, api: CoreAPI, event_cursor: str) -> str:
        """输出自上次游标以来的事件行，返回新游标。"""
        event_cursor, events = api.event.get_since_cursor(event_cursor)
        if events:
            for ev in events:
                data = dict(ev.data) if isinstance(ev.data, dict) else {}
                if ev.event_type == "assistant_message":
                    texts = [
                        b.get("text", "")
                        for b in data.get("content", [])
                        if isinstance(b, dict) and b.get("type") == "text"
                    ]
                    data["text"] = "".join(texts)
                self._emit(
                    {
                        "ts": ev.timestamp or "",
                        "event": ev.event_type,
                        "session": ev.session_id,
                        "data": data,
                    }
                )
        return event_cursor

    def _emit_sessions_snapshot(
        self, api: CoreAPI, prev: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        """会话快照有变化时输出 session_update；返回 (最新快照, 是否全部终态)。"""
        sessions = api.session.list_sessions()
        sessions_snap = {
            s.id: (s.state, s.current_iteration, s.progress, int(s.elapsed_seconds))
            for s in sessions
        }
        if sessions_snap == prev:
            return prev, False
        if sessions:
            self._emit(
                {
                    "event": "session_update",
                    "sessions": [
                        {
                            "id": s.id,
                            "state": s.state,
                            "iter": s.current_iteration,
                            "progress": int(s.progress * 100),
                            "elapsed": int(s.elapsed_seconds),
                        }
                        for s in sessions
                    ],
                }
            )
        terminal = {"completed", "stopped", "failed"}
        return sessions_snap, all(s.state in terminal for s in sessions)

    def _emit_pipeline(self, api: CoreAPI, prev: str) -> str:
        """进度有变化时输出 pipeline 行；返回最新进度串。"""
        pipeline = api.pipeline.snapshot()
        pipe_str = f"{pipeline.total_progress:.2f}"
        if pipe_str == prev:
            return prev
        self._emit(
            {
                "event": "pipeline",
                "progress": pipeline.total_progress,
                "detail": pipeline.detail,
                "stages": [
                    {
                        "name": s["name"],
                        "completed": s["completed"],
                        "active": s["active"],
                    }
                    for s in pipeline.stages
                ],
            }
        )
        return pipe_str

    def _emit_usage(self, api: CoreAPI) -> None:
        """输出最终 usage 统计（有 turn 才发）。"""
        usage = api.usage.snapshot(force=True)
        if usage and usage.turns > 0:
            self._emit(
                {
                    "event": "usage",
                    "turns": usage.turns,
                    "tokens_in": usage.tokens_in,
                    "tokens_out": usage.tokens_out,
                    "cache_read": usage.cache_read,
                    "cache_created": usage.cache_created,
                    "cost": usage.cost,
                }
            )
