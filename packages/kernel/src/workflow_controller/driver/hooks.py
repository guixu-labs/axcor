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

"""Driver-agnostic hook contract.

Each driver runtime exposes tool-use / stop interception differently:
  - claude_cli  : tool/stop hooks POST to the HTTP hook server; subagent
                  lifecycle callbacks come from the transcript poller.
  - claude_sdk  : claude_agent_sdk's in-process Python hook callbacks
                  (ClaudeAgentOptions.hooks).
  - opencode    : (future) its own mechanism.
  - codex_cli   : command hook 回流 Stop / Subagent 事件。

The orchestrator supplies a single ``DriverHooks`` bundle with callbacks keyed
by the *Workflow Controller* session id. Each driver is responsible for translating its
native hook payload into the CC-style ``data`` dict and translating the CC-style
result back. This keeps the reviewer decision logic in one place
(orchestrator) and confines per-runtime quirks to each driver.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import sys
import time
import urllib.request
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from workflow_controller.logging_config import emit_stdout

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


# (workflow_controller_session_id, data) -> CC hook output dict | None
# PreToolUse data carries at least {"tool_name", "tool_input"}.
# Stop data carries at least {"last_assistant_message"}.
HookHandler = Callable[[str, dict[str, Any]], Awaitable[dict[str, Any] | None]]

# (workflow_controller_session_id, data) -> None — notification only (no CC decision).
# Subagent data carries {"agent_id", "agent_type"} (+ "agent_transcript_path"
# on stop; + "messages" (incremental new blocks) on progress). Used to track
# in-flight sub-agents, wake waiting sessions, and emit subagent events.
SubagentHandler = Callable[[str, dict[str, Any]], Awaitable[None]]
ToolNotificationHandler = Callable[[str, dict[str, Any]], Awaitable[None]]

# (workflow_controller_session_id, tool_input) -> answer string. opencode gate entry: the
# worker called the gate MCP tool (AskUserQuestion-equivalent); the orchestrator
# resolves an answer (auto: reviewer; manual: human via the manual queue) and
# the driver returns it as the tool result. ``tool_input`` is the raw AUQ-shaped
# dict ``{"questions": [...]}``; the orchestrator parses it. Driver-layer
# primitive (dict, not core's AskQuestion) to avoid a layer leak.
GateQuestionHandler = Callable[[str, dict[str, Any]], Awaitable[str]]

# () -> {"subagent_hints": {type: [hint, ...]}, "instructions": [...]}.
# Overlay seam — only opencode consumes; SDK/CLI get an empty snapshot.
OverlaysGetter = Callable[[], dict[str, Any]]

_MANAGED_DESCRIPTION = "Workflow Controller managed Codex hooks"
_HOOK_EVENTS = {
    "Stop": ("stop", 330, 320),
    "SubagentStart": ("subagent-start", 15, 10),
    "SubagentStop": ("subagent-stop", 15, 10),
}
_STOP_RELAY_ATTEMPTS = 3
_STOP_RELAY_RETRY_BUDGET_SECONDS = 5.0
_STOP_RELAY_RETRY_DELAY_SECONDS = 0.5


class SubagentProgressBridge:
    """会话级子代理进度对账桥，持有当前查询的监视器。"""

    def __init__(self) -> None:
        self._monitor: Any | None = None

    def register(self, monitor: Any) -> None:
        """登记当前查询的监视器。"""
        self._monitor = monitor

    def unregister(self, monitor: Any) -> None:
        """移除已结束查询的监视器。"""
        if self._monitor is monitor:
            self._monitor = None

    async def notify_started(self, child_thread_id: str) -> None:
        """转发 SubagentStart 给当前监视器。"""
        if self._monitor is not None:
            await self._monitor.notify_subagent_started(child_thread_id)

    async def notify_stopped(self, child_thread_id: str) -> None:
        """转发 SubagentStop 给当前监视器。"""
        if self._monitor is not None:
            await self._monitor.notify_subagent_stopped(child_thread_id)


@dataclass
class DriverHooks:
    """Hook callbacks the orchestrator hands to a driver for one session."""

    session_id: str
    # 通用 Hook Server base URL；需要子进程回连 Hook Server 的 runtime 可用。
    hook_server_base_url: str | None = None
    on_pre_tool_use: HookHandler | None = None
    on_post_tool_use: ToolNotificationHandler | None = None
    file_change_hook_url: str | None = None
    file_changes_enabled: Callable[[], bool] | None = None
    on_stop: HookHandler | None = None
    on_subagent_start: SubagentHandler | None = None
    on_subagent_progress: SubagentHandler | None = None
    on_subagent_stop: SubagentHandler | None = None
    subagent_progress_bridge: SubagentProgressBridge | None = None
    # opencode only (cc has native AskUserQuestion). When set, the opencode
    # driver starts a per-query in-process MCP gate server (see opencode_gate)
    # so the worker can call the gate tool; None → no gate (graceful).
    on_gate_question: GateQuestionHandler | None = None
    # Overlay seam (opencode-only; SDK/CLI get empty). See OverlaysGetter.
    get_overlays: OverlaysGetter | None = None
    auto_compaction: bool = False
    # 当前 runtime 的 hooks 配置已通过托管校验。
    runtime_hook_config_valid: Callable[[], bool] | None = None


class CodexHookConfigWriter:
    """生成并校验 Workflow Controller 托管的 Codex hooks 配置。"""

    @staticmethod
    def build_config() -> dict[str, Any]:
        """构建固定版本的 managed hooks 配置。"""
        hooks: dict[str, list[dict[str, Any]]] = {}
        for event, (_, hook_timeout, _) in _HOOK_EVENTS.items():
            hooks[event] = [
                {
                    "hooks": [
                        {
                            "type": "command",
                            "command": shlex.join(
                                [
                                    sys.executable,
                                    "-m",
                                    "workflow_controller",
                                    "codex-hook",
                                    event,
                                ]
                            ),
                            "timeout": hook_timeout,
                        }
                    ],
                }
            ]
        return {"description": _MANAGED_DESCRIPTION, "hooks": hooks}

    def prepare(self, project_dir: Path) -> None:
        """仅在配置缺失或已是 managed 格式时写入。"""
        hooks_file = project_dir / ".codex" / "hooks.json"
        expected = self.build_config()
        if not hooks_file.exists():
            hooks_file.parent.mkdir(parents=True, exist_ok=True)
            hooks_file.write_text(
                json.dumps(expected, indent=2) + "\n",
                encoding="utf-8",
            )
            return
        try:
            existing = json.loads(hooks_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("invalid Codex hooks configuration") from exc
        if existing != expected:
            raise ValueError("unknown Codex hooks configuration")

    def is_managed(self, project_dir: Path) -> bool:
        """判断配置是否仍与 managed 定义完全一致。"""
        hooks_file = project_dir / ".codex" / "hooks.json"
        try:
            existing = json.loads(hooks_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False
        return bool(existing == self.build_config())


def build_hook_payload(
    codex_input: dict[str, Any],
    env: dict[str, str],
) -> dict[str, Any]:
    """把 Codex 输入映射为按 Workflow Controller session 路由的请求。"""
    payload = dict(codex_input)
    payload["codex_input"] = codex_input
    return payload


def _post_hook_once(url: str, payload: bytes, request_timeout: int) -> dict[str, Any]:
    """发一次 HookServer POST 并解析 JSON 响应；非 200 以 RuntimeError 拒绝。"""
    request = urllib.request.Request(
        url,
        data=payload,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=request_timeout) as response:
        if response.status != 200:
            raise RuntimeError("Codex hook server rejected request")
        return json.loads(response.read().decode())


def _request_hook(
    url: str,
    payload: bytes,
    request_timeout: int,
    retry: bool,
) -> dict[str, Any]:
    """请求 HookServer 并按需对快速失败有限重试。"""
    attempts = _STOP_RELAY_ATTEMPTS if retry else 1
    deadline = time.monotonic() + (_STOP_RELAY_RETRY_BUDGET_SECONDS if retry else 0.0)
    for attempt in range(1, attempts + 1):
        try:
            return _post_hook_once(url, payload, request_timeout)
        except Exception:
            remaining = deadline - time.monotonic()
            if attempt == attempts or remaining <= 0:
                raise
            logger.warning(
                "Codex hook relay attempt %d/%d failed; retrying",
                attempt,
                attempts,
            )
            time.sleep(min(_STOP_RELAY_RETRY_DELAY_SECONDS, remaining))
    raise RuntimeError("Codex hook relay retries exhausted")


def relay_hook(
    event: str,
    codex_input: dict[str, Any],
    env: dict[str, str],
) -> str | None:
    """回流单个 Codex hook 并返回 Stop 输出。"""
    path, _, request_timeout = _HOOK_EVENTS[event]
    base_url = env.get("WORKFLOW_CONTROLLER_HOOK_SERVER_URL", "").rstrip("/")
    if not base_url:
        raise ValueError("Codex hook relay environment is incomplete")
    result = _request_hook(
        f"{base_url}/hooks/{path}",
        json.dumps(build_hook_payload(codex_input, env)).encode(),
        request_timeout,
        event == "Stop",
    )
    if event != "Stop":
        return None
    if result.get("decision") == "block":
        reason = result.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            logger.error("Invalid Codex stop block reason: %r", reason)
            reason = "Workflow Controller reviewer requested continuation."
        return json.dumps({"decision": "block", "reason": reason})
    return "{}"


def run_codex_hook(event: str) -> int:
    """执行 Codex command hook 并返回退出码。"""
    if event not in _HOOK_EVENTS:
        logger.error("invalid codex hook event")
        return 2
    try:
        output = relay_hook(event, _read_hook_input(), dict(os.environ))
        if output is not None:
            emit_stdout(output)
        return 0
    except Exception:
        logger.exception("Codex hook relay failed")
        if event == "Stop":
            emit_stdout("{}")
        return 0


def _read_hook_input() -> dict[str, Any]:
    """读取并校验 Codex stdin JSON。"""
    try:
        value = json.loads(sys.stdin.read())
    except json.JSONDecodeError as exc:
        raise ValueError("invalid Codex hook input") from exc
    if not isinstance(value, dict):
        raise ValueError("invalid Codex hook input")
    return value
