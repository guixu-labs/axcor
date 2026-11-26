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

"""graph 插件 + run 平面的联合装配（多层断言测试共用）。

FakeRunSO 提供 session 出生/控制/谓词面；真实 RunStore/EventStore 落库。
bash 图与 agent 图均可驱动：auto_release=False 时成员 session 需手动放行。
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml

from workflow_controller.core.run_coordinator import RunConfig, RunCoordinator
from workflow_controller.core.runs import RunStore
from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService
from workflow_controller.event.type_registry import EventTypeRegistry
from workflow_controller.plugin.base import PluginConfig
from workflow_controller.plugin.context import PluginContext, PluginServices
from workflow_controller.plugins.graph.plugin import GRAPH_FORMAT, GraphPlugin
from workflow_controller.session.events import Event, EventType, SessionOutcome
from workflow_controller.store.event_store import EventStore

if TYPE_CHECKING:
    from pathlib import Path


class SwitchBus:
    """测试总线：把 session 事件转发给已注册的观察者。

    对齐生产总线的单库双表分派：装配了 run 键落库面时，有 run 归属且无
    session 身份的事件（如 watchdog 的 run 键告警）经落库面进 run_events
    表；session 键事件仅分发（成员事件落库不归测试总线）。
    """

    def __init__(self) -> None:
        self._observers: dict[Any, list[Any]] = {}
        self._awaited: dict[Any, list[Any]] = {}
        self._run_event_sink: Any = None

    def observe(self, event_type: Any, callback: Any) -> None:
        self._observers.setdefault(event_type, []).append(callback)

    def observe_awaited(self, event_type: Any, callback: Any) -> None:
        self._awaited.setdefault(event_type, []).append(callback)

    def detach_awaited_observer(self, event_type: Any, callback: Any) -> None:
        handlers = self._awaited.get(event_type)
        if handlers:
            handlers.remove(callback)

    def set_run_event_sink(self, sink: Any) -> None:
        """装配 run 键事件落库面（异步回调：Event → run_events 表）。"""
        self._run_event_sink = sink

    async def _dispatch(self, event: Event) -> None:
        for callback in self._awaited.get(event.event_type, []) + self._observers.get(
            event.event_type, []
        ):
            await callback(event)

    async def emit(self, event: Event) -> None:
        if event.run_id is not None and not event.session_id and self._run_event_sink is not None:
            await self._run_event_sink(event)
        await self._dispatch(event)

    async def emit_ephemeral(self, event: Event) -> None:
        await self._dispatch(event)


class FakeAgentSession:
    """假成员 session：出生即 running，放行后落 completed 终态。"""

    def __init__(self, sid: str, gate: asyncio.Event) -> None:
        self.id = sid
        self.plugin_metadata: dict[str, Any] = {}
        self.state = SimpleNamespace(value="running", name="RUNNING")
        self.stop_reason: str | None = None
        self.pause_requested = False
        self.loop_status = "yes"
        self.finished = asyncio.Event()
        self._gate = gate

    async def conclude(self) -> None:
        await self._gate.wait()
        self.state.value = "idle"
        self.state.name = "IDLE"
        self.stop_reason = "reviewer_complete"
        self.loop_status = "no"
        self.finished.set()


class FakeRunSO:
    """编排器替身：成员 session 出生与终态事件 + 协调器消费的谓词面。"""

    # sid 全局唯一：同库重装配（模拟重启）的第二个装配不能再发 s1，
    # 归属库是共享的（session 已属旧 run，重发即 attach 冲突）
    _sid_seq = 0

    def __init__(
        self,
        bus: SwitchBus,
        affiliation: Any,
        tmp: Path,
        auto_release: bool,
    ) -> None:
        self.bus = bus
        self._affiliation = affiliation
        self._auto_release = auto_release
        self.sessions: dict[str, FakeAgentSession] = {}
        self.spawn_calls: list[dict[str, Any]] = []
        self.applied_overrides: list[tuple[str, dict[str, Any]]] = []
        # registry 替身：当前已装载插件实例（reload 时换内容）
        self.plugins: list[Any] = []
        self.config = SimpleNamespace(runtime="opencode", project_dir=str(tmp))
        self.default_project_dir = tmp
        self.event_router = MagicMock()
        self.event_router.default_store = EventStore(tmp / ".workflow-controller" / "events.db")
        self.pool = MagicMock()
        setattr(self.pool, "_persist_session", AsyncMock())
        self.pool.get = self.sessions.get

        def _list_sessions() -> list[FakeAgentSession]:
            return list(self.sessions.values())

        self.pool.list_sessions = _list_sessions

    @staticmethod
    def is_restarting(sid: str) -> bool:
        return False

    async def ensure_reviewer_snapshot(self, sid: str) -> None:
        """reviewer 快照重建接口的测试替身（无 reviewer 概念，空实现）。"""

    def release_all(self) -> None:
        """放行全部成员 session（触发终态与结局事件）。"""
        for session in self.sessions.values():
            getattr(session, "_gate").set()

    async def start_session_with_config(
        self,
        cfg: Any,
        *,
        prompt: str | None = None,
        plugin_metadata: dict[str, Any] | None = None,
    ):
        setattr(FakeRunSO, "_sid_seq", getattr(FakeRunSO, "_sid_seq") + 1)
        sid = f"s{getattr(FakeRunSO, '_sid_seq')}"
        gate = asyncio.Event()
        if self._auto_release:
            gate.set()
        session = FakeAgentSession(sid, gate)
        self.sessions[sid] = session
        # 出生调用留档：断言 spec 组装与溯源参数
        self.spawn_calls.append({"cfg": cfg, "plugin_metadata": plugin_metadata})
        if cfg.run_id:
            await self._affiliation.attach(sid, cfg.run_id, role=cfg.role)
        # 激活回执：出生即运行时就绪
        await self.bus.emit(
            Event.create(
                session_id=sid,
                iteration=0,
                event_type=EventType.SESSION_ACTIVATED,
                data={"ok": True, "error": None},
            )
        )

        async def _conclude_and_report() -> None:
            await session.conclude()
            await self.bus.emit(
                Event.create(
                    session_id=sid,
                    iteration=1,
                    event_type=EventType.SESSION_OUTCOME,
                    data=SessionOutcome(
                        session_id=sid,
                        kind="completed",
                        stop_reason="reviewer_complete",
                        last_error=None,
                        result_digest="done",
                        iteration=1,
                        context=None,
                    ).to_payload(),
                )
            )

        asyncio.get_running_loop().create_task(_conclude_and_report())
        return session

    async def pause_session(self, sid: str) -> None:
        session = self.sessions.get(sid)
        if session is not None and session.loop_status == "yes":
            session.loop_status = "no"
            session.state.value = "idle"
            session.state.name = "IDLE"
            session.stop_reason = "user_pause"
            session.pause_requested = True
            await self.bus.emit(
                Event.create(
                    session_id=sid,
                    iteration=1,
                    event_type=EventType.SESSION_STATE_CHANGED,
                    data={"from": "running", "to": "idle"},
                )
            )
            await self.bus.emit(
                Event.create(
                    session_id=sid,
                    iteration=1,
                    event_type=EventType.SESSION_OUTCOME,
                    data=SessionOutcome(
                        session_id=sid,
                        kind="paused",
                        stop_reason="user_pause",
                        last_error=None,
                        result_digest=None,
                        iteration=1,
                        context=None,
                    ).to_payload(),
                )
            )

    async def resume_session(self, sid: str) -> None:
        session = self.sessions.get(sid)
        if session is not None and session.stop_reason == "user_pause":
            session.loop_status = "yes"
            session.state.value = "running"
            session.state.name = "RUNNING"
            session.stop_reason = None
            session.pause_requested = False
            await self.bus.emit(
                Event.create(
                    session_id=sid,
                    iteration=0,
                    event_type=EventType.SESSION_ACTIVATED,
                    data={"ok": True, "error": None},
                )
            )
            await self.bus.emit(
                Event.create(
                    session_id=sid,
                    iteration=0,
                    event_type=EventType.SESSION_STATE_CHANGED,
                    data={"from": "idle", "to": "running"},
                )
            )

    async def apply_session_overrides(self, sid: str, overrides: dict[str, Any]) -> None:
        self.applied_overrides.append((sid, dict(overrides)))

    async def stop_session(self, sid: str) -> None:
        session = self.sessions.get(sid)
        if session is not None:
            session.state.value = "stopped"
            session.state.name = "STOPPED"
            session.loop_status = "no"

    async def wait_session_done(self, sid: str, timeout: float | None = None) -> bool:
        session = self.sessions.get(sid)
        if session is None:
            return True
        try:
            await asyncio.wait_for(session.finished.wait(), timeout=timeout)
        except TimeoutError:
            return False
        return True

    def get_session_facts(self, sid: str) -> Any:
        session = self.sessions.get(sid)
        if session is None:
            return None
        return SimpleNamespace(
            state=session.state.value,
            stop_reason=session.stop_reason,
            loop_status=session.loop_status,
        )

    def is_pausable(self, sid: str) -> bool:
        facts = self.get_session_facts(sid)
        return facts is not None and facts.loop_status == "yes"

    def is_resumable(self, sid: str) -> bool:
        facts = self.get_session_facts(sid)
        return facts is not None and facts.state == "idle" and facts.stop_reason == "user_pause"

    def is_terminal(self, sid: str) -> bool:
        facts = self.get_session_facts(sid)
        if facts is None:
            return True
        if facts.loop_status == "no":
            return not (facts.state == "idle" and facts.stop_reason == "user_pause")
        return False

    def discard_terminal_session(self, sid: str) -> bool:
        if sid not in self.sessions or not self.is_terminal(sid):
            return False
        self.pool.discard(sid)
        self.sessions.pop(sid, None)
        return True

    def restore_session_from_store(self, sid: str) -> bool:
        return self.pool.restore_from_store(sid) is not None

    def discard_deleted_session(self, sid: str) -> None:
        """删除链内存驱逐（对齐生产 pool.discard：只清内存行）。"""
        self.sessions.pop(sid, None)


class GraphRunPlaneEnv:
    """run 平面 + graph 插件的联合装配。"""

    _label_seq = 0

    def __init__(self, tmp: Path, auto_release: bool = True) -> None:
        self.tmp = tmp
        # 单库纪律：run 账面与事件同库——delete_run 级联删
        # events/loop_results 等成员数据表，分库会让事务撞缺表
        self.store = RunStore(tmp / ".workflow-controller" / "events.db")
        self.affiliation = SessionRunAffiliationService(self.store)
        self.bus = SwitchBus()
        self.so = FakeRunSO(self.bus, self.affiliation, tmp, auto_release)
        # run 键事件落库面：内核订阅者（watchdog）经总线发 run 键告警时与
        # 生产同规则落 run_events 表（协调器分发面已自行落库、只分发）
        self.bus.set_run_event_sink(self._store_run_event)
        self.plugin = GraphPlugin(
            PluginConfig(name="graph", version="0.1.0", enabled=True),
            tmp / "config.yaml",
        )
        self.coord: RunCoordinator | None = None

        def _find_plugin(name: str) -> Any:
            return next((p for p in self.so.plugins if p.config.name == name), None)

        self.plugin.install(
            PluginContext(
                bus=self.bus,
                type_registry=EventTypeRegistry(),
                hook_server=MagicMock(),
                work_dir=tmp,
                services=PluginServices(find_plugin=_find_plugin),
            )
        )
        self.so.plugins.append(self.plugin)
        self.coord = RunCoordinator(
            orchestrator=self.so,
            run_store=self.store,
            bus=self.bus,
            driver_factories=self.plugin.get_run_driver_factories(),
            affiliation=self.affiliation,
        )

    def write_workflow(self, nodes: list[dict]) -> Path:
        path = self.tmp / "wf.yaml"
        path.write_text(
            yaml.safe_dump(
                {
                    "workflow": "switch",
                    "max_parallel": 2,
                    "work_dir": ".",
                    "nodes": nodes,
                }
            ),
            encoding="utf-8",
        )
        return path

    async def start_graph_run(
        self,
        nodes: list[dict] | None = None,
        *,
        yaml_path: Path | None = None,
        label: str | None = None,
        params: dict[str, str] | None = None,
    ) -> str:
        """经 run 平面直启 workflow run（唯一启动通道），返回 run_id。"""
        setattr(GraphRunPlaneEnv, "_label_seq", getattr(GraphRunPlaneEnv, "_label_seq") + 1)
        path = yaml_path or self.write_workflow(nodes or [])
        if self.coord is None:
            raise RuntimeError("coord 未初始化（需先经 run 平面装配）")
        run = await self.coord.start_run(
            RunConfig(
                format=GRAPH_FORMAT,
                params=params or {},
                labels={"name": label or f"g{getattr(GraphRunPlaneEnv, '_label_seq')}"},
                program_ref=str(path),
                work_dir=str(self.tmp / "wd"),
            ),
            path,
        )
        return run.run_id

    def driver_of(self, run_id: str) -> Any:
        """run 的内存驱动实例（插件 run_id 键注册表）。"""
        return getattr(self.plugin, "_drivers")[run_id]

    async def _store_run_event(self, event: Event) -> None:
        """run 键事件落库回调（SwitchBus 落库面）。"""
        await self.so.event_router.default_store.store_run_event(
            event.run_id or "",
            event.event_type,
            dict(event.data or {}),
            context=event.context,
            timestamp=event.timestamp,
        )

    def node_info(self, run_id: str, node_id: str) -> dict[str, Any]:
        """节点态快照切片。"""
        return self.driver_of(run_id).state.snapshot()["nodes"][node_id]

    async def recover(self) -> None:
        """模拟重启后的 run 平面恢复（boot 时序里的 recover_at_boot）。"""
        if self.coord is None:
            raise RuntimeError("coord 未初始化（需先经 run 平面装配）")
        await self.coord.recover_at_boot()


async def await_run_status(
    env: GraphRunPlaneEnv,
    run_id: str,
    targets: set[str],
    timeout_s: float = 10.0,
) -> Any:
    """轮询等待 run 账面到达目标状态。"""
    deadline = time.monotonic() + timeout_s
    while True:
        run = await env.store.get(run_id)
        if run.status in targets:
            return run
        if time.monotonic() > deadline:
            pytest.fail(f"run {run_id} 未达 {targets}（当前 {run.status}）")
        await asyncio.sleep(0.02)


async def await_graph_status(
    env: GraphRunPlaneEnv,
    run_id: str,
    targets: set[str],
    timeout_s: float = 15.0,
) -> None:
    """轮询等待内存驱动图状态到达目标。"""
    deadline = time.monotonic() + timeout_s
    while True:
        status = env.driver_of(run_id).state.snapshot().get("status")
        if status in targets:
            return
        if time.monotonic() > deadline:
            pytest.fail(f"run {run_id} 图状态未达 {targets}（当前 {status}）")
        await asyncio.sleep(0.02)
