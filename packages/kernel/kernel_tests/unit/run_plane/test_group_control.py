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

"""组控制、准入、cwd 与 run 态投影：run 级控制原语的验收。"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

import pytest

from workflow_controller.api.session_api import SessionAPI, WorkflowSessionOperationError
from workflow_controller.core.run_coordinator import RunConfig, RunCoordinator
from workflow_controller.core.runs import MemberResumeDeniedError, RunStore
from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService
from workflow_controller.core.status_maps import PauseReasonConflictError
from workflow_controller.plugin.run_driver import (
    AwaitSessions,
    ControlNotification,
    FinishOutcome,
    FinishRun,
    PauseRun,
    ProcessExit,
    RestartPolicy,
    RestartSession,
    SessionActivated,
    SessionControlApplied,
    SessionOutcomeNotification,
    SessionSpec,
    StartProcess,
    StartSession,
)

from ._run_plane_stubs import (
    PoolFactsMixin,  # SO 谓词面（get_session_facts + 三判定）
    StubBus,
    StubPool,
    StubSession,
    member_outcome_payload,
)

if TYPE_CHECKING:
    from workflow_controller.session.events import SessionOutcome

# ── 测试替身 ──────────────────────────────────────────────────────


class FakeEventRouter:
    def __init__(self) -> None:
        self.run_events: list[tuple[str, str, dict]] = []

    @property
    def default_store(self):
        return self

    async def store_run_event(
        self, run_id: str, event_type: str, data: dict, *, context: str | None = None
    ) -> None:
        self.run_events.append((run_id, event_type, dict(data)))


class GroupFakeSO(PoolFactsMixin):
    """组控用 SO 替身：谓词镜像真判据，控制原语记录并翻态。"""

    def __init__(self, affiliation, bus, project_dir, run_store) -> None:
        self.pool = StubPool()
        self.affiliation = affiliation
        self.bus = bus
        self.default_project_dir = project_dir
        self.run_store = run_store
        self.event_router = FakeEventRouter()
        self.plugins: list = []  # graph 同步钩子遍历用（空 = 无插件）
        self.started: list = []
        self.paused: list[str] = []
        self.resumed: list[str] = []
        self.stopped: list[str] = []
        self.restarted: list[str] = []
        self.injected: list[tuple[str, str]] = []
        self.overridden: list[tuple[str, dict]] = []
        self.restarting: set[str] = set()
        self.birth_gates: dict[str, asyncio.Event] = {}

    async def start_session_with_config(self, cfg):
        gate = self.birth_gates.get(cfg.name)
        if gate is not None:
            await gate.wait()
        s = StubSession(
            f"s{len(self.started) + 1}",
            cfg.name,
            run_id=cfg.run_id,
        )
        self.pool.sessions[s.id] = s
        self.started.append(cfg)
        await self.affiliation.attach(s.id, cfg.run_id, role=cfg.role)
        if "fail" in cfg.name:
            asyncio.get_running_loop().create_task(self._conclude(s, "failed"))
        elif "slow" not in cfg.name:
            asyncio.get_running_loop().create_task(self._conclude(s, "completed"))
        return s

    def mark_terminal(self, s: StubSession, kind: str) -> None:
        s.state = "idle"
        s.loop_status = "no"
        s.stop_reason = "error" if kind == "failed" else "reviewer_complete"
        asyncio.get_running_loop().create_task(
            self.bus.emit_outcome(s.id, member_outcome_payload(s.id, kind, s.stop_reason))
        )

    async def pause_session(self, sid: str) -> None:
        s = self.pool.get(sid)
        if s is not None:  # Session.pause 落点：IDLE + no + user_pause
            s.state, s.loop_status, s.stop_reason = "idle", "no", "user_pause"
        self.paused.append(sid)

    async def resume_session(self, sid: str) -> None:
        s = self.pool.get(sid)
        if s is not None:
            s.state, s.loop_status, s.stop_reason = "running", "yes", None
        self.resumed.append(sid)

    async def stop_session(self, sid: str) -> None:
        s = self.pool.get(sid)
        if s is not None:
            s.state, s.loop_status = "stopped", "no"
        self.stopped.append(sid)

    async def restart_session(self, sid: str) -> None:
        s = self.pool.get(sid)
        if s is None:
            raise ValueError(f"session {sid} 不在池内，无法重启")
        s.state, s.loop_status, s.stop_reason = "running", "yes", None
        self.restarted.append(sid)

    async def apply_session_overrides(self, sid: str, overrides: dict) -> None:
        if self.pool.get(sid) is None:
            raise ValueError(f"session {sid} 不在池内，无法更新配置")
        self.overridden.append((sid, dict(overrides)))

    async def inject_prompt(self, sid: str, prompt: str) -> None:
        self.injected.append((sid, prompt))

    async def wait_session_done(self, sid: str, timeout: float | None = None) -> bool:
        s = self.pool.get(sid)
        if s is None:
            return False
        deadline = None if timeout is None else time.monotonic() + timeout
        while s.loop_status == "yes":
            if deadline is not None and time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.01)
        return True

    def is_restarting(self, sid: str) -> bool:
        return sid in self.restarting

    async def _conclude(self, s: StubSession, kind: str) -> None:
        await asyncio.sleep(0.05)
        self.mark_terminal(s, kind)


def _spec(name: str, cwd: str = "/tmp") -> SessionSpec:
    return SessionSpec(name=name, prompt="p", cwd=cwd)


class RecordingDriver:
    """记录通知的驱动基型；slow 成员不自动收口。"""

    def __init__(self, first_actions: list | None = None) -> None:
        self.first_actions = first_actions or []
        self.activated: list[SessionActivated] = []
        self.outcomes: dict[str, SessionOutcome] = {}
        self.controls: list[ControlNotification] = []
        self.session_controls: list[SessionControlApplied] = []
        self.exits: list[ProcessExit] = []
        self.timeline: list[tuple[str, Any]] = []  # 跨通道消费序（on_control/decide）
        self.worlds: list[Any] = []
        self.rounds = 0

    @staticmethod
    def snapshot():
        return {"schema_version": 1, "state": {}, "pending_timers": []}

    @staticmethod
    def handle_op(op, args):
        return {}

    def decide(self, world, notifications):
        self.rounds += 1
        self.worlds.append(world)
        self._consume(notifications)
        if self.first_actions and self.rounds == 1:
            return list(self.first_actions)
        return []

    def on_control(self, control) -> None:
        self.controls.append(control)
        self.timeline.append(("control", control))

    def projection(self):
        return {"schema_version": 1, "rounds": self.rounds}

    def _consume(self, notifications) -> None:
        for n in notifications:
            if isinstance(n, SessionActivated):
                self.activated.append(n)
            elif isinstance(n, SessionOutcomeNotification):
                self.outcomes[n.outcome.session_id] = n.outcome
            elif isinstance(n, SessionControlApplied):
                self.session_controls.append(n)
            elif isinstance(n, ProcessExit):
                self.exits.append(n)
            self.timeline.append(("note", n))


class OneShotDriver(RecordingDriver):
    """起一个慢成员即等待。"""

    def __init__(self, name: str = "n-slow", cwd: str = "/tmp") -> None:
        super().__init__([StartSession(spec=_spec(name, cwd), context="a")])


class SimpleFinishDriver(RecordingDriver):
    """起一个快成员，收到结局即 Finish。"""

    def decide(self, world, notifications):
        self.rounds += 1
        self.worlds.append(world)
        self._consume(notifications)
        if self.rounds == 1:
            return [StartSession(spec=_spec("n1"), context="a")]
        if self.outcomes:
            ok = all(o.kind == "completed" for o in self.outcomes.values())
            return [FinishRun(FinishOutcome.COMPLETED if ok else FinishOutcome.FAILED)]
        return []


class FakeFactory:
    def __init__(self, fmt: str, driver) -> None:
        self.format = fmt
        self.driver = driver
        self.created = 0
        self.restored = 0

    def create(self, run, program_path, params):
        self.created += 1
        return self.driver

    def restore(self, run, snapshot, sessions):
        self.restored += 1
        return self.driver


class GroupHarness:
    def __init__(self, tmp_path, *factories: FakeFactory) -> None:
        self.store = RunStore(tmp_path / "events.db")
        self.affiliation = SessionRunAffiliationService(self.store)
        self.bus = StubBus()
        self.so = GroupFakeSO(self.affiliation, self.bus, tmp_path, self.store)
        self.factories = {f.format: f for f in factories}
        self.coord = RunCoordinator(
            orchestrator=self.so,
            run_store=self.store,
            bus=self.bus,
            driver_factories=dict(self.factories),
            affiliation=self.affiliation,
        )
        self.program = tmp_path / "prog.txt"
        self.program.write_text("format: fake", encoding="utf-8")

    async def start(
        self,
        fmt: str = "fake@1",
        *,
        max_parallel: int | None = None,
    ) -> str:
        run = await self.coord.start_run(
            RunConfig(format=fmt, max_parallel=max_parallel),
            self.program,
        )
        return run.run_id

    async def eventually(self, pred, timeout_s: float = 5.0) -> None:
        """轮询等待条件成立（谓词可为协程函数）。"""
        deadline = time.monotonic() + timeout_s
        while True:
            ok = pred()
            if asyncio.iscoroutine(ok):
                ok = await ok
            if ok:
                return
            assert time.monotonic() < deadline, "条件等待超时"
            await asyncio.sleep(0.01)

    async def await_terminal(self, run_id: str, timeout_s: float = 5.0) -> str:
        async def _poll() -> str:
            while True:
                run = await self.store.get(run_id)
                if run.finished_at is not None:
                    return run.status
                await asyncio.sleep(0.01)

        return await asyncio.wait_for(_poll(), timeout_s)

    async def status_is(self, run_id: str, status: str, reason: str | None = None) -> bool:
        run = await self.store.get(run_id)
        return (run.status, run.pause_reason) == (status, reason)

    def session_by_name(self, name: str) -> StubSession:
        return next(s for s in self.so.pool.sessions.values() if s.name == name)

    async def paused_session(self, fmt: str = "fake@1") -> tuple[str, str]:
        """起 run、等首个慢成员出生并置为用户暂停断点（restart 用例公共前置）。"""
        run_id = await self.start(fmt)
        driver = self.factories[fmt].driver
        await self.eventually(lambda: len(driver.activated) >= 1)
        sid = driver.activated[0].session_id
        session = self.session_by_name("n-slow")
        session.state, session.loop_status, session.stop_reason = "idle", "no", "user_pause"
        return run_id, sid

    def restart_member_fresh(
        self,
        run_id: str,
        sid: str,
        spec: SessionSpec,
        *,
        context: str | None = None,
    ) -> None:
        """FRESH 成员重启的测试直通道（run 键宿主面）。"""
        overrides: dict = {"spec_override": spec}
        if context is not None:
            overrides["context_override"] = context
        getattr(self.coord, "_host").restart_run_session(
            run_id,
            sid,
            RestartPolicy.FRESH,
            **overrides,
        )


# ── pause / resume 组控 ──────────────────────────────────────────


class TestPauseResume:
    @staticmethod
    def test_pause_run_reason_domain_enforced_at_construction():
        """reason 值域构造期白名单校验：manual 与任意值域外取值都拒。"""
        for bad_reason in ("manual", "hold", ""):
            with pytest.raises(ValueError, match=r"PauseRun\.reason"):
                PauseRun(reason=bad_reason)
        PauseRun(reason="awaiting_answer")  # 值域内照常构造
        PauseRun(reason="awaiting_external_fix")

    async def test_roundtrip_with_bookkeeping(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)
        sid = driver.activated[0].session_id

        record = await h.coord.pause_run(run_id)
        assert (record.status, record.pause_reason) == ("paused", "user")
        assert h.so.paused == [sid]
        assert (await h.store.list_run_sessions(run_id))[0].paused_by == "run"
        await h.eventually(lambda: any(c.kind == "paused" for c in driver.controls))
        paused = next(c for c in driver.controls if c.kind == "paused")
        assert paused.payload == {
            "reason": "manual"
        }  # 受理序列杀了在途（run 键进程），payload 带 manual 语境

        record = await h.coord.resume_run(run_id)
        assert (record.status, record.pause_reason) == ("running", None)
        assert h.so.resumed == [sid]
        assert (await h.store.list_run_sessions(run_id))[0].paused_by is None
        await h.eventually(lambda: any(c.kind == "resumed" for c in driver.controls))
        resumed = next(c for c in driver.controls if c.kind == "resumed")
        assert resumed.payload is None  # 不带 payload 的恢复载荷为 None（既有调用方）

    async def test_pause_receipt_precedes_killed_process_exit(self, tmp_path):
        """手动暂停：受理回执先于被杀回执进驱动，且载荷带 manual。

        kill_processes 等进程真死后同步回投 ProcessExit——回执若在杀后才
        投，被杀回执先进驱动、暂停语境还没记，照真失败结算（「暂停变
        失败」的老问题）。
        """
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)  # 慢成员撑住 run 不收尾
        getattr(h.coord, "_host").start_process(
            run_id, "sleep 30", cwd=str(tmp_path), context="bash#1"
        )
        # 等 spawn 落定进注册表（kill 按注册表杀，未落定的 spawn 会漏杀）
        await h.eventually(
            lambda: bool(getattr(getattr(h.coord, "_host"), "_processes").get(run_id))
        )
        await h.coord.pause_run(run_id)
        # 受理回执即消费（暂停期只排空控制通知）；被杀回执滞留 FIFO，
        # 恢复轮才进 decide——两条通道的消费先后在 timeline 里验
        await h.eventually(lambda: any(c.kind == "paused" for c in driver.controls))
        paused = next(c for c in driver.controls if c.kind == "paused")
        assert paused.payload == {"reason": "manual"}
        await h.coord.resume_run(run_id)
        await h.eventually(lambda: driver.exits)

        # 消费序：PAUSED（on_control 通道）先于被杀 ProcessExit（decide 通道）
        idx_paused = next(
            i
            for i, (tag, n) in enumerate(driver.timeline)
            if tag == "control" and n.kind == "paused"
        )
        idx_exit = next(
            i for i, (tag, n) in enumerate(driver.timeline) if isinstance(n, ProcessExit)
        )
        assert idx_paused < idx_exit
        [exit_note] = driver.exits
        assert exit_note.returncode < 0  # 信号终止形态（-15）
        assert exit_note.exit_mark == "plane_killed"  # 本面组杀标记
        assert exit_note.context == "bash#1"
        await h.coord.stop_run(run_id)

    async def test_pause_skips_terminal_session(self, tmp_path):
        h = GroupHarness(
            tmp_path,
            FakeFactory(
                "fake@1",
                RecordingDriver(
                    [
                        StartSession(spec=_spec("n-done"), context="a"),
                        StartSession(spec=_spec("n-slow"), context="b"),
                    ]
                ),
            ),
        )
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: bool(driver.outcomes))

        record = await h.coord.pause_run(run_id)
        assert record.status == "paused"
        # 已结束成员跳过（不覆盖 stop_reason），只暂停在途成员
        assert h.so.paused == [h.session_by_name("n-slow").id]
        assert h.session_by_name("n-done").stop_reason == "reviewer_complete"

    async def test_pause_drains_inflight_birth_late_session_caught(self, tmp_path):
        """受理时出生在途——排空后才逐成员暂停，迟到成员不逃逸。"""
        # 名字带 slow 防替身自动收口：成员须活到组暂停扫掠，用例才有判定意义
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver("late-slow")))
        h.so.birth_gates["late-slow"] = asyncio.Event()
        run_id = await h.start()
        rt = getattr(h.coord, "_runtimes")[run_id]
        await h.eventually(lambda: rt.in_flight == 1)

        async def _release() -> None:
            await asyncio.sleep(0.05)
            h.so.birth_gates["late-slow"].set()

        releaser = asyncio.get_running_loop().create_task(_release())
        record = await h.coord.pause_run(run_id)
        await releaser
        assert record.status == "paused"
        assert h.so.paused == [h.session_by_name("late-slow").id]
        assert (await h.store.list_run_sessions(run_id))[0].paused_by == "run"

    async def test_batch_start_frozen_by_pause_pumped_after_resume(self, tmp_path):
        """批内窗口：decide 在途时受理 pause，批内 Start 挂起 FIFO 不派发。"""

        class SlowDecideDriver(RecordingDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                self.worlds.append(world)
                self._consume(notifications)
                if self.rounds == 1:
                    time.sleep(0.2)
                    return [StartSession(spec=_spec("frozen"), context="a")]
                return []

        driver = SlowDecideDriver()
        h = GroupHarness(tmp_path, FakeFactory("fake@1", driver))
        run_id = await h.start()
        rt = getattr(h.coord, "_runtimes")[run_id]
        await h.eventually(lambda: driver.rounds >= 1)

        await h.coord.pause_run(run_id)
        await h.eventually(lambda: len(rt.deferred) == 1)
        await asyncio.sleep(0.15)  # decide 已返回，批内 Start 必须仍冻结
        assert h.so.started == []
        assert rt.in_flight == 1  # 含挂起：world 口径一致

        await h.coord.resume_run(run_id)
        await h.eventually(lambda: len(h.so.started) == 1)
        assert h.so.started[0].name == "frozen"

    async def test_driver_pause_run_action_pauses_with_reason(self, tmp_path):
        """驱动自暂停：批内 PauseRun 先受理，同批 Start 冻住；PAUSED 载荷带驱动原 reason。"""

        class SelfPauseDriver(RecordingDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                self.worlds.append(world)
                self._consume(notifications)
                if self.rounds == 1:
                    return [
                        PauseRun(reason="awaiting_answer"),
                        StartSession(spec=_spec("frozen"), context="a"),
                    ]
                return []

        driver = SelfPauseDriver()
        h = GroupHarness(tmp_path, FakeFactory("fake@1", driver))
        run_id = await h.start()
        rt = getattr(h.coord, "_runtimes")[run_id]

        # 受理即落 paused(user)：同批后续 Start 冻住不派发
        await h.eventually(lambda: h.status_is(run_id, "paused", "user"))
        await h.eventually(lambda: len(rt.deferred) == 1)
        assert h.so.started == []
        await h.eventually(lambda: any(c.kind == "paused" for c in driver.controls))
        paused = next(c for c in driver.controls if c.kind == "paused")
        assert paused.payload == {"reason": "awaiting_answer"}  # 不杀在途，不带 manual
        # 暂停期 run 不调 decide（冻结路，非轮询）
        rounds_at_pause = driver.rounds
        await asyncio.sleep(0.15)
        assert driver.rounds == rounds_at_pause
        # resume 解冻：被冻 Start 派发、decide 恢复
        await h.coord.resume_run(run_id)
        await h.eventually(lambda: len(h.so.started) == 1)
        assert h.so.started[0].name == "frozen"
        await h.eventually(lambda: driver.rounds > rounds_at_pause)
        await h.coord.stop_run(run_id)

    async def test_driver_pause_leaves_inflight_running(self, tmp_path):
        """驱动自暂停不杀在途：在途进程放行跑完、回执滞留 FIFO，恢复（带
        答案）后 RESUMED 透传、回执照常进 decide 结算。
        """

        class PauseWithProcessDriver(RecordingDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                self.worlds.append(world)
                self._consume(notifications)
                if self.rounds == 1:
                    return [
                        StartProcess(
                            cmd=f"sleep 0.2 && touch {tmp_path}/done.marker",
                            cwd=str(tmp_path),
                            context="bash#1",
                        ),
                        PauseRun(reason="awaiting_answer"),
                    ]
                return []

        driver = PauseWithProcessDriver()
        h = GroupHarness(tmp_path, FakeFactory("fake@1", driver))
        run_id = await h.start()
        await h.eventually(lambda: h.status_is(run_id, "paused", "user"))
        await h.eventually(lambda: any(c.kind == "paused" for c in driver.controls))
        paused = next(c for c in driver.controls if c.kind == "paused")
        assert paused.payload == {"reason": "awaiting_answer"}

        # 在途进程不被杀：注册表条目还在，进程自然跑完（marker 落盘）
        await h.eventually(lambda: (tmp_path / "done.marker").exists())
        procs = getattr(getattr(h.coord, "_host"), "_processes").get(run_id)
        assert procs is not None and "bash#1" in {e.context for e in procs.values()}
        # 暂停档冻结决策循环：进程已死、回执已入队，仍滞留不进 decide
        await asyncio.sleep(0.1)
        assert driver.exits == []

        # 恢复带答案：RESUMED 载荷透传；被冻的退出回执照常进 decide（rc=0）
        record = await h.coord.resume_run(run_id, payload={"answer": "B"})
        assert (record.status, record.pause_reason) == ("running", None)
        await h.eventually(lambda: any(c.kind == "resumed" for c in driver.controls))
        resumed = next(c for c in driver.controls if c.kind == "resumed")
        assert resumed.payload == {"answer": "B"}
        await h.eventually(lambda: driver.exits)
        [exit_note] = driver.exits
        assert exit_note.returncode == 0  # 没被杀（负码），自然跑完
        assert exit_note.exit_mark is None  # 非组杀
        await h.coord.stop_run(run_id)

    async def test_driver_pause_run_on_auth_gear_keeps_auth(self, tmp_path):
        """auth 档下驱动 PauseRun：不改判 user 档（资源挂起优先）、不记成员账
        （auth 恢复不经 resume_run，'run' 账无清账通路），resume_run 照旧拒绝。
        """

        class AuthPauseDriver(RecordingDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                self.worlds.append(world)
                self._consume(notifications)
                if self.rounds == 1:
                    return [StartSession(spec=_spec("n-slow"), context="a")]
                if any(c.kind == "steered" for c in self.controls):
                    return [PauseRun(reason="awaiting_external_fix")]
                return []

        h = GroupHarness(tmp_path, FakeFactory("fake@1", AuthPauseDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)

        await h.store.update_pause_state(run_id, "paused", "auth")  # 资源挂起落账
        await h.coord.steer_run(run_id, "gate")  # 唤醒决策轮发 PauseRun
        await h.eventually(lambda: any(c.kind == "paused" for c in driver.controls))
        paused = next(c for c in driver.controls if c.kind == "paused")
        assert paused.payload == {"reason": "awaiting_external_fix"}
        assert await h.status_is(run_id, "paused", "auth")  # 不改判 user 档
        assert (await h.store.list_run_sessions(run_id))[0].paused_by is None  # 不记成员账
        with pytest.raises(PauseReasonConflictError, match="auth 档挂起未恢复"):
            await h.coord.resume_run(run_id)  # 用户档恢复照旧拒绝
        assert await h.status_is(run_id, "paused", "auth")  # 拒绝后档位不丢
        await h.store.update_pause_state(run_id, "running", None)  # key 恢复落账
        await h.eventually(lambda: driver.rounds >= 3)  # 决策循环照常跑
        await h.coord.stop_run(run_id)

    async def test_pause_run_after_finish_in_batch_dropped(self, tmp_path):
        """同批 [FinishRun, PauseRun]：FinishRun 落终态即退出循环，PauseRun 静默丢弃。"""
        driver = RecordingDriver(
            [
                FinishRun(FinishOutcome.COMPLETED),
                PauseRun(reason="awaiting_answer"),
            ]
        )
        h = GroupHarness(tmp_path, FakeFactory("fake@1", driver))
        run_id = await h.start()

        assert await h.await_terminal(run_id) == "completed"
        assert not any(c.kind == "paused" for c in driver.controls)  # 从未受理
        assert h.so.paused == []

    async def test_pause_run_before_finish_in_batch_runs_sequence(self, tmp_path):
        """同批 [PauseRun, FinishRun]：先走完整受理序列（组暂停账落成员），再落终态。"""

        class PauseFinishDriver(RecordingDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                self.worlds.append(world)
                self._consume(notifications)
                if self.rounds == 1:
                    return [StartSession(spec=_spec("n-slow"), context="a")]
                if self.rounds == 2:
                    return [PauseRun(reason="awaiting_answer"), FinishRun(FinishOutcome.COMPLETED)]
                return []

        h = GroupHarness(tmp_path, FakeFactory("fake@1", PauseFinishDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)

        assert await h.await_terminal(run_id) == "completed"
        assert h.so.paused == []  # 不杀在途形态：成员不被组挂起
        assert (await h.store.list_run_sessions(run_id))[0].paused_by == "run"

    async def test_pause_run_dispatch_error_fails_run(self, tmp_path, monkeypatch):
        """pause 序列在 _dispatch 内抛异常：run 落 failed(run_loop_error)（决策循环异常口径）。"""
        h = GroupHarness(
            tmp_path, FakeFactory("fake@1", RecordingDriver([PauseRun(reason="awaiting_answer")]))
        )

        async def _boom(run_id, reason):
            raise RuntimeError("pause sequence blew up")

        monkeypatch.setattr(getattr(h.coord, "_group_control"), "driver_pause_run", _boom)
        run_id = await h.start()

        assert await h.await_terminal(run_id) == "failed"
        run = await h.store.get(run_id)
        assert run.error == "run_loop_error: RuntimeError: pause sequence blew up"
        assert h.so.paused == []  # 序列未走到成员暂停

    async def test_driver_pause_resume_clears_unsuspended_accounting(self, tmp_path):
        """驱动路组暂停账在恢复时全量清：未挂起成员（在跑/已终态）仅清账、不代 resume。"""

        class SelfPauseDriver(RecordingDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                self.worlds.append(world)
                self._consume(notifications)
                if self.rounds == 1:
                    return [StartSession(spec=_spec("n1"), context="a")]
                if self.rounds == 2:
                    return [PauseRun(reason="awaiting_answer")]
                return []

        h = GroupHarness(tmp_path, FakeFactory("fake@1", SelfPauseDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)
        await h.eventually(lambda: any(c.kind == "paused" for c in driver.controls))
        paused = next(c for c in driver.controls if c.kind == "paused")
        assert paused.payload == {"reason": "awaiting_answer"}  # 回执带驱动原 reason（非 manual）
        assert (await h.store.list_run_sessions(run_id))[0].paused_by == "run"

        await h.coord.resume_run(run_id)
        await h.eventually(lambda: any(c.kind == "resumed" for c in driver.controls))
        assert (await h.store.list_run_sessions(run_id))[0].paused_by is None  # 从未挂起也清账
        assert h.so.resumed == []  # 非 resumable（从未挂起）不代 resume
        assert h.so.paused == []  # 全程未挂起成员（不杀在途形态）
        await h.coord.stop_run(run_id)

    async def test_pause_resume_reject_implicit_run(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.store.mint_implicit(session_id="s-imp", role="interactive")
        with pytest.raises(ValueError, match="隐式"):
            await h.coord.pause_run(run_id)
        with pytest.raises(ValueError, match="隐式"):
            await h.coord.resume_run(run_id)


class TestPauseGearMatrix:
    """档位写者矩阵的组控受理语义。"""

    async def test_resume_rejects_auth_gear(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        await h.store.update_pause_state(run_id, "paused", "auth")
        with pytest.raises(PauseReasonConflictError, match="auth"):
            await h.coord.resume_run(run_id)
        run = await h.store.get(run_id)
        assert (run.status, run.pause_reason) == ("paused", "auth")  # 档位不丢

    async def test_finish_and_restart_clear_pause_reason(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", SimpleFinishDriver()))
        run_id = await h.start()
        await h.coord.pause_run(run_id)
        await h.coord.stop_run(run_id)
        assert (await h.store.get(run_id)).pause_reason is None  # 终态清档

        reopened = await h.coord.restart_run(run_id)
        assert reopened.status == "running" and reopened.pause_reason is None


# ── stop 与终态序列 ──────────────────────────────────────────────


class TestStopAndTerminal:
    async def test_reverse_race_finish_first_is_noop(self, tmp_path):
        """FinishRun 先落 completed，stop 后到：查账 no-op、账面终态不变。"""
        h = GroupHarness(tmp_path, FakeFactory("fake@1", SimpleFinishDriver()))
        run_id = await h.start()
        assert await h.await_terminal(run_id) == "completed"
        stopped_after_finish = list(h.so.stopped)  # 驱动收口扫掠的结果
        record = await h.coord.stop_run(run_id)
        assert record.status == "completed"  # 不改判
        assert h.so.stopped == stopped_after_finish  # 终态 run 不再扫掠

    async def test_stop_clears_deferred_and_sweeps_sessions(self, tmp_path):
        """终态收尾轮冻结在途出生：挂起队列清空，已派发成员停机。"""
        driver = RecordingDriver(
            [
                StartSession(spec=_spec("slow-a"), context="a"),
                StartSession(spec=_spec("slow-b"), context="b"),
            ]
        )
        h = GroupHarness(tmp_path, FakeFactory("fake@1", driver))
        run_id = await h.start("fake@1", max_parallel=1)
        rt = getattr(h.coord, "_runtimes")[run_id]
        await h.eventually(lambda: len(h.so.started) == 1 and len(rt.deferred) == 1)

        record = await h.coord.stop_run(run_id)
        assert record.status == "stopped"
        assert len(rt.deferred) == 0  # 冻结在途出生：旧挂起不再派发
        assert h.so.stopped == ["s1"]


# ── steer ────────────────────────────────────────────────────────


class TestSteer:
    async def test_workflow_steer_goes_through_notification(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)

        await h.coord.steer_run(run_id, "优先完成文档")
        await h.eventually(lambda: any(c.kind == "steered" for c in driver.controls))
        steered = next(c for c in driver.controls if c.kind == "steered")
        assert steered.payload == {"message": "优先完成文档"}
        assert h.so.injected == []  # 驱动决定转向谁，核心不盲播

    async def test_no_driver_steer_broadcasts_running_sessions(self, tmp_path):
        h = GroupHarness(tmp_path)
        run = await h.coord.create_run("ops-group")
        running = StubSession("m-live", "live", run_id=run.run_id)
        idle = StubSession(
            "m-rest",
            "rest",
            state="idle",
            loop_status="no",
            stop_reason="reviewer_complete",
            run_id=run.run_id,
        )
        h.so.pool.sessions.update({running.id: running, idle.id: idle})
        await h.affiliation.attach(running.id, run.run_id)
        await h.affiliation.attach(idle.id, run.run_id)

        await h.coord.steer_run(run.run_id, "快收尾")
        assert h.so.injected == [(running.id, "快收尾")]  # 只投 RUNNING 成员
        assert any(et == "run_steered" for _, et, _ in h.so.event_router.run_events)


# ── 准入门 ───────────────────────────────────────────────────────


class TestAdmission:
    async def test_max_parallel_fifo_with_world_consistency(self, tmp_path):
        driver = RecordingDriver(
            [StartSession(spec=_spec(f"fan{i}-slow"), context=f"slot={i}") for i in range(4)]
        )
        h = GroupHarness(tmp_path, FakeFactory("fake@1", driver))
        run_id = await h.start("fake@1", max_parallel=2)
        rt = getattr(h.coord, "_runtimes")[run_id]

        await h.eventually(lambda: len(h.so.started) == 2)  # 并发活成员只有 2
        await h.eventually(lambda: len(rt.deferred) == 2)
        assert rt.in_flight >= 2  # world 口径含挂起

        h.so.mark_terminal(h.session_by_name("fan0-slow"), "completed")
        await h.eventually(lambda: len(h.so.started) == 3)
        assert h.so.started[2].name == "fan2-slow"  # FIFO 序补放

        h.so.mark_terminal(h.session_by_name("fan1-slow"), "completed")
        await h.eventually(lambda: len(h.so.started) == 4)
        assert h.so.started[3].name == "fan3-slow"
        await h.coord.stop_run(run_id)

    async def test_restart_of_last_slot_session_not_deadlocked(self, tmp_path):
        """cap 被目标成员占满时的重启是槽位置换：不得永挂挂起队列。"""

        class RestartDriver(RecordingDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                self.worlds.append(world)
                self._consume(notifications)
                if self.rounds == 1:
                    return [StartSession(spec=_spec("slot-slow"), context="a")]
                if self.rounds == 2 and self.activated:
                    return [
                        RestartSession(
                            session_id=self.activated[0].session_id,
                            policy=RestartPolicy.FRESH,
                        )
                    ]
                return []

        h = GroupHarness(tmp_path, FakeFactory("fake@1", RestartDriver()))
        run_id = await h.start("fake@1", max_parallel=1)
        driver = h.factories["fake@1"].driver
        rt = getattr(h.coord, "_runtimes")[run_id]
        await h.eventually(lambda: len(driver.activated) >= 1)
        sid = driver.activated[0].session_id

        # 成员未终态且 cap=1：重启必须照常受理执行，不进永挂队列
        await h.eventually(lambda: h.so.restarted == [sid])
        assert len(rt.deferred) == 0
        await h.coord.stop_run(run_id)


# ── AwaitSessions ────────────────────────────────────────────────


class TestAwaitSessions:
    async def test_timeout_guarantees_session_stopped(self, tmp_path):
        class AwaitDriver(RecordingDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                self.worlds.append(world)
                self._consume(notifications)
                if self.rounds == 1:
                    return [StartSession(spec=_spec("slow"), context="a")]
                if self.rounds == 2:
                    return [AwaitSessions(ids=(self.activated[0].session_id,), timeout_s=0.2)]
                return []

        h = GroupHarness(tmp_path, FakeFactory("fake@1", AwaitDriver()))
        run_id = await h.start()
        await h.eventually(lambda: len(h.so.started) == 1)
        session = h.session_by_name("slow")
        await h.eventually(lambda: session.state == "stopped")  # 超时必停
        await h.coord.stop_run(run_id)


# ── 成员级操作（run 平面写者面）────────────────────────────────


class TestRunSessionOperations:
    async def test_workflow_resume_rejects_terminal_run_and_nonpaused_member(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)
        sid = driver.activated[0].session_id

        h.session_by_name("n-slow").state = "idle"
        h.session_by_name("n-slow").stop_reason = "error"
        with pytest.raises(MemberResumeDeniedError, match="不是可恢复的用户暂停成员"):
            await h.coord.resume_run_session(run_id, sid)
        assert sid not in h.so.resumed

        await h.coord.stop_run(run_id)
        with pytest.raises(MemberResumeDeniedError, match="重新启动"):
            await h.coord.resume_run_session(run_id, sid)
        assert sid not in h.so.resumed

    async def test_workflow_resume_rejects_group_paused_run(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)
        sid = driver.activated[0].session_id
        await h.coord.pause_run(run_id)

        with pytest.raises(MemberResumeDeniedError, match="已暂停"):
            await h.coord.resume_run_session(run_id, sid)
        assert sid not in h.so.resumed
        await h.coord.stop_run(run_id)

    async def test_pause_books_user_gear_and_resume_scopes(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)
        sid = driver.activated[0].session_id

        await h.coord.pause_run_session(run_id, sid)
        assert (await h.store.list_run_sessions(run_id))[0].paused_by == "user"
        await h.eventually(lambda: any(m.operation == "pause" for m in driver.session_controls))

        await h.coord.resume_run(run_id)  # 组 resume 不代劳用户暂停的成员
        assert sid not in h.so.resumed

        await h.coord.resume_run_session(run_id, sid)
        assert sid in h.so.resumed
        assert (await h.store.list_run_sessions(run_id))[0].paused_by is None

    async def test_inject_and_stop_notify_driver(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)
        sid = driver.activated[0].session_id

        await h.coord.inject_run_session(run_id, sid, "check docs")
        assert h.so.injected == [(sid, "check docs")]
        await h.eventually(lambda: any(m.operation == "inject" for m in driver.session_controls))

        await h.coord.stop_run_session(run_id, sid)
        await h.eventually(lambda: any(m.operation == "stop" for m in driver.session_controls))


# ── implicit 投影与 container 收尾 ──────────────────────────────


class TestRunStateProjection:
    async def test_implicit_projection_lifecycle(self, tmp_path):
        h = GroupHarness(tmp_path)
        session = StubSession("m-chat", "chat")
        h.so.pool.sessions[session.id] = session
        run_id = await h.affiliation.mint_for(
            session_id=session.id,
            role="interactive",
            label="chat",
        )
        session.run_id = run_id

        await h.bus.emit_state(session.id)
        await h.eventually(lambda: h.status_is(run_id, "running"))

        session.state = "idle"  # 成员休息 → paused(projection)
        await h.bus.emit_state(session.id)
        await h.eventually(lambda: h.status_is(run_id, "paused", "projection"))

        session.state = "running"  # 成员复活：投影随行回 running
        await h.bus.emit_state(session.id)
        await h.eventually(lambda: h.status_is(run_id, "running"))

    async def test_container_auto_close_when_all_terminal(self, tmp_path):
        h = GroupHarness(tmp_path)
        run = await h.coord.create_run("ops")
        live = StubSession("m-live", "live", run_id=run.run_id)
        done = StubSession(
            "m-done",
            "done",
            state="idle",
            loop_status="no",
            stop_reason="reviewer_complete",
            run_id=run.run_id,
        )
        h.so.pool.sessions.update({live.id: live, done.id: done})
        await h.affiliation.attach(live.id, run.run_id)
        await h.affiliation.attach(done.id, run.run_id)

        h.so.mark_terminal(live, "completed")  # 最后一个成员终态 → 收尾
        await h.eventually(lambda: h.status_is(run.run_id, "completed"))

    async def test_container_close_deferred_during_restart_window(self, tmp_path):
        """直通重启的 stop→重激活窗内终态为瞬态：收口短路，窗后复评照常收尾。"""
        h = GroupHarness(tmp_path)
        run = await h.coord.create_run("ops")
        only = StubSession("m-only", "only", run_id=run.run_id)
        h.so.pool.sessions[only.id] = only
        await h.affiliation.attach(only.id, run.run_id)

        h.so.restarting.add(only.id)  # session 平面重启窗开（stop 后、重激活前）
        h.so.mark_terminal(only, "completed")
        await asyncio.sleep(0.2)
        assert (await h.store.get(run.run_id)).status == "running"  # 瞬态终态不收口

        h.so.restarting.discard(only.id)  # 窗关（SO 补发状态事件 → 复评）
        await h.bus.emit_state(only.id)
        await h.eventually(lambda: h.status_is(run.run_id, "completed"))

    async def test_container_stop_leaves_no_stop_marker(self, tmp_path):
        h = GroupHarness(tmp_path)
        run = await h.coord.create_run("ops")
        live = StubSession("m-live", "live", run_id=run.run_id)
        h.so.pool.sessions[live.id] = live
        await h.affiliation.attach(live.id, run.run_id)

        record = await h.coord.stop_run(run.run_id)

        assert record.status == "stopped"
        # 受理标记只为 workflow 的 mint→spawn 窗口而设：container 不置位
        assert run.run_id not in getattr(h.coord, "_stop_marked")

    async def test_stop_marker_blocks_spawn_until_consumed(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        await h.coord.stop_run(run_id)
        assert run_id not in getattr(h.coord, "_stop_marked")  # run 任务退出即清

        # 窗口语义：标记在场时 _spawn 拒绝拉起并消费标记
        getattr(h.coord, "_stop_marked").add(run_id)
        getattr(h.coord, "_spawn")(run_id, "fake@1", h.factories["fake@1"].driver)
        assert run_id not in getattr(h.coord, "_tasks")
        assert run_id not in getattr(h.coord, "_runtimes")
        assert run_id not in getattr(h.coord, "_stop_marked")

    async def test_spawn_backfills_pause_state_from_ledger(self, tmp_path):
        """rt 缺席窗口受理的 pause 只落账面：拉起时回灌派生缓存，冻结谓词生效。"""
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        await h.coord.stop_run(run_id)

        # 模拟重开 restore 窗口内的用户暂停：账面 paused(user)、rt 缺席
        await h.store.reopen(run_id)
        await h.store.update_pause_state(run_id, "paused", "user")
        births_before = len(h.so.started)
        getattr(h.coord, "_spawn")(run_id, "fake@1", OneShotDriver())

        rt = getattr(h.coord, "_runtimes")[run_id]
        await h.eventually(lambda: getattr(h.coord, "_user_paused")(rt))
        await asyncio.sleep(0.1)
        assert len(h.so.started) == births_before  # 冻结谓词生效：不派发 Start

        await h.coord.resume_run(run_id)
        await h.eventually(lambda: len(h.so.started) == births_before + 1)  # 恢复后照常出生
        await h.coord.stop_run(run_id)


# ── restart_run / restart_run_session ────────────────────────────


async def _paused_restart_env(tmp_path) -> tuple[GroupHarness, str, str]:
    """restart 用例公共前置：OneShot 慢成员 run 落用户暂停断点。"""
    h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
    run_id, sid = await h.paused_session()
    return h, run_id, sid


class TestRestart:
    async def test_restart_run_restores_driver_from_snapshot(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", SimpleFinishDriver()))
        run_id = await h.start()
        assert await h.await_terminal(run_id) == "completed"

        record = await h.coord.restart_run(run_id)
        factory = h.factories["fake@1"]
        assert factory.restored == 1  # 快照恢复优先
        assert factory.created == 1  # 首次 create 一次
        assert record.status == "running"
        assert (await h.store.get(run_id)).reopened_at is not None

    async def test_restart_run_recreates_without_snapshot(self, tmp_path):
        import sqlite3

        h = GroupHarness(tmp_path, FakeFactory("fake@1", SimpleFinishDriver()))
        run_id = await h.start()
        await h.await_terminal(run_id)
        with sqlite3.connect(tmp_path / "events.db") as conn:
            conn.execute("DELETE FROM run_driver_state WHERE run_id = ?", (run_id,))
        h.store.close()

        record = await h.coord.restart_run(run_id)
        assert h.factories["fake@1"].created == 2  # 回退冻结程序 re-create
        assert record.status == "running"

    async def test_restart_session_fresh_with_overrides(self, tmp_path):
        h, run_id, sid = await _paused_restart_env(tmp_path)
        driver = h.factories["fake@1"].driver

        h.restart_member_fresh(
            run_id,
            sid,
            SessionSpec(name="n-slow", prompt="retry with report", cwd="/tmp"),
            context="attempt=2",
        )
        await h.eventually(lambda: len(driver.activated) >= 2)
        receipt = driver.activated[1]
        assert receipt.ok and receipt.session_id == sid
        assert receipt.context == "attempt=2"
        assert h.so.restarted == [sid]  # 停旧起新
        assert h.so.overridden[0][1]["initial_prompt"] == "retry with report"
        assert (await h.store.list_run_sessions(run_id))[0].context == "attempt=2"
        await h.coord.stop_run(run_id)

    async def test_restart_session_config_overrides_passthrough_and_strip(self, tmp_path):
        """config_overrides 与 Start 路径同款透传；宿主注入字段剔除，不污染归属。"""
        h, run_id, sid = await _paused_restart_env(tmp_path)

        h.restart_member_fresh(
            run_id,
            sid,
            SessionSpec(
                name="n-slow",
                prompt="retry",
                cwd="/tmp",
                config_overrides={
                    "run_id": "other-run",
                    "role": "rogue",
                    "criteria": {"task_goal": "g2"},
                },
            ),
        )
        await h.eventually(lambda: h.so.restarted == [sid])
        applied = h.so.overridden[0][1]
        assert applied["criteria"] == {"task_goal": "g2"}  # 普通载荷不静默丢弃
        # run_id/role 不入部分更新：session 沿用 Start 侧注入值
        assert "run_id" not in applied and "role" not in applied
        await h.coord.stop_run(run_id)

    async def test_restart_session_loop_injects_unattended_default(self, tmp_path):
        """restart 局部更新的 loop 载荷同款注入：question_detect=None 填 False。"""
        from workflow_controller.config.settings import LoopConfig

        h, run_id, sid = await _paused_restart_env(tmp_path)

        h.restart_member_fresh(
            run_id,
            sid,
            SessionSpec(
                name="n-slow",
                prompt="retry",
                cwd="/tmp",
                loop=LoopConfig(max_iterations=5),
            ),
        )
        await h.eventually(lambda: h.so.restarted == [sid])
        applied = h.so.overridden[0][1]
        assert applied["loop"].question_detect is False
        assert applied["loop"].max_iterations == 5  # 其余字段不动
        await h.coord.stop_run(run_id)

    async def test_restart_session_rejects_reserved_overrides(self, tmp_path):
        h, run_id, sid = await _paused_restart_env(tmp_path)

        with pytest.raises(NotImplementedError, match="reviewer"):
            h.restart_member_fresh(
                run_id,
                sid,
                SessionSpec(
                    name="n-slow",
                    prompt="retry",
                    cwd="/tmp",
                    config_overrides={"reviewer": {"enabled": True}},
                ),
            )
        assert h.so.restarted == []  # 派发口拒绝，不进重启任务
        await h.coord.stop_run(run_id)

    async def test_restart_session_spec_default_vs_explicit_fields(self, tmp_path):
        """非 None 默认值字段按值判定未提供：最小 spec 不清 agent 绑定、不重置权限档；显式异值照常覆盖。"""
        h, run_id, sid = await _paused_restart_env(tmp_path)

        h.restart_member_fresh(
            run_id,
            sid,
            SessionSpec(name="n-slow", prompt="retry", cwd="/tmp"),
        )
        await h.eventually(lambda: h.so.restarted == [sid])
        minimal = h.so.overridden[0][1]
        assert "agent_name" not in minimal and "permission_mode" not in minimal

        getattr(h.coord, "_host").restart_run_session(
            run_id,
            sid,
            RestartPolicy.FRESH,
            spec_override=SessionSpec(
                name="n-slow",
                prompt="retry",
                cwd="/tmp",
                agent_name="coder",
                permission_mode="default",
            ),
        )
        await h.eventually(lambda: h.so.restarted == [sid, sid])
        applied = h.so.overridden[1][1]
        assert applied["agent_name"] == "coder"
        assert applied["permission_mode"] == "default"
        await h.coord.stop_run(run_id)

    async def test_restart_session_resume_if_paused_reuses(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id, sid = await h.paused_session()

        getattr(h.coord, "_host").restart_run_session(run_id, sid, RestartPolicy.RESUME_IF_PAUSED)
        await h.eventually(lambda: h.so.resumed == [sid])  # 复用续跑，不重置
        assert h.so.restarted == []
        await h.coord.stop_run(run_id)

    async def test_run_member_send_injects_and_resumes_same_member(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id, sid = await h.paused_session()
        assert await h.coord.send_run_session(run_id, sid, "continue") is True
        assert h.so.injected == [(sid, "continue")]
        assert h.so.resumed == [sid]
        await h.coord.stop_run(run_id)

    async def test_resume_applies_model_before_reusing_member(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id, sid = await h.paused_session()
        getattr(h.coord, "_host").restart_run_session(
            run_id,
            sid,
            RestartPolicy.RESUME_IF_PAUSED,
            spec_override=SessionSpec(
                name="graph:w",
                prompt="",
                cwd="/tmp",
                model="new-provider/new-model",
                provider_index=2,
                config_overrides={"model_pinned": True},
            ),
        )
        await h.eventually(lambda: h.so.resumed == [sid])
        assert h.so.overridden == [
            (
                sid,
                {
                    "model": "new-provider/new-model",
                    "provider_index": 2,
                    "model_pinned": True,
                },
            )
        ]
        assert h.so.restarted == []
        await h.coord.stop_run(run_id)

    async def test_restart_session_absent_target_rejected(self, tmp_path):
        """目标 session 不在池内（如被直接删除）：SO 原语显式失败，回执不空转假 ok。"""
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)
        sid = driver.activated[0].session_id
        h.so.pool.sessions.pop(sid)  # 成员 session 被删（归属行保留）

        getattr(h.coord, "_host").restart_run_session(run_id, sid, RestartPolicy.FRESH)
        await h.eventually(lambda: len(driver.activated) >= 2)
        receipt = driver.activated[1]
        assert not receipt.ok and receipt.session_id == sid
        assert "不在池内" in (receipt.error or "")
        assert h.so.restarted == []  # 未触重启原语
        await h.coord.stop_run(run_id)


# ── SessionAPI 重定向 ───────────────────────────────────────────


class TestWorkflowSessionRedirect:
    async def test_workflow_session_redirected_implicit_passthrough(self, tmp_path):
        h = GroupHarness(tmp_path, FakeFactory("fake@1", OneShotDriver()))
        run_id = await h.start()
        driver = h.factories["fake@1"].driver
        await h.eventually(lambda: len(driver.activated) >= 1)
        wf_sid = driver.activated[0].session_id

        solo = StubSession("m-solo", "solo")
        h.so.pool.sessions[solo.id] = solo
        implicit_run = await h.affiliation.mint_for(
            session_id=solo.id,
            role="interactive",
            label="solo",
        )
        solo.run_id = implicit_run

        api = SessionAPI(h.so)
        with pytest.raises(WorkflowSessionOperationError, match=run_id):
            await api.pause(wf_sid)
        assert wf_sid not in h.so.paused  # 直通被拒，未触 SO

        await api.pause(solo.id)  # 隐式 run 成员零改动直通
        assert h.so.paused == [solo.id]
        assert implicit_run
