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

"""RunCoordinator 决策循环：FakeDriver 全链路、守护、应答通道与停止语义。"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from types import SimpleNamespace
from typing import ClassVar

import pytest

from workflow_controller.core.run_coordinator import LoopState, RunHost, next_loop_state
from workflow_controller.core.runs import RunStatus, RunStore
from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService
from workflow_controller.plugin.run_driver import (
    AllSessionsTerminal,
    CancelTimer,
    ControlNotification,
    EmitEvent,
    FinishOutcome,
    FinishRun,
    PersistDriverState,
    RestartPolicy,
    SessionActivated,
    SessionOutcomeNotification,
    SessionSpec,
    StartSession,
)
from workflow_controller.session.events import SessionOutcome

from ._run_plane_stubs import (
    PoolFactsMixin,
    StubBus,
    StubPool,
    StubSession,
    member_outcome_payload,
)

# ── 测试替身 ──────────────────────────────────────────────────────


class FakeSO(PoolFactsMixin):
    """SO 替身：激活即归属落账；名含 fail 的成员自动落 failed，slow 不落，
    名含 nobirth 的成员激活即抛（出生失败回执路径）。
    """

    def __init__(self, affiliation, bus, project_dir) -> None:
        self.pool = StubPool()
        self.affiliation = affiliation
        self.bus = bus
        self.default_project_dir = project_dir
        self.started: list = []
        self.stopped: list[str] = []

    async def start_session_with_config(self, cfg):
        if "nobirth" in cfg.name:
            raise RuntimeError(f"birth boom: {cfg.name}")
        s = StubSession(f"s{len(self.started) + 1}", cfg.name)
        self.pool.sessions[s.id] = s
        self.started.append(cfg)
        await self.affiliation.attach(s.id, cfg.run_id, role=cfg.role)
        if "slow" not in cfg.name:
            asyncio.get_running_loop().create_task(self._conclude(s))
        return s

    async def stop_session(self, sid: str) -> None:
        self.stopped.append(sid)

    def is_pausable(self, sid) -> bool:
        facts = self.get_session_facts(sid)
        return facts is not None and facts.state == "running"

    async def pause_session(self, sid: str) -> None:
        s = self.pool.sessions[sid]
        s.state, s.loop_status, s.stop_reason = "idle", "no", "user_pause"

    async def ensure_reviewer_snapshot(self, sid: str) -> None:
        """reviewer 快照重建接口的测试替身（无 reviewer 概念，空实现）。"""

    async def _conclude(self, s: StubSession) -> None:
        # 等激活回执先入队（真路径中回执在激活缝即发、结局随运行时推进）
        await asyncio.sleep(0.05)
        kind = "failed" if "fail" in s.name else "completed"
        # 真实 session 终态停点：state 回 IDLE，loop_status 落 no
        s.state = "idle"
        s.loop_status = "no"
        s.stop_reason = "error" if kind == "failed" else "reviewer_complete"
        await self.bus.emit_outcome(s.id, member_outcome_payload(s.id, kind, s.stop_reason))


def _spec(name: str) -> SessionSpec:
    return SessionSpec(name=name, prompt="p", cwd="/tmp")


class FakeDriver:
    """两成员一成一败的玩具驱动。"""

    def __init__(self, sleep_s: float = 0.0) -> None:
        self.sleep_s = sleep_s
        self.activated: list[SessionActivated] = []
        self.outcomes: dict[str, SessionOutcome] = {}
        self.controls: list[ControlNotification] = []
        self.rounds = 0

    @staticmethod
    def handle_op(op, args):
        return {"echo": op, "args": dict(args)}

    def decide(self, world, notifications):
        self.rounds += 1
        if self.sleep_s:
            time.sleep(self.sleep_s)
        for n in notifications:
            if isinstance(n, SessionActivated) and n.ok:
                self.activated.append(n)
            elif isinstance(n, SessionOutcomeNotification):
                self.outcomes[n.outcome.session_id] = n.outcome
        if self.activated and self.controls:
            return [FinishRun(FinishOutcome.COMPLETED)]  # 停止收尾轮
        if len(self.outcomes) >= 2:
            ok = all(o.kind == "completed" for o in self.outcomes.values())
            return [FinishRun(FinishOutcome.COMPLETED if ok else FinishOutcome.FAILED)]
        if not self.activated and not self.outcomes and world.in_flight == 0:
            return [
                StartSession(spec=_spec("n1"), context="a"),
                StartSession(spec=_spec("n2-fail"), context="b"),
            ]
        return []

    def on_control(self, control) -> None:
        self.controls.append(control)

    def projection(self):
        return {"schema_version": 1, "rounds": self.rounds}

    def snapshot(self):
        return {"schema_version": 1, "state": {"rounds": self.rounds}, "pending_timers": []}


class FakeFactory:
    def __init__(self, fmt: str, driver: FakeDriver | None = None) -> None:
        self.format = fmt
        self.driver = driver or FakeDriver()
        self.created_with: list = []

    def create(self, run, program_path, params):
        self.created_with.append((run.run_id, program_path.name, dict(params)))
        return self.driver

    def restore(self, run, snapshot, sessions):
        return self.driver


class Harness:
    def __init__(self, tmp_path, *factories: FakeFactory, decide_timeout_s: float = 60.0):
        self.store = RunStore(tmp_path / "events.db")
        self.affiliation = SessionRunAffiliationService(self.store)
        self.bus = StubBus()
        self.so = FakeSO(self.affiliation, self.bus, tmp_path)
        self.factories = {f.format: f for f in factories}
        from workflow_controller.core.run_coordinator import RunCoordinator

        self.coord = RunCoordinator(
            orchestrator=self.so,
            run_store=self.store,
            bus=self.bus,
            driver_factories=dict(self.factories),
            affiliation=self.affiliation,
            decide_timeout_s=decide_timeout_s,
        )
        self.program = tmp_path / "prog.txt"
        self.program.write_text("format: fake", encoding="utf-8")

    async def start(self, fmt: str = "fake@1") -> str:
        from workflow_controller.core.run_coordinator import RunConfig

        run = await self.coord.start_run(RunConfig(format=fmt), self.program)
        return run.run_id

    async def await_terminal(self, run_id: str, timeout_s: float = 5.0) -> str:
        async def _poll() -> str:
            while True:
                run = await self.store.get(run_id)
                if run.finished_at is not None:
                    return run.status
                await asyncio.sleep(0.01)

        return await asyncio.wait_for(_poll(), timeout_s)


# ── 循环状态迁移矩阵 ──────────────────────────────────────────────


def _expected_loop_target(state: LoopState, user_paused: bool, terminal: bool) -> LoopState:
    """期望矩阵的推导式写法：终态不可逆，终态控制优先于用户暂停。"""
    if state in (LoopState.DRAINING, LoopState.EXITED):
        return LoopState.EXITED
    if terminal:
        return LoopState.DRAINING
    return LoopState.PAUSED if user_paused else LoopState.RUNNING


# (user_paused, terminal) 全组合，按原 user_paused×terminal 双层循环顺序展开
_LOOP_INPUTS: tuple[tuple[bool, bool], ...] = (
    (False, False),
    (False, True),
    (True, False),
    (True, True),
)


class TestLoopTransitionMatrix:
    EXPECTED: ClassVar[dict] = {
        (state, user_paused, terminal): _expected_loop_target(state, user_paused, terminal)
        for state in LoopState
        for user_paused, terminal in _LOOP_INPUTS
    }

    def test_exhaustive(self):
        """状态×输入全覆盖：终态控制优先于暂停；DRAINING/EXITED 不可逆。"""
        seen = set()
        for state in LoopState:
            for user_paused in (False, True):
                for terminal in (False, True):
                    key = (state, user_paused, terminal)
                    got = next_loop_state(state, user_paused=user_paused, terminal=terminal)
                    assert got == self.EXPECTED[key], key
                    seen.add(key)
        assert len(seen) == len(LoopState) * 4


# ── 全链路 ────────────────────────────────────────────────────────


class TestFakeDriverChain:
    async def test_driver_checkpoint_commits_before_decide_returns(self, tmp_path):
        class CheckpointDriver(FakeDriver):
            def bind_checkpoint(self, emit):
                self.emit = emit

            def decide(self, world, notifications):
                blob = {"schema_version": 1, "marker": "inside_decide"}
                self.emit(PersistDriverState(blob=blob))
                self.verified = h.store.load_driver_state_sync(world.run.run_id) == blob
                return [FinishRun(FinishOutcome.COMPLETED)]

        driver = CheckpointDriver()
        h = Harness(tmp_path, FakeFactory("fake@1", driver))
        run_id = await h.start()
        assert await h.await_terminal(run_id) == "completed"
        assert driver.verified
        task = getattr(h.coord, "_tasks").get(run_id)
        if task is not None:
            await asyncio.wait_for(task, 1.0)
        settled = await h.store.load_driver_state(run_id)
        await asyncio.to_thread(
            driver.emit,
            PersistDriverState(blob={"marker": "late"}),
        )
        assert await h.store.load_driver_state(run_id) == settled

    async def test_two_sessions_one_fails_run_fails(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("fake@1"))
        run_id = await h.start()
        status = await h.await_terminal(run_id)
        assert status == "failed"
        driver = h.factories["fake@1"].driver
        assert len(driver.activated) == 2
        assert {o.kind for o in driver.outcomes.values()} == {"completed", "failed"}
        sessions = await h.coord.list_run_sessions(run_id)
        assert len(sessions) == 2
        assert {m.context for m in sessions} == {"a", "b"}

    async def test_program_frozen_into_run_root(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("fake@1"))
        run_id = await h.start()
        frozen = h.coord.run_data_root(run_id) / "program" / "prog.txt"
        assert frozen.read_text(encoding="utf-8") == "format: fake"
        factory = h.factories["fake@1"]
        assert factory.created_with[0][1] == "prog.txt"

    async def test_boundary_snapshots_latest_and_history(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("fake@1"))
        run_id = await h.start()
        await h.await_terminal(run_id)
        blob = await h.store.load_driver_state(run_id)
        assert blob["schema_version"] == 1 and "pending_timers" in blob
        history = await h.store.list_driver_snapshots(run_id)
        assert len(history) >= 2  # 批边界 + 收尾前
        assert [s["seq"] for s in history] == list(range(1, len(history) + 1))
        assert history[-1]["blob"] == blob

    async def test_unregistered_format_rejected_before_mint(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("fake@1"))
        from workflow_controller.core.run_coordinator import RunConfig

        with pytest.raises(ValueError, match="没有驱动工厂"):
            await h.coord.start_run(RunConfig(format="ghost@1"), h.program)
        assert await h.store.list_runs() == []  # 无孤儿行

    async def test_factory_failure_finishes_pending(self, tmp_path):
        class BoomFactory(FakeFactory):
            def create(self, run, program_path, params):
                raise RuntimeError("翻译失败")

        h = Harness(tmp_path, BoomFactory("boom@1"))
        from workflow_controller.core.run_coordinator import RunConfig

        with pytest.raises(RuntimeError):
            await h.coord.start_run(RunConfig(format="boom@1"), h.program)
        run = (await h.store.list_runs())[0]
        assert run.status == "failed" and "crash_during_start" in run.error

    async def test_start_run_persists_program_ref_and_labels(self, tmp_path):
        from workflow_controller.core.run_coordinator import RunConfig

        h = Harness(tmp_path, FakeFactory("fake@1"))
        run = await h.coord.start_run(
            RunConfig(
                format="fake@1",
                labels={"name": "layr", "team": "ops"},
                program_ref="programs/main.yaml@deadbeef",
            ),
            h.program,
        )
        assert run.program_ref == "programs/main.yaml@deadbeef"
        assert run.labels == {"name": "layr", "team": "ops"}


# ── boot 恢复面 ──────────────────────────────────────────────────


class TestBootRecovery:
    async def test_has_runs_table_probe_is_readonly(self, tmp_path):
        # 只读探测：无库文件/无 runs 表判 False 且不建库不初始化 schema；
        # 有过 run（runs 表已建）判 True
        store = RunStore(tmp_path / "events.db")
        assert not await store.has_runs_table()
        assert not (tmp_path / "events.db").exists()

        plain = tmp_path / "plain.db"
        with sqlite3.connect(plain) as conn:
            conn.execute("CREATE TABLE t (x)")
        assert not await RunStore(plain).has_runs_table()
        with sqlite3.connect(plain) as conn:
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        assert tables == {"t"}  # 探测未初始化 runs 族 schema

        await store.create("p", "fake@1")
        assert await store.has_runs_table()

    async def test_per_run_failure_logged_and_isolated(
        self,
        tmp_path,
        monkeypatch,
    ):
        # 单 run 恢复的意外异常（如 pending 判崩溃的 finish 落账失败）：
        # 告警留痕（exc_info）且不阻断其余 run 恢复
        h = Harness(tmp_path, FakeFactory("fake@1"))
        broken = await h.store.create("p", "fake@1", status=RunStatus.PENDING)
        healthy = await h.store.create("p", "fake@1", status=RunStatus.PENDING)
        real_finish = h.store.finish

        async def _finish(run_id, status, *, error=None):
            if run_id == broken.run_id:
                raise sqlite3.OperationalError("disk i/o error")
            return await real_finish(run_id, status, error=error)

        monkeypatch.setattr(h.store, "finish", _finish)
        # 直挂目标 logger 收记录：套件里其他用例的日志装配可能关传播，
        # 经 root 的 caplog 不可靠
        target = logging.getLogger("workflow_controller.core.run_coordinator")
        collected: list[logging.LogRecord] = []

        class _Collect(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                collected.append(record)

        collector = _Collect(level=logging.WARNING)
        prev_level, prev_propagate = target.level, target.propagate
        target.addHandler(collector)
        target.setLevel(logging.WARNING)
        target.propagate = True
        try:
            await h.coord.recover_at_boot()
        finally:
            target.removeHandler(collector)
            target.setLevel(prev_level)
            target.propagate = prev_propagate

        assert (await h.store.get(broken.run_id)).status == RunStatus.PENDING
        recovered = await h.store.get(healthy.run_id)
        assert recovered.status == RunStatus.FAILED
        assert recovered.error == "crash_during_start"
        assert any(
            broken.run_id in r.getMessage() and r.exc_info
            for r in collected
            if r.levelno == logging.WARNING
        )


# ── 程序冻结面 ────────────────────────────────────────────────────


class TestProgramFreezeFace:
    """多文件程序与组件面进冻结面；入口按清单取（不按排序）。"""

    async def test_freeze_copies_program_tree_and_scan_face(self, tmp_path):
        proj = tmp_path / "proj"
        (proj / "myprog").mkdir(parents=True)
        (proj / "myprog" / "program.yaml").write_text("format: fake", encoding="utf-8")
        (proj / "myprog" / "SKILL.md").write_text("hello", encoding="utf-8")
        (proj / "layr.yaml").write_text(
            "scan:\n  - { name: examples, path: ./components }\n", encoding="utf-8"
        )
        comp = proj / "components" / "agent" / "tech-writer" / "1.0.0.md"
        comp.parent.mkdir(parents=True)
        comp.write_text(
            "---\ntype: agent\nname: tech-writer\nversion: 1.0.0\n---\n", encoding="utf-8"
        )

        h = Harness(tmp_path, FakeFactory("fake@1"))
        h.program = proj / "myprog" / "program.yaml"
        run_id = await h.start()

        frozen = h.coord.run_data_root(run_id) / "program"
        assert (frozen / "program.yaml").read_text(encoding="utf-8") == "format: fake"
        assert (frozen / "SKILL.md").read_text(encoding="utf-8") == "hello"
        assert (frozen / "components" / "agent" / "tech-writer" / "1.0.0.md").is_file()
        import yaml as _yaml

        cfg = _yaml.safe_load((frozen / "layr.yaml").read_text(encoding="utf-8"))
        assert cfg["scan"][0]["path"] == "./components"
        assert cfg["scan"][0]["name"] == "examples"
        # 入口清单与恢复路定位
        entry_marker = (frozen / ".workflow-controller-entry").read_text(encoding="utf-8").strip()
        assert entry_marker == "program.yaml"
        assert getattr(h.coord, "_frozen_program")(run_id).name == "program.yaml"

    async def test_frozen_program_picks_entry_over_sorted_first_file(self, tmp_path):
        progdir = tmp_path / "sortable"
        progdir.mkdir()
        # README.md 码点在 program.yaml 之前——排序取首文件会错选
        (progdir / "README.md").write_text("doc", encoding="utf-8")
        (progdir / "program.yaml").write_text("format: fake", encoding="utf-8")

        h = Harness(tmp_path, FakeFactory("fake@1"))
        h.program = progdir / "program.yaml"
        run_id = await h.start()
        assert getattr(h.coord, "_frozen_program")(run_id).name == "program.yaml"
        assert not (h.coord.run_data_root(run_id) / "program" / "manifest.json").exists()

    async def test_freeze_without_config_copies_no_yaml(self, tmp_path):
        progdir = tmp_path / "lonely"
        progdir.mkdir()
        (progdir / "program.yaml").write_text("format: fake", encoding="utf-8")

        h = Harness(tmp_path, FakeFactory("fake@1"))
        h.program = progdir / "program.yaml"
        run_id = await h.start()
        frozen = h.coord.run_data_root(run_id) / "program"
        assert not (frozen / "layr.yaml").exists()
        assert getattr(h.coord, "_frozen_program")(run_id).name == "program.yaml"

    async def test_freeze_vendor_face_from_manifest(self, tmp_path):
        import hashlib

        root = tmp_path / "registry"
        vdir = root / "programs" / "demo" / "1.0.0"
        vdir.mkdir(parents=True)
        (vdir / "program.yaml").write_text("format: fake", encoding="utf-8")
        comp = root / "components" / "agent" / "tech-writer" / "1.0.0.md"
        comp.parent.mkdir(parents=True)
        content = "---\ntype: agent\nname: tech-writer\nversion: 1.0.0\n---\n"
        comp.write_text(content, encoding="utf-8")
        manifest = {
            "program": "demo@1.0.0",
            "format": "fake",
            "resolved": [
                {
                    "ref": "agent/local:tech-writer@1.0",
                    "resolved_version": "1.0.0",
                    "source": "examples",
                    "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                }
            ],
        }
        (vdir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        h = Harness(tmp_path, FakeFactory("fake@1"))
        h.program = vdir / "program.yaml"
        run_id = await h.start()

        frozen = h.coord.run_data_root(run_id) / "program"
        # manifest.json 不进冻结面；vendor 组件与指过去的 layr.yaml 在
        assert not (frozen / "manifest.json").exists()
        frozen_comp = frozen / "components" / "agent" / "tech-writer" / "1.0.0.md"
        assert frozen_comp.read_text(encoding="utf-8") == content
        import yaml as _yaml

        cfg = _yaml.safe_load((frozen / "layr.yaml").read_text(encoding="utf-8"))
        assert cfg["scan"][0]["path"] == "./components"
        assert getattr(h.coord, "_frozen_program")(run_id).name == "program.yaml"

    async def test_freeze_vendor_sha_mismatch_rejects(self, tmp_path):
        root = tmp_path / "registry"
        vdir = root / "programs" / "demo" / "1.0.0"
        vdir.mkdir(parents=True)
        (vdir / "program.yaml").write_text("format: fake", encoding="utf-8")
        comp = root / "components" / "agent" / "tech-writer" / "1.0.0.md"
        comp.parent.mkdir(parents=True)
        comp.write_text("drifted", encoding="utf-8")
        manifest = {
            "program": "demo@1.0.0",
            "format": "fake",
            "resolved": [
                {
                    "ref": "agent/tech-writer@1.0",
                    "resolved_version": "1.0.0",
                    "source": "x",
                    "sha256": "0" * 64,
                }
            ],
        }
        (vdir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        h = Harness(tmp_path, FakeFactory("fake@1"))
        h.program = vdir / "program.yaml"
        from workflow_controller.core.run_coordinator import RunConfig

        with pytest.raises(ValueError, match="sha256 不符"):
            await h.coord.start_run(RunConfig(format="fake@1"), h.program)
        run = (await h.store.list_runs())[0]
        assert run.status == "failed" and "crash_during_start" in run.error


# ── 守护：超时/中毒/分片 ──────────────────────────────────────────


class TestDriverGuards:
    async def test_decide_timeout_fails_run_and_poisons(self, tmp_path):
        h = Harness(
            tmp_path,
            FakeFactory("slow@1", FakeDriver(sleep_s=1.0)),
            decide_timeout_s=0.05,
        )
        run_id = await h.start("slow@1")
        status = await h.await_terminal(run_id)
        assert status == "failed"
        run = await h.store.get(run_id)
        assert "driver_decide_timeout" in run.error
        assert "slow@1" in run.error and "第 1 轮" in run.error
        driver = h.factories["slow@1"].driver
        assert driver.rounds == 1  # 中毒后不再被调用
        with pytest.raises(RuntimeError, match="无可用驱动运行时"):
            await h.coord.driver_projection(run_id)

    async def test_decide_pool_sharded_by_format(self, tmp_path):
        h = Harness(
            tmp_path,
            FakeFactory("slow@1", FakeDriver(sleep_s=0.4)),
            FakeFactory("fast@1", FakeDriver()),
        )
        assert getattr(h.coord, "_decide_pool")("slow@1") is not getattr(h.coord, "_decide_pool")(
            "fast@1"
        )
        slow_a = await h.start("slow@1")  # 占满 slow 分片的两轮长决策
        slow_b = await h.start("slow@1")
        fast = await h.start("fast@1")
        assert await h.await_terminal(fast, timeout_s=2.0) == "failed"
        slow_run = await h.store.get(slow_a)
        assert slow_run.finished_at is None  # fast 不被 slow 拖累
        await h.await_terminal(slow_b, timeout_s=10.0)

    async def test_unsupported_action_fails_run(self, tmp_path):
        from dataclasses import dataclass

        from workflow_controller.plugin.run_driver import RunAction

        @dataclass(frozen=True)
        class OddAction(RunAction):
            """协调器未实现的驱动动作。"""

        class OddDriver(FakeDriver):
            def decide(self, world, notifications):
                if self.rounds == 0:
                    self.rounds += 1
                    return [OddAction()]
                return super().decide(world, notifications)

        h = Harness(tmp_path, FakeFactory("odd@1", OddDriver()))
        run_id = await h.start("odd@1")
        status = await h.await_terminal(run_id)
        run = await h.store.get(run_id)
        assert status == "failed" and "OddAction" in run.error


# ── 应答通道与投影 ────────────────────────────────────────────────


class TestOpChannel:
    async def test_handle_op_runs_in_run_task(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("fake@1"))
        run_id = await h.start()
        result = await h.coord.request_driver_op(run_id, "ping", {"x": 1}, timeout_s=5.0)
        assert result == {"echo": "ping", "args": {"x": 1}}

    async def test_projection_via_reply_channel(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("fake@1"))
        run_id = await h.start()
        projection = await h.coord.driver_projection(run_id, timeout_s=5.0)
        assert projection["schema_version"] == 1

    async def test_projection_served_while_user_paused(self, tmp_path):
        """暂停期应答通道不冻结：/state 投影照常经 run 任务取回。

        冻结面是 decide；OpRequest 若随成员事件一并滞留 FIFO，暂停期 /state
        会恒超时降级读盘上快照（非投影形状），前端呈「驱动投影不可用」。
        """

        class SlowRunDriver(FakeDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                for n in notifications:
                    if isinstance(n, SessionActivated) and n.ok:
                        self.activated.append(n)
                if not self.activated and world.in_flight == 0:
                    return [StartSession(spec=_spec("n1-slow"), context="a")]
                return []

        h = Harness(tmp_path, FakeFactory("pauseproj@1", SlowRunDriver()))
        run_id = await h.start("pauseproj@1")
        for _ in range(300):
            if h.so.started:
                break
            await asyncio.sleep(0.01)
        assert h.so.started, "slow 成员应已激活"

        run = await h.coord.pause_run(run_id)
        assert run.status == "paused"

        projection = await h.coord.driver_projection(run_id, timeout_s=5.0)
        assert projection["schema_version"] == 1

    async def test_world_op_events_dispatch_cancel_timer_not_birth(self, monkeypatch, tmp_path):
        """读路径对账的事件动作：定时器回收照派发，出生类动作丢弃。"""

        class WorldOpDriver(FakeDriver):
            @staticmethod
            def handle_world_op(world, op, args):
                return (
                    {"ok": True},
                    True,
                    [
                        CancelTimer(timer_id="t1"),
                        EmitEvent(event_type="projection_updated", data={}),
                        StartSession(spec=_spec("sneaky"), context="c"),
                    ],
                )

            def decide(self, world, notifications):
                self.rounds += 1
                return []

        h = Harness(tmp_path, FakeFactory("world@1", WorldOpDriver()))
        run_id = await h.start("world@1")
        getattr(h.coord, "_host").arm_timer(run_id, "t1", 60.0)
        emitted: list[str] = []

        async def record_event(run_id, event_type, data, context=None):
            emitted.append(event_type)

        # FakeSO 无事件库门面：替身只记录派发达
        def _run_event_store():
            return SimpleNamespace(store_run_event=record_event)

        monkeypatch.setattr(getattr(h.coord, "_host"), "_run_event_store", _run_event_store)

        result = await h.coord.request_driver_op(run_id, "reconcile", timeout_s=5.0)

        assert result == {"ok": True}
        assert "t1" not in getattr(getattr(h.coord, "_host"), "_timers").get(
            run_id, {}
        )  # CancelTimer 已派发
        assert emitted == ["projection_updated"]  # 事件照派发
        assert h.so.started == []  # 读路径不出生成员
        # 驱动 run 键事件同流总线分发（只分发不落库）：watchdog 等订阅者的事实源
        assert len(h.bus.ephemeral) == 1
        relayed = h.bus.ephemeral[0]
        assert relayed.event_type == "projection_updated"
        assert relayed.run_id == run_id
        assert relayed.session_id == ""

    async def test_late_op_settle_leaves_trace(self, tmp_path, caplog, monkeypatch):
        """回执晚于调用方等待上界：留痕「结果未送达」，不得静默丢弃。"""

        class SlowOpDriver(FakeDriver):
            @staticmethod
            def handle_op(op, args):
                time.sleep(0.3)
                return {"echo": op}

        # configure_logging 会把 workflow_controller logger 的 propagate 关掉（caplog 在
        # root 捕获，同进程内先跑过的日志配置用例会污染本断言）
        monkeypatch.setattr(logging.getLogger("workflow_controller"), "propagate", True)
        h = Harness(tmp_path, FakeFactory("fake@1", SlowOpDriver()))
        run_id = await h.start()
        with caplog.at_level(logging.WARNING, logger="workflow_controller.core.run_coordinator"):
            with pytest.raises(TimeoutError):
                await h.coord.request_driver_op(run_id, "reload", timeout_s=0.05)
            deadline = time.monotonic() + 2.0
            while not any("结果未送达" in r.getMessage() for r in caplog.records):
                if time.monotonic() > deadline:
                    pytest.fail("迟达应答未留痕：结果被静默丢弃")
                await asyncio.sleep(0.02)


# ── 重启：池缺席成员的恢复读 ──────────────────────────────────────


class TestRestartAbsentMember:
    async def test_resume_restore_of_absent_member_runs_off_loop(self, tmp_path, monkeypatch):
        """池缺席分支的恢复读走线程池：同步 SQLite 读不上事件循环（评审修订）。"""

        class QuietDriver(FakeDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                return []

        h = Harness(tmp_path, FakeFactory("quiet@1", QuietDriver()))
        run_id = await h.start("quiet@1")
        probe: dict[str, bool | None] = {"on_loop": None}

        def fake_restore(sid: str) -> bool:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                probe["on_loop"] = False
                return True
            probe["on_loop"] = True
            return True

        monkeypatch.setattr(h.so, "restore_session_from_store", fake_restore, raising=False)
        getattr(h.coord, "_host").restart_run_session(
            run_id,
            "gone",
            RestartPolicy.RESUME_IF_PAUSED,
        )

        deadline = time.monotonic() + 2.0
        while probe["on_loop"] is None:
            if time.monotonic() > deadline:
                pytest.fail("恢复读未被调用")
            await asyncio.sleep(0.01)
        assert probe["on_loop"] is False


# ── 停止语义 ──────────────────────────────────────────────────────


class TestStopRun:
    async def test_stop_terminal_priority_and_session_sweep(self, tmp_path):
        class SlowStartDriver(FakeDriver):
            """起一个不落终态的成员，收 stop 后回 completed（不得落账）。"""

            def decide(self, world, notifications):
                if not self.activated and not self.controls and self.rounds == 0:
                    self.rounds += 1
                    return [StartSession(spec=_spec("n-slow"), context="a")]
                return super().decide(world, notifications)

        h = Harness(tmp_path, FakeFactory("slowstart@1", SlowStartDriver()))
        run_id = await h.start("slowstart@1")
        driver = h.factories["slowstart@1"].driver
        for _ in range(200):
            if driver.activated:
                break
            await asyncio.sleep(0.01)
        assert driver.activated, "成员应已激活"

        record = await h.coord.stop_run(run_id)
        assert record.status == "stopped"  # 驱动收尾轮的 completed 不落账
        assert any(c.is_terminal() for c in driver.controls)
        assert h.so.stopped == ["s1"]
        again = await h.coord.stop_run(run_id)
        assert again.status == "stopped"  # 终态重复控制 no-op

    async def test_stop_rejects_implicit(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("fake@1"))
        run_id = await h.store.mint_implicit(session_id="s-imp", role="interactive")
        with pytest.raises(ValueError, match="隐式"):
            await h.coord.stop_run(run_id)

    async def test_stop_sweeps_inflight_birth(self, tmp_path):
        """stop 受理时出生仍在途（账面未落）：排空后才扫掠，迟到成员不逃逸。"""

        class SingleStartDriver(FakeDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                if self.rounds == 1:
                    return [StartSession(spec=_spec("slow"), context="a")]
                return []

        h = Harness(tmp_path, FakeFactory("gated@1", SingleStartDriver()))
        gate = asyncio.Event()
        orig_start = h.so.start_session_with_config

        async def gated_start(cfg):
            await gate.wait()
            return await orig_start(cfg)

        h.so.start_session_with_config = gated_start
        run_id = await h.start("gated@1")
        for _ in range(200):
            rt = getattr(h.coord, "_runtimes").get(run_id)
            if rt is not None and rt.in_flight == 1:
                break
            await asyncio.sleep(0.01)
        assert rt is not None and rt.in_flight == 1, "StartSession 应已派发"

        async def _release() -> None:
            await asyncio.sleep(0.1)
            gate.set()

        releaser = asyncio.get_running_loop().create_task(_release())
        record = await h.coord.stop_run(run_id)
        await releaser
        assert record.status == "stopped"
        assert h.so.stopped == ["s1"]  # 迟到出生被排空后扫进停集

    async def test_stop_in_mint_spawn_window_never_spawns(self, tmp_path):
        """start 的 mint→spawn 窗口内受理 stop：_spawn 不得拉起，run 不复活。"""
        h = Harness(tmp_path, FakeFactory("race@1"))
        gate = asyncio.Event()
        orig_update = h.store.update_status

        async def gated_update(run_id, status):
            record = await orig_update(run_id, status)
            if status == "running":
                await gate.wait()  # start_run 挂起在 update_status 与 _spawn 之间
            return record

        h.store.update_status = gated_update
        start_task = asyncio.get_running_loop().create_task(h.start("race@1"))
        run_id = None
        for _ in range(300):
            runs = await h.store.list_runs()
            if runs:
                run_id = runs[0].run_id
                break
            await asyncio.sleep(0.01)
        assert run_id is not None

        record = await h.coord.stop_run(run_id)  # 窗口内受理：账面落 stopped
        assert record.status == "stopped"

        gate.set()
        await start_task
        assert (await h.store.get(run_id)).status == "stopped"  # 账面终态不再复活
        driver = h.factories["race@1"].driver
        assert driver.rounds == 0  # 驱动零轮——run 任务从未拉起
        assert run_id not in getattr(h.coord, "_runtimes")
        assert run_id not in getattr(h.coord, "_tasks")
        again = await h.coord.stop_run(run_id)
        assert again.status == "stopped"  # 重复控制 no-op，无主任务不受影响


# ── 成员配置组装：宿主注入字段不可自配 ────────────────────────────


class TestRunSessionConfigAssembly:
    @staticmethod
    def test_host_injected_fields_refilled_over_overrides(tmp_path):
        h = Harness(tmp_path, FakeFactory("fake@1"))
        spec = SessionSpec(
            name="n1",
            prompt="p",
            cwd="/tmp",
            config_overrides={
                "run_id": "another-run",
                "role": "hijacked",
            },
        )
        cfg = getattr(getattr(h.coord, "_host"), "_build_config")("real-run", spec, "executor")
        assert cfg.run_id == "real-run"
        assert cfg.role == "executor"

    @staticmethod
    def test_unattended_loop_defaults_question_detect_off(tmp_path):
        """无人值守成员的问句检测默认关。

        显式提供的 LoopConfig 且 question_detect=None → 填 False（驱动未
        表态）；显式 True/False 尊重驱动选择；loop=None 不凭空造——组装会
        连带改变 prompt 渲染与迭代语义。
        """
        from workflow_controller.config.settings import LoopConfig

        h = Harness(tmp_path, FakeFactory("fake@1"))
        host = getattr(h.coord, "_host")

        def build(loop):
            spec = SessionSpec(name="n1", prompt="p", cwd="/tmp", loop=loop)
            return getattr(host, "_build_config")("r", spec, "executor").loop

        injected = build(LoopConfig(max_iterations=3))
        assert injected is not None and injected.question_detect is False
        assert injected.max_iterations == 3  # 其余字段不动

        explicit = build(LoopConfig(question_detect=True))
        assert explicit is not None and explicit.question_detect is True

        assert build(None) is None  # 不凭空造 LoopConfig

    async def test_reviewer_overrides_explicitly_rejected(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("fake@1"))
        for key, value in (
            ("reviewer", {"model": "x"}),
            ("reviewer_profile", "strict"),
        ):
            spec = SessionSpec(
                name="n1",
                prompt="p",
                cwd="/tmp",
                config_overrides={key: value},
            )
            with pytest.raises(NotImplementedError, match="不可经 config_overrides 自配"):
                getattr(h.coord, "_host").start_run_session("real-run", spec, "executor", None)
        assert h.so.started == []

    async def test_reviewer_profile_parsed_into_member_config(self, tmp_path):
        """reviewer_profile 内联 JSON 经宿主解析为成员 SessionConfig.reviewer。

        回归：派发口曾残留「尚未支持」的显式拒绝（与 _build_config 已实现的
        profile→SessionReviewerConfig 映射矛盾）——带 reviewer 配置的图
        （如 workflow-controller.yaml reviewer.enabled=false 的示例工程）全部成员出生失败。
        """
        from workflow_controller.config.settings import SessionReviewerConfig

        h = Harness(tmp_path, FakeFactory("fake@1"))
        run = await h.coord.create_run("reviewer-demo")
        profile = json.dumps({"enabled": False, "task_goal": "T"})
        getattr(h.coord, "_host").start_run_session(
            run.run_id,
            SessionSpec(
                name="n1",
                prompt="p",
                cwd="/tmp",
                reviewer_profile=profile,
            ),
            "executor",
            "node=n1",
        )
        for _ in range(200):
            if h.so.started:
                break
            await asyncio.sleep(0.01)
        assert len(h.so.started) == 1
        reviewer = h.so.started[0].reviewer
        assert isinstance(reviewer, SessionReviewerConfig)
        assert reviewer.enabled is False
        assert reviewer.task_goal == "T"

    async def test_reviewer_profile_non_json_fails_member_not_run(self, tmp_path):
        """非法 profile 是该成员的出生失败回执，不是 run 级崩溃。"""
        h = Harness(tmp_path, FakeFactory("fake@1"))
        run = await h.coord.create_run("reviewer-bad")
        getattr(h.coord, "_host").start_run_session(
            run.run_id,
            SessionSpec(name="n1", prompt="p", cwd="/tmp", reviewer_profile="oops"),
            "executor",
            None,
        )
        # 出生任务异步推进：给失败回执留时间，随后确认 run 仍可正常查询
        await asyncio.sleep(0.1)
        record = await h.store.get(run.run_id)
        assert record.status == "running"


class TestSessionSpecDerivations:
    """spec→配置两条派生归 SessionSpec 自身（信息专家）：全量组装 vs 显式字段局部更新。"""

    @staticmethod
    def test_assembly_fields_full_mapping_and_passthrough():
        spec = SessionSpec(
            name="n1",
            prompt="p",
            cwd="/tmp",
            agent_name="coder",
            model="m1",
            permission_mode="default",
            allowed_tools=("read", "bash"),
            env_overrides={"K": "V"},
            config_overrides={"custom": 1},
        )
        out = spec.assembly_fields()
        assert out["name"] == "n1" and out["initial_prompt"] == "p" and out["cwd"] == "/tmp"
        assert out["agent_name"] == "coder" and out["model"] == "m1"
        assert out["permission_mode"] == "default"
        assert out["allowed_tools"] == ["read", "bash"]  # tuple → list 拷贝
        assert out["env_overrides"] == {"K": "V"}
        assert out["custom"] == 1  # config_overrides 并入
        assert "run_id" not in out and "role" not in out  # 注入字段归宿主

    @staticmethod
    def test_refresh_overrides_omits_unset_and_defaults():
        spec = SessionSpec(name="n1", prompt="new-prompt", cwd="/tmp")
        out = spec.refresh_overrides()
        # 默认值（agent_name=""/permission_mode 默认档）= 未提供，不覆盖成员现值
        assert out == {"initial_prompt": "new-prompt", "cwd": "/tmp"}

    @staticmethod
    def test_refresh_overrides_keeps_explicit_and_merges_passthrough():
        spec = SessionSpec(
            name="n1",
            prompt="p2",
            cwd="/tmp",
            agent_name="coder",
            model="m1",
            allowed_tools=("read",),
            env_overrides={"K": "V"},
            config_overrides={"custom": 2, "run_id": "hijack"},
        )
        out = spec.refresh_overrides()
        assert out["agent_name"] == "coder" and out["model"] == "m1"
        assert out["allowed_tools"] == ["read"] and out["env_overrides"] == {"K": "V"}
        assert out["custom"] == 2
        assert "name" not in out and "reviewer_profile" not in out  # 不入局部更新
        assert out["run_id"] == "hijack"  # 宿主注入键的剔除是宿主策略，不在此层


# ── 防挂起通知 ────────────────────────────────────────────────────


class SpawningAmtDriver(FakeDriver):
    """AMT 用驱动基型：空世界出生两个成员，见 AMT 即按完成收尾。"""

    def __init__(self, first: str, second: str) -> None:
        super().__init__()
        self.amt_seen = False
        self._members = (first, second)

    def decide(self, world, notifications):
        self.rounds += 1
        for n in notifications:
            if isinstance(n, AllSessionsTerminal):
                self.amt_seen = True
            elif isinstance(n, SessionActivated) and n.ok:
                self.activated.append(n)
            elif isinstance(n, SessionOutcomeNotification):
                self.outcomes[n.outcome.session_id] = n.outcome
        if self.amt_seen:
            return [FinishRun(FinishOutcome.COMPLETED)]
        if not self.activated and not self.outcomes and world.in_flight == 0:
            return [
                StartSession(spec=_spec(self._members[0]), context="a"),
                StartSession(spec=_spec(self._members[1]), context="b"),
            ]
        return []


class TestAllSessionsTerminal:
    async def test_amt_fires_for_idle_sessions_and_driver_recovers(self, tmp_path):
        """成员终态停点是 IDLE+loop_status=no：判定不得依赖 completed 态。"""
        h = Harness(tmp_path, FakeFactory("amt@1", SpawningAmtDriver("n1", "n2")))
        run_id = await h.start("amt@1")
        assert await h.await_terminal(run_id) == "completed"
        driver = h.factories["amt@1"].driver
        assert driver.amt_seen

    async def test_amt_fires_when_all_births_fail(self, tmp_path):
        """全员出生失败：失败回执不进成员集，零成员不得恒判非终态挂死。"""

        class AllBirthFailDriver(FakeDriver):
            def __init__(self) -> None:
                super().__init__()
                self.amt_seen = False
                self.failed_births: list[SessionActivated] = []

            def decide(self, world, notifications):
                self.rounds += 1
                for n in notifications:
                    if isinstance(n, AllSessionsTerminal):
                        self.amt_seen = True
                    elif isinstance(n, SessionActivated) and not n.ok:
                        self.failed_births.append(n)
                if self.amt_seen:
                    return [FinishRun(FinishOutcome.FAILED)]
                if not self.failed_births and world.in_flight == 0:
                    return [
                        StartSession(spec=_spec("nobirth-1"), context="a"),
                        StartSession(spec=_spec("nobirth-2"), context="b"),
                    ]
                return []

        h = Harness(tmp_path, FakeFactory("amt@2", AllBirthFailDriver()))
        run_id = await h.start("amt@2")
        assert await h.await_terminal(run_id) == "failed"
        driver = h.factories["amt@2"].driver
        assert len(driver.failed_births) == 2
        assert driver.amt_seen

    async def test_amt_not_fired_for_user_paused_session(self, tmp_path):
        """用户暂停与终态同形（IDLE+loop_status=no）：AMT 不得注入收口暂停成员。"""
        h = Harness(tmp_path, FakeFactory("pause@1", SpawningAmtDriver("n1", "n2-slow")))
        driver = h.factories["pause@1"].driver
        run_id = await h.start("pause@1")
        for _ in range(300):
            if "s1" in driver.outcomes:
                break
            await asyncio.sleep(0.01)
        assert "s1" in driver.outcomes, "n1 应已完成"

        # 用户暂停 n2：与 Session.pause() 落点同形（idle+no+user_pause），投 paused 结局
        s2 = h.so.pool.sessions["s2"]
        s2.state, s2.loop_status, s2.stop_reason = "idle", "no", "user_pause"
        await h.so.bus.emit_outcome(
            "s2",
            SessionOutcome(
                session_id="s2",
                kind="paused",
                stop_reason="user_pause",
                last_error=None,
                result_digest="",
                iteration=1,
                context=None,
            ).to_payload(),
        )
        await asyncio.sleep(0.2)
        assert not driver.amt_seen, "暂停成员不得触发 AMT"
        assert (await h.store.get(run_id)).finished_at is None
        assert "s2" not in h.so.stopped

        # 暂停成员随后完成：AMT 才注入收口
        s2.stop_reason = "reviewer_complete"
        await h.so.bus.emit_outcome(
            "s2",
            SessionOutcome(
                session_id="s2",
                kind="completed",
                stop_reason="reviewer_complete",
                last_error=None,
                result_digest="d",
                iteration=1,
                context=None,
            ).to_payload(),
        )
        assert await h.await_terminal(run_id) == "completed"
        assert driver.amt_seen


# ── 终态收尾扫掠 ──────────────────────────────────────────────────


class TestTerminalSweep:
    async def test_decide_crash_sweeps_sessions(self, tmp_path):
        class CrashDriver(FakeDriver):
            def decide(self, world, notifications):
                if self.rounds == 0:
                    self.rounds += 1
                    return [StartSession(spec=_spec("slow"), context="a")]
                raise RuntimeError("decide boom")

        h = Harness(tmp_path, FakeFactory("crash@1", CrashDriver()))
        run_id = await h.start("crash@1")
        status = await h.await_terminal(run_id)
        run = await h.store.get(run_id)
        assert status == "failed" and "driver_decide_error" in run.error
        assert h.so.stopped == ["s1"]  # 孤儿成员被扫掠停机

    async def test_snapshot_poison_exits_loop_and_sweeps(self, tmp_path):
        """边界快照中毒被吞后，循环也必须退出并扫掠成员（零成员 run 不滞留）。"""

        class SnapshotPoisonDriver(FakeDriver):
            def __init__(self) -> None:
                super().__init__()
                self.poison = False

            def decide(self, world, notifications):
                self.rounds += 1
                for n in notifications:
                    if isinstance(n, SessionActivated) and n.ok:
                        self.activated.append(n)
                        self.poison = True
                if not self.activated and world.in_flight == 0:
                    return [StartSession(spec=_spec("slow"), context="a")]
                return []

            def snapshot(self):
                if self.poison:
                    raise RuntimeError("snapshot boom")
                return super().snapshot()

        h = Harness(tmp_path, FakeFactory("poison@1", SnapshotPoisonDriver()))
        run_id = await h.start("poison@1")
        status = await h.await_terminal(run_id)
        run = await h.store.get(run_id)
        assert status == "failed" and "driver_snapshot_error" in run.error
        assert h.so.stopped == ["s1"]
        for _ in range(200):
            if run_id not in getattr(h.coord, "_runtimes"):
                break
            await asyncio.sleep(0.01)
        assert run_id not in getattr(h.coord, "_runtimes")

    async def test_poison_during_inflight_birth_sweeps_late_session(self, tmp_path):
        """出生在途（账面未落）时快照即炸：失败收尾也必须等出生落定再扫掠。"""

        class PoisonAtOnceDriver(FakeDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                if self.rounds == 1:
                    return [StartSession(spec=_spec("slow"), context="a")]
                return []

            def snapshot(self):
                raise RuntimeError("snapshot boom")

        h = Harness(tmp_path, FakeFactory("inflight@1", PoisonAtOnceDriver()))
        gate = asyncio.Event()
        orig_start = h.so.start_session_with_config

        async def gated_start(cfg):
            await gate.wait()
            return await orig_start(cfg)

        h.so.start_session_with_config = gated_start
        run_id = await h.start("inflight@1")
        for _ in range(200):
            rt = getattr(h.coord, "_runtimes").get(run_id)
            if rt is not None and rt.poisoned:
                break
            await asyncio.sleep(0.01)
        assert rt is not None and rt.poisoned, "快照应已中毒"
        assert (await h.store.get(run_id)).finished_at is None  # 排空在途出生中
        gate.set()  # 迟到成员此时才落账面
        status = await h.await_terminal(run_id)
        run = await h.store.get(run_id)
        assert status == "failed" and "driver_snapshot_error" in run.error
        assert h.so.stopped == ["s1"]  # 迟到出生不逃逸失败收尾

    async def test_driver_finish_sweeps_living_sessions(self, tmp_path):
        """驱动在成员仍在跑时 FinishRun 收口：退出路径同款扫掠，不留守孤儿。"""

        class FinishFastDriver(FakeDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                for n in notifications:
                    if isinstance(n, SessionActivated) and n.ok:
                        self.activated.append(n)
                if self.activated:
                    return [FinishRun(FinishOutcome.COMPLETED)]
                return [StartSession(spec=_spec("n-slow"), context="a")]

        h = Harness(tmp_path, FakeFactory("failfast@1", FinishFastDriver()))
        run_id = await h.start("failfast@1")
        status = await h.await_terminal(run_id)
        assert status == "completed"
        assert h.so.stopped == ["s1"]  # 仍在跑的成员被收尾扫掠停机

    async def test_driver_finish_sweeps_before_ledger(self, tmp_path):
        """FinishRun 收口序与 stop/失败收尾同款：扫掠未完成前账面不落终态。"""

        class FinishFastDriver(FakeDriver):
            def decide(self, world, notifications):
                self.rounds += 1
                for n in notifications:
                    if isinstance(n, SessionActivated) and n.ok:
                        self.activated.append(n)
                if self.activated:
                    return [FinishRun(FinishOutcome.COMPLETED)]
                return [StartSession(spec=_spec("n-slow"), context="a")]

        h = Harness(tmp_path, FakeFactory("sweepfirst@1", FinishFastDriver()))
        gate = asyncio.Event()
        sweeping = asyncio.Event()
        orig_stop = h.so.stop_session

        async def gated_stop(sid: str) -> None:
            sweeping.set()
            await gate.wait()
            await orig_stop(sid)

        h.so.stop_session = gated_stop
        run_id = await h.start("sweepfirst@1")
        await asyncio.wait_for(sweeping.wait(), timeout=5.0)
        assert (await h.store.get(run_id)).finished_at is None  # 收口扫掠中，账面仍非终态
        gate.set()
        status = await h.await_terminal(run_id)
        assert status == "completed"
        assert h.so.stopped == ["s1"]


# ── state 面：投影降级（忙时 / 已退出）────────────────────────────


class TestStateProjectionDegrade:
    async def test_busy_run_falls_back_to_persisted_snapshot(self, tmp_path, monkeypatch):
        import workflow_controller.core.run_coordinator as rc

        monkeypatch.setattr(rc, "STATE_PROJECTION_TIMEOUT_S", 0.05)

        class SlowDecideDriver:
            """激活后的每轮 decide 长睡——run 任务持续在途。"""

            def __init__(self) -> None:
                self.activated = False
                self.rounds = 0

            @staticmethod
            def on_control(control) -> None:
                pass

            @staticmethod
            def handle_op(op, args):
                return {}

            def decide(self, world, notifications):
                self.rounds += 1
                if any(isinstance(n, SessionActivated) and n.ok for n in notifications):
                    self.activated = True
                if self.activated:
                    time.sleep(1.0)
                return [StartSession(spec=_spec("slow"), context="a")] if self.rounds == 1 else []

            def projection(self):
                return {"schema_version": 1, "rounds": self.rounds}

            def snapshot(self):
                return {
                    "schema_version": 1,
                    "state": {"rounds": self.rounds},
                    "pending_timers": [],
                }

        driver = SlowDecideDriver()
        h = Harness(tmp_path, FakeFactory("busy@1", driver))
        run_id = await h.start("busy@1")
        for _ in range(200):
            if driver.activated:
                break
            await asyncio.sleep(0.01)
        assert driver.activated

        state = await h.coord.run_state(run_id)
        # 降级读的是盘上快照（含 state/pending_timers），不是 projection 形状
        assert "state" in state["driver"] and "pending_timers" in state["driver"]
        assert "rounds" not in state["driver"]

    async def test_exited_run_reads_final_projection(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("fake@1"))
        run_id = await h.start()
        await h.await_terminal(run_id)
        for _ in range(200):
            if run_id not in getattr(h.coord, "_runtimes"):
                break
            await asyncio.sleep(0.01)
        assert run_id not in getattr(h.coord, "_runtimes")

        state = await h.coord.run_state(run_id)
        projection = await h.store.load_driver_projection(run_id)
        # runtime 已弹出：终态后 /state 读收尾落盘的最终投影（消费面可渲染形状）
        assert projection is not None and "state" not in projection
        assert state["driver"] == projection

    async def test_exited_run_projection_failure_falls_back_to_snapshot(self, tmp_path):
        class BoomProjectionDriver(FakeDriver):
            def projection(self):
                raise RuntimeError("boom")

        h = Harness(tmp_path, FakeFactory("fake@1", BoomProjectionDriver()))
        run_id = await h.start()
        await h.await_terminal(run_id)  # 收尾投影软失败不拦收尾

        state = await h.coord.run_state(run_id)
        blob = await h.store.load_driver_state(run_id)
        # 未落盘最终投影：降级回落盘上最新快照
        assert await h.store.load_driver_projection(run_id) is None
        assert state["driver"] == blob


# ── 玩具驱动：词表中立性矩阵 ──────────────────────────────────────


class PipelineDriver:
    """纯顺序编排：上一步 completed 才起下一步；任一 failed 即整体失败。"""

    def __init__(self, steps: tuple[str, ...] = ("a", "b", "c")) -> None:
        self.steps = steps
        self.cursor = 0
        self.outcomes: dict[str, str] = {}

    @staticmethod
    def on_control(control) -> None:
        pass

    @staticmethod
    def handle_op(op, args):
        return {}

    def decide(self, world, notifications):
        for n in notifications:
            if isinstance(n, SessionOutcomeNotification):
                self.outcomes[n.outcome.session_id] = n.outcome.kind
        if any(k == "failed" for k in self.outcomes.values()):
            return [FinishRun(FinishOutcome.FAILED)]
        # 上一步 completed 才起下一步（outcomes 追平已起数 = 无在途）
        if len(self.outcomes) == self.cursor and self.cursor < len(self.steps):
            step = self.steps[self.cursor]
            self.cursor += 1
            return [StartSession(spec=_spec(step), context=f"step={step}")]
        if self.cursor >= len(self.steps) and len(self.outcomes) >= len(self.steps):
            return [FinishRun(FinishOutcome.COMPLETED)]
        return []

    def projection(self):
        return {"schema_version": 1, "cursor": self.cursor}

    def snapshot(self):
        return {
            "schema_version": 1,
            "state": {"cursor": self.cursor},
            "pending_timers": [],
        }


class FanOutDriver:
    """扇出/收拢编排：一批起 N 个成员，全员终态后收拢裁决。"""

    def __init__(self, n: int = 4) -> None:
        self.n = n
        self.dispatched = False
        self.outcomes: dict[str, str] = {}
        self.round_sizes: list[int] = []

    @staticmethod
    def on_control(control) -> None:
        pass

    @staticmethod
    def handle_op(op, args):
        return {}

    def decide(self, world, notifications):
        self.round_sizes.append(len(world.sessions))
        for n in notifications:
            if isinstance(n, SessionOutcomeNotification):
                self.outcomes[n.outcome.session_id] = n.outcome.kind
        if not self.dispatched and world.in_flight == 0 and not world.sessions:
            self.dispatched = True
            return [
                StartSession(
                    spec=_spec(f"fan{i}" if i else "fan-fail"),  # 一员必败
                    context=f"slot={i}",
                )
                for i in range(self.n)
            ]
        if len(self.outcomes) >= self.n:
            ok = all(k == "completed" for k in self.outcomes.values())
            return [FinishRun(FinishOutcome.COMPLETED if ok else FinishOutcome.FAILED)]
        return []

    def projection(self):
        return {"schema_version": 1, "outcomes": dict(self.outcomes)}

    def snapshot(self):
        return {
            "schema_version": 1,
            "state": {"outcomes": len(self.outcomes)},
            "pending_timers": [],
        }


class TestVocabularyNeutrality:
    """顺序与扇出两类编排共用同一词表与循环——词表不按 DAG 裁剪。"""

    async def test_pipeline_sequential_steps(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("pipe@1", PipelineDriver()))
        run_id = await h.start("pipe@1")
        assert await h.await_terminal(run_id) == "completed"
        assert [c.name for c in h.so.started] == ["a", "b", "c"]  # 顺序推进
        assert len(await h.coord.list_run_sessions(run_id)) == 3

    async def test_fanout_batch_dispatch_and_converge(self, tmp_path):
        h = Harness(tmp_path, FakeFactory("fan@1", FanOutDriver(n=4)))
        run_id = await h.start("fan@1")
        assert await h.await_terminal(run_id) == "failed"  # fan-fail 成员必败
        assert len(h.so.started) == 4  # 一批派发
        driver = h.factories["fan@1"].driver
        assert driver.round_sizes[1] >= 4  # 第二轮 world 已见全员


async def test_await_sessions_uses_execution_budget_and_stops_on_timeout(tmp_path):
    """run 等待成员使用执行预算，超时后停止成员。"""
    wait_calls = []
    stop_calls = []

    async def wait_session_done(session_id, timeout=None, *, wait_timeout=None):
        wait_calls.append((session_id, timeout, wait_timeout))
        return False

    async def stop_session(session_id, **kwargs):
        stop_calls.append(session_id)

    orchestrator = SimpleNamespace(
        default_project_dir=tmp_path,
        wait_session_done=wait_session_done,
        stop_session=stop_session,
    )
    host = RunHost(
        orchestrator=orchestrator,
        run_store=SimpleNamespace(),
        affiliation=SimpleNamespace(),
        notifier=lambda *_: None,
        run_root=lambda run_id: tmp_path / run_id,
        bus=StubBus(),
    )

    await host.await_sessions("run", ["session-1"], 30.0)

    assert wait_calls == [("session-1", 30.0, None)]
    assert stop_calls == ["session-1"]
