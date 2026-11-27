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

"""协调器工作区执行桥测试：SDK 动作进、真实建区、通知回投与失败路径。

私有仓走真实 git（临时目录）；根作用域随首个建区动作惰性建立，
run 结束后树运行时随任务退出清理。
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from workflow_controller.core.checkpoints import CheckpointStore
from workflow_controller.core.run_coordinator import RunConfig, RunCoordinator
from workflow_controller.core.runs import RunStore
from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService
from workflow_controller.plugin.run_driver import (
    ArtifactRead,
    FinishOutcome,
    FinishRun,
    ReadArtifact,
    RunAction,
)
from workflow_controller.plugin.workspace import (
    AbandonScope,
    CommitScopeTerminal,
    IntegrateScope,
    IntegrationLineRetracted,
    PrepareScopeWorkspace,
    RetractIntegrationLine,
    RetractTarget,
    ScopeAbandoned,
    ScopeIntegrated,
    ScopeIntegrationRejected,
    ScopeTerminalCommitted,
    ScopeWorkspacePrepared,
    WorkspaceActionFailed,
    WorkspaceTreeConfig,
)
from workflow_controller.store.event_store import EventStore

from ._run_plane_stubs import PoolFactsMixin, StubBus, StubPool

if TYPE_CHECKING:
    from collections.abc import Callable

    from workflow_controller.plugin.run_driver import RunNotification


class WorkspaceSO(PoolFactsMixin):
    """无成员运行的最小编排器替身：工作区动作不触发成员面。"""

    def __init__(self, project_dir: Path) -> None:
        self.pool = StubPool()
        self.default_project_dir = project_dir
        # run 键事件落库门面（工作区故障与 claims 拒绝事件的断言面）
        self.event_router = MagicMock()
        self.event_router.default_store = EventStore(project_dir / "run-events.db")

    async def start_session_with_config(self, cfg: Any) -> None:
        raise AssertionError("工作区桥测试不应出生成员")

    async def stop_session(self, sid: str) -> None:
        pass

    async def pause_session(self, sid: str) -> None:
        pass

    async def ensure_reviewer_snapshot(self, sid: str) -> None:
        pass


class ProbeDriver:
    """探针驱动：按计划函数产出动作，留档全部回投通知。"""

    def __init__(self, plan: Callable[[ProbeDriver], list[RunAction] | None]) -> None:
        self._plan = plan
        self.notes: list[RunNotification] = []

    def decide(self, world: Any, notifications: Any) -> list[RunAction]:
        self.notes.extend(notifications)
        actions = self._plan(self)
        return actions if actions is not None else [FinishRun(FinishOutcome.COMPLETED)]

    def on_control(self, control: Any) -> None:
        return None

    def projection(self) -> dict[str, Any]:
        return {"schema_version": 1}

    def snapshot(self) -> dict[str, Any]:
        return {"schema_version": 1, "state": {}, "pending_timers": []}


class OpProbeDriver(ProbeDriver):
    """支持 handle_op 的探针：留档所见 op 并按既定表应答。"""

    def __init__(self, plan: Callable[[ProbeDriver], list[RunAction] | None]) -> None:
        super().__init__(plan)
        self.ops: list[tuple[str, dict[str, Any]]] = []

    def handle_op(self, op: str, args: dict[str, Any]) -> dict[str, Any]:
        self.ops.append((op, dict(args)))
        if op == "get_operation":
            if args.get("operation_id") != "co-9":
                raise LookupError(f"操作不存在：{args.get('operation_id')}")
            return {"operation_id": "co-9", "phase": "succeeded"}
        return {"operation_id": args.get("client_op_id"), "phase": "pending"}


class BridgeHarness:
    """run 平面装配：真实协调器 + 探针驱动 + 真实私有仓根目录。"""

    def __init__(
        self,
        tmp_path: Path,
        plan: Callable[[ProbeDriver], list[RunAction] | None],
        *,
        coord_kwargs: dict[str, Any] | None = None,
        driver: ProbeDriver | None = None,
    ) -> None:
        self.store = RunStore(tmp_path / "events.db")
        self.affiliation = SessionRunAffiliationService(self.store)
        self.bus = StubBus()
        self.so = WorkspaceSO(tmp_path)
        self.driver = driver if driver is not None else ProbeDriver(plan)
        self.coord = RunCoordinator(
            orchestrator=self.so,
            run_store=self.store,
            bus=self.bus,
            driver_factories={
                "probe@1": SimpleFactory(self.driver),
            },
            affiliation=self.affiliation,
            **(coord_kwargs or {}),
        )
        self.program = tmp_path / "prog.txt"
        self.program.write_text("probe", encoding="utf-8")

    async def start(self, *, work_dir: Path | None = None) -> str:
        """启动 run（不等待终态，供运行中查询与 op 通道用例）。

        work_dir 即工作区树的根目录：盘上树配置按它推导位置，须与建区
        动作携带的树配置一致。
        """
        run = await self.coord.start_run(
            RunConfig(format="probe@1", work_dir=str(work_dir) if work_dir is not None else None),
            self.program,
        )
        return run.run_id

    async def start_and_finish(
        self, timeout_s: float = 60.0, *, work_dir: Path | None = None
    ) -> str:
        """启动 run 并等待终态，返回 run_id。"""
        run_id = await self.start(work_dir=work_dir)
        deadline = asyncio.get_running_loop().time() + timeout_s
        while asyncio.get_running_loop().time() < deadline:
            record = await self.store.get(run_id)
            if record.finished_at is not None:
                return run_id
            await asyncio.sleep(0.02)
        raise AssertionError("run 未在限期内落终态")

    def run_events(self, run_id: str, event_type: str) -> list[dict[str, Any]]:
        """读取指定类型的 run 键事件载荷。"""
        rows = self.so.event_router.default_store.get_run_events_sync(run_id)
        return [dict(row["data"]) for row in rows if row["event_type"] == event_type]


class SimpleFactory:
    """固定驱动实例的工厂。"""

    def __init__(self, driver: ProbeDriver) -> None:
        self.driver = driver

    @property
    def format(self) -> str:
        return "probe@1"

    def create(self, run: Any, program_path: Any, params: Any) -> ProbeDriver:
        return self.driver

    def restore(self, run: Any, snapshot: Any, sessions: Any) -> ProbeDriver:
        return self.driver


def _tree_config(work_root: Path) -> WorkspaceTreeConfig:
    """默认策略的树配置。"""
    return WorkspaceTreeConfig(root_dir=str(work_root))


def _notes_of(driver: ProbeDriver, kind: type) -> list[Any]:
    return [n for n in driver.notes if isinstance(n, kind)]


async def test_prepare_builds_real_workspace_and_replies(tmp_path: Path) -> None:
    """建区动作触发基线导入与真实建区，回执携带路径与基线提交。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    (work_root / "design").mkdir()
    (work_root / "design" / "base.md").write_text("base\n", encoding="utf-8")

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, ScopeWorkspacePrepared):
            return None
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id="op-1",
                context="node=a",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    run_id = await h.start_and_finish()

    prepared = _notes_of(h.driver, ScopeWorkspacePrepared)
    assert len(prepared) == 1
    assert prepared[0].context == "node=a"
    assert len(prepared[0].base_commit) == 40
    assert len(prepared[0].integration_base_commit) == 40
    workspace = Path(prepared[0].workspace_path)
    assert workspace.is_dir()
    assert (workspace / "design" / "base.md").read_text(encoding="utf-8") == "base\n"
    assert (work_root / ".workflow-controller" / "scopes" / run_id / "repo.git").is_dir()
    assert not _notes_of(h.driver, WorkspaceActionFailed)


async def test_duplicate_scope_key_replies_failure(tmp_path: Path) -> None:
    """重复建区键被账本拒绝：失败显式回投，run 正常收口。"""
    work_root = tmp_path / "root"
    work_root.mkdir()

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, WorkspaceActionFailed):
            return None
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id=f"op-{len(driver.notes) + 1}",
                context="node=a",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    await h.start_and_finish()

    failures = _notes_of(h.driver, WorkspaceActionFailed)
    assert len(failures) == 1
    assert failures[0].action == "PrepareScopeWorkspace"
    assert "作用域标识已被占用" in failures[0].error


async def test_abandon_removes_workspace_and_bumps_epoch(tmp_path: Path) -> None:
    """废弃动作移除工作区并回投新代次。"""
    work_root = tmp_path / "root"
    work_root.mkdir()

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, ScopeAbandoned):
            return None
        prepared = _notes_of(driver, ScopeWorkspacePrepared)
        if prepared:
            return [
                AbandonScope(
                    scope_id=prepared[0].scope_id,
                    expected_scope_epoch=prepared[0].scope_epoch,
                    client_op_id="op-2",
                    context="node=a",
                )
            ]
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id="op-1",
                context="node=a",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    await h.start_and_finish()

    prepared = _notes_of(h.driver, ScopeWorkspacePrepared)
    abandoned = _notes_of(h.driver, ScopeAbandoned)
    assert len(abandoned) == 1
    assert abandoned[0].new_scope_epoch == prepared[0].scope_epoch + 1
    assert not Path(prepared[0].workspace_path).exists()


async def test_integrate_merges_writer_output_and_releases(tmp_path: Path) -> None:
    """集成动作把写者产出并入根检出，成功后默认释放作用域工作区。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    (work_root / "base.md").write_text("base\n", encoding="utf-8")

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, ScopeIntegrated):
            return None
        prepared = _notes_of(driver, ScopeWorkspacePrepared)
        if prepared:
            (Path(prepared[0].workspace_path) / "out.txt").write_text(
                "writer-a\n", encoding="utf-8"
            )
            return [
                IntegrateScope(
                    scope_id=prepared[0].scope_id,
                    client_op_id="op-2",
                    context="node=a",
                )
            ]
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id="op-1",
                context="node=a",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    await h.start_and_finish()

    integrated = _notes_of(h.driver, ScopeIntegrated)
    assert len(integrated) == 1
    assert integrated[0].integrated_paths == ("out.txt",)
    assert len(integrated[0].new_parent_head) == 40
    assert (work_root / "out.txt").read_text(encoding="utf-8") == "writer-a\n"
    prepared = _notes_of(h.driver, ScopeWorkspacePrepared)
    assert not Path(prepared[0].workspace_path).exists()  # 默认释放
    assert not _notes_of(h.driver, WorkspaceActionFailed)


async def test_integration_conflict_replies_rejection(tmp_path: Path) -> None:
    """同基线双写者改同一路径：先集成者胜，后到者按档位拒绝并携带冲突载荷。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    config = WorkspaceTreeConfig(root_dir=str(work_root), conflict_policy="strict")

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, ScopeIntegrationRejected):
            return None
        prepared = _notes_of(driver, ScopeWorkspacePrepared)
        if _notes_of(driver, ScopeIntegrated):
            return [
                IntegrateScope(
                    scope_id=prepared[1].scope_id,
                    client_op_id="op-4",
                    context="node=b",
                )
            ]
        if len(prepared) == 1:
            return [
                PrepareScopeWorkspace(
                    config=config,
                    scope_key="node-b",
                    client_op_id="op-2",
                    context="node=b",
                )
            ]
        if len(prepared) == 2:
            # 双方建区完毕（同一集成头基线）再各自写入同一路径
            for note, content in zip(prepared, ("from-a\n", "from-b\n"), strict=True):
                (Path(note.workspace_path) / "out.txt").write_text(content, encoding="utf-8")
            return [
                IntegrateScope(
                    scope_id=prepared[0].scope_id,
                    client_op_id="op-3",
                    context="node=a",
                )
            ]
        return [
            PrepareScopeWorkspace(
                config=config,
                scope_key="node-a",
                client_op_id="op-1",
                context="node=a",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    await h.start_and_finish()

    rejected = _notes_of(h.driver, ScopeIntegrationRejected)
    assert len(rejected) == 1
    assert rejected[0].scope_id == "node-b"
    assert rejected[0].conflict_paths == ("out.txt",)
    assert len(rejected[0].base_commit) == 40
    assert (work_root / "out.txt").read_text(encoding="utf-8") == "from-a\n"
    assert not _notes_of(h.driver, WorkspaceActionFailed)


async def test_terminal_commit_records_checkpoints(tmp_path: Path) -> None:
    """终态提交动作回执提交与检查点；scope_start/terminal 行先有文件引用再有编排引用。"""
    work_root = tmp_path / "root"
    work_root.mkdir()

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, ScopeTerminalCommitted):
            return None
        prepared = _notes_of(driver, ScopeWorkspacePrepared)
        if prepared:
            (Path(prepared[0].workspace_path) / "work.txt").write_text("done\n", encoding="utf-8")
            return [
                CommitScopeTerminal(
                    scope_id=prepared[0].scope_id,
                    client_op_id="op-2",
                    context="node=a",
                )
            ]
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id="op-1",
                context="node=a",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    await h.start_and_finish()

    committed = _notes_of(h.driver, ScopeTerminalCommitted)
    assert len(committed) == 1
    assert len(committed[0].commit) == 40
    checkpoints = CheckpointStore(h.store.db_path)
    rows = await checkpoints.list_for_scope("node-a")
    checkpoints.close()
    by_trigger = {row.trigger.value: row for row in rows}
    assert set(by_trigger) == {"scope_start", "terminal"}
    for row in rows:
        assert len(row.workspace_commit) == 40
        assert row.orchestration_state_ref
        assert row.orchestration_state_ref.startswith("driver-snapshot:")
        assert row.barrier_quality.value == "precise"
    assert by_trigger["scope_start"].integration_commit
    # 终态提交只落账不释放：工作区保留给随后的验收与集成
    assert Path(_notes_of(h.driver, ScopeWorkspacePrepared)[0].workspace_path).is_dir()


async def test_abandon_without_tree_replies_failure(tmp_path: Path) -> None:
    """无树 run 的废弃动作显式失败回执。"""

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, WorkspaceActionFailed):
            return None
        return [
            AbandonScope(
                scope_id="node-a",
                expected_scope_epoch=1,
                client_op_id="op-1",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    await h.start_and_finish()

    failures = _notes_of(h.driver, WorkspaceActionFailed)
    assert len(failures) == 1
    assert failures[0].action == "AbandonScope"
    assert "无工作区树" in failures[0].error


async def test_claims_enforce_rejects_overlapping_birth_with_event(tmp_path: Path) -> None:
    """强制档：出生期 claims 相交拒绝第二个单元并落 claim_rejected 事件。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    config = WorkspaceTreeConfig(root_dir=str(work_root), claims_enforce=True)

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, WorkspaceActionFailed):
            return None
        if not _notes_of(driver, ScopeWorkspacePrepared):
            return [
                PrepareScopeWorkspace(
                    config=config,
                    scope_key="node-a",
                    client_op_id="op-1",
                    writes=("op/src/**",),
                    context="node=a",
                )
            ]
        return [
            PrepareScopeWorkspace(
                config=config,
                scope_key="node-b",
                client_op_id="op-2",
                writes=("op/src/gen/**",),
                context="node=b",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    run_id = await h.start_and_finish()

    prepared = _notes_of(h.driver, ScopeWorkspacePrepared)
    assert [p.scope_id for p in prepared] == ["node-a"]
    failures = _notes_of(h.driver, WorkspaceActionFailed)
    assert len(failures) == 1
    assert "claims 相交" in failures[0].error
    events = h.run_events(run_id, "workspace.claim_rejected")
    assert len(events) == 1
    assert events[0]["scope_key"] == "node-b"
    assert events[0]["claims"] == ["op/src/gen/**"]
    assert events[0]["conflicts"] == [{"holder_scope_id": "node-a", "claims": ["op/src/gen/**"]}]


async def test_claims_default_registers_and_lands_in_checkpoint(tmp_path: Path) -> None:
    """默认档：claims 相交只登记不拒绝，声明落 scope_start 检查点行。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    claims = ("op/src/**",)

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if len(_notes_of(driver, ScopeWorkspacePrepared)) >= 2:
            return None
        if _notes_of(driver, ScopeWorkspacePrepared):
            return [
                PrepareScopeWorkspace(
                    config=_tree_config(work_root),
                    scope_key="node-b",
                    client_op_id="op-2",
                    writes=claims,
                    context="node=b",
                )
            ]
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id="op-1",
                writes=claims,
                context="node=a",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    await h.start_and_finish()

    assert len(_notes_of(h.driver, ScopeWorkspacePrepared)) == 2
    assert not _notes_of(h.driver, WorkspaceActionFailed)
    checkpoints = CheckpointStore(h.store.db_path)
    for scope_id in ("node-a", "node-b"):
        rows = await checkpoints.list_for_scope(scope_id)
        scope_start = next(row for row in rows if row.trigger.value == "scope_start")
        assert scope_start.claims == claims
    checkpoints.close()


async def test_git_merge_conflict_emits_fault_event(tmp_path: Path) -> None:
    """可合并档真冲突：拒绝回执之外另落 fault.workspace_conflict 事件。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    (work_root / "shared.txt").write_text("base\n", encoding="utf-8")

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, ScopeIntegrationRejected):
            return None
        prepared = _notes_of(driver, ScopeWorkspacePrepared)
        if _notes_of(driver, ScopeIntegrated):
            return [
                IntegrateScope(
                    scope_id=prepared[1].scope_id,
                    client_op_id="op-4",
                    context="node=b",
                )
            ]
        if len(prepared) == 1:
            return [
                PrepareScopeWorkspace(
                    config=_tree_config(work_root),
                    scope_key="node-b",
                    client_op_id="op-2",
                    context="node=b",
                )
            ]
        if len(prepared) == 2:
            # 同文件同片段不同改动：三方合并真冲突
            (Path(prepared[0].workspace_path) / "shared.txt").write_text(
                "from-a\n", encoding="utf-8"
            )
            (Path(prepared[1].workspace_path) / "shared.txt").write_text(
                "from-b\n", encoding="utf-8"
            )
            return [
                IntegrateScope(
                    scope_id=prepared[0].scope_id,
                    client_op_id="op-3",
                    context="node=a",
                )
            ]
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id="op-1",
                context="node=a",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    run_id = await h.start_and_finish()

    rejected = _notes_of(h.driver, ScopeIntegrationRejected)
    assert len(rejected) == 1
    assert rejected[0].conflict_paths == ("shared.txt",)
    events = h.run_events(run_id, "fault.workspace_conflict")
    assert len(events) == 1
    assert events[0]["conflict_paths"] == ["shared.txt"]
    assert events[0]["scope_id"] == rejected[0].scope_id
    assert events[0]["parent_scope_id"] == rejected[0].parent_scope_id
    assert len(events[0]["base_commit"]) == 40


async def test_writer_local_change_never_conflicts_at_bridge(tmp_path: Path) -> None:
    """双基线回归（桥层）：写者只改自建路径、父树无变化时不误报冲突。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    (work_root / "code.txt").write_text("base\n", encoding="utf-8")

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, ScopeIntegrated):
            return None
        prepared = _notes_of(driver, ScopeWorkspacePrepared)
        if prepared:
            writer_local = Path(prepared[0].workspace_path) / "run.log"
            writer_local.write_text("writer-log\n", encoding="utf-8")
            return [
                IntegrateScope(
                    scope_id=prepared[0].scope_id,
                    client_op_id="op-2",
                    context="node=a",
                )
            ]
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id="op-1",
                context="node=a",
            )
        ]

    h = BridgeHarness(tmp_path, plan)
    run_id = await h.start_and_finish()

    integrated = _notes_of(h.driver, ScopeIntegrated)
    assert len(integrated) == 1
    assert integrated[0].integrated_paths == ()
    assert not _notes_of(h.driver, ScopeIntegrationRejected)
    assert h.run_events(run_id, "fault.workspace_conflict") == []


async def test_quiesce_failure_emits_fault_and_fails_action(tmp_path: Path) -> None:
    """闸门不可静止：动作按失败回执，另落 fault.scope_quiesce_failed 事件。"""
    work_root = tmp_path / "root"
    work_root.mkdir()

    async def never_drains(_scope_id: str) -> None:
        raise RuntimeError("后台写进程无法排空")

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, WorkspaceActionFailed):
            return None
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id="op-1",
                context="node=a",
            )
        ]

    h = BridgeHarness(tmp_path, plan, coord_kwargs={"workspace_drain_hook": never_drains})
    run_id = await h.start_and_finish()

    failures = _notes_of(h.driver, WorkspaceActionFailed)
    assert len(failures) == 1
    assert "闸门未能静止" in failures[0].error
    events = h.run_events(run_id, "fault.scope_quiesce_failed")
    assert len(events) == 1
    assert events[0]["action"] == "PrepareScopeWorkspace"
    assert "闸门未能静止" in events[0]["error"]


async def test_run_api_lists_scopes_and_checkpoints(tmp_path: Path) -> None:
    """CoreAPI 作用域/检查点查询：树在位返回账面行，检查点按树根锚定。"""
    from workflow_controller.api.run_api import RunAPI

    work_root = tmp_path / "root"
    work_root.mkdir()
    (work_root / "design").mkdir()
    (work_root / "design" / "base.md").write_text("base\n", encoding="utf-8")
    gate = {"finish": False}

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if gate["finish"]:
            return None
        prepared = _notes_of(driver, ScopeWorkspacePrepared)
        if not prepared:
            return [
                PrepareScopeWorkspace(
                    config=_tree_config(work_root),
                    scope_key="node-a",
                    client_op_id="op-1",
                    context="node=a",
                )
            ]
        if not _notes_of(driver, ScopeTerminalCommitted):
            (Path(prepared[0].workspace_path) / "work.txt").write_text("done\n", encoding="utf-8")
            return [
                CommitScopeTerminal(
                    scope_id=prepared[0].scope_id,
                    client_op_id="op-2",
                    context="node=a",
                )
            ]
        return []  # 保持运行供查询

    h = BridgeHarness(tmp_path, plan)
    run_id = await h.start()
    api = RunAPI(h.coord)
    deadline = asyncio.get_running_loop().time() + 30.0
    while asyncio.get_running_loop().time() < deadline:
        checkpoints = await api.list_run_checkpoints(run_id)
        if len(checkpoints) >= 2:
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("检查点未在限期内落账")

    scopes = {s["scope_id"]: s for s in await api.list_run_scopes(run_id)}
    assert f"{run_id}-root" in scopes
    node_scope = scopes["node-a"]
    assert node_scope["scope_kind"] == "main_writer"
    assert node_scope["scope_epoch"] == 1
    assert node_scope["state"] == "active"
    assert Path(node_scope["workspace_path"]).is_dir()

    by_trigger = {row["trigger"]: row for row in checkpoints}
    assert set(by_trigger) == {"scope_start", "terminal"}
    for row in checkpoints:
        assert row["scope_id"] == "node-a"
        assert row["orchestration_state_ref"].startswith("driver-snapshot:")
        assert len(row["workspace_commit"]) == 40
    filtered = await api.list_run_checkpoints(run_id, scope_id="node-a")
    assert {row["checkpoint_id"] for row in filtered} == {
        row["checkpoint_id"] for row in checkpoints
    }
    assert await api.list_run_checkpoints(run_id, scope_id="nobody") == []

    gate["finish"] = True
    await h.coord.stop_run(run_id)


async def test_run_api_rollback_op_channel_and_operation_lookup(tmp_path: Path) -> None:
    """request_run_rollback 经驱动 op 转发；get_run_operation 查询与错误翻译。"""
    from workflow_controller.api.run_api import (
        RunAPI,
        RunApiInvalidStateError,
        RunApiNotFoundError,
    )

    work_root = tmp_path / "root"
    work_root.mkdir()

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        return []  # 保持运行供 op 通道用例

    probe = OpProbeDriver(plan)
    h = BridgeHarness(tmp_path, plan, driver=probe)
    run_id = await h.start()
    api = RunAPI(h.coord)

    result = await api.request_run_rollback(
        run_id,
        node_id="a",
        client_op_id="co-9",
        expected_epoch=1,
    )
    assert result == {"operation_id": "co-9", "phase": "pending"}
    assert probe.ops[0][0] == "rollback_node"
    assert probe.ops[0][1]["node_id"] == "a"

    op = await api.get_run_operation(run_id, "co-9")
    assert op["phase"] == "succeeded"
    with pytest.raises(RunApiNotFoundError):
        await api.get_run_operation(run_id, "missing")

    # 显式给出的检查点不在 run 账上：明确拒绝
    with pytest.raises(ValueError, match="检查点不存在"):
        await api.request_run_rollback(
            run_id,
            node_id="a",
            client_op_id="co-10",
            expected_epoch=1,
            checkpoint_id="nope",
        )

    await h.coord.stop_run(run_id)
    with pytest.raises(RunApiInvalidStateError):
        await api.get_run_operation(run_id, "co-9")


async def test_run_api_batch_rollback_op_channel(tmp_path: Path) -> None:
    """request_run_rollback_nodes 经驱动 op 转发；显式检查点先对账核验。"""
    from workflow_controller.api.run_api import RunAPI

    work_root = tmp_path / "root"
    work_root.mkdir()

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        return []  # 保持运行供 op 通道用例

    probe = OpProbeDriver(plan)
    h = BridgeHarness(tmp_path, plan, driver=probe)
    run_id = await h.start()
    api = RunAPI(h.coord)

    result = await api.request_run_rollback_nodes(
        run_id,
        targets=[
            {"node_id": "a", "expected_epoch": 1},
            {"node_id": "b", "expected_epoch": 2, "scope_id": "b-w1"},
        ],
        client_op_id="co-11",
        expected_root_epoch=2,
        reason="批量回滚",
    )
    assert result == {"operation_id": "co-11", "phase": "pending"}
    assert probe.ops[0][0] == "rollback_nodes"
    forwarded = probe.ops[0][1]["targets"]
    assert [t["node_id"] for t in forwarded] == ["a", "b"]
    assert forwarded[1]["expected_epoch"] == 2
    assert forwarded[1]["scope_id"] == "b-w1"
    assert probe.ops[0][1]["expected_root_epoch"] == 2

    # 显式给出的检查点不在 run 账上：明确拒绝
    with pytest.raises(ValueError, match="检查点不存在"):
        await api.request_run_rollback_nodes(
            run_id,
            targets=[{"node_id": "a", "expected_epoch": 1, "checkpoint_id": "nope"}],
            client_op_id="co-12",
        )
    # 空目标集：明确拒绝
    with pytest.raises(ValueError, match="targets"):
        await api.request_run_rollback_nodes(run_id, targets=[], client_op_id="co-13")

    await h.coord.stop_run(run_id)


def _retraction_plan(
    work_root: Path,
    *,
    bogus_first: bool = False,
):
    """双写者集成后回退 node-a 的动作计划（可先注入一次未登记目标的拒绝）。"""

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, IntegrationLineRetracted):
            return None
        failures = _notes_of(driver, WorkspaceActionFailed)
        if failures:
            return [
                RetractIntegrationLine(
                    targets=(
                        RetractTarget(scope_id="node-a", checkpoint_id="node-a:scope_start:e1"),
                    ),
                    expected_root_epoch=1,
                    client_op_id="op-retract-2",
                    context="node=a",
                )
            ]
        integrated = _notes_of(driver, ScopeIntegrated)
        if len(integrated) >= 2:
            checkpoint = "node-a:scope_start:missing" if bogus_first else "node-a:scope_start:e1"
            return [
                RetractIntegrationLine(
                    targets=(RetractTarget(scope_id="node-a", checkpoint_id=checkpoint),),
                    expected_root_epoch=1,
                    client_op_id="op-retract-1",
                    context="node=a",
                )
            ]
        terminals = _notes_of(driver, ScopeTerminalCommitted)
        if len(terminals) >= 2:
            return [
                IntegrateScope(scope_id="node-a", client_op_id="op-i1", context="node=a"),
                IntegrateScope(scope_id="node-b", client_op_id="op-i2", context="node=b"),
            ]
        prepared = _notes_of(driver, ScopeWorkspacePrepared)
        if len(prepared) >= 2:
            for note in prepared:
                (Path(note.workspace_path) / f"{note.scope_id}.md").write_text(
                    f"from-{note.scope_id}\n", encoding="utf-8"
                )
            return [
                CommitScopeTerminal(scope_id="node-a", client_op_id="op-t1", context="node=a"),
                CommitScopeTerminal(scope_id="node-b", client_op_id="op-t2", context="node=b"),
            ]
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id="op-p1",
                context="node=a",
            ),
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-b",
                client_op_id="op-p2",
                context="node=b",
            ),
        ]

    return plan


async def test_integration_retract_retreats_line_and_preserves_sibling(tmp_path: Path) -> None:
    """已合并回滚经桥执行：线回退到目标基线、同基线兄弟保留、根代次 +1。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    (work_root / "base.md").write_text("base\n", encoding="utf-8")

    h = BridgeHarness(tmp_path, _retraction_plan(work_root))
    run_id = await h.start_and_finish()

    retracted = _notes_of(h.driver, IntegrationLineRetracted)
    assert len(retracted) == 1
    assert retracted[0].new_root_epoch == 2
    assert {scope for scope, _epoch in retracted[0].invalidated} == {"node-a"}
    assert retracted[0].preserved == ("node-b",)
    # 根检出：目标产出撤回、保留者产出在盘
    assert not (work_root / "node-a.md").exists()
    assert (work_root / "node-b.md").read_text(encoding="utf-8") == "from-node-b\n"
    assert not _notes_of(h.driver, WorkspaceActionFailed)
    # 私有仓在场、集成线引用即新头
    repo_dir = work_root / ".workflow-controller" / "scopes" / run_id / "repo.git"
    assert repo_dir.is_dir()


async def test_integration_retract_missing_checkpoint_rejected_zero_change(tmp_path: Path) -> None:
    """回退目标未在检查点账登记：明确拒绝且零改动（根代次与线未被触碰）。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    (work_root / "base.md").write_text("base\n", encoding="utf-8")

    h = BridgeHarness(tmp_path, _retraction_plan(work_root, bogus_first=True))
    await h.start_and_finish()

    failures = _notes_of(h.driver, WorkspaceActionFailed)
    assert len(failures) == 1
    assert failures[0].action == "RetractIntegrationLine"
    assert "未在账本登记" in failures[0].error
    # 拒绝零改动的判据：同根代次的合法回退随后成功（代次仍为 1）
    retracted = _notes_of(h.driver, IntegrationLineRetracted)
    assert len(retracted) == 1
    assert retracted[0].invalidated and retracted[0].new_root_epoch == 2


def _multi_retraction_plan(work_root: Path, *, bogus_second: bool = False):
    """双写者集成后一次回退两个目标的动作计划（可注入第二目标检查点缺席）。"""

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        if _notes_of(driver, IntegrationLineRetracted) or _notes_of(driver, WorkspaceActionFailed):
            return None
        integrated = _notes_of(driver, ScopeIntegrated)
        if len(integrated) >= 2:
            second = "node-b:scope_start:missing" if bogus_second else "node-b:scope_start:e1"
            return [
                RetractIntegrationLine(
                    targets=(
                        RetractTarget(scope_id="node-a", checkpoint_id="node-a:scope_start:e1"),
                        RetractTarget(scope_id="node-b", checkpoint_id=second),
                    ),
                    expected_root_epoch=1,
                    client_op_id="op-retract-1",
                    context="node=a",
                )
            ]
        terminals = _notes_of(driver, ScopeTerminalCommitted)
        if len(terminals) >= 2:
            return [
                IntegrateScope(scope_id="node-a", client_op_id="op-i1", context="node=a"),
                IntegrateScope(scope_id="node-b", client_op_id="op-i2", context="node=b"),
            ]
        prepared = _notes_of(driver, ScopeWorkspacePrepared)
        if len(prepared) >= 2:
            for note in prepared:
                (Path(note.workspace_path) / f"{note.scope_id}.md").write_text(
                    f"from-{note.scope_id}\n", encoding="utf-8"
                )
            return [
                CommitScopeTerminal(scope_id="node-a", client_op_id="op-t1", context="node=a"),
                CommitScopeTerminal(scope_id="node-b", client_op_id="op-t2", context="node=b"),
            ]
        return [
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-a",
                client_op_id="op-p1",
                context="node=a",
            ),
            PrepareScopeWorkspace(
                config=_tree_config(work_root),
                scope_key="node-b",
                client_op_id="op-p2",
                context="node=b",
            ),
        ]

    return plan


async def test_multi_target_retract_executes_through_bridge(tmp_path: Path) -> None:
    """多目标回退经桥执行：两个目标一并失效换代、根代次 +1、产出全部撤回。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    (work_root / "base.md").write_text("base\n", encoding="utf-8")

    h = BridgeHarness(tmp_path, _multi_retraction_plan(work_root))
    await h.start_and_finish()

    retracted = _notes_of(h.driver, IntegrationLineRetracted)
    assert len(retracted) == 1
    assert retracted[0].new_root_epoch == 2
    assert {scope for scope, _epoch in retracted[0].invalidated} == {"node-a", "node-b"}
    assert retracted[0].preserved == ()
    assert not (work_root / "node-a.md").exists()
    assert not (work_root / "node-b.md").exists()
    assert (work_root / "base.md").read_text(encoding="utf-8") == "base\n"
    assert not _notes_of(h.driver, WorkspaceActionFailed)


async def test_multi_target_retract_second_target_unregistered_rejected(
    tmp_path: Path,
) -> None:
    """第二目标检查点未登记：明确拒绝且零改动（双目标产物与根代次原样）。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    (work_root / "base.md").write_text("base\n", encoding="utf-8")

    h = BridgeHarness(tmp_path, _multi_retraction_plan(work_root, bogus_second=True))
    await h.start_and_finish()

    failures = _notes_of(h.driver, WorkspaceActionFailed)
    assert len(failures) == 1
    assert failures[0].action == "RetractIntegrationLine"
    assert "node-b:scope_start:missing" in failures[0].error
    assert not _notes_of(h.driver, IntegrationLineRetracted)
    # 零改动：两位目标的集成产出仍在根检出
    assert (work_root / "node-a.md").read_text(encoding="utf-8") == "from-node-a\n"
    assert (work_root / "node-b.md").read_text(encoding="utf-8") == "from-node-b\n"


async def test_read_artifact_resolves_node_scope_workspace(tmp_path: Path) -> None:
    """产物读取按发起方节点的活动作用域解析基准目录；没有活动作用域时回落根检出。"""
    work_root = tmp_path / "root"
    work_root.mkdir()
    (work_root / "probe.txt").write_text("from-root\n", encoding="utf-8")

    def plan(driver: ProbeDriver) -> list[RunAction] | None:
        prepared = _notes_of(driver, ScopeWorkspacePrepared)
        reads = _notes_of(driver, ArtifactRead)
        if not prepared:
            return [
                PrepareScopeWorkspace(
                    config=_tree_config(work_root),
                    scope_key="node-a",
                    client_op_id="op-1",
                    owner_ref="a",
                    context="node=a",
                )
            ]
        if not reads:
            # 工作区与根检出同名文件内容不同：读到哪一份即说明基准目录是哪
            (Path(prepared[0].workspace_path) / "probe.txt").write_text(
                "from-scope\n", encoding="utf-8"
            )
            return [
                ReadArtifact(path="probe.txt", context="node=a"),
                ReadArtifact(path="probe.txt", context="node=a/verify"),
                ReadArtifact(path="probe.txt", context="probe"),
            ]
        if not _notes_of(driver, ScopeAbandoned):
            return [
                AbandonScope(
                    scope_id="node-a",
                    expected_scope_epoch=1,
                    client_op_id="op-2",
                    context="node=a",
                )
            ]
        if len(reads) == 3:
            return [ReadArtifact(path="probe.txt", context="node=a")]  # 作用域已废弃
        return None

    h = BridgeHarness(tmp_path, plan)
    await h.start_and_finish(work_dir=work_root)

    assert [(n.context, n.value) for n in _notes_of(h.driver, ArtifactRead)] == [
        ("node=a", "from-scope"),
        ("node=a/verify", "from-scope"),
        ("probe", "from-root"),
        ("node=a", "from-root"),
    ]
    assert not _notes_of(h.driver, WorkspaceActionFailed)
