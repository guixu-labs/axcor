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

"""深度恢复对账测试：从账本表重建账本，对操作日志与盘上事实对账。

现场以真实 git、检查点账与作用域账本表构造各状态的作用域后丢弃内存账
本，断言重建结果逐字段与重启前一致；崩溃注入覆盖回退补账、未生效放弃、
分叉即停，以及建区、集成、恢复按各自所在的引用对账后的行补账。
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.checkpoints import (
    BarrierQuality,
    CheckpointStore,
    CheckpointTrigger,
    ScopeCheckpoint,
)
from workflow_controller.core.runs import RunStore
from workflow_controller.core.workspace.actions import (
    PrepareScopeWorkspace,
    ScopeIntegrated,
    ScopeIntegrationRejected,
    ScopeWorkspaceSpec,
)
from workflow_controller.core.workspace.integration import IntegrationCoordinator
from workflow_controller.core.workspace.manager import RetractTarget, WorkspaceManager
from workflow_controller.core.workspace.manifest import WorkspaceManifest
from workflow_controller.core.workspace.oplog import OperationPhase, SagaOperation, SagaOplog
from workflow_controller.core.workspace.recovery import (
    WorkspaceRecoveryError,
    rebuild_workspace_tree,
)
from workflow_controller.core.workspace.repository import (
    ROOT_INTEGRATION_REF,
    WorkspaceRepository,
)
from workflow_controller.core.workspace.scope_ledger import ScopeLedgerStore
from workflow_controller.core.workspace.scope_tree import (
    ScopeGateState,
    ScopeKind,
    ScopeRecord,
    ScopeState,
    ScopeTree,
)

from ._support import FakeOrchestrator, at_rest, build_workspace, run_git

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from workflow_controller.core.workspace.recovery import WorkspaceRecoveryReport

_ROOT_SCOPE_ID = "run-1-root"


async def _record(manager: WorkspaceManager, scope_id: str) -> ScopeRecord:
    """取作用域账本行；缺席即断言失败。"""
    record = await manager.get_scope(scope_id)
    assert record is not None
    return record


async def _exploding(*_args: object, **_kwargs: object) -> None:
    """崩溃注入桩：把指定步骤换成抛错，模拟进程崩溃。"""
    raise RuntimeError("崩溃注入")


class _CrashSite:
    """带检查点账与账本表的谱系现场：桥式落 scope_start 行，账本行随动作落表。"""

    def __init__(self, tmp_path: Path, *, retain_after_merge: bool = False) -> None:
        self.db_path = tmp_path / "events.db"
        self.ledger = ScopeLedgerStore(self.db_path, root_scope_id=_ROOT_SCOPE_ID)
        self.root, self.repository, self.manager = build_workspace(
            tmp_path, scope_tree=ScopeTree(store=self.ledger)
        )
        self.orchestrator = FakeOrchestrator(self.manager)
        self.checkpoints = CheckpointStore(self.db_path)
        self.retain_after_merge = retain_after_merge
        # bases：建区时的集成头；scope_bases：作用域自己的建区基线提交
        self.bases: dict[str, str] = {}
        self.scope_bases: dict[str, str] = {}
        self._row_seq = 0
        # rebuild() 重建出的账本存储（与原存储不是同一连接，对应重启后的新进程）
        self.reloaded_ledger = ScopeLedgerStore(self.db_path, root_scope_id=_ROOT_SCOPE_ID)

    async def build(self) -> None:
        """铺检查点表与账本表后建现场：w1（集成后释放）、w2（活动带声明）、w3（废弃）。"""
        await RunStore(self.db_path).list_runs()  # 统一迁移铺 scope_checkpoints 与 scopes 表
        await self.orchestrator.bootstrap_root()
        ws_a = await self.prepare("node-a")
        ws_b = await self.prepare("node-b")
        ws_w1 = await self.prepare("node-w1")
        ws_w2 = await self.prepare("node-w2", claims=("design/w2/**",))
        await self.prepare("node-w3")
        for key, ws in (("node-a", ws_a), ("node-b", ws_b), ("node-w1", ws_w1)):
            (ws / "design" / f"{key}.md").write_text(f"from-{key}\n", encoding="utf-8")
        (ws_w2 / "design" / "w2.md").write_text("from-w2\n", encoding="utf-8")
        await self.integrate("node-a")
        await self.integrate("node-b")
        await self.integrate("node-w1")
        await self.orchestrator.abandon("node-w3", 1)  # 废弃：工作区无、未进线
        # node-w2 保持活动（工作区在盘）

    async def prepare(
        self,
        key: str,
        *,
        parent_scope_id: str | None = None,
        scope_kind: ScopeKind = ScopeKind.MAIN_WRITER,
        claims: tuple[str, ...] = (),
        owner_ref: str | None = None,
    ) -> Path:
        prepared = await self.orchestrator.prepare(
            key,
            parent_scope_id=parent_scope_id,
            scope_kind=scope_kind,
            claims=claims,
            owner_ref=owner_ref,
        )
        self.bases[key] = prepared.integration_base_commit
        self.scope_bases[key] = prepared.base_commit
        self._row_seq += 1
        await self.checkpoints.record(
            ScopeCheckpoint(
                checkpoint_id=f"{key}:scope_start:e1",
                scope_id=key,
                scope_kind=scope_kind,
                parent_scope_id=parent_scope_id or _ROOT_SCOPE_ID,
                scope_epoch=1,
                root_epoch=1,
                trigger=CheckpointTrigger.SCOPE_START,
                barrier_quality=BarrierQuality.PRECISE,
                workspace_commit=prepared.base_commit,
                workspace_branch=f"scope/{key}",
                integration_commit=prepared.integration_base_commit,
                claims=claims,
                orchestration_state_ref="driver-snapshot:1",
                created_at=f"2026-01-01T00:00:{self._row_seq:02d}+00:00",
            )
        )
        return Path(prepared.workspace_path)

    async def integrate(self, key: str) -> None:
        outcome = await self.orchestrator.integrate(key)
        assert isinstance(outcome, ScopeIntegrated)
        # 默认收口：集成成功即释放工作区（分支保留）；保留配置下工作区留在盘上
        if not self.retain_after_merge:
            await self.manager.release_scope(key)

    async def retract_a(self) -> None:
        await self.manager.retract_integration_line(
            targets=(RetractTarget(scope_id="node-a", base_commit=self.bases["node-a"]),),
            expected_root_epoch=1,
            client_op_id="retract-crash",
        )

    async def pending_op(self) -> SagaOperation:
        """崩溃现场唯一挂起的操作。"""
        [operation] = await SagaOplog(self.repository.tree_dir).pending_operations()
        return operation

    async def crash_at(
        self,
        monkeypatch: pytest.MonkeyPatch,
        target: type,
        step: str,
        action: Callable[[], Awaitable[object]],
    ) -> SagaOperation:
        """把指定步骤换成抛错来模拟进程崩溃，返回崩溃后挂起的操作。"""

        async def exploding(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("崩溃注入")

        monkeypatch.setattr(target, step, exploding)
        with pytest.raises(RuntimeError, match="崩溃注入"):
            await action()
        monkeypatch.undo()
        return await self.pending_op()

    async def rebuild(self) -> tuple[WorkspaceManager, WorkspaceRecoveryReport]:
        """丢弃内存账本后以全新部件重建（重开/崩溃后的树运行时再生）。"""
        repository = WorkspaceRepository(self.root, tree_id="run-1")
        ledger = ScopeLedgerStore(self.db_path, root_scope_id=_ROOT_SCOPE_ID)
        scope_tree = ScopeTree(store=ledger)
        oplog = SagaOplog(repository.tree_dir)
        integration = IntegrationCoordinator(repository)
        manager = WorkspaceManager(
            repository=repository,
            scope_tree=scope_tree,
            oplog=oplog,
            manifest=WorkspaceManifest(),
            integration_coordinator=integration,
        )
        report = await rebuild_workspace_tree(
            repository=repository,
            scope_tree=scope_tree,
            integration=integration,
            manager=manager,
            checkpoints=self.checkpoints,
            oplog=oplog,
            ledger=ledger,
        )
        self.reloaded_ledger = ledger
        return manager, report


async def test_recovery_rebuilds_ledger_from_ledger_table(tmp_path: Path) -> None:
    """正常重建：状态、代次与属主取自账本表，claims 复登记，活动组内存态显式降级。"""
    site = _CrashSite(tmp_path)
    await site.build()

    manager, report = await site.rebuild()

    states = {r.scope_id: (r.state, r.scope_epoch) for r in await manager.list_scopes()}
    assert states == {
        _ROOT_SCOPE_ID: (ScopeState.ACTIVE, 1),
        "node-a": (ScopeState.RELEASED, 1),
        "node-b": (ScopeState.RELEASED, 1),
        "node-w1": (ScopeState.RELEASED, 1),
        "node-w2": (ScopeState.ACTIVE, 1),
        "node-w3": (ScopeState.DISCARDED, 2),
    }
    assert report.root_epoch == 1
    assert report.rebuilt_scope_ids == [
        _ROOT_SCOPE_ID,
        "node-a",
        "node-b",
        "node-w1",
        "node-w2",
        "node-w3",
    ]
    assert report.claims_restored == ["node-w2"]
    assert report.degraded == ["活动组内存态未恢复：恢复后的首个副作用将开新组"]
    # 重建后账本可用：新写者建区照常（基线取当前集成头）
    prepared = await manager.prepare_scope_workspace(
        PrepareScopeWorkspace(
            parent_scope_id=_ROOT_SCOPE_ID,
            scope_key="node-new",
            spec=ScopeWorkspaceSpec(scope_kind=ScopeKind.MAIN_WRITER),
            client_op_id="recovered-prepare-1",
        )
    )
    assert len(prepared.base_commit) == 40


@pytest.mark.parametrize(
    ("retain_after_merge", "key"),
    [(True, "node-b"), (True, "node-empty"), (False, "node-empty")],
    ids=["on-line-retained", "empty-retained", "empty-released"],
)
async def test_recovery_keeps_integrated_scope_state(
    tmp_path: Path, retain_after_merge: bool, key: str
) -> None:
    """已集成作用域重启后状态不变：工作区保留的停在已集成，释放后的停在已释放。"""
    site = _CrashSite(tmp_path, retain_after_merge=retain_after_merge)
    await site.build()
    if key == "node-empty":
        # 不改 merge 路径文件：空集成，集成线上没有它的集成提交
        await site.prepare(key)
        await site.integrate(key)

    manager, _report = await site.rebuild()

    states = {r.scope_id: r.state for r in await manager.list_scopes()}
    expected = ScopeState.INTEGRATED if retain_after_merge else ScopeState.RELEASED
    assert states[key] is expected
    assert (site.repository.workspaces_dir / key).exists() is retain_after_merge
    # 仍在执行的与已废弃的作用域不受影响
    assert states["node-w2"] is ScopeState.ACTIVE
    assert states["node-w3"] is ScopeState.DISCARDED


async def test_recovery_lets_retraction_preserve_retained_sibling(tmp_path: Path) -> None:
    """保留配置下恢复后回退：目标按已集成受理，已集成的兄弟按保留者重放、不废弃。"""
    site = _CrashSite(tmp_path, retain_after_merge=True)
    await site.build()
    manager, _report = await site.rebuild()

    outcome = await manager.retract_integration_line(
        targets=(RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),),
        expected_root_epoch=1,
        client_op_id="retract-after-recovery",
    )

    # b 与 w1 的建区基线不含 a 的集成提交、集成在 a 之后：保留并按原集成序重放
    assert outcome.preserved == ("node-b", "node-w1")
    assert [scope for scope, _epoch in outcome.invalidated] == ["node-a"]
    assert not (site.root / "design" / "node-a.md").exists()
    assert (site.root / "design" / "node-b.md").exists()
    assert (site.root / "design" / "node-w1.md").exists()
    states = {r.scope_id: r.state for r in await manager.list_scopes()}
    assert states["node-b"] is ScopeState.INTEGRATED
    assert (site.repository.workspaces_dir / "node-b").exists()


async def test_recovery_lets_retraction_accept_released_empty_integration(tmp_path: Path) -> None:
    """默认释放下的空集成作用域重启后仍可作回退目标：直接废弃换代，线不动。"""
    site = _CrashSite(tmp_path)
    await site.build()
    await site.prepare("node-empty")
    await site.integrate("node-empty")
    manager, _report = await site.rebuild()

    outcome = await manager.retract_integration_line(
        targets=(RetractTarget(scope_id="node-empty", base_commit=site.bases["node-empty"]),),
        expected_root_epoch=1,
        client_op_id="retract-empty-after-recovery",
    )

    assert outcome.new_root_epoch == 1
    assert outcome.preserved == ()
    assert outcome.invalidated == (("node-empty", 2),)


async def test_recovery_reloads_every_scope_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """各状态的作用域重启后逐字段与重启前一致：状态、代次、属主、分支与遍历序。"""
    site = _CrashSite(tmp_path)
    await site.build()
    # 待集成：集成在登记意图前崩溃，状态已转待集成、没有挂起操作
    await site.prepare("node-aw")
    monkeypatch.setattr(SagaOplog, "begin", _exploding)
    with pytest.raises(RuntimeError, match="崩溃注入"):
        await site.orchestrator.integrate("node-aw")
    monkeypatch.undo()
    # 冲突（与 node-c1 改同一文件）；保留工作区的已集成；带属主的子代理与 reader 快照
    ws_c1 = await site.prepare("node-c1")
    ws_c2 = await site.prepare("node-c2")
    (ws_c1 / "design" / "x.md").write_text("from-c1\n", encoding="utf-8")
    (ws_c2 / "design" / "x.md").write_text("from-c2\n", encoding="utf-8")
    await site.integrate("node-c1")
    rejected = await site.orchestrator.integrate("node-c2")
    assert isinstance(rejected, ScopeIntegrationRejected)
    await site.prepare("node-keep")
    (site.repository.workspaces_dir / "node-keep" / "design" / "keep.md").write_text(
        "keep\n", encoding="utf-8"
    )
    await site.orchestrator.integrate("node-keep")
    await site.prepare(
        "node-w2-c1", parent_scope_id="node-w2", scope_kind=ScopeKind.SUBAGENT, owner_ref="child-1"
    )
    await site.manager.prepare_reader_workspace(
        "node-w2", "node-w2-r1", base_commit=site.scope_bases["node-w2"]
    )
    # 废弃后同键重建：代次沿用 2，工作区与分支按代次后缀另起
    await site.prepare("node-rb")
    await site.orchestrator.abandon("node-rb", 1)
    await site.prepare("node-rb")
    before = await site.manager.list_scopes()

    manager, report = await site.rebuild()

    # 遍历序（根最先、各父作用域的子作用域按建区序）与重启前一致
    assert await manager.list_scopes() == before
    # 表里的行与内存账本一致（表按建区序存放，不按父子遍历序；闸门状态列按稳态看）
    rows = await site.reloaded_ledger.list_for_tree()
    assert {row.scope_id: at_rest(row) for row in rows} == {r.scope_id: r for r in before}
    by_id = {r.scope_id: r for r in before}
    assert by_id["node-rb"].scope_epoch == 2
    assert by_id["node-rb"].workspace_ref == (
        f"{(site.repository.workspaces_dir / 'node-rb-e2').as_posix()}|scope/node-rb-e2"
    )
    assert manager.is_reader_scope("node-w2-r1")
    assert report.degraded == ["活动组内存态未恢复：恢复后的首个副作用将开新组"]


async def test_recovery_keeps_epochs_after_completed_retraction(tmp_path: Path) -> None:
    """回退完成后重启：失效作用域的代次是回退后的值，根代次随线推进。"""
    site = _CrashSite(tmp_path)
    await site.build()
    ws_d = await site.prepare("node-d")
    (ws_d / "design" / "d.md").write_text("from-d\n", encoding="utf-8")
    await site.integrate("node-d")
    await site.manager.retract_integration_line(
        targets=(RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),),
        expected_root_epoch=1,
        client_op_id="retract-done",
    )
    before = await site.manager.list_scopes()
    by_id = {r.scope_id: r for r in before}
    assert (by_id["node-a"].scope_epoch, by_id["node-d"].scope_epoch) == (2, 2)

    manager, report = await site.rebuild()

    assert report.completed_op_ids == []
    assert await manager.list_scopes() == before
    assert report.root_epoch == 2


async def test_recovery_completes_retract_ledger_after_reset_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """崩溃在 reset 后、epoch 落账前：按已记新头核对补账（代次与失效废弃补齐）。"""
    site = _CrashSite(tmp_path)
    await site.build()
    # 下游 d 在 a 集成后建区并集成（基线谱系含 a 的集成提交 → 失效面）
    ws_d = await site.prepare("node-d")
    (ws_d / "design" / "d.md").write_text("from-d\n", encoding="utf-8")
    await site.integrate("node-d")

    async def exploding(self: ScopeTree, *, expected_root_epoch: int) -> int:
        raise RuntimeError("崩溃注入：reset 后 epoch 落账前")

    monkeypatch.setattr(ScopeTree, "advance_root_epoch", exploding)
    with pytest.raises(RuntimeError, match="崩溃注入"):
        await site.retract_a()
    monkeypatch.undo()

    # 崩溃现场：引用与根检出已回退（a/d 的产物撤回、b 的保留），账面代次未落
    assert not (site.root / "design" / "node-a.md").exists()
    assert not (site.root / "design" / "d.md").exists()
    assert (site.root / "design" / "node-b.md").exists()

    manager, report = await site.rebuild()

    assert report.completed_op_ids == ["retract-crash"]
    assert report.root_epoch == 2
    states = {r.scope_id: (r.state, r.scope_epoch) for r in await manager.list_scopes()}
    # 谱系失效集补废弃：目标 a 与下游 d 失效换代；同基线兄弟 b/w1 保留不动
    assert states["node-a"] == (ScopeState.DISCARDED, 2)
    assert states["node-d"] == (ScopeState.DISCARDED, 2)
    assert states["node-b"][0] is ScopeState.RELEASED
    assert states["node-w1"][0] is ScopeState.RELEASED
    # 基线干净的活动写者不在失效面：工作区保留、状态活动
    assert states["node-w2"][0] is ScopeState.ACTIVE
    assert (site.repository.workspaces_dir / "node-w2").exists()


async def test_recovery_completes_retract_after_partial_invalidation_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """回退在失效集废弃到一半时崩溃：已废弃的作用域不重复换代，其余照常补账。"""
    site = _CrashSite(tmp_path)
    await site.build()
    ws_d = await site.prepare("node-d")
    (ws_d / "design" / "d.md").write_text("from-d\n", encoding="utf-8")
    await site.integrate("node-d")
    operation = await site.crash_at(
        monkeypatch,
        WorkspaceManager,
        "_release_workspace_if_present",
        site.retract_a,
    )
    assert operation.phase is OperationPhase.GIT_WRITTEN
    # 崩溃现场：首个失效作用域的账已落、工作区未移除，线与根代次已推进
    assert (await _record(site.manager, "node-a")).state is ScopeState.DISCARDED
    assert (await _record(site.manager, "node-d")).state is ScopeState.RELEASED
    assert (await _record(site.manager, _ROOT_SCOPE_ID)).root_epoch == 2

    manager, report = await site.rebuild()

    assert report.completed_op_ids == ["retract-crash"]
    assert report.root_epoch == 2
    states = {r.scope_id: (r.state, r.scope_epoch) for r in await manager.list_scopes()}
    assert states["node-a"] == (ScopeState.DISCARDED, 2)
    assert states["node-d"] == (ScopeState.DISCARDED, 2)
    assert not (site.repository.workspaces_dir / "node-a").exists()
    assert not (site.repository.workspaces_dir / "node-d").exists()
    # 重放命中已收口缓存：失效清单列全（崩溃前已废弃的也在内）
    replay = await manager.retract_integration_line(
        targets=(RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),),
        expected_root_epoch=1,
        client_op_id="retract-crash",
    )
    assert {scope for scope, _epoch in replay.invalidated} == {"node-a", "node-d"}


async def test_recovery_completes_multi_target_retract_after_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """多目标回退在 reset 后、代次落账前崩溃：补账收口后同操作号重放返回原结果。"""
    site = _CrashSite(tmp_path)
    await site.build()
    ws_d = await site.prepare("node-d")
    (ws_d / "design" / "d.md").write_text("from-d\n", encoding="utf-8")
    await site.integrate("node-d")
    targets = (
        RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),
        RetractTarget(scope_id="node-b", base_commit=site.bases["node-b"]),
    )

    async def exploding(self: ScopeTree, *, expected_root_epoch: int) -> int:
        raise RuntimeError("崩溃注入：reset 后 epoch 落账前")

    monkeypatch.setattr(ScopeTree, "advance_root_epoch", exploding)
    with pytest.raises(RuntimeError, match="崩溃注入"):
        await site.manager.retract_integration_line(
            targets=targets, expected_root_epoch=1, client_op_id="retract-multi-crash"
        )
    monkeypatch.undo()

    # 崩溃现场：引用与根检出已回退（a/b/d 的产物撤回、保留者 w1 的产出在）
    for name in ("node-a.md", "node-b.md", "node-d.md"):
        assert not (site.root / "design" / name).exists()
    assert (site.root / "design" / "node-w1.md").exists()

    manager, report = await site.rebuild()
    assert report.completed_op_ids == ["retract-multi-crash"]
    assert report.root_epoch == 2
    states = {r.scope_id: (r.state, r.scope_epoch) for r in await manager.list_scopes()}
    # 两个目标与谱系吸收的下游 d 失效换代；同基线兄弟 w1 保留不动
    assert states["node-a"] == (ScopeState.DISCARDED, 2)
    assert states["node-b"] == (ScopeState.DISCARDED, 2)
    assert states["node-d"] == (ScopeState.DISCARDED, 2)
    assert states["node-w1"][0] is ScopeState.RELEASED
    assert states["node-w2"][0] is ScopeState.ACTIVE

    # 重载后的操作账命中已收口缓存：规范形目标集一致即返回原结果
    replay = await manager.retract_integration_line(
        targets=(targets[1], targets[0]), expected_root_epoch=1, client_op_id="retract-multi-crash"
    )
    assert replay.new_root_epoch == 2
    assert {scope for scope, _epoch in replay.invalidated} == {
        "node-a",
        "node-b",
        "node-d",
    }


async def test_recovery_completes_empty_only_retract_without_epoch_advance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """仅空集成目标（线不动）在废弃前崩溃：补齐废弃换代，根代次不推进。"""
    site = _CrashSite(tmp_path)
    await site.build()
    await site.prepare("node-empty")
    outcome = await site.orchestrator.integrate("node-empty")
    assert isinstance(outcome, ScopeIntegrated)

    operation = await site.crash_at(
        monkeypatch,
        WorkspaceManager,
        "_abandon_scope_physically",
        lambda: site.manager.retract_integration_line(
            targets=(RetractTarget(scope_id="node-empty", base_commit=site.bases["node-empty"]),),
            expected_root_epoch=1,
            client_op_id="retract-empty-crash",
        ),
    )
    assert operation.phase is OperationPhase.GIT_WRITTEN

    manager, report = await site.rebuild()

    assert report.completed_op_ids == ["retract-empty-crash"]
    assert report.root_epoch == 1  # 线不动：恢复后根代次仍为 1
    states = {r.scope_id: (r.state, r.scope_epoch) for r in await manager.list_scopes()}
    assert states["node-empty"] == (ScopeState.DISCARDED, 2)
    assert not (site.repository.workspaces_dir / "node-empty").exists()
    # 重放命中已落定缓存：返回原结果、根代次不变
    replay = await manager.retract_integration_line(
        targets=(RetractTarget(scope_id="node-empty", base_commit=site.bases["node-empty"]),),
        expected_root_epoch=1,
        client_op_id="retract-empty-crash",
    )
    assert replay.new_root_epoch == 1
    assert {scope for scope, _epoch in replay.invalidated} == {"node-empty"}


@pytest.mark.parametrize("abandoned_before", [False, True], ids=["first-attempt", "rebuilt"])
async def test_recovery_completes_prepare_written_before_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, abandoned_before: bool
) -> None:
    """建区崩溃在已记新头后、账本落账前：按作用域分支核对补账，不误报分叉。

    废弃后同键重建（rebuilt）的新工作区按代次另起分支，分支名只能取自建
    区意图。两种情况都只收口操作、不补行：半成品作用域不进账本。
    """
    site = _CrashSite(tmp_path)
    await site.build()
    key = "node-new"
    if abandoned_before:
        await site.prepare(key)
        await site.orchestrator.abandon(key, 1)
    ledger_step = "begin_attempt" if abandoned_before else "create_child_scope"

    operation = await site.crash_at(
        monkeypatch, ScopeTree, ledger_step, lambda: site.orchestrator.prepare(key)
    )
    assert operation.phase is OperationPhase.GIT_WRITTEN

    manager, report = await site.rebuild()

    assert report.completed_op_ids == [operation.client_op_id]
    assert report.abandoned_op_ids == []
    if abandoned_before:
        assert (await _record(manager, key)).state is ScopeState.DISCARDED
        assert (await _record(manager, key)).scope_epoch == 2
    else:
        assert await manager.get_scope(key) is None


async def test_recovery_abandons_prepare_before_new_head_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """子作用域建区崩溃在工作区建成、新头未记时：父作用域分支头仍是旧头，判未生效放弃。"""
    site = _CrashSite(tmp_path)
    await site.build()
    operation = await site.crash_at(
        monkeypatch,
        SagaOplog,
        "record_new_head",
        lambda: site.orchestrator.prepare(
            "node-w2-c1", parent_scope_id="node-w2", scope_kind=ScopeKind.SUBAGENT
        ),
    )
    assert operation.phase is OperationPhase.PREPARED

    manager, report = await site.rebuild()

    assert report.abandoned_op_ids == [operation.client_op_id]
    assert report.completed_op_ids == []
    assert await manager.get_scope("node-w2-c1") is None


@pytest.mark.parametrize("written", [False, True], ids=["before-reset", "after-record"])
async def test_recovery_reconciles_restore_against_scope_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, written: bool
) -> None:
    """组检查点恢复按作用域自己的分支对账：reset 前崩溃判未生效放弃，已记新头补代次。"""
    site = _CrashSite(tmp_path)
    await site.build()
    crash_target = ScopeTree if written else WorkspaceRepository
    crash_step = "restore_scope" if written else "reset_workspace_state"

    operation = await site.crash_at(
        monkeypatch,
        crash_target,
        crash_step,
        lambda: site.manager.restore_scope(
            "node-w2",
            restore_commit=site.scope_bases["node-w2"],
            expected_scope_epoch=1,
            client_op_id="restore-crash",
        ),
    )
    assert operation.phase is (OperationPhase.GIT_WRITTEN if written else OperationPhase.PREPARED)

    manager, report = await site.rebuild()

    assert report.completed_op_ids == (["restore-crash"] if written else [])
    assert report.abandoned_op_ids == ([] if written else ["restore-crash"])
    # 已记新头：目标换代保持活动；未生效：代次不动
    restored = await _record(manager, "node-w2")
    assert (restored.state, restored.scope_epoch) == (ScopeState.ACTIVE, 2 if written else 1)


@pytest.mark.parametrize("under_scope", [False, True], ids=["parent-root", "parent-scope"])
@pytest.mark.parametrize(
    ("crash_target", "crash_step", "phase", "completed"),
    [
        (IntegrationCoordinator, "integrate_writer", OperationPhase.PREPARED, False),
        (SagaOplog, "record_new_head", OperationPhase.PREPARED, True),
        (SagaOplog, "commit", OperationPhase.GIT_WRITTEN, True),
    ],
    ids=["before-commit", "after-commit", "after-record"],
)
async def test_recovery_reconciles_integration_against_parent_ref(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    under_scope: bool,
    crash_target: type,
    crash_step: str,
    phase: OperationPhase,
    completed: bool,
) -> None:
    """集成按父作用域的引用对账（父为根即集成线）。

    未落提交判未生效放弃，作用域留在待集成；落了提交未记新头按判据补记
    新头；已记新头只补账。核对通过的作用域行转已集成。
    """
    site = _CrashSite(tmp_path)
    await site.build()
    key = "node-w2"
    if under_scope:
        key = "node-w2-c1"
        ws_child = await site.prepare(key, parent_scope_id="node-w2", scope_kind=ScopeKind.SUBAGENT)
        (ws_child / "design" / "c1.md").write_text("from-c1\n", encoding="utf-8")

    operation = await site.crash_at(
        monkeypatch, crash_target, crash_step, lambda: site.orchestrator.integrate(key)
    )
    assert operation.phase is phase

    manager, report = await site.rebuild()

    assert report.completed_op_ids == ([operation.client_op_id] if completed else [])
    assert report.abandoned_op_ids == ([] if completed else [operation.client_op_id])
    expected = ScopeState.INTEGRATED if completed else ScopeState.AWAITING_INTEGRATION
    assert (await _record(manager, key)).state is expected


@pytest.mark.parametrize("action", ["abandon", "release"], ids=["abandon", "release"])
async def test_recovery_prunes_workspace_left_after_ledger_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    """废弃/释放在账面落表后、工作区移除前崩溃：行已是终态，恢复按账移除残留工作区。"""
    site = _CrashSite(tmp_path)
    await site.build()
    key = "node-w2" if action == "abandon" else "node-w1"
    if action == "release":
        await site.prepare("node-keep-ws")
        outcome = await site.orchestrator.integrate("node-keep-ws")
        assert isinstance(outcome, ScopeIntegrated)
        key = "node-keep-ws"
    workspace = site.repository.workspaces_dir / key
    do = (
        (lambda: site.orchestrator.abandon(key, 1))
        if action == "abandon"
        else (lambda: site.manager.release_scope(key))
    )

    async def exploding(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("崩溃注入：账面已落、工作区移除前")

    monkeypatch.setattr(WorkspaceManager, "_release_workspace_if_present", exploding)
    with pytest.raises(RuntimeError, match="崩溃注入"):
        await do()
    monkeypatch.undo()
    expected_state = ScopeState.DISCARDED if action == "abandon" else ScopeState.RELEASED
    crashed = await _record(site.manager, key)
    assert (crashed.state, workspace.exists()) == (expected_state, True)

    manager, report = await site.rebuild()

    assert report.completed_op_ids == []
    assert (await _record(manager, key)).state is expected_state
    assert not workspace.exists()
    # 分支保留：已集成的仍可作回退目标
    assert await site.repository.resolve_ref(f"refs/heads/scope/{key}") is not None


@pytest.mark.parametrize("gone", ["workspace", "branch"], ids=["workspace", "branch"])
async def test_recovery_fails_when_live_workspace_is_gone(tmp_path: Path, gone: str) -> None:
    """活动作用域的工作区目录或分支不在盘：作用域损坏，显式失败，不推断成已废弃。"""
    site = _CrashSite(tmp_path)
    await site.build()
    if gone == "workspace":
        shutil.rmtree(site.repository.workspaces_dir / "node-w2")
    else:
        # 分支被工作区检出，branch -D 会被拒：按底层引用删除
        run_git(["update-ref", "-d", "refs/heads/scope/node-w2"], site.repository.repo_dir)

    with pytest.raises(WorkspaceRecoveryError, match="作用域损坏：node-w2"):
        await site.rebuild()


async def test_recovery_resets_gate_state_left_by_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """闸门内发生的字段落表把静止态带进行里：崩溃后重建归运行并写回，此后闸门照常可用。"""
    site = _CrashSite(tmp_path)
    await site.build()
    operation = await site.crash_at(
        monkeypatch, SagaOplog, "commit", lambda: site.orchestrator.integrate("node-w2")
    )
    assert operation.phase is OperationPhase.GIT_WRITTEN
    # 崩溃现场：集成在闸门内把行转已集成，行里的闸门状态随这次落表停在已静止
    crashed = await site.ledger.list_for_tree()
    assert [(r.state, r.gate_state) for r in crashed if r.scope_id == "node-w2"] == [
        (ScopeState.INTEGRATED, ScopeGateState.QUIESCED)
    ]

    manager, _report = await site.rebuild()

    assert (await _record(manager, "node-w2")).gate_state is ScopeGateState.RUNNING
    reloaded = await site.reloaded_ledger.list_for_tree()
    assert all(row.gate_state is ScopeGateState.RUNNING for row in reloaded)
    # 重建后的树照常执行带闸门的动作
    await manager.prepare_scope_workspace(
        PrepareScopeWorkspace(
            parent_scope_id=_ROOT_SCOPE_ID,
            scope_key="node-after",
            spec=ScopeWorkspaceSpec(scope_kind=ScopeKind.MAIN_WRITER),
            client_op_id="prepare-after",
        )
    )
    assert (await _record(manager, "node-after")).state is ScopeState.ACTIVE


async def test_recovery_fails_when_root_row_is_missing(tmp_path: Path) -> None:
    """盘上有树而账本表没有根行：建树未完成，显式失败。"""
    site = _CrashSite(tmp_path)
    await site.build()
    with sqlite3.connect(site.db_path) as conn:
        conn.execute("DELETE FROM scopes WHERE scope_id = ?", (_ROOT_SCOPE_ID,))

    with pytest.raises(WorkspaceRecoveryError, match="建树未完成"):
        await site.rebuild()


@pytest.mark.parametrize(
    ("op_kind", "ref"),
    [("prepare", "refs/heads/scope/node-fork"), ("integrate", ROOT_INTEGRATION_REF)],
)
async def test_recovery_reports_fork_on_operation_ref_without_reset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, op_kind: str, ref: str
) -> None:
    """操作所在引用的头被后续提交推进：停止并报告分叉，不补账、不回退。"""
    site = _CrashSite(tmp_path)
    await site.build()
    if op_kind == "prepare":
        operation = await site.crash_at(
            monkeypatch,
            ScopeTree,
            "create_child_scope",
            lambda: site.orchestrator.prepare("node-fork"),
        )
    else:
        operation = await site.crash_at(
            monkeypatch, SagaOplog, "commit", lambda: site.orchestrator.integrate("node-w2")
        )
    head = await site.repository.resolve_ref(ref)
    fork = run_git(
        ["commit-tree", f"{head}^{{tree}}", "-p", head or "", "-m", "forked by stranger"],
        site.repository.repo_dir,
    ).strip()
    run_git(["update-ref", ref, fork], site.repository.repo_dir)

    with pytest.raises(WorkspaceRecoveryError, match="分叉") as excinfo:
        await site.rebuild()

    assert ref in str(excinfo.value)
    # 引用仍停在被推进后的提交上，已记新头原样保留
    assert await site.repository.resolve_ref(ref) == fork
    reloaded = await SagaOplog(site.repository.tree_dir).get(operation.client_op_id)
    assert reloaded is not None
    assert reloaded.phase is OperationPhase.GIT_WRITTEN
    assert reloaded.actual_new_head == operation.actual_new_head


async def test_recovery_stops_when_operation_branch_is_gone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """已写建区的作用域分支被删除：无从核对，显式失败，不按分叉或放弃处理。"""
    site = _CrashSite(tmp_path)
    await site.build()
    operation = await site.crash_at(
        monkeypatch,
        ScopeTree,
        "create_child_scope",
        lambda: site.orchestrator.prepare("node-gone"),
    )
    # 同键残留清理会一并删掉工作树与分支
    await site.repository.prune_scope_residue(
        site.repository.workspaces_dir / "node-gone", "node-gone"
    )

    with pytest.raises(WorkspaceRecoveryError, match="引用不存在") as excinfo:
        await site.rebuild()

    assert "refs/heads/scope/node-gone" in str(excinfo.value)
    reloaded = await SagaOplog(site.repository.tree_dir).get(operation.client_op_id)
    assert reloaded is not None
    assert reloaded.phase is OperationPhase.GIT_WRITTEN


async def test_recovery_stops_when_operation_ref_cannot_be_located(tmp_path: Path) -> None:
    """操作意图定位不到所在引用（缺父作用域、作用域不在账本内）：显式失败。"""
    site = _CrashSite(tmp_path)
    await site.build()
    await SagaOplog(site.repository.tree_dir).begin(
        client_op_id="orphan-integrate",
        op_kind="integrate_scope",
        expected_old_head="h0",
        scope_epoch=1,
        root_epoch=1,
    )

    with pytest.raises(WorkspaceRecoveryError, match="无法定位"):
        await site.rebuild()


async def test_recovery_reports_fork_and_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """对不上账（线头既非旧头也非已记新头）：显式失败，绝不自动回退。"""
    site = _CrashSite(tmp_path)
    await site.build()

    async def exploding(self: ScopeTree, *, expected_root_epoch: int) -> int:
        raise RuntimeError("崩溃注入：reset 后 epoch 落账前")

    monkeypatch.setattr(ScopeTree, "advance_root_epoch", exploding)
    with pytest.raises(RuntimeError, match="崩溃注入"):
        await site.retract_a()
    monkeypatch.undo()
    # 分叉注入：在旧头之上再落一个无关提交并把引用拨过去
    head = await site.manager.current_integration_head()
    fork = run_git(
        ["commit-tree", f"{head}^{{tree}}", "-p", head or "", "-m", "forked by stranger"],
        site.repository.repo_dir,
    ).strip()
    run_git(["update-ref", ROOT_INTEGRATION_REF, fork], site.repository.repo_dir)

    with pytest.raises(WorkspaceRecoveryError, match="分叉即停"):
        await site.rebuild()
