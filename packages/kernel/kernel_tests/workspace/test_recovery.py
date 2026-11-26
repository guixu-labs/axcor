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

"""深度恢复对账测试：树运行时重建（检查点行 + 私有仓事实）与操作日志三态核对。

现场以真实 git 与检查点账构造「已集成 / 已释放 / 活动 / 已废弃」四类
作用域后丢弃内存账本，断言重建结果、回退崩溃补账（reset 后 epoch 落账
前）、未生效放弃与分叉即停。
"""

from __future__ import annotations

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
    ScopeWorkspaceSpec,
)
from workflow_controller.core.workspace.integration import IntegrationCoordinator
from workflow_controller.core.workspace.manager import RetractTarget, WorkspaceManager
from workflow_controller.core.workspace.manifest import WorkspaceManifest
from workflow_controller.core.workspace.oplog import SagaOplog
from workflow_controller.core.workspace.recovery import (
    WorkspaceRecoveryError,
    rebuild_workspace_tree,
)
from workflow_controller.core.workspace.repository import (
    ROOT_INTEGRATION_REF,
    WorkspaceRepository,
)
from workflow_controller.core.workspace.scope_tree import ScopeKind, ScopeState, ScopeTree

from ._support import FakeOrchestrator, build_workspace, run_git

if TYPE_CHECKING:
    from workflow_controller.core.workspace.recovery import WorkspaceRecoveryReport

_ROOT_SCOPE_ID = "run-1-root"


class _CrashSite:
    """带检查点账的谱系现场：桥式落 scope_start 行（崩溃重建的事实源）。"""

    def __init__(self, tmp_path: Path) -> None:
        self.root, self.repository, self.manager = build_workspace(tmp_path)
        self.orchestrator = FakeOrchestrator(self.manager)
        self.db_path = tmp_path / "events.db"
        self.checkpoints = CheckpointStore(self.db_path)
        self.bases: dict[str, str] = {}
        self._row_seq = 0

    async def build(self) -> None:
        """铺检查点表后建现场：w1（集成后释放）、w2（活动带声明）、w3（废弃）。"""
        await RunStore(self.db_path).list_runs()  # 统一迁移铺 scope_checkpoints 表
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

    async def prepare(self, key: str, *, claims: tuple[str, ...] = ()) -> Path:
        prepared = await self.orchestrator.prepare(key, claims=claims)
        self.bases[key] = prepared.integration_base_commit
        self._row_seq += 1
        await self.checkpoints.record(
            ScopeCheckpoint(
                checkpoint_id=f"{key}:scope_start:e1",
                scope_id=key,
                scope_kind=ScopeKind.MAIN_WRITER,
                parent_scope_id=_ROOT_SCOPE_ID,
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
        # 桥的默认收口：集成成功即释放工作区（分支保留）——重建面的
        # 状态推断依赖此事实（工作区在盘即活动）
        await self.manager.release_scope(key)

    async def retract_a(self) -> None:
        await self.manager.retract_integration_line(
            targets=(RetractTarget(scope_id="node-a", base_commit=self.bases["node-a"]),),
            expected_root_epoch=1,
            client_op_id="retract-crash",
        )

    async def rebuild(self) -> tuple[WorkspaceManager, WorkspaceRecoveryReport]:
        """丢弃内存账本后以全新部件重建（重开/崩溃后的树运行时再生）。"""
        repository = WorkspaceRepository(self.root, tree_id="run-1")
        scope_tree = ScopeTree()
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
            root_scope_id=_ROOT_SCOPE_ID,
        )
        return manager, report


async def test_recovery_rebuilds_ledger_from_checkpoints_and_repo(tmp_path: Path) -> None:
    """正常重建：状态按盘上事实推断、代次取行、claims 复登记、降级显式报告。"""
    site = _CrashSite(tmp_path)
    await site.build()

    manager, report = await site.rebuild()

    states = {r.scope_id: (r.state, r.scope_epoch) for r in await manager.list_scopes()}
    # 活动推断：工作区在盘；已进线推断：已集成（释放与集成在重建面等价收敛）
    assert states["node-w2"][0] is ScopeState.ACTIVE
    assert states["node-w1"][0] is ScopeState.INTEGRATED
    assert states["node-a"][0] is ScopeState.INTEGRATED
    assert states["node-b"][0] is ScopeState.INTEGRATED
    # 未进线且无工作区：已废弃
    assert states["node-w3"][0] is ScopeState.DISCARDED
    assert report.root_epoch == 1
    assert set(report.rebuilt_scope_ids) == {
        _ROOT_SCOPE_ID,
        "node-a",
        "node-b",
        "node-w1",
        "node-w2",
        "node-w3",
    }
    assert report.claims_restored == ["node-w2"]
    assert any("活动组内存态" in note for note in report.degraded)
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


async def test_recovery_completes_retract_ledger_after_reset_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """崩溃在 reset 后、epoch 落账前：按已记新头核对补账（代次与失效废弃补齐）。"""
    from workflow_controller.core.workspace.scope_tree import ScopeTree

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
    assert states["node-b"][0] is ScopeState.INTEGRATED
    assert states["node-w1"][0] is ScopeState.INTEGRATED
    # 基线干净的活动写者不在失效面：工作区保留、状态活动
    assert states["node-w2"][0] is ScopeState.ACTIVE
    assert (site.repository.workspaces_dir / "node-w2").exists()


async def test_recovery_completes_multi_target_retract_after_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """多目标回退在 reset 后、代次落账前崩溃：补账收口后同操作号重放返回原结果。"""
    from workflow_controller.core.workspace.scope_tree import ScopeTree

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
    assert states["node-w1"][0] is ScopeState.INTEGRATED
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
    from workflow_controller.core.workspace.scope_tree import AbandonedScope

    site = _CrashSite(tmp_path)
    await site.build()
    # 空集成为真实 git 事实：不释放工作区（保持活动态在盘，补账废弃面）
    await site.prepare("node-empty")
    outcome = await site.orchestrator.integrate("node-empty")
    assert isinstance(outcome, ScopeIntegrated)

    async def exploding(self: WorkspaceManager, target: object) -> list[AbandonedScope]:
        raise RuntimeError("崩溃注入：线记录后废弃前")

    monkeypatch.setattr(WorkspaceManager, "_abandon_scope_physically", exploding)
    with pytest.raises(RuntimeError, match="崩溃注入"):
        await site.manager.retract_integration_line(
            targets=(RetractTarget(scope_id="node-empty", base_commit=site.bases["node-empty"]),),
            expected_root_epoch=1,
            client_op_id="retract-empty-crash",
        )
    monkeypatch.undo()

    manager, report = await site.rebuild()

    assert report.completed_op_ids == ["retract-empty-crash"]
    assert report.root_epoch == 1  # 线不动：恢复后根代次仍为 1
    states = {r.scope_id: (r.state, r.scope_epoch) for r in await manager.list_scopes()}
    assert states["node-empty"] == (ScopeState.DISCARDED, 2)
    # 重放命中已落定缓存：返回原结果、根代次不变
    replay = await manager.retract_integration_line(
        targets=(RetractTarget(scope_id="node-empty", base_commit=site.bases["node-empty"]),),
        expected_root_epoch=1,
        client_op_id="retract-empty-crash",
    )
    assert replay.new_root_epoch == 1
    assert {scope for scope, _epoch in replay.invalidated} == {"node-empty"}


async def test_recovery_abandons_uneffective_prepared_op(tmp_path: Path) -> None:
    """意图未生效（头未动）：恢复即放弃，不重放。"""
    site = _CrashSite(tmp_path)
    await site.build()
    head = await site.manager.current_integration_head()
    await SagaOplog(site.repository.tree_dir).begin(
        client_op_id="stale-integrate",
        op_kind="integrate_scope",
        expected_old_head=head or "",
        scope_epoch=1,
        root_epoch=1,
    )

    _manager, report = await site.rebuild()

    assert report.abandoned_op_ids == ["stale-integrate"]
    assert report.completed_op_ids == []


async def test_recovery_reports_fork_and_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """对不上账（线头既非旧头也非已记新头）：显式失败，绝不自动回退。"""
    from workflow_controller.core.workspace.scope_tree import ScopeTree

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
