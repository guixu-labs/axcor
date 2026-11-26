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

"""工作区管理器：作用域工作区的生命周期编排、物化与回收。

建区经私有仓完成检出与物化、账本登记激活，全程走三阶段操作日志；
集成前先落父侧与写者侧的未提交状态，再交集成协调器按两档判定并
入父树；废弃在换代前先物理移除目标及后代工作区；释放把已集成或
已废弃的工作区裁剪掉、分支保留。本模块不直接调 git，一切 git 操
作经私有仓与集成协调器。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from workflow_controller.core.workspace.actions import (
    AbandonScope,
    IntegrateScope,
    IntegrationLineRetracted,
    PrepareScopeWorkspace,
    ScopeAbandoned,
    ScopeIntegrated,
    ScopeIntegrationRejected,
    ScopeRestored,
    ScopeWorkspacePrepared,
)
from workflow_controller.core.workspace.claims import (
    ClaimRejectedError,
    ClaimsRegistry,
)
from workflow_controller.core.workspace.gate import (
    GateState,
    QuiesceOutcome,
    ScopeGate,
)
from workflow_controller.core.workspace.integration import (
    IntegrationConflict,
    ReplayStep,
    WorkspaceTarget,
)
from workflow_controller.core.workspace.oplog import (
    OperationPhase,
    SagaOperation,
    operation_marker,
)
from workflow_controller.core.workspace.repository import ROOT_INTEGRATION_REF, ROOT_SEED_REF
from workflow_controller.core.workspace.scope_tree import (
    INITIAL_EPOCH,
    ScopeGateState,
    ScopeKind,
    ScopeState,
    ScopeTreeError,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Sequence

    from workflow_controller.core.workspace.integration import IntegrationCoordinator
    from workflow_controller.core.workspace.manifest import WorkspaceManifest
    from workflow_controller.core.workspace.oplog import SagaOplog
    from workflow_controller.core.workspace.repository import (
        RootRefs,
        ScopeWorkspace,
        WorkspaceRepository,
    )
    from workflow_controller.core.workspace.scope_tree import AbandonedScope, ScopeRecord, ScopeTree

__all__ = [
    "RetractTarget",
    "ScopeQuiesceFailedError",
    "WorkspaceManager",
    "WorkspaceManagerError",
]

# 后台写进程排空钩子：入参为作用域标识
BackgroundDrainHook = Callable[[str], Awaitable[None]]


@dataclass(frozen=True)
class RetractTarget:
    """集成线回退的单个撤回目标：作用域与其建区基线提交。"""

    scope_id: str
    base_commit: str


@dataclass(frozen=True)
class _RetractionPlan:
    """一次集成线回退的失效集与保留者重放序。"""

    invalidated: list[ScopeRecord]
    preserved: list[ReplayStep]


class WorkspaceManagerError(RuntimeError):
    """工作区编排拒绝：状态、代次或能力不满足时显式失败。"""


class ScopeQuiesceFailedError(WorkspaceManagerError):
    """作用域闸门不可静止：操作未执行，按故障收口。"""


class WorkspaceManager:
    """一棵作用域树的工作区编排入口。"""

    def __init__(
        self,
        *,
        repository: WorkspaceRepository,
        scope_tree: ScopeTree,
        oplog: SagaOplog,
        manifest: WorkspaceManifest,
        integration_coordinator: IntegrationCoordinator,
        drain_background_writers: BackgroundDrainHook | None = None,
        claims_enforce: bool = False,
    ) -> None:
        """装配编排所需的账本、日志、清单、协调器、排空钩子与 claims 档位。"""
        self._repository = repository
        self._tree = scope_tree
        self._oplog = oplog
        self._manifest = manifest
        self._coordinator = integration_coordinator
        self._drain_hook = drain_background_writers
        self._claims = ClaimsRegistry()
        self._claims_enforce = claims_enforce
        self._gates: dict[str, ScopeGate] = {}
        # reader 子代理作用域（detached worktree）：永不进集成线
        self._reader_scopes: set[str] = set()

    async def initialize_root(
        self,
        *,
        scope_id: str,
        owner_ref: str | None = None,
        root_workspace_dir: Path | None = None,
    ) -> RootRefs:
        """建立根作用域：导入基线、立根双引用并登记账本。

        root_workspace_dir 显式指定根检出落点（默认根目录本身）：单会话
        树的原目录须保持基线不动，集成线检出落在树目录内的内部目录。
        """
        refs = await self._repository.import_baseline(
            writer_local_pathspecs=self._manifest.git_writer_local_specs(),
        )
        await self._tree.create_root_scope(
            scope_id=scope_id,
            owner_ref=owner_ref,
            workspace_ref=(
                root_workspace_dir if root_workspace_dir is not None else self._repository.root_dir
            ).as_posix(),
            scope_base_commit=refs.seed_checkpoint,
            integration_base_commit=refs.integration_head,
        )
        return refs

    async def prepare_scope_workspace(
        self, request: PrepareScopeWorkspace
    ) -> ScopeWorkspacePrepared:
        """建子工作区：父闸门内自父当前头检出、按策略物化并登记激活。"""
        parent = await self._require_scope(request.parent_scope_id)
        if request.spec.inherit_from is not None:
            raise WorkspaceManagerError("继承绑定建区尚未开放：先经检查点账本解析")
        if request.spec.scope_kind is ScopeKind.ROOT:
            raise WorkspaceManagerError("根作用域须经初始化建立")
        if parent.state is not ScopeState.ACTIVE:
            raise WorkspaceManagerError(
                f"父作用域非活动态不能建子：{parent.scope_id}（{parent.state.value}）"
            )
        existing = await self._tree.get_scope(request.scope_key)
        if existing is not None and existing.state is not ScopeState.DISCARDED:
            raise WorkspaceManagerError(f"作用域标识已被占用：{request.scope_key}")
        epoch = existing.scope_epoch if existing is not None else INITIAL_EPOCH

        claims = tuple(request.spec.claims)
        if claims and self._claims_enforce:
            conflicts = self._claims.conflicts_for(request.scope_key, claims)
            if conflicts:
                raise ClaimRejectedError(request.scope_key, claims, conflicts)

        async with self._quiesced(parent.scope_id):
            if parent.scope_kind is ScopeKind.ROOT:
                base = await self._repository.resolve_ref(ROOT_INTEGRATION_REF)
                materialize_from = await self._repository.resolve_ref(ROOT_SEED_REF)
                materialize_specs = self._manifest.git_materialize_specs()
            else:
                base = await self._coordinator.commit_pending_state(
                    self._target_for(parent),
                    exclude_pathspecs=self._manifest.git_scope_commit_exclude_specs(),
                    message=f"parent state before preparing {request.scope_key}",
                )
                materialize_from = None
                materialize_specs = ()
            await self._oplog.begin(
                client_op_id=request.client_op_id,
                op_kind="prepare_scope_workspace",
                expected_old_head=base,
                scope_epoch=parent.scope_epoch,
                root_epoch=parent.root_epoch,
                metadata={"scope_id": request.scope_key, "parent_scope_id": parent.scope_id},
            )
            workspace = await self._create_workspace_guarded(
                scope_id=request.scope_key,
                epoch=epoch,
                base=base,
                materialize_from=materialize_from,
                materialize_specs=materialize_specs,
                client_op_id=request.client_op_id,
                workspace_dir=request.workspace_dir,
            )
            await self._oplog.record_new_head(request.client_op_id, workspace.base_commit)
            workspace_ref = self._format_workspace_ref(workspace)
            if existing is not None:
                await self._tree.begin_attempt(
                    request.scope_key,
                    workspace_ref=workspace_ref,
                    scope_base_commit=workspace.base_commit,
                    integration_base_commit=base,
                )
                record = await self._tree.activate_scope(
                    request.scope_key,
                    workspace_ref=workspace_ref,
                    scope_base_commit=workspace.base_commit,
                )
            else:
                await self._tree.create_child_scope(
                    scope_id=request.scope_key,
                    parent_scope_id=parent.scope_id,
                    scope_kind=request.spec.scope_kind,
                    integration_base_commit=base,
                    owner_ref=request.spec.owner_ref,
                )
                record = await self._tree.activate_scope(
                    request.scope_key,
                    workspace_ref=workspace_ref,
                    scope_base_commit=workspace.base_commit,
                )
            await self._oplog.commit(
                request.client_op_id,
                result={"scope_base_commit": workspace.base_commit, "branch": workspace.branch},
            )
        self._claims.register(request.scope_key, claims)
        return ScopeWorkspacePrepared(
            scope_id=record.scope_id,
            scope_epoch=record.scope_epoch,
            base_commit=workspace.base_commit,
            integration_base_commit=base,
            workspace_path=workspace.path.as_posix(),
        )

    async def integrate_scope(
        self, request: IntegrateScope
    ) -> ScopeIntegrated | ScopeIntegrationRejected:
        """串行集成：双侧闸门内先落各自未提交状态，再按两档判定并入父树。"""
        record = await self._require_scope(request.scope_id)
        if record.scope_id in self._reader_scopes:
            raise WorkspaceManagerError(f"reader 作用域永不集成：{record.scope_id}")
        if record.state is ScopeState.ACTIVE:
            record = await self._tree.set_state(record.scope_id, ScopeState.AWAITING_INTEGRATION)
        if record.state is not ScopeState.AWAITING_INTEGRATION:
            raise WorkspaceManagerError(
                f"作用域不在待集成态：{record.scope_id}（{record.state.value}）"
            )
        if record.parent_scope_id is None or record.integration_base_commit is None:
            raise WorkspaceManagerError(f"作用域缺少父引用或集成基线：{record.scope_id}")
        parent = await self._require_scope(record.parent_scope_id)
        await self._tree.require_baseline_current(record.scope_id)
        integration_base_commit = record.integration_base_commit
        writer_path, _writer_branch = self._require_workspace(record)
        writer_target = WorkspaceTarget(path=writer_path, separated_env=False)
        parent_target = self._target_for(parent)

        async with (
            self._quiesced(record.scope_id),
            self._quiesced(parent.scope_id),
        ):
            writer_head = await self._coordinator.commit_pending_state(
                writer_target,
                exclude_pathspecs=self._manifest.git_scope_commit_exclude_specs(),
                message=f"scope {record.scope_id} terminal state",
            )
            parent_head = await self._coordinator.commit_pending_state(
                parent_target,
                exclude_pathspecs=self._parent_state_exclude_pathspecs(parent),
                message=f"parent state before integrating {record.scope_id}",
            )
            await self._oplog.begin(
                client_op_id=request.client_op_id,
                op_kind="integrate_scope",
                expected_old_head=parent_head,
                scope_epoch=record.scope_epoch,
                root_epoch=record.root_epoch,
                metadata={
                    "scope_id": record.scope_id,
                    "parent_scope_id": parent.scope_id,
                    "writer_head": writer_head,
                    "integration_base_commit": integration_base_commit,
                },
            )
            outcome = await self._coordinator.integrate_writer(
                writer_scope_id=record.scope_id,
                parent_scope_id=parent.scope_id,
                base_commit=integration_base_commit,
                writer_head=writer_head,
                parent_head=parent_head,
                parent_target=parent_target,
                merge_pathspecs=self._manifest.git_diff_restriction_specs(),
                commit_message=(
                    f"integrate {record.scope_id}\n\n{operation_marker(request.client_op_id)}"
                ),
            )
            if isinstance(outcome, IntegrationConflict):
                await self._tree.set_state(record.scope_id, ScopeState.CONFLICTED)
                await self._oplog.abandon(request.client_op_id)
                self._claims.release(record.scope_id)
                return ScopeIntegrationRejected(
                    scope_id=record.scope_id,
                    parent_scope_id=parent.scope_id,
                    conflict=outcome,
                )
            await self._oplog.record_new_head(request.client_op_id, outcome.new_head)
            await self._tree.set_state(record.scope_id, ScopeState.INTEGRATED)
            self._claims.release(record.scope_id)
            await self._oplog.commit(
                request.client_op_id,
                result={
                    "new_parent_head": outcome.new_head,
                    "integrated_paths": list(outcome.integrated_paths),
                },
            )
            return ScopeIntegrated(
                scope_id=record.scope_id,
                parent_scope_id=parent.scope_id,
                new_parent_head=outcome.new_head,
                integrated_paths=outcome.integrated_paths,
            )

    async def prepare_reader_workspace(
        self, parent_scope_id: str, scope_key: str, *, base_commit: str
    ) -> ScopeWorkspacePrepared:
        """建 reader 子代理工作区：父作用域最近检查点 commit 的 detached 快照。

        无分支、永不集成；不占用闸门内提交（快照基点已发布）。严格模式
        下其写入不构成保证——只读性按审计约定处理（见 repository 同名方法）。
        """
        parent = await self._require_scope(parent_scope_id)
        if parent.state is not ScopeState.ACTIVE:
            raise WorkspaceManagerError(
                f"父作用域非活动态不能建 reader 快照：{parent_scope_id}（{parent.state.value}）"
            )
        existing = await self._tree.get_scope(scope_key)
        if existing is not None:
            raise WorkspaceManagerError(f"作用域标识已被占用：{scope_key}")
        workspace_path = await self._repository.create_reader_workspace(
            scope_key, commit=base_commit
        )
        await self._tree.create_child_scope(
            scope_id=scope_key,
            parent_scope_id=parent.scope_id,
            scope_kind=ScopeKind.SUBAGENT,
            integration_base_commit=base_commit,
            owner_ref=None,
        )
        record = await self._tree.activate_scope(
            scope_key,
            workspace_ref=f"{workspace_path.as_posix()}|",
            scope_base_commit=base_commit,
        )
        self._reader_scopes.add(scope_key)
        return ScopeWorkspacePrepared(
            scope_id=record.scope_id,
            scope_epoch=record.scope_epoch,
            base_commit=base_commit,
            integration_base_commit=base_commit,
            workspace_path=workspace_path.as_posix(),
        )

    async def restore_scope(
        self,
        scope_id: str,
        *,
        restore_commit: str,
        expected_scope_epoch: int,
        client_op_id: str,
    ) -> ScopeRestored:
        """组检查点恢复：闸门内 reset 到组恢复点并撤回全部组内副作用。

        物理序：先移除后代工作区，再闸门内落当前态、reset 到组恢复点；
        账面序：目标换代保持活动、后代废弃换代；全程三阶段记账。
        """
        record = await self._require_scope(scope_id)
        if record.scope_kind is ScopeKind.ROOT:
            raise WorkspaceManagerError("根作用域不经组检查点恢复")
        if record.state is not ScopeState.ACTIVE:
            raise WorkspaceManagerError(
                f"作用域非活动态不能恢复：{scope_id}（{record.state.value}）"
            )
        verified = await self._tree.require_scope_epoch(scope_id, expected_scope_epoch)
        for descendant in await self._tree.list_descendants(scope_id):
            if descendant.workspace_ref is None:
                continue
            path, _branch = self._parse_workspace_ref(descendant.workspace_ref)
            if path.exists():
                await self._coordinator.release_worktree(path)
        writer_path, _branch = self._require_workspace(verified)
        writer_target = WorkspaceTarget(path=writer_path, separated_env=False)
        async with self._quiesced(scope_id):
            old_head = await self._coordinator.commit_pending_state(
                writer_target,
                exclude_pathspecs=self._manifest.git_scope_commit_exclude_specs(),
                message=f"scope {scope_id} state before restore",
            )
            await self._oplog.begin(
                client_op_id=client_op_id,
                op_kind="restore_scope",
                expected_old_head=old_head,
                scope_epoch=verified.scope_epoch,
                root_epoch=verified.root_epoch,
                metadata={"scope_id": scope_id, "restore_commit": restore_commit},
            )
            await self._repository.reset_workspace_state(writer_path, restore_commit)
            await self._oplog.record_new_head(client_op_id, restore_commit)
            restored, abandoned = await self._tree.restore_scope(
                scope_id, expected_scope_epoch=expected_scope_epoch
            )
            for item in abandoned:
                self._claims.release(item.scope_id)
                self._reader_scopes.discard(item.scope_id)
            await self._oplog.commit(
                client_op_id,
                result={"restore_commit": restore_commit},
            )
        return ScopeRestored(
            scope_id=scope_id,
            new_scope_epoch=restored.new_scope_epoch,
            root_epoch=(await self._require_scope(scope_id)).root_epoch,
            abandoned_descendants=tuple(
                (item.scope_id, item.new_scope_epoch) for item in abandoned
            ),
        )

    async def retract_integration_line(
        self,
        *,
        targets: Sequence[RetractTarget],
        expected_root_epoch: int,
        client_op_id: str,
    ) -> IntegrationLineRetracted:
        """已合并回滚的集成线多目标回退：先校验后 reset，保留者按原集成序重放。

        回退点取线上有集成提交的各目标建区基线在首父链上的最早者——通过
        但未改动文件的目标线上无成果可撤，不参与回退点；目标集全部如此时
        线与根检出不动、root epoch 不推进。逐目标过全部校验条件（在册、
        根直接写者、已集成/已释放、提交在当前线上、是当前集成头的严格祖
        先、区间不含自建路径变更、根检出干净；空集成目标免线位置与区间
        校验），任一不满足即拒绝——根头、根工作树、根代次与账本零变化。
        失效集 = 目标集 ∪ 基线谱系含任一目标集成提交的写者（含基于失效基
        线的活动写者）；保留者中最晚一条集成提交晚于回退点的按原集成序重
        放（preserved 只列被重放者，空集成与回退点之前的保留者不在内）；
        重放链先在对象库内构造、记新头后再推进引用与根检出，根代次只随
        线动、失效作用域废弃随后落账、操作日志落定。
        """
        root_id = self._tree.root_scope_id
        if root_id is None:
            raise WorkspaceManagerError("作用域树尚未建立根作用域")
        if not targets:
            raise WorkspaceManagerError("撤回目标集为空（非法输入）")
        scope_ids = [target.scope_id for target in targets]
        if len(set(scope_ids)) != len(scope_ids):
            raise WorkspaceManagerError("撤回目标集含重复作用域（非法输入）")
        canonical = sorted([target.scope_id, target.base_commit] for target in targets)
        existing = await self._oplog.get(client_op_id)
        if existing is not None:
            return self._retract_cached_or_reject(
                existing,
                canonical_targets=canonical,
                expected_root_epoch=expected_root_epoch,
            )
        await self._tree.require_root_epoch(expected_root_epoch)
        current_head = await self._repository.resolve_ref(ROOT_INTEGRATION_REF)
        if current_head is None:
            raise WorkspaceManagerError("集成线引用缺失，无法回退")
        # 线事实一次解析共享：集成序（含各作用域的集成提交）与首父链
        order_pairs = await self._coordinator.line_integration_order(current_head)
        line_chain = await self._coordinator.first_parent_commits(current_head)
        line_integrated_scopes = {scope_id for scope_id, _commit in order_pairs}
        if not await self._coordinator.root_status_clean(
            self._manifest.git_line_commit_exclude_specs()
        ):
            raise WorkspaceManagerError("根检出存在未提交变化，先经集成落账再回退")
        for target in targets:
            await self._validate_retract_target(
                target, current_head, line_chain, line_integrated_scopes
            )
        retract_bases = [
            target.base_commit for target in targets if target.scope_id in line_integrated_scopes
        ]
        retract_commit = current_head
        if retract_bases:
            earliest = await self._coordinator.earliest_line_commit(current_head, retract_bases)
            if earliest is None:
                raise WorkspaceManagerError("撤回目标不在当前集成线上，无法定回退点")
            retract_commit = earliest
        plan = await self._plan_retraction(scope_ids, order_pairs, retract_commit)
        root = await self._require_scope(root_id)
        async with self._quiesced(root_id):
            # 闸门内复核：头与根代次仍与校验时一致，否则按分叉拒绝
            if await self._repository.resolve_ref(ROOT_INTEGRATION_REF) != current_head:
                raise WorkspaceManagerError("集成头在受理后发生变化，回退拒绝（分叉）")
            await self._tree.require_root_epoch(expected_root_epoch)
            await self._oplog.begin(
                client_op_id=client_op_id,
                op_kind="retract_integration_line",
                expected_old_head=current_head,
                scope_epoch=root.scope_epoch,
                root_epoch=expected_root_epoch,
                metadata={
                    "targets": canonical,
                    "target_commit": retract_commit,
                    "invalidated": [item.scope_id for item in plan.invalidated],
                    "preserved": [step.scope_id for step in plan.preserved],
                },
            )
            new_head = await self._coordinator.retract_line_to(
                target_commit=retract_commit,
                replay=plan.preserved,
                merge_pathspecs=self._manifest.git_diff_restriction_specs(),
                client_op_id=client_op_id,
            )
            await self._oplog.record_new_head(client_op_id, new_head)
            await self._coordinator.advance_root_checkout(new_head)
            # 根代次只随线动：线未动（目标集全为空集成）时不推进，
            # 在途写者基线仍属当前代次、集成不被旧代次拒绝
            if new_head != current_head:
                new_root_epoch = await self._tree.advance_root_epoch(
                    expected_root_epoch=expected_root_epoch
                )
            else:
                new_root_epoch = expected_root_epoch
            abandoned: list[tuple[str, int]] = []
            for record in plan.invalidated:
                for item in await self._abandon_scope_physically(record):
                    abandoned.append((item.scope_id, item.new_scope_epoch))
            result = IntegrationLineRetracted(
                new_root_head=new_head,
                new_root_epoch=new_root_epoch,
                target_commit=retract_commit,
                invalidated=tuple(abandoned),
                preserved=tuple(step.scope_id for step in plan.preserved),
            )
            await self._oplog.commit(
                client_op_id,
                result={
                    "new_root_head": new_head,
                    "new_root_epoch": new_root_epoch,
                    "target_commit": retract_commit,
                    "invalidated": abandoned,
                    "preserved": list(result.preserved),
                },
            )
        return result

    def _retract_cached_or_reject(
        self,
        existing: SagaOperation,
        *,
        canonical_targets: list[list[str]],
        expected_root_epoch: int,
    ) -> IntegrationLineRetracted:
        """同操作号重放：已收口按参数一致（规范形目标集）返回原结果，其余显式拒绝。"""
        metadata = existing.metadata
        consistent = (
            existing.op_kind == "retract_integration_line"
            and metadata.get("targets") == canonical_targets
            and existing.root_epoch == expected_root_epoch
        )
        if not consistent:
            raise WorkspaceManagerError(f"操作号已绑定不同的回退请求：{existing.client_op_id}")
        if existing.phase is not OperationPhase.COMMITTED:
            raise WorkspaceManagerError(
                f"回退操作未收口（{existing.phase.value}），须经崩溃恢复核对后重试"
            )
        payload = existing.result
        if not isinstance(payload, dict):
            raise WorkspaceManagerError(f"已收口回退缺少结果账：{existing.client_op_id}")
        return IntegrationLineRetracted(
            new_root_head=str(payload["new_root_head"]),
            new_root_epoch=int(payload["new_root_epoch"]),
            target_commit=str(payload["target_commit"]),
            invalidated=tuple((str(i[0]), int(i[1])) for i in payload.get("invalidated", ())),
            preserved=tuple(str(i) for i in payload.get("preserved", ())),
        )

    async def _validate_retract_target(
        self,
        target: RetractTarget,
        current_head: str,
        line_chain: set[str],
        line_integrated_scopes: set[str],
    ) -> None:
        """单个撤回目标校验：在册、属当前集成线、是当前头祖先、区间无自建变更。

        全部为只读判定——任一不满足时本次回退零改动（校验先于闸门与
        一切落账）。通过但未改动文件的目标在集成线上没有集成提交，免
        线位置与区间校验（其建区基线不作为回退点）。
        """
        target_scope_id = target.scope_id
        target_commit = target.base_commit
        record = await self._require_scope(target_scope_id)
        if record.parent_scope_id != self._tree.root_scope_id:
            raise WorkspaceManagerError(f"回退目标必须是集成线的直接写者：{target_scope_id}")
        if record.state not in (ScopeState.INTEGRATED, ScopeState.RELEASED):
            raise WorkspaceManagerError(
                f"回退目标未集成：{target_scope_id}（{record.state.value}）"
            )
        if not await self._coordinator.commit_exists(target_commit):
            raise WorkspaceManagerError(f"回退目标提交不存在于私有仓：{target_commit}")
        if target_scope_id not in line_integrated_scopes:
            # 线上无集成提交按空集成处理，须以分支事实核对（防解析错位）
            await self._verify_empty_integration(record)
            return
        if target_commit not in line_chain:
            raise WorkspaceManagerError(f"回退目标不属于当前集成线：{target_commit}")
        if target_commit == current_head or not await self._coordinator.is_ancestor(
            target_commit, current_head
        ):
            raise WorkspaceManagerError(f"回退目标不是当前集成头的（严格）祖先：{target_commit}")
        dirty_paths = await self._coordinator.changed_paths(
            target_commit,
            current_head,
            self._manifest.git_writer_local_specs(),
        )
        if dirty_paths:
            raise WorkspaceManagerError(
                f"回退区间含自建路径变更，拒绝回退：{sorted(dirty_paths)[:5]}"
            )

    async def _plan_retraction(
        self,
        target_scope_ids: Sequence[str],
        order_pairs: Sequence[tuple[str, str]],
        retract_commit: str,
    ) -> _RetractionPlan:
        """失效集计算（谱系判据）与保留者重放序解析。

        谱系基准是各撤回目标的集成提交（其成果进入集成线的那个提交，自
        线历史解析，同目标多次集成取最早），不是回退点——回退点只定线
        回退到哪。已集成写者的建区基线祖先链含任一目标集成提交即失效重
        跑；不含则保留，重放判据取其最晚一条集成提交（同目标多次集成、
        回退点居中时最早一条不代表线上现状）——最晚集成提交严格晚于回
        退点的按原集成序重放，其余（回退点之前的集成、线上无集成提交的
        空集成）原样保留不重放；基于失效基线的活动写者一并废弃。
        """
        root_id = self._tree.root_scope_id or ""
        target_set = set(target_scope_ids)
        invalidated: list[ScopeRecord] = []
        preserved: list[ReplayStep] = []
        order = {scope_id: index for index, (scope_id, _commit) in enumerate(order_pairs)}
        integration_commits: dict[str, str] = {}
        for scope_id, commit in order_pairs:
            # 集成序按时间正序，持续覆盖即取最晚一条（重放判据用）
            integration_commits[scope_id] = commit
        pivots: dict[str, str] = {}
        for scope_id, commit in order_pairs:
            if scope_id in target_set and scope_id not in pivots:
                pivots[scope_id] = commit
        for record in await self._tree.list_all_scopes():
            if record.parent_scope_id != root_id or record.scope_id in target_set:
                continue
            if record.scope_kind is not ScopeKind.MAIN_WRITER:
                continue
            base = record.integration_base_commit or ""
            consumed = bool(base) and await self._consumed_by_any_target(pivots.values(), base)
            if record.state in (ScopeState.INTEGRATED, ScopeState.RELEASED):
                if consumed:
                    invalidated.append(record)
                else:
                    # 保留者只需重放被回退撤下的集成：最晚集成提交不晚于
                    # 回退点的（历史原样在线）不重放；线上无集成提交的空
                    # 集成保留者原样保留
                    integration = integration_commits.get(record.scope_id)
                    if integration is None:
                        await self._verify_empty_integration(record)
                    elif not await self._coordinator.is_ancestor(integration, retract_commit):
                        preserved.append(await self._replay_step(record))
            elif record.state in (ScopeState.ACTIVE, ScopeState.AWAITING_INTEGRATION) and consumed:
                invalidated.append(record)
        target_records = [await self._require_scope(scope_id) for scope_id in target_scope_ids]
        preserved.sort(key=lambda step: order[step.scope_id])
        return _RetractionPlan(
            invalidated=[*target_records, *invalidated],
            preserved=preserved,
        )

    async def _consumed_by_any_target(self, pivots: Iterable[str], base_commit: str) -> bool:
        """基线谱系是否含任一撤回目标的集成提交（谱系判据）。"""
        for pivot in pivots:
            if await self._coordinator.is_ancestor(pivot, base_commit):
                return True
        return False

    async def _verify_empty_integration(self, record: ScopeRecord) -> None:
        """核对线上无集成提交的作用域确为空集成。

        集成序解析依赖提交信息前缀；分支对建区基线仍带集成路径变更而
        线上无集成提交时说明解析错位，显式拒绝而非静默按空集成跳过。
        """
        if not record.workspace_ref or not record.integration_base_commit:
            raise WorkspaceManagerError(f"作用域缺少工作区或基线引用：{record.scope_id}")
        _path, branch = self._parse_workspace_ref(record.workspace_ref)
        head = await self._repository.resolve_ref(f"refs/heads/{branch}") if branch else None
        if head is None:
            raise WorkspaceManagerError(f"作用域分支缺失，无法核对空集成：{record.scope_id}")
        changed = await self._coordinator.changed_paths(
            record.integration_base_commit,
            head,
            self._manifest.git_diff_restriction_specs(),
        )
        if changed:
            raise WorkspaceManagerError(
                f"线上无集成提交但作用域分支带集成路径变更：{record.scope_id}"
            )

    async def _replay_step(self, record: ScopeRecord) -> ReplayStep:
        """把一个保留写者解析为重放步：分支头存在且在册可解析。"""
        if not record.workspace_ref:
            raise WorkspaceManagerError(f"保留写者缺少工作区引用：{record.scope_id}")
        _path, branch = self._parse_workspace_ref(record.workspace_ref)
        head = await self._repository.resolve_ref(f"refs/heads/{branch}") if branch else None
        if head is None:
            raise WorkspaceManagerError(
                f"保留写者分支缺失，对不上账：{record.scope_id}（{branch}）"
            )
        return ReplayStep(
            scope_id=record.scope_id,
            base_commit=record.integration_base_commit or "",
            head_commit=head,
        )

    async def commit_scope_terminal(self, scope_id: str) -> tuple[str, str]:
        """闸门内提交作用域未提交状态为终态执行态，返回提交与分支。

        单步 git 提交自身原子，崩溃间隙只产生孤儿提交、无账可错位，
        不入操作日志；检查点行由调用方在其后落账。
        """
        return await self.commit_scope_state(scope_id, message=f"scope {scope_id} terminal state")

    async def commit_scope_state(self, scope_id: str, *, message: str) -> tuple[str, str]:
        """闸门内提交作用域当前文件态（终态/组检查点共用的提交步）。

        排除规格与终态提交一致：不入检查点的路径不落提交。返回提交与
        分支；检查点行由调用方在其后落账。
        """
        record = await self._require_scope(scope_id)
        if record.scope_kind is ScopeKind.ROOT:
            raise WorkspaceManagerError("根作用域无终态执行态提交")
        if record.state is not ScopeState.ACTIVE:
            raise WorkspaceManagerError(
                f"作用域非活动态不能提交状态：{scope_id}（{record.state.value}）"
            )
        writer_path, branch = self._require_workspace(record)
        writer_target = WorkspaceTarget(path=writer_path, separated_env=False)
        async with self._quiesced(scope_id):
            commit = await self._coordinator.commit_pending_state(
                writer_target,
                exclude_pathspecs=self._manifest.git_scope_commit_exclude_specs(),
                message=message,
            )
        return commit, branch

    async def abandon_scope(self, request: AbandonScope) -> ScopeAbandoned:
        """废弃作用域及后代：先移除各自工作区，再账面各自换代一次。"""
        target = await self._require_scope(request.scope_id)
        if target.scope_kind is ScopeKind.ROOT:
            raise WorkspaceManagerError("根作用域不可废弃")
        abandoned = await self._abandon_scope_physically(
            await self._tree.require_scope_epoch(request.scope_id, request.expected_scope_epoch)
        )
        return ScopeAbandoned(
            scope_id=request.scope_id,
            new_scope_epoch=abandoned[0].new_scope_epoch,
            abandoned_descendants=tuple(
                (item.scope_id, item.new_scope_epoch) for item in abandoned[1:]
            ),
        )

    async def _abandon_scope_physically(self, target: ScopeRecord) -> list[AbandonedScope]:
        """先移除目标及后代工作区，再账面各自换代一次并回收登记。"""
        for record in (target, *await self._tree.list_descendants(target.scope_id)):
            if record.workspace_ref is None:
                continue
            path, _branch = self._parse_workspace_ref(record.workspace_ref)
            if path.exists():
                await self._coordinator.release_worktree(path)
        abandoned = await self._tree.abandon_scope(
            target.scope_id, expected_scope_epoch=target.scope_epoch
        )
        for item in abandoned:
            self._claims.release(item.scope_id)
            self._reader_scopes.discard(item.scope_id)
        return abandoned

    async def release_scope(self, scope_id: str) -> ScopeRecord:
        """回收工作区：物理裁剪工作树，已集成者账面转已释放，分支保留。"""
        record = await self._require_scope(scope_id)
        if record.scope_kind is ScopeKind.ROOT:
            raise WorkspaceManagerError("根作用域不可释放")
        if record.state not in (ScopeState.INTEGRATED, ScopeState.DISCARDED):
            raise WorkspaceManagerError(
                f"作用域当前状态不可释放：{record.scope_id}（{record.state.value}）"
            )
        if record.workspace_ref is not None:
            path, _branch = self._parse_workspace_ref(record.workspace_ref)
            if path.exists():
                await self._coordinator.release_worktree(path)
        if record.state is ScopeState.INTEGRATED:
            record = await self._tree.set_state(scope_id, ScopeState.RELEASED)
        self._claims.release(scope_id)
        self._reader_scopes.discard(scope_id)
        return record

    async def get_scope(self, scope_id: str) -> ScopeRecord | None:
        """查询作用域账本行。"""
        return await self._tree.get_scope(scope_id)

    def restore_claims(self, scope_id: str, claims: tuple[str, ...]) -> None:
        """崩溃恢复后补登记作用域的声明写路径（出生声明的账面重建）。"""
        if claims:
            self._claims.register(scope_id, claims)

    async def complete_retraction_ledger(self, operation: SagaOperation) -> None:
        """集成线回退的崩溃补账：根代次推进与失效废弃按已生效事实补齐。

        只在引用推进确认后调用（操作处于已写阶段）；各步幂等——根代
        次已推进到目标值或失效作用域已废弃时跳过，收口结果按操作账
        重建。
        """
        if operation.op_kind != "retract_integration_line":
            raise WorkspaceManagerError(f"非集成线回退操作：{operation.client_op_id}")
        # 线未动（已记新头即旧头）的回退不推进根代次
        line_moved = operation.actual_new_head != operation.expected_old_head
        if line_moved:
            try:
                await self._tree.advance_root_epoch(expected_root_epoch=operation.root_epoch)
            except ScopeTreeError as exc:
                if exc.current_root_epoch != operation.root_epoch + 1:
                    raise
        abandoned: list[tuple[str, int]] = []
        for scope_id in operation.metadata.get("invalidated", ()):
            record = await self._tree.get_scope(str(scope_id))
            if record is None:
                continue  # 未经检查点行重建的边角（显式降级面）
            try:
                items = await self._abandon_scope_physically(record)
            except ScopeTreeError as exc:
                if exc.current_scope_epoch == record.scope_epoch + 1:
                    continue  # 崩溃前已废弃过（部分补账幂等）
                raise
            for item in items:
                abandoned.append((item.scope_id, item.new_scope_epoch))
        await self._oplog.commit(
            operation.client_op_id,
            result={
                "new_root_head": operation.actual_new_head,
                "new_root_epoch": operation.root_epoch + 1 if line_moved else operation.root_epoch,
                "target_commit": operation.metadata.get("target_commit"),
                "invalidated": abandoned,
                "preserved": list(operation.metadata.get("preserved", ())),
            },
        )

    async def list_scopes(self) -> list[ScopeRecord]:
        """全部作用域账本行（根起深度优先）。"""
        return await self._tree.list_all_scopes()

    async def current_integration_head(self) -> str | None:
        """当前集成线头；引用缺失返回 None。"""
        return await self._repository.resolve_ref(ROOT_INTEGRATION_REF)

    async def _create_workspace_guarded(
        self,
        *,
        scope_id: str,
        epoch: int,
        base: str,
        materialize_from: str | None,
        materialize_specs: tuple[str, ...],
        client_op_id: str,
        workspace_dir: Path | None = None,
    ) -> ScopeWorkspace:
        """建区失败即清理半成品工作区并放弃操作意图，异常原样上抛。"""
        workspace_scope_key = f"{scope_id}-e{epoch}" if epoch > 1 else scope_id
        try:
            return await self._repository.create_scope_workspace(
                workspace_scope_key,
                base_commit=base,
                materialize_from=materialize_from,
                materialize_pathspecs=materialize_specs,
                workspace_dir=workspace_dir,
            )
        except Exception:
            candidate = (
                workspace_dir
                if workspace_dir is not None
                else self._repository.workspace_dir_for(workspace_scope_key)
            )
            if candidate.exists():
                with suppress(Exception):
                    await self._coordinator.release_worktree(candidate)
            await self._oplog.abandon(client_op_id)
            raise

    @asynccontextmanager
    async def _quiesced(self, scope_id: str) -> AsyncIterator[None]:
        """作用域闸门内的操作段：静止失败或需延迟时复位后上抛。"""
        gate = self._gates.get(scope_id)
        if gate is None:
            gate = ScopeGate()
            self._gates[scope_id] = gate
        await self._tree.set_gate_state(scope_id, ScopeGateState.QUIESCING)
        gate.begin_quiescing()
        outcome = await gate.quiesce(
            lambda: self._drain_background_writers(scope_id),
            parent_blocked_on_result=False,
        )
        if outcome is not QuiesceOutcome.QUIESCED:
            if gate.state is not GateState.RUNNING:
                gate.resume_running()
            await self._tree.set_gate_state(scope_id, ScopeGateState.RUNNING)
            raise ScopeQuiesceFailedError(
                f"作用域闸门未能静止，操作未执行：{scope_id}（{outcome.value}）"
            )
        await self._tree.set_gate_state(scope_id, ScopeGateState.QUIESCED)
        try:
            yield
        finally:
            gate.resume_running()
            await self._tree.set_gate_state(scope_id, ScopeGateState.RUNNING)

    async def _drain_background_writers(self, scope_id: str) -> None:
        """无注入钩子时视为无后台写进程。"""
        if self._drain_hook is not None:
            await self._drain_hook(scope_id)

    def _target_for(self, record: ScopeRecord) -> WorkspaceTarget:
        """把账本行转为可提交的 git 工作区定位（根检出按账面引用落点）。"""
        if record.scope_kind is ScopeKind.ROOT:
            path = Path(record.workspace_ref) if record.workspace_ref else self._repository.root_dir
            return WorkspaceTarget(path=path, separated_env=True)
        path, _branch = self._require_workspace(record)
        return WorkspaceTarget(path=path, separated_env=False)

    def _parent_state_exclude_pathspecs(self, parent: ScopeRecord) -> tuple[str, ...]:
        """父侧状态提交的排除规格：根检出排除一切非集成线路径，作用域
        工作区排除一切不入检查点的路径；用户仓与托管目录由协调器固定
        排除。"""
        if parent.scope_kind is ScopeKind.ROOT:
            return self._manifest.git_line_commit_exclude_specs()
        return self._manifest.git_scope_commit_exclude_specs()

    def _require_workspace(self, record: ScopeRecord) -> tuple[Path, str]:
        """解析作用域的工作区路径与分支；缺失即报错。"""
        if record.workspace_ref is None:
            raise WorkspaceManagerError(f"作用域没有工作区引用：{record.scope_id}")
        return self._parse_workspace_ref(record.workspace_ref)

    @staticmethod
    def _format_workspace_ref(workspace: ScopeWorkspace) -> str:
        """工作区引用编码为「路径|分支」。"""
        return f"{workspace.path.as_posix()}|{workspace.branch}"

    @staticmethod
    def _parse_workspace_ref(workspace_ref: str) -> tuple[Path, str]:
        """解码「路径|分支」形式的工作区引用。"""
        path, _, branch = workspace_ref.rpartition("|")
        return Path(path), branch

    async def _require_scope(self, scope_id: str) -> ScopeRecord:
        """取作用域账本行；不存在即报错。"""
        record = await self._tree.get_scope(scope_id)
        if record is None:
            raise WorkspaceManagerError(f"作用域不存在：{scope_id}")
        return record
