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

"""dispatch_group 组仲裁：message 级副作用分组与组检查点/子基线分离。

硬不变量「文件恢复点与对话边界表示同一逻辑时刻」的落点：message 内
首个副作用前建组（组恢复点不可变），同 message 后续副作用入组只前进
child_base_commit（用于该子建区与补账，不作为恢复目标）。本模块只做
runtime 无关的核心逻辑，屏障的受理与有界等待在 run 平面。

另承载组副作用台账（DispatchReplayLedger）：按组留存原始 task 调用
参数与副作用计数，供 replay_dispatch 的「单 task、无其他副作用」校验。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from workflow_controller.core.checkpoints import (
    BarrierQuality,
    CheckpointTrigger,
    ScopeCheckpoint,
)
from workflow_controller.core.workspace.actions import (
    PrepareScopeWorkspace,
    ScopeWorkspaceSpec,
)
from workflow_controller.core.workspace.scope_tree import ScopeKind

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.core.checkpoints import (
        CheckpointStore,
        DispatchAnchor,
    )
    from workflow_controller.core.workspace.manager import WorkspaceManager
    from workflow_controller.core.workspace.scope_tree import ScopeRecord

__all__ = [
    "ChildDispatchRequest",
    "DispatchAdmission",
    "DispatchGroupArbiter",
    "DispatchGroupError",
    "DispatchReplayLedger",
    "GroupSideEffectRecord",
    "ReplayRejectedError",
]

# 子代理工作区声明词面：writer（默认，独立可写工作区）| reader（detached 快照）
WORKSPACE_WRITER = "writer"
WORKSPACE_READER = "reader"


class DispatchGroupError(RuntimeError):
    """组仲裁拒绝：状态或参数不满足时显式失败。"""


class ReplayRejectedError(DispatchGroupError):
    """replay_dispatch 拒绝：组不满足「单 task、无其他副作用」。"""


@dataclass(frozen=True)
class ChildDispatchRequest:
    """task 派发的建区参数：预置作用域键、可选属主、声明写路径与工作区档位。"""

    scope_key: str
    client_op_id: str
    owner_ref: str | None = None
    claims: tuple[str, ...] = ()
    workspace: str = WORKSPACE_WRITER


@dataclass(frozen=True)
class DispatchAdmission:
    """屏障准入结果：组身份与检查点事实，task 场景另带子工作区。"""

    dispatch_group_id: str
    group_checkpoint_id: str
    checkpoint_id: str
    anchor: DispatchAnchor
    child_scope_id: str | None = None
    child_cwd: str | None = None
    child_base_commit: str | None = None
    reader_snapshot: bool = False


@dataclass
class _ActiveGroup:
    """一个父作用域当前活动组：恢复点不可变、子基线可前进。"""

    group_id: str
    message_key: str
    group_commit: str
    group_checkpoint_id: str
    scope_epoch: int
    root_epoch: int
    established: bool = field(default=False)


@dataclass(frozen=True)
class GroupSideEffectRecord:
    """组副作用台账行：task 调用参数与全部副作用的工具调用号。"""

    group_id: str
    tasks: tuple[dict[str, str], ...] = ()
    side_effect_tool_use_ids: tuple[str, ...] = ()


class DispatchReplayLedger:
    """组副作用台账：JSON 文件按组留存，供恢复时重放与校验。"""

    def __init__(self, base_dir: Path) -> None:
        """台账目录惰性创建于首次写入。"""
        self._base_dir = base_dir

    def record_task(
        self,
        group_id: str,
        *,
        tool_use_id: str,
        tool_name: str,
        tool_input: object,
    ) -> None:
        """登记一个 task 派发的原始调用参数。"""
        record = self._load(group_id)
        tasks = [t for t in record.tasks if t.get("tool_use_id") != tool_use_id]
        tasks.append(
            {
                "tool_use_id": tool_use_id,
                "tool_name": tool_name,
                "tool_input": json.dumps(tool_input, ensure_ascii=False, default=str),
            }
        )
        self._save(group_id, replace(record, tasks=tuple(tasks)))

    def record_side_effect(self, group_id: str, *, tool_use_id: str) -> None:
        """登记一个非 task 副作用（写文件类/Bash 工具）。"""
        record = self._load(group_id)
        ids = list(record.side_effect_tool_use_ids)
        if tool_use_id and tool_use_id not in ids:
            ids.append(tool_use_id)
        self._save(group_id, replace(record, side_effect_tool_use_ids=tuple(ids)))

    def require_single_task(self, group_id: str, checkpoint_id: str) -> dict[str, str]:
        """校验组满足重放前提并返回唯一 task 的原始调用参数。

        多 task 或存在其他副作用的组回滚会撤回整组，只重放目标 task 会
        缺输入——明确拒绝；台账缺席（崩溃残留丢失或旧组）同样拒绝。
        """
        record = self._load(group_id)
        if not record.tasks or record.side_effect_tool_use_ids or len(record.tasks) != 1:
            raise ReplayRejectedError(
                f"replay_dispatch 仅允许单 task、无其他副作用的派发组：{checkpoint_id}"
                f"（组内 task {len(record.tasks)} 个、其他副作用 {len(record.side_effect_tool_use_ids)} 个），"
                "多副作用组请用自然续跑"
            )
        return record.tasks[0]

    def _load(self, group_id: str) -> GroupSideEffectRecord:
        """读组台账；文件缺席返回空记录。"""
        path = self._path(group_id)
        if not path.is_file():
            return GroupSideEffectRecord(group_id=group_id)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return GroupSideEffectRecord(group_id=group_id)
        return GroupSideEffectRecord(
            group_id=group_id,
            tasks=tuple(raw.get("tasks") or ()),
            side_effect_tool_use_ids=tuple(raw.get("side_effects") or ()),
        )

    def _save(self, group_id: str, record: GroupSideEffectRecord) -> None:
        """写组台账（原子替换）。"""
        self._base_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "tasks": [dict(t) for t in record.tasks],
            "side_effects": list(record.side_effect_tool_use_ids),
        }
        path = self._path(group_id)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)

    def _path(self, group_id: str) -> Path:
        """组台账文件路径（组标识含作用域前缀，天然不冲突）。"""
        return self._base_dir / f"{group_id}.json"


class DispatchGroupArbiter:
    """按父作用域维护活动派发组并执行组检查点落账。"""

    def __init__(
        self,
        manager: WorkspaceManager,
        checkpoints: CheckpointStore,
        replay_ledger: DispatchReplayLedger | None = None,
    ) -> None:
        """装配工作区管理器、检查点账本与可选组副作用台账。"""
        self._manager = manager
        self._checkpoints = checkpoints
        self._replay_ledger = replay_ledger
        self._active: dict[str, _ActiveGroup] = {}
        self._group_seq = 0

    async def admit_side_effect(
        self,
        *,
        parent_scope_id: str,
        anchor: DispatchAnchor,
        child: ChildDispatchRequest | None = None,
        barrier_quality: BarrierQuality,
        orchestration_state_ref: str,
        tool_name: str = "",
        tool_input: object = None,
    ) -> DispatchAdmission:
        """受理一个副作用屏障：建组或入组，task 场景建子工作区并回 cwd。

        锚点草稿的 dispatch_group_id 由本方法回填（组身份在此产生）；
        编排状态引用必须先于文件提交持久化（调用方保证）。
        """
        if not orchestration_state_ref:
            raise DispatchGroupError("编排状态引用须先于组检查点持久化")
        parent = await self._manager.get_scope(parent_scope_id)
        if parent is None:
            raise DispatchGroupError(f"父作用域不存在：{parent_scope_id}")
        message_key = anchor.parent_message_id
        if not message_key:
            raise DispatchGroupError("锚点缺少父消息标识，无法做 message 级分组")
        if child is not None and anchor.preallocated_scope_id != child.scope_key:
            raise DispatchGroupError("锚点预置作用域与派发建区请求不一致")

        group = self._active.get(parent_scope_id)
        establishing = (
            group is None
            or group.message_key != message_key
            or group.scope_epoch != parent.scope_epoch
            or group.root_epoch != parent.root_epoch
        )
        if establishing:
            group = await self._establish_group(
                parent, anchor, message_key, barrier_quality, orchestration_state_ref
            )

        if child is None:
            # 写文件类副作用：组检查点已先于本副作用，放行（修改落父分支）
            self._record_side_effect(group, anchor.tool_use_id)
            return DispatchAdmission(
                dispatch_group_id=group.group_id,
                group_checkpoint_id=group.group_checkpoint_id,
                checkpoint_id=group.group_checkpoint_id,
                anchor=self._finalize_anchor(anchor, group.group_id),
            )

        self._record_task(group, anchor.tool_use_id, tool_name, tool_input)
        if child.workspace == WORKSPACE_READER:
            return await self._admit_reader(group, parent, anchor, child)
        prepared = await self._manager.prepare_scope_workspace(
            PrepareScopeWorkspace(
                parent_scope_id=parent_scope_id,
                scope_key=child.scope_key,
                spec=ScopeWorkspaceSpec(
                    scope_kind=ScopeKind.SUBAGENT,
                    owner_ref=child.owner_ref,
                    claims=child.claims,
                ),
                client_op_id=child.client_op_id,
            )
        )
        final_anchor = self._finalize_anchor(anchor, group.group_id)
        checkpoint_id = group.group_checkpoint_id
        if group.established:
            # 后续派发：入组行单独落账，workspace/integration 记 child_base_commit
            checkpoint_id = f"{parent_scope_id}:before_child:{anchor.tool_use_id}"
            await self._record_checkpoint(
                scope=parent,
                checkpoint_id=checkpoint_id,
                anchor=final_anchor,
                barrier_quality=barrier_quality,
                workspace_commit=prepared.base_commit,
                branch=None,
                integration_commit=prepared.base_commit,
                orchestration_state_ref=orchestration_state_ref,
            )
        group.established = True
        return DispatchAdmission(
            dispatch_group_id=group.group_id,
            group_checkpoint_id=group.group_checkpoint_id,
            checkpoint_id=checkpoint_id,
            anchor=final_anchor,
            child_scope_id=prepared.scope_id,
            child_cwd=prepared.workspace_path,
            child_base_commit=prepared.base_commit,
        )

    async def _admit_reader(
        self,
        group: _ActiveGroup,
        parent: ScopeRecord,
        anchor: DispatchAnchor,
        child: ChildDispatchRequest,
    ) -> DispatchAdmission:
        """reader 派发准入：组恢复点上的 detached 快照，无分支不入账后续派发行。"""
        prepared = await self._manager.prepare_reader_workspace(
            parent.scope_id, child.scope_key, base_commit=group.group_commit
        )
        group.established = True
        return DispatchAdmission(
            dispatch_group_id=group.group_id,
            group_checkpoint_id=group.group_checkpoint_id,
            checkpoint_id=group.group_checkpoint_id,
            anchor=self._finalize_anchor(anchor, group.group_id),
            child_scope_id=prepared.scope_id,
            child_cwd=prepared.workspace_path,
            child_base_commit=group.group_commit,
            reader_snapshot=True,
        )

    def _record_task(
        self,
        group: _ActiveGroup,
        tool_use_id: str,
        tool_name: str,
        tool_input: object,
    ) -> None:
        """组台账登记 task 调用参数（replay_dispatch 的重放数据源）。"""
        if self._replay_ledger is not None:
            self._replay_ledger.record_task(
                group.group_id,
                tool_use_id=tool_use_id,
                tool_name=tool_name or "Task",
                tool_input=tool_input,
            )

    def _record_side_effect(self, group: _ActiveGroup, tool_use_id: str) -> None:
        """组台账登记非 task 副作用（多副作用组拒绝重放的判据）。"""
        if self._replay_ledger is not None:
            self._replay_ledger.record_side_effect(group.group_id, tool_use_id=tool_use_id)

    async def attach_child_session(
        self,
        checkpoint_id: str,
        child_scope_id: str,
        native_child_session_id: str,
    ) -> None:
        """子代理启动后向组检查点行回填子会话映射。"""
        await self._checkpoints.attach_child_session(
            checkpoint_id, child_scope_id, native_child_session_id
        )

    def forget(self, parent_scope_id: str) -> None:
        """丢弃父作用域的活动组（作用域收口/换代后组失效）。"""
        self._active.pop(parent_scope_id, None)

    async def _establish_group(
        self,
        parent: ScopeRecord,
        anchor: DispatchAnchor,
        message_key: str,
        barrier_quality: BarrierQuality,
        orchestration_state_ref: str,
    ) -> _ActiveGroup:
        """建组：闸门内提交父当前文件态为不可变组恢复点并落组检查点行。"""
        self._group_seq += 1
        group_id = f"{parent.scope_id}-g{self._group_seq}"
        commit, branch = await self._manager.commit_scope_state(
            parent.scope_id, message=f"dispatch group {group_id} start"
        )
        group_checkpoint_id = f"{parent.scope_id}:before_child:{group_id}"
        await self._record_checkpoint(
            scope=parent,
            checkpoint_id=group_checkpoint_id,
            anchor=self._finalize_anchor(anchor, group_id),
            barrier_quality=barrier_quality,
            workspace_commit=commit,
            branch=branch,
            integration_commit=commit,
            orchestration_state_ref=orchestration_state_ref,
        )
        group = _ActiveGroup(
            group_id=group_id,
            message_key=message_key,
            group_commit=commit,
            group_checkpoint_id=group_checkpoint_id,
            scope_epoch=parent.scope_epoch,
            root_epoch=parent.root_epoch,
        )
        self._active[parent.scope_id] = group
        return group

    async def _record_checkpoint(
        self,
        *,
        scope: ScopeRecord,
        checkpoint_id: str,
        anchor: DispatchAnchor,
        barrier_quality: BarrierQuality,
        workspace_commit: str,
        branch: str | None,
        integration_commit: str | None,
        orchestration_state_ref: str,
    ) -> None:
        """落一行派发屏障检查点（文件提交与编排引用都已先在）。"""
        await self._checkpoints.record(
            ScopeCheckpoint(
                checkpoint_id=checkpoint_id,
                scope_id=scope.scope_id,
                scope_kind=scope.scope_kind,
                parent_scope_id=scope.parent_scope_id,
                scope_epoch=scope.scope_epoch,
                root_epoch=scope.root_epoch,
                trigger=CheckpointTrigger.BEFORE_CHILD,
                barrier_quality=barrier_quality,
                workspace_commit=workspace_commit,
                workspace_branch=branch,
                integration_commit=integration_commit,
                dispatch_anchor=anchor,
                orchestration_state_ref=orchestration_state_ref,
                created_at=datetime.now(tz=UTC).isoformat(),
            )
        )

    @staticmethod
    def _finalize_anchor(anchor: DispatchAnchor, group_id: str) -> DispatchAnchor:
        """回填锚点的组标识；已携带且一致的保留原值。"""
        if anchor.dispatch_group_id and anchor.dispatch_group_id != group_id:
            raise DispatchGroupError("锚点组标识与活动组不一致")
        return replace(anchor, dispatch_group_id=group_id)
