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

"""dispatch_group 组仲裁单测：建组/入组、恢复点不可变、子基线前进与回填。

真实 git（临时目录）走既有工作区测试装配；检查点表经 RunStore 统一迁移
铺设。屏障受理与有界等待在 run 平面链路测试（run_plane/test_dispatch_barrier_chain.py）。
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.checkpoints import (
    BarrierQuality,
    CheckpointStore,
    CheckpointTrigger,
    DispatchAnchor,
)
from workflow_controller.core.runs import RunStore
from workflow_controller.core.workspace.actions import (
    PrepareScopeWorkspace,
    ScopeWorkspaceSpec,
)
from workflow_controller.core.workspace.dispatch_groups import (
    ChildDispatchRequest,
    DispatchGroupArbiter,
    DispatchGroupError,
)
from workflow_controller.core.workspace.scope_tree import ScopeKind

from ._support import build_workspace

if TYPE_CHECKING:
    from workflow_controller.core.checkpoints import ScopeCheckpoint
    from workflow_controller.core.workspace.manager import WorkspaceManager

_ORIGIN_REF = "driver-snapshot:1"


def _anchor(message_id: str, tool_use_id: str, *, preallocated: str = "") -> DispatchAnchor:
    """构造锚点草稿（组标识由仲裁回填）。"""
    return DispatchAnchor(
        parent_native_session_id="ses-parent",
        parent_message_id=message_id,
        tool_use_id=tool_use_id,
        preallocated_scope_id=preallocated,
        exclude_message_id=message_id,
        dispatch_group_id="",
    )


def _tree_paths(repo_dir: Path, commit: str) -> str:
    """提交树内全部路径名（组恢复点内容的断言面）。"""
    proc = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", commit],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout


async def _make_arbiter(
    tmp_path: Path,
) -> tuple[DispatchGroupArbiter, WorkspaceManager, str, CheckpointStore]:
    """装配根 + 节点主写者作用域 + 组仲裁器（检查点表经 RunStore 迁移）。"""
    _root, _repository, manager = build_workspace(tmp_path, tree_id="grp-1")
    db_path = tmp_path / "events.db"
    await RunStore(db_path).list_runs()  # 首次访问触发统一迁移铺设检查点表
    store = CheckpointStore(db_path)
    await manager.initialize_root(scope_id="grp-1-root")
    prepared = await manager.prepare_scope_workspace(
        PrepareScopeWorkspace(
            parent_scope_id="grp-1-root",
            scope_key="node-a",
            spec=ScopeWorkspaceSpec(scope_kind=ScopeKind.MAIN_WRITER, owner_ref="a"),
            client_op_id="prepare-1",
        )
    )
    return DispatchGroupArbiter(manager, store), manager, prepared.scope_id, store


async def _parent_workspace(manager: WorkspaceManager, parent: str) -> Path:
    """取父作用域工作区路径。"""
    scope = await manager.get_scope(parent)
    assert scope is not None and scope.workspace_ref
    return Path(scope.workspace_ref.rpartition("|")[0])


async def test_first_write_tool_establishes_group_before_side_effect(tmp_path: Path) -> None:
    """message 内首副作用是写工具：组检查点先落，恢复点不含其后的副作用。"""
    arbiter, manager, parent, store = await _make_arbiter(tmp_path)
    workspace = await _parent_workspace(manager, parent)

    admission = await arbiter.admit_side_effect(
        parent_scope_id=parent,
        anchor=_anchor("msg-1", "tool-w1"),
        child=None,
        barrier_quality=BarrierQuality.PRECISE,
        orchestration_state_ref=_ORIGIN_REF,
    )
    assert admission.child_cwd is None
    group = admission.dispatch_group_id
    assert group

    row = await store.get(admission.group_checkpoint_id)
    assert row is not None
    assert row.scope_id == parent
    assert row.trigger is CheckpointTrigger.BEFORE_CHILD
    assert row.barrier_quality is BarrierQuality.PRECISE
    assert row.orchestration_state_ref == _ORIGIN_REF
    assert row.dispatch_anchor is not None
    assert row.dispatch_anchor.parent_native_session_id == "ses-parent"
    assert row.dispatch_anchor.parent_message_id == "msg-1"
    assert row.dispatch_anchor.tool_use_id == "tool-w1"
    assert row.dispatch_anchor.exclude_message_id == "msg-1"
    assert row.dispatch_anchor.dispatch_group_id == group

    # 副作用发生在组提交之后：恢复点树内不含本副作用产物
    (workspace / "design" / "side.md").write_text("written after checkpoint\n", encoding="utf-8")
    assert "design/side.md" not in _tree_paths(_repo_dir(manager), row.workspace_commit)


def _repo_dir(manager: WorkspaceManager) -> Path:
    """取管理器私有仓路径（测试断言面）。"""
    return manager._repository.repo_dir


async def test_subsequent_task_joins_group_and_advances_child_base(tmp_path: Path) -> None:
    """同 message 后续 task 入组：不重建组、组恢复点不动、子基线前进。"""
    arbiter, manager, parent, store = await _make_arbiter(tmp_path)
    workspace = await _parent_workspace(manager, parent)

    first = await arbiter.admit_side_effect(
        parent_scope_id=parent,
        anchor=_anchor("msg-1", "tool-t1", preallocated="node-a-c1"),
        child=ChildDispatchRequest(scope_key="node-a-c1", client_op_id="dispatch-1"),
        barrier_quality=BarrierQuality.PRECISE,
        orchestration_state_ref=_ORIGIN_REF,
    )
    assert first.child_cwd and Path(first.child_cwd).is_dir()
    assert first.child_scope_id == "node-a-c1"
    # 首派发的组行即本派发锚点行（组检查点 = 首副作用锚点）
    assert first.checkpoint_id == first.group_checkpoint_id
    group_commit_row = await store.get(first.group_checkpoint_id)
    assert group_commit_row is not None
    group_commit = group_commit_row.workspace_commit

    # 组内前序成果落父侧后再派发第二个 task
    (workspace / "design" / "gained.md").write_text("prior work\n", encoding="utf-8")
    second = await arbiter.admit_side_effect(
        parent_scope_id=parent,
        anchor=_anchor("msg-1", "tool-t2", preallocated="node-a-c2"),
        child=ChildDispatchRequest(scope_key="node-a-c2", client_op_id="dispatch-2"),
        barrier_quality=BarrierQuality.PRECISE,
        orchestration_state_ref=_ORIGIN_REF,
    )
    assert second.dispatch_group_id == first.dispatch_group_id
    assert second.group_checkpoint_id == first.group_checkpoint_id
    assert second.checkpoint_id != first.group_checkpoint_id

    # 组恢复点不可变；子基线前进且含组内前序成果
    refreshed_group = await store.get(first.group_checkpoint_id)
    assert refreshed_group is not None
    assert refreshed_group.workspace_commit == group_commit
    assert second.child_base_commit
    assert second.child_base_commit != group_commit
    repo_dir = _repo_dir(manager)
    assert "design/gained.md" in _tree_paths(repo_dir, second.child_base_commit)
    assert "design/gained.md" not in _tree_paths(repo_dir, group_commit)

    second_row = await store.get(second.checkpoint_id)
    assert second_row is not None
    assert second_row.workspace_commit == second.child_base_commit
    assert second_row.integration_commit == second.child_base_commit
    assert second_row.dispatch_anchor is not None
    assert second_row.dispatch_anchor.tool_use_id == "tool-t2"
    assert second_row.dispatch_anchor.preallocated_scope_id == "node-a-c2"
    assert second_row.dispatch_anchor.dispatch_group_id == first.dispatch_group_id
    # 子工作区自子基线检出：前序成果对第二个子可见
    assert (Path(second.child_cwd) / "design" / "gained.md").read_text(encoding="utf-8") == (
        "prior work\n"
    )


async def test_subsequent_write_tool_joins_without_moving_restore_point(
    tmp_path: Path,
) -> None:
    """同 message 后续写工具入组：放行不建行，组恢复点不动。"""
    arbiter, _manager, parent, store = await _make_arbiter(tmp_path)
    first = await arbiter.admit_side_effect(
        parent_scope_id=parent,
        anchor=_anchor("msg-1", "tool-w1"),
        child=None,
        barrier_quality=BarrierQuality.PRECISE,
        orchestration_state_ref=_ORIGIN_REF,
    )
    row = await store.get(first.group_checkpoint_id)
    assert row is not None
    later = await arbiter.admit_side_effect(
        parent_scope_id=parent,
        anchor=_anchor("msg-1", "tool-w2"),
        child=None,
        barrier_quality=BarrierQuality.PRECISE,
        orchestration_state_ref=_ORIGIN_REF,
    )
    assert later.dispatch_group_id == first.dispatch_group_id
    refreshed = await store.get(first.group_checkpoint_id)
    assert refreshed is not None
    assert refreshed.workspace_commit == row.workspace_commit
    parent_rows = [r for r in await store.list_for_scope(parent)]
    assert len(parent_rows) == 1  # 写工具入组不新增检查点行


async def test_new_message_starts_new_group(tmp_path: Path) -> None:
    """不同 message 开新组：新恢复点、新组标识。"""
    arbiter, _manager, parent, _store = await _make_arbiter(tmp_path)
    first = await arbiter.admit_side_effect(
        parent_scope_id=parent,
        anchor=_anchor("msg-1", "tool-a"),
        child=None,
        barrier_quality=BarrierQuality.PRECISE,
        orchestration_state_ref=_ORIGIN_REF,
    )
    second = await arbiter.admit_side_effect(
        parent_scope_id=parent,
        anchor=_anchor("msg-2", "tool-b"),
        child=None,
        barrier_quality=BarrierQuality.PRECISE,
        orchestration_state_ref=_ORIGIN_REF,
    )
    assert second.dispatch_group_id != first.dispatch_group_id
    assert second.group_checkpoint_id != first.group_checkpoint_id


async def test_compat_mode_marks_late_barrier(tmp_path: Path) -> None:
    """弱化档入组行标注 late_barrier。"""
    arbiter, _manager, parent, store = await _make_arbiter(tmp_path)
    admission = await arbiter.admit_side_effect(
        parent_scope_id=parent,
        anchor=_anchor("msg-1", "tool-l"),
        child=None,
        barrier_quality=BarrierQuality.LATE_BARRIER,
        orchestration_state_ref=_ORIGIN_REF,
    )
    row = await store.get(admission.group_checkpoint_id)
    assert row is not None
    assert row.barrier_quality is BarrierQuality.LATE_BARRIER


async def test_attach_child_session_backfills_map(tmp_path: Path) -> None:
    """子代理启动后回填 child_session_map（scope_id → 原生子会话）。"""
    arbiter, _manager, parent, store = await _make_arbiter(tmp_path)
    admission = await arbiter.admit_side_effect(
        parent_scope_id=parent,
        anchor=_anchor("msg-1", "tool-t9", preallocated="node-a-c9"),
        child=ChildDispatchRequest(scope_key="node-a-c9", client_op_id="dispatch-9"),
        barrier_quality=BarrierQuality.PRECISE,
        orchestration_state_ref=_ORIGIN_REF,
    )
    assert admission.child_scope_id
    await arbiter.attach_child_session(
        admission.checkpoint_id, admission.child_scope_id, "ses-child-9"
    )
    row: ScopeCheckpoint | None = await store.get(admission.checkpoint_id)
    assert row is not None
    assert row.child_session_map == {admission.child_scope_id: "ses-child-9"}


async def test_missing_orchestration_ref_rejected(tmp_path: Path) -> None:
    """编排状态引用缺失即拒绝：文件提交不得先于编排版本。"""
    arbiter, _manager, parent, _store = await _make_arbiter(tmp_path)
    with pytest.raises(DispatchGroupError):
        await arbiter.admit_side_effect(
            parent_scope_id=parent,
            anchor=_anchor("msg-1", "tool-x"),
            child=None,
            barrier_quality=BarrierQuality.PRECISE,
            orchestration_state_ref="",
        )
