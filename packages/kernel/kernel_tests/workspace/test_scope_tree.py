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

"""作用域树账本测试：CRUD、两层代次与生命周期。"""

from __future__ import annotations

import pytest

from workflow_controller.core.workspace import (
    ScopeGateState,
    ScopeKind,
    ScopeState,
    ScopeTree,
    ScopeTreeError,
)


async def _tree_with_active_writer() -> ScopeTree:
    """建一棵「根 → 主写者」树，主写者已完成建区转活动。"""
    tree = ScopeTree()
    await tree.create_root_scope(
        scope_id="run-root",
        owner_ref="run-1",
        workspace_ref="/work/root",
        scope_base_commit="commit-seed",
        integration_base_commit="commit-line",
    )
    await tree.create_child_scope(
        scope_id="node-a",
        parent_scope_id="run-root",
        scope_kind=ScopeKind.MAIN_WRITER,
        integration_base_commit="commit-line",
    )
    await tree.activate_scope(
        "node-a", workspace_ref="/work/node-a", scope_base_commit="commit-node-a"
    )
    return tree


async def test_create_scopes_record_fields_and_epochs() -> None:
    tree = await _tree_with_active_writer()

    root = await tree.get_scope("run-root")
    assert root is not None
    assert root.scope_kind is ScopeKind.ROOT
    assert root.parent_scope_id is None
    assert root.scope_epoch == 1
    assert root.root_epoch == 1
    assert root.state is ScopeState.ACTIVE
    assert root.owner_ref == "run-1"

    node = await tree.get_scope("node-a")
    assert node is not None
    assert node.scope_kind is ScopeKind.MAIN_WRITER
    assert node.parent_scope_id == "run-root"
    assert node.state is ScopeState.ACTIVE
    assert node.scope_base_commit == "commit-node-a"
    assert node.integration_base_commit == "commit-line"

    subagent = await tree.create_child_scope(
        scope_id="agent-1",
        parent_scope_id="node-a",
        scope_kind=ScopeKind.SUBAGENT,
        integration_base_commit="commit-node-a",
        owner_ref="task-1",
    )
    assert subagent.state is ScopeState.PREPARING
    assert subagent.gate_state is ScopeGateState.RUNNING
    assert subagent.root_epoch == 1

    assert [child.scope_id for child in await tree.list_children("node-a")] == ["agent-1"]
    assert [item.scope_id for item in await tree.list_descendants("run-root")] == [
        "node-a",
        "agent-1",
    ]
    assert await tree.get_scope("missing") is None


async def test_create_scope_validates_tree_shape() -> None:
    tree = await _tree_with_active_writer()

    with pytest.raises(ScopeTreeError):
        await tree.create_root_scope(scope_id="another-root")
    with pytest.raises(ScopeTreeError):
        await tree.create_child_scope(
            scope_id="bad-root",
            parent_scope_id="node-a",
            scope_kind=ScopeKind.ROOT,
            integration_base_commit="commit-node-a",
        )
    with pytest.raises(ScopeTreeError):
        await tree.create_child_scope(
            scope_id="bad-sub",
            parent_scope_id="run-root",
            scope_kind=ScopeKind.SUBAGENT,
            integration_base_commit="commit-line",
        )
    with pytest.raises(ScopeTreeError):
        await tree.create_child_scope(
            scope_id="bad-writer",
            parent_scope_id="node-a",
            scope_kind=ScopeKind.MAIN_WRITER,
            integration_base_commit="commit-node-a",
        )
    with pytest.raises(ScopeTreeError):
        await tree.create_child_scope(
            scope_id="orphan",
            parent_scope_id="missing",
            scope_kind=ScopeKind.SUBAGENT,
            integration_base_commit="commit-node-a",
        )
    with pytest.raises(ScopeTreeError):
        await tree.create_child_scope(
            scope_id="node-a",
            parent_scope_id="run-root",
            scope_kind=ScopeKind.MAIN_WRITER,
            integration_base_commit="commit-line",
        )
    with pytest.raises(ScopeTreeError):
        await tree.create_child_scope(
            scope_id="no-base",
            parent_scope_id="node-a",
            scope_kind=ScopeKind.SUBAGENT,
            integration_base_commit="",
        )

    # 建区中的父作用域尚未活动，不能建子
    preparing = await tree.create_child_scope(
        scope_id="agent-1",
        parent_scope_id="node-a",
        scope_kind=ScopeKind.SUBAGENT,
        integration_base_commit="commit-node-a",
    )
    with pytest.raises(ScopeTreeError):
        await tree.create_child_scope(
            scope_id="grand-child",
            parent_scope_id="agent-1",
            scope_kind=ScopeKind.SUBAGENT,
            integration_base_commit=preparing.integration_base_commit or "",
        )


async def test_lifecycle_transitions_follow_table() -> None:
    tree = await _tree_with_active_writer()

    released = await tree.set_state("node-a", ScopeState.AWAITING_INTEGRATION)
    assert released.state is ScopeState.AWAITING_INTEGRATION
    conflicted_tree = ScopeTree()
    await conflicted_tree.create_root_scope(scope_id="root")
    await conflicted_tree.create_child_scope(
        scope_id="node-b",
        parent_scope_id="root",
        scope_kind=ScopeKind.MAIN_WRITER,
        integration_base_commit="c0",
    )
    await conflicted_tree.activate_scope(
        "node-b", workspace_ref="/work/node-b", scope_base_commit="c1"
    )
    await conflicted_tree.set_state("node-b", ScopeState.AWAITING_INTEGRATION)
    conflicted = await conflicted_tree.set_state("node-b", ScopeState.CONFLICTED)
    assert conflicted.state is ScopeState.CONFLICTED

    integrated = await tree.set_state("node-a", ScopeState.INTEGRATED)
    assert integrated.state is ScopeState.INTEGRATED
    final = await tree.set_state("node-a", ScopeState.RELEASED)
    assert final.state is ScopeState.RELEASED


async def test_illegal_transitions_rejected() -> None:
    tree = await _tree_with_active_writer()

    with pytest.raises(ScopeTreeError):
        await tree.set_state("node-a", ScopeState.INTEGRATED)
    with pytest.raises(ScopeTreeError):
        await tree.set_state("node-a", ScopeState.PREPARING)
    with pytest.raises(ScopeTreeError):
        await tree.activate_scope("node-a", workspace_ref="/work/node-a", scope_base_commit="again")

    fresh = ScopeTree()
    await fresh.create_root_scope(scope_id="root")
    preparing = await fresh.create_child_scope(
        scope_id="node-c",
        parent_scope_id="root",
        scope_kind=ScopeKind.MAIN_WRITER,
        integration_base_commit="c0",
    )
    assert preparing.state is ScopeState.PREPARING
    with pytest.raises(ScopeTreeError):
        await fresh.set_state("node-c", ScopeState.AWAITING_INTEGRATION)

    released_tree = await _tree_with_active_writer()
    await released_tree.set_state("node-a", ScopeState.AWAITING_INTEGRATION)
    await released_tree.set_state("node-a", ScopeState.INTEGRATED)
    await released_tree.set_state("node-a", ScopeState.RELEASED)
    with pytest.raises(ScopeTreeError):
        await released_tree.set_state("node-a", ScopeState.ACTIVE)


async def test_abandon_discards_subtree_and_advances_epochs() -> None:
    tree = await _tree_with_active_writer()
    await tree.create_child_scope(
        scope_id="agent-1",
        parent_scope_id="node-a",
        scope_kind=ScopeKind.SUBAGENT,
        integration_base_commit="commit-node-a",
    )

    abandoned = await tree.abandon_scope("node-a", expected_scope_epoch=1)

    assert [(item.scope_id, item.new_scope_epoch) for item in abandoned] == [
        ("node-a", 2),
        ("agent-1", 2),
    ]
    node = await tree.get_scope("node-a")
    assert node is not None
    assert node.state is ScopeState.DISCARDED
    assert node.scope_epoch == 2
    root = await tree.get_scope("run-root")
    assert root is not None
    assert root.state is ScopeState.ACTIVE
    assert root.scope_epoch == 1

    # 旧代次请求拒绝，并在异常上携带当前代次
    with pytest.raises(ScopeTreeError) as stale_abandon:
        await tree.abandon_scope("node-a", expected_scope_epoch=1)
    assert stale_abandon.value.current_scope_epoch == 2

    # 已废弃父作用域不能建子
    with pytest.raises(ScopeTreeError):
        await tree.create_child_scope(
            scope_id="agent-2",
            parent_scope_id="node-a",
            scope_kind=ScopeKind.SUBAGENT,
            integration_base_commit="commit-node-a",
        )


async def test_abandon_released_scope_bumps_epoch_only() -> None:
    """验收失败回滚的常态路径：目标已集成并释放，废弃只做账面换代。"""
    tree = await _tree_with_active_writer()
    await tree.set_state("node-a", ScopeState.AWAITING_INTEGRATION)
    await tree.set_state("node-a", ScopeState.INTEGRATED)
    await tree.set_state("node-a", ScopeState.RELEASED)

    abandoned = await tree.abandon_scope("node-a", expected_scope_epoch=1)

    assert abandoned[0].new_scope_epoch == 2
    node = await tree.get_scope("node-a")
    assert node is not None
    assert node.state is ScopeState.DISCARDED


async def test_discarded_scope_rebuilds_on_current_root_epoch() -> None:
    tree = await _tree_with_active_writer()
    await tree.abandon_scope("node-a", expected_scope_epoch=1)
    await tree.advance_root_epoch(expected_root_epoch=1)

    rebuilt = await tree.begin_attempt(
        "node-a",
        workspace_ref="/work/node-a",
        scope_base_commit="commit-node-a-2",
        integration_base_commit="commit-line-2",
    )

    assert rebuilt.state is ScopeState.PREPARING
    assert rebuilt.scope_epoch == 2
    assert rebuilt.root_epoch == 2
    assert rebuilt.workspace_ref == "/work/node-a"
    assert rebuilt.integration_base_commit == "commit-line-2"

    activated = await tree.activate_scope(
        "node-a", workspace_ref="/work/node-a", scope_base_commit="commit-node-a-2"
    )
    assert activated.state is ScopeState.ACTIVE
    assert activated.scope_epoch == 2

    with pytest.raises(ScopeTreeError):
        await tree.begin_attempt(
            "node-a",
            workspace_ref="/work/node-a",
            scope_base_commit="commit-node-a-3",
            integration_base_commit="commit-line-3",
        )


async def test_root_epoch_rejects_stale_requests_and_baselines() -> None:
    tree = await _tree_with_active_writer()
    await tree.create_child_scope(
        scope_id="agent-1",
        parent_scope_id="node-a",
        scope_kind=ScopeKind.SUBAGENT,
        integration_base_commit="commit-node-a",
    )

    assert await tree.advance_root_epoch(expected_root_epoch=1) == 2

    with pytest.raises(ScopeTreeError) as stale_require:
        await tree.require_root_epoch(1)
    assert stale_require.value.current_root_epoch == 2
    await tree.require_root_epoch(2)

    with pytest.raises(ScopeTreeError) as stale_advance:
        await tree.advance_root_epoch(expected_root_epoch=1)
    assert stale_advance.value.current_root_epoch == 2

    # 旧根代次下建区的作用域基线失效；新建作用域继承当前根代次
    with pytest.raises(ScopeTreeError) as stale_baseline:
        await tree.require_baseline_current("node-a")
    assert stale_baseline.value.current_root_epoch == 2
    fresh = await tree.create_child_scope(
        scope_id="node-b",
        parent_scope_id="run-root",
        scope_kind=ScopeKind.MAIN_WRITER,
        integration_base_commit="commit-line-2",
    )
    assert fresh.root_epoch == 2
    await tree.require_baseline_current("node-b")


async def test_scope_epoch_receipt_validation() -> None:
    tree = await _tree_with_active_writer()

    matched = await tree.require_scope_epoch("node-a", 1)
    assert matched.scope_id == "node-a"
    with pytest.raises(ScopeTreeError) as stale_receipt:
        await tree.require_scope_epoch("node-a", 0)
    assert stale_receipt.value.current_scope_epoch == 1
    with pytest.raises(ScopeTreeError):
        await tree.require_scope_epoch("missing", 1)


async def test_gate_state_cycle_enforced() -> None:
    tree = await _tree_with_active_writer()

    with pytest.raises(ScopeTreeError):
        await tree.set_gate_state("node-a", ScopeGateState.QUIESCED)
    await tree.set_gate_state("node-a", ScopeGateState.QUIESCING)
    with pytest.raises(ScopeTreeError):
        await tree.set_gate_state("node-a", ScopeGateState.QUIESCING)
    await tree.set_gate_state("node-a", ScopeGateState.QUIESCED)
    await tree.set_gate_state("node-a", ScopeGateState.RUNNING)

    # 静止中回运行合法：延迟分支复位
    await tree.set_gate_state("node-a", ScopeGateState.QUIESCING)
    quiesced = await tree.set_gate_state("node-a", ScopeGateState.RUNNING)
    assert quiesced.gate_state is ScopeGateState.RUNNING
