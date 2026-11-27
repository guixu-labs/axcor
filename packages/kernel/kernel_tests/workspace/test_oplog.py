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

"""三阶段操作日志与崩溃恢复判定测试。"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.workspace import (
    OPLOG_FILENAME,
    CommitFacts,
    OperationPhase,
    OplogError,
    RecoveryAction,
    SagaOplog,
    operation_marker,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path


def _facts(parent: str, *, op_id: str | None = None) -> CommitFacts:
    """构造判据核验用的提交事实；op_id 为 None 时信息不含操作标识。"""
    message = "workspace commit"
    if op_id is not None:
        message = f"{message}\n{operation_marker(op_id)}"
    return CommitFacts(parent=parent, message=message)


def _inspector(
    facts_by_head: dict[str, CommitFacts],
) -> Callable[[str], Awaitable[CommitFacts | None]]:
    """按头查提交事实的核验桩；未登记的头返回 None。"""

    async def read_commit(head: str) -> CommitFacts | None:
        return facts_by_head.get(head)

    return read_commit


async def _begun(oplog: SagaOplog, client_op_id: str) -> None:
    """登记一条意图阶段的示例操作。"""
    await oplog.begin(
        client_op_id=client_op_id,
        op_kind="integrate_scope",
        expected_old_head="h0",
        scope_epoch=1,
        root_epoch=1,
        metadata={"scope_id": "node-a"},
    )


async def test_begin_is_idempotent_and_rejects_parameter_conflict(tmp_path: Path) -> None:
    oplog = SagaOplog(tmp_path)

    first = await oplog.begin(
        client_op_id="op-1",
        op_kind="integrate_scope",
        expected_old_head="h0",
        scope_epoch=1,
        root_epoch=1,
        metadata={"scope_id": "node-a"},
    )
    replay = await oplog.begin(
        client_op_id="op-1",
        op_kind="integrate_scope",
        expected_old_head="h0",
        scope_epoch=1,
        root_epoch=1,
        metadata={"scope_id": "node-a"},
    )
    assert replay == first

    with pytest.raises(OplogError):
        await oplog.begin(
            client_op_id="op-1",
            op_kind="integrate_scope",
            expected_old_head="h1",
            scope_epoch=1,
            root_epoch=1,
            metadata={"scope_id": "node-a"},
        )
    with pytest.raises(OplogError):
        await oplog.begin(
            client_op_id="op-1",
            op_kind="integrate_scope",
            expected_old_head="h0",
            scope_epoch=2,
            root_epoch=1,
            metadata={"scope_id": "node-a"},
        )
    with pytest.raises(OplogError):
        await oplog.begin(
            client_op_id="op-1",
            op_kind="integrate_scope",
            expected_old_head="h0",
            scope_epoch=1,
            root_epoch=1,
            metadata={"scope_id": "node-b"},
        )


async def test_three_phase_progression_survives_restart(tmp_path: Path) -> None:
    oplog = SagaOplog(tmp_path)
    await _begun(oplog, "op-1")
    await oplog.record_new_head("op-1", "h1")
    committed = await oplog.commit("op-1", result={"new_head": "h1"})
    assert committed.phase is OperationPhase.COMMITTED
    assert (tmp_path / OPLOG_FILENAME).is_file()

    restarted = SagaOplog(tmp_path)
    loaded = await restarted.get("op-1")
    assert loaded == committed
    replay = await restarted.begin(
        client_op_id="op-1",
        op_kind="integrate_scope",
        expected_old_head="h0",
        scope_epoch=1,
        root_epoch=1,
        metadata={"scope_id": "node-a"},
    )
    assert replay.result == {"new_head": "h1"}


async def test_phase_guards_reject_skips(tmp_path: Path) -> None:
    oplog = SagaOplog(tmp_path)
    await _begun(oplog, "op-1")

    with pytest.raises(OplogError):
        await oplog.commit("op-1")

    await oplog.record_new_head("op-1", "h1")
    with pytest.raises(OplogError):
        await oplog.record_new_head("op-1", "h2")
    repeated = await oplog.record_new_head("op-1", "h1")
    assert repeated.phase is OperationPhase.GIT_WRITTEN

    await oplog.commit("op-1")
    with pytest.raises(OplogError):
        await oplog.commit("op-1")
    with pytest.raises(OplogError):
        await oplog.record_new_head("op-1", "h1")


async def test_abandon_only_before_effect(tmp_path: Path) -> None:
    oplog = SagaOplog(tmp_path)
    await _begun(oplog, "op-1")
    await oplog.record_new_head("op-1", "h1")
    with pytest.raises(OplogError):
        await oplog.abandon("op-1")

    await _begun(oplog, "op-2")
    abandoned = await oplog.abandon("op-2")
    assert abandoned.phase is OperationPhase.ABANDONED
    assert [op.client_op_id for op in await oplog.pending_operations()] == ["op-1"]


async def test_recovery_after_commit_before_written_verifies_criteria(tmp_path: Path) -> None:
    """git 提交后、新头落账前崩溃：判据唯一匹配则补记，否则报告分叉。"""
    oplog = SagaOplog(tmp_path)
    await _begun(oplog, "op-1")

    matched = await oplog.resolve(
        "op-1",
        current_head="h-new",
        read_commit=_inspector({"h-new": _facts("h0", op_id="op-1")}),
    )
    assert matched.action is RecoveryAction.COMPLETE_LEDGER
    assert matched.actual_new_head == "h-new"
    operation = await oplog.get("op-1")
    assert operation is not None
    assert operation.phase is OperationPhase.GIT_WRITTEN
    assert operation.actual_new_head == "h-new"

    foreign = SagaOplog(tmp_path / "foreign")
    await _begun(foreign, "op-2")
    stranger = await foreign.resolve(
        "op-2",
        current_head="h-x",
        read_commit=_inspector({"h-x": _facts("h0", op_id="op-9")}),
    )
    assert stranger.action is RecoveryAction.FORK_REPORTED
    stranger_op = await foreign.get("op-2")
    assert stranger_op is not None
    assert stranger_op.phase is OperationPhase.PREPARED

    advanced = SagaOplog(tmp_path / "advanced")
    await _begun(advanced, "op-3")
    later = await advanced.resolve(
        "op-3",
        current_head="h-later",
        read_commit=_inspector({"h-later": _facts("h-new", op_id="op-3")}),
    )
    assert later.action is RecoveryAction.FORK_REPORTED

    unreadable = SagaOplog(tmp_path / "unreadable")
    await _begun(unreadable, "op-4")
    missing = await unreadable.resolve("op-4", current_head="h-gone", read_commit=_inspector({}))
    assert missing.action is RecoveryAction.FORK_REPORTED


async def test_recovery_head_advanced_reports_fork_without_reset(tmp_path: Path) -> None:
    """已记新头后头又前进：停止并报告分叉，账面绝不被改写回退。"""
    oplog = SagaOplog(tmp_path)
    await _begun(oplog, "op-1")
    await oplog.record_new_head("op-1", "h1")

    decision = await oplog.resolve(
        "op-1",
        current_head="h2",
        read_commit=_inspector({"h2": _facts("h1", op_id="op-1")}),
    )

    assert decision.action is RecoveryAction.FORK_REPORTED
    assert decision.actual_new_head is None
    operation = await oplog.get("op-1")
    assert operation is not None
    assert operation.phase is OperationPhase.GIT_WRITTEN
    assert operation.actual_new_head == "h1"

    unreadable = await oplog.resolve("op-1", current_head="h9", read_commit=_inspector({}))
    assert unreadable.action is RecoveryAction.FORK_REPORTED


async def test_resolve_rejects_finished_operations(tmp_path: Path) -> None:
    oplog = SagaOplog(tmp_path)
    await _begun(oplog, "op-1")
    await oplog.record_new_head("op-1", "h1")
    await oplog.commit("op-1")

    with pytest.raises(OplogError):
        await oplog.resolve("op-1", current_head="h1", read_commit=_inspector({}))
    with pytest.raises(OplogError):
        await oplog.resolve("missing", current_head="h1", read_commit=_inspector({}))
