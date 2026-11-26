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

"""写入闸门状态机与降级分支测试。"""

from __future__ import annotations

import asyncio
import time

import pytest

from workflow_controller.core.workspace import (
    GateState,
    QuiesceOutcome,
    ScopeGate,
    ScopeGateError,
)


async def _drained_writer() -> None:
    """立即到达可暂停点的后台排空桩。"""
    await asyncio.sleep(0)


async def _never_draining_writer() -> None:
    """永不可静止的后台排空桩。"""
    await asyncio.Event().wait()


async def test_quiesce_reaches_stillness_then_unfreezes() -> None:
    gate = ScopeGate(background_drain_timeout_s=1.0)

    outcome = await gate.quiesce(_drained_writer, parent_blocked_on_result=False)

    assert outcome is QuiesceOutcome.QUIESCED
    assert gate.state is GateState.QUIESCED
    with pytest.raises(ScopeGateError):
        gate.ensure_writes_allowed()

    gate.resume_running()
    assert gate.state is GateState.RUNNING
    gate.ensure_writes_allowed()


async def test_parent_blocked_failure_fails_fast() -> None:
    gate = ScopeGate(background_drain_timeout_s=0.05)

    outcome = await gate.quiesce(_never_draining_writer, parent_blocked_on_result=True)

    assert outcome is QuiesceOutcome.FAILED_FAST
    assert gate.state is GateState.GATE_FAILED
    assert gate.failure_reason is not None

    gate.resume_running()
    assert gate.state is GateState.RUNNING


async def test_unblocked_failure_defers_then_fails_at_boundary() -> None:
    gate = ScopeGate(background_drain_timeout_s=0.05)

    first = await gate.quiesce(_never_draining_writer, parent_blocked_on_result=False)

    assert first is QuiesceOutcome.DEFERRED
    assert gate.state is GateState.RUNNING

    second = await gate.quiesce(
        _never_draining_writer, parent_blocked_on_result=False, allow_deferral=False
    )
    assert second is QuiesceOutcome.FAILED_FAST
    assert gate.state is GateState.GATE_FAILED


async def test_drain_timeout_is_bounded() -> None:
    """排空超窗即失败：窗口外仍在等待的后台进程不得继续阻塞闸门。"""
    gate = ScopeGate(background_drain_timeout_s=0.05)
    started = time.monotonic()

    outcome = await gate.quiesce(_never_draining_writer, parent_blocked_on_result=True)

    assert outcome is QuiesceOutcome.FAILED_FAST
    assert time.monotonic() - started < 1.0


async def test_drain_error_counts_as_not_still() -> None:
    async def broken_drain() -> None:
        """抛错的后台排空桩。"""
        raise RuntimeError("后台进程拒绝暂停")

    gate = ScopeGate(background_drain_timeout_s=1.0)

    outcome = await gate.quiesce(broken_drain, parent_blocked_on_result=True)

    assert outcome is QuiesceOutcome.FAILED_FAST
    assert gate.failure_reason is not None
    assert "后台进程拒绝暂停" in gate.failure_reason


async def test_gate_rejects_illegal_transitions() -> None:
    gate = ScopeGate()
    with pytest.raises(ScopeGateError):
        gate.resume_running()
    with pytest.raises(ScopeGateError):
        gate.fail("运行态不可标记失败")
    gate.begin_quiescing()
    with pytest.raises(ScopeGateError):
        gate.begin_quiescing()
    with pytest.raises(ScopeGateError):
        gate.ensure_writes_allowed()

    fresh = ScopeGate(background_drain_timeout_s=1.0)
    await fresh.quiesce(_drained_writer, parent_blocked_on_result=False)
    with pytest.raises(ScopeGateError):
        await fresh.quiesce(_drained_writer, parent_blocked_on_result=False)

    broken_stillness = ScopeGate(background_drain_timeout_s=1.0)
    await broken_stillness.quiesce(_drained_writer, parent_blocked_on_result=False)
    broken_stillness.fail("静止期被后台写入破坏")
    assert broken_stillness.state is GateState.GATE_FAILED
    assert broken_stillness.failure_reason == "静止期被后台写入破坏"
