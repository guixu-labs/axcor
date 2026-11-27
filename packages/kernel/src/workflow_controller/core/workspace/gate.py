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

"""作用域写入闸门：单作用域内的局部写入静止协议。

协调器对父工作区做路径扫描、合并与检查点提交前，该作用域必须静止：会话
停在工具调用上与同步工具排空由运行时结构性保证，本模块负责其后的后台
写进程有界排空与状态机推进。父代理阻塞在子代理结果上时延迟不可用——
排空失败立即按故障收口，绝不形成「父等结果、结果等集成、集成等排空」
的等待环。
"""

from __future__ import annotations

import asyncio
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = [
    "DEFAULT_BACKGROUND_DRAIN_TIMEOUT_S",
    "GateState",
    "QuiesceOutcome",
    "ScopeGate",
    "ScopeGateError",
]

# 后台写进程排空的默认有界窗口（秒）
DEFAULT_BACKGROUND_DRAIN_TIMEOUT_S = 60.0


class ScopeGateError(RuntimeError):
    """闸门状态机非法使用。"""


class GateState(StrEnum):
    """闸门状态：静止失败为瞬态，故障收口后复位回运行。"""

    RUNNING = "running"
    QUIESCING = "quiescing"
    QUIESCED = "quiesced"
    GATE_FAILED = "gate_failed"


class QuiesceOutcome(StrEnum):
    """排空结果：决定继续闸门内操作、延迟重试还是故障收口。"""

    QUIESCED = "quiesced"
    DEFERRED = "deferred"
    FAILED_FAST = "failed_fast"


# 闸门合法迁移：静止中可回运行（延迟分支），静止失败经收口后复位
_ALLOWED_TRANSITIONS: dict[GateState, frozenset[GateState]] = {
    GateState.RUNNING: frozenset({GateState.QUIESCING}),
    GateState.QUIESCING: frozenset({GateState.QUIESCED, GateState.GATE_FAILED, GateState.RUNNING}),
    GateState.QUIESCED: frozenset({GateState.RUNNING, GateState.GATE_FAILED}),
    GateState.GATE_FAILED: frozenset({GateState.RUNNING}),
}


class ScopeGate:
    """单作用域的写入闸门状态机。"""

    def __init__(
        self, *, background_drain_timeout_s: float = DEFAULT_BACKGROUND_DRAIN_TIMEOUT_S
    ) -> None:
        """绑定后台写进程排空的有界窗口。"""
        self._state = GateState.RUNNING
        self._background_drain_timeout_s = background_drain_timeout_s
        self._failure_reason: str | None = None

    @property
    def state(self) -> GateState:
        """当前闸门状态。"""
        return self._state

    @property
    def failure_reason(self) -> str | None:
        """最近一次静止失败的原因；未失败为 None。"""
        return self._failure_reason

    def begin_quiescing(self) -> None:
        """进入闸门：此后到解冻前拒绝新的写入安排。"""
        self._transition(GateState.QUIESCING)
        self._failure_reason = None

    async def quiesce(
        self,
        drain_background_writers: Callable[[], Awaitable[None]],
        *,
        parent_blocked_on_result: bool,
        allow_deferral: bool = True,
    ) -> QuiesceOutcome:
        """排空后台写并静止；父阻塞或无延迟余地时失败立即收口。

        drain_background_writers 由调用方注入，负责等待后台写进程到达可
        暂停点；超窗或抛错都视为不可静止。父未阻塞且允许延迟时先解冻回
        运行，由调用方延迟到可达边界后重试；边界重试（allow_deferral=False）
        仍不可静止即收口。
        """
        if self._state is GateState.RUNNING:
            self.begin_quiescing()
        elif self._state is not GateState.QUIESCING:
            raise ScopeGateError(f"闸门不在可排空状态：{self._state.value}")
        failure_reason = await self._drain_within_window(drain_background_writers)
        if failure_reason is None:
            self._transition(GateState.QUIESCED)
            return QuiesceOutcome.QUIESCED
        self._failure_reason = failure_reason
        if parent_blocked_on_result or not allow_deferral:
            self._transition(GateState.GATE_FAILED)
            return QuiesceOutcome.FAILED_FAST
        self._transition(GateState.RUNNING)
        return QuiesceOutcome.DEFERRED

    def fail(self, reason: str) -> None:
        """标记不可静止或静止被破坏：携带原因供故障收口使用。"""
        self._transition(GateState.GATE_FAILED)
        self._failure_reason = reason

    def resume_running(self) -> None:
        """解冻：闸门内操作完成，或静止失败经故障收口后复位。"""
        self._transition(GateState.RUNNING)

    async def _drain_within_window(
        self, drain_background_writers: Callable[[], Awaitable[None]]
    ) -> str | None:
        """有界等待排空：成功返回 None，失败返回原因。"""
        try:
            await asyncio.wait_for(
                drain_background_writers(), timeout=self._background_drain_timeout_s
            )
        except TimeoutError:
            return "后台写进程排空超窗"
        except Exception as exc:
            return f"后台写进程排空失败：{exc}"
        return None

    def _transition(self, new_state: GateState) -> None:
        """按状态机合法迁移推进。"""
        if new_state not in _ALLOWED_TRANSITIONS[self._state]:
            raise ScopeGateError(f"非法闸门迁移：{self._state.value} → {new_state.value}")
        self._state = new_state
