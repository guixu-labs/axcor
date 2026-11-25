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

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import asyncio

    from workflow_controller.session.events import Event

logger = logging.getLogger(__name__)


@dataclass
class EvaluationResult:
    is_complete: bool
    progress: float
    summary: str
    feedback: str | None = None
    artifacts: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class Evaluator(Protocol):
    async def evaluate(
        self,
        events: list[Event],
        criteria: dict[str, Any],
        iteration: int,
    ) -> EvaluationResult:
        raise NotImplementedError


class PassThroughEvaluator:
    async def evaluate(
        self,
        events: list[Event],
        criteria: dict[str, Any],
        iteration: int,
    ) -> EvaluationResult:
        return EvaluationResult(
            is_complete=False,
            progress=0.0,
            summary="Pass-through: no evaluation",
        )


class LockedEvaluator:
    """Serialize calls to a legacy evaluator shared by multiple sessions."""

    def __init__(self, inner: Evaluator, lock: asyncio.Lock) -> None:
        self._inner = inner
        self._lock = lock

    async def evaluate(
        self,
        events: list[Event],
        criteria: dict[str, Any],
        iteration: int,
    ) -> EvaluationResult:
        async with self._lock:
            return await self._inner.evaluate(events, criteria, iteration)


class AlwaysCompleteEvaluator:
    """Evaluator that always returns is_complete=True.

    Used for one-shot tasks (e.g. the breather handoff agent writing a
    single handoff doc): the worker does all its work in one pass and
    the loop should stop after it — no LLM quality gate.
    """

    async def evaluate(
        self,
        events: list[Event],
        criteria: dict[str, Any],
        iteration: int,
    ) -> EvaluationResult:
        return EvaluationResult(
            is_complete=True,
            progress=1.0,
            summary="Always-complete: one-shot task (no quality gate)",
        )
