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

"""Synchronous LLM-as-user-substitute reviewer.

Hook handlers (Stop / PreToolUse on AskUserQuestion) ``await`` this service
to decide what to respond as a stand-in user. The service spawns a
``claude`` subprocess via the runner and parses its JSON output into a
:class:`HookDecision`.

This module used to host an asynchronous post-RESULT path (gate-detector
regex scanning, REVISE counters, queue injection). That entire mechanism
has been removed: every human-interaction point now flows through the
synchronous hook → LLM judge pipeline.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from workflow_controller.core.artifact_facts import (
    build_stage_artifact_facts,
    build_stop_artifact_facts,
)
from workflow_controller.core.hook_decision import (
    HookDecision,
    parse_hook_decision_auq,
    parse_hook_decision_stop,
    parse_stage_review,
    render_auq_prompt,
    render_nl_question_prompt,
    render_stage_review_prompt,
    render_stop_prompt,
)

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.core.question_payload import AskQuestion
    from workflow_controller.core.reviewer_runner import ReviewerRunner
    from workflow_controller.session.manager import SessionPool
    from workflow_controller.store.event_router import EventStoreRouter

logger = logging.getLogger(__name__)


@dataclass
class ReviewerRuntimeConfig:
    sandbox_root: Path
    model: str
    timeout_seconds: float
    thinking_budget_tokens: int = 2048
    max_tokens: int = 1024
    task_goal: str = ""
    thinking_mode: str = "disabled"

    def __post_init__(self) -> None:
        # Defensive fallback: an empty task_goal lands the reviewer LLM in
        # "no grounding" mode and it starts inventing artifact names like
        # `spec.md` / `plan.md`. Any caller that constructs this dataclass
        # with task_goal="" (because yaml lacks the reviewer: block, or
        # because an upstream config writer dropped the field) should still
        # get a working reviewer.
        if not (self.task_goal or "").strip():
            from workflow_controller.config.bootstrap import DEFAULT_TASK_GOAL

            self.task_goal = DEFAULT_TASK_GOAL


class ReviewerService:
    """Synchronous LLM judge for Stop / PreToolUse(AUQ) hook callbacks.

    Acts as a stand-in user: when worker triggers a hook, the orchestrator
    awaits :meth:`decide_stop` or :meth:`decide_auq`, which run the reviewer
    LLM subprocess and return a :class:`HookDecision` for the hook handler
    to translate into an HTTP response.
    """

    def __init__(
        self,
        pool: SessionPool,
        event_router: EventStoreRouter,
        config: ReviewerRuntimeConfig,
        runner: ReviewerRunner,
    ) -> None:
        self._pool = pool
        self._event_router = event_router
        self._config = config
        self._runner = runner

    @staticmethod
    def pause() -> None:
        """No-op kept for the ``set_reviewer_mode("manual")`` call path.

        Originally suspended the async auto-replying path. In the sync-only
        model the manual/auto switch is enforced by orchestrator hook
        handlers directly (they read ``_reviewer_mode``), so this is a
        no-op kept callable so the mode-switch flow doesn't crash.
        """
        return

    @staticmethod
    def resume() -> None:
        """Companion no-op to :meth:`pause`."""
        return

    async def start(self) -> None:
        """No-op startup. Kept for API compat with the orchestrator's
        ``await reviewer.start()`` call site. Previously subscribed to the
        event bus for the async post-RESULT path; now hook handlers
        invoke the service directly.
        """

    async def stop(self) -> None:
        """No-op tear-down (was needed when async tasks were tracked)."""

    async def decide_stop(self, context: str) -> HookDecision:
        """Decide whether to let worker exit, called from Stop hook handler.

        Runs the reviewer LLM subprocess synchronously (the caller awaits
        this) with the worker's last assistant message as ``context``.
        Returns a :class:`HookDecision` the caller translates into an HTTP
        response.

        Never raises — subprocess timeout / crash / parse failure all
        produce a safe ``block_with_feedback`` fallback so the worker stays
        alive on reviewer glitches.
        """
        prompt = render_stop_prompt(
            context=context,
            task_goal=self._config.task_goal,
            sandbox_root=str(self._config.sandbox_root),
            artifact_facts=build_stop_artifact_facts(
                context,
                self._config.sandbox_root,
            ),
        )
        try:
            raw = await self._run_reviewer(prompt)
        except TimeoutError:
            logger.warning("decide_stop: reviewer subprocess timed out")
            return HookDecision(
                action="block_with_feedback",
                feedback="继续完成本轮剩余工作。",
                rationale="fallback: reviewer timeout",
            )
        except Exception as exc:
            logger.exception("decide_stop: reviewer subprocess crashed: %s", exc)
            return HookDecision(
                action="block_with_feedback",
                feedback="继续完成本轮剩余工作。",
                rationale=f"fallback: {type(exc).__name__}: {exc}",
            )
        return parse_hook_decision_stop(raw)

    async def review_stage(
        self,
        context: str,
        stage: dict[str, Any],
        next_stage: dict[str, Any],
    ) -> HookDecision:
        """Per-stage semantic review (逐阶段检查点的语义层)。

        worker 刚完成一个阶段、停下汇报。reviewer 只判**这一阶段**的产物实质（不是整轮）。
        返回的 HookDecision 永远是 ``block_with_feedback``（approve→推进下一阶段、reject→
        重做该阶段），per-stage reviewer 不放行 worker 退出（整轮完成归整体 reviewer）。

        approve 时 feedback 留空，由调用方（orchestrator）填标准化的「推进下一阶段」文案；
        reject 时 feedback 是 reviewer 给的「该阶段哪里不合格、怎么改」。镜像 ``decide_stop``：
        render → runner.run → parse；timeout/crash → 安全 fallback「重做该阶段」。
        """
        prompt = render_stage_review_prompt(
            context=context,
            stage=stage,
            next_stage=next_stage,
            sandbox_root=str(self._config.sandbox_root),
            artifact_facts=build_stage_artifact_facts(
                self._config.sandbox_root,
                stage,
            ),
        )
        stage_name = str(stage.get("name", "?"))
        try:
            raw = await self._run_reviewer(prompt)
        except TimeoutError:
            logger.warning("review_stage: reviewer subprocess timed out (stage=%s)", stage_name)
            return HookDecision(
                action="block_with_feedback",
                feedback=f"「{stage_name}」阶段审查超时，请重做该阶段后停下汇报。",
                rationale="fallback: stage review timeout",
            )
        except Exception as exc:
            logger.exception("review_stage: reviewer subprocess crashed: %s", exc)
            return HookDecision(
                action="block_with_feedback",
                feedback=f"「{stage_name}」阶段审查异常，请重做该阶段后停下汇报。",
                rationale=f"fallback: {type(exc).__name__}: {exc}",
            )
        return parse_stage_review(raw)

    async def decide_auq(
        self,
        questions: list[AskQuestion],
    ) -> HookDecision:
        """Decide answers for a batch of AskUserQuestion items.

        Always returns ``block_with_feedback`` — AskUserQuestion cannot be
        "let through" in ``claude -p`` mode. LLM is asked to pick one option
        per question; on any error each question falls back to its first
        option (per :func:`parse_hook_decision_auq`).
        """
        if not questions:
            return HookDecision(
                action="block_with_feedback",
                feedback="继续。",
                rationale="auq: no questions to answer",
            )
        prompt = render_auq_prompt(
            questions=questions,
            task_goal=self._config.task_goal,
            sandbox_root=str(self._config.sandbox_root),
        )
        try:
            raw = await self._run_reviewer(prompt)
        except TimeoutError:
            logger.warning("decide_auq: reviewer subprocess timed out")
            return parse_hook_decision_auq("", questions)
        except Exception as exc:
            logger.exception("decide_auq: reviewer subprocess crashed: %s", exc)
            return parse_hook_decision_auq("", questions)
        return parse_hook_decision_auq(raw, questions)

    async def decide_nl_question(
        self,
        context: str,
    ) -> HookDecision:
        """Auto-answer a natural-language question from the worker.

        Uses a lightweight LLM prompt to generate a direct answer, then
        injects it as feedback so the worker continues without human
        intervention.
        """
        prompt = render_nl_question_prompt(
            context=context,
            task_goal=self._config.task_goal,
        )
        try:
            raw = await self._run_reviewer(prompt)
        except TimeoutError:
            logger.warning("decide_nl_question: reviewer subprocess timed out")
            return HookDecision(
                action="block_with_feedback",
                feedback="继续。",
                rationale="fallback: nl question timeout",
            )
        except Exception as exc:
            logger.exception("decide_nl_question: reviewer crashed: %s", exc)
            return HookDecision(
                action="block_with_feedback",
                feedback="继续。",
                rationale=f"fallback: {type(exc).__name__}: {exc}",
            )
        return parse_hook_decision_stop(raw)  # same JSON format

    async def _run_reviewer(self, prompt: str) -> str:
        """经 runner 跑一次 reviewer 子进程（四个评审入口共用同一组参数）。"""
        return await self._runner.run(
            prompt=prompt,
            sandbox_root=self._config.sandbox_root,
            model=self._config.model,
            timeout_seconds=self._config.timeout_seconds,
            thinking_budget_tokens=self._config.thinking_budget_tokens,
            max_tokens=self._config.max_tokens,
            thinking_mode=self._config.thinking_mode,
        )
