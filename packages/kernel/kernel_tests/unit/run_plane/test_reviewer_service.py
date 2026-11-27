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

from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.question_payload import AskQuestion, QuestionOption
from workflow_controller.core.reviewer_runner import FakeReviewerRunner
from workflow_controller.core.reviewer_service import ReviewerRuntimeConfig, ReviewerService
from workflow_controller.session.manager import SessionPool
from workflow_controller.store.event_router import EventStoreRouter

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.store.event_store import EventStore


@pytest.fixture
def store(tmp_path: Path) -> EventStore:
    return EventStoreRouter(default_project_dir=tmp_path)


@pytest.fixture
def pool() -> SessionPool:
    return SessionPool()


@pytest.fixture
def runtime(tmp_path: Path) -> ReviewerRuntimeConfig:
    return ReviewerRuntimeConfig(
        sandbox_root=tmp_path,
        model="claude-haiku-4-5",
        timeout_seconds=5.0,
        task_goal="跑完 a2h pipeline 5 步骤",
    )


@pytest.fixture
def runner() -> FakeReviewerRunner:
    return FakeReviewerRunner()


@pytest.fixture
def service(
    pool: SessionPool,
    store: EventStore,
    runtime: ReviewerRuntimeConfig,
    runner: FakeReviewerRunner,
) -> ReviewerService:
    return ReviewerService(pool, store, runtime, runner)


class TestDecideStop:
    async def test_thinking_config_reaches_runner(
        self,
        service: ReviewerService,
        runner: FakeReviewerRunner,
    ) -> None:
        getattr(service, "_config").thinking_mode = "enabled"
        getattr(service, "_config").thinking_budget_tokens = 4096
        getattr(service, "_config").max_tokens = 8192
        runner.responses["done"] = '{"action":"let_exit","feedback":"","rationale":"ok"}'
        await service.decide_stop("done")
        assert runner.thinking_calls[-1] == {
            "thinking_budget_tokens": 4096,
            "max_tokens": 8192,
            "thinking_mode": "enabled",
        }

    async def test_let_exit_passthrough(
        self,
        service: ReviewerService,
        runner: FakeReviewerRunner,
    ) -> None:
        runner.responses["worker said done"] = (
            '{"action":"let_exit","feedback":"","rationale":"5/5 done"}'
        )
        d = await service.decide_stop("worker said done")
        assert d.action == "let_exit"
        assert d.rationale == "5/5 done"

    async def test_block_passthrough(
        self,
        service: ReviewerService,
        runner: FakeReviewerRunner,
    ) -> None:
        runner.responses["finished spec"] = (
            '{"action":"block_with_feedback","feedback":"now do plan","rationale":"1/5"}'
        )
        d = await service.decide_stop("finished spec")
        assert d.action == "block_with_feedback"
        assert d.feedback == "now do plan"

    async def test_timeout_falls_back_to_block(
        self,
        service: ReviewerService,
    ) -> None:
        # FakeReviewerRunner raises TimeoutError when no response matches
        d = await service.decide_stop("nothing programmed")
        assert d.action == "block_with_feedback"
        assert d.feedback, "fallback feedback must not be empty"
        assert "timeout" in d.rationale.lower()

    async def test_includes_task_goal_in_prompt(
        self,
        service: ReviewerService,
        runner: FakeReviewerRunner,
    ) -> None:
        runner.responses["跑完 a2h pipeline"] = '{"action":"let_exit","feedback":"","rationale":""}'
        d = await service.decide_stop("anything")
        # Triggered by task_goal substring → confirms goal is rendered in prompt
        assert d.action == "let_exit"


class TestDecideAUQ:
    @staticmethod
    def _q(header: str, options: list[str]) -> AskQuestion:
        return AskQuestion(
            tool_use_id=f"t-{header}",
            question=f"q for {header}?",
            header=header,
            options=[QuestionOption(label=o) for o in options],
        )

    async def test_empty_questions_returns_safe_default(
        self,
        service: ReviewerService,
        runner: FakeReviewerRunner,
    ) -> None:
        d = await service.decide_auq([])
        assert d.action == "block_with_feedback"
        assert d.feedback
        assert runner.call_log == [], "must not invoke runner for empty list"

    async def test_single_question_picks_label(
        self,
        service: ReviewerService,
        runner: FakeReviewerRunner,
    ) -> None:
        runner.responses["Phase choice"] = (
            '{"action":"block_with_feedback",'
            '"answers":[{"header":"Phase choice","selected":"Continue"}],'
            '"rationale":"go"}'
        )
        q = self._q("Phase choice", ["Continue", "Abort"])
        d = await service.decide_auq([q])
        assert d.feedback == "Continue"

    async def test_timeout_falls_back_to_first_option(
        self,
        service: ReviewerService,
    ) -> None:
        q = self._q("h1", ["First-option", "Second"])
        d = await service.decide_auq([q])
        # FakeReviewerRunner not programmed → TimeoutError → fallback
        assert d.action == "block_with_feedback"
        assert d.feedback == "First-option"

    async def test_multi_question_routes_through_runner(
        self,
        service: ReviewerService,
        runner: FakeReviewerRunner,
    ) -> None:
        runner.responses["Phase"] = (
            '{"action":"block_with_feedback",'
            '"answers":['
            '{"header":"Phase","selected":"Yes"},'
            '{"header":"Build","selected":"prod"}'
            "]}"
        )
        qa = self._q("Phase", ["Yes", "No"])
        qb = self._q("Build", ["prod", "dev"])
        d = await service.decide_auq([qa, qb])
        assert d.feedback == "Phase: Yes; Build: prod"


class TestNoOpPauseResume:
    """pause / resume are kept for TUI Reply Auto/Manual toggle compat but
    have no internal effect in the sync-only architecture.
    """

    async def test_pause_then_decide_still_runs(
        self,
        service: ReviewerService,
        runner: FakeReviewerRunner,
    ) -> None:
        runner.responses["x"] = '{"action":"let_exit","feedback":"","rationale":"ok"}'
        service.pause()
        d = await service.decide_stop("x")
        assert d.action == "let_exit", (
            "pause must not suppress sync decision; manual-mode gating "
            "is handled by the orchestrator, not the reviewer"
        )

    async def test_resume_is_callable(self, service: ReviewerService) -> None:
        service.pause()
        service.resume()
        # No exception = pass; resume is a no-op
