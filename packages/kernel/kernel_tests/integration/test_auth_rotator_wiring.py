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

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.asyncio
async def test_orchestrator_attaches_rotator_to_session(tmp_path: Path) -> None:
    """When orchestrator starts a session, the session must receive the
    same rotator instance the orchestrator owns.
    """
    from workflow_controller.core.orchestrator import Orchestrator

    orch = Orchestrator.create_default(config_dir=tmp_path)
    assert orch.auth_rotator is not None

    session = await orch.create_session(name="t", role="worker")
    # session should have the same rotator
    assert getattr(session, "_auth_rotator") is orch.auth_rotator


@pytest.mark.asyncio
async def test_orchestrator_attaches_rotator_to_reviewer_runner(tmp_path: Path) -> None:
    from workflow_controller.core.orchestrator import Orchestrator

    orch = Orchestrator.create_default(config_dir=tmp_path)
    # reviewer_runner is constructed lazily — force construction
    runner = orch.get_or_create_reviewer_runner()
    assert getattr(runner, "_auth_rotator") is orch.auth_rotator


def test_feedback_evaluator_receives_rotator(tmp_path: Path) -> None:
    """When _resolve_evaluator creates a FeedbackEvaluator, it must wire
    the auth_rotator so the evaluator uses the correct auth env for LLM calls.
    """
    from workflow_controller.config.settings import LoopConfig, SessionConfig
    from workflow_controller.core.feedback_evaluator import FeedbackEvaluator
    from workflow_controller.core.orchestrator import Orchestrator

    orch = Orchestrator.create_default(config_dir=tmp_path)
    assert orch.auth_rotator is not None

    # Create a SessionConfig whose loop.evaluator == "feedback" to trigger
    # the FeedbackEvaluator branch in _resolve_evaluator.
    config = SessionConfig(
        name="test-fb",
        role="worker",
        loop=LoopConfig(max_iterations=5, evaluator="feedback"),
    )
    fb_eval = getattr(orch, "_resolve_evaluator")(config)

    assert isinstance(fb_eval, FeedbackEvaluator)
    assert getattr(fb_eval, "_auth_rotator") is orch.auth_rotator


def test_feedback_evaluator_receives_rotator_implicit_branch(tmp_path: Path) -> None:
    """The implicit FeedbackEvaluator branch (evaluator=None, max_iterations>0)
    must also wire the auth_rotator.
    """
    from workflow_controller.config.settings import LoopConfig, SessionConfig
    from workflow_controller.core.feedback_evaluator import FeedbackEvaluator
    from workflow_controller.core.orchestrator import Orchestrator

    orch = Orchestrator.create_default(config_dir=tmp_path)
    assert orch.auth_rotator is not None

    config = SessionConfig(
        name="test-fb-implicit",
        role="worker",
        loop=LoopConfig(max_iterations=3, evaluator=None),
    )
    fb_eval = getattr(orch, "_resolve_evaluator")(config)

    assert isinstance(fb_eval, FeedbackEvaluator)
    assert getattr(fb_eval, "_auth_rotator") is orch.auth_rotator
