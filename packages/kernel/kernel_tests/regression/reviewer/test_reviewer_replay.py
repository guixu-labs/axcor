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

"""Reviewer prompt regression replay.

Loads every YAML fixture in fixtures/, renders the prompt using the
CURRENT _STOP_JUDGE_PROMPT / _AUQ_JUDGE_PROMPT template (via the
production render_stop_prompt / render_auq_prompt functions), then sends
it to the reviewer LLM via the deterministic HTTP driver and asserts
the resulting decision against the fixture's `expected` block.

The point: as you iterate on hook_decision.py's prompt template, this
suite measures whether the change actually fixed historical bugs without
breaking the positive case (fixture 06).

Tests are marked `live_llm` — `pytest` alone skips them; opt in with
`pytest --live`. Each fixture is one parametrize case so pytest reports
which specific scenarios still fail.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
import yaml

from workflow_controller.config.bootstrap import DEFAULT_TASK_GOAL
from workflow_controller.core.artifact_facts import build_stop_artifact_facts
from workflow_controller.core.hook_decision import (
    HookDecision,
    parse_hook_decision_auq,
    parse_hook_decision_stop,
    render_auq_prompt,
    render_stop_prompt,
)
from workflow_controller.core.question_payload import AskQuestion, QuestionOption
from workflow_controller.core.reviewer_runner import DriverReviewerRunner

_FIXTURES_DIR = Path(__file__).parent / "fixtures"
# Real model the HttpDriver will call. Reads the configured GLM model from env
# (the driver strips any CLI-only [variant] suffix); falls back to a real GLM
# model name when the env isn't set.
_REVIEWER_MODEL = os.environ.get("ANTHROPIC_MODEL", "glm-5.2")
_REVIEWER_TIMEOUT = 300.0


# ---------------------------------------------------------------------------
# Fixture loading
# ---------------------------------------------------------------------------


def _load_fixtures() -> list[tuple[str, dict[str, Any]]]:
    """Return [(fixture_id, fixture_data), ...] sorted by filename."""
    paths = sorted(_FIXTURES_DIR.glob("*.yaml"))
    return [(p.stem, yaml.safe_load(p.read_text(encoding="utf-8"))) for p in paths]


def _resolve_task_goal(value: str | None) -> str:
    """Empty / missing task_goal in fixture → fall back to DEFAULT_TASK_GOAL
    (same fallback the production ReviewerRuntimeConfig.__post_init__ does).
    """
    if not (value or "").strip():
        return DEFAULT_TASK_GOAL
    return value


def _build_auq_questions(
    fixture_questions: list[dict[str, Any]],
) -> list[AskQuestion]:
    out: list[AskQuestion] = []
    for i, q in enumerate(fixture_questions):
        opts = [
            QuestionOption(
                label=o["label"],
                description=o.get("description", ""),
            )
            for o in q.get("options", [])
        ]
        out.append(
            AskQuestion(
                tool_use_id=f"toolu_fixture_{i}",
                question=q["question"],
                header=q.get("header", ""),
                multi_select=q.get("multi_select", False),
                options=opts,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Reviewer invocation
# ---------------------------------------------------------------------------


async def _call_reviewer(prompt: str, sandbox_root: Path) -> str:
    """Call the reviewer LLM via the deterministic HTTP driver; return raw text."""
    runner = DriverReviewerRunner()
    return await runner.run(
        prompt=prompt,
        sandbox_root=sandbox_root,
        model=_REVIEWER_MODEL,
        timeout_seconds=_REVIEWER_TIMEOUT,
    )


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------


def _assert_stop(
    decision: HookDecision,
    expected: dict[str, Any],
    label: str,
) -> None:
    expected_action = expected["action"]
    assert decision.action == expected_action, (
        f"[{label}] action mismatch: got '{decision.action}', "
        f"expected '{expected_action}'. "
        f"feedback={decision.feedback!r}, rationale={decision.rationale!r}"
    )
    feedback = decision.feedback
    for needle in expected.get("feedback_must_include", []):
        assert needle in feedback, (
            f"[{label}] feedback missing required substring {needle!r}.\n"
            f"  actual feedback: {feedback!r}"
        )
    for forbidden in expected.get("feedback_must_not_include", []):
        assert forbidden not in feedback, (
            f"[{label}] feedback contains forbidden substring {forbidden!r}.\n"
            f"  actual feedback: {feedback!r}"
        )


def _assert_auq(
    decision: HookDecision,
    expected: dict[str, Any],
    label: str,
) -> None:
    """AUQ assertions. parse_hook_decision_auq returns a HookDecision whose
    .feedback contains the selected label(s). For single question, feedback
    IS the label verbatim; for multi-question it's "header1: label1; ...".
    """
    expected_action = expected["action"]
    assert decision.action == expected_action, f"[{label}] action mismatch: got '{decision.action}'"
    selected_text = decision.feedback

    # auq_must_select_one_of / auq_must_not_select are dicts keyed by header
    for header, allowed in expected.get("auq_must_select_one_of", {}).items():
        ok = any(lbl in selected_text for lbl in allowed)
        assert ok, (
            f"[{label}] header={header!r}: selected NOT in allowed list.\n"
            f"  allowed: {allowed}\n  selected_text: {selected_text!r}"
        )
    for header, forbidden_list in expected.get(
        "auq_must_not_select",
        {},
    ).items():
        for forbidden in forbidden_list:
            assert forbidden not in selected_text, (
                f"[{label}] header={header!r}: selected matched forbidden "
                f"label {forbidden!r}.\n  selected_text: {selected_text!r}"
            )


# ---------------------------------------------------------------------------
# The replay test
# ---------------------------------------------------------------------------

_FIXTURES = _load_fixtures()


def _stage_disk(fixture: dict[str, Any], tmp_root: Path) -> Path:
    """Stage declared paths with fixture-specific or generic file content."""
    staged = fixture.get("staged_disk")
    if staged is None:
        return Path(fixture["sandbox_root"])
    staged_entries = staged if isinstance(staged, dict) else dict.fromkeys(staged)
    for rel_path, content in staged_entries.items():
        full = tmp_root / rel_path
        if rel_path.endswith("/"):
            # Directory deliverable (e.g. `src/`).
            full.mkdir(parents=True, exist_ok=True)
        else:
            full.parent.mkdir(parents=True, exist_ok=True)
            if content is None:
                content = (
                    f"# {rel_path}\n\n"
                    "Auto-generated fixture content representing a completed, "
                    "substantive deliverable. Multiple paragraphs of realistic body "
                    "text so the artifact-facts truth table classifies this file as "
                    "non-stub.\n"
                )
            full.write_text(content, encoding="utf-8")
    return tmp_root


@pytest.mark.live_llm
@pytest.mark.parametrize(
    "fixture_id,fixture",
    _FIXTURES,
    ids=[fid for fid, _ in _FIXTURES],
)
async def test_reviewer_replay(
    fixture_id: str,
    fixture: dict[str, Any],
    tmp_path: Path,
) -> None:
    kind = fixture["kind"]
    sandbox_root = _stage_disk(fixture, tmp_path)
    task_goal = _resolve_task_goal(fixture.get("task_goal"))
    expected = fixture["expected"]
    repeat = int(fixture.get("repeat", 1))

    # ---- render the prompt with current production template ----
    questions: list[AskQuestion] | None = None
    if kind == "stop_hook":
        prompt = render_stop_prompt(
            context=fixture["worker_context"],
            task_goal=task_goal,
            sandbox_root=str(sandbox_root),
            artifact_facts=build_stop_artifact_facts(
                fixture["worker_context"],
                sandbox_root,
            ),
        )
    elif kind == "auq":
        questions = _build_auq_questions(fixture["auq_questions"])
        prompt = render_auq_prompt(
            questions=questions,
            task_goal=task_goal,
            sandbox_root=str(sandbox_root),
        )
    else:
        pytest.fail(f"unknown fixture kind: {kind!r}")
        return  # for type checker

    # ---- call real LLM (repeated for stability fixtures) ----
    results: list[HookDecision] = []
    for run_i in range(repeat):
        run_label = f"{fixture_id}" if repeat == 1 else f"{fixture_id} run {run_i + 1}/{repeat}"
        raw = await _call_reviewer(prompt, sandbox_root)
        if kind == "stop_hook":
            decision = parse_hook_decision_stop(raw)
            _assert_stop(decision, expected, run_label)
        else:
            assert questions is not None
            decision = parse_hook_decision_auq(raw, questions)
            _assert_auq(decision, expected, run_label)
        results.append(decision)

    # ---- stability assertion (fixture 7) ----
    if repeat > 1:
        stab = fixture.get("stability_assertion", {})
        if stab.get("same_action", False):
            actions = {r.action for r in results}
            assert len(actions) == 1, (
                f"[{fixture_id}] stability fail: {repeat} runs produced "
                f"different actions: {sorted(actions)}"
            )
