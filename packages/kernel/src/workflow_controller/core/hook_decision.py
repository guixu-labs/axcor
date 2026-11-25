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

"""LLM hook decision logic.

When worker tries to stop or calls AskUserQuestion, the corresponding hook
handler invokes the reviewer LLM as an automated pipeline inspector. The LLM
returns a structured ``HookDecision``:

- For Stop hook: ``let_exit`` (worker may exit), ``block_with_feedback``
  (keep worker running with feedback), or ``wait_and_retry`` (worker exits;
  orchestrator respawns after 600s via --resume; see design.md §11).
- For AskUserQuestion: always ``block_with_feedback`` with the LLM's chosen
  option label(s) as feedback (since ``-p`` mode can't really wait for a
  real human anyway).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from workflow_controller.core._llm_json import extract_json_object
from workflow_controller.core._stop_prompts import (
    _AUQ_JUDGE_PROMPT,
    _NL_QUESTION_PROMPT,
    _STAGE_REVIEW_PROMPT,
    _STOP_JUDGE_PROMPT,
    _WAIT_AND_RETRY_SECTION,
)

if TYPE_CHECKING:
    from workflow_controller.core.question_payload import AskQuestion

logger = logging.getLogger(__name__)

HookAction = Literal["let_exit", "block_with_feedback", "wait_and_retry"]


@dataclass
class HookDecision:
    """Reviewer decision for a stop hook. Three actions:
    - let_exit             : let worker exit naturally
    - block_with_feedback  : keep worker alive, inject feedback as user msg
    - wait_and_retry       : worker exits, orchestrator sleeps 600s then
                             respawns with --resume (design.md §11)
    """

    action: HookAction
    feedback: str = ""  # block reason text; empty when action is let_exit or wait_and_retry
    rationale: str = ""


# Prompt templates. {task_goal_block} renders to "" or "\n## 会话任务目标\n...\n".


def render_task_goal_block(task_goal: str) -> str:
    cleaned = (task_goal or "").strip()
    if not cleaned:
        return ""
    return f"\n## 会话任务目标\n{cleaned}\n"


def _format_auq_options(q: AskQuestion) -> list[str]:
    """单个问题的 options 渲染行。

    label 与 description 分行输出、label 加引号，让 LLM 清楚看到该复制进
    ``selected`` 的字符串——此前用 " — " 连接会让模型整串复制，破坏
    parse_hook_decision_auq 的 ``in option_labels`` 校验。
    """
    if not q.options:
        return ["  options: (none)"]
    lines = ["  options:"]
    for opt in q.options:
        lines.append(f"    - label: {opt.label!r}")
        if opt.description:
            lines.append(f"      description: {opt.description}")
    return lines


def format_auq_questions_block(questions: list[AskQuestion]) -> str:
    """Render question list as numbered block for the LLM prompt."""
    parts: list[str] = []
    for i, q in enumerate(questions, start=1):
        header = q.header or "(no header)"
        parts.append(f"[Q{i}] header={header!r}")
        parts.append(f"  question: {q.question}")
        parts.extend(_format_auq_options(q))
    return "\n".join(parts)


def build_answers_skeleton(questions: list[AskQuestion]) -> str:
    """Build inline answers-array example with real values.

    Uses each question's first option label as the example selected value
    so the LLM sees a complete, valid JSON object, not a placeholder.
    """
    parts: list[str] = []
    for q in questions:
        header = (q.header or "").replace('"', '\\"')
        # Use first option label as example value, not a placeholder
        example_label = q.options[0].label.replace('"', '\\"') if q.options else "继续"
        parts.append(f'{{"header":"{header}","selected":"{example_label}"}}')
    return ",".join(parts)


def render_stop_prompt(
    context: str,
    task_goal: str,
    sandbox_root: str,
    artifact_facts: str = "",
) -> str:
    """Render the Stop hook judgment prompt.

    ``artifact_facts`` (optional) is a caller-supplied markdown fact table
    injected into the prompt; pass ``""`` to omit. Kept as a parameter so
    this function stays pure (filesystem-free) and easy to unit-test.
    """
    return (
        _STOP_JUDGE_PROMPT.format(
            context=context,
            task_goal_block=render_task_goal_block(task_goal),
            sandbox_root=sandbox_root,
            artifact_facts=artifact_facts,
        )
        + _WAIT_AND_RETRY_SECTION
    )


def render_nl_question_prompt(
    context: str,
    task_goal: str,
) -> str:
    """Render a prompt for answering a natural-language question from the worker."""
    return _NL_QUESTION_PROMPT.format(
        context=context,
        task_goal_block=render_task_goal_block(task_goal),
    )


def render_auq_prompt(
    questions: list[AskQuestion],
    task_goal: str,
    sandbox_root: str,
) -> str:
    return _AUQ_JUDGE_PROMPT.format(
        n_questions=len(questions),
        questions_block=format_auq_questions_block(questions),
        answers_skeleton=build_answers_skeleton(questions),
        task_goal_block=render_task_goal_block(task_goal),
        sandbox_root=sandbox_root,
    )


def parse_hook_decision_stop(raw: str) -> HookDecision:
    """Parse LLM output for Stop hook context.

    Falls back to ``block_with_feedback`` on unparseable output — keeping
    worker running is safer than letting it exit on a malformed response.
    """
    text = raw.strip()
    if not text:
        return _stop_fallback("empty reviewer response")

    data = extract_json_object(text)
    if data is None:
        return _stop_fallback(f"unparseable: {text[:120]}")

    action_raw = str(data.get("action", "")).strip().lower()
    feedback = str(data.get("feedback", "")).strip()
    rationale = str(data.get("rationale", "")).strip()

    if action_raw == "let_exit":
        return HookDecision(action="let_exit", feedback="", rationale=rationale)

    if action_raw == "wait_and_retry":
        if rationale:
            return HookDecision(action="wait_and_retry", feedback="", rationale=rationale)
        # Missing/empty rationale → degrade to block_with_feedback (design.md §11.7)
        return HookDecision(
            action="block_with_feedback",
            feedback=raw,
            rationale="wait_and_retry missing rationale, degraded",
        )

    if action_raw != "block_with_feedback":
        # Unknown action → safe fallback (design.md §11.7)
        return HookDecision(
            action="block_with_feedback",
            feedback=raw,
            rationale="unknown action, degraded",
        )

    if not feedback:
        feedback = "继续完成本轮剩余工作。"
    return HookDecision(
        action="block_with_feedback",
        feedback=feedback,
        rationale=rationale or f"action={action_raw}",
    )


def render_stage_review_prompt(
    context: str,
    stage: dict[str, Any],
    next_stage: dict[str, Any],
    sandbox_root: str,
    artifact_facts: str = "",
) -> str:
    """Render the per-stage review prompt (judges ONE stage, not the whole task).

    ``stage`` / ``next_stage`` are stage dicts from the project's ``stages:``
    (name/artifact/description). ``artifact_facts`` is the caller-supplied disk
    truth for the stage's declared artifact (see ``build_stage_artifact_facts``)。
    Pure / filesystem-free for easy unit-testing.
    """
    return _STAGE_REVIEW_PROMPT.format(
        context=context,
        stage_name=stage.get("name", "?"),
        stage_artifact=stage.get("artifact", "?"),
        stage_description=stage.get("description", "") or "(未声明)",
        next_name=next_stage.get("name", "?"),
        next_artifact=next_stage.get("artifact", "?"),
        sandbox_root=sandbox_root,
        artifact_facts=artifact_facts,
    )


def parse_stage_review(raw: str) -> HookDecision:
    """Parse per-stage reviewer output.

    ``approve`` / ``reject`` both map to ``block_with_feedback``——per-stage
    reviewer 永不放行 worker 退出（整轮完成与否归整体 reviewer）。approve 的 feedback
    由调用方填「推进下一阶段」；reject 的 feedback 是 reviewer 给的「重做该阶段」。
    malformed → 安全 block fallback（保 worker 不退出）。
    """
    text = raw.strip()
    if not text:
        return _stop_fallback("empty stage-review response")
    data = extract_json_object(text)
    if data is None:
        return _stop_fallback(f"unparseable: {text[:120]}")
    action_raw = str(data.get("action", "")).strip().lower()
    feedback = str(data.get("feedback", "")).strip()
    rationale = str(data.get("rationale", "")).strip()
    if action_raw == "approve":
        # feedback 留空——调用方（orchestrator）填「推进下一阶段」的标准化文案
        return HookDecision(
            action="block_with_feedback",
            feedback=feedback,
            rationale=rationale or "stage approved",
        )
    if action_raw == "reject":
        if not feedback:
            feedback = "该阶段产物未通过审查，请重做。"
        return HookDecision(
            action="block_with_feedback",
            feedback=feedback,
            rationale=rationale or "stage rejected",
        )
    # 未知 action → 安全 fallback
    return HookDecision(
        action="block_with_feedback",
        feedback=feedback or raw,
        rationale=f"unknown stage action={action_raw}, degraded",
    )


def _match_answer_by_header(
    answers_raw: list[Any],
    target_header: str,
    option_labels: list[str],
) -> str | None:
    """按 header 找回答并映射选项 label（无 header / 未命中返回 None）。"""
    for ans_raw in answers_raw:
        if not isinstance(ans_raw, dict):
            continue
        ans_header = str(ans_raw.get("header", "")).strip()
        ans_selected = str(ans_raw.get("selected", "")).strip()
        if ans_header == target_header:
            matched = _match_option_label(ans_selected, option_labels)
            if matched is not None:
                return matched
    return None


def _match_answer_positional(ans_raw: Any, option_labels: list[str]) -> str | None:
    """按序号位的回答映射选项 label（非 dict / 未命中返回 None）。"""
    if not isinstance(ans_raw, dict):
        return None
    ans_selected = str(ans_raw.get("selected", "")).strip()
    return _match_option_label(ans_selected, option_labels)


def parse_hook_decision_auq(
    raw: str,
    questions: list[AskQuestion],
) -> HookDecision:
    """Parse LLM output for AskUserQuestion hook context.

    AUQ always returns ``block_with_feedback`` (let_exit makes no sense —
    worker calling AUQ wants to pause). Labels are validated against each
    question's options; mismatches fall back to the first option per question.
    """
    if not questions:
        return HookDecision(
            action="block_with_feedback",
            feedback="继续。",
            rationale="auq fallback: no questions to answer",
        )

    text = raw.strip()
    if not text:
        return _auq_fallback(questions, "empty reviewer response")

    data = extract_json_object(text)
    if data is None:
        return _auq_fallback(questions, f"unparseable: {text[:120]}")

    answers_raw = data.get("answers", [])
    if not isinstance(answers_raw, list):
        return _auq_fallback(questions, "answers not a list")

    rationale = str(data.get("rationale", "")).strip()

    paired: list[tuple[AskQuestion, str]] = []
    for i, question in enumerate(questions):
        option_labels = [opt.label for opt in question.options]
        chosen_label: str | None = None

        # 1. Try match by header
        if question.header:
            chosen_label = _match_answer_by_header(answers_raw, question.header, option_labels)

        # 2. Fallback to positional match
        if chosen_label is None and i < len(answers_raw):
            chosen_label = _match_answer_positional(answers_raw[i], option_labels)

        # 3. Final fallback: first option
        if chosen_label is None:
            chosen_label = option_labels[0] if option_labels else ""

        paired.append((question, chosen_label))

    return HookDecision(
        action="block_with_feedback",
        feedback=format_auq_feedback(paired),
        rationale=rationale or "auq decision",
    )


def format_auq_feedback(paired: list[tuple[AskQuestion, str]]) -> str:
    """Format AUQ answers as deny-reason text the worker sees.

    Single question → just the label.
    Multiple questions → "header1: label1; header2: label2".
    """
    if not paired:
        return ""
    if len(paired) == 1:
        _, label = paired[0]
        return label
    parts: list[str] = []
    for q, label in paired:
        header = q.header or q.question[:30]
        parts.append(f"{header}: {label}")
    return "; ".join(parts)


def _match_option_label(
    ans_selected: str,
    option_labels: list[str],
) -> str | None:
    """Map a possibly-decorated `selected` value back to one of the option labels.

    Reviewer sometimes copies more than the bare label — e.g. when prompts show
    options as ``label — description``, the model treats the whole concatenation
    as the label. Match leniently: exact first, then by longest label that is a
    prefix/substring of ``ans_selected`` (longest-first so ``"yes I do"`` beats
    a stray ``"yes"`` when both are options).
    """
    if not ans_selected or not option_labels:
        return None
    if ans_selected in option_labels:
        return ans_selected
    for label in sorted(option_labels, key=len, reverse=True):
        if not label:
            continue
        if ans_selected.startswith(label):
            return label
        if label in ans_selected:
            return label
    return None


def _stop_fallback(reason: str) -> HookDecision:
    logger.warning("HookDecision (stop) fallback: %s", reason)
    return HookDecision(
        action="block_with_feedback",
        feedback="继续完成本轮剩余工作。",
        rationale=f"fallback: {reason}",
    )


def _auq_fallback(
    questions: list[AskQuestion],
    reason: str,
) -> HookDecision:
    logger.warning("HookDecision (auq) fallback: %s", reason)
    paired: list[tuple[AskQuestion, str]] = []
    for q in questions:
        label = q.options[0].label if q.options else ""
        paired.append((q, label))
    return HookDecision(
        action="block_with_feedback",
        feedback=format_auq_feedback(paired),
        rationale=f"fallback: {reason}",
    )
