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

from workflow_controller.core.hook_decision import (
    HookDecision,
    format_auq_feedback,
    parse_hook_decision_auq,
    parse_hook_decision_stop,
    parse_stage_review,
    render_auq_prompt,
    render_stage_review_prompt,
    render_stop_prompt,
)
from workflow_controller.core.question_payload import AskQuestion, QuestionOption


def _q(header: str, options: list[str]) -> AskQuestion:
    return AskQuestion(
        tool_use_id=f"toolu_{header}",
        question=f"q for {header}?",
        header=header,
        options=[QuestionOption(label=o) for o in options],
    )


class TestParseHookDecisionStop:
    @staticmethod
    def test_parses_let_exit() -> None:
        d = parse_hook_decision_stop('{"action":"let_exit","feedback":"","rationale":"all 5 done"}')
        assert d == HookDecision("let_exit", "", "all 5 done")

    @staticmethod
    def test_parses_block_with_feedback() -> None:
        d = parse_hook_decision_stop(
            '{"action":"block_with_feedback","feedback":"do plan","rationale":"4 left"}'
        )
        assert d == HookDecision("block_with_feedback", "do plan", "4 left")

    @staticmethod
    def test_unparseable_falls_back_to_block() -> None:
        d = parse_hook_decision_stop("nonsense output not json")
        assert d.action == "block_with_feedback"
        assert d.feedback, "fallback must supply non-empty feedback"
        assert "fallback" in d.rationale.lower()

    @staticmethod
    def test_empty_input_falls_back_to_block() -> None:
        d = parse_hook_decision_stop("")
        assert d.action == "block_with_feedback"
        assert "fallback" in d.rationale.lower()

    @staticmethod
    def test_json_inside_chatter_is_extracted() -> None:
        raw = (
            "Sure! Here's my decision:\n"
            '{"action":"let_exit","feedback":"","rationale":"ok"}\n'
            "Let me know if you need anything else."
        )
        d = parse_hook_decision_stop(raw)
        assert d.action == "let_exit"


class TestParseHookDecisionAUQ:
    @staticmethod
    def test_single_question_match_by_header() -> None:
        q = _q("h1", ["A", "B", "C"])
        d = parse_hook_decision_auq(
            '{"action":"block_with_feedback",'
            '"answers":[{"header":"h1","selected":"B"}],'
            '"rationale":"chose B"}',
            [q],
        )
        assert d.action == "block_with_feedback"
        assert d.feedback == "B"

    @staticmethod
    def test_multi_question_concatenated_feedback() -> None:
        qa = _q("Phase", ["Yes", "No"])
        qb = _q("Build", ["prod", "dev"])
        d = parse_hook_decision_auq(
            '{"action":"block_with_feedback",'
            '"answers":['
            '{"header":"Phase","selected":"Yes"},'
            '{"header":"Build","selected":"prod"}'
            "],"
            '"rationale":"multi"}',
            [qa, qb],
        )
        assert d.feedback == "Phase: Yes; Build: prod"

    @staticmethod
    def test_invalid_label_falls_back_to_first_option() -> None:
        q = _q("h1", ["A", "B"])
        d = parse_hook_decision_auq(
            '{"action":"block_with_feedback","answers":'
            '[{"header":"h1","selected":"completely-fake"}]}',
            [q],
        )
        assert d.feedback == "A"

    @staticmethod
    def test_unparseable_falls_back_to_first_options() -> None:
        qa = _q("h1", ["A", "B"])
        qb = _q("h2", ["X", "Y"])
        d = parse_hook_decision_auq("garbage", [qa, qb])
        assert d.action == "block_with_feedback"
        assert d.feedback == "h1: A; h2: X"

    @staticmethod
    def test_no_questions_returns_default_block() -> None:
        d = parse_hook_decision_auq("ignored", [])
        assert d.action == "block_with_feedback"
        assert d.feedback

    @staticmethod
    def test_positional_match_when_header_missing() -> None:
        q = _q("", ["A", "B"])  # empty header
        d = parse_hook_decision_auq(
            '{"action":"block_with_feedback","answers":[{"header":"anything","selected":"B"}]}',
            [q],
        )
        # Header-match fails (empty target_header), falls through to positional
        assert d.feedback == "B"

    @staticmethod
    def test_selected_with_appended_description_matches_label() -> None:
        # Regression: reviewer once copied "label — description" because the
        # prompt joined them with an em-dash. parser should still resolve back
        # to the bare label, not silently fall back to options[0].
        q = _q("Reference docs", ["跳过", "自动生成 (Recommended)"])
        d = parse_hook_decision_auq(
            '{"action":"block_with_feedback","answers":[{'
            '"header":"Reference docs",'
            '"selected":"自动生成 (Recommended) — 调度 a2h-android-analyzer agent 扫描 OpenCalc 源码"'
            "}]}",
            [q],
        )
        # Must pick the actual reviewer-chosen label, NOT options[0] (which is
        # "跳过" — opposite of what reviewer wanted).
        assert d.feedback == "自动生成 (Recommended)"

    @staticmethod
    def test_longest_label_wins_when_ans_contains_multiple() -> None:
        # If options share a prefix, longest match wins (avoid "yes" matching
        # when "yes I do" is the real choice).
        q = _q("h1", ["yes", "yes I do"])
        d = parse_hook_decision_auq(
            '{"action":"block_with_feedback","answers":'
            '[{"header":"h1","selected":"yes I do — definitely"}]}',
            [q],
        )
        assert d.feedback == "yes I do"


class TestFormatAuqFeedback:
    @staticmethod
    def test_single_uses_just_label() -> None:
        q = _q("ignored", ["A"])
        assert format_auq_feedback([(q, "A")]) == "A"

    @staticmethod
    def test_multiple_uses_header_label_pairs() -> None:
        qa = _q("h1", ["A"])
        qb = _q("h2", ["X"])
        out = format_auq_feedback([(qa, "A"), (qb, "X")])
        assert out == "h1: A; h2: X"

    @staticmethod
    def test_empty_returns_empty() -> None:
        assert format_auq_feedback([]) == ""


class TestRenderStopPrompt:
    @staticmethod
    def test_includes_context_and_task_goal() -> None:
        out = render_stop_prompt(
            context="worker said spec done",
            task_goal="complete 5 stages",
            sandbox_root="/tmp/sb",
        )
        assert "worker said spec done" in out
        assert "complete 5 stages" in out
        # Hard constraint must be present
        assert "let_exit" in out
        assert "block_with_feedback" in out

    @staticmethod
    def test_required_items_blocked_by_external_dependencies_cannot_exit() -> None:
        """Keep the external-blocker rule in the production stop prompt."""
        prompt = render_stop_prompt(
            context="本轮已做完产物，整个任务要求的设备验收仍待设备释放。",
            task_goal="产物落盘并通过设备验收",
            sandbox_root="/tmp",
        )

        release, blocking = prompt.split("2. **block_with_feedback 的常见情境**:", 1)
        rule = "必需项因外部依赖未就绪而未完成时,必须返回 block_with_feedback"
        assert "会话任务目标下的全部必需项已完成" in release
        assert "能做的全部工作" not in prompt
        assert rule not in release
        assert rule in blocking
        assert "已做完能做的, 剩下卡外部" not in prompt

    @staticmethod
    def test_empty_task_goal_omits_goal_block() -> None:
        out = render_stop_prompt(
            context="x",
            task_goal="",
            sandbox_root="/tmp/sb",
        )
        # The goal block has a literal header marker we can probe for
        assert "## 会话任务目标" not in out


class TestRenderAuqPrompt:
    @staticmethod
    def test_includes_all_questions_and_options() -> None:
        qa = _q("Phase", ["A", "B"])
        qb = _q("Build", ["prod", "dev"])
        out = render_auq_prompt([qa, qb], "goal", "/tmp/sb")
        assert "Phase" in out
        assert "Build" in out
        assert "A" in out
        assert "prod" in out
        # Skeleton hint must list both headers
        assert "Phase" in out
        assert "Build" in out

    @staticmethod
    def test_includes_n_questions_count() -> None:
        qa = _q("h1", ["A"])
        out = render_auq_prompt([qa], "goal", "/tmp/sb")
        assert " 1 " in out  # n_questions=1 appears somewhere


class TestHookDecisionWaitAndRetry:
    """Tests for the new wait_and_retry action introduced in design.md §11."""

    @staticmethod
    def test_dataclass_accepts_wait_and_retry_action():
        d = HookDecision(action="wait_and_retry", rationale="等 sub-agent")
        assert d.action == "wait_and_retry"
        assert d.feedback == ""
        assert d.rationale == "等 sub-agent"

    @staticmethod
    def test_parse_wait_and_retry_happy_path():
        raw = '{"action": "wait_and_retry", "rationale": "等 sub-agent 完成"}'
        decision = parse_hook_decision_stop(raw)
        assert decision.action == "wait_and_retry"
        assert decision.feedback == ""
        assert decision.rationale == "等 sub-agent 完成"

    @staticmethod
    def test_parse_wait_and_retry_missing_rationale_degrades_to_block():
        """Per design.md §11.7: missing/bad rationale → fall back to
        block_with_feedback to keep worker safe (don't accidentally exit it).
        feedback carries the raw JSON so worker / logs see what reviewer sent;
        rationale contains 'degraded' so the degradation is searchable.
        """
        raw = '{"action": "wait_and_retry"}'  # no rationale
        decision = parse_hook_decision_stop(raw)
        assert decision.action == "block_with_feedback"
        assert decision.feedback == raw  # worker sees reviewer's raw output
        assert "degraded" in decision.rationale  # debuggable in logs

    @staticmethod
    def test_parse_wait_and_retry_extra_fields_ignored():
        """Reviewer may include hint fields (resume_prompt etc.) — they're
        ignored per design.md §11.4 (orchestrator uses fixed template).
        """
        raw = (
            '{"action": "wait_and_retry", "rationale": "等",'
            ' "resume_prompt": "ignored", "wait_seconds": 999}'
        )
        decision = parse_hook_decision_stop(raw)
        assert decision.action == "wait_and_retry"
        assert decision.rationale == "等"

    @staticmethod
    def test_parse_unknown_action_degrades_to_block():
        """Any unknown action string → block_with_feedback fallback. Preserves
        safety under reviewer prompt regressions. feedback carries raw JSON
        and rationale contains 'degraded' for log diagnosability.
        """
        raw = '{"action": "explode", "rationale": "x"}'
        decision = parse_hook_decision_stop(raw)
        assert decision.action == "block_with_feedback"
        assert decision.feedback == raw  # worker sees reviewer's raw output
        assert "degraded" in decision.rationale  # debuggable in logs


class TestRenderStopPromptWaitAndRetry:
    @staticmethod
    def test_prompt_contains_wait_and_retry_section():
        """The rendered prompt must include the wait_and_retry few-shot
        section so reviewer LLM can learn the pattern (design.md §11.5).
        """
        prompt = render_stop_prompt(
            context="dummy",
            task_goal="dummy goal",
            sandbox_root="/tmp/sb",
        )
        assert "wait_and_retry" in prompt
        assert "等" in prompt  # at least one Chinese example present
        assert "我等通知" in prompt or "等 sub-agent" in prompt

    @staticmethod
    def test_prompt_examples_block_default_wait_rare():
        """wait_and_retry is the rare exception, block_with_feedback the
        default. The few-shot section has the narrow wait example plus
        several block examples — and block must outnumber wait (design.md
        §11.5).
        """
        prompt = render_stop_prompt(
            context="dummy",
            task_goal="g",
            sandbox_root="/tmp",
        )
        # The narrow wait_and_retry example is present...
        assert prompt.count('"action": "wait_and_retry"') >= 1
        # ...and block_with_feedback (the default) appears more often.
        assert prompt.count('"action": "block_with_feedback"') >= 3
        assert prompt.count('"action": "block_with_feedback"') > prompt.count(
            '"action": "wait_and_retry"'
        )


# ── per-stage review prompt + parse（reviewer 逐阶段评判用）───────────────


class TestStageReviewPrompt:
    @staticmethod
    def test_render_contains_stage_and_facts() -> None:
        prompt = render_stage_review_prompt(
            context="spec 写完了：...",
            stage={"name": "spec", "artifact": "spec.md", "description": "需求"},
            next_stage={"name": "plan", "artifact": "plan.md"},
            sandbox_root="/tmp/proj",
            artifact_facts="| spec.md | 有内容 (200B) |",
        )
        assert "spec" in prompt
        assert "plan" in prompt  # 下一阶段
        assert "/tmp/proj" in prompt
        assert "有内容" in prompt  # artifact 事实

    @staticmethod
    def test_parse_approve() -> None:
        d = parse_stage_review('{"action":"approve","feedback":"","rationale":"spec 实质"}')
        # approve → block_with_feedback（proceed），per-stage reviewer 永不 let_exit
        assert d.action == "block_with_feedback"

    @staticmethod
    def test_parse_reject() -> None:
        d = parse_stage_review(
            '{"action":"reject","feedback":"spec 太薄，重写","rationale":"缺边界 case"}'
        )
        assert d.action == "block_with_feedback"
        assert "重写" in d.feedback

    @staticmethod
    def test_parse_malformed_fallback() -> None:
        """malformed → 安全 fallback（block，保 worker 不退出）。"""
        d = parse_stage_review("not json at all")
        assert d.action == "block_with_feedback"
