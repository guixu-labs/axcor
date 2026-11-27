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

import json
from datetime import UTC, datetime
from unittest.mock import ANY, Mock, patch

import pytest

from workflow_controller.core.feedback_evaluator import _EVAL_PROMPT, FeedbackEvaluator
from workflow_controller.driver.codex_cli import CodexCLIDriver
from workflow_controller.session.events import Event, EventType


def test_eval_prompt_is_generic_not_translation_specific() -> None:
    """通用化：prompt 不该再有翻译专用措辞，该按 criteria 判完成。"""
    assert "translation" not in _EVAL_PROMPT.lower()
    assert "criteria" in _EVAL_PROMPT.lower()
    assert "complete" in _EVAL_PROMPT.lower()


def _make_event(
    event_type: EventType,
    data: dict | None = None,
    iteration: int = 1,
    session_id: str = "test-session",
) -> Event:
    return Event(
        id="fake-id",
        session_id=session_id,
        iteration=iteration,
        timestamp=datetime.now(tz=UTC),
        event_type=event_type,
        data=data or {},
    )


def _make_assistant_event(
    text: str = "",
    iteration: int = 1,
) -> Event:
    """构造只含 text 块的 ASSISTANT_MESSAGE。工具调用走独立 TOOL_USE 事件
    （见 _make_tool_use_event）。
    """
    content: list[dict] = []
    if text:
        content.append({"type": "text", "text": text})
    return _make_event(
        EventType.ASSISTANT_MESSAGE,
        {"content": content, "model": "claude-sonnet-4-6"},
        iteration=iteration,
    )


def _make_tool_use_event(
    tool_name: str = "Read",
    file_path: str = "/src/Main.java",
    iteration: int = 1,
) -> Event:
    """构造 TOOL_USE 事件，data = {id, name, input}。"""
    return _make_event(
        EventType.TOOL_USE,
        {
            "id": "tu-1",
            "name": tool_name,
            "input": {"file_path": file_path, "command": "ls", "pattern": "*.java"},
        },
        iteration=iteration,
    )


def _make_result_event(cost: float = 0.05, iteration: int = 1) -> Event:
    return _make_event(
        EventType.STEP_FINISH,
        {
            "total_cost_usd": cost,
            "num_turns": 3,
            "is_error": False,
            "usage": {"input_tokens": 5000, "output_tokens": 1200},
            "modelUsage": {},
        },
        iteration=iteration,
    )


class TestFeedbackEvaluatorParseResponse:
    @staticmethod
    def test_parse_valid_json() -> None:
        response = '{"is_complete": false, "progress": 0.6, "summary": "Good progress", "feedback": "Fix imports"}'
        result = getattr(FeedbackEvaluator, "_parse_response")(response)
        assert result.is_complete is False
        assert result.progress == 0.6
        assert result.summary == "Good progress"
        assert result.feedback == "Fix imports"

    @staticmethod
    def test_parse_json_embedded_in_text() -> None:
        response = (
            "Here is my evaluation:\n"
            '{"is_complete": true, "progress": 1.0, "summary": "Done", "feedback": null}\n'
            "Hope this helps!"
        )
        result = getattr(FeedbackEvaluator, "_parse_response")(response)
        assert result.is_complete is True
        assert result.progress == 1.0

    @staticmethod
    def test_parse_invalid_json_falls_back() -> None:
        response = "The project is about 60% complete."
        result = getattr(FeedbackEvaluator, "_parse_response")(response)
        assert result.is_complete is False
        assert result.progress == 0.0
        assert result.summary == response

    @staticmethod
    def test_parse_invalid_json_does_not_leak_into_feedback() -> None:
        """Critical: non-JSON LLM response must NOT be set as feedback,
        else it becomes next iter's worker prompt and causes a death loop
        (the 'based on the following information' bug).
        """
        response = (
            "I don't see any information attached to your message — "
            "the prompt ends with 'based on the following information' "
            "but no content follows."
        )
        result = getattr(FeedbackEvaluator, "_parse_response")(response)
        assert result.feedback is None

    @staticmethod
    def test_parse_empty_response() -> None:
        result = getattr(FeedbackEvaluator, "_parse_response")("")
        assert result.is_complete is False
        assert result.feedback is None


class TestFeedbackEvaluatorBuildSummary:
    @staticmethod
    def test_summary_with_tools_and_files() -> None:
        # summary builder 从独立 TOOL_USE 事件读工具名 / file_path。
        evaluator = FeedbackEvaluator()
        events = [
            _make_tool_use_event("Read", "/src/Main.java", iteration=1),
            _make_tool_use_event("Edit", "/src/Main.ets", iteration=1),
            _make_result_event(cost=0.05, iteration=1),
        ]
        summary = getattr(evaluator, "_build_event_summary")(events, 1)
        assert "Read" in summary
        assert "Edit" in summary
        assert "Main.java" in summary
        assert "$0.05" in summary

    @staticmethod
    def test_summary_malformed_cost_and_usage_do_not_crash() -> None:
        # cost 与 usage 同为事件上的外部读数：字符串/None 占位按 0 累计，
        # 评估器统计不得崩溃（此前 cost 只挡 None，字符串仍会 TypeError）。
        evaluator = FeedbackEvaluator()
        events = [
            _make_event(
                EventType.STEP_FINISH,
                {
                    "total_cost_usd": "0",
                    "num_turns": 1,
                    "is_error": False,
                    "usage": {"input_tokens": "unknown", "output_tokens": None},
                    "modelUsage": {},
                },
                iteration=1,
            ),
            _make_result_event(cost=0.05, iteration=1),
        ]
        summary = getattr(evaluator, "_build_event_summary")(events, 1)
        assert "$0.05" in summary
        assert "5,000 in" in summary

    @staticmethod
    def test_summary_nonfinite_readings_do_not_crash() -> None:
        # JSON 合法字面量 1e999 经 json.loads 解析为 inf：int(inf) 抛
        # OverflowError，非有限读数须在累计前过滤为 0；巨大整数 cost
        # （任意精度）转 float 同样会溢出，一并按 0。
        evaluator = FeedbackEvaluator()
        events = [
            _make_event(
                EventType.STEP_FINISH,
                {
                    "total_cost_usd": float("inf"),
                    "num_turns": 1,
                    "is_error": False,
                    "usage": json.loads('{"input_tokens": 1e999, "output_tokens": NaN}'),
                    "modelUsage": {},
                },
                iteration=1,
            ),
            _make_event(
                EventType.STEP_FINISH,
                {
                    "total_cost_usd": 10**400,
                    "num_turns": 1,
                    "is_error": False,
                    "usage": {},
                    "modelUsage": {},
                },
                iteration=1,
            ),
            _make_result_event(cost=0.05, iteration=1),
        ]
        summary = getattr(evaluator, "_build_event_summary")(events, 1)
        assert "$0.05" in summary
        assert "5,000 in" in summary
        assert "inf" not in summary

    @staticmethod
    def test_summary_with_errors() -> None:
        evaluator = FeedbackEvaluator()
        events = [
            _make_event(EventType.ERROR, {"error": "build failed"}, iteration=1),
            _make_event(
                EventType.NOTIFICATION,
                {"subtype": "api_retry"},
                iteration=1,
            ),
        ]
        summary = getattr(evaluator, "_build_event_summary")(events, 1)
        assert "error" in summary.lower() or "API" in summary

    @staticmethod
    def test_summary_empty_events() -> None:
        evaluator = FeedbackEvaluator()
        summary = getattr(evaluator, "_build_event_summary")([], 1)
        assert summary == "" or len(summary) > 0


def _text_response_sdk_query(mock_response: str):
    """构造按标准两段式（Assistant 文本 + Result）吐出响应的假 sdk query。"""
    from claude_agent_sdk.types import AssistantMessage, ResultMessage, TextBlock

    async def mock_sdk_query(*args, **kwargs):
        yield AssistantMessage(
            content=[TextBlock(text=mock_response)],
            model="claude-sonnet-4-6",
        )
        yield ResultMessage(
            subtype="success",
            duration_ms=100,
            duration_api_ms=100,
            is_error=False,
            num_turns=1,
            session_id="test",
        )

    return mock_sdk_query


class TestFeedbackEvaluatorLLMCall:
    @pytest.mark.asyncio
    async def test_evaluate_calls_llm_and_parses(self) -> None:
        evaluator = FeedbackEvaluator()
        mock_response = (
            '{"is_complete": false, "progress": 0.3, "summary": "Starting",'
            ' "feedback": "Add error handling"}'
        )

        with patch("claude_agent_sdk.query", side_effect=_text_response_sdk_query(mock_response)):
            events = [_make_assistant_event(iteration=1), _make_result_event(iteration=1)]
            result = await evaluator.evaluate(events, {}, 1)

        assert result.is_complete is False
        assert result.progress == 0.3
        assert result.feedback == "Add error handling"

    @pytest.mark.asyncio
    async def test_evaluate_falls_back_on_llm_failure(self) -> None:
        evaluator = FeedbackEvaluator()

        with patch("claude_agent_sdk.query", side_effect=Exception("SDK error")):
            events = [_make_assistant_event(iteration=1)]
            result = await evaluator.evaluate(events, {}, 1)

        assert result.is_complete is False
        assert result.feedback is None

    @pytest.mark.asyncio
    async def test_summary_cache(self) -> None:
        evaluator = FeedbackEvaluator()
        mock_response = (
            '{"is_complete": true, "progress": 1.0, "summary": "All done", "feedback": null}'
        )

        with patch("claude_agent_sdk.query", side_effect=_text_response_sdk_query(mock_response)):
            events = [
                _make_event(
                    EventType.ASSISTANT_MESSAGE, {"content": [], "model": "x"}, session_id="s1"
                )
            ]
            await evaluator.evaluate(events, {}, 1)

        assert evaluator.get_summary("s1") == "All done"
        assert evaluator.get_summary("nonexistent") == ""

    @pytest.mark.asyncio
    async def test_feedback_truncated_if_too_long(self) -> None:
        evaluator = FeedbackEvaluator(max_feedback_chars=50)
        long_feedback = "x" * 200
        mock_response = json.dumps(
            {
                "is_complete": False,
                "progress": 0.1,
                "summary": "test",
                "feedback": long_feedback,
            }
        )

        with patch("claude_agent_sdk.query", side_effect=_text_response_sdk_query(mock_response)):
            events = [_make_assistant_event()]
            result = await evaluator.evaluate(events, {}, 1)

        assert len(result.feedback) == 50

    @pytest.mark.asyncio
    async def test_non_codex_runtime_passes_tool_blacklist(self) -> None:
        """非 codex 反馈评估靠工具黑名单限权；codex 分支走 read-only，不吃黑名单。"""
        captured: dict = {}

        async def capturing_query(*args, **kwargs):
            captured.update(kwargs)
            async for msg in _text_response_sdk_query(
                '{"is_complete": true, "progress": 1.0, "summary": "done", "feedback": null}'
            )(*args, **kwargs):
                yield msg

        with patch("claude_agent_sdk.query", side_effect=capturing_query):
            evaluator = FeedbackEvaluator()
            events = [_make_assistant_event(iteration=1), _make_result_event(iteration=1)]
            await evaluator.evaluate(events, {}, 1)

        options = captured["options"]
        for tool in ("Bash", "Write", "Edit", "Skill", "Read"):
            assert tool in options.disallowed_tools

    @pytest.mark.parametrize("configured_model", [None, "explicit-model"])
    async def test_codex_feedback_uses_read_only_and_resolves_model(
        self, monkeypatch, configured_model
    ):
        """The Codex feedback query keeps its sandbox and model selection."""
        monkeypatch.setenv("WORKFLOW_CONTROLLER_DRIVER", "codex_cli")
        env = {"ANTHROPIC_MODEL": "provider-model"}
        rotator = Mock()
        rotator.env_for_driver.return_value = env
        evaluator = FeedbackEvaluator(model=configured_model)
        evaluator.set_auth_rotator(rotator)

        async def feedback_events():
            yield _make_assistant_event('{"is_complete": true, "progress": 1.0}')

        with patch.object(CodexCLIDriver, "query", return_value=feedback_events()) as query:
            result = await evaluator.evaluate([_make_assistant_event()], {}, 1)

        assert result.is_complete
        query.assert_called_once_with(
            ANY, model=configured_model or "provider-model", permission_mode="read-only", env=env
        )
