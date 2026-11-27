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
import logging
import math
import os
import sys
from typing import TYPE_CHECKING, Any, NamedTuple

from workflow_controller.core._llm_json import extract_json_object
from workflow_controller.core.evaluator import EvaluationResult

if TYPE_CHECKING:
    from workflow_controller.core.auth_rotator import AuthRotator
    from workflow_controller.session.events import Event

logger = logging.getLogger(__name__)

_EVAL_PROMPT = """\
You are a task completion evaluator. Judge whether the task is complete based on the info below.

Current iteration: {iteration}
Evaluation criteria (task goal / completion definition): {criteria}

Recent operations:
{event_summary}

Please evaluate:
1. Completion percentage (float 0.0 to 1.0)
2. Whether the task is complete (true/false) — judge strictly against the criteria
3. One-sentence summary of the current state
4. Specific feedback: what's still missing or incomplete (file names / items). Leave empty if complete.

Reply in the following JSON format only, no other content:
{{"is_complete": false, "progress": 0.5, "summary": "one sentence summary", "feedback": "specific guidance on what's missing"}}

Notes:
- feedback should be specific (file names / concrete items), not vague
- if there are errors or missing deliverables, list them in feedback
- progress should be estimated based on actual completed work against the criteria
"""


class _IterationStats(NamedTuple):
    """单轮事件的聚合读数（评估提示词的摘要原料）。"""

    tools: list[str]
    files: list[str]
    errors: list[str]
    texts: list[str]
    cost: float
    tokens_in: int
    tokens_out: int


def _content_text_blocks(data: dict[str, Any]) -> list[str]:
    """content 块序列中 text 块的原文（保持顺序，不做清洗）。"""
    return [
        block.get("text", "") for block in data.get("content", []) if block.get("type") == "text"
    ]


def _assistant_text_snippets(data: dict[str, Any]) -> list[str]:
    """assistant 文本块去空白后截 200 字符（事件摘要原料）。"""
    snippets: list[str] = []
    for raw in _content_text_blocks(data):
        text = raw.strip()
        if text:
            snippets.append(text[:200])
    return snippets


def _assistant_raw_texts(parts: list[str], data: dict[str, Any]) -> None:
    """把非空白文本块原文追加进 parts（LLM 应答文本拼接）。"""
    for text in _content_text_blocks(data):
        if text.strip():
            parts.append(text)


def _warn_step_error(event: Event) -> None:
    """STEP_FINISH 带错时告警（评估器继续收尾，不中断）。"""
    if event.data.get("is_error"):
        logger.warning("FeedbackEvaluator LLM error: %s", event.data.get("result", "unknown"))


def _error_text(data: dict[str, Any]) -> str:
    """error 事件的展示文本（dict 形态取 message）。"""
    err = data.get("error", "unknown")
    if isinstance(err, dict):
        err = err.get("message", "") or str(err)
    return str(err)


def _notification_errors(data: dict[str, Any]) -> list[str]:
    """notification 事件的错误外显（api_retry 记限流）。"""
    if data.get("subtype", "") == "api_retry":
        return ["API rate limit"]
    return []


class FeedbackEvaluator:
    def __init__(
        self,
        model: str | None = None,
        max_feedback_chars: int = 2000,
        provider_index: int | None = None,
    ) -> None:
        self._model = model
        self._max_feedback_chars = max_feedback_chars
        self._summaries: dict[str, str] = {}
        self._auth_rotator: AuthRotator | None = None
        self._provider_index = provider_index

    @staticmethod
    def _token_value(usage: Any, key: str) -> int:
        """usage 读数的数值校验累计口径：非有限数值（含字符串/None/负数）按 0。"""
        if not isinstance(usage, dict):
            return 0
        value = usage.get(key, 0)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return 0
        # isfinite 只对 float 调用：int 不经转换直接判定，任意大整数都不崩溃；
        # inf 过滤在 int() 之前（int(inf) 抛 OverflowError），JSON 的 1e999 即 inf。
        if isinstance(value, float) and not math.isfinite(value):
            return 0
        return int(value) if value > 0 else 0

    @staticmethod
    def _cost_value(data: Any, key: str) -> float:
        """cost 读数的数值校验口径与 _token_value 相同：非数值/非有限/非正值按 0。"""
        value = data.get(key, 0) if isinstance(data, dict) else 0
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return 0.0
        if isinstance(value, float) and not math.isfinite(value):
            return 0.0
        # 上界用比较而非转换判定：巨大整数（JSON 任意精度）转 float 会溢出抛错。
        if not 0 < value <= sys.float_info.max:
            return 0.0
        return float(value)

    @staticmethod
    def _fmt_tool_input(inp: dict[str, Any] | object, tool_name: str) -> str:
        if not isinstance(inp, dict):
            return str(inp)[:60]
        if tool_name in ("Read", "Write", "Edit"):
            return str(inp.get("file_path", ""))[:80]
        if tool_name == "Bash":
            return str(inp.get("command", ""))[:80]
        if tool_name in ("Glob", "Grep"):
            return str(inp.get("pattern", ""))[:60]
        return str(inp)[:60]

    @staticmethod
    def _parse_response(response: str) -> EvaluationResult:
        data = extract_json_object(response)
        if data is not None:
            try:
                return EvaluationResult(
                    is_complete=bool(data.get("is_complete", False)),
                    progress=float(data.get("progress", 0.0)),
                    summary=str(data.get("summary", "")),
                    feedback=data.get("feedback"),
                    artifacts=data,
                )
            except ValueError as e:
                logger.warning("Failed to parse evaluator JSON: %s", e)

        # Unparseable response: keep summary for UI/audit but DO NOT
        # leak the raw LLM output into feedback. The loop template will
        # substitute feedback=None with the default "继续。", preventing
        # the evaluator's natural-language complaint (e.g.
        # "no information provided") from being fed back as the next
        # iter's worker prompt and triggering a death loop.
        return EvaluationResult(
            is_complete=False,
            progress=0.0,
            summary=response[:200],
            feedback=None,
        )

    def set_auth_rotator(self, rotator: AuthRotator) -> None:
        self._auth_rotator = rotator

    async def evaluate(
        self,
        events: list[Event],
        criteria: dict[str, Any],
        iteration: int,
    ) -> EvaluationResult:
        summary = self._build_event_summary(events, iteration)
        criteria_str = json.dumps(criteria, ensure_ascii=False) if criteria else "无特定标准"

        prompt = _EVAL_PROMPT.format(
            iteration=iteration,
            criteria=criteria_str,
            event_summary=summary,
        )

        try:
            response = await self._call_llm(prompt)
            result = self._parse_response(response)

            sid = events[0].session_id if events else ""
            self._summaries[sid] = result.summary
            if len(self._summaries) > 128:
                # Bound memory in long-running processes; oldest sessions'
                # summaries are no longer read once their loop has ended.
                del self._summaries[next(iter(self._summaries))]

            if result.feedback and len(result.feedback) > self._max_feedback_chars:
                result = EvaluationResult(
                    is_complete=result.is_complete,
                    progress=result.progress,
                    summary=result.summary,
                    feedback=result.feedback[: self._max_feedback_chars],
                    artifacts=result.artifacts,
                )

            return result
        except Exception as e:
            logger.exception("FeedbackEvaluator LLM call failed: %s", e)
            return EvaluationResult(
                is_complete=False,
                progress=0.0,
                summary=f"Evaluation failed: {e}",
                feedback="LLM evaluation unavailable, continuing without feedback",
            )

    def get_summary(self, session_id: str) -> str:
        return self._summaries.get(session_id, "")

    def _build_event_summary(self, events: list[Event], iteration: int) -> str:
        iter_events = [e for e in events if e.iteration == iteration]
        if not iter_events:
            iter_events = events[-50:]

        stats = self._collect_iteration_stats(iter_events)

        parts: list[str] = []
        if stats.tools:
            parts.append("工具调用:")
            parts.extend(stats.tools[-20:])
        if stats.files:
            unique_files = list(dict.fromkeys(stats.files))[-10:]
            parts.append(f"操作文件: {', '.join(unique_files)}")
        if stats.texts:
            parts.append("助手回复摘要:")
            for t in stats.texts[-3:]:
                parts.append(f"  {t}")
        if stats.errors:
            parts.append(f"错误({len(stats.errors)}): {'; '.join(stats.errors[-5:])}")
        if stats.cost > 0:
            parts.append(
                f"费用: ${stats.cost:.4f}, tokens: {stats.tokens_in:,} in / {stats.tokens_out:,} out"
            )

        return "\n".join(parts) if parts else "无事件数据"

    def _collect_iteration_stats(self, iter_events: list[Event]) -> _IterationStats:
        """按事件类型聚合一轮读数（工具/文件/错误/文本/费用与 token）。"""
        tools: list[str] = []
        files: list[str] = []
        errors: list[str] = []
        texts: list[str] = []
        cost = 0.0
        tokens_in = 0
        tokens_out = 0

        for event in iter_events:
            data = event.data
            match (
                event.event_type.value if hasattr(event.event_type, "value") else event.event_type
            ):
                case "assistant_message":
                    texts.extend(_assistant_text_snippets(data))
                case "tool_use":
                    self._record_tool_use(data, tools, files)
                case "result" | "step_finish":
                    cost += self._cost_value(data, "total_cost_usd")
                    usage = data.get("usage", {})
                    # 读数做数值校验后累计：usage/cost 形状异常（缺字段/字符串
                    # 值等，如探测失败回填的占位）按 0 处理，不让评估器统计崩溃
                    # 拖垮整个 loop（与 session/usage.context_input_tokens 的防御口径一致）。
                    tokens_in += self._token_value(usage, "input_tokens")
                    tokens_out += self._token_value(usage, "output_tokens")
                case "error":
                    errors.append(_error_text(data))
                case "notification":
                    errors.extend(_notification_errors(data))

        return _IterationStats(tools, files, errors, texts, cost, tokens_in, tokens_out)

    def _record_tool_use(self, data: dict[str, Any], tools: list[str], files: list[str]) -> None:
        """tool_use 事件读数：调用行 + 涉及文件（从独立 TOOL_USE 事件读，data
        shape: {id, name, input}）。
        """
        name = data.get("name", "?")
        inp = data.get("input", {})
        tools.append(f"  {name}({self._fmt_tool_input(inp, name)})")
        fp = inp.get("file_path", "") if isinstance(inp, dict) else ""
        if fp:
            files.append(str(fp))

    async def _call_llm(self, prompt: str) -> str:
        from workflow_controller.driver.factory import create_driver
        from workflow_controller.session.events import EventType

        sdk_env = (
            self._auth_rotator.env_for_driver(
                dict(os.environ),
                self._provider_index,
            )
            if self._auth_rotator is not None
            else {}
        )

        driver = create_driver()
        parts: list[str] = []
        try:
            async for event in driver.query_for_evaluation(prompt, model=self._model, env=sdk_env):
                if event.event_type == EventType.STEP_FINISH:
                    _warn_step_error(event)
                    break
                if event.event_type == EventType.ASSISTANT_MESSAGE:
                    _assistant_raw_texts(parts, event.data)
        except Exception as e:
            logger.warning("FeedbackEvaluator query error: %s", e)

        return "\n".join(parts).strip()
