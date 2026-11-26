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

"""ClaudeSDKDriver — uses claude-agent-sdk Python package."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import re
import signal
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from workflow_controller.core._subagent import summarize_subagent_transcript
from workflow_controller.core.auth_rotator.signal import LimitMatch, LimitSignal, Scope
from workflow_controller.driver._content import extract_user_message_blocks
from workflow_controller.driver._permission import effective_permission_mode
from workflow_controller.driver.claude_events import tool_pair_events
from workflow_controller.driver.claude_lineage import ClaudeSubagentLineage
from workflow_controller.driver.cli_base import CLIDriverBase, QueryOptions, error_end_events
from workflow_controller.driver.hooks import (
    DISPATCH_TOOL_NAMES,
    SIDE_EFFECT_TOOL_NAMES,
    BarrierDeny,
    BarrierProceed,
    is_dispatch_tool,
    is_side_effect_tool,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

    from workflow_controller.driver.hooks import DriverHooks
    from workflow_controller.session.events import Event

logger = logging.getLogger(__name__)

_sdk_setsid_patched = False

# 副作用工具与 task 派发工具的 PreToolUse matcher（与驱动层分类器同源）
_SIDE_EFFECT_MATCHER = "|".join(sorted(SIDE_EFFECT_TOOL_NAMES))
_DISPATCH_MATCHER = "|".join(sorted(DISPATCH_TOOL_NAMES))


def _patch_sdk_setsid() -> None:
    """Patch claude_agent_sdk 的 SubprocessCLITransport,让 claude 子进程独立成进程组。

    SDK 默认 connect() 不 setsid,close() 的 SIGTERM/SIGKILL 只打到 claude 主进程,
    fork 出来的孙进程(node / mcp)变孤儿继续吃 CPU。给 anyio.open_process 加
    start_new_session=True 后,claude 进程独立成组,kill() 的 killpg 能整组 reap。

    SDK internal API,失败静默降级(kill 兜底失效,但 stop() 仍 cancel 正常)。
    幂等(模块级 patch 一次)。
    """
    global _sdk_setsid_patched
    if _sdk_setsid_patched:
        return
    try:
        from claude_agent_sdk._internal.transport import subprocess_cli as _cli

        _orig_open = _cli.anyio.open_process  # type: ignore[attr-defined]

        async def _open_with_session(*args: Any, **kwargs: Any) -> Any:
            kwargs.setdefault("start_new_session", True)
            return await _orig_open(*args, **kwargs)

        _cli.anyio.open_process = _open_with_session  # type: ignore[attr-defined]
        _sdk_setsid_patched = True
    except Exception:
        logger.debug("SDK setsid patch failed (internals changed?)", exc_info=True)


def _make_transport(prompt: str, options: Any) -> Any:
    """构造 SubprocessCLITransport 让 driver 持有 _process 句柄(kill 用 pid)。

    SDK internal API,失败返回 None(query 走默认 transport,kill 兜底失效)。
    """
    try:
        from claude_agent_sdk._internal.transport.subprocess_cli import (
            SubprocessCLITransport,
        )

        return SubprocessCLITransport(prompt=prompt, options=options)
    except Exception:
        return None


class ClaudeSDKDriver(CLIDriverBase):
    """Runs Claude via the claude-agent-sdk library (in-process, no subprocess)."""

    def __init__(
        self,
        *,
        permission_mode: str = "bypassPermissions",
        add_dirs: list[str] | None = None,
        resume: str | None = None,
        fork_session: bool = False,
    ) -> None:
        super().__init__(permission_mode=permission_mode)
        self._add_dirs: list[str] = add_dirs or []
        self._resume: str | None = resume
        self._fork_session = fork_session
        self._query_task: asyncio.Task[None] | None = None
        self._transport: Any = None  # SubprocessCLITransport(kill 用其 _process.pid)
        # Last assistant text seen this query — fed to the Stop hook (the SDK's
        # StopHookInput carries no final message).
        self._last_assistant_text: str = ""
        self._lineage = ClaudeSubagentLineage()

    @property
    def is_running(self) -> bool:
        return self._query_task is not None and not self._query_task.done()

    async def stop(self) -> None:
        self._stop_event.set()
        task = self._query_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def kill(self) -> None:
        # stop() 已让 SDK 优雅 close(让 session 文件落盘);kill() 强杀整个
        # 进程组(含孙进程)。依赖 query() 注入的 transport + setsid patch。
        transport = self._transport
        if transport is None:
            return
        proc = getattr(transport, "_process", None)
        pid = getattr(proc, "pid", None) if proc is not None else None
        if not pid:
            return
        with contextlib.suppress(ProcessLookupError, OSError):
            os.killpg(os.getpgid(pid), signal.SIGKILL)

    def _query_events(self, prompt: str, opts: QueryOptions) -> AsyncIterator[Event]:
        """返回 SDK 查询事件流。"""
        return self._query_event_stream(prompt, opts)

    async def _query_event_stream(self, prompt: str, opts: QueryOptions) -> AsyncIterator[Event]:
        """执行一次 SDK 查询（消息转换见 ``_sdk_to_events``）。"""
        iteration = opts.iteration
        session_id = opts.session_id
        self._last_assistant_text = ""
        self._query_task = asyncio.current_task()

        try:
            from claude_agent_sdk import query as sdk_query
            from claude_agent_sdk.types import ClaudeAgentOptions
        except ImportError:
            self._query_task = None
            for event in error_end_events(
                "claude-agent-sdk not installed",
                session_id=session_id,
                iteration=iteration,
                end_reason="internal_error",
            ):
                yield event
            return

        self._lineage = ClaudeSubagentLineage()
        options = self._build_agent_options(opts, ClaudeAgentOptions)
        _patch_sdk_setsid()
        self._transport = _make_transport(prompt, options)
        try:
            async for event in self._run_sdk_query(
                sdk_query, prompt, options, session_id, iteration, opts.hooks
            ):
                yield event
        finally:
            self._query_task = None

    async def _run_sdk_query(
        self,
        sdk_query: Callable[..., Any],
        prompt: str,
        options: Any,
        session_id: str,
        iteration: int,
        hooks: DriverHooks | None = None,
    ) -> AsyncIterator[Event]:
        """流式消费 SDK 消息产出事件；异常路径发限流/错误事件对收尾。"""
        from workflow_controller.session.events import Event, EventType

        try:
            async for msg in sdk_query(prompt=prompt, options=options, transport=self._transport):
                if self._stop_event.is_set():
                    break
                await self._notify_subagent_starts(msg, session_id, hooks)
                for event in self._sdk_events_tracked(msg, session_id, iteration):
                    yield event
            yield Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.ITERATION_END,
                data={"reason": "sdk_query_exit"},
            )
        except Exception as e:
            logger.exception("SDK query error session=%s", session_id)
            # Recognized limit/auth errors → RATE_LIMIT control signal
            for limit_ev in _sdk_limit_events(session_id, iteration, e):
                yield limit_ev
            # 异常路径收尾标记：事件流消费者（如前端拓扑归档）依赖
            # iteration_end 判定 query 边界；不进 finally（aclose 风险）。
            for event in error_end_events(
                str(e), session_id=session_id, iteration=iteration, end_reason="internal_error"
            ):
                yield event

    async def _notify_subagent_starts(
        self,
        msg: object,
        session_id: str,
        hooks: DriverHooks | None,
    ) -> None:
        if hooks is None or hooks.on_subagent_start is None:
            return
        for start in self._lineage.observe_sdk_message(msg):
            if start["agent_id"]:
                await hooks.on_subagent_start(session_id, start)

    def _sdk_events_tracked(
        self,
        msg: object,
        session_id: str,
        iteration: int,
    ) -> Iterator[Event]:
        """产出单条 SDK 消息的事件；途经 ASSISTANT_MESSAGE 时记录最后文本。"""
        from workflow_controller.session.events import EventType

        for event in _sdk_to_events(msg, session_id, iteration):
            if event.event_type == EventType.ASSISTANT_MESSAGE:
                text = _event_last_assistant_text(event)
                if text:
                    self._last_assistant_text = text
            yield event

    def _build_agent_options(self, opts: QueryOptions, options_cls: Any) -> Any:
        """构造 ClaudeAgentOptions（权限模式、工具面、resume 与 hook 配置）。"""
        return options_cls(
            model=opts.model or None,
            cwd=opts.cwd,
            max_turns=opts.max_turns,
            allowed_tools=list(opts.allowed_tools) if opts.allowed_tools else [],
            disallowed_tools=list(opts.disallowed_tools) if opts.disallowed_tools else [],
            permission_mode=effective_permission_mode(  # type: ignore[arg-type]
                opts.permission_mode or self._permission_mode,
            ),
            include_partial_messages=True,
            env=opts.env or dict(os.environ),
            add_dirs=list(self._add_dirs) if self._add_dirs else [],
            resume=self._resume or "",
            fork_session=self._fork_session,
            system_prompt=opts.system_prompt or None,
            hooks=self._build_sdk_hooks(opts.hooks) if opts.hooks is not None else None,  # type: ignore[arg-type]
        )

    def _build_sdk_hooks(self, hooks: DriverHooks) -> dict[str, Any]:
        """Adapt driver-agnostic DriverHooks to claude_agent_sdk's in-process
        hook config. Callbacks translate the SDK payload to the CC-style dict
        the orchestrator expects and pass its CC-style result straight back
        (same hook protocol), so reviewer logic stays in the orchestrator.
        """
        from claude_agent_sdk.types import HookMatcher

        pre_cb = functools.partial(_sdk_pre_tool_cb, hooks=hooks)
        post_cb = functools.partial(_sdk_post_tool_cb, hooks=hooks)
        post_fail_cb = functools.partial(_sdk_post_tool_failure_cb, post_cb=post_cb)
        stop_cb = functools.partial(
            _sdk_stop_cb, hooks=hooks, get_last_text=lambda: self._last_assistant_text
        )
        subagent_start_cb = functools.partial(
            _sdk_subagent_start_cb, hooks=hooks, lineage=self._lineage
        )
        subagent_stop_cb = functools.partial(_sdk_subagent_stop_cb, hooks=hooks)
        sdk_hooks: dict[str, Any] = {}
        pre_matchers: list[Any] = []
        if hooks.on_pre_tool_use is not None:
            pre_matchers.append(HookMatcher(matcher="AskUserQuestion", hooks=[pre_cb]))  # type: ignore[list-item]
            pre_matchers.append(
                HookMatcher(matcher=_SIDE_EFFECT_MATCHER, hooks=[pre_cb])  # type: ignore[list-item]
            )
        elif hooks.before_side_effect_tool is not None:
            # ⑤ 副作用屏障独立接线（无 reviewer 拦截面时仍拦副作用工具）
            pre_matchers.append(
                HookMatcher(matcher=_SIDE_EFFECT_MATCHER, hooks=[pre_cb])  # type: ignore[list-item]
            )
        if hooks.before_subagent_dispatch is not None:
            # ① task 派发前置屏障：Task 工具执行（即子代理创建）前触发
            pre_matchers.append(
                HookMatcher(matcher=_DISPATCH_MATCHER, hooks=[pre_cb])  # type: ignore[list-item]
            )
        if pre_matchers:
            sdk_hooks["PreToolUse"] = pre_matchers
        if hooks.on_post_tool_use is not None:
            sdk_hooks["PostToolUse"] = [HookMatcher(matcher="*", hooks=[post_cb])]  # type: ignore[list-item]
            sdk_hooks["PostToolUseFailure"] = [
                HookMatcher(matcher="*", hooks=[post_fail_cb])  # type: ignore[list-item]
            ]
        if hooks.on_stop is not None:
            sdk_hooks["Stop"] = [HookMatcher(hooks=[stop_cb])]  # type: ignore[list-item]
        if hooks.on_subagent_start is not None:
            sdk_hooks["SubagentStart"] = [HookMatcher(hooks=[subagent_start_cb])]  # type: ignore[list-item]
        if hooks.on_subagent_stop is not None:
            sdk_hooks["SubagentStop"] = [HookMatcher(hooks=[subagent_stop_cb])]  # type: ignore[list-item]
        return sdk_hooks


def _sdk_to_events(msg: object, session_id: str, iteration: int) -> list[Event]:
    """Convert a claude-agent-sdk message to Workflow Controller Events (may produce multiple)."""
    t = type(msg).__name__

    if t == "AssistantMessage":
        return _sdk_assistant_events(msg, session_id, iteration)

    if t == "ResultMessage":
        # 必须排在下面的 subtype 兜底之前：SDK 0.2.128 起 ResultMessage 也带
        # subtype='success' + session_id，兜底会抢先把它映射成 ITERATION_START，
        # STEP_FINISH 永远不出现——worker validation 探针只认它判成功，丢失即
        # 「ended without a successful step」、全部 agent 会话被验证拦死
        return _sdk_result_events(msg, session_id, iteration)

    subtype = getattr(msg, "subtype", None)
    if subtype is not None:
        return _sdk_system_events(msg, subtype, session_id, iteration)

    if t == "UserMessage":
        return [_sdk_user_event(msg, session_id, iteration)]

    return []


def _event_last_assistant_text(event: Event) -> str:
    """ASSISTANT_MESSAGE 事件里最后一个非空 text block 的文本（无则空串）。"""
    text = ""
    for block in event.data.get("content", []):
        if isinstance(block, dict) and block.get("type") == "text":
            candidate = block.get("text", "")
            if candidate.strip():
                text = candidate
    return text


def _sdk_assistant_events(msg: object, session_id: str, iteration: int) -> list[Event]:
    """AssistantMessage：text/thinking block → ASSISTANT_MESSAGE，工具块经配对发事件。"""
    from workflow_controller.session.events import Event, EventType

    msg_blocks: list[dict[str, Any]] = []
    tool_uses: dict[str, dict[str, Any]] = {}
    tool_results: dict[str, dict[str, Any]] = {}
    for b in getattr(msg, "content", []) or []:
        bt = type(b).__name__
        if bt == "TextBlock":
            text = getattr(b, "text", "")
            if text.strip():
                msg_blocks.append({"type": "text", "text": text})
        elif bt == "ToolUseBlock":
            block: dict[str, Any] = {
                "type": "tool_use",
                "id": getattr(b, "id", ""),
                "name": getattr(b, "name", ""),
                "input": getattr(b, "input", {}),
            }
            tool_uses[block["id"]] = block
        elif bt == "ToolResultBlock":
            result_block: dict[str, Any] = {
                "type": "tool_result",
                "tool_use_id": getattr(b, "tool_use_id", ""),
                "content": b.content,
                "is_error": getattr(b, "is_error", False),
            }
            tool_results[result_block["tool_use_id"]] = result_block
        elif bt == "ThinkingBlock":
            msg_blocks.append({"type": "thinking", "thinking": getattr(b, "thinking", "")})
    events: list[Event] = []
    if msg_blocks:
        events.append(
            Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.ASSISTANT_MESSAGE,
                data={"content": msg_blocks, "model": getattr(msg, "model", "")},
            )
        )
    return events + tool_pair_events(
        tool_uses, tool_results, session_id=session_id, iteration=iteration
    )


def _sdk_result_events(msg: object, session_id: str, iteration: int) -> list[Event]:
    """ResultMessage → STEP_FINISH（必须先于 subtype 分流判型，见 ``_sdk_to_events``）。"""
    from workflow_controller.session.events import Event, EventType

    usage = getattr(msg, "usage", {}) or {}
    model_usage = getattr(msg, "model_usage", {}) or {}
    sf_data = {
        "session_id": getattr(msg, "session_id", ""),
        "duration_ms": getattr(msg, "duration_ms", 0),
        "num_turns": getattr(msg, "num_turns", 0),
        "total_cost_usd": float(getattr(msg, "total_cost_usd", 0) or 0),
        "is_error": getattr(msg, "is_error", False),
        "usage": dict(usage) if isinstance(usage, dict) else {},
        "modelUsage": dict(model_usage) if isinstance(model_usage, dict) else {},
    }
    return [
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.STEP_FINISH,
            data=sf_data,
        ),
    ]


def _sdk_system_events(
    msg: object,
    subtype: str,
    session_id: str,
    iteration: int,
) -> list[Event]:
    """SystemMessage 及其子类 → ITERATION_START（带 session_id）或 NOTIFICATION。"""
    from workflow_controller.session.events import Event, EventType

    # 初始化消息建立会话映射；task 消息只是子代理进度，不能作为 query 边界。
    data = {"type": "system", "subtype": subtype, **dict(getattr(msg, "data", {}) or {})}
    sid = getattr(msg, "session_id", None)
    if sid:
        data["session_id"] = sid
    if sid and not subtype.startswith("task_"):
        return [
            Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.ITERATION_START,
                data=data,
            )
        ]
    return [
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.NOTIFICATION,
            data=data,
        )
    ]


def _sdk_user_event(msg: object, session_id: str, iteration: int) -> Event:
    """UserMessage → USER_MESSAGE（content 经 extract_user_message_blocks 规整）。"""
    from workflow_controller.session.events import Event, EventType

    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.USER_MESSAGE,
        data={"content": extract_user_message_blocks(getattr(msg, "content", []))},
    )


# ── SDK hook callbacks (module-level; bound to hooks via functools.partial) ──


async def _sdk_pre_tool_cb(
    input_data: dict[str, Any], tool_use_id: str | None, context: object, *, hooks: DriverHooks
) -> dict[str, Any]:
    tool_name = str(input_data.get("tool_name", ""))
    data = {
        "tool_name": tool_name,
        "tool_input": input_data.get("tool_input", {}),
        "tool_use_id": tool_use_id or input_data.get("tool_use_id", ""),
        "native_session_id": input_data.get("session_id", ""),
        "parent_session_id": input_data.get("parent_session_id", ""),
        "parent_tool_use_id": input_data.get("parent_tool_use_id", ""),
        "agent_id": input_data.get("agent_id", ""),
        "agent_type": input_data.get("agent_type", ""),
        "prompt_id": input_data.get("prompt_id", ""),
        # 子代理工作区声明（reader 快照）经工具输入透传；未声明按 writer
        "workspace": (input_data.get("tool_input") or {}).get("workspace", "writer")
        if isinstance(input_data.get("tool_input"), dict)
        else "writer",
    }
    # 前置屏障（⑤ 副作用 / ① task 派发）：拒绝即阻断工具执行
    barrier_outcome: BarrierProceed | BarrierDeny | None = None
    if is_dispatch_tool(tool_name) and hooks.before_subagent_dispatch is not None:
        barrier_outcome = await hooks.before_subagent_dispatch(hooks.session_id, data)
    elif is_side_effect_tool(tool_name) and hooks.before_side_effect_tool is not None:
        barrier_outcome = await hooks.before_side_effect_tool(hooks.session_id, data)
    if isinstance(barrier_outcome, BarrierDeny):
        reason = barrier_outcome.reason
        if barrier_outcome.fault:
            reason = f"{reason}（{barrier_outcome.fault}）"
        return {"decision": "block", "reason": reason}
    result: dict[str, Any] = {}
    if hooks.on_pre_tool_use is not None:
        decision = await hooks.on_pre_tool_use(hooks.session_id, data)
        result = decision or {}
    if result.get("decision") == "block":
        return result
    if (
        isinstance(barrier_outcome, BarrierProceed)
        and barrier_outcome.child_cwd
        and is_dispatch_tool(tool_name)
    ):
        # ② cwd 注入：PreToolUse updatedInput 重写 Task 输入，子代理在
        # 作用域工作区内启动
        tool_input = dict(input_data.get("tool_input") or {})
        tool_input["cwd"] = barrier_outcome.child_cwd
        return {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow",
                "updatedInput": tool_input,
            }
        }
    return result


async def _sdk_post_tool_cb(
    input_data: dict[str, Any], tool_use_id: str | None, context: object, *, hooks: DriverHooks
) -> dict[str, Any]:
    if hooks.on_post_tool_use is not None:
        await hooks.on_post_tool_use(
            hooks.session_id,
            {
                "tool_name": input_data.get("tool_name", ""),
                "tool_input": input_data.get("tool_input", {}),
                "tool_response": input_data.get("tool_response"),
                "tool_use_id": tool_use_id or input_data.get("tool_use_id", ""),
                "is_error": bool(input_data.get("is_error", False)),
                "native_session_id": input_data.get("session_id", ""),
                "parent_session_id": input_data.get("parent_session_id", ""),
                "parent_tool_use_id": input_data.get("parent_tool_use_id", ""),
                "agent_id": input_data.get("agent_id", ""),
                "agent_type": input_data.get("agent_type", ""),
            },
        )
    return {}


async def _sdk_post_tool_failure_cb(
    input_data: dict[str, Any],
    tool_use_id: str | None,
    context: object,
    *,
    post_cb: Callable[[dict[str, Any], str | None, object], Awaitable[dict[str, Any]]],
) -> dict[str, Any]:
    return await post_cb({**input_data, "is_error": True}, tool_use_id, context)


async def _sdk_stop_cb(
    input_data: dict[str, Any],
    tool_use_id: str | None,
    context: object,
    *,
    hooks: DriverHooks,
    get_last_text: Callable[[], str],
) -> dict[str, Any]:
    if hooks.on_stop is None:
        return {}
    data = {"last_assistant_message": get_last_text()}
    result = await hooks.on_stop(hooks.session_id, data)
    return result or {}


async def _sdk_subagent_start_cb(
    input_data: dict[str, Any],
    tool_use_id: str | None,
    context: object,
    *,
    hooks: DriverHooks,
    lineage: ClaudeSubagentLineage,
) -> dict[str, Any]:
    """把 SDK 子代理 hook 转为带父级的统一启动信息。"""
    if hooks.on_subagent_start is not None:
        await hooks.on_subagent_start(hooks.session_id, lineage.hook_payload(input_data))
    return {}


async def _sdk_subagent_stop_cb(
    input_data: dict[str, Any], tool_use_id: str | None, context: object, *, hooks: DriverHooks
) -> dict[str, Any]:
    if hooks.on_subagent_stop is not None:
        # Boundary normalization: summarize the cc transcript path into
        # the {content, total_tokens} format.
        total_tokens, content = summarize_subagent_transcript(
            input_data.get("agent_transcript_path", ""),
        )
        await hooks.on_subagent_stop(
            hooks.session_id,
            {
                "agent_id": input_data.get("agent_id", ""),
                "agent_type": input_data.get("agent_type", ""),
                "content": content,
                "total_tokens": total_tokens,
            },
        )
    return {}


def _sdk_limit_events(
    session_id: str,
    iteration: int,
    exc: BaseException,
) -> list[Event]:
    """Translate a recognized limit/auth error in an SDK exception string
    into a RATE_LIMIT carrying the normalized limit_signal. SDK exceptions
    surface as free text (no structured rate-limit channel), so the regex
    library below classifies them. Returns [] for non-limit errors (those just
    emit ERROR — observability; the detector does not act on them).
    """
    from workflow_controller.session.events import Event, EventType

    text = str(exc)
    match = _parse_limit_error(text)
    if match is None:
        return []
    reset_at = (
        _resolve_reset_at(match.reset_hhmm, now=datetime.now(tz=UTC).astimezone())
        if match.scope == "5h"
        else None
    )
    limit_signal = _build_signal_from_match(
        match,
        reset_at=reset_at,
        source=f"sdk:regex:{match.source}",
    )
    return [
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.RATE_LIMIT,
            data=limit_signal.to_event_data(raw=text),
        )
    ]


# ── SDK limit-text detection (exception strings have no structured channel) ─

_PATTERNS: list[tuple[re.Pattern[str], Scope, str]] = [
    (
        re.compile(
            r"5[\s-]?hour.{0,20}limit reached.{0,30}"
            r"(?:[Tt]ry again at|[Rr]esets? at)\s+(\d{1,2}:\d{2})\s*(?:(am|pm))?",
            re.IGNORECASE,
        ),
        "5h",
        "with_reset_time",
    ),
    (
        re.compile(
            r"[Ss]ession limit.{0,30}"
            r"(?:[Rr]esets?)\s+\D*(\d{1,2}:\d{2})\s*(?:(am|pm))?",
            re.IGNORECASE,
        ),
        "5h",
        "session_limit",
    ),
    (
        re.compile(r"[Ww]eekly.{0,10}limit reached"),
        "weekly",
        "weekly_no_time",
    ),
    (
        re.compile(r"[Uu]sage limit (?:reached|exceeded)"),
        "5h",
        "generic_5h",
    ),
    (
        re.compile(r"[Rr]ate.?limit.{0,20}(?:exceeded|reached)"),
        "rate_limit",
        "api_rate_limit",
    ),
    (
        re.compile(r"(?:invalid|incorrect|missing)[\s_-]*api[\s_-]*key", re.IGNORECASE),
        "auth_error",
        "invalid_key",
    ),
    (
        re.compile(r"authentication[\s_-]*(?:failed|error)", re.IGNORECASE),
        "auth_error",
        "auth_failed",
    ),
]


def _parse_limit_error(text: str) -> LimitMatch | None:
    """Return a LimitMatch if text contains a recognised limit/auth error."""
    if not text:
        return None
    for regex, scope, source in _PATTERNS:
        m = regex.search(text)
        if not m:
            continue
        if scope == "5h" and m.groups():
            hhmm = m.group(1)
            ampm = m.group(2) if len(m.groups()) >= 2 else None
            return LimitMatch(
                scope=scope,
                source=source,
                reset_hhmm=_merge_hhmm_ampm(hhmm, ampm),
                raw=text,
            )
        return LimitMatch(scope=scope, source=source, reset_hhmm=None, raw=text)
    return None


def _merge_hhmm_ampm(hhmm: str, ampm: str | None) -> str:
    """Convert '10:50' + 'pm' → '22:50'. Returns original if no am/pm."""
    if not ampm:
        return hhmm
    try:
        h, m = map(int, hhmm.split(":"))
    except ValueError:
        return hhmm
    if ampm.lower() == "pm" and h < 12:
        h += 12
    elif ampm.lower() == "am" and h == 12:
        h = 0
    return f"{h:02d}:{m:02d}"


def _resolve_reset_at(reset_hhmm: str | None, *, now: datetime) -> datetime:
    """Convert HH:MM (24h local time, no date) into an absolute timestamp.

    If reset_hhmm <= now's HH:MM, assume it means tomorrow. None falls back to
    a conservative now+5h.
    """
    if reset_hhmm is None:
        return now + timedelta(hours=5)
    hh, mm = reset_hhmm.split(":")
    candidate = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


def _build_signal_from_match(
    match: LimitMatch,
    *,
    reset_at: datetime | None,
    source: str,
) -> LimitSignal:
    """Map a regex LimitMatch onto the signal contract. 5h/weekly/rate_limit
    → rejected (rotate); auth_error → auth_error (mark INVALID + rotate).
    rate_limit is rejected but the rotator logs & stays — a generic API
    rate-limit shouldn't switch keys.
    """
    if match.scope in ("5h", "weekly", "rate_limit"):
        return LimitSignal(
            kind="rejected",
            scope=match.scope,
            reset_at=reset_at,
            utilization=None,
            source=source,
        )
    return LimitSignal(
        kind="auth_error",
        scope=match.scope,
        reset_at=None,
        utilization=None,
        source=source,
    )
