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

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple
from uuid import uuid4

from workflow_controller.core.evaluator import (
    AlwaysCompleteEvaluator,
    Evaluator,
    LockedEvaluator,
    PassThroughEvaluator,
)
from workflow_controller.core.feedback_evaluator import FeedbackEvaluator
from workflow_controller.core.file_changes import path_hints
from workflow_controller.core.reviewer_service import ReviewerRuntimeConfig
from workflow_controller.plugin.file_changes import FileChangeContext, FileChangeOutcome
from workflow_controller.plugin.stages import declared_stages
from workflow_controller.session.events import Event, EventType, SessionOutcome

if TYPE_CHECKING:
    from collections.abc import Callable

    from workflow_controller.config.settings import (
        AppConfig,
        ReviewerConfig,
        SessionConfig,
        SessionReviewerConfig,
    )
    from workflow_controller.core.auth_rotator import AuthRotator
    from workflow_controller.core.loop import SingleLoopEngine
    from workflow_controller.session.session import Session


class SessionFacts(NamedTuple):
    """session 运行事实的只读投影（run 平面消费，字段语义归 session 平面）。"""

    state: str
    stop_reason: str | None
    loop_status: str


def _get_session_facts(session: Session | None) -> SessionFacts | None:
    if session is None:
        return None
    return SessionFacts(
        state=session.state.value,
        stop_reason=session.stop_reason,
        loop_status=session.loop_status,
    )


def _is_terminal_session(facts: SessionFacts | None) -> bool:
    if facts is None:
        return True
    if facts.loop_status == "no":
        return not (facts.state == "idle" and facts.stop_reason == "user_pause")
    return False


def _session_reviewer_mode(session: Session | None, default: str) -> str:
    if session is not None:
        if session.waiting_for_auto_reply_fallback is True:
            return "manual"
        snapshot = getattr(session.config, "reviewer", None)
        mode = getattr(snapshot, "mode", None)
        if isinstance(mode, str) and mode in ("auto", "manual"):
            return mode
    return default


def _unattached_run_id(session: Session, affiliation: Any) -> str | None:
    if (
        session.config.run_id is None
        or affiliation is None
        or affiliation.resolve(session.id) is not None
    ):
        return None
    return session.config.run_id


def _get_session_config(
    name: str,
    sessions: list[SessionConfig],
    config_getter: Callable[[str], SessionConfig | None],
) -> SessionConfig:
    for session_config in sessions:
        if session_config.name == name:
            return session_config

    config = config_getter(name)
    if config is not None:
        return config
    raise ValueError(f"No session config found for '{name}'")


def _running_loop_sessions(loops: list[SingleLoopEngine]) -> list[dict[str, str]]:
    briefs = []
    for loop in loops:
        brief = loop.current_session_brief()
        if brief is not None:
            briefs.append(brief)
    return briefs


def _session_has_stages(session_obj: Any) -> bool:
    """session 是否声明了至少一个有效阶段（name + artifacts 必填）。"""
    try:
        raw = list(getattr(session_obj.config, "stages", None) or [])
    except AttributeError:
        return False
    return bool(declared_stages(raw))


def _list_project_agent_types(project_dir: str) -> list[str]:
    """Subagent types with an on-disk def in <project_dir>/.opencode/agents."""
    agents_dir = Path(project_dir) / ".opencode" / "agents"
    if not agents_dir.is_dir():
        return []
    return sorted(p.stem for p in agents_dir.glob("*.md"))


def _limit_rotation_args(event: Event) -> tuple[str, datetime | None, str]:
    """limit_signal 事件的轮换参数：(scope, reset_at, error 溯源串)。"""
    signal = event.data.get("limit_signal") or {}
    kind = str(signal.get("kind") or "")
    scope = "auth_error" if kind == "auth_error" else str(signal.get("scope") or "5h")
    reset_raw = signal.get("reset_at")
    try:
        reset_at = datetime.fromisoformat(reset_raw) if reset_raw else None
    except (TypeError, ValueError):
        reset_at = None
    if reset_at is not None and reset_at.tzinfo is None:
        # 历史事件携带 naive 本地时间串，按本地时区补全；全量 aware 写入一个版本后可删除
        reset_at = reset_at.astimezone()
    return scope, reset_at, str(signal.get("source") or "")


def _preset_resume_session(
    session: Session,
    resume_sdk_session_id: str | None,
    initial_current_iteration: int | None,
) -> None:
    """激活前预设 resume 位与迭代计数（首轮 query 恢复种子会话）。"""
    if resume_sdk_session_id:
        session.sdk_session_id = resume_sdk_session_id
        session.mark_resume_next()
    if initial_current_iteration is not None:
        # Transparent replay: align iteration counter with origin's checkpoint
        # so Jinja {{ iteration }} matches the LLM's self-perceived progress.
        session.current_iteration = int(initial_current_iteration)


def _resolve_evaluator(
    session_config: SessionConfig,
    evaluators: dict[str, Evaluator],
    evaluator_locks: dict[str, asyncio.Lock],
    auth_rotator: AuthRotator | None,
) -> Evaluator:
    if session_config.loop is None:
        return PassThroughEvaluator()

    evaluator_name = session_config.loop.evaluator

    if evaluator_name and evaluator_name in evaluators:
        lock = evaluator_locks.setdefault(evaluator_name, asyncio.Lock())
        return LockedEvaluator(evaluators[evaluator_name], lock)

    if evaluator_name == "always_complete":
        return AlwaysCompleteEvaluator()

    if evaluator_name == "feedback":
        reviewer = session_config.reviewer
        fb_eval = FeedbackEvaluator(
            model=reviewer.model if reviewer else None,
            provider_index=reviewer.provider_index if reviewer else None,
        )
        if auth_rotator is not None:
            fb_eval.set_auth_rotator(auth_rotator)
        return fb_eval

    if session_config.loop.max_iterations > 0 and evaluator_name is None:
        reviewer = session_config.reviewer
        fb_eval = FeedbackEvaluator(
            model=reviewer.model if reviewer else None,
            provider_index=reviewer.provider_index if reviewer else None,
        )
        if auth_rotator is not None:
            fb_eval.set_auth_rotator(auth_rotator)
        return fb_eval

    return PassThroughEvaluator()


def _build_reviewer_runtime_config(
    sandbox_root: Path,
    config: ReviewerConfig | SessionReviewerConfig,
) -> ReviewerRuntimeConfig:
    return ReviewerRuntimeConfig(
        sandbox_root=sandbox_root,
        model=config.model,
        timeout_seconds=config.timeout_seconds,
        thinking_mode=config.thinking_mode,
        thinking_budget_tokens=config.thinking_budget_tokens,
        task_goal=config.task_goal,
    )


def _build_stop_requested_event(
    mig_session_id: str,
    final_text: str,
    block_key: str,
    role: str,
) -> Event:
    """stop_requested 控制事件（角色随池内 session，缺席按 worker）。"""
    return Event.create(
        session_id=mig_session_id,
        iteration=0,
        event_type="stop_requested",
        data={
            "final_text": final_text,
            "block_key": block_key,
            "role": role,
        },
    )


def _build_session_started_event(session_id: str, config: SessionConfig) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type=EventType.LOG_AUTO,
        data={"action": "session_started", "name": config.name, "role": config.role},
    )


def _build_manual_prompt_event(session_id: str, prompt: str) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type=EventType.LOG_MANUAL,
        data={"prompt": prompt, "session_id": session_id},
    )


def _build_session_state_changed_event(session_id: str, old: str, new: str) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type=EventType.SESSION_STATE_CHANGED,
        data={"from": old, "to": new},
    )


def _build_session_outcome_event(
    session_id: str,
    kind: str,
    stop_reason: str | None,
    last_error: str | None,
    result_digest: str | None,
    iteration: int,
    context: str | None,
) -> Event:
    outcome = SessionOutcome(
        session_id=session_id,
        kind=kind,
        stop_reason=stop_reason,
        last_error=last_error,
        result_digest=result_digest,
        iteration=iteration,
        context=context,
    )
    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.SESSION_OUTCOME,
        data=outcome.to_payload(),
    )


def _build_activation_receipt_event(session_id: str, error: str | None) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type=EventType.SESSION_ACTIVATED,
        data={"ok": error is None, "error": error},
    )


def _build_run_attached_event(session_id: str, run_id: str) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type=EventType.LOG_AUTO,
        data={"action": "run_attached", "run_id": run_id},
    )


def _build_session_complete_event(session_id: str) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type="session_complete",
        data={"session_id": session_id},
    )


def _build_iteration_boundary_event(session_id: str, iteration: int) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type="iteration_boundary",
        data={"iteration": iteration},
    )


def _build_pre_tool_use_event(session_id: str, data: dict[str, Any]) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type="pre_tool_use",
        data=data,
    )


def _build_pre_tool_use_denial(feedback: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": feedback,
        },
    }


def _build_gate_question_event(session_id: str, tool_input: dict[str, Any]) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type="gate_question",
        data={"tool_input": tool_input},
    )


def _build_session_auto_event(
    session_id: str,
    action: str,
    config: SessionConfig,
    resume_sdk_session_id: str,
) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type=EventType.LOG_AUTO,
        data={
            "action": action,
            "name": config.name,
            "role": config.role,
            "resume_sdk_session_id": resume_sdk_session_id,
        },
    )


def _build_file_change_context(
    config: AppConfig,
    session: Session,
    session_id: str,
    data: dict[str, Any],
    tool_use_id: str,
    tool_input: dict[str, Any],
    affiliation: Any,
) -> FileChangeContext:
    """PreToolUse 捕获上下文（run 维度经归属缓存打戳，随执行面并发判定）。"""
    return FileChangeContext(
        capture_id=uuid4().hex,
        actor_kind="agent",
        workflow_controller_session_id=session_id,
        native_session_id=str(data.get("native_session_id") or "") or session.sdk_session_id,
        parent_native_session_id=str(data.get("parent_session_id") or "") or None,
        runtime=session.config.runtime or config.runtime,
        model=session.config.model,
        agent_name=str(data.get("agent_type") or "") or session.config.agent_name or None,
        tool_name=str(data.get("tool_name", "")),
        tool_use_id=tool_use_id,
        cwd=Path(session.config.cwd or config.project_dir).expanduser().resolve(),
        tool_input=tool_input,
        started_at=datetime.now(tz=UTC),
        attributes={
            "initial_prompt": session.config.initial_prompt,
            "prompt": session.current_prompt,
            "system_prompt": session.config.system_prompt,
            "parent_tool_use_id": str(data.get("parent_tool_use_id") or ""),
        },
        native_agent_id=str(data.get("agent_id") or "") or None,
        run_id=affiliation.run_id if affiliation is not None else None,
        run_context=affiliation.context if affiliation is not None else None,
    )


def _build_file_change_outcome(
    data: dict[str, Any],
    context: FileChangeContext,
) -> FileChangeOutcome:
    tool_response = data.get("tool_response")
    is_error = bool(data.get("is_error", False))
    if isinstance(tool_response, dict):
        is_error = is_error or bool(tool_response.get("is_error", False))
    return FileChangeOutcome(
        succeeded=not is_error,
        is_error=is_error,
        tool_output=data.get("tool_response", data.get("tool_output")),
        path_hints=path_hints(context.tool_input, context.cwd),
        finished_at=datetime.now(tz=UTC),
    )


def _build_incomplete_file_change_outcome(context: FileChangeContext) -> FileChangeOutcome:
    return FileChangeOutcome(
        succeeded=False,
        is_error=True,
        tool_output={"reason": "session ended before post-tool-use"},
        path_hints=path_hints(context.tool_input, context.cwd),
        finished_at=datetime.now(tz=UTC),
        incomplete=True,
    )


def _last_assistant_digest(events: list[Event]) -> str | None:
    for evt in reversed(events[-10:]):
        if evt.event_type == EventType.ASSISTANT_MESSAGE:
            text = "".join(
                block.get("text", "")
                for block in evt.data.get("content", [])
                if isinstance(block, dict) and block.get("type") == "text"
            ).strip()
            return text or None
    return None
