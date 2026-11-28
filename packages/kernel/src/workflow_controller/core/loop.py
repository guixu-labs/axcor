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
import contextlib
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any

from jinja2 import BaseLoader, Environment, TemplateError, select_autoescape

from workflow_controller.core.reviewer import NL_QUESTION_CORE_ZH, is_nl_question
from workflow_controller.session.events import (
    COMPLETED_STOP_REASONS,
    Event,
    EventType,
    outcome_kind_for_reason,
)
from workflow_controller.session.session import SessionState
from workflow_controller.support.tasks import spawn

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from workflow_controller.core.evaluator import Evaluator
    from workflow_controller.event.event_bus import EventBus
    from workflow_controller.session.session import Session
    from workflow_controller.store.event_router import EventStoreRouter


# Resume feedback injected after a wait_and_retry sleep window.
_WAIT_RETRY_RESUME_PROMPT = "10 分钟过去了,等待的任务应该已完成,继续吧。"

# loop 等人检测的问询词表：比 reviewer 停机路径的默认词表宽（历史措辞更全），
# 两条路径各自保持既有命中范围，不按共享词表裁剪；中文核心段与 reviewer
# 共用（NL_QUESTION_CORE_ZH），两侧的外围词项各自维护。
_LOOP_QUESTION_MARKERS = [
    *NL_QUESTION_CORE_ZH,
    "你想怎么处理",
    "你想怎么解决",
    "请指示",
    "你希望我",
    "你想让我",
    "你想让我怎么",
    "你希望我怎么",
    "请告诉我你",
    "你想继续吗",
    "是否继续",
    "要继续吗",
    "你想怎么继续",
    "请确认方向",
    "你想用哪个",
    "你选哪个",
    "你想选哪个",
    # English markers
    "please choose",
    "please select",
    "your choice",
    "what would you like",
    "which option",
    "please confirm",
    "please tell me",
    "do you want",
    "would you like",
    "should i",
    "how would you like",
    "what do you prefer",
    "please decide",
    "let me know",
    "which approach",
    "which direction",
    "do you want me to",
    "shall i",
    "would you like me to",
    "what should i do",
    "how should i",
    "do you want to continue",
    "should we continue",
]


class LoopStatus(Enum):
    COMPLETED = "completed"
    MAX_ITERATIONS = "max_iterations"
    BLOCKED = "blocked"
    ERROR = "error"
    MANUAL_STOP = "manual_stop"


def _assistant_text_blocks(content: list[Any]) -> str:
    """content 块序列中 text 块的原文拼接（问句检测取助手消息文本用）。"""
    text = ""
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            text += block.get("text", "")
    return text


# ── Per-session LoopEngine ─────────────────────────────────────────────
# Orchestrator creates one engine per running session.  The historical class
# name is kept for source compatibility with plugins/tests.


class SingleLoopEngine:
    """Execution loop for one active session.

    A fresh instance is created on each start/resume. ``take_over`` remains for
    compatibility, but production code never moves an engine between sessions.
    """

    def __init__(
        self,
        event_router: EventStoreRouter,
        bus: EventBus,
        mode_getter: Callable[[], str] | None = None,
        iteration_gate: Callable[..., Any] | None = None,
        on_session_complete: Callable[[str], Awaitable[None]] | None = None,
        on_query_start: Callable[[str], None] | None = None,
    ) -> None:
        self.event_router = event_router
        self.bus = bus
        self._mode_getter = mode_getter
        self._iteration_gate = iteration_gate
        self._on_session_complete = on_session_complete
        self._on_query_start = on_query_start
        self._current_session: Session | None = None
        self._current_evaluator: Evaluator | None = None
        self._current_task: asyncio.Task[None] | None = None
        self._running = False
        self._stop_event = asyncio.Event()
        self._reviewer_let_exit: bool = False
        self._auth_switch_requested: asyncio.Event = asyncio.Event()
        self._retry_wake_event: asyncio.Event = asyncio.Event()
        self._jinja_env = Environment(loader=BaseLoader(), autoescape=select_autoescape())
        logger.info("SingleLoopEngine initialized (per-session runtime)")

    @staticmethod
    def _last_assistant_text(events: list[Event]) -> str:
        """最后一条助手消息的文本块拼接（问句检测输入）。"""
        for evt in reversed(events):
            if evt.event_type == EventType.ASSISTANT_MESSAGE:
                return _assistant_text_blocks(evt.data.get("content", []))
        return ""

    async def take_over(self, session: Session) -> None:
        """Switch to watching a different session.

        If currently watching a session, stop watching it first (set loop_status=no).
        Then start watching the new session.
        """
        if self._current_session is not None and self._current_session.id != session.id:
            old_session = self._current_session
            await self._set_loop_status(old_session, "no", "switched_to_another")
            self._current_session = None
            task = self._current_task
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                self._current_task = None
            logger.info("SingleLoopEngine: switching from %s to %s", old_session.id, session.id)

        self._current_session = session
        await self._set_loop_status(session, "yes", None)
        logger.info("SingleLoopEngine: now watching %s", session.id)

    def configure_for_session(self, session: Session, evaluator: Evaluator | None) -> None:
        """Configure the evaluator for the current session.

        Called by orchestrator when activating a session to inject the evaluator.
        """
        self._current_evaluator = evaluator
        logger.info(
            "SingleLoopEngine: configured evaluator %s for %s",
            type(evaluator).__name__ if evaluator else "None",
            session.id,
        )

    def mark_reviewer_let_exit(self) -> None:
        """Signal that the reviewer judged the task complete via let_exit.

        The loop will honor this by ending with stop_reason=reviewer_complete,
        bypassing the evaluator.
        """
        self._reviewer_let_exit = True

    def wake_retry(self) -> None:
        """Wake a WAITING_RETRY session early (e.g. sub-agents finished)."""
        self._retry_wake_event.set()

    def is_current(self, session: Session) -> bool:
        """True iff ``session`` is the one this loop is currently watching.

        Wraps the ``_current_session`` read so callers (e.g. SubagentTracker's
        precise-wake) don't reach into the private attribute directly.
        """
        return self._current_session == session

    def is_current_id(self, session_id: str) -> bool:
        """True iff the currently-watched session's id matches ``session_id``.

        Id-based counterpart to :meth:`is_current` for call sites that hold a
        session id rather than the object. ``None``-safe.
        """
        return self._current_session is not None and self._current_session.id == session_id

    def request_auth_switch_now(self) -> None:
        """Manual auth-switch trigger.

        Interrupts only the active driver/query. Session lifecycle state stays
        running and the next iteration reads the updated provider snapshot.
        """
        self._auth_switch_requested.set()
        if self._current_session is not None:
            with contextlib.suppress(RuntimeError):
                spawn(self._current_session.interrupt_for_auth_switch())

    async def pause_current(self) -> None:
        """Pause the currently watched session.

        This is called when the user clicks Pause. It sets loop_status=no
        and stop_reason=user_pause, which will cause the run() loop to stop
        watching this session after the current iteration completes.
        """
        # 先把引用存局部变量：await _set_loop_status 会让出事件循环，期间
        # run() 收尾可能把 _current_session 置 None，之后再取 .id 会炸
        # （检查与使用之间的竞态窗口）。
        session = self._current_session
        if session is not None:
            await self._set_loop_status(session, "no", "user_pause")
            logger.info("SingleLoopEngine: pausing %s", session.id)

    def current_session_brief(self) -> dict[str, str] | None:
        """当前监视中 session 的 id/name 概要；未监视任何 session 时为 None。"""
        session = self._current_session
        if session is None:
            return None
        return {"id": session.id, "name": session.name}

    def request_stop(self) -> None:
        """Request the entire loop to stop.

        This is different from pause_current() - this stops the entire
        SingleLoopEngine, not just the current session.
        """
        self._running = False
        self._stop_event.set()
        task = self._current_task
        if task is not None and not task.done():
            task.cancel()
        logger.info("SingleLoopEngine: stop requested")

    async def run(self) -> None:
        """Main loop that watches sessions and runs their iterations.

        This loop runs indefinitely until request_stop() is called.
        It watches _current_session and runs iterations as long as
        loop_status == "yes". When loop_status becomes "no", it stops
        watching that session and waits for take_over() to be called
        with a new session.
        """
        self._running = True
        logger.info("SingleLoopEngine: starting main loop")

        had_session = self._current_session is not None
        while self._running and not self._stop_event.is_set():
            if self._current_session is None:
                # A per-session runtime exits after its bound session leaves
                # the loop. Before first activation retain the old wait
                # behavior for direct test/legacy callers.
                if had_session:
                    break
                await asyncio.sleep(0.1)
                continue

            had_session = True

            session = self._current_session

            if self._auth_switch_requested.is_set():
                await self._resume_after_auth_switch(session)

            # Check if we should still be watching this session
            if session.loop_status != "yes":
                await self._stop_watching_session(session)
                continue

            # 检查 loop_hold
            if session.loop_hold == "waiting_retry":
                await self._drain_waiting_retry_hold(session)
                continue

            # Run one iteration with error handling
            try:
                logger.info("SingleLoopEngine: starting iteration for %s", session.id)
                self._current_task = asyncio.current_task()
                await self._run_iteration(session)
                logger.info("SingleLoopEngine: iteration completed for %s", session.id)
            except asyncio.CancelledError:
                logger.info("SingleLoopEngine: iteration cancelled for %s", session.id)
                if self._auth_switch_requested.is_set():
                    await self._resume_after_auth_switch(session)
                if self._stop_event.is_set():
                    break
                continue
            except Exception as e:
                await self._handle_iteration_error(session, e)

        self._running = False
        logger.info("SingleLoopEngine: main loop stopped")

    async def _stop_watching_session(self, session: Session) -> None:
        """run() 主循环停止监视当前 session 时的清理（状态置 IDLE 后解除引用）。"""
        logger.info(
            "SingleLoopEngine: %s loop_status=%s (stop_reason=%s), stopping watch",
            session.id,
            session.loop_status,
            session.stop_reason,
        )
        session.state = SessionState.IDLE
        self._current_session = None

    async def _drain_waiting_retry_hold(self, session: Session) -> None:
        """run() 主循环内 waiting_retry 的轮询等待版（0.1s 粒度检查退出条件）。"""
        logger.info("SingleLoopEngine: %s is in waiting_retry, sleeping", session.id)
        # Sleep for the retry duration (or until loop_status changes)
        retry_duration = 600  # 10 minutes default
        for _ in range(int(retry_duration * 10)):  # Check every 0.1s
            if self._auth_switch_requested.is_set():
                await self._resume_after_auth_switch(session)
                break
            if session.loop_status != "yes" or session.loop_hold != "waiting_retry":
                break
            await asyncio.sleep(0.1)
        session.loop_hold = None

    async def _handle_iteration_error(self, session: Session, error: Exception) -> None:
        """run() 主循环的迭代异常收尾：记录并按 stop_reason 决定是否覆盖。"""
        logger.exception("SingleLoopEngine: iteration failed for %s: %s", session.id, error)
        error_msg = f"{type(error).__name__}: {error!s}"
        session.state = SessionState.IDLE
        # Only overwrite stop_reason if not already set (e.g. user_cancel)
        if session.stop_reason not in ("user_cancel", "user_pause"):
            session.last_error = error_msg
            await self._set_loop_status(session, "no", "error")
        self._current_session = None

    async def _set_loop_status(
        self, session: Session, loop_status: str, stop_reason: str | None = None
    ) -> None:
        """Set loop_status and stop_reason, persisting to database.

        ``loop_status`` 首次翻到 ``no`` 时发射 session_outcome 终态事件
        （五类终态路径的统一发射点；重复置 no 不重发）。完成类 stop_reason
        另触发 ``on_session_complete`` 回调，让插件做终态检查点快照
        （best-effort，never blocks the loop）。
        """
        was_terminal = session.loop_status == "no"
        session.loop_status = loop_status
        session.stop_reason = stop_reason
        await self.event_router.update_loop_status(
            session.id,
            loop_status=loop_status,
            stop_reason=stop_reason,
        )
        if loop_status == "no" and stop_reason is not None and not was_terminal:
            await session.emit_outcome(outcome_kind_for_reason(stop_reason))
        if (
            loop_status == "no"
            and stop_reason in COMPLETED_STOP_REASONS
            and self._on_session_complete is not None
        ):
            try:
                await self._on_session_complete(session.id)
            except Exception:
                logger.exception("on_session_complete callback failed for %s", session.id[:12])

    async def _resume_after_auth_switch(self, session: Session) -> None:
        self._auth_switch_requested.clear()
        if self._stop_event.is_set() or session.stop_requested:
            return
        session.last_stop_reason = "user_stop"
        session.state = SessionState.RUNNING
        session.clear_stop_request()
        await self._set_loop_status(session, "yes", None)

    async def _await_first(
        self, session: Session, *tasks: asyncio.Task[Any]
    ) -> list[asyncio.Task[Any]] | None:
        """等待首个完成并取消其余；命中 stop/auth 控制位时按各自路径收尾。

        返回完成集合；返回 None 表示控制位已收尾（调用方直接 return）。
        """
        stop_task = asyncio.create_task(self._stop_event.wait())
        auth_task = asyncio.create_task(self._auth_switch_requested.wait())
        pending = set((*tasks, stop_task, auth_task))
        try:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        if stop_task in done:
            await self._set_loop_status(session, "no", "user_pause")
            return None
        if auth_task in done:
            await self._resume_after_auth_switch(session)
            return None
        return list(done)

    async def _run_iteration(self, session: Session) -> None:
        """Run one iteration of the session.

        端到端流程：前置检查 → 组装 prompt → 执行 query 并转发事件 →
        边界门禁 → 问句检测 / waiting_retry / 手动模式 / reviewer let_exit
        分支 → 评估器 → max_iterations → 继续或收尾。各分支收尾时直接
        return（下一轮由 run() 主循环再进入）。
        """
        iteration = await self._begin_iteration(session)
        if iteration is None:
            return

        prompt, feedback = await self._compose_iteration_prompt(session, iteration)

        event_count = await self._run_session_query(session, iteration, prompt)
        if event_count is None:
            return

        # Driver interruption can end the stream without yielding another event.
        if self._auth_switch_requested.is_set() or session.last_stop_reason == "auth_switch":
            await self._resume_after_auth_switch(session)
            return  # Continue to next iteration

        # Run iteration gate FIRST — Force handoff 需要在 paused 状态下也能触发
        if await self._run_iteration_gate(session, iteration):
            return

        # Check if we should stop after iteration
        if session.loop_status != "yes":
            return

        # Check if the last assistant message contains an unanswered question
        # If yes, wait for user input before continuing to the next iteration
        if await self._wait_if_unanswered_question(session, iteration, event_count):
            return

        # _stop_event race fix: re-check after query
        if self._stop_event.is_set():
            await self._set_loop_status(session, "no", "user_pause")
            return

        # Handle WAITING_RETRY (loop_hold)
        if await self._run_waiting_retry(session, iteration):
            return

        # Manual mode: block on await_manual_prompt
        # Skip handoff_agent sessions — they auto-complete via evaluator.
        if await self._run_manual_mode(session, iteration):
            return

        # Check reviewer_let_exit
        if await self._finish_on_reviewer_let_exit(session, iteration):
            return

        # Run evaluator (if configured)
        feedback, eval_completed = await self._run_evaluator(session, iteration, feedback)
        if eval_completed:
            return

        # Check max_iterations
        if await self._finish_on_max_iterations(session, iteration):
            return

        # Continue to next iteration
        await self._emit_iteration_continue(session, iteration, feedback)

    async def _begin_iteration(self, session: Session) -> int | None:
        """迭代前置检查（pause/stop_event/loop_status/等待恢复）；跳过时返回 None。"""
        if session.pause_requested:
            logger.info("SingleLoopEngine: skip iteration for %s — pause_requested", session.id)
            return None
        logger.info(
            "SingleLoopEngine._run_iteration: starting for session %s (role=%s, sdk_id=%s)",
            session.id,
            session.role,
            session.native_session_id,
        )

        iteration = session.current_iteration + 1
        session.current_iteration = iteration

        # Check _stop_event before starting
        if self._stop_event.is_set():
            await self._set_loop_status(session, "no", "user_pause")
            return None

        # Check if we should stop before starting
        if session.loop_status != "yes":
            return None

        # Wait if paused
        await session.wait_if_paused()
        return iteration

    async def _compose_iteration_prompt(
        self, session: Session, iteration: int
    ) -> tuple[str, str | None]:
        """组装本轮 prompt 与 feedback（含 _resume_next 恢复分支）。"""
        # Check for manual prompt
        manual = await session.poll_manual_prompt()
        # Gate feedback from iteration boundary (review_stage approve/reject) —
        # programmatic, not human; goes straight to prompt without
        # "User intervention:" prefix. Takes priority over manual input.
        gate_fb = session.take_gate_feedback()
        if gate_fb:
            feedback = gate_fb
        elif manual:
            feedback = f"User intervention: {manual}"
        else:
            feedback = None

        # Handle _resume_next (resume from pause)
        if session.resume_next and iteration > 1:
            session.clear_resume_next()
            reprompt = session.consume_resume_reprompt()
            if not session.native_session_id:
                # 首轮渲染可能让 feedback 替代初始任务；先渲染任务，再附上待发输入。
                initial = self._render_session_prompt(session, 1, None)
                pending = [gate_fb, f"User intervention: {manual}" if manual else None]
                feedback = "\n\n".join(item for item in pending if item) or None
                prompt = f"{initial}\n\n{feedback}" if feedback else initial
            elif gate_fb:
                prompt = self._render_session_prompt(session, iteration, feedback)
            elif manual:
                feedback = f"User intervention: {manual}"
                prompt = self._render_session_prompt(session, iteration, feedback)
            elif reprompt:
                # 首轮取消且零事件：sdk 会话无任务上下文，重发初始 prompt 锚定任务
                # （渲染口径与首轮一致，恢复进程拿到与冷启动等价的指令）
                feedback = None
                prompt = self._render_session_prompt(session, 1, None)
            else:
                prompt = "请继续你之前的工作。"
                feedback = None
        else:
            # Render prompt using Jinja template
            prompt = self._render_session_prompt(session, iteration, feedback)

        logger.info(
            "SingleLoopEngine._run_iteration: prompt rendered for %s (len=%d, has_feedback=%s)",
            session.id,
            len(prompt),
            feedback is not None,
        )
        return prompt, feedback

    def _render_session_prompt(self, session: Session, iteration: int, feedback: str | None) -> str:
        """session 维度的 prompt 渲染（模板与初始 prompt 均取自 session 配置）。"""
        return self._render_prompt(
            initial_prompt=session.config.initial_prompt or "",
            iteration=iteration,
            feedback=feedback,
            prompt_template=session.config.loop.prompt_template if session.config.loop else None,
        )

    async def _run_session_query(self, session: Session, iteration: int, prompt: str) -> int | None:
        """执行本轮 query 并转发事件到总线；返回事件数，auth-switch 路径返回 None。"""
        # Store LOG_QUERY event
        await self._emit_bus_event(
            session.id,
            iteration,
            EventType.LOG_QUERY,
            {"prompt": prompt[:200], "iteration": iteration},
        )

        # Run the session query with auth_switch check
        logger.info(
            "SingleLoopEngine._run_iteration: calling session.query() for %s (iter=%d, prompt_len=%d)",
            session.id,
            iteration,
            len(prompt),
        )
        # 上一轮 query 遗留在途子代理的清理点：必须放在本轮 query 开始而非上轮
        # query 结束。query 结束后到 iteration 边界门禁（_iteration_gate）之间，
        # 仍有子代理在后台运行时（如 reviewer 的 wait_and_retry 在子代理在途时
        # 结束 query），breather 的 on_iteration_boundary / _on_context_event
        # 要靠 in_flight 判断「子代理仍在跑」来推迟 handoff——若在 query 结束就
        # 清空，这些延迟分支会永远读不到在途记录，变成静默死代码并提前拉起
        # handoff。改到下一轮 query 开始清空，既保证二轮 reviewer 读不到上轮残留，
        # 又保住边界门禁的延迟语义。代价是真正仍在运行的子代理（wait 窗口超时唤醒时
        # 尚未 stop）的记录会在下一轮 query 开始时被丢弃，导致下一轮 stop-hook 的
        # 「仍有 N 个 sub-agent 在运行」注入少报、breather 边界门禁不再计入它——这
        # 是「在途记录不跨 query 存活」语义的有意取舍，不破坏正确性（最终 SUBAGENT_STOP
        # 仍会发出，on_stop 的 pop 幂等）。
        if self._on_query_start is not None:
            self._on_query_start(session.id)
        session.start_active()  # 运行段起算（超时预算按实际运行计）
        event_count = 0
        cancelled = False
        try:
            async for event in session.query(prompt, iteration=iteration):
                event_count += 1
                # Check auth_switch mid-iteration
                if self._auth_switch_requested.is_set():
                    await self._resume_after_auth_switch(session)
                    return None
                # Check if we should stop mid-iteration
                # 失败终态仍需转发 ERROR/ITERATION_END 到事件总线
                if session.loop_status != "yes" and session.stop_reason not in {
                    "error",
                    "gate_failed",
                }:
                    break
                await self.bus.emit(event)
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            session.settle_active()  # 结算（正常/异常/中断均计费到此刻）
            # 仅取消路径置位：首轮被中途取消且零事件，prompt 大概率未持久化
            # 进 sdk 会话（进程组在 spawn→落库窗口被杀），恢复轮若只发
            # 「请继续你之前的工作。」会得到一个无任务上下文的空会话——标记
            # 后由恢复分支重发初始 prompt。正常完成的零事件轮（驱动空产出）
            # prompt 已进 sdk 会话，置位会让恢复轮重发造成任务指令重复。
            session.set_resume_reprompt(cancelled and iteration == 1 and event_count == 0)

        logger.info(
            "SingleLoopEngine._run_iteration: session.query() returned for %s (events=%d)",
            session.id,
            event_count,
        )
        return event_count

    async def _run_iteration_gate(self, session: Session, iteration: int) -> bool:
        """iteration 边界门禁；返回 True 表示本轮已收尾（调用方 return）。"""
        if self._iteration_gate is None:
            return False
        try:
            gate = await self._iteration_gate(session.id, session, iteration)
        except Exception:
            logger.warning("iteration_gate raised — ignoring", exc_info=True)
            gate = None

        if gate is None:
            return False

        if gate.action == "stop":
            reason = gate.reason or "iteration_gate_stop"
            await self._set_loop_status(session, "no", reason)
            await self._emit_bus_event(
                session.id,
                iteration,
                EventType.LOG_LOOP,
                {
                    "decision": "stop",
                    "reason": gate.reason or "iteration_gate_stop",
                    "iteration": iteration,
                },
            )
            return True
        if gate.action == "block":
            return await self._apply_gate_block(session, iteration, gate)
        return False

    async def _apply_gate_block(self, session: Session, iteration: int, gate: Any) -> bool:
        """gate.action == "block" 的继续分支；返回 True 表示本轮已收尾。"""
        max_iter = session.config.loop.max_iterations if session.config.loop else 10
        if iteration >= max_iter:
            await self._set_loop_status(session, "no", "max_iterations")
            await self.event_router.store_loop_result(
                session_id=session.id, iterations=iteration, status="max_iterations"
            )
            logger.info(
                "SingleLoopEngine: max_iterations reached for %s (iter=%d/%d)",
                session.id,
                iteration,
                max_iter,
            )
            return True
        # 把 gate feedback（如 review_stage 的 approve/reject）存到 session，
        # 下一轮 prompt 渲染时直接用它当 feedback——不给 worker 扔通用 "继续"。
        if gate.feedback:
            session.set_gate_feedback(gate.feedback)
        await self._emit_bus_event(
            session.id,
            iteration,
            EventType.LOG_LOOP,
            {
                "decision": "continue",
                "reason": gate.reason or "iteration_gate_block",
                "iteration": iteration,
                "next_iteration": iteration + 1,
            },
        )
        return True

    async def _wait_if_unanswered_question(
        self, session: Session, iteration: int, event_count: int
    ) -> bool:
        """末条助手消息含未答问句时等用户输入；返回 True 表示本轮已收尾。"""
        # 问句检测开关：False 跳过整段检测；None（含未配 loop）沿现状
        loop_cfg = session.config.loop
        skip_question_detect = loop_cfg is not None and loop_cfg.question_detect is False
        if event_count <= 0 or skip_question_detect:
            return False
        all_events = await self.event_router.get_events(session.id)
        last_events = all_events[-10:] if len(all_events) > 10 else all_events
        last_assistant_text = self._last_assistant_text(last_events)

        # 判定逻辑（词表命中 + 问号结尾）与 reviewer 共用，词表用 loop 完整集
        has_question = is_nl_question(last_assistant_text, markers=_LOOP_QUESTION_MARKERS)

        if not has_question:
            return False
        logger.info(
            "SingleLoopEngine._run_iteration: detected unanswered question in %s, waiting for user input",
            session.id,
        )
        reviewer = session.config.reviewer
        session.waiting_for_auto_reply_fallback = (
            reviewer is not None
            and reviewer.enabled
            and self._mode_getter is not None
            and self._mode_getter() == "auto"
        )
        try:
            await self._emit_bus_event(
                session.id,
                iteration,
                EventType.LOG_LOOP,
                {
                    "decision": "waiting_for_input",
                    "reason": "unanswered_question",
                    "iteration": iteration,
                },
            )
            prompt_task = asyncio.create_task(session.await_manual_prompt())
            done = await self._await_first(session, prompt_task)
        finally:
            session.waiting_for_auto_reply_fallback = False
        if done is None:
            return True
        user_input = prompt_task.result()
        feedback = f"User answer: {user_input}"
        await self._emit_bus_event(
            session.id,
            iteration,
            EventType.LOG_LOOP,
            {
                "decision": "continue",
                "reason": "user_answered_question",
                "iteration": iteration,
                "next_iteration": iteration + 1,
            },
        )
        # Continue to next iteration with user's answer as feedback
        # (feedback will be used in the next iteration's prompt rendering)
        await session.inject_prompt(feedback)
        return True

    async def _run_waiting_retry(self, session: Session, iteration: int) -> bool:
        """WAITING_RETRY 窗口（loop_hold）：可中断睡眠后注入恢复提示的续跑事件。"""
        if session.loop_hold != "waiting_retry":
            return False
        logger.info("SingleLoopEngine: %s is in waiting_retry, sleeping", session.id)
        retry_duration = 600  # 10 minutes default
        # Sleep with interruptible wake (stop or sub-agent completion)
        sleep_task = asyncio.create_task(asyncio.sleep(retry_duration))
        wake_task = asyncio.create_task(self._retry_wake_event.wait())
        done = await self._await_first(session, sleep_task, wake_task)
        if done is None:
            return True
        # Sleep finished or woken early
        session.loop_hold = None
        self._retry_wake_event.clear()
        await self._emit_bus_event(
            session.id,
            iteration,
            EventType.LOG_LOOP,
            {
                "decision": "continue",
                "reason": "wait_and_retry_resumed",
                "iteration": iteration,
                "next_iteration": iteration + 1,
            },
        )
        return True  # Continue to next iteration

    async def _run_manual_mode(self, session: Session, iteration: int) -> bool:
        """Manual 模式：阻塞等手动输入；返回 True 表示本轮已收尾。"""
        if (
            self._mode_getter is not None
            and self._mode_getter() == "manual"
            and session.role != "handoff_agent"
        ):
            await self._emit_bus_event(
                session.id,
                iteration,
                EventType.LOG_LOOP,
                {
                    "decision": "waiting_for_input",
                    "reason": "manual_mode",
                    "iteration": iteration,
                },
            )
            prompt_task = asyncio.create_task(session.await_manual_prompt())
            done = await self._await_first(session, prompt_task)
            if done is None:
                return True
            manual_text = prompt_task.result()
            await self._emit_bus_event(
                session.id,
                iteration,
                EventType.LOG_LOOP,
                {
                    "decision": "continue",
                    "reason": "manual_intervention",
                    "iteration": iteration,
                    "next_iteration": iteration + 1,
                },
            )
            await session.inject_prompt(manual_text)
            return True  # Continue to next iteration with user input
        return False

    async def _finish_on_reviewer_let_exit(self, session: Session, iteration: int) -> bool:
        """reviewer 判定完成（let_exit）时的收尾；返回 True 表示已收尾。"""
        if not self._reviewer_let_exit:
            return False
        self._reviewer_let_exit = False
        await self._emit_bus_event(
            session.id,
            iteration,
            EventType.LOG_LOOP,
            {
                "decision": "stop",
                "reason": "reviewer_let_exit",
                "iteration": iteration,
            },
        )
        # Route through _set_loop_status so the on_session_complete callback
        # fires (final-state checkpoint snapshot). update_loop_status is
        # called inside _set_loop_status, so this replaces the direct call.
        await self._set_loop_status(session, "no", "reviewer_complete")
        await self.event_router.store_loop_result(
            session_id=session.id,
            iterations=iteration,
            status="completed",
        )
        return True

    async def _run_evaluator(
        self, session: Session, iteration: int, feedback: str | None
    ) -> tuple[str | None, bool]:
        """评估器执行与完成收尾；返回 (feedback, 是否已完成收尾)。"""
        if self._current_evaluator is None:
            return feedback, False
        all_events = await self.event_router.get_events(session.id)
        criteria = session.config.criteria if session.config else {}

        result = await self._current_evaluator.evaluate(
            events=all_events,
            criteria=criteria,
            iteration=iteration,
        )
        await self.event_router.store_evaluation(session.id, iteration, result)

        await self._emit_bus_event(
            session.id,
            iteration,
            EventType.LOG_EVAL,
            {
                "is_complete": result.is_complete,
                "progress": result.progress,
                "summary": result.summary,
                "iteration": iteration,
            },
        )
        logger.info(
            "evaluator: iter %d is_complete=%s progress=%s — %s",
            iteration,
            result.is_complete,
            result.progress,
            result.summary,
        )

        if result.is_complete:
            await self._finish_on_eval_complete(session, iteration)
            return feedback, True

        return result.feedback, False

    async def _finish_on_eval_complete(self, session: Session, iteration: int) -> None:
        """评估器判定完成的收尾（停止事件 + 终态落账）。"""
        logger.info("SingleLoopEngine: evaluator marked %s as complete", session.id)
        await self._emit_bus_event(
            session.id,
            iteration,
            EventType.LOG_LOOP,
            {
                "decision": "stop",
                "reason": "eval_complete",
                "iteration": iteration,
            },
        )
        await self._set_loop_status(session, "no", "evaluator_complete")
        await self.event_router.store_loop_result(
            session_id=session.id,
            iterations=iteration,
            status="completed",
        )

    async def _finish_on_max_iterations(self, session: Session, iteration: int) -> bool:
        """max_iterations 上限收尾；返回 True 表示已收尾。"""
        max_iter = session.config.loop.max_iterations if session.config.loop else 10
        if iteration < max_iter:
            return False
        stop_reason = "evaluator_complete" if session.role == "handoff_agent" else "max_iterations"
        await self._set_loop_status(session, "no", stop_reason)
        await self.event_router.store_loop_result(
            session_id=session.id,
            iterations=iteration,
            status="completed" if session.role == "handoff_agent" else "max_iterations",
        )
        return True

    async def _emit_iteration_continue(
        self, session: Session, iteration: int, feedback: str | None
    ) -> None:
        """本轮继续下一轮的 LOG_LOOP 事件（reason 携带截断 feedback）。"""
        await self._emit_bus_event(
            session.id,
            iteration,
            EventType.LOG_LOOP,
            {
                "decision": "continue",
                "reason": feedback[:100] if feedback else "iteration_complete",
                "iteration": iteration,
                "next_iteration": iteration + 1,
            },
        )

    async def _emit_bus_event(
        self, session_id: str, iteration: int, event_type: EventType, data: dict[str, Any]
    ) -> None:
        """统一的事件总线发射点（LOG_QUERY/LOG_LOOP/LOG_EVAL 等）。"""
        await self.bus.emit(
            Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=event_type,
                data=data,
            )
        )

    def _render_prompt(
        self,
        initial_prompt: str,
        iteration: int,
        feedback: str | None,
        prompt_template: str | None = None,
    ) -> str:
        """Render the prompt for an iteration using Jinja template.

        If prompt_template is provided (from session.config.loop.prompt_template),
        use it. Otherwise fall back to simple logic.
        """
        if prompt_template:
            try:
                template = self._jinja_env.from_string(prompt_template)
                return template.render(
                    initial_prompt=initial_prompt,
                    iteration=iteration,
                    feedback=feedback,
                )
            except TemplateError:
                logger.warning("Jinja template render failed, using fallback", exc_info=True)

        # Fallback logic
        if iteration == 1:
            return feedback if feedback else initial_prompt
        elif feedback:
            return feedback
        else:
            return "继续。"
