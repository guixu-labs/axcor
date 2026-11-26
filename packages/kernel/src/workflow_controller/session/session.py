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
import logging
import os
import time
import traceback
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from workflow_controller.config.settings import SessionConfig
from workflow_controller.session.events import Event, EventType, outcome_kind_for_reason

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine

    from workflow_controller.core.auth_rotator import AuthRotator
    from workflow_controller.core.loop import LoopStatus
    from workflow_controller.driver.hooks import DriverHooks
    from workflow_controller.driver.protocol import Driver

logger = logging.getLogger(__name__)


def resolve_query_model(config: SessionConfig, sdk_env: dict[str, str]) -> str:
    """query 实际使用的 model：opencode 裸名改写为 env 已解析路由，非 codex 再兜底
    全局默认。探测 fork 等旁路路径必须经同一解析，否则 opencode 会话会以默认
    provider 而非原会话注入的 provider 起查询。
    """
    from workflow_controller.driver.factory import resolve_runtime

    model = config.model or ""
    # 按 effective runtime 判定（与 create_driver 同规则）：runtime=None 约定
    # 用全局 runtime，判原始 None 会让全局 opencode 下的裸 model 漏掉改写。
    if resolve_runtime(getattr(config, "runtime", None)) == "opencode" and "/" not in model:
        # opencode 只认 provider/model 路由：裸/缺省 model 不会进 -m，会
        # 回落到 opencode 默认 provider。改用 env 中已解析的路由（provider.model
        # 或 default_model，池条目注入合成为 workflow-controller-pool/<model>），实际 query
        # 才会命中注入的 provider。
        model = sdk_env.get("ANTHROPIC_MODEL") or model
    runtime = resolve_runtime(getattr(config, "runtime", None))
    if runtime == "codex_cli":
        model = model or sdk_env.get("ANTHROPIC_MODEL", "")
    else:
        model = model or os.environ.get("ANTHROPIC_MODEL", "")
    return model


class SessionState(Enum):
    CREATING = "creating"
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    STOPPED = "stopped"
    FAILED = "failed"
    WAITING_RETRY = "waiting_retry"  # set during the 10-minute wait window
    # between reviewer dead-loop detection
    # and worker --resume. See design.md §11.


# New design: simplified states (will replace SessionState in future)
# For now, keep both for backward compatibility
class SessionStateNew(Enum):
    IDLE = "idle"
    RUNNING = "running"


class Session:
    def __init__(
        self,
        session_id: str | None = None,
        name: str = "unnamed",
        role: str = "worker",
        config: SessionConfig | None = None,
    ) -> None:
        self.id: str = session_id or uuid4().hex[:12]
        self.name: str = name
        self.role: str = role
        self._state: SessionState = SessionState.IDLE
        # 状态迁移钩子：任意 SessionState 变化时同步回调（发射方负责进总线）
        self._state_change_hook: Callable[[str, SessionState, SessionState], None] | None = None
        # 终态结局发射器：SO 在激活时安装；未安装（未激活/测试替身）时 no-op
        self._outcome_emitter: Callable[[Session, str], Awaitable[None]] | None = None
        self.config: SessionConfig = config or SessionConfig(name=name, role=role)
        self.native_session_id: str | None = None
        self.created_at: datetime = datetime.now(tz=UTC)
        self.last_activity: datetime = datetime.now(tz=UTC)

        self.pause_requested: bool = False
        self.pause_event: asyncio.Event = asyncio.Event()
        self.pause_event.set()

        # 实际运行时长记账（超时预算按此计，暂停/等待不消耗）：
        # active_seconds = 历史累计；_active_since = 当前运行段起点（None=未在跑）。
        # 由 loop 在 query 前后与 pause/stop 时结算；持久化进 sessions 表，
        # 后端重启 resume 后余额不丢。
        self.active_seconds: float = 0.0
        self._active_since: float | None = None

        self._manual_prompt_queue: asyncio.Queue[str] = asyncio.Queue()
        self.waiting_for_auto_reply_fallback = False
        self._stop_requested: bool = False
        # resume() 后下次 query 用 native_session_id 续同一个 claude session
        # （区别于 play()：play 新起一个 claude session，不续上下文）。
        self._resume_next: bool = False
        # 首轮被中途取消且零事件（prompt 未进 sdk 会话）——恢复轮须重发
        # 初始 prompt 而非无上下文的「请继续你之前的工作。」
        self._resume_reprompt: bool = False
        self._driver: Driver | None = None
        # P2 fix: record real LoopStatus so callers can distinguish
        # COMPLETED vs MANUAL_STOP vs MAX_ITERATIONS (all of which map to
        # SessionState.COMPLETED via Orchestrator._run_session).
        # loop 当前 iteration（每轮由 LoopEngine 更新，供 UI 显示 iter X/Y）
        self.current_iteration: int = 0
        self.final_loop_status: LoopStatus | None = None

        # When reviewer returns action="wait_and_retry" (design.md §11),
        # orchestrator schedules a 600s asyncio task to respawn this session
        # via --resume. These two fields drive the UI countdown display and
        # track how many wait_and_retry cycles this session has gone through.
        # retry_scheduled_at is monotonic() + wait_seconds at scheduling time,
        # cleared back to None when the wait task fires or is cancelled.
        self.retry_scheduled_at: float | None = None
        self.retry_count: int = 0

        # Set by Orchestrator at construction. None → spawn inherits os.environ unchanged
        # (legacy behavior). When set, build_env strips/adds ANTHROPIC_API_KEY by mode.
        self._auth_rotator: AuthRotator | None = None

        # In-process driver hooks (PreToolUse/Stop → reviewer). Used by SDK/
        # opencode drivers; CLI driver ignores it (uses the HTTP hook server).
        self._driver_hooks: DriverHooks | None = None
        self._provider_change_callback: Callable[[int, str], Awaitable[None]] | None = None

        # Set by orchestrator when a wait_and_retry spawn fails. Surfaces
        # the underlying exception text so the UI / log show what went wrong.
        # None on healthy sessions.
        self.last_error: str | None = None

        # Set by stop() to distinguish user-initiated stop from auth-switch
        # restart (LoopEngine treats them differently). Defaults to "user_stop".
        self.last_stop_reason: str = "user_stop"

        # ── New design fields (redesign.md §1.4) ──
        self.loop_status: str = "yes"
        self.stop_reason: str | None = None
        self.loop_hold: str | None = None
        self.birth_handoff_doc: str | None = None
        self.result: str = "fail"

        # Per-plugin metadata (e.g. agent_traceback's birth_traceback: origin
        # session/checkpoint for fork tracking). Generic dict — plugins own
        # their sub-keys; the session model is agnostic.
        self.plugin_metadata: dict[str, dict[str, Any]] = {}

        # staged_pipeline per-stage review channel
        self.pending_stage_review: dict[str, Any] | None = None

        # worker 本轮最后一条 assistant 文本。_decide_stop 在 staged session 上捕获（stop-hook
        # 退化为「让 turn 结束」时顺手存），iteration-boundary 的 per-stage reviewer 读它当 context
        # （判 worker 的汇报与磁盘产物是否一致）。每轮 stop 时覆盖。
        self.last_worker_text: str = ""
        # 当前发给 worker 的完整 iteration prompt；file_provenance 在同步
        # PreToolUse 边界读取，随本地 capture 保存，Session 删除时由插件脱敏。
        self.current_prompt: str = ""

        # staged session 完成信号：on_iteration_boundary 在所有阶段过门禁（k==总）时
        # 置 True。iteration-boundary 闭包读到后，auto 模式 mark_reviewer_let_exit → COMPLETED
        # （staged 不跑整体 reviewer，k==总 + 逐阶段 reviewer 即完成）；manual 模式不 mark，落到
        # manual-prompt 等用户最终拍板。确定性完成，不依赖 evaluator 配置（避免 PassThroughEvaluator
        # 下 k==总 死循环到 max_iterations）。
        self.stages_complete: bool = False

        # 最近一次 per-stage review 审完的阶段索引（done_index）。boundary 用它在下次
        # 门禁触发时跳过已审阶段的重审——worker 被推进后本轮的产物可能还在进行中、
        # disk 上 k 未变，但结构门禁和语义 review 结果仍然有效。 -1 = 未审过。
        self._last_reviewed_stage_index: int = -1

        # iteration gate 反馈（如 per-stage review_stage 的 approve/reject）——gate 返回
        # block 时 loop engine 把 feedback 存到这里，下一轮 prompt 渲染时直接用它当 prompt
        # feedback（不加 "User intervention:" 前缀，它是程序化的，不是人的手工输入）。一轮一清。
        self._pending_gate_feedback: str | None = None

    @property
    def run_id(self) -> str | None:
        """归属的 run（ARCHITECTURE.md「Run 身份」），创建时从 SessionConfig 继承。

        None = 不归属任何 run（普通 session 用法，行为与没有 run 层时完全一致）。
        """
        return self.config.run_id

    @property
    def state(self) -> SessionState:
        return self._state

    @state.setter
    def state(self, new_state: SessionState) -> None:
        old = self._state
        if new_state == old:
            return
        self._state = new_state
        hook = self._state_change_hook
        if hook is not None:
            try:
                hook(self.id, old, new_state)
            except Exception:
                logger.exception("state 迁移钩子失败（session=%s）", self.id)

    @property
    def stop_requested(self) -> bool:
        """用户停止请求是否在途（stop() 置位；play/resume/reset 清除）。"""
        return self._stop_requested

    @property
    def driver(self) -> Driver | None:
        """当前已安装的 driver（未安装为 None）：供插件读取的只读访问面。"""
        return self._driver

    @property
    def resume_next(self) -> bool:
        """下次 query 是否续用既有 sdk 会话（--session 恢复上下文）。"""
        return self._resume_next

    @property
    def last_reviewed_stage_index(self) -> int:
        """最近一次 per-stage review 审完的阶段索引（-1 = 未审过）。"""
        return self._last_reviewed_stage_index

    def set_state_change_hook(
        self,
        hook: Callable[[str, SessionState, SessionState], None] | None,
    ) -> None:
        """状态迁移钩子（同步，每 session 一个，重复安装覆盖）。"""
        self._state_change_hook = hook

    def set_outcome_emitter(
        self,
        emitter: Callable[[Session, str], Awaitable[None]] | None,
    ) -> None:
        """终态结局发射器（async (session, kind) -> None）。"""
        self._outcome_emitter = emitter

    async def emit_outcome(self, kind: str) -> None:
        """终态路径发射结构化结局；未装发射器时 no-op。"""
        emitter = self._outcome_emitter
        if emitter is not None:
            await emitter(self, kind)

    def set_auth_rotator(self, rotator: AuthRotator) -> None:
        self._auth_rotator = rotator

    def set_driver_hooks(self, hooks: DriverHooks) -> None:
        self._driver_hooks = hooks

    @property
    def driver_hooks(self) -> DriverHooks | None:
        """当前会话的 driver hook 组装。"""
        return self._driver_hooks

    def set_provider_change_callback(
        self,
        callback: Callable[[int, str], Awaitable[None]],
    ) -> None:
        self._provider_change_callback = callback

    async def query(self, prompt: str, iteration: int = 1) -> AsyncIterator[Event]:
        if self.pause_requested:
            return
        self.state = SessionState.RUNNING
        self.last_activity = datetime.now(tz=UTC)
        self.current_prompt = prompt

        await self._apply_resolved_provider()
        model, sdk_env = self._resolve_model_and_env()
        driver = self._create_driver_for_query(iteration)

        try:
            driver_error = ""
            async for event in driver.query(
                prompt,
                iteration=iteration,
                session_id=self.id,
                model=model,
                cwd=self.config.cwd if self.config.cwd else None,
                max_turns=self.config.max_turns if self.config.max_turns else None,
                allowed_tools=list(self.config.allowed_tools) if self.config.allowed_tools else [],
                env=sdk_env,
                hooks=self._driver_hooks,
                system_prompt=self.config.system_prompt,
                agent_name=getattr(self.config, "agent_name", "") or "",
            ):
                driver_error = await self._process_query_event(event, driver_error)
                yield event
        except Exception as e:
            async for error_event in self._emit_query_error(e, iteration):
                yield error_event
        finally:
            self._driver = None

    async def stop(self, reason: str = "user_stop") -> None:
        self.last_stop_reason = reason
        self._stop_requested = True
        self.settle_active()  # 停止结算：已运行时长定格（超时续跑判断用）
        self.state = SessionState.STOPPED
        self.loop_status = "no"
        self.stop_reason = "user_pause" if reason == "user_stop" else reason
        self.loop_hold = None
        driver = self._driver
        if driver is not None:
            await driver.stop()
            await driver.kill()
        # 显式停止始终发结局（STOPPED 是独立事实）；kind 按停止意图消歧
        await self.emit_outcome(outcome_kind_for_reason(reason))

    def interrupt_for_auth_switch(self) -> Coroutine[Any, Any, None]:
        """Stop only the active driver so the session runtime can continue.

        Provider rotation is not a Session lifecycle transition: it must not
        set STOPPED, ``loop_status=no`` or the user-stop flag.
        """
        self.last_stop_reason = "auth_switch"
        # Capture now: the coroutine may run after the next iteration has
        # already installed a new driver, which must never be interrupted.
        driver = self._driver

        async def interrupt_driver() -> None:
            if driver is not None:
                try:
                    await driver.stop()
                finally:
                    await driver.kill()

        return interrupt_driver()

    # ── 运行时长记账（超时预算按实际运行计，暂停不消耗）─────────────
    def start_active(self) -> None:
        """进入运行段（loop 每轮 query 前调用；幂等）。"""
        if self._active_since is None:
            self._active_since = time.monotonic()

    def settle_active(self) -> None:
        """结算当前运行段到累计值（query 结束/暂停/停止时调用；幂等）。"""
        if self._active_since is not None:
            self.active_seconds += time.monotonic() - self._active_since
            self._active_since = None

    def active_elapsed(self) -> float:
        """已用运行时长 = 历史累计 + 进行中运行段（只读，不冻结也不结算）。

        挂死的 query 不会有人 settle，对账方必须经此取实时值，否则预算永不耗尽。
        """
        if self._active_since is None:
            return self.active_seconds
        return self.active_seconds + (time.monotonic() - self._active_since)

    async def pause(self) -> None:
        was_terminal = self.loop_status == "no"
        self.pause_requested = True
        self.pause_event.clear()
        self.settle_active()  # 暂停即冻结：等待期不计入超时预算
        self.state = SessionState.IDLE
        self.loop_status = "no"
        self.stop_reason = "user_pause"
        self.loop_hold = None
        driver = self._driver
        if driver is not None:
            await driver.stop()
            await driver.kill()
        # 已终态再暂停不重复发（loop 侧已发过同类结局）
        if not was_terminal:
            await self.emit_outcome("paused")

    async def play(self) -> None:
        self.pause_requested = False
        self.pause_event.set()
        self._stop_requested = False
        self.last_stop_reason = None
        self.state = SessionState.RUNNING
        self.loop_status = "yes"
        self.stop_reason = None
        self.result = None
        # 新执行段的超时预算从零计，避免沿用上一次运行的耗时。
        self.active_seconds = 0.0
        self._active_since = None

    async def resume(self) -> None:
        """Resume after pause, continuing the SAME opencode session (native_session_id)
        on the next query via --session. If native_session_id is not yet captured,
        tries to recover it from opencode.db as a best-effort fallback; if still
        not found, the driver simply omits --session (fresh opencode session).
        """
        if not self.native_session_id:
            # 兜底恢复走线程：内含 subprocess（opencode db path，首调未缓存）
            # + 同步 sqlite 读，在 loop 上跑会停摆事件循环（定时器/信号迟到）
            await asyncio.to_thread(self._recover_native_session_id)
        self._resume_next = True
        self.pause_requested = False
        self.pause_event.set()
        self._stop_requested = False
        self.last_stop_reason = None
        self.state = SessionState.RUNNING
        self.loop_status = "yes"
        self.stop_reason = None
        self.result = None
        # 续跑按新执行段计时，旧执行段耗时不挤占本段预算。
        self.active_seconds = 0.0
        self._active_since = None

    async def inject_prompt(self, prompt: str) -> None:
        await self._manual_prompt_queue.put(prompt)

    async def poll_manual_prompt(self) -> str | None:
        try:
            return self._manual_prompt_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def await_manual_prompt(self) -> str:
        return await self._manual_prompt_queue.get()

    async def wait_if_paused(self) -> None:
        if self.pause_requested:
            self.state = SessionState.PAUSED
            await self.pause_event.wait()
            self.state = SessionState.RUNNING

    def reset(self) -> None:
        self.state = SessionState.IDLE
        self.pause_requested = False
        self.pause_event.set()
        self._stop_requested = False
        self.native_session_id = None
        self.retry_scheduled_at = None
        self.retry_count = 0
        self.loop_status = "yes"
        self.stop_reason = None
        self.loop_hold = None
        self.pending_stage_review = None
        self.last_worker_text = ""
        self.stages_complete = False

    # ── loop / orchestrator 的公有访问口（受保护运行态经方法读写）──

    def clear_stop_request(self) -> None:
        """清除用户停止请求标记（auth-switch 恢复等继续运行路径用）。"""
        self._stop_requested = False

    def mark_resume_next(self) -> None:
        """置位「下次 query 续用既有 sdk 会话」（编排预设 / 暂停恢复）。"""
        self._resume_next = True

    def clear_resume_next(self) -> None:
        """清除续会话标记（恢复分支消费后调用）。"""
        self._resume_next = False

    def consume_resume_reprompt(self) -> bool:
        """取走「恢复轮须重发初始 prompt」标记（读并复位）。"""
        flag = self._resume_reprompt
        self._resume_reprompt = False
        return flag

    def set_resume_reprompt(self, flag: bool) -> None:
        """设置「恢复轮须重发初始 prompt」标记（query 收尾路径结算）。"""
        self._resume_reprompt = flag

    def take_gate_feedback(self) -> str | None:
        """取走 iteration gate 反馈（读并清空；None = 无待注入反馈）。"""
        feedback = self._pending_gate_feedback
        self._pending_gate_feedback = None
        return feedback

    def set_gate_feedback(self, feedback: str) -> None:
        """存 iteration gate 反馈（下一轮 prompt 渲染直接用作 feedback）。"""
        self._pending_gate_feedback = feedback

    def mark_stage_reviewed(self, index: int) -> None:
        """记录最近一次 per-stage review 审完的阶段索引。"""
        self._last_reviewed_stage_index = index

    def _create_driver_for_query(self, iteration: int) -> Driver:
        from workflow_controller.driver.factory import create_driver

        # iteration 2+ 默认续同一 claude session（保留上下文，让 loop 多轮无缝 +
        # handoff 有累积前提）；iteration 1 冷启动；_resume_next（用户点 Resume）强制续。
        should_resume = self._resume_next or (iteration > 1 and self.native_session_id is not None)
        resume_id = self.native_session_id if should_resume else None
        driver = create_driver(
            getattr(self.config, "runtime", None),
            cli_wrapper=getattr(self.config, "cli_wrapper", ""),
            permission_mode=getattr(self.config, "permission_mode", "bypassPermissions"),
            resume=resume_id,
        )
        if self._resume_next:
            self._resume_next = False
        self._driver = driver
        return driver

    async def _process_query_event(self, event: Event, driver_error: str) -> str:
        self.last_activity = datetime.now(tz=UTC)
        if event.event_type == EventType.ERROR:
            driver_error = str(event.data.get("error", ""))
        if (
            event.event_type == EventType.ITERATION_END
            and event.data.get("reason") == "gate_failed"
        ):
            self.state = SessionState.IDLE
            self.loop_status = "no"
            self.stop_reason = "gate_failed"
            self.last_error = driver_error or "gate failed"
            await self.emit_outcome("failed")
        return driver_error

    def _recover_native_session_id(self) -> None:
        """按 cwd 和创建时间从 opencode.db 恢复 native_session_id。"""
        import contextlib
        import sqlite3

        from workflow_controller.driver.opencode import _resolve_opencode_db_path

        db_path = _resolve_opencode_db_path()
        if db_path is None or not db_path.is_file():
            return
        cwd = getattr(self.config, "cwd", None) or ""
        if not cwd:
            return
        try:
            with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
                row = conn.execute(
                    "SELECT id FROM session WHERE directory = ? ORDER BY time_created DESC LIMIT 1",
                    (cwd,),
                ).fetchone()
                if row:
                    self.native_session_id = row[0]
                    # 启发式兜底：并行 agent 节点共享同一 work_dir 时，
                    # 「最近一条」未必对应当前 Workflow Controller session，可能 resume 到
                    # 错误 session。命中即告警，便于串台时排查。
                    logger.warning(
                        "recovered native_session_id=%s for session %s via "
                        "cwd+time heuristic; may be ambiguous when multiple "
                        "opencode sessions share work_dir %s",
                        row[0],
                        self.id,
                        cwd,
                    )
        except Exception:
            logger.debug("native_session_id 恢复探测失败（sqlite 读失败），保持现状")
        # plugin_metadata 是溯源信息（birth_traceback 等），不属于运行态，reset 保留。

    async def _apply_resolved_provider(self) -> None:
        """auth_rotator 解析可用 provider 并同步 config（必要时通知回调）。"""
        if self._auth_rotator is None or self.config.provider_index is None:
            return
        from workflow_controller.driver.factory import resolve_runtime

        runtime = resolve_runtime(getattr(self.config, "runtime", None))
        provider_index = self._auth_rotator.resolve_available_provider(
            self.config.provider_index,
            runtime=getattr(self.config, "runtime", None),
        )
        if provider_index is None:
            raise RuntimeError("No available provider for session")
        provider = self._auth_rotator.get_provider(provider_index)
        model_stale = not self.config.model_pinned and (
            (bool(provider.model) and provider.model != self.config.model)
            or (runtime == "codex_cli" and not provider.model and bool(self.config.model))
        )
        if provider_index != self.config.provider_index or model_stale:
            self.config.provider_index = provider_index
            if provider.model:
                self.config.model = provider.model
                self.config.model_pinned = False
            elif runtime == "codex_cli" and not self.config.model_pinned:
                self.config.model = ""
                self.config.model_pinned = False
            if self._provider_change_callback is not None:
                await self._provider_change_callback(provider_index, self.config.model or "")

    def _resolve_model_and_env(self) -> tuple[str, dict[str, str]]:
        """(model, sdk_env)：rotator env 先解析 model，再叠加会话级 env_overrides。"""
        sdk_env = (
            self._auth_rotator.env_for_driver(
                dict(os.environ),
                getattr(self.config, "provider_index", None),
            )
            if self._auth_rotator is not None
            else {}
        )
        model = resolve_query_model(self.config, sdk_env)
        # Per-session env_overrides（traceback 重放注入 XDG_DATA_HOME 等）覆盖 auth_rotator
        # 的 env：env_overrides 是会话级显式配置，应当赢。traceback 的 env_overrides 只含
        # XDG_DATA_HOME 等 isolation key，不与 auth_rotator 的 API key 冲突。
        env_overrides = getattr(self.config, "env_overrides", None) or {}
        if env_overrides:
            sdk_env = {**sdk_env, **env_overrides}
        return model, sdk_env

    async def _emit_query_error(self, e: Exception, iteration: int) -> AsyncIterator[Event]:
        """driver 异常收尾：置 IDLE、发 failed 结局、补 ERROR + ITERATION_END。"""
        error_msg = f"{type(e).__name__}: {e!s}"
        logger.exception("Driver query error session=%s type=%s", self.id, type(e).__name__)
        self.state = SessionState.IDLE  # Changed from FAILED to IDLE
        self.loop_status = "no"
        self.stop_reason = "error"
        self.last_error = error_msg
        await self.emit_outcome("failed")
        yield Event.create(
            session_id=self.id,
            iteration=iteration,
            event_type=EventType.ERROR,
            data={
                "error": str(e),
                "error_type": type(e).__name__,
                "traceback": traceback.format_exc()[-500:],
            },
        )
        # driver 异常（如 CLI 非零退出）在 session 层收尾：ERROR 后补
        # ITERATION_END，事件流消费者依赖它判定 query 边界。
        yield Event.create(
            session_id=self.id,
            iteration=iteration,
            event_type=EventType.ITERATION_END,
            data={"reason": "internal_error"},
        )
