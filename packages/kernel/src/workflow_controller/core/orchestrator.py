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
import os
import time
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from workflow_controller.config.settings import (
    DEFAULT_HOOK_SERVER_PORT,
    AppConfig,
    SessionConfig,
    SessionReviewerConfig,
)
from workflow_controller.core._subagent import summarize_subagent_transcript
from workflow_controller.core.auth_rotator import AuthRotator, build_default_rotator
from workflow_controller.core.auth_rotator.detector import LimitDetector
from workflow_controller.core.context_reader import (
    read_main_context,
    read_subagent_context,
    snapshot_main_source,
    snapshot_source,
)
from workflow_controller.core.file_changes import FileChangeRegistry, is_mutating_tool
from workflow_controller.core.loop import SingleLoopEngine
from workflow_controller.core.orchestrator_helpers import (
    SessionFacts as SessionFacts,
)
from workflow_controller.core.orchestrator_helpers import (
    _build_activation_receipt_event,
    _build_file_change_context,
    _build_file_change_outcome,
    _build_gate_question_event,
    _build_incomplete_file_change_outcome,
    _build_iteration_boundary_event,
    _build_manual_prompt_event,
    _build_pre_tool_use_denial,
    _build_pre_tool_use_event,
    _build_reviewer_runtime_config,
    _build_run_attached_event,
    _build_session_auto_event,
    _build_session_complete_event,
    _build_session_end_event,
    _build_session_failed_event,
    _build_session_outcome_event,
    _build_session_started_event,
    _build_session_state_changed_event,
    _build_stop_requested_event,
    _get_session_config,
    _get_session_facts,
    _is_terminal_session,
    _last_assistant_digest,
    _limit_rotation_args,
    _list_project_agent_types,
    _preset_resume_session,
    _resolve_evaluator,
    _running_loop_sessions,
    _session_has_stages,
    _session_reviewer_mode,
    _unattached_run_id,
)
from workflow_controller.core.overlay_registry import OverlayRegistry
from workflow_controller.core.reviewer import HookDecisionEmitter, ReviewerInterceptor
from workflow_controller.core.reviewer_runner import DriverReviewerRunner
from workflow_controller.core.reviewer_service import ReviewerService
from workflow_controller.core.runs import RunKind, RunNotFoundError, RunStore
from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService
from workflow_controller.core.subagent_tracker import SubagentTracker
from workflow_controller.driver.hooks import (
    CodexHookConfigWriter,
    DriverHooks,
    SubagentProgressBridge,
)
from workflow_controller.event.event_bus import EventBus, InterceptResult
from workflow_controller.event.type_registry import EventTypeRegistry
from workflow_controller.hooks.adapter import HookPayloadAdapter
from workflow_controller.hooks.hook_config import HookConfigGenerator
from workflow_controller.hooks.server import HookServer, HookServerStartupError
from workflow_controller.plugin.base import IterationGateResult, Plugin, compose_initial_prompt
from workflow_controller.plugin.file_changes import (
    FileChangeContext,
    tool_execution_outcome_event,
    tool_will_execute_event,
)
from workflow_controller.plugin.registry import PluginRegistry
from workflow_controller.plugin.stages import artifacts_display
from workflow_controller.session.events import Event, EventType, outcome_kind_for_reason
from workflow_controller.session.manager import SessionPool
from workflow_controller.session.session import SessionState
from workflow_controller.store.event_router import EventStoreRouter
from workflow_controller.watchdog.core import WatchdogCore

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from workflow_controller.core.evaluator import Evaluator
    from workflow_controller.session.session import Session

logger = logging.getLogger(__name__)

SESSION_DRAIN_TIMEOUT_S = 5.0  # 成员 runtime 任务停后的排空上限（D-state 等卡死形态的兜底等待）

# 等待重试的固定等待窗；可配化属后续任务，不随初版实现
_WAIT_RETRY_SECONDS: int = 600


class Orchestrator:
    # 实例属性在类体声明、由 __init__ 调用的 _init_* 分组赋值（定义点与
    # 使用顺序解耦，mypy 不受方法文本序影响）
    _evaluators: dict[str, Evaluator]
    _evaluator_locks: dict[str, asyncio.Lock]
    _loops: dict[str, SingleLoopEngine]
    _loop_tasks: dict[str, asyncio.Task[None]]
    _restarting_sessions: set[str]
    _single_loop: SingleLoopEngine | None
    _hook_server: HookServer | None
    _reviewer: ReviewerService | None
    _session_reviewers: dict[str, ReviewerService]
    _shutdown_steps: list[Callable[[], Awaitable[None]]]
    _reviewer_interceptor: ReviewerInterceptor | None
    _hook_decision_emitter: HookDecisionEmitter | None
    _cli_to_workflow_controller_session: dict[str, str]
    _overlays: OverlayRegistry
    _reviewer_mode: str
    _stop_block_counts: dict[str, int]
    _stop_block_limit: int
    _hook_server_port_override: int | None

    def __init__(
        self,
        config: AppConfig,
        default_project_dir: Path | None = None,
        hook_server_port_override: int | None = None,
    ) -> None:
        self.config = config
        self._hook_server_port_override = hook_server_port_override
        _project_dir = (default_project_dir or Path(config.project_dir)).resolve()
        # 守护进程库锚点：一切 run/session 事件与账面（event_router 单库、
        # run_store 账面与 run 数据根）都锚这里，不随 config 漂移出两套真相；
        # boot 时 resolve 定格，运行期 cwd 变化不重锚。
        self._default_project_dir = _project_dir
        self._init_event_plane(_project_dir)
        self._init_runtime_slots()
        self._init_plane_integrations()
        self._file_change_contexts: dict[tuple[str, str], FileChangeContext] = {}
        self._run_store: RunStore | None = None  # 懒建（run_store 属性）——boot 不落 events.db 账面
        self._affiliation: SessionRunAffiliationService | None = (
            None  # 懒建（affiliation 属性），随 run_store
        )
        # 激活后挂靠钩子：显式归属在 _runtime_lock 临界区内落账的唯一通道
        self._post_attach_hook: Callable[[Session], Awaitable[None]] | None = None
        self.register_post_attach_hook(self._attach_on_activate)
        self.auth_rotator: AuthRotator | None = None
        self._limit_detector: LimitDetector | None = None
        self._reviewer_runner: DriverReviewerRunner | None = None

    @property
    def plugin_registry(self) -> PluginRegistry:
        """插件注册表（组合根直配注入 run 平面用，只读）。"""
        return self._plugin_registry

    @property
    def hook_server_active(self) -> bool:
        """hook server 是否在役（api 层健康快照等只读消费方用）。"""
        return self._hook_server is not None

    @property
    def effective_hook_server_port(self) -> int:
        """运行期唯一 Hook Server 端口（CLI 或默认值）。"""
        if self._hook_server_port_override is None:
            return DEFAULT_HOOK_SERVER_PORT
        return self._hook_server_port_override

    @property
    def hook_server_base_url(self) -> str:
        """运行期唯一 Hook Server 基础 URL。"""
        return f"http://{self.config.hook_server.host}:{self.effective_hook_server_port}"

    @property
    def default_project_dir(self) -> Path:
        """守护进程存储标识（启动期定格，run 根等记录路径的家）。"""
        return self._default_project_dir

    @property
    def run_store(self) -> RunStore:
        """run 记录（项目库 events.db）；懒建——实例化零落盘，记录库随首次访问初始化。"""
        if self._run_store is None:
            # 与 event_router 默认库路径同纪律（resolve）：symlink 下两锚不串
            self._run_store = RunStore(
                (self._default_project_dir / ".workflow-controller" / "events.db").resolve()
            )
        return self._run_store

    @property
    def affiliation(self) -> SessionRunAffiliationService:
        """归属服务（缓存 + 显式挂靠 + 隐式创建）；懒建，随 run_store。"""
        if self._affiliation is None:
            self._affiliation = SessionRunAffiliationService(
                self.run_store,
                audit=self._emit_run_attached,
                on_native_bound=self._on_native_bound,
            )
        return self._affiliation

    @property
    def reviewer(self) -> ReviewerService | None:
        return self._reviewer

    @property
    def overlays(self) -> OverlayRegistry:
        """The overlay registry (opencode-consumed)."""
        return self._overlays

    @property
    def reviewer_mode(self) -> str:
        return self._reviewer_mode

    @property
    def plugins(self) -> list[Plugin]:
        """Loaded plugin instances (public accessor for UI layers)."""
        return self._plugin_registry.plugins

    @property
    def plugins_dir(self) -> Path:
        return self._plugin_registry.plugins_dir

    @staticmethod
    def read_main_context(runtime: str, cwd: str, session_id: str, env: dict[str, str]) -> int:
        return read_main_context(runtime, cwd, session_id, env)

    @staticmethod
    def read_subagent_context(
        runtime: str,
        cwd: str,
        parent_id: str,
        agent_id: str,
        env: dict[str, str],
    ) -> int:
        return read_subagent_context(runtime, cwd, parent_id, agent_id, env)

    @staticmethod
    def snapshot_main_source(
        runtime: str,
        cwd: str,
        session_id: str,
        target: Path,
        env: dict[str, str],
    ) -> bool:
        return snapshot_main_source(runtime, cwd, session_id, target, env)

    @staticmethod
    def snapshot_subagent_source(
        runtime: str,
        cwd: str,
        parent_id: str,
        agent_id: str,
        target: Path,
        env: dict[str, str],
    ) -> bool:
        return snapshot_source(runtime, cwd, parent_id, agent_id, target, env)

    list_project_agent_types = staticmethod(_list_project_agent_types)
    _limit_rotation_args = staticmethod(_limit_rotation_args)
    _preset_resume_session = staticmethod(_preset_resume_session)

    @classmethod
    def create_default(
        cls,
        config: AppConfig | None = None,
        config_dir: Path | None = None,
    ) -> Orchestrator:
        """Convenience constructor that attaches a default AuthRotator.

        If ``config_dir`` is given, rotator state goes under that directory;
        otherwise uses the platformdirs default (~/.config/workflow-controller/ etc.).
        """
        cfg = config or AppConfig()
        orch = cls(config=cfg)
        if config_dir is not None:
            orch.auth_rotator = AuthRotator(
                state_path=config_dir / "auth_state.json",
                env_path=config_dir / ".env",
            )
        else:
            orch.auth_rotator = build_default_rotator()
        return orch

    def register_evaluator(self, name: str, evaluator: Evaluator) -> None:
        self._evaluators[name] = evaluator

    def register_post_attach_hook(self, cb: Callable[[Session], Awaitable[None]]) -> None:
        """激活后挂靠钩子注册位（run 平面的显式归属写点挂进临界区）。"""
        self._post_attach_hook = cb

    async def deactivate_session(self, session: Session, *, pause: bool = True) -> None:
        """停排空绑定到指定 session 的运行时（session 控制面的公开原语）。"""
        loop = self._loops.get(session.id)
        if loop is not None:
            if pause and loop.is_current(session):
                await loop.pause_current()
            loop.request_stop()
            # 等待 runtime task 真正退出：request_stop 只是发信号 + cancel，
            # run() 收尾是异步的。不 await 会导致紧接着的 resume 在
            # _activate_session 里读到旧 task 还没 done 而 return，
            # resume 静默失败。超时回退防止 task 卡死（如子进程 D-state）。
            task = self._loop_tasks.get(session.id)
            if task is not None and task is not asyncio.current_task() and not task.done():
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=SESSION_DRAIN_TIMEOUT_S)
                except TimeoutError:
                    # shield 阻止取消向内传播，超时后底层 task 可能仍在运行
                    # （如子进程 D-state 卡死）；显式告警，避免紧接着的 resume
                    # 静默失败时无从排查。
                    logger.warning(
                        "runtime task for session %s did not finish within "
                        "%ss; a subsequent resume may still race with it",
                        session.id,
                        SESSION_DRAIN_TIMEOUT_S,
                    )
            logger.info("Deactivated runtime for session %s", session.id)

    async def start(self) -> None:
        # Load ghost sessions from events.db before anything else, so the UI
        # immediately shows historical sessions on restart.
        self.pool.load_sessions()
        self.bus.start()
        self.bus.observe(EventType.ITERATION_START, self._register_session)
        self.watchdog.start()
        if self.config.hook_server.enabled:
            self._hook_server = self._build_hook_server()
            self._plugin_registry.load(self, self._hook_server)
            try:
                await self._hook_server.start()
                self._refresh_runtime_hooks(self.config.runtime, Path(self.config.project_dir))
            except Exception as exc:
                hook_server = self._hook_server
                host, port = hook_server.host, hook_server.port
                self._hook_server = None
                try:
                    await hook_server.stop_server()
                except Exception as cleanup_exc:
                    logger.warning(
                        "Hook Server cleanup failed during startup rollback: %s",
                        cleanup_exc,
                    )
                self._plugin_registry.unload_all()
                await self.bus.stop()
                message = (
                    f"Hook Server 启动或 Claude hooks 刷新失败（{host}:{port}），"
                    f"Workflow Controller 启动失败。{exc}"
                )
                logger.error(message)
                raise HookServerStartupError(message) from exc

        if self.auth_rotator is not None:
            self._limit_detector = LimitDetector(
                rotator=self.auth_rotator,
                bus=self.bus,
                session_handler=self._handle_session_limit,
            )
            self.auth_rotator.schedule_pending_resets()

        await self._maybe_start_reviewer()
        self._register_reviewer_interceptors()

    async def switch_session_provider(self, session_id: str) -> int:
        if self.auth_rotator is None:
            raise ValueError("auth rotator not available")
        session = self.pool.get(session_id)
        if session is None:
            raise ValueError(f"session not found: {session_id}")
        await self._ensure_session_snapshots(session)
        if session.config.provider_index is None:
            raise ValueError(f"session {session_id} has no provider binding")
        replacement = self.auth_rotator.find_alternative_provider(
            session.config.provider_index,
            runtime=session.config.runtime,
        )
        if replacement is None:
            raise ValueError("no alternative provider available")
        await self.auth_rotator.ensure_provider_capability(
            replacement,
            "worker",
            runtime=session.config.runtime or self.config.runtime,
            cwd=session.config.cwd or None,
        )
        provider = self.auth_rotator.get_provider(replacement)
        session.config.provider_index = replacement
        session.sdk_session_id = None
        if provider.model:
            session.config.model = provider.model
        await self.pool.persist(session)
        loop = self._loops.get(session_id)
        if loop is not None:
            loop.request_auth_switch_now()
        return replacement

    async def restart_reviewer(self) -> None:
        """Stop and restart the reviewer with current config.

        Safe to call at any time — if the reviewer is mid-decision the
        in-flight call will raise and the caller (Stop hook / AUQ) will
        fall back gracefully on the next worker iteration.
        """
        if self._reviewer:
            await self._reviewer.stop()
            self._reviewer = None
        await self._maybe_start_reviewer()

    async def start_session(self, session_name: str) -> Session:
        config = self._get_session_config(session_name)
        session = await self.pool.create(config)
        try:
            await self._activate_session(session)
        except (Exception, asyncio.CancelledError):
            await self._discard_unstarted_session(session)
            raise
        return session

    async def reclaim_deleted_session_affiliation(self, session_id: str) -> None:
        """用户删除路径的归属+sessions 表行回收（与 session 删除同点调用，单事务）。

        隐式归属连行带 run 同事务回收；显式 run 的成员行是调用方的记录，
        不连坐删（只清 sessions 表行）。删除即清账，不等 boot 回退。
        """
        # 直查 _affiliation 而非属性：未铸过归属时不触记录（懒建纪律）。
        if self._affiliation is None:
            return
        current = self._affiliation.resolve(session_id)
        if current is not None and (
            (await self.run_store.get(current.run_id)).kind == RunKind.IMPLICIT
        ):
            await self._affiliation.discard(session_id, delete_roster=True)
            return
        await self.event_router.delete_roster(session_id)
        self._affiliation.evict(session_id)

    def discard_deleted_session(self, session_id: str) -> None:
        """删除链的内存驱逐：只清 pool 行——持久层已由调用方的删除事务清零。"""
        self.pool.discard(session_id)

    def discard_terminal_session(self, session_id: str) -> bool:
        """仅释放已终结 session 的内存副本，保留持久记录。"""
        if self.pool.get(session_id) is None or not self.is_terminal(session_id):
            return False
        return self.pool.discard(session_id)

    def restore_session_from_store(self, session_id: str) -> bool:
        """按需从持久记录恢复已离开内存的 session。"""
        return self.pool.restore_from_store(session_id) is not None

    async def start_session_with_config(
        self,
        config: SessionConfig,
        *,
        plugin_metadata: dict[str, dict[str, Any]] | None = None,
        compose_prompt: bool = True,
    ) -> Session:
        """Create a session and start its independent runtime.

        ``plugin_metadata`` 在入 pool 前设好（与 resume 变体相同），调用方
        无需事后直改并回写列表。``compose_prompt=False`` 保留调用方
        initial_prompt 原样、不改写传入对象——精确 prompt 入口（程序化
        交接）用，跳过插件 prefix 聚合。
        """
        if compose_prompt:
            config.initial_prompt = compose_initial_prompt(self.plugins, config)
        session = await self.pool.create(config, plugin_metadata=plugin_metadata)

        if self.auth_rotator is not None:
            session.set_auth_rotator(self.auth_rotator)

        # Pre-registered project overlays → new session (project bucket stays for reuse).
        self._overlays.transfer_project_overlays(session.id, getattr(config, "cwd", ""))

        await self.event_router.store(_build_session_started_event(session.id, config))

        return await self._activate_or_discard(session)

    async def start_session_with_resume(
        self,
        config: SessionConfig,
        *,
        resume_sdk_session_id: str | None = None,
        plugin_metadata: dict[str, dict[str, Any]] | None = None,
        start_action: str = "session_started",
        inherit_overlays_from: str | None = None,
        initial_current_iteration: int | None = None,
    ) -> Session:
        """Start a session, optionally resuming a seeded SDK session and carrying
        plugin metadata (e.g. agent_traceback's fork origin).

        Like :meth:`start_session_with_config` but, BEFORE activation, pre-sets the
        SDK session id to resume (the seeded session) and merges ``plugin_metadata``
        onto the session, so the first query resumes the seeded session.

        ``inherit_overlays_from`` (optional source session id) copies that
        session's session-scoped subagent hints + global instructions into the new
        session's buckets BEFORE activation, so the driver's first query round
        picks them up. Used by checkpoint replay: a hint registered on the origin
        session before the replay must reach the new replay session (otherwise the
        re-called subagent's system prompt would lose the user's diagnostic).

        ``initial_current_iteration`` (optional) pre-sets ``session.current_iteration``
        BEFORE activation so the loop's first iteration number aligns with the
        origin's progress at the checkpoint (transparent replay: the Jinja template's
        ``{{ iteration }}`` matches the LLM's self-perceived progress). Used by
        traceback replay; ``None`` leaves the default 0.
        """
        config.initial_prompt = compose_initial_prompt(self.plugins, config)
        # plugin_metadata 在入 pool 前设好，避免读端看到空 metadata。
        session = await self.pool.create(config, plugin_metadata=plugin_metadata)
        if self.auth_rotator is not None:
            session.set_auth_rotator(self.auth_rotator)
        self._overlays.transfer_project_overlays(session.id, getattr(config, "cwd", ""))
        if inherit_overlays_from is not None:
            self._overlays.transfer_session_overlays(session.id, inherit_overlays_from)
        # Set resume + plugin metadata BEFORE activation so the first query
        # resumes the seeded session.
        self._preset_resume_session(session, resume_sdk_session_id, initial_current_iteration)
        # 再次落库：sdk_session_id / _resume_next / current_iteration 在 create 之后才设。
        await self.pool.persist(session)
        await self._emit_session_auto_event(
            session.id,
            start_action,
            config,
            resume_sdk_session_id or "",
        )
        return await self._activate_or_discard(session)

    # ------------------------------------------------------------------
    # run 归属基础设施（记录与归属服务的中立持有者）
    # ------------------------------------------------------------------

    async def create_session(self, name: str, role: str = "worker") -> Session:
        """Create a bare session (no loop) and attach auth_rotator if present.

        Useful for testing and for callers that want a Session object without
        immediately starting a LoopEngine.
        """
        config = SessionConfig(name=name, role=role)
        session = await self.pool.create(config)
        if self.auth_rotator is not None:
            session.set_auth_rotator(self.auth_rotator)
        return session

    def get_or_create_reviewer_runner(self) -> DriverReviewerRunner:
        """Return the reviewer runner, creating one lazily if not yet built.

        If ``auth_rotator`` is present on this orchestrator, it is attached to
        the runner so reviewer subprocesses inherit the correct auth env.
        """
        if self._reviewer_runner is None:
            self._reviewer_runner = DriverReviewerRunner()
            if self.auth_rotator is not None:
                self._reviewer_runner.set_auth_rotator(self.auth_rotator)
        return self._reviewer_runner

    def reviewer_for(self, session_id: str) -> ReviewerService | None:
        session = self.pool.get(session_id)
        if session is not None and session.config.reviewer is not None:
            if not session.config.reviewer.enabled:
                return None
            reviewers = getattr(self, "_session_reviewers", {})
            return reviewers.get(session_id)
        return None

    async def ensure_reviewer_snapshot(self, session_id: str) -> None:
        """按持久化 profile 补建 reviewer 快照（进程重启后 ghost 不缺位）。"""
        session = self.pool.get(session_id)
        if session is not None:
            await self._ensure_session_reviewer(session)

    def register_shutdown_step(self, step: Callable[[], Awaitable[None]]) -> None:
        """登记 run 平面关停步骤（在 session 运行时与池停止之前执行）。"""
        self._shutdown_steps.append(step)

    def subagent_status(self, sid: str) -> dict[str, dict[str, Any]]:
        return self._subagents.in_flight(sid)

    async def interrupt_subagents(
        self, session_id: str, snapshot: dict[str, dict[str, Any]]
    ) -> None:
        """中断会话的全部在途子代理（plugin registry 的中断端口）。"""
        await self._subagents.interrupt_all(session_id, snapshot)

    def get_stop_block_count(self, key: str) -> int:
        return self._stop_block_counts.get(key, 0)

    def bump_stop_block_count(self, key: str) -> None:
        self._stop_block_counts[key] = self._stop_block_counts.get(key, 0) + 1

    def get_session(self, sid: str) -> Session | None:
        return self.pool.get(sid)

    def schedule_retry(self, sid: str) -> int:
        """Flip session to WAITING_RETRY. Encapsulates the
        ``_WAIT_RETRY_SECONDS`` constant + the 3 session mutations so they
        don't leak into the reviewer interceptor. Returns the new
        retry_count, or 0 if the session isn't in the pool (logs a warning).
        """
        session_obj = self.pool.get(sid)
        if session_obj is None:
            logger.warning(
                "wait_and_retry: pool has no session for %s, falling back to let_exit",
                sid[:12],
            )
            return 0
        session_obj.state = SessionState.WAITING_RETRY
        session_obj.retry_scheduled_at = time.monotonic() + _WAIT_RETRY_SECONDS
        session_obj.retry_count += 1
        return session_obj.retry_count

    def mark_loop_let_exit(self, session_id: str) -> None:
        """Signal the target session runtime that it should end
        with ``stop_reason=reviewer_complete`` (bypass the evaluator).

        Encapsulates runtime lookup so neither plugin
        interceptors nor the reviewer interceptor touch loop internals:
        any control-event verdict of ``action="let_exit"`` routes through
        here. No-op when that session has no running loop.
        """
        loops = getattr(self, "_loops", {})
        loop = loops.get(session_id)
        if loop is None:
            loop = getattr(self, "_single_loop", None)
        if loop is not None and loop.is_current_id(session_id):
            loop.mark_reviewer_let_exit()

    def make_driver_hooks(
        self,
        mig_session_id: str,
    ) -> DriverHooks:
        """Build the in-process hook bundle a driver uses to reach the reviewer.

        CLI runtime ignores this (it uses the HTTP hook server); SDK/opencode
        drivers translate their native hooks into these callbacks.
        """
        session = self.pool.get(mig_session_id)
        from workflow_controller.driver.factory import resolve_runtime

        runtime_hook_config_valid: Callable[[], bool] | None = None
        session_uses_codex_hooks = (
            session is not None
            and resolve_runtime(session.config.runtime or self.config.runtime) == "codex_cli"
        )
        hook_active = self.config.hook_server.enabled and self.hook_server_active
        if session is not None and session_uses_codex_hooks and hook_active:
            project_dir = Path(session.config.cwd or self.config.project_dir).resolve()
            CodexHookConfigWriter().prepare(project_dir)
            runtime_hook_config_valid = partial(
                self._codex_hooks_are_managed,
                mig_session_id,
            )
        return DriverHooks(
            session_id=mig_session_id,
            hook_server_base_url=self.hook_server_base_url,
            on_pre_tool_use=self._handle_driver_pre_tool_use,
            on_post_tool_use=self._handle_driver_post_tool_use,
            file_change_hook_url=self.hook_server_base_url,
            file_changes_enabled=lambda: self.file_changes.has_subscribers,
            on_stop=lambda sid, data: self._decide_stop(
                sid,
                data.get("last_assistant_message", "") or "",
                block_key=sid,
            ),
            on_subagent_start=self._subagents.on_start,
            on_subagent_progress=self._subagents.on_progress,
            on_subagent_stop=self._subagents.on_stop,
            subagent_progress_bridge=SubagentProgressBridge(),
            # opencode gate (AskUserQuestion-equivalent); ignored by cc drivers.
            on_gate_question=self._resolve_gate_question,
            # lazy so iteration-boundary registrations land next round
            get_overlays=lambda: self._overlays.get_overlays(mig_session_id),
            auto_compaction=not any(
                p.config.enabled and p.manages_context_window() for p in self.plugins
            ),
            runtime_hook_config_valid=runtime_hook_config_valid,
        )

    def register_subagent_hint(
        self,
        session_id: str,
        subagent_type: str,
        hint: str,
    ) -> None:
        self._overlays.register_subagent_hint(session_id, subagent_type, hint)

    def clear_subagent_hint(
        self,
        session_id: str,
        subagent_type: str | None = None,
        hint_index: int | None = None,
    ) -> int:
        return self._overlays.clear_subagent_hint(
            session_id,
            subagent_type,
            hint_index,
        )

    def get_subagent_hints(self, session_id: str) -> dict[str, list[str]]:
        return self._overlays.get_subagent_hints(session_id)

    def register_session_instruction(
        self,
        session_id: str,
        instruction: str,
    ) -> None:
        self._overlays.register_session_instruction(session_id, instruction)

    def clear_session_instruction(
        self,
        session_id: str,
        instruction_index: int | None = None,
    ) -> int:
        return self._overlays.clear_session_instruction(
            session_id,
            instruction_index,
        )

    def get_session_instructions(self, session_id: str) -> list[str]:
        return self._overlays.get_session_instructions(session_id)

    def register_project_hint(
        self,
        project_dir: str,
        subagent_type: str,
        hint: str,
    ) -> None:
        self._overlays.register_project_hint(project_dir, subagent_type, hint)

    def clear_project_hint(
        self,
        project_dir: str,
        subagent_type: str | None = None,
        hint_index: int | None = None,
    ) -> int:
        return self._overlays.clear_project_hint(
            project_dir,
            subagent_type,
            hint_index,
        )

    def get_project_hints(self, project_dir: str) -> dict[str, list[str]]:
        return self._overlays.get_project_hints(project_dir)

    def register_project_instruction(
        self,
        project_dir: str,
        instruction: str,
    ) -> None:
        self._overlays.register_project_instruction(project_dir, instruction)

    def clear_project_instruction(
        self,
        project_dir: str,
        instruction_index: int | None = None,
    ) -> int:
        return self._overlays.clear_project_instruction(
            project_dir,
            instruction_index,
        )

    # ── ReviewerHost implementation ─────────────────────────────────────
    # Phase 2.1: the ReviewerInterceptor (core/reviewer.py) reads live
    # reviewer / mode / loop / session state through this narrow protocol.
    # All members read at call time so restart_reviewer / set_reviewer_mode
    # need no re-registration of interceptors.

    def get_project_instructions(self, project_dir: str) -> list[str]:
        return self._overlays.get_project_instructions(project_dir)

    async def resolve_boundary_pending(
        self,
        session_obj: Session,
        iteration: int,
    ) -> IterationGateResult | None:
        """iteration-boundary gate 放行后的 staged-session 收尾。

        - pending_stage_review 非空 → 跑 per-stage reviewer
        - stages_complete → 确定性完成（auto 模式 mark let_exit）
        - 两者皆无 → None
        """
        if getattr(session_obj, "pending_stage_review", None) is not None:
            return await self._run_stage_review_at_boundary(session_obj, iteration)
        if getattr(session_obj, "stages_complete", False):
            session_obj.stages_complete = False
            # manual：完成判定权归人，不 mark（门禁按盘面重推导，切回 auto 自会补）
            if self.get_reviewer_mode(session_obj.id) != "manual":
                self.mark_loop_let_exit(session_obj.id)
        return None

    def resolve_cli_session_id(self, cli_session_id: str) -> str | None:
        return self._cli_to_workflow_controller_session.get(cli_session_id)

    async def stop_session(
        self,
        session_id: str,
        reason: str = "user_stop",
        *,
        persist_stopped_state: bool = True,
    ) -> None:
        """Stop one session and drain only its bound runtime task.

        ``persist_stopped_state=False`` 停+排空不落中间 STOPPED 列表——终态
        由调用方置好后单次落库（程序化交接停 worker：一次落账、终态 IDLE）。
        """
        session = self.pool.get(session_id)
        if session is None:
            return
        await session.stop(reason=reason)
        if persist_stopped_state:
            await self.pool.persist(session)
        await self.deactivate_session(session, pause=False)

    def get_session_facts(self, session_id: str) -> SessionFacts | None:
        """成员运行事实只读原语：run 平面（world 成员表/终态谓词）经此读
        成员态，不摸池内部；不在池返回 None（不会再产结局）。
        """
        return _get_session_facts(self.pool.get(session_id))

    def is_pausable(self, session_id: str) -> bool:
        """可暂停谓词：loop 在途（run 平面组控只消费谓词，不消费字段值）。"""
        facts = self.get_session_facts(session_id)
        return facts is not None and facts.loop_status == "yes"

    def is_resumable(self, session_id: str) -> bool:
        """可复用谓词：休息态（IDLE + user_pause）。"""
        facts = self.get_session_facts(session_id)
        return facts is not None and facts.state == "idle" and facts.stop_reason == "user_pause"

    def is_terminal(self, session_id: str) -> bool:
        """终态谓词：loop 落 no 且非用户暂停（暂停是可恢复断点，不是终态）；
        不在池 = 不会再产结局，按终态计。
        """
        return _is_terminal_session(self.get_session_facts(session_id))

    def is_restarting(self, session_id: str) -> bool:
        """重启窗谓词：restart_session 的 stop→重激活之间，终态为瞬态。"""
        return session_id in self._restarting_sessions

    async def wait_session_done(
        self,
        session_id: str,
        timeout: float | None = None,
        *,
        wait_timeout: float | None = None,
    ) -> bool:
        """Wait for one session's runtime task to drain.

        A stopped session without a registered runtime is already drained when
        ``loop_status=no``. Awaiting the task avoids state polling and accepts
        both IDLE (natural loop exit) and STOPPED (direct lifecycle stop).

        竞态防护（resume 窗口）：task 未注册且 loop_status=yes 时短轮询等待
        注册，不误判超时（带超时预算 15s 上界，无预算调用 60s）。
        超时语义（timeout 传入时）：按 session 的**实际运行时长**
        （active_seconds，暂停不计）核对而非 wall-clock；每轮先核对再等待、
        单轮等待不超过剩余预算。wait_timeout 限制本次调用等待时长，
        不消耗执行预算。
        """
        if timeout is not None and wait_timeout is not None:
            raise ValueError("timeout and wait_timeout are mutually exclusive")
        deadline = (
            asyncio.get_running_loop().time() + wait_timeout if wait_timeout is not None else None
        )
        session = self.pool.get(session_id)
        if session is None:
            return False

        outcome = await self._await_loop_task_registration(session, session_id, deadline, timeout)
        if isinstance(outcome, bool):
            return outcome
        task = outcome

        if task is None or task.done():
            return session.loop_status != "yes" or task is not None
        if task is asyncio.current_task():
            return session.loop_status != "yes"

        if deadline is not None:
            done, _ = await asyncio.wait(
                {task}, timeout=max(0, deadline - asyncio.get_running_loop().time())
            )
            return bool(done)

        if timeout is None:
            return await self._shield_wait_outcome(session, task)

        interval = getattr(self, "_reconcile_interval", 30.0)
        return await self._wait_with_active_budget(session_id, task, timeout, interval)

    def running_loop_sessions(self) -> list[dict[str, str]]:
        """各 loop 运行时当前监视中的 session 概要（SSE loop_update 数据源）。

        可能经 to_thread 在非事件循环线程执行，先快照再遍历。
        """
        return _running_loop_sessions(list(self._loops.values()))

    async def pause_session(self, session_id: str) -> None:
        session = self.pool.get(session_id)
        if session:
            await self.pool.pause(session_id)
            await self.deactivate_session(session)

    async def play_session(self, session_id: str) -> None:
        session = self.pool.get(session_id)
        if session:
            await self._activate_session(
                session,
                prepare=lambda: self.pool.play(session_id),
            )

    async def resume_session(self, session_id: str) -> None:
        session = self.pool.get(session_id)
        if session:
            await self._activate_session(
                session,
                prepare=lambda: self.pool.resume(session_id),
            )

    async def continue_session(self, session_id: str, prompt: str | None = None) -> None:
        """超时续跑：停掉挂死运行时后按同一 sdk 会话续跑（上下文保留）。

        续跑预算归零重计——挂死段已白烧的时长不挤占续跑段额度；可附续跑提示。
        """
        session = self.pool.get(session_id)
        if session is None:
            raise ValueError(f"session {session_id} 不在池内，无法续跑")
        await self.stop_session(session_id)
        await self.pool.resume(session_id)
        session.settle_active()
        session.active_seconds = 0.0
        if prompt:
            await self.pool.inject_prompt(session_id, prompt)
        await self.resume_session(session_id)

    # ── prompt overlay registry — delegates to OverlayRegistry ─────────
    # (opencode-only; SDK/CLI get empty via DriverHooks.get_overlays.)

    async def restart_session(self, session_id: str) -> None:
        """停止、重置并仅重新激活目标 session。"""
        session = self.pool.get(session_id)
        if session is None:
            raise ValueError(f"session {session_id} 不在池内，无法重启")
        self._restarting_sessions.add(session_id)
        try:
            await self.stop_session(session_id)

            async def reset_session() -> None:
                session.reset()
                await self.pool.persist(session)

            await self._activate_session(session, prepare=reset_session)
        finally:
            self._restarting_sessions.discard(session_id)
            # 窗口内的终态事件被收尾判定短路；窗口关闭补发一次状态事件，
            # 重启失败滞留终态时收尾规则可复评（成功路径成员运行中，复评无害）
            session = self.pool.get(session_id)
            if session is not None:
                with contextlib.suppress(Exception):
                    await self._emit_session_state_changed(
                        session_id,
                        session.state,
                        session.state,
                    )

    async def inject_prompt(self, session_id: str, prompt: str) -> None:
        """成员定向注入薄原语（注入 + 手工操作留痕）。"""
        await self.pool.inject_prompt(session_id, prompt)
        await self.bus.emit(_build_manual_prompt_event(session_id, prompt))

    async def apply_session_overrides(self, session_id: str, overrides: Mapping[str, Any]) -> None:
        """成员配置部分更新（新 attempt 上下文刷新用），改后即持久化。"""
        session = self.pool.get(session_id)
        if session is None:
            raise ValueError(f"session {session_id} 不在池内，无法更新配置")
        for key, value in overrides.items():
            setattr(session.config, key, value)
        await self.pool.persist(session)

    def get_reviewer_mode(self, session_id: str) -> str:
        return _session_reviewer_mode(self.pool.get(session_id), self._reviewer_mode)

    async def set_session_reviewer_mode(self, session_id: str, mode: str) -> None:
        if mode not in ("auto", "manual"):
            raise ValueError("reviewer mode must be 'auto' or 'manual'")
        session = self.pool.get(session_id)
        if session is None:
            raise ValueError(f"session not found: {session_id}")
        await self._ensure_session_snapshots(session)
        if session.config.reviewer is None:
            raise RuntimeError("session reviewer 配置缺失")
        session.config.reviewer.mode = mode
        await self.pool.persist(session)

    def set_reviewer_mode(self, mode: str) -> None:
        """Toggle between 'auto' (reviewer auto-answers + hooks inject reminder)
        and 'manual' (reviewer paused, user replies via the Web UI input).

        Idempotent. Safe to call any time after start().
        """
        if mode not in ("auto", "manual"):
            raise ValueError(f"invalid reviewer mode: {mode!r} (expected 'auto' or 'manual')")
        self._reviewer_mode = mode
        if self._reviewer is not None:
            if mode == "manual":
                self._reviewer.pause()
            else:
                self._reviewer.resume()

    def discover_plugins(self) -> list[tuple[Any, Path, type[Plugin]]]:
        return self._plugin_registry.discover()

    # ── project-dir-keyed (pre-session) ──────────────────────────────────

    def reload_plugin(self, name: str) -> bool:
        # 插件 reload 独立于 hook_server：hook_server 端口被占（如 18765 被
        # 外部进程占用）时 _hook_server 为 None，但插件加载/卸载仍应生效，
        # 否则 toggle 后 config.yaml 的 enabled 已变、内存插件状态却不变，
        # 前端 tab 显隐（依赖 manifest 加载标志）与磁盘配置脱节。
        return self._plugin_registry.reload_plugin(name, self, self._hook_server)

    async def reload_plugins(self) -> None:
        """Stop hook server, unload all plugins, reload from disk, restart server.

        This allows plugin enable/disable changes to take effect at runtime
        without restarting the entire application.
        """
        logger.info("reloading plugins: stopping hook server")
        if self._hook_server:
            await self._hook_server.stop_server()
            self._hook_server = None
            await asyncio.sleep(0.1)

        self._plugin_registry.unload_all()

        if self.config.hook_server.enabled:
            await self._restart_hook_server()

    async def shutdown(self) -> None:
        # 0. run 平面先收尾：驱动收尾轮与 run 键进程终止先于 session 运行时停止
        for step in self._shutdown_steps:
            # 关停序的完整性优先于单步骤成败：留痕后继续，不让异常跳过
            # pool.stop_all/插件卸载/bus 停止等后续收尾步骤
            try:
                await step()
            except Exception:
                logger.exception("run-plane shutdown step failed; continuing")
        # 1. Stop every per-session runtime before stopping drivers.
        runtime_tasks = list(self._loop_tasks.values())
        for loop in list(self._loops.values()):
            loop.request_stop()
        if runtime_tasks:
            await asyncio.gather(*runtime_tasks, return_exceptions=True)
        self._loops.clear()
        self._loop_tasks.clear()
        self._single_loop = None
        reviewers = list(self._session_reviewers.values())
        self._session_reviewers.clear()
        for reviewer in reviewers:
            await reviewer.stop()
        # 2. Cancel + drain the fire-and-forget decision-event emit pool.
        if self._hook_decision_emitter is not None:
            await self._hook_decision_emitter.shutdown()
        # 3. Persist session state before shutdown so ghost sessions survive.
        #    (_persist_session 已在每次状态变更时实时 UPSERT 到 events.db，
        #    stop_all 会再 persist 一次终态。无需 _save_sessions 写 JSON。)
        # 4. Kill worker subprocesses
        await self.pool.stop_all()
        # 5. Unload plugins + stop services
        self._plugin_registry.unload_all()
        if self._reviewer:
            await self._reviewer.stop()
            self._reviewer = None
        if self._hook_server:
            await self._hook_server.stop_server()
        await self.bus.stop()

    def generate_hook_config(self, project_dir: Path | None = None) -> None:
        """写入各 runtime 需要的本地 hook 配置。"""
        generator = HookConfigGenerator(
            port=self.effective_hook_server_port,
            host=self.config.hook_server.host,
        )
        generator.write_settings((project_dir or Path.cwd()).resolve())

    def _refresh_runtime_hooks(self, runtime: str | None, project_dir: Path) -> None:
        """让具体 driver 准备自己的本地 hook 配置。"""
        from workflow_controller.driver.factory import create_driver

        create_driver(runtime or self.config.runtime).refresh_hooks(
            project_dir,
            hook_server_active=self.hook_server_active,
            hook_server_host=self.config.hook_server.host,
            hook_server_port=self.effective_hook_server_port,
        )

    async def _restart_hook_server(self) -> None:
        """重载后重启 hook server：端口占用最多重试 3 次（间隔 0.3s）。"""
        for attempt in range(3):
            if await self._try_start_hook_server():
                return
            if attempt < 2:
                await asyncio.sleep(0.3)
        logger.warning("Hook server failed to start (port in use?)")
        self._hook_server = None

    async def _try_start_hook_server(self) -> bool:
        """单次尝试重建并启动 hook server；OSError（如端口占用）返回 False。"""
        try:
            self._hook_server = self._build_hook_server()
            self._plugin_registry.load(self, self._hook_server)
            await self._hook_server.start()
            logger.info("hook server restarted with updated plugins")
            return True
        except OSError:
            return False

    def _build_hook_server(self) -> HookServer:
        """按配置装配 HookServer（启动与 reload 重建共用同一组回调）。"""
        return HookServer(
            port=self.effective_hook_server_port,
            host=self.config.hook_server.host,
            on_event=HookPayloadAdapter(self.bus),
            on_stop_request=self._handle_stop_request,
            on_pre_tool_use_request=self._handle_pre_tool_use_request,
            on_post_tool_use_request=self._handle_post_tool_use_request,
            on_subagent_start_request=self._handle_subagent_start_request,
            on_subagent_stop_request=self._handle_subagent_stop_request,
        )

    def _new_loop(self, session_id: str) -> SingleLoopEngine:
        return SingleLoopEngine(
            event_router=self.event_router,
            bus=self.bus,
            mode_getter=lambda: self.get_reviewer_mode(session_id),
            iteration_gate=self._run_iteration_boundary_gates,
            on_session_complete=self._run_plugin_session_complete,
            on_query_start=self._subagents.clear_session,
        )

    async def _run_session_runtime(
        self,
        session_id: str,
        loop: SingleLoopEngine,
    ) -> None:
        reviewer: ReviewerService | None = None
        try:
            await loop.run()
        finally:
            await self._close_file_change_contexts(session_id)
            async with self._runtime_lock:
                if self._loops.get(session_id) is loop:
                    self._loops.pop(session_id, None)
                task = self._loop_tasks.get(session_id)
                if task is asyncio.current_task():
                    self._loop_tasks.pop(session_id, None)
                self._single_loop = next(iter(self._loops.values()), None)
                reviewer = self._session_reviewers.pop(session_id, None)
            if reviewer is not None:
                await reviewer.stop()

    async def _activate_or_discard(self, session: Session) -> Session:
        """激活 session；失败时回执并按未启动路径丢弃后重抛（两条启动入口共用）。"""
        try:
            await self._activate_session(session)
        except (Exception, asyncio.CancelledError) as e:
            run_id = self._unattached_run_id(session)
            await self._discard_unstarted_session(session)
            await self._emit_activation_receipt(
                session,
                error=f"{type(e).__name__}: {e}",
                run_id=run_id,
            )
            raise
        return session

    async def _activate_session(
        self,
        session: Session,
        *,
        prepare: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """Create and start the runtime bound to ``session``.

        Each active session owns its loop task and reviewer service.  The
        registry lock makes duplicate requests idempotent and permits at most
        one runtime for each normalized project directory.
        """
        # Materialize configuration defaults before the runtime starts so
        # global provider/reviewer changes cannot affect this session later.
        await self._ensure_session_snapshots(session)
        await self._validate_session_provider_bindings(session)
        self._refresh_runtime_hooks(session.config.runtime, Path(session.config.cwd))
        # 状态迁移事件接线：钩子随激活安装，session 终身有效（去激活后仍可发射）
        session.set_state_change_hook(self._schedule_session_state_event)
        session.set_outcome_emitter(self._emit_session_outcome_event)

        # Set up session
        if self.auth_rotator is not None:
            session.set_auth_rotator(self.auth_rotator)
        session.set_driver_hooks(self.make_driver_hooks(session.id))
        session.set_provider_change_callback(
            lambda provider_index, model: self._persist_session_provider(
                session,
                provider_index,
                model,
            ),
        )

        async with self._runtime_lock:
            existing = self._loop_tasks.get(session.id)
            if existing is not None and not existing.done():
                return
            if prepare is not None:
                await prepare()
            await self._ensure_session_reviewer(session)
            loop = self._new_loop(session.id)
            evaluator = self._resolve_evaluator(session.config)
            loop.configure_for_session(session, evaluator)
            await loop.take_over(session)
            from workflow_controller.support.tasks import spawn

            task = spawn(
                self._run_session_runtime(session.id, loop),
                name=f"workflow-controller-session-{session.id}",
            )
            self._loops[session.id] = loop
            self._loop_tasks[session.id] = task
            self._single_loop = loop
            # 显式归属锁内落账：激活成功才记账，且不给「已运行不在记录」留窗口
            if session.config.run_id is not None and self._post_attach_hook is not None:
                await self._post_attach_hook(session)
            # 激活回执：本次激活运行时就绪且归属已落账的观测点
            # （并发重复激活不重发；pause 后 play/resume/restart 是重激活，每次都发）
            await self._emit_activation_receipt(session)

        logger.info("Activated independent runtime for session %s", session.id)

    def _schedule_session_state_event(
        self,
        session_id: str,
        old: SessionState,
        new: SessionState,
    ) -> None:
        """状态迁移钩子（同步）：调度总线发射；无事件循环的同步上下文丢弃。"""
        try:
            from workflow_controller.support.tasks import spawn

            spawn(self._emit_session_state_changed(session_id, old, new))
        except RuntimeError:
            pass

    async def _emit_session_state_changed(
        self,
        session_id: str,
        old: SessionState,
        new: SessionState,
    ) -> None:
        await self.bus.emit(_build_session_state_changed_event(session_id, old.value, new.value))

    async def _emit_session_outcome_event(self, session: Session, kind: str) -> None:
        """终态结局发射：payload组装 + 末条 assistant 文本 best-effort 摘要。"""
        await self.bus.emit(
            _build_session_outcome_event(
                session.id,
                kind,
                session.stop_reason,
                session.last_error,
                await self._last_assistant_digest(session.id),
                session.current_iteration,
                self._session_context(session.id),
            )
        )

    async def _last_assistant_digest(self, session_id: str) -> str | None:
        """末条 assistant 文本（结局摘要）；读不到或无内容时 None。"""
        try:
            events = await self.event_router.get_events(session_id)
            return _last_assistant_digest(events)
        except Exception:
            return None

    def _session_context(self, session_id: str) -> str | None:
        """run 归属上下文（归属缓存热路径查找，未铸归属时 None）。"""
        if self._affiliation is None:
            return None
        aff = self._affiliation.resolve(session_id)
        return aff.context if aff is not None else None

    async def _emit_activation_receipt(
        self,
        session: Session,
        *,
        error: str | None = None,
        run_id: str | None = None,
    ) -> None:
        """激活回执：成功=运行时就绪且归属已落账；失败=回滚后补发细因。"""
        try:
            event = _build_activation_receipt_event(session.id, error)
            # create 刻意不收 run_id（维度戳唯一权威来源是写库路径的打戳器，
            # 只补空值）——此直补是唯一例外：失败回执发射前归属缓存已被回滚
            # 驱逐，打戳器无从补，显式 run 的戳只能由调用方带入。
            event.run_id = run_id
            await self.bus.emit(event)
        except Exception:
            logger.exception("激活回执发射失败（session=%s）", session.id)

    async def _attach_on_activate(self, session: Session) -> None:
        """激活成功路径的显式归属：config.run_id 在 → 挂进对应 run。"""
        run_id = session.config.run_id
        if run_id is None:
            return
        try:
            await self.affiliation.attach(session.id, run_id, role=session.config.role)
        except RunNotFoundError as e:
            raise ValueError(f"run_id {run_id} 不在账面，归属被拒") from e

    async def _on_pool_session_created(
        self,
        session: Session,
        config: SessionConfig,
        roster: dict,
    ) -> bool:
        """pool 创建入口 → 归属创建。

        隐式段 mint 与 sessions 表写入单事务落账（返回 True）；显式段不在此写
        （返回 False，sessions 表行由 pool 独立写、归属在激活后挂靠缝落账）。
        """
        return await self.affiliation.on_session_created(session, config, roster=roster)

    async def _persist_session_provider(
        self,
        session: Session,
        provider_index: int,
        model: str,
    ) -> None:
        session.config.provider_index = provider_index
        session.config.model = model
        await self.pool.persist(session)

    async def _ensure_session_snapshots(self, session: Session) -> None:
        from workflow_controller.driver.factory import resolve_runtime

        changed = False
        runtime = resolve_runtime(session.config.runtime or self.config.runtime)
        if (
            session.config.provider_index is None
            and self.auth_rotator is not None
            and runtime != "codex_cli"
        ):
            session.config.provider_index = self.auth_rotator.get_default_provider_index(
                runtime,
            )
            if session.config.provider_index is not None and not session.config.model:
                provider = self.auth_rotator.get_provider(session.config.provider_index)
                session.config.model = provider.model
            changed = True
        if session.config.reviewer is None:
            rc = self.config.reviewer
            reviewer_provider = (
                self.auth_rotator.get_default_provider_index(capability="reviewer")
                if self.auth_rotator is not None
                else None
            )
            session.config.reviewer = SessionReviewerConfig(
                enabled=rc.enabled,
                mode=self._reviewer_mode,
                model=rc.model,
                provider_index=reviewer_provider,
                timeout_seconds=rc.timeout_seconds,
                thinking_mode=rc.thinking_mode,
                thinking_budget_tokens=rc.thinking_budget_tokens,
                task_goal=str((session.config.criteria or {}).get("task_goal") or rc.task_goal),
            )
            changed = True
        elif session.config.reviewer.provider_index is None and self.auth_rotator is not None:
            session.config.reviewer.provider_index = self.auth_rotator.get_default_provider_index(
                capability="reviewer"
            )
            if (
                session.config.reviewer.provider_index is not None
                and not session.config.reviewer.model
            ):
                provider = self.auth_rotator.get_provider(
                    session.config.reviewer.provider_index,
                )
                session.config.reviewer.model = provider.model or ""
            changed = True
        if changed:
            await self.pool.persist(session)

    async def _validate_session_provider_bindings(self, session: Session) -> None:
        from workflow_controller.driver.factory import resolve_runtime

        runtime = resolve_runtime(session.config.runtime or self.config.runtime)
        if session.config.provider_index is None and runtime == "codex_cli":
            raise ValueError(f"worker runtime {runtime} requires a provider")
        rotator = self.auth_rotator
        if rotator is None:
            return
        if session.config.provider_index is None:
            if rotator.get_provider_count() > 0:
                raise ValueError(f"no provider matches worker runtime {runtime}")
        else:
            await rotator.ensure_provider_capability(
                session.config.provider_index,
                "worker",
                runtime=runtime,
                cwd=session.config.cwd or None,
            )
        reviewer = session.config.reviewer
        if reviewer is not None and reviewer.enabled and reviewer.provider_index is not None:
            await rotator.ensure_provider_capability(
                reviewer.provider_index,
                "reviewer",
                cwd=session.config.cwd or None,
            )

    def _unattached_run_id(self, session: Session) -> str | None:
        """失败回执的直补 run 戳：显式 run 激活前未挂靠（缓存无条目），
        回滚驱逐缓存后打戳器无从补；隐式段创建即铸（回滚连 run 一起删），
        不带。须在 discard 前调用（凭缓存区分显式/隐式）。
        """
        return _unattached_run_id(session, self._affiliation)

    async def _discard_unstarted_session(self, session: Session) -> None:
        """Roll back metadata and partial runtime resources after activation fails."""
        loop = self._loops.get(session.id)
        if loop is not None:
            loop.request_stop()
        task = self._loop_tasks.get(session.id)
        if task is not None and task is not asyncio.current_task():
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await task
        self._loops.pop(session.id, None)
        self._loop_tasks.pop(session.id, None)
        self._single_loop = next(iter(self._loops.values()), None)
        reviewer = self._session_reviewers.pop(session.id, None)
        if reviewer is not None:
            with contextlib.suppress(Exception):
                await reviewer.stop()
        self._overlays.clear_subagent_hint(session.id)
        self._overlays.clear_session_instruction(session.id)
        self.pool.remove(session.id)
        # 创建入口铸的归属随 sessions 表行同事务回滚；显式段激活前未落账，discard 幂等。
        # 直查 _affiliation 而非属性：未铸过归属时不触记录（懒建纪律）。
        if self._affiliation is not None:
            try:
                await self._affiliation.discard(session.id, delete_roster=True)
            except Exception:
                logger.exception("run 归属回滚清理失败（session=%s）", session.id)

    async def _ensure_session_reviewer(self, session: Session) -> None:
        snapshot = session.config.reviewer
        if snapshot is None or not snapshot.enabled:
            self._session_reviewers.pop(session.id, None)
            return
        if session.id in self._session_reviewers:
            return
        runtime = _build_reviewer_runtime_config(
            Path(session.config.cwd or self.config.project_dir), snapshot
        )

        async def _persist_reviewer_provider(provider_index: int, model: str) -> None:
            current = session.config.reviewer
            if current is None:
                return
            current.provider_index = provider_index
            current.model = model
            runtime.model = model
            await self.pool.persist(session)

        async def _mark_reviewer_provider_exhausted() -> None:
            session.last_error = "Reviewer has no available provider"
            session.loop_status = "no"
            session.stop_reason = "error"
            await self.pool.persist(session)
            # 直写终态先于引擎翻牌，loop 侧 was_terminal 守卫会抑制其重发——结局由本点独占发射
            await session.emit_outcome(outcome_kind_for_reason(session.stop_reason))
            loop = self._loops.get(session.id)
            if loop is not None:
                loop.request_stop()

        runner = DriverReviewerRunner(
            snapshot.provider_index,
            on_provider_change=_persist_reviewer_provider,
            on_provider_exhausted=_mark_reviewer_provider_exhausted,
        )
        if self.auth_rotator is not None:
            runner.set_auth_rotator(self.auth_rotator)
        reviewer = ReviewerService(
            pool=self.pool,
            event_router=self.event_router,
            config=runtime,
            runner=runner,
        )
        await reviewer.start()
        self._session_reviewers[session.id] = reviewer

    def _resolve_evaluator(self, session_config: SessionConfig) -> Evaluator:
        return _resolve_evaluator(
            session_config,
            self._evaluators,
            self._evaluator_locks,
            self.auth_rotator,
        )

    async def _handle_session_limit(self, event: Event) -> bool:
        """Rotate only the provider binding used by the emitting session."""
        if self.auth_rotator is None:
            return False
        session = self.pool.get(event.session_id)
        if session is None or session.config.provider_index is None:
            return False
        scope, reset_at, error_source = self._limit_rotation_args(event)
        rotator = self.auth_rotator
        replacement = rotator.rotate_bound_provider(
            session.config.provider_index,
            scope=scope,
            reset_at=reset_at,
            runtime=session.config.runtime,
            error=error_source,
        )
        replacement = await self._ensure_replacement_provider(rotator, session, replacement)
        if replacement is None:
            error = "No available provider after rate limit"
            await session.stop(reason="auth_error")
            if session.id in self._loops:
                await self.deactivate_session(session, pause=False)
            session.last_error = error
            session.state = SessionState.FAILED
            session.loop_status = "no"
            session.stop_reason = "error"
            await self.pool.persist(session)
            return True
        session.config.provider_index = replacement
        session.sdk_session_id = None
        provider = rotator.get_provider(replacement)
        if provider.model:
            session.config.model = provider.model
        await self.pool.persist(session)
        loop = self._loops.get(session.id)
        if loop is not None:
            loop.request_auth_switch_now()
        logger.warning(
            "session %s rotated provider to %d after %s",
            session.id,
            replacement,
            scope,
        )
        return True

    async def _ensure_replacement_provider(
        self, rotator: AuthRotator, session: Session, replacement: int | None
    ) -> int | None:
        """校验替换 provider；校验失败按序找替代，直至无可用（返回 None）。"""
        while replacement is not None:
            try:
                await rotator.ensure_provider_capability(
                    replacement,
                    "worker",
                    runtime=session.config.runtime or self.config.runtime,
                    cwd=session.config.cwd or None,
                )
                break
            except ValueError as exc:
                logger.warning(
                    "session %s replacement provider %d failed validation: %s",
                    session.id,
                    replacement,
                    exc,
                )
                replacement = rotator.find_alternative_provider(
                    # 调用方已保证 provider_index 非 None（_handle_session_limit 前置检查）
                    cast("int", session.config.provider_index),
                    runtime=session.config.runtime or self.config.runtime,
                )
        return replacement

    async def _maybe_start_reviewer(self) -> None:
        rc = self.config.reviewer
        if not rc.enabled:
            return
        if os.environ.get("WORKFLOW_CONTROLLER_REVIEWER_DISABLED", "").strip() not in (
            "",
            "0",
            "false",
            "False",
        ):
            logger.info("reviewer disabled via WORKFLOW_CONTROLLER_REVIEWER_DISABLED")
            return

        sandbox_root_str = self.config.project_dir
        sandbox_root = Path(sandbox_root_str)
        try:
            runtime = _build_reviewer_runtime_config(sandbox_root, rc)
            runner = DriverReviewerRunner()
            if self.auth_rotator is not None:
                runner.set_auth_rotator(self.auth_rotator)
            self._reviewer_runner = runner
            self._reviewer = ReviewerService(
                pool=self.pool,
                event_router=self.event_router,
                config=runtime,
                runner=runner,
            )
            await self._reviewer.start()
        except Exception as exc:
            logger.exception("reviewer failed to start: %s", exc)
            self._reviewer = None

    async def _emit_run_attached(self, session_id: str, run_id: str) -> None:
        """新归属落行时的审计事件（幂等 no-op 不重复发）。"""
        await self.event_router.store(_build_run_attached_event(session_id, run_id))

    def _on_native_bound(self, native_session_id: str, session_id: str) -> None:
        """attach 换绑写点的路由映射同步（与 _register_session 同序：列先落、映射随后）。"""
        self._cli_to_workflow_controller_session[native_session_id] = session_id

    async def _emit_session_end(
        self,
        session_id: str,
        end_state: str,
    ) -> None:
        try:
            await self.bus.emit(_build_session_end_event(session_id, end_state))
        except Exception:
            logger.exception("failed to emit session_ended event")

    async def _emit_session_failed(
        self,
        session_id: str,
        reason: str,
        traceback_text: str,
    ) -> None:
        try:
            await self.bus.emit(_build_session_failed_event(session_id, reason, traceback_text))
        except Exception:
            logger.exception("failed to emit session_failed event")

    async def _handle_stop_request(
        self,
        session_id: str,
        data: dict[str, Any],
    ) -> dict[str, Any] | None:
        """CLI hook-server entry: map the CLI uuid, then run the core decision.
        Block count is keyed by the CLI uuid (per-subprocess).
        """
        final_text = data.get("last_assistant_message") or ""
        mig_session_id = await self._resolve_hook_session(session_id)
        if not mig_session_id:
            logger.warning(
                "stop_hook: no Workflow Controller session for CLI uuid %s",
                session_id,
            )
            return None
        return await self._decide_stop(mig_session_id, final_text, block_key=session_id)

    async def _decide_stop(
        self,
        mig_session_id: str,
        final_text: str,
        *,
        block_key: str,
    ) -> dict[str, Any] | None:
        """Reviewer decision for a stop hook. Driver-agnostic.

        Emits a stop_requested control event via the event bus. Plugin
        interceptors (priority 10) and the reviewer interceptor (priority 0)
        respond in order. The first non-pass result determines the outcome.
        """
        if self.reviewer_for(mig_session_id) is None:
            return None

        if not final_text:
            return None

        # Safety net: cap blocks per execution unit. Without this, an LLM that
        # always returns block_with_feedback (e.g. due to a prompt bug) would
        # trap worker in a same-subprocess loop until CC's context window
        # runs out.
        if self._stop_block_limit_reached(block_key):
            return None

        session_obj = self.pool.get(mig_session_id)

        # 角色 gate：非 worker session（交接类 agent、generator 等）
        # 跳过 reviewer——它们有自己的终止条件（如 max_turns），
        # 不应被 reviewer 拦截问"五个阶段走完没有"
        if session_obj is not None and session_obj.config.role not in ("worker", ""):
            logger.info(
                "stop_hook: role=%s session %s — 跳过 reviewer",
                session_obj.config.role,
                mig_session_id[:12],
            )
            return None

        stop_event = _build_stop_requested_event(
            mig_session_id,
            final_text,
            block_key,
            session_obj.config.role if session_obj else "worker",
        )
        stop_result = await self.bus.emit(stop_event)
        if stop_result is not None:
            # Plugin-sourced verdicts carry a non-empty `source` (the plugin
            # name); emit the stop_hook_plugin audit event for them only — the
            # reviewer path emits its own pretool_hook_llm decisions deeper in
            # ReviewerInterceptor._on_stop (core/reviewer.py), so emitting here
            # too would double-log. (Translation + on_let_exit scheduling are
            # the plugin layer's job; framework side-effects live here.)
            if stop_result.source:
                self._emit_plugin_stop_verdict(mig_session_id, stop_result)
            if stop_result.action == "block":
                return {"decision": "block", "reason": stop_result.feedback}
            if stop_result.action == "let_exit":
                # Release worker + force-complete loop (bypass evaluator).
                self.mark_loop_let_exit(mig_session_id)
                return None
            if stop_result.action == "stop":
                return None
        return None

    def _emit_plugin_stop_verdict(self, mig_session_id: str, stop_result: InterceptResult) -> None:
        """插件判定（source 非空）的 stop_hook_plugin 审计事件。"""
        self._hook_decision_emitter.emit_async(
            mig_session_id,
            "stop_hook_plugin",
            {
                "action": stop_result.action,
                "feedback": stop_result.feedback,
                "rationale": stop_result.source,
            },
        )

    def _stop_block_limit_reached(self, block_key: str) -> bool:
        """同子进程 block 次数达上限（LLM 反复 block 的安全网，强制放行）。"""
        prior = self._stop_block_counts.get(block_key, 0)
        if prior >= self._stop_block_limit:
            logger.warning(
                "stop_hook: %s hit block limit %d, force-releasing",
                block_key[:12],
                self._stop_block_limit,
            )
            return True
        return False

    def _codex_hooks_are_managed(self, session_id: str) -> bool:
        """校验当前 session 工作区的 Codex hooks 配置。"""
        session = self.pool.get(session_id)
        if session is None:
            return False
        project_dir = Path(session.config.cwd or self.config.project_dir).resolve()
        return CodexHookConfigWriter().is_managed(project_dir)

    async def _run_plugin_session_complete(self, session_id: str) -> None:
        """Emit the ``session_complete`` event; plugins observe it on the bus."""
        await self.bus.emit(_build_session_complete_event(session_id))

    async def _handle_subagent_start_request(self, session_id: str, data: dict[str, Any]) -> None:
        """路由主线程或嵌套线程的子代理启动事件。"""
        mig_session_id = await self._resolve_hook_session(session_id)
        if mig_session_id:
            data["session_id"] = session_id
            agent_id = str(data.get("agent_id") or "")
            if agent_id:
                self._cli_to_workflow_controller_session[agent_id] = mig_session_id
            await self._subagents.on_start(mig_session_id, data)
            await self._notify_subagent_progress(
                mig_session_id,
                str(data.get("agent_id") or ""),
                started=True,
            )

    async def _handle_subagent_stop_request(self, session_id: str, data: dict[str, Any]) -> None:
        """路由主线程或嵌套线程的子代理结束事件。"""
        mig_session_id = await self._resolve_hook_session(session_id)
        if not mig_session_id:
            return
        await self._notify_subagent_progress(
            mig_session_id,
            str(data.get("agent_id") or ""),
            started=False,
        )
        total_tokens, content = summarize_subagent_transcript(
            data.get("agent_transcript_path", ""),
        )
        data["content"] = content
        data["total_tokens"] = total_tokens
        await self._subagents.on_stop(mig_session_id, data)

    async def _notify_subagent_progress(
        self,
        session_id: str,
        child_thread_id: str,
        *,
        started: bool,
    ) -> None:
        """经会话桥把 Subagent 生命周期对账给当前进度监视器。"""
        session = self.pool.get(session_id)
        hooks = session.driver_hooks if session is not None else None
        bridge = hooks.subagent_progress_bridge if hooks is not None else None
        if bridge is None:
            return
        if started:
            await bridge.notify_started(child_thread_id)
        else:
            await bridge.notify_stopped(child_thread_id)

    def _register_reviewer_interceptors(self) -> None:
        """Construct + register the reviewer control-event interceptor.

        Phase 2.1: the 4 control-event interceptors (stop_requested /
        pre_tool_use / iteration_boundary / gate_question, priority 0)
        live in ``core/reviewer.py:ReviewerInterceptor``. We additionally
        register a priority-5 ``_on_stop_pre`` for the non-reviewer stop
        short-circuit (staged deferral) — an orchestrator control concern,
        not a reviewer judgment.
        """
        self._hook_decision_emitter = HookDecisionEmitter(self.bus)
        self._reviewer_interceptor = ReviewerInterceptor(
            host=self,
            bus=self.bus,
            hook_decision_emitter=self._hook_decision_emitter,
        )
        self._reviewer_interceptor.register()
        # Non-reviewer stop short-circuit: run after plugins (≥10) but
        # before the reviewer (0). Returns non-pass to short-circuit the
        # reviewer's manual/LLM judgment.
        self.bus.intercept("stop_requested", self._on_stop_pre, priority=5)

    async def _on_stop_pre(self, event: Event) -> InterceptResult | None:
        """Priority-5 ``stop_requested`` interceptor owning the staged-session
        deferral: captures ``last_worker_text`` for the boundary reviewer and
        returns ``stop`` so the turn ends.

        Returns ``None`` (pass) when it does not apply → the priority-0
        reviewer interceptor runs.
        """
        mig_session_id = event.session_id
        final_text = event.data.get("final_text", "")
        session_obj = self.pool.get(mig_session_id)

        # Staged session: stop-hook 退化为 defer——捕获 worker 本轮汇报
        # (last_worker_text，给 boundary 的 per-stage reviewer 当 context) +
        # return stop 让 turn 结束（CC kernel 收 stop 结束当前 turn → boundary
        # 接管；opencode 本就忽略 stop 返回值）。**不**跑整体 reviewer——
        # 逐阶段 reviewer 已覆盖语义、k==总 时 boundary 的 stages_complete 信号即完成。
        if session_obj is not None and _session_has_stages(session_obj):
            session_obj.last_worker_text = final_text
            logger.info(
                "stop_hook: staged session %s — defer to boundary, captured worker text (%d chars)",
                mig_session_id[:12],
                len(final_text),
            )
            return InterceptResult(action="stop")

        return None

    async def _run_iteration_boundary_gates(
        self,
        session_id: str,
        session_obj: Session | None,
        iteration: int,
    ) -> IterationGateResult | None:
        """Emit an iteration_boundary control event via the event bus.

        Plugin interceptors (priority 10) and the reviewer interceptor
        (priority 0, wraps resolve_boundary_pending via ReviewerHost) respond in order.
        Returns IterationGateResult or None.
        """
        gate_event = _build_iteration_boundary_event(session_id, iteration)
        result = await self.bus.emit(gate_event)
        if result is not None:
            # Plugin-sourced verdicts carry a non-empty `source` (the plugin
            # name); emit the stop_hook_plugin audit event for them only —
            # the reviewer path emits its own decisions deeper in
            # _run_stage_review_at_boundary (stop_hook_llm for the verdict,
            # stop_hook_plugin for the gate_pass/skip checkpoint), so emitting
            # here too would double-log (same decision 落库两条 log_auto，前端
            # 看到重复行). Mirrors _decide_stop (翻译/副作用分层：副作用层归
            # 框架，但 plugin vs reviewer 的可见事件各自 owns).
            if result.source:
                await self._hook_decision_emitter.emit(
                    session_id,
                    "stop_hook_plugin",
                    {
                        "action": result.action,
                        "feedback": result.feedback,
                        "rationale": result.reason or f"gate:{result.action}",
                    },
                )
            return IterationGateResult(
                action=result.action,  # type: ignore[arg-type]
                feedback=result.feedback,
                reason=result.reason,
            )
        return None

    async def _run_stage_review_at_boundary(
        self,
        session_obj: Session,
        iteration: int,
    ) -> IterationGateResult | None:
        """Per-stage reviewer at the iteration boundary (driver-agnostic)."""
        pending = session_obj.pending_stage_review
        if pending is None:
            return None
        done = pending["done"]
        nxt = pending["next"]
        done_index = pending.get("done_index", -1)
        mig_session_id = session_obj.id
        session_obj.pending_stage_review = None

        # 已审过的阶段不重复审：worker 上次收到推进指令后这一轮还没产出下一阶段产物，
        # disk 上 k 不变，但结构门禁和语义 review 结果仍然有效。直接用上次结果继续推进。
        last_idx = session_obj.last_reviewed_stage_index
        if not isinstance(last_idx, int):
            last_idx = -1
        if done_index <= last_idx:
            return await self._emit_skip_re_review_gate(
                mig_session_id, done, nxt, done_index, last_idx
            )

        session_obj.mark_stage_reviewed(done_index)

        feedback, rationale, reviewer, reviewer_mode = await self._decide_stage_feedback(
            session_obj, done, nxt
        )

        # Per-stage 检查点：先发门禁结构通过（stop_hook_plugin），再发
        # reviewer 语义判定（stop_hook_llm / stop_hook_plugin for manual）。
        # gate_pass 放 review_stage 之后 emit，让两个检查点成对出现在同一
        # timestamp，避免 13s 的 LLM 调用把两者割裂（gate_pass 假早、reviewer
        # 真晚，前端看到脱节的两个事件）。
        await self._emit_stage_gate_checkpoints(
            session_obj,
            done,
            nxt,
            feedback,
            rationale,
            reviewer is not None and reviewer_mode != "manual",
        )
        return IterationGateResult(action="block", feedback=feedback, reason=rationale)

    async def _emit_skip_re_review_gate(
        self,
        mig_session_id: str,
        done: dict[str, Any],
        nxt: dict[str, Any],
        done_index: int,
        last_idx: int,
    ) -> IterationGateResult:
        """已审过的阶段免重审：发 skip 检查点并返回继续推进的 block 结果。"""
        skipped = (
            f"✓「{done['name']}」阶段已审，继续等待产出「{nxt['name']}」阶段"
            f"（产出 {artifacts_display(nxt)}），做完停下汇报，不要连做后面的阶段。"
        )
        logger.info(
            "reviewer: skip re-review for %s (done_index=%d <= last_reviewed=%d)",
            done["name"],
            done_index,
            last_idx,
        )
        # 审查 block 同步 await（非 fire-and-forget），确保它在 gate 返回、
        # loop 进入下一轮发 User Message 之前 emit——否则审查事件会晚于
        # User Message 出现（与下面 advance 路径的 reviewer verdict 对称）。
        await self._hook_decision_emitter.emit(
            mig_session_id,
            "stop_hook_plugin",
            {
                "action": "block",
                "feedback": skipped,
                "rationale": f"stage:{done['name']}->skip",
            },
        )
        return IterationGateResult(
            action="block", feedback=skipped, reason=f"stage:{done['name']}->skip"
        )

    async def _decide_stage_feedback(
        self,
        session_obj: Session,
        done: dict[str, Any],
        nxt: dict[str, Any],
    ) -> tuple[str, str, ReviewerService | None, str]:
        """per-stage 语义判定（manual 模式或无 reviewer 时直通）。

        返回 (feedback, rationale, reviewer, reviewer_mode)；reviewer/mode
        取判定前的读数（与原实现一致，事件命名不受判定期间换挡影响）。
        """
        reviewer = self.reviewer_for(session_obj.id)
        reviewer_mode = self.get_reviewer_mode(session_obj.id)
        if reviewer_mode == "manual" or reviewer is None:
            feedback = (
                f"✓「{done['name']}」阶段通过门禁。现在做「{nxt['name']}」阶段"
                f"（产出 {artifacts_display(nxt)}），做完停下汇报，不要连做后面的阶段。"
            )
            rationale = f"stage:{done['name']}->advance(manual)"
        else:
            decision = await reviewer.review_stage(
                session_obj.last_worker_text,
                done,
                nxt,
            )
            if not decision.feedback:
                feedback = (
                    f"✓「{done['name']}」阶段通过审查。现在做「{nxt['name']}」阶段"
                    f"（产出 {artifacts_display(nxt)}），做完停下汇报，不要连做后面的阶段。"
                )
                rationale = decision.rationale or f"stage:{done['name']}->advance"
            else:
                feedback = decision.feedback
                rationale = decision.rationale or f"stage:{done['name']}->redo"
        return feedback, rationale, reviewer, reviewer_mode

    async def _emit_stage_gate_checkpoints(
        self,
        session_obj: Session,
        done: dict[str, Any],
        nxt: dict[str, Any],
        feedback: str,
        rationale: str,
        llm_reviewed: bool,
    ) -> None:
        """per-stage 检查点对：门禁结构通过 + 语义判定（llm_reviewed 决定事件名）。"""
        mig_session_id = session_obj.id
        await self._hook_decision_emitter.emit(
            mig_session_id,
            "stop_hook_plugin",
            {
                "action": "gate_pass",
                "feedback": "交 reviewer 审语义",
                "rationale": f"门禁 ✓「{done['name']}」结构通过",
            },
        )
        logger.info(
            "reviewer: per-stage %s for %s (next=%s)",
            done["name"],
            mig_session_id[:12],
            nxt["name"],
        )
        # 审查 block 同步 await（非 create_task fire-and-forget），确保它在 gate 返回、
        # loop 进入下一轮发 User Message 之前 emit——否则审查事件会晚于 User Message 出现。
        await self._hook_decision_emitter.emit(
            mig_session_id,
            "stop_hook_llm" if llm_reviewed else "stop_hook_plugin",
            {"action": "block", "feedback": feedback, "rationale": rationale},
        )

    async def _handle_pre_tool_use_request(
        self,
        session_id: str,
        data: dict[str, Any],
    ) -> dict[str, Any] | None:
        """CLI hook-server entry: map the CLI uuid, then run the core decision."""
        mig_session_id = await self._resolve_hook_session(session_id)
        if not mig_session_id:
            logger.warning(
                "pre_tool_use: no Workflow Controller session for CLI uuid %s",
                session_id,
            )
            return None
        return await self._handle_driver_pre_tool_use(mig_session_id, data)

    async def _handle_post_tool_use_request(
        self,
        session_id: str,
        data: dict[str, Any],
    ) -> None:
        mig_session_id = await self._resolve_hook_session(session_id)
        if mig_session_id:
            await self._handle_driver_post_tool_use(mig_session_id, data)

    async def _resolve_hook_session(self, native_or_workflow_controller_id: str) -> str | None:
        """native id → Workflow Controller session（最新绑定唯一持有人）。

        映射与列绑定在 ITERATION_START 同一写点维护（列先落、映射随后
        ——重启后列是唯一权威源，换绑两段间崩溃不把路由回退到旧持有），
        进程内映射命中即快路径——hook 在工具调用关键路径，免去每次存储
        往返；miss 再查列回退（重启后映射为空、或未经本进程写点落列的
        绑定），池直查/线性扫最后回退，同 id 换绑以最新持有为准。
        """
        mapped = self._cli_to_workflow_controller_session.get(native_or_workflow_controller_id)
        if mapped:
            return mapped
        if self._affiliation is not None:
            mapped = await self.run_store.find_session_by_native_id(
                native_or_workflow_controller_id
            )
            if mapped:
                return mapped
        if self.pool.get(native_or_workflow_controller_id) is not None:
            return native_or_workflow_controller_id
        for session in self.pool.list_sessions():
            if session.sdk_session_id == native_or_workflow_controller_id:
                self._cli_to_workflow_controller_session[native_or_workflow_controller_id] = (
                    session.id
                )
                return session.id
        return None

    async def _handle_driver_pre_tool_use(
        self,
        session_id: str,
        data: dict[str, Any],
    ) -> dict[str, Any] | None:
        if self.file_changes.has_subscribers and is_mutating_tool(str(data.get("tool_name", ""))):
            session = self.pool.get(session_id)
            if session is not None:
                tool_use_id = str(data.get("tool_use_id") or uuid4().hex)
                raw_tool_input = data.get("tool_input")
                tool_input = (
                    cast("dict[str, Any]", raw_tool_input)
                    if isinstance(raw_tool_input, dict)
                    else {}
                )
                # run 维度随归属缓存打戳：捕获上下文与并发判定按 run 感知
                affiliation = (
                    self._affiliation.resolve(session_id) if self._affiliation is not None else None
                )
                context = _build_file_change_context(
                    self.config, session, session_id, data, tool_use_id, tool_input, affiliation
                )
                self._file_change_contexts[(session_id, tool_use_id)] = context
                data["tool_use_id"] = tool_use_id
                try:
                    await self.bus.emit(tool_will_execute_event(context))
                except Exception:
                    self._file_change_contexts.pop((session_id, tool_use_id), None)
                    logger.exception("file-change pre capture failed open")
        return await self._decide_pre_tool_use(session_id, data)

    async def _handle_driver_post_tool_use(
        self,
        session_id: str,
        data: dict[str, Any],
    ) -> None:
        tool_use_id = str(data.get("tool_use_id") or "")
        context = self._file_change_contexts.pop((session_id, tool_use_id), None)
        if context is None:
            candidates = [
                key
                for key, value in self._file_change_contexts.items()
                if key[0] == session_id and value.tool_name == data.get("tool_name")
            ]
            if len(candidates) == 1:
                context = self._file_change_contexts.pop(candidates[0])
        if context is None:
            return
        outcome = _build_file_change_outcome(data, context)
        try:
            await self.bus.emit(tool_execution_outcome_event(context, outcome))
        except Exception:
            logger.exception("file-change post capture failed open")

    async def _close_file_change_contexts(self, session_id: str) -> None:
        pending = [
            self._file_change_contexts.pop(key)
            for key in tuple(self._file_change_contexts)
            if key[0] == session_id
        ]
        for context in pending:
            try:
                outcome = _build_incomplete_file_change_outcome(context)
                await self.bus.emit(tool_execution_outcome_event(context, outcome))
            except Exception:
                logger.exception("file-change session cleanup failed open")

    async def _decide_pre_tool_use(
        self,
        mig_session_id: str,
        data: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Reviewer decision for an AskUserQuestion pre-tool-use hook.

        Emits a pre_tool_use control event via the event bus. Plugin
        interceptors (priority 10) and the reviewer interceptor (priority 0)
        respond in order. Returns a CC-style hook output dict or None.
        """
        if data.get("tool_name") != "AskUserQuestion":
            return None

        if self.reviewer_for(mig_session_id) is None:
            return None

        pre_event = _build_pre_tool_use_event(mig_session_id, data)
        result = await self.bus.emit(pre_event)
        if result is not None and result.action == "block":
            return _build_pre_tool_use_denial(result.feedback)
        return None

    async def _resolve_gate_question(
        self,
        workflow_controller_session_id: str,
        tool_input: dict[str, Any],
    ) -> str:
        """opencode gate entry — the ``on_gate_question`` callback.

        Emits a gate_question control event via the event bus. The reviewer
        interceptor (priority 0) resolves the answer (auto: reviewer AUQ,
        manual: await human). Returns the answer string.
        """
        gate_event = _build_gate_question_event(workflow_controller_session_id, tool_input)
        result = await self.bus.emit(gate_event)
        if result is not None:
            return result.feedback
        return ""

    async def _register_session(self, event: Event) -> None:
        data = event.data
        cli_id = data.get("session_id")
        if not cli_id or not event.session_id:
            return
        session = self.pool.get(event.session_id)
        # ITERATION_START（cli 的 init / sdk 的 task_started / opencode 的
        # id_ready ）首次带 session id 时存下，供 pause→resume 续 session 用。
        if session and not session.sdk_session_id:
            session.sdk_session_id = cli_id
            # Persist immediately so it survives restarts
            await self.pool.persist(session)
            logger.info(
                "Captured sdk_session_id %s for session %s", cli_id[:12], event.session_id[:12]
            )
        # native id 记账（hook 路由权威源）：每次绑定都重记，最新持有唯一。
        # 列先落、映射随后——列是重启后的唯一权威源，换绑两段间崩溃不会
        # 把重启路由回退到旧持有人
        if self._affiliation is not None:
            await self._affiliation.set_native_session_id(event.session_id, cli_id)
        self._cli_to_workflow_controller_session[cli_id] = event.session_id

    def _get_session_config(self, name: str) -> SessionConfig:
        return _get_session_config(name, self.config.sessions, self.pool.get_config)

    def _init_event_plane(self, project_dir: Path) -> None:
        """事件面组装：event_router / pool / bus 与相互接线（__init__ 专用）。"""
        self.event_router = EventStoreRouter(
            default_project_dir=project_dir,
        )
        # 事件打戳：写库前按归属缓存补 run 维度（直查 _affiliation，不触懒建）
        self.event_router.set_run_stamper(
            lambda sid: self._affiliation.resolve(sid) if self._affiliation is not None else None
        )
        self.pool = SessionPool()
        # 反向注入：让 pool._persist_session 能调到 event_router.store_session
        # 落盘到 events.db 的 sessions 表。漏调这条会导致 _persist_session 一直
        # return None（_event_router is None），events.db sessions 表没人写，
        # 进程被 kill 时丢 sdk_session_id / result 等关键状态。
        self.pool.set_event_router(self.event_router)
        # session 创建入口接线：一切创建路径（含 import/重建旁路）都过此生成归属
        self.pool.set_affiliation_hook(self._on_pool_session_created)
        self.bus = EventBus(
            store_router=self.event_router,
            type_registry=EventTypeRegistry(),
        )

    def _init_runtime_slots(self) -> None:
        """运行时槽位初始化：评估器/loop 注册表/评审器句柄等（__init__ 专用）。"""
        self._evaluators = {}
        self._evaluator_locks = {}
        # One engine/task per running session. _single_loop is a transitional
        # alias for older extensions that only ever observe one runtime.
        self._loops = {}
        self._loop_tasks = {}
        # session 平面重启窗（stop→重激活）：窗口内终态为瞬态，收尾判定据此短路
        self._restarting_sessions = set()
        self._runtime_lock = asyncio.Lock()
        self._single_loop = None
        self._hook_server = None
        self._reviewer = None
        self._session_reviewers = {}
        # run 平面关停步骤位：pool 停止前先让 run 任务收尾（组合根登记）
        self._shutdown_steps = []
        # Phase 2.1 — reviewer control-event interceptor + decision emitter
        # （定义见 core/reviewer.py）。二者在 start() 阶段由
        # _register_reviewer_interceptors 构造；这里先置 None，保证
        # start() 未跑过时 shutdown() 也安全。
        self._reviewer_interceptor = None
        self._hook_decision_emitter = None
        self._cli_to_workflow_controller_session = {}

    def _init_plane_integrations(self) -> None:
        """平面集成组件初始化：watchdog/subagent/overlay/文件变更（__init__ 专用）。"""
        # watchdog（subagent 异常检测通用能力，规则以事件库订阅者接入）
        self.watchdog = WatchdogCore(
            self.config.watchdog,
            self.bus,
            self.pool,
        )
        # In-flight sub-agent tracking + SUBAGENT_* event emission + precise
        # WAITING_RETRY wake (dc370e6 overlay 剥离相同手法抽出).
        self._subagents = SubagentTracker(
            self.bus,
            self.pool,
            lambda sid: self._loops.get(sid) if self._loops else self._single_loop,
        )
        # Per-session/project prompt overlay registry (opencode-only; SDK/CLI get
        # empty via DriverHooks.get_overlays). Public methods below delegate to it.
        self._overlays = OverlayRegistry()
        # "auto" → reviewer auto-replies, Stop hook injects reminder.
        # "manual" → reviewer paused, Stop hook returns None (lets CC end),
        #            PreToolUse still denies but with manual-reply reason.
        # Toggled at runtime via set_reviewer_mode() (e.g. from the Web UI).
        self._reviewer_mode = "auto"
        # Per-CC-session block counter for Stop hook. Safety net only:
        # decide_stop calls LLM and trusts its action, but if the LLM
        # repeatedly returns block_with_feedback (e.g. due to a buggy prompt
        # or worker truly stuck), this cap prevents an infinite same-subprocess
        # loop. 200 is generous for a healthy 5-stage pipeline with retries.
        self._stop_block_counts = {}
        self._stop_block_limit = 200
        self._plugin_registry: PluginRegistry = PluginRegistry()
        self.file_changes = FileChangeRegistry()
        self.file_changes.bind(self.bus)

    async def _emit_session_auto_event(
        self,
        session_id: str,
        action: str,
        config: SessionConfig,
        resume_sdk_session_id: str,
    ) -> None:
        """session 出生审计事件（start_action 与 resume 标识参数化）。"""
        await self.event_router.store(
            _build_session_auto_event(session_id, action, config, resume_sdk_session_id)
        )

    async def _await_loop_task_registration(
        self,
        session: Session,
        session_id: str,
        deadline: float | None,
        timeout: float | None,
    ) -> asyncio.Task[None] | bool | None:
        """等 runtime task 注册（resume 窗口防护）。

        返回 task（含 None：有界轮询耗尽仍未注册）交调用方继续；返回
        bool 表示已有提前结论（真停止=已 drain，deadline 到点=未 drain）。
        """
        budget = 150 if timeout is not None else 600
        polled = 0
        while True:
            task = self._loop_tasks.get(session_id)
            if task is not None:
                return task
            if session.loop_status != "yes":
                # 真停止（非 resume 窗口），无 runtime = 已 drain（含直接
                # stop 的 session：loop_status=no 且从未注册过 task）。
                return True
            if deadline is not None:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return False
                await asyncio.sleep(min(0.1, remaining))
                continue
            if polled >= budget:
                return None
            await asyncio.sleep(0.1)
            polled += 1

    async def _shield_wait_outcome(self, session: Session, task: asyncio.Task[None]) -> bool:
        """无预算等待：shield 依附 task；取消/异常按 loop_status 下结论。"""
        try:
            await asyncio.shield(task)
            return True
        except (asyncio.CancelledError, Exception):
            return session.loop_status != "yes"

    async def _wait_with_active_budget(
        self,
        session_id: str,
        task: asyncio.Task[None],
        timeout: float,
        interval: float,
    ) -> bool:
        """按实际运行时长核对预算的等待：每轮先查余额再等待（暂停不计时）。

        已用时长经 active_elapsed() 实时含进行中运行段（挂死的 query 没人
        settle，只看累计值会让预算永不耗尽）。暂停期间 active_seconds 冻结
        （loop 记账），余额不减、继续等。
        """
        while True:
            if task.done():
                return True
            s = self.pool.get(session_id)
            if s is None:
                return False
            if s.state.value == "idle" and s.stop_reason == "user_pause":
                wait = interval  # 暂停中：不计时，等恢复
            else:
                budget_left = timeout - s.active_elapsed()
                if budget_left <= 0:
                    return False
                wait = min(interval, budget_left)
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=wait)
                return True
            except TimeoutError:
                if task.done():
                    return True
