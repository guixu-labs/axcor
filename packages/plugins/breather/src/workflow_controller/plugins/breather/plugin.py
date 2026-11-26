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
import shutil
import time
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from aiohttp import web

from workflow_controller.plugin.sdk import IterationGateResult, Plugin, StopHookResult
from workflow_controller.plugins.breather.context import ContextTracker
from workflow_controller.plugins.breather.group_handoff import GroupHandoff
from workflow_controller.plugins.breather.handoff import HandoffGenerator
from workflow_controller.session.events import EventType, outcome_kind_for_reason
from workflow_controller.session.session import SessionState
from workflow_controller.support.tasks import spawn

if TYPE_CHECKING:
    from workflow_controller.hooks.server import HookServer
    from workflow_controller.plugin.base import PluginConfig
    from workflow_controller.plugin.context import PluginContext, PluginServices

logger = logging.getLogger(__name__)


@dataclass
class _HandoffRuntimeState:
    pending: bool = False
    started_at: float = 0.0
    session_id: str = ""
    finalizing: bool = False
    stop_armed: bool = False
    force_pending: bool = False
    captured_final_text: str = ""
    watchdog_task: asyncio.Task[None] | None = None
    completed_event: asyncio.Event | None = None


@dataclass
class _HandoffPlan:
    """finalize_handoff 本轮的命名与 prompt 计划（一次算好、全程复用）。"""

    gen: HandoffGenerator
    handoff_count: int
    handoff_doc_name: str
    done_file_name: str
    agent_prompt: str
    agent_session_name: str
    continuation_name: str


class BreatherPlugin(Plugin):
    def __init__(self, config: PluginConfig, config_path: Path | None) -> None:
        super().__init__(config, config_path)
        self._installed = False
        self._hook_server: HookServer | None = None
        self._handoff_states: dict[str, _HandoffRuntimeState] = {}
        self._group_handoff = GroupHandoff(self)
        self._handoff_context: ContextVar[str] = ContextVar(
            "breather_session_id",
            default="__global__",
        )
        self._handoff_generator = HandoffGenerator(
            skill=self._cfg("handoff_skill", "operator-handoff"),
            handoff_filename=self._cfg("handoff_filename", "handoff.md"),
            handoff_prompt_path=self._cfg("handoff_prompt_path", None),
            continuation_prompt_path=self._cfg("continuation_prompt_path", None),
        )

    @property
    def services(self) -> PluginServices:
        """宿主能力端口的公开只读入口（group_handoff 等协作组件使用）。"""
        return self._services

    @property
    def ctx(self) -> PluginContext:
        """宿主上下文（事件总线等）的公开只读入口。"""
        return self._ctx

    @property
    def installed(self) -> bool:
        """生命周期标志：install 之后、uninstall 之前为 True。"""
        return self._installed

    @property
    def _services(self) -> PluginServices:
        """宿主能力端口（install 注入的 ctx 派生）。"""
        return self._ctx.services

    @property
    def _handoff_pending(self) -> bool:
        return self._state_for().pending

    @_handoff_pending.setter
    def _handoff_pending(self, value: bool) -> None:
        self._state_for().pending = value

    @property
    def _pending_handoff_started_at(self) -> float:
        return self._state_for().started_at

    @_pending_handoff_started_at.setter
    def _pending_handoff_started_at(self, value: float) -> None:
        self._state_for().started_at = value

    @property
    def _pending_handoff_session_id(self) -> str:
        return self._state_for().session_id

    @_pending_handoff_session_id.setter
    def _pending_handoff_session_id(self, value: str) -> None:
        self._state_for().session_id = value

    @property
    def _handoff_finalizing(self) -> bool:
        return self._state_for().finalizing

    @_handoff_finalizing.setter
    def _handoff_finalizing(self, value: bool) -> None:
        self._state_for().finalizing = value

    @property
    def _handoff_stop_armed(self) -> bool:
        return self._state_for().stop_armed

    @_handoff_stop_armed.setter
    def _handoff_stop_armed(self, value: bool) -> None:
        self._state_for().stop_armed = value

    @property
    def _handoff_force_pending(self) -> bool:
        return self._state_for().force_pending

    @_handoff_force_pending.setter
    def _handoff_force_pending(self, value: bool) -> None:
        self._state_for().force_pending = value

    @property
    def _captured_final_text(self) -> str:
        return self._state_for().captured_final_text

    @_captured_final_text.setter
    def _captured_final_text(self, value: str) -> None:
        self._state_for().captured_final_text = value

    @property
    def _handoff_watchdog_task(self) -> asyncio.Task[None] | None:
        return self._state_for().watchdog_task

    @_handoff_watchdog_task.setter
    def _handoff_watchdog_task(self, value: asyncio.Task[None] | None) -> None:
        self._state_for().watchdog_task = value

    @property
    def _handoff_completed_event(self) -> asyncio.Event | None:
        return self._state_for().completed_event

    @_handoff_completed_event.setter
    def _handoff_completed_event(self, value: asyncio.Event | None) -> None:
        self._state_for().completed_event = value

    @classmethod
    def builtin_instruction_files(cls) -> dict[str, tuple[str, str]]:
        from workflow_controller.plugins.breather.group_handoff import GROUP_HANDOFF_PROMPT
        from workflow_controller.plugins.breather.handoff import HANDOFF_AGENT_PROMPT, RESUME_PROMPT

        return {
            "handoff": (
                "workflow-handoff-instructions.md",
                "<!--\n"
                "编辑说明：在这一份文件中分别写明主代理和子代理的交接方式；保留下面的分段标记。\n"
                "workflow:main：主代理如何交接自己的工作，以及交接后如何重新派发原本运行中的子代理；"
                "子代理阈值设为 100% 时无需重新派发说明。\n"
                "workflow:group：每名子代理如何交接自己的工作。\n"
                "可选 workflow:common 段写两者共用的要求；主代理和子代理同时交接时，两段都会应用。\n"
                "交接文档名称由系统按会话 ID 和轮次生成，无需在指令中指定。\n"
                "-->\n\n"
                "<!-- workflow:main -->\n"
                + HANDOFF_AGENT_PROMPT.strip()
                + "\n\n<!-- workflow:group -->\n"
                + GROUP_HANDOFF_PROMPT
                + "\n",
            ),
            "continuation": ("workflow-continuation-instructions.md", RESUME_PROMPT),
        }

    def manages_context_window(self) -> bool:
        return True

    def get_handoff_state(self, session_id: str | None = None) -> dict[str, Any]:
        """Return current handoff state for SSE updates."""
        state = self._state_for(session_id)
        active_subagents: list[dict[str, str]] = []
        if self._installed and state.session_id:
            for agent_id, info in self._services.in_flight_subagents(state.session_id).items():
                active_subagents.append(
                    {
                        "agent_id": agent_id,
                        "agent_type": info.get("agent_type", "unknown"),
                    }
                )
        return {
            "pending": state.pending,
            "finalizing": state.finalizing,
            "stop_armed": state.stop_armed,
            "force_pending": state.force_pending,
            "pending_session_id": state.session_id,
            "active_subagents": active_subagents,
        }

    def get_handoff_states(self) -> dict[str, dict[str, Any]]:
        return {
            sid: self.get_handoff_state(sid) for sid in self._handoff_states if sid != "__global__"
        }

    def install(self, ctx: PluginContext) -> None:
        """Single lifecycle entry: state init + hook routes.

        ``super().install(ctx)`` captures ``_ctx`` and dispatches
        :meth:`_register_interceptors` / :meth:`_register_observers`
        (bus interceptors + bridge observers). Then setup: store refs,
        register the ``/breather`` hook route.
        """
        super().install(ctx)
        self._hook_server = ctx.hook_server
        self.register_hook_routes(
            ctx.hook_server,
            [
                ("POST", "/breather", self._handle_breather),
            ],
        )
        self._installed = True
        logger.info("breather plugin installed")

    def uninstall(self) -> None:
        self._group_handoff.close()
        for state in self._handoff_states.values():
            if state.watchdog_task is not None and not state.watchdog_task.done():
                state.watchdog_task.cancel()
            state.watchdog_task = None
            state.completed_event = None
        self._installed = False
        self._hook_server = None
        logger.info("breather plugin uninstalled")

    async def on_iteration_boundary(
        self,
        session_id: str,
        session: Any,
        iteration: int,
    ) -> IterationGateResult:
        """Driver-agnostic loop 边界门禁：worker 自然 EOF 后由 loop 每轮调一次。
        超上下文阈值 → 发起 handoff，返回 stop（loop 结束本 worker）；否则 None。
        经宿主的 iteration 边界派发器到达，不按插件名硬编码寻址。
        """
        if not self._installed:
            return IterationGateResult(action="pass")
        if getattr(session, "pause_requested", False):
            return IterationGateResult(action="pass")
        # Skip non-worker sessions (handoff_agent, etc.) — they are short-lived
        # and should never be handed off themselves.
        if getattr(session, "role", "worker") != "worker":
            return IterationGateResult(action="pass")
        self._select_state(session.id)
        # 粘性：主动停在 subagent 返回点（轮内干净 turn 边界）触发的 handoff，
        # on_iteration_boundary 见 _handoff_stop_armed 即跳过 _check_context 直接
        # finalize——决策一次，边界不 re-check disagree。
        if self._handoff_stop_armed:
            self._handoff_finalizing = True
            if not self._captured_final_text:
                self._captured_final_text = self._extract_last_assistant_text(session.id)
            self._pending_handoff_session_id = session.id
            logger.info(
                "iteration_boundary: armed（主动停在 subagent 返回点），直接 finalize %s",
                session.name,
            )
            spawn(self.finalize_handoff(session))
            return IterationGateResult(action="stop", reason="breather_handoff")
        forced = self._boundary_force_pending(session)
        if forced is not None:
            return forced
        if self._handoff_pending:
            if self._handoff_finalizing or self._services.in_flight_subagents(session.id):
                return IterationGateResult(action="pass")
            self._handoff_finalizing = True
            if not self._captured_final_text:
                self._captured_final_text = self._extract_last_assistant_text(session.id)
            spawn(self.finalize_handoff(session))
            return IterationGateResult(action="stop", reason="breather_handoff")
        return await self._boundary_threshold_handoff(session)

    async def request_handoff(
        self,
        session_id: str,
        force: bool,
    ) -> dict[str, Any]:
        """handoff 请求核心逻辑，format-agnostic（返回 dict）。

        两条入口共享：aiohttp hook 路由 _handle_breather、浏览器端点 _web_handoff。
        force=True 跳过阈值门（让 skill/UI 在语义交接点主动切片）；沿用 MigBot 等
        stop 模式——只设 pending、不起子进程，等 worker 自然 stop 触发 finalize。
        """
        if not self._installed:
            return {"handoff_triggered": False, "reason": "plugin not ready"}

        worker = self._resolve_session(session_id)
        if worker is None:
            logger.info("breather: no active session for handoff request")
            return {"handoff_triggered": False, "reason": "no active session"}
        if worker.role != "worker":
            return {"handoff_triggered": False, "reason": "handoff only applies to worker sessions"}
        self._select_state(worker.id)

        if self._handoff_force_pending or self._handoff_stop_armed or self._handoff_finalizing:
            return {"handoff_triggered": False, "reason": "handoff already pending"}

        if not force:
            rejected, result = await self._handoff_usage_gate(worker)
            if rejected is not None:
                return rejected

        self._handoff_force_pending = True
        self._pending_handoff_session_id = worker.id
        self._pending_handoff_started_at = time.time()
        self._start_handoff_watchdog(worker)

        # force → 等下个 iteration boundary
        if force:
            logger.info("breather: force handoff for %s", worker.name)
            return {"handoff_triggered": True, "phase": "pending"}

        # 阈值满 → 等下个 subagent 返回点
        logger.info("breather: 阈值超 for %s (%.1f%%)", worker.name, result.percentage)
        return {
            "handoff_triggered": True,
            "phase": "pending",
            "context": {"percentage": result.percentage},
        }

    def get_web_router(self) -> Any:
        """暴露浏览器端点 /api/breather/{handoff,state,cancel}，由 Console 装配器挂载。"""
        from fastapi import APIRouter

        router = APIRouter()
        router.add_api_route(
            "/api/breather/handoff",
            self._web_handoff,
            methods=["POST"],
        )
        router.add_api_route(
            "/api/breather/state",
            self._web_state,
            methods=["GET"],
        )
        router.add_api_route(
            "/api/breather/cancel",
            self._web_cancel,
            methods=["POST"],
        )
        return router

    def on_stop_hook(
        self, session_id: str, _session: Any, final_text: str = ""
    ) -> StopHookResult | None:
        """读取实时快照；超阈值或已 pending 时捕获文本并交接。

        Worker 不写 SESSION-HANDOFF.md。on_stop_hook 只捕获 final_text
        然后 let_exit，回调 finalize_handoff 起 handoff agent 写文件。
        """
        self._select_state(session_id)
        if getattr(_session, "pause_requested", False):
            return None
        handoff_eligible = (
            not self._handoff_pending
            and self._installed
            and getattr(_session, "role", "worker") == "worker"
            and not self._services.in_flight_subagents(session_id)
        )
        if handoff_eligible:
            # Stop hook 在最终 result 之前到达，只读已记录快照，不 fork 活跃会话。
            result = self._make_tracker(_session).check_recorded()
            if (
                self._threshold_pct(_session) < 100
                and result.percentage is not None
                and result.percentage > self._threshold_pct(_session)
            ):
                self._handoff_pending = True
                self._pending_handoff_session_id = session_id
                self._pending_handoff_started_at = time.time()
        if not self._handoff_pending:
            return None
        if session_id != self._pending_handoff_session_id:
            return None
        if not self._installed:
            return None
        # Re-entrancy guard (see _handoff_finalizing init). A worker can fire
        # the Stop hook twice in one handoff round; only the first arms
        # finalize_handoff. Without this, the second arms a duplicate handoff
        # agent that races the first.
        if self._handoff_finalizing:
            logger.info(
                "stop_hook: handoff for %s 已在 finalize 中，忽略重复 stop hook",
                session_id[:12],
            )
            return None
        self._handoff_finalizing = True

        self._captured_final_text = final_text
        logger.info(
            "stop_hook: 捕获 final_text for %s（%d 字）",
            session_id[:12],
            len(final_text),
        )

        return StopHookResult(
            action="let_exit",
            on_let_exit=self.finalize_handoff,
        )

    async def finalize_handoff(self, session: Any) -> None:
        """v2: 起 handoff agent → 成功才停旧 worker → 起 worker-h2。

        Handoff agent 是独立 session（role="handoff_agent"），干净上下文。
        用确定性工具 + 模板 prompt 写 SESSION-HANDOFF.md。
        旧 worker 在文件写入前不被停止——失败时它是天然的降级。
        """
        services = self._services
        self._select_state(session.id)
        logger.info("finalize_handoff: START for session %s (state=%s)", session.id, session.state)

        # Await the bound runtime task instead of polling SessionState. A direct
        # stop legitimately leaves STOPPED, which is also safe once no runtime
        # remains.
        if not await services.wait_session_done(session.id, wait_timeout=10.0):
            logger.error(
                "finalize_handoff: timeout waiting for worker %s runtime (state=%s)",
                session.id,
                session.state,
            )
            self.reset_handoff_state(session.id)
            return

        logger.info("finalize_handoff: worker %s is idle, proceeding", session.id)

        # 阶段二开始：worker 已停（stop hook 已到），phase-1 watchdog 退场，
        # 后续超时全交给本函数的 wait_session_done 独管，避免两个计时器重叠。
        self._disarm_watchdog()

        try:
            # 1. 部署确定性工具脚本到工程 .workflow-controller/，供 handoff agent 用
            #    Bash 调用（handoff agent 的 cwd 是目标工程目录，
            #    PYTHONPATH 上没有 workflow_controller 包，所以工具必须是自包含脚本）。
            cwd = session.config.cwd or "."
            logger.info("finalize_handoff: deploying tools to %s", cwd)
            self._deploy_handoff_tools(cwd)

            # 2. 用 captured final_text + 项目目录构建 agent prompt。
            #    完整指令写进 .workflow-controller/handoff-agent-prompt.md，返回的 prompt
            #    只是一句短引用，避开 cmd.exe 8191 字符命令行长度限制。
            # 3. 配置并启动 handoff agent session
            plan = self._plan_handoff_docs(session, cwd)
            agent_session = await self._start_handoff_agent(session, cwd, plan)
            spawn(self._update_title_with_retry(agent_session, plan.agent_session_name))

            # 2b. 等 handoff agent 真正跑完再检查产物（超时/被暂停按失败收尾）。
            if not await self._await_handoff_agent(session, agent_session):
                return  # worker 未停——天然降级

            # 3. 校验 handoff agent 在本轮真正（重新）写了文件。
            handoff_file = self._fresh_handoff_files(session, agent_session, plan)
            if handoff_file is None:
                await self._fail_session(agent_session, "error")
                return  # worker 未停——天然降级

            # 4. 文件就位 → 先起 worker-h2（成功才停旧，避免 phase-2 失败断档：
            #    旧已停 + 新没起 = 接力断档）。起 h2 失败则 return，旧 worker 留着天然降级。
            new_session = await self._continue_or_degrade(
                session, agent_session, plan, handoff_file
            )
            if new_session is None:
                return

            # 5. 新 worker 已创建 → 停旧 worker 并把原 worker 恢复为 loop 可复用态。
            await self._finish_handoff(session, new_session, handoff_file)
        except Exception:
            logger.exception("finalize_handoff v2 失败 — worker 继续")
        finally:
            self.reset_handoff_state(session.id)

    async def execute_handoff(
        self,
        worker_session_id: str,
        handoff_session_config: Any,
        new_worker_config: Any,
    ) -> dict[str, Any]:
        """程序化交接：worker → 交接 session → 新 worker，全程经编排端口组合
        宿主的 session 原语（启停/等待/落盘），不触碰宿主私有成员。

        1. 停 worker 并按 handoff_triggered 落账 IDLE
        2. 起交接 session 并等待其结束
        3. 成功则起新 worker；失败则保留交接 session 供人工介入
        """
        services = self._services
        worker = services.find_session(worker_session_id)
        if worker is None:
            return {"success": False, "error": "Worker session not found"}

        # 谱系继承：交接/接续成员入 worker 的 run（worker 无归属则留给创建入口铸）
        for lineage_config in (handoff_session_config, new_worker_config):
            if lineage_config.run_id is None:
                lineage_config.run_id = worker.run_id

        await self._park_worker_for_handoff(worker)
        handoff_session = await self._start_linked_session(handoff_session_config, worker.id)

        timeout = 300.0  # 交接 session 的等待上限
        if not await services.wait_session_done(handoff_session.id, timeout=timeout):
            logger.warning("handoff: timeout waiting for session %s", handoff_session.id)
            return await self._abort_handoff_session(
                handoff_session,
                "timeout",
                "Timeout waiting for handoff session",
                report_id=False,
            )

        if handoff_session.result != "success":
            return await self._abort_handoff_session(
                handoff_session,
                "error",
                "Handoff session failed",
                report_id=True,
            )

        new_worker = await self._start_linked_session(new_worker_config, handoff_session.id)
        return {
            "success": True,
            "handoff_session_id": handoff_session.id,
            "new_worker_id": new_worker.id,
        }

    def reset_handoff_state(self, session_id: str) -> None:
        key = session_id
        state = self._handoff_states.get(key)
        if state is None:
            return
        state.pending = False
        state.started_at = 0.0
        state.session_id = ""
        state.finalizing = False
        state.stop_armed = False
        state.force_pending = False
        state.captured_final_text = ""
        # Wake any running watchdog so it exits cleanly instead of burning
        # the rest of its timeout. set() is idempotent if already set.
        if state.completed_event is not None:
            state.completed_event.set()
        watchdog = state.watchdog_task
        if watchdog is not None and watchdog is not asyncio.current_task() and not watchdog.done():
            watchdog.cancel()
        self._handoff_states.pop(key, None)
        if self._handoff_context.get() == key:
            self._handoff_context.set("__global__")

    def apply_config(self) -> None:
        """同步配置页面刚保存的默认模板路径。"""
        self._handoff_generator.handoff_prompt_path = self._cfg("handoff_prompt_path", None)
        self._handoff_generator.continuation_prompt_path = self._cfg(
            "continuation_prompt_path", None
        )

    def state_for(self, session_id: str | None = None) -> _HandoffRuntimeState:
        """handoff 运行态的公开读取入口（contextvar 或显式 session_id）。"""
        return self._state_for(session_id)

    def cfg(self, key: str, default: Any = None) -> Any:
        """插件 config extra 读取的公开入口。"""
        return self._cfg(key, default)

    def context_window(self, session: Any) -> int | None:
        """session 覆盖或全局配置的上下文窗口大小。"""
        return self._context_window(session)

    def threshold_pct(self, session: Any | None = None) -> float:
        """主代理上下文阈值百分比。"""
        return self._threshold_pct(session)

    def subagent_threshold_pct(self, session: Any) -> float:
        """子代理上下文阈值百分比。"""
        return self._subagent_threshold_pct(session)

    def check_context_from_events(self, session: Any) -> Any:
        """按已记录事件快照检查上下文用量（不起实时探测）。"""
        return self._check_context_from_events(session)

    def effective_generator(self, session: Any) -> HandoffGenerator:
        """按 session 级模板覆盖解析出的交接文档生成器。"""
        return self._effective_generator(session)

    def _state_for(self, session_id: str | None = None) -> _HandoffRuntimeState:
        key = session_id or self._handoff_context.get()
        return self._handoff_states.setdefault(
            key, _HandoffRuntimeState(session_id=key if key != "__global__" else "")
        )

    def _select_state(self, session_id: str) -> None:
        if session_id not in self._handoff_states:
            legacy = self._handoff_states.get("__global__")
            if legacy is not None and any(
                (
                    legacy.pending,
                    legacy.finalizing,
                    legacy.stop_armed,
                    legacy.force_pending,
                    bool(legacy.captured_final_text),
                )
            ):
                legacy.session_id = session_id
                self._handoff_states[session_id] = legacy
                self._handoff_states.pop("__global__", None)
        self._handoff_context.set(session_id)
        self._state_for(session_id)

    def _effective_generator(self, session: Any) -> HandoffGenerator:
        """Return a HandoffGenerator honoring per-project prompt overrides.

        The default generator (self._handoff_generator) carries the paths from
        the GLOBAL breather config.yaml. A worker session may declare its own
        handoff_prompt_path / continuation_prompt_path via the project's
        workflow-controller.yaml (flows through SessionConfig). When either is set we build a
        session-scoped generator so this project's handoff uses its own
        templates; otherwise we reuse the shared default.
        """
        cfg = getattr(session, "config", None)
        sess_handoff = getattr(cfg, "handoff_prompt_path", None) if cfg else None
        sess_cont = getattr(cfg, "continuation_prompt_path", None) if cfg else None
        if not sess_handoff and not sess_cont:
            return self._handoff_generator
        return HandoffGenerator(
            skill=self._cfg("handoff_skill", "operator-handoff"),
            handoff_filename=self._cfg("handoff_filename", "handoff.md"),
            handoff_prompt_path=sess_handoff or self._cfg("handoff_prompt_path", None),
            continuation_prompt_path=sess_cont or self._cfg("continuation_prompt_path", None),
        )

    def _register_interceptors(self, ctx: Any) -> None:
        """Declare the control events breather intercepts.

        stop_requested: on_stop_hook captures worker final_text + triggers
        finalize_handoff via let_exit. iteration_boundary: on_iteration_boundary
        checks context threshold / armed handoff at the loop boundary. Both run
        at priority 10 (before the reviewer, priority 0). The base class
        translation helpers (_on_stop_intercept / _on_iteration_boundary_intercept)
        adapt breather's StopHookResult / IterationGateResult into InterceptResult.
        """
        ctx.bus.intercept("stop_requested", self._on_stop_intercept, priority=10)
        ctx.bus.intercept(
            "iteration_boundary",
            self._on_iteration_boundary_intercept,
            priority=10,
        )

    def _register_observers(self, ctx: Any) -> None:
        """Subscribe breather's typed observers.

        - ``_on_context_event``: 类型化订阅 SUBAGENT_STOP，在全部子代理结束后
          检查主代理用量并标记待交接；主代理处理完工具结果后才实际停机。
        - ``_on_notify_skill_hook``: 类型化订阅 TOOL_RESULT，仅当配置了
          ``notify_skill`` 时注册，按 ``tool_name`` 匹配。
        """
        ctx.bus.observe(EventType.SUBAGENT_STOP, self._on_context_event)
        ctx.bus.observe(EventType.SUBAGENT_START, self._on_subagent_start)
        notify_skill = self._cfg("notify_skill", "")
        if notify_skill:
            ctx.bus.observe(EventType.TOOL_RESULT, self._on_notify_skill_hook)
            logger.info("breather notify_skill hook armed (notify_skill=%s)", notify_skill)

    def _strategy(self) -> str:
        return str(self._cfg("strategy", "auto"))

    def _threshold_pct(self, session: Any | None = None) -> float:
        project = getattr(getattr(session, "config", None), "context_threshold_pct", None)
        if project is not None:
            return float(project)
        # 全局值每次重新读 config.yaml，确保 UI 修改后生效
        cfg = self._load_config()
        return float(cfg.get("context_threshold_pct", 70.0))

    def _subagent_threshold_pct(self, session: Any) -> float:
        project = getattr(session.config, "subagent_context_threshold_pct", None)
        if project is not None:
            return float(project)
        global_value = self._load_config().get("subagent_context_threshold_pct")
        return float(global_value) if global_value is not None else self._threshold_pct(session)

    def _context_window(self, session: Any) -> int | None:
        project = getattr(session.config, "context_window", None)
        return int(project) if project is not None else self._cfg("context_window", None)

    def _load_config(self) -> dict:
        """从 config.yaml 重新加载配置（支持热更新）。"""
        from pathlib import Path as _Path

        if isinstance(self.config_path, _Path) and self.config_path.is_file():
            import yaml as _yaml

            data = _yaml.safe_load(self.config_path.read_text()) or {}
            return data
        return self.config.extra or {}

    def _find_session_by_sdk_id(self, sdk_id: str) -> Any:
        services = self._services
        ouro_id = services.resolve_cli_session_id(sdk_id)
        if ouro_id:
            session = services.find_session(ouro_id)
            if session is not None:
                return session
        # Also try direct Workflow Controller session ID lookup (SUBAGENT events from
        # opencode driver carry the Workflow Controller session ID, not an SDK session ID).
        session = services.find_session(sdk_id)
        if session is not None:
            return session
        return next(
            (s for s in services.list_sessions() if s.native_session_id == sdk_id),
            None,
        )

    def _make_tracker(self, session: Any) -> ContextTracker:
        return ContextTracker(
            events_reader=self._services.session_events,
            session=session,
            strategy=self._strategy(),
            threshold_pct=self._threshold_pct(session),
            context_window=self._context_window(session),
            driver_factory=self._services.create_driver,
        )

    async def _check_context(self, session: Any) -> Any:
        result = await self._make_tracker(session).check()
        logger.info(
            "breather: ouro_id=%s sdk_id=%s context=%d/%d (%s), threshold=%.0f%%, method=%s",
            session.id,
            session.native_session_id,
            result.current_tokens,
            result.max_tokens,
            f"{result.percentage:.1f}%" if result.percentage is not None else "unknown",
            self._threshold_pct(session),
            result.method,
        )
        return result

    def _check_context_from_events(self, session: Any) -> Any:
        return self._make_tracker(session).check_recorded()

    async def _on_subagent_start(self, event: Any) -> None:
        if not self._installed:
            return
        session = self._services.find_session(event.session_id)
        if session is not None and session.role == "worker":
            self._group_handoff.start_monitor(session.id)

    def _extract_last_assistant_text(self, session_id: str) -> str:
        """从事件库抓该 session 最后一条 ASSISTANT_MESSAGE 的 text blocks
        （只取 type=="text"，过滤 thinking/tool_use）。无则空串。
        """
        events = self._services.session_events(session_id)
        for e in reversed(events):
            et = e.event_type
            et_str = et.value if hasattr(et, "value") else et
            if et_str == "assistant_message":
                blocks = e.data.get("content", [])
                texts = [
                    b.get("text", "")
                    for b in blocks
                    if isinstance(b, dict) and b.get("type") == "text"
                ]
                joined = "".join(texts).strip()
                if joined:
                    return joined
        return ""

    async def _on_notify_skill_hook(self, event: Any) -> None:
        if not self._installed:
            return
        notify = self._cfg("notify_skill", "")
        if event.data.get("tool_name") != notify:
            return
        sdk_id = event.session_id
        worker = self._find_session_by_sdk_id(sdk_id)
        if worker is None:
            logger.debug("notify_skill hook: no session for sdk_id=%s", sdk_id)
            return
        self._select_state(worker.id)
        if self._handoff_pending:
            return

        result = await self._check_context(worker)
        if result.percentage is None:
            return
        if self._threshold_pct(worker) >= 100 or result.percentage <= self._threshold_pct(worker):
            logger.debug(
                "notify_skill hook: context %.1f%% <= %.0f%%, continuing session",
                result.percentage,
                self._threshold_pct(worker),
            )
            spawn(self._continue_session(worker))
            return

        logger.info(
            "notify_skill hook: threshold exceeded (%.1f%% > %.0f%%), requesting handoff for %s",
            result.percentage,
            self._threshold_pct(worker),
            worker.name,
        )
        spawn(self._mark_handoff_pending(worker))

    async def _on_context_event(self, event: Any) -> None:
        """订阅 SUBAGENT_STOP（subagent 返回点）check 主 worker context，
        满 或 force 想 → _arm_active_handoff 主动停在干净 turn 边界。
        只 arm 或返，绝不 _continue_session（mid-iter 注入"请继续"会毁算子流程）。
        """
        if not self._installed:
            return
        worker = self._context_event_worker(event)
        if worker is None:
            return
        self._select_state(worker.id)
        if self._handoff_pending or self._handoff_finalizing or self._handoff_stop_armed:
            logger.info(
                "on_context_event: skip (pending=%s finalizing=%s armed=%s)",
                self._handoff_pending,
                self._handoff_finalizing,
                self._handoff_stop_armed,
            )
            return
        if not self._worker_ready_for_handoff(worker):
            return
        if not await self._should_arm_context_handoff(worker):
            return
        logger.info(
            "on_context_event: 所有 subagent 已结束，立即交接 %s（%s）",
            worker.name,
            "force" if self._handoff_force_pending else "阈值超",
        )
        self._arm_active_handoff(worker)

    def _context_event_worker(self, event: Any) -> Any | None:
        """解析 SUBAGENT_STOP 对应的 worker；中断事件或找不到时返回 None。"""
        if event.data.get("status") == "interrupted":
            logger.info("on_context_event: skip interrupted stop (internal bookkeeping)")
            return None
        worker = self._find_session_by_sdk_id(event.session_id)
        if worker is None:
            logger.info("on_context_event: worker not found for sdk_id=%s", event.session_id[:20])
            return None
        return worker

    def _worker_ready_for_handoff(self, worker: Any) -> bool:
        """worker 可在返回点交接：是 worker、未暂停、且已无在途子代理。"""
        if worker.role != "worker":
            logger.info(
                "on_context_event: skip non-worker role=%s id=%s", worker.role, worker.id[:12]
            )
            return False
        if getattr(worker, "pause_requested", False):
            logger.info("on_context_event: skip paused worker %s", worker.id[:12])
            return False
        # Use Workflow Controller session ID (worker.id) for in_flight() lookup
        # in_flight() is keyed by Workflow Controller session ID, not SDK session ID
        in_flight = self._services.in_flight_subagents(worker.id)
        remaining = len(in_flight)
        if remaining > 0:
            logger.info(
                "on_context_event: subagent 返回但仍有 %d 个 in-flight，跳过 handoff check",
                remaining,
            )
            return False
        return True

    async def _should_arm_context_handoff(self, worker: Any) -> bool:
        """force 已有明确意图直接 True；否则读用量，异常/未知/未超阈值 → False。"""
        if not self._handoff_force_pending:
            # force 已有明确意图，无需再读用量；读不到有效读数也不动作。
            try:
                result = await self._check_context(worker)
            except Exception:
                logger.exception(
                    "on_context_event: _check_context failed for %s",
                    worker.id,
                )
                return False
            if result.percentage is None:
                return False
            if self._threshold_pct(worker) >= 100 or result.percentage <= self._threshold_pct(
                worker
            ):
                logger.info(
                    "on_context_event: context not over threshold, skipping handoff for %s",
                    worker.id[:12],
                )
                return False
            logger.info(
                "on_context_event: %s context=%.1f%% > thresh=%.0f%%",
                worker.id[:12],
                result.percentage,
                self._threshold_pct(worker),
            )
        return True

    def _arm_active_handoff(self, session: Any) -> None:
        """在 subagent 返回的干净 turn 边界主动停 worker → 早边界 → finalize。

        关键：不能调用 session.stop()，因为它会设 loop_status="no"，导致
        SingleLoopEngine 在运行 iteration gate 之前就退出。只杀 worker 进程，
        让 session.query() 返回，loop 继续走到 iteration gate，由 gate 触发
        finalize_handoff()。
        """
        self._select_state(session.id)
        self._captured_final_text = self._extract_last_assistant_text(session.id)
        self._handoff_stop_armed = True
        self._handoff_pending = True
        self._handoff_finalizing = True
        # 注意：不清除 _handoff_force_pending！让前端继续显示 pending 状态。
        # _handoff_force_pending 会在 finalize_handoff 完成后由 reset_handoff_state 清除。
        self._pending_handoff_session_id = session.id
        self._pending_handoff_started_at = time.time()
        self._start_handoff_watchdog(session)
        # Only kill the driver (worker process), don't change loop_status
        driver = session.driver
        if driver is not None:
            spawn(driver.stop())

    async def _continue_session(self, session: Any) -> None:
        try:
            async for event in session.query("请继续工作", iteration=0):
                if self._installed:
                    await self._services.store_events([event])
        except Exception:
            logger.exception("continue session failed for %s", session.id)

    async def _handle_breather(self, request: web.Request) -> web.Response:
        """aiohttp hook 路由（CC hook / curl / 外部脚本）：POST /breather。

        薄包装——核心逻辑在 request_handoff，与浏览器端点 _web_handoff 共享。
        """
        body = await request.json() or {}
        data = await self.request_handoff(
            body.get("session_id", ""),
            bool(body.get("force", False)),
        )
        return web.json_response(data)

    async def _web_handoff(
        self,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """浏览器端点（FastAPI，web UI 手动 handoff 按钮）。

        body: {"session_id"?: str, "force"?: bool}。与 _handle_breather 同一核心
        （request_handoff），只是入口是 FastAPI 而非 aiohttp hook。
        """
        body = body or {}
        return await self.request_handoff(
            body.get("session_id", ""),
            bool(body.get("force", False)),
        )

    async def _web_state(self, session_id: str = "") -> dict[str, Any]:
        """web 轮询端点：返回与事件面一致的 handoff 状态形状。"""
        return self.get_handoff_state(session_id or None)

    async def _web_cancel(self, body: dict[str, Any] | None = None) -> dict[str, Any]:
        """取消一个 session 的待决 handoff，缺 session_id 拒 400。"""
        from fastapi import HTTPException

        session_id = str((body or {}).get("session_id", ""))
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id is required")
        self.reset_handoff_state(session_id)
        logger.info("breather: handoff state reset by user for %s", session_id)
        return {"ok": True}

    # Filesystem mtime is coarser than time.time() — NTFS/FAT quantize it
    # (often to ~1–2s), so a file written microseconds after started_at can
    # report an mtime slightly *below* started_at. Allow this much slack when
    # judging freshness, so a genuinely fresh write is never misread as stale.
    # A real stale leftover from a prior round is older by far more than this.
    _MTIME_SLACK_SEC = 2.0

    def _resolve_session(self, cli_session_id: str) -> Any:
        services = self._services

        if cli_session_id:
            # Try direct lookup as Workflow Controller session ID first
            session = services.find_session(cli_session_id)
            if session is not None:
                return session
            # Try resolving CLI session ID to Workflow Controller session ID
            ouro_id = services.resolve_cli_session_id(cli_session_id)
            if ouro_id:
                session = services.find_session(ouro_id)
                if session is not None:
                    return session

        sessions = services.list_sessions()
        for s in sessions:
            if s.role == "worker":
                return s
        return sessions[0] if sessions else None

    # Bundled deterministic-tools script (source of truth). Copied into each
    # target project's .workflow-controller/ so the handoff agent can invoke it via Bash
    # without needing the workflow_controller package on its PYTHONPATH.
    _HANDOFF_TOOLS_SRC = Path(__file__).parent / "scripts" / "handoff_tools.py"

    async def _mark_handoff_pending(self, session: Any) -> dict[str, Any]:
        """Mark a pending handoff and return a dict — does NOT spawn a subprocess.

        The actual handoff is driven by the host's stop-request dispatcher:
          1. stop hook arrives → plugin.on_stop_hook captures worker's
             final_text and returns let_exit, scheduling finalize_handoff.
          2. finalize_handoff spawns a handoff agent (role="handoff_agent",
             clean context) which uses deterministic tools to write
             SESSION-HANDOFF.md.
          3. on success: old worker is stopped, worker-h2 starts.
             on failure: state resets, old worker continues (natural fallback).

        Background: an earlier design spawned a new claude subprocess here
        with --resume <sdk_id> to feed the handoff prompt. That subprocess
        was rejected by Claude Code (same SDK session can't be attached
        twice), exited with returncode=1, and _mark_handoff_pending then waited the
        full handoff_timeout for a PostToolUse hook that never arrived.
        Routing through stop hook avoids spawning a parallel subprocess.
        """
        self._select_state(session.id)
        if self._handoff_pending:
            # Log the swallow so the audit trail accounts for every
            # /breather request, not just the one that wins the lock.
            # Without this, repeated /breather calls during a long-running
            # handoff look like silent no-ops in the log.
            elapsed = time.time() - self._pending_handoff_started_at
            logger.info(
                "breather: handoff request for %s skipped — already pending "
                "for %.0fs (since started_at=%.0f)",
                session.name,
                elapsed,
                self._pending_handoff_started_at,
            )
            return {
                "handoff_triggered": False,
                "reason": "handoff already pending",
            }

        self._handoff_pending = True
        self._pending_handoff_started_at = time.time()
        self._pending_handoff_session_id = session.id
        self._start_handoff_watchdog(session)

        logger.info(
            "breather: handoff pending for %s (sdk=%s, started_at=%.0f), "
            "waiting for stop hook to capture final_text",
            session.name,
            session.native_session_id,
            self._pending_handoff_started_at,
        )
        return {
            "handoff_triggered": True,
            "phase": "pending",
            "session": session.name,
        }

    def _start_handoff_watchdog(self, session: Any) -> None:
        """Arm the per-session timeout while waiting for a clean loop boundary."""
        state = self._state_for(session.id)
        if state.watchdog_task is not None and not state.watchdog_task.done():
            return
        # execute 阶段 worker 可能十几分钟后才到停止点，默认给 30min；
        # handoff_wait_stop_timeout=0 表示显式禁用。
        timeout = float(
            self._cfg(
                "handoff_wait_stop_timeout",
                self._cfg("handoff_timeout", 1800),
            )
        )
        if timeout <= 0:
            return
        state.completed_event = asyncio.Event()
        state.watchdog_task = asyncio.create_task(
            self._handoff_watchdog(session, timeout),
            name=f"breather-handoff-watchdog-{session.id}",
        )

    def _handoff_file_fresh(self, handoff_file: Path) -> bool:
        """True iff handoff_file was (re)written during the current handoff.

        Freshness = the file exists AND its mtime is at or after the moment
        this handoff started (_pending_handoff_started_at, a time.time()
        wall-clock stamp set in _do_handoff), minus a small slack for
        filesystem mtime quantization (see _MTIME_SLACK_SEC). A stale
        leftover from a prior handoff round has an mtime earlier by far more
        than the slack and is treated as "not written".
        """
        try:
            mtime = handoff_file.stat().st_mtime
        except OSError:
            return False  # missing / unreadable → not fresh
        return mtime >= self._pending_handoff_started_at - self._MTIME_SLACK_SEC

    def _deploy_handoff_tools(self, cwd: str) -> None:
        """把 handoff_tools.py 复制到 {cwd}/.workflow-controller/handoff_tools.py。

        每次 finalize 都覆盖复制，确保目标工程拿到的是当前版本的工具
        （避免旧副本与新模板调用方式不一致）。复制失败不致命——只 warn，
        handoff agent 会自然降级到用内置 Read/Glob 手动盘点。
        """
        try:
            dest_dir = Path(cwd) / ".workflow-controller"
            dest_dir.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self._HANDOFF_TOOLS_SRC, dest_dir / "handoff_tools.py")
        except Exception as e:
            # Best-effort: deploy failure must never abort a handoff. Any
            # exception (OSError, ValueError on a malformed path, shutil
            # errors, …) → warn and continue; the handoff agent degrades to
            # manual inventory via its built-in Read/Glob tools.
            logger.warning(
                "breather: 部署 handoff_tools.py 到 %s/.workflow-controller 失败：%s"
                "（handoff agent 将降级手动盘点）",
                cwd,
                e,
            )

    def _disarm_watchdog(self) -> None:
        """进入阶段二（finalize）后让 phase-1 watchdog 退场。

        watchdog（在 _do_handoff 启动）守护的是"等 worker 发 stop hook"这段
        （阶段一，防 worker 卡死永不停 → pending 永久锁死）。一旦 worker 真停了、
        finalize_handoff 开始跑（阶段二，handoff agent 在正常写文件），阶段一的
        "worker 卡死"风险已不存在，watchdog 的使命就结束了。若不让它退场，它会
        继续烧自己那份 timeout，在 agent 正常干活期间误 fire、把锁解开，从而把
        一次正在成功的 handoff 当成卡死放弃（见 workflow-controller.log 01:16:43 那次）。

        只关 watchdog，**不**重置 handoff 状态：finalize 还要用
        _pending_handoff_started_at 做产物新鲜度校验、用 _handoff_finalizing
        防重入。阶段二的超时改由 finalize 自己的 wait_session_done 独管。
        """
        if self._handoff_completed_event is not None:
            self._handoff_completed_event.set()
        if self._handoff_watchdog_task is not None and not self._handoff_watchdog_task.done():
            self._handoff_watchdog_task.cancel()
        self._handoff_watchdog_task = None
        self._handoff_completed_event = None

    async def _fail_session(
        self,
        session: Any,
        stop_reason: str,
        *,
        persist_failed_state: bool = True,
    ) -> None:
        """改判 fail 并统一落账：loop 侧可能已按原 stop_reason 发过 completed
        结局——改判 failed 是新的阶段事实，须再发一条。``persist_failed_state=False``
        用于运行时仍在/列表由调用方落终态的场景。
        """
        session.result = "fail"
        session.loop_status = "no"
        session.stop_reason = stop_reason
        if persist_failed_state:
            await self._services.persist_session(session)
        await session.emit_outcome(outcome_kind_for_reason(stop_reason))

    async def _update_session_title(
        self, session: Any, expected_name: str, timeout: float = 30.0
    ) -> None:
        """轮询 native_session_id 后经 driver 改 session 标题（装饰性，失败仅告警）。"""
        start_time = time.time()
        while not session.native_session_id and (time.time() - start_time) < timeout:
            await asyncio.sleep(0.5)

        if not session.native_session_id:
            logger.warning(
                "Session %s has no native_session_id after %.1fs, skipping title update",
                session.id,
                timeout,
            )
            return

        # let the runtime write the session row before renaming it
        await asyncio.sleep(1.0)

        runtime = getattr(getattr(session, "config", None), "runtime", None)
        try:
            driver = self._services.create_driver(runtime)
            renamed = driver.update_session_title(session.native_session_id, expected_name)
        except Exception:
            logger.warning("update_session_title failed for %s", session.id, exc_info=True)
            return
        if renamed:
            logger.info("Updated session title for %s to '%s'", session.id, expected_name)
        else:
            logger.warning(
                "Failed to update title for %s (sdk_id=%s)", session.id, session.native_session_id
            )

    def _find_initial_session(self, session: Any) -> Any | None:
        """沿 handoff 关系回溯链头（非接续的原始 worker session）。"""
        services = self._services
        current = session
        visited = set()
        while current and current.id not in visited:
            visited.add(current.id)
            pred = services.find_session_predecessor(current.id)
            if not pred:
                return current  # 链头即初始 session
            pred_session = services.find_session(pred["from_session_id"])
            if not pred_session:
                return current
            # 前序不是接续 session 时，它就是初始 session
            if not pred_session.name or "-continue-" not in pred_session.name:
                return pred_session
            current = pred_session
        return session

    async def _handoff_watchdog(self, session: Any, timeout: float) -> None:
        """Safety net for a stuck handoff.

        If the worker never reaches a clean handoff boundary after being asked
        (crashed, stopped externally, or stuck in one iteration), pending
        would stay True forever and every subsequent /breather call would
        be rejected with "already pending". After `timeout` seconds we
        give up and reset state so the next call can re-trigger.

        Normal path: finalize_handoff -> reset_handoff_state -> sets the
        event, which wakes wait_for() so this coroutine exits silently
        without firing the warn-and-reset branch.
        """
        self._select_state(session.id)
        event = self._handoff_completed_event
        if event is None:
            return
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return  # finalize signalled completion, exit silently
        except TimeoutError:
            pass

        # Defensive: another path may have already reset state.
        if not any(
            (
                self._handoff_pending,
                self._handoff_force_pending,
                self._handoff_stop_armed,
                self._handoff_finalizing,
            )
        ):
            return
        if self._pending_handoff_session_id != session.id:
            return

        logger.warning(
            "breather: handoff watchdog fired for %s after %.0fs; "
            "worker did not reach a handoff boundary. Resetting pending state.",
            session.name,
            timeout,
        )
        self.reset_handoff_state(session.id)

    def _boundary_force_pending(self, session: Any) -> IterationGateResult | None:
        """Force handoff 在自然边界的收尾；仍有子代理在跑则交给 subagent 完成点。

        非 force 场景（未设标志或已在 finalize）返回 None，由调用方继续。
        """
        if not self._handoff_force_pending or self._handoff_finalizing:
            return None
        # Check if subagents are still running - if so, defer to _on_context_event
        in_flight = self._services.in_flight_subagents(session.id)
        if in_flight:
            logger.info(
                "iteration_boundary: force pending but %d subagents still running, "
                "deferring to subagent completion for %s",
                len(in_flight),
                session.name,
            )
            return IterationGateResult(action="pass")
        self._captured_final_text = self._extract_last_assistant_text(session.id)
        self._handoff_pending = True
        self._handoff_finalizing = True
        # 注意：不清除 _handoff_force_pending！让前端继续显示 pending 状态。
        # _handoff_force_pending 会在 finalize_handoff 完成后由 reset_handoff_state 清除。
        self._pending_handoff_session_id = session.id
        self._pending_handoff_started_at = time.time()
        logger.info(
            "iteration_boundary: force pending（无 subagent 返回点兜底），直接 finalize %s",
            session.name,
        )
        spawn(self.finalize_handoff(session))
        return IterationGateResult(action="stop", reason="breather_handoff")

    async def _boundary_threshold_handoff(self, session: Any) -> IterationGateResult:
        """用量检查；超阈值且无在途子代理时在本边界交接，否则 pass。"""
        try:
            result = await self._check_context(session)
            if (
                self._threshold_pct(session) >= 100
                or result.percentage is None
                or result.percentage <= self._threshold_pct(session)
            ):
                return IterationGateResult(action="pass")
            # Check if subagents are still running - if so, defer handoff to
            # _on_context_event which will trigger when all subagents complete
            in_flight = self._services.in_flight_subagents(session.id)
            if in_flight:
                logger.info(
                    "iteration_boundary: %.1f%% > %.0f%% but %d subagents still running, "
                    "deferring handoff to subagent completion for %s",
                    result.percentage,
                    self._threshold_pct(session),
                    len(in_flight),
                    session.name,
                )
                return IterationGateResult(action="pass")
            self._captured_final_text = self._extract_last_assistant_text(session.id)
            self._handoff_pending = True
            self._pending_handoff_session_id = session.id
            self._pending_handoff_started_at = time.time()
            logger.info(
                "iteration_boundary: %.1f%% > %.0f%%, handoff %s",
                result.percentage,
                self._threshold_pct(session),
                session.name,
            )
            spawn(self.finalize_handoff(session))
            return IterationGateResult(action="stop", reason="breather_handoff")
        except Exception:
            logger.exception("on_iteration_boundary failed for %s", session.id)
            return IterationGateResult(action="pass")

    async def _handoff_usage_gate(self, worker: Any) -> tuple[dict[str, Any] | None, Any]:
        """非 force 的用量门；返回 (回绝响应或 None, 用量结果)。

        强制交接不依赖用量；非 force 读不到有效读数时如实报告未知，
        不能伪装成低于阈值；阈值满才切（None 表示已超阈值可交接）。
        """
        result = await self._check_context(worker)
        if result.percentage is None:
            return {
                "handoff_triggered": False,
                "reason": "context_usage_unknown",
                "context": {"percentage": None},
            }, result
        if self._threshold_pct(worker) >= 100 or result.percentage <= self._threshold_pct(worker):
            return {
                "handoff_triggered": False,
                "context": {
                    "current_tokens": result.current_tokens,
                    "max_tokens": result.max_tokens,
                    "percentage": result.percentage,
                },
            }, result
        return None, result

    def _plan_handoff_docs(self, session: Any, cwd: str) -> _HandoffPlan:
        """算本轮 handoff 的命名（agent/doc/done/接续 session）与 agent prompt。

        per-project handoff/continuation 模板覆盖（workflow-controller.yaml →
        SessionConfig）；未声明则用全局默认 generator（老项目零变化）。
        """
        gen = self._effective_generator(session)

        # Check if this is an initial handoff (predecessor is not a continuation session)
        initial_session = self._find_initial_session(session)
        is_initial_handoff = bool(initial_session and initial_session.id == session.id)

        session_id_short = session.id[:8]
        successors = self._services.list_session_successors(session.id)
        handoff_count = len(successors) + 1

        # Get starting_prompt for initial handoff (to extract operator name)
        initial_prompt = (self._services.starting_prompt() or "") if is_initial_handoff else ""

        # Get birth_handoff_doc from current worker session (for inheriting historical context)
        birth_handoff_doc = getattr(session, "birth_handoff_doc", None) or ""

        # Generate handoff session and doc names (no operator in name)
        handoff_doc_name = gen.handoff_doc_name(session_id_short, handoff_count)
        done_file_name = gen.done_file_name(session_id_short, handoff_count)

        agent_prompt = gen.build_handoff_agent_prompt(
            final_text=self._captured_final_text,
            project_dir=cwd,
            worker_session_id=session.id,
            handoff_doc_name=handoff_doc_name,
            initial_prompt=initial_prompt,
            birth_handoff_doc=birth_handoff_doc,
            events_project_dir=self._services.events_project_dir(),
        )
        return _HandoffPlan(
            gen=gen,
            handoff_count=handoff_count,
            handoff_doc_name=handoff_doc_name,
            done_file_name=done_file_name,
            agent_prompt=agent_prompt,
            agent_session_name=gen.handoff_session_name(session_id_short, handoff_count),
            continuation_name=gen.continuation_session_name(session_id_short, handoff_count),
        )

    async def _start_handoff_agent(self, session: Any, cwd: str, plan: _HandoffPlan) -> Any:
        """配置并启动 handoff agent session，随即登记 worker → agent 关系。"""
        from workflow_controller.config.settings import LoopConfig, SessionConfig

        agent_config = SessionConfig(
            name=plan.agent_session_name,
            role="handoff_agent",
            cwd=cwd,
            initial_prompt=plan.agent_prompt,
            max_turns=100,
            model=session.config.model,
            provider_index=session.config.provider_index,
            run_id=session.run_id,  # 谱系继承：handoff agent 入 worker 的 run
            reviewer=(
                session.config.reviewer.model_copy(deep=True) if session.config.reviewer else None
            ),
            # 显式继承 worker 的 runtime，与 worker-h2（build_new_session_config）
            # 对齐。否则 runtime 回落全局 _config_runtime——worker 用 per-session
            # 覆盖且与全局不同时，handoff agent 会跑在跟 worker 不同的 runtime 上。
            runtime=session.config.runtime,
            cli_wrapper=session.config.cli_wrapper,
            loop=LoopConfig(max_iterations=1, evaluator="always_complete"),
        )
        agent_session = await self._services.start_session(agent_config)

        # Add worker → handoff relation immediately so frontend can track
        # handoff progress.  MUST be before any slow operations.
        try:
            await self._services.add_handoff_relation(
                from_session_id=session.id,
                to_session_id=agent_session.id,
            )
        except Exception:
            logger.exception(
                "Failed to add handoff relation for %s → %s", session.id, agent_session.id
            )
        return agent_session

    async def _update_title_with_retry(self, session_obj: Any, name: str) -> None:
        """Fire-and-forget 标题改写（装饰性，失败仅告警，不阻塞交接）。"""
        for attempt in (1, 2):
            try:
                await self._update_session_title(session_obj, name)
                return
            except Exception:
                if attempt == 2:
                    logger.warning(
                        "Failed to update session title for %s after 2 attempts",
                        session_obj.id[:12],
                    )

    async def _await_handoff_agent(self, session: Any, agent_session: Any) -> bool:
        """等 handoff agent 写完产物；超时或被用户暂停按失败收尾并返回 False。

        start_session 在后台 task 被调度后立即返回（见 SessionManager.create_and_run），
        此刻 agent 才刚开始写文档。不等它结束就检查文件，会看到尚未落盘的
        handoff.md → 误判"未写" → 错误降级。
        阶段二超时（等 handoff agent 写完 handoff.md）默认 10min——实测 agent
        写文件约 4min，10min 留足余量。与阶段一（30min）独立配置，避免两段
        共用一个值时互相迁就。优先读 handoff_agent_timeout，未设回退旧
        handoff_timeout，再回退默认 600。=0 表示无限等。
        """
        timeout = float(
            self._cfg(
                "handoff_agent_timeout",
                self._cfg("handoff_timeout", 600),
            )
        )
        execution_timeout = timeout if timeout > 0 else None
        done = await self._services.wait_session_done(
            agent_session.id,
            timeout=execution_timeout,
        )
        if not done:
            logger.error(
                "handoff agent %s 在 %.0fs 内未结束 — 重置状态，worker 继续",
                agent_session.name,
                timeout,
            )
            await self._fail_session(agent_session, "error")
            return False
        if agent_session.stop_reason == "user_pause":
            logger.warning(
                "handoff agent %s was paused by user — treating as fail, not creating continuation",
                agent_session.name,
            )
            agent_session.result = "fail"
            await self._services.persist_session(agent_session)
            self.reset_handoff_state(session.id)
            return False
        return True

    def _fresh_handoff_files(
        self, session: Any, agent_session: Any, plan: _HandoffPlan
    ) -> Path | None:
        """校验本轮 handoff 文档/完成信号是新写的；文档过期返回 None（调用方降级）。

        只判 exists() 不够：handoff doc 被设计成跨轮保留、增量更新（见
        HANDOFF_AGENT_PROMPT 的"增量更新"段），上一轮的旧文件一直都在。改判
        mtime 不早于本轮 handoff 起点（started_at 在 arm 时用 time.time() 赋值），
        同时检查 done 信号文件（agent 完成全部工作后写入）。
        """
        handoff_file = plan.gen.handoff_path(session, filename=plan.handoff_doc_name)
        done_file = plan.gen.handoff_path(session, filename=plan.done_file_name)

        if not self._handoff_file_fresh(handoff_file):
            logger.error(
                "handoff agent %s 未在本轮更新 %s（exists=%s, "
                "started_at=%.0f）— 重置状态，worker 继续",
                agent_session.name,
                handoff_file,
                handoff_file.exists(),
                self._pending_handoff_started_at,
            )
            return None
        if not self._handoff_file_fresh(done_file):
            logger.warning(
                "handoff agent %s 未写入完成信号 %s（handoff 文档已更新但 agent 可能未完成）"
                "— 仍继续 handoff（文档已就位）",
                agent_session.name,
                done_file,
            )
        return handoff_file

    async def _start_continuation(
        self, session: Any, agent_session: Any, plan: _HandoffPlan, handoff_file: Path
    ) -> Any:
        """起 worker-h2：继承 birth 文档、落 success、登记关系并改标题。"""
        new_config = plan.gen.build_new_session_config(
            session,
            handoff_file,
            plan.continuation_name,
        )
        new_config.name = plan.continuation_name
        new_session = await self._services.start_session(new_config)
        # Set birth_handoff_doc on continuation session (for future handoffs to inherit)
        new_session.birth_handoff_doc = plan.handoff_doc_name
        await self._services.persist_session(new_session)

        # 新 worker 创建成功 → 立即设置 result
        agent_session.result = "success"
        await self._services.persist_session(agent_session)
        await self._services.add_handoff_relation(
            from_session_id=agent_session.id,
            to_session_id=new_session.id,
        )

        await self._update_session_title(new_session, plan.continuation_name)
        return new_session

    async def _continue_or_degrade(
        self, session: Any, agent_session: Any, plan: _HandoffPlan, handoff_file: Path
    ) -> Any | None:
        """起 worker-h2；失败则按失败收尾并返回 None（旧 worker 留作降级）。"""
        try:
            return await self._start_continuation(session, agent_session, plan, handoff_file)
        except Exception:
            logger.error(
                "起 worker-h2 失败 — 不停旧 worker %s，天然降级",
                session.id,
                exc_info=True,
            )
            await self._fail_session(agent_session, "error")
            return None

    async def _finish_handoff(self, session: Any, new_session: Any, handoff_file: Path) -> None:
        """停旧 worker，并把原 worker 恢复为 loop 可复用态后单次落账。"""
        try:
            # persist_stopped_state=False：中间 STOPPED 态不落会话记录——恢复 loop
            # 可复用态后单次落账，会话记录不残留不可复用终态
            await self._services.stop_session(session.id, persist_stopped_state=False)
        except Exception:
            logger.warning(
                "stop_session 失败 for %s（可能已停止）",
                session.id,
            )

        # 交接完成后把原 worker 的 loop_status 恢复为 "yes"——原 worker 可再被 loop 复用
        session.loop_status = "yes"
        session.stop_reason = None
        await self._services.update_loop_status(
            session.id,
            loop_status="yes",
            stop_reason=None,
        )
        await self._services.persist_session(session)

        logger.info(
            "handoff v2 完成: %s → handoff-agent → %s, file=%s",
            session.name,
            new_session.name,
            handoff_file,
        )

    async def _park_worker_for_handoff(self, worker: Any) -> None:
        """停 worker 并按 handoff_triggered 落账 IDLE（loop 可复用态）。"""
        logger.info("handoff: stopping worker %s", worker.id)
        # persist_stopped_state=False：中间 STOPPED 态不落列表——两写间崩溃会把
        # worker 以 STOPPED+handoff_triggered 终态落账，而非 IDLE 可复用态
        await self._services.stop_session(
            worker.id,
            reason="handoff_triggered",
            persist_stopped_state=False,
        )
        worker.state = SessionState.IDLE
        worker.loop_status = "no"
        worker.stop_reason = "handoff_triggered"
        await self._services.persist_session(worker)
        logger.info("handoff: worker %s is now idle", worker.id)

    async def _start_linked_session(self, config: Any, from_session_id: str) -> Any:
        """起 session 并登记 handoff 关系。

        交接/接续 prompt 是调用方构造的精确指令，跳过插件 prefix 聚合。
        """
        session = await self._services.start_session(config, compose_prompt=False)
        await self._services.add_handoff_relation(
            from_session_id=from_session_id,
            to_session_id=session.id,
        )
        return session

    async def _abort_handoff_session(
        self,
        handoff_session: Any,
        stop_reason: str,
        error: str,
        *,
        report_id: bool,
    ) -> dict[str, Any]:
        """交接 session 未成：改判失败并返回失败结果（按需附 session id）。"""
        await self._services.update_loop_status(
            handoff_session.id,
            loop_status="no",
            stop_reason=stop_reason,
        )
        await self._fail_session(handoff_session, stop_reason, persist_failed_state=False)
        result: dict[str, Any] = {"success": False, "error": error}
        if report_id:
            result["handoff_session_id"] = handoff_session.id
        return result
