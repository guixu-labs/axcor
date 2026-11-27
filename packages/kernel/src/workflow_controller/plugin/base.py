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

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from workflow_controller.event.event_bus import InterceptResult
from workflow_controller.support.tasks import spawn

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from pathlib import Path

    from workflow_controller.hooks.server import HookServer
    from workflow_controller.plugin.context import PluginContext
    from workflow_controller.plugin.run_driver import RunDriverFactory
    from workflow_controller.session.events import Event

logger = logging.getLogger(__name__)


@dataclass
class PluginConfig:
    name: str
    version: str
    enabled: bool = False
    extra: dict[str, Any] | None = None


@dataclass
class StopHookResult:
    """Returned by Plugin.on_stop_hook to intercept the stop-hook flow.

    Attributes:
        action: "block" to keep worker alive with feedback, "let_exit" to
                release worker (optionally with a side-effect callback).
        reason: Feedback text for "block" (injected back to worker).
        on_let_exit: Optional async callback for "let_exit" (e.g. spawn
                     successor session). Called with the session object.
    """

    action: Literal["block", "let_exit"]
    reason: str = ""
    on_let_exit: Callable[[Any], Coroutine[Any, Any, None]] | None = None


@dataclass
class IterationGateResult:
    """Returned by Plugin.on_iteration_boundary to gate the loop at the
    iteration boundary (after the worker subprocess EOFs, before the
    reviewer/evaluator).

    Attributes:
        action: "pass" = no intervention, proceed normally (default);
                "block" = inject feedback and force another iteration
                (phase redirect); "stop" = end the loop now
                (e.g. breather handoff).
        feedback: For "block" — injected as the next iteration's user message.
        reason: Short tag for logs / LOG_LOOP events.
    """

    action: Literal["pass", "block", "stop"]
    feedback: str = ""
    reason: str = ""


@dataclass
class RunPanelDef:
    """run 详情驱动面板的贡献声明（Web 面板槽位路由用）。

    Attributes:
        format: 面板承接的驱动格式串（run 账面的 format 列）。
        component: 前端组件 id（前端组件注册表的键）。
        schema_version: 面板可渲染的投影 schema_version 上限。
    """

    format: str
    component: str
    schema_version: int


class Plugin(ABC):
    _ctx: PluginContext

    def __init__(self, config: PluginConfig, config_path: Path | None) -> None:
        self.config = config
        self.config_path = config_path

    @classmethod
    def builtin_instruction_files(cls) -> dict[str, tuple[str, str]]:
        """Expose built-in instruction templates for settings export."""
        return {}

    def manages_context_window(self) -> bool:
        return False

    # ------------------------------------------------------------------
    # 生命周期（唯一入口：install 与 uninstall 两个扩展点）
    # ------------------------------------------------------------------

    def install(self, ctx: PluginContext) -> None:
        """Single lifecycle entry — the only install method.

        Called by :class:`PluginRegistry` at load / reload time with a
        :class:`PluginContext` bundling ``bus`` / ``hook_server`` /
        ``type_registry`` / ``services``（宿主能力端口）.

        The **default body** captures ``self._ctx`` and dispatches the two
        opt-in extension points — :meth:`_register_interceptors` (control-event
        interceptors) and :meth:`_register_observers` (typed observers). A
        plugin that only needs bus wiring leaves this default alone and just
        overrides the relevant helper(s); a plugin that also needs setup
        (hook routes, state, typed ``observe`` subscriptions) overrides
        ``install``, calls ``super().install(ctx)`` to keep the bus wiring,
        then does its setup::

            def install(self, ctx):
                super().install(ctx)            # _ctx + interceptors/observers
                self.register_hook_routes(ctx.hook_server, [...])
                # 需要宿主能力时经 ctx.services 端口调用，不持有宿主对象

        ``register_hook_routes`` is frozen-aware — when the aiohttp router is
        already frozen (runtime reload after the first request), it skips route
        registration with a warning instead of raising, so this method is safe
        to call at any lifecycle stage. Bus interceptors / observers only
        depend on ``ctx`` and always register.

        The base class also provides an opt-in translation helper
        (:meth:`_on_stop_intercept` /
        :meth:`_on_iteration_boundary_intercept`) that adapts the legacy
        ``on_stop_hook`` / ``on_iteration_boundary`` results into
        ``InterceptResult``. A plugin may instead register its own
        interceptor returning ``InterceptResult`` directly.
        """
        self._ctx = ctx
        self._register_interceptors(ctx)
        self._register_observers(ctx)

    @abstractmethod
    def uninstall(self) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Compat interceptors (translate legacy on_* hook results → InterceptResult)
    # ------------------------------------------------------------------
    # Pure translation + plugin-owned side-effects only. Framework side-effects
    # (decision audit event, SingleLoopEngine.mark_reviewer_let_exit) live in
    # Orchestrator._decide_stop / _run_iteration_boundary_gates, driven by the
    # returned InterceptResult — so these never reach into orchestrator internals.

    def get_run_driver_factories(self) -> dict[str, RunDriverFactory]:
        """覆写以登记 run 驱动工厂（``{format: 工厂}``）；默认不登记。

        格式是开放集合：定制格式的编排逻辑住在驱动里（纯决策，I/O 经动作
        交宿主执行），不进内核；start_run 按程序 format 字段到这里查表，
        未登记格式会被拒绝。
        """
        return {}

    def register_hook_routes(
        self,
        hook_server: HookServer | None,
        routes: list[tuple[str, str, Any]],
    ) -> None:
        """Register HTTP routes on the aiohttp HookServer (port 18765).

        Each route is a tuple of (method, path, handler).  Handlers receive
        a ``aiohttp.web.Request`` and must return a ``aiohttp.web.Response``.

        Use this for endpoints called by Claude Code subprocess hooks
        (PostToolUse, Stop, etc.).  For browser-accessible endpoints, use
        :meth:`get_web_router` instead.

        Frozen-aware: when the aiohttp router is already frozen (runtime
        reload after the first request has started the server), adding
        routes would raise ``RuntimeError``. In that case we skip route
        registration with a warning — bus interceptors / observers (which
        only need ``ctx``) still register, so a runtime-reloaded plugin's
        control-event wiring keeps working even if its HTTP hook routes
        can't be added.
        """
        if hook_server is None:
            logger.warning(
                "register_hook_routes skipped — hook_server is None "
                "(port busy / not started); routes not added",
            )
            return
        router = getattr(getattr(hook_server, "app", None), "router", None)
        if router is not None and getattr(router, "frozen", False):
            logger.warning(
                "register_hook_routes skipped — hook_server router is "
                "frozen (runtime reload); routes not added",
            )
            return
        for method, path, handler in routes:
            hook_server.register_route(method, path, handler)

    def prompt_prefix(self, config: Any) -> str | None:
        """Contribute a prefix prepended to the session's initial prompt.

        Return a string to prepend (e.g. the generic staged-pipeline work
        discipline), or None to contribute nothing. The orchestrator calls
        :func:`compose_initial_prompt` at session start, which concatenates
        every plugin's prefix (in load order) **before** the project's
        ``starting_prompt``.

        Why a hook: the "one stage per turn / stop / report / no cross-stage"
        discipline describes how the stage *mechanism* works, not domain
        knowledge — so it belongs in the framework/plugin, not copy-pasted
        into every domain's ``starting_prompt``. Domain prompts stay lean
        (task + per-stage content notes). ``config`` is the SessionConfig, so
        plugins can contribute conditionally (e.g. only when stages declared).
        """
        return None

    def on_stop_hook(
        self, session_id: str, session: Any, final_text: str = ""
    ) -> StopHookResult | None:
        """Intercept the stop-hook before the reviewer is consulted.

        Return a StopHookResult to short-circuit the normal reviewer flow,
        or None to let the reviewer decide as usual. The orchestrator
        iterates through all loaded plugins; the first non-None result wins.

        final_text is the last assistant message from the worker's CC
        subprocess (empty string if unavailable).
        """
        return None

    async def on_iteration_boundary(
        self,
        session_id: str,
        session: Any,
        iteration: int,
    ) -> IterationGateResult:
        """Gate the loop at the iteration boundary. Return IterationGateResult
        with action="pass" (no intervention, proceed normally), action="block"
        (inject feedback + retry), or action="stop" (end the loop).
        The orchestrator iterates all plugins; the first block/stop wins.
        """
        return IterationGateResult(action="pass")

    # ------------------------------------------------------------------
    # Utilities (helpers for plugin authors)
    # ------------------------------------------------------------------

    def get_run_panels(self) -> list[RunPanelDef]:
        """run 详情驱动面板的贡献声明（默认无面板；有面板的插件覆写）。

        声明经 /api/plugins 下发，前端按 (run.format, 投影 schema_version)
        路由到注册表里的组件。
        """
        return []

    # ------------------------------------------------------------------
    # Hook interceptors (optional — override to intercept hook flow)
    # ------------------------------------------------------------------

    def get_handoff_states(self) -> dict[str, dict[str, Any]] | None:
        """插件持有的 handoff 状态（session_id → state）；不提供该能力时返回 None。

        注册表聚合各插件的返回值，作为 SSE ``handoff_update`` 的数据源——
        消费方不按插件名反射查找。
        """
        return None

    def get_web_router(self) -> Any | None:
        """Return a FastAPI APIRouter for browser-accessible endpoints.

        The web app's ``create_app()`` calls this on every loaded plugin
        and includes the router via ``app.include_router()``.

        Return None if the plugin has no web/browser endpoints.  Use
        :meth:`register_hook_routes` for aiohttp HookServer (CC-hook) routes.
        """
        return None

    def _register_interceptors(self, ctx: PluginContext) -> None:
        """Override to register bus interceptors. Default: none.

        Called from the default :meth:`install` body. A plugin that wants to
        intercept a control event overrides this and calls
        ``ctx.bus.intercept(event_type, handler, priority=...)``, typically
        with a base translation helper (``self._on_stop_intercept`` etc.) or
        its own interceptor returning ``InterceptResult`` directly. Plugins
        that only observe (no interception) leave this default no-op.

        Not reflection-based — a plugin that wants to intercept declares it
        here explicitly (OCP-friendly, debuggable).
        """
        return

    def _register_observers(self, ctx: PluginContext) -> None:
        """Override to register bus observers (typed subscription). Default: none.

        Symmetric counterpart of :meth:`_register_interceptors` for
        fire-and-forget hooks. Write your own async handler and call
        ``ctx.bus.observe(event_type, handler)``, e.g.::

            async def _on_tool_result(self, event):
                ...
            ctx.bus.observe(EventType.TOOL_RESULT, self._on_tool_result)
        """
        return

    # ------------------------------------------------------------------
    # UI extensions (optional — override to contribute UI elements)
    # ------------------------------------------------------------------

    async def _on_stop_intercept(self, event: Event) -> InterceptResult | None:
        """stop_requested interceptor: translate ``on_stop_hook`` result."""
        session = self._ctx.services.find_session(event.session_id)
        final_text = event.data.get("final_text", "")
        result = self.on_stop_hook(event.session_id, session, final_text)
        if result is None:
            return None
        if result.action == "block":
            logger.info(
                "stop_hook: plugin %s block for %s",
                self.config.name,
                event.session_id[:12],
            )
            return InterceptResult(
                action="block",
                feedback=result.reason,
                source=self.config.name,
            )
        if result.action == "let_exit":
            logger.info(
                "stop_hook: plugin %s let_exit for %s",
                self.config.name,
                event.session_id[:12],
            )
            # on_let_exit is the plugin's own successor-spawn callback;
            # schedule it fire-and-forget here (plugin-owned concern).
            if result.on_let_exit is not None and session is not None:
                spawn(result.on_let_exit(session))
            # action="let_exit" tells _decide_stop to release the worker AND
            # force-complete the loop (mark_reviewer_let_exit). source carries
            # the plugin name for the stop_hook_plugin audit event.
            return InterceptResult(action="let_exit", source=self.config.name)
        return None

    async def _on_iteration_boundary_intercept(
        self,
        event: Event,
    ) -> InterceptResult | None:
        """iteration_boundary interceptor: translate ``on_iteration_boundary``.

        ``source`` carries the plugin name: the dispatcher
        (_run_iteration_boundary_gates) emits the stop_hook_plugin audit
        event **only** for plugin-sourced verdicts (mirrors _decide_stop /
        _on_stop_intercept). The reviewer path emits its own decisions
        (stop_hook_llm verdict, stop_hook_plugin gate_pass/skip checkpoint)
        in _run_stage_review_at_boundary, so leaving source empty there keeps
        the dispatcher from double-logging them.
        """
        session = self._ctx.services.find_session(event.session_id)
        iteration = event.data.get("iteration", 0)
        result = await self.on_iteration_boundary(
            event.session_id,
            session,
            iteration,
        )
        if result.action == "pass":
            return None
        if result.action == "block":
            return InterceptResult(
                action="block",
                feedback=result.feedback,
                reason=result.reason,
                source=self.config.name,
            )
        if result.action == "stop":
            return InterceptResult(
                action="stop",
                reason=result.reason,
                source=self.config.name,
            )
        return None

    def _cfg(self, key: str, default: Any = None) -> Any:
        """Read a value from plugin config ``extra`` section in config.yaml."""
        if self.config.extra is None:
            return default
        return self.config.extra.get(key, default)


def compose_initial_prompt(plugins: list[Plugin], config: Any) -> str:
    """Build the session's initial prompt = (plugin prefixes) + base prompt.

    Each plugin's :meth:`Plugin.prompt_prefix` is asked (in load order);
    None / blank results are skipped. The project's ``config.initial_prompt``
    is appended last. With zero prefixes the base prompt is returned unchanged
    (general workflows are unaffected). Plugins own generic mechanism
    descriptions (e.g. staged-pipeline work discipline) so domain prompts
    don't have to repeat them.
    """
    parts: list[str] = []
    for plugin in plugins:
        try:
            prefix = plugin.prompt_prefix(config)
        except Exception:
            prefix = None
        if prefix and prefix.strip():
            parts.append(prefix)
    base = getattr(config, "initial_prompt", "") or ""
    if not parts:
        return base
    parts.append(base)
    return "\n\n".join(parts)
