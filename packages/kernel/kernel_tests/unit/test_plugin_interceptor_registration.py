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

"""Plugin interceptor explicit declaration via _register_interceptors override.

Covers the design: the base no longer reflection-scans for overridden
``on_*`` hooks. A plugin that wants to intercept **explicitly** overrides
``_register_interceptors`` and calls ``ctx.bus.intercept(...)`` with a base
translation helper (``self._on_stop_intercept`` etc.) or its own interceptor.
The default :meth:`Plugin.install` body captures ``_ctx`` and dispatches the
helper, so subclasses that only need bus wiring don't override ``install``.
Plugins that only observe (no interception) leave the default no-op.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from workflow_controller.event.event_bus import EventBus
from workflow_controller.event.type_registry import EventTypeRegistry
from workflow_controller.plugin.base import (
    IterationGateResult,
    Plugin,
    PluginConfig,
    StopHookResult,
)
from workflow_controller.plugin.context import PluginContext
from workflow_controller.plugin.registry import _build_plugin_services


def _ctx_with_bus() -> tuple[PluginContext, EventBus]:
    store_router = MagicMock()
    store_router.store = AsyncMock()
    bus = EventBus(store_router=store_router, type_registry=EventTypeRegistry())
    orch = MagicMock()
    orch.pool.get.return_value = None
    ctx = PluginContext(
        bus=bus,
        type_registry=EventTypeRegistry(),
        hook_server=MagicMock(),
        services=_build_plugin_services(orch),
    )
    return ctx, bus


def _cfg() -> PluginConfig:
    return PluginConfig(name="t", version="0", enabled=True)


class _Base(Plugin):
    """Abstract lifecycle only; inherits install (default: _ctx + helpers)."""

    def uninstall(self) -> None:
        pass


class _StopPlugin(_Base):
    """Mirrors breather: override on_stop_hook + declare it explicitly."""

    def __init__(self, cfg, path, ret):
        super().__init__(cfg, path)
        self._ret = ret

    def on_stop_hook(self, session_id, session, final_text="") -> StopHookResult | None:
        return self._ret

    def _register_interceptors(self, ctx) -> None:
        ctx.bus.intercept("stop_requested", self._on_stop_intercept, priority=10)


class _GatePlugin(_Base):
    def __init__(self, cfg, path, ret):
        super().__init__(cfg, path)
        self._ret = ret

    async def on_iteration_boundary(self, sid, session, iteration) -> IterationGateResult:
        return self._ret

    def _register_interceptors(self, ctx) -> None:
        ctx.bus.intercept(
            "iteration_boundary",
            self._on_iteration_boundary_intercept,
            priority=10,
        )


def _emit(bus, event_type, session_id="s1", **data):
    from workflow_controller.session.events import Event

    return asyncio.run(
        bus.emit(
            Event.create(
                session_id=session_id,
                iteration=0,
                event_type=event_type,
                data=data,
            )
        )
    )


def test_stop_explicit_register_priority_10():
    ctx, bus = _ctx_with_bus()
    p = _StopPlugin(_cfg(), Path("."), None)
    p.install(ctx)
    chain = getattr(bus, "_interceptors")
    assert len(chain["stop_requested"]) == 1
    prio, handler = chain["stop_requested"][0]
    assert prio == 10
    assert handler == getattr(p, "_on_stop_intercept")  # the helper, registered explicitly
    assert not chain.get("pre_tool_use")
    assert not chain.get("iteration_boundary")


def test_iteration_boundary_explicit_register_priority_10():
    ctx, bus = _ctx_with_bus()
    p = _GatePlugin(_cfg(), Path("."), IterationGateResult(action="pass"))
    p.install(ctx)
    chain = getattr(bus, "_interceptors")
    assert len(chain["iteration_boundary"]) == 1
    assert chain["iteration_boundary"][0][0] == 10
    assert chain["iteration_boundary"][0][1] == getattr(p, "_on_iteration_boundary_intercept")
    assert not chain.get("stop_requested")


def test_default_install_is_noop():
    """A plugin whose _register_interceptors is the default no-op registers nothing."""
    ctx, bus = _ctx_with_bus()
    _Base(_cfg(), Path(".")).install(ctx)
    assert not getattr(bus, "_interceptors")


def test_no_override_of_on_stop_does_not_register_anything():
    """Crucially: merely defining on_stop_hook no longer auto-registers it.
    The reflection auto-registration is gone — only an explicit
    _register_interceptors override calling ctx.bus.intercept(...) registers.
    """
    ctx, bus = _ctx_with_bus()

    class _Silent(_Base):
        def on_stop_hook(self, sid, session, final_text=""):  # defined but not declared
            return None

    _Silent(_cfg(), Path(".")).install(ctx)
    assert not getattr(bus, "_interceptors")  # no auto-registration


def test_plugins_run_before_reviewer_via_priority():
    ctx, bus = _ctx_with_bus()
    _StopPlugin(_cfg(), Path("."), None).install(ctx)
    bus.intercept("stop_requested", lambda event: None, priority=0)  # reviewer
    chain = getattr(bus, "_interceptors")["stop_requested"]
    assert chain[0][0] == 10 and chain[1][0] == 0


def test_stop_block_translation_sets_source():
    ctx, bus = _ctx_with_bus()
    p = _StopPlugin(_cfg(), Path("."), StopHookResult(action="block", reason="nope"))
    p.install(ctx)
    r = _emit(bus, "stop_requested", final_text="done")
    assert r is not None and r.action == "block"
    assert r.feedback == "nope"
    assert r.source == p.config.name


def test_stop_let_exit_translation_schedules_callback_and_marks_source():
    ctx, bus = _ctx_with_bus()

    def _session_finder(_sid):
        return "session-obj"

    ctx.services.session_finder = _session_finder
    cb = AsyncMock()
    p = _StopPlugin(
        _cfg(),
        Path("."),
        StopHookResult(action="let_exit", on_let_exit=cb),
    )
    p.install(ctx)

    r = _emit(bus, "stop_requested", final_text="bye")
    assert r is not None and r.action == "let_exit"
    assert r.source == p.config.name

    async def _drain():
        await asyncio.sleep(0)

    asyncio.run(_drain())
    cb.assert_awaited_once_with("session-obj")


def test_stop_none_passes_through():
    ctx, bus = _ctx_with_bus()
    p = _StopPlugin(_cfg(), Path("."), None)
    p.install(ctx)
    assert _emit(bus, "stop_requested", final_text="x") is None


def test_iteration_boundary_block_translation_sets_source():
    ctx, bus = _ctx_with_bus()
    p = _GatePlugin(
        _cfg(),
        Path("."),
        IterationGateResult(action="block", feedback="fb", reason="jump"),
    )
    p.install(ctx)
    r = _emit(bus, "iteration_boundary", iteration=2)
    assert r is not None and r.action == "block"
    assert r.feedback == "fb" and r.reason == "jump"
    # source carries the plugin name so the dispatcher can emit a
    # stop_hook_plugin audit event **only** for plugin-sourced verdicts
    # (mirrors _on_stop_intercept); the reviewer path leaves source empty
    # so the dispatcher doesn't double-log its own decisions.
    assert r.source == p.config.name


def test_iteration_boundary_pass_returns_none():
    ctx, bus = _ctx_with_bus()
    p = _GatePlugin(_cfg(), Path("."), IterationGateResult(action="pass"))
    p.install(ctx)
    assert _emit(bus, "iteration_boundary", iteration=1) is None
