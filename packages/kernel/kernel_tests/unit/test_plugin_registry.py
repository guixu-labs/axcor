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

"""handoff 聚合端口：注册表聚合插件上报（含失败隔离）。"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from workflow_controller.config.settings import AppConfig, ReviewerConfig
from workflow_controller.core.orchestrator import Orchestrator
from workflow_controller.plugin.base import Plugin, PluginConfig

if TYPE_CHECKING:
    from workflow_controller.plugin.registry import PluginRegistry


class _HandoffPlugin(Plugin):
    def uninstall(self) -> None:
        pass

    def get_handoff_states(self):
        return {"s1": {"pending": True, "source": "plugin"}}


class _BrokenHandoffPlugin(Plugin):
    def uninstall(self) -> None:
        pass

    def get_handoff_states(self):
        raise RuntimeError("boom")


def _mock_orchestrator(**overrides):
    """最简 orchestrator 替身：补齐 _build_plugin_services 读取的端口属性。"""
    return SimpleNamespace(
        read_main_context=lambda *a, **k: 0,
        read_subagent_context=lambda *a, **k: 0,
        snapshot_main_source=lambda *a, **k: False,
        snapshot_subagent_source=lambda *a, **k: False,
        **overrides,
    )


def test_handoff_states_merge_plugin_reports_with_failure_isolation(tmp_path) -> None:
    config = AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False))
    orch = Orchestrator(config, default_project_dir=tmp_path)
    getattr(orch.plugin_registry, "_plugins").append(
        _HandoffPlugin(PluginConfig(name="a", version="0", enabled=True), tmp_path / "a.yaml")
    )
    getattr(orch.plugin_registry, "_plugins").append(
        _BrokenHandoffPlugin(PluginConfig(name="b", version="0", enabled=True), tmp_path / "b.yaml")
    )

    states = orch.plugin_registry.get_handoff_states()

    assert states == {"s1": {"pending": True, "source": "plugin"}}


class _Factory:
    def __init__(self, fmt: str) -> None:
        self.format = fmt


class _DriverPlugin(Plugin):
    def __init__(self, config, path, factories: dict) -> None:
        super().__init__(config, path)
        self._factories = factories

    def uninstall(self) -> None:
        pass

    def get_run_driver_factories(self):
        return dict(self._factories)


def _registry_with(plugins: list[Plugin]) -> PluginRegistry:
    from workflow_controller.plugin.registry import PluginRegistry

    registry = PluginRegistry(plugins_dir=Path("/nonexistent"))
    getattr(registry, "_plugins").extend(plugins)
    return registry


def test_run_driver_factories_default_empty() -> None:
    bare = _HandoffPlugin(PluginConfig(name="x", version="0", enabled=True), None)
    assert bare.get_run_driver_factories() == {}
    assert _registry_with([bare]).run_driver_factories() == {}


def test_run_driver_factories_merge_and_duplicate_rejected(tmp_path) -> None:
    a = _DriverPlugin(
        PluginConfig(name="a", version="0", enabled=True),
        tmp_path,
        {"graph-yaml@1": _Factory("graph-yaml@1"), "skill-md": _Factory("skill-md")},
    )
    b = _DriverPlugin(
        PluginConfig(name="b", version="0", enabled=True),
        tmp_path,
        {"graph-yaml@1": _Factory("graph-yaml@1")},
    )
    merged = _registry_with([a]).run_driver_factories()
    assert set(merged) == {"graph-yaml@1", "skill-md"}
    with pytest.raises(ValueError, match="graph-yaml@1"):
        _registry_with([a, b]).run_driver_factories()


def test_plugin_services_expose_driver_factory_from_composition_root(monkeypatch) -> None:
    from workflow_controller.driver import factory
    from workflow_controller.plugin.registry import _build_plugin_services

    sentinel = object()
    monkeypatch.setattr(factory, "create_driver", lambda *args, **kwargs: sentinel)
    services = _build_plugin_services(_mock_orchestrator())

    assert services.create_driver is not None
    assert services.create_driver(runtime="claude_sdk") is sentinel


def test_plugin_services_preserve_legacy_false_thinking(tmp_path, monkeypatch) -> None:
    from workflow_controller.config.reviewer import ReviewerThinkingValues
    from workflow_controller.plugin.registry import _build_plugin_services

    captured: dict[str, object] = {}

    def fake_resolve(_project_dir, _global, *, budget_override, mode_override):
        captured["mode_override"] = mode_override
        return ReviewerThinkingValues(
            mode="disabled" if mode_override == "disabled" else "enabled",
            budget_tokens=budget_override or 2048,
        )

    monkeypatch.setattr(
        "workflow_controller.config.reviewer.resolve_reviewer_thinking",
        fake_resolve,
    )
    services = _build_plugin_services(
        _mock_orchestrator(config=AppConfig(project_dir=str(tmp_path)))
    )

    assert services.reviewer_thinking(str(tmp_path), False, 4096) == (False, 4096)
    assert captured["mode_override"] == "disabled"


@pytest.mark.parametrize(
    ("invoke", "unavailable"),
    [
        (lambda s: asyncio.run(s.start_session(None)), "session start service"),
        (lambda s: asyncio.run(s.stop_session("sid")), "session stop service"),
        (lambda s: asyncio.run(s.wait_session_done("sid")), "session wait service"),
        (lambda s: asyncio.run(s.persist_session(None)), "session persist service"),
        (lambda s: s.in_flight_subagents("sid"), "subagent tracking service"),
    ],
)
def test_session_primitive_ports_raise_when_unwired(invoke, unavailable) -> None:
    """session 原语端口未装配时报 RuntimeError，而非裸字段的 NoneType 调用错误。"""
    from workflow_controller.plugin.context import PluginServices

    with pytest.raises(RuntimeError, match=unavailable):
        invoke(PluginServices())


def test_session_primitive_ports_delegate_to_wired_impls() -> None:
    from workflow_controller.plugin.context import PluginServices

    calls: list[tuple] = []

    async def starter(config, *, compose_prompt=True):
        calls.append(("start", config, compose_prompt))
        return "session"

    async def stopper(session_id, reason="user_stop", *, persist_stopped_state=True):
        calls.append(("stop", session_id, reason, persist_stopped_state))

    services = PluginServices(session_starter=starter, session_stopper=stopper)

    assert asyncio.run(services.start_session("cfg")) == "session"
    asyncio.run(services.stop_session("sid"))
    assert asyncio.run(services.start_session("cfg2", compose_prompt=False)) == "session"
    asyncio.run(
        services.stop_session(
            "sid2",
            reason="handoff_triggered",
            persist_stopped_state=False,
        )
    )
    assert calls == [
        ("start", "cfg", True),
        ("stop", "sid", "user_stop", True),
        ("start", "cfg2", False),
        ("stop", "sid2", "handoff_triggered", False),
    ]


def test_wait_session_done_preserves_legacy_waiter_signature() -> None:
    """默认等待不向旧 session waiter 下发新增关键字。"""
    from workflow_controller.plugin.context import PluginServices
    from workflow_controller.plugin.registry import _build_plugin_services

    calls: list[tuple[str, float | None]] = []

    async def legacy_waiter(session_id: str, timeout: float | None = None) -> bool:
        calls.append((session_id, timeout))
        return True

    services = PluginServices(session_done_waiter=legacy_waiter)
    assert asyncio.run(services.wait_session_done("sid")) is True

    composed = _build_plugin_services(_mock_orchestrator(wait_session_done=legacy_waiter))
    assert asyncio.run(composed.wait_session_done("sid", timeout=10.0)) is True
    assert calls == [("sid", None), ("sid", 10.0)]


def test_orchestrator_reload_plugin_delegates_without_hook_server() -> None:
    """hook_server=None（端口被占启动失败）时 reload_plugin 仍委托 registry 卸载/加载。

    回归锚：历史实现里 ``if self._hook_server is None: return False`` 会在
    hook_server 起不来时把插件 toggle 整体短路——config.yaml 已翻 enabled、
    内存插件状态却不变，前端 tab 显隐（依赖 manifest 加载标志）脱节。
    """
    orch = Orchestrator.__new__(Orchestrator)
    calls: list[tuple[str, Any]] = []
    setattr(
        orch,
        "_plugin_registry",
        SimpleNamespace(
            reload_plugin=lambda name, o, hs: calls.append((name, hs)) or True,
        ),
    )
    setattr(orch, "_hook_server", None)

    assert orch.reload_plugin("demo") is True
    assert calls == [("demo", None)]


def test_register_hook_routes_tolerates_none_hook_server() -> None:
    """hook_server=None 时 register_hook_routes 安全跳过，不抛 AttributeError。

    插件在 hook_server 端口被占（_hook_server=None）时仍须能 install：注册
    hook 路由这一步要容错跳过，而不是 ``None.register_route`` 崩掉整个 reload。
    """
    plugin = _HandoffPlugin(PluginConfig(name="x", version="0", enabled=True), None)
    plugin.register_hook_routes(None, [("GET", "/x", lambda *a, **k: None)])
