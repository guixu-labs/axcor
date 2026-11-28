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

from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


from tests.architecture.boundaries import (
    BoundaryOccurrence,
    format_boundary_diagnostics,
    scan_boundaries,
)
from tests.architecture.boundary_debt import BOUNDARY_DEBT
from tests.architecture.layout import (
    ROOT,
    builtin_plugin_py_files,
    http_routes_root,
    kernel_package_root,
    normalized_source_exists,
    scan_boundaries_normalized,
)

# 路由层禁触的内核私有符号（含字符串形态，如 getattr(orch, "_hook_server")）
_KERNEL_PRIVATE_SYMBOLS = (
    "_persist_session",
    "_deactivate_session",
    "_parse_jsonc",
    "_resolve_opencode_config_path",
    "_hook_server",
)


def _enforced_boundary_debt() -> Counter:
    """在场源码的边界债务基线；缺席源（如 workspace 布局无 web）条目免检。"""
    return Counter(
        {
            item.debt_key: item.expected
            for item in BOUNDARY_DEBT
            if normalized_source_exists(item.source)
        }
    )


def test_boundary_accesses_match_exact_debt_baseline() -> None:
    occurrences = scan_boundaries_normalized()
    actual = Counter(item.debt_key for item in occurrences)
    expected = _enforced_boundary_debt()
    assert actual == expected, format_boundary_diagnostics(occurrences, expected)


def test_http_routes_never_touch_kernel_private_symbols() -> None:
    """内核 HTTP 路由不得触碰内核私有符号，只能经 CoreAPI 访问内核。"""
    routes_root = http_routes_root()
    offenders: list[str] = []
    for path in sorted(routes_root.glob("*.py")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if any(symbol in line for symbol in _KERNEL_PRIVATE_SYMBOLS):
                offenders.append(f"{path.relative_to(ROOT)}:{lineno}")
    assert not offenders, f"routes 触碰内核私有符号: {offenders}"


def test_orchestrator_has_no_handoff_or_breather_references() -> None:
    """交接流程整族住在 breather 插件——session 平面不含交接语义与插件名。"""
    source = (kernel_package_root() / "core" / "orchestrator.py").read_text(encoding="utf-8")
    lowered = source.lower()
    assert "handoff" not in lowered, "orchestrator 不得残留交接流程语义"
    assert "breather" not in lowered, "orchestrator 不得按名引用插件"


def test_plugins_do_not_touch_orchestrator_private_members() -> None:
    """插件对宿主只走服务端口——不得伸手 orchestrator 私有成员。"""
    offenders: list[str] = []
    for path in builtin_plugin_py_files():
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if "_orchestrator._" in line:
                offenders.append(f"{path}:{lineno}")
    assert not offenders, f"插件私有直访宿主成员: {offenders}"


def _boundary_diagnostics_case() -> tuple[list[BoundaryOccurrence], Counter]:
    """诊断样例数据：超量访问、新增 shim 访问与一条过期基线。"""
    occurrences = [
        BoundaryOccurrence(
            "builtin-plugin-direct-orchestrator-access",
            "src/workflow_controller/plugins/demo/plugin.py",
            8,
            "PluginContext.orchestrator",
            "orchestrator",
        ),
        BoundaryOccurrence(
            "builtin-plugin-direct-orchestrator-access",
            "src/workflow_controller/plugins/demo/plugin.py",
            12,
            "PluginContext.orchestrator",
            "orchestrator",
        ),
        BoundaryOccurrence(
            "builtin-plugin-no-orchestrator-shim",
            "src/workflow_controller/plugins/demo/runner.py",
            20,
            "Plugin compatibility shim",
            "_orchestrator",
        ),
    ]
    expected = Counter(
        {
            occurrences[0].debt_key: 1,
            (
                "web-no-state-orchestrator",
                "src/workflow_controller/web/routes/old.py",
                "app.state.orchestrator",
                "orchestrator",
            ): 1,
        }
    )
    return occurrences, expected


def test_boundary_diagnostics_explain_new_and_stale_debt() -> None:
    occurrences, expected = _boundary_diagnostics_case()

    assert format_boundary_diagnostics(occurrences, expected) == (
        "Architecture boundary gate failed.\n"
        "\n"
        "NEW OR INCREASED VIOLATIONS — fix the access, do not blindly expand debt:\n"
        "- src/workflow_controller/plugins/demo/plugin.py:8: "
        "builtin-plugin-direct-orchestrator-access: PluginContext.orchestrator "
        "(orchestrator); src/workflow_controller/plugins/demo/plugin.py:12: "
        "builtin-plugin-direct-orchestrator-access: PluginContext.orchestrator "
        "(orchestrator)\n"
        "  problem: A built-in plugin accesses the host Orchestrator through "
        "PluginContext.\n"
        "  access: PluginContext.orchestrator (orchestrator)\n"
        "  count: actual=2, allowed=1\n"
        "  preferred fix: Use PluginContext.services; add or extend a capability "
        "port for the operation.\n"
        "- src/workflow_controller/plugins/demo/runner.py:20: "
        "builtin-plugin-no-orchestrator-shim: Plugin compatibility shim "
        "(_orchestrator)\n"
        "  problem: A built-in plugin reads a private Orchestrator compatibility "
        "shim.\n"
        "  access: Plugin compatibility shim (_orchestrator)\n"
        "  count: actual=1, allowed=0\n"
        "  preferred fix: Use PluginContext.services and remove the private "
        "Orchestrator compatibility shim.\n"
        "\n"
        "STALE OR REDUCED DEBT — the code improved; tighten the baseline:\n"
        "- [web-no-state-orchestrator] src/workflow_controller/web/routes/old.py\n"
        "  access: app.state.orchestrator (orchestrator)\n"
        "  count: baseline=1, actual=0\n"
        "  preferred fix: reduce or remove the exact entry in "
        "tests/architecture/boundary_debt.py.\n"
        "\n"
        "Boundary debt baseline changes require an intentional architecture decision. "
        "Prefer repairing the access path."
    )


def _write_web_route_sources(tmp_path: Path) -> None:
    """routes 样本：app.state.orchestrator 的直访/别名/getattr 各形态与合规访问。"""
    routes = tmp_path / "src" / "workflow_controller" / "web" / "routes"
    routes.mkdir(parents=True)
    (routes / "red.py").write_text("orch = request.app.state.orchestrator\n", encoding="utf-8")
    (routes / "getattr_red.py").write_text(
        'orch = getattr(request.app.state, "orchestrator", None)\n', encoding="utf-8"
    )
    (routes / "alias_red.py").write_text(
        "state = request.app.state\norch = state.orchestrator\n", encoding="utf-8"
    )
    (routes / "chained_alias_red.py").write_text(
        "state = request.app.state\nfoo = state\norch = foo.orchestrator\n",
        encoding="utf-8",
    )
    (routes / "alias_getattr_red.py").write_text(
        'state = request.app.state\norch = getattr(state, "orchestrator", None)\n',
        encoding="utf-8",
    )
    (routes / "getattr_var_red.py").write_text(
        "state = request.app.state\norch = getattr(state, name, None)\n", encoding="utf-8"
    )
    (routes / "const_name_red.py").write_text(
        'ATTR = "orchestrator"\nstate = request.app.state\norch = getattr(state, ATTR, None)\n',
        encoding="utf-8",
    )
    (routes / "ann_assign_red.py").write_text(
        "from typing import Any\nstate: Any = request.app.state\norch = state.orchestrator\n",
        encoding="utf-8",
    )
    (routes / "walrus_red.py").write_text(
        "if (state := request.app.state).orchestrator:\n    pass\n", encoding="utf-8"
    )
    (routes / "walrus_alias_red.py").write_text(
        "if state := request.app.state:\n    orch = state.orchestrator\n",
        encoding="utf-8",
    )
    (routes / "rebind_back_red.py").write_text(
        "state = build_state()\nstate = request.app.state\norch = state.orchestrator\n",
        encoding="utf-8",
    )
    (routes / "green.py").write_text("api = request.app.state.api\n", encoding="utf-8")


def _write_builtin_plugin_sources(tmp_path: Path) -> None:
    """plugins/demo 样本：直访宿主、shim、宿主私有成员与合规访问。"""
    plugins = tmp_path / "src" / "workflow_controller" / "plugins" / "demo"
    plugins.mkdir(parents=True)
    (plugins / "red.py").write_text("host = plugin._orchestrator\n", encoding="utf-8")
    (plugins / "direct.py").write_text("host = ctx.orchestrator\n", encoding="utf-8")
    (plugins / "self_direct.py").write_text("host = self._ctx.orchestrator\n", encoding="utf-8")
    (plugins / "host_private.py").write_text(
        "agents = plugin._orchestrator._subagents\n", encoding="utf-8"
    )
    (plugins / "ctx_host_private.py").write_text(
        "agents = ctx.orchestrator._subagents\n", encoding="utf-8"
    )
    (plugins / "param_host_private.py").write_text(
        "store = orchestrator.event_router._default_store()\n", encoding="utf-8"
    )
    (plugins / "green.py").write_text(
        "sessions = ctx.services.sessions\n"
        "agents = plugin._orchestrator_cache._subagents\n"
        "public = foo.orchestrator\n"
        "private = foo._orchestrator\n"
        "hidden = foo.orchestrator._private\n"
        'api = getattr(request.app.state, "api", None)\n'
        'shim = getattr(app, "orchestrator", None)\n',
        encoding="utf-8",
    )


def test_boundary_scanner_has_synthetic_red_and_green_cases(tmp_path: Path) -> None:
    _write_web_route_sources(tmp_path)
    _write_builtin_plugin_sources(tmp_path)
    found = scan_boundaries(tmp_path / "src" / "workflow_controller", tmp_path)
    assert [(item.source, item.rule) for item in found] == [
        (
            "src/workflow_controller/plugins/demo/ctx_host_private.py",
            "builtin-plugin-direct-orchestrator-access",
        ),
        (
            "src/workflow_controller/plugins/demo/direct.py",
            "builtin-plugin-direct-orchestrator-access",
        ),
        (
            "src/workflow_controller/plugins/demo/self_direct.py",
            "builtin-plugin-direct-orchestrator-access",
        ),
        (
            "src/workflow_controller/plugins/demo/ctx_host_private.py",
            "builtin-plugin-no-host-private-access",
        ),
        (
            "src/workflow_controller/plugins/demo/host_private.py",
            "builtin-plugin-no-host-private-access",
        ),
        (
            "src/workflow_controller/plugins/demo/param_host_private.py",
            "builtin-plugin-no-host-private-access",
        ),
        (
            "src/workflow_controller/plugins/demo/host_private.py",
            "builtin-plugin-no-orchestrator-shim",
        ),
        ("src/workflow_controller/plugins/demo/red.py", "builtin-plugin-no-orchestrator-shim"),
        ("src/workflow_controller/web/routes/alias_getattr_red.py", "web-no-state-orchestrator"),
        ("src/workflow_controller/web/routes/alias_red.py", "web-no-state-orchestrator"),
        ("src/workflow_controller/web/routes/ann_assign_red.py", "web-no-state-orchestrator"),
        ("src/workflow_controller/web/routes/chained_alias_red.py", "web-no-state-orchestrator"),
        ("src/workflow_controller/web/routes/const_name_red.py", "web-no-state-orchestrator"),
        ("src/workflow_controller/web/routes/getattr_red.py", "web-no-state-orchestrator"),
        ("src/workflow_controller/web/routes/getattr_var_red.py", "web-no-state-orchestrator"),
        ("src/workflow_controller/web/routes/rebind_back_red.py", "web-no-state-orchestrator"),
        ("src/workflow_controller/web/routes/red.py", "web-no-state-orchestrator"),
        ("src/workflow_controller/web/routes/walrus_alias_red.py", "web-no-state-orchestrator"),
        ("src/workflow_controller/web/routes/walrus_red.py", "web-no-state-orchestrator"),
    ]


def test_boundary_scanner_allows_state_aliases_for_other_attributes(tmp_path: Path) -> None:
    """别名链与常量名 getattr 只拦 orchestrator：其余 app.state 键保持可用。"""
    routes = tmp_path / "src" / "workflow_controller" / "web" / "routes"
    routes.mkdir(parents=True)
    (routes / "green.py").write_text(
        'CACHE_ATTR = "sse_snapshot_cache"\n'
        "state = request.app.state\n"
        "api = state.api\n"
        "cache = getattr(state, CACHE_ATTR, None)\n"
        'alias_routes = getattr(state, "config_section_providers", None)\n',
        encoding="utf-8",
    )
    assert scan_boundaries(tmp_path / "src" / "workflow_controller", tmp_path) == []


def test_boundary_scanner_isolates_scope_bindings(tmp_path: Path) -> None:
    """别名与常量按词法作用域解析：兄弟函数重绑、局部同名遮蔽不误报。"""
    routes = tmp_path / "src" / "workflow_controller" / "web" / "routes"
    routes.mkdir(parents=True)
    (routes / "sibling.py").write_text(
        "def a():\n"
        "    state = request.app.state\n"
        "    return state.api\n"
        "\n"
        "\n"
        "def b():\n"
        "    state = build_state()\n"
        "    return state.orchestrator\n",
        encoding="utf-8",
    )
    (routes / "shadow.py").write_text(
        "state = request.app.state\n"
        "def other():\n"
        "    state = {'orchestrator': object()}\n"
        "    return state.orchestrator\n",
        encoding="utf-8",
    )
    (routes / "const_shadow.py").write_text(
        "ATTR = 'orchestrator'\n"
        "def ok():\n"
        "    ATTR = 'settings'\n"
        "    state = request.app.state\n"
        "    return getattr(state, ATTR, None)\n",
        encoding="utf-8",
    )
    assert scan_boundaries(tmp_path / "src" / "workflow_controller", tmp_path) == []


def test_boundary_scanner_applies_last_write_wins_for_rebinding(tmp_path: Path) -> None:
    """同作用域同名末次绑定生效：别名重绑为普通对象、常量重绑为非字符串不再沿用早前绑定。"""
    routes = tmp_path / "src" / "workflow_controller" / "web" / "routes"
    routes.mkdir(parents=True)
    (routes / "alias_rebind.py").write_text(
        "def a():\n"
        "    state = request.app.state\n"
        "    state = build_state()\n"
        "    return state.orchestrator\n",
        encoding="utf-8",
    )
    (routes / "const_rebind.py").write_text(
        "def a():\n"
        "    ATTR = 'orchestrator'\n"
        "    ATTR = ['orchestrator']\n"
        "    state = request.app.state\n"
        "    return getattr(state, ATTR, None)\n",
        encoding="utf-8",
    )
    found = scan_boundaries(tmp_path / "src" / "workflow_controller", tmp_path)
    assert [(item.source, item.symbol) for item in found] == [
        ("src/workflow_controller/web/routes/const_rebind.py", "getattr"),
    ]


def test_boundary_scanner_preserves_same_line_occurrences(tmp_path: Path) -> None:
    plugins = tmp_path / "src" / "workflow_controller" / "plugins" / "demo"
    plugins.mkdir(parents=True)
    source = plugins / "duplicates.py"
    source.write_text(
        "a = plugin._orchestrator._subagents; b = plugin._orchestrator._subagents\n",
        encoding="utf-8",
    )
    found = scan_boundaries(tmp_path / "src" / "workflow_controller", tmp_path)
    assert Counter(item.rule for item in found) == Counter(
        {
            "builtin-plugin-no-host-private-access": 2,
            "builtin-plugin-no-orchestrator-shim": 2,
        }
    )


def test_boundary_scanner_honors_python_source_encoding_declarations(
    tmp_path: Path,
) -> None:
    plugins = tmp_path / "src" / "workflow_controller" / "plugins" / "demo"
    plugins.mkdir(parents=True)
    source = plugins / "encoded.py"
    source.write_bytes(
        '# -*- coding: gbk -*-\nlabel = "中文"\nhost = plugin._orchestrator\n'.encode("gbk")
    )
    found = scan_boundaries(tmp_path / "src" / "workflow_controller", tmp_path)
    assert [item.rule for item in found] == ["builtin-plugin-no-orchestrator-shim"]
