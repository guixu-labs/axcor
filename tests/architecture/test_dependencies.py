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

import ast
import tokenize
from collections import Counter
from pathlib import Path

import pytest

from tests.architecture.dependency_debt import DEPENDENCY_DEBT
from tests.architecture.layout import (
    kernel_package_root,
    normalized_source_exists,
    scan_imports_normalized,
    source_roots,
)
from tests.architecture.rules import (
    Violation,
    dependency_violations,
    format_dependency_diagnostics,
    unregistered_top_level_packages,
)
from tests.architecture.scanner import scan_imports

# 动态导入白名单：仅插件装载器单点（路径为归一化 src/... 形式）
PLUGIN_LOADER_SOURCE = "src/workflow_controller/plugin/registry.py"


def test_forbidden_dependencies_match_exact_debt_baseline() -> None:
    records = scan_imports_normalized()
    invalid_sources = sorted(
        {item.source for item in records if not item.source.startswith("src/")}
    )
    assert not invalid_sources, f"Scanner source paths must use src/...: {invalid_sources}"

    violations = dependency_violations(records)
    actual = Counter(item.debt_key for item in violations)
    expected = Counter(
        {
            item.debt_key: item.expected
            for item in DEPENDENCY_DEBT
            if normalized_source_exists(item.source)
        }
    )
    if actual != expected:
        raise AssertionError(format_dependency_diagnostics(violations, expected))


def test_new_top_level_packages_require_role_registration() -> None:
    unregistered = unregistered_top_level_packages(kernel_package_root())
    assert not unregistered, f"Register package roles before adding them: {unregistered}"


def test_dynamic_imports_are_confined_to_the_plugin_loader() -> None:
    """运行时动态导入（__import__ / import_module）只允许插件装载器单点。"""
    offenders: list[str] = []
    for source_root in source_roots():
        package_root = source_root / "workflow_controller"
        for path in sorted(package_root.rglob("*.py")):
            relative = "src/" + path.relative_to(source_root).as_posix()
            with tokenize.open(path) as source_file:
                tree = ast.parse(source_file.read(), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                is_dynamic_import = (
                    isinstance(func, ast.Name) and func.id in ("__import__", "import_module")
                ) or (isinstance(func, ast.Attribute) and func.attr == "import_module")
                if is_dynamic_import and relative != PLUGIN_LOADER_SOURCE:
                    offenders.append(f"{relative}:{node.lineno}")
    assert not offenders, f"动态导入仅限插件装载器 {PLUGIN_LOADER_SOURCE}: {offenders}"


def test_scanner_recognizes_static_relative_and_dynamic_imports(tmp_path: Path) -> None:
    package = tmp_path / "workflow_controller" / "sample"
    package.mkdir(parents=True)
    source = package / "module.py"
    source.write_text(
        """
import workflow_controller.core
import importlib
from .helpers import Thing
from ..driver import factory
__import__("workflow_controller.store.event_store")
importlib.import_module("workflow_controller.plugins.demo")
""",
        encoding="utf-8",
    )
    records = scan_imports(source, tmp_path)
    targets = {(item.kind, item.target_module, item.symbol) for item in records}
    expected_targets = {
        ("import", "workflow_controller.core", "workflow_controller.core"),
        ("from", "workflow_controller.sample.helpers", "Thing"),
        ("from", "workflow_controller.driver", "factory"),
        ("__import__", "workflow_controller.store.event_store", ""),
        ("importlib", "workflow_controller.plugins.demo", ""),
    }
    missing = expected_targets - targets
    assert not missing, f"Scanner did not recognize imports: {sorted(missing)}"


def test_scanner_ignores_type_checking_imports(tmp_path: Path) -> None:
    source = tmp_path / "workflow_controller" / "api" / "sample.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from workflow_controller.plugins.demo import Demo\n",
        encoding="utf-8",
    )
    violations = dependency_violations(scan_imports(source, tmp_path))
    assert not violations, f"TYPE_CHECKING imports must be ignored: {violations}"


def test_scanner_checks_compound_type_checking_conditions(tmp_path: Path) -> None:
    source = tmp_path / "workflow_controller" / "api" / "sample.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING or runtime_flag:\n"
        "    from workflow_controller.plugins.demo import Demo\n",
        encoding="utf-8",
    )
    violations = dependency_violations(scan_imports(source, tmp_path))
    assert [item.rule for item in violations] == ["api-or-core-imports-concrete-plugin"]


def test_scanner_honors_python_source_encoding_declarations(tmp_path: Path) -> None:
    source = tmp_path / "workflow_controller" / "api" / "sample.py"
    source.parent.mkdir(parents=True)
    source.write_bytes(
        '# -*- coding: gbk -*-\n"中文"\nimport workflow_controller.core\n'.encode("gbk")
    )
    targets = {item.target_module for item in scan_imports(source, tmp_path)}
    assert "workflow_controller.core" in targets


@pytest.mark.parametrize(
    ("relative_path", "source_text", "expected_rule"),
    [
        (
            "workflow_controller/api/http/routes/__init__.py",
            "from workflow_controller.api.http.app import create_app\n",
            "entrypoint-bypasses-core-api",
        ),
        (
            "workflow_controller/api/http/routes/sample.py",
            "from workflow_controller.core.orchestrator import Orchestrator\n",
            "entrypoint-bypasses-core-api",
        ),
        (
            "workflow_controller/api/http/routes/sample.py",
            "from workflow_controller.api.http.mounting import register_kernel_routes\n",
            "entrypoint-bypasses-core-api",
        ),
        (
            "workflow_controller/web/context_bay/sample.py",
            "from workflow_controller.config.settings import AppConfig\n",
            "entrypoint-bypasses-core-api",
        ),
        (
            "workflow_controller/api/__init__.py",
            "from workflow_controller.plugins import breather\n",
            "api-or-core-imports-concrete-plugin",
        ),
        (
            "workflow_controller/plugin/__init__.py",
            "from workflow_controller.core import Orchestrator\n",
            "plugin-sdk-imports-host-or-plugin-code",
        ),
        (
            "workflow_controller/plugins/graph/__init__.py",
            "from workflow_controller.plugins import breather\n",
            "builtin-plugin-imports-sibling-plugin",
        ),
        (
            "workflow_controller/plugins/graph/plugin.py",
            "from workflow_controller.driver.factory import create_driver\n",
            "builtin-plugin-bypasses-service-ports",
        ),
        (
            "workflow_controller/driver/__init__.py",
            "from workflow_controller.api import CoreAPI\n",
            "runtime-layer-imports-control-plane-or-plugin",
        ),
    ],
)
def test_each_dependency_rule_has_a_synthetic_red_case(
    tmp_path: Path,
    relative_path: str,
    source_text: str,
    expected_rule: str,
) -> None:
    source = tmp_path / relative_path
    source.parent.mkdir(parents=True)
    source.write_text(source_text, encoding="utf-8")
    violations = dependency_violations(scan_imports(source, tmp_path))
    actual_rules = [item.rule for item in violations]
    assert len(actual_rules) == 1, f"Expected one synthetic violation, got {actual_rules!r}"
    assert actual_rules[0] == expected_rule, (
        f"Expected synthetic violation {expected_rule!r}, got {actual_rules[0]!r}"
    )


@pytest.mark.parametrize(
    ("relative_path", "source_text"),
    [
        (
            "workflow_controller/api/http/routes/sample.py",
            "from workflow_controller.api import CoreAPI\n",
        ),
        (
            "workflow_controller/api/http/routes/sample.py",
            "from workflow_controller.api.http.routes import sse_routes\n",
        ),
        (
            "workflow_controller/api/http/routes/common.py",
            "from workflow_controller.api.http.routes.sessions_routes import start_session\n",
        ),
        (
            "workflow_controller/api/http/routes/sample.py",
            "from workflow_controller.api.core_api import CoreAPI\n",
        ),
        (
            "workflow_controller/api/http/sample.py",
            "from workflow_controller.config.settings import AppConfig\n",
        ),
        (
            "workflow_controller/headless.py",
            "from workflow_controller.api.core_api import boot\n",
        ),
        (
            "workflow_controller/web/context_bay/sample.py",
            "from workflow_controller.api.core_api import CoreAPI\n",
        ),
        (
            "workflow_controller/web/context_bay/sample.py",
            "from workflow_controller.transcript.sources import resolve_transcript_source\n",
        ),
        (
            "workflow_controller/web/context_bay/sample.py",
            "from workflow_controller.context_bay_exporter import export_session_to_context_bay_db\n",
        ),
        (
            "workflow_controller/web/context_bay/sample.py",
            "from workflow_controller.web.context_bay.service import ContextBayConsoleService\n",
        ),
        (
            "workflow_controller/api/sample.py",
            "from workflow_controller.session.events import Event\n",
        ),
        ("workflow_controller/plugin/sample.py", "from dataclasses import dataclass\n"),
        (
            "workflow_controller/plugins/graph/sample.py",
            "from workflow_controller.plugins.graph import dag\n",
        ),
        (
            "workflow_controller/plugins/graph/events.py",
            "from workflow_controller.session.events import Event\n",
        ),
        (
            "workflow_controller/driver/sample.py",
            "from workflow_controller.session.events import Event\n",
        ),
    ],
)
def test_each_dependency_rule_has_a_synthetic_green_case(
    tmp_path: Path,
    relative_path: str,
    source_text: str,
) -> None:
    source = tmp_path / relative_path
    source.parent.mkdir(parents=True)
    source.write_text(source_text, encoding="utf-8")
    violations = dependency_violations(scan_imports(source, tmp_path))
    assert not violations, f"Expected allowed dependency, got: {violations}"


def test_plugin_namespace_import_is_normalized_to_concrete_plugin(tmp_path: Path) -> None:
    source = tmp_path / "workflow_controller" / "api" / "sample.py"
    source.parent.mkdir(parents=True)
    source.write_text("from workflow_controller.plugins import breather\n", encoding="utf-8")
    violations = dependency_violations(scan_imports(source, tmp_path))
    actual = [(item.rule, item.target, item.symbol) for item in violations]
    expected = [
        ("api-or-core-imports-concrete-plugin", "workflow_controller.plugins.breather", "breather")
    ]
    assert actual == expected, f"Plugin namespace import was not normalized: {actual}"


def test_package_matching_does_not_confuse_common_prefixes(tmp_path: Path) -> None:
    source = tmp_path / "workflow_controller" / "plugin" / "sample.py"
    source.parent.mkdir(parents=True)
    source.write_text("import workflow_controller.apiary\n", encoding="utf-8")
    violations = dependency_violations(scan_imports(source, tmp_path))
    assert not violations, f"Common module prefix caused a false positive: {violations}"


def test_plugin_namespace_is_not_treated_as_a_builtin_plugin(tmp_path: Path) -> None:
    source = tmp_path / "workflow_controller" / "plugins" / "__init__.py"
    source.parent.mkdir(parents=True)
    source.write_text("from workflow_controller.plugins import graph\n", encoding="utf-8")
    violations = dependency_violations(scan_imports(source, tmp_path))
    assert not violations, f"Plugin namespace was treated as a built-in plugin: {violations}"


def _mismatch_diagnostics_case() -> tuple[list[Violation], dict[tuple[str, str, str, str], int]]:
    """诊断样例数据：一条不变、一条翻倍、一条新增的违规与一条过期基线。"""
    unchanged = Violation(
        "entrypoint-bypasses-core-api",
        "src/unchanged.py",
        10,
        "workflow_controller.web",
        "workflow_controller.core",
        "Core",
    )
    increased = Violation(
        "api-or-core-imports-concrete-plugin",
        "src/increased.py",
        20,
        "workflow_controller.api",
        "workflow_controller.plugins.demo",
        "Demo",
    )
    new = Violation(
        "runtime-layer-imports-control-plane-or-plugin",
        "src/new.py",
        30,
        "workflow_controller.driver",
        "workflow_controller.api",
        "CoreAPI",
    )
    stale_key = (
        "builtin-plugin-bypasses-service-ports",
        "src/stale.py",
        "workflow_controller.api",
        "CoreAPI",
    )
    expected = {unchanged.debt_key: 1, increased.debt_key: 1, stale_key: 1}
    return [unchanged, increased, increased, new], expected


def test_dependency_mismatch_reports_only_actionable_differences() -> None:
    violations, expected = _mismatch_diagnostics_case()

    message = format_dependency_diagnostics(violations, expected)

    expected_message = "\n".join(
        [
            "Architecture dependency gate failed.",
            "",
            "NEW OR INCREASED VIOLATIONS — fix the dependency, do not blindly expand debt:",
            "- [api-or-core-imports-concrete-plugin] src/increased.py:20",
            "  problem: The API or core layer imports a concrete built-in plugin.",
            "  dependency: workflow_controller.api -> workflow_controller.plugins.demo (Demo)",
            "  count: actual=2, allowed=1",
            "  preferred fix: Move shared contracts or calculations to a neutral module, "
            "or read plugin state through a public extension point.",
            "- [runtime-layer-imports-control-plane-or-plugin] src/new.py:30",
            "  problem: A low-level runtime package imports the control plane or plugin code.",
            "  dependency: workflow_controller.driver -> workflow_controller.api (CoreAPI)",
            "  count: actual=1, allowed=0",
            "  preferred fix: Invert the dependency through a neutral protocol, event, or "
            "injected port owned by the composition root.",
            "",
            "STALE OR REDUCED DEBT — the code improved; tighten the baseline:",
            "- [builtin-plugin-bypasses-service-ports] src/stale.py",
            "  dependency: workflow_controller.api (CoreAPI)",
            "  count: baseline=1, actual=0",
            "  preferred fix: reduce or remove the exact entry in "
            "tests/architecture/dependency_debt.py.",
            "",
            "Debt baseline changes require an intentional architecture decision. "
            "Prefer repairing the dependency edge.",
        ]
    )
    assert message == expected_message


def test_architecture_tests_do_not_use_runtime_import_introspection() -> None:
    def dotted_name(node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            prefix = dotted_name(node.value)
            return f"{prefix}.{node.attr}" if prefix else node.attr
        return None

    forbidden_calls = {
        "__import__",
        "find_loader",
        "find_spec",
        "get_loader",
        "import_module",
        "importlib.import_module",
        "importlib.util.find_spec",
        "importlib.util.module_from_spec",
        "module_from_spec",
        "pkgutil.find_loader",
        "pkgutil.get_loader",
    }
    forbidden_accesses = {"sys.modules"}
    for path in Path(__file__).parent.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        calls: set[str] = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if (name := dotted_name(node.func)) is None:
                continue
            calls.add(name)
        accesses: set[str] = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute):
                continue
            if (name := dotted_name(node)) is None:
                continue
            accesses.add(name)
        forbidden = (calls & forbidden_calls) | (accesses & forbidden_accesses)
        assert not forbidden, f"{path} must stay source-only: {sorted(forbidden)}"
