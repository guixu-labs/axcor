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

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tests.architecture.gate_report import (
    debt_gate_summary,
    increased_entry_tail,
    stale_debt_section,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from tests.architecture.scanner import ImportRecord

DebtKey = tuple[str, str, str, str]


REGISTERED_TOP_LEVEL_PACKAGES = frozenset(
    {
        "api",
        "context_bay_exporter",
        "config",
        "core",
        "driver",
        "event",
        "hooks",
        "plugin",
        "plugins",
        "resources",
        "session",
        "store",
        "support",
        "transcript",
        "watchdog",
        "web",
    }
)


@dataclass(frozen=True)
class RuleDefinition:
    problem: str
    fix_guidance: str


DEPENDENCY_RULES = {
    "entrypoint-bypasses-core-api": RuleDefinition(
        problem="An HTTP route or headless entrypoint imports backend internals.",
        fix_guidance=(
            "Expose the required capability through workflow_controller.api, then replace the "
            "internal import with that API."
        ),
    ),
    "api-or-core-imports-concrete-plugin": RuleDefinition(
        problem="The API or core layer imports a concrete built-in plugin.",
        fix_guidance=(
            "Move shared contracts or calculations to a neutral module, or read plugin "
            "state through a public extension point."
        ),
    ),
    "plugin-sdk-imports-host-or-plugin-code": RuleDefinition(
        problem="The plugin SDK imports host internals or concrete plugin code.",
        fix_guidance=(
            "Keep workflow_controller.plugin limited to contracts; inject host capabilities through "
            "PluginContext services."
        ),
    ),
    "builtin-plugin-imports-sibling-plugin": RuleDefinition(
        problem="A built-in plugin imports another concrete built-in plugin.",
        fix_guidance=(
            "Extract shared contracts to a neutral module and communicate through "
            "services or events."
        ),
    ),
    "builtin-plugin-bypasses-service-ports": RuleDefinition(
        problem="A built-in plugin imports the API or Web layer directly.",
        fix_guidance=(
            "Use PluginContext.services; add a capability port when the required "
            "operation is not exposed."
        ),
    ),
    "runtime-layer-imports-control-plane-or-plugin": RuleDefinition(
        problem="A low-level runtime package imports the control plane or plugin code.",
        fix_guidance=(
            "Invert the dependency through a neutral protocol, event, or injected port "
            "owned by the composition root."
        ),
    ),
}


@dataclass(frozen=True, order=True)
class Violation:
    rule: str
    source: str
    line: int
    source_package: str
    target: str
    symbol: str

    @property
    def debt_key(self) -> tuple[str, str, str, str]:
        return (self.rule, self.source, self.target, self.symbol)

    def format(self) -> str:
        return (
            f"{self.source}:{self.line}: {self.rule}: {self.source_package} -> "
            f"{self.target} ({self.symbol or '-'})"
        )


def _effective_target(record: ImportRecord) -> str:
    if record.kind == "from" and record.target_module in {
        "workflow_controller",
        "workflow_controller.plugins",
    }:
        return f"{record.target_module}.{record.symbol}"
    return record.target_module


def _is_module_or_descendant(module: str, package: str) -> bool:
    return module == package or module.startswith(f"{package}.")


def _matches_any_package(module: str, packages: tuple[str, ...]) -> bool:
    return any(_is_module_or_descendant(module, package) for package in packages)


def _plugin_name(module: str) -> str | None:
    parts = module.split(".")
    if len(parts) >= 3 and parts[:2] == ["workflow_controller", "plugins"]:
        return parts[2]
    return None


def _bypasses_allowed_edges(target: str, allowed: tuple[str, ...]) -> str | None:
    """目标落在 workflow_controller 内却不属于允许边：命中入口层绕过规则。"""
    if _is_module_or_descendant(target, "workflow_controller") and not _matches_any_package(
        target, allowed
    ):
        return "entrypoint-bypasses-core-api"
    return None


def _entrypoint_rule(target: str, is_http_route: bool, is_context_bay_staging: bool) -> str | None:
    """入口层允许边判定后再决定是否命中 entrypoint 规则。"""
    if is_context_bay_staging:
        # 领域暂存包：transcript（转录定位）与 context_bay_exporter
        # （上传面）为允许边，配置经 CoreAPI 读取
        allowed = (
            "workflow_controller.api",
            "workflow_controller.context_bay_exporter",
            "workflow_controller.transcript",
            "workflow_controller.web.context_bay",
        )
    elif is_http_route:
        # 内核 HTTP 路由只经 CoreAPI 门面访问内核（含 api/http/routes 同目录互引）；
        # 应用工厂与挂载层虽在 api 包内，路由不得回导
        if _is_module_or_descendant(target, "workflow_controller.api.http.routes"):
            return None
        if _is_module_or_descendant(target, "workflow_controller.api.http"):
            return "entrypoint-bypasses-core-api"
        allowed = ("workflow_controller.api",)
    else:
        allowed = ("workflow_controller.api",)
    return _bypasses_allowed_edges(target, allowed)


def _builtin_plugin_rule(source_plugin: str, target: str) -> str | None:
    """内置插件源的依赖规则判定。"""
    target_plugin = _plugin_name(target)
    imports_plugin_namespace = target == "workflow_controller.plugins"
    if target_plugin and target_plugin != source_plugin:
        return "builtin-plugin-imports-sibling-plugin"
    if imports_plugin_namespace or _matches_any_package(
        target,
        (
            "workflow_controller.core",
            "workflow_controller.driver",
            "workflow_controller.store",
            "workflow_controller.api",
            "workflow_controller.web",
        ),
    ):
        return "builtin-plugin-bypasses-service-ports"
    return None


def _runtime_layer_rule(source: str, target: str) -> str | None:
    """低层运行时包不得反向依赖控制面或插件。"""
    if not _matches_any_package(
        source,
        (
            "workflow_controller.driver",
            "workflow_controller.event",
            "workflow_controller.hooks",
            "workflow_controller.store",
            "workflow_controller.watchdog",
        ),
    ):
        return None
    if _matches_any_package(
        target,
        (
            "workflow_controller.api",
            "workflow_controller.web",
            "workflow_controller.plugins",
        ),
    ):
        return "runtime-layer-imports-control-plane-or-plugin"
    return None


def _record_rule(record: ImportRecord) -> tuple[str | None, str]:
    """单条 import 记录的 (规则名, 归一化目标)；未命中规则时规则名为 None。"""
    target = _effective_target(record)
    source = record.source_module

    is_http_route = _is_module_or_descendant(source, "workflow_controller.api.http.routes")
    is_context_bay_staging = _is_module_or_descendant(source, "workflow_controller.web.context_bay")
    if is_http_route or is_context_bay_staging or source == "workflow_controller.headless":
        return _entrypoint_rule(target, is_http_route, is_context_bay_staging), target
    if _matches_any_package(source, ("workflow_controller.api", "workflow_controller.core")):
        if _is_module_or_descendant(target, "workflow_controller.plugins"):
            return "api-or-core-imports-concrete-plugin", target
        return None, target
    if _is_module_or_descendant(source, "workflow_controller.plugin"):
        if _matches_any_package(
            target,
            (
                "workflow_controller.core",
                "workflow_controller.api",
                "workflow_controller.web",
                "workflow_controller.plugins",
            ),
        ):
            return "plugin-sdk-imports-host-or-plugin-code", target
        return None, target
    if (source_plugin := _plugin_name(source)) is not None:
        return _builtin_plugin_rule(source_plugin, target), target
    return _runtime_layer_rule(source, target), target


def dependency_violations(records: list[ImportRecord]) -> list[Violation]:
    violations: list[Violation] = []
    for record in records:
        rule, target = _record_rule(record)
        if rule:
            violations.append(
                Violation(
                    rule=rule,
                    source=record.source,
                    line=record.line,
                    source_package=record.source_package,
                    target=target,
                    symbol=record.symbol,
                )
            )
    return sorted(violations)


def format_dependency_diagnostics(
    violations: list[Violation], expected: Mapping[DebtKey, int]
) -> str:
    actual, allowed, increased, reduced, occurrences = debt_gate_summary(violations, expected)

    lines = ["Architecture dependency gate failed."]
    if increased:
        lines.extend(
            [
                "",
                "NEW OR INCREASED VIOLATIONS — fix the dependency, do not blindly expand debt:",
            ]
        )
        for key in sorted(increased):
            rule, _source, target, symbol = key
            definition = DEPENDENCY_RULES[rule]
            locations = ", ".join(
                sorted({f"{item.source}:{item.line}" for item in occurrences[key]})
            )
            source_package = occurrences[key][0].source_package
            lines.extend(
                [
                    f"- [{rule}] {locations}",
                    f"  problem: {definition.problem}",
                    f"  dependency: {source_package} -> {target} ({symbol or '-'})",
                    *increased_entry_tail(actual, allowed, key, definition),
                ]
            )

    if reduced:
        lines.extend(
            stale_debt_section(reduced, allowed, actual, "dependency", "dependency_debt.py")
        )

    lines.extend(
        [
            "",
            "Debt baseline changes require an intentional architecture decision. "
            "Prefer repairing the dependency edge.",
        ]
    )
    return "\n".join(lines)


def unregistered_top_level_packages(package_root: Path) -> list[str]:
    actual = {
        path.name for path in package_root.iterdir() if path.is_dir() and path.name != "__pycache__"
    }
    return sorted(actual - REGISTERED_TOP_LEVEL_PACKAGES)
