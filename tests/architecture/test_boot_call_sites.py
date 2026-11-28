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

"""生产 boot() 调用点门禁 — 本仓只允许内核 HTTP 应用与 Headless 两个组合根调用。

控制台 Web 组合根住控制台发行包（保留自己的 boot() 调用，见 ADR-0002），
不在本仓扫描面内。
"""

from __future__ import annotations

import ast

from tests.architecture.layout import source_roots

BOOT_MODULE = "workflow_controller.api.core_api"

# 本仓允许调用 boot() 的生产组合根（归一化 src/... 路径）
BOOT_CALL_SITES = frozenset(
    {
        "src/workflow_controller/api/http/app.py",
        "src/workflow_controller/headless.py",
    }
)


def _dotted_name(node: ast.AST | None) -> str | None:
    """递归解析 Name/Attribute 链为点分名。"""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else None
    return None


def _boot_bound_names(tree: ast.Module) -> set[str]:
    """模块内可调用的 boot 绑定名（直接导入、core_api 属性与模块属性形）。"""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == BOOT_MODULE:
                names.update(alias.asname or "boot" for alias in node.names if alias.name == "boot")
            elif node.module == "workflow_controller.api":
                names.update(
                    f"{alias.asname or 'core_api'}.boot"
                    for alias in node.names
                    if alias.name == "core_api"
                )
        elif isinstance(node, ast.Import):
            names.update(
                f"{alias.asname or alias.name}.boot"
                for alias in node.names
                if alias.name == BOOT_MODULE
            )
    return names


def _boot_call_sites(tree: ast.Module) -> list[int]:
    names = _boot_bound_names(tree)
    if not names:
        return []
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _dotted_name(node.func) in names
    ]


def test_production_boot_call_sites_are_pinned() -> None:
    callers = {}
    for source_root in source_roots():
        for path in sorted(source_root.rglob("*.py")):
            if lines := _boot_call_sites(ast.parse(path.read_text(encoding="utf-8"))):
                callers["src/" + path.relative_to(source_root).as_posix()] = lines
    unexpected = set(callers) - BOOT_CALL_SITES
    missing = BOOT_CALL_SITES - set(callers)
    assert not unexpected and not missing, (
        f"boot() 生产调用点必须与登记的组合根一致："
        f"新增={sorted(unexpected)}，缺失={sorted(missing)}"
    )
