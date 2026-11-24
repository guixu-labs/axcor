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

"""测试树解耦门禁：约束内核/插件/共享测试树的 import 归属，防依赖回流。"""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

from tests.architecture.layout import ROOT

if TYPE_CHECKING:
    from pathlib import Path

_TESTS = ROOT / "tests"

# 内核测试住 packages/kernel/kernel_tests，插件测试随包，共享基建住仓根 tests/
_KERNEL_TESTS_PACKAGE = ROOT / "packages" / "kernel" / "kernel_tests"
_PLUGINS_PACKAGE_ROOT = ROOT / "packages" / "plugins"
_SHARED_TEST_DIRS = ("support", "architecture")

# 内核测试声明的测试依赖插件（headless 验收驱动）
_KERNEL_ALLOWED_PLUGINS = {"fake_driver"}


def _absolute_imports(path: Path) -> set[str]:
    """收集文件内绝对 import 的模块全名（相对导入不在其列）。"""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None and node.level == 0:
            modules.add(node.module)
    return modules


def _py_files_under(directory: Path) -> list[Path]:
    return sorted(path for path in directory.rglob("*.py") if "__pycache__" not in path.parts)


def _kernel_test_py_files() -> list[Path]:
    """内核侧测试文件：包内测试根 + 仓根共享基建。"""
    files = _py_files_under(_KERNEL_TESTS_PACKAGE)
    for dirname in _SHARED_TEST_DIRS:
        files.extend(_py_files_under(_TESTS / dirname))
    return files


def _plugin_test_py_files() -> list[Path]:
    """插件侧测试文件：随包目录。"""
    files: list[Path] = []
    for plugin_tests in sorted(_PLUGINS_PACKAGE_ROOT.glob("*/tests")):
        files.extend(_py_files_under(plugin_tests))
    return files


def test_kernel_tests_do_not_import_production_plugins() -> None:
    """内核侧测试不得 import 生产插件；fake_driver 是声明的测试依赖例外。"""
    violations = []
    for path in _kernel_test_py_files():
        for module in _absolute_imports(path):
            if module.startswith("workflow_controller.plugins."):
                plugin = module.split(".")[2]
                if plugin not in _KERNEL_ALLOWED_PLUGINS:
                    violations.append(f"{path.relative_to(ROOT)}: import {module}")
    assert not violations, "内核测试出现生产插件依赖：\n" + "\n".join(violations)


def test_plugin_test_dirs_are_self_contained() -> None:
    """插件测试目录不得使用 tests.* 绝对导入（拆包后路径失效）。"""
    violations = []
    for path in _plugin_test_py_files():
        for module in _absolute_imports(path):
            if module == "tests" or module.startswith("tests."):
                violations.append(f"{path.relative_to(ROOT)}: import {module}")
    assert not violations, "插件测试目录存在 tests.* 绝对导入：\n" + "\n".join(violations)
