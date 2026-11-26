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

"""架构门禁：main code 禁止字符串形态访问受保护成员。

字符串成员名形态（getattr/setattr/hasattr/delattr）在 main code
（src/ 与 packages/）中会绕过静态检查对受保护成员访问的拦截，掩盖
真实的模块边界破坏；该形态仅作为 tests/ 与 examples/ 测试桩的惯例
保留。外部确需访问受保护成员时，应在所属类提供公有成员函数或
@property。

口径：AST 扫描 getattr/setattr/hasattr/delattr 调用，第二个参数是
以单下划线开头的字符串字面量（dunder 排除）。既有清单冻结为棘轮
基线（只减不增）；main code 的新增用法一律失败。
"""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

from tests.architecture.layout import (
    hookserver_package_root,
    kernel_package_root,
    source_roots,
)

if TYPE_CHECKING:
    from pathlib import Path

_FUNC_NAMES = {"getattr", "setattr", "hasattr", "delattr"}

# 棘轮基线（file, member）：历史遗留的防御式读取（可选属性缺省回退、
# 框架对象兼容），并非边界逃逸；只减不增，条目清零后删除。
BASELINE: set[tuple[str, str]] = {
    ("core/orchestrator.py", "_session_reviewers"),
    ("core/orchestrator.py", "_loops"),
    ("core/orchestrator.py", "_single_loop"),
    ("core/orchestrator.py", "_reconcile_interval"),
    ("plugins/file_provenance/plugin.py", "_ctx"),
    ("plugins/breather/context.py", "_auth_rotator"),
    ("driver/claude_sdk.py", "_process"),
    ("web/console.py", "_mark_routes_changed"),
    ("api/session_api.py", "_error"),
    ("api/session_api.py", "_orch"),
}


def _scan_roots() -> list[tuple[Path, str]]:
    """(扫描根, 基线键前缀)：内核与 hookserver 无前缀，插件包带 plugins/ 前缀。

    hookserver 缺席的布局（导出子集不含该包）跳过对应扫描根。
    """
    roots = [(kernel_package_root(), "")]
    hookserver_root = hookserver_package_root()
    if hookserver_root is not None:
        roots.append((hookserver_root, ""))
    for source_root in source_roots()[1:]:
        plugins_dir = source_root / "workflow_controller" / "plugins"
        if plugins_dir.is_dir():
            roots.append((plugins_dir, "plugins/"))
    return roots


def _baseline_file_present(relative_key: str) -> bool:
    """基线条目的源文件在本布局是否存在（缺席源的条目免检）。"""
    for root, prefix in _scan_roots():
        if prefix and relative_key.startswith(prefix):
            if (root / relative_key[len(prefix) :]).is_file():
                return True
        elif not prefix and (root / relative_key).is_file():
            return True
    return False


def _is_dunder(name: str) -> bool:
    return len(name) > 4 and name.startswith("__") and name.endswith("__")


def _all_violations() -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for root, prefix in _scan_roots():
        for path in sorted(root.rglob("*.py")):
            rel = prefix + str(path.relative_to(root))
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
                    continue
                if node.func.id not in _FUNC_NAMES or len(node.args) < 2:
                    continue
                arg = node.args[1]
                if not isinstance(arg, ast.Constant) or not isinstance(arg.value, str):
                    continue
                member = arg.value
                if member.startswith("_") and not _is_dunder(member):
                    out.add((rel, member))
    return out


def test_no_new_protected_member_escape_in_main_code() -> None:
    current = _all_violations()
    new_violations = current - BASELINE
    assert not new_violations, (
        "main code 新增了字符串形态的受保护成员访问（getattr/setattr/...）：\n"
        + "\n".join(f"  {f}: _{m}" for f, m in sorted(new_violations))
        + "\n请在所属类中提供公有成员函数或 @property，"
        "字符串逃逸形态仅限 tests/ 与 examples/。"
    )


def test_baseline_entries_still_exist() -> None:
    """棘轮只减不增的另一侧：基线条目清零后应从 BASELINE 删除。"""
    current = _all_violations()
    enforced = {entry for entry in BASELINE if _baseline_file_present(entry[0])}
    stale = enforced - current
    assert not stale, "以下基线条目已不存在，请从 BASELINE 删除（只减不增）：\n" + "\n".join(
        f"  {f}: _{m}" for f, m in sorted(stale)
    )
