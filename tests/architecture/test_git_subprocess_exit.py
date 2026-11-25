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

"""git 子进程出口收敛门禁。"""

from __future__ import annotations

import ast
import tokenize
from typing import TYPE_CHECKING

from tests.architecture.layout import source_roots

if TYPE_CHECKING:
    from pathlib import Path

# 允许直接调用 git CLI 的内核模块；其余模块须经二者 API
GIT_SUBPROCESS_EXIT_MODULES = frozenset(
    {
        "core/workspace/repository.py",
        "core/workspace/integration.py",
    }
)


def _starts_with_git(command: ast.expr) -> bool:
    """命令实参是否以 git 开头（裸串、shell 命令串或列表首元素）。"""
    if isinstance(command, ast.Constant) and isinstance(command.value, str):
        words = command.value.split()
        return bool(words) and words[0] == "git"
    return (
        isinstance(command, ast.List)
        and bool(command.elts)
        and isinstance(command.elts[0], ast.Constant)
        and command.elts[0].value == "git"
    )


def _command_expressions(node: ast.Call) -> list[ast.expr]:
    """调用中可能承载命令的实参：首位置实参与 args= 关键字实参。"""
    commands: list[ast.expr] = node.args[:1]
    commands.extend(keyword.value for keyword in node.keywords if keyword.arg == "args")
    return commands


def _scan_git_invocations(package_root: Path) -> list[str]:
    """扫描包内越界的 git 子进程调用，返回 模块:行号 清单（插件豁免）。"""
    offenders: list[str] = []
    for path in sorted(package_root.rglob("*.py")):
        relative = path.relative_to(package_root).as_posix()
        if relative.startswith("plugins/"):
            continue
        with tokenize.open(path) as source_file:
            tree = ast.parse(source_file.read(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if relative in GIT_SUBPROCESS_EXIT_MODULES:
                continue
            if any(_starts_with_git(command) for command in _command_expressions(node)):
                offenders.append(f"{relative}:{node.lineno}")
    return offenders


def test_git_subprocess_calls_confined_to_git_exit_modules() -> None:
    """内核里 git CLI 只允许出现在私有仓与集成模块，插件不在本门禁范围。"""
    offenders: list[str] = []
    for source_root in source_roots():
        offenders.extend(_scan_git_invocations(source_root / "workflow_controller"))
    assert not offenders, f"git 子进程调用越界（须经私有仓/集成模块 API）: {offenders}"


def test_git_subprocess_gate_recognizes_violation_forms(tmp_path: Path) -> None:
    """越界形态可被识别：裸串、shell 命令串、列表首元素与 args= 关键字；出口模块与插件目录豁免。"""
    package_root = tmp_path / "workflow_controller"
    offenders_dir = package_root / "core"
    offenders_dir.mkdir(parents=True)
    (offenders_dir / "offender.py").write_text(
        "import os\n"
        "import subprocess\n"
        'subprocess.run(["git", "status"])\n'
        'subprocess.Popen("git", )\n'
        'subprocess.run("git status", shell=True)\n'
        'os.system("git status")\n'
        'subprocess.run(args=["git", "fetch"])\n'
        'subprocess.run("hg status")\n'
        'subprocess.run(["hg", "status"])\n'
        'subprocess.run(args=["hg", "serve"])\n',
        encoding="utf-8",
    )
    exit_dir = package_root / "core" / "workspace"
    exit_dir.mkdir()
    (exit_dir / "repository.py").write_text('proc = run("git", "rev-parse")\n', encoding="utf-8")
    plugin_dir = package_root / "plugins" / "legacy"
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "engine.py").write_text(
        'subprocess.run(["git", "add", "-A"])\n', encoding="utf-8"
    )

    assert _scan_git_invocations(package_root) == [
        "core/offender.py:3",
        "core/offender.py:4",
        "core/offender.py:5",
        "core/offender.py:6",
        "core/offender.py:7",
    ]
