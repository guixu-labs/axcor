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

"""双仓布局发现：单仓 src 根与 workspace 多包根的统一源码定位。

单仓布局下 ``src/workflow_controller`` 同时容纳内核、插件与 web；
workspace 布局下内核住 ``packages/kernel/src``、各插件住
``packages/plugins/<name>/src``、web 不在本仓。扫描记录统一归一为
``src/...`` 虚拟路径，债务基线在两种布局下保持同键；不在本仓的源码面
（如 web、graph 插件）按布局缺席，调用方跳过对应检查。
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from tests.architecture.boundaries import scan_boundaries
from tests.architecture.scanner import scan_tree

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.architecture.boundaries import BoundaryOccurrence
    from tests.architecture.scanner import ImportRecord

ROOT = Path(__file__).resolve().parents[2]


def source_roots() -> list[Path]:
    """全部源码根（含 workflow_controller 的 src 目录），内核在前、插件按名序。"""
    single = ROOT / "src"
    if (single / "workflow_controller").is_dir():
        return [single]
    kernel = ROOT / "packages" / "kernel" / "src"
    return [kernel, *sorted((ROOT / "packages" / "plugins").glob("*/src"))]


def kernel_package_root() -> Path:
    """内核 workflow_controller 包根（api/core/... 所在）。"""
    return source_roots()[0] / "workflow_controller"


def plugin_package_dir(name: str) -> Path | None:
    """按名取内置插件包目录；不在本仓的布局返回 None。"""
    for root in source_roots():
        candidate = root / "workflow_controller" / "plugins" / name
        if candidate.is_dir():
            return candidate
    return None


def http_routes_root() -> Path:
    """内核 HTTP 路由目录（api/http/routes）。"""
    return kernel_package_root() / "api" / "http" / "routes"


def hookserver_package_root() -> Path | None:
    """hookserver 包根；不在本仓的布局（如导出子集）返回 None。"""
    for candidate in (
        ROOT
        / "packages"
        / "workflow-controller-hookserver"
        / "src"
        / "workflow_controller_hookserver",
        ROOT / "packages" / "hookserver" / "src" / "workflow_controller_hookserver",
    ):
        if candidate.is_dir():
            return candidate
    return None


def builtin_plugin_py_files() -> Iterator[Path]:
    """全部内置插件源码文件（单仓布局在内核包内，workspace 布局在各插件包）。"""
    for root in source_roots():
        plugins_root = root / "workflow_controller" / "plugins"
        if plugins_root.is_dir():
            yield from sorted(plugins_root.rglob("*.py"))


def tree_roots() -> list[Path]:
    """全部测试树根（单仓布局仅仓根 tests，workspace 布局含各包测试根）。"""
    candidates = [
        ROOT / "tests",
        ROOT / "packages" / "kernel" / "kernel_tests",
        *sorted((ROOT / "packages" / "plugins").glob("*/tests")),
    ]
    return [path for path in candidates if path.is_dir()]


def normalized_virtual_path(path: Path, source_root: Path) -> str:
    """物理文件路径归一为 src/... 虚拟路径（两种布局同键）。"""
    return "src/" + path.resolve().relative_to(source_root.resolve()).as_posix()


def normalized_source_exists(normalized: str) -> bool:
    """归一化 src/... 路径在本布局是否存在。"""
    if not normalized.startswith("src/"):
        return False
    relative = Path(normalized[len("src/") :])
    return any((root / relative).exists() for root in source_roots())


def scan_imports_normalized() -> list[ImportRecord]:
    """多源码根 import 扫描，记录路径归一为 src/...。"""
    records: list[ImportRecord] = []
    for root in source_roots():
        for record in scan_tree(root):
            physical = root.parent / record.source
            records.append(replace(record, source=normalized_virtual_path(physical, root)))
    return records


def scan_boundaries_normalized() -> list[BoundaryOccurrence]:
    """多源码根边界扫描，记录路径归一为 src/...。"""
    occurrences: list[BoundaryOccurrence] = []
    for root in source_roots():
        for occurrence in scan_boundaries(root / "workflow_controller", root):
            physical = root / occurrence.source
            occurrences.append(replace(occurrence, source=normalized_virtual_path(physical, root)))
    return occurrences
