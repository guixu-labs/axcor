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

"""内核领域词汇与源码明文凭据门禁。

词汇白名单沿用债务基线模式：精确到文件与计数，只减不增——代码改善后必须
同步收紧白名单，新增命中直接失败。
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

from tests.architecture.layout import (
    ROOT,
    builtin_plugin_py_files,
    kernel_package_root,
    normalized_source_exists,
    normalized_virtual_path,
    source_roots,
)

PACKAGE_ROOT = kernel_package_root()

# 内核目录与文件（包归属口径：不含 web / plugins / context_bay_exporter）
KERNEL_DIRS = (
    "api",
    "core",
    "driver",
    "event",
    "hooks",
    "plugin",
    "session",
    "store",
    "support",
    "watchdog",
    "config",
    "transcript",
)
KERNEL_FILES = ("__main__.py", "headless.py", "logging_config.py")

# 领域词汇：context_bay / cannbot / 独立成词的 CANN
DOMAIN_TERM_PATTERN = re.compile(r"context_bay|cannbot|\bcann\b", re.IGNORECASE)

# 词汇白名单：文件 → 允许命中次数；条目仅在源文件在场时参与互锁（导出子集免检）
DOMAIN_TERM_WHITELIST = {
    # 跨系统格式标识（meta 包装行键），转录编码中立化后仅剩此一处
    "src/workflow_controller/core/run_export.py": 1,
    # 跨系统格式标识 x_context_bay（docstring + 行键），claude-jsonl 写出轨的元数据键
    "src/workflow_controller/transcript/claude_jsonl_out.py": 2,
    # agent_traceback 读 cannbot session db 的外部格式标识（插件成包入扫描面后生效）
    "src/workflow_controller/plugins/agent_traceback/claude_session_seed.py": 1,
    "src/workflow_controller/plugins/agent_traceback/plugin.py": 1,
    "src/workflow_controller/plugins/agent_traceback/session_reconstructor/__init__.py": 2,
    "src/workflow_controller/plugins/agent_traceback/session_reconstructor/db_reader.py": 9,
    "src/workflow_controller/plugins/agent_traceback/session_reconstructor/l2_transcriber.py": 3,
    "src/workflow_controller/plugins/agent_traceback/session_reconstructor/orchestrator.py": 7,
    "src/workflow_controller/plugins/agent_traceback/web_api.py": 2,
}

# 任意协议 URL 中的 user:pass@ 明文凭据
CREDENTIAL_URL_PATTERN = re.compile(r"[a-z][a-z0-9+.\-]*://[^\s/@:]+:[^\s/@]+@", re.IGNORECASE)

# 第三方与产物目录不入扫描（node_modules 为前端 vendored 依赖）
EXCLUDED_SCAN_DIRS = frozenset({"__pycache__", "node_modules", "dist"})


def _strip_license_header(text: str) -> str:
    """去掉文件头部的许可证样板注释块（法律声明文本，非领域逻辑）。

    识别口径：shebang 之后，由连续 `#`/`//` 行注释与空行组成的首段，
    且其中含 "Copyright (c)"。许可证名称（如 CANN Open Software License
    Agreement）出现在该块内不计入领域词汇。
    """
    lines = text.splitlines(keepends=True)
    start = 1 if lines and lines[0].startswith("#!") else 0
    block_end = start
    while block_end < len(lines):
        stripped = lines[block_end].strip()
        if not stripped or stripped.startswith(("#", "//")):
            block_end += 1
        else:
            break
    block = "".join(lines[start:block_end])
    if "Copyright (c)" in block:
        return "".join(lines[block_end:])
    return text


def _kernel_py_files() -> Iterator[Path]:
    for name in KERNEL_FILES:
        path = PACKAGE_ROOT / name
        if path.is_file():
            yield path
    for dirname in KERNEL_DIRS:
        yield from (PACKAGE_ROOT / dirname).rglob("*.py")


_HEADER_MIT = (
    "# Copyright (c) 2026 Workflow Controller Contributors\n#\n# Licensed under the MIT License.\n"
)
_HEADER_CANN = (
    "# Copyright (c) 2026 CANN Authors\n"
    "#\n"
    "# CANN Open Software License Agreement V1.0\n"
    "# Use of this source code is governed by the CANN Open Software License Agreement.\n"
)
_PLAIN_BODY = '"""模块说明。"""\n\nTERM = "context_bay"\n'


def _normalized_key(path: Path) -> str:
    """扫描文件的归一化 src/... 键（多源码根统一键）。"""
    for root in source_roots():
        if path.resolve().is_relative_to(root.resolve()):
            return normalized_virtual_path(path, root)
    raise ValueError(f"文件不在任何源码根下: {path}")


def _plugin_py_files() -> Iterator[tuple[Path, str]]:
    """插件源码扫描面（各插件包）。"""
    for path in builtin_plugin_py_files():
        yield path, _normalized_key(path)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        pytest.param("", _PLAIN_BODY, id="no-header"),
        pytest.param(_HEADER_MIT, _PLAIN_BODY, id="mit"),
        pytest.param(_HEADER_CANN, _PLAIN_BODY, id="cann"),
        pytest.param("#!/usr/bin/env python3\n" + _HEADER_CANN, _PLAIN_BODY, id="shebang-cann"),
        pytest.param(
            "# 仅普通注释，无版权行\n",
            "# 仅普通注释，无版权行\n" + _PLAIN_BODY,
            id="no-copyright",
        ),
    ],
)
def test_strip_license_header_removes_license_block(header: str, expected: str) -> None:
    assert _strip_license_header(header + _PLAIN_BODY) == expected


def test_kernel_domain_vocabulary_matches_exact_whitelist() -> None:
    hits: dict[str, int] = {}
    scanned: Iterator[tuple[Path, str]] = (
        *((path, _normalized_key(path)) for path in _kernel_py_files()),
        *_plugin_py_files(),
    )
    for path, key in scanned:
        text = _strip_license_header(path.read_text(encoding="utf-8"))
        count = len(DOMAIN_TERM_PATTERN.findall(text))
        if count:
            hits[key] = count

    # 插件路径白名单条目仅在源文件在场时参与互锁（导出子集缺该插件时条目免检）
    active_whitelist = {
        key: allowed
        for key, allowed in DOMAIN_TERM_WHITELIST.items()
        if normalized_source_exists(key)
    }
    drifted = sorted(set(hits) | set(active_whitelist))
    details = [
        f"{path}: actual={hits.get(path, 0)}, allowed={active_whitelist.get(path, 0)}"
        for path in drifted
        if hits.get(path, 0) != active_whitelist.get(path, 0)
    ]
    assert not details, (
        "内核领域词汇门禁失败（白名单只减不增，新增词汇移到领域侧或收紧白名单）：\n"
        + "\n".join(details)
    )


def test_source_tree_has_no_embedded_credentials() -> None:
    offenders = []
    for source_root in source_roots():
        for path in sorted((source_root / "workflow_controller").rglob("*")):
            if not path.is_file() or EXCLUDED_SCAN_DIRS & set(path.parts):
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if CREDENTIAL_URL_PATTERN.search(text):
                offenders.append(str(path.relative_to(ROOT)))
    assert not offenders, f"源码不得保留明文凭据 URL（改用环境变量或本机凭据助手）：{offenders}"
