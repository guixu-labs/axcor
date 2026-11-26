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

"""重放种子的路径重写：源 cwd → fork cwd（结构化 JSON walk + 规范化比对）。"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

__all__ = [
    "rewrite_content_string",
    "rewrite_part_data",
    "rewrite_paths",
]

# 路径字段白名单：值是绝对路径，做 resolve + prefix-match 替换为 fork_cwd/rel
_PATH_FIELDS: frozenset[str] = frozenset(
    {
        "filePath",
        "filepath",
        "path",
        "directory",
        "cwd",
        "absPath",
        "workdir",
    }
)


def rewrite_part_data(raw: str, origin_cwd: str, fork_cwd: str) -> str:
    """解析 part.data JSON、walk 树重写路径字段、re-serialize。

    parse 失败（非 JSON / 空）退化为 path-prefix-aware 字符串替换。
    """
    if not raw:
        return raw
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return rewrite_content_string(raw, origin_cwd, fork_cwd)
    rewrite_paths(obj, origin_cwd, fork_cwd)
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def rewrite_paths(obj: Any, origin_cwd: str, fork_cwd: str) -> None:
    """递归 walk JSON 树，按字段类别重写路径。

    dict 的路径字段走规范化前缀替换；其余所有字符串值走 path-prefix-aware
    替换（覆盖 prompt/description/text 等未知字段）；标量不动。
    """
    if isinstance(obj, dict):
        for key, value in list(obj.items()):
            if key in _PATH_FIELDS:
                obj[key] = _rewrite_one(value, origin_cwd, fork_cwd)
            elif isinstance(value, str):
                obj[key] = rewrite_content_string(value, origin_cwd, fork_cwd)
            else:
                rewrite_paths(value, origin_cwd, fork_cwd)
    elif isinstance(obj, list):
        for value in obj:
            rewrite_paths(value, origin_cwd, fork_cwd)


def _rewrite_one(value: Any, origin_cwd: str, fork_cwd: str) -> Any:
    """路径字段重写：``Path.resolve`` + 前缀比对，非 origin 绝对路径不动。"""
    if not isinstance(value, str) or not value:
        return value
    try:
        resolved = Path(value).expanduser()
        if not resolved.is_absolute():
            return value
        resolved = resolved.resolve(strict=False)
        origin_resolved = Path(origin_cwd).expanduser().resolve(strict=False)
        if resolved.is_relative_to(origin_resolved):
            rel = resolved.relative_to(origin_resolved)
            return str(Path(fork_cwd) / rel)
    except (ValueError, OSError):
        return value
    return value


def rewrite_content_string(text: str, origin_cwd: str, fork_cwd: str) -> str:
    """内容字段重写：origin_cwd 后跟非路径段字符的 occurrence 才替换。

    负向先行断言消除 ``/foo/bar-baz`` 一类子串误伤；origin_cwd 用原始字符
    串做 pattern（转录层写入的是原始形式），fork_cwd 由调用方传 resolve
    后的形式（与 runtime 的 project worktree 判定一致）。
    """
    if not text or origin_cwd == fork_cwd:
        return text
    pattern = re.escape(origin_cwd) + r"(?![a-zA-Z0-9_.-])"
    return re.sub(pattern, fork_cwd, text)
