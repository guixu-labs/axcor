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

"""opencode.db 只读访问原语：路径解析、只读连接与 part 归一。

驱动流式读取的定义处；只依赖标准库。
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

# 只读连接等待写入锁释放的时长（毫秒）；opencode 的写入是亚毫秒级，
# 超时只在罕见争用时触发，由调用方按 best-effort 回退
BUSY_TIMEOUT_MS = 5000


def opencode_db_path(env: dict[str, str] | None = None) -> Path:
    """解析 opencode 会话库路径：XDG_DATA_HOME 优先，回落 HOME/.local/share。"""
    e = env or os.environ
    xdg = e.get("XDG_DATA_HOME") or str(Path(e.get("HOME") or Path.home()) / ".local" / "share")
    return Path(xdg) / "opencode" / "opencode.db"


def open_ro_db(path: Path) -> sqlite3.Connection | None:
    """打开带 busy_timeout 的只读连接；失败返回 None（调用方回落空结果）。"""
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=BUSY_TIMEOUT_MS / 1000)
        con.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        return con
    except sqlite3.Error:
        return None


def normalize_part(part: dict[str, Any]) -> list[dict[str, Any]]:
    """单个 part 归一为 0..N 条统一消息（tool part 拆 tool_use + tool_result）。"""
    ptype = part.get("type", "")
    if ptype == "text":
        text = str(part.get("text", "")).strip()
        if text:
            return [{"type": "text", "text": text}]
    elif ptype == "reasoning":
        text = str(part.get("text", "")).strip()
        if text:
            return [{"type": "thinking", "text": text}]
    elif ptype == "tool":
        state = part.get("state", {})
        if isinstance(state, dict):
            return [
                {
                    "type": "tool_use",
                    "name": str(part.get("tool", "")),
                    "id": str(part.get("callID", "")),
                    "input": state.get("input", {}),
                },
                {
                    "type": "tool_result",
                    "tool_use_id": str(part.get("callID", "")),
                    "content": str(state.get("output", "") or ""),
                    "is_error": state.get("status") == "error",
                },
            ]
    elif ptype == "step-start":
        return [{"type": "step_start"}]
    return []
