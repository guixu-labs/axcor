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

"""历史检查点兼容读取器：旧 ``checkpoints`` 表的内核侧读写面。

旧表 schema（十五列、旧 ``main_part_id`` 锚点）由本模块以复制常量的方式
直连；读取面不建表（表缺席即空结果），写入面仅供 session 导入能力落
历史行（DDL 为旧表形状的逐字复刻，新库缺席时铺设、与历史库共存）。
"""

from __future__ import annotations

import contextlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = [
    "LEGACY_CHECKPOINTS_DDL",
    "LegacyCheckpoint",
    "LegacyCheckpointReader",
    "legacy_checkpoint_to_dict",
    "store_legacy_checkpoint",
]

# 旧表列序（与历史行写入/读取的 SELECT 顺序一致，行映射按位置取值）
LEGACY_CHECKPOINT_COLUMNS = (
    "checkpoint_id, session_id, iteration, agent_id, agent_type, trigger, "
    "git_ref, event_rowid, main_part_id, parent_checkpoint_id, created_at, "
    "file_count, byte_size, status, inner_git"
)

# 旧表 DDL：旧表形状的逐字复刻（导入能力在旧表缺席时铺设；读取面永不建表）
LEGACY_CHECKPOINTS_DDL = """
    CREATE TABLE IF NOT EXISTS checkpoints (
        checkpoint_id    TEXT PRIMARY KEY,
        session_id       TEXT NOT NULL,
        iteration         INTEGER NOT NULL,
        agent_id          TEXT,
        agent_type        TEXT,
        trigger           TEXT NOT NULL,
        git_ref           TEXT,
        event_rowid       INTEGER,
        main_part_id      TEXT,
        parent_checkpoint_id TEXT,
        created_at        TEXT NOT NULL,
        file_count        INTEGER DEFAULT 0,
        byte_size         INTEGER DEFAULT 0,
        status            TEXT DEFAULT 'ok',
        inner_git         TEXT DEFAULT ''
    )
"""


@dataclass(frozen=True)
class LegacyCheckpoint:
    """旧 ``checkpoints`` 表的历史行（快照代检查点元数据）。

    ``git_ref`` 指向插件快照仓（``<cwd>/.workflow-controller/snapshots``）内
    的文件态提交；``main_part_id`` 是旧锚点格式——主会话 L2 里派发该子代理
    的 task part 标识（message 级截断的定位起点）。
    """

    checkpoint_id: str
    session_id: str
    iteration: int
    agent_id: str
    agent_type: str
    trigger: str
    git_ref: str | None
    event_rowid: int | None
    main_part_id: str | None
    parent_checkpoint_id: str | None
    created_at: str
    file_count: int
    byte_size: int
    status: str = "ok"
    inner_git: str = ""


def _row_to_legacy_checkpoint(row: tuple[Any, ...]) -> LegacyCheckpoint:
    """按列序把旧行映射为记录（None 数值/文本列落默认值）。"""
    return LegacyCheckpoint(
        checkpoint_id=str(row[0]),
        session_id=str(row[1]),
        iteration=int(row[2] or 0),
        agent_id="" if row[3] is None else str(row[3]),
        agent_type="" if row[4] is None else str(row[4]),
        trigger=str(row[5]),
        git_ref=None if row[6] is None else str(row[6]),
        event_rowid=int(row[7]) if row[7] is not None else None,
        main_part_id=None if row[8] is None else str(row[8]),
        parent_checkpoint_id=None if row[9] is None else str(row[9]),
        created_at=str(row[10]),
        file_count=int(row[11] or 0),
        byte_size=int(row[12] or 0),
        status="ok" if row[13] is None else str(row[13]),
        inner_git="" if row[14] is None else str(row[14]),
    )


def legacy_checkpoint_to_dict(checkpoint: LegacyCheckpoint) -> dict[str, Any]:
    """历史行的对外形状：插件列表口径 + 来源代标注（快照代）。"""
    return {
        "checkpoint_id": checkpoint.checkpoint_id,
        "session_id": checkpoint.session_id,
        "iteration": checkpoint.iteration,
        "agent_id": checkpoint.agent_id,
        "agent_type": checkpoint.agent_type,
        "trigger": checkpoint.trigger,
        "git_ref": checkpoint.git_ref,
        "event_rowid": checkpoint.event_rowid,
        "main_part_id": checkpoint.main_part_id,
        "parent_checkpoint_id": checkpoint.parent_checkpoint_id,
        "created_at": checkpoint.created_at,
        "file_count": checkpoint.file_count,
        "byte_size": checkpoint.byte_size,
        "status": checkpoint.status,
        "inner_git": checkpoint.inner_git,
        "generation": "snapshot",
    }


class LegacyCheckpointReader:
    """旧 ``checkpoints`` 表的只读面（短连接、表缺席即空结果）。"""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)

    def list_for_session(self, session_id: str) -> list[LegacyCheckpoint]:
        """列一个会话的全部历史检查点（按创建时间升序，对齐插件口径）。"""
        rows: list[tuple[Any, ...]] = []
        with contextlib.suppress(sqlite3.Error), self._connect() as conn:
            rows = conn.execute(
                f"SELECT {LEGACY_CHECKPOINT_COLUMNS} FROM checkpoints "
                "WHERE session_id = ? ORDER BY created_at",
                (session_id,),
            ).fetchall()
        return [_row_to_legacy_checkpoint(row) for row in rows]

    def get(self, checkpoint_id: str) -> LegacyCheckpoint | None:
        """按标识取单条历史检查点；不存在或表缺席返回 None。"""
        row: tuple[Any, ...] | None = None
        with contextlib.suppress(sqlite3.Error), self._connect() as conn:
            row = conn.execute(
                f"SELECT {LEGACY_CHECKPOINT_COLUMNS} FROM checkpoints WHERE checkpoint_id = ?",
                (checkpoint_id,),
            ).fetchone()
        return None if row is None else _row_to_legacy_checkpoint(row)

    @contextlib.contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        """开短连接读旧表；库文件不存在时按空结果处理。"""
        if not self.db_path.is_file():
            yield _NullConnection()
            return
        conn = sqlite3.connect(str(self.db_path), timeout=5.0)
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
        finally:
            conn.close()


class _NullConnection:
    """库缺席时的空结果替身：任何查询都返回空。"""

    def execute(self, _sql: str, _params: tuple[Any, ...] = ()) -> _NullCursor:
        return _NullCursor()

    def __enter__(self) -> _NullConnection:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None


class _NullCursor:
    """空游标：fetchone/fetchall 恒空。"""

    def fetchone(self) -> None:
        return None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []


def store_legacy_checkpoint(db_path: Path, checkpoint: LegacyCheckpoint) -> None:
    """落一条历史检查点行（导入能力专用；表缺席时按插件 DDL 铺设）。

    幂等于 checkpoint_id（INSERT OR REPLACE），与插件写入形态一致。
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=5.0)
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        conn.execute(LEGACY_CHECKPOINTS_DDL)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cp_session ON checkpoints (session_id)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cp_parent ON checkpoints (parent_checkpoint_id)"
        )
        conn.execute(
            f"INSERT OR REPLACE INTO checkpoints ({LEGACY_CHECKPOINT_COLUMNS}) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                checkpoint.checkpoint_id,
                checkpoint.session_id,
                checkpoint.iteration,
                checkpoint.agent_id,
                checkpoint.agent_type,
                checkpoint.trigger,
                checkpoint.git_ref,
                checkpoint.event_rowid,
                checkpoint.main_part_id,
                checkpoint.parent_checkpoint_id,
                checkpoint.created_at,
                checkpoint.file_count,
                checkpoint.byte_size,
                checkpoint.status,
                checkpoint.inner_git,
            ),
        )
        conn.commit()
    finally:
        conn.close()
