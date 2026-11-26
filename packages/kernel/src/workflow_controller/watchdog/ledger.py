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

"""节点累计时钟账本——节点超时规则的持久化行。

watchdog_node_clocks 表住守护进程库 events.db，建表归 run 记录库的统一迁移
管理（阶梯 v6），本模块只提供读写面、不自行建表。每行是 (run_id, node_id,
gen) 键的累计用时与计时段起点；服务重启保留已用时间——加载时 running_since
非空的行按冻结收段（崩溃点不可知，开段至多损失一个巡检周期，宕机时长不计入）。
"""

from __future__ import annotations

__all__ = [
    "IDX_WATCHDOG_NODE_CLOCKS_RUN_DDL",
    "WATCHDOG_NODE_CLOCKS_DDL",
    "NodeClockLedger",
]

import asyncio
import sqlite3
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.watchdog.state import NodeClock


WATCHDOG_NODE_CLOCKS_DDL = """
    CREATE TABLE IF NOT EXISTS watchdog_node_clocks (
        run_id TEXT NOT NULL,
        node_id TEXT NOT NULL,
        gen INTEGER NOT NULL,
        threshold_seconds INTEGER NOT NULL,
        accumulated_seconds REAL NOT NULL DEFAULT 0,
        running_since TEXT,
        started_at TEXT NOT NULL,
        fired INTEGER NOT NULL DEFAULT 0,
        attempt INTEGER NOT NULL DEFAULT 0,
        session_id TEXT,
        verify_session_id TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (run_id, node_id, gen)
    )
"""

# run 维度热路径索引（幂等，随统一迁移铺设）
IDX_WATCHDOG_NODE_CLOCKS_RUN_DDL = """
    CREATE INDEX IF NOT EXISTS idx_watchdog_node_clocks_run
    ON watchdog_node_clocks (run_id, updated_at)
"""

_COLUMNS = (
    "run_id, node_id, gen, threshold_seconds, accumulated_seconds, running_since, "
    "started_at, fired, attempt, session_id, verify_session_id, updated_at"
)


class NodeClockLedger:
    """节点累计时钟账本的读写面（建表归 run 记录库统一迁移）。"""

    def __init__(self, db_path: Path) -> None:
        """只记路径不落盘：连接与表核查随首次访问发生。"""
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._schema_checked = False

    def close(self) -> None:
        """释放复用连接（测试与关停释放文件级占用）。"""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # ── 异步面（事件循环侧调用）──

    async def load_all(self) -> list[NodeClock]:
        """全量读回时钟行（启动恢复用）。"""
        return await asyncio.to_thread(self._load_all_locked)

    async def save(self, clock: NodeClock, updated_at: str) -> None:
        """落一行时钟（按主键幂等覆盖）。"""
        await asyncio.to_thread(self._save_locked, clock, updated_at)

    # ── 同步面（锁内）──

    def _table_present(self) -> bool:
        """账本表是否已在库（建表归 run 记录库统一迁移，非本模块职责）。"""
        conn = self._ensure_conn()
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        present = "watchdog_node_clocks" in tables
        self._schema_checked = present
        return present

    def _ensure_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(self.db_path, timeout=5.0, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA busy_timeout=5000")
        return self._conn

    def _connect(self) -> sqlite3.Connection:
        """写路径连接：表缺席即失败（写入方必经 run 记账库初始化的库）。"""
        if not self._schema_checked and not self._table_present():
            raise RuntimeError(
                f"{self.db_path}：watchdog_node_clocks 表缺席——须经 run 记录库统一迁移铺设"
            )
        return self._ensure_conn()

    def _load_all_locked(self) -> list[NodeClock]:
        """全量读回；表缺席（run 记账库尚未初始化）视为无存留行。"""
        from datetime import datetime

        from workflow_controller.watchdog.state import NodeClock

        conn = self._ensure_conn()
        if not self._table_present():
            return []
        clocks: list[NodeClock] = []
        for row in conn.execute(f"SELECT {_COLUMNS} FROM watchdog_node_clocks"):
            running_since = row["running_since"]
            clocks.append(
                NodeClock(
                    run_id=row["run_id"],
                    node_id=row["node_id"],
                    gen=int(row["gen"]),
                    threshold_seconds=int(row["threshold_seconds"]),
                    started_at=datetime.fromisoformat(row["started_at"]),
                    accumulated_seconds=float(row["accumulated_seconds"] or 0.0),
                    running_since=(
                        datetime.fromisoformat(running_since) if running_since else None
                    ),
                    fired=bool(row["fired"]),
                    attempt=int(row["attempt"] or 0),
                    session_id=row["session_id"] or "",
                    verify_session_id=row["verify_session_id"] or "",
                )
            )
        return clocks

    def _save_locked(self, clock: NodeClock, updated_at: str) -> None:
        conn = self._connect()
        conn.execute(
            "INSERT OR REPLACE INTO watchdog_node_clocks"
            f" ({_COLUMNS}) VALUES ({', '.join('?' for _ in range(12))})",
            (
                clock.run_id,
                clock.node_id,
                clock.gen,
                clock.threshold_seconds,
                clock.accumulated_seconds,
                (clock.running_since.isoformat() if clock.running_since is not None else None),
                clock.started_at.isoformat(),
                int(clock.fired),
                clock.attempt,
                clock.session_id,
                clock.verify_session_id,
                updated_at,
            ),
        )
        conn.commit()
