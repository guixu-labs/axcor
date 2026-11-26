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

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from workflow_controller.session.events import Event, EventType

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path

# events 表（v2 形状）：主键为持久 INTEGER（AUTOINCREMENT，VACUUM 安全），
# 客户端 uuid 降为 UNIQUE 幂等列；run_id/context 为维度打戳列。
EVENTS_DDL = """
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uuid TEXT UNIQUE,
        run_id TEXT,
        context TEXT,
        session_id TEXT NOT NULL,
        iteration INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        event_type TEXT NOT NULL,
        data TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    )
"""

IDX_EVENTS_SESSION_DDL = """
    CREATE INDEX IF NOT EXISTS idx_events_session ON events (session_id, iteration)
"""

IDX_EVENTS_RUN_DDL = """
    CREATE INDEX IF NOT EXISTS idx_events_run ON events (run_id)
"""

# run 键事件表：进程输出/驱动事件等不挂 session 身份的事件。
RUN_EVENTS_DDL = """
    CREATE TABLE IF NOT EXISTS run_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        context TEXT,
        timestamp TEXT NOT NULL,
        event_type TEXT NOT NULL,
        data TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
"""

IDX_RUN_EVENTS_DDL = """
    CREATE INDEX IF NOT EXISTS idx_run_events_run ON run_events (run_id, id)
"""

# sessions 表：DDL 与普通写/读均在此单一来源（事务内嵌写见 RunStore 连接）
SESSIONS_DDL = """
    CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'worker',
        state TEXT NOT NULL DEFAULT 'idle',
        loop_status TEXT NOT NULL DEFAULT 'yes',
        stop_reason TEXT,
        loop_hold TEXT,
        config_json TEXT NOT NULL,
        native_session_id TEXT,
        created_at TEXT NOT NULL,
        last_activity TEXT NOT NULL,
        current_iteration INTEGER DEFAULT 0,
        birth_handoff_doc TEXT,
        result TEXT DEFAULT 'fail',
        last_error TEXT,
        plugin_metadata TEXT,
        final_loop_status TEXT,
        last_stop_reason TEXT,
        retry_count INTEGER DEFAULT 0
    )
"""

# 事件读取的统一列序（uuid 即对外暴露的 id，持久主键只作排序/游标键）。
_EVENT_COLS = "uuid, run_id, context, session_id, iteration, timestamp, event_type, data"


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def ensure_sessions_table(conn: sqlite3.Connection) -> None:
    """初始化会话列表，并在调用方的写事务内迁移旧列。"""
    conn.execute(SESSIONS_DDL)
    if "sdk_session_id" in _table_columns(conn, "sessions"):
        conn.execute("ALTER TABLE sessions RENAME COLUMN sdk_session_id TO native_session_id")


def _create_eval_tables(conn: sqlite3.Connection) -> None:
    """evaluations + loop_results 建表与索引（幂等；DDL 文本保持与内联版逐字节一致）。"""
    conn.execute("""
                CREATE TABLE IF NOT EXISTS evaluations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    iteration INTEGER NOT NULL,
                    is_complete INTEGER NOT NULL,
                    progress REAL NOT NULL,
                    summary TEXT NOT NULL,
                    feedback TEXT,
                    artifacts TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)
    conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_evaluations_session
                ON evaluations (session_id)
            """)
    conn.execute("""
                CREATE TABLE IF NOT EXISTS loop_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    iterations INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    final_evaluation_id INTEGER REFERENCES evaluations(id),
                    started_at TEXT,
                    completed_at TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)


def _create_handoff_tables(conn: sqlite3.Connection) -> None:
    """handoff_relations 建表与索引（幂等；DDL 文本保持与内联版逐字节一致）。"""
    conn.execute("""
                CREATE TABLE IF NOT EXISTS handoff_relations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_session_id TEXT NOT NULL,
                    to_session_id TEXT NOT NULL,
                    handoff_doc TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(from_session_id, to_session_id)
                )
            """)
    conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_handoff_from
                ON handoff_relations (from_session_id)
            """)
    conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_handoff_to
                ON handoff_relations (to_session_id)
            """)


class EventStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        # threading.RLock：跨线程同步原语（事件循环与 to_thread 工作线程共用）。
        # 纪律：异步方法一律经 asyncio.to_thread 委托 *_locked 同步方法，禁止在
        # 事件循环线程内直接 acquire——与工作线程竞争时会把整个事件循环卡住，
        # 且可能被 SQLite busy_timeout=5s 放大。
        self._lock = threading.RLock()
        # 单写连接复用（热路径免每次建连）；跨线程使用一律持 self._lock
        self._write_conn: sqlite3.Connection | None = None
        self._init_db()

    @staticmethod
    def _et_str(et: Any) -> str:
        if isinstance(et, str):
            return et
        return str(et.value)

    @staticmethod
    def _et_parse(raw: str) -> Any:
        try:
            return EventType(raw)
        except ValueError:
            return raw

    @staticmethod
    def _insert_event(conn: sqlite3.Connection, event: Event) -> None:
        # uuid 为客户端幂等键：重放同 uuid 覆盖内容、保留原持久主键
        conn.execute(
            "INSERT INTO events (uuid, run_id, context, session_id, iteration,"
            " timestamp, event_type, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(uuid) DO UPDATE SET run_id=excluded.run_id,"
            " context=excluded.context, session_id=excluded.session_id,"
            " iteration=excluded.iteration, timestamp=excluded.timestamp,"
            " event_type=excluded.event_type, data=excluded.data",
            (
                event.id,
                event.run_id,
                event.context,
                event.session_id,
                event.iteration,
                event.timestamp.isoformat(),
                EventStore._et_str(event.event_type),
                json.dumps(event.data, default=str),
            ),
        )

    @staticmethod
    def _row_to_run_event(row: tuple) -> dict[str, Any]:
        return {
            "id": row[0],
            "run_id": row[1],
            "context": row[2],
            "timestamp": row[3],
            "event_type": row[4],
            "data": json.loads(row[5]),
        }

    @staticmethod
    def _row_to_event(row: tuple) -> Event:
        ts = datetime.fromisoformat(row[5])
        return Event(
            id=row[0],
            run_id=row[1],
            context=row[2],
            session_id=row[3],
            iteration=row[4],
            # 历史版本写入 naive 本地时间串，按本地时区补全；全量 aware 写入一个版本后可删除
            timestamp=ts if ts.tzinfo is not None else ts.astimezone(),
            event_type=EventStore._et_parse(row[6]),
            data=json.loads(row[7]),
        )

    async def store(self, event: Event) -> None:
        await asyncio.to_thread(self._store_locked, event)

    async def store_many(self, events: list[Event]) -> None:
        await asyncio.to_thread(self._store_many_locked, events)

    async def store_run_event(
        self,
        run_id: str,
        event_type: Any,
        data: dict[str, Any],
        *,
        context: str | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._store_run_event_locked, run_id, event_type, data, context, timestamp
        )

    async def store_run_events_batch(
        self,
        run_id: str,
        events: Sequence[tuple[Any, dict[str, Any]]],
        *,
        context: str | None = None,
    ) -> None:
        """批量写 run 键事件（单事务多行；进程输出按读块聚合的落库通道）。"""
        await asyncio.to_thread(self._store_run_events_batch_locked, run_id, events, context)

    def get_run_events_sync(
        self, run_id: str, since_id: int = 0, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """run 键事件（按持久主键增量读）。"""
        sql = (
            "SELECT id, run_id, context, timestamp, event_type, data"
            " FROM run_events WHERE run_id = ? AND id > ? ORDER BY id"
        )
        params: list[Any] = [run_id, since_id]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_run_event(r) for r in rows]

    def get_run_events_since_sync(
        self, since_id: int, limit: int | None = None
    ) -> tuple[int, list[dict[str, Any]]]:
        """全部 run 键事件按主键增量读（SSE 全局通道，消费端按 run_id 过滤）。

        limit 非空时单拍读数有界：两次 SSE 节拍间的突发输出不撑爆单个 tick 的
        内存/JSON 序列化；游标只推进到已读的最大 id，余量交下一拍补齐。
        """
        sql = (
            "SELECT id, run_id, context, timestamp, event_type, data"
            " FROM run_events WHERE id > ? ORDER BY id"
        )
        params: list[Any] = [since_id]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        if not rows:
            return since_id, []
        return rows[-1][0], [self._row_to_run_event(r) for r in rows]

    # ── run 键事件写面 ─────────────────────────────────────────────

    def get_max_run_event_id_sync(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM run_events").fetchone()
        return int(row[0] if row else 0)

    def get_stamped_events_sync(
        self, run_id: str, since_id: int = 0, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """成员 session 事件（按 run 维度打戳列增量读；uuid 为对外幂等 id）。"""
        sql = (
            "SELECT id, uuid, run_id, context, session_id, iteration, timestamp, event_type, data"
            " FROM events WHERE run_id = ? AND id > ? ORDER BY id"
        )
        params: list[Any] = [run_id, since_id]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            {
                "id": r[0],
                "uuid": r[1],
                "run_id": r[2],
                "context": r[3],
                "session_id": r[4],
                "iteration": r[5],
                "timestamp": r[6],
                "event_type": r[7],
                "data": json.loads(r[8]),
            }
            for r in rows
        ]

    async def get_events(
        self,
        session_id: str,
        iteration: int | None = None,
    ) -> list[Event]:
        return await asyncio.to_thread(self._get_events_locked, session_id, iteration)

    def get_events_sync(
        self,
        session_id: str,
        iteration: int | None = None,
    ) -> list[Event]:
        return self._read_events(session_id, iteration)

    def get_recent_events_sync(self, session_id: str, limit: int) -> list[Event]:
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT {_EVENT_COLS} FROM events WHERE session_id = ? ORDER BY id DESC LIMIT ?",
                (session_id, limit),
            ).fetchall()
        return [self._row_to_event(row) for row in reversed(rows)]

    def get_latest_session_outcome_sync(self, session_id: str) -> Event | None:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT {_EVENT_COLS} FROM events"
                " WHERE session_id = ? AND event_type = ? ORDER BY id DESC LIMIT 1",
                (session_id, EventType.SESSION_OUTCOME.value),
            ).fetchone()
        return self._row_to_event(row) if row is not None else None

    def get_events_since_sync(self, last_id: int) -> tuple[int, list[Event]]:
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT id, {_EVENT_COLS} FROM events WHERE id > ? ORDER BY id",
                (last_id,),
            ).fetchall()
        if not rows:
            return last_id, []
        new_max = rows[-1][0]
        events = [self._row_to_event(row[1:]) for row in rows]
        return new_max, events

    def get_max_event_id_sync(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM events").fetchone()
        return int(row[0] if row else 0)

    def get_events_through_id_sync(
        self,
        max_id: int,
        limit: int | None = None,
    ) -> list[Event]:
        sql = f"SELECT {_EVENT_COLS} FROM events WHERE id <= ? ORDER BY id DESC"
        params: tuple[int, ...] = (max_id,)
        if limit is not None and limit > 0:
            sql += " LIMIT ?"
            params = (max_id, limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_event(row) for row in reversed(rows)]

    def get_events_since_ts(self, ts: str, session_id: str = "") -> list[Event]:
        """按 timestamp 高水位查。session_id 非空时只查该 session。"""
        if session_id:
            rows = self._query_events(
                f"SELECT {_EVENT_COLS} FROM events"
                " WHERE session_id = ? AND timestamp > ? ORDER BY id",
                (session_id, ts),
            )
        else:
            rows = self._query_events(
                f"SELECT {_EVENT_COLS} FROM events WHERE timestamp > ? ORDER BY id",
                (ts,),
            )
        return [self._row_to_event(row) for row in rows]

    def get_all_events_sync(self) -> list[Event]:
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT {_EVENT_COLS} FROM events ORDER BY id",
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def get_usage_events_sync(self) -> list[Event]:
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT {_EVENT_COLS} FROM events WHERE event_type IN (?, ?, ?, ?) ORDER BY id",
                ("result", "RESULT", EventType.STEP_FINISH.value, "STEP_FINISH"),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    async def store_evaluation(
        self,
        session_id: str,
        iteration: int,
        result: Any,
    ) -> int:
        return await asyncio.to_thread(self._store_evaluation_locked, session_id, iteration, result)

    async def get_latest_evaluation(self, session_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_latest_evaluation_locked, session_id)

    async def store_loop_result(
        self,
        session_id: str,
        iterations: int,
        status: str,
        error: str | None = None,
        final_evaluation_id: int | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._store_loop_result_locked,
            session_id,
            iterations,
            status,
            error,
            final_evaluation_id,
            started_at,
            completed_at,
        )

    async def get_iterations(self, session_id: str) -> int:
        return await asyncio.to_thread(self._get_iterations_locked, session_id)

    def get_unique_session_ids_sync(self) -> list[str]:
        """Return all distinct session_ids in the store (oldest first)."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT session_id FROM events GROUP BY session_id ORDER BY MIN(id)"
            ).fetchall()
        return [r[0] for r in rows]

    def session_has_result_sync(self, session_id: str) -> bool:
        """Check if a session already has a RESULT or STEP_FINISH event."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM events WHERE session_id = ? "
                "AND event_type IN ('result', 'step_finish') LIMIT 1",
                (session_id,),
            ).fetchone()
        return row is not None

    def delete_session(self, session_id: str) -> None:
        with self._lock, self._connect_write() as conn:
            conn.execute("DELETE FROM events WHERE session_id = ?", (session_id,))
            conn.execute("DELETE FROM evaluations WHERE session_id = ?", (session_id,))
            conn.execute("DELETE FROM loop_results WHERE session_id = ?", (session_id,))
            conn.execute(
                "DELETE FROM handoff_relations WHERE from_session_id = ? OR to_session_id = ?",
                (session_id, session_id),
            )

    async def get_session(self, session_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_session_locked, session_id)

    def get_session_sync(self, session_id: str) -> dict[str, Any] | None:
        return self._get_session_locked(session_id)

    async def list_sessions(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.list_sessions_sync)

    def list_sessions_sync(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT id, name, role, state, loop_status, stop_reason, loop_hold, "
                "config_json, native_session_id, created_at, last_activity, "
                "current_iteration, birth_handoff_doc, result, last_error, "
                "plugin_metadata, final_loop_status, "
                "last_stop_reason, retry_count FROM sessions ORDER BY created_at"
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d: dict[str, Any] = {
                "id": r["id"],
                "name": r["name"],
                "role": r["role"],
                "state": r["state"],
                "loop_status": r["loop_status"],
                "stop_reason": r["stop_reason"],
                "loop_hold": r["loop_hold"],
                "config_json": r["config_json"],
                "native_session_id": r["native_session_id"],
                "created_at": r["created_at"],
                "last_activity": r["last_activity"],
                "current_iteration": r["current_iteration"],
                "birth_handoff_doc": r["birth_handoff_doc"],
                "result": r["result"],
                "last_error": r["last_error"],
            }
            d["plugin_metadata"] = r["plugin_metadata"]
            d["final_loop_status"] = r["final_loop_status"]
            d["last_stop_reason"] = r["last_stop_reason"]
            d["retry_count"] = r["retry_count"]
            out.append(d)
        return out

    async def upsert_roster(self, fields: dict) -> None:
        """sessions 表行 UPSERT（键为 sessions 表列名）。"""
        await asyncio.to_thread(self._upsert_roster_locked, fields)

    async def update_roster_state(
        self,
        session_id: str,
        state: str,
        *,
        loop_status: str | None = None,
        stop_reason: str | None = None,
        loop_hold: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._update_roster_state_locked,
            session_id,
            state,
            loop_status,
            stop_reason,
            loop_hold,
        )

    async def update_roster_loop_status(
        self,
        session_id: str,
        loop_status: str,
        *,
        stop_reason: str | None = None,
        loop_hold: str | None = None,
    ) -> None:
        if loop_status == "no" and stop_reason is None:
            raise ValueError("stop_reason is required when loop_status='no'")
        await asyncio.to_thread(
            self._update_roster_loop_status_locked,
            session_id,
            loop_status,
            stop_reason,
            loop_hold,
        )

    async def update_roster_name(self, session_id: str, name: str) -> None:
        await asyncio.to_thread(self._update_roster_name_locked, session_id, name)

    def update_roster_name_sync(self, session_id: str, name: str) -> None:
        self._update_roster_name_locked(session_id, name)

    async def delete_roster(self, session_id: str) -> None:
        await asyncio.to_thread(self._delete_roster_locked, session_id)

    async def add_handoff_relation(
        self,
        from_session_id: str,
        to_session_id: str,
        handoff_doc: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._add_handoff_relation_locked, from_session_id, to_session_id, handoff_doc
        )

    async def get_successors(self, session_id: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._get_successors_locked, session_id)

    def get_successors_sync(self, session_id: str) -> list[dict[str, Any]]:
        """Sync version for use in sync contexts."""
        return self._get_successors_sync(session_id)

    async def get_predecessor(self, session_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_predecessor_locked, session_id)

    def get_predecessor_sync(self, session_id: str) -> dict[str, Any] | None:
        """Sync version for use in sync contexts."""
        return self._get_predecessor_sync(session_id)

    # ── Session metadata reads（普通写见下方；事务内嵌写走 RunStore 连接）──

    async def get_handoff_chain(self, session_id: str) -> dict[str, Any]:
        """Return full chain: predecessors (walking back) + successors (walking forward)."""
        predecessors: list[dict[str, Any]] = []
        current = session_id
        visited: set[str] = set()
        while current and current not in visited:
            visited.add(current)
            pred = await self.get_predecessor(current)
            if pred is None:
                break
            predecessors.insert(0, pred)
            current = pred["from_session_id"]

        successors: list[dict[str, Any]] = []
        current = session_id
        visited = set()
        while current and current not in visited:
            visited.add(current)
            succs = await self.get_successors(current)
            if not succs:
                break
            for s in succs:
                successors.append(s)
            current = succs[-1]["to_session_id"]

        return {"predecessors": predecessors, "successors": successors}

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(EVENTS_DDL)
            conn.execute(IDX_EVENTS_SESSION_DDL)
            # 旧形状库（无 run_id 列）尚未迁移时不炸——重建由记录迁移统一执行
            if "run_id" in _table_columns(conn, "events"):
                conn.execute(IDX_EVENTS_RUN_DDL)
            conn.execute(RUN_EVENTS_DDL)
            conn.execute(IDX_RUN_EVENTS_DDL)
            _create_eval_tables(conn)
            conn.execute("BEGIN IMMEDIATE")
            ensure_sessions_table(conn)
            _create_handoff_tables(conn)
            # 旧 graph 插件的遗留表：零存量无导出义务，建库时幂等 DROP
            conn.execute("DROP TABLE IF EXISTS graphs")
            conn.execute("DROP TABLE IF EXISTS graph_node_history")

    @contextlib.contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        # 独立读连接：每连接设 busy_timeout（WAL checkpoint 期的并发读等待
        # 而非立即抛 database is locked）；导出/列表等大读走此路，不阻塞写者
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _connect_write(self) -> sqlite3.Connection:
        # 单写连接：写路径复用（journal_mode=WAL 单写者序列化，持锁跨线程安全）
        if self._write_conn is None:
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.execute("PRAGMA busy_timeout=5000")
            self._write_conn = conn
        return self._write_conn

    def _store_locked(self, event: Event) -> None:
        with self._lock:
            self._write_event(event)

    def _store_many_locked(self, events: list[Event]) -> None:
        # 批量单事务提交：整批一次 COMMIT
        with self._lock, self._connect_write() as conn:
            for event in events:
                self._insert_event(conn, event)

    def _write_event(self, event: Event) -> None:
        with self._connect_write() as conn:
            self._insert_event(conn, event)

    # ── sessions 列表写面（普通单行写；mint/discard 的事务内嵌写归 RunStore 连接）──

    def _store_run_event_locked(
        self,
        run_id: str,
        event_type: Any,
        data: dict[str, Any],
        context: str | None,
        timestamp: datetime | None,
    ) -> None:
        with self._lock, self._connect_write() as conn:
            conn.execute(
                "INSERT INTO run_events (run_id, context, timestamp, event_type, data)"
                " VALUES (?, ?, ?, ?, ?)",
                (
                    run_id,
                    context,
                    (timestamp or datetime.now(tz=UTC)).isoformat(),
                    self._et_str(event_type),
                    json.dumps(data, default=str),
                ),
            )

    def _store_run_events_batch_locked(
        self,
        run_id: str,
        events: Sequence[tuple[Any, dict[str, Any]]],
        context: str | None,
    ) -> None:
        with self._lock, self._connect_write() as conn:
            # 锁内取点：并发批量写的时间戳随主键序单调
            now = datetime.now(tz=UTC).isoformat()
            conn.executemany(
                "INSERT INTO run_events (run_id, context, timestamp, event_type, data)"
                " VALUES (?, ?, ?, ?, ?)",
                [
                    (run_id, context, now, self._et_str(et), json.dumps(data, default=str))
                    for et, data in events
                ],
            )

    def _get_events_locked(
        self,
        session_id: str,
        iteration: int | None = None,
    ) -> list[Event]:
        with self._lock:
            return self._read_events(session_id, iteration)

    def _read_events(self, session_id: str, iteration: int | None = None) -> list[Event]:
        with self._connect() as conn:
            if iteration is not None:
                rows = conn.execute(
                    f"SELECT {_EVENT_COLS} FROM events"
                    " WHERE session_id = ? AND iteration = ? ORDER BY id",
                    (session_id, iteration),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"SELECT {_EVENT_COLS} FROM events WHERE session_id = ? ORDER BY id",
                    (session_id,),
                ).fetchall()

        return [self._row_to_event(row) for row in rows]

    def _query_events(self, sql: str, params: tuple) -> list:
        with self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def _store_evaluation_locked(
        self,
        session_id: str,
        iteration: int,
        result: Any,
    ) -> int:
        with self._lock, self._connect_write() as conn:
            cursor = conn.execute(
                "INSERT INTO evaluations (session_id, iteration, is_complete, progress, summary, feedback, artifacts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    session_id,
                    iteration,
                    int(result.is_complete),
                    result.progress,
                    result.summary,
                    result.feedback,
                    json.dumps(result.artifacts, default=str),
                ),
            )
            return cursor.lastrowid or 0

    def _get_latest_evaluation_locked(self, session_id: str) -> dict[str, Any] | None:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT id, session_id, iteration, is_complete, progress, summary, feedback, artifacts "
                    "FROM evaluations WHERE session_id = ? ORDER BY iteration DESC LIMIT 1",
                    (session_id,),
                ).fetchone()
            if row is None:
                return None
            return {
                "id": row[0],
                "session_id": row[1],
                "iteration": row[2],
                "is_complete": bool(row[3]),
                "progress": row[4],
                "summary": row[5],
                "feedback": row[6],
                "artifacts": json.loads(row[7]) if row[7] else {},
            }

    def _store_loop_result_locked(
        self,
        session_id: str,
        iterations: int,
        status: str,
        error: str | None,
        final_evaluation_id: int | None,
        started_at: datetime | None,
        completed_at: datetime | None,
    ) -> None:
        with self._lock, self._connect_write() as conn:
            conn.execute(
                "INSERT INTO loop_results "
                "(session_id, iterations, status, error, final_evaluation_id, started_at, completed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    session_id,
                    iterations,
                    status,
                    error,
                    final_evaluation_id,
                    started_at.isoformat() if started_at else None,
                    completed_at.isoformat() if completed_at else None,
                ),
            )

    def _get_iterations_locked(self, session_id: str) -> int:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT MAX(iteration) FROM events WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
            return row[0] if row[0] is not None else 0

    def _get_session_locked(self, session_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._read_session(session_id)

    def _get_session_row_sync(self, session_id: str) -> sqlite3.Row | None:
        """同步查询 sessions 表原始行（插件经存储门面寻址后自开连接的回退路径）。"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(
                "SELECT * FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()

    def _read_session(self, session_id: str) -> dict[str, Any] | None:
        row = self._get_session_row_sync(session_id)
        if row is None:
            return None
        out: dict[str, Any] = {
            "id": row["id"],
            "name": row["name"],
            "role": row["role"],
            "state": row["state"],
            "loop_status": row["loop_status"],
            "stop_reason": row["stop_reason"],
            "loop_hold": row["loop_hold"],
            "config_json": row["config_json"],
            "native_session_id": row["native_session_id"],
            "created_at": row["created_at"],
            "last_activity": row["last_activity"],
            "current_iteration": row["current_iteration"],
            "birth_handoff_doc": row["birth_handoff_doc"],
            "result": row["result"],
            "last_error": row["last_error"],
        }
        # 这些列由 ALTER TABLE migration 补齐，老库可能缺列；Row 无 get/包含判断，
        # 先取 keys() 集合再查
        row_keys = set(row.keys())
        out["plugin_metadata"] = row["plugin_metadata"] if "plugin_metadata" in row_keys else None
        out["final_loop_status"] = (
            row["final_loop_status"] if "final_loop_status" in row_keys else None
        )
        out["last_stop_reason"] = (
            row["last_stop_reason"] if "last_stop_reason" in row_keys else None
        )
        out["retry_count"] = row["retry_count"] if "retry_count" in row_keys else 0
        return out

    def _upsert_roster_locked(self, fields: dict) -> None:
        with self._lock, self._connect_write() as conn:
            cols = list(fields)
            conn.execute(
                f"INSERT OR REPLACE INTO sessions ({', '.join(cols)})"
                f" VALUES ({', '.join('?' for _ in cols)})",
                [fields[c] for c in cols],
            )

    def _update_roster_state_locked(
        self,
        session_id: str,
        state: str,
        loop_status: str | None,
        stop_reason: str | None,
        loop_hold: str | None,
    ) -> None:
        with self._lock, self._connect_write() as conn:
            sets = ["state = ?"]
            params: list = [state]
            if loop_status is not None:
                sets.append("loop_status = ?")
                params.append(loop_status)
            if stop_reason is not None or loop_status is not None:
                sets.append("stop_reason = ?")
                params.append(stop_reason)
            if loop_hold is not None or loop_status is not None:
                sets.append("loop_hold = ?")
                params.append(loop_hold)
            params.append(session_id)
            conn.execute(
                f"UPDATE sessions SET {', '.join(sets)} WHERE id = ?",
                params,
            )

    def _update_roster_loop_status_locked(
        self,
        session_id: str,
        loop_status: str,
        stop_reason: str | None,
        loop_hold: str | None,
    ) -> None:
        with self._lock, self._connect_write() as conn:
            if loop_status == "yes":
                conn.execute(
                    "UPDATE sessions SET loop_status = ?, stop_reason = NULL,"
                    " loop_hold = ? WHERE id = ?",
                    (loop_status, loop_hold, session_id),
                )
            else:
                conn.execute(
                    "UPDATE sessions SET loop_status = ?, stop_reason = ?,"
                    " loop_hold = ? WHERE id = ?",
                    (loop_status, stop_reason, loop_hold, session_id),
                )

    def _update_roster_name_locked(self, session_id: str, name: str) -> None:
        with self._lock, self._connect_write() as conn:
            conn.execute(
                "UPDATE sessions SET name = ? WHERE id = ?",
                (name, session_id),
            )

    def _delete_roster_locked(self, session_id: str) -> None:
        with self._lock, self._connect_write() as conn:
            conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))

    def _add_handoff_relation_locked(
        self,
        from_session_id: str,
        to_session_id: str,
        handoff_doc: str | None,
    ) -> None:
        with self._lock, self._connect_write() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO handoff_relations "
                "(from_session_id, to_session_id, handoff_doc, created_at) "
                "VALUES (?, ?, ?, ?)",
                (
                    from_session_id,
                    to_session_id,
                    handoff_doc,
                    datetime.now(tz=UTC).isoformat(),
                ),
            )

    def _get_successors_locked(self, session_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return self._get_successors_sync(session_id)

    def _get_successors_sync(self, session_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT to_session_id, handoff_doc, created_at "
                "FROM handoff_relations WHERE from_session_id = ? ORDER BY created_at",
                (session_id,),
            ).fetchall()
        return [{"to_session_id": r[0], "handoff_doc": r[1], "created_at": r[2]} for r in rows]

    def _get_predecessor_locked(self, session_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._get_predecessor_sync(session_id)

    def _get_predecessor_sync(self, session_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT from_session_id, handoff_doc, created_at "
                "FROM handoff_relations WHERE to_session_id = ? LIMIT 1",
                (session_id,),
            ).fetchone()
        if row is None:
            return None
        return {"from_session_id": row[0], "handoff_doc": row[1], "created_at": row[2]}
