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

"""守护进程级单库的事件存储门面。

一切 run/session 事件与记录落启动目录 ``.workflow-controller/events.db``（守护进程库）；
按 session cwd 分库的路由与 default 兜底已退役——成员 session 事件、评测、
loop_results 与 run 键事件同库直写。

游标（cursor）是 events 表持久主键高水位的字符串形式，供 SSE / 增量拉取；
事件 timestamp 只用于展示排序，不参与是否已消费的判断。
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any

from workflow_controller.store.event_store import EventStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from workflow_controller.core.session_run_affiliation import SessionRunAffiliation
    from workflow_controller.session.events import Event

    # 归属解析器：session_id → 归属（打戳唯一入口，归属缓存的热路径查找）
    RunStamper = Callable[[str], "SessionRunAffiliation | None"]

# SSE 全局通道单拍推送 run 键事件的行数上限：突发输出分多拍推送，单拍内存/序列化有界，
# 游标只推进到已读的最大 id，余量交下一拍补齐（与前端 cursor 分页语义对齐）
_RUN_EVENTS_PUSH_LIMIT = 500


def _decode_cursor(cursor: str) -> int:
    try:
        return int(cursor)
    except (TypeError, ValueError):
        return 0


class EventStoreRouter:
    """守护进程库门面：单 EventStore + 游标/聚合查询面 + 写入路径打戳。"""

    def __init__(self, default_project_dir: Path) -> None:
        self._default_project_dir = Path(default_project_dir)
        self._stores: dict[str, EventStore] = {}
        self._run_stamper: RunStamper | None = None

    @property
    def db_path(self) -> Path:
        return (self._default_project_dir / ".workflow-controller" / "events.db").resolve()

    # --- 库寻址 ---

    @property
    def project_dir(self) -> Path:
        """守护进程库所在工程目录（与 db_path 同锚 resolve，供外部读库方寻址）。"""
        return self._default_project_dir.resolve()

    @property
    def default_store(self) -> EventStore:
        """守护进程库存储实例（只读共享面，如 run 事件游标）。"""
        return self._store()

    def set_run_stamper(self, stamper: RunStamper | None) -> None:
        """注入归属解析器：session 键事件写库前按归属缓存补 run 维度。"""
        self._run_stamper = stamper

    async def store(self, event: Event) -> None:
        self._stamp_run(event)
        await self._store().store(event)

    async def store_many(self, events: list[Event]) -> None:
        for event in events:
            self._stamp_run(event)
        await self._store().store_many(events)

    # --- 事件写/读 ---

    async def store_run_event(self, event: Event) -> None:
        """run 键事件（无 session 身份）落 run_events 表。"""
        await self._store().store_run_event(
            event.run_id or "",
            event.event_type,
            event.data,
            context=event.context,
            timestamp=event.timestamp,
        )

    async def get_events(
        self,
        session_id: str,
        iteration: int | None = None,
    ) -> list[Event]:
        return await self._store().get_events(session_id, iteration)

    def get_events_sync(
        self,
        session_id: str,
        iteration: int | None = None,
    ) -> list[Event]:
        return self._store().get_events_sync(session_id, iteration)

    async def store_evaluation(
        self,
        session_id: str,
        iteration: int,
        result: Any,
    ) -> int:
        return await self._store().store_evaluation(session_id, iteration, result)

    async def get_latest_evaluation(self, session_id: str) -> dict[str, Any] | None:
        return await self._store().get_latest_evaluation(session_id)

    async def store_loop_result(self, session_id: str, *args: Any, **kwargs: Any) -> None:
        await self._store().store_loop_result(session_id, *args, **kwargs)

    async def get_iterations(self, session_id: str) -> int:
        return await self._store().get_iterations(session_id)

    def delete_session(self, session_id: str) -> None:
        """删除 session 的事件 + 评测 + loop 结果 + handoff 关系。

        sessions 表行不在此时删——隐式归属随回收在 RunStore 单事务内
        连行删；显式成员的行删走本 store 的 delete_roster。
        """
        self._store().delete_session(session_id)

    def read_session_birth_traceback(self, session_id: str) -> dict[str, Any] | None:
        """读 sessions 表行的 birth_traceback（须在该行被删前调用）。"""
        if not session_id:
            return None
        try:
            con = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
            try:
                row = con.execute(
                    "SELECT plugin_metadata FROM sessions WHERE id = ?",
                    (session_id,),
                ).fetchone()
            finally:
                con.close()
            if row and row[0]:
                try:
                    pm = json.loads(row[0])
                except (ValueError, TypeError):
                    return None
                bt = (pm.get("agent_traceback") or {}).get("birth_traceback")
                if isinstance(bt, dict):
                    return bt
        except (sqlite3.Error, OSError):
            return None
        return None

    def snapshot_event_cursor(self) -> str:
        return str(self._store().get_max_event_id_sync())

    def snapshot_run_event_cursor(self) -> str:
        return str(self._store().get_max_run_event_id_sync())

    def get_run_events_since_cursor(self, cursor: str) -> tuple[str, list[dict[str, Any]]]:
        """run 键事件全局增量（SSE 通道推送用，消费端按 run_id 过滤）。"""
        new_max, rows = self._store().get_run_events_since_sync(
            _decode_cursor(cursor), limit=_RUN_EVENTS_PUSH_LIMIT
        )
        return str(new_max), rows

    # --- 游标与聚合读 ---

    def get_recent_at_cursor(self, cursor: str, limit: int = 50) -> list[Event]:
        return self._store().get_events_through_id_sync(
            _decode_cursor(cursor),
            limit if limit > 0 else None,
        )

    def get_events_since_cursor(
        self,
        cursor: str,
        session_id: str = "",
    ) -> tuple[str, list[Event]]:
        new_max, events = self._store().get_events_since_sync(_decode_cursor(cursor))
        if session_id:
            events = [e for e in events if e.session_id == session_id]
        return str(new_max), events

    def get_events_since(self, ts: str, session_id: str = "") -> tuple[str, list[Event]]:
        """聚合 ``timestamp > ts`` 的事件，按 id 升序（SSE 全景回放用）。"""
        events = self._store().get_events_since_ts(ts, session_id=session_id)
        new_ts = max((e.timestamp.isoformat() for e in events), default=ts)
        return new_ts, events

    def get_all_events_sync(self) -> list[Event]:
        return self._store().get_all_events_sync()

    def get_usage_events_sync(self) -> list[Event]:
        return self._store().get_usage_events_sync()

    def get_recent_events_sync(self, session_id: str, limit: int) -> list[Event]:
        return self._store().get_recent_events_sync(session_id, limit)

    def get_latest_session_outcome_sync(self, session_id: str) -> Event | None:
        return self._store().get_latest_session_outcome_sync(session_id)

    def get_recent(self, limit: int = 50) -> list[Event]:
        """最近 N 条（按 id 升序返回，取尾部 limit）。"""
        events = self.get_all_events_sync()
        return events[-limit:] if limit > 0 else events

    async def store_session(self, session_id: str, **kwargs: Any) -> None:
        fields = dict(kwargs)
        fields["id"] = session_id
        await self._store().upsert_roster(fields)

    async def get_session(self, session_id: str) -> dict[str, Any] | None:
        return await self._store().get_session(session_id)

    def get_session_sync(self, session_id: str) -> dict[str, Any] | None:
        return self._store().get_session_sync(session_id)

    # --- Session metadata（sessions 表读/普通写均走事件库；事务内嵌写归 RunStore）---

    async def list_sessions(self) -> list[dict[str, Any]]:
        return await self._store().list_sessions()

    def list_sessions_sync(self) -> list[dict[str, Any]]:
        return self._store().list_sessions_sync()

    async def update_loop_status(self, session_id: str, loop_status: str, **kwargs: Any) -> None:
        await self._store().update_roster_loop_status(session_id, loop_status, **kwargs)

    def update_session_name_sync(self, session_id: str, name: str) -> None:
        self._store().update_roster_name_sync(session_id, name)

    async def delete_roster(self, session_id: str) -> None:
        """删除 sessions 表行（普通写；归属连行的级联删在 RunStore 事务内）。"""
        await self._store().delete_roster(session_id)

    async def add_handoff_relation(
        self, from_session_id: str, to_session_id: str, **kwargs: Any
    ) -> None:
        await self._store().add_handoff_relation(from_session_id, to_session_id, **kwargs)

    async def get_successors(self, session_id: str) -> list[dict[str, Any]]:
        return await self._store().get_successors(session_id)

    def get_successors_sync(self, session_id: str) -> list[dict[str, Any]]:
        """Sync version for use in sync contexts."""
        return self._store().get_successors_sync(session_id)

    async def get_predecessor(self, session_id: str) -> dict[str, Any] | None:
        return await self._store().get_predecessor(session_id)

    def get_predecessor_sync(self, session_id: str) -> dict[str, Any] | None:
        """Sync version for use in sync contexts."""
        return self._store().get_predecessor_sync(session_id)

    async def get_handoff_chain(self, session_id: str) -> dict[str, Any]:
        return await self._store().get_handoff_chain(session_id)

    def _get_or_create(self, db_path: Path) -> EventStore:
        key = str(db_path)
        store = self._stores.get(key)
        if store is None or not db_path.exists():
            # db 文件被删时缓存里的 store 指向已不存在的路径，重建触发 _init_db
            store = EventStore(db_path)
            self._stores[key] = store
        return store

    def _store(self) -> EventStore:
        return self._get_or_create(self.db_path)

    def _stamp_run(self, event: Event) -> None:
        """写库前打戳：维度只从归属缓存来，已有维度不覆盖。"""
        if event.run_id is None and event.session_id and self._run_stamper is not None:
            aff = self._run_stamper(event.session_id)
            if aff is not None:
                event.run_id = aff.run_id
                if event.context is None:
                    event.context = aff.context
