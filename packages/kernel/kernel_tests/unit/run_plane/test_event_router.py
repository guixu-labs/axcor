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

"""EventStoreRouter 单测：守护进程级单库直写与游标增量。

单库化后一切事件落 default 库；session→cwd→库 路由与 default 兜底退役——
work_dir 不再产生事件库文件。游标是 events 表持久主键高水位。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from workflow_controller.session.events import Event, EventType
from workflow_controller.store.event_router import EventStoreRouter

if TYPE_CHECKING:
    from pathlib import Path


def _evt(sid: str, ts: str, *, run_id: str | None = None) -> Event:
    e = Event.create(
        session_id=sid,
        iteration=0,
        event_type=EventType.NOTIFICATION,
        data={"t": ts},
    )
    # aware-UTC 时间戳（与 Event.create 相同；naive 输入串会被读侧按本地时区补全）
    e.timestamp = datetime.fromisoformat(ts).replace(tzinfo=UTC)
    e.run_id = run_id
    return e


def _make_router(tmp_path: Path) -> EventStoreRouter:
    return EventStoreRouter(default_project_dir=tmp_path / "default")


async def test_events_land_in_daemon_db_only(tmp_path: Path) -> None:
    """成员事件单库直写：session 的 cwd 不再产生任何事件库文件。"""
    r = _make_router(tmp_path)
    (tmp_path / "projA").mkdir()
    await r.store(_evt("sess-a", "2026-01-01T00:00:00"))

    assert (tmp_path / "default" / ".workflow-controller" / "events.db").exists()
    assert not (tmp_path / "projA" / ".workflow-controller").exists()
    assert len(await r.get_events("sess-a")) == 1


async def test_unknown_session_id_lands_in_daemon_db(tmp_path: Path) -> None:
    r = _make_router(tmp_path)
    await r.store(_evt("unknown-sid", "2026-01-01T00:00:00"))
    assert (tmp_path / "default" / ".workflow-controller" / "events.db").exists()


async def test_get_events_since_orders_by_persistent_pk(tmp_path: Path) -> None:
    r = _make_router(tmp_path)
    await r.store(_evt("a", "2026-01-01T00:00:02"))
    await r.store(_evt("b", "2026-01-01T00:00:01"))
    await r.store(_evt("a", "2026-01-01T00:00:03"))

    new_ts, events = r.get_events_since("")  # 空 = 从头

    assert len(events) == 3
    assert [e.data["t"] for e in events] == [
        "2026-01-01T00:00:02",
        "2026-01-01T00:00:01",
        "2026-01-01T00:00:03",
    ]  # 按 id（写入序）而非 timestamp
    assert new_ts == "2026-01-01T00:00:03+00:00"


async def test_cursor_incremental_roundtrip(tmp_path: Path) -> None:
    r = _make_router(tmp_path)
    await r.store(_evt("a", "2026-01-01T00:00:00"))
    cursor = r.snapshot_event_cursor()

    replay = r.get_recent_at_cursor(cursor, 50)
    assert [e.session_id for e in replay] == ["a"]

    await r.store(_evt("a", "2026-01-01T00:00:01"))
    await r.store(_evt("b", "2026-01-01T00:00:02"))
    next_cursor, events = r.get_events_since_cursor(cursor)

    assert {e.session_id for e in events} == {"a", "b"}
    final_cursor, duplicate = r.get_events_since_cursor(next_cursor)
    assert duplicate == []
    assert final_cursor == next_cursor


async def test_cursor_invalid_falls_back_to_zero(tmp_path: Path) -> None:
    r = _make_router(tmp_path)
    await r.store(_evt("a", "2026-01-01T00:00:00"))
    _, events = r.get_events_since_cursor("garbage")
    assert len(events) == 1


async def test_delete_session_only_removes_that_session(tmp_path: Path) -> None:
    r = _make_router(tmp_path)
    await r.store(_evt("a", "2026-01-01T00:00:00"))
    await r.store(_evt("b", "2026-01-01T00:00:01"))

    r.delete_session("a")

    assert await r.get_events("a") == []
    assert len(await r.get_events("b")) == 1


async def test_get_recent_returns_last_n(tmp_path: Path) -> None:
    r = _make_router(tmp_path)
    for i in range(5):
        await r.store(_evt("a", f"2026-01-01T00:00:0{i}"))

    recent = r.get_recent(limit=2)

    assert [e.data["t"] for e in recent] == [
        "2026-01-01T00:00:03",
        "2026-01-01T00:00:04",
    ]


async def test_store_run_event_lands_in_run_events(tmp_path: Path) -> None:
    """run 键事件（无 session 身份）落 run_events 表，不混入 events。"""
    import sqlite3

    r = _make_router(tmp_path)
    event = Event.create(session_id="", iteration=0, event_type=EventType.LOG_AUTO, data={"x": 1})
    event.run_id = "run-1"
    event.context = "node=b"
    await r.store_run_event(event)

    with sqlite3.connect(r.db_path) as conn:
        row = conn.execute("SELECT run_id, context, event_type FROM run_events").fetchone()
        events_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assert row == ("run-1", "node=b", "log_auto")
    assert events_count == 0
