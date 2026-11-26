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

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.runs import RunStore
from workflow_controller.session.events import Event, EventType
from workflow_controller.store.event_store import EventStore

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("run_store_first", [False, True], ids=["event-store", "run-store"])
async def test_legacy_session_id_is_preserved_on_startup(
    tmp_path: Path, run_store_first: bool
) -> None:
    """两个存储入口均可原地升级已有会话记录。"""
    db_path = tmp_path / "events.db"
    runs = RunStore(db_path)
    await runs.list_runs()
    runs.close()
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute("ALTER TABLE sessions RENAME COLUMN native_session_id TO sdk_session_id")
        conn.execute(
            "INSERT INTO sessions (id, name, config_json, sdk_session_id, created_at, last_activity)"
            " VALUES ('s1', 'worker', '{}', 'runtime-1', datetime('now'), datetime('now'))"
        )

    if run_store_first:
        runs = RunStore(db_path)
        await runs.list_runs()
        runs.close()
        with closing(sqlite3.connect(db_path)) as conn:
            assert conn.execute("SELECT native_session_id FROM sessions").fetchone() == (
                "runtime-1",
            )

    store = EventStore(db_path)
    session = store.get_session_sync("s1")
    assert session is not None
    assert session["native_session_id"] == "runtime-1"
    assert store.list_sessions_sync()[0]["native_session_id"] == "runtime-1"


def _make_event(session_id: str = "s1", iteration: int = 0) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.NOTIFICATION,
        data={"subtype": "init"},
    )


@pytest.mark.asyncio
async def test_get_events_since_zero_returns_all(tmp_store: EventStore) -> None:
    for _ in range(3):
        await tmp_store.store(_make_event())

    new_id, events = tmp_store.get_events_since_sync(0)

    assert new_id > 0
    assert len(events) == 3


@pytest.mark.asyncio
async def test_get_events_since_returns_only_newer(tmp_store: EventStore) -> None:
    for _ in range(3):
        await tmp_store.store(_make_event())
    id_after_first_batch, _ = tmp_store.get_events_since_sync(0)

    second_batch = [_make_event(), _make_event()]
    for e in second_batch:
        await tmp_store.store(e)

    new_id, events = tmp_store.get_events_since_sync(id_after_first_batch)

    assert new_id > id_after_first_batch
    assert len(events) == 2
    returned_ids = {e.id for e in events}
    assert returned_ids == {e.id for e in second_batch}


@pytest.mark.asyncio
async def test_get_events_since_no_new_returns_empty(tmp_store: EventStore) -> None:
    for _ in range(3):
        await tmp_store.store(_make_event())
    max_id, _ = tmp_store.get_events_since_sync(0)

    next_id, events = tmp_store.get_events_since_sync(max_id)

    assert next_id == max_id
    assert events == []


async def test_recent_and_replay_queries_return_only_bounded_tail(tmp_store: EventStore) -> None:
    events = [_make_event("s1", i) for i in range(6)]
    for event in events:
        await tmp_store.store(event)
    other = _make_event("other")
    await tmp_store.store(other)
    cursor = tmp_store.get_max_event_id_sync()
    assert [e.id for e in tmp_store.get_recent_events_sync("s1", 2)] == [
        events[4].id,
        events[5].id,
    ]
    assert [e.id for e in tmp_store.get_events_through_id_sync(cursor, 2)] == [
        events[5].id,
        other.id,
    ]


async def test_usage_query_filters_non_usage_events(tmp_store: EventStore) -> None:
    await tmp_store.store(_make_event())
    usage = Event.create("s1", 1, EventType.STEP_FINISH, {"usage": {"input_tokens": 3}})
    await tmp_store.store(usage)
    assert [e.id for e in tmp_store.get_usage_events_sync()] == [usage.id]


async def test_latest_session_outcome_is_read_by_session(tmp_store: EventStore) -> None:
    def completed(sid: str, iteration: int) -> Event:
        return Event.create(
            sid,
            iteration,
            EventType.SESSION_OUTCOME,
            {
                "session_id": sid,
                "kind": "completed",
                "stop_reason": "evaluator_complete",
                "last_error": None,
                "result_digest": "PASS",
                "iteration": iteration,
                "context": "node=w/verify",
            },
        )

    first = completed("verify-1", 1)
    latest = completed("verify-1", 2)
    for event in (first, completed("other", 1), latest):
        await tmp_store.store(event)
    assert tmp_store.get_latest_session_outcome_sync("verify-1").id == latest.id


@pytest.mark.asyncio
async def test_delete_session_removes_only_that_session(
    tmp_store: EventStore,
) -> None:
    e_a1 = _make_event(session_id="a")
    e_a2 = _make_event(session_id="a")
    e_b = _make_event(session_id="b")
    await tmp_store.store(e_a1)
    await tmp_store.store(e_a2)
    await tmp_store.store(e_b)

    tmp_store.delete_session("a")

    _, events = tmp_store.get_events_since_sync(0)
    assert {e.id for e in events} == {e_b.id}


def test_event_store_uses_wal_journal_mode(tmp_store: EventStore) -> None:
    with sqlite3.connect(str(tmp_store.db_path)) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"


async def test_wal_read_transaction_does_not_block_writes(tmp_store: EventStore) -> None:
    """WAL 下读者持读事务（导出型大读）不阻塞写者；读者快照隔离。"""
    await tmp_store.store(_make_event())
    reader = sqlite3.connect(str(tmp_store.db_path))
    try:
        reader.execute("BEGIN")
        assert reader.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        await tmp_store.store(_make_event())  # 读事务在途，写入照常落账
        assert reader.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    finally:
        reader.rollback()
        reader.close()
    _, events = tmp_store.get_events_since_sync(0)
    assert len(events) == 2


async def test_pk_persistent_monotonic_from_one(tmp_store: EventStore) -> None:
    """持久主键：自增且单调，从 1 起（重建后新序从头计）。"""
    for i in range(3):
        await tmp_store.store(_make_event(iteration=i))
    with sqlite3.connect(str(tmp_store.db_path)) as conn:
        pks = [r[0] for r in conn.execute("SELECT id FROM events ORDER BY id")]
    assert pks == [1, 2, 3]


async def test_uuid_is_idempotency_key(tmp_store: EventStore) -> None:
    """客户端 uuid 为幂等键：重放同 uuid 只留一行且保留原主键。"""
    event = _make_event()
    event.run_id = "run-1"
    await tmp_store.store(event)
    max_id, _ = tmp_store.get_events_since_sync(0)

    replayed = _make_event()
    replayed.id = event.id
    replayed.run_id = "run-2"
    await tmp_store.store(replayed)

    with sqlite3.connect(str(tmp_store.db_path)) as conn:
        rows = conn.execute("SELECT id, run_id FROM events WHERE uuid = ?", (event.id,)).fetchall()
    assert len(rows) == 1
    assert rows[0] == (max_id, "run-2")


async def test_run_dimensions_roundtrip(tmp_store: EventStore) -> None:
    event = _make_event()
    event.run_id = "run-abc"
    event.context = "node=s3"
    await tmp_store.store(event)

    _, events = tmp_store.get_events_since_sync(0)
    assert events[0].run_id == "run-abc"
    assert events[0].context == "node=s3"
    assert events[0].id == event.id  # 对外 id 仍是 uuid


async def test_store_run_event_and_fetch(tmp_store: EventStore) -> None:
    await tmp_store.store_run_event("run-1", "process_output", {"line": "hi"}, context="node=b")
    await tmp_store.store_run_event("run-1", "process_exit", {"rc": 0}, context="node=b")
    await tmp_store.store_run_event("run-2", "process_exit", {"rc": 1})

    rows = tmp_store.get_run_events_sync("run-1")
    assert [r["event_type"] for r in rows] == ["process_output", "process_exit"]
    assert rows[0]["data"] == {"line": "hi"} and rows[0]["context"] == "node=b"

    first_id = rows[0]["id"]
    tail = tmp_store.get_run_events_sync("run-1", since_id=first_id)
    assert [r["event_type"] for r in tail] == ["process_exit"]

    assert tmp_store.get_run_events_sync("run-2")[0]["data"]["rc"] == 1


async def test_run_events_global_increment_advances_watermark(
    tmp_store: EventStore,
) -> None:
    await tmp_store.store_run_event("run-1", "process_output", {"line": "a"})
    await tmp_store.store_run_event("run-2", "process_exit", {"rc": 0})

    max_id, rows = tmp_store.get_run_events_since_sync(0)
    assert max_id == tmp_store.get_max_run_event_id_sync()
    assert {r["run_id"] for r in rows} == {"run-1", "run-2"}

    await tmp_store.store_run_event("run-1", "process_output", {"line": "b"})
    new_max, tail = tmp_store.get_run_events_since_sync(max_id)
    assert new_max > max_id
    assert [r["data"]["line"] for r in tail] == ["b"]

    _, empty = tmp_store.get_run_events_since_sync(new_max)
    assert empty == []


async def test_run_events_global_increment_batches_by_limit(tmp_store: EventStore) -> None:
    """SSE 全局通道单拍读数有界：limit 只读上限行、游标推进到已读最大 id，余量交下一拍。"""
    for i in range(3):
        await tmp_store.store_run_event("run-1", "process_output", {"line": str(i)})

    first_max, first_rows = tmp_store.get_run_events_since_sync(0, limit=2)
    assert len(first_rows) == 2
    assert first_max == first_rows[-1]["id"]

    second_max, second_rows = tmp_store.get_run_events_since_sync(first_max, limit=2)
    assert len(second_rows) == 1
    assert second_max == second_rows[0]["id"]

    _, rest = tmp_store.get_run_events_since_sync(second_max, limit=2)
    assert rest == []
