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

"""节点累计时钟账本与 run 记录库 v6 迁移测试。"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.runs import SCHEMA_VERSION, RunStore
from workflow_controller.watchdog.ledger import NodeClockLedger
from workflow_controller.watchdog.state import NodeClock

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "events.db"


@pytest.fixture
async def ledger(db_path: Path):
    """先经 run 记录库统一迁移铺表，再开账本。"""
    await RunStore(db_path).list_runs()
    store = NodeClockLedger(db_path)
    yield store
    store.close()


def _clock(**overrides: object) -> NodeClock:
    """一行时钟账（默认值可按测试覆写）。"""
    fields: dict[str, object] = {
        "run_id": "run_1",
        "node_id": "n1",
        "gen": 2,
        "threshold_seconds": 600,
        "started_at": datetime.now(tz=UTC) - timedelta(seconds=100),
        "accumulated_seconds": 42.5,
        "running_since": None,
        "fired": False,
        "attempt": 3,
        "session_id": "ses-exec",
        "verify_session_id": "ses-verify",
    }
    fields.update(overrides)
    return NodeClock(**fields)  # pyright: ignore[reportCallIssue]


class TestSchemaMigration:
    async def test_migration_creates_table_and_stamps_version(self, db_path: Path) -> None:
        await RunStore(db_path).list_runs()  # 首次访问触发迁移（构造零落盘）

        with sqlite3.connect(db_path) as conn:
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert "watchdog_node_clocks" in tables
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
            columns = [r[1] for r in conn.execute("PRAGMA table_info(watchdog_node_clocks)")]
            assert columns == [
                "run_id",
                "node_id",
                "gen",
                "threshold_seconds",
                "accumulated_seconds",
                "running_since",
                "started_at",
                "fired",
                "attempt",
                "session_id",
                "verify_session_id",
                "updated_at",
            ]
            indexes = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
            }
            assert "idx_watchdog_node_clocks_run" in indexes

    async def test_previous_version_db_upgraded_in_one_step(self, db_path: Path) -> None:
        await RunStore(db_path).list_runs()
        with sqlite3.connect(db_path) as conn:  # 降回上一档形状：删新表、盖旧档
            conn.execute("DROP TABLE watchdog_node_clocks")
            conn.execute("DROP INDEX IF EXISTS idx_watchdog_node_clocks_run")
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION - 1}")

        await RunStore(db_path).list_runs()  # 重开一次性升档

        with sqlite3.connect(db_path) as conn:
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert "watchdog_node_clocks" in tables
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION

    async def test_ledger_requires_migrated_table(self, tmp_path: Path) -> None:
        bare = NodeClockLedger(tmp_path / "empty" / "events.db")  # 未经统一迁移
        assert await bare.load_all() == []  # 读路径容忍缺席（boot 早于 run 记账库初始化）
        with pytest.raises(RuntimeError, match="watchdog_node_clocks"):
            await bare.save(_clock(), "t")
        bare.close()


class TestLedgerRoundTrip:
    async def test_save_and_load_round_trip(self, ledger: NodeClockLedger) -> None:
        running_since = datetime.now(tz=UTC) - timedelta(seconds=10)
        await ledger.save(_clock(running_since=running_since), "2026-11-25T00:00:00+00:00")
        await ledger.save(_clock(gen=3, fired=True), "2026-11-25T00:01:00+00:00")

        loaded = {(c.run_id, c.node_id, c.gen): c for c in await ledger.load_all()}
        assert set(loaded) == {("run_1", "n1", 2), ("run_1", "n1", 3)}

        open_segment = loaded[("run_1", "n1", 2)]
        assert open_segment.threshold_seconds == 600
        assert open_segment.accumulated_seconds == 42.5
        assert open_segment.running_since == running_since
        assert not open_segment.fired
        assert open_segment.attempt == 3
        assert open_segment.session_id == "ses-exec"
        assert open_segment.verify_session_id == "ses-verify"

        fired_row = loaded[("run_1", "n1", 3)]
        assert fired_row.fired
        assert fired_row.running_since is None

    async def test_save_is_idempotent_overwrite(self, ledger: NodeClockLedger) -> None:
        await ledger.save(_clock(accumulated_seconds=1.0), "t1")
        await ledger.save(_clock(accumulated_seconds=99.0), "t2")
        (only,) = await ledger.load_all()
        assert only.accumulated_seconds == 99.0

    async def test_run_delete_cascades_clock_rows(self, db_path: Path) -> None:
        from workflow_controller.store.event_store import EventStore

        EventStore(db_path)  # eval/handoff 族表随事件库初始化（delete 级联面）
        store = RunStore(db_path)
        run = await store.create("prog", "fake@1")
        ledger = NodeClockLedger(db_path)
        await ledger.save(_clock(run_id=run.run_id, started_at=datetime.now(tz=UTC)), "t")
        ledger.close()

        await store.delete_run(run.run_id)

        with sqlite3.connect(db_path) as conn:
            count = conn.execute(
                "SELECT count(*) FROM watchdog_node_clocks WHERE run_id = ?", (run.run_id,)
            ).fetchone()[0]
        assert count == 0
