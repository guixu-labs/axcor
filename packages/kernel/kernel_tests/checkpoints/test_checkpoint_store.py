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

"""作用域检查点账本测试：建表迁移、与遗留插件旧表并存、行读写与形态校验。

内核测试不得 import 生产插件，旧表并存断言以 sqlite 直查完成：在库内按插件
的旧列集复刻旧表（该表形状在插件退役前冻结），核对统一迁移不触碰它、其旧
形态写入照常可用。
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.checkpoints import (
    BarrierQuality,
    CheckpointStore,
    CheckpointTrigger,
    DispatchAnchor,
    ScopeCheckpoint,
)
from workflow_controller.core.runs import SCHEMA_VERSION, RunStore
from workflow_controller.core.workspace import ScopeKind

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

# 冻结的表列清单（列名 + 可空性探针用）
_EXPECTED_COLUMNS = [
    "checkpoint_id",
    "scope_id",
    "scope_kind",
    "parent_scope_id",
    "scope_epoch",
    "root_epoch",
    "trigger",
    "barrier_quality",
    "workspace_branch",
    "workspace_commit",
    "integration_commit",
    "dispatch_anchor",
    "child_session_map",
    "claims",
    "orchestration_state_ref",
    "created_at",
    "file_count",
    "byte_size",
    "status",
]

# 遗留插件的旧检查点表形状（退役前冻结，此处按旧列集复刻做并存断言）
_LEGACY_CHECKPOINTS_DDL = """
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

_LEGACY_INSERT = (
    "INSERT OR REPLACE INTO checkpoints "
    "(checkpoint_id, session_id, iteration, agent_id, agent_type, "
    "trigger, git_ref, event_rowid, main_part_id, parent_checkpoint_id, "
    "created_at, file_count, byte_size, status, inner_git) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


def _checkpoint(**overrides: object) -> ScopeCheckpoint:
    """默认形态的检查点行（主写者的作用域起点），按需覆写字段。"""
    fields: dict[str, object] = dict(
        checkpoint_id="cp-1",
        scope_id="node-a",
        scope_kind=ScopeKind.MAIN_WRITER,
        scope_epoch=2,
        root_epoch=1,
        trigger=CheckpointTrigger.SCOPE_START,
        barrier_quality=BarrierQuality.PRECISE,
        workspace_commit="commit-a1",
        created_at="2026-11-24T10:00:00+00:00",
        parent_scope_id="run-root",
        workspace_branch="scopes/node-a",
        integration_commit="commit-line",
        orchestration_state_ref="driver-state#4",
        file_count=12,
        byte_size=3456,
    )
    fields.update(overrides)
    return ScopeCheckpoint(**fields)  # type: ignore[arg-type]


def _table_columns(db_path: Path, table: str) -> list[tuple[object, ...]]:
    """PRAGMA table_info 原始行（列名、类型、非空标记一并比对）。"""
    with sqlite3.connect(db_path) as conn:
        return conn.execute(f"PRAGMA table_info({table})").fetchall()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "events.db"


@pytest.fixture
async def store(db_path: Path) -> AsyncIterator[CheckpointStore]:
    """先经 run 记录库统一迁移铺表，再开检查点账本。"""
    await RunStore(db_path).list_runs()
    checkpoints = CheckpointStore(db_path)
    yield checkpoints
    checkpoints.close()


class TestSchemaMigration:
    async def test_migration_creates_table_and_stamps_version(self, db_path: Path) -> None:
        await RunStore(db_path).list_runs()  # 首次访问触发迁移（构造零落盘）

        with sqlite3.connect(db_path) as conn:
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert "scope_checkpoints" in tables
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
            columns = [r[1] for r in conn.execute("PRAGMA table_info(scope_checkpoints)")]
            assert columns == _EXPECTED_COLUMNS
            # 主键列在 table_info 里不标非空（非整数主键的历史行为），单独以主键探针核对
            pk_columns = [
                r[1] for r in conn.execute("PRAGMA table_info(scope_checkpoints)") if r[5]
            ]
            assert pk_columns == ["checkpoint_id"]
            not_null = {r[1] for r in conn.execute("PRAGMA table_info(scope_checkpoints)") if r[3]}
            assert {
                "scope_id",
                "scope_kind",
                "scope_epoch",
                "root_epoch",
                "trigger",
                "barrier_quality",
                "created_at",
            } <= not_null
            indexes = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
            }
            assert "idx_scope_checkpoints_scope" in indexes

    async def test_migration_idempotent_on_reopen(self, db_path: Path) -> None:
        await RunStore(db_path).list_runs()
        with sqlite3.connect(db_path) as conn:
            before = conn.execute("SELECT count(*) FROM scope_checkpoints").fetchone()[0]

        reopened = RunStore(db_path)  # 已盖戳不重跑
        await reopened.list_runs()

        with sqlite3.connect(db_path) as conn:
            assert conn.execute("SELECT count(*) FROM scope_checkpoints").fetchone()[0] == before
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION

    async def test_previous_version_db_upgraded_in_one_step(self, db_path: Path) -> None:
        await RunStore(db_path).list_runs()
        with sqlite3.connect(db_path) as conn:  # 降回上一档形状：删新表、盖旧档
            conn.execute("DROP TABLE scope_checkpoints")
            conn.execute("DROP INDEX IF EXISTS idx_scope_checkpoints_scope")
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION - 1}")

        await RunStore(db_path).list_runs()  # 重开一次性升档

        with sqlite3.connect(db_path) as conn:
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert "scope_checkpoints" in tables
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION

    async def test_rows_survive_store_restart(self, db_path: Path) -> None:
        first = CheckpointStore(db_path)
        await RunStore(db_path).list_runs()
        await first.record(_checkpoint())
        first.close()

        second = CheckpointStore(db_path)  # 重启加载：新实例读回既有行
        loaded = await second.get("cp-1")
        second.close()
        assert loaded is not None
        assert loaded.workspace_commit == "commit-a1"

    async def test_store_requires_migrated_table(self, tmp_path: Path) -> None:
        bare = CheckpointStore(tmp_path / "empty" / "events.db")  # 未经 run 记录库迁移
        with pytest.raises(RuntimeError, match="scope_checkpoints"):
            await bare.record(_checkpoint())
        bare.close()


class TestLegacyTableCoexistence:
    async def test_migration_leaves_legacy_table_untouched(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:  # 插件先在库内落旧表与旧行
            conn.execute(_LEGACY_CHECKPOINTS_DDL)
            conn.execute(
                _LEGACY_INSERT,
                (
                    "legacy-cp-1",
                    "sess-1",
                    3,
                    "agent-x",
                    "default",
                    "subagent_stop",
                    "refs/snap/legacy-cp-1",
                    42,
                    "part-9",
                    None,
                    "2026-11-01T00:00:00+00:00",
                    5,
                    100,
                    "ok",
                    "captured",
                ),
            )
        before = _table_columns(db_path, "checkpoints")

        await RunStore(db_path).list_runs()  # 统一迁移在新表上执行

        assert _table_columns(db_path, "checkpoints") == before  # 旧表结构零改动
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT checkpoint_id, git_ref FROM checkpoints WHERE checkpoint_id = 'legacy-cp-1'"
            ).fetchone()
            assert row == ("legacy-cp-1", "refs/snap/legacy-cp-1")
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        assert {"checkpoints", "scope_checkpoints"} <= tables  # 两表并存

    async def test_legacy_insert_still_works_after_migration(
        self, store: CheckpointStore, db_path: Path
    ) -> None:
        with sqlite3.connect(db_path) as conn:  # 插件在迁移后的库内建旧表并旧形态写入
            conn.execute(_LEGACY_CHECKPOINTS_DDL)
            conn.execute(
                _LEGACY_INSERT,
                (
                    "legacy-cp-2",
                    "sess-1",
                    4,
                    "agent-y",
                    "default",
                    "subagent_stop",
                    None,
                    None,
                    None,
                    "legacy-cp-1",
                    "2026-11-02T00:00:00+00:00",
                    0,
                    0,
                    "ok",
                    "",
                ),
            )
            row = conn.execute(
                "SELECT parent_checkpoint_id FROM checkpoints WHERE checkpoint_id = 'legacy-cp-2'"
            ).fetchone()
        assert row == ("legacy-cp-1",)

        await store.record(_checkpoint())  # 新账本写入不受旧表牵连
        assert (await store.get("cp-1")) is not None


class TestCheckpointRoundtrip:
    async def test_scope_start_roundtrip_all_fields(self, store: CheckpointStore) -> None:
        await store.record(_checkpoint())

        loaded = await store.get("cp-1")
        assert loaded == _checkpoint()
        assert loaded.scope_epoch == 2  # 代次字段按整数回读
        assert loaded.root_epoch == 1
        assert loaded.trigger is CheckpointTrigger.SCOPE_START
        assert loaded.barrier_quality is BarrierQuality.PRECISE
        assert loaded.scope_kind is ScopeKind.MAIN_WRITER
        assert loaded.dispatch_anchor is None  # 非屏障检查点无锚点
        assert loaded.claims == ()

    async def test_before_child_roundtrip_anchor_claims_and_child_map(
        self, store: CheckpointStore
    ) -> None:
        anchor = DispatchAnchor(
            parent_native_session_id="native-1",
            parent_message_id="msg_01",
            tool_use_id="toolu_01",
            preallocated_scope_id="agent-m1",
            exclude_message_id="msg_01",
            dispatch_group_id="group-7",
        )
        await store.record(
            _checkpoint(
                checkpoint_id="cp-group",
                scope_id="node-a",
                trigger=CheckpointTrigger.BEFORE_CHILD,
                barrier_quality=BarrierQuality.LATE_BARRIER,
                dispatch_anchor=anchor,
                child_session_map={"agent-m1": "native-child-1"},
                claims=("op/op_kernel/**", "op/build/autogen/**"),
                workspace_commit="commit-group",
            )
        )

        loaded = await store.get("cp-group")
        assert loaded is not None
        assert loaded.dispatch_anchor == anchor  # 六元组 JSON 往返逐字段一致
        assert loaded.child_session_map == {"agent-m1": "native-child-1"}
        assert loaded.claims == ("op/op_kernel/**", "op/build/autogen/**")
        assert loaded.barrier_quality is BarrierQuality.LATE_BARRIER

    async def test_terminal_trigger_accepted(self, store: CheckpointStore) -> None:
        await store.record(
            _checkpoint(trigger=CheckpointTrigger.TERMINAL, workspace_commit="commit-final")
        )
        loaded = await store.get("cp-1")
        assert loaded is not None
        assert loaded.trigger is CheckpointTrigger.TERMINAL

    async def test_record_is_idempotent_on_checkpoint_id(
        self, store: CheckpointStore, db_path: Path
    ) -> None:
        await store.record(_checkpoint())
        await store.record(  # 崩溃补账重放：同标识覆盖不报错
            _checkpoint(workspace_commit="commit-a2", file_count=20, byte_size=6000)
        )

        loaded = await store.get("cp-1")
        assert loaded is not None
        assert loaded.workspace_commit == "commit-a2"
        with sqlite3.connect(db_path) as conn:
            count = conn.execute(
                "SELECT count(*) FROM scope_checkpoints WHERE checkpoint_id = 'cp-1'"
            ).fetchone()[0]
        assert count == 1

    async def test_list_for_scope_orders_and_filters(self, store: CheckpointStore) -> None:
        await store.record(
            _checkpoint(checkpoint_id="cp-b", created_at="2026-11-24T10:00:02+00:00")
        )
        await store.record(
            _checkpoint(checkpoint_id="cp-a", created_at="2026-11-24T10:00:01+00:00")
        )
        await store.record(
            _checkpoint(
                checkpoint_id="cp-other",
                scope_id="node-b",
                created_at="2026-11-24T09:00:00+00:00",
            )
        )

        listed = await store.list_for_scope("node-a")
        assert [item.checkpoint_id for item in listed] == ["cp-a", "cp-b"]
        assert [item.scope_id for item in await store.list_for_scope("node-b")] == ["node-b"]
        assert await store.list_for_scope("missing") == []
        assert await store.get("missing") is None

    async def test_list_for_tree_anchors_on_root(self, store: CheckpointStore) -> None:
        """树查询以根锚定：根行与直接子作用域行命中，他树行排除。"""
        await store.record(
            _checkpoint(
                checkpoint_id="cp-root",
                scope_id="run-1-root",
                parent_scope_id=None,
                created_at="2026-11-24T10:00:00+00:00",
            )
        )
        await store.record(
            _checkpoint(
                checkpoint_id="cp-child",
                scope_id="node-a",
                parent_scope_id="run-1-root",
                created_at="2026-11-24T10:00:01+00:00",
            )
        )
        await store.record(
            _checkpoint(
                checkpoint_id="cp-other-tree",
                scope_id="node-x",
                parent_scope_id="run-2-root",
                created_at="2026-11-24T10:00:02+00:00",
            )
        )

        listed = await store.list_for_tree("run-1-root")
        assert [item.checkpoint_id for item in listed] == ["cp-root", "cp-child"]
        assert await store.list_for_tree("run-9-root") == []

    async def test_attach_child_session_backfills_and_is_idempotent(
        self, store: CheckpointStore
    ) -> None:
        anchor = DispatchAnchor(
            parent_native_session_id="native-1",
            parent_message_id="msg_01",
            tool_use_id="toolu_01",
            preallocated_scope_id="agent-m1",
            exclude_message_id="msg_01",
            dispatch_group_id="group-7",
        )
        await store.record(
            _checkpoint(
                checkpoint_id="cp-group",
                trigger=CheckpointTrigger.BEFORE_CHILD,
                dispatch_anchor=anchor,
                workspace_commit="commit-group",
            )
        )

        await store.attach_child_session("cp-group", "agent-m1", "native-child-1")
        await store.attach_child_session("cp-group", "agent-m2", "native-child-2")
        await store.attach_child_session("cp-group", "agent-m1", "native-child-1")  # 重填幂等

        loaded = await store.get("cp-group")
        assert loaded is not None
        assert loaded.child_session_map == {
            "agent-m1": "native-child-1",
            "agent-m2": "native-child-2",
        }

    async def test_attach_rejected_outside_group_checkpoint(self, store: CheckpointStore) -> None:
        await store.record(_checkpoint())
        with pytest.raises(ValueError, match="派发屏障"):
            await store.attach_child_session("cp-1", "agent-m1", "native-child-1")

    async def test_attach_missing_checkpoint_raises(self, store: CheckpointStore) -> None:
        with pytest.raises(LookupError, match="cp-ghost"):
            await store.attach_child_session("cp-ghost", "agent-m1", "native-child-1")


class TestRecordValidation:
    async def test_empty_workspace_commit_rejected(self, store: CheckpointStore) -> None:
        with pytest.raises(ValueError, match="workspace_commit 必填"):
            await store.record(_checkpoint(workspace_commit=""))
        assert await store.get("cp-1") is None  # 拒绝发生在落行前

    async def test_anchor_rejected_outside_before_child(self, store: CheckpointStore) -> None:
        anchor = DispatchAnchor(
            parent_native_session_id="native-1",
            parent_message_id="msg_01",
            tool_use_id="toolu_01",
            preallocated_scope_id="agent-m1",
            exclude_message_id="msg_01",
            dispatch_group_id="group-7",
        )
        with pytest.raises(ValueError, match="派发屏障检查点"):
            await store.record(
                _checkpoint(trigger=CheckpointTrigger.TERMINAL, dispatch_anchor=anchor)
            )

    async def test_late_barrier_rejected_outside_before_child(self, store: CheckpointStore) -> None:
        with pytest.raises(ValueError, match="滞后屏障"):
            await store.record(
                _checkpoint(
                    trigger=CheckpointTrigger.SCOPE_START,
                    barrier_quality=BarrierQuality.LATE_BARRIER,
                )
            )

    async def test_non_positive_epoch_rejected(self, store: CheckpointStore) -> None:
        with pytest.raises(ValueError, match="不得小于 1"):
            await store.record(_checkpoint(scope_epoch=0))
        with pytest.raises(ValueError, match="不得小于 1"):
            await store.record(_checkpoint(root_epoch=0))

    async def test_enum_literals_required(self, store: CheckpointStore) -> None:
        with pytest.raises(ValueError, match="作用域种类枚举"):
            await store.record(_checkpoint(scope_kind="main_writer"))  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="检查点触发枚举"):
            await store.record(_checkpoint(trigger="scope_start"))  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="屏障质量枚举"):
            await store.record(_checkpoint(barrier_quality="precise"))  # type: ignore[arg-type]

    async def test_json_columns_stored_as_text(self, store: CheckpointStore, db_path: Path) -> None:
        anchor = DispatchAnchor(
            parent_native_session_id="native-1",
            parent_message_id="msg_01",
            tool_use_id="toolu_01",
            preallocated_scope_id="agent-m1",
            exclude_message_id="msg_01",
            dispatch_group_id="group-7",
        )
        await store.record(
            _checkpoint(
                checkpoint_id="cp-group",
                trigger=CheckpointTrigger.BEFORE_CHILD,
                dispatch_anchor=anchor,
                claims=("op/build/**",),
                workspace_commit="commit-group",
            )
        )
        with sqlite3.connect(db_path) as conn:
            raw = conn.execute(
                "SELECT dispatch_anchor, claims FROM scope_checkpoints WHERE checkpoint_id = ?",
                ("cp-group",),
            ).fetchone()
        assert set(json.loads(raw[0])) == {
            "parent_native_session_id",
            "parent_message_id",
            "tool_use_id",
            "preallocated_scope_id",
            "exclude_message_id",
            "dispatch_group_id",
        }
        assert json.loads(raw[1]) == ["op/build/**"]
