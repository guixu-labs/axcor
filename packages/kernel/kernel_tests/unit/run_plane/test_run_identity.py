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

"""Run 身份（ARCHITECTURE.md「Run 身份」）——宿主级 run 记账层。

件 2：SessionConfig.run_id / labels 照存不解析 + Session.run_id 创建时继承。
件 1a：RunRecord / RunStore——runs 表与 run 名下 session 清单的记账。
"""

from __future__ import annotations

import pytest

from workflow_controller.api.session_api import SessionAPI
from workflow_controller.config.settings import SessionConfig
from workflow_controller.core.runs import (
    SCHEMA_VERSION,
    RunKind,
    RunNotFoundError,
    RunStatus,
    RunStore,
)
from workflow_controller.session.session import Session

from .test_run_recovery import (
    _boot_orch,
    _fake_activate,
)


class TestSessionRunAttribution:
    @staticmethod
    def test_config_stores_run_id_and_labels_opaquely():
        cfg = SessionConfig(
            name="node-1",
            run_id="run-abc123",
            labels={"run_id": "run-abc123", "node_id": "node-1", "format": "dag-yaml@1"},
        )
        assert cfg.run_id == "run-abc123"
        assert cfg.labels["node_id"] == "node-1"

    @staticmethod
    def test_session_inherits_run_id_from_config():
        session = Session(config=SessionConfig(name="node-1", run_id="run-abc123"))
        assert session.run_id == "run-abc123"

    @staticmethod
    def test_session_run_id_none_when_unattributed():
        assert Session(name="plain").run_id is None
        assert Session(config=SessionConfig(name="plain")).run_id is None

    @staticmethod
    def test_config_roundtrip_preserves_run_id():
        """config_json 落库往返不丢归属（model_dump 自动带新字段，钉住这个前提）。"""
        cfg = SessionConfig(name="node-1", run_id="run-abc123", labels={"k": "v"})
        restored = SessionConfig.model_validate(cfg.model_dump())
        assert restored.run_id == "run-abc123"
        assert restored.labels == {"k": "v"}


class TestRunStore:
    async def test_create_mints_running_record(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create(
            "release-notes@0.1.0",
            "dag-yaml@1",
            labels={"project": "demo"},
            params={"env": "staging"},
        )
        assert rec.status == "running" and rec.finished_at is None
        assert rec.labels == {"project": "demo"} and rec.params == {"env": "staging"}
        assert len(rec.run_id) == 12  # session id 风格短 uuid
        got = await store.get(rec.run_id)
        assert got.program_ref == "release-notes@0.1.0" and got.format == "dag-yaml@1"

    async def test_get_unknown_raises(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        with pytest.raises(RunNotFoundError):
            await store.get("no-such-run")

    async def test_finish_is_the_only_terminal_write(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        done = await store.finish(rec.run_id, "completed")
        assert done.record.status == "completed" and done.record.finished_at is not None
        assert done.was_applied is True
        assert (await store.get(rec.run_id)).status == "completed"

    async def test_finish_same_status_is_idempotent(self, tmp_path):
        """收尾重试（调用方闪断重发）同状态幂等，不炸也不改判。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.finish(rec.run_id, "stopped")
        again = await store.finish(rec.run_id, "stopped")
        assert again.record.status == "stopped" and again.was_applied is False

    async def test_finish_conflicting_status_rejected(self, tmp_path):
        """终态不许改判：stop 中崩溃重启又被写成 completed = 账面说谎。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.finish(rec.run_id, "stopped")
        with pytest.raises(ValueError, match="stopped"):
            await store.finish(rec.run_id, "completed")

    async def test_finish_invalid_status_rejected(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        with pytest.raises(ValueError):
            await store.finish(rec.run_id, "running")
        with pytest.raises(ValueError):
            await store.finish(rec.run_id, "bogus")

    async def test_finish_unknown_run_raises(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        with pytest.raises(RunNotFoundError):
            await store.finish("no-such-run", "completed")

    async def test_attach_session_and_list(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.attach_session(rec.run_id, "sess-1")
        await store.attach_session(rec.run_id, "sess-2")
        await store.attach_session(rec.run_id, "sess-1")  # 幂等
        assert await store.list_session_ids(rec.run_id) == ["sess-1", "sess-2"]
        with pytest.raises(RunNotFoundError):  # unknown 不许静默空表
            await store.list_session_ids("no-such-run")

    async def test_attach_session_to_other_run_rejected(self, tmp_path):
        """UNIQUE(session_id) 存储层强制：session 恰属一个 run，跨 run 冲突报错。"""
        store = RunStore(tmp_path / "events.db")
        r1 = await store.create("p", "dag-yaml@1")
        r2 = await store.create("p2", "dag-yaml@1")
        await store.attach_session(r1.run_id, "sess-1")
        with pytest.raises(ValueError, match="已属于 run"):
            await store.attach_session(r2.run_id, "sess-1")

    async def test_attach_with_native_id_clears_previous_holder(self, tmp_path):
        """最新绑定唯一持有人在 attach 落行同款生效：同 id 旧持有行先清。"""
        store = RunStore(tmp_path / "events.db")
        r1 = await store.create("p", "dag-yaml@1")
        r2 = await store.create("p2", "dag-yaml@1")
        await store.attach_session(r1.run_id, "sess-old", native_session_id="native-1")
        await store.attach_session(r2.run_id, "sess-new", native_session_id="native-1")

        assert await store.find_session_by_native_id("native-1") == "sess-new"
        old = (await store.list_run_sessions(r1.run_id))[0]
        assert old.session_id == "sess-old" and old.native_session_id is None

    async def test_attach_idempotent_reattach_keeps_native_discipline(self, tmp_path):
        """幂等重挂（已属本 run）带 native id：同款唯一持有人记账，不静默丢弃。"""
        store = RunStore(tmp_path / "events.db")
        r1 = await store.create("p", "dag-yaml@1")
        r2 = await store.create("p2", "dag-yaml@1")
        await store.attach_session(r1.run_id, "sess-1")
        await store.attach_session(r2.run_id, "sess-other", native_session_id="native-1")

        await store.attach_session(r1.run_id, "sess-1", native_session_id="native-1")
        assert await store.find_session_by_native_id("native-1") == "sess-1"
        mine = (await store.list_run_sessions(r1.run_id))[0]
        assert mine.session_id == "sess-1" and mine.native_session_id == "native-1"
        other = (await store.list_run_sessions(r2.run_id))[0]
        assert other.session_id == "sess-other" and other.native_session_id is None

        # 同值幂等重挂：返回 False（幂等 no-op），持有关系不变
        assert (
            await store.attach_session(r1.run_id, "sess-1", native_session_id="native-1") is False
        )
        assert await store.find_session_by_native_id("native-1") == "sess-1"

    async def test_records_survive_reopen(self, tmp_path):
        """events.db 落盘可重开——崩溃恢复与 GET /runs/{id} 都靠它。"""
        db = tmp_path / "events.db"
        store = RunStore(db)
        rec = await store.create("p", "skill-md")
        await store.attach_session(rec.run_id, "sess-1")
        await store.finish(rec.run_id, "failed", error="boom")
        reopened = RunStore(db)
        got = await reopened.get(rec.run_id)
        assert got.status == "failed" and got.error == "boom"
        assert await reopened.list_session_ids(rec.run_id) == ["sess-1"]


class TestLifecycleApi:
    async def test_create_pending_then_update_to_running_paused(self, tmp_path):
        """start_run 写序：mint(pending) → running → paused → running。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1", status=RunStatus.PENDING)
        assert rec.status == "pending" and rec.finished_at is None
        r1 = await store.update_status(rec.run_id, "running")
        assert r1.status == "running"
        r2 = await store.update_status(rec.run_id, RunStatus.PAUSED)
        assert r2.status == "paused"
        r3 = await store.update_status(rec.run_id, "running")
        assert r3.status == "running"

    async def test_create_rejects_terminal_status(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        with pytest.raises(ValueError, match="非终态"):
            await store.create("p", "dag-yaml@1", status="completed")

    async def test_update_status_rejects_terminal_target(self, tmp_path):
        """终态只能经 finish 落账——update_status 不开后门。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        for terminal in ("completed", "failed", "stopped"):
            with pytest.raises(ValueError, match="终态走 finish"):
                await store.update_status(rec.run_id, terminal)

    async def test_update_status_from_terminal_rejected(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.finish(rec.run_id, "stopped")
        with pytest.raises(ValueError, match="已终态"):
            await store.update_status(rec.run_id, "running")

    async def test_update_status_same_state_idempotent(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        again = await store.update_status(rec.run_id, "running")
        assert again.status == "running"

    async def test_update_status_unknown_run_raises(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        with pytest.raises(RunNotFoundError):
            await store.update_status("no-such-run", "running")

    async def test_reopen_clears_finished_and_audits(self, tmp_path):
        """reopen 清 finished_at/error、留 reopened_at 审计、回 running。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.finish(rec.run_id, "failed", error="boom")
        reopened = await store.reopen(rec.run_id)
        assert reopened.status == "running"
        assert reopened.finished_at is None and reopened.error is None
        assert reopened.reopened_at is not None
        got = await store.get(rec.run_id)
        assert got.reopened_at is not None and got.finished_at is None

    async def test_reopen_then_finish_and_reopen_again(self, tmp_path):
        """重开后可再次终态、再次重开（reopened_at 刷新）。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.finish(rec.run_id, "completed")
        await store.reopen(rec.run_id)
        await store.finish(rec.run_id, "stopped")
        reopened2 = await store.reopen(rec.run_id)
        assert reopened2.status == "running" and reopened2.reopened_at

    async def test_reopen_non_terminal_rejected(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        with pytest.raises(ValueError, match="无从 reopen"):
            await store.reopen(rec.run_id)

    async def test_reopen_unknown_run_raises(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        with pytest.raises(RunNotFoundError):
            await store.reopen("no-such-run")


class TestStatusEnum:
    async def test_unknown_status_falls_back_without_raising(self, tmp_path):
        """值集外读回原样透传（回退不抛）。"""
        import sqlite3

        db = tmp_path / "events.db"
        store = RunStore(db)
        rec = await store.create("p", "dag-yaml@1")
        with sqlite3.connect(db) as conn:
            conn.execute(
                "UPDATE runs SET status = 'weird-future', kind = 'exotic' WHERE run_id = ?",
                (rec.run_id,),
            )
        got = await store.get(rec.run_id)
        assert got.status == "weird-future" and got.kind == "exotic"


class TestColumnExtensions:
    async def test_create_with_kind_work_dir_max_parallel_roundtrip(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create(
            "p",
            "",
            kind=RunKind.CONTAINER,
            work_dir="/w/ascend",
            max_parallel=4,
        )
        assert rec.kind == "container" and rec.work_dir == "/w/ascend"
        assert rec.max_parallel == 4 and rec.pause_reason is None
        got = await store.get(rec.run_id)
        assert got.kind == "container" and got.work_dir == "/w/ascend"
        assert got.max_parallel == 4 and got.pause_reason is None

    async def test_create_rejects_kind_format_mismatch(self, tmp_path):
        """kind 与 format 一致性在 mint 单点校验。"""
        store = RunStore(tmp_path / "events.db")
        with pytest.raises(ValueError, match="不携带 format"):
            await store.create("p", "dag-yaml@1", kind=RunKind.CONTAINER)
        with pytest.raises(ValueError, match="非空 format"):
            await store.create("p", "", kind=RunKind.WORKFLOW)

    async def test_create_defaults_workflow_no_max_parallel(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        assert rec.kind == "workflow" and rec.work_dir is None
        assert rec.max_parallel is None and rec.pause_reason is None

    async def test_create_rejects_unknown_kind(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        with pytest.raises(ValueError, match="kind"):
            await store.create("p", "dag-yaml@1", kind="bogus")

    async def test_attach_with_role_context_native_roundtrip(self, tmp_path):
        import sqlite3

        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.attach_session(
            rec.run_id,
            "sess-v",
            role="verifier",
            context="node=design-s3#verify",
            native_session_id="cli-009",
        )
        await store.attach_session(rec.run_id, "sess-d")  # 默认 role=executor
        with sqlite3.connect(tmp_path / "events.db") as conn:
            conn.row_factory = sqlite3.Row
            rows = {
                r["session_id"]: r
                for r in conn.execute("SELECT * FROM run_sessions WHERE run_id = ?", (rec.run_id,))
            }
        assert rows["sess-v"]["role"] == "verifier"
        assert rows["sess-v"]["context"] == "node=design-s3#verify"
        assert rows["sess-v"]["native_session_id"] == "cli-009"
        assert rows["sess-d"]["role"] == "executor"
        assert rows["sess-d"]["context"] is None


class TestImplicitGuards:
    async def test_finish_rejects_implicit(self, tmp_path):
        """隐式 run 不进终态机——finish 对 implicit 拒绝服务。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "", kind=RunKind.IMPLICIT)
        with pytest.raises(ValueError, match="implicit"):
            await store.finish(rec.run_id, "completed")
        assert (await store.get(rec.run_id)).status == "running"  # 未被改写

    async def test_reopen_rejects_implicit(self, tmp_path):
        import sqlite3

        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "", kind=RunKind.IMPLICIT)
        db = tmp_path / "events.db"
        # 隐式 run 正常路径永不终态；构造越界终态行验证 reopen 守卫同样拒绝
        with sqlite3.connect(db) as conn:
            conn.execute(
                "UPDATE runs SET status='completed',"
                " finished_at='2026-09-01T00:00:00' WHERE run_id = ?",
                (rec.run_id,),
            )
        with pytest.raises(ValueError, match="implicit"):
            await store.reopen(rec.run_id)

    async def test_update_status_allows_implicit_projection(self, tmp_path):
        """投影写路径不受 implicit 守卫影响：running↔paused 随成员状态投影。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "", kind=RunKind.IMPLICIT)
        paused = await store.update_status(rec.run_id, RunStatus.PAUSED)
        assert paused.status == "paused"
        back = await store.update_status(rec.run_id, "running")
        assert back.status == "running"


class TestImplicitMint:
    """隐式铸造：mint 单事务落 run 行 + 成员行；三段规则在归属服务收口。"""

    async def test_mint_implicit_single_transaction(self, tmp_path):
        import sqlite3

        store = RunStore(tmp_path / "events.db")
        from datetime import UTC, datetime

        created = datetime(2026, 9, 4, 10, 0, 0, tzinfo=UTC)
        run_id = await store.mint_implicit(
            session_id="sess-1",
            role="worker",
            label="会话-1",
            created_at=created,
        )
        rec = await store.get(run_id)
        assert rec.kind == "implicit" and rec.status == "running"
        assert rec.labels == {"name": "会话-1"}  # label = session 名
        assert rec.created_at == created  # created_at 对齐 session
        assert rec.program_ref == "" and rec.format == ""
        assert await store.list_session_ids(run_id) == ["sess-1"]  # 成员行同事务
        with sqlite3.connect(tmp_path / "events.db") as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM run_sessions WHERE session_id = 'sess-1'").fetchone()
            assert row["run_id"] == run_id and row["role"] == "worker"
        # 隐式 run 无根目录——mint 不造 run 数据根
        assert not (tmp_path / ".workflow-controller" / "runs" / run_id).exists()

    async def test_reattach_same_run_backfills_context(self, tmp_path):
        """幂等重挂补记 context：激活钩子先挂（无 context）、分派方随后带
        context 重挂——事件打戳与结局回执都从归属缓存取 context，缓存丢了
        它、消费端（驱动按 context 核对）就收不到结局。
        """
        from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService

        store = RunStore(tmp_path / "events.db")
        run = await store.create("p", "dag-yaml@1")
        svc = SessionRunAffiliationService(store)
        assert await svc.attach("s1", run.run_id, role="executor") is True
        # 幂等重挂（同 run）：仍是 no-op（不重发审计），但 context 补进缓存
        assert await svc.attach("s1", run.run_id, role="executor", context="member#1") is False
        aff = svc.resolve("s1")
        assert aff.run_id == run.run_id and aff.context == "member#1"
        # 无 context 的重挂不动已有值
        assert await svc.attach("s1", run.run_id, role="executor") is False
        assert svc.resolve("s1").context == "member#1"

    async def test_set_context_updates_cache_and_row(self, tmp_path):
        """成员标签（分派 context）换代：账面行与归属缓存同写——事件打戳与
        结局回执都从缓存取 context，只写行不写缓存会让驱动侧按旧 context
        对不上账（RestartSession 换代场景实测踩出）。
        """
        from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService

        store = RunStore(tmp_path / "events.db")
        run = await store.create("p", "dag-yaml@1")
        svc = SessionRunAffiliationService(store)
        await svc.attach("s1", run.run_id, context="node#1")
        await svc.set_context("s1", "node@1#1")
        assert svc.resolve("s1").context == "node@1#1"
        (row,) = await store.list_run_sessions(run.run_id)
        assert row.context == "node@1#1"
        await svc.set_context("ghost", "x")  # 无归属：静默跳过（没有缓存投影可写）

    async def test_on_session_created_mints_and_writes_back(self, tmp_path):
        from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService

        svc = SessionRunAffiliationService(RunStore(tmp_path / "events.db"))
        cfg = SessionConfig(name="n")
        session = Session(config=cfg)
        ok = await svc.on_session_created(session, cfg)
        assert ok is True  # 隐式段：名册随 mint 单事务（由调用方组装）
        assert cfg.run_id is not None  # 回写：名册 config_json 与派生都看得到
        assert svc.resolve(session.id).run_id == cfg.run_id
        assert svc.resolve("ghost") is None

    async def test_on_session_created_skips_explicit(self, tmp_path):
        """显式段不在咽喉落账（留给激活后挂靠钩子：激活失败半截不进账面）。"""
        from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService

        store = RunStore(tmp_path / "events.db")
        explicit = await store.create("p", "dag-yaml@1")
        svc = SessionRunAffiliationService(store)
        cfg = SessionConfig(name="n", run_id=explicit.run_id)
        session = Session(config=cfg)
        assert await svc.on_session_created(session, cfg) is False
        assert svc.resolve(session.id) is None  # 咽喉未落账
        assert await store.list_session_ids(explicit.run_id) == []

    async def test_mint_implicit_with_roster_single_transaction(self, tmp_path):
        """mint 与名册写单事务：run 行、成员行、名册行一并落账。"""
        import json
        import sqlite3

        store = RunStore(tmp_path / "events.db")
        run_id = await store.mint_implicit(
            session_id="sess-1",
            role="worker",
            label="会话-1",
            roster={
                "id": "sess-1",
                "name": "会话-1",
                "role": "worker",
                "state": "idle",
                "loop_status": "yes",
                "config_json": '{"name": "会话-1", "run_id": null}',
                "created_at": "2026-09-10T10:00:00",
                "last_activity": "2026-09-10T10:00:00",
            },
        )
        with sqlite3.connect(tmp_path / "events.db") as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM sessions WHERE id = 'sess-1'").fetchone()
        assert row is not None and row["name"] == "会话-1"
        cfg = json.loads(row["config_json"])
        assert cfg["run_id"] == run_id  # 旁证随单事务注入，不等下一次 persist
        assert cfg["name"] == "会话-1"
        assert await store.list_session_ids(run_id) == ["sess-1"]

    async def test_discard_affiliation_cleans_memberless_implicit_run(self, tmp_path):
        """回滚清理：归属行删除，无成员的隐式 run 连行清（不留孤儿账面行）。"""
        store = RunStore(tmp_path / "events.db")
        run_id = await store.mint_implicit(session_id="sess-1", role="worker", label="n")
        assert await store.discard_affiliation("sess-1") == run_id
        with pytest.raises(RunNotFoundError):
            await store.get(run_id)
        assert await store.discard_affiliation("sess-1") is None  # 幂等

    async def test_discard_affiliation_keeps_run_with_other_sessions(self, tmp_path):
        """同 run 尚有成员（谱系段加入）：只删该归属行，run 保留。"""
        store = RunStore(tmp_path / "events.db")
        run_id = await store.mint_implicit(session_id="sess-1", role="worker", label="n")
        await store.attach_session(run_id, "sess-2")
        assert await store.discard_affiliation("sess-1") == run_id
        assert (await store.get(run_id)).kind == "implicit"
        assert await store.list_session_ids(run_id) == ["sess-2"]

    async def test_discard_affiliation_keeps_explicit_run(self, tmp_path):
        """显式 run 是调用方的记录：成员回滚不连坐删 run。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.attach_session(rec.run_id, "sess-1")
        assert await store.discard_affiliation("sess-1") == rec.run_id
        assert (await store.get(rec.run_id)).run_id == rec.run_id
        assert await store.list_session_ids(rec.run_id) == []

    async def test_activation_failure_rolls_back_implicit_mint(self, orch, tmp_path, monkeypatch):
        """激活失败回滚：咽喉铸的隐式归属随名册一起清，账面与缓存零残留。"""
        import sqlite3

        seen: dict[str, str] = {}

        async def boom(session, *, prepare=None):
            seen["sid"] = session.id
            raise RuntimeError("activate failed")

        monkeypatch.setattr(orch, "_activate_session", boom)
        with pytest.raises(RuntimeError, match="activate failed"):
            await orch.start_session_with_config(SessionConfig(name="n", cwd=str(tmp_path)))

        sid = seen["sid"]
        assert orch.pool.get(sid) is None  # 名册已回滚
        assert orch.affiliation.resolve(sid) is None  # 缓存已驱逐
        assert _runs_row_count(tmp_path) == 0  # 无孤儿 run 行
        with sqlite3.connect(tmp_path / ".workflow-controller" / "events.db") as conn:
            left = conn.execute(
                "SELECT COUNT(*) FROM run_sessions WHERE session_id = ?",
                (sid,),
            ).fetchone()[0]
            roster_left = conn.execute(
                "SELECT COUNT(*) FROM sessions WHERE id = ?",
                (sid,),
            ).fetchone()[0]
        assert left == 0  # 无孤儿归属行
        assert roster_left == 0  # 名册行随归属同事务回滚


class TestAnchorUnification:
    @staticmethod
    def test_run_store_and_data_root_anchor_default_project_dir(tmp_path):
        """run 账面与 run 数据根锚 _default_project_dir——与 event_router 默认库
        同锚，不随 config.project_dir 分叉。
        """
        from workflow_controller.config.settings import AppConfig, HookServerConfig, ReviewerConfig
        from workflow_controller.core.orchestrator import Orchestrator

        cfg_proj = tmp_path / "cfg-project"
        default_proj = tmp_path / "default-project"
        cfg = AppConfig(
            project_dir=str(cfg_proj),
            hook_server=HookServerConfig(enabled=False),
            reviewer=ReviewerConfig(enabled=False),
        )
        orch = Orchestrator(cfg, default_project_dir=default_proj)
        assert (
            orch.run_store.db_path
            == (default_proj / ".workflow-controller" / "events.db").resolve()
        )
        assert orch.default_project_dir == default_proj.resolve()

    @staticmethod
    def test_run_store_path_resolved_under_symlink(tmp_path):
        """与 event_router 默认库同纪律（resolve）：symlink 锚下不产生第二真相。"""
        from workflow_controller.config.settings import AppConfig, HookServerConfig, ReviewerConfig
        from workflow_controller.core.orchestrator import Orchestrator

        real = tmp_path / "real-project"
        real.mkdir()
        link = tmp_path / "link-project"
        link.symlink_to(real)
        cfg = AppConfig(
            project_dir=str(tmp_path / "cfg"),
            hook_server=HookServerConfig(enabled=False),
            reviewer=ReviewerConfig(enabled=False),
        )
        orch = Orchestrator(cfg, default_project_dir=link)
        assert "..link-project.." not in str(orch.run_store.db_path)
        assert orch.run_store.db_path == (real / ".workflow-controller" / "events.db").resolve()
        # 与 router 的默认库解析同一路径字符串
        router_db = (
            getattr(orch.event_router, "_default_project_dir")
            / ".workflow-controller"
            / "events.db"
        ).resolve()
        assert orch.run_store.db_path == router_db

    @staticmethod
    def test_anchor_pinned_at_boot_under_relative_dir(tmp_path, monkeypatch):
        """相对 default_project_dir 在 boot resolve 定格，运行期 cwd 变化不重锚。"""
        from pathlib import Path

        from workflow_controller.config.settings import AppConfig, HookServerConfig, ReviewerConfig
        from workflow_controller.core.orchestrator import Orchestrator

        proj = tmp_path / "proj"
        proj.mkdir()
        other = tmp_path / "other"
        other.mkdir()
        monkeypatch.chdir(proj)
        cfg = AppConfig(
            project_dir=str(tmp_path / "cfg"),
            hook_server=HookServerConfig(enabled=False),
            reviewer=ReviewerConfig(enabled=False),
        )
        orch = Orchestrator(cfg, default_project_dir=Path("."))
        monkeypatch.chdir(other)
        assert orch.run_store.db_path == (proj / ".workflow-controller" / "events.db").resolve()


class TestEventsDbCoexistence:
    async def test_run_store_first_then_event_store(self, tmp_path):
        """两族表同库共存（并库后半边）：RunStore 先建 runs 族，EventStore 后建
        events/sessions 族——双方幂等 DDL 互不干扰。
        """
        from workflow_controller.store.event_store import EventStore

        db = tmp_path / ".workflow-controller" / "events.db"
        store = RunStore(db)
        rec = await store.create("p", "dag-yaml@1")
        EventStore(db)
        import sqlite3

        with sqlite3.connect(db) as conn:
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert {"runs", "run_sessions", "events", "run_events", "sessions"} <= tables
            # user_version 唯一写者 = RunStore：EventStore 初始化后不得改档
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert (await store.get(rec.run_id)).run_id == rec.run_id

    async def test_event_store_first_then_run_store(self, tmp_path):
        from workflow_controller.store.event_store import EventStore

        db = tmp_path / ".workflow-controller" / "events.db"
        EventStore(db)
        store = RunStore(db)
        rec = await store.create("p", "dag-yaml@1")
        import sqlite3

        with sqlite3.connect(db) as conn:
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert {"runs", "run_sessions", "events", "run_events", "sessions"} <= tables
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0].lower()
            assert mode == "wal"
        assert (await store.get(rec.run_id)).status == "running"

    async def test_fk_rejects_affiliation_without_run(self, tmp_path):
        """FK 生效断言：run_sessions 直插不存在的 run 被拒（建而未启=没有）。"""
        import sqlite3

        store = RunStore(tmp_path / "events.db")
        with getattr(store, "_connect")() as conn, pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO run_sessions (run_id, session_id) VALUES ('ghost-run', 'sess-x')"
            )

    async def test_wal_restored_when_schema_already_complete(self, tmp_path):
        """稳态 boot 也核对 WAL：上次启动若在 schema 提交与设 WAL 之间崩溃
        （库留 DELETE 模式），本次启动补救——不留永久 DELETE 态。
        """
        import sqlite3

        db = tmp_path / "events.db"
        store = RunStore(db)
        await store.list_runs()  # 首次访问初始化（构造零落盘）
        store.close()  # 切 journal_mode 需文件级独占
        conn = sqlite3.connect(db)
        try:
            assert conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0].lower() == "delete"
        finally:
            conn.close()
        store = RunStore(db)
        await store.list_runs()  # schema 已完整——WAL 补救不依赖 schema 不完整
        store.close()
        conn = sqlite3.connect(db)
        try:
            assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        finally:
            conn.close()


_PRE_V1_RUNS_DDL = """
    CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        program_ref TEXT NOT NULL,
        format TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'running',
        labels TEXT NOT NULL DEFAULT '{}',
        params TEXT NOT NULL DEFAULT '{}',
        error TEXT,
        created_at TEXT NOT NULL,
        finished_at TEXT
    )
"""

_PRE_V1_SESSIONS_DDL = """
    CREATE TABLE IF NOT EXISTS run_sessions (
        run_id TEXT NOT NULL REFERENCES runs(run_id),
        session_id TEXT NOT NULL,
        attached_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (run_id, session_id)
    )
"""


def _make_pre_v1_db(path) -> None:
    import sqlite3

    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(_PRE_V1_RUNS_DDL)
        conn.execute(_PRE_V1_SESSIONS_DDL)


def _make_v1_db(path) -> None:
    """v1 形状库：runs 含预算两列、缺 v2 两列；events 为 TEXT 主键旧形状。"""
    import sqlite3

    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute("""
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                program_ref TEXT NOT NULL,
                format TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                kind TEXT NOT NULL DEFAULT 'workflow',
                work_dir TEXT,
                labels TEXT NOT NULL DEFAULT '{}',
                params TEXT NOT NULL DEFAULT '{}',
                max_budget_usd REAL,
                spent_usd REAL,
                reopened_at TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                finished_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE run_sessions (
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                session_id TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'executor',
                context TEXT,
                native_session_id TEXT,
                paused_by TEXT,
                -- v1 存量库原样照搬（约束名省略形态与实现一致）
                attached_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (run_id, session_id),
                UNIQUE (session_id)
            )
        """)
        conn.execute("""
            CREATE TABLE events (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                iteration INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                data TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("PRAGMA user_version = 1")


class TestSchemaV2Migration:
    """v2 迁移：events 重建 + runs 扩删列 + run_events，单事务盖戳、重放幂等。"""

    async def test_v1_db_migrated_in_one_step(self, tmp_path):
        import sqlite3

        db = tmp_path / "events.db"
        _make_v1_db(db)
        store = RunStore(db)
        rec = await store.create("p", "dag-yaml@1", max_parallel=3)
        assert (await store.get(rec.run_id)).max_parallel == 3

        with sqlite3.connect(db) as conn:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
            runs_cols = {r[1] for r in conn.execute("PRAGMA table_info(runs)")}
            assert {"pause_reason", "max_parallel"} <= runs_cols
            assert "max_budget_usd" not in runs_cols and "spent_usd" not in runs_cols
            events_cols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
            assert "uuid" in events_cols and "run_id" in events_cols
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert "run_events" in tables

    async def test_migration_idempotent_on_reopen(self, tmp_path):
        import sqlite3

        db = tmp_path / "events.db"
        store = RunStore(db)
        rec = await store.create("p", "dag-yaml@1")
        stamp = None
        with sqlite3.connect(db) as conn:
            stamp = conn.execute("PRAGMA user_version").fetchone()[0]
        reopened = RunStore(db)  # 已盖戳不重跑
        assert (await reopened.get(rec.run_id)).run_id == rec.run_id
        with sqlite3.connect(db) as conn:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == stamp

    async def test_v1_events_table_dropped_not_migrated(self, tmp_path):
        """旧形状 events 表整体 DROP：零存量无数据搬移义务，重建后新序从 1 起。"""
        import sqlite3

        db = tmp_path / "events.db"
        _make_v1_db(db)
        with sqlite3.connect(db) as conn:
            conn.execute(
                "INSERT INTO events (id, session_id, iteration, timestamp,"
                " event_type, data) VALUES ('old-uuid', 's1', 0,"
                " '2026-01-01T00:00:00', 'notification', '{}')"
            )
        await RunStore(db).list_runs()  # 首次访问触发迁移（构造零落盘）
        with sqlite3.connect(db) as conn:
            left = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            pk_type = next(r[2] for r in conn.execute("PRAGMA table_info(events)") if r[1] == "id")
        assert left == 0
        assert pk_type.upper() == "INTEGER"

    async def test_pre_v1_debris_fails_fast(self, tmp_path):
        """阶梯之外的施工残骸 fail-fast：不原地补列，删库重建即可。"""
        db = tmp_path / "events.db"
        _make_pre_v1_db(db)
        store = RunStore(db)
        with pytest.raises(RuntimeError, match="删库重建"):
            await store.list_runs()  # fail-fast 在首次访问（构造零落盘）


class TestSchemaV3DriverState:
    """v3：驱动状态两表（最新 + append-only 历史），迁移单事务盖戳、重放幂等。"""

    @staticmethod
    def _downgrade_to_v2(db) -> None:
        import sqlite3

        with sqlite3.connect(db) as conn:
            conn.execute("DROP TABLE run_driver_state")
            conn.execute("DROP TABLE run_driver_snapshots")
            conn.execute("PRAGMA user_version = 2")

    async def test_v2_db_migrated_in_one_step(self, tmp_path):
        import sqlite3

        db = tmp_path / "events.db"
        await RunStore(db).list_runs()  # 首次访问初始化到当前档（构造零落盘）
        self._downgrade_to_v2(db)
        store = RunStore(db)
        rec = await store.create("p", "dag-yaml@1")
        await store.save_driver_state(rec.run_id, {"schema_version": 1})
        with sqlite3.connect(db) as conn:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        assert {"run_driver_state", "run_driver_snapshots"} <= tables

    async def test_migration_idempotent_on_reopen(self, tmp_path):
        import sqlite3

        db = tmp_path / "events.db"
        await RunStore(db).list_runs()  # 首次访问初始化（构造零落盘）
        with sqlite3.connect(db) as conn:
            stamp = conn.execute("PRAGMA user_version").fetchone()[0]
        reopened = RunStore(db)
        assert (await reopened.load_driver_state("ghost")) is None
        with sqlite3.connect(db) as conn:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == stamp

    async def test_driver_state_latest_overwrite(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.save_driver_state(rec.run_id, {"state": 1})
        await store.save_driver_state(rec.run_id, {"state": 2})
        assert (await store.load_driver_state(rec.run_id)) == {"state": 2}

    async def test_driver_projection_roundtrip_and_invalidation(self, tmp_path):
        """终态投影：收尾后落盘可读；任何快照写将其清空（重启续跑失效）。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        assert (await store.load_driver_projection(rec.run_id)) is None
        await store.save_driver_state(rec.run_id, {"state": 1})
        await store.save_driver_projection(rec.run_id, {"schema_version": 2, "nodes": []})
        assert (await store.load_driver_projection(rec.run_id)) == {
            "schema_version": 2,
            "nodes": [],
        }
        assert (await store.load_driver_state(rec.run_id)) == {"state": 1}
        await store.save_driver_state(rec.run_id, {"state": 2})
        assert (await store.load_driver_projection(rec.run_id)) is None

    async def test_driver_projection_without_state_row_noop(self, tmp_path):
        """快照行不存在时投影写 no-op（收尾前必有最终快照）。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.save_driver_projection(rec.run_id, {"schema_version": 2})
        assert (await store.load_driver_projection(rec.run_id)) is None

    async def test_migration_v3_to_v4_adds_projection_column(self, tmp_path):
        """v3 形状库（run_driver_state 无 projection 列）升级：补列、数据保留、重放幂等。"""
        import sqlite3

        db = tmp_path / "events.db"
        seed = RunStore(db)
        rec = await seed.create("p", "dag-yaml@1")
        await seed.save_driver_state(rec.run_id, {"state": 1})
        with sqlite3.connect(db) as conn:
            conn.execute("PRAGMA user_version = 3")
            conn.execute("ALTER TABLE run_driver_state DROP COLUMN projection")

        store = RunStore(db)  # 首次访问升级：v4 补列 + 盖戳
        assert (await store.load_driver_state(rec.run_id)) == {"state": 1}
        await store.save_driver_projection(rec.run_id, {"schema_version": 2})
        assert (await store.load_driver_projection(rec.run_id)) == {"schema_version": 2}
        with sqlite3.connect(db) as conn:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION

        reopened = RunStore(db)  # 重放幂等：档位与数据不漂
        assert (await reopened.load_driver_projection(rec.run_id)) == {"schema_version": 2}
        with sqlite3.connect(db) as conn:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION

    async def test_snapshot_history_hash_dedup(self, tmp_path):
        """同 blob 哈希去重不重复入行；变化后按 seq 递增追加。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        first = await store.append_driver_snapshot(rec.run_id, {"state": 1})
        assert first is not None
        assert await store.append_driver_snapshot(rec.run_id, {"state": 1}) is None
        await store.append_driver_snapshot(rec.run_id, {"state": 2})
        history = await store.list_driver_snapshots(rec.run_id)
        assert [h["seq"] for h in history] == [1, 2]
        assert [h["blob"] for h in history] == [{"state": 1}, {"state": 2}]

    async def test_finish_and_reopen_clear_pause_reason(self, tmp_path):
        """终态清档：finish/reopen 后档位必空。"""
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.update_status(rec.run_id, "paused")
        import sqlite3

        with sqlite3.connect(store.db_path) as conn:
            conn.execute("UPDATE runs SET pause_reason = 'user' WHERE run_id = ?", (rec.run_id,))
        done = await store.finish(rec.run_id, "stopped")
        assert done.record.pause_reason is None
        await store.reopen(rec.run_id)
        assert (await store.get(rec.run_id)).pause_reason is None

    async def test_list_run_sessions_orders_by_attached_at(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        rec = await store.create("p", "dag-yaml@1")
        await store.attach_session(rec.run_id, "s1", role="executor", context="a")
        await store.attach_session(rec.run_id, "s2", role="verifier", context="b")
        sessions = await store.list_run_sessions(rec.run_id)
        assert [m.session_id for m in sessions] == ["s1", "s2"]
        assert sessions[1].role == "verifier" and sessions[1].context == "b"


@pytest.fixture
def orch(tmp_path):
    return _boot_orch(tmp_path)


def _runs_row_count(tmp_path) -> int:
    import sqlite3

    db = tmp_path / ".workflow-controller" / "events.db"
    if not db.exists():  # 从没建过库 = 账面为空
        return 0
    with sqlite3.connect(db) as conn:
        has = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runs'"
        ).fetchone()
        if has is None:
            return 0
        return conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]


class TestAffiliationRecovery:
    """boot 归属恢复：双源重建（run_sessions 为准）+ 崩溃窗口补铸。"""

    async def test_rebuild_prefers_run_sessions_over_config_json(self, orch, tmp_path, monkeypatch):
        """双源冲突：名册 config_json 说是 A，账面 run_sessions 说是 B → 以 B 为准。"""
        _fake_activate(orch, monkeypatch)
        session = await orch.start_session_with_config(SessionConfig(name="n", cwd=str(tmp_path)))
        true_run = session.run_id

        rebooted = _boot_orch(tmp_path)
        rebooted.pool.load_sessions()
        ghost = rebooted.pool.get(session.id)
        ghost.config.run_id = "stray-run"  # 名册旁证被污染
        await rebooted.affiliation.recover_at_boot(rebooted.pool)

        assert rebooted.affiliation.resolve(session.id).run_id == true_run
        assert ghost.config.run_id == true_run  # config 被拉回权威值

    async def test_backfill_mints_for_roster_without_affiliation(self, orch, tmp_path, monkeypatch):
        """崩溃窗口：名册行在、归属缺、无 run_id → 补铸隐式 run，恒等式恢复。"""
        import sqlite3

        _fake_activate(orch, monkeypatch)
        session = await orch.start_session_with_config(SessionConfig(name="n", cwd=str(tmp_path)))
        db = tmp_path / ".workflow-controller" / "events.db"
        with sqlite3.connect(db) as conn:  # 模拟「pool.create 已落名册、铸造未及落账」
            conn.execute("DELETE FROM run_sessions")
            conn.execute("DELETE FROM runs")
            conn.execute(
                "UPDATE sessions SET config_json = json_remove(config_json, '$.run_id')"
                " WHERE id = ?",
                (session.id,),
            )

        rebooted = _boot_orch(tmp_path)
        rebooted.pool.load_sessions()
        await rebooted.affiliation.recover_at_boot(rebooted.pool)

        ghost = rebooted.pool.get(session.id)
        assert ghost.config.run_id is not None
        rec = await rebooted.run_store.get(ghost.config.run_id)
        assert rec.kind == "implicit"
        assert rebooted.affiliation.resolve(session.id).run_id == rec.run_id

    async def test_backfill_attaches_from_config_run_id(self, orch, tmp_path, monkeypatch):
        """崩溃窗口：config.run_id 在且 run 存在 → 补铸走 attach 段。"""
        import sqlite3

        _fake_activate(orch, monkeypatch)
        run = await orch.run_store.create("p", "dag-yaml@1")
        session = await orch.start_session_with_config(
            SessionConfig(name="n", cwd=str(tmp_path), run_id=run.run_id)
        )
        db = tmp_path / ".workflow-controller" / "events.db"
        with sqlite3.connect(db) as conn:  # 模拟「激活临界区前崩溃：归属未及落账」
            conn.execute("DELETE FROM run_sessions WHERE session_id = ?", (session.id,))
            conn.execute(
                "DELETE FROM runs WHERE kind = 'implicit'"
            )  # 清掉显式 run 之外的账（本例只有它）

        rebooted = _boot_orch(tmp_path)
        rebooted.pool.load_sessions()
        await rebooted.affiliation.recover_at_boot(rebooted.pool)

        assert rebooted.affiliation.resolve(session.id).run_id == run.run_id
        assert await rebooted.run_store.list_session_ids(run.run_id) == [session.id]

    async def test_recovery_initializes_ledger_for_roster_orphans(
        self, orch, tmp_path, monkeypatch
    ):
        """账面整体缺失（表被清）+ 名册行在 → 恢复才初始化账面并补铸隐式。"""
        import sqlite3

        _fake_activate(orch, monkeypatch)
        session = await orch.start_session_with_config(SessionConfig(name="n", cwd=str(tmp_path)))
        db = tmp_path / ".workflow-controller" / "events.db"
        with sqlite3.connect(db) as conn:  # 名册保留，账面两表清空
            conn.execute("DROP TABLE run_sessions")
            conn.execute("DROP TABLE runs")

        rebooted = _boot_orch(tmp_path)
        rebooted.pool.load_sessions()
        await rebooted.affiliation.recover_at_boot(rebooted.pool)

        ghost = rebooted.pool.get(session.id)
        assert ghost.config.run_id is not None
        rec = await rebooted.run_store.get(ghost.config.run_id)
        assert rec.kind == "implicit"
        assert _runs_row_count(tmp_path) == 1  # 账面按需重建

    async def test_backfill_skips_cross_run_conflict(self, orch, tmp_path, monkeypatch):
        """防御分支：attach 报跨 run 冲突 → 跳过不强铸（不撞 UNIQUE），账面权威。"""
        import sqlite3

        _fake_activate(orch, monkeypatch)
        run = await orch.run_store.create("p", "dag-yaml@1")
        session = await orch.start_session_with_config(
            SessionConfig(name="n", cwd=str(tmp_path), run_id=run.run_id)
        )
        other = await orch.run_store.create("p2", "dag-yaml@1")
        db = tmp_path / ".workflow-controller" / "events.db"
        with sqlite3.connect(db) as conn:  # 账面直接改挂 other，制造跨 run 冲突
            conn.execute(
                "UPDATE run_sessions SET run_id = ? WHERE session_id = ?",
                (other.run_id, session.id),
            )
        await orch.affiliation.rebuild()

        await getattr(orch.affiliation, "_backfill")(
            orch.pool, session
        )  # boot 时序外直接打防御分支

        assert orch.affiliation.resolve(session.id).run_id == other.run_id
        with sqlite3.connect(db) as conn:
            rows = conn.execute(
                "SELECT COUNT(*) FROM run_sessions WHERE session_id = ?",
                (session.id,),
            ).fetchone()[0]
        assert rows == 1  # 没有强铸第二行

    async def test_boot_cleans_forward_orphan_implicit_affiliation(
        self, orch, tmp_path, monkeypatch
    ):
        """正向孤儿：归属行在、名册行缺且属隐式 run → boot 连行带 run 幂等回收。"""
        import sqlite3

        _fake_activate(orch, monkeypatch)
        orphan = await orch.start_session_with_config(SessionConfig(name="a", cwd=str(tmp_path)))
        live = await orch.start_session_with_config(SessionConfig(name="b", cwd=str(tmp_path)))
        db = tmp_path / ".workflow-controller" / "events.db"
        with sqlite3.connect(db) as conn:  # 模拟「mint 已提交、名册写未及」
            conn.execute("DELETE FROM sessions WHERE id = ?", (orphan.id,))

        rebooted = _boot_orch(tmp_path)
        rebooted.pool.load_sessions()
        await rebooted.affiliation.recover_at_boot(rebooted.pool)

        with pytest.raises(RunNotFoundError):
            await rebooted.run_store.get(orphan.run_id)
        assert rebooted.affiliation.resolve(orphan.id) is None
        assert _runs_row_count(tmp_path) == 1  # 只剩 live 的隐式 run
        assert rebooted.affiliation.resolve(live.id).run_id == live.run_id

    async def test_boot_keeps_forward_orphan_on_explicit_run(self, orch, tmp_path, monkeypatch):
        """显式 run 的孤儿成员行不连坐删——run 是调用方的记录。"""
        _fake_activate(orch, monkeypatch)
        live = await orch.start_session_with_config(SessionConfig(name="b", cwd=str(tmp_path)))
        run = await orch.run_store.create("p", "dag-yaml@1")
        await orch.run_store.attach_session(run.run_id, "ghost-sess")

        rebooted = _boot_orch(tmp_path)
        rebooted.pool.load_sessions()
        await rebooted.affiliation.recover_at_boot(rebooted.pool)

        assert (await rebooted.run_store.get(run.run_id)).run_id == run.run_id
        assert await rebooted.run_store.list_session_ids(run.run_id) == ["ghost-sess"]
        assert rebooted.affiliation.resolve(live.id).run_id == live.run_id

    async def test_recovery_does_not_create_ledger(self, tmp_path):
        """空名册环境 boot 全路径（含 load_sessions 建事件库）不落 runs 表账面。"""
        orch = _boot_orch(tmp_path)
        orch.pool.load_sessions()  # 真实时序：先建事件库（events/sessions 表）
        await orch.affiliation.recover_at_boot(orch.pool)
        assert _runs_row_count(tmp_path) == 0

    async def test_run_plane_wiring_creates_no_ledger(self, tmp_path):
        """boot 恢复序同款接线（归属恢复 + 构造 RunCoordinator + run 恢复）在
        空列表环境不建记录库——RunStore 构造零落盘，记录随首次访问初始化。
        """
        import sqlite3

        from workflow_controller.core.run_coordinator import RunCoordinator

        orch = _boot_orch(tmp_path)
        orch.pool.load_sessions()
        await orch.affiliation.recover_at_boot(orch.pool)
        coordinator = RunCoordinator(
            orchestrator=orch,
            run_store=orch.run_store,
            bus=orch.bus,
            driver_factories=orch.plugin_registry.run_driver_factories(),
            affiliation=orch.affiliation,
        )
        await coordinator.recover_at_boot()
        db = tmp_path / ".workflow-controller" / "events.db"
        if db.exists():  # 事件面可建库，但 runs 族表不得出现
            with sqlite3.connect(db) as conn:
                tables = {
                    r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            assert "runs" not in tables

    async def test_attach_audit_emitted_only_on_fresh_insert(self, orch, tmp_path, monkeypatch):
        """run_attached 审计：新落行发一次，幂等 no-op 不重复发。"""
        _fake_activate(orch, monkeypatch)
        stored: list[str] = []

        async def fake_store(event):
            if event.data.get("action") == "run_attached":
                stored.append(event.data["run_id"])

        monkeypatch.setattr(orch.event_router, "store", fake_store)
        run = await orch.run_store.create("p", "dag-yaml@1")
        session = await orch.start_session_with_config(
            SessionConfig(name="n", cwd=str(tmp_path), run_id=run.run_id)
        )
        assert stored == [run.run_id]
        await orch.affiliation.attach(session.id, run.run_id, role=session.config.role)  # 幂等
        assert stored == [run.run_id]


class TestDeleteAffiliationReclaim:
    """用户删除路径的归属回收：与 boot 正向孤儿回收同一判定。"""

    async def test_delete_reclaims_implicit_affiliation(self, orch, tmp_path, monkeypatch):
        """删隐式归属 session → 归属行 + 隐式 run 即时回收，不等 boot。"""
        _fake_activate(orch, monkeypatch)
        session = await orch.start_session_with_config(SessionConfig(name="n", cwd=str(tmp_path)))
        run_id = session.run_id

        await SessionAPI(orch, None).delete(session.id)

        with pytest.raises(RunNotFoundError):
            await orch.run_store.get(run_id)
        assert orch.affiliation.resolve(session.id) is None
        assert _runs_row_count(tmp_path) == 0

    async def test_delete_keeps_explicit_session_row(self, orch, tmp_path, monkeypatch):
        """删显式 run 成员 → 成员行与 run 都保留（调用方的记录，不连坐删）。"""
        _fake_activate(orch, monkeypatch)
        run = await orch.run_store.create("p", "dag-yaml@1")
        session = await orch.start_session_with_config(
            SessionConfig(name="n", cwd=str(tmp_path), run_id=run.run_id)
        )

        await SessionAPI(orch, None).delete(session.id)

        assert (await orch.run_store.get(run.run_id)).run_id == run.run_id
        assert await orch.run_store.list_session_ids(run.run_id) == [session.id]

    async def test_delete_removes_roster_row_too(self, orch, tmp_path, monkeypatch):
        """删除路径的归属与名册同事务：隐式与显式成员的名册行都清掉。"""
        import sqlite3

        _fake_activate(orch, monkeypatch)
        run = await orch.run_store.create("p", "dag-yaml@1")
        implicit = await orch.start_session_with_config(SessionConfig(name="i", cwd=str(tmp_path)))
        explicit = await orch.start_session_with_config(
            SessionConfig(name="e", cwd=str(tmp_path), run_id=run.run_id)
        )

        await SessionAPI(orch, None).delete(implicit.id)
        await SessionAPI(orch, None).delete(explicit.id)

        with sqlite3.connect(tmp_path / ".workflow-controller" / "events.db") as conn:
            left = conn.execute(
                "SELECT COUNT(*) FROM sessions WHERE id IN (?, ?)",
                (implicit.id, explicit.id),
            ).fetchone()[0]
        assert left == 0

    async def test_reclaim_without_ledger_creates_nothing(self, orch, tmp_path):
        """账面未铸过（懒建未触）→ 回收 no-op，不落 runs 表。"""
        await orch.reclaim_deleted_session_affiliation("ghost")
        assert _runs_row_count(tmp_path) == 0


class TestTracebackForkNulling:
    """traceback 类 fork 置 run_id=None → 铸新隐式 run（不进源 run 账）。"""

    async def test_fork_with_null_run_id_mints_new_implicit(self, orch, tmp_path, monkeypatch):
        _fake_activate(orch, monkeypatch)
        origin = await orch.start_session_with_config(
            SessionConfig(name="origin", cwd=str(tmp_path))
        )

        fork_cfg = origin.config.model_copy(deep=True)  # traceback 深拷贝基底
        fork_cfg.name = "replay-1"
        fork_cfg.run_id = None  # 置空规则：用户级实验 fork 不入源 run
        fork = await orch.start_session_with_config(fork_cfg)

        assert fork.run_id is not None
        assert fork.run_id != origin.run_id  # 新隐式 run，与源 run 分账
        assert (await orch.run_store.get(fork.run_id)).kind == "implicit"
        assert await orch.run_store.list_session_ids(origin.run_id) == [origin.id]


class TestImplicitVisibility:
    """list/boot 过滤 implicit：消费面默认不见隐式 run；隐式不受组控制。"""

    async def test_list_runs_filters_implicit_by_default(self, orch, tmp_path, monkeypatch):
        _fake_activate(orch, monkeypatch)
        explicit = await orch.run_store.create("p", "dag-yaml@1")
        session = await orch.start_session_with_config(SessionConfig(name="n", cwd=str(tmp_path)))
        assert session.run_id  # 咽喉已铸隐式 run

        visible = await orch.run_store.list_runs()
        assert [r.run_id for r in visible] == [explicit.run_id]
        everything = await orch.run_store.list_runs(include_implicit=True)
        assert {r.run_id for r in everything} == {explicit.run_id, session.run_id}
        assert all(r.kind == "implicit" for r in everything if r.run_id == session.run_id)

    async def test_list_runs_status_filter(self, tmp_path):
        store = RunStore(tmp_path / "events.db")
        running = await store.create("p", "dag-yaml@1")
        done = await store.create("q", "dag-yaml@1")
        await store.finish(done.run_id, "completed")
        got = await store.list_runs(status="completed")
        assert [r.run_id for r in got] == [done.run_id]
        assert running.run_id not in {r.run_id for r in got}

    async def test_list_runs_aggregates_member_status_in_single_query(self, tmp_path):
        """列表含隐式 run 时成员计数随行返回，且整次调用只执行一条 SQL。"""
        import sqlite3

        store = RunStore(tmp_path / "events.db")
        bare = await store.create("p", "dag-yaml@1")  # 无成员 run：0/0
        busy = await store.create("q", "dag-yaml@1")
        with sqlite3.connect(tmp_path / "events.db") as conn:
            conn.execute(
                "INSERT INTO sessions (id, name, state, config_json, created_at,"
                " last_activity) VALUES ('s-run', 'n1', 'running', '{}',"
                " '2026-10-16T10:00:00', '2026-10-16T10:00:00')"
            )
            conn.execute(
                "INSERT INTO sessions (id, name, state, config_json, created_at,"
                " last_activity) VALUES ('s-idle', 'n2', 'idle', '{}',"
                " '2026-10-16T10:00:00', '2026-10-16T10:00:00')"
            )
        await store.attach_session(busy.run_id, "s-run")
        await store.attach_session(busy.run_id, "s-idle")
        implicit = await store.mint_implicit(
            session_id="s-imp",
            role="interactive",
            label="会话",
            roster={
                "id": "s-imp",
                "name": "会话",
                "role": "worker",
                "state": "idle",
                "loop_status": "yes",
                "config_json": "{}",
                "created_at": "2026-10-16T10:00:00",
                "last_activity": "2026-10-16T10:00:00",
            },
        )

        statements: list[str] = []
        assert getattr(store, "_conn") is not None
        getattr(store, "_conn").set_trace_callback(statements.append)
        rows = {r.run_id: r for r in await store.list_runs(include_implicit=True)}
        getattr(store, "_conn").set_trace_callback(None)

        assert len(statements) == 1  # 无 per-run 循环
        assert "GROUP BY" in statements[0]
        assert (rows[bare.run_id].member_count, rows[bare.run_id].running_count) == (0, 0)
        assert (rows[busy.run_id].member_count, rows[busy.run_id].running_count) == (2, 1)
        assert (rows[implicit].member_count, rows[implicit].running_count) == (1, 0)


class TestRunLifecycle:
    async def test_finish_via_store_guard(self, orch, tmp_path):
        run = await orch.run_store.create("p", "dag-yaml@1")
        done = await orch.run_store.finish(run.run_id, "completed")
        assert done.record.status == "completed"
        with pytest.raises(ValueError, match="不许改判"):
            await orch.run_store.finish(run.run_id, "failed")

    async def test_explicit_attach_idempotent_after_activation(self, orch, tmp_path, monkeypatch):
        _fake_activate(orch, monkeypatch)
        run = await orch.run_store.create("p", "dag-yaml@1")
        session = await orch.start_session_with_config(
            SessionConfig(name="n", cwd=str(tmp_path), run_id=run.run_id)
        )
        await orch.affiliation.attach(
            session.id, run.run_id, role=session.config.role
        )  # 已属本 run：幂等
        assert await orch.run_store.list_session_ids(run.run_id) == [session.id]
        assert session.run_id == run.run_id  # 归属标记同步，后续 spawn 继承


class TestSessionAttribution:
    async def test_unknown_run_id_rejected_before_create(self, orch, tmp_path, monkeypatch):
        _fake_activate(orch, monkeypatch)
        cfg = SessionConfig(name="n", cwd=str(tmp_path), run_id="ghost")
        with pytest.raises(ValueError, match="ghost"):
            await orch.start_session_with_config(cfg)
        assert orch.pool.list_sessions() == []  # 没造出半截 session

    async def test_session_created_with_run_id_is_attached(self, orch, tmp_path, monkeypatch):
        _fake_activate(orch, monkeypatch)
        run = await orch.run_store.create("p", "dag-yaml@1")
        session = await orch.start_session_with_config(
            SessionConfig(name="n", cwd=str(tmp_path), run_id=run.run_id)
        )
        assert session.run_id == run.run_id
        assert await orch.run_store.list_session_ids(run.run_id) == [session.id]
        # 激活失败半截不进账面：attach 只发生在激活成功路径（钩子内）
        assert orch.affiliation.resolve(session.id).run_id == run.run_id

    async def test_unattributed_session_mints_implicit_run(self, orch, tmp_path, monkeypatch):
        """无显式归属的 session 在创建咽喉铸隐式 run（mint 必发）。"""
        _fake_activate(orch, monkeypatch)
        session = await orch.start_session_with_config(SessionConfig(name="n", cwd=str(tmp_path)))
        assert session.run_id is not None
        rec = await orch.run_store.get(session.run_id)
        assert rec.kind == "implicit" and rec.labels == {"name": "n"}
        assert await orch.run_store.list_session_ids(rec.run_id) == [session.id]
        assert orch.affiliation.resolve(session.id).run_id == rec.run_id


class TestEventRunStamping:
    """成员事件打戳：归属缓存为唯一入口，写库与三消费端字段一致。"""

    async def test_run_session_event_stamped_and_lands_in_daemon_db(
        self, orch, tmp_path, monkeypatch
    ):
        import sqlite3

        from workflow_controller.api.event_api import EventInfo
        from workflow_controller.session.events import Event, EventType

        _fake_activate(orch, monkeypatch)
        session = await orch.start_session_with_config(SessionConfig(name="n", cwd=str(tmp_path)))
        assert session.run_id  # 咽喉已铸隐式归属

        emitted = Event.create(session.id, 1, EventType.NOTIFICATION, {"k": "v"})
        await orch.bus.emit(emitted)

        db = tmp_path / ".workflow-controller" / "events.db"
        with sqlite3.connect(db) as conn:
            rows = conn.execute(
                "SELECT run_id, context FROM events WHERE session_id = ?",
                (session.id,),
            ).fetchall()
        # 隐式成员事件自始有戳：session_started 与后续事件都带 run 维度
        assert rows and all(r[0] == session.run_id for r in rows)
        # 三消费端一致：内存对象 / to_dict / EventInfo 投影
        assert emitted.run_id == session.run_id
        assert emitted.to_dict()["run_id"] == session.run_id
        info = EventInfo.from_event(0, emitted)
        assert info.run_id == session.run_id and info.session_id == session.id


class _QuietDriver:
    """不产出动作的驱动：run 停在 running，供 API 面测试用。"""

    @staticmethod
    def decide(world, notifications):
        return []

    @staticmethod
    def on_control(control) -> None:
        pass

    @staticmethod
    def projection():
        return {"schema_version": 1}

    @staticmethod
    def snapshot():
        return {"schema_version": 1, "pending_timers": []}

    @staticmethod
    def handle_op(op, args):
        return {}


class _QuietFactory:
    format = "quiet@1"

    @staticmethod
    def create(run, program_path, params):
        return _QuietDriver()

    @staticmethod
    def restore(run, snapshot, sessions):
        return _QuietDriver()


def _coordinator(orch):
    from workflow_controller.core.run_coordinator import RunCoordinator

    return RunCoordinator(
        orchestrator=orch,
        run_store=orch.run_store,
        bus=orch.bus,
        driver_factories={"quiet@1": _QuietFactory()},
        affiliation=orch.affiliation,
    )


class TestRunApi:
    async def test_facade_start_get_state_stop(self, orch, tmp_path):
        from workflow_controller.api.run_api import RunAPI

        api = RunAPI(_coordinator(orch))
        program = tmp_path / "prog.yaml"
        program.write_text("format: quiet@1\n", encoding="utf-8")
        run = await api.start("file:prog", program, params={"a": "1"})
        assert run.status == "running" and run.format == "quiet@1"
        assert (await api.get_run(run.run_id)).run_id == run.run_id
        assert await api.sessions(run.run_id) == []
        state = await api.state(run.run_id)
        assert state["status"] == "running"
        assert state["driver"] == {"schema_version": 1}  # 投影经应答通道透传
        assert state["root"].endswith(run.run_id)
        stopped = await api.stop(run.run_id)
        assert stopped.status == "stopped"

    async def test_start_unregistered_format_rejected(self, orch, tmp_path):
        from workflow_controller.api.run_api import RunAPI

        api = RunAPI(_coordinator(orch))
        program = tmp_path / "prog.yaml"
        program.write_text("format: ghost@1\n", encoding="utf-8")
        with pytest.raises(ValueError, match="没有驱动工厂"):
            await api.start("file:prog", program)

    async def test_unknown_run_translated_to_api_error(self, orch):
        from workflow_controller.api.run_api import RunAPI, RunApiNotFoundError

        api = RunAPI(_coordinator(orch))
        with pytest.raises(RunApiNotFoundError):
            await api.get_run("ghost")
        with pytest.raises(RunApiNotFoundError):
            await api.sessions("ghost")
        with pytest.raises(RunApiNotFoundError):
            await api.state("ghost")
        with pytest.raises(RunApiNotFoundError):
            await api.stop("ghost")


class TestSessionControlPredicates:
    """SO 谓词原语：组控只消费谓词，字段取值语义收进 session 平面。"""

    async def test_predicates_across_session_lifecycle(self, orch, tmp_path):
        from workflow_controller.session.session import SessionState

        config = SessionConfig(name="pred", cwd=str(tmp_path))
        session = await orch.pool.create(config)

        session.state, session.loop_status = SessionState.RUNNING, "yes"
        assert orch.is_pausable(session.id) is True
        assert orch.is_resumable(session.id) is False
        assert orch.is_terminal(session.id) is False

        # 用户暂停：IDLE + no + user_pause——可恢复断点，不是终态
        session.state = SessionState.IDLE
        session.loop_status = "no"
        session.stop_reason = "user_pause"
        assert orch.is_pausable(session.id) is False
        assert orch.is_resumable(session.id) is True
        assert orch.is_terminal(session.id) is False

        # 真终态：no + 非 user_pause
        session.stop_reason = "reviewer_complete"
        assert orch.is_terminal(session.id) is True
        assert orch.is_resumable(session.id) is False

    async def test_predicate_on_missing_session_is_terminal(self, orch):
        assert orch.is_terminal("ghost-session") is True
        assert orch.is_pausable("ghost-session") is False
        assert orch.is_resumable("ghost-session") is False
