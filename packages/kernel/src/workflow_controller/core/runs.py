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

"""Run 身份记账——宿主级 run 账面。

runs 族表住在项目库 events.db。RunStore 是 events.db user_version 阶梯的唯一
写者——EventStore 维持幂等 DDL、永不触碰 user_version。

执行主体仍是 session（真 driver 子进程）与裸进程（bash 类节点）；本模块只管
记账：mint run_id、run 名下 session 清单、终态回写。``finish`` 是终态唯一
写口（解释器不直写本表），且终态不改判——同状态幂等（收尾重试）、换状态
拒绝（stop 中崩溃重启不许被改写成 completed）。
"""

from __future__ import annotations

__all__ = [
    "NON_TERMINAL_STATUSES",
    "TERMINAL_STATUSES",
    "FinishResult",
    "RunKind",
    "RunNotFoundError",
    "RunRecord",
    "RunStatus",
    "RunStore",
    "SessionRunAffiliationRecord",
]

import asyncio
import hashlib
import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from workflow_controller.core.checkpoints import (
    IDX_SCOPE_CHECKPOINTS_SCOPE_DDL,
    SCOPE_CHECKPOINTS_DDL,
)
from workflow_controller.core.status_maps import (
    NON_TERMINAL_STATUSES,
    TERMINAL_STATUSES,
    validate_kind_format,
)
from workflow_controller.store.event_store import (
    EVENTS_DDL,
    IDX_EVENTS_RUN_DDL,
    IDX_EVENTS_SESSION_DDL,
    IDX_RUN_EVENTS_DDL,
    RUN_EVENTS_DDL,
    SESSIONS_DDL,
)
from workflow_controller.watchdog.ledger import (
    IDX_WATCHDOG_NODE_CLOCKS_RUN_DDL,
    WATCHDOG_NODE_CLOCKS_DDL,
)

if TYPE_CHECKING:
    from pathlib import Path


class RunStatus(StrEnum):
    """run 状态（值集与裸字符串时代完全一致，双向兼容；未知值读回原样透传）。"""

    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"


class RunKind(StrEnum):
    """run 类别：workflow（程序+驱动）/ container（显式纯容器）/ implicit（归属
    写点自动创建，状态=成员投影，不进 finish/reopen 终态机）。
    """

    WORKFLOW = "workflow"
    CONTAINER = "container"
    IMPLICIT = "implicit"


def _coerce_enum(value: str, enum_cls: type[StrEnum]) -> str:
    """未知值回退不抛：值集外读回原样，枚举仅作词面约束。"""
    try:
        return enum_cls(value)
    except ValueError:
        return value


# events.db user_version 阶梯当前档（唯一写者 = RunStore）：
# v1 = runs 族建表；v2 = events 重建（维度列/持久主键）+ run_events + runs 扩删列；
# v3 = 驱动状态两表（最新快照 + append-only 历史）；
# v4 = run_driver_state 增终态投影列（/state 降级读源）；
# v5 = 作用域检查点账本表（与遗留插件的旧检查点表并存，互不相扰）；
# v6 = watchdog 节点累计时钟账本表（节点超时规则的重启保留面）。
SCHEMA_VERSION = 6

# 档位特征列：判定库 schema 是否齐备（幂等位探针）。
_V1_RUNS_FEATURES = ("kind", "work_dir", "reopened_at")
_V2_RUNS_FEATURES = ("pause_reason", "max_parallel")
_V3_TABLES = ("run_driver_state", "run_driver_snapshots")
_V4_RDS_FEATURES = ("projection",)
_V5_TABLES = ("scope_checkpoints",)
_V6_TABLES = ("watchdog_node_clocks",)
# v2 撤销的预算两列（run 级预算不实现）
_RETIRED_RUNS_COLUMNS = ("max_budget_usd", "spent_usd")
_V1_SESSIONS_ADDITIONS = (
    ("role", "TEXT NOT NULL DEFAULT 'executor'"),
    ("context", "TEXT"),
    ("native_session_id", "TEXT"),
    ("paused_by", "TEXT"),
)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """PRAGMA table_info 的列名集——schema 完整性探针与升级列映射用。"""
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


class RunNotFoundError(LookupError):
    """run_id 不在账面（get / finish / attach 的拒因）。"""


class SessionNotInRunError(LookupError):
    """session 不隶属该 run（成员操作前置守卫的拒因；与 RunNotFoundError 平级）。"""


class MemberResumeDeniedError(RuntimeError):
    """成员级 resume 被准入门拒绝（run 未运行 / 成员非用户暂停断点）。

    与 ValueError 刻意区分：只有准入拒绝映射 409，成员操作路径上的其他
    ValueError 属于 bug，应原样冒成 500 而非误报冲突。
    """


@dataclass
class SessionRunAffiliationRecord:
    """run_sessions 行的内存投影（归属缓存重建的权威源）。"""

    session_id: str
    run_id: str
    role: str = "executor"
    context: str | None = None
    native_session_id: str | None = None
    paused_by: str | None = None  # 组暂停归属：NULL/'user'/'run'


@dataclass
class RunRecord:
    run_id: str
    program_ref: str
    format: str
    status: str = "running"
    kind: str = "workflow"
    work_dir: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    params: dict[str, str] = field(default_factory=dict)
    max_parallel: int | None = None
    pause_reason: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    finished_at: datetime | None = None
    reopened_at: datetime | None = None
    error: str | None = None
    # 列表面聚合列：list_runs 的聚合 SQL 随行填充；单 run 读路径不填，恒为 0
    member_count: int = 0
    running_count: int = 0


@dataclass(frozen=True)
class FinishResult:
    """finish 的返回值：was_applied=True 表示终态由本次调用写入；
    False 表示终态此前已写入（收尾重试、或 stop 与关机两条路径同时收尾），
    本次不改动，record 返回库中已有记录。
    已是不同终态时不返回 False 而是直接 raise（终态不许改判）。
    """

    record: RunRecord
    was_applied: bool


_RUNS_DDL = """
    CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        program_ref TEXT NOT NULL,
        format TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'running',
        kind TEXT NOT NULL DEFAULT 'workflow',
        work_dir TEXT,
        labels TEXT NOT NULL DEFAULT '{}',
        params TEXT NOT NULL DEFAULT '{}',
        max_parallel INTEGER,
        pause_reason TEXT,
        error TEXT,
        created_at TEXT NOT NULL,
        finished_at TEXT,
        reopened_at TEXT
    )
"""

_RUN_SESSIONS_DDL = """
    CREATE TABLE IF NOT EXISTS run_sessions (
        run_id TEXT NOT NULL REFERENCES runs(run_id),
        session_id TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'executor',
        context TEXT,
        native_session_id TEXT,
        paused_by TEXT,
        attached_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (run_id, session_id),
        UNIQUE (session_id)
    )
"""

RUN_DRIVER_STATE_DDL = """
    CREATE TABLE IF NOT EXISTS run_driver_state (
        run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
        blob TEXT NOT NULL,
        projection TEXT,
        updated_at TEXT NOT NULL
    )
"""

RUN_DRIVER_SNAPSHOTS_DDL = """
    CREATE TABLE IF NOT EXISTS run_driver_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
        seq INTEGER NOT NULL,
        blob TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
"""

IDX_RDS_HIST_DDL = "CREATE INDEX IF NOT EXISTS idx_rds_hist ON run_driver_snapshots (run_id, seq)"

# native 路由权威源查询的热路径索引（幂等，建库事务外执行以覆盖已盖戳的库）
IDX_RUN_SESSIONS_NATIVE_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_run_sessions_native ON run_sessions (native_session_id)"
)


# 非终态迁移的档位哨兵：区分「不动档位列」（update_status）与「写 NULL」
# （update_pause_state 清档）——None 本身是合法档位值，不能兼任
_KEEP_PAUSE = object()


def _aware(dt: datetime) -> datetime:
    """历史版本写入 naive 本地时间串，按本地时区补全；全量 aware 写入一个版本后可删除。"""
    return dt if dt.tzinfo is not None else dt.astimezone()


def _rows_to_affiliations(rows: list[Any]) -> list[SessionRunAffiliationRecord]:
    """run_sessions 行 → 归属记录（全表与按 run 两个读口共用）。"""
    return [
        SessionRunAffiliationRecord(
            session_id=r["session_id"],
            run_id=r["run_id"],
            role=r["role"],
            context=r["context"],
            native_session_id=r["native_session_id"],
            paused_by=r["paused_by"],
        )
        for r in rows
    ]


def _to_record(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        run_id=row["run_id"],
        program_ref=row["program_ref"],
        format=row["format"],
        status=_coerce_enum(row["status"], RunStatus),
        kind=_coerce_enum(row["kind"], RunKind),
        work_dir=row["work_dir"],
        labels=json.loads(row["labels"]),
        params=json.loads(row["params"]),
        max_parallel=row["max_parallel"],
        pause_reason=row["pause_reason"],
        created_at=_aware(datetime.fromisoformat(row["created_at"])),
        finished_at=(
            _aware(datetime.fromisoformat(row["finished_at"])) if row["finished_at"] else None
        ),
        reopened_at=(
            _aware(datetime.fromisoformat(row["reopened_at"])) if row["reopened_at"] else None
        ),
        error=row["error"],
    )


def _stamp_roster_run_id(roster: dict[str, Any], run_id: str) -> None:
    """roster 的 config_json 缺 run_id 时补写（铸号前序列化，此处补旁证）。"""
    cfg_raw = roster.get("config_json")
    if not cfg_raw:
        return
    cfg = json.loads(cfg_raw)
    if cfg.get("run_id") is None:
        cfg["run_id"] = run_id
        roster["config_json"] = json.dumps(cfg)


def _delete_run_if_implicit_and_empty(conn: sqlite3.Connection, run_id: str) -> None:
    """隐式 run 成员归零时连删 run 行；显式 run 是调用方的记录，不连坐。"""
    kind = conn.execute("SELECT kind FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    if kind is None or kind["kind"] != RunKind.IMPLICIT:
        return
    left = conn.execute("SELECT 1 FROM run_sessions WHERE run_id = ?", (run_id,)).fetchone()
    if left is None:
        conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))


class RunStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        # 与 EventStore 同纪律：RLock 跨线程同步，异步方法经 asyncio.to_thread
        # 委托同步方法，不在事件循环线程内直接 acquire。
        self._lock = threading.RLock()
        # 单写连接复用：runs 族读写与事务内嵌名册行写全走此连接（均短查询）；
        # 持锁跨线程安全
        self._conn: sqlite3.Connection | None = None
        # 构造零落盘：账面库随首次访问初始化——boot 装配 run 平面（构造即持引用）
        # 不在无 session 环境建库
        self._schema_ready = False

    @staticmethod
    def _schema_complete(conn: sqlite3.Connection) -> bool:
        """幂等位：各档特征列 + 存储层约束 + user_version 档位。特征列探针防
        「缺列库被盖戳」——表在而缺列不算齐，否则 INSERT 会撞缺列；约束探针防
        「列齐而 UNIQUE(session_id)/FK 缺失」的库蒙混过关。
        """
        runs_cols = _table_columns(conn, "runs")
        runs_ok = (
            set(_V1_RUNS_FEATURES) <= runs_cols
            and set(_V2_RUNS_FEATURES) <= runs_cols
            and not (set(_RETIRED_RUNS_COLUMNS) & runs_cols)
        )
        sessions_ok = {n for n, _ in _V1_SESSIONS_ADDITIONS} <= _table_columns(
            conn, "run_sessions"
        ) and RunStore._sessions_constraints_ok(conn)
        # uuid 幂等列是 events v2 形状的特征列；表可缺（账面先建时本档内补）
        events_ok = "uuid" in _table_columns(conn, "events")
        # sessions 表由本档迁移一并铺（mint 与 sessions 表写入同事务的表前提）
        roster_ok = bool(_table_columns(conn, "sessions"))
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        v3_ok = set(_V3_TABLES) <= tables
        v4_ok = set(_V4_RDS_FEATURES) <= _table_columns(conn, "run_driver_state")
        v5_ok = set(_V5_TABLES) <= tables
        v6_ok = set(_V6_TABLES) <= tables
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        return (
            runs_ok
            and sessions_ok
            and events_ok
            and roster_ok
            and v3_ok
            and v4_ok
            and v5_ok
            and v6_ok
            and version >= SCHEMA_VERSION
        )

    @staticmethod
    def _sessions_constraints_ok(conn: sqlite3.Connection) -> bool:
        """UNIQUE 约束经自动索引实现（origin='u'），无法从 table_info 探知，
        须走 index_list/index_info；FK 同理走 foreign_key_list。
        """
        unique_ok = False
        for idx in conn.execute("PRAGMA index_list(run_sessions)").fetchall():
            if not idx[2]:
                continue
            cols = [r[2] for r in conn.execute(f'PRAGMA index_info("{idx[1]}")')]
            # 仅单列 UNIQUE(session_id) 约束 session 单值；PK (run_id, session_id)
            # 这类复合唯一索引不约束 session 单独唯一。
            if cols == ["session_id"]:
                unique_ok = True
        fk_ok = any(
            fk[2] == "runs" and fk[3] == "run_id" and fk[4] == "run_id"
            for fk in conn.execute("PRAGMA foreign_key_list(run_sessions)")
        )
        return unique_ok and fk_ok

    # ── 库初始化 ──────────────────────────────────────────────────

    @staticmethod
    def _claim_native_id_locked(
        conn: sqlite3.Connection,
        session_id: str,
        native_session_id: str,
    ) -> None:
        """最新绑定唯一持有人（换绑面）：清同 id 旧持有行并绑到本行。

        两条 UPDATE 天然幂等（同值重绑落同值行）；attach 新落行路径对
        自身的 UPDATE 落空由随后的 INSERT 补齐。
        """
        conn.execute(
            "UPDATE run_sessions SET native_session_id = NULL"
            " WHERE native_session_id = ? AND session_id != ?",
            (native_session_id, session_id),
        )
        conn.execute(
            "UPDATE run_sessions SET native_session_id = ? WHERE session_id = ?",
            (native_session_id, session_id),
        )

    @staticmethod
    def _upsert_roster(conn: sqlite3.Connection, fields: dict) -> None:
        cols = list(fields)
        conn.execute(
            f"INSERT OR REPLACE INTO sessions ({', '.join(cols)})"
            f" VALUES ({', '.join('?' for _ in cols)})",
            [fields[c] for c in cols],
        )

    def close(self) -> None:
        """释放单写连接（测试/工具释放文件级独占，如切换 journal_mode）；
        后续访问按需重连，schema 已就绪不重跑。
        """
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    async def create(
        self,
        program_ref: str,
        fmt: str,
        *,
        labels: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        status: str = RunStatus.RUNNING,
        kind: str = RunKind.WORKFLOW,
        work_dir: str | None = None,
        max_parallel: int | None = None,
    ) -> RunRecord:
        return await asyncio.to_thread(
            self._create_locked,
            program_ref,
            fmt,
            labels or {},
            params or {},
            status,
            kind,
            work_dir,
            max_parallel,
        )

    async def get(self, run_id: str) -> RunRecord:
        return await asyncio.to_thread(self._get_locked, run_id)

    async def finish(self, run_id: str, status: str, *, error: str | None = None) -> FinishResult:
        """给 run 写终态的唯一入口（非终态值拒、implicit 拒，守卫在 _finish_locked）。
        终态只落一次：先到的写入成功；后到的写同值不重复写（was_applied=False）；
        想写成不同的终态直接 raise。
        """
        return await asyncio.to_thread(self._finish_locked, run_id, status, error)

    # ── 异步面（事件循环侧调用）──

    async def update_status(self, run_id: str, status: str) -> RunRecord:
        """非终态迁移（pending/running/paused）——终态只能走 finish；不动档位列。"""
        return await asyncio.to_thread(self._update_state_locked, run_id, status)

    async def update_pause_state(
        self, run_id: str, status: str, pause_reason: str | None
    ) -> RunRecord:
        """非终态迁移并连同写暂停档位（user/auth/projection）；同态同档不写。"""
        return await asyncio.to_thread(self._update_state_locked, run_id, status, pause_reason)

    async def set_session_paused_by(
        self, run_id: str, session_id: str, paused_by: str | None
    ) -> None:
        """成员 session 组暂停归属记账（NULL/'user'/'run'）。"""
        await asyncio.to_thread(self._set_session_paused_by_locked, run_id, session_id, paused_by)

    async def reopen(self, run_id: str) -> RunRecord:
        """终态 run 重开：清 finished_at/error，留 reopened_at 审计，回 running。"""
        return await asyncio.to_thread(self._reopen_locked, run_id)

    async def attach_session(
        self,
        run_id: str,
        session_id: str,
        *,
        role: str = "executor",
        context: str | None = None,
        native_session_id: str | None = None,
    ) -> bool:
        """返回是否新落行（False = 已属本 run 的幂等 no-op）。"""
        return await asyncio.to_thread(
            self._attach_locked, run_id, session_id, role, context, native_session_id
        )

    async def mint_implicit(
        self,
        *,
        session_id: str,
        role: str = "executor",
        label: str = "",
        created_at: datetime | None = None,
        roster: dict | None = None,
    ) -> str:
        """铸隐式 run 并落首个成员行（单事务：无「有 run 无成员」的半截态）。

        隐式 run 无程序、无驱动、无 run 根目录；label=session 名；created_at
        对齐 session。roster 给定时 sessions 表行随 mint 同事务落账（session 创建入口单事务），
        其 config_json 无 run_id 时由铸号在事务内注入（旁证首次落库即带归属）。
        返回新 run_id。
        """
        return await asyncio.to_thread(
            self._mint_implicit_locked, session_id, role, label, created_at, roster
        )

    async def discard_affiliation(
        self,
        session_id: str,
        *,
        delete_roster: bool = False,
    ) -> str | None:
        """回滚清理：删归属行；隐式 run 归零成员时连 run 行一起删（单事务）。

        delete_roster=True 时 sessions 表行同事务删除（删除/回滚路径的正向反向
        账面原子对称）。无归属行则幂等 no-op。
        """
        return await asyncio.to_thread(self._discard_affiliation_locked, session_id, delete_roster)

    async def list_session_ids(self, run_id: str) -> list[str]:
        """run 名下成员 session id（按 attached_at 序；全行读走 list_run_sessions）。"""
        return await asyncio.to_thread(self._list_session_ids_locked, run_id)

    async def list_run_affiliations(self) -> list[SessionRunAffiliationRecord]:
        """全部归属行（归属缓存重建的权威源）。"""
        return await asyncio.to_thread(self._list_run_affiliations_locked)

    async def has_runs_table(self) -> bool:
        """runs 族表是否已存在（boot 恢复守卫）；只读探测，不初始化 schema。"""
        return await asyncio.to_thread(self._has_runs_table_locked)

    async def list_runs(
        self,
        *,
        include_implicit: bool = False,
        status: str | None = None,
    ) -> list[RunRecord]:
        """run 列表：消费面默认过滤隐式（对话噪声不淹没显式 run）。"""
        return await asyncio.to_thread(self._list_runs_locked, include_implicit, status)

    async def list_run_sessions(self, run_id: str) -> list[SessionRunAffiliationRecord]:
        """run 名下成员 session 行（按 attached_at 序；world 与 API sessions 共用）。"""
        return await asyncio.to_thread(self._list_run_sessions_locked, run_id)

    async def session_in_run(self, run_id: str, session_id: str) -> bool:
        """成员隶属判定（单行定向查询；成员操作前置守卫）。"""
        return await asyncio.to_thread(self._session_in_run_locked, run_id, session_id)

    async def set_session_context(self, run_id: str, session_id: str, context: str | None) -> None:
        """成员标签补记：归属行由激活钩子先落，context 由执行面补写。"""
        await asyncio.to_thread(self._set_session_context_locked, run_id, session_id, context)

    async def set_session_native_id(self, session_id: str, native_session_id: str) -> None:
        """native 会话 id 记账（最新绑定唯一持有人：先清同 id 旧持有行）。"""
        await asyncio.to_thread(self._set_session_native_id_locked, session_id, native_session_id)

    async def find_session_by_native_id(self, native_session_id: str) -> str | None:
        """native id → Workflow Controller session（hook 路由的权威源查询）。"""
        return await asyncio.to_thread(self._find_session_by_native_id_locked, native_session_id)

    async def save_driver_state(self, run_id: str, blob: dict) -> None:
        """驱动快照（最新）覆盖写；核心不解析 blob。"""
        await asyncio.to_thread(self._save_driver_state_locked, run_id, blob)

    def save_driver_state_sync(self, run_id: str, blob: dict) -> None:
        """驱动决策线程的同步 checkpoint；返回时最新快照已提交。"""
        self._save_driver_state_locked(run_id, blob)

    async def load_driver_state(self, run_id: str) -> dict | None:
        """读驱动最新快照；无则 None。"""
        return await asyncio.to_thread(self._load_driver_state_locked, run_id)

    def load_driver_state_sync(self, run_id: str) -> dict | None:
        """同步面：读驱动最新快照，无则 None。

        事件循环外线程（如驱动决策线程验证检查点轮内提交）的读取通道，
        与 save_driver_state_sync 检查点端口成对。
        """
        return self._load_driver_state_locked(run_id)

    async def save_driver_projection(self, run_id: str, projection: dict) -> None:
        """终态投影覆盖写（仅最新；视图面数据，不入快照历史）。"""
        await asyncio.to_thread(self._save_driver_projection_locked, run_id, projection)

    async def load_driver_projection(self, run_id: str) -> dict | None:
        """读终态投影；未落盘（非终态 / 收尾投影失败 / v4 前旧库）为 None。"""
        return await asyncio.to_thread(self._load_driver_projection_locked, run_id)

    def load_driver_projection_sync(self, run_id: str) -> dict | None:
        """同步面：读终态投影，无则 None（导出装配的单线程批量读）。"""
        return self._load_driver_projection_locked(run_id)

    async def append_driver_snapshot(self, run_id: str, blob: dict) -> int | None:
        """边界快照追加历史：内容哈希去重，未变不入行；返回新行 id 或 None。"""
        return await asyncio.to_thread(self._append_driver_snapshot_locked, run_id, blob)

    async def latest_driver_snapshot_id(self, run_id: str) -> int | None:
        """该 run 最新历史快照行 id（从未追加过为 None）。"""
        return await asyncio.to_thread(self._latest_driver_snapshot_id_locked, run_id)

    async def list_driver_snapshots(self, run_id: str) -> list[dict]:
        """快照历史（(run_id, seq) 有序，导出与回放的数据面）。"""
        return await asyncio.to_thread(self._list_driver_snapshots_locked, run_id)

    async def delete_run(self, run_id: str) -> list[str] | None:
        """单库单事务级联删除，可重复调用；返回成员 session id 清单，run 不存在返回 None。"""
        return await asyncio.to_thread(self._delete_run_locked, run_id)

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None（显式事务）：DDL 与 user_version 盖戳需同一事务，
        # 而 python-sqlite3 的隐式事务只覆盖 DML，不含 DDL/PRAGMA。
        # FK 是每连接开关——建而未启等于没有。
        if self._conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(
                self.db_path,
                timeout=5.0,
                isolation_level=None,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            self._conn = conn
        if not self._schema_ready:
            self._init_db()
        return self._conn

    def _init_db(self) -> None:
        conn = self._conn
        if conn is None:
            raise RuntimeError("连接未初始化：_init_db 仅应经 _connect 抵达")
        self._ensure_schema(conn)
        conn.execute(IDX_RUN_SESSIONS_NATIVE_DDL)
        self._ensure_wal(conn)
        self._schema_ready = True

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        """v2 迁移单显式事务：全部 DDL 与 user_version 盖戳同事务，重放幂等
        （user_version 守卫）——不存在「新形状表 + 旧档位」的半升级态。
        """
        if self._schema_complete(conn):
            return
        self._assert_migratable(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(_RUNS_DDL)
            conn.execute(_RUN_SESSIONS_DDL)
            runs_cols = _table_columns(conn, "runs")
            if "pause_reason" not in runs_cols:
                conn.execute("ALTER TABLE runs ADD COLUMN pause_reason TEXT")
            if "max_parallel" not in runs_cols:
                conn.execute("ALTER TABLE runs ADD COLUMN max_parallel INTEGER")
            for legacy in _RETIRED_RUNS_COLUMNS:
                if legacy in runs_cols:
                    conn.execute(f"ALTER TABLE runs DROP COLUMN {legacy}")
            # events 旧形状（TEXT 主键、无维度列）整体 DROP 重建，零存量不搬数据
            events_cols = _table_columns(conn, "events")
            if events_cols and "uuid" not in events_cols:
                conn.execute("DROP TABLE events")
            conn.execute(EVENTS_DDL)
            conn.execute(IDX_EVENTS_SESSION_DDL)
            conn.execute(IDX_EVENTS_RUN_DDL)
            conn.execute(RUN_EVENTS_DDL)
            conn.execute(IDX_RUN_EVENTS_DDL)
            conn.execute(SESSIONS_DDL)
            conn.execute(RUN_DRIVER_STATE_DDL)
            # v4：旧档库补终态投影列（表可缺——本档内建）
            if "projection" not in _table_columns(conn, "run_driver_state"):
                conn.execute("ALTER TABLE run_driver_state ADD COLUMN projection TEXT")
            conn.execute(RUN_DRIVER_SNAPSHOTS_DDL)
            conn.execute(IDX_RDS_HIST_DDL)
            conn.execute(SCOPE_CHECKPOINTS_DDL)
            conn.execute(IDX_SCOPE_CHECKPOINTS_SCOPE_DDL)
            conn.execute(WATCHDOG_NODE_CLOCKS_DDL)
            conn.execute(IDX_WATCHDOG_NODE_CLOCKS_RUN_DDL)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def _assert_migratable(self, conn: sqlite3.Connection) -> None:
        """schema 不齐一律 fail-fast：阶梯之外只可能是施工残骸，零存量下删库
        重建即可，不做原地补列/整表重建。
        """
        runs_cols = _table_columns(conn, "runs")
        if runs_cols and not set(_V1_RUNS_FEATURES) <= runs_cols:
            raise RuntimeError(
                f"{self.db_path}：runs 表缺 v1 特征列（现有 {sorted(runs_cols)}）——"
                "schema 不齐，删库重建即可"
            )
        sessions_cols = _table_columns(conn, "run_sessions")
        if sessions_cols and (
            not {n for n, _ in _V1_SESSIONS_ADDITIONS} <= sessions_cols
            or not RunStore._sessions_constraints_ok(conn)
        ):
            raise RuntimeError(
                f"{self.db_path}：run_sessions 表列/约束不齐"
                "（UNIQUE(session_id)/FK 缺失）——schema 不齐，删库重建即可"
            )

    # ── 同步面（RLock 内）──

    def _ensure_wal(self, conn: sqlite3.Connection) -> None:
        """每次启动无条件核对 WAL：schema 已完整时若上次启动在事务提交与设 WAL
        之间崩溃，库会永留 DELETE 模式而稳态 boot 不再补救。journal_mode 切换
        失败不抛错，须核对返回值。
        """
        mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        if mode == "wal":
            return
        got = str(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
        if got != "wal":
            raise RuntimeError(
                f"{self.db_path} journal_mode 无法切换为 WAL（实际 {got}）——"
                "并发读写纪律失效，请检查文件系统后重启"
            )

    def _has_runs_table_locked(self) -> bool:
        with self._lock:
            if not self.db_path.exists():
                return False
            try:
                conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
            except sqlite3.Error:
                # 探测失败按有记录处理：走正常访问路径，不静默跳过恢复
                return True
            try:
                row = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runs'"
                ).fetchone()
                return row is not None
            finally:
                conn.close()

    def _create_locked(
        self,
        program_ref: str,
        fmt: str,
        labels: dict[str, str],
        params: dict[str, str],
        status: str = RunStatus.RUNNING,
        kind: str = RunKind.WORKFLOW,
        work_dir: str | None = None,
        max_parallel: int | None = None,
    ) -> RunRecord:
        if status not in NON_TERMINAL_STATUSES:
            raise ValueError(
                f"mint 只能落非终态（{'/'.join(NON_TERMINAL_STATUSES)}），收到：{status!r}"
            )
        if kind not in RunKind.__members__.values():
            raise ValueError(f"kind 只能是 {'/'.join(k.value for k in RunKind)}，收到：{kind!r}")
        validate_kind_format(str(kind), fmt)
        rec = RunRecord(
            run_id=uuid.uuid4().hex[:12],
            program_ref=program_ref,
            format=fmt,
            status=str(status),
            kind=str(kind),
            work_dir=work_dir,
            labels=dict(labels),
            params=dict(params),
            max_parallel=max_parallel,
        )
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, program_ref, format, status, kind,"
                " work_dir, labels, params, max_parallel, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    rec.run_id,
                    rec.program_ref,
                    rec.format,
                    rec.status,
                    rec.kind,
                    rec.work_dir,
                    json.dumps(rec.labels),
                    json.dumps(rec.params),
                    rec.max_parallel,
                    rec.created_at.isoformat(),
                ),
            )
        return rec

    def _get_locked(self, run_id: str) -> RunRecord:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise RunNotFoundError(run_id)
        return _to_record(row)

    def _finish_locked(self, run_id: str, status: str, error: str | None) -> FinishResult:
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"终态只能是 {'/'.join(TERMINAL_STATUSES)}，收到：{status!r}")
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise RunNotFoundError(run_id)
            current = _to_record(row)
            if current.kind == RunKind.IMPLICIT:
                raise ValueError(
                    f"run {run_id} 是 kind=implicit（状态=成员投影，"
                    "不进 finish/reopen 终态机——对话是循环的，隐式 run 永不落终态）"
                )
            if current.finished_at is not None and current.status != status:
                raise ValueError(f"run {run_id} 终态已是 {current.status}，不许改判为 {status}")
            if current.finished_at is not None:  # 终态已落且与请求相同：本次不写，原样返回
                return FinishResult(record=current, was_applied=False)
            conn.execute(
                "UPDATE runs SET status = ?, finished_at = ?, error = ?,"
                " pause_reason = NULL WHERE run_id = ?",
                (status, datetime.now(tz=UTC).isoformat(), error, run_id),
            )
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            return FinishResult(record=_to_record(row), was_applied=True)

    def _update_state_locked(
        self, run_id: str, status: str, pause_reason: str | None | object = _KEEP_PAUSE
    ) -> RunRecord:
        """同步面：非终态迁移守卫（非词表值拒/已终态拒/同态不写，原样返回）。

        带 pause_reason 时连同写档位（同态同档不写）；缺省哨兵不动档位列。
        """
        if status not in NON_TERMINAL_STATUSES:
            raise ValueError(
                f"非终态迁移只能落 {'/'.join(NON_TERMINAL_STATUSES)}，"
                f"终态走 finish，收到：{status!r}"
            )
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise RunNotFoundError(run_id)
            current = _to_record(row)
            if current.status in TERMINAL_STATUSES:
                raise ValueError(
                    f"run {run_id} 已终态（{current.status}），非终态迁移被拒（重开走 reopen）"
                )
            if current.status == status and (
                pause_reason is _KEEP_PAUSE or current.pause_reason == pause_reason
            ):
                return current
            if pause_reason is _KEEP_PAUSE:
                conn.execute(
                    "UPDATE runs SET status = ? WHERE run_id = ?",
                    (str(status), run_id),
                )
            else:
                conn.execute(
                    "UPDATE runs SET status = ?, pause_reason = ? WHERE run_id = ?",
                    (str(status), pause_reason, run_id),
                )
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            return _to_record(row)

    def _set_session_paused_by_locked(
        self, run_id: str, session_id: str, paused_by: str | None
    ) -> None:
        """同步面：成员 session 组暂停归属列写（行不存在时静默 no-op）。"""
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE run_sessions SET paused_by = ? WHERE run_id = ? AND session_id = ?",
                (paused_by, run_id, session_id),
            )

    def _reopen_locked(self, run_id: str) -> RunRecord:
        """同步面：仅终态且非 implicit 可重开，回 running 并留 reopened_at 审计。"""
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise RunNotFoundError(run_id)
            current = _to_record(row)
            if current.kind == RunKind.IMPLICIT:
                raise ValueError(
                    f"run {run_id} 是 kind=implicit（状态=成员投影，不进 finish/reopen 终态机）"
                )
            if current.status not in TERMINAL_STATUSES:
                raise ValueError(f"run {run_id} 非终态（{current.status}），无从 reopen")
            conn.execute(
                "UPDATE runs SET status = ?, finished_at = NULL, error = NULL,"
                " pause_reason = NULL, reopened_at = ? WHERE run_id = ?",
                (RunStatus.RUNNING, datetime.now(tz=UTC).isoformat(), run_id),
            )
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            return _to_record(row)

    def _attach_locked(
        self,
        run_id: str,
        session_id: str,
        role: str,
        context: str | None,
        native_session_id: str | None,
    ) -> bool:
        """同步面：归属行落账——已属本 run 幂等 False，跨 run 冲突 raise。"""
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise RunNotFoundError(run_id)
            owned = conn.execute(
                "SELECT run_id FROM run_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if owned is not None:
                if owned["run_id"] != run_id:
                    # 不用 INSERT OR IGNORE——它会把跨 run 冲突一并吞掉
                    raise ValueError(
                        f"session {session_id} 已属于 run {owned['run_id']}，"
                        f"不能再挂到 run {run_id}（归属转移走 adopt/detach）"
                    )
                # 已属本 run：幂等 no-op——native id 若提供仍走唯一持有人记账，
                # 静默丢弃会让恢复路重挂的绑定退回线性扫兜底
                if native_session_id is None:
                    return False
                self._claim_native_id_locked(conn, session_id, native_session_id)
                return False
            if native_session_id is not None:
                # 最新绑定唯一持有人：新行落账前先清同 id 旧持有行
                self._claim_native_id_locked(conn, session_id, native_session_id)
            conn.execute(
                "INSERT INTO run_sessions (run_id, session_id, role, context,"
                " native_session_id) VALUES (?, ?, ?, ?, ?)",
                (run_id, session_id, role, context, native_session_id),
            )
            return True

    def _list_session_ids_locked(self, run_id: str) -> list[str]:
        """同步面：run 名下 session_id 清单（按 attached_at 序）。"""
        with self._lock, self._connect() as conn:
            exists = conn.execute("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if exists is None:
                raise RunNotFoundError(run_id)
            rows = conn.execute(
                "SELECT session_id FROM run_sessions WHERE run_id = ?"
                " ORDER BY attached_at, session_id",
                (run_id,),
            ).fetchall()
        return [r["session_id"] for r in rows]

    def _list_run_affiliations_locked(self) -> list[SessionRunAffiliationRecord]:
        """同步面：读全表归属行。"""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT session_id, run_id, role, context, native_session_id, paused_by"
                " FROM run_sessions"
            ).fetchall()
        return _rows_to_affiliations(rows)

    def _list_runs_locked(
        self,
        include_implicit: bool,
        status: str | None,
    ) -> list[RunRecord]:
        """同步面：按隐式过滤/状态条件读 run 行。

        单条聚合 SQL（LEFT JOIN 成员表与名册，GROUP BY 聚合成员计数）——
        含隐式 run 的全量列表不得逐 run 查成员。
        """
        conds, args = [], []
        if not include_implicit:
            conds.append("runs.kind != 'implicit'")
        if status is not None:
            conds.append("runs.status = ?")
            args.append(str(status))
        where = f" WHERE {' AND '.join(conds)}" if conds else ""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT runs.*,"
                " COUNT(rs.session_id) AS member_count,"
                " COALESCE(SUM(s.state = 'running'), 0) AS running_count"
                " FROM runs"
                " LEFT JOIN run_sessions rs ON rs.run_id = runs.run_id"
                " LEFT JOIN sessions s ON s.id = rs.session_id"
                f"{where}"
                " GROUP BY runs.run_id"
                " ORDER BY runs.created_at, runs.run_id",
                args,
            ).fetchall()
        records = []
        for row in rows:
            record = _to_record(row)
            record.member_count = row["member_count"]
            record.running_count = row["running_count"]
            records.append(record)
        return records

    def _list_run_sessions_locked(self, run_id: str) -> list[SessionRunAffiliationRecord]:
        """同步面：run 名下成员 session 行（按 attached_at 序）。"""
        with self._lock, self._connect() as conn:
            exists = conn.execute("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if exists is None:
                raise RunNotFoundError(run_id)
            rows = conn.execute(
                "SELECT session_id, run_id, role, context, native_session_id, paused_by"
                " FROM run_sessions WHERE run_id = ?"
                " ORDER BY attached_at, session_id",
                (run_id,),
            ).fetchall()
        return _rows_to_affiliations(rows)

    def _session_in_run_locked(self, run_id: str, session_id: str) -> bool:
        """同步面：session 是否为 run 成员。"""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM run_sessions WHERE run_id = ? AND session_id = ?",
                (run_id, session_id),
            ).fetchone()
            return row is not None

    def _set_session_context_locked(
        self, run_id: str, session_id: str, context: str | None
    ) -> None:
        """同步面：成员标签写；行不存在或同值不写（热路径免冗余写）。"""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT context FROM run_sessions WHERE run_id = ? AND session_id = ?",
                (run_id, session_id),
            ).fetchone()
            if row is None or row["context"] == context:
                return
            conn.execute(
                "UPDATE run_sessions SET context = ? WHERE run_id = ? AND session_id = ?",
                (context, run_id, session_id),
            )

    def _set_session_native_id_locked(self, session_id: str, native_session_id: str) -> None:
        """同步面：native id 记账（最新绑定唯一持有人）。"""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM run_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if row is None:
                return
            self._claim_native_id_locked(conn, session_id, native_session_id)

    def _find_session_by_native_id_locked(self, native_session_id: str) -> str | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT session_id FROM run_sessions WHERE native_session_id = ? LIMIT 1",
                (native_session_id,),
            ).fetchone()
        return row["session_id"] if row else None

    def _save_driver_state_locked(self, run_id: str, blob: dict) -> None:
        """同步面：最新快照覆盖写。未列名的 projection 随 REPLACE 清空——
        终态投影只在收尾快照之后落盘，任何后续快照（如重启续跑）使其失效。
        """
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO run_driver_state (run_id, blob, updated_at)"
                " VALUES (?, ?, ?)",
                (run_id, json.dumps(blob, default=str), datetime.now(tz=UTC).isoformat()),
            )

    def _save_driver_projection_locked(self, run_id: str, projection: dict) -> None:
        """同步面：终态投影覆盖写；快照行不存在时 no-op（收尾前必有最终快照）。"""
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE run_driver_state SET projection = ? WHERE run_id = ?",
                (json.dumps(projection, default=str), run_id),
            )

    def _load_driver_projection_locked(self, run_id: str) -> dict | None:
        """同步面：读终态投影，无则 None。"""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT projection FROM run_driver_state WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        return (
            json.loads(row["projection"])
            if row is not None and row["projection"] is not None
            else None
        )

    def _load_driver_state_locked(self, run_id: str) -> dict | None:
        """同步面：读最新快照，无则 None。"""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT blob FROM run_driver_state WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        return json.loads(row["blob"]) if row is not None else None

    def _append_driver_snapshot_locked(self, run_id: str, blob: dict) -> int | None:
        """同步面：历史快照追加，内容哈希去重；返回新行 id 或 None。"""
        text = json.dumps(blob, default=str, sort_keys=True)
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        with self._lock, self._connect() as conn:
            last = conn.execute(
                "SELECT content_hash FROM run_driver_snapshots WHERE run_id = ?"
                " ORDER BY seq DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            if last is not None and last["content_hash"] == digest:
                return None
            max_seq = conn.execute(
                "SELECT MAX(seq) FROM run_driver_snapshots WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]
            cur = conn.execute(
                "INSERT INTO run_driver_snapshots (run_id, seq, blob, content_hash)"
                " VALUES (?, ?, ?, ?)",
                (run_id, (max_seq or 0) + 1, text, digest),
            )
            return int(cur.lastrowid)

    def _latest_driver_snapshot_id_locked(self, run_id: str) -> int | None:
        """同步面：最新历史快照行 id。"""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM run_driver_snapshots WHERE run_id = ? ORDER BY seq DESC LIMIT 1",
                (run_id,),
            ).fetchone()
        return int(row["id"]) if row is not None else None

    def _list_driver_snapshots_locked(self, run_id: str) -> list[dict]:
        """同步面：快照历史按 seq 有序读出。"""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT seq, blob, content_hash, created_at"
                " FROM run_driver_snapshots WHERE run_id = ? ORDER BY seq",
                (run_id,),
            ).fetchall()
        return [
            {
                "seq": r["seq"],
                "blob": json.loads(r["blob"]),
                "content_hash": r["content_hash"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def _delete_run_locked(self, run_id: str) -> list[str] | None:
        """同步面：级联删除单事务。

        成员数据（events/loop_results/evaluations/handoff_relations）按
        run_sessions 子查询定位后删——早期未打 run_id 戳的事件也靠名册行
        找到，故先删数据后删名册；handoff 两端任一指向成员即删（对齐
        delete_session 口径）；成员的 sessions/run_sessions 行随 run 删，
        不留指向已删 run 的残留；已脱离的 session 不追删，残留 run_id
        戳只是历史痕迹。
        """
        session_subquery = "SELECT session_id FROM run_sessions WHERE run_id = ?"
        with self._lock, self._connect() as conn:
            exists = conn.execute("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if exists is None:
                return None
            conn.execute("BEGIN IMMEDIATE")
            try:
                # loop_results 先于 evaluations：前者持对后者的外键引用
                for table in ("events", "loop_results", "evaluations"):
                    conn.execute(
                        f"DELETE FROM {table} WHERE session_id IN ({session_subquery})",
                        (run_id,),
                    )
                conn.execute(
                    "DELETE FROM handoff_relations"
                    f" WHERE from_session_id IN ({session_subquery})"
                    f" OR to_session_id IN ({session_subquery})",
                    (run_id, run_id),
                )
                session_ids = [
                    r["session_id"] for r in conn.execute(session_subquery, (run_id,)).fetchall()
                ]
                conn.execute(f"DELETE FROM sessions WHERE id IN ({session_subquery})", (run_id,))
                conn.execute("DELETE FROM run_sessions WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM run_events WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM run_driver_snapshots WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM run_driver_state WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM watchdog_node_clocks WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return session_ids

    def _mint_implicit_locked(
        self,
        session_id: str,
        role: str,
        label: str,
        created_at: datetime | None,
        roster: dict | None,
    ) -> str:
        """同步面：铸隐式 run + 首成员行（+ 可选 sessions 表行）单事务。"""
        run_id = uuid.uuid4().hex[:12]
        stamp = (created_at or datetime.now(tz=UTC)).isoformat()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO runs (run_id, program_ref, format, status, kind,"
                    " labels, params, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        "",
                        "",
                        RunStatus.RUNNING,
                        RunKind.IMPLICIT,
                        json.dumps({"name": label} if label else {}),
                        "{}",
                        stamp,
                    ),
                )
                conn.execute(
                    "INSERT INTO run_sessions (run_id, session_id, role) VALUES (?, ?, ?)",
                    (run_id, session_id, role),
                )
                if roster is not None:
                    # roster 在铸号前序列化（run_id 事后才可知），旁证在此补齐——
                    # 首次落库即带归属，不等下一次 persist
                    _stamp_roster_run_id(roster, run_id)
                    self._upsert_roster(conn, roster)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return run_id

    # ── sessions 表事务内嵌写面（与 runs 族同连接——记录原子的连接前提；
    #     普通单行写归 EventStore 写连接）──

    def _discard_affiliation_locked(
        self,
        session_id: str,
        delete_roster: bool,
    ) -> str | None:
        """同步面：删归属行；隐式 run 归零成员时连删 run 行（+ 可选 sessions 表行）单事务。"""
        run_id: str | None = None
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT run_id FROM run_sessions WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                if row is not None:
                    run_id = row["run_id"]
                    conn.execute(
                        "DELETE FROM run_sessions WHERE session_id = ?",
                        (session_id,),
                    )
                    _delete_run_if_implicit_and_empty(conn, run_id)
                if delete_roster:
                    conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return run_id
