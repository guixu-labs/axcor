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

"""作用域检查点账本——回滚恢复点的持久化行。

scope_checkpoints 表住项目库 events.db，建表归 run 记录库的统一迁移管理，
本模块只提供读写面、不自行建表。每行绑定「文件态提交引用 + 可选对话恢复
锚点 + 不透明编排状态引用」；文件提交引用必须先于行存在（record 入口强制
workspace_commit 必填）。遗留插件在同一库内维护自己的旧检查点表，以旧列集
继续读写，与本表互不相扰。
"""

from __future__ import annotations

__all__ = [
    "IDX_SCOPE_CHECKPOINTS_SCOPE_DDL",
    "SCOPE_CHECKPOINTS_DDL",
    "BarrierQuality",
    "CheckpointStore",
    "CheckpointTrigger",
    "DispatchAnchor",
    "ScopeCheckpoint",
    "checkpoint_to_dict",
]

import asyncio
import json
import sqlite3
import threading
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from workflow_controller.core.workspace.scope_tree import ScopeKind

if TYPE_CHECKING:
    from pathlib import Path


class CheckpointTrigger(StrEnum):
    """检查点触发时机：作用域建立 / 派发屏障内 / 会话终态。"""

    SCOPE_START = "scope_start"
    BEFORE_CHILD = "before_child"
    TERMINAL = "terminal"


class BarrierQuality(StrEnum):
    """派发屏障质量：精确屏障 / 拦截滞后的弱化标注。"""

    PRECISE = "precise"
    LATE_BARRIER = "late_barrier"


@dataclass(frozen=True)
class DispatchAnchor:
    """派发屏障内形成的对话恢复锚点六元组（子会话诞生前即可写出）。"""

    parent_native_session_id: str
    parent_message_id: str
    tool_use_id: str
    preallocated_scope_id: str
    exclude_message_id: str
    dispatch_group_id: str


@dataclass(frozen=True)
class ScopeCheckpoint:
    """检查点行：字段与表列一一对应，JSON 列在内存侧还原为结构化值。"""

    checkpoint_id: str
    scope_id: str
    scope_kind: ScopeKind
    scope_epoch: int
    root_epoch: int
    trigger: CheckpointTrigger
    barrier_quality: BarrierQuality
    workspace_commit: str
    created_at: str
    parent_scope_id: str | None = None
    workspace_branch: str | None = None
    integration_commit: str | None = None
    dispatch_anchor: DispatchAnchor | None = None
    child_session_map: dict[str, str] | None = None
    claims: tuple[str, ...] = ()
    orchestration_state_ref: str | None = None
    file_count: int = 0
    byte_size: int = 0
    status: str = "ok"


SCOPE_CHECKPOINTS_DDL = """
    CREATE TABLE IF NOT EXISTS scope_checkpoints (
        checkpoint_id TEXT PRIMARY KEY,
        scope_id TEXT NOT NULL,
        scope_kind TEXT NOT NULL,
        parent_scope_id TEXT,
        scope_epoch INTEGER NOT NULL,
        root_epoch INTEGER NOT NULL,
        trigger TEXT NOT NULL,
        barrier_quality TEXT NOT NULL,
        workspace_branch TEXT,
        workspace_commit TEXT,
        integration_commit TEXT,
        dispatch_anchor TEXT,
        child_session_map TEXT,
        claims TEXT,
        orchestration_state_ref TEXT,
        created_at TEXT NOT NULL,
        file_count INTEGER DEFAULT 0,
        byte_size INTEGER DEFAULT 0,
        status TEXT DEFAULT 'ok'
    )
"""

# 作用域内按时间取恢复点的热路径索引（幂等，随统一迁移铺设）
IDX_SCOPE_CHECKPOINTS_SCOPE_DDL = """
    CREATE INDEX IF NOT EXISTS idx_scope_checkpoints_scope
    ON scope_checkpoints (scope_id, created_at)
"""

_CHECKPOINT_COLUMNS = (
    "checkpoint_id, scope_id, scope_kind, parent_scope_id, scope_epoch, root_epoch, "
    "trigger, barrier_quality, workspace_branch, workspace_commit, integration_commit, "
    "dispatch_anchor, child_session_map, claims, orchestration_state_ref, created_at, "
    "file_count, byte_size, status"
)


def _row_to_checkpoint(row: sqlite3.Row) -> ScopeCheckpoint:
    """行 → 检查点记录：JSON 列还原为结构化值，枚举列按词面重建。"""
    anchor_raw = row["dispatch_anchor"]
    child_map_raw = row["child_session_map"]
    claims_raw = row["claims"]
    return ScopeCheckpoint(
        checkpoint_id=row["checkpoint_id"],
        scope_id=row["scope_id"],
        scope_kind=ScopeKind(row["scope_kind"]),
        parent_scope_id=row["parent_scope_id"],
        scope_epoch=int(row["scope_epoch"]),
        root_epoch=int(row["root_epoch"]),
        trigger=CheckpointTrigger(row["trigger"]),
        barrier_quality=BarrierQuality(row["barrier_quality"]),
        workspace_branch=row["workspace_branch"],
        workspace_commit=row["workspace_commit"] or "",
        integration_commit=row["integration_commit"],
        dispatch_anchor=DispatchAnchor(**json.loads(anchor_raw)) if anchor_raw else None,
        child_session_map=dict(json.loads(child_map_raw)) if child_map_raw else None,
        claims=tuple(json.loads(claims_raw)) if claims_raw else (),
        orchestration_state_ref=row["orchestration_state_ref"],
        created_at=row["created_at"],
        file_count=int(row["file_count"] or 0),
        byte_size=int(row["byte_size"] or 0),
        status=row["status"] or "ok",
    )


class CheckpointStore:
    """作用域检查点账本的读写面（建表归 run 记录库统一迁移）。"""

    def __init__(self, db_path: Path) -> None:
        """只记路径不落盘：连接与表核查随首次访问发生。"""
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._schema_checked = False

    def close(self) -> None:
        """释放复用连接（测试与工具释放文件级占用）。"""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # ── 异步面（事件循环侧调用）──

    async def record(self, checkpoint: ScopeCheckpoint) -> None:
        """落一行检查点；按 checkpoint_id 幂等覆盖（崩溃补账重放安全）。"""
        await asyncio.to_thread(self._record_locked, checkpoint)

    async def get(self, checkpoint_id: str) -> ScopeCheckpoint | None:
        """读单个检查点；不存在返回 None。"""
        return await asyncio.to_thread(self._get_locked, checkpoint_id)

    async def list_for_scope(self, scope_id: str) -> list[ScopeCheckpoint]:
        """作用域名下全部检查点（按创建时刻序）。"""
        return await asyncio.to_thread(self._list_for_scope_locked, scope_id)

    async def list_for_tree(self, root_scope_id: str) -> list[ScopeCheckpoint]:
        """一棵作用域树的全部检查点（根与直接子作用域，按创建时刻序）。

        更深层级（子代理挂主写者下）随派发屏障接线扩展为递归收集。
        """
        return await asyncio.to_thread(self._list_for_tree_locked, root_scope_id)

    async def attach_child_session(
        self,
        checkpoint_id: str,
        child_scope_id: str,
        native_child_session_id: str,
    ) -> None:
        """向派发屏障检查点回填子会话映射（子会话启动后的补账）。"""
        await asyncio.to_thread(
            self._attach_child_session_locked,
            checkpoint_id,
            child_scope_id,
            native_child_session_id,
        )

    async def delete_for_tree(self, root_scope_id: str) -> int:
        """删一棵作用域树的全部检查点行（树回收/残留清理用），返回删除行数。"""
        return await asyncio.to_thread(self._delete_for_tree_locked, root_scope_id)

    # ── 同步面（锁内）──

    def _record_locked(self, checkpoint: ScopeCheckpoint) -> None:
        """同步面：接口形态校验后落行。"""
        _validate_for_record(checkpoint)
        params = (
            checkpoint.checkpoint_id,
            checkpoint.scope_id,
            checkpoint.scope_kind.value,
            checkpoint.parent_scope_id,
            checkpoint.scope_epoch,
            checkpoint.root_epoch,
            checkpoint.trigger.value,
            checkpoint.barrier_quality.value,
            checkpoint.workspace_branch,
            checkpoint.workspace_commit,
            checkpoint.integration_commit,
            (
                json.dumps(asdict(checkpoint.dispatch_anchor))
                if checkpoint.dispatch_anchor is not None
                else None
            ),
            (
                json.dumps(checkpoint.child_session_map)
                if checkpoint.child_session_map is not None
                else None
            ),
            json.dumps(list(checkpoint.claims)) if checkpoint.claims else None,
            checkpoint.orchestration_state_ref,
            checkpoint.created_at,
            checkpoint.file_count,
            checkpoint.byte_size,
            checkpoint.status,
        )
        with self._lock, self._connect() as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO scope_checkpoints ({_CHECKPOINT_COLUMNS})"
                f" VALUES ({', '.join('?' for _ in params)})",
                params,
            )

    def _get_locked(self, checkpoint_id: str) -> ScopeCheckpoint | None:
        """同步面：按标识读单行。"""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                f"SELECT {_CHECKPOINT_COLUMNS} FROM scope_checkpoints WHERE checkpoint_id = ?",
                (checkpoint_id,),
            ).fetchone()
        return None if row is None else _row_to_checkpoint(row)

    def _list_for_scope_locked(self, scope_id: str) -> list[ScopeCheckpoint]:
        """同步面：作用域内按创建时刻序读全部行。"""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"SELECT {_CHECKPOINT_COLUMNS} FROM scope_checkpoints"
                " WHERE scope_id = ? ORDER BY created_at, checkpoint_id",
                (scope_id,),
            ).fetchall()
        return [_row_to_checkpoint(row) for row in rows]

    def _list_for_tree_locked(self, root_scope_id: str) -> list[ScopeCheckpoint]:
        """同步面：树内（根与直接子作用域）按创建时刻序读全部行。"""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"SELECT {_CHECKPOINT_COLUMNS} FROM scope_checkpoints"
                " WHERE scope_id = ? OR parent_scope_id = ?"
                " ORDER BY created_at, checkpoint_id",
                (root_scope_id, root_scope_id),
            ).fetchall()
        return [_row_to_checkpoint(row) for row in rows]

    def _attach_child_session_locked(
        self,
        checkpoint_id: str,
        child_scope_id: str,
        native_child_session_id: str,
    ) -> None:
        """同步面：读改写回填子会话映射，单事务防并发丢更新。"""
        if not child_scope_id:
            raise ValueError("child_scope_id 不能为空")
        if not native_child_session_id:
            raise ValueError("native_child_session_id 不能为空")
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT trigger, child_session_map FROM scope_checkpoints"
                    " WHERE checkpoint_id = ?",
                    (checkpoint_id,),
                ).fetchone()
                if row is None:
                    raise LookupError(f"检查点不存在：{checkpoint_id}")
                if row["trigger"] != CheckpointTrigger.BEFORE_CHILD.value:
                    raise ValueError(
                        f"子会话映射只回填到派发屏障检查点：{checkpoint_id}（{row['trigger']}）"
                    )
                mapping: dict[str, str] = (
                    dict(json.loads(row["child_session_map"])) if row["child_session_map"] else {}
                )
                mapping[child_scope_id] = native_child_session_id
                conn.execute(
                    "UPDATE scope_checkpoints SET child_session_map = ? WHERE checkpoint_id = ?",
                    (json.dumps(mapping), checkpoint_id),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def _delete_for_tree_locked(self, root_scope_id: str) -> int:
        """同步面：按父子链展开树内全部作用域并删行，返回删除行数。"""
        if not root_scope_id:
            return 0
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                known = {root_scope_id}
                frontier = [root_scope_id]
                while frontier:
                    placeholders = ",".join("?" for _ in frontier)
                    children = [
                        str(row[0])
                        for row in conn.execute(
                            "SELECT DISTINCT scope_id FROM scope_checkpoints"
                            f" WHERE parent_scope_id IN ({placeholders})",
                            frontier,
                        ).fetchall()
                        if str(row[0]) not in known
                    ]
                    known.update(children)
                    frontier = children
                placeholders = ",".join("?" for _ in known)
                cursor = conn.execute(
                    f"DELETE FROM scope_checkpoints WHERE scope_id IN ({placeholders})",
                    sorted(known),
                )
                conn.execute("COMMIT")
                return cursor.rowcount
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def _connect(self) -> sqlite3.Connection:
        """懒开复用连接；首次访问核对检查点表已在库（本模块不建表）。"""
        if self._conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(
                self.db_path,
                timeout=5.0,
                isolation_level=None,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=5000")
            self._conn = conn
        if not self._schema_checked:
            self._require_scope_checkpoints_table(self._conn)
            self._schema_checked = True
        return self._conn

    def _require_scope_checkpoints_table(self, conn: sqlite3.Connection) -> None:
        """表缺席即快速失败：须经 run 记录库统一迁移铺设。"""
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'scope_checkpoints'"
        ).fetchone()
        if present is None:
            raise RuntimeError(
                f"{self.db_path}：scope_checkpoints 表未建——"
                "检查点账本须经 run 记录库的统一迁移建表后使用"
            )


def checkpoint_to_dict(checkpoint: ScopeCheckpoint) -> dict[str, Any]:
    """检查点行的对外形状（查询面序列化）。"""
    return {
        "checkpoint_id": checkpoint.checkpoint_id,
        "scope_id": checkpoint.scope_id,
        "scope_kind": checkpoint.scope_kind.value,
        "parent_scope_id": checkpoint.parent_scope_id,
        "scope_epoch": checkpoint.scope_epoch,
        "root_epoch": checkpoint.root_epoch,
        "trigger": checkpoint.trigger.value,
        "barrier_quality": checkpoint.barrier_quality.value,
        "workspace_commit": checkpoint.workspace_commit,
        "workspace_branch": checkpoint.workspace_branch,
        "integration_commit": checkpoint.integration_commit,
        "dispatch_anchor": asdict(checkpoint.dispatch_anchor)
        if checkpoint.dispatch_anchor is not None
        else None,
        "child_session_map": dict(checkpoint.child_session_map)
        if checkpoint.child_session_map is not None
        else None,
        "claims": list(checkpoint.claims),
        "orchestration_state_ref": checkpoint.orchestration_state_ref,
        "created_at": checkpoint.created_at,
        "file_count": checkpoint.file_count,
        "byte_size": checkpoint.byte_size,
        "status": checkpoint.status,
    }


def _validate_for_record(checkpoint: ScopeCheckpoint) -> None:
    """落行前的接口形态校验：文件提交引用先行、枚举与代次合法、锚点归属触发。

    作用域身份与编排状态引用的存在性不在此层（接线方负责）；此处只保证
    「先有文件提交引用再有行」的写入形态与词面合法。
    """
    if not checkpoint.checkpoint_id:
        raise ValueError("checkpoint_id 不能为空")
    if not checkpoint.scope_id:
        raise ValueError("scope_id 不能为空")
    if not isinstance(checkpoint.scope_kind, ScopeKind):
        raise ValueError("scope_kind 必须是作用域种类枚举")
    if not isinstance(checkpoint.trigger, CheckpointTrigger):
        raise ValueError("trigger 必须是检查点触发枚举")
    if not isinstance(checkpoint.barrier_quality, BarrierQuality):
        raise ValueError("barrier_quality 必须是屏障质量枚举")
    if checkpoint.scope_epoch < 1 or checkpoint.root_epoch < 1:
        raise ValueError("作用域代次与根代次均不得小于 1")
    if not checkpoint.workspace_commit:
        raise ValueError("workspace_commit 必填：文件态提交引用须先于检查点行存在")
    if (
        checkpoint.dispatch_anchor is not None
        and checkpoint.trigger is not CheckpointTrigger.BEFORE_CHILD
    ):
        raise ValueError("对话恢复锚点只随派发屏障检查点落账")
    if (
        checkpoint.barrier_quality is BarrierQuality.LATE_BARRIER
        and checkpoint.trigger is not CheckpointTrigger.BEFORE_CHILD
    ):
        raise ValueError("滞后屏障标注只适用于派发屏障检查点")
