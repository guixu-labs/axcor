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

"""工作区残留 TTL 清理器测试：崩溃残留树 + 过期账本行。"""

from __future__ import annotations

import os
import sqlite3
import time
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from workflow_controller.core.checkpoints import CheckpointStore
from workflow_controller.core.workspace.residue_reaper import WorkspaceResidueReaper
from workflow_controller.core.workspace.scope_ledger import ScopeLedgerStore

if TYPE_CHECKING:
    from pathlib import Path


def _make_old(path: Path, days: float = 30.0) -> Path:
    """把路径 mtime 拨回 N 天前（模拟闲置超 TTL 的残留）。"""
    stamp = time.time() - days * 86400
    os.utime(path, (stamp, stamp))
    return path


def _live_session(sid: str, cwd: Path) -> SimpleNamespace:
    """在池会话替身：指定 id 与 cwd。"""
    return SimpleNamespace(id=sid, config=SimpleNamespace(cwd=str(cwd)))


def _plant_tree(original: Path, sid: str) -> Path:
    """在原目录下落一棵崩溃残留树（含 tree-config.json 标记）。"""
    tree_dir = original / ".workflow-controller" / "scopes" / sid
    tree_dir.mkdir(parents=True, exist_ok=True)
    (tree_dir / "tree-config.json").write_text("{}", encoding="utf-8")
    return tree_dir


async def _plant_tree_rows(db_path: Path, sid: str) -> None:
    """给残留树落检查点行（根 + 主写者 + 子代理三层）与作用域账本行（根 + 主写者）。"""
    import sqlite3

    from workflow_controller.core.checkpoints import (
        SCOPE_CHECKPOINTS_DDL,
        BarrierQuality,
        CheckpointTrigger,
        ScopeCheckpoint,
        ScopeKind,
    )
    from workflow_controller.core.workspace.scope_ledger import SCOPES_DDL

    con = sqlite3.connect(db_path)
    con.execute(SCOPE_CHECKPOINTS_DDL)
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_scope_checkpoints_scope"
        " ON scope_checkpoints (scope_id, created_at)"
    )
    con.execute(SCOPES_DDL)
    for seq, (scope_id, parent, kind) in enumerate(
        [
            (f"{sid}-root", None, "root"),
            (f"{sid}-main", f"{sid}-root", "main_writer"),
        ],
        start=1,
    ):
        con.execute(
            "INSERT INTO scopes (root_scope_id, scope_id, parent_scope_id, scope_kind,"
            " scope_epoch, root_epoch, gate_state, state, created_at, seq)"
            " VALUES (?, ?, ?, ?, 1, 1, 'running', 'active', '2026-11-20T10:00:00+00:00', ?)",
            (f"{sid}-root", scope_id, parent, kind, seq),
        )
    con.commit()
    con.close()
    store = CheckpointStore(db_path)
    base = dict(
        scope_epoch=1,
        root_epoch=1,
        trigger=CheckpointTrigger.SCOPE_START,
        barrier_quality=BarrierQuality.PRECISE,
        workspace_commit="abc123",
        created_at="2026-11-20T10:00:00+00:00",
    )
    await store.record(
        ScopeCheckpoint(
            checkpoint_id=f"{sid}-root:scope_start:e1",
            scope_id=f"{sid}-root",
            scope_kind=ScopeKind.ROOT,
            parent_scope_id=None,
            **base,
        )
    )
    await store.record(
        ScopeCheckpoint(
            checkpoint_id=f"{sid}-main:scope_start:e1",
            scope_id=f"{sid}-main",
            scope_kind=ScopeKind.MAIN_WRITER,
            parent_scope_id=f"{sid}-root",
            **base,
        )
    )
    await store.record(
        ScopeCheckpoint(
            checkpoint_id=f"{sid}-w1:before_child:g1",
            scope_id=f"{sid}-w1",
            scope_kind=ScopeKind.SUBAGENT,
            parent_scope_id=f"{sid}-main",
            trigger=CheckpointTrigger.BEFORE_CHILD,
            barrier_quality=BarrierQuality.PRECISE,
            scope_epoch=1,
            root_epoch=1,
            workspace_commit="def456",
            created_at="2026-11-20T10:01:00+00:00",
        )
    )
    store.close()


def _reaper(
    tmp_path: Path,
    sessions: list[Any],
    *,
    ttl_days: float = 7.0,
    enabled: bool = True,
    pool_available: bool = True,
    scan_roots: list[Path] | None = None,
) -> WorkspaceResidueReaper:
    """测试装配：检查点库在 tmp_path，池枚举与扫描根按注入替身。"""
    db_path = tmp_path / "events.db"
    db_path.touch()
    return WorkspaceResidueReaper(
        checkpoint_db_path=lambda: db_path,
        list_sessions=(lambda: sessions) if pool_available else None,
        scan_roots=(lambda: scan_roots) if scan_roots is not None else None,
        ttl_days=ttl_days,
        enabled=enabled,
    )


async def test_reaps_crash_residual_tree_and_rows(tmp_path: Path) -> None:
    """会话已消失 + 树超 TTL → 整树回收 + 三层检查点行与作用域账本行删除。"""
    original = tmp_path / "proj"
    original.mkdir()
    sid = uuid.uuid4().hex
    tree_dir = _make_old(_plant_tree(original, sid))
    await _plant_tree_rows(tmp_path / "events.db", sid)
    # 另一个无关会话仍持有自己的树，但不在池里且树龄新 → 不动
    fresh_tree = _plant_tree(original, uuid.uuid4().hex)

    report = await _reaper(tmp_path, sessions=[], scan_roots=[original]).reap()

    assert report["removed_trees"] == 1
    assert report["removed_checkpoint_rows"] == 3
    assert not tree_dir.exists()
    assert fresh_tree.exists()
    store = CheckpointStore(tmp_path / "events.db")
    assert await store.list_for_tree(f"{sid}-root") == []
    store.close()
    ledger = ScopeLedgerStore(tmp_path / "events.db", root_scope_id=f"{sid}-root")
    assert await ledger.list_for_tree() == []
    ledger.close()


async def test_keeps_live_session_tree(tmp_path: Path) -> None:
    """会话仍在池内 → 树属其生命周期（深度恢复可续用），不回收。"""
    original = tmp_path / "proj"
    original.mkdir()
    sid = uuid.uuid4().hex
    tree_dir = _make_old(_plant_tree(original, sid))
    await _plant_tree_rows(tmp_path / "events.db", sid)

    report = await _reaper(tmp_path, sessions=[_live_session(sid, original)]).reap()

    assert report["removed_trees"] == 0
    assert report["removed_checkpoint_rows"] == 0
    assert tree_dir.exists()
    con = sqlite3.connect(tmp_path / "events.db")
    assert con.execute("SELECT count(*) FROM scope_checkpoints").fetchone()[0] == 3
    con.close()


async def test_managed_cwd_derived_scan_root(tmp_path: Path) -> None:
    """会话 cwd 已指向托管工作区时仍能反推原目录并清残留。"""
    original = tmp_path / "proj"
    original.mkdir()
    sid = uuid.uuid4().hex
    tree_dir = _make_old(_plant_tree(original, sid))
    managed_cwd = original / ".workflow-controller" / "scopes" / sid / "root"

    report = await _reaper(tmp_path, sessions=[_live_session(sid, managed_cwd)]).reap()

    # 会话存活即受保护（无论 cwd 形态）
    assert report["removed_trees"] == 0
    assert tree_dir.exists()

    gone = uuid.uuid4().hex
    gone_tree = _make_old(_plant_tree(original, gone))
    report2 = await _reaper(tmp_path, sessions=[_live_session(sid, original)]).reap()
    assert report2["removed_trees"] == 1
    assert not gone_tree.exists()


async def test_pool_unavailable_skips_entirely(tmp_path: Path) -> None:
    """池枚举不可用（活动集不可判定）→ 整轮跳过，残留树不动（fail-closed）。"""
    original = tmp_path / "proj"
    original.mkdir()
    tree_dir = _make_old(_plant_tree(original, uuid.uuid4().hex))
    report = await _reaper(
        tmp_path, sessions=[], pool_available=False, scan_roots=[original]
    ).reap()
    assert report["skipped"] == "pool enumeration unavailable"
    assert tree_dir.exists()


async def test_disabled_reaper_noop(tmp_path: Path) -> None:
    """enabled=False 时零动作。"""
    original = tmp_path / "proj"
    original.mkdir()
    tree_dir = _make_old(_plant_tree(original, uuid.uuid4().hex))
    report = await _reaper(tmp_path, sessions=[], enabled=False, scan_roots=[original]).reap()
    assert report["skipped"] == "disabled"
    assert tree_dir.exists()
