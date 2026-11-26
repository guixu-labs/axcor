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

"""工作区残留 TTL 清理器测试：崩溃残留树 + 过期检查点行 + fork 孤儿目录。"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from workflow_controller.core.checkpoints import CheckpointStore
from workflow_controller.core.workspace.residue_reaper import (
    FORK_TEMPDIR_PREFIX,
    WorkspaceResidueReaper,
)

if TYPE_CHECKING:
    from pathlib import Path


def _make_old(path: Path, days: float = 30.0) -> Path:
    """把路径 mtime 拨回 N 天前（模拟闲置超 TTL 的残留）。"""
    stamp = time.time() - days * 86400
    os.utime(path, (stamp, stamp))
    return path


def _live_session(sid: str, cwd: Path) -> SimpleNamespace:
    """在池会话替身：近期活动 + 指定 cwd。"""
    return SimpleNamespace(
        id=sid,
        last_activity=datetime.now(tz=UTC),
        plugin_metadata={},
        config=SimpleNamespace(cwd=str(cwd)),
    )


def _gone_session(sid: str, cwd: Path, *, idle_days: float = 30.0) -> SimpleNamespace:
    """闲置超 TTL 的会话替身（ghost，不受 fork 保护）。"""
    return SimpleNamespace(
        id=sid,
        last_activity=datetime.now(tz=UTC) - timedelta(days=idle_days),
        plugin_metadata={},
        config=SimpleNamespace(cwd=str(cwd)),
    )


def _plant_tree(original: Path, sid: str) -> Path:
    """在原目录下落一棵崩溃残留树（含 tree-config.json 标记）。"""
    tree_dir = original / ".workflow-controller" / "scopes" / sid
    tree_dir.mkdir(parents=True, exist_ok=True)
    (tree_dir / "tree-config.json").write_text("{}", encoding="utf-8")
    return tree_dir


async def _plant_tree_rows(db_path: Path, sid: str) -> None:
    """给残留树落检查点行（根 + 主写者 + 子代理三层）。"""
    import sqlite3

    from workflow_controller.core.checkpoints import (
        SCOPE_CHECKPOINTS_DDL,
        BarrierQuality,
        CheckpointTrigger,
        ScopeCheckpoint,
        ScopeKind,
    )

    con = sqlite3.connect(db_path)
    con.execute(SCOPE_CHECKPOINTS_DDL)
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_scope_checkpoints_scope"
        " ON scope_checkpoints (scope_id, created_at)"
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
    """会话已消失 + 树超 TTL → 整树回收 + 三层检查点行删除。"""
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


async def test_fork_root_liveness_protection(tmp_path: Path, monkeypatch: Any) -> None:
    """fork 孤儿目录：近期活动会话引用的受保护，闲置 ghost 与无名残留回收。"""
    sandbox = tmp_path / "tmp"
    sandbox.mkdir()
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(sandbox))
    live_dir = sandbox / f"{FORK_TEMPDIR_PREFIX}live"
    live_dir.mkdir()
    (live_dir / "fork.db").write_text("x", encoding="utf-8")
    stale_dir = sandbox / f"{FORK_TEMPDIR_PREFIX}stale"
    stale_dir.mkdir()
    _make_old(stale_dir)

    original = tmp_path / "proj"
    original.mkdir()
    live_sid = uuid.uuid4().hex
    live = _live_session(live_sid, original)
    live.plugin_metadata = {"traceback_replay": {"birth": {"fork_root": str(live_dir)}}}
    report = await _reaper(tmp_path, sessions=[live], scan_roots=[original]).reap()

    assert live_dir.exists()
    assert not stale_dir.exists()
    assert report["removed_fork_roots"] == 1

    # 活动集整体不可判定 → 跳过（fail-closed）
    stale2 = sandbox / f"{FORK_TEMPDIR_PREFIX}stale2"
    stale2.mkdir()
    _make_old(stale2)
    report2 = await _reaper(
        tmp_path, sessions=[], pool_available=False, scan_roots=[original]
    ).reap()
    assert report2["skipped"] == "pool enumeration unavailable"
    assert stale2.exists()


async def test_disabled_reaper_noop(tmp_path: Path) -> None:
    """enabled=False 时零动作。"""
    original = tmp_path / "proj"
    original.mkdir()
    tree_dir = _make_old(_plant_tree(original, uuid.uuid4().hex))
    report = await _reaper(tmp_path, sessions=[], enabled=False, scan_roots=[original]).reap()
    assert report["skipped"] == "disabled"
    assert tree_dir.exists()
