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

"""历史检查点兼容面测试：兼容读取器、双代合并查询与文件预览。"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from workflow_controller.config.settings import SessionTracebackConfig
from workflow_controller.core.checkpoint_compat import (
    LegacyCheckpoint,
    LegacyCheckpointReader,
    legacy_checkpoint_to_dict,
    store_legacy_checkpoint,
)
from workflow_controller.core.runs import RunStore
from workflow_controller.core.session_traceback import SessionTracebackHost
from workflow_controller.core.session_workspace import SessionWorkspaceHost

if TYPE_CHECKING:
    from pathlib import Path


def _legacy_row(session_id: str, checkpoint_id: str, **overrides: Any) -> LegacyCheckpoint:
    """一条典型历史检查点行（subagent_start 触发、带 git 引用）。"""
    fields: dict[str, Any] = {
        "checkpoint_id": checkpoint_id,
        "session_id": session_id,
        "iteration": 1,
        "agent_id": "agt-1",
        "agent_type": "reviewer",
        "trigger": "subagent_start",
        "git_ref": "abc1234",
        "event_rowid": None,
        "main_part_id": "prt_1",
        "parent_checkpoint_id": None,
        "created_at": "2026-11-20T10:00:00+00:00",
        "file_count": 3,
        "byte_size": 100,
        "status": "ok",
        "inner_git": "captured",
    }
    fields.update(overrides)
    return LegacyCheckpoint(**fields)


def test_reader_roundtrip_and_absent_table(tmp_path: Path) -> None:
    """读取器往返：旧表十五列逐字段一致；库/表缺席为空结果（不建表）。"""
    db = tmp_path / "events.db"
    row = _legacy_row("sess-1", "cp_1234_001", parent_checkpoint_id=None)
    store_legacy_checkpoint(db, row)

    reader = LegacyCheckpointReader(db)
    listed = reader.list_for_session("sess-1")
    assert len(listed) == 1
    assert listed[0] == row
    assert reader.get("cp_1234_001") == row
    assert reader.get("missing") is None
    assert reader.list_for_session("other") == []

    # 读取面不建库不建表
    empty = LegacyCheckpointReader(tmp_path / "absent.db")
    assert empty.list_for_session("sess-1") == []
    assert empty.get("cp_1234_001") is None
    assert not (tmp_path / "absent.db").exists()

    # 空库（文件在、表不在）同样为空
    bare = tmp_path / "bare.db"
    sqlite3.connect(bare).close()
    assert LegacyCheckpointReader(bare).list_for_session("sess-1") == []


def test_legacy_dict_marks_snapshot_generation() -> None:
    """历史行对外形状：插件列表口径字段齐全 + 来源代标注。"""
    data = legacy_checkpoint_to_dict(_legacy_row("sess-1", "cp_1", main_part_id=None))
    assert data["generation"] == "snapshot"
    assert data["trigger"] == "subagent_start"
    assert data["main_part_id"] is None
    assert data["inner_git"] == "captured"


class PreviewHarness:
    """双代预览装配：真实 git（快照仓 + 会话树）与假会话事实。"""

    def __init__(self, tmp_path: Path) -> None:
        self.db_path = tmp_path / "events.db"
        self.original = tmp_path / "proj"
        self.original.mkdir(parents=True)
        (self.original / "design").mkdir()
        (self.original / "design" / "base.md").write_text("base\n", encoding="utf-8")
        self.sessions: dict[str, Any] = {}
        self.roster: dict[str, dict[str, Any]] = {}
        self.workspace_host = SessionWorkspaceHost(
            checkpoint_db_path=lambda: self.db_path,
            resolve_session=self._resolve_session,
            persist_session=self._persist_session,
            stop_session=self._noop,
            start_resumed_session=self._start_resumed,
        )
        self.host = SessionTracebackHost(
            checkpoint_db_path=lambda: self.db_path,
            resolve_session=self._resolve_session,
            load_roster_session=self._load_roster,
            session_workspaces=self.workspace_host,
            start_resumed_session=self._start_resumed,
            create_session=self._start_resumed,
            persist_session=self._persist_session,
            store_events=self._noop_events,
            default_runtime=lambda: "claude_sdk",
            resolve_runtime=lambda name: name or "claude_sdk",
        )

    async def migrate(self) -> None:
        await RunStore(self.db_path).list_runs()

    def add_session(
        self, session_id: str, *, cwd: Path | None = None, traceback: Any = None
    ) -> None:
        self.sessions[session_id] = SimpleNamespace(
            id=session_id,
            config=SimpleNamespace(
                cwd=str(cwd or self.original),
                runtime="claude_sdk",
                traceback=traceback if traceback is not None else SessionTracebackConfig(),
            ),
        )

    def plant_roster(self, session_id: str, cwd: Path) -> None:
        """持久化行形态的会话（池外重开场景）。"""
        import json

        self.roster[session_id] = {"config_json": json.dumps({"cwd": str(cwd)})}

    def build_snapshot_repo(self) -> str:
        """按插件快照仓口径建仓并打一个检查点提交，返回 git 引用。"""
        snap = self.original / ".workflow-controller" / "snapshots"
        snap.mkdir(parents=True, exist_ok=True)
        working = snap / "working"
        (working / "docs").mkdir(parents=True)
        (working / "docs" / "spec.md").write_text("v1\n", encoding="utf-8")
        (working / "run.log").write_text("log\n", encoding="utf-8")
        (working / ".workflow-controller-inner-git").mkdir()
        (working / ".workflow-controller-inner-git" / "HEAD").write_text("ref: refs/heads/main\n")
        for args in (
            ["init", "--quiet"],
            ["add", "-A", "--force"],
            ["commit", "--quiet", "-m", "cp1", "--allow-empty"],
        ):
            result = _git(snap, args)
            assert result.returncode == 0, result.stderr
        return _git(snap, ["rev-parse", "HEAD"]).stdout.strip()

    def plant_legacy_checkpoint(self, session_id: str, checkpoint_id: str, git_ref: str) -> None:
        store_legacy_checkpoint(
            self.db_path,
            _legacy_row(session_id, checkpoint_id, git_ref=git_ref, main_part_id="prt_seed"),
        )

    async def _resolve_session(self, session_id: str) -> Any:
        return self.sessions.get(session_id)

    async def _load_roster(self, session_id: str) -> dict[str, Any] | None:
        return self.roster.get(session_id)

    async def _persist_session(self, session: Any) -> None:
        return None

    async def _noop(self, session_id: str) -> None:
        return None

    async def _start_resumed(self, config: Any, **kwargs: Any) -> Any:
        return SimpleNamespace(id="new-session", config=config)

    async def _noop_events(self, events: list[Any]) -> None:
        return None


def _git(cwd: Path, args: list[str]) -> Any:
    import subprocess

    return subprocess.run(
        ["git", "-c", "core.hooksPath=", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )


@pytest.mark.asyncio
async def test_merged_listing_marks_both_generations(tmp_path: Path) -> None:
    """合并查询：工作区行与历史行并存，各自标注来源代。"""
    h = PreviewHarness(tmp_path)
    await h.migrate()
    h.add_session("sess-1")
    await h.workspace_host.prepare_session(h.sessions["sess-1"])
    h.plant_legacy_checkpoint("sess-1", "cp_legacy_1", git_ref="abc123")

    rows = await h.host.list_checkpoints("sess-1")

    generations = {row["generation"] for row in rows}
    assert generations == {"workspace", "snapshot"}
    legacy = next(row for row in rows if row["generation"] == "snapshot")
    assert legacy["checkpoint_id"] == "cp_legacy_1"
    assert legacy["main_part_id"] == "prt_seed"
    workspace_rows = [row for row in rows if row["generation"] == "workspace"]
    assert any(row["trigger"] == "scope_start" for row in workspace_rows)


@pytest.mark.asyncio
async def test_legacy_snapshot_preview_reads_plugin_repo(tmp_path: Path) -> None:
    """快照代预览：从插件快照仓读目录与内容，隐藏内部 git 保留目录。"""
    h = PreviewHarness(tmp_path)
    await h.migrate()
    git_ref = h.build_snapshot_repo()
    h.add_session("sess-legacy")
    h.plant_legacy_checkpoint("sess-legacy", "cp_1", git_ref)

    entries = await h.host.list_checkpoint_files("sess-legacy", "cp_1")
    names = {e["name"] for e in entries}
    assert names == {"docs", "run.log"}
    docs = next(e for e in entries if e["name"] == "docs")
    assert docs["is_dir"] and docs["path"] == "docs"

    sub = await h.host.list_checkpoint_files("sess-legacy", "cp_1", "docs")
    assert [(e["name"], e["is_dir"]) for e in sub] == [("spec.md", False)]

    data = await h.host.read_checkpoint_file("sess-legacy", "cp_1", "docs/spec.md")
    assert data is not None
    assert data["content"] == "v1\n" and data["encoding"] == "utf-8"
    assert data["truncated"] is False
    missing = await h.host.read_checkpoint_file("sess-legacy", "cp_1", "nope.txt")
    assert missing is None

    # 他会话的检查点不命中（按会话核验）
    h.add_session("sess-other")
    assert await h.host.list_checkpoint_files("sess-other", "cp_1") == []


@pytest.mark.asyncio
async def test_legacy_preview_via_roster_and_managed_cwd(tmp_path: Path) -> None:
    """池外会话经持久化行解析 cwd；托管形态反推原目录寻址快照仓。"""
    h = PreviewHarness(tmp_path)
    await h.migrate()
    git_ref = h.build_snapshot_repo()
    h.plant_legacy_checkpoint("sess-roster", "cp_r", git_ref)
    h.plant_roster("sess-roster", h.original)

    data = await h.host.read_checkpoint_file("sess-roster", "cp_r", "docs/spec.md")
    assert data is not None and data["content"] == "v1\n"

    # cwd 指向托管工作区（崩溃/重开形态）也能反推原目录
    managed = h.original / ".workflow-controller" / "scopes" / "sess-managed" / "root"
    h.plant_roster("sess-managed", managed)
    h.plant_legacy_checkpoint("sess-managed", "cp_m", git_ref)
    data = await h.host.read_checkpoint_file("sess-managed", "cp_m", "run.log")
    assert data is not None and data["content"] == "log\n"


@pytest.mark.asyncio
async def test_workspace_generation_preview_reads_checkpoint_commit(tmp_path: Path) -> None:
    """工作区代预览：读私有仓检查点提交（非实时盘面），目录树与内容一致。"""
    h = PreviewHarness(tmp_path)
    await h.migrate()
    h.add_session("sess-ws")
    await h.workspace_host.prepare_session(h.sessions["sess-ws"])
    rows = await h.host.list_checkpoints("sess-ws")
    start = next(
        row for row in rows if row["generation"] == "workspace" and row["trigger"] == "scope_start"
    )
    managed = h.original / ".workflow-controller" / "scopes" / "sess-ws" / "root"
    # 实时盘面继续演进：检查点读到的仍是打点时刻内容
    (managed / "design" / "base.md").write_text("rewritten-live\n", encoding="utf-8")
    (managed / "design" / "live-only.md").write_text("fresh\n", encoding="utf-8")

    entries = await h.host.list_checkpoint_files("sess-ws", start["checkpoint_id"])
    names = {e["name"] for e in entries}
    assert "design" in names and "live-only.md" not in names
    design = next(e for e in entries if e["name"] == "design")
    assert design["is_dir"]

    data = await h.host.read_checkpoint_file("sess-ws", start["checkpoint_id"], "design/base.md")
    assert data is not None and data["content"] == "base\n"

    sub = await h.host.list_checkpoint_files("sess-ws", start["checkpoint_id"], "design")
    assert any(e["name"] == "base.md" and not e["is_dir"] for e in sub)


@pytest.mark.asyncio
async def test_unknown_checkpoint_yields_empty(tmp_path: Path) -> None:
    """未知检查点：预览面为空、文件读取为 None（查询面 404 由 API 层转）。"""
    h = PreviewHarness(tmp_path)
    await h.migrate()
    h.add_session("sess-x")
    assert await h.host.list_checkpoint_files("sess-x", "cp-none") == []
    assert await h.host.read_checkpoint_file("sess-x", "cp-none", "a.txt") is None
