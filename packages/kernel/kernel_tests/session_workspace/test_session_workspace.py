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

"""单会话工作区宿主测试：接入、D5 终态同步、实时视图与子代理回滚。"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from workflow_controller.config.settings import SessionTracebackConfig
from workflow_controller.core.runs import RunStore
from workflow_controller.core.session_workspace import (
    SessionWorkspaceHost,
    SessionWorkspaceNotFoundError,
)
from workflow_controller.core.workspace.scope_tree import ScopeState

pytestmark = pytest.mark.asyncio


class Harness:
    """单会话宿主装配：真实 git/检查点库 + 假会话与假恢复入口。"""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.db_path = tmp_path / "events.db"
        self.original = tmp_path / "proj"
        self._make_original()
        self.sessions: dict[str, Any] = {}
        self.persisted: list[str] = []
        self.stopped: list[str] = []
        self.started: list[dict[str, Any]] = []
        self.transcript_sources: dict[str, Path] = {}
        self.host = SessionWorkspaceHost(
            checkpoint_db_path=lambda: self.db_path,
            resolve_session=self._resolve_session,
            persist_session=self._persist_session,
            stop_session=self._stop_session,
            start_resumed_session=self._start_resumed_session,
            transcript_locator=self._locate_transcript,
        )

    def _make_original(self) -> None:
        """用户原目录现场：源码、嵌套日志与构建产物。"""
        self.original.mkdir(parents=True)
        (self.original / "design").mkdir()
        (self.original / "design" / "base.md").write_text("base\n", encoding="utf-8")
        (self.original / "run.log").write_text("root-log\n", encoding="utf-8")
        (self.original / "op" / "build").mkdir(parents=True)
        (self.original / "op" / "build" / "artifact.bin").write_bytes(b"\x00artifact")

    def add_session(
        self,
        session_id: str,
        *,
        traceback: SessionTracebackConfig | None = None,
        runtime: str = "claude_sdk",
        run_id: str | None = "run-implicit",
    ) -> Any:
        """登记一个假会话（cwd 指原目录）。"""
        session = SimpleNamespace(
            id=session_id,
            config=SimpleNamespace(
                cwd=str(self.original),
                traceback=traceback if traceback is not None else _traceback_on(),
                runtime=runtime,
                run_id=run_id,
            ),
        )
        self.sessions[session_id] = session
        return session

    def managed_dir(self, session_id: str) -> Path:
        """会话托管工作区固定落点。"""
        return self.original / ".workflow-controller" / "scopes" / session_id / "root"

    def tree_dir(self, session_id: str) -> Path:
        """会话树目录。"""
        return self.original / ".workflow-controller" / "scopes" / session_id

    async def migrate(self) -> None:
        """铺检查点表（统一迁移）。"""
        await RunStore(self.db_path).list_runs()

    async def _resolve_session(self, session_id: str) -> Any:
        return self.sessions.get(session_id)

    async def _persist_session(self, session: Any) -> None:
        self.persisted.append(str(session.id))

    async def _stop_session(self, session_id: str) -> None:
        self.stopped.append(session_id)

    async def _start_resumed_session(self, config: Any, **kwargs: Any) -> Any:
        """假恢复入口：登记启动参数并回一个新假会话。"""
        new_id = f"restored-{len(self.started) + 1}"
        self.started.append({"session_id": new_id, "config": config, **kwargs})
        session = SimpleNamespace(id=new_id, config=config)
        self.sessions[new_id] = session
        return session

    def _locate_transcript(self, native_session_id: str, workspace_path: str) -> Path | None:
        return self.transcript_sources.get(native_session_id)

    def plant_transcript(self, native_session_id: str, rows: list[dict[str, Any]]) -> None:
        """铺父会话 transcript 源（回滚链的截断输入）。"""
        source = self.tmp / "transcripts" / f"{native_session_id}.jsonl"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(
            "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8"
        )
        self.transcript_sources[native_session_id] = source


def _traceback_on() -> SessionTracebackConfig:
    """默认开启的单会话 traceback 配置。"""
    return SessionTracebackConfig()


async def test_prepare_points_cwd_to_managed_workspace(tmp_path):
    """接入：cwd 指向 root/ 终态检出、原目录保持基线、scope_start 落账。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)

    managed = h.managed_dir("sess-1")
    assert Path(session.config.cwd) == managed
    assert (managed / "design" / "base.md").read_text(encoding="utf-8") == "base\n"
    # 自建路径从 seed 物化（含 *.log）
    assert (managed / "run.log").read_text(encoding="utf-8") == "root-log\n"
    # 原目录保持基线：无托管内容混入
    assert (h.original / "design" / "base.md").read_text(encoding="utf-8") == "base\n"
    assert not (h.original / "design" / "base.md").read_text(encoding="utf-8").startswith("from")
    rows = await h.host.list_checkpoints("sess-1")
    starts = [r for r in rows if r["trigger"] == "scope_start" and r["scope_id"] == "sess-1-main"]
    assert len(starts) == 1 and starts[0]["workspace_commit"]
    assert h.host.owns("sess-1")


async def test_run_period_original_stays_baseline_live_view_tracks(tmp_path):
    """运行期：原目录基线不变；live view 反映托管工作区当前态。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    managed = h.managed_dir("sess-1")
    (managed / "design" / "base.md").write_text("rewritten\n", encoding="utf-8")
    (managed / "design" / "new.md").write_text("fresh\n", encoding="utf-8")

    assert (h.original / "design" / "base.md").read_text(encoding="utf-8") == "base\n"
    assert not (h.original / "design" / "new.md").exists()
    view = await h.host.live_view("sess-1")
    assert view["state"] == "active"
    paths = {e["path"]: e for e in view["entries"]}
    assert paths["design/new.md"]["type"] == "file"
    assert paths["design/base.md"]["size"] == len("rewritten\n")
    file_view = await h.host.live_view("sess-1", "design/new.md")
    assert file_view["content"] == "fresh\n" and file_view["encoding"] == "utf-8"


async def test_finalize_syncs_terminal_and_prunes_tree(tmp_path):
    """终态：三方同步写回（含删除生效）后整树回收，幂等。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    managed = h.managed_dir("sess-1")
    (managed / "design" / "base.md").write_text("terminal\n", encoding="utf-8")
    (managed / "run.log").unlink()
    (managed / "design" / "new.md").write_text("fresh\n", encoding="utf-8")

    report = await h.host.finalize_session("sess-1")

    assert not report.refused
    assert (h.original / "design" / "base.md").read_text(encoding="utf-8") == "terminal\n"
    assert not (h.original / "run.log").exists()
    assert (h.original / "design" / "new.md").read_text(encoding="utf-8") == "fresh\n"
    assert not h.tree_dir("sess-1").exists()
    view = await h.host.live_view("sess-1")
    assert view["state"] == "synced"
    assert view["final_sync"]["removed"] == ["run.log"]
    # 幂等：重复收口返回原结论
    again = await h.host.finalize_session("sess-1")
    assert again == report


async def test_finalize_conflict_retains_workspace(tmp_path):
    """外部变化：停止覆盖、冲突路径报告、托管终态保留可查。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    managed = h.managed_dir("sess-1")
    (managed / "design" / "base.md").write_text("terminal\n", encoding="utf-8")
    (h.original / "design" / "base.md").write_text("user edit\n", encoding="utf-8")

    report = await h.host.finalize_session("sess-1")

    assert report.refused
    assert report.conflict_paths == ("design/base.md",)
    assert (h.original / "design" / "base.md").read_text(encoding="utf-8") == "user edit\n"
    assert h.tree_dir("sess-1").exists()  # 托管终态保留
    assert (managed / "design" / "base.md").read_text(encoding="utf-8") == "terminal\n"
    view = await h.host.live_view("sess-1")
    assert view["state"] == "retained"
    assert view["final_sync"]["conflict_paths"] == ["design/base.md"]

    # 会话续用保留树：重激活后回到运行态，外部变化解除即收口成功
    await h.host.prepare_session(h.sessions["sess-1"])
    (h.original / "design" / "base.md").write_text("base\n", encoding="utf-8")
    retry = await h.host.finalize_session("sess-1")
    assert not retry.refused
    assert (h.original / "design" / "base.md").read_text(encoding="utf-8") == "terminal\n"
    assert not h.tree_dir("sess-1").exists()


async def test_baseline_external_addition_survives_sync(tmp_path):
    """基线外用户新增文件：同步不误删。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    (h.original / "user-extra.txt").write_text("keep\n", encoding="utf-8")

    report = await h.host.finalize_session("sess-1")

    assert not report.refused
    assert (h.original / "user-extra.txt").read_text(encoding="utf-8") == "keep\n"


async def test_crash_reopen_recovers_tree(tmp_path):
    """崩溃重开：注册表丢失后按检查点账与私有仓事实重建，工作区续用。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    managed = h.managed_dir("sess-1")
    (managed / "design" / "base.md").write_text("in-flight\n", encoding="utf-8")

    fresh_host = SessionWorkspaceHost(
        checkpoint_db_path=lambda: h.db_path,
        resolve_session=h._resolve_session,
        persist_session=h._persist_session,
        stop_session=h._stop_session,
        start_resumed_session=h._start_resumed_session,
        transcript_locator=h._locate_transcript,
    )
    await fresh_host.prepare_session(session)

    assert Path(session.config.cwd) == managed
    assert (managed / "design" / "base.md").read_text(encoding="utf-8") == "in-flight\n"
    rows = [
        r for r in await fresh_host.list_checkpoints("sess-1") if r["scope_id"] == "sess-1-main"
    ]
    assert rows
    # 重建后终态同步照常收口
    report = await fresh_host.finalize_session("sess-1")
    assert not report.refused
    assert (h.original / "design" / "base.md").read_text(encoding="utf-8") == "in-flight\n"


async def test_crash_window_between_prepare_and_checkpoint_row(tmp_path):
    """崩溃窗（建区已落盘、检查点行未落）：重开清理半成品后重建主写者。"""
    import sqlite3

    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    entry = await h.host.prepare_session(session)
    # 砍掉主写者检查点行：恢复面看不到该作用域，但工作区/分支仍在盘上
    conn = sqlite3.connect(h.db_path)
    conn.execute(
        "DELETE FROM scope_checkpoints WHERE scope_id = ?", (entry.tree.main_writer_scope_id,)
    )
    conn.commit()
    conn.close()

    fresh_host = SessionWorkspaceHost(
        checkpoint_db_path=lambda: h.db_path,
        resolve_session=h._resolve_session,
        persist_session=h._persist_session,
        stop_session=h._stop_session,
        start_resumed_session=h._start_resumed_session,
        transcript_locator=h._locate_transcript,
    )
    await fresh_host.prepare_session(session)

    assert Path(session.config.cwd) == h.managed_dir("sess-1")
    assert (h.managed_dir("sess-1") / "design" / "base.md").exists()
    rows = [
        r for r in await fresh_host.list_checkpoints("sess-1") if r["scope_id"] == "sess-1-main"
    ]
    assert len(rows) == 1  # 重建后的新 scope_start 行


async def test_barrier_builds_group_and_child_workspace(tmp_path):
    """派发屏障：组检查点先于子代理、子工作区落 cwd、子会话映射回填。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    h.plant_transcript(
        "native-1",
        [
            {
                "type": "user",
                "uuid": "u1",
                "promptId": "p1",
                "message": {"content": "go"},
            },
            {
                "type": "assistant",
                "uuid": "a1",
                "promptId": "p1",
                "message": {"content": [{"type": "tool_use", "id": "tool-1"}]},
            },
        ],
    )

    outcome = await h.host.submit_dispatch_barrier(
        "sess-1",
        "dispatch",
        {
            "prompt_id": "p1",
            "tool_use_id": "tool-1",
            "native_session_id": "native-1",
            "tool_name": "Task",
            "tool_input": {"prompt": "sub"},
        },
        runtime="claude_sdk",
    )

    from workflow_controller.driver.hooks import BarrierProceed

    assert isinstance(outcome, BarrierProceed)
    assert outcome.child_cwd and Path(outcome.child_cwd).is_dir()
    rows = await h.host.list_checkpoints("sess-1")
    groups = [
        r
        for r in rows
        if r["trigger"] == "before_child" and (r["dispatch_anchor"] or {}).get("dispatch_group_id")
    ]
    assert len(groups) == 1
    await h.host.record_subagent_start(
        "sess-1", {"parent_tool_use_id": "tool-1", "agent_id": "child-native-1"}
    )
    refreshed = await h.host.list_checkpoints("sess-1")
    group_row = next(
        r
        for r in refreshed
        if r["trigger"] == "before_child" and (r["dispatch_anchor"] or {}).get("dispatch_group_id")
    )
    assert group_row["child_session_map"]


async def test_strict_barrier_denies_opencode_writer(tmp_path):
    """严格档 + opencode（无 cwd 注入）：writer 子代理派发被拒。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)

    outcome = await h.host.submit_dispatch_barrier(
        "sess-1",
        "dispatch",
        {"prompt_id": "p1", "tool_use_id": "tool-9", "native_session_id": "native-1"},
        runtime="opencode",
    )

    from workflow_controller.driver.hooks import BarrierDeny

    assert isinstance(outcome, BarrierDeny)


async def test_subagent_rollback_restores_group_and_binds_session(tmp_path):
    """子代理回滚：组检查点恢复 + 恢复会话绑定（宿主为确认方）。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    managed = h.managed_dir("sess-1")
    h.plant_transcript(
        "native-1",
        [
            {"type": "user", "uuid": "u1", "promptId": "p1", "message": {"content": "go"}},
            {
                "type": "assistant",
                "uuid": "a1",
                "promptId": "p1",
                "message": {"content": [{"type": "tool_use", "id": "tool-1"}]},
            },
        ],
    )
    await h.host.submit_dispatch_barrier(
        "sess-1",
        "dispatch",
        {
            "prompt_id": "p1",
            "tool_use_id": "tool-1",
            "native_session_id": "native-1",
            "tool_name": "Task",
            "tool_input": {"prompt": "sub"},
        },
        runtime="claude_sdk",
    )
    # 组建立后的副作用：主写者工作区被改
    (managed / "design" / "base.md").write_text("side effect\n", encoding="utf-8")
    (managed / "design" / "extra.md").write_text("extra\n", encoding="utf-8")

    rows = await h.host.list_checkpoints("sess-1")
    group_row = next(
        r
        for r in rows
        if r["trigger"] == "before_child" and (r["dispatch_anchor"] or {}).get("dispatch_group_id")
    )
    result = await h.host.restore_session_subagent(
        "sess-1",
        checkpoint_id=group_row["checkpoint_id"],
        expected_epoch=1,
        client_op_id="rb-1",
    )

    # 文件恢复到组检查点（副作用撤回）
    assert (managed / "design" / "base.md").read_text(encoding="utf-8") == "base\n"
    assert not (managed / "design" / "extra.md").exists()
    # 定序：旧会话先停、恢复会话后建并接续同一棵树
    assert h.stopped == ["sess-1"]
    assert len(h.started) == 1
    assert result["restored_session_id"] == h.started[0]["session_id"]
    assert Path(h.started[0]["config"].cwd) == managed
    assert h.started[0]["config"].traceback is not None
    assert h.host.owns(result["restored_session_id"])
    view = await h.host.live_view(result["restored_session_id"])
    assert view["state"] == "active"
    # 恢复会话仍可终态收口（别名解析到同一棵树）
    report = await h.host.finalize_session(result["restored_session_id"])
    assert not report.refused


async def test_live_view_missing_session_rejected(tmp_path):
    """未接入会话的实时视图按查询面 404 拒绝。"""
    h = Harness(tmp_path)
    with pytest.raises(SessionWorkspaceNotFoundError):
        await h.host.live_view("nobody")


async def test_workspace_mode_only(tmp_path):
    """snapshot 兼容模式在内核通道显式拒绝（由插件承载）。"""
    from workflow_controller.core.session_workspace import SessionWorkspaceError

    h = Harness(tmp_path)
    session = h.add_session("sess-1", traceback=SessionTracebackConfig(mode="snapshot_compatible"))
    with pytest.raises(SessionWorkspaceError):
        await h.host.prepare_session(session)
    assert not h.tree_dir("sess-1").exists()


async def test_scope_state_vocab(tmp_path):
    """词面在场断言：作用域状态枚举（防误删导入）。"""
    assert ScopeState.ACTIVE
