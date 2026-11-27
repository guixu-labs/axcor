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

import workflow_controller.core.session_workspace as session_workspace
from workflow_controller.config.settings import SessionTracebackConfig
from workflow_controller.core.checkpoints import CheckpointStore
from workflow_controller.core.runs import RunStore
from workflow_controller.core.session_workspace import (
    SessionWorkspaceError,
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
        self.stopped: list[str] = []
        self.started: list[dict[str, Any]] = []
        self.transcript_sources: dict[str, Path] = {}
        # 置非空时假恢复入口抛该异常（模拟恢复会话创建失败）
        self.fail_start: Exception | None = None
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

    def reopened_host(self) -> SessionWorkspaceHost:
        """注册表丢失后的新宿主装配（模拟重开）。"""
        return SessionWorkspaceHost(
            checkpoint_db_path=lambda: self.db_path,
            resolve_session=self._resolve_session,
            persist_session=self._persist_session,
            stop_session=self._stop_session,
            start_resumed_session=self._start_resumed_session,
            transcript_locator=self._locate_transcript,
        )

    def restore_seed_dir(self, session_id: str, client_op_id: str) -> Path:
        """对话恢复种子的落点（树目录下按恢复操作号分目录）。"""
        return self.tree_dir(session_id) / "restore" / client_op_id

    async def migrate(self) -> None:
        """铺检查点表（统一迁移）。"""
        await RunStore(self.db_path).list_runs()

    async def _resolve_session(self, session_id: str) -> Any:
        return self.sessions.get(session_id)

    async def _persist_session(self, session: Any) -> None:
        """落盘端口替身（本套件不断言落盘调用）。"""

    async def _stop_session(self, session_id: str) -> None:
        self.stopped.append(session_id)

    async def _start_resumed_session(self, config: Any, **kwargs: Any) -> Any:
        """假恢复入口：登记启动参数并回一个新假会话。"""
        if self.fail_start is not None:
            raise self.fail_start
        new_id = f"restored-{len(self.started) + 1}"
        self.started.append({"session_id": new_id, "config": config, **kwargs})
        session = SimpleNamespace(id=new_id, config=config)
        self.sessions[new_id] = session
        return session

    def _locate_transcript(self, native_session_id: str, workspace_path: str) -> Path | None:
        return self.transcript_sources.get(native_session_id)

    async def writer_dispatch(self, session_id: str, runtime: str) -> Any:
        """提交一次 writer 子代理的 task 派发屏障，返回屏障结论。"""
        return await self.host.submit_dispatch_barrier(
            session_id,
            "dispatch",
            {
                "prompt_id": "p1",
                "tool_use_id": "tool-1",
                "native_session_id": "native-1",
                "tool_name": "Task",
                "tool_input": {"prompt": "sub"},
            },
            runtime=runtime,
        )

    async def side_effect(
        self, session_id: str, runtime: str, tool_use_id: str, message: str, **origin: str
    ) -> Any:
        """提交一次 Bash 副作用屏障（origin 可带子代理来源标识），返回屏障结论。"""
        return await self.host.submit_dispatch_barrier(
            session_id,
            "side_effect",
            {
                "prompt_id": message,
                "tool_use_id": tool_use_id,
                "native_session_id": "native-1",
                "tool_name": "Bash",
                "tool_input": {"command": "true"},
            },
            runtime=runtime,
            **origin,
        )

    async def group_checkpoint_row(self, session_id: str) -> dict[str, Any]:
        """取会话树里当前的组检查点行。"""
        rows = await self.host.list_checkpoints(session_id)
        return next(
            r
            for r in rows
            if r["trigger"] == "before_child"
            and (r["dispatch_anchor"] or {}).get("dispatch_group_id")
        )

    async def group_rows(self, session_id: str) -> list[tuple[str, str, str]]:
        """主写者作用域上的组检查点行：（行标识，工具调用号，组恢复点）。"""
        return [
            (r["checkpoint_id"], r["dispatch_anchor"]["tool_use_id"], r["workspace_commit"])
            for r in await self.host.list_checkpoints(session_id)
            if r["trigger"] == "before_child" and r["scope_id"] == f"{session_id}-main"
        ]

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


# 父会话 transcript：用户消息 + 含 task 调用的助手消息（回滚链的截断输入）
_PARENT_TRANSCRIPT = [
    {"type": "user", "uuid": "u1", "promptId": "p1", "message": {"content": "go"}},
    {
        "type": "assistant",
        "uuid": "a1",
        "promptId": "p1",
        "message": {"content": [{"type": "tool_use", "id": "tool-1"}]},
    },
]


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
    """崩溃重开：注册表丢失后从作用域账本表重建，工作区续用。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    managed = h.managed_dir("sess-1")
    (managed / "design" / "base.md").write_text("in-flight\n", encoding="utf-8")

    fresh_host = h.reopened_host()
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
    """崩溃窗（建区已落账、scope_start 检查点行未落）：重开清理半成品后重建主写者。"""
    import sqlite3

    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    entry = await h.host.prepare_session(session)
    # 砍掉主写者的 scope_start 行：账本行仍在（建区已落账），工作区/分支也仍在盘上
    conn = sqlite3.connect(h.db_path)
    conn.execute(
        "DELETE FROM scope_checkpoints WHERE scope_id = ?", (entry.tree.main_writer_scope_id,)
    )
    conn.commit()
    conn.close()

    fresh_host = h.reopened_host()
    await fresh_host.prepare_session(session)

    assert Path(session.config.cwd) == h.managed_dir("sess-1")
    assert (h.managed_dir("sess-1") / "design" / "base.md").exists()
    rows = [
        r for r in await fresh_host.list_checkpoints("sess-1") if r["scope_id"] == "sess-1-main"
    ]
    assert len(rows) == 1  # 重建后的新 scope_start 行


async def test_reopen_after_terminal_integration_rebuilds_main_writer(tmp_path, monkeypatch):
    """终态集成后、同步前崩溃重开：主写者按已集成重建，废弃后在当前头重建，收口照常。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    (h.managed_dir("sess-1") / "design" / "base.md").write_text("terminal\n", encoding="utf-8")

    async def crashing(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("崩溃注入：集成后、同步前")

    monkeypatch.setattr(session_workspace, "sync_terminal_to_original", crashing)
    with pytest.raises(RuntimeError, match="崩溃注入"):
        await h.host.finalize_session("sess-1")
    monkeypatch.undo()

    fresh_host = h.reopened_host()
    entry = await fresh_host.prepare_session(session)

    report = entry.tree.recovery_report
    assert report is not None and report.degraded == []  # 已集成的主写者不按活动重建
    writer = await entry.tree.manager.get_scope(entry.tree.main_writer_scope_id)
    assert (writer.state, writer.scope_epoch) == (ScopeState.ACTIVE, 2)  # 废弃换代后重建
    assert (h.managed_dir("sess-1") / "design" / "base.md").read_text(
        encoding="utf-8"
    ) == "terminal\n"
    # 再次收口：新代次的集成操作不与崩溃前的操作号相撞
    synced = await fresh_host.finalize_session("sess-1")
    assert not synced.refused
    assert (h.original / "design" / "base.md").read_text(encoding="utf-8") == "terminal\n"


async def test_barrier_builds_group_and_child_workspace(tmp_path, full_capability_runtime):
    """派发屏障：组检查点先于子代理、子工作区落 cwd、子会话映射回填。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    h.plant_transcript("native-1", _PARENT_TRANSCRIPT)

    outcome = await h.writer_dispatch("sess-1", full_capability_runtime)

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
    group_row = await h.group_checkpoint_row("sess-1")
    assert group_row["child_session_map"]


async def test_subagent_completion_integrates_into_main_writer(tmp_path, full_capability_runtime):
    """子代理完成：产出并入主写者工作区并随终态同步写回，子工作区释放，terminal 行落账。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    outcome = await h.writer_dispatch("sess-1", full_capability_runtime)
    child_cwd = Path(outcome.child_cwd)
    await h.host.record_subagent_start(
        "sess-1", {"parent_tool_use_id": "tool-1", "agent_id": "child-native-1"}
    )
    (child_cwd / "design" / "from-child.md").write_text("child\n", encoding="utf-8")

    await h.host.settle_subagent("sess-1", {"agent_id": "child-native-1"})

    assert (h.managed_dir("sess-1") / "design" / "from-child.md").read_text(
        encoding="utf-8"
    ) == "child\n"
    assert not child_cwd.exists()
    checkpoints = CheckpointStore(h.db_path)
    rows = await checkpoints.list_for_scope("sess-1-main-c1")
    checkpoints.close()
    assert [row.trigger.value for row in rows] == ["terminal"]
    assert rows[0].orchestration_state_ref == "session-ledger:sess-1"
    # 产出已在主写者分支上：终态同步把它写回原目录
    report = await h.host.finalize_session("sess-1")
    assert not report.refused
    assert (h.original / "design" / "from-child.md").read_text(encoding="utf-8") == "child\n"


async def test_subagent_side_effect_barrier_stays_out_of_main_writer_group(
    tmp_path, full_capability_runtime
):
    """子代理自身的副作用屏障放行：主写者作用域上不因子代理的调用多出组检查点行。

    主会话在同一消息内的下一次副作用仍入原组；子会话映射查不到时同样放行、不落行。
    """
    from workflow_controller.driver.hooks import BarrierProceed

    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    assert isinstance(await h.writer_dispatch("sess-1", full_capability_runtime), BarrierProceed)
    await h.host.record_subagent_start(
        "sess-1", {"parent_tool_use_id": "tool-1", "agent_id": "child-native-1"}
    )
    group_before = await h.group_rows("sess-1")
    assert len(group_before) == 1  # 派发时建的组

    # 子代理在自己的回合里做副作用：子会话映射已回填的与未回填的各两次
    for agent_id in ("child-native-1", "child-native-unmapped"):
        for tool_use_id in ("tool-c1", "tool-c2"):
            outcome = await h.side_effect(
                "sess-1",
                full_capability_runtime,
                tool_use_id,
                "child-turn",
                parent_tool_use_id="tool-1",
                agent_id=agent_id,
            )
            assert isinstance(outcome, BarrierProceed) and outcome.child_cwd is None

    assert await h.group_rows("sess-1") == group_before
    checkpoints = CheckpointStore(h.db_path)
    child_rows = await checkpoints.list_for_scope("sess-1-main-c1")
    checkpoints.close()
    assert child_rows == []  # 子作用域上也不落组检查点行

    # 主会话在同一消息内的下一次副作用仍入原组：不另建组，组恢复点不动
    parent_call = await h.side_effect("sess-1", full_capability_runtime, "tool-2", "p1")
    assert isinstance(parent_call, BarrierProceed)
    assert await h.group_rows("sess-1") == group_before


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


async def test_subagent_rollback_restores_group_and_binds_session(
    tmp_path, full_capability_runtime
):
    """子代理回滚：组检查点恢复 + 恢复会话绑定（宿主为确认方）。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    managed = h.managed_dir("sess-1")
    h.plant_transcript("native-1", _PARENT_TRANSCRIPT)
    await h.writer_dispatch("sess-1", full_capability_runtime)
    # 组建立后的副作用：主写者工作区被改
    (managed / "design" / "base.md").write_text("side effect\n", encoding="utf-8")
    (managed / "design" / "extra.md").write_text("extra\n", encoding="utf-8")

    group_row = await h.group_checkpoint_row("sess-1")
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


@pytest.mark.parametrize("failure", ["transcript_missing", "seed_write_fails"])
async def test_rollback_fails_before_reset_when_restore_data_unavailable(
    tmp_path, full_capability_runtime, request, failure
):
    """对话恢复数据备不好：回滚失败，文件态、代次、后代工作区原样未动，旧会话不停、树不留种子。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    entry = await h.host.prepare_session(session)
    if failure == "seed_write_fails":
        h.plant_transcript("native-1", _PARENT_TRANSCRIPT)
        request.getfixturevalue("restore_seed_write_fails")
    managed = h.managed_dir("sess-1")
    dispatch = await h.writer_dispatch("sess-1", full_capability_runtime)
    child_ws = Path(dispatch.child_cwd)
    (child_ws / "out.txt").write_text("child\n", encoding="utf-8")
    (managed / "design" / "base.md").write_text("after group\n", encoding="utf-8")
    group_row = await h.group_checkpoint_row("sess-1")

    with pytest.raises(Exception) as excinfo:
        await h.host.restore_session_subagent(
            "sess-1",
            checkpoint_id=group_row["checkpoint_id"],
            expected_epoch=1,
            client_op_id="rb-1",
        )

    # 破坏性动作没有发生：文件未回退、后代子工作区仍在、代次未变、旧会话没停
    assert (managed / "design" / "base.md").read_text(encoding="utf-8") == "after group\n"
    assert (child_ws / "out.txt").read_text(encoding="utf-8") == "child\n"
    for scope_id in (entry.tree.main_writer_scope_id, "sess-1-main-c1"):
        scope = await entry.tree.manager.get_scope(scope_id)
        assert (scope.scope_epoch, scope.state) == (1, ScopeState.ACTIVE)
    assert h.stopped == [] and h.started == []
    assert (await h.host.live_view("sess-1"))["state"] == "active"
    # 失败说明点名阶段，树目录里不留种子
    assert isinstance(excinfo.value, SessionWorkspaceError)
    assert "对话恢复数据准备失败" in str(excinfo.value)
    assert not h.restore_seed_dir("sess-1", "rb-1").exists()


async def test_rollback_failure_after_reset_reports_stage_and_discards_seed(
    tmp_path, full_capability_runtime
):
    """文件已恢复后恢复会话创建失败：报错点名阶段，种子回收，树回到运行态。"""
    h = Harness(tmp_path)
    await h.migrate()
    session = h.add_session("sess-1")
    await h.host.prepare_session(session)
    h.plant_transcript("native-1", _PARENT_TRANSCRIPT)
    managed = h.managed_dir("sess-1")
    await h.writer_dispatch("sess-1", full_capability_runtime)
    (managed / "design" / "base.md").write_text("after group\n", encoding="utf-8")
    group_row = await h.group_checkpoint_row("sess-1")
    h.fail_start = RuntimeError("运行时启动失败")

    with pytest.raises(SessionWorkspaceError, match="文件已恢复到组检查点、旧会话已停") as excinfo:
        await h.host.restore_session_subagent(
            "sess-1",
            checkpoint_id=group_row["checkpoint_id"],
            expected_epoch=1,
            client_op_id="rb-1",
        )

    assert "运行时启动失败" in str(excinfo.value)
    assert (managed / "design" / "base.md").read_text(encoding="utf-8") == "base\n"
    assert h.stopped == ["sess-1"] and h.started == []
    assert (await h.host.live_view("sess-1"))["state"] == "active"
    assert not h.restore_seed_dir("sess-1", "rb-1").exists()


async def test_live_view_missing_session_rejected(tmp_path):
    """未接入会话的实时视图按查询面 404 拒绝。"""
    h = Harness(tmp_path)
    with pytest.raises(SessionWorkspaceNotFoundError):
        await h.host.live_view("nobody")


async def test_workspace_mode_only(tmp_path):
    """snapshot 兼容模式已随旧插件退役，内核通道显式拒绝。"""
    from workflow_controller.core.session_workspace import SessionWorkspaceError

    h = Harness(tmp_path)
    session = h.add_session("sess-1", traceback=SessionTracebackConfig(mode="snapshot_compatible"))
    with pytest.raises(SessionWorkspaceError):
        await h.host.prepare_session(session)
    assert not h.tree_dir("sess-1").exists()
