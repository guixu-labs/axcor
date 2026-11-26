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

"""子代理回滚链集成测试：真实 git + 探针驱动 + 可绑定会话的宿主替身。

覆盖：组检查点恢复撤回整组副作用（#23/#24）、恢复会话绑定四元组、
RestoreCommitted 前不创建会话的定序崩溃注入、多副作用组 replay 拒绝
（#35）、reader 子代理 detached 快照与永不集成、子会话映射回填。
"""

from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from workflow_controller.core.run_coordinator import RunConfig, RunCoordinator
from workflow_controller.core.runs import RunStore
from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService
from workflow_controller.core.workspace.dispatch_groups import ReplayRejectedError
from workflow_controller.plugin.run_driver import EmitEvent, RunAction, TimerFired
from workflow_controller.plugin.workspace import (
    IntegrateScope,
    PrepareScopeWorkspace,
    RestoreCommitted,
    RestoreScope,
    RestoreSessionBound,
    ScopeIntegrated,
    ScopeRestored,
    WorkspaceActionFailed,
    WorkspaceTreeConfig,
)

from ._run_plane_stubs import StubBus
from .test_workspace_bridge import ProbeDriver, WorkspaceSO

_FINISH: dict[str, bool] = {}


class RestoreSO(WorkspaceSO):
    """支持恢复会话创建的编排器替身：记录调用并返回带 id 的会话对象。"""

    def __init__(self, project_dir: Path) -> None:
        super().__init__(project_dir)
        self.resumed: list[dict[str, Any]] = []
        self._next = 0

    async def start_session_with_resume(self, config: Any, **kwargs: Any) -> Any:
        self._next += 1
        session = type("S", (), {"id": f"ses-restore-{self._next}"})()
        self.resumed.append({"config": config, **kwargs})
        return session


class _Factory:
    """固定驱动实例的工厂。"""

    def __init__(self, driver: ProbeDriver) -> None:
        self.driver = driver

    @property
    def format(self) -> str:
        return "probe@1"

    def create(self, run: Any, program_path: Any, params: Any) -> ProbeDriver:
        return self.driver

    def restore(self, run: Any, snapshot: Any, sessions: Any) -> ProbeDriver:
        return self.driver


class OpChainDriver(ProbeDriver):
    """支持 handle_op 的链探针：留档所见 op 并按既定表应答。"""

    def __init__(self, plan: Any) -> None:
        super().__init__(plan)
        self.ops: list[tuple[str, dict[str, Any]]] = []

    def handle_op(self, op: str, args: dict[str, Any]) -> dict[str, Any]:
        self.ops.append((op, dict(args)))
        return {"operation_id": args.get("client_op_id"), "phase": "pending"}


class ChainHarness:
    """run 平面装配：真实协调器 + 真实 git + 阶段化探针计划。"""

    def __init__(self, tmp_path: Path, transcript: Path | None = None) -> None:
        self.work_root = tmp_path / "root"
        (self.work_root / "design").mkdir(parents=True)
        (self.work_root / "design" / "base.md").write_text("base\n", encoding="utf-8")
        self.store = RunStore(tmp_path / "events.db")
        self.affiliation = SessionRunAffiliationService(self.store)
        self.so = RestoreSO(tmp_path)
        self.driver = OpChainDriver(self._plan)
        locator = None
        if transcript is not None:

            def locator(native_session_id: str, workspace_path: str) -> Path | None:  # type: ignore[no-redef]
                return transcript if native_session_id == "ses-native-exec" else None

        self.coord = RunCoordinator(
            orchestrator=self.so,
            run_store=self.store,
            bus=StubBus(),
            driver_factories={"probe@1": _Factory(self.driver)},
            affiliation=self.affiliation,
            workspace_transcript_locator=locator,
        )
        self.program = tmp_path / "prog.txt"
        self.program.write_text("probe", encoding="utf-8")
        self.pending: RunAction | None = None

    def _plan(self, driver: ProbeDriver) -> list[RunAction] | None:
        if _FINISH.get(id(self)):
            return None
        if self.pending is not None:
            action, self.pending = self.pending, None
            return [action]
        if not any(type(n).__name__ == "ScopeWorkspacePrepared" for n in driver.notes):
            return [
                PrepareScopeWorkspace(
                    config=WorkspaceTreeConfig(root_dir=str(self.work_root)),
                    scope_key="node-a",
                    client_op_id="prepare-1",
                    owner_ref="a",
                    context="node=a",
                )
            ]
        return [EmitEvent("hold", {})]

    async def start(self) -> str:
        """启动 run 并等待节点作用域建立，挂执行会话归属。"""
        run = await self.coord.start_run(RunConfig(format="probe@1"), self.program)
        self.run_id = run.run_id
        _FINISH[id(self)] = False
        deadline = asyncio.get_running_loop().time() + 30.0
        while asyncio.get_running_loop().time() < deadline:
            scopes = await self.coord.list_run_scopes(self.run_id)
            if any(s["scope_id"] == "node-a" for s in scopes):
                break
            await asyncio.sleep(0.02)
        else:
            raise AssertionError("节点作用域未在限期内建立")
        await self.affiliation.attach("ses-exec", self.run_id, role="executor", context="node=a")
        return self.run_id

    async def send(self, action: RunAction) -> None:
        """排一个动作给探针计划并注入一轮通知驱动 decide。"""
        self.pending = action
        self.coord.notify_run(self.run_id, TimerFired(timer_id="poke"))

    async def wait_note(self, kind: type) -> Any:
        """等待指定类型回执到达驱动。"""
        deadline = asyncio.get_running_loop().time() + 30.0
        while asyncio.get_running_loop().time() < deadline:
            for note in self.driver.notes:
                if isinstance(note, kind):
                    return note
            await asyncio.sleep(0.02)
        raise AssertionError(f"回执超时：{kind.__name__}")

    async def wait_scope_epoch(self, scope_id: str, epoch: int) -> None:
        """等待作用域账面达到指定代次。"""
        deadline = asyncio.get_running_loop().time() + 30.0
        while asyncio.get_running_loop().time() < deadline:
            scopes = await self.coord.list_run_scopes(self.run_id)
            row = next((s for s in scopes if s["scope_id"] == scope_id), None)
            if row and row["scope_epoch"] == epoch:
                return
            await asyncio.sleep(0.02)
        raise AssertionError(f"作用域代次超时：{scope_id} e{epoch}")

    async def finish(self) -> None:
        """收口 run 并等待终态。"""
        _FINISH[id(self)] = True
        self.coord.notify_run(self.run_id, TimerFired(timer_id="test-finish"))
        deadline = asyncio.get_running_loop().time() + 30.0
        while asyncio.get_running_loop().time() < deadline:
            record = await self.store.get(self.run_id)
            if record.finished_at is not None:
                return
            await asyncio.sleep(0.02)
        raise AssertionError("run 未在限期内落终态")

    async def barrier(
        self,
        kind: str,
        tool_use_id: str,
        *,
        workspace: str = "writer",
        tool_name: str = "Task",
        tool_input: dict[str, Any] | None = None,
    ) -> Any:
        """提交一次屏障请求。"""
        return await self.coord.submit_dispatch_barrier(
            "ses-exec",
            kind,
            {
                "tool_name": tool_name,
                "tool_input": tool_input or {"prompt": "do work"},
                "tool_use_id": tool_use_id,
                "native_session_id": "ses-native-exec",
                "prompt_id": "msg-1",
            },
            runtime="claude_sdk",
            workspace=workspace,
        )

    async def group_checkpoint_id(self) -> str:
        """取当前活动派发组的组检查点标识。"""
        rows = await self.coord.list_run_checkpoints(self.run_id, scope_id="node-a")
        for row in rows:
            if row["trigger"] != "before_child":
                continue
            group = (row["dispatch_anchor"] or {}).get("dispatch_group_id") or ""
            if group and row["checkpoint_id"].endswith(group):
                return row["checkpoint_id"]
        raise AssertionError("未找到组检查点行")


def _write_transcript(tmp_path: Path) -> Path:
    """合成父会话 transcript（用户消息 + 含 task 调用的助手消息）。"""
    source = tmp_path / "claude-src" / "ses-native-exec.jsonl"
    source.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"type": "user", "uuid": "msg-user", "promptId": "msg-1", "message": {"content": []}},
        {
            "type": "assistant",
            "uuid": "msg-b",
            "promptId": "msg-1",
            "message": {
                "content": [
                    {"type": "text", "text": "我将调用子代理"},
                    {"type": "tool_use", "id": "toolu-1", "name": "Task"},
                ]
            },
        },
    ]
    with source.open("w", encoding="utf-8") as out:
        for row in rows:
            out.write(json.dumps(row) + "\n")
    return source


async def _node_workspace_path(h: ChainHarness) -> Path:
    """节点作用域工作区路径（账面查询）。"""
    scopes = await h.coord.list_run_scopes(h.run_id)
    row = next(s for s in scopes if s["scope_id"] == "node-a")
    return Path(row["workspace_path"])


async def test_group_rollback_withdraws_integrated_sibling_changes(tmp_path: Path) -> None:
    """#23 核心：组内 task A 已集成、回滚 task B 时 A 的修改随组检查点一并撤回。"""
    h = ChainHarness(tmp_path, transcript=_write_transcript(tmp_path))
    await h.start()
    try:
        outcome_a = await h.barrier("dispatch", "toolu-1")
        assert outcome_a.child_cwd
        (Path(outcome_a.child_cwd) / "out").mkdir(exist_ok=True)
        (Path(outcome_a.child_cwd) / "out" / "a.txt").write_text("A\n", encoding="utf-8")
        await h.send(IntegrateScope(scope_id="node-a-c1", client_op_id="int-a", context="node=a"))
        await h.wait_note(ScopeIntegrated)
        node_ws = await _node_workspace_path(h)
        assert (node_ws / "out" / "a.txt").read_text(encoding="utf-8") == "A\n"

        outcome_b = await h.barrier("dispatch", "toolu-2")
        assert outcome_b.child_cwd
        (Path(outcome_b.child_cwd) / "out").mkdir(exist_ok=True)
        (Path(outcome_b.child_cwd) / "out" / "b.txt").write_text("B\n", encoding="utf-8")

        checkpoint_id = await h.group_checkpoint_id()
        await h.send(
            RestoreScope(
                scope_id="node-a",
                checkpoint_id=checkpoint_id,
                expected_scope_epoch=1,
                client_op_id="rs-1",
                session_runtime="claude_sdk",
                context="node=a",
            )
        )
        restored = await h.wait_note(ScopeRestored)
        assert restored.new_scope_epoch == 2
        # 失效范围 = 组检查点之后全部：A 的集成修改与父检出变化一并撤回
        assert not (node_ws / "out" / "a.txt").exists()
        assert (node_ws / "design" / "base.md").read_text(encoding="utf-8") == "base\n"
        # 后代子代理工作区废弃：B 的工作区已移除、账面各自换代
        assert not Path(outcome_b.child_cwd).exists()
        await h.wait_scope_epoch("node-a-c2", 2)
        scopes = {s["scope_id"]: s for s in await h.coord.list_run_scopes(h.run_id)}
        assert scopes["node-a"]["scope_epoch"] == 2
        assert scopes["node-a"]["state"] == "active"
        assert scopes["node-a-c1"]["state"] == "discarded"
        assert scopes["node-a-c2"]["state"] == "discarded"
        # 定序保证：RestoreCommitted 之前不得创建恢复会话
        assert h.so.resumed == []
    finally:
        await h.finish()


async def test_restore_session_bound_to_run_scope_new_epoch(tmp_path: Path) -> None:
    """恢复会话绑定原 run/作用域/新 epoch，cwd 指向父作用域工作区。"""
    h = ChainHarness(tmp_path, transcript=_write_transcript(tmp_path))
    await h.start()
    try:
        await h.barrier("dispatch", "toolu-1")
        checkpoint_id = await h.group_checkpoint_id()
        node_ws = await _node_workspace_path(h)
        await h.send(
            RestoreScope(
                scope_id="node-a",
                checkpoint_id=checkpoint_id,
                expected_scope_epoch=1,
                client_op_id="rs-1",
                session_runtime="claude_sdk",
                context="node=a",
            )
        )
        await h.wait_note(ScopeRestored)
        await h.send(
            RestoreCommitted(
                scope_id="node-a",
                client_op_id="op-restore",
                context="node=a",
                restore_binding=True,
            )
        )
        bound = await h.wait_note(RestoreSessionBound)
        assert bound.session_id == "ses-restore-1"
        created = h.so.resumed[0]
        assert created["config"].cwd == str(node_ws)
        assert created["config"].run_id == h.run_id
        assert created["config"].initial_prompt == "请继续你之前的工作。"
        assert created["config"].runtime == "claude_sdk"
        assert created["resume_sdk_session_id"]
        assert created["plugin_metadata"]["workspace_restore"] == {
            "run_id": h.run_id,
            "scope_id": "node-a",
            "scope_epoch": 2,
            "checkpoint_id": checkpoint_id,
        }
        aff = h.affiliation.resolve("ses-restore-1")
        assert aff is not None and aff.run_id == h.run_id and aff.context == "node=a"
    finally:
        await h.finish()


async def test_crash_before_restore_commit_creates_no_session(tmp_path: Path) -> None:
    """定序崩溃注入：RestoreCommitted 前链路停摆，不创建新 session。"""
    h = ChainHarness(tmp_path, transcript=_write_transcript(tmp_path))
    await h.start()
    try:
        await h.barrier("dispatch", "toolu-1")
        checkpoint_id = await h.group_checkpoint_id()
        await h.send(
            RestoreScope(
                scope_id="node-a",
                checkpoint_id=checkpoint_id,
                expected_scope_epoch=1,
                client_op_id="rs-1",
                context="node=a",
            )
        )
        await h.wait_note(ScopeRestored)
        # 崩溃注入：不发送 RestoreCommitted，等待一个观察窗
        await asyncio.sleep(0.3)
        assert h.so.resumed == []
        # 恢复推进后绑定才发生
        await h.send(
            RestoreCommitted(
                scope_id="node-a",
                client_op_id="op-restore",
                context="node=a",
                restore_binding=True,
            )
        )
        await _wait_resumed(h, expected=1)
    finally:
        await h.finish()


async def test_write_tool_side_effect_withdrawn_and_not_reproduced(tmp_path: Path) -> None:
    """#24：组内写文件工具的修改随组撤回，恢复后不重复产生。"""
    h = ChainHarness(tmp_path, transcript=_write_transcript(tmp_path))
    await h.start()
    try:
        write_outcome = await h.barrier(
            "side_effect",
            "toolu-w1",
            tool_name="Write",
            tool_input={"file_path": "design/written.md", "content": "x"},
        )
        assert write_outcome.child_cwd is None
        node_ws = await _node_workspace_path(h)
        (node_ws / "design" / "written.md").write_text("x\n", encoding="utf-8")
        await h.barrier("dispatch", "toolu-1")

        checkpoint_id = await h.group_checkpoint_id()
        await h.send(
            RestoreScope(
                scope_id="node-a",
                checkpoint_id=checkpoint_id,
                expected_scope_epoch=1,
                client_op_id="rs-1",
                context="node=a",
            )
        )
        await h.wait_note(ScopeRestored)
        # 组检查点先于写副作用：文件随组撤回，恢复数据不重放写工具调用
        assert not (node_ws / "design" / "written.md").exists()
        assert (node_ws / "design" / "base.md").exists()
    finally:
        await h.finish()


async def test_multi_side_effect_group_rejects_replay_dispatch(tmp_path: Path) -> None:
    """#35：多副作用组（写工具 + task）的 replay_dispatch 请求被明确拒绝。"""
    h = ChainHarness(tmp_path, transcript=_write_transcript(tmp_path))
    await h.start()
    try:
        await h.barrier(
            "side_effect",
            "toolu-w1",
            tool_name="Write",
            tool_input={"file_path": "design/written.md", "content": "x"},
        )
        await h.barrier("dispatch", "toolu-1")
        checkpoint_id = await h.group_checkpoint_id()
        with pytest.raises(ReplayRejectedError, match="replay_dispatch 仅允许单 task"):
            await h.coord.validate_dispatch_replay(h.run_id, checkpoint_id)
        # 恢复动作携带 replay 标志同样在宿主侧被拒绝（权威校验）
        await h.send(
            RestoreScope(
                scope_id="node-a",
                checkpoint_id=checkpoint_id,
                expected_scope_epoch=1,
                client_op_id="rs-1",
                replay_dispatch=True,
                context="node=a",
            )
        )
        failed = await h.wait_note(WorkspaceActionFailed)
        assert "replay_dispatch" in failed.error
        assert h.so.resumed == []
    finally:
        await h.finish()


async def test_single_task_group_allows_replay_signal(tmp_path: Path) -> None:
    """单 task 组：replay 请求通过校验，首轮信号携带原始调用参数。"""
    h = ChainHarness(tmp_path, transcript=_write_transcript(tmp_path))
    await h.start()
    try:
        await h.barrier(
            "dispatch", "toolu-1", tool_input={"prompt": "重跑这个任务", "subagent_type": "dev"}
        )
        checkpoint_id = await h.group_checkpoint_id()
        await h.coord.validate_dispatch_replay(h.run_id, checkpoint_id)  # 不抛即通过
        await h.send(
            RestoreScope(
                scope_id="node-a",
                checkpoint_id=checkpoint_id,
                expected_scope_epoch=1,
                client_op_id="rs-1",
                replay_dispatch=True,
                context="node=a",
            )
        )
        await h.wait_note(ScopeRestored)
        await h.send(
            RestoreCommitted(
                scope_id="node-a",
                client_op_id="op-restore",
                context="node=a",
                restore_binding=True,
            )
        )
        await _wait_resumed(h, expected=1)
        prompt = h.so.resumed[0]["config"].initial_prompt
        assert "重跑这个任务" in prompt
        assert "请重新派发子代理任务" in prompt
    finally:
        await h.finish()


async def test_reader_subagent_gets_detached_snapshot_never_integrated(tmp_path: Path) -> None:
    """reader 子代理：detached worktree 只读快照，无分支、永不集成。"""
    h = ChainHarness(tmp_path)
    await h.start()
    try:
        outcome = await h.barrier("dispatch", "toolu-1", workspace="reader")
        assert outcome.child_cwd
        reader = Path(outcome.child_cwd)
        assert (reader / "design" / "base.md").read_text(encoding="utf-8") == "base\n"
        # 无分支：detached HEAD
        branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=reader,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        assert branch == "HEAD"
        scopes = {s["scope_id"]: s for s in await h.coord.list_run_scopes(h.run_id)}
        reader_row = scopes["node-a-c1"]
        assert reader_row["workspace_ref"].endswith("|")
        # 永不集成：集成动作被明确拒绝
        await h.send(IntegrateScope(scope_id="node-a-c1", client_op_id="int-r", context="node=a"))
        failed = await h.wait_note(WorkspaceActionFailed)
        assert "reader 作用域永不集成" in failed.error
    finally:
        await h.finish()


async def test_subagent_start_backfills_child_session_map(tmp_path: Path) -> None:
    """子代理启动回执：按派发准入回填组检查点行的子会话映射。"""
    h = ChainHarness(tmp_path)
    await h.start()
    try:
        await h.barrier("dispatch", "toolu-1")
        await h.coord.record_subagent_start(
            "ses-exec", {"agent_id": "ses-child-native", "parent_tool_use_id": "toolu-1"}
        )
        checkpoint_id = await h.group_checkpoint_id()
        rows = await h.coord.list_run_checkpoints(h.run_id, scope_id="node-a")
        row = next(r for r in rows if r["checkpoint_id"] == checkpoint_id)
        assert row["child_session_map"] == {"node-a-c1": "ses-child-native"}
    finally:
        await h.finish()


async def _wait_resumed(h: ChainHarness, *, expected: int) -> None:
    """等待宿主替身记录到指定数量的恢复会话创建。"""
    deadline = asyncio.get_running_loop().time() + 30.0
    while asyncio.get_running_loop().time() < deadline:
        if len(h.so.resumed) >= expected:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("恢复会话未在限期内创建")


async def test_api_subagent_rollback_validates_and_forwards(tmp_path: Path) -> None:
    """API 面：组检查点核验、非组行拒绝与 op 转发。"""
    from workflow_controller.api.run_api import RunAPI

    h = ChainHarness(tmp_path, transcript=_write_transcript(tmp_path))
    await h.start()
    try:
        await h.barrier("dispatch", "toolu-1")
        checkpoint_id = await h.group_checkpoint_id()
        api = RunAPI(h.coord)
        result = await api.request_subagent_rollback(
            h.run_id,
            node_id="a",
            client_op_id="co-5",
            expected_epoch=1,
            checkpoint_id=checkpoint_id,
        )
        assert result["operation_id"] == "co-5"
        assert h.driver.ops[0][0] == "rollback_subagent"
        assert h.driver.ops[0][1]["checkpoint_id"] == checkpoint_id
        # 非组检查点行（scope_start）拒绝
        rows = await h.coord.list_run_checkpoints(h.run_id, scope_id="node-a")
        scope_start = next(r["checkpoint_id"] for r in rows if r["trigger"] == "scope_start")
        with pytest.raises(ValueError, match="派发组检查点"):
            await api.request_subagent_rollback(
                h.run_id,
                node_id="a",
                client_op_id="co-6",
                expected_epoch=1,
                checkpoint_id=scope_start,
            )
        # 不在账上的检查点拒绝
        with pytest.raises(ValueError, match="检查点不存在"):
            await api.request_subagent_rollback(
                h.run_id,
                node_id="a",
                client_op_id="co-7",
                expected_epoch=1,
                checkpoint_id="nope",
            )
    finally:
        await h.finish()
