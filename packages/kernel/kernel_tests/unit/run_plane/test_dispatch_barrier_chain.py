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

"""派发屏障链测试：hook → 宿主端口 → 协调器队列 → 核心动作 → 回 cwd/放行。

以 fake hook 直接调用协调器屏障入口模拟 runtime 回调；真实 git 与真实
run 平面装配（探针驱动）。能力矩阵门与有界等待收口同在此固化。
子代理完成走同一条队列：结果交付前并入父作用域、释放子工作区，
冲突与超窗按故障事件留痕。

链路本身以能力全备的假 runtime 为前提（夹具 full_capability_runtime），
不依赖任何真实 runtime 的能力声明；真实 runtime 的降级行为另有用例。
"""

from __future__ import annotations

import asyncio
import logging
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from workflow_controller.core.checkpoints import BarrierQuality, CheckpointStore
from workflow_controller.core.workspace import ScopeState
from workflow_controller.core.workspace.dispatch_groups import ReplayRejectedError
from workflow_controller.core.workspace.tree_runtime import WorkspaceTreeRuntime
from workflow_controller.driver.hooks import BarrierDeny, BarrierProceed
from workflow_controller.plugin.run_driver import ArtifactRead, EmitEvent, ReadArtifact, TimerFired
from workflow_controller.plugin.workspace import (
    AbandonScope,
    PrepareScopeWorkspace,
    WorkspaceTreeConfig,
)

from .test_workspace_bridge import BridgeHarness, ProbeDriver

# 探针计划收口开关（run_id → 是否允许 Finish）
_READY: dict[str, bool] = {}


def _hold_plan(run_key: str) -> Any:
    """探针计划：测试置位 READY 前保持 run 存活（屏障经 FIFO 服务）。"""

    def plan(driver: ProbeDriver) -> list[Any] | None:
        if _READY.get(run_key):
            return None  # → FinishRun
        return [EmitEvent("dispatch_barrier_probe", {"run": run_key})]

    return plan


class SlowSnapshotDriver(ProbeDriver):
    """snapshot 慢驱动：制造屏障等待超窗（快照在屏障动作链内先执行）。"""

    def __init__(self, plan: Any, delay_s: float) -> None:
        super().__init__(plan)
        self._delay_s = delay_s

    def snapshot(self) -> dict[str, Any]:
        time.sleep(self._delay_s)
        return {"schema_version": 1, "state": {}, "pending_timers": []}


def _prepare_then_hold(
    h: BridgeHarness,
    work_root: Path,
    overrides: dict[str, Any],
    *,
    scope_keys: tuple[str, ...] = ("node-a",),
    abandon_previous: bool = True,
) -> Any:
    """「逐键建区 → 回执后按 READY 收口」的计划。

    多键且 abandon_previous 时先废弃前一作用域再建下一个，即节点重试路径：
    旧作用域以已废弃态留账，新作用域换键重建，两者的属主相同。
    """
    base = _hold_plan(h.run_id)

    def plan(driver: ProbeDriver) -> list[Any] | None:
        prepared = [n for n in driver.notes if type(n).__name__ == "ScopeWorkspacePrepared"]
        if len(prepared) == len(scope_keys):
            return base(driver)
        abandoned = [n for n in driver.notes if type(n).__name__ == "ScopeAbandoned"]
        if prepared and abandon_previous and not abandoned:
            return [
                AbandonScope(
                    scope_id=prepared[-1].scope_id,
                    expected_scope_epoch=prepared[-1].scope_epoch,
                    client_op_id=f"abandon-{len(prepared)}",
                    context="node=a",
                )
            ]
        return [
            PrepareScopeWorkspace(
                config=WorkspaceTreeConfig(root_dir=str(work_root), **overrides),
                scope_key=scope_keys[len(prepared)],
                client_op_id=f"prepare-{len(prepared) + 1}",
                owner_ref="a",
                context="node=a",
            )
        ]

    return plan


def _make_work_root(tmp_path: Path) -> Path:
    """用户现场根目录（基线文件一份）。"""
    work_root = tmp_path / "root"
    (work_root / "design").mkdir(parents=True)
    (work_root / "design" / "base.md").write_text("base\n", encoding="utf-8")
    return work_root


async def _finish_run(h: BridgeHarness) -> None:
    """置位收口开关并注入一轮定时器通知触发 Finish，等待 run 终态。"""
    _READY[h.run_id] = True
    h.coord.notify_run(h.run_id, TimerFired(timer_id="test-finish"))
    deadline = asyncio.get_running_loop().time() + 30.0
    while asyncio.get_running_loop().time() < deadline:
        record = await h.store.get(h.run_id)
        if record.finished_at is not None:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("run 未在限期内落终态")


async def _setup_run(
    tmp_path: Path,
    *,
    driver: ProbeDriver | None = None,
    tree_overrides: dict[str, Any] | None = None,
    scope_keys: tuple[str, ...] = ("node-a",),
    abandon_previous: bool = True,
) -> BridgeHarness:
    """启动 run、按序建节点作用域并挂执行会话归属；返回装配。"""
    work_root = _make_work_root(tmp_path)
    h = BridgeHarness(tmp_path, _hold_plan("boot"), driver=driver)
    h.run_id = await h.start(work_dir=work_root)
    _READY[h.run_id] = False
    h.driver._plan = _prepare_then_hold(
        h, work_root, tree_overrides or {}, scope_keys=scope_keys, abandon_previous=abandon_previous
    )
    deadline = asyncio.get_running_loop().time() + 30.0
    while asyncio.get_running_loop().time() < deadline:
        scopes = await h.coord.list_run_scopes(h.run_id)
        if any(s["scope_id"] == scope_keys[-1] for s in scopes):
            break
        await asyncio.sleep(0.02)
    else:
        raise AssertionError("节点作用域未在限期内建立")
    await h.affiliation.attach("ses-exec", h.run_id, role="executor", context="node=a")
    return h


async def test_dispatch_barrier_returns_child_cwd_through_queue(
    tmp_path: Path, full_capability_runtime: str
) -> None:
    """task 派发屏障：请求过协调器队列执行核心动作，返回子工作区 cwd。"""
    h = await _setup_run(tmp_path)
    try:
        outcome = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "dispatch",
            {
                "tool_name": "Task",
                "tool_input": {"prompt": "do work"},
                "tool_use_id": "toolu-1",
                "native_session_id": "ses-native-exec",
                "prompt_id": "msg-1",
            },
            runtime=full_capability_runtime,
        )
        assert isinstance(outcome, BarrierProceed)
        assert outcome.child_cwd
        assert Path(outcome.child_cwd).is_dir()
        assert (Path(outcome.child_cwd) / "design" / "base.md").read_text(
            encoding="utf-8"
        ) == "base\n"

        rows = await h.coord.list_run_checkpoints(h.run_id, scope_id="node-a")
        group_rows = [r for r in rows if r["trigger"] == "before_child"]
        assert len(group_rows) == 1
        anchor = group_rows[0]["dispatch_anchor"]
        assert anchor["parent_native_session_id"] == "ses-native-exec"
        assert anchor["tool_use_id"] == "toolu-1"
        assert anchor["parent_message_id"] == "msg-1"
        assert anchor["exclude_message_id"] == "msg-1"
        assert anchor["preallocated_scope_id"]
        assert anchor["dispatch_group_id"]
        assert group_rows[0]["barrier_quality"] == "precise"
    finally:
        await _finish_run(h)


async def test_side_effect_barrier_establishes_group(tmp_path: Path) -> None:
    """副作用屏障：建组检查点先于副作用，放行不携带 cwd。"""
    h = await _setup_run(tmp_path)
    try:
        outcome = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "side_effect",
            {
                "tool_name": "Write",
                "tool_input": {"file_path": "design/x.md", "content": "x"},
                "tool_use_id": "toolu-w1",
                "prompt_id": "msg-1",
            },
            runtime="claude_sdk",
        )
        assert isinstance(outcome, BarrierProceed)
        assert outcome.child_cwd is None
        rows = await h.coord.list_run_checkpoints(h.run_id, scope_id="node-a")
        assert [r["trigger"] for r in rows if r["trigger"] == "before_child"] == ["before_child"]
    finally:
        await _finish_run(h)


@pytest.mark.parametrize(
    ("role", "context", "abandon_previous"),
    [
        ("executor", "node=a", True),
        ("verifier", "node=a/verify", True),
        ("executor", "node=a", False),
    ],
)
async def test_barrier_lands_on_latest_active_scope_of_node(
    tmp_path: Path,
    role: str,
    context: str,
    abandon_previous: bool,
    caplog: pytest.LogCaptureFixture,
    full_capability_runtime: str,
) -> None:
    """节点重试后旧作用域已废弃、新作用域活动：执行与验收会话的屏障都落在活动作用域。

    旧作用域未废弃就重建（不应发生）时同样落在最近建立的活动作用域，并告警。
    """
    h = await _setup_run(
        tmp_path, scope_keys=("node-a", "node-a2"), abandon_previous=abandon_previous
    )
    try:
        await h.affiliation.attach("ses-call", h.run_id, role=role, context=context)
        with caplog.at_level(logging.WARNING, logger="workflow_controller.core.run_coordinator"):
            side_effect = await h.coord.submit_dispatch_barrier(
                "ses-call",
                "side_effect",
                {"tool_name": "Bash", "tool_use_id": "toolu-r1", "prompt_id": "msg-retry"},
                runtime=full_capability_runtime,
            )
            dispatch = await h.coord.submit_dispatch_barrier(
                "ses-call",
                "dispatch",
                {"tool_name": "Task", "tool_use_id": "toolu-r2", "prompt_id": "msg-retry"},
                runtime=full_capability_runtime,
            )
        assert isinstance(side_effect, BarrierProceed)
        assert isinstance(dispatch, BarrierProceed)
        assert dispatch.child_cwd

        group_rows = [
            r
            for r in await h.coord.list_run_checkpoints(h.run_id)
            if r["trigger"] == "before_child"
        ]
        assert {r["scope_id"] for r in group_rows} == {"node-a2"}
        child_parents = [
            s["parent_scope_id"]
            for s in await h.coord.list_run_scopes(h.run_id)
            if s["scope_kind"] == "subagent"
        ]
        assert child_parents == ["node-a2"]
        assert ("有多个活动作用域" in caplog.text) is (not abandon_previous)
    finally:
        await _finish_run(h)


async def test_barrier_denied_when_node_scope_left_active(tmp_path: Path) -> None:
    """节点作用域停在待集成或冲突态：屏障拒绝，拒绝原因带出作用域及其状态。"""
    h = await _setup_run(tmp_path)
    try:
        await h.affiliation.attach("ses-exec", h.run_id, role="executor", context="node=a")
        ledger = h.coord._workspace_trees[h.run_id].scope_tree
        for state in (ScopeState.AWAITING_INTEGRATION, ScopeState.CONFLICTED):
            await ledger.set_state("node-a", state)
            outcome = await h.coord.submit_dispatch_barrier(
                "ses-exec",
                "side_effect",
                {"tool_name": "Bash", "tool_use_id": "toolu-1", "prompt_id": "msg-1"},
                runtime="claude_sdk",
            )
            assert isinstance(outcome, BarrierDeny)
            assert "屏障父作用域未找到" in outcome.reason
            assert f"node-a={state.value}" in outcome.reason
    finally:
        await _finish_run(h)


async def test_strict_mode_denies_barrierless_runtime(tmp_path: Path) -> None:
    """严格档：缺②的 runtime 拒绝 writer 子代理，缺⑤拒绝副作用（fail-closed）。"""
    h = await _setup_run(tmp_path)
    try:
        dispatch = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "dispatch",
            {"tool_name": "Task", "tool_use_id": "t1", "prompt_id": "m1"},
            runtime="opencode",
        )
        assert isinstance(dispatch, BarrierDeny)
        assert "cwd 注入" in dispatch.reason
        side_effect = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "side_effect",
            {"tool_name": "Write", "tool_use_id": "t2", "prompt_id": "m1"},
            runtime="opencode",
        )
        assert isinstance(side_effect, BarrierDeny)
        assert "组仲裁" in side_effect.reason
        # claude_cli：PreToolUse matcher 面不受本仓控制，同样按缺失处理
        cli_outcome = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "dispatch",
            {"tool_name": "Task", "tool_use_id": "t3", "prompt_id": "m1"},
            runtime="claude_cli",
        )
        assert isinstance(cli_outcome, BarrierDeny)
    finally:
        await _finish_run(h)


async def test_strict_mode_denies_claude_sdk_writer_dispatch(tmp_path: Path) -> None:
    """严格档 + claude_sdk（不声明子代理 cwd 注入）：writer 派发被拒、不建子作用域，副作用屏障照常放行。"""
    h = await _setup_run(tmp_path)
    try:
        dispatch = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "dispatch",
            {"tool_name": "Task", "tool_use_id": "toolu-1", "prompt_id": "msg-1"},
            runtime="claude_sdk",
        )
        assert isinstance(dispatch, BarrierDeny)
        assert "cwd 注入" in dispatch.reason
        scopes = await h.coord.list_run_scopes(h.run_id)
        assert not [s for s in scopes if s["scope_kind"] == "subagent"]
        group_rows = [
            r
            for r in await h.coord.list_run_checkpoints(h.run_id)
            if r["trigger"] == "before_child"
        ]
        assert group_rows == []
        side_effect = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "side_effect",
            {"tool_name": "Bash", "tool_use_id": "toolu-2", "prompt_id": "msg-1"},
            runtime="claude_sdk",
        )
        assert isinstance(side_effect, BarrierProceed)
    finally:
        await _finish_run(h)


async def test_compat_mode_proceeds_with_late_barrier(tmp_path: Path) -> None:
    """弱化档：显式 compat_late_barrier 下放行并标注 late_barrier。"""
    h = await _setup_run(tmp_path, tree_overrides={"on_no_dispatch_barrier": "compat_late_barrier"})
    try:
        outcome = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "dispatch",
            {"tool_name": "Task", "tool_use_id": "t1", "prompt_id": "m1"},
            runtime="opencode",
        )
        assert isinstance(outcome, BarrierProceed)
        assert outcome.child_cwd
        rows = await h.coord.list_run_checkpoints(h.run_id, scope_id="node-a")
        group_rows = [r for r in rows if r["trigger"] == "before_child"]
        assert group_rows
        assert group_rows[0]["barrier_quality"] == BarrierQuality.LATE_BARRIER.value
    finally:
        await _finish_run(h)


async def test_barrier_wait_window_closes_with_fault(tmp_path: Path) -> None:
    """有界等待：超窗按 fault.scope_quiesce_failed 收口，hook 不挂死。"""
    slow = SlowSnapshotDriver(_hold_plan("slow"), delay_s=1.0)
    h = await _setup_run(tmp_path, driver=slow, tree_overrides={"background_drain_timeout_s": 0.05})
    try:
        started = time.monotonic()
        outcome = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "dispatch",
            {"tool_name": "Task", "tool_use_id": "t1", "prompt_id": "m1"},
            runtime="claude_sdk",
        )
        assert time.monotonic() - started < 0.9
        assert isinstance(outcome, BarrierDeny)
        assert outcome.fault == "fault.scope_quiesce_failed"
        assert "超窗" in outcome.reason
    finally:
        await _finish_run(h)


def _drop_workspace_tree(h: BridgeHarness) -> None:
    """让内存里的工作区树运行时离场（模拟重启或重开后树只在盘上）。"""
    tree = h.coord._workspace_trees.pop(h.run_id)
    tree.close()


async def test_barrier_rebuilds_tree_left_only_on_disk(
    tmp_path: Path, full_capability_runtime: str
) -> None:
    """树只在盘上：屏障先按持久化配置重建树再执行，不静默直通。"""
    h = await _setup_run(tmp_path)
    try:
        _drop_workspace_tree(h)
        dispatch = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "dispatch",
            {"tool_name": "Task", "tool_use_id": "toolu-1", "prompt_id": "msg-1"},
            runtime=full_capability_runtime,
        )
        assert isinstance(dispatch, BarrierProceed)
        assert dispatch.child_cwd
        assert Path(dispatch.child_cwd).is_dir()
        side_effect = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "side_effect",
            {"tool_name": "Bash", "tool_use_id": "toolu-2", "prompt_id": "msg-2"},
            runtime=full_capability_runtime,
        )
        assert isinstance(side_effect, BarrierProceed)
        assert side_effect.child_cwd is None
        group_rows = [
            (row["scope_id"], row["dispatch_anchor"]["tool_use_id"])
            for row in await h.coord.list_run_checkpoints(h.run_id)
            if row["trigger"] == "before_child"
        ]
        assert group_rows == [("node-a", "toolu-1"), ("node-a", "toolu-2")]
    finally:
        await _finish_run(h)


async def test_list_run_scopes_rebuilds_tree_left_only_on_disk(tmp_path: Path) -> None:
    """树只在盘上而 run 仍在运行：作用域查询重建树，返回的行与重启前逐字段一致。"""
    h = await _setup_run(tmp_path, scope_keys=("node-a", "node-a2"))  # 节点重试：旧作用域已废弃
    try:
        before = await h.coord.list_run_scopes(h.run_id)
        _drop_workspace_tree(h)
        assert await h.coord.list_run_scopes(h.run_id) == before
        scopes = {s["scope_id"]: (s["owner_ref"], s["state"], s["scope_epoch"]) for s in before}
        assert scopes == {
            f"{h.run_id}-root": (None, "active", 1),
            "node-a": ("a", "discarded", 2),
            "node-a2": ("a", "active", 1),
        }
    finally:
        await _finish_run(h)


async def test_read_artifact_rebuilds_tree_left_only_on_disk(tmp_path: Path) -> None:
    """树只在盘上：产物读取先按持久化配置重建树，再从节点作用域工作区读出。"""
    h = await _setup_run(tmp_path)
    try:
        node_workspace = next(
            s["workspace_path"]
            for s in await h.coord.list_run_scopes(h.run_id)
            if s["scope_id"] == "node-a"
        )
        (Path(node_workspace) / "design" / "base.md").write_text("scope edit\n", encoding="utf-8")
        hold = h.driver._plan

        def plan(driver: ProbeDriver) -> list[Any] | None:
            """读回执到达前发一次读取，之后沿用挂起计划。"""
            if any(isinstance(n, ArtifactRead) for n in driver.notes):
                return hold(driver)
            return [ReadArtifact(path="design/base.md", context="node=a")]

        h.driver._plan = plan
        _drop_workspace_tree(h)
        h.coord.notify_run(h.run_id, TimerFired(timer_id="poke"))
        deadline = asyncio.get_running_loop().time() + 30.0
        while asyncio.get_running_loop().time() < deadline:
            reads = [n for n in h.driver.notes if isinstance(n, ArtifactRead)]
            if reads:
                break
            await asyncio.sleep(0.02)
        else:
            raise AssertionError("产物读取回执未在限期内到达")
        assert (reads[0].exists, reads[0].value) == (True, "scope edit")
        assert h.run_id in h.coord._workspace_trees  # 树已重建
    finally:
        await _finish_run(h)


async def test_replay_validation_rebuilds_tree_left_only_on_disk(
    tmp_path: Path, full_capability_runtime: str
) -> None:
    """树只在盘上：重放前置校验先重建树，结论与树在内存时一致。"""
    h = await _setup_run(tmp_path)
    try:
        # 节点作用域上每个消息的首个副作用各建一个组：一个单 task，一个 task 加 Bash
        for kind, tool_name, tool_use_id, message in (
            ("dispatch", "Task", "toolu-task-1", "msg-single"),
            ("side_effect", "Bash", "toolu-bash", "msg-multi"),
            ("dispatch", "Task", "toolu-task-2", "msg-multi"),
        ):
            await h.coord.submit_dispatch_barrier(
                "ses-exec",
                kind,
                {"tool_name": tool_name, "tool_use_id": tool_use_id, "prompt_id": message},
                runtime=full_capability_runtime,
            )
        group_rows = {
            row["dispatch_anchor"]["tool_use_id"]: row["checkpoint_id"]
            for row in await h.coord.list_run_checkpoints(h.run_id)
            if row["trigger"] == "before_child"
        }
        single_task_group = group_rows["toolu-task-1"]  # 首个派发即建组行
        multi_effect_group = group_rows["toolu-bash"]  # Bash 建组，task 入组

        async def assert_conclusions() -> None:
            """单 task 组通过，多副作用组拒绝。"""
            await h.coord.validate_dispatch_replay(h.run_id, single_task_group)
            with pytest.raises(ReplayRejectedError, match="仅允许单 task"):
                await h.coord.validate_dispatch_replay(h.run_id, multi_effect_group)

        await assert_conclusions()  # 树在内存
        _drop_workspace_tree(h)
        await assert_conclusions()  # 树只在盘上：重建后结论不变
    finally:
        await _finish_run(h)
    with pytest.raises(ReplayRejectedError, match="无工作区树运行时"):  # run 已无运行时
        await h.coord.validate_dispatch_replay(h.run_id, single_task_group)


async def test_query_and_barrier_rebuild_tree_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    full_capability_runtime: str,
) -> None:
    """查询面与屏障并发到场只重建一次树：后到者等同一棵树，不各建一棵。"""
    h = await _setup_run(tmp_path)
    entered = asyncio.Event()
    release = asyncio.Event()
    builds: list[WorkspaceTreeRuntime] = []
    original_initialize = WorkspaceTreeRuntime.initialize

    async def gated_initialize(self: WorkspaceTreeRuntime) -> None:
        builds.append(self)
        entered.set()
        await release.wait()
        await original_initialize(self)

    monkeypatch.setattr(WorkspaceTreeRuntime, "initialize", gated_initialize)
    try:
        _drop_workspace_tree(h)
        query = asyncio.create_task(h.coord.list_run_scopes(h.run_id))
        await asyncio.wait_for(entered.wait(), 30)
        barrier = asyncio.create_task(
            h.coord.submit_dispatch_barrier(
                "ses-exec",
                "dispatch",
                {"tool_name": "Task", "tool_use_id": "toolu-1", "prompt_id": "msg-1"},
                runtime=full_capability_runtime,
            )
        )
        await asyncio.sleep(0.5)  # 留出 run 任务取到屏障请求的时间
        release.set()
        assert isinstance((await barrier).child_cwd, str)
        assert [s["scope_id"] for s in await query] == [f"{h.run_id}-root", "node-a"]
        assert len(builds) == 1
    finally:
        release.set()
        await _finish_run(h)


async def test_barrier_denied_when_persisted_tree_has_no_runtime(tmp_path: Path) -> None:
    """盘上有树而 run 已无运行时：屏障拒绝，不因树不在内存而直通。"""
    h = await _setup_run(tmp_path)
    await _finish_run(h)  # 运行时与内存树随之退出，树配置留在盘上
    outcome = await h.coord.submit_dispatch_barrier(
        "ses-exec",
        "dispatch",
        {"tool_name": "Task", "tool_use_id": "toolu-1", "prompt_id": "msg-1"},
        runtime="claude_sdk",
    )
    assert isinstance(outcome, BarrierDeny)
    assert "无可用驱动运行时" in outcome.reason


async def test_run_without_tree_passes_through(tmp_path: Path) -> None:
    """无工作区树（traceback 未开）：屏障直通放行，与既有路径零差异。"""
    h = BridgeHarness(tmp_path, _hold_plan("boot"))
    h.run_id = await h.start()
    _READY[h.run_id] = False
    h.driver._plan = _hold_plan(h.run_id)
    try:
        await h.affiliation.attach("ses-exec", h.run_id, role="executor", context="node=a")
        outcome = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "dispatch",
            {"tool_name": "Task", "tool_use_id": "t1", "prompt_id": "m1"},
            runtime="opencode",
        )
        assert isinstance(outcome, BarrierProceed)
        assert outcome.child_cwd is None
    finally:
        await _finish_run(h)


async def test_unaffiliated_session_passes_through(tmp_path: Path) -> None:
    """无 run 归属的会话（单 session 旧路径）：屏障直通放行。"""
    h = BridgeHarness(tmp_path, _hold_plan("boot"))
    h.run_id = await h.start()
    _READY[h.run_id] = False
    h.driver._plan = _hold_plan(h.run_id)
    try:
        outcome = await h.coord.submit_dispatch_barrier(
            "ses-solo",
            "side_effect",
            {"tool_name": "Write", "tool_use_id": "t1", "prompt_id": "m1"},
            runtime="claude_sdk",
        )
        assert isinstance(outcome, BarrierProceed)
    finally:
        await _finish_run(h)


# ── 子代理完成：结果交付前并入父作用域 ────────────────────────────────


async def _dispatch_subagent(
    h: BridgeHarness,
    *,
    runtime: str,
    workspace: str = "writer",
    agent_id: str = "agent-1",
    child_mapped: bool = True,
) -> str:
    """派发一个 writer/reader 子代理，返回子工作区路径；child_mapped 为真时回填子会话映射。"""
    outcome = await h.coord.submit_dispatch_barrier(
        "ses-exec",
        "dispatch",
        {"tool_name": "Task", "tool_use_id": "toolu-1", "prompt_id": "msg-1"},
        runtime=runtime,
        workspace=workspace,
    )
    assert isinstance(outcome, BarrierProceed) and outcome.child_cwd
    if child_mapped:
        await h.coord.record_subagent_start(
            "ses-exec", {"agent_id": agent_id, "parent_tool_use_id": "toolu-1"}
        )
    return outcome.child_cwd


async def _scope_states(h: BridgeHarness) -> dict[str, str]:
    """run 内全部作用域的状态表。"""
    return {s["scope_id"]: s["state"] for s in await h.coord.list_run_scopes(h.run_id)}


async def test_writer_subagent_completion_integrates_into_parent_scope(
    tmp_path: Path, full_capability_runtime: str
) -> None:
    """writer 子代理完成：子分支并入父作用域、产出在父工作区可见、子工作区释放、terminal 行落账。"""
    h = await _setup_run(tmp_path)
    try:
        child_cwd = await _dispatch_subagent(h, runtime=full_capability_runtime)
        (Path(child_cwd) / "design" / "from-child.md").write_text("child\n", encoding="utf-8")

        await h.coord.settle_subagent_result("ses-exec", {"agent_id": "agent-1"})
        await h.coord.settle_subagent_result("ses-exec", {"agent_id": "agent-1"})  # 重复回执幂等

        node_workspace = next(
            s["workspace_path"]
            for s in await h.coord.list_run_scopes(h.run_id)
            if s["scope_id"] == "node-a"
        )
        assert (Path(node_workspace) / "design" / "from-child.md").read_text(
            encoding="utf-8"
        ) == "child\n"
        # 并入发生在父作用域分支上：父分支头即子作用域的集成提交
        parent_head_subject = subprocess.run(
            ["git", "log", "-1", "--format=%s"],
            cwd=node_workspace,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        assert parent_head_subject == "integrate node-a-c1"
        assert not Path(child_cwd).exists()
        assert (await _scope_states(h))["node-a-c1"] == "released"
        checkpoints = CheckpointStore(h.store.db_path)
        rows = await checkpoints.list_for_scope("node-a-c1")
        checkpoints.close()
        assert [row.trigger.value for row in rows] == ["terminal"]
        assert rows[0].parent_scope_id == "node-a"
        assert len(rows[0].workspace_commit) == 40
        assert rows[0].orchestration_state_ref.startswith("driver-snapshot:")
    finally:
        await _finish_run(h)


async def test_reader_subagent_completion_releases_snapshot_workspace(
    tmp_path: Path, full_capability_runtime: str
) -> None:
    """reader 子代理完成：快照工作区释放，作用域按废弃换代、永不集成。"""
    h = await _setup_run(tmp_path)
    try:
        reader_cwd = await _dispatch_subagent(
            h, runtime=full_capability_runtime, workspace="reader"
        )
        assert Path(reader_cwd).is_dir()

        await h.coord.settle_subagent_result("ses-exec", {"agent_id": "agent-1"})

        assert not Path(reader_cwd).exists()
        assert (await _scope_states(h))["node-a-c1"] == "discarded"
        assert not h.run_events(h.run_id, "fault.workspace_conflict")
    finally:
        await _finish_run(h)


async def test_subagent_completion_conflict_emits_fault_and_keeps_workspace(
    tmp_path: Path,
    full_capability_runtime: str,
) -> None:
    """集成被拒：落 fault.workspace_conflict，子工作区与子分支保留，入口照常返回。"""
    h = await _setup_run(tmp_path)
    try:
        child_cwd = await _dispatch_subagent(h, runtime=full_capability_runtime)
        node_workspace = next(
            s["workspace_path"]
            for s in await h.coord.list_run_scopes(h.run_id)
            if s["scope_id"] == "node-a"
        )
        (Path(node_workspace) / "design" / "base.md").write_text("parent edit\n", encoding="utf-8")
        (Path(child_cwd) / "design" / "base.md").write_text("child edit\n", encoding="utf-8")

        await h.coord.settle_subagent_result("ses-exec", {"agent_id": "agent-1"})

        events = h.run_events(h.run_id, "fault.workspace_conflict")
        assert len(events) == 1
        assert events[0]["conflict_paths"] == ["design/base.md"]
        assert events[0]["scope_id"] == "node-a-c1"
        assert events[0]["parent_scope_id"] == "node-a"
        assert events[0]["session_id"] == "ses-exec"
        assert events[0]["agent_id"] == "agent-1"
        assert Path(child_cwd).is_dir()
        assert (Path(child_cwd) / "design" / "base.md").read_text(
            encoding="utf-8"
        ) == "child edit\n"
        assert (await _scope_states(h))["node-a-c1"] == "conflicted"
    finally:
        await _finish_run(h)


async def test_subagent_completion_quiesce_failure_emits_fault_and_keeps_workspace(
    tmp_path: Path,
    full_capability_runtime: str,
) -> None:
    """闸门不可静止：落 fault.scope_quiesce_failed，子工作区保留，入口照常返回。"""
    h = await _setup_run(tmp_path)
    try:
        child_cwd = await _dispatch_subagent(h, runtime=full_capability_runtime)
        (Path(child_cwd) / "design" / "from-child.md").write_text("child\n", encoding="utf-8")

        async def never_drains(_scope_id: str) -> None:
            raise RuntimeError("后台写进程无法排空")

        h.coord._workspace_trees[h.run_id].manager._drain_hook = never_drains
        await h.coord.settle_subagent_result("ses-exec", {"agent_id": "agent-1"})

        events = h.run_events(h.run_id, "fault.scope_quiesce_failed")
        assert len(events) == 1
        assert events[0]["agent_id"] == "agent-1"
        assert "闸门未能静止" in events[0]["error"]
        assert (Path(child_cwd) / "design" / "from-child.md").exists()
        assert (await _scope_states(h))["node-a-c1"] == "active"
    finally:
        await _finish_run(h)


async def test_subagent_completion_wait_window_closes_with_fault(
    tmp_path: Path, full_capability_runtime: str
) -> None:
    """有界等待：超窗落 fault.scope_quiesce_failed 后放行，钩子不挂死。"""
    slow = SlowSnapshotDriver(_hold_plan("slow"), delay_s=0.0)
    h = await _setup_run(tmp_path, driver=slow)
    try:
        child_cwd = await _dispatch_subagent(h, runtime=full_capability_runtime)
        slow._delay_s = 1.0  # 收尾链里的编排快照变慢，撞上收窄后的等待窗口
        h.coord._workspace_trees[h.run_id].barrier_wait_timeout_s = 0.05

        started = time.monotonic()
        await h.coord.settle_subagent_result("ses-exec", {"agent_id": "agent-1"})

        assert time.monotonic() - started < 0.9
        events = h.run_events(h.run_id, "fault.scope_quiesce_failed")
        assert len(events) == 1
        assert events[0]["agent_id"] == "agent-1"
        assert "超窗" in events[0]["error"]
        assert Path(child_cwd).is_dir()
    finally:
        await _finish_run(h)


# ── 子代理自身的调用：不进父作用域组仲裁 ──────────────────────────────


async def _group_rows_of(h: BridgeHarness, scope_id: str) -> list[tuple[str, str, str]]:
    """作用域上的组检查点行：（行标识，工具调用号，组恢复点）。"""
    return [
        (r["checkpoint_id"], r["dispatch_anchor"]["tool_use_id"], r["workspace_commit"])
        for r in await h.coord.list_run_checkpoints(h.run_id)
        if r["scope_id"] == scope_id and r["trigger"] == "before_child"
    ]


async def _child_side_effect(
    h: BridgeHarness, runtime: str, tool_use_id: str, **origin: str
) -> BarrierProceed | BarrierDeny:
    """子代理在自己的回合里发一次副作用屏障（带来源标识）。"""
    return await h.coord.submit_dispatch_barrier(
        "ses-exec",
        "side_effect",
        {"tool_name": "Bash", "tool_use_id": tool_use_id, "prompt_id": "child-turn-9"},
        runtime=runtime,
        **origin,
    )


@pytest.mark.parametrize(
    ("origin", "child_mapped"),
    [
        ({"parent_tool_use_id": "toolu-1", "agent_id": "agent-1"}, True),
        # 捆绑 CLI 的工具前置钩子只带 agent_id、不带父工具调用号；映射未回填时反查不到
        ({"agent_id": "agent-1"}, False),
    ],
    ids=["mapped", "unmapped-agent-id-only"],
)
async def test_subagent_side_effect_barrier_stays_out_of_parent_group(
    tmp_path: Path,
    full_capability_runtime: str,
    caplog: pytest.LogCaptureFixture,
    origin: dict[str, str],
    child_mapped: bool,
) -> None:
    """writer 子代理自身的副作用屏障放行，不进父作用域组仲裁；父会话同消息的后续副作用仍入原组。

    按子会话映射反查子作用域只为留痕：映射未回填时同样放行、同样不落行，并留一条日志。
    """
    h = await _setup_run(tmp_path)
    try:
        child_cwd = await _dispatch_subagent(
            h, runtime=full_capability_runtime, child_mapped=child_mapped
        )
        (Path(child_cwd) / "design" / "from-child.md").write_text("child\n", encoding="utf-8")
        group_before = await _group_rows_of(h, "node-a")
        assert len(group_before) == 1  # 派发时建的组

        with caplog.at_level(
            logging.INFO, logger="workflow_controller.core.workspace.tree_runtime"
        ):
            for tool_use_id in ("toolu-c1", "toolu-c2"):
                outcome = await _child_side_effect(
                    h, full_capability_runtime, tool_use_id, **origin
                )
                assert isinstance(outcome, BarrierProceed) and outcome.child_cwd is None

        # 父作用域的组检查点行与组恢复点都没动
        assert await _group_rows_of(h, "node-a") == group_before
        assert ("未反查到子作用域" in caplog.text) is (not child_mapped)
        # 子作用域上也不落组检查点行
        checkpoints = CheckpointStore(h.store.db_path)
        child_rows = await checkpoints.list_for_scope("node-a-c1")
        checkpoints.close()
        assert child_rows == []
        # 子代理的调用没有记进父作用域的组台账：组仍是「单 task、无其他副作用」
        await h.coord.validate_dispatch_replay(h.run_id, group_before[0][0])

        # 父会话在同一消息内的下一次副作用仍入原组：不另建组，组恢复点不动
        parent_call = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "side_effect",
            {"tool_name": "Bash", "tool_use_id": "toolu-w2", "prompt_id": "msg-1"},
            runtime=full_capability_runtime,
        )
        assert isinstance(parent_call, BarrierProceed)
        assert await _group_rows_of(h, "node-a") == group_before
    finally:
        await _finish_run(h)


async def test_subagent_task_dispatch_barrier_keeps_existing_path(
    tmp_path: Path, full_capability_runtime: str
) -> None:
    """子代理发起的 task 派发（孙代理）：带来源字段照旧在父作用域仲裁并建出子工作区，不因字段被拒。"""
    h = await _setup_run(tmp_path)
    try:
        await _dispatch_subagent(h, runtime=full_capability_runtime)
        outcome = await h.coord.submit_dispatch_barrier(
            "ses-exec",
            "dispatch",
            {"tool_name": "Task", "tool_use_id": "toolu-2", "prompt_id": "child-turn-9"},
            runtime=full_capability_runtime,
            parent_tool_use_id="toolu-1",
            agent_id="agent-1",
        )
        assert isinstance(outcome, BarrierProceed) and outcome.child_cwd
        child_parents = [
            s["parent_scope_id"]
            for s in await h.coord.list_run_scopes(h.run_id)
            if s["scope_kind"] == "subagent"
        ]
        assert child_parents == ["node-a", "node-a"]
    finally:
        await _finish_run(h)
