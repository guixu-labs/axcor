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
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from workflow_controller.core.checkpoints import BarrierQuality
from workflow_controller.driver.hooks import BarrierDeny, BarrierProceed
from workflow_controller.plugin.run_driver import EmitEvent, TimerFired
from workflow_controller.plugin.workspace import PrepareScopeWorkspace, WorkspaceTreeConfig

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


def _prepare_then_hold(h: BridgeHarness, work_root: Path, overrides: dict[str, Any]) -> Any:
    """「首轮建区 → 回执后按 READY 收口」的完整计划。"""
    base = _hold_plan(h.run_id)

    def plan(driver: ProbeDriver) -> list[Any] | None:
        prepared = [n for n in driver.notes if type(n).__name__ == "ScopeWorkspacePrepared"]
        if not prepared:
            return [
                PrepareScopeWorkspace(
                    config=WorkspaceTreeConfig(root_dir=str(work_root), **overrides),
                    scope_key="node-a",
                    client_op_id="prepare-1",
                    owner_ref="a",
                    context="node=a",
                )
            ]
        return base(driver)

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
) -> BridgeHarness:
    """启动 run、建节点作用域并挂执行会话归属；返回装配。"""
    work_root = _make_work_root(tmp_path)
    h = BridgeHarness(tmp_path, _hold_plan("boot"), driver=driver)
    h.run_id = await h.start()
    _READY[h.run_id] = False
    h.driver._plan = _prepare_then_hold(h, work_root, tree_overrides or {})
    deadline = asyncio.get_running_loop().time() + 30.0
    while asyncio.get_running_loop().time() < deadline:
        scopes = await h.coord.list_run_scopes(h.run_id)
        if any(s["scope_id"] == "node-a" for s in scopes):
            break
        await asyncio.sleep(0.02)
    else:
        raise AssertionError("节点作用域未在限期内建立")
    await h.affiliation.attach("ses-exec", h.run_id, role="executor", context="node=a")
    return h


async def test_dispatch_barrier_returns_child_cwd_through_queue(tmp_path: Path) -> None:
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
            runtime="claude_sdk",
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
