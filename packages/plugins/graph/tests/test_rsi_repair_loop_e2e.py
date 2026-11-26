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

"""RSI 修复闭环协议的 run 平面端到端回归（本仓可实现部分）。

真实 run 平面（真实 GraphPlugin + RunCoordinator + 内核 workspace/检查点 +
真实 git 与 bash 进程，fake session 宿主）固化 RSI 适配器消费的协议序列：
目标节点超时（fault.node_timeout 落 run 事件面）→ 暂停（在途执行停止）→
导出证据 → 模拟修复应用（改工作流源文件 + 原地重载定义，Run 不换）→
异步回滚（先 resume 再轮询 operation_id）→ 目标新代重跑（新基线含上游
已集成成果）→ 下游继续 → 整图完成；同键回滚幂等、旧代迟到超时不触发
新动作、作用域/检查点/集成线账本一致。

跨仓部分（RSI worker/诊断/修复 agent、接入仓 adapter、控制台路由）不在
本仓施工；真实 fix-findings 环境的验收留待跨仓联调，本测试是其后骨架。
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import yaml

from workflow_controller.api.run_api import RunAPI
from workflow_controller.config.settings import WatchdogConfig
from workflow_controller.session.events import Event
from workflow_controller.store.run_events_cursor import RunEventsCursor
from workflow_controller.watchdog.core import NODE_HISTORY_EVENT, NodeTimeoutRule

from .run_plane_helpers import GraphRunPlaneEnv, await_run_status

# 目标节点累计超时时限（秒）：首轮长眠必超、修复后重跑必远低于
_TIMEOUT_S = 2


def _write_loop_workflow(path: Path, target_command: str) -> Path:
    """写 RSI 闭环演示工作流（同一文件原地改写，模拟 RSI 对源分支打 patch）。"""
    nodes = [
        {
            "id": "upstream",
            "task_type": "bash",
            "command": "echo one > upstream.txt",
            "acceptance": {"enabled": True, "type": "script", "script": "true"},
        },
        {
            "id": "target",
            "task_type": "bash",
            "command": target_command,
            "depends_on": ["upstream"],
            # 验收读节点工作区文件：新代工作区含上游已集成产物才可能通过
            "acceptance": {"enabled": True, "type": "script", "script": "test -f upstream.txt"},
        },
        {
            "id": "downstream",
            "task_type": "bash",
            "command": "cat target.txt > downstream-proof.txt",
            "depends_on": ["target"],
            "acceptance": {"enabled": True, "type": "script", "script": "true"},
        },
    ]
    path.write_text(
        yaml.safe_dump(
            {
                "workflow": "rsi-loop",
                "max_parallel": 2,
                "work_dir": ".",
                "traceback": {"enabled": True},
                "timeouts": {"nodes": {"target": _TIMEOUT_S}},
                "nodes": nodes,
            }
        ),
        encoding="utf-8",
    )
    return path


async def _wait_until(predicate: Any, timeout_s: float, message: str) -> None:
    """轮询等待异步条件成立（失败即 pytest.fail）。"""
    deadline = time.monotonic() + timeout_s
    while True:
        if await predicate():
            return
        if time.monotonic() > deadline:
            pytest.fail(message)
        await asyncio.sleep(0.05)


async def _target_first_attempt_running(env: GraphRunPlaneEnv, run_id: str) -> bool:
    """目标节点已在首轮执行且上游已集成通过（可安全制造超时的现场）。"""
    return (
        env.node_info(run_id, "target")["status"] == "running"
        and env.node_info(run_id, "upstream")["status"] == "passed"
    )


async def _inflight_stopped(api: RunAPI, run_id: str) -> bool:
    """在途执行停止证据：长眠进程被杀（负值退码）且图已暂停的事件落面。"""
    rows = (await api.fetch_events(run_id))[0]
    exited = any(
        row["event_type"] == "process_exit" and (row["data"].get("returncode") or 0) < 0
        for row in rows
    )
    paused = any(row["event_type"] == "graph_paused" for row in rows)
    return exited and paused


async def _fire_node_timeout(rule: NodeTimeoutRule, api: RunAPI, run_id: str) -> dict[str, Any]:
    """推进 watchdog 巡检直至目标节点超时告警落 run 事件面，返回告警行。

    巡检拍是常量，直调 sweep 最稳：时限经 node_history 载荷建账，累计到
    阈值即告警（与生产周期巡检同一判定路径）。
    """
    deadline = time.monotonic() + 15.0
    while True:
        await asyncio.sleep(0.25)
        await rule.sweep(datetime.now(tz=UTC))
        rows = [
            row
            for row in (await api.fetch_events(run_id))[0]
            if row["event_type"] == "fault.node_timeout"
        ]
        if rows:
            return rows[-1]
        if time.monotonic() > deadline:
            pytest.fail("节点累计超时未在限期内触发")


async def _wait_operation_succeeded(api: RunAPI, run_id: str, operation_id: str) -> dict[str, Any]:
    """轮询异步回滚操作至成功（先 resume 后轮询的消费侧形状）。"""
    deadline = time.monotonic() + 30.0
    while True:
        status = await api.get_run_operation(run_id, operation_id)
        if status["phase"] == "succeeded":
            return status
        if status["phase"] == "failed":
            pytest.fail(f"回滚操作失败：{status['error']}")
        if time.monotonic() > deadline:
            pytest.fail(f"回滚操作未在限期内完成（当前 {status['phase']}）")
        await asyncio.sleep(0.05)


async def test_rsi_repair_loop_timeout_to_rollback_to_completion(tmp_path: Path) -> None:
    """超时→暂停→导出→重载（模拟修复）→异步回滚→resume→下游继续→整图完成。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _write_loop_workflow(env.tmp / "wf-rsi-loop.yaml", "sleep 300")
    # 节点超时规则订阅真实总线：node_history/graph_* 经协调器同流分发建账
    rule = NodeTimeoutRule(WatchdogConfig(), env.bus, env.so.pool)
    rule.subscribe()
    api = RunAPI(env.coord, RunEventsCursor(env.so.event_router.default_store))

    run_id = await env.start_graph_run(yaml_path=yaml_path)

    # a. 目标节点超时：告警经 run 事件面可轮询、载荷完整
    await _wait_until(
        lambda: _target_first_attempt_running(env, run_id),
        15.0,
        "target 未在限期内进入首轮执行",
    )
    fault = await _fire_node_timeout(rule, api, run_id)
    payload = fault["data"]
    assert payload["run_id"] == run_id
    assert payload["node_id"] == "target"
    assert payload["gen"] == 0
    assert payload["attempt"] == 1
    # 进程节点无会话身份，契约键恒在
    assert "session_id" in payload and "verify_session_id" in payload
    assert payload["started_at"]
    assert payload["elapsed_seconds"] >= _TIMEOUT_S
    assert payload["threshold_seconds"] == _TIMEOUT_S

    # b. 暂停：run 状态 paused、在途执行停止（杀进程负值退码事件先于图暂停落面）
    await env.coord.pause_run(run_id)
    run = await env.store.get(run_id)
    assert run.status == "paused"
    await _wait_until(lambda: _inflight_stopped(api, run_id), 15.0, "暂停后在途进程未停止")

    # c. 导出证据：本轮运行档案（bundle 落 run 根 export/）
    exported = await api.export_run(run_id)
    assert exported["ok"] is True
    assert (Path(exported["path"]) / "workflow.json").is_file()
    assert exported["members"] >= 3

    # 回滚前快照：作用域账面与目标检查点（失效集外零变化比对基准）
    scopes_before = {row["scope_id"]: row for row in await api.list_run_scopes(run_id)}
    by_owner = {row["owner_ref"]: row for row in scopes_before.values()}
    target_scope = by_owner["target"]
    upstream_before = by_owner["upstream"]
    old_workspace = Path(target_scope["workspace_path"] or "")
    target_checkpoints = await api.list_run_checkpoints(run_id, scope_id=target_scope["scope_id"])
    scope_start = next(row for row in target_checkpoints if row["trigger"] == "scope_start")
    assert len(scope_start["workspace_commit"]) == 40  # 真实 git 提交

    # d. 模拟修复应用：patch 工作流源文件 → 原地重载定义（Run 不换）
    # 重载命令带短眠：rsi-repair-2 请求须在目标再度通过前发出，未合并链的拒绝面才确定
    _write_loop_workflow(
        yaml_path, "sleep 2 && echo fixed > target.txt && cat upstream.txt >> target.txt"
    )
    reloaded = await api.reload_run_definition(run_id)
    assert reloaded["reloaded"] is True
    run = await env.store.get(run_id)
    assert run.run_id == run_id  # 同一 Run 继续，不新建
    assert run.status == "paused"  # 换装不改暂停态
    assert "fixed" in env.driver_of(run_id).dag.get_node("target")["command"]
    assert env.node_info(run_id, "upstream")["status"] == "passed"  # 节点状态保留
    assert any(
        row["event_type"] == "definition_reloaded" for row in (await api.fetch_events(run_id))[0]
    )

    # e. 异步回滚：请求（目标 scope_start 检查点）→ 先 resume 再轮询
    op = await api.request_run_rollback(
        run_id,
        node_id="target",
        client_op_id="rsi-repair-1",
        expected_epoch=target_scope["scope_epoch"],
        checkpoint_id=scope_start["checkpoint_id"],
        reason="节点累计超时，按修复后定义重建",
    )
    assert op["operation_id"] == "rsi-repair-1"
    assert op["phase"] == "pending"
    await env.coord.resume_run(run_id)
    final_op = await _wait_operation_succeeded(api, run_id, "rsi-repair-1")
    assert final_op["new_gen"] == 1

    # 幂等重放：同 client_op_id 返回原结果；旧作用域的新请求被拒
    replay = await api.request_run_rollback(
        run_id,
        node_id="target",
        client_op_id="rsi-repair-1",
        expected_epoch=target_scope["scope_epoch"],
        checkpoint_id=scope_start["checkpoint_id"],
    )
    assert replay["operation_id"] == "rsi-repair-1"
    assert replay["phase"] == "succeeded"
    with pytest.raises(ValueError, match="作用域不符"):
        await api.request_run_rollback(
            run_id,
            node_id="target",
            client_op_id="rsi-repair-2",
            expected_epoch=target_scope["scope_epoch"],
            scope_id=target_scope["scope_id"],
        )

    # 账本断言：旧作用域废弃换代、失效集外零变化、私有仓全程在场
    scopes_now = {row["scope_id"]: row for row in await api.list_run_scopes(run_id)}
    old = scopes_now[target_scope["scope_id"]]
    assert old["state"] == "discarded"
    assert old["scope_epoch"] == target_scope["scope_epoch"] + 1  # scope epoch+1
    assert scopes_now[f"{run_id}-root"]["scope_epoch"] == 1  # 未合并回滚不动集成线
    upstream_now = scopes_now[upstream_before["scope_id"]]
    for key in (
        "scope_epoch",
        "root_epoch",
        "state",
        "scope_base_commit",
        "integration_base_commit",
    ):
        assert upstream_now[key] == upstream_before[key]
    assert not old_workspace.exists()  # 旧工作区已随废弃移除
    new_target = next(
        row
        for row in scopes_now.values()
        if row["owner_ref"] == "target" and row["state"] == "active"
    )
    assert new_target["scope_id"] != target_scope["scope_id"]
    # redo on latest：新工作区含上游已集成产物（目标验收据此通过）
    new_workspace = Path(env.node_info(run_id, "target")["workspace_path"])
    assert (new_workspace / "upstream.txt").is_file()
    new_start = next(
        row
        for row in await api.list_run_checkpoints(run_id, scope_id=new_target["scope_id"])
        if row["trigger"] == "scope_start"
    )
    assert new_start["orchestration_state_ref"].startswith("driver-snapshot:")
    work_root = env.tmp / "wd"
    assert (work_root / ".workflow-controller" / "scopes" / run_id / "repo.git").is_dir()

    # 目标重跑再度通过后走已合并链受理：携带旧作用域检查点的新请求被拒
    # （旧检查点对不上当前作用域；须在整图完成前请求，run 终态后无运行时）
    async def _target_repassed() -> bool:
        return env.node_info(run_id, "target")["status"] == "passed"

    await _wait_until(_target_repassed, 15.0, "target 未再度通过")
    with pytest.raises(ValueError, match="scope_start 检查点"):
        await api.request_run_rollback(
            run_id,
            node_id="target",
            client_op_id="rsi-repair-3",
            expected_epoch=target_scope["scope_epoch"],
            scope_id=target_scope["scope_id"],
            checkpoint_id=scope_start["checkpoint_id"],
        )

    # f. 下游继续 → 整图完成：根检出（集成线物化）三节点产物齐备
    run = await await_run_status(env, run_id, {"completed"}, timeout_s=30.0)
    assert run.status == "completed"
    assert env.node_info(run_id, "target")["gen"] == 1
    for node_id in ("upstream", "target", "downstream"):
        assert env.node_info(run_id, node_id)["status"] == "passed"
    assert (work_root / "upstream.txt").read_text(encoding="utf-8") == "one\n"
    assert (work_root / "target.txt").read_text(encoding="utf-8") == "fixed\none\n"
    assert (work_root / "downstream-proof.txt").read_text(encoding="utf-8") == "fixed\none\n"
    blob = await env.coord.load_driver_snapshot(run_id)
    record = blob["state"]["operations"]["rsi-repair-1"]
    assert record["phase"] == "succeeded"
    assert record["new_gen"] == 1

    # 旧代迟到超时不触发新动作：回滚换代后旧 gen 迟到 running 事件 + 巡检
    # 不再产生新告警（同代只报一次、新代按时完成不告警）
    await env.bus.emit_ephemeral(
        Event(
            id=uuid4().hex,
            session_id="",
            iteration=0,
            timestamp=datetime.now(tz=UTC),
            event_type=NODE_HISTORY_EVENT,
            data={
                "graph_id": "late-arrival",
                "node_id": "target",
                "attempt": 1,
                "status": "running",
                "gen": 0,
                "timeout_seconds": _TIMEOUT_S,
            },
            run_id=run_id,
        )
    )
    await rule.sweep(datetime.now(tz=UTC) + timedelta(seconds=120))
    faults = [
        row
        for row in (await api.fetch_events(run_id))[0]
        if row["event_type"] == "fault.node_timeout"
    ]
    assert len(faults) == 1
