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

"""未合并节点回滚的 run 平面全链路（真实 git）。

暂停中的 run 受理回滚 op，恢复后异步链推进：废弃目标作用域（scope
epoch+1）→ 失效计划落账 → 重排建区（基线 = 当前集成头，redo on
latest）→ 节点重跑；失效集外作用域与文件零变化、旧作用域/旧代次请求拒绝。
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest
import yaml

from .run_plane_helpers import GraphRunPlaneEnv, await_run_status


def _rollback_workflow(tmp: Path) -> Path:
    """n1 快速集成；n2 首轮无 n1 产出即长眠等待回滚，重跑轮写出可见标记。"""
    path = tmp / "wf-rollback.yaml"
    nodes = [
        {
            "id": "n1",
            "task_type": "bash",
            "command": "echo one > n1.txt",
            "acceptance": {"enabled": True, "type": "script", "script": "true"},
        },
        {
            "id": "n2",
            "task_type": "bash",
            "command": (
                "if [ -f n1.txt ]; then sleep 3; echo seen > n2-sees.txt; else sleep 300; fi"
            ),
            "acceptance": {"enabled": True, "type": "script", "script": "true"},
        },
    ]
    path.write_text(
        yaml.safe_dump(
            {
                "workflow": "rollback-demo",
                "max_parallel": 2,
                "work_dir": ".",
                "traceback": {"enabled": True},
                "nodes": nodes,
            }
        ),
        encoding="utf-8",
    )
    return path


async def _wait_until(predicate, timeout_s: float, message: str) -> None:
    """轮询等待异步条件成立（失败即 pytest.fail）。"""
    deadline = time.monotonic() + timeout_s
    while True:
        if await predicate():
            return
        if time.monotonic() > deadline:
            pytest.fail(message)
        await asyncio.sleep(0.05)


async def test_unmerged_rollback_rebuilds_on_latest_integration_head(tmp_path: Path) -> None:
    """全链路：暂停受理回滚 → 恢复推进异步链 → 新 attempt 基线含兄弟已集成成果。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _rollback_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)

    # 两节点同轮建区（互不可见）；n1 先集成通过后 n2 仍在首轮工作区长眠
    await _wait_until(
        lambda: _n2_first_attempt_running(env, run_id),
        15.0,
        "n2 未在限期内进入首轮执行",
    )
    # 暂停杀掉 n2 的首轮进程（组杀回执滞留 FIFO，恢复轮才消费——断点由
    # 恢复复位与回滚重排共同收敛，不影响链路）
    await env.coord.pause_run(run_id)

    # 回滚前快照：根检出文件、作用域账面与 n1 检查点行（零变化比对基准）
    work_root = env.tmp / "wd"
    pre_files = {p.name: p.read_text(encoding="utf-8") for p in work_root.iterdir() if p.is_file()}
    pre_scopes = {s["scope_id"]: s for s in await env.coord.list_run_scopes(run_id)}
    n1_scope = next(s for s in pre_scopes.values() if s["owner_ref"] == "n1")
    n2_scope_id = next(s["scope_id"] for s in pre_scopes.values() if s["owner_ref"] == "n2")
    n2_old_workspace = Path(
        next(s for s in pre_scopes.values() if s["owner_ref"] == "n2")["workspace_path"] or ""
    )
    pre_n1_checkpoints = await env.coord.list_run_checkpoints(run_id, scope_id=n1_scope["scope_id"])

    # 暂停期受理回滚 op：只登记立即返回 operation_id（链待恢复后推进）
    result = await env.coord.request_driver_op(
        run_id,
        "rollback_node",
        {"node_id": "n2", "client_op_id": "rsi-1", "expected_epoch": 1},
    )
    assert result["operation_id"] == "rsi-1"
    status = await env.coord.request_driver_op(run_id, "get_operation", {"operation_id": "rsi-1"})
    assert status["phase"] == "pending"

    await env.coord.resume_run(run_id)
    await _wait_until(
        lambda: _operation_succeeded(env, run_id),
        30.0,
        "回滚操作未在限期内完成",
    )

    # 旧作用域/旧代次请求拒绝：重建后映射指向新作用域
    with pytest.raises(ValueError, match="作用域不符"):
        await env.coord.request_driver_op(
            run_id,
            "rollback_node",
            {
                "node_id": "n2",
                "client_op_id": "rsi-2",
                "expected_epoch": 1,
                "scope_id": n2_scope_id,
            },
        )
    # 幂等重放：同键请求返回原操作与当前状态
    replay = await env.coord.request_driver_op(
        run_id,
        "rollback_node",
        {"node_id": "n2", "client_op_id": "rsi-1", "expected_epoch": 1},
    )
    assert replay["operation_id"] == "rsi-1"
    assert replay["phase"] == "succeeded"

    # 账本断言：旧作用域废弃换代、集成线与失效集外作用域零变化
    scopes = {s["scope_id"]: s for s in await env.coord.list_run_scopes(run_id)}
    old = scopes[n2_scope_id]
    assert old["state"] == "discarded"
    assert old["scope_epoch"] == 2  # scope epoch+1
    assert scopes[f"{run_id}-root"]["scope_epoch"] == 1  # 未合并回滚不动集成线
    n1_post = scopes[n1_scope["scope_id"]]
    for key in (
        "scope_epoch",
        "root_epoch",
        "state",
        "scope_base_commit",
        "integration_base_commit",
    ):
        assert n1_post[key] == n1_scope[key]
    assert not n2_old_workspace.exists()  # 旧工作区已随废弃移除
    # n1 检查点行零变化（失效集外账本双断言之检查点侧）
    post_n1_checkpoints = await env.coord.list_run_checkpoints(
        run_id, scope_id=n1_scope["scope_id"]
    )
    assert post_n1_checkpoints == pre_n1_checkpoints

    run = await await_run_status(env, run_id, {"completed"}, timeout_s=30.0)
    assert run.status == "completed"

    # 文件断言：既有文件零变化；n2 重跑轮看到 n1 已集成产出（redo on latest）
    final_names = {p.name for p in work_root.iterdir() if p.is_file()}
    assert set(pre_files) <= final_names
    for name, content in pre_files.items():
        assert (work_root / name).read_text(encoding="utf-8") == content
    assert (work_root / "n1.txt").read_text(encoding="utf-8").strip() == "one"
    assert (work_root / "n2-sees.txt").read_text(encoding="utf-8").strip() == "seen"
    n2_info = env.node_info(run_id, "n2")
    assert n2_info["status"] == "passed"
    assert n2_info["gen"] == 1
    assert env.node_info(run_id, "n1")["status"] == "passed"
    blob = await env.coord.load_driver_snapshot(run_id)
    op_record = blob["state"]["operations"]["rsi-1"]
    assert op_record["phase"] == "succeeded"
    assert op_record["new_gen"] == 1


async def _n2_first_attempt_running(env, run_id: str) -> bool:
    """n2 已在首轮工作区执行且 n1 已集成通过。"""
    n2 = env.node_info(run_id, "n2")
    return bool(n2.get("workspace_path")) and env.node_info(run_id, "n1")["status"] == "passed"


async def _operation_succeeded(env, run_id: str) -> bool:
    """持久化快照中回滚操作已达完成（重排建区回执时刻）。"""
    blob = await env.coord.load_driver_snapshot(run_id)
    record = (blob.get("state", {}).get("operations") or {}).get("rsi-1")
    return bool(record) and record.get("phase") == "succeeded"


def _integrated_rollback_workflow(tmp: Path) -> Path:
    """n1/n2 同基线并行集成；n3 依赖 n1（基线谱系含 n1 集成）；n4 长眠保活现场。"""
    path = tmp / "wf-integrated-rollback.yaml"
    nodes = [
        {
            "id": "n1",
            "task_type": "bash",
            "command": "echo one > n1.txt",
            "acceptance": {"enabled": True, "type": "script", "script": "true"},
        },
        {
            "id": "n2",
            "task_type": "bash",
            "command": "echo two > n2.txt",
            "acceptance": {"enabled": True, "type": "script", "script": "true"},
        },
        {
            "id": "n3",
            "task_type": "bash",
            "command": "cat n1.txt > n3.txt",
            "depends_on": ["n1"],
            "acceptance": {"enabled": True, "type": "script", "script": "true"},
        },
        {
            "id": "n4",
            "task_type": "bash",
            "command": "sleep 300",
            "acceptance": {"enabled": False},
        },
    ]
    path.write_text(
        yaml.safe_dump(
            {
                "workflow": "integrated-rollback-demo",
                "max_parallel": 3,
                "work_dir": ".",
                "traceback": {"enabled": True},
                "nodes": nodes,
            }
        ),
        encoding="utf-8",
    )
    return path


async def _wait_all_passed(env, run_id: str) -> None:
    """等待 n1/n2/n3 全部集成通过、n4 在跑（rollback 的前置现场）。"""

    async def ready() -> bool:
        return all(env.node_info(run_id, nid)["status"] == "passed" for nid in ("n1", "n2", "n3"))

    await _wait_until(ready, 30.0, "n1/n2/n3 未在限期内全部通过")


async def test_integrated_rollback_retracts_line_and_reruns_only_lineage_set(
    tmp_path: Path,
) -> None:
    """已合并回滚全链路：集成线回退到目标基线、保留者不重跑、失效集换代重跑。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _integrated_rollback_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_all_passed(env, run_id)

    scopes = {s["scope_id"]: s for s in await env.coord.list_run_scopes(run_id)}
    n1_scope_id = next(s["scope_id"] for s in scopes.values() if s["owner_ref"] == "n1")
    n2_scope_id = next(s["scope_id"] for s in scopes.values() if s["owner_ref"] == "n2")
    n2_before = dict(scopes[n2_scope_id])

    # 既定顺序：暂停受理回滚 op（登记即返回），恢复后异步链推进
    await env.coord.pause_run(run_id)
    result = await env.coord.request_driver_op(
        run_id,
        "rollback_node",
        {
            "node_id": "n1",
            "client_op_id": "rsi-i1",
            "expected_epoch": 1,
            "expected_root_epoch": 1,
        },
    )
    assert result["kind"] == "rollback_integrated"
    await env.coord.resume_run(run_id)

    await _wait_until(
        lambda: _snapshot_op(env, run_id, "rsi-i1", "succeeded"), 60.0, "已合并回滚操作未完成"
    )
    # 账本：根代次 +1；目标与下游换代废弃；保留者（n2）零变化
    scopes = {s["scope_id"]: s for s in await env.coord.list_run_scopes(run_id)}
    assert scopes[f"{run_id}-root"]["root_epoch"] == 2
    assert scopes[n1_scope_id]["state"] == "discarded"
    assert scopes[n1_scope_id]["scope_epoch"] == 2
    n2_after = scopes[n2_scope_id]
    for key in ("scope_epoch", "root_epoch", "state", "integration_base_commit"):
        assert n2_after[key] == n2_before[key], key

    # 幂等重放返回原操作
    replay = await env.coord.request_driver_op(
        run_id,
        "rollback_node",
        {
            "node_id": "n1",
            "client_op_id": "rsi-i1",
            "expected_epoch": 1,
            "expected_root_epoch": 1,
        },
    )
    assert replay["operation_id"] == "rsi-i1"
    assert replay["phase"] == "succeeded"
    assert replay["new_root_epoch"] == 2

    # 旧根代次的回滚请求一律拒绝：保留者 n2 的回退被核心按代次纪律挡下
    await env.coord.request_driver_op(
        run_id,
        "rollback_node",
        {
            "node_id": "n2",
            "client_op_id": "rsi-i2",
            "expected_epoch": 1,
            "expected_root_epoch": 1,
        },
    )
    await _wait_until(
        lambda: _snapshot_op(env, run_id, "rsi-i2", "failed"), 30.0, "旧根代次请求未按期拒绝"
    )
    failed_view = await env.coord.request_driver_op(
        run_id, "get_operation", {"operation_id": "rsi-i2"}
    )
    assert "根代次不匹配" in (failed_view["error"] or "")

    # 失效重跑 / 保留不重跑：n1、n3 换代重跑并重新集成，n2 原代原状态保持
    async def lineage_rerun_done() -> bool:
        n1 = env.node_info(run_id, "n1")
        n3 = env.node_info(run_id, "n3")
        return (
            n1["gen"] == 1
            and n1["status"] == "passed"
            and n3["gen"] == 1
            and n3["status"] == "passed"
        )

    await _wait_until(lineage_rerun_done, 60.0, "失效集未在限期内完成重跑")
    n2_info = env.node_info(run_id, "n2")
    assert n2_info["status"] == "passed"
    assert n2_info["gen"] == 0

    await env.coord.stop_run(run_id)
    work_root = env.tmp / "wd"
    for name, content in (("n1.txt", "one"), ("n2.txt", "two"), ("n3.txt", "one")):
        assert (work_root / name).read_text(encoding="utf-8").strip() == content


async def _snapshot_op(env, run_id: str, operation_id: str, phase: str) -> bool:
    """持久化快照中指定操作已达目标相位。"""
    blob = await env.coord.load_driver_snapshot(run_id)
    record = (blob.get("state", {}).get("operations") or {}).get(operation_id)
    return bool(record) and record.get("phase") == phase
