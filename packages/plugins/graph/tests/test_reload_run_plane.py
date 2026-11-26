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

"""当前定义重载的 run 平面全链路：暂停态受理、原 Run 原地换装续跑。

RSI 修复流的产品路径：暂停 → 工作流源文件被 patch → 重读当前定义换装
（保留节点状态与产物、不新建 Run）→ resume 用新定义继续；坏定义、删除
既有节点、运行中重载均被拒绝且 Run 保持原定义原状态。
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from typing import TYPE_CHECKING

import pytest
import yaml

from .run_plane_helpers import GraphRunPlaneEnv, await_run_status

if TYPE_CHECKING:
    from pathlib import Path


def _n1() -> dict:
    """上游节点：快速写标记文件并通过验收。"""
    return {
        "id": "n1",
        "task_type": "bash",
        "command": "echo one > n1.txt",
        "acceptance": {"enabled": True, "type": "script", "script": "true"},
    }


def _n2(command: str, script: str = "true") -> dict:
    """下游节点：首轮长眠占位，重载后可换成快速命令。"""
    return {
        "id": "n2",
        "task_type": "bash",
        "command": command,
        "depends_on": ["n1"],
        "acceptance": {"enabled": True, "type": "script", "script": script},
    }


def _write_workflow(path: Path, nodes: list[dict], *, traceback: bool = False) -> Path:
    """写工作流 yaml（同一文件原地改写，模拟 RSI 对源分支打 patch）。"""
    cfg: dict[str, object] = {
        "workflow": "reload-demo",
        "max_parallel": 2,
        "work_dir": ".",
        "nodes": nodes,
    }
    if traceback:
        cfg["traceback"] = {"enabled": True}
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return path


async def _wait_until(predicate, timeout_s: float, message: str) -> None:
    """轮询等待同步条件成立（失败即 pytest.fail）。"""
    deadline = time.monotonic() + timeout_s
    while True:
        if predicate():
            return
        if time.monotonic() > deadline:
            pytest.fail(message)
        await asyncio.sleep(0.05)


def _n2_executing(env: GraphRunPlaneEnv, run_id: str) -> bool:
    """n2 已在执行且 n1 已通过（可安全暂停的现场）。"""
    return (
        env.node_info(run_id, "n2")["status"] == "running"
        and env.node_info(run_id, "n1")["status"] == "passed"
    )


async def _start_paused_demo(tmp_path: Path) -> tuple[GraphRunPlaneEnv, str, Path]:
    """起一个 n2 长眠的 run 并暂停（重载测试的公共现场）。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _write_workflow(tmp_path / "wf-reload.yaml", [_n1(), _n2("sleep 300")])
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _n2_executing(env, run_id), 15.0, "n2 未在限期内进入执行")
    await env.coord.pause_run(run_id)
    return env, run_id, yaml_path


async def test_reload_updates_existing_run_and_continues_with_new_definition(
    tmp_path: Path,
) -> None:
    """全链路：暂停期改写源文件 → 原 Run 换装 → resume 用新定义完成。"""
    env, run_id, yaml_path = await _start_paused_demo(tmp_path)
    nodes_before = env.driver_of(run_id).state.snapshot()["nodes"]
    n1_before = env.node_info(run_id, "n1")

    _write_workflow(yaml_path, [_n1(), _n2("echo two > n2.txt", script="test -f n2.txt")])
    result = await env.coord.reload_run_definition(run_id)

    assert result["reloaded"] is True
    run = await env.store.get(run_id)
    assert run.status == "paused"  # 换装不改暂停态
    assert env.driver_of(run_id).state.snapshot()["nodes"] == nodes_before  # 节点状态零重置
    assert env.node_info(run_id, "n1") == n1_before
    assert env.driver_of(run_id).dag.get_node("n2")["command"] == "echo two > n2.txt"
    # 冻结副本随定义更新：后续恢复不读外部漂移
    frozen = env.coord.run_data_root(run_id) / "program" / yaml_path.name
    assert frozen.read_text(encoding="utf-8") == yaml_path.read_text(encoding="utf-8")

    await env.coord.resume_run(run_id)
    run = await await_run_status(env, run_id, {"completed"}, timeout_s=30.0)
    assert run.run_id == run_id  # 同一 Run 继续，不新建
    assert [r.run_id for r in await env.coord.list_runs() if r.kind == "workflow"] == [run_id]
    work_root = env.tmp / "wd"
    assert (work_root / "n2.txt").read_text(encoding="utf-8").strip() == "two"
    assert env.node_info(run_id, "n2")["status"] == "passed"
    assert env.node_info(run_id, "n1")["status"] == "passed"
    # 重载事件留痕（RSI 观察面）
    with sqlite3.connect(env.store.db_path) as conn:
        rows = conn.execute(
            "SELECT event_type FROM run_events WHERE run_id = ?", (run_id,)
        ).fetchall()
    assert any(row[0] == "definition_reloaded" for row in rows)


async def test_reload_rejected_while_running(tmp_path: Path) -> None:
    """运行中（非暂停）重载被拒绝。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _write_workflow(tmp_path / "wf-reload.yaml", [_n1(), _n2("sleep 300")])
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _n2_executing(env, run_id), 15.0, "n2 未在限期内进入执行")

    with pytest.raises(ValueError, match="仅暂停态受理"):
        await env.coord.reload_run_definition(run_id)

    await env.coord.stop_run(run_id)


async def test_failed_reload_keeps_original_definition_and_state(tmp_path: Path) -> None:
    """坏 YAML 重载失败：Run 保持原定义原状态（不带半个现场继续）。"""
    env, run_id, yaml_path = await _start_paused_demo(tmp_path)
    nodes_before = env.driver_of(run_id).state.snapshot()["nodes"]
    frozen = env.coord.run_data_root(run_id) / "program" / yaml_path.name
    frozen_before = frozen.read_text(encoding="utf-8")

    yaml_path.write_text("nodes: [unclosed", encoding="utf-8")
    with pytest.raises(ValueError, match="工作流定义重读失败"):
        await env.coord.reload_run_definition(run_id)

    run = await env.store.get(run_id)
    assert run.status == "paused"
    assert env.driver_of(run_id).state.snapshot()["nodes"] == nodes_before
    assert env.driver_of(run_id).dag.get_node("n2")["command"] == "sleep 300"
    assert frozen.read_text(encoding="utf-8") == frozen_before  # 冻结副本零改写
    await env.coord.stop_run(run_id)


async def test_reload_rejects_definition_removing_existing_node(tmp_path: Path) -> None:
    """新定义删除既有节点被拒：错误点名节点，定义与状态保持。"""
    env, run_id, yaml_path = await _start_paused_demo(tmp_path)
    nodes_before = env.driver_of(run_id).state.snapshot()["nodes"]
    frozen = env.coord.run_data_root(run_id) / "program" / yaml_path.name
    frozen_before = frozen.read_text(encoding="utf-8")

    _write_workflow(yaml_path, [_n1()])  # n2 被移除
    with pytest.raises(ValueError, match="既有状态无法保留") as excinfo:
        await env.coord.reload_run_definition(run_id)

    assert "n2" in str(excinfo.value)
    assert "n2" in env.driver_of(run_id).dag.all_ids()
    assert env.driver_of(run_id).state.snapshot()["nodes"] == nodes_before
    assert frozen.read_text(encoding="utf-8") == frozen_before  # 拒绝路径不触碰冻结副本
    await env.coord.stop_run(run_id)


async def test_reload_with_explicit_source_path(tmp_path: Path) -> None:
    """显式新路径重载：按新位置翻译换装，原 program_ref 不改写。"""
    env, run_id, _ = await _start_paused_demo(tmp_path)
    other = _write_workflow(tmp_path / "wf-other.yaml", [_n1(), _n2("echo alt > n2.txt")])

    result = await env.coord.reload_run_definition(run_id, other)

    assert result["reloaded"] is True
    assert env.driver_of(run_id).dag.get_node("n2")["command"] == "echo alt > n2.txt"
    run = await env.store.get(run_id)
    assert run.program_ref.endswith("wf-reload.yaml")  # Run 身份字段不变
    await env.coord.stop_run(run_id)


async def test_reload_preserves_integrated_artifacts_with_traceback(tmp_path: Path) -> None:
    """开启隔离的 run 重载：集成成果与节点映射档案保留，resume 重建工作区续跑。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _write_workflow(
        tmp_path / "wf-reload.yaml", [_n1(), _n2("sleep 300")], traceback=True
    )
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _n2_executing(env, run_id), 15.0, "n2 未在限期内进入执行")
    await env.coord.pause_run(run_id)

    n1_mapping = env.driver_of(run_id).state.node_workspace("n1")
    work_root = env.tmp / "wd"
    n1_content = (work_root / "n1.txt").read_text(encoding="utf-8")

    _write_workflow(yaml_path, [_n1(), _n2("echo two > n2.txt")], traceback=True)
    result = await env.coord.reload_run_definition(run_id)

    assert result["reloaded"] is True
    # 已集成节点的映射档案与根检出成果零变化
    assert env.driver_of(run_id).state.node_workspace("n1") == n1_mapping
    assert (work_root / "n1.txt").read_text(encoding="utf-8") == n1_content

    await env.coord.resume_run(run_id)
    await await_run_status(env, run_id, {"completed"}, timeout_s=30.0)
    assert (work_root / "n2.txt").read_text(encoding="utf-8").strip() == "two"
    assert env.node_info(run_id, "n1")["status"] == "passed"
    assert env.node_info(run_id, "n2")["status"] == "passed"
