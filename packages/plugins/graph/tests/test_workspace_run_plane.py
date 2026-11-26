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

"""工作区隔离的 run 平面全链路：graph 引擎发工作区动作、协调器真实执行，

回执后 bash 进程在作用域工作区内执行、验收通过经真实 git 集成回根检出。
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


def _write_traceback_workflow(tmp: Path, nodes: list[dict]) -> Path:
    """开启工作区隔离的 bash 节点工作流。"""
    path = tmp / "wf-traceback.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "workflow": "traceback",
                "max_parallel": 2,
                "work_dir": ".",
                "traceback": {"enabled": True},
                "nodes": nodes,
            }
        ),
        encoding="utf-8",
    )
    return path


async def test_bash_run_writes_inside_scope_workspace(tmp_path: Path) -> None:
    """开启隔离的 bash run：产出经集成回根检出，作用域工作区默认释放。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _write_traceback_workflow(
        env.tmp, [{"id": "n1", "task_type": "bash", "command": "echo scoped > out.txt"}]
    )
    run_id = await env.start_graph_run(yaml_path=yaml_path)

    run = await await_run_status(env, run_id, {"completed"})
    assert run.status == "completed"

    work_root = env.tmp / "wd"
    assert (work_root / "out.txt").read_text(encoding="utf-8").strip() == "scoped"
    scopes_dir = work_root / ".workflow-controller" / "scopes" / run_id
    workspaces = scopes_dir / "workspaces"
    assert not (workspaces.exists() and any(workspaces.iterdir()))  # 集成后默认释放
    assert env.node_info(run_id, "n1")["status"] == "passed"


async def test_subgraph_child_with_colon_id_gets_valid_workspace_branch(
    tmp_path: Path,
) -> None:
    """子图子节点（ID 含冒号）建区：分支名转义后合法，run 完成且产出集成。"""
    env = GraphRunPlaneEnv(tmp_path)
    work_root = env.tmp / "wd"
    work_root.mkdir()
    (work_root / "sub.yaml").write_text(
        yaml.safe_dump(
            {
                "workflow": "sub",
                "max_parallel": 1,
                "work_dir": ".",
                "nodes": [
                    {
                        "id": "s1",
                        "task_type": "bash",
                        "command": "echo sub-done > sub-out.txt",
                        "acceptance": {"enabled": False},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    yaml_path = _write_traceback_workflow(
        env.tmp,
        [
            {
                "id": "gen",
                "task_type": "subgraph",
                "file": "sub.yaml",
                "acceptance": {"enabled": False},
            }
        ],
    )
    run_id = await env.start_graph_run(yaml_path=yaml_path)

    run = await await_run_status(env, run_id, {"completed"})

    assert run.status == "completed"
    assert (work_root / "sub-out.txt").read_text(encoding="utf-8").strip() == "sub-done"
    assert env.node_info(run_id, "gen:s1")["status"] == "passed"
    # 子节点作用域分支按转义键留存：冒号编码为 %3A，为合法分支名
    repo_git = work_root / ".workflow-controller" / "scopes" / run_id / "repo.git"
    assert (repo_git / "refs" / "heads" / "scope" / "gen%3As1-w1").is_file()


async def test_parallel_nodes_isolate_then_integrate_into_root(tmp_path: Path) -> None:
    """双节点并行写不相交路径：隔离期互不可见，依次集成后根检出含双方产出。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _write_traceback_workflow(
        env.tmp,
        [
            {"id": "n1", "task_type": "bash", "command": "echo one > n1.txt"},
            {"id": "n2", "task_type": "bash", "command": "echo two > n2.txt"},
        ],
    )
    run_id = await env.start_graph_run(yaml_path=yaml_path)

    run = await await_run_status(env, run_id, {"completed"})
    assert run.status == "completed"

    work_root = env.tmp / "wd"
    assert (work_root / "n1.txt").read_text(encoding="utf-8").strip() == "one"
    assert (work_root / "n2.txt").read_text(encoding="utf-8").strip() == "two"
    assert env.node_info(run_id, "n1")["status"] == "passed"
    assert env.node_info(run_id, "n2")["status"] == "passed"

    scopes_dir = work_root / ".workflow-controller" / "scopes" / run_id
    workspaces = scopes_dir / "workspaces"
    assert not (workspaces.exists() and any(workspaces.iterdir()))  # 双双默认释放

    # 打点落账：每节点 scope_start + terminal 各一行，编排状态引用非空
    with sqlite3.connect(env.store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT scope_id, trigger, workspace_commit, orchestration_state_ref"
            " FROM scope_checkpoints ORDER BY scope_id, trigger"
        ).fetchall()
    by_scope: dict[str, set[str]] = {}
    for row in rows:
        assert len(row["workspace_commit"]) == 40
        assert row["orchestration_state_ref"].startswith("driver-snapshot:")
        by_scope.setdefault(row["scope_id"], set()).add(row["trigger"])
    assert len(by_scope) == 2
    assert all(triggers == {"scope_start", "terminal"} for triggers in by_scope.values())


async def test_crash_recovery_rebuilds_workspace_tree_and_resumes(tmp_path: Path) -> None:
    """崩溃后重开：树运行时经深度恢复重建（检查点行 + 私有仓事实）后续跑。

    n1 已集成（账面重建保留）、n2 崩溃时在跑（重排走欠账清理：旧作用域
    废弃、新工作区重跑）；集成线与根检出原样，根代次不变。
    """
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _write_traceback_workflow(
        env.tmp,
        [
            {
                "id": "n1",
                "task_type": "bash",
                "command": "echo one > n1.txt",
                "acceptance": {"enabled": True, "type": "script", "script": "true"},
            },
            {
                "id": "n2",
                "task_type": "bash",
                "command": "sleep 300",
                "acceptance": {"enabled": False},
            },
        ],
    )
    run_id = await env.start_graph_run(yaml_path=yaml_path)

    deadline = time.monotonic() + 20.0
    while True:
        n1 = env.node_info(run_id, "n1")
        n2 = env.node_info(run_id, "n2")
        if n1["status"] == "passed" and n2["status"] == "running":
            break
        if time.monotonic() > deadline:
            pytest.fail("崩溃前现场未就绪")
        await asyncio.sleep(0.05)
    old_scopes = {s["scope_id"]: s for s in await env.coord.list_run_scopes(run_id)}
    old_n1 = next(s["scope_id"] for s in old_scopes.values() if s["owner_ref"] == "n1")
    old_n2 = next(s["scope_id"] for s in old_scopes.values() if s["owner_ref"] == "n2")

    # 模拟 kill -9：弃置环境（无 stop 收尾），同库重装配后 boot 恢复
    rebooted = GraphRunPlaneEnv(tmp_path)
    await rebooted.recover()

    # 恢复后 n2 重排：旧作用域经欠账清理废弃、新作用域建区重跑
    deadline = time.monotonic() + 30.0
    while True:
        scopes = {s["scope_id"]: s for s in await rebooted.coord.list_run_scopes(run_id)}
        new_n2 = [s for sid, s in scopes.items() if s["owner_ref"] == "n2" and sid != old_n2]
        if new_n2 and scopes[old_n2]["state"] == "discarded":
            break
        if time.monotonic() > deadline:
            pytest.fail("恢复后未完成旧作用域废弃与新工作区重建")
        await asyncio.sleep(0.05)

    # 账面重建断言：n1 已集成的行保留、根代次不变、n2 旧作用域换代废弃
    assert scopes[old_n1]["state"] in ("integrated", "released")
    assert (
        scopes[old_n1]["integration_base_commit"] == old_scopes[old_n1]["integration_base_commit"]
    )
    assert scopes[f"{run_id}-root"]["root_epoch"] == 1
    assert scopes[old_n2]["scope_epoch"] == 2
    assert new_n2[0]["state"] == "active"

    await rebooted.coord.stop_run(run_id)
    # 集成线与根检出经恢复原样：n1 产出仍在
    work_root = tmp_path / "wd"
    assert (work_root / "n1.txt").read_text(encoding="utf-8").strip() == "one"
