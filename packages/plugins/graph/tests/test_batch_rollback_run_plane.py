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

"""批量节点回滚的 run 平面全链路回归（真实 git 与真实进程）。

混合批量端到端（受理 → 集成线回退 → 谱系失效与吸收 → 废弃切片 → 重建
→ 整图完成）、归并等价性、原子拒绝零改动、并发交错防护、迟到回执落账
与各相位崩溃注入后的幂等收口。命令自证回滚轮次：首轮工作区无标记文件
即长眠/失败，回滚重建后的工作区含上游产出即快速通过。
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest
import yaml

from .run_plane_helpers import GraphRunPlaneEnv, await_run_status

if TYPE_CHECKING:
    from pathlib import Path

# 免验收脚本：bash 节点过 script 验收后集成（隔离链路最短）
_PASSING_ACC = {"enabled": True, "type": "script", "script": "true"}


def _bash_node(node_id: str, command: str, **extra: Any) -> dict:
    """通过 script 验收的 bash 节点清单项。"""
    return {
        "id": node_id,
        "task_type": "bash",
        "command": command,
        "acceptance": dict(_PASSING_ACC),
        **extra,
    }


def _write_workflow(path: Path, nodes: list[dict], *, max_parallel: int = 4) -> Path:
    """落盘工作流（开启 traceback 工作区隔离）。"""
    path.write_text(
        yaml.safe_dump(
            {
                "workflow": "batch-rollback",
                "max_parallel": max_parallel,
                "work_dir": ".",
                "traceback": {"enabled": True},
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


async def _op_record(env: GraphRunPlaneEnv, run_id: str, operation_id: str) -> dict | None:
    """持久化快照中的操作账原始记录。"""
    blob = await env.coord.load_driver_snapshot(run_id)
    return (blob.get("state", {}).get("operations") or {}).get(operation_id)


async def _wait_op_phase(env: GraphRunPlaneEnv, run_id: str, operation_id: str, phase: str) -> dict:
    """轮询等待操作到达目标相位，返回最终记录。"""
    deadline = time.monotonic() + 90.0
    while True:
        record = await _op_record(env, run_id, operation_id)
        if record and record.get("phase") == phase:
            return record
        if record and record.get("phase") == "failed":
            pytest.fail(f"操作失败：{record.get('error')}")
        if time.monotonic() > deadline:
            pytest.fail(f"操作未在限期内到达 {phase}（当前 {record and record.get('phase')}）")
        await asyncio.sleep(0.05)


def _scope_row(owner: str, rows: dict) -> dict:
    """按 owner 取作用域账面行。"""
    return next(row for row in rows.values() if row["owner_ref"] == owner)


async def _run_scopes(env: GraphRunPlaneEnv, run_id: str) -> dict[str, dict]:
    """作用域账面（scope_id 键）。"""
    return {row["scope_id"]: row for row in await env.coord.list_run_scopes(run_id)}


def _rollback_edge_counts(env: GraphRunPlaneEnv, run_id: str) -> Counter:
    """回滚边多重集（from, from_gen, to, to_gen）——重复落账会翻倍。"""
    blob = env.driver_of(run_id).state.snapshot()
    return Counter(
        (e["from"], e["from_gen"], e["to"], e["to_gen"]) for e in blob.get("rollback_edges", [])
    )


def _mixed_batch_workflow(tmp: Path) -> Path:
    """混合批量现场。

    p1 已集成（回退锚点，首轮 one-before / 重跑 one-after 自证回退）、
    p2 保留者（同基线并行集成）、d1 依赖 p1（谱系失效）、u1 未合并目标
    （基线含 p1 集成 → 回退连带吸收）、u2 未合并目标（独立基线 → 走废弃
    切片，f 失败闸门在恢复期挡住重排抢跑）、d2 为 u2 下游。
    """
    nodes = [
        _bash_node(
            "p1",
            "if [ -f p2.txt ]; then echo one-after > p1.txt; else echo one-before > p1.txt; fi",
        ),
        _bash_node("p2", "echo two > p2.txt"),
        _bash_node("f", "if [ -f p2.txt ]; then echo f-ok > f.txt; else exit 1; fi"),
        _bash_node("d1", "cat p1.txt > d1.txt", depends_on=["p1"]),
        _bash_node(
            "u1",
            "if grep -q one-after d1.txt; then echo u1-done > u1.txt; else exit 1; fi",
            depends_on=["d1"],
        ),
        _bash_node(
            "u2",
            "if [ -f f.txt ]; then echo u2-done > u2.txt; else sleep 300; fi",
            depends_on=["f"],
            run_on_failure=True,
        ),
        _bash_node("d2", "cat u2.txt > d2.txt", depends_on=["u2"]),
    ]
    return _write_workflow(tmp / "wf-batch-mixed.yaml", nodes)


async def _mixed_scene_ready(env: GraphRunPlaneEnv, run_id: str) -> bool:
    """混合现场就绪：p1/p2/d1 已集成、f/u1 已失败、u2 长眠保活。"""
    return (
        all(env.node_info(run_id, n)["status"] == "passed" for n in ("p1", "p2", "d1"))
        and env.node_info(run_id, "f")["status"] == "failed"
        and env.node_info(run_id, "u1")["status"] == "failed"
        and env.node_info(run_id, "u2")["status"] == "running"
        and bool(env.node_info(run_id, "u2").get("workspace_path"))
    )


async def _request_batch_rollback(
    env: GraphRunPlaneEnv,
    run_id: str,
    targets: list[dict],
    operation_id: str,
    *,
    expected_root_epoch: int | None = None,
    reason: str = "",
) -> dict:
    """经 run 平面 op 通道请求批量回滚。"""
    args: dict[str, Any] = {"targets": targets, "client_op_id": operation_id}
    if expected_root_epoch is not None:
        args["expected_root_epoch"] = expected_root_epoch
    if reason:
        args["reason"] = reason
    return await env.coord.request_driver_op(run_id, "rollback_nodes", args)


def _assert_mixed_final_ledger(env: GraphRunPlaneEnv, run_id: str, record: dict) -> None:
    """混合链收口的账面终态（混合端到端与崩溃注入共用，须在整图完成后调用）。"""
    assert record["phase"] == "succeeded"
    assert record["new_gen"] == 1
    assert record["new_root_epoch"] == 2
    assert record["merged_from"] == {"u1": "集成线回退"}
    assert record["slices"]["u2"]["gen"] == 1
    assert record["covered"] == ["d1", "d2", "p1", "u1", "u2"]
    # 代次：回退谱系集（p1/d1/u1）与切片集（u2/d2）全部进新代；f 从未被
    # 回滚重置（恢复期重跑属普通重排，gen 不动）
    for node_id in ("p1", "d1", "u1", "u2", "d2"):
        assert env.node_info(run_id, node_id)["gen"] == 1, node_id
    assert env.node_info(run_id, "f")["gen"] == 0
    # 回滚边各一条（重复落账会翻倍）；f 无边（未参与回滚）
    edges = _rollback_edge_counts(env, run_id)
    assert edges == Counter({("p1", 0, "p1", 1): 1, ("u1", 0, "u1", 1): 1, ("u2", 0, "u2", 1): 1})
    # 归档实例键不碰撞：每个被重置节点恰好归档一次
    retired = set(env.driver_of(run_id).state.snapshot()["retired_instances"])
    assert retired == {"p1#0", "d1#0", "u1#0", "u2#0", "d2#0"}
    for node_id in ("p1", "p2", "f", "d1", "u1", "u2", "d2"):
        assert env.node_info(run_id, node_id)["status"] == "passed", node_id
    # 根检出：保留者原样、被撤目标旧产出（one-before）不在、新代产出齐备
    work_root = env.tmp / "wd"
    contents = {
        p.name: p.read_text(encoding="utf-8").strip() for p in work_root.iterdir() if p.is_file()
    }
    assert contents == {
        "p1.txt": "one-after",
        "p2.txt": "two",
        "d1.txt": "one-after",
        "u1.txt": "u1-done",
        "u2.txt": "u2-done",
        "d2.txt": "u2-done",
        "f.txt": "f-ok",
    }


async def test_mixed_batch_end_to_end_full_chain(tmp_path: Path) -> None:
    """混合批量全链路：暂停受理 → 回退（最早基线 + 保留者重放 + 谱系吸收）
    → 未合并目标废弃切片 → 重建重跑 → 下游继续 → 整图完成；同键重放返原。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _mixed_batch_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _mixed_scene_ready(env, run_id), 30.0, "混合批量现场未在限期内就绪")

    # 回滚前快照：保留者作用域行与根检出（零变化比对基准）
    pre_scopes = await _run_scopes(env, run_id)
    p2_before = dict(_scope_row("p2", pre_scopes))
    p1_old = _scope_row("p1", pre_scopes)["scope_id"]
    u1_old = _scope_row("u1", pre_scopes)["scope_id"]
    u2_old = _scope_row("u2", pre_scopes)["scope_id"]
    d1_old = _scope_row("d1", pre_scopes)["scope_id"]

    # 暂停态受理：只登记立即返回，携带混合目标（已合并 p1 + 未合并 u1/u2）
    await env.coord.pause_run(run_id)
    result = await _request_batch_rollback(
        env,
        run_id,
        [
            {"node_id": "p1", "expected_epoch": 1},
            {"node_id": "u1", "expected_epoch": 1},
            {"node_id": "u2", "expected_epoch": 1},
        ],
        "mix-1",
        expected_root_epoch=1,
        reason="批量混合回滚",
    )
    assert result["kind"] == "rollback_nodes"
    assert result["phase"] == "pending"
    assert result["targets"] == ["p1", "u1", "u2"]
    assert result["merged_from"] == {}
    await env.coord.resume_run(run_id)

    record = await _wait_op_phase(env, run_id, "mix-1", "succeeded")

    # 同键重放幂等返原（run 终态后 op 通道无运行时，须在完成前请求）：
    # 目标已换映射，仍返回原操作与当前状态
    replay = await _request_batch_rollback(
        env,
        run_id,
        [
            {"node_id": "p1", "expected_epoch": 1},
            {"node_id": "u1", "expected_epoch": 1},
            {"node_id": "u2", "expected_epoch": 1},
        ],
        "mix-1",
        expected_root_epoch=1,
    )
    assert replay["operation_id"] == "mix-1"
    assert replay["phase"] == "succeeded"
    assert replay["merged_from"] == {"u1": "集成线回退"}

    # 作用域账本（run 终态后树运行时退出、账面不可查，须在完成前断言）：
    # 根代次 +1；被撤/吸收/切片目标旧作用域废弃换代；保留者 p2 零变化
    scopes = await _run_scopes(env, run_id)
    assert scopes[f"{run_id}-root"]["root_epoch"] == 2
    for old_id in (p1_old, d1_old, u1_old, u2_old):
        assert scopes[old_id]["state"] == "discarded"
        assert scopes[old_id]["scope_epoch"] == 2
    p2_after = _scope_row("p2", scopes)
    for key in ("scope_epoch", "root_epoch", "state", "integration_base_commit"):
        assert p2_after[key] == p2_before[key], key

    # 重建产生新作用域：各回滚目标档案映射不再指向旧作用域
    for owner, old_id in (("p1", p1_old), ("u1", u1_old), ("u2", u2_old)):
        assert env.node_info(run_id, owner)["workspace_scope_id"] != old_id

    run = await await_run_status(env, run_id, {"completed"}, timeout_s=60.0)
    assert run.status == "completed"
    _assert_mixed_final_ledger(env, run_id, record)


async def _run_scene_to_completion(
    tmp: Path,
    build_workflow: Any,
    pre_ready: Any,
    request_rollback: Any,
    operation_id: str,
) -> tuple[GraphRunPlaneEnv, str, dict]:
    """跑完一个回滚场景：起 run → 等现场 → 暂停受理 → 恢复 → 操作与整图完成。"""
    env = GraphRunPlaneEnv(tmp)
    yaml_path = build_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: pre_ready(env, run_id), 30.0, "回滚现场未在限期内就绪")
    await env.coord.pause_run(run_id)
    await request_rollback(env, run_id)
    await env.coord.resume_run(run_id)
    record = await _wait_op_phase(env, run_id, operation_id, "succeeded")
    run = await await_run_status(env, run_id, {"completed"}, timeout_s=60.0)
    assert run.status == "completed"
    return env, run_id, record


def _execution_ledger(env: GraphRunPlaneEnv, run_id: str) -> tuple:
    """执行结果口径：节点态/代次、回滚预算、回滚边多重集、归档实例键。"""
    snap = env.driver_of(run_id).state.snapshot()
    return (
        {nid: (info["status"], info["gen"]) for nid, info in snap["nodes"].items()},
        snap.get("rollbacks", {}),
        sorted(
            (e["from"], e["from_gen"], e["to"], e["to_gen"]) for e in snap.get("rollback_edges", [])
        ),
        set(snap.get("retired_instances", {})),
    )


def _root_checkout(env: GraphRunPlaneEnv) -> dict[str, str]:
    """根检出文件内容（集成线物化面）。"""
    work_root = env.tmp / "wd"
    return {
        p.name: p.read_text(encoding="utf-8").strip() for p in work_root.iterdir() if p.is_file()
    }


def _merge_equivalence_workflow(tmp: Path) -> Path:
    """未合并归并现场：u 失败（回滚锚点）、v 为其后代且长眠（未合并目标）。"""
    nodes = [
        _bash_node("w", "echo w-ok > w.txt"),
        _bash_node("u", "if [ -f w.txt ]; then echo u-ok > u.txt; else exit 1; fi"),
        _bash_node(
            "v",
            "if [ -f u.txt ]; then echo v-ok > v.txt; else sleep 300; fi",
            depends_on=["u"],
            run_on_failure=True,
        ),
    ]
    return _write_workflow(tmp / "wf-batch-merge.yaml", nodes)


async def _merge_scene_ready(env: GraphRunPlaneEnv, run_id: str) -> bool:
    """w 已集成、u 失败、v 长眠保活（未合并后代目标现场）。"""
    return (
        env.node_info(run_id, "w")["status"] == "passed"
        and env.node_info(run_id, "u")["status"] == "failed"
        and env.node_info(run_id, "v")["status"] == "running"
    )


async def test_batch_with_descendant_equivalent_to_ancestor_only(
    tmp_path: Path,
) -> None:
    """归并等价性：batch([u, v])（v 为 u 的未合并后代）与 batch([u]) 执行
    结果等价（reset 集/代次/回滚边/根检出一致），仅归并账可审计地不同。"""
    (tmp_path / "with-descendant").mkdir()
    (tmp_path / "ancestor-only").mkdir()

    async def request_both(env: GraphRunPlaneEnv, run_id: str) -> dict:
        return await _request_batch_rollback(
            env,
            run_id,
            [{"node_id": "u", "expected_epoch": 1}, {"node_id": "v", "expected_epoch": 1}],
            "merge-1",
        )

    async def request_anchor(env: GraphRunPlaneEnv, run_id: str) -> dict:
        return await _request_batch_rollback(
            env, run_id, [{"node_id": "u", "expected_epoch": 1}], "merge-1"
        )

    env_a, run_a, record_a = await _run_scene_to_completion(
        tmp_path / "with-descendant",
        _merge_equivalence_workflow,
        _merge_scene_ready,
        request_both,
        "merge-1",
    )
    env_b, run_b, record_b = await _run_scene_to_completion(
        tmp_path / "ancestor-only",
        _merge_equivalence_workflow,
        _merge_scene_ready,
        request_anchor,
        "merge-1",
    )

    # 执行结果等价（节点态/代次/回滚预算/回滚边/归档实例/根检出）
    assert _execution_ledger(env_a, run_a) == _execution_ledger(env_b, run_b)
    assert (
        _root_checkout(env_a)
        == _root_checkout(env_b)
        == {
            "w.txt": "w-ok",
            "u.txt": "u-ok",
            "v.txt": "v-ok",
        }
    )
    # 归并账不同：携带后代目标的请求记 merged_from（可审计），锚点单独请求无
    assert record_a["merged_from"] == {"v": "u"}
    assert record_b["merged_from"] == {}
    # 无已合并桶：不做集成线回退
    assert record_a["new_root_epoch"] == 0 and record_b["new_root_epoch"] == 0


def _cross_bucket_workflow(tmp: Path) -> Path:
    """跨桶现场：u 失败（未合并）、v 为其后代且已集成（已合并目标）、d 长眠保活。"""
    nodes = [
        _bash_node("w", "echo w-ok > w.txt"),
        _bash_node("u", "if [ -f w.txt ]; then echo u-ok > u.txt; else exit 1; fi"),
        _bash_node("v", "echo v-ok > v.txt", depends_on=["u"], run_on_failure=True),
        _bash_node(
            "d",
            "if [ -f u.txt ]; then echo d-ok > d.txt; else sleep 300; fi",
            depends_on=["u"],
            run_on_failure=True,
        ),
    ]
    return _write_workflow(tmp / "wf-batch-cross-bucket.yaml", nodes)


async def _cross_bucket_scene_ready(env: GraphRunPlaneEnv, run_id: str) -> bool:
    """w/v 已集成、u 失败、d 长眠保活（跨桶归并现场）。"""
    return (
        env.node_info(run_id, "w")["status"] == "passed"
        and env.node_info(run_id, "v")["status"] == "passed"
        and env.node_info(run_id, "u")["status"] == "failed"
        and env.node_info(run_id, "d")["status"] == "running"
    )


async def test_cross_bucket_batch_equivalent_to_single_ancestor_rollback(
    tmp_path: Path,
) -> None:
    """跨桶归并：batch([u, v])（u 未合并、v 已合并后代）等价于只回滚 u——
    v 的集成成果保留在集成线、不发生集成线回退、v 随 u 新 attempt 重跑。"""
    (tmp_path / "cross-bucket").mkdir()
    (tmp_path / "single-anchor").mkdir()

    async def request_batch(env: GraphRunPlaneEnv, run_id: str) -> dict:
        return await _request_batch_rollback(
            env,
            run_id,
            [{"node_id": "u", "expected_epoch": 1}, {"node_id": "v", "expected_epoch": 1}],
            "cross-1",
            expected_root_epoch=1,
        )

    async def request_single(env: GraphRunPlaneEnv, run_id: str) -> dict:
        return await env.coord.request_driver_op(
            run_id,
            "rollback_node",
            {"node_id": "u", "client_op_id": "cross-1", "expected_epoch": 1},
        )

    env_a, run_a, record_a = await _run_scene_to_completion(
        tmp_path / "cross-bucket",
        _cross_bucket_workflow,
        _cross_bucket_scene_ready,
        request_batch,
        "cross-1",
    )
    env_b, run_b, _record_b = await _run_scene_to_completion(
        tmp_path / "single-anchor",
        _cross_bucket_workflow,
        _cross_bucket_scene_ready,
        request_single,
        "cross-1",
    )

    assert _execution_ledger(env_a, run_a) == _execution_ledger(env_b, run_b)
    assert (
        _root_checkout(env_a)
        == _root_checkout(env_b)
        == {
            "w.txt": "w-ok",
            "u.txt": "u-ok",
            "v.txt": "v-ok",
            "d.txt": "d-ok",
        }
    )
    # 跨桶归并把已合并后代改判未合并编排语义：不发回退动作、根代次不动、
    # 归并来源可审计
    assert record_a["merged_from"] == {"v": "u"}
    assert record_a["new_root_epoch"] == 0
    assert not record_a.get("retract_op_id")


def _lineage_staleness_workflow(tmp: Path) -> Path:
    """阶段一交错现场：c 失败（他者回滚目标，其后代含已合并 a）、a 已集成
    （批量回滚目标，回退锚点）、d 依赖 a（谱系失效）、k 依赖 c 长眠保活。

    他者回滚 c 的 reset 集含后代 a：其计划先行落账会把 a 的代次推进到
    批量链登记时的期望之上，批量链回退回执命中过期谱系计划。
    """
    nodes = [
        _bash_node("w", "echo w-ok > w.txt"),
        _bash_node("c", "if [ -f w.txt ]; then echo c-ok > c.txt; else exit 1; fi"),
        _bash_node("a", "echo a-ok > a.txt", depends_on=["c"], run_on_failure=True),
        _bash_node("d", "echo d-ok > d.txt", depends_on=["a"]),
        _bash_node(
            "k",
            "if [ -f c.txt ]; then echo k-ok > k.txt; else sleep 300; fi",
            depends_on=["c"],
            run_on_failure=True,
        ),
    ]
    return _write_workflow(tmp / "wf-batch-stale-lineage.yaml", nodes)


async def _staleness_scene_ready(env: GraphRunPlaneEnv, run_id: str) -> bool:
    """w/a/d 已集成、c 失败、k 长眠保活（阶段一交错现场）。"""
    return (
        all(env.node_info(run_id, n)["status"] == "passed" for n in ("w", "a", "d"))
        and env.node_info(run_id, "c")["status"] == "failed"
        and env.node_info(run_id, "k")["status"] == "running"
    )


async def test_concurrent_rollback_stales_lineage_plan_fails_with_cleanup(
    tmp_path: Path,
) -> None:
    """并发交错阶段一：他者回滚先行推进已合并目标代次——批量链回退回执
    命中过期谱系计划，按实际失效清单以新鲜代收口落账后操作置 failed；
    失效覆盖不丢、代次单调不回退、归档键不碰撞。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _lineage_staleness_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _staleness_scene_ready(env, run_id), 30.0, "阶段一交错现场未就绪")

    await env.coord.pause_run(run_id)
    # 他者回滚（单目标链）先登记先推进；批量回滚（已合并目标 a）后登记
    other = await env.coord.request_driver_op(
        run_id,
        "rollback_node",
        {"node_id": "c", "client_op_id": "other-1", "expected_epoch": 1},
    )
    assert other["phase"] == "pending"
    await _request_batch_rollback(
        env, run_id, [{"node_id": "a", "expected_epoch": 1}], "batch-1", expected_root_epoch=1
    )
    await env.coord.resume_run(run_id)

    record = await _wait_op_phase(env, run_id, "batch-1", "failed")
    assert "陈旧" in (record.get("error") or "")
    # 他者链照常完成；物理后果反映到编排：失效清单节点以新鲜代收口
    await _wait_op_phase(env, run_id, "other-1", "succeeded")

    # 回退只发生一次（根代次恰好 +1）；代次单调：a 0→1（他者）→2（收口）
    scopes = await _run_scopes(env, run_id)
    assert scopes[f"{run_id}-root"]["root_epoch"] == 2
    # 失效覆盖不丢：回执失效集（a/d）与显式目标全部被重置过
    for node_id in ("a", "d"):
        assert env.node_info(run_id, node_id)["gen"] == 2, node_id
    assert env.node_info(run_id, "c")["gen"] == 1
    assert env.node_info(run_id, "k")["gen"] == 1
    # 回滚边不重：他者一条（c）、收口一条（a，从推进后代次到新鲜代）
    assert _rollback_edge_counts(env, run_id) == Counter({("c", 0, "c", 1): 1, ("a", 1, "a", 2): 1})
    # 归档键不碰撞：他者计划归档一轮（c/k/a/d 的 0 代）、收口再归档 a/d 的 1 代
    retired = set(env.driver_of(run_id).state.snapshot()["retired_instances"])
    assert retired == {"c#0", "k#0", "a#0", "a#1", "d#0", "d#1"}

    run = await await_run_status(env, run_id, {"completed"}, timeout_s=60.0)
    assert run.status == "completed"
    assert _root_checkout(env) == {
        "w.txt": "w-ok",
        "c.txt": "c-ok",
        "a.txt": "a-ok",
        "d.txt": "d-ok",
        "k.txt": "k-ok",
    }


def _slice_monotonicity_workflow(tmp: Path) -> Path:
    """阶段二交错现场：a 已集成（批量回滚目标，回退锚点）、d 依赖 a（谱系
    失效）、c 失败（他者回滚目标，其后代 u 为批量链未合并目标）、u 长眠保活。

    他者回滚 c 的 reset 集含后代 u：其计划先行落账推进 u 的代次但不动 u
    的作用域——批量链对 u 的废弃照常成功，切片按单调性核验跳过落账。
    """
    nodes = [
        _bash_node("w", "echo w-ok > w.txt"),
        _bash_node("a", "echo a-ok > a.txt"),
        _bash_node("d", "echo d-ok > d.txt", depends_on=["a"]),
        _bash_node("c", "if [ -f w.txt ]; then echo c-ok > c.txt; else exit 1; fi"),
        _bash_node(
            "u",
            "if [ -f c.txt ]; then echo u-ok > u.txt; else sleep 300; fi",
            depends_on=["c"],
            run_on_failure=True,
        ),
    ]
    return _write_workflow(tmp / "wf-batch-slice-skip.yaml", nodes)


async def _slice_skip_scene_ready(env: GraphRunPlaneEnv, run_id: str) -> bool:
    """w/a/d 已集成、c 失败、u 长眠保活（阶段二交错现场）。"""
    return (
        all(env.node_info(run_id, n)["status"] == "passed" for n in ("w", "a", "d"))
        and env.node_info(run_id, "c")["status"] == "failed"
        and env.node_info(run_id, "u")["status"] == "running"
    )


async def test_concurrent_rollback_skips_advanced_slice_target(tmp_path: Path) -> None:
    """并发交错阶段二：他者回滚推进未合并目标代次——切片按单调性核验
    跳过落账（失效覆盖由他者计划承担，不丢不重），操作照常收口。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _slice_monotonicity_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _slice_skip_scene_ready(env, run_id), 30.0, "阶段二交错现场未就绪")

    await env.coord.pause_run(run_id)
    await env.coord.request_driver_op(
        run_id,
        "rollback_node",
        {"node_id": "c", "client_op_id": "other-1", "expected_epoch": 1},
    )
    await _request_batch_rollback(
        env,
        run_id,
        [{"node_id": "a", "expected_epoch": 1}, {"node_id": "u", "expected_epoch": 1}],
        "batch-1",
        expected_root_epoch=1,
    )
    await env.coord.resume_run(run_id)

    record = await _wait_op_phase(env, run_id, "batch-1", "succeeded")
    await _wait_op_phase(env, run_id, "other-1", "succeeded")

    # 切片跳过记档：u 的编排失效由他者计划覆盖（覆盖不丢），本链不重复重置
    assert record["slices"]["u"]["status"] == "skipped"
    assert env.node_info(run_id, "u")["gen"] == 1  # 他者推进的代次，不回退不改写
    assert env.node_info(run_id, "a")["gen"] == 2
    assert env.node_info(run_id, "d")["gen"] == 2
    assert env.node_info(run_id, "c")["gen"] == 1
    # u 无自身回滚边（切片跳过）；a/d 走谱系失效、c 走他者链
    assert _rollback_edge_counts(env, run_id) == Counter({("c", 0, "c", 1): 1, ("a", 0, "a", 2): 1})
    # 归档键不碰撞：u 恰好归档一次（他者计划），本链未再重置
    retired = set(env.driver_of(run_id).state.snapshot()["retired_instances"])
    assert retired == {"c#0", "u#0", "a#0", "d#0"}
    scopes = await _run_scopes(env, run_id)
    assert scopes[f"{run_id}-root"]["root_epoch"] == 2

    run = await await_run_status(env, run_id, {"completed"}, timeout_s=60.0)
    assert run.status == "completed"
    assert _root_checkout(env) == {
        "w.txt": "w-ok",
        "a.txt": "a-ok",
        "d.txt": "d-ok",
        "c.txt": "c-ok",
        "u.txt": "u-ok",
    }


def _late_receipt_workflow(tmp: Path) -> Path:
    """迟到回执现场：c/e 失败（批量链两个未合并目标）、k 依赖 c 长眠保活。

    他者链与批量链同轮发废弃：c 的废弃被他者先执行而失败（操作置败），
    e 的废弃成功且回执后到——置败后仍照常落切片。
    """
    nodes = [
        _bash_node("w", "echo w-ok > w.txt"),
        _bash_node("c", "if [ -f w.txt ]; then echo c-ok > c.txt; else exit 1; fi"),
        _bash_node("e", "if [ -f w.txt ]; then echo e-ok > e.txt; else exit 1; fi"),
        _bash_node(
            "k",
            "if [ -f c.txt ]; then echo k-ok > k.txt; else sleep 300; fi",
            depends_on=["c"],
            run_on_failure=True,
        ),
    ]
    return _write_workflow(tmp / "wf-batch-late-receipt.yaml", nodes)


async def _late_receipt_scene_ready(env: GraphRunPlaneEnv, run_id: str) -> bool:
    """w 已集成、c/e 失败、k 长眠保活（迟到回执现场）。"""
    return (
        env.node_info(run_id, "w")["status"] == "passed"
        and env.node_info(run_id, "c")["status"] == "failed"
        and env.node_info(run_id, "e")["status"] == "failed"
        and env.node_info(run_id, "k")["status"] == "running"
    )


async def test_late_abandon_receipt_after_failure_still_falls_slice(
    tmp_path: Path,
) -> None:
    """迟到成功回执：c 的废弃动作失败置操作 failed 后，e 的成功废弃回执
    后到仍照常落切片（终态只门控新动作与重建判定，不门控回执落账）。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _late_receipt_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _late_receipt_scene_ready(env, run_id), 30.0, "迟到回执现场未就绪")

    await env.coord.pause_run(run_id)
    # 他者链（单目标 c）先登记先发废弃：抢先废弃 c 的作用域，批量链对 c
    # 的废弃按代次失败、对 e 的废弃照常成功且回执晚于操作置败
    await env.coord.request_driver_op(
        run_id,
        "rollback_node",
        {"node_id": "c", "client_op_id": "other-1", "expected_epoch": 1},
    )
    await _request_batch_rollback(
        env,
        run_id,
        [{"node_id": "c", "expected_epoch": 1}, {"node_id": "e", "expected_epoch": 1}],
        "late-1",
    )
    await env.coord.resume_run(run_id)

    record = await _wait_op_phase(env, run_id, "late-1", "failed")
    assert "废弃动作失败" in (record.get("error") or "")
    assert "c" in (record.get("error") or "")
    await _wait_op_phase(env, run_id, "other-1", "succeeded")

    # 迟到回执照常落账：失败操作内 e 的切片在场且代次已推进（他者链与切片
    # 各一次，失效覆盖不丢）；终态不被回执改写
    assert record["slices"]["e"]["gen"] == 2
    assert env.node_info(run_id, "e")["gen"] == 2
    assert env.node_info(run_id, "c")["gen"] == 1
    assert env.node_info(run_id, "k")["gen"] == 1
    assert _rollback_edge_counts(env, run_id) == Counter({("c", 0, "c", 1): 1, ("e", 0, "e", 2): 1})
    scopes = await _run_scopes(env, run_id)
    assert scopes[f"{run_id}-root"]["root_epoch"] == 1  # 无已合并桶，不做回退

    run = await await_run_status(env, run_id, {"completed"}, timeout_s=60.0)
    assert run.status == "completed"
    assert _root_checkout(env) == {
        "w.txt": "w-ok",
        "c.txt": "c-ok",
        "e.txt": "e-ok",
        "k.txt": "k-ok",
    }
    # 操作终态不被迟到回执改写
    final = await _op_record(env, run_id, "late-1")
    assert final is not None and final["phase"] == "failed"


async def _crash_run_task(env: GraphRunPlaneEnv, run_id: str) -> None:
    """模拟进程崩溃：取消 run 任务（无任何收尾），现场留给恢复对账。"""
    task = env.coord._tasks.get(run_id)
    assert task is not None, "run 任务不在册（可能已终态）"
    done = asyncio.Event()
    task.add_done_callback(lambda _task: done.set())
    task.cancel()
    await asyncio.wait_for(done.wait(), 5.0)


async def _crash_at_operation_phase(
    env: GraphRunPlaneEnv, run_id: str, operation_id: str, phase: str
) -> dict:
    """轮询持久化快照至操作到达目标相位即取消 run 任务（该相位崩溃注入）。"""
    deadline = time.monotonic() + 30.0
    while True:
        record = await _op_record(env, run_id, operation_id)
        if record and record.get("phase") == phase:
            await _crash_run_task(env, run_id)
            return record
        if time.monotonic() > deadline:
            pytest.fail(f"操作未到达 {phase}（当前 {record and record.get('phase')}）")
        await asyncio.sleep(0.005)


async def _reboot_and_recover(tmp: Path) -> GraphRunPlaneEnv:
    """同库重装配后 boot 恢复（模拟重启：驱动快照往返 + 树运行时重建）。"""
    rebooted = GraphRunPlaneEnv(tmp)
    await rebooted.recover()
    return rebooted


async def _crash_recovery_assertions(
    rebooted: GraphRunPlaneEnv, run_id: str, operation_id: str
) -> None:
    """崩溃恢复后的收口断言：操作幂等收口、整图完成、终态账面与文件一致。"""
    record = await _wait_op_phase(rebooted, run_id, operation_id, "succeeded")
    # 根代次恰 +1（run 终态后树运行时退出，须在完成前断言回退未重复执行）
    scopes = await _run_scopes(rebooted, run_id)
    assert scopes[f"{run_id}-root"]["root_epoch"] == 2
    run = await await_run_status(rebooted, run_id, {"completed"}, timeout_s=90.0)
    assert run.status == "completed"
    _assert_mixed_final_ledger(rebooted, run_id, record)
    return record


async def test_crash_at_pending_phase_recovers_idempotently(tmp_path: Path) -> None:
    """登记态崩溃：暂停期受理后崩溃，恢复重发稳定动作号——回退只发生一次、
    失效计划不重复落账，整图照常完成。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _mixed_batch_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _mixed_scene_ready(env, run_id), 30.0, "混合批量现场未在限期内就绪")
    await env.coord.pause_run(run_id)
    await _request_batch_rollback(
        env,
        run_id,
        [
            {"node_id": "p1", "expected_epoch": 1},
            {"node_id": "u1", "expected_epoch": 1},
            {"node_id": "u2", "expected_epoch": 1},
        ],
        "crash-pending",
        expected_root_epoch=1,
    )
    await _crash_run_task(env, run_id)

    rebooted = await _reboot_and_recover(tmp_path)
    # 崩溃发生在暂停期：恢复后仍是暂停态，显式续跑
    run = await rebooted.store.get(run_id)
    assert run.status == "paused"
    await rebooted.coord.resume_run(run_id)
    await _crash_recovery_assertions(rebooted, run_id, "crash-pending")


async def test_crash_at_retracting_phase_recovers_idempotently(tmp_path: Path) -> None:
    """回退中崩溃：集成线回退动作在途崩溃（核心侧三阶段 saga 补账或重试），
    重发稳定动作号后回退不产生第二次、代次与账面恰一次。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _mixed_batch_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _mixed_scene_ready(env, run_id), 30.0, "混合批量现场未在限期内就绪")
    await env.coord.pause_run(run_id)
    await _request_batch_rollback(
        env,
        run_id,
        [
            {"node_id": "p1", "expected_epoch": 1},
            {"node_id": "u1", "expected_epoch": 1},
            {"node_id": "u2", "expected_epoch": 1},
        ],
        "crash-retracting",
        expected_root_epoch=1,
    )
    await env.coord.resume_run(run_id)
    # 回退动作在途（真实 git 重放进行中）时取消任务
    await _crash_at_operation_phase(env, run_id, "crash-retracting", "retracting")
    # 等被取消任务触发的孤儿子进程（git 引用推进）落定，恢复按盘上事实对账
    await asyncio.sleep(0.5)

    rebooted = await _reboot_and_recover(tmp_path)
    await _crash_recovery_assertions(rebooted, run_id, "crash-retracting")


async def test_crash_at_abandoning_phase_recovers_idempotently(tmp_path: Path) -> None:
    """废弃中崩溃：谱系失效已落账、未合并目标的废弃在途（部分账已落），
    恢复后重发废弃按稳定动作号幂等收口——切片恰落一次、不重复废弃。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _mixed_batch_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _mixed_scene_ready(env, run_id), 30.0, "混合批量现场未在限期内就绪")
    await env.coord.pause_run(run_id)
    await _request_batch_rollback(
        env,
        run_id,
        [
            {"node_id": "p1", "expected_epoch": 1},
            {"node_id": "u1", "expected_epoch": 1},
            {"node_id": "u2", "expected_epoch": 1},
        ],
        "crash-abandoning",
        expected_root_epoch=1,
    )
    await env.coord.resume_run(run_id)
    record = await _crash_at_operation_phase(env, run_id, "crash-abandoning", "abandoning")
    # 崩溃点：谱系失效计划已落账（lineage_done 持久化）、切片未落
    assert record["lineage_done"] is True
    assert record["slices"] == {}
    await asyncio.sleep(0.5)

    rebooted = await _reboot_and_recover(tmp_path)
    await _crash_recovery_assertions(rebooted, run_id, "crash-abandoning")


async def test_crash_at_rebuilding_phase_recovers_idempotently(tmp_path: Path) -> None:
    """重建中崩溃：全部失效账已落（切片/covered 持久化），恢复后不重复落账、
    重建照常收口，整图完成。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _mixed_batch_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _mixed_scene_ready(env, run_id), 30.0, "混合批量现场未在限期内就绪")
    await env.coord.pause_run(run_id)
    await _request_batch_rollback(
        env,
        run_id,
        [
            {"node_id": "p1", "expected_epoch": 1},
            {"node_id": "u1", "expected_epoch": 1},
            {"node_id": "u2", "expected_epoch": 1},
        ],
        "crash-rebuilding",
        expected_root_epoch=1,
    )
    await env.coord.resume_run(run_id)
    record = await _crash_at_operation_phase(env, run_id, "crash-rebuilding", "rebuilding")
    # 崩溃点：全部显式目标账已落（谱系 + 切片），重建未收口
    assert record["lineage_done"] is True
    assert set(record["slices"]) == {"u2"}

    rebooted = await _reboot_and_recover(tmp_path)
    await _crash_recovery_assertions(rebooted, run_id, "crash-rebuilding")


def _terminal_finalize_workflow(tmp: Path) -> Path:
    """终态化收口现场：p1 已集成（回退锚点）、m 依赖 p1（首轮过、重跑轮因
    新产出必失败——跳过传播源）、x 依赖 m 长眠（未合并目标，回退连带吸收）。

    回滚后 p1 重跑产出 one-after 使 m 重跑失败，x（依赖 m）被失败传播置
    skipped 且无新工作区——操作按终态收口，恰发生在整图失败收口的同一轮。
    """
    nodes = [
        _bash_node(
            "p1",
            "if [ -f p2.txt ]; then echo one-after > p1.txt; else echo one-before > p1.txt; fi",
        ),
        _bash_node("p2", "echo two > p2.txt"),
        _bash_node(
            "m",
            "if grep -q one-after p1.txt; then exit 1; else echo m-ok > m.txt; fi",
            depends_on=["p1"],
        ),
        _bash_node("x", "sleep 300", depends_on=["m"]),
    ]
    return _write_workflow(tmp / "wf-batch-terminal.yaml", nodes)


async def _terminal_scene_ready(env: GraphRunPlaneEnv, run_id: str) -> bool:
    """p1/p2/m 已集成、x 长眠保活（终态化收口现场）。"""
    return (
        all(env.node_info(run_id, n)["status"] == "passed" for n in ("p1", "p2", "m"))
        and env.node_info(run_id, "x")["status"] == "running"
        and bool(env.node_info(run_id, "x").get("workspace_path"))
    )


async def test_rebuilding_terminal_finalization_completes_in_failure_round(
    tmp_path: Path,
) -> None:
    """rebuilding 终态收口：被吸收目标被失败传播置 skipped 且无新工作区时，
    操作在该轮按终态收口记入操作记录（与工作区就绪路径可区分），不悬挂。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _terminal_finalize_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _terminal_scene_ready(env, run_id), 30.0, "终态化收口现场未就绪")

    await env.coord.pause_run(run_id)
    await _request_batch_rollback(
        env,
        run_id,
        [{"node_id": "p1", "expected_epoch": 1}, {"node_id": "x", "expected_epoch": 1}],
        "term-1",
        expected_root_epoch=1,
    )
    await env.coord.resume_run(run_id)

    record = await _wait_op_phase(env, run_id, "term-1", "succeeded")
    # 终态化目标记入操作记录：x 被失败传播置 skipped（无新工作区）；
    # p1 走工作区就绪路径——两种完成路径可区分
    assert record["terminal_finalized"] == {"x": "skipped"}
    x_info = env.node_info(run_id, "x")
    assert x_info["status"] == "skipped"
    assert x_info["gen"] == 1
    assert x_info["workspace_scope_id"] is None
    # 被吸收目标补记归并来源；谱系失效把 p1/m 一起重置进新代
    assert record["merged_from"] == {"x": "集成线回退"}
    assert env.node_info(run_id, "p1")["gen"] == 1
    assert env.node_info(run_id, "m")["gen"] == 1
    assert env.node_info(run_id, "m")["status"] == "failed"
    assert record["new_root_epoch"] == 2
    assert _rollback_edge_counts(env, run_id) == Counter(
        {("p1", 0, "p1", 1): 1, ("x", 0, "x", 1): 1}
    )
    # m 重跑失败使整图失败——操作收口与整图失败同轮（晚于终局评估即悬挂）
    run = await await_run_status(env, run_id, {"failed"}, timeout_s=60.0)
    assert run.status == "failed"
    # 根检出：保留者与 p1 新代产出在场；m 旧产出随回退撤除且重跑失败未再集成
    assert _root_checkout(env) == {"p1.txt": "one-after", "p2.txt": "two"}


async def test_invalid_target_rejects_whole_request_zero_change(tmp_path: Path) -> None:
    """原子拒绝：任一目标代次不符整体拒绝，文件/账本/编排状态零改动。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _mixed_batch_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _mixed_scene_ready(env, run_id), 30.0, "混合批量现场未在限期内就绪")
    await env.coord.pause_run(run_id)

    work_root = env.tmp / "wd"
    pre_files = {p.name: p.read_bytes() for p in work_root.iterdir() if p.is_file()}
    pre_scopes = await _run_scopes(env, run_id)
    pre_nodes = {node_id: env.node_info(run_id, node_id) for node_id in ("p1", "u1", "u2")}
    pre_blob = await env.coord.load_driver_snapshot(run_id)

    # u2 携带错误代次：整体拒绝
    with pytest.raises(ValueError, match="expected_epoch 不匹配"):
        await _request_batch_rollback(
            env,
            run_id,
            [
                {"node_id": "p1", "expected_epoch": 1},
                {"node_id": "u1", "expected_epoch": 1},
                {"node_id": "u2", "expected_epoch": 2},
            ],
            "reject-1",
            expected_root_epoch=1,
        )
    # 缺已合并目标的旧根代次：整体拒绝
    with pytest.raises(ValueError, match="expected_root_epoch"):
        await _request_batch_rollback(
            env,
            run_id,
            [
                {"node_id": "p1", "expected_epoch": 1},
                {"node_id": "u1", "expected_epoch": 1},
            ],
            "reject-2",
        )

    # 快照级零改动：根检出逐字节、作用域账本、节点档案与操作账全一致
    post_files = {p.name: p.read_bytes() for p in work_root.iterdir() if p.is_file()}
    assert post_files == pre_files
    assert await _run_scopes(env, run_id) == pre_scopes
    for node_id, pre_info in pre_nodes.items():
        assert env.node_info(run_id, node_id) == pre_info, node_id
    blob = await env.coord.load_driver_snapshot(run_id)
    assert (
        (blob.get("state", {}).get("operations") or {})
        == (pre_blob.get("state", {}).get("operations") or {})
        == {}
    )
    await env.coord.stop_run(run_id)


def _empty_integration_workflow(tmp: Path) -> Path:
    """空集成批量现场：b 首轮无文件改动（空集成），a/c 各写一文件，down 长眠保活。

    b 重跑轮以 a.txt 在场判别（首轮 a 尚未运行、重跑轮 a 保留者已集成），
    产出 b2.txt 使 down 的重跑轮快速通过。
    """
    nodes = [
        _bash_node("b", "if [ -f a.txt ]; then printf rerun > b2.txt; else true; fi"),
        _bash_node("a", "printf a > a.txt", depends_on=["b"]),
        _bash_node("c", "printf c > c.txt", depends_on=["b"]),
        _bash_node(
            "down",
            "if [ -f b2.txt ]; then printf done > down.txt; else sleep 300; fi",
            depends_on=["a", "b", "c"],
        ),
    ]
    return _write_workflow(tmp / "wf-batch-empty.yaml", nodes)


async def _empty_scene_ready(env: GraphRunPlaneEnv, run_id: str) -> bool:
    """空集成现场就绪：a/b/c 已通过、down 长眠在途。"""
    return (
        all(env.node_info(run_id, n)["status"] == "passed" for n in ("a", "b", "c"))
        and env.node_info(run_id, "down")["status"] == "running"
        and bool(env.node_info(run_id, "down").get("workspace_path"))
    )


async def test_batch_rollback_with_empty_integration_node(tmp_path: Path) -> None:
    """空集成节点不阻塞批量回滚：与真实目标一并撤回，保留者不重跑，整图完成。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _empty_integration_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _empty_scene_ready(env, run_id), 30.0, "空集成批量现场未就绪")

    pre_scopes = await _run_scopes(env, run_id)
    a_before = dict(_scope_row("a", pre_scopes))
    b_old = _scope_row("b", pre_scopes)["scope_id"]
    c_old = _scope_row("c", pre_scopes)["scope_id"]

    await env.coord.pause_run(run_id)
    result = await _request_batch_rollback(
        env,
        run_id,
        [{"node_id": "b", "expected_epoch": 1}, {"node_id": "c", "expected_epoch": 1}],
        "empty-1",
        expected_root_epoch=1,
    )
    assert result["targets"] == ["b", "c"]
    await env.coord.resume_run(run_id)

    record = await _wait_op_phase(env, run_id, "empty-1", "succeeded")
    assert record["new_gen"] == 1
    assert record["new_root_epoch"] == 2
    assert record["merged_from"] == {}

    # 作用域记录：空集成目标与真实目标一并废弃换代；保留者 a 零变化；根代次 +1
    scopes = await _run_scopes(env, run_id)
    assert scopes[f"{run_id}-root"]["root_epoch"] == 2
    for old_id in (b_old, c_old):
        assert scopes[old_id]["state"] == "discarded"
        assert scopes[old_id]["scope_epoch"] == 2
    a_after = _scope_row("a", scopes)
    for key in ("scope_epoch", "root_epoch", "state", "integration_base_commit"):
        assert a_after[key] == a_before[key], key

    # 编排：b/c/down 进新代重跑；谱系不含目标集成的保留者 a 不重跑
    assert env.node_info(run_id, "a")["gen"] == 0
    for node_id in ("b", "c", "down"):
        assert env.node_info(run_id, node_id)["gen"] == 1, node_id

    await await_run_status(env, run_id, {"completed"}, timeout_s=60.0)
    # 根检出：保留者产物在场、目标产物经重跑重建、空集成节点首轮本无产物
    assert _root_checkout(env) == {
        "a.txt": "a",
        "b2.txt": "rerun",
        "c.txt": "c",
        "down.txt": "done",
    }


def _empty_only_rollback_workflow(tmp: Path) -> Path:
    """仅空集成目标回滚现场：b 首轮无文件改动，a/c 各写一文件，down 短眠在途。"""
    nodes = [
        _bash_node("b", "true"),
        _bash_node("a", "printf a > a.txt", depends_on=["b"]),
        _bash_node("c", "printf c > c.txt", depends_on=["b"]),
        _bash_node("down", "sleep 3 && printf done > down.txt", depends_on=["a", "b", "c"]),
    ]
    return _write_workflow(tmp / "wf-batch-empty-only.yaml", nodes)


async def test_batch_rollback_of_only_empty_node_keeps_inflight_downstream(
    tmp_path: Path,
) -> None:
    """仅回滚空集成节点（不暂停）：线不动、根代次不推进，在途下游不重跑、
    其集成不被旧根代次拒绝，整图完成。"""
    env = GraphRunPlaneEnv(tmp_path)
    yaml_path = _empty_only_rollback_workflow(env.tmp)
    run_id = await env.start_graph_run(yaml_path=yaml_path)
    await _wait_until(lambda: _empty_scene_ready(env, run_id), 30.0, "仅空集成回滚现场未就绪")

    pre_scopes = await _run_scopes(env, run_id)
    a_before = dict(_scope_row("a", pre_scopes))
    b_old = _scope_row("b", pre_scopes)["scope_id"]

    # 不暂停：down 在途时直接受理（线不动形态的可达现场）
    result = await _request_batch_rollback(
        env,
        run_id,
        [{"node_id": "b", "expected_epoch": 1}],
        "empty-only-1",
        expected_root_epoch=1,
    )
    assert result["targets"] == ["b"]

    record = await _wait_op_phase(env, run_id, "empty-only-1", "succeeded")
    assert record["new_gen"] == 1
    assert record["new_root_epoch"] == 1  # 线不动：根代次不推进

    # 作用域记录：仅空集成目标废弃换代；保留者 a 零变化；根代次不动
    scopes = await _run_scopes(env, run_id)
    assert scopes[f"{run_id}-root"]["root_epoch"] == 1
    assert scopes[b_old]["state"] == "discarded"
    assert scopes[b_old]["scope_epoch"] == 2
    a_after = _scope_row("a", scopes)
    for key in ("scope_epoch", "root_epoch", "state", "integration_base_commit"):
        assert a_after[key] == a_before[key], key

    # 编排：空集成目标换代重跑；谱系与在途下游不受牵连（不重跑）
    assert env.node_info(run_id, "b")["gen"] == 1
    for node_id in ("a", "c", "down"):
        assert env.node_info(run_id, node_id)["gen"] == 0, node_id

    # 在途下游完成后照常集成（基线仍属当前根代次），整图完成不失败
    await await_run_status(env, run_id, {"completed"}, timeout_s=60.0)
    assert _root_checkout(env) == {"a.txt": "a", "c.txt": "c", "down.txt": "done"}
