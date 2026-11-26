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

"""批量节点回滚异步状态机测试：原子受理、归并、混合链与回执增量落账。

批量链每一步以合成通知推进（回退回执 → 谱系失效计划 + 逐目标废弃 →
切片落账 → 重建收口），宿主执行面缺席、驱动不感知；动作号与相位迁移
随操作账可查。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import yaml

from workflow_controller.plugin.workspace import (
    AbandonScope,
    IntegrationLineRetracted,
    PrepareScopeWorkspace,
    RestoreCommitted,
    RetractIntegrationLine,
    RetractTarget,
    ScopeAbandoned,
    ScopeIntegrated,
    ScopeTerminalCommitted,
    ScopeWorkspacePrepared,
    WorkspaceActionFailed,
)

from .test_engine import _artifact, build_driver, decide, pick, proc_exit
from .test_engine_workspace import _TRACEBACK_CFG

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.plugins.graph.driver.engine import GraphDriver


def _bash_node(node_id: str, **extra: object) -> dict:
    """免验收 bash 节点清单项（隔离定序相位照常，链路最短）。"""
    return {
        "id": node_id,
        "task_type": "bash",
        "command": "true",
        "acceptance": {"enabled": False},
        **extra,
    }


def _prepared(node_id: str, seq: int = 1) -> ScopeWorkspacePrepared:
    """建区回执（作用域 = 节点-w序号）。"""
    return ScopeWorkspacePrepared(
        scope_id=f"{node_id}-w{seq}",
        scope_epoch=1,
        base_commit=f"b{node_id}{seq}" + "0" * 34,
        integration_base_commit=f"i{node_id}{seq}" + "0" * 34,
        workspace_path=f"/tmp/ws-{node_id}-{seq}",
        context=f"node={node_id}",
    )


def _terminal(node_id: str, seq: int = 1) -> ScopeTerminalCommitted:
    """终态提交回执。"""
    return ScopeTerminalCommitted(
        scope_id=f"{node_id}-w{seq}",
        commit=f"c-{node_id}{seq}" + "0" * 34,
        checkpoint_id=f"{node_id}-w{seq}:terminal:e1",
        context=f"node={node_id}",
    )


def _integrated(node_id: str, seq: int = 1) -> ScopeIntegrated:
    """集成成功回执。"""
    return ScopeIntegrated(
        scope_id=f"{node_id}-w{seq}",
        parent_scope_id="run-1-root",
        new_parent_head=f"h-{node_id}{seq}" + "0" * 34,
        integrated_paths=(f"out/{node_id}.txt",),
        context=f"node={node_id}",
    )


def _retracted_note(
    invalidated: tuple[tuple[str, int], ...],
    preserved: tuple[str, ...] = (),
) -> IntegrationLineRetracted:
    """集成线回退回执（根代次 1 → 2，批量链 context）。"""
    return IntegrationLineRetracted(
        new_root_head="n" * 40,
        new_root_epoch=2,
        target_commit="t" * 40,
        invalidated=invalidated,
        preserved=preserved,
        context="batch=co-1",
    )


def _drive_passed(driver: GraphDriver, node_id: str, seq: int = 1) -> None:
    """把在建区/运行中的节点推进到已集成（档案留作用域映射）。"""
    decide(driver, (proc_exit(f"node={node_id}", 0),))
    decide(driver, (_terminal(node_id, seq),))
    decide(driver, (_integrated(node_id, seq),))
    assert driver.state.status(node_id) == "passed"


def _independent_running_driver(
    tmp_path: Path, node_ids: tuple[str, ...], extra_nodes: list[dict] | None = None
) -> GraphDriver:
    """无依赖节点全部推进到运行中（在册作用域映射）的驱动。"""
    nodes = [_bash_node(n) for n in node_ids] + (extra_nodes or [])
    driver = build_driver(tmp_path, nodes, traceback=_TRACEBACK_CFG, max_parallel=8)
    decide(driver)  # 无依赖节点全部建区
    decide(driver, tuple(_prepared(n) for n in node_ids))  # type: ignore[arg-type]
    for node_id in node_ids:
        assert driver.state.status(node_id) == "running"
    return driver


def _op_phase(driver: GraphDriver, operation_id: str = "co-1") -> str:
    """操作当前阶段。"""
    return str(driver.handle_op("get_operation", {"operation_id": operation_id})["phase"])


def _op_view(driver: GraphDriver, operation_id: str = "co-1") -> dict:
    """操作记录视图。"""
    return driver.handle_op("get_operation", {"operation_id": operation_id})


def _op_record(driver: GraphDriver, operation_id: str = "co-1") -> dict:
    """操作账原始记录。"""
    record = driver.state.operation(operation_id)
    assert record is not None
    return record


def _rollback_edges(driver: GraphDriver) -> set[tuple[str, int, str, int]]:
    """回滚边集合（from, from_gen, to, to_gen）。"""
    snap = driver.state.snapshot()
    return {
        (e["from"], e["from_gen"], e["to"], e["to_gen"]) for e in snap.get("rollback_edges", [])
    }


def test_registration_merges_dag_descendants(tmp_path: Path) -> None:
    """受理即归并：未合并后代目标并入祖先（run_on_failure 下游在祖先
    failed 时运行），视图携带归并后目标集与归并来源。"""
    driver = build_driver(
        tmp_path,
        [
            _bash_node("u"),
            _bash_node("v", depends_on=["u"], run_on_failure=True),
        ],
        traceback=_TRACEBACK_CFG,
        max_parallel=8,
    )
    decide(driver)  # u 建区
    decide(driver, (_prepared("u"),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=u", 1),))  # u 失败 → v（run_on_failure）就绪
    decide(driver, (_prepared("v"),))  # type: ignore[arg-type]
    assert driver.state.status("u") == "failed"
    assert driver.state.status("v") == "running"

    result = driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "u", "expected_epoch": 1},
                {"node_id": "v", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
        },
    )
    assert result["phase"] == "pending"
    assert result["targets"] == ["u"]
    assert result["merged_from"] == {"v": "u"}


def test_any_invalid_target_rejects_whole_request_zero_change(tmp_path: Path) -> None:
    """任一目标不合格整体拒绝：代次/检查点形态/缺根代次/存在性/无映射，零改动。"""
    driver = _independent_running_driver(tmp_path, ("a", "b"))
    _drive_passed(driver, "a")
    snap = driver.state.snapshot()
    base = {
        "targets": [
            {"node_id": "a", "expected_epoch": 1},
            {"node_id": "b", "expected_epoch": 1},
        ],
        "client_op_id": "co-1",
    }
    with pytest.raises(ValueError, match="expected_epoch 不匹配"):
        driver.handle_op(
            "rollback_nodes",
            {
                **base,
                "targets": [
                    {"node_id": "a", "expected_epoch": 1},
                    {"node_id": "b", "expected_epoch": 2},
                ],
            },
        )
    with pytest.raises(ValueError, match="scope_start"):
        driver.handle_op(
            "rollback_nodes",
            {
                **base,
                "targets": [
                    {"node_id": "a", "expected_epoch": 1},
                    {"node_id": "b", "expected_epoch": 1, "checkpoint_id": "b-w1:before_child:g1"},
                ],
            },
        )
    with pytest.raises(ValueError, match="节点不存在"):
        driver.handle_op(
            "rollback_nodes",
            {"targets": [{"node_id": "nope", "expected_epoch": 1}], "client_op_id": "co-1"},
        )
    with pytest.raises(ValueError, match="无有效检查点"):
        no_scope = build_driver(tmp_path, [_bash_node("x")], traceback=_TRACEBACK_CFG)
        decide(no_scope)  # 建区动作在途、映射未落账
        no_scope.handle_op(
            "rollback_nodes",
            {"targets": [{"node_id": "x", "expected_epoch": 1}], "client_op_id": "co-1"},
        )
    # 含已合并目标（a passed）而缺旧根代次：整体拒绝
    with pytest.raises(ValueError, match="expected_root_epoch"):
        driver.handle_op("rollback_nodes", dict(base))
    assert driver.state.snapshot() == snap
    assert driver.state.operations() == {}


def test_replay_returns_original_and_conflicting_params_rejected(tmp_path: Path) -> None:
    """同键重放返原记录；同键异参（目标集或根代次）按原始请求比较冲突。"""
    driver = _independent_running_driver(tmp_path, ("a", "b"))
    _drive_passed(driver, "a")
    args = {
        "targets": [
            {"node_id": "a", "expected_epoch": 1},
            {"node_id": "b", "expected_epoch": 1},
        ],
        "client_op_id": "co-1",
        "expected_root_epoch": 1,
    }
    first = driver.handle_op("rollback_nodes", dict(args))
    replay = driver.handle_op("rollback_nodes", dict(args))
    assert replay == first
    with pytest.raises(ValueError, match="冲突"):
        driver.handle_op(
            "rollback_nodes",
            {
                **args,
                "targets": [
                    {"node_id": "a", "expected_epoch": 1},
                    {"node_id": "b", "expected_epoch": 2},
                ],
            },
        )
    with pytest.raises(ValueError, match="冲突"):
        driver.handle_op("rollback_nodes", {**args, "expected_root_epoch": 2})
    with pytest.raises(ValueError, match="冲突"):
        driver.handle_op(
            "rollback_nodes",
            {
                **args,
                "targets": [{"node_id": "a", "expected_epoch": 1}],
            },
        )


def test_traceback_disabled_rejects_batch_rollback(tmp_path: Path) -> None:
    """traceback workspace 未开启：明确拒绝批量文件回滚。"""
    driver = build_driver(tmp_path, [_bash_node("a")])
    with pytest.raises(ValueError, match="未开启"):
        driver.handle_op(
            "rollback_nodes",
            {"targets": [{"node_id": "a", "expected_epoch": 1}], "client_op_id": "co-1"},
        )


def _mixed_scene_driver(tmp_path: Path) -> GraphDriver:
    """混合链现场：a/b 已集成，c/e 依赖 a 运行中（在册映射）。"""
    driver = build_driver(
        tmp_path,
        [
            _bash_node("a"),
            _bash_node("b"),
            _bash_node("c", depends_on=["a"]),
            _bash_node("e", depends_on=["a"]),
        ],
        traceback=_TRACEBACK_CFG,
        max_parallel=8,
    )
    decide(driver)  # a、b 建区
    decide(driver, (_prepared("a"), _prepared("b")))  # type: ignore[arg-type]
    _drive_passed(driver, "a")  # a 过验收集成；c/e 就绪建区
    _drive_passed(driver, "b")
    decide(driver, (_prepared("c"), _prepared("e")))  # type: ignore[arg-type]
    assert driver.state.status("c") == "running"
    assert driver.state.status("e") == "running"
    return driver


def test_mixed_chain_retracts_then_abandons_slices_and_rebuilds(tmp_path: Path) -> None:
    """混合链：回退回执当场落谱系失效计划，废弃回执逐张落切片，重建收口。

    已合并 a 回退（连带失效未合并 e、保留 b）→ 未合并 c 废弃切片共用
    预分配代 → 全部目标新工作区就绪后操作收口。
    """
    driver = _mixed_scene_driver(tmp_path)
    result = driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "a", "expected_epoch": 1},
                {"node_id": "c", "expected_epoch": 1},
                {"node_id": "e", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
            "expected_root_epoch": 1,
            "reason": "批量混合回滚",
        },
    )
    assert result["targets"] == ["a", "c", "e"]

    actions = decide(driver)
    retracts = pick(actions, RetractIntegrationLine)
    assert len(retracts) == 1
    assert retracts[0].targets == (
        RetractTarget(scope_id="a-w1", checkpoint_id="a-w1:scope_start:e1"),
    )
    assert retracts[0].expected_root_epoch == 1
    assert retracts[0].context == "batch=co-1"
    assert _op_phase(driver) == "retracting"

    # 回退回执：谱系失效计划当场落账（a 与连带失效的 e 重置进新代、
    # 映射全清、保留者 b 零变化；a 已重排新 attempt 等待建区），未被吸收的
    # c 同轮发废弃动作
    actions = decide(driver, (_retracted_note((("a-w1", 2), ("e-w1", 2)), ("b-w1",)),))
    assert driver.state.status("a") == "running" and driver.state.gen("a") == 1
    assert driver.state.status("e") == "pending" and driver.state.gen("e") == 1
    assert driver.state.status("b") == "passed" and driver.state.gen("b") == 0
    assert driver.state.node_workspace("a") is None
    assert driver.state.node_workspace("e") is None
    assert driver.state.node_workspace("b") == ("b-w1", 1, "/tmp/ws-b-1")
    op = _op_view(driver)
    assert op["merged_from"] == {"e": "集成线回退"}
    assert op["new_gen"] == 1 and op["new_root_epoch"] == 2
    assert _op_phase(driver) == "abandoning"
    abandons = pick(actions, AbandonScope)
    assert [ab.scope_id for ab in abandons] == ["c-w1"]
    assert abandons[0].context == "batch=co-1/c"
    # a 重排起步：新 attempt 建区（基线取回退后集成头）
    assert any(
        isinstance(p, PrepareScopeWorkspace)
        and p.scope_key.startswith("a-w")
        and p.scope_key != "a-w1"
        for p in actions
    )
    # 批量回滚事件留痕（失效/保留划分与归并来源）
    events = [a for a in actions if getattr(a, "event_type", "") == "nodes_rollback"]
    assert events and events[0].data["invalidated"] == ["a-w1", "e-w1"]
    assert events[0].data["preserved"] == ["b-w1"]
    assert events[0].data["merged_from"] == {"e": "集成线回退"}

    # 废弃回执：切片当场落账（共用预分配代、清目标映射、记自身回滚边）
    actions = decide(
        driver,
        (ScopeAbandoned(scope_id="c-w1", new_scope_epoch=2, context="batch=co-1/c"),),  # type: ignore[arg-type]
    )
    assert driver.state.status("c") == "pending" and driver.state.gen("c") == 1
    assert driver.state.node_workspace("c") is None
    confirms = pick(actions, RestoreCommitted)
    assert len(confirms) == 1 and confirms[0].context == "batch=co-1"
    assert _op_phase(driver) == "rebuilding"
    edges = _rollback_edges(driver)
    assert ("a", 0, "a", 1) in edges  # 已合并目标自身回滚边
    assert ("e", 0, "e", 1) in edges  # 被吸收的显式未合并目标同样分别记账
    assert ("c", 0, "c", 1) in edges
    assert driver.state.rollback_left("a", "a", 1) == 0
    assert driver.state.rollback_left("c", "c", 1) == 0

    # 重建收口：a 重跑过验收后 c/e 才可重排，目标新工作区就绪即完成
    decide(driver, (_prepared("a", seq=2),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=a", 0),))
    decide(driver, (_terminal("a", seq=2),))  # type: ignore[arg-type]
    decide(driver, (_integrated("a", seq=2),))  # type: ignore[arg-type]
    assert driver.state.status("a") == "passed"
    decide(driver, (_prepared("c", seq=2), _prepared("e", seq=2)))  # type: ignore[arg-type]
    assert _op_phase(driver) == "succeeded"
    op = _op_view(driver)
    assert op["new_gen"] == 1 and op["new_root_epoch"] == 2
    # 幂等重放：链完成后同键请求返回原操作与当前状态
    replay = driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "a", "expected_epoch": 1},
                {"node_id": "c", "expected_epoch": 1},
                {"node_id": "e", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
            "expected_root_epoch": 1,
        },
    )
    assert replay["phase"] == "succeeded"


def test_unmerged_only_batch_skips_retract_and_shares_preallocated_gen(
    tmp_path: Path,
) -> None:
    """无已合并桶：跳过回退相位，逐目标同轮发废弃、切片共用首张新鲜分配代。"""
    driver = _independent_running_driver(
        tmp_path, ("c", "e"), extra_nodes=[_bash_node("d", depends_on=["c"])]
    )
    result = driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "c", "expected_epoch": 1},
                {"node_id": "e", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
        },
    )
    assert result["merged_from"] == {}  # 兄弟目标互不包含：合法共存
    actions = decide(driver)
    assert not pick(actions, RetractIntegrationLine)
    abandons = pick(actions, AbandonScope)
    assert {ab.scope_id for ab in abandons} == {"c-w1", "e-w1"}
    assert _op_phase(driver) == "abandoning"

    decide(
        driver,
        (ScopeAbandoned(scope_id="c-w1", new_scope_epoch=2, context="batch=co-1/c"),),  # type: ignore[arg-type]
    )
    actions = decide(
        driver,
        (ScopeAbandoned(scope_id="e-w1", new_scope_epoch=2, context="batch=co-1/e"),),  # type: ignore[arg-type]
    )
    # 切片共用同一预分配代（含 c 的下游 d）；最后一张切片后发纯确认
    assert driver.state.gen("c") == 1 and driver.state.gen("d") == 1 and driver.state.gen("e") == 1
    confirms = pick(actions, RestoreCommitted)
    assert len(confirms) == 1
    assert _op_phase(driver) == "rebuilding"
    assert _op_view(driver)["new_root_epoch"] == 0  # 无集成线回退
    # 重建：目标各自新工作区就绪后收口
    decide(driver, (_prepared("c", seq=2), _prepared("e", seq=2)))  # type: ignore[arg-type]
    assert _op_phase(driver) == "succeeded"


def test_single_target_batch_equivalent_to_single_chain(tmp_path: Path) -> None:
    """单目标批量请求与未合并单目标链语义等价（状态账本逐字段一致）。"""
    scene = [_bash_node("a"), _bash_node("down", depends_on=["a"])]

    def build(tmp: Path) -> GraphDriver:
        driver = build_driver(tmp, scene, traceback=_TRACEBACK_CFG)
        decide(driver)  # a 建区（down 依赖 a 未就绪）
        decide(driver, (_prepared("a"),))  # type: ignore[arg-type]
        assert driver.state.status("a") == "running"
        return driver

    (tmp_path / "single").mkdir()
    (tmp_path / "batch").mkdir()
    single = build(tmp_path / "single")
    single.handle_op("rollback_node", {"node_id": "a", "client_op_id": "co-1", "expected_epoch": 1})
    decide(single)
    decide(single, (ScopeAbandoned(scope_id="a-w1", new_scope_epoch=2, context="node=a"),))  # type: ignore[arg-type]

    batch = build(tmp_path / "batch")
    batch.handle_op(
        "rollback_nodes",
        {"targets": [{"node_id": "a", "expected_epoch": 1}], "client_op_id": "co-1"},
    )
    decide(batch)
    decide(batch, (ScopeAbandoned(scope_id="a-w1", new_scope_epoch=2, context="batch=co-1/a"),))  # type: ignore[arg-type]

    def ledger(driver: GraphDriver) -> tuple:
        snap = driver.state.snapshot()
        return (
            snap.get("status"),
            {nid: (info["status"], info["gen"]) for nid, info in snap["nodes"].items()},
            snap.get("rollbacks", {}),
            snap.get("rollback_edges", []),
            set(snap.get("retired_instances", {})),
        )

    assert ledger(single) == ledger(batch)
    assert _op_phase(single) == "rebuilding" and _op_phase(batch) == "rebuilding"
    # 新工作区就绪即收口（两条链同轮完成）
    decide(single, (_prepared("a", seq=2),))  # type: ignore[arg-type]
    decide(batch, (_prepared("a", seq=2),))  # type: ignore[arg-type]
    assert _op_phase(single) == "succeeded" and _op_phase(batch) == "succeeded"
    assert ledger(single) == ledger(batch)


def test_cross_bucket_merge_runs_as_ancestor_rollback(tmp_path: Path) -> None:
    """跨桶归并：已合并后代并入未合并祖先改判未合并编排语义（不撤集成线）。

    run_on_failure 下游在祖先 failed 时通过验收并集成——batch(u, v) 等
    价 rollback(u)：不发回退动作，u 的切片把 v 一并重置、v 的集成成果
    保留在集成线。
    """
    driver = build_driver(
        tmp_path,
        [
            _bash_node("u"),
            _bash_node("v", depends_on=["u"], run_on_failure=True),
        ],
        traceback=_TRACEBACK_CFG,
        max_parallel=8,
    )
    decide(driver)  # u 建区
    decide(driver, (_prepared("u"),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=u", 1),))  # u 失败 → v 就绪
    decide(driver, (_prepared("v"),))  # type: ignore[arg-type]
    _drive_passed(driver, "v")
    assert driver.state.status("u") == "failed"

    result = driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "u", "expected_epoch": 1},
                {"node_id": "v", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
            "expected_root_epoch": 1,
        },
    )
    assert result["targets"] == ["u"]
    assert result["merged_from"] == {"v": "u"}

    actions = decide(driver)
    assert not pick(actions, RetractIntegrationLine)
    abandons = pick(actions, AbandonScope)
    assert [ab.scope_id for ab in abandons] == ["u-w1"]

    decide(
        driver,
        (ScopeAbandoned(scope_id="u-w1", new_scope_epoch=2, context="batch=co-1/u"),),  # type: ignore[arg-type]
    )
    # u 切片把后代 v 一并重置进新代（u 已重排新 attempt 等待建区）；v 的
    # 映射保留（集成线未动）
    assert driver.state.gen("u") == 1 and driver.state.gen("v") == 1
    assert driver.state.status("u") == "running" and driver.state.status("v") == "pending"
    assert driver.state.node_workspace("u") is None
    assert driver.state.node_workspace("v") == ("v-w1", 1, "/tmp/ws-v-1")
    assert _op_phase(driver) == "rebuilding"
    # 重建收口：归并后唯一显式目标 u 的新工作区就绪
    decide(driver, (_prepared("u", seq=2),))  # type: ignore[arg-type]
    assert _op_phase(driver) == "succeeded"


def test_slice_monotonicity_skips_advanced_target_and_freshens_other(
    tmp_path: Path,
) -> None:
    """切片单调性核验：目标已被他者推进的切片跳过记档，其余情形改新鲜代。"""
    driver = _independent_running_driver(
        tmp_path, ("c", "e"), extra_nodes=[_bash_node("d", depends_on=["c"])]
    )
    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "c", "expected_epoch": 1},
                {"node_id": "e", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
        },
    )
    decide(driver)  # 发废弃（c、e）

    # 他者回滚先推进 e（代次 5、映射已清）：e 的切片跳过落账并记档
    driver.state.reset_rollback("e", 5)
    driver.state.clear_node_workspace("e")
    decide(
        driver,
        (ScopeAbandoned(scope_id="e-w1", new_scope_epoch=2, context="batch=co-1/e"),),  # type: ignore[arg-type]
    )
    record = _op_record(driver)
    assert record["slices"]["e"]["status"] == "skipped"
    assert driver.state.gen("e") == 5  # 他者代次不回退
    assert _op_phase(driver) == "abandoning"  # c 尚未落账

    # c 自身代次未动（切片有效），但切片集内 d 已被他者推进到预分配代之上：
    # 该切片改用新鲜代落账并标注（代次单调优先于共用同代）
    preallocated = int(record["preallocated_gen"])
    driver.state.reset_rollback("d", preallocated + 1)
    decide(
        driver,
        (ScopeAbandoned(scope_id="c-w1", new_scope_epoch=2, context="batch=co-1/c"),),  # type: ignore[arg-type]
    )
    record = _op_record(driver)
    assert record["slices"]["c"]["status"] == "fresh"
    fresh_gen = int(record["slices"]["c"]["gen"])
    assert fresh_gen > preallocated + 1
    assert driver.state.gen("c") == fresh_gen
    assert driver.state.gen("d") == fresh_gen


def test_skipped_slice_as_last_piece_completes_accounting(tmp_path: Path) -> None:
    """切片跳过为最后一片：照常发确认收口进重建相，操作不悬挂。"""
    driver = _independent_running_driver(tmp_path, ("c", "e"))
    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "c", "expected_epoch": 1},
                {"node_id": "e", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
        },
    )
    decide(driver)  # 发废弃（c、e）
    decide(
        driver,
        (ScopeAbandoned(scope_id="c-w1", new_scope_epoch=2, context="batch=co-1/c"),),  # type: ignore[arg-type]
    )
    assert _op_phase(driver) == "abandoning"  # e 尚未落账
    # e 被他者回滚推进（代次 5、映射已清）：其切片跳过且为最后一片
    driver.state.reset_rollback("e", 5)
    driver.state.clear_node_workspace("e")
    actions = decide(
        driver,
        (ScopeAbandoned(scope_id="e-w1", new_scope_epoch=2, context="batch=co-1/e"),),  # type: ignore[arg-type]
    )
    assert pick(actions, RestoreCommitted)
    assert _op_phase(driver) == "rebuilding"


def test_stale_lineage_plan_falls_cleanup_invalidation_then_fails(tmp_path: Path) -> None:
    """回退回执命中过期计划：按实际失效清单以新鲜代收口落账后操作置败。"""
    driver = _independent_running_driver(tmp_path, ("a", "b"))
    _drive_passed(driver, "a")
    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [{"node_id": "a", "expected_epoch": 1}],
            "client_op_id": "co-1",
            "expected_root_epoch": 1,
        },
    )
    decide(driver)  # 发回退
    driver.state.reset_rollback("a", 4)  # 他者回滚先推进目标代次（计划过期）
    decide(driver, (_retracted_note((("a-w1", 2),)),))
    op = _op_view(driver)
    assert op["phase"] == "failed"
    assert "陈旧" in (op["error"] or "")
    # 物理后果反映到编排：失效清单节点以新鲜代收口（已重排新 attempt）、映射清
    assert driver.state.gen("a") == 5
    assert driver.state.status("a") == "running"
    assert driver.state.node_workspace("a") is None


def test_late_abandon_receipt_after_failure_still_falls_slice(tmp_path: Path) -> None:
    """迟到成功回执在操作已置败后仍照常落账（终态只门控新动作与重建判定）。"""
    driver = _independent_running_driver(tmp_path, ("c", "e"))
    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "c", "expected_epoch": 1},
                {"node_id": "e", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
        },
    )
    actions = decide(driver)
    abandons = {ab.scope_id: ab for ab in pick(actions, AbandonScope)}
    # c 的废弃动作逻辑失败：操作置败留因（失败目标原状、不回撤已落账切片）
    decide(
        driver,
        (
            WorkspaceActionFailed(
                action="AbandonScope",
                client_op_id=abandons["c-w1"].client_op_id,
                error="代次已被他者改变",
                context="batch=co-1/c",
            ),
        ),  # type: ignore[arg-type]
    )
    op = _op_view(driver)
    assert op["phase"] == "failed"
    assert "c" in (op["error"] or "")
    # e 的成功回执迟到到达：切片照常落账（回执即落账不因操作终态止步）
    decide(
        driver,
        (ScopeAbandoned(scope_id="e-w1", new_scope_epoch=2, context="batch=co-1/e"),),  # type: ignore[arg-type]
    )
    assert driver.state.gen("e") == 1
    assert driver.state.status("e") == "running"  # 已重排新 attempt（等待建区）
    assert driver.state.node_workspace("e") is None
    # 失败目标 c 保持原状（其作用域未被动过）、终态不被回执改写
    assert driver.state.gen("c") == 0
    assert _op_phase(driver) == "failed"


def test_batch_generator_targets_share_parent_gen_once(tmp_path: Path) -> None:
    """同一 generator 下双子图目标：父 generator 只进一次 reset 集、立即置回
    passed、其子图不废弃、回滚边只记一条、各子图目标各记自身回滚边。"""
    driver = build_driver(
        tmp_path,
        [
            {
                "id": "gen",
                "task_type": "generator",
                "generator_type": "bash",
                "command": "cat > sub.yaml",
                "subgraph": {"output": "sub.yaml"},
                "acceptance": {"enabled": True, "type": "script", "script": "test -f sub.yaml"},
            }
        ],
        traceback=_TRACEBACK_CFG,
        workflow="wf",
    )
    sub_two = {
        "workflow": "sub",
        "nodes": [
            {"id": "s1", "task_type": "bash", "command": "true", "acceptance": {"enabled": False}},
            {"id": "s2", "task_type": "bash", "command": "true", "acceptance": {"enabled": False}},
        ],
    }
    decide(driver)  # gen 建区
    decide(driver, (_prepared("gen"),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=gen", 0),))
    decide(driver, (_terminal("gen"),))  # type: ignore[arg-type]
    decide(driver, (_artifact("node=gen", "sub.yaml", yaml.safe_dump(sub_two)),))
    decide(driver, (proc_exit("node=gen/verify", 0),))  # 验收过 → 子图并入
    decide(driver, (_integrated("gen"),))  # type: ignore[arg-type]
    assert driver.state.status("gen") == "passed"
    decide(driver, (_prepared("gen:s1"), _prepared("gen:s2")))  # type: ignore[arg-type]
    assert driver.state.status("gen:s1") == "running"
    assert driver.state.status("gen:s2") == "running"

    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "gen:s1", "expected_epoch": 1},
                {"node_id": "gen:s2", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
        },
    )
    actions = decide(driver)
    abandons = pick(actions, AbandonScope)
    assert {ab.scope_id for ab in abandons} == {"gen:s1-w1", "gen:s2-w1"}

    decide(
        driver,
        (ScopeAbandoned(scope_id="gen:s1-w1", new_scope_epoch=2, context="batch=co-1/gen:s1"),),  # type: ignore[arg-type]
    )
    decide(
        driver,
        (ScopeAbandoned(scope_id="gen:s2-w1", new_scope_epoch=2, context="batch=co-1/gen:s2"),),  # type: ignore[arg-type]
    )
    # 父 generator 只进一次 reset 集、立即置回 passed、子图不废弃
    assert driver.state.status("gen") == "passed"
    assert driver.state.gen("gen") == 1
    assert driver.state.subgraph_node_ids("gen") == ["gen:s1", "gen:s2"]
    assert "gen:s1" in driver.state.snapshot()["nodes"]
    # 回滚边：generator 一条 + 各子图目标自身各一条；共用同一新回滚代
    edges = _rollback_edges(driver)
    assert {e for e in edges if e[0] == "gen" and e[2] == "gen"} == {("gen", 0, "gen", 1)}
    assert ("gen:s1", 0, "gen:s1", 1) in edges
    assert ("gen:s2", 0, "gen:s2", 1) in edges
    assert driver.state.gen("gen:s1") == 1 and driver.state.gen("gen:s2") == 1
    assert _op_phase(driver) == "rebuilding"
    # 子图目标重排：新工作区就绪后收口
    decide(driver, (_prepared("gen:s1", seq=2), _prepared("gen:s2", seq=2)))  # type: ignore[arg-type]
    assert _op_phase(driver) == "succeeded"


def test_recovery_demotes_phases_and_resends_stable_action_ids(tmp_path: Path) -> None:
    """崩溃恢复：回退中/废弃中退登记态重发，链内动作号稳定、已落账不重落。"""
    driver = _independent_running_driver(tmp_path, ("a", "b"))
    _drive_passed(driver, "a")
    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "a", "expected_epoch": 1},
                {"node_id": "b", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
            "expected_root_epoch": 1,
        },
    )
    actions = decide(driver)
    retract_op_id = pick(actions, RetractIntegrationLine)[0].client_op_id

    # 回退中崩溃：退登记态重发命中同一动作号（核心幂等缓存兜底）
    driver.prepare_recovery(running=False)
    assert _op_phase(driver) == "pending"
    actions = decide(driver)
    retracts = pick(actions, RetractIntegrationLine)
    assert len(retracts) == 1 and retracts[0].client_op_id == retract_op_id

    # 回退回执落谱系失效计划、b 的废弃动作在途崩溃：已落账目标不重落、
    # 未落账目标按稳定动作号重发
    actions = decide(driver, (_retracted_note((("a-w1", 2),)),))
    abandon_op_id = pick(actions, AbandonScope)[0].client_op_id
    driver.prepare_recovery(running=False)
    assert _op_phase(driver) == "pending"
    actions = decide(driver)
    re_abandons = pick(actions, AbandonScope)
    assert len(re_abandons) == 1 and re_abandons[0].client_op_id == abandon_op_id
    # 已落账的谱系失效不重发回退动作
    assert not pick(actions, RetractIntegrationLine)

    decide(
        driver,
        (ScopeAbandoned(scope_id="b-w1", new_scope_epoch=2, context="batch=co-1/b"),),  # type: ignore[arg-type]
    )
    assert _op_phase(driver) == "rebuilding"
    decide(driver, (_prepared("a", seq=2), _prepared("b", seq=2)))  # type: ignore[arg-type]
    assert _op_phase(driver) == "succeeded"


def test_rebuilding_terminal_state_completes_without_workspace(tmp_path: Path) -> None:
    """终态化收口：目标被编排终态化（skipped/failed）而无新工作区时操作不悬挂，
    终态化目标记入操作记录（与工作区就绪路径可区分）。"""
    driver = _independent_running_driver(
        tmp_path, ("c",), extra_nodes=[_bash_node("down", depends_on=["c"])]
    )
    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [{"node_id": "c", "expected_epoch": 1}],
            "client_op_id": "co-1",
        },
    )
    decide(driver)
    decide(
        driver,
        (ScopeAbandoned(scope_id="c-w1", new_scope_epoch=2, context="batch=co-1/c"),),  # type: ignore[arg-type]
    )
    assert _op_phase(driver) == "rebuilding"
    # 目标被失败传播置 skipped（无新工作区）：按终态收口
    driver.state.set_node_status("c", "skipped")
    decide(driver)
    op = _op_view(driver)
    assert op["phase"] == "succeeded"
    assert op["terminal_finalized"] == {"c": "skipped"}


def test_retract_action_failure_fails_operation_zero_change(tmp_path: Path) -> None:
    """回退动作失败（按链内动作号对账）：操作置败，谱系失效不落账。"""
    driver = _independent_running_driver(tmp_path, ("a", "b"))
    _drive_passed(driver, "a")
    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [{"node_id": "a", "expected_epoch": 1}],
            "client_op_id": "co-1",
            "expected_root_epoch": 1,
        },
    )
    actions = decide(driver)
    retract_op_id = pick(actions, RetractIntegrationLine)[0].client_op_id
    decide(
        driver,
        (
            WorkspaceActionFailed(
                action="RetractIntegrationLine",
                client_op_id=retract_op_id,
                error="根代次不匹配：expected=1",
                context="batch=co-1",
            ),
        ),  # type: ignore[arg-type]
    )
    op = _op_view(driver)
    assert op["phase"] == "failed"
    assert "集成线回退失败" in (op["error"] or "")
    # 零改动：目标保持 passed、映射与代次原样
    assert driver.state.status("a") == "passed"
    assert driver.state.gen("a") == 0
    assert driver.state.node_workspace("a") == ("a-w1", 1, "/tmp/ws-a-1")


def test_operations_persist_through_snapshot_roundtrip(tmp_path: Path) -> None:
    """批量操作账随驱动快照持久化，恢复后切片完成态与链内动作号可查。"""
    from workflow_controller.plugins.graph.driver.state import GraphState

    driver = _independent_running_driver(tmp_path, ("a", "b"))
    _drive_passed(driver, "a")
    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "a", "expected_epoch": 1},
                {"node_id": "b", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
            "expected_root_epoch": 1,
        },
    )
    decide(driver)
    decide(driver, (_retracted_note((("a-w1", 2),)),))
    blob = driver.snapshot()
    record = blob["state"]["operations"]["co-1"]
    assert record["phase"] == "abandoning"
    assert record["lineage_done"] is True
    assert record["retract_op_id"]
    assert record["abandon_op_ids"]
    restored = GraphState("g1", "wf", set(blob["state"]["nodes"]), initial_data=blob["state"])
    assert restored.operation_by_client("co-1")["phase"] == "abandoning"
    assert restored.operation("co-1")["abandon_op_ids"] == record["abandon_op_ids"]


def test_rebuild_prepare_failure_fails_operation(tmp_path: Path) -> None:
    """重建期建区失败（按目标对账）：操作置失败（重建未完成）。"""
    driver = _independent_running_driver(tmp_path, ("c",))
    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [{"node_id": "c", "expected_epoch": 1}],
            "client_op_id": "co-1",
        },
    )
    decide(driver)
    decide(
        driver,
        (ScopeAbandoned(scope_id="c-w1", new_scope_epoch=2, context="batch=co-1/c"),),  # type: ignore[arg-type]
    )
    decide(
        driver,
        (
            WorkspaceActionFailed(
                action="PrepareScopeWorkspace",
                client_op_id="prepare-9",
                error="no space",
                context="node=c",
            ),
        ),  # type: ignore[arg-type]
    )
    op = _op_view(driver)
    assert op["phase"] == "failed"
    assert "重建工作区失败" in (op["error"] or "")


def test_stale_mapping_at_advance_fails_operation_zero_change(tmp_path: Path) -> None:
    """登记后映射被他者改变：整操作置败留因，未发任何物理动作。"""
    driver = _independent_running_driver(tmp_path, ("a", "b"))
    driver.handle_op(
        "rollback_nodes",
        {
            "targets": [
                {"node_id": "a", "expected_epoch": 1},
                {"node_id": "b", "expected_epoch": 1},
            ],
            "client_op_id": "co-1",
        },
    )
    driver.state.reset_rollback("b", 3)  # 他者回滚先行
    driver.state.clear_node_workspace("b")
    decide(driver)
    op = _op_view(driver)
    assert op["phase"] == "failed"
    assert "映射已变化" in (op["error"] or "")
    assert driver.state.gen("a") == 0
    assert driver.state.status("a") == "running"
