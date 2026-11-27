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

"""已合并回滚异步链测试：集成线回退动作、失效重排与保留者不重跑。

回退链每一步以合成通知推进（回退回执 → 失效计划 + 恢复确认 → 重排
建区回执 → 操作收口），宿主执行面缺席、驱动不感知。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from workflow_controller.plugin.workspace import (
    IntegrationLineRetracted,
    PrepareScopeWorkspace,
    RestoreCommitted,
    RetractIntegrationLine,
    RetractTarget,
    ScopeIntegrated,
    ScopeTerminalCommitted,
    ScopeWorkspacePrepared,
    WorkspaceActionFailed,
)

from .test_engine import build_driver, decide, pick, proc_exit

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.plugins.graph.driver.engine import GraphDriver

_TRACEBACK_CFG = {
    "enabled": True,
    "workspace": {"conflict_policy": "strict", "manifest": {"writer_local": ["out/**"]}},
}


def _bash_node(node_id: str) -> dict:
    """免验收 bash 节点清单项（集成动作仍走隔离相位）。"""
    return {"id": node_id, "task_type": "bash", "command": "true", "acceptance": {"enabled": False}}


def _prepared(node_id: str, seq: int = 1) -> ScopeWorkspacePrepared:
    """建区回执（作用域键 = 节点-w序号）。"""
    return ScopeWorkspacePrepared(
        scope_id=f"{node_id}-w{seq}",
        scope_epoch=1,
        base_commit=f"{node_id[0]}{seq}" + "0" * 38,
        integration_base_commit=f"i-{node_id[0]}{seq}" + "0" * 36,
        workspace_path=f"/tmp/ws-{node_id}-{seq}",
        context=f"node={node_id}",
    )


def _integrated(node_id: str) -> ScopeIntegrated:
    """集成成功回执。"""
    return ScopeIntegrated(
        scope_id=f"{node_id}-w1",
        parent_scope_id="run-1-root",
        new_parent_head=f"h-{node_id[0]}" + "0" * 38,
        integrated_paths=(f"out/{node_id}.txt",),
        context=f"node={node_id}",
    )


def _terminal(node_id: str) -> ScopeTerminalCommitted:
    """终态提交回执。"""
    return ScopeTerminalCommitted(
        scope_id=f"{node_id}-w1",
        commit=f"c-{node_id[0]}" + "0" * 38,
        checkpoint_id=f"{node_id}-w1:terminal:e1",
        context=f"node={node_id}",
    )


def _all_passed_driver(tmp_path: Path, node_ids: tuple[str, ...]) -> GraphDriver:
    """三节点均已集成（passed、档案有作用域映射）的驱动。"""
    driver = build_driver(
        tmp_path, [_bash_node(n) for n in node_ids], traceback=_TRACEBACK_CFG, max_parallel=8
    )
    decide(driver)  # 全部建区
    decide(driver, tuple(_prepared(n) for n in node_ids))  # type: ignore[arg-type]
    decide(driver, tuple(proc_exit(f"node={n}", 0) for n in node_ids))  # 终态提交
    decide(driver, tuple(_terminal(n) for n in node_ids))  # type: ignore[arg-type]
    decide(driver, tuple(_integrated(n) for n in node_ids))  # type: ignore[arg-type]
    for node_id in node_ids:
        assert driver.state.status(node_id) == "passed"
    return driver


def _retracted_note(
    invalidated: tuple[tuple[str, int], ...], preserved: tuple[str, ...]
) -> IntegrationLineRetracted:
    """集成线回退回执（根代次 1 → 2）。"""
    return IntegrationLineRetracted(
        new_root_head="n" * 40,
        new_root_epoch=2,
        target_commit="t" * 40,
        invalidated=invalidated,
        preserved=preserved,
        context="node=a",
    )


def _op_phase(driver: GraphDriver, operation_id: str = "co-1") -> str:
    """操作当前阶段。"""
    return str(driver.handle_op("get_operation", {"operation_id": operation_id})["phase"])


def _retract_op_id(driver: GraphDriver) -> str:
    """链内回退动作的操作号（登记后由推进段落入账）。"""
    for record in driver.state.operations().values():
        if record.get("kind") == "rollback_integrated":
            return str(record.get("retract_op_id"))
    raise AssertionError("缺少已合并回滚操作记录")


def test_integrated_chain_retracts_invalidates_and_preserves(tmp_path: Path) -> None:
    """完整链：回退动作 → 失效重排新代、保留者保持 passed → 新工作区就绪收口。"""
    driver = _all_passed_driver(tmp_path, ("a", "b", "d"))
    result = driver.handle_op(
        "rollback_node",
        {
            "node_id": "a",
            "client_op_id": "co-1",
            "expected_epoch": 1,
            "expected_root_epoch": 1,
        },
    )
    assert result["kind"] == "rollback_integrated"
    assert result["phase"] == "pending"

    actions = decide(driver)
    retracts = pick(actions, RetractIntegrationLine)
    assert len(retracts) == 1
    assert retracts[0].targets == (
        RetractTarget(scope_id="a-w1", checkpoint_id="a-w1:scope_start:e1"),
    )
    assert retracts[0].expected_root_epoch == 1
    assert _op_phase(driver) == "retracting"

    actions = decide(
        driver,
        (_retracted_note((("a-w1", 2), ("d-w1", 2)), ("b-w1",)),),  # type: ignore[arg-type]
    )
    # 失效集（a 与下游 d）重置进新代并重排新 attempt；保留者 b 零变化
    assert driver.state.gen("a") == 1
    assert driver.state.gen("d") == 1
    assert driver.state.status("b") == "passed"
    assert driver.state.node_workspace("a") is None
    assert driver.state.node_workspace("d") is None
    assert driver.state.node_workspace("b") == ("b-w1", 1, "/tmp/ws-b-1")
    assert pick(actions, RestoreCommitted)
    assert _op_phase(driver) == "rebuilding"
    # 失效节点已重排：新 attempt 建区动作就绪（基线取回退后的新集成头）
    prepares = {p.scope_key: p for p in pick(actions, PrepareScopeWorkspace)}
    assert {key for key in prepares if key.startswith("a-w")} != {"a-w1"}
    assert {key for key in prepares if key.startswith("d-w")} != {"d-w1"}
    assert not any(key.startswith("b-w") for key in prepares)
    assert driver.state.status("a") == "running"
    # 回滚事件携带失效/保留划分与新根代次（编排观察面）
    events = [a for a in actions if getattr(a, "event_type", "") == "node_rollback"]
    assert events and events[0].data["invalidated"] == ["a-w1", "d-w1"]
    assert events[0].data["preserved"] == ["b-w1"]
    assert events[0].data["new_root_epoch"] == 2

    # 目标新工作区就绪：操作收口（其后执行属节点自身路径）
    decide(driver, (_prepared("a", seq=2),))  # type: ignore[arg-type]
    assert _op_phase(driver) == "succeeded"
    op = driver.handle_op("get_operation", {"operation_id": "co-1"})
    assert op["new_root_epoch"] == 2
    assert op["new_gen"] == 1


def test_integrated_chain_failure_fails_operation(tmp_path: Path) -> None:
    """回退动作失败（如旧根代次被核心拒绝）：操作置失败并留因。"""
    driver = _all_passed_driver(tmp_path, ("a", "b"))
    driver.handle_op(
        "rollback_node",
        {
            "node_id": "a",
            "client_op_id": "co-1",
            "expected_epoch": 1,
            "expected_root_epoch": 1,
        },
    )
    decide(driver)  # 发回退动作
    decide(
        driver,
        (
            WorkspaceActionFailed(
                action="RetractIntegrationLine",
                client_op_id=_retract_op_id(driver),
                error="根代次不匹配：expected=1",
                context="node=a",
            ),
        ),  # type: ignore[arg-type]
    )
    assert _op_phase(driver) == "failed"
    op = driver.handle_op("get_operation", {"operation_id": "co-1"})
    assert "集成线回退失败" in (op["error"] or "")


def test_integrated_chain_receipt_epoch_mismatch_fails(tmp_path: Path) -> None:
    """回执根代次与预期不符（非 +1）：操作置失败，不落失效计划。"""
    driver = _all_passed_driver(tmp_path, ("a", "b"))
    driver.handle_op(
        "rollback_node",
        {
            "node_id": "a",
            "client_op_id": "co-1",
            "expected_epoch": 1,
            "expected_root_epoch": 1,
        },
    )
    decide(driver)
    decide(
        driver,
        (
            IntegrationLineRetracted(
                new_root_head="n" * 40,
                new_root_epoch=3,
                target_commit="t" * 40,
                invalidated=(("a-w1", 2),),
                context="node=a",
            ),
        ),  # type: ignore[arg-type]
    )
    assert _op_phase(driver) == "failed"
    assert driver.state.status("a") == "passed"  # 未落任何失效


def test_integrated_chain_recovery_demotes_and_resends_stably(tmp_path: Path) -> None:
    """崩溃恢复把回退中退回登记态：重发命中同一动作号（核心幂等键稳定）。"""
    driver = _all_passed_driver(tmp_path, ("a", "b"))
    driver.handle_op(
        "rollback_node",
        {
            "node_id": "a",
            "client_op_id": "co-1",
            "expected_epoch": 1,
            "expected_root_epoch": 1,
        },
    )
    decide(driver)  # 发回退动作、入账动作号
    first_op_id = _retract_op_id(driver)

    driver.prepare_recovery(running=False)
    assert _op_phase(driver) == "pending"
    actions = decide(driver)
    retracts = pick(actions, RetractIntegrationLine)
    assert len(retracts) == 1
    assert retracts[0].client_op_id == first_op_id


def test_integrated_chain_target_missing_from_receipt_fails(tmp_path: Path) -> None:
    """回执失效集不含目标作用域（谱系对不上账）：操作置失败。"""
    driver = _all_passed_driver(tmp_path, ("a", "b"))
    driver.handle_op(
        "rollback_node",
        {
            "node_id": "a",
            "client_op_id": "co-1",
            "expected_epoch": 1,
            "expected_root_epoch": 1,
        },
    )
    decide(driver)
    decide(driver, (_retracted_note((("b-w1", 2),), ()),))  # type: ignore[arg-type]
    assert _op_phase(driver) == "failed"
    assert "失效集不含目标" in (
        driver.handle_op("get_operation", {"operation_id": "co-1"})["error"] or ""
    )
