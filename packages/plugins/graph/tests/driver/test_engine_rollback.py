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

"""未合并节点回滚异步状态机测试：op 登记立即返回、确认链推进与拒绝路径。

回滚链每一步以合成通知推进（废弃回执 → 失效计划 + 恢复确认 → 重排建区
回执 → 节点重跑 + 操作完成），宿主执行面缺席、驱动不感知。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from workflow_controller.plugin.run_driver import EmitEvent, StartProcess
from workflow_controller.plugin.workspace import (
    AbandonScope,
    PrepareScopeWorkspace,
    RestoreCommitted,
    ScopeAbandoned,
    ScopeWorkspacePrepared,
    WorkspaceActionFailed,
)
from workflow_controller.plugins.graph.driver.state import GraphState

from .test_engine import build_driver, decide, pick, proc_exit
from .test_engine_workspace import _TRACEBACK_CFG, integrated_note, terminal_note

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.plugins.graph.driver.engine import GraphDriver


def _bash_node() -> dict:
    """script 验收的 bash 节点清单项。"""
    return {
        "id": "a",
        "task_type": "bash",
        "command": "true",
        "acceptance": {"enabled": True, "type": "script", "script": "true"},
    }


def _prepared(context: str, scope_id: str, path: str = "/tmp/ws") -> ScopeWorkspacePrepared:
    """建区回执（可指定作用域标识）。"""
    return ScopeWorkspacePrepared(
        scope_id=scope_id,
        scope_epoch=1,
        base_commit="b" * 40,
        integration_base_commit="i" * 40,
        workspace_path=path,
        context=context,
    )


def _started_driver(tmp_path: Path) -> GraphDriver:
    """建区完毕、bash 已派发的驱动（节点在跑、档案有作用域映射）。"""
    driver = build_driver(tmp_path, [_bash_node()], traceback=_TRACEBACK_CFG)
    decide(driver)  # 建区
    decide(driver, (_prepared("node=a", "a-w1"),))  # type: ignore[arg-type]
    return driver


def _op_args(**overrides: object) -> dict:
    """回滚 op 请求参数（默认目标 a、幂等键 co-1、代次 1）。"""
    args: dict[str, object] = {
        "node_id": "a",
        "client_op_id": "co-1",
        "expected_epoch": 1,
    }
    args.update(overrides)
    return args


def _op_phase(driver: GraphDriver, operation_id: str = "co-1") -> str:
    """操作当前阶段。"""
    return str(driver.handle_op("get_operation", {"operation_id": operation_id})["phase"])


def test_rollback_op_registers_immediately_then_decide_abandons(tmp_path: Path) -> None:
    """op 只登记并立即返回 operation_id；废弃动作由后续 decide 发出。"""
    driver = _started_driver(tmp_path)
    result = driver.handle_op("rollback_node", _op_args())
    assert result["operation_id"] == "co-1"
    assert result["phase"] == "pending"

    actions = decide(driver)
    abandons = pick(actions, AbandonScope)
    assert len(abandons) == 1
    assert abandons[0].scope_id == "a-w1"
    assert abandons[0].expected_scope_epoch == 1
    assert _op_phase(driver) == "abandoning"


def test_async_chain_applies_plan_rebuilds_and_reruns(tmp_path: Path) -> None:
    """完整链：废弃回执落失效计划 → 恢复确认 + 重排建区 → 新工作区就绪后节点重跑、操作完成。"""
    driver = _started_driver(tmp_path)
    driver.handle_op("rollback_node", _op_args())
    decide(driver)  # 发废弃

    actions = decide(
        driver,
        (ScopeAbandoned(scope_id="a-w1", new_scope_epoch=2, context="node=a"),),  # type: ignore[arg-type]
    )
    # 失效计划落账：目标重置进新代并同轮重排进新 attempt、旧映射清除、
    # 恢复确认发出、事件留痕
    assert driver.state.status("a") == "running"  # 已重排新 attempt（等待建区）
    assert driver.state.attempts("a") == 1  # 回滚重置后从 1 重新计数
    assert driver.state.gen("a") == 1
    assert driver.state.node_workspace("a") is None
    confirms = pick(actions, RestoreCommitted)
    assert len(confirms) == 1 and confirms[0].scope_id == "a-w1"
    prepares = pick(actions, PrepareScopeWorkspace)
    assert [p.scope_key for p in prepares] == ["a-w3"]  # 失效节点重排进新 attempt
    assert any(isinstance(a, EmitEvent) and a.event_type == "node_rollback" for a in actions)
    op = driver.handle_op("get_operation", {"operation_id": "co-1"})
    assert op["phase"] == "rebuilding"
    assert op["new_gen"] == 1

    actions = decide(driver, (_prepared("node=a", "a-w3", path="/tmp/ws2"),))  # type: ignore[arg-type]
    assert pick(actions, StartProcess)  # 节点重跑
    assert _op_phase(driver) == "succeeded"
    # 幂等重放：链完成后同键请求返回原操作与当前状态
    replay = driver.handle_op("rollback_node", _op_args())
    assert replay["operation_id"] == "co-1" and replay["phase"] == "succeeded"
    # 旧作用域请求拒绝（重建后映射指向新作用域）
    with pytest.raises(ValueError, match="作用域不符"):
        driver.handle_op("rollback_node", _op_args(scope_id="a-w1", client_op_id="co-2"))


def test_replay_returns_original_and_conflict_rejected(tmp_path: Path) -> None:
    """同 client_op_id 重复请求返回原操作；同键不同检查点返回冲突错误。"""
    driver = _started_driver(tmp_path)
    first = driver.handle_op("rollback_node", _op_args())
    second = driver.handle_op("rollback_node", _op_args())
    assert second == first
    with pytest.raises(ValueError, match="冲突"):
        driver.handle_op("rollback_node", _op_args(checkpoint_id="other"))


def test_traceback_disabled_rejects_rollback(tmp_path: Path) -> None:
    """traceback workspace 未开启：明确拒绝文件回滚。"""
    driver = build_driver(tmp_path, [_bash_node()])
    with pytest.raises(ValueError, match="未开启"):
        driver.handle_op("rollback_node", _op_args())


def test_integrated_target_registers_integrated_chain(tmp_path: Path) -> None:
    """显式请求命中已集成（passed）目标：登记为已合并回滚链（须携带旧根代次）。"""
    driver = _started_driver(tmp_path)
    decide(driver, (proc_exit("node=a", 0),))
    decide(driver, (terminal_note("node=a"),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=a/verify", 0),))
    decide(driver, (integrated_note("node=a"),))  # type: ignore[arg-type]
    assert driver.state.status("a") == "passed"
    # 未携带旧根代次：明确拒绝
    with pytest.raises(ValueError, match="expected_root_epoch"):
        driver.handle_op("rollback_node", _op_args())
    result = driver.handle_op("rollback_node", _op_args(expected_root_epoch=1))
    assert result["kind"] == "rollback_integrated"
    assert result["phase"] == "pending"
    assert result["checkpoint_id"] == "a-w1:scope_start:e1"


def test_missing_scope_rejected(tmp_path: Path) -> None:
    """目标无在册作用域（无有效检查点）：明确拒绝。"""
    driver = build_driver(tmp_path, [_bash_node()], traceback=_TRACEBACK_CFG)
    decide(driver)  # 节点尚未建区
    with pytest.raises(ValueError, match="无有效检查点"):
        driver.handle_op("rollback_node", _op_args())


def test_epoch_and_scope_mismatch_rejected(tmp_path: Path) -> None:
    """expected_epoch 不匹配拒绝并携带当前代次；scope_id 不符拒绝。"""
    driver = _started_driver(tmp_path)
    with pytest.raises(ValueError, match="当前作用域代次 1"):
        driver.handle_op("rollback_node", _op_args(expected_epoch=2))
    with pytest.raises(ValueError, match="作用域不符"):
        driver.handle_op("rollback_node", _op_args(scope_id="a-w0"))


def test_stale_gen_fails_operation(tmp_path: Path) -> None:
    """登记后代次被他者回滚改变：操作置失败并留原因。"""
    driver = _started_driver(tmp_path)
    driver.handle_op("rollback_node", _op_args())
    driver.state.reset_rollback("a", 3)  # 他者回滚先行
    decide(driver)
    op = driver.handle_op("get_operation", {"operation_id": "co-1"})
    assert op["phase"] == "failed"
    assert "代次" in (op["error"] or "")


def test_abandon_action_failure_fails_operation(tmp_path: Path) -> None:
    """链内废弃动作失败（按动作号精确对账）：操作置失败。"""
    driver = _started_driver(tmp_path)
    driver.handle_op("rollback_node", _op_args())
    actions = decide(driver)
    abandon = pick(actions, AbandonScope)[0]
    decide(
        driver,
        (
            WorkspaceActionFailed(
                action="AbandonScope",
                client_op_id=abandon.client_op_id,
                error="boom",
                context="node=a",
            ),
        ),  # type: ignore[arg-type]
    )
    op = driver.handle_op("get_operation", {"operation_id": "co-1"})
    assert op["phase"] == "failed"
    assert "废弃动作失败" in (op["error"] or "")


def test_prepare_failure_fails_rebuilding_operation(tmp_path: Path) -> None:
    """重建期建区失败：操作置失败（重建未完成）。"""
    driver = _started_driver(tmp_path)
    driver.handle_op("rollback_node", _op_args())
    decide(driver)
    decide(
        driver,
        (ScopeAbandoned(scope_id="a-w1", new_scope_epoch=2, context="node=a"),),  # type: ignore[arg-type]
    )
    decide(
        driver,
        (
            WorkspaceActionFailed(
                action="PrepareScopeWorkspace",
                client_op_id="prepare-3",
                error="no space",
                context="node=a",
            ),
        ),  # type: ignore[arg-type]
    )
    op = driver.handle_op("get_operation", {"operation_id": "co-1"})
    assert op["phase"] == "failed"
    assert "重建工作区失败" in (op["error"] or "")


def test_rebuilding_terminal_state_completes_unmerged_chain(tmp_path: Path) -> None:
    """终态化收口（对齐批量链边界）：目标被失败传播置 skipped 且无新工作区时
    操作按终态收口记入操作记录，不悬挂。"""
    driver = _started_driver(tmp_path)
    driver.handle_op("rollback_node", _op_args())
    decide(driver)
    decide(
        driver,
        (ScopeAbandoned(scope_id="a-w1", new_scope_epoch=2, context="node=a"),),  # type: ignore[arg-type]
    )
    assert _op_phase(driver) == "rebuilding"
    # 失效计划落账后目标被失败传播置 skipped（无新工作区）
    driver.state.set_node_status("a", "skipped")
    decide(driver)
    op = driver.handle_op("get_operation", {"operation_id": "co-1"})
    assert op["phase"] == "succeeded"
    assert op["terminal_finalized"] == {"a": "skipped"}


def test_rebuilding_workspace_ready_leaves_finalize_record_empty(tmp_path: Path) -> None:
    """工作区就绪收口：终态化记录为空，两种完成路径可区分。"""
    driver = _started_driver(tmp_path)
    driver.handle_op("rollback_node", _op_args())
    decide(driver)
    decide(
        driver,
        (ScopeAbandoned(scope_id="a-w1", new_scope_epoch=2, context="node=a"),),  # type: ignore[arg-type]
    )
    decide(driver, (_prepared("node=a", "a-w3", path="/tmp/ws2"),))  # type: ignore[arg-type]
    op = driver.handle_op("get_operation", {"operation_id": "co-1"})
    assert op["phase"] == "succeeded"
    assert op["terminal_finalized"] == {}


def test_recovery_demotes_abandoning_phase(tmp_path: Path) -> None:
    """恢复时在途废弃相位退回登记态：崩溃窗内废弃是否已执行未知，重发幂等收口。"""
    driver = _started_driver(tmp_path)
    driver.handle_op("rollback_node", _op_args())
    decide(driver)
    assert _op_phase(driver) == "abandoning"
    driver.prepare_recovery(running=True)
    assert _op_phase(driver) == "pending"


def test_operations_persist_through_snapshot(tmp_path: Path) -> None:
    """操作账随驱动快照持久化，恢复后幂等键可查。"""
    driver = _started_driver(tmp_path)
    driver.handle_op("rollback_node", _op_args())
    decide(driver)
    blob = driver.snapshot()
    assert blob["state"]["operations"]["co-1"]["phase"] == "abandoning"
    restored = GraphState("g1", "wf", set(blob["state"]["nodes"]), initial_data=blob["state"])
    assert restored.operation_by_client("co-1")["phase"] == "abandoning"


def test_unknown_operation_lookup_error(tmp_path: Path) -> None:
    """get_operation 对未知标识显式报错。"""
    driver = _started_driver(tmp_path)
    with pytest.raises(LookupError, match="操作不存在"):
        driver.handle_op("get_operation", {"operation_id": "nope"})
