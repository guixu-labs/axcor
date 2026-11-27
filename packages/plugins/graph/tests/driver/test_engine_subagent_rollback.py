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

"""子代理回滚异步确认链测试：op 登记立即返回、链推进与拒绝路径。

链每一步以合成通知推进（恢复回执 → 失效落账 + 绑定确认 → 绑定回执 →
节点重绑新会话 + 操作完成），宿主执行面缺席、驱动不感知。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from workflow_controller.plugin.run_driver import DiscardSession, EmitEvent, SessionActivated
from workflow_controller.plugin.workspace import (
    RestoreCommitted,
    RestoreScope,
    RestoreSessionBound,
    ScopeRestored,
    WorkspaceActionFailed,
)

from .test_engine import build_driver, decide, pick
from .test_engine_workspace import _TRACEBACK_CFG

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.plugins.graph.driver.engine import GraphDriver


def _member_node() -> dict:
    """成员执行节点的清单项（script 验收）。"""
    return {
        "id": "a",
        "task_type": "agent",
        "prompt": "do work",
        "acceptance": {"enabled": True, "type": "script", "script": "true"},
    }


def _prepared(context: str, scope_id: str, path: str = "/tmp/ws") -> object:
    """建区回执。"""
    from workflow_controller.plugin.workspace import ScopeWorkspacePrepared

    return ScopeWorkspacePrepared(
        scope_id=scope_id,
        scope_epoch=1,
        base_commit="b" * 40,
        integration_base_commit="i" * 40,
        workspace_path=path,
        context=context,
    )


def _running_member_driver(tmp_path: Path) -> GraphDriver:
    """建区完毕、成员已激活的驱动（节点在跑、档案有作用域与会话映射）。"""
    driver = build_driver(tmp_path, [_member_node()], traceback=_TRACEBACK_CFG)
    decide(driver)  # 建区
    decide(driver, (_prepared("node=a", "a-w1"),))  # type: ignore[arg-type]
    decide(driver, (SessionActivated(session_id="ses-exec-1", context="node=a", ok=True),))  # type: ignore[arg-type]
    return driver


def _op_args(**overrides: object) -> dict:
    """子代理回滚 op 请求参数（默认目标 a、幂等键 co-1、代次 1、组检查点）。"""
    args: dict[str, object] = {
        "node_id": "a",
        "client_op_id": "co-1",
        "expected_epoch": 1,
        "checkpoint_id": "a-w1:before_child:a-w1-g1",
    }
    args.update(overrides)
    return args


def _op_phase(driver: GraphDriver, operation_id: str = "co-1") -> str:
    """操作当前阶段。"""
    return str(driver.handle_op("get_operation", {"operation_id": operation_id})["phase"])


def test_subagent_rollback_op_registers_then_decide_sends_restore(tmp_path: Path) -> None:
    """op 只登记并立即返回；恢复动作由后续 decide 发出并携带链内操作号。"""
    driver = _running_member_driver(tmp_path)
    result = driver.handle_op("rollback_subagent", _op_args())
    assert result["operation_id"] == "co-1"
    assert result["phase"] == "pending"

    actions = decide(driver)
    restores = pick(actions, RestoreScope)
    assert len(restores) == 1
    assert restores[0].scope_id == "a-w1"
    assert restores[0].checkpoint_id == "a-w1:before_child:a-w1-g1"
    assert restores[0].expected_scope_epoch == 1
    assert restores[0].client_op_id.startswith("rb-sub-restore-")
    assert restores[0].replay_dispatch is False
    assert _op_phase(driver) == "restoring"


def test_subagent_chain_restores_rebinds_and_completes(tmp_path: Path) -> None:
    """完整链：恢复回执落失效（弃旧会话）→ 绑定确认 → 绑定回执重绑会话、操作完成。"""
    driver = _running_member_driver(tmp_path)
    driver.handle_op("rollback_subagent", _op_args())
    decide(driver)  # 发恢复动作

    actions = decide(
        driver,
        (
            ScopeRestored(
                scope_id="a-w1",
                new_scope_epoch=2,
                checkpoint_id="a-w1:before_child:a-w1-g1",
                context="node=a",
            ),
        ),  # type: ignore[arg-type]
    )
    # 失效落账：旧执行会话弃置、档案会话清空、工作区映射换代
    discards = pick(actions, DiscardSession)
    assert [d.session_id for d in discards] == ["ses-exec-1"]
    assert driver.state.node_session_id("a") is None
    assert driver.state.node_workspace("a") == ("a-w1", 2, "/tmp/ws")
    confirms = pick(actions, RestoreCommitted)
    assert len(confirms) == 1 and confirms[0].restore_binding is True
    assert any(isinstance(a, EmitEvent) and a.event_type == "subagent_rollback" for a in actions)
    assert _op_phase(driver) == "binding"

    actions = decide(
        driver,
        (
            RestoreSessionBound(
                scope_id="a-w1",
                session_id="ses-restore-9",
                checkpoint_id="a-w1:before_child:a-w1-g1",
                context="node=a",
            ),
        ),  # type: ignore[arg-type]
    )
    assert driver.state.node_session_id("a") == "ses-restore-9"
    assert _op_phase(driver) == "succeeded"
    assert not pick(actions, RestoreCommitted)  # 完成后不再重发确认
    # 幂等重放：链完成后同键请求返回原操作与当前状态
    replay = driver.handle_op("rollback_subagent", _op_args())
    assert replay["operation_id"] == "co-1" and replay["phase"] == "succeeded"


def test_subagent_replay_flag_reaches_restore_action(tmp_path: Path) -> None:
    """replay_dispatch 请求标志透传到恢复动作（校验在宿主侧）。"""
    driver = _running_member_driver(tmp_path)
    driver.handle_op("rollback_subagent", _op_args(replay_dispatch=True))
    actions = decide(driver)
    assert pick(actions, RestoreScope)[0].replay_dispatch is True


def test_subagent_rollback_registration_rejections(tmp_path: Path) -> None:
    """受理拒绝面：未开启/缺检查点/非执行中/代次与作用域不符/键冲突。"""
    driver = _running_member_driver(tmp_path)
    with pytest.raises(ValueError, match="未开启"):
        bare = build_driver(tmp_path, [_member_node()])
        bare.handle_op("rollback_subagent", _op_args())
    with pytest.raises(ValueError, match="组检查点"):
        driver.handle_op("rollback_subagent", _op_args(checkpoint_id=""))
    with pytest.raises(ValueError, match="执行中节点"):
        stopped = _running_member_driver(tmp_path)
        stopped.state.set_node_status("a", "stopped")
        stopped.handle_op("rollback_subagent", _op_args())
    with pytest.raises(ValueError, match="expected_epoch 不匹配"):
        driver.handle_op("rollback_subagent", _op_args(expected_epoch=9))
    with pytest.raises(ValueError, match="作用域不符"):
        driver.handle_op("rollback_subagent", _op_args(scope_id="a-w9", client_op_id="co-2"))
    driver.handle_op("rollback_subagent", _op_args())  # 登记幂等键 co-1
    with pytest.raises(ValueError, match="冲突"):
        driver.handle_op("rollback_subagent", _op_args(checkpoint_id="other"))
    with pytest.raises(ValueError, match="节点不存在"):
        driver.handle_op("rollback_subagent", _op_args(node_id="zz"))


def test_subagent_rollback_on_passed_propagates_to_integrated_chain(tmp_path: Path) -> None:
    """命中已集成到根的传播：登记为已合并回滚链（须携带旧根代次）。"""
    passed = _running_member_driver(tmp_path)
    passed.state.set_node_status("a", "passed")
    with pytest.raises(ValueError, match="expected_root_epoch"):
        passed.handle_op("rollback_subagent", _op_args())
    result = passed.handle_op(
        "rollback_subagent", _op_args(expected_root_epoch=1, client_op_id="co-8")
    )
    assert result["kind"] == "rollback_integrated"
    assert result["phase"] == "pending"
    assert "子代理回滚传播" in result["reason"]


def test_subagent_chain_action_failures_fail_operation(tmp_path: Path) -> None:
    """链内动作失败置操作失败：恢复动作按链内操作号、绑定失败按操作号对账。"""
    driver = _running_member_driver(tmp_path)
    driver.handle_op("rollback_subagent", _op_args())
    decide(driver)  # 发恢复动作
    restore_op_id = None
    for record in driver.state.operations().values():
        restore_op_id = record.get("restore_op_id")
    decide(
        driver,
        (WorkspaceActionFailed(action="RestoreScope", client_op_id=restore_op_id, error="boom"),),  # type: ignore[arg-type]
    )
    assert _op_phase(driver) == "failed"
    op = driver.handle_op("get_operation", {"operation_id": "co-1"})
    assert "组检查点恢复失败" in op["error"]

    driver2 = _running_member_driver(tmp_path)
    driver2.handle_op("rollback_subagent", _op_args(client_op_id="co-9"))
    decide(driver2)
    decide(
        driver2,
        (
            ScopeRestored(
                scope_id="a-w1",
                new_scope_epoch=2,
                checkpoint_id="a-w1:before_child:a-w1-g1",
                context="node=a",
            ),
        ),  # type: ignore[arg-type]
    )
    decide(
        driver2,
        (WorkspaceActionFailed(action="RestoreCommitted", client_op_id="co-9", error="seed lost"),),  # type: ignore[arg-type]
    )
    assert _op_phase(driver2, "co-9") == "failed"
    assert (
        "恢复会话绑定失败" in driver2.handle_op("get_operation", {"operation_id": "co-9"})["error"]
    )


def test_subagent_recovery_demotes_restoring_and_rebinds_after_crash(tmp_path: Path) -> None:
    """崩溃恢复：恢复中相位退回登记态重发；绑定中相位每轮重发确认直至回执。"""
    driver = _running_member_driver(tmp_path)
    driver.handle_op("rollback_subagent", _op_args())
    decide(driver)  # 发恢复动作
    driver.prepare_recovery(running=True)
    assert _op_phase(driver) == "pending"
    actions = decide(driver)
    assert pick(actions, RestoreScope)  # 重发恢复动作

    # 走到绑定相位后模拟崩溃：恢复后每轮重发绑定确认
    decide(
        driver,
        (
            ScopeRestored(
                scope_id="a-w1",
                new_scope_epoch=2,
                checkpoint_id="a-w1:before_child:a-w1-g1",
                context="node=a",
            ),
        ),  # type: ignore[arg-type]
    )
    driver.prepare_recovery(running=True)
    assert _op_phase(driver) == "binding"
    actions = decide(driver)
    confirms = pick(actions, RestoreCommitted)
    assert confirms and confirms[0].restore_binding is True
    decide(
        driver,
        (
            RestoreSessionBound(
                scope_id="a-w1",
                session_id="ses-restore-2",
                checkpoint_id="a-w1:before_child:a-w1-g1",
                context="node=a",
            ),
        ),  # type: ignore[arg-type]
    )
    assert _op_phase(driver) == "succeeded"


def test_subagent_stale_mapping_fails_operation(tmp_path: Path) -> None:
    """映射或代次被并发改变时操作置失败，不静默吞。"""
    driver = _running_member_driver(tmp_path)
    driver.handle_op("rollback_subagent", _op_args(client_op_id="co-3"))
    driver.state.clear_node_workspace("a")
    decide(driver)
    assert _op_phase(driver, "co-3") == "failed"
    assert "映射已变化" in driver.handle_op("get_operation", {"operation_id": "co-3"})["error"]
