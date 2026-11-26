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

"""工作区隔离下的节点出生映射测试：建区先行、通知回执后派发与目录注入。

关闭隔离时行为零变化（不产工作区动作、目录保持共享值）也在此固化。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import yaml

from workflow_controller.plugin.run_driver import (
    RunNotification,
    RunSessionInfo,
    SessionActivated,
    StartProcess,
    StartSession,
)
from workflow_controller.plugin.workspace import (
    AbandonScope,
    CommitScopeTerminal,
    IntegrateScope,
    PrepareScopeWorkspace,
    ScopeAbandoned,
    ScopeIntegrated,
    ScopeIntegrationRejected,
    ScopeTerminalCommitted,
    ScopeWorkspacePrepared,
    WorkspaceActionFailed,
)

from .test_engine import _SUB_YAML, _artifact, build_driver, decide, pick, proc_exit

if TYPE_CHECKING:
    from pathlib import Path

_TRACEBACK_CFG = {
    "enabled": True,
    "workspace": {"conflict_policy": "strict", "manifest": {"writer_local": ["out/**"]}},
}


def prepared_note(context: str, path: str = "/tmp/ws") -> ScopeWorkspacePrepared:
    """建区回执通知构造。"""
    return ScopeWorkspacePrepared(
        scope_id="a-w1",
        scope_epoch=1,
        base_commit="b" * 40,
        integration_base_commit="i" * 40,
        workspace_path=path,
        context=context,
    )


def terminal_note(context: str) -> ScopeTerminalCommitted:
    """终态提交回执通知构造。"""
    return ScopeTerminalCommitted(
        scope_id="a-w1",
        commit="t" * 40,
        checkpoint_id="a-w1:terminal:e1",
        context=context,
    )


def integrated_note(context: str) -> ScopeIntegrated:
    """集成成功回执通知构造。"""
    return ScopeIntegrated(
        scope_id="a-w1",
        parent_scope_id="run-1-root",
        new_parent_head="h" * 40,
        integrated_paths=("out.txt",),
        context=context,
    )


def rejected_note(context: str) -> ScopeIntegrationRejected:
    """集成冲突回执通知构造。"""
    return ScopeIntegrationRejected(
        scope_id="a-w1",
        parent_scope_id="run-1-root",
        conflict_paths=("out.txt",),
        base_commit="b" * 40,
        writer_head="w" * 40,
        parent_head="p" * 40,
        context=context,
    )


def abandoned_note(context: str) -> ScopeAbandoned:
    """废弃完成回执通知构造。"""
    return ScopeAbandoned(scope_id="a-w1", new_scope_epoch=2, context=context)


def test_workspace_prepare_precedes_member_birth(tmp_path: Path) -> None:
    """开启隔离后节点出生先建区：通知未到不派成员。"""
    driver = build_driver(
        tmp_path,
        [{"id": "a", "task_type": "agent", "prompt": "go"}],
        traceback=_TRACEBACK_CFG,
    )
    actions = decide(driver)

    prepares = pick(actions, PrepareScopeWorkspace)
    assert len(prepares) == 1
    assert prepares[0].scope_key == "a-w1"
    assert prepares[0].config.root_dir == str(tmp_path)
    assert prepares[0].config.conflict_policy == "strict"
    assert prepares[0].config.writer_local == ("out/**",)
    assert prepares[0].context == "node=a"
    assert not pick(actions, StartSession)


def test_prepare_carries_node_writes_and_claims_enforce(tmp_path: Path) -> None:
    """节点 writes 声明随建区动作携带，claims.enforce 进树配置。"""
    driver = build_driver(
        tmp_path,
        [
            {
                "id": "a",
                "task_type": "bash",
                "command": "true",
                "writes": ["op/op_kernel/**", "op/build/autogen/**"],
            }
        ],
        traceback={
            "enabled": True,
            "claims": {"enforce": True},
        },
    )
    actions = decide(driver)

    prepares = pick(actions, PrepareScopeWorkspace)
    assert len(prepares) == 1
    assert prepares[0].writes == ("op/op_kernel/**", "op/build/autogen/**")
    assert prepares[0].config.claims_enforce is True


def test_claims_enforce_defaults_off(tmp_path: Path) -> None:
    """未声明 claims 配置时强制档默认关闭。"""
    driver = build_driver(
        tmp_path,
        [{"id": "a", "task_type": "bash", "command": "true"}],
        traceback={"enabled": True},
    )
    actions = decide(driver)

    assert pick(actions, PrepareScopeWorkspace)[0].config.claims_enforce is False


def test_member_spawned_with_workspace_after_prepared(tmp_path: Path) -> None:
    """建区回执到达后成员才出生：cwd 与环境指向作用域工作区。"""
    driver = build_driver(
        tmp_path,
        [{"id": "a", "task_type": "agent", "prompt": "go"}],
        traceback=_TRACEBACK_CFG,
    )
    decide(driver)
    actions = decide(driver, (prepared_note("node=a", "/tmp/ws-a"),))  # type: ignore[arg-type]

    starts = pick(actions, StartSession)
    assert len(starts) == 1
    assert starts[0].spec.cwd == "/tmp/ws-a"
    assert starts[0].spec.env_overrides is not None
    assert starts[0].spec.env_overrides["WORKSPACE_DIR"] == "/tmp/ws-a"
    assert starts[0].spec.env_overrides["WORK_DIR"] == "/tmp/ws-a"


def test_bash_node_uses_workspace_cwd_and_env(tmp_path: Path) -> None:
    """bash 进程节点同样先建区，回执后进程的 cwd 与环境指向工作区。"""
    driver = build_driver(
        tmp_path,
        [{"id": "b", "task_type": "bash", "command": "echo hi"}],
        traceback=_TRACEBACK_CFG,
    )
    actions = decide(driver)
    assert len(pick(actions, PrepareScopeWorkspace)) == 1
    assert not pick(actions, StartProcess)

    actions = decide(driver, (prepared_note("node=b", "/tmp/ws-b"),))  # type: ignore[arg-type]
    procs = pick(actions, StartProcess)
    assert len(procs) == 1
    assert procs[0].cwd == "/tmp/ws-b"
    assert procs[0].env is not None
    assert procs[0].env["WORKSPACE_DIR"] == "/tmp/ws-b"
    assert procs[0].env["WORK_DIR"] == "/tmp/ws-b"


def test_prepare_failure_fails_node(tmp_path: Path) -> None:
    """建区失败回执按执行失败收口：节点落失败，不降级共享目录。"""
    driver = build_driver(
        tmp_path,
        [
            {
                "id": "a",
                "task_type": "bash",
                "command": "true",
                "acceptance": {"enabled": False},
            }
        ],
        traceback=_TRACEBACK_CFG,
    )
    decide(driver)
    note = WorkspaceActionFailed(
        action="PrepareScopeWorkspace",
        client_op_id="prepare-1",
        error="私有仓不可用",
        context="node=a",
    )
    actions = decide(driver, (note,))

    assert not pick(actions, StartProcess)
    assert driver.state.status("a") == "failed"
    failure_reason = driver.state.snapshot()["nodes"]["a"].get("failure_reason") or ""
    assert "节点工作区建立失败" in failure_reason


def test_workspace_disabled_keeps_shared_cwd(tmp_path: Path) -> None:
    """未开启隔离：不产工作区动作，成员与进程直接以共享目录出生。"""
    driver = build_driver(
        tmp_path,
        [
            {"id": "a", "task_type": "agent", "prompt": "go"},
            {"id": "b", "task_type": "bash", "command": "echo hi"},
        ],
    )
    actions = decide(driver)

    assert not pick(actions, PrepareScopeWorkspace)
    starts = pick(actions, StartSession)
    assert len(starts) == 1
    assert starts[0].spec.cwd == str(tmp_path)
    assert not starts[0].spec.env_overrides
    procs = pick(actions, StartProcess)
    assert len(procs) == 1
    assert procs[0].cwd == str(tmp_path)
    assert not procs[0].env


def test_merged_subgraph_node_keeps_colon_id_in_scope_key(tmp_path: Path) -> None:
    """运行时子图节点（ID 含冒号）建区：作用域键保留原始节点 ID。"""
    driver = build_driver(
        tmp_path,
        [
            {
                "id": "gen",
                "task_type": "generator",
                "generator_type": "bash",
                "command": "cat > sub.yaml",
                "subgraph": {"output": "sub.yaml"},
                "acceptance": {"enabled": True, "type": "script", "script": "true"},
            }
        ],
        traceback=_TRACEBACK_CFG,
    )
    decide(driver)  # gen 建区（gen-w1）
    decide(driver, (prepared_note("node=gen"),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=gen", 0),))  # 执行完成 → 终态提交
    decide(driver, (terminal_note("node=gen"),))  # type: ignore[arg-type]
    decide(driver, (_artifact("node=gen", "sub.yaml", yaml.safe_dump(_SUB_YAML)),))
    decide(driver, (proc_exit("node=gen/verify", 0),))  # 验收通过 → 待集成
    actions = decide(driver, (integrated_note("node=gen"),))  # type: ignore[arg-type]
    # 集成回执到达即并图：子节点起步先建区

    prepares = pick(actions, PrepareScopeWorkspace)
    assert [prepare.scope_key for prepare in prepares] == ["gen:s1-w4"]
    assert prepares[0].owner_ref == "gen:s1"


def test_late_workspace_note_after_activation_ignored(tmp_path: Path) -> None:
    """迟到建区回执（活动已过建区相位）不重复派发执行。"""
    driver = build_driver(
        tmp_path,
        [{"id": "a", "task_type": "agent", "prompt": "go"}],
        traceback=_TRACEBACK_CFG,
    )
    decide(driver)
    decide(driver, (prepared_note("node=a", "/tmp/ws-a"),))  # type: ignore[arg-type]
    notes: tuple[RunNotification, ...] = (
        SessionActivated(session_id="s1", context="node=a"),
        prepared_note("node=a", "/tmp/ws-a"),
    )
    actions = decide(
        driver,
        notes,  # type: ignore[arg-type]
        (RunSessionInfo(session_id="s1", context="node=a", state="running"),),
    )
    assert len(pick(actions, StartSession)) == 0


def _verify_bash_node(command: str = "true", **acc_extra: object) -> dict:
    """script 验收的 bash 节点清单项。"""
    return {
        "id": "a",
        "task_type": "bash",
        "command": command,
        "acceptance": {"enabled": True, "type": "script", "script": "true", **acc_extra},
    }


def test_terminal_commit_precedes_verifier_and_integration_gates_pass(tmp_path: Path) -> None:
    """executor 完成先过闸门落终态，验收通过进待集成，集成回执到达才 passed。"""
    driver = build_driver(tmp_path, [_verify_bash_node()], traceback=_TRACEBACK_CFG)
    decide(driver)  # 建区
    decide(driver, (prepared_note("node=a", "/tmp/ws-a"),))  # type: ignore[arg-type]
    info = driver.state.snapshot()["nodes"]["a"]
    assert info["workspace_scope_id"] == "a-w1"  # 映射进节点档案
    assert info["workspace_path"] == "/tmp/ws-a"

    actions = decide(driver, (proc_exit("node=a", 0),))
    terminals = pick(actions, CommitScopeTerminal)
    assert len(terminals) == 1
    assert terminals[0].scope_id == "a-w1"
    assert not pick(actions, StartProcess)  # 终态回执前验收不出生

    actions = decide(driver, (terminal_note("node=a"),))  # type: ignore[arg-type]
    verifies = pick(actions, StartProcess)
    assert len(verifies) == 1
    assert verifies[0].context == "node=a/verify"
    assert verifies[0].cwd == "/tmp/ws-a"  # verifier 与 executor 共用节点工作区

    actions = decide(driver, (proc_exit("node=a/verify", 0),))
    integrates = pick(actions, IntegrateScope)
    assert len(integrates) == 1
    assert integrates[0].scope_id == "a-w1"
    assert driver.state.status("a") == "running"  # 验收通过 ≠ 集成完成

    decide(driver, (integrated_note("node=a"),))  # type: ignore[arg-type]
    assert driver.state.status("a") == "passed"


def test_integration_rejected_fails_node(tmp_path: Path) -> None:
    """集成冲突回执走验收失败路径：节点落失败并携带冲突路径。"""
    driver = build_driver(tmp_path, [_verify_bash_node()], traceback=_TRACEBACK_CFG)
    decide(driver)
    decide(driver, (prepared_note("node=a"),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=a", 0),))
    decide(driver, (terminal_note("node=a"),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=a/verify", 0),))

    decide(driver, (rejected_note("node=a"),))  # type: ignore[arg-type]

    assert driver.state.status("a") == "failed"
    reason = driver.state.snapshot()["nodes"]["a"].get("failure_reason") or ""
    assert "集成冲突" in reason
    assert "out.txt" in reason


def test_retry_abandons_stale_scope_before_rebuild(tmp_path: Path) -> None:
    """验收失败重试先废弃旧作用域再建新：回执到达前不建区、档案随之清理。"""
    driver = build_driver(
        tmp_path,
        [_verify_bash_node(max_retries=1)],
        traceback=_TRACEBACK_CFG,
    )
    decide(driver)  # prepare a-w1
    decide(driver, (prepared_note("node=a"),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=a", 0),))
    decide(driver, (terminal_note("node=a"),))  # type: ignore[arg-type]

    actions = decide(driver, (proc_exit("node=a/verify", 1),))  # 验收失败 → retry + 重排
    assert driver.state.snapshot()["nodes"]["a"]["attempts"] == 2  # 重试已重排新 attempt
    abandons = pick(actions, AbandonScope)
    assert len(abandons) == 1
    assert abandons[0].scope_id == "a-w1"
    assert abandons[0].expected_scope_epoch == 1
    assert not pick(actions, PrepareScopeWorkspace)  # 废弃回执前不建新

    actions = decide(driver, (abandoned_note("node=a"),))  # type: ignore[arg-type]
    prepares = pick(actions, PrepareScopeWorkspace)
    assert len(prepares) == 1
    assert prepares[0].scope_key == "a-w4"  # 新作用域键（序号不回卷）
    assert driver.state.snapshot()["nodes"]["a"]["workspace_scope_id"] is None

    # 第二次执行走完同样链路，重试预算耗尽后失败收口
    decide(driver, (prepared_note("node=a", "/tmp/ws-a2"),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=a", 0),))
    decide(driver, (terminal_note("node=a"),))  # type: ignore[arg-type]
    decide(driver, (proc_exit("node=a/verify", 1),))
    assert driver.state.status("a") == "failed"
