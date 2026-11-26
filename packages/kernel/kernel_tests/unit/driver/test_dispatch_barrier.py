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

"""派发屏障能力矩阵与 SDK 前置屏障接线测试。

矩阵断言按 S4a 代码级核验结论固化：claude SDK 五项全备（Task 输入含
cwd 参数 + PreToolUse updatedInput 重写，经捆绑 CLI 2.1.327 输入面核验）；
claude CLI 的 matcher 面由部署侧管理按缺失处理；opencode DB 轮询全缺。
"""

from __future__ import annotations

import inspect
from typing import Any

from workflow_controller.driver.claude_sdk import ClaudeSDKDriver, _sdk_pre_tool_cb
from workflow_controller.driver.hooks import (
    BarrierDeny,
    BarrierProceed,
    DriverHooks,
    is_dispatch_tool,
    is_side_effect_tool,
)
from workflow_controller.driver.opencode import build_restricted_agent_config
from workflow_controller.plugin.dispatch_barrier import (
    DispatchCapability,
    dispatch_capabilities_for,
    group_arbitration_block_reason,
    writer_dispatch_block_reason,
)

_ALL = frozenset(DispatchCapability)


def test_claude_sdk_declares_all_five_capabilities() -> None:
    """claude SDK：进程内 hook 路径五项能力全备（核验结论落档）。"""
    caps = dispatch_capabilities_for("claude_sdk")
    assert caps.capabilities == _ALL
    assert writer_dispatch_block_reason(caps, strict=True) is None
    assert group_arbitration_block_reason(caps, strict=True) is None


def test_opencode_declares_no_capabilities_and_strict_denies() -> None:
    """opencode：DB 轮询全缺；严格档拒绝 writer 子代理与组仲裁。"""
    caps = dispatch_capabilities_for("opencode")
    assert not caps.capabilities
    writer_reason = writer_dispatch_block_reason(caps, strict=True)
    assert writer_reason is not None and "cwd 注入" in writer_reason
    group_reason = group_arbitration_block_reason(caps, strict=True)
    assert group_reason is not None and "组仲裁" in group_reason


def test_claude_cli_matcher_face_unmanaged_counts_as_missing() -> None:
    """claude CLI：matcher 面不受本仓控制，①②④⑤按缺失处理。"""
    caps = dispatch_capabilities_for("claude_cli")
    assert caps.capabilities == frozenset({DispatchCapability.PARENT_LOCAL_QUIESCE})
    assert writer_dispatch_block_reason(caps, strict=True) is not None


def test_unknown_runtime_counts_as_missing() -> None:
    """未知名 runtime 按全缺处理（不静默放行）。"""
    caps = dispatch_capabilities_for("some_new_runtime")
    assert not caps.capabilities
    assert writer_dispatch_block_reason(caps, strict=True) is not None


def test_non_strict_mode_does_not_block() -> None:
    """弱化档（compat_late_barrier）不做 fail-closed 拦截。"""
    caps = dispatch_capabilities_for("opencode")
    assert writer_dispatch_block_reason(caps, strict=False) is None
    assert group_arbitration_block_reason(caps, strict=False) is None


def test_opencode_denied_tools_translate_to_task_permission_deny() -> None:
    """严格档出生侧拒绝：disallowed_tools 无 allowlist 也产出顶层 task deny。"""
    config = build_restricted_agent_config(None, ["Task"])
    assert config is not None
    assert config["permission"] == {"task": "deny"}
    # allowlist 面行为不变：只读沙箱仍按未列类目拒绝
    readonly = build_restricted_agent_config(["Read", "Grep"])
    assert readonly is not None
    assert "task" in readonly["permission"]
    assert "read" not in readonly["permission"]


def _hooks(
    *,
    dispatch: Any = None,
    side_effect: Any = None,
    pre_tool: Any = None,
) -> DriverHooks:
    """构造带屏障回调的 hooks 桩（回调统一包成协程函数）。"""
    return DriverHooks(
        session_id="ses-1",
        on_pre_tool_use=_async_barrier(pre_tool),
        before_subagent_dispatch=_async_barrier(dispatch),
        before_side_effect_tool=_async_barrier(side_effect),
    )


def _async_barrier(fn: Any) -> Any:
    """把回调包成异步（同步/异步函数与 None 均可）。"""
    if fn is None:
        return None

    async def wrapper(sid: str, data: dict[str, Any]) -> Any:
        outcome = fn(sid, data)
        if inspect.isawaitable(outcome):
            outcome = await outcome
        return outcome

    return wrapper


async def test_sdk_pre_tool_dispatch_proceeds_with_cwd_injection() -> None:
    """task 派发屏障放行：updatedInput 注入子代理工作区 cwd。"""
    hooks = _hooks(dispatch=lambda sid, data: _ok("/ws/child-1"))
    result = await _sdk_pre_tool_cb(
        {"tool_name": "Task", "tool_input": {"prompt": "work"}, "session_id": "nat-1"},
        "toolu-1",
        None,
        hooks=hooks,
    )
    specific = result["hookSpecificOutput"]
    assert specific["hookEventName"] == "PreToolUse"
    assert specific["permissionDecision"] == "allow"
    assert specific["updatedInput"] == {"prompt": "work", "cwd": "/ws/child-1"}


async def test_sdk_pre_tool_dispatch_deny_blocks() -> None:
    """task 派发屏障拒绝：工具阻断，fault 语义并入理由。"""
    hooks = _hooks(
        dispatch=lambda sid, data: BarrierDeny(
            reason="缺少 cwd 注入能力", fault="fault.scope_quiesce_failed"
        )
    )
    result = await _sdk_pre_tool_cb(
        {"tool_name": "Task", "tool_input": {}, "session_id": "nat-1"},
        "toolu-1",
        None,
        hooks=hooks,
    )
    assert result["decision"] == "block"
    assert "缺少 cwd 注入能力" in result["reason"]
    assert "fault.scope_quiesce_failed" in result["reason"]


async def test_sdk_pre_tool_passes_workspace_declaration() -> None:
    """task 输入的 workspace 声明随屏障载荷透传（reader 快照声明）。"""
    seen: list[dict[str, Any]] = []

    def dispatch(sid: str, data: dict[str, Any]) -> BarrierProceed:
        seen.append(data)
        return BarrierProceed(child_cwd="/ws/reader-1")

    hooks = _hooks(dispatch=dispatch)
    await _sdk_pre_tool_cb(
        {
            "tool_name": "Task",
            "tool_input": {"prompt": "read only", "workspace": "reader"},
            "session_id": "nat-1",
        },
        "toolu-1",
        None,
        hooks=hooks,
    )
    assert seen[0]["workspace"] == "reader"
    # 未声明时按 writer 默认
    await _sdk_pre_tool_cb(
        {"tool_name": "Task", "tool_input": {"prompt": "write"}, "session_id": "nat-1"},
        "toolu-2",
        None,
        hooks=hooks,
    )
    assert seen[1]["workspace"] == "writer"


async def test_sdk_pre_tool_side_effect_barrier_routes() -> None:
    """写文件类工具走副作用屏障；非副作用工具不触发。"""
    seen: list[str] = []

    async def barrier(sid: str, data: dict[str, Any]) -> BarrierProceed:
        seen.append(data["tool_name"])
        return BarrierProceed()

    hooks = _hooks(side_effect=barrier)
    result = await _sdk_pre_tool_cb(
        {"tool_name": "Write", "tool_input": {"file_path": "a"}, "session_id": "nat-1"},
        "toolu-2",
        None,
        hooks=hooks,
    )
    assert result == {}
    assert seen == ["Write"]
    assert is_side_effect_tool("Write") and is_side_effect_tool("Bash")
    assert not is_side_effect_tool("Read")
    assert is_dispatch_tool("Task") and is_dispatch_tool("Agent")


async def test_sdk_pre_tool_reviewer_block_wins_over_injection() -> None:
    """拦截面阻断优先于 cwd 注入（不放行被 reviewer 拒绝的派发）。"""
    hooks = _hooks(
        dispatch=lambda sid, data: _ok("/ws/child-2"),
        pre_tool=lambda sid, data: {"decision": "block", "reason": "not allowed"},
    )
    result = await _sdk_pre_tool_cb(
        {"tool_name": "Task", "tool_input": {}, "session_id": "nat-1"},
        "toolu-1",
        None,
        hooks=hooks,
    )
    assert result == {"decision": "block", "reason": "not allowed"}


async def test_sdk_pre_tool_without_barriers_keeps_legacy_shape() -> None:
    """未接屏障时回调形态与旧行为零差异（旧路径回归面）。"""
    hooks = _hooks(pre_tool=lambda sid, data: {"decision": "block", "reason": "r"})
    result = await _sdk_pre_tool_cb(
        {"tool_name": "Write", "tool_input": {}, "session_id": "nat-1"},
        "toolu-1",
        None,
        hooks=hooks,
    )
    assert result == {"decision": "block", "reason": "r"}
    plain = DriverHooks(session_id="ses-1")
    assert (
        await _sdk_pre_tool_cb(
            {"tool_name": "Write", "tool_input": {}, "session_id": "nat-1"},
            "toolu-1",
            None,
            hooks=plain,
        )
        == {}
    )


def test_sdk_hook_matchers_follow_barrier_wiring() -> None:
    """matcher 装配：派发屏障接 Task 面，副作用屏障独立可接。"""
    driver = ClaudeSDKDriver()
    with_dispatch = driver._build_sdk_hooks(_hooks(dispatch=lambda sid, data: _ok(None)))
    matchers = [m.matcher for m in with_dispatch["PreToolUse"]]
    assert any("Task" in matcher for matcher in matchers)

    side_only = driver._build_sdk_hooks(_hooks(side_effect=lambda sid, data: _ok(None)))
    assert [m.matcher for m in side_only["PreToolUse"]]

    legacy = driver._build_sdk_hooks(_hooks(pre_tool=lambda sid, data: None))
    legacy_matchers = [m.matcher for m in legacy["PreToolUse"]]
    assert "AskUserQuestion" in legacy_matchers
    assert not any("Task" in matcher for matcher in legacy_matchers)


def _ok(cwd: str | None) -> BarrierProceed:
    """放行结果桩（可带 cwd）。"""
    return BarrierProceed(child_cwd=cwd)
