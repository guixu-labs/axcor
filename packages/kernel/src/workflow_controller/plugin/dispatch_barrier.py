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

"""派发屏障能力矩阵——各 runtime 的五项屏障能力声明与 fail-closed 门。

内核 run 平面与编排插件共用本模块：屏障链在受理派发/副作用屏障时按
声明执行严格模式门（缺子代理 cwd 注入能力即拒绝 writer 子代理、缺
message 级组仲裁能力即拒绝派发），编排层在出生规格侧按同一声明禁用
task 工具。声明是代码级核验结论的落档，核验依据见开发日志 S4a 条。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

__all__ = [
    "COMPAT_LATE_BARRIER",
    "DISPATCH_CAPABILITIES_BY_RUNTIME",
    "DISPATCH_TOOL_NAMES",
    "DispatchCapability",
    "RuntimeDispatchCapabilities",
    "dispatch_capabilities_for",
    "group_arbitration_block_reason",
    "writer_dispatch_block_reason",
]

# 显式弱化档词面：沿用现有事后拦截打点，检查点标注 late_barrier
COMPAT_LATE_BARRIER = "compat_late_barrier"

# task 派发工具名（cc 词面；opencode 驱动内映射为 task 类目）
DISPATCH_TOOL_NAMES = ("Task", "Agent")


class DispatchCapability(StrEnum):
    """派发屏障五项能力：拦截 / cwd 注入 / 本地静止 / 锚点 / 组仲裁。"""

    TASK_PRE_INTERCEPT = "task_pre_intercept"
    CHILD_CWD_INJECTION = "child_cwd_injection"
    PARENT_LOCAL_QUIESCE = "parent_local_quiesce"
    PRE_DISPATCH_ANCHOR = "pre_dispatch_anchor"
    SIDE_EFFECT_ARBITRATION = "side_effect_arbitration"


@dataclass(frozen=True)
class RuntimeDispatchCapabilities:
    """单个 runtime 的能力声明集。"""

    runtime: str
    capabilities: frozenset[DispatchCapability]

    def supports(self, capability: DispatchCapability) -> bool:
        """是否声明了指定能力。"""
        return capability in self.capabilities


_NO_CAPABILITIES = frozenset[DispatchCapability]()

# claude SDK（进程内 hook，经 claude_agent_sdk 0.2.184 + 捆绑 CLI 2.1.327
# 输入面核验）：PreToolUse 回调先于工具执行（①）；Task 输入含 cwd 参数、
# PreToolUse 可返回 updatedInput 重写工具输入（②）；父会话停在工具调用
# 上结构性静止（③）；回调载荷带 session/tool_use/prompt 上下文、锚点可
# 在派发前形成（④）；写文件类工具前置拦截 matcher 可自建（⑤）。
_SDK_CAPS = frozenset(DispatchCapability)

DISPATCH_CAPABILITIES_BY_RUNTIME: dict[str, RuntimeDispatchCapabilities] = {
    "claude_sdk": RuntimeDispatchCapabilities("claude_sdk", _SDK_CAPS),
    # claude CLI 的 hook matcher 面由部署侧 settings.json 管理、不受本仓
    # 控制，前置屏障不可声明（SubagentStart 亦为轮询事后通知）
    "claude_cli": RuntimeDispatchCapabilities(
        "claude_cli", frozenset({DispatchCapability.PARENT_LOCAL_QUIESCE})
    ),
    # opencode 经 DB 轮询发现子会话，五项全缺
    "opencode": RuntimeDispatchCapabilities("opencode", _NO_CAPABILITIES),
    # codex CLI command hook 只回流 Stop/Subagent 生命周期，无前置工具面
    "codex_cli": RuntimeDispatchCapabilities("codex_cli", _NO_CAPABILITIES),
}


def dispatch_capabilities_for(runtime: str | None) -> RuntimeDispatchCapabilities:
    """按 runtime 名取能力声明；未知名按全缺处理。"""
    name = (runtime or "").strip()
    return DISPATCH_CAPABILITIES_BY_RUNTIME.get(
        name, RuntimeDispatchCapabilities(name or "unknown", _NO_CAPABILITIES)
    )


def writer_dispatch_block_reason(caps: RuntimeDispatchCapabilities, *, strict: bool) -> str | None:
    """严格模式下缺子代理 cwd 注入能力的 writer 派发拒绝理由。"""
    if not strict or caps.supports(DispatchCapability.CHILD_CWD_INJECTION):
        return None
    return (
        f"runtime {caps.runtime} 缺少子代理 cwd 注入能力（②），"
        "严格模式下拒绝启动 writer 子代理：无 cwd 注入则工作区隔离本身不成立，"
        "可改用 on_no_dispatch_barrier: compat_late_barrier 显式弱化（恢复点保证随之弱化）"
    )


def group_arbitration_block_reason(
    caps: RuntimeDispatchCapabilities, *, strict: bool
) -> str | None:
    """严格模式下缺 message 级组仲裁能力的副作用/派发拒绝理由。"""
    if not strict or caps.supports(DispatchCapability.SIDE_EFFECT_ARBITRATION):
        return None
    return (
        f"runtime {caps.runtime} 缺少副作用工具前置拦截与 message 级组仲裁能力（⑤），"
        "严格模式下组检查点无法先于 message 内全部副作用（如「写文件工具 → task」"
        "顺序下组恢复点会落在写副作用之后），fail-closed 拒绝；"
        "可改用 on_no_dispatch_barrier: compat_late_barrier 显式弱化（标注 late_barrier）"
    )
