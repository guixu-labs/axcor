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

"""工作区作用域动作与通知——编排驱动与宿主执行面之间的文件态协议。

驱动以动作请求建立、废弃作用域工作区等文件态操作，宿主执行后以通知
回投结果；失败同样显式回投，不静默丢弃。本模块只承载 DTO，字段保持
可 JSON 序列化；内核实现与编排插件互不因本协议产生直接依赖，映射与
执行归宿主。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

from workflow_controller.plugin.run_driver import RunAction, RunNotification

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "AbandonScope",
    "CommitScopeTerminal",
    "IntegrateScope",
    "IntegrationLineRetracted",
    "PrepareScopeWorkspace",
    "RebuildScriptDecl",
    "RestoreCommitted",
    "RestoreScope",
    "RestoreSessionBound",
    "RetractIntegrationLine",
    "RetractTarget",
    "ScopeAbandoned",
    "ScopeIntegrated",
    "ScopeIntegrationRejected",
    "ScopeRestored",
    "ScopeTerminalCommitted",
    "ScopeWorkspacePrepared",
    "WorkspaceActionFailed",
    "WorkspaceScopeKind",
    "WorkspaceTreeConfig",
]


class WorkspaceScopeKind(StrEnum):
    """作用域种类词表：编排层对执行单元的形态声明。"""

    MAIN_WRITER = "main_writer"
    SUBAGENT = "subagent"


@dataclass(frozen=True)
class RebuildScriptDecl:
    """可重建路径的确定性脚本与验收命令声明。"""

    cmd: str
    verify: str


@dataclass(frozen=True)
class WorkspaceTreeConfig:
    """一棵作用域树的装配配置：根目录、路径策略、冲突档位与 claims 档位。

    首个到达宿主的工作区动作携带本配置完成惰性装配，其后动作携带的
    配置不再生效；未给出的策略桶与冲突档位由宿主取默认。
    retain_after_merge 为真时集成后保留作用域工作区（默认释放）；
    claims_enforce 为真时出生期声明写路径相交即拒绝后出生单元。
    """

    root_dir: str
    conflict_policy: str = "git_merge"
    retain_after_merge: bool = False
    claims_enforce: bool = False
    # 派发屏障弱化档：fail（默认，严格）| compat_late_barrier（显式弱化，
    # 检查点标注 late_barrier）；驱动屏障受理与编排出生侧共用
    on_no_dispatch_barrier: str = "fail"
    # 屏障有界等待窗口（秒），与后台写进程排空窗口同界
    background_drain_timeout_s: float = 60.0
    merge: tuple[str, ...] = ()
    writer_local: tuple[str, ...] = ()
    shared_readonly: tuple[str, ...] = ()
    shared_exclusive: tuple[str, ...] = ()
    external: tuple[str, ...] = ()
    rebuild_scripts: Mapping[str, RebuildScriptDecl] = field(default_factory=dict)


@dataclass(frozen=True)
class PrepareScopeWorkspace(RunAction):
    """建区动作：宿主在父作用域当前头建子工作区并登记账本。

    writes 为节点声明的写路径 glob（claims 软预检登记面），可空。
    """

    config: WorkspaceTreeConfig
    scope_key: str
    client_op_id: str
    parent_scope_id: str | None = None  # None = 本 run 的根作用域
    scope_kind: str = WorkspaceScopeKind.MAIN_WRITER
    owner_ref: str | None = None
    writes: tuple[str, ...] = ()
    context: str | None = None


@dataclass(frozen=True)
class AbandonScope(RunAction):
    """废弃动作：宿主移除作用域及后代工作区并各自换代。"""

    scope_id: str
    expected_scope_epoch: int
    client_op_id: str
    context: str | None = None


@dataclass(frozen=True)
class IntegrateScope(RunAction):
    """集成动作：宿主在父作用域闸门内串行集成，冲突显式拒绝回执。"""

    scope_id: str
    client_op_id: str
    receipt: str | None = None
    context: str | None = None


@dataclass(frozen=True)
class CommitScopeTerminal(RunAction):
    """终态提交动作：宿主在闸门内提交作用域执行态并落 terminal 检查点。"""

    scope_id: str
    client_op_id: str
    context: str | None = None


@dataclass(frozen=True)
class RestoreCommitted(RunAction):
    """失效计划已持久化确认：核心此后方可重建工作区。

    无独立回执通知——紧随的建区/绑定动作回执即确认（核心重建闸门接线
    前本动作为纯确认，宿主接受即生效）。restore_binding 为真时表示子
    代理回滚链：宿主须在此后创建并绑定恢复 session，恢复数据缺席即
    动作失败。
    """

    scope_id: str
    client_op_id: str
    context: str | None = None
    restore_binding: bool = False


@dataclass(frozen=True)
class RestoreScope(RunAction):
    """组检查点恢复动作：宿主把父作用域文件态恢复到组恢复点。

    撤回该组全部副作用（scope_epoch+1、后代子作用域废弃），并准备
    对话恢复数据（截断 + 种子，不启动新 session）；replay_dispatch
    为真时按组台账校验并携带原始 task 调用参数。
    """

    scope_id: str
    checkpoint_id: str
    expected_scope_epoch: int
    client_op_id: str
    session_runtime: str | None = None
    replay_dispatch: bool = False
    context: str | None = None


@dataclass(frozen=True)
class RetractTarget:
    """集成线回退的单个撤回目标：作用域与其建区基线检查点。"""

    scope_id: str
    checkpoint_id: str


@dataclass(frozen=True)
class RetractIntegrationLine(RunAction):
    """集成线回退动作：宿主把集成线回退到目标集建区基线的最早者（空集成目标不参与）。

    逐目标先校验后 reset（在册、属当前集成线、是当前头祖先、区间不
    含自建路径变更；线上无集成提交的空集成目标免线位置与区间校验），
    任一不满足零改动拒绝；保留者按原集成序换序重放，根代次随线动 +1
    （目标集全为空集成时线不动、根代次不推进）、失效作用域废弃；
    expected_root_epoch 为受理时的根代次，不匹配拒绝。
    """

    targets: tuple[RetractTarget, ...]
    expected_root_epoch: int
    client_op_id: str
    context: str | None = None


@dataclass(frozen=True)
class ScopeRestored(RunNotification):
    """组检查点恢复完成通知：新代次、一并废弃的后代与恢复数据就绪。"""

    scope_id: str
    new_scope_epoch: int
    checkpoint_id: str
    abandoned_descendants: tuple[tuple[str, int], ...] = ()
    context: str | None = None


@dataclass(frozen=True)
class IntegrationLineRetracted(RunNotification):
    """集成线回退完成通知：新线头、新根代次与失效/保留划分。

    target_commit 为回退点（线上有集成提交的各目标建区基线在首父链
    上的最早者，目标集全为空集成时即当前头）；invalidated 为失效作用
    域及其新代次（全部撤回目标在列）；preserved 只列被重放的保留者
    （空集成与回退点之前已集成的保留者不重放、不在其中，也不在
    invalidated 内）；线不动时 new_root_head 即旧头、new_root_epoch
    不变。
    """

    new_root_head: str
    new_root_epoch: int
    target_commit: str
    invalidated: tuple[tuple[str, int], ...] = ()
    preserved: tuple[str, ...] = ()
    context: str | None = None


@dataclass(frozen=True)
class RestoreSessionBound(RunNotification):
    """恢复会话绑定完成通知：新会话已按原 run/作用域/新代次绑定。"""

    scope_id: str
    session_id: str
    checkpoint_id: str
    context: str | None = None


@dataclass(frozen=True)
class ScopeWorkspacePrepared(RunNotification):
    """建区完成通知：工作区路径供宿主注入执行单元的出生目录。"""

    scope_id: str
    scope_epoch: int
    base_commit: str
    integration_base_commit: str
    workspace_path: str
    context: str | None = None


@dataclass(frozen=True)
class ScopeAbandoned(RunNotification):
    """废弃完成通知：目标与一并废弃的后代各自新代次。"""

    scope_id: str
    new_scope_epoch: int
    abandoned_descendants: tuple[tuple[str, int], ...] = ()
    context: str | None = None


@dataclass(frozen=True)
class ScopeTerminalCommitted(RunNotification):
    """终态提交完成通知：执行态提交与 terminal 检查点已落账。"""

    scope_id: str
    commit: str
    checkpoint_id: str
    context: str | None = None


@dataclass(frozen=True)
class ScopeIntegrated(RunNotification):
    """集成成功通知：父线新头与写者进入父树的路径清单。"""

    scope_id: str
    parent_scope_id: str
    new_parent_head: str
    integrated_paths: tuple[str, ...] = ()
    context: str | None = None


@dataclass(frozen=True)
class ScopeIntegrationRejected(RunNotification):
    """集成拒绝通知：冲突路径与双侧提交，节点按验收失败处理。"""

    scope_id: str
    parent_scope_id: str
    conflict_paths: tuple[str, ...] = ()
    base_commit: str = ""
    writer_head: str = ""
    parent_head: str = ""
    context: str | None = None


@dataclass(frozen=True)
class WorkspaceActionFailed(RunNotification):
    """动作失败回执：错误串供编排层按执行失败收口。"""

    action: str
    client_op_id: str
    error: str
    context: str | None = None
