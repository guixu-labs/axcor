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

"""通用动作 / 通知协议类型：编排层与工作区核心之间的数据契约。

动作是编排层经宿主发给核心的请求，通知是核心回投编排层的结果；多步
动作一律携带操作号供幂等与崩溃恢复核对。本模块只定义数据形态，不接
运行时；恢复与检查点类动作的类型先于此能力落地，宿主接线随相应能
力开放。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.core.workspace.integration import IntegrationConflict
    from workflow_controller.core.workspace.manifest import InheritBinding
    from workflow_controller.core.workspace.scope_tree import ScopeKind

__all__ = [
    "AbandonScope",
    "CheckpointRecorded",
    "IntegrateScope",
    "IntegrationLineRetracted",
    "PrepareScopeWorkspace",
    "RestoreCommitted",
    "RestoreScope",
    "ScopeAbandoned",
    "ScopeIntegrated",
    "ScopeIntegrationRejected",
    "ScopeRestored",
    "ScopeWorkspacePrepared",
    "ScopeWorkspaceSpec",
]


@dataclass(frozen=True)
class ScopeWorkspaceSpec:
    """建区请求的作用域规格：种类、属主、声明写路径与可选继承绑定。"""

    scope_kind: ScopeKind
    owner_ref: str | None = None
    inherit_from: InheritBinding | None = None
    claims: tuple[str, ...] = ()


@dataclass(frozen=True)
class PrepareScopeWorkspace:
    """建区动作请求：在父作用域当前头建子工作区并登记账本。

    workspace_dir 显式指定工作区落点（默认按作用域键转义落 workspaces/ 下）：
    单会话主写者用固定 ``root/`` 落点承载终态检出。
    """

    parent_scope_id: str
    scope_key: str
    spec: ScopeWorkspaceSpec
    client_op_id: str
    workspace_dir: Path | None = None


@dataclass(frozen=True)
class IntegrateScope:
    """集成动作请求：父闸门内串行集成，预检不过即拒绝。"""

    scope_id: str
    # 编排层出具的验收回执，核心不解读只随账留存
    receipt: str | None
    client_op_id: str


@dataclass(frozen=True)
class RestoreScope:
    """恢复动作请求：把作用域文件态恢复到检查点，代次不符拒绝。"""

    scope_id: str
    checkpoint_id: str
    client_op_id: str
    expected_epoch: int


@dataclass(frozen=True)
class AbandonScope:
    """废弃动作请求：废弃作用域及后代工作区并各自换代。"""

    scope_id: str
    expected_scope_epoch: int
    client_op_id: str


@dataclass(frozen=True)
class RestoreCommitted:
    """恢复落账确认：编排层失效计划已持久化，核心方可继续重建。"""

    scope_id: str
    client_op_id: str


@dataclass(frozen=True)
class ScopeWorkspacePrepared:
    """建区完成通知：新作用域身份、代次、两个基线提交与工作区路径。"""

    scope_id: str
    scope_epoch: int
    base_commit: str
    integration_base_commit: str
    workspace_path: str


@dataclass(frozen=True)
class ScopeIntegrated:
    """集成成功通知：父线新头与写者进入父树的路径清单。"""

    scope_id: str
    parent_scope_id: str
    new_parent_head: str
    integrated_paths: tuple[str, ...]


@dataclass(frozen=True)
class ScopeIntegrationRejected:
    """集成拒绝通知：冲突载荷含冲突路径与双方身份。"""

    scope_id: str
    parent_scope_id: str
    conflict: IntegrationConflict


@dataclass(frozen=True)
class ScopeRestored:
    """恢复完成通知：作用域新代次与当前根代次。"""

    scope_id: str
    new_scope_epoch: int
    root_epoch: int
    checkpoint_id: str = ""
    abandoned_descendants: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class ScopeAbandoned:
    """废弃完成通知：目标与一并废弃的后代各自新代次。"""

    scope_id: str
    new_scope_epoch: int
    abandoned_descendants: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class IntegrationLineRetracted:
    """集成线回退完成通知：新线头、新根代次与失效/保留划分。

    target_commit 为回退点（线上有集成提交的各目标建区基线在首父链
    上的最早者，目标集全为空集成时即当前头）；invalidated 为失效作
    域及其新代次（全部撤回目标在列）；preserved 只列被重放的保留者
    （空集成与回退点之前已集成的保留者不在其中，也不在 invalidated
    内）；线不动时 new_root_head 即旧头、new_root_epoch 不变。
    """

    new_root_head: str
    new_root_epoch: int
    target_commit: str
    invalidated: tuple[tuple[str, int], ...] = ()
    preserved: tuple[str, ...] = ()


@dataclass(frozen=True)
class CheckpointRecorded:
    """检查点落账通知：生命周期点自动打点的回执。"""

    scope_id: str
    checkpoint_id: str
