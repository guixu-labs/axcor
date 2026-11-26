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

"""工作区编排测试共享基建：用户现场、装配与假编排驱动。"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from workflow_controller.core.workspace.actions import (
    AbandonScope,
    IntegrateScope,
    PrepareScopeWorkspace,
    ScopeWorkspaceSpec,
)
from workflow_controller.core.workspace.integration import (
    ConflictPolicy,
    IntegrationCoordinator,
)
from workflow_controller.core.workspace.manager import WorkspaceManager
from workflow_controller.core.workspace.manifest import WorkspaceManifest
from workflow_controller.core.workspace.oplog import SagaOplog
from workflow_controller.core.workspace.repository import WorkspaceRepository
from workflow_controller.core.workspace.scope_tree import ScopeKind, ScopeTree

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from workflow_controller.core.workspace.actions import (
        ScopeAbandoned,
        ScopeIntegrated,
        ScopeIntegrationRejected,
        ScopeWorkspacePrepared,
    )
    from workflow_controller.core.workspace.scope_tree import ScopeRecord


def run_git(args: Sequence[str], cwd: Path) -> str:
    """测试侧 git 调用：失败即断言。"""
    proc = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def make_user_root(root: Path) -> None:
    """用户现场：源码、日志与构建产物（含嵌套目录形态）。"""
    (root / "design").mkdir(parents=True)
    (root / "design" / "base.md").write_text("base\n", encoding="utf-8")
    (root / "run.log").write_text("root-log\n", encoding="utf-8")
    (root / "sub").mkdir()
    (root / "sub" / "x.log").write_text("sub-log\n", encoding="utf-8")
    (root / "op" / "build").mkdir(parents=True)
    (root / "op" / "build" / "artifact.bin").write_bytes(b"\x00\x01artifact")


def build_workspace(
    tmp_path: Path,
    *,
    manifest: WorkspaceManifest | None = None,
    conflict_policy: ConflictPolicy = ConflictPolicy.GIT_MERGE,
    tree_id: str = "run-1",
    claims_enforce: bool = False,
) -> tuple[Path, WorkspaceRepository, WorkspaceManager]:
    """装配根目录、私有仓与工作区管理器，不初始化根作用域。"""
    root = tmp_path / "root"
    root.mkdir()
    make_user_root(root)
    repository = WorkspaceRepository(root, tree_id)
    manager = WorkspaceManager(
        repository=repository,
        scope_tree=ScopeTree(),
        oplog=SagaOplog(repository.tree_dir),
        manifest=manifest if manifest is not None else WorkspaceManifest(),
        integration_coordinator=IntegrationCoordinator(repository, conflict_policy=conflict_policy),
        claims_enforce=claims_enforce,
    )
    return root, repository, manager


class FakeOrchestrator:
    """假编排驱动：只经动作请求与结果通知同工作区核心交互。"""

    def __init__(self, manager: WorkspaceManager, *, root_scope_id: str = "run-1-root") -> None:
        """绑定管理器与根作用域标识。"""
        self._manager = manager
        self.root_scope_id = root_scope_id
        self._operation_seq = 0

    async def bootstrap_root(self) -> None:
        """建立根作用域（基线导入）。"""
        await self._manager.initialize_root(scope_id=self.root_scope_id)

    async def prepare(
        self,
        scope_key: str,
        *,
        parent_scope_id: str | None = None,
        scope_kind: ScopeKind = ScopeKind.MAIN_WRITER,
        claims: tuple[str, ...] = (),
    ) -> ScopeWorkspacePrepared:
        """派发建区动作并回传通知。"""
        self._operation_seq += 1
        return await self._manager.prepare_scope_workspace(
            PrepareScopeWorkspace(
                parent_scope_id=parent_scope_id or self.root_scope_id,
                scope_key=scope_key,
                spec=ScopeWorkspaceSpec(scope_kind=scope_kind, claims=claims),
                client_op_id=f"fake-prepare-{self._operation_seq}",
            )
        )

    async def integrate(self, scope_id: str) -> ScopeIntegrated | ScopeIntegrationRejected:
        """派发集成动作并回传通知（验收回执以通过论）。"""
        self._operation_seq += 1
        return await self._manager.integrate_scope(
            IntegrateScope(
                scope_id=scope_id,
                receipt="verified",
                client_op_id=f"fake-integrate-{self._operation_seq}",
            )
        )

    async def abandon(self, scope_id: str, expected_scope_epoch: int) -> ScopeAbandoned:
        """派发废弃动作并回传通知。"""
        self._operation_seq += 1
        return await self._manager.abandon_scope(
            AbandonScope(
                scope_id=scope_id,
                expected_scope_epoch=expected_scope_epoch,
                client_op_id=f"fake-abandon-{self._operation_seq}",
            )
        )

    async def release(self, scope_id: str) -> ScopeRecord:
        """回收作用域工作区，回传回收后的账本行。"""
        return await self._manager.release_scope(scope_id)
