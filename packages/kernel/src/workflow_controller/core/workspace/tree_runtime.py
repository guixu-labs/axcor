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

"""作用域树运行时的共享装配：Graph run 与单会话两平面共用的树组件。

树运行时把私有仓、账本、闸门、操作日志、集成协调器、管理器、检查点
账本与组仲裁装配为一件可执行单元；首个工作区动作惰性建立，重开或崩
溃后的既有仓经深度恢复重建账本（对不上账显式失败）。本模块自 run
协调器抽出，供会话宿主侧等价装配（树 id 与根作用域标识不同）。
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from workflow_controller.core.workspace.dispatch_groups import (
    DispatchGroupArbiter,
    DispatchReplayLedger,
)
from workflow_controller.core.workspace.integration import (
    ConflictPolicy,
    IntegrationCoordinator,
)
from workflow_controller.core.workspace.manager import WorkspaceManager
from workflow_controller.core.workspace.manifest import (
    DEFAULT_WRITER_LOCAL_PATHSPECS,
    WorkspaceManifest,
)
from workflow_controller.core.workspace.oplog import SagaOplog
from workflow_controller.core.workspace.recovery import (
    WorkspaceRecoveryReport,
    rebuild_workspace_tree,
)
from workflow_controller.core.workspace.repository import WorkspaceRepository
from workflow_controller.core.workspace.scope_tree import ScopeTree
from workflow_controller.plugin.dispatch_barrier import COMPAT_LATE_BARRIER
from workflow_controller.plugin.workspace import RebuildScriptDecl, WorkspaceTreeConfig

if TYPE_CHECKING:
    from workflow_controller.core.checkpoints import CheckpointStore
    from workflow_controller.core.restore.conversation import ConversationRestoreData
    from workflow_controller.core.workspace.manager import BackgroundDrainHook

__all__ = [
    "PendingSubagentRestore",
    "WorkspaceTreeRuntime",
    "load_tree_config",
    "load_tree_config_from_dir",
    "persist_tree_config",
]

logger = logging.getLogger(__name__)

# 树配置的持久化文件名（住仓树目录；崩溃后无配置动作也能按它重建树）
TREE_CONFIG_FILENAME = "tree-config.json"


def persist_tree_config(tree_dir: Path, config: WorkspaceTreeConfig) -> None:
    """树配置落盘：重开/崩溃后无配置动作（废弃/恢复/回退）的重建依据。"""
    payload = {
        "root_dir": config.root_dir,
        "conflict_policy": config.conflict_policy,
        "retain_after_merge": config.retain_after_merge,
        "claims_enforce": config.claims_enforce,
        "on_no_dispatch_barrier": config.on_no_dispatch_barrier,
        "background_drain_timeout_s": config.background_drain_timeout_s,
        "merge": list(config.merge),
        "writer_local": list(config.writer_local),
        "shared_readonly": list(config.shared_readonly),
        "shared_exclusive": list(config.shared_exclusive),
        "external": list(config.external),
        "rebuild_scripts": {
            pathspec: {"cmd": decl.cmd, "verify": decl.verify}
            for pathspec, decl in config.rebuild_scripts.items()
        },
    }
    tree_dir.mkdir(parents=True, exist_ok=True)
    (tree_dir / TREE_CONFIG_FILENAME).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def load_tree_config_from_dir(tree_dir: Path) -> WorkspaceTreeConfig | None:
    """从树目录读回持久化的树配置；未落盘或读回失败返回 None。"""
    path = tree_dir / TREE_CONFIG_FILENAME
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return WorkspaceTreeConfig(
            root_dir=str(payload["root_dir"]),
            conflict_policy=str(payload.get("conflict_policy", "git_merge")),
            retain_after_merge=bool(payload.get("retain_after_merge", False)),
            claims_enforce=bool(payload.get("claims_enforce", False)),
            on_no_dispatch_barrier=str(payload.get("on_no_dispatch_barrier", "fail")),
            background_drain_timeout_s=float(payload.get("background_drain_timeout_s", 60.0)),
            merge=tuple(str(p) for p in payload.get("merge", ())),
            writer_local=tuple(str(p) for p in payload.get("writer_local", ())),
            shared_readonly=tuple(str(p) for p in payload.get("shared_readonly", ())),
            shared_exclusive=tuple(str(p) for p in payload.get("shared_exclusive", ())),
            external=tuple(str(p) for p in payload.get("external", ())),
            rebuild_scripts={
                str(pathspec): RebuildScriptDecl(
                    cmd=str(decl.get("cmd", "")), verify=str(decl.get("verify", ""))
                )
                for pathspec, decl in dict(payload.get("rebuild_scripts", {})).items()
            },
        )
    except (OSError, ValueError, KeyError, TypeError):
        logger.warning("树配置读回失败：%s", path, exc_info=True)
        return None


def load_tree_config(work_dir: str | None, tree_id: str) -> WorkspaceTreeConfig | None:
    """按工作目录与树标识读回树配置；工作目录缺失或未落盘返回 None。"""
    if not work_dir:
        return None
    tree_dir = Path(work_dir) / ".workflow-controller" / "scopes" / tree_id
    return load_tree_config_from_dir(tree_dir)


def tree_manifest_of(config: WorkspaceTreeConfig) -> WorkspaceManifest:
    """树配置 → 路径策略清单（两平面共用的换算；空自建桶取内核默认）。"""
    return WorkspaceManifest(
        merge=tuple(config.merge),
        writer_local=tuple(config.writer_local) or DEFAULT_WRITER_LOCAL_PATHSPECS,
        shared_readonly=tuple(config.shared_readonly),
        shared_exclusive=tuple(config.shared_exclusive),
        external=tuple(config.external),
        rebuild_scripts={
            pathspec: RebuildScriptDecl(cmd=decl.cmd, verify=decl.verify)
            for pathspec, decl in config.rebuild_scripts.items()
        },
    )


@dataclass
class PendingSubagentRestore:
    """子代理回滚链的待绑定恢复：数据就绪，会话在确认后创建。"""

    data: ConversationRestoreData
    bound_session_id: str | None = None


class WorkspaceTreeRuntime:
    """一棵作用域树的运行时装配：管理器、账本与组仲裁的就绪形态。

    树随首个工作区动作惰性建立；私有仓已存在时走深度恢复重建账本。
    Graph run 与单会话共用本装配，差异仅在树标识、根作用域命名与根
    检出落点（单会话树的根检出在树目录内的内部目录，原目录不动）。
    """

    def __init__(
        self,
        config: WorkspaceTreeConfig,
        tree_id: str,
        checkpoints: CheckpointStore,
        *,
        root_scope_id: str | None = None,
        root_checkout_subdir: str | None = None,
        drain_background_writers: BackgroundDrainHook | None = None,
    ) -> None:
        """按配置装配私有仓、清单、协调器、管理器与检查点账本。

        冲突档位词面校验失败即抛错（转失败通知），不静默取默认；
        root_checkout_subdir 非空时根检出落树目录内的该子目录（单会话
        形态），否则直接用根目录（Graph 形态）。
        """
        repository = WorkspaceRepository(Path(config.root_dir), tree_id=tree_id)
        self.tree_id = tree_id
        self.tree_dir = repository.tree_dir
        self.repository = repository
        self.replay_ledger = DispatchReplayLedger(repository.tree_dir / "dispatch")
        scope_tree = ScopeTree()
        oplog = SagaOplog(repository.tree_dir)
        integration = IntegrationCoordinator(
            repository, conflict_policy=ConflictPolicy(config.conflict_policy)
        )
        self.scope_tree = scope_tree
        self.oplog = oplog
        self.integration = integration
        self.manifest = tree_manifest_of(config)
        self.manager = WorkspaceManager(
            repository=repository,
            scope_tree=scope_tree,
            oplog=oplog,
            manifest=self.manifest,
            integration_coordinator=integration,
            drain_background_writers=drain_background_writers,
            claims_enforce=config.claims_enforce,
        )
        self.checkpoints = checkpoints
        self.arbiter = DispatchGroupArbiter(self.manager, checkpoints, self.replay_ledger)
        self.retain_after_merge = config.retain_after_merge
        self.on_no_dispatch_barrier = config.on_no_dispatch_barrier
        self.barrier_wait_timeout_s = config.background_drain_timeout_s
        self.root_scope_id = root_scope_id or f"{tree_id}-root"
        self._config = config
        self._root_checkout_subdir = root_checkout_subdir
        self._child_seq = 0
        # 子代理回滚链的待绑定恢复数据（scope_id → 恢复事实与已绑定会话）
        self.pending_restores: dict[str, PendingSubagentRestore] = {}
        # 深度恢复结论（仅经恢复路径建立时非空，供日志与观测）
        self.recovery_report: WorkspaceRecoveryReport | None = None

    @property
    def root_checkout_dir(self) -> Path:
        """根检出落点：Graph 为根目录本身，单会话为树目录内子目录。"""
        if self._root_checkout_subdir is not None:
            return self.repository.tree_dir / self._root_checkout_subdir
        return self.repository.root_dir

    @property
    def session_shaped(self) -> bool:
        """是否单会话形态（根检出在树目录内、主写者工作区固定 root/）。"""
        return self._root_checkout_subdir is not None

    async def initialize(self) -> None:
        """建立根作用域并持久化树配置；私有仓已存在时走深度恢复重建账本。"""
        if self.repository.repo_dir.exists():
            self.recovery_report = await rebuild_workspace_tree(
                repository=self.repository,
                scope_tree=self.scope_tree,
                integration=self.integration,
                manager=self.manager,
                checkpoints=self.checkpoints,
                oplog=self.oplog,
                root_scope_id=self.root_scope_id,
                root_checkout_dir=self.root_checkout_dir if self.session_shaped else None,
                main_writer_workspace_dir=(
                    self.repository.tree_dir / "root" if self.session_shaped else None
                ),
            )
            if self.recovery_report.degraded:
                logger.warning(
                    "工作区树 %s 深度恢复降级项：%s",
                    self.root_scope_id,
                    "；".join(self.recovery_report.degraded),
                )
            return
        persist_tree_config(self.repository.tree_dir, self._config)
        refs = await self.manager.initialize_root(
            scope_id=self.root_scope_id,
            root_workspace_dir=self.root_checkout_dir if self.session_shaped else None,
        )
        if self.session_shaped:
            # 单会话形态：集成线检出落在树目录内，原目录保持基线不动
            await self.integration.populate_checkout(self.root_checkout_dir, refs.integration_head)

    def next_child_identity(self, parent_scope_id: str) -> tuple[str, str]:
        """预分配一个子代理作用域键与配套操作号（键在树内不重号）。"""
        self._child_seq += 1
        scope_key = f"{parent_scope_id}-c{self._child_seq}"
        return scope_key, f"dispatch-{self._child_seq}"

    @property
    def strict_barrier(self) -> bool:
        """派发屏障是否严格档（非显式弱化即严格）。"""
        return self.on_no_dispatch_barrier != COMPAT_LATE_BARRIER
