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

"""终态三方同步单测：基线 CAS、删除生效、基线外新增与保留路径排除。"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.workspace.actions import (
    PrepareScopeWorkspace,
    ScopeWorkspaceSpec,
)
from workflow_controller.core.workspace.integration import (
    ConflictPolicy,
    IntegrationCoordinator,
)
from workflow_controller.core.workspace.manager import WorkspaceManager
from workflow_controller.core.workspace.manifest import RebuildScript, WorkspaceManifest
from workflow_controller.core.workspace.oplog import SagaOplog
from workflow_controller.core.workspace.repository import (
    INNER_GIT_RESERVED_PATH,
    WorkspaceRepository,
)
from workflow_controller.core.workspace.scope_tree import ScopeKind, ScopeTree
from workflow_controller.core.workspace.terminal_sync import sync_terminal_to_original

from ._support import build_workspace, make_user_root

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.asyncio


async def _prepare_writer(manager, *, client_op_id: str = "prepare-1") -> None:
    """建根后建首层主写者工作区（自集成头检出、seed 物化自建路径）。"""
    await manager.initialize_root(scope_id="t-root")
    await manager.prepare_scope_workspace(
        PrepareScopeWorkspace(
            parent_scope_id="t-root",
            scope_key="t-main",
            spec=ScopeWorkspaceSpec(scope_kind=ScopeKind.MAIN_WRITER),
            client_op_id=client_op_id,
        )
    )


def _writer_dir(root: Path) -> Path:
    """首层主写者工作区固定落点。"""
    return root / ".workflow-controller" / "scopes" / "run-1" / "workspaces" / "t-main"


async def _sync(root, repository, manifest):
    """按「seed 基线 + 主写者终态 → 原目录」执行三方同步。"""
    return await sync_terminal_to_original(
        repository=repository,
        workspace_dir=_writer_dir(root),
        original_dir=root,
        manifest=manifest,
    )


async def test_terminal_sync_applies_modify_add_and_delete(tmp_path):
    """终态同步：修改/新增写回、基线内删除生效、基线外用户新增不动。"""
    root, repository, manager = build_workspace(tmp_path)
    await _prepare_writer(manager)
    writer = _writer_dir(root)

    # 托管终态：改基线文件、删基线文件、新增文件
    (writer / "design" / "base.md").write_text("rewritten\n", encoding="utf-8")
    (writer / "run.log").unlink()
    (writer / "design" / "new.md").write_text("fresh\n", encoding="utf-8")
    # 原目录基线外用户新增：同步不得触碰
    (root / "user-extra.txt").write_text("keep me\n", encoding="utf-8")

    report = await _sync(root, repository, manager._manifest)

    assert not report.refused
    assert sorted(report.applied) == ["design/base.md", "design/new.md"]
    assert report.removed == ("run.log",)
    assert (root / "design" / "base.md").read_text(encoding="utf-8") == "rewritten\n"
    assert not (root / "run.log").exists()
    assert (root / "design" / "new.md").read_text(encoding="utf-8") == "fresh\n"
    assert (root / "user-extra.txt").read_text(encoding="utf-8") == "keep me\n"


async def test_terminal_sync_stops_on_external_change(tmp_path):
    """外部变化：停止覆盖、按路径报告冲突、原目录保留用户版本。"""
    root, repository, manager = build_workspace(tmp_path)
    await _prepare_writer(manager)
    writer = _writer_dir(root)
    (writer / "design" / "base.md").write_text("terminal\n", encoding="utf-8")
    # 原目录基线内文件被外部修改
    (root / "design" / "base.md").write_text("user edit\n", encoding="utf-8")

    report = await _sync(root, repository, manager._manifest)

    assert report.refused
    assert report.conflict_paths == ("design/base.md",)
    assert report.applied == () and report.removed == ()
    assert (root / "design" / "base.md").read_text(encoding="utf-8") == "user edit\n"
    assert (writer / "design" / "base.md").read_text(encoding="utf-8") == "terminal\n"


async def test_terminal_sync_external_delete_is_conflict(tmp_path):
    """基线内文件被外部删除：按冲突报告，不在同步中复活。"""
    root, repository, manager = build_workspace(tmp_path)
    await _prepare_writer(manager)
    (root / "design" / "base.md").unlink()

    report = await _sync(root, repository, manager._manifest)

    assert report.conflict_paths == ("design/base.md",)
    assert report.applied == () and report.removed == ()
    assert not (root / "design" / "base.md").exists()


async def test_terminal_sync_skips_reserved_and_rebuildable(tmp_path):
    """保留路径不参与同步；声明重建脚本的自建路径不写回原目录。"""
    root = tmp_path / "root"
    root.mkdir()
    make_user_root(root)
    manifest = WorkspaceManifest(
        writer_local=("op/build/**",),
        rebuild_scripts={
            "op/build/**": RebuildScript(cmd="tools/rebuild.sh", verify="tools/verify.sh")
        },
    )
    repository = WorkspaceRepository(root, "run-1")
    manager = WorkspaceManager(
        repository=repository,
        scope_tree=ScopeTree(),
        oplog=SagaOplog(repository.tree_dir),
        manifest=manifest,
        integration_coordinator=IntegrationCoordinator(
            repository, conflict_policy=ConflictPolicy.GIT_MERGE
        ),
    )
    await _prepare_writer(manager)
    writer = _writer_dir(root)
    # 可重建路径建区留空后重建出内容、基线文件照常改
    (writer / "op" / "build").mkdir(parents=True)
    (writer / "op" / "build" / "rebuilt.bin").write_bytes(b"\x00rebuilt")
    (writer / "design" / "base.md").write_text("rewritten\n", encoding="utf-8")

    report = await _sync(root, repository, manifest)

    assert not report.refused
    assert report.applied == ("design/base.md",)
    assert not (root / "op" / "build" / "rebuilt.bin").exists()
    assert (root / "op" / "build" / "artifact.bin").exists()  # 基线产物按原样保留
    assert INNER_GIT_RESERVED_PATH not in report.applied + report.removed


async def test_terminal_sync_new_symlink_in_terminal(tmp_path):
    """终态新增符号链接：写回为符号链接本体（非解引用拷贝）。"""
    root, repository, manager = build_workspace(tmp_path)
    await _prepare_writer(manager)
    writer = _writer_dir(root)
    (writer / "design" / "target.txt").write_text("t\n", encoding="utf-8")
    (writer / "design" / "link.md").symlink_to("target.txt")

    report = await _sync(root, repository, manager._manifest)

    assert not report.refused
    assert "design/link.md" in report.applied
    assert (root / "design" / "link.md").is_symlink()
    assert (root / "design" / "link.md").read_text(encoding="utf-8") == "t\n"


async def test_terminal_sync_no_change_is_noop(tmp_path):
    """终态与基线一致：零写回零删除（幂等空同步）。"""
    root, repository, manager = build_workspace(tmp_path)
    await _prepare_writer(manager)

    report = await _sync(root, repository, manager._manifest)

    assert not report.refused
    assert report.applied == () and report.removed == ()
