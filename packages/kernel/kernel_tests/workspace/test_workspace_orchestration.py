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

"""假编排全链路测试：动作协议驱动建区、隔离、集成、废弃与回收。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from workflow_controller.core.workspace.actions import (
    ScopeIntegrated,
    ScopeIntegrationRejected,
)
from workflow_controller.core.workspace.manifest import RebuildScript, WorkspaceManifest
from workflow_controller.core.workspace.oplog import OPLOG_FILENAME
from workflow_controller.core.workspace.scope_tree import ScopeKind, ScopeState

from ._support import FakeOrchestrator, build_workspace, run_git

if TYPE_CHECKING:
    from workflow_controller.core.workspace.repository import WorkspaceRepository


def _combination_manifest() -> WorkspaceManifest:
    """四组合路径策略：默认进线 + 自建两类 + 外部路径。"""
    return WorkspaceManifest(
        external=("vendor/**",),
        rebuild_scripts={
            "op/build/**": RebuildScript(cmd="tools/rebuild.sh", verify="tools/verify.sh")
        },
    )


def _branch_tree_paths(repository: WorkspaceRepository, scope_key: str) -> list[str]:
    """列出作用域分支头包含的路径清单。"""
    return run_git(
        ["ls-tree", "-r", "--name-only", f"scope/{scope_key}"], repository.repo_dir
    ).splitlines()


async def test_parallel_writers_isolated_then_serially_integrated(tmp_path: Path) -> None:
    """双写者并行隔离、互不可见，验收通过后按序集成入根检出。"""
    root, _repository, manager = build_workspace(tmp_path)
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared_a = await orchestrator.prepare("node-a")
    prepared_b = await orchestrator.prepare("node-b")
    workspace_a = Path(prepared_a.workspace_path)
    workspace_b = Path(prepared_b.workspace_path)

    (workspace_a / "design" / "a.md").write_text("from-a\n", encoding="utf-8")

    # 隔离期间互不可见：根检出与兄弟工作区都看不到未集成的产出
    assert not (root / "design" / "a.md").exists()
    assert not (workspace_b / "design" / "a.md").exists()

    integrated_a = await orchestrator.integrate("node-a")
    assert isinstance(integrated_a, ScopeIntegrated)
    assert integrated_a.integrated_paths == ("design/a.md",)
    assert integrated_a.parent_scope_id == orchestrator.root_scope_id
    assert (root / "design" / "a.md").read_text(encoding="utf-8") == "from-a\n"

    (workspace_b / "design" / "b.md").write_text("from-b\n", encoding="utf-8")
    integrated_b = await orchestrator.integrate("node-b")
    assert isinstance(integrated_b, ScopeIntegrated)
    assert (root / "design" / "b.md").read_text(encoding="utf-8") == "from-b\n"
    assert integrated_b.new_parent_head == await manager.current_integration_head()


async def test_path_policy_combinations_behave_per_dimension(tmp_path: Path) -> None:
    """四组合路径策略：进线、物化、留空与排除各按其维度生效。"""
    root, repository, manager = build_workspace(tmp_path, manifest=_combination_manifest())
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared = await orchestrator.prepare("node-all")
    workspace = Path(prepared.workspace_path)

    # 建区时：随检查点的自建路径从种子物化；可重建与外部路径留空
    assert (workspace / "run.log").read_text(encoding="utf-8") == "root-log\n"
    assert (workspace / "sub" / "x.log").exists()
    assert not (workspace / "op" / "build" / "artifact.bin").exists()
    assert not (workspace / "vendor").exists()

    (workspace / "design" / "report.md").write_text("report\n", encoding="utf-8")
    (workspace / "run.log").write_text("writer-log\n", encoding="utf-8")
    (workspace / "op" / "build").mkdir(parents=True)
    (workspace / "op" / "build" / "new.out").write_bytes(b"\x07new")
    (workspace / "vendor").mkdir()
    (workspace / "vendor" / "ext.dat").write_text("ext\n", encoding="utf-8")

    integrated = await orchestrator.integrate("node-all")

    # 只有进线路径进入集成线与根检出
    assert isinstance(integrated, ScopeIntegrated)
    assert integrated.integrated_paths == ("design/report.md",)
    assert (root / "design" / "report.md").read_text(encoding="utf-8") == "report\n"
    assert (root / "run.log").read_text(encoding="utf-8") == "root-log\n"
    assert not (root / "op" / "build" / "new.out").exists()
    assert not (root / "vendor" / "ext.dat").exists()

    # 作用域分支：随检查点的自建修改入检查点提交，可重建与外部不入
    branch_paths = _branch_tree_paths(repository, "node-all")
    assert "design/report.md" in branch_paths
    assert "run.log" in branch_paths
    assert not any(path.startswith("op/build/") for path in branch_paths)
    assert not any(path.startswith("vendor/") for path in branch_paths)


async def test_writer_local_only_change_with_advanced_parent_does_not_conflict(
    tmp_path: Path,
) -> None:
    """写者只改自建路径：父线已前进也不误报冲突，集成即空操作。"""
    root, _repository, manager = build_workspace(tmp_path)
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared_a = await orchestrator.prepare("node-local-only")
    prepared_b = await orchestrator.prepare("node-line-writer")
    workspace_a = Path(prepared_a.workspace_path)
    workspace_b = Path(prepared_b.workspace_path)

    (workspace_a / "run.log").write_text("local-only-log\n", encoding="utf-8")
    (workspace_a / "sub" / "x.log").write_text("local-sub-log\n", encoding="utf-8")
    (workspace_b / "design" / "b.md").write_text("from-b\n", encoding="utf-8")

    integrated_b = await orchestrator.integrate("node-line-writer")
    integrated_a = await orchestrator.integrate("node-local-only")

    assert isinstance(integrated_b, ScopeIntegrated)
    assert isinstance(integrated_a, ScopeIntegrated)
    assert integrated_a.integrated_paths == ()
    assert integrated_a.new_parent_head == integrated_b.new_parent_head
    assert (root / "run.log").read_text(encoding="utf-8") == "root-log\n"


async def test_real_conflict_rejected_and_scope_marked_conflicted(tmp_path: Path) -> None:
    """真实合并冲突：集成不发生，冲突载荷齐全，账本转冲突态。"""
    root, _repository, manager = build_workspace(tmp_path)
    (root / "design" / "notes.txt").write_text("v0\n", encoding="utf-8")
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared_a = await orchestrator.prepare("node-a")
    prepared_b = await orchestrator.prepare("node-b")
    workspace_a = Path(prepared_a.workspace_path)
    workspace_b = Path(prepared_b.workspace_path)

    (workspace_a / "design" / "notes.txt").write_text("A\n", encoding="utf-8")
    (workspace_b / "design" / "notes.txt").write_text("B\n", encoding="utf-8")

    integrated_a = await orchestrator.integrate("node-a")
    rejected_b = await orchestrator.integrate("node-b")

    assert isinstance(integrated_a, ScopeIntegrated)
    assert isinstance(rejected_b, ScopeIntegrationRejected)
    assert rejected_b.conflict.conflict_paths == ("design/notes.txt",)
    assert rejected_b.conflict.writer_scope_id == "node-b"
    assert rejected_b.conflict.parent_scope_id == orchestrator.root_scope_id
    assert rejected_b.conflict.base_commit == prepared_b.integration_base_commit
    assert (root / "design" / "notes.txt").read_text(encoding="utf-8") == "A\n"
    assert (await manager.get_scope("node-b")).state is ScopeState.CONFLICTED
    assert (await manager.get_scope("node-a")).state is ScopeState.INTEGRATED


async def test_subagent_integrates_into_parent_scope_before_root(tmp_path: Path) -> None:
    """子代理先并入父作用域：根线不动，父集成时产出随之上线。"""
    root, _repository, manager = build_workspace(tmp_path)
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared_parent = await orchestrator.prepare("node-parent")
    prepared_child = await orchestrator.prepare(
        "sub-child", parent_scope_id="node-parent", scope_kind=ScopeKind.SUBAGENT
    )
    parent_workspace = Path(prepared_parent.workspace_path)
    child_workspace = Path(prepared_child.workspace_path)
    head_before = await manager.current_integration_head()

    (child_workspace / "design" / "sub.md").write_text("from-sub\n", encoding="utf-8")
    integrated_child = await orchestrator.integrate("sub-child")

    assert isinstance(integrated_child, ScopeIntegrated)
    assert integrated_child.parent_scope_id == "node-parent"
    assert (parent_workspace / "design" / "sub.md").read_text(encoding="utf-8") == "from-sub\n"
    assert not (root / "design" / "sub.md").exists()
    assert await manager.current_integration_head() == head_before

    (parent_workspace / "design" / "main.md").write_text("from-parent\n", encoding="utf-8")
    integrated_parent = await orchestrator.integrate("node-parent")

    assert isinstance(integrated_parent, ScopeIntegrated)
    assert sorted(integrated_parent.integrated_paths) == ["design/main.md", "design/sub.md"]
    assert (root / "design" / "sub.md").read_text(encoding="utf-8") == "from-sub\n"
    assert (root / "design" / "main.md").read_text(encoding="utf-8") == "from-parent\n"


async def test_abandon_releases_workspace_and_rebuild_uses_latest_head(tmp_path: Path) -> None:
    """废弃换代后按当前集成头重建：新基线含兄弟已集成成果。"""
    root, _repository, manager = build_workspace(tmp_path)
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared_keep = await orchestrator.prepare("node-keep")
    prepared_gone = await orchestrator.prepare("node-gone")
    workspace_keep = Path(prepared_keep.workspace_path)
    workspace_gone = Path(prepared_gone.workspace_path)

    (workspace_keep / "design" / "keep.md").write_text("keep\n", encoding="utf-8")
    await orchestrator.integrate("node-keep")

    abandoned = await orchestrator.abandon("node-gone", expected_scope_epoch=1)

    assert abandoned.new_scope_epoch == 2
    assert not workspace_gone.exists()
    assert (await manager.get_scope("node-gone")).state is ScopeState.DISCARDED

    rebuilt = await orchestrator.prepare("node-gone")

    assert rebuilt.scope_epoch == 2
    assert rebuilt.integration_base_commit == await manager.current_integration_head()
    rebuilt_workspace = Path(rebuilt.workspace_path)
    assert rebuilt_workspace.exists()
    # 兄弟已集成的成果对新代次立即可见
    assert (rebuilt_workspace / "design" / "keep.md").read_text(encoding="utf-8") == "keep\n"

    (rebuilt_workspace / "design" / "retry.md").write_text("retry\n", encoding="utf-8")
    integrated = await orchestrator.integrate("node-gone")
    assert isinstance(integrated, ScopeIntegrated)
    assert (root / "design" / "retry.md").read_text(encoding="utf-8") == "retry\n"


async def test_scope_key_with_colon_round_trips_prepare_and_rebuild(tmp_path: Path) -> None:
    """含冒号的作用域键（运行时子图节点）建区/集成/换代重建全程可用，记录保留原键。"""
    root, repository, manager = build_workspace(tmp_path)
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared = await orchestrator.prepare("gen:child-w1")
    workspace = Path(prepared.workspace_path)
    (workspace / "design" / "sub.md").write_text("from-sub\n", encoding="utf-8")
    integrated = await orchestrator.integrate("gen:child-w1")

    assert isinstance(integrated, ScopeIntegrated)
    assert prepared.scope_id == "gen:child-w1"  # 作用域记录保留原始节点 ID
    assert (root / "design" / "sub.md").read_text(encoding="utf-8") == "from-sub\n"
    run_git(["check-ref-format", "--branch", "scope/gen%3Achild-w1"], repository.repo_dir)

    abandoned = await orchestrator.abandon("gen:child-w1", expected_scope_epoch=1)
    rebuilt = await orchestrator.prepare("gen:child-w1")

    assert abandoned.new_scope_epoch == 2
    assert rebuilt.scope_epoch == 2
    # 换代分支同样合法：转义键 + -e<代次> 后缀
    rebuilt_branch = (await manager.get_scope("gen:child-w1")).workspace_ref.rpartition("|")[2]
    assert rebuilt_branch == "scope/gen%3Achild-w1-e2"
    run_git(["check-ref-format", "--branch", rebuilt_branch], repository.repo_dir)
    assert Path(rebuilt.workspace_path).exists()


async def test_release_after_integration_prunes_worktree_keeps_branch(tmp_path: Path) -> None:
    """集成后回收：工作树裁剪、账面转已释放，分支与提交留存。"""
    _root, repository, manager = build_workspace(tmp_path)
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared = await orchestrator.prepare("node-a")
    workspace = Path(prepared.workspace_path)
    (workspace / "design" / "a.md").write_text("from-a\n", encoding="utf-8")
    integrated = await orchestrator.integrate("node-a")

    released = await orchestrator.release("node-a")

    assert released.state is ScopeState.RELEASED
    assert not workspace.exists()
    branch_head = await repository.resolve_ref("refs/heads/scope/node-a")
    assert branch_head is not None
    assert "design/a.md" in _branch_tree_paths(repository, "node-a")

    # 全部操作三阶段收口，无未完账
    oplog_payload = json.loads((repository.tree_dir / OPLOG_FILENAME).read_text(encoding="utf-8"))
    phases = {operation["phase"] for operation in oplog_payload["operations"]}
    assert phases == {"committed"}
    assert integrated.new_parent_head == await manager.current_integration_head()
