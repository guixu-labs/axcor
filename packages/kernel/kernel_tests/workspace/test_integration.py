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

"""集成协调器测试：两档判定与按路径集成树（直连协调器，不经账本）。"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.workspace.integration import (
    ConflictPolicy,
    IntegrationConflict,
    IntegrationCoordinator,
    IntegrationSuccess,
    WorkspaceTarget,
)
from workflow_controller.core.workspace.manifest import WorkspaceManifest
from workflow_controller.core.workspace.repository import (
    ROOT_INTEGRATION_REF,
    WorkspaceRepository,
)

from ._support import make_user_root

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from workflow_controller.core.workspace.repository import RootRefs, ScopeWorkspace

NOTES = "design/notes.txt"
NOTES_BASE = "w1\nw2\nmid1\nmid2\np1\np2\n"


async def _setup(
    tmp_path: Path, *, policy: ConflictPolicy
) -> tuple[Path, WorkspaceRepository, IntegrationCoordinator, RootRefs]:
    """建根目录与私有仓，返回根检出、私有仓、协调器与根双引用。"""
    root = tmp_path / "root"
    root.mkdir()
    make_user_root(root)
    manifest = WorkspaceManifest()
    repository = WorkspaceRepository(root, "run-1")
    refs = await repository.import_baseline(
        writer_local_pathspecs=manifest.git_writer_local_specs()
    )
    coordinator = IntegrationCoordinator(repository, conflict_policy=policy)
    return root, repository, coordinator, refs


async def _commit_writer(
    coordinator: IntegrationCoordinator, repository: WorkspaceRepository, workspace: ScopeWorkspace
) -> str:
    """落写者未提交状态并返回其分支头。"""
    manifest = WorkspaceManifest()
    head = await coordinator.commit_pending_state(
        WorkspaceTarget(path=workspace.path, separated_env=False),
        exclude_pathspecs=manifest.git_scope_commit_exclude_specs(),
        message=f"scope {workspace.scope_key} terminal state",
    )
    assert head == await repository.resolve_ref(f"refs/heads/{workspace.branch}")
    return head


async def _integrate(
    coordinator: IntegrationCoordinator,
    repository: WorkspaceRepository,
    root: Path,
    *,
    scope_key: str,
    base: str,
    writer_head: str,
) -> IntegrationSuccess | IntegrationConflict:
    """把写者并入根集成线。"""
    manifest = WorkspaceManifest()
    parent_head = await repository.resolve_ref(ROOT_INTEGRATION_REF)
    return await coordinator.integrate_writer(
        writer_scope_id=scope_key,
        parent_scope_id="root",
        base_commit=base,
        writer_head=writer_head,
        parent_head=parent_head,
        parent_target=WorkspaceTarget(path=root, separated_env=True),
        merge_pathspecs=manifest.git_diff_restriction_specs(),
        commit_message=f"integrate {scope_key} [client-op:test]",
    )


async def test_disjoint_paths_from_parallel_writers_both_integrated(tmp_path: Path) -> None:
    """并行写者改不相交路径：先后集成，根检出含双方产出。"""
    root, repository, coordinator, refs = await _setup(tmp_path, policy=ConflictPolicy.GIT_MERGE)
    writer_a = await repository.create_scope_workspace("node-a", base_commit=refs.integration_head)
    writer_b = await repository.create_scope_workspace("node-b", base_commit=refs.integration_head)

    (writer_a.path / "design" / "a.md").write_text("from-a\n", encoding="utf-8")
    (writer_b.path / "design" / "b.md").write_text("from-b\n", encoding="utf-8")
    head_a = await _commit_writer(coordinator, repository, writer_a)
    head_b = await _commit_writer(coordinator, repository, writer_b)

    first = await _integrate(
        coordinator,
        repository,
        root,
        scope_key="node-a",
        base=refs.integration_head,
        writer_head=head_a,
    )
    second = await _integrate(
        coordinator,
        repository,
        root,
        scope_key="node-b",
        base=refs.integration_head,
        writer_head=head_b,
    )

    assert isinstance(first, IntegrationSuccess) and first.integrated_paths == ("design/a.md",)
    assert isinstance(second, IntegrationSuccess) and second.integrated_paths == ("design/b.md",)
    assert (root / "design" / "a.md").read_text(encoding="utf-8") == "from-a\n"
    assert (root / "design" / "b.md").read_text(encoding="utf-8") == "from-b\n"
    assert second.new_head == await repository.resolve_ref(ROOT_INTEGRATION_REF)


async def test_git_merge_policy_auto_merges_disjoint_hunks_of_same_file(tmp_path: Path) -> None:
    """可合并档：同文件不相交片段的并行修改自动合并放行。"""
    root, repository, coordinator, _refs = await _setup(tmp_path, policy=ConflictPolicy.GIT_MERGE)
    (root / "design" / "notes.txt").write_text(NOTES_BASE, encoding="utf-8")
    await coordinator.commit_pending_state(
        WorkspaceTarget(path=root, separated_env=True),
        exclude_pathspecs=WorkspaceManifest().git_line_commit_exclude_specs(),
        message="root notes baseline",
    )
    base = await repository.resolve_ref(ROOT_INTEGRATION_REF)
    upper = await repository.create_scope_workspace("node-upper", base_commit=base)
    lower = await repository.create_scope_workspace("node-lower", base_commit=base)

    (upper.path / "design" / "notes.txt").write_text(
        "w1-UP\nw2-UP\nmid1\nmid2\np1\np2\n", encoding="utf-8"
    )
    (lower.path / "design" / "notes.txt").write_text(
        "w1\nw2\nmid1\nmid2\np1-LOW\np2-LOW\n", encoding="utf-8"
    )
    head_upper = await _commit_writer(coordinator, repository, upper)
    head_lower = await _commit_writer(coordinator, repository, lower)

    first = await _integrate(
        coordinator, repository, root, scope_key="node-upper", base=base, writer_head=head_upper
    )
    second = await _integrate(
        coordinator, repository, root, scope_key="node-lower", base=base, writer_head=head_lower
    )

    assert isinstance(first, IntegrationSuccess)
    assert isinstance(second, IntegrationSuccess)
    assert (root / "design" / "notes.txt").read_text(encoding="utf-8") == (
        "w1-UP\nw2-UP\nmid1\nmid2\np1-LOW\np2-LOW\n"
    )


async def test_git_merge_policy_rejects_real_conflict_with_payload(tmp_path: Path) -> None:
    """可合并档：同片段不同改动的真实冲突被拒，载荷齐全。"""
    root, repository, coordinator, _refs = await _setup(tmp_path, policy=ConflictPolicy.GIT_MERGE)
    (root / "design" / "notes.txt").write_text(NOTES_BASE, encoding="utf-8")
    await coordinator.commit_pending_state(
        WorkspaceTarget(path=root, separated_env=True),
        exclude_pathspecs=WorkspaceManifest().git_line_commit_exclude_specs(),
        message="root notes baseline",
    )
    base = await repository.resolve_ref(ROOT_INTEGRATION_REF)
    mine = await repository.create_scope_workspace("node-mine", base_commit=base)
    theirs = await repository.create_scope_workspace("node-theirs", base_commit=base)

    (mine.path / "design" / "notes.txt").write_text(
        "MINE\nw2\nmid1\nmid2\np1\np2\n", encoding="utf-8"
    )
    (theirs.path / "design" / "notes.txt").write_text(
        "THEIRS\nw2\nmid1\nmid2\np1\np2\n", encoding="utf-8"
    )
    head_mine = await _commit_writer(coordinator, repository, mine)
    head_theirs = await _commit_writer(coordinator, repository, theirs)

    first = await _integrate(
        coordinator, repository, root, scope_key="node-mine", base=base, writer_head=head_mine
    )
    second = await _integrate(
        coordinator, repository, root, scope_key="node-theirs", base=base, writer_head=head_theirs
    )

    assert isinstance(first, IntegrationSuccess)
    assert isinstance(second, IntegrationConflict)
    assert second.conflict_paths == (NOTES,)
    assert second.writer_scope_id == "node-theirs"
    assert second.parent_scope_id == "root"
    assert second.base_commit == base
    assert second.writer_head == head_theirs
    assert second.parent_head == first.new_head
    assert (root / "design" / "notes.txt").read_text(encoding="utf-8").startswith("MINE\n")


async def test_strict_policy_rejects_any_path_overlap(tmp_path: Path) -> None:
    """严格档：路径集重叠即拒，即使片段不相交可自动合并。"""
    root, repository, coordinator, _refs = await _setup(tmp_path, policy=ConflictPolicy.STRICT)
    (root / "design" / "notes.txt").write_text(NOTES_BASE, encoding="utf-8")
    await coordinator.commit_pending_state(
        WorkspaceTarget(path=root, separated_env=True),
        exclude_pathspecs=WorkspaceManifest().git_line_commit_exclude_specs(),
        message="root notes baseline",
    )
    base = await repository.resolve_ref(ROOT_INTEGRATION_REF)
    upper = await repository.create_scope_workspace("node-upper", base_commit=base)
    lower = await repository.create_scope_workspace("node-lower", base_commit=base)

    (upper.path / "design" / "notes.txt").write_text(
        "w1-UP\nw2-UP\nmid1\nmid2\np1\np2\n", encoding="utf-8"
    )
    (lower.path / "design" / "notes.txt").write_text(
        "w1\nw2\nmid1\nmid2\np1-LOW\np2-LOW\n", encoding="utf-8"
    )
    head_upper = await _commit_writer(coordinator, repository, upper)
    head_lower = await _commit_writer(coordinator, repository, lower)

    first = await _integrate(
        coordinator, repository, root, scope_key="node-upper", base=base, writer_head=head_upper
    )
    second = await _integrate(
        coordinator, repository, root, scope_key="node-lower", base=base, writer_head=head_lower
    )

    assert isinstance(first, IntegrationSuccess)
    assert isinstance(second, IntegrationConflict)
    assert second.conflict_paths == (NOTES,)


async def test_writer_local_only_change_is_noop_integration(tmp_path: Path) -> None:
    """写者只改自建路径且父线未动：空集成不误报、不产生提交。"""
    root, repository, coordinator, refs = await _setup(tmp_path, policy=ConflictPolicy.GIT_MERGE)
    writer = await repository.create_scope_workspace("node-a", base_commit=refs.integration_head)

    (writer.path / "run.log").write_text("writer-log\n", encoding="utf-8")
    (writer.path / "op" / "build").mkdir(parents=True)
    (writer.path / "op" / "build" / "new.out").write_bytes(b"\x07new")
    head = await _commit_writer(coordinator, repository, writer)

    outcome = await _integrate(
        coordinator,
        repository,
        root,
        scope_key="node-a",
        base=refs.integration_head,
        writer_head=head,
    )

    assert outcome == IntegrationSuccess(new_head=refs.integration_head, integrated_paths=())
    assert await repository.resolve_ref(ROOT_INTEGRATION_REF) == refs.integration_head
    assert (root / "run.log").read_text(encoding="utf-8") == "root-log\n"
    assert not (root / "op" / "build" / "new.out").exists()


# ── 路径事件对全覆盖（§8.1 两档判定的验收矩阵）──────────────────────


@dataclass(frozen=True)
class _EventPair:
    """一个路径事件对场景：基线种子、双方变更与两档期望。"""

    name: str
    seed: Callable[[Path], None]
    first: Callable[[Path], None]
    second: Callable[[Path], None]
    # 路径出现于双侧变更集即命中（strict 档拒绝）
    overlap_path: str
    # 可合并档下该事件对是否干净（真 git 三方合并判定）
    merge_clean: bool
    # 可合并档冲突时的主冲突路径（默认取 overlap_path）
    merge_conflict_path: str = ""


def _write(path: Path, text: str) -> None:
    """写文件（含父目录）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _replace_with_symlink(path: Path, target: str) -> None:
    """普通文件替换为符号链接。"""
    path.unlink()
    path.symlink_to(target)


_EVENT_PAIRS: list[_EventPair] = [
    _EventPair(
        name="modify_modify_disjoint_hunks",
        seed=lambda root: _write(root / "design" / "notes.txt", NOTES_BASE),
        first=lambda ws: _write(ws / "design" / "notes.txt", "w1-UP\nw2-UP\nmid1\nmid2\np1\np2\n"),
        second=lambda ws: _write(
            ws / "design" / "notes.txt", "w1\nw2\nmid1\nmid2\np1-LOW\np2-LOW\n"
        ),
        overlap_path="design/notes.txt",
        merge_clean=True,
    ),
    _EventPair(
        name="add_add_same_content",
        seed=lambda root: None,
        first=lambda ws: _write(ws / "design" / "new.txt", "same\n"),
        second=lambda ws: _write(ws / "design" / "new.txt", "same\n"),
        overlap_path="design/new.txt",
        merge_clean=True,
    ),
    _EventPair(
        name="add_add_different_content",
        seed=lambda root: None,
        first=lambda ws: _write(ws / "design" / "new.txt", "from-first\n"),
        second=lambda ws: _write(ws / "design" / "new.txt", "from-second\n"),
        overlap_path="design/new.txt",
        merge_clean=False,
    ),
    _EventPair(
        name="delete_modify",
        seed=lambda root: _write(root / "design" / "target.txt", "content\n"),
        first=lambda ws: (ws / "design" / "target.txt").unlink(),
        second=lambda ws: _write(ws / "design" / "target.txt", "modified\n"),
        overlap_path="design/target.txt",
        merge_clean=False,
    ),
    _EventPair(
        name="delete_delete_rebuild",
        seed=lambda root: _write(root / "design" / "target.txt", "content\n"),
        first=lambda ws: _write(ws / "design" / "target.txt", "re-first\n"),
        second=lambda ws: _write(ws / "design" / "target.txt", "re-second\n"),
        overlap_path="design/target.txt",
        merge_clean=False,
    ),
    _EventPair(
        name="rename_crossing",
        seed=lambda root: _write(root / "design" / "mv.txt", "payload\n"),
        first=lambda ws: (ws / "design" / "mv.txt").rename(ws / "design" / "moved-a.txt"),
        second=lambda ws: (ws / "design" / "mv.txt").rename(ws / "design" / "moved-b.txt"),
        overlap_path="design/mv.txt",
        merge_clean=False,
    ),
    _EventPair(
        name="dir_file_swap",
        seed=lambda root: (
            _write(root / "design" / "pkg" / "x.txt", "x\n"),
            _write(root / "design" / "pkg" / "y.txt", "y\n"),
        ),
        first=lambda ws: (
            shutil.rmtree(ws / "design" / "pkg"),
            _write(ws / "design" / "pkg", "file-now\n"),
        ),
        second=lambda ws: _write(ws / "design" / "pkg" / "x.txt", "x-mod\n"),
        overlap_path="design/pkg/x.txt",
        merge_clean=False,
    ),
    _EventPair(
        name="symlink_file_swap",
        seed=lambda root: (
            _write(root / "design" / "anchor.txt", "anchor\n"),
            _write(root / "design" / "link.txt", "plain\n"),
        ),
        first=lambda ws: _replace_with_symlink(ws / "design" / "link.txt", "anchor.txt"),
        second=lambda ws: _write(ws / "design" / "link.txt", "plain-mod\n"),
        overlap_path="design/link.txt",
        merge_clean=False,
    ),
    _EventPair(
        name="type_mode_change",
        seed=lambda root: (
            _write(root / "design" / "anchor.txt", "anchor\n"),
            _write(root / "design" / "bin.sh", "script\n"),
        ),
        first=lambda ws: _replace_with_symlink(ws / "design" / "bin.sh", "anchor.txt"),
        second=lambda ws: os.chmod(ws / "design" / "bin.sh", 0o755),
        overlap_path="design/bin.sh",
        merge_clean=False,
    ),
]


async def _run_event_pair(
    tmp_path: Path, policy: ConflictPolicy, pair: _EventPair
) -> IntegrationSuccess | IntegrationConflict:
    """构造真实 git 事件对：先到者集成成功后，后到者按档位判定。"""
    root, repository, coordinator, _refs = await _setup(tmp_path, policy=policy)
    pair.seed(root)
    await coordinator.commit_pending_state(
        WorkspaceTarget(path=root, separated_env=True),
        exclude_pathspecs=WorkspaceManifest().git_line_commit_exclude_specs(),
        message="event pair seed",
    )
    base = await repository.resolve_ref(ROOT_INTEGRATION_REF)
    first_ws = await repository.create_scope_workspace("node-first", base_commit=base)
    second_ws = await repository.create_scope_workspace("node-second", base_commit=base)
    pair.first(first_ws.path)
    pair.second(second_ws.path)
    head_first = await _commit_writer(coordinator, repository, first_ws)
    head_second = await _commit_writer(coordinator, repository, second_ws)

    first = await _integrate(
        coordinator,
        repository,
        root,
        scope_key="node-first",
        base=base,
        writer_head=head_first,
    )
    assert isinstance(first, IntegrationSuccess), f"先到者集成应成功：{first!r}"
    return await _integrate(
        coordinator,
        repository,
        root,
        scope_key="node-second",
        base=base,
        writer_head=head_second,
    )


@pytest.mark.parametrize("pair", _EVENT_PAIRS, ids=lambda pair: pair.name)
async def test_strict_policy_rejects_every_event_pair(tmp_path: Path, pair: _EventPair) -> None:
    """严格档：全部路径事件对（含 Git 可自动合并者）一律拒绝。"""
    outcome = await _run_event_pair(tmp_path, ConflictPolicy.STRICT, pair)

    assert isinstance(outcome, IntegrationConflict)
    assert pair.overlap_path in outcome.conflict_paths
    assert outcome.writer_scope_id == "node-second"
    assert outcome.parent_scope_id == "root"


@pytest.mark.parametrize("pair", _EVENT_PAIRS, ids=lambda pair: pair.name)
async def test_git_merge_policy_judges_by_mergeability(tmp_path: Path, pair: _EventPair) -> None:
    """可合并档：同一事件集按三方合并结果判定——干净放行、真冲突拒绝。"""
    outcome = await _run_event_pair(tmp_path, ConflictPolicy.GIT_MERGE, pair)

    if pair.merge_clean:
        assert isinstance(outcome, IntegrationSuccess), f"可合并事件对被误拒：{outcome!r}"
        assert pair.overlap_path in outcome.integrated_paths
    else:
        assert isinstance(outcome, IntegrationConflict), f"真冲突事件对被误放行：{outcome!r}"
        primary = pair.merge_conflict_path or pair.overlap_path
        assert primary in outcome.conflict_paths
