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

"""私有仓基线导入与首层建区测试。"""

from __future__ import annotations

import io
import os
import subprocess
import tarfile
from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.workspace import (
    INNER_GIT_RESERVED_PATH,
    ROOT_SEED_REF,
    RootRefs,
    WorkspaceRepository,
    WorkspaceRepositoryError,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

WRITER_LOCAL_PATHSPECS = ("op/build", "*.log")


def _run_git(args: Sequence[str], cwd: Path) -> str:
    """测试侧 git 调用：失败即断言。"""
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def _run_git_bytes(args: Sequence[str], cwd: Path, *, input: bytes | None = None) -> bytes:
    """测试侧 git 调用（字节输出，路径不转码）：失败即断言。"""
    proc = subprocess.run(["git", *args], cwd=cwd, capture_output=True, check=False, input=input)
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def _make_user_workspace(root: Path) -> None:
    """用户现场：已提交文件 + 脏文件 + 未跟踪文件 + 构建产物。"""
    _run_git(["init", "--quiet"], root)
    _run_git(["config", "user.email", "user@example.com"], root)
    _run_git(["config", "user.name", "user"], root)
    (root / "committed.txt").write_text("committed\n", encoding="utf-8")
    _run_git(["add", "."], root)
    _run_git(["commit", "--quiet", "--message", "init"], root)
    with (root / "committed.txt").open("a", encoding="utf-8") as committed:
        committed.write("dirty\n")
    (root / "untracked.txt").write_text("untracked\n", encoding="utf-8")
    (root / "op" / "build").mkdir(parents=True)
    (root / "op" / "build" / "artifact.bin").write_bytes(b"\x00\x01artifact")
    (root / "run.log").write_text("log\n", encoding="utf-8")


def _collect_files(root: Path, skip_top: set[str] = frozenset()) -> dict[str, bytes]:
    """收集相对路径 → 文件内容（按顶层名跳过）。"""
    files: dict[str, bytes] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if path.is_dir() or relative.parts[0] in skip_top:
            continue
        files[relative.as_posix()] = path.read_bytes()
    return files


def _extract_commit_tree(repo_dir: Path, commit: str, dest: Path) -> None:
    """把私有仓某提交的树解包到 dest。"""
    archive = subprocess.run(
        ["git", "--git-dir", str(repo_dir), "archive", commit],
        capture_output=True,
        check=True,
    ).stdout
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r|") as tar:
        tar.extractall(dest, filter="data")


async def _imported_repository(root: Path) -> tuple[WorkspaceRepository, RootRefs]:
    """建仓并导入基线，返回仓库实例与根双引用。"""
    repository = WorkspaceRepository(root, "run-1")
    refs = await repository.import_baseline(
        writer_local_pathspecs=WRITER_LOCAL_PATHSPECS,
    )
    return repository, refs


async def test_import_baseline_captures_full_root_state(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    _make_user_workspace(root)
    original = _collect_files(root, skip_top={".git", ".workflow-controller"})

    repository, refs = await _imported_repository(root)

    extracted = tmp_path / "seed-extract"
    extracted.mkdir()
    _extract_commit_tree(repository.repo_dir, refs.seed_checkpoint, extracted)
    extracted_files = _collect_files(extracted)
    mirror_prefix = f"{INNER_GIT_RESERVED_PATH}/"
    mirror_files = {
        path[len(mirror_prefix) :]: content
        for path, content in extracted_files.items()
        if path.startswith(mirror_prefix)
    }
    assert mirror_files == _collect_files(root / ".git")
    assert {
        path: content
        for path, content in extracted_files.items()
        if not path.startswith(mirror_prefix)
    } == original


async def test_import_baseline_keeps_user_git_and_root_clean(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    _make_user_workspace(root)
    user_git_before = _collect_files(root / ".git")
    names_before = sorted(entry.name for entry in root.iterdir())

    repository, _ = await _imported_repository(root)

    assert _collect_files(root / ".git") == user_git_before
    assert sorted(entry.name for entry in root.iterdir()) == sorted(
        [*names_before, ".workflow-controller"]
    )
    assert not (repository.tree_dir / "baseline-stage").exists()


async def test_root_refs_split_seed_and_integration_line(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    _make_user_workspace(root)

    repository, refs = await _imported_repository(root)

    seed_paths = _run_git(
        ["ls-tree", "-r", "--name-only", refs.seed_checkpoint], repository.repo_dir
    ).splitlines()
    line_paths = _run_git(
        ["ls-tree", "-r", "--name-only", refs.integration_head], repository.repo_dir
    ).splitlines()
    assert "op/build/artifact.bin" in seed_paths
    assert f"{INNER_GIT_RESERVED_PATH}/HEAD" in seed_paths
    assert "committed.txt" in line_paths
    assert "untracked.txt" in line_paths
    assert not any(path.startswith("op/build/") for path in line_paths)
    assert not any(path.startswith(INNER_GIT_RESERVED_PATH) for path in line_paths)
    _run_git(
        ["merge-base", "--is-ancestor", refs.seed_checkpoint, refs.integration_head],
        repository.repo_dir,
    )
    assert await repository.resolve_ref(ROOT_SEED_REF) == refs.seed_checkpoint


async def test_first_layer_scope_materializes_writer_local_from_seed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    _make_user_workspace(root)
    original = _collect_files(root, skip_top={".git", ".workflow-controller"})

    repository, refs = await _imported_repository(root)
    workspace = await repository.create_scope_workspace(
        "node-a",
        base_commit=refs.integration_head,
        materialize_from=refs.seed_checkpoint,
        materialize_pathspecs=WRITER_LOCAL_PATHSPECS,
    )

    assert workspace.path == repository.workspaces_dir / "node-a"
    assert _collect_files(workspace.path, skip_top={".git"}) == original
    assert not (workspace.path / INNER_GIT_RESERVED_PATH).exists()
    git_pointer = (workspace.path / ".git").read_text(encoding="utf-8")
    assert git_pointer.startswith("gitdir:")
    branch_ref = f"refs/heads/{workspace.branch}"
    assert await repository.resolve_ref(branch_ref) == workspace.base_commit
    parent = _run_git(["rev-parse", "HEAD^"], workspace.path).strip()
    assert parent == refs.integration_head


async def test_import_baseline_without_strippable_paths_keeps_line_at_seed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "plain.txt").write_text("无用户仓、无 writer_local 路径\n", encoding="utf-8")

    repository = WorkspaceRepository(root, "run-1")
    refs = await repository.import_baseline(writer_local_pathspecs=[])

    assert refs.integration_head == refs.seed_checkpoint


async def test_import_baseline_twice_fails_fast(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    _make_user_workspace(root)
    repository, _ = await _imported_repository(root)

    with pytest.raises(WorkspaceRepositoryError):
        await repository.import_baseline(writer_local_pathspecs=[])


async def test_resolve_ref_missing_returns_none_but_corruption_raises(
    tmp_path: Path,
) -> None:
    """引用缺失返回 None；仓库损坏（非引用缺席退出码）仍显式抛错。"""
    root = tmp_path / "root"
    root.mkdir()
    _make_user_workspace(root)
    repository, _ = await _imported_repository(root)

    assert await repository.resolve_ref("refs/heads/nonexistent") is None

    (repository.repo_dir / "HEAD").write_text("garbage\n", encoding="utf-8")
    with pytest.raises(WorkspaceRepositoryError):
        await repository.resolve_ref(ROOT_SEED_REF)


async def test_git_output_round_trips_non_utf8_paths(tmp_path: Path) -> None:
    """ls-files 输出的非 UTF-8 路径回灌 update-index（stdin）与路径匹配（argv）不损坏字节。"""
    root = tmp_path / "root"
    root.mkdir()
    (root / "plain.txt").write_text("x\n", encoding="utf-8")
    repository = WorkspaceRepository(root, "run-1")
    await repository.import_baseline(writer_local_pathspecs=())

    # macOS 文件系统拒绝非法 UTF-8 文件名，经 cacheinfo 以字节 argv 注入索引
    raw_path = b"caf\xe9.bin"
    blob_sha = _run_git_bytes(
        ["hash-object", "-w", "--stdin"], repository.repo_dir, input=b"payload"
    ).strip()
    subprocess.run(
        [
            b"git",
            b"update-index",
            b"--add",
            b"--cacheinfo",
            b"100644," + blob_sha + b"," + raw_path,
        ],
        cwd=root,
        env={**os.environ, "GIT_DIR": str(repository.repo_dir), "GIT_WORK_TREE": str(root)},
        check=True,
    )

    paths = await repository._list_index_paths(("*",))
    assert b"caf\xe9.bin" in {os.fsencode(path) for path in paths}
    decoded_path = next(path for path in paths if os.fsencode(path) == raw_path)

    # argv 回灌（checkout 物化同源）：按解码后的路径匹配须命中
    listed = await repository._run_repo_git(
        ["ls-files", "-z", "--", decoded_path], work_tree=root, cwd=root
    )
    assert listed.strip("\0") == decoded_path

    # stdin 回灌（剥除路径同源）：force-remove 须按原始字节命中
    await repository._run_repo_git(
        ["update-index", "-z", "--force-remove", "--stdin"],
        work_tree=root,
        cwd=root,
        input="\0".join(paths) + "\0",
    )
    assert b"caf\xe9.bin" not in _run_git_bytes(["ls-files", "-z"], repository.repo_dir)
