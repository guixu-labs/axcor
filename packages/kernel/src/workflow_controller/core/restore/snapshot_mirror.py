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

"""快照仓镜像原语：目录镜像、内部 git 捕获与结构化提交信息（自插件迁移）。

rsync 优先、不可用降级纯 Python（size+mtime_ns 判定）；git 调用经私有仓
模块的同步出口。导入重建轨与既有快照仓布局共用同一套约定。
"""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
import subprocess
from fnmatch import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING

from workflow_controller.core.workspace.repository import (
    INNER_GIT_RESERVED_PATH,
    remove_all_remotes,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = [
    "DEFAULT_IGNORE_PATTERNS",
    "build_snapshot_commit_message",
    "mirror_inner_git",
    "python_sync_mirror",
    "rsync_mirror",
]

logger = logging.getLogger(__name__)

# 镜像排除默认清单（用户可在导入选项覆盖）
DEFAULT_IGNORE_PATTERNS: list[str] = [
    ".workflow-controller",
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    "dist",
    "build",
    "*.pyc",
]


def _match_double_star(parts: list[str], clean: str) -> bool:
    """``**`` 模式匹配：把 ``**`` 当作「任意层」，逐段比对非 ``**`` 部分。"""
    segs = [s for s in clean.split("/") if s and s != "**"]
    if not segs:
        return False
    pi = 0
    for seg in parts:
        if pi < len(segs) and fnmatch(seg, segs[pi]):
            pi += 1
        if pi == len(segs):
            return True
    return False


def _match_ignore(rel_path: str, patterns: Sequence[str]) -> bool:
    """纯 Python glob 判断 rel_path 是否应被忽略（目录段与整路径两级匹配）。"""
    parts = rel_path.replace("\\", "/").split("/")
    for pattern in patterns:
        p = pattern.strip()
        if not p:
            continue
        clean = p.rstrip("/")
        if fnmatch(rel_path, clean):
            return True
        for seg in parts:
            if fnmatch(seg, clean):
                return True
        if "**" in clean and _match_double_star(parts, clean):
            return True
    return False


def _collect_src_files(src: Path, ignore_patterns: Sequence[str]) -> dict[str, Path]:
    """收集源文件（相对路径 → 绝对路径），忽略目录剪枝。"""
    src_files: dict[str, Path] = {}
    for root, dirs, files in os.walk(src):
        rel_root = os.path.relpath(root, src).replace("\\", "/")
        kept_dirs = []
        for d in dirs:
            if not _match_ignore(f"{rel_root}/{d}".lstrip("./"), ignore_patterns):
                kept_dirs.append(d)
        dirs[:] = kept_dirs
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), src).replace("\\", "/")
            if _match_ignore(rel, ignore_patterns):
                continue
            src_files[rel] = Path(root) / f
    return src_files


def _unlink_stale_dest_file(
    root: str,
    fname: str,
    rel: str,
    src_files: dict[str, Path],
    ignore_patterns: Sequence[str],
) -> None:
    """删除目标里源已不存在的单个文件；排除项不受删除影响（对齐 rsync）。"""
    if rel in src_files or _match_ignore(rel, ignore_patterns):
        return
    with contextlib.suppress(OSError):
        (Path(root) / fname).unlink()


def _delete_stale_dest_files(
    dest: Path, src_files: dict[str, Path], ignore_patterns: Sequence[str]
) -> None:
    """删除 dest 中不在 src 的文件（排除项跳过，对齐 rsync --delete --exclude）。"""
    for root, _dirs, files in os.walk(dest):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), dest).replace("\\", "/")
            _unlink_stale_dest_file(root, f, rel, src_files, ignore_patterns)


def _mirror_copy_needed(src_path: Path, dest_path: Path) -> bool:
    """目标缺失或 size/mtime_ns 不一致时需复制（同秒同长不同内容不漏）。"""
    if not dest_path.exists():
        return True
    try:
        ss = src_path.stat()
        ds = dest_path.stat()
    except OSError:
        return True
    return not (ss.st_size == ds.st_size and ss.st_mtime_ns == ds.st_mtime_ns)


def _copy_changed_files(src_files: dict[str, Path], dest: Path) -> tuple[int, int]:
    """复制新/改文件，返回 (file_count, byte_size)。"""
    file_count = 0
    byte_size = 0
    for rel, src_path in src_files.items():
        dest_path = dest / rel
        try:
            if not dest_path.parent.exists():
                dest_path.parent.mkdir(parents=True, exist_ok=True)
            if _mirror_copy_needed(src_path, dest_path):
                shutil.copy2(src_path, dest_path)
            st = dest_path.stat()
            file_count += 1
            byte_size += st.st_size
        except OSError:
            logger.debug("快照镜像跳过 %s", rel, exc_info=True)
    return file_count, byte_size


def _prune_empty_dirs(dest: Path) -> None:
    """清理空目录（best-effort）。"""
    for root, dirs, _files in list(os.walk(dest, topdown=False)):
        for d in dirs:
            with contextlib.suppress(OSError):
                (Path(root) / d).rmdir()


def python_sync_mirror(
    src: Path,
    dest: Path,
    ignore_patterns: Sequence[str],
) -> tuple[int, int]:
    """纯 Python 增量镜像（rsync 降级路径），返回 (file_count, byte_size)。"""
    src = src.resolve()
    dest = dest.resolve()
    src_files = _collect_src_files(src, ignore_patterns)
    _delete_stale_dest_files(dest, src_files, ignore_patterns)
    file_count, byte_size = _copy_changed_files(src_files, dest)
    _prune_empty_dirs(dest)
    return file_count, byte_size


def rsync_mirror(
    src: Path,
    dest: Path,
    ignore_patterns: Sequence[str],
) -> tuple[int, int] | None:
    """用 rsync 镜像（``--checksum`` 防同秒同长漏传）；不可用返回 None。"""
    rsync = shutil.which("rsync")
    if rsync is None:
        return None
    excludes: list[str] = []
    for pattern in ignore_patterns:
        p = pattern.strip().rstrip("/")
        if p:
            excludes.extend(["--exclude", p])
    src_arg = f"{src}/"
    cmd = [rsync, "-a", "--delete", "--checksum", *excludes, src_arg, str(dest)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    if result.returncode != 0:
        return None
    file_count = 0
    byte_size = 0
    for root, _dirs, files in os.walk(dest):
        for f in files:
            fp = Path(root) / f
            try:
                st = fp.stat()
                file_count += 1
                byte_size += st.st_size
            except OSError:
                continue
    return file_count, byte_size


def _dir_size_bytes(path: Path, limit: int = 0) -> int:
    """目录体积（字节）；limit>0 时超限提前返回（只判超限的大仓免全量扫）。"""
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                continue
        if limit > 0 and total > limit:
            break
    return total


def _rsync_inner_git(src: Path, dest: Path, rsync: str) -> bool:
    """两趟 rsync 镜像内部 .git；返回 quick-check 趟是否成功。"""
    try:
        result = subprocess.run(
            [
                rsync,
                "-a",
                "--checksum",
                "--delete",
                "--exclude",
                "objects",
                f"{src}/",
                str(dest),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            logger.debug("内部 git 镜像 checksum 趟失败 rc=%d", result.returncode)
        result = subprocess.run(
            [rsync, "-a", "--delete", f"{src}/", str(dest)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def mirror_inner_git(project_dir: Path, working_dir: Path, max_mb: int = 200) -> str:
    """镜像 ``project_dir/.git`` → 保留路径，返回 captured/absent/skipped_size。

    两趟 rsync（checksum 抓固定名小文件的同秒重写、quick-check 收 objects
    增量）；不可用降级纯 Python。镜像残缺（缺 HEAD）视为 absent；超限或
    absent 时删除保留目录；同步后剥除 remote（URL 可能内嵌 token）。
    """
    src = Path(project_dir) / ".git"
    dest = Path(working_dir) / INNER_GIT_RESERVED_PATH
    if not src.is_dir():
        # .git 为文件（linked worktree/submodule 的 gitdir 指针）→ 恢复后必坏
        if dest.exists():
            shutil.rmtree(dest, ignore_errors=True)
        return "absent"
    size_limit = max_mb * 1024 * 1024
    if max_mb > 0 and _dir_size_bytes(src, limit=size_limit) > size_limit:
        if dest.exists():
            shutil.rmtree(dest, ignore_errors=True)
        return "skipped_size"
    captured = False
    rsync = shutil.which("rsync")
    if rsync is not None:
        captured = _rsync_inner_git(src, dest, rsync)
    if not captured:
        python_sync_mirror(src, dest, [])
        if not (dest / "HEAD").is_file():
            shutil.rmtree(dest, ignore_errors=True)
            return "absent"
    remove_all_remotes(dest)
    return "captured"


def build_snapshot_commit_message(
    session_id: str,
    checkpoint_id: str,
    agent_id: str,
    agent_type: str,
    iteration: int,
    trigger: str,
    event_rowid: int | None,
    parent_checkpoint_id: str | None,
    main_part_id: str | None,
) -> str:
    """快照 commit 的结构化 trailer，可 ``git log --grep`` 反查。"""
    return (
        "workflow-controller snapshot\n"
        "\n"
        f"session: {session_id}\n"
        f"checkpoint: {checkpoint_id}\n"
        f"iteration: {iteration}\n"
        f"agent_id: {agent_id or 'main'}\n"
        f"agent_type: {agent_type or ''}\n"
        f"trigger: {trigger}\n"
        f"parent_checkpoint: {parent_checkpoint_id or ''}\n"
        f"event_rowid: {event_rowid if event_rowid is not None else ''}\n"
        f"main_part_id: {main_part_id or ''}\n"
    )
