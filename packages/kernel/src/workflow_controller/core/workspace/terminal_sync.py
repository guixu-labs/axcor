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

"""终态三方同步：会话完成后把托管终态按基线参照写回原目录。

基线参照是启动时导入的 seed 快照（全量不可变）：原目录与基线一致时
同步完整终态——seed 内文件被托管终态删除则原目录同删，基线之外的用户
新增文件不触碰；原目录基线内文件被外部改过即停止覆盖、按路径报告冲
突并保留托管终态（CAS 语义）。同步只应用 seed→终态的差异（merge 路
径加随检查点的自建路径），不整目录拷贝；原目录 ``.git``、保留路径与
托管目录自身不参与。
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from workflow_controller.core.workspace.manifest import RollbackPolicy
from workflow_controller.core.workspace.repository import (
    INNER_GIT_RESERVED_PATH,
    ROOT_SEED_REF,
)

if TYPE_CHECKING:
    from workflow_controller.core.workspace.manifest import WorkspaceManifest
    from workflow_controller.core.workspace.repository import WorkspaceRepository

__all__ = [
    "TerminalSyncReport",
    "sync_terminal_to_original",
]

# 原目录侧不参与同步的顶层名字：用户仓、内部 git 保留路径、托管目录
_ORIGINAL_EXCLUDED_TOP_LEVEL = frozenset({".git", ".workflow-controller", INNER_GIT_RESERVED_PATH})

# 托管工作区内不属于文件态的顶层名字（worktree 的 .git 指针文件）
_WORKSPACE_EXCLUDED_TOP_LEVEL = frozenset({".git"})


@dataclass(frozen=True)
class TerminalSyncReport:
    """一次终态同步的结论：写回/删除清单，冲突时全量拒改。"""

    applied: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    conflict_paths: tuple[str, ...] = ()

    @property
    def refused(self) -> bool:
        """是否因外部变化拒绝覆盖（托管终态保留）。"""
        return bool(self.conflict_paths)


def _blob_sha(data: bytes) -> str:
    """按 git blob 对象口径计算内容哈希（与 seed 树条目可直接比对）。"""
    hasher = hashlib.sha1()
    hasher.update(f"blob {len(data)}\0".encode("ascii"))
    hasher.update(data)
    return hasher.hexdigest()


def _entry_sha(path: Path) -> str | None:
    """盘上一条文件态的内容哈希；非文件/符号链接或读失败返回 None。"""
    try:
        if path.is_symlink():
            return _blob_sha(os.readlink(path).encode("utf-8", "surrogateescape"))
        if path.is_file():
            size = path.stat().st_size
            hasher = hashlib.sha1()
            hasher.update(f"blob {size}\0".encode("ascii"))
            with path.open("rb") as handle:
                while chunk := handle.read(1 << 20):
                    hasher.update(chunk)
            return hasher.hexdigest()
    except OSError:
        return None
    return None


def _walk_state(root: Path, excluded_top: frozenset[str]) -> dict[str, str]:
    """遍历目录收集 文件/符号链接 → 内容哈希（目录不入账）。"""
    entries: dict[str, str] = {}
    if not root.is_dir():
        return entries
    stack: list[Path] = [root]
    while stack:
        current = stack.pop()
        try:
            children = sorted(os.scandir(current), key=lambda item: item.name)
        except OSError:
            continue
        for child in children:
            child_path = Path(child.path)
            rel = child_path.relative_to(root).as_posix()
            if current == root and child.name in excluded_top:
                continue
            if child.is_symlink():
                entries[rel] = _blob_sha(os.readlink(child_path).encode("utf-8", "surrogateescape"))
            elif child.is_file(follow_symlinks=False):
                entries[rel] = _entry_sha(child_path) or ""
            elif child.is_dir(follow_symlinks=False):
                stack.append(child_path)
    return entries


def _sync_scoped(path: str, manifest: WorkspaceManifest) -> bool:
    """路径是否属同步面：非保留名字且路径策略为随检查点（可精确恢复）。"""
    if path.split("/", 1)[0] in _ORIGINAL_EXCLUDED_TOP_LEVEL:
        return False
    return manifest.classify_path(path).rollback is RollbackPolicy.CHECKPOINTED


def _present(target: Path) -> bool:
    """路径是否有任何盘上事实（含悬空符号链接）。"""
    return target.exists() or target.is_symlink()


def _remove_path(target: Path) -> None:
    """删除原目录侧一条路径（文件/符号链接/目录均可）。"""
    if target.is_symlink() or (target.exists() and not target.is_dir()):
        target.unlink(missing_ok=True)
    elif target.is_dir():
        shutil.rmtree(target, ignore_errors=True)


def _copy_entry(source: Path, target: Path) -> None:
    """把托管终态的一条文件态写入原目录（copy2 保留权限位，链接重建）。"""
    if source.is_symlink():
        if _present(target):
            _remove_path(target)
        os.symlink(os.readlink(source), target)
        return
    if target.is_dir():
        shutil.rmtree(target)
    elif _present(target):
        target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def _collect_conflicts(
    seed: dict[str, str], original_dir: Path, manifest: WorkspaceManifest
) -> list[str]:
    """基线内同步面路径的外部变化核对：改过/删除/类型漂移都算冲突。"""
    conflicts: list[str] = []
    for path, seed_sha in seed.items():
        if not _sync_scoped(path, manifest):
            continue
        target = original_dir / path
        if not _present(target) or target.is_dir() or (_entry_sha(target) or "") != seed_sha:
            conflicts.append(path)
    return sorted(set(conflicts))


def _plan_and_apply(
    *,
    seed: dict[str, str],
    final: dict[str, str],
    original_dir: Path,
    workspace_dir: Path,
    manifest: WorkspaceManifest,
) -> TerminalSyncReport:
    """先全量核对基线（CAS），干净再应用差异；冲突路径即报告。"""
    conflicts = _collect_conflicts(seed, original_dir, manifest)
    if conflicts:
        return TerminalSyncReport(conflict_paths=tuple(conflicts))
    applied: list[str] = []
    for path, final_sha in sorted(final.items()):
        if not _sync_scoped(path, manifest):
            continue
        if seed.get(path) == final_sha:
            continue  # 与基线一致：无需写回
        _copy_entry(workspace_dir / path, original_dir / path)
        applied.append(path)
    removed: list[str] = []
    for path in sorted(seed):
        if not _sync_scoped(path, manifest) or path in final:
            continue
        target = original_dir / path
        if _present(target):
            _remove_path(target)
            removed.append(path)
    return TerminalSyncReport(applied=tuple(applied), removed=tuple(removed))


async def sync_terminal_to_original(
    *,
    repository: WorkspaceRepository,
    workspace_dir: Path,
    original_dir: Path,
    manifest: WorkspaceManifest,
) -> TerminalSyncReport:
    """执行终态三方同步：seed 为基线，托管工作区为终态，原目录为落点。"""
    seed_ref = await repository.resolve_ref(ROOT_SEED_REF)
    if seed_ref is None:
        raise RuntimeError(f"私有仓缺少 seed 引用，无法做终态同步：{repository.repo_dir}")
    seed = await repository.tree_blob_entries(seed_ref)
    final = await asyncio.to_thread(_walk_state, workspace_dir, _WORKSPACE_EXCLUDED_TOP_LEVEL)
    return await asyncio.to_thread(
        _plan_and_apply,
        seed=seed,
        final=final,
        original_dir=original_dir,
        workspace_dir=workspace_dir,
        manifest=manifest,
    )
