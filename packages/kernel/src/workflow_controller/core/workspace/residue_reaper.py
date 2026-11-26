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

"""工作区残留 TTL 清理器：单会话崩溃残留树、过期检查点行与 fork 孤儿目录。

正常路径的树回收是确定性的（终态同步干净即整树删除、会话删除即回收），
本清理器只兜崩溃/断电窗口：守护进程死在「树已落盘、会话已消失」的组合里。
TTL 对齐既有 fork 孤儿清理器的 7 天语义——同一窗口内可深度恢复续用（按
tree-config.json 重建），闲置更久的残留才回收。活动集不可判定时整体跳过
（fail-closed，宁可下次再清也不误删可恢复的树）。
"""

from __future__ import annotations

__all__ = [
    "FORK_TEMPDIR_PREFIX",
    "WorkspaceResidueReaper",
]

import asyncio
import contextlib
import logging
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from workflow_controller.core.checkpoints import CheckpointStore
from workflow_controller.core.session_workspace import derive_original_dir

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# fork 孤儿目录前缀（重放/导入轨的 tempdir 约定，与插件清理器同一约定）
FORK_TEMPDIR_PREFIX = "workflow-controller-tb-"

# 默认 TTL（天）：对齐插件 fork 孤儿清理器的 7 天窗口
DEFAULT_TTL_DAYS = 7.0


@dataclass
class _ReapReport:
    """一次清理的结论（内部可累计，外部形状为 dict，见 as_dict）。"""

    removed_trees: int = 0
    removed_checkpoint_rows: int = 0
    removed_fork_roots: int = 0
    skipped: str = ""

    def as_dict(self) -> dict[str, Any]:
        """对外结论形状（CoreAPI/日志消费）。"""
        return {
            "removed_trees": self.removed_trees,
            "removed_checkpoint_rows": self.removed_checkpoint_rows,
            "removed_fork_roots": self.removed_fork_roots,
            "skipped": self.skipped,
        }


def _dir_is_old(path: Path, cutoff: float) -> bool:
    """目录 mtime 早于 cutoff（stat 失败按不老处理，保守不删）。"""
    try:
        return path.stat().st_mtime < cutoff
    except OSError:
        return False


def _session_is_live(session: Any, cutoff: float) -> bool:
    """会话是否仍在 TTL 活动窗内（last_activity 近期即活动，取不到保守活动）。"""
    last_activity = getattr(session, "last_activity", None)
    if last_activity is None:
        return True
    try:
        return bool(last_activity.timestamp() >= cutoff)
    except (AttributeError, ValueError, OSError):
        return True


class WorkspaceResidueReaper:
    """单会话残留树 + 过期检查点行 + fork 孤儿目录的 TTL 清理器。"""

    def __init__(
        self,
        *,
        checkpoint_db_path: Callable[[], Path],
        list_sessions: Callable[[], list[Any]] | None,
        scan_roots: Callable[[], list[Path]] | None = None,
        ttl_days: float = DEFAULT_TTL_DAYS,
        enabled: bool = True,
    ) -> None:
        """装配端口：检查点库、池枚举（None 即不可判定）与补充扫描根。"""
        self._checkpoint_db_path = checkpoint_db_path
        self._list_sessions = list_sessions
        self._scan_roots = scan_roots
        self._ttl_days = ttl_days
        self._enabled = enabled

    async def reap(self) -> dict[str, Any]:
        """跑一轮清理（best-effort，非阻塞异常只记日志）。"""
        if not self._enabled or self._ttl_days <= 0:
            return _ReapReport(skipped="disabled").as_dict()
        if self._list_sessions is None:
            return _ReapReport(skipped="pool enumeration unavailable").as_dict()
        try:
            sessions = self._list_sessions()
        except Exception:
            logger.warning("残留清理：池枚举失败，本轮跳过", exc_info=True)
            return _ReapReport(skipped="pool enumeration unavailable").as_dict()
        cutoff = time.time() - self._ttl_days * 86400
        live_ids = {str(getattr(s, "id", "") or "") for s in sessions}
        report = _ReapReport()
        for root in self._candidate_scan_roots(sessions):
            report = _merge_reports(report, await self._reap_scope_root(root, live_ids, cutoff))
        report = _merge_reports(report, await self._reap_fork_roots(sessions, cutoff))
        if report.removed_trees or report.removed_fork_roots:
            logger.info(
                "残留清理：回收崩溃残留树 %d 棵（检查点行 %d）、fork 孤儿目录 %d 个",
                report.removed_trees,
                report.removed_checkpoint_rows,
                report.removed_fork_roots,
            )
        return report.as_dict()

    def _candidate_scan_roots(self, sessions: list[Any]) -> list[Path]:
        """候选扫描根：各会话原目录下的 scopes 目录 + 注入的补充根。"""
        roots: list[Path] = []
        seen: set[str] = set()
        for session in sessions:
            cwd = str(getattr(getattr(session, "config", None), "cwd", "") or "")
            sid = str(getattr(session, "id", "") or "")
            if not cwd:
                continue
            original = derive_original_dir(Path(cwd), sid) or Path(cwd)
            scopes_dir = original / ".workflow-controller" / "scopes"
            key = str(scopes_dir)
            if key not in seen and scopes_dir.is_dir():
                seen.add(key)
                roots.append(scopes_dir)
        for extra in self._scan_roots() if self._scan_roots is not None else []:
            key = str(extra)
            if key not in seen:
                seen.add(key)
                roots.append(extra / ".workflow-controller" / "scopes")
        return [root for root in roots if root.is_dir()]

    async def _reap_scope_root(
        self, scopes_dir: Path, live_ids: set[str], cutoff: float
    ) -> _ReapReport:
        """清理一个 scopes 目录下的崩溃残留树（会话已消失且超 TTL）。"""
        report = _ReapReport()
        try:
            entries = list(scopes_dir.iterdir())
        except OSError:
            return report
        for tree_dir in entries:
            sid = tree_dir.name
            if sid in live_ids:
                # 会话仍在（含崩溃后经 tree-config 深度恢复续用的形态）：
                # 树属其生命周期，留给终态同步或删除路径回收。
                continue
            if not tree_dir.is_dir() or not _dir_is_old(tree_dir, cutoff):
                continue
            await asyncio.to_thread(shutil.rmtree, tree_dir, ignore_errors=True)
            if tree_dir.exists():
                continue
            report.removed_trees += 1
            report.removed_checkpoint_rows += await self._delete_tree_rows(sid)
            logger.info("残留清理：回收会话 %s 的残留树 %s", sid[:12], tree_dir)
        return report

    async def _delete_tree_rows(self, sid: str) -> int:
        """删除残留树的检查点行（树根作用域锚定，best-effort）。"""
        store = CheckpointStore(self._checkpoint_db_path())
        try:
            return int(await store.delete_for_tree(f"{sid}-root"))
        except Exception:
            logger.debug("残留清理：检查点行删除失败（不阻塞树回收）", exc_info=True)
            return 0
        finally:
            store.close()

    async def _reap_fork_roots(self, sessions: list[Any], cutoff: float) -> _ReapReport:
        """清理 tempdir 里的 fork 孤儿目录（近期活动会话引用的受保护）。"""
        report = _ReapReport()
        protected: set[str] = set()
        for session in sessions:
            metadata = getattr(session, "plugin_metadata", None) or {}
            birth = (metadata.get("traceback_replay") or {}).get("birth") or {}
            fork_root = birth.get("fork_root") if isinstance(birth, dict) else None
            if not fork_root or not _session_is_live(session, cutoff):
                continue
            with contextlib.suppress(OSError, ValueError):
                protected.add(str(Path(str(fork_root)).resolve()))
        tempdir = Path(tempfile.gettempdir())
        try:
            entries = list(tempdir.iterdir())
        except OSError:
            return report
        for entry in entries:
            if not entry.name.startswith(FORK_TEMPDIR_PREFIX) or not entry.is_dir():
                continue
            if str(entry.resolve()) in protected:
                continue
            if not _dir_is_old(entry, cutoff):
                continue
            await asyncio.to_thread(shutil.rmtree, entry, ignore_errors=True)
            if entry.exists():
                continue
            report.removed_fork_roots += 1
            logger.info("残留清理：回收 fork 孤儿目录 %s", entry)
        return report


def _merge_reports(left: _ReapReport, right: _ReapReport) -> _ReapReport:
    """合并两份结论（skipped 取先出现的非空值）。"""
    return _ReapReport(
        removed_trees=left.removed_trees + right.removed_trees,
        removed_checkpoint_rows=left.removed_checkpoint_rows + right.removed_checkpoint_rows,
        removed_fork_roots=left.removed_fork_roots + right.removed_fork_roots,
        skipped=left.skipped or right.skipped,
    )
