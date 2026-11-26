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

"""单会话工作区宿主：作用域树接入、终态三方同步与实时视图。

会话宿主对 traceback 工作区模式的单会话自任编排确认方：激活前把原目
录做基线导入（seed + 集成线），主写者工作区固定落 ``scopes/<会话>/root/``
（自集成头建区 + seed 物化），会话 cwd 指向托管工作区——运行期间原目
录保持基线不变。会话终态经三方同步写回原目录，同步干净即整树回收，
外部变化则保留托管终态待查。子代理经派发屏障自然进树，回滚链复用组
检查点恢复 + 恢复会话绑定（宿主为确认方）。
"""

from __future__ import annotations

import asyncio
import base64
import logging
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from workflow_controller.core.checkpoints import (
    BarrierQuality,
    CheckpointStore,
    CheckpointTrigger,
    DispatchAnchor,
    ScopeCheckpoint,
)
from workflow_controller.core.restore.conversation import (
    ConversationRestorePlan,
    ConversationRestorer,
    TranscriptLocator,
)
from workflow_controller.core.workspace.actions import (
    AbandonScope,
    IntegrateScope,
    PrepareScopeWorkspace,
    ScopeIntegrationRejected,
    ScopeWorkspaceSpec,
)
from workflow_controller.core.workspace.dispatch_groups import (
    WORKSPACE_READER,
    ChildDispatchRequest,
)
from workflow_controller.core.workspace.scope_tree import ScopeKind, ScopeState
from workflow_controller.core.workspace.terminal_sync import (
    TerminalSyncReport,
    sync_terminal_to_original,
)
from workflow_controller.core.workspace.tree_runtime import WorkspaceTreeRuntime
from workflow_controller.driver.hooks import BarrierDeny, BarrierOutcome, BarrierProceed
from workflow_controller.plugin.dispatch_barrier import (
    dispatch_capabilities_for,
    group_arbitration_block_reason,
    writer_dispatch_block_reason,
)
from workflow_controller.plugin.workspace import WorkspaceTreeConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from workflow_controller.config.settings import SessionTracebackConfig
    from workflow_controller.core.workspace.scope_tree import ScopeRecord

__all__ = [
    "SessionWorkspaceError",
    "SessionWorkspaceHost",
    "SessionWorkspaceNotFoundError",
    "workspace_config_of",
]

logger = logging.getLogger(__name__)

# 实时视图单文件读取上限（超出拒绝，目录树不受限）
_LIVE_VIEW_MAX_FILE_BYTES = 8 * 1024 * 1024

# 单会话树内目录名：主写者工作区（终态检出）与集成线内部检出
_MAIN_WRITER_DIRNAME = "root"
_LINE_CHECKOUT_DIRNAME = "line"


class SessionWorkspaceError(RuntimeError):
    """单会话工作区宿主拒绝：状态或参数不满足时显式失败。"""


class SessionWorkspaceNotFoundError(LookupError):
    """会话没有托管工作区（查询面 404 用）。"""


def workspace_config_of(traceback: SessionTracebackConfig, root_dir: Path) -> WorkspaceTreeConfig:
    """单会话 traceback 配置 → 树装配配置（root_dir 为原目录）。"""
    if traceback.mode != "workspace":
        raise SessionWorkspaceError(
            f"单会话 traceback 仅支持 workspace 模式（收到 {traceback.mode!r}）；"
            "snapshot 兼容模式已随旧插件退役移除"
        )
    return WorkspaceTreeConfig(
        root_dir=str(root_dir),
        conflict_policy=traceback.conflict_policy,
        retain_after_merge=traceback.retain_after_merge,
        on_no_dispatch_barrier=traceback.on_no_dispatch_barrier,
        background_drain_timeout_s=traceback.background_drain_timeout_s,
        merge=tuple(traceback.merge),
        writer_local=tuple(traceback.writer_local),
        shared_readonly=tuple(traceback.shared_readonly),
        shared_exclusive=tuple(traceback.shared_exclusive),
        external=tuple(traceback.external),
    )


def derive_original_dir(cwd: Path, session_id: str) -> Path | None:
    """cwd 已指向托管工作区时反推原目录（崩溃/重开后的续用形态）。"""
    for ancestor in cwd.parents:
        if (
            ancestor.name == session_id
            and ancestor.parent.name == "scopes"
            and ancestor.parent.parent.name == ".workflow-controller"
        ):
            return ancestor.parent.parent.parent
    return None


class SessionWorkspaceTree(WorkspaceTreeRuntime):
    """单会话作用域树：根作用域为会话根，主写者工作区固定落 root/。"""

    def __init__(
        self,
        config: WorkspaceTreeConfig,
        session_id: str,
        checkpoints: CheckpointStore,
        *,
        drain_background_writers: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        super().__init__(
            config,
            session_id,
            checkpoints,
            root_scope_id=f"{session_id}-root",
            root_checkout_subdir=_LINE_CHECKOUT_DIRNAME,
            drain_background_writers=drain_background_writers,
        )
        self.session_id = session_id
        self.main_writer_scope_id = f"{session_id}-main"

    @property
    def main_writer_workspace_dir(self) -> Path:
        """主写者工作区（终态检出）固定落点。"""
        return self.tree_dir / _MAIN_WRITER_DIRNAME


@dataclass
class _SessionTreeEntry:
    """一个已接入会话的树登记项：树、原目录与收口状态。"""

    tree: SessionWorkspaceTree
    session_id: str
    original_dir: Path
    # active 运行中｜restoring 回滚链执行中｜finalizing 终态收口中
    # synced 已同步并回收｜retained 冲突/失败保留托管终态
    state: str = "active"
    final_sync: TerminalSyncReport | None = None
    # 派发准入的子会话回填线索：tool_use_id → (检查点, 子作用域)
    dispatch_admissions: dict[str, tuple[str, str]] = field(default_factory=dict)


def _sync_report_to_dict(report: TerminalSyncReport | None) -> dict[str, Any] | None:
    """同步报告的对外形状。"""
    if report is None:
        return None
    return {
        "refused": report.refused,
        "applied": list(report.applied),
        "removed": list(report.removed),
        "conflict_paths": list(report.conflict_paths),
    }


def _list_live_entries(workspace_dir: Path) -> list[dict[str, Any]]:
    """托管工作区目录树（跳过 .git 指针，目录/文件/符号链接分型）。"""
    entries: list[dict[str, Any]] = []
    if not workspace_dir.is_dir():
        return entries
    for path in sorted(workspace_dir.rglob("*")):
        rel = path.relative_to(workspace_dir).as_posix()
        if rel.split("/", 1)[0] == ".git":
            continue
        if path.is_symlink():
            entries.append({"path": rel, "type": "symlink"})
        elif path.is_dir():
            entries.append({"path": rel, "type": "dir"})
        else:
            entries.append({"path": rel, "type": "file", "size": path.stat().st_size})
    return entries


def _checkpoint_tree_entries(blobs: dict[str, str], subpath: str) -> list[dict[str, Any]]:
    """把提交的路径 → blob 平面集裁成预览子项（目录在前，含内部 git 保留路径隐藏）。"""
    prefix = f"{subpath}/" if subpath else ""
    names: dict[str, bool] = {}
    for path in blobs:
        if not path.startswith(prefix):
            continue
        name = path[len(prefix) :].split("/", 1)[0]
        if not name:
            continue
        if not subpath and name == ".workflow-controller-inner-git":
            continue
        is_file = len(path) == len(prefix) + len(name)
        names[name] = names.get(name, False) or is_file
    entries = [
        {"name": name, "path": f"{subpath}/{name}".strip("/"), "is_dir": not is_file_only}
        for name, is_file_only in names.items()
    ]
    entries.sort(key=lambda e: (not e["is_dir"], e["name"]))
    return entries


def _read_live_file(workspace_dir: Path, rel_path: str) -> dict[str, Any]:
    """读托管工作区内一个文件：文本优先、二进制回退 base64。"""
    target = (workspace_dir / rel_path).resolve()
    if not target.is_relative_to(workspace_dir.resolve()):
        raise SessionWorkspaceError(f"实时视图路径越界：{rel_path}")
    if not target.is_file():
        raise SessionWorkspaceNotFoundError(f"文件不在托管工作区：{rel_path}")
    size = target.stat().st_size
    if size > _LIVE_VIEW_MAX_FILE_BYTES:
        raise SessionWorkspaceError(f"文件超出实时视图大小上限（{size} 字节）：{rel_path}")
    raw = target.read_bytes()
    try:
        return {"path": rel_path, "size": size, "encoding": "utf-8", "content": raw.decode("utf-8")}
    except UnicodeDecodeError:
        return {
            "path": rel_path,
            "size": size,
            "encoding": "base64",
            "content": base64.b64encode(raw).decode("ascii"),
        }


class SessionWorkspaceHost:
    """会话宿主侧的单会话工作区注册与生命周期入口。"""

    def __init__(
        self,
        *,
        checkpoint_db_path: Callable[[], Path],
        resolve_session: Callable[[str], Awaitable[Any]],
        persist_session: Callable[[Any], Awaitable[None]],
        stop_session: Callable[[str], Awaitable[None]],
        start_resumed_session: Callable[..., Awaitable[Any]],
        transcript_locator: TranscriptLocator | None = None,
        drain_background_writers: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        """装配端口：检查点库、会话事实与恢复会话创建（均可注入替身）。"""
        self._checkpoint_db_path = checkpoint_db_path
        self._resolve_session = resolve_session
        self._persist_session = persist_session
        self._stop_session = stop_session
        self._restorer = ConversationRestorer(
            locate_transcript=transcript_locator or _default_locator,
            start_resumed_session=start_resumed_session,
        )
        self._drain_background_writers = drain_background_writers
        self._entries: dict[str, _SessionTreeEntry] = {}
        # 恢复会话等派生会话 → 树主会话（同树复用）
        self._aliases: dict[str, str] = {}
        # 每会话的屏障串行锁：并行工具调用的组仲裁不交错
        self._barrier_locks: dict[str, asyncio.Lock] = {}
        # 接入串行锁：并发重复激活不 double 建树
        self._prepare_lock = asyncio.Lock()

    # ── 查询面 ────────────────────────────────────────────────

    def owns(self, session_id: str) -> bool:
        """会话是否已接入托管工作区（含恢复会话等派生身份）。"""
        return session_id in self._entries or session_id in self._aliases

    def _resolve(self, session_id: str) -> _SessionTreeEntry | None:
        """会话身份 → 树登记项（派生身份经别名映射）。"""
        owner = self._aliases.get(session_id, session_id)
        return self._entries.get(owner)

    def _require_entry(self, session_id: str) -> _SessionTreeEntry:
        """取树登记项；未接入按查询面 404 拒绝。"""
        entry = self._resolve(session_id)
        if entry is None:
            raise SessionWorkspaceNotFoundError(f"会话没有托管工作区：{session_id}")
        return entry

    async def live_view(self, session_id: str, path: str | None = None) -> dict[str, Any]:
        """托管工作区当前文件态的只读视图：无 path 给目录树，有 path 读文件。"""
        entry = self._require_entry(session_id)
        payload: dict[str, Any] = {
            "session_id": session_id,
            "owner_session_id": entry.session_id,
            "state": entry.state,
            "workspace_path": str(entry.tree.main_writer_workspace_dir),
            "original_dir": str(entry.original_dir),
            "final_sync": _sync_report_to_dict(entry.final_sync),
        }
        if entry.state == "synced":
            return payload  # 树已回收：只余结论面
        workspace_dir = entry.tree.main_writer_workspace_dir
        if not workspace_dir.is_dir():
            return payload
        if path is None:
            payload["entries"] = await asyncio.to_thread(_list_live_entries, workspace_dir)
        else:
            payload.update(await asyncio.to_thread(_read_live_file, workspace_dir, path))
        return payload

    async def list_checkpoints(self, session_id: str) -> list[dict[str, Any]]:
        """会话树的检查点查询面（查询与回滚入口共用）。"""
        entry = self._require_entry(session_id)
        from workflow_controller.core.checkpoints import checkpoint_to_dict

        rows = await entry.tree.checkpoints.list_for_tree(entry.tree.root_scope_id)
        return [checkpoint_to_dict(row) for row in rows]

    async def checkpoint_tree(
        self, session_id: str, checkpoint_id: str, subpath: str = ""
    ) -> list[dict[str, Any]]:
        """按检查点列私有仓文件态直接子项（目录在前，供预览面）。"""
        entry = self._require_entry(session_id)
        commit = await self._checkpoint_commit(entry, checkpoint_id)
        if commit is None:
            return []
        blobs = await entry.tree.repository.tree_blob_entries(commit)
        return _checkpoint_tree_entries(blobs, subpath.strip("/"))

    async def checkpoint_file(
        self, session_id: str, checkpoint_id: str, path: str
    ) -> dict[str, Any] | None:
        """按检查点读私有仓内一个文件的内容（文本优先、二进制回退 base64）。"""
        entry = self._require_entry(session_id)
        commit = await self._checkpoint_commit(entry, checkpoint_id)
        if commit is None:
            return None
        rel = path.strip("/").lstrip("./")
        if not rel:
            return None
        repository = entry.tree.repository
        size = await repository.blob_size(commit, rel)
        if size is None:
            return None
        if size > _LIVE_VIEW_MAX_FILE_BYTES:
            return {
                "name": rel.rsplit("/", 1)[-1],
                "path": rel,
                "size": size,
                "content": "(file too large to preview)",
                "encoding": "utf-8",
                "truncated": True,
            }
        raw = await repository.blob_content(commit, rel)
        if raw is None:
            return None
        try:
            content: str = raw.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            content = base64.b64encode(raw).decode("ascii")
            encoding = "base64"
        return {
            "name": rel.rsplit("/", 1)[-1],
            "path": rel,
            "size": size,
            "content": content,
            "encoding": encoding,
            "truncated": False,
        }

    async def _checkpoint_commit(self, entry: _SessionTreeEntry, checkpoint_id: str) -> str | None:
        """取检查点行的文件态提交引用；行缺席或树已回收返回 None。"""
        rows = await entry.tree.checkpoints.list_for_tree(entry.tree.root_scope_id)
        for row in rows:
            if row.checkpoint_id == checkpoint_id:
                return row.workspace_commit or None
        return None

    # ── 接入与复用 ────────────────────────────────────────────

    async def prepare_session(self, session: Any) -> _SessionTreeEntry:
        """接入（或复用）会话的托管工作区，并把 cwd 指向主写者工作区。

        建区失败即抛错（fail-fast 不降级）并尽力回收半成品树；崩溃/重
        开后的既有树经深度恢复重建账本后复用同一工作区。
        """
        async with self._prepare_lock:
            return await self._prepare_session_locked(session)

    async def _prepare_session_locked(self, session: Any) -> _SessionTreeEntry:
        """接入执行体（接入串行锁内）。"""
        sid = str(session.id)
        existing = self._resolve(sid)
        if existing is not None:
            if existing.state == "synced":
                # 树已回收：会话重激活按当前原目录重新建树（新基线）
                self._drop_entry(existing)
            else:
                if existing.state == "retained":
                    existing.state = "active"  # 会话续用保留树，收口将重试同步
                await self._point_session_cwd(session, existing)
                return existing
        traceback = session.config.traceback
        cwd = Path(session.config.cwd).expanduser().resolve()
        # cwd 已在托管树内（崩溃/重开）：反推原目录续用既有树
        original_dir = derive_original_dir(cwd, sid) or cwd
        if not original_dir.is_dir():
            raise SessionWorkspaceError(f"会话工作目录不存在：{original_dir}")
        tree = SessionWorkspaceTree(
            workspace_config_of(traceback, original_dir),
            sid,
            CheckpointStore(self._checkpoint_db_path()),
            drain_background_writers=self._drain_background_writers,
        )
        entry = _SessionTreeEntry(tree=tree, session_id=sid, original_dir=original_dir)
        fresh = not tree.repository.repo_dir.exists()
        self._entries[sid] = entry
        try:
            await tree.initialize()
            writer = await tree.manager.get_scope(tree.main_writer_scope_id)
            if writer is None:
                if tree.main_writer_workspace_dir.exists():
                    # 崩溃窗残留的半成品建区（无检查点行无账）：清理后重建
                    await tree.repository.prune_scope_residue(
                        tree.main_writer_workspace_dir,
                        tree.main_writer_scope_id,
                    )
                await self._prepare_main_writer(entry, session)
            elif writer.state is not ScopeState.ACTIVE:
                # 崩溃窗残留（已集成/已释放/冲突）：废弃换代后按当前头重建
                await tree.manager.abandon_scope(
                    AbandonScope(
                        scope_id=tree.main_writer_scope_id,
                        expected_scope_epoch=writer.scope_epoch,
                        client_op_id=f"session-{sid}-reabandon",
                    )
                )
                await self._prepare_main_writer(entry, session)
            await self._point_session_cwd(session, entry)
        except BaseException:
            self._entries.pop(sid, None)
            tree.checkpoints.close()
            if fresh:
                # 半成品新树整目录回收；既有树（恢复失败）保留待查
                await asyncio.to_thread(shutil.rmtree, tree.tree_dir, ignore_errors=True)
            raise
        return entry

    async def _prepare_main_writer(self, entry: _SessionTreeEntry, session: Any) -> None:
        """建主写者作用域：会话账本先落账，再建区并落 scope_start 检查点。"""
        tree = entry.tree
        orchestration_ref = await self._persist_ledger_ref(session)
        prepared = await tree.manager.prepare_scope_workspace(
            PrepareScopeWorkspace(
                parent_scope_id=tree.root_scope_id,
                scope_key=tree.main_writer_scope_id,
                spec=ScopeWorkspaceSpec(
                    scope_kind=ScopeKind.MAIN_WRITER,
                    owner_ref=f"session={entry.session_id}",
                ),
                # 操作号带随机后缀：宿主重启后序号归零不与既有 saga 撞号
                client_op_id=f"session-{entry.session_id}-prepare-{uuid4().hex[:8]}",
                workspace_dir=tree.main_writer_workspace_dir,
            )
        )
        scope = await tree.manager.get_scope(prepared.scope_id)
        if scope is None:
            raise SessionWorkspaceError(f"建区后作用域账本缺行：{prepared.scope_id}")
        await self._record_checkpoint(
            tree,
            scope=scope,
            trigger=CheckpointTrigger.SCOPE_START,
            workspace_commit=prepared.base_commit,
            workspace_branch=self._branch_of(scope),
            integration_commit=prepared.integration_base_commit,
            orchestration_state_ref=orchestration_ref,
        )

    def _drop_entry(self, entry: _SessionTreeEntry) -> None:
        """摘除树登记项及其全部派生别名。"""
        self._entries.pop(entry.session_id, None)
        for alias in [a for a, owner in self._aliases.items() if owner == entry.session_id]:
            self._aliases.pop(alias, None)
        self._barrier_locks.pop(entry.session_id, None)

    async def _point_session_cwd(self, session: Any, entry: _SessionTreeEntry) -> None:
        """会话 cwd 指向托管工作区（原目录保持基线不动）。"""
        managed = entry.tree.main_writer_workspace_dir
        if Path(session.config.cwd) != managed:
            session.config.cwd = str(managed)
            await self._persist_session(session)

    async def _persist_ledger_ref(self, session: Any) -> str:
        """会话账本版本先持久化，再取其不透明引用（检查点前置顺序）。"""
        await self._persist_session(session)
        return f"session-ledger:{session.id}"

    # ── 派发屏障与子会话回填 ──────────────────────────────────

    async def submit_dispatch_barrier(
        self,
        session_id: str,
        kind: str,
        data: dict[str, Any],
        *,
        runtime: str = "",
        workspace: str = "writer",
    ) -> BarrierOutcome:
        """会话宿主侧屏障入口：组仲裁建检查点，task 场景建子工作区回 cwd。"""
        entry = self._resolve(session_id)
        if entry is None:
            return BarrierProceed()  # 无树：照旧放行（与 run 平面同语义）
        if entry.state in ("finalizing", "restoring"):
            return BarrierDeny(reason=f"会话工作区正在收口/回滚，拒绝本次副作用：{session_id}")
        if entry.state != "active":
            return BarrierProceed()  # 已收口树的迟到回执路径：照旧放行
        tree = entry.tree
        caps = dispatch_capabilities_for(runtime)
        strict = tree.strict_barrier
        if kind == "dispatch":
            block = writer_dispatch_block_reason(caps, strict=strict)
        else:
            block = group_arbitration_block_reason(caps, strict=strict)
        if block is not None:
            return BarrierDeny(reason=block)
        lock = self._barrier_locks.setdefault(entry.session_id, asyncio.Lock())
        async with lock:
            try:
                return await self._execute_barrier(entry, session_id, kind, data, workspace)
            except Exception as e:
                # 屏障失败显式拒绝（不抛进驱动的工具回调），本次副作用不执行
                return BarrierDeny(reason=f"{type(e).__name__}: {e}")

    async def _execute_barrier(
        self,
        entry: _SessionTreeEntry,
        session_id: str,
        kind: str,
        data: dict[str, Any],
        workspace: str,
    ) -> BarrierOutcome:
        """屏障核心动作链：账本先落 → 组仲裁 → 回 cwd/放行。"""
        tree = entry.tree
        parent = await tree.manager.get_scope(tree.main_writer_scope_id)
        if parent is None or parent.state is not ScopeState.ACTIVE:
            return BarrierDeny(reason=f"会话主写者作用域不可用，屏障拒绝：{session_id}")
        child: ChildDispatchRequest | None = None
        preallocated = ""
        if kind == "dispatch":
            scope_key, client_op_id = tree.next_child_identity(tree.main_writer_scope_id)
            child = ChildDispatchRequest(
                scope_key=scope_key,
                client_op_id=client_op_id,
                owner_ref=f"session={session_id}",
                workspace=WORKSPACE_READER if workspace == WORKSPACE_READER else "writer",
            )
            preallocated = scope_key
        anchor = self._barrier_anchor(data, preallocated)
        session = await self._resolve_session(session_id)
        if session is None:
            return BarrierDeny(reason=f"会话不在池内，屏障无法落账：{session_id}")
        orchestration_ref = await self._persist_ledger_ref(session)
        admission = await tree.arbiter.admit_side_effect(
            parent_scope_id=tree.main_writer_scope_id,
            anchor=anchor,
            child=child,
            barrier_quality=(
                BarrierQuality.PRECISE if tree.strict_barrier else BarrierQuality.LATE_BARRIER
            ),
            orchestration_state_ref=orchestration_ref,
            tool_name=str(data.get("tool_name") or ""),
            tool_input=data.get("tool_input"),
        )
        if child is not None and admission.child_scope_id:
            entry.dispatch_admissions[anchor.tool_use_id] = (
                admission.checkpoint_id,
                admission.child_scope_id,
            )
        return BarrierProceed(child_cwd=admission.child_cwd)

    @staticmethod
    def _barrier_anchor(data: dict[str, Any], preallocated_scope_id: str) -> DispatchAnchor:
        """由屏障载荷构造锚点草稿（组标识由组仲裁回填）。"""
        message_key = str(data.get("prompt_id") or data.get("message_id") or "")
        if not message_key:
            raise SessionWorkspaceError("屏障载荷缺少消息标识，无法形成派发锚点")
        return DispatchAnchor(
            parent_native_session_id=str(
                data.get("native_session_id") or data.get("session_id") or ""
            ),
            parent_message_id=message_key,
            tool_use_id=str(data.get("tool_use_id") or ""),
            preallocated_scope_id=preallocated_scope_id,
            exclude_message_id=message_key,
            dispatch_group_id="",
        )

    async def record_subagent_start(self, session_id: str, data: dict[str, Any]) -> None:
        """子代理启动回执：按派发准入回填组检查点行的子会话映射（尽力）。"""
        entry = self._resolve(session_id)
        if entry is None:
            return
        tool_use_id = str(data.get("parent_tool_use_id") or data.get("tool_use_id") or "")
        native_child = str(data.get("agent_id") or "")
        admission = entry.dispatch_admissions.get(tool_use_id)
        if not tool_use_id or not native_child or admission is None:
            return
        try:
            await entry.tree.arbiter.attach_child_session(admission[0], admission[1], native_child)
        except Exception:
            logger.warning("子会话映射回填失败（不阻塞子代理运行）：checkpoint=%s", admission[0])

    # ── 终态同步与回收 ────────────────────────────────────────

    async def finalize_session(self, session_id: str) -> TerminalSyncReport | None:
        """会话终态收口：主写者终态提交并集成到会话根，三方同步写回原目录。

        同步干净即整树回收（工作区、私有仓与检查点连接）；外部变化按路
        径报告冲突并保留托管终态。幂等：非活动态返回已有结论。
        """
        entry = self._resolve(session_id)
        if entry is None:
            return None
        if entry.state != "active":
            return entry.final_sync
        entry.state = "finalizing"
        tree = entry.tree
        try:
            report = await self._finalize_active_entry(entry)
        except BaseException:
            entry.state = "retained"
            raise
        entry.final_sync = report
        if report.refused:
            entry.state = "retained"
            logger.warning(
                "会话 %s 终态同步遇外部变化，托管终态保留待查：%s",
                entry.session_id,
                ", ".join(report.conflict_paths[:10]),
            )
        else:
            entry.state = "synced"
            tree.checkpoints.close()
            await asyncio.to_thread(shutil.rmtree, tree.tree_dir, ignore_errors=True)
        return report

    async def _finalize_active_entry(self, entry: _SessionTreeEntry) -> TerminalSyncReport:
        """终态收口执行体：提交 → 集成 → 同步。"""
        tree = entry.tree
        sid = entry.session_id
        writer = await tree.manager.get_scope(tree.main_writer_scope_id)
        if writer is None:
            raise SessionWorkspaceError(f"会话主写者作用域缺行：{sid}")
        if writer.state is ScopeState.ACTIVE:
            session = await self._resolve_session(sid)
            if session is None:
                raise SessionWorkspaceError(f"会话不在池内，无法落终态检查点：{sid}")
            orchestration_ref = await self._persist_ledger_ref(session)
            commit, branch = await tree.manager.commit_scope_terminal(tree.main_writer_scope_id)
            scope = await tree.manager.get_scope(tree.main_writer_scope_id)
            assert scope is not None
            integration_head = await tree.manager.current_integration_head()
            await self._record_checkpoint(
                tree,
                scope=scope,
                trigger=CheckpointTrigger.TERMINAL,
                workspace_commit=commit,
                workspace_branch=branch,
                integration_commit=integration_head,
                orchestration_state_ref=orchestration_ref,
            )
            outcome = await tree.manager.integrate_scope(
                IntegrateScope(
                    scope_id=tree.main_writer_scope_id,
                    receipt=None,
                    client_op_id=f"session-{sid}-finalize-e{scope.scope_epoch}",
                )
            )
            if isinstance(outcome, ScopeIntegrationRejected):
                return TerminalSyncReport(conflict_paths=outcome.conflict_paths)
        return await sync_terminal_to_original(
            repository=tree.repository,
            workspace_dir=tree.main_writer_workspace_dir,
            original_dir=entry.original_dir,
            manifest=tree.manifest,
        )

    async def cleanup_session(self, session_id: str) -> None:
        """会话删除路径的整树回收（对已同步树为幂等空操作）。

        检查点行随树一并删除——会话既已删除，行不再有查询面（重放/回滚
        均以会话在场为前提），留存只会成为账面孤儿。
        """
        owner = self._aliases.pop(session_id, session_id)
        entry = self._entries.pop(owner, None)
        self._barrier_locks.pop(owner, None)
        if entry is None:
            return
        try:
            await entry.tree.checkpoints.delete_for_tree(entry.tree.root_scope_id)
        except Exception:
            logger.warning("会话 %s 删除时检查点行清理失败（不阻塞树回收）", session_id[:12])
        entry.tree.checkpoints.close()
        await asyncio.to_thread(shutil.rmtree, entry.tree.tree_dir, ignore_errors=True)

    # ── 子代理回滚（宿主自任编排确认方） ──────────────────────

    async def restore_session_subagent(
        self,
        session_id: str,
        *,
        checkpoint_id: str,
        expected_epoch: int,
        client_op_id: str,
        replay_dispatch: bool = False,
    ) -> dict[str, Any]:
        """单会话子代理回滚：组检查点恢复 + 恢复会话绑定（同一条链内串行）。

        链内次序对齐异步确认链：恢复文件态并备好对话数据 → 宿主落失效
        账（停旧会话）→ 才创建并绑定恢复会话；恢复会话经别名接续同一棵
        托管树。
        """
        entry = self._resolve(session_id)
        if entry is None:
            raise SessionWorkspaceNotFoundError(f"会话没有托管工作区：{session_id}")
        if entry.state != "active":
            raise SessionWorkspaceError(f"会话工作区不在运行态，拒绝回滚：{session_id}")
        tree = entry.tree
        sid = entry.session_id
        checkpoint = await tree.checkpoints.get(checkpoint_id)
        if checkpoint is None:
            raise ValueError(f"检查点不存在：{checkpoint_id}")
        if checkpoint.trigger is not CheckpointTrigger.BEFORE_CHILD:
            raise ValueError(f"回滚目标须为派发组检查点：{checkpoint_id}")
        if checkpoint.scope_id != tree.main_writer_scope_id:
            raise ValueError(
                f"检查点不属于会话主写者作用域：{checkpoint_id}（{checkpoint.scope_id}）"
            )
        anchor = checkpoint.dispatch_anchor
        if anchor is None or not anchor.dispatch_group_id:
            raise ValueError(f"检查点缺少派发锚点：{checkpoint_id}")
        if checkpoint_id != (
            f"{tree.main_writer_scope_id}:before_child:{anchor.dispatch_group_id}"
        ):
            raise ValueError(f"回滚目标须为组恢复点行（而非派发行）：{checkpoint_id}")
        replay_task: dict[str, str] | None = None
        if replay_dispatch:
            replay_task = tree.replay_ledger.require_single_task(
                anchor.dispatch_group_id, checkpoint_id
            )
        session = await self._resolve_session(sid)
        if session is None:
            raise SessionWorkspaceError(f"会话不在池内，无法回滚：{sid}")

        entry.state = "restoring"
        try:
            restored = await tree.manager.restore_scope(
                tree.main_writer_scope_id,
                restore_commit=checkpoint.workspace_commit,
                expected_scope_epoch=expected_epoch,
                client_op_id=client_op_id,
            )
            plan = ConversationRestorePlan(
                run_id=self._run_id_of(session),
                scope_id=tree.main_writer_scope_id,
                scope_epoch=restored.new_scope_epoch,
                checkpoint_id=checkpoint_id,
                anchor=anchor,
                workspace_path=str(tree.main_writer_workspace_dir),
                seed_dir=tree.tree_dir / "restore" / client_op_id,
                session_runtime=session.config.runtime,
                replay_tool_name=replay_task.get("tool_name", "") if replay_task else "",
                replay_tool_input=(
                    _json_loads_or_none(replay_task.get("tool_input", "")) if replay_task else None
                ),
                traceback=session.config.traceback,
            )
            data = await self._restorer.prepare(plan)
            tree.arbiter.forget(tree.main_writer_scope_id)
            # 宿主自任编排确认方：失效账（停旧会话）持久化后才创建新会话
            await self._stop_session(sid)
            binding = await self._restorer.bind(data)
        except BaseException:
            entry.state = "active"
            raise
        entry.state = "active"
        self._aliases[str(binding.session_id)] = sid
        return {
            "restored_session_id": str(binding.session_id),
            "scope_epoch": restored.new_scope_epoch,
            "checkpoint_id": checkpoint_id,
            "kept_messages": data.kept_messages,
            "boundary_message_id": data.boundary_message_id,
        }

    # ── 内部工具 ──────────────────────────────────────────────

    @staticmethod
    def _run_id_of(session: Any) -> str:
        """会话的 run 归属（隐式 run 保持恢复会话的谱系连续）。"""
        return str(session.config.run_id or "")

    @staticmethod
    def _branch_of(scope: ScopeRecord) -> str | None:
        """从工作区引用解出分支名供检查点行落账。"""
        if not scope.workspace_ref:
            return None
        return scope.workspace_ref.rpartition("|")[2] or None

    async def _record_checkpoint(
        self,
        tree: SessionWorkspaceTree,
        *,
        scope: ScopeRecord,
        trigger: CheckpointTrigger,
        workspace_commit: str,
        workspace_branch: str | None,
        integration_commit: str | None,
        orchestration_state_ref: str,
    ) -> str:
        """落一行生命周期检查点（文件提交与账本引用都已先在）。"""
        checkpoint_id = f"{scope.scope_id}:{trigger.value}:e{scope.scope_epoch}"
        await tree.checkpoints.record(
            ScopeCheckpoint(
                checkpoint_id=checkpoint_id,
                scope_id=scope.scope_id,
                scope_kind=scope.scope_kind,
                parent_scope_id=scope.parent_scope_id,
                scope_epoch=scope.scope_epoch,
                root_epoch=scope.root_epoch,
                trigger=trigger,
                barrier_quality=BarrierQuality.PRECISE,
                workspace_commit=workspace_commit,
                workspace_branch=workspace_branch,
                integration_commit=integration_commit,
                orchestration_state_ref=orchestration_state_ref,
                created_at=datetime.now(tz=UTC).isoformat(),
            )
        )
        return checkpoint_id


def _default_locator(native_session_id: str, workspace_path: str) -> Path | None:
    """默认 L2 定位：按父会话原生标识与工作目录解析 claude 系 JSONL。"""
    if not native_session_id:
        return None
    from workflow_controller.transcript.sources import resolve_claude_session_path

    return resolve_claude_session_path(native_session_id, workspace_path, None)


def _json_loads_or_none(raw: str) -> Any:
    """组台账留存的工具参数 JSON 还原；坏行返回 None。"""
    import json

    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
