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

"""历史检查点兼容面：快照代检查点的统一查询、预览、重放与 session 导入。

单会话回溯能力的内核承载：查询面合并展示工作区模式行（scope_checkpoints）
与旧表历史行（checkpoints）并标注来源代；文件预览按检查点所属代分发——
工作区代读私有仓任意提交、快照代读历史快照仓（目录约定直连）；重放与
导入经本宿主的注入端口执行。
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from workflow_controller.core.checkpoint_compat import (
    LegacyCheckpointReader,
    legacy_checkpoint_to_dict,
)
from workflow_controller.core.restore.claude_seed import detect_replay_runtime
from workflow_controller.core.restore.import_runner import (
    ImportRunnerPorts,
    SessionImportRunner,
    import_log_path_for,
)
from workflow_controller.core.restore.replay import ReplayPorts, TracebackReplayRunner
from workflow_controller.core.workspace.repository import (
    legacy_snapshot_dir_for,
    legacy_snapshot_file,
    legacy_snapshot_list,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from workflow_controller.core.checkpoint_compat import LegacyCheckpoint
    from workflow_controller.core.session_workspace import SessionWorkspaceHost

__all__ = ["SessionTracebackHost"]

logger = logging.getLogger(__name__)


async def _offload(fn: Any, /, *args: Any) -> Any:
    """同步阻塞调用下放线程池（快照仓 git 读取不走事件循环）。"""
    return await asyncio.get_running_loop().run_in_executor(None, functools.partial(fn, *args))


def _noop_hook_config(_project_dir: Path) -> None:
    """hook 配置端口未装配时的空实现（headless/测试形态）。"""
    return None


def _is_safe_fork_root(fork_root: str) -> bool:
    """fork 目录安全校验：须位于系统 tempdir 且带内核前缀。"""
    try:
        path = Path(fork_root).resolve()
        tempdir = Path(tempfile.gettempdir()).resolve()
        return path.is_relative_to(tempdir) and path.name.startswith("workflow-controller-tb-")
    except (OSError, ValueError):
        return False


class SessionTracebackHost:
    """快照代检查点能力宿主：统一查询、双代预览、重放与导入的编排入口。"""

    def __init__(
        self,
        *,
        checkpoint_db_path: Callable[[], Path],
        resolve_session: Callable[[str], Awaitable[Any | None]],
        load_roster_session: Callable[[str], Awaitable[dict[str, Any] | None]],
        session_workspaces: SessionWorkspaceHost,
        start_resumed_session: Callable[..., Awaitable[Any]],
        create_session: Callable[..., Awaitable[Any]],
        persist_session: Callable[[Any], Awaitable[None]],
        store_events: Callable[[list[Any]], Awaitable[None]],
        default_runtime: Callable[[], str | None],
        resolve_runtime: Callable[[str | None], str],
        work_dir: Callable[[], Path] | None = None,
        write_hook_config: Callable[[Path], None] | None = None,
        snapshot_timeout_seconds: float = 30.0,
        git_history_max_mb: int = 0,
        source_opencode_db: Callable[[], Path | None] | None = None,
    ) -> None:
        """装配端口：检查点库、会话事实、会话创建/持久化与 runtime 解析。"""
        self._checkpoint_db_path = checkpoint_db_path
        self._resolve_session = resolve_session
        self._load_roster_session = load_roster_session
        self._session_workspaces = session_workspaces
        self._start_resumed_session = start_resumed_session
        self._create_session = create_session
        self._persist_session = persist_session
        self._store_events = store_events
        self._default_runtime = default_runtime
        self._resolve_runtime = resolve_runtime
        self._work_dir = work_dir
        self._write_hook_config = write_hook_config
        # fork 会话 → fork 目录（内存生命周期追踪；删除会话时回收，崩溃残留由清理器兜底）
        self._fork_roots: dict[str, str] = {}
        self._snapshot_timeout_seconds = snapshot_timeout_seconds
        self._git_history_max_mb = git_history_max_mb
        self._source_opencode_db = source_opencode_db

    # ── 统一查询面 ──────────────────────────────────────────

    async def list_checkpoints(self, session_id: str) -> list[dict[str, Any]]:
        """合并列出会话的两代检查点：工作区模式行 + 旧表历史行（标注来源代）。"""
        rows: list[dict[str, Any]] = []
        if self._session_workspaces.owns(session_id):
            for row in await self._session_workspaces.list_checkpoints(session_id):
                row["generation"] = "workspace"
                rows.append(row)
        reader = LegacyCheckpointReader(self._checkpoint_db_path())
        for checkpoint in reader.list_for_session(session_id):
            rows.append(legacy_checkpoint_to_dict(checkpoint))
        return rows

    async def list_checkpoint_files(
        self,
        session_id: str,
        checkpoint_id: str,
        subpath: str = "",
    ) -> list[dict[str, Any]]:
        """按检查点列文件态直接子项（预览面；按检查点所属代分发读取）。"""
        if await self._workspace_checkpoint_row(session_id, checkpoint_id) is not None:
            return await self._session_workspaces.checkpoint_tree(
                session_id, checkpoint_id, subpath
            )
        checkpoint = self._legacy_checkpoint(session_id, checkpoint_id)
        snapshot_dir = await self._snapshot_dir_of(session_id)
        if checkpoint is None or checkpoint.git_ref is None or snapshot_dir is None:
            return []
        return await _offload(legacy_snapshot_list, snapshot_dir, checkpoint.git_ref, subpath)

    async def read_checkpoint_file(
        self,
        session_id: str,
        checkpoint_id: str,
        path: str,
    ) -> dict[str, Any] | None:
        """按检查点读一个文件内容（预览面；按检查点所属代分发读取）。"""
        if not path:
            return None
        if await self._workspace_checkpoint_row(session_id, checkpoint_id) is not None:
            data = await self._session_workspaces.checkpoint_file(session_id, checkpoint_id, path)
            if data is not None:
                data.setdefault("encoding", "utf-8")
            return data
        checkpoint = self._legacy_checkpoint(session_id, checkpoint_id)
        snapshot_dir = await self._snapshot_dir_of(session_id)
        if checkpoint is None or checkpoint.git_ref is None or snapshot_dir is None:
            return None
        data = await _offload(legacy_snapshot_file, snapshot_dir, checkpoint.git_ref, path)
        if data is not None:
            data.setdefault("encoding", "utf-8")
        return data

    # ── 重放（traceback 触发的内核等价物） ──────────────────

    async def replay_checkpoint(
        self,
        session_id: str,
        checkpoint_id: str,
        *,
        runtime: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """从历史检查点重放出新会话（fork 文件态 + message 级截断对话史）。"""
        runner = TracebackReplayRunner(
            ReplayPorts(
                find_session=self._resolve_session,
                load_roster_session=self._load_roster_session,
                legacy_checkpoint=self._legacy_checkpoint_by_id,
                start_resumed_session=self._start_resumed_session,
                default_runtime=self._default_runtime,
                resolve_runtime=self._resolve_runtime,
                write_hook_config=self._write_hook_config or _noop_hook_config,
                register_fork=self._register_fork,
                work_dir=str(self._work_dir()) if self._work_dir is not None else "",
                snapshot_timeout_seconds=self._snapshot_timeout_seconds,
                git_history_max_mb=self._git_history_max_mb,
            )
        )
        return await runner.run(
            session_id,
            checkpoint_id,
            runtime=runtime,
            overrides=overrides,
        )

    def _register_fork(self, session_id: str, fork_root: str) -> None:
        """登记 fork 会话 → 目录映射（删除会话时回收，崩溃残留由清理器兜底）。"""
        self._fork_roots[session_id] = fork_root

    # ── 外部 session db 导入（import-db 的内核等价物） ───────

    async def replay_runtime(self, session_id: str) -> str:
        """探测会话源对话的 runtime（重放表单预填面；会话不在场返回空串）。"""
        session = await self._resolve_session(session_id)
        if session is not None:
            config = getattr(session, "config", None)
            sdk_session_id = str(getattr(session, "sdk_session_id", "") or "")
            cwd = str(getattr(config, "cwd", "") or "")
            runtime = getattr(config, "runtime", None)
            config_dir = (getattr(config, "env_overrides", None) or {}).get("CLAUDE_CONFIG_DIR")
        else:
            row = await self._load_roster_session(session_id)
            if row is None:
                return ""
            try:
                config = json.loads(row.get("config_json") or "{}")
            except (TypeError, ValueError):
                config = {}
            sdk_session_id = str(row.get("sdk_session_id") or "")
            cwd = str(config.get("cwd") or "")
            runtime = config.get("runtime")
            config_dir = (config.get("env_overrides") or {}).get("CLAUDE_CONFIG_DIR")
        return await asyncio.to_thread(
            detect_replay_runtime,
            sdk_session_id=sdk_session_id,
            cwd=cwd,
            configured_runtime=runtime,
            default_runtime=self._default_runtime() or "claude_sdk",
            source_config_dir=config_dir,
        )

    async def import_session(
        self,
        db_path: str | Path,
        target_cwd: str | Path,
        *,
        log_path: str | Path | None = None,
    ) -> dict[str, Any]:
        """从外部 cannbot session db 重建为本仓会话（含历史检查点）。

        同步等待到完成；``log_path`` 非空时边重建边写日志文件（行缓冲，
        供查询面下载）。结果结构与插件 import-db 逐字段一致。
        """
        runner = SessionImportRunner(
            ImportRunnerPorts(
                create_session=self._create_session,
                persist_session=self._persist_session,
                store_events=self._store_events,
                checkpoint_db_path=self._checkpoint_db_path,
                source_opencode_db=self._source_opencode_db,
            )
        )
        return await runner.import_session(
            Path(db_path),
            Path(target_cwd),
            log_path=Path(log_path) if log_path else None,
        )

    def read_import_log(self, target_cwd: str | Path) -> dict[str, Any]:
        """读导入日志内容（重建中读到部分行，完成后读全文；缺席为 None）。"""
        base = Path(target_cwd).expanduser().resolve()
        log_file = import_log_path_for(base)
        if not base.is_dir() or not log_file.is_file():
            return {"exists": False, "path": str(log_file), "content": ""}
        try:
            content = log_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return {"exists": False, "path": str(log_file), "content": ""}
        return {"exists": True, "path": str(log_file), "content": content}

    async def cleanup_fork(self, session_id: str) -> None:
        """会话删除时回收其 fork 目录（事件缺失时回落持久化溯源行）。"""
        fork_root = self._fork_roots.pop(session_id, None)
        if not fork_root:
            birth = await self._read_birth(session_id)
            fork_root = (birth or {}).get("fork_root")
        if not fork_root or not _is_safe_fork_root(str(fork_root)):
            return
        await asyncio.to_thread(shutil.rmtree, fork_root, ignore_errors=True)
        logger.info("traceback: 已回收会话 %s 的 fork 目录 %s", session_id[:12], fork_root)

    async def _read_birth(self, session_id: str) -> dict[str, Any] | None:
        """读会话的重放溯源（池内优先，回落持久化记录）。"""
        session = await self._resolve_session(session_id)
        if session is not None:
            metadata = getattr(session, "plugin_metadata", None) or {}
            birth = (metadata.get("traceback_replay") or {}).get("birth")
            return birth if isinstance(birth, dict) else None
        row = await self._load_roster_session(session_id)
        if row is None:
            return None
        raw = row.get("plugin_metadata")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                return None
        if not isinstance(raw, dict):
            return None
        birth = (raw.get("traceback_replay") or {}).get("birth")
        return birth if isinstance(birth, dict) else None

    # ── 代际解析 ────────────────────────────────────────────

    async def _workspace_checkpoint_row(
        self, session_id: str, checkpoint_id: str
    ) -> dict[str, Any] | None:
        """检查点是否属于会话的工作区树（树在位才可能命中工作区代）。"""
        if not self._session_workspaces.owns(session_id):
            return None
        for row in await self._session_workspaces.list_checkpoints(session_id):
            if row["checkpoint_id"] == checkpoint_id:
                return row
        return None

    def _legacy_checkpoint(self, session_id: str, checkpoint_id: str) -> LegacyCheckpoint | None:
        """取属于该会话的旧表历史检查点（他 session 的行不命中）。"""
        checkpoint = LegacyCheckpointReader(self._checkpoint_db_path()).get(checkpoint_id)
        if checkpoint is None or checkpoint.session_id != session_id:
            return None
        return checkpoint

    def _legacy_checkpoint_by_id(
        self, session_id: str, checkpoint_id: str
    ) -> LegacyCheckpoint | None:
        """按标识取旧表历史行（归属校验留给重放前置的拒绝口径）。"""
        return LegacyCheckpointReader(self._checkpoint_db_path()).get(checkpoint_id)

    async def _snapshot_dir_of(self, session_id: str) -> Path | None:
        """解析会话原目录对应的插件快照仓目录（托管形态反推原目录）。"""
        from workflow_controller.core.session_workspace import derive_original_dir

        cwd = await self._session_cwd(session_id)
        if not cwd:
            return None
        original = derive_original_dir(Path(cwd), session_id) or Path(cwd)
        snapshot_dir = legacy_snapshot_dir_for(original)
        return snapshot_dir if snapshot_dir.is_dir() else None

    async def _session_cwd(self, session_id: str) -> str:
        """会话工作目录：池内会话优先，回落持久化记录的配置。"""
        session = await self._resolve_session(session_id)
        if session is not None:
            return str(getattr(session.config, "cwd", "") or "")
        row = await self._load_roster_session(session_id)
        if row is None:
            return ""
        try:
            config = json.loads(row.get("config_json") or "{}")
        except (TypeError, ValueError):
            return ""
        return str(config.get("cwd") or "")
