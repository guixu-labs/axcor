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

"""Agent Traceback 插件 — 检查点重放。

每个 subagent start 时刻打一次文件态快照（git 仓）；session 跑完时打终态快照。
用户可从任一检查点起重放（re-call X / final-state replay）：恢复文件态 + 截断对话史
→ 起 fork session 续跑。

阻塞型钩子 ``on_subagent_start`` 在 driver hook 调用栈内 ``await``，worker 被管道背压
暂停，保证文件态冻结在 subagent start 前（设计 §3.8）。

config.yaml 承载配置（snapshot_dir/ignore/max_checkpoints/snapshot_timeout_seconds/
snapshot_git_state/snapshot_git_state_max_mb），进 PluginConfig.extra。插件加载即启用，
enabled 由 config.yaml 控制。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from workflow_controller.plugin.sdk import InterceptResult, Plugin
from workflow_controller.plugins.agent_traceback.checkpoint_store import Checkpoint, CheckpointStore
from workflow_controller.plugins.agent_traceback.snapshot_engine import (
    DEFAULT_IGNORE_PATTERNS,
    SnapshotEngine,
    SnapshotResult,
)
from workflow_controller.session.events import Event, EventType

if TYPE_CHECKING:
    from workflow_controller.plugin.base import PluginConfig
    from workflow_controller.plugin.context import PluginContext, PluginServices

logger = logging.getLogger(__name__)

# Reaper 默认值：扫描 host tempdir 里 Workflow Controller 进程崩溃/重启留下的孤儿
# ``workflow-controller-tb-*`` 目录，mtime 超过 TTL 的会被清理。
#
# fork_root 是纯隔离 tempdir（~2GB）。默认 7 天，给闲置 replay session 充裕恢复窗口；
# 闲置更久的 ghost 才在下次启动时回收（liveness = last_activity 在 TTL 窗口内）。
_REAPER_DEFAULT_MAX_AGE_DAYS = 7
_REAPER_TEMPDIR_PREFIX = "workflow-controller-tb-"


def _reconstruct_aggressive_clean(session_id: str, target_cwd: str, base: Path) -> list[str]:
    """激进清理：rmtree 整个 target_cwd（拒绝根目录 / 过浅路径，防 metadata 注入误删）。"""
    removed: list[str] = []
    if base.parent == base or len(base.parts) <= 2:
        logger.warning(
            "traceback: refusing aggressive rmtree of %s — root or too-shallow path",
            target_cwd,
        )
        return removed
    shutil.rmtree(base, ignore_errors=True)
    removed.append(str(base))
    logger.info(
        "traceback: aggressive-clean removed target_cwd %s for %s",
        target_cwd,
        session_id[:12],
    )
    return removed


def _reconstruct_subset_clean(session_id: str, base: Path, birth: dict[str, Any]) -> list[str]:
    """默认清理：只清 Workflow Controller 自己产出的子集（best-effort）。"""
    removed: list[str] = []
    workflow_controller_p = base / ".workflow-controller"
    if workflow_controller_p.is_dir():
        shutil.rmtree(workflow_controller_p, ignore_errors=True)
        removed.append(str(workflow_controller_p))
    if birth.get("created_opencode_dir"):
        opencode_p = base / ".opencode"
        if opencode_p.is_dir():
            shutil.rmtree(opencode_p, ignore_errors=True)
            removed.append(str(opencode_p))
    # .git 仅当重建产生（created_git_dir=True）。旧 session 无此标记 → 不清。
    if birth.get("created_git_dir"):
        git_p = base / ".git"
        if git_p.is_dir():
            shutil.rmtree(git_p, ignore_errors=True)
            removed.append(str(git_p))
    if removed:
        logger.info(
            "traceback: cleaned %d reconstruct artifacts for %s: %s",
            len(removed),
            session_id[:12],
            removed,
        )
    return removed


class _CheckpointContext(NamedTuple):
    """检查点快照上下文（``_prepare_checkpoint_context`` 的产物）。"""

    engine: SnapshotEngine
    cp_store: CheckpointStore
    existing: list[Checkpoint]
    checkpoint_id: str
    parent_cp_id: str | None
    iteration: int


def _reap_orphan_entry(entry: Path, cutoff: float) -> bool:
    """孤儿目录 mtime 早于 cutoff 时 rmtree，返回是否已清理（stat 失败不清理）。"""
    try:
        too_old = entry.stat().st_mtime < cutoff
    except OSError:
        return False
    if not too_old:
        return False
    shutil.rmtree(entry, ignore_errors=True)
    return True


def _fork_root_is_live(session: Any, cutoff: float) -> bool:
    """session 的 fork_root 是否仍受保护：last_activity 在 cutoff 之后。

    last_activity 取不到或解析失败时保守视为 live（不误删可恢复的 fork）。
    """
    la = getattr(session, "last_activity", None)
    if la is None:
        return True
    try:
        la_ts = la.timestamp()
    except (AttributeError, ValueError, OSError):
        return True
    return bool(la_ts >= cutoff)


class AgentTracebackPlugin(Plugin):
    def __init__(self, config: PluginConfig, config_path: Path | None) -> None:
        super().__init__(config, config_path)
        # Per-project SnapshotEngine 仓（按 cwd 缓存，与 EventStoreRouter 对称）。
        self._engines: dict[str, SnapshotEngine] = {}
        # Per-project CheckpointStore（按 db 路径缓存，与 events 同库）。核心不再
        # 含 checkpoint 概念；插件经能力端口拿路径自己开连接。
        self._cp_stores: dict[str, CheckpointStore] = {}
        # fork session_id → fork_root path（in-memory 生命周期追踪）。
        # TracebackRunner.run() 在 session 注册成功后登记；on_session_delete 取出
        # rmtree 掉 fork_root（含 ~2GB fork opencode.db + .git 副本 + 恢复的文件）。
        # 进程崩溃时此 map 丢失——由启动期 reaper（_reap_orphan_fork_roots）兜底。
        self._fork_roots: dict[str, str] = {}

    @property
    def work_dir(self) -> Path:
        """启动期工作目录（traceback_runner 等插件内协作者的公开读取面）。"""
        return self._ctx.work_dir

    @property
    def _services(self) -> PluginServices:
        """宿主能力端口（install 注入的 ctx 派生）。"""
        return self._ctx.services

    @staticmethod
    def _is_safe_fork_root(fork_root: str) -> bool:
        """路径安全校验：fork_root 必须位于系统 tempdir 且名称符合 workflow-controller-tb-* 前缀。

        metadata 属不可信输入（持久化 JSON），直接 rmtree 其携带的路径有风险。
        fork_root 本就是 ``tempfile.mkdtemp(prefix="workflow-controller-tb-")`` 产物，严格匹配此约定。
        """
        try:
            p = Path(fork_root).resolve()
            tempdir = Path(tempfile.gettempdir()).resolve()
            return p.is_relative_to(tempdir) and p.name.startswith(_REAPER_TEMPDIR_PREFIX)
        except (OSError, ValueError):
            return False

    def install(self, ctx: PluginContext) -> None:
        """Single lifecycle entry: snapshot engine init + bus wiring.

        ``super().install(ctx)`` captures ``_ctx`` and dispatches
        :meth:`_register_interceptors` (SUBAGENT_START / session_delete) and
        :meth:`_register_observers` (session_complete). Then setup: run the
        orphan-fork-root reaper once. The reaper is a startup-only crash
        fallback that skips fork_roots of still-running forks.
        """
        super().install(ctx)
        # 不注册 hook routes（无 CC-hook）；web 路由由 get_web_router 贡献
        # Best-effort reaper：清理上次崩溃/重启留下的孤儿 fork_root 目录。
        # 非阻塞：失败只记录日志，绝不影响启动。
        self._reap_orphan_fork_roots()
        logger.info("agent-traceback plugin installed")

    def uninstall(self) -> None:
        # 先注销 bus handler，避免 reload 后旧实例的 handler 仍留在链中。
        self._detach_bus_handlers()
        self._engines.clear()
        self._fork_roots.clear()
        logger.info("agent-traceback plugin uninstalled")

    def register_fork_root(self, fork_session_id: str, fork_root: str) -> None:
        """登记 fork session → fork_root 映射，供生命周期清理。

        由 ``TracebackRunner.run`` 在 fork session 注册到 pool 成功后调用。
        ``on_session_delete`` 取出此映射，在 fork session 被删除时 rmtree fork_root。
        """
        self._fork_roots[fork_session_id] = fork_root

    def get_web_router(self) -> Any | None:
        # 捕获按名解析端口而非插件实例：router 注册一次，插件可运行期重载，
        # 端口每次请求解析当前活跃实例，重载后不指向已卸载实例。
        from workflow_controller.plugins.agent_traceback.web_api import build_router

        return build_router(self._services.plugin_by_name)

    def list_checkpoint_files(
        self,
        session_id: str,
        checkpoint_id: str,
        subpath: str = "",
    ) -> list[dict[str, Any]]:
        """列某检查点的文件态直接子项（供前端预览，不 restore 整树）。"""
        cp = self._cp_store_for_session(session_id).get_sync(checkpoint_id)
        if cp is None or not cp.git_ref:
            return []
        session_obj = self._services.find_session(session_id)
        cwd = (
            getattr(session_obj.config, "cwd", "") if session_obj else str(self._ctx.work_dir)
        ) or str(self._ctx.work_dir)
        engine = self._resolve_engine(cwd)
        return engine.list_files(cp.git_ref, subpath)

    def read_checkpoint_file(
        self,
        session_id: str,
        checkpoint_id: str,
        path: str,
    ) -> dict[str, Any] | None:
        """读某检查点的某文件内容（供前端预览）。"""
        cp = self._cp_store_for_session(session_id).get_sync(checkpoint_id)
        if cp is None or not cp.git_ref:
            return None
        session_obj = self._services.find_session(session_id)
        cwd = (
            getattr(session_obj.config, "cwd", "") if session_obj else str(self._ctx.work_dir)
        ) or str(self._ctx.work_dir)
        engine = self._resolve_engine(cwd)
        return engine.read_file(cp.git_ref, path)

    def list_checkpoints(self, session_id: str) -> list[dict[str, Any]]:
        """List checkpoints for a session (file-state snapshot metadata)."""
        cps = self._cp_store_for_session(session_id).list_sync(session_id)
        return [
            {
                "checkpoint_id": cp.checkpoint_id,
                "session_id": cp.session_id,
                "iteration": cp.iteration,
                "agent_id": cp.agent_id,
                "agent_type": cp.agent_type,
                "trigger": cp.trigger,
                "git_ref": cp.git_ref,
                "event_rowid": cp.event_rowid,
                "main_part_id": cp.main_part_id,
                "parent_checkpoint_id": cp.parent_checkpoint_id,
                "created_at": cp.created_at,
                "file_count": cp.file_count,
                "byte_size": cp.byte_size,
                "status": cp.status,
                "inner_git": cp.inner_git,
            }
            for cp in cps
        ]

    async def replay_runtime(self, session_id: str) -> str:
        """Return the source conversation runtime for the replay form."""
        from workflow_controller.plugins.agent_traceback.claude_session_seed import (
            detect_replay_runtime,
        )

        session = self._services.find_session(session_id)
        if session is not None:
            config = session.config
            native_session_id = session.native_session_id or ""
            cwd = config.cwd
            runtime = config.runtime
            config_dir = (config.env_overrides or {}).get("CLAUDE_CONFIG_DIR")
        else:
            row = await self._services.load_roster_session(session_id)
            if row is None:
                return ""
            try:
                config = json.loads(row.get("config_json") or "{}")
            except (TypeError, ValueError):
                config = {}
            native_session_id = row.get("native_session_id") or ""
            cwd = config.get("cwd") or ""
            runtime = config.get("runtime")
            config_dir = (config.get("env_overrides") or {}).get("CLAUDE_CONFIG_DIR")
        return await asyncio.to_thread(
            detect_replay_runtime,
            native_session_id=native_session_id,
            cwd=cwd,
            configured_runtime=runtime,
            default_runtime=self._services.default_runtime() or "claude_sdk",
            source_config_dir=config_dir,
        )

    async def run_traceback(
        self,
        session_id: str,
        checkpoint_id: str,
        *,
        runtime: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Replay from a checkpoint — forks a new session.

        ``overrides`` 是配置式重放的 driver 层覆盖（model / system_prompt /
        loop.evaluator / loop.max_iterations / reviewer_task_goal / initial_prompt）。
        ``reviewer_model`` 不在此处消费（前端经 PUT /api/config 改全局 reviewer.model）。
        """
        from workflow_controller.plugins.agent_traceback.traceback_runner import TracebackRunner

        runner = TracebackRunner(self)
        return await runner.run(
            session_id,
            checkpoint_id,
            runtime=runtime,
            overrides=overrides,
        )

    async def import_session(
        self,
        db_path: str,
        target_cwd: str,
        *,
        log_path: str | None = None,
    ) -> dict[str, Any]:
        """从 cannbot session db 文件重建 Workflow Controller session（含检查点）。

        在 target_cwd 产出：
          - ``<target_cwd>/.workflow-controller/opencode/opencode.db``（转写的主 agent L2）
          - ``<target_cwd>/.workflow-controller/snapshots/.git``（检查点文件态仓）
          - ``<target_cwd>/.git``（若 session 内有 git 命令重放）
          - ``<target_cwd>/.workflow-controller/events.db``（含 ``checkpoints`` 表）

        同步等待到完成。``log_path`` 非空时边重建边写日志文件，供 HTTP 下载。
        """
        from workflow_controller.plugins.agent_traceback.session_reconstructor.orchestrator import (
            ReconstructOrchestrator,
        )

        runner = ReconstructOrchestrator(self)
        return await runner.import_session(
            Path(db_path),
            Path(target_cwd),
            log_path=Path(log_path) if log_path else None,
        )

    def cfg(self, key: str, default: Any = None) -> Any:
        """读插件配置 ``extra`` 段（traceback_runner / 重建编排器等协作者的公开读取面）。"""
        return self._cfg(key, default)

    def get_checkpoint(self, session_id: str, checkpoint_id: str) -> Checkpoint | None:
        """取单条检查点（重放前置校验等只读场景的公开入口）。"""
        return self._cp_store_for_session(session_id).get_sync(checkpoint_id)

    def resolve_engine(self, cwd: str) -> SnapshotEngine:
        """Per-project SnapshotEngine（公开读取面，语义与 ``_resolve_engine`` 一致）。"""
        return self._resolve_engine(cwd)

    def _reap_orphan_fork_roots(self) -> None:
        """扫描 host tempdir 里的孤儿 ``workflow-controller-tb-*`` 目录，按 TTL 清理旧的。

        回退进程崩溃/重启场景：Workflow Controller 崩溃后 in-memory ``_fork_roots`` map 丢失，
        tempdir 里堆积 ``workflow-controller-tb-*``（每个 ~2GB fork db + .git 副本）。reaper 按
        mtime + 配置 TTL 清理。Best-effort，非阻塞。

        活动集保护：绝不删仍被近期活动 session 引用的 fork_root。
        liveness = ``now - session.last_activity < TTL``——近期活动过的 fork 受保护，
        闲置超 TTL 的 ghost 可回收。活动集不可判定时本次整体跳过。详见
        ``_collect_active_fork_roots``。
        TTL 同时作 mtime 阈值与 ghost 回收阈值。
        """
        if not self._cfg("reaper_enabled", True):
            return
        try:
            max_age_days = float(self._cfg("reaper_max_age_days", _REAPER_DEFAULT_MAX_AGE_DAYS))
            if max_age_days <= 0:
                return
            active_fork_roots = self._collect_active_fork_roots(max_age_days)
            if active_fork_roots is None:
                # liveness 不可判定时宁可不回收：误删可恢复的 fork_root
                # 不可逆，孤儿目录等下次可判定的启动再清。
                logger.info("traceback: reaper skipped — pool enumeration unavailable")
                return
            tempdir = Path(tempfile.gettempdir())
            cutoff = time.time() - max_age_days * 86400
            count = 0
            for entry in tempdir.iterdir():
                if not entry.name.startswith(_REAPER_TEMPDIR_PREFIX):
                    continue
                if not entry.is_dir():
                    continue
                resolved = str(entry.resolve())
                if resolved in active_fork_roots:
                    continue
                if _reap_orphan_entry(entry, cutoff):
                    count += 1
            if count:
                logger.info(
                    "traceback: reaper removed %d orphan fork_root dirs (>%g days old)",
                    count,
                    max_age_days,
                )
        except Exception:
            logger.info(
                "traceback: orphan fork_root reaper failed (non-fatal)",
                exc_info=True,
            )

    def _collect_active_fork_roots(self, max_age_days: float) -> set[str] | None:
        """收集仍被近期活动 session 引用的 fork_root 绝对路径（reaper 须跳过）。

        liveness = ``now - session.last_activity < max_age_days``。崩溃后重启所有
        session 都被 ``load_sessions`` 置为 IDLE，无法单凭 state 区分「刚崩可恢复」与
        「长期闲置 ghost」；用每次状态变更都 UPSERT 的 ``last_activity`` 作判据。

        仅扫 pool（重启后所有持久化 session 都已 load 进 pool）——DB 行的 state 是
        崩溃前的 stale 值，会把 IDLE ghost 误判为活动。未 install（无 ctx）时本就
        无宿主，活动集为空；宿主存在但 pool 枚举端口未装配 / 枚举失败时活动集
        不可判定，返回 None，reaper 须整体跳过，不 fail-open。
        """
        active: set[str] = set()
        ctx = getattr(self, "_ctx", None)
        if ctx is None:
            return active
        if ctx.services.session_lister is None:
            return None
        cutoff = time.time() - max_age_days * 86400
        try:
            for s in ctx.services.list_sessions():
                bt = (
                    (getattr(s, "plugin_metadata", None) or {})
                    .get("agent_traceback", {})
                    .get("birth_traceback")
                )
                if not (isinstance(bt, dict) and bt.get("fork_root")):
                    continue
                # liveness：last_activity 在 TTL 窗口内才保护。取不到 / 解析失败时
                # 保守视为 live（不误删）。
                if not _fork_root_is_live(s, cutoff):
                    continue  # 闲置超 TTL → ghost，不保护
                with contextlib.suppress(OSError, ValueError):
                    active.add(str(Path(str(bt["fork_root"])).resolve()))
        except Exception:
            logger.debug("traceback: collect active fork_roots (pool) failed", exc_info=True)
            return None
        return active

    def _register_interceptors(self, ctx: Any) -> None:
        ctx.bus.intercept(EventType.SUBAGENT_START, self._on_subagent_start, priority=10)
        ctx.bus.intercept("session_delete", self._on_session_delete, priority=10)

    def _register_observers(self, ctx: Any) -> None:
        ctx.bus.observe("session_complete", self._on_session_complete)

    # ── web 路由 ──

    def _detach_bus_handlers(self) -> None:
        """注销本插件注册的 bus handler，避免 reload 后旧实例的 handler 仍触发。"""
        ctx = getattr(self, "_ctx", None)
        if ctx is None:
            return
        try:
            ctx.bus.detach_interceptor(EventType.SUBAGENT_START, self._on_subagent_start)
            ctx.bus.detach_interceptor("session_delete", self._on_session_delete)
            ctx.bus.detach_observer("session_complete", self._on_session_complete)
        except Exception:
            logger.debug("traceback: detach bus handlers failed (non-fatal)", exc_info=True)

    async def _on_subagent_start(self, event: Event) -> InterceptResult | None:
        """subagent start 时打文件态快照（await 阻塞 worker 保证文件态一致）。

        仅主 agent 的直接子代打检查点（``parent_agent_id`` 为空）：检查点
        语义 = 主 agent 派发边界，重放截断点是其主 session 的 task part
        （``find_main_part_id_for_subagent``）。孙 agent（嵌套派发，
        ``parent_agent_id`` 非空）的派发 part 在子会话里，主 session 无锚，
        打出的检查点不可重放——不打。
        """
        if not self._is_enabled():
            return None
        if str(event.data.get("parent_agent_id") or ""):
            logger.debug(
                "traceback: skip checkpoint for nested subagent %s (no main dispatch anchor)",
                str(event.data.get("agent_id", ""))[:12],
            )
            return None
        await self._snapshot_subagent_start(
            mig_session_id=event.session_id,
            agent_id=event.data.get("agent_id", ""),
            agent_type=event.data.get("agent_type", ""),
        )
        return None

    # ── 检查点快照 ──

    async def _on_session_complete(self, event: Event) -> None:
        """session 完成时打终态检查点。"""
        if not self._is_enabled():
            return
        await self._snapshot_session_complete(event.session_id)

    async def _on_session_delete(self, event: Event) -> InterceptResult | None:
        """session 删除时清本插件 own 的 checkpoints 行 + fork_root 目录（best-effort，不抛）。

        db_path 经插件服务端口解析（session 对象此时可能已不在 pool）。
        fork_root 优先从 ``session_delete`` 事件 data 携带的 ``birth_traceback.fork_root``
        取出 rmtree——``SessionAPI.delete`` 在 ``pool.remove`` 前已把 birth_traceback
        透传进事件，即使 in-memory ``_fork_roots`` 被清空也能立即清理。

        回退链（事件 data 无 birth_traceback 时）：
        1. in-memory ``_fork_roots`` map（同进程内重放登记的）；
        2. ``_read_birth_traceback`` 读 pool / 持久化行的 plugin_metadata
           （重启场景回退）。

        崩溃残留由启动期 reaper 兜底。
        """
        session_id = event.session_id
        data = event.data or {}
        try:
            self._cp_store_for_session(session_id).delete_for_session(session_id)
        except Exception:
            logger.warning(
                "traceback: on_session_delete checkpoint cleanup failed for %s",
                session_id[:12],
                exc_info=True,
            )
        try:
            # 无条件 pop，避免事件 data 已带 fork_root 时被 ``or`` 短路，
            # 让已删除 session 永久残留在生命周期映射中。
            birth = data.get("birth_traceback")
            if not isinstance(birth, dict):
                birth = {}
            memory_root = self._fork_roots.pop(session_id, None)
            fork_root = birth.get("fork_root") or memory_root
            if not fork_root and not birth:
                birth = await self._read_birth_traceback(session_id) or {}
                fork_root = birth.get("fork_root")
            if fork_root:
                await self._rmtree_fork_root(session_id, str(fork_root), birth)
            # 重建 session 的对称清理：birth_traceback.reconstructed_from 标记（无
            # fork_root）→ 清 Workflow Controller 产物子集 + 同步 recent_dirs.json。
            # clean_reconstruct 由前端经 SessionAPI.delete 透传（"workflow_controller"|"all"）：
            # - None = 不清磁盘；
            # - "workflow_controller" = 清 Workflow Controller 产物子集
            #   （.workflow-controller/.opencode + .git 若 created_git_dir）；
            # - "all" = rmtree 整个 target_cwd（前端须二次确认）。
            # recent_dirs 同步无论是否清磁盘都做——侧边栏不留幽灵是基本体验。
            if birth and birth.get("reconstructed_from") and not birth.get("fork_root"):
                await self._cleanup_reconstruct(session_id, birth, data.get("clean_reconstruct"))
        except Exception:
            logger.warning(
                "traceback: on_session_delete fork_root cleanup failed for %s",
                session_id[:12],
                exc_info=True,
            )

    def _clean_reconstruct_artifacts(
        self,
        session_id: str,
        target_cwd: str,
        birth: dict[str, Any],
        *,
        aggressive: bool = False,
    ) -> list[str]:
        """清理重建 session 在 ``target_cwd`` 产出的 Workflow Controller 产物（best-effort，不抛）。

        ``target_cwd`` 是用户选定的真实目录（非 tempdir），不能无脑 rmtree——
        用户可能在重建后往里加了别的文件。清理策略：

        - 默认（``aggressive=False``）：只清 Workflow Controller 自己产出的子集——
          ``<target_cwd>/.workflow-controller/`` + ``<target_cwd>/.opencode/``（仅当
          ``created_opencode_dir`` 为真）+ ``<target_cwd>/.git/``（仅当
          ``created_git_dir`` 为真；旧 session 无标记时保守不清）。
        - 激进（``aggressive=True``）：rmtree 整个 ``target_cwd``，前端须二次确认才传此模式。

        路径安全：拒绝根目录 / 相对路径 / 过浅路径——metadata 属不可信输入。
        """
        removed: list[str] = []
        base = Path(target_cwd).expanduser()
        try:
            if not base.is_absolute() or not base.is_dir():
                return removed
            if aggressive:
                return _reconstruct_aggressive_clean(session_id, target_cwd, base)
            # 默认：只清 Workflow Controller 产物子集
            return _reconstruct_subset_clean(session_id, base, birth)
        except Exception:
            logger.warning(
                "traceback: _clean_reconstruct_artifacts failed for %s",
                session_id[:12],
                exc_info=True,
            )
        return removed

    def _remove_cwd_from_recent_dirs(self, target_cwd: str) -> None:
        """从最近项目清单移除 cwd（best-effort，失败只记日志）。"""
        try:
            self._services.remove_recent_dir(target_cwd)
        except Exception:
            logger.warning(
                "traceback: remove_recent_dir for %s failed (non-fatal)",
                target_cwd,
                exc_info=True,
            )

    # ── 检查点文件态预览（重放前浏览文件态决定是否/从哪重放） ──────────

    async def _read_birth_traceback(self, session_id: str) -> dict[str, Any] | None:
        """回退路径：pool 未命中时读持久化行的 birth_traceback。

        事件 data 缺 ``birth_traceback`` 时的回退读取。持久化行的
        plugin_metadata 是 JSON 串，解析失败按未命中处理。
        """
        try:
            session = self._services.find_session(session_id)
            if session is not None:
                pm = getattr(session, "plugin_metadata", None) or {}
                bt = (pm.get("agent_traceback") or {}).get("birth_traceback")
                if isinstance(bt, dict):
                    return bt
            row = await self._services.load_roster_session(session_id)
        except Exception:
            logger.debug(
                "traceback: read birth_traceback failed for %s",
                session_id[:12],
                exc_info=True,
            )
            return None
        if not row:
            return None
        pm_raw = row.get("plugin_metadata")
        if isinstance(pm_raw, str):
            try:
                pm_raw = json.loads(pm_raw)
            except (json.JSONDecodeError, TypeError):
                return None
        if not isinstance(pm_raw, dict):
            return None
        bt = (pm_raw.get("agent_traceback") or {}).get("birth_traceback")
        return bt if isinstance(bt, dict) else None

    def _is_enabled(self) -> bool:
        """插件是否启用。由 config.yaml 顶层 enabled 控制。"""
        return bool(self.config.enabled)

    def _resolve_engine(self, cwd: str) -> SnapshotEngine:
        """Per-project SnapshotEngine（按 cwd 缓存，与 EventStoreRouter 对称）。"""
        key = (
            str(Path(cwd).expanduser().resolve())
            if cwd and cwd != "."
            else str(self._ctx.work_dir.expanduser().resolve())
        )
        eng = self._engines.get(key)
        if eng is None:
            ignore = list(self._cfg("ignore", []) or []) or DEFAULT_IGNORE_PATTERNS
            snap_dir = Path(key) / (self._cfg("snapshot_dir", ".workflow-controller/snapshots"))
            eng = SnapshotEngine(
                project_dir=Path(key),
                snapshot_dir=snap_dir,
                ignore_patterns=ignore,
                snapshot_timeout_seconds=float(self._cfg("snapshot_timeout_seconds", 2.0)),
                enabled=True,  # _is_enabled() is the gatekeeper
                max_checkpoints=int(self._cfg("max_checkpoints", 200)),
                snapshot_git_state=bool(self._cfg("snapshot_git_state", True)),
                snapshot_git_state_max_mb=int(self._cfg("snapshot_git_state_max_mb", 200)),
            )
            self._engines[key] = eng
        return eng

    def _cp_store_for_session(self, session_id: str) -> CheckpointStore:
        """Per-project CheckpointStore（按 db 路径缓存，与 events 同库）。

        核心不再含 checkpoint 概念：插件经能力端口解析 per-project events.db
        路径后自己开连接操作 checkpoints 表（DDL+CRUD 全在插件）。
        """
        db_path = self._services.events_db_path(session_id)
        key = str(db_path)
        store = self._cp_stores.get(key)
        if store is None:
            store = CheckpointStore(db_path)
            self._cp_stores[key] = store
        return store

    async def _snapshot_subagent_start(
        self,
        *,
        mig_session_id: str,
        agent_id: str,
        agent_type: str,
    ) -> None:
        """打一次检查点快照并写 checkpoints 表（best-effort，不抛）。

        在 subagent start 时刻触发：捕获 subagent 开始工作前的文件态，
        让用户能从「subagent X 开始前」的检查点重放。
        """
        prepared = self._prepare_checkpoint_context(mig_session_id)
        if prepared is None:
            return
        engine, cp_store, _existing, checkpoint_id, parent_cp_id, iteration = prepared
        await self._record_checkpoint_snapshot(
            engine=engine,
            cp_store=cp_store,
            checkpoint_id=checkpoint_id,
            parent_cp_id=parent_cp_id,
            iteration=iteration,
            session_id=mig_session_id,
            agent_id=agent_id,
            agent_type=agent_type,
            trigger="subagent_start",
            snapshot_fail_log="traceback: snapshot failed for %s/%s — skipped",
            snapshot_fail_args=(mig_session_id[:12], checkpoint_id),
            stored_log="traceback: checkpoint %s stored (status=%s, ref=%s, files=%d) for %s",
            stored_log_args=(mig_session_id[:12],),
        )

    async def _store_checkpoint(
        self,
        cp_store: CheckpointStore,
        *,
        checkpoint_id: str,
        session_id: str,
        iteration: int,
        agent_id: str,
        agent_type: str,
        trigger: str,
        result: SnapshotResult,
        parent_cp_id: str | None,
    ) -> None:
        """构造并落一个检查点（subagent_start 与 session_complete 两轨共用）。"""
        cp = Checkpoint(
            checkpoint_id=checkpoint_id,
            session_id=session_id,
            iteration=iteration,
            agent_id=agent_id,
            agent_type=agent_type,
            trigger=trigger,
            git_ref=result.git_ref,
            event_rowid=None,
            main_part_id=None,
            parent_checkpoint_id=parent_cp_id,
            created_at=datetime.now(tz=UTC).isoformat(),
            file_count=result.file_count,
            byte_size=result.byte_size,
            status=result.status,
            inner_git=result.inner_git,
        )
        try:
            await cp_store.store(cp)
        except Exception:
            logger.exception("traceback: store_checkpoint failed for %s", checkpoint_id)

    async def _snapshot_session_complete(self, session_id: str) -> None:
        """最终状态检查点：session 跑完时打一次文件态快照（best-effort，不抛）。

        让用户能「跳回原始 workflow 跑完后的最终文件态」重放——区别于 subagent_start
        检查点（中途），这是终态。trigger=session_complete / agent_id=空（无 subagent
        可 re-call，重放走 final-state 冷启路径）。
        """
        prepared = self._prepare_checkpoint_context(session_id)
        if prepared is None:
            return
        engine, cp_store, existing, checkpoint_id, parent_cp_id, iteration = prepared
        # 跳过已有 session_complete 检查点（幂等：完成回调可能多次触发）
        if any(cp.trigger == "session_complete" for cp in existing):
            return
        await self._record_checkpoint_snapshot(
            engine=engine,
            cp_store=cp_store,
            checkpoint_id=checkpoint_id,
            parent_cp_id=parent_cp_id,
            iteration=iteration,
            session_id=session_id,
            agent_id="",  # 终态检查点无 subagent
            agent_type="main",
            trigger="session_complete",
            snapshot_fail_log="traceback: session-complete snapshot failed for %s — skipped",
            snapshot_fail_args=(session_id[:12],),
            stored_log="traceback: final-state checkpoint %s stored (status=%s, ref=%s, files=%d)",
            stored_log_args=(),
        )

    def _prepare_checkpoint_context(self, session_id: str) -> _CheckpointContext | None:
        """解析 engine / cp_store / 检查点链位置 / iteration（session 不在 pool 返回 None）。"""
        session_obj = self._services.find_session(session_id)
        if session_obj is None:
            return None
        cwd = getattr(session_obj.config, "cwd", "") or str(self._ctx.work_dir)
        engine = self._resolve_engine(cwd)
        # 用 session.current_iteration（真实），不用事件里的 0。
        iteration = int(getattr(session_obj, "current_iteration", 0) or 0)
        # checkpoint_id 是 checkpoints 表 PRIMARY KEY，须全局唯一 → 带 session 前缀；
        # parent 指向上一条（检查点链）。
        cp_store = self._cp_store_for_session(session_id)
        existing = cp_store.list_sync(session_id)
        next_n = len(existing) + 1
        checkpoint_id = f"cp_{session_id[:8]}_{next_n:03d}"
        parent_cp_id = existing[-1].checkpoint_id if existing else None
        return _CheckpointContext(
            engine=engine,
            cp_store=cp_store,
            existing=existing,
            checkpoint_id=checkpoint_id,
            parent_cp_id=parent_cp_id,
            iteration=iteration,
        )

    async def _record_checkpoint_snapshot(
        self,
        *,
        engine: SnapshotEngine,
        cp_store: CheckpointStore,
        checkpoint_id: str,
        parent_cp_id: str | None,
        iteration: int,
        session_id: str,
        agent_id: str,
        agent_type: str,
        trigger: str,
        snapshot_fail_log: str,
        snapshot_fail_args: tuple[str, ...],
        stored_log: str,
        stored_log_args: tuple[str, ...],
    ) -> None:
        """engine.snapshot + 检查点落库 + 落库日志的共用轨（start / complete 两轨共用）。"""
        try:
            result = await engine.snapshot(
                session_id=session_id,
                checkpoint_id=checkpoint_id,
                agent_id=agent_id,
                agent_type=agent_type,
                iteration=iteration,
                trigger=trigger,
                event_rowid=None,  # start/complete 时刻尚无 stop 事件，event_rowid 留空
                parent_checkpoint_id=parent_cp_id,
                main_part_id=None,  # 重放时由 find_main_part_id_for_subagent 查
            )
        except Exception:  # engine 内已 best-effort，这里再拦一层
            logger.exception(snapshot_fail_log, *snapshot_fail_args)
            return
        await self._store_checkpoint(
            cp_store,
            checkpoint_id=checkpoint_id,
            session_id=session_id,
            iteration=iteration,
            agent_id=agent_id,
            agent_type=agent_type,
            trigger=trigger,
            result=result,
            parent_cp_id=parent_cp_id,
        )
        log_args = (checkpoint_id, result.status, (result.git_ref or "")[:8], result.file_count)
        logger.info(stored_log, *log_args, *stored_log_args)

    async def _rmtree_fork_root(
        self,
        session_id: str,
        fork_root: str,
        birth: dict[str, Any],
    ) -> None:
        """rmtree fork_root（路径安全校验后）并同步清理 recent_dirs。"""
        # 路径安全：metadata 属不可信输入，rmtree 前必须确认 fork_root
        # 位于系统 tempdir 且名称符合 workflow-controller-tb-* 前缀。
        if self._is_safe_fork_root(fork_root):
            await asyncio.to_thread(
                shutil.rmtree,
                fork_root,
                ignore_errors=True,
            )
            logger.info(
                "traceback: cleaned fork_root %s for deleted session %s",
                fork_root,
                session_id[:12],
            )
        else:
            logger.warning(
                "traceback: refusing to rmtree fork_root %s — outside tempdir or wrong prefix",
                fork_root,
            )
        # fork tempdir cwd 若误入 recent_dirs.json，一并移除（即时清理，不等下次 load 再处理）。
        fork_cwd = (birth.get("fork_cwd") if birth else None) or str(Path(fork_root) / "cwd")
        await asyncio.to_thread(
            self._remove_cwd_from_recent_dirs,
            str(fork_cwd),
        )

    async def _cleanup_reconstruct(
        self,
        session_id: str,
        birth: dict[str, Any],
        clean_mode: Any,
    ) -> None:
        """重建 session 的清理：按 clean_mode 清产物 / 整目录 + 同步 recent_dirs。"""
        target_cwd = birth.get("fork_cwd") or birth.get("target_cwd")
        if not target_cwd:
            return
        if clean_mode is not None:
            await asyncio.to_thread(
                self._clean_reconstruct_artifacts,
                session_id,
                str(target_cwd),
                birth,
                aggressive=(clean_mode == "all"),
            )
        # 同步 recent_dirs.json：删除重建 session 后侧边栏不应留幽灵条目。
        await asyncio.to_thread(
            self._remove_cwd_from_recent_dirs,
            str(target_cwd),
        )
