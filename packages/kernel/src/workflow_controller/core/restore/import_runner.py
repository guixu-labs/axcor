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

"""cannbot session db 导入编排器：外部录制库重建为本仓会话（自插件迁移）。

四步编排：读源库构建统一时间线 → 转写主 agent 对话为 opencode.db → 重放
文件态并在 task 边界打快照仓提交 → 推导历史检查点行；随后经注入端口注册
会话（池创建 + 持久化 + 事件落库）。检查点写旧 ``checkpoints`` 表（与读取
面同库），会话为「已跑完」态——用户从检查点列表重放时才起新会话。
"""

from __future__ import annotations

__all__ = [
    "IMPORT_LOG_BASENAME",
    "ImportRunnerPorts",
    "SessionImportRunner",
    "import_log_path_for",
]

import asyncio
import contextlib
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple, TextIO
from uuid import uuid4

from workflow_controller.config.settings import SessionConfig
from workflow_controller.core.checkpoint_compat import (
    LegacyCheckpoint,
    store_legacy_checkpoint,
)
from workflow_controller.core.restore.cannbot_reader import (
    CannbotDbReader,
    SessionMeta,
    TimelineEntry,
    extract_subagent_sid_from_args,
    extract_subagent_type_from_args,
    iso_to_ms,
    lookup_bridge_sub,
)
from workflow_controller.core.restore.checkpoint_deriver import CheckpointDeriver
from workflow_controller.core.restore.file_rebuilder import FileStateRebuilder
from workflow_controller.core.restore.transcriber import L2Transcriber
from workflow_controller.session.events import Event, EventType

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from workflow_controller.core.restore.file_rebuilder import CheckpointFileState

logger = logging.getLogger(__name__)

# 导入日志固定落点：<target_cwd>/.workflow-controller/reconstruct/reconstruction.log
IMPORT_LOG_BASENAME = "reconstruction.log"

# 同一 target_cwd 的导入互斥：已在跑时直接报错（不排队），防并发写坏产物
_import_locks: dict[str, asyncio.Lock] = {}

# 可导入的 session db framework。claude-code-proxy 的工具名/时间戳方言经
# 读取器归一化后与 opencode 同构，重建链路零分叉。
_SUPPORTED_FRAMEWORKS: frozenset[str] = frozenset({"opencode", "claude-code-proxy"})


def import_log_path_for(target_cwd: Path) -> Path:
    """导入日志路径（与插件落点逐字一致，日志查询面共用）。"""
    return target_cwd / ".workflow-controller" / "reconstruct" / IMPORT_LOG_BASENAME


@dataclass(frozen=True)
class ImportRunnerPorts:
    """导入编排的注入端口（会话创建/持久化/事件与检查点库定位）。"""

    create_session: Callable[[SessionConfig], Awaitable[Any]]
    persist_session: Callable[[Any], Awaitable[None]]
    store_events: Callable[[list[Any]], Awaitable[None]]
    checkpoint_db_path: Callable[[], Path]
    # source opencode.db（schema 模板）解析；None/空 → 转写器回落宿主默认
    source_opencode_db: Callable[[], Path | None] | None = None


class SessionImportRunner:
    """从 cannbot session db 文件重建本仓会话（含历史检查点）。"""

    def __init__(
        self,
        ports: ImportRunnerPorts,
        *,
        ignore_patterns: list[str] | None = None,
        bash_replay_mode: str = "all",
        git_hash_fidelity: bool = True,
        bash_timeout_seconds: float | None = 30.0,
        snapshot_git_state: bool = True,
        snapshot_git_state_max_mb: int = 200,
        max_replay_tool_calls: int | None = None,
    ) -> None:
        """装配端口与重建选项（默认值对齐插件 reconstruct 段缺省）。"""
        self._ports = ports
        self._ignore_patterns = ignore_patterns
        self._bash_replay_mode = bash_replay_mode
        self._git_hash_fidelity = git_hash_fidelity
        self._bash_timeout_seconds = bash_timeout_seconds
        self._snapshot_git_state = snapshot_git_state
        self._snapshot_git_state_max_mb = snapshot_git_state_max_mb
        self._max_replay_tool_calls = max_replay_tool_calls

    async def import_session(
        self,
        db_path: Path,
        target_cwd: Path,
        *,
        log_path: Path | None = None,
    ) -> dict[str, Any]:
        """入口：从 cannbot db 文件重建会话（同步等待到完成）。

        ``log_path`` 非空时边重建边写日志文件（行缓冲，覆盖写）。同一
        target_cwd 已有导入在跑时立即返回错误（不排队）。
        """
        target_key = str(Path(target_cwd).expanduser().resolve())
        lock = _import_locks.setdefault(target_key, asyncio.Lock())
        if lock.locked():
            return _import_failure(f"import already running for target_cwd: {target_key}")
        async with lock:
            try:
                return await self._do_import(db_path, target_cwd, log_path=log_path)
            except Exception as exc:
                logger.exception("导入重建失败：db=%s cwd=%s", db_path, target_cwd, exc_info=True)
                return _import_failure(f"{type(exc).__name__}: {exc}")

    async def _do_import(
        self,
        db_path: Path,
        target_cwd: Path,
        *,
        log_path: Path | None = None,
    ) -> dict[str, Any]:
        """实际编排四步 + 注册（每阶段边界写一行日志）。"""
        target_cwd = Path(target_cwd).expanduser().resolve()
        target_cwd.mkdir(parents=True, exist_ok=True)
        dir_flags = _capture_dir_flags(target_cwd)
        log = _ImportLogger(log_path)
        try:
            source = _read_source(db_path, log)
            if isinstance(source, dict):
                return source
            transcribe_result = await self._run_transcribe(source, target_cwd, log)
            file_states = await self._run_replay(source, target_cwd, log)
            checkpoints = _derive_checkpoints(source, transcribe_result, file_states, log)
            session = await self._register_session(
                source, target_cwd, db_path, checkpoints, dir_flags, log
            )
            return await self._finalize_import(
                session, source, db_path, target_cwd, checkpoints, transcribe_result, log
            )
        finally:
            # 仅未预期异常（未写出任何终态 phase 行）才补写失败行；
            # 显式失败路径已在失败点写过 phase=failed，此处不重复。
            if not log.finalized:
                with contextlib.suppress(Exception):
                    log.write("phase=failed error=unexpected exception")
            log.close()

    async def _run_transcribe(
        self,
        source: _ReconstructSource,
        target_cwd: Path,
        log: _ImportLogger,
    ) -> Any:
        """对话态重建（phase=transcribe）：Turn/ToolCall → opencode.db。"""
        log.write("phase=transcribe")
        source_opencode_db = self._resolve_source_opencode_db()
        try:
            src_mb = source_opencode_db.stat().st_size / (1024 * 1024)
        except OSError:
            src_mb = -1
        log.write(
            f"transcribe: source_opencode_db={source_opencode_db} "
            f"size_mb={src_mb:.0f} (schema-only init)"
        )
        transcriber = L2Transcriber(
            target_cwd=target_cwd,
            origin_cwd=source.origin_cwd,
            source_opencode_db=source_opencode_db,
        )
        # 同步 sqlite I/O，丢线程池避免阻塞事件循环
        t0 = time.monotonic()
        transcribe_result = await asyncio.get_running_loop().run_in_executor(
            None,
            transcriber.transcribe,
            source.timeline,
            source.session_meta,
            source.sdk_session_id,
            source.bridges,
        )
        log.write(
            f"transcribe: done messages={transcribe_result.message_count} "
            f"parts={transcribe_result.part_count} dur={time.monotonic() - t0:.1f}s"
        )
        return transcribe_result

    async def _run_replay(
        self,
        source: _ReconstructSource,
        target_cwd: Path,
        log: _ImportLogger,
    ) -> list[CheckpointFileState]:
        """文件态重建：重放 tool calls + 快照仓（每步经日志回调留痕）。"""
        rebuilder = FileStateRebuilder(
            target_cwd=target_cwd,
            origin_cwd=source.origin_cwd,
            ignore_patterns=self._ignore_patterns,
            bash_replay_mode=self._bash_replay_mode,
            git_hash_fidelity=self._git_hash_fidelity,
            bash_timeout_seconds=self._bash_timeout_seconds,
            log_cb=log.write,
            snapshot_git_state=self._snapshot_git_state,
            snapshot_git_state_max_mb=self._snapshot_git_state_max_mb,
            max_replay_tool_calls=self._max_replay_tool_calls,
        )
        # replay() 重阻塞，丢线程池跑保持事件循环响应。
        return await asyncio.get_running_loop().run_in_executor(
            None, rebuilder.replay, source.timeline
        )

    async def _register_session(
        self,
        source: _ReconstructSource,
        target_cwd: Path,
        db_path: Path,
        checkpoints: list[LegacyCheckpoint],
        dir_flags: tuple[bool, bool],
        log: _ImportLogger,
    ) -> Any:
        """注册会话（phase=register）：池创建 + 已跑完态 + 溯源 + 事件落库。"""
        log.write("phase=register")
        config = _build_session_config(
            session_meta=source.session_meta,
            target_cwd=target_cwd,
            sdk_session_id=source.sdk_session_id,
            checkpoints=checkpoints,
        )
        session = await self._ports.create_session(config)
        # 供检查点重放查转写出的 opencode.db；已跑完态不进运行队列
        session.sdk_session_id = source.sdk_session_id
        session.loop_status = "yes"
        git_existed_before, opencode_existed_before = dir_flags
        session.plugin_metadata["agent_traceback"] = _birth_traceback_metadata(
            db_path, source, target_cwd, checkpoints, git_existed_before, opencode_existed_before
        )
        # create 在设置 sdk_session_id / loop_status / metadata 前就写了会话行——
        # 此处重新持久化让溯源落库，否则重启后导入会话的清理面失效。
        await self._ports.persist_session(session)
        await self._ports.store_events(
            [_imported_event(session.id, config, db_path, source.sdk_session_id, checkpoints)]
        )
        return session

    async def _finalize_import(
        self,
        session: Any,
        source: _ReconstructSource,
        db_path: Path,
        target_cwd: Path,
        checkpoints: list[LegacyCheckpoint],
        transcribe_result: Any,
        log: _ImportLogger,
    ) -> dict[str, Any]:
        """替换临时会话标识写检查点 + 合成历史事件 + 成功收尾。"""
        real_checkpoints = [_replace_session_id(cp, session.id) for cp in checkpoints]
        # 检查点与读路径同库：落检查点库（不落 target_cwd，重建目录 ≠ 库目录会查不到）
        checkpoint_db = self._ports.checkpoint_db_path()
        for cp in real_checkpoints:
            await asyncio.to_thread(store_legacy_checkpoint, checkpoint_db, cp)
        await _synthesize_historical_events(
            store_events=self._ports.store_events,
            session_id=session.id,
            timeline=source.timeline,
            bridges=source.bridges,
        )
        logger.info(
            "导入会话 %s（sdk=%s）自 %s → cwd=%s，检查点 %d 个（%d 消息 + %d part）",
            session.id,
            source.sdk_session_id[:12],
            db_path,
            target_cwd,
            len(real_checkpoints),
            transcribe_result.message_count,
            transcribe_result.part_count,
        )
        log.finish(
            f"phase=complete session_id={session.id[:12]} checkpoints={len(real_checkpoints)}"
        )
        return _import_success(
            session.id, source.sdk_session_id, real_checkpoints, target_cwd, transcribe_result
        )

    def _resolve_source_opencode_db(self) -> Path | None:
        """解析 source opencode.db（schema 模板）；端口未装配返回 None 走宿主默认。"""
        if self._ports.source_opencode_db is None:
            return None
        return self._ports.source_opencode_db()


class _ReconstructSource(NamedTuple):
    """db 读取阶段的产物（session 元数据 + 时间线 + bridge + 推断的 origin）。"""

    session_meta: SessionMeta
    timeline: list[TimelineEntry]
    bridges: list[dict[str, Any]]
    sdk_session_id: str
    origin_cwd: str


def _read_source(db_path: Path, log: _ImportLogger) -> _ReconstructSource | dict[str, Any]:
    """phase=db_read：校验 + 读 session/timeline/bridges + 推断 origin_cwd。

    失败时返回失败 dict（且已写出终态 ``phase=failed`` 日志行）。
    """
    log.write("phase=db_read")
    with CannbotDbReader(Path(db_path)) as reader:
        if not reader.validate():
            error = f"not a cannbot session db (missing required tables): {db_path}"
            log.finish(f"phase=failed error={error}")
            return _import_failure(error)
        session_meta = reader.read_session()
        if session_meta.framework not in _SUPPORTED_FRAMEWORKS:
            error = (
                f"unsupported framework: {session_meta.framework} "
                f"(supported: {', '.join(sorted(_SUPPORTED_FRAMEWORKS))})"
            )
            log.finish(f"phase=failed error={error}")
            return _import_failure(error)
        timeline = reader.read_timeline()
        bridges = reader.read_bridges()
    # cpx 导出的 Session.directory 常为空：从文件工具的目标路径推断
    # 项目根，否则文件重放拿不到可重写的 origin、路径前缀替换失效
    origin_cwd = session_meta.origin_cwd or _infer_origin_cwd(timeline)
    sdk_session_id = session_meta.sdk_session_id
    if not sdk_session_id:
        error = "db has no sdk_session_id (Session.taskId is empty)"
        log.finish(f"phase=failed error={error}")
        return _import_failure(error)
    return _ReconstructSource(session_meta, timeline, bridges, sdk_session_id, origin_cwd)


def _derive_checkpoints(
    source: _ReconstructSource,
    transcribe_result: Any,
    file_states: list[CheckpointFileState],
    log: _ImportLogger,
) -> list[LegacyCheckpoint]:
    """检查点推导（phase=checkpoint_derive）：session_id 先用占位，最后回填。"""
    log.write("phase=checkpoint_derive")
    temp_session_id = f"rc_temp_{source.sdk_session_id[:12]}"
    return CheckpointDeriver.derive(
        timeline=source.timeline,
        file_states=file_states,
        task_part_ids=transcribe_result.task_part_ids,
        session_id=temp_session_id,
        sdk_session_id=source.sdk_session_id,
        bridges=source.bridges,
    )


def _infer_origin_cwd(timeline: list[TimelineEntry]) -> str:
    """从时间线推断录制时的项目根（``Session.directory`` 为空时的回退）。

    取全部 write/edit/multi_edit 绝对目标路径父目录的最长公共目录——
    cpx 导出不记录 directory，但文件工具的目标路径都落在项目根下。
    无绝对路径、公共目录退化到文件系统根、或公共目录恰为某条目标
    路径的父目录（单文件/全部同目录，无法与项目根下的子目录区分）
    时返回空串（调用方维持空原 origin 的 best-effort 行为）。
    """
    dirs: list[str] = []
    for entry in timeline:
        tc = entry.tool_call
        if entry.kind != "tool_call" or tc is None:
            continue
        if str(tc.get("toolName") or "") not in ("write", "edit", "multi_edit"):
            continue
        try:
            args = json.loads(str(tc.get("argsJson") or "") or "null")
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(args, dict):
            continue
        fp = str(args.get("filePath") or args.get("file_path") or "")
        if fp.startswith("/"):
            dirs.append(str(Path(fp).parent))
    if not dirs:
        return ""
    try:
        common = os.path.commonpath(dirs)
    except ValueError:
        return ""
    # 过深的 origin 会让兄弟目录的绝对路径免于重写、或把文件拍平到
    # target 根，宁保守回退空串
    if len(dirs) == 1 or common in dirs:
        return ""
    return common if common not in ("", "/") else ""


def _read_project_yaml(target_cwd: Path) -> dict[str, Any] | None:
    """从 target_cwd/workflow-controller.yaml 读项目配置（best-effort）。

    yaml 不存在/解析失败 → 返回 None（不阻塞重建）。
    """
    yaml_path = target_cwd / "workflow-controller.yaml"
    if not yaml_path.is_file():
        return None
    try:
        import yaml

        data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        logger.debug("重建项目 yaml 读取失败（不阻塞）：%s", target_cwd, exc_info=True)
        return None


def _build_loop_config(raw: Any) -> Any:
    """从 yaml dict 构造 LoopConfig（best-effort）。"""
    if not isinstance(raw, dict):
        return None
    try:
        from workflow_controller.config.settings import LoopConfig

        kwargs: dict[str, Any] = {}
        if "max_iterations" in raw:
            kwargs["max_iterations"] = int(raw["max_iterations"])
        if "evaluator" in raw:
            ev = raw["evaluator"]
            kwargs["evaluator"] = str(ev) if ev else None
        if "prompt_template" in raw:
            kwargs["prompt_template"] = str(raw["prompt_template"])
        return LoopConfig(**kwargs)
    except Exception:
        logger.debug("重建 LoopConfig 构造失败（不阻塞）", exc_info=True)
        return None


def _derive_loop_from_checkpoints(checkpoints: list[LegacyCheckpoint] | None) -> Any:
    """从 checkpoints 推导 LoopConfig：max_iterations 取原始会话实际总轮数。

    最大 cp.iteration = 主 agent 最后一次 task 派发的序号，即原始 session
    实际跑完的总轮数。无检查点时退到默认值 10。
    """
    from workflow_controller.config.settings import LoopConfig

    max_iter = 10
    if checkpoints:
        max_iter = max(max(cp.iteration for cp in checkpoints), 10)
    return LoopConfig(max_iterations=max_iter)


def _build_session_config(
    *,
    session_meta: SessionMeta,
    target_cwd: Path,
    sdk_session_id: str,
    checkpoints: list[LegacyCheckpoint] | None = None,
) -> SessionConfig:
    """构造导入会话配置：worker + opencode + XDG 指向重建目录 + yaml 优先。"""
    env_overrides = {"XDG_DATA_HOME": str(target_cwd / ".workflow-controller")}
    system_prompt = ""
    stages: list[dict[str, Any]] = []
    initial_prompt = session_meta.initial_prompt or ""

    yaml_cfg = _read_project_yaml(target_cwd)
    loop_cfg = None
    if yaml_cfg:
        if yaml_cfg.get("system_prompt"):
            system_prompt = str(yaml_cfg["system_prompt"])
        if yaml_cfg.get("stages"):
            stages = list(yaml_cfg["stages"])
        if yaml_cfg.get("loop"):
            loop_cfg = _build_loop_config(yaml_cfg["loop"])
        if yaml_cfg.get("starting_prompt"):
            initial_prompt = str(yaml_cfg["starting_prompt"])
    # cannbot db 不存 LoopConfig，yaml 未配时从 checkpoints 推导：
    # 最大 cp.iteration = 原始 session 实际总轮数，避免重放首轮即停
    if loop_cfg is None:
        loop_cfg = _derive_loop_from_checkpoints(checkpoints)

    return SessionConfig(
        name=f"reconstructed-{sdk_session_id[:8]}",
        role="worker",
        cwd=str(target_cwd),
        runtime="opencode",
        initial_prompt=initial_prompt,
        model=session_meta.model or None,
        env_overrides=env_overrides,
        system_prompt=system_prompt,
        stages=stages,
        loop=loop_cfg,
    )


def _replace_session_id(cp: LegacyCheckpoint, new_session_id: str) -> LegacyCheckpoint:
    """替换冻结检查点行的 session_id（真实会话标识回填）。"""
    from dataclasses import replace

    return replace(cp, session_id=new_session_id)


def _capture_dir_flags(target_cwd: Path) -> tuple[bool, bool]:
    """记录重建前 ``.git`` / ``.opencode`` 是否已存在。

    用于溯源的 created_git_dir / created_opencode_dir 标记（删除清理据此
    判定目录是否由重建产生 → 是否可清）。target_cwd 约定为空目录；若用户
    塞了已有 .git，重建不应声称「我建的」。
    """
    return (target_cwd / ".git").exists(), (target_cwd / ".opencode").exists()


def _import_failure(error: str) -> dict[str, Any]:
    """导入失败时返回给调用方的统一结构。"""
    return {"ok": False, "session_id": "", "checkpoints": [], "error": error}


def _import_success(
    session_id: str,
    sdk_session_id: str,
    checkpoints: list[LegacyCheckpoint],
    target_cwd: Path,
    transcribe_result: Any,
) -> dict[str, Any]:
    """导入成功时返回给调用方的结构（含检查点摘要列表）。"""
    return {
        "ok": True,
        "session_id": session_id,
        "sdk_session_id": sdk_session_id,
        "checkpoints": [
            {
                "checkpoint_id": cp.checkpoint_id,
                "trigger": cp.trigger,
                "agent_id": cp.agent_id,
                "git_ref": cp.git_ref,
                "main_part_id": cp.main_part_id,
                "parent_checkpoint_id": cp.parent_checkpoint_id,
            }
            for cp in checkpoints
        ],
        "fork_cwd": str(target_cwd),
        "opencode_db_path": str(transcribe_result.opencode_db_path),
        "error": "",
    }


def _birth_traceback_metadata(
    db_path: Path,
    source: _ReconstructSource,
    target_cwd: Path,
    checkpoints: list[LegacyCheckpoint],
    git_existed_before: bool,
    opencode_existed_before: bool,
) -> dict[str, Any]:
    """birth_traceback 溯源 metadata（含 .git/.opencode 是否由本次重建产生）。"""
    return {
        "birth_traceback": {
            "reconstructed_from": str(db_path),
            "reconstructed_at": datetime.now(UTC).isoformat(),
            "origin_sdk_session_id": source.sdk_session_id,
            "origin_cwd": source.origin_cwd,
            "fork_cwd": str(target_cwd),
            "checkpoints": len(checkpoints),
            # .git 是否由本次重建产生。删除清理时据此判定 .git 是否可清——
            # 旧 session 无此字段 → 保守不清（target_cwd 默认不整体 rmtree）。
            "created_git_dir": ((target_cwd / ".git").exists() and not git_existed_before),
            "created_opencode_dir": (
                (target_cwd / ".opencode").exists() and not opencode_existed_before
            ),
        },
    }


def _imported_event(
    session_id: str,
    config: SessionConfig,
    db_path: Path,
    sdk_session_id: str,
    checkpoints: list[LegacyCheckpoint],
) -> Event:
    """导入完成的 LOG_AUTO 事件行。"""
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type=EventType.LOG_AUTO,
        data={
            "action": "reconstructed_session_imported",
            "name": config.name,
            "role": config.role,
            "source_db": str(db_path),
            "sdk_session_id": sdk_session_id,
            "checkpoints": len(checkpoints),
        },
    )


async def _synthesize_historical_events(
    *,
    store_events: Callable[[list[Any]], Awaitable[None]],
    session_id: str,
    timeline: list[TimelineEntry],
    bridges: list[dict[str, Any]] | None,
) -> None:
    """从 InteractionBridge + timeline 合成历史 SUBAGENT/STEP_FINISH 事件。

    重建 session 无实时 driver 事件流，但前端「概览」从 events 表的
    subagent/step_finish 事件派生数据。合成策略（与实时 driver 同构）：
    bridge 驱动（派发关系的完备真相，覆盖主/子/孙全层级），timeline 派发
    ToolCall 提供后备；时间戳与父级解析的优先级见各 helper docstring；
    末尾一个 STEP_FINISH（``num_turns`` = 主 agent turn 数）。
    """
    bridge_by_sid, bridge_map = _build_bridge_maps(bridges)
    dispatch_by_sid, parent_by_sid, turn_owner_sid = _build_dispatch_maps(timeline, bridge_map)
    sub_first_ts = _build_sub_first_ts(timeline)
    starts = _collect_starts(bridge_by_sid, dispatch_by_sid, sub_first_ts)

    events: list[Event] = []
    for sid, start_ms in starts:
        bridge = bridge_by_sid.get(sid) or {}
        tc = dispatch_by_sid.get(sid) or {}
        agent_type = str(bridge.get("subagentType") or "") or extract_subagent_type_from_args(tc)
        agent_name = str(bridge.get("subagentName") or "")
        parent_sid = _resolve_parent_sid(sid, bridge, parent_by_sid, turn_owner_sid)
        events.append(
            _subagent_start_event(session_id, sid, start_ms, agent_type, agent_name, parent_sid)
        )
        stop = _subagent_stop_event(session_id, sid, start_ms, bridge, tc, agent_type, agent_name)
        if stop is not None:
            events.append(stop)

    main_turn_count = sum(1 for e in timeline if e.role == "main" and e.kind == "turn")
    if main_turn_count > 0:
        last_ts = timeline[-1].ts if timeline else 0
        # 末尾发一个 STEP_FINISH，携带最终结果（usage 留 0）
        events.append(_step_finish_event(session_id, main_turn_count, last_ts))

    if events:
        await store_events(events)
        logger.info(
            "导入会话 %s 合成 %d 条历史事件（子代理 %d 个，主 turn %d 轮）",
            session_id[:12],
            len(events),
            sum(1 for e in events if e.event_type is EventType.SUBAGENT_START),
            main_turn_count,
        )


def _build_bridge_maps(
    bridges: list[dict[str, Any]] | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """(subagentSessionId → 首条 bridge, dispatchToolCallId → subagentSessionId)。"""
    bridge_by_sid: dict[str, dict[str, Any]] = {}
    bridge_map: dict[str, str] = {}
    for b in bridges or []:
        tc_id = str(b.get("dispatchToolCallId") or "")
        sub_sid = str(b.get("subagentSessionId") or "")
        if not sub_sid:
            continue
        bridge_by_sid.setdefault(sub_sid, b)
        if tc_id:
            bridge_map[tc_id] = sub_sid
    return bridge_by_sid, bridge_map


def _build_dispatch_maps(
    timeline: list[TimelineEntry],
    bridge_map: dict[str, str],
) -> tuple[dict[str, dict[str, Any]], dict[str, str], dict[str, str]]:
    """从 timeline 收集派发索引：(dispatch_by_sid, parent_by_sid, turn_owner_sid)。

    ``dispatch_by_sid``：子会话 → 派发 task tool call（主 + 子代理派发都算），
    作 bridge 的后备（bridge 无时间戳 / bridge 表整体缺失时退化为纯 timeline
    驱动）。``parent_by_sid``：派发者 agent id（主 agent 派发为空串）——派发
    条目的 subagent_session_id 语义随 role 而异：主 agent 的 task 条目携带被
    派发子会话 id（检查点链路语义），子 agent 的 task 条目携带所属会话 id
    （= 派发者），须按 role 区分，否则主派发会得到 parent = 自身；不可用
    bridge.dispatchExecutionId 判父级（cpx 录制形态下它总是指向根 execution）。
    ``turn_owner_sid``：turn_id → 该 turn 所属子会话 id，bridge-only（派发
    ToolCall 行缺失）时经 bridge.dispatchTurnId 解析父级。
    """
    dispatch_by_sid: dict[str, dict[str, Any]] = {}
    parent_by_sid: dict[str, str] = {}
    turn_owner_sid: dict[str, str] = {}
    for entry in timeline:
        if entry.kind == "turn" and entry.turn is not None:
            turn_id = str(entry.turn.get("id") or "")
            if turn_id:
                turn_owner_sid[turn_id] = str(entry.turn.get("subagentSessionId") or "")
            continue
        if entry.kind != "tool_call" or entry.tool_call is None:
            continue
        tc = entry.tool_call
        if str(tc.get("toolName") or "") != "task":
            continue
        sid = lookup_bridge_sub(bridge_map, tc) or extract_subagent_sid_from_args(tc)
        if sid:
            dispatch_by_sid.setdefault(sid, tc)
            parent_by_sid.setdefault(
                sid,
                str(entry.subagent_session_id or "") if entry.role == "subagent" else "",
            )
    return dispatch_by_sid, parent_by_sid, turn_owner_sid


def _build_sub_first_ts(timeline: list[TimelineEntry]) -> dict[str, int]:
    """子会话在 timeline 里的最早 ts（派发行与 bridge 均无时间戳时的最后回退）。"""
    sub_first_ts: dict[str, int] = {}
    for entry in timeline:
        sid = entry.subagent_session_id
        if not sid or not entry.ts:
            continue
        cur = sub_first_ts.get(sid)
        if cur is None or entry.ts < cur:
            sub_first_ts[sid] = entry.ts
    return sub_first_ts


def _collect_starts(
    bridge_by_sid: dict[str, dict[str, Any]],
    dispatch_by_sid: dict[str, dict[str, Any]],
    sub_first_ts: dict[str, int],
) -> list[tuple[str, int]]:
    """(sub_sid, start_ms) 列表（按 start_ms 升序）：bridge 时间戳优先，派发回退。

    全部为空再退子 agent 首条 entry 的 ts（子 turn createdAt）——旧 db 的
    派发 ToolCall startedAt/completedAt 可能均为 NULL，缺这级回退子代理
    节点会整条丢失。
    """
    starts: list[tuple[str, int]] = []
    for sid in set(bridge_by_sid) | set(dispatch_by_sid):
        bridge = bridge_by_sid.get(sid) or {}
        tc = dispatch_by_sid.get(sid) or {}
        start_ms = (
            iso_to_ms(str(bridge.get("dispatchTimestamp") or ""))
            or iso_to_ms(str(tc.get("startedAt") or ""))
            or iso_to_ms(str(tc.get("completedAt") or ""))
            or sub_first_ts.get(sid, 0)
        )
        if not start_ms:
            logger.warning("导入事件合成：子代理 %s 无可用开始时间戳，跳过", sid)
            continue
        starts.append((sid, start_ms))
    starts.sort(key=lambda kv: kv[1])
    return starts


def _resolve_parent_sid(
    sid: str,
    bridge: dict[str, Any],
    parent_by_sid: dict[str, str],
    turn_owner_sid: dict[str, str],
) -> str:
    """父级解析：timeline 派发条目优先，bridge-only 时经 dispatchTurnId 回退。"""
    parent_sid = parent_by_sid.get(sid)
    if parent_sid is None:
        parent_sid = turn_owner_sid.get(str(bridge.get("dispatchTurnId") or ""), "")
    return parent_sid or ""


def _ms_to_utc_dt(ms: int) -> datetime:
    """毫秒 epoch → aware UTC datetime（与 Event.create 的 aware-UTC 风格一致）。"""
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def _subagent_start_event(
    session_id: str,
    sid: str,
    start_ms: int,
    agent_type: str,
    agent_name: str,
    parent_sid: str,
) -> Event:
    """SUBAGENT_START 事件（start 另带 parent_agent_id 供前端拓扑边层级化）。"""
    return Event(
        id=uuid4().hex,
        session_id=session_id,
        iteration=0,
        timestamp=_ms_to_utc_dt(start_ms),
        event_type=EventType.SUBAGENT_START,
        data={
            "agent_id": sid,
            "agent_type": agent_type,
            "agent_name": agent_name,
            "parent_agent_id": parent_sid,
            "_synthesized": True,
        },
    )


def _subagent_stop_event(
    session_id: str,
    sid: str,
    start_ms: int,
    bridge: dict[str, Any],
    tc: dict[str, Any],
    agent_type: str,
    agent_name: str,
) -> Event | None:
    """SUBAGENT_STOP 事件；bridge 表明未返回（dispatched）时返回 None。

    bridge 是真相：status 表明未返回（dispatched）时不发 stop，节点由前端按
    session 终态归档；completed / 无 bridge 的 timeline 后备路径回退
    completedAt，再退 start_ms——重建 session 是已跑完态，stop 缺失会让节点
    一直显示 running。
    """
    end_ms = iso_to_ms(str(bridge.get("responseTimestamp") or ""))
    if not end_ms:
        bridge_completed = not bridge or str(bridge.get("status") or "") == "completed"
        if bridge_completed:
            end_ms = iso_to_ms(str(tc.get("completedAt") or "")) or start_ms
    if not end_ms:
        return None
    return Event(
        id=uuid4().hex,
        session_id=session_id,
        iteration=0,
        timestamp=_ms_to_utc_dt(max(end_ms, start_ms)),
        event_type=EventType.SUBAGENT_STOP,
        data={
            "agent_id": sid,
            "agent_type": agent_type,
            "agent_name": agent_name,
            "total_tokens": int(bridge.get("subagentTokens") or 0),
            "content": "",
            "messages": [],
            "_synthesized": True,
        },
    )


def _step_finish_event(session_id: str, num_turns: int, last_ts: int) -> Event:
    """末尾 STEP_FINISH 事件（携带最终结果，usage 留 0）。"""
    return Event(
        id=uuid4().hex,
        session_id=session_id,
        iteration=0,
        timestamp=_ms_to_utc_dt(last_ts),
        event_type=EventType.STEP_FINISH,
        data={
            "session_id": session_id,
            "duration_ms": 0,
            "num_turns": num_turns,
            "total_cost_usd": 0.0,
            "is_error": False,
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0,
            },
            "modelUsage": {},
            "_synthesized": True,
        },
    )


class _ImportLogger:
    """导入日志文件写入器：行缓冲覆盖写，行首带时间戳；log_path 为空时不写。"""

    def __init__(self, log_path: Path | None) -> None:
        # 句柄跨整个导入生命周期、由 close() 统一关闭，无法收窄为 with 块
        self._fp: TextIO | None = None
        if log_path is not None:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            self._fp = open(log_path, "w", encoding="utf-8", buffering=1)  # noqa: SIM115
        # 终态 phase 是否已写出（成功 phase=complete 或显式失败 phase=failed）
        self.finalized = False

    def write(self, line: str) -> None:
        """写一行阶段/步骤日志（时间戳前缀，本地时间带时区偏移）。"""
        if self._fp is not None:
            now = datetime.now(tz=UTC).astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
            self._fp.write(f"{now} {line}\n")

    def finish(self, line: str) -> None:
        """写出终态 phase 行；finally 据此判断是否需补写意外失败行。"""
        self.write(line)
        self.finalized = True

    def close(self) -> None:
        """关闭日志句柄（best-effort）。"""
        if self._fp is not None:
            with contextlib.suppress(OSError):
                self._fp.close()
