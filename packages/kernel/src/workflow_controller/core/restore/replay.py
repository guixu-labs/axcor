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

"""快照代检查点重放编排（自插件 TracebackRunner 迁移重写）。

流程：查历史检查点 → 解析 origin 会话与 runtime → fork 临时目录恢复文件
态（含 git 沙箱）→ 按 message 级截断种子对话史 → 完整继承 origin 配置并
应用 overrides → 经注入端口起新 resume 会话。透明重放原则：文件态/对话
史/配置忠实继承，首轮信号是通用「继续」，不注入重放语义。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from workflow_controller.config.settings import SessionConfig
from workflow_controller.core.restore.claude_seed import (
    detect_replay_runtime,
    seed_claude_session,
)
from workflow_controller.core.restore.opencode_seed import (
    find_exclude_message_id,
    find_main_part_id_for_subagent,
    get_last_seed_error,
    seed_prebuilt_session,
)
from workflow_controller.core.workspace.repository import (
    init_replay_git_sandbox,
    legacy_snapshot_dir_for,
    legacy_snapshot_restore,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from workflow_controller.core.checkpoint_compat import LegacyCheckpoint

__all__ = ["ReplayPorts", "TracebackReplayRunner"]

logger = logging.getLogger(__name__)

# 透明重放首轮信号：与正常 pause→resume 一致的通用「继续」信号
TRACEBACK_CONTINUE_PROMPT = "请继续你之前的工作。"

# 重放从检查点续跑，给 max_iterations 留余量避免撞上限截断
_TRACEBACK_MAX_ITER_BUFFER = 10

_REVIEWER_OVERRIDE_KEYS = (
    "reviewer_enabled",
    "reviewer_model",
    "reviewer_provider_index",
    "reviewer_thinking_enabled",
    "reviewer_thinking_budget_tokens",
)


@dataclass(frozen=True)
class ReplayPorts:
    """重放编排的注入端口（会话事实、会话创建与 runtime 解析）。"""

    find_session: Callable[[str], Awaitable[Any | None]]
    load_roster_session: Callable[[str], Awaitable[dict[str, Any] | None]]
    legacy_checkpoint: Callable[[str, str], LegacyCheckpoint | None]
    start_resumed_session: Callable[..., Awaitable[Any]]
    default_runtime: Callable[[], str | None]
    resolve_runtime: Callable[[str | None], str]
    write_hook_config: Callable[[Path], None]
    # fork 会话注册成功后的目录登记（生命周期清理由宿主收回）
    register_fork: Callable[[str, str], None]
    work_dir: str = ""
    snapshot_timeout_seconds: float = 30.0
    git_history_max_mb: int = 0


@dataclass
class _ReplayPreflight:
    """前置校验通过的产物：origin 事实 + 检查点 + 匹配 runtime + 配置基底。"""

    origin_session_id: str
    checkpoint: Any
    origin: Any
    cwd: str
    sdk_session_id: str
    runtime: str
    base_config: SessionConfig


@dataclass
class _ForkSeed:
    """截断对话史写入 fork 独立存储后的产物：新 sdk 会话 + per-fork env。"""

    fork_id: str
    new_sdk_session_id: str
    fork_env: dict[str, str] = field(default_factory=dict)


def _checkpoint_reject_reason(
    checkpoint: LegacyCheckpoint | None, origin_session_id: str, checkpoint_id: str
) -> str | None:
    """检查点不可重放的原因（None = 可用）。"""
    if checkpoint is None:
        return f"checkpoint {checkpoint_id} not found"
    if checkpoint.session_id != origin_session_id:
        return "checkpoint does not belong to this session"
    if not checkpoint.git_ref:
        return (
            f"checkpoint {checkpoint_id} has no git_ref (status={checkpoint.status}); "
            "file-state not snapshot-able, cannot replay"
        )
    return None


def _apply_reviewer_overrides(base_config: SessionConfig, overrides: Mapping[str, Any]) -> None:
    """reviewer 覆盖落到 fork 的 reviewer 配置（origin 无块时按需补空块）。"""
    if base_config.reviewer is None:
        if not any(overrides.get(key) is not None for key in _REVIEWER_OVERRIDE_KEYS):
            return
        from workflow_controller.config.settings import SessionReviewerConfig

        # 不硬编码默认 model：留空交给下游按全局配置补齐，避免绕过全局 reviewer.model
        base_config.reviewer = SessionReviewerConfig(model="")
    if (enabled := overrides.get("reviewer_enabled")) is not None:
        base_config.reviewer.enabled = bool(enabled)
    if (model := overrides.get("reviewer_model")) is not None and str(model) != "":
        base_config.reviewer.model = str(model)
    if (index := overrides.get("reviewer_provider_index")) is not None:
        with contextlib.suppress(TypeError, ValueError):
            base_config.reviewer.provider_index = int(index)
    if (thinking_enabled := overrides.get("reviewer_thinking_enabled")) is not None:
        base_config.reviewer.thinking_mode = "enabled" if bool(thinking_enabled) else "disabled"
    if (budget := overrides.get("reviewer_thinking_budget_tokens")) is not None:
        with contextlib.suppress(TypeError, ValueError):
            base_config.reviewer.thinking_budget_tokens = max(1024, int(budget))


def _apply_overrides(base_config: SessionConfig, overrides: Mapping[str, Any] | None) -> bool:
    """应用 driver 层 overrides（赢过 origin 继承值），返回 max_iterations 是否被显式覆盖。"""
    ov = dict(overrides or {})
    if (cli_wrapper := ov.get("cli_wrapper")) is not None:
        if cli_wrapper not in ("", "cpx"):
            raise ValueError(f"Unsupported CLI wrapper: {cli_wrapper!r}")
        base_config.cli_wrapper = str(cli_wrapper)
    if model := ov.get("model"):
        base_config.model = str(model)
    if (system_prompt := ov.get("system_prompt")) is not None and str(system_prompt) != "":
        base_config.system_prompt = str(system_prompt)
    if (index := ov.get("provider_index")) is not None:
        with contextlib.suppress(TypeError, ValueError):
            base_config.provider_index = int(index)
    _apply_reviewer_overrides(base_config, ov)
    if (initial_prompt := ov.get("initial_prompt")) is not None and str(initial_prompt) != "":
        base_config.initial_prompt = str(initial_prompt)
    if (evaluator := ov.get("evaluator")) is not None and str(evaluator) != "":
        if base_config.loop is None:
            from workflow_controller.config.settings import LoopConfig

            base_config.loop = LoopConfig()
        base_config.loop.evaluator = str(evaluator)
    max_iter_overridden = False
    if (max_iterations := ov.get("max_iterations")) is not None:
        try:
            iterations = int(max_iterations)
        except (TypeError, ValueError):
            iterations = 0
        if iterations > 0:
            if base_config.loop is None:
                from workflow_controller.config.settings import LoopConfig

                base_config.loop = LoopConfig()
            base_config.loop.max_iterations = iterations
            max_iter_overridden = True
    if (task_goal := ov.get("reviewer_task_goal")) is not None and str(task_goal) != "":
        base_config.criteria = {**(base_config.criteria or {}), "task_goal": str(task_goal)}
    return max_iter_overridden


def _resolve_origin_env(origin_cwd: str) -> dict[str, str] | None:
    """按 origin cwd 解析其 fork db 所在 env（嵌套重放支持）。

    每层重放的 opencode.db 在 ``<cwd>/.workflow-controller/opencode/``；存在
    即指向该 XDG 根，否则回落宿主默认（origin 是原始会话）。
    """
    if not origin_cwd:
        return None
    candidate = Path(origin_cwd) / ".workflow-controller" / "opencode" / "opencode.db"
    if candidate.is_file():
        return {"XDG_DATA_HOME": str(Path(origin_cwd) / ".workflow-controller")}
    return None


def _write_permission_config(fork_cwd: Path) -> None:
    """在 fork cwd 写 ``.opencode/opencode.json`` 设 ``external_directory: allow``。

    种子会话的 project 继承自 origin，fork cwd 内的文件访问会被判外部目录
    而悬死在权限询问上（无用户可确认）；显式放行，既有字段只合并不覆盖。
    """
    config_path = fork_cwd / ".opencode" / "opencode.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, Any] = {}
    if config_path.is_file():
        try:
            existing = json.loads(config_path.read_text())
        except (OSError, json.JSONDecodeError):
            existing = {}
    existing["$schema"] = "https://opencode.ai/config.json"
    existing["permission"] = {"external_directory": "allow"}
    try:
        config_path.write_text(json.dumps(existing, indent=2))
    except OSError:
        logger.debug("重放权限配置写入失败（不阻塞）", exc_info=True)


def _seed_host_auth(fork_cwd: Path) -> None:
    """Best-effort 把宿主 ``auth.json`` 拷进 fork 的 XDG opencode 目录。"""
    try:
        from workflow_controller.transcript.opencode_store import opencode_db_path

        source = opencode_db_path().parent / "auth.json"
        if not source.is_file():
            return
        dest_dir = fork_cwd / ".workflow-controller" / "opencode"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / "auth.json"
        need_copy = True
        if dest.is_file():
            try:
                need_copy = source.read_bytes() != dest.read_bytes()
            except OSError:
                need_copy = True
        if need_copy:
            shutil.copy2(source, dest)
    except Exception:
        logger.debug("重放宿主凭证拷贝失败（不阻塞）", exc_info=True)


def _build_birth_traceback(
    preflight: _ReplayPreflight, fork_root: Path, fork_cwd: Path
) -> dict[str, Any]:
    """构造 fork 溯源：逻辑归属固定为根 origin（嵌套重放沿用根归属）。"""
    origin = preflight.origin
    origin_birth: dict[str, Any] = (
        {}
        if origin is None
        else (
            ((getattr(origin, "plugin_metadata", None) or {}).get("traceback_replay") or {}).get(
                "birth"
            )
            or {}
        )
    )
    if origin_birth.get("fork_root"):
        logical_sid = origin_birth.get("origin_session_id") or preflight.origin_session_id
        logical_cwd = origin_birth.get("origin_cwd") or str(preflight.cwd)
        logical_name = origin_birth.get("origin_name") or getattr(origin, "name", "")
    else:
        logical_sid = preflight.origin_session_id
        logical_cwd = str(preflight.cwd)
        logical_name = getattr(origin, "name", "") if origin is not None else ""
    return {
        "origin_session_id": logical_sid,
        "origin_checkpoint_id": preflight.checkpoint.checkpoint_id,
        "forked_at": datetime.now(tz=UTC).isoformat(),
        "fork_root": str(fork_root),
        "fork_cwd": str(fork_cwd),
        "origin_cwd": logical_cwd,
        "origin_name": logical_name,
    }


class TracebackReplayRunner:
    """快照代检查点重放编排器（端口注入，内核不依赖插件）。"""

    def __init__(self, ports: ReplayPorts) -> None:
        self._ports = ports

    async def run(
        self,
        origin_session_id: str,
        checkpoint_id: str,
        *,
        runtime: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """从历史检查点起重放，返回新会话信息（``{"ok", "session_id", "error"}``）。"""
        preflight = await self._preflight(origin_session_id, checkpoint_id, runtime)
        if isinstance(preflight, dict):
            return preflight
        return await self._execute_replay(preflight, overrides)

    # ── 前置校验 ────────────────────────────────────────────

    async def _preflight(
        self,
        origin_session_id: str,
        checkpoint_id: str,
        runtime: str | None,
    ) -> _ReplayPreflight | dict[str, Any]:
        """重放前置校验；不可重放时返回 error dict。"""
        checkpoint = self._ports.legacy_checkpoint(origin_session_id, checkpoint_id)
        reason = _checkpoint_reject_reason(checkpoint, origin_session_id, checkpoint_id)
        if reason is not None:
            return {"ok": False, "error": reason}
        context = await self._resolve_origin_context(origin_session_id)
        if context is None:
            return {"ok": False, "error": "origin session not found"}
        origin, cwd, sdk_session_id, origin_runtime, base_config = context
        if not sdk_session_id:
            return {
                "ok": False,
                "error": "origin session has no sdk_session_id; cannot replay its conversation",
            }
        if origin is not None and origin.loop_status == "yes" and origin.state.value == "running":
            return {
                "ok": False,
                "error": "origin session still running — traceback only on finished sessions",
            }
        resolved = await self._resolve_replay_runtime(
            sdk_session_id, cwd, origin_runtime, base_config, runtime
        )
        if isinstance(resolved, dict):
            return resolved
        return _ReplayPreflight(
            origin_session_id=origin_session_id,
            checkpoint=checkpoint,
            origin=origin,
            cwd=cwd,
            sdk_session_id=sdk_session_id,
            runtime=resolved,
            base_config=base_config,
        )

    async def _resolve_origin_context(
        self, origin_session_id: str
    ) -> tuple[Any, str, str | None, str | None, SessionConfig] | None:
        """解析 origin：池内命中深拷贝配置；池外读持久化行反序列化。"""
        origin = await self._ports.find_session(origin_session_id)
        if origin is not None:
            cwd = getattr(origin.config, "cwd", "") or self._ports.work_dir
            sdk_session_id = origin.sdk_session_id
            origin_runtime = getattr(origin.config, "runtime", None)
            base_config = origin.config.model_copy(deep=True)
        else:
            row = await self._ports.load_roster_session(origin_session_id)
            if row is None:
                return None
            try:
                config_dict = json.loads(row.get("config_json") or "{}")
            except json.JSONDecodeError:
                config_dict = {}
            cwd = config_dict.get("cwd", "") or self._ports.work_dir
            sdk_session_id = row.get("sdk_session_id")
            origin_runtime = config_dict.get("runtime")
            try:
                base_config = SessionConfig(**config_dict)
            except Exception:
                base_config = SessionConfig(name="origin", cwd=cwd)
        return origin, cwd, sdk_session_id, origin_runtime, base_config

    async def _resolve_replay_runtime(
        self,
        sdk_session_id: str,
        cwd: str,
        origin_runtime: str | None,
        base_config: SessionConfig,
        runtime: str | None,
    ) -> str | dict[str, Any]:
        """探测源 runtime 并解析目标 runtime；不支持/不匹配返回 error dict。"""
        source_runtime: str = await asyncio.to_thread(
            detect_replay_runtime,
            sdk_session_id=sdk_session_id,
            cwd=cwd,
            configured_runtime=origin_runtime,
            default_runtime=self._ports.default_runtime() or "claude_sdk",
            source_config_dir=(base_config.env_overrides or {}).get("CLAUDE_CONFIG_DIR"),
        )
        chosen_runtime = self._ports.resolve_runtime(runtime or source_runtime)
        if source_runtime not in ("opencode", "claude_cli"):
            return {
                "ok": False,
                "error": f"traceback does not support {source_runtime} source sessions",
            }
        if chosen_runtime != source_runtime:
            env_driver = os.environ.get("WORKFLOW_CONTROLLER_DRIVER")
            hint = f" (WORKFLOW_CONTROLLER_DRIVER={env_driver} is set)" if env_driver else ""
            return {
                "ok": False,
                "error": (
                    f"Cannot replay {source_runtime} conversation with {chosen_runtime}; "
                    f"select the source runtime{hint}"
                ),
            }
        return source_runtime

    # ── 重放主体 ────────────────────────────────────────────

    async def _execute_replay(
        self, preflight: _ReplayPreflight, overrides: dict[str, Any] | None
    ) -> dict[str, Any]:
        """执行重放主体；失败（含异常）时 finally 清理 fork 目录。"""
        fork_root = Path(tempfile.mkdtemp(prefix="workflow-controller-tb-")).resolve()
        fork_cwd = fork_root / "cwd"
        fork_cwd.mkdir(parents=True, exist_ok=True)
        registered = False
        try:
            seed = await self._restore_and_seed(preflight, fork_cwd)
            if isinstance(seed, dict):
                return seed
            config = self._build_fork_config(preflight, seed, fork_cwd, overrides)
            birth = _build_birth_traceback(preflight, fork_root, fork_cwd)
            await self._prepare_fork_runtime(preflight, fork_cwd)
            session = await self._ports.start_resumed_session(
                config,
                resume_sdk_session_id=seed.new_sdk_session_id,
                plugin_metadata={"traceback_replay": {"birth": birth}},
                start_action="traceback_session_started",
                inherit_overlays_from=preflight.origin_session_id,
                initial_current_iteration=(
                    int(preflight.checkpoint.iteration) if preflight.checkpoint.iteration else None
                ),
            )
            self._ports.register_fork(str(session.id), str(fork_root))
            registered = True
            logger.info(
                "traceback: forked %s from %s/%s (cwd=%s, resume=%s)",
                session.id,
                preflight.origin_session_id[:12],
                preflight.checkpoint.checkpoint_id,
                fork_cwd,
                seed.new_sdk_session_id,
            )
            return {
                "ok": True,
                "session_id": str(session.id),
                "fork_cwd": str(fork_cwd),
                "fork_root": str(fork_root),
                "resume_sdk_session_id": seed.new_sdk_session_id,
                "origin_checkpoint_id": preflight.checkpoint.checkpoint_id,
            }
        finally:
            if not registered:
                await asyncio.to_thread(shutil.rmtree, fork_root, ignore_errors=True)

    async def _restore_and_seed(
        self, preflight: _ReplayPreflight, fork_cwd: Path
    ) -> _ForkSeed | dict[str, Any]:
        """恢复文件态 + git 沙箱 + 按源 runtime 截断种子对话史。"""
        snapshot_dir = await self._locate_snapshot_repo(preflight)
        restored, inner_git_captured = await asyncio.to_thread(
            legacy_snapshot_restore, snapshot_dir, preflight.checkpoint.git_ref, fork_cwd
        )
        if not restored:
            return {
                "ok": False,
                "error": (
                    f"restore snapshot {preflight.checkpoint.git_ref[:8]} to {fork_cwd} failed"
                ),
            }
        await asyncio.to_thread(
            init_replay_git_sandbox,
            fork_cwd,
            preflight.cwd,
            git_history_max_mb=self._ports.git_history_max_mb,
            inner_git_captured=inner_git_captured,
        )
        fork_id = uuid4().hex[:12]
        is_final = not preflight.checkpoint.agent_id or preflight.checkpoint.trigger == (
            "session_complete"
        )
        if preflight.runtime == "claude_cli":
            return await self._seed_claude_cli_fork(preflight, fork_cwd, fork_id, is_final)
        return await self._seed_opencode_fork(preflight, fork_cwd, fork_id, is_final)

    async def _locate_snapshot_repo(self, preflight: _ReplayPreflight) -> Path:
        """定位 origin cwd 对应的快照仓目录（寻址失败按不存在处理）。"""
        return legacy_snapshot_dir_for(Path(preflight.cwd))

    async def _seed_claude_cli_fork(
        self,
        preflight: _ReplayPreflight,
        fork_cwd: Path,
        fork_id: str,
        is_final: bool,
    ) -> _ForkSeed | dict[str, Any]:
        """Claude CLI 源：按检查点截断 JSONL，写入 fork 独立配置目录。"""
        new_sdk_session_id = str(uuid4())
        claude_config, seed_error = await asyncio.to_thread(
            seed_claude_session,
            source_session_id=preflight.sdk_session_id,
            source_cwd=preflight.cwd,
            new_session_id=new_sdk_session_id,
            fork_cwd=fork_cwd,
            checkpoint_at=preflight.checkpoint.created_at,
            agent_type=preflight.checkpoint.agent_type,
            final_checkpoint=is_final,
            source_config_dir=(preflight.base_config.env_overrides or {}).get("CLAUDE_CONFIG_DIR"),
            snapshot_timeout_seconds=self._ports.snapshot_timeout_seconds,
        )
        if claude_config is None:
            return {"ok": False, "error": seed_error}
        return _ForkSeed(
            fork_id=fork_id,
            new_sdk_session_id=new_sdk_session_id,
            fork_env={"CLAUDE_CONFIG_DIR": str(claude_config)},
        )

    async def _seed_opencode_fork(
        self,
        preflight: _ReplayPreflight,
        fork_cwd: Path,
        fork_id: str,
        is_final: bool,
    ) -> _ForkSeed | dict[str, Any]:
        """OpenCode 源：按 message 级截断 opencode.db，种子进 fork 独立 db。"""
        origin_env = _resolve_origin_env(preflight.cwd)
        exclude_part_id, exclude_message_id = await self._find_exclude_ids(
            preflight, is_final, origin_env
        )
        new_sdk_session_id = f"ses_tb_{fork_id}"
        fork_env = {"XDG_DATA_HOME": str(fork_cwd / ".workflow-controller")}
        seeded = await asyncio.to_thread(
            seed_prebuilt_session,
            source_session_id=preflight.sdk_session_id,
            new_session_id=new_sdk_session_id,
            up_to_part_id=None,
            title=f"[traceback] {preflight.origin_session_id[:8]} {preflight.checkpoint.checkpoint_id}",
            env=fork_env,
            source_env=origin_env,
            exclude_part_id=exclude_part_id,
            exclude_message_id=exclude_message_id,
            directory=str(fork_cwd),
        )
        if not seeded:
            return {
                "ok": False,
                "error": (
                    f"seed_prebuilt_session failed: "
                    f"{get_last_seed_error() or 'source OpenCode conversation unavailable'}"
                ),
            }
        await asyncio.to_thread(_seed_host_auth, fork_cwd)
        return _ForkSeed(fork_id=fork_id, new_sdk_session_id=new_sdk_session_id, fork_env=fork_env)

    async def _find_exclude_ids(
        self, preflight: _ReplayPreflight, is_final: bool, origin_env: dict[str, str] | None
    ) -> tuple[str | None, str | None]:
        """定位 message 级截断点，返回 ``(exclude_part_id, exclude_message_id)``。"""
        if is_final:
            return None, None
        task_pid = preflight.checkpoint.main_part_id or await asyncio.to_thread(
            find_main_part_id_for_subagent,
            preflight.sdk_session_id,
            preflight.checkpoint.agent_id,
            origin_env,
        )
        if not task_pid:
            return None, None
        exclude_message_id = await asyncio.to_thread(
            find_exclude_message_id,
            preflight.sdk_session_id,
            task_pid,
            origin_env,
        )
        return (None if exclude_message_id else task_pid), exclude_message_id

    def _build_fork_config(
        self,
        preflight: _ReplayPreflight,
        seed: _ForkSeed,
        fork_cwd: Path,
        overrides: dict[str, Any] | None,
    ) -> SessionConfig:
        """构造 fork 配置：完整继承 origin + 重放必需字段 + overrides。"""
        base_config = preflight.base_config
        base_config.name = f"replay-{seed.fork_id[:8]}"
        base_config.role = "worker"
        base_config.cwd = str(fork_cwd)
        base_config.initial_prompt = TRACEBACK_CONTINUE_PROMPT
        base_config.runtime = preflight.runtime
        # 用户级重放不进源 run 账（谱系经 birth 溯源表达）：置空归属，由创建
        # 路径生成独立隐式 run；traceback 配置随继承保留（新会话按 session
        # 作用域树模型接入托管工作区）
        base_config.run_id = None
        base_config.env_overrides = dict(seed.fork_env)
        max_iter_overridden = _apply_overrides(base_config, overrides)
        if base_config.loop and not max_iter_overridden:
            base_config.loop.max_iterations = (
                int(base_config.loop.max_iterations) + _TRACEBACK_MAX_ITER_BUFFER
            )
        return base_config

    async def _prepare_fork_runtime(self, preflight: _ReplayPreflight, fork_cwd: Path) -> None:
        """写 hook 配置；opencode 源再写权限配置（防工具调用悬死）。"""
        try:
            await asyncio.to_thread(self._ports.write_hook_config, fork_cwd)
        except Exception:
            logger.debug("重放 hook 配置写入失败（不阻塞）", exc_info=True)
        if preflight.runtime == "opencode":
            await asyncio.to_thread(_write_permission_config, fork_cwd)
