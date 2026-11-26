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

"""Session API — 会话状态查询与控制。

所有方法返回纯数据结构，不依赖 Web 框架。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from workflow_controller.config.reviewer import resolve_reviewer_thinking
from workflow_controller.config.settings import AppConfig, SessionConfig, SessionReviewerConfig
from workflow_controller.core.runs import RunKind, RunNotFoundError
from workflow_controller.driver.anthropic_http import AnthropicHttpClient
from workflow_controller.driver.opencode import _resolve_opencode_db_path
from workflow_controller.session.events import Event, EventType, outcome_kind_for_reason
from workflow_controller.transcript.sources import claude_config_dirs, resolve_claude_session_path

logger = logging.getLogger(__name__)


class WorkflowSessionOperationError(ValueError):
    """workflow 成员的操作拒绝直通（run 平面是唯一写者，须经 run 平面操作）。"""

    def __init__(self, session_id: str, run_id: str) -> None:
        super().__init__(
            f"session {session_id} 是 workflow run {run_id} 的成员，"
            f"操作请经 /api/v1/runs/{run_id}/sessions/{session_id}/*"
        )


@dataclass
class ReviewerStartOverrides:
    """start session 时 reviewer 相关的覆盖项；None 表示跟随项目/全局配置。"""

    model: str | None = None
    task_goal: str | None = None
    provider_index: int | None = None
    enabled: bool | None = None
    thinking_mode: str | None = None
    thinking_budget_tokens: int | None = None


if TYPE_CHECKING:
    from workflow_controller.core.orchestrator import Orchestrator


def _path_mtime(path: Path) -> float:
    """文件 mtime 探测兜底：不可 stat 的历史条目视为最旧，不中断列表。"""
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _parse_opencode_model(raw: str | None) -> str | None:
    """Parse opencode model field (JSON or plain string) to 'provider/model'."""
    if not raw:
        return None
    if raw.startswith("{"):
        try:
            m = json.loads(raw)
            pid = m.get("providerID", "")
            mid = m.get("id", "")
            if pid and mid:
                return f"{pid}/{mid}"
            return mid or None
        except (json.JSONDecodeError, TypeError):
            pass
    return raw


def _first_user_message_text(entry: dict[str, Any]) -> str:
    """从 user 事件行提取文本（content 为块列表时拼接其中 text 块）。"""
    msg = entry.get("message", {})
    content = msg.get("content", "")
    if isinstance(content, list):
        return "".join(
            b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
        )
    return str(content)


def _get_opencode_session_model(sdk_session_id: str) -> str | None:
    """Get the model in 'provider/model' format from opencode DB."""
    db_path = _resolve_opencode_db_path()
    if not db_path or not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path))
        row = conn.execute("SELECT model FROM session WHERE id=?", (sdk_session_id,)).fetchone()
        conn.close()
        if not row or not row[0]:
            return None
        return _parse_opencode_model(str(row[0]))
    except Exception:
        return None


def _get_opencode_session_title(sdk_session_id: str | None) -> str | None:
    if not sdk_session_id:
        return None
    db_path = _resolve_opencode_db_path()
    if not db_path:
        return None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT title FROM session WHERE id = ?", (sdk_session_id,))
        row = cur.fetchone()
        conn.close()
        if row and row["title"] and not row["title"].startswith("New session"):
            return row["title"]
    except sqlite3.Error:
        pass
    return None


def _is_design_md_compliant_name(name: str) -> bool:
    """Check if session name follows handoff/continuation naming pattern.

    Pattern: {hex_id}-handoff-{n} or {hex_id}-continue-{n}
    Example: 76c3c040-handoff-4, 76c3c040-continue-2
    """
    import re

    pattern = r"^[a-f0-9]+-(handoff|continue)-\d+$"
    return bool(re.match(pattern, name))


def _stage_artifact_entries(stage: dict[str, Any]) -> list[Any]:
    """stage 的产物条目列表：``artifacts``（复数）优先，兼容旧单数 ``artifact``。

    每个 entry 是 ``{artifact: <path>, gate: ...}``（格式见
    workflow_controller.plugin.stages 的 normalize_artifacts）。
    """
    entries = stage.get("artifacts") or []
    if not entries and stage.get("artifact"):
        entries = [{"artifact": stage["artifact"]}]
    return entries


def _remove_artifact(base: Path, artifact: str) -> str | None:
    """删除单个 stage 产物；逃出 cwd/解析失败/删除失败返回 None（best-effort）。"""
    try:
        target = (base / artifact.rstrip("/")).resolve()
        target.relative_to(base)  # ValueError → 逃出 cwd，跳过
    except (OSError, ValueError):
        logger.warning("delete: skip out-of-cwd or invalid artifact %r", artifact)
        return None
    if target.is_dir():
        shutil.rmtree(target, ignore_errors=True)
        return str(target)
    if not target.exists():
        return None
    try:
        target.unlink()
    except OSError:
        return None
    return str(target)


def _clean_stage_artifacts(cwd: str, stages: list[dict[str, Any]]) -> list[str]:
    """删除 session 声明的 stage 产物（delete session 时清残留，让重跑从干净态开始）。

    只删 ``stages`` 里声明的 ``artifact`` 路径——用户在项目 workflow-controller.yaml 声明 stages 即
    opt-in，这些就是门禁查的产物（worker 生成的、可重生成），不是用户源码。路径必须落在
    ``cwd`` 内（防 ``..`` 逃逸删到 cwd 外）；逃逸或解析失败的跳过。文件/目录都删，best-effort
    （不存在 / 删失败静默跳过）。返回实际删掉的绝对路径列表（供日志/测试）。
    """
    try:
        base = Path(cwd).resolve()
    except (OSError, ValueError):
        return []
    removed: list[str] = []
    for stage in stages or []:
        if not isinstance(stage, dict):
            continue
        for entry in _stage_artifact_entries(stage):
            if not isinstance(entry, dict):
                continue
            artifact = entry.get("artifact")
            if not artifact or not isinstance(artifact, str):
                continue
            gone = _remove_artifact(base, artifact)
            if gone:
                removed.append(gone)
    return removed


def _session_elapsed_seconds(session: object) -> float:
    """会话已进行时长：running 从创建起算，其余状态从最近活动（缺省当前时间）起算。"""
    import time

    elapsed = 0.0
    if session.created_at:  # type: ignore[attr-defined]
        if session.state and session.state.value == "running":  # type: ignore[attr-defined]
            elapsed = time.time() - session.created_at.timestamp()  # type: ignore[attr-defined]
        else:
            la = getattr(session, "last_activity", None)
            elapsed = (
                (la.timestamp() if la else time.time()) - session.created_at.timestamp()  # type: ignore[attr-defined]
            )
    return elapsed


def _session_loop_progress(session: object, iterations: int, max_iter: int) -> float:
    """loop 进度：running/completed 按迭代数与上限之比，其余状态为 0。"""
    if session.state and session.state.value in ("running", "completed"):  # type: ignore[attr-defined]
        return min(1.0, iterations / max(max_iter, 1))
    return 0.0


def _recent_activity(events: list[Any]) -> list[dict[str, Any]]:
    """事件尾部（至多 5 条）的活动摘要：类型/迭代/时间戳 + 文本摘要。"""
    activity: list[dict[str, Any]] = []
    for e in reversed(events[-5:]):
        act = {
            "event_type": e.event_type.value
            if hasattr(e.event_type, "value")
            else str(e.event_type),
            "iteration": e.iteration,
            "timestamp": e.timestamp.isoformat()
            if hasattr(e.timestamp, "isoformat")
            else str(e.timestamp),
        }
        data = getattr(e, "data", {})
        if isinstance(data, dict):
            act["summary"] = str(
                data.get("action") or data.get("message") or data.get("content") or ""
            )[:80]
        activity.append(act)
    return activity


def _resolve_session_model(config: Any, sdk_id: str | None) -> str:
    """会话模型：配置值为空/占位且 sdk 会话属于 opencode 时改读 opencode DB。"""
    model = getattr(config, "model", "") if config else ""
    model_is_placeholder = not model or model.startswith("{")
    if model_is_placeholder and sdk_id is not None and sdk_id.startswith("ses_"):
        fb = _get_opencode_session_model(sdk_id)
        if fb:
            model = fb
    return model


@dataclass
class SessionInfo:
    """会话纯数据结构，Web/Headless 共用。"""

    id: str
    name: str
    role: str
    state: str
    loop_status: str
    stop_reason: str | None
    loop_hold: str | None
    current_iteration: int
    max_iterations: int
    progress: float
    elapsed_seconds: float
    created_at_iso: str
    error: str | None
    last_activity: list[dict[str, Any]] = field(default_factory=list)
    birth_handoff_doc: str | None = None
    result: str | None = None
    predecessor: dict[str, str] | None = None
    successors: list[dict[str, str]] = field(default_factory=list)
    sdk_session_id: str | None = None
    model: str = ""
    project_dir: str = ""
    run_id: str | None = None  # run 归属投影（账面真相在 run_sessions，此为冗余读面）
    plugin_metadata: dict[str, Any] = field(default_factory=dict)
    reviewer_mode: str = "auto"
    waiting_for_auto_reply_fallback: bool = False
    # 会话自身 reviewer 开关（None=未配置，回落全局 config.reviewer.enabled）。
    # 前端输入门控/回复模式按钮据此按会话区分，而非一律套全局配置。
    reviewer_enabled: bool | None = None
    # 会话启动时冻结的 reviewer 任务目标；None=旧快照缺字段。
    reviewer_task_goal: str | None = None
    provider_index: int | None = None
    reviewer_provider_index: int | None = None
    context_threshold_pct: float | None = None
    subagent_context_threshold_pct: float | None = None
    context_window: int | None = None

    @classmethod
    def from_session(
        cls,
        session: object,
        events: list[Any] | None = None,
        *,
        default_reviewer_mode: str = "auto",
    ) -> SessionInfo:
        """从 Session 对象构建 SessionInfo。"""
        s = session
        config = getattr(s, "config", None)
        elapsed = _session_elapsed_seconds(s)

        iterations = getattr(s, "current_iteration", 0) or 0
        loop_config = getattr(config, "loop", None)
        max_iter = getattr(loop_config, "max_iterations", 10)
        progress = _session_loop_progress(s, iterations, max_iter)

        activity = _recent_activity(events) if events else []

        sdk_id = getattr(s, "sdk_session_id", None)
        model = _resolve_session_model(config, sdk_id)
        reviewer = getattr(config, "reviewer", None) if config else None
        reply_fallback = getattr(s, "waiting_for_auto_reply_fallback", False) is True
        reviewer_mode = getattr(reviewer, "mode", None)
        if reviewer_mode not in ("auto", "manual"):
            reviewer_mode = default_reviewer_mode
        return cls(
            id=s.id,  # type: ignore[attr-defined]
            name=s.name,  # type: ignore[attr-defined]
            role=s.role,  # type: ignore[attr-defined]
            state=s.state.value if s.state else "unknown",  # type: ignore[attr-defined]
            loop_status=getattr(s, "loop_status", "yes"),
            stop_reason=getattr(s, "stop_reason", None),
            loop_hold=getattr(s, "loop_hold", None),
            current_iteration=iterations,
            max_iterations=max_iter,
            progress=progress,
            elapsed_seconds=elapsed,
            created_at_iso=s.created_at.isoformat() if s.created_at else "",  # type: ignore[attr-defined]
            error=getattr(s, "last_error", None) or getattr(s, "_error", None),
            birth_handoff_doc=getattr(s, "birth_handoff_doc", None),
            result=getattr(s, "result", None),
            sdk_session_id=sdk_id,
            model=model,
            project_dir=getattr(config, "cwd", "") if config else "",
            run_id=getattr(config, "run_id", None) if config else None,
            last_activity=activity,
            plugin_metadata=getattr(s, "plugin_metadata", {}) or {},
            reviewer_mode="manual" if reply_fallback else reviewer_mode,
            waiting_for_auto_reply_fallback=reply_fallback,
            reviewer_enabled=getattr(reviewer, "enabled", None) if reviewer else None,
            reviewer_task_goal=getattr(reviewer, "task_goal", None),
            provider_index=getattr(config, "provider_index", None) if config else None,
            reviewer_provider_index=getattr(reviewer, "provider_index", None),
            context_threshold_pct=getattr(config, "context_threshold_pct", None),
            subagent_context_threshold_pct=getattr(config, "subagent_context_threshold_pct", None),
            context_window=getattr(config, "context_window", None),
        )


def _prepare_start_workspace(orch: Orchestrator, project_dir: str) -> None:
    """start 前置写入：worker hooks.json 与 workspace.json（同一 project_dir 保留历史数据）。"""
    # Write the worker hook config (hooks.json) into the project so the
    # worker session's PreToolUse / Stop callbacks reach the hook server.
    try:
        orch.generate_hook_config(Path(project_dir))
    except Exception as exc:
        logger.warning("Hook config not written (%s); loop may need more iterations", exc)

    # 同一 project_dir 保留历史数据;换项目则清空事件库
    ws_dir = Path(orch.config.project_dir) / ".workflow-controller"
    ws_file = ws_dir / "workspace.json"
    ws_dir.mkdir(parents=True, exist_ok=True)
    same_project = False
    if ws_file.exists():
        try:
            prev = json.loads(ws_file.read_text(encoding="utf-8"))
            same_project = prev.get("project_dir") == project_dir
        except Exception as exc:
            logger.warning("failed to read workspace.json: %s", exc)
    if not same_project:
        ws_file.write_text(
            json.dumps({"project_dir": project_dir}, ensure_ascii=False),
            encoding="utf-8",
        )


def _start_config_source(orch: Orchestrator, project_dir: str) -> AppConfig:
    """start 的配置源：选了 project_dir 后优先用该目录的 workflow-controller.yaml
    （runtime/loop/system_prompt/starting_prompt/reviewer.task_goal 全部跟随该目录 yaml）；
    无 yaml 或加载失败则维持 web 启动时的全局 config。
    """
    src = orch.config
    proj_yaml = Path(project_dir) / "workflow-controller.yaml"
    if proj_yaml.exists():
        try:
            src = AppConfig.from_yaml(proj_yaml)
        except Exception as exc:
            logger.warning("failed to load %s, falling back to global config: %s", proj_yaml, exc)
    return src


def _sync_start_discovery_dirs(project_dir: str, src: AppConfig) -> None:
    """把项目配置的 skills_path/agents_path overlay-sync 进 worker 的原生发现目录
    （CC→.claude/、opencode→.opencode/），字段才真正生效。失败不阻断 session 启动。
    """
    try:
        from workflow_controller.config.bootstrap import sync_worker_discovery_dirs

        sync_worker_discovery_dirs(
            project_dir=Path(project_dir),
            runtime=src.runtime,
            skills_path=src.skills_path or "",
            agents_path=src.agents_path or "",
        )
    except Exception as exc:
        logger.warning("skill/agent sync failed (%s); worker may lack custom skills/agents", exc)


def _start_loop_config(src: AppConfig, evaluator: str | None, max_iterations: int | None) -> Any:
    """loop 配置副本：前端传入的 evaluator/max_iterations 覆盖 yaml 内存值（不写回 yaml）。"""
    from copy import deepcopy

    loop = deepcopy(src.loop)
    if evaluator:
        loop.evaluator = evaluator
    if max_iterations is not None:
        loop.max_iterations = max_iterations
    return loop


def _start_model_pinned(
    orch: Orchestrator, effective_model: str, effective_provider_index: int | None
) -> bool:
    """模型是否为用户显式指定：与该 provider 下标当前模型不同即视为固定选择。"""
    if not effective_model:
        return False
    rotator = getattr(orch, "auth_rotator", None)
    if rotator is None:
        return False
    return effective_model != (rotator.get_provider(effective_provider_index).model or "")


def _start_session_config(
    *,
    project_dir: str,
    prompt: str | None,
    system_prompt: str | None,
    runtime: str,
    cli_wrapper: str,
    model: str,
    provider_index: int | None,
    model_pinned: bool,
    src: AppConfig,
    reviewer: SessionReviewerConfig,
    loop: Any,
    task_goal: str,
) -> SessionConfig:
    """组装 start 的 SessionConfig（含临时会话名生成）。"""
    from datetime import UTC, datetime
    from uuid import uuid4

    _timestamp = datetime.now(tz=UTC).astimezone().strftime("%m-%d %H:%M:%S")
    return SessionConfig(
        name=f"session {_timestamp} {uuid4().hex[:4]}",
        role="worker",
        cwd=project_dir,
        initial_prompt=prompt if prompt else src.starting_prompt,
        model=model,
        provider_index=provider_index,
        model_pinned=model_pinned,
        reviewer=reviewer,
        loop=loop,
        criteria={"task_goal": task_goal},
        system_prompt=system_prompt or src.system_prompt,
        runtime=runtime,
        cli_wrapper=cli_wrapper,
        skills_path=src.skills_path,
        agents_path=src.agents_path,
        stages=src.stages,
        handoff_prompt_path=src.handoff_prompt_path,
        continuation_prompt_path=src.continuation_prompt_path,
        group_handoff_prompt_path=src.group_handoff_prompt_path,
        context_threshold_pct=src.context_threshold_pct,
        subagent_context_threshold_pct=src.subagent_context_threshold_pct,
        context_window=src.context_window,
    )


class SessionAPI:
    """会话状态查询与控制。

    封装 Orchestrator.pool 和 session 操作方法。
    """

    def __init__(self, orchestrator: Orchestrator, config_path: Path | None = None) -> None:
        self._orch = orchestrator
        self._config_path = config_path

    @staticmethod
    def list_opencode_sessions(directory: str = "") -> list[dict[str, Any]]:
        """List importable sessions from the opencode DB, filtered by directory."""
        db_path = _resolve_opencode_db_path()
        if not db_path or not db_path.exists():
            return []
        try:
            conn = sqlite3.connect(str(db_path))
            if directory:
                rows = conn.execute(
                    """
                    SELECT id, title, time_created, time_updated,
                           tokens_input, tokens_output, cost, agent, model, directory
                    FROM session
                    WHERE (parent_id IS NULL OR parent_id = '')
                      AND (directory = ? OR directory IS NULL OR directory = '')
                    ORDER BY time_updated DESC
                """,
                    (directory,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, title, time_created, time_updated,
                           tokens_input, tokens_output, cost, agent, model, directory
                    FROM session
                    WHERE parent_id IS NULL OR parent_id = ''
                    ORDER BY time_updated DESC
                """,
                    (),
                ).fetchall()
            conn.close()
            return [
                {
                    "id": r[0],
                    "title": r[1] or "Untitled",
                    "created": r[2],
                    "updated": r[3],
                    "tokens": {"input": r[4] or 0, "output": r[5] or 0},
                    "cost": r[6] or 0.0,
                    "agent": r[7] or "default",
                    "model": r[8] or "unknown",
                    "directory": r[9] or ".",
                }
                for r in rows
            ]
        except Exception as e:
            logger.exception("list_opencode_sessions failed: %s", e)
            return []

    @staticmethod
    def _loads_recent_events(s: object) -> bool:
        """非终态 session 才查最近事件：running/paused/waiting_retry 与
        用户暂停断点的 idle（终态及自然歇息的 idle 不查，压列表查询量）。
        """
        state = getattr(s, "state", None)
        if not state:
            return False
        value = state.value
        if value in ("running", "paused", "waiting_retry"):
            return True
        return value == "idle" and getattr(s, "stop_reason", None) == "user_pause"

    def list_sessions(self) -> list[SessionInfo]:
        sessions = self._orch.pool.list_sessions()
        result = []
        for s in sessions:
            events = None
            if self._loads_recent_events(s):
                try:
                    events = self._orch.event_router.get_recent_events_sync(s.id, 10)
                except Exception:
                    logger.debug("failed to load events for session %s", s.id, exc_info=True)
                    events = []
            info = SessionInfo.from_session(
                s, events, default_reviewer_mode=self._orch.reviewer_mode
            )
            info.name = self._resolve_session_name(s.id)
            self._populate_chain(info, s.id)
            result.append(info)
        return result

    def running_loop_sessions(self) -> list[dict[str, str]]:
        """各 loop 运行时当前监视中的 session 概要（SSE loop_update 数据源）。"""
        return self._orch.running_loop_sessions()

    def get_session(self, session_id: str) -> SessionInfo | None:
        s = self._orch.pool.get(session_id)
        restored = False
        if s is None:
            s = self._orch.pool.restore_from_store(session_id)
            if s is None:
                return None
            restored = True
        self._sync_session_name(session_id)
        info = SessionInfo.from_session(s, default_reviewer_mode=self._orch.reviewer_mode)
        info.name = self._resolve_session_name(session_id)
        self._populate_chain(info, session_id)
        if restored:
            # 按需恢复读后即弃：详情读不改变池驻留，反复查看旧终态
            # 详情不会把内存池涨回去（抵消 DiscardSession 的省内存意图）
            self._orch.pool.discard(session_id)
        return info

    def effective_runtime(self, session_id: str) -> str:
        """Per-session effective runtime：session.config.runtime 为空时继承
        全局默认（与 orchestrator / create_driver 共用 resolve_runtime 规则，
        保证 Web 路由与 driver 创建侧的 runtime 判定一致）。
        """
        from workflow_controller.driver.factory import resolve_runtime

        session = self._orch.pool.get(session_id)
        if session is None:
            return resolve_runtime(self._orch.config.runtime)
        session_runtime = getattr(getattr(session, "config", None), "runtime", None)
        return resolve_runtime(session_runtime or self._orch.config.runtime)

    def transcript_context(self, session_id: str) -> dict[str, Any] | None:
        """轻量取转录定位字段（sdk_session_id/name/cwd）；session 不在池内返回 None。

        无 get_session 的名称同步与链填充副作用，供转录源定位消费。
        """
        session = self._orch.pool.get(session_id)
        if session is None:
            return None
        config = getattr(session, "config", None)
        return {
            "sdk_session_id": getattr(session, "sdk_session_id", None),
            "name": getattr(config, "name", "") or session_id,
            "cwd": getattr(config, "cwd", "") or "",
            "env_overrides": getattr(config, "env_overrides", None),
            "cli_wrapper": getattr(config, "cli_wrapper", "") or "",
        }

    def providers_snapshot(self) -> dict[str, Any]:
        """opencode provider 清单与当前模型（Session 卡片数据源）。"""
        providers, default_model = list_opencode_providers()
        current_model = self._orch.config.model or ""
        return {
            "providers": providers,
            "current_model": current_model or default_model or "",
        }

    async def test_model(self, model: str) -> dict[str, Any]:
        """用 opencode.jsonc 里的 key 实测 provider/model 连通性。"""
        if not model or "/" not in model:
            return {"success": False, "error": "Invalid model format, use provider/model"}

        provider_key, _, bare_model = model.partition("/")
        providers, _ = list_opencode_providers()
        provider_cfg = next((p for p in providers if p["provider"] == provider_key), None)
        if not provider_cfg or not provider_cfg["baseURL"]:
            return {
                "success": False,
                "error": f"Provider {provider_key} not found in opencode config",
            }

        config_path = _resolve_opencode_config_path()
        if not config_path:
            return {"success": False, "error": "opencode config not found"}
        try:
            raw = _parse_jsonc(config_path)
            api_key = (
                raw.get("provider", {}).get(provider_key, {}).get("options", {}).get("apiKey", "")
            )
        except Exception:
            return {"success": False, "error": "Failed to read opencode config"}
        if not api_key:
            return {"success": False, "error": f"No API key configured for provider {provider_key}"}

        sdk_env = {
            "ANTHROPIC_API_KEY": api_key,
            "ANTHROPIC_BASE_URL": provider_cfg["baseURL"],
            "ANTHROPIC_AUTH_TOKEN": api_key,
        }
        client = AnthropicHttpClient()
        try:
            result = await client.request(
                prompt="你好，请直接回答：你的模型名称、版本、开发公司分别是什么？",
                model=bare_model,
                env=sdk_env,
            )
        except Exception as exc:
            return {"success": False, "error": str(exc), "response": ""}
        if result.is_error:
            return {"success": False, "error": result.error, "response": ""}
        return {"success": True, "error": "", "response": result.text.strip()}

    def health_snapshot(self) -> dict[str, Any]:
        """health 端点数据源：hook server 存活与 session 计数。"""
        return {
            "hook_server": self._orch.hook_server_active,
            "sessions": len(self._orch.pool.list_sessions()),
        }

    async def start(
        self,
        project_dir: str,
        prompt: str | None = None,
        runtime: str | None = None,
        model: str | None = None,
        cli_wrapper: str | None = None,
        system_prompt: str | None = None,
        evaluator: str | None = None,
        max_iterations: int | None = None,
        provider_index: int | None = None,
        reviewer: ReviewerStartOverrides | None = None,
    ) -> dict[str, object]:
        """创建并启动一个 worker session,在 project_dir 下推进既定任务。

        同一 project_dir 重启时保留历史 events/logs。

        Args:
            project_dir: 项目目录路径
            prompt: 可选的初始 prompt，覆盖 workflow-controller.yaml 中的 starting_prompt

        Returns {"session_id": str}
        """
        _prepare_start_workspace(self._orch, project_dir)
        src = _start_config_source(self._orch, project_dir)
        _sync_start_discovery_dirs(project_dir, src)

        # 前端传入的值覆盖 yaml 内存值，不写回 yaml 文件
        reviewer = reviewer or ReviewerStartOverrides()
        _task_goal = reviewer.task_goal if reviewer.task_goal else src.reviewer.task_goal
        _loop = _start_loop_config(src, evaluator, max_iterations)
        config = self._build_start_config(
            project_dir=project_dir,
            prompt=prompt,
            runtime=runtime,
            model=model,
            cli_wrapper=cli_wrapper,
            system_prompt=system_prompt,
            provider_index=provider_index,
            reviewer=reviewer,
            loop=_loop,
            task_goal=_task_goal,
            src=src,
        )
        session = await self._orch.start_session_with_config(config)
        return {"session_id": session.id}

    async def stop(self, session_id: str) -> None:
        await self._assert_session_writable(session_id)
        await self._orch.stop_session(session_id)

    async def stop_all(self) -> None:
        sessions = self.list_sessions()
        await asyncio.gather(*(self._orch.stop_session(s.id) for s in sessions))

    async def pause(self, session_id: str) -> None:
        await self._assert_session_writable(session_id)
        await self._orch.pause_session(session_id)

    async def play(self, session_id: str) -> None:
        await self._assert_session_writable(session_id)
        await self._orch.play_session(session_id)

    async def resume(self, session_id: str) -> None:
        await self._assert_session_writable(session_id)
        await self._orch.resume_session(session_id)

    async def set_session_reviewer_mode(self, session_id: str, mode: str) -> None:
        await self._orch.set_session_reviewer_mode(session_id, mode)

    async def restart(self, session_id: str) -> None:
        await self._assert_session_writable(session_id)
        await self._orch.restart_session(session_id)

    async def emit_outcome_for_reason(self, session_id: str, reason: str | None) -> None:
        """终态改判后按停止意图补发一条 session_outcome（同 session 多结局=阶段事实）。"""
        session = self._orch.pool.get(session_id)
        if session is None:
            return
        await session.emit_outcome(outcome_kind_for_reason(reason))

    async def cancel_handoff_session(self, session_id: str) -> dict[str, Any]:
        """取消 handoff 会话：停运行时、改判 fail、落名册并补发结局事件。"""
        session = self._orch.pool.get(session_id)
        if not session:
            return {"ok": False, "error": "Session not found"}
        if session.role != "handoff_agent":
            return {"ok": False, "error": "Not a handoff session"}

        # 先脱离 loop 再停止，避免 stop 异常被 loop 捕获
        await self._orch.deactivate_session(session, pause=False)
        await self._orch.pool.stop(session_id)

        session.result = "fail"
        session.loop_status = "no"
        session.stop_reason = "user_cancel"
        await self._orch.pool.persist(session)

        # pool.stop 已发过 stopped（user_pause）结局；改判 user_cancel+fail 是新的阶段事实，须再发一条
        await self.emit_outcome_for_reason(session_id, session.stop_reason)
        return {"ok": True}

    async def delete(
        self,
        session_id: str,
        *,
        clean_reconstruct: str | None = None,
    ) -> dict[str, Any]:
        """删除 session：stop( reap worker) → 清声明的 stage 产物 → 移出 pool → 清 events。

        stop 走完整路径 ``orchestrator.stop_session``（= engine.request_stop + pool.stop +
        driver.kill），**不是**直接 ``pool.stop``——后者不设 loop engine 的 ``_stop_event``，
        worker 子进程在当前 query 被杀后会被下一轮 iteration 重新拉起，表现为 delete 后面板
        仍一直有输出（停按钮走 stop_session 所以管用，delete 绕过它所以不管用）。先 stop 再
        清产物：worker 已死才删它的文件，避免它就地重建（曾出现 delete 后 worker 把被删的产物
        又做出来、一路跑到 let_exit）。

        清 stage 产物是该 session 在 workflow-controller.yaml 声明的 artifact(= 门禁查的那批文件/
        目录)。没声明 stages 的 session 这步空操作。这让 delete→start 从干净态重跑——
        旧产物不会把进度顶到完成态、也不会让新 session 秒完成跳过真实门禁/reviewer 循环。
        未声明的文件(用户自己的源码等)不动;逃出 cwd 的 artifact 路径跳过(``_clean_stage_artifacts``)。

        Args:
            clean_reconstruct: 重建 session 磁盘清理范围。
                ``"workflow_controller"`` = 清 .workflow-controller，以及仅当重建创建时的 .opencode/.git；
                ``"all"`` = rmtree 整个 target_cwd（前端须二次确认）；
                ``None`` = 不清。recent_dirs.json 同步不受此影响。非法值抛 ValueError。

        Returns: ``{"ok": bool, "deleted_session_ids": [session_id]}``。
        """
        if clean_reconstruct is not None and clean_reconstruct not in (
            "workflow_controller",
            "all",
        ):
            raise ValueError(
                f"clean_reconstruct must be 'workflow_controller', 'all', or None; got {clean_reconstruct!r}"
            )
        deleted: list[str] = []
        await self._delete_one(
            session_id,
            clean_reconstruct=clean_reconstruct,
            deleted=deleted,
        )
        return {"ok": True, "deleted_session_ids": deleted}

    async def import_opencode_session(self, opencode_session_id: str) -> str:
        """Import an opencode session as a Workflow Controller session.

        Reads metadata + events from the opencode DB, creates a matching
        Workflow Controller Session, and stores the sdk_session_id so it can be resumed.
        """
        db_path = _resolve_opencode_db_path()
        if not db_path or not db_path.exists():
            raise ValueError(f"opencode DB not found at {db_path}")

        title, directory, model, parts = _read_opencode_import_rows(db_path, opencode_session_id)

        # Create Workflow Controller session
        provider_index, reviewer = self._default_runtime_snapshot()
        config = SessionConfig(
            name=title,
            role="worker",
            cwd=directory,
            model=model,
            initial_prompt="",
            runtime="opencode",
            provider_index=provider_index,
            reviewer=reviewer,
        )
        oid = await self._create_import_session(config, opencode_session_id)

        # Import events with original timestamps
        event_objs = _opencode_import_events(parts, oid, title, opencode_session_id)
        if event_objs:
            await self._orch.event_router.store_many(event_objs)

        # Import token/cost data from the latest session.updated.1 event
        # to populate "当前上下文" and "累计用量" cards
        await self._import_opencode_usage(db_path, opencode_session_id, oid, event_objs)

        from datetime import UTC
        from datetime import datetime as _dt

        now = _dt.now(tz=UTC).isoformat()
        await self._store_imported_session(
            oid, title=title, config=config, sdk_session_id=opencode_session_id, now=now
        )

        logger.info("Imported %s → %s (events=%d)", opencode_session_id[:12], oid, len(event_objs))
        return oid

    def claude_scan_user_homes(self) -> bool:
        """Claude 配置目录跨用户主目录扫描开关（AppConfig 字段的 API 暴露）。

        Web 侧定位转录源时经 CoreAPI 读此标志。
        """
        orch = getattr(self, "_orch", None)
        config = getattr(orch, "config", None)
        return bool(getattr(config, "claude_scan_user_homes", False))

    def list_claude_sessions(self, directory: str = "") -> list[dict[str, Any]]:
        """List Claude Code sessions across the configured and local user homes."""
        scan_user_homes = self.claude_scan_user_homes()
        jsonl_files = _claude_jsonl_files(scan_user_homes, directory)

        results: list[dict[str, Any]] = []
        for f in jsonl_files:
            sid = f.stem  # UUID filename
            title = _claude_session_title(f)
            model_name = _claude_session_model(f)
            try:
                created = int(f.stat().st_ctime * 1000)
                updated = int(f.stat().st_mtime * 1000)
            except OSError:
                created = updated = 0
            results.append(
                {
                    "id": sid,
                    "title": title,
                    "created": created,
                    "updated": updated,
                    "tokens": {"input": 0, "output": 0},
                    "cost": 0.0,
                    "agent": "claude_code",
                    "model": model_name,
                    "directory": f.parent.name.replace("-", "/"),
                }
            )
        return results

    async def import_claude_session(self, claude_session_id: str, directory: str = "") -> str:
        """Import a Claude Code session (JSONL file) as a Workflow Controller session."""
        if not directory:
            raise ValueError("directory is required for Claude Code import")

        jsonl_path = resolve_claude_session_path(
            claude_session_id,
            directory,
            scan_user_homes=self.claude_scan_user_homes(),
        )
        if jsonl_path is None:
            raise ValueError(f"Session {claude_session_id} not found")

        # Read all lines
        with open(jsonl_path) as fp:
            try:
                lines = [json.loads(line) for line in fp]
            except (json.JSONDecodeError, TypeError) as exc:
                raise ValueError("Failed to parse Claude Code session file") from exc

        if not lines:
            raise ValueError("Empty session file")

        title, cwd, model_name = _claude_session_meta(lines)

        # Create Workflow Controller session
        provider_index, reviewer = self._default_runtime_snapshot()
        config = SessionConfig(
            name=title,
            role="worker",
            cwd=cwd or directory,
            initial_prompt="",
            runtime="claude_code",
            model=model_name,
            provider_index=provider_index,
            reviewer=reviewer,
        )
        oid = await self._create_import_session(config, claude_session_id)

        # Build events
        event_objs = _claude_import_events(lines, oid, title)
        _append_claude_usage_event(event_objs, lines, oid)
        if event_objs:
            await self._orch.event_router.store_many(event_objs)

        # Persist session
        from datetime import UTC
        from datetime import datetime as _dt

        now = _dt.now(tz=UTC).isoformat()
        await self._store_imported_session(
            oid, title=title, config=config, sdk_session_id=claude_session_id, now=now
        )

        return oid

    async def inject_prompt(self, session_id: str, text: str) -> None:
        """注入手动 prompt，同时记录 LOG_MANUAL 事件。"""
        await self._assert_session_writable(session_id)
        await self._orch.inject_prompt(session_id, text)

    # ── opencode session import ─────────────────────────────────────────

    async def send_message(self, session_id: str, text: str) -> bool:
        """在同一个会话对象上注入消息，并恢复可续跑的直连会话。"""
        await self._assert_session_writable(session_id)
        session = self._orch.pool.get(session_id)
        if session is None:
            # 同步 SQLite 读不上事件循环：经 Orchestrator 缝隙走线程池
            await asyncio.to_thread(
                self._orch.restore_session_from_store,
                session_id,
            )
            session = self._orch.pool.get(session_id)
        if session is None:
            raise ValueError(f"session not found: {session_id}")
        await self._orch.inject_prompt(session_id, text)
        if session.state.value == "idle":
            await self._orch.resume_session(session_id)
            return True
        return False

    async def register_subagent_hint(
        self,
        session_id: str,
        subagent_type: str,
        hint: str,
    ) -> None:
        self._orch.register_subagent_hint(session_id, subagent_type, hint)
        await self._orch.bus.emit(
            Event.create(
                session_id,
                0,
                EventType.PROMPT_OVERLAY_REGISTERED,
                {"scope": "per_subagent_type", "target": subagent_type, "content": hint},
            )
        )

    async def clear_subagent_hint(
        self,
        session_id: str,
        subagent_type: str | None = None,
        hint_index: int | None = None,
    ) -> dict[str, Any]:
        cleared = self._orch.clear_subagent_hint(session_id, subagent_type, hint_index)
        data: dict[str, Any] = {
            "scope": "per_subagent_type",
            "index": hint_index,
            "cleared": cleared,
        }
        if subagent_type is not None:
            data["target"] = subagent_type
        await self._orch.bus.emit(
            Event.create(
                session_id,
                0,
                EventType.PROMPT_OVERLAY_CLEARED,
                data,
            )
        )
        return {"cleared": cleared}

    def get_subagent_hints(self, session_id: str) -> dict[str, list[str]]:
        return self._orch.get_subagent_hints(session_id)

    def list_subagent_types(self, session_id: str) -> list[str]:
        """Subagent types with an on-disk def for this session's project."""
        session = self._orch.pool.get(session_id)
        if session is None:
            return []
        agents_dir = Path(getattr(session.config, "cwd", "")) / ".opencode" / "agents"
        if not agents_dir.is_dir():
            return []
        return sorted(p.stem for p in agents_dir.glob("*.md"))

    def register_project_hint(
        self,
        project_dir: str,
        subagent_type: str,
        hint: str,
    ) -> None:
        self._orch.register_project_hint(project_dir, subagent_type, hint)

    def clear_project_hint(
        self,
        project_dir: str,
        subagent_type: str | None = None,
        hint_index: int | None = None,
    ) -> int:
        return self._orch.clear_project_hint(project_dir, subagent_type, hint_index)

    # ── subagent system-prompt hint (opencode overlay) ──────────────────

    def get_project_hints(self, project_dir: str) -> dict[str, list[str]]:
        return self._orch.get_project_hints(project_dir)

    def list_project_agent_types(self, project_dir: str) -> list[str]:
        return self._orch.list_project_agent_types(project_dir)

    async def register_session_instruction(
        self,
        session_id: str,
        instruction: str,
    ) -> None:
        self._orch.register_session_instruction(session_id, instruction)
        await self._orch.bus.emit(
            Event.create(
                session_id,
                0,
                EventType.PROMPT_OVERLAY_REGISTERED,
                {"scope": "global", "content": instruction},
            )
        )

    async def clear_session_instruction(
        self,
        session_id: str,
        instruction_index: int | None = None,
    ) -> dict[str, Any]:
        cleared = self._orch.clear_session_instruction(session_id, instruction_index)
        await self._orch.bus.emit(
            Event.create(
                session_id,
                0,
                EventType.PROMPT_OVERLAY_CLEARED,
                {"scope": "global", "index": instruction_index, "cleared": cleared},
            )
        )
        return {"cleared": cleared}

    # ── project-dir-keyed hints (pre-session, main Dashboard) ────────────
    # Operate on the project directory directly so hints can be registered
    # before any session exists; transferred into the session on start.

    def get_session_instructions(self, session_id: str) -> list[str]:
        return self._orch.get_session_instructions(session_id)

    def register_project_instruction(
        self,
        project_dir: str,
        instruction: str,
    ) -> None:
        self._orch.register_project_instruction(project_dir, instruction)

    def clear_project_instruction(
        self,
        project_dir: str,
        instruction_index: int | None = None,
    ) -> int:
        return self._orch.clear_project_instruction(project_dir, instruction_index)

    def get_project_instructions(self, project_dir: str) -> list[str]:
        return self._orch.get_project_instructions(project_dir)

    # ── global session instructions (opencode `instructions` overlay) ────
    # Register/clear/get global instructions appended to ALL agents' system
    # prompts (worker + every subagent) via the opencode `instructions` config
    # section. Multiple instructions accumulate; build_config_content combines
    # them into one temp file referenced by OPENCODE_CONFIG_CONTENT on the next
    # query() (iteration boundary). Non-invasive (disk .md untouched),
    # reversible, session-scoped. Complementary to per-type subagent hints.

    def set_reviewer_mode(self, mode: str) -> None:
        self._orch.set_reviewer_mode(mode)

    def _default_runtime_snapshot(
        self,
        runtime: str | None = None,
    ) -> tuple[int | None, SessionReviewerConfig]:
        from workflow_controller.driver.factory import resolve_runtime

        rotator = getattr(self._orch, "auth_rotator", None)
        resolved_runtime = resolve_runtime(runtime or self._orch.config.runtime)
        provider_index = (
            rotator.get_default_provider_index(resolved_runtime)
            if rotator is not None and resolved_runtime != "codex_cli"
            else None
        )
        reviewer_provider_index = (
            rotator.get_default_provider_index(capability="reviewer")
            if rotator is not None
            else None
        )
        reviewer_config = self._orch.config.reviewer
        mode = getattr(self._orch, "reviewer_mode", "auto")
        if mode not in ("auto", "manual"):
            mode = "auto"
        reviewer = SessionReviewerConfig(
            enabled=reviewer_config.enabled,
            mode=mode,
            model=reviewer_config.model,
            provider_index=reviewer_provider_index,
            timeout_seconds=reviewer_config.timeout_seconds,
            thinking_budget_tokens=reviewer_config.thinking_budget_tokens,
            thinking_mode=reviewer_config.thinking_mode,
            task_goal=reviewer_config.task_goal,
        )
        return provider_index, reviewer

    def _start_reviewer_snapshot(
        self,
        project_dir: str,
        src: AppConfig,
        reviewer: ReviewerStartOverrides,
        default_reviewer: SessionReviewerConfig,
        task_goal: str,
    ) -> SessionReviewerConfig:
        """start 实际生效的 reviewer 配置：启动页覆盖优先，缺项回落项目/全局值。"""
        effective_reviewer_model = reviewer.model or src.reviewer.model
        if reviewer.thinking_mode not in (
            None,
            "",
            "disabled",
            "enabled",
            "adaptive",
            "omitted",
        ):
            raise ValueError(f"Unsupported reviewer thinking mode: {reviewer.thinking_mode!r}")
        thinking = resolve_reviewer_thinking(
            project_dir,
            self._orch.config.reviewer,
            budget_override=reviewer.thinking_budget_tokens,
            mode_override=reviewer.thinking_mode,
        )
        reviewer_mode = getattr(self._orch, "reviewer_mode", "auto")
        if reviewer_mode not in ("auto", "manual"):
            reviewer_mode = "auto"
        return SessionReviewerConfig(
            enabled=reviewer.enabled if reviewer.enabled is not None else src.reviewer.enabled,
            mode=reviewer_mode,
            model=effective_reviewer_model,
            provider_index=(
                reviewer.provider_index
                if reviewer.provider_index is not None
                else default_reviewer.provider_index
            ),
            timeout_seconds=src.reviewer.timeout_seconds,
            thinking_mode=thinking.mode,
            thinking_budget_tokens=thinking.budget_tokens,
            task_goal=task_goal,
        )

    def _build_start_config(
        self,
        *,
        project_dir: str,
        prompt: str | None,
        runtime: str | None,
        model: str | None,
        cli_wrapper: str | None,
        system_prompt: str | None,
        provider_index: int | None,
        reviewer: ReviewerStartOverrides,
        loop: Any,
        task_goal: str,
        src: AppConfig,
    ) -> SessionConfig:
        """解析 start 的有效值（runtime/wrapper/model/provider/reviewer 快照）并组装配置。"""
        effective_runtime = runtime or src.runtime
        effective_cli_wrapper = src.cli_wrapper if cli_wrapper is None else cli_wrapper
        if effective_cli_wrapper not in ("", "cpx"):
            raise ValueError(f"Unsupported CLI wrapper: {effective_cli_wrapper!r}")
        default_provider_index, default_reviewer = self._default_runtime_snapshot(effective_runtime)
        reviewer_snapshot = self._start_reviewer_snapshot(
            project_dir, src, reviewer, default_reviewer, task_goal
        )
        effective_model = model or src.model
        effective_provider_index = (
            provider_index if provider_index is not None else default_provider_index
        )
        model_pinned = _start_model_pinned(self._orch, effective_model, effective_provider_index)
        return _start_session_config(
            project_dir=project_dir,
            prompt=prompt,
            system_prompt=system_prompt,
            runtime=effective_runtime,
            cli_wrapper=effective_cli_wrapper,
            model=effective_model,
            provider_index=effective_provider_index,
            model_pinned=model_pinned,
            src=src,
            reviewer=reviewer_snapshot,
            loop=loop,
            task_goal=task_goal,
        )

    async def _store_imported_session(
        self,
        oid: str,
        *,
        title: str,
        config: Any,
        sdk_session_id: str,
        now: str,
    ) -> None:
        """导入会话的会话记录落库（opencode/Claude 两条导入路径共用）。"""
        import json as _json

        await self._orch.event_router.store_session(
            oid,
            name=title,
            role="worker",
            state="idle",
            loop_status="no",
            stop_reason="imported",
            loop_hold=None,
            config_json=_json.dumps(config.model_dump(), default=str),
            sdk_session_id=sdk_session_id,
            created_at=now,
            last_activity=now,
            current_iteration=0,
            birth_handoff_doc=None,
            result="success",
            last_error=None,
        )

    async def _create_import_session(self, config: SessionConfig, sdk_session_id: str) -> str:
        """按导入语义创建 session（idle/imported 终态，不接 loop），返回其 id。"""
        session = await self._orch.pool.create(config)
        session.sdk_session_id = sdk_session_id
        session.loop_status = "no"
        session.stop_reason = "imported"
        return session.id

    async def _import_opencode_usage(
        self,
        db_path: Path,
        opencode_session_id: str,
        oid: str,
        event_objs: list[Event],
    ) -> None:
        """从最新 session.updated.1 事件导入用量/花费（「当前上下文」「累计用量」卡片数据源）。"""
        import uuid as _uuid
        from datetime import UTC
        from datetime import datetime as _dt

        conn2 = sqlite3.connect(str(db_path))
        update_rows = conn2.execute(
            "SELECT data FROM event WHERE aggregate_id=? AND type='session.updated.1' ORDER BY seq DESC LIMIT 1",
            (opencode_session_id,),
        ).fetchall()
        conn2.close()
        if not update_rows:
            return
        try:
            upd = json.loads(update_rows[0][0])
            info = upd.get("info", upd.get("data", {}))
            tokens = info.get("tokens", {})
            cost = info.get("cost", 0)
            num_turns = len([e for e in event_objs if e.event_type == EventType.ASSISTANT_MESSAGE])
            step_finish_event = Event(
                id=_uuid.uuid4().hex,
                session_id=oid,
                iteration=1,
                timestamp=_dt.now(tz=UTC),
                event_type=EventType.STEP_FINISH,
                data=dict(_step_finish_data(oid, num_turns, _opencode_usage_payload(tokens), cost)),
            )
            await self._orch.event_router.store(step_finish_event)
            logger.info(
                "Imported token/cost data for %s: cost=%.4f, input=%d, output=%d",
                oid,
                cost,
                tokens.get("input", 0),
                tokens.get("output", 0),
            )
        except Exception as e:
            logger.warning("Failed to import token/cost data: %s", e, exc_info=True)

    def _resolve_session_name(self, session_id: str) -> str:
        s = self._orch.pool.get(session_id)
        if s is None:
            return session_id[:12]
        name = s.name
        if _is_design_md_compliant_name(name):
            return name
        sdk_id = getattr(s, "sdk_session_id", None)
        title = _get_opencode_session_title(sdk_id)
        return title if title else name

    # ── project-dir-keyed global instructions (pre-session, Dashboard) ────
    # Operate on the project directory directly so instructions can be
    # registered before any session exists; transferred into the session on
    # start.

    def _sync_session_name(self, session_id: str) -> None:
        """Sync session name with opencode title if needed.

        For initial sessions (non-design.md-compliant names), update the session
        name in the database to match the opencode title. This ensures that
        sessions created with temporary names (like timestamps) get proper names
        once opencode generates a title.

        Handoff/continuation sessions keep their structured names.
        """
        s = self._orch.pool.get(session_id)
        if s is None:
            return
        name = s.name
        if _is_design_md_compliant_name(name):
            return  # Keep structured names as-is
        sdk_id = getattr(s, "sdk_session_id", None)
        title = _get_opencode_session_title(sdk_id)
        if title and title != name:
            try:
                self._orch.event_router.update_session_name_sync(session_id, title)
                s.name = title
            except Exception:
                logger.debug("failed to sync session name for %s", session_id, exc_info=True)

    def _populate_chain(self, info: SessionInfo, session_id: str) -> None:
        pred = self._orch.event_router.get_predecessor_sync(session_id)
        if pred:
            info.predecessor = {
                "id": pred["from_session_id"],
                "name": self._resolve_session_name(pred["from_session_id"]),
            }
        succs = self._orch.event_router.get_successors_sync(session_id)
        info.successors = [
            {"id": succ["to_session_id"], "name": self._resolve_session_name(succ["to_session_id"])}
            for succ in succs
        ]

    async def _assert_session_writable(self, session_id: str) -> None:
        """单一写者守卫：workflow 成员拒绝直通并指路 run 面；其余成员零改动放行。"""
        aff = self._orch.affiliation.resolve(session_id)
        if aff is None:
            return
        try:
            run = await self._orch.run_store.get(aff.run_id)
        except RunNotFoundError:
            return  # 记录已无此 run：无 run 平面可指路，按无归属直通
        if run.kind == RunKind.WORKFLOW:
            raise WorkflowSessionOperationError(session_id, aff.run_id)

    def _session_birth_traceback(
        self, session_id: str, session: object | None
    ) -> dict[str, Any] | None:
        """删除时取 birth_traceback：pool 命中直接取；否则扫 sessions 表预解析
        （须在 delete_session 删行前完成；读失败仅记日志，不阻断删除）。
        """
        if session is not None:
            pm = getattr(session, "plugin_metadata", None) or {}
            bt = (pm.get("agent_traceback") or {}).get("birth_traceback")
            if isinstance(bt, dict):
                return bt
        try:
            return self._orch.event_router.read_session_birth_traceback(session_id)
        except Exception:
            logger.debug(
                "delete: read_session_birth_traceback(%s) failed (non-fatal)",
                session_id[:12],
                exc_info=True,
            )
            return None

    async def _delete_one(
        self,
        session_id: str,
        *,
        clean_reconstruct: str | None = None,
        deleted: list[str],
    ) -> None:
        """删单个 session。``deleted`` 收集已删 id 供响应体聚合。

        session 不在 pool 时（重启孤儿），从 event_router 预解析 birth_traceback
        （须在 sessions 行被删除前），注入事件 data 让插件清理能拿到 fork_root。
        """
        session = self._orch.pool.get(session_id)
        # 先停 worker（完整 stop 路径 reap 整个进程组），再清它的产物。
        await self._orch.stop_session(session_id)
        if session is not None:
            cwd = getattr(session.config, "cwd", None)
            stages = list(getattr(session.config, "stages", None) or [])
            if cwd and stages:
                removed = _clean_stage_artifacts(cwd, stages)
                if removed:
                    logger.info(
                        "delete: cleaned %d stage artifacts for %s: %s",
                        len(removed),
                        session_id[:12],
                        removed,
                    )
        birth = self._session_birth_traceback(session_id, session)
        self._orch.pool.remove(session_id)
        # sessions 表行已删，归属随行回收——隐式连 run 行，显式成员行保留。
        await self._orch.reclaim_deleted_session_affiliation(session_id)
        # 必须在 delete_session 之前 emit：bus.emit 会落盘事件，若放在 delete_session
        # 之后，事件会重新写回刚清空的 DB 留下孤儿行。先 emit 再 delete_session：
        # 事件落入正确项目库，随即被清掉。拦截器（如 file_provenance 的脱敏）
        # 在此内联完成 own 状态清理；birth_traceback / clean_reconstruct 为
        # 溯源载荷（消费面在路由/清理侧），fork tempdir 孤儿由内核启动期
        # residue reaper 承接。
        #
        # birth_traceback 经事件 data 直接透传：session 此时已从 pool 移除，
        # 事件消费方无法再经 pool.get 拿到它。
        await self._orch.bus.emit(
            Event.create(
                session_id=session_id,
                iteration=0,
                event_type="session_delete",
                data={
                    "session_id": session_id,
                    "birth_traceback": birth,
                    "clean_reconstruct": clean_reconstruct,
                },
            )
        )
        # 清事件 + metadata（含刚由 bus.emit 落盘的 session_delete 事件行）。
        self._orch.event_router.delete_session(session_id)
        deleted.append(session_id)


def _read_opencode_import_rows(
    db_path: Path, opencode_session_id: str
) -> tuple[str, str, str, list[Any]]:
    """读 opencode DB 的 session 行与消息 parts（含角色）；session 不存在抛 ValueError。"""
    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT id, title, time_created, time_updated, directory, model FROM session WHERE id=?",
        (opencode_session_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise ValueError(f"Session {opencode_session_id[:12]} not found")

    parts = conn.execute(
        """
        SELECT p.data, p.time_created, json_extract(m.data, '$.role') as msg_role
        FROM part p JOIN message m ON p.message_id = m.id
        WHERE p.session_id=? ORDER BY p.time_created ASC
    """,
        (opencode_session_id,),
    ).fetchall()
    conn.close()

    title = row[1] or "Imported Session"
    directory = row[4] or "."
    model = _parse_opencode_model(row[5]) or ""
    return title, directory, model, parts


def _opencode_part_event(
    d: dict[str, Any], pt: str, is_user: bool, oid: str
) -> tuple[EventType | None, dict[str, Any] | None]:
    """text/reasoning part → (事件类型, 数据)；无对应事件返回 (None, None)。"""
    if pt == "text":
        if is_user:
            return EventType.LOG_MANUAL, {"prompt": d.get("text", ""), "session_id": oid}
        return (
            EventType.ASSISTANT_MESSAGE,
            {
                "content": [{"type": "text", "text": d.get("text", "")}],
                "role": "assistant",
            },
        )
    if pt == "reasoning" and not is_user:
        return (
            EventType.ASSISTANT_MESSAGE,
            {
                "content": [{"type": "thinking", "thinking": d.get("text", "")[:500]}],
                "role": "assistant",
            },
        )
    return None, None


def _opencode_task_envelope(tool_output: Any) -> tuple[str, str]:
    """从 task 工具输出提取（子会话 id, 结果文本）；非字符串输出返回空值。"""
    sub_sid = ""
    sub_content = ""
    if not isinstance(tool_output, str):
        return sub_sid, sub_content
    import re as _re

    m = _re.search(r'<task\s+id="([^"]+)"', tool_output)
    if m:
        sub_sid = m.group(1)
    m2 = _re.search(r"<task_result>\s*(.*?)\s*</task_result>", tool_output, _re.DOTALL)
    if m2:
        sub_content = m2.group(1)
    return sub_sid, sub_content


def _append_opencode_subagent_events(
    event_objs: list[Event],
    tool_input: Any,
    tool_output: Any,
    call_id: str,
    tc: Any,
    oid: str,
) -> None:
    """task 工具 → SUBAGENT_START/STOP 事件对（id 取子会话，缺省用调用 id）。"""
    import uuid as _uuid
    from datetime import UTC
    from datetime import datetime as _dt

    sub_input = tool_input if isinstance(tool_input, dict) else {}
    agent_type = sub_input.get("subagent_type", "general")
    sub_sid, sub_content = _opencode_task_envelope(tool_output)
    event_objs.append(
        Event(
            id=_uuid.uuid4().hex,
            session_id=oid,
            iteration=1,
            timestamp=_dt.fromtimestamp(tc / 1000 + 0.001, tz=UTC),
            event_type=EventType.SUBAGENT_START,
            data={
                "agent_id": sub_sid or call_id,
                "agent_type": agent_type,
            },
        )
    )
    event_objs.append(
        Event(
            id=_uuid.uuid4().hex,
            session_id=oid,
            iteration=1,
            timestamp=_dt.fromtimestamp(tc / 1000 + 0.002, tz=UTC),
            event_type=EventType.SUBAGENT_STOP,
            data={
                "agent_id": sub_sid or call_id,
                "agent_type": agent_type,
                "content": sub_content[:2000] if sub_content else "",
                "total_tokens": 0,
            },
        )
    )


def _append_opencode_tool_events(
    event_objs: list[Event], d: dict[str, Any], tc: Any, oid: str
) -> None:
    """tool part → TOOL_USE +（task 工具的）SUBAGENT start/stop + TOOL_RESULT 事件序列。"""
    import uuid as _uuid
    from datetime import UTC
    from datetime import datetime as _dt

    tool_name = d.get("tool", "")
    state = d.get("state", {})
    status = state.get("status", "")
    tool_input = state.get("input", {})
    tool_output = state.get("output", "")
    call_id = d.get("callID", d.get("id", ""))
    event_objs.append(
        Event(
            id=_uuid.uuid4().hex,
            session_id=oid,
            iteration=1,
            timestamp=_dt.fromtimestamp(tc / 1000, tz=UTC),
            event_type=EventType.TOOL_USE,
            data={"name": tool_name, "input": tool_input, "id": call_id},
        )
    )
    if tool_name == "task":
        _append_opencode_subagent_events(event_objs, tool_input, tool_output, call_id, tc, oid)
    event_objs.append(
        Event(
            id=_uuid.uuid4().hex,
            session_id=oid,
            iteration=1,
            timestamp=_dt.fromtimestamp(tc / 1000 + 0.003, tz=UTC),
            event_type=EventType.TOOL_RESULT,
            data={
                "content": str(tool_output)[:500] if tool_output else "",
                "tool_use_id": call_id,
                "is_error": status == "error",
            },
        )
    )


def _append_opencode_iteration_start(
    event_objs: list[Event], tc: Any, oid: str, opencode_session_id: str
) -> None:
    """补发 ITERATION_START 恢复标识事件（序列中已有则跳过）。"""
    import uuid as _uuid
    from datetime import UTC
    from datetime import datetime as _dt

    if any(e.event_type == EventType.ITERATION_START for e in event_objs):
        return
    event_objs.append(
        Event(
            id=_uuid.uuid4().hex,
            session_id=oid,
            iteration=1,
            timestamp=_dt.fromtimestamp(tc / 1000 - 0.001, tz=UTC),
            event_type=EventType.ITERATION_START,
            data={
                "subtype": "init",
                "session_id": opencode_session_id,
                "model": "",
                "cwd": "",
            },
        )
    )


def _append_opencode_step_finish(
    event_objs: list[Event], d: dict[str, Any], tc: Any, oid: str, opencode_session_id: str
) -> None:
    """step-finish part → STEP_FINISH 事件；并补发 ITERATION_START（若序列尚无）。"""
    import uuid as _uuid
    from datetime import UTC
    from datetime import datetime as _dt

    tokens = d.get("tokens", {})
    reason = d.get("reason", "")
    sf_data = _step_finish_data(
        oid, 1, _opencode_usage_payload(tokens), d.get("cost", 0), reason == "error"
    )
    sf_ts = _dt.fromtimestamp(tc / 1000, tz=UTC)
    event_objs.append(
        Event(
            id=_uuid.uuid4().hex,
            session_id=oid,
            iteration=1,
            timestamp=sf_ts,
            event_type=EventType.STEP_FINISH,
            data=dict(sf_data),
        )
    )
    # Also emit ITERATION_START so _register_session-style consumers
    # treating ITERATION_START as "the resume-id event" work against
    # imported history too. Mirrors opencode driver's id_ready emit.
    _append_opencode_iteration_start(event_objs, tc, oid, opencode_session_id)


def _opencode_import_events(
    parts: list[Any], oid: str, title: str, opencode_session_id: str
) -> list[Event]:
    """opencode parts → 事件序列：首条 session_started + 逐 part 事件（保留原时间戳）。"""
    import uuid as _uuid
    from datetime import UTC
    from datetime import datetime as _dt

    event_objs: list[Event] = []
    if parts:
        tc = parts[0][1] / 1000
        event_objs.append(
            Event(
                id=_uuid.uuid4().hex,
                session_id=oid,
                iteration=0,
                timestamp=_dt.fromtimestamp(tc, tz=UTC),
                event_type=EventType.LOG_AUTO,
                data={"action": "session_started", "name": title, "role": "worker"},
            )
        )
    for data_str, tc, msg_role in parts:
        try:
            d = json.loads(data_str)
        except (json.JSONDecodeError, TypeError):
            continue
        pt = d.get("type", "")
        is_user = msg_role == "user"
        if pt == "tool":
            _append_opencode_tool_events(event_objs, d, tc, oid)
            continue
        if pt == "step-finish":
            _append_opencode_step_finish(event_objs, d, tc, oid, opencode_session_id)
            continue
        et, data = _opencode_part_event(d, pt, is_user, oid)
        if et and data:
            event_objs.append(
                Event(
                    id=_uuid.uuid4().hex,
                    session_id=oid,
                    iteration=1,
                    timestamp=_dt.fromtimestamp(tc / 1000, tz=UTC),
                    event_type=et,
                    data=data,
                )
            )
    return event_objs


def _opencode_usage_payload(tokens: dict[str, Any]) -> dict[str, Any]:
    """opencode tokens → STEP_FINISH 的 usage 子字典。"""
    return {
        "input_tokens": tokens.get("input", 0),
        "output_tokens": tokens.get("output", 0),
        "cache_creation_input_tokens": tokens.get("cache", {}).get("write", 0),
        "cache_read_input_tokens": tokens.get("cache", {}).get("read", 0),
    }


def _step_finish_data(
    session_id: str,
    num_turns: int,
    usage: dict[str, Any],
    total_cost_usd: Any,
    is_error: bool = False,
) -> dict[str, Any]:
    """STEP_FINISH 事件 data 体（两条导入路径共用）。"""
    return {
        "session_id": session_id,
        "num_turns": num_turns,
        "total_cost_usd": total_cost_usd,
        "duration_ms": 0,
        "is_error": is_error,
        "usage": usage,
        "modelUsage": {},
    }


def _glob_jsonl_into(search_dir: Path, pattern: str, files: dict[Path, Path]) -> None:
    """单个配置目录下的 .jsonl 收集进 files（按 resolve 去重）；目录不可访则跳过。"""
    try:
        for path in search_dir.glob(pattern):
            if path.is_file():
                files[path.resolve()] = path
    except OSError:
        pass


def _claude_jsonl_files(scan_user_homes: bool, directory: str) -> list[Path]:
    """收集各配置目录下的 Claude 会话 .jsonl（mtime 降序；全量模式截前 200）。"""
    files: dict[Path, Path] = {}
    for config_dir in claude_config_dirs(scan_user_homes=scan_user_homes):
        projects_dir = config_dir / "projects"
        search_dir = projects_dir / directory.replace("/", "-") if directory else projects_dir
        pattern = "*.jsonl" if directory else "*/*.jsonl"
        _glob_jsonl_into(search_dir, pattern, files)
    jsonl_files = sorted(
        files.values(),
        key=_path_mtime,
        reverse=True,
    )
    if not directory:
        jsonl_files = jsonl_files[:200]
    return jsonl_files


def _first_jsonl_user_title(fp: TextIO) -> str:
    """逐行扫描 jsonl：首条 user 消息的非空文本截 80 字符；无 user 行返回空串。"""
    for line in fp:
        d = json.loads(line)
        if d.get("type") == "user":
            return _first_user_message_text(d).strip()[:80]
    return ""


def _claude_session_title(f: Path) -> str:
    """会话文件里第一条非空 user 消息（截 80 字符）作标题，解析失败保留默认。"""
    try:
        with open(f) as fp:
            return _first_jsonl_user_title(fp) or "Untitled"
    except Exception:
        logger.debug("会话文件 %s 标题解析失败，保留默认标题", f)
        return "Untitled"


def _first_jsonl_assistant_model(fp: TextIO) -> str:
    """逐行扫描 jsonl：首条含模型名的 assistant 消息的模型名；无则空串。"""
    for line in fp:
        d = json.loads(line)
        if d.get("type") == "assistant":
            msg = d.get("message", {})
            if isinstance(msg, dict) and msg.get("model"):
                return str(msg["model"])
    return ""


def _claude_session_model(f: Path) -> str:
    """会话文件里第一条 assistant 消息的模型名，解析失败保留 unknown。"""
    try:
        with open(f) as fp:
            return _first_jsonl_assistant_model(fp) or "unknown"
    except Exception:
        logger.debug("会话文件 %s 模型名解析失败，保留 unknown", f)
        return "unknown"


def _claude_session_meta(lines: list[dict[str, Any]]) -> tuple[str, str, str]:
    """从会话行提取（标题, cwd, 模型名）：标题取首条 user 消息、cwd 取首条含值行、模型取首条 assistant。"""
    title = "Imported Session"
    cwd = ""
    for entry in lines:
        if entry.get("cwd"):
            cwd = entry["cwd"]
        if entry.get("type") == "user":
            text = _first_user_message_text(entry)
            if text.strip():
                title = text.strip()
                break
    model_name = "unknown"
    for entry in lines:
        if entry.get("type") == "assistant":
            msg = entry.get("message", {})
            if isinstance(msg, dict) and msg.get("model"):
                model_name = msg["model"]
                break
    return title, cwd, model_name


def _claude_user_event(
    line: dict[str, Any], oid: str
) -> tuple[EventType | None, dict[str, Any] | None]:
    """user 行 → LOG_MANUAL 事件候选；无非空文本返回 (None, None)。"""
    msg = line.get("message", {}) if isinstance(line.get("message"), dict) else {}
    content = msg.get("content", "")
    text = ""
    if isinstance(content, list):
        text = "".join(
            b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
        )
    elif isinstance(content, str):
        text = content
    if text.strip():
        return EventType.LOG_MANUAL, {"prompt": text, "session_id": oid}
    return None, None


def _claude_assistant_content(
    event_objs: list[Event], content_blocks: list[Any], ts: Any, oid: str
) -> list[dict[str, Any]]:
    """遍历 assistant 内容块：文本块聚合为消息内容，tool_use/tool_result 即时追加为独立事件。"""
    import uuid as _uuid

    event_content: list[dict[str, Any]] = []
    for b in content_blocks:
        if not isinstance(b, dict):
            continue
        bt = b.get("type", "")
        if bt == "text":
            event_content.append({"type": "text", "text": b.get("text", "")})
        elif bt == "tool_use":
            # Emit as separate tool_use event
            event_objs.append(
                Event(
                    id=_uuid.uuid4().hex,
                    session_id=oid,
                    iteration=1,
                    timestamp=ts,
                    event_type=EventType.TOOL_USE,
                    data={
                        "name": b.get("name", "?"),
                        "input": b.get("input", {}),
                        "id": b.get("id", ""),
                    },
                )
            )
        elif bt == "tool_result":
            event_objs.append(
                Event(
                    id=_uuid.uuid4().hex,
                    session_id=oid,
                    iteration=1,
                    timestamp=ts,
                    event_type=EventType.TOOL_RESULT,
                    data={
                        "content": str(b.get("content", ""))[:500],
                        "tool_use_id": b.get("tool_use_id", ""),
                    },
                )
            )
    return event_content


def _append_claude_assistant_events(
    event_objs: list[Event], line: dict[str, Any], ts: Any, oid: str
) -> tuple[EventType | None, dict[str, Any] | None]:
    """assistant 行 → tool_use/tool_result 独立事件 + 文本块聚合为 ASSISTANT_MESSAGE 候选。"""
    msg = line.get("message", {}) if isinstance(line.get("message"), dict) else {}
    content_blocks = msg.get("content", [])
    if not isinstance(content_blocks, list):
        return None, None

    event_content = _claude_assistant_content(event_objs, content_blocks, ts, oid)
    if event_content:
        return (
            EventType.ASSISTANT_MESSAGE,
            {
                "content": event_content,
                "role": "assistant",
            },
        )
    return None, None


def _claude_import_events(lines: list[dict[str, Any]], oid: str, title: str) -> list[Event]:
    """Claude 会话行 → 事件序列：首条 session_started + 逐行 user/assistant 事件。"""
    import uuid as _uuid
    from datetime import UTC
    from datetime import datetime as _dt

    event_objs: list[Event] = []
    base_ts = lines[0].get("timestamp", _dt.now(tz=UTC).isoformat())
    try:
        base_dt = _dt.fromisoformat(base_ts.replace("Z", "+00:00"))
    except ValueError:
        base_dt = _dt.now(tz=UTC)

    event_objs.append(
        Event(
            id=_uuid.uuid4().hex,
            session_id=oid,
            iteration=0,
            timestamp=base_dt,
            event_type=EventType.LOG_AUTO,
            data={"action": "session_started", "name": title, "role": "worker"},
        )
    )

    for line in lines:
        t = line.get("type", "")
        ts_str = line.get("timestamp", "")
        try:
            ts = _dt.fromisoformat(ts_str.replace("Z", "+00:00")) if ts_str else base_dt
        except ValueError:
            ts = base_dt

        if t == "user":
            et, data = _claude_user_event(line, oid)
        elif t == "assistant":
            et, data = _append_claude_assistant_events(event_objs, line, ts, oid)
        else:
            et, data = None, None

        if et and data:
            event_objs.append(
                Event(
                    id=_uuid.uuid4().hex,
                    session_id=oid,
                    iteration=1,
                    timestamp=ts,
                    event_type=et,
                    data=data,
                )
            )
    return event_objs


def _append_claude_usage_event(
    event_objs: list[Event], lines: list[dict[str, Any]], oid: str
) -> None:
    """尾部 usage 数据 → STEP_FINISH 事件（Phase B 瘦身 RESULT 不带 usage，
    usage 只在 STEP_FINISH，见 docs/result-event-analysis.md §10 / §11）。"""
    import uuid as _uuid
    from datetime import UTC
    from datetime import datetime as _dt

    last_usage: dict[str, Any] = {}
    for line in reversed(lines):
        if line.get("type") == "assistant":
            msg = line.get("message", {})
            if isinstance(msg, dict) and msg.get("usage"):
                last_usage = msg["usage"]
                break
    if not last_usage:
        return
    last_ts = event_objs[-1].timestamp if event_objs else _dt.now(tz=UTC)
    event_objs.append(
        Event(
            id=_uuid.uuid4().hex,
            session_id=oid,
            iteration=1,
            timestamp=last_ts,
            event_type=EventType.STEP_FINISH,
            data=dict(_step_finish_data(oid, 1, last_usage, 0.0)),
        )
    )


def _parse_jsonc(path: Path) -> dict[str, Any]:
    """Parse a JSONC file (JSON with // and /* */ comments)."""
    import re

    text = path.read_text(encoding="utf-8")
    # Remove // comments only outside strings (preceded by comma, brace, bracket, whitespace, or at line start)
    text = re.sub(r"(^|\s|,|\{|\[|\])\s*//.*$", r"\1", text, flags=re.MULTILINE)
    # Remove /* */ comments
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    return json.loads(text)


def _resolve_opencode_config_path() -> Path | None:
    """Resolve the opencode config file path (cross-platform)."""
    import sys

    # Check OPENCODE_CONFIG env first
    env_path = os.environ.get("OPENCODE_CONFIG")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p
    # Platform-specific default
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", ""))
    else:
        base = Path.home() / ".config"
    config_path = base / "opencode" / "opencode.jsonc"
    return config_path if config_path.exists() else None


def list_opencode_providers() -> tuple[list[dict[str, Any]], str | None]:
    """Read opencode.jsonc and return (providers_list, current_default_model).

    每个 provider 条目含 provider、name、baseURL 与 models（模型名列表）。
    """
    config_path = _resolve_opencode_config_path()
    if not config_path:
        return [], None
    try:
        data = _parse_jsonc(config_path)
    except Exception:
        logger.warning("Failed to parse opencode config: %s", config_path)
        return [], None

    providers_raw = data.get("provider", {})
    result: list[dict[str, Any]] = []
    for provider_key, provider_cfg in (providers_raw or {}).items():
        if not isinstance(provider_cfg, dict):
            continue
        models = list((provider_cfg.get("models") or {}).keys())
        result.append(
            {
                "provider": provider_key,
                "name": provider_cfg.get("name", provider_key),
                "baseURL": (provider_cfg.get("options") or {}).get("baseURL", ""),
                "models": models,
            }
        )
    # Default model from provider list (first provider's first model with "/")
    default_model = None
    for p in result:
        for m in p["models"]:
            default_model = f"{p['provider']}/{m}"
            break
        if default_model:
            break
    return result, default_model
