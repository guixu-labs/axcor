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

"""Agent traceback plugin — FastAPI web routes.

暴露检查点列表/重放/文件预览端点（web Dashboard 用）。路由在 web app 的
SPA catch-all 之前 include，与核心 /api/* 路由并列。

路由在 app 启动时注册一次，闭包持有按名解析插件的回调。每个请求经该回调
解析当前已加载的 agent_traceback 插件实例——插件运行时重载后 API 端点仍
落到活跃实例上。端点实现是模块级私有函数，build_router 只留薄包装注册。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse

if TYPE_CHECKING:
    from collections.abc import Callable

    from workflow_controller.plugins.agent_traceback.plugin import AgentTracebackPlugin

logger = logging.getLogger(__name__)


def build_router(
    resolve_plugin: Callable[[str], AgentTracebackPlugin | None],
) -> APIRouter:
    """构造 agent_traceback 插件的 FastAPI router（按名解析活跃实例）。"""
    router = APIRouter()

    @router.get("/api/sessions/{session_id}/checkpoints")
    async def list_checkpoints(session_id: str) -> dict[str, Any]:
        return await _list_checkpoints(resolve_plugin, session_id)

    @router.get("/api/sessions/{session_id}/traceback/runtime")
    async def traceback_runtime(session_id: str) -> dict[str, str]:
        return await _traceback_runtime(resolve_plugin, session_id)

    @router.post("/api/sessions/{session_id}/traceback")
    async def traceback_session(
        session_id: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        return await _traceback_session(resolve_plugin, session_id, body)

    @router.get("/api/sessions/{session_id}/checkpoints/{checkpoint_id}/files")
    async def list_checkpoint_files(
        session_id: str,
        checkpoint_id: str,
        path: str = "",
    ) -> dict[str, Any]:
        return await _list_checkpoint_files(resolve_plugin, session_id, checkpoint_id, path)

    @router.get("/api/sessions/{session_id}/checkpoints/{checkpoint_id}/file")
    async def read_checkpoint_file(
        session_id: str,
        checkpoint_id: str,
        path: str = "",
    ) -> dict[str, Any]:
        return await _read_checkpoint_file(resolve_plugin, session_id, checkpoint_id, path)

    @router.post("/api/sessions/import-db")
    async def import_db_session(
        body: dict[str, Any],
        request: Request,
    ) -> dict[str, Any]:
        return await _import_db_session(resolve_plugin, body, request)

    @router.get("/api/sessions/import-db/log")
    async def get_import_log(target_cwd: str = "") -> Any:
        return _get_import_log(target_cwd)

    return router


async def _list_checkpoints(
    resolve_plugin: Callable[[str], AgentTracebackPlugin | None],
    session_id: str,
) -> dict[str, Any]:
    """List traceback checkpoints for a session."""
    plugin = resolve_plugin("agent_traceback")
    if plugin is None:
        return {"checkpoints": []}
    return {"checkpoints": plugin.list_checkpoints(session_id)}


async def _traceback_runtime(
    resolve_plugin: Callable[[str], AgentTracebackPlugin | None],
    session_id: str,
) -> dict[str, str]:
    plugin = resolve_plugin("agent_traceback")
    if plugin is None:
        return {"runtime": ""}
    return {"runtime": await plugin.replay_runtime(session_id)}


_TRACEBACK_OVERRIDE_KEYS = (
    "cli_wrapper",
    "model",
    "provider_index",
    "reviewer_model",
    "reviewer_provider_index",
    "reviewer_enabled",
    "reviewer_thinking_enabled",
    "reviewer_thinking_budget_tokens",
    "reviewer_max_tokens",
    "system_prompt",
    "evaluator",
    "max_iterations",
    "reviewer_task_goal",
    "initial_prompt",
)


def _clean_traceback_overrides(overrides_raw: Any) -> dict[str, Any] | None:
    """只透传白名单键；空值不覆盖继承值，cli_wrapper 空串例外表示关闭。"""
    if not isinstance(overrides_raw, dict) or not overrides_raw:
        return None
    cleaned = {
        k: v
        for k, v in overrides_raw.items()
        if k in _TRACEBACK_OVERRIDE_KEYS and v is not None and (k == "cli_wrapper" or v != "")
    }
    return cleaned or None


async def _traceback_session(
    resolve_plugin: Callable[[str], AgentTracebackPlugin | None],
    session_id: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    """Replay from a checkpoint — forks a new session.

    Body::

        {
          "checkpoint_id": "cp_...",            # required
          "runtime": "opencode",                 # optional
          "overrides": {                         # optional
            "model": "bailian-payg/glm-5.2",     # provider/model
            "system_prompt": "...",
            "evaluator": "feedback",
            "max_iterations": 30,
            "reviewer_task_goal": "...",
            "initial_prompt": "请继续你之前的工作。",
          }
        }

    ``overrides`` 透传给 ``TracebackRunner.run`` 逐字段覆盖 origin 继承值；
    reviewer 覆盖（``reviewer_model`` / ``reviewer_enabled`` /
    ``reviewer_provider_index`` / ``reviewer_thinking_*``）落到 fork session
    的 ``reviewer`` 配置。

    重建中的同步重 I/O 已 offload 到线程池，不阻塞事件循环，重放期间 SSE 持续推送。
    """
    plugin = resolve_plugin("agent_traceback")
    if plugin is None:
        return {"ok": False, "error": "agent_traceback plugin not loaded"}
    checkpoint_id = (body.get("checkpoint_id") or "").strip()
    if not checkpoint_id:
        return {"ok": False, "error": "checkpoint_id is required"}
    runtime = (body.get("runtime") or "").strip() or None
    overrides = _clean_traceback_overrides(body.get("overrides"))
    return await plugin.run_traceback(
        session_id,
        checkpoint_id,
        runtime=runtime,
        overrides=overrides,
    )


async def _list_checkpoint_files(
    resolve_plugin: Callable[[str], AgentTracebackPlugin | None],
    session_id: str,
    checkpoint_id: str,
    path: str,
) -> dict[str, Any]:
    """Preview file-state entries at a checkpoint (decide whether/where to replay).

    Query: ``?path=<subpath>`` to descend into a subdirectory. Returns
    ``{"entries": [{"name", "path", "is_dir"}]}``.
    """
    plugin = resolve_plugin("agent_traceback")
    if plugin is None:
        return {"entries": []}
    return {
        "entries": plugin.list_checkpoint_files(
            session_id,
            checkpoint_id,
            path,
        )
    }


async def _read_checkpoint_file(
    resolve_plugin: Callable[[str], AgentTracebackPlugin | None],
    session_id: str,
    checkpoint_id: str,
    path: str,
) -> dict[str, Any]:
    """Read a single file's content at a checkpoint (preview before replay).

    Query: ``?path=<relpath>``. Returns ``{"name","path","size","content","truncated"}``.
    """
    plugin = resolve_plugin("agent_traceback")
    if plugin is None:
        return {"error": "agent_traceback plugin not loaded"}
    if not path:
        return {"error": "path is required"}
    data = plugin.read_checkpoint_file(session_id, checkpoint_id, path)
    if data is None:
        return {"error": "file not found in checkpoint"}
    return data


async def _import_db_session(
    resolve_plugin: Callable[[str], AgentTracebackPlugin | None],
    body: dict[str, Any],
    request: Request,
) -> dict[str, Any]:
    """从 cannbot session db 文件重建 Workflow Controller session（含检查点）。

    Body: ``{"db_path": "<cannbot db>", "target_cwd": "<空目录>"}``。
    路径用 ``/import-db`` 而非 ``/import``（后者已被
    ``sessions_routes.import_session`` 占用）。

    同步阻塞到完成；响应带 ``log_path``，可经 ``GET /import-db/log`` 下载。

    重建成功后把 ``target_cwd`` 登记进「最近项目」：重建 session 落在
    ``<target_cwd>/.workflow-controller/events.db``，但该目录通常不是启动期 cwd → 重启后
    ``load_sessions`` 不扫描它 → session 不还原进 pool → 侧边栏丢失该项目。
    显式登记保证重启后仍可见。
    """
    plugin = resolve_plugin("agent_traceback")
    if plugin is None:
        return {"ok": False, "error": "agent_traceback plugin not loaded", "log_path": ""}
    db_path = (body.get("db_path") or "").strip()
    target_cwd_raw = (body.get("target_cwd") or "").strip()
    if not db_path or not target_cwd_raw:
        return {"ok": False, "error": "db_path and target_cwd are required", "log_path": ""}
    # 与 orchestrator 内部保持同一解析：expanduser + resolve，
    # 保证返回给客户端的 log_path 即磁盘真实路径。
    resolved_cwd = Path(target_cwd_raw).expanduser().resolve()
    log_path = str(resolved_cwd / ".workflow-controller" / "reconstruct" / "reconstruction.log")
    result = await plugin.import_session(
        db_path,
        str(resolved_cwd),
        log_path=log_path,
    )
    if result.get("ok"):
        _register_recent_dir(request, resolved_cwd)
    result["log_path"] = log_path
    return result


def _register_recent_dir(request: Request, resolved_cwd: Path) -> None:
    """把重建出的项目目录登记进「最近项目」，使其在侧边栏跨重启可见。

    ConfigAPI 是 recent_dirs.json 的唯一 owner，经 app.state.api 复用其
    load/save/去重/上限逻辑。卫生谓词拒绝的 target（如系统临时区里的
    重建目录）不登记——临时区目录本就不构成可持久项目。headless / 测试
    无 app.state.api → 跳过。
    """
    api = getattr(request.app.state, "api", None)
    config_api = getattr(api, "config", None)
    if config_api is None:
        return
    try:
        _, add_err = config_api.add_recent_dir(str(resolved_cwd))
        if add_err:
            logger.debug(
                "traceback: reconstructed cwd %s not registered in recent dirs (%s)",
                resolved_cwd,
                add_err,
            )
    except Exception:
        logger.debug(
            "traceback: add_recent_dir for reconstructed cwd %s failed (non-fatal)",
            resolved_cwd,
            exc_info=True,
        )


def _get_import_log(target_cwd: str) -> Any:
    """下载重建日志（text/plain）。重建中 → partial；完成后 → 完整。

    Query: ``?target_cwd=<encoded>``。target_cwd 非法/日志不存在 → 404。
    """
    from fastapi import HTTPException

    if not target_cwd:
        raise HTTPException(status_code=404, detail="target_cwd is required")
    base = Path(target_cwd).expanduser().resolve()
    if not base.is_dir():
        raise HTTPException(status_code=404, detail="target_cwd not found")
    log_file = base / ".workflow-controller" / "reconstruct" / "reconstruction.log"
    # base 已 resolve()，log_file 不会跳出 base，无需穿越检查。
    if not log_file.is_file():
        raise HTTPException(status_code=404, detail="log file not found")
    return FileResponse(
        str(log_file),
        media_type="text/plain",
        filename="reconstruction.log",
    )
