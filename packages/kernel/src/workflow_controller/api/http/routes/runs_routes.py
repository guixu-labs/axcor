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

"""公共 API v1 — runs（全量面）。

run 的唯一控制/观测入口：启停查、组控、转向、成员操作、驱动自定义
操作、事件游标与删除。薄 handler 惯例：逻辑在 api/run_api.py，本文件
只做参数校验与错误码翻译（404 run 不存在 / 409 形态冲突 / 422 输入问题 /
503 驱动不可用）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from workflow_controller.api.http.routes.common import get_core_api
from workflow_controller.api.run_api import (
    RunApiInvalidStateError,
    RunApiNotFoundError,
    RunApiSessionNotFoundError,
    run_record_to_dict,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# ── 异步导出任务注册表 ─────────────────────────────────────────────
# task_id -> {run_id, status(running/done/error), done, total, error,
#             result, zip_path, cancel, done_at}
_EXPORT_TASKS: dict[str, dict[str, Any]] = {}
_EXPORT_LOCK = threading.Lock()
# 终态任务保留窗口（前端延迟下载/读错误信息）；超时由后台线程回收 zip
_EXPORT_TTL = 3600.0
_EXPORT_SWEEP_INTERVAL = 600.0
_EXPORT_JANITOR_STARTED = False


class _ExportCancelledError(Exception):
    """客户端 cancel 时中断后台导出线程。"""


def _sweep_exports_locked() -> None:
    now = time.monotonic()
    expired = [
        tid
        for tid, t in _EXPORT_TASKS.items()
        if t.get("done_at") is not None and now - t["done_at"] > _EXPORT_TTL
    ]
    for tid in expired:
        t = _EXPORT_TASKS.pop(tid)
        zip_path = t.get("zip_path")
        if zip_path:
            # 只删 zip 本身：export/ 目录还装着同 run 的 bundle 目录与其他
            # 尚未结束的导出的产物，整目录删除会误伤并发的重导出。zip 按任务
            # 唯一命名（archive_name），删的是本任务自己的产物，不影响重导出。
            Path(zip_path).unlink(missing_ok=True)


def _start_exports_janitor_locked() -> None:
    global _EXPORT_JANITOR_STARTED
    if _EXPORT_JANITOR_STARTED:
        return
    _EXPORT_JANITOR_STARTED = True

    def _arm() -> None:
        timer = threading.Timer(_EXPORT_SWEEP_INTERVAL, _janitor)
        timer.daemon = True
        timer.start()

    def _janitor() -> None:
        try:
            with _EXPORT_LOCK:
                _sweep_exports_locked()
        finally:
            _arm()

    _arm()


def _run_export_task(task_id: str, run_id: str, api: Any) -> None:
    """后台线程跑导出；progress_cb 逐会话上报进度，cancel 中断。"""
    loop = asyncio.new_event_loop()
    try:

        def cb(done: int, total: int) -> None:
            with _EXPORT_LOCK:
                t = _EXPORT_TASKS.get(task_id)
                if t is None or t.get("cancel"):
                    raise _ExportCancelledError()
                t["done"] = done
                t["total"] = total

        result = loop.run_until_complete(
            api.run.export_run(
                run_id,
                archive=True,
                progress_cb=cb,
                archive_name=f"{run_id}-bundle-{task_id}.zip",
            )
        )
        with _EXPORT_LOCK:
            t = _EXPORT_TASKS.get(task_id)
            if t is not None and not t.get("cancel"):
                t.update(
                    {
                        "status": "done",
                        "result": result,
                        "zip_path": result.get("archive"),
                        "done_at": time.monotonic(),
                    }
                )
                return
        # 已取消：回收本次产物
        _cleanup_export_archive(result)
    except _ExportCancelledError:
        with _EXPORT_LOCK:
            _EXPORT_TASKS.pop(task_id, None)
    except Exception as exc:
        logger.exception("run %s export failed", run_id)
        with _EXPORT_LOCK:
            t = _EXPORT_TASKS.get(task_id)
            if t is not None:
                t.update(
                    {
                        "status": "error",
                        "error": f"{type(exc).__name__}: {exc}",
                        "done_at": time.monotonic(),
                    }
                )
    finally:
        loop.close()


def _cleanup_export_archive(result: dict[str, Any] | None) -> None:
    # 只删 zip 文件：bundle 目录是最后一次完整导出的发布产物，保留可被后续
    # 下载/覆盖复用，整目录删除会连带清掉它。zip 按任务唯一命名，删的是本
    # 任务自己的产物，不碰同 run 更新一次导出的 zip。
    if not result or not result.get("archive"):
        return
    Path(result["archive"]).unlink(missing_ok=True)


async def _read_json_object(request: Request, *, optional: bool = False) -> dict[str, Any]:
    """读请求体为 JSON 对象；非法 body 报 422，非对象按空字典容错。

    空 body 在 optional 时容错为空字典（无 body 的既有调用方照常受理）。
    """
    raw = await request.body()
    if not raw and optional:
        return {}
    try:
        body = json.loads(raw)
    except ValueError:  # json.JSONDecodeError 未被框架转译，不兜会冒成 500
        raise HTTPException(422, "请求体需为 JSON 对象") from None
    return body if isinstance(body, dict) else {}


def _nonempty_str(body: dict[str, Any], key: str) -> str | None:
    """取非空字符串字段；缺失/空白串/类型不对都视为未给（None），返回去空白后的值。"""
    value = body.get(key)
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return None


def _nonempty_dict_value(body: dict[str, Any], key: str) -> dict[str, Any]:
    """取非空对象字段；缺失/空对象/类型不对都视为未给（空字典）。"""
    value = body.get(key)
    return value if isinstance(value, dict) and value else {}


@router.post("", status_code=201)
async def start_run(request: Request) -> dict[str, Any]:
    """启动 run：给 format（或程序文件头携带）走 workflow，都不给则纯容器。"""
    body = await _read_json_object(request)
    fmt = _nonempty_str(body, "format")
    program_path = _nonempty_str(body, "program_path")
    program_ref = _nonempty_str(body, "program_ref")
    label = _nonempty_str(body, "label")
    work_dir = _nonempty_str(body, "work_dir")
    params = _nonempty_dict_value(body, "params")
    max_parallel = body.get("max_parallel")
    if max_parallel is not None and (not isinstance(max_parallel, int) or max_parallel < 1):
        raise HTTPException(422, "max_parallel 需为正整数")
    api = get_core_api(request)
    try:
        if fmt or program_path:
            if not program_path:
                raise HTTPException(422, "启动 workflow run 需要 program_path（程序文件）")
            if not work_dir:
                raise HTTPException(
                    422,
                    "启动 workflow run 需要 work_dir（agent 产物目录，"
                    "须在 run 根之外——run 根只保存 Workflow Controller 记录，删除时不留产物）",
                )
            run = await api.run.start(
                program_ref or "",
                program_path,
                params=params,
                labels={"name": label} if label else None,
                fmt=fmt,
                work_dir=work_dir,
                max_parallel=max_parallel,
            )
        else:
            run = await api.run.create(
                label or "",
                work_dir=work_dir,
                max_parallel=max_parallel,
            )
    except (ValueError, OSError, yaml.YAMLError) as e:
        raise HTTPException(422, str(e)) from e
    return {
        "run_id": run.run_id,
        "status": run.status,
        "format": run.format,
        "kind": run.kind,
    }


@router.get("")
async def list_runs(
    request: Request,
    include_implicit: bool = False,
    status: str | None = None,
) -> dict[str, Any]:
    """run 列表（默认过滤隐式 run；status 过滤）。"""
    runs = await get_core_api(request).run.list_runs(
        include_implicit=include_implicit,
        status=status,
    )
    return {"runs": runs}


@router.get("/{run_id}")
async def get_run(request: Request, run_id: str) -> dict[str, Any]:
    """run 记录 + 成员全行视图。"""
    api = get_core_api(request)
    try:
        run = await api.run.get_run(run_id)
        sessions = await api.run.session_infos(run_id)
    except RunApiNotFoundError as e:
        raise HTTPException(404, f"run 不存在：{run_id}") from e
    return {"run": run_record_to_dict(run), "sessions": sessions}


@router.get("/{run_id}/state")
async def get_run_state(request: Request, run_id: str) -> dict[str, Any]:
    """通用面（run/成员/进程/指标）+ 驱动投影透传。"""
    try:
        return await get_core_api(request).run.state(run_id)
    except RunApiNotFoundError as e:
        raise HTTPException(404, f"run 不存在：{run_id}") from e


async def _control(
    request: Request, run_id: str, action: str, *, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    """组控端点公共体：受理即返回（202）；payload 仅 resume 消费（随 RESUMED 递驱动）。"""
    api = get_core_api(request)
    try:
        if action == "pause":
            record = await api.run.pause(run_id)
        elif action == "resume":
            record = await api.run.resume(run_id, payload)
        elif action == "restart":
            record = await api.run.restart(run_id)
        else:
            record = await api.run.stop(run_id)
    except RunApiNotFoundError as e:
        raise HTTPException(404, f"run 不存在：{run_id}") from e
    except ValueError as e:  # 隐式 run / auth 档恢复 / 非终态重开：冲突而非出错
        raise HTTPException(409, str(e)) from e
    return {"run_id": run_id, "status": record.status, "accepted": True}


@router.post("/{run_id}/pause", status_code=202)
async def pause_run(request: Request, run_id: str) -> dict[str, Any]:
    return await _control(request, run_id, "pause")


@router.post("/{run_id}/resume", status_code=202)
async def resume_run(request: Request, run_id: str) -> dict[str, Any]:
    """恢复 run；请求体可选——带 payload 字段时随 RESUMED 通知递给驱动。"""
    body = await _read_json_object(request, optional=True)
    payload = body.get("payload")
    if payload is not None and not isinstance(payload, dict):
        raise HTTPException(422, "字段 payload 需为 JSON 对象")
    return await _control(request, run_id, "resume", payload=payload)


@router.post("/{run_id}/stop", status_code=202)
async def stop_run(request: Request, run_id: str) -> dict[str, Any]:
    return await _control(request, run_id, "stop")


@router.post("/{run_id}/restart", status_code=202)
async def restart_run(request: Request, run_id: str) -> dict[str, Any]:
    return await _control(request, run_id, "restart")


@router.post("/{run_id}/steer", status_code=202)
async def steer_run(request: Request, run_id: str) -> dict[str, Any]:
    """run 级转向注入：有驱动进决策轮（驱动决定转向谁），无驱动直播成员。"""
    message = _nonempty_str(await _read_json_object(request), "message")
    if message is None:
        raise HTTPException(422, "需要字符串字段：message")
    try:
        await get_core_api(request).run.steer(run_id, message)
    except RunApiNotFoundError as e:
        raise HTTPException(404, f"run 不存在：{run_id}") from e
    return {"run_id": run_id, "accepted": True}


@router.post("/{run_id}/driver/op")
async def driver_op(request: Request, run_id: str) -> dict[str, Any]:
    """驱动自定义操作转发（请求经通知通道在 run 任务内执行）。"""
    body = await _read_json_object(request)
    op = _nonempty_str(body, "op")
    if op is None:
        raise HTTPException(422, "需要字符串字段：op")
    if op == "__projection__":
        raise HTTPException(422, "保留操作名，驱动投影经 GET /state 读取")
    args = _nonempty_dict_value(body, "args")
    try:
        return await get_core_api(request).run.request_driver_op(run_id, op, args)
    except RunApiNotFoundError as e:
        raise HTTPException(404, f"run 不存在：{run_id}") from e
    except Exception as e:  # 应答通道回退：驱动侧任意异常不以 500 暴露
        raise HTTPException(503, str(e)) from e


@router.post("/{run_id}/export")
async def export_run(request: Request, run_id: str) -> dict[str, Any]:
    """启动后台导出，立即返回 task_id；前端轮询进度后下载 zip。

    同步段只做形态校验（不在账 404 / 隐式或 pending 409），真正导出在
    后台线程执行，逐会话上报 done/total 进度。
    """
    api = get_core_api(request)
    try:
        run = await api.run.get_run(run_id)
    except RunApiNotFoundError as e:
        raise HTTPException(404, f"run 不存在：{run_id}") from e
    refusal = api.run.export_refusal_reason(run)
    if refusal is not None:
        raise HTTPException(409, refusal)

    task_id = uuid.uuid4().hex
    with _EXPORT_LOCK:
        _sweep_exports_locked()
        _start_exports_janitor_locked()
        _EXPORT_TASKS[task_id] = {
            "run_id": run_id,
            "status": "running",
            "done": 0,
            "total": 0,
            "error": None,
            "result": None,
            "zip_path": None,
            "cancel": False,
            "done_at": None,
        }
    threading.Thread(
        target=_run_export_task,
        args=(task_id, run_id, api),
        daemon=True,
    ).start()
    return {"task_id": task_id}


@router.get("/{run_id}/export/{task_id}")
async def export_progress(run_id: str, task_id: str) -> dict[str, Any]:
    """导出进度轮询：status=running/done/error，done/total 逐会话计数。"""
    with _EXPORT_LOCK:
        t = _EXPORT_TASKS.get(task_id)
        if t is None:
            raise HTTPException(404, "导出任务不存在：可能已被回收或取消")
        if t["run_id"] != run_id:
            raise HTTPException(404, "导出任务不属于该 run")
        snapshot = {
            "status": t["status"],
            "done": t["done"],
            "total": t["total"],
            "error": t["error"],
        }
    return snapshot


@router.post("/{run_id}/export/{task_id}/cancel")
async def export_cancel(run_id: str, task_id: str) -> dict[str, Any]:
    """取消后台导出：标记 cancel 并回收已产出（失败不影响前端流程）。"""
    with _EXPORT_LOCK:
        t = _EXPORT_TASKS.get(task_id)
        if t is None:
            raise HTTPException(404, "导出任务不存在：可能已完成或被回收")
        if t["run_id"] != run_id:
            raise HTTPException(404, "导出任务不属于该 run")
        t["cancel"] = True
        zip_path = t.get("zip_path")
        _EXPORT_TASKS.pop(task_id, None)
    if zip_path:
        # zip 按任务唯一命名，取消删的是本任务自己的产物
        Path(zip_path).unlink(missing_ok=True)
    return {"status": "cancelled"}


@router.get("/{run_id}/export/{task_id}/download")
async def export_download(run_id: str, task_id: str) -> Any:
    """下载导出 zip；一次性——下载后回收注册表项并删除 zip。

    zip 按任务唯一命名（见 _run_export_task 的 archive_name），只服务本次
    导出的产物；流送完成后经 BackgroundTask 删除，避免同 run 多次导出在
    export/ 下累积孤儿 zip。
    """
    with _EXPORT_LOCK:
        t = _EXPORT_TASKS.get(task_id)
        if t is None:
            raise HTTPException(404, "导出任务不存在：可能已被回收或取消")
        if t["run_id"] != run_id:
            raise HTTPException(404, "导出任务不属于该 run")
        if t["status"] != "done" or not t.get("zip_path"):
            raise HTTPException(409, "导出尚未就绪，请等待完成或稍后重试")
        zip_path = t["zip_path"]
        result = t.get("result") or {}
        _EXPORT_TASKS.pop(task_id, None)
    if not Path(zip_path).is_file():
        raise HTTPException(410, "导出产物已被回收，请重新导出")
    return FileResponse(
        zip_path,
        filename=f"{run_id}-bundle.zip",
        media_type="application/zip",
        headers={
            "X-Export-Members": str(result.get("members", 0)),
            "X-Export-Relations": str(result.get("relations", 0)),
            "X-Export-Sessions": str(result.get("sessions", 0)),
            "X-Export-Complete": str(result.get("exportDataComplete", False)).lower(),
        },
        background=BackgroundTask(Path(zip_path).unlink, missing_ok=True),
    )


@router.get("/{run_id}/events")
async def get_run_events(
    request: Request,
    run_id: str,
    cursor: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """run 事件增量（run 键 + 成员打戳两表合并序，游标翻页）。"""
    try:
        events, next_cursor, effective_limit = await get_core_api(request).run.fetch_events(
            run_id,
            cursor,
            limit,
        )
    except RunApiNotFoundError as e:
        raise HTTPException(404, f"run 不存在：{run_id}") from e
    except ValueError as e:  # 游标不合法
        raise HTTPException(422, str(e)) from e
    # limit 回传钳制后实际生效页长：翻页终止判据以应答值为准
    return {"events": events, "next_cursor": next_cursor, "limit": effective_limit}


async def _run_session_control(
    request: Request, run_id: str, session_id: str, action: str
) -> dict[str, Any]:
    """run 名下 session 操作端点公共体。"""
    api = get_core_api(request)
    prompt = None
    if action == "inject":
        prompt = _nonempty_str(await _read_json_object(request), "prompt")
        if prompt is None:
            raise HTTPException(422, "需要字符串字段：prompt")
    try:
        if action == "pause":
            await api.run.pause_session(run_id, session_id)
        elif action == "resume":
            await api.run.resume_session(run_id, session_id)
        elif action == "stop":
            await api.run.stop_session(run_id, session_id)
        else:
            await api.run.inject_session(run_id, session_id, prompt or "")
    except RunApiSessionNotFoundError as e:  # 平级细分：run 在账、session 非成员
        raise HTTPException(404, f"session {session_id} 不隶属 run {run_id}") from e
    except RunApiNotFoundError as e:
        raise HTTPException(404, f"run 不存在：{run_id}") from e
    except RunApiInvalidStateError as e:  # 只映射 resume 准入拒绝；其他 ValueError 冒 500
        raise HTTPException(409, str(e)) from e
    return {"run_id": run_id, "session_id": session_id, "accepted": True}


@router.post("/{run_id}/sessions/{session_id}/pause", status_code=202)
async def pause_session(request: Request, run_id: str, session_id: str) -> dict[str, Any]:
    return await _run_session_control(request, run_id, session_id, "pause")


@router.post("/{run_id}/sessions/{session_id}/resume", status_code=202)
async def resume_session(request: Request, run_id: str, session_id: str) -> dict[str, Any]:
    return await _run_session_control(request, run_id, session_id, "resume")


@router.post("/{run_id}/sessions/{session_id}/stop", status_code=202)
async def stop_session(request: Request, run_id: str, session_id: str) -> dict[str, Any]:
    return await _run_session_control(request, run_id, session_id, "stop")


@router.post("/{run_id}/sessions/{session_id}/inject", status_code=202)
async def inject_session(request: Request, run_id: str, session_id: str) -> dict[str, Any]:
    return await _run_session_control(request, run_id, session_id, "inject")


@router.post("/{run_id}/sessions/{session_id}/send", status_code=202)
async def send_session(request: Request, run_id: str, session_id: str) -> dict[str, Any]:
    body = await _read_json_object(request)
    prompt = _nonempty_str(body, "prompt")
    if prompt is None:
        raise HTTPException(422, "需要字符串字段：prompt")
    allow_resume = body.get("allow_resume", True)
    if not isinstance(allow_resume, bool):
        raise HTTPException(422, "字段 allow_resume 需为布尔值")
    api = get_core_api(request)
    try:
        resumed = await api.run.send_session(
            run_id,
            session_id,
            prompt,
            allow_resume=allow_resume,
        )
    except RunApiSessionNotFoundError as e:
        raise HTTPException(404, f"session {session_id} 不隶属 run {run_id}") from e
    except RunApiNotFoundError as e:
        raise HTTPException(404, f"run 不存在：{run_id}") from e
    except RunApiInvalidStateError as e:
        raise HTTPException(409, str(e)) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"run_id": run_id, "session_id": session_id, "accepted": True, "resumed": resumed}


@router.delete("/{run_id}")
async def delete_run(request: Request, run_id: str) -> dict[str, Any]:
    """级联删除：run 不存在（含重复删除）同样返回成功；不碰 work_dir。"""
    await get_core_api(request).run.delete(run_id)
    return {"run_id": run_id, "deleted": True}
