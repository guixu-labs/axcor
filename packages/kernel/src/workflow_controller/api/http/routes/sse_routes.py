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

"""SSE 路由 — 实时事件流推送。

通过 Server-Sent Events 向浏览器推送 session 状态、日志、流水线进度等。
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import anyio
from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from workflow_controller.api.http.routes.common import compose_config_dict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable

router = APIRouter()
log = logging.getLogger(__name__)
_cache_init_lock = threading.Lock()


class _SnapshotCache:
    """同一应用的 SSE 连接共享短期快照，避免标签页数量放大同步读。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: dict[str, tuple[float, Any]] = {}

    def get(self, key: str, builder: Callable[[], Any]) -> Any:
        with self._lock:
            hit = self._values.get(key)
            if hit is not None and time.monotonic() - hit[0] < 0.5:
                return hit[1]
            value = builder()
            self._values[key] = (time.monotonic(), value)
            return value


def _cached_snap(request: Request, key: str, builder: Callable[[], Any]) -> Any:
    state = request.app.state
    cache = getattr(state, "sse_snapshot_cache", None)
    if cache is None:
        with _cache_init_lock:
            cache = getattr(state, "sse_snapshot_cache", None)
            if cache is None:
                cache = _SnapshotCache()
                state.sse_snapshot_cache = cache
    return cache.get(key, builder)


@dataclass
class _SSECursors:
    """SSE 流两端的增量游标（跨轮次共享的可变状态）。"""

    event_cursor: str = ""
    run_event_cursor: str | None = ""


async def _sse_replay(api: Any, cursors: _SSECursors) -> AsyncIterator[dict[str, str]]:
    """首次连接回放最近 50 个事件；游标记入 cursors 供后续增量。"""
    try:
        cursors.event_cursor = await asyncio.to_thread(api.event.snapshot_cursor)
        replay_events = await asyncio.to_thread(
            api.event.get_recent_at_cursor, cursors.event_cursor, 50
        )
        for event in replay_events:
            yield {
                "event": "claude_event",
                "data": json.dumps(event.__dict__, default=str),
            }
    except Exception:
        log.exception("SSE replay failed — continuing with incremental only")


async def _sse_run_cursor_snapshot(api: Any) -> str | None:
    """run 键事件初始游标（当前最大值）；重试一次仍失败返回 None 跳过该段。"""
    try:
        return await asyncio.to_thread(api.event.snapshot_run_event_cursor)
    except Exception:
        try:
            return await asyncio.to_thread(api.event.snapshot_run_event_cursor)
        except Exception:
            log.exception("SSE run event cursor snapshot failed — skipping run_events push")
            return None


async def _sse_event_segments(api: Any, cursors: _SSECursors) -> AsyncIterator[dict[str, str]]:
    """增量事件段：claude_event + run_events（run_events 表增量；消费端按 run_id 分桶）。

    每段独立 try/except：单段异常（如 sqlite3.OperationalError）只跳过本段；
    CancelledError / GeneratorExit 属 BaseException 不被 ``except Exception`` 捕获，
    正常向上传播到外层生成器关闭逻辑。同步 DB 调用走 to_thread（见下）。
    """
    try:
        cursors.event_cursor, new_events = await asyncio.to_thread(
            api.event.get_since_cursor, cursors.event_cursor
        )
        for event in new_events:
            yield {
                "event": "claude_event",
                "data": json.dumps(event.__dict__, default=str),
            }
    except Exception as e:
        log.debug("SSE claude_event segment failed: %s", e)

    if cursors.run_event_cursor is not None:
        try:
            cursors.run_event_cursor, run_events = await asyncio.to_thread(
                api.event.get_run_events_since_cursor, cursors.run_event_cursor
            )
            for row in run_events:
                yield {
                    "event": "claude_event",
                    "data": json.dumps(
                        {
                            "rowid": row["id"],
                            "session_id": "",
                            "run_id": row["run_id"],
                            "context": row["context"],
                            "event_type": row["event_type"],
                            "iteration": 0,
                            "timestamp": row["timestamp"],
                            "data": row["data"],
                        },
                        default=str,
                    ),
                }
        except Exception as e:
            log.debug("SSE run_events segment failed: %s", e)


async def _sse_state_segments(api: Any, request: Request) -> AsyncIterator[dict[str, str]]:
    """会话状态 / 流水线进度（per-session）/ 用量统计快照段。"""
    try:
        sessions = await asyncio.to_thread(
            _cached_snap,
            request,
            "sessions",
            api.session.list_sessions,
        )
        yield {
            "event": "session_update",
            "data": json.dumps([s.__dict__ for s in sessions], default=str),
        }
    except Exception as e:
        log.debug("SSE session_update segment failed: %s", e)

    try:
        all_snaps = await asyncio.to_thread(
            _cached_snap,
            request,
            "pipeline",
            api.pipeline.all_snapshots,
        )
        yield {
            "event": "pipeline_update",
            "data": json.dumps(
                {sid: snap.__dict__ for sid, snap in all_snaps.items()},
                default=str,
            ),
        }
    except Exception as e:
        log.debug("SSE pipeline_update segment failed: %s", e)

    # snapshot() 全量扫描 events 聚合，也必须走 to_thread（同上）。
    try:
        usage_snap = await asyncio.to_thread(
            _cached_snap,
            request,
            "usage",
            api.usage.snapshot,
        )
        yield {
            "event": "usage_update",
            "data": json.dumps(usage_snap.__dict__, default=str),
        }
    except Exception as e:
        log.debug("SSE usage_update segment failed: %s", e)


async def _sse_config_segments(api: Any, request: Request) -> AsyncIterator[dict[str, str]]:
    """配置 / Auth / Handoff（注册表聚合各插件上报的交接状态）快照段。"""
    try:
        yield {
            "event": "config_update",
            "data": json.dumps(
                await asyncio.to_thread(
                    _cached_snap,
                    request,
                    "config",
                    lambda: compose_config_dict(request, api.config.get_config_dict()),
                ),
                default=str,
            ),
        }
    except Exception as e:
        log.debug("SSE config_update segment failed: %s", e)

    try:
        yield {
            "event": "auth_update",
            "data": json.dumps(
                await asyncio.to_thread(
                    _cached_snap,
                    request,
                    "auth",
                    api.auth.key_status,
                ),
                default=str,
            ),
        }
    except Exception as e:
        log.debug("SSE auth_update segment failed: %s", e)

    try:
        handoff_states = await asyncio.to_thread(
            _cached_snap,
            request,
            "handoff",
            api.plugin.get_handoff_states,
        )
        yield {
            "event": "handoff_update",
            "data": json.dumps({"states": handoff_states}, default=str),
        }
    except Exception as e:
        log.debug("SSE handoff_update segment failed: %s", e)


async def _sse_loop_segment(api: Any, request: Request) -> AsyncIterator[dict[str, str]]:
    """Loop 状态段。"""
    try:
        running = await asyncio.to_thread(
            _cached_snap,
            request,
            "loop",
            api.session.running_loop_sessions,
        )
        watching_id = running[0]["id"] if len(running) == 1 else None
        watching_name = running[0]["name"] if len(running) == 1 else None
        yield {
            "event": "loop_update",
            "data": json.dumps(
                {
                    "running_sessions": running,
                    "watching_session_id": watching_id,
                    "watching_session_name": watching_name,
                },
                default=str,
            ),
        }
    except Exception as e:
        log.debug("SSE loop_update segment failed: %s", e)


async def event_stream(
    request: Request, shutdown_event: anyio.Event
) -> AsyncGenerator[dict[str, str], None]:
    """SSE 事件流 — 每 250ms 推送一次状态快照。

    首次连接时回放最近 50 个事件，避免客户端错过 SSE 断开期间的 subagent 等事件。
    服务端 shutdown（shutdown_event 置位）时在退出前发 server_reload farewell。
    """
    api = request.app.state.api
    cursors = _SSECursors()
    async for item in _sse_replay(api, cursors):
        yield item
    # run 键事件游标从当前最大值起（连接前的不回放，走各端 cursor 拉取）；快照
    # 失败不能回退到 0（_decode_cursor 把非法游标解成 0 会触发整表逐拍全量回放）。
    cursors.run_event_cursor = await _sse_run_cursor_snapshot(api)
    try:
        while True:
            if await request.is_disconnected():
                break

            # 服务端 shutdown（sse_starlette 捕获 uvicorn SIGTERM 后 set 此 event）。
            # 必须趁任务尚未被取消在此 break 去发 farewell——一旦被 CancelledError
            # 打断，再 yield 的事件也到不了浏览器：_stream_response 的 send 在已
            # 取消的 task group scope 里会立即再次被取消。
            if shutdown_event.is_set():
                break

            # 所有同步 DB 调用走 to_thread，避免阻塞事件循环导致 Ctrl+C 无响应。
            async for item in _sse_event_segments(api, cursors):
                yield item
            async for item in _sse_state_segments(api, request):
                yield item
            async for item in _sse_config_segments(api, request):
                yield item
            async for item in _sse_loop_segment(api, request):
                yield item
            await asyncio.sleep(0.25)
    except Exception as exc:
        log.exception("SSE stream error: %s", exc)

    # uvicorn reload 时后端发 server_reload 事件，前端立即重连（useSSE.ts）
    # 不等 TCP 超时。只有协作式 shutdown 路径（上方 shutdown_event break）
    # 走到这里时事件才可送达。
    if shutdown_event.is_set():
        yield {"event": "server_reload", "data": "{}"}


@router.get("/events/stream")
async def sse_stream(request: Request) -> EventSourceResponse:
    shutdown = anyio.Event()
    return EventSourceResponse(
        event_stream(request, shutdown),
        # 协作式 shutdown：sse_starlette 捕获 uvicorn SIGTERM 后 set shutdown_event，
        # grace 期内生成器仍可发送（farewell 即在此窗口送达），期满才强杀。
        # grace 必须 < cmd_web 的 timeout_graceful_shutdown（3s）。
        shutdown_event=shutdown,
        shutdown_grace_period=2.0,
    )
