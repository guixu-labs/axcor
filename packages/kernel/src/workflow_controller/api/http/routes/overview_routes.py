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

"""观测领域路由 — health、pipeline、usage 与事件轮询。"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Request

from workflow_controller.api.http.routes.common import get_core_api

router = APIRouter()


@router.get("/health")
async def health_check(request: Request) -> dict[str, Any]:
    return {
        "ok": True,
        "orchestrator": "running",
        **get_core_api(request).session.health_snapshot(),
    }


@router.get("/pipeline")
async def get_pipeline(request: Request, session_id: str | None = None) -> dict[str, Any]:
    return get_core_api(request).pipeline.snapshot(session_id).__dict__


@router.get("/sessions/{session_id}/pipeline")
async def get_session_pipeline(session_id: str, request: Request) -> dict[str, Any]:
    return get_core_api(request).pipeline.snapshot(session_id).__dict__


@router.get("/usage")
async def get_usage(request: Request) -> dict[str, Any]:
    snap = await asyncio.to_thread(get_core_api(request).usage.snapshot, force=True)
    return snap.__dict__


@router.get("/events")
async def get_events(
    since: str = "",
    cursor: str = "",
    session_id: str = "",
    request: Request = None,
) -> dict[str, Any]:
    if request is None:
        raise RuntimeError("request 参数缺失")
    if cursor:
        new_cursor, events = get_core_api(request).event.get_since_cursor(
            cursor,
            session_id=session_id,
        )
        return {"cursor": new_cursor, "last_ts": since, "events": [e.__dict__ for e in events]}
    event_api = get_core_api(request).event
    snapshot = event_api.snapshot_cursor()
    if session_id:
        ts, events = event_api.get_since(since, session_id=session_id)
    else:
        events = [
            event
            for event in event_api.get_recent_at_cursor(snapshot, 0)
            if event.timestamp > since
        ]
        ts = max((event.timestamp for event in events), default=since)
    return {
        "last_ts": ts,
        "cursor": snapshot,
        "events": [e.__dict__ for e in events],
    }
