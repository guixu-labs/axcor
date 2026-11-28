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

"""Watchdog 告警查询路由 — 历史告警只读端点。"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from workflow_controller.api.http.routes.common import get_core_api

router = APIRouter()


@router.get("/sessions/{session_id}/watchdog/alerts")
async def list_watchdog_alerts(session_id: str, request: Request) -> dict[str, Any]:
    """拉取该 session 的历史 watchdog 告警。"""
    return {"alerts": get_core_api(request).watchdog.list_alerts(session_id)}
