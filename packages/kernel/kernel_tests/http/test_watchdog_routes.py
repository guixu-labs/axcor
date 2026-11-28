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

"""Watchdog 告警查询路由测试：响应形状经公开 API 透传。"""

from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from workflow_controller.api.http.routes.watchdog_routes import router


def _client(alerts: list[dict]) -> TestClient:
    app = FastAPI()
    app.state.api = SimpleNamespace(
        watchdog=SimpleNamespace(list_alerts=lambda session_id: alerts),
    )
    app.include_router(router)
    return TestClient(app)


def test_list_alerts_returns_public_api_payload() -> None:
    alert = {"alert_id": "a1", "rule": "file_stall", "severity": "warning"}
    resp = _client([alert]).get("/sessions/ses_1/watchdog/alerts")
    assert resp.status_code == 200
    assert resp.json() == {"alerts": [alert]}


def test_list_alerts_empty() -> None:
    assert _client([]).get("/sessions/ses_1/watchdog/alerts").json() == {"alerts": []}
