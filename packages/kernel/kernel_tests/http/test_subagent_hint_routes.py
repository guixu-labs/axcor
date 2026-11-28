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

"""subagent-hint 路由约定：register / clear / get 经 CoreAPI 且可审计。"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock


def _app_with_routes():
    """api.session 置为 mock 装配路由应用，免起完整编排器即可测路由逻辑。"""
    from fastapi import FastAPI

    from workflow_controller.api.http.routes import sessions_routes

    app = FastAPI()
    api = MagicMock()
    api.session.register_subagent_hint = AsyncMock()
    api.session.clear_subagent_hint = AsyncMock(return_value={"cleared": 1})
    api.session.get_subagent_hints = MagicMock(return_value={"researcher": ["h"]})
    app.state.api = api
    app.include_router(sessions_routes.router, prefix="/api/v1")
    return app, api


class TestSubagentHintRoutes:
    async def test_post_registers_hint(self):
        from httpx import ASGITransport, AsyncClient

        app, api = _app_with_routes()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.post(
                "/api/v1/sessions/s1/subagent-hint",
                json={"subagent_type": "researcher", "hint": "fix"},
            )
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        api.session.register_subagent_hint.assert_awaited_once_with("s1", "researcher", "fix")

    async def test_post_missing_subagent_type_errors(self):
        from httpx import ASGITransport, AsyncClient

        app, _ = _app_with_routes()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.post("/api/v1/sessions/s1/subagent-hint", json={"hint": "x"})
        assert r.json()["error"] == "subagent_type is required"

    async def test_post_empty_hint_errors(self):
        from httpx import ASGITransport, AsyncClient

        app, _ = _app_with_routes()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.post(
                "/api/v1/sessions/s1/subagent-hint", json={"subagent_type": "r", "hint": "   "}
            )
        assert r.json()["error"] == "hint is required"

    async def test_get_returns_hints(self):
        from httpx import ASGITransport, AsyncClient

        app, _ = _app_with_routes()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/api/v1/sessions/s1/subagent-hint")
        assert r.status_code == 200
        assert r.json() == {"hints": {"researcher": ["h"]}}

    async def test_delete_one(self):
        from httpx import ASGITransport, AsyncClient

        app, api = _app_with_routes()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.delete("/api/v1/sessions/s1/subagent-hint/researcher")
        assert r.status_code == 200
        assert r.json() == {"cleared": 1}
        # route forwards the optional index query (defaults to None)
        api.session.clear_subagent_hint.assert_awaited_once_with("s1", "researcher", None)

    async def test_delete_one_by_index(self):
        # ?index=N drops a single hint (0-based) for the subagent_type
        from httpx import ASGITransport, AsyncClient

        app, api = _app_with_routes()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.delete("/api/v1/sessions/s1/subagent-hint/researcher?index=0")
        assert r.status_code == 200
        api.session.clear_subagent_hint.assert_awaited_once_with("s1", "researcher", 0)

    async def test_delete_all(self):
        from httpx import ASGITransport, AsyncClient

        app, api = _app_with_routes()
        api.session.clear_subagent_hint = AsyncMock(return_value={"cleared": 2})
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.delete("/api/v1/sessions/s1/subagent-hint")
        assert r.status_code == 200
        assert r.json() == {"cleared": 2}
        api.session.clear_subagent_hint.assert_awaited_once_with("s1", None)
