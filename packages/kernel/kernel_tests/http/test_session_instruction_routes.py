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

"""session instructions 路由约定：register / clear / get 经 CoreAPI 且可审计。"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock


def _app_with_routes():
    """api.session 置为 mock 装配路由应用，免起完整编排器即可测路由逻辑。"""
    from fastapi import FastAPI

    from workflow_controller.api.http.routes import sessions_routes

    app = FastAPI()
    api = MagicMock()
    api.session.register_session_instruction = AsyncMock()
    api.session.clear_session_instruction = AsyncMock(return_value={"cleared": 1})
    api.session.get_session_instructions = MagicMock(return_value=["h"])
    app.state.api = api
    app.include_router(sessions_routes.router, prefix="/api/v1")
    return app, api


class TestSessionInstructionRoutes:
    async def test_post_registers_instruction(self):
        from httpx import ASGITransport, AsyncClient

        app, api = _app_with_routes()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.post(
                "/api/v1/sessions/s1/instructions", json={"instruction": "use arch32"}
            )
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        api.session.register_session_instruction.assert_awaited_once_with("s1", "use arch32")

    async def test_post_empty_instruction_errors(self):
        from httpx import ASGITransport, AsyncClient

        app, _ = _app_with_routes()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.post("/api/v1/sessions/s1/instructions", json={"instruction": "   "})
        assert r.json()["error"] == "instruction is required"

    async def test_get_returns_instructions(self):
        from httpx import ASGITransport, AsyncClient

        app, _ = _app_with_routes()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/api/v1/sessions/s1/instructions")
        assert r.status_code == 200
        assert r.json() == {"instructions": ["h"]}

    async def test_delete_one_by_index(self):
        from httpx import ASGITransport, AsyncClient

        app, api = _app_with_routes()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.delete("/api/v1/sessions/s1/instructions?index=0")
        assert r.status_code == 200
        assert r.json() == {"cleared": 1}
        api.session.clear_session_instruction.assert_awaited_once_with("s1", 0)

    async def test_delete_all(self):
        from httpx import ASGITransport, AsyncClient

        app, api = _app_with_routes()
        api.session.clear_session_instruction = AsyncMock(return_value={"cleared": 2})
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.delete("/api/v1/sessions/s1/instructions")
        assert r.status_code == 200
        assert r.json() == {"cleared": 2}
        api.session.clear_session_instruction.assert_awaited_once_with("s1", None)
