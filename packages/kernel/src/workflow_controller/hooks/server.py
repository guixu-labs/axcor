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

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class HookServerStartupError(RuntimeError):
    """A required Hook Server failed to start or is unavailable."""


class HookServer:
    def __init__(
        self,
        port: int = 18765,
        host: str = "localhost",
        on_event: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
        on_stop_request: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any] | None]]
        | None = None,
        on_pre_tool_use_request: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any] | None]]
        | None = None,
        on_post_tool_use_request: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
        on_subagent_start_request: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
        on_subagent_stop_request: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
    ) -> None:
        self.port = port
        self.host = host
        self.on_event = on_event
        self.on_stop_request = on_stop_request
        self.on_pre_tool_use_request = on_pre_tool_use_request
        self.on_post_tool_use_request = on_post_tool_use_request
        self.on_subagent_start_request = on_subagent_start_request
        self.on_subagent_stop_request = on_subagent_stop_request
        self.app = web.Application()
        self._runner: web.AppRunner | None = None
        self._setup_routes()

    def register_route(self, method: str, path: str, handler: Any) -> None:
        self.app.router.add_route(method, path, handler)

    async def start(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
        runner = web.AppRunner(self.app)
        try:
            await runner.setup()
            await web.TCPSite(runner, self.host, self.port).start()
        except (Exception, asyncio.CancelledError):
            await runner.cleanup()
            raise
        self._runner = runner

    async def stop_server(self) -> None:
        if self._runner:
            await self._runner.shutdown()
            await self._runner.cleanup()
            self._runner = None

    def _setup_routes(self) -> None:
        self.app.router.add_post("/hooks/post-tool-use", self._handle_post_tool_use)
        self.app.router.add_post("/hooks/post-tool-use-failure", self._handle_post_tool_use_failure)
        self.app.router.add_post("/hooks/pre-tool-use", self._handle_pre_tool_use)
        self.app.router.add_post("/hooks/file-changes/pre", self._handle_file_changes_pre)
        self.app.router.add_post("/hooks/file-changes/post", self._handle_file_changes_post)
        self.app.router.add_post("/hooks/stop", self._handle_stop)
        self.app.router.add_post("/hooks/notification", self._handle_notification)
        self.app.router.add_post("/hooks/session-start", self._handle_session_start)
        self.app.router.add_post("/hooks/session-end", self._handle_session_end)
        self.app.router.add_post("/hooks/subagent-start", self._handle_subagent_start)
        self.app.router.add_post("/hooks/subagent-stop", self._handle_subagent_stop)

    async def _handle_post_tool_use(self, request: web.Request) -> web.Response:
        data = await request.json()
        if self.on_post_tool_use_request:
            await self.on_post_tool_use_request(data.get("session_id", "unknown"), data)
        await self._emit_event(data, "tool_result")
        await self._emit_tool_call(data)
        return web.json_response({"status": "ok"})

    async def _handle_post_tool_use_failure(self, request: web.Request) -> web.Response:
        data = await request.json()
        data["is_error"] = True
        if self.on_post_tool_use_request:
            await self.on_post_tool_use_request(data.get("session_id", "unknown"), data)
        await self._emit_event(data, "tool_result")
        await self._emit_tool_call(data)
        return web.json_response({"status": "ok"})

    async def _handle_pre_tool_use(self, request: web.Request) -> web.Response:
        data = await request.json()
        await self._emit_event(data, "tool_use")

        if self.on_pre_tool_use_request:
            session_id = data.get("session_id", "unknown")
            decision = await self.on_pre_tool_use_request(session_id, data)
            if decision:
                return web.json_response(decision)

        return web.json_response({"status": "ok"})

    async def _handle_file_changes_pre(self, request: web.Request) -> web.Response:
        data = await request.json()
        if self.on_pre_tool_use_request:
            await self.on_pre_tool_use_request(data.get("session_id", "unknown"), data)
        return web.json_response({"status": "ok"})

    async def _handle_file_changes_post(self, request: web.Request) -> web.Response:
        data = await request.json()
        if self.on_post_tool_use_request:
            await self.on_post_tool_use_request(data.get("session_id", "unknown"), data)
        return web.json_response({"status": "ok"})

    async def _handle_stop(self, request: web.Request) -> web.Response:
        data = await request.json()
        session_id = data.get("session_id", "unknown")

        if self.on_stop_request:
            decision = await self.on_stop_request(session_id, data)
            if decision:
                return web.json_response(decision)

        return web.json_response({})

    async def _handle_notification(self, request: web.Request) -> web.Response:
        data = await request.json()
        await self._emit_event(data, "notification")
        return web.json_response({"status": "ok"})

    async def _handle_session_start(self, request: web.Request) -> web.Response:
        data = await request.json()
        await self._emit_event(data, "iteration_start")
        return web.json_response({"status": "ok"})

    async def _handle_session_end(self, request: web.Request) -> web.Response:
        data = await request.json()
        await self._emit_event(data, "iteration_end")
        return web.json_response({"status": "ok"})

    async def _handle_subagent_start(self, request: web.Request) -> web.Response:
        data = await request.json()
        if self.on_subagent_start_request:
            await self.on_subagent_start_request(data.get("session_id", "unknown"), data)
        return web.json_response({"status": "ok"})

    async def _handle_subagent_stop(self, request: web.Request) -> web.Response:
        data = await request.json()
        if self.on_subagent_stop_request:
            await self.on_subagent_stop_request(data.get("session_id", "unknown"), data)
        return web.json_response({"status": "ok"})

    async def _emit_event(
        self,
        data: dict[str, Any],
        event_type: str,
    ) -> None:
        if self.on_event is None:
            return
        session_id = data.get("session_id", data.get("session", "unknown"))
        tool_name = data.get("tool_name", "")
        await self.on_event(
            {
                "session_id": session_id,
                "event_type": event_type,
                "iteration": 0,
                "data": {
                    "source": "http_hook",
                    "tool_name": tool_name,
                    **data,
                },
            }
        )

    async def _emit_tool_call(self, data: dict[str, Any]) -> None:
        """Emit a normalized tool_call event from hook server post-tool-use data."""
        if self.on_event is None:
            return
        from workflow_controller.session.events import extract_tool_arg, extract_tool_error

        session_id = data.get("session_id", data.get("session", "unknown"))
        tool_input = data.get("tool_input", {})
        tool_response = (
            data.get("tool_response", {}) if isinstance(data.get("tool_response"), dict) else {}
        )
        is_error = data.get("is_error", False) or tool_response.get("is_error", False)
        tool_call_data: dict[str, Any] = {
            "name": data.get("tool_name", ""),
            "arg": extract_tool_arg(tool_input if isinstance(tool_input, dict) else {}),
            "tool_use_id": data.get("tool_use_id", ""),
            "is_error": is_error,
        }
        if is_error:
            error_reason = extract_tool_error(tool_response.get("content", ""))
            if not error_reason:
                error_reason = extract_tool_error(data.get("tool_error", ""))
            if error_reason:
                tool_call_data["error"] = error_reason
        await self.on_event(
            {
                "session_id": session_id,
                "event_type": "tool_call",
                "iteration": 0,
                "data": tool_call_data,
            }
        )
