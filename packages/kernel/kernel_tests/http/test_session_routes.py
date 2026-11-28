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

"""Session 路由的行为断言（cancel-handoff 改判补发、启动参数映射、409 映射与探针）。"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

import workflow_controller.api.session_api as session_api_module
from workflow_controller.api.event_api import EventAPI
from workflow_controller.api.http.routes import sessions_routes
from workflow_controller.api.http.routes.sessions_routes import cancel_handoff_session
from workflow_controller.api.session_api import SessionAPI, WorkflowSessionOperationError


class _HandoffSession:
    """最小 session 桩：仅覆盖 cancel-handoff 路径触达的字段与 emit_outcome。"""

    def __init__(self) -> None:
        self.id = "s1"
        self.role = "handoff_agent"
        self.result: str | None = None
        self.loop_status = "yes"
        self.stop_reason: str | None = None
        self.emitted: list[tuple[str, str | None]] = []
        self.order: list[str] = []

    async def emit_outcome(self, kind: str) -> None:
        self.order.append("emit")
        self.emitted.append((kind, self.stop_reason))


def _request_with(orch) -> SimpleNamespace:
    # 真实 SessionAPI：补发经 api 面走，词表映射保持被测
    api = SimpleNamespace(session=SessionAPI(orch))
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(api=api)))


async def test_cancel_handoff_rewrites_outcome_to_failed() -> None:
    session = _HandoffSession()
    order = session.order
    pool = SimpleNamespace(
        get=lambda _sid: session,
        stop=AsyncMock(side_effect=lambda _sid: order.append("stop")),
        persist=AsyncMock(side_effect=lambda _s: order.append("persist")),
    )
    orch = SimpleNamespace(
        pool=pool,
        deactivate_session=AsyncMock(side_effect=lambda _s, **_kw: order.append("deactivate")),
    )

    resp = await cancel_handoff_session("s1", _request_with(orch))

    assert resp == {"ok": True}
    # 停用 → 停止 → 持久化 session → 补发结局：pool.stop 已发 stopped（user_pause），
    # 改判 failed/user_cancel 是新的阶段事实，须在 session 持久化后补发
    assert order == ["deactivate", "stop", "persist", "emit"]
    assert session.emitted == [("failed", "user_cancel")]
    assert session.result == "fail"
    assert session.loop_status == "no"
    assert session.stop_reason == "user_cancel"


async def test_cancel_handoff_rejects_missing_or_non_handoff_session() -> None:
    missing = SimpleNamespace(pool=SimpleNamespace(get=lambda _sid: None))
    assert await cancel_handoff_session("s1", _request_with(missing)) == {
        "ok": False,
        "error": "Session not found",
    }

    plain = _HandoffSession()
    plain.role = "worker"
    orch = SimpleNamespace(pool=SimpleNamespace(get=lambda _sid: plain))
    assert await cancel_handoff_session("s1", _request_with(orch)) == {
        "ok": False,
        "error": "Not a handoff session",
    }


async def test_start_session_null_cli_wrapper_falls_back_to_project_default() -> None:
    """验证 null wrapper 回退项目默认。"""
    session = AsyncMock()
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(api=SimpleNamespace(session=session))),
    )

    await sessions_routes.start_session(
        {"project_dir": "/proj", "prompt": "task", "cli_wrapper": None},
        request,
    )

    assert session.start.await_args.kwargs["cli_wrapper"] is None


async def test_start_session_legacy_thinking_false_maps_to_disabled() -> None:
    session = AsyncMock()
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(api=SimpleNamespace(session=session))),
    )

    await sessions_routes.start_session(
        {"project_dir": "/proj", "reviewer_thinking_enabled": False},
        request,
    )

    reviewer = session.start.await_args.kwargs["reviewer"]
    assert reviewer.thinking_mode == "disabled"


_SESSION_ROUTES = (
    (sessions_routes.stop_session, "stop"),
    (sessions_routes.pause_session, "pause"),
    (sessions_routes.play_session, "play"),
    (sessions_routes.resume_session, "resume"),
    (sessions_routes.restart_session, "restart"),
    (sessions_routes.inject_prompt, "inject_prompt"),
    (sessions_routes.send_message, "send_message"),
)


async def _call_session_route(route_fn, method: str) -> None:
    exc = WorkflowSessionOperationError("s1", "r9")
    session = SimpleNamespace(**{meth: AsyncMock(side_effect=exc) for _, meth in _SESSION_ROUTES})
    state = SimpleNamespace(api=SimpleNamespace(session=session))
    request = SimpleNamespace(app=SimpleNamespace(state=state))
    if method in ("inject_prompt", "send_message"):
        await route_fn("s1", {"text": "hi"}, request)
    else:
        await route_fn("s1", request)


@pytest.mark.parametrize("route_fn,method", _SESSION_ROUTES)
async def test_workflow_session_rejection_maps_409(route_fn, method) -> None:
    with pytest.raises(HTTPException) as ei:
        await _call_session_route(route_fn, method)

    assert ei.value.status_code == 409
    assert "/api/v1/runs/r9/sessions/s1" in ei.value.detail


async def test_send_restores_evicted_session_before_inject_and_resume() -> None:
    session = SimpleNamespace(state=SimpleNamespace(value="idle"))
    current = [None]
    steps: list[str] = []

    def restore(_sid: str):
        steps.append("restore")
        current[0] = session
        return session

    async def inject(_sid: str, _text: str):
        assert current[0] is session
        steps.append("inject")

    async def resume(_sid: str):
        assert current[0] is session
        steps.append("resume")

    orch = SimpleNamespace(
        affiliation=SimpleNamespace(resolve=lambda _sid: None),
        pool=SimpleNamespace(get=lambda _sid: current[0]),
        restore_session_from_store=restore,
        inject_prompt=inject,
        resume_session=resume,
    )
    assert await SessionAPI(orch).send_message("s1", "hello") is True
    assert steps == ["restore", "inject", "resume"]


# ── 探针（providers / test-model / chain）────────────────────────────────


async def test_providers_prefers_configured_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        session_api_module,
        "list_opencode_providers",
        lambda: ([{"provider": "p", "models": ["m"]}], "p/default"),
    )
    orch = SimpleNamespace(config=SimpleNamespace(model="p/custom"))
    assert await sessions_routes.session_providers(_request_with(orch)) == {
        "providers": [{"provider": "p", "models": ["m"]}],
        "current_model": "p/custom",
    }


async def test_providers_falls_back_to_first_default_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        session_api_module,
        "list_opencode_providers",
        lambda: ([{"provider": "p", "models": ["m"]}], "p/default"),
    )
    orch = SimpleNamespace(config=SimpleNamespace(model=""))
    resp = await sessions_routes.session_providers(_request_with(orch))
    assert resp["current_model"] == "p/default"


async def test_test_model_rejects_invalid_model_format() -> None:
    request = _request_with(SimpleNamespace())
    resp = await sessions_routes.session_test_model({"model": "no-slash"}, request)
    assert resp == {"success": False, "error": "Invalid model format, use provider/model"}


async def test_chain_returns_payload_and_folds_errors() -> None:
    store = SimpleNamespace(
        get_handoff_chain=AsyncMock(return_value={"predecessors": ["a"], "successors": []})
    )
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(api=SimpleNamespace(event=EventAPI(store))))
    )

    assert await sessions_routes.get_session_chain("s1", request) == {
        "predecessors": ["a"],
        "successors": [],
    }

    store.get_handoff_chain = AsyncMock(side_effect=RuntimeError("db busy"))
    assert await sessions_routes.get_session_chain("s1", request) == {
        "error": "RuntimeError: db busy",
    }
