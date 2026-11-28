"""Verify user input interrupts an active session runtime before resuming it."""

from __future__ import annotations

import asyncio

from workflow_controller.api.session_api import SessionAPI
from workflow_controller.config.settings import (
    AppConfig,
    HookServerConfig,
    ReviewerConfig,
    SessionConfig,
)
from workflow_controller.core.orchestrator import Orchestrator


async def test_send_during_query_drains_before_inject_and_keeps_iteration(tmp_path) -> None:
    """The resumed runtime consumes the new input without rewinding its counter."""
    config = AppConfig(
        project_dir=str(tmp_path),
        hook_server=HookServerConfig(enabled=False),
        reviewer=ReviewerConfig(enabled=False),
    )
    orch = Orchestrator(config, default_project_dir=tmp_path)
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    events: list[tuple[str, int, bool]] = []
    await orch.start()
    session = await orch.pool.create(
        SessionConfig(name="worker", role="worker", cwd=str(tmp_path), initial_prompt="initial")
    )

    async def query(prompt: str, iteration: int = 1):
        events.append(("start", iteration, "User intervention: check old process" in prompt))
        (first_started if not first_started.is_set() else second_started).set()
        try:
            await asyncio.Event().wait()
        finally:
            events.append(("exit", iteration, False))
        if False:
            yield

    session.query = query
    try:
        await orch.play_session(session.id)
        await asyncio.wait_for(first_started.wait(), timeout=5)
        assert session.is_query_active

        resumed = await asyncio.wait_for(
            SessionAPI(orch).send_message(session.id, "check old process"), timeout=10
        )
        await asyncio.wait_for(second_started.wait(), timeout=5)

        assert resumed is True
        assert events[:3] == [("start", 1, False), ("exit", 1, False), ("start", 2, True)]
        assert session.current_iteration == 2
        assert session.is_query_active
    finally:
        await orch.stop_session(session.id)
        await orch.shutdown()
