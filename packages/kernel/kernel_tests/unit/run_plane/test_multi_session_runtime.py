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
from unittest.mock import AsyncMock, MagicMock

import pytest

from workflow_controller.config.settings import (
    AppConfig,
    LoopConfig,
    ReviewerConfig,
    SessionConfig,
    SessionReviewerConfig,
)
from workflow_controller.core.loop import SingleLoopEngine
from workflow_controller.core.orchestrator import Orchestrator
from workflow_controller.core.subagent_tracker import SubagentTracker
from workflow_controller.session.events import Event, EventType
from workflow_controller.session.session import Session, SessionState


class _BlockingLoop:
    def __init__(self) -> None:
        self._stop_event = asyncio.Event()
        self._current_session = None
        self._current_task = None
        self._running = False
        self.evaluator = None

    async def take_over(self, session) -> None:
        self._current_session = session
        session.loop_status = "yes"

    def configure_for_session(self, session, evaluator) -> None:
        self.evaluator = evaluator

    async def run(self) -> None:
        self._running = True
        await self._stop_event.wait()
        self._running = False

    def is_current(self, session) -> bool:
        return self._current_session is session

    def current_session_brief(self):
        session = self._current_session
        if session is None:
            return None
        return {"id": session.id, "name": session.name}

    async def pause_current(self) -> None:
        self._current_session.loop_status = "no"

    def request_stop(self) -> None:
        self._stop_event.set()


def _blocking_loop_orchestrator(tmp_path, monkeypatch) -> Orchestrator:
    """建 _BlockingLoop 化的 orchestrator（reviewer 关闭，loop 永不自行结束）。"""
    config = AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False))
    orchestrator = Orchestrator(config, default_project_dir=tmp_path)
    monkeypatch.setattr(orchestrator, "_new_loop", lambda _sid: _BlockingLoop())
    return orchestrator


async def _start_blocking_loop_sessions(orchestrator: Orchestrator, tmp_path, dir_names: list):
    """按目录名批量建 cwd 并发启动 _BlockingLoop worker 会话。"""
    project_dirs = [tmp_path / name for name in dir_names]
    for project_dir in project_dirs:
        project_dir.mkdir()
    return await asyncio.gather(
        *(
            orchestrator.start_session_with_config(
                SessionConfig(
                    name=f"worker-{index}",
                    role="worker",
                    cwd=str(project_dirs[index]),
                    initial_prompt="test",
                )
            )
            for index in range(len(dir_names))
        )
    )


@pytest.mark.asyncio
async def test_five_sessions_run_independently_and_targeted_stop_isolated(
    tmp_path,
    monkeypatch,
) -> None:
    orchestrator = _blocking_loop_orchestrator(tmp_path, monkeypatch)
    loops: list[_BlockingLoop] = []

    def new_loop(_session_id: str) -> _BlockingLoop:
        loop = _BlockingLoop()
        loops.append(loop)
        return loop

    monkeypatch.setattr(orchestrator, "_new_loop", new_loop)

    sessions = await _start_blocking_loop_sessions(
        orchestrator, tmp_path, [f"project-{index}" for index in range(5)]
    )
    await asyncio.sleep(0)

    assert len(getattr(orchestrator, "_loops")) == 5
    assert len(getattr(orchestrator, "_loop_tasks")) == 5
    assert all(session.loop_status == "yes" for session in sessions)

    targeted = sessions[0]
    await orchestrator.pause_session(targeted.id)

    assert targeted.id not in getattr(orchestrator, "_loops")
    assert all(session.id in getattr(orchestrator, "_loops") for session in sessions[1:])
    assert all(
        not getattr(orchestrator, "_loop_tasks")[session.id].done() for session in sessions[1:]
    )

    await orchestrator.resume_session(targeted.id)
    assert targeted.id in getattr(orchestrator, "_loops")
    assert len(getattr(orchestrator, "_loops")) == 5

    await targeted.inject_prompt("only session zero")
    assert await targeted.poll_manual_prompt() == "only session zero"
    other_prompts = await asyncio.gather(
        *(session.poll_manual_prompt() for session in sessions[1:])
    )
    assert all(prompt is None for prompt in other_prompts)

    await orchestrator.stop_session(targeted.id)
    assert targeted.id not in getattr(orchestrator, "_loops")

    await asyncio.gather(*(orchestrator.stop_session(session.id) for session in sessions[1:]))
    assert not getattr(orchestrator, "_loops")
    assert not getattr(orchestrator, "_loop_tasks")


@pytest.mark.asyncio
async def test_running_loop_sessions_reports_watched_sessions_only(
    tmp_path,
    monkeypatch,
) -> None:
    orchestrator = _blocking_loop_orchestrator(tmp_path, monkeypatch)

    def new_loop(_session_id: str) -> _BlockingLoop:
        return _BlockingLoop()

    monkeypatch.setattr(orchestrator, "_new_loop", new_loop)

    sessions = await _start_blocking_loop_sessions(
        orchestrator, tmp_path, ["project-a", "project-b"]
    )
    await asyncio.sleep(0)

    briefs = orchestrator.running_loop_sessions()
    assert briefs == [
        {"id": sessions[0].id, "name": "worker-0"},
        {"id": sessions[1].id, "name": "worker-1"},
    ] or briefs == [
        {"id": sessions[1].id, "name": "worker-1"},
        {"id": sessions[0].id, "name": "worker-0"},
    ]

    await orchestrator.pause_session(sessions[0].id)
    assert orchestrator.running_loop_sessions() == [
        {"id": sessions[1].id, "name": "worker-1"},
    ]

    await asyncio.gather(*(orchestrator.stop_session(s.id) for s in sessions))
    assert orchestrator.running_loop_sessions() == []


@pytest.mark.asyncio
async def test_same_project_sessions_run_concurrently(
    tmp_path,
    monkeypatch,
) -> None:
    """同目录互斥已取消：第二个 session 在第一个运行中即可恢复，两者并存。"""
    config = AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False))
    orchestrator = Orchestrator(config, default_project_dir=tmp_path)
    monkeypatch.setattr(orchestrator, "_new_loop", lambda _sid: _BlockingLoop())

    first = await orchestrator.start_session_with_config(
        SessionConfig(
            name="first",
            role="worker",
            cwd=str(tmp_path),
            initial_prompt="first",
        )
    )
    second = await orchestrator.pool.create(
        SessionConfig(
            name="second",
            role="worker",
            cwd=f"{tmp_path}/",
            initial_prompt="second",
        )
    )

    await orchestrator.resume_session(second.id)

    assert first.id in getattr(orchestrator, "_loops")
    assert second.id in getattr(orchestrator, "_loops")

    await orchestrator.stop_session(first.id)
    assert first.id not in getattr(orchestrator, "_loops")
    assert second.id in getattr(orchestrator, "_loops")
    await orchestrator.stop_session(second.id)


@pytest.mark.asyncio
async def test_concurrent_same_project_start_keeps_both(
    tmp_path,
    monkeypatch,
) -> None:
    """同目录并发启动不再择一淘汰：两个 session 都激活成功。"""
    config = AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False))
    orchestrator = Orchestrator(config, default_project_dir=tmp_path)
    monkeypatch.setattr(orchestrator, "_new_loop", lambda _sid: _BlockingLoop())

    results = await asyncio.gather(
        *(
            orchestrator.start_session_with_config(
                SessionConfig(
                    name=f"worker-{index}",
                    role="worker",
                    cwd=str(tmp_path),
                    initial_prompt="test",
                )
            )
            for index in range(2)
        ),
        return_exceptions=True,
    )

    sessions = [result for result in results if not isinstance(result, BaseException)]
    assert len(sessions) == 2
    assert all(not isinstance(result, BaseException) for result in results)
    assert len(getattr(orchestrator, "_loops")) == 2
    assert set(s.id for s in orchestrator.pool.list_sessions()) == set(s.id for s in sessions)

    await asyncio.gather(*(orchestrator.stop_session(s.id) for s in sessions))


@pytest.mark.asyncio
async def test_activation_failure_rolls_back_session_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    config = AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False))
    orchestrator = Orchestrator(config, default_project_dir=tmp_path)
    partial_reviewer = MagicMock(stop=AsyncMock())

    async def fail_activation(session) -> None:
        getattr(orchestrator, "_session_reviewers")[session.id] = partial_reviewer
        raise RuntimeError("reviewer init failed")

    monkeypatch.setattr(orchestrator, "_activate_session", fail_activation)

    with pytest.raises(RuntimeError, match="reviewer init failed"):
        await orchestrator.start_session_with_config(
            SessionConfig(
                name="failed",
                role="worker",
                cwd=str(tmp_path),
                initial_prompt="test",
            )
        )

    assert orchestrator.pool.list_sessions() == []
    partial_reviewer.stop.assert_awaited_once()
    assert getattr(orchestrator, "_session_reviewers") == {}
    orchestrator.pool.load_sessions()
    assert orchestrator.pool.list_sessions() == []


def _single_loop_test_doubles() -> tuple[MagicMock, MagicMock]:
    router = MagicMock()
    router.update_loop_status = AsyncMock()
    router.store_loop_result = AsyncMock()
    router.get_events = AsyncMock(return_value=[])
    router.store_evaluation = AsyncMock()
    bus = MagicMock()
    bus.emit = AsyncMock()
    return router, bus


@pytest.mark.asyncio
async def test_query_start_discards_stale_subagents_from_previous_iteration() -> None:
    router, bus = _single_loop_test_doubles()
    tracker = SubagentTracker(bus, MagicMock(), lambda _sid: None)
    engine = SingleLoopEngine(router, bus, on_query_start=tracker.clear_session)
    session = Session(
        config=SessionConfig(
            name="worker",
            role="worker",
            initial_prompt="test",
            loop=LoopConfig(max_iterations=2, question_detect=False),
        )
    )
    seen_at_next_query: list[dict] = []

    async def query(_prompt: str, iteration: int = 1):
        if iteration == 1:
            await tracker.on_start(session.id, {"agent_id": "old", "agent_type": "researcher"})
            await tracker.on_start(session.id, {"agent_id": "other", "agent_type": "writer"})
            assert set(tracker.in_flight(session.id)) == {"old", "other"}
        else:
            seen_at_next_query.append(tracker.in_flight(session.id).copy())
        if False:
            yield

    session.query = query  # type: ignore[method-assign]
    await getattr(engine, "_run_iteration")(session)
    # 第一轮 query 结束后在途记录仍在（清理点已移到下一轮 query 开始）
    assert set(tracker.in_flight(session.id)) == {"old", "other"}
    await getattr(engine, "_run_iteration")(session)
    # 第二轮 query 开始时已清空上轮残留，reviewer 读不到
    assert seen_at_next_query == [{}]


@pytest.mark.asyncio
async def test_cancelled_query_keeps_subagents_until_next_query_start() -> None:
    router, bus = _single_loop_test_doubles()
    tracker = SubagentTracker(bus, MagicMock(), lambda _sid: None)
    engine = SingleLoopEngine(router, bus, on_query_start=tracker.clear_session)
    session = Session(config=SessionConfig(name="worker", role="worker", initial_prompt="test"))
    started = asyncio.Event()

    async def query(_prompt: str, iteration: int = 1):
        await tracker.on_start(session.id, {"agent_id": "old", "agent_type": "researcher"})
        started.set()
        await asyncio.Event().wait()
        if False:
            yield

    session.query = query  # type: ignore[method-assign]
    task = asyncio.create_task(getattr(engine, "_run_iteration")(session))
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # 取消不在 query 结束清理——在途记录保留到下一轮 query 开始
    assert set(tracker.in_flight(session.id)) == {"old"}


@pytest.mark.asyncio
async def test_failed_query_keeps_subagents_until_next_query_start() -> None:
    router, bus = _single_loop_test_doubles()
    tracker = SubagentTracker(bus, MagicMock(), lambda _sid: None)
    engine = SingleLoopEngine(router, bus, on_query_start=tracker.clear_session)
    session = Session(config=SessionConfig(name="worker", role="worker", initial_prompt="test"))

    async def query(_prompt: str, iteration: int = 1):
        await tracker.on_start(session.id, {"agent_id": "old", "agent_type": "researcher"})
        if False:
            yield
        raise RuntimeError("query failed")

    session.query = query  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="query failed"):
        await getattr(engine, "_run_iteration")(session)
    # 异常不在 query 结束清理——在途记录保留到下一轮 query 开始
    assert set(tracker.in_flight(session.id)) == {"old"}


@pytest.mark.asyncio
async def test_auth_switch_interrupt_captures_old_driver_without_stopping_session(
    monkeypatch,
) -> None:
    session = Session(config=SessionConfig(name="worker", role="worker"))
    session.state = SessionState.RUNNING
    session.loop_status = "yes"
    old_driver = MagicMock(stop=AsyncMock(), kill=AsyncMock())
    new_driver = MagicMock(stop=AsyncMock(), kill=AsyncMock())
    monkeypatch.setattr(session, "_driver", old_driver)

    interrupt = session.interrupt_for_auth_switch()
    monkeypatch.setattr(session, "_driver", new_driver)
    await interrupt

    old_driver.stop.assert_awaited_once()
    old_driver.kill.assert_awaited_once()
    new_driver.stop.assert_not_awaited()
    new_driver.kill.assert_not_awaited()
    assert session.state is SessionState.RUNNING
    assert session.loop_status == "yes"


@pytest.mark.asyncio
async def test_auth_switch_interrupts_query_and_continues_next_iteration() -> None:
    router, bus = _single_loop_test_doubles()
    engine = SingleLoopEngine(router, bus)
    session = Session(
        config=SessionConfig(
            name="worker",
            role="worker",
            initial_prompt="test",
            loop=LoopConfig(max_iterations=2),
        )
    )
    query_iterations: list[int] = []
    state_seen_by_next_query: list[tuple[SessionState, str, str | None]] = []

    async def query(_prompt: str, iteration: int = 1):
        query_iterations.append(iteration)
        if iteration == 1:
            engine.request_auth_switch_now()
            yield Event.create(
                session_id=session.id,
                iteration=iteration,
                event_type=EventType.ASSISTANT_MESSAGE,
                data={"content": []},
            )
        else:
            state_seen_by_next_query.append(
                (session.state, session.loop_status, session.stop_reason),
            )
            if False:
                yield

    session.query = query  # type: ignore[method-assign]
    await engine.take_over(session)
    engine.configure_for_session(session, None)
    await asyncio.wait_for(engine.run(), timeout=1)

    assert query_iterations == [1, 2]
    assert state_seen_by_next_query == [(SessionState.RUNNING, "yes", None)]
    assert session.last_stop_reason == "user_stop"


@pytest.mark.asyncio
async def test_auth_switch_wakes_manual_wait_and_continues() -> None:
    router, bus = _single_loop_test_doubles()
    query_iterations: list[int] = []
    first_query_done = asyncio.Event()
    engine = SingleLoopEngine(
        router,
        bus,
        mode_getter=lambda: "manual" if len(query_iterations) == 1 else "auto",
    )
    session = Session(
        config=SessionConfig(
            name="worker",
            role="worker",
            initial_prompt="test",
            loop=LoopConfig(max_iterations=2),
        )
    )

    async def query(_prompt: str, iteration: int = 1):
        query_iterations.append(iteration)
        if iteration == 1:
            first_query_done.set()
        if False:
            yield

    session.query = query  # type: ignore[method-assign]
    await engine.take_over(session)
    engine.configure_for_session(session, None)
    run_task = asyncio.create_task(engine.run())
    await first_query_done.wait()
    await asyncio.sleep(0)
    engine.request_auth_switch_now()
    await asyncio.wait_for(run_task, timeout=1)

    assert query_iterations == [1, 2]
    assert session.last_stop_reason == "user_stop"


@pytest.mark.asyncio
async def test_auth_switch_does_not_resurrect_concurrently_stopped_session() -> None:
    router, bus = _single_loop_test_doubles()
    engine = SingleLoopEngine(router, bus)
    session = Session(config=SessionConfig(name="worker", role="worker"))
    session.state = SessionState.RUNNING
    session.loop_status = "yes"

    getattr(engine, "_auth_switch_requested").set()
    await session.stop(reason="user_stop")
    await getattr(engine, "_resume_after_auth_switch")(session)

    assert not getattr(engine, "_auth_switch_requested").is_set()
    assert getattr(session, "_stop_requested") is True
    assert session.state is SessionState.STOPPED
    assert session.loop_status == "no"
    assert session.stop_reason == "user_pause"
    router.update_loop_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_exhaustion_drains_runtime_and_persists_failed(
    tmp_path,
    monkeypatch,
) -> None:
    config = AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False))
    orchestrator = Orchestrator(config, default_project_dir=tmp_path)
    monkeypatch.setattr(orchestrator, "_new_loop", lambda _sid: _BlockingLoop())
    rotator = MagicMock()
    rotator.get_active_provider_index.return_value = 0
    rotator.get_default_provider_index.return_value = None
    rotator.ensure_provider_capability = AsyncMock()
    rotator.rotate_bound_provider.return_value = None
    orchestrator.auth_rotator = rotator
    session = await orchestrator.start_session_with_config(
        SessionConfig(
            name="worker",
            role="worker",
            cwd=str(tmp_path),
            provider_index=0,
            reviewer=SessionReviewerConfig(enabled=False),
            initial_prompt="test",
        )
    )

    handled = await getattr(orchestrator, "_handle_session_limit")(
        Event.create(
            session_id=session.id,
            iteration=1,
            event_type=EventType.RATE_LIMIT,
            data={"limit_signal": {"kind": "rejected", "scope": "5h"}},
        )
    )

    assert handled is True
    assert session.state is SessionState.FAILED
    assert session.loop_status == "no"
    assert session.stop_reason == "error"
    assert session.last_error == "No available provider after rate limit"
    assert session.id not in getattr(orchestrator, "_loops")
    assert session.id not in getattr(orchestrator, "_loop_tasks")


@pytest.mark.asyncio
async def test_session_start_fails_when_no_provider_matches_runtime(
    tmp_path,
    monkeypatch,
) -> None:
    config = AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False))
    orchestrator = Orchestrator(config, default_project_dir=tmp_path)
    monkeypatch.setattr(orchestrator, "_new_loop", lambda _sid: _BlockingLoop())
    rotator = MagicMock()
    rotator.get_default_provider_index.return_value = None
    rotator.get_provider_count.return_value = 1
    rotator.ensure_provider_capability = AsyncMock()
    orchestrator.auth_rotator = rotator

    with pytest.raises(ValueError, match="no provider matches worker runtime opencode"):
        await orchestrator.start_session_with_config(
            SessionConfig(
                name="worker",
                role="worker",
                cwd=str(tmp_path),
                runtime="opencode",
                reviewer=SessionReviewerConfig(enabled=False),
                initial_prompt="test",
            )
        )

    assert orchestrator.pool.list_sessions() == []
    rotator.ensure_provider_capability.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_start_allows_env_only_runtime_without_providers(
    tmp_path,
    monkeypatch,
) -> None:
    config = AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False))
    orchestrator = Orchestrator(config, default_project_dir=tmp_path)
    monkeypatch.setattr(orchestrator, "_new_loop", lambda _sid: _BlockingLoop())
    rotator = MagicMock()
    rotator.get_default_provider_index.return_value = None
    rotator.get_provider_count.return_value = 0
    rotator.ensure_provider_capability = AsyncMock()
    orchestrator.auth_rotator = rotator

    session = await orchestrator.start_session_with_config(
        SessionConfig(
            name="worker",
            role="worker",
            cwd=str(tmp_path),
            runtime="opencode",
            reviewer=SessionReviewerConfig(enabled=False),
            initial_prompt="test",
        )
    )

    assert session.config.provider_index is None
    rotator.ensure_provider_capability.assert_not_awaited()
    await orchestrator.stop_session(session.id)


@pytest.mark.asyncio
async def test_codex_session_requires_explicit_provider_even_when_default_available(
    tmp_path,
    monkeypatch,
) -> None:
    config = AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False))
    orchestrator = Orchestrator(config, default_project_dir=tmp_path)
    monkeypatch.setattr(orchestrator, "_new_loop", lambda _sid: _BlockingLoop())
    rotator = MagicMock()
    rotator.get_default_provider_index.return_value = 0
    rotator.get_provider_count.return_value = 1
    rotator.get_provider.return_value = MagicMock(model="codex-model")
    rotator.ensure_provider_capability = AsyncMock()
    orchestrator.auth_rotator = rotator

    with pytest.raises(ValueError, match="worker runtime codex_cli requires a provider"):
        await orchestrator.start_session_with_config(
            SessionConfig(
                name="worker",
                role="worker",
                cwd=str(tmp_path),
                runtime="codex_cli",
                reviewer=SessionReviewerConfig(enabled=False),
                initial_prompt="test",
            )
        )

    assert orchestrator.pool.list_sessions() == []
    rotator.ensure_provider_capability.assert_not_awaited()


@pytest.mark.asyncio
async def test_auth_error_switch_clears_native_session_before_next_driver(
    tmp_path,
    monkeypatch,
) -> None:
    config = AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False))
    orchestrator = Orchestrator(config, default_project_dir=tmp_path)
    loop = _BlockingLoop()
    monkeypatch.setattr(
        loop,
        "request_auth_switch_now",
        MagicMock(),  # type: ignore[attr-defined]
        raising=False,
    )
    monkeypatch.setattr(orchestrator, "_new_loop", lambda _sid: loop)
    replacement_provider = MagicMock(model="replacement-model")
    rotator = MagicMock()
    rotator.get_default_provider_index.return_value = None
    rotator.ensure_provider_capability = AsyncMock()
    rotator.rotate_bound_provider.return_value = 1
    rotator.get_provider.return_value = replacement_provider
    orchestrator.auth_rotator = rotator
    session = await orchestrator.start_session_with_config(
        SessionConfig(
            name="worker",
            role="worker",
            cwd=str(tmp_path),
            runtime="opencode",
            provider_index=0,
            reviewer=SessionReviewerConfig(enabled=False),
            initial_prompt="test",
        )
    )
    session.native_session_id = "old-opencode-session"

    handled = await getattr(orchestrator, "_handle_session_limit")(
        Event.create(
            session_id=session.id,
            iteration=1,
            event_type=EventType.RATE_LIMIT,
            data={"limit_signal": {"kind": "auth_error", "scope": "auth_error"}},
        )
    )

    assert handled is True
    assert session.config.provider_index == 1
    assert session.native_session_id is None
    loop.request_auth_switch_now.assert_called_once()


@pytest.mark.asyncio
async def test_wait_session_done_accepts_directly_stopped_session(tmp_path) -> None:
    orchestrator = Orchestrator(
        AppConfig(project_dir=str(tmp_path), reviewer=ReviewerConfig(enabled=False)),
        default_project_dir=tmp_path,
    )
    session = await orchestrator.pool.create(
        SessionConfig(
            name="stopped",
            role="worker",
            cwd=str(tmp_path),
        )
    )
    await session.stop()

    assert session.state is SessionState.STOPPED
    assert await orchestrator.wait_session_done(session.id, timeout=0.01)


@pytest.mark.asyncio
async def test_reviewer_for_never_falls_back_to_global_instance(monkeypatch, tmp_path) -> None:
    orchestrator = Orchestrator(
        AppConfig(project_dir=str(tmp_path)),
        default_project_dir=tmp_path,
    )
    session = await orchestrator.pool.create(
        SessionConfig(
            name="worker",
            role="worker",
            cwd=str(tmp_path),
            reviewer=SessionReviewerConfig(enabled=True),
        )
    )
    global_reviewer = MagicMock()
    monkeypatch.setattr(orchestrator, "_reviewer", global_reviewer)

    assert orchestrator.reviewer_for(session.id) is None

    session_reviewer = MagicMock()
    getattr(orchestrator, "_session_reviewers")[session.id] = session_reviewer
    assert orchestrator.reviewer_for(session.id) is session_reviewer
