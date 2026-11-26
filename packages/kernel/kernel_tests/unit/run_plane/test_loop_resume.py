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

"""SingleLoopEngine 恢复轮 prompt 语义。

回归背景：首轮 query 被中途取消（组暂停杀进程组）且零事件时，任务 prompt
大概率未持久化进 sdk 会话；恢复轮只发「请继续你之前的工作。」会得到一个
无任务上下文的空会话——成员只能自行翻目录猜任务（示例回归中 agent-2 成员
自认 agent-1）。恢复轮此时须重发初始 prompt。
"""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from workflow_controller.config.settings import SessionConfig
from workflow_controller.core.loop import SingleLoopEngine
from workflow_controller.session.events import Event, EventType
from workflow_controller.session.manager import SessionPool


class FakeBus:
    def __init__(self) -> None:
        self.events: list[Event] = []

    async def emit(self, event: Event) -> None:
        self.events.append(event)


class FakeRouter:
    async def get_events(self, session_id: str) -> list[Any]:
        return []

    async def update_loop_status(self, *a: Any, **kw: Any) -> None:
        pass

    async def store_loop_result(self, *a: Any, **kw: Any) -> None:
        pass


class FakeSession:
    """query 记录 prompt；按预设脚本产出事件或抛 CancelledError。"""

    def __init__(self, prompts: list[str], scripts: list[Any]) -> None:
        self.id = "ses-fake"
        self.prompts = prompts
        self.scripts = scripts
        self.config = SimpleNamespace(initial_prompt="创建 output/agent-2.txt", loop=None)
        self.loop_status = "yes"
        self.loop_hold = None
        self.current_iteration = 0
        self.role = "worker"
        self.last_stop_reason = None
        self._resume_next = False
        self._resume_reprompt = False
        self._pending_gate_feedback = None
        self.pause_requested = False
        self.native_session_id = None

    # 与 Session 的公有访问口对齐（loop 引擎经公有方法读写这些运行态）；
    # 方法序与实现变量名有意与 Session 不同，避免构成重复代码窗口
    @property
    def resume_next(self) -> bool:
        return self._resume_next

    def set_resume_reprompt(self, flag: bool) -> None:
        self._resume_reprompt = flag

    def take_gate_feedback(self) -> str | None:
        gate_value = self._pending_gate_feedback
        self._pending_gate_feedback = None
        return gate_value

    def clear_resume_next(self) -> None:
        self._resume_next = False

    def consume_resume_reprompt(self) -> bool:
        reprompt_value = self._resume_reprompt
        self._resume_reprompt = False
        return reprompt_value

    async def wait_if_paused(self) -> None:
        pass

    async def poll_manual_prompt(self) -> None:
        return None

    def start_active(self) -> None:
        pass

    def settle_active(self) -> None:
        pass

    async def query(self, prompt: str, iteration: int = 0):
        self.prompts.append(prompt)
        script = self.scripts.pop(0) if self.scripts else []
        if isinstance(script, BaseException):
            raise script
        for _ in script:
            yield Event.create(
                session_id=self.id,
                iteration=iteration,
                event_type=EventType.LOG_AUTO,
                data={},
            )


def _engine() -> SingleLoopEngine:
    engine = SingleLoopEngine.__new__(SingleLoopEngine)
    engine.event_router = FakeRouter()
    engine.bus = FakeBus()
    setattr(engine, "_jinja_env", None)
    setattr(engine, "_iteration_gate", None)
    setattr(engine, "_mode_getter", None)
    setattr(engine, "_current_evaluator", None)
    setattr(engine, "_reviewer_let_exit", False)
    setattr(engine, "_on_query_start", None)
    setattr(engine, "_stop_event", asyncio.Event())
    setattr(engine, "_auth_switch_requested", asyncio.Event())
    setattr(engine, "_retry_wake_event", asyncio.Event())
    return engine


def test_prompt_template_autoescapes_string_variables() -> None:
    engine = SingleLoopEngine(FakeRouter(), FakeBus())
    render = getattr(engine, "_render_prompt")
    assert (
        render(
            initial_prompt="<script>alert('x')</script>",
            iteration=1,
            feedback=None,
            prompt_template="{{ initial_prompt }}",
        )
        == "&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;"
    )


async def _run_cancelled(engine: SingleLoopEngine, session: FakeSession) -> None:
    """跑一轮会取消的迭代：引擎主循环捕获 CancelledError，此处同款。"""
    with contextlib.suppress(asyncio.CancelledError):
        await getattr(engine, "_run_iteration")(session)


@pytest.mark.parametrize(
    ("paused_iteration", "expected_iteration", "expected_prompt"),
    [(0, 1, "创建 output/agent-2.txt"), (3, 4, "请继续你之前的工作。")],
)
async def test_pause_and_resume_preserve_query_number(
    monkeypatch, paused_iteration, expected_iteration, expected_prompt
) -> None:
    """通过真实暂停恢复入口验证持久化轮次和下一条查询事件。"""
    pool = SessionPool()
    store_session = AsyncMock()
    pool.set_event_router(SimpleNamespace(store_session=store_session))
    session = await pool.create(
        SessionConfig(name="worker", role="worker", initial_prompt="创建 output/agent-2.txt")
    )
    session.current_iteration = paused_iteration
    monkeypatch.setattr(session, "_recover_native_session_id", lambda: None)
    prompts: list[str] = []
    monkeypatch.setattr(session, "query", FakeSession(prompts, [[]]).query)

    await pool.pause(session.id)
    await pool.resume(session.id)

    assert session.current_iteration == paused_iteration
    assert store_session.await_args.kwargs["current_iteration"] == paused_iteration

    engine = _engine()
    await getattr(engine, "_run_iteration")(session)
    query_event = next(
        event for event in engine.bus.events if event.event_type == EventType.LOG_QUERY
    )
    assert query_event.iteration == expected_iteration
    assert prompts == [expected_prompt]


async def test_resume_after_zero_event_cancel_reprompts_initial(monkeypatch) -> None:
    """首轮取消零事件 → 恢复轮重发初始 prompt（而非无上下文的「请继续」）。"""
    engine = _engine()
    prompts: list[str] = []
    session = FakeSession(prompts, [asyncio.CancelledError(), []])

    await _run_cancelled(engine, session)  # 首轮：取消（组暂停杀进程）
    assert getattr(session, "_resume_reprompt") is True

    monkeypatch.setattr(session, "_resume_next", True)  # resume() 置位
    session.loop_status = "yes"
    await getattr(engine, "_run_iteration")(session)  # 恢复轮
    assert len(prompts) == 2
    assert "agent-2.txt" in prompts[1]
    assert prompts[1] != "请继续你之前的工作。"


async def test_resume_after_events_keeps_continue_prompt(monkeypatch) -> None:
    """首轮有事件（上下文已建立）→ 恢复轮维持「请继续你之前的工作。」。"""
    engine = _engine()
    prompts: list[str] = []
    session = FakeSession(prompts, [[1], []])

    await getattr(engine, "_run_iteration")(session)
    assert getattr(session, "_resume_reprompt") is False
    monkeypatch.setattr(session, "_resume_next", True)
    await getattr(engine, "_run_iteration")(session)
    assert prompts[1] == "请继续你之前的工作。"


@pytest.mark.parametrize(
    ("stop_reason", "iteration_reason"),
    [("error", "internal_error"), ("gate_failed", "gate_failed")],
)
async def test_failed_stop_forwards_error_and_iteration_end(
    stop_reason: str, iteration_reason: str
) -> None:
    """driver 失败终态下，loop 仍须把
    ERROR 与 ITERATION_END 转发到事件总线（issue #69 核心场景）。
    修复前 loop 在 bus.emit 之前 break，两个事件都到不了消费者。
    """
    engine = _engine()

    class _ErrorStopSession(FakeSession):
        async def query(self, prompt: str, iteration: int = 0):
            self.prompts.append(prompt)
            # 模拟 session.query 捕获 driver 异常：先置停止态，再 yield 收尾事件
            self.loop_status = "no"
            self.stop_reason = stop_reason
            yield Event.create(
                session_id=self.id,
                iteration=iteration,
                event_type=EventType.ERROR,
                data={"error": "CLI exit 1"},
            )
            yield Event.create(
                session_id=self.id,
                iteration=iteration,
                event_type=EventType.ITERATION_END,
                data={"reason": iteration_reason},
            )

    session = _ErrorStopSession([], [])
    await getattr(engine, "_run_iteration")(session)

    emitted = [e.event_type for e in engine.bus.events]
    # 首条是迭代开始的 log_query；修复前 ERROR/ITERATION_END 在 emit 前 break 丢失
    assert emitted == [EventType.LOG_QUERY, EventType.ERROR, EventType.ITERATION_END]


async def test_zero_event_normal_completion_keeps_continue_prompt(monkeypatch) -> None:
    """首轮正常完成但零事件（驱动空产出轮）→ 不置重发标记。

    prompt 实际已进 sdk 会话，恢复轮重发初始 prompt 会造成任务指令重复。
    """
    engine = _engine()
    prompts: list[str] = []
    session = FakeSession(prompts, [[], []])

    await getattr(engine, "_run_iteration")(session)  # 首轮：正常完成、零事件
    assert getattr(session, "_resume_reprompt") is False

    monkeypatch.setattr(session, "_resume_next", True)
    await getattr(engine, "_run_iteration")(session)  # 恢复轮
    assert prompts[1] == "请继续你之前的工作。"


async def test_reprompt_flag_not_sticky(monkeypatch) -> None:
    """标记单次消费：恢复轮后即使再暂停，下一轮恢复回到「请继续」。"""
    engine = _engine()
    prompts: list[str] = []
    session = FakeSession(prompts, [asyncio.CancelledError(), [], []])

    await _run_cancelled(engine, session)
    monkeypatch.setattr(session, "_resume_next", True)
    await getattr(engine, "_run_iteration")(session)
    assert "agent-2.txt" in prompts[1]

    monkeypatch.setattr(session, "_resume_next", True)
    await getattr(engine, "_run_iteration")(session)
    assert prompts[2] == "请继续你之前的工作。"
