from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import pytest

from workflow_controller.driver import codex_store
from workflow_controller.driver.hooks import SubagentProgressBridge

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

from workflow_controller.driver.codex_store import (
    RolloutFileTail,
    RolloutProgressReader,
    SubagentProgressMonitor,
    find_rollout_file,
    rollout_meta_matches,
)
from workflow_controller.driver.hooks import DriverHooks

PARENT_ID = "parent-1"
CHILD_ID = "child-1"


async def wait_until(condition: Callable[[], bool], timeout: float = 1.0) -> None:
    """等待条件成立，避免时间敏感测试依赖固定睡眠。"""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"Condition was not met within {timeout}s")


def write_rollout(
    codex_home: Path,
    child_id: str,
    parent_id: str,
    payload: bytes,
) -> Path:
    """写入带正确首行 meta 的子 rollout 文件。"""
    day_dir = codex_home / "sessions" / "2026" / "11" / "05"
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / f"rollout-2026-11-05T10-00-00-{child_id}.jsonl"
    meta = (
        json.dumps(
            {
                "type": "session_meta",
                "payload": {"id": child_id, "parent_thread_id": parent_id},
            }
        ).encode()
        + b"\n"
    )
    path.write_bytes(meta + payload)
    return path


def record(ordinal: int, record_type: str, payload: dict[str, Any]) -> bytes:
    """构造一行 rollout JSONL。"""
    return (
        json.dumps(
            {
                "ordinal": ordinal,
                "type": record_type,
                "payload": payload,
            }
        ).encode()
        + b"\n"
    )


@pytest.mark.asyncio
async def test_monitor_buffers_until_start_then_streams_and_completes(tmp_path):
    progress: list[tuple[str, list[dict[str, Any]]]] = []

    async def on_progress(_session_id: str, data: dict[str, Any]) -> None:
        progress.append((data["agent_id"], data["messages"]))

    hooks = DriverHooks(session_id="sid", on_subagent_progress=on_progress)
    write_rollout(
        tmp_path,
        CHILD_ID,
        PARENT_ID,
        record(
            1,
            "response_item",
            {
                "type": "reasoning",
                "content": [{"type": "reasoning_text", "text": "想"}],
            },
        ),
    )
    monitor = SubagentProgressMonitor(
        hooks=hooks,
        session_id="sid",
        codex_home=tmp_path,
        poll_interval=0.01,
        discovery_timeout=1.0,
    )
    await monitor.handle_fact({"kind": "parent_started", "parent_id": PARENT_ID})
    await monitor.handle_fact(
        {
            "kind": "collab",
            "tool": "spawn_agent",
            "child_ids": [CHILD_ID],
            "states": {},
        }
    )
    expected_initial_progress = [{"type": "thinking", "text": "想"}]
    await wait_until(lambda: monitor._children[CHILD_ID].buffer == expected_initial_progress)
    assert progress == []

    await monitor.notify_subagent_started(CHILD_ID)
    assert progress == [(CHILD_ID, expected_initial_progress)]

    rollout = find_rollout_file(tmp_path, CHILD_ID)
    assert rollout is not None and rollout_meta_matches(rollout, CHILD_ID, PARENT_ID)
    with rollout.open("ab") as file:
        file.write(
            record(
                2,
                "response_item",
                {
                    "type": "function_call",
                    "call_id": "call-1",
                    "name": "exec",
                    "arguments": '{"cmd": "ls"}',
                },
            )
        )
        file.write(
            record(
                3,
                "event_msg",
                {
                    "type": "item_completed",
                    "item": {
                        "type": "CommandExecution",
                        "id": "call-1",
                        "status": "completed",
                        "aggregated_output": "a.py",
                        "exit_code": 0,
                    },
                },
            )
        )
        file.write(
            record(
                4,
                "response_item",
                {
                    "type": "function_call_output",
                    "call_id": "call-1",
                    "output": "重复",
                },
            )
        )
        file.write(
            record(
                5,
                "event_msg",
                {
                    "type": "item_completed",
                    "item": {
                        "type": "AgentMessage",
                        "content": [{"type": "Text", "text": "完成"}],
                    },
                },
            )
        )
        file.write(record(6, "event_msg", {"type": "task_complete"}))
    expected_final_types = [
        "tool_use",
        "tool_result",
        "text",
    ]
    await wait_until(
        lambda: [message["type"] for message in progress[-1][1]] == expected_final_types
    )
    assert progress[-1][1][1]["tool_use_id"] == "call-1"
    assert progress[-1][1][1]["content"] == "a.py"
    assert monitor._children[CHILD_ID].is_completed
    progress_before_late_stop = len(progress)
    await monitor.notify_subagent_stopped(CHILD_ID)
    await asyncio.sleep(0.02)
    assert len(progress) == progress_before_late_stop
    child = monitor._children[CHILD_ID]
    await monitor._emit_child_progress(child, [{"type": "text", "text": "迟到"}])
    assert child.buffer == []
    await monitor.finish()


@pytest.mark.asyncio
async def test_child_thread_started_does_not_replace_parent(tmp_path):
    monitor = SubagentProgressMonitor(
        hooks=DriverHooks(session_id="sid"),
        session_id="sid",
        codex_home=tmp_path,
    )
    await monitor.handle_fact({"kind": "parent_started", "parent_id": PARENT_ID})
    await monitor.handle_fact({"kind": "parent_started", "parent_id": CHILD_ID})

    assert monitor._parent_thread_id == PARENT_ID
    await monitor.finish()


@pytest.mark.asyncio
async def test_stop_flushes_final_progress_batch(tmp_path):
    progress: list[tuple[str, list[dict[str, Any]]]] = []

    async def on_progress(_session_id: str, data: dict[str, Any]) -> None:
        progress.append((data["agent_id"], data["messages"]))

    rollout = write_rollout(
        tmp_path,
        CHILD_ID,
        PARENT_ID,
        record(
            1,
            "response_item",
            {"type": "reasoning", "content": [{"type": "reasoning_text", "text": "想"}]},
        ),
    )
    monitor = SubagentProgressMonitor(
        hooks=DriverHooks(session_id="sid", on_subagent_progress=on_progress),
        session_id="sid",
        codex_home=tmp_path,
        poll_interval=0.01,
        discovery_timeout=1.0,
    )
    await monitor.handle_fact({"kind": "parent_started", "parent_id": PARENT_ID})
    await monitor.handle_fact(
        {
            "kind": "collab",
            "tool": "spawn_agent",
            "child_ids": [CHILD_ID],
            "states": {},
        }
    )
    await monitor.notify_subagent_started(CHILD_ID)
    await asyncio.sleep(0.03)
    with rollout.open("ab") as file:
        file.write(
            record(
                2,
                "event_msg",
                {
                    "type": "item_completed",
                    "item": {
                        "type": "AgentMessage",
                        "content": [{"type": "Text", "text": "最终答复"}],
                    },
                },
            )
        )
    await monitor.notify_subagent_stopped(CHILD_ID)
    await asyncio.sleep(0.02)

    assert progress[-1] == (CHILD_ID, [{"type": "text", "text": "最终答复"}])
    assert monitor._children[CHILD_ID].is_completed
    await monitor.finish()


@pytest.mark.asyncio
async def test_stop_during_discovery_reads_final_rollout(tmp_path, monkeypatch):
    progress: list[tuple[str, list[dict[str, Any]]]] = []

    async def on_progress(_session_id: str, data: dict[str, Any]) -> None:
        progress.append((data["agent_id"], data["messages"]))

    write_rollout(
        tmp_path,
        CHILD_ID,
        PARENT_ID,
        record(
            1,
            "response_item",
            {"type": "reasoning", "content": [{"type": "reasoning_text", "text": "想"}]},
        )
        + record(2, "event_msg", {"type": "task_complete"}),
    )
    first_find_finished = asyncio.Event()
    actual_find_rollout_file = codex_store.find_rollout_file
    find_count = 0

    def find_after_first_stop(*args: object) -> object:
        nonlocal find_count
        find_count += 1
        if find_count == 1:
            first_find_finished.set()
            return None
        return actual_find_rollout_file(*args)

    monkeypatch.setattr(codex_store, "find_rollout_file", find_after_first_stop)
    monitor = SubagentProgressMonitor(
        hooks=DriverHooks(session_id="sid", on_subagent_progress=on_progress),
        session_id="sid",
        codex_home=tmp_path,
        poll_interval=0.01,
        discovery_timeout=1.0,
    )
    await monitor.handle_fact({"kind": "parent_started", "parent_id": PARENT_ID})
    await monitor.handle_fact(
        {
            "kind": "collab",
            "tool": "spawn_agent",
            "child_ids": [CHILD_ID],
            "states": {},
        }
    )
    await monitor.notify_subagent_started(CHILD_ID)
    await first_find_finished.wait()
    await asyncio.sleep(0.03)
    await monitor.notify_subagent_stopped(CHILD_ID)
    await asyncio.sleep(0.03)

    assert progress == [(CHILD_ID, [{"type": "thinking", "text": "想"}])]
    assert monitor._children[CHILD_ID].is_completed
    await monitor.finish()


@pytest.mark.asyncio
async def test_late_start_flushes_buffered_completed_rollout(tmp_path):
    progress: list[tuple[str, list[dict[str, Any]]]] = []

    async def on_progress(_session_id: str, data: dict[str, Any]) -> None:
        progress.append((data["agent_id"], data["messages"]))

    write_rollout(
        tmp_path,
        CHILD_ID,
        PARENT_ID,
        record(
            1,
            "response_item",
            {"type": "reasoning", "content": [{"type": "reasoning_text", "text": "想"}]},
        )
        + record(2, "event_msg", {"type": "task_complete"}),
    )
    monitor = SubagentProgressMonitor(
        hooks=DriverHooks(session_id="sid", on_subagent_progress=on_progress),
        session_id="sid",
        codex_home=tmp_path,
        poll_interval=0.01,
        discovery_timeout=1.0,
    )
    await monitor.handle_fact({"kind": "parent_started", "parent_id": PARENT_ID})
    await monitor.handle_fact(
        {
            "kind": "collab",
            "tool": "spawn_agent",
            "child_ids": [CHILD_ID],
            "states": {},
        }
    )
    await asyncio.sleep(0.05)
    assert progress == []
    assert monitor._children[CHILD_ID].is_completed

    await monitor.notify_subagent_started(CHILD_ID)
    assert progress == [(CHILD_ID, [{"type": "thinking", "text": "想"}])]
    await monitor.finish()


@pytest.mark.asyncio
async def test_completed_child_stops_discovery_early(tmp_path):
    monitor = SubagentProgressMonitor(
        hooks=DriverHooks(session_id="sid"),
        session_id="sid",
        codex_home=tmp_path,
        poll_interval=0.01,
        discovery_timeout=5.0,
    )
    await monitor.handle_fact(
        {
            "kind": "collab",
            "tool": "spawn_agent",
            "child_ids": [CHILD_ID],
            "states": {},
        }
    )
    await monitor.notify_subagent_stopped(CHILD_ID)
    await asyncio.sleep(0.05)

    child = monitor._children[CHILD_ID]
    assert child.is_completed
    assert child.task is not None and child.task.done()
    await monitor.finish()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["completed", "interrupted", "errored", "shutdown", "not_found"])
async def test_wait_close_terminal_state_releases_child(tmp_path, state):
    monitor = SubagentProgressMonitor(
        hooks=DriverHooks(session_id="sid"),
        session_id="sid",
        codex_home=tmp_path,
        poll_interval=0.01,
        discovery_timeout=5.0,
    )
    await monitor.handle_fact(
        {
            "kind": "collab",
            "tool": "spawn_agent",
            "child_ids": [CHILD_ID],
            "states": {},
        }
    )
    await monitor.handle_fact(
        {
            "kind": "collab",
            "tool": "wait",
            "child_ids": [],
            "states": {CHILD_ID: state},
        }
    )
    await asyncio.sleep(0.05)

    child = monitor._children[CHILD_ID]
    assert child.is_completed
    assert child.task is not None and child.task.done()
    await monitor.finish()


@pytest.mark.asyncio
async def test_monitor_rejects_wrong_parent_and_finds_delayed_rollout(tmp_path):
    progress: list[dict[str, Any]] = []

    async def on_progress(_session_id: str, data: dict[str, Any]) -> None:
        progress.append(data)

    hooks = DriverHooks(session_id="sid", on_subagent_progress=on_progress)
    write_rollout(
        tmp_path,
        CHILD_ID,
        "other-parent",
        record(
            1,
            "event_msg",
            {
                "type": "item_completed",
                "item": {"type": "AgentMessage", "content": [{"type": "Text", "text": "错"}]},
            },
        ),
    )
    monitor = SubagentProgressMonitor(
        hooks=hooks,
        session_id="sid",
        codex_home=tmp_path,
        poll_interval=0.01,
        discovery_timeout=0.2,
    )
    await monitor.handle_fact({"kind": "parent_started", "parent_id": PARENT_ID})
    await monitor.handle_fact(
        {
            "kind": "collab",
            "tool": "spawn_agent",
            "child_ids": [CHILD_ID],
            "states": {},
        }
    )
    await monitor.notify_subagent_started(CHILD_ID)
    await asyncio.sleep(0.05)
    assert progress == []

    write_rollout(
        tmp_path,
        CHILD_ID,
        PARENT_ID,
        record(
            1,
            "event_msg",
            {
                "type": "item_completed",
                "item": {"type": "AgentMessage", "content": [{"type": "Text", "text": "对"}]},
            },
        ),
    )
    await asyncio.sleep(0.15)
    assert progress and progress[-1]["messages"] == [{"type": "text", "text": "对"}]
    await monitor.finish()


@pytest.mark.asyncio
async def test_bridge_notifies_and_releases_monitor(tmp_path):
    bridge = SubagentProgressBridge()
    monitor = SubagentProgressMonitor(
        hooks=DriverHooks(session_id="sid", subagent_progress_bridge=bridge),
        session_id="sid",
        codex_home=tmp_path,
    )
    bridge.register(monitor)
    await monitor.handle_fact(
        {
            "kind": "collab",
            "tool": "spawn_agent",
            "child_ids": [CHILD_ID],
            "states": {},
        }
    )
    await bridge.notify_started(CHILD_ID)
    assert CHILD_ID in monitor._started_children
    await monitor.notify_subagent_stopped(CHILD_ID)
    await monitor.finish()
    bridge.unregister(monitor)
    assert bridge._monitor is None


@pytest.mark.asyncio
async def test_shared_codex_home_does_not_cross_sessions(tmp_path):
    received: dict[str, list[str]] = {"sid-a": [], "sid-b": []}

    def make_hooks(session_id: str) -> DriverHooks:
        async def on_progress(sid: str, data: dict[str, Any]) -> None:
            received[sid].append(data["agent_id"])

        return DriverHooks(session_id=session_id, on_subagent_progress=on_progress)

    write_rollout(
        tmp_path,
        "child-a",
        "parent-a",
        record(
            1,
            "event_msg",
            {
                "type": "item_completed",
                "item": {"type": "AgentMessage", "content": [{"type": "Text", "text": "a"}]},
            },
        ),
    )
    write_rollout(
        tmp_path,
        "child-b",
        "parent-b",
        record(
            1,
            "event_msg",
            {
                "type": "item_completed",
                "item": {"type": "AgentMessage", "content": [{"type": "Text", "text": "b"}]},
            },
        ),
    )
    monitors = {}
    for sid, parent_id, child_id in (
        ("sid-a", "parent-a", "child-a"),
        ("sid-b", "parent-b", "child-b"),
    ):
        monitor = SubagentProgressMonitor(
            hooks=make_hooks(sid),
            session_id=sid,
            codex_home=tmp_path,
            poll_interval=0.01,
            discovery_timeout=1.0,
        )
        monitors[sid] = monitor
        await monitor.handle_fact({"kind": "parent_started", "parent_id": parent_id})
        await monitor.handle_fact(
            {
                "kind": "collab",
                "tool": "spawn_agent",
                "child_ids": [child_id],
                "states": {},
            }
        )
        await monitor.notify_subagent_started(child_id)
    await asyncio.sleep(0.1)
    for monitor in monitors.values():
        await monitor.finish()
    assert received == {"sid-a": ["child-a"], "sid-b": ["child-b"]}


def test_tail_consumes_only_complete_lines(tmp_path):
    path = tmp_path / "rollout.jsonl"
    path.write_bytes(b'{"ordinal":1}\n{"ordinal":2}')
    tail = RolloutFileTail(path)

    assert tail.read_lines() == [b'{"ordinal":1}']
    with path.open("ab") as file:
        file.write(b'\n{"ordinal":3}\n')
    assert tail.read_lines() == [b'{"ordinal":2}', b'{"ordinal":3}']
    assert tail.read_lines() == []


def test_reader_reads_reasoning_summary_without_content():
    reader = RolloutProgressReader()
    line = record(
        1,
        "response_item",
        {
            "type": "reasoning",
            "summary": [{"type": "summary_text", "text": "总结了步骤"}],
        },
    )

    assert reader.read_messages([line]) == [{"type": "thinking", "text": "总结了步骤"}]


def test_reader_dedups_and_truncates(monkeypatch):

    monkeypatch.setattr(codex_store, "OUTPUT_TEXT_LIMIT", 4)
    reader = RolloutProgressReader()
    line = record(
        1,
        "response_item",
        {
            "type": "function_call_output",
            "call_id": "call-1",
            "output": "abcdefgh",
        },
    )

    messages = reader.read_messages([line, line])
    assert messages == [
        {
            "type": "tool_result",
            "tool_use_id": "call-1",
            "content": "abcd…[截断]",
            "is_error": None,
        }
    ]
    assert reader.read_messages([line]) == []


def test_reader_infers_protocol_output_error_from_content():
    reader = RolloutProgressReader()
    line = record(
        1,
        "response_item",
        {
            "type": "function_call_output",
            "call_id": "call-1",
            "output": [
                {"type": "input_text", "text": "Output:"},
                {"type": "input_text", "text": '{"error":"search failed"}'},
            ],
        },
    )

    assert reader.read_messages([line])[0]["is_error"] is True


def test_reader_marks_plain_protocol_output_status_unknown():
    reader = RolloutProgressReader()
    line = record(
        1,
        "response_item",
        {"type": "function_call_output", "call_id": "call-1", "output": "plain result"},
    )

    assert reader.read_messages([line])[0]["is_error"] is None


@pytest.mark.parametrize(
    ("status", "expected_error"),
    [("declined", True), ("", None)],
)
def test_reader_reads_declined_and_unknown_command_status(status, expected_error):
    reader = RolloutProgressReader()
    line = record(
        1,
        "event_msg",
        {
            "type": "item_completed",
            "item": {
                "type": "CommandExecution",
                "id": "call-1",
                "status": status,
                "aggregated_output": "done",
            },
        },
    )

    assert reader.read_messages([line])[0]["is_error"] is expected_error


def test_reader_prefers_command_execution_result():
    reader = RolloutProgressReader()
    lines = [
        record(
            1,
            "response_item",
            {"type": "function_call", "call_id": "call-1", "name": "exec_command"},
        ),
        record(
            2,
            "response_item",
            {"type": "function_call_output", "call_id": "call-1", "output": "fallback"},
        ),
        record(
            3,
            "event_msg",
            {
                "type": "item_completed",
                "item": {
                    "type": "CommandExecution",
                    "id": "call-1",
                    "status": "failed",
                    "exit_code": 2,
                    "aggregated_output": "boom",
                },
            },
        ),
    ]

    messages = reader.read_messages(lines)
    assert messages[-1] == {
        "type": "tool_result",
        "tool_use_id": "call-1",
        "content": "boom",
        "is_error": True,
    }


def test_reader_uses_command_output_as_fallback():
    reader = RolloutProgressReader()
    lines = [
        record(
            1,
            "response_item",
            {"type": "function_call", "call_id": "call-1", "name": "exec_command"},
        ),
        record(
            2,
            "response_item",
            {
                "type": "function_call_output",
                "call_id": "call-1",
                "output": "Process exited with code 2\nfailed",
            },
        ),
        record(3, "event_msg", {"type": "task_complete"}),
    ]

    messages = reader.read_messages(lines)
    assert messages[-1]["is_error"] is True
