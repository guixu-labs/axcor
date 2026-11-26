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
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from workflow_controller.core.context_reader import (
    claude_transcript_path,
    read_claude_context,
    read_main_context,
    read_opencode_context,
    read_subagent_context,
    snapshot_main_source,
    snapshot_source,
)
from workflow_controller.core.subagent_tracker import SubagentTracker
from workflow_controller.driver.claude_cli import ClaudeCLIDriver, _poll_cli_subagents
from workflow_controller.driver.hooks import DriverHooks
from workflow_controller.plugins.breather.group_handoff import GroupHandoff
from workflow_controller.plugins.breather.handoff import HandoffGenerator, handoff_template_for_mode
from workflow_controller.session.events import Event, EventType
from workflow_controller.session.session import SessionState

from ._helpers import (
    append_transcript_row,
    claude_child_transcript,
    cli_child_done_row,
    cli_child_running_row,
)


def services_for(orch):
    pool = getattr(orch, "pool", SimpleNamespace())
    router = getattr(orch, "event_router", SimpleNamespace())
    tracker = getattr(orch, "_subagents", SimpleNamespace())
    return SimpleNamespace(
        find_session=getattr(pool, "get", lambda _id: None),
        session_events=getattr(router, "get_events_sync", lambda _id: []),
        list_session_successors=getattr(router, "get_successors_sync", lambda _id: []),
        add_handoff_relation=getattr(router, "add_handoff_relation", AsyncMock()),
        in_flight_subagents=getattr(tracker, "in_flight", lambda _id: {}),
        interrupt_subagents=getattr(tracker, "interrupt_all", AsyncMock()),
        pause_session=getattr(orch, "pause_session", AsyncMock()),
        resume_session=getattr(orch, "resume_session", AsyncMock()),
        stop_session=getattr(orch, "stop_session", AsyncMock()),
        start_session=getattr(orch, "start_session_with_config", AsyncMock()),
        wait_session_done=getattr(orch, "wait_session_done", AsyncMock(return_value=True)),
        inject_prompt=getattr(pool, "inject_prompt", AsyncMock()),
        persist_session=getattr(pool, "_persist_session", AsyncMock()),
        default_runtime=lambda: getattr(getattr(orch, "config", None), "runtime", "opencode"),
        read_main_context=read_main_context,
        read_subagent_context=read_subagent_context,
        snapshot_main_source=snapshot_main_source,
        snapshot_subagent_source=snapshot_source,
    )


def _spawn_cli_child_poller(tmp_path, on_start, on_stop):
    """起 _poll_cli_subagents 任务：parent 会话、native sid、无已见子代理。"""
    hooks = SimpleNamespace(
        session_id="parent", on_subagent_start=on_start, on_subagent_stop=on_stop
    )
    ready = asyncio.Event()
    ready.set()
    return asyncio.create_task(
        _poll_cli_subagents(
            hooks,
            str(tmp_path),
            {"HOME": str(tmp_path)},
            ready,
            lambda: "native",
            set(),
        )
    )


def _dispatch_verify_plugin(events, *, state=SessionState.RUNNING, stop_reason=None):
    """_verify_dispatch 用 plugin 桩：pool.get→continuation session，事件取 events。"""
    session = SimpleNamespace(state=state, pause_requested=False, stop_reason=stop_reason)
    bus = SimpleNamespace(emit=AsyncMock())
    plugin = SimpleNamespace(
        services=services_for(
            SimpleNamespace(
                pool=SimpleNamespace(get=lambda _sid: session),
                event_router=SimpleNamespace(get_events_sync=lambda _sid: events),
            )
        ),
        ctx=SimpleNamespace(bus=bus),
    )
    return session, plugin, bus


def _group_handoff_parent_state_session(tmp_path, prior_handoffs=0):
    """group handoff 的共享 handoff state + parent worker 会话（opencode runtime）。"""
    state = SimpleNamespace(pending=False, finalizing=False, session_id="", started_at=0)
    cfg = SimpleNamespace(
        cwd=str(tmp_path),
        runtime="opencode",
        env_overrides={},
        model="opencode/beam-free",
        provider_index=None,
        group_handoff_prompt_path="group.md" if prior_handoffs else None,
    )
    session = SimpleNamespace(
        id="parent",
        name="worker",
        config=cfg,
        native_session_id="native",
        run_id=None,
        pause_requested=False,
        birth_handoff_doc="previous-handoff-doc" if prior_handoffs else None,
    )
    return state, session


def _group_handoff_orch(
    agents,
    *,
    events=None,
    successor_count=0,
    paused=None,
    resumed=None,
    injected=None,
    interrupted=None,
    persist=None,
):
    """group handoff 流程用 orch 桩：pause/resume/inject/interrupt/persist 按需注入。"""
    router = SimpleNamespace(
        get_events_sync=lambda _id: events or [],
        get_successors_sync=lambda _id: [None] * successor_count,
        add_handoff_relation=AsyncMock(),
    )
    return SimpleNamespace(
        pause_session=paused or AsyncMock(),
        resume_session=resumed or AsyncMock(),
        stop_session=AsyncMock(),
        event_router=router,
        pool=SimpleNamespace(
            inject_prompt=injected or AsyncMock(), _persist_session=persist or AsyncMock()
        ),
        _subagents=SimpleNamespace(
            interrupt_all=interrupted or AsyncMock(), in_flight=lambda _id: agents
        ),
    )


def _group_handoff_plugin(orch, generator, state, agent_timeout=None, main_threshold=70):
    """group handoff 用 plugin 桩；handoff_agent_timeout 与阈值可经参数覆盖。"""
    return SimpleNamespace(
        services=services_for(orch),
        ctx=SimpleNamespace(),
        cfg=lambda key, default=None: (
            agent_timeout
            if key == "handoff_agent_timeout" and agent_timeout is not None
            else default
        ),
        threshold_pct=lambda _session: main_threshold,
        context_window=lambda _session: 1000,
        check_context_from_events=lambda _s: SimpleNamespace(percentage=10, max_tokens=1000),
        effective_generator=lambda _s: generator,
        state_for=lambda _id: state,
        reset_handoff_state=lambda _id: None,
    )


def _wire_successor_start(orch, start):
    """orch 接上 start 桩与常绿 wait；continuation 配置由返回的 generator 生成。"""
    generator = SimpleNamespace(
        continuation_session_name=lambda _sid, _number: "continuation",
        build_new_session_config=lambda _session, _path, _name: SimpleNamespace(
            role="worker", initial_prompt="", env_overrides={}
        ),
    )
    orch.start_session_with_config = start
    orch.wait_session_done = AsyncMock(return_value=True)
    return generator


def _monitor_session(tmp_path, child_threshold=None):
    """_monitor 用 parent 会话；child_threshold 为 None 时不带覆盖字段。"""
    cfg_kwargs: dict = {"cwd": str(tmp_path), "runtime": "opencode", "env_overrides": {}}
    if child_threshold is not None:
        cfg_kwargs["subagent_context_threshold_pct"] = child_threshold
    return SimpleNamespace(
        id="parent",
        name="worker",
        pause_requested=False,
        config=SimpleNamespace(**cfg_kwargs),
        native_session_id="native",
        current_iteration=1,
    )


def _monitor_plugin(session, in_flight_agents, subagent_threshold):
    """_monitor 用 plugin 桩：bus 收集事件，按 in_flight 子代理读上下文。"""
    emitted = []

    async def emit(event):
        emitted.append(event)

    plugin = SimpleNamespace(
        services=services_for(
            SimpleNamespace(
                pool=SimpleNamespace(get=lambda _id: session),
                config=SimpleNamespace(runtime="opencode"),
                _subagents=SimpleNamespace(in_flight=lambda _id: in_flight_agents),
            )
        ),
        ctx=SimpleNamespace(bus=SimpleNamespace(emit=emit)),
        installed=True,
        cfg=lambda _key, _default=None: 1000,
        threshold_pct=lambda _session: 70,
        subagent_threshold_pct=lambda _session: subagent_threshold,
        context_window=lambda _session: 1000,
    )
    return plugin, emitted


def test_claude_context_uses_latest_assistant_input(tmp_path: Path) -> None:
    transcript = tmp_path / "agent-a.jsonl"
    transcript_rows = (
        {
            "type": "assistant",
            "message": {
                "usage": {
                    "input_tokens": 100,
                    "cache_read_input_tokens": 40,
                    "cache_creation_input_tokens": 10,
                }
            },
        },
        {"type": "user", "message": {"content": "tool result"}},
        {
            "type": "assistant",
            "message": {
                "usage": {
                    "input_tokens": 20,
                    "cache_read_input_tokens": 230,
                    "cache_creation_input_tokens": 0,
                }
            },
        },
    )
    transcript.write_text(
        "\n".join(json.dumps(row) for row in transcript_rows) + "\n{partial",
        encoding="utf-8",
    )
    assert read_claude_context(transcript) == 250


def test_claude_transcript_finds_cli_process_cwd(tmp_path: Path) -> None:
    transcript = (
        tmp_path
        / ".claude"
        / "projects"
        / "-actual-cwd"
        / "native"
        / "subagents"
        / "agent-child.jsonl"
    )
    transcript.parent.mkdir(parents=True)
    transcript.write_text("", encoding="utf-8")
    assert (
        claude_transcript_path(
            "/configured/add-dir",
            "native",
            "child",
            {"HOME": str(tmp_path)},
        )
        == transcript
    )
    main = transcript.parent.parent.parent / "native.jsonl"
    main.write_text(
        json.dumps(
            {
                "type": "assistant",
                "message": {
                    "usage": {
                        "input_tokens": 10,
                        "cache_read_input_tokens": 490,
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    assert (
        read_main_context("claude_cli", "/configured/add-dir", "native", {"HOME": str(tmp_path)})
        == 500
    )


def test_claude_cli_discovers_and_finishes_child(tmp_path: Path) -> None:
    async def run() -> None:
        transcript = claude_child_transcript(tmp_path)
        append_transcript_row(transcript, cli_child_running_row())
        start_seen = asyncio.Event()
        stop_seen = asyncio.Event()
        started = []
        stopped = []

        async def on_start(_sid, data):
            started.append(data)
            start_seen.set()

        async def on_stop(_sid, data):
            stopped.append(data)
            stop_seen.set()

        task = _spawn_cli_child_poller(tmp_path, on_start, on_stop)
        try:
            await asyncio.wait_for(start_seen.wait(), timeout=2)
            assert not stopped
            append_transcript_row(transcript, cli_child_done_row())
            await asyncio.wait_for(stop_seen.wait(), timeout=2)
            assert len(started) == 1
            assert started[0]["agent_type"] == "explore"
            assert stopped[0]["agent_id"] == "child"
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(run())


def _append_child_type_row(transcript: Path, stop_reason: str) -> None:
    """向子代理 transcript 追加一行 reader 归属的 assistant 记录。"""
    with transcript.open("a", encoding="utf-8") as out:
        out.write(
            json.dumps(
                {
                    "type": "assistant",
                    "attributionAgent": "reader",
                    "message": {"stop_reason": stop_reason},
                }
            )
            + "\n"
        )


def _child_type_upgrade_hooks() -> SimpleNamespace:
    """记录 start/stop，并在首次 start、类型升级、结束时各置一个事件。"""
    first, upgraded, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()
    starts: list = []
    stops: list = []

    async def on_start(_sid, data):
        starts.append(data)
        (first if len(starts) == 1 else upgraded).set()

    async def on_stop(_sid, data):
        stops.append(data)
        stopped.set()

    hooks = SimpleNamespace(
        session_id="parent", on_subagent_start=on_start, on_subagent_stop=on_stop
    )
    return SimpleNamespace(
        first=first, upgraded=upgraded, stopped=stopped, starts=starts, stops=stops, hooks=hooks
    )


def test_claude_cli_upgrades_child_type_after_first_assistant_row(tmp_path: Path) -> None:
    async def run() -> None:
        project = str(tmp_path.resolve()).replace("/", "-")
        directory = tmp_path / ".claude" / "projects" / project / "native" / "subagents"
        directory.mkdir(parents=True)
        transcript = directory / "agent-child.jsonl"
        transcript.write_text(
            json.dumps({"type": "user", "agentId": "child"}) + "\n", encoding="utf-8"
        )
        rig = _child_type_upgrade_hooks()
        ready = asyncio.Event()
        ready.set()
        task = asyncio.create_task(
            _poll_cli_subagents(
                rig.hooks,
                str(tmp_path),
                {"HOME": str(tmp_path)},
                ready,
                lambda: "native",
                set(),
            )
        )
        try:
            await asyncio.wait_for(rig.first.wait(), timeout=2)
            _append_child_type_row(transcript, "tool_use")
            await asyncio.wait_for(rig.upgraded.wait(), timeout=2)
            _append_child_type_row(transcript, "end_turn")
            await asyncio.wait_for(rig.stopped.wait(), timeout=2)
            assert [item["agent_type"] for item in rig.starts] == ["general-purpose", "reader"]
            assert rig.stops[0]["agent_type"] == "reader"
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_claude_cli_poller_interrupts_in_flight_child_on_cancel(tmp_path: Path) -> None:
    async def run() -> None:
        transcript = claude_child_transcript(tmp_path)
        append_transcript_row(transcript, cli_child_running_row())
        start_seen = asyncio.Event()
        stopped = []

        async def on_start(_sid, data):
            start_seen.set()

        async def on_stop(_sid, data):
            stopped.append(data)

        task = _spawn_cli_child_poller(tmp_path, on_start, on_stop)
        await asyncio.wait_for(start_seen.wait(), timeout=2)
        await asyncio.sleep(0.6)
        assert not stopped
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(stopped) == 1
        assert stopped[0]["agent_id"] == "child"
        assert stopped[0]["status"] == "interrupted"

    asyncio.run(run())


def _fake_claude_with_child_transcript(tmp_path):
    """fake claude CLI：query 启动即写 child transcript，稍后再补结束行。"""
    script = tmp_path / "fake-claude"
    script.write_text(
        """#!/usr/bin/env python3
import json, os, pathlib, time
print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': 'native'}), flush=True)
path = pathlib.Path(os.environ['TEST_TRANSCRIPT'])
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps({'type': 'assistant', 'attributionAgent': 'explore',
                            'message': {'usage': {'input_tokens': 30}, 'stop_reason': 'tool_use'}}) + '\\n')
time.sleep(0.6)
with path.open('a') as out:
    out.write(json.dumps({'type': 'assistant', 'message':
                          {'usage': {'input_tokens': 50}, 'stop_reason': 'end_turn'}}) + '\\n')
time.sleep(0.6)
print(json.dumps({'type': 'result', 'session_id': 'native', 'usage': {}}), flush=True)
""",
        encoding="utf-8",
    )
    script.chmod(0o755)
    project = str(tmp_path.resolve()).replace("/", "-")
    transcript = (
        tmp_path / ".claude" / "projects" / project / "native" / "subagents" / "agent-child.jsonl"
    )
    return script, transcript


def test_claude_cli_query_wires_child_poller(tmp_path: Path) -> None:
    async def run() -> None:
        script, transcript = _fake_claude_with_child_transcript(tmp_path)
        started = []
        stopped = []

        async def on_start(_sid, data):
            started.append(data)

        async def on_stop(_sid, data):
            stopped.append(data)

        driver = ClaudeCLIDriver(claude_bin=str(script))
        hooks = DriverHooks(
            session_id="parent", on_subagent_start=on_start, on_subagent_stop=on_stop
        )
        async for _ in driver.query(
            "hi",
            cwd=str(tmp_path),
            session_id="parent",
            env={"HOME": str(tmp_path), "TEST_TRANSCRIPT": str(transcript)},
            hooks=hooks,
        ):
            pass
        assert [item["agent_id"] for item in started] == ["child"]
        assert [item["agent_id"] for item in stopped] == ["child"]
        assert started[0]["agent_type"] == "explore"

    asyncio.run(run())


def _opencode_context_db(tmp_path):
    """opencode.db 种子：parent/child 会话、parent 消息与 child 三条消息（末条未完成）。"""
    db_path = tmp_path / "opencode" / "opencode.db"
    db_path.parent.mkdir()
    with sqlite3.connect(db_path) as db:
        db.executescript("""
            CREATE TABLE session (id TEXT, parent_id TEXT);
            CREATE TABLE message (session_id TEXT, time_created INTEGER, data TEXT);
            CREATE TABLE part (session_id TEXT, time_created INTEGER, data TEXT);
        """)
        db.execute("INSERT INTO session VALUES ('child','parent')")
        db.execute("INSERT INTO session VALUES ('parent',NULL)")
        db.execute(
            "INSERT INTO message VALUES (?,?,?)",
            (
                "parent",
                1,
                json.dumps(
                    {
                        "role": "assistant",
                        "tokens": {"input": 50, "cache": {"read": 350, "write": 0}},
                        "time": {"completed": 1},
                    }
                ),
            ),
        )
        for time_created, tokens, completed in (
            (1, {"input": 100, "cache": {"read": 50, "write": 0}}, True),
            (2, {"input": 10, "cache": {"read": 210, "write": 5}}, True),
            (3, {"input": 0, "cache": {"read": 0, "write": 0}}, False),
        ):
            db.execute(
                "INSERT INTO message VALUES (?,?,?)",
                (
                    "child",
                    time_created,
                    json.dumps(
                        {
                            "role": "assistant",
                            "tokens": tokens,
                            "time": {"completed": time_created if completed else None},
                        }
                    ),
                ),
            )
        db.execute(
            "INSERT INTO part VALUES ('child',1,?)", (json.dumps({"type": "text", "text": "done"}),)
        )
    return db_path


def test_opencode_context_checks_parent_and_settled_message(tmp_path: Path) -> None:
    _opencode_context_db(tmp_path)
    env = {"XDG_DATA_HOME": str(tmp_path)}
    assert read_opencode_context("child", "parent", env) == 225
    assert read_main_context("opencode", ".", "parent", env) == 400
    assert read_opencode_context("child", "other", env) == 0
    target = tmp_path / "snapshot.jsonl"
    assert snapshot_source("opencode", ".", "parent", "child", target, env)
    assert len(target.read_text().splitlines()) == 4
    main_target = tmp_path / "main.jsonl"
    assert snapshot_main_source("opencode", ".", "parent", main_target, env)
    assert len(main_target.read_text().splitlines()) == 1


def _snapshot_stub(_runtime, _cwd, _parent, agent_id, target, _env):
    """子代理 source 快照桩：把 agent_id 写进目标文件。"""
    target.write_text(agent_id, encoding="utf-8")
    return True


def _manifest_completing_start(tmp_path, handoff_complete, handoff_agent):
    """start 桩：记录配置；handoff_complete 时按 manifest 写齐各 doc/done 文件。"""
    started_configs = []

    async def start(config):
        started_configs.append(config)
        manifest = next((tmp_path / ".workflow-controller" / "handoffs").rglob("manifest.json"))
        data = json.loads(manifest.read_text())
        if handoff_complete:
            for item in data["entries"]:
                Path(item["handoff"]).write_text("handoff", encoding="utf-8")
            if data["main_handoff"]:
                Path(data["main_handoff"]).write_text("main handoff", encoding="utf-8")
            Path(data["done"]).write_text("done", encoding="utf-8")
        return handoff_agent if config.role == "handoff_agent" else SimpleNamespace(id="successor")

    return start, started_configs


def _subagent_start_event(agent_id, agent_type):
    """parent 会话的 SUBAGENT_START 事件。"""
    return Event.create(
        session_id="parent",
        iteration=0,
        event_type=EventType.SUBAGENT_START,
        data={"agent_id": agent_id, "agent_type": agent_type},
    )


def _threshold_handoff_fixture(tmp_path, prior_handoffs):
    """阈值交接流程底座：parent 会话、SUBAGENT_START 事件序列与带记录桩的 orch。"""
    state, session = _group_handoff_parent_state_session(tmp_path, prior_handoffs)
    agents = {"a": {"agent_type": "general-purpose"}, "b": {"agent_type": "explore"}}
    if prior_handoffs:
        (tmp_path / "group.md").write_text(
            "CUSTOM GROUP {manifest_path} {done_path}",
            encoding="utf-8",
        )
    events = [
        _subagent_start_event("a", "general"),
        _subagent_start_event("a", "general-purpose"),
    ]

    async def resume(_sid):
        events.append(_subagent_start_event("new-a", "general"))

    paused, interrupted, injected = AsyncMock(), AsyncMock(), AsyncMock()
    resumed = AsyncMock(side_effect=resume)
    orch = _group_handoff_orch(
        agents,
        events=events,
        successor_count=prior_handoffs,
        paused=paused,
        resumed=resumed,
        injected=injected,
        interrupted=interrupted,
    )
    return SimpleNamespace(
        session=session,
        agents=agents,
        orch=orch,
        state=state,
        paused=paused,
        interrupted=interrupted,
        injected=injected,
        resumed=resumed,
    )


async def _run_threshold_handoff(group, plugin, session, agents, main_over):
    """在 snapshot/read_main 读数桩下执行一次阈值触发的 group handoff。"""
    services = plugin.services
    with (
        patch.object(services, "snapshot_subagent_source", _snapshot_stub),
        patch.object(services, "read_main_context", return_value=750 if main_over else 100),
    ):
        await getattr(group, "_handoff")(session, "a", {"a": agents["a"]}, "opencode", {})


def _assert_handoff_manifest(tmp_path, expected_main_handoff, prior_handoffs):
    """manifest/prompt 文件形态断言组；返回 entries 列表。"""
    manifest = next((tmp_path / ".workflow-controller" / "handoffs").rglob("manifest.json"))
    data = json.loads(manifest.read_text())
    entries = data["entries"]
    assert {x["old_agent_id"] for x in entries} == {"a", "b"}
    assert {item["old_agent_id"]: item["agent_type"] for item in entries} == {
        "a": "general",
        "b": "explore",
    }
    for item in entries:
        child_prompt = item["dispatch_prompt"]
        assert f"Read 工具完整读取你自己的交接文档：{item['handoff']}" in child_prompt
        assert "从上次中断的步骤继续执行" in child_prompt
        assert all(other["handoff"] not in child_prompt for other in entries if other is not item)
    assert bool(data["main_handoff"]) is expected_main_handoff
    number = prior_handoffs + 1
    assert data["handoff_number"] == number
    assert data["birth_handoff_doc"] == ("previous-handoff-doc" if prior_handoffs else "")
    assert Path(data["done"]).name == f"parent-handoff-{number}-done"
    assert all(Path(item["handoff"]).name.endswith(f"-handoff-{number}-doc") for item in entries)
    if expected_main_handoff:
        assert Path(data["main_handoff"]).name == f"parent-handoff-{number}-doc"
    prompt = (manifest.parent / "handoff-prompt.md").read_text(encoding="utf-8")
    if prior_handoffs:
        assert prompt == f"CUSTOM GROUP {manifest} {data['done']}"
    else:
        assert "每个 entry 必须在 handoff 路径写一份 Markdown 文档" in prompt
        if expected_main_handoff:
            assert "摘要须逐项列出每个 entry 的 handoff 路径与 dispatch_prompt" in prompt
    return entries


def _assert_handoff_outcomes(fix, expected_main_handoff, agent_timeout):
    """handoff 后各桩的调用次数与等待超时形态断言组。"""
    assert fix.paused.await_count == 1
    assert fix.interrupted.await_count == 1
    assert fix.injected.await_count == (0 if expected_main_handoff else 1)
    assert fix.resumed.await_count == (0 if expected_main_handoff else 1)
    assert fix.orch.stop_session.await_count == (1 if expected_main_handoff else 0)
    assert fix.orch.wait_session_done.await_count == 2
    assert fix.orch.wait_session_done.await_args.kwargs["wait_timeout"] == (
        None if agent_timeout == 0 else agent_timeout
    )


@pytest.mark.parametrize("main_over", [False, True])
@pytest.mark.parametrize("handoff_complete", [False, True])
@pytest.mark.parametrize("prior_handoffs", [0, 1])
@pytest.mark.parametrize("agent_timeout", [0, 600])
@pytest.mark.parametrize("main_threshold", [70, 100])
def test_one_child_threshold_hands_off_all_in_flight(
    monkeypatch,
    tmp_path: Path,
    main_over: bool,
    handoff_complete: bool,
    prior_handoffs: int,
    agent_timeout: int,
    main_threshold: int,
) -> None:
    async def run() -> None:
        expected_main_handoff = main_over and main_threshold < 100
        fix = _threshold_handoff_fixture(tmp_path, prior_handoffs)
        handoff_agent = SimpleNamespace(id="handoff-session", result="fail")
        start, started_configs = _manifest_completing_start(
            tmp_path, handoff_complete, handoff_agent
        )
        generator = _wire_successor_start(fix.orch, start)
        plugin = _group_handoff_plugin(
            fix.orch, generator, fix.state, agent_timeout, main_threshold
        )
        group = GroupHandoff(plugin)
        monkeypatch.setattr(group, "_watch_dispatch", Mock())
        await _run_threshold_handoff(group, plugin, fix.session, fix.agents, main_over)

        entries = _assert_handoff_manifest(tmp_path, expected_main_handoff, prior_handoffs)
        recovery_prompt = (
            started_configs[1].initial_prompt
            if expected_main_handoff
            else fix.injected.await_args.args[1]
        )
        assert "dispatch_prompt 原文放进对应 Agent/Task 调用的 prompt 参数" in recovery_prompt
        assert "在同一条回复中并行调用 Agent/Task" in recovery_prompt
        _assert_handoff_outcomes(fix, expected_main_handoff, agent_timeout)
        assert handoff_agent.result == ("success" if handoff_complete else "fail")
        assert any(
            call.args[0] is handoff_agent
            for call in getattr(fix.orch.pool, "_persist_session").await_args_list
        ) is (handoff_complete or not expected_main_handoff)
        if not expected_main_handoff:
            assert handoff_agent.plugin_metadata["breather"]["continuation_session_id"] == "parent"
        dispatch_args = getattr(group, "_watch_dispatch").call_args.args
        assert dispatch_args[1] == (set() if expected_main_handoff else {"a"})
        assert dispatch_args[2] == [item["handoff"] for item in entries]
        if not expected_main_handoff and not handoff_complete:
            assert "从 source 快照" in fix.injected.await_args.args[1]

    asyncio.run(run())


def test_group_handoff_prompt_uses_project_template(tmp_path: Path) -> None:
    (tmp_path / "group.md").write_text(
        "清单 {manifest_path}，完成 {done_path}，保留 {custom_slot}",
        encoding="utf-8",
    )
    plugin = SimpleNamespace(cfg=lambda _key, default=None: "group.md")
    group = GroupHandoff(plugin)
    session = SimpleNamespace(
        config=SimpleNamespace(
            cwd=str(tmp_path),
            group_handoff_prompt_path="group.md",
        )
    )

    template = getattr(group, "_prompt_template")(session)

    assert "清单 {manifest_path}" in template
    assert "完成 {done_path}" in template
    assert "{custom_slot}" in template
    session.config.group_handoff_prompt_path = None
    assert getattr(group, "_prompt_template")(session) == template


def test_group_handoff_uses_shared_template_before_legacy_path(tmp_path: Path) -> None:
    (tmp_path / "shared.md").write_text(
        "<!-- workflow:common -->\n主代理和子代理都要核对项目文件。\n"
        "<!-- workflow:group -->\n子代理读取 {manifest_path}。\n",
        encoding="utf-8",
    )
    (tmp_path / "legacy.md").write_text("旧模板", encoding="utf-8")
    plugin = SimpleNamespace(
        cfg=lambda key, default=None: {
            "handoff_prompt_path": "shared.md",
            "group_handoff_prompt_path": "legacy.md",
        }.get(key, default)
    )
    session = SimpleNamespace(
        config=SimpleNamespace(
            cwd=str(tmp_path), handoff_prompt_path=None, group_handoff_prompt_path="legacy.md"
        )
    )

    template = getattr(GroupHandoff(plugin), "_prompt_template")(session)

    assert template.startswith("主代理和子代理都要核对项目文件。")
    assert "清单" in template and "{manifest_path}" in template
    assert "旧模板" not in template


def test_unmarked_handoff_prompt_path_falls_back_to_legacy_group_template(tmp_path: Path) -> None:
    (tmp_path / "old-main.md").write_text(
        "盘点并写出 {handoff_filename}，最后写 {handoff_filename}.done。", encoding="utf-8"
    )
    (tmp_path / "legacy-group.md").write_text("旧整组模板 {manifest_path}", encoding="utf-8")
    plugin = SimpleNamespace(
        cfg=lambda key, default=None: {
            "handoff_prompt_path": "old-main.md",
            "group_handoff_prompt_path": "legacy-group.md",
        }.get(key, default)
    )
    session = SimpleNamespace(
        config=SimpleNamespace(
            cwd=str(tmp_path), handoff_prompt_path=None, group_handoff_prompt_path=None
        )
    )

    template = getattr(GroupHandoff(plugin), "_prompt_template")(session)

    assert template == "旧整组模板 {manifest_path}"
    assert "{handoff_filename}" not in template


def test_one_template_selects_main_or_group_instructions(tmp_path: Path) -> None:
    (tmp_path / "shared.md").write_text(
        "<!-- workflow:common -->\n核对项目文件。\n"
        "<!-- workflow:main -->\n主代理写 {handoff_filename}。\n"
        "<!-- workflow:group -->\n子代理读取 {manifest_path}。\n",
        encoding="utf-8",
    )
    main = getattr(
        HandoffGenerator(
            skill="handoff", handoff_filename="handoff.md", handoff_prompt_path="shared.md"
        ),
        "_load_template",
    )(str(tmp_path))
    plugin = SimpleNamespace(
        cfg=lambda key, default=None: "shared.md" if key == "handoff_prompt_path" else default
    )
    session = SimpleNamespace(
        config=SimpleNamespace(
            cwd=str(tmp_path), handoff_prompt_path="shared.md", group_handoff_prompt_path=None
        )
    )
    group = getattr(GroupHandoff(plugin), "_prompt_template")(session)

    assert "核对项目文件" in main and "核对项目文件" in group
    assert "主代理写 {handoff_filename}" in main
    assert "主代理写" not in group
    assert "子代理读取 {manifest_path}" in group
    assert "子代理读取" not in main
    assert "完成信号已写入 {done_path}" in group

    joint = getattr(GroupHandoff(plugin), "_prompt_template")(session, main_over=True)
    assert "主代理写 清单中的 main_handoff" in joint
    assert "子代理读取 {manifest_path}" in joint
    assert joint.count("完成信号已写入 {done_path}") == 1


def test_group_handoff_replaces_legacy_placeholders_in_common_and_group_sections(
    tmp_path: Path,
) -> None:
    (tmp_path / "shared.md").write_text(
        "<!-- workflow:common -->\n"
        "文档写到 {handoff_filename}，最后写 {handoff_filename}.done，worker={worker_session_id}。\n"
        "<!-- workflow:group -->\n"
        "子代理读取 {manifest_path}。\n",
        encoding="utf-8",
    )
    plugin = SimpleNamespace(
        cfg=lambda key, default=None: "shared.md" if key == "handoff_prompt_path" else default
    )
    session = SimpleNamespace(
        id="ws-main-1",
        config=SimpleNamespace(
            cwd=str(tmp_path), handoff_prompt_path="shared.md", group_handoff_prompt_path=None
        ),
    )

    template = getattr(GroupHandoff(plugin), "_prompt_template")(session)

    assert "{handoff_filename}" not in template
    assert "{worker_session_id}" not in template
    assert "清单中对应 entry 的 handoff 路径" in template
    assert "最后写 {done_path}" in template
    assert "worker=ws-main-1" in template
    assert "子代理读取 {manifest_path}" in template


def test_group_handoff_replaces_birth_handoff_doc_section_placeholder(
    tmp_path: Path,
) -> None:
    (tmp_path / "shared.md").write_text(
        "<!-- workflow:common -->\n"
        "继承历史：{birth_handoff_doc_section}\n"
        "<!-- workflow:main -->\n"
        "主代理继承历史：{birth_handoff_doc_section}\n"
        "<!-- workflow:group -->\n"
        "子代理读取 {manifest_path}。\n",
        encoding="utf-8",
    )
    plugin = SimpleNamespace(
        cfg=lambda key, default=None: "shared.md" if key == "handoff_prompt_path" else default
    )
    session = SimpleNamespace(
        id="ws-main-1",
        birth_handoff_doc="prev-handoff-doc",
        config=SimpleNamespace(
            cwd=str(tmp_path), handoff_prompt_path="shared.md", group_handoff_prompt_path=None
        ),
    )

    template = getattr(GroupHandoff(plugin), "_prompt_template")(session, main_over=True)

    assert "{birth_handoff_doc_section}" not in template
    assert "`prev-handoff-doc`" in template
    assert "第 6、7、8 节" in template
    assert "子代理读取 {manifest_path}" in template


def test_shared_template_without_main_section_keeps_main_completion_contract() -> None:
    selected = handoff_template_for_mode(
        "<!-- workflow:common -->\n核对项目文件。\n<!-- workflow:group -->\n逐个盘点子代理。",
        "main",
    )
    assert "核对项目文件" in selected
    assert "逐个盘点子代理" not in selected
    assert "{handoff_filename}.done" in selected


def test_handoff_sections_skip_editor_comment_block() -> None:
    template = (
        "<!--\n编辑说明：请保留下面的分段标记。\n-->\n\n"
        "<!-- workflow:main -->\n主代理写 {handoff_filename}。\n"
        "<!-- workflow:group -->\n子代理读取 {manifest_path}。\n"
    )
    main = handoff_template_for_mode(template, "main")
    group = handoff_template_for_mode(template, "group")

    assert "编辑说明" not in main
    assert "编辑说明" not in group
    assert "请保留下面的分段标记" not in main
    assert "主代理写 {handoff_filename}" in main
    assert "子代理读取 {manifest_path}" in group


def test_group_handoff_default_reply_reports_files_without_optional_question(
    tmp_path: Path,
) -> None:
    group = GroupHandoff(SimpleNamespace(cfg=lambda _key, default=None: default))
    session = SimpleNamespace(
        config=SimpleNamespace(
            cwd=str(tmp_path),
            group_handoff_prompt_path=None,
        )
    )

    template = getattr(group, "_prompt_template")(session)

    assert "不在最终回复中征询用户是否继续" in template
    assert "完成信号已写入 {done_path}" in template


@pytest.mark.parametrize("omit_child_path", [False, True])
def test_dispatch_verifier_checks_each_child_prompt(omit_child_path: bool) -> None:
    async def run() -> None:
        paths = ["/handoffs/child-a", "/handoffs/child-b"]
        events = [
            Event.create(
                session_id="next",
                iteration=1,
                event_type=EventType.SUBAGENT_START,
                data={"agent_id": child},
            )
            for child in ("new-a", "new-b")
        ]
        for path in paths[: 1 if omit_child_path else 2]:
            events.append(
                Event.create(
                    session_id="next",
                    iteration=1,
                    event_type=EventType.TOOL_USE,
                    data={"name": "Agent", "input": {"prompt": f"先读 {path}"}},
                )
            )
        session, plugin, bus = _dispatch_verify_plugin(events)

        polls = {"count": 0}

        async def end_after_first_check(_delay: float) -> None:
            polls["count"] += 1
            session.state = SessionState.STOPPED
            session.stop_reason = "evaluator_complete"

        with patch(
            "workflow_controller.plugins.breather.group_handoff.asyncio.sleep",
            side_effect=end_after_first_check,
        ):
            await getattr(GroupHandoff(plugin), "_verify_dispatch")("next", set(), paths)

        if omit_child_path:
            assert polls["count"] == 1
            assert bus.emit.await_count == 1
            error = bus.emit.await_args.args[0]
            assert error.data["missing_handoff_paths"] == [paths[1]]
        else:
            assert polls["count"] == 0
            bus.emit.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize("stop_mode", ["pause", "stop"])
def test_dispatch_verifier_exits_when_continuation_is_manually_stopped(
    stop_mode: str,
) -> None:
    async def run() -> None:
        session, plugin, bus = _dispatch_verify_plugin([])

        async def stop_after_first_check(_delay: float) -> None:
            session.stop_reason = "user_pause"
            if stop_mode == "pause":
                session.pause_requested = True
                session.state = SessionState.IDLE
            else:
                session.state = SessionState.STOPPED

        with patch(
            "workflow_controller.plugins.breather.group_handoff.asyncio.sleep",
            side_effect=stop_after_first_check,
        ):
            await getattr(GroupHandoff(plugin), "_verify_dispatch")(
                "parent", set(), ["/handoffs/a"]
            )

        bus.emit.assert_not_awaited()

    asyncio.run(run())


def test_dispatch_verifier_reports_missing_agents_after_natural_completion() -> None:
    async def run() -> None:
        _session, plugin, bus = _dispatch_verify_plugin(
            [], state=SessionState.STOPPED, stop_reason="evaluator_complete"
        )

        with patch(
            "workflow_controller.plugins.breather.group_handoff.asyncio.sleep",
            new_callable=AsyncMock,
        ):
            await getattr(GroupHandoff(plugin), "_verify_dispatch")(
                "parent", set(), ["/handoffs/a"]
            )

        assert bus.emit.await_count == 1
        assert bus.emit.await_args.args[0].data["error"] == "子代理交接后重新派发数量不足"

    asyncio.run(run())


def test_dispatch_verifier_waits_for_running_session_before_reporting() -> None:
    async def run() -> None:
        session, plugin, bus = _dispatch_verify_plugin([])
        polls = {"count": 0}

        async def progress_then_end(_delay: float) -> None:
            polls["count"] += 1
            if polls["count"] >= 3:
                session.state = SessionState.STOPPED
                session.stop_reason = "evaluator_complete"

        with patch(
            "workflow_controller.plugins.breather.group_handoff.asyncio.sleep",
            side_effect=progress_then_end,
        ):
            await getattr(GroupHandoff(plugin), "_verify_dispatch")(
                "parent", set(), ["/handoffs/a"]
            )

        assert polls["count"] == 3
        assert bus.emit.await_count == 1
        assert bus.emit.await_args.args[0].data["error"] == "子代理交接后重新派发数量不足"

    asyncio.run(run())


def test_group_handoff_aborts_when_parent_pause_times_out(tmp_path: Path) -> None:
    async def run() -> None:
        state, session = _group_handoff_parent_state_session(tmp_path)
        interrupted = AsyncMock()
        paused = AsyncMock(return_value=False)
        router = SimpleNamespace(
            get_events_sync=lambda _id: [],
            get_successors_sync=lambda _id: [],
            add_handoff_relation=AsyncMock(),
        )
        orch = SimpleNamespace(
            pause_session=paused,
            wait_session_done=AsyncMock(return_value=False),
            resume_session=AsyncMock(),
            stop_session=AsyncMock(),
            event_router=router,
            pool=SimpleNamespace(inject_prompt=AsyncMock(), _persist_session=AsyncMock()),
            _subagents=SimpleNamespace(interrupt_all=interrupted, in_flight=lambda _id: {}),
        )
        plugin = SimpleNamespace(
            services=services_for(orch),
            ctx=SimpleNamespace(),
            cfg=lambda key, default=None: default,
            threshold_pct=lambda _session: 70,
            context_window=lambda _session: 1000,
            check_context_from_events=lambda _s: SimpleNamespace(percentage=10, max_tokens=1000),
            state_for=lambda _id: state,
            reset_handoff_state=lambda _id: None,
        )
        group = GroupHandoff(plugin)

        with patch.object(plugin.services, "read_main_context", return_value=100):
            await getattr(group, "_handoff")(session, "a", {}, "opencode", {})

        interrupted.assert_not_awaited()
        orch.resume_session.assert_not_awaited()
        orch.stop_session.assert_not_awaited()

    asyncio.run(run())


def test_group_handoff_aborts_when_handoff_agent_pause_times_out(tmp_path: Path) -> None:
    async def run() -> None:
        state, session = _group_handoff_parent_state_session(tmp_path)
        interrupted = AsyncMock()
        router = SimpleNamespace(
            get_events_sync=lambda _id: [],
            get_successors_sync=lambda _id: [],
            add_handoff_relation=AsyncMock(),
        )
        handoff_agent = SimpleNamespace(id="handoff-session", result="fail")
        started_configs = []

        async def start(config):
            started_configs.append(config)
            return (
                handoff_agent if config.role == "handoff_agent" else SimpleNamespace(id="successor")
            )

        async def pause(sid):
            return sid == "parent"

        orch = SimpleNamespace(
            pause_session=AsyncMock(side_effect=pause),
            resume_session=AsyncMock(),
            stop_session=AsyncMock(),
            event_router=router,
            wait_session_done=AsyncMock(side_effect=[True, False, False]),
            pool=SimpleNamespace(inject_prompt=AsyncMock(), _persist_session=AsyncMock()),
            _subagents=SimpleNamespace(interrupt_all=interrupted, in_flight=lambda _id: {}),
        )
        orch.start_session_with_config = start
        plugin = SimpleNamespace(
            services=services_for(orch),
            ctx=SimpleNamespace(),
            cfg=lambda key, default=None: default,
            threshold_pct=lambda _session: 70,
            context_window=lambda _session: 1000,
            check_context_from_events=lambda _s: SimpleNamespace(percentage=10, max_tokens=1000),
            effective_generator=lambda _s: SimpleNamespace(
                build_new_session_config=lambda _session, _path, _name: SimpleNamespace(
                    role="worker", initial_prompt="", env_overrides={}
                )
            ),
            state_for=lambda _id: state,
            reset_handoff_state=lambda _id: None,
        )
        group = GroupHandoff(plugin)

        with patch.object(plugin.services, "read_main_context", return_value=100):
            await getattr(group, "_handoff")(session, "a", {}, "opencode", {})

        assert orch.pause_session.await_count == 2
        assert {c.role for c in started_configs} == {"handoff_agent"}
        assert handoff_agent.result == "fail"

    asyncio.run(run())


def test_group_handoff_early_return_keeps_plugin_handoff_state() -> None:
    async def run() -> None:
        state = SimpleNamespace(pending=True, finalizing=False, session_id="", started_at=0)
        reset = AsyncMock()
        session = SimpleNamespace(id="parent", pause_requested=False)
        plugin = SimpleNamespace(
            services=SimpleNamespace(),
            state_for=lambda _id: state,
            reset_handoff_state=reset,
        )
        group = GroupHandoff(plugin)
        await getattr(group, "_handoff")(session, "a", {}, "opencode", {})
        reset.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize(("child_threshold", "trigger_tokens"), [(None, 750), (60, 650)])
def test_monitor_emits_context_and_triggers_once(
    monkeypatch,
    tmp_path: Path,
    child_threshold: float | None,
    trigger_tokens: int,
) -> None:
    async def run() -> None:
        session = _monitor_session(tmp_path, child_threshold)
        agents = {"a": {"agent_type": "general"}, "b": {"agent_type": "explore"}}
        plugin, emitted = _monitor_plugin(
            session, agents, child_threshold if child_threshold is not None else 70
        )
        group = GroupHandoff(plugin)
        monkeypatch.setattr(group, "_handoff", AsyncMock())

        def sample(_runtime, _cwd, _parent, agent_id, _env):
            return trigger_tokens if agent_id == "a" else 100

        with (
            patch.object(plugin.services, "read_subagent_context", sample),
            patch.object(plugin.services, "read_main_context", return_value=250),
        ):
            await getattr(group, "_monitor")("parent")

        assert len(emitted) == 2
        assert emitted[0].event_type == EventType.MAIN_CONTEXT
        assert emitted[0].data["percentage"] == 25.0
        assert emitted[1].data["percentage"] == trigger_tokens / 10
        assert getattr(group, "_handoff").await_count == 1
        assert set(getattr(group, "_handoff").await_args.args[2]) == {"a", "b"}

    asyncio.run(run())


def test_monitor_at_100_pct_reports_without_handoff(tmp_path: Path) -> None:
    async def run() -> None:
        session = SimpleNamespace(
            id="parent",
            pause_requested=False,
            config=SimpleNamespace(cwd=str(tmp_path), runtime="opencode", env_overrides={}),
            native_session_id="native",
            current_iteration=1,
        )
        emitted = []

        async def emit(event):
            emitted.append(event)

        services = services_for(
            SimpleNamespace(
                pool=SimpleNamespace(get=lambda _id: session),
                config=SimpleNamespace(runtime="opencode"),
                _subagents=SimpleNamespace(in_flight=lambda _id: {"a": {"agent_type": "checker"}}),
            )
        )
        plugin = SimpleNamespace(
            services=services,
            ctx=SimpleNamespace(bus=SimpleNamespace(emit=emit)),
            installed=True,
            cfg=lambda _key, _default=None: 1000,
            subagent_threshold_pct=lambda _session: 100,
            context_window=lambda _session: 1000,
        )
        group = GroupHandoff(plugin)
        setattr(group, "_handoff", AsyncMock())
        with (
            patch.object(services, "read_main_context", return_value=None),
            patch.object(services, "read_subagent_context", return_value=1200),
            patch.object(asyncio, "sleep", side_effect=asyncio.CancelledError),
            pytest.raises(asyncio.CancelledError),
        ):
            await getattr(group, "_monitor")("parent")
        assert emitted[0].event_type == EventType.SUBAGENT_CONTEXT
        assert emitted[0].data["percentage"] == 120.0
        getattr(group, "_handoff").assert_not_awaited()

    asyncio.run(run())


def test_interrupted_children_ignore_late_stop() -> None:
    async def run() -> None:
        bus = SimpleNamespace(emit=AsyncMock())
        tracker = SubagentTracker(bus, SimpleNamespace(get=lambda _sid: None), lambda _sid: None)
        await tracker.on_start("parent", {"agent_id": "a", "agent_type": "general"})
        await tracker.interrupt_all("parent")
        assert tracker.in_flight("parent") == {}
        count = bus.emit.await_count
        await tracker.on_stop("parent", {"agent_id": "a", "agent_type": "general"})
        assert bus.emit.await_count == count

    asyncio.run(run())


def test_cli_hook_type_is_not_overwritten_by_transcript_fallback() -> None:
    async def run() -> None:
        bus = SimpleNamespace(emit=AsyncMock())
        tracker = SubagentTracker(bus, SimpleNamespace(get=lambda _sid: None), lambda _sid: None)
        await tracker.on_start("parent", {"agent_id": "a", "agent_type": "reader"})
        await tracker.on_start("parent", {"agent_id": "a", "agent_type": "general-purpose"})
        assert bus.emit.await_count == 1
        assert tracker.in_flight("parent")["a"]["agent_type"] == "reader"
        await tracker.on_stop("parent", {"agent_id": "a", "agent_type": "general-purpose"})
        assert bus.emit.await_args.args[0].data["agent_type"] == "reader"
        assert bus.emit.await_count == 2
        await tracker.on_stop("parent", {"agent_id": "a", "agent_type": "reader"})
        assert bus.emit.await_count == 2

    asyncio.run(run())


def test_group_handoff_stops_paused_parent_when_successor_persist_fails(
    monkeypatch, tmp_path: Path
) -> None:
    async def run() -> None:
        state, session = _group_handoff_parent_state_session(tmp_path)
        persist = AsyncMock(side_effect=RuntimeError("db write failed"))
        orch = _group_handoff_orch({"a": {"agent_type": "general"}}, persist=persist)

        async def start(config):
            if config.role == "handoff_agent":
                return SimpleNamespace(id="handoff-session", result="fail")
            return SimpleNamespace(id="successor")

        generator = _wire_successor_start(orch, start)
        plugin = _group_handoff_plugin(orch, generator, state)
        group = GroupHandoff(plugin)
        monkeypatch.setattr(group, "_watch_dispatch", Mock())

        with (
            patch.object(plugin.services, "read_main_context", return_value=750),
            patch.object(
                plugin.services,
                "snapshot_subagent_source",
                side_effect=lambda _r, _c, _p, _a, _t, _e: True,
            ),
            patch.object(
                plugin.services,
                "snapshot_main_source",
                side_effect=lambda _r, _c, _p, _t, _e: True,
            ),
        ):
            await getattr(group, "_handoff")(
                session, "a", {"a": {"agent_type": "general"}}, "opencode", {}
            )

        orch.stop_session.assert_awaited_with("parent")
        orch.resume_session.assert_not_awaited()

    asyncio.run(run())


def test_monitor_retries_after_transient_sample_error(monkeypatch, tmp_path: Path) -> None:
    async def run() -> None:
        session = _monitor_session(tmp_path)
        plugin, _emitted = _monitor_plugin(session, {"a": {"agent_type": "general"}}, 70)
        group = GroupHandoff(plugin)
        monkeypatch.setattr(group, "_handoff", AsyncMock())
        calls = {"n": 0}

        def sample(_runtime, _cwd, _parent, agent_id, _env):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("transient read error")
            return 750

        with (
            patch.object(plugin.services, "read_subagent_context", sample),
            patch.object(plugin.services, "read_main_context", return_value=250),
        ):
            await getattr(group, "_monitor")("parent")

        assert calls["n"] >= 2
        assert getattr(group, "_handoff").await_count == 1

    asyncio.run(run())
