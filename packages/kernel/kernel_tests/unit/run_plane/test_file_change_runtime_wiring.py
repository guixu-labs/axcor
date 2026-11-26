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

"""轨迹捕获的运行时接线：opencode 注入、SO 捕获点打戳与 run 键进程捕获。"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from workflow_controller.core.file_changes import FileChangeRegistry
from workflow_controller.core.orchestrator import Orchestrator
from workflow_controller.core.run_coordinator import RunHost
from workflow_controller.driver.hooks import DriverHooks
from workflow_controller.driver.opencode import OpenCodeDriver
from workflow_controller.event.event_bus import EventBus
from workflow_controller.event.type_registry import EventTypeRegistry
from workflow_controller.plugin.file_changes import FileChangeContext


async def _none():
    return None


async def test_opencode_injects_provenance_plugin_per_process(tmp_path, monkeypatch):
    from workflow_controller.driver import opencode as module

    captured = {}

    def build(*args, **kwargs):
        captured.update(kwargs)
        return json.dumps({"compaction": {"auto": False}, "plugin": kwargs["plugins"]})

    monkeypatch.setattr(module, "build_config_content", build)
    monkeypatch.setattr(OpenCodeDriver, "_maybe_start_gate", lambda *args: _none())
    hooks = DriverHooks(
        session_id="sid",
        on_post_tool_use=lambda _sid, _data: _none(),
        file_change_hook_url="http://127.0.0.1:19999",
        file_changes_enabled=lambda: True,
    )
    driver = OpenCodeDriver(opencode_bin="echo")
    async for _ in driver.query(
        "p",
        session_id="sid",
        cwd=str(tmp_path),
        hooks=hooks,
        env={"OPENCODE_CONFIG_CONTENT": json.dumps({"plugin": ["existing"]})},
    ):
        pass
    assert captured["plugins"][0].endswith("opencode_file_changes.ts")
    assert json.loads(captured["base_content"])["plugin"] == ["existing"]


async def test_opencode_without_provenance_does_not_inject_plugin(tmp_path, monkeypatch):
    from workflow_controller.driver import opencode as module

    called = []
    original = module.build_config_content

    def build(*args, **kwargs):
        called.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "build_config_content", build)
    monkeypatch.setattr(OpenCodeDriver, "_maybe_start_gate", lambda *args: _none())
    driver = OpenCodeDriver(opencode_bin="echo")
    hooks = DriverHooks(
        session_id="sid",
        on_post_tool_use=lambda _sid, _data: _none(),
    )
    async for _ in driver.query("p", session_id="sid", cwd=str(tmp_path), hooks=hooks):
        pass
    assert called == []


def test_opencode_adapter_uses_capture_routes_and_canonical_tool_names():
    from workflow_controller.driver import opencode as module

    adapter = Path(module.__file__).with_name("opencode_file_changes.ts").read_text()
    assert 'post("/hooks/file-changes/pre"' in adapter
    assert 'post("/hooks/file-changes/post"' in adapter
    assert 'new Set(["edit", "write", "apply_patch", "bash"])' in adapter
    assert "normalizeToolName" not in adapter


async def test_cli_nested_tool_response_error_marks_capture_failed(tmp_path):
    context = FileChangeContext(
        capture_id="capture-1",
        actor_kind="agent",
        workflow_controller_session_id="session-1",
        native_session_id="native-1",
        parent_native_session_id=None,
        runtime="claude_cli",
        model=None,
        agent_name=None,
        tool_name="Bash",
        tool_use_id="tool-1",
        cwd=tmp_path,
        tool_input={"command": "false"},
        started_at=datetime.now(tz=UTC),
    )
    bus = SimpleNamespace(emit=AsyncMock())
    orchestrator = SimpleNamespace(
        _file_change_contexts={("session-1", "tool-1"): context},
        bus=bus,
    )

    await getattr(Orchestrator, "_handle_driver_post_tool_use")(
        orchestrator,
        "session-1",
        {
            "tool_use_id": "tool-1",
            "tool_name": "Bash",
            "tool_response": {"content": "exit code 1", "is_error": True},
        },
    )

    event = bus.emit.await_args.args[0]
    assert event.data["outcome"]["succeeded"] is False
    assert event.data["outcome"]["is_error"] is True


async def test_pre_tool_capture_stamps_run_dimension(tmp_path):
    """SO 捕获点：捕获上下文按归属缓存带上 run 维度。"""
    from workflow_controller.core.session_run_affiliation import SessionRunAffiliation

    session = SimpleNamespace(
        native_session_id="native-1",
        current_prompt="working",
        config=SimpleNamespace(
            runtime=None,
            model=None,
            agent_name=None,
            cwd=None,
            initial_prompt="prompt",
            system_prompt="",
        ),
    )
    bus = SimpleNamespace(emit=AsyncMock())
    orchestrator = SimpleNamespace(
        file_changes=SimpleNamespace(has_subscribers=True),
        pool=SimpleNamespace(get=lambda sid: session),
        config=SimpleNamespace(runtime="claude_sdk", project_dir=str(tmp_path)),
        _affiliation=SimpleNamespace(
            resolve=lambda sid: SessionRunAffiliation(
                run_id="run-1",
                role="executor",
                context="node=A",
            )
        ),
        _file_change_contexts={},
        bus=bus,
        _decide_pre_tool_use=AsyncMock(return_value=None),
    )

    await getattr(Orchestrator, "_handle_driver_pre_tool_use")(
        orchestrator,
        "session-1",
        {"tool_name": "Write", "tool_input": {"file_path": "a.py"}},
    )

    context_payload = bus.emit.await_args.args[0].data["context"]
    assert context_payload["run_id"] == "run-1"
    assert context_payload["run_context"] == "node=A"
    assert context_payload["workflow_controller_session_id"] == "session-1"


# ── run 键进程的轨迹捕获（宿主进程面内联）──────────────────────────


def _capture_env() -> tuple[RunHost, list, list]:
    """带捕获订阅的进程面装配：返回 (host, 捕获上下文, 结局)。"""
    registry = FileChangeRegistry()
    store = MagicMock()
    store.store = AsyncMock()
    store.store_run_event = AsyncMock()
    bus = EventBus(store_router=store, type_registry=EventTypeRegistry())
    registry.bind(bus)
    contexts: list = []
    outcomes: list = []

    async def before(context):
        contexts.append(context)

    async def after(_context, outcome):
        outcomes.append(outcome)

    registry.subscribe(before, after)
    orch = SimpleNamespace(file_changes=registry)
    host = RunHost(
        orch,
        None,
        None,
        lambda run_id, notification: None,
        lambda run_id: Path("."),
        bus,
    )
    return host, contexts, outcomes


async def _await_exit(notifications: list, proc_id: str, timeout_s: float = 8.0) -> None:
    """等指定进程的退出通知到达。"""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        exits = [n for n in notifications if getattr(n, "proc_id", None) == proc_id]
        if exits:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"进程 {proc_id} 未在时限内退出")


async def test_run_process_capture_carries_run_identity(monkeypatch, tmp_path):
    """run 键进程捕获：automation 身份 + run/context 维度，成功结局收口。"""
    host, contexts, outcomes = _capture_env()
    notifications: list = []

    def _notify(run_id, note):
        notifications.append(note)

    monkeypatch.setattr(host, "_notifier", _notify)  # type: ignore[assignment]
    proc_id = host.start_process(
        "run-9",
        "true",
        str(tmp_path),
        context="node=node-7",
    )
    await _await_exit(notifications, proc_id)

    context = contexts[0]
    assert context.workflow_controller_session_id is None
    assert context.actor_kind == "automation"
    assert context.run_id == "run-9"
    assert context.run_context == "node=node-7"
    assert outcomes[0].succeeded is True
    assert outcomes[0].incomplete is False


async def test_run_process_spawn_failure_closes_capture_incomplete(tmp_path, monkeypatch):
    """进程起动失败：捕获开环收口为 incomplete，退出通知带 spawn_failed。"""
    host, _contexts, outcomes = _capture_env()
    notifications: list = []

    def _notify(run_id, note):
        notifications.append(note)

    monkeypatch.setattr(host, "_notifier", _notify)  # type: ignore[assignment]

    async def fail_spawn(*args, **kwargs):
        raise OSError("spawn failed")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fail_spawn)
    proc_id = host.start_process("run-9", "true", str(tmp_path), context="node=a")
    await _await_exit(notifications, proc_id)

    assert len(outcomes) == 1
    assert outcomes[0].incomplete is True
    exit_note = notifications[0]
    assert exit_note.returncode == -1
    assert exit_note.exit_mark == "spawn_failed"


async def test_run_process_plane_kill_closes_capture_incomplete(monkeypatch, tmp_path):
    """本面组杀：捕获按 incomplete 收口，退出通知带 plane_killed。"""
    host, _contexts, outcomes = _capture_env()
    notifications: list = []

    def _notify(run_id, note):
        notifications.append(note)

    monkeypatch.setattr(host, "_notifier", _notify)  # type: ignore[assignment]
    proc_id = host.start_process(
        "run-9",
        "sleep 30",
        str(tmp_path),
        context="node=a",
    )
    await _await_spawn(host, "run-9")
    kill_task = asyncio.create_task(host.kill_processes("run-9"))
    await _await_exit(notifications, proc_id)
    await kill_task

    assert len(outcomes) == 1
    assert outcomes[0].incomplete is True
    assert outcomes[0].succeeded is False
    exit_note = next(n for n in notifications if getattr(n, "proc_id", None) == proc_id)
    assert exit_note.exit_mark == "plane_killed"


async def _await_spawn(host: RunHost, run_id: str, timeout_s: float = 5.0) -> None:
    """等 run 名下进程注册表出现条目。"""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if getattr(host, "_processes").get(run_id):
            return
        await asyncio.sleep(0.01)
    raise AssertionError("进程未在时限内起动")
