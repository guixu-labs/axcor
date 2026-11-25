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
import contextlib
import json
import logging
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from workflow_controller.driver import cli_base, codex_catalog, codex_cli
from workflow_controller.driver.codex_cli import (
    CodexCLIDriver,
    _build_args,
    _codex_config_overrides,
    _codex_line_to_events,
    _codex_proc_env,
    _codex_subagent_fact,
    _drain_stderr,
    _stderr_tail_text,
)
from workflow_controller.driver.hooks import (
    CodexHookConfigWriter,
    DriverHooks,
    SubagentProgressBridge,
)
from workflow_controller.session.events import EventType

_QUERY_ENV = {
    "ANTHROPIC_API_KEY": "secret",
    "ANTHROPIC_BASE_URL": "https://example.test/v1",
}

# 命令/MCP 工具项产出的事件类型三连（USE→RESULT→CALL）
_TOOL_EVENT_TRIPLE = [
    EventType.TOOL_USE,
    EventType.TOOL_RESULT,
    EventType.TOOL_CALL,
]


async def _events_from_spawned_proc(
    monkeypatch: pytest.MonkeyPatch,
    proc: _FakeCodexProc,
    driver: CodexCLIDriver | None = None,
) -> list:
    """把 asyncio.create_subprocess_exec 打成返回 proc，跑标准 query 并收集事件。"""

    async def fake_spawn(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    if driver is None:
        driver = CodexCLIDriver()
    events = []
    async for event in driver.query(
        "hi",
        model="gpt-test",
        env=_QUERY_ENV,
    ):
        events.append(event)
    return events


async def _query_with_managed_hooks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    writer: CodexHookConfigWriter,
    fake_spawn,
) -> list:
    """以给定 fake_spawn 跑一次带托管 hooks 的标准 codex query，收集事件。"""
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    events = []
    async for event in CodexCLIDriver().query(
        "hi",
        session_id="session-1",
        model="gpt-test",
        cwd=str(tmp_path),
        env=_QUERY_ENV,
        hooks=DriverHooks(
            session_id="session-1",
            file_change_hook_url="http://127.0.0.1:18765",
            runtime_hook_config_valid=lambda: writer.is_managed(tmp_path),
        ),
    ):
        events.append(event)
    return events


async def _query_with_gate(monkeypatch: pytest.MonkeyPatch, gate, fake_spawn) -> list:
    async def answer_question(session_id: str, data: dict):
        return "yes"

    monkeypatch.setattr(
        "workflow_controller.driver.runtime_gate.PerQueryGate",
        lambda *, hooks, session_id, require_callback: gate,
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    events = []
    async for event in CodexCLIDriver().query(
        "hi",
        session_id="session-1",
        model="gpt-test",
        env=_QUERY_ENV,
        hooks=DriverHooks(
            session_id="session-1",
            on_gate_question=answer_question,
        ),
    ):
        events.append(event)
    return events


class TestCodexLineToEvents:
    @staticmethod
    def test_thread_started():
        line = json.dumps({"type": "thread.started", "thread_id": "thread-123"})
        events = _codex_line_to_events(line, "sid", 1)
        assert len(events) == 1
        assert events[0].event_type == EventType.ITERATION_START
        assert events[0].data["session_id"] == "thread-123"
        assert events[0].data["subtype"] == "init"

    @staticmethod
    @pytest.mark.parametrize(
        "item_type",
        [
            "CollabAgentToolCall",
            "collab_agent_tool_call",
            "collab_tool_call",
        ],
    )
    def test_subagent_fact_extracts_collab_spawn(item_type):
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": item_type,
                    "tool": "spawn_agent",
                    "receiver_thread_ids": ["child-1"],
                    "agents_states": {"child-1": "pending_init"},
                },
            }
        )

        assert _codex_subagent_fact(json.loads(line)) == {
            "kind": "collab",
            "tool": "spawn_agent",
            "child_ids": ["child-1"],
            "states": {"child-1": "pending_init"},
        }

    @staticmethod
    def test_subagent_fact_extracts_parent_started():
        data = {"type": "thread.started", "thread_id": "parent-1"}

        assert _codex_subagent_fact(data) == {
            "kind": "parent_started",
            "parent_id": "parent-1",
        }

    @staticmethod
    @pytest.mark.parametrize(
        "state_value",
        [
            {"status": "completed", "message": None},
            {"completed": "Ran the requested command"},
            "completed",
        ],
    )
    def test_subagent_fact_normalizes_collab_states(state_value):
        data = {
            "type": "item.completed",
            "item": {
                "type": "CollabAgentToolCall",
                "tool": "wait",
                "receiver_thread_ids": [],
                "agents_states": {"child-1": state_value},
            },
        }

        assert _codex_subagent_fact(data)["states"] == {"child-1": "completed"}

    @staticmethod
    def test_turn_completed_normalizes_usage():
        line = json.dumps(
            {
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 100,
                    "cached_input_tokens": 40,
                    "output_tokens": 30,
                },
            }
        )
        events = _codex_line_to_events(line, "sid", 2)
        assert len(events) == 1
        assert events[0].event_type == EventType.STEP_FINISH
        assert events[0].data["usage"] == {
            "input_tokens": 100,
            "output_tokens": 30,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 40,
        }

    @staticmethod
    def test_turn_completed_normalizes_cache_write_input_tokens():
        line = json.dumps(
            {
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 100,
                    "cached_input_tokens": 40,
                    "cache_write_input_tokens": 55,
                    "output_tokens": 30,
                },
            }
        )
        events = _codex_line_to_events(line, "sid", 2)
        assert len(events) == 1
        assert events[0].event_type == EventType.STEP_FINISH
        assert events[0].data["usage"]["cache_creation_input_tokens"] == 55

    @staticmethod
    def test_turn_failed():
        line = json.dumps({"type": "turn.failed", "error": {"message": "boom"}})
        events = _codex_line_to_events(line, "sid", 1)
        assert len(events) == 1
        assert events[0].event_type == EventType.ERROR
        assert events[0].data["error"] == "boom"

    @staticmethod
    def test_turn_failed_normalizes_responses_protocol_mismatch() -> None:
        line = json.dumps(
            {
                "type": "turn.failed",
                "error": {
                    "message": (
                        "[Invalid request body: body.messages: Field required; "
                        "body.tool_choice: Input should be a valid dictionary]"
                    ),
                },
            }
        )

        events = _codex_line_to_events(line, "sid", 1)

        assert [event.event_type for event in events] == [EventType.ERROR]
        assert events[0].data["error"] == "response is not OpenAI Responses format"

    @staticmethod
    def test_turn_failed_normalizes_missing_messages_parameter():
        line = json.dumps(
            {
                "type": "turn.failed",
                "error": {
                    "message": ("The request failed because it is missing `messages` parameter."),
                },
            }
        )

        events = _codex_line_to_events(line, "sid", 1)

        assert [event.event_type for event in events] == [EventType.ERROR]
        assert events[0].data["error"] == "response is not OpenAI Responses format"

    @staticmethod
    @pytest.mark.parametrize(
        "message",
        [
            "not implemented",
            '{"error":{"message":"not implemented","code":"convert_request_failed"}}',
        ],
    )
    def test_turn_failed_normalizes_gateway_model_conversion_not_implemented(message: str):
        line = json.dumps({"type": "turn.failed", "error": {"message": message}})

        events = _codex_line_to_events(line, "sid", 1)

        assert [event.event_type for event in events] == [EventType.ERROR]
        assert (
            events[0].data["error"]
            == "gateway does not support this model with OpenAI Responses format"
        )

    @staticmethod
    def test_hook_trust_warning_is_ignored():
        line = json.dumps(
            {
                "type": "error",
                "message": "`--dangerously-bypass-hook-trust` is enabled. "
                "Enabled hooks may run without review for this invocation.",
            }
        )

        assert _codex_line_to_events(line, "sid", 1) == []

    @staticmethod
    def test_status_numbers_do_not_match_as_substrings():
        line = json.dumps(
            {
                "type": "turn.failed",
                "error": {"message": "port 14018, line 2403, and timeout 429ms"},
            }
        )
        events = _codex_line_to_events(line, "sid", 1)

        assert [event.event_type for event in events] == [EventType.ERROR]


class TestCodexProcEnv:
    @staticmethod
    def test_custom_provider_does_not_rewrite_workspace_config(
        tmp_path,
        monkeypatch,
    ):
        for key in ("ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "ANTHROPIC_MODEL"):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("OPENAI_API_KEY", "host-openai-real-key")
        monkeypatch.setenv("WORKFLOW_CONTROLLER_CODEX_API_KEY", "host-codex-real-key")
        monkeypatch.setenv("WORKFLOW_CONTROLLER_CODEX_TEST_RESIDUE", "host-codex-residue")
        config_dir = tmp_path / ".codex"
        config_dir.mkdir()
        (config_dir / "config.toml").write_text(
            'notify = true\n[model_providers.existing]\nname = "Existing"\n',
            encoding="utf-8",
        )

        env = _codex_proc_env(
            {
                "ANTHROPIC_API_KEY": "secret",
                "ANTHROPIC_BASE_URL": "https://example.test/v1",
                "ANTHROPIC_MODEL": "gpt-test",
            },
            cwd=str(tmp_path),
        )
        config_text = (config_dir / "config.toml").read_text(encoding="utf-8")
        overrides = _codex_config_overrides("https://example.test/v1")

        assert env["CODEX_HOME"] == str(config_dir)
        assert env["WORKFLOW_CONTROLLER_CODEX_API_KEY"] == "secret"
        assert "ANTHROPIC_API_KEY" not in env
        assert "OPENAI_API_KEY" not in env
        assert "WORKFLOW_CONTROLLER_CODEX_TEST_RESIDUE" not in env
        assert config_text == 'notify = true\n[model_providers.existing]\nname = "Existing"\n'
        assert overrides == [
            "-c",
            'model_provider="workflow_controller"',
            "-c",
            'model_providers.workflow_controller.name="Workflow Controller provider"',
            "-c",
            'model_providers.workflow_controller.base_url="https://example.test/v1"',
            "-c",
            'model_providers.workflow_controller.env_key="WORKFLOW_CONTROLLER_CODEX_API_KEY"',
            "-c",
            'model_providers.workflow_controller.wire_api="responses"',
        ]

    @staticmethod
    def test_gateway_root_base_url_gets_versioned_responses_path():
        overrides = _codex_config_overrides("https://yibuapi.com")

        assert 'model_providers.workflow_controller.base_url="https://yibuapi.com/v1"' in overrides

        explicit = _codex_config_overrides("https://open.bigmodel.cn/api/v1")
        assert (
            'model_providers.workflow_controller.base_url="https://open.bigmodel.cn/api/v1"'
            in explicit
        )

    @staticmethod
    def test_missing_base_url_fails_closed(tmp_path, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.setenv("OPENAI_API_KEY", "host-openai-secret")

        with pytest.raises(ValueError, match="explicit provider base_url"):
            _codex_proc_env(
                {"ANTHROPIC_API_KEY": "secret", "ANTHROPIC_MODEL": "gpt-test"},
                cwd=str(tmp_path),
            )

        with pytest.raises(ValueError, match="explicit provider base_url"):
            _codex_config_overrides("")
        assert not (tmp_path / ".codex").exists()

    @staticmethod
    def test_creates_isolated_codex_home(tmp_path):
        env = _codex_proc_env(_QUERY_ENV, cwd=str(tmp_path))

        assert (tmp_path / ".codex").is_dir()
        assert env["CODEX_HOME"] == str(tmp_path / ".codex")
        assert not (tmp_path / ".codex" / "config.toml").exists()

    @staticmethod
    def test_agent_message():
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "i1", "type": "agent_message", "text": "hello"},
            }
        )
        events = _codex_line_to_events(line, "sid", 1)
        assert len(events) == 1
        assert events[0].event_type == EventType.ASSISTANT_MESSAGE
        assert events[0].data["content"] == [{"type": "text", "text": "hello"}]

    @staticmethod
    def test_reasoning_uses_thinking_block():
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "i1", "type": "reasoning", "text": "thinking..."},
            }
        )
        events = _codex_line_to_events(line, "sid", 1)
        assert events[0].event_type == EventType.ASSISTANT_MESSAGE
        assert events[0].data["content"][0]["type"] == "thinking"

    @staticmethod
    def test_command_execution_emits_tool_events():
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "id": "cmd1",
                    "type": "command_execution",
                    "command": "bash -lc ls",
                    "aggregated_output": "README.md",
                    "exit_code": 0,
                    "status": "completed",
                },
            }
        )
        events = _codex_line_to_events(line, "sid", 1)
        assert [e.event_type for e in events] == _TOOL_EVENT_TRIPLE
        assert events[0].data == {
            "id": "cmd1",
            "name": "Bash",
            "input": {"command": "bash -lc ls"},
        }
        assert events[1].data["tool_name"] == "Bash"
        assert events[1].data["content"] == "README.md"
        assert events[1].data["is_error"] is False
        assert events[2].data["arg"] == "bash -lc ls"

    @staticmethod
    def test_command_execution_nonzero_exit_is_error():
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "id": "cmd1",
                    "type": "command_execution",
                    "command": "false",
                    "aggregated_output": "failed",
                    "exit_code": 1,
                    "status": "completed",
                },
            }
        )
        events = _codex_line_to_events(line, "sid", 1)
        assert events[1].data["is_error"] is True
        assert events[2].data["is_error"] is True
        assert events[2].data["error"] == "failed"

    @staticmethod
    def test_mcp_tool_call_emits_tool_events():
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "id": "mcp1",
                    "type": "mcp_tool_call",
                    "server": "github",
                    "tool": "get_issue",
                    "arguments": {"issue_number": 1},
                    "result": {"content": "issue body"},
                    "status": "completed",
                },
            }
        )
        events = _codex_line_to_events(line, "sid", 1)
        assert [e.event_type for e in events] == _TOOL_EVENT_TRIPLE
        assert events[0].data["name"] == "get_issue"
        assert events[1].data["content"] == "issue body"

    @staticmethod
    def test_mcp_error_is_error():
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "id": "mcp1",
                    "type": "mcp_tool_call",
                    "tool": "bad",
                    "arguments": {},
                    "error": {"message": "denied"},
                    "status": "failed",
                },
            }
        )
        events = _codex_line_to_events(line, "sid", 1)
        assert events[1].data["is_error"] is True
        assert events[2].data["error"] == "denied"

    @staticmethod
    def test_started_and_updated_ignored():
        started = json.dumps(
            {
                "type": "item.started",
                "item": {"id": "i1", "type": "agent_message", "text": "partial"},
            }
        )
        updated = json.dumps(
            {
                "type": "item.updated",
                "item": {"id": "i1", "type": "agent_message", "text": "partial"},
            }
        )
        assert _codex_line_to_events(started, "sid", 1) == []
        assert _codex_line_to_events(updated, "sid", 1) == []

    @staticmethod
    def test_top_level_error():
        line = json.dumps({"type": "error", "message": "auth failed"})
        events = _codex_line_to_events(line, "sid", 1)
        assert len(events) == 1
        assert events[0].event_type == EventType.ERROR
        assert events[0].data["error"] == "auth failed"

    @staticmethod
    def test_unknown_type_and_bad_json_ignored():
        assert _codex_line_to_events(json.dumps({"type": "unknown"}), "sid", 1) == []
        assert _codex_line_to_events("not json {", "sid", 1) == []

    @pytest.mark.parametrize(
        "provider_env",
        [
            {},
            {"ANTHROPIC_API_KEY": ""},
            {"ANTHROPIC_API_KEY": " \t\n"},
        ],
    )
    def test_missing_provider_api_key_fails_closed(
        self,
        provider_env: dict[str, str],
        monkeypatch,
    ):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "host-anthropic-secret")
        monkeypatch.setenv("OPENAI_API_KEY", "host-openai-secret")
        monkeypatch.setenv("CODEX_HOME", "/tmp/host-codex-home")

        with pytest.raises(ValueError, match="provider API key"):
            _codex_proc_env(provider_env)

    @pytest.mark.parametrize(
        ("message", "kind"),
        [
            ("authentication failed", "auth_error"),
            ("HTTP 429: too many requests", "rejected"),
        ],
    )
    def test_turn_failed_error_text_becomes_limit_signal(self, message, kind):
        line = json.dumps({"type": "turn.failed", "error": {"message": message}})
        events = _codex_line_to_events(line, "sid", 1)

        assert [event.event_type for event in events] == [
            EventType.RATE_LIMIT,
            EventType.ERROR,
        ]
        assert events[0].data["limit_signal"]["kind"] == kind
        assert events[0].data["raw"] == message

    @pytest.mark.parametrize("message", ["authentication failed", "HTTP 429"])
    def test_top_level_error_becomes_limit_signal(self, message):
        line = json.dumps({"type": "error", "message": message})
        events = _codex_line_to_events(line, "sid", 1)

        assert [event.event_type for event in events] == [
            EventType.RATE_LIMIT,
            EventType.ERROR,
        ]
        assert events[1].data["error"] == message


class TestCodexGateConfig:
    @staticmethod
    def test_gate_overrides_fail_closed():
        overrides = _codex_config_overrides(
            "https://example.test/v1",
            gate_url="http://127.0.0.1:9/mcp",
        )

        assert 'mcp_servers.workflow_controller_gate.url="http://127.0.0.1:9/mcp"' in overrides
        assert "mcp_servers.workflow_controller_gate.required=true" in overrides
        assert "mcp_servers.workflow_controller_gate.tool_timeout_sec=86400" in overrides
        assert (
            "mcp_servers.workflow_controller_gate.tools."
            'workflow_controller_ask.approval_mode="approve"' in overrides
        )


class TestBuildArgs:
    @staticmethod
    def test_command_prefix_precedes_codex_command():
        """验证 CLI wrapper 位于完整 Codex 命令前。"""
        args = _build_args(
            bin_path="codex",
            prompt="p",
            model="",
            cwd=None,
            permission_mode="read-only",
            add_dirs=[],
            resume=None,
            fork_session=False,
            command_prefix=["cpx"],
        )

        assert args[:5] == ["cpx", "codex", "exec", "--json", "--skip-git-repo-check"]

    @staticmethod
    def test_basic_args():
        args = _build_args(
            bin_path="codex",
            prompt="do work",
            model="gpt-5",
            cwd="/tmp/proj",
            permission_mode="bypassPermissions",
            add_dirs=[],
            resume=None,
            fork_session=False,
        )
        assert args[:6] == [
            "codex",
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--cd",
            "/tmp/proj",
        ]
        assert "--model" in args
        assert "gpt-5" in args
        assert "--approve-for-me" not in args
        assert args[args.index("--sandbox") + 1] == "danger-full-access"
        assert args[-2:] == ["--", "do work"]

    @staticmethod
    def test_config_overrides_are_process_arguments():
        overrides = _codex_config_overrides("https://a.test/v1")
        args = _build_args(
            bin_path="codex",
            prompt="do work",
            model="gpt-5",
            cwd="/tmp/proj",
            permission_mode="read-only",
            add_dirs=[],
            resume=None,
            fork_session=False,
            config_overrides=overrides,
        )

        assert args[:8] == [
            "codex",
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--cd",
            "/tmp/proj",
            "-c",
            'model_provider="workflow_controller"',
        ]
        assert 'model_providers.workflow_controller.base_url="https://a.test/v1"' in args
        assert "--model" in args

    @staticmethod
    def test_model_catalog_is_injected_as_config_argument(tmp_path):
        catalog = tmp_path / "models.json"
        catalog.write_text(
            '{"models":[{"slug":"test-model"}]}',
            encoding="utf-8",
        )

        overrides = _codex_config_overrides(
            "https://a.test/v1",
            model_catalog=catalog,
        )

        assert f'model_catalog_json="{catalog.resolve()}"' in overrides
        assert 'web_search="disabled"' in overrides

    @staticmethod
    def test_resolve_model_catalog_uses_provider_path(tmp_path):
        catalog = tmp_path / "explicit-models.json"
        catalog.write_text('{"models":[{"slug":"test-model"}]}', encoding="utf-8")

        assert (
            getattr(codex_cli, "_resolve_model_catalog")(
                {"model_catalog_path": str(catalog)},
                "test-model",
            )
            == catalog.resolve()
        )

    @staticmethod
    def test_resolve_model_catalog_expands_home_path(tmp_path, monkeypatch):
        home = tmp_path / "home"
        catalog = home / "models.json"
        catalog.parent.mkdir(parents=True)
        catalog.write_text('{"models":[{"slug":"test-model"}]}', encoding="utf-8")
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setattr(Path, "home", lambda: home)

        assert (
            getattr(codex_cli, "_resolve_model_catalog")(
                {"model_catalog_path": "~/models.json"},
                "test-model",
            )
            == catalog.resolve()
        )

    @staticmethod
    def test_resolve_model_catalog_does_not_fallback_to_host_catalog(
        tmp_path,
        monkeypatch,
    ):
        home = tmp_path / "home"
        host_catalog = home / ".codex" / "models.json"
        host_catalog.parent.mkdir(parents=True)
        host_catalog.write_text('{"models":[]}', encoding="utf-8")
        (host_catalog.parent / "config.toml").write_text(
            f"model_catalog_json = {json.dumps(str(host_catalog))}\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setattr(Path, "home", lambda: home)
        monkeypatch.delenv("CODEX_HOME", raising=False)

        assert getattr(codex_cli, "_resolve_model_catalog")({}, "test-model") is None

    @staticmethod
    def test_resolve_model_catalog_rejects_inaccessible_path(
        tmp_path,
        monkeypatch,
    ):
        catalog = tmp_path / "models.json"
        catalog.write_text('{"models":[{"slug":"test-model"}]}', encoding="utf-8")
        real_is_file = Path.is_file

        def inaccessible_is_file(path: Path) -> bool:
            if path == catalog:
                raise PermissionError
            return real_is_file(path)

        monkeypatch.setattr(Path, "is_file", inaccessible_is_file)

        with pytest.raises(
            ValueError,
            match="请输入合法且包含正确模型名的 model catalog JSON 文件路径",
        ):
            getattr(codex_cli, "_resolve_model_catalog")(
                {"model_catalog_path": str(catalog)},
                "test-model",
            )

    @staticmethod
    def test_resume_and_fork():
        args = _build_args(
            bin_path="codex",
            prompt="continue",
            model="",
            cwd=None,
            permission_mode="workspace-write",
            add_dirs=[],
            resume="thread-1",
            fork_session=True,
        )
        assert args == [
            "codex",
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--sandbox",
            "workspace-write",
            "fork",
            "thread-1",
            "--",
            "continue",
        ]

    @pytest.mark.parametrize(
        ("raw_path", "content"),
        [
            pytest.param("models.json", None, id="relative"),
            pytest.param(None, "{", id="invalid-json"),
            pytest.param(None, "{}", id="missing-models"),
            pytest.param(
                None,
                '{"models":[{"slug":"other-model"}]}',
                id="missing-model",
            ),
        ],
    )
    def test_resolve_model_catalog_rejects_invalid_catalog(
        self,
        tmp_path,
        raw_path,
        content,
    ):
        catalog = tmp_path / "models.json"
        if content is not None:
            catalog.write_text(content, encoding="utf-8")

        with pytest.raises(
            ValueError,
            match="请输入合法且包含正确模型名的 model catalog JSON 文件路径",
        ):
            getattr(codex_cli, "_resolve_model_catalog")(
                {"model_catalog_path": raw_path or str(catalog)},
                "test-model",
            )

    @pytest.mark.parametrize(
        ("permission_mode", "sandbox"),
        [
            ("acceptEdits", "workspace-write"),
            ("default", "read-only"),
            ("read-only", "read-only"),
            ("plan", "read-only"),
            ("workspace-write", "workspace-write"),
            ("danger-full-access", "danger-full-access"),
            ("unknown-mode", "read-only"),
        ],
    )
    def test_permission_mode_mapping(self, permission_mode, sandbox):
        args = _build_args(
            bin_path="codex",
            prompt="p",
            model="",
            cwd=None,
            permission_mode=permission_mode,
            add_dirs=[],
            resume=None,
            fork_session=False,
        )
        assert args == [
            "codex",
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--sandbox",
            sandbox,
            "--",
            "p",
        ]


@pytest.mark.asyncio
async def test_drain_stderr_downgrades_known_and_unknown_noise(caplog):
    """Hook trust、cpx 状态、stdin 提示与普通文本都降级为 debug。"""

    class FakeStderr:
        """逐行输出预设 stderr。"""

        def __init__(self, lines: list[str]):
            self.lines = lines

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.lines:
                return self.lines.pop(0).encode()
            raise StopAsyncIteration

    hook_warning = (
        "`--dangerously-bypass-hook-trust` is enabled. Enabled hooks may run "
        "without review for this invocation."
    )
    lines = [
        hook_warning,
        "Reading additional input from stdin...",
        "[cpx-cli] launching: codex exec --json",
        "[cpx-cli] ERROR: launch failed",
        "plain continuation text",
    ]
    proc = SimpleNamespace(stderr=FakeStderr(lines))
    tail = deque()
    error_tail = deque()

    with caplog.at_level(logging.DEBUG, logger="workflow_controller.driver.codex_cli"):
        await _drain_stderr(proc, tail, error_tail)

    assert [record.levelno for record in caplog.records] == [
        logging.DEBUG,
        logging.DEBUG,
        logging.DEBUG,
        logging.ERROR,
        logging.DEBUG,
    ]
    assert list(error_tail) == ["[cpx-cli] ERROR: launch failed"]


@pytest.mark.asyncio
async def test_drain_stderr_warns_errors_and_preserves_error_tail():
    """错误行记错误日志且单独保留，不被后续普通行挤出 tail。"""
    error = "2026-11-17T00:00:00.000000Z ERROR codex_core: boom"
    lines = [f"{error}\n".encode()] + [f"normal {i}\n".encode() for i in range(25)]
    proc = SimpleNamespace(stderr=_FakeCodexErr(lines))
    tail = deque(maxlen=20)
    error_tail = deque(maxlen=20)

    await _drain_stderr(proc, tail, error_tail)

    assert list(error_tail) == [error]
    assert len(tail) == 20
    assert _stderr_tail_text(tail, error_tail).startswith(error)


@pytest.mark.asyncio
async def test_query_resolves_permission_mode(monkeypatch):
    captured: dict[str, object] = {}

    def fake_build_args(*, permission_mode: str, **kwargs):
        captured["permission_mode"] = permission_mode
        return ["/nonexistent-codex-bin"]

    monkeypatch.setattr(codex_cli, "_build_args", fake_build_args)
    driver = CodexCLIDriver(permission_mode="read-only")
    events = []

    async for event in driver.query(
        "hi",
        model="gpt-test",
        env=_QUERY_ENV,
    ):
        events.append(event)

    assert captured["permission_mode"] == "read-only"
    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]
    assert events[-1].data["reason"] == "spawn_failed"

    driver = CodexCLIDriver(permission_mode="read-only")

    async for _ in driver.query(
        "hi",
        model="gpt-test",
        permission_mode="workspace-write",
        env=_QUERY_ENV,
    ):
        pass

    assert captured["permission_mode"] == "workspace-write"


@pytest.mark.asyncio
async def test_query_rejects_system_prompt():
    driver = CodexCLIDriver()
    events = []
    async for event in driver.query(
        "hi",
        model="gpt-test",
        system_prompt="x",
    ):
        events.append(event)
    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]
    assert "system_prompt" in events[1].data["error"]


@pytest.mark.asyncio
async def test_query_missing_provider_api_key_rejects_before_spawn(monkeypatch):
    def fake_spawn(*args, **kwargs):
        raise AssertionError("codex must not spawn without provider credentials")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "host-anthropic-secret")
    monkeypatch.setenv("OPENAI_API_KEY", "host-openai-secret")
    monkeypatch.setenv("CODEX_HOME", "/tmp/host-codex-home")
    driver = CodexCLIDriver()
    events = [event async for event in driver.query("hi", model="gpt-test")]

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]
    assert "provider API key" in events[1].data["error"]
    assert events[-1].data["reason"] == "rejected_params"


@pytest.mark.asyncio
async def test_query_missing_base_url_rejects_before_spawn(monkeypatch):
    def fake_spawn(*args, **kwargs):
        raise AssertionError("codex must not spawn without an explicit base_url")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setenv("OPENAI_API_KEY", "host-openai-secret")
    driver = CodexCLIDriver()
    events = []
    async for event in driver.query(
        "hi",
        model="gpt-test",
        env={"ANTHROPIC_API_KEY": "secret"},
    ):
        events.append(event)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]
    assert "explicit provider base_url" in events[1].data["error"]
    assert events[-1].data["reason"] == "rejected_params"


@pytest.mark.asyncio
async def test_query_requires_explicit_model():
    driver = CodexCLIDriver()
    events = [event async for event in driver.query("hi", model="")]

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]
    assert "explicit model" in events[1].data["error"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_turns": 1},
        {"allowed_tools": ["Bash"]},
        {"disallowed_tools": ["Bash"]},
    ],
)
async def test_query_rejects_unsupported_tool_and_turn_params(kwargs):
    driver = CodexCLIDriver()
    events = [event async for event in driver.query("hi", model="gpt-test", **kwargs)]
    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"system_prompt": "x"},
        {"max_turns": 1},
        {"allowed_tools": ["Bash"]},
    ],
)
async def test_query_rejects_params_before_gate_start(monkeypatch, kwargs):
    async def answer_question(session_id: str, data: dict):
        return "yes"

    def fake_gate(*args, **kwargs):
        raise AssertionError("invalid params must not start gate")

    monkeypatch.setattr(
        "workflow_controller.driver.runtime_gate.PerQueryGate",
        fake_gate,
    )
    events = []
    async for event in CodexCLIDriver().query(
        "hi",
        model="gpt-test",
        env=_QUERY_ENV,
        hooks=DriverHooks(
            session_id="session-1",
            on_gate_question=answer_question,
        ),
        **kwargs,
    ):
        events.append(event)

    assert events[-1].data["reason"] == "rejected_params"


@pytest.mark.asyncio
async def test_query_internal_error_emits_iteration_end(monkeypatch):
    proc = _FakeCodexProc(0, [b""], [])

    async def failing_wait() -> int:
        raise RuntimeError("boom")

    proc.wait = failing_wait
    events = await _events_from_spawned_proc(monkeypatch, proc)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]
    assert events[-1].data["reason"] == "internal_error"


@pytest.mark.asyncio
async def test_query_rate_limit_error_emits_limit_signal(monkeypatch):
    proc = _FakeCodexProc(
        1,
        [b'{"type":"turn.failed","error":{"message":"HTTP 429: too many requests"}}\n', b""],
        [],
    )
    events = await _events_from_spawned_proc(monkeypatch, proc)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.RATE_LIMIT,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]
    assert events[1].data["limit_signal"]["kind"] == "rejected"


class _FakeCodexIn:
    @staticmethod
    def close() -> None:
        pass


class _FakeCodexOut:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)

    async def read(self, _n: int = -1) -> bytes:
        if not self._chunks:
            return b""
        return self._chunks.pop(0)


class _HangingCodexOut:
    """读完给定块后模拟输出流持续阻塞。"""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)

    async def read(self, _n: int = -1) -> bytes:
        if self._chunks:
            return self._chunks.pop(0)
        await asyncio.Event().wait()
        return b""


class _FakeSubagentMonitor:
    """记录协作事实与 finish 调用的假监视器。"""

    def __init__(self) -> None:
        self.facts: list[dict] = []
        self.finished = False
        self.started_children: set[str] = set()

    async def handle_fact(self, fact: dict) -> None:
        self.facts.append(fact)

    async def finish(self) -> None:
        self.finished = True

    async def notify_subagent_started(self, child_thread_id: str) -> None:
        self.started_children.add(child_thread_id)


class _FakeCodexOpenOut:
    """返回首块后模拟输出流保持打开。"""

    def __init__(self, chunk: bytes, driver: CodexCLIDriver) -> None:
        self._chunk = chunk
        self._driver = driver
        self.second_read = False

    async def read(self, _n: int = -1) -> bytes:
        if self._chunk is None:
            self.second_read = True
            await self._driver.stop()
            return b""
        chunk, self._chunk = self._chunk, None
        return chunk


class _FakeCodexErr:
    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)
        self._index = 0

    def __aiter__(self) -> _FakeCodexErr:
        return self

    async def __anext__(self) -> bytes:
        if self._index >= len(self._lines):
            raise StopAsyncIteration
        line = self._lines[self._index]
        self._index += 1
        return line


class _FakeCodexProc:
    def __init__(
        self,
        returncode: int,
        stdout_chunks: list[bytes],
        stderr_lines: list[bytes],
    ) -> None:
        self.returncode = returncode
        self.pid = 1234
        self.stdin = _FakeCodexIn()
        self.stdout = _FakeCodexOut(stdout_chunks)
        self.stderr = _FakeCodexErr(stderr_lines)

    async def wait(self) -> int:
        return self.returncode


@pytest.mark.asyncio
async def test_missing_cpx_reports_wrapper_command_and_remedy(monkeypatch):
    """验证 cpx 缺失时按 wrapper 报错。"""
    spawn = AsyncMock(side_effect=FileNotFoundError("cpx"))
    monkeypatch.setattr(
        "workflow_controller.driver.codex_cli.asyncio.create_subprocess_exec", spawn
    )
    events = []
    async for event in CodexCLIDriver(
        codex_bin="codex",
        command_prefix=["cpx"],
    ).query("hi", model="gpt-test", env=_QUERY_ENV):
        events.append(event)

    assert spawn.call_args.args[:2] == ("cpx", "codex")
    assert events[1].data["error"].startswith("command not found: cpx;")
    assert "install cpx" in events[1].data["error"]


@pytest.mark.asyncio
async def test_glm_5_2_preset_catalog_is_injected(monkeypatch, tmp_path):
    monkeypatch.setattr(
        codex_catalog,
        "_bundled_model_slugs",
        lambda: frozenset(),
    )
    resolution = codex_catalog.resolve_codex_catalog(
        model="glm-5.2",
        config_dir=tmp_path,
    )
    assert resolution.status == "preset"
    catalog = resolution.model_catalog_path
    assert catalog is not None
    spawn = AsyncMock(return_value=_FakeCodexProc(0, [b""], []))
    monkeypatch.setattr(
        "workflow_controller.driver.codex_cli.asyncio.create_subprocess_exec", spawn
    )

    _ = []
    async for event in CodexCLIDriver().query(
        "hi",
        model="glm-5.2",
        env={**_QUERY_ENV, "model_catalog_path": str(catalog)},
    ):
        _.append(event)

    assert f'model_catalog_json="{catalog.resolve()}"' in spawn.call_args.args
    model_index = spawn.call_args.args.index("--model")
    assert spawn.call_args.args[model_index + 1] == "glm-5.2"
    assert "model_catalog_path" not in spawn.call_args.kwargs["env"]
    assert not [key for key in spawn.call_args.kwargs["env"] if key.endswith("MODEL_CATALOG")]


@pytest.mark.asyncio
async def test_claude_model_with_catalog_is_passed_to_codex(monkeypatch, tmp_path):
    catalog = tmp_path / "models.json"
    catalog.write_text('{"models":[{"slug":"claude-opus-5-5"}]}', encoding="utf-8")
    spawn = AsyncMock(return_value=_FakeCodexProc(0, [b""], []))
    monkeypatch.setattr(
        "workflow_controller.driver.codex_cli.asyncio.create_subprocess_exec", spawn
    )

    events = []
    async for event in CodexCLIDriver().query(
        "hi",
        model="claude-opus-5-5",
        env={**_QUERY_ENV, "model_catalog_path": str(catalog)},
    ):
        events.append(event)

    assert not [event for event in events if event.event_type == EventType.ERROR]
    assert f'model_catalog_json="{catalog.resolve()}"' in spawn.call_args.args
    model_index = spawn.call_args.args.index("--model")
    assert spawn.call_args.args[model_index + 1] == "claude-opus-5-5"


@pytest.mark.asyncio
async def test_missing_provider_catalog_omits_codex_config(monkeypatch):
    spawn = AsyncMock(return_value=_FakeCodexProc(0, [b""], []))
    monkeypatch.setattr(
        "workflow_controller.driver.codex_cli.asyncio.create_subprocess_exec", spawn
    )

    _ = []
    async for event in CodexCLIDriver().query(
        "hi",
        model="gpt-test",
        env=_QUERY_ENV,
    ):
        _.append(event)

    assert spawn.await_count == 1
    assert not any(argument.startswith("model_catalog_json=") for argument in spawn.call_args.args)


@pytest.mark.asyncio
async def test_query_spawns_codex_in_requested_cwd(monkeypatch, tmp_path):
    spawn = AsyncMock(return_value=_FakeCodexProc(0, [b""], []))
    monkeypatch.setattr(
        "workflow_controller.driver.codex_cli.asyncio.create_subprocess_exec", spawn
    )

    async for _ in CodexCLIDriver().query(
        "hi",
        model="gpt-test",
        cwd=str(tmp_path),
        env=_QUERY_ENV,
    ):
        pass

    assert spawn.call_args.kwargs["cwd"] == str(tmp_path)
    cwd_index = spawn.call_args.args.index("--cd")
    assert spawn.call_args.args[cwd_index + 1] == str(tmp_path)


@pytest.mark.asyncio
async def test_query_zero_exit_emits_iteration_end(monkeypatch):
    proc = _FakeCodexProc(
        0,
        [b'{"type":"thread.started","thread_id":"t1"}\n', b""],
        [],
    )
    events = await _events_from_spawned_proc(monkeypatch, proc)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ITERATION_START,
        EventType.ITERATION_END,
    ]


@pytest.mark.asyncio
async def test_query_starts_and_stops_gate(monkeypatch):
    class FakeGate:
        url = "http://127.0.0.1:9/mcp"

        def __init__(self):
            self.started = False
            self.stopped = False

        async def start(self):
            self.started = True

        async def stop(self):
            self.stopped = True

    gate = FakeGate()
    proc = _FakeCodexProc(0, [], [])
    captured = {}

    async def fake_spawn(*args, **kwargs):
        captured["args"] = args
        return proc

    events = await _query_with_gate(monkeypatch, gate, fake_spawn)

    assert events[-1].event_type == EventType.ITERATION_END
    assert gate.started and gate.stopped
    assert 'mcp_servers.workflow_controller_gate.url="http://127.0.0.1:9/mcp"' in captured["args"]


@pytest.mark.asyncio
async def test_query_gate_failure_does_not_spawn(monkeypatch):
    class FakeGate:
        stopped = False

        async def start(self):
            raise RuntimeError("no port")

        async def stop(self):
            self.stopped = True

    gate = FakeGate()

    def fake_spawn(*args, **kwargs):
        raise AssertionError("gate failure must not spawn codex")

    events = await _query_with_gate(monkeypatch, gate, fake_spawn)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]
    assert events[-1].data["reason"] == "gate_failed"
    assert gate.stopped


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_during", ["gate_start", "spawn"])
async def test_query_cancellation_stops_gate_before_stream(monkeypatch, cancel_during):
    async def answer_question(session_id: str, data: dict):
        return "yes"

    class FakeGate:
        url = "http://127.0.0.1:9/mcp"

        def __init__(self):
            self.stopped = False

        async def start(self):
            reached_cancel_point.set()
            if cancel_during == "gate_start":
                await asyncio.Event().wait()

        async def stop(self):
            self.stopped = True

    gate = FakeGate()
    reached_cancel_point = asyncio.Event()

    async def fake_spawn(*args, **kwargs):
        reached_cancel_point.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(
        "workflow_controller.driver.runtime_gate.PerQueryGate",
        lambda *, hooks, session_id, require_callback: gate,
    )
    monkeypatch.setattr(
        "workflow_controller.driver.codex_cli.asyncio.create_subprocess_exec",
        fake_spawn,
    )
    events = CodexCLIDriver().query(
        "hi",
        session_id="session-1",
        model="gpt-test",
        env=_QUERY_ENV,
        hooks=DriverHooks(
            session_id="session-1",
            on_gate_question=answer_question,
        ),
    )

    try:
        assert (await events.__anext__()).event_type == EventType.USER_MESSAGE
        query_task = asyncio.create_task(events.__anext__())
        await asyncio.wait_for(reached_cancel_point.wait(), timeout=1)
        query_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await query_task
    finally:
        await events.aclose()

    assert gate.stopped


async def test_query_forwards_subagent_facts_and_finishes_monitor(monkeypatch):
    monitor = _FakeSubagentMonitor()
    proc = _FakeCodexProc(
        0,
        [
            b'{"type":"thread.started","thread_id":"parent-1"}\n',
            b'{"type":"item.completed","item":{"type":"CollabAgentToolCall",'
            b'"tool":"spawn_agent","receiver_thread_ids":["child-1"],'
            b'"agents_states":{"child-1":"pending_init"}}}\n',
            b'{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":2}}\n',
            b"",
        ],
        [],
    )

    async def fake_spawn(*args, **kwargs):
        return proc

    async def on_subagent_progress(_session_id: str, _data: dict) -> None:
        return None

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(
        codex_cli,
        "SubagentProgressMonitor",
        lambda hooks, session_id, codex_home: monitor,
    )
    driver = CodexCLIDriver()
    events = [
        event
        async for event in driver.query(
            "hi",
            model="gpt-test",
            env=_QUERY_ENV,
            session_id="sid",
            hooks=DriverHooks(session_id="sid", on_subagent_progress=on_subagent_progress),
        )
    ]

    assert events[-1].event_type == EventType.ITERATION_END
    assert monitor.facts == [
        {"kind": "parent_started", "parent_id": "parent-1"},
        {
            "kind": "collab",
            "tool": "spawn_agent",
            "child_ids": ["child-1"],
            "states": {"child-1": "pending_init"},
        },
    ]
    assert monitor.finished


@pytest.mark.asyncio
async def test_query_cancel_finishes_subagent_monitor(monkeypatch):
    monitor = _FakeSubagentMonitor()
    bridge = SubagentProgressBridge()
    proc = _FakeCodexProc(0, [], [])
    proc.stdout = _HangingCodexOut(
        [
            b'{"type":"thread.started","thread_id":"parent-1"}\n',
            b'{"type":"item.completed","item":{"type":"CollabAgentToolCall",'
            b'"tool":"spawn_agent","receiver_thread_ids":["child-1"],'
            b'"agents_states":{"child-1":"pending_init"}}}\n',
        ]
    )

    async def fake_spawn(*args, **kwargs):
        return proc

    async def on_subagent_progress(_session_id: str, _data: dict) -> None:
        return None

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(
        codex_cli,
        "SubagentProgressMonitor",
        lambda hooks, session_id, codex_home: monitor,
    )
    driver = CodexCLIDriver()
    events = driver.query(
        "hi",
        model="gpt-test",
        env=_QUERY_ENV,
        session_id="sid",
        hooks=DriverHooks(
            session_id="sid",
            on_subagent_progress=on_subagent_progress,
            subagent_progress_bridge=bridge,
        ),
    )

    async def consume() -> None:
        async for _event in events:
            pass

    task = asyncio.create_task(consume())
    for _ in range(200):
        if len(monitor.facts) == 2:
            break
        await asyncio.sleep(0.01)
    await bridge.notify_started("child-1")
    assert monitor.started_children == {"child-1"}
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert len(monitor.facts) == 2
    assert monitor.finished
    assert bridge._monitor is None


@pytest.mark.asyncio
async def test_query_ends_after_turn_completed_without_stdout_eof(monkeypatch):
    driver = CodexCLIDriver()
    proc = _FakeCodexProc(-9, [], [])
    proc.stdout = _FakeCodexOpenOut(
        b'{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":2}}\n',
        driver,
    )
    events = await _events_from_spawned_proc(monkeypatch, proc, driver)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.STEP_FINISH,
        EventType.ITERATION_END,
    ]
    assert not proc.stdout.second_read


@pytest.mark.asyncio
async def test_query_nonzero_exit_after_final_turn_completed_without_newline(monkeypatch):
    proc = _FakeCodexProc(
        3,
        [b'{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":2}}', b""],
        [],
    )
    events = await _events_from_spawned_proc(monkeypatch, proc)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.STEP_FINISH,
        EventType.ITERATION_END,
    ]


@pytest.mark.asyncio
async def test_query_nonzero_exit_emits_error_with_stderr_tail(monkeypatch):
    proc = _FakeCodexProc(
        3,
        [b'{"type":"thread.started","thread_id":"t1"}\n', b""],
        [b"boom one\n", b"boom two\n"],
    )
    events = await _events_from_spawned_proc(monkeypatch, proc)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ITERATION_START,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]
    err = events[2].data["error"]
    assert "code 3" in err
    assert "boom one" in err
    assert "boom two" in err


@pytest.mark.asyncio
async def test_query_nonzero_exit_after_stream_error_no_fallback_and_ends_iteration(monkeypatch):
    proc = _FakeCodexProc(
        1,
        [
            b'{"type":"error","message":"auth failed"}\n'
            b'{"type":"turn.failed","error":{"message":"boom"}}\n',
            b"",
        ],
        [b"fallback stderr\n"],
    )
    events = await _events_from_spawned_proc(monkeypatch, proc)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ERROR,
        EventType.ERROR,
        EventType.ITERATION_END,
    ]
    assert events[1].data["error"] == "auth failed"
    assert events[2].data["error"] == "boom"
    assert "fallback stderr" not in events[-1].data.get("error", "")


@pytest.mark.asyncio
async def test_query_stop_emits_iteration_end(monkeypatch):
    proc = _FakeCodexProc(-9, [], [])
    driver = CodexCLIDriver()
    proc.stdout = _FakeCodexStoppingOut(driver)

    def fake_kill_proc(fake_proc: _FakeCodexProc) -> None:
        fake_proc.returncode = -9

    # stop() 继承自基类（经 cli_base.kill_proc_group），query 收尾走本模块绑定
    monkeypatch.setattr(codex_cli, "kill_proc_group", fake_kill_proc)
    monkeypatch.setattr(cli_base, "kill_proc_group", fake_kill_proc)
    events = await _events_from_spawned_proc(monkeypatch, proc, driver)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ITERATION_END,
    ]
    assert all(event.event_type != EventType.ERROR for event in events)


class _FakeCodexStoppingOut:
    def __init__(self, driver: CodexCLIDriver) -> None:
        self._driver = driver
        self._stopped = False

    async def read(self, _n: int = -1) -> bytes:
        if not self._stopped:
            self._stopped = True
            await self._driver.stop()
        return b""


class _FakeCodexStopWithChunkOut:
    """stop 与 chunk 同批到达：已读 chunk 的行必须完整消费（块级 stop 语义）。"""

    def __init__(self, chunk: bytes, driver: CodexCLIDriver) -> None:
        self._chunk = chunk
        self._driver = driver
        self._read = False

    async def read(self, _n: int = -1) -> bytes:
        if self._read:
            return b""
        self._read = True
        await self._driver.stop()
        return self._chunk


@pytest.mark.asyncio
async def test_query_stop_mid_chunk_still_consumes_read_chunk(monkeypatch):
    proc = _FakeCodexProc(-9, [], [])
    driver = CodexCLIDriver()
    proc.stdout = _FakeCodexStopWithChunkOut(
        b'{"type":"thread.started","thread_id":"t1"}\n'
        b'{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":2}}\n',
        driver,
    )

    def fake_kill_proc(fake_proc: _FakeCodexProc) -> None:
        fake_proc.returncode = -9

    monkeypatch.setattr(codex_cli, "kill_proc_group", fake_kill_proc)
    monkeypatch.setattr(cli_base, "kill_proc_group", fake_kill_proc)
    events = await _events_from_spawned_proc(monkeypatch, proc, driver)

    assert [event.event_type for event in events] == [
        EventType.USER_MESSAGE,
        EventType.ITERATION_START,
        EventType.STEP_FINISH,
        EventType.ITERATION_END,
    ]


@pytest.mark.asyncio
async def test_query_enables_managed_hooks(tmp_path, monkeypatch):
    writer = CodexHookConfigWriter()
    writer.prepare(tmp_path)
    proc = _FakeCodexProc(0, [], [])
    captured: dict[str, object] = {}

    async def fake_spawn(*args, **kwargs):
        captured["args"] = args
        captured["env"] = kwargs["env"]
        return proc

    events = await _query_with_managed_hooks(monkeypatch, tmp_path, writer, fake_spawn)

    assert [event.event_type for event in events][-1] == EventType.ITERATION_END
    assert "--dangerously-bypass-hook-trust" in captured["args"]
    env = captured["env"]
    assert env["WORKFLOW_CONTROLLER_HOOK_SERVER_URL"] == "http://127.0.0.1:18765"


@pytest.mark.asyncio
async def test_query_rejects_changed_hooks_before_spawn(tmp_path, monkeypatch):
    writer = CodexHookConfigWriter()
    writer.prepare(tmp_path)
    hooks_file = tmp_path / ".codex" / "hooks.json"
    hooks_file.write_text("{}", encoding="utf-8")

    def fake_spawn(*args, **kwargs):
        raise AssertionError("changed hooks must fail before spawn")

    events = await _query_with_managed_hooks(monkeypatch, tmp_path, writer, fake_spawn)

    assert events[-1].data["reason"] == "rejected_params"
    assert "managed" in events[1].data["error"]


def test_codex_proc_env_drops_host_provider_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
    monkeypatch.setenv("ANTHROPIC_MODEL", "claude-sonnet-4")
    monkeypatch.setenv("OPENCODE_CONFIG_CONTENT", "{}")
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "claude-oauth-secret")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-secret")
    monkeypatch.setenv("CODEX_HOME", "/tmp/codex-home")

    env = getattr(codex_cli, "_codex_proc_env")(
        _QUERY_ENV,
        cwd=str(tmp_path),
    )

    assert "ANTHROPIC_API_KEY" not in env
    assert "ANTHROPIC_MODEL" not in env
    assert "OPENCODE_CONFIG_CONTENT" not in env
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in env
    assert "OPENAI_API_KEY" not in env
    assert env["WORKFLOW_CONTROLLER_CODEX_API_KEY"] == "secret"
    assert env["CODEX_HOME"] == str(tmp_path / ".codex")
