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

import json

import pytest

from workflow_controller.driver.hooks import DriverHooks
from workflow_controller.driver.opencode import (
    OpenCodeDriver,
    _build_args,
    _dispatch_hook,
    _extract_line_session_id,
    _extract_subagent_content,
    _extract_subagent_session_id,
    _opencode_line_to_event,
    _poll_subagent_stream,
    build_restricted_agent_config,
)
from workflow_controller.driver.opencode_gate import PerQueryGate, build_config_content
from workflow_controller.session.events import EventType


@pytest.mark.asyncio
async def test_missing_cpx_reports_wrapper_command_and_remedy(tmp_path, monkeypatch):
    async def fail_spawn(*args, **kwargs):
        raise FileNotFoundError("cpx")

    monkeypatch.setattr(
        "workflow_controller.driver.opencode.asyncio.create_subprocess_exec", fail_spawn
    )
    events = []
    async for event in OpenCodeDriver(
        opencode_bin="opencode-that-is-not-installed",
        command_prefix=["cpx"],
    ).query("prompt", session_id="sid", cwd=str(tmp_path)):
        events.append(event)

    error_events = [event for event in events if event.event_type == EventType.ERROR]
    assert len(error_events) == 1
    assert error_events[0].data["error"].startswith("command not found: cpx;")
    assert "install cpx" in error_events[0].data["error"]


@pytest.mark.asyncio
async def test_unexecutable_command_reports_execute_error(tmp_path, monkeypatch):
    async def fail_spawn(*args, **kwargs):
        raise PermissionError("Permission denied")

    monkeypatch.setattr(
        "workflow_controller.driver.opencode.asyncio.create_subprocess_exec", fail_spawn
    )
    events = []
    async for event in OpenCodeDriver(
        opencode_bin="opencode",
        command_prefix=["cpx"],
    ).query("prompt", session_id="sid", cwd=str(tmp_path)):
        events.append(event)

    error_events = [event for event in events if event.event_type == EventType.ERROR]
    assert len(error_events) == 1
    assert error_events[0].data["error"].startswith("failed to execute cpx:")
    assert "command not found" not in error_events[0].data["error"]


@pytest.mark.asyncio
async def test_spawn_failure_emits_iteration_end(tmp_path, monkeypatch):
    """异常路径收尾（docs/event-system.md）：spawn 失败先 ERROR 再 ITERATION_END，
    事件流消费者依赖 iteration_end 判定 query 边界。
    """

    async def fail_spawn(*args, **kwargs):
        raise FileNotFoundError("opencode")

    monkeypatch.setattr(
        "workflow_controller.driver.opencode.asyncio.create_subprocess_exec", fail_spawn
    )
    events = []
    async for event in OpenCodeDriver(
        opencode_bin="opencode-that-is-not-installed",
    ).query("prompt", session_id="sid", cwd=str(tmp_path)):
        events.append(event)

    # 首条是 USER_MESSAGE（prompt 回显），异常路径收尾在其后
    assert [e.event_type for e in events][-2:] == [EventType.ERROR, EventType.ITERATION_END]
    assert events[-1].data["reason"] == "internal_error"


@pytest.mark.asyncio
async def test_internal_error_emits_iteration_end(tmp_path, monkeypatch):
    """主循环 except Exception：ERROR 后补 ITERATION_END（不进 finally）。"""
    import asyncio
    from types import SimpleNamespace

    import workflow_controller.driver.opencode as oc

    class _FakeProc:
        def __init__(self) -> None:
            self.pid = 424242
            self.returncode = 0
            self.stdin = SimpleNamespace(close=lambda: None)
            self.stdout = SimpleNamespace(read=self._read)
            self.stderr = self
            self._lines = [b'{"type":"text"}\n']

        def __aiter__(self):
            return self

        async def __anext__(self) -> bytes:
            raise StopAsyncIteration

        async def wait(self) -> int:
            return self.returncode

        async def _read(self, _n: int) -> bytes:
            return self._lines.pop(0) if self._lines else b""

    monkeypatch.setattr(oc, "_detect_run_flags", lambda _bin: frozenset())

    async def _fake_spawn(*_args, **_kwargs):
        return _FakeProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_spawn)

    def _raise_parse(*_args, **_kwargs):
        raise RuntimeError("line parse blew up")

    monkeypatch.setattr(oc, "_opencode_line_to_event", _raise_parse)

    driver = OpenCodeDriver()
    events = [event async for event in driver.query("hi", session_id="sid", cwd=str(tmp_path))]
    await asyncio.sleep(0)

    types = [e.event_type for e in events]
    assert types[-2:] == [EventType.ERROR, EventType.ITERATION_END]
    assert "opencode driver internal error" in events[-2].data["error"]
    assert events[-1].data["reason"] == "internal_error"


@pytest.mark.asyncio
async def test_early_internal_error_emits_iteration_end(tmp_path, monkeypatch):
    """异常发生在 poller 声明之前（首个 assert 失败）：except 补发
    ERROR+ITERATION_END 后，finally 不得因 poller 未绑定抛
    UnboundLocalError 污染错误流。
    """
    import asyncio
    from types import SimpleNamespace

    import workflow_controller.driver.opencode as oc

    class _NoStdinProc:
        def __init__(self) -> None:
            self.pid = 424242
            self.returncode = 0
            self.stdin = None  # 触发 try 首行的 assert
            self.stdout = SimpleNamespace()
            self.stderr = self

    monkeypatch.setattr(oc, "_detect_run_flags", lambda _bin: frozenset())

    async def _fake_spawn(*_args, **_kwargs):
        return _NoStdinProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_spawn)

    driver = OpenCodeDriver()
    events = [event async for event in driver.query("hi", session_id="sid", cwd=str(tmp_path))]

    types = [e.event_type for e in events]
    assert types[-2:] == [EventType.ERROR, EventType.ITERATION_END]
    assert events[-1].data["reason"] == "internal_error"


class TestOpenCodeLineToEvent:
    """opencode `run --format json` line -> Workflow Controller Event(s).

    Event shapes here are captured first-hand from opencode 1.17.11 + GLM
    (see docs/design/opencode-runtime-feasibility.md Stage 1 spike).
    """

    @staticmethod
    def test_text():
        line = json.dumps(
            {
                "type": "text",
                "timestamp": 1,
                "sessionID": "ses_x",
                "part": {
                    "id": "prt_1",
                    "messageID": "msg_1",
                    "sessionID": "ses_x",
                    "type": "text",
                    "text": "ok",
                },
            }
        )
        events = _opencode_line_to_event(line, "sid", 1)
        assert len(events) == 1
        ev = events[0]
        assert ev.event_type == EventType.ASSISTANT_MESSAGE
        assert ev.data["content"][0] == {"type": "text", "text": "ok"}
        assert ev.data["model"] == ""

    @staticmethod
    def test_tool_use_emits_call_result_and_tool_call():
        # opencode 把 call + result 合在一个事件里，Workflow Controller 拆成 TOOL_USE +
        # TOOL_RESULT + TOOL_CALL。TOOL_RESULT.data.tool_name 取自 part.tool。
        line = json.dumps(
            {
                "type": "tool_use",
                "sessionID": "ses_x",
                "part": {
                    "id": "prt_2",
                    "type": "tool",
                    "tool": "bash",
                    "state": {
                        "status": "completed",
                        "input": {"command": "ls"},
                        "output": "a\nb",
                        "metadata": {"exit": 0},
                    },
                },
            }
        )
        events = _opencode_line_to_event(line, "sid", 1)
        assert len(events) == 3
        call, result, tool_call = events
        assert call.event_type == EventType.TOOL_USE
        assert call.data == {"id": "prt_2", "name": "bash", "input": {"command": "ls"}}
        assert result.event_type == EventType.TOOL_RESULT
        assert result.data == {
            "tool_use_id": "prt_2",
            "tool_name": "bash",
            "content": "a\nb",
            "is_error": False,
        }
        assert tool_call.event_type == EventType.TOOL_CALL
        assert tool_call.data == {
            "name": "bash",
            "arg": "ls",
            "tool_use_id": "prt_2",
            "is_error": False,
        }

    @staticmethod
    def test_tool_use_nonzero_exit_is_error():
        line = json.dumps(
            {
                "type": "tool_use",
                "part": {
                    "id": "prt_3",
                    "type": "tool",
                    "tool": "bash",
                    "state": {
                        "status": "completed",
                        "input": {"command": "bad"},
                        "output": "boom",
                        "metadata": {"exit": 127},
                    },
                },
            }
        )
        events = _opencode_line_to_event(line, "sid", 1)
        result = events[1]
        assert result.data["is_error"] is True
        # error 透传到 TOOL_CALL.error（extract_tool_error 取原始 output）
        tool_call = events[2]
        assert tool_call.data["is_error"] is True
        assert tool_call.data["error"] == "boom"

    @staticmethod
    def test_step_finish_tool_calls_reason_not_error():
        # "tool-calls" 步进表示 worker 调了工具（如 gate），属正常推进；
        # 仅 reason 取 "error" 才按错误处理。
        line = json.dumps(
            {"type": "step_finish", "part": {"reason": "tool-calls", "tokens": {}, "cost": 0}}
        )
        events = _opencode_line_to_event(line, "sid", 1)
        assert events[0].data["is_error"] is False

    @staticmethod
    def test_error():
        line = json.dumps(
            {
                "type": "error",
                "sessionID": "ses_x",
                "error": {"name": "ProviderAuthError", "data": {"message": "key missing"}},
            }
        )
        events = _opencode_line_to_event(line, "sid", 1)
        assert len(events) == 1
        assert events[0].event_type == EventType.ERROR

    @staticmethod
    def test_error_401_emits_rate_limit_signal_then_error():
        """401 → RATE_LIMIT (auth_error signal) BEFORE the ERROR log.
        Driver owns the statusCode→signal translation; detector consumes one
        event type. raw preserves the native error dict for observability.
        """
        line = json.dumps(
            {
                "type": "error",
                "sessionID": "ses_x",
                "error": {
                    "name": "APIError",
                    "data": {"statusCode": 401, "message": "Unauthorized"},
                },
            }
        )
        events = _opencode_line_to_event(line, "sid", 1)
        assert len(events) == 2
        sig_ev, err_ev = events
        assert sig_ev.event_type == EventType.RATE_LIMIT
        assert sig_ev.data["limit_signal"]["kind"] == "auth_error"
        assert sig_ev.data["limit_signal"]["scope"] == "auth_error"
        assert sig_ev.data["limit_signal"]["source"] == "opencode:APIError:401"
        assert sig_ev.data["raw"]["data"]["statusCode"] == 401
        assert err_ev.event_type == EventType.ERROR
        assert err_ev.data["error"]["data"]["statusCode"] == 401

    @staticmethod
    def test_error_429_emits_rejected_rate_limit_signal():
        line = json.dumps(
            {
                "type": "error",
                "sessionID": "ses_x",
                "error": {
                    "name": "APIError",
                    "data": {"statusCode": 429, "message": "Too Many Requests"},
                },
            }
        )
        events = _opencode_line_to_event(line, "sid", 1)
        assert len(events) == 2
        sig_ev, err_ev = events
        assert sig_ev.event_type == EventType.RATE_LIMIT
        assert sig_ev.data["limit_signal"]["kind"] == "rejected"
        assert sig_ev.data["limit_signal"]["scope"] == "5h"
        assert sig_ev.data["limit_signal"]["reset_at"] is None
        assert err_ev.event_type == EventType.ERROR

    @staticmethod
    def test_error_500_emits_only_error():
        """Non-limit server errors (500) emit only ERROR — no spurious signal.
        The detector does not act on ERROR (observability-only).
        """
        line = json.dumps(
            {
                "type": "error",
                "sessionID": "ses_x",
                "error": {"name": "APIError", "data": {"statusCode": 500, "message": "boom"}},
            }
        )
        events = _opencode_line_to_event(line, "sid", 1)
        assert len(events) == 1
        assert events[0].event_type == EventType.ERROR

    @staticmethod
    def test_step_start_skipped():
        line = json.dumps({"type": "step_start", "part": {"id": "p", "type": "step-start"}})
        assert _opencode_line_to_event(line, "sid", 1) == []

    @staticmethod
    def test_unknown_type_skipped():
        line = json.dumps({"type": "something_else", "part": {}})
        assert _opencode_line_to_event(line, "sid", 1) == []

    @staticmethod
    def test_invalid_json_skipped():
        assert _opencode_line_to_event("not json {{{", "sid", 1) == []

    @staticmethod
    def test_non_dict_root_skipped():
        assert _opencode_line_to_event("[1,2,3]", "sid", 1) == []


class TestOpenCodeBuildArgs:
    @staticmethod
    def test_basic():
        # run_flags 置空模拟「探测失败/旧版」：回退保守旗标面
        args = _build_args(
            bin_path="opencode",
            prompt="hello",
            model="",
            cwd=None,
            resume=None,
            permission_mode="bypassPermissions",
            run_flags=frozenset(),
        )
        assert args[:4] == ["opencode", "run", "--format", "json"]
        assert "--dangerously-skip-permissions" in args
        assert "--standalone" not in args
        assert args[-1] == "hello"
        # bare model name is NOT passed (let opencode.json default govern)
        assert "--model" not in args and "-m" not in args

    @staticmethod
    def test_v2_flags():
        """v2 旗标面：standalone 私有服务 + --auto 审批（显式 deny 保强制）。"""
        args = _build_args(
            bin_path="opencode",
            prompt="hello",
            model="",
            cwd=None,
            resume=None,
            permission_mode="bypassPermissions",
            run_flags=frozenset({"--standalone", "--auto"}),
        )
        assert "--standalone" in args
        assert "--auto" in args
        assert "--dangerously-skip-permissions" not in args

    @staticmethod
    def test_command_prefix():
        """验证 OpenCode 命令保留 wrapper 前缀。"""
        args = _build_args(
            bin_path="opencode",
            prompt="p",
            model="",
            cwd=None,
            resume=None,
            permission_mode="bypassPermissions",
            run_flags=frozenset(),
            command_prefix=["cpx"],
        )
        assert args[:2] == ["cpx", "opencode"]

    @staticmethod
    def test_cwd_not_in_args():
        """工作目录经子进程 cwd 传入（spawn 处），argv 不含 --dir——opencode
        v2 的 run 子命令已移除该旗标，传入直接拒绝执行。
        """
        args = _build_args(
            bin_path="opencode",
            prompt="p",
            model="",
            cwd="/tmp/x",
            resume=None,
            permission_mode="bypassPermissions",
            run_flags=frozenset(),
        )
        assert "--dir" not in args

    @staticmethod
    def test_compose_proc_env_rewrites_pwd(monkeypatch):
        """PWD 随 cwd 重写：opencode 按 PWD 做项目发现，陈旧 PWD 会让
        成员在错误目录找 .opencode/agents（Agent not found）。
        """
        from workflow_controller.driver.opencode import _compose_proc_env

        monkeypatch.setenv("PWD", "/definitely/not/the/cwd")
        merged = _compose_proc_env({"CUSTOM": "1"}, "/tmp/the-cwd")
        assert merged["PWD"] == "/tmp/the-cwd"
        assert merged["CUSTOM"] == "1"

        no_cwd = _compose_proc_env(None, None)
        assert no_cwd.get("PWD") == "/definitely/not/the/cwd"  # 未指定 cwd 不改写

    @staticmethod
    def test_resume():
        args = _build_args(
            bin_path="opencode",
            prompt="p",
            model="",
            cwd=None,
            resume="ses_abc",
            permission_mode="bypassPermissions",
            run_flags=frozenset(),
        )
        assert "--session" in args
        assert "ses_abc" in args

    @staticmethod
    def test_provider_qualified_model_passed():
        args = _build_args(
            bin_path="opencode",
            prompt="p",
            model="zai-anthropic/glm-5.2",
            cwd=None,
            resume=None,
            permission_mode="bypassPermissions",
            run_flags=frozenset(),
        )
        assert "-m" in args
        assert "zai-anthropic/glm-5.2" in args

    @staticmethod
    def test_bare_model_not_passed():
        # 形如 ANTHROPIC_MODEL 里带 "[1m]" 后缀的裸模型名会被 opencode 拒绝，
        # 此时应跳过并回落默认模型。
        args = _build_args(
            bin_path="opencode",
            prompt="p",
            model="glm-5.2[1m]",
            cwd=None,
            resume=None,
            permission_mode="bypassPermissions",
            run_flags=frozenset(),
        )
        assert "-m" not in args

    @staticmethod
    def test_restricted_adds_agent_flag():
        # a read-only caller (reviewer) selects the restricted agent
        args = _build_args(
            bin_path="opencode",
            prompt="p",
            model="",
            cwd=None,
            resume=None,
            permission_mode="bypassPermissions",
            restricted=True,
            run_flags=frozenset(),
        )
        assert "--agent" in args
        assert "workflow-controller-restricted" in args

    @staticmethod
    def test_unrestricted_omits_agent_flag():
        # default (worker) keeps the full default agent
        args = _build_args(
            bin_path="opencode",
            prompt="p",
            model="",
            cwd=None,
            resume=None,
            permission_mode="bypassPermissions",
            run_flags=frozenset(),
        )
        assert "--agent" not in args

    @staticmethod
    def test_system_prompt_adds_worker_agent_flag():
        # a worker whose system_prompt was injected selects the worker agent
        # (mirrors how the restricted reviewer selects workflow-controller-restricted)
        args = _build_args(
            bin_path="opencode",
            prompt="p",
            model="",
            cwd=None,
            resume=None,
            permission_mode="bypassPermissions",
            worker_agent=True,
            run_flags=frozenset(),
        )
        assert "--agent" in args
        assert "workflow-controller-worker" in args

    async def test_query_detects_flags_off_loop_and_spawns_with_cwd(
        self,
        monkeypatch,
        tmp_path,
    ):
        """query 的子进程纪律：旗标探测离事件循环线程、子进程以 cwd 为
        工作目录（项目发现的唯一通道）。探测是同步 subprocess（首查最长
        15s），在 loop 上跑会停摆全部会话/定时器/信号处理。
        """
        import asyncio
        import threading
        from types import SimpleNamespace

        import workflow_controller.driver.opencode as oc

        class _FakeProc:
            """最小假子进程：stdout 立即 EOF、stderr 空流、已退出态。"""

            def __init__(self) -> None:
                self.pid = 424242
                self.returncode = 0
                self.stdin = SimpleNamespace(close=lambda: None)
                self.stdout = SimpleNamespace(read=self._read)
                self.stderr = self

            def __aiter__(self):
                return self

            async def __anext__(self) -> bytes:
                raise StopAsyncIteration

            @staticmethod
            async def _read(_n: int) -> bytes:
                return b""

            async def wait(self) -> int:
                return self.returncode

        detect_threads: list[int] = []

        def _fake_detect(bin_path: str) -> frozenset[str]:
            detect_threads.append(threading.get_ident())
            return frozenset({"--standalone", "--auto"})

        monkeypatch.setattr(oc, "_detect_run_flags", _fake_detect)

        seen_flags: list[frozenset[str]] = []

        def _fake_build_args(*, run_flags: frozenset[str], **_kw: object) -> list[str]:
            seen_flags.append(run_flags)
            return ["opencode", "run"]

        monkeypatch.setattr(oc, "_build_args", _fake_build_args)

        spawns: list[dict] = []

        async def _fake_spawn(*_args: object, **kwargs: object) -> _FakeProc:
            spawns.append(kwargs)
            return _FakeProc()

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_spawn)

        driver = OpenCodeDriver()
        cwd = str(tmp_path / "proj")
        async for _ in driver.query("hi", session_id="s", cwd=cwd):
            pass
        await asyncio.sleep(0)  # 让 stderr 排空任务收尾

        assert detect_threads and detect_threads[0] != threading.get_ident()
        assert seen_flags == [frozenset({"--standalone", "--auto"})]
        assert spawns and spawns[0]["cwd"] == cwd


@pytest.mark.asyncio
async def test_query_stop_with_trailing_task_line_still_fires_on_subagent_stop(
    monkeypatch,
):
    """stop 与输出尾部竞态：EOF 无换行尾行 batch 必须照常过 _dispatch_hook——
    尾行 task tool_use 触发 on_subagent_stop（子代理停靠检查点），stop 置位
    只在批间生效，不丢弃已缓冲行。
    """
    import asyncio
    from types import SimpleNamespace

    import workflow_controller.driver.opencode as oc

    hooks, _starts, stops = _hooks_capturing_subagent()
    task_line = _task_tool_use_line(
        '<task id="ses_sub1" state="completed"><task_result>done</task_result></task>'
    ).encode()
    driver = OpenCodeDriver()

    class _FakeStdout:
        def __init__(self) -> None:
            self._read = False

        async def read(self, _n: int = -1) -> bytes:
            if self._read:
                return b""
            self._read = True
            await driver.stop()
            return task_line  # 无换行：落在 EOF 尾行 batch

    class _FakeProc:
        def __init__(self) -> None:
            self.pid = 424243
            self.returncode = -9
            self.stdin = SimpleNamespace(close=lambda: None)
            self.stdout = _FakeStdout()
            self.stderr = self

        def __aiter__(self):
            return self

        async def __anext__(self) -> bytes:
            raise StopAsyncIteration

        async def wait(self) -> int:
            return self.returncode

    monkeypatch.setattr(oc, "_detect_run_flags", lambda _bin: frozenset())

    async def _fake_spawn(*_args: object, **_kwargs: object) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_spawn)

    events = []
    async for event in driver.query("hi", session_id="sid", hooks=hooks):
        events.append(event)
    await asyncio.sleep(0)  # 让 stderr 排空任务收尾

    assert len(stops) == 1
    assert stops[0][1]["agent_id"] == "ses_sub1"
    assert events[-1].event_type == EventType.ITERATION_END


class TestRestrictedAgentConfig:
    """build_restricted_agent_config: allowed_tools (cc PascalCase) -> an opencode
    agent config that denies mutating tool categories the caller did NOT allow.

    opencode's --dangerously-skip-permissions auto-approves anything that is not
    explicitly denied, so restricting the reviewer = explicitly denying the
    mutating categories its allowlist omits (read-only categories are never
    denied). Enforcement of permission.deny under bypass was verified by the
    readonly spike: edit/write/bash = deny blocked a file creation that the
    unrestricted default agent performed.
    """

    @staticmethod
    def test_none_when_no_allowed_tools():
        # no restriction -> default agent, full toolset (the worker case)
        assert build_restricted_agent_config(None) is None
        assert build_restricted_agent_config([]) is None

    @staticmethod
    def test_readonly_allowlist_denies_mutating():
        cfg = build_restricted_agent_config(["Read", "Glob", "Grep"])
        assert cfg is not None
        perm = cfg["agent"]["workflow-controller-restricted"]["permission"]
        # spike-proven blockers must be denied
        assert perm.get("write") == "deny"
        assert perm.get("bash") == "deny"
        assert perm.get("edit") == "deny"
        assert perm.get("task") == "deny"
        assert perm.get("webfetch") == "deny"
        # read-only categories the caller allowed must NOT be denied
        assert "read" not in perm
        assert "glob" not in perm
        assert "grep" not in perm

    @staticmethod
    def test_explicitly_allowed_mutating_not_denied():
        # a caller that allows Bash keeps it; other mutating tools still denied
        cfg = build_restricted_agent_config(["Read", "Bash"])
        perm = cfg["agent"]["workflow-controller-restricted"]["permission"]
        assert "bash" not in perm
        assert perm.get("write") == "deny"


class TestConfigContentRestrictedAgent:
    """build_config_content merges the restricted-agent section (and makes
    gate_url optional — the reviewer restricts tools but has no gate).
    """

    @staticmethod
    def test_restricted_only_no_gate():
        restricted = {
            "agent": {"workflow-controller-restricted": {"permission": {"write": "deny"}}}
        }
        cfg = json.loads(build_config_content(gate_url=None, restricted_agent=restricted))
        assert cfg["agent"]["workflow-controller-restricted"]["permission"]["write"] == "deny"
        assert "mcp" not in cfg  # no gate -> no mcp section
        assert cfg["compaction"]["auto"] is False

    @staticmethod
    def test_gate_and_restricted_merged():
        restricted = {"agent": {"workflow-controller-restricted": {"permission": {"bash": "deny"}}}}
        cfg = json.loads(
            build_config_content(gate_url="http://127.0.0.1:9/mcp", restricted_agent=restricted)
        )
        assert cfg["mcp"]["workflow_controller_gate"]["url"] == "http://127.0.0.1:9/mcp"
        assert cfg["agent"]["workflow-controller-restricted"]["permission"]["bash"] == "deny"
        assert cfg["compaction"]["auto"] is False

    @staticmethod
    def test_gate_only_still_works():
        # existing gate path unchanged
        cfg = json.loads(build_config_content(gate_url="http://127.0.0.1:9/mcp"))
        assert cfg["mcp"]["workflow_controller_gate"]["url"] == "http://127.0.0.1:9/mcp"
        assert "agent" not in cfg


class TestConfigContentSystemPrompt:
    """build_config_content injects the worker's system_prompt as a named
    opencode agent's ``prompt`` (opencode's per-agent system instructions) —
    mirroring how the restricted reviewer agent is injected, and selected at
    run via ``--agent workflow-controller-worker``. opencode otherwise has no system-prompt
    CLI flag, so the agent-config channel is the only way to honor it.
    """

    @staticmethod
    def test_system_prompt_adds_worker_agent():
        cfg = json.loads(build_config_content(system_prompt="be disciplined; no placeholders"))
        assert (
            cfg["agent"]["workflow-controller-worker"]["prompt"]
            == "be disciplined; no placeholders"
        )
        assert cfg["compaction"]["auto"] is False

    @staticmethod
    def test_system_prompt_and_restricted_coexist():
        # worker system_prompt + reviewer restricted agent must not overwrite
        # each other — both named agents land in the same agent map
        restricted = {
            "agent": {"workflow-controller-restricted": {"permission": {"write": "deny"}}}
        }
        cfg = json.loads(
            build_config_content(
                gate_url="http://127.0.0.1:9/mcp",
                restricted_agent=restricted,
                system_prompt="be disciplined",
            )
        )
        assert cfg["agent"]["workflow-controller-worker"]["prompt"] == "be disciplined"
        assert cfg["agent"]["workflow-controller-restricted"]["permission"]["write"] == "deny"
        assert cfg["mcp"]["workflow_controller_gate"]["url"] == "http://127.0.0.1:9/mcp"


def _hooks_capturing_stop() -> tuple[DriverHooks, list]:
    """A DriverHooks whose on_stop records (sid, data) for assertions."""
    captured: list = []

    async def on_stop(sid: str, data: dict) -> None:
        captured.append((sid, data))

    return DriverHooks(session_id="sid", on_stop=on_stop), captured


def _hooks_capturing_subagent() -> tuple[DriverHooks, list, list]:
    """DriverHooks whose on_subagent_start/stop record (sid, data) calls."""
    starts: list = []
    stops: list = []

    async def on_start(sid: str, data: dict) -> None:
        starts.append((sid, data))

    async def on_stop_sub(sid: str, data: dict) -> None:
        stops.append((sid, data))

    return (
        DriverHooks(
            session_id="sid",
            on_subagent_start=on_start,
            on_subagent_stop=on_stop_sub,
        ),
        starts,
        stops,
    )


def _task_tool_use_line(output: str) -> str:
    """构造 task 工具的 tool_use JSONL 行（researcher 子代理、给定 output）。"""
    return json.dumps(
        {
            "type": "tool_use",
            "part": {
                "id": "t1",
                "tool": "task",
                "state": {"input": {"subagent_type": "researcher"}, "output": output},
            },
        }
    )


class TestDispatchHook:
    """_dispatch_hook: per-line DriverHooks signaling (completion path).

    opencode events arrive post-completion, so only the on_stop side-effect
    (mark_reviewer_let_exit via the orchestrator) matters; the return value is
    ignored. _dispatch_hook returns the updated last_assistant_text.
    """

    async def test_text_accumulates_and_no_stop(self):
        hooks, captured = _hooks_capturing_stop()
        line = json.dumps({"type": "text", "part": {"text": "hello "}})
        out = await _dispatch_hook(line, hooks, "")
        assert out == "hello "
        assert captured == []

    async def test_step_finish_stop_calls_on_stop(self):
        hooks, captured = _hooks_capturing_stop()
        line = json.dumps({"type": "step_finish", "part": {"reason": "stop"}})
        out = await _dispatch_hook(line, hooks, "final text")
        assert out == "final text"  # unchanged
        assert captured == [("sid", {"last_assistant_message": "final text"})]

    async def test_step_finish_end_turn_also_calls_on_stop(self):
        hooks, captured = _hooks_capturing_stop()
        line = json.dumps({"type": "step_finish", "part": {"reason": "end_turn"}})
        await _dispatch_hook(line, hooks, "done")
        assert len(captured) == 1

    async def test_step_finish_non_stop_no_call(self):
        hooks, captured = _hooks_capturing_stop()
        line = json.dumps({"type": "step_finish", "part": {"reason": "error"}})
        await _dispatch_hook(line, hooks, "x")
        assert captured == []

    async def test_no_hooks_safe_and_text_still_accumulates(self):
        line = json.dumps({"type": "text", "part": {"text": "hi"}})
        out = await _dispatch_hook(line, None, "")
        assert out == "hi"

    async def test_invalid_json_skipped(self):
        out = await _dispatch_hook("not json {{{", None, "kept")
        assert out == "kept"

    async def test_non_dict_skipped(self):
        out = await _dispatch_hook("[1,2,3]", None, "kept")
        assert out == "kept"

    async def test_accumulation_across_lines(self):
        hooks, captured = _hooks_capturing_stop()
        text1 = await _dispatch_hook(json.dumps({"type": "text", "part": {"text": "a"}}), hooks, "")
        text2 = await _dispatch_hook(
            json.dumps({"type": "text", "part": {"text": "b"}}), hooks, text1
        )
        assert text2 == "ab"
        await _dispatch_hook(
            json.dumps({"type": "step_finish", "part": {"reason": "stop"}}), hooks, text2
        )
        assert captured == [("sid", {"last_assistant_message": "ab"})]

    # ── task 工具 = subagent（opencode 接 subagent 语义）──

    async def test_task_tool_use_only_emits_stop(self):
        """The stdout task line is the stop source only; start is the poller's
        job. Asserting ``starts`` stays empty guards the no-belated-fake-start
        contract.
        """
        hooks, starts, stops = _hooks_capturing_subagent()
        line = json.dumps(
            {
                "type": "tool_use",
                "part": {
                    "id": "t1",
                    "tool": "task",
                    "state": {
                        "input": {"subagent_type": "researcher", "description": "x", "prompt": "y"},
                        "output": "done",
                    },
                },
            }
        )
        await _dispatch_hook(line, hooks, "")
        # start is NOT fired here — the poller owns it; no belated fake start.
        assert starts == []
        # stop carries the subagent's return text inline (state.output, envelope
        # stripped) so the orchestrator surfaces it in the SUBAGENT event — same
        # Dashboard parity as cc's transcript-derived content. "done" has no
        # envelope → no sub-session id → total_tokens 0, messages [] (no db read).
        assert stops == [
            (
                "sid",
                {
                    "agent_id": "t1",
                    "agent_type": "researcher",
                    "content": "done",
                    "total_tokens": 0,
                    "messages": [],
                },
            )
        ]

    async def test_task_error_before_subagent_creation_has_no_stop(self):
        hooks, starts, stops = _hooks_capturing_subagent()
        line = json.dumps(
            {
                "type": "tool_use",
                "part": {
                    "id": "prt_failed",
                    "tool": "task",
                    "state": {
                        "status": "error",
                        "input": {"subagent_type": "reader", "prompt": "read articles"},
                        "error": "Unknown agent type: reader is not a valid agent type",
                    },
                },
            }
        )
        events = _opencode_line_to_event(line, "sid", 1)
        assert events[-1].data["is_error"] is True
        assert events[-1].data["error"] == "Unknown agent type: reader is not a valid agent type"
        await _dispatch_hook(line, hooks, "")
        assert starts == []
        assert stops == []

    async def test_task_stop_passes_subagent_tokens(self, monkeypatch):
        """With a real envelope, _dispatch_hook extracts the sub-session id and
        looks up its token total, passing both content and tokens inline.
        """
        from workflow_controller.driver import opencode as oc_module

        hooks, _starts, stops = _hooks_capturing_subagent()
        envelope = (
            '<task id="ses_abc123" state="completed">\n'
            "<task_result>\nthe report\n</task_result>\n</task>"
        )
        line = _task_tool_use_line(envelope)
        # hermetic: don't touch the host's real opencode.db
        monkeypatch.setattr(
            oc_module,
            "lookup_subagent_output_tokens",
            lambda sid, env=None: 4356,
        )
        await _dispatch_hook(line, hooks, "")
        assert stops[0][1]["content"] == "the report"
        assert stops[0][1]["total_tokens"] == 4356

    async def test_task_stop_truncates_long_content(self):
        """Boundary normalization: _dispatch_hook caps the subagent's return
        text at 2000 chars before forwarding, so SubagentTracker.on_stop (pure
        consumer) receives already-truncated content — same cap cc's
        summarize_subagent_transcript applies.
        """
        hooks, _starts, stops = _hooks_capturing_subagent()
        line = _task_tool_use_line("z" * 5000)
        await _dispatch_hook(line, hooks, "")
        content = stops[0][1]["content"]
        assert content.startswith("z")
        assert len(content) <= 2001  # 2000 + ellipsis

    async def test_task_stop_forwards_proc_env_to_db_lookups(self, monkeypatch):
        """The env the opencode subprocess ran with is forwarded to the token +
        message db lookups, so a relocated data dir (custom XDG_DATA_HOME / HOME
        via the query env override) resolves the same db opencode wrote — not the
        host's default. Without this, the best-effort reads would silently miss
        and the subagent context panel come up empty.
        """
        from workflow_controller.driver import opencode as oc_module

        seen: dict[str, object] = {}

        def fake_tokens(sid, env=None):
            seen["tokens_env"] = env
            return 100

        def fake_messages(sid, env=None, max_parts=200):
            seen["messages_env"] = env
            return [{"type": "text", "text": "hi"}]

        monkeypatch.setattr(oc_module, "lookup_subagent_output_tokens", fake_tokens)
        monkeypatch.setattr(oc_module, "read_subagent_messages", fake_messages)

        hooks, _starts, stops = _hooks_capturing_subagent()
        envelope = (
            '<task id="ses_env_fwd" state="completed">\n<task_result>\nrep\n</task_result>\n</task>'
        )
        line = _task_tool_use_line(envelope)
        custom_env = {"XDG_DATA_HOME": "/custom/xdg", "HOME": "/custom/home"}
        await _dispatch_hook(line, hooks, "", env=custom_env)
        # both lookups received the SAME env opencode ran with
        assert seen["tokens_env"] is custom_env
        assert seen["messages_env"] is custom_env
        # and the messages flow through to the stop payload
        assert stops[0][1]["messages"] == [{"type": "text", "text": "hi"}]

    async def test_task_stop_defaults_env_to_none_safe(self, monkeypatch):
        """Omitting env (old 3-arg call sites) stays safe: lookups get None and
        fall back to os.environ, matching the pre-fix behavior.
        """
        from workflow_controller.driver import opencode as oc_module

        seen: dict[str, object] = {}
        monkeypatch.setattr(
            oc_module,
            "lookup_subagent_output_tokens",
            lambda sid, env=None: (seen.setdefault("env", env), 0)[1],
        )
        monkeypatch.setattr(
            oc_module,
            "read_subagent_messages",
            lambda sid, env=None, max_parts=200: (seen.setdefault("menv", env), [])[1],
        )
        hooks, _starts, _stops = _hooks_capturing_subagent()
        envelope = '<task id="ses_noenv" state="completed"><task_result>x</task_result></task>'
        line = _task_tool_use_line(envelope)
        await _dispatch_hook(line, hooks, "")  # no env → backward compatible
        assert seen["env"] is None
        assert seen["menv"] is None

    async def test_task_stop_content_strips_opencode_envelope(self):
        """Real opencode wraps the subagent text in <task ...><task_result>…;
        the driver surfaces only the inner text. Uses a fake sub-session id so
        the best-effort token lookup (which still runs) stays hermetic.
        """
        hooks, _starts, stops = _hooks_capturing_subagent()
        envelope = (
            '<task id="ses_notreal_test" state="completed">\n'
            "<task_result>\nthe real report\n</task_result>\n</task>"
        )
        line = _task_tool_use_line(envelope)
        await _dispatch_hook(line, hooks, "")
        assert stops[0][1]["content"] == "the real report"

    async def test_non_task_tool_use_does_not_trigger_subagent_hooks(self):
        hooks, starts, stops = _hooks_capturing_subagent()
        line = json.dumps(
            {
                "type": "tool_use",
                "part": {
                    "id": "t2",
                    "tool": "read",
                    "state": {"input": {}, "output": "x"},
                },
            }
        )
        await _dispatch_hook(line, hooks, "")
        assert starts == [] and stops == []

    async def test_task_missing_subagent_type_no_crash(self):
        hooks, starts, stops = _hooks_capturing_subagent()
        line = json.dumps(
            {
                "type": "tool_use",
                "part": {
                    "id": "t3",
                    "tool": "task",
                    "state": {"input": {}, "output": "x"},
                },
            }
        )
        await _dispatch_hook(line, hooks, "")
        # start never fires on the stdout path; stop carries the defaulted
        # (empty) agent_type through without crashing.
        assert starts == []
        assert stops == [
            (
                "sid",
                {
                    "agent_id": "t3",
                    "agent_type": "",
                    "content": "x",
                    "total_tokens": 0,
                    "messages": [],
                },
            )
        ]

    async def test_task_no_hooks_no_crash(self):
        line = json.dumps(
            {
                "type": "tool_use",
                "part": {
                    "id": "t4",
                    "tool": "task",
                    "state": {"input": {"subagent_type": "x"}, "output": "y"},
                },
            }
        )
        await _dispatch_hook(line, None, "")  # hooks=None 不崩

    # ── streaming: stdout path emits stop only (start is the poller's job) ──

    async def test_stdout_task_line_never_fires_start(self, monkeypatch):
        """The stdout task line is the stop source only — it must NEVER fire
        on_subagent_start, even when the poller has already started the
        sub-session. The poller owns the real-time start+progress lifecycle;
        without this guard the panel would show two START lines (one from the
        poller, one from stdout).
        """
        from workflow_controller.driver import opencode as oc_module

        monkeypatch.setattr(oc_module, "lookup_subagent_output_tokens", lambda sid, env=None: 0)
        monkeypatch.setattr(
            oc_module, "read_subagent_messages", lambda sid, env=None, max_parts=200: []
        )

        hooks, starts, stops = _hooks_capturing_subagent()
        envelope = (
            '<task id="ses_pol_started" state="completed"><task_result>r</task_result></task>'
        )
        line = json.dumps(
            {
                "type": "tool_use",
                "part": {
                    "id": "t9",
                    "tool": "task",
                    "state": {"input": {"subagent_type": "explore"}, "output": envelope},
                },
            }
        )
        await _dispatch_hook(line, hooks, "")
        # start is NOT fired on the stdout path (poller owns it)
        assert starts == []
        # stop WAS fired, with agent_id = sub_session_id (not tool part id)
        assert len(stops) == 1
        assert stops[0][1]["agent_id"] == "ses_pol_started"

    async def test_stdout_does_not_fire_belated_start_when_poller_missed(self, monkeypatch):
        """No belated fake start: when the poller is absent or hadn't
        discovered the sub-session yet (so no real start was ever fired),
        the stdout line still only emits stop — it does NOT synthesize a
        late start. agent_id falls back to sub_session_id when available.
        """
        from workflow_controller.driver import opencode as oc_module

        monkeypatch.setattr(oc_module, "lookup_subagent_output_tokens", lambda sid, env=None: 0)
        monkeypatch.setattr(
            oc_module, "read_subagent_messages", lambda sid, env=None, max_parts=200: []
        )

        hooks, starts, stops = _hooks_capturing_subagent()
        envelope = '<task id="ses_fallback" state="completed"><task_result>r</task_result></task>'
        line = json.dumps(
            {
                "type": "tool_use",
                "part": {
                    "id": "t10",
                    "tool": "task",
                    "state": {"input": {"subagent_type": "explore"}, "output": envelope},
                },
            }
        )
        await _dispatch_hook(line, hooks, "")
        # no fake start is synthesized even though the poller never saw it
        assert starts == []
        assert stops[0][1]["agent_id"] == "ses_fallback"

    async def test_task_agent_id_falls_back_to_part_id_without_envelope(self, monkeypatch):
        """When there's no envelope (no sub_session_id), agent_id falls back
        to the tool part id on the stop event — backward compatible with the
        pre-streaming tests.
        """
        from workflow_controller.driver import opencode as oc_module

        monkeypatch.setattr(oc_module, "lookup_subagent_output_tokens", lambda sid, env=None: 0)
        monkeypatch.setattr(
            oc_module, "read_subagent_messages", lambda sid, env=None, max_parts=200: []
        )

        hooks, _starts, stops = _hooks_capturing_subagent()
        line = json.dumps(
            {
                "type": "tool_use",
                "part": {
                    "id": "t11",
                    "tool": "task",
                    "state": {"input": {"subagent_type": "x"}, "output": "plain"},
                },
            }
        )
        await _dispatch_hook(line, hooks, "")
        assert stops[0][1]["agent_id"] == "t11"


class TestExtractLineSessionId:
    """_extract_line_session_id: pull sessionID from an opencode json line.
    The poller needs the main session id to exclude it from subagent discovery.
    """

    @staticmethod
    def test_extracts_session_id():
        line = json.dumps({"type": "text", "sessionID": "ses_main", "part": {"text": "hi"}})
        assert _extract_line_session_id(line) == "ses_main"

    @staticmethod
    def test_missing_session_id_returns_empty():
        line = json.dumps({"type": "text", "part": {"text": "hi"}})
        assert _extract_line_session_id(line) == ""

    @staticmethod
    def test_invalid_json_returns_empty():
        assert _extract_line_session_id("not json {{{") == ""

    @staticmethod
    def test_non_dict_returns_empty():
        assert _extract_line_session_id("[1,2,3]") == ""


def _hooks_capturing_start_progress() -> tuple[DriverHooks, list, list]:
    """DriverHooks whose on_subagent_start/progress record (sid, data) calls."""
    starts: list = []
    progress: list = []

    async def on_start(sid: str, data: dict) -> None:
        starts.append((sid, data))

    async def on_progress(sid: str, data: dict) -> None:
        progress.append((sid, data))

    return (
        DriverHooks(
            session_id="sid",
            on_subagent_start=on_start,
            on_subagent_progress=on_progress,
        ),
        starts,
        progress,
    )


def _subagent_start_stop_hooks(on_start, on_stop) -> DriverHooks:
    """孙事件测试通用 hooks：给定 start/stop 回调，progress 用空协程。"""
    return DriverHooks(
        session_id="sid",
        on_subagent_start=on_start,
        on_subagent_progress=lambda *a, **k: _noop_coro(),
        on_subagent_stop=on_stop,
    )


def _task_part_row(content: str) -> tuple:
    """一组 task 工具的 (part_id, parts) 行：tool_use + 指定 content 的 tool_result。"""
    return (
        "p_task",
        [
            {"type": "tool_use", "name": "task", "id": "call_1", "input": {}},
            {
                "type": "tool_result",
                "tool_use_id": "call_1",
                "content": content,
                "is_error": False,
            },
        ],
    )


def _subagent_stop_to_thread(tokens: int):
    """构造按 oc_module 读函数分流的 fake asyncio.to_thread（孙 stop payload 用）。"""
    from workflow_controller.driver import opencode as oc_module

    async def fake_to_thread(fn, *args, **kwargs):
        if fn is oc_module.read_subagent_type:
            return "explore"
        if fn is oc_module.lookup_subagent_output_tokens:
            return tokens
        if fn is oc_module.read_subagent_messages:
            return [{"type": "text", "text": "grand"}]
        return fn(*args, **kwargs)

    return fake_to_thread


async def _run_poll_subagent_stream(
    monkeypatch,
    hooks: DriverHooks,
    *,
    list_ids,
    read_parts,
    started,
    stopped,
    duration: float,
    to_thread=None,
) -> None:
    """给 _poll_subagent_stream 打快轮询与假 db 读，跑 duration 秒后取消。

    duration 取轮询间隔的数十倍：余量留给线程池调度，机器负载高时它可能延迟上百毫秒。
    """
    from workflow_controller.driver import opencode as oc_module

    monkeypatch.setattr(oc_module, "_SUBAGENT_POLL_INTERVAL", 0.01)
    monkeypatch.setattr(oc_module, "list_descendant_sessions", list_ids)
    monkeypatch.setattr(oc_module, "read_subagent_parts", read_parts)
    if to_thread is not None:
        monkeypatch.setattr(oc_module.asyncio, "to_thread", to_thread)

    import asyncio

    id_ready = asyncio.Event()
    id_ready.set()
    task = asyncio.ensure_future(
        _poll_subagent_stream(
            {},
            hooks,
            id_ready,
            lambda: "ses_main",
            started,
            stopped,
        )
    )
    await asyncio.sleep(duration)
    task.cancel()
    with contextlib_suppress():
        await task


class TestPollSubagentStream:
    """_poll_subagent_stream: background DB poller that discovers sub-sessions
    and emits incremental progress events in real time.
    """

    async def test_emits_start_and_progress_for_new_subagent(self, monkeypatch):
        """When a new sub-session appears in the db, the poller fires
        on_subagent_start (once) + on_subagent_progress (new parts).
        """
        # Simulate: first poll sees nothing; second poll sees one sub-session
        # with one part; third poll sees the same sub with an additional part.
        call_state = {"n": 0}
        hooks, starts, progress = _hooks_capturing_start_progress()

        # Phase 1: no sub-sessions. Phase 2: one sub with 1 part. Phase 3: same
        # sub with 2 parts. After phase 3, cancel.
        def fake_list_ids(root_id, env=None):
            call_state["n"] += 1
            if call_state["n"] <= 1:
                return []  # baseline poll + first real poll: nothing
            return [("ses_sub", "ses_main")]

        def fake_read_parts(sid, env=None, max_parts=200):
            if call_state["n"] <= 2:
                return [("p1", [{"type": "text", "text": "first"}])]
            return [
                ("p1", [{"type": "text", "text": "first"}]),
                ("p2", [{"type": "thinking", "text": "second"}]),
            ]

        started: set[str] = set()
        # Let it run 3 polls
        await _run_poll_subagent_stream(
            monkeypatch,
            hooks,
            list_ids=fake_list_ids,
            read_parts=fake_read_parts,
            started=started,
            stopped=set(),
            duration=1.0,
        )

        assert "ses_sub" in started
        assert len(starts) == 1
        assert starts[0][1]["agent_id"] == "ses_sub"
        # Progress fired with the new messages
        assert len(progress) >= 1
        # First progress had "first"; a later one had "second"
        all_msgs: list = []
        for _sid, data in progress:
            all_msgs.extend(data["messages"])
        assert any(m.get("text") == "first" for m in all_msgs)
        assert any(m.get("text") == "second" for m in all_msgs)

    async def test_excludes_main_session(self, monkeypatch):
        """The main opencode session's own parts are never treated as a
        subagent — they're in the exclude set.
        """
        starts: list = []

        async def on_start(sid, data):
            starts.append((sid, data))

        hooks = DriverHooks(session_id="sid", on_subagent_start=on_start)

        await _run_poll_subagent_stream(
            monkeypatch,
            hooks,
            list_ids=lambda root_id, env=None: [],
            read_parts=lambda sid, env=None, max_parts=200: [],
            started=set(),
            stopped=set(),
            duration=0.5,
        )
        assert starts == []

    async def test_no_progress_hook_no_crash(self, monkeypatch):
        """When on_subagent_progress is None (cc path), the poller is never
        started by query() — but calling it directly must not crash.
        """
        # on_subagent_progress is None
        hooks = DriverHooks(session_id="sid")
        await _run_poll_subagent_stream(
            monkeypatch,
            hooks,
            list_ids=lambda root_id, env=None: [],
            read_parts=lambda sid, env=None, max_parts=200: [],
            started=set(),
            stopped=set(),
            duration=0.3,
        )

    async def test_stopped_subsession_emits_no_start_or_progress(self, monkeypatch):
        """stop 已触发的子会话，poller 不应再发 start 或 progress。"""
        call_state = {"n": 0}
        hooks, starts, progress = _hooks_capturing_start_progress()

        # 预置 stopped：模拟 stop 先于 poller 发现；首次返回空（baseline），
        # 否则 ses_sub 被当作既有会话过滤
        stopped: set[str] = {"ses_sub"}

        def fake_list_ids(root_id, env=None):
            call_state["n"] += 1
            if call_state["n"] == 1:
                return []
            return [("ses_sub", "ses_main")]

        await _run_poll_subagent_stream(
            monkeypatch,
            hooks,
            list_ids=fake_list_ids,
            read_parts=lambda sid, env=None, max_parts=200: [
                ("p1", [{"type": "text", "text": "final"}])
            ],
            started=set(),
            stopped=stopped,
            duration=0.5,
        )

        assert len(starts) == 0
        assert len(progress) == 0

    async def test_grandson_start_carries_parent_agent_id(self, monkeypatch):
        """孙 agent 的 start 事件携带 parent_agent_id=派发者子会话 id；
        直接子代为空串。
        """
        starts: list = []

        async def on_start(sid, data):
            starts.append((sid, data))

        hooks = DriverHooks(
            session_id="sid",
            on_subagent_start=on_start,
            on_subagent_progress=lambda *a, **k: _noop_coro(),
        )

        call_state = {"n": 0}

        def fake_list_ids(root_id, env=None):
            call_state["n"] += 1
            if call_state["n"] <= 1:
                return []
            # 直接子代 ses_child（parent = main）+ 孙 ses_grand（parent = ses_child）
            return [("ses_child", "ses_main"), ("ses_grand", "ses_child")]

        await _run_poll_subagent_stream(
            monkeypatch,
            hooks,
            list_ids=fake_list_ids,
            read_parts=lambda sid, env=None, max_parts=200: [],
            started=set(),
            stopped=set(),
            duration=0.8,
        )

        by_id = {data["agent_id"]: data for _sid, data in starts}
        assert by_id["ses_child"]["parent_agent_id"] == ""
        assert by_id["ses_grand"]["parent_agent_id"] == "ses_child"

    async def test_grandson_stop_fired_from_parent_task_part(self, monkeypatch):
        """子会话 part 流里完成的 task tool part（带 <task id="ses_…"> 完成
        标记）为孙补发 stop，payload 与 stdout 路径同构；pending task part
        每轮复查，完成标记出现前不发。
        """
        starts: list = []
        stops: list = []

        async def on_start(sid, data):
            starts.append((sid, data))

        async def on_stop(sid, data):
            stops.append((sid, data))

        hooks = _subagent_start_stop_hooks(on_start, on_stop)

        call_state = {"n": 0}

        def fake_list_ids(root_id, env=None):
            call_state["n"] += 1
            if call_state["n"] <= 1:
                return []
            # 孙 ses_grand 已落 part（真实 db 里完成过的孙必有 part 行）
            return [("ses_child", "ses_main"), ("ses_grand", "ses_child")]

        envelope = '<task id="ses_grand" state="completed"><task_result>done</task_result></task>'

        def fake_read_parts(sid, env=None, max_parts=200):
            if sid != "ses_child":
                return [("p_g1", [{"type": "text", "text": "grand"}])]
            # 轮 2-3：task part pending（tool_result 无完成标记）；轮 4+：完成
            if call_state["n"] <= 3:
                return [_task_part_row("")]
            return [_task_part_row(envelope)]

        stopped: set[str] = set()
        await _run_poll_subagent_stream(
            monkeypatch,
            hooks,
            list_ids=fake_list_ids,
            read_parts=fake_read_parts,
            started=set(),
            stopped=stopped,
            duration=1.2,
            to_thread=_subagent_stop_to_thread(42),
        )

        # 完成标记出现前不发 stop；出现后恰发一次，payload 同构
        grandson_stops = [(s, d) for s, d in stops if d["agent_id"] == "ses_grand"]
        assert len(grandson_stops) == 1
        data = grandson_stops[0][1]
        assert data["agent_type"] == "explore"
        assert data["content"] == "done"
        assert data["total_tokens"] == 42
        assert data["messages"] == [{"type": "text", "text": "grand"}]
        # 孙登记进共享 stopped，poller 不再为其发 progress
        assert "ses_grand" in stopped
        # 同一完成标记重复出现不重复发
        assert len([1 for _s, d in stops if d["agent_id"] == "ses_grand"]) == 1

    async def test_grandson_stop_waits_for_start(self, monkeypatch):
        """孙的首个 part 与父会话的完成标记同轮到达（父按最早 part 时间先被
        处理）时，stop 不得早于 start 发出——无 start 的 stop 会被前端拓扑
        整条丢弃。
        """
        events: list[tuple[str, str]] = []

        async def on_start(sid, data):
            events.append(("start", data["agent_id"]))

        async def on_stop(sid, data):
            events.append(("stop", data["agent_id"]))

        hooks = _subagent_start_stop_hooks(on_start, on_stop)

        call_state = {"n": 0}

        def fake_list_ids(root_id, env=None):
            call_state["n"] += 1
            if call_state["n"] <= 1:
                return []
            # 父会话最早 part 先于孙 → 处理顺序父在前，孙在后
            return [("ses_child", "ses_main"), ("ses_grand", "ses_child")]

        envelope = '<task id="ses_grand" state="completed"><task_result>done</task_result></task>'

        def fake_read_parts(sid, env=None, max_parts=200):
            if sid == "ses_child":
                # 首轮即完成（孙整个生命周期落在一个轮询周期内）
                return [_task_part_row(envelope)]
            return [("p_g1", [{"type": "text", "text": "grand"}])]

        stopped: set[str] = set()
        await _run_poll_subagent_stream(
            monkeypatch,
            hooks,
            list_ids=fake_list_ids,
            read_parts=fake_read_parts,
            started=set(),
            stopped=stopped,
            duration=1.0,
            to_thread=_subagent_stop_to_thread(7),
        )

        assert ("start", "ses_grand") in events
        grandson_events = [e for e in events if e[1] == "ses_grand"]
        assert grandson_events == [("start", "ses_grand"), ("stop", "ses_grand")]
        assert "ses_grand" in stopped


def contextlib_suppress():
    import contextlib

    return contextlib.suppress(BaseException)


async def _noop_coro():
    """占位 hook：测试里给非关键回调用的空协程。"""
    return None


class TestExtractSubagentContent:
    """_extract_subagent_content: pull the subagent's text out of opencode's
    <task><task_result> envelope (captured shape from opencode 1.17.11 + GLM).
    """

    @staticmethod
    def test_strips_envelope():
        envelope = (
            '<task id="ses_x" state="completed">\n<task_result>\nbody\n</task_result>\n</task>'
        )
        assert _extract_subagent_content(envelope) == "body"

    @staticmethod
    def test_falls_back_to_raw_without_envelope():
        assert _extract_subagent_content("plain output") == "plain output"

    @staticmethod
    def test_non_string_coerced():
        assert _extract_subagent_content(None) == ""
        assert _extract_subagent_content(123) == "123"

    @staticmethod
    def test_multiline_inner_text_preserved():
        envelope = "<task_result>\nline1\nline2\n</task_result>"
        assert _extract_subagent_content(envelope) == "line1\nline2"

    @staticmethod
    def test_session_id_from_envelope():
        envelope = (
            '<task id="ses_0e3e2714affevcEer4Bkk8z9tu" state="completed">\n'
            "<task_result>\nx\n</task_result>\n</task>"
        )
        assert _extract_subagent_session_id(envelope) == "ses_0e3e2714affevcEer4Bkk8z9tu"

    @staticmethod
    def test_session_id_empty_without_envelope():
        assert _extract_subagent_session_id("plain output") == ""
        assert _extract_subagent_session_id(None) == ""


class TestOpenCodeGateWiring:
    """Stage 3b: the driver starts a per-query gate only when on_gate_question
    is wired, and points opencode at it via OPENCODE_CONFIG_CONTENT.
    """

    @staticmethod
    def test_gate_rejects_missing_callback() -> None:
        with pytest.raises(ValueError, match="requires on_gate_question"):
            PerQueryGate(
                hooks=DriverHooks(session_id="s1"),
                session_id="s1",
                require_callback=True,
            )

    async def test_no_gate_when_hooks_none(self) -> None:
        d = OpenCodeDriver()
        assert await getattr(d, "_maybe_start_gate")(None, "s1") is None

    async def test_no_gate_when_callback_absent(self) -> None:
        hooks = DriverHooks(session_id="s1")  # on_gate_question is None
        d = OpenCodeDriver()
        assert await getattr(d, "_maybe_start_gate")(hooks, "s1") is None

    async def test_starts_gate_when_callback_present(self) -> None:
        async def cb(sid: str, data: dict) -> str:
            return "ans"

        hooks = DriverHooks(session_id="ses-1", on_gate_question=cb)
        d = OpenCodeDriver()
        gate = await getattr(d, "_maybe_start_gate")(hooks, "ses-1")
        try:
            assert gate is not None
            assert gate.url.startswith("http://127.0.0.1:")
            # the env overlay the driver writes is the gate config JSON
            cfg = json.loads(build_config_content(gate.url))
            assert cfg["mcp"]["workflow_controller_gate"]["url"] == gate.url
        finally:
            if gate is not None:
                await gate.stop()

    async def test_spawn_failure_stops_started_gate(self, monkeypatch, tmp_path) -> None:
        async def fail_spawn(*args, **kwargs):
            raise FileNotFoundError("opencode")

        monkeypatch.setattr(
            "workflow_controller.driver.opencode.asyncio.create_subprocess_exec", fail_spawn
        )

        stopped = False

        class FakeGate:
            url = "http://127.0.0.1:9/mcp"

            async def stop(self) -> None:
                nonlocal stopped
                stopped = True

        async def fake_start_gate(hooks, session_id):
            return FakeGate()

        d = OpenCodeDriver(opencode_bin="opencode-that-is-not-installed")
        monkeypatch.setattr(d, "_maybe_start_gate", fake_start_gate)

        events = [event async for event in d.query("prompt", session_id="sid", cwd=str(tmp_path))]

        assert stopped
        error_events = [event for event in events if event.event_type == EventType.ERROR]
        assert len(error_events) == 1


def test_config_content_merges_existing_process_config(tmp_path) -> None:
    existing = json.dumps(
        {
            "plugin": ["existing-plugin"],
            "mcp": {"existing": {"type": "local"}},
            "agent": {"existing-agent": {"prompt": "keep"}},
            "instructions": ["existing.md"],
            "compaction": {"prune": True},
            "provider": {"custom": {"name": "Custom"}},
        }
    )

    cfg = json.loads(
        build_config_content(
            gate_url="http://127.0.0.1:9/mcp",
            system_prompt="worker",
            instructions=["session instruction"],
            session_id="merge-test",
            plugins=["workflow-controller-plugin"],
            base_content=existing,
        )
    )

    assert cfg["plugin"] == ["existing-plugin", "workflow-controller-plugin"]
    assert set(cfg["mcp"]) == {"existing", "workflow_controller_gate"}
    assert set(cfg["agent"]) == {"existing-agent", "workflow-controller-worker"}
    assert cfg["instructions"][0] == "existing.md"
    assert len(cfg["instructions"]) == 2
    assert cfg["compaction"] == {"prune": True, "auto": False}
    assert cfg["provider"]["custom"]["name"] == "Custom"
