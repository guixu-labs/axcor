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
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from workflow_controller.config.settings import SessionConfig
from workflow_controller.plugins.breather.context import ContextTracker
from workflow_controller.plugins.breather.handoff import HandoffGenerator
from workflow_controller.plugins.breather.plugin import BreatherPlugin
from workflow_controller.session.events import Event, EventType
from workflow_controller.session.session import Session

from ._helpers import assistant_message_query, subagent_stop_event


def _make_session(name="worker", cwd=None):
    cfg = SessionConfig(
        name=name,
        role="worker",
        cwd=str(cwd) if cwd else "/tmp/test-cwd",
        initial_prompt="run a2h pipeline",
    )
    s = Session(name=name, role="worker", config=cfg)
    s.native_session_id = "cc-" + uuid.uuid4().hex[:8]
    return s


def _make_result(input_tokens=20000, cache_creation=0, cache_read=0, window=200000):
    return Event.create(
        session_id="sid",
        iteration=1,
        event_type=EventType.STEP_FINISH,
        data={
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": 2000,
                "cache_creation_input_tokens": cache_creation,
                "cache_read_input_tokens": cache_read,
            },
            "modelUsage": {"claude": {"contextWindow": window}},
        },
    )


def _make_orchestrator(session, events, **kwargs):
    orch = MagicMock()
    orch.pool.list_sessions.return_value = [session]
    orch.pool.get.return_value = session
    orch.event_router.get_events_sync.return_value = events
    orch.event_router.store = AsyncMock()
    orch.event_router.add_handoff_relation = AsyncMock()
    orch.event_router.update_loop_status = AsyncMock()
    orch.event_router.get_successors_sync = MagicMock(return_value=[])
    orch.event_router.get_predecessor_sync = MagicMock(return_value=None)
    orch.config.starting_prompt = ""
    orch.start_session_with_config = kwargs.get("start", AsyncMock())
    orch.stop_session = kwargs.get("stop", AsyncMock())
    orch.wait_session_done = kwargs.get(
        "wait_done",
        AsyncMock(return_value=True),
    )
    orch.resolve_cli_session_id = kwargs.get("resolve", MagicMock(return_value=None))
    orch.bus = MagicMock()
    orch.bus.observe = MagicMock()
    getattr(orch, "_subagents").in_flight.return_value = {}
    orch.subagent_status.return_value = {}
    return orch


def _ctx(orch, hook_server=None):
    """Build a PluginContext for install (orch/bus are MagicMocks)."""
    from ._helpers import plugin_context

    return plugin_context(orch, hook_server)


def _cfg(strategy="token", threshold=70.0, notify_skill="", handoff_timeout=30):
    from workflow_controller.plugin.base import PluginConfig

    return PluginConfig(
        name="breather",
        version="0.1",
        enabled=True,
        extra={
            "strategy": strategy,
            "context_threshold_pct": threshold,
            "handoff_skill": "a2h-handoff",
            "handoff_filename": "handoff.md",
            "notify_skill": notify_skill,
            "handoff_timeout": handoff_timeout,
        },
    )


def test_handoff_state_lookup_never_aliases_an_unselected_session():
    plugin = BreatherPlugin(_cfg(), MagicMock())
    getattr(plugin, "_select_state")("session-a")
    scoped = getattr(plugin, "_state_for")("session-a")
    scoped.pending = True

    getattr(plugin, "_handoff_context").set("__global__")
    assert getattr(plugin, "_state_for")() is not scoped
    assert getattr(plugin, "_state_for")().pending is False


def test_reset_handoff_state_removes_scoped_entry():
    plugin = BreatherPlugin(_cfg(), MagicMock())
    getattr(plugin, "_select_state")("session-a")
    getattr(plugin, "_state_for")("session-a").pending = True

    plugin.reset_handoff_state("session-a")

    assert "session-a" not in getattr(plugin, "_handoff_states")


def test_global_continuation_template_reaches_default_generator(tmp_path):
    (tmp_path / "continue.md").write_text("继续读取 {handoff_path}", encoding="utf-8")
    config = _cfg()
    config.extra["continuation_prompt_path"] = "continue.md"
    plugin = BreatherPlugin(config, None)
    session = _make_session(cwd=tmp_path)

    prompt = getattr(plugin, "_effective_generator")(session).build_resume_prompt(
        tmp_path / "handoff.md",
        project_dir=str(tmp_path),
    )

    assert prompt == f"继续读取 {tmp_path / 'handoff.md'}"


@pytest.mark.asyncio
async def test_active_handoff_arms_per_session_watchdog():
    plugin = BreatherPlugin(_cfg(handoff_timeout=30), MagicMock())
    session = _make_session()
    plugin.install(_ctx(_make_orchestrator(session, [])))

    getattr(plugin, "_arm_active_handoff")(session)

    state = getattr(plugin, "_state_for")(session.id)
    assert state.pending is True
    assert state.finalizing is True
    assert state.watchdog_task is not None
    assert not state.watchdog_task.done()

    watchdog = state.watchdog_task
    plugin.reset_handoff_state(session.id)
    await asyncio.gather(watchdog, return_exceptions=True)
    assert session.id not in getattr(plugin, "_handoff_states")


@pytest.mark.asyncio
async def test_long_running_worker_still_starts_main_handoff(tmp_path):
    session = _make_session(cwd=tmp_path)
    session.active_seconds = 3600.0

    async def wait_done(_sid, timeout=None, *, wait_timeout=None):
        return timeout is None and wait_timeout == 10.0

    start = AsyncMock(side_effect=RuntimeError("stop after handoff agent start"))
    orch = _make_orchestrator(
        session,
        [],
        wait_done=AsyncMock(side_effect=wait_done),
        start=start,
    )
    plugin = BreatherPlugin(_cfg(), MagicMock())
    plugin.install(_ctx(orch))

    await plugin.finalize_handoff(session)

    orch.wait_session_done.assert_awaited_once_with(session.id, timeout=None, wait_timeout=10.0)
    start.assert_awaited_once()


def _make_request(body=None):
    mock_req = MagicMock()

    async def json_side_effect():
        return body or {}

    mock_req.json = json_side_effect
    return mock_req


def _install_pending_flow_plugin(input_tokens, start=None):
    """装好的 breather 插件 + 会话/orch：query 产 ASSISTANT_MESSAGE，start 可注入失败。"""
    session = _make_session()
    orch = _make_orchestrator(
        session,
        [_make_result(input_tokens)],
        start=start or AsyncMock(),
        stop=AsyncMock(),
    )
    session.query = assistant_message_query(session)
    plugin = BreatherPlugin(_cfg("token", 50), MagicMock())
    plugin.install(_ctx(orch))
    return session, orch, plugin


def _stub_finalize_deps(monkeypatch, plugin, tmp_path, *, with_session_name=False):
    """打桩 finalize 依赖：handoff generator、文件新鲜度、工具部署与收尾清理。"""
    gen = MagicMock()
    gen.build_handoff_agent_prompt.return_value = "agent prompt"
    gen.handoff_path.return_value = tmp_path / "handoff.md"
    if with_session_name:
        gen.handoff_session_name.return_value = "agent-name"
    gen.build_new_session_config.return_value = MagicMock()
    monkeypatch.setattr(plugin, "_handoff_generator", gen)
    monkeypatch.setattr(plugin, "_handoff_file_fresh", MagicMock(return_value=True))
    monkeypatch.setattr(plugin, "_deploy_handoff_tools", MagicMock())
    monkeypatch.setattr(plugin, "_captured_final_text", "final")
    monkeypatch.setattr(plugin, "_disarm_watchdog", MagicMock())
    return gen


def _install_over_threshold_with_driver_stop(monkeypatch, input_tokens=160000):
    """SUBAGENT_STOP 用例底座：resolve 回自身、80%>70% 阈值、driver.stop 可断言。"""
    session = _make_session()
    events = [_make_result(input_tokens=input_tokens, window=200000)]
    orch = _make_orchestrator(session, events, resolve=MagicMock(return_value=session.id))
    plugin = BreatherPlugin(_cfg(threshold=70.0), MagicMock())
    plugin.install(_ctx(orch))
    monkeypatch.setattr(session, "_driver", MagicMock())
    getattr(session, "_driver").stop = AsyncMock()
    return session, plugin


# ── ContextTracker ─────────────────────────────────────────────


class TestContextTracker:
    """优先读已记录快照（context_usage/step_finish/RESULT）；读不到有效读数才
    回退探测，探测无结果返回未知（percentage=None）。
    """

    @staticmethod
    def test_check_uses_events_when_result_present(session):
        """有 result event 时 check 走 check_recorded（免 fork），不调 /context。
        driver_factory 注入抛错的工厂证明不走 command 路径。
        """
        session.native_session_id = "sdk-1"
        store = MagicMock()
        store.get_events_sync.return_value = [_make_result(50000)]
        failing_factory = MagicMock(side_effect=AssertionError("should not fork"))
        for strategy in ("token", "auto", "context", "unknown"):
            tracker = ContextTracker(
                store.get_events_sync,
                session,
                strategy=strategy,
                threshold_pct=70,
                driver_factory=failing_factory,
            )
            r = asyncio.run(tracker.check())
            assert r.method == "token", f"strategy={strategy} did not route to events"
            assert r.current_tokens == 50000

    # ── check() 路由 ──────────────────────────────────────────────

    @staticmethod
    def test_check_falls_back_to_command_when_no_result(session):
        """无已记录读数 → fallback _check_from_command。sdk_id=None 使 command
        短路返回未知（不真起子进程）。
        """
        session.native_session_id = None
        store = MagicMock()
        store.get_events_sync.return_value = []  # 无 result event
        tracker = ContextTracker(store.get_events_sync, session, strategy="token", threshold_pct=70)
        r = asyncio.run(tracker.check())
        assert r.method == "context_command"

    @staticmethod
    def test_context_cmd_no_sdk(session):
        """_check_from_command 无 native_session_id（无 resume 目标）时返回未知。"""
        session.native_session_id = None
        store = MagicMock()
        tracker = ContextTracker(
            store.get_events_sync, session, strategy="context", threshold_pct=70
        )
        r = asyncio.run(tracker.check())
        assert r.current_tokens == 0
        assert r.percentage is None
        assert r.method == "context_command"

    @staticmethod
    def test_context_cmd_without_driver_factory_raises(session):
        """driver_factory 未装配（组合根没接线）是装配错误：显式抛错让上层
        感知，而不是静默返回 0 令牌降级 handoff（fail-open 危险）。
        """
        session.native_session_id = "sdk-1"
        store = MagicMock()
        store.get_events_sync.return_value = []  # 无 result → fallback command
        tracker = ContextTracker(store.get_events_sync, session, strategy="auto", threshold_pct=70)
        with pytest.raises(RuntimeError, match="driver_factory"):
            asyncio.run(tracker.check())

    @staticmethod
    def test_check_from_command_parses_result(session):
        """check() -> _check_from_command 经 driver_factory 构建 driver——
        runtime 取会话配置（None 时由 factory 解析全局）；并解析事件 usage
        为上下文读数。Mocks the driver to stay offline.
        """
        captured: dict = {}

        class _FakeDriver:
            @property
            def is_running(self) -> bool:
                return False

            async def query(self, **kwargs):
                yield _make_result(input_tokens=50000, cache_read=10000, window=200000)

            async def stop(self) -> None:
                pass

        def _fake_create(rt=None, **kw):
            captured["runtime_arg"] = rt
            return _FakeDriver()

        tracker = ContextTracker(
            lambda _sid: [],
            session,
            strategy="auto",
            threshold_pct=70,
            driver_factory=_fake_create,
        )
        r = asyncio.run(tracker.check())

        assert captured["runtime_arg"] is None  # runtime 交由 factory 内部解析
        assert r.method == "context_command"
        assert r.current_tokens == 60000  # 50000 input + 10000 cache_read
        assert r.max_tokens == 200000
        assert r.percentage == 30.0

    @staticmethod
    def test_probe_keeps_partial_reading_when_stream_dies(session):
        """探测流先产出有效读数再抛错（慢/半挂死 driver 命中 15s 超时的场景）：
        已得读数必须保留返回，不得作废成 unknown——否则该场景下 handoff
        永不触发。读数经可变容器逐条落账，与收尾异常解耦。
        """

        class _DyingDriver:
            async def query(self, **kwargs):
                yield _make_result(input_tokens=90000, window=100000)
                raise RuntimeError("driver died mid-stream")

            async def stop(self) -> None:
                pass

        tracker = ContextTracker(
            lambda _sid: [],
            session,
            strategy="auto",
            threshold_pct=80,
            driver_factory=lambda *a, **kw: _DyingDriver(),
        )
        r = asyncio.run(tracker.check())

        assert r.method == "context_command"
        assert r.current_tokens == 90000
        assert r.percentage == 90.0

    @staticmethod
    def test_probe_model_follows_session_query_resolution(session, monkeypatch):
        """探测 fork 的 model 走 resolve_query_model（Session.query 同口径）：
        opencode 裸名改写为 env 已解析路由，探测才会命中原会话注入的 provider，
        而不是 opencode 默认 provider。
        """
        monkeypatch.delenv("WORKFLOW_CONTROLLER_DRIVER", raising=False)
        captured: dict = {}

        class _FakeDriver:
            async def query(self, **kwargs):
                captured.update(kwargs)
                yield _make_result(input_tokens=50000)

            async def stop(self) -> None:
                pass

        session.config.runtime = "opencode"
        session.config.model = "glm-5.2"  # 裸名：无 provider/ 前缀
        rotator = MagicMock()
        rotator.env_for_driver.return_value = {
            "ANTHROPIC_MODEL": "workflow-controller-pool/glm-5.2"
        }
        monkeypatch.setattr(session, "_auth_rotator", rotator)

        tracker = ContextTracker(
            lambda _sid: [],
            session,
            strategy="auto",
            threshold_pct=70,
            driver_factory=lambda *a, **kw: _FakeDriver(),
        )
        r = asyncio.run(tracker.check())

        assert captured["model"] == "workflow-controller-pool/glm-5.2"
        assert captured["env"]["ANTHROPIC_MODEL"] == "workflow-controller-pool/glm-5.2"
        assert r.method == "context_command"

    @staticmethod
    def test_events_path_token_below(session):
        store = MagicMock()
        store.get_events_sync.return_value = [_make_result(20000)]
        tracker = ContextTracker(store.get_events_sync, session, strategy="token", threshold_pct=70)
        r = tracker.check_recorded()
        assert r.percentage == 10.0
        assert r.method == "token"

    # ── 已记录读数 ────────────────────────────────────────────────

    @staticmethod
    def test_events_path_token_above(session):
        store = MagicMock()
        store.get_events_sync.return_value = [_make_result(150000)]
        tracker = ContextTracker(store.get_events_sync, session, strategy="token", threshold_pct=70)
        r = tracker.check_recorded()
        assert r.percentage == 75.0

    @staticmethod
    def test_events_path_with_cache(session):
        store = MagicMock()
        store.get_events_sync.return_value = [
            _make_result(input_tokens=100000, cache_creation=10000, cache_read=5000),
        ]
        tracker = ContextTracker(store.get_events_sync, session, strategy="token", threshold_pct=70)
        r = tracker.check_recorded()
        assert r.current_tokens == 115000

    @staticmethod
    def test_events_path_no_events(session):
        """无已记录读数是未知，不是测得的零。"""
        store = MagicMock()
        store.get_events_sync.return_value = []
        tracker = ContextTracker(store.get_events_sync, session, strategy="token", threshold_pct=70)
        r = tracker.check_recorded()
        assert r.current_tokens == 0
        assert r.max_tokens == 0
        assert r.percentage is None

    @staticmethod
    def test_events_path_multiple_models_takes_max_window(session):
        store = MagicMock()
        store.get_events_sync.return_value = [
            Event.create(
                session_id="sid",
                iteration=1,
                event_type=EventType.STEP_FINISH,
                data={
                    "usage": {"input_tokens": 100000},
                    "modelUsage": {
                        "opus": {"contextWindow": 200000},
                        "sonnet": {"contextWindow": 180000},
                    },
                },
            ),
        ]
        tracker = ContextTracker(store.get_events_sync, session, strategy="token", threshold_pct=70)
        r = tracker.check_recorded()
        assert r.max_tokens == 200000
        assert r.percentage == 50.0

    @staticmethod
    def test_events_path_no_model_usage_falls_back_default(session):
        store = MagicMock()
        store.get_events_sync.return_value = [
            Event.create(
                session_id="sid",
                iteration=1,
                event_type=EventType.STEP_FINISH,
                data={"usage": {"input_tokens": 100000}},
            ),
        ]
        tracker = ContextTracker(store.get_events_sync, session, strategy="token", threshold_pct=70)
        r = tracker.check_recorded()
        assert r.max_tokens == 200000
        assert r.percentage == 50.0

    @staticmethod
    def test_check_recorded_uses_configured_window(session):
        """context_window 配置覆盖 modelUsage（修 GLM 1M 被当 200k 算占比）。"""
        store = MagicMock()
        store.get_events_sync.return_value = [_make_result(80000, window=200000)]
        tracker = ContextTracker(
            store.get_events_sync,
            session,
            strategy="token",
            threshold_pct=70,
            context_window=100000,
        )
        r = tracker.check_recorded()
        assert r.max_tokens == 100000
        assert r.percentage == 80.0

    # ── context_window 可配（优先级：yaml > modelUsage > 默认 200000）──

    @staticmethod
    def test_check_recorded_window_falls_back_to_modelusage(session):
        """context_window=None + modelUsage > 默认 200000 → window 取 modelUsage（max 逻辑）。"""
        store = MagicMock()
        store.get_events_sync.return_value = [_make_result(80000, window=250000)]
        tracker = ContextTracker(
            store.get_events_sync, session, strategy="token", threshold_pct=70, context_window=None
        )
        r = tracker.check_recorded()
        assert r.max_tokens == 250000

    @staticmethod
    def test_check_recorded_window_falls_back_to_default(session):
        """context_window=None + events 无 modelUsage(contextWindow=0) → 默认 200000。"""
        store = MagicMock()
        store.get_events_sync.return_value = [_make_result(80000, window=0)]
        tracker = ContextTracker(
            store.get_events_sync, session, strategy="token", threshold_pct=70, context_window=None
        )
        r = tracker.check_recorded()
        assert r.max_tokens == 200000

    @staticmethod
    def test_context_usage_reuses_last_reported_window(session):
        """轮内检查点读最新 context_usage（不带 modelUsage）时沿用上一条
        step_finish 上报的窗口：>200k 模型不再按 200k 扁平分母放大占比误触交接。
        """
        context_only = Event.create(
            session_id="sid",
            iteration=1,
            event_type=EventType.CONTEXT_USAGE,
            data={"context_usage": {"input_tokens": 500000}, "model": "glm"},
        )
        store = MagicMock()
        store.get_events_sync.return_value = [_make_result(100000, window=1_000_000), context_only]
        tracker = ContextTracker(store.get_events_sync, session, strategy="token", threshold_pct=70)
        r = tracker.check_recorded()
        assert r.current_tokens == 500000
        assert r.max_tokens == 1_000_000
        assert r.percentage == 50.0

        # 会话从未上报过窗口才落 200k 扁平兜底
        store.get_events_sync.return_value = [context_only]
        r = tracker.check_recorded()
        assert r.max_tokens == 200000

    @staticmethod
    def test_probe_window_follows_fork_step_finish(session):
        """探测流先到 context_usage（无窗口）、收尾 step_finish 带 modelUsage：
        取流上最新读数与窗口，不提前按 200k 扁平分母返回。
        """

        class _FakeDriver:
            async def query(self, **kwargs):
                yield Event.create(
                    session_id="sid",
                    iteration=1,
                    event_type=EventType.CONTEXT_USAGE,
                    data={"context_usage": {"input_tokens": 500000}, "model": "glm"},
                )
                yield _make_result(500000, window=1_000_000)

            async def stop(self) -> None:
                pass

        tracker = ContextTracker(
            lambda _sid: [],
            session,
            strategy="auto",
            threshold_pct=70,
            driver_factory=lambda *a, **kw: _FakeDriver(),
        )
        r = asyncio.run(tracker.check())
        assert r.method == "context_command"
        assert r.current_tokens == 500000
        assert r.max_tokens == 1_000_000
        assert r.percentage == 50.0

    @pytest.fixture
    def session(self):
        s = _make_session()
        return s


# ── HandoffGenerator ───────────────────────────────────────────


class TestHandoffGenerator:
    @staticmethod
    def test_file_path(tmp_path):
        s = _make_session(cwd=str(tmp_path))
        gen = HandoffGenerator(skill="x", handoff_filename="handoff-test.md")
        path = gen.handoff_path(s)
        assert path.name == "handoff-test.md"
        assert str(tmp_path) in str(path.parent)

    @staticmethod
    def test_resume_prompt(tmp_path):
        hf = tmp_path / "handoff.md"
        hf.write_text("data: execute done", encoding="utf-8")
        gen = HandoffGenerator(skill="x", handoff_filename="handoff.md")
        prompt = gen.build_resume_prompt(hf)
        # Resume prompt references the file path so the new worker reads
        # via Read tool — inlining the content would blow past the Windows
        # cmd.exe 8191-char command line limit when used as `claude -p`.
        assert str(hf) in prompt
        # And it must NOT inline file content (regression guard).
        assert "execute done" not in prompt

    @staticmethod
    def test_new_config(tmp_path):
        s = _make_session(cwd=str(tmp_path))
        hf = tmp_path / "x.md"
        hf.write_text("continue from verify", encoding="utf-8")
        gen = HandoffGenerator(skill="x", handoff_filename="handoff.md")
        cfg = gen.build_new_session_config(s, hf)
        assert cfg.name == "worker-h2"
        assert cfg.cwd == s.config.cwd
        # New session's initial_prompt now references the handoff file
        # path (worker reads it via Read tool) instead of inlining the
        # content. See test_resume_prompt comment for why.
        assert str(hf) in cfg.initial_prompt
        assert "continue from verify" not in cfg.initial_prompt

    @staticmethod
    def test_new_config_inherits_system_prompt_and_runtime(tmp_path):
        """handoff 接力必须继承原 session 的 system_prompt 与 runtime，否则
        successor 丢失行为约束 / driver 类型回退全局。
        """
        cfg = SessionConfig(
            name="worker",
            role="worker",
            cwd=str(tmp_path),
            initial_prompt="run",
            system_prompt="AUQ gate",
            runtime="claude_cli",
        )
        s = Session(name="worker", role="worker", config=cfg)
        hf = tmp_path / "x.md"
        hf.write_text("continue", encoding="utf-8")
        gen = HandoffGenerator(skill="x", handoff_filename="handoff.md")
        new_cfg = gen.build_new_session_config(s, hf)
        assert new_cfg.system_prompt == "AUQ gate"
        assert new_cfg.runtime == "claude_cli"

    @staticmethod
    def test_handoff_path_absolute(tmp_path):
        abs_file = tmp_path / "sub" / "out.md"
        s = _make_session()
        gen = HandoffGenerator(skill="x", handoff_filename=str(abs_file))
        path = gen.handoff_path(s)
        assert str(path) == str(abs_file)

    @staticmethod
    def test_handoff_path_relative(tmp_path):
        s = _make_session(cwd=str(tmp_path))
        gen = HandoffGenerator(skill="x", handoff_filename="sub/handoff.md")
        path = gen.handoff_path(s)
        assert str(path) == str(tmp_path / "sub" / "handoff.md")

    @staticmethod
    def test_handoff_path_empty_cwd():
        s = _make_session(cwd="")
        gen = HandoffGenerator(skill="x", handoff_filename="out.md")
        path = gen.handoff_path(s)
        assert path.name == "out.md"

    @staticmethod
    def test_template_path_loaded_when_set(tmp_path):
        """设了且可读 → 用文件内容作模板，内置 prompt 不出场。"""
        tpl = tmp_path / "my-template.md"
        tpl.write_text(
            "CUSTOM 模板：交接文件={handoff_filename} 上轮worker={worker_session_id}",
            encoding="utf-8",
        )
        gen = HandoffGenerator(
            skill="x",
            handoff_filename="handoff.md",
            handoff_prompt_path=str(tpl),
        )
        gen.build_handoff_agent_prompt(
            final_text="ft",
            project_dir=str(tmp_path),
            worker_session_id="ws-1",
        )
        written = (tmp_path / ".workflow-controller" / "handoff-agent-prompt.md").read_text(
            encoding="utf-8"
        )
        assert "CUSTOM 模板：交接文件=handoff.md 上轮worker=ws-1" in written
        assert "你是一个交接文档组装器" not in written  # 内置不出场

    # ── handoff_prompt_path：prompt 模板可配（领域注入自己的交接结构）──

    @staticmethod
    def test_template_path_missing_falls_back_to_builtin(tmp_path, caplog):
        """设了但读不到 → 回落内置 prompt + warn（graceful 降级）。"""
        gen = HandoffGenerator(
            skill="x",
            handoff_filename="handoff.md",
            handoff_prompt_path=str(tmp_path / "nope.md"),
        )
        with caplog.at_level(
            logging.WARNING, logger="workflow_controller.plugins.breather.handoff"
        ):
            gen.build_handoff_agent_prompt(
                final_text="ft",
                project_dir=str(tmp_path),
                worker_session_id="ws",
            )
        written = (tmp_path / ".workflow-controller" / "handoff-agent-prompt.md").read_text(
            encoding="utf-8"
        )
        assert "你是一个交接文档组装器" in written  # 内置兜底
        assert any("handoff_prompt_path" in r.message for r in caplog.records)

    @staticmethod
    def test_no_template_path_uses_builtin(tmp_path):
        """未设模板时，主代理交接和接续都使用领域无关的默认指令。"""
        gen = HandoffGenerator(skill="x", handoff_filename="handoff.md")
        gen.build_handoff_agent_prompt(
            final_text="ft",
            project_dir=str(tmp_path),
            worker_session_id="ws",
            initial_prompt="并行派发 researcher、checker、writer",
        )
        written = (tmp_path / ".workflow-controller" / "handoff-agent-prompt.md").read_text(
            encoding="utf-8"
        )
        assert "你是一个交接文档组装器" in written
        assert "{handoff_filename}" not in written  # 槽已填
        assert "并行派发 researcher、checker、writer" in written
        assert "算子目录" not in written
        assert "LOG.md" not in written

        resume = gen.build_resume_prompt(tmp_path / "handoff.md", project_dir=str(tmp_path))
        assert "中断的子代理" in resume
        assert "算子目录" not in resume
        assert "LOG.md" not in resume

    @staticmethod
    def test_template_relative_to_project_dir(tmp_path):
        """相对路径 → 相对 project_dir 解析（和 handoff_filename 一致）。"""
        (tmp_path / "rel-tpl.md").write_text(
            "REL 模板 {handoff_filename}",
            encoding="utf-8",
        )
        gen = HandoffGenerator(
            skill="x",
            handoff_filename="handoff.md",
            handoff_prompt_path="rel-tpl.md",  # 相对
        )
        gen.build_handoff_agent_prompt(
            final_text="",
            project_dir=str(tmp_path),
            worker_session_id="",
        )
        written = (tmp_path / ".workflow-controller" / "handoff-agent-prompt.md").read_text(
            encoding="utf-8"
        )
        assert "REL 模板 handoff.md" in written

    @staticmethod
    def test_template_unknown_slot_left_literal(tmp_path):
        """模板里未知的 {slot} 原样保留不崩（领域可写自定义占位，只有已知槽被填）。"""
        tpl = tmp_path / "tpl.md"
        tpl.write_text("known={handoff_filename}未知={custom_field}", encoding="utf-8")
        gen = HandoffGenerator(
            skill="x",
            handoff_filename="handoff.md",
            handoff_prompt_path=str(tpl),
        )
        gen.build_handoff_agent_prompt(
            final_text="",
            project_dir=str(tmp_path),
            worker_session_id="",
        )
        written = (tmp_path / ".workflow-controller" / "handoff-agent-prompt.md").read_text(
            encoding="utf-8"
        )
        assert "known=handoff.md" in written
        assert "未知={custom_field}" in written  # 未知槽原样，未崩

    @staticmethod
    def test_prompt_narrative_points_at_daemon_events_dir(tmp_path):
        """单库化后 narrative 读库位置必须指向守护进程库所在目录（agent cwd
        = worker 工程下没有该库），prompt 中不再出现 "." 实参。
        """
        gen = HandoffGenerator(skill="x", handoff_filename="handoff.md")
        gen.build_handoff_agent_prompt(
            final_text="ft",
            project_dir=str(tmp_path),
            worker_session_id="ws-1",
            events_project_dir="/daemon-side/proj",
        )
        written = (tmp_path / ".workflow-controller" / "handoff-agent-prompt.md").read_text(
            encoding="utf-8"
        )
        assert 'extract_worker_narrative "/daemon-side/proj" "ws-1"' in written
        assert 'extract_worker_narrative "."' not in written

    @staticmethod
    def test_prompt_narrative_injected_in_custom_template(tmp_path):
        """领域自定义模板沿用内置调用写法时，"." 同样被替换为守护进程目录。"""
        tpl = tmp_path / "my-tpl.md"
        tpl.write_text(
            "narrative: python .workflow-controller/handoff_tools.py "
            'extract_worker_narrative "." "{worker_session_id}"',
            encoding="utf-8",
        )
        gen = HandoffGenerator(
            skill="x",
            handoff_filename="handoff.md",
            handoff_prompt_path=str(tpl),
        )
        gen.build_handoff_agent_prompt(
            final_text="",
            project_dir=str(tmp_path),
            worker_session_id="ws-1",
            events_project_dir="/daemon-side/proj",
        )
        written = (tmp_path / ".workflow-controller" / "handoff-agent-prompt.md").read_text(
            encoding="utf-8"
        )
        assert 'extract_worker_narrative "/daemon-side/proj" "ws-1"' in written

    @staticmethod
    def test_prompt_narrative_keeps_template_default_without_events_dir(tmp_path):
        """未传 events_project_dir（旧调用方/未接线）→ 模板原文保留，不崩。"""
        gen = HandoffGenerator(skill="x", handoff_filename="handoff.md")
        gen.build_handoff_agent_prompt(
            final_text="ft",
            project_dir=str(tmp_path),
            worker_session_id="ws-1",
        )
        written = (tmp_path / ".workflow-controller" / "handoff-agent-prompt.md").read_text(
            encoding="utf-8"
        )
        assert 'extract_worker_narrative "."' in written

    @pytest.mark.parametrize(
        "name,expected",
        [
            # First handoff — name has no -hN suffix yet.
            ("android-to-harmony", "android-to-harmony-h2"),
            ("worker", "worker-h2"),
            # Subsequent handoffs — increment trailing -hN.
            ("android-to-harmony-h2", "android-to-harmony-h3"),
            ("android-to-harmony-h3", "android-to-harmony-h4"),
            ("worker-h9", "worker-h10"),
            ("worker-h99", "worker-h100"),
            # Legacy doubled-suffix from before this scheme — only the trailing
            # -hN segment is incremented, so chained names start growing
            # linearly instead of doubling further.
            ("android-to-harmony-h2-h2", "android-to-harmony-h2-h3"),
            ("foo-h2-h2-h2", "foo-h2-h2-h3"),
            # Middle-of-name patterns must NOT be matched — only end-of-name.
            ("foo-h2-bar", "foo-h2-bar-h2"),
            ("h2-prefix-only", "h2-prefix-only-h2"),
            # `h` followed by non-digit at end is not a handoff suffix.
            ("weird-hX", "weird-hX-h2"),
            ("weird-h", "weird-h-h2"),
        ],
    )
    def test_next_handoff_name(self, name, expected):
        assert HandoffGenerator.next_handoff_name(name) == expected


# ── BreatherPlugin helpers ─────────────────────────────────────


def _capture_subscribe(orch):
    cb_store = []

    def fake_subscribe(event_type, cb):
        cb_store.append(cb)

    orch.bus.observe = fake_subscribe
    return cb_store


async def _run_handoff_test(
    plugin, orch, session, cb_store, handoff_file=None, write_file=True, max_triggers=1
):
    """Common helper for handoff tests. Runs plugin and trigger concurrently."""
    if handoff_file is None:
        handoff_file = Path(session.config.cwd) / "handoff.md"
        handoff_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        handoff_file.parent.mkdir(parents=True, exist_ok=True)

    async def trigger():
        for _i in range(max_triggers):
            await asyncio.sleep(0.05)
            if write_file:
                handoff_file.write_text("# content", encoding="utf-8")
            # install 现在 subscribe _on_context_event（SUBAGENT_STOP，cb_store[0]），
            # notify_skill hook 在其后；trigger 要 notify_skill hook（处理 TOOL_RESULT）。
            notify_cb = next(
                (cb for cb in cb_store if cb.__name__ == "_on_notify_skill_hook"),
                None,
            )
            if notify_cb:
                await notify_cb(
                    Event.create(
                        session_id=session.native_session_id,
                        iteration=0,
                        event_type=EventType.TOOL_RESULT,
                        data={"tool_name": "a2h-handoff"},
                    )
                )

    trigger_task = asyncio.ensure_future(trigger())

    async def call_plugin():
        return await getattr(plugin, "_handle_breather")(_make_request())

    resp = await asyncio.wait_for(call_plugin(), timeout=5)
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(trigger_task, timeout=1)
    return json.loads(resp.text)


# ── BreatherPlugin - full flow ─────────────────────────────────


class TestBreather:
    """BreatherPlugin full-flow tests — install + hook handling."""

    @staticmethod
    def test_force_false_below_threshold_no_action():
        """force=False（默认）+ 低于阈值 → 不触发（回归：显式 force=False 行为不变）。"""
        session = _make_session()
        orch = _make_orchestrator(session, [_make_result(20000)])
        plugin = BreatherPlugin(_cfg("token", 50), MagicMock())
        plugin.install(_ctx(orch))

        resp = asyncio.run(getattr(plugin, "_handle_breather")(_make_request({"force": False})))
        data = json.loads(resp.text)
        assert data["handoff_triggered"] is False
        assert data["context"]["percentage"] == 10.0

    @staticmethod
    def test_get_web_router_exposes_handoff_endpoint():
        """breather 经 get_web_router 暴露浏览器端点 POST /api/breather/handoff。"""
        plugin = BreatherPlugin(_cfg(), MagicMock())
        router = plugin.get_web_router()
        assert router is not None
        assert any(getattr(r, "path", None) == "/api/breather/handoff" for r in router.routes)

    @staticmethod
    def test_session_id_known_resolves_correctly():
        session = _make_session("worker")
        orch = _make_orchestrator(
            session,
            [_make_result(20000)],
            resolve=MagicMock(return_value=session.id),
        )

        plugin = BreatherPlugin(_cfg("token", 70), MagicMock())
        plugin.install(_ctx(orch))

        resp = asyncio.run(
            getattr(plugin, "_handle_breather")(
                _make_request({"session_id": "known-cli-sid"}),
            )
        )
        data = json.loads(resp.text)
        assert data["handoff_triggered"] is False
        assert data["context"]["percentage"] == 10.0
        orch.pool.get.assert_called_once_with("known-cli-sid")

    # ── web 端点（get_web_router）：浏览器手动 handoff，共享 request_handoff ──

    @staticmethod
    def test_session_id_unknown_falls_back_to_traversal():
        session = _make_session("worker")
        orch = _make_orchestrator(
            session,
            [_make_result(20000)],
            resolve=MagicMock(return_value=None),
        )
        orch.pool.get = MagicMock(return_value=None)

        plugin = BreatherPlugin(_cfg("token", 70), MagicMock())
        plugin.install(_ctx(orch))

        resp = asyncio.run(
            getattr(plugin, "_handle_breather")(
                _make_request({"session_id": "unknown-cli-sid"}),
            )
        )
        data = json.loads(resp.text)
        assert data["handoff_triggered"] is False
        orch.resolve_cli_session_id.assert_called_once_with("unknown-cli-sid")

    @staticmethod
    def test_no_session_id_uses_traversal():
        session = _make_session("worker")
        orch = _make_orchestrator(session, [_make_result(20000)])

        plugin = BreatherPlugin(_cfg("token", 70), MagicMock())
        plugin.install(_ctx(orch))

        resp = asyncio.run(getattr(plugin, "_handle_breather")(_make_request()))
        data = json.loads(resp.text)
        assert data["handoff_triggered"] is False
        orch.resolve_cli_session_id.assert_not_called()

    @staticmethod
    def test_no_sessions_at_all():
        orch = MagicMock()
        orch.pool.list_sessions.return_value = []
        plugin = BreatherPlugin(_cfg(), MagicMock())
        plugin.install(_ctx(orch))

        resp = asyncio.run(getattr(plugin, "_handle_breather")(_make_request()))
        data = json.loads(resp.text)
        assert data["handoff_triggered"] is False
        assert "no active session" in data["reason"]

    @staticmethod
    def test_find_session_by_sdk_id_direct_match():
        session = _make_session("worker")
        orch = _make_orchestrator(
            session, [_make_result(20000)], resolve=MagicMock(return_value=session.id)
        )

        plugin = BreatherPlugin(_cfg("token", 70), MagicMock())
        plugin.install(_ctx(orch))

        result = getattr(plugin, "_find_session_by_sdk_id")(session.native_session_id)
        assert result is session

    @staticmethod
    def test_find_session_by_sdk_id_not_found():
        session = _make_session("worker")
        orch = _make_orchestrator(
            session, [_make_result(20000)], resolve=MagicMock(return_value=None)
        )
        orch.pool.get = MagicMock(return_value=None)

        plugin = BreatherPlugin(_cfg("token", 70), MagicMock())
        plugin.install(_ctx(orch))

        result = getattr(plugin, "_find_session_by_sdk_id")("nonexistent-id")
        assert result is None  # 不匹配任何 native_session_id

    @staticmethod
    def test_check_context_method():
        session = _make_session("worker")
        orch = _make_orchestrator(session, [_make_result(30000)])

        plugin = BreatherPlugin(_cfg("token", 70), MagicMock())
        plugin.install(_ctx(orch))

        result = asyncio.run(getattr(plugin, "_check_context")(session))
        assert result.percentage == 15.0
        assert result.current_tokens == 30000

    @pytest.mark.asyncio
    async def test_below_threshold_no_action(self):
        session = _make_session()
        orch = _make_orchestrator(session, [_make_result(20000)])
        plugin = BreatherPlugin(_cfg("token", 50), MagicMock())
        plugin.install(_ctx(orch))

        resp = await getattr(plugin, "_handle_breather")(_make_request())
        data = json.loads(resp.text)
        assert data["handoff_triggered"] is False
        assert data["context"]["percentage"] == 10.0

    @pytest.mark.asyncio
    async def test_force_true_bypasses_threshold(self):
        """force=True 跳过阈值检查：上下文 10% < 50% 阈值，仍标记想切（pending_subagent_return）。

        force 设 _handoff_force_pending（等 subagent 返回点切），不立即起 handoff
        agent（start_session_with_config 不被调，handoff agent 是 finalize 干的）。
        """
        _session, orch, plugin = _install_pending_flow_plugin(20000)  # 10% < 50% 阈值

        resp = await getattr(plugin, "_handle_breather")(_make_request({"force": True}))
        data = json.loads(resp.text)
        assert data["handoff_triggered"] is True
        assert data["phase"] == "pending"
        assert getattr(plugin, "_handoff_force_pending") is True
        # 还没 arm（没 subagent 返回）→ 不起 handoff agent
        orch.start_session_with_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_web_handoff_force_bypasses_threshold(self):
        """浏览器入口 _web_handoff 走同一 request_handoff：force 跳阈值，标记想切。"""
        _session, orch, plugin = _install_pending_flow_plugin(20000)  # 10% < 50% 阈值

        data = await getattr(plugin, "_web_handoff")({"force": True})
        assert data["handoff_triggered"] is True
        assert data["phase"] == "pending"
        orch.start_session_with_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_handoff_pending_response(self):
        _session, orch, plugin = _install_pending_flow_plugin(150000)

        resp = await getattr(plugin, "_handle_breather")(_make_request())
        data = json.loads(resp.text)
        assert data["handoff_triggered"] is True
        assert data["phase"] == "pending"
        orch.start_session_with_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_concurrent_calls_one_blocked(self):
        session = _make_session()

        orch = _make_orchestrator(
            session,
            [_make_result(150000)],
            start=AsyncMock(),
            stop=AsyncMock(),
        )

        count = 0

        async def slow_query(prompt, iteration=1):
            nonlocal count
            count += 1
            if count == 1:
                await asyncio.sleep(0.1)
            yield Event.create(
                session_id=session.id, iteration=0, event_type=EventType.ASSISTANT_MESSAGE
            )

        session.query = slow_query

        plugin = BreatherPlugin(_cfg("token", 50), MagicMock())
        plugin.install(_ctx(orch))

        async def call():
            resp = await getattr(plugin, "_handle_breather")(_make_request())
            return json.loads(resp.text)

        results = await asyncio.gather(call(), call())
        triggered = [r for r in results if r.get("handoff_triggered")]
        blocked = [r for r in results if not r.get("handoff_triggered")]
        assert len(triggered) == 1
        assert len(blocked) == 1

    @pytest.mark.asyncio
    async def test_new_session_failure_old_untouched(self, monkeypatch):
        session, orch, plugin = _install_pending_flow_plugin(
            150000, start=AsyncMock(side_effect=RuntimeError("start failed"))
        )

        # Trigger handoff → force_pending（等 subagent 返回点）
        resp = await getattr(plugin, "_handle_breather")(_make_request())
        data = json.loads(resp.text)
        assert data["handoff_triggered"] is True

        # armed 等效（主动停后 on_iteration_boundary 调 finalize）：
        # finalize 起 handoff agent → start_session 失败 → 降级，old untouched。
        monkeypatch.setattr(plugin, "_captured_final_text", "final text")
        monkeypatch.setattr(plugin, "_pending_handoff_session_id", session.id)
        await plugin.finalize_handoff(session)
        orch.stop_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_check_context_wires_services_create_driver(self):
        """端到端 wiring 锁定：_check_context 把 PluginServices.create_driver
        注入 ContextTracker——events 为空 + session 带 native_session_id 时走
        command 路径，注入的工厂必须被调用。若未来误删 driver_factory=
        传参，会命中未装配的 None 提前抛 RuntimeError，测试失败。
        """
        session = _make_session("worker")
        orch = _make_orchestrator(session, [])  # 无 result event → fallback command
        captured: dict = {}

        class _FakeDriver:
            @property
            def is_running(self) -> bool:
                return False

            async def query(self, **kwargs):
                yield _make_result(input_tokens=50000, window=200000)

            async def stop(self) -> None:
                pass

        def _stub_create_driver(*args, **kwargs):
            captured["runtime_arg"] = args[0] if args else kwargs.get("runtime")
            return _FakeDriver()

        ctx = _ctx(orch)
        ctx.services.create_driver = _stub_create_driver
        plugin = BreatherPlugin(_cfg("token", 70), MagicMock())
        plugin.install(ctx)
        result = await getattr(plugin, "_check_context")(session)

        assert captured["runtime_arg"] is None  # runtime 交由 factory 内部解析
        assert result.method == "context_command"
        assert result.current_tokens == 50000


# ── notify_skill 自动触发测试 ────────────────────────────────


class TestNotifySkill:
    @staticmethod
    def _capture_subscribe(orch):
        cb_store = []

        def fake_subscribe(event_type, cb):
            # install 现在 subscribe _on_context_event（SUBAGENT_STOP，先）+
            # _on_notify_skill_hook（TOOL_RESULT，notify_skill 配时后）。
            # TestNotifySkill 只关心 notify_skill hook。
            if cb.__name__ == "_on_notify_skill_hook":
                cb_store.append(cb)

        orch.bus.observe = fake_subscribe
        return cb_store

    @pytest.mark.asyncio
    async def test_below_threshold_skipped(self):
        session = _make_session()

        orch = _make_orchestrator(
            session, [_make_result(20000)], resolve=MagicMock(return_value=session.id)
        )
        cb_store = self._capture_subscribe(orch)

        plugin = BreatherPlugin(_cfg("token", 50, notify_skill="a2h-notify"), MagicMock())
        plugin.install(_ctx(orch))

        query_called = False

        async def tracking_query(prompt, iteration=1):
            nonlocal query_called
            query_called = True
            yield Event.create(
                session_id=session.id, iteration=0, event_type=EventType.ASSISTANT_MESSAGE
            )

        session.query = tracking_query

        await cb_store[0](
            Event.create(
                session_id=session.native_session_id,
                iteration=0,
                event_type=EventType.TOOL_RESULT,
                data={"tool_name": "a2h-notify"},
            )
        )

        orch.start_session_with_config.assert_not_called()
        await asyncio.sleep(0.05)
        assert query_called

    @pytest.mark.asyncio
    async def test_unknown_sdk_id_skipped(self):
        session = _make_session()

        orch = _make_orchestrator(session, [_make_result(150000)])
        cb_store = self._capture_subscribe(orch)

        plugin = BreatherPlugin(_cfg("token", 50, notify_skill="a2h-notify"), MagicMock())
        plugin.install(_ctx(orch))

        await cb_store[0](
            Event.create(
                session_id="unknown-sdk-id",
                iteration=0,
                event_type=EventType.TOOL_RESULT,
                data={"tool_name": "a2h-notify"},
            )
        )

        orch.start_session_with_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_wrong_tool_name_ignored(self):
        session = _make_session()
        orch = _make_orchestrator(session, [_make_result(150000)])
        cb_store = self._capture_subscribe(orch)

        plugin = BreatherPlugin(_cfg("token", 50, notify_skill="a2h-notify"), MagicMock())
        plugin.install(_ctx(orch))

        await cb_store[0](
            Event.create(
                session_id=session.native_session_id,
                iteration=0,
                event_type=EventType.TOOL_RESULT,
                data={"tool_name": "some-other-skill"},
            )
        )

        orch.start_session_with_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_continue_session_stores_events(self):
        session = _make_session()
        orch = _make_orchestrator(
            session, [_make_result(20000)], resolve=MagicMock(return_value=session.id)
        )
        cb_store = self._capture_subscribe(orch)

        stored_events = []

        async def store_side_effect(events):
            stored_events.extend(events)

        orch.event_router.store_many = AsyncMock(side_effect=store_side_effect)

        session.query = assistant_message_query(session)

        plugin = BreatherPlugin(_cfg("token", 50, notify_skill="a2h-notify"), MagicMock())
        plugin.install(_ctx(orch))

        await cb_store[0](
            Event.create(
                session_id=session.native_session_id,
                iteration=0,
                event_type=EventType.TOOL_RESULT,
                data={"tool_name": "a2h-notify"},
            )
        )

        await asyncio.sleep(0.05)
        assert len(stored_events) > 0

    @pytest.mark.asyncio
    async def test_notify_hook_before_install_ignored(self):
        plugin = BreatherPlugin(_cfg("token", 50, notify_skill="a2h-notify"), MagicMock())
        await getattr(plugin, "_on_notify_skill_hook")(
            Event.create(
                session_id="any",
                iteration=0,
                event_type=EventType.TOOL_RESULT,
                data={"tool_name": "a2h-notify"},
            )
        )


# ── BreatherPlugin HTTP 集成测试 ────────────────────────────────


class TestBreatherHTTP:
    pass


class TestDoHandoffPhase2:
    """C1: phase-2 起 worker-h2 失败时不停旧 worker（避免断档：旧停 + 新没起）。"""

    @pytest.mark.asyncio
    async def test_new_worker_start_fails_does_not_stop_old(self, monkeypatch, tmp_path):
        session = _make_session(cwd=str(tmp_path))
        session.native_session_id = "sdk-old"
        agent_session = MagicMock(id="agent-1")
        agent_session.name = "worker-handoff"
        stop_mock = AsyncMock()
        orch = _make_orchestrator(
            session,
            [],
            start=AsyncMock(side_effect=[agent_session, RuntimeError("h2 boom")]),
            stop=stop_mock,
        )

        plugin = BreatherPlugin(_cfg(), tmp_path / "config.yaml")
        plugin.install(_ctx(orch))
        _stub_finalize_deps(monkeypatch, plugin, tmp_path)

        await plugin.finalize_handoff(session)

        # C1: h2 起不来时，旧 worker 不能已被停（否则断档）
        stop_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_handoff_agent_config_inherits_worker_runtime(self, monkeypatch, tmp_path):
        """handoff agent 显式继承 worker 的 runtime（与 worker-h2 经
        build_new_session_config 的继承对齐）。否则当 worker 用 per-session
        runtime 覆盖且与全局不同时，handoff agent 会回落到全局 runtime——
        worker / handoff-agent / worker-h2 三者 runtime 不一致。
        """
        session = _make_session(cwd=str(tmp_path))
        session.native_session_id = "sdk-old"
        session.config.runtime = "opencode"  # worker 用 per-session 覆盖

        agent_session = MagicMock(id="agent-1")
        agent_session.name = "worker-handoff"
        captured: list = []

        async def _capture_start(cfg, *, compose_prompt=True):
            captured.append(cfg)
            if len(captured) == 1:
                return agent_session
            raise RuntimeError("h2 boom")  # 起 h2 处降级，足以观察到 agent config

        orch = _make_orchestrator(session, [], start=_capture_start, stop=AsyncMock())

        plugin = BreatherPlugin(_cfg(), tmp_path / "config.yaml")
        plugin.install(_ctx(orch))
        _stub_finalize_deps(monkeypatch, plugin, tmp_path, with_session_name=True)

        await plugin.finalize_handoff(session)

        assert captured, "handoff agent session should have been started"
        assert captured[0].runtime == "opencode", (
            "handoff agent config must inherit the worker's runtime "
            "(parity with worker-h2 via build_new_session_config)"
        )

    @pytest.mark.asyncio
    async def test_prompt_events_dir_via_services_port(self, monkeypatch, tmp_path):
        """prompt 注入的事件库目录取自 PluginServices 端口（events_project_dir），
        不新增 _orchestrator shim 直连——breather shim 访问数受架构门禁
        精确基线约束。
        """
        session = _make_session(cwd=str(tmp_path))
        orch = _make_orchestrator(session, [], start=AsyncMock(), stop=AsyncMock())
        # mock router 上 project_dir 返回 MagicMock——若走 shim 直连，
        # 注入值会变成 "<MagicMock ...>"，断言即可分辨取值来源。
        ctx = _ctx(orch)

        def _events_project_dir():
            return "/daemon-side/proj"

        ctx.services.get_events_project_dir = _events_project_dir

        plugin = BreatherPlugin(_cfg(), tmp_path / "config.yaml")
        plugin.install(ctx)
        gen = _stub_finalize_deps(monkeypatch, plugin, tmp_path, with_session_name=True)

        await plugin.finalize_handoff(session)

        kwargs = gen.build_handoff_agent_prompt.call_args.kwargs
        assert kwargs["events_project_dir"] == "/daemon-side/proj"


class TestOnIterationBoundary:
    """on_iteration_boundary：loop 边界门禁（A3 起），由 orchestrator
    _run_iteration_boundary_gates 派发器调用，返回 IterationGateResult。
    A4 已删掉旧的按名字硬编码找 breather 的路径，本类是唯一覆盖。
    """

    @staticmethod
    def _make_plugin(session, events=None):
        orch = _make_orchestrator(session, events or [])
        plugin = BreatherPlugin(_cfg(), MagicMock())
        plugin.install(_ctx(orch))
        return plugin, orch

    @pytest.fixture
    def session(self):
        return _make_session()

    @pytest.mark.asyncio
    async def test_below_threshold_returns_none(self, monkeypatch, session):
        plugin, _ = self._make_plugin(session)
        monkeypatch.setattr(
            plugin, "_check_context", AsyncMock(return_value=MagicMock(percentage=30.0))
        )
        result = await plugin.on_iteration_boundary(session.id, session, 1)
        assert result.action == "pass"
        assert getattr(plugin, "_captured_final_text") == ""

    @pytest.mark.asyncio
    async def test_main_threshold_100_disables_automatic_handoff_even_above_window(self, session):
        plugin, orch = self._make_plugin(session, [_live_usage_event(tokens=220000)])
        session.config.context_threshold_pct = 100
        setattr(plugin, "_check_context", AsyncMock(return_value=MagicMock(percentage=120.0)))

        assert (await plugin.on_iteration_boundary(session.id, session, 1)).action == "pass"
        assert getattr(plugin, "_handoff_pending") is False
        assert plugin.on_stop_hook(session.id, session) is None
        orch.resolve_cli_session_id.return_value = session.id
        setattr(plugin, "_arm_active_handoff", MagicMock())
        await getattr(plugin, "_on_context_event")(
            Event.create(session.native_session_id, 1, EventType.SUBAGENT_STOP)
        )
        getattr(plugin, "_arm_active_handoff").assert_not_called()
        response = await plugin.request_handoff(session.id, force=False)
        assert response["handoff_triggered"] is False
        assert (await plugin.request_handoff(session.id, force=True))["handoff_triggered"] is True

    @pytest.mark.asyncio
    async def test_above_threshold_returns_stop_and_handoffs(self, session, monkeypatch):
        plugin, _orch = self._make_plugin(
            session,
            [
                Event.create(
                    session_id=session.id,
                    iteration=1,
                    event_type=EventType.ASSISTANT_MESSAGE,
                    data={"content": [{"type": "text", "text": "最后输出"}]},
                ),
            ],
        )
        monkeypatch.setattr(
            plugin, "_check_context", AsyncMock(return_value=MagicMock(percentage=85.0))
        )
        spawned = []
        monkeypatch.setattr(
            "workflow_controller.plugins.breather.plugin.spawn", lambda coro: spawned.append(coro)
        )
        result = await plugin.on_iteration_boundary(session.id, session, 1)
        assert result is not None
        assert result.action == "stop"
        assert result.reason == "breather_handoff"
        assert getattr(plugin, "_captured_final_text") == "最后输出"
        assert getattr(plugin, "_handoff_pending") is True
        assert getattr(plugin, "_pending_handoff_session_id") == session.id
        assert getattr(plugin, "_pending_handoff_started_at") > 0
        assert len(spawned) == 1

    @pytest.mark.asyncio
    async def test_skips_when_handoff_already_pending(self, monkeypatch, session):
        plugin, orch = self._make_plugin(session)
        monkeypatch.setattr(plugin, "_handoff_pending", True)
        getattr(orch, "_subagents").in_flight.return_value = {"still-running": {}}
        orch.subagent_status.return_value = {"still-running": {}}
        monkeypatch.setattr(
            plugin, "_check_context", AsyncMock(return_value=MagicMock(percentage=85.0))
        )
        result = await plugin.on_iteration_boundary(session.id, session, 1)
        assert result.action == "pass"

    @pytest.mark.asyncio
    async def test_pending_handoff_finishes_at_natural_boundary(self, session, monkeypatch):
        plugin, _ = self._make_plugin(session)
        monkeypatch.setattr(plugin, "_handoff_pending", True)
        monkeypatch.setattr(plugin, "_pending_handoff_session_id", session.id)
        spawned = []
        monkeypatch.setattr("workflow_controller.plugins.breather.plugin.spawn", spawned.append)

        result = await plugin.on_iteration_boundary(session.id, session, 1)

        assert result.action == "stop"
        assert result.reason == "breather_handoff"
        assert getattr(plugin, "_handoff_finalizing") is True
        assert len(spawned) == 1
        spawned[0].close()

    @pytest.mark.asyncio
    async def test_exception_returns_none(self, monkeypatch, session):
        plugin, _ = self._make_plugin(session)
        monkeypatch.setattr(plugin, "_check_context", AsyncMock(side_effect=RuntimeError("boom")))
        result = await plugin.on_iteration_boundary(session.id, session, 1)
        assert result.action == "pass"


# ── subagent 返回点 handoff（轮内主动停在干净 turn 边界）──────────────


class TestSubagentReturnHandoff:
    @staticmethod
    def test_subagent_threshold_precedence():
        session = _make_session()
        session.config.context_threshold_pct = 10
        cfg = _cfg(threshold=70)
        plugin = BreatherPlugin(cfg, None)
        assert getattr(plugin, "_subagent_threshold_pct")(session) == 10

        cfg.extra["subagent_context_threshold_pct"] = 5
        assert getattr(plugin, "_subagent_threshold_pct")(session) == 5

        session.config.subagent_context_threshold_pct = 3
        assert getattr(plugin, "_subagent_threshold_pct")(session) == 3

    @pytest.mark.asyncio
    async def test_armed_short_circuits_iteration_boundary(self, monkeypatch):
        """_handoff_stop_armed（主动停触发）→ on_iteration_boundary 跳 re-check
        直接 finalize。粘性：决策一次，边界不 re-check disagree。
        """
        session = _make_session()
        events = [_make_result(input_tokens=50000, window=200000)]  # 25%，未满
        orch = _make_orchestrator(session, events)
        plugin = BreatherPlugin(_cfg(threshold=70.0), MagicMock())
        plugin.install(_ctx(orch))
        monkeypatch.setattr(plugin, "_handoff_stop_armed", True)
        monkeypatch.setattr(plugin, "_pending_handoff_session_id", session.id)
        monkeypatch.setattr(plugin, "_captured_final_text", "armed 时已抓")
        spawned = []
        monkeypatch.setattr(
            "workflow_controller.plugins.breather.plugin.spawn",
            lambda c: spawned.append(c),
        )
        monkeypatch.setattr(plugin, "_check_context", AsyncMock())  # armed 时不应被调

        result = await plugin.on_iteration_boundary(session.id, session, 1)

        getattr(plugin, "_check_context").assert_not_awaited()  # 粘性：没 re-check
        assert getattr(plugin, "_handoff_finalizing") is True
        assert result is not None
        assert result.action == "stop"
        assert result.reason == "breather_handoff"

    @pytest.mark.asyncio
    async def test_subagent_stop_over_threshold_stops_parent_immediately(self, monkeypatch):
        """最后一个子代理结束后，直接停止主 CLI 进入交接。"""
        session, plugin = _install_over_threshold_with_driver_stop(monkeypatch)
        stop_evt = subagent_stop_event(session)
        await getattr(plugin, "_on_context_event")(stop_evt)

        await asyncio.sleep(0)
        assert getattr(plugin, "_handoff_stop_armed") is True
        assert getattr(plugin, "_handoff_pending") is True
        assert getattr(plugin, "_handoff_finalizing") is True
        getattr(session, "_driver").stop.assert_awaited_once()
        assert plugin.on_stop_hook(session.id, session, "partial") is None
        watchdog = getattr(plugin, "_state_for")(session.id).watchdog_task
        plugin.reset_handoff_state(session.id)
        if watchdog is not None:
            await asyncio.gather(watchdog, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_project_main_threshold_and_window_override_global(self, monkeypatch):
        session = _make_session()
        session.config.context_threshold_pct = 10
        session.config.context_window = 1_000_000
        orch = _make_orchestrator(session, [_make_result(input_tokens=110_000, window=200_000)])
        plugin = BreatherPlugin(_cfg(threshold=70), MagicMock())
        plugin.install(_ctx(orch))
        spawned = []
        monkeypatch.setattr("workflow_controller.plugins.breather.plugin.spawn", spawned.append)

        context = getattr(plugin, "_check_context_from_events")(session)
        decision = await plugin.on_iteration_boundary(session.id, session, 1)

        assert context.max_tokens == 1_000_000
        assert context.percentage == 11.0
        assert getattr(plugin, "_threshold_pct")(session) == 10
        assert decision.action == "stop"
        spawned[0].close()

    @pytest.mark.asyncio
    async def test_subagent_stop_below_threshold_no_action(self, monkeypatch):
        """SUBAGENT stop 但 context 未满 → 不 arm、不停。"""
        session = _make_session()
        events = [_make_result(input_tokens=50000, window=200000)]  # 25%
        orch = _make_orchestrator(session, events)
        plugin = BreatherPlugin(_cfg(threshold=70.0), MagicMock())
        plugin.install(_ctx(orch))
        session.stop = AsyncMock()

        stop_evt = subagent_stop_event(session)
        await getattr(plugin, "_on_context_event")(stop_evt)

        assert getattr(plugin, "_handoff_stop_armed") is False
        session.stop.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_interrupted_stop_does_not_arm_handoff(self, monkeypatch):
        """interrupted stop 是内部记账事件（整组交接/清理补发），不是子代理真实返回点，
        即使录制上下文超阈值也不得对已停止会话 arm 交接。
        """
        session, plugin = _install_over_threshold_with_driver_stop(monkeypatch)
        stop_evt = subagent_stop_event(session, status="interrupted")
        await getattr(plugin, "_on_context_event")(stop_evt)

        assert getattr(plugin, "_handoff_stop_armed") is False
        assert getattr(plugin, "_handoff_pending") is False
        assert getattr(plugin, "_handoff_finalizing") is False
        getattr(session, "_driver").stop.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_subscribed_to_subagent_stop_only(self, monkeypatch):
        """_on_context_event 只订阅 SUBAGENT_STOP（类型本身做过滤，不再靠 data.phase）。"""
        session = _make_session()
        events = [_make_result(input_tokens=160000, window=200000)]
        orch = _make_orchestrator(session, events)
        observed: list = []

        def fake_observe(event_type, cb):
            if cb.__name__ == "_on_context_event":
                observed.append(event_type)

        orch.bus.observe = fake_observe
        plugin = BreatherPlugin(_cfg(threshold=70.0), MagicMock())
        plugin.install(_ctx(orch))

        assert observed == [EventType.SUBAGENT_STOP]

    @pytest.mark.asyncio
    async def test_force_stops_parent_after_last_subagent_return(self, monkeypatch):
        """force 跳过阈值，并在最后一个子代理结束时停止主 CLI。"""
        session, plugin = _install_over_threshold_with_driver_stop(monkeypatch, input_tokens=50000)

        # force（被动调）→ 设标志，不立即切
        await plugin.request_handoff(session.native_session_id, force=True)
        assert getattr(plugin, "_handoff_force_pending") is True
        assert getattr(plugin, "_handoff_stop_armed") is False  # 还没 arm（没 subagent 返回）

        # 最后一个 subagent 返回点 → 立即交接（force 跳阈值）
        stop_evt = subagent_stop_event(session)
        await getattr(plugin, "_on_context_event")(stop_evt)
        await asyncio.sleep(0)
        assert getattr(plugin, "_handoff_pending") is True
        assert getattr(plugin, "_handoff_stop_armed") is True
        getattr(session, "_driver").stop.assert_awaited_once()
        watchdog = getattr(plugin, "_state_for")(session.id).watchdog_task
        plugin.reset_handoff_state(session.id)
        if watchdog is not None:
            await asyncio.gather(watchdog, return_exceptions=True)


# ── 流式用量驱动的交接检查 ──────────────────────────────────────


def _live_usage_event(tokens=150000):
    """真实 CLI 解析产物：主会话 assistant 用量即时事件。"""
    from workflow_controller.driver.claude_cli import _cli_line_to_events

    return _cli_line_to_events(
        json.dumps(
            {
                "type": "assistant",
                "message": {
                    "content": [],
                    "usage": {"input_tokens": tokens, "cache_creation_input_tokens": 10000},
                },
            }
        ),
        "sid",
        1,
    )[0]


@pytest.mark.asyncio
async def test_live_context_triggers_subagent_and_stop_handoff(monkeypatch):
    session = _make_session()
    orch = _make_orchestrator(
        session,
        [_live_usage_event()],
        resolve=MagicMock(return_value=session.id),
    )
    plugin = BreatherPlugin(_cfg(), MagicMock())
    plugin.install(_ctx(orch))
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        MagicMock(side_effect=AssertionError("must not probe")),
    )
    monkeypatch.setattr(plugin, "_arm_active_handoff", MagicMock())
    await getattr(plugin, "_on_context_event")(
        Event.create(session.native_session_id, 1, EventType.SUBAGENT_STOP),
    )
    getattr(plugin, "_arm_active_handoff").assert_called_once_with(session)
    # Stop hook 后备停点：自然停止时快照已超阈值 → 捕获 final_text 交接
    decision = plugin.on_stop_hook(session.id, session, "finished work")
    assert decision.action == "let_exit"
    assert getattr(plugin, "_captured_final_text") == "finished work"
    assert plugin.on_stop_hook(session.id, session) is None


@pytest.mark.parametrize(
    "role,paused,in_flight",
    [
        ("handoff_agent", False, {}),
        ("worker", True, {}),
        ("worker", False, {"sub": {}}),
    ],
)
def test_live_context_stop_hook_respects_session_guards(role, paused, in_flight):
    session = _make_session()
    session.role, session.pause_requested = role, paused
    orch = _make_orchestrator(session, [_live_usage_event()])
    getattr(orch, "_subagents").in_flight.return_value = in_flight
    orch.subagent_status.return_value = in_flight
    plugin = BreatherPlugin(_cfg(), MagicMock())
    plugin.install(_ctx(orch))
    assert plugin.on_stop_hook(session.id, session) is None


@pytest.mark.asyncio
async def test_unknown_context_never_claims_below_threshold_and_force_skips_probe(monkeypatch):
    session = _make_session()
    session.native_session_id = None
    orch = _make_orchestrator(session, [])
    plugin = BreatherPlugin(_cfg(), MagicMock())
    plugin.install(_ctx(orch))
    response = await plugin.request_handoff(session.id, force=False)
    assert response["reason"] == "context_usage_unknown"
    assert response["context"]["percentage"] is None
    assert not response["handoff_triggered"]
    assert plugin.on_stop_hook(session.id, session) is None
    assert (await plugin.on_iteration_boundary(session.id, session, 1)).action == "pass"
    monkeypatch.setattr(
        plugin, "_check_context", AsyncMock(side_effect=AssertionError("force must not probe"))
    )
    monkeypatch.setattr(plugin, "_start_handoff_watchdog", MagicMock())
    assert (await plugin.request_handoff(session.id, force=True))["handoff_triggered"]
    monkeypatch.setattr(plugin, "_arm_active_handoff", MagicMock())
    await getattr(plugin, "_on_context_event")(Event.create(session.id, 1, EventType.SUBAGENT_STOP))
    getattr(plugin, "_arm_active_handoff").assert_called_once_with(session)


@pytest.mark.asyncio
async def test_context_prefers_snapshot_and_preserves_real_zero(monkeypatch):
    session = _make_session()
    store = MagicMock()
    result = _make_result(900000)
    result.data["context_usage"] = {"cache_creation_input_tokens": 12000}
    store.get_events_sync.return_value = [result]
    tracker = ContextTracker(store.get_events_sync, session, "auto", 70)
    assert (await tracker.check()).current_tokens == 12000
    # 明确上报的全零是有效读数；其后无效读数不覆盖
    zero = _live_usage_event(0)
    zero.data["context_usage"] = {"input_tokens": 0}
    store.get_events_sync.return_value = [result, zero, _make_result()]
    store.get_events_sync.return_value[-1].data = {"usage": {}}
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        MagicMock(side_effect=AssertionError("must not probe")),
    )
    assert (await tracker.check()).percentage == 0.0


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["snapshot", "empty", "error", "timeout"])
async def test_context_probe_uses_session_settings_and_always_stops(monkeypatch, outcome):
    session = _make_session()
    session.config.runtime = "claude_cli"
    session.config.model = "test-model"
    driver = MagicMock()
    captured = {}

    async def query(**kwargs):
        captured.update(kwargs)
        if outcome == "error":
            raise RuntimeError("probe failed")
        if outcome == "timeout":
            await asyncio.sleep(10)
        if outcome == "snapshot":
            yield _live_usage_event()
        else:
            yield Event.create("sid", 1, EventType.ERROR, {"error": "no usage"})

    driver.query = query
    driver.stop = AsyncMock()
    factory = MagicMock(return_value=driver)
    original_timeout = asyncio.timeout
    monkeypatch.setattr(
        "workflow_controller.plugins.breather.context.asyncio.timeout",
        lambda _: original_timeout(0.01),
    )
    tracker = ContextTracker(MagicMock(), session, "auto", 70, driver_factory=factory)
    result = await getattr(tracker, "_check_from_command")()
    assert factory.call_args.args == ("claude_cli",)
    assert captured["cwd"] == session.config.cwd
    assert captured["model"] == "test-model"
    assert result.percentage == (80.0 if outcome == "snapshot" else None)
    driver.stop.assert_awaited_once()
