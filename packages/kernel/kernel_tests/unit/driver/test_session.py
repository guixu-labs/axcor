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

import pytest

from workflow_controller.config.settings import SessionConfig
from workflow_controller.session.session import Session, SessionState


class TestSession:
    @staticmethod
    def test_initial_state():
        session = Session(name="worker-1", role="worker")
        assert session.state == SessionState.IDLE
        assert session.name == "worker-1"
        assert session.role == "worker"
        assert session.native_session_id is None

    @staticmethod
    def test_reset():
        session = Session(name="test", role="worker")
        session.state = SessionState.FAILED
        session.native_session_id = "old-id"
        session.reset()
        assert session.state == SessionState.IDLE
        assert session.native_session_id is None

    @staticmethod
    def test_retry_fields_default_to_clean_state():
        s = Session(name="w", role="worker")
        assert s.retry_scheduled_at is None
        assert s.retry_count == 0

    @staticmethod
    def test_retry_count_increments():
        s = Session(name="w", role="worker")
        s.retry_count += 1
        assert s.retry_count == 1
        s.retry_count += 1
        assert s.retry_count == 2

    @staticmethod
    def test_reset_clears_retry_fields():
        """reset() must clear retry_scheduled_at and retry_count so a restarted
        session does not inherit stale wait-and-retry state from the previous
        cycle (would show negative countdown or wrong retry_count in TUI).
        """
        s = Session(name="w", role="worker")
        s.retry_scheduled_at = 99999.0
        s.retry_count = 5
        s.reset()
        assert s.retry_scheduled_at is None
        assert s.retry_count == 0

    @staticmethod
    async def test_session_records_gate_failed_without_runtime_coupling(monkeypatch):
        from workflow_controller.driver.hooks import DriverHooks
        from workflow_controller.session.events import Event, EventType

        outcomes = []

        async def emit_outcome(_session, kind):
            outcomes.append(kind)

        class FakeDriver:
            async def query(self, *args, **kwargs):
                assert "gate_url" not in kwargs
                assert kwargs["hooks"] is driver_hooks
                yield Event.create(
                    session_id=session.id,
                    iteration=1,
                    event_type=EventType.ERROR,
                    data={"error": "no port"},
                )
                yield Event.create(
                    session_id=session.id,
                    iteration=1,
                    event_type=EventType.ITERATION_END,
                    data={"reason": "gate_failed"},
                )

        session = Session(config=SessionConfig(name="worker", runtime="codex_cli"))
        driver_hooks = DriverHooks(session_id=session.id)
        session.set_driver_hooks(driver_hooks)
        session.set_outcome_emitter(emit_outcome)
        monkeypatch.setattr(
            "workflow_controller.driver.factory.create_driver",
            lambda *args, **kwargs: FakeDriver(),
        )

        async for _event in session.query("work"):
            pass

        assert session.state is SessionState.IDLE
        assert session.loop_status == "no"
        assert session.stop_reason == "gate_failed"
        assert session.last_error == "no port"
        assert outcomes == ["failed"]

    @staticmethod
    def _model_rotation_session(runtime: str, pinned: bool) -> Session:
        """构造带旧 model 与 provider_index 的会话（模型轮换测试用）。"""
        return Session(
            name="test",
            role="worker",
            config=SessionConfig(
                name="test",
                runtime=runtime,
                model="old-model",
                provider_index=0,
                model_pinned=pinned,
            ),
        )

    @staticmethod
    async def _model_capturing_query(monkeypatch, session) -> str:
        """跑一次 query，返回 driver 实际收到的 model 参数（产一条 STEP_FINISH）。"""
        from workflow_controller.session.events import Event, EventType

        captured: dict = {}

        class _FakeDriver:
            async def query(self, *a, **kw):
                captured["model"] = kw.get("model")
                yield Event.create(
                    session_id=session.id, iteration=1, event_type=EventType.STEP_FINISH, data={}
                )

        monkeypatch.setattr(
            "workflow_controller.driver.factory.create_driver", lambda *args, **kw: _FakeDriver()
        )
        async for _ in session.query("prompt"):
            pass
        return captured["model"]

    async def test_pause_and_play(self):
        session = Session(name="test", role="worker")
        assert not session.pause_requested

        await session.pause()
        assert session.pause_requested

        await session.play()
        assert not session.pause_requested

    async def test_resume_continues_claude_session_on_next_query(self, monkeypatch):
        from workflow_controller.session.events import Event, EventType

        session = Session(name="test", role="worker")
        session.native_session_id = "sdk-xyz"
        await session.pause()

        captured: dict = {}

        class _FakeDriver:
            async def query(self, *a, **kw):
                yield Event.create(
                    session_id=session.id, iteration=1, event_type=EventType.STEP_FINISH, data={}
                )

        def fake_create_driver(runtime=None, **kwargs):
            captured.update(kwargs)
            return _FakeDriver()

        monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create_driver)

        await session.resume()
        async for _ in session.query("prompt"):
            pass
        assert captured.get("resume") == "sdk-xyz"

    async def test_query_passes_cli_wrapper_to_driver(self, monkeypatch):
        """验证 Session 向驱动传递 wrapper。"""
        from workflow_controller.session.events import Event, EventType

        session = Session(
            config=SessionConfig(
                name="worker",
                runtime="claude_cli",
                cli_wrapper="cpx",
            )
        )
        captured: dict = {}

        class _FakeDriver:
            async def query(self, *a, **kw):
                yield Event.create(
                    session_id=session.id, iteration=1, event_type=EventType.STEP_FINISH, data={}
                )

        def fake_create_driver(runtime=None, **kwargs):
            captured["runtime"] = runtime
            captured.update(kwargs)
            return _FakeDriver()

        monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create_driver)
        async for _ in session.query("prompt"):
            pass
        assert captured["runtime"] == "claude_cli"
        assert captured["cli_wrapper"] == "cpx"

    async def test_driver_exception_emits_iteration_end(self, monkeypatch):
        """driver 异常（如 CLI 非零退出）由 session 层接住：ERROR 后补
        ITERATION_END（issue #69 核心场景，对齐上游 862cebc）。
        """
        from workflow_controller.session.events import EventType

        session = Session(config=SessionConfig(name="worker", runtime="claude_cli"))

        class _RaisingQuery:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise RuntimeError("CLI exited with code 1")

        class _RaisingDriver:
            @staticmethod
            def query(*a, **kw):
                return _RaisingQuery()

        def fake_create_driver(runtime=None, **kwargs):
            return _RaisingDriver()

        monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create_driver)
        events = [event async for event in session.query("prompt")]

        assert [e.event_type for e in events] == [EventType.ERROR, EventType.ITERATION_END]
        assert session.stop_reason == "error"
        assert events[-1].data["reason"] == "internal_error"

    async def test_inject_and_poll_prompt(self):
        session = Session(name="test", role="worker")
        assert await session.poll_manual_prompt() is None

        await session.inject_prompt("hello")
        result = await session.poll_manual_prompt()
        assert result == "hello"

        assert await session.poll_manual_prompt() is None

    async def test_stop(self):
        session = Session(name="test", role="worker")
        await session.stop()
        assert session.state == SessionState.STOPPED

    async def test_stop_reaps_driver_even_when_stop_clears_driver_ref(self, monkeypatch):
        """session.stop() 必须先把 driver 捕获到局部变量再调 stop/kill。

        真实 driver.stop() 会 cancel query 任务，query 的 finally（session.query 末尾）
        把 ``self._driver = None``。若 stop() 里先 ``await self._driver.stop()`` 再重读
        ``self._driver.kill()``，第二次读到的已是 None → AttributeError，killpg 没发 →
        worker 子进程成孤儿（web 上实测：stop-all 500 + delete 后 worker 仍在跑）。
        """
        session = Session(name="test", role="worker")
        killed: list[bool] = []

        class _FakeDriver:
            async def stop(self):
                # 复刻真实 query 的 finally：driver.stop() cancel query → finally 清 _driver
                monkeypatch.setattr(session, "_driver", None)

            async def kill(self):
                killed.append(True)

        monkeypatch.setattr(session, "_driver", _FakeDriver())  # type: ignore[assignment]
        await session.stop()  # 不应抛 AttributeError
        assert session.state == SessionState.STOPPED
        assert killed == [True]  # kill 被调 → worker 进程组被 reap

    async def test_query_passes_session_runtime_to_driver(self, monkeypatch):
        """session.query 把 config.runtime 传给 create_driver，让 per-session runtime
        生效（web 选目录后用该目录 workflow-controller.yaml 的 runtime，例如 multi-iteration 的
        claude_cli AUQ gate）。
        """
        from workflow_controller.session.events import Event, EventType

        session = Session(
            name="test", role="worker", config=SessionConfig(name="test", runtime="claude_cli")
        )
        captured: dict = {}

        class _FakeDriver:
            async def query(self, *a, **kw):
                yield Event.create(
                    session_id=session.id, iteration=1, event_type=EventType.STEP_FINISH, data={}
                )

        def fake_create_driver(runtime=None, **kwargs):
            captured["runtime"] = runtime
            return _FakeDriver()

        monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create_driver)
        async for _ in session.query("prompt"):
            pass
        assert captured["runtime"] == "claude_cli"

    @pytest.mark.parametrize(
        "pinned,expected",
        [
            (False, "new-model"),
            (True, "old-model"),
        ],
    )
    async def test_query_rebinds_only_unpinned_model_at_same_provider_index(
        self,
        monkeypatch,
        pinned,
        expected,
    ):
        from types import SimpleNamespace

        from workflow_controller.session.events import Event, EventType

        session = self._model_rotation_session("claude_sdk", pinned)

        class _Rotator:
            @staticmethod
            def resolve_available_provider(index, runtime=None, capability="worker"):
                return index

            @staticmethod
            def get_provider(index=None):
                return SimpleNamespace(model="new-model")

            @staticmethod
            def env_for_driver(base, provider_index=None):
                return {}

        session.set_auth_rotator(_Rotator())  # type: ignore[arg-type]
        captured = {}

        class _Driver:
            async def query(self, *args, **kwargs):
                captured["model"] = kwargs.get("model")
                yield Event.create(
                    session_id=session.id, iteration=1, event_type=EventType.STEP_FINISH, data={}
                )

        monkeypatch.setattr(
            "workflow_controller.driver.factory.create_driver", lambda *args, **kw: _Driver()
        )
        async for _ in session.query("prompt"):
            pass
        assert captured["model"] == expected
        assert session.config.model == expected
        assert session.config.model_pinned is pinned

    @pytest.mark.parametrize(
        ("pinned", "expected"),
        [
            (False, ""),
            (True, "old-model"),
        ],
    )
    async def test_codex_rotation_to_modelless_provider_clears_inherited_model(
        self,
        monkeypatch,
        pinned,
        expected,
    ):
        from types import SimpleNamespace

        from workflow_controller.session.events import Event, EventType

        session = self._model_rotation_session("codex_cli", pinned)

        class _Rotator:
            @staticmethod
            def resolve_available_provider(index, runtime=None, capability="worker"):
                return 1

            @staticmethod
            def get_provider(index=None):
                return SimpleNamespace(model=None)

            @staticmethod
            def env_for_driver(base, provider_index=None):
                return {"ANTHROPIC_API_KEY": "secret"}

        session.set_auth_rotator(_Rotator())  # type: ignore[arg-type]
        captured = {}

        class _Driver:
            async def query(self, *args, **kwargs):
                captured["model"] = kwargs.get("model")
                yield Event.create(
                    session_id=session.id, iteration=1, event_type=EventType.STEP_FINISH, data={}
                )

        monkeypatch.setattr(
            "workflow_controller.driver.factory.create_driver",
            lambda *args, **kw: _Driver(),
        )
        async for _ in session.query("prompt"):
            pass

        assert captured["model"] == expected
        assert session.config.provider_index == 1
        assert session.config.model == expected
        assert session.config.model_pinned is pinned

    async def test_query_rewrites_bare_model_under_global_opencode_runtime(self, monkeypatch):
        """runtime=None 约定用全局 runtime：全局 opencode 下裸 model 同样要改写成
        env 已解析的路由，否则 opencode 不传 -m、注入的 workflow-controller-pool 永远选不中。
        """
        monkeypatch.setattr("workflow_controller.driver.factory._config_runtime", "opencode")
        monkeypatch.delenv("WORKFLOW_CONTROLLER_DRIVER", raising=False)

        session = Session(
            name="test",
            role="worker",
            config=SessionConfig(name="test", runtime=None, model="glm-5.2"),
        )

        class _StubRotator:
            @staticmethod
            def env_for_driver(base, provider_index=None):
                return {"ANTHROPIC_MODEL": "workflow-controller-pool/glm-5.2"}

        session.set_auth_rotator(_StubRotator())  # type: ignore[arg-type]

        assert (
            await self._model_capturing_query(monkeypatch, session)
            == "workflow-controller-pool/glm-5.2"
        )

    async def test_codex_session_uses_provider_model_without_base_url(self, monkeypatch):
        from workflow_controller.session.events import Event, EventType

        monkeypatch.setenv("ANTHROPIC_MODEL", "claude-sonnet-not-a-codex-model")
        session = Session(
            name="test",
            role="worker",
            config=SessionConfig(
                name="test",
                runtime="codex_cli",
                model="",
                provider_index=0,
            ),
        )
        captured: dict = {}

        class _Rotator:
            @staticmethod
            def resolve_available_provider(index, runtime=None, capability="worker"):
                return index

            @staticmethod
            def get_provider(index=None):
                from types import SimpleNamespace

                return SimpleNamespace(model="gpt-test")

            @staticmethod
            def env_for_driver(base, provider_index=None):
                return {"ANTHROPIC_API_KEY": "secret", "ANTHROPIC_MODEL": "gpt-test"}

        session.set_auth_rotator(_Rotator())  # type: ignore[arg-type]

        class _FakeDriver:
            async def query(self, *a, **kw):
                captured["query_kwargs"] = kw
                yield Event.create(
                    session_id=session.id,
                    iteration=1,
                    event_type=EventType.STEP_FINISH,
                    data={},
                )

        monkeypatch.setattr(
            "workflow_controller.driver.factory.create_driver",
            lambda runtime=None, **kwargs: _FakeDriver(),
        )
        async for _ in session.query("prompt"):
            pass

        assert captured["query_kwargs"]["model"] == "gpt-test"

    async def test_query_passes_permission_mode_only_to_constructor(self, monkeypatch):
        from workflow_controller.session.events import Event, EventType

        session = Session(
            name="test",
            role="worker",
            config=SessionConfig(
                name="test",
                permission_mode="read-only",
            ),
        )
        captured: dict = {}

        class _FakeDriver:
            def __init__(self, **kwargs):
                captured["constructor"] = kwargs

            async def query(self, *a, **kw):
                captured["query"] = kw
                yield Event.create(
                    session_id=session.id,
                    iteration=1,
                    event_type=EventType.STEP_FINISH,
                    data={},
                )

        monkeypatch.setattr(
            "workflow_controller.driver.factory.create_driver",
            lambda runtime=None, **kwargs: _FakeDriver(**kwargs),
        )
        async for _ in session.query("prompt"):
            pass

        assert captured["constructor"]["permission_mode"] == "read-only"
        assert "permission_mode" not in captured["query"]

    async def test_query_rewrites_bare_model_under_workflow_controller_driver_override(
        self, monkeypatch
    ):
        """WORKFLOW_CONTROLLER_DRIVER=opencode 覆盖全局 claude_sdk（workflow-controller.yaml=claude_sdk、
        session runtime=None）时，改写判定按 effective runtime 生效：与
        _build_api_env 的注入（同 resolve_runtime 规则）联动后，-m 才命中
        注入的 workflow-controller-pool provider 而非回落默认。
        """
        monkeypatch.setenv("WORKFLOW_CONTROLLER_DRIVER", "opencode")
        monkeypatch.setattr("workflow_controller.driver.factory._config_runtime", "claude_sdk")

        session = Session(
            name="test",
            role="worker",
            config=SessionConfig(name="test", runtime=None, model="glm-5.2"),
        )

        class _StubRotator:
            @staticmethod
            def env_for_driver(base, provider_index=None):
                # 与修复后的 _build_api_env 同产出：注入定义 + 改写后的路由
                return {
                    "OPENCODE_CONFIG_CONTENT": "{}",
                    "ANTHROPIC_MODEL": "workflow-controller-pool/glm-5.2",
                }

        session.set_auth_rotator(_StubRotator())  # type: ignore[arg-type]

        assert (
            await self._model_capturing_query(monkeypatch, session)
            == "workflow-controller-pool/glm-5.2"
        )

    async def test_query_keeps_bare_model_under_global_claude_runtime(self, monkeypatch):
        """对照：全局非 opencode（默认 claude_sdk）时裸 model 原样传给 driver。"""
        monkeypatch.delenv("WORKFLOW_CONTROLLER_DRIVER", raising=False)

        session = Session(
            name="test",
            role="worker",
            config=SessionConfig(name="test", runtime=None, model="glm-5.2"),
        )
        assert await self._model_capturing_query(monkeypatch, session) == "glm-5.2"

    async def test_query_resumes_same_session_on_iteration_2(self, monkeypatch):
        """iteration 2+ 默认续同一 claude session（保留上下文），无需 _resume_next。
        让 loop 多轮无缝衔接 + handoff 有上下文累积前提。
        """
        from workflow_controller.session.events import Event, EventType

        session = Session(name="test", role="worker")
        session.native_session_id = "sdk-abc"  # iteration 1 跑完写入
        captured: dict = {}

        class _FakeDriver:
            async def query(self, *a, **kw):
                yield Event.create(
                    session_id=session.id, iteration=2, event_type=EventType.STEP_FINISH, data={}
                )

        def fake_create_driver(runtime=None, **kwargs):
            captured["resume"] = kwargs.get("resume")
            return _FakeDriver()

        monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create_driver)
        async for _ in session.query("prompt", iteration=2):
            pass
        assert captured["resume"] == "sdk-abc"

    async def test_query_cold_starts_on_iteration_1(self, monkeypatch):
        """iteration 1 冷启动（resume=None），不被 iteration 2+ 默认 resume 影响。"""
        from workflow_controller.session.events import Event, EventType

        session = Session(name="test", role="worker")
        captured: dict = {}

        class _FakeDriver:
            async def query(self, *a, **kw):
                yield Event.create(
                    session_id=session.id, iteration=1, event_type=EventType.STEP_FINISH, data={}
                )

        def fake_create_driver(runtime=None, **kwargs):
            captured["resume"] = kwargs.get("resume")
            return _FakeDriver()

        monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create_driver)
        async for _ in session.query("prompt", iteration=1):
            pass
        assert captured["resume"] is None

    async def test_wait_if_paused_does_not_block_when_not_paused(self):
        session = Session(name="test", role="worker")
        await asyncio.wait_for(session.wait_if_paused(), timeout=0.1)

    async def test_wait_if_paused_blocks_and_resumes(self):
        session = Session(name="test", role="worker")
        await session.pause()

        async def resume():
            await asyncio.sleep(0.05)
            await session.play()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(resume())
            await asyncio.wait_for(session.wait_if_paused(), timeout=1.0)

        assert session.state == SessionState.RUNNING


class TestSessionState:
    @staticmethod
    def test_all_states_exist():
        expected = {
            SessionState.CREATING,
            SessionState.IDLE,
            SessionState.RUNNING,
            SessionState.PAUSED,
            SessionState.COMPLETED,
            SessionState.STOPPED,
            SessionState.FAILED,
            SessionState.WAITING_RETRY,
        }
        assert set(SessionState) == expected

    @staticmethod
    def test_waiting_retry_state_exists():
        """SessionState.WAITING_RETRY is the new state used during the 10 minute
        wait window between detecting a reviewer dead-loop and respawning the
        worker via --resume. See docs/design.md §11.6.
        """
        assert SessionState.WAITING_RETRY.value == "waiting_retry"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason,expected_reason",
    [
        (None, "user_stop"),
        ("auth_switch", "auth_switch"),
    ],
)
async def test_session_stop_records_reason(reason: str | None, expected_reason: str) -> None:
    session = Session(name="t", role="worker")
    if reason is None:
        await session.stop()
    else:
        await session.stop(reason=reason)
    assert session.state.value == "stopped"
    assert session.last_stop_reason == expected_reason


def test_event_type_has_rate_limit_event() -> None:
    """EventType.RATE_LIMIT exists and maps to 'rate_limit_event'."""
    from workflow_controller.session.events import EventType

    assert EventType.RATE_LIMIT.value == "rate_limit_event"


def test_from_cli_message_parses_rate_limit_event() -> None:
    """_cli_line_to_events returns a RATE_LIMIT with a normalized
    limit_signal (kind derived from status) + raw preserving rate_limit_info.
    """
    from workflow_controller.driver.claude_cli import _cli_line_to_events
    from workflow_controller.session.events import EventType

    line = (
        '{"type":"rate_limit_event","rate_limit_info":{"status":"allowed",'
        '"rateLimitType":"five_hour","resetsAt":1779301200,"utilization":0.76},'
        '"uuid":"evt-abc","session_id":"sdk-sid"}'
    )
    ev = _cli_line_to_events(line, session_id="ouro-sid", iteration=3)[0]
    assert ev is not None
    assert ev.event_type == EventType.RATE_LIMIT
    assert ev.session_id == "ouro-sid"
    assert ev.iteration == 3
    # normalized signal — the only field LimitDetector reads
    assert ev.data["limit_signal"]["kind"] == "allowed"
    assert ev.data["limit_signal"]["scope"] == "five_hour"
    # native payload preserved verbatim for observability
    assert ev.data["raw"]["status"] == "allowed"
    assert ev.data["raw"]["rateLimitType"] == "five_hour"
    assert ev.data["raw"]["resetsAt"] == 1779301200


def test_from_cli_message_rate_limit_event_missing_info_returns_empty_dict() -> None:
    """Defensive: rate_limit_event without rate_limit_info still parses, with
    an `unknown` kind (no status to derive from) and an empty raw payload.
    """
    from workflow_controller.driver.claude_cli import _cli_line_to_events
    from workflow_controller.session.events import EventType

    line = '{"type":"rate_limit_event","uuid":"x","session_id":"y"}'
    ev = _cli_line_to_events(line, session_id="ouro-sid", iteration=1)[0]
    assert ev is not None
    assert ev.event_type == EventType.RATE_LIMIT
    assert ev.data["limit_signal"]["kind"] == "unknown"
    assert ev.data["raw"] == {}
