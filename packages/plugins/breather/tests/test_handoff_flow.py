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

"""handoff 端到端流程测试。

覆盖：
0. 上下文超阈值触发 handoff（subagent 返回 / 主 agent 返回均触发）
1. handoff session 名称格式
2. handoff doc + done 文件 → handoff session 标记完成
3. handoff 完成 → 触发 worker continuation session
4. worker continuation session 名称格式
"""

from __future__ import annotations

import asyncio
import time
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from workflow_controller.config.settings import LoopConfig, SessionConfig
from workflow_controller.plugin.base import PluginConfig
from workflow_controller.plugins.breather.handoff import HandoffGenerator
from workflow_controller.plugins.breather.plugin import BreatherPlugin
from workflow_controller.session.events import Event, EventType
from workflow_controller.session.session import Session, SessionState

# ── 辅助工厂 ────────────────────────────────────────────────────────


def _worker_session(name="worker-1", cwd="/tmp/test-handoff", sid=None):
    """创建 worker session，带 handoff 所需的 config 字段。"""
    sid = sid or uuid.uuid4().hex[:12]
    cfg = SessionConfig(
        name=name,
        role="worker",
        cwd=cwd,
        initial_prompt="开发一个abs算子",
        runtime="opencode",
        model="deepseek/deepseek-v4-pro",
        loop=LoopConfig(max_iterations=10),
    )
    s = Session(name=name, role="worker", config=cfg)
    s.id = sid
    s.native_session_id = "sdk-" + uuid.uuid4().hex[:8]
    return s


def _handoff_session(sid, name, cwd="/tmp/test-handoff"):
    """创建 handoff_agent session。"""
    cfg = SessionConfig(
        name=name,
        role="handoff_agent",
        cwd=cwd,
        initial_prompt="盘点当前工程状态",
        runtime="opencode",
        loop=LoopConfig(max_iterations=1, evaluator="always_complete"),
    )
    s = Session(name=name, role="handoff_agent", config=cfg)
    s.id = sid
    return s


def _cfg(threshold=4.0):
    """创建 breather plugin config。"""
    return PluginConfig(
        name="breather",
        version="0.1",
        enabled=True,
        extra={
            "context_threshold_pct": threshold,
            "context_window": 1000000,
            "handoff_skill": "operator-handoff",
            "handoff_filename": "handoff.md",
            "handoff_timeout": 30,
            "handoff_agent_timeout": 10,
        },
    )


def _make_result_event(session_id, input_tokens=50000, window=200000):
    """创建一个 RESULT event。"""
    return Event.create(
        session_id=session_id,
        iteration=1,
        event_type=EventType.STEP_FINISH,
        data={
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": 2000,
            },
            "modelUsage": {"claude": {"contextWindow": window}},
        },
    )


def _make_orchestrator(worker, events=None, start_side_effect=None, stop_side_effect=None):
    """创建 mock orchestrator。"""
    orch = MagicMock()
    orch.pool = MagicMock()
    orch.pool.get.return_value = worker
    orch.pool.list_sessions.return_value = [worker]
    orch.pool.persist = AsyncMock()
    setattr(orch.pool, "_persist_session", AsyncMock())

    if events is not None:
        orch.event_router.get_events_sync.return_value = events
    else:
        orch.event_router.get_events_sync.return_value = []

    orch.event_router.store = AsyncMock()
    orch.event_router.add_handoff_relation = AsyncMock()
    orch.event_router.update_loop_status = AsyncMock()
    orch.event_router.get_successors_sync = MagicMock(return_value=[])
    orch.event_router.get_predecessor_sync = MagicMock(return_value=None)
    orch.config.starting_prompt = ""
    orch.start_session_with_config = AsyncMock(side_effect=start_side_effect)
    orch.stop_session = AsyncMock(side_effect=stop_side_effect)
    orch.wait_session_done = AsyncMock(return_value=True)
    orch.resolve_cli_session_id = MagicMock(return_value=None)
    getattr(orch, "_subagents").in_flight.return_value = {}
    orch.subagent_status.return_value = {}

    orch.bus = MagicMock()
    orch.bus.observe = MagicMock()

    return orch


def _ctx(orch, hook_server=None):
    """Build a PluginContext for install（端口闭包经生产装配函数接到 mock 上）."""
    from ._helpers import plugin_context

    return plugin_context(orch, hook_server)


def _install(orch, cfg=None) -> BreatherPlugin:
    """装好的 breather 插件——标题改写打桩，跳过 30s 轮询。"""
    plugin = BreatherPlugin(cfg or _cfg(), Path("."))
    plugin.install(_ctx(orch))
    setattr(plugin, "_update_session_title", AsyncMock())
    return plugin


def _orch_with_session_lookup(worker, sessions, start_side_effect):
    """orch 桩：pool.get 按 id 解析 worker/agent/continuation，start 按序吐会话。"""
    orch = _make_orchestrator(worker, start_side_effect=start_side_effect)
    orch.pool.get = MagicMock(side_effect=lambda sid: {s.id: s for s in sessions}.get(sid))
    return orch


def _armed_pending_plugin(monkeypatch, orch, gen, agent_sid):
    """装 plugin 并进入 pending 交接态——finalize 从这里接管。"""
    plugin = _install(orch)
    monkeypatch.setattr(plugin, "_handoff_generator", gen)
    monkeypatch.setattr(plugin, "_pending_handoff_session_id", agent_sid)
    monkeypatch.setattr(plugin, "_handoff_pending", True)
    monkeypatch.setattr(plugin, "_pending_handoff_started_at", time.time())
    return plugin


def _stub_handoff_paths(gen, handoff_dir, sid_short, number=1):
    """gen.handoff_path 桩：doc/done 文件名映射到 handoff_dir 下的真实路径。"""
    gen.handoff_path = MagicMock(
        side_effect=lambda s, filename=None: {
            f"{sid_short}-handoff-{number}-doc": handoff_dir / f"{sid_short}-handoff-{number}-doc",
            f"{sid_short}-handoff-{number}-done": handoff_dir
            / f"{sid_short}-handoff-{number}-done",
        }.get(filename, handoff_dir / (filename or ""))
    )


def _far_below_threshold_plugin(tmp_path):
    """1% 上下文（10k/1M）+ 70% 阈值——force 语义用例共用底座。"""
    worker = _worker_session(cwd=str(tmp_path))
    orch = _make_orchestrator(worker)
    orch.event_router.get_events_sync.return_value = [
        _make_result_event(worker.id, input_tokens=10000, window=1000000),
    ]
    plugin = _install(orch, _cfg(threshold=70.0))
    return worker, orch, plugin


def _parallel_handoff_chains(tmp_path):
    """两条并行交接链：worker/handoff/continuation 会话与 doc/done 文件、目录映射。"""
    workers = [
        _worker_session(name="w1", cwd=str(tmp_path / "p1"), sid="a" * 12),
        _worker_session(name="w2", cwd=str(tmp_path / "p2"), sid="b" * 12),
    ]
    agents, continuations, handoff_dirs = [], [], {}
    for worker in workers:
        short = worker.id[:8]
        handoff_dir = tmp_path / worker.name / ".workflow-controller" / "handoff"
        handoff_dir.mkdir(parents=True)
        (handoff_dir / f"{short}-handoff-1-doc").write_text("# doc")
        (handoff_dir / f"{short}-handoff-1-done").write_text("done")
        handoff_dirs[worker.id] = handoff_dir
        agents.append(
            _handoff_session(
                uuid.uuid4().hex[:12],
                f"{short}-handoff-1",
                cwd=str(tmp_path / worker.name),
            )
        )
        continuations.append(
            _worker_session(
                name=f"{short}-continue-1",
                cwd=str(tmp_path / worker.name),
                sid=uuid.uuid4().hex[:12],
            )
        )
    return workers, agents, continuations, handoff_dirs


def _armed_parallel_finalize(monkeypatch, workers, agents, continuations, handoff_dirs):
    """并行 finalize 底座：pool 按 id 解析会话、两侧进入 pending、handoff 路径可解析。"""
    gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
    orch = _make_orchestrator(
        workers[0],
        start_side_effect=[agents[0], continuations[0], agents[1], continuations[1]],
    )
    by_id = {s.id: s for s in (*workers, *agents, *continuations)}
    orch.pool.get = MagicMock(side_effect=lambda sid: by_id.get(sid))
    orch.pool.list_sessions.return_value = workers
    orch.event_router.update_loop_status = AsyncMock()

    plugin = _install(orch)
    monkeypatch.setattr(plugin, "_handoff_generator", gen)
    for worker in workers:
        state = getattr(plugin, "_state_for")(worker.id)
        state.pending = True
        state.session_id = worker.id
        state.started_at = time.time()

    gen.handoff_path = MagicMock(
        side_effect=lambda session, filename=None: handoff_dirs[session.id] / filename
    )
    return orch, plugin


# ── Test 0: 上下文超阈值触发 handoff ──────────────────────────────────


class TestHandoffTrigger:
    """上下文超过阈值时，无论是 subagent 还是主 agent 返回都会触发 handoff。"""

    @staticmethod
    def test_context_over_threshold_triggers_handoff(monkeypatch, tmp_path):
        """上下文超阈值 → iteration_boundary 触发 handoff pending。"""
        worker = _worker_session(cwd=str(tmp_path))
        monkeypatch.setattr(worker, "_stop_requested", False)

        orch = _make_orchestrator(worker)
        orch.event_router.get_events_sync.return_value = [
            _make_result_event(worker.id, input_tokens=500000, window=1000000),
        ]
        getattr(orch, "_subagents").in_flight.return_value = {}
        orch.subagent_status.return_value = {}
        orch.subagent_status.return_value = {}

        plugin = _install(orch, _cfg(threshold=4.0))
        # 防止 finalize_handoff spawn 干扰：mock 为直接返回
        plugin.finalize_handoff = AsyncMock()

        asyncio.run(plugin.on_iteration_boundary(worker.id, worker, 1))

        assert getattr(plugin, "_state_for")(worker.id).pending

    @staticmethod
    def test_context_below_threshold_no_handoff(tmp_path):
        """上下文未超阈值 → handoff 不触发。"""
        worker = _worker_session(cwd=str(tmp_path))

        orch = _make_orchestrator(worker)
        orch.event_router.get_events_sync.return_value = [
            _make_result_event(worker.id, input_tokens=10000, window=1000000),
        ]
        getattr(orch, "_subagents").in_flight.return_value = {}
        orch.subagent_status.return_value = {}
        orch.subagent_status.return_value = {}

        plugin = _install(orch, _cfg(threshold=70.0))

        asyncio.run(plugin.on_iteration_boundary(worker.id, worker, 1))

        assert not getattr(plugin, "_state_for")(worker.id).pending


# ── Test 1: handoff session 名称格式 ──────────────────────────────────


class TestHandoffNaming:
    """handoff session 名称符合格式：{id}-handoff-{n}"""

    @staticmethod
    def test_handoff_session_name_format():
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        name = gen.handoff_session_name("abc12345", 1)
        assert name == "abc12345-handoff-1"

    @staticmethod
    def test_handoff_session_name_increments():
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        name = gen.handoff_session_name("abc12345", 2)
        assert name == "abc12345-handoff-2"

    @staticmethod
    def test_handoff_doc_name_format():
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        name = gen.handoff_doc_name("abc12345", 1)
        assert name == "abc12345-handoff-1-doc"

    @staticmethod
    def test_done_file_name_format():
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        name = gen.done_file_name("abc12345", 1)
        assert name == "abc12345-handoff-1-done"


# ── Test 2: handoff doc + done 文件 → 标记完成 ────────────────────────


class TestHandoffCompletion:
    """handoff session 产出 handoff doc 和 done 文件 → 标记为 success。"""

    @staticmethod
    def test_done_file_marks_success(monkeypatch, tmp_path):
        """handoff agent 写出 done 文件 → session.result = 'success'。"""
        worker = _worker_session(cwd=str(tmp_path))
        sid_short = worker.id[:8]

        # 创建 done 文件
        handoff_dir = tmp_path / ".workflow-controller" / "handoff"
        handoff_dir.mkdir(parents=True)
        doc_name = f"{sid_short}-handoff-1-doc"
        done_name = f"{sid_short}-handoff-1-done"
        (handoff_dir / doc_name).write_text("# Handoff Document\ncontent")
        (handoff_dir / done_name).write_text("done")

        # 创建 handoff agent session
        agent_sid = uuid.uuid4().hex[:12]
        agent_name = f"{sid_short}-handoff-1"
        agent = _handoff_session(agent_sid, agent_name, cwd=str(tmp_path))

        # 创建 handoff generator，设置正确的 handoff_filename
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")

        def _handoff_dir(_s):
            return handoff_dir

        monkeypatch.setattr(gen, "_handoff_dir", _handoff_dir, raising=False)  # 覆盖 handoff 目录

        # 创建新 worker session
        new_worker = _worker_session(
            name="abs-h2",
            cwd=str(tmp_path),
            sid=uuid.uuid4().hex[:12],
        )
        new_worker.state = SessionState.RUNNING  # ← 新 worker 必须已启动

        orch = _orch_with_session_lookup(worker, (worker, agent, new_worker), [agent, new_worker])
        plugin = _armed_pending_plugin(monkeypatch, orch, gen, agent_sid)
        # 伪造 handoff_path
        _stub_handoff_paths(gen, handoff_dir, sid_short)

        # 执行 finalize_handoff
        asyncio.run(plugin.finalize_handoff(worker))

        # agent 应该被标记为 success
        assert agent.result == "success"
        wait_calls = orch.wait_session_done.await_args_list
        assert wait_calls[0].kwargs == {
            "timeout": None,
            "wait_timeout": 10.0,
        }
        assert wait_calls[1].kwargs == {"timeout": 10}

    @staticmethod
    def test_no_done_file_still_continues(monkeypatch, tmp_path):
        """即使没有 done 文件，handoff doc 存在也应该继续（不标记 success 但仍起新 worker）。"""
        worker = _worker_session(cwd=str(tmp_path))
        sid_short = worker.id[:8]

        handoff_dir = tmp_path / ".workflow-controller" / "handoff"
        handoff_dir.mkdir(parents=True)
        doc_name = f"{sid_short}-handoff-1-doc"
        (handoff_dir / doc_name).write_text("# Handoff Document\ncontent")
        # 不创建 done 文件

        agent_sid = uuid.uuid4().hex[:12]
        agent_name = f"{sid_short}-handoff-1"
        agent = _handoff_session(agent_sid, agent_name, cwd=str(tmp_path))

        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        new_worker = _worker_session(name="abs-h2", cwd=str(tmp_path), sid=uuid.uuid4().hex[:12])

        orch = _orch_with_session_lookup(worker, (worker, agent, new_worker), [agent, new_worker])
        plugin = _armed_pending_plugin(monkeypatch, orch, gen, agent_sid)
        _stub_handoff_paths(gen, handoff_dir, sid_short)

        # 确保 done 路径不存在
        done_path = handoff_dir / f"{sid_short}-handoff-1-done"
        if done_path.exists():
            done_path.unlink()

        asyncio.run(plugin.finalize_handoff(worker))

        # 没有 done 文件 → agent.result 不设 success，但仍会起新 worker
        # handoff 继续（文档已就位）
        assert orch.start_session_with_config.call_count >= 2  # agent + new worker


# ── Test 3: handoff 完成 → 触发 worker session ────────────────────────


class TestHandoffTriggersWorker:
    """handoff session 完成后，触发新的 worker continuation session。"""

    @staticmethod
    def test_handoff_completion_creates_new_worker(monkeypatch, tmp_path):
        """handoff agent 完成 → 创建新的 worker session。"""
        worker = _worker_session(cwd=str(tmp_path))
        sid_short = worker.id[:8]

        handoff_dir = tmp_path / ".workflow-controller" / "handoff"
        handoff_dir.mkdir(parents=True)
        doc_name = f"{sid_short}-handoff-1-doc"
        done_name = f"{sid_short}-handoff-1-done"
        (handoff_dir / doc_name).write_text("# Handoff\ncontent")
        (handoff_dir / done_name).write_text("done")

        agent_sid = uuid.uuid4().hex[:12]
        agent_name = f"{sid_short}-handoff-1"
        agent = _handoff_session(agent_sid, agent_name, cwd=str(tmp_path))

        continuation_name = f"{sid_short}-continue-1"
        new_worker = _worker_session(
            name=continuation_name, cwd=str(tmp_path), sid=uuid.uuid4().hex[:12]
        )
        new_worker.state = SessionState.RUNNING

        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")

        orch = _make_orchestrator(worker, start_side_effect=[agent, new_worker])

        plugin = _armed_pending_plugin(monkeypatch, orch, gen, agent_sid)
        _stub_handoff_paths(gen, handoff_dir, sid_short)

        asyncio.run(plugin.finalize_handoff(worker))

        # 验证：调用了 start_session_with_config 至少 2 次（agent + new worker）
        assert orch.start_session_with_config.call_count >= 2

        # 验证：旧 worker 被停止（persist_stopped_state=False：中间 STOPPED 不落名册）
        orch.stop_session.assert_called_once_with(
            worker.id,
            reason="user_stop",
            persist_stopped_state=False,
        )

        # 验证：前序/后续关系链正确建立
        # 1) worker → handoff agent
        # 2) handoff agent → continuation worker
        assert orch.event_router.add_handoff_relation.call_count >= 2
        calls = orch.event_router.add_handoff_relation.call_args_list
        assert calls[0].kwargs["from_session_id"] == worker.id
        assert calls[0].kwargs["to_session_id"] == agent.id
        assert calls[1].kwargs["from_session_id"] == agent.id
        assert calls[1].kwargs["to_session_id"] == new_worker.id

        # 验证：agent 和 new_worker 被持久化
        persist_calls = orch.pool.persist.call_args_list
        persisted_ids = [c.args[0].id for c in persist_calls if c.args]
        assert agent.id in persisted_ids
        assert new_worker.id in persisted_ids

        # 验证：agent status 正确入库（result="success" 是关键完成标记）
        assert agent.result == "success"

        # 验证：continuation worker 携带 birth_handoff_doc
        assert new_worker.birth_handoff_doc == f"{sid_short}-handoff-1-doc"

    @staticmethod
    def test_handoff_completion_persists_worker_once_with_reusable_loop(monkeypatch, tmp_path):
        """交接成功后原 worker 单次落账、终态 loop 可复用——不落中间 STOPPED 行。"""
        worker = _worker_session(cwd=str(tmp_path))
        sid_short = worker.id[:8]

        handoff_dir = tmp_path / ".workflow-controller" / "handoff"
        handoff_dir.mkdir(parents=True)
        doc_name = f"{sid_short}-handoff-1-doc"
        done_name = f"{sid_short}-handoff-1-done"
        (handoff_dir / doc_name).write_text("# Handoff\ncontent")
        (handoff_dir / done_name).write_text("done")

        agent_sid = uuid.uuid4().hex[:12]
        agent = _handoff_session(agent_sid, f"{sid_short}-handoff-1", cwd=str(tmp_path))
        new_worker = _worker_session(
            name=f"{sid_short}-continue-1",
            cwd=str(tmp_path),
            sid=uuid.uuid4().hex[:12],
        )

        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        orch = _make_orchestrator(worker, start_side_effect=[agent, new_worker])
        orch.event_router.update_loop_status = AsyncMock()

        plugin = _armed_pending_plugin(monkeypatch, orch, gen, agent_sid)
        _stub_handoff_paths(gen, handoff_dir, sid_short)

        asyncio.run(plugin.finalize_handoff(worker))

        # worker 名册只落一次账，且是恢复后的 loop 可复用终态
        worker_rows = [
            (c.args[0].loop_status, c.args[0].stop_reason)
            for c in orch.pool.persist.call_args_list
            if c.args and c.args[0].id == worker.id
        ]
        assert worker_rows == [("yes", None)]

    @staticmethod
    def test_handoff_failure_does_not_stop_worker(monkeypatch, tmp_path):
        """handoff agent 超时 → 不停旧 worker（天然降级）。"""
        worker = _worker_session(cwd=str(tmp_path))
        sid_short = worker.id[:8]

        # 不创建任何文件 → handoff 会失败
        agent_sid = uuid.uuid4().hex[:12]
        agent = _handoff_session(agent_sid, f"{sid_short}-handoff-1", cwd=str(tmp_path))

        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")

        orch = _orch_with_session_lookup(worker, (worker, agent), [agent])

        plugin = _armed_pending_plugin(monkeypatch, orch, gen, agent_sid)
        gen.handoff_path = MagicMock(return_value=tmp_path / "nonexistent")

        asyncio.run(plugin.finalize_handoff(worker))

        # worker 不应该被停止（天然降级）
        orch.stop_session.assert_not_called()


# ── Test 4: worker continuation session 名称格式 ───────────────────────


class TestContinuationNaming:
    """worker continuation session 名称符合格式：{id}-continue-{n}"""

    @staticmethod
    def test_continuation_session_name_format():
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        name = gen.continuation_session_name("abc12345", 1)
        assert name == "abc12345-continue-1"

    @staticmethod
    def test_continuation_session_name_increments():
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        name = gen.continuation_session_name("abc12345", 3)
        assert name == "abc12345-continue-3"


class TestExecuteHandoff:
    @staticmethod
    def test_waits_for_handoff_session_with_execution_budget(tmp_path):
        """程序化交接按执行预算等待交接会话。"""
        worker = _worker_session(cwd=str(tmp_path))
        handoff_session = _handoff_session("handoff", "handoff")
        handoff_session.result = "success"
        new_worker = _worker_session(name="new-worker", cwd=str(tmp_path))

        orch = _make_orchestrator(
            worker,
            start_side_effect=[handoff_session, new_worker],
        )
        plugin = _install(orch)
        result = asyncio.run(
            plugin.execute_handoff(
                worker.id,
                SessionConfig(name="handoff", role="handoff_agent", cwd=str(tmp_path)),
                SessionConfig(name="new-worker", cwd=str(tmp_path)),
            )
        )

        assert result["success"] is True
        assert orch.wait_session_done.await_args.kwargs == {"timeout": 300.0}


# ── Test 5: 链式多次 handoff ──────────────────────────────────────────


class TestChainedHandoff:
    """同一 session 链多次 handoff：worker → h2 → h3 → ..."""

    @staticmethod
    def test_next_handoff_name_first_handoff():
        """首次 handoff：worker → worker-h2"""
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        assert gen.next_handoff_name("my-worker") == "my-worker-h2"

    @staticmethod
    def test_next_handoff_name_second_handoff():
        """第二次 handoff：worker-h2 → worker-h3"""
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        assert gen.next_handoff_name("my-worker-h2") == "my-worker-h3"

    @staticmethod
    def test_next_handoff_name_third_handoff():
        """第三次 handoff：worker-h3 → worker-h4"""
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        assert gen.next_handoff_name("my-worker-h3") == "my-worker-h4"

    @staticmethod
    def test_next_handoff_name_legacy_chained():
        """兼容旧格式：foo-h2-h2 → foo-h2-h3（只增尾部 -hN）"""
        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")
        assert gen.next_handoff_name("foo-h2-h2") == "foo-h2-h3"

    @staticmethod
    def test_second_handoff_from_continuation(monkeypatch, tmp_path):
        """worker-h2 触发第二次 handoff → handoff-2 + continue-2。"""
        worker = _worker_session(name="abs-h2", cwd=str(tmp_path))
        sid_short = worker.id[:8]

        handoff_dir = tmp_path / ".workflow-controller" / "handoff"
        handoff_dir.mkdir(parents=True)
        doc_name = f"{sid_short}-handoff-2-doc"
        done_name = f"{sid_short}-handoff-2-done"
        (handoff_dir / doc_name).write_text("# Handoff 2\ncontent")
        (handoff_dir / done_name).write_text("done")

        agent_sid = uuid.uuid4().hex[:12]
        agent = _handoff_session(agent_sid, f"{sid_short}-handoff-2", cwd=str(tmp_path))

        continuation_name = f"{sid_short}-continue-2"
        new_worker = _worker_session(
            name=continuation_name, cwd=str(tmp_path), sid=uuid.uuid4().hex[:12]
        )
        new_worker.state = SessionState.RUNNING

        gen = HandoffGenerator(skill="test", handoff_filename="handoff.md")

        orch = _make_orchestrator(worker, start_side_effect=[agent, new_worker])
        # 模拟已有 1 个 successor（上一次 handoff）→ handoff_count=2
        orch.event_router.get_successors_sync.return_value = [{"id": "prev-agent-1"}]

        plugin = _armed_pending_plugin(monkeypatch, orch, gen, agent_sid)
        monkeypatch.setattr(plugin, "_find_initial_session", MagicMock(return_value=worker))
        # 模拟这是第二次 handoff（handoff_count=2）
        monkeypatch.setattr(plugin, "_handoff_count", 2, raising=False)

        _stub_handoff_paths(gen, handoff_dir, sid_short, number=2)

        asyncio.run(plugin.finalize_handoff(worker))

        assert agent.result == "success"
        assert orch.start_session_with_config.call_count >= 2
        orch.stop_session.assert_called_once_with(
            worker.id,
            reason="user_stop",
            persist_stopped_state=False,
        )


# ── Test 6: Force handoff 直接触发 ─────────────────────────────────────


class TestForceHandoff:
    """Force 按钮：跳过阈值设 flag，等 boundary 触发 finalize。"""

    @staticmethod
    def test_force_skips_threshold(monkeypatch, tmp_path):
        """Force=true 时，上下文未超阈值也设 _handoff_force_pending。"""
        worker, _orch, plugin = _far_below_threshold_plugin(tmp_path)
        monkeypatch.setattr(
            plugin,
            "_check_context",
            AsyncMock(
                return_value=MagicMock(percentage=1.0, current_tokens=10000, max_tokens=1000000)
            ),
        )

        result = asyncio.run(plugin.request_handoff(worker.id, force=True))
        assert result["handoff_triggered"] is True
        assert getattr(plugin, "_state_for")(worker.id).force_pending is True

    @staticmethod
    def test_no_force_below_threshold_rejected(monkeypatch, tmp_path):
        """force=false 且上下文未超 → 不触发。"""
        worker, _orch, plugin = _far_below_threshold_plugin(tmp_path)
        monkeypatch.setattr(
            plugin,
            "_check_context",
            AsyncMock(
                return_value=MagicMock(percentage=1.0, current_tokens=10000, max_tokens=1000000)
            ),
        )

        result = asyncio.run(plugin.request_handoff(worker.id, force=False))
        assert result["handoff_triggered"] is False

    @staticmethod
    def test_force_flag_picked_up_by_boundary(monkeypatch, tmp_path):
        """Force 设 flag → on_iteration_boundary 检测 → spawn finalize_handoff。"""
        worker, _orch, plugin = _far_below_threshold_plugin(tmp_path)
        plugin.finalize_handoff = AsyncMock()
        monkeypatch.setattr(plugin, "_handoff_finalizing", False)
        monkeypatch.setattr(
            plugin, "_check_context", AsyncMock(return_value=MagicMock(percentage=1.0))
        )

        # Step 1: Force → 设 _handoff_force_pending
        result = asyncio.run(plugin.request_handoff(worker.id, force=True))
        assert result["handoff_triggered"] is True
        assert getattr(plugin, "_state_for")(worker.id).force_pending is True

        # Step 2: on_iteration_boundary 检测 flag → spawn finalize
        gate = asyncio.run(plugin.on_iteration_boundary(worker.id, worker, 1))
        assert plugin.finalize_handoff.called
        assert gate is not None


# ── Test 7: inject prompt 文本输入传递 ──────────────────────────────────


class TestInjectPrompt:
    """文本框输入 → session._manual_prompt_queue 正确入队。"""

    @staticmethod
    def test_inject_prompt_queued_correctly(tmp_path):
        """注入文本后，poll_manual_prompt 应返回该文本。"""
        cfg = SessionConfig(name="test-worker", initial_prompt="测试任务", cwd=str(tmp_path))
        session = Session(session_id="abc123", config=cfg)
        session.state = SessionState.IDLE

        asyncio.run(session.inject_prompt("你好，请告诉我今天星期几"))
        result = asyncio.run(session.poll_manual_prompt())
        assert result == "你好，请告诉我今天星期几"

    @staticmethod
    def test_inject_prompt_queue_empty_after_poll(tmp_path):
        """注入 → poll → 再 poll 应为 None（队列已空）。"""
        cfg = SessionConfig(name="test-worker", initial_prompt="测试", cwd=str(tmp_path))
        session = Session(session_id="abc234", config=cfg)
        session.state = SessionState.IDLE

        asyncio.run(session.inject_prompt("用户手动输入"))
        result = asyncio.run(session.poll_manual_prompt())
        assert result == "用户手动输入"
        second = asyncio.run(session.poll_manual_prompt())
        assert second is None

    @staticmethod
    def test_first_iteration_uses_initial_prompt(monkeypatch):
        """首次启动 iteration=1 → initial_prompt（不走 resume 分支）。"""
        from workflow_controller.core.loop import SingleLoopEngine

        engine = SingleLoopEngine.__new__(SingleLoopEngine)
        monkeypatch.setattr(engine, "_jinja_env", None, raising=False)
        result = getattr(engine, "_render_prompt")(
            initial_prompt="原始worker任务",
            iteration=1,
            feedback=None,
        )
        assert "原始worker任务" in result

    @staticmethod
    def test_resume_without_input_uses_default_prompt():
        """resume 无文本框输入 → "请继续你之前的工作。"。"""
        cfg = SessionConfig(name="test-worker", initial_prompt="测试", cwd="/tmp")
        session = Session(session_id="resume2", config=cfg)
        session.native_session_id = "resume2-native"
        session.state = SessionState.IDLE

        asyncio.run(session.resume())
        # resume 后 loop 侧下一轮 iteration 递增，manual 输入被清空
        manual = asyncio.run(session.poll_manual_prompt())
        assert manual is None  # 没有输入

    @staticmethod
    def test_resume_with_input_uses_text():
        """resume 有文本框输入 → 用输入文本。"""
        cfg = SessionConfig(name="test-worker", initial_prompt="测试", cwd="/tmp")
        session = Session(session_id="resume3", config=cfg)
        session.native_session_id = "resume3-native"
        session.state = SessionState.IDLE

        asyncio.run(session.inject_prompt("告诉我是期几"))
        asyncio.run(session.resume())
        # 第二轮 resume，此前注入的文本作为 prompt
        manual = asyncio.run(session.poll_manual_prompt())
        assert manual == "告诉我是期几"


# ── Test 7: 并行交接互不干扰 ──────────────────────────────────────────


class TestParallelHandoff:
    """两个 worker 同时交接：状态按 session 隔离，链条各自完整。"""

    @staticmethod
    def test_parallel_finalizes_keep_chains_isolated(monkeypatch, tmp_path):
        workers, agents, continuations, handoff_dirs = _parallel_handoff_chains(tmp_path)
        orch, plugin = _armed_parallel_finalize(
            monkeypatch, workers, agents, continuations, handoff_dirs
        )

        async def run_both():
            await asyncio.gather(
                plugin.finalize_handoff(workers[0]),
                plugin.finalize_handoff(workers[1]),
            )

        asyncio.run(run_both())

        assert [a.result for a in agents] == ["success", "success"]
        assert orch.start_session_with_config.call_count == 4
        stopped = sorted(c.args[0] for c in orch.stop_session.call_args_list)
        assert stopped == sorted(w.id for w in workers)
        relations = {
            (c.kwargs["from_session_id"], c.kwargs["to_session_id"])
            for c in orch.event_router.add_handoff_relation.call_args_list
        }
        assert relations == {
            (workers[0].id, agents[0].id),
            (agents[0].id, continuations[0].id),
            (workers[1].id, agents[1].id),
            (agents[1].id, continuations[1].id),
        }
        assert continuations[0].birth_handoff_doc.startswith(workers[0].id[:8])
        assert continuations[1].birth_handoff_doc.startswith(workers[1].id[:8])
