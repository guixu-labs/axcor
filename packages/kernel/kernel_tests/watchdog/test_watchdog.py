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

"""Watchdog 检测规则订阅者与 fault.* 告警事件测试。"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

from workflow_controller.api.watchdog_api import WatchdogAPI
from workflow_controller.config.settings import WatchdogConfig
from workflow_controller.session.events import Event, EventType
from workflow_controller.watchdog.core import (
    FileStallRule,
    KeywordRule,
    NodeTimeoutRule,
    SubagentTimeoutRule,
    WatchdogCore,
)
from workflow_controller.watchdog.fault import fault_event_type
from workflow_controller.watchdog.state import Alert, Severity, make_alert_id


class _FakeBus:
    """记录告警发射，并按事件类型把事件分派回已注册的订阅者。"""

    def __init__(self) -> None:
        self.emitted: list[Event] = []
        self.observers: dict[str, list[Any]] = defaultdict(list)

    def observe(self, event_type: Any, handler: Any) -> None:
        self.observers[getattr(event_type, "value", event_type)].append(handler)

    async def emit(self, event: Event) -> None:
        self.emitted.append(event)

    async def dispatch(self, event: Event) -> None:
        key = getattr(event.event_type, "value", event.event_type)
        for handler in self.observers[key]:
            await handler(event)


class _FakeSession:
    def __init__(self, cwd: str = "", state: str = "running"):
        self.config = MagicMock()
        self.config.cwd = cwd
        self.state = MagicMock()
        self.state.value = state


def _make_rule(rule_cls: Any, cfg: WatchdogConfig, *, cwd: str = "", **extra: Any) -> _FakeBus:
    """构造规则订阅者并接入假总线，返回总线。"""
    bus = _FakeBus()
    pool = MagicMock()
    pool.get.return_value = _FakeSession(cwd)
    rule = rule_cls(cfg, bus, pool, **extra)
    rule.subscribe()
    return bus


def _subagent_event(
    session_id: str,
    phase: str,
    agent_id: str = "ag_1",
    agent_type: str = "tester",
    *,
    run_id: str | None = None,
    context: str | None = None,
) -> Event:
    event = Event.create(
        session_id=session_id,
        iteration=0,
        event_type=EventType[f"SUBAGENT_{phase.upper()}"],
        data={"agent_id": agent_id, "agent_type": agent_type},
    )
    event.run_id = run_id
    event.context = context
    return event


def _node_history_event(
    run_id: str,
    node_id: str,
    status: str,
    *,
    gen: int = 0,
    attempt: int = 1,
    timeout_seconds: int | None = 60,
    session_id: str = "ses-exec",
    verify_session_id: str | None = None,
    when: datetime | None = None,
) -> Event:
    """构造 run 键 node_history 事件（协调器同流总线分发的形状）。"""
    data: dict[str, Any] = {
        "graph_id": "g1",
        "node_id": node_id,
        "attempt": attempt,
        "status": status,
        "workflow_controller_session_id": session_id,
        "gen": gen,
    }
    if verify_session_id is not None:
        data["verify_session_id"] = verify_session_id
    if timeout_seconds is not None:
        data["timeout_seconds"] = timeout_seconds
    return Event(
        id=f"nh-{node_id}-{gen}-{status}-{attempt}",
        session_id="",
        iteration=0,
        timestamp=when or datetime.now(tz=UTC),
        event_type="node_history",
        data=data,
        run_id=run_id,
    )


def _graph_event(run_id: str, event_type: str, when: datetime | None = None) -> Event:
    """构造 run 键图事件（graph_paused / graph_completed / graph_failed）。"""
    return Event(
        id=f"ge-{event_type}-{run_id}",
        session_id="",
        iteration=0,
        timestamp=when or datetime.now(tz=UTC),
        event_type=event_type,
        data={"graph_id": "g1"},
        run_id=run_id,
    )


def _make_node_rule(
    cfg: WatchdogConfig | None = None, **extra: Any
) -> tuple[NodeTimeoutRule, _FakeBus]:
    """构造节点累计超时规则（内存态，无账本），返回 (规则, 总线)。"""
    bus = _FakeBus()
    rule = NodeTimeoutRule(cfg or WatchdogConfig(), bus, MagicMock(), **extra)
    rule.subscribe()
    return rule, bus


# ── 组合根 ─────────────────────────────────────────────────────────────


class TestWatchdogCore:
    @staticmethod
    async def _started_bus(cfg: WatchdogConfig) -> _FakeBus:
        bus = _FakeBus()
        core = WatchdogCore(cfg, bus, MagicMock())
        await core.restore_node_clocks()
        core.start()
        await core.stop()
        return bus

    async def test_start_subscribes_all_rules(self):
        bus = await self._started_bus(WatchdogConfig())
        assert set(bus.observers) == {
            "subagent_start",
            "subagent_progress",
            "subagent_stop",
            "node_history",
            "graph_paused",
            "graph_completed",
            "graph_failed",
        }

    async def test_start_noop_when_disabled(self):
        assert not (await self._started_bus(WatchdogConfig(enabled=False))).observers


# ── 规则 1：subagent 超时 ─────────────────────────────────────────────


class TestSubagentTimeout:
    async def test_alert_via_progress(self):
        bus = _make_rule(SubagentTimeoutRule, WatchdogConfig(subagent_timeout_seconds=60))
        now = datetime.now(tz=UTC)
        start = _subagent_event("ses", "start", agent_type="kernel-developer")
        start.timestamp = now - timedelta(seconds=70)
        await bus.dispatch(start)
        await bus.dispatch(_subagent_event("ses", "progress", agent_type="kernel-developer"))
        assert [e.event_type for e in bus.emitted] == [fault_event_type("subagent_timeout")]
        alert = Alert.from_dict(bus.emitted[0].data)
        assert alert.severity == Severity.WARNING
        assert alert.agent_type == "kernel-developer"
        assert alert.elapsed_seconds >= 70
        assert alert.threshold_seconds == 60

    async def test_no_alert_below_threshold(self):
        bus = _make_rule(SubagentTimeoutRule, WatchdogConfig(subagent_timeout_seconds=60))
        start = _subagent_event("ses", "start")
        start.timestamp = datetime.now(tz=UTC) - timedelta(seconds=30)
        await bus.dispatch(start)
        await bus.dispatch(_subagent_event("ses", "progress"))
        assert not bus.emitted

    async def test_alert_via_stop_and_dedup(self):
        """stop 后备触发；同 call 后续事件不再重复告警。"""
        bus = _make_rule(SubagentTimeoutRule, WatchdogConfig(subagent_timeout_seconds=60))
        start = _subagent_event("ses", "start")
        start.timestamp = datetime.now(tz=UTC) - timedelta(seconds=65)
        await bus.dispatch(start)
        await bus.dispatch(_subagent_event("ses", "stop"))
        await bus.dispatch(_subagent_event("ses", "progress"))  # stop 后迟到尾部
        assert len(bus.emitted) == 1

    async def test_threshold_zero_disabled(self):
        bus = _make_rule(SubagentTimeoutRule, WatchdogConfig(subagent_timeout_seconds=0))
        start = _subagent_event("ses", "start")
        start.timestamp = datetime.now(tz=UTC) - timedelta(seconds=999)
        await bus.dispatch(start)
        await bus.dispatch(_subagent_event("ses", "stop"))
        assert not bus.emitted

    async def test_sweep_alerts_silent_hang_without_progress_or_stop(self):
        """完全静默卡死（无 progress 无 stop）：周期巡检到期照常告警。"""
        bus = _make_rule(SubagentTimeoutRule, WatchdogConfig(subagent_timeout_seconds=60))
        start = _subagent_event("ses", "start")
        start.timestamp = datetime.now(tz=UTC) - timedelta(seconds=70)
        await bus.dispatch(start)
        rule = bus.observers["subagent_start"][0].__self__
        await rule.sweep(datetime.now(tz=UTC))
        assert [e.event_type for e in bus.emitted] == [fault_event_type("subagent_timeout")]

    async def test_sweep_drops_call_of_terminal_parent_session(self):
        """终态检查：父 session 已终态（别的规则结束了工作流）则不再报。"""
        bus = _make_rule(SubagentTimeoutRule, WatchdogConfig(subagent_timeout_seconds=60))
        pool = MagicMock()
        pool.get.return_value = _FakeSession(state="completed")
        rule = SubagentTimeoutRule(WatchdogConfig(subagent_timeout_seconds=60), bus, pool)
        rule.subscribe()
        start = _subagent_event("ses", "start")
        start.timestamp = datetime.now(tz=UTC) - timedelta(seconds=70)
        await bus.dispatch(start)
        await rule.sweep(datetime.now(tz=UTC))
        assert not bus.emitted

    async def test_alert_payload_carries_run_and_node_identity(self):
        """载荷补齐：Run/节点/session/子代理身份、开始时间/实际耗时/阈值。"""
        bus = _make_rule(SubagentTimeoutRule, WatchdogConfig(subagent_timeout_seconds=60))
        start = _subagent_event(
            "ses",
            "start",
            agent_type="kernel-developer",
            run_id="run_9",
            context="node=fix-findings/execute",
        )
        start.timestamp = datetime.now(tz=UTC) - timedelta(seconds=65)
        await bus.dispatch(start)
        await bus.dispatch(
            _subagent_event("ses", "stop", agent_type="kernel-developer", run_id="run_9")
        )
        alert = Alert.from_dict(bus.emitted[0].data)
        assert alert.run_id == "run_9"
        assert alert.node_id == "fix-findings"
        assert alert.session_id == "ses"
        assert alert.agent_id == "ag_1"
        assert alert.started_at == start.timestamp.isoformat()
        assert alert.elapsed_seconds >= 65
        assert alert.threshold_seconds == 60


# ── 规则 2：文件不变 ──────────────────────────────────────────────────


class TestFileStall:
    async def test_alert_at_threshold_refires_after_change(self, tmp_path):
        track = tmp_path / "STATE.md"
        track.write_text("v1")
        bus = _make_rule(
            FileStallRule,
            WatchdogConfig(track_file="STATE.md", file_stall_threshold=2),
            cwd=str(tmp_path),
        )
        await bus.dispatch(_subagent_event("ses", "start"))  # 记基线 hash
        for _ in range(3):
            await bus.dispatch(_subagent_event("ses", "stop"))
        assert len(bus.emitted) == 1
        alert = Alert.from_dict(bus.emitted[0].data)
        assert alert.rule == "file_stall"
        assert alert.stall_count == 2
        await bus.dispatch(_subagent_event("ses", "stop"))  # 仍无变化 → 去重
        assert len(bus.emitted) == 1
        track.write_text("v2")
        await bus.dispatch(_subagent_event("ses", "stop"))  # 变化 → 计数清零
        for _ in range(2):
            await bus.dispatch(_subagent_event("ses", "stop"))
        assert len(bus.emitted) == 2  # 重新累计到阈值再告警

    async def test_no_alert_when_unconfigured_or_disabled(self, tmp_path):
        track = tmp_path / "STATE.md"
        track.write_text("v1")
        for cfg in (
            WatchdogConfig(track_file="", file_stall_threshold=3),
            WatchdogConfig(track_file="STATE.md", file_stall_threshold=0),
        ):
            bus = _make_rule(FileStallRule, cfg, cwd=str(tmp_path))
            for _ in range(5):
                await bus.dispatch(_subagent_event("ses", "stop"))
            assert not bus.emitted


# ── 规则 3：关键词检测 ────────────────────────────────────────────────


class TestKeywordDetection:
    async def test_alert_and_dedup_until_keyword_gone(self, tmp_path):
        f = tmp_path / "DESIGN.md"
        f.write_text("MEMBASE")  # 大小写不敏感
        bus = _make_rule(
            KeywordRule,
            WatchdogConfig(track_keyword_file="DESIGN.md", track_keyword="membase"),
            cwd=str(tmp_path),
        )
        await bus.dispatch(_subagent_event("ses", "stop"))
        assert len(bus.emitted) == 1
        alert = Alert.from_dict(bus.emitted[0].data)
        assert alert.rule == "keyword_detected"
        assert alert.keyword == "membase"
        await bus.dispatch(_subagent_event("ses", "stop"))  # 仍命中 → 去重
        assert len(bus.emitted) == 1
        f.write_text("regbase")  # 关键词消失 → 重置
        await bus.dispatch(_subagent_event("ses", "stop"))
        f.write_text("membase again")  # 再次命中 → 再告警
        await bus.dispatch(_subagent_event("ses", "stop"))
        assert len(bus.emitted) == 2

    async def test_no_alert_when_not_found_file_missing_or_unconfigured(self, tmp_path):
        (tmp_path / "DESIGN.md").write_text("regbase")
        for cfg in (
            WatchdogConfig(track_keyword_file="DESIGN.md", track_keyword="membase"),
            WatchdogConfig(track_keyword_file="no_such.md", track_keyword="membase"),
            WatchdogConfig(track_keyword_file="DESIGN.md", track_keyword=""),
        ):
            bus = _make_rule(KeywordRule, cfg, cwd=str(tmp_path))
            await bus.dispatch(_subagent_event("ses", "stop"))
            assert not bus.emitted


# ── 规则 4：节点累计超时 ─────────────────────────────────────────────


class TestNodeTimeout:
    async def test_stuck_node_fires_on_sweep(self):
        """完全卡住的节点（单一 running 事件后再无事件）巡检到期告警。"""
        rule, bus = _make_node_rule()
        t0 = datetime.now(tz=UTC) - timedelta(seconds=70)
        await bus.dispatch(_node_history_event("run_1", "n1", "running", when=t0))
        await rule.sweep(datetime.now(tz=UTC))
        assert [e.event_type for e in bus.emitted] == [fault_event_type("node_timeout")]

    async def test_node_without_timeout_not_tracked(self):
        """未配时限（timeout_seconds 缺席）的节点不建账、不告警。"""
        rule, bus = _make_node_rule()
        t0 = datetime.now(tz=UTC) - timedelta(seconds=999)
        await bus.dispatch(
            _node_history_event("run_1", "n1", "running", timeout_seconds=None, when=t0)
        )
        await rule.sweep(datetime.now(tz=UTC))
        assert not bus.emitted

    async def test_continuous_events_still_fire_and_payload_complete(self):
        """持续输出（反复 running 刷新）的慢任务照常告警；载荷契约完整。"""
        rule, bus = _make_node_rule()
        t0 = datetime.now(tz=UTC) - timedelta(seconds=100)
        await bus.dispatch(
            _node_history_event("run_1", "n1", "running", attempt=1, when=t0, timeout_seconds=60)
        )
        # 验收相位照发 running（载荷带验收会话）
        await bus.dispatch(
            _node_history_event(
                "run_1",
                "n1",
                "running",
                attempt=1,
                when=t0 + timedelta(seconds=50),
                timeout_seconds=60,
                verify_session_id="ses-verify",
            )
        )
        await rule.sweep(datetime.now(tz=UTC))
        assert len(bus.emitted) == 1
        payload = bus.emitted[0].data
        assert bus.emitted[0].run_id == "run_1"
        assert payload["run_id"] == "run_1"
        assert payload["node_id"] == "n1"
        assert payload["gen"] == 0
        assert payload["attempt"] == 1
        assert payload["session_id"] == "ses-exec"
        assert payload["verify_session_id"] == "ses-verify"
        assert payload["started_at"] == t0.isoformat()
        assert payload["elapsed_seconds"] >= 100
        assert payload["threshold_seconds"] == 60

    async def test_pause_freezes_clock_and_resume_keeps_accumulated(self):
        """暂停不计时（时钟冻结）、续跑不清零（暂停前用时保留）。"""
        rule, bus = _make_node_rule()
        t0 = datetime.now(tz=UTC) - timedelta(seconds=200)
        await bus.dispatch(_node_history_event("run_1", "n1", "running", when=t0))
        t_pause = t0 + timedelta(seconds=50)
        await bus.dispatch(_graph_event("run_1", "graph_paused", when=t_pause))
        # 暂停期内巡检：50s < 60s 不告警
        await rule.sweep(t_pause + timedelta(seconds=30))
        assert not bus.emitted
        # 长暂停后恢复：暂停时长不吃预算，续跑累计继续
        t_resume = t_pause + timedelta(seconds=500)
        await bus.dispatch(_node_history_event("run_1", "n1", "running", when=t_resume))
        await rule.sweep(t_resume + timedelta(seconds=20))
        assert len(bus.emitted) == 1
        assert bus.emitted[0].data["elapsed_seconds"] >= 70

    async def test_same_generation_fires_once(self):
        """同代同调用只报一次：后续事件与巡检不重复告警。"""
        rule, bus = _make_node_rule()
        t0 = datetime.now(tz=UTC) - timedelta(seconds=100)
        await bus.dispatch(_node_history_event("run_1", "n1", "running", when=t0))
        await rule.sweep(datetime.now(tz=UTC))
        await rule.sweep(datetime.now(tz=UTC) + timedelta(seconds=30))
        await bus.dispatch(_node_history_event("run_1", "n1", "running", when=datetime.now(tz=UTC)))
        await rule.sweep(datetime.now(tz=UTC) + timedelta(seconds=60))
        assert len(bus.emitted) == 1

    async def test_new_generation_resets_clock(self):
        """回滚换代重新计时：新 gen 从零累计，旧代用时不清零新代。"""
        rule, bus = _make_node_rule()
        t0 = datetime.now(tz=UTC) - timedelta(seconds=300)
        await bus.dispatch(_node_history_event("run_1", "n1", "running", gen=0, when=t0))
        t_rollback = t0 + timedelta(seconds=120)
        # 回滚落 stopped（旧代冻结），新代 running 重新起步
        await bus.dispatch(_node_history_event("run_1", "n1", "stopped", gen=0, when=t_rollback))
        await bus.dispatch(_node_history_event("run_1", "n1", "running", gen=1, when=t_rollback))
        await rule.sweep(t_rollback + timedelta(seconds=30))
        assert not bus.emitted  # 新代仅 30s，旧代 120s 不带入
        await rule.sweep(t_rollback + timedelta(seconds=70))
        assert len(bus.emitted) == 1
        assert bus.emitted[0].data["gen"] == 1
        assert bus.emitted[0].data["elapsed_seconds"] >= 70

    async def test_terminal_status_freezes_and_never_fires(self):
        """验收结束（passed/failed/skipped/stopped）冻结时钟，终态不再报。"""
        rule, bus = _make_node_rule()
        t0 = datetime.now(tz=UTC) - timedelta(seconds=500)
        await bus.dispatch(_node_history_event("run_1", "n1", "running", when=t0))
        await bus.dispatch(
            _node_history_event("run_1", "n1", "passed", when=t0 + timedelta(seconds=10))
        )
        await rule.sweep(datetime.now(tz=UTC))
        assert not bus.emitted

    async def test_graph_terminal_freezes_all(self):
        """图终态（failed/completed）冻结该 run 全部时钟。"""
        rule, bus = _make_node_rule()
        t0 = datetime.now(tz=UTC) - timedelta(seconds=500)
        await bus.dispatch(_node_history_event("run_1", "n1", "running", when=t0))
        await bus.dispatch(
            _node_history_event(
                "run_2", "n2", "running", when=t0, session_id="ses-2", timeout_seconds=None
            )
        )
        await bus.dispatch(_graph_event("run_1", "graph_failed", when=t0 + timedelta(seconds=5)))
        await rule.sweep(datetime.now(tz=UTC))
        assert not bus.emitted  # run_1 被图终态冻结；run_2 未配时限不建账

    async def test_node_history_without_run_id_ignored(self):
        """无 run 归属的档案事件（异常形态）不建账。"""
        rule, bus = _make_node_rule()
        event = _node_history_event("", "n1", "running", when=datetime.now(tz=UTC))
        event.run_id = None
        await bus.dispatch(event)
        await rule.sweep(datetime.now(tz=UTC) + timedelta(seconds=120))
        assert not bus.emitted

    async def test_restart_preserves_accumulated_via_ledger(self, tmp_path):
        """重启保留已用时间：账本行往返，崩溃开段按冻结收段。"""
        from workflow_controller.core.runs import RunStore
        from workflow_controller.watchdog.ledger import NodeClockLedger

        db_path = tmp_path / "events.db"
        await RunStore(db_path).list_runs()  # 统一迁移铺表
        ledger = NodeClockLedger(db_path)

        rule, _ = _make_node_rule(ledger=ledger)
        t0 = datetime.now(tz=UTC) - timedelta(seconds=200)
        await rule.on_node_history(
            _node_history_event("run_1", "n1", "running", when=t0, timeout_seconds=200)
        )
        await rule.sweep(t0 + timedelta(seconds=100))  # 滚动落账：累计 100s、开段前移
        ledger.close()

        # 重启：新规则实例从账本恢复，崩溃开段冻结（宕机时长不计入）
        rule2, bus2 = _make_node_rule(ledger=NodeClockLedger(db_path))
        await rule2.restore()
        t_resume = t0 + timedelta(seconds=5000)
        await rule2.on_node_history(
            _node_history_event("run_1", "n1", "running", when=t_resume, timeout_seconds=200)
        )
        await rule2.sweep(t_resume + timedelta(seconds=110))
        assert len(bus2.emitted) == 1
        payload = bus2.emitted[0].data
        # 100s 落账保留 + 续跑 110s；重启间隔（数千秒）不吃预算
        assert 205 <= payload["elapsed_seconds"] < 400


# ── 查询面：fault.* 解码 ──────────────────────────────────────────────


class TestWatchdogAPI:
    @staticmethod
    def test_list_alerts_decodes_fault_events_only():
        alert = Alert(
            alert_id=make_alert_id(),
            rule="file_stall",
            severity=Severity.WARNING,
            session_id="ses_x",
            message="test",
            stall_count=3,
        )
        fault_ev = Event.create(
            session_id="ses_x",
            iteration=0,
            event_type=fault_event_type("file_stall"),
            data=alert.to_dict(),
        )
        other_ev = Event.create(
            session_id="ses_x", iteration=0, event_type=EventType.LOG_MANUAL, data={"prompt": "hi"}
        )
        router = MagicMock()
        router.get_events_sync.return_value = [fault_ev, other_ev]
        assert WatchdogAPI(router).list_alerts("ses_x") == [alert.to_dict()]

    @staticmethod
    def test_list_alerts_skips_malformed_fault_payload():
        bad = Event.create(
            session_id="ses_x",
            iteration=0,
            event_type=fault_event_type("subagent_timeout"),
            data={"severity": 5},
        )
        router = MagicMock()
        router.get_events_sync.return_value = [bad]
        assert WatchdogAPI(router).list_alerts("ses_x") == []
