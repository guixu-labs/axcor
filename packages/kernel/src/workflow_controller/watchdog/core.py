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

"""Watchdog 检测规则 —— 事件库订阅者，命中条件发 fault.* 告警。

四条规则：
1. subagent 超时：start 记录开始时间，progress/stop/周期巡检检查 elapsed，
   超阈值告警（无论是否持续输出日志，到期检查都执行）。
2. 文件不变：每个 subagent stop 后读 track_file 内容 hash，跟上一次 stop 时比，
   连续 N 次无变化 → 告警。不依赖 git，纯文件 hash 比较。
3. 关键词检测：subagent stop 后查 track_keyword_file 是否含 track_keyword。
4. 节点累计超时：node_history/graph_* 驱动事件驱动时钟迁移 + 周期巡检到期
   检查，本代累计超时限 → fault.node_timeout（run 键事件）。

设计要点：
- 周期巡检由 WatchdogCore 单一任务承担（15s 一拍、逐规则 sweep），规则侧
  不自持 task；事件回调内的检查是机会性快路径。
- 告警事件经 fault.py 编码进事件库（events/run_events 表持久化 + SSE 透传），
  前端按 fault. 前缀过滤。
- 节点是编排概念：节点身份经驱动事件载荷透传，本模块不 import 编排插件。
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from workflow_controller.session.events import Event, EventType
from workflow_controller.watchdog.fault import emit_fault_event, emit_run_fault_event
from workflow_controller.watchdog.ledger import NodeClockLedger
from workflow_controller.watchdog.state import (
    Alert,
    CallRecord,
    NodeClock,
    Severity,
    make_alert_id,
)

if TYPE_CHECKING:
    from workflow_controller.config.settings import WatchdogConfig
    from workflow_controller.event.event_bus import EventBus
    from workflow_controller.session.manager import SessionPool

log = logging.getLogger(__name__)

# 周期巡检间隔：不依赖事件的到期检查拍长（崩溃残留开段的最大损失上界）
SWEEP_INTERVAL_S = 15.0

# 节点计时段迁移的节点档案状态：running 起步/续跑，其余冻结
_NODE_RUNNING_STATUS = "running"
_NODE_FROZEN_STATUSES = frozenset({"stopped", "passed", "failed", "skipped"})

# 父 session 终态集：巡检时 call 记录的清理判据（终态不再报）
_PARENT_TERMINAL_STATES = frozenset({"completed", "stopped", "failed"})

# 驱动事件词面（run 键、经协调器同流总线分发）
NODE_HISTORY_EVENT = "node_history"
GRAPH_PAUSED_EVENT = "graph_paused"
GRAPH_TERMINAL_EVENTS = ("graph_completed", "graph_failed")


def _node_id_of_context(context: str | None) -> str:
    """graph 成员 context（node=<id>/execute）解析节点身份；非节点成员为空。"""
    if not context or not context.startswith("node="):
        return ""
    return context.removeprefix("node=").partition("/")[0]


def _coerce_int(raw: Any) -> int | None:
    """载荷值 → int；缺失或非法返回 None。"""
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


class DetectionRule:
    """检测规则订阅者基类：订阅事件总线，命中条件时发 fault.<rule> 告警。"""

    rule = ""

    def __init__(self, config: WatchdogConfig, bus: EventBus, pool: SessionPool) -> None:
        self._config = config
        self._bus = bus
        self._pool = pool

    @staticmethod
    def _hash_file(file_path: Path) -> str | None:
        """读文件内容算 md5 hash。文件不可读返回 None（视为"无变化"——文件丢了也算卡住）。"""
        try:
            return hashlib.md5(file_path.read_bytes(), usedforsecurity=False).hexdigest()
        except OSError:
            return None

    def subscribe(self) -> None:
        """向事件总线注册本规则的事件订阅（子类覆写，需保持实例方法签名）。"""
        raise NotImplementedError(f"{type(self).__name__}.subscribe() is not implemented")

    async def sweep(self, now: datetime) -> None:
        """周期巡检拍：不依赖事件的到期检查（默认无巡检语义）。"""

    async def _emit_alert(self, alert: Alert) -> None:
        """发 fault 告警事件；失败只记日志，不影响后续回调。"""
        try:
            await emit_fault_event(self._bus, alert)
            log.warning("watchdog alert fired: %s", alert.message)
        except Exception:
            log.exception("watchdog failed to submit alert")

    def _resolve_file_path(self, session_id: str, file_name: str) -> Path | None:
        """相对 session cwd 解析追踪文件路径。"""
        session = self._pool.get(session_id)
        if session is None:
            return None
        cwd = session.config.cwd or "."
        return (Path(cwd) / file_name).resolve()


class SubagentTimeoutRule(DetectionRule):
    """规则 1：subagent 运行超过阈值 → fault.subagent_timeout。"""

    rule = "subagent_timeout"

    def __init__(self, config: WatchdogConfig, bus: EventBus, pool: SessionPool) -> None:
        super().__init__(config, bus, pool)
        # (session_id, agent_id) -> CallRecord
        self._active_calls: dict[tuple[str, str], CallRecord] = {}

    def subscribe(self) -> None:
        self._bus.observe(EventType.SUBAGENT_START, self.on_start)
        self._bus.observe(EventType.SUBAGENT_PROGRESS, self.on_progress)
        self._bus.observe(EventType.SUBAGENT_STOP, self.on_stop)

    async def on_start(self, event: Event) -> None:
        """建 CallRecord，记录开始时间与 run/node 归属（事件打戳）。"""
        if not self._config.enabled:
            return
        data = event.data or {}
        agent_id = data.get("agent_id", "")
        if not agent_id:
            return
        self._active_calls[(event.session_id, agent_id)] = CallRecord(
            session_id=event.session_id,
            agent_id=agent_id,
            agent_type=data.get("agent_type", ""),
            started_at=event.timestamp,
            run_id=event.run_id or "",
            node_id=_node_id_of_context(event.context),
        )
        log.debug(
            "watchdog track start: %s/%s for %s",
            data.get("agent_type", ""),
            agent_id[:8],
            event.session_id[:8],
        )

    async def on_progress(self, event: Event) -> None:
        """运行中超时检查（progress 不规律，仅机会性）。"""
        if not self._config.enabled:
            return
        data = event.data or {}
        agent_id = data.get("agent_id", "")
        if not agent_id:
            return
        rec = self._active_calls.get((event.session_id, agent_id))
        if rec is None:
            # 该 call 已 stop（rec 已清）→ poller 在 stop 之后才推的迟到尾部，
            # 不重建记录（否则 elapsed 永远≈0，测不到超时），直接忽略。
            return
        await self._check_timeout(rec, event.timestamp)

    async def on_stop(self, event: Event) -> None:
        """超时检查的后备路径：progress 错过的在 stop 时一定能查到。"""
        if not self._config.enabled:
            return
        data = event.data or {}
        agent_id = data.get("agent_id", "")
        if not agent_id:
            return
        rec = self._active_calls.pop((event.session_id, agent_id), None)
        if rec is not None:
            await self._check_timeout(rec, event.timestamp)

    async def sweep(self, now: datetime) -> None:
        """周期巡检：静默卡死（无 progress 也无 stop）的 call 到期照常告警。

        先做终态检查——父 session 已终态或不在池内的记录直接清掉
        （工作流已被别的路径结束则不再报）。
        """
        if not self._config.enabled or self._config.subagent_timeout_seconds <= 0:
            return
        for key, rec in list(self._active_calls.items()):
            if self._parent_ended(rec.session_id):
                self._active_calls.pop(key, None)
                continue
            await self._check_timeout(rec, now)

    def _parent_ended(self, session_id: str) -> bool:
        """父 session 终态/缺失判定（巡检清据用）。"""
        session = self._pool.get(session_id)
        if session is None:
            return True
        return session.state.value in _PARENT_TERMINAL_STATES

    async def _check_timeout(self, rec: CallRecord, now: datetime) -> None:
        threshold = self._config.subagent_timeout_seconds
        if threshold <= 0:
            return  # 0 = 关闭该规则
        if self.rule in rec.fired_alerts:
            return  # 同一 call 只发一次

        elapsed = int((now - rec.started_at).total_seconds())
        if elapsed < threshold:
            return

        alert = Alert(
            alert_id=make_alert_id(),
            rule=self.rule,
            severity=Severity.WARNING,
            session_id=rec.session_id,
            message=f"subagent '{rec.agent_type}' 运行超时，agent_id={rec.agent_id[:12]}",
            agent_id=rec.agent_id,
            agent_type=rec.agent_type,
            run_id=rec.run_id,
            node_id=rec.node_id,
            started_at=rec.started_at.isoformat(),
            elapsed_seconds=elapsed,
            threshold_seconds=threshold,
        )
        rec.fired_alerts.add(self.rule)
        await self._emit_alert(alert)


class FileStallRule(DetectionRule):
    """规则 2：连续 N 个 subagent 结束后追踪文件无变化 → fault.file_stall。"""

    rule = "file_stall"

    def __init__(self, config: WatchdogConfig, bus: EventBus, pool: SessionPool) -> None:
        super().__init__(config, bus, pool)
        # session_id -> 基线文件 hash（第一个 subagent start 前的文件内容）
        self._baseline_hashes: dict[str, str | None] = {}
        # session_id -> 上一次 subagent stop 时的文件 hash（用于连续比较）
        self._last_hashes: dict[str, str | None] = {}
        # session_id -> 连续未变化计数
        self._stall_counts: dict[str, int] = {}
        # session_id -> 本轮是否已告警（文件变化后重置）
        self._fired: dict[str, bool] = {}

    def subscribe(self) -> None:
        self._bus.observe(EventType.SUBAGENT_START, self.on_start)
        self._bus.observe(EventType.SUBAGENT_STOP, self.on_stop)

    async def on_start(self, event: Event) -> None:
        """第一个 subagent start 前记录追踪文件基线 hash（后续 start 不覆盖）。"""
        if not self._config.enabled:
            return
        data = event.data or {}
        if not data.get("agent_id"):
            return
        track_file = self._config.track_file
        if not track_file:
            return
        file_path = self._resolve_file_path(event.session_id, track_file)
        if file_path is not None:
            self._baseline_hashes.setdefault(event.session_id, self._hash_file(file_path))

    async def on_stop(self, event: Event) -> None:
        if not self._config.enabled:
            return
        data = event.data or {}
        if not data.get("agent_id"):
            return
        await self._check_stall(event.session_id, data.get("agent_type", ""))

    async def _check_stall(self, session_id: str, agent_type: str) -> None:
        """每次 stop 与上次 stop 的 hash 比较，相同则计数 +1，达到阈值告警。"""
        track_file = self._config.track_file
        threshold = self._config.file_stall_threshold
        if not track_file or threshold <= 0:
            return  # 未配置或关闭

        file_path = self._resolve_file_path(session_id, track_file)
        if file_path is None:
            return  # 找不到 session cwd，跳过
        current_hash = self._hash_file(file_path)

        # 第一次 stop 无上次 hash，用基线做比较
        prev_hash = self._last_hashes.get(session_id)
        if prev_hash is None:
            prev_hash = self._baseline_hashes.get(session_id)
        if prev_hash is not None and current_hash == prev_hash:
            self._stall_counts[session_id] = self._stall_counts.get(session_id, 0) + 1
        else:
            self._stall_counts[session_id] = 0
            self._fired[session_id] = False
        self._last_hashes[session_id] = current_hash

        count = self._stall_counts[session_id]
        if count >= threshold and not self._fired.get(session_id, False):
            alert = Alert(
                alert_id=make_alert_id(),
                rule=self.rule,
                severity=Severity.WARNING,
                session_id=session_id,
                message=f"连续 {count} 个 subagent 结束后 {track_file} 无变化",
                agent_type=agent_type,
                file_path=str(file_path),
                stall_count=count,
            )
            self._fired[session_id] = True
            await self._emit_alert(alert)


class KeywordRule(DetectionRule):
    """规则 3：subagent 结束后追踪文件中出现关键词 → fault.keyword_detected。"""

    rule = "keyword_detected"

    def __init__(self, config: WatchdogConfig, bus: EventBus, pool: SessionPool) -> None:
        super().__init__(config, bus, pool)
        # session_id -> 是否已告警（关键词消失后重置）
        self._fired: dict[str, bool] = {}

    def subscribe(self) -> None:
        self._bus.observe(EventType.SUBAGENT_STOP, self.on_stop)

    async def on_stop(self, event: Event) -> None:
        if not self._config.enabled:
            return
        data = event.data or {}
        if not data.get("agent_id"):
            return
        await self._check_keyword(event.session_id, data.get("agent_type", ""))

    async def _check_keyword(self, session_id: str, agent_type: str) -> None:
        keyword_file = self._config.track_keyword_file
        keyword = self._config.track_keyword
        if not keyword_file or not keyword:
            return  # 未配置或关闭

        file_path = self._resolve_file_path(session_id, keyword_file)
        if file_path is None:
            return
        try:
            content = file_path.read_text(errors="replace")
        except OSError:
            return  # 文件不存在，不告警

        found = keyword.lower() in content.lower()
        if found and not self._fired.get(session_id, False):
            alert = Alert(
                alert_id=make_alert_id(),
                rule=self.rule,
                severity=Severity.WARNING,
                session_id=session_id,
                message=f"在 {keyword_file} 中发现关键词 '{keyword}'",
                agent_type=agent_type,
                file_path=str(file_path),
                keyword=keyword,
            )
            self._fired[session_id] = True
            await self._emit_alert(alert)
        elif not found:
            self._fired[session_id] = False


class NodeTimeoutRule(DetectionRule):
    """规则 4：节点本代累计耗时超时限 → fault.node_timeout（run 键事件）。

    事实源是驱动 run 键事件（协调器同流总线分发）：node_history 迁移计时
    段（running 起步/续跑、终态与 stopped 冻结、更大 gen 冻结旧代），
    graph_paused / graph 终态冻结该 run 全部时钟。时限随 node_history 载荷
    透传（工作流 YAML timeouts 段解析值），无时限的节点不建账。
    """

    rule = "node_timeout"

    def __init__(
        self,
        config: WatchdogConfig,
        bus: EventBus,
        pool: SessionPool,
        ledger: NodeClockLedger | None = None,
    ) -> None:
        super().__init__(config, bus, pool)
        self._ledger = ledger
        # (run_id, node_id, gen) -> NodeClock
        self._clocks: dict[tuple[str, str, int], NodeClock] = {}

    def subscribe(self) -> None:
        self._bus.observe(NODE_HISTORY_EVENT, self.on_node_history)
        self._bus.observe(GRAPH_PAUSED_EVENT, self.on_graph_paused)
        for event_type in GRAPH_TERMINAL_EVENTS:
            self._bus.observe(event_type, self.on_graph_terminal)

    def close_ledger(self) -> None:
        """释放账本连接（组合根关停入口）。"""
        if self._ledger is not None:
            self._ledger.close()

    async def restore(self) -> None:
        """启动恢复：读回账本存留行；开段（崩溃残留）按冻结收段。"""
        if self._ledger is None:
            return
        for clock in await self._ledger.load_all():
            clock.running_since = None
            self._clocks[(clock.run_id, clock.node_id, clock.gen)] = clock

    async def on_node_history(self, event: Event) -> None:
        """节点档案事件：计时段迁移 + 会话/时限/attempt 载荷刷新。"""
        if not self._config.enabled:
            return
        run_id = event.run_id or ""
        if not run_id:
            return
        data = event.data or {}
        node_id = str(data.get("node_id", "") or "")
        if not node_id:
            return
        try:
            gen = int(data.get("gen", 0) or 0)
        except (TypeError, ValueError):
            return
        status = str(data.get("status", "") or "")
        attempt = _coerce_int(data.get("attempt"))
        threshold = _coerce_int(data.get("timeout_seconds"))
        session_id = str(data.get("workflow_controller_session_id", "") or "")
        verify_session_id = str(data.get("verify_session_id", "") or "")

        if status == _NODE_RUNNING_STATUS:
            await self._freeze_older_generations(run_id, node_id, gen, event.timestamp)
            clock = self._clocks.get((run_id, node_id, gen))
            if clock is None:
                if threshold is None or threshold <= 0:
                    return  # 本节点未配时限：不建账（规则对其关闭）
                clock = NodeClock(
                    run_id=run_id,
                    node_id=node_id,
                    gen=gen,
                    threshold_seconds=threshold,
                    started_at=event.timestamp,
                    running_since=event.timestamp,
                )
                self._clocks[(run_id, node_id, gen)] = clock
            elif clock.running_since is None:
                clock.running_since = event.timestamp  # 续跑不清零
            if threshold is not None and threshold > 0:
                clock.threshold_seconds = threshold
            clock.attempt = attempt if attempt is not None else clock.attempt
            clock.session_id = session_id or clock.session_id
            clock.verify_session_id = verify_session_id or clock.verify_session_id
            await self._check_clock(clock, event.timestamp)
            await self._persist(clock)
            return

        if status in _NODE_FROZEN_STATUSES:
            await self._freeze(run_id, node_id, gen, event.timestamp)

    async def on_graph_paused(self, event: Event) -> None:
        """暂停不计时：该 run 全部开段时钟冻结。"""
        await self._freeze_run(event.run_id or "", event.timestamp)

    async def on_graph_terminal(self, event: Event) -> None:
        """图终态（完成/失败）：该 run 全部时钟冻结，终态不再报。"""
        await self._freeze_run(event.run_id or "", event.timestamp)

    async def sweep(self, now: datetime) -> None:
        """周期巡检：开段落账（崩溃损失 ≤ 一拍）+ 到期检查。"""
        if not self._config.enabled:
            return
        for clock in list(self._clocks.values()):
            if clock.running_since is None or clock.fired:
                continue
            await self._check_clock(clock, now)
            await self._rollover(clock, now)

    async def _check_clock(self, clock: NodeClock, now: datetime) -> None:
        """到期检查：只在开段（运行中）且未报过时告警（终态检查）。"""
        if clock.fired or clock.running_since is None:
            return
        elapsed_exact = clock.elapsed_seconds_exact(now)
        if elapsed_exact < clock.threshold_seconds:
            return
        clock.fired = True
        clock.accumulated_seconds = elapsed_exact
        clock.running_since = None
        try:
            await emit_run_fault_event(
                self._bus,
                self.rule,
                {
                    "rule": self.rule,
                    "run_id": clock.run_id,
                    "node_id": clock.node_id,
                    "gen": clock.gen,
                    "attempt": clock.attempt,
                    "session_id": clock.session_id,
                    "verify_session_id": clock.verify_session_id,
                    "started_at": clock.started_at.isoformat(),
                    "elapsed_seconds": int(elapsed_exact),
                    "threshold_seconds": clock.threshold_seconds,
                },
            )
            log.warning(
                "watchdog alert fired: node %s (gen %s) 累计 %ss 超时限 %ss",
                clock.node_id,
                clock.gen,
                int(elapsed_exact),
                clock.threshold_seconds,
            )
        except Exception:
            log.exception("watchdog failed to submit node timeout alert")
        await self._persist(clock)

    async def _freeze(
        self,
        run_id: str,
        node_id: str,
        gen: int,
        now: datetime,
    ) -> None:
        """单键冻结：开段并入累计。"""
        clock = self._clocks.get((run_id, node_id, gen))
        if clock is None:
            return
        if clock.running_since is not None:
            clock.accumulated_seconds = clock.elapsed_seconds_exact(now)
            clock.running_since = None
            await self._persist(clock)

    async def _freeze_run(self, run_id: str, now: datetime) -> None:
        """整 run 冻结（暂停/终态）。"""
        if not run_id:
            return
        for clock in list(self._clocks.values()):
            if clock.run_id == run_id and clock.running_since is not None:
                clock.accumulated_seconds = clock.elapsed_seconds_exact(now)
                clock.running_since = None
                await self._persist(clock)

    async def _freeze_older_generations(
        self,
        run_id: str,
        node_id: str,
        gen: int,
        now: datetime,
    ) -> None:
        """更大 gen 出现：旧代时钟一律冻结（回滚换代的兜底收口）。"""
        for clock in list(self._clocks.values()):
            if (
                clock.run_id == run_id
                and clock.node_id == node_id
                and clock.gen < gen
                and clock.running_since is not None
            ):
                clock.accumulated_seconds = clock.elapsed_seconds_exact(now)
                clock.running_since = None
                await self._persist(clock)

    async def _rollover(self, clock: NodeClock, now: datetime) -> None:
        """开段滚动落账：计时段起点前移到本拍（崩溃损失 ≤ 一拍）。"""
        if clock.running_since is None:
            return
        clock.accumulated_seconds = clock.elapsed_seconds_exact(now)
        clock.running_since = now
        await self._persist(clock)

    async def _persist(self, clock: NodeClock) -> None:
        """账本落行（无账本配置时跳过——重启保留已用时间不生效）。"""
        if self._ledger is None:
            return
        try:
            await self._ledger.save(clock, datetime.now(tz=UTC).isoformat())
        except Exception:
            log.exception("watchdog node clock ledger save failed")


class WatchdogCore:
    """watchdog 组合根：构造检测规则订阅者并在启动时接入事件总线。

    生命周期：orchestrator.start() 里构造 + start()；进程生命周期内常驻，
    shutdown() 停周期巡检。配置热更新：每次回调现读 config（host.config.watchdog
    直接引用，update_config 改的是同一对象，下一回调即生效）。
    """

    def __init__(
        self,
        config: WatchdogConfig,
        bus: EventBus,
        pool: SessionPool,
        db_path: Path | None = None,
    ) -> None:
        self._config = config
        ledger = NodeClockLedger(db_path) if db_path is not None else None
        self._node_rule = NodeTimeoutRule(config, bus, pool, ledger=ledger)
        self._rules: list[DetectionRule] = [
            SubagentTimeoutRule(config, bus, pool),
            FileStallRule(config, bus, pool),
            KeywordRule(config, bus, pool),
            self._node_rule,
        ]
        self._sweep_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """各规则接入事件总线并起周期巡检。由 orchestrator.start() 调用。"""
        if not self._config.enabled:
            return
        for rule in self._rules:
            rule.subscribe()
        self._sweep_task = asyncio.create_task(self._sweep_loop())
        log.info("watchdog installed (subagent_timeout=%ds)", self._config.subagent_timeout_seconds)

    async def restore_node_clocks(self) -> None:
        """节点时钟账本恢复（start 之前调用：加载存留行并冻结崩溃开段）。"""
        await self._node_rule.restore()

    async def stop(self) -> None:
        """停周期巡检并释放账本连接。由 orchestrator.shutdown() 调用。"""
        task, self._sweep_task = self._sweep_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._node_rule.close_ledger()

    async def _sweep_loop(self) -> None:
        """周期巡检：固定拍长逐规则 sweep。"""
        while True:
            await asyncio.sleep(SWEEP_INTERVAL_S)
            now = datetime.now(tz=UTC)
            for rule in self._rules:
                try:
                    await rule.sweep(now)
                except Exception:
                    log.exception("watchdog sweep failed for rule %s", rule.rule)
