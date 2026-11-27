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

"""RunCoordinator——run 平面的协调组件簇。

run 生命周期、成员编排与决策循环的宿主。对上提供 run 锚定的操作面；
对下只经 Session Orchestrator 原语操作成员；驱动在侧翼经 SDK 协议
（动作/通知）交互，其方法调用全部发生在所属 run 的任务内。

组件按变更轴拆分，防单类膨胀：本模块 = 本体（装配 + 决策循环 + 任务
注册表）+ _GroupControlEngine / _AdmissionGate / _TerminalSequencer /
_RecoveryService（模块内部实现类，`_` 前缀私有命名、
无模块外消费者；可见性与 coord._ 耦合棘轮由架构测试钉死）。
RunHost（成员执行服务面，公共命名 + 显式构造注入）住 core/run_host.py，
此处保留兼容导出。导出实现住 core/run_export.py，export_run 惰性装配、不设组件类。
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import hashlib
import json
import logging
import shutil
import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import yaml

from workflow_controller.core.checkpoints import (
    BarrierQuality,
    CheckpointStore,
    CheckpointTrigger,
    DispatchAnchor,
    checkpoint_to_dict,
)
from workflow_controller.core.restore.conversation import (
    ConversationRestorePlan,
    ConversationRestorer,
    TranscriptLocator,
)
from workflow_controller.core.run_export import RunExportService
from workflow_controller.core.run_host import KILL_GRACE_S as KILL_GRACE_S
from workflow_controller.core.run_host import STOP_DRAIN_TIMEOUT_S as STOP_DRAIN_TIMEOUT_S
from workflow_controller.core.run_host import RunHost
from workflow_controller.core.runs import (
    MemberResumeDeniedError,
    RunKind,
    RunNotFoundError,
    RunStatus,
    SessionNotInRunError,
)
from workflow_controller.core.status_maps import (
    TERMINAL_STATUSES,
    implicit_projection_status,
    next_pause_gear,
)
from workflow_controller.core.workspace import (
    ScopeKind,
    ScopeState,
)
from workflow_controller.core.workspace.actions import (
    AbandonScope as CoreAbandonScope,
)
from workflow_controller.core.workspace.actions import (
    IntegrateScope as CoreIntegrateScope,
)
from workflow_controller.core.workspace.actions import (
    PrepareScopeWorkspace as CorePrepareScopeWorkspace,
)
from workflow_controller.core.workspace.actions import (
    ScopeIntegrated as CoreScopeIntegrated,
)
from workflow_controller.core.workspace.actions import (
    ScopeIntegrationRejected as CoreScopeIntegrationRejected,
)
from workflow_controller.core.workspace.actions import (
    ScopeWorkspaceSpec as CoreScopeWorkspaceSpec,
)
from workflow_controller.core.workspace.claims import (
    WORKSPACE_CLAIM_REJECTED_EVENT,
    ClaimRejectedError,
)
from workflow_controller.core.workspace.dispatch_groups import (
    WORKSPACE_READER,
    ChildDispatchRequest,
    ReplayRejectedError,
)
from workflow_controller.core.workspace.manager import RetractTarget, ScopeQuiesceFailedError
from workflow_controller.core.workspace.tree_runtime import (
    PendingSubagentRestore,
    WorkspaceTreeRuntime,
    load_tree_config,
)
from workflow_controller.driver.hooks import BarrierDeny, BarrierOutcome, BarrierProceed
from workflow_controller.plugin.dispatch_barrier import (
    dispatch_capabilities_for,
    group_arbitration_block_reason,
    writer_dispatch_block_reason,
)
from workflow_controller.plugin.run_driver import (
    START_LIKE_ACTIONS,
    AllSessionsTerminal,
    ArmTimer,
    AwaitSessions,
    CancelTimer,
    ControlKind,
    ControlNotification,
    DiscardSession,
    DispatchBarrierRequest,
    EmitEvent,
    FinishRun,
    KillProcesses,
    OpRequest,
    PauseRun,
    PersistDriverState,
    ReadArtifact,
    ReplaySessionOutcome,
    RestartSession,
    RunAction,
    RunNotification,
    RunOccupiedError,
    RunSessionInfo,
    RunWorld,
    SessionActivated,
    SessionControlApplied,
    SessionOutcomeNotification,
    SessionOutcomeReplayed,
    StartProcess,
    StartSession,
    SubagentResultRequest,
)
from workflow_controller.plugin.workspace import (
    AbandonScope,
    CommitScopeTerminal,
    IntegrateScope,
    IntegrationLineRetracted,
    PrepareScopeWorkspace,
    RestoreCommitted,
    RestoreScope,
    RestoreSessionBound,
    RetractIntegrationLine,
    ScopeAbandoned,
    ScopeIntegrated,
    ScopeIntegrationRejected,
    ScopeRestored,
    ScopeTerminalCommitted,
    ScopeWorkspacePrepared,
    WorkspaceActionFailed,
    WorkspaceScopeKind,
    WorkspaceTreeConfig,
)
from workflow_controller.session.events import Event, EventType, SessionOutcome
from workflow_controller.transcript.sources import resolve_claude_session_path
from workflow_controller.watchdog.fault import (
    SCOPE_QUIESCE_FAILED_RULE,
    WORKSPACE_CONFLICT_RULE,
    fault_event_type,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping

    from workflow_controller.core.orchestrator import Orchestrator, SessionFacts
    from workflow_controller.core.runs import RunRecord, RunStore, SessionRunAffiliationRecord
    from workflow_controller.core.session_run_affiliation import (
        SessionRunAffiliation,
        SessionRunAffiliationService,
    )
    from workflow_controller.core.workspace.integration import IntegrationConflict
    from workflow_controller.core.workspace.manager import BackgroundDrainHook
    from workflow_controller.core.workspace.scope_tree import ScopeRecord
    from workflow_controller.event.event_bus import EventBus
    from workflow_controller.plugin.run_driver import RunDriver, RunDriverFactory

logger = logging.getLogger(__name__)

DECIDE_TIMEOUT_S = 60.0  # 单轮驱动调用（decide/snapshot 等）的超时上界
_OP_DRIVER_WAIT_S = (
    75.0  # 操作通道软调上界：高于 decide 守护（60s），慢操作的真实落点先于本层超时回执
)
TASK_CANCEL_EXIT_TIMEOUT_S = (
    5.0  # cancel 后等 run 任务退出的上界：取消可能被收尾路径吸收（任务不退出），关停不随之挂死
)
SURROGATE_THREAD_DRAIN_S = 2.0  # 代办快照前等在途驱动线程退出的上界（保证单写者）
SURROGATE_SNAPSHOT_TIMEOUT_S = 10.0  # 代办快照专用超时：宽于投影短超时（大状态快照需数秒）、紧于 decide 快照超时（不因卡死驱动拖住关停）
OP_REPLY_TIMEOUT_S = 30.0  # OpRequest 应答通道的通用等待上界
STATE_PROJECTION_TIMEOUT_S = 2.0  # /state 投影请求短超时（忙时降级读盘上快照）
_FINAL_PROJECTION_TIMEOUT_S = (
    10.0  # 收尾投影档：与代办快照同界（宽于投影短超时、紧于 decide 守护），不因卡死驱动拖住任务退出
)
_DECIDE_POOL_PER_FORMAT = 2  # 每 format 分片的 decide 线程池 worker 数
_FROZEN_ENTRY_MARKER = (
    ".workflow-controller-entry"  # 冻结面入口清单文件名（内容 = 入口程序文件名一行）
)
_LAYR_CONFIG_NAME = (
    "layr.yaml"  # layr 程序的组件面配置（从程序目录向上找，同名同序镜像 layr 侧 find_layr_config）
)


def _json_loads_or_none(text: str) -> Any:
    """解析 JSON 字符串；空串或坏输入返回 None。"""
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _find_layr_config(start_dir: Path) -> Path | None:
    """从目录向上逐级找最近的 layr.yaml（与 layr 侧的查找结构一致）。"""
    for cand in (start_dir, *start_dir.parents):
        p = cand / _LAYR_CONFIG_NAME
        if p.is_file():
            return p
    return None


def _copy_md_tree(src: Path, dest: Path) -> None:
    """组件源目录里的 *.md 按源内相对结构拷贝（组件是 md 文件，其余不冻）。"""
    for md in sorted(src.rglob("*.md")):
        if not md.is_file():
            continue
        target = dest / md.relative_to(src)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(md, target)


def _freeze_scan_face(program_dir: Path, dest: Path) -> None:
    """无 manifest 的程序：向上找 layr.yaml，scan 各源目录拷进冻结面。

    源在配置目录内 → 保持相对名落位（examples 的 ./components 原样可用）；
    逃出配置目录或扫描配置目录本身 → 落 _frozen_scan/{i}。yaml 的 scan 路径
    改写为冻结面内相对位置，其余键原样；解析不了就原样拷（装载报错由驱动带出）。
    """
    cfg = _find_layr_config(program_dir)
    if cfg is None:
        return
    raw_text = cfg.read_text(encoding="utf-8")
    try:
        raw = yaml.safe_load(raw_text)
    except yaml.YAMLError:
        shutil.copy2(cfg, dest / _LAYR_CONFIG_NAME)
        return
    if not isinstance(raw, dict) or not isinstance(raw.get("scan"), list):
        shutil.copy2(cfg, dest / _LAYR_CONFIG_NAME)
        return
    base = cfg.parent
    for i, item in enumerate(raw["scan"]):
        if not (isinstance(item, dict) and isinstance(item.get("path"), str)):
            continue
        spath = Path(item["path"]).expanduser()
        src = spath if spath.is_absolute() else base / spath
        if not src.is_dir():
            continue
        if src != base and src.is_relative_to(base):
            rel = src.relative_to(base)
        else:
            rel = Path(f"_frozen_scan/{i}")
        _copy_md_tree(src, dest / rel)
        item["path"] = f"./{rel.as_posix()}"
    (dest / _LAYR_CONFIG_NAME).write_text(
        yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )


def _freeze_vendor_face(manifest_file: Path, dest: Path) -> None:
    """manifest 程序（目录仓库版本目录）的组件面：resolved 记录逐个从仓库
    vendor 区取文件（sha256 核对）落冻结面 components/，并写最小 layr.yaml
    把扫描面指过去。冻结面没有 {root}/programs/{name}/{version} 的三层深度，
    vendor 校验路（parents[2] 找仓库根）在这里走不通——组件改走扫描面。
    """
    try:
        data = json.loads(manifest_file.read_text(encoding="utf-8"))
        resolved = data["resolved"]
    except (OSError, ValueError, KeyError, TypeError) as e:
        raise ValueError(f"manifest.json 读不出解析记录：{manifest_file}（{e}）") from e
    if not isinstance(resolved, list) or not resolved:
        return
    root = manifest_file.parent.parents[2]  # {root}/programs/{name}/{version} → 仓库根
    for rec in resolved:
        if not isinstance(rec, dict):
            raise ValueError(f"manifest resolved 记录不是映射：{rec!r}")
        ref, ver, sha = rec.get("ref"), rec.get("resolved_version"), rec.get("sha256")
        if not (isinstance(ref, str) and isinstance(ver, str) and isinstance(sha, str)):
            raise ValueError(f"manifest resolved 记录缺 ref/resolved_version/sha256：{rec!r}")
        # ref 形如 type/[来源:]名字@版本区间——来源前缀去后按 type/name 落（与
        # 驱动侧 vendor 落位同约定）
        type_part, _, rest = ref.partition("/")
        name_part = rest.rsplit("@", 1)[0].rsplit(":", 1)[-1]
        src = root / "components" / type_part / name_part / f"{ver}.md"
        if not src.is_file():
            raise ValueError(f"vendor 组件缺文件：{src}")
        blob = src.read_bytes()
        if hashlib.sha256(blob).hexdigest() != sha:
            raise ValueError(f"vendor 组件校验不过（sha256 不符）：{src}")
        target = dest / "components" / type_part / name_part / f"{ver}.md"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob)
    (dest / _LAYR_CONFIG_NAME).write_text(
        "scan:\n  - { name: vendor, path: ./components }\n", encoding="utf-8"
    )


@dataclass(frozen=True)
class RunConfig:
    """显式 run 的创建参数面（format 为 None 表示纯容器）。"""

    format: str | None = None
    params: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)
    program_ref: str = ""
    work_dir: str | None = None
    max_parallel: int | None = None


class LoopState(StrEnum):
    """run 任务的显式状态机（决策循环内部）。"""

    BOOTING = "booting"
    RUNNING = "running"
    PAUSED = "paused"  # 入口谓词：用户暂停档
    DRAINING = "draining"  # 入口动作：终态收尾轮
    EXITED = "exited"


def next_loop_state(state: LoopState, *, user_paused: bool, terminal: bool) -> LoopState:
    """循环状态迁移表单点；FinishRun 落账由调用方直接退出，不经本表。"""
    return _LOOP_TRANSITIONS[(state, bool(user_paused), bool(terminal))]


_LOOP_TRANSITIONS: dict[tuple[LoopState, bool, bool], LoopState] = {
    (LoopState.BOOTING, False, False): LoopState.RUNNING,
    (LoopState.BOOTING, True, False): LoopState.PAUSED,
    (LoopState.BOOTING, False, True): LoopState.DRAINING,
    (LoopState.BOOTING, True, True): LoopState.DRAINING,
    (LoopState.RUNNING, False, False): LoopState.RUNNING,
    (LoopState.RUNNING, True, False): LoopState.PAUSED,
    (LoopState.RUNNING, False, True): LoopState.DRAINING,
    (LoopState.RUNNING, True, True): LoopState.DRAINING,
    (LoopState.PAUSED, False, False): LoopState.RUNNING,
    (LoopState.PAUSED, True, False): LoopState.PAUSED,
    (LoopState.PAUSED, False, True): LoopState.DRAINING,
    (LoopState.PAUSED, True, True): LoopState.DRAINING,
    (LoopState.DRAINING, False, False): LoopState.EXITED,
    (LoopState.DRAINING, False, True): LoopState.EXITED,
    (LoopState.DRAINING, True, False): LoopState.EXITED,
    (LoopState.DRAINING, True, True): LoopState.EXITED,
    (LoopState.EXITED, False, False): LoopState.EXITED,
    (LoopState.EXITED, False, True): LoopState.EXITED,
    (LoopState.EXITED, True, False): LoopState.EXITED,
    (LoopState.EXITED, True, True): LoopState.EXITED,
}


class _DriverError(Exception):
    """驱动失败已收口（账面已落 failed），循环应退出。"""


@dataclass
class RunRuntime:
    """per-run 运行时（仅 workflow run 有）；paused 是账面状态的派生缓存。"""

    run_id: str
    format: str
    driver: RunDriver
    max_parallel: int | None = None  # 准入上限（None = 不限）
    wake: asyncio.Event = field(
        default_factory=asyncio.Event
    )  # 通知入队即 set；挂起等待复用同一事件
    notifications: deque[RunNotification] = field(
        default_factory=deque
    )  # 通知 FIFO，排空后进 decide
    deferred: deque[RunAction] = field(
        default_factory=deque
    )  # 派发点冻结/准入挂起的动作，驱动不感知
    deferred_starts: int = 0  # 挂起队列中的 Start 类计数（容量判定用）
    in_flight: int = 0  # 在途 Start 类（含挂起，与 world 口径一致），回执为界回收
    in_flight_contexts: set[str] = field(
        default_factory=set
    )  # 在途出生的 context 集（world 成员 in_flight 标记）
    in_flight_sessions: set[str] = field(default_factory=set)  # 在途重启的成员集（同上）
    session_ids: set[str] = field(
        default_factory=set
    )  # 出生成功（回执 ok）的 session 成员集；失败出生不入
    paused: bool = False  # 账面 status/pause_reason 的派生缓存（K 唯一写者，spawn 回灌/boot 重建）
    pause_reason: str | None = None  # 冻结谓词仅认 'user' 档；auth 挂起不冻结决策循环
    pause_synced: bool = (
        False  # 派生缓存已同步（组控直写置位；任务起始恢复据此跳过，防恢复覆盖直写）
    )
    stopping: bool = False  # 仅两处置位：终态控制收尾后 / FinishRun 落账后
    terminal_accepted: bool = False  # 终态裁决权已归控制侧：其后驱动 FinishRun 不落账
    poisoned: bool = False  # 驱动实例中毒弃用（超时/异常后不再调用）
    checkpoint_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # 工作区树建树互斥：run 任务的建区/屏障与查询面并发到场只建一次
    workspace_tree_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    amt_injected: bool = False  # AllSessionsTerminal 已注入（防重复；新派发复位）
    rounds: int = 0  # decide 轮次（超时/异常报错点名用）
    await_tasks: set[asyncio.Task[None]] = field(default_factory=set)  # 在途 session 等待任务
    driver_calls: set[Future[Any]] = field(
        default_factory=set
    )  # 在途池提交（线程退出即完成；关停代办路径的单写者依据）


class _GroupControlEngine:
    """组控语义：pause/resume/stop/steer 的档位记账、成员扫掠与进程处置。"""

    def __init__(self, coordinator: RunCoordinator) -> None:
        self._coord = coordinator

    async def pause_run(self, run_id: str) -> RunRecord:
        """手动暂停：置档→排空在途出生→挂起在跑 session（记组暂停账）→投受理
        回执（manual）→杀 run 键在途进程。

        回执必须先于杀入队：被杀回执若先到，驱动尚未记下暂停语境，会把
        被杀照真失败结算（「暂停变失败」）。manual 即「宿主已杀在途」，
        驱动据此把被杀回执照暂停终止入账。
        """
        coord = self._coord
        done, rt, _ = await self._begin_pause(run_id, "pause_run")
        if done is not None:
            return done
        await self._suspend_sessions(run_id)
        self._emit_paused(run_id, rt, {"reason": "manual"})
        # 终止不是挂起：暂停窗 run 键进程零存活（先排空在途 spawn 再按注册表杀）
        await coord.drain_run_process_spawns(run_id, STOP_DRAIN_TIMEOUT_S)
        await coord.kill_run_processes(run_id)
        return await self._gated_run(run_id)

    async def driver_pause_run(self, run_id: str, reason: str) -> RunRecord:
        """驱动自暂停受理（不杀在途）：置档→排空在途出生→记组暂停账（不挂
        起 session）→投受理回执（带驱动原 reason，不带 manual）。

        条件暂停卡的是等待，在途 session 的活与等待无关——放行跑完，回执
        滞留 FIFO 到恢复轮一并进 decide；受理期到点的定时器/会话超时是真
        超时，照常结算。auth 保持档不记成员账：该档恢复直落 running、不
        经 resume_run，'run' 账无清理通路，记了必成与事实的漂移残留。
        """
        done, rt, pause_reason = await self._begin_pause(run_id, "driver_pause")
        if done is not None:
            return done
        if pause_reason != "auth":
            await self._mark_all_sessions_paused_by_run(run_id)
        self._emit_paused(run_id, rt, {"reason": reason})
        return await self._gated_run(run_id)

    async def resume_run(self, run_id: str, payload: Mapping[str, Any] | None = None) -> RunRecord:
        """恢复：仅受理用户档（auth 档未恢复时拒绝）；挂起过的 session 恢复、组暂停账全量清。

        payload 随 RESUMED 通知透传给驱动（外部答复等恢复上下文）。从未挂
        起的 'run' 记账（驱动路受理不挂 session，恢复时仍在跑或已落终态）
        仅清账——组暂停窗已结束，残留会让「组暂停压住」与事实漂移。
        """
        coord = self._coord
        run = await self._gated_run(run_id)
        if run.status in TERMINAL_STATUSES:
            return run  # 终态重复控制 = no-op
        status, reason = next_pause_gear("resume_run", run.status, run.pause_reason)
        rt = coord.get_runtime(run_id)
        await coord.apply_pause_state(run_id, rt, status, reason)
        for row in await coord.list_run_session_rows(run_id):
            if row.paused_by != "run":
                continue  # 用户手动暂停的照旧跳过、不代劳
            if coord.session_is_resumable(row.session_id):
                try:
                    await coord.resume_session(row.session_id)
                except Exception:
                    continue  # 恢复失败保留记账，下次 resume 重试
            with contextlib.suppress(Exception):
                await coord.set_session_paused_by(run_id, row.session_id, None)
        if rt is not None:
            coord.notify_run(run_id, ControlNotification(kind=ControlKind.RESUMED, payload=payload))
        return await coord.get_run(run_id)

    async def stop_run(self, run_id: str) -> RunRecord:
        """终态控制：受理即取得终态裁决权；run 任务收尾轮后统一走终态序列。"""
        coord = self._coord
        run = await self._gated_run(run_id)
        if run.status in TERMINAL_STATUSES:
            return run  # 查账 no-op（驱动裁决先落账的同款受理）
        # 受理标记先置位（本段无 await）：mint→spawn 窗口内受理的 stop 不再拉起。
        # container 无 spawn，置位即永久残留（无 run 任务退出可清），不设。
        if run.kind == RunKind.WORKFLOW:
            coord.mark_stop_accepted(run_id)
        await coord.kill_run_processes(run_id)  # 进程组先于 session 停止
        rt = coord.get_runtime(run_id)
        if rt is not None:
            coord.notify_run(run_id, ControlNotification(kind=ControlKind.STOPPED))
            task = coord.get_run_task(run_id)
            if task is not None:
                await self._drain_run_task(task)
        record = await coord.sequence_run_terminal(
            run_id, finish_status=RunStatus.STOPPED, drain_timeout=STOP_DRAIN_TIMEOUT_S
        )
        if record is None:
            raise RuntimeError("finish_status 给定时必须返回记录行")
        return record

    async def steer_run(self, run_id: str, message: str) -> None:
        """run 级转向：有驱动经通知（驱动决定转向谁）；无驱动直播 RUNNING 成员并留痕。"""
        coord = self._coord
        run = await coord.get_run(run_id)
        if run.status in TERMINAL_STATUSES:
            return
        if run.kind == RunKind.WORKFLOW:
            if coord.get_runtime(run_id) is not None:
                coord.notify_run(
                    run_id,
                    ControlNotification(kind=ControlKind.STEERED, payload={"message": message}),
                )
            return
        for sid in await coord.list_run_session_ids(run_id):
            facts = coord.get_session_facts(sid)
            if facts is not None and facts.state == "running":
                with contextlib.suppress(Exception):
                    await coord.inject_session_prompt(sid, message)
        with contextlib.suppress(Exception):
            await coord.emit_run_event(
                run_id,
                "run_steered",
                {"message": message},
            )

    async def _gated_run(self, run_id: str) -> RunRecord:
        """组控受理门：查账并拒绝 implicit run（状态=成员投影，无独立控制）。"""
        run = await self._coord.get_run(run_id)
        if run.kind == RunKind.IMPLICIT:
            raise ValueError(f"run {run_id} 是隐式 run（状态=成员投影，无独立控制）")
        return run

    async def _drain_run_task(self, task: asyncio.Task[None]) -> None:
        """run 任务的受控收尾：限时等退出，超时 cancel 后彻底收割。"""
        try:
            await asyncio.wait_for(asyncio.shield(task), STOP_DRAIN_TIMEOUT_S)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(BaseException):
                await task

    async def _begin_pause(
        self, run_id: str, writer: str
    ) -> tuple[RunRecord | None, RunRuntime | None, str | None]:
        """暂停受理公共骨架：终态门→落档→排空在途出生。

        返回 (终态记录行, 运行时, 落定档位)——记录行非 None 即终态重复
        控制，调用方照 no-op 返回。
        """
        coord = self._coord
        run = await self._gated_run(run_id)
        if run.status in TERMINAL_STATUSES:
            return run, None, run.pause_reason
        status, pause_reason = next_pause_gear(writer, run.status, run.pause_reason)
        rt = coord.get_runtime(run_id)
        await coord.apply_pause_state(run_id, rt, status, pause_reason)
        await coord.drain_run_session_births(run_id, STOP_DRAIN_TIMEOUT_S)
        return None, rt, pause_reason

    async def _pausable_session_ids(self, run_id: str) -> list[str]:
        """在跑成员 session 清单（跳过已结束/已暂停，绝不覆盖 stop_reason）。"""
        coord = self._coord
        rows = await coord.list_run_session_rows(run_id)
        return [row.session_id for row in rows if coord.session_is_pausable(row.session_id)]

    async def _mark_session_paused_by_run(self, run_id: str, session_id: str) -> None:
        """记组暂停账（paused_by='run'）；记录写失败不阻断扫掠。"""
        with contextlib.suppress(Exception):
            await self._coord.set_session_paused_by(run_id, session_id, "run")

    async def _suspend_sessions(self, run_id: str) -> None:
        """挂起在跑 session 并记组暂停账；挂起失败的跳过（记账与挂起同体）。"""
        coord = self._coord
        for sid in await self._pausable_session_ids(run_id):
            try:
                await coord.pause_session(sid)
            except Exception:
                logger.warning("挂起 session %s 失败，跳过该成员的暂停记账", sid, exc_info=True)
            else:
                await self._mark_session_paused_by_run(run_id, sid)

    async def _mark_all_sessions_paused_by_run(self, run_id: str) -> None:
        """只记组暂停账、不挂起 session（驱动自暂停路：在途照常跑完）。"""
        for sid in await self._pausable_session_ids(run_id):
            await self._mark_session_paused_by_run(run_id, sid)

    def _emit_paused(
        self, run_id: str, rt: RunRuntime | None, payload: dict[str, str] | None
    ) -> None:
        """受理回执 PAUSED 入队（payload reason 表宿主杀在途与否）。"""
        if rt is not None:
            self._coord.notify_run(
                run_id, ControlNotification(kind=ControlKind.PAUSED, payload=payload)
            )


class _AdmissionGate:
    """并发准入：Start 类动作按 run.max_parallel 挂起/放行（驱动不感知）。

    容量语义承接编排信号量：槽位自派发起占用、成员终态才释放（在途出生
    与活成员都占槽；in_flight 另按回执回收，仅作 world 口径）。
    """

    def __init__(self, coordinator: RunCoordinator) -> None:
        self._coord = coordinator

    @staticmethod
    def defer(rt: RunRuntime, action: RunAction) -> None:
        """挂起动作（容量不足或冻结窗）；Start 类计入在途，与 world 口径一致。"""
        rt.deferred.append(action)
        if isinstance(action, START_LIKE_ACTIONS):
            rt.deferred_starts += 1
            rt.in_flight += 1

    @staticmethod
    def _mark_in_flight(rt: RunRuntime, action: RunAction) -> None:
        """在途标记（world 成员 in_flight 投影）：出生记 context，重启记 session。"""
        if isinstance(action, StartSession) and action.context:
            rt.in_flight_contexts.add(action.context)
        elif isinstance(action, RestartSession):
            rt.in_flight_sessions.add(action.session_id)

    def admit(self, rt: RunRuntime, action: RunAction | None = None) -> bool:
        """容量判定：在途出生 + 未终态成员未达上限（None = 不限）。

        在册未终态成员的重启是槽位置换：目标成员自身不占新槽（正在重启的
        成员从活成员计数中剔除）——否则 in_flight 与 deferred_starts 相消后
        active 仍含目标成员，pump 永不弹、run 挂死。
        """
        cap = rt.max_parallel
        if cap is None:
            return True
        replacing = set(rt.in_flight_sessions)
        if isinstance(action, RestartSession):
            replacing.add(action.session_id)
        active = rt.in_flight - rt.deferred_starts
        active += sum(
            1
            for sid in rt.session_ids
            if sid not in replacing and not self._coord.session_is_terminal(sid)
        )
        return active < cap

    def charge(self, rt: RunRuntime, action: RunAction) -> None:
        """直发动作计入在途（与 defer 对称，回执统一回收）。"""
        if isinstance(action, START_LIKE_ACTIONS):
            rt.in_flight += 1
            self._mark_in_flight(rt, action)

    def pump(self, rt: RunRuntime) -> list[RunAction]:
        """容量内按序弹出挂起动作（暂停窗/终态轮由调用方把关，此处不弹）。"""
        out: list[RunAction] = []
        while rt.deferred:
            action = rt.deferred[0]
            if isinstance(action, START_LIKE_ACTIONS) and not self.admit(rt, action):
                break
            rt.deferred.popleft()
            if isinstance(action, START_LIKE_ACTIONS):
                rt.deferred_starts -= 1
                self._mark_in_flight(rt, action)
            out.append(action)
        return out


class _TerminalSequencer:
    """终态收尾原语：冻结在途出生、清挂起、收尾轮与退出后处置的唯一实现。"""

    def __init__(self, coordinator: RunCoordinator, host: RunHost) -> None:
        self._coord = coordinator
        self._host = host

    async def sequence(
        self, run_id: str, *, finish_status: str | None, drain_timeout: float
    ) -> RunRecord | None:
        """终态收尾序列：排空在途出生→停成员→排空 spawn→置杀进程→清定时器→落终态（None=保持非终态）。"""
        coord = self._coord
        await coord.drain_run_session_births(run_id, drain_timeout)
        for sid in await coord.list_run_session_ids(run_id):
            with contextlib.suppress(Exception):
                await coord.stop_session(sid)
        await coord.drain_run_process_spawns(run_id, drain_timeout)
        await coord.kill_run_processes(run_id)
        self._host.cancel_run_timers(run_id)
        if finish_status is None:
            return None
        run = await coord.get_run(run_id)
        if run.status in TERMINAL_STATUSES:
            return run  # 落账前查账：终态已定即受理 no-op，不调 finish
        return await coord.finish_run(run_id, finish_status)


class _Reconciler:
    """幂等重入对账骨架：逐项隔离失败（单项告警留痕不阻断其余），整轮可重放。"""

    def __init__(self, name: str, key: Callable[[Any], str] = str) -> None:
        self._name = name
        self._key = key

    async def reconcile(
        self, items: Iterable[Any], handle: Callable[[Any], Awaitable[None]]
    ) -> None:
        """逐项执行对账；单项异常告警留痕后继续。"""
        for item in items:
            try:
                await handle(item)
            except Exception:
                logger.warning(
                    "%s：%s 对账失败，留待下次重入",
                    self._name,
                    self._key(item),
                    exc_info=True,
                )


class _RecoveryService:
    """boot 恢复对账组件序列：名册成员对账 → 分类判定 → 驱动重建 → 续跑拉起。

    每步按名可独立跳过（skip 名单，启动路径排障用）；run 级与成员级两条
    对账循环共用 _Reconciler 骨架；归属补铸由归属服务在更早的 boot 阶段完成，
    此处不重复。
    """

    def __init__(
        self,
        coordinator: RunCoordinator,
        run_store: RunStore,
        orchestrator: Orchestrator,
        factories: Mapping[str, RunDriverFactory],
        guarded_call: Callable[..., Awaitable[RunDriver]],
        spawner: Callable[..., None],
        state_reconciler: Callable[[RunRecord], Awaitable[None]],
    ) -> None:
        self._coord = coordinator
        self._run_store = run_store
        self._orch = orchestrator
        self._factories = dict(factories)
        self._guarded_call = guarded_call
        self._spawner = spawner
        self._state_reconciler = state_reconciler
        self._runs = _Reconciler("run 恢复", key=lambda run: run.run_id)
        self._members = _Reconciler("名册成员对账")

    async def recover_at_boot(self, *, skip: Collection[str] = ()) -> None:
        """boot 恢复入口：无账面零写入；成员对账后逐 run 走恢复管线。"""
        disabled = frozenset(skip)
        if "reconcile_members" not in disabled:
            # 成员对账不触碰账面，先于账面守卫：runs 族表缺失的旧库升级形态
            # 不因此漏掉 reviewer 重建（全新安装时名册为空，零成本 no-op）
            await self._members.reconcile(
                [s.id for s in self._orch.pool.list_sessions()],
                self._reconcile_member,
            )
        if not await self._run_store.has_runs_table():
            # 判定以账面自身为准：纯 bash run 走进程桥不落 session 名册，
            # 名册为空不代表账面为空，不能据名册跳过恢复
            return
        # 含隐式 run：崩溃时账面停在 running 的隐式 run 若被默认过滤排除
        # 在循环外，ghost 成员重启后不再发状态事件，投影永不重算
        runs = await self._run_store.list_runs(include_implicit=True)
        await self._runs.reconcile(runs, functools.partial(self._recover_run, disabled=disabled))

    async def _reconcile_member(self, session_id: str) -> None:
        """名册成员对账：按持久化 profile 重建 reviewer 快照（进程重启后不缺位）。"""
        await self._orch.ensure_reviewer_snapshot(session_id)

    async def _recover_run(self, run: RunRecord, *, disabled: frozenset[str]) -> None:
        """单 run 恢复管线：分类判定 → 无驱动 run 状态重算 → 驱动重建 → 续跑。"""
        if run.status == RunStatus.PENDING:
            if "classify" not in disabled:
                # start_run 的异常防护已同步回滚，此处兜底进程崩溃：
                # pending 必然无成员，判失败安全
                await self._run_store.finish(
                    run.run_id, RunStatus.FAILED, error="crash_during_start"
                )
            return
        if run.status in TERMINAL_STATUSES:
            return
        if run.kind != RunKind.WORKFLOW:
            # 无驱动 run：按 ghost 名册事实重算状态（隐式投影/容器收尾）；
            # 状态重算就是无驱动 run 的恢复步，随 restore 步名一并跳过
            if "restore" not in disabled:
                await self._state_reconciler(run)
            return
        if "restore" in disabled:
            return
        factory = self._factories.get(run.format)
        if factory is None:
            await self._fail(run, f"格式 {run.format} 没有驱动工厂")
            return
        blob = await self._run_store.load_driver_state(run.run_id)
        if blob is None:
            await self._fail(run, "无驱动快照，无法恢复")
            return
        try:
            sessions = await self._coord.list_run_sessions(run.run_id)
            driver = await self._guarded_call(
                run.format, factory.restore, run, blob, list(sessions)
            )
        except Exception as e:
            await self._fail(run, f"unrecoverable: {e}")
            return
        if "resume" not in disabled:
            self._spawner(
                run.run_id,
                run.format,
                driver,
                max_parallel=run.max_parallel,
                session_ids=[m.session_id for m in sessions],
            )
            logger.info("boot 恢复 run %s（status=%s）", run.run_id, run.status)

    async def _fail(self, run: RunRecord, reason: str) -> None:
        logger.error("boot 恢复 run %s 失败：%s", run.run_id, reason)
        await self._run_store.finish(run.run_id, RunStatus.FAILED, error=reason)


# SDK 作用域种类 → 内核账本种类
_WORKSPACE_SCOPE_KINDS = {
    WorkspaceScopeKind.MAIN_WRITER: ScopeKind.MAIN_WRITER,
    WorkspaceScopeKind.SUBAGENT: ScopeKind.SUBAGENT,
}


def _default_transcript_locator(native_session_id: str, workspace_path: str) -> Path | None:
    """默认 L2 定位：按父会话原生标识与工作目录解析 claude 系 JSONL。"""
    if not native_session_id:
        return None
    found = resolve_claude_session_path(native_session_id, workspace_path, None)
    return found


def _subagent_fault_identity(session_id: str, data: Mapping[str, Any]) -> dict[str, Any]:
    """子代理收尾故障事件的归属字段：会话与子代理标识。"""
    return {"session_id": session_id, "agent_id": str(data.get("agent_id") or "")}


def _node_id_of_context(context: str | None) -> str | None:
    """节点归属上下文（node=<id> 或 node=<id>/<相位>）里的节点 id；不是节点上下文返回 None。"""
    if not context or not context.startswith("node="):
        return None
    return context.removeprefix("node=").partition("/")[0]


def _workspace_conflict_payload(conflict: IntegrationConflict) -> dict[str, Any]:
    """集成冲突的 run 键事件 payload：路径、双方身份、双侧提交与共同基准。"""
    return {
        "scope_id": conflict.writer_scope_id,
        "parent_scope_id": conflict.parent_scope_id,
        "conflict_paths": list(conflict.conflict_paths),
        "base_commit": conflict.base_commit,
        "writer_head": conflict.writer_head,
        "parent_head": conflict.parent_head,
    }


class RunCoordinator:
    """run 平面协调器：run 生命周期、成员编排与决策循环宿主。

    驱动调用（decide/on_control/snapshot/handle_op/projection）全部发生在
    所属 run 的任务内、经有界线程池与超时守护执行；API 协程经通知 FIFO
    与应答通道交互，不直调驱动。
    """

    def __init__(
        self,
        *,
        orchestrator: Orchestrator,
        run_store: RunStore,
        bus: EventBus,
        driver_factories: Mapping[str, RunDriverFactory],
        affiliation: SessionRunAffiliationService,
        decide_timeout_s: float = DECIDE_TIMEOUT_S,
        workspace_drain_hook: BackgroundDrainHook | None = None,
        workspace_transcript_locator: TranscriptLocator | None = None,
    ) -> None:
        self._orch = orchestrator
        self._run_store = run_store
        self._bus = bus
        # 会话宿主侧的派发屏障端口：屏障请求经协调器队列执行（唯一 I/O 手）
        set_barrier_port = getattr(orchestrator, "set_dispatch_barrier_port", None)
        if callable(set_barrier_port):
            set_barrier_port(self.submit_dispatch_barrier)
        # 会话宿主侧的子代理启动端口：组检查点行回填子会话映射（尽力）
        set_start_port = getattr(orchestrator, "set_subagent_start_port", None)
        if callable(set_start_port):
            set_start_port(self.record_subagent_start)
        # 会话宿主侧的子代理结果端口：结果交付前并入父作用域（经协调器队列执行）
        set_result_port = getattr(orchestrator, "set_subagent_result_port", None)
        if callable(set_result_port):
            set_result_port(self.settle_subagent_result)
        # 工作区闸门的可注入后台写进程排空回调（后台写者清单接线前默认无）
        self._workspace_drain_hook = workspace_drain_hook
        # 对话恢复的 L2 定位器（默认按 claude 系路径解析；测试可注入）
        resume_starter = getattr(orchestrator, "start_session_with_resume", None)
        self._restorer = ConversationRestorer(
            locate_transcript=workspace_transcript_locator or _default_transcript_locator,
            start_resumed_session=resume_starter
            if callable(resume_starter)
            else self._reject_resumed_session,
        )
        # 冻结映射：工厂集在插件装载后不可变（无热重载），重名注册已在
        # 组合根聚合时 raise——boot 期 fail-fast，而非首次 start_run
        self._driver_factories: dict[str, RunDriverFactory] = dict(driver_factories)
        self._affiliation = affiliation
        self._decide_timeout_s = decide_timeout_s
        self._host = RunHost(
            orchestrator,
            run_store,
            affiliation,
            self._notify,
            self.run_data_root,
            bus,
        )
        self._group_control = _GroupControlEngine(self)
        self._admission = _AdmissionGate(self)
        self._terminal = _TerminalSequencer(self, self._host)
        self._recovery = _RecoveryService(
            self,
            run_store,
            orchestrator,
            self._driver_factories,
            self._guarded_factory_call,
            self._spawn,
            self._reconcile_recovered_run_state,
        )
        self._export_service: RunExportService | None = None
        self._export_service_guard = threading.Lock()
        self._runtimes: dict[str, RunRuntime] = {}  # run_id → 运行时（仅 workflow run）
        self._tasks: dict[str, asyncio.Task[None]] = {}  # run_id → run 任务（唯一 spawn 口登记）
        self._workspace_trees: dict[
            str, WorkspaceTreeRuntime
        ] = {}  # run_id → 工作区树运行时（首个工作区动作惰性建立）
        # 派发准入的子会话回填线索：(run_id, tool_use_id) → (检查点, 子作用域)
        self._dispatch_admissions: dict[tuple[str, str], tuple[str, str]] = {}
        self._decide_pools: dict[
            str, ThreadPoolExecutor
        ] = {}  # format → decide 线程子池（惰性创建）
        self._decide_pools_guard = threading.Lock()
        self._run_kinds: dict[str, str] = {}  # run_id → kind 懒缓存（kind 不可变）
        # stop 受理标记：_spawn 同步复查，关闭 mint→spawn 窗口竞态
        # （标记在 _spawn 拒绝拉起或 run 任务退出时清除）
        self._stop_marked: set[str] = set()
        bus.observe(EventType.SESSION_OUTCOME, self._on_session_outcome_event)
        bus.observe(EventType.SESSION_STATE_CHANGED, self._on_session_state_event)

    # ── 生命周期 ────────────────────────────────────────────────

    @staticmethod
    def _user_paused(rt: RunRuntime) -> bool:
        """冻结谓词：仅用户暂停档冻结决策循环。"""
        return rt.paused and rt.pause_reason == "user"

    @staticmethod
    def _idle(rt: RunRuntime) -> bool:
        """挂起与在途不算活跃：推进条件是回执/容量事件（必 set wake），不是轮询。"""
        return not rt.notifications

    @staticmethod
    def _drain_notifications(rt: RunRuntime) -> list[RunNotification]:
        """整批取走 FIFO 内全部通知。"""
        notes = list(rt.notifications)
        rt.notifications.clear()
        return notes

    @staticmethod
    def _drain_controls(
        rt: RunRuntime,
    ) -> tuple[
        list[ControlNotification],
        list[OpRequest],
        list[DispatchBarrierRequest],
        list[SubagentResultRequest],
    ]:
        """取走控制通知、应答请求与宿主服务请求，其余保序滞留 FIFO（暂停期专用，成员事件不丢失）。"""
        ctrls: list[ControlNotification] = []
        ops: list[OpRequest] = []
        barriers: list[DispatchBarrierRequest] = []
        results: list[SubagentResultRequest] = []
        rest: deque[RunNotification] = deque()
        while rt.notifications:
            n = rt.notifications.popleft()
            if isinstance(n, ControlNotification):
                ctrls.append(n)
            elif isinstance(n, OpRequest):
                ops.append(n)
            elif isinstance(n, DispatchBarrierRequest):
                barriers.append(n)
            elif isinstance(n, SubagentResultRequest):
                results.append(n)
            else:
                rest.append(n)
        rt.notifications.extend(rest)
        return ctrls, ops, barriers, results

    @staticmethod
    def _settle_op_reply(op: OpRequest, outcome: dict[str, Any] | BaseException) -> None:
        """回填应答；调用方已超时离场时留痕（结果未送达，操作照常落账）。"""
        if op.reply.done():
            logger.warning(
                "驱动操作 %s 应答晚于调用方等待上界：结果未送达（操作仍会落账）: %r",
                op.op,
                outcome,
            )
            return
        if isinstance(outcome, BaseException):
            op.reply.set_exception(outcome)
        else:
            op.reply.set_result(outcome)

    def runs_data_root(self) -> Path:
        """项目级 run 数据根（各 run 根的父级）。"""
        return (self._orch.default_project_dir / ".workflow-controller" / "runs").resolve()

    def run_data_root(self, run_id: str) -> Path:
        """run 数据根目录（run 根，程序与产物的家）。"""
        return self.runs_data_root() / run_id

    async def start_run(self, config: RunConfig, program_path: Path | str) -> RunRecord:
        """写序：派发校验 → mint(pending) → 冻结程序 → 建驱动 → running → spawn。

        mint 后任一步失败立即 finish(failed)，不留 pending 残行。
        """
        fmt = config.format or ""
        factory = self._driver_factories.get(fmt)
        if factory is None:
            registered = ", ".join(sorted(self._driver_factories)) or "无"
            raise ValueError(
                f"格式 {fmt} 没有驱动工厂（已登记：{registered}）——"
                "工厂由插件经 get_run_driver_factories 登记"
            )
        program = Path(program_path)
        params = dict(config.params)
        run = await self._run_store.create(
            config.program_ref,
            fmt,
            labels=dict(config.labels),
            params=params,
            status=RunStatus.PENDING,
            kind=RunKind.WORKFLOW,
            work_dir=config.work_dir,
            max_parallel=config.max_parallel,
        )
        try:
            frozen = self._freeze_program(run.run_id, program)
            driver = factory.create(run, frozen, dict(config.params))
            await self._run_store.update_status(run.run_id, RunStatus.RUNNING)
        except RunOccupiedError as e:
            # 占用拒绝不是启动崩溃：显式落占用原因，不贴 crash_during_start
            await self._run_store.finish(run.run_id, RunStatus.FAILED, error=str(e))
            raise
        except Exception as e:
            await self._run_store.finish(
                run.run_id, RunStatus.FAILED, error=f"crash_during_start: {e}"
            )
            raise
        self._spawn(
            run.run_id,
            fmt,
            driver,
            max_parallel=config.max_parallel,
        )
        return await self.get_run(run.run_id)

    async def create_run(
        self, label: str, *, work_dir: str | None = None, max_parallel: int | None = None
    ) -> RunRecord:
        """创建纯容器 run（无驱动，全员终态自动收尾）。"""
        return await self._run_store.create(
            program_ref="",
            fmt="",
            labels={"name": label} if label else {},
            status=RunStatus.RUNNING,
            kind=RunKind.CONTAINER,
            work_dir=work_dir,
            max_parallel=max_parallel,
        )

    async def get_run(self, run_id: str) -> RunRecord:
        """取 run 账面行。"""
        return await self._run_store.get(run_id)

    async def list_runs(
        self, *, include_implicit: bool = False, status: str | None = None
    ) -> list[RunRecord]:
        """run 列表（默认过滤隐式）。"""
        return await self._run_store.list_runs(
            include_implicit=include_implicit,
            status=status,
        )

    async def run_state(self, run_id: str) -> dict[str, Any]:
        """通用面：run 记录 + session 表 + 进程表 + 驱动投影透传。"""
        run = await self._run_store.get(run_id)
        sessions = [
            {
                "session_id": m.session_id,
                "role": m.role,
                "context": m.context,
                "state": m.state,
                "stop_reason": m.stop_reason,
                "in_flight": m.in_flight,
            }
            for m in await self.list_run_sessions(run_id)
        ]
        driver: dict[str, Any] | None = None
        if run.kind == RunKind.WORKFLOW:
            if run_id in self._runtimes:
                with contextlib.suppress(Exception):
                    driver = await self.driver_projection(
                        run_id, timeout_s=STATE_PROJECTION_TIMEOUT_S
                    )
            # 投影应答不可用（任务忙超时 / runtime 已退出）降级：优先终态收尾
            # 落盘的最终投影（消费面要的投影形状）；未落盘（忙时窗 / 收尾投影
            # 软失败 / v4 前旧 run）回落盘上最新快照，快照亦缺保持 None
            if driver is None:
                with contextlib.suppress(Exception):
                    driver = await self._run_store.load_driver_projection(run_id)
            if driver is None:
                with contextlib.suppress(Exception):
                    driver = await self._run_store.load_driver_state(run_id)
        rt = self._runtimes.get(run_id)
        metrics: dict[str, Any] | None = None
        if rt is not None:
            metrics = {
                "rounds": rt.rounds,
                "queue_depth": len(rt.notifications),
                "deferred": len(rt.deferred),
                "in_flight": rt.in_flight,
            }
        return {
            "run_id": run.run_id,
            "status": run.status,
            "kind": run.kind,
            "format": run.format,
            "sessions": sessions,
            "processes": [
                {"proc_id": p.proc_id, "context": p.context, "returncode": p.returncode}
                for p in self._host.process_infos(run_id)
            ],
            "driver": driver,
            "metrics": metrics,
            "root": str(self.run_data_root(run_id)),
        }

    async def export_run(
        self,
        run_id: str,
        *,
        archive: bool = False,
        progress_cb: Callable[[int, int], None] | None = None,
        archive_name: str | None = None,
    ) -> dict[str, Any]:
        """导出 run 为 workflow bundle，产物落 run 根 export/。"""
        if self._export_service is None:
            # 惰性装配：不导出不触事件库；全局 runtime 只在首次导出解析。
            # 后台导出线程各自持独立事件循环并发首导，须以线程锁守卫
            # check-then-set，防两线程各建实例、同 run 串行锁被绕过。
            with self._export_service_guard:
                if self._export_service is None:
                    self._export_service = RunExportService(
                        run_store=self._run_store,
                        event_store=self._orch.event_router.default_store,
                        run_root=self.run_data_root,
                        factory_lookup=self._driver_factories.get,
                        guarded_call=self._guarded_factory_call,
                        default_runtime=getattr(self._orch.config, "runtime", "") or "",
                        scan_user_homes=getattr(self._orch.config, "claude_scan_user_homes", False),
                    )
        return await self._export_service.export_run(
            run_id,
            archive=archive,
            progress_cb=progress_cb,
            archive_name=archive_name,
        )

    async def pause_run(self, run_id: str) -> RunRecord:
        """暂停 run（经组控引擎）。"""
        return await self._group_control.pause_run(run_id)

    async def resume_run(self, run_id: str, payload: Mapping[str, Any] | None = None) -> RunRecord:
        """恢复 run（经组控引擎）；payload 随 RESUMED 通知透传给驱动。"""
        return await self._group_control.resume_run(run_id, payload)

    async def stop_run(self, run_id: str) -> RunRecord:
        """终态控制：受理即取得终态裁决权，其后的驱动裁决不再落账。"""
        return await self._group_control.stop_run(run_id)

    async def steer_run(self, run_id: str, message: str) -> None:
        """注入用户指示（经组控引擎）。"""
        await self._group_control.steer_run(run_id, message)

    async def restart_run(self, run_id: str) -> RunRecord:
        """终态 run 重开续跑：workflow 恢复驱动，container 立即重估收尾。"""
        reopened = await self._run_store.reopen(run_id)  # 非终态/implicit 在 store 拒绝
        self._stop_marked.discard(run_id)
        self._run_kinds.pop(run_id, None)
        if reopened.kind == RunKind.WORKFLOW:
            driver = await self._restore_workflow_run(reopened)
            self._spawn(
                run_id,
                reopened.format,
                driver,
                max_parallel=reopened.max_parallel,
                session_ids=[m.session_id for m in await self._run_store.list_run_sessions(run_id)],
            )
            return await self.get_run(run_id)
        await self._close_container_if_idle(run_id)  # 重开即重估：全终态随即收尾
        return await self.get_run(run_id)

    async def reload_run_definition(
        self, run_id: str, program_path: Path | str | None = None
    ) -> dict[str, Any]:
        """重读当前工作流定义并换装到现有 run（不新建 run，仅暂停态受理）。

        翻译先行（工厂侧 I/O，坏定义在此失败、run 零改动）、换装经
        reload_definition 驱动 op 在 run 任务内完成（与驱动调用同序）、
        成功后重冻结 run 根程序副本（恢复不随外部文件漂移；拒绝路径
        不触碰副本）。
        """
        run = await self._run_store.get(run_id)
        if run.kind != RunKind.WORKFLOW:
            raise ValueError(f"run {run_id} 是 {run.kind} 形态，不支持重载定义")
        if run.status in TERMINAL_STATUSES:
            raise ValueError(f"run {run_id} 已终态（{run.status}），重载定义需在暂停态进行")
        rt = self._runtimes.get(run_id)
        if rt is None or rt.poisoned:
            raise RuntimeError(f"run {run_id} 无可用驱动运行时，重载定义需在暂停态进行")
        if run.status != RunStatus.PAUSED:
            raise ValueError(f"run {run_id} 运行中（{run.status}），重载定义仅暂停态受理，请先暂停")
        factory = self._driver_factories.get(run.format)
        reloader = getattr(factory, "reload_definition", None) if factory is not None else None
        if not callable(reloader):
            raise ValueError(f"格式 {run.format} 的驱动工厂不支持重载定义")
        source = Path(program_path).expanduser() if program_path else Path(run.program_ref)
        if not source.is_file():
            raise ValueError(f"工作流源文件不存在：{source}")
        try:
            payload = await self._guarded_factory_call(run.format, reloader, run, rt.driver, source)
        except Exception as e:
            # 翻译/校验失败统一包装为可读错误（保留原因）；此时零改动，
            # run 保持原定义原状态
            raise ValueError(f"工作流定义重读失败：{type(e).__name__}: {e}") from e
        # 换装成功后才冻结：驱动侧拒绝（如删除既有节点）时 run 根程序
        # 副本不被改写，恢复源与在册定义不脱节。冻结失败时换装已生效，
        # 报错由调用方重试同一重载收敛
        entry = self.run_data_root(run_id) / "program" / source.name
        payload["yaml_path"] = str(entry)
        payload["agents_dir_base"] = str(source.parent)
        result = await self.request_driver_op(run_id, "reload_definition", payload)
        await asyncio.to_thread(self._freeze_program, run_id, source)
        return result

    async def delete_run(self, run_id: str) -> None:
        """级联删除：停运行面 → 账面单事务 → 物理删 run 根 → 内存驱逐与成员清理通知。

        可安全重试：run 不存在或已删过不报错，重试只补做物理删除；
        work_dir 在 run 根之外，不删。驱逐成员前先给每个成员广播
        session_delete（只通知不落库），触发插件的按 session 清理。
        """
        run: RunRecord | None = None
        with contextlib.suppress(RunNotFoundError):
            run = await self._run_store.get(run_id)
        if run is not None:
            await self._halt_run_activities(run)
        # 删除抑制前置于事务：级联提交后仍在收尾的输出采集不得对已删
        # run_id 复活孤儿行
        self._host.forget_run(run_id)
        session_ids = await self._run_store.delete_run(run_id)
        for sid in session_ids or []:
            await self._broadcast_session_delete(sid)
        await asyncio.to_thread(
            shutil.rmtree,
            self.run_data_root(run_id),
            ignore_errors=True,
        )
        self._run_kinds.pop(run_id, None)
        self._stop_marked.discard(run_id)
        if session_ids is None:
            return
        for sid in session_ids:
            self._affiliation.evict(sid)
            self._orch.discard_deleted_session(sid)

    async def list_run_sessions(self, run_id: str) -> list[RunSessionInfo]:
        """run 名下 session 成员当前事实视图。"""
        rows = await self._run_store.list_run_sessions(run_id)
        rt = self._runtimes.get(run_id)
        return [self._run_session_info(row, rt) for row in rows]

    async def list_run_scopes(self, run_id: str) -> list[dict[str, Any]]:
        """run 的作用域账面查询：树不在内存时按盘上配置重建。

        重建需要 run 仍在运行（有驱动运行时）；未开启工作区隔离、或 run 已
        无运行时（终态）时返回空。
        """
        await self._run_store.get(run_id)
        tree = await self._ensure_workspace_tree_if_running(run_id)
        if tree is None:
            return []
        return [
            {
                "scope_id": record.scope_id,
                "parent_scope_id": record.parent_scope_id,
                "scope_kind": record.scope_kind.value,
                "owner_ref": record.owner_ref,
                "workspace_ref": record.workspace_ref,
                "workspace_path": (record.workspace_ref.rpartition("|")[0] or record.workspace_ref)
                if record.workspace_ref
                else None,
                "scope_base_commit": record.scope_base_commit,
                "integration_base_commit": record.integration_base_commit,
                "scope_epoch": record.scope_epoch,
                "root_epoch": record.root_epoch,
                "gate_state": record.gate_state.value,
                "state": record.state.value,
                "created_at": record.created_at,
            }
            for record in await tree.manager.list_scopes()
        ]

    async def list_run_checkpoints(
        self, run_id: str, *, scope_id: str | None = None
    ) -> list[dict[str, Any]]:
        """run 的检查点查询（树根锚定本 run 的行；可按作用域过滤）。"""
        await self._run_store.get(run_id)
        store = CheckpointStore(self._run_store.db_path)
        try:
            rows = await store.list_for_tree(f"{run_id}-root")
        finally:
            store.close()
        if scope_id is not None:
            rows = [row for row in rows if row.scope_id == scope_id]
        return [checkpoint_to_dict(row) for row in rows]

    # ── 单会话工作区查询与回滚面（实时视图住控制台路由，跨仓 PR）──

    async def get_session_live_view(
        self, session_id: str, path: str | None = None
    ) -> dict[str, Any]:
        """单会话托管工作区的只读实时视图（编排器会话宿主承载）。"""
        return await self._orch.get_session_live_view(session_id, path)

    async def list_session_workspace_checkpoints(self, session_id: str) -> list[dict[str, Any]]:
        """单会话树的检查点查询。"""
        return await self._orch.list_session_workspace_checkpoints(session_id)

    async def restore_session_subagent(
        self,
        session_id: str,
        *,
        checkpoint_id: str,
        expected_epoch: int,
        client_op_id: str,
        replay_dispatch: bool = False,
    ) -> dict[str, Any]:
        """单会话子代理回滚请求（宿主链内串行执行，返回恢复结论）。"""
        return await self._orch.restore_session_subagent(
            session_id,
            checkpoint_id=checkpoint_id,
            expected_epoch=expected_epoch,
            client_op_id=client_op_id,
            replay_dispatch=replay_dispatch,
        )

    async def reap_workspace_residue(self) -> dict[str, Any]:
        """显式触发一次工作区残留清理（启动期自动清理的手动等价物）。"""
        return await self._orch.reap_workspace_residue()

    # ── 成员 session 管理（成员始终为 session）─────────────────────

    async def pause_run_session(self, run_id: str, session_id: str) -> None:
        """成员级暂停：用户档记账 + 驱动通知。"""
        await self._require_run_session(run_id, session_id)
        await self._orch.pause_session(session_id)
        await self._run_store.set_session_paused_by(run_id, session_id, "user")
        self._notify_session_control_applied(run_id, session_id, "pause")

    async def resume_run_session(self, run_id: str, session_id: str) -> None:
        """成员级恢复：清暂停记账 + 驱动通知。"""
        await self._require_run_session(run_id, session_id)
        run = await self._run_store.get(run_id)
        if run.kind == RunKind.WORKFLOW:
            if (
                run.status != RunStatus.RUNNING
                or run_id not in self._runtimes
                or run_id in self._stop_marked
            ):
                if run.status == RunStatus.PAUSED:
                    raise MemberResumeDeniedError(f"run {run_id} 已暂停，请先恢复整个 Run")
                raise MemberResumeDeniedError(f"run {run_id} 已结束，请重新启动整个 Run")
            if not self._orch.is_resumable(session_id):
                raise MemberResumeDeniedError(f"session {session_id} 不是可恢复的用户暂停成员")
        await self._orch.resume_session(session_id)
        await self._run_store.set_session_paused_by(run_id, session_id, None)
        self._notify_session_control_applied(run_id, session_id, "resume")

    # ── 成员级操作（run 平面唯一写者通道）────────────────────────

    async def stop_run_session(self, run_id: str, session_id: str) -> None:
        """成员级停止：驱动通知。"""
        await self._require_run_session(run_id, session_id)
        await self._orch.stop_session(session_id)
        self._notify_session_control_applied(run_id, session_id, "stop")

    async def inject_run_session(self, run_id: str, session_id: str, prompt: str) -> None:
        """成员级定向注入：驱动通知。"""
        await self._require_run_session(run_id, session_id)
        await self._orch.inject_prompt(session_id, prompt)
        self._notify_session_control_applied(run_id, session_id, "inject")

    async def send_run_session(
        self,
        run_id: str,
        session_id: str,
        prompt: str,
        *,
        allow_resume: bool = True,
    ) -> bool:
        """Run 成员的单请求发送；恢复资格沿用成员恢复规则。

        allow_resume=False 时只注入不恢复（前端同目录冲突等仲裁信号），返回
        False 让调用方提示「已入队未恢复」。注入是非幂等步骤，先落账再谈恢复：
        并发 run 暂停/停止落在注入与恢复之间时，恢复被拒返回 False 而非抛 409，
        避免调用方回填输入重试造成同一条消息重复注入。
        """
        await self._require_run_session(run_id, session_id)
        run = await self._run_store.get(run_id)
        # 同步 SQLite 读不上事件循环：经 Orchestrator 缝隙走线程池
        if self._orch.pool.get(session_id) is None and not await asyncio.to_thread(
            self._orch.restore_session_from_store,
            session_id,
        ):
            raise ValueError(f"session not found: {session_id}")
        await self._orch.inject_prompt(session_id, prompt)
        self._notify_session_control_applied(run_id, session_id, "inject")
        if not allow_resume:
            return False
        can_resume = run.kind != RunKind.WORKFLOW or (
            run.status == RunStatus.RUNNING
            and run_id in self._runtimes
            and run_id not in self._stop_marked
        )
        if can_resume and self._orch.is_resumable(session_id):
            try:
                await self.resume_run_session(run_id, session_id)
            except (MemberResumeDeniedError, ValueError, RunNotFoundError, SessionNotInRunError):
                # 注入已完成（非幂等步骤）；恢复阶段的并发竞态失败只入队不抛错，
                # 避免调用方回填输入重试造成同一条消息二次注入。
                logger.warning(
                    "run %s session %s 恢复被拒或竞态失败，仅入队",
                    run_id,
                    session_id,
                    exc_info=True,
                )
                return False
            return True
        return False

    async def driver_projection(
        self, run_id: str, *, timeout_s: float = OP_REPLY_TIMEOUT_S
    ) -> dict[str, Any]:
        """驱动投影：经应答通道以 __projection__ 伪 op 取回。"""
        return await self.request_driver_op(run_id, "__projection__", {}, timeout_s=timeout_s)

    async def request_driver_op(
        self,
        run_id: str,
        op: str,
        args: Mapping[str, Any] | None = None,
        *,
        timeout_s: float = OP_REPLY_TIMEOUT_S,
    ) -> dict[str, Any]:
        """应答通道：请求经 FIFO 在 run 任务内执行，API 协程只等 Future。"""
        await self._run_store.get(run_id)  # 先查账：不在账 404、在账无运行时 503
        rt = self._runtimes.get(run_id)
        if rt is None or rt.poisoned:
            raise RuntimeError(f"run {run_id} 无可用驱动运行时")
        reply: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._notify(run_id, OpRequest(op=op, args=dict(args or {}), reply=reply))
        return await asyncio.wait_for(reply, timeout_s)

    async def submit_dispatch_barrier(
        self,
        session_id: str,
        kind: str,
        data: Mapping[str, Any],
        *,
        runtime: str = "",
        workspace: str = "writer",
        parent_tool_use_id: str = "",
        agent_id: str = "",
    ) -> BarrierOutcome:
        """派发屏障入口：会话宿主发起，经 FIFO 在 run 任务内执行核心动作。

        有界等待：窗口与后台排空同界，超窗按 fault.scope_quiesce_failed
        语义收口为拒绝，不让 hook 挂死。盘上没有树配置的 run（未开启工作
        区隔离）直接放行——不注入 cwd、不建组，与既有路径零差异；树只在
        盘上的 run（重启或重开之后）照常入队，由 run 任务重建树后执行。
        parent_tool_use_id 与 agent_id 标识子代理来源，缺省为空即父会话自身的调用。
        """
        affiliation = self._affiliation.resolve(session_id)
        if affiliation is None:
            return BarrierProceed()
        run_id = affiliation.run_id
        tree = self._workspace_trees.get(run_id)
        if tree is not None:
            wait_timeout_s = tree.barrier_wait_timeout_s
        else:
            config = await self._persisted_tree_config(run_id)
            if config is None:
                return BarrierProceed()
            wait_timeout_s = config.background_drain_timeout_s
        rt = self._runtimes.get(run_id)
        if rt is None or rt.poisoned:
            return BarrierDeny(
                reason=f"run {run_id} 无可用驱动运行时，屏障无法受理",
                fault=None,
            )
        reply: asyncio.Future[BarrierOutcome] = asyncio.get_running_loop().create_future()
        self._notify(
            run_id,
            DispatchBarrierRequest(
                kind=kind,
                session_id=session_id,
                runtime=runtime,
                context=affiliation.context,
                data=dict(data),
                reply=reply,
                workspace=workspace,
                parent_tool_use_id=parent_tool_use_id,
                agent_id=agent_id,
            ),
        )
        try:
            return await asyncio.wait_for(reply, timeout=wait_timeout_s)
        except TimeoutError:
            return BarrierDeny(
                reason=(
                    f"派发屏障等待超窗（>{wait_timeout_s}s），"
                    f"按 {fault_event_type(SCOPE_QUIESCE_FAILED_RULE)} 收口，本次副作用不执行"
                ),
                fault=fault_event_type(SCOPE_QUIESCE_FAILED_RULE),
            )

    async def record_subagent_start(self, session_id: str, data: Mapping[str, Any]) -> None:
        """子代理启动回执：按派发准入回填组检查点行的子会话映射（尽力）。"""
        affiliation = self._affiliation.resolve(session_id)
        if affiliation is None:
            return
        tree = self._workspace_trees.get(affiliation.run_id)
        if tree is None:
            return
        tool_use_id = str(data.get("parent_tool_use_id") or data.get("tool_use_id") or "")
        native_child = str(data.get("agent_id") or "")
        if not tool_use_id or not native_child:
            return
        admission = self._dispatch_admissions.get((affiliation.run_id, tool_use_id))
        if admission is None:
            return
        checkpoint_id, child_scope_id = admission
        try:
            await tree.arbiter.attach_child_session(checkpoint_id, child_scope_id, native_child)
        except Exception:
            logger.warning("子会话映射回填失败（不阻塞子代理运行）：checkpoint=%s", checkpoint_id)

    async def settle_subagent_result(self, session_id: str, data: Mapping[str, Any]) -> None:
        """子代理结果交付前的收尾入口：请求经 FIFO 在 run 任务内执行，钩子返回前完成。

        没有工作区树的 run（未开启工作区隔离）与无归属会话零动作。等待
        有界，窗口与派发屏障同界；超窗按 fault.scope_quiesce_failed 留痕
        后放行，不让钩子挂死——迟到的请求仍由 run 任务执行完毕。
        """
        affiliation = self._affiliation.resolve(session_id)
        if affiliation is None:
            return
        run_id = affiliation.run_id
        tree = self._workspace_trees.get(run_id)
        rt = self._runtimes.get(run_id)
        if tree is None or rt is None or rt.poisoned or rt.stopping:
            return
        reply: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._notify(
            run_id,
            SubagentResultRequest(
                session_id=session_id,
                context=affiliation.context,
                data=dict(data),
                reply=reply,
            ),
        )
        try:
            await asyncio.wait_for(reply, timeout=tree.barrier_wait_timeout_s)
        except TimeoutError:
            await self._emit_run_event_best_effort(
                rt,
                fault_event_type(SCOPE_QUIESCE_FAILED_RULE),
                {
                    **_subagent_fault_identity(session_id, data),
                    "error": f"子代理结果收尾等待超窗（>{tree.barrier_wait_timeout_s}s）",
                },
                context=affiliation.context,
            )

    async def validate_dispatch_replay(self, run_id: str, checkpoint_id: str) -> None:
        """replay_dispatch 前置校验：目标组须单 task 且无其他副作用。

        树不在内存而 run 仍有运行时则先重建再校验。本方法只做受理前的
        只读校验，回滚动作本身仍经驱动 op 在 run 任务内执行。
        """
        tree = await self._ensure_workspace_tree_if_running(run_id)
        if tree is None:
            raise ReplayRejectedError(
                f"run {run_id} 无工作区树运行时，无法校验重放组：{checkpoint_id}"
            )
        checkpoint = await tree.checkpoints.get(checkpoint_id)
        if (
            checkpoint is None
            or checkpoint.dispatch_anchor is None
            or checkpoint.trigger is not CheckpointTrigger.BEFORE_CHILD
        ):
            raise ReplayRejectedError(f"重放目标不是派发组检查点：{checkpoint_id}")
        anchor = checkpoint.dispatch_anchor
        tree.replay_ledger.require_single_task(anchor.dispatch_group_id, checkpoint_id)

    @staticmethod
    async def _reject_resumed_session(*args: Any, **kwargs: Any) -> Any:
        """编排器无 resume 入口时显式失败（不静默降级）。"""
        raise RuntimeError("编排器缺少 start_session_with_resume 入口，无法绑定恢复会话")

    async def load_driver_snapshot(self, run_id: str) -> dict[str, Any] | None:
        """最新驱动快照（读盘；配置重读与外部对账用）。"""
        return await self._run_store.load_driver_state(run_id)

    async def recover_at_boot(self, *, skip: Collection[str] = ()) -> None:
        """boot 恢复对账入口（skip 按步名单跳过，启动路径排障用）。"""
        await self._discard_restored_terminal_sessions()
        await self._recovery.recover_at_boot(skip=skip)

    # ── 驱动通道 ────────────────────────────────────────────────

    async def shutdown(self) -> None:
        """关停步骤位：各 run 并发收尾（单 run 有界，多 run 不串行累加）；账面保持非终态。

        不落终态——下次 boot 恢复链接续（可恢复性优于标记 stopped）；收尾
        经终态序列停成员、杀掉 run 键进程，decide 线程池随关停收敛。
        """
        runtimes = list(self._runtimes.items())
        if runtimes:
            # 各 run 的 FIFO/任务/进程注册表彼此隔离，并发收尾把 run 平面的
            # 耗时上界限制为单个卡死 run 的完整收尾时长，不随 run 数累加而超出
            # 调用方的全局关停预算
            results = await asyncio.gather(
                *(self._shutdown_run(run_id, rt) for run_id, rt in runtimes),
                return_exceptions=True,
            )
            for (run_id, _), result in zip(runtimes, results, strict=True):
                if isinstance(result, Exception):
                    logger.warning("run %s 关停收尾异常", run_id, exc_info=result)
        for pool in self._decide_pools.values():
            # 卡死驱动的线程不可杀：取消排队项后不等待在途调用，等待会把
            # 整个关停挂死在它身上；被弃调用的迟到结果本就无人消费
            pool.shutdown(wait=False, cancel_futures=True)
        self._decide_pools.clear()

    # ── 组件服务面（模块内组件类经此访问协调器状态与成员原语）──

    def get_runtime(self, run_id: str) -> RunRuntime | None:
        """run 运行时查询（None = 未拉起）。"""
        return self._runtimes.get(run_id)

    def get_run_task(self, run_id: str) -> asyncio.Task[None] | None:
        """run 任务查询（None = 无在途任务）。"""
        return self._tasks.get(run_id)

    def mark_stop_accepted(self, run_id: str) -> None:
        """置 stop 受理标记（_spawn 同步复查，关闭 mint→spawn 窗口竞态）。"""
        self._stop_marked.add(run_id)

    def notify_run(self, run_id: str, notification: RunNotification) -> None:
        """通知入队并唤醒对应 run 任务。"""
        self._notify(run_id, notification)

    def session_is_resumable(self, session_id: str) -> bool:
        """成员是否可恢复。"""
        return self._orch.is_resumable(session_id)

    def session_is_pausable(self, session_id: str) -> bool:
        """成员是否可挂起。"""
        return self._orch.is_pausable(session_id)

    def session_is_terminal(self, session_id: str) -> bool:
        """成员是否已终态。"""
        return self._orch.is_terminal(session_id)

    def get_session_facts(self, session_id: str) -> SessionFacts | None:
        """成员运行事实（不在池返回 None）。"""
        return self._orch.get_session_facts(session_id)

    async def resume_session(self, session_id: str) -> None:
        """恢复成员 session。"""
        await self._orch.resume_session(session_id)

    async def pause_session(self, session_id: str) -> None:
        """挂起成员 session。"""
        await self._orch.pause_session(session_id)

    async def stop_session(self, session_id: str) -> None:
        """停止成员 session。"""
        await self._orch.stop_session(session_id)

    async def inject_session_prompt(self, session_id: str, prompt: str) -> None:
        """向成员 session 注入 prompt。"""
        await self._orch.inject_prompt(session_id, prompt)

    async def kill_run_processes(self, run_id: str) -> None:
        """杀 run 键全部进程。"""
        await self._host.kill_processes(run_id)

    async def drain_run_process_spawns(self, run_id: str, timeout_s: float) -> None:
        """排空 run 键在途进程派生。"""
        await self._host.drain_process_spawns(run_id, timeout_s)

    async def drain_run_session_births(self, run_id: str, timeout_s: float) -> None:
        """排空 run 键在途成员出生。"""
        await self._host.drain_session_births(run_id, timeout_s)

    async def emit_run_event(
        self, run_id: str, event_type: str, data: Mapping[str, Any], *, context: str | None = None
    ) -> None:
        """发射 run 级事件。"""
        await self._host.emit_run_event(run_id, event_type, data, context=context)

    async def list_run_session_rows(self, run_id: str) -> list[SessionRunAffiliationRecord]:
        """run 成员归属行（含 paused_by 归属原样）。"""
        return await self._run_store.list_run_sessions(run_id)

    async def list_run_session_ids(self, run_id: str) -> list[str]:
        """run 成员 session id 清单。"""
        return await self._run_store.list_session_ids(run_id)

    async def set_session_paused_by(
        self, run_id: str, session_id: str, paused_by: str | None
    ) -> None:
        """记/清成员组暂停归属（paused_by：'run'/'user'/None）。"""
        await self._run_store.set_session_paused_by(run_id, session_id, paused_by)

    async def apply_pause_state(
        self, run_id: str, rt: RunRuntime | None, status: str, pause_reason: str | None
    ) -> None:
        """档位落账 + 派生缓存同步。"""
        await self._apply_pause_state(run_id, rt, status, pause_reason)

    async def finish_run(self, run_id: str, finish_status: str) -> RunRecord:
        """run 落终态并返回记录行。"""
        return (await self._run_store.finish(run_id, finish_status)).record

    async def sequence_run_terminal(
        self, run_id: str, *, finish_status: str | None, drain_timeout: float
    ) -> RunRecord | None:
        """终态收尾序列（None = 保持非终态）。"""
        return await self._terminal.sequence(
            run_id, finish_status=finish_status, drain_timeout=drain_timeout
        )

    def _freeze_program(self, run_id: str, program: Path) -> Path:
        """程序冻结进 run 根 program/：run 的执行不随外部文件漂移。

        拷贝范围是程序目录全树（多文件程序：SKILL.md、prompt_file 伴生文件）
        加组件面（manifest 程序走 vendor 记录、其余走 layr.yaml 扫描面，见
        _freeze_vendor_face / _freeze_scan_face）；入口文件名记 .workflow-controller-entry。
        """
        dest = self.run_data_root(run_id) / "program"
        dest.mkdir(parents=True, exist_ok=True)
        manifest = program.parent / "manifest.json"
        # .workflow-controller / .git 永不进冻结面：程序目录在 run 根祖先上时防自递归，
        # 宿主状态与版本库也不是程序资产；manifest.json 不进（vendor 校验路
        # 假设版本目录在仓库三层结构内，见 _freeze_vendor_face）
        shutil.copytree(
            program.parent,
            dest,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns(".workflow-controller", ".git", "manifest.json"),
        )
        if manifest.is_file():
            _freeze_vendor_face(manifest, dest)
        else:
            _freeze_scan_face(program.parent, dest)
        entry = dest / program.name
        (dest / _FROZEN_ENTRY_MARKER).write_text(f"{program.name}\n", encoding="utf-8")
        return entry

    # ── 恢复与关停 ──────────────────────────────────────────────

    async def _restore_workflow_run(self, run: RunRecord) -> RunDriver:
        """终态重开的驱动重建：优先快照 restore，缺快照回退冻结程序 re-create。"""
        factory = self._driver_factories.get(run.format)
        if factory is None:
            error = f"格式 {run.format} 没有驱动工厂，无法重开 run {run.run_id}"
            await self._run_store.finish(run.run_id, RunStatus.FAILED, error=error)
            raise ValueError(error)
        sessions = await self.list_run_sessions(run.run_id)
        try:
            blob = await self._run_store.load_driver_state(run.run_id)
            if blob is not None:
                return cast(
                    "RunDriver",
                    await self._guarded_factory_call(
                        run.format, factory.restore, run, blob, list(sessions)
                    ),
                )
            return cast(
                "RunDriver",
                await self._guarded_factory_call(
                    run.format,
                    factory.create,
                    run,
                    self._frozen_program(run.run_id),
                    dict(run.params),
                ),
            )
        except Exception as e:
            await self._run_store.finish(run.run_id, RunStatus.FAILED, error=f"unrecoverable: {e}")
            raise

    def _frozen_program(self, run_id: str) -> Path:
        """run 根冻结程序入口（.workflow-controller-entry 记录的入口文件名——目录内排序
        取首文件会错选 README.md / SKILL.md 这类伴生文件）。
        """
        program_dir = self.run_data_root(run_id) / "program"
        marker = program_dir / _FROZEN_ENTRY_MARKER
        if not marker.is_file():
            raise FileNotFoundError(f"run {run_id} 无驱动快照且 run 根无冻结程序清单（{marker}）")
        entry = program_dir / marker.read_text(encoding="utf-8").strip()
        if not entry.is_file():
            raise FileNotFoundError(f"run {run_id} 冻结程序清单指向的入口不在：{entry}")
        return entry

    async def _guarded_factory_call(self, fmt: str, fn: Any, *args: Any) -> Any:
        """工厂调用相同有界线程池 + 超时守护（与驱动调用一致）。"""
        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(self._decide_pool(fmt), functools.partial(fn, *args)),
            timeout=self._decide_timeout_s,
        )

    async def _broadcast_session_delete(self, session_id: str) -> None:
        """成员的 session_delete 清理通知：只分发、不落库——成员事件行已随删除事务清零。"""
        event = Event.create(
            session_id=session_id,
            iteration=0,
            event_type="session_delete",
            data={"session_id": session_id},
        )
        await self._bus.emit_ephemeral(event)

    async def _halt_run_activities(self, run: RunRecord) -> None:
        """删除前的运行面停机：已停的跳过，失败不阻断——记录以删除事务为准。"""
        if run.kind == RunKind.WORKFLOW and run.run_id in self._runtimes:
            with contextlib.suppress(Exception):
                await self._group_control.stop_run(run.run_id)
            return
        with contextlib.suppress(Exception):
            await self._terminal.sequence(
                run.run_id, finish_status=None, drain_timeout=STOP_DRAIN_TIMEOUT_S
            )

    def _run_session_info(self, row: Any, rt: RunRuntime | None) -> RunSessionInfo:
        """记录归属行叠池内实时态，组装 world/API 共用的 RunSessionInfo。"""
        facts = self._orch.get_session_facts(row.session_id)
        in_flight = bool(
            rt
            and (
                row.session_id in rt.in_flight_sessions
                or (row.context is not None and row.context in rt.in_flight_contexts)
            )
        )
        return RunSessionInfo(
            session_id=row.session_id,
            role=row.role,
            context=row.context,
            state=facts.state if facts else "",
            stop_reason=facts.stop_reason if facts else None,
            in_flight=in_flight,
        )

    async def _require_run_session(self, run_id: str, session_id: str) -> None:
        """成员操作前置：run 在账且 session 隶属该 run（单行定向查询）。"""
        await self._run_store.get(run_id)
        if not await self._run_store.session_in_run(run_id, session_id):
            raise SessionNotInRunError(f"session {session_id} 不隶属 run {run_id}")

    # ── 决策循环 ────────────────────────────────────────────────

    def _notify_session_control_applied(self, run_id: str, session_id: str, operation: str) -> None:
        """成员操作回执：仅进在册 run 任务的 FIFO。"""
        if run_id in self._runtimes:
            self._notify(run_id, SessionControlApplied(session_id=session_id, operation=operation))

    async def _discard_restored_terminal_sessions(self) -> None:
        """按驱动工厂的内存保留策略清理已恢复的终态成员。"""
        if not await self._run_store.has_runs_table():
            return
        for run in await self._run_store.list_runs():
            factory = self._driver_factories.get(run.format)
            if not getattr(factory, "discard_terminal_sessions_at_boot", False):
                continue
            for member in await self._run_store.list_run_sessions(run.run_id):
                self._orch.discard_terminal_session(member.session_id)

    async def _reconcile_recovered_run_state(self, run: RunRecord) -> None:
        """boot 后无驱动 run 的状态重算：隐式投影随 ghost 事实、容器全员终态即收尾。"""
        if run.kind == RunKind.IMPLICIT:
            await self._project_implicit(run.run_id)
        elif run.kind == RunKind.CONTAINER:
            await self._close_container_if_idle(run.run_id, ghost_recalc=True)

    async def _shutdown_run(self, run_id: str, rt: RunRuntime) -> None:
        """单 run 关停收尾：投递 stopped、排空窗（超时走代办快照）后走终态序列。"""
        task = self._tasks.get(run_id)
        if task is not None and not task.done():
            self._notify(run_id, ControlNotification(kind=ControlKind.STOPPED))
            try:
                await asyncio.wait_for(asyncio.shield(task), STOP_DRAIN_TIMEOUT_S)
            except TimeoutError:
                await self._surrogate_snapshot(run_id, rt, task)
            except asyncio.CancelledError:
                if not task.cancelled():
                    raise  # 被取消的是关停自身：保住外层取消语义
                # run 任务被并发路径取消（stop_run 超时兜底 / delete_run 链）：
                # 按意外退场处理，终态序列照常执行，不向外抛出打断关停序
                logger.warning("run %s 任务在关停排空窗内被并发取消", run_id)
            except Exception:
                logger.exception("run %s 关停排空异常", run_id)
        await self._terminal.sequence(
            run_id, finish_status=None, drain_timeout=STOP_DRAIN_TIMEOUT_S
        )
        for waiter in list(rt.await_tasks):
            waiter.cancel()

    async def _surrogate_snapshot(
        self, run_id: str, rt: RunRuntime, task: asyncio.Task[None]
    ) -> None:
        """排空超时后的代办快照（run 任务未自行退出，由关停方代为落盘）：
        「驱动调用全在 run 任务内」的唯一显式例外。

        cancel 杀不死执行中的驱动线程——线程退出（而非任务退出）才算拿到
        单写者：依次有界等待任务与在途驱动线程退出，任一等不到即放弃快照
        （撕裂内存落盘成唯一恢复源，不如无快照——boot 恢复落确定性 failed，
        留痕可查）；快照超时/落盘失败同样留痕放弃。
        """
        task.cancel()
        if not await self._wait_task_exit(task, TASK_CANCEL_EXIT_TIMEOUT_S):
            logger.warning(
                "run %s 代办快照放弃：任务取消后 %ss 内未退出",
                run_id,
                TASK_CANCEL_EXIT_TIMEOUT_S,
            )
            return
        if not await self._drain_driver_calls(rt, SURROGATE_THREAD_DRAIN_S):
            logger.warning(
                "run %s 代办快照放弃：%ss 内在途驱动线程未退出",
                run_id,
                SURROGATE_THREAD_DRAIN_S,
            )
            return
        try:
            blob = await asyncio.wait_for(
                self._call_driver_soft(rt, "snapshot"),
                timeout=SURROGATE_SNAPSHOT_TIMEOUT_S,
            )
            await self._host.persist_driver_state(run_id, blob)
        except Exception as e:
            logger.warning(
                "run %s 代办快照放弃（%s: %s）",
                run_id,
                type(e).__name__,
                e,
            )

    def _spawn(
        self,
        run_id: str,
        fmt: str,
        driver: RunDriver,
        *,
        max_parallel: int | None = None,
        session_ids: Iterable[str] = (),
    ) -> None:
        """run 任务唯一 spawn 口；全部驱动调用发生在此任务内。

        session_ids 为账面 session 种子（重开/恢复的运行时据此对账容量与 AMT）。
        """
        if run_id in self._stop_marked:
            self._stop_marked.discard(run_id)
            # stop 已在 mint→spawn 窗口内受理收口：不再拉起（账面已终态）
            return
        rt = RunRuntime(
            run_id=run_id,
            format=fmt,
            driver=driver,
            max_parallel=max_parallel,
        )
        bind_checkpoint = getattr(driver, "bind_checkpoint", None)
        if callable(bind_checkpoint):
            loop = asyncio.get_running_loop()

            def emit_checkpoint(action: PersistDriverState) -> None:
                # 驱动线程等宿主提交；SQLite I/O 留在 Run 平面，事件循环不阻塞。
                try:
                    caller_loop = asyncio.get_running_loop()
                except RuntimeError:
                    caller_loop = None
                if caller_loop is loop:
                    raise RuntimeError("驱动 checkpoint 不得在宿主事件循环线程执行")
                asyncio.run_coroutine_threadsafe(
                    self._commit_checkpoint(rt, action),
                    loop,
                ).result()

            bind_checkpoint(emit_checkpoint)
        rt.session_ids = set(session_ids)
        self._runtimes[run_id] = rt
        task = asyncio.create_task(self._run_loop(rt), name=f"run:{run_id}")
        self._tasks[run_id] = task
        task.add_done_callback(functools.partial(self._on_run_task_done, run_id))

    async def _commit_checkpoint(
        self,
        rt: RunRuntime,
        action: PersistDriverState,
    ) -> None:
        async with rt.checkpoint_lock:
            if rt.poisoned or self._runtimes.get(rt.run_id) is not rt:
                return  # 已弃用的驱动线程不得覆写新运行时或终态快照
            await self._run_store.save_driver_state(rt.run_id, action.blob)

    def _on_run_task_done(self, run_id: str, task: asyncio.Task[None]) -> None:
        """run 任务退出回调：清运行时/任务注册表与 stale 停止标记，异常留痕。"""
        if self._tasks.get(run_id) is task:
            self._tasks.pop(run_id, None)
            # 旧任务迟到回调不得清掉同一 run 新任务的停止受理标记。
            self._stop_marked.discard(run_id)
        tree = self._workspace_trees.pop(run_id, None)
        if tree is not None:
            tree.close()  # 释放账本连接（磁盘产物留给 GC）
        for key in [k for k in self._dispatch_admissions if k[0] == run_id]:
            self._dispatch_admissions.pop(key, None)
        if not task.cancelled() and task.exception() is not None:
            logger.error("run %s 任务异常结束", run_id, exc_info=task.exception())

    def _notify(self, run_id: str, notification: RunNotification) -> None:
        """通知入队并唤醒对应 run 任务。"""
        rt = self._runtimes.get(run_id)
        if rt is None:
            return
        if isinstance(notification, SessionActivated):
            rt.in_flight = max(0, rt.in_flight - 1)
            rt.in_flight_contexts.discard(notification.context or "")
            rt.in_flight_sessions.discard(notification.session_id)
            if notification.ok:
                rt.session_ids.add(notification.session_id)
                rt.amt_injected = False
        rt.notifications.append(notification)
        rt.wake.set()

    async def _on_session_outcome_event(self, event: Any) -> None:
        """终态事件路由：按归属进通知 FIFO，再做 run 态对账。"""
        aff = self._affiliation.resolve(event.session_id)
        if aff is not None and aff.run_id in self._runtimes:
            try:
                outcome = SessionOutcome(**event.data)
            except TypeError:
                outcome = None
            if outcome is not None:
                self._notify(aff.run_id, SessionOutcomeNotification(outcome=outcome))
        await self._reconcile_session_run(aff)

    async def _on_session_state_event(self, event: Any) -> None:
        """状态事件观察：implicit 投影与 container 收尾对账入口。"""
        await self._reconcile_session_run(self._affiliation.resolve(event.session_id))

    async def _reconcile_session_run(self, aff: SessionRunAffiliation | None) -> None:
        """run 态对账：implicit 重算投影（唯一写者）、container 评估全员终态收尾。"""
        if aff is None:
            return
        kind = await self._run_kind(aff.run_id)
        if kind == RunKind.IMPLICIT:
            await self._project_implicit(aff.run_id)
        elif kind == RunKind.CONTAINER:
            await self._close_container_if_idle(aff.run_id)

    async def _run_kind(self, run_id: str) -> str:
        """run 类别懒缓存（kind 不可变；避免逐事件读账面）。"""
        kind = self._run_kinds.get(run_id)
        if kind is None:
            kind = (await self._run_store.get(run_id)).kind
            self._run_kinds[run_id] = kind
        return kind

    async def _project_implicit(self, run_id: str) -> None:
        """implicit 投影唯一写点：任一成员运行→running，否则 paused(projection)；有变才写。"""
        any_running = False
        for row in await self._run_store.list_run_sessions(run_id):
            facts = self._orch.get_session_facts(row.session_id)
            if facts is not None and facts.state == "running":
                any_running = True
                break
        status, reason = implicit_projection_status(any_running)
        run = await self._run_store.get(run_id)
        if run.status == status and run.pause_reason == reason:
            return
        with contextlib.suppress(Exception):
            await self._run_store.update_pause_state(run_id, status, reason)

    async def _close_container_if_idle(self, run_id: str, *, ghost_recalc: bool = False) -> None:
        """container 收尾规则：全员终态即 finish(completed)（与决策循环共用 Orchestrator 终态谓词）。

        ghost_recalc 是 boot 重算形态：恢复成员无活 runtime，改用 ghost 谓词
        （陈旧 loop_status=yes 不代表成员仍在跑，与隐式投影的 state 判据一致）。
        """
        run = await self._run_store.get(run_id)
        if run.kind != RunKind.CONTAINER or run.status in TERMINAL_STATUSES:
            return
        session_ids = await self._run_store.list_session_ids(run_id)
        final = self._ghost_final if ghost_recalc else self._orch.is_terminal
        # 重启窗内的终态是瞬态（session 平面直通重启的 stop→重激活之间），不计收口
        if all(final(sid) and not self._orch.is_restarting(sid) for sid in session_ids):
            with contextlib.suppress(Exception):
                await self._run_store.finish(run_id, RunStatus.COMPLETED)

    def _ghost_final(self, sid: str) -> bool:
        """boot 重算的成员终态谓词：ghost 无活 runtime——state 非 running 即不再
        产生结局，loop_status 沿用的 'yes' 是崩溃残留；idle+user_pause 是可恢复
        断点（与 is_terminal 的暂停豁免语义相同），不计终态。
        """
        facts = self._orch.get_session_facts(sid)
        if facts is None:
            return True
        if facts.state == "running":
            return False
        return not (facts.state == "idle" and facts.stop_reason == "user_pause")

    async def _apply_pause_state(
        self,
        run_id: str,
        rt: RunRuntime | None,
        status: str,
        pause_reason: str | None,
    ) -> None:
        """档位落账 + 派生缓存同步（缓存先置使冻结即时生效，账面写失败回滚）。"""
        if rt is None:
            await self._run_store.update_pause_state(run_id, status, pause_reason)
            return
        prev = (rt.paused, rt.pause_reason)
        rt.paused, rt.pause_reason = status == RunStatus.PAUSED, pause_reason
        rt.pause_synced = True
        try:
            await self._run_store.update_pause_state(run_id, status, pause_reason)
        except BaseException:
            rt.paused, rt.pause_reason = prev
            raise

    def _all_sessions_terminal(self, rt: RunRuntime) -> bool:
        """全员终态谓词：无在途/挂起且成员全终态；首轮后零成员（真空为真）防挂死。"""
        # 首轮 decide 前不注入（驱动还没派发机会）；其后零成员（全员出生
        # 失败，失败回执不进 session_ids）同属全员终态，防 run 无 wake 源挂死
        if rt.in_flight or rt.deferred or not (rt.session_ids or rt.rounds >= 1):
            return False
        return all(self._orch.is_terminal(sid) for sid in rt.session_ids)

    def _should_defer(self, rt: RunRuntime, action: RunAction, *, terminal: bool) -> bool:
        """派发点冻结与准入门：终态轮冻 Start 类；暂停窗冻 Start 类与进程；容量不足挂起。"""
        if isinstance(action, START_LIKE_ACTIONS):
            return terminal or self._user_paused(rt) or not self._admission.admit(rt, action)
        if isinstance(action, StartProcess):
            return self._user_paused(rt) and not terminal
        return False

    async def _run_loop(self, rt: RunRuntime) -> None:
        """决策循环体：事件驱动唤醒，无轮询；退出与等待不交叠。"""
        state = LoopState.BOOTING
        first = True  # 首轮无通知也 decide（驱动先拿到初始 world）
        carried: list[ControlNotification] = []  # 暂停分支已消费的终态控制，携带入主路径收尾
        try:
            # rt 缺席窗口（mint→spawn / 重开 restore→spawn）受理的组控只落账面：拉起即回灌；
            # 期间组控直写已同步过（pause_synced），回灌让位不覆盖
            await self._resync_pause_from_ledger(rt)
            while state is not LoopState.EXITED:
                if rt.poisoned:
                    break  # 失败收尾已落账（含异常被吞的路径），不再推进
                terminal_pending = bool(carried) or any(
                    isinstance(n, ControlNotification) and n.is_terminal() for n in rt.notifications
                )
                state = next_loop_state(
                    state,
                    user_paused=self._user_paused(rt),
                    terminal=terminal_pending,
                )
                if state is LoopState.PAUSED:
                    # 暂停期排空控制通知与应答请求；成员事件滞留 FIFO，恢复后一并
                    # 进 decide。应答通道不属 decide 冻结面——投影等只读请求被一并
                    # 冻结会把 /state 饿到超时，降级读到快照形状而非投影形状
                    carried = await self._run_paused_round(rt)
                    continue
                notes, ctrls = await self._drain_round_notifications(rt)
                terminal = await self._settle_round_terminality(rt, carried, ctrls)
                carried = []
                if not notes and not rt.amt_injected and self._all_sessions_terminal(rt):
                    rt.amt_injected = True
                    notes = [AllSessionsTerminal()]
                has_round_activity = bool(notes or ctrls or terminal)
                ran_decide = has_round_activity or first
                # decide 轮与首轮合并判定：FinishRun 落账即整循环退出（finally 清注册表）
                finish_requested = ran_decide and await self._run_decide_round(rt, notes, terminal)
                if finish_requested:
                    return
                if ran_decide:
                    first = False
                if terminal:
                    state = await self._finish_terminal_round(rt)
                    continue
                if not self._idle(rt) or rt.stopping:
                    continue
                if await self._wait_idle_round(rt):
                    continue  # 最后一条结局使全员终态：不挂起，回顶注入防挂起通知
        except _DriverError:
            pass  # 账面已收口
        except Exception as e:
            logger.exception("run %s 决策循环异常退出", rt.run_id)
            await self._fail_run(rt, "run_loop_error", f"{type(e).__name__}: {e}")
        finally:
            async with rt.checkpoint_lock:
                if self._runtimes.get(rt.run_id) is rt:
                    self._runtimes.pop(rt.run_id, None)

    async def _resync_pause_from_ledger(self, rt: RunRuntime) -> None:
        """拉起时按存储的暂停记录恢复状态（组控已同步过则让位不覆盖）。"""
        ledger = await self._run_store.get(rt.run_id)
        if not rt.pause_synced:
            rt.paused = ledger.status == RunStatus.PAUSED
            rt.pause_reason = ledger.pause_reason

    async def _finish_terminal_round(self, rt: RunRuntime) -> LoopState:
        """终态收尾轮：最终快照/投影落盘后交 DRAINING（迁移表送 EXITED）。"""
        await self._persist_snapshot(rt)
        await self._persist_final_projection(rt)
        rt.stopping = True
        return LoopState.DRAINING

    async def _run_paused_round(self, rt: RunRuntime) -> list[ControlNotification]:
        """暂停轮：排空控制/应答/宿主服务请求、落快照；返回待携带入主路径收尾的终态控制。"""
        ctrls, ops, barriers, results = self._drain_controls(rt)
        for c in ctrls:
            await self._call_control(rt, c)
        await self._serve_ops(rt, ops)
        await self._serve_dispatch_barriers(rt, barriers)
        await self._serve_subagent_results(rt, results)
        await self._persist_snapshot(rt)
        carried = [c for c in ctrls if c.is_terminal()]
        if not carried:
            await rt.wake.wait()
            rt.wake.clear()
        return carried

    async def _drain_round_notifications(
        self, rt: RunRuntime
    ) -> tuple[list[RunNotification], list[ControlNotification]]:
        """排空通知 FIFO：控制类即时处理、应答与宿主服务请求类服务，剩余成员事件按序返回。"""
        notes = self._drain_notifications(rt)
        ctrls = [n for n in notes if isinstance(n, ControlNotification)]
        ops = [n for n in notes if isinstance(n, OpRequest)]
        barriers = [n for n in notes if isinstance(n, DispatchBarrierRequest)]
        results = [n for n in notes if isinstance(n, SubagentResultRequest)]
        notes = [
            n
            for n in notes
            if not isinstance(
                n, (ControlNotification, OpRequest, DispatchBarrierRequest, SubagentResultRequest)
            )
        ]
        for c in ctrls:
            await self._call_control(rt, c)
        await self._serve_ops(rt, ops)
        await self._serve_dispatch_barriers(rt, barriers)
        await self._serve_subagent_results(rt, results)
        return notes, ctrls

    async def _settle_round_terminality(
        self,
        rt: RunRuntime,
        carried: list[ControlNotification],
        ctrls: list[ControlNotification],
    ) -> list[ControlNotification]:
        """受理终态控制（冻结挂起出生）或按容量补放挂起动作；返回本轮终态控制。"""
        terminal = carried + [c for c in ctrls if c.is_terminal()]
        if terminal:
            rt.terminal_accepted = True
            rt.deferred.clear()  # 终态收尾轮冻结在途出生（旧挂起不再派发）
            rt.deferred_starts = 0
        elif not self._user_paused(rt):
            # 容量已释放则按序补放挂起动作（冻结窗/终态轮不泵）
            for act in self._admission.pump(rt):
                await self._dispatch(rt, act)
        return terminal

    async def _run_decide_round(
        self, rt: RunRuntime, notes: list[RunNotification], terminal: list[ControlNotification]
    ) -> bool:
        """有活动轮（或首轮）的 decide 与动作派发；返回 True 表示 FinishRun 已落账退出。"""
        actions = await self._decide(rt, notes)
        for act in actions:
            if self._should_defer(rt, act, terminal=bool(terminal)):
                self._admission.defer(rt, act)
                continue
            self._admission.charge(rt, act)
            if isinstance(act, FinishRun):
                await self._apply_driver_finish(rt, act)
                return True
            await self._dispatch(rt, act)
        return False

    async def _wait_idle_round(self, rt: RunRuntime) -> bool:
        """空闲挂起前落快照；全员终态且未注入时回顶注入（返回 True 表示回顶）。"""
        await self._persist_snapshot(rt)
        if not rt.amt_injected and self._all_sessions_terminal(rt):
            return True
        await rt.wake.wait()
        rt.wake.clear()
        return False

    async def _decide(self, rt: RunRuntime, notes: list[RunNotification]) -> list[RunAction]:
        """一轮决策：先组装 world 快照，再调驱动 decide。"""
        rt.rounds += 1
        world = await self._world(rt)
        return cast("list[RunAction]", await self._call_driver(rt, "decide", world, notes))

    async def _world(self, rt: RunRuntime) -> RunWorld:
        """每轮 decide 前即时组装的输入快照，不缓存陈值。"""
        run = await self._run_store.get(rt.run_id)
        rows = await self._run_store.list_run_sessions(rt.run_id)
        return RunWorld(
            run=run,
            sessions=tuple(self._run_session_info(row, rt) for row in rows),
            processes=tuple(self._host.process_infos(rt.run_id)),
            in_flight=rt.in_flight,
        )

    async def _call_control(self, rt: RunRuntime, control: ControlNotification) -> None:
        """控制通知转驱动 on_control（同款守护）；即时动作只放行事件类。"""
        result = await self._call_driver(rt, "on_control", control)
        for act in result or ():
            if isinstance(act, (*START_LIKE_ACTIONS, StartProcess)):
                continue  # 出生类不在控制反应里派发（冻结窗语义由派发点把关）
            await self._dispatch(rt, act)

    async def _serve_ops(self, rt: RunRuntime, ops: list[OpRequest]) -> None:
        """消费 OpRequest：run 任务内调驱动方法并回填 Future（异常转应答错误）。

        操作异常只回错误应答、不中毒 run——操作是外部请求（如未知 op 名），
        不构成驱动失效；中毒语义留给 decide/snapshot 这类决策面失败。
        """
        for op in ops:
            try:
                result = await self._execute_op(rt, op)
            except _DriverError:
                self._settle_op_reply(op, RuntimeError(f"驱动 {rt.format} 不可用"))
            except Exception as e:
                self._settle_op_reply(op, e)
            else:
                self._settle_op_reply(op, result)

    async def _serve_dispatch_barriers(
        self, rt: RunRuntime, requests: list[DispatchBarrierRequest]
    ) -> None:
        """消费派发屏障请求：执行核心动作后回填 Future，与 decide 串行不交错。"""
        for req in requests:
            try:
                outcome = await self._execute_dispatch_barrier(rt, req)
            except Exception as e:
                self._settle_barrier_reply(req, BarrierDeny(reason=f"{type(e).__name__}: {e}"))
            else:
                self._settle_barrier_reply(req, outcome)

    async def _execute_dispatch_barrier(
        self, rt: RunRuntime, req: DispatchBarrierRequest
    ) -> BarrierOutcome:
        """执行屏障核心动作链：能力门 → 子代理自身的副作用直接放行 → 编排状态先持久化 → 组仲裁 → 回 cwd/放行。

        屏障内一切 I/O 由本方法（宿主侧）执行，hook 不直写核心；编排状态
        版本先于文件提交（与生命周期检查点同一持久化顺序）。树不在内存
        时先按盘上配置重建（重启或重开后的首个屏障）；没有树的 run 放行。
        """
        tree = await self._ensure_workspace_tree(rt)
        if tree is None:
            return BarrierProceed()
        caps = dispatch_capabilities_for(req.runtime)
        strict = tree.strict_barrier
        if req.kind == "dispatch":
            block = writer_dispatch_block_reason(caps, strict=strict)
        else:
            block = group_arbitration_block_reason(caps, strict=strict)
        if block is not None:
            return BarrierDeny(reason=block)
        if await tree.passes_without_arbitration(
            kind=req.kind, parent_tool_use_id=req.parent_tool_use_id, agent_id=req.agent_id
        ):
            # 子代理自身的副作用：写的是子代理自己的工作区，不进父作用域的组仲裁
            return BarrierProceed()
        parent_scope = await self._barrier_parent_scope(tree, req)
        child: ChildDispatchRequest | None = None
        preallocated = ""
        if req.kind == "dispatch":
            scope_key, client_op_id = tree.next_child_identity(parent_scope.scope_id)
            child = ChildDispatchRequest(
                scope_key=scope_key,
                client_op_id=client_op_id,
                owner_ref=f"session={req.session_id}",
                workspace=WORKSPACE_READER if req.workspace == WORKSPACE_READER else "writer",
            )
            preallocated = scope_key
        anchor = self._barrier_anchor(req, preallocated)
        orchestration_ref = await self._persist_orchestration_ref(rt)
        admission = await tree.arbiter.admit_side_effect(
            parent_scope_id=parent_scope.scope_id,
            anchor=anchor,
            child=child,
            barrier_quality=(BarrierQuality.PRECISE if strict else BarrierQuality.LATE_BARRIER),
            orchestration_state_ref=orchestration_ref,
            tool_name=str(req.data.get("tool_name") or ""),
            tool_input=req.data.get("tool_input"),
        )
        if child is not None and admission.child_scope_id:
            self._dispatch_admissions[(rt.run_id, anchor.tool_use_id)] = (
                admission.checkpoint_id,
                admission.child_scope_id,
            )
        return BarrierProceed(child_cwd=admission.child_cwd)

    async def _active_scope_of_node(
        self, tree: WorkspaceTreeRuntime, node_id: str
    ) -> ScopeRecord | None:
        """节点的活动作用域；没有返回 None。

        只认活动态：节点重试会废弃旧作用域并换键重建，旧作用域仍以相同属主
        留账。不应出现多个活动作用域并存（重试恒先废弃旧作用域）；并存时
        会话跑在最近建区的一个里，取它并留痕。
        """
        active = [
            record
            for record in await tree.manager.list_scopes()
            if record.owner_ref == node_id and record.state is ScopeState.ACTIVE
        ]
        if len(active) > 1:
            logger.warning(
                "节点 %s 有多个活动作用域，取最近建立的 %s：%s",
                node_id,
                active[-1].scope_id,
                [record.scope_id for record in active],
            )
        return active[-1] if active else None

    async def _barrier_parent_scope(
        self, tree: WorkspaceTreeRuntime, req: DispatchBarrierRequest
    ) -> ScopeRecord:
        """按会话归属上下文解析屏障父作用域（node=<id> 或 node=<id>/<相位>）。

        执行与验收会话共用节点作用域：相位后缀只区分成员，不区分作用域。
        没有活动作用域时拒绝，拒绝原因带出该节点现有作用域及其状态。
        """
        node_id = _node_id_of_context(req.context)
        if node_id is None:
            raise RuntimeError(f"会话 {req.session_id} 缺少节点上下文，屏障无法定位父作用域")
        if not node_id:
            raise RuntimeError(f"会话 {req.session_id} 节点上下文为空，屏障无法定位父作用域")
        scope = await self._active_scope_of_node(tree, node_id)
        if scope is not None:
            return scope
        owned_states = (
            "、".join(
                f"{record.scope_id}={record.state.value}"
                for record in await tree.manager.list_scopes()
                if record.owner_ref == node_id
            )
            or "无"
        )
        raise RuntimeError(f"屏障父作用域未找到：node={node_id}（现有作用域：{owned_states}）")

    async def _artifact_base_dir(self, rt: RunRuntime, context: str | None) -> str | None:
        """产物读取的基准目录：发起方节点有活动作用域时是其工作区，否则返回 None（读根检出）。

        未开启工作区隔离、树重建不出、节点没有活动作用域（产物已集成释放）
        都回落根检出。读取是只读动作，树的问题只留痕、不中断 run。
        """
        node_id = _node_id_of_context(context)
        if not node_id:
            return None
        try:
            tree = await self._ensure_workspace_tree(rt)
            if tree is None:
                return None
            scope = await self._active_scope_of_node(tree, node_id)
            if scope is None:
                return None
            return self._scope_workspace_path(scope)
        except Exception as e:
            logger.warning(
                "节点 %s 的产物读取基准目录解析失败，回落根检出：%s: %s",
                node_id,
                type(e).__name__,
                e,
            )
            return None

    @staticmethod
    def _barrier_anchor(req: DispatchBarrierRequest, preallocated_scope_id: str) -> DispatchAnchor:
        """由屏障载荷构造锚点草稿（组标识由组仲裁回填）。"""
        data = req.data
        message_key = str(data.get("prompt_id") or data.get("message_id") or "")
        if not message_key:
            raise RuntimeError("屏障载荷缺少消息标识，无法形成派发锚点")
        return DispatchAnchor(
            parent_native_session_id=str(
                data.get("native_session_id") or data.get("session_id") or ""
            ),
            parent_message_id=message_key,
            tool_use_id=str(data.get("tool_use_id") or ""),
            preallocated_scope_id=preallocated_scope_id,
            exclude_message_id=message_key,
            dispatch_group_id="",
        )

    @staticmethod
    def _settle_barrier_reply(req: DispatchBarrierRequest, outcome: BarrierOutcome) -> None:
        """回填屏障应答；调用方已超时离场时留痕（该次派发按拒绝收口）。"""
        reply: asyncio.Future[BarrierOutcome] = req.reply
        if reply.done():
            logger.warning(
                "派发屏障 %s 应答晚于调用方等待上界：结果未送达: %r",
                req.kind,
                outcome,
            )
            return
        reply.set_result(outcome)

    async def _serve_subagent_results(
        self, rt: RunRuntime, requests: list[SubagentResultRequest]
    ) -> None:
        """消费子代理结果收尾请求：run 任务内执行收尾链，与 decide 串行不交错。"""
        for req in requests:
            await self._settle_subagent_result(rt, req)
            if not req.reply.done():
                # 调用方已超窗或取消离场时无人等这份回执，照常结束
                req.reply.set_result(None)

    async def _settle_subagent_result(self, rt: RunRuntime, req: SubagentResultRequest) -> None:
        """执行子代理结果收尾链：定位子作用域 → 终态提交 → 并入父作用域 → 释放工作区。

        失败只留痕、不抛回调用方：集成被拒落 fault.workspace_conflict，闸门
        不可静止落 fault.scope_quiesce_failed，其余失败只告警。三种情况下
        子作用域都保留不释放，供排查。
        """
        tree = self._workspace_trees.get(rt.run_id)
        if tree is None:
            return
        agent_id = str(req.data.get("agent_id") or "")
        scope_id = await tree.child_scope_for_session(agent_id)
        if scope_id is None:
            logger.warning(
                "子代理 %s 完成但未找到子作用域，结果未并入父作用域：session=%s",
                agent_id,
                req.session_id,
            )
            return
        identity = _subagent_fault_identity(req.session_id, req.data)
        try:
            orchestration_ref = await self._persist_orchestration_ref(rt)
            outcome = await tree.settle_subagent_scope(
                scope_id, orchestration_state_ref=orchestration_ref
            )
        except ScopeQuiesceFailedError as error:
            await self._emit_run_event_best_effort(
                rt,
                fault_event_type(SCOPE_QUIESCE_FAILED_RULE),
                {**identity, "error": str(error)},
                context=req.context,
            )
            return
        except Exception as error:
            logger.warning(
                "子代理 %s 的作用域 %s 收尾失败，工作区保留待查：%s: %s",
                agent_id,
                scope_id,
                type(error).__name__,
                error,
            )
            return
        if isinstance(outcome, CoreScopeIntegrationRejected):
            await self._emit_run_event_best_effort(
                rt,
                fault_event_type(WORKSPACE_CONFLICT_RULE),
                {**_workspace_conflict_payload(outcome.conflict), **identity},
                context=req.context,
            )

    async def _execute_op(self, rt: RunRuntime, op: OpRequest) -> Any:
        """单个操作的执行体：projection / world op / 普通 op 三分支。"""
        if op.op == "__projection__":
            return await self._call_driver_soft(rt, "projection")
        world_handler = getattr(rt.driver, "handle_world_op", None)
        if not callable(world_handler):
            return await self._call_driver_soft(rt, "handle_op", op.op, dict(op.args))
        result, changed, events = await self._call_driver_soft(
            rt,
            "handle_world_op",
            await self._world(rt),
            op.op,
            dict(op.args),
        )
        if changed:
            await self._persist_snapshot(rt)
        await self._dispatch_op_events(rt, events)
        return result

    async def _dispatch_op_events(self, rt: RunRuntime, events: list[RunAction]) -> None:
        """应答通道只派发事件与定时器回收，不借读路径出生成员。"""
        for act in events:
            if isinstance(act, (EmitEvent, CancelTimer)):
                await self._dispatch(rt, act)

    async def _call_driver_soft(self, rt: RunRuntime, method: str, *args: Any) -> Any:
        """有界线程池调用但不因异常收尾 run（操作通道专用，见 _serve_ops）。"""
        fn = getattr(rt.driver, method)
        return await asyncio.wait_for(
            asyncio.wrap_future(self._submit_driver_call(rt, fn, *args)),
            timeout=_OP_DRIVER_WAIT_S,
        )

    async def _dispatch(self, rt: RunRuntime, action: RunAction) -> None:
        """动作 → 执行面；Start 类派发即返回，回执经通知回投。"""
        if await self._dispatch_session_action(rt, action):
            return
        if await self._dispatch_process_action(rt, action):
            return
        if await self._dispatch_workspace_action(rt, action):
            return
        raise NotImplementedError(f"动作 {type(action).__name__} 的执行面尚未落地")

    async def _dispatch_session_action(self, rt: RunRuntime, action: RunAction) -> bool:
        """成员类动作派发；返回 False 表示 action 不属本组。"""
        if isinstance(action, StartSession):
            rt.amt_injected = False
            self._host.start_run_session(rt.run_id, action.spec, action.role, action.context)
        elif isinstance(action, RestartSession):
            rt.amt_injected = False
            self._host.restart_run_session(
                rt.run_id,
                action.session_id,
                action.policy,
                spec_override=action.spec_override,
                context_override=action.context_override,
            )
        elif isinstance(action, AwaitSessions):
            task = asyncio.create_task(
                self._await_sessions_task(rt, action),
                name=f"run-await:{rt.run_id}",
            )
            rt.await_tasks.add(task)
            task.add_done_callback(rt.await_tasks.discard)
        elif isinstance(action, ReplaySessionOutcome):
            event = await asyncio.to_thread(
                self._orch.event_router.get_latest_session_outcome_sync,
                action.session_id,
            )
            outcome = None
            if event is not None:
                with contextlib.suppress(TypeError, ValueError):
                    outcome = SessionOutcome(**event.data)
            self._notify(rt.run_id, SessionOutcomeReplayed(action.session_id, outcome))
        elif isinstance(action, DiscardSession):
            await self._host.discard_run_session(rt.run_id, action.session_id)
        else:
            return False
        return True

    async def _dispatch_process_action(self, rt: RunRuntime, action: RunAction) -> bool:
        """进程/定时器/事件/持久化类动作派发；返回 False 表示 action 不属本组。"""
        if isinstance(action, StartProcess):
            self._host.start_process(
                rt.run_id,
                action.cmd,
                action.cwd,
                env=dict(action.env) if action.env else None,
                context=action.context,
                stream=action.stream,
            )
        elif isinstance(action, KillProcesses):
            await self._host.kill_processes(rt.run_id, action.context)
        elif isinstance(action, ArmTimer):
            self._host.arm_timer(rt.run_id, action.timer_id, action.delay_s)
        elif isinstance(action, CancelTimer):
            self._host.cancel_timer(rt.run_id, action.timer_id)
        elif isinstance(action, ReadArtifact):
            base_dir = await self._artifact_base_dir(rt, action.context)
            await self._host.read_artifact(
                rt.run_id, action.path, action.field, action.context, base_dir=base_dir
            )
        elif isinstance(action, EmitEvent):
            await self._host.emit_run_event(
                rt.run_id, action.event_type, action.data or {}, context=action.context
            )
            # 驱动 run 键事件同流总线分发（只分发不落库）：watchdog 等内核
            # 订阅者由此观察节点/图生命周期，节点身份经事件载荷透传
            await self._bus.emit_ephemeral(
                Event(
                    id=uuid4().hex,
                    session_id="",
                    iteration=0,
                    timestamp=datetime.now(tz=UTC),
                    event_type=action.event_type,
                    data=dict(action.data or {}),
                    run_id=rt.run_id,
                    context=action.context,
                )
            )
        elif isinstance(action, PersistDriverState):
            await self._host.persist_driver_state(rt.run_id, dict(action.blob), history=False)
        elif isinstance(action, PauseRun):
            # 驱动自暂停：不杀在途的受理分道（手动路杀在途、回执带 manual）
            await self._group_control.driver_pause_run(rt.run_id, action.reason)
        else:
            return False
        return True

    async def _dispatch_workspace_action(self, rt: RunRuntime, action: RunAction) -> bool:
        """工作区类动作派发：经每 run 工作区树执行，结果以通知回投。"""
        if isinstance(action, PrepareScopeWorkspace):
            await self._execute_workspace_prepare(rt, action)
            return True
        if isinstance(action, CommitScopeTerminal):
            await self._execute_workspace_terminal(rt, action)
            return True
        if isinstance(action, IntegrateScope):
            await self._execute_workspace_integrate(rt, action)
            return True
        if isinstance(action, AbandonScope):
            await self._execute_workspace_abandon(rt, action)
            return True
        if isinstance(action, RestoreScope):
            await self._execute_workspace_restore(rt, action)
            return True
        if isinstance(action, RetractIntegrationLine):
            await self._execute_integration_retract(rt, action)
            return True
        if isinstance(action, RestoreCommitted):
            await self._execute_restore_commit(rt, action)
            return True
        return False

    async def _ensure_workspace_tree(
        self, rt: RunRuntime, config: WorkspaceTreeConfig | None = None
    ) -> WorkspaceTreeRuntime | None:
        """取本 run 的工作区树；缺席时按持久化配置（或建区携带的配置）重建。

        建区动作携带完整配置，可全新初始化；屏障、废弃、恢复、回退与查询
        面不携带配置，只能依据仓树目录里持久化的配置经深度恢复重建——
        两者都不可得时返回 None（该 run 未开启工作区隔离）。建树按运行时
        互斥：树配置先于私有仓根引用落盘，建树窗口里并发到场的重建者若
        不等待会读到半成品仓；等待者随后直接取到已登记的树，只建一次。
        """
        async with rt.workspace_tree_lock:
            tree = self._workspace_trees.get(rt.run_id)
            if tree is not None:
                return tree
            if config is None:
                config = await self._persisted_tree_config(rt.run_id)
                if config is None:
                    return None
            tree = WorkspaceTreeRuntime(
                config,
                rt.run_id,
                CheckpointStore(self._run_store.db_path),
                drain_background_writers=self._workspace_drain_hook,
            )
            await tree.initialize()
            if self._runtimes.get(rt.run_id) is not rt:
                # 建树期间运行时已被替换或退出（查询面触发时可能）：树无人清理，不登记
                tree.close()
                return None
            self._workspace_trees[rt.run_id] = tree
            return tree

    async def _persisted_tree_config(self, run_id: str) -> WorkspaceTreeConfig | None:
        """读回 run 盘上持久化的树配置；未开启工作区隔离的 run 返回 None。"""
        run = await self._run_store.get(run_id)
        return load_tree_config(run.work_dir, run_id)

    async def _ensure_workspace_tree_if_running(self, run_id: str) -> WorkspaceTreeRuntime | None:
        """run 任务之外的取树入口（作用域查询与重放前置校验）。

        树不在内存而 run 仍有驱动运行时则重建；run 已无运行时或盘上没有
        树配置时返回 None，由调用方按各自语义收口。
        """
        tree = self._workspace_trees.get(run_id)
        if tree is not None:
            return tree
        rt = self._runtimes.get(run_id)
        if rt is None:
            return None
        return await self._ensure_workspace_tree(rt)

    async def _execute_workspace_prepare(
        self, rt: RunRuntime, action: PrepareScopeWorkspace
    ) -> None:
        """执行建区：首动作惰性建树（基线导入 + 根作用域），再建子工作区。

        持久化顺序：编排状态版本先落 → 建区文件提交（闸门内）→ scope_start
        检查点行 → 建区回执发布；任一步失败按动作失败回执，不发布半成品。
        """
        try:
            tree = await self._ensure_workspace_tree(rt, action.config)
            orchestration_ref = await self._persist_orchestration_ref(rt)
            scope_kind = _WORKSPACE_SCOPE_KINDS.get(action.scope_kind)
            if scope_kind is None:
                raise ValueError(f"未知作用域种类：{action.scope_kind!r}")
            claims = tuple(action.writes)
            prepared = await tree.manager.prepare_scope_workspace(
                CorePrepareScopeWorkspace(
                    parent_scope_id=(
                        tree.root_scope_id
                        if action.parent_scope_id is None
                        else action.parent_scope_id
                    ),
                    scope_key=action.scope_key,
                    spec=CoreScopeWorkspaceSpec(
                        scope_kind=scope_kind,
                        owner_ref=action.owner_ref,
                        claims=claims,
                    ),
                    client_op_id=action.client_op_id,
                )
            )
            scope = await tree.manager.get_scope(prepared.scope_id)
            if scope is None:
                raise RuntimeError(f"建区后作用域账本缺行：{prepared.scope_id}")
            await tree.record_lifecycle_checkpoint(
                scope=scope,
                trigger=CheckpointTrigger.SCOPE_START,
                workspace_commit=prepared.base_commit,
                integration_commit=prepared.integration_base_commit,
                orchestration_state_ref=orchestration_ref,
                claims=claims,
            )
        except Exception as e:
            await self._reply_workspace_failure(rt, action, error=e)
            return
        self._notify(
            rt.run_id,
            ScopeWorkspacePrepared(
                scope_id=prepared.scope_id,
                scope_epoch=prepared.scope_epoch,
                base_commit=prepared.base_commit,
                integration_base_commit=prepared.integration_base_commit,
                workspace_path=prepared.workspace_path,
                context=action.context,
            ),
        )

    async def _reply_workspace_failure(
        self,
        rt: RunRuntime,
        action: (
            PrepareScopeWorkspace
            | CommitScopeTerminal
            | IntegrateScope
            | AbandonScope
            | RestoreScope
            | RetractIntegrationLine
            | RestoreCommitted
        ),
        *,
        error: BaseException,
    ) -> None:
        """动作失败统一回执：run 平面故障先落事件，再回投失败通知。"""
        if isinstance(error, ClaimRejectedError):
            await self._emit_run_event_best_effort(
                rt,
                WORKSPACE_CLAIM_REJECTED_EVENT,
                {
                    "scope_key": error.scope_key,
                    "claims": list(error.claims),
                    "conflicts": [
                        {"holder_scope_id": holder, "claims": list(overlapped)}
                        for holder, overlapped in error.conflicts
                    ],
                },
                context=action.context,
            )
        elif isinstance(error, ScopeQuiesceFailedError):
            await self._emit_run_event_best_effort(
                rt,
                fault_event_type(SCOPE_QUIESCE_FAILED_RULE),
                {"action": type(action).__name__, "error": str(error)},
                context=action.context,
            )
        self._notify(
            rt.run_id,
            WorkspaceActionFailed(
                action=type(action).__name__,
                client_op_id=action.client_op_id,
                error=f"{type(error).__name__}: {error}",
                context=action.context,
            ),
        )

    async def _emit_run_event_best_effort(
        self,
        rt: RunRuntime,
        event_type: str,
        data: dict[str, Any],
        *,
        context: str | None = None,
    ) -> None:
        """run 键事件尽力落库：失败只记日志，不吞掉原动作的失败回执。"""
        try:
            await self._host.emit_run_event(rt.run_id, event_type, data, context=context)
        except Exception:
            logger.warning("run %s 事件 %s 落库失败", rt.run_id, event_type, exc_info=True)

    async def _execute_workspace_terminal(
        self, rt: RunRuntime, action: CommitScopeTerminal
    ) -> None:
        """执行终态提交：闸门内提交执行态并落 terminal 检查点后回执。

        持久化顺序与建区同构：编排状态版本 → 文件终态提交 → 检查点行 →
        发布；回执到达前编排层不得启动验收单元。
        """
        tree = await self._ensure_workspace_tree(rt)
        if tree is None:
            self._notify(
                rt.run_id,
                WorkspaceActionFailed(
                    action="CommitScopeTerminal",
                    client_op_id=action.client_op_id,
                    error=f"run {rt.run_id} 无工作区树",
                    context=action.context,
                ),
            )
            return
        try:
            orchestration_ref = await self._persist_orchestration_ref(rt)
            commit, _branch = await tree.manager.commit_scope_terminal(action.scope_id)
            scope = await tree.manager.get_scope(action.scope_id)
            if scope is None:
                raise RuntimeError(f"终态提交后作用域账本缺行：{action.scope_id}")
            integration_head = await tree.manager.current_integration_head()
            checkpoint_id = await tree.record_lifecycle_checkpoint(
                scope=scope,
                trigger=CheckpointTrigger.TERMINAL,
                workspace_commit=commit,
                integration_commit=integration_head,
                orchestration_state_ref=orchestration_ref,
            )
        except Exception as e:
            await self._reply_workspace_failure(rt, action, error=e)
            return
        self._notify(
            rt.run_id,
            ScopeTerminalCommitted(
                scope_id=action.scope_id,
                commit=commit,
                checkpoint_id=checkpoint_id,
                context=action.context,
            ),
        )

    async def _execute_workspace_integrate(self, rt: RunRuntime, action: IntegrateScope) -> None:
        """执行集成：父闸门内串行集成，成功默认释放工作区，冲突显式拒绝。"""
        tree = await self._ensure_workspace_tree(rt)
        if tree is None:
            self._notify(
                rt.run_id,
                WorkspaceActionFailed(
                    action="IntegrateScope",
                    client_op_id=action.client_op_id,
                    error=f"run {rt.run_id} 无工作区树",
                    context=action.context,
                ),
            )
            return
        try:
            outcome = await tree.manager.integrate_scope(
                CoreIntegrateScope(
                    scope_id=action.scope_id,
                    receipt=action.receipt,
                    client_op_id=action.client_op_id,
                )
            )
        except Exception as e:
            await self._reply_workspace_failure(rt, action, error=e)
            return
        if isinstance(outcome, CoreScopeIntegrated):
            if not tree.retain_after_merge:
                try:
                    await tree.manager.release_scope(action.scope_id)
                except Exception:
                    logger.warning(
                        "作用域 %s 集成后释放失败，工作区暂时保留",
                        action.scope_id,
                        exc_info=True,
                    )
            self._notify(
                rt.run_id,
                ScopeIntegrated(
                    scope_id=outcome.scope_id,
                    parent_scope_id=outcome.parent_scope_id,
                    new_parent_head=outcome.new_parent_head,
                    integrated_paths=outcome.integrated_paths,
                    context=action.context,
                ),
            )
            return
        conflict = outcome.conflict
        await self._emit_run_event_best_effort(
            rt,
            fault_event_type(WORKSPACE_CONFLICT_RULE),
            _workspace_conflict_payload(conflict),
            context=action.context,
        )
        self._notify(
            rt.run_id,
            ScopeIntegrationRejected(
                scope_id=conflict.writer_scope_id,
                parent_scope_id=conflict.parent_scope_id,
                conflict_paths=conflict.conflict_paths,
                base_commit=conflict.base_commit,
                writer_head=conflict.writer_head,
                parent_head=conflict.parent_head,
                context=action.context,
            ),
        )

    async def _persist_orchestration_ref(self, rt: RunRuntime) -> str:
        """先落编排状态快照再取其版本引用（检查点持久化顺序的第一步）。"""
        blob = await self._call_driver(rt, "snapshot")
        await self._run_store.save_driver_state(rt.run_id, dict(blob))
        seq = await self._run_store.append_driver_snapshot(rt.run_id, dict(blob))
        if seq is None:  # 内容未变：最新历史行即当前版本
            seq = await self._run_store.latest_driver_snapshot_id(rt.run_id)
        if seq is None:
            raise RuntimeError("编排状态快照版本缺失：检查点前置持久化失败")
        return f"driver-snapshot:{seq}"

    async def _execute_workspace_abandon(self, rt: RunRuntime, action: AbandonScope) -> None:
        """执行废弃：移除作用域及后代工作区并各自换代。"""
        tree = await self._ensure_workspace_tree(rt)
        if tree is None:
            self._notify(
                rt.run_id,
                WorkspaceActionFailed(
                    action="AbandonScope",
                    client_op_id=action.client_op_id,
                    error=f"run {rt.run_id} 无工作区树",
                    context=action.context,
                ),
            )
            return
        try:
            abandoned = await tree.manager.abandon_scope(
                CoreAbandonScope(
                    scope_id=action.scope_id,
                    expected_scope_epoch=action.expected_scope_epoch,
                    client_op_id=action.client_op_id,
                )
            )
        except Exception as e:
            await self._reply_workspace_failure(rt, action, error=e)
            return
        self._notify(
            rt.run_id,
            ScopeAbandoned(
                scope_id=abandoned.scope_id,
                new_scope_epoch=abandoned.new_scope_epoch,
                abandoned_descendants=abandoned.abandoned_descendants,
                context=action.context,
            ),
        )

    async def _execute_workspace_restore(self, rt: RunRuntime, action: RestoreScope) -> None:
        """执行组检查点恢复：先备好对话恢复数据，再闸门内 reset 撤回组内副作用。

        对话恢复数据只读父会话 transcript、只写树目录下的种子目录，与作用
        域文件态无关，故整体先于 reset：备不好即失败时文件、代次与后代工
        作区原样未动。本方法不启动新 session——编排层持久化失效状态
        （RestoreCommitted）之后才创建绑定。检查点须为该父作用域的组检查
        点行（before_child + 组行）。
        """
        tree = await self._ensure_workspace_tree(rt)
        if tree is None:
            self._notify(
                rt.run_id,
                WorkspaceActionFailed(
                    action="RestoreScope",
                    client_op_id=action.client_op_id,
                    error=f"run {rt.run_id} 无工作区树",
                    context=action.context,
                ),
            )
            return
        try:
            checkpoint = await tree.checkpoints.get(action.checkpoint_id)
            if checkpoint is None:
                raise ValueError(f"检查点不存在：{action.checkpoint_id}")
            if checkpoint.trigger is not CheckpointTrigger.BEFORE_CHILD:
                raise ValueError(f"回滚目标须为派发组检查点：{action.checkpoint_id}")
            if checkpoint.scope_id != action.scope_id:
                raise ValueError(
                    f"检查点不属于目标作用域：{action.checkpoint_id}（{checkpoint.scope_id}）"
                )
            anchor = checkpoint.dispatch_anchor
            if anchor is None or not anchor.dispatch_group_id:
                raise ValueError(f"检查点缺少派发锚点：{action.checkpoint_id}")
            if action.checkpoint_id != (
                f"{action.scope_id}:before_child:{anchor.dispatch_group_id}"
            ):
                raise ValueError(f"回滚目标须为组恢复点行（而非派发行）：{action.checkpoint_id}")
            scope = await tree.manager.get_scope(action.scope_id)
            if scope is None:
                raise ValueError(f"作用域不存在：{action.scope_id}")
            # 代次先于一切准备核对：过期请求不写种子，回执直接点名真实原因
            await tree.scope_tree.require_scope_epoch(action.scope_id, action.expected_scope_epoch)
            replay_task: dict[str, str] | None = None
            if action.replay_dispatch:
                replay_task = tree.replay_ledger.require_single_task(
                    anchor.dispatch_group_id, action.checkpoint_id
                )
            plan = ConversationRestorePlan(
                run_id=rt.run_id,
                scope_id=action.scope_id,
                scope_epoch=action.expected_scope_epoch + 1,
                checkpoint_id=action.checkpoint_id,
                anchor=anchor,
                workspace_path=self._scope_workspace_path(scope),
                seed_dir=tree.restore_seed_dir(action.client_op_id),
                session_runtime=action.session_runtime,
                replay_tool_name=replay_task.get("tool_name", "") if replay_task else "",
                replay_tool_input=(
                    _json_loads_or_none(replay_task.get("tool_input", "")) if replay_task else None
                ),
            )
        except Exception as e:
            await self._reply_workspace_failure(rt, action, error=e)
            return
        try:
            data = await self._restorer.prepare(plan)
        except Exception as e:
            await tree.discard_restore_seed(action.client_op_id)
            await self._reply_workspace_failure(
                rt,
                action,
                error=RuntimeError(f"对话恢复数据准备失败，作用域文件态与代次未动：{e}"),
            )
            return
        try:
            restored = await tree.manager.restore_scope(
                action.scope_id,
                restore_commit=checkpoint.workspace_commit,
                expected_scope_epoch=action.expected_scope_epoch,
                client_op_id=action.client_op_id,
            )
        except Exception as e:
            # 对话数据已备好而文件态恢复未成：种子无人消费，回收
            await tree.discard_restore_seed(action.client_op_id)
            await self._reply_workspace_failure(rt, action, error=e)
            return
        tree.pending_restores[action.scope_id] = PendingSubagentRestore(data=data)
        # 组恢复点已回退：该作用域的活动组失效（新 message 开新组）
        tree.arbiter.forget(action.scope_id)
        self._notify(
            rt.run_id,
            ScopeRestored(
                scope_id=restored.scope_id,
                new_scope_epoch=restored.new_scope_epoch,
                checkpoint_id=action.checkpoint_id,
                abandoned_descendants=restored.abandoned_descendants,
                context=action.context,
            ),
        )

    async def _execute_integration_retract(
        self, rt: RunRuntime, action: RetractIntegrationLine
    ) -> None:
        """执行集成线回退：逐目标核验检查点后先校验后 reset 回退。

        每个目标须为其作用域的 scope_start 行（建区时的集成头即该目标
        建区基线提交）；回退点由线上有集成提交的目标建区基线在首父链
        上的最早者决定（空集成目标不参与，目标集全为空集成时线不动），
        回退结果以失效/保留划分回投，编排层据此重排失效集并保留
        保留者。
        """
        tree = await self._ensure_workspace_tree(rt)
        if tree is None:
            self._notify(
                rt.run_id,
                WorkspaceActionFailed(
                    action="RetractIntegrationLine",
                    client_op_id=action.client_op_id,
                    error=f"run {rt.run_id} 无工作区树",
                    context=action.context,
                ),
            )
            return
        try:
            targets: list[RetractTarget] = []
            for entry in action.targets:
                checkpoint = await tree.checkpoints.get(entry.checkpoint_id)
                if checkpoint is None:
                    raise ValueError(f"回退目标检查点未在账本登记：{entry.checkpoint_id}")
                if checkpoint.trigger is not CheckpointTrigger.SCOPE_START:
                    raise ValueError(f"回退目标须为 scope_start 检查点：{entry.checkpoint_id}")
                if checkpoint.scope_id != entry.scope_id:
                    raise ValueError(
                        f"检查点不属于回退目标作用域：{entry.checkpoint_id}（{checkpoint.scope_id}）"
                    )
                if not checkpoint.integration_commit:
                    raise ValueError(f"检查点缺少集成提交引用：{entry.checkpoint_id}")
                targets.append(
                    RetractTarget(
                        scope_id=entry.scope_id,
                        base_commit=checkpoint.integration_commit,
                    )
                )
            result = await tree.manager.retract_integration_line(
                targets=targets,
                expected_root_epoch=action.expected_root_epoch,
                client_op_id=action.client_op_id,
            )
        except Exception as e:
            await self._reply_workspace_failure(rt, action, error=e)
            return
        self._notify(
            rt.run_id,
            IntegrationLineRetracted(
                new_root_head=result.new_root_head,
                new_root_epoch=result.new_root_epoch,
                target_commit=result.target_commit,
                invalidated=result.invalidated,
                preserved=result.preserved,
                context=action.context,
            ),
        )

    async def _execute_restore_commit(self, rt: RunRuntime, action: RestoreCommitted) -> None:
        """恢复确认执行：子代理链在此创建并绑定恢复 session（幂等）。"""
        if not action.restore_binding:
            return  # 未合并节点回滚链：纯确认，紧随的建区回执即生效信号
        tree = self._workspace_trees.get(rt.run_id)
        pending = None if tree is None else tree.pending_restores.get(action.scope_id)
        if pending is None:
            self._notify(
                rt.run_id,
                WorkspaceActionFailed(
                    action="RestoreCommitted",
                    client_op_id=action.client_op_id,
                    error=(f"恢复数据缺席（树运行时重建或恢复链丢失）：{action.scope_id}"),
                    context=action.context,
                ),
            )
            return
        if pending.bound_session_id is not None:
            self._notify(
                rt.run_id,
                RestoreSessionBound(
                    scope_id=action.scope_id,
                    session_id=pending.bound_session_id,
                    checkpoint_id=pending.data.plan.checkpoint_id,
                    context=action.context,
                ),
            )
            return
        try:
            binding = await self._restorer.bind(pending.data)
            with contextlib.suppress(Exception):
                await self._affiliation.attach(
                    binding.session_id, rt.run_id, role="executor", context=action.context
                )
            with contextlib.suppress(Exception):
                await self._affiliation.set_context(binding.session_id, action.context)
        except Exception as e:
            await self._reply_workspace_failure(rt, action, error=e)
            return
        pending.bound_session_id = binding.session_id
        self._notify(
            rt.run_id,
            RestoreSessionBound(
                scope_id=action.scope_id,
                session_id=binding.session_id,
                checkpoint_id=pending.data.plan.checkpoint_id,
                context=action.context,
            ),
        )

    @staticmethod
    def _scope_workspace_path(scope: ScopeRecord) -> str:
        """作用域工作区路径（workspace_ref 前段）。"""
        if not scope.workspace_ref:
            raise RuntimeError(f"作用域没有工作区引用：{scope.scope_id}")
        return scope.workspace_ref.rpartition("|")[0]

    async def _await_sessions_task(self, rt: RunRuntime, action: AwaitSessions) -> None:
        """session 等待后台任务：等待完成（超时已保证停 session）后仅唤醒循环。"""
        try:
            await self._host.await_sessions(rt.run_id, list(action.ids), action.timeout_s)
        except Exception:
            logger.exception("run %s 成员等待异常", rt.run_id)
        rt.wake.set()

    async def _apply_driver_finish(self, rt: RunRuntime, action: FinishRun) -> None:
        """驱动终态收口：先快照再扫掠、落账（与 stop/失败收尾同序）。

        成员仍在跑时收口是文档化模式：账面终态是「成员已收口」的观测点，
        不留守孤儿；终态控制已受理时不落账（控制拥有终态优先权）。收尾前
        快照保证终态 run 的最终驱动状态已落盘——终态受理后的 FinishRun 直接退出
        循环、绕过收尾轮的边界快照，最终裁决只能在此落盘。
        """
        rt.stopping = True
        await self._persist_snapshot(rt)
        await self._persist_final_projection(rt)
        if rt.terminal_accepted:
            return
        await self._terminal.sequence(
            rt.run_id, finish_status=None, drain_timeout=STOP_DRAIN_TIMEOUT_S
        )
        # 已终态（竞态落败，如扫掠期间 stop 先落账）不覆账
        with contextlib.suppress(Exception):
            await self._run_store.finish(rt.run_id, action.outcome.value, error=action.summary)

    async def _persist_snapshot(self, rt: RunRuntime) -> None:
        """边界快照：动作批后/控制后/收尾前；最新 + 追加历史两落点。"""
        if rt.poisoned:
            return
        try:
            blob = await self._call_driver(rt, "snapshot")
        except _DriverError:
            return
        await self._host.persist_driver_state(rt.run_id, blob)

    async def _persist_final_projection(self, rt: RunRuntime) -> None:
        """终态最终投影：run 任务退出前把 projection() 落盘——终态后 /state 的
        降级首选读源（快照是恢复形状，消费面不可渲染）。软守护：失败/中毒
        不拦收尾，降级读自然回落快照。
        """
        if rt.poisoned:
            return
        try:
            projection = await asyncio.wait_for(
                self._call_driver_soft(rt, "projection"),
                timeout=_FINAL_PROJECTION_TIMEOUT_S,
            )
        except Exception:
            logger.debug("终态投影读取失败，回落快照读源", exc_info=True)
        else:
            if isinstance(projection, dict):
                await self._run_store.save_driver_projection(rt.run_id, projection)

    async def _call_driver(self, rt: RunRuntime, method: str, *args: Any) -> Any:
        """有界线程池 + 超时守护；超时/异常即中毒弃用并按驱动失败收尾。"""
        if rt.poisoned:
            raise _DriverError(rt.format)
        fn = getattr(rt.driver, method)
        try:
            return await asyncio.wait_for(
                asyncio.wrap_future(self._submit_driver_call(rt, fn, *args)),
                timeout=self._decide_timeout_s,
            )
        except TimeoutError:
            await self._fail_run(
                rt,
                f"driver_{method}_timeout",
                f"驱动 {rt.format} 第 {rt.rounds} 轮 {method} 超时（>{self._decide_timeout_s}s）",
            )
            raise _DriverError(rt.format) from None
        except Exception as e:
            await self._fail_run(
                rt,
                f"driver_{method}_error",
                f"驱动 {rt.format} 第 {rt.rounds} 轮 {method} 异常：{e}",
            )
            raise _DriverError(rt.format) from e

    def _decide_pool(self, fmt: str) -> ThreadPoolExecutor:
        """按 format 分片的有界线程池：单驱动故障只废自己的分片。

        后台导出线程（每任务独立线程）经 guarded_call 会到达这里；check-then-set
        以线程锁守卫，防两个并发首导各建一个池、输家从 _decide_pools 脱落泄漏。
        """
        pool = self._decide_pools.get(fmt)
        if pool is None:
            with self._decide_pools_guard:
                pool = self._decide_pools.get(fmt)
                if pool is None:
                    pool = ThreadPoolExecutor(
                        max_workers=_DECIDE_POOL_PER_FORMAT, thread_name_prefix=f"decide:{fmt}"
                    )
                    self._decide_pools[fmt] = pool
        return pool

    def _submit_driver_call(self, rt: RunRuntime, fn: Any, *args: Any) -> Future[Any]:
        """驱动调用入 format 分片池并登记在途：cf.Future 完成即线程已返回。"""
        fut = self._decide_pool(rt.format).submit(fn, *args)
        rt.driver_calls.add(fut)
        fut.add_done_callback(rt.driver_calls.discard)
        return fut

    async def _drain_driver_calls(self, rt: RunRuntime, timeout_s: float) -> bool:
        """有界等待该 run 在途驱动线程退出；超时返回 False（卡死线程等不到）。"""
        deadline = time.monotonic() + timeout_s
        while rt.driver_calls:
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.05)
        return True

    async def _wait_task_exit(self, task: asyncio.Task[None], timeout_s: float) -> bool:
        """有界等待已取消的 run 任务退出；取消被收尾路径吸收（任务不退出）时返回 False。"""
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout_s)
        except TimeoutError:
            return False
        except asyncio.CancelledError:
            if not task.cancelled():
                raise  # 被取消的是等待方自身：保住外层取消语义
        except Exception:
            return True  # 任务带着自身异常退出：已死即可，异常面由运行环留痕
        return True

    async def _fail_run(self, rt: RunRuntime, code: str, detail: str) -> None:
        """驱动失败收尾：实例中毒弃用 + 成员尽力扫掠 + run 落 failed + run 事件留痕。"""
        async with rt.checkpoint_lock:
            rt.poisoned = True
        rt.wake.set()  # 异常被吞的路径（如边界快照）下循环也能立即醒来退出
        logger.error("run %s 失败收尾：%s：%s", rt.run_id, code, detail)
        with contextlib.suppress(Exception):
            await self._host.emit_run_event(
                rt.run_id,
                code,
                {"detail": detail, "round": rt.rounds},
            )
        await self._terminal.sequence(
            rt.run_id, finish_status=None, drain_timeout=STOP_DRAIN_TIMEOUT_S
        )
        # 已终态（竞态）不覆账
        with contextlib.suppress(Exception):
            await self._run_store.finish(rt.run_id, RunStatus.FAILED, error=f"{code}: {detail}")
