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

"""RunDriver 协议——run 编排决策组件的 SDK 契约。

驱动不直接执行 I/O、asyncio 或墙钟操作；对宿主的一切需求以动作
表达，执行结果经通知回投。需要逐次同步持久化的驱动可通过可选动作
发射端口交付 PersistDriverState，由宿主执行。关联键（session_id/proc_id/timer_id）由核心
分配、经回执通知交付；context 由驱动自定义、动作带出通知带回、核心
不解析仅透传。增补字段、枚举值与动作词属兼容演进；不兼容演进升 PROTOCOL_VERSION。
"""

from __future__ import annotations

from dataclasses import MISSING, dataclass, field, fields
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from pathlib import Path

    from workflow_controller.config.settings import LoopConfig
    from workflow_controller.core.runs import RunRecord
    from workflow_controller.session.events import SessionOutcome

PROTOCOL_VERSION = 1


class RunOccupiedError(ValueError):
    """工厂创建被拒：驱动绑定的资源仍被账面活跃的 run 占用。"""


# handle_op 调用方等待上界：高于协调器软调上界（75s），慢操作的真实落点
# 先于本层超时送达；超时语义=未取回结果，非操作失败
OP_HANDLE_TIMEOUT_S = 80.0


class RestartPolicy(StrEnum):
    """成员重启策略：策略选择是驱动决策，机制归核心。"""

    RESUME_IF_PAUSED = "resume_if_paused"
    FRESH = "fresh"
    # 超时续跑：停掉挂死运行时后按 sdk 会话续跑（上下文保留），可附续跑提示
    CONTINUE = "continue"


class FinishOutcome(StrEnum):
    """驱动侧的 run 终态裁决。

    STOPPED 供恢复实例复述停止终态（宿主终态重开把账面翻回 running 后
    在等一个裁决；停止语境的快照恢复后复述 stopped，不是新判）——正常
    停止路的终态仍归宿主（stop 受理即取得终态裁决权），驱动不发。
    """

    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass(frozen=True)
class SessionSpec:
    """成员 session 的组装规格（宿主据此组装配置并注入归属）。"""

    name: str
    prompt: str
    cwd: str
    agent_name: str = ""
    model: str | None = None
    provider_index: int | None = None
    allowed_tools: tuple[str, ...] | None = None
    permission_mode: str = "bypassPermissions"
    reviewer_profile: str | None = None  # None = 不挂 reviewer；否则内联 JSON 配置
    loop: LoopConfig | None = None
    max_turns: int | None = None
    max_budget_usd: float | None = None
    env_overrides: Mapping[str, str] | None = None
    config_overrides: Mapping[str, Any] | None = None
    plugin_metadata: Mapping[str, dict[str, Any]] | None = None  # 溯源元数据，随出生参数入 pool

    def assembly_fields(self) -> dict[str, Any]:
        """Start 组装基座：全量字段映射（prompt→initial_prompt）+并入 config_overrides。

        宿主在此基座上注入 reviewer/run_id/role 并后置回填（越权覆盖无效）。
        """
        out: dict[str, Any] = dict(
            name=self.name,
            initial_prompt=self.prompt,
            cwd=self.cwd,
            agent_name=self.agent_name,
            model=self.model,
            provider_index=self.provider_index,
            permission_mode=self.permission_mode,
            max_turns=self.max_turns,
            max_budget_usd=self.max_budget_usd,
            loop=self.loop,
            env_overrides=dict(self.env_overrides or {}),
        )
        if self.allowed_tools is not None:
            out["allowed_tools"] = list(self.allowed_tools)
        out.update(dict(self.config_overrides or {}))
        return out

    def refresh_overrides(self) -> dict[str, Any]:
        """Restart 局部更新载荷：显式字段才覆盖（None/默认值视为未提供），prompt 恒更。

        默认值经字段声明反射判定（单一真相）；name/reviewer_profile 不入局部
        更新（重命名与 reviewer 面由宿主策略管辖）。宿主剔除自身注入键
        （run_id/role）后经 apply_session_overrides 应用。
        """
        out: dict[str, Any] = {"initial_prompt": self.prompt, "cwd": self.cwd}
        for name in (
            "agent_name",
            "model",
            "provider_index",
            "permission_mode",
            "max_turns",
            "max_budget_usd",
            "loop",
        ):
            value = getattr(self, name)
            if value is None or value == _SPEC_FIELD_DEFAULTS.get(name):
                continue
            out[name] = value
        if self.allowed_tools is not None:
            out["allowed_tools"] = list(self.allowed_tools)
        if self.env_overrides:
            out["env_overrides"] = dict(self.env_overrides)
        out.update(dict(self.config_overrides or {}))
        return out


_SPEC_FIELD_DEFAULTS = {
    f.name: f.default
    for f in fields(SessionSpec)
    if f.default is not MISSING and f.default is not None
}


@dataclass(frozen=True)
class RunSessionInfo:
    """run 名下 session 的当前事实（驱动 world 与 RunsAPI sessions 共用）。"""

    session_id: str
    role: str = "executor"
    context: str | None = None
    state: str = ""
    stop_reason: str | None = None
    in_flight: bool = False


@dataclass(frozen=True)
class ProcessInfo:
    """在途 run 键进程的句柄视图。"""

    proc_id: str
    context: str | None = None
    returncode: int | None = None  # None = 在途


@dataclass(frozen=True)
class RunWorld:
    """每轮 decide 前即时组装的决策输入快照，不缓存陈值。

    驱动需要的一切要么在 world/通知里，要么在自己状态里——不允许反向
    查询。
    """

    run: RunRecord
    sessions: tuple[RunSessionInfo, ...] = ()
    processes: tuple[ProcessInfo, ...] = ()
    in_flight: int = 0


# ── 动作词表 ─────────────────────────────────────────


@dataclass(frozen=True)
class RunAction:
    """驱动产出、核心异步执行；结果以后续通知回投，绝无同步返回值。"""


@dataclass(frozen=True)
class StartSession(RunAction):
    """按 spec 启动成员 session；成败都以 SessionActivated 回执。"""

    spec: SessionSpec
    role: str = "executor"
    context: str | None = None


@dataclass(frozen=True)
class RestartSession(RunAction):
    """重启既有成员；覆盖项用于新 attempt 的上下文刷新。"""

    session_id: str
    policy: RestartPolicy = RestartPolicy.RESUME_IF_PAUSED
    spec_override: SessionSpec | None = None
    context_override: str | None = None


@dataclass(frozen=True)
class AwaitSessions(RunAction):
    """等待指定 session 成员终态后再继续决策。"""

    ids: tuple[str, ...] = ()  # session_id 集
    timeout_s: float | None = None  # None = 无限等；超时前核心保证已停该 session


@dataclass(frozen=True)
class ReplaySessionOutcome(RunAction):
    """按成员身份回放最后一条持久结局，供驱动恢复阶段决策。"""

    session_id: str


@dataclass(frozen=True)
class DiscardSession(RunAction):
    """成员终态后只释放内存副本，保留持久会话与 Run 归属。"""

    session_id: str


@dataclass(frozen=True)
class StartProcess(RunAction):
    """起 run 键 bash 进程组（setsid 独立进程组）。"""

    cmd: str
    cwd: str
    env: Mapping[str, str] | None = None
    context: str | None = None
    stream: bool = True  # False = 只收退出码与尾部聚合


@dataclass(frozen=True)
class KillProcesses(RunAction):
    """杀 run 键进程组。"""

    context: str | None = None  # None = run 名下全部进程组


@dataclass(frozen=True)
class ReadArtifact(RunAction):
    """读 run 工作目录下的产物文件，结果以 ArtifactRead 回投。"""

    path: str  # 相对 run.work_dir
    field: str | None = None  # None = 整文件去空白；给定 = JSON 标量字段
    context: str | None = None


@dataclass(frozen=True)
class ArmTimer(RunAction):
    """挂一次性定时器，到期以 TimerFired 回投。"""

    timer_id: str
    delay_s: float  # runtime-only，崩溃恢复后由驱动重挂


@dataclass(frozen=True)
class CancelTimer(RunAction):
    """取消定时器。"""

    timer_id: str  # 不存在或已触发 = 幂等 no-op


@dataclass(frozen=True)
class EmitEvent(RunAction):
    """向事件总线发射 run 级事件。"""

    event_type: str
    data: Mapping[str, Any] | None = None
    context: str | None = None


@dataclass(frozen=True)
class PersistDriverState(RunAction):
    """驱动状态落盘（最新快照 + 追加历史）。"""

    blob: Mapping[str, Any]


@dataclass(frozen=True)
class FinishRun(RunAction):
    """驱动对 run 终态的唯一裁决动作。"""

    outcome: FinishOutcome
    summary: str | None = None


# PauseRun.reason 值域单点：增补值改此处；消费方按本值域解析 PAUSED 回执
_PAUSE_RUN_REASONS = frozenset({"awaiting_answer", "awaiting_external_fix"})


@dataclass(frozen=True)
class PauseRun(RunAction):
    """驱动声明条件暂停（等外部答复/等外部修复）；受理以 PAUSED 回投。"""

    reason: str  # 值域见 _PAUSE_RUN_REASONS，构造期白名单校验；manual 禁发（见校验）

    def __post_init__(self) -> None:
        if self.reason not in _PAUSE_RUN_REASONS:
            # manual 是「宿主已杀在途」回执的保留标记：驱动自暂停不杀在途，
            # 回执带本动作的 reason——混入 manual 会被消费方误读为在途已杀
            raise ValueError(
                f"PauseRun.reason 需为 {sorted(_PAUSE_RUN_REASONS)} 之一"
                f"（manual 保留给手动暂停的受理回执）：{self.reason!r}"
            )


@dataclass(frozen=True)
class SetRunLabel(RunAction):
    """改 run 的可读标签。"""

    label: str


@dataclass(frozen=True)
class AdoptSession(RunAction):
    """把既有 session 收编为本 run 成员。"""

    session_id: str
    role: str
    context: str | None = None


START_LIKE_ACTIONS = (StartSession, RestartSession)


# ── 通知词表 ─────────────────────────────────────────


@dataclass(frozen=True)
class RunNotification:
    """核心→驱动的单向事实投递；入 FIFO 后由 run 任务随 decide 消费。"""


@dataclass(frozen=True)
class SessionActivated(RunNotification):
    """Start 类动作的回执：成败都回执，槽位与在途计数以此为界回收。"""

    session_id: str  # 失败回执时仅作关联键
    context: str | None = None
    ok: bool = True
    error: str | None = None


@dataclass(frozen=True)
class SessionOutcomeNotification(RunNotification):
    """成员 session 的结构化结局。"""

    outcome: SessionOutcome


@dataclass(frozen=True)
class SessionOutcomeReplayed(RunNotification):
    session_id: str
    outcome: SessionOutcome | None


@dataclass(frozen=True)
class ProcessOutput(RunNotification):
    """run 键进程的一行流式输出。"""

    proc_id: str
    context: str | None = None
    line: str = ""


@dataclass(frozen=True)
class ProcessExit(RunNotification):
    """run 键进程退出（尾部聚合输出可查事件流）。"""

    proc_id: str
    context: str | None = None
    returncode: int = 0
    exit_mark: str | None = None  # "plane_killed"=本面组杀 / "spawn_failed"=起动失败


@dataclass(frozen=True)
class ArtifactRead(RunNotification):
    """ReadArtifact 的结果回执。"""

    context: str | None = None
    path: str = ""
    field: str | None = None
    exists: bool = False
    value: str | None = None
    error: str | None = None  # 缺文件/缺字段/非标量的判假细因


class ControlKind(StrEnum):
    """组控结果词表；stopped 受理即取得终态裁决权。"""

    PAUSED = "paused"
    RESUMED = "resumed"
    STOPPED = "stopped"  # 终态控制：受理即取得终态裁决权
    STEERED = "steered"


@dataclass(frozen=True)
class ControlNotification(RunNotification):
    """组控结果告知（暂停/恢复/停止/注入指示）。"""

    kind: ControlKind
    payload: Mapping[str, Any] | None = None
    affected: tuple[str, ...] = ()

    def is_terminal(self) -> bool:
        """是否终态控制。"""
        return self.kind is ControlKind.STOPPED


@dataclass(frozen=True)
class SessionControlApplied(RunNotification):
    """session 成员级控制操作（pause/resume/stop/inject）的回执。"""

    session_id: str
    operation: str  # pause/resume/stop/inject


@dataclass(frozen=True)
class TimerFired(RunNotification):
    """定时器到期信号。"""

    timer_id: str


@dataclass(frozen=True)
class AllSessionsTerminal(RunNotification):
    """全员终态而驱动未 Finish 的防挂起通知（只进有驱动 run）。"""


@dataclass(frozen=True)
class OpRequest(RunNotification):
    """带应答句柄的通知：run 任务内执行驱动方法、经 Future 回传结果。"""

    op: str
    args: Mapping[str, Any] = field(default_factory=dict)
    reply: Any = None


@dataclass(frozen=True)
class DispatchBarrierRequest(RunNotification):
    """派发屏障请求：会话宿主发起，run 任务内执行核心动作后经 Future 回传。

    kind 词面：side_effect（⑤ 写文件类/Bash 副作用前）| dispatch（① task
    创建前）。data 为驱动层规整后的 CC 风格载荷（tool_name/tool_input/
    tool_use_id/native 会话与消息标识）；reply 为 asyncio.Future，结果为
    驱动层屏障结果（放行 + 子工作区 cwd / 拒绝）。
    """

    kind: str
    session_id: str
    runtime: str
    context: str | None = None
    data: Mapping[str, Any] = field(default_factory=dict)
    reply: Any = None
    # 子代理工作区声明：writer（默认）| reader（detached 只读快照）
    workspace: str = "writer"


# ── 协议 ──────────────────────────────────────────────────────────


class RunDriver(Protocol):
    """决策协议：同步方法，world/notifications 只读。"""

    def decide(self, world: RunWorld, notifications: Sequence[RunNotification]) -> list[RunAction]:
        """消费通知、产出动作；同 run 串行调用，无需内部锁。"""
        ...

    def on_control(self, control: ControlNotification) -> list[RunAction] | None:
        """控制结果告知（run 任务内调用）；可返回即时动作（事件类，宿主直接执行）。

        暂停受理时决策循环被冻结、decide 不再被调——暂停侧的事件只能经此
        返回。出生类动作不在此派发（冻结窗语义由派发点把关）。
        """
        ...

    def projection(self) -> dict[str, Any]:
        """带 schema_version 的状态投影（API /state 透传）。"""
        ...

    def snapshot(self) -> dict[str, Any]:
        """不透明状态 blob；推荐含 schema_version/state/pending_timers。"""
        ...

    def handle_op(self, op: str, args: Mapping[str, Any]) -> dict[str, Any]:
        """驱动自定义操作（经应答通道在 run 任务内执行）。"""
        ...


class RunWorldOpDriver(Protocol):
    """可选扩展：操作需要最新 RunWorld 时，由宿主在同一 run 任务内提供。"""

    def handle_world_op(
        self,
        world: RunWorld,
        op: str,
        args: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bool, list[RunAction]]:
        """返回操作结果、是否追加边界快照、以及可派发的事件动作。"""
        ...


class RunCheckpointDriver(Protocol):
    """可选扩展：决策中途同步发出持久化动作，执行机制仍归宿主。"""

    def bind_checkpoint(
        self,
        emit: Callable[[PersistDriverState], None],
    ) -> None:
        """发射端口返回后，该最新快照已由宿主提交。"""
        ...


class RunDriverFactory(Protocol):
    """按 format 注册的驱动工厂；可声明终态成员的启动期内存保留策略。"""

    @property
    def format(self) -> str:
        """带版本格式串（如 graph-yaml@1），重复注册报错。"""
        ...

    def create(self, run: RunRecord, program_path: Path, params: Mapping[str, str]) -> RunDriver:
        """新建驱动；翻译错误由调用方落 finish(failed)。"""
        ...

    def restore(
        self, run: RunRecord, snapshot: Mapping[str, Any], sessions: Sequence[RunSessionInfo]
    ) -> RunDriver:
        """从快照恢复；快照不可解析时抛错、run 标 failed(unrecoverable)。"""
        ...


class RunDriverReloader(Protocol):
    """可选扩展：工厂侧重读当前程序定义（I/O 侧）并翻译为换装载荷。

    载荷不透明（协调器只透传），经 reload_definition 驱动 op 在 run 任务
    内完成换装；翻译/校验失败抛错，run 保持原定义原状态。
    """

    def reload_definition(self, run: RunRecord, driver: RunDriver, source: Path) -> Any:
        """重读工作流源文件并翻译；driver 为协调器在册实例（只读快照可用）。"""
        ...
