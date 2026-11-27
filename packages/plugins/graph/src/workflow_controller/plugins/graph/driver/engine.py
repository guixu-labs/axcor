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

"""graph 正规驱动：decide 步进驱动 DAG 编排，执行机制经动作表达。

节点执行建模为阶段机：成员出生/等待、进程执行、产物读取与终态裁决全部
经 run 平面动作派发，执行结果以通知回投推进阶段迁移。决策数学（就绪/
传播/回滚/验收判定/提示词）在包内纯函数模块；本模块只持步进状态。
"""

from __future__ import annotations

import json
import logging
import shlex
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from workflow_controller.plugin.run_driver import (
    ArmTimer,
    ArtifactRead,
    AwaitSessions,
    CancelTimer,
    ControlKind,
    ControlNotification,
    DiscardSession,
    EmitEvent,
    FinishOutcome,
    FinishRun,
    KillProcesses,
    PersistDriverState,
    ProcessExit,
    ProcessOutput,
    ReadArtifact,
    ReplaySessionOutcome,
    RestartPolicy,
    RestartSession,
    RunAction,
    RunNotification,
    RunWorld,
    SessionActivated,
    SessionControlApplied,
    SessionOutcomeNotification,
    SessionOutcomeReplayed,
    SessionSpec,
    StartProcess,
    StartSession,
    TimerFired,
)
from workflow_controller.plugins.graph import translator
from workflow_controller.plugins.graph.driver import gates, prompts, scheduler
from workflow_controller.plugins.graph.driver.dag import prefix_subgraph_nodes, subgraph_topology
from workflow_controller.plugins.graph.driver.state import MAX_FAILURE_REASON_CHARS, GraphState
from workflow_controller.plugins.graph.template import render

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from pathlib import Path

    from workflow_controller.plugins.graph.driver.dag import DAG
    from workflow_controller.plugins.graph.translator import GraphProgram

logger = logging.getLogger(__name__)

# 验收脚本与兜底判定的执行超时上界
_VERIFY_SCRIPT_TIMEOUT_S = 300.0
_VERDICT_FALLBACK_TIMEOUT_S = 180.0


def _resolve_timeout(raw: Any, default: float = 7200.0) -> float | None:
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else None


# 运行配置项键集：构造入参与驱动快照 options 共用，防两处键漂移
RUNNER_OPTION_KEYS = (
    "runtime",
    "cli_wrapper",
    "model",
    "reviewer_enabled",
    "reviewer_model",
    "reviewer_provider_index",
    "evaluator",
    "max_iterations",
    "reviewer_thinking_enabled",
    "reviewer_thinking_budget_tokens",
    "reviewer_max_tokens",
    "system_prompt",
    "reviewer_task_goal",
    "provider_index",
    "model_pinned",
)


def pick_runner_options(src: Mapping[str, Any]) -> dict[str, Any]:
    """按键集提取非 None 运行配置项。"""
    return {k: src[k] for k in RUNNER_OPTION_KEYS if src.get(k) is not None}


# 节点活动阶段：执行（bash 进程 / agent 成员）→ 子图读取 → 报告落盘 → 验收
_PHASE_EXEC_PROC = "exec_proc"
_PHASE_SPAWN = "spawn"
_PHASE_RUNNING = "running"
_PHASE_CONTINUE = "continue"
_PHASE_STOPPING = "stopping"
_PHASE_PAUSED_WAIT = "paused_wait"
_PHASE_SUB_READ = "sub_read"
_PHASE_REPORT = "report"
_PHASE_VERIFY_PROC = "verify_proc"
_PHASE_VERIFY_SPAWN = "verify_spawn"
_PHASE_VERIFY_RUNNING = "verify_running"
_PHASE_VERIFY_CONTINUE = "verify_continue"
_PHASE_VERIFY_STOPPING = "verify_stopping"
_PHASE_VERIFY_PAUSED_WAIT = "verify_paused_wait"
_PHASE_VERDICT_SPAWN = "verdict_spawn"
_PHASE_VERDICT_RUNNING = "verdict_running"

_VERIFY_PHASES = frozenset(
    {
        _PHASE_VERIFY_PROC,
        _PHASE_VERIFY_SPAWN,
        _PHASE_VERIFY_RUNNING,
        _PHASE_VERIFY_CONTINUE,
        _PHASE_VERIFY_STOPPING,
        _PHASE_VERIFY_PAUSED_WAIT,
        _PHASE_VERDICT_SPAWN,
        _PHASE_VERDICT_RUNNING,
    }
)

# 定时器用途（同节点同时至多一个定时器，按阶段区分语义）
_TIMER_MEMBER = "member"
_TIMER_VERIFY_SCRIPT = "verify_script"
_TIMER_VERDICT = "verdict"


@dataclass
class NodeActivity:
    """单节点一次编排的活动记录：等待中的成员/进程/产物与阶段事实。"""

    node_id: str
    phase: str
    attempt: int
    session_id: str | None = None
    verify_sid: str | None = None
    proc_id: str | None = None
    timer_id: str | None = None
    timer_kind: str = ""
    timer_delay_s: float = 0.0
    max_continues: int = 2
    continues: int = 0
    executor_error: str | None = None
    result_digest: str | None = None
    result_iteration: int | None = None
    pending_subgraph: list[dict] | None = None
    verify: dict[str, Any] | None = None
    verify_text: str = ""
    verify_tail: list[str] = field(default_factory=list)
    verify_kill_pending: bool = False
    restoring_missing: bool = False


def _resume_reconcile_target(
    act: NodeActivity | None,
    info: Mapping[str, Any],
) -> tuple[bool, Any]:
    """组恢复核对的目标成员：验收侧用在途成员，执行侧用档案绑定。"""
    # 验收/回退相位：在途成员是验收/回退 agent，workflow_controller_session_id
    # 指向的执行器早已终局，核对须按活动的在途成员查 world
    verify_sid = info.get("verify_session_id")
    verify_side = (act is not None and act.phase in _VERIFY_PHASES) or (
        act is None and bool(verify_sid)
    )
    if act is not None and verify_side and act.session_id:
        return verify_side, act.session_id
    if verify_side:
        return verify_side, verify_sid
    return verify_side, info.get("workflow_controller_session_id")


class GraphDriver:
    """graph-yaml@1 正规驱动：decide 产出动作；状态写入经宿主同步 checkpoint。"""

    def __init__(
        self,
        *,
        graph_id: str,
        program: GraphProgram,
        state: GraphState,
        work_dir: Path,
        yaml_path: Path,
        input_variables: Mapping[str, Any] | None = None,
        agents_dir_base: Path | None = None,
        starting_prompt: str | None = None,
        user_prompt_path: Path | None = None,
        applied_reopen_at: str | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> None:
        self._graph_id = graph_id
        self._program = program
        self._dag = program.dag
        self._state = state
        self._work_dir = work_dir
        self._yaml_path = yaml_path
        # 原始入参变量（翻译产物不可逆推，恢复按原入参重翻译）
        self._input_variables = dict(input_variables or {})
        # 相对 agents 资源的解析基准（快照往返；直连 run 为原始程序目录）
        self._agents_dir_base = agents_dir_base
        self._starting_prompt = starting_prompt
        self._user_prompt_path = user_prompt_path
        self._applied_reopen_at = applied_reopen_at
        self._init_runner_options(options or {})
        # 步进状态（runtime-only；恢复后由调度重建）
        self._nodes: dict[str, NodeActivity] = {}
        self._agents_ready = program.agents_dir is None
        self._agents_prep_inflight = False
        self._started = False
        self._group_paused = False
        self._stopping = False
        self._stop_finalized = False
        self._resume_pending = False
        self._resume_nodes: frozenset[str] | None = None
        self._recovering = False
        self._last_verify_reason: dict[str, str] = {}
        # 早到结局缓冲：秒回 session 的结局可能先于出生回执到达（宿主出生
        # 任务在回执前已让位），回执到达时重放
        self._early_outcomes: dict[str, list[Any]] = {}
        self._agents_prep_failures = 0
        # 定时器 id 自增序号：一次挂表一个 id，过期触发不与新挂表串台
        self._timer_seq = 0

    # ── 协议面 ──────────────────────────────────────────────────

    @property
    def state(self) -> GraphState:
        """节点状态机与档案（只读面）。"""
        return self._state

    @property
    def description(self) -> str:
        return self._program.description

    @property
    def dag(self) -> DAG:
        """工作流拓扑（只读面）。"""
        return self._dag

    @property
    def work_dir(self) -> Path:
        """执行工作目录（只读面）。"""
        return self._work_dir

    @property
    def agents_dir(self) -> Path | None:
        """解析后的 agents 资源目录（相对基准由程序翻译决定）。"""
        return self._program.agents_dir

    @property
    def yaml_path(self) -> Path:
        """程序 yaml 路径（快照往返用，只读面）。"""
        return self._yaml_path

    @property
    def starting_prompt(self) -> str | None:
        """创建期提示词（列表展示用，只读面）。"""
        return self._starting_prompt

    @property
    def variables(self) -> dict[str, Any]:
        """原始入参变量（快照往返用）。"""
        return dict(self._input_variables)

    @property
    def prompt_fallback(self) -> str:
        """YAML 归一并渲染后的 OP（创建期提示词缺省值）。"""
        return self._program.variables.get("OP", "")

    @staticmethod
    def _naturally_finished(member: Any) -> bool:
        return member.stop_reason in (
            "evaluator_complete",
            "reviewer_complete",
            "max_iterations",
        )

    # ── 配置面 ──────────────────────────────────────────────────

    @staticmethod
    def _node_needs_member(node: dict) -> bool:
        """节点执行或验收是否要出生 agent 成员（决定 agents 软链是否先备）。"""
        acc = node.get("acceptance") or {}
        if node.get("task_type") == "agent":
            return True
        if node.get("task_type") == "generator":
            return node.get("generator_type", "agent") != "bash"
        return bool(acc.get("enabled")) and acc.get("type", "script") == "agent"

    @staticmethod
    def _world_session(world: RunWorld, session_id: str | None) -> Any:
        """world 里查成员事实（无则 None）。"""
        if not session_id:
            return None
        for info in world.sessions:
            if info.session_id == session_id:
                # 归属行仍在但池中已无此成员时，RunWorld 只有空 state；
                # 这不是可续跑的 session 事实。
                return info if info.state else None
        return None

    # ── 通知消费 ────────────────────────────────────────────────

    @staticmethod
    def _resume_wait_phase(act: NodeActivity) -> None:
        """续跑等待相位的换挡（验收侧/执行侧按进入前相位判定）。"""
        was_verify = act.phase == _PHASE_VERIFY_PAUSED_WAIT
        act.phase = _PHASE_VERIFY_RUNNING if was_verify else _PHASE_RUNNING

    @staticmethod
    def _spawn_wait_phase(act: NodeActivity) -> None:
        """出生等待相位换挡（验收侧/执行侧按暂停前相位判定）。"""
        was_verify = act.phase == _PHASE_VERIFY_PAUSED_WAIT
        act.phase = _PHASE_VERIFY_SPAWN if was_verify else _PHASE_SPAWN

    @staticmethod
    def _restart_context(act: NodeActivity) -> str:
        """成员重启回执的核对标签：与出生 context 同径，验收侧带 /verify。"""
        suffix = "/verify" if act.phase in _VERIFY_PHASES else ""
        return f"node={act.node_id}{suffix}"

    @staticmethod
    def _cancel_timer(act: NodeActivity, actions: list[RunAction]) -> None:
        """取消在途定时器（幂等）。"""
        if act.timer_id is not None:
            actions.append(CancelTimer(timer_id=act.timer_id))
            act.timer_id = None
            act.timer_kind = ""

    @staticmethod
    def _verify_extra_fields(act: NodeActivity, reason: str) -> dict[str, Any]:
        """验收结论的档案附加字段（方式/成员/原因）。"""
        return {
            "verify_type": (act.verify or {}).get("verify_type"),
            "verify_session_id": act.verify_sid,
            "verify_reason": reason or "",
        }

    @staticmethod
    def _duplicate_phase_sessions(
        snapshot: Mapping[str, Any],
        world: RunWorld,
    ) -> dict[tuple[str, str], set[str]]:
        """按 (节点, 执行/验收相位) 归集在跑成员（retired 归档代不计）。"""
        node_ids = snapshot["nodes"]
        retired_ids: dict[str, set[str]] = {}
        for old in snapshot.get("retired_instances", {}).values():
            node_id = old.get("id")
            if node_id not in node_ids or old.get("gen", 0) == node_ids[node_id].get("gen", 0):
                continue
            retired_ids.setdefault(node_id, set()).update(
                sid
                for sid in (old.get("workflow_controller_session_id"), old.get("verify_session_id"))
                if sid
            )
        by_phase: dict[tuple[str, str], set[str]] = {}
        for member in world.sessions:
            context = member.context or ""
            if member.state != "running" or not context.startswith("node="):
                continue
            node_id, _, phase = context.removeprefix("node=").partition("/")
            if node_id not in node_ids or member.session_id in retired_ids.get(node_id, ()):
                continue
            kind = "verify" if phase in ("verify", "verdict") else "execute"
            by_phase.setdefault((node_id, kind), set()).add(member.session_id)
        return by_phase

    def decide(self, world: RunWorld, notifications: Sequence[RunNotification]) -> list[RunAction]:
        """消费通知、产出动作：先复位组恢复，再消费通知、调度就绪、评估终局。"""
        prior_state = self._state.snapshot()
        actions: list[RunAction] = []
        # 整轮批式提交：轮内全部字段变更合并为轮末一次 checkpoint
        with self._state.batch():
            if self._resume_pending:
                # 组恢复的复位需要 world 对账成员存活与运行态，落在 decide 内做
                self._resume_pending = False
                self._apply_group_resume(world, actions)
            if not self._stopping and not self._group_paused:
                self._stop_duplicate_members(world, actions)
            for note in notifications:
                self._consume(note, world, actions)
            if self._stopping:
                self._finalize_stop(actions)
                return self._persist_before_actions(prior_state, actions)
            if self._group_paused:
                return self._persist_before_actions(prior_state, actions)
            if not self._started:
                self._started = True
                self._state.set_graph_status("running")
                self._emit_graph_event(actions, "graph_started")
            actions.extend(self._schedule(world))
            # 就绪节点启动会改变图状态；本轮返回前与节点档案保持一致。
            self._sync_graph_status(actions)
            actions.extend(self._maybe_finish(world))
            return self._persist_before_actions(prior_state, actions)

    def prepare_recovery(
        self,
        *,
        running: bool,
        reopened_at: str | None = None,
    ) -> None:
        """显式重开检查整图；普通崩溃仅恢复被中断的 running 节点。"""
        if not running:
            return
        if reopened_at and reopened_at != self._applied_reopen_at:
            self._applied_reopen_at = reopened_at
            self._resume_nodes = None
            self._resume_pending = True
            self._recovering = True
            self._stopping = False
            self._stop_finalized = False
        elif self._state.interrupted_nodes:
            self._resume_nodes = self._state.interrupted_nodes
            self._resume_pending = True
            self._recovering = True
        else:
            return
        # 恢复的是已启动过的 run：首轮 decide 的 _apply_group_resume 已发
        # graph_resumed，再补 graph_started 会把同一次续跑描述成两段启动
        self._started = True

    def on_control(self, control: ControlNotification) -> list[RunAction]:
        """控制受理反应：暂停即时落图状态（可返回事件动作），复位/停止记标记待 decide。"""
        if control.kind is ControlKind.PAUSED:
            self._group_paused = True
            # 有失败节点时保持 failed（并行一个暂停一个失败必须标 failed）
            status = "failed" if self._state.has_failed() else "paused"
            snap = self._state.snapshot()
            actions: list[RunAction] = []
            # 墙钟定时器不随暂停冻结：成员定时器取消、恢复对账时全额重挂，
            # 否则暂停时长吃掉超时预算、滞留 TimerFired 恢复后被当活超时；
            # 进程定时器所属进程已被组杀，同样作废
            for act in self._nodes.values():
                self._cancel_timer(act, actions)
            if status != snap.get("status"):
                self._state.set_graph_status(status)
                if status == "failed":
                    self._emit_graph_event(
                        actions,
                        "graph_failed",
                        failed=[
                            nid
                            for nid in self._dag.all_ids()
                            if self._state.status(nid) == "failed"
                        ],
                    )
                else:
                    self._emit_graph_event(actions, "graph_paused")
            return self._persist_before_actions(snap, actions)
        if control.kind is ControlKind.RESUMED:
            self._resume_nodes = None
            self._resume_pending = True
        elif control.kind is ControlKind.STOPPED:
            self._stopping = True
        return []

    def projection(self) -> dict[str, Any]:
        """带 schema_version 的状态投影（详情/状态端点透传）。"""
        from workflow_controller.plugins.graph.projection import build_projection

        return build_projection(
            self._state.snapshot(),
            self._dag,
            workflow=self._state.workflow,
            description=self._program.description,
            work_dir=str(self._work_dir),
            model=self._model,
            provider_index=self._provider_index,
        )

    def reconcile_projection(
        self,
        world: RunWorld,
    ) -> tuple[dict[str, Any], bool, list[RunAction]]:
        """详情读取时对账已消失的运行成员，沿 Run 串行通道保存修正。

        修正迁移照常产事件（node_state/node_history/graph_*/projection_updated）
        并随返回值交宿主派发：读路径不在事件流里静默改状态。
        """
        actions: list[RunAction] = []
        changed = False
        for node_id, info in self._state.snapshot()["nodes"].items():
            if info.get("status") != "running":
                continue
            act = self._nodes.get(node_id)
            if act is not None and act.phase in (
                _PHASE_SPAWN,
                _PHASE_VERIFY_SPAWN,
                _PHASE_VERDICT_SPAWN,
            ):
                continue
            sid = info.get("verify_session_id") or info.get("workflow_controller_session_id")
            if not sid or any(
                member.session_id == sid and (member.state or member.in_flight)
                for member in world.sessions
            ):
                continue
            if act is not None:
                self._cancel_timer(act, actions)
                self._drop_activity(act)
            self._set_node_status(actions, node_id, "stopped")
            self._emit_node_history(actions, node_id, "stopped")
            changed = True
        if changed:
            self._sync_graph_status(actions)
        return self.projection(), changed, actions

    def snapshot(self) -> dict[str, Any]:
        """不透明状态 blob：节点档案 + 运行配置 + 程序定位，恢复的完整输入。"""
        snap = self._state.snapshot()
        return {
            "schema_version": 1,
            "graph_id": snap.get("graph_id"),
            "status": snap.get("status"),
            "workflow": self._state.workflow,
            "description": self._program.description,
            "yaml_path": str(self._yaml_path),
            "work_dir": str(self._work_dir),
            "agents_dir_base": (str(self._agents_dir_base) if self._agents_dir_base else None),
            "variables": self._input_variables,
            "starting_prompt": self._starting_prompt,
            "user_prompt_path": str(self._user_prompt_path) if self._user_prompt_path else None,
            "applied_reopen_at": self._applied_reopen_at,
            # 未设置项不落键：键集即「已显式配置」的事实
            "options": {k: v for k, v in self.runtime_options().items() if v is not None},
            "state": {
                "nodes": snap.get("nodes", {}),
                "rollbacks": snap.get("rollbacks", {}),
                "rollback_edges": snap.get("rollback_edges", []),
                "retired_instances": snap.get("retired_instances", {}),
            },
            "pending_timers": [],
        }

    def bind_checkpoint(self, emit: Callable[[PersistDriverState], None]) -> None:
        """通过宿主动作端口同步提交节点状态，不直接接触存储。"""
        self._state.bind_checkpoint(lambda: emit(PersistDriverState(blob=self.snapshot())))

    def handle_world_op(
        self,
        world: RunWorld,
        op: str,
        args: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bool, list[RunAction]]:
        if op == "reconcile_projection":
            return self.reconcile_projection(world)
        return self.handle_op(op, args), False, []

        # stop/inject：结局流与核心成员操作面收尾，此处无需反应

    # ── 组控反应 ────────────────────────────────────────────────

    def handle_op(self, op: str, args: Mapping[str, Any]) -> dict[str, Any]:
        """驱动自定义操作：reload 重读运行配置，update_model 切换模型。"""
        if op == "reload":
            self.apply_runtime_config(dict(args.get("options") or {}))
            return {"reloaded": True}
        if op == "update_model":
            updates = {
                "model": args.get("model"),
                "provider_index": args.get("provider_index"),
                "model_pinned": args.get("model_pinned"),
            }
            runtime_config = {}
            for k, v in updates.items():
                if v is not None:
                    runtime_config[k] = v
            self.apply_runtime_config(runtime_config)
            return {
                "updated": True,
                "model": args.get("model"),
                "provider_index": args.get("provider_index"),
            }
        raise NotImplementedError(f"未知驱动操作 {op}")

    def runtime_options(self) -> dict[str, Any]:
        """当前运行配置项（快照往返与热切换共用的键集）。"""
        return {
            "runtime": self._runtime,
            "cli_wrapper": self._cli_wrapper,
            "model": self._model,
            "provider_index": self._provider_index,
            "model_pinned": self._model_pinned,
            "reviewer_enabled": self._reviewer_enabled,
            "reviewer_model": self._reviewer_model,
            "reviewer_provider_index": self._reviewer_provider_index,
            "reviewer_thinking_enabled": self._reviewer_thinking_enabled,
            "reviewer_thinking_budget_tokens": self._reviewer_thinking_budget_tokens,
            "reviewer_max_tokens": self._reviewer_max_tokens,
            "evaluator": self._evaluator,
            "max_iterations": self._max_iterations,
            "system_prompt": self._system_prompt,
            "reviewer_task_goal": self._reviewer_task_goal,
        }

    def apply_runtime_config(self, cfg: Mapping[str, Any]) -> None:
        """运行配置热切换：仅覆盖给出的键，新出生成员即时生效。"""
        for key in RUNNER_OPTION_KEYS:
            if key in cfg:
                setattr(self, f"_{key}", cfg[key])

    def _init_runner_options(self, options: Mapping[str, Any]) -> None:
        """构造期运行配置入账：键集内未给出的项记 None，model_pinned 记 bool。"""
        opts = dict(options)
        self._runtime = opts.get("runtime")
        self._cli_wrapper = opts.get("cli_wrapper")
        self._model = opts.get("model")
        self._provider_index = opts.get("provider_index")
        self._model_pinned = bool(opts.get("model_pinned", False))
        self._reviewer_enabled = opts.get("reviewer_enabled")
        self._reviewer_model = opts.get("reviewer_model")
        self._reviewer_provider_index = opts.get("reviewer_provider_index")
        self._reviewer_thinking_enabled = opts.get("reviewer_thinking_enabled")
        self._reviewer_thinking_budget_tokens = opts.get("reviewer_thinking_budget_tokens")
        self._reviewer_max_tokens = opts.get("reviewer_max_tokens")
        self._evaluator = opts.get("evaluator")
        self._max_iterations = opts.get("max_iterations")
        self._system_prompt = opts.get("system_prompt")
        self._reviewer_task_goal = opts.get("reviewer_task_goal")

    def _persist_before_actions(
        self,
        prior_state: dict[str, Any],
        actions: list[RunAction],
    ) -> list[RunAction]:
        """节点状态先落 Run 快照，再允许本轮动作产生外部副作用。

        checkpoint 钩子在位时轮末提交（batch 退出/逐次落盘）已同步写
        run_driver_state 最新槽位，动作再写一遍是热路径冗余；钩子缺席
        （直连/测试路径）时动作列表才是唯一持久化通道。
        """
        if prior_state != self._state.snapshot() and not self._state.checkpoint_bound:
            actions.insert(0, PersistDriverState(blob=self.snapshot()))
        return actions

    def _consume(self, note: RunNotification, world: RunWorld, actions: list[RunAction]) -> None:
        """按通知类型分派到对应处理面。"""
        if isinstance(note, SessionOutcomeNotification):
            self._on_outcome(note, world, actions)
        elif isinstance(note, SessionOutcomeReplayed):
            act = self._activity_by_session(note.session_id)
            if act is not None:
                if note.outcome is None:
                    self._drop_activity(act)
                    self._reset_node_for_rerun(act.node_id, keep_attempts=True)
                else:
                    self._on_outcome(
                        SessionOutcomeNotification(note.outcome),
                        world,
                        actions,
                    )
        elif isinstance(note, SessionActivated):
            self._on_activated(note, world, actions)
        elif isinstance(note, ProcessOutput):
            self._on_proc_output(note)
        elif isinstance(note, ProcessExit):
            self._on_proc_exit(note, actions)
        elif isinstance(note, ArtifactRead):
            self._on_artifact(note, actions)
        elif isinstance(note, TimerFired):
            self._on_timer(note, actions)
        elif isinstance(note, SessionControlApplied):
            self._on_member_control(note, world, actions)

    # ── 调度 ────────────────────────────────────────────────────

    def _on_activated(
        self,
        note: SessionActivated,
        world: RunWorld,
        actions: list[RunAction],
    ) -> None:
        """出生/续跑回执：换相等待、挂定时器、重放早到结局。"""
        act = self._activity_by_context(note.context)
        if act is None:
            return
        if not note.ok:
            self._on_activate_failed(act, note.error or "成员激活失败", actions)
            return
        act.session_id = note.session_id
        act.restoring_missing = False
        if act.phase in (_PHASE_SPAWN, _PHASE_CONTINUE):
            self._on_executor_activated(act, note, world, actions)
        elif act.phase in (_PHASE_VERIFY_SPAWN, _PHASE_VERIFY_CONTINUE):
            self._on_verify_activated(act, note, world, actions)
        elif act.phase == _PHASE_VERDICT_SPAWN:
            act.phase = _PHASE_VERDICT_RUNNING
            self._arm_timer(act, _TIMER_VERDICT, _VERDICT_FALLBACK_TIMEOUT_S, actions)
            self._replay_early_outcomes(act, note.session_id, world, actions)

    def _on_activate_failed(
        self,
        act: NodeActivity,
        error: str,
        actions: list[RunAction],
    ) -> None:
        """出生失败回执：按所处相位分别记执行错误或验收失败。"""
        if act.restoring_missing:
            self._drop_activity(act)
            self._reset_node_for_rerun(act.node_id, keep_attempts=True)
            return
        if act.phase in (_PHASE_SPAWN, _PHASE_CONTINUE):
            act.executor_error = f"agent session 出生失败: {error}"
            self._after_executor(act, actions)
        elif act.phase in (_PHASE_VERIFY_SPAWN, _PHASE_VERIFY_CONTINUE):
            self._conclude_verify(act, False, f"验收 agent 出生失败: {error}", actions)
        elif act.phase == _PHASE_VERDICT_SPAWN:
            self._conclude_verify(act, False, "兜底判定 agent 出生失败", actions)

    def _on_executor_activated(
        self,
        act: NodeActivity,
        note: SessionActivated,
        world: RunWorld,
        actions: list[RunAction],
    ) -> None:
        """执行侧出生回执：换 running 相、挂定时器、重放早到结局。"""
        from_continue = act.phase == _PHASE_CONTINUE
        act.phase = _PHASE_RUNNING
        self._state.set_node_status(act.node_id, "running", session_id=note.session_id)
        self._arm_member_timer(act, actions)
        if act.continues == 0:
            self._emit_node_history(actions, act.node_id, "running", attempt=act.attempt)
        # 秒回成员的早到结局在此重放（回执先核对、结局再分类）
        self._replay_early_outcomes(
            act,
            note.session_id,
            world,
            actions,
            skip_leading_stopped=from_continue,
        )

    def _on_verify_activated(
        self,
        act: NodeActivity,
        note: SessionActivated,
        world: RunWorld,
        actions: list[RunAction],
    ) -> None:
        """验收侧出生回执：记录验收 session、挂定时器、重放早到结局。"""
        from_continue = act.phase == _PHASE_VERIFY_CONTINUE
        act.phase = _PHASE_VERIFY_RUNNING
        act.verify_sid = note.session_id
        self._state.set_verify_session_id(act.node_id, note.session_id)
        self._arm_member_timer(act, actions)
        self._emit_verify_running_history(actions, act)
        self._replay_early_outcomes(
            act,
            note.session_id,
            world,
            actions,
            skip_leading_stopped=from_continue,
        )

    def _replay_early_outcomes(
        self,
        act: NodeActivity,
        session_id: str,
        world: RunWorld,
        actions: list[RunAction],
        *,
        skip_leading_stopped: bool = False,
    ) -> None:
        """重放该成员的早到结局；活动已在此间收口则剩余结局作废。

        skip_leading_stopped：续跑回执先丢内部停止产生的 stopped 结局
        （停止的旧结局不是分类输入，续跑后的新结局才是）。
        """
        early = self._early_outcomes.pop(session_id, None)
        if not early:
            return
        if skip_leading_stopped and early[0].outcome.kind == "stopped":
            early = early[1:]
        for outcome_note in early:
            if self._nodes.get(act.node_id) is not act:
                return  # 活动已收口（如秒过验收），余下结局作废
            self._on_outcome(outcome_note, world, actions)

    def _buffer_early_outcome(self, note: SessionOutcomeNotification) -> None:
        """结局入早到缓冲（有界：单成员至多 4 条、缓冲至多 64 键）。"""
        early = self._early_outcomes.setdefault(note.outcome.session_id, [])
        if len(early) < 4 and len(self._early_outcomes) < 64:
            early.append(note)

    def _on_outcome(
        self,
        note: SessionOutcomeNotification,
        world: RunWorld,
        actions: list[RunAction],
    ) -> None:
        """成员结局分类：暂停落断点、停止保断点、完成交验收。"""
        outcome = note.outcome
        act = self._activity_by_session(outcome.session_id)
        if act is None:
            # 未对上账的结局：可能是早于出生回执到达的秒回结局，入缓冲重放；
            # 无关成员的结局随缓冲上限自然淘汰
            self._buffer_early_outcome(note)
            return
        kind = outcome.kind
        if act.phase in (_PHASE_CONTINUE, _PHASE_VERIFY_CONTINUE):
            # 续跑中：停止产生的旧结局入缓冲，由续跑回执重放时判别
            self._buffer_early_outcome(note)
            return
        if act.phase == _PHASE_STOPPING:
            self._after_executor(act, actions)
            return
        if act.phase == _PHASE_VERIFY_STOPPING:
            reason = (act.verify or {}).get("verify_reason") or "验收 agent 超时"
            self._conclude_verify(act, False, reason, actions)
            return
        if kind == "paused":
            self._on_paused_outcome(act, world, outcome, actions)
            return
        if kind == "stopped":
            self._node_stopped_outcome(act, actions)
            return
        # completed / failed：session 跑完即交验收判定（失败语义由 verdict 决定）
        # 成员已终态，其定时器使命结束：先取消，防在 report/验收/兜底的
        # 换相窗口内对已完成成员误触发续跑或落假超时
        self._cancel_timer(act, actions)
        self._on_final_outcome(act, outcome, actions)

    def _on_paused_outcome(
        self,
        act: NodeActivity,
        world: RunWorld,
        outcome: Any,
        actions: list[RunAction],
    ) -> None:
        """paused 结局：节点落 stopped 断点并同步 graph 状态。"""
        # 陈旧判定以 world 为准：成员当前在跑说明暂停已被恢复超越
        info = self._world_session(world, outcome.session_id)
        if info is not None and info.state == "running":
            return
        self._cancel_timer(act, actions)
        act.phase = _PHASE_VERIFY_PAUSED_WAIT if act.phase in _VERIFY_PHASES else _PHASE_PAUSED_WAIT
        if self._state.status(act.node_id) != "stopped":
            self._set_node_status(actions, act.node_id, "stopped")
            self._emit_node_history(actions, act.node_id, "stopped")
        self._sync_graph_status(actions)

    def _on_final_outcome(
        self,
        act: NodeActivity,
        outcome: Any,
        actions: list[RunAction],
    ) -> None:
        """completed/failed 结局：按相位交执行收尾或验收判定。"""
        if act.phase == _PHASE_RUNNING:
            act.result_digest = outcome.result_digest
            act.result_iteration = outcome.iteration
            self._after_executor(act, actions)
        elif act.phase == _PHASE_VERIFY_RUNNING:
            self._judge_verify(act, outcome.result_digest or "", actions)
        elif act.phase == _PHASE_VERDICT_RUNNING:
            self._conclude_verify_from_verdict(
                act,
                gates.judge_verdict_last_line(outcome.result_digest or "") is True,
                actions,
            )
        elif act.phase == _PHASE_PAUSED_WAIT:
            # 暂停成员续跑后的正常结束：graph 回运行态，交验收
            self._state.set_graph_status("running")
            act.result_digest = outcome.result_digest
            act.result_iteration = outcome.iteration
            self._after_executor(act, actions)
        elif act.phase == _PHASE_VERIFY_PAUSED_WAIT:
            self._state.set_graph_status("running")
            self._judge_verify(act, outcome.result_digest or "", actions)

    def _node_stopped_outcome(self, act: NodeActivity, actions: list[RunAction]) -> None:
        """成员被停止（非暂停）：节点落 stopped 断点，不算失败。"""
        self._cancel_timer(act, actions)
        self._set_node_status(actions, act.node_id, "stopped")
        self._emit_node_history(actions, act.node_id, "stopped")
        self._sync_graph_status(actions)
        self._drop_activity(act)

    # ── 节点阶段推进 ─────────────────────────────────────────────

    def _on_proc_output(self, note: ProcessOutput) -> None:
        """进程输出流：bash 落节点档案、验收脚本留尾部。"""
        act = self._activity_by_context(note.context)
        if act is None:
            return
        if act.phase == _PHASE_EXEC_PROC:
            self._state.append_output(act.node_id, note.line)
        elif act.phase == _PHASE_VERIFY_PROC:
            act.verify_tail.append(note.line)

    def _on_proc_exit(self, note: ProcessExit, actions: list[RunAction]) -> None:
        """进程退出分类：执行/报告/验收三相位各自收口。"""
        context = note.context or ""
        if context == "prep:agents":
            self._on_agents_prep_exit(note)
            return
        act = self._activity_by_context(context)
        if act is None:
            return
        mark = note.exit_mark
        if act.phase == _PHASE_EXEC_PROC:
            self._on_exec_proc_exit(note, act, mark, actions)
        elif act.phase == _PHASE_REPORT:
            self._on_report_proc_exit(note, act, mark, actions)
        elif act.phase == _PHASE_VERIFY_PROC:
            self._on_verify_proc_exit(note, act, mark, actions)

    def _on_agents_prep_exit(self, note: ProcessExit) -> None:
        """agents 软链准备退出：失败重试有界，超限后放行调度。"""
        self._agents_ready = note.returncode == 0
        self._agents_prep_inflight = False
        if not self._agents_ready:
            # 软链准备是尽力面：失败重试有界，超限后放行调度
            # （缺链的成员问题由其自身的执行失败呈现）
            self._agents_prep_failures += 1
            if self._agents_prep_failures >= 3:
                logger.warning("agents 目录准备连续失败，放行调度（无软链）")
                self._agents_ready = True

    def _on_exec_proc_exit(
        self,
        note: ProcessExit,
        act: NodeActivity,
        mark: str | None,
        actions: list[RunAction],
    ) -> None:
        """执行 bash 进程退出：组杀走断点，其余按退出码记执行错误。"""
        if mark == "plane_killed":
            self._bash_killed_outcome(act, actions)
            return
        outcome = gates.classify_process_exit(note.returncode, False, mark)
        if outcome == "spawn_failed":
            act.executor_error = f"bash 起动失败 (exit {note.returncode})"
        elif outcome == "signaled":
            act.executor_error = f"bash 被外部信号终止 (exit {note.returncode})"
        elif outcome == "failed":
            act.executor_error = f"bash exit {note.returncode}"
        self._after_executor(act, actions)

    def _on_report_proc_exit(
        self,
        note: ProcessExit,
        act: NodeActivity,
        mark: str | None,
        actions: list[RunAction],
    ) -> None:
        """报告落盘进程退出：组杀不进验收，失败仅告警。"""
        if mark == "plane_killed":
            # 停止/暂停组杀落在 report 相位：报告未落盘即工作未完成，
            # 不得进验收（终局轮仍会派发残留的验收进程动作）
            self._bash_killed_outcome(act, actions)
            return
        if note.returncode != 0:
            logger.warning(
                "node %s 报告落盘失败 (exit %s)",
                act.node_id,
                note.returncode,
            )
        self._enter_verify(act, actions)

    def _on_verify_proc_exit(
        self,
        note: ProcessExit,
        act: NodeActivity,
        mark: str | None,
        actions: list[RunAction],
    ) -> None:
        """验收脚本退出：超时组杀/起动失败/正常判定三路收尾。"""
        self._cancel_timer(act, actions)
        if act.verify_kill_pending:
            act.verify_kill_pending = False
            self._conclude_verify(
                act,
                False,
                f"验收脚本超时 (exit {note.returncode})",
                actions,
            )
            return
        if mark == "plane_killed":
            self._bash_killed_outcome(act, actions)
            return
        outcome = gates.classify_process_exit(note.returncode, False, mark)
        if outcome == "spawn_failed":
            self._conclude_verify(
                act,
                False,
                f"验收脚本起动失败 (exit {note.returncode})",
                actions,
            )
            return
        out_tail = "\n".join(act.verify_tail[-5:]).strip()[-300:]
        script = (act.verify or {}).get("script", "")
        self._conclude_verify(
            act,
            outcome == "ok",
            gates.script_verify_reason(script, note.returncode, out_tail),
            actions,
        )

    def _bash_killed_outcome(self, act: NodeActivity, actions: list[RunAction]) -> None:
        """run 键进程被组杀（暂停/停止）：不算失败，保留断点或重排重跑。"""
        if self._stopping or self._group_paused:
            self._set_node_status(actions, act.node_id, "stopped")
            self._emit_node_history(actions, act.node_id, "stopped")
            self._sync_graph_status(actions)
        else:
            # 恢复后才到达的暂停期组杀：工作未完成，重排为 pending 重跑
            self._reset_node_for_rerun(act.node_id)
            self._emit_graph_event(
                actions,
                "node_state",
                node_id=act.node_id,
                status="pending",
            )
        self._drop_activity(act)

    def _on_artifact(self, note: ArtifactRead, actions: list[RunAction]) -> None:
        """子图 yaml 读取结果：解析校验通过挂待合并，失败走执行错误。"""
        act = self._activity_by_context(note.context)
        if act is None or act.phase != _PHASE_SUB_READ:
            return
        if not note.exists or note.value is None:
            if note.error:
                act.executor_error = f"subgraph 未就绪: {note.error}: {note.path}"
            else:
                act.executor_error = f"subgraph 未就绪: subgraph yaml 不存在: {note.path}"
            self._after_executor(act, actions)
            return
        try:
            config = translator.normalize_config(translator.parse_yaml_text(note.value) or {})
        except Exception as e:
            act.executor_error = f"subgraph 未就绪: subgraph yaml 解析失败: {e}"
            self._after_executor(act, actions)
            return
        ready, why = scheduler.validate_subgraph_config(config, self._state.workflow)
        if not ready:
            act.executor_error = f"subgraph 未就绪: {why}"
            self._after_executor(act, actions)
            return
        act.pending_subgraph = prefix_subgraph_nodes(config, act.node_id)
        self._after_executor(act, actions)

    def _on_timer(self, note: TimerFired, actions: list[RunAction]) -> None:
        """定时器到期：验收脚本组杀、兜底判定收口、成员超时续跑或判败。"""
        node_id = note.timer_id.removeprefix("t:").rsplit(":", 1)[0]
        act = self._nodes.get(node_id)
        if act is None or act.timer_id != note.timer_id:
            return
        act.timer_id = None
        kind = act.timer_kind
        act.timer_kind = ""
        if kind == _TIMER_VERIFY_SCRIPT:
            act.verify_kill_pending = True
            actions.append(KillProcesses(context=f"node={node_id}/verify"))
            return
        if kind == _TIMER_VERDICT:
            # 兜底 agent 超时：结论按无判定收（False），停掉兜底成员
            actions.append(AwaitSessions(ids=(act.session_id,), timeout_s=0))
            self._conclude_verify_from_verdict(act, False, actions)
            return
        self._on_member_timeout(act, actions)

    def _on_member_timeout(self, act: NodeActivity, actions: list[RunAction]) -> None:
        """成员超时：续跑（checkpoint 语义）；预算耗尽按超时失败。"""
        act.continues += 1
        if act.continues > act.max_continues:
            detail = (
                f"({act.timer_delay_s}s x {act.continues}, continues exhausted)"
                if act.max_continues > 0
                else f"({act.timer_delay_s}s)"
            )
            error = f"agent session {act.session_id} timed out {detail}"
            if act.phase == _PHASE_VERIFY_RUNNING:
                act.verify = {"verify_type": "agent", "verify_reason": error}
                act.phase = _PHASE_VERIFY_STOPPING
            else:
                act.executor_error = error
                act.phase = _PHASE_STOPPING
            actions.append(AwaitSessions(ids=(act.session_id,), timeout_s=0))
            return
        logger.info(
            "member session %s timed out (%ss), continuing (%d/%d)",
            act.session_id,
            act.timer_delay_s,
            act.continues,
            act.max_continues,
        )
        continue_prompt = (
            prompts.VERIFY_CONTINUE_PROMPT
            if act.phase == _PHASE_VERIFY_RUNNING
            else prompts.CONTINUE_PROMPT
        )
        act.phase = (
            _PHASE_VERIFY_CONTINUE if act.phase == _PHASE_VERIFY_RUNNING else _PHASE_CONTINUE
        )
        actions.append(
            RestartSession(
                session_id=act.session_id,
                policy=RestartPolicy.CONTINUE,
                spec_override=SessionSpec(
                    name=f"graph:{act.node_id}",
                    prompt=continue_prompt,
                    cwd=str(self._work_dir),
                ),
                context_override=self._restart_context(act),
            )
        )

    def _on_member_control(
        self,
        note: SessionControlApplied,
        world: RunWorld,
        actions: list[RunAction],
    ) -> None:
        """成员级控制的节点态同步（组控成员操作经通知回投）。"""
        nid = self._node_bound_to_session(note.session_id)
        if nid is None:
            return
        act = self._nodes.get(nid)
        if note.operation == "pause":
            # 暂停中的成员由结局流自愈（paused 结局标 stopped）；此处先落
            # 断点供前端即时呈现
            if self._state.status(nid) == "running":
                self._set_node_status(actions, nid, "stopped")
                self._sync_graph_status(actions)
        elif note.operation == "resume":
            if self._state.status(nid) != "stopped":
                return
            self._set_node_status(actions, nid, "running")
            self._sync_graph_status(actions)
            if act is None:
                act = self._reuse_activity(nid, note.session_id)
            if act is not None:
                info = self._world_session(world, note.session_id)
                if info is not None and info.state == "running":
                    self._resume_wait_phase(act)
                    self._arm_member_timer(act, actions)
                elif info is not None and (
                    info.state == "idle" and info.stop_reason == "user_pause"
                ):
                    self._spawn_wait_phase(act)
                    actions.append(self._resume_member_action(act, note.session_id))
                else:
                    # 成员已消亡：节点复活按全新执行走（新成员出生）。
                    # 状态不落回 pending 则调度器永不再选中，run 挂死
                    self._drop_activity(act)
                    self._reset_node_for_rerun(nid, keep_attempts=True)

    def _apply_group_resume(self, world: RunWorld, actions: list[RunAction]) -> None:
        """组恢复的复位：failed/skipped 重跑但保留 attempt 计数（历史不覆盖）；
        stopped/running 按 world 对账成员存续——可复用则续跑/重挂守护，消亡则重排重跑。
        """
        self._group_paused = False
        recovering = self._recovering
        self._recovering = False
        resume_nodes = self._resume_nodes
        self._resume_nodes = None
        snapshot = self._state.snapshot()
        for nid, info in snapshot["nodes"].items():
            if resume_nodes is not None and nid not in resume_nodes:
                continue
            status = info.get("status")
            if status in ("failed", "skipped"):
                # 续跑（reopen/resume）不重置 attempts：新一次执行拿到递增的
                # attempt 号，node_history 事件按 (gen, attempt) 去重时不覆盖
                # 之前失败的尝试记录。
                self._reset_node_for_rerun(nid, keep_attempts=True)
            elif status == "stopped":
                self._resume_stopped_node(
                    nid, self._nodes.get(nid), world, info, recovering, actions
                )
            elif status == "running":
                self._resume_running_node(
                    nid, self._nodes.get(nid), world, info, recovering, actions
                )
        self._state.set_graph_status("running")
        self._emit_graph_event(actions, "graph_resumed")

    def _resume_reconcile_member(
        self, act: NodeActivity | None, world: RunWorld, info: Mapping[str, Any]
    ) -> tuple[bool, Any, Any]:
        """复位核对目标与它的 world 成员，返回 (verify_side, recon_sid, member)。"""
        verify_side, recon_sid = _resume_reconcile_target(act, info)
        member = self._world_session(world, recon_sid) if recon_sid else None
        return verify_side, recon_sid, member

    def _resume_stopped_node(
        self,
        nid: str,
        act: NodeActivity | None,
        world: RunWorld,
        info: Mapping[str, Any],
        recovering: bool,
        actions: list[RunAction],
    ) -> None:
        """stopped 断点复位：按 world 核对成员存续，可续则续，消亡重排。"""
        verify_side, recon_sid, member = self._resume_reconcile_member(act, world, info)
        resumable = member is not None and (
            member.state == "running"
            or (member.state == "idle" and member.stop_reason == "user_pause")
        )
        if recon_sid and resumable:
            # 验收侧恢复不覆写执行器档案（workflow_controller_session_id 仍指执行器）
            self._state.set_node_status(
                nid,
                "running",
                session_id=None if verify_side else recon_sid,
            )
            act = self._reuse_activity(nid, recon_sid)
            if act is not None:
                if member.state == "running":
                    self._resume_wait_phase(act)
                    self._arm_member_timer(act, actions)
                else:
                    self._spawn_wait_phase(act)
                    actions.append(self._resume_member_action(act, recon_sid))
        elif recon_sid and member is not None and self._naturally_finished(member):
            self._state.set_node_status(nid, "running")
            self._replay_finished_member(nid, recon_sid, verify_side, actions)
        elif recon_sid and any(
            item.session_id == recon_sid and not item.state and not item.in_flight
            for item in world.sessions
        ):
            self._resume_missing_member(nid, act, recon_sid, actions)
        else:
            # 无 session（bash 被组暂停终止）或成员已消亡/终态：重排重跑
            self._reset_node_for_rerun(
                nid,
                keep_attempts=recovering or bool(recon_sid),
            )

    def _resume_missing_member(
        self,
        nid: str,
        act: NodeActivity | None,
        recon_sid: str,
        actions: list[RunAction],
    ) -> None:
        """池中消失但 Run 归属仍在：按源提交尝试从持久层续原 session。"""
        if act is not None:
            self._cancel_timer(act, actions)
            self._drop_activity(act)
        self._state.set_node_status(nid, "running")
        act = self._reuse_activity(nid, recon_sid)
        self._spawn_wait_phase(act)
        act.restoring_missing = True
        actions.append(self._resume_member_action(act, recon_sid))

    def _resume_running_node(
        self,
        nid: str,
        act: NodeActivity | None,
        world: RunWorld,
        info: Mapping[str, Any],
        recovering: bool,
        actions: list[RunAction],
    ) -> None:
        """running 节点复位：滞留暂停结局按 world 核对，消亡才重排。

        终态成员不在此重跑——其结局可能在途（完成先于暂停扫掠或紧随
        恢复到达），交结局流推进。
        """
        verify_side, recon_sid, member = self._resume_reconcile_member(act, world, info)
        if member is not None and member.state == "running":
            act = self._reuse_activity(nid, recon_sid)
            if act is not None:
                if act.phase in (_PHASE_PAUSED_WAIT, _PHASE_VERIFY_PAUSED_WAIT):
                    self._resume_wait_phase(act)
                if act.phase == _PHASE_VERDICT_RUNNING:
                    self._arm_timer(act, _TIMER_VERDICT, _VERDICT_FALLBACK_TIMEOUT_S, actions)
                else:
                    self._arm_member_timer(act, actions)
        elif member is not None and member.state == "idle" and member.stop_reason == "user_pause":
            act = self._reuse_activity(nid, recon_sid)
            if act is not None:
                actions.append(self._resume_member_action(act, recon_sid))
        elif recon_sid and member is not None and self._naturally_finished(member):
            self._replay_finished_member(nid, recon_sid, verify_side, actions)
        elif member is None:
            self._rerun_disappeared_member(nid, act, recon_sid, recovering, actions)

    def _rerun_disappeared_member(
        self,
        nid: str,
        act: NodeActivity | None,
        recon_sid: str | None,
        recovering: bool,
        actions: list[RunAction],
    ) -> None:
        """成员已消亡（world 无此成员）：弃置活动并重排 pending 重跑。

        出生回执滞留 FIFO（组暂停冻结窗内出生落定）时除外——session
        尚未绑定但成员在途，按消亡重排会重复出生。
        """
        if act is not None and act.phase in (
            _PHASE_SPAWN,
            _PHASE_VERIFY_SPAWN,
            _PHASE_VERDICT_SPAWN,
        ):
            return
        # bash（无 session、已被组杀）或成员已消亡（池中无此
        # session）：pending 重跑；活动弃置，滞留的 paused 结局/
        # TimerFired 随之作废
        gone = self._nodes.pop(nid, None)
        if gone is not None:
            self._cancel_timer(gone, actions)
        self._reset_node_for_rerun(
            nid,
            keep_attempts=recovering or bool(recon_sid),
        )

    def _replay_finished_member(
        self,
        node_id: str,
        session_id: str,
        verify: bool,
        actions: list[RunAction],
    ) -> None:
        act = self._reuse_activity(node_id, session_id)
        act.phase = _PHASE_VERIFY_RUNNING if verify else _PHASE_RUNNING
        act.session_id = session_id
        if verify:
            act.verify_sid = session_id
        actions.append(ReplaySessionOutcome(session_id))

    def _stop_duplicate_members(
        self,
        world: RunWorld,
        actions: list[RunAction],
    ) -> None:
        """已有成员事实显示同节点同相位并发时，停止它们而非再出生。"""
        snapshot = self._state.snapshot()
        by_phase = self._duplicate_phase_sessions(snapshot, world)
        for (node_id, kind), ids in by_phase.items():
            if len(ids) <= 1 or self._state.status(node_id) == "failed":
                continue
            related = set(ids)
            for (other_node, _), other_ids in by_phase.items():
                if other_node == node_id:
                    related.update(other_ids)
            info = snapshot["nodes"][node_id]
            for sid in (
                info.get("workflow_controller_session_id"),
                info.get("verify_session_id"),
            ):
                if sid:
                    related.add(sid)
            self._quash_duplicate_phase(node_id, kind, related, actions)

    def _quash_duplicate_phase(
        self,
        node_id: str,
        kind: str,
        related: set[str],
        actions: list[RunAction],
    ) -> None:
        """停掉同节点并发成员，弃置绑定活动并落失败。"""
        actions[:] = [
            action
            for action in actions
            if not (isinstance(action, RestartSession) and action.session_id in related)
        ]
        bound = self._nodes.pop(node_id, None)
        if bound is not None:
            self._cancel_timer(bound, actions)
        actions.append(AwaitSessions(ids=tuple(sorted(related)), timeout_s=0))
        self._state.set_failure_reason(
            node_id,
            f"检测到并发重复{kind} session，已停止",
        )
        self._set_node_status(actions, node_id, "failed")
        self._sync_graph_status(actions)

    # ── 组装与工具 ──────────────────────────────────────────────

    def _finalize_stop(self, actions: list[RunAction]) -> None:
        """stop 收尾轮：活动节点落 stopped 断点，graph 落 paused（终态归控制侧）。"""
        if self._stop_finalized:
            return
        self._stop_finalized = True
        for act in list(self._nodes.values()):
            self._cancel_timer(act, actions)
            self._set_node_status(actions, act.node_id, "stopped")
            self._emit_node_history(actions, act.node_id, "stopped")
        self._nodes.clear()
        status = self._state.snapshot().get("status")
        if status not in ("completed", "failed"):
            self._state.set_graph_status("paused")
            self._emit_graph_event(actions, "graph_paused")

    def _schedule(self, world: RunWorld) -> list[RunAction]:
        """就绪节点调度：agents 软链先备，节点容量门内逐个起步。"""
        actions: list[RunAction] = []
        ready = scheduler.select_ready_nodes(self._dag, self._state)
        if not ready:
            return actions
        if not self._agents_ready and any(self._node_needs_member(n) for n in ready):
            # agent 成员出生前先备好 cwd 下的 agents 软链（opencode 发现面）
            if not self._agents_prep_inflight:
                self._agents_prep_inflight = True
                actions.append(
                    StartProcess(
                        cmd=self._agents_link_command(),
                        cwd=str(self._work_dir),
                        context="prep:agents",
                        stream=False,
                    )
                )
            return actions
        for node in ready:
            if len(self._nodes) >= self._program.max_parallel:
                break
            actions.extend(self._start_node(node, world))
        return actions

    def _start_node(self, node: dict[str, Any], world: RunWorld) -> list[RunAction]:
        """节点起步：bash 派进程、agent 复用或新出生，attempt 记账。"""
        node_id = node["id"]
        task_type = node.get("task_type")
        actions: list[RunAction] = []
        existing_sid = (
            self._state.snapshot()["nodes"].get(node_id, {}).get("workflow_controller_session_id")
        )
        # resume 场景不增 attempts：节点已有在跑/被暂停的 session，本次走
        # 复用路径（继续同一次运行）；只有全新起 session 或 bash 才增
        member_node = task_type == "agent" or (
            task_type == "generator" and node.get("generator_type", "agent") != "bash"
        )
        resuming = member_node and self._reusable_session(existing_sid, world) is not None
        if not resuming:
            self._state.increment_attempts(node_id)
        attempt = self._state.attempts(node_id)
        self._state.set_node_status(node_id, "running")
        self._state.clear_output(node_id)
        ctx = {**self._program.variables, "node_id": node_id, "attempt": str(attempt)}

        if task_type == "generator":
            # 已合并过（续跑/恢复）或已编排完成的 generator：直接进验收/读挂
            if self._start_merged_generator(node, node_id, ctx, attempt, actions):
                return actions
            if node.get("generator_type") == "bash":
                return self._spawn_bash_node(node, node_id, ctx, attempt, actions)

        if task_type == "bash":
            return self._spawn_bash_node(node, node_id, ctx, attempt, actions)

        # agent 成员（执行器或 generator 编排）
        reused = self._reuse_member_node(node, node_id, existing_sid, attempt, world, actions)
        if reused is not None:
            return reused
        self._spawn_member_node(node, node_id, ctx, attempt, actions)
        return actions

    def _start_merged_generator(
        self,
        node: dict[str, Any],
        node_id: str,
        ctx: dict[str, Any],
        attempt: int,
        actions: list[RunAction],
    ) -> bool:
        """已合并/已编排的 generator 短路进验收或读挂；返回是否已收尾。"""
        merged = self._state.subgraph_node_ids(node_id)
        if not merged and not (
            node.get("_xlsx_subgraph") or self._state.subgraph_generated(node_id)
        ):
            return False
        act = NodeActivity(node_id=node_id, phase=_PHASE_SUB_READ, attempt=attempt)
        self._nodes[node_id] = act
        if merged:
            self._enter_verify(act, actions)
        else:
            self._dispatch_subgraph_read(node, ctx, act, actions)
        return True

    def _reuse_member_node(
        self,
        node: dict[str, Any],
        node_id: str,
        existing_sid: str | None,
        attempt: int,
        world: RunWorld,
        actions: list[RunAction],
    ) -> list[RunAction] | None:
        """agent 成员起步：复用在池成员（暂停的先续跑）；无可复用返回 None。

        复用保持原超时语义——不续跑，超时即失败（max_continues=0）。
        """
        reuse_sid = self._reusable_session(existing_sid, world)
        act = NodeActivity(
            node_id=node_id,
            phase=_PHASE_SPAWN,
            attempt=attempt,
            max_continues=int(node.get("max_session_continues", 2) or 0),
        )
        self._nodes[node_id] = act
        if reuse_sid is None:
            return None
        act.max_continues = 0
        act.session_id = reuse_sid
        info = self._world_session(world, reuse_sid)
        if info is not None and info.state != "running":
            actions.append(self._resume_member_action(act, reuse_sid))
            return actions
        act.phase = _PHASE_RUNNING
        self._arm_member_timer(act, actions)
        self._emit_node_history(actions, node_id, "running", attempt=attempt)
        return actions

    def _spawn_member_node(
        self,
        node: dict[str, Any],
        node_id: str,
        ctx: dict[str, Any],
        attempt: int,
        actions: list[RunAction],
    ) -> None:
        """全新 agent 成员出生（执行器或 generator 编排）。"""
        retry_ctx = ""
        if attempt > 1 and self._last_verify_reason.get(node_id):
            retry_ctx = prompts.build_retry_context(
                node,
                ctx,
                self._last_verify_reason[node_id],
            )
        prompt = prompts.build_node_prompt(
            node,
            ctx,
            "" if node.get("task_type") == "generator" else retry_ctx,
            workflow_system_prompt=self._program.config.get("system_prompt", ""),
        )
        actions.append(
            StartSession(
                spec=self._member_spec(
                    name=f"graph:{node_id}",
                    prompt=prompt,
                    agent_name=node.get("executor", ""),
                    loop_max_iter=self._max_iterations or node.get("max_iterations", 1),
                    loop_evaluator=self._evaluator or "always_complete",
                    node_id=node_id,
                    role="executor",
                    verify=False,
                ),
                role="executor",
                context=f"node={node_id}",
            )
        )

    def _spawn_bash_node(
        self, node: dict, node_id: str, ctx: dict, attempt: int, actions: list[RunAction]
    ) -> list[RunAction]:
        """bash 节点起进程（generator 与 task 两条派发入口共用）。"""
        self._nodes[node_id] = NodeActivity(
            node_id=node_id, phase=_PHASE_EXEC_PROC, attempt=attempt
        )
        actions.append(
            StartProcess(
                cmd=render(node.get("command", "true"), ctx),
                cwd=str(self._work_dir),
                context=f"node={node_id}",
                env=self._graph_env(),
            )
        )
        return actions

    def _dispatch_subgraph_read(
        self,
        node: dict[str, Any],
        ctx: dict[str, Any],
        act: NodeActivity,
        actions: list[RunAction],
    ) -> None:
        """读子图 yaml（ReadArtifact）：驱动文件读取的合法通道。"""
        subgraph_cfg = node.get("subgraph") or {}
        output_rel = render(subgraph_cfg.get("output", ""), ctx)
        if not output_rel:
            act.executor_error = "generator 节点缺少 subgraph.output"
            self._after_executor(act, actions)
            return
        if not self._state.subgraph_output(node["id"]):
            # 持久化实际解析的 output 路径，供恢复复用（与生成上下文一致）
            self._state.set_subgraph_output(node["id"], output_rel)
        actions.append(ReadArtifact(path=output_rel, context=f"node={node['id']}"))

    def _maybe_finish(self, world: RunWorld) -> list[RunAction]:
        """终局评估：传播失败跳过后按 graph 状态裁决 FinishRun。"""
        actions: list[RunAction] = []
        if self._nodes or self._group_paused or self._stopping:
            return actions
        while True:
            if scheduler.select_ready_nodes(self._dag, self._state):
                actions.extend(self._schedule(world))
                self._sync_graph_status(actions)
                return actions
            # 事件驱动循环不能等待下一轮空通知：同轮传播到稳定，
            # 才能让多级失败依赖后的 run_on_failure 节点出生。
            if not scheduler.propagate_failure_skips(self._dag, self._state):
                break
        self._sync_graph_status(actions)
        status = self._state.snapshot().get("status")
        if status == "failed":
            scheduler.skip_pending_on_failure(self._dag, self._state)
            actions.append(FinishRun(FinishOutcome.FAILED, summary="存在失败节点"))
        elif status == "completed":
            actions.append(FinishRun(FinishOutcome.COMPLETED))
        # paused/有 stopped 断点：不收口，等组恢复或用户控制
        return actions

    def _after_executor(self, act: NodeActivity, actions: list[RunAction]) -> None:
        """执行阶段收口：generator 先读子图，report 启用先落盘，再进验收。"""
        node = self._dag_node(act.node_id)
        if node is None:
            self._drop_activity(act)
            return
        if act.phase == _PHASE_SUB_READ:
            self._merge_or_verify(act, actions)
            return
        if node.get("task_type") == "generator" and not act.executor_error:
            act.phase = _PHASE_SUB_READ
            ctx = {
                **self._program.variables,
                "node_id": act.node_id,
                "attempt": str(act.attempt),
            }
            self._dispatch_subgraph_read(node, ctx, act, actions)
            return
        report_cfg = node.get("report") or {}
        if report_cfg.get("enabled"):
            act.phase = _PHASE_REPORT
            ctx = {
                **self._program.variables,
                "node_id": act.node_id,
                "attempt": str(act.attempt),
            }
            actions.append(
                StartProcess(
                    cmd=self._report_command(act, node, ctx),
                    cwd=str(self._work_dir),
                    context=f"node={act.node_id}/report",
                    stream=False,
                )
            )
            return
        self._enter_verify(act, actions)

    def _merge_or_verify(self, act: NodeActivity, actions: list[RunAction]) -> None:
        """子图读取后：通过硬校验则挂起待验收合并，随后进验收。"""
        if act.executor_error:
            self._enter_verify(act, actions)
            return
        if act.pending_subgraph:
            self._state.set_subgraph_generated(act.node_id, True)
        self._enter_verify(act, actions)

    def _enter_verify(self, act: NodeActivity, actions: list[RunAction]) -> None:
        """验收起步：执行错误直判、脚本起进程、agent 起验收成员。"""
        node = self._dag_node(act.node_id)
        acc = node.get("acceptance") or {}
        if act.executor_error:
            self._conclude_verify(act, False, act.executor_error, actions)
            return
        if not acc.get("enabled", False):
            self._merge_subgraph(act)
            self._finish_node(act, "passed", actions)
            return
        ctx = {
            **self._program.variables,
            "node_id": act.node_id,
            "attempt": str(act.attempt),
        }
        if acc.get("type", "script") == "agent":
            act.phase = _PHASE_VERIFY_SPAWN
            act.verify = {"verify_type": "agent"}
            actions.append(
                StartSession(
                    spec=self._member_spec(
                        name=f"graph:verify:{act.node_id}",
                        prompt=prompts.build_verifier_prompt(
                            node,
                            ctx,
                            workflow_system_prompt=self._program.config.get("system_prompt", ""),
                        ),
                        agent_name=acc.get("verifier", ""),
                        loop_max_iter=self._max_iterations or node.get("max_iterations", 10),
                        loop_evaluator=self._evaluator or "feedback",
                        node_id=act.node_id,
                        role="verifier",
                        verify=True,
                    ),
                    role="verifier",
                    context=f"node={act.node_id}/verify",
                )
            )
            return
        act.phase = _PHASE_VERIFY_PROC
        script = render(acc.get("script", "true"), ctx)
        act.verify = {"verify_type": "script", "script": script}
        self._arm_timer(act, _TIMER_VERIFY_SCRIPT, _VERIFY_SCRIPT_TIMEOUT_S, actions)
        actions.append(
            StartProcess(
                cmd=script,
                cwd=str(self._work_dir),
                context=f"node={act.node_id}/verify",
                env=self._graph_env(),
            )
        )

    def _judge_verify(self, act: NodeActivity, text: str, actions: list[RunAction]) -> None:
        """三级判定前两级在此；无结论起兜底 agent 分析全文（undetermined 按失败）。"""
        act.verify_text = text
        verdict = gates.judge_verify_output(text)
        if verdict is None:
            act.phase = _PHASE_VERDICT_SPAWN
            snippet = text[-4000:] if len(text) > 4000 else text
            prompt = (
                "下面是一个验证 agent 的完整输出（可能截断）。\n"
                "请判断最终验收结论是 PASS 还是 FAIL。\n"
                "只回复一个词：PASS 或 FAIL。\n\n"
                f"---\n{snippet}\n---"
            )
            actions.append(
                StartSession(
                    spec=self._member_spec(
                        name="graph:verdict-fallback",
                        prompt=prompt,
                        agent_name="",
                        loop_max_iter=1,
                        loop_evaluator="always_complete",
                        node_id=act.node_id,
                        role="verifier",
                        verify=False,
                    ),
                    role="verifier",
                    context=f"node={act.node_id}/verdict",
                )
            )
            return
        self._conclude_verify(
            act,
            verdict,
            "" if verdict else text[:MAX_FAILURE_REASON_CHARS] or "验收 agent 未输出 PASS",
            actions,
        )

    def _conclude_verify_from_verdict(
        self,
        act: NodeActivity,
        passed: bool,
        actions: list[RunAction],
    ) -> None:
        """兜底判定的收口：结论 False 时以验收输出尾部为失败原因。"""
        reason = (
            "" if passed else act.verify_text[:MAX_FAILURE_REASON_CHARS] or "验收 agent 未输出 PASS"
        )
        self._conclude_verify(act, passed, reason, actions)

    def _conclude_verify(
        self,
        act: NodeActivity,
        passed: bool,
        reason: str,
        actions: list[RunAction],
    ) -> None:
        """验收结论落账：通过合并子图，失败按 retry/continue/rollback/fail 去向。"""
        node = self._dag_node(act.node_id)
        acc = node.get("acceptance") or {}
        verify_extra = self._verify_extra_fields(act, reason)
        self._state.clear_verify_session_id(act.node_id)
        if passed:
            self._merge_subgraph(act)
            self._finish_node(act, "passed", actions, verify=verify_extra)
            self._last_verify_reason.pop(act.node_id, None)
            return
        # 验收失败：failure_reason 只记执行阶段的错误，验收失败原因归 verify_reason
        self._apply_verify_failure(act, acc, verify_extra, reason, actions)

    def _apply_verify_failure(
        self,
        act: NodeActivity,
        acc: Mapping[str, Any],
        verify_extra: dict[str, Any],
        reason: str,
        actions: list[RunAction],
    ) -> None:
        """验收失败去向：retry 全新重跑 / continue 跳过 / rollback 回退 / fail 终局。"""
        self._emit_node_history(
            actions,
            act.node_id,
            "failed",
            failure_reason=act.executor_error,
            verify=verify_extra,
        )
        self._last_verify_reason[act.node_id] = reason or "acceptance failed"
        rollback_to = acc.get("rollback_to", "") or ""
        verdict = gates.retry_or_exhaust(
            self._state.retries(act.node_id) + 1,
            int(acc.get("max_retries", 0) or 0),
            acc.get("on_exhaust", "stop"),
            rollback_to,
            self._state.rollback_left(
                act.node_id,
                rollback_to,
                self._program.rollback_budget,
            ),
        )
        self._apply_failure_verdict(act, verdict, rollback_to, verify_extra, reason, actions)

    def _apply_failure_verdict(
        self,
        act: NodeActivity,
        verdict: str,
        rollback_to: str,
        verify_extra: dict[str, Any],
        reason: str,
        actions: list[RunAction],
    ) -> None:
        """按判定迁移节点：retry 重跑 / continue 跳过 / rollback 回退 / fail 终局。"""
        if verdict == "retry":
            # 重试：清 session id，下一轮全新执行；本轮终态成员一并回收
            self._state.increment_retries(act.node_id)
            self._discard_finished_members(act, actions)
            self._state.clear_session_id(act.node_id)
            self._state.clear_failure_reason(act.node_id)
            self._set_node_status(actions, act.node_id, "pending")
            self._drop_activity(act)
            return
        if verdict == "continue":
            self._state.set_failure_reason(act.node_id, reason)
            self._set_node_status(actions, act.node_id, "skipped")
            self._sync_graph_status(actions)
            self._discard_finished_members(act, actions)
            self._drop_activity(act)
            return
        if verdict == "rollback":
            self._state.set_node_status(act.node_id, "failed")
            self._state.set_failure_reason(act.node_id, reason)
            self._apply_rollback(
                rollback_to,
                act.node_id,
                self._state.gen(act.node_id),
                actions,
            )
            self._drop_activity(act)
            return
        self._finish_node(act, "failed", actions, verify=verify_extra, reason=reason)

    def _finish_node(
        self,
        act: NodeActivity,
        status: str,
        actions: list[RunAction],
        verify: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> None:
        """节点终态收口：状态、档案事件与 graph 同步。"""
        self._cancel_timer(act, actions)
        self._state.set_node_status(act.node_id, status)
        if reason:
            self._state.set_failure_reason(act.node_id, reason)
        self._emit_node_history(actions, act.node_id, status, verify=verify)
        self._set_node_status(actions, act.node_id, status)
        self._sync_graph_status(actions)
        if status in ("passed", "failed"):
            self._discard_finished_members(act, actions)
        self._drop_activity(act)

    def _apply_rollback(
        self,
        target: str,
        failed_node: str,
        failed_gen: int,
        actions: list[RunAction],
    ) -> None:
        """回滚：reset 集落账并停掉仍在运行的并行成员（决策在 scheduler）。"""
        plan = scheduler.plan_rollback(
            self._state,
            self._dag,
            target,
            failed_node,
            failed_gen,
        )
        if plan is None:
            return
        snapshot = self._state.snapshot()
        stop_sids = []
        for nid in plan.stop_nodes:
            sid = snapshot["nodes"].get(nid, {}).get("workflow_controller_session_id")
            if sid:
                stop_sids.append(sid)
        if stop_sids:
            # 停掉 reset 集里仍在跑的成员（超时 0 = 立即停止语义）
            actions.append(AwaitSessions(ids=tuple(stop_sids), timeout_s=0))
        scheduler.apply_rollback(self._state, self._dag, plan)
        # 被 reset 的活动作废（节点回到 pending，等待重排）
        for nid in plan.reset_nodes:
            reset_act = self._nodes.pop(nid, None)
            if reset_act is not None:
                self._cancel_timer(reset_act, actions)
            self._last_verify_reason.pop(nid, None)
        self._emit_graph_event(
            actions,
            "node_rollback",
            node_id=failed_node,
            rollback_to=target,
        )
        self._set_graph_status(
            actions,
            scheduler.derive_status_after_rollback(self._state.snapshot()),
        )

    def _merge_subgraph(self, act: NodeActivity) -> None:
        """验收通过后把子图节点合并进父图 DAG/state。

        子图节点继承父 generator 当前的 gen（回滚后 generator 已提升到新代，
        子图节点须同代）；拓扑快照在合并前冻结，供归档/恢复还原历史子图。
        """
        if not act.pending_subgraph:
            return
        prefixed = act.pending_subgraph
        act.pending_subgraph = None
        parent_gen = self._state.gen(act.node_id)
        topology = subgraph_topology(prefixed)
        self._dag.add_nodes(prefixed, parent_node_id=act.node_id)
        for pn in prefixed:
            self._state.add_node(pn["id"], gen=parent_gen)
        self._state.set_subgraph_node_ids(
            act.node_id,
            [pn["id"] for pn in prefixed],
        )
        self._state.set_subgraph_nodes(act.node_id, topology)

    def _graph_env(self) -> dict[str, str]:
        if self._user_prompt_path is None:
            return {}
        return {
            "WORK_DIR": str(self._work_dir),
            "USER_PROMPT": str(self._user_prompt_path),
        }

    def _member_spec(
        self,
        *,
        name: str,
        prompt: str,
        agent_name: str,
        loop_max_iter: int,
        loop_evaluator: str,
        node_id: str,
        role: str,
        verify: bool,
    ) -> SessionSpec:
        """成员出生规格组装（提示词/agent 绑定/loop 配置/溯源与 reviewer）。"""
        from workflow_controller.config.settings import LoopConfig

        metadata: dict[str, dict[str, Any]] = {
            "graph": {"graph_id": self._graph_id, "node_id": node_id},
        }
        if verify:
            metadata["graph"]["verify"] = True
        config_overrides: dict[str, Any] = {"system_prompt": self._system_prompt or ""}
        if self._runtime:
            config_overrides["runtime"] = self._runtime
        if self._cli_wrapper is not None:
            config_overrides["cli_wrapper"] = self._cli_wrapper or ""
        config_overrides["model_pinned"] = self._model_pinned
        return SessionSpec(
            name=name,
            prompt=prompt,
            cwd=str(self._work_dir),
            agent_name=agent_name,
            model=self._model,
            provider_index=self._provider_index,
            env_overrides=self._graph_env(),
            loop=LoopConfig(max_iterations=int(loop_max_iter), evaluator=loop_evaluator),
            config_overrides=config_overrides,
            reviewer_profile=self._reviewer_profile_json(),
            plugin_metadata=metadata,
        )

    def _reviewer_profile_json(self) -> str | None:
        """graph 级 reviewer 配置（未显式配置时不挂 reviewer）。"""
        if self._reviewer_enabled is None:
            return None
        from workflow_controller.config.bootstrap import DEFAULT_TASK_GOAL

        payload: dict[str, Any] = {
            "enabled": bool(self._reviewer_enabled),
            "task_goal": self._reviewer_task_goal or DEFAULT_TASK_GOAL,
        }
        if self._reviewer_model:
            payload["model"] = self._reviewer_model
        if self._reviewer_provider_index is not None:
            payload["provider_index"] = self._reviewer_provider_index
        if self._reviewer_thinking_enabled is not None:
            payload["thinking_enabled"] = self._reviewer_thinking_enabled
        if self._reviewer_thinking_budget_tokens is not None:
            payload["thinking_budget_tokens"] = self._reviewer_thinking_budget_tokens
        if self._reviewer_max_tokens is not None:
            payload["max_tokens"] = self._reviewer_max_tokens
        return json.dumps(payload)

    def _report_command(self, act: NodeActivity, node: dict, ctx: dict) -> str:
        """报告落盘命令（heredoc 写文件——run 键进程是驱动的唯一写文件通道）。"""
        rel = prompts.report_save_path(node, ctx)
        sid: str | None = act.session_id if node.get("task_type") != "bash" else None
        executor_output = ""
        num_turns: int | None = None
        if node.get("task_type") == "agent":
            executor_output = act.result_digest or ""
            num_turns = act.result_iteration
        else:
            executor_output = (
                self._state.snapshot()["nodes"].get(act.node_id, {}).get("output") or ""
            )
        content = prompts.render_report(
            act.node_id,
            ctx.get("attempt", "0"),
            act.executor_error,
            executor_output,
            sid,
            num_turns,
        )
        # 哨兵行带节点名：内容撞上哨兵的概率可忽略
        sentinel = "WORKFLOW_CONTROLLER_GRAPH_REPORT_" + "".join(
            ch if ch.isalnum() else "_" for ch in act.node_id
        )
        parent = rel.rsplit("/", 1)[0] if "/" in rel else ""
        return (
            f"mkdir -p {shlex.quote(parent or '.')} && "
            f"cat > {shlex.quote(rel)} <<'{sentinel}'\n{content}\n{sentinel}\n"
        )

    def _agents_link_command(self) -> str:
        """cwd 下建 .opencode/agents 软链指向 yaml 的 agents_dir（幂等）。"""
        target = shlex.quote(str(self._program.agents_dir))
        return (
            "mkdir -p .opencode && "
            "if [ -e .opencode/agents ] || [ -L .opencode/agents ]; then "
            'if [ -L .opencode/agents ] && [ "$(readlink .opencode/agents)" != '
            f"{target} ]; then rm .opencode/agents; ln -s {target} .opencode/agents; fi; "
            f"else ln -s {target} .opencode/agents; fi"
        )

    def _reusable_session(self, existing_sid: str | None, world: RunWorld) -> str | None:
        """可复用成员判定：在池且运行中或暂停待续（resume_if_paused 的适用面）。"""
        if not existing_sid:
            return None
        info = self._world_session(world, existing_sid)
        if info is None:
            return None
        if info.state == "running" or (info.state == "idle" and info.stop_reason == "user_pause"):
            return existing_sid
        return None

    def _dag_node(self, node_id: str) -> dict | None:
        """DAG 节点安全查找（子图废弃等竞态下可能缺席）。"""
        try:
            return self._dag.get_node(node_id)
        except KeyError:
            return None

    def _activity_by_context(self, context: str | None) -> NodeActivity | None:
        """按 context 标签定位活动（node={id} 及其派生相位）。"""
        if not context or not context.startswith("node="):
            return None
        node_id = context.removeprefix("node=").split("/", 1)[0]
        return self._nodes.get(node_id)

    # ── 状态与事件 ──────────────────────────────────────────────

    def _activity_by_session(self, session_id: str) -> NodeActivity | None:
        """按成员 session 定位活动。"""
        for act in self._nodes.values():
            if act.session_id == session_id:
                return act
        return None

    def _node_bound_to_session(self, session_id: str) -> str | None:
        """session → node 定位：活动优先，回退节点档案绑定（一个 session 至多隶属一个 node）。"""
        act = self._activity_by_session(session_id)
        if act is not None:
            return act.node_id
        for nid, info in self._state.snapshot()["nodes"].items():
            if (
                info.get("workflow_controller_session_id") == session_id
                or info.get("verify_session_id") == session_id
            ):
                return nid
        return None

    def _reuse_activity(self, node_id: str, session_id: str) -> NodeActivity:
        """为 stopped 节点重建等待活动（组恢复/成员恢复的续跑面）。"""
        act = self._nodes.get(node_id)
        if act is not None:
            return act
        verify = self._state.verify_session_id(node_id) == session_id
        act = NodeActivity(
            node_id=node_id,
            phase=_PHASE_VERIFY_PAUSED_WAIT if verify else _PHASE_PAUSED_WAIT,
            attempt=self._state.attempts(node_id),
            session_id=session_id,
            verify_sid=session_id if verify else None,
            # 复用路径保持原超时语义：不续跑，超时即失败
            max_continues=0,
        )
        self._nodes[node_id] = act
        return act

    def _resume_model_spec(self, node_id: str) -> SessionSpec:
        return SessionSpec(
            name=f"graph:{node_id}",
            prompt="",
            cwd=str(self._work_dir),
            model=self._model,
            provider_index=self._provider_index,
            config_overrides={
                "model_pinned": self._model_pinned,
            }
            if self._model
            else {},
        )

    def _resume_member_action(self, act: NodeActivity, session_id: str) -> RestartSession:
        """既有成员的续跑重启动作（保留模型，暂停中的先续跑）。"""
        return RestartSession(
            session_id=session_id,
            policy=RestartPolicy.RESUME_IF_PAUSED,
            spec_override=self._resume_model_spec(act.node_id),
            context_override=self._restart_context(act),
        )

    def _arm_member_timer(self, act: NodeActivity, actions: list[RunAction]) -> None:
        """按节点/验收配置挂成员超时定时器。"""
        node = self._dag_node(act.node_id)
        timeout: float | None = 7200.0
        if node is not None:
            if act.phase == _PHASE_VERIFY_RUNNING:
                timeout = _resolve_timeout((node.get("acceptance") or {}).get("timeout"))
            else:
                timeout = _resolve_timeout(node.get("timeout"))
        if timeout is not None:
            self._arm_timer(act, _TIMER_MEMBER, timeout, actions)

    def _arm_timer(
        self,
        act: NodeActivity,
        kind: str,
        delay_s: float,
        actions: list[RunAction],
    ) -> None:
        """挂定时器并记录用途与时长（超时反馈文案用；id 带序号一次一换）。"""
        self._timer_seq += 1
        act.timer_id = f"t:{act.node_id}:{self._timer_seq}"
        act.timer_kind = kind
        act.timer_delay_s = delay_s
        actions.append(ArmTimer(timer_id=act.timer_id, delay_s=delay_s))

    def _reset_node_for_rerun(
        self,
        node_id: str,
        *,
        keep_attempts: bool = False,
    ) -> None:
        """节点按全新执行重排；恢复中失效成员的已消耗次数仍需保留。

        ``attempts``（展示/历史计数）按 keep_attempts 决定是否保留；
        ``retries``（本续跑周期的重试预算）无论是否保留 attempt 都清零，
        让每次重跑拿到完整重试预算，同时上一次的失败原因随之失效（否则
        重跑的首个 attempt 会误注入「第 N 次尝试」的重试上下文）。
        """
        self._state.set_node_status(node_id, "pending")
        self._state.clear_failure_reason(node_id)
        self._state.clear_session_id(node_id)
        self._state.clear_verify_session_id(node_id)
        self._state.reset_retries(node_id)
        self._last_verify_reason.pop(node_id, None)
        if not keep_attempts:
            self._state.reset_attempts(node_id)

    # ── 只读面（plugin 消费）────────────────────────────────────

    def _drop_activity(self, act: NodeActivity) -> None:
        """活动收口出表。"""
        if self._nodes.get(act.node_id) is act:
            self._nodes.pop(act.node_id, None)

    def _discard_finished_members(
        self,
        act: NodeActivity,
        actions: list[RunAction],
    ) -> None:
        """释放节点收口成员的内存副本：执行/验收/兜底 session 一并回收。

        只清内存池，持久行与 Run 归属保留（历史详情由存储按需恢复）；
        宿主侧仅终态成员实际生效。
        """
        info = self._state.snapshot()["nodes"].get(act.node_id) or {}
        sids: list[str] = []
        for sid in (info.get("workflow_controller_session_id"), act.verify_sid, act.session_id):
            if sid and sid not in sids:
                sids.append(sid)
        for sid in sids:
            actions.append(DiscardSession(session_id=sid))

    def _set_node_status(
        self,
        actions: list[RunAction],
        node_id: str,
        status: str,
    ) -> None:
        """节点状态变更 + node_state 事件。"""
        self._state.set_node_status(node_id, status)
        self._emit_graph_event(actions, "node_state", node_id=node_id, status=status)

    def _set_graph_status(self, actions: list[RunAction], status: str) -> None:
        """graph 状态有变才写 + graph_{status} 事件。"""
        if status != self._state.snapshot().get("status"):
            self._state.set_graph_status(status)
            extra: dict[str, Any] = {}
            if status == "failed":
                extra["failed"] = [
                    nid for nid in self._dag.all_ids() if self._state.status(nid) == "failed"
                ]
            self._emit_graph_event(actions, f"graph_{status}", **extra)

    def _sync_graph_status(self, actions: list[RunAction]) -> None:
        """按全体节点状态推导并同步 graph 状态（推导在 scheduler）。"""
        status = scheduler.derive_graph_status(self._state.snapshot())
        if self._group_paused and status == "pending":
            status = "paused"
        self._set_graph_status(actions, status)

    def _emit_graph_event(
        self,
        actions: list[RunAction],
        event_name: str,
        **extra: Any,
    ) -> None:
        """图事件动作 + 投影变更信号。"""
        actions.append(
            EmitEvent(
                event_type=event_name,
                data={"graph_id": self._graph_id, **extra},
            )
        )
        self._emit_projection_updated(actions)

    def _emit_projection_updated(self, actions: list[RunAction]) -> None:
        """投影变更信号动作（消费端按需拉取详情）。"""
        actions.append(
            EmitEvent(
                event_type="projection_updated",
                data={"graph_id": self._graph_id},
            )
        )

    def _emit_node_history(
        self,
        actions: list[RunAction],
        node_id: str,
        status: str,
        *,
        attempt: int | None = None,
        failure_reason: str | None = None,
        verify: dict[str, Any] | None = None,
    ) -> None:
        """节点尝试档案事件动作（run_events 回放面）。"""
        info = self._state.snapshot()["nodes"].get(node_id, {})
        actions.append(
            EmitEvent(
                event_type="node_history",
                data={
                    "graph_id": self._graph_id,
                    "node_id": node_id,
                    "attempt": attempt if attempt is not None else info.get("attempts", 0),
                    "status": status,
                    "workflow_controller_session_id": info.get("workflow_controller_session_id"),
                    "failure_reason": failure_reason,
                    "output": info.get("output"),
                    "started_at": info.get("started_at"),
                    "finished_at": info.get("finished_at"),
                    "verify_type": (verify or {}).get("verify_type"),
                    "verify_session_id": (verify or {}).get("verify_session_id"),
                    "verify_reason": (verify or {}).get("verify_reason"),
                    "gen": info.get("gen", 0),
                },
            )
        )

    def _emit_verify_running_history(
        self,
        actions: list[RunAction],
        act: NodeActivity,
    ) -> None:
        """验收进行中的执行记录：前端节点历史立即出现验收会话入口。"""
        self._emit_node_history(
            actions,
            act.node_id,
            "running",
            attempt=act.attempt,
            verify={
                "verify_type": "agent",
                "verify_session_id": act.session_id,
                "verify_reason": "验收中…",
            },
        )
