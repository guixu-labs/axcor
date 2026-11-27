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

"""正规驱动的 decide 步进测试：通知驱动阶段机，无 asyncio fixture（纯决策）。

bash 节点、agent 成员、验收、回滚、子图、组控与终态裁决全部以动作产出/
通知回投的方式断言——执行面缺席时以合成通知推进，驱动永不感知宿主。
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
import yaml

from workflow_controller.config.settings import SessionReviewerConfig
from workflow_controller.core.runs import RunRecord
from workflow_controller.plugin.run_driver import (
    ArmTimer,
    AwaitSessions,
    CancelTimer,
    DiscardSession,
    EmitEvent,
    FinishRun,
    PersistDriverState,
    ProcessExit,
    ProcessOutput,
    ReadArtifact,
    ReplaySessionOutcome,
    RestartPolicy,
    RestartSession,
    RunAction,
    RunSessionInfo,
    RunWorld,
    SessionActivated,
    SessionControlApplied,
    SessionOutcomeNotification,
    SessionOutcomeReplayed,
    StartProcess,
    StartSession,
    TimerFired,
)
from workflow_controller.plugin.run_driver import (  # 进程/回执构造 helper：proc_exit/activation
    KillProcesses as _KillProcesses,
)
from workflow_controller.plugins.graph import translator
from workflow_controller.plugins.graph.driver.engine import GraphDriver, NodeActivity
from workflow_controller.plugins.graph.driver.state import GraphState
from workflow_controller.session.events import SessionOutcome

if TYPE_CHECKING:
    from pathlib import Path

_RUN = RunRecord(run_id="r1", program_ref="/tmp/wf.yaml", format="graph-yaml@1")


def build_driver(tmp_path: Path, nodes: list[dict], **cfg: object) -> GraphDriver:
    """从节点清单构造驱动（yaml 落盘后经翻译器走生产构造路径）。"""
    cfg.setdefault("workflow", "wf")
    cfg.setdefault("max_parallel", 2)
    yaml_path = tmp_path / "wf.yaml"
    yaml_path.write_text(yaml.safe_dump({**cfg, "nodes": nodes}), encoding="utf-8")
    program = translator.translate_program(yaml_path, {}, None)
    state = GraphState(
        graph_id="g1",
        workflow=program.workflow,
        node_ids=program.dag.all_ids(),
    )
    return GraphDriver(
        graph_id="g1",
        program=program,
        state=state,
        work_dir=tmp_path,
        yaml_path=yaml_path,
        options={},
    )


def decide(
    driver: GraphDriver,
    notes: tuple[RunAction, ...] = (),
    sessions: tuple[RunSessionInfo, ...] = (),
) -> list[RunAction]:
    """驱动一轮决策：合成 world + 通知。"""
    world = RunWorld(run=_RUN, sessions=sessions)
    return driver.decide(world, list(notes))


def outcome(sid: str, kind: str, digest: str | None = None) -> SessionOutcomeNotification:
    """成员结局通知。"""
    return SessionOutcomeNotification(
        outcome=SessionOutcome(
            session_id=sid,
            kind=kind,
            stop_reason=None,
            last_error=None,
            result_digest=digest,
            iteration=1,
            context=None,
        )
    )


def member(sid: str, state: str = "running", stop_reason: str | None = None) -> RunSessionInfo:
    """world 成员事实。"""
    return RunSessionInfo(
        session_id=sid,
        role="executor",
        context=None,
        state=state,
        stop_reason=stop_reason,
    )


def pick(actions: list[RunAction], kind: type) -> list:
    return [a for a in actions if isinstance(a, kind)]


def first(actions: list[RunAction], kind: type) -> object:
    found = pick(actions, kind)
    assert found, f"动作里没有 {kind.__name__}: {actions}"
    return found[0]


def start_process_for(actions: list[RunAction], context: str) -> StartProcess:
    for a in actions:
        if isinstance(a, StartProcess) and a.context == context:
            return a
    raise AssertionError(f"缺少进程动作 {context}: {actions}")


def node_status(driver: GraphDriver, nid: str) -> str:
    return driver.state.snapshot()["nodes"][nid]["status"]


def bash_node(nid: str, command: str, **extra: object) -> dict:
    """bash 节点清单项（extra 并入 acceptance/depends_on/report 等）。"""
    return {"id": nid, "task_type": "bash", "command": command, **extra}


def reported_bash_node() -> dict:
    """report 相位用例的 bash 节点：先写报告，再过 script 验收。"""
    return bash_node(
        "a",
        "echo hi",
        report={"enabled": True, "save_to": "reports/a-{{attempt}}.md"},
        acceptance={"enabled": True, "type": "script", "script": "true"},
    )


def agent_node(nid: str, **extra: object) -> dict:
    """agent 节点清单项（extra 并入 acceptance/timeout/executor 等）。"""
    return {"id": nid, "task_type": "agent", **extra}


def proc_exit(
    context: str, returncode: int, proc_id: str = "p1", exit_mark: str | None = None
) -> ProcessExit:
    """进程退出通知。"""
    return ProcessExit(proc_id=proc_id, context=context, returncode=returncode, exit_mark=exit_mark)


def activation(
    sid: str, context: str, *, ok: bool = True, error: str | None = None
) -> SessionActivated:
    """成员出生回执。"""
    return SessionActivated(session_id=sid, context=context, ok=ok, error=error)


def assert_restart_receipt_roundtrip(driver: GraphDriver, actions: list, sid: str) -> None:
    """断言 restart 回执按派发 context 对齐：消费回执换相并重挂预算，随后终局收尾。"""
    restart = first(actions, RestartSession)
    assert restart.context_override == "node=w"
    reactivated = decide(driver, [activation(sid, restart.context_override)])
    assert first(reactivated, ArmTimer)  # 回执消费换相并重挂预算
    final_actions = decide(driver, [outcome(sid, "completed")])
    assert first(final_actions, FinishRun).outcome.value == "completed"


def cold_reload(driver: GraphDriver, monkeypatch) -> None:
    """按当前快照重建状态面（模拟重启后的装配路径）。"""
    snap = driver.state.snapshot()
    monkeypatch.setattr(
        driver,
        "_state",
        GraphState("g1", "wf", set(snap["nodes"]), initial_data=snap),
    )


def test_changed_state_is_persisted_before_start_action(tmp_path: Path):
    driver = build_driver(tmp_path, [{"id": "a", "task_type": "bash", "command": "true"}])
    actions = decide(driver)
    assert isinstance(actions[0], PersistDriverState)
    assert actions[0].blob["state"]["nodes"]["a"]["status"] == "running"
    assert any(isinstance(action, StartProcess) for action in actions[1:])


def test_checkpoint_survives_abort_before_decide_returns(tmp_path: Path):
    driver = build_driver(tmp_path, [{"id": "a", "task_type": "bash", "command": "true"}])
    checkpoint_file = tmp_path / "checkpoint.json"

    def save(action: PersistDriverState) -> None:
        blob = action.blob
        checkpoint_file.write_text(json.dumps(blob), encoding="utf-8")
        if blob["state"]["nodes"]["a"]["status"] == "running":
            raise RuntimeError("模拟决策尚未返回时进程退出")

    driver.bind_checkpoint(save)
    with pytest.raises(RuntimeError, match="决策尚未返回"):
        decide(driver)

    persisted = json.loads(checkpoint_file.read_text(encoding="utf-8"))
    assert persisted["state"]["nodes"]["a"]["status"] == "running"
    restored = GraphState("g1", "wf", {"a"}, initial_data=persisted["state"])
    assert restored.status("a") == "stopped"
    assert restored.interrupted_nodes == {"a"}


def test_verifier_feedback_keeps_long_failure_reason(tmp_path: Path):
    """验收 agent 的长反馈在直接判定与兜底收口时都保留 5000 字。"""
    node = bash_node("a", "true", acceptance={"enabled": True, "type": "agent", "max_retries": 0})
    long_text = "x" * 6000

    direct_dir = tmp_path / "direct"
    direct_dir.mkdir()
    direct = build_driver(direct_dir, [node])
    getattr(direct, "_judge_verify")(  # exercise the driver verdict boundary
        NodeActivity(node_id="a", phase="verify_running", attempt=1),
        long_text + "\nFAILED",
        [],
    )
    assert len(direct.state.snapshot()["nodes"]["a"]["failure_reason"]) == 5000

    fallback_dir = tmp_path / "fallback"
    fallback_dir.mkdir()
    fallback = build_driver(fallback_dir, [node])
    activity = NodeActivity(
        node_id="a",
        phase="verify_running",
        attempt=1,
        verify_text=long_text,
    )
    getattr(fallback, "_conclude_verify_from_verdict")(
        activity,
        False,
        [],
    )
    assert len(fallback.state.snapshot()["nodes"]["a"]["failure_reason"]) == 5000


def test_decide_coalesces_round_mutations_into_one_checkpoint(tmp_path: Path):
    """decide 轮内的多次字段变更合并为轮末一次 checkpoint（评审修订）。"""
    driver = build_driver(
        tmp_path,
        [
            {"id": "a", "task_type": "agent"},
            {"id": "b", "task_type": "agent"},
        ],
    )
    commits: list[dict] = []
    driver.bind_checkpoint(lambda action: commits.append(action.blob))

    actions = decide(driver)

    assert len(pick(actions, StartSession)) == 2  # 两节点并行启动
    assert len(commits) == 1
    info = commits[0]["state"]["nodes"]["a"]
    assert info["status"] == "running" and info["attempts"] == 1


def test_detail_reconcile_only_stops_member_missing_from_pool(tmp_path: Path):
    driver = build_driver(tmp_path, [{"id": "a", "task_type": "agent"}])
    decide(driver)
    decide(driver, [SessionActivated(session_id="exec-1", context="node=a", ok=True)])

    paused_world = RunWorld(run=_RUN, sessions=(member("exec-1", "idle", "user_pause"),))
    paused_detail, changed, paused_events = driver.reconcile_projection(paused_world)
    assert paused_detail["nodes"][0]["status"] == "running"
    assert not changed
    assert paused_events == []

    missing_world = RunWorld(run=_RUN, sessions=(member("exec-1", ""),))
    detail, changed, events = driver.reconcile_projection(missing_world)
    assert detail["nodes"][0]["status"] == "stopped"
    assert changed
    assert detail["status"] == "paused"
    # 在途定时器随活动一并作废，不留 armed 的孤儿定时器
    assert pick(events, CancelTimer), events
    # 读修复的状态迁移在事件流可见，不静默
    types = [e.event_type for e in pick(events, EmitEvent)]
    assert "node_state" in types
    assert "node_history" in types
    assert "graph_paused" in types
    assert "projection_updated" in types


def test_orchestrator_yaml_fields_reach_the_correct_sessions(tmp_path: Path):
    driver = build_driver(
        tmp_path,
        [
            {
                "id": "a",
                "task_type": "normal",
                "title": "Deliver the report",
                "goal": ["REQUEST_PATH=/tmp/report.json"],
                "approach": ["Write the report in sections"],
                "acceptance": ["The report exists and is readable"],
                "procedure": ["Run check-report /tmp/report.json"],
                "out_of_scope": ["Do not edit the source data"],
                "executor": "writer",
                "verifier": "checker",
            },
        ],
        system_prompt="Check the project reference when needed",
    )
    executor = first(decide(driver), StartSession).spec
    decide(driver, [activation("exec-1", "node=a")])
    verifier = first(decide(driver, [outcome("exec-1", "completed")]), StartSession).spec

    for spec in (executor, verifier):
        assert "$SYSTEM_PROMPT=Check the project reference when needed" in spec.prompt
        assert "REQUEST_PATH=/tmp/report.json" in spec.prompt
        assert "The report exists and is readable" in spec.prompt
        assert "Do not edit the source data" in spec.prompt
    assert executor.agent_name == "writer"
    assert "Write the report in sections" in executor.prompt
    assert "Run check-report" not in executor.prompt
    assert verifier.agent_name == "checker"
    assert "Run check-report /tmp/report.json" in verifier.prompt
    assert "Write the report in sections" not in verifier.prompt


def test_finished_node_discards_all_its_members(tmp_path: Path):
    driver = build_driver(
        tmp_path,
        [agent_node("a", acceptance={"enabled": True, "type": "agent"})],
    )
    decide(driver)
    decide(driver, [SessionActivated(session_id="exec-1", context="node=a", ok=True)])
    verifying = decide(driver, [outcome("exec-1", "completed")])
    assert not pick(verifying, DiscardSession)

    decide(
        driver,
        [activation("verify-1", "node=a/verify")],
    )
    finished = decide(driver, [outcome("verify-1", "completed", digest="PASS")])
    assert [action.session_id for action in pick(finished, DiscardSession)] == [
        "exec-1",
        "verify-1",
    ]
    assert isinstance(finished[0], PersistDriverState)


def test_verdict_fallback_finish_discards_verify_and_verdict(tmp_path: Path):
    driver = build_driver(
        tmp_path,
        [agent_node("a", acceptance={"enabled": True, "type": "agent"})],
    )
    decide(driver)
    decide(driver, [SessionActivated(session_id="exec-1", context="node=a", ok=True)])
    decide(driver, [outcome("exec-1", "completed")])
    decide(driver, [SessionActivated(session_id="verify-1", context="node=a/verify", ok=True)])
    # 验收输出无结论 → 起兜底 agent
    decide(driver, [outcome("verify-1", "completed", digest="今天天气不错")])
    decide(driver, [SessionActivated(session_id="verdict-1", context="node=a/verdict", ok=True)])
    finished = decide(driver, [outcome("verdict-1", "completed", digest="PASS")])
    assert [action.session_id for action in pick(finished, DiscardSession)] == [
        "exec-1",
        "verify-1",
        "verdict-1",
    ]


def test_verify_failure_retry_discards_previous_members(tmp_path: Path):
    driver = build_driver(
        tmp_path,
        [agent_node("a", acceptance={"enabled": True, "type": "agent", "max_retries": 1})],
    )
    decide(driver)
    decide(driver, [SessionActivated(session_id="exec-1", context="node=a", ok=True)])
    decide(driver, [outcome("exec-1", "completed")])
    decide(driver, [SessionActivated(session_id="verify-1", context="node=a/verify", ok=True)])
    retried = decide(driver, [outcome("verify-1", "completed", digest="FAILED: x")])
    assert [action.session_id for action in pick(retried, DiscardSession)] == [
        "exec-1",
        "verify-1",
    ]
    # 重排即重派（同轮调度）：新一次执行起步，节点回 running
    assert first(retried, StartSession).context == "node=a"
    assert node_status(driver, "a") == "running"


def test_prep_round_keeps_running_status_until_first_node(tmp_path: Path):
    """agents 软链预备轮未启动任何节点：图保持 running，不回吐 graph_pending。"""
    (tmp_path / "agents").mkdir()
    driver = build_driver(
        tmp_path,
        [agent_node("a")],
        agents_dir="agents",
    )
    first_actions = decide(driver)
    assert any(
        isinstance(action, EmitEvent) and action.event_type == "graph_started"
        for action in first_actions
    )
    start_process_for(first_actions, "prep:agents")
    assert not any(
        isinstance(action, EmitEvent) and action.event_type == "graph_pending"
        for action in first_actions
    )
    assert graph_status(driver) == "running"
    ready = decide(
        driver,
        [proc_exit("prep:agents", 0, proc_id="p1")],
    )
    assert any(isinstance(action, StartSession) for action in ready)
    assert graph_status(driver) == "running"


def test_paused_node_does_not_discard_resumable_session(tmp_path: Path):
    driver = build_driver(tmp_path, [{"id": "a", "task_type": "agent"}])
    decide(driver)
    decide(driver, [SessionActivated(session_id="exec-1", context="node=a", ok=True)])
    paused = decide(
        driver,
        [outcome("exec-1", "paused")],
        sessions=(member("exec-1", "idle", "user_pause"),),
    )
    assert not pick(paused, DiscardSession)


def graph_status(driver: GraphDriver) -> str:
    return driver.state.snapshot()["status"]


def session_for(actions: list[RunAction], context: str) -> StartSession:
    for a in actions:
        if isinstance(a, StartSession) and a.context == context:
            return a
    raise AssertionError(f"缺少成员出生动作 {context}: {actions}")


# ── bash 节点执行与验收 ────────────────────────────────────────────


class TestBashNode:
    @staticmethod
    def test_failed_node_does_not_hide_parallel_running_node(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                bash_node("a", "false", acceptance={"enabled": False}),
                bash_node("b", "sleep 10", acceptance={"enabled": False}),
            ],
        )
        assert len(pick(decide(driver), StartProcess)) == 2

        actions = decide(
            driver,
            [proc_exit("node=a", 1, proc_id="p1")],
        )

        assert node_status(driver, "a") == "failed"
        assert node_status(driver, "b") == "running"
        assert graph_status(driver) == "running"
        assert not pick(actions, FinishRun)

    @staticmethod
    def test_pass_through_script_acceptance(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                bash_node(
                    "a", "true", acceptance={"enabled": True, "type": "script", "script": "true"}
                )
            ],
        )
        actions = decide(driver)
        assert "node=a" in str(first(actions, StartProcess).context)
        actions = decide(
            driver,
            [proc_exit("node=a", 0, proc_id="p1")],
        )
        assert "node=a/verify" in str(first(actions, StartProcess).context)
        assert first(actions, ArmTimer)  # 验收脚本有超时守护
        actions = decide(
            driver,
            [proc_exit("node=a/verify", 0, proc_id="p2")],
        )
        assert isinstance(first(actions, FinishRun), FinishRun)
        assert node_status(driver, "a") == "passed"
        assert graph_status(driver) == "completed"

    @staticmethod
    def test_bash_failure_reason_carried_to_verify(tmp_path):
        driver = build_driver(tmp_path, [bash_node("a", "x", acceptance={"enabled": False})])
        decide(driver)
        actions = decide(
            driver,
            [proc_exit("node=a", 3, proc_id="p1")],
        )
        finish = first(actions, FinishRun)
        assert finish.outcome.value == "failed"
        reason = driver.state.snapshot()["nodes"]["a"]["failure_reason"]
        assert "bash exit 3" in reason

    @staticmethod
    def test_external_signal_death_is_failure(tmp_path):
        driver = build_driver(tmp_path, [bash_node("a", "x", acceptance={"enabled": False})])
        decide(driver)
        actions = decide(
            driver,
            [proc_exit("node=a", -9, proc_id="p1")],
        )
        assert first(actions, FinishRun).outcome.value == "failed"
        assert "外部信号" in driver.state.snapshot()["nodes"]["a"]["failure_reason"]

    @staticmethod
    def test_spawn_failure_is_failure(tmp_path):
        driver = build_driver(tmp_path, [bash_node("a", "x", acceptance={"enabled": False})])
        decide(driver)
        actions = decide(
            driver,
            [proc_exit("node=a", -1, proc_id="p1", exit_mark="spawn_failed")],
        )
        assert first(actions, FinishRun).outcome.value == "failed"

    @staticmethod
    def test_plane_kill_during_stop_keeps_breakpoint(tmp_path):
        driver = build_driver(tmp_path, [bash_node("a", "x", acceptance={"enabled": False})])
        decide(driver)
        driver.on_control(_stopped_control())
        actions = decide(
            driver,
            [proc_exit("node=a", -15, proc_id="p1", exit_mark="plane_killed")],
        )
        assert not pick(actions, FinishRun)  # 终态裁决权归控制侧
        assert node_status(driver, "a") == "stopped"
        assert graph_status(driver) == "paused"

    @staticmethod
    def test_late_plane_kill_after_resume_reschedules(tmp_path):
        """恢复后才到达的暂停期组杀：工作未完成，节点重排重跑。"""
        driver = build_driver(tmp_path, [bash_node("a", "x", acceptance={"enabled": False})])
        decide(driver)
        driver.on_control(_paused_control())
        driver.on_control(_resumed_control())
        actions = decide(
            driver,
            [proc_exit("node=a", -15, proc_id="p1", exit_mark="plane_killed")],
        )
        # 重排即重派（同轮调度），attempts 归零后重新计数
        assert first(actions, StartProcess).context == "node=a"
        assert node_status(driver, "a") == "running"
        assert driver.state.snapshot()["nodes"]["a"]["attempts"] == 1

    @staticmethod
    def test_verify_script_failure_feeds_retry_context(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                bash_node(
                    "a",
                    "x",
                    acceptance={
                        "enabled": True,
                        "type": "script",
                        "script": "grep -q done out.txt",
                        "max_retries": 1,
                        "on_exhaust": "stop",
                    },
                )
            ],
        )
        decide(driver)
        decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        # 第一次验收失败 → retry（同轮重排重跑，attempts 递增）
        actions = decide(
            driver,
            [proc_exit("node=a/verify", 1, proc_id="p2")],
        )
        assert first(actions, StartProcess).context == "node=a"
        assert driver.state.snapshot()["nodes"]["a"]["attempts"] == 2
        # 第二次验收失败 → 耗尽 → failed
        decide(driver, [ProcessExit(proc_id="p3", context="node=a", returncode=0)])
        actions = decide(
            driver,
            [proc_exit("node=a/verify", 1, proc_id="p4")],
        )
        assert first(actions, FinishRun).outcome.value == "failed"
        assert node_status(driver, "a") == "failed"

    @staticmethod
    def test_verify_script_timeout_kills_and_fails(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                bash_node(
                    "a",
                    "true",
                    acceptance={"enabled": True, "type": "script", "script": "sleep 301"},
                )
            ],
        )
        decide(driver)
        verified = decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        actions = decide(
            driver,
            [TimerFired(timer_id=first(verified, ArmTimer).timer_id)],
        )
        assert first(actions, _KillProcesses).context == "node=a/verify"
        actions = decide(
            driver,
            [proc_exit("node=a/verify", -15, proc_id="p2", exit_mark="plane_killed")],
        )
        assert first(actions, FinishRun).outcome.value == "failed"
        reason = driver.state.snapshot()["nodes"]["a"]["failure_reason"]
        assert "验收脚本超时" in reason

    @staticmethod
    def test_verify_script_plane_kill_marks_stopped(tmp_path):
        """组暂停终止验收脚本：不算节点失败，保留断点。"""
        driver = build_driver(
            tmp_path,
            [
                bash_node(
                    "a",
                    "true",
                    acceptance={"enabled": True, "type": "script", "script": "sleep 30"},
                )
            ],
        )
        decide(driver)
        decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        driver.on_control(_paused_control())
        actions = decide(
            driver,
            [proc_exit("node=a/verify", -15, proc_id="p2", exit_mark="plane_killed")],
        )
        assert not pick(actions, FinishRun)
        assert node_status(driver, "a") == "stopped"
        assert graph_status(driver) == "paused"

    @staticmethod
    def test_verify_output_streamed_into_reason_tail(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                bash_node(
                    "a",
                    "true",
                    acceptance={"enabled": True, "type": "script", "script": "false"},
                )
            ],
        )
        decide(driver)
        decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        decide(driver, [ProcessOutput(proc_id="p2", context="node=a/verify", line="boom")])
        actions = decide(
            driver,
            [proc_exit("node=a/verify", 1, proc_id="p2")],
        )
        assert first(actions, FinishRun).outcome.value == "failed"
        reason = driver.state.snapshot()["nodes"]["a"]["failure_reason"]
        assert "boom" in reason and "false" in reason

    @staticmethod
    def test_report_written_before_verify(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                reported_bash_node(),
            ],
        )
        decide(driver)
        decide(driver, [ProcessOutput(proc_id="p1", context="node=a", line="hi")])
        report = first(
            decide(
                driver,
                [proc_exit("node=a", 0, proc_id="p1")],
            ),
            StartProcess,
        )
        assert report.context == "node=a/report"
        assert "reports/a-1.md" in report.cmd
        actions = decide(driver, [ProcessExit(proc_id="p9", context="node=a/report", returncode=0)])
        assert first(actions, StartProcess).context == "node=a/verify"

    @staticmethod
    def test_report_plane_kill_during_stop_skips_verify(tmp_path):
        """stop 组杀落在 report 相位：不进验收，节点落 stopped 断点。"""
        driver = build_driver(
            tmp_path,
            [
                reported_bash_node(),
            ],
        )
        decide(driver)
        decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        driver.on_control(_stopped_control())
        actions = decide(
            driver,
            [proc_exit("node=a/report", -15, proc_id="p9", exit_mark="plane_killed")],
        )
        assert not pick(actions, StartProcess)  # 已 stop 的 run 不再执行验收脚本
        assert node_status(driver, "a") == "stopped"
        assert graph_status(driver) == "paused"


# ── agent 成员执行 ────────────────────────────────────────────────


class TestAgentNode:
    @staticmethod
    def test_member_lifecycle_and_spec(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                agent_node(
                    "w",
                    executor="worker",
                    task_description="干活",
                    acceptance={"enabled": False},
                )
            ],
        )
        spawn = first(decide(driver), StartSession)
        assert spawn.context == "node=w"
        assert spawn.role == "executor"
        assert spawn.spec.name == "graph:w"
        assert spawn.spec.agent_name == "worker"
        assert spawn.spec.plugin_metadata == {
            "graph": {
                "graph_id": "g1",
                "node_id": "w",
            }
        }
        # 激活回执 → 运行等待 + 超时定时器 + running 档案
        actions = decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        assert first(actions, ArmTimer)
        assert driver.state.snapshot()["nodes"]["w"]["workflow_controller_session_id"] == "s1"
        # 结局 → 直接过验收（无验收）
        actions = decide(driver, [outcome("s1", "completed", digest="DONE")])
        assert first(actions, FinishRun).outcome.value == "completed"
        assert node_status(driver, "w") == "passed"

    @staticmethod
    def test_member_spawn_failure_fails_node(tmp_path):
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        actions = decide(
            driver,
            [activation("graph:w", "node=w", ok=False, error="ValueError: bad")],
        )
        assert first(actions, FinishRun).outcome.value == "failed"
        assert "出生失败" in driver.state.snapshot()["nodes"]["w"]["failure_reason"]

    @staticmethod
    def test_member_paused_waits_then_resumes(tmp_path):
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        actions = decide(
            driver, [outcome("s1", "paused")], sessions=(member("s1", "idle", "user_pause"),)
        )
        assert not pick(actions, FinishRun)
        assert node_status(driver, "w") == "stopped"
        assert graph_status(driver) == "paused"
        # 成员恢复（通知 + world 已 running）→ 重新等待；正常结束 → 完成
        actions = decide(
            driver,
            [SessionControlApplied(session_id="s1", operation="resume")],
            sessions=(member("s1"),),
        )
        assert first(actions, ArmTimer)
        actions = decide(driver, [outcome("s1", "completed")])
        assert first(actions, FinishRun).outcome.value == "completed"

    @staticmethod
    def test_stale_paused_outcome_ignored_when_member_running(tmp_path):
        """组恢复后被超越的暂停结局：以 world 为准，不当真暂停。"""
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        driver.on_control(_resumed_control())
        actions = decide(driver, [outcome("s1", "paused")], sessions=(member("s1"),))
        assert node_status(driver, "w") == "running"
        actions = decide(driver, [outcome("s1", "completed")])
        assert first(actions, FinishRun).outcome.value == "completed"

    @staticmethod
    def test_member_stopped_keeps_breakpoint(tmp_path):
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        actions = decide(driver, [outcome("s1", "stopped")])
        assert not pick(actions, FinishRun)
        assert node_status(driver, "w") == "stopped"
        assert graph_status(driver) == "paused"

    @staticmethod
    def test_member_resume_with_dead_member_reruns_node(tmp_path):
        """成员消亡后的成员级恢复：节点落 pending 重排，不挂在 running。"""
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        decide(
            driver,
            [outcome("s1", "paused")],
            sessions=(member("s1", "idle", "user_pause"),),
        )
        # 崩溃恢复后 session 不在池内 / 成员被删：world 无此成员
        actions = decide(driver, [SessionControlApplied(session_id="s1", operation="resume")])
        spawn = first(actions, StartSession)
        assert spawn.context == "node=w"
        assert node_status(driver, "w") == "running"
        # 全新执行：新成员计入下一次尝试，旧 session 档案清除
        snap = driver.state.snapshot()["nodes"]["w"]
        assert snap["attempts"] == 2
        assert snap.get("workflow_controller_session_id") is None

    @staticmethod
    def test_timeout_continue_then_complete(tmp_path):
        driver = build_driver(
            tmp_path,
            [agent_node("w", timeout=120, max_session_continues=2, acceptance={"enabled": False})],
        )
        decide(driver)
        activated = decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        actions = decide(driver, [TimerFired(timer_id=first(activated, ArmTimer).timer_id)])
        restart = first(actions, RestartSession)
        assert restart.policy is RestartPolicy.CONTINUE
        assert restart.spec_override.prompt  # 续跑提示
        # 续跑回执（运行态重建）→ 结局完成
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        actions = decide(driver, [outcome("s1", "completed")])
        assert first(actions, FinishRun).outcome.value == "completed"

    @staticmethod
    def test_timeout_continue_exhausted_fails(tmp_path):
        driver = build_driver(
            tmp_path,
            [agent_node("w", timeout=60, max_session_continues=1, acceptance={"enabled": False})],
        )
        decide(driver)
        activated = decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        decide(driver, [TimerFired(timer_id=first(activated, ArmTimer).timer_id)])
        reactivated = decide(
            driver,
            [SessionActivated(session_id="s1", context="node=w", ok=True)],
        )
        # 第二次超时：预算耗尽 → 停成员 → 停止结局后按超时失败
        actions = decide(
            driver,
            [TimerFired(timer_id=first(reactivated, ArmTimer).timer_id)],
        )
        assert first(actions, RunAction)  # AwaitSessions(0) 停成员
        assert any(type(a).__name__ == "AwaitSessions" for a in actions), actions
        actions = decide(driver, [outcome("s1", "stopped")])
        assert first(actions, FinishRun).outcome.value == "failed"
        assert "timed out" in driver.state.snapshot()["nodes"]["w"]["failure_reason"]

    @staticmethod
    def test_member_completion_cancels_timer_before_report(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                agent_node(
                    "w",
                    report={"enabled": True, "save_to": "reports/w-{{attempt}}.md"},
                    acceptance={"enabled": False},
                )
            ],
        )
        decide(driver)
        activated = decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        timer = first(activated, ArmTimer)
        actions = decide(driver, [outcome("s1", "completed")])
        # 成员完成即取消在途定时器，报告窗口内迟到触发不再匹配
        assert any(isinstance(a, CancelTimer) and a.timer_id == timer.timer_id for a in actions), (
            actions
        )
        assert start_process_for(actions, "node=w/report")
        actions = decide(driver, [TimerFired(timer_id=timer.timer_id)])
        assert not pick(actions, RestartSession)
        assert node_status(driver, "w") == "running"

    @staticmethod
    def test_pause_cancels_member_timer_and_resume_rearms_full(tmp_path):
        driver = build_driver(
            tmp_path,
            [agent_node("w", timeout=120, acceptance={"enabled": False})],
        )
        decide(driver)
        activated = decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        timer = first(activated, ArmTimer)
        actions = driver.on_control(_paused_control())
        assert isinstance(actions[0], PersistDriverState)
        # 墙钟定时器不随暂停冻结：暂停即取消，暂停段不吃超时预算
        assert any(isinstance(a, CancelTimer) and a.timer_id == timer.timer_id for a in actions), (
            actions
        )
        # 取消前一刻起爆的滞留 TimerFired：恢复重挂的是新 id，旧触发作废
        driver.on_control(_resumed_control())
        actions = decide(
            driver,
            [TimerFired(timer_id=timer.timer_id)],
            sessions=(member("s1"),),
        )
        assert not pick(actions, RestartSession)
        assert node_status(driver, "w") == "running"
        assert any(isinstance(a, ArmTimer) and a.timer_id != timer.timer_id for a in actions), (
            actions
        )

    @staticmethod
    def test_resume_leaves_terminal_member_to_outcome_flow(tmp_path):
        """world 已终态但 completed 结局尚在途：不得重跑废弃在途成果。"""
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        driver.on_control(_paused_control())
        driver.on_control(_resumed_control())
        # 恢复位照时成员已终态（完成紧随恢复）、结局下一轮才到
        actions = decide(
            driver,
            sessions=(member("s1", "idle", "reviewer_complete"),),
        )
        assert not pick(actions, StartSession)  # 不重跑
        assert node_status(driver, "w") != "pending"
        actions = decide(driver, [outcome("s1", "completed")])
        assert first(actions, FinishRun).outcome.value == "completed"

    @staticmethod
    def test_resume_reruns_running_node_whose_member_died(tmp_path):
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        driver.on_control(_paused_control())
        driver.on_control(_resumed_control())
        # 暂停期成员消亡（world 已无此成员）；滞留 paused 结局随活动弃置作废
        actions = decide(driver, [outcome("s1", "paused")], sessions=())
        assert node_status(driver, "w") == "running"  # 重排起跑
        assert first(actions, StartSession)  # 全新出生，不复用
        assert not pick(actions, RestartSession)
        assert driver.state.snapshot()["nodes"]["w"]["attempts"] == 2

    @staticmethod
    def test_reuse_running_session_waits_without_rebirth(tmp_path):
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        # 模拟恢复：节点 stopped、档案带 session
        driver.state.set_node_status("w", "stopped", session_id="s9")
        driver.on_control(_resumed_control())
        actions = decide(driver, sessions=(member("s9"),))
        assert not pick(actions, StartSession)  # 运行中的成员直接等
        assert first(actions, ArmTimer)
        assert node_status(driver, "w") == "running"
        actions = decide(driver, [outcome("s9", "completed")])
        assert first(actions, FinishRun).outcome.value == "completed"

    @staticmethod
    def test_cold_recovery_resumes_verify_before_executor(monkeypatch, tmp_path):
        driver = build_driver(
            tmp_path,
            [agent_node("w", acceptance={"enabled": True, "type": "agent"})],
        )
        driver.state.increment_attempts("w")
        driver.state.set_node_status("w", "running", session_id="exec-1")
        driver.state.set_verify_session_id("w", "verify-1")
        cold_reload(driver, monkeypatch)
        assert node_status(driver, "w") == "stopped"
        driver.prepare_recovery(running=True)

        actions = decide(
            driver,
            sessions=(member("verify-1", "idle", "user_pause"),),
        )

        restart = first(actions, RestartSession)
        assert restart.session_id == "verify-1"
        assert restart.context_override == "node=w/verify"
        assert not pick(actions, StartSession)
        info = driver.state.snapshot()["nodes"]["w"]
        assert info["workflow_controller_session_id"] == "exec-1"
        assert info["verify_session_id"] == "verify-1"
        assert info["attempts"] == 1

    @staticmethod
    def test_cold_recovery_reuses_completed_verify_verdict(monkeypatch, tmp_path):
        driver = build_driver(
            tmp_path,
            [agent_node("w", acceptance={"enabled": True, "type": "agent"})],
        )
        driver.state.increment_attempts("w")
        driver.state.set_node_status("w", "running", session_id="exec-1")
        driver.state.set_verify_session_id("w", "verify-1")
        cold_reload(driver, monkeypatch)
        driver.prepare_recovery(running=True)
        actions = decide(
            driver,
            sessions=(member("verify-1", "idle", "evaluator_complete"),),
        )
        assert first(actions, ReplaySessionOutcome).session_id == "verify-1"
        assert not pick(actions, StartSession)
        replay = SessionOutcomeReplayed(
            "verify-1",
            outcome("verify-1", "completed", digest="ALL PASS").outcome,
        )
        actions = decide(driver, [replay])
        assert first(actions, FinishRun).outcome.value == "completed"
        assert not pick(actions, StartSession)

    @staticmethod
    def test_cold_recovery_leaves_manually_paused_member_alone(monkeypatch, tmp_path):
        driver = build_driver(
            tmp_path,
            [
                {"id": "a", "task_type": "agent"},
                {"id": "b", "task_type": "agent"},
            ],
        )
        driver.state.set_node_status("a", "running", session_id="a-session")
        driver.state.set_node_status("b", "stopped", session_id="b-session")
        cold_reload(driver, monkeypatch)
        assert driver.state.interrupted_nodes == {"a"}
        driver.prepare_recovery(running=True)

        actions = decide(
            driver,
            sessions=(
                member("a-session", "idle", "user_pause"),
                member("b-session", "idle", "user_pause"),
            ),
        )

        assert [a.session_id for a in pick(actions, RestartSession)] == ["a-session"]
        assert node_status(driver, "a") == "running"
        assert node_status(driver, "b") == "stopped"

    @staticmethod
    def test_cold_recovery_resumes_without_replaying_graph_started(monkeypatch, tmp_path):
        driver = build_driver(tmp_path, [{"id": "a", "task_type": "agent"}])
        driver.state.set_node_status("a", "running", session_id="old-session")
        cold_reload(driver, monkeypatch)
        driver.prepare_recovery(running=True)

        actions = decide(driver)

        types = [e.event_type for e in pick(actions, EmitEvent)]
        assert "graph_resumed" in types
        assert "graph_started" not in types

    @staticmethod
    def test_cold_recovery_restores_or_restarts_missing_pool_member(monkeypatch, tmp_path):
        driver = build_driver(tmp_path, [{"id": "a", "task_type": "agent"}])
        driver.state.set_node_status("a", "running", session_id="old-session")
        cold_reload(driver, monkeypatch)
        driver.prepare_recovery(running=True)

        actions = decide(
            driver,
            sessions=(
                RunSessionInfo(
                    session_id="old-session",
                    context="node=a",
                    state="",
                ),
            ),
        )

        assert first(actions, RestartSession).session_id == "old-session"
        assert not pick(actions, StartSession)
        assert driver.state.attempts("a") == 0

        fallback = decide(
            driver,
            [activation("old-session", "node=a", ok=False, error="无持久记录")],
            sessions=(member("old-session", ""),),
        )
        assert first(fallback, StartSession).context == "node=a"
        assert driver.state.attempts("a") == 1

    @staticmethod
    def test_reopened_marker_does_not_retry_old_failed_node_on_boot(monkeypatch, tmp_path):
        driver = build_driver(
            tmp_path,
            [
                {"id": "a", "task_type": "agent"},
                {"id": "b", "task_type": "agent"},
            ],
        )
        driver.state.set_node_status("a", "running", session_id="a-session")
        driver.state.set_node_status("b", "failed")
        cold_reload(driver, monkeypatch)
        monkeypatch.setattr(driver, "_applied_reopen_at", "2026-10-14T10:00:00")
        driver.prepare_recovery(
            running=True,
            reopened_at="2026-10-14T10:00:00",
        )

        actions = decide(driver, sessions=(member("a-session"),))

        assert not pick(actions, StartSession)
        assert node_status(driver, "b") == "failed"

    @staticmethod
    def test_duplicate_live_executors_are_stopped_before_another_birth(tmp_path):
        driver = build_driver(tmp_path, [{"id": "w", "task_type": "agent"}])
        actions = decide(
            driver,
            sessions=(
                RunSessionInfo(session_id="s1", context="node=w", state="running"),
                RunSessionInfo(session_id="s2", context="node=w", state="running"),
            ),
        )

        stop = first(actions, AwaitSessions)
        assert stop.ids == ("s1", "s2")
        assert stop.timeout_s == 0
        assert not pick(actions, StartSession)
        assert node_status(driver, "w") == "failed"

    @staticmethod
    def test_duplicate_executors_also_stop_bound_verifier(tmp_path):
        driver = build_driver(tmp_path, [{"id": "w", "task_type": "agent"}])
        driver.state.set_node_status("w", "running", session_id="s1")
        driver.state.set_verify_session_id("w", "v1")
        actions = decide(
            driver,
            sessions=(
                RunSessionInfo(session_id="s1", context="node=w", state="running"),
                RunSessionInfo(session_id="s2", context="node=w", state="running"),
                RunSessionInfo(session_id="v1", context="node=w/verify", state="running"),
            ),
        )

        assert first(actions, AwaitSessions).ids == ("s1", "s2", "v1")
        assert node_status(driver, "w") == "failed"

    @staticmethod
    def test_duplicate_members_block_resume_action_before_stop(tmp_path):
        driver = build_driver(tmp_path, [{"id": "w", "task_type": "agent"}])
        driver.state.set_node_status("w", "stopped", session_id="s1")
        driver.on_control(_resumed_control())
        actions = decide(
            driver,
            sessions=(
                RunSessionInfo(
                    session_id="s1",
                    context="node=w",
                    state="idle",
                    stop_reason="user_pause",
                ),
                RunSessionInfo(session_id="s2", context="node=w", state="running"),
                RunSessionInfo(session_id="s3", context="node=w", state="running"),
            ),
        )

        assert not pick(actions, RestartSession)
        assert first(actions, AwaitSessions).ids == ("s1", "s2", "s3")
        assert node_status(driver, "w") == "failed"

    @staticmethod
    def test_duplicate_verifiers_also_stop_bound_executor(tmp_path):
        driver = build_driver(tmp_path, [{"id": "w", "task_type": "agent"}])
        driver.state.set_node_status("w", "running", session_id="s1")
        driver.state.set_verify_session_id("w", "v1")
        actions = decide(
            driver,
            sessions=(
                RunSessionInfo(session_id="s1", context="node=w", state="running"),
                RunSessionInfo(session_id="v1", context="node=w/verify", state="running"),
                RunSessionInfo(session_id="v2", context="node=w/verify", state="running"),
            ),
        )

        assert first(actions, AwaitSessions).ids == ("s1", "v1", "v2")
        assert node_status(driver, "w") == "failed"

    @staticmethod
    def test_duplicate_check_ignores_archived_rollback_generation(tmp_path):
        driver = build_driver(tmp_path, [{"id": "w", "task_type": "agent"}])
        driver.state.set_node_status("w", "running", session_id="old")
        driver.state.reset_rollback("w", 1)
        driver.state.set_node_status("w", "running", session_id="current")
        actions = decide(
            driver,
            sessions=(
                RunSessionInfo(session_id="old", context="node=w", state="running"),
                RunSessionInfo(session_id="current", context="node=w", state="running"),
            ),
        )

        assert not pick(actions, AwaitSessions)
        assert node_status(driver, "w") == "running"

    @staticmethod
    def test_executor_and_verifier_are_distinct_phases(tmp_path):
        driver = build_driver(tmp_path, [{"id": "w", "task_type": "agent"}])
        driver.state.set_node_status("w", "running", session_id="s1")
        driver.state.set_verify_session_id("w", "s2")
        actions = decide(
            driver,
            sessions=(
                RunSessionInfo(session_id="s1", context="node=w", state="running"),
                RunSessionInfo(session_id="s2", context="node=w/verify", state="running"),
            ),
        )

        assert not pick(actions, AwaitSessions)
        assert node_status(driver, "w") == "running"

    @staticmethod
    def test_failed_resume_clears_old_verify_binding(tmp_path):
        driver = build_driver(
            tmp_path,
            [agent_node("w", acceptance={"enabled": True, "type": "agent"})],
        )
        driver.state.set_node_status("w", "failed", session_id="exec-1")
        driver.state.set_verify_session_id("w", "verify-old")
        driver.on_control(_resumed_control())

        actions = decide(driver)

        assert first(actions, StartSession).context == "node=w"
        info = driver.state.snapshot()["nodes"]["w"]
        assert info["verify_session_id"] is None
        assert info["workflow_controller_session_id"] is None

    @staticmethod
    def test_reuse_paused_session_resumes(tmp_path):
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        driver.state.increment_attempts("w")
        driver.state.set_node_status("w", "stopped", session_id="s9")
        driver.on_control(_resumed_control())
        actions = decide(
            driver,
            sessions=(member("s9", "idle", "user_pause"),),
        )
        restart = first(actions, RestartSession)
        assert restart.policy is RestartPolicy.RESUME_IF_PAUSED
        assert driver.state.attempts("w") == 1

    @staticmethod
    def test_resume_stopped_terminal_session_respawns(tmp_path):
        # 暂停（pause）与停止（stop）语义不同：pause 后 member.state=IDLE 可复用
        # （见 test_reuse_paused_session_resumes）；stop 后 member.state=STOPPED
        # 是终态，续跑时不可复用，须新建成员。回归锚：用户 stop 掉成员后点
        # 续跑，不应卡在「无 session 可复用」而静默，也不应复用已终态的 session。
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        driver.state.increment_attempts("w")
        driver.state.set_node_status("w", "stopped", session_id="s9")
        driver.on_control(_resumed_control())
        actions = decide(
            driver,
            sessions=(member("s9", "stopped", "user_pause"),),
        )
        spawn = first(actions, StartSession)
        assert spawn.context == "node=w"
        assert not pick(actions, RestartSession)
        # 终态复用失败时保留已消耗的 attempt 计数，新建成员计入下一次尝试
        assert driver.state.attempts("w") == 2

    @staticmethod
    def test_resume_uses_current_graph_model_without_new_member(monkeypatch, tmp_path):
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        monkeypatch.setattr(driver, "_model", "new-provider/new-model")
        monkeypatch.setattr(driver, "_provider_index", 2)
        monkeypatch.setattr(driver, "_model_pinned", True)
        driver.state.set_node_status("w", "stopped", session_id="s9")
        driver.on_control(_resumed_control())
        actions = decide(driver, sessions=(member("s9", "idle", "user_pause"),))
        restart = first(actions, RestartSession)
        assert not pick(actions, StartSession)
        assert restart.spec_override.refresh_overrides()["model"] == "new-provider/new-model"
        assert restart.spec_override.refresh_overrides()["provider_index"] == 2
        assert restart.spec_override.refresh_overrides()["model_pinned"] is True

    @staticmethod
    def test_timeout_continue_receipt_uses_dispatched_context(tmp_path):
        """续跑回执按派发动作的 context 原样透传：无标签回执对不上账即弃单。"""
        driver = build_driver(
            tmp_path,
            [agent_node("w", timeout=120, max_session_continues=2, acceptance={"enabled": False})],
        )
        decide(driver)
        activated = decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        actions = decide(driver, [TimerFired(timer_id=first(activated, ArmTimer).timer_id)])
        assert_restart_receipt_roundtrip(driver, actions, "s1")

    @staticmethod
    def test_member_resume_restart_receipt_threads(tmp_path):
        """成员级恢复的 RESUME_IF_PAUSED 回执按派发 context 对账换相重挂。"""
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        decide(
            driver,
            [outcome("s1", "paused")],
            sessions=(member("s1", "idle", "user_pause"),),
        )
        actions = decide(
            driver,
            [SessionControlApplied(session_id="s1", operation="resume")],
            sessions=(member("s1", "idle", "user_pause"),),
        )
        assert_restart_receipt_roundtrip(driver, actions, "s1")

    @staticmethod
    def test_group_resume_restarts_carry_reconciliation_context(tmp_path):
        """组恢复两分支（stopped/running 节点）的重启回执按派发 context 对账。"""
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        driver.state.set_node_status("w", "stopped", session_id="s9")
        driver.on_control(_resumed_control())
        actions = decide(driver, sessions=(member("s9", "idle", "user_pause"),))
        restart = first(actions, RestartSession)
        assert restart.context_override == "node=w"
        reactivated = decide(
            driver,
            [activation("s9", restart.context_override)],
        )
        assert first(reactivated, ArmTimer)
        actions = decide(driver, [outcome("s9", "completed")])
        assert first(actions, FinishRun).outcome.value == "completed"

        # running 节点上的用户暂停成员（回执消费后走 PAUSED_WAIT 结局面）
        driver2 = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        driver2.state.set_node_status("w", "running", session_id="s9")
        driver2.on_control(_resumed_control())
        actions = decide(driver2, sessions=(member("s9", "idle", "user_pause"),))
        restart = first(actions, RestartSession)
        assert restart.context_override == "node=w"
        decide(
            driver2,
            [activation("s9", restart.context_override)],
        )
        actions = decide(driver2, [outcome("s9", "completed")])
        assert first(actions, FinishRun).outcome.value == "completed"

    @staticmethod
    def test_group_resume_keeps_inflight_birth_for_stuck_receipt(tmp_path):
        """出生回执滞留 FIFO 的在途出生：恢复不得按消亡重排（防重复出生）。"""
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)  # StartSession 已派发，出生回执未达
        driver.on_control(_paused_control())  # 组暂停冻结，回执滞留 FIFO
        driver.on_control(_resumed_control())
        actions = decide(driver)  # 恢复复位先于滞留回执：不按消亡重排
        assert not pick(actions, StartSession)
        assert node_status(driver, "w") == "running"
        actions = decide(
            driver,
            [SessionActivated(session_id="s1", context="node=w", ok=True)],
        )
        assert first(actions, ArmTimer)  # 滞留回执正常消费换相
        actions = decide(driver, [outcome("s1", "completed")])
        assert first(actions, FinishRun).outcome.value == "completed"

    @pytest.mark.parametrize(
        "raw, expected", [(None, 7200.0), ("bad", 7200.0), (300, 300.0), (0, None), (-1, None)]
    )
    def test_member_timeout_policy(self, tmp_path, raw, expected):
        driver = build_driver(
            tmp_path,
            [agent_node("w", timeout=raw, acceptance={"enabled": False})],
        )
        decide(driver)
        actions = decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        timers = pick(actions, ArmTimer)
        assert [timer.delay_s for timer in timers] == ([] if expected is None else [expected])


# ── agent 验收与 verdict ───────────────────────────────────────────


class TestVerifyAgent:
    @staticmethod
    def _to_verify(tmp_path, **acc) -> tuple[GraphDriver, list[RunAction]]:
        driver = build_driver(
            tmp_path,
            [bash_node("a", "true", acceptance={"enabled": True, "type": "agent", **acc})],
        )
        decide(driver)
        actions = decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        return driver, actions

    def test_verdict_pass_and_fail(self, tmp_path):
        driver, actions = self._to_verify(tmp_path, verifier="checker")
        spawn = first(actions, StartSession)
        assert spawn.context == "node=a/verify" and spawn.role == "verifier"
        decide(driver, [SessionActivated(session_id="v1", context="node=a/verify", ok=True)])
        actions = decide(driver, [outcome("v1", "completed", digest="ALL PASS")])
        assert first(actions, FinishRun).outcome.value == "completed"
        assert node_status(driver, "a") == "passed"

    def test_verdict_fail_without_fallback(self, tmp_path):
        driver, actions = self._to_verify(tmp_path)
        decide(driver, [SessionActivated(session_id="v1", context="node=a/verify", ok=True)])
        actions = decide(driver, [outcome("v1", "completed", digest="FAILED: x")])
        assert first(actions, FinishRun).outcome.value == "failed"

    def test_verdict_fallback_agent_judges(self, tmp_path):
        driver, actions = self._to_verify(tmp_path)
        decide(driver, [SessionActivated(session_id="v1", context="node=a/verify", ok=True)])
        # 验收输出无结论 → 起兜底 agent
        actions = decide(driver, [outcome("v1", "completed", digest="今天天气不错")])
        fallback = first(actions, StartSession)
        assert fallback.context == "node=a/verdict"
        decide(driver, [SessionActivated(session_id="f1", context="node=a/verdict", ok=True)])
        actions = decide(driver, [outcome("f1", "completed", digest="PASS")])
        assert first(actions, FinishRun).outcome.value == "completed"

    def test_verify_member_completion_cancels_timer_before_verdict(self, tmp_path):
        driver, actions = self._to_verify(tmp_path)
        activated = decide(
            driver,
            [SessionActivated(session_id="v1", context="node=a/verify", ok=True)],
        )
        timer = first(activated, ArmTimer)
        # 验收输出无结论 → 起兜底；验收成员定时器已在结局消费时取消
        actions = decide(driver, [outcome("v1", "completed", digest="今天天气不错")])
        assert first(actions, StartSession).context == "node=a/verdict"
        assert any(isinstance(a, CancelTimer) and a.timer_id == timer.timer_id for a in actions), (
            actions
        )
        # 兜底出生窗口内的迟到触发不再匹配：不对验收成员误发续跑
        actions = decide(driver, [TimerFired(timer_id=timer.timer_id)])
        assert not pick(actions, RestartSession)
        decide(driver, [SessionActivated(session_id="f1", context="node=a/verdict", ok=True)])
        actions = decide(driver, [outcome("f1", "completed", digest="PASS")])
        assert first(actions, FinishRun).outcome.value == "completed"

    def test_verify_agent_paused_then_stopped_keeps_breakpoint(self, tmp_path):
        driver, actions = self._to_verify(tmp_path)
        decide(driver, [SessionActivated(session_id="v1", context="node=a/verify", ok=True)])
        actions = decide(
            driver,
            [outcome("v1", "paused")],
            sessions=(member("v1", "idle", "user_pause"),),
        )
        assert not pick(actions, FinishRun)
        assert node_status(driver, "a") == "stopped"
        assert graph_status(driver) == "paused"

    def test_verify_continue_receipt_uses_verify_context(self, tmp_path):
        """验收成员续跑回执带 /verify 标签（与出生同径），按此对账换相。"""
        driver, actions = self._to_verify(tmp_path, timeout=60)
        activated = decide(
            driver,
            [SessionActivated(session_id="v1", context="node=a/verify", ok=True)],
        )
        actions = decide(driver, [TimerFired(timer_id=first(activated, ArmTimer).timer_id)])
        restart = first(actions, RestartSession)
        assert restart.context_override == "node=a/verify"
        reactivated = decide(
            driver,
            [activation("v1", restart.context_override)],
        )
        assert first(reactivated, ArmTimer)
        actions = decide(driver, [outcome("v1", "completed", digest="ALL PASS")])
        assert first(actions, FinishRun).outcome.value == "completed"

    def test_group_resume_reconciles_verify_member_running(self, tmp_path):
        """组恢复对账验收/兜底成员：running 节点重挂定时器，不误重排弃孤儿。"""
        driver, actions = self._to_verify(tmp_path)
        decide(driver, [SessionActivated(session_id="v1", context="node=a/verify", ok=True)])
        driver.on_control(_paused_control())
        driver.on_control(_resumed_control())
        # 执行器（bash 进程）已终局、world 里在跑的是验收成员
        actions = decide(driver, sessions=(member("v1"),))
        assert first(actions, ArmTimer)  # 暂停取消的验收超时守护重挂
        assert node_status(driver, "a") == "running"
        assert not pick(actions, StartSession)  # 不整节点重排（验收成员免成孤儿）
        actions = decide(driver, [outcome("v1", "completed", digest="ALL PASS")])
        assert first(actions, FinishRun).outcome.value == "completed"

    def test_group_resume_reconciles_verify_member_paused(self, tmp_path):
        """组恢复 stopped 节点对账验收成员：派发恢复而非按消亡整节点重跑。"""
        driver, actions = self._to_verify(tmp_path)
        decide(driver, [SessionActivated(session_id="v1", context="node=a/verify", ok=True)])
        decide(
            driver,
            [outcome("v1", "paused")],
            sessions=(member("v1", "idle", "user_pause"),),
        )
        assert node_status(driver, "a") == "stopped"
        driver.on_control(_paused_control())
        driver.on_control(_resumed_control())
        actions = decide(
            driver,
            sessions=(member("v1", "idle", "user_pause"),),
        )
        restart = first(actions, RestartSession)
        assert restart.session_id == "v1"
        assert restart.context_override == "node=a/verify"
        assert node_status(driver, "a") == "running"
        assert not pick(actions, StartSession)
        decide(driver, [SessionActivated(session_id="v1", context="node=a/verify", ok=True)])
        actions = decide(driver, [outcome("v1", "completed", digest="ALL PASS")])
        assert first(actions, FinishRun).outcome.value == "completed"

    def test_group_resume_rearms_verdict_fallback_timer(self, tmp_path):
        """兜底判定相位组暂停后恢复：重挂兜底定时器，不误重排弃孤儿。"""
        driver, actions = self._to_verify(tmp_path)
        decide(driver, [SessionActivated(session_id="v1", context="node=a/verify", ok=True)])
        decide(driver, [outcome("v1", "completed", digest="今天天气不错")])
        decide(driver, [SessionActivated(session_id="f1", context="node=a/verdict", ok=True)])
        driver.on_control(_paused_control())
        driver.on_control(_resumed_control())
        actions = decide(driver, sessions=(member("f1"),))
        assert first(actions, ArmTimer)
        assert node_status(driver, "a") == "running"
        assert not pick(actions, StartSession)
        actions = decide(driver, [outcome("f1", "completed", digest="PASS")])
        assert first(actions, FinishRun).outcome.value == "completed"


# ── 回滚 / 传播 / run_on_failure ──────────────────────────────────


class TestRollbackAndPropagation:
    @staticmethod
    def test_failure_propagation_skips_unreachable(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                {"id": "a", "task_type": "bash", "command": "x", "acceptance": {"enabled": False}},
                bash_node("b", "true", depends_on=["a"], acceptance={"enabled": False}),
            ],
        )
        decide(driver)
        actions = decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=1)])
        # 事件驱动下同轮传播并裁决，不能等待不存在的下一轮空通知
        assert node_status(driver, "b") == "skipped"
        assert first(actions, FinishRun).outcome.value == "failed"

    @staticmethod
    def test_run_on_failure_executes_after_dep_failed(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                {"id": "a", "task_type": "bash", "command": "x", "acceptance": {"enabled": False}},
                bash_node(
                    "cleanup",
                    "true",
                    depends_on=["a"],
                    run_on_failure=True,
                    acceptance={"enabled": False},
                ),
            ],
        )
        decide(driver)
        actions = decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=1)])
        assert first(actions, StartProcess).context == "node=cleanup"
        decide(driver)
        actions = decide(driver, [ProcessExit(proc_id="p2", context="node=cleanup", returncode=0)])
        assert first(actions, FinishRun).outcome.value == "failed"

    @staticmethod
    def test_on_exhaust_continue_marks_skipped(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                bash_node(
                    "a",
                    "true",
                    depends_on=[],
                    acceptance={
                        "enabled": True,
                        "type": "script",
                        "script": "false",
                        "max_retries": 0,
                        "on_exhaust": "continue",
                    },
                ),
                bash_node("b", "true", depends_on=["a"], acceptance={"enabled": False}),
            ],
        )
        decide(driver)
        decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        actions = decide(
            driver,
            [proc_exit("node=a/verify", 1, proc_id="p2")],
        )
        assert node_status(driver, "a") == "skipped"
        assert first(actions, StartProcess).context == "node=b"  # skipped 解锁下游

    @staticmethod
    def _graph(tmp_path):
        return build_driver(
            tmp_path,
            [
                bash_node("init", "true", acceptance={"enabled": False}),
                bash_node("mid", "true", depends_on=["init"], acceptance={"enabled": False}),
                bash_node(
                    "tail",
                    "true",
                    depends_on=["mid"],
                    acceptance={
                        "enabled": True,
                        "type": "script",
                        "script": "false",
                        "max_retries": 0,
                        "on_exhaust": "rollback",
                        "rollback_to": "init",
                    },
                ),
            ],
            rollback_budget=2,
        )

    def test_rollback_resets_target_and_descendants(self, tmp_path):
        driver = self._graph(tmp_path)
        for pid, ctx in (("p1", "node=init"), ("p2", "node=mid"), ("p3", "node=tail")):
            decide(driver)
            decide(driver, [ProcessExit(proc_id=pid, context=ctx, returncode=0)])
        actions = decide(
            driver,
            [proc_exit("node=tail/verify", 1, proc_id="p4")],
        )
        assert any(isinstance(a, EmitEvent) and a.event_type == "node_rollback" for a in actions)
        snap = driver.state.snapshot()
        # 失败节点与目标及后裔整体 reset（旧实例归档），同轮重排新一代
        assert snap["nodes"]["init"]["status"] == "running"
        assert snap["nodes"]["tail"]["status"] == "pending"
        assert snap["nodes"]["mid"]["status"] == "pending"
        assert snap["rollback_edges"], "回滚边应入账"
        assert snap["nodes"]["init"]["gen"] == 1
        assert "tail#0" in snap["retired_instances"]
        assert first(actions, StartProcess).context == "node=init"

    def test_rollback_resets_retries_budget(self, tmp_path):
        """回滚路径清零重试预算：回滚前消耗过 retries 的节点回滚后不再立即耗尽。"""
        driver = self._graph(tmp_path)
        driver.state.increment_retries("tail")
        assert driver.state.retries("tail") == 1
        for pid, ctx in (("p1", "node=init"), ("p2", "node=mid"), ("p3", "node=tail")):
            decide(driver)
            decide(driver, [ProcessExit(proc_id=pid, context=ctx, returncode=0)])
        decide(driver, [proc_exit("node=tail/verify", 1, proc_id="p4")])
        assert driver.state.retries("tail") == 0


# ── 子图（generator）──────────────────────────────────────────────


_SUB_YAML = {
    "workflow": "sub",
    "max_parallel": 1,
    "nodes": [
        {"id": "s1", "task_type": "bash", "command": "true", "acceptance": {"enabled": False}},
        bash_node("s2", "true", depends_on=["s1"], acceptance={"enabled": False}),
    ],
}


class TestGeneratorSubgraph:
    @staticmethod
    def _gen_driver(tmp_path, **acc_extra):
        return build_driver(
            tmp_path,
            [
                {
                    "id": "gen",
                    "task_type": "generator",
                    "generator_type": "bash",
                    "command": "cat > sub.yaml",
                    "subgraph": {"output": "sub.yaml"},
                    "acceptance": {
                        "enabled": True,
                        "type": "script",
                        "script": "test -f sub.yaml",
                        **acc_extra,
                    },
                }
            ],
        )

    @staticmethod
    def _feed_generation(driver) -> None:
        decide(driver)
        decide(driver, [ProcessExit(proc_id="p1", context="node=gen", returncode=0)])

    def test_generation_reads_and_merges_on_pass(self, tmp_path):
        driver = self._gen_driver(tmp_path)
        decide(driver)
        actions = decide(driver, [ProcessExit(proc_id="p1", context="node=gen", returncode=0)])
        assert first(actions, ReadArtifact).path == "sub.yaml"
        sub_text = yaml.safe_dump(_SUB_YAML)
        decide(driver, [_artifact("node=gen", "sub.yaml", sub_text)])
        # 验收通过后子图节点并入父图并在同轮调度
        actions = decide(
            driver, [ProcessExit(proc_id="p9", context="node=gen/verify", returncode=0)]
        )
        snap = driver.state.snapshot()
        assert snap["nodes"]["gen"]["subgraph_generated"] is True
        assert snap["nodes"]["gen"]["subgraph_node_ids"] == ["gen:s1", "gen:s2"]
        assert first(actions, StartProcess).context == "node=gen:s1"

    def test_generation_missing_yaml_fails_node(self, tmp_path):
        driver = self._gen_driver(tmp_path)
        self._feed_generation(driver)
        decide(driver)
        actions = decide(driver, [_artifact("node=gen", "sub.yaml", None, exists=False)])
        assert first(actions, FinishRun).outcome.value == "failed"
        reason = driver.state.snapshot()["nodes"]["gen"]["failure_reason"]
        assert "subgraph 未就绪" in reason

    def test_generation_invalid_config_fails_node(self, tmp_path):
        driver = self._gen_driver(tmp_path)
        self._feed_generation(driver)
        decide(driver)
        bad = yaml.safe_dump(
            {
                "workflow": "sub",
                "nodes": [
                    {"id": "n", "task_type": "generator", "subgraph": {"output": "x.yaml"}},
                ],
            }
        )
        actions = decide(driver, [_artifact("node=gen", "sub.yaml", bad)])
        assert first(actions, FinishRun).outcome.value == "failed"

    def test_merged_generator_retry_rereads_without_regeneration(self, tmp_path):
        """验收失败重试：已编排的 generator 不再重跑生成，只重读子 yaml 再验收。"""
        driver = self._gen_driver(tmp_path, max_retries=1, on_exhaust="stop")
        decide(driver)
        actions = decide(driver, [ProcessExit(proc_id="p1", context="node=gen", returncode=0)])
        sub_text = yaml.safe_dump(_SUB_YAML)
        decide(driver, [_artifact("node=gen", "sub.yaml", sub_text)])
        actions = decide(
            driver, [ProcessExit(proc_id="p9", context="node=gen/verify", returncode=1)]
        )
        assert first(actions, ReadArtifact).path == "sub.yaml"
        assert not any(isinstance(a, StartProcess) and a.context == "node=gen" for a in actions), (
            "已编排的 generator 不得重新生成"
        )


# ── 组控反应 ──────────────────────────────────────────────────────


def _paused_control():
    from workflow_controller.plugin.run_driver import ControlKind, ControlNotification

    return ControlNotification(kind=ControlKind.PAUSED)


def _resumed_control():
    from workflow_controller.plugin.run_driver import ControlKind, ControlNotification

    return ControlNotification(kind=ControlKind.RESUMED)


def _stopped_control():
    from workflow_controller.plugin.run_driver import ControlKind, ControlNotification

    return ControlNotification(kind=ControlKind.STOPPED)


class TestGroupControlReaction:
    @staticmethod
    def test_pause_freezes_scheduling_and_marks_paused(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                bash_node("a", "true", acceptance={"enabled": False}),
                bash_node("b", "true", depends_on=["a"], acceptance={"enabled": False}),
            ],
        )
        decide(driver)
        driver.on_control(_paused_control())
        actions = decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        assert not pick(actions, StartProcess)  # 冻结新出生
        assert graph_status(driver) == "paused"
        # 恢复：无断点节点直接续排
        driver.on_control(_resumed_control())
        actions = decide(driver)
        assert first(actions, StartProcess).context == "node=b"
        assert graph_status(driver) == "running"

    @staticmethod
    def test_stop_finalizes_breakpoints(tmp_path):
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        driver.on_control(_stopped_control())
        actions = decide(driver)
        assert not pick(actions, FinishRun)  # 终态裁决权归控制侧
        assert node_status(driver, "w") == "stopped"
        assert graph_status(driver) == "paused"

    @staticmethod
    def test_resume_resets_failed_and_bash_stopped_to_pending(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                {"id": "a", "task_type": "bash", "command": "x", "acceptance": {"enabled": False}},
                bash_node("b", "true", depends_on=["a"], acceptance={"enabled": False}),
            ],
        )
        decide(driver)
        decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=1)])
        decide(driver)  # 传播轮（b 标 skipped）
        driver.on_control(_paused_control())
        driver.on_control(_resumed_control())
        actions = decide(driver)
        # failed/skipped 节点复位 pending，attempts 保留（历史不覆盖），重跑为下一次尝试
        assert node_status(driver, "a") == "running"
        assert driver.state.snapshot()["nodes"]["a"]["attempts"] == 2
        assert first(actions, StartProcess).context == "node=a"

    @staticmethod
    def test_resume_resets_retry_budget_not_just_attempts(tmp_path):
        """续跑重置重试预算：上一轮耗尽的重试次数不继承到新一轮（attempts 仍保留）。"""
        driver = build_driver(
            tmp_path,
            [
                {"id": "a", "task_type": "bash", "command": "x", "acceptance": {"enabled": False}},
                bash_node("b", "true", depends_on=["a"], acceptance={"enabled": False}),
            ],
        )
        decide(driver)
        decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=1)])
        decide(driver)  # 传播轮（b 标 skipped）
        driver.state.increment_retries("a")  # 模拟上一轮已耗尽重试预算
        assert driver.state.retries("a") == 1
        driver.on_control(_paused_control())
        driver.on_control(_resumed_control())
        decide(driver)
        assert driver.state.retries("a") == 0  # 预算重置
        assert driver.state.attempts("a") == 2  # 展示计数保留

    @staticmethod
    def test_resume_reruns_running_bash_killed_during_pause(tmp_path):
        driver = build_driver(tmp_path, [bash_node("a", "true", acceptance={"enabled": False})])
        decide(driver)  # bash 起跑（节点 running、无 session）
        driver.on_control(_paused_control())
        driver.on_control(_resumed_control())
        # 恢复复位先于滞留的组杀回执：活动弃置、回执作废，节点重排重跑
        actions = decide(
            driver,
            [proc_exit("node=a", 0, proc_id="p1")],
        )
        assert first(actions, StartProcess).context == "node=a"
        assert driver.state.snapshot()["nodes"]["a"]["attempts"] == 1  # 计数已重置


# ── 终态裁决与快照 ─────────────────────────────────────────────────


class TestFinishAndSnapshot:
    @staticmethod
    def test_all_passed_finishes_completed(tmp_path):
        driver = build_driver(tmp_path, [bash_node("a", "true", acceptance={"enabled": False})])
        decide(driver)
        actions = decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        finish = first(actions, FinishRun)
        assert finish.outcome.value == "completed"

    @staticmethod
    def test_paused_graph_does_not_finish(tmp_path):
        """有 stopped 断点（成员暂停）不收口：等组恢复或用户控制。"""
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        actions = decide(
            driver,
            [outcome("s1", "paused")],
            sessions=(member("s1", "idle", "user_pause"),),
        )
        assert not pick(actions, FinishRun)

    @staticmethod
    def test_max_parallel_gates_node_starts(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                bash_node("a", "true", acceptance={"enabled": False}),
                bash_node("b", "true", acceptance={"enabled": False}),
                bash_node("c", "true", acceptance={"enabled": False}),
            ],
            max_parallel=2,
        )
        actions = decide(driver)
        assert len(pick(actions, StartProcess)) == 2  # 容量门
        actions = decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        assert first(actions, StartProcess).context == "node=c"

    @staticmethod
    def test_snapshot_roundtrip_restores_state(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                bash_node(
                    "a", "true", acceptance={"enabled": True, "type": "script", "script": "true"}
                )
            ],
        )
        decide(driver)
        blob = driver.snapshot()
        assert blob["schema_version"] == 1
        assert blob["graph_id"] == "g1"
        assert blob["state"]["nodes"]["a"]["status"] == "running"
        # 按快照重建（生产路径：plugin._driver_from_blob 的状态面）
        program = translator.translate_program(driver.yaml_path, {}, None)
        state = GraphState(
            graph_id="g1",
            workflow=program.workflow,
            node_ids=program.dag.all_ids(),
            initial_data={
                "graph_id": blob["graph_id"],
                "status": blob["status"],
                "nodes": blob["state"]["nodes"],
                "rollbacks": blob["state"]["rollbacks"],
                "rollback_edges": blob["state"]["rollback_edges"],
                "retired_instances": blob["state"]["retired_instances"],
            },
        )
        assert state.snapshot()["nodes"]["a"]["status"] == "stopped"  # 先保留断点，再按成员事实决定
        assert state.snapshot()["status"] == "running"

    @staticmethod
    def test_projection_shape(tmp_path):
        driver = build_driver(tmp_path, [bash_node("a", "true", acceptance={"enabled": False})])
        projection = driver.projection()
        assert projection["schema_version"] == 2
        assert projection["graph_id"] == "g1"
        assert projection["description"] == ""
        assert "nodes" in projection

    @staticmethod
    def test_handle_op_reload_and_update_model(tmp_path):
        driver = build_driver(tmp_path, [agent_node("a", acceptance={"enabled": False})])
        driver.handle_op("update_model", {"model": "m2", "provider_index": 1})
        assert driver.runtime_options()["model"] == "m2"
        driver.handle_op("reload", {"options": {"evaluator": "feedback"}})
        assert driver.runtime_options()["evaluator"] == "feedback"
        blob = driver.snapshot()
        assert blob["options"]["model"] == "m2" and blob["options"]["evaluator"] == "feedback"

    @staticmethod
    def test_cli_wrapper_enters_member_config(tmp_path):
        """验证 graph 热切换 wrapper 进入成员配置。"""
        driver = build_driver(tmp_path, [agent_node("a", acceptance={"enabled": False})])
        driver.apply_runtime_config({"cli_wrapper": "cpx"})
        action = first(decide(driver), StartSession)
        assert action.spec.config_overrides["cli_wrapper"] == "cpx"

    @staticmethod
    @pytest.mark.parametrize(
        ("legacy_enabled", "expected_mode"),
        [(True, "enabled"), (False, "disabled")],
    )
    def test_reviewer_thinking_enters_member_profile(tmp_path, legacy_enabled, expected_mode):
        driver = build_driver(tmp_path, [agent_node("a", acceptance={"enabled": False})])
        driver.apply_runtime_config(
            {
                "reviewer_enabled": True,
                "reviewer_thinking_enabled": legacy_enabled,
                "reviewer_thinking_budget_tokens": 4096,
                "reviewer_max_tokens": 8192,
            }
        )
        action = first(decide(driver), StartSession)
        profile = json.loads(action.spec.reviewer_profile)
        assert profile["thinking_enabled"] is legacy_enabled
        assert SessionReviewerConfig(**profile).thinking_mode == expected_mode
        assert profile["thinking_budget_tokens"] == 4096
        assert profile["max_tokens"] == 8192

    @staticmethod
    def test_low_reviewer_max_tokens_enters_member_profile(tmp_path):
        driver = build_driver(tmp_path, [agent_node("a", acceptance={"enabled": False})])
        driver.apply_runtime_config(
            {
                "reviewer_enabled": True,
                "reviewer_max_tokens": 512,
            }
        )
        action = first(decide(driver), StartSession)
        profile = json.loads(action.spec.reviewer_profile)
        assert SessionReviewerConfig(**profile).max_tokens == 512

    @staticmethod
    def test_update_model_without_model_pinned_keeps_pin(tmp_path):
        """update_model 未传 model_pinned 时不隐式重置 pin（仅覆盖给出的键）。"""
        driver = build_driver(tmp_path, [agent_node("a", acceptance={"enabled": False})])
        driver.apply_runtime_config({"model_pinned": True})
        driver.handle_op("update_model", {"model": "m2", "provider_index": 1})
        assert driver.runtime_options()["model"] == "m2"
        assert driver.runtime_options()["model_pinned"] is True

    @staticmethod
    def test_member_control_marks_node_state(tmp_path):
        driver = build_driver(tmp_path, [agent_node("w", acceptance={"enabled": False})])
        decide(driver)
        decide(driver, [SessionActivated(session_id="s1", context="node=w", ok=True)])
        decide(driver, [SessionControlApplied(session_id="s1", operation="pause")])
        assert node_status(driver, "w") == "stopped"
        assert graph_status(driver) == "paused"
        # 未知 session 的控制不影响任何节点
        decide(
            driver,
            [SessionControlApplied(session_id="ghost", operation="pause")],
            sessions=(member("s1"),),
        )
        assert node_status(driver, "w") == "stopped"


def _artifact(context: str, path: str, value: str | None, exists: bool = True):
    from workflow_controller.plugin.run_driver import ArtifactRead

    return ArtifactRead(context=context, path=path, value=value, exists=exists)


# ── 事件面 ─────────────────────────────────────────────────────────


class TestEventFace:
    @staticmethod
    def test_node_and_graph_events_emitted(tmp_path):
        driver = build_driver(tmp_path, [bash_node("a", "true", acceptance={"enabled": False})])
        events = [a for a in decide(driver) if isinstance(a, EmitEvent)]
        types = [e.event_type for e in events]
        assert "graph_started" in types
        assert "projection_updated" in types
        events = [
            a
            for a in decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
            if isinstance(a, EmitEvent)
        ]
        types = [e.event_type for e in events]
        assert "node_state" in types and "node_history" in types
        assert "graph_completed" in types

    @staticmethod
    def test_node_history_carries_verify_facts(tmp_path):
        driver = build_driver(
            tmp_path,
            [
                bash_node(
                    "a", "true", acceptance={"enabled": True, "type": "script", "script": "true"}
                )
            ],
        )
        decide(driver)
        decide(driver, [ProcessExit(proc_id="p1", context="node=a", returncode=0)])
        after_verify = decide(
            driver,
            [proc_exit("node=a/verify", 0, proc_id="p2")],
        )
        events = []
        for a in after_verify:
            if isinstance(a, EmitEvent):
                events.append(a)
        history = next(
            e for e in events if e.event_type == "node_history" and e.data.get("status") == "passed"
        )
        assert history.data["verify_type"] == "script"
        assert history.data["gen"] == 0
