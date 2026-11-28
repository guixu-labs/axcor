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
import json
import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from workflow_controller.api.session_api import (
    ReviewerStartOverrides,
    SessionAPI,
    SessionInfo,
    WorkflowSessionOperationError,
)
from workflow_controller.config.settings import AppConfig, SessionConfig, SessionReviewerConfig
from workflow_controller.core.runs import RunKind
from workflow_controller.session.manager import SessionPool
from workflow_controller.session.session import Session, SessionState


async def test_stop_all_waits_for_direct_session_stop_and_rejects_workflow_member(monkeypatch):
    """批量停止对每个会话执行归属校验，并等待已受理的停止完成。"""
    stopped = []

    async def stop_session(session_id):
        await asyncio.sleep(0)
        stopped.append(session_id)

    orch = MagicMock()
    orch.affiliation.resolve.side_effect = {"member": MagicMock(run_id="run"), "direct": None}.get
    orch.run_store.get = AsyncMock(return_value=MagicMock(kind=RunKind.WORKFLOW))
    orch.stop_session = AsyncMock(side_effect=stop_session)
    api = SessionAPI(orch)
    monkeypatch.setattr(
        api, "list_sessions", lambda: [MagicMock(id="member"), MagicMock(id="direct")]
    )

    with pytest.raises(ExceptionGroup, match="部分会话停止失败") as excinfo:
        await api.stop_all()

    orch.stop_session.assert_awaited_once_with("direct")
    assert stopped == ["direct"]
    assert any(
        isinstance(err, WorkflowSessionOperationError) and "run" in str(err)
        for err in excinfo.value.exceptions
    )


def test_list_claude_sessions_discovers_other_user_home(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "workflow_controller.transcript.sources._USER_HOME_ROOTS", (tmp_path / "users",)
    )
    monkeypatch.setenv("HOME", str(tmp_path / "server"))
    monkeypatch.delenv("CLAUDE_CONFIG_DIR", raising=False)
    transcript = tmp_path / "users" / "alice" / ".claude" / "projects" / "-work" / "ses_1.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_text(json.dumps({"type": "user", "message": {"content": "hello"}}) + "\n")

    orch = MagicMock()
    orch.config.claude_scan_user_homes = True
    sessions = SessionAPI(orch, None).list_claude_sessions("/work")

    assert [(item["id"], item["title"], item["directory"]) for item in sessions] == [
        ("ses_1", "hello", "/work"),
    ]


def test_list_claude_sessions_skips_other_user_home_by_default(tmp_path, monkeypatch):
    """隐私默认：未开启 claude_scan_user_homes 时不扫描其他用户主目录。"""
    monkeypatch.setattr(
        "workflow_controller.transcript.sources._USER_HOME_ROOTS", (tmp_path / "users",)
    )
    monkeypatch.setenv("HOME", str(tmp_path / "server"))
    monkeypatch.delenv("CLAUDE_CONFIG_DIR", raising=False)
    transcript = tmp_path / "users" / "alice" / ".claude" / "projects" / "-work" / "ses_1.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_text(json.dumps({"type": "user", "message": {"content": "hello"}}) + "\n")

    orch = MagicMock()
    orch.config.claude_scan_user_homes = False
    sessions = SessionAPI(orch, None).list_claude_sessions("/work")

    assert sessions == []


def _fake_session(
    state: SessionState, *, created_at: datetime, last_activity: datetime
) -> MagicMock:
    s = MagicMock()
    s.state = state
    s.created_at = created_at
    s.last_activity = last_activity
    s.config = MagicMock()
    s.config.loop = MagicMock(max_iterations=10)
    s.current_iteration = 0
    s.id = "test-1"
    s.name = "test"
    s.role = "worker"
    return s


def test_running_uses_realtime():
    created = datetime.now(tz=UTC)
    s = _fake_session(SessionState.RUNNING, created_at=created, last_activity=created)

    info = SessionInfo.from_session(s)
    assert info.elapsed_seconds == pytest.approx(time.time() - created.timestamp(), abs=1)


def test_completed_uses_last_activity():
    created = datetime(2026, 1, 1, 0, 0, 0)
    last = datetime(2026, 1, 1, 1, 0, 0)
    s = _fake_session(SessionState.COMPLETED, created_at=created, last_activity=last)

    info = SessionInfo.from_session(s)
    assert info.elapsed_seconds == 3600.0


def test_stopped_uses_last_activity():
    created = datetime(2026, 1, 1, 0, 0, 0)
    last = datetime(2026, 1, 1, 0, 30, 0)
    s = _fake_session(SessionState.STOPPED, created_at=created, last_activity=last)

    info = SessionInfo.from_session(s)
    assert info.elapsed_seconds == 1800.0


def test_paused_uses_last_activity():
    created = datetime(2026, 1, 1, 0, 0, 0)
    last = datetime(2026, 1, 1, 0, 10, 0)
    s = _fake_session(SessionState.PAUSED, created_at=created, last_activity=last)

    info = SessionInfo.from_session(s)
    assert info.elapsed_seconds == 600.0


def test_running_frozen_last_activity_still_counts():
    """Simulates a long tool execution: last_activity is 60s stale, elapsed should
    still reflect wall-clock time (not frozen at last_activity).
    """
    created = datetime.now(tz=UTC)
    stale_last = datetime.now(tz=UTC)
    s = _fake_session(SessionState.RUNNING, created_at=created, last_activity=stale_last)
    info = SessionInfo.from_session(s)
    assert info.elapsed_seconds == pytest.approx(time.time() - created.timestamp(), abs=1)


def test_birth_traceback_populated_from_session():
    """SessionInfo.from_session must expose plugin_metadata (fork origin) for
    fork visualization (设计 agent-traceback §5.3 / §7.2).
    """
    created = datetime.now(tz=UTC)
    s = _fake_session(SessionState.COMPLETED, created_at=created, last_activity=created)
    s.plugin_metadata = {
        "agent_traceback": {
            "birth_traceback": {
                "origin_session_id": "abc123",
                "origin_checkpoint_id": "cp_abc123_001",
                "forked_at": "2026-07-13T10:00:00",
            },
        },
    }
    info = SessionInfo.from_session(s)
    bt = info.plugin_metadata.get("agent_traceback", {}).get("birth_traceback")
    assert bt is not None
    assert bt["origin_session_id"] == "abc123"
    assert bt["origin_checkpoint_id"] == "cp_abc123_001"
    assert bt["forked_at"] == "2026-07-13T10:00:00"


def test_birth_traceback_none_when_not_set():
    """SessionInfo.plugin_metadata is empty for non-fork sessions."""
    created = datetime.now(tz=UTC)
    s = _fake_session(SessionState.IDLE, created_at=created, last_activity=created)
    s.plugin_metadata = {}
    info = SessionInfo.from_session(s)
    assert not info.plugin_metadata


def test_session_info_exposes_last_error(monkeypatch):
    created = datetime.now(tz=UTC)
    s = _fake_session(SessionState.FAILED, created_at=created, last_activity=created)
    s.last_error = "No available provider after rate limit"
    monkeypatch.setattr(s, "_error", None)

    info = SessionInfo.from_session(s)

    assert info.error == "No available provider after rate limit"


def test_session_api_projects_effective_reviewer_mode(monkeypatch):
    inherited = Session(config=SessionConfig(name="inherited"))
    automatic = Session(config=SessionConfig(name="automatic", reviewer=SessionReviewerConfig()))
    fallback = Session(config=SessionConfig(name="fallback", reviewer=SessionReviewerConfig()))
    fallback.waiting_for_auto_reply_fallback = True
    sessions = {session.id: session for session in (inherited, automatic, fallback)}
    orch = MagicMock()
    orch.pool.list_sessions.return_value = list(sessions.values())
    orch.pool.get.side_effect = sessions.get
    api = SessionAPI(orch, None)
    monkeypatch.setattr(api, "_resolve_session_name", lambda sid: sid)
    monkeypatch.setattr(api, "_populate_chain", lambda info, sid: None)
    monkeypatch.setattr(api, "_sync_session_name", lambda sid: None)

    for mode in ("manual", "auto"):
        orch.reviewer_mode = mode
        assert [info.reviewer_mode for info in api.list_sessions()] == [mode, "auto", "manual"]
        assert api.get_session(inherited.id).reviewer_mode == mode
        assert api.get_session(automatic.id).reviewer_mode == "auto"
        assert api.get_session(fallback.id).reviewer_mode == "manual"


def test_session_info_exposes_reviewer_task_goal():
    created = datetime.now(tz=UTC)
    s = _fake_session(SessionState.IDLE, created_at=created, last_activity=created)
    s.config.reviewer = MagicMock(
        mode="auto",
        enabled=True,
        task_goal="完成任务并通过验收",
    )

    info = SessionInfo.from_session(s)

    assert info.reviewer_task_goal == "完成任务并通过验收"


def test_session_info_reviewer_task_goal_missing_for_old_snapshot():
    created = datetime.now(tz=UTC)
    s = _fake_session(SessionState.IDLE, created_at=created, last_activity=created)
    s.config.reviewer = MagicMock(spec=["mode", "enabled"])

    info = SessionInfo.from_session(s)

    assert info.reviewer_task_goal is None


def test_list_sessions_reads_recent_events_for_nonterminal_members(monkeypatch):
    now = datetime.now(tz=UTC)
    idle = _fake_session(SessionState.IDLE, created_at=now, last_activity=now)
    idle.stop_reason = "max_iterations"
    paused_breakpoint = _fake_session(SessionState.IDLE, created_at=now, last_activity=now)
    paused_breakpoint.stop_reason = "user_pause"
    paused_breakpoint.id = "paused-breakpoint-1"
    paused = _fake_session(SessionState.PAUSED, created_at=now, last_activity=now)
    paused.id = "paused-1"
    running = _fake_session(SessionState.RUNNING, created_at=now, last_activity=now)
    running.id = "running-1"
    idle.id = "idle-1"
    orch = MagicMock()
    orch.pool.list_sessions.return_value = [idle, paused_breakpoint, paused, running]
    orch.event_router.get_recent_events_sync.return_value = []
    api = SessionAPI(orch, None)

    def _resolve_session_name(sid):
        return sid

    def _populate_chain(info, sid):
        return None

    monkeypatch.setattr(api, "_resolve_session_name", _resolve_session_name)
    monkeypatch.setattr(api, "_populate_chain", _populate_chain)

    result = api.list_sessions()

    assert [info.id for info in result] == [
        "idle-1",
        "paused-breakpoint-1",
        "paused-1",
        "running-1",
    ]
    queried = {call.args[0] for call in orch.event_router.get_recent_events_sync.call_args_list}
    assert queried == {"paused-breakpoint-1", "paused-1", "running-1"}
    assert all(
        call.args[1] == 10 for call in orch.event_router.get_recent_events_sync.call_args_list
    )


def test_get_session_restores_discarded_history_without_deleting_ledger(monkeypatch):
    pool = SessionPool()
    router = MagicMock()
    router.get_session_sync.return_value = {
        "id": "historic-1",
        "name": "historic",
        "role": "worker",
        "config_json": json.dumps(SessionConfig(name="historic").model_dump()),
        "state": "idle",
    }
    pool.set_event_router(router)
    assert getattr(pool, "_restore_session_from_entry")(router.get_session_sync.return_value)
    assert pool.discard("historic-1")
    assert pool.get("historic-1") is None
    orch = MagicMock()
    orch.pool = pool
    api = SessionAPI(orch, None)

    def _resolve_session_name(sid):
        return sid

    def _populate_chain(info, sid):
        return None

    monkeypatch.setattr(api, "_resolve_session_name", _resolve_session_name)
    monkeypatch.setattr(api, "_populate_chain", _populate_chain)

    info = api.get_session("historic-1")

    assert info is not None and info.id == "historic-1"
    # 按需恢复读后即弃：详情读不改变池驻留，反复查看不把内存池涨回去
    assert pool.get("historic-1") is None
    assert api.get_session("historic-1") is not None
    router.get_session_sync.assert_called_with("historic-1")
    # 读路径只恢复不删账：持久行始终可再读
    assert router.get_session_sync.return_value["id"] == "historic-1"


def test_get_session_restored_terminal_state_is_not_idle(monkeypatch):
    """按需恢复保留持久化终态：DiscardSession 清出的完成成员显示 completed。"""
    pool = SessionPool()
    router = MagicMock()
    router.get_session_sync.return_value = {
        "id": "done-1",
        "name": "done",
        "role": "worker",
        "config_json": json.dumps(SessionConfig(name="done").model_dump()),
        "state": "completed",
    }
    pool.set_event_router(router)
    orch = MagicMock()
    orch.pool = pool
    api = SessionAPI(orch, None)

    def _resolve_session_name(sid):
        return sid

    def _populate_chain(info, sid):
        return None

    monkeypatch.setattr(api, "_resolve_session_name", _resolve_session_name)
    monkeypatch.setattr(api, "_populate_chain", _populate_chain)

    info = api.get_session("done-1")

    assert info is not None and info.state == "completed"
    assert pool.get("done-1") is None


def test_restore_from_store_normalizes_nonterminal_state():
    """非终态持久行按需恢复仍归一 idle（无 loop 附着，同 boot 语义）。"""
    pool = SessionPool()
    router = MagicMock()
    router.get_session_sync.return_value = {
        "id": "paused-1",
        "name": "paused",
        "role": "worker",
        "config_json": json.dumps(SessionConfig(name="paused").model_dump()),
        "state": "running",
    }
    pool.set_event_router(router)

    session = pool.restore_from_store("paused-1")

    assert session is not None and session.state is SessionState.IDLE


def _orch_with_config(cfg: AppConfig) -> MagicMock:
    """Minimal mock orchestrator: real config + AsyncMock start_session_with_config
    so we can introspect the SessionConfig it was handed.
    """
    orch = MagicMock()
    orch.config = cfg
    orch.start_session_with_config = AsyncMock(return_value=MagicMock(id="s1"))
    return orch


def test_start_loads_project_dir_yaml(tmp_path):
    """选了 project_dir 后，start 读该目录 workflow-controller.yaml，用其 runtime/loop/
    system_prompt/starting_prompt/reviewer.task_goal 填该 session（web「选目录即
    用该目录配置」的正解）。
    """
    (tmp_path / "workflow-controller.yaml").write_text(
        "project_dir: .\n"
        "runtime: claude_cli\n"
        "system_prompt: 'AUQ gate discipline'\n"
        "starting_prompt: 'build the calc'\n"
        "context_threshold_pct: 10\n"
        "subagent_context_threshold_pct: 6\n"
        "context_window: 1000000\n"
        "loop: {evaluator: passthrough, max_iterations: 5}\n"
        "reviewer: {enabled: true, task_goal: 'calc feature done'}\n",
        encoding="utf-8",
    )
    orch = _orch_with_config(AppConfig(project_dir=str(tmp_path)))
    api = SessionAPI(orch, None)
    result = asyncio.run(api.start(str(tmp_path)))
    cfg = orch.start_session_with_config.call_args.args[0]
    assert cfg.runtime == "claude_cli"
    assert cfg.system_prompt == "AUQ gate discipline"
    assert cfg.initial_prompt == "build the calc"
    assert cfg.context_threshold_pct == 10
    assert cfg.subagent_context_threshold_pct == 6
    assert cfg.context_window == 1000000
    assert cfg.loop is not None and cfg.loop.evaluator == "passthrough"
    assert cfg.loop.max_iterations == 5
    assert cfg.criteria["task_goal"] == "calc feature done"
    assert cfg.cwd == str(tmp_path)
    assert result["session_id"] == "s1"


def test_start_no_yaml_falls_back_to_global(tmp_path):
    """project_dir 下无 workflow-controller.yaml → 维持 web 启动时的全局 config。"""
    global_cfg = AppConfig(
        project_dir=str(tmp_path),
        runtime="claude_sdk",
        system_prompt="GLOBAL",
        starting_prompt="global prompt",
    )
    global_cfg.loop.evaluator = "always_complete"
    global_cfg.loop.max_iterations = 7
    global_cfg.reviewer.task_goal = "global goal"
    orch = _orch_with_config(global_cfg)
    api = SessionAPI(orch, None)
    asyncio.run(api.start(str(tmp_path)))  # tmp_path 无 yaml
    cfg = orch.start_session_with_config.call_args.args[0]
    assert cfg.runtime == "claude_sdk"
    assert cfg.system_prompt == "GLOBAL"
    assert cfg.initial_prompt == "global prompt"
    assert cfg.loop.evaluator == "always_complete"
    assert cfg.loop.max_iterations == 7
    assert cfg.criteria["task_goal"] == "global goal"


def test_start_reviewer_enabled_override_is_session_local(tmp_path):
    global_cfg = AppConfig(project_dir=str(tmp_path))
    global_cfg.reviewer.enabled = True
    orch = _orch_with_config(global_cfg)
    api = SessionAPI(orch, None)
    asyncio.run(api.start(str(tmp_path), reviewer=ReviewerStartOverrides(enabled=False)))
    cfg = orch.start_session_with_config.call_args.args[0]
    assert cfg.reviewer.enabled is False
    assert orch.config.reviewer.enabled is True


def test_start_freezes_resolved_reviewer_thinking(tmp_path):
    (tmp_path / "workflow-controller.yaml").write_text(
        "reviewer: {thinking_mode: disabled, thinking_budget_tokens: 3072, max_tokens: 4096}\n",
        encoding="utf-8",
    )
    global_cfg = AppConfig(project_dir=str(tmp_path))
    global_cfg.reviewer.thinking_mode = "enabled"
    global_cfg.reviewer.max_tokens = 8192
    orch = _orch_with_config(global_cfg)
    api = SessionAPI(orch, None)
    asyncio.run(api.start(str(tmp_path)))
    reviewer = orch.start_session_with_config.call_args.args[0].reviewer
    assert reviewer.thinking_mode == "disabled"
    assert reviewer.thinking_budget_tokens == 3072
    assert reviewer.max_tokens == 4096

    asyncio.run(
        api.start(
            str(tmp_path),
            reviewer=ReviewerStartOverrides(
                thinking_mode="enabled",
                thinking_budget_tokens=8192,
                max_tokens=16384,
            ),
        )
    )
    reviewer = orch.start_session_with_config.call_args.args[0].reviewer
    assert reviewer.thinking_mode == "enabled"
    assert reviewer.thinking_budget_tokens == 8192
    assert reviewer.max_tokens == 16384


def test_start_rejects_unknown_cli_wrapper_before_session_creation(tmp_path):
    """无效 wrapper 在创建会话前失败。"""
    orch = _orch_with_config(AppConfig(project_dir=str(tmp_path)))
    with pytest.raises(ValueError, match="Unsupported CLI wrapper"):
        asyncio.run(SessionAPI(orch, None).start(str(tmp_path), cli_wrapper="bogus"))
    orch.start_session_with_config.assert_not_called()


@pytest.mark.parametrize(
    "chosen,pinned",
    [
        ("provider-model", False),
        ("custom-model", True),
    ],
)
def test_start_records_whether_model_is_inherited(tmp_path, chosen, pinned):
    from types import SimpleNamespace

    orch = _orch_with_config(AppConfig(project_dir=str(tmp_path)))
    orch.auth_rotator.get_provider.return_value = SimpleNamespace(model="provider-model")
    asyncio.run(SessionAPI(orch, None).start(str(tmp_path), model=chosen))
    cfg = orch.start_session_with_config.call_args.args[0]
    assert cfg.model == chosen
    assert cfg.model_pinned is pinned


def test_start_does_not_bind_default_codex_provider(tmp_path):
    orch = _orch_with_config(AppConfig(project_dir=str(tmp_path)))
    orch.auth_rotator.get_default_provider_index.return_value = 0

    asyncio.run(
        SessionAPI(orch, None).start(
            str(tmp_path),
            runtime="codex_cli",
            provider_index=None,
        )
    )

    cfg = orch.start_session_with_config.call_args.args[0]
    assert cfg.provider_index is None


def test_start_syncs_discovery_dirs_and_populates_config(tmp_path):
    """项目 workflow-controller.yaml 声明 skills_path/agents_path → start 把它们 overlay-sync 进
    runtime 的原生发现目录（CC→.claude/、opencode→.opencode/），并灌进 SessionConfig。
    """
    skills_src = tmp_path / "skills-src"
    (skills_src / "calc").mkdir(parents=True)
    (skills_src / "calc" / "SKILL.md").write_text("name: calc", encoding="utf-8")
    agents_src = tmp_path / "agents-src"
    agents_src.mkdir()
    (agents_src / "bot.md").write_text("---\nname: bot\n---\nhi", encoding="utf-8")

    (tmp_path / "workflow-controller.yaml").write_text(
        "project_dir: .\n"
        "runtime: claude_sdk\n"
        f"skills_path: {skills_src}\n"
        f"agents_path: {agents_src}\n",
        encoding="utf-8",
    )
    orch = _orch_with_config(AppConfig(project_dir=str(tmp_path)))
    api = SessionAPI(orch, None)
    asyncio.run(api.start(str(tmp_path)))

    # 发现目录已填充（CC runtime → .claude/）
    assert (tmp_path / ".claude" / "skills" / "calc" / "SKILL.md").exists()
    assert (tmp_path / ".claude" / "agents" / "bot.md").exists()
    # SessionConfig 带上了两个字段（此前被漏掉，字段有值也不进 live session）
    cfg = orch.start_session_with_config.call_args.args[0]
    assert cfg.skills_path == str(skills_src)
    assert cfg.agents_path == str(agents_src)


# ── delete 清声明的 stage 产物 ───────────────────────────────────────────
# delete session 时清理该 session 在 workflow-controller.yaml 声明的 stage 产物（= 门禁查的那批
# 文件/目录），让 delete→start 从干净态重跑，不再被旧产物顶到完成态。只清声明的、
# 且必须落在 cwd 内（防 .. 逃逸）；未声明的文件不动。


def _orch_for_delete(cwd: str, stages: list) -> tuple[MagicMock, MagicMock, MagicMock]:
    """Mock orchestrator: pool.get 返回带 config.cwd/stages 的 session;stop_session 是
    AsyncMock(delete 必须走它 reap worker,而非直接 pool.stop);pool.remove /
    event_router.delete_session 是 MagicMock。返回 (orch, pool, er)。
    """
    session = MagicMock()
    session.id = "s1"
    session.config.cwd = cwd
    session.config.stages = stages
    pool = MagicMock()
    pool.get = MagicMock(return_value=session)
    pool.stop = AsyncMock()
    pool.remove = MagicMock(return_value=True)
    er = MagicMock()
    er.delete_session = MagicMock()
    orch = MagicMock()
    orch.pool = pool
    orch.event_router = er
    orch.affiliation.resolve.return_value = None
    orch.stop_session = AsyncMock()
    orch.bus.emit = AsyncMock()  # SessionAPI.delete awaits bus.emit("session_delete")
    orch.reclaim_deleted_session_affiliation = AsyncMock()  # delete 在名册删除同点回收归属
    return orch, pool, er


async def test_reviewer_task_goal_update_bypasses_workflow_member_guard():
    """reviewer 任务目标更新不套用 workflow 成员的写者守卫，原样转调 orchestrator。"""
    orch = MagicMock()
    orch.affiliation.resolve.return_value = MagicMock(run_id="workflow-1")
    orch.run_store.get = AsyncMock(return_value=MagicMock(kind=RunKind.WORKFLOW))
    orch.set_session_reviewer_task_goal = AsyncMock(return_value="新目标")
    api = SessionAPI(orch)

    with pytest.raises(WorkflowSessionOperationError, match="workflow-1"):
        await api.pause("s1")
    assert await api.set_session_reviewer_task_goal("s1", " 新目标 ") == "新目标"

    orch.set_session_reviewer_task_goal.assert_awaited_once_with("s1", " 新目标 ")


async def test_delete_rejects_workflow_member_before_cleanup(tmp_path):
    """拒绝删除时保留会话、事件和阶段产物。"""
    artifact = tmp_path / "spec.md"
    artifact.write_text("spec", encoding="utf-8")
    orch, pool, er = _orch_for_delete(str(tmp_path), [{"name": "spec", "artifact": "spec.md"}])
    orch.affiliation.resolve.return_value = MagicMock(run_id="workflow-1")
    orch.run_store.get = AsyncMock(return_value=MagicMock(kind=RunKind.WORKFLOW))

    with pytest.raises(WorkflowSessionOperationError, match="workflow-1"):
        await SessionAPI(orch).delete("s1")

    orch.stop_session.assert_not_awaited()
    pool.remove.assert_not_called()
    orch.reclaim_deleted_session_affiliation.assert_not_awaited()
    orch.bus.emit.assert_not_awaited()
    er.delete_session.assert_not_called()
    assert artifact.read_text(encoding="utf-8") == "spec"


def test_delete_stops_worker_via_full_stop_path(tmp_path):
    """delete 必须走 orchestrator.stop_session（engine.request_stop + pool.stop + driver.kill
    整套 reap），不能只调 pool.stop——后者不设 loop engine 的 _stop_event，worker 子进程被杀后
    会被下一轮 iteration 重新拉起，表现为 delete 后面板仍一直有输出。
    """
    (tmp_path / "spec.md").write_text("spec", encoding="utf-8")
    stages = [{"name": "spec", "artifact": "spec.md"}]
    orch, pool, er = _orch_for_delete(str(tmp_path), stages)
    asyncio.run(SessionAPI(orch, None).delete("s1"))
    orch.stop_session.assert_awaited_once_with("s1")  # 完整停 worker 路径
    pool.stop.assert_not_awaited()  # 不再绕过 stop_session 直接停
    pool.remove.assert_called_once_with("s1")
    er.delete_session.assert_called_once_with("s1")


def test_delete_cleans_declared_stage_artifacts(tmp_path):
    """delete 删除声明的 stage 产物(文件 + 目录)，再 stop/remove/清 events。"""
    (tmp_path / "spec.md").write_text("spec content", encoding="utf-8")
    (tmp_path / "impl").mkdir()
    (tmp_path / "impl" / "conv.py").write_text("code", encoding="utf-8")
    stages = [
        {"name": "spec", "artifact": "spec.md"},
        {"name": "execute", "artifact": "impl/"},
    ]
    orch, pool, er = _orch_for_delete(str(tmp_path), stages)
    asyncio.run(SessionAPI(orch, None).delete("s1"))
    assert not (tmp_path / "spec.md").exists()
    assert not (tmp_path / "impl").exists()
    orch.stop_session.assert_awaited_once_with("s1")  # 走完整停 worker 路径，不只 pool.stop
    pool.remove.assert_called_once_with("s1")
    er.delete_session.assert_called_once_with("s1")


def test_delete_preserves_undeclared_files(tmp_path):
    """未在 stages 声明的文件(如 README.md)不被 delete 碰。"""
    (tmp_path / "spec.md").write_text("spec", encoding="utf-8")
    (tmp_path / "README.md").write_text("readme", encoding="utf-8")  # 未声明
    stages = [{"name": "spec", "artifact": "spec.md"}]
    orch, _pool, _er = _orch_for_delete(str(tmp_path), stages)
    asyncio.run(SessionAPI(orch, None).delete("s1"))
    assert not (tmp_path / "spec.md").exists()
    assert (tmp_path / "README.md").exists()


def test_delete_no_stages_cleans_nothing(tmp_path):
    """没声明 stages 的 session → delete 不碰磁盘(只 stop/remove/清 events)。"""
    (tmp_path / "spec.md").write_text("spec", encoding="utf-8")
    orch, _pool, _er = _orch_for_delete(str(tmp_path), [])
    asyncio.run(SessionAPI(orch, None).delete("s1"))
    assert (tmp_path / "spec.md").exists()


def test_delete_missing_session_is_safe(tmp_path):
    """pool.get 返回 None(无此 session)→ 不崩,仍 stop/remove/清 events。"""
    pool = MagicMock()
    pool.get = MagicMock(return_value=None)
    pool.stop = AsyncMock()
    pool.remove = MagicMock(return_value=False)
    er = MagicMock()
    er.delete_session = MagicMock()
    orch = MagicMock()
    orch.pool = pool
    orch.event_router = er
    orch.affiliation.resolve.return_value = None
    orch.stop_session = AsyncMock()
    orch.bus.emit = AsyncMock()  # SessionAPI.delete awaits bus.emit("session_delete")
    orch.reclaim_deleted_session_affiliation = AsyncMock()
    asyncio.run(SessionAPI(orch, None).delete("nope"))
    orch.stop_session.assert_awaited_once_with("nope")  # 即使 session 不在 pool，也走完整 stop
    er.delete_session.assert_called_once_with("nope")


def test_delete_reclaims_run_affiliation_at_roster_removal(tmp_path):
    """delete 在 pool.remove 同点回收 run 归属——账面不留孤儿行。"""
    orch, _pool, _er = _orch_for_delete(str(tmp_path), [])
    asyncio.run(SessionAPI(orch, None).delete("s1"))
    orch.reclaim_deleted_session_affiliation.assert_awaited_once_with("s1")


def test_delete_skips_out_of_cwd_artifact(tmp_path):
    """artifact 用 .. 逃出 cwd → 跳过(不删 cwd 外的文件),不报错。"""
    work = tmp_path / "work"
    work.mkdir()
    secret = tmp_path / "secret.md"  # 在 work 之外
    secret.write_text("keep me", encoding="utf-8")
    stages = [{"name": "x", "artifact": "../secret.md"}]  # 从 work 逃到 tmp_path
    orch, _pool, _er = _orch_for_delete(str(work), stages)
    asyncio.run(SessionAPI(orch, None).delete("s1"))
    assert secret.exists()  # 逃逸路径被 skip,没删


@pytest.mark.parametrize("bad", ["bogus", "OPS", "all2", ""])
def test_delete_rejects_invalid_clean_reconstruct(bad, tmp_path):
    """clean_reconstruct 仅接受 None|'workflow_controller'|"all"；非法值抛 ValueError（直调保护）。"""
    orch, _pool, _er = _orch_for_delete(str(tmp_path), [])
    with pytest.raises(ValueError):
        asyncio.run(SessionAPI(orch, None).delete("s1", clean_reconstruct=bad))


def test_session_info_exposes_run_id_projection():
    """SessionInfo 增 run_id 投影：消费面免 join 看到 run 归属。"""
    s = _fake_session(
        SessionState.RUNNING,
        created_at=datetime.now(tz=UTC),
        last_activity=datetime.now(tz=UTC),
    )
    s.config = SessionConfig(name="n", run_id="run-abc123")
    assert SessionInfo.from_session(s).run_id == "run-abc123"

    s.config = SessionConfig(name="n2")
    assert SessionInfo.from_session(s).run_id is None
