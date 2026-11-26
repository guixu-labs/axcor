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

"""TracebackRunner 集成测试（设计 agent-traceback §6）。

不启真实 driver：mock 掉 Orchestrator._activate_session（避免 spawn opencode），
验证重放编排链路：checkpoint 查找 → 快照 restore → L2 读取 → seed 预制 session
→ 新 session 带 birth_traceback + resume。
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest

from workflow_controller.config.settings import (
    AppConfig,
    HookServerConfig,
    SessionConfig,
)
from workflow_controller.config.settings import (
    ReviewerConfig as _ReviewerConfig,
)
from workflow_controller.core.orchestrator import Orchestrator
from workflow_controller.plugins.agent_traceback.checkpoint_store import Checkpoint
from workflow_controller.plugins.agent_traceback.claude_session_seed import _project_key
from workflow_controller.plugins.agent_traceback.traceback_runner import TracebackRunner

from ._fixtures import build_opencode_db
from ._fixtures import (
    opencode_assistant_message_row as _asst_msg,
)
from ._fixtures import (
    opencode_done_task_part_row as _task_part,
)
from ._fixtures import (
    opencode_text_part_row as _text_part,
)
from ._fixtures import (
    opencode_user_message_row as _user_msg,
)

if TYPE_CHECKING:
    from workflow_controller.plugins.agent_traceback.plugin import AgentTracebackPlugin

_HAS_GIT = shutil.which("git") is not None
pytestmark = pytest.mark.skipif(not _HAS_GIT, reason="git not installed")


def _make_opencode_db(xdg_root: Path, *, sessions, parts, messages=None) -> Path:
    """假 opencode.db（session 表用 traceback 重放子集列）。"""
    return build_opencode_db(
        xdg_root,
        sessions=sessions,
        parts=parts,
        messages=messages,
        traceback_session_schema=True,
    )


async def _origin_session(orch, cwd: Path, *, stop_reason: str | None = "reviewer_complete"):
    """建一个已结束的 origin worker 会话（sdk 会话 ses_orig_main）。"""
    origin = await orch.pool.create(SessionConfig(name="orig", role="worker", cwd=str(cwd)))
    origin.native_session_id = "ses_orig_main"
    origin.loop_status = "no"
    origin.stop_reason = stop_reason
    return origin


def _checkpoint_for(
    origin,
    git_ref: str | None,
    *,
    cp_suffix: str = "001",
    iteration: int = 2,
    agent_id: str = "ses_sub_x",
    agent_type: str = "developer",
    trigger: str = "subagent_stop",
    event_rowid: int | None = 1,
    main_part_id: str | None = None,
    created_at: str = "2026-07-13T00:00:00",
    file_count: int = 1,
    byte_size: int = 10,
    status: str = "ok",
) -> Checkpoint:
    """检查点构造（checkpoint_id 与实时写入的 cp_<sid[:8]>_<n> 格式一致）。"""
    return Checkpoint(
        checkpoint_id=f"cp_{origin.id[:8]}_{cp_suffix}",
        session_id=origin.id,
        iteration=iteration,
        agent_id=agent_id,
        agent_type=agent_type,
        trigger=trigger,
        git_ref=git_ref,
        event_rowid=event_rowid,
        main_part_id=main_part_id,
        parent_checkpoint_id=None,
        created_at=created_at,
        file_count=file_count,
        byte_size=byte_size,
        status=status,
    )


async def _store_checkpoint(o: Orchestrator, cp: Checkpoint) -> None:
    """把检查点写入其 session 对应的 CheckpointStore。"""
    await getattr(_get_plugin(o), "_cp_store_for_session")(cp.session_id).store(cp)


def _seed_origin_db(xdg_root: Path, cwd, parts: list, messages: list) -> None:
    """播种 ses_orig_main 主会话的标准假 opencode.db（L2 + seed 数据源）。"""
    _make_opencode_db(
        xdg_root,
        sessions=[
            {
                "id": "ses_orig_main",
                "title": "orig",
                "directory": str(cwd),
                "time_created": 100,
                "time_updated": 100,
                "agent": "worker",
                "model": "m",
            },
        ],
        parts=parts,
        messages=messages,
    )


def _three_part_stream() -> tuple[list, list]:
    """p1 text → p2 task → p3 text 的标准 3-part 对话流（含 message 链）。"""
    return (
        [
            _text_part("p1", "ses_orig_main", 1, "before"),
            _task_part("p2", "ses_orig_main", 2, "ses_sub_x"),
            _text_part("p3", "ses_orig_main", 3, "after checkpoint"),
        ],
        [
            _user_msg("m_p1", "ses_orig_main", 1),
            _asst_msg("m_p2", "ses_orig_main", 2, parent_id="m_p1"),
            _user_msg("m_p3", "ses_orig_main", 3),
        ],
    )


def _two_part_stream() -> tuple[list, list]:
    """p1 text → p2 task 的最小 2-part 对话流（含 message 链）。"""
    return (
        [
            _text_part("p1", "ses_orig_main", 1, "before"),
            _task_part("p2", "ses_orig_main", 2, "ses_sub_x"),
        ],
        [
            _user_msg("m_p1", "ses_orig_main", 1),
            _asst_msg("m_p2", "ses_orig_main", 2, parent_id="m_p1"),
        ],
    )


def _shared_message_stream() -> tuple[list, list]:
    """p2(text) 与 p3(task) 同属 message m_b 的 part/message 流（message 级截断用例）。"""
    return (
        [
            _text_part("p1", "ses_orig_main", 1, "before"),
            (
                "p2",
                "m_b",
                "ses_orig_main",
                2,
                2,
                json.dumps({"type": "text", "text": "I'll call X"}),
            ),
            # task part p3 shares message m_b (same message_id as p2)
            (
                "p3",
                "m_b",
                "ses_orig_main",
                3,
                3,
                json.dumps(
                    {
                        "type": "tool",
                        "tool": "task",
                        "callID": "c_p3",
                        "state": {
                            "status": "completed",
                            "input": {},
                            "output": "<task_result session_id='ses_sub_x'>done</task_result>",
                        },
                    }
                ),
            ),
            _text_part("p4", "ses_orig_main", 4, "after"),
        ],
        [
            _user_msg("m_p1", "ses_orig_main", 1),
            (
                "m_b",
                "ses_orig_main",
                2,
                2,
                json.dumps({"role": "assistant", "parentID": "m_p1"}),
            ),
            _user_msg("m_p4", "ses_orig_main", 4),
        ],
    )


def _expected_birth_traceback(new_bt: dict, origin, cp, *, origin_cwd: str) -> dict:
    """runner 应写入的 birth_traceback（动态字段取自实测值）。"""
    return {
        "origin_session_id": origin.id,
        "origin_checkpoint_id": cp.checkpoint_id,
        "forked_at": new_bt["forked_at"],
        "fork_root": new_bt["fork_root"],
        "fork_cwd": new_bt["fork_cwd"],
        "origin_cwd": origin_cwd,
        "origin_name": "orig",
    }


async def _run_with_overrides(runner, origin, cp, overrides: dict) -> dict:
    """带 overrides 的标准重放调用（统一三个 reviewer 覆盖用例的形态）。"""
    return await runner.run(origin.id, cp.checkpoint_id, runtime="opencode", overrides=overrides)


def _git_test_env() -> dict[str, str]:
    """git 测试身份（不依赖宿主全局 git config）。"""
    return {
        "GIT_AUTHOR_NAME": "orig",
        "GIT_AUTHOR_EMAIL": "orig@local",
        "GIT_COMMITTER_NAME": "orig",
        "GIT_COMMITTER_EMAIL": "orig@local",
    }


def _seed_origin_git_repo(repo: Path) -> str:
    """把 origin cwd 做成 git 仓并打一个可识别的 baseline commit，返回 HEAD sha。"""
    _env = _git_test_env()
    subprocess.run(["git", "init"], cwd=str(repo), capture_output=True, env=_env)
    (repo / "README.md").write_text("# origin project\n")
    subprocess.run(["git", "add", "-A"], cwd=str(repo), capture_output=True, env=_env)
    subprocess.run(
        ["git", "commit", "-m", "origin baseline: initial README"],
        cwd=str(repo),
        capture_output=True,
        env=_env,
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        capture_output=True,
        text=True,
        env=_env,
    ).stdout.strip()
    assert head
    return head


@pytest.fixture
def config(tmp_path):
    return AppConfig(
        project_dir=str(tmp_path),
        hook_server=HookServerConfig(enabled=False),
        reviewer=_ReviewerConfig(enabled=False),
    )


@pytest.fixture
async def orch(config, tmp_path, monkeypatch):
    """Orchestrator with _activate_session mocked (no driver spawn)."""
    o = Orchestrator(config, default_project_dir=tmp_path / "test.db")
    await o.start()
    # Prevent real driver spawn on activation.
    monkeypatch.setattr(o, "_activate_session", AsyncMock())
    # Install the agent_traceback plugin (enabled, generous snapshot timeout).
    from ._plugin_harness import install_plugin

    install_plugin(o)
    yield o
    await o.shutdown()


def _get_plugin(o: Orchestrator) -> AgentTracebackPlugin:
    return next(p for p in o.plugins if p.config.name == "agent_traceback")  # type: ignore[return-value]


async def _seed_snapshot(o: Orchestrator, cwd: Path) -> str:
    """Take a real snapshot of cwd and return its git_ref."""
    (cwd / "spec.md").write_text("# spec v1\n")
    eng = getattr(_get_plugin(o), "_resolve_engine")(str(cwd))
    result = await eng.snapshot(
        session_id="orig",
        checkpoint_id="cp_orig_001",
        agent_id="ses_sub_x",
        agent_type="developer",
        iteration=2,
    )
    assert result.status == "ok"
    return result.git_ref  # type: ignore[return-value]


def _claude_replay_rows(session_id: str, cwd: str, now: datetime) -> list[dict]:
    """claude 私有 jsonl 的两行转录（user 建档 + assistant 派发 Agent）。"""
    return [
        {
            "type": "user",
            "sessionId": session_id,
            "cwd": cwd,
            "timestamp": (now - timedelta(seconds=2)).isoformat(),
            "message": {"content": "create files"},
        },
        {
            "type": "assistant",
            "sessionId": session_id,
            "cwd": cwd,
            "timestamp": (now - timedelta(milliseconds=100)).isoformat(),
            "message": {
                "content": [
                    {"type": "tool_use", "name": "Agent", "input": {"subagent_type": "developer"}}
                ]
            },
        },
    ]


async def _claude_cli_origin(orch, tmp_path: Path, monkeypatch):
    """claude_cli origin 装配：origin 会话 + 私有 jsonl 转录 + subagent_start 检查点。"""
    origin = await orch.pool.create(
        SessionConfig(
            name="claude-origin",
            role="worker",
            cwd=str(tmp_path),
            runtime="claude_cli",
        )
    )
    origin.native_session_id = "735b20da-1e5d-4a94-9ec9-f8f6670d20bf"
    origin.loop_status = "no"
    git_ref = await _seed_snapshot(orch, tmp_path)
    config_dir = tmp_path / "claude-config"
    transcript = (
        config_dir / "projects" / _project_key(str(tmp_path)) / f"{origin.native_session_id}.jsonl"
    )
    transcript.parent.mkdir(parents=True)
    now = datetime.now(tz=UTC).astimezone()
    rows = _claude_replay_rows(origin.native_session_id, str(tmp_path), now)
    transcript.write_text("".join(json.dumps(row) + "\n" for row in rows))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config_dir))
    cp = _checkpoint_for(
        origin,
        git_ref,
        iteration=1,
        agent_id="agent-x",
        trigger="subagent_start",
        event_rowid=None,
        created_at=now.isoformat(),
    )
    await _store_checkpoint(orch, cp)
    return origin, cp


async def test_claude_cli_replay_uses_private_jsonl_and_rejects_runtime_mismatch(
    orch,
    tmp_path,
    monkeypatch,
):
    origin, cp = await _claude_cli_origin(orch, tmp_path, monkeypatch)
    assert await _get_plugin(orch).replay_runtime(origin.id) == "claude_cli"
    result = await TracebackRunner(_get_plugin(orch)).run(origin.id, cp.checkpoint_id)
    assert result["ok"] is True
    fork = orch.pool.get(result["session_id"])
    assert fork.config.runtime == "claude_cli"
    assert fork.config.env_overrides["CLAUDE_CONFIG_DIR"].startswith(result["fork_cwd"])
    target = (
        Path(fork.config.env_overrides["CLAUDE_CONFIG_DIR"])
        / "projects"
        / _project_key(result["fork_cwd"])
        / f"{result['resume_native_session_id']}.jsonl"
    )
    seeded = [json.loads(line) for line in target.read_text().splitlines()]
    assert len(seeded) == 1
    assert seeded[0]["sessionId"] == result["resume_native_session_id"]
    mismatch = await TracebackRunner(_get_plugin(orch)).run(
        origin.id,
        cp.checkpoint_id,
        runtime="opencode",
    )
    assert mismatch["ok"] is False
    assert "Cannot replay claude_cli conversation with opencode" in mismatch["error"]


async def _replay_runner(
    orch,
    tmp_path,
    monkeypatch,
    *,
    parts=None,
    messages=None,
    cp_iteration: int = 2,
    cp_agent_id: str = "ses_sub_x",
    cp_agent_type: str = "developer",
    main_part_id: str | None = "p2",
    origin_loop=None,
    origin_cwd: Path | None = None,
    origin_session_overlays: bool = False,
):
    """参数化 helper：搭一套标准的「重派 X」检查点重放场景。

    构造 origin session + snapshot + 假 opencode.db + checkpoint，返回
    ``(origin, cp, runner)``，直接 ``await runner.run(...)`` 即可。调用方只传各测试
    不同的部分（``parts`` / ``cp_iteration`` / ``main_part_id`` / ``origin_loop`` / …），
    重复的 setup 由本 helper 统一承担。
    """
    cwd = origin_cwd or tmp_path
    cfg_kwargs: dict = {"name": "orig", "role": "worker", "cwd": str(cwd)}
    if origin_loop is not None:
        cfg_kwargs["loop"] = origin_loop
    origin = await orch.pool.create(SessionConfig(**cfg_kwargs))
    origin.native_session_id = "ses_orig_main"
    origin.loop_status = "no"
    origin.stop_reason = "reviewer_complete"
    git_ref = await _seed_snapshot(orch, cwd)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    default_parts, default_messages = _two_part_stream()
    if parts is None:
        parts = default_parts
    if messages is None:
        messages = default_messages
    _seed_origin_db(tmp_path / "xdg", cwd, parts, messages)
    cp = _checkpoint_for(
        origin,
        git_ref,
        iteration=cp_iteration,
        agent_id=cp_agent_id,
        agent_type=cp_agent_type,
        main_part_id=main_part_id,
    )
    await _store_checkpoint(orch, cp)
    if origin_session_overlays:
        # for overlay-inheritance test (not in the 4 refactored tests, kept for completeness)
        pass
    runner = TracebackRunner(_get_plugin(orch))
    return origin, cp, runner


def _append_nested_subagent_stream(fork_db: Path, sid: str) -> None:
    """向 fork1 的 opencode.db 追加 ses_sub_y 的 text+task part 与 message 链。"""
    con = sqlite3.connect(str(fork_db))
    con.execute(
        "INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)",
        (
            "fy_p1",
            "m_fy_p1",
            sid,
            10,
            10,
            json.dumps({"type": "text", "text": "fork1 work"}),
        ),
    )
    con.execute(
        "INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)",
        (
            "fy_p2",
            "m_fy_p2",
            sid,
            11,
            11,
            json.dumps(
                {
                    "type": "tool",
                    "tool": "task",
                    "callID": "c_fy_p2",
                    "state": {
                        "status": "completed",
                        "input": {},
                        "output": "<task_result session_id='ses_sub_y'>done</task_result>",
                    },
                }
            ),
        ),
    )
    con.execute(
        "INSERT INTO message VALUES (?, ?, ?, ?, ?)",
        ("m_fy_p1", sid, 10, 10, json.dumps({"role": "user"})),
    )
    con.execute(
        "INSERT INTO message VALUES (?, ?, ?, ?, ?)",
        ("m_fy_p2", sid, 11, 11, json.dumps({"role": "assistant", "parentID": "m_fy_p1"})),
    )
    con.commit()
    con.close()


async def _prepare_nested_replay(orch, fork1, fork1_cwd: Path, f1_sid: str) -> Checkpoint:
    """fork1 打快照 + 追加 ses_sub_y 流 + 落 cp2（嵌套重放 #2 的全部输入）。"""
    (fork1_cwd / "impl.py").write_text("def add(a,b): return a+b\n")
    eng1 = getattr(_get_plugin(orch), "_resolve_engine")(str(fork1_cwd))
    result1 = await eng1.snapshot(
        session_id=fork1.id,
        checkpoint_id=f"cp_{fork1.id[:8]}_001",
        agent_id="ses_sub_y",
        agent_type="tester",
        iteration=3,
    )
    assert result1.status == "ok"
    fork1_db = fork1_cwd / ".workflow-controller" / "opencode" / "opencode.db"
    _append_nested_subagent_stream(fork1_db, f1_sid)
    cp2 = _checkpoint_for(
        fork1, result1.git_ref, iteration=3, agent_id="ses_sub_y", agent_type="tester"
    )
    await _store_checkpoint(orch, cp2)
    return cp2


class TestTracebackRunner:
    async def test_run_seeds_and_resumes(self, orch, tmp_path, monkeypatch):
        # The orchestrator's _activate_session is mocked, so no real opencode is
        # spawned — this tests the seed+resume wiring end to end.
        # 1. origin session (done) with cwd + native_session_id
        origin = await orch.pool.create(
            SessionConfig(name="orig", role="worker", cwd=str(tmp_path))
        )
        origin.native_session_id = "ses_orig_main"
        origin.loop_status = "no"
        origin.stop_reason = "reviewer_complete"
        # 2. take a snapshot at tmp_path → git_ref
        git_ref = await _seed_snapshot(orch, tmp_path)
        # 3. fake opencode.db with the main session + task part (for L2 + seed)
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
        stream_parts, stream_messages = _three_part_stream()
        _seed_origin_db(tmp_path / "xdg", tmp_path, stream_parts, stream_messages)
        # 4. store a checkpoint pointing at git_ref, agent_id=ses_sub_x
        # main_part_id 留空 → forces find_main_part_id_for_subagent
        cp = _checkpoint_for(origin, git_ref)
        await _store_checkpoint(orch, cp)

        # 5. run traceback
        runner = TracebackRunner(_get_plugin(orch))
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")

        assert result["ok"] is True
        new_sid = result["session_id"]
        new_session = orch.pool.get(new_sid)
        assert new_session is not None
        new_bt = (
            (new_session.plugin_metadata or {}).get("agent_traceback", {}).get("birth_traceback")
        )
        assert new_bt == _expected_birth_traceback(new_bt, origin, cp, origin_cwd=str(tmp_path))
        # fork_root / fork_cwd persisted in plugin_metadata for lifecycle
        # cleanup + topology display.
        assert Path(new_bt["fork_root"]).is_dir()
        assert Path(new_bt["fork_cwd"]).is_dir()
        # seed set the native_session_id to the seeded id; _resume_next True
        assert new_session.native_session_id == result["resume_native_session_id"]
        assert getattr(new_session, "_resume_next") is True
        # fork cwd has the restored spec.md
        fork_cwd = Path(result["fork_cwd"])
        assert (fork_cwd / "spec.md").read_text() == "# spec v1\n"
        # config inherits runtime + stages
        assert new_session.config.runtime == "opencode"

    async def test_run_inherits_origin_session_overlays(self, orch, tmp_path, monkeypatch):
        # Regression: a 子代理补充指令 (subagent hint) + global instruction
        # registered on the ORIGIN session before the replay must reach the new
        # replay session so the re-called subagent's system prompt keeps the
        # user's diagnostic. Before the fix, start_session_with_resume only
        # transferred project-level (project-dir-keyed) overlays; session-scoped
        # overlays (keyed by origin session_id) were dropped → the driver's
        # get_subagent_hints() returned {} for the new session.
        origin = await _origin_session(orch, tmp_path)
        git_ref = await _seed_snapshot(orch, tmp_path)
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
        stream_parts, stream_messages = _three_part_stream()
        _seed_origin_db(tmp_path / "xdg", tmp_path, stream_parts, stream_messages)
        cp = _checkpoint_for(origin, git_ref, main_part_id="p2")
        await _store_checkpoint(orch, cp)

        # Register a subagent hint + a global session instruction on the ORIGIN
        # session BEFORE replaying.
        hint_text = "ORIGIN_HINT_MARKER: developer add(a,b) -> a*b"
        instr_text = "GLOBAL_INSTR_MARKER: be terse"
        orch.register_subagent_hint(origin.id, "developer", hint_text)
        orch.register_session_instruction(origin.id, instr_text)

        runner = TracebackRunner(_get_plugin(orch))
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        new_sid = result["session_id"]

        # The new replay session inherits the origin's overlays so the driver
        # picks them up on its first query round (re-called subagent's prompt).
        assert orch.get_subagent_hints(new_sid) == {"developer": [hint_text]}
        assert orch.get_session_instructions(new_sid) == [instr_text]
        # The driver's lazy hooks read the carried overlays off the new session.
        hooks = orch.make_driver_hooks(new_sid)
        assert hooks.get_overlays()["subagent_hints"] == {"developer": [hint_text]}
        assert hooks.get_overlays()["instructions"] == [instr_text]
        # Deep-copy: the origin session keeps its overlays (source not drained),
        # and clearing on the new session does not mutate the origin's bucket.
        assert orch.get_subagent_hints(origin.id) == {"developer": [hint_text]}
        assert orch.get_session_instructions(origin.id) == [instr_text]
        orch.clear_subagent_hint(new_sid, "developer")
        assert orch.get_subagent_hints(origin.id) == {"developer": [hint_text]}

    async def test_run_final_state_checkpoint_keeps_full_history(self, orch, tmp_path, monkeypatch):
        # A session-complete checkpoint (trigger=session_complete, no subagent)
        # replays keeping the FULL history (no task part removed) and instructs
        # "continue from the final state" instead of "re-dispatch subagent X".
        origin = await _origin_session(orch, tmp_path, stop_reason=None)
        git_ref = await _seed_snapshot(orch, tmp_path)
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
        _seed_origin_db(
            tmp_path / "xdg",
            tmp_path,
            [
                _text_part("p1", "ses_orig_main", 1, "intro"),
                _task_part("p2", "ses_orig_main", 2, "ses_sub_x"),
                _text_part("p3", "ses_orig_main", 3, "final wrap"),
            ],
            [
                _user_msg("m_p1", "ses_orig_main", 1),
                _asst_msg("m_p2", "ses_orig_main", 2, parent_id="m_p1"),
                _user_msg("m_p3", "ses_orig_main", 3),
            ],
        )
        # 无 subagent —— 终态检查点
        cp = _checkpoint_for(
            origin,
            git_ref,
            cp_suffix="002",
            agent_id="",
            agent_type="main",
            trigger="session_complete",
            event_rowid=None,
        )
        await _store_checkpoint(orch, cp)

        runner = TracebackRunner(_get_plugin(orch))
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        # seed ran: resume id set
        assert result["resume_native_session_id"]
        new_session = orch.pool.get(result["session_id"])
        assert new_session.native_session_id == result["resume_native_session_id"]
        assert getattr(new_session, "_resume_next") is True
        prompt = new_session.config.initial_prompt or ""
        # transparency: the replay prompt must not leak replay/checkpoint/re-dispatch
        # terminology to the worker (which would then leak it into subagent task prompts).
        for leak in ("重放", "检查点", "重新派发", "最终状态", "恢复", "移除"):
            assert leak not in prompt, f"replay prompt leaks '{leak}': {prompt!r}"
        assert "继续" in prompt  # a natural continuation prompt
        # the word "fork" must NOT appear in the agent-facing prompt (blocked)
        assert "fork" not in prompt.lower()
        # birth_traceback (via plugin_metadata) still links origin + checkpoint
        new_bt = (
            (new_session.plugin_metadata or {}).get("agent_traceback", {}).get("birth_traceback")
        )
        assert new_bt == _expected_birth_traceback(new_bt, origin, cp, origin_cwd=str(tmp_path))
        # fork_root / fork_cwd persisted in plugin_metadata for lifecycle
        # cleanup + topology display.
        assert Path(new_bt["fork_root"]).is_dir()
        assert Path(new_bt["fork_cwd"]).is_dir()

    async def test_run_seed_failure_returns_error(self, orch, tmp_path, monkeypatch):
        # If seed_prebuilt_session fails, traceback cannot proceed (no handoff
        # fallback) — it returns an error instead of starting a broken session.
        origin = await _origin_session(orch, tmp_path, stop_reason=None)
        git_ref = await _seed_snapshot(orch, tmp_path)
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
        stream_parts, stream_messages = _two_part_stream()
        _seed_origin_db(tmp_path / "xdg", tmp_path, stream_parts, stream_messages)
        # Force seed to fail by patching the name bound into traceback.py.
        import workflow_controller.plugins.agent_traceback.traceback_runner as tb_mod

        monkeypatch.setattr(tb_mod, "seed_prebuilt_session", lambda **kw: False)
        failed_fork_root = tmp_path / "workflow-controller-tb-seed-failure"

        def make_failed_fork_root(*, prefix):
            assert prefix == "workflow-controller-tb-"
            failed_fork_root.mkdir()
            return str(failed_fork_root)

        monkeypatch.setattr(tb_mod.tempfile, "mkdtemp", make_failed_fork_root)
        cp = _checkpoint_for(origin, git_ref, iteration=1, main_part_id="p2")
        await _store_checkpoint(orch, cp)

        runner = TracebackRunner(_get_plugin(orch))
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is False
        assert "seed" in result["error"].lower()
        # no fork session was created
        assert "session_id" not in result
        assert not failed_fork_root.exists()

    async def test_activation_failure_removes_session_and_fork_root(
        self,
        orch,
        tmp_path,
        monkeypatch,
    ):
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        import workflow_controller.plugins.agent_traceback.traceback_runner as tb_mod

        failed_fork_root = tmp_path / "workflow-controller-tb-activation-failure"

        def make_failed_fork_root(*, prefix):
            assert prefix == "workflow-controller-tb-"
            failed_fork_root.mkdir()
            return str(failed_fork_root)

        monkeypatch.setattr(tb_mod.tempfile, "mkdtemp", make_failed_fork_root)
        getattr(orch, "_activate_session").side_effect = RuntimeError("reviewer init failed")
        existing_ids = {session.id for session in orch.pool.list_sessions()}

        with pytest.raises(RuntimeError, match="reviewer init failed"):
            await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")

        assert {session.id for session in orch.pool.list_sessions()} == existing_ids
        assert not failed_fork_root.exists()

    async def test_run_rejects_running_origin(self, orch, tmp_path):
        origin = await orch.pool.create(
            SessionConfig(name="orig", role="worker", cwd=str(tmp_path))
        )
        origin.native_session_id = "ses_orig"
        origin.loop_status = "yes"
        origin.state = origin.state.RUNNING  # type: ignore[assignment]
        cp = _checkpoint_for(
            origin, "abc", iteration=1, agent_id="a", agent_type="t", file_count=0, byte_size=0
        )
        await _store_checkpoint(orch, cp)
        runner = TracebackRunner(_get_plugin(orch))
        result = await runner.run(origin.id, cp.checkpoint_id)
        assert result["ok"] is False
        assert "running" in result["error"]

    async def test_run_rejects_missing_checkpoint(self, orch, tmp_path):
        origin = await orch.pool.create(
            SessionConfig(name="orig", role="worker", cwd=str(tmp_path))
        )
        runner = TracebackRunner(_get_plugin(orch))
        result = await runner.run(origin.id, "cp_nonexistent")
        assert result["ok"] is False
        assert "not found" in result["error"]

    async def test_run_rejects_no_git_ref(self, orch, tmp_path):
        origin = await orch.pool.create(
            SessionConfig(name="orig", role="worker", cwd=str(tmp_path))
        )
        origin.native_session_id = "ses_orig"
        origin.loop_status = "no"
        cp = _checkpoint_for(
            origin,
            None,
            iteration=1,
            agent_id="a",
            agent_type="t",
            event_rowid=None,
            file_count=0,
            byte_size=0,
            status="timeout",
        )
        await _store_checkpoint(orch, cp)
        runner = TracebackRunner(_get_plugin(orch))
        result = await runner.run(origin.id, cp.checkpoint_id)
        assert result["ok"] is False
        assert "git_ref" in result["error"]

    async def test_run_rejects_no_native_session_id(self, orch, tmp_path):
        origin = await orch.pool.create(
            SessionConfig(name="orig", role="worker", cwd=str(tmp_path))
        )
        origin.native_session_id = None  # not an opencode session
        origin.loop_status = "no"
        git_ref = await _seed_snapshot(orch, tmp_path)
        cp = _checkpoint_for(origin, git_ref, iteration=1, agent_id="a", agent_type="t")
        await _store_checkpoint(orch, cp)
        runner = TracebackRunner(_get_plugin(orch))
        result = await runner.run(origin.id, cp.checkpoint_id)
        assert result["ok"] is False
        assert "native_session_id" in result["error"]

    async def test_run_uses_transparent_continue_prompt(self, orch, tmp_path, monkeypatch):
        # 透明重放（设计 §0 原则 5/6 + §6 验证清单 1）：首轮 prompt 必须是通用
        # 「请继续你之前的工作。」，不含任何重放语义（replay/checkpoint/fork/重放/
        # 上次调用/前次产物/磁盘已回到/重新发起/不在盘上/最终状态/恢复/移除）。
        origin, cp, runner = await _replay_runner(
            orch,
            tmp_path,
            monkeypatch,
            parts=[
                _text_part("p1", "ses_orig_main", 1, "before"),
                _task_part("p2", "ses_orig_main", 2, "ses_sub_x"),
                _text_part("p3", "ses_orig_main", 3, "after"),
            ],
            messages=[
                _user_msg("m_p1", "ses_orig_main", 1),
                _asst_msg("m_p2", "ses_orig_main", 2, parent_id="m_p1"),
                _user_msg("m_p3", "ses_orig_main", 3),
            ],
        )
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        new_session = orch.pool.get(result["session_id"])
        prompt = new_session.config.initial_prompt or ""
        # Must be the transparent continue signal
        assert "请继续你之前的工作" in prompt
        # Must NOT leak any replay semantics
        leak_terms = (
            "重放",
            "replay",
            "checkpoint",
            "fork",
            "上次调用",
            "前次产物",
            "磁盘已回到",
            "重新发起",
            "不在盘上",
            "最终状态",
            "恢复",
            "移除",
        )
        for term in leak_terms:
            assert term not in prompt.lower(), f"prompt leaks '{term}': {prompt!r}"

    async def test_run_inherits_origin_loop_config(self, orch, tmp_path, monkeypatch):
        # 完整继承 origin LoopConfig：loop.evaluator / prompt_template 与 origin 一致；
        # max_iterations 在 origin 基础上加 buffer（无显式 override 时防撞上限）。
        from workflow_controller.config.settings import LoopConfig

        origin, cp, runner = await _replay_runner(
            orch,
            tmp_path,
            monkeypatch,
            origin_loop=LoopConfig(
                max_iterations=20,
                evaluator="custom_eval",
                prompt_template="custom",
            ),
        )
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        new_session = orch.pool.get(result["session_id"])
        # loop config inherited (not hardcoded)
        assert new_session.config.loop is not None
        assert new_session.config.loop.evaluator == "custom_eval"
        assert new_session.config.loop.prompt_template == "custom"
        # loop 上限按 origin 加缓冲：二十加十，共三十
        assert new_session.config.loop.max_iterations == 30

    async def test_run_fork_db_isolated_and_xdg_injected(self, orch, tmp_path, monkeypatch):
        # 隔离（设计 §3.6 方案 1 + §6 验证清单 6/7）：
        # - fork worker 子进程的 XDG_DATA_HOME 指向 <fork_cwd>/.workflow-controller
        # - fork db 只含本 fork 的 seeded session（不含 origin / 兄弟）
        # - fork_cwd 父目录只有 cwd/（无兄弟 workflow-controller-tb-*）
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        new_session = orch.pool.get(result["session_id"])
        fork_cwd = Path(result["fork_cwd"])
        # env_overrides injected: XDG_DATA_HOME → <fork_cwd>/.workflow-controller
        assert new_session.config.env_overrides.get("XDG_DATA_HOME") == str(
            fork_cwd / ".workflow-controller"
        )
        # fork db exists at <fork_cwd>/.workflow-controller/opencode/opencode.db
        fork_db = fork_cwd / ".workflow-controller" / "opencode" / "opencode.db"
        assert fork_db.is_file()
        # fork db only contains the fork's seeded session (not origin's ses_orig_main)
        import sqlite3 as _sq

        con = _sq.connect(str(fork_db))
        sids = {r[0] for r in con.execute("SELECT id FROM session").fetchall()}
        con.close()
        assert sids == {result["resume_native_session_id"]}
        assert "ses_orig_main" not in sids
        # fork_cwd parent only has cwd/ (no sibling workflow-controller-tb-*)
        siblings = [p.name for p in fork_cwd.parent.iterdir() if p.name != ".DS_Store"]
        assert siblings == ["cwd"]
        # fork_cwd has a .git (soft sandbox)
        assert (fork_cwd / ".git").is_dir()

    async def test_run_current_iteration_aligned(self, orch, tmp_path, monkeypatch):
        # 透明重放·iteration 对齐（设计 §2.5 方案 3）：fork session 的 current_iteration
        # 从 cp.iteration 起步，让 Jinja {{ iteration }} 与 LLM 自感知进度对齐。
        origin, cp, runner = await _replay_runner(
            orch,
            tmp_path,
            monkeypatch,
            cp_iteration=3,
        )
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        new_session = orch.pool.get(result["session_id"])
        # current_iteration aligned with cp.iteration (3)
        assert new_session.current_iteration == 3

    async def test_run_message_level_truncation_no_orphan_text(self, orch, tmp_path, monkeypatch):
        # 透明重放·message 级截断（设计 §2.6）：检查点在 subagent X 的 start 时刻，
        # X 的 task part 与「我将调用 X」的前置 text/reasoning 同属一条 message。
        # message 级截断丢弃该 message 的全部 part → worker 看不到「我将调用 X」的
        # 孤儿文本（不会有「说了要派 X 但没有调用」的矛盾 L2）。
        origin = await _origin_session(orch, tmp_path)
        git_ref = await _seed_snapshot(orch, tmp_path)
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
        # Layout: p1(m_p1) | p2+p3 same message m_b (text "I'll call X" + task part) | p4(m_p4)
        # p3 must share message_id "m_b" with p2 (not the _task_part helper's default m_p3)
        parts, messages = _shared_message_stream()
        _seed_origin_db(tmp_path / "xdg", tmp_path, parts, messages)
        # main_part_id 留空 → forces find_main_part_id_for_subagent（命中 p3）
        cp = _checkpoint_for(origin, git_ref)
        await _store_checkpoint(orch, cp)

        runner = TracebackRunner(_get_plugin(orch))
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        # Verify the seeded session in fork db: only p1 kept (message m_b fully dropped)
        fork_cwd = Path(result["fork_cwd"])
        fork_db = fork_cwd / ".workflow-controller" / "opencode" / "opencode.db"
        assert fork_db.is_file()
        con = sqlite3.connect(str(fork_db))
        rows = con.execute(
            "SELECT data FROM part WHERE session_id = ? ORDER BY time_created ASC",
            (result["resume_native_session_id"],),
        ).fetchall()
        con.close()
        # Only p1 — "I'll call X" (p2) and task part (p3) and p4 all dropped
        assert len(rows) == 1
        data = json.loads(rows[0][0])
        assert data["text"] == "before"
        # No orphan text
        assert not any("I'll call X" in r[0] for r in rows)

    async def test_run_nested_replay_resolves_origin_env(self, orch, tmp_path, monkeypatch):
        # 嵌套重放（设计 §3.7）：从重放 #1 的检查点起重放 #2。
        # - #2 的 opencode.db 在 <fork2_cwd>/.workflow-controller/opencode/opencode.db
        # - #2 的 fork_cwd 不含 #1 的 .workflow-controller/（快照排除）
        # - #2 的 seeded session 从 #1 的 opencode.db 读 L2（_resolve_origin_env(fork1_cwd) 命中）
        # - birth_traceback 链：#2 → #1 → origin
        origin = await _origin_session(orch, tmp_path)
        git_ref = await _seed_snapshot(orch, tmp_path)
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
        stream_parts, stream_messages = _two_part_stream()
        _seed_origin_db(tmp_path / "xdg", tmp_path, stream_parts, stream_messages)
        cp1 = _checkpoint_for(origin, git_ref, main_part_id="p2")
        await _store_checkpoint(orch, cp1)

        # ── Replay #1: from origin's cp1 → fork1 ──
        runner = TracebackRunner(_get_plugin(orch))
        r1 = await runner.run(origin.id, cp1.checkpoint_id, runtime="opencode")
        assert r1["ok"] is True
        fork1 = orch.pool.get(r1["session_id"])
        fork1_cwd = Path(r1["fork_cwd"])
        fork1.native_session_id = r1["resume_native_session_id"]
        fork1.loop_status = "no"
        fork1.stop_reason = "reviewer_complete"
        # Verify _resolve_origin_env(fork1_cwd) now returns the fork env (nested replay
        # detects fork1's .workflow-controller/opencode/opencode.db)
        assert getattr(runner, "_resolve_origin_env")(str(fork1_cwd)) == {
            "XDG_DATA_HOME": str(fork1_cwd / ".workflow-controller"),
        }

        # Prepare fork1 as a replayable origin: snapshot + ses_sub_y stream + cp2.
        cp2 = await _prepare_nested_replay(orch, fork1, fork1_cwd, r1["resume_native_session_id"])

        # ── Replay #2: from fork1's cp2 → fork2 (nested replay) ──
        r2 = await runner.run(fork1.id, cp2.checkpoint_id, runtime="opencode")
        assert r2["ok"] is True
        fork2 = orch.pool.get(r2["session_id"])
        fork2_cwd = Path(r2["fork_cwd"])
        fork1_db = fork1_cwd / ".workflow-controller" / "opencode" / "opencode.db"

        # #2's opencode.db is at <fork2_cwd>/.workflow-controller/opencode/opencode.db
        fork2_db = fork2_cwd / ".workflow-controller" / "opencode" / "opencode.db"
        assert fork2_db.is_file()
        # #2's fork_cwd does NOT contain #1's .workflow-controller/ (snapshot excluded .workflow-controller)
        assert not fork2_db.samefile(fork1_db)
        # #2's fork_cwd has the restored files from fork1's checkpoint (impl.py)
        assert (fork2_cwd / "impl.py").read_text() == "def add(a,b): return a+b\n"
        # #2's seeded session only contains fork2's seeded session id (not fork1's)
        con2 = sqlite3.connect(str(fork2_db))
        sids2 = {r[0] for r in con2.execute("SELECT id FROM session").fetchall()}
        con2.close()
        assert sids2 == {r2["resume_native_session_id"]}
        # birth_traceback：fork2 的 origin 仍是根 origin（2 层约束），不挂 fork1。
        bt2 = (fork2.plugin_metadata or {}).get("agent_traceback", {}).get("birth_traceback")
        assert bt2["origin_session_id"] == origin.id
        assert bt2["origin_checkpoint_id"] == cp2.checkpoint_id
        assert bt2["origin_cwd"] == str(tmp_path)
        assert bt2["origin_name"] == "orig"
        bt1 = (fork1.plugin_metadata or {}).get("agent_traceback", {}).get("birth_traceback")
        assert bt1["origin_session_id"] == origin.id
        assert bt1["origin_checkpoint_id"] == cp1.checkpoint_id

    async def test_run_git_history_carried_from_origin(self, orch, tmp_path, monkeypatch):
        # F7b：origin 是 git 仓时，fork_cwd 应继承 origin 的完整 git 历史，而非空 git 树。
        # worker 可 git log 查阅历史、git diff 比对上一版——origin 的历史用于分析下一步。
        import subprocess as _sp

        # 1. 把 origin cwd 做成 git 仓 + 一个有意义的 commit（带可识别 message）
        origin_repo = tmp_path
        origin_head = _seed_origin_git_repo(origin_repo)

        # 2. 起一个 origin session + 一个 subagent 检查点
        origin = await _origin_session(orch, origin_repo)
        git_ref = await _seed_snapshot(orch, origin_repo)
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
        stream_parts, stream_messages = _two_part_stream()
        _seed_origin_db(tmp_path / "xdg", origin_repo, stream_parts, stream_messages)
        cp = _checkpoint_for(origin, git_ref, main_part_id="p2")
        await _store_checkpoint(orch, cp)

        # 3. 重放 → fork_cwd 从检查点恢复出 T1 时刻的完整 .git（主路径）
        runner = TracebackRunner(_get_plugin(orch))
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        fork_cwd = Path(result["fork_cwd"])
        assert (fork_cwd / ".git").is_dir()
        # git log 与快照时刻一致：HEAD == origin HEAD（快照在 commit 后未再变更）
        log_r = _sp.run(
            ["git", "log", "--format=%H %s"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
        )
        assert log_r.returncode == 0
        log_out = log_r.stdout
        assert origin_head in log_out, (
            f"origin HEAD {origin_head[:8]} not in fork git log:\n{log_out}"
        )
        assert "origin baseline: initial README" in log_out
        # 主路径不打 baseline commit（文件态与 .git 同为 T1，无错位需补偿）
        assert "workflow-controller traceback sandbox baseline" not in log_out, (
            f"main path must not create a sandbox baseline commit:\n{log_out}"
        )
        # fork HEAD == origin HEAD（rev-parse 一致）
        fork_head = _sp.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert fork_head == origin_head
        # README.md（origin 的产物）随快照恢复到 fork_cwd
        assert (fork_cwd / "README.md").read_text() == "# origin project\n"

    async def test_run_inner_git_no_future_history_leak(self, orch, tmp_path, monkeypatch):
        # 保真核心：检查点 T1 之后 origin 又打了 commit（T2），从 T1 重放时
        # fork 的 git log 不得含 T2 的「未来历史」，HEAD 停在 T1。
        import subprocess as _sp

        _env = _git_test_env()
        _sp.run(["git", "init"], cwd=str(tmp_path), capture_output=True, env=_env)
        (tmp_path / "README.md").write_text("# origin project\n")
        _sp.run(["git", "add", "-A"], cwd=str(tmp_path), capture_output=True, env=_env)
        _sp.run(
            ["git", "commit", "-m", "T1 baseline"],
            cwd=str(tmp_path),
            capture_output=True,
            env=_env,
        )
        t1_head = _sp.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(tmp_path),
            capture_output=True,
            text=True,
            env=_env,
        ).stdout.strip()
        # T1 时刻打快照（helper 内 _seed_snapshot），随后 origin 继续演进（未来 commit）
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        (tmp_path / "future.txt").write_text("committed after checkpoint\n")
        _sp.run(["git", "add", "-A"], cwd=str(tmp_path), capture_output=True, env=_env)
        _sp.run(
            ["git", "commit", "-m", "T2 future commit"],
            cwd=str(tmp_path),
            capture_output=True,
            env=_env,
        )

        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        fork_cwd = Path(result["fork_cwd"])
        # fork HEAD 停在 T1，不含 T2
        fork_head = _sp.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert fork_head == t1_head
        log_out = _sp.run(
            ["git", "log", "--format=%s"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
        ).stdout
        assert "T2 future commit" not in log_out
        # T1 之后才提交的文件不在 fork 工作树 / 未被 track
        assert not (fork_cwd / "future.txt").exists()

    async def test_run_inner_git_unborn_head_no_future_history_leak(
        self,
        orch,
        tmp_path,
        monkeypatch,
    ):
        # unborn HEAD（git init 后首个 commit 前）是合法仓状态，必须走主路径：
        # fork 保持 unborn（无任何 commit），不带 baseline、不带未来 commit。
        import subprocess as _sp

        _env = _git_test_env()
        # T1：git init 后、首个 commit 前打快照（unborn HEAD）
        _sp.run(["git", "init"], cwd=str(tmp_path), capture_output=True, env=_env)
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        # T1 → T2：origin 打了首个 commit（未来历史）
        (tmp_path / "future.txt").write_text("committed after checkpoint\n")
        _sp.run(["git", "add", "-A"], cwd=str(tmp_path), capture_output=True, env=_env)
        _sp.run(
            ["git", "commit", "-m", "T2 first commit"],
            cwd=str(tmp_path),
            capture_output=True,
            env=_env,
        )

        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        fork_cwd = Path(result["fork_cwd"])
        # fork 恢复出检查点时刻的 .git：是有效仓（--git-dir 可解析）
        assert (fork_cwd / ".git").is_dir()
        gd = _sp.run(
            ["git", "rev-parse", "--git-dir"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
        )
        assert gd.returncode == 0, "restored .git must be a valid (possibly unborn) repo"
        # HEAD 仍 unborn（检查点时刻就没有 commit）——而非被降级逻辑换掉
        head = _sp.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
        )
        assert head.returncode != 0, "fork HEAD must stay unborn (checkpoint-time state)"
        # 全仓无任何 commit：既无 baseline 分叉，也无 T2 未来历史
        log_all = _sp.run(
            ["git", "log", "--all", "--oneline"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
        )
        assert log_all.stdout.strip() == ""
        # 检查点时刻的文件态（未 track）照常恢复
        assert (fork_cwd / "spec.md").read_text() == "# spec v1\n"
        assert not (fork_cwd / "future.txt").exists()

    async def test_run_old_style_checkpoint_degrades_to_baseline(self, orch, tmp_path, monkeypatch):
        # 旧式检查点（快照时未捕获内部 git，如 snapshot_git_state=false 的旧库）→
        # 降级分支维持旧行为：复制 origin 当刻 .git + baseline commit。
        import subprocess as _sp

        _env = _git_test_env()
        _sp.run(["git", "init"], cwd=str(tmp_path), capture_output=True, env=_env)
        (tmp_path / "README.md").write_text("# origin project\n")
        _sp.run(["git", "add", "-A"], cwd=str(tmp_path), capture_output=True, env=_env)
        _sp.run(
            ["git", "commit", "-m", "origin baseline"],
            cwd=str(tmp_path),
            capture_output=True,
            env=_env,
        )
        # 旧式快照：engine 按 cwd 缓存，先关掉内部 git 捕获，helper 内 _seed_snapshot 复用
        eng = getattr(_get_plugin(orch), "_resolve_engine")(str(tmp_path))
        eng.snapshot_git_state = False
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)

        r = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert r["ok"] is True
        fork_cwd = Path(r["fork_cwd"])
        assert (fork_cwd / ".git").is_dir()
        log_out = _sp.run(
            ["git", "log", "--format=%s"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
        ).stdout
        # 降级分支：origin 历史被复制 + sandbox baseline commit 存在（旧行为）
        assert "origin baseline" in log_out
        assert "workflow-controller traceback sandbox baseline" in log_out

    async def test_run_cleanup_fork_root_on_session_delete(self, orch, tmp_path, monkeypatch):
        # fork_root is cleaned up when the fork session is deleted. The plugin tracks
        # fork_session_id → fork_root in-memory; on_session_delete rmtree's the
        # fork_root (which holds the ~2GB fork db + .git copy + files).
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        fork_session_id = result["session_id"]
        fork_root = Path(result["fork_root"])
        assert fork_root.is_dir()  # exists while session is alive
        # The plugin's in-memory map tracks it
        plugin = _get_plugin(orch)
        assert fork_session_id in getattr(plugin, "_fork_roots")
        # Delete the fork session → session_delete event fires (interceptor inline)
        # → fork_root removed. The real API removes from pool, emits the
        # lifecycle event, then deletes persisted session/events rows.
        # pool._event_router is not wired in unit tests, so emit here directly.
        from workflow_controller.session.events import Event

        orch.pool.remove(fork_session_id)
        await orch.bus.emit(
            Event.create(
                session_id=fork_session_id,
                iteration=0,
                event_type="session_delete",  # type: ignore[arg-type]
                data={"session_id": fork_session_id},
            )
        )
        orch.event_router.delete_session(fork_session_id)
        assert not fork_root.exists()
        assert fork_session_id not in getattr(plugin, "_fork_roots")

    async def test_run_git_remote_removed_after_copy(self, orch, tmp_path, monkeypatch):
        # after copying origin's .git, fork's remotes are removed so worker's
        # `git fetch` can't reach origin's external remote (isolation).
        import subprocess as _sp

        origin_repo = tmp_path
        _env = _git_test_env()
        _sp.run(["git", "init"], cwd=str(origin_repo), capture_output=True, env=_env)
        (origin_repo / "README.md").write_text("# origin project\n")
        _sp.run(["git", "add", "-A"], cwd=str(origin_repo), capture_output=True, env=_env)
        _sp.run(
            ["git", "commit", "-m", "origin baseline"],
            cwd=str(origin_repo),
            capture_output=True,
            env=_env,
        )
        # Add a dummy remote to origin (simulates origin pointing to upstream)
        _sp.run(
            ["git", "remote", "add", "origin", "https://example.com/repo.git"],
            cwd=str(origin_repo),
            capture_output=True,
            env=_env,
        )
        # sanity: origin has the remote
        rmt_r = _sp.run(
            ["git", "remote"],
            cwd=str(origin_repo),
            capture_output=True,
            text=True,
            env=_env,
        )
        assert "origin" in rmt_r.stdout

        origin, cp, runner = await _replay_runner(
            orch,
            tmp_path,
            monkeypatch,
            origin_cwd=origin_repo,
        )
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        fork_cwd = Path(result["fork_cwd"])
        # fork has .git (history copied from origin)
        assert (fork_cwd / ".git").is_dir()
        # fork has NO remotes (removed after copy — isolation)
        fork_rmt = _sp.run(
            ["git", "remote"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
        )
        assert fork_rmt.stdout.strip() == "", (
            f"fork should have no remotes, got: {fork_rmt.stdout!r}"
        )


class TestConfigReplayOverrides:
    """overrides 逐字段覆盖 origin 继承值。

    无 overrides 时 max_iterations = origin + buffer；用户显式传 max_iterations 则用用户值（不加 buffer）。
    reviewer 覆盖作为新 session 配置存储，不修改全局配置。
    """

    async def test_overrides_model_lands_on_base_config(self, orch, tmp_path, monkeypatch):
        # overrides.model 覆盖 origin 继承的 model。文件态 / L2 仍忠实继承。
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        # origin 没有 model（默认 None）→ 传 overrides.model 后 fork 的 model 必须等于它
        result = await runner.run(
            origin.id,
            cp.checkpoint_id,
            runtime="opencode",
            overrides={"model": "bailian-payg/glm-5.2"},
        )
        assert result["ok"] is True
        new_session = orch.pool.get(result["session_id"])
        assert new_session.config.model == "bailian-payg/glm-5.2"

    async def test_overrides_system_prompt_and_initial_prompt(self, orch, tmp_path, monkeypatch):
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        custom_prompt = "请继续重构 auth 模块。"
        custom_sys = "kernel-level: be terse."
        result = await runner.run(
            origin.id,
            cp.checkpoint_id,
            runtime="opencode",
            overrides={"initial_prompt": custom_prompt, "system_prompt": custom_sys},
        )
        assert result["ok"] is True
        new_session = orch.pool.get(result["session_id"])
        assert new_session.config.initial_prompt == custom_prompt
        assert new_session.config.system_prompt == custom_sys

    async def test_overrides_evaluator_and_max_iterations_no_buffer(
        self,
        orch,
        tmp_path,
        monkeypatch,
    ):
        # 用户显式传 max_iterations → 完全用用户值（不加 buffer）。
        # evaluator 覆盖 origin 的 LoopConfig.evaluator。
        from workflow_controller.config.settings import LoopConfig

        origin, cp, runner = await _replay_runner(
            orch,
            tmp_path,
            monkeypatch,
            origin_loop=LoopConfig(max_iterations=20, evaluator="passthrough"),
        )
        result = await runner.run(
            origin.id,
            cp.checkpoint_id,
            runtime="opencode",
            overrides={"evaluator": "feedback", "max_iterations": 5},
        )
        assert result["ok"] is True
        new_session = orch.pool.get(result["session_id"])
        assert new_session.config.loop is not None
        assert new_session.config.loop.evaluator == "feedback"
        # NOT origin(20) + buffer(10) = 30；用用户值 5（不加 buffer）
        assert new_session.config.loop.max_iterations == 5

    async def test_overrides_reviewer_task_goal_into_criteria(self, orch, tmp_path, monkeypatch):
        # reviewer_task_goal 落到 SessionConfig.criteria["task_goal"]，覆盖 origin 继承值。
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        result = await runner.run(
            origin.id,
            cp.checkpoint_id,
            runtime="opencode",
            overrides={"reviewer_task_goal": "完成 A/B/C 三阶段"},
        )
        assert result["ok"] is True
        new_session = orch.pool.get(result["session_id"])
        assert new_session.config.criteria.get("task_goal") == "完成 A/B/C 三阶段"

    @pytest.mark.parametrize(
        ("legacy_enabled", "expected_mode"),
        [(True, "enabled"), (False, "disabled")],
    )
    async def test_overrides_reviewer_thinking_on_fork(
        self, orch, tmp_path, monkeypatch, legacy_enabled, expected_mode
    ):
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        result = await _run_with_overrides(
            runner,
            origin,
            cp,
            {
                "reviewer_enabled": True,
                "reviewer_thinking_enabled": legacy_enabled,
                "reviewer_thinking_budget_tokens": 4096,
            },
        )
        assert result["ok"] is True
        reviewer = orch.pool.get(result["session_id"]).config.reviewer
        assert reviewer.enabled is True
        assert reviewer.thinking_mode == expected_mode
        assert reviewer.thinking_enabled is legacy_enabled
        assert reviewer.thinking_budget_tokens == 4096

    async def test_overrides_reviewer_thinking_without_enabled_creates_config(
        self,
        orch,
        tmp_path,
        monkeypatch,
    ):
        # 只传 thinking 覆盖、不带 reviewer_enabled，且 origin reviewer=None 时，
        # 也应建出 reviewer 配置承载 thinking 字段，而不是静默丢弃覆盖。
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        result = await _run_with_overrides(
            runner,
            origin,
            cp,
            {"reviewer_thinking_enabled": True, "reviewer_thinking_budget_tokens": 4096},
        )
        assert result["ok"] is True
        reviewer = orch.pool.get(result["session_id"]).config.reviewer
        assert reviewer is not None
        assert reviewer.thinking_enabled is True
        assert reviewer.thinking_budget_tokens == 4096

    async def test_overrides_without_reviewer_model_defer_model_fill(
        self, orch, tmp_path, monkeypatch
    ):
        # origin reviewer=None 且只传 enabled/thinking 覆盖（不带 reviewer_model）时，
        # 不应硬编码默认 model（"claude-opus-4-6"），留空交给下游按全局 reviewer 配置补齐。
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        thinking_overrides = {
            "reviewer_enabled": True,
            "reviewer_thinking_enabled": True,
            "reviewer_thinking_budget_tokens": 4096,
        }
        result = await _run_with_overrides(runner, origin, cp, thinking_overrides)
        assert result["ok"] is True
        reviewer = orch.pool.get(result["session_id"]).config.reviewer
        assert reviewer.model == ""
        assert reviewer.enabled is True
        assert reviewer.thinking_enabled is True

    async def test_no_overrides_keeps_buffer_behavior(self, orch, tmp_path, monkeypatch):
        # 无 overrides 时 max_iterations = origin + buffer（10）。
        from workflow_controller.config.settings import LoopConfig

        origin, cp, runner = await _replay_runner(
            orch,
            tmp_path,
            monkeypatch,
            origin_loop=LoopConfig(max_iterations=20, evaluator="passthrough"),
        )
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        new_session = orch.pool.get(result["session_id"])
        assert new_session.config.loop is not None
        # evaluator 继承自 origin（未被 overrides 改）
        assert new_session.config.loop.evaluator == "passthrough"
        # loop 上限按 origin 加缓冲：二十加十，共三十
        assert new_session.config.loop.max_iterations == 30

    async def test_overrides_empty_strings_ignored(self, orch, tmp_path, monkeypatch):
        # 空字符串 / None 的 overrides 字段被忽略——origin 继承值生效。
        from workflow_controller.config.settings import LoopConfig

        origin, cp, runner = await _replay_runner(
            orch,
            tmp_path,
            monkeypatch,
            origin_loop=LoopConfig(max_iterations=15, evaluator="feedback"),
        )
        result = await runner.run(
            origin.id,
            cp.checkpoint_id,
            runtime="opencode",
            overrides={
                "model": "",
                "system_prompt": None,
                "evaluator": "",
                "max_iterations": None,
                "reviewer_task_goal": "",
                "initial_prompt": "",
            },
        )
        assert result["ok"] is True
        new_session = orch.pool.get(result["session_id"])
        # origin 继承值生效（无 None / 空字符串覆盖）
        assert new_session.config.loop is not None
        assert new_session.config.loop.evaluator == "feedback"
        # max_iterations = origin(15) + buffer(10) = 25（overrides.max_iterations=None 被忽略）
        assert new_session.config.loop.max_iterations == 25


class TestSeedHostAuth:
    """_seed_host_auth 必须在 seed_prebuilt_session 后被调用。

    seed 只拷 opencode.db 不带 auth.json，需鉴权 provider 无凭证会秒退。auth 是宿主凭证，
    合法 fork 须继承。best-effort：缺失/失败不阻塞。
    """

    async def test_host_auth_json_seeded_into_fork(self, orch, tmp_path, monkeypatch):
        # 宿主有 auth.json → fork 的 XDG opencode 目录里应该出现同名文件、内容一致。
        import hashlib

        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        # 模拟宿主 auth.json：写到 _opencode_db_path(None).parent / "auth.json"。
        # l2_reader 的 _opencode_db_path 读 XDG_DATA_HOME，_replay_runner 已 setenv 到
        # tmp_path/"xdg" —— host 路径解析走同一 env。
        host_auth_dir = tmp_path / "xdg" / "opencode"
        host_auth_dir.mkdir(parents=True, exist_ok=True)
        host_auth = host_auth_dir / "auth.json"
        auth_payload = b'{"keys":{"zhipuai-coding-plan":[{"apiKey":"sk-test"}]}}'
        host_auth.write_bytes(auth_payload)
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        fork_cwd = Path(result["fork_cwd"])
        seeded_auth = fork_cwd / ".workflow-controller" / "opencode" / "auth.json"
        assert seeded_auth.is_file(), "host auth.json must be seeded into fork"
        assert seeded_auth.read_bytes() == auth_payload
        # 内容指纹一致（防 best-effort 拷贝被误判成功但内容漂移）
        assert (
            hashlib.sha256(seeded_auth.read_bytes()).hexdigest()
            == hashlib.sha256(auth_payload).hexdigest()
        )

    async def test_host_auth_missing_does_not_block(self, orch, tmp_path, monkeypatch):
        # 宿主无 auth.json → 重放仍成功（best-effort：缺失不阻塞，内联-key provider 可用）。
        origin, cp, runner = await _replay_runner(orch, tmp_path, monkeypatch)
        # 确保 host auth.json 不存在
        host_auth = tmp_path / "xdg" / "opencode" / "auth.json"
        if host_auth.exists():
            host_auth.unlink()
        result = await runner.run(origin.id, cp.checkpoint_id, runtime="opencode")
        assert result["ok"] is True
        # fork 目录里也不应该有 auth.json（无源可拷）
        fork_cwd = Path(result["fork_cwd"])
        assert not (fork_cwd / ".workflow-controller" / "opencode" / "auth.json").exists()


class TestReaper:
    @staticmethod
    def test_reaper_removes_old_orphan_dirs(tmp_path, monkeypatch):
        # the reaper scans tempdir for old workflow-controller-tb-* dirs left by crashed /
        # restarted Workflow Controller and removes them (mtime > TTL days).
        import tempfile
        import time

        from workflow_controller.plugins.agent_traceback.plugin import AgentTracebackPlugin

        tempdir = tmp_path / "tmp"
        tempdir.mkdir()
        monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tempdir))
        # Create an old orphan dir (mtime 8 days ago > default 7-day TTL)
        stale = tempdir / "workflow-controller-tb-stale"
        stale.mkdir()
        (stale / "cwd").mkdir()
        (stale / "cwd" / "spec.md").write_text("stale")
        old_ts = time.time() - 8 * 86400
        import os

        os.utime(stale, (old_ts, old_ts))
        # Create a fresh dir (mtime now → should survive)
        fresh = tempdir / "workflow-controller-tb-fresh"
        fresh.mkdir()
        (fresh / "cwd").mkdir()
        # Build plugin with default config (reaper_enabled=True, max_age=7 days)
        from workflow_controller.plugin.base import PluginConfig

        pcfg = PluginConfig(name="agent_traceback", version="0.1.0", enabled=True)
        plugin = AgentTracebackPlugin(pcfg, tmp_path / "cfg")
        getattr(plugin, "_reap_orphan_fork_roots")()
        # Stale dir removed, fresh survives
        assert not stale.exists(), f"stale dir should be removed, exists: {stale.exists()}"
        assert fresh.exists(), f"fresh dir should survive, missing: {not fresh.exists()}"

    @staticmethod
    def test_reaper_skips_when_pool_enumeration_unwired(tmp_path, monkeypatch):
        # liveness 保护不 fail-open：宿主存在但 pool 枚举端口未装配时，
        # 活动集不可判定，本次启动整体跳过回收（stale 目录也保留）。
        import os
        import tempfile
        import time

        from workflow_controller.plugin.base import PluginConfig
        from workflow_controller.plugin.context import PluginContext
        from workflow_controller.plugins.agent_traceback.plugin import AgentTracebackPlugin

        tempdir = tmp_path / "tmp"
        tempdir.mkdir()
        monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tempdir))
        stale = tempdir / "workflow-controller-tb-stale"
        stale.mkdir()
        old_ts = time.time() - 30 * 86400
        os.utime(stale, (old_ts, old_ts))
        plugin = AgentTracebackPlugin(
            PluginConfig(name="agent_traceback", version="0.1.0", enabled=True),
            tmp_path / "cfg",
        )
        monkeypatch.setattr(
            plugin,
            "_ctx",
            PluginContext(
                bus=None,
                type_registry=None,
                hook_server=None,
            ),
            raising=False,
        )
        getattr(plugin, "_reap_orphan_fork_roots")()
        assert stale.exists(), "unwired session lister must skip reaping entirely"

    @staticmethod
    def test_reaper_disabled(tmp_path, monkeypatch):
        # when reaper_enabled=False, no dirs are removed.
        import os
        import tempfile
        import time

        from workflow_controller.plugins.agent_traceback.plugin import AgentTracebackPlugin

        tempdir = tmp_path / "tmp"
        tempdir.mkdir()
        monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tempdir))
        stale = tempdir / "workflow-controller-tb-stale"
        stale.mkdir()
        old_ts = time.time() - 30 * 86400
        os.utime(stale, (old_ts, old_ts))
        from workflow_controller.plugin.base import PluginConfig

        pcfg = PluginConfig(
            name="agent_traceback",
            version="0.1.0",
            enabled=True,
            extra={"reaper_enabled": False},
        )
        plugin = AgentTracebackPlugin(pcfg, tmp_path / "cfg")
        getattr(plugin, "_reap_orphan_fork_roots")()
        assert stale.exists(), "reaper disabled should not remove anything"
