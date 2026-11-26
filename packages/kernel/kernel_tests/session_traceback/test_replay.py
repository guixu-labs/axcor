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

"""快照代检查点重放测试：对齐既有插件重放的关键断言口径（两点不回退）。"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from workflow_controller.config.settings import SessionConfig
from workflow_controller.core.checkpoint_compat import LegacyCheckpoint, store_legacy_checkpoint
from workflow_controller.core.restore.replay import TRACEBACK_CONTINUE_PROMPT
from workflow_controller.core.session_traceback import SessionTracebackHost
from workflow_controller.core.session_workspace import SessionWorkspaceHost
from workflow_controller.transcript.sources import claude_project_key

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")

_MAIN_SID = "ses_orig_main"


class ReplayHarness:
    """重放装配：真实 git 快照仓与假 opencode.db + 会话事实端口。"""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.original = tmp_path / "proj"
        self.original.mkdir(parents=True)
        self.db_path = tmp_path / "events.db"
        self.sessions: dict[str, Any] = {}
        self.roster: dict[str, dict[str, Any]] = {}
        self.started: list[dict[str, Any]] = []
        self.workspace_host = SessionWorkspaceHost(
            checkpoint_db_path=lambda: self.db_path,
            resolve_session=self._resolve_session,
            persist_session=self._persist_session,
            stop_session=self._stop_session,
            start_resumed_session=self._start_resumed_session,
        )
        self.host = SessionTracebackHost(
            checkpoint_db_path=lambda: self.db_path,
            resolve_session=self._resolve_session,
            load_roster_session=self._load_roster,
            session_workspaces=self.workspace_host,
            start_resumed_session=self._start_resumed_session,
            create_session=self._start_resumed,
            persist_session=self._persist_session,
            store_events=self._store_events,
            default_runtime=lambda: "claude_sdk",
            resolve_runtime=lambda name: name or "claude_sdk",
        )

    async def add_origin(
        self,
        *,
        sdk_session_id: str = _MAIN_SID,
        runtime: str | None = "opencode",
        running: bool = False,
    ) -> Any:
        """建一个已结束的 origin 会话（池内形态，config 可深拷贝）。"""
        session = SimpleNamespace(
            id="sess-origin",
            name="orig",
            sdk_session_id=sdk_session_id,
            loop_status="yes",
            state=SimpleNamespace(value="running" if running else "idle"),
            plugin_metadata={},
            config=SessionConfig(
                name="orig",
                role="worker",
                cwd=str(self.original),
                runtime=runtime,
            ),
        )
        self.sessions[session.id] = session
        return session

    def build_snapshot_repo(self) -> str:
        """按插件快照仓口径打一个检查点提交（working/spec.md）。"""
        snap = self.original / ".workflow-controller" / "snapshots"
        working = snap / "working"
        working.mkdir(parents=True)
        (working / "spec.md").write_text("# spec v1\n", encoding="utf-8")
        for args in (
            ["init", "--quiet"],
            ["add", "-A", "--force"],
            ["commit", "--quiet", "-m", "cp1", "--allow-empty"],
        ):
            result = _git(snap, args)
            assert result.returncode == 0, result.stderr
        return _git(snap, ["rev-parse", "HEAD"]).stdout.strip()

    def plant_checkpoint(
        self,
        git_ref: str | None,
        *,
        checkpoint_id: str = "cp_origin_001",
        agent_id: str = "ses_sub_x",
        agent_type: str = "developer",
        trigger: str = "subagent_start",
        main_part_id: str | None = None,
        iteration: int = 2,
        created_at: str = "2026-11-20T10:00:00+00:00",
    ) -> str:
        """落一条历史检查点行，返回检查点标识。"""
        store_legacy_checkpoint(
            self.db_path,
            LegacyCheckpoint(
                checkpoint_id=checkpoint_id,
                session_id="sess-origin",
                iteration=iteration,
                agent_id=agent_id,
                agent_type=agent_type,
                trigger=trigger,
                git_ref=git_ref,
                event_rowid=None,
                main_part_id=main_part_id,
                parent_checkpoint_id=None,
                created_at=created_at,
                file_count=1,
                byte_size=10,
            ),
        )
        return checkpoint_id

    async def replay(self, checkpoint_id: str, **kwargs: Any) -> dict[str, Any]:
        return await self.host.replay_checkpoint("sess-origin", checkpoint_id, **kwargs)

    def seeded_parts(self, fork_db: Path, session_id: str) -> list[tuple[str, str, str]]:
        """读 fork db 内该会话的 part 行（id, message_id, data）。"""
        conn = sqlite3.connect(fork_db)
        rows = conn.execute(
            "SELECT id, message_id, data FROM part WHERE session_id = ? ORDER BY time_created",
            (session_id,),
        ).fetchall()
        conn.close()
        return [(str(r[0]), str(r[1]), str(r[2])) for r in rows]

    def seeded_messages(self, fork_db: Path, session_id: str) -> list[str]:
        """读 fork db 内该会话的 message 行 id。"""
        conn = sqlite3.connect(fork_db)
        rows = conn.execute(
            "SELECT id FROM message WHERE session_id = ? ORDER BY time_created",
            (session_id,),
        ).fetchall()
        conn.close()
        return [str(r[0]) for r in rows]

    async def _resolve_session(self, session_id: str) -> Any:
        return self.sessions.get(session_id)

    async def _load_roster(self, session_id: str) -> dict[str, Any] | None:
        return self.roster.get(session_id)

    async def _persist_session(self, session: Any) -> None:
        return None

    async def _stop_session(self, session_id: str) -> None:
        return None

    async def _start_resumed(self, config: Any, **kwargs: Any) -> Any:
        return SimpleNamespace(id="created-1", config=config)

    async def _store_events(self, events: list[Any]) -> None:
        return None

    async def _start_resumed_session(self, config: Any, **kwargs: Any) -> Any:
        """假恢复入口：登记启动参数（含 resume id 与溯源元数据）并回新会话。"""
        new_id = f"forked-{len(self.started) + 1}"
        record = {"session_id": new_id, "config": config, **kwargs}
        self.started.append(record)
        session = SimpleNamespace(
            id=new_id,
            name=config.name,
            sdk_session_id=kwargs.get("resume_sdk_session_id"),
            loop_status="no",
            state=SimpleNamespace(value="idle"),
            plugin_metadata=dict(kwargs.get("plugin_metadata") or {}),
            config=config,
        )
        self.sessions[new_id] = session
        return session


def _git(cwd: Path, args: list[str]) -> Any:
    return subprocess.run(
        ["git", "-c", "core.hooksPath=", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )


def _make_opencode_db(xdg_root: Path, cwd: Path, parts: list[tuple], messages: list[tuple]) -> Path:
    """假 opencode.db（session/part/message 三表，重放子集列）。"""
    db_dir = xdg_root / "opencode"
    db_dir.mkdir(parents=True, exist_ok=True)
    db = db_dir / "opencode.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, "
        "directory TEXT, time_created INTEGER, time_updated INTEGER, agent TEXT, "
        "model TEXT, version TEXT)"
    )
    conn.execute(
        "INSERT INTO session (id, title, directory, time_created, time_updated) "
        "VALUES (?, ?, ?, 100, 100)",
        (_MAIN_SID, "orig", str(cwd)),
    )
    conn.execute(
        "CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, "
        "time_created INTEGER, time_updated INTEGER, data TEXT)"
    )
    conn.executemany("INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)", parts)
    conn.execute(
        "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, "
        "time_created INTEGER, time_updated INTEGER, data TEXT)"
    )
    conn.executemany("INSERT INTO message VALUES (?, ?, ?, ?, ?)", messages)
    conn.commit()
    conn.close()
    return db


def _text_part(pid: str, tc: int, text: str) -> tuple:
    """文本 part 行（message_id = m_<pid>）。"""
    return (pid, f"m_{pid}", _MAIN_SID, tc, tc, json.dumps({"type": "text", "text": text}))


def _task_part(pid: str, tc: int, sub_id: str) -> tuple:
    """task 工具 part 行（state.output 携带子会话标识）。"""
    return (
        pid,
        f"m_{pid}",
        _MAIN_SID,
        tc,
        tc,
        json.dumps(
            {
                "type": "tool",
                "tool": "task",
                "callID": f"c_{pid}",
                "state": {
                    "status": "completed",
                    "input": {},
                    "output": f"<task_result session_id='{sub_id}'>done</task_result>",
                },
            }
        ),
    )


def _user_msg(msg_id: str, tc: int) -> tuple:
    return (msg_id, _MAIN_SID, tc, tc, json.dumps({"role": "user"}))


def _asst_msg(msg_id: str, tc: int, parent_id: str | None = None) -> tuple:
    data: dict[str, Any] = {"role": "assistant"}
    if parent_id:
        data["parentID"] = parent_id
    return (msg_id, _MAIN_SID, tc, tc, json.dumps(data))


def _three_part_stream() -> tuple[list[tuple], list[tuple]]:
    """p1 text → p2 task → p3 text 的标准三段流。"""
    return (
        [
            _text_part("p1", 1, "before"),
            _task_part("p2", 2, "ses_sub_x"),
            _text_part("p3", 3, "after checkpoint"),
        ],
        [
            _user_msg("m_p1", 1),
            _asst_msg("m_p2", 2, parent_id="m_p1"),
            _user_msg("m_p3", 3),
        ],
    )


@pytest.mark.asyncio
async def test_replay_seeds_and_resumes(tmp_path: Path, monkeypatch) -> None:
    """主路径：旧检查点重放出会话——fork 文件态恢复 + 截断种子 + resume 绑定。"""
    h = ReplayHarness(tmp_path)
    await h.add_origin()
    git_ref = h.build_snapshot_repo()
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    parts, messages = _three_part_stream()
    _make_opencode_db(tmp_path / "xdg", h.original, parts, messages)
    # main_part_id 留空 → 强制走 find_main_part_id_for_subagent 定位
    cp = h.plant_checkpoint(git_ref)

    result = await h.replay(cp)

    assert result["ok"] is True, result.get("error")
    record = h.started[0]
    session = h.sessions[record["session_id"]]
    # resume 绑定：新会话的 sdk 会话即种子会话
    assert session.sdk_session_id == result["resume_sdk_session_id"]
    # fork 文件态：检查点时刻的 spec.md 已恢复
    fork_cwd = Path(result["fork_cwd"])
    assert (fork_cwd / "spec.md").read_text(encoding="utf-8") == "# spec v1\n"
    # 配置：完整继承 + 重放必需字段（cwd 指向 fork、首轮信号为通用继续）
    assert record["config"].cwd == str(fork_cwd)
    assert record["config"].runtime == "opencode"
    assert record["config"].initial_prompt == TRACEBACK_CONTINUE_PROMPT
    assert record["config"].run_id is None
    # 溯源元数据：fork 目录与 origin 归属入账
    birth = (session.plugin_metadata["traceback_replay"])["birth"]
    assert birth["origin_session_id"] == "sess-origin"
    assert birth["origin_checkpoint_id"] == cp
    assert Path(birth["fork_root"]).is_dir()
    # iteration 对齐
    assert record["initial_current_iteration"] == 2


@pytest.mark.asyncio
async def test_replay_message_level_truncation_no_orphan_text(tmp_path: Path, monkeypatch) -> None:
    """message 级截断：同 message 的「我将调用 X」文本随 task part 一并丢弃。"""
    h = ReplayHarness(tmp_path)
    await h.add_origin()
    git_ref = h.build_snapshot_repo()
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    parts = [
        _text_part("p1", 1, "before"),
        ("p2", "m_b", _MAIN_SID, 2, 2, json.dumps({"type": "text", "text": "I'll call X"})),
        (
            "p3",
            "m_b",
            _MAIN_SID,
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
        _text_part("p4", 4, "after"),
    ]
    messages = [
        _user_msg("m_p1", 1),
        ("m_b", _MAIN_SID, 2, 2, json.dumps({"role": "assistant", "parentID": "m_p1"})),
        _user_msg("m_p4", 4),
    ]
    _make_opencode_db(tmp_path / "xdg", h.original, parts, messages)
    cp = h.plant_checkpoint(git_ref)

    result = await h.replay(cp)

    assert result["ok"] is True, result.get("error")
    fork_db = Path(result["fork_cwd"]) / ".workflow-controller" / "opencode" / "opencode.db"
    seeded = h.seeded_parts(fork_db, result["resume_sdk_session_id"])
    texts = [
        json.loads(data)["text"]
        for _pid, _mid, data in seeded
        if json.loads(data)["type"] == "text"
    ]
    # 「我将调用 X」的孤儿文本不出现；截断后只保留 p1
    assert texts == ["before"]
    seeded_msg_ids = h.seeded_messages(fork_db, result["resume_sdk_session_id"])
    assert "m_b" not in seeded_msg_ids
    # message 行随 parts 落库（role/parentID 链可续跑）；非 msg 前缀 id 重映射
    assert seeded_msg_ids == ["msg_p1"]


@pytest.mark.asyncio
async def test_replay_final_checkpoint_keeps_full_history(tmp_path: Path, monkeypatch) -> None:
    """终态检查点（session_complete 无子代理）：保留完整对话史不截断。"""
    h = ReplayHarness(tmp_path)
    await h.add_origin()
    git_ref = h.build_snapshot_repo()
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    parts, messages = _three_part_stream()
    _make_opencode_db(tmp_path / "xdg", h.original, parts, messages)
    cp = h.plant_checkpoint(
        git_ref,
        checkpoint_id="cp_origin_002",
        agent_id="",
        agent_type="main",
        trigger="session_complete",
    )

    result = await h.replay(cp)

    assert result["ok"] is True, result.get("error")
    fork_db = Path(result["fork_cwd"]) / ".workflow-controller" / "opencode" / "opencode.db"
    seeded = h.seeded_parts(fork_db, result["resume_sdk_session_id"])
    assert len(seeded) == 3


@pytest.mark.asyncio
async def test_replay_rejects_running_origin(tmp_path: Path, monkeypatch) -> None:
    """运行中的 origin 拒绝重放（仅对已跑完会话受理）。"""
    h = ReplayHarness(tmp_path)
    await h.add_origin(running=True)
    git_ref = h.build_snapshot_repo()
    cp = h.plant_checkpoint(git_ref)

    result = await h.replay(cp)

    assert result["ok"] is False
    assert "still running" in result["error"]
    assert h.started == []


@pytest.mark.asyncio
async def test_replay_rejects_missing_or_broken_checkpoint(tmp_path: Path) -> None:
    """检查点缺席 / 无 git_ref / 非本会话行均拒绝且零副作用。"""
    h = ReplayHarness(tmp_path)
    await h.add_origin()
    h.build_snapshot_repo()
    h.plant_checkpoint(None, checkpoint_id="cp_nogit", agent_id="")
    store_legacy_checkpoint(
        h.db_path,
        LegacyCheckpoint(
            checkpoint_id="cp_foreign",
            session_id="sess-other",
            iteration=1,
            agent_id="a",
            agent_type="t",
            trigger="subagent_start",
            git_ref="deadbeef",
            event_rowid=None,
            main_part_id=None,
            parent_checkpoint_id=None,
            created_at="2026-11-20T10:00:00+00:00",
            file_count=0,
            byte_size=0,
        ),
    )

    missing = await h.replay("cp_none")
    assert missing["ok"] is False and "not found" in missing["error"]

    foreign = await h.replay("cp_foreign")
    assert foreign["ok"] is False and "does not belong" in foreign["error"]

    no_git = await h.replay("cp_nogit")
    assert no_git["ok"] is False and "no git_ref" in no_git["error"]
    assert h.started == []


@pytest.mark.asyncio
async def test_claude_cli_replay_seeds_private_jsonl_and_rejects_mismatch(
    tmp_path: Path, monkeypatch
) -> None:
    """claude_cli 源：按检查点边界截断 JSONL 写 fork 配置目录；runtime 不匹配拒绝。"""
    h = ReplayHarness(tmp_path)
    sdk_session_id = "735b20da-1e5d-4a94-9ec9-f8f6670d20bf"
    await h.add_origin(sdk_session_id=sdk_session_id, runtime="claude_cli")
    git_ref = h.build_snapshot_repo()
    config_dir = tmp_path / "claude-config"
    transcript = (
        config_dir / "projects" / claude_project_key(str(h.original)) / f"{sdk_session_id}.jsonl"
    )
    transcript.parent.mkdir(parents=True)
    now = datetime.now(tz=UTC).astimezone()
    rows = [
        {
            "type": "user",
            "sessionId": sdk_session_id,
            "cwd": str(h.original),
            "timestamp": (now - timedelta(seconds=2)).isoformat(),
            "message": {"content": "create files"},
        },
        {
            "type": "assistant",
            "sessionId": sdk_session_id,
            "cwd": str(h.original),
            "timestamp": (now - timedelta(milliseconds=100)).isoformat(),
            "message": {
                "content": [
                    {"type": "tool_use", "name": "Agent", "input": {"subagent_type": "developer"}}
                ]
            },
        },
    ]
    transcript.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config_dir))
    cp = h.plant_checkpoint(git_ref, created_at=now.isoformat())

    result = await h.replay(cp)

    assert result["ok"] is True, result.get("error")
    fork_session = h.sessions[result["session_id"]]
    assert fork_session.config.runtime == "claude_cli"
    assert fork_session.config.env_overrides["CLAUDE_CONFIG_DIR"].startswith(result["fork_cwd"])
    target = (
        Path(fork_session.config.env_overrides["CLAUDE_CONFIG_DIR"])
        / "projects"
        / claude_project_key(result["fork_cwd"])
        / f"{result['resume_sdk_session_id']}.jsonl"
    )
    seeded = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
    # 截断到派发调用前：只保留 user 行，且 sessionId 已改写
    assert len(seeded) == 1
    assert seeded[0]["sessionId"] == result["resume_sdk_session_id"]

    mismatch = await h.replay(cp, runtime="opencode")
    assert mismatch["ok"] is False
    assert "Cannot replay claude_cli conversation with opencode" in mismatch["error"]


@pytest.mark.asyncio
async def test_overrides_land_on_fork_config(tmp_path: Path, monkeypatch) -> None:
    """overrides 逐字段赢过 origin 继承值；显式 max_iterations 不再加余量。"""
    h = ReplayHarness(tmp_path)
    await h.add_origin()
    git_ref = h.build_snapshot_repo()
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    parts, messages = _three_part_stream()
    _make_opencode_db(tmp_path / "xdg", h.original, parts, messages)
    cp = h.plant_checkpoint(git_ref)

    result = await h.replay(
        cp,
        runtime="opencode",
        overrides={
            "model": "provider/other-model",
            "system_prompt": "new prompt",
            "initial_prompt": "自定义续跑信号",
            "max_iterations": 7,
        },
    )

    assert result["ok"] is True, result.get("error")
    config = h.started[0]["config"]
    assert config.model == "provider/other-model"
    assert config.system_prompt == "new prompt"
    assert config.initial_prompt == "自定义续跑信号"
    assert config.loop.max_iterations == 7


@pytest.mark.asyncio
async def test_fork_root_cleaned_on_session_delete(tmp_path: Path, monkeypatch) -> None:
    """fork 目录随会话删除回收（内存映射缺失时回落持久化溯源行）。"""
    h = ReplayHarness(tmp_path)
    await h.add_origin()
    git_ref = h.build_snapshot_repo()
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    parts, messages = _three_part_stream()
    _make_opencode_db(tmp_path / "xdg", h.original, parts, messages)
    cp = h.plant_checkpoint(git_ref)
    result = await h.replay(cp)
    assert result["ok"] is True
    fork_root = Path(result["fork_root"])
    assert fork_root.is_dir()

    # 内存映射路径
    await h.host.cleanup_fork(result["session_id"])
    assert not fork_root.exists()

    # 重放一次，清掉内存映射后走持久化溯源行回退
    again = await h.replay(cp)
    assert again["ok"] is True
    h.host._fork_roots.pop(again["session_id"], None)
    h.roster[again["session_id"]] = {
        "config_json": json.dumps({"cwd": str(h.original)}),
        "plugin_metadata": json.dumps(h.sessions[again["session_id"]].plugin_metadata),
    }
    h.sessions.pop(again["session_id"], None)
    await h.host.cleanup_fork(again["session_id"])
    assert not Path(again["fork_root"]).exists()
