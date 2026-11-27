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

"""外部 cannbot session db 导入测试：全链（真实 sqlite 源库 → 导入 → 会话可读）。

对齐插件 import-db 的关键断言口径（两点不回退）：导入出的会话可列出、
消息/工具结果可读（转写出的 opencode.db 为消息读取源），导入日志语义
与插件一致（阶段行 + 终态行 + 查询面）。
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from workflow_controller.core.checkpoint_compat import LegacyCheckpointReader
from workflow_controller.core.restore.import_runner import import_log_path_for
from workflow_controller.core.session_traceback import SessionTracebackHost
from workflow_controller.core.session_workspace import SessionWorkspaceHost

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")

# ── cannbot 源库构造（行字典 → 5 表 sqlite） ──────────────────────

_SESSION_COLUMNS = (
    "id",
    "taskId",
    "query",
    "framework",
    "model",
    "directory",
    "sourcePath",
    "rootExecutionId",
    "createdAt",
)
_TURN_COLUMNS = (
    "id",
    "sessionId",
    "turnIndex",
    "role",
    "content",
    "contentJson",
    "contentSummary",
    "agentName",
    "subagentName",
    "subagentSessionId",
    "parentExecutionId",
    "finishReason",
    "isSubagent",
    "createdAt",
    "completedAt",
    "model",
)
_TOOL_CALL_COLUMNS = (
    "id",
    "turnId",
    "toolCallId",
    "toolName",
    "argsJson",
    "resultJson",
    "state",
    "errorType",
    "errorMessage",
    "startedAt",
    "completedAt",
    "durationMs",
    "dispatchBridgeId",
    "isSkillRelated",
    "createdAt",
)
_BRIDGE_COLUMNS = (
    "id",
    "sessionId",
    "dispatchExecutionId",
    "dispatchTurnId",
    "dispatchToolCallId",
    "dispatchContent",
    "dispatchTimestamp",
    "responseExecutionId",
    "responseTurnId",
    "responseContent",
    "responseTimestamp",
    "subagentSessionId",
    "subagentType",
    "subagentName",
    "subagentTokens",
    "status",
)

_SESSION_DEFAULTS = {
    "id": "cmr_test",
    "taskId": "ses_main_001",
    "query": "do X",
    "framework": "opencode",
    "model": "m",
    "directory": "/workspace/orig",
    "sourcePath": "",
    "rootExecutionId": "exec_root",
    "createdAt": "2026-07-16T12:00:00.000Z",
}


def _turn_row(
    tid: str,
    *,
    idx: int = 0,
    role: str | None = "user",
    content: str = "",
    sub_sid: str | None = None,
    finish: str | None = None,
    at: str | None = None,
) -> dict[str, Any]:
    """Turn 行字典：主/子 agent 对话回合（缺列由建库补默认值）。"""
    row: dict[str, Any] = {
        "id": tid,
        "sessionId": "cmr_test",
        "turnIndex": idx,
        "role": role,
        "content": content,
        "createdAt": at,
    }
    if sub_sid is not None:
        row["isSubagent"] = True
        row["subagentSessionId"] = sub_sid
    if finish is not None:
        row["finishReason"] = finish
    return row


def _task_call_row(
    row_id: str,
    turn_id: str,
    call_id: str,
    sub_sid: str,
    sub_type: str = "developer",
    *,
    result: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """主 agent 的 task 派发 ToolCall 行字典（argsJson 内嵌子会话 id）。"""
    return {
        "id": row_id,
        "turnId": turn_id,
        "toolCallId": call_id,
        "toolName": "task",
        "argsJson": json.dumps(
            {
                "description": "do X",
                "prompt": "be X",
                "subagent_type": sub_type,
                "subagent_session_id": sub_sid,
            }
        ),
        "resultJson": result if result is not None else f"task_id: {sub_sid}",
        "startedAt": start,
        "completedAt": end,
    }


def _tool_call_row(
    row_id: str,
    turn_id: str,
    call_id: str,
    tool_name: str,
    args: dict[str, Any],
    *,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """任意工具的 ToolCall 行字典。"""
    return {
        "id": row_id,
        "turnId": turn_id,
        "toolCallId": call_id,
        "toolName": tool_name,
        "argsJson": json.dumps(args),
        "resultJson": "",
        "startedAt": start,
        "completedAt": end,
    }


def _bridge_row(dispatch_call_id: str, sub_sid: str, sub_type: str) -> dict[str, Any]:
    """InteractionBridge 行字典（派发 ↔ 子会话的桥接记录）。"""
    return {
        "id": "b1",
        "sessionId": "cmr_test",
        "dispatchToolCallId": dispatch_call_id,
        "subagentSessionId": sub_sid,
        "subagentType": sub_type,
        "subagentName": None,
        "subagentTokens": 100,
        "status": "completed",
        "dispatchTimestamp": None,
        "responseTimestamp": None,
    }


def _insert(
    con: sqlite3.Connection, table: str, columns: tuple[str, ...], row: dict[str, Any]
) -> None:
    """按列序补默认值后插入一行（None 列落 NULL）。"""
    values = tuple(row.get(col) for col in columns)
    placeholders = ",".join("?" for _ in columns)
    con.execute(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})", values)


def build_cannbot_db(tmp_path: Path, *, framework: str = "opencode") -> Path:
    """建一个最小可重建的 cannbot 源库：1 user + 1 assistant(task) + 子 write。"""
    turns = [
        _turn_row("t1", content="do X", at="2026-07-16T12:00:00.000Z"),
        _turn_row(
            "t2",
            idx=1,
            role="assistant",
            finish="tool-calls",
            content="<thinking>ok</thinking>Dispatching.",
            at="2026-07-16T12:00:01.000Z",
        ),
        _turn_row(
            "t3",
            content="sub prompt",
            sub_sid="ses_sub_001",
            at="2026-07-16T12:00:02.000Z",
        ),
        _turn_row(
            "t4",
            idx=1,
            role="assistant",
            finish="tool-calls",
            content="writing",
            sub_sid="ses_sub_001",
            at="2026-07-16T12:00:03.000Z",
        ),
    ]
    tool_calls = [
        _task_call_row(
            "tc1",
            "t2",
            "call_main_task_1",
            "ses_sub_001",
            result="task_id: ses_sub_001\n<task_result>ok</task_result>",
            start="2026-07-16T12:00:01.500Z",
            end="2026-07-16T12:00:10.000Z",
        ),
        _tool_call_row(
            "tc2",
            "t4",
            "call_sub_write_1",
            "write",
            {"filePath": "/workspace/orig/sub/a.txt", "content": "hello"},
            start="2026-07-16T12:00:03.500Z",
            end="2026-07-16T12:00:03.600Z",
        ),
    ]
    bridges = [_bridge_row("call_main_task_1", "ses_sub_001", "developer")]
    db = tmp_path / "cannbot.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE Session (id TEXT PRIMARY KEY, taskId TEXT, query TEXT, framework TEXT,"
        " model TEXT, directory TEXT, sourcePath TEXT, rootExecutionId TEXT, createdAt TEXT)"
    )
    con.execute(
        "CREATE TABLE Turn (id TEXT PRIMARY KEY, sessionId TEXT, turnIndex INTEGER, role TEXT,"
        " content TEXT, contentJson TEXT, contentSummary TEXT, agentName TEXT, subagentName TEXT,"
        " subagentSessionId TEXT, parentExecutionId TEXT, finishReason TEXT, isSubagent BOOLEAN,"
        " createdAt TEXT, completedAt TEXT, model TEXT)"
    )
    con.execute(
        "CREATE TABLE ToolCall (id TEXT PRIMARY KEY, turnId TEXT, toolCallId TEXT, toolName TEXT,"
        " argsJson TEXT, resultJson TEXT, state TEXT, errorType TEXT, errorMessage TEXT,"
        " startedAt TEXT, completedAt TEXT, durationMs INTEGER, dispatchBridgeId TEXT,"
        " isSkillRelated BOOLEAN, createdAt TEXT)"
    )
    con.execute("CREATE TABLE Execution (id TEXT PRIMARY KEY, sessionId TEXT, depth INTEGER)")
    con.execute(
        "CREATE TABLE InteractionBridge (id TEXT PRIMARY KEY, sessionId TEXT,"
        " dispatchExecutionId TEXT, dispatchTurnId TEXT, dispatchToolCallId TEXT,"
        " dispatchContent TEXT, dispatchTimestamp TEXT, responseExecutionId TEXT,"
        " responseTurnId TEXT, responseContent TEXT, responseTimestamp TEXT,"
        " subagentSessionId TEXT, subagentType TEXT, subagentName TEXT,"
        " subagentTokens INTEGER DEFAULT 0, status TEXT)"
    )
    session_row = dict(_SESSION_DEFAULTS)
    session_row["framework"] = framework
    _insert(con, "Session", _SESSION_COLUMNS, session_row)
    for row in turns:
        merged = dict(row)
        merged["isSubagent"] = 1 if merged.get("isSubagent") else 0
        merged.setdefault("turnIndex", 0)
        _insert(con, "Turn", _TURN_COLUMNS, merged)
    for row in tool_calls:
        merged = dict(row)
        merged.setdefault("state", "completed")
        merged.setdefault("durationMs", 0)
        merged["isSkillRelated"] = 1 if merged.get("isSkillRelated") else 0
        _insert(con, "ToolCall", _TOOL_CALL_COLUMNS, merged)
    for row in bridges:
        _insert(con, "InteractionBridge", _BRIDGE_COLUMNS, row)
    con.commit()
    con.close()
    return db


def build_source_opencode_db(tmp_path: Path) -> Any:
    """建一个最小 source opencode.db（schema 模板：迁移表 + 四表 + project）。"""
    src = tmp_path / "source_opencode" / "opencode.db"
    src.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(src)
    con.execute("CREATE TABLE __drizzle_migrations (id TEXT, hash TEXT, created_at INTEGER)")
    con.execute("INSERT INTO __drizzle_migrations VALUES ('0001', 'fakehash', 0)")
    con.execute(
        "CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT, parent_id TEXT, slug TEXT,"
        " directory TEXT, title TEXT, version TEXT, share_url TEXT, summary_additions INTEGER,"
        " summary_deletions INTEGER, summary_files INTEGER, summary_diffs TEXT, revert TEXT,"
        " permission TEXT, time_created INTEGER, time_updated INTEGER, time_compacting INTEGER,"
        " time_archived INTEGER, workspace_id TEXT, path TEXT, agent TEXT, model TEXT, cost REAL,"
        " tokens_input INTEGER, tokens_output INTEGER, tokens_reasoning INTEGER,"
        " tokens_cache_read INTEGER, tokens_cache_write INTEGER, metadata TEXT)"
    )
    con.execute(
        "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER,"
        " time_updated INTEGER, data TEXT)"
    )
    con.execute(
        "CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT,"
        " time_created INTEGER, time_updated INTEGER, data TEXT)"
    )
    con.execute(
        "CREATE TABLE project (id TEXT PRIMARY KEY, worktree TEXT, name TEXT, vcs TEXT,"
        " icon_url TEXT, icon_color TEXT, time_created INTEGER, time_updated INTEGER,"
        " time_initialized INTEGER, sandboxes TEXT, commands TEXT, icon_url_override TEXT)"
    )
    for table in ("todo", "event", "event_sequence", "session_input", "session_message"):
        con.execute(f"CREATE TABLE {table} (id TEXT PRIMARY KEY)")
    con.commit()
    con.close()
    return src


# ── 导入装配（端口替身 + 真实 git/sqlite 产物） ───────────────────


class ImportHarness:
    """导入装配：真实重建链 + 会话创建/持久化/事件端口替身。"""

    def __init__(self, tmp_path: Path, source_db: Path) -> None:
        self.tmp = tmp_path
        self.db_path = tmp_path / "events.db"
        self.created: list[Any] = []
        self.stored_events: list[Any] = []
        self.workspace_host = SessionWorkspaceHost(
            checkpoint_db_path=lambda: self.db_path,
            resolve_session=self._resolve_session,
            persist_session=self._persist_session,
            stop_session=self._noop_stop,
            start_resumed_session=self._noop_start,
        )
        self.host = SessionTracebackHost(
            checkpoint_db_path=lambda: self.db_path,
            resolve_session=self._resolve_session,
            load_roster_session=self._load_roster,
            session_workspaces=self.workspace_host,
            start_resumed_session=self._noop_start,
            create_session=self._create_session,
            persist_session=self._persist_session,
            store_events=self._store_events,
            default_runtime=lambda: "claude_sdk",
            resolve_runtime=lambda name: name or "claude_sdk",
            source_opencode_db=lambda: source_db,
        )

    async def _resolve_session(self, session_id: str) -> Any | None:
        return next((s for s in self.created if s.id == session_id), None)

    async def _load_roster(self, session_id: str) -> dict[str, Any] | None:
        return None

    async def _create_session(self, config: Any) -> Any:
        session = SimpleNamespace(
            id=uuid.uuid4().hex,
            sdk_session_id=None,
            loop_status="",
            plugin_metadata={},
            config=config,
        )
        self.created.append(session)
        return session

    async def _persist_session(self, session: Any) -> None:
        """落盘端口替身（本套件不断言落盘调用）。"""

    async def _store_events(self, events: list[Any]) -> None:
        self.stored_events.extend(events)

    async def _noop_stop(self, session_id: str) -> None:
        return None

    async def _noop_start(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("导入链不应启动恢复会话")

    async def import_db(self, cannbot_db: Path, target_cwd: Path) -> dict[str, Any]:
        # 与内核 API 层同径：导入即写日志（查询面随时可下载）
        return await self.host.import_session(
            str(cannbot_db), target_cwd, log_path=import_log_path_for(target_cwd)
        )


async def test_import_full_chain_session_readable(tmp_path: Path) -> None:
    """全链导入：会话可列出、消息/工具结果可读、检查点落库可查。"""
    cannbot_db = build_cannbot_db(tmp_path)
    source_db = build_source_opencode_db(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    harness = ImportHarness(tmp_path, source_db)

    result = await harness.import_db(cannbot_db, target)

    assert result["ok"] is True, result.get("error")
    assert result["sdk_session_id"] == "ses_main_001"
    assert result["fork_cwd"] == str(target)
    # 会话已注册（可列出）：worker + opencode + XDG 指向重建目录 + 已跑完态
    assert len(harness.created) == 1
    session = harness.created[0]
    assert session.id == result["session_id"]
    assert session.sdk_session_id == "ses_main_001"
    assert session.loop_status == "yes"
    assert session.config.cwd == str(target)
    assert session.config.runtime == "opencode"
    assert session.config.env_overrides["XDG_DATA_HOME"] == str(target / ".workflow-controller")
    # 消息/工具结果可读：转写出的 opencode.db 是会话消息读取源
    fork_db = target / ".workflow-controller" / "opencode" / "opencode.db"
    assert fork_db.is_file()
    assert str(fork_db) == result["opencode_db_path"]
    con = sqlite3.connect(fork_db)
    parts = con.execute(
        "SELECT id, data FROM part WHERE session_id = ? ORDER BY time_created", ("ses_main_001",)
    ).fetchall()
    con.close()
    tool_parts = [
        (row[0], json.loads(row[1])) for row in parts if json.loads(row[1])["type"] == "tool"
    ]
    task_part_id, task_part = next(p for p in tool_parts if p[1]["tool"] == "task")
    assert "ses_sub_001" in task_part["state"]["output"]
    # 检查点落检查点库（与读取面同库、不落 target_cwd）
    reader = LegacyCheckpointReader(harness.db_path)
    listed = reader.list_for_session(session.id)
    assert len(listed) == 2
    assert [cp.trigger for cp in listed] == ["subagent_start", "session_complete"]
    assert listed[0].agent_id == "ses_sub_001"
    assert listed[0].main_part_id == task_part_id
    assert not (target / ".workflow-controller" / "events.db").exists()
    # 统一查询面合并可见（快照代标注）
    merged = await harness.host.list_checkpoints(session.id)
    assert [row["checkpoint_id"] for row in merged] == [cp.checkpoint_id for cp in listed]
    assert all(row["generation"] == "snapshot" for row in merged)
    # 文件态重放产物在 target 可见
    assert (target / "sub" / "a.txt").read_text(encoding="utf-8") == "hello"


async def test_import_metadata_and_synthesized_events(tmp_path: Path) -> None:
    """溯源 metadata 与合成历史事件（SUBAGENT 对 + STEP_FINISH）与插件口径一致。"""
    cannbot_db = build_cannbot_db(tmp_path)
    source_db = build_source_opencode_db(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    harness = ImportHarness(tmp_path, source_db)

    result = await harness.import_db(cannbot_db, target)

    assert result["ok"] is True, result.get("error")
    session = harness.created[0]
    birth = session.plugin_metadata.get("agent_traceback", {}).get("birth_traceback", {})
    assert birth.get("reconstructed_from") == str(cannbot_db)
    assert birth.get("origin_sdk_session_id") == "ses_main_001"
    assert birth.get("fork_cwd") == str(target)
    assert birth.get("checkpoints") == 2
    assert birth.get("created_git_dir") is True
    assert birth.get("created_opencode_dir") is False
    assert (target / ".git").is_dir()

    event_types = [e.event_type.value for e in harness.stored_events]
    assert "subagent_start" in event_types and "subagent_stop" in event_types
    assert event_types.count("step_finish") == 1
    start = next(e for e in harness.stored_events if e.event_type.value == "subagent_start")
    assert start.data["agent_id"] == "ses_sub_001"
    assert start.data["parent_agent_id"] == ""
    finish = next(e for e in harness.stored_events if e.event_type.value == "step_finish")
    assert finish.data["num_turns"] == 2


async def test_import_loop_derivation_and_yaml_override(tmp_path: Path) -> None:
    """无 yaml 时 loop 从检查点推导（下限 10）；yaml 显式配置优先。"""
    cannbot_db = build_cannbot_db(tmp_path)
    source_db = build_source_opencode_db(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    harness = ImportHarness(tmp_path, source_db)
    result = await harness.import_db(cannbot_db, target)
    assert result["ok"] is True
    assert harness.created[0].config.loop is not None
    assert harness.created[0].config.loop.max_iterations == 10

    target2 = tmp_path / "target2"
    target2.mkdir()
    (target2 / "workflow-controller.yaml").write_text(
        "loop:\n  max_iterations: 50\n", encoding="utf-8"
    )
    harness2 = ImportHarness(tmp_path, source_db)
    result2 = await harness2.import_db(cannbot_db, target2)
    assert result2["ok"] is True
    assert harness2.created[0].config.loop.max_iterations == 50


async def test_import_failure_paths(tmp_path: Path) -> None:
    """非 cannbot 库与不支持的 framework 明确报错（口径与插件一致）。"""
    source_db = build_source_opencode_db(tmp_path)
    bad_db = tmp_path / "bad.db"
    con = sqlite3.connect(bad_db)
    con.execute("CREATE TABLE foo (id TEXT)")
    con.commit()
    con.close()
    harness = ImportHarness(tmp_path, source_db)
    result = await harness.import_db(bad_db, tmp_path / "target")
    assert result["ok"] is False
    assert "not a cannbot session db" in result["error"]
    assert result["session_id"] == "" and result["checkpoints"] == []

    unsupported = build_cannbot_db(tmp_path / "cpx", framework="claude_sdk")
    harness2 = ImportHarness(tmp_path, source_db)
    result2 = await harness2.import_db(unsupported, tmp_path / "target")
    assert result2["ok"] is False
    assert "unsupported framework" in result2["error"]
    assert "claude-code-proxy" in result2["error"]


async def test_import_claude_code_proxy_framework(tmp_path: Path) -> None:
    """framework=claude-code-proxy（经方言归一化）可导入。"""
    cannbot_db = build_cannbot_db(tmp_path / "cpx", framework="claude-code-proxy")
    source_db = build_source_opencode_db(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    harness = ImportHarness(tmp_path, source_db)
    result = await harness.import_db(cannbot_db, target)
    assert result["ok"] is True, result.get("error")
    assert result["sdk_session_id"] == "ses_main_001"


async def test_import_log_semantics(tmp_path: Path) -> None:
    """导入日志语义：阶段行齐备、终态行收口、查询面缺席返回不存在。"""
    cannbot_db = build_cannbot_db(tmp_path)
    source_db = build_source_opencode_db(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    harness = ImportHarness(tmp_path, source_db)

    result = await harness.import_db(cannbot_db, target)

    log_file = target / ".workflow-controller" / "reconstruct" / "reconstruction.log"
    text = log_file.read_text(encoding="utf-8")
    for phase in (
        "phase=db_read",
        "phase=transcribe",
        "phase=replay_start",
        "phase=checkpoint_derive",
        "phase=register",
        f"phase=complete session_id={result['session_id'][:12]}",
    ):
        assert phase in text, phase
    # 查询面：读回全文；缺席目录返回不存在
    data = harness.host.read_import_log(str(target))
    assert data["exists"] is True
    assert "phase=complete" in data["content"]
    missing = harness.host.read_import_log(str(tmp_path / "nowhere"))
    assert missing["exists"] is False


async def test_replay_runtime_probe(tmp_path: Path) -> None:
    """重放 runtime 探测面：池内会话按配置返回，不在场返回空串。"""
    cannbot_db = build_cannbot_db(tmp_path)
    source_db = build_source_opencode_db(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    harness = ImportHarness(tmp_path, source_db)
    result = await harness.import_db(cannbot_db, target)
    assert result["ok"] is True

    # 导入会话显式配置 runtime=opencode → 探测面直接命中
    runtime = await harness.host.replay_runtime(result["session_id"])
    assert runtime == "opencode"
    assert await harness.host.replay_runtime("missing-session") == ""


async def test_import_concurrent_same_target_rejected(tmp_path: Path) -> None:
    """同一 target_cwd 已有导入在跑时立即报错（不排队）。"""
    import asyncio

    cannbot_db = build_cannbot_db(tmp_path)
    source_db = build_source_opencode_db(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    harness = ImportHarness(tmp_path, source_db)

    first = asyncio.create_task(harness.import_db(cannbot_db, target))
    await asyncio.sleep(0)  # 让首个导入先拿到互斥锁
    second = await harness.import_db(cannbot_db, target)
    assert second["ok"] is False
    assert "already running" in second["error"]
    assert (await first)["ok"] is True
