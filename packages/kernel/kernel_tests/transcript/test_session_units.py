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

"""会话单元生产测试：claude-jsonl 转换轨与转录源定位。"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from workflow_controller.transcript import (
    resolve_roster_source,
    resolve_transcript_source,
    write_opencode_session,
    write_opencode_subagents,
)
from workflow_controller.transcript.sources import resolve_claude_session_path


def _create_v1_tables(conn: sqlite3.Connection) -> None:
    """建 opencode v1 三表：session / message / part。"""
    conn.executescript("""
        CREATE TABLE session (
            id TEXT PRIMARY KEY, title TEXT, version TEXT, parent_id TEXT,
            time_created INTEGER, directory TEXT, summary_additions INTEGER,
            summary_deletions INTEGER, summary_files INTEGER
        );
        CREATE TABLE message (
            id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT
        );
        CREATE TABLE part (
            id TEXT PRIMARY KEY, message_id TEXT, time_created INTEGER, data TEXT
        );
    """)


def _create_v2_tables(conn: sqlite3.Connection) -> None:
    """建 opencode v2 两表：session_v2 / session_message。"""
    conn.executescript("""
        CREATE TABLE session_v2 (
            id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, version TEXT,
            directory TEXT, summary_additions INTEGER, summary_deletions INTEGER,
            summary_files INTEGER
        );
        CREATE TABLE session_message (
            id TEXT PRIMARY KEY, session_id TEXT, type TEXT, seq TEXT,
            time_created TEXT, data TEXT
        );
    """)


def _v1_assistant_message_data() -> str:
    """m2 assistant 行的 data JSON（modelID/tokens/finish 全量形态）。"""
    return json.dumps(
        {
            "role": "assistant",
            "modelID": "glm",
            "providerID": "z",
            "time": {"created": 1753696801000, "completed": 1753696807000},
            "tokens": {
                "input": 10,
                "output": 5,
                "reasoning": 7,
                "cache": {"read": 3, "write": 0},
            },
            "finish": "tool-calls",
        }
    )


def _v1_tool_part_data() -> str:
    """p2 工具 part 行的 data JSON（bash 调用 + 完成态 state）。"""
    return json.dumps(
        {
            "type": "tool",
            "tool": "bash",
            "callID": "c1",
            "input": {"cmd": "ls"},
            "output": "file-a",
            "state": {"status": "completed", "time": {"created": 1753696802000}},
        }
    )


def _synthetic_opencode_db(db_path: Path, session_id: str) -> None:
    conn = sqlite3.connect(str(db_path))
    _create_v1_tables(conn)
    conn.execute(
        "INSERT INTO session (id, time_created) VALUES (?, ?)",
        (session_id, 1753696800000),
    )
    conn.execute(
        "INSERT INTO message VALUES (?, ?, ?, ?)",
        ("m1", session_id, 1753696800000, json.dumps({"role": "user", "content": "请构建"})),
    )
    conn.execute(
        "INSERT INTO message VALUES (?, ?, ?, ?)",
        ("m2", session_id, 1753696801000, _v1_assistant_message_data()),
    )
    conn.execute(
        "INSERT INTO part VALUES (?, ?, ?, ?)",
        ("p0", "m2", 1753696801500, json.dumps({"type": "reasoning", "text": "思考中"})),
    )
    conn.execute(
        "INSERT INTO part VALUES (?, ?, ?, ?)",
        ("p1", "m2", 1753696801000, json.dumps({"type": "text", "text": "完成"})),
    )
    conn.execute(
        "INSERT INTO part VALUES (?, ?, ?, ?)",
        ("p2", "m2", 1753696802000, _v1_tool_part_data()),
    )
    conn.commit()
    conn.close()


def _v2_assistant_message_data() -> str:
    """m2 assistant 行的 data JSON（content 内嵌 reasoning/text/tool 三段）。"""
    return json.dumps(
        {
            "time": {"created": 1753696801000, "completed": 1753696807000},
            "agent": "simple-worker",
            "model": {"id": "glm", "providerID": "z"},
            "finish": "tool-calls",
            "cost": 0,
            "tokens": {
                "input": 10,
                "output": 5,
                "reasoning": 7,
                "cache": {"read": 3, "write": 0},
            },
            "content": [
                {"type": "reasoning", "text": "思考中"},
                {"type": "text", "text": "完成"},
                {
                    "type": "tool",
                    "id": "c1",
                    "name": "bash",
                    "executed": False,
                    "time": {"created": 1753696802000},
                    "state": {
                        "status": "completed",
                        "input": {"cmd": "ls"},
                        "content": [{"type": "text", "text": "file-a"}],
                        "metadata": {"exit": 0},
                    },
                },
            ],
        }
    )


def _v2_session_message_rows() -> list[tuple[str, str, str, str, str]]:
    """v2 会话的 session_message 行值（m0/m3 为非转录行，读取轨须跳过）。"""
    return [
        (
            "m0",
            "agent-switched",
            "1",
            "1753696800000",
            json.dumps({"time": {"created": 1753696800000}, "agent": "simple-worker"}),
        ),
        (
            "m1",
            "user",
            "5",
            "1753696800000",
            json.dumps({"time": {"created": 1753696800000}, "text": "请构建"}),
        ),
        ("m2", "assistant", "6", "1753696801000", _v2_assistant_message_data()),
        (
            "m3",
            "idle",
            "25",
            "1753696808000",
            json.dumps({"time": {"created": 1753696808000}, "outcome": "succeeded"}),
        ),
    ]


def _synthetic_opencode_db_v2(db_path: Path, session_id: str) -> None:
    """opencode ≥2.0.26 形状：session_v2 + session_message（内容内嵌 data JSON）。

    与 _synthetic_opencode_db 表示同一逻辑对话（时间戳/token/工具调用一致），
    供双轨等价性测试使用；seq/time_created 按 v2 实况为 TEXT。
    """
    conn = sqlite3.connect(str(db_path))
    _create_v2_tables(conn)
    conn.execute(
        "INSERT INTO session_v2 (id, version) VALUES (?, ?)",
        (session_id, "2.0.26"),
    )
    for row_id, mtype, seq, created, data in _v2_session_message_rows():
        conn.execute(
            "INSERT INTO session_message VALUES (?, ?, ?, ?, ?, ?)",
            (row_id, session_id, mtype, seq, created, data),
        )
    conn.commit()
    conn.close()


def _synthetic_subagent_db_v1(db_path: Path) -> None:
    """v1 库：父会话 ses_parent + 子代理 ses_sub（parent_id 关联）。"""
    conn = sqlite3.connect(str(db_path))
    _create_v1_tables(conn)
    conn.execute("INSERT INTO session (id) VALUES (?)", ("ses_parent",))
    conn.execute("INSERT INTO session (id) VALUES (?)", ("ses_solo",))
    conn.execute(
        "INSERT INTO session (id, parent_id, title) VALUES (?, ?, ?)",
        ("ses_sub", "ses_parent", "@(build) subagent"),
    )
    conn.execute(
        "INSERT INTO message VALUES (?, ?, ?, ?)",
        ("m1", "ses_sub", 1753696800000, json.dumps({"role": "user", "content": "子代理执行"})),
    )
    conn.commit()
    conn.close()


def _synthetic_subagent_db_v2(db_path: Path) -> None:
    """v2 库：父会话 ses_parent + 子代理 ses_sub（session_v2.parent_id 关联）。"""
    conn = sqlite3.connect(str(db_path))
    _create_v2_tables(conn)
    conn.execute("INSERT INTO session_v2 (id, version) VALUES (?, ?)", ("ses_parent", "2.0.26"))
    conn.execute(
        "INSERT INTO session_v2 (id, parent_id, title, version) VALUES (?, ?, ?, ?)",
        ("ses_sub", "ses_parent", "@(build) subagent", "2.0.26"),
    )
    conn.execute(
        "INSERT INTO session_message VALUES (?, ?, ?, ?, ?, ?)",
        (
            "m1",
            "ses_sub",
            "user",
            "1",
            "1753696800000",
            json.dumps({"time": {"created": 1753696800000}, "text": "子代理执行"}),
        ),
    )
    conn.commit()
    conn.close()


def test_write_opencode_subagents_v1(tmp_path: Path) -> None:
    """v1 库：子代理按 session.parent_id 定位并落 subagents/，meta name 派生去前缀。"""
    db_path = tmp_path / "v1.db"
    _synthetic_subagent_db_v1(db_path)
    session_dir = tmp_path / "unit"
    session_dir.mkdir()
    assert write_opencode_subagents(db_path, "ses_parent", session_dir) == 1
    sub_dir = session_dir / "subagents"
    assert (sub_dir / "ses_sub.jsonl").is_file()
    meta = json.loads((sub_dir / "ses_sub.meta.json").read_text(encoding="utf-8"))
    assert meta["name"] == "build"  # 复用 _subagent_name_from_title，非原始 "@(build) subagent"
    assert meta["x_context_bay"]["data"]["subagentSessionId"] == "ses_sub"
    # 无子代理的父会话：0 且不建 subagents 目录
    assert write_opencode_subagents(db_path, "ses_solo", tmp_path / "other") == 0
    assert not (tmp_path / "other" / "subagents").exists()


def test_write_opencode_subagents_v2(tmp_path: Path) -> None:
    """v2 库（session_v2/session_message）：子代理导出不静默失效。"""
    db_path = tmp_path / "v2.db"
    _synthetic_subagent_db_v2(db_path)
    session_dir = tmp_path / "unit"
    session_dir.mkdir()
    assert write_opencode_subagents(db_path, "ses_parent", session_dir) == 1
    sub_dir = session_dir / "subagents"
    assert (sub_dir / "ses_sub.jsonl").is_file()
    meta = json.loads((sub_dir / "ses_sub.meta.json").read_text(encoding="utf-8"))
    assert meta["name"] == "build"
    assert meta["x_context_bay"]["data"]["subagentSessionId"] == "ses_sub"


def test_write_opencode_subagents_no_session_table_is_silent_noop(tmp_path: Path) -> None:
    """父会话不在 v1/v2 任何轨：返回 0、不抛、不落残目录（表缺失不冒 OperationalError）。"""
    db_path = tmp_path / "empty.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE unrelated (id TEXT)")
    conn.commit()
    conn.close()
    session_dir = tmp_path / "unit"
    session_dir.mkdir()
    assert write_opencode_subagents(db_path, "ses_parent", session_dir) == 0
    assert not (session_dir / "subagents").exists()


def _assert_assistant_line(assistant: dict) -> None:
    """v1/v2 双轨共用的 assistant 行转换断言（model 前缀 / usage / content 三段）。"""
    assert assistant["message"]["id"] == "m2"
    assert assistant["message"]["model"] == "z/glm"
    assert assistant["message"]["usage"] == {
        "input_tokens": 10,
        "output_tokens": 5,
        "cache_read_input_tokens": 3,
        "cache_creation_input_tokens": None,
    }
    content = assistant["message"]["content"]
    assert content[0] == {"type": "thinking", "thinking": "思考中"}
    assert content[1] == {"type": "text", "text": "完成"}
    assert content[2] == {"type": "tool_use", "id": "c1", "name": "bash", "input": {"cmd": "ls"}}


def test_write_opencode_session_jsonl_shape(tmp_path: Path) -> None:
    db_path = tmp_path / "opencode.db"
    _synthetic_opencode_db(db_path, "ses_x")
    out_path = tmp_path / "ses_x.jsonl"
    assert write_opencode_session(db_path, "ses_x", out_path) is True
    lines = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 3  # user + assistant(thinking+text+tool_use) + tool_result
    assert lines[0] == {
        "type": "user",
        "message": {"role": "user", "content": "请构建"},
        "timestamp": "2025-07-28T10:00:00+00:00",
    }
    assistant = lines[1]
    # provider 前缀；正文无 <thinking> 污染（共用断言见 _assert_assistant_line）
    _assert_assistant_line(assistant)
    assert assistant["duration_ms"] == 6000
    assert assistant["stopReason"] == "tool-calls"
    xc = assistant["x_context_bay"]
    assert xc["schema"] == "cc-db-turn" and xc["version"] == 1
    assert xc["data"]["latencyMs"] == 6000 and xc["data"]["reasoningTokens"] == 7
    assert xc["data"]["modelId"] == "glm" and xc["data"]["providerId"] == "z"
    assert xc["data"]["toolCalls"] == [
        {
            "toolUseId": "c1",
            "state": "completed",
            "startedAt": "2025-07-28T10:00:02+00:00",
        }
    ]
    assert xc["data"]["turnKind"] == "normal"
    assert lines[2]["message"]["content"][0] == {
        "type": "tool_result",
        "tool_use_id": "c1",
        "content": "file-a",
        "is_error": False,
    }
    # 会话不在库内：单元缺席
    assert write_opencode_session(db_path, "ses_missing", tmp_path / "y.jsonl") is False


def test_resolve_roster_source_runtime_matrix(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OPENCODE_DB_PATH", str(tmp_path / "oc.db"))
    (tmp_path / "oc.db").write_bytes(b"")
    row = {
        "native_session_id": "ses_n",
        "config_json": json.dumps(
            {
                "runtime": None,
                "cwd": "/w/x",
                "env_overrides": {},
            }
        ),
    }
    # per-session runtime 缺省回落全局默认
    opencode = resolve_roster_source(row, default_runtime="opencode")
    assert (opencode.kind, opencode.framework) == ("opencode-db", "opencode")
    claude = resolve_roster_source(
        row | {"config_json": json.dumps({"runtime": "claude_code", "cwd": "/w/x"})},
        default_runtime="opencode",
    )
    assert claude.kind == "claude-jsonl"
    assert claude.path == (Path.home() / ".claude" / "projects" / "-w-x" / "ses_n.jsonl")
    direct = resolve_transcript_source("ses_m", runtime="claude_cli", cwd="/a")
    assert direct.path.name == "ses_m.jsonl"
    assert resolve_transcript_source(None, runtime="opencode") is None


def test_codex_source_uses_newest_main_rollout(tmp_path: Path) -> None:
    sessions = tmp_path / ".codex" / "sessions" / "2026" / "11" / "05"
    sessions.mkdir(parents=True)

    def rollout(name: str, thread_source: object = "user") -> Path:
        path = sessions / name
        path.write_text(
            json.dumps(
                {
                    "type": "session_meta",
                    "payload": {"id": "thread-1", "thread_source": thread_source},
                }
            ),
            encoding="utf-8",
        )
        return path

    rollout("rollout-old-thread-1.jsonl")
    expected = rollout("rollout-new-thread-1.jsonl")
    rollout("rollout-sub-thread-1.jsonl", thread_source={"subagent": {}})
    os.utime(sessions.joinpath("rollout-old-thread-1.jsonl"), ns=(1, 1))
    os.utime(expected, ns=(2, 2))

    source = resolve_transcript_source(
        "thread-1",
        runtime="codex_cli",
        cwd=str(tmp_path),
    )

    assert (source.framework, source.kind, source.path) == (
        "codex",
        "codex-jsonl",
        expected,
    )


def test_codex_source_missing_rollout_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Codex rollout not found"):
        resolve_transcript_source(
            "thread-1",
            runtime="codex_cli",
            cwd=str(tmp_path),
        )


def test_codex_source_skips_rollout_removed_before_stat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessions = tmp_path / ".codex" / "sessions"
    sessions.mkdir(parents=True)
    (sessions / "rollout-thread-1.jsonl").write_text(
        '{"type":"session_meta","payload":{"id":"thread-1"}}\n',
        encoding="utf-8",
    )

    def missing_stat(_path: Path) -> None:
        raise FileNotFoundError

    monkeypatch.setattr(Path, "stat", missing_stat)

    with pytest.raises(ValueError, match="Codex rollout not found"):
        resolve_transcript_source(
            "thread-1",
            runtime="codex_cli",
            cwd=str(tmp_path),
        )


def test_claude_source_prefers_config_and_requested_project(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("workflow_controller.transcript.sources._USER_HOME_ROOTS", ())
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    custom = tmp_path / "custom"
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(custom))
    expected = custom / "projects" / "-work" / "ses_1.jsonl"
    expected.parent.mkdir(parents=True)
    expected.write_text("{}\n")
    other = tmp_path / "home" / ".claude" / "projects" / "-old" / "ses_1.jsonl"
    other.parent.mkdir(parents=True)
    other.write_text("{}\n")

    assert resolve_claude_session_path("ses_1", "/work") == expected
    assert resolve_transcript_source("ses_1", runtime="claude_cli", cwd="/work").path == expected


def test_claude_source_fallback_uses_project_key(tmp_path: Path, monkeypatch) -> None:
    """transcript 未命中时 fallback 仍用 claude_project_key（非字母数字 → "-"），
    与 CLI 实际的 projects/ 布局一致，含 `.`/`_` 的 cwd 也不例外。
    """
    monkeypatch.setattr("workflow_controller.transcript.sources._USER_HOME_ROOTS", ())
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("CLAUDE_CONFIG_DIR", raising=False)
    src = resolve_transcript_source("ses_9", runtime="claude_cli", cwd="/a.b_c")
    assert src is not None
    assert src.path == Path.home() / ".claude" / "projects" / "-a-b-c" / "ses_9.jsonl"


def test_claude_legacy_fallback_requires_unique_match(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("workflow_controller.transcript.sources._USER_HOME_ROOTS", ())
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "custom"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    first = tmp_path / "custom" / "projects" / "-old" / "ses_2.jsonl"
    first.parent.mkdir(parents=True)
    first.write_text("{}\n")
    assert resolve_claude_session_path("ses_2", "/new") == first
    second = tmp_path / "home" / ".claude" / "projects" / "-other" / "ses_2.jsonl"
    second.parent.mkdir(parents=True)
    second.write_text("{}\n")
    assert resolve_claude_session_path("ses_2", "/new") is None
    assert resolve_claude_session_path("../ses_2", "/new") is None


def test_claude_session_id_rejects_glob_metacharacters(tmp_path: Path, monkeypatch) -> None:
    """session_id 含通配符时不得进入兜底 glob，避免命中非目标会话。"""
    monkeypatch.setattr("workflow_controller.transcript.sources._USER_HOME_ROOTS", ())
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "custom"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    only = tmp_path / "custom" / "projects" / "-work" / "only.jsonl"
    only.parent.mkdir(parents=True)
    only.write_text("{}\n")
    assert resolve_claude_session_path("only", "/work") == only
    assert resolve_claude_session_path("onl*", "/work") is None
    assert resolve_claude_session_path("onl[y]", "/work") is None
    assert resolve_transcript_source("onl*", runtime="claude_cli", cwd="/work") is None


def test_claude_roster_uses_session_environment(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("workflow_controller.transcript.sources._USER_HOME_ROOTS", ())
    path = tmp_path / "custom" / "projects" / "-work" / "ses_3.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("{}\n")
    row = {
        "native_session_id": "ses_3",
        "config_json": json.dumps(
            {
                "runtime": "claude_cli",
                "cwd": "/work",
                "env_overrides": {"CLAUDE_CONFIG_DIR": str(tmp_path / "custom")},
            }
        ),
    }
    assert resolve_roster_source(row).path == path


def test_resolve_claude_session_path_tolerates_unreadable_home(tmp_path: Path, monkeypatch) -> None:
    """共享主机存在不可读主目录时，探测不抛 PermissionError 而是跳过。

    用 chmod 000 的 fixture 目录模拟 /Users/Guest 这类 EACCES 主目录，
    且不 patch _USER_HOME_ROOTS（防回归：真实扫描路径必须吞 OSError）。
    """
    import stat

    users = tmp_path / "users"
    blocked = users / "blocked"
    blocked.mkdir(parents=True)
    blocked.chmod(0)
    monkeypatch.setattr("workflow_controller.transcript.sources._USER_HOME_ROOTS", (users,))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("CLAUDE_CONFIG_DIR", raising=False)

    try:
        # 无异常返回即可：不可读目录被跳过，其余候选正常参与探测
        assert resolve_claude_session_path("ses_x", "/w", scan_user_homes=True) is None
        assert (
            resolve_transcript_source(
                "ses_x",
                runtime="claude_cli",
                cwd="/w",
                scan_user_homes=True,
            ).path.name
            == "ses_x.jsonl"
        )
    finally:
        blocked.chmod(stat.S_IRWXU)


def test_graph_roster_uses_opencode_db_from_inherited_env(tmp_path: Path, monkeypatch) -> None:
    """Graph 的非空 env_overrides 不应遮蔽 OpenCode 实际使用的数据库。"""
    stale = tmp_path / "xdg" / "opencode" / "opencode.db"
    stale.parent.mkdir(parents=True)
    _synthetic_opencode_db(stale, "ses_stale")
    actual = tmp_path / "actual.db"
    _synthetic_opencode_db(actual, "ses_target")
    monkeypatch.delenv("OPENCODE_DB_PATH", raising=False)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("OPENCODE_DB", str(actual))

    row = {
        "native_session_id": "ses_target",
        "config_json": json.dumps(
            {
                "runtime": "opencode",
                "cwd": str(tmp_path),
                "env_overrides": {"WORK_DIR": str(tmp_path)},
            }
        ),
    }
    source = resolve_roster_source(row)
    assert source is not None and source.path == actual
    out = tmp_path / "ses_target.jsonl"
    assert write_opencode_session(source.path, "ses_target", out) is True
    assert out.is_file()


def test_v2_track_jsonl_shape_and_missing(tmp_path: Path) -> None:
    """v2 库（仅 session_v2/session_message）走转换轨；信封行跳过。"""
    db_path = tmp_path / "opencode-v2.db"
    _synthetic_opencode_db_v2(db_path, "ses_x")
    out_path = tmp_path / "ses_x.jsonl"
    assert write_opencode_session(db_path, "ses_x", out_path) is True
    lines = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 3  # agent-switched/idle 跳过：user + assistant + tool_result
    assistant = lines[1]
    # v2 model.id + providerID（共用断言见 _assert_assistant_line）
    _assert_assistant_line(assistant)
    assert lines[2]["message"]["content"][0] == {
        "type": "tool_result",
        "tool_use_id": "c1",
        "content": "file-a",
        "is_error": False,
    }
    assert write_opencode_session(db_path, "ses_missing", tmp_path / "y.jsonl") is False


def test_v1_v2_tracks_byte_equivalent(tmp_path: Path) -> None:
    """同一逻辑对话的 v1/v2 两库，转换产出逐字节一致（v2 轨不漂移判据）。"""
    v1_db = tmp_path / "v1.db"
    v2_db = tmp_path / "v2.db"
    _synthetic_opencode_db(v1_db, "ses_x")
    _synthetic_opencode_db_v2(v2_db, "ses_x")
    out_v1 = tmp_path / "v1.jsonl"
    out_v2 = tmp_path / "v2.jsonl"
    assert write_opencode_session(v1_db, "ses_x", out_v1) is True
    assert write_opencode_session(v2_db, "ses_x", out_v2) is True
    assert out_v1.read_bytes() == out_v2.read_bytes()


def test_dual_write_db_reads_both_schemas(tmp_path: Path) -> None:
    """双写库共存：v1 会话与 v2 会话各一，同一库内均可读（按会话落表探测）。"""
    db_path = tmp_path / "dual.db"
    _synthetic_opencode_db(db_path, "ses_old")
    _synthetic_opencode_db_v2(db_path, "ses_new")
    assert write_opencode_session(db_path, "ses_old", tmp_path / "o.jsonl") is True
    assert write_opencode_session(db_path, "ses_new", tmp_path / "n.jsonl") is True
    assert (tmp_path / "o.jsonl").read_bytes() == (tmp_path / "n.jsonl").read_bytes()
