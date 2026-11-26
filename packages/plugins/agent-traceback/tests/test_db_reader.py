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

"""``db_reader.py`` 单元测试 — cannbot db 读取 + 时间线交错排序。

Hermetic：每个测试用 tmp_path 造一个最小 cannbot-style SQLite db（5 表），
通过 CannbotDbReader 读出，校验时间线交错顺序正确。不碰真实样例 db。
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from workflow_controller.plugins.agent_traceback.session_reconstructor.db_reader import (
    CannbotDbReader,
    _iso_to_ms,
)

from ._fixtures import (
    build_cannbot_db,
    cannbot_bridge_row,
    cannbot_execution_row,
    cannbot_task_call_row,
    cannbot_tool_call_row,
    cannbot_turn_row,
)
from ._fixtures import (
    can_open_readonly as _can_open_readonly,
)

_DB_READER_SESSION_DEFAULTS = {
    "id": "cmr_test_session_id",
    "taskId": "ses_main_001",
    "query": "please do X",
    "framework": "opencode",
    "model": "cannbot/glm-5.2",
    "directory": "/workspace/orig",
    "sourcePath": "/root/.local/share/opencode/opencode.db",
    "rootExecutionId": "exec_root",
    "createdAt": "2026-07-16T12:00:00.000Z",
}


def _make_cannbot_db(
    tmp_path: Path,
    *,
    session_row: dict | None = None,
    turns: list[dict] | None = None,
    tool_calls: list[dict] | None = None,
    executions: list[dict] | None = None,
    bridges: list[dict] | None = None,
) -> Path:
    """建 cannbot db（本文件统一 Session 默认行 + 默认 sessionId）。"""
    return build_cannbot_db(
        tmp_path,
        session_row=session_row,
        turns=turns,
        tool_calls=tool_calls,
        executions=executions,
        bridges=bridges,
        session_defaults=_DB_READER_SESSION_DEFAULTS,
        default_session_id="cmr_test_session_id",
    )


# ── _iso_to_ms ──────────────────────────────────────────────────────


class TestIsoToMs:
    @staticmethod
    def test_with_millis_z():
        # 2026-07-17T01:56:42.863Z → 应转成毫秒 epoch
        ms = _iso_to_ms("2026-07-17T01:56:42.863Z")
        assert ms > 0
        # 末 3 位是毫秒
        assert ms % 1000 == 863

    @staticmethod
    def test_without_millis():
        ms = _iso_to_ms("2026-07-17T01:56:42Z")
        assert ms > 0
        assert ms % 1000 == 0

    @staticmethod
    def test_no_trailing_z():
        ms = _iso_to_ms("2026-07-17T01:56:42.500")
        assert ms > 0
        assert ms % 1000 == 500

    @staticmethod
    def test_none_empty():
        assert _iso_to_ms(None) == 0
        assert _iso_to_ms("") == 0

    @staticmethod
    def test_invalid():
        assert _iso_to_ms("not a date") == 0


# ── validate ───────────────────────────────────────────────────────


class TestValidate:
    @staticmethod
    def test_valid_db(tmp_path: Path):
        db = _make_cannbot_db(tmp_path)
        with CannbotDbReader(db) as r:
            assert r.validate() is True

    @staticmethod
    def test_missing_table(tmp_path: Path):
        # 只建 Session 表 → validate 应 False
        db = tmp_path / "partial.db"
        con = sqlite3.connect(db)
        con.execute("CREATE TABLE Session (id TEXT)")
        con.execute("INSERT INTO Session VALUES ('x')")
        con.commit()
        con.close()
        with CannbotDbReader(db) as r:
            assert r.validate() is False

    @staticmethod
    def test_not_sqlite(tmp_path: Path):
        p = tmp_path / "not_a_db.db"
        p.write_text("not sqlite content")
        with CannbotDbReader(p) as r:
            assert r.validate() is False

    @staticmethod
    def test_missing_file(tmp_path: Path):
        # 不用 __enter__（会抛 FileNotFoundError）；validate 应直接返回 False
        r = CannbotDbReader(tmp_path / "nope.db")
        try:
            assert r.validate() is False
        finally:
            r.close()


# ── read_session ────────────────────────────────────────────────────


class TestReadSession:
    @staticmethod
    def test_reads_top_level_meta(tmp_path: Path):
        db = _make_cannbot_db(
            tmp_path,
            session_row={
                "taskId": "ses_main_001",
                "query": "please do X",
                "framework": "opencode",
                "model": "cannbot/glm-5.2",
                "directory": "/workspace/orig",
                "sourcePath": "/root/.local/share/opencode/opencode.db",
                "rootExecutionId": "exec_root",
            },
        )
        with CannbotDbReader(db) as r:
            meta = r.read_session()
        assert meta.native_session_id == "ses_main_001"
        assert meta.initial_prompt == "please do X"
        assert meta.framework == "opencode"
        assert meta.model == "cannbot/glm-5.2"
        assert meta.origin_cwd == "/workspace/orig"
        assert meta.source_path == "/root/.local/share/opencode/opencode.db"
        assert meta.root_execution_id == "exec_root"

    @staticmethod
    def test_empty_session_raises(tmp_path: Path):
        # 建 5 表（Session 用完整 schema）但 Session 不插行
        db = tmp_path / "empty.db"
        con = sqlite3.connect(db)
        con.execute(
            "CREATE TABLE Session (id TEXT PRIMARY KEY, taskId TEXT, query TEXT, "
            "framework TEXT, model TEXT, directory TEXT, sourcePath TEXT, "
            "rootExecutionId TEXT, createdAt TEXT)"
        )
        for t in ("Turn", "ToolCall", "Execution", "InteractionBridge"):
            con.execute(f"CREATE TABLE {t} (id TEXT)")
        con.commit()
        con.close()
        with CannbotDbReader(db) as r, pytest.raises(ValueError):
            r.read_session()


# ── read_timeline ────────────────────────────────────────────────────


def _interleaved_turns() -> list[dict]:
    """交错用例的 4 个 Turn 行（主 user/assistant + 子 user/assistant）。"""
    return [
        cannbot_turn_row("t1", content="please do X", at="2026-07-16T12:00:00.000Z"),
        cannbot_turn_row(
            "t2",
            idx=1,
            role="assistant",
            finish="tool-calls",
            content="<thinking>ok</thinking>",
            at="2026-07-16T12:00:01.000Z",
        ),
        # 子 agent turn（user 起始 + assistant 结束）
        cannbot_turn_row(
            "t3",
            content="sub task prompt",
            sub_sid="ses_sub_001",
            at="2026-07-16T12:00:02.000Z",
        ),
        cannbot_turn_row(
            "t4",
            idx=1,
            role="assistant",
            finish="tool-calls",
            content="I will write",
            sub_sid="ses_sub_001",
            at="2026-07-16T12:00:03.000Z",
        ),
    ]


class TestReadTimeline:
    @staticmethod
    def test_task_args_fallback_when_bridge_missing(tmp_path: Path):
        """若 InteractionBridge 缺失，task argsJson 的 subagent_session_id 兜底。"""
        # 与上面同样的 db 但不写 bridge
        turns = [
            cannbot_turn_row("t1", content="do X", at="2026-07-16T12:00:00.000Z"),
            cannbot_turn_row(
                "t2",
                idx=1,
                role="assistant",
                finish="tool-calls",
                content="ok",
                at="2026-07-16T12:00:01.000Z",
            ),
            cannbot_turn_row(
                "t3", content="sub", sub_sid="ses_sub_002", at="2026-07-16T12:00:02.000Z"
            ),
        ]
        tool_calls = [
            cannbot_task_call_row(
                "tc1",
                "t2",
                "call_main_task_2",
                "ses_sub_002",
                start="2026-07-16T12:00:01.500Z",
            ),
        ]
        db = _make_cannbot_db(tmp_path, turns=turns, tool_calls=tool_calls)
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        # 子 turn 仍能被交错插入（靠 argsJson 的 subagent_session_id）
        sub_entries = [e for e in tl if e.role == "subagent"]
        assert sub_entries
        assert all(e.subagent_session_id == "ses_sub_002" for e in sub_entries)

    @staticmethod
    def test_orphan_subagent_appended_at_end(tmp_path: Path):
        """孤儿子 turn（无匹配主 task call）按时间序追加。"""
        turns = [
            cannbot_turn_row("t1", content="do X", at="2026-07-16T12:00:00.000Z"),
            # 孤儿子 turn（subagentSessionId 不在任何 bridge / task argsJson 里）
            cannbot_turn_row(
                "t_orphan",
                content="orphan",
                sub_sid="ses_orphan",
                at="2026-07-16T12:00:05.000Z",
            ),
        ]
        db = _make_cannbot_db(tmp_path, turns=turns)
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        # 孤儿应出现在末尾
        assert tl[-1].subagent_session_id == "ses_orphan"

    @staticmethod
    def test_null_role_turn_kept(tmp_path: Path):
        """role IS NULL 的 turn 不是 system，不应被 WHERE != 'system' 滤掉。"""
        turns = [
            cannbot_turn_row("t1", content="x", at="2026-07-16T12:00:00.000Z"),
            cannbot_turn_row(
                "t2",
                idx=1,
                role=None,
                content="null role turn",
                at="2026-07-16T12:00:01.000Z",
            ),
        ]
        db = _make_cannbot_db(tmp_path, turns=turns)
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        assert {e.turn["id"] for e in tl if e.kind == "turn"} == {"t1", "t2"}

    @staticmethod
    def test_null_started_at_fallback(tmp_path: Path):
        """startedAt 为 NULL 时退到 completedAt。"""
        turns = [
            cannbot_turn_row("t1", content="x", at="2026-07-16T12:00:00.000Z"),
            cannbot_turn_row(
                "t2",
                idx=1,
                role="assistant",
                finish="tool-calls",
                content="ok",
                at="2026-07-16T12:00:01.000Z",
            ),
        ]
        tool_calls = [
            cannbot_tool_call_row(
                "tc1",
                "t2",
                "call_x",
                "bash",
                {"command": "pwd"},
                end="2026-07-16T12:00:02.000Z",
            ),
        ]
        db = _make_cannbot_db(tmp_path, turns=turns, tool_calls=tool_calls)
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        # 找到 tc1 entry，其 ts 应来自 completedAt
        tc_entry = next(e for e in tl if e.kind == "tool_call")
        assert tc_entry.ts > 0

    @staticmethod
    def test_sample_db_shape():
        """读真实样例 db（仅校验形状，不依赖其内容正确性）。

        通过环境变量 ``WORKFLOW_CONTROLLER_TEST_SAMPLE_CANNBOT_DB`` 指向本地样例 db
        （cannbot session 导出的脱敏副本）；未设置时 skip（CI 友好），
        不可读（如 macOS TCC/隔离属性）时同样 skip。
        行数断言对应该 canonical 样例，其他样例需自行调整断言。
        """
        sample_env = os.environ.get("WORKFLOW_CONTROLLER_TEST_SAMPLE_CANNBOT_DB", "")
        if not sample_env:
            pytest.skip("WORKFLOW_CONTROLLER_TEST_SAMPLE_CANNBOT_DB not set (optional sample db)")
        sample = Path(sample_env)
        if not sample.is_file():
            pytest.skip(f"sample cannbot db not available: {sample}")
        if not _can_open_readonly(sample):
            pytest.skip(f"sample cannbot db not readable (TCC/quarantine?): {sample}")
        with CannbotDbReader(sample) as r:
            assert r.validate() is True
            meta = r.read_session()
            assert meta.framework == "opencode"
            assert meta.native_session_id.startswith("ses_")
            tl = r.read_timeline()
            # 样例 db 含 1962 turn + 2186 tool_call = 4148 entries
            assert len(tl) == 1962 + 2186
            # 至少应有 41 个 main 的 task tool_call（设计 §2.2）
            main_tasks = []
            for e in tl:
                if not (
                    e.role == "main"
                    and e.kind == "tool_call"
                    and e.tool_call
                    and e.tool_call.get("toolName") == "task"
                ):
                    continue
                main_tasks.append(e)
            assert len(main_tasks) >= 40

    @staticmethod
    def _make_interleaved_db(tmp_path: Path) -> Path:
        """建一个最小交错 db：1 个主 turn(user) + 1 主 turn(assistant, 含 task) +
        子 agent 1 turn + 1 write tool_call + 子 agent 终 turn。
        """
        tool_calls = [
            # 主 agent 的 task tool_call
            cannbot_task_call_row(
                "tc1",
                "t2",
                "call_main_task_1",
                "ses_sub_001",
                result="task_id: ses_sub_001\n<task_result>ok</task_result>",
                start="2026-07-16T12:00:01.500Z",
                end="2026-07-16T12:00:10.000Z",
            ),
            # 子 agent 的 write tool_call
            cannbot_tool_call_row(
                "tc2",
                "t4",
                "call_sub_write_1",
                "write",
                {"filePath": "/workspace/orig/out.txt", "content": "hello"},
                start="2026-07-16T12:00:03.500Z",
                end="2026-07-16T12:00:03.600Z",
            ),
        ]
        executions = [
            cannbot_execution_row("exec_root", "ses_main_001"),
            cannbot_execution_row(
                "exec_sub_001",
                "ses_sub_001",
                agent_name="developer",
                sub=True,
                sub_type="developer",
                parent="exec_root",
            ),
        ]
        bridges = [cannbot_bridge_row("call_main_task_1", "ses_sub_001", "developer")]
        return _make_cannbot_db(
            tmp_path,
            turns=_interleaved_turns(),
            tool_calls=tool_calls,
            executions=executions,
            bridges=bridges,
        )

    def test_interleave_order(self, tmp_path: Path):
        db = self._make_interleaved_db(tmp_path)
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        # 期望顺序按 ts 升序交错：main 的 t1/t2/tc1 → subagent 的 t3/t4/tc2
        assert len(tl) == 6
        assert tl[0].kind == "turn" and tl[0].role == "main"
        assert tl[0].turn and tl[0].turn["id"] == "t1"
        assert tl[1].kind == "turn" and tl[1].turn and tl[1].turn["id"] == "t2"
        assert (
            tl[2].kind == "tool_call"
            and tl[2].tool_call
            and tl[2].tool_call["toolCallId"] == "call_main_task_1"
        )
        # 子 agent 的 entries 在主 task 之后
        for e in tl[3:]:
            assert e.role == "subagent"
            assert e.subagent_session_id == "ses_sub_001"
            assert e.parent_task_call_id == "call_main_task_1"
        assert tl[3].turn and tl[3].turn["id"] == "t3"
        assert tl[4].turn and tl[4].turn["id"] == "t4"
        assert tl[5].tool_call and tl[5].tool_call["toolCallId"] == "call_sub_write_1"


# ── 框架方言（claude-code-proxy）───────────────────────────────────


class TestToolNameNormalization:
    @staticmethod
    def test_cpx_aliases(tmp_path: Path):
        """claude-code-proxy 的 Claude 原生工具名 → opencode 方言。"""
        cpx_started = (
            "2026-10-28T13:00:01.000Z",
            "2026-10-28T13:00:02.000Z",
            "2026-10-28T13:00:03.000Z",
            "2026-10-28T13:00:04.000Z",
            "2026-10-28T13:00:05.000Z",
            "2026-10-28T13:00:06.000Z",
        )
        cpx_names = ("Agent", "Write", "Edit", "Bash", "Read", "MultiEdit")
        db = _make_cannbot_db(
            tmp_path,
            tool_calls=[
                cannbot_tool_call_row(f"tc{i + 1}", "t1", f"c{i + 1}", name, start=at)
                for i, (name, at) in enumerate(zip(cpx_names, cpx_started, strict=True))
            ],
            turns=[
                cannbot_turn_row(
                    "t1", role="assistant", content="x", at="2026-10-28T13:00:00.000Z"
                ),
            ],
        )
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        names = {e.tool_call["id"]: e.tool_call["toolName"] for e in tl if e.kind == "tool_call"}
        assert names == {
            "tc1": "task",
            "tc2": "write",
            "tc3": "edit",
            "tc4": "bash",
            "tc5": "read",
            "tc6": "multi_edit",
        }

    @staticmethod
    def test_opencode_and_unknown_names_passthrough(tmp_path: Path):
        """opencode 原生名与未知工具名原样透传。"""
        passthrough_turn = cannbot_turn_row(
            "t1", role="assistant", content="x", at="2026-10-28T13:00:00.000Z"
        )
        db = _make_cannbot_db(
            tmp_path,
            turns=[passthrough_turn],
            tool_calls=[
                cannbot_tool_call_row("tc1", "t1", "c1", "task", start="2026-10-28T13:00:01.000Z"),
                cannbot_tool_call_row(
                    "tc2", "t1", "c2", "SendMessage", start="2026-10-28T13:00:02.000Z"
                ),
                cannbot_tool_call_row(
                    "tc3",
                    "t1",
                    "c3",
                    "mcp__codebase-memory-mcp__search_graph",
                    start="2026-10-28T13:00:03.000Z",
                ),
            ],
        )
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        names = {e.tool_call["id"]: e.tool_call["toolName"] for e in tl if e.kind == "tool_call"}
        assert names == {
            "tc1": "task",
            "tc2": "SendMessage",
            "tc3": "mcp__codebase-memory-mcp__search_graph",
        }


def _orphan_nested_rows(
    *,
    grandchild_row_first: bool = False,
) -> tuple[list[dict], list[dict], list[dict]]:
    """孤儿祖先 + 嵌套孙用例的行数据；grandchild_row_first 时孙 turn 行序在前。"""
    turns = [
        cannbot_turn_row("t1", content="do X", at="2026-10-28T13:00:00.000Z"),
        cannbot_turn_row(
            "t2",
            content="sub prompt",
            sub_sid="sub-orphan",
            at="2026-10-28T13:00:02.000Z",
        ),
        cannbot_turn_row(
            "t3",
            idx=1,
            role="assistant",
            content="nesting",
            sub_sid="sub-orphan",
            at="2026-10-28T13:00:03.000Z",
        ),
        cannbot_turn_row(
            "t4",
            content="grandchild",
            sub_sid="sub-grand",
            at="2026-10-28T13:00:04.000Z",
        ),
    ]
    if grandchild_row_first:
        turns = [turns[0], turns[3], turns[1], turns[2]]
    tool_calls = [
        # 孤儿的嵌套派发（bridge.dispatchToolCallId = ToolCall 行 id）
        cannbot_tool_call_row(
            "tc_nest",
            "t3",
            "toolu_nest",
            "Agent",
            {"description": "explore"},
            start="2026-10-28T13:00:03.800Z",
        ),
    ]
    bridges = [cannbot_bridge_row("tc_nest", "sub-grand", "Explore")]
    return turns, tool_calls, bridges


def _cpx_dialect_turns() -> list[dict]:
    """cpx 方言 db 的 Turn 行（含 system 噪声 / 主 / 子 / 孙）。"""
    return [
        # cpx 导出的注入噪声（应被读取层丢弃）
        cannbot_turn_row(
            "ts1",
            role="system",
            content="# Environment",
            at="2026-10-29T12:39:30.454Z",
        ),
        cannbot_turn_row("t1", idx=1, content="do X", at="2026-10-28T13:00:00.000Z"),
        cannbot_turn_row(
            "t2",
            idx=2,
            role="assistant",
            content="dispatching",
            at="2026-10-28T13:00:01.000Z",
        ),
        # 子 agent（Designer）turns
        cannbot_turn_row(
            "t3",
            content="sub prompt",
            sub_sid="sub-1",
            at="2026-10-28T13:00:02.000Z",
        ),
        cannbot_turn_row(
            "t4",
            idx=1,
            role="assistant",
            content="nesting",
            sub_sid="sub-1",
            at="2026-10-28T13:00:03.000Z",
        ),
        # 孙 agent（Designer 派的 Explore）turns
        cannbot_turn_row(
            "t5",
            content="grandchild prompt",
            sub_sid="sub-2",
            at="2026-10-28T13:00:04.000Z",
        ),
    ]


def _cpx_dialect_tool_calls() -> list[dict]:
    """cpx 方言 db 的 ToolCall 行（主派发 / 子 Write / 子嵌套派发孙）。"""
    return [
        # 主派发：toolName=Agent，bridge.dispatchToolCallId = ToolCall.id（行 id）
        cannbot_tool_call_row(
            "tc1",
            "t2",
            "toolu_01Sc",
            "Agent",
            {"description": "design", "subagent_type": "designer"},
            start="2026-10-28T13:00:01.500Z",
            end="2026-10-28T13:30:00.000Z",
        ),
        # 子 agent 的 Write
        cannbot_tool_call_row(
            "tc2",
            "t4",
            "toolu_02Wr",
            "Write",
            {"file_path": "/orig/a.txt", "content": "x"},
            start="2026-10-28T13:00:03.500Z",
            end="2026-10-28T13:00:03.600Z",
        ),
        # 子 agent 的嵌套派发（孙）
        cannbot_tool_call_row(
            "tc3",
            "t4",
            "toolu_03Ag",
            "Agent",
            {"description": "explore", "subagent_type": "Explore"},
            start="2026-10-28T13:00:03.800Z",
            end="2026-10-28T13:10:00.000Z",
        ),
    ]


class TestCpxDialectTimeline:
    """cpx 方言 db：Agent 派发、bridge 键 = ToolCall.id、system turn 噪声、嵌套派发。"""

    @staticmethod
    def test_orphan_ancestor_nested_grandchild_not_duplicated(tmp_path: Path):
        """孤儿祖先补齐经递归 emit 孙 agent 后，孙不再被循环重复补齐。"""
        turns, tool_calls, bridges = _orphan_nested_rows()
        db = _make_cannbot_db(tmp_path, turns=turns, tool_calls=tool_calls, bridges=bridges)
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        # 孤儿自身 turns 各出现一次
        orphan_turn_ids = [
            e.turn["id"] for e in tl if e.kind == "turn" and e.subagent_session_id == "sub-orphan"
        ]
        assert orphan_turn_ids == ["t2", "t3"]
        # 孙 agent 只被递归 emit 一次（修复前：孤儿循环再补一次 → 重复）
        grand = [e for e in tl if e.subagent_session_id == "sub-grand"]
        assert len(grand) == 1
        assert grand[0].parent_task_call_id == "toolu_nest"

    @staticmethod
    def test_orphan_reverse_row_order_not_duplicated(tmp_path: Path):
        """Turn 行序后代先于孤儿祖先时，孙不被递归重复 emit。

        sub_turns 的迭代顺序跟随 Turn 行序；行序与 createdAt 语义序不
        一致是合法输入（时间线本就依赖末尾全局 ts 排序）。
        """
        turns, tool_calls, bridges = _orphan_nested_rows(grandchild_row_first=True)
        db = _make_cannbot_db(tmp_path, turns=turns, tool_calls=tool_calls, bridges=bridges)
        with CannbotDbReader(db) as r:
            timeline = r.read_timeline()
        orphan_ids = [
            e.turn["id"]
            for e in timeline
            if e.kind == "turn" and e.subagent_session_id == "sub-orphan"
        ]
        assert orphan_ids == ["t2", "t3"]
        # 修复前：先补齐孙（登记进 timeline），孤儿祖先的递归层 seen 是
        # 独立新建的局部集合，孙被再 emit 一次
        grand_total = sum(1 for e in timeline if e.subagent_session_id == "sub-grand")
        assert grand_total == 1

    @staticmethod
    def test_same_subagent_two_dispatches_not_duplicated(tmp_path: Path):
        """两个 task 派发指向同一子会话时只 emit 一次。"""
        turns = [
            cannbot_turn_row("t1", content="do X", at="2026-10-28T13:00:00.000Z"),
            cannbot_turn_row(
                "t2",
                content="sub prompt",
                sub_sid="sub-1",
                at="2026-10-28T13:00:02.000Z",
            ),
        ]
        tool_calls = [
            cannbot_tool_call_row(
                "tc1",
                "t1",
                "toolu_01",
                "Agent",
                {"description": "a"},
                start="2026-10-28T13:00:01.000Z",
            ),
            cannbot_tool_call_row(
                "tc2",
                "t1",
                "toolu_02",
                "Agent",
                {"description": "b"},
                start="2026-10-28T13:00:01.500Z",
            ),
        ]
        bridges = [
            cannbot_bridge_row("tc1", "sub-1", "Explore"),
            cannbot_bridge_row("tc2", "sub-1", "Explore", row_id="b2"),
        ]
        db = _make_cannbot_db(
            tmp_path,
            turns=turns,
            tool_calls=tool_calls,
            bridges=bridges,
        )
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        # 两个主 task 条目各自携带解析出的 sub-1（检查点链路依赖），
        # 去重不变式看的是子 agent 侧内容只 emit 一次
        sub_entries = [e for e in tl if e.role == "subagent" and e.subagent_session_id == "sub-1"]
        # 修复前：两次顶层 _interleave_subagent 各自新建 seen，子会话 emit 两份
        assert len(sub_entries) == 1
        assert sub_entries[0].parent_task_call_id == "toolu_01"

    @staticmethod
    def _make_cpx_db(tmp_path: Path) -> Path:
        """主 agent 派 Designer（sub-1），Designer 再嵌套派 Explore（sub-2）。"""
        bridges = [
            cannbot_bridge_row(
                "tc1",
                "sub-1",
                "designer",
                sub_name="Designer: X",
                tokens=100,
                at_dispatch="2026-10-28T13:00:01.500Z",
                at_response="2026-10-28T13:30:00.000Z",
            ),
            cannbot_bridge_row(
                "tc3",
                "sub-2",
                "Explore",
                row_id="b2",
                sub_name="Explore Y",
                tokens=50,
                at_dispatch="2026-10-28T13:00:03.800Z",
                at_response="2026-10-28T13:10:00.000Z",
            ),
        ]
        return _make_cannbot_db(
            tmp_path,
            turns=_cpx_dialect_turns(),
            tool_calls=_cpx_dialect_tool_calls(),
            bridges=bridges,
        )

    def test_system_turns_dropped(self, tmp_path: Path):
        db = self._make_cpx_db(tmp_path)
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        turn_entries = [e for e in tl if e.kind == "turn"]
        assert all(e.turn and e.turn.get("role") != "system" for e in turn_entries)
        assert {e.turn["id"] for e in turn_entries} == {"t1", "t2", "t3", "t4", "t5"}

    def test_bridge_keyed_by_tool_call_row_id_interleaves(self, tmp_path: Path):
        """bridge.dispatchToolCallId = ToolCall.id（行 id）也能定位子 agent。"""
        db = self._make_cpx_db(tmp_path)
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        sub_sids = {e.subagent_session_id for e in tl if e.role == "subagent"}
        assert sub_sids == {"sub-1", "sub-2"}

    def test_main_task_entry_carries_bridge_sub_sid(self, tmp_path: Path):
        """主 task 条目携带 bridge 解析出的子会话 id（cpx args 无该字段）。

        检查点链路只读 entry.subagent_session_id、args 抽取仅回退——若
        emit 时留空，cpx 会话的检查点 agent_id 全空、main_part_id 查不到。
        """
        db = self._make_cpx_db(tmp_path)
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        tc1 = next(e for e in tl if e.kind == "tool_call" and e.tool_call["id"] == "tc1")
        assert tc1.role == "main"
        assert tc1.subagent_session_id == "sub-1"

        from workflow_controller.plugins.agent_traceback.session_reconstructor.checkpoint_deriver import (
            _collect_main_task_boundaries,
        )

        boundaries = _collect_main_task_boundaries(tl)
        assert [b.subagent_session_id for b in boundaries] == ["sub-1"]

    def test_nested_dispatch_recursive_interleave(self, tmp_path: Path):
        """子 agent 的 Agent 派发递归 interleave 孙 agent 的 turns。"""
        db = self._make_cpx_db(tmp_path)
        with CannbotDbReader(db) as r:
            tl = r.read_timeline()
        # 孙 agent 的 turn 在时间线里
        assert any(
            e.kind == "turn"
            and e.turn
            and e.turn["id"] == "t5"
            and e.subagent_session_id == "sub-2"
            for e in tl
        )
        # 孙的派发 tool call 带 parent_task_call_id = 子 agent 的 Agent 调用 id
        tc3 = next(e for e in tl if e.kind == "tool_call" and e.tool_call["id"] == "tc3")
        assert tc3.tool_call["toolName"] == "task"
        child_turn = next(e for e in tl if e.kind == "turn" and e.subagent_session_id == "sub-2")
        assert child_turn.parent_task_call_id == "toolu_03Ag"

    def test_read_bridges_carries_tokens(self, tmp_path: Path):
        db = self._make_cpx_db(tmp_path)
        with CannbotDbReader(db) as r:
            bridges = r.read_bridges()
        by_sid = {b["subagentSessionId"]: b for b in bridges}
        assert by_sid["sub-1"]["subagentTokens"] == 100
        assert by_sid["sub-2"]["subagentName"] == "Explore Y"

    def test_read_bridges_without_tokens_column(self, tmp_path: Path):
        """旧版 recorder 的 InteractionBridge 无 subagentTokens 列时回退 0。"""
        db = self._make_cpx_db(tmp_path)
        con = sqlite3.connect(db)
        con.execute("ALTER TABLE InteractionBridge DROP COLUMN subagentTokens")
        con.commit()
        con.close()
        with CannbotDbReader(db) as r:
            bridges = r.read_bridges()
        by_sid = {b["subagentSessionId"]: b for b in bridges}
        assert by_sid["sub-1"]["subagentTokens"] == 0
        assert by_sid["sub-2"]["subagentName"] == "Explore Y"
