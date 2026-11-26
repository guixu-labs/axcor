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

"""``l2_transcriber.py`` 单元测试 — Turn/ToolCall → opencode.db 转写。

Hermetic：每个测试造一个最小 source opencode.db（仅 schema + migrations），
跑 L2Transcriber.transcribe，校验产出的 fork opencode.db 的 session/message/part
行 + task_part_ids 映射正确。
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from workflow_controller.plugins.agent_traceback.session_reconstructor.db_reader import (
    SessionMeta,
    TimelineEntry,
)
from workflow_controller.plugins.agent_traceback.session_reconstructor.l2_transcriber import (
    L2Transcriber,
)

from ._fixtures import (
    build_source_opencode_db,
    cannbot_bridge_row,
    tool_timeline_entry,
    turn_timeline_entry,
)
from ._fixtures import (
    can_open_readonly as _can_open_readonly,
)


def _transcriber_at(
    tmp_path: Path, *, origin_cwd: str = "/workspace/orig"
) -> tuple[L2Transcriber, Path]:
    """标准转写装配：最小 source opencode.db + target 目录 + L2Transcriber。"""
    src_db = build_source_opencode_db(tmp_path)
    target_cwd = tmp_path / "target"
    target_cwd.mkdir()
    transcriber = L2Transcriber(
        target_cwd=target_cwd,
        origin_cwd=origin_cwd,
        source_opencode_db=src_db,
    )
    return transcriber, target_cwd


def _meta(
    native_session_id: str,
    *,
    initial_prompt: str = "",
    origin_cwd: str = "/workspace/orig",
    model: str = "m",
    framework: str = "opencode",
    root_execution_id: str = "",
) -> SessionMeta:
    """本文件的 SessionMeta 快捷构造（source_path 始终为空）。"""
    return SessionMeta(
        native_session_id=native_session_id,
        initial_prompt=initial_prompt,
        origin_cwd=origin_cwd,
        model=model,
        framework=framework,
        source_path="",
        root_execution_id=root_execution_id,
    )


def _make_timeline_main_only() -> list[TimelineEntry]:
    """构造一条最小主 agent 时间线：1 user + 1 assistant(thinking+正文) + 1 task。"""
    return [
        turn_timeline_entry(
            1784203200000,
            "t1",
            content="please do X",
            at="2026-07-16T12:00:00.000Z",
        ),
        turn_timeline_entry(
            1784203201000,
            "t2",
            idx=1,
            role="assistant",
            finish="tool-calls",
            content="<thinking>let me dispatch subagent</thinking>Dispatching now.",
            at="2026-07-16T12:00:01.000Z",
        ),
        tool_timeline_entry(
            1784203201500,
            "tc1",
            "t2",
            "call_main_task_1",
            "task",
            {
                "description": "do X",
                "prompt": "be X",
                "subagent_type": "developer",
                "subagent_session_id": "ses_sub_001",
            },
            result="task_id: ses_sub_001\n<task_result>ok</task_result>",
            start="2026-07-16T12:00:01.500Z",
            end="2026-07-16T12:00:10.000Z",
        ),
    ]


# ── transcribe ───────────────────────────────────────────────────────


def _read_transcribed_rows(db_path: Path) -> tuple[tuple, list[tuple], list[dict]]:
    """读转写产出库：session 行、message 行（按 time_created）与解析后的 part data。"""
    con = sqlite3.connect(db_path)
    sess = con.execute("SELECT id, title, directory, model FROM session").fetchone()
    msgs = con.execute("SELECT id, data FROM message ORDER BY time_created").fetchall()
    parts = [
        json.loads(row[1])
        for row in con.execute("SELECT id, data FROM part ORDER BY time_created, id").fetchall()
    ]
    con.close()
    return sess, msgs, parts


class TestTranscribe:
    @staticmethod
    def test_writes_session_message_part_rows(tmp_path: Path):
        transcriber, target_cwd = _transcriber_at(tmp_path)
        meta = _meta(
            "ses_main_001",
            initial_prompt="please do X",
            model="cannbot/glm-5.2",
            root_execution_id="exec_root",
        )
        tl = _make_timeline_main_only()
        bridges = [cannbot_bridge_row("call_main_task_1", "ses_sub_001", "developer")]
        result = transcriber.transcribe(tl, meta, "ses_main_001", bridges)
        # 验证产出的 db 路径
        expected = target_cwd / ".workflow-controller" / "opencode" / "opencode.db"
        assert result.opencode_db_path == expected
        assert expected.is_file()
        # 验证行数
        assert result.message_count == 2  # 1 user + 1 assistant
        # parts 共 6 个：user text、step-start、reasoning、body text、task tool、step-finish
        assert result.part_count == 6
        # task part 落在 part_id 索引 4 的槽位（前面依次是 user text、
        # step-start、reasoning、body text）
        assert result.task_part_ids == {"ses_sub_001": expect_part_id("ses_main_001", 4)}

        # 读出 db 校验内容
        sess, msgs, parts = _read_transcribed_rows(expected)
        assert sess[0] == "ses_main_001"
        assert sess[1].startswith("[reconstructed]")
        assert sess[2] == str(target_cwd.resolve())
        assert sess[3] == '{"id": "glm-5.2", "providerID": "cannbot", "variant": "default"}'
        assert len(msgs) == 2
        m0 = json.loads(msgs[0][1])
        assert m0["role"] == "user"
        assert "parentID" not in m0  # 首条 user 无 parentID
        m1 = json.loads(msgs[1][1])
        assert m1["role"] == "assistant"
        assert m1["parentID"] == msgs[0][0]
        assert len(parts) == 6
        p0, p1, p2, p3, p4, p5 = parts
        # 0: user text
        assert p0["type"] == "text"
        assert p0["text"] == "please do X"
        # 1: step-start
        assert p1["type"] == "step-start"
        # 2: reasoning
        assert p2["type"] == "reasoning"
        assert "dispatch subagent" in p2["text"]
        # 3: text body
        assert p3["type"] == "text"
        assert p3["text"] == "Dispatching now."
        # 4: task tool part
        assert p4["type"] == "tool"
        assert p4["tool"] == "task"
        assert p4["callID"] == "call_main_task_1"
        assert p4["state"]["status"] == "completed"
        assert "ses_sub_001" in p4["state"]["output"]
        # 5: step-finish
        assert p5["type"] == "step-finish"
        assert p5["reason"] == "tool-calls"

    @staticmethod
    def test_task_part_output_contains_subagent_id(tmp_path: Path):
        """关键：task part 的 state.output 必须含子 session id（§6.5）。

        若原 resultJson 不含子 session id，transcriber 应显式构造。
        """
        transcriber, _target = _transcriber_at(tmp_path)
        meta = _meta("ses_main_002")
        # 1 user + 1 assistant + 1 task（resultJson 不含子 session id）
        tl = [
            turn_timeline_entry(1, "t1", content="do X"),
            turn_timeline_entry(
                2, "t2", idx=1, role="assistant", finish="tool-calls", content="ok"
            ),
            tool_timeline_entry(
                3,
                "tc1",
                "t2",
                "call_main_task_99",
                "task",
                {
                    "description": "x",
                    "prompt": "x",
                    "subagent_type": "dev",
                    "subagent_session_id": "ses_sub_orphan",
                },
                # resultJson 不含 ses_sub_orphan
                result="task done with no sub id",
                start="2026-07-16T12:00:01.500Z",
                end="2026-07-16T12:00:10.000Z",
            ),
        ]
        # 无 bridge → 从 argsJson 取 subagent_session_id
        result = transcriber.transcribe(tl, meta, "ses_main_002", bridges=[])
        # 读出 task part，验证 state.output 含子 session id
        con = sqlite3.connect(result.opencode_db_path)
        parts = con.execute('SELECT data FROM part WHERE data LIKE \'%"tool": "task"%\'').fetchall()
        assert parts
        p = json.loads(parts[0][0])
        assert "ses_sub_orphan" in p["state"]["output"]
        con.close()

    @staticmethod
    def test_empty_timeline_yields_no_messages(tmp_path: Path):
        """空时间线：仅写 session 行，无 message/part。"""
        transcriber, _target = _transcriber_at(tmp_path, origin_cwd="/x")
        meta = _meta("ses_main_003", origin_cwd="/x")
        result = transcriber.transcribe([], meta, "ses_main_003")
        assert result.message_count == 0
        assert result.part_count == 0
        con = sqlite3.connect(result.opencode_db_path)
        # session 行存在
        n = con.execute("SELECT COUNT(*) FROM session").fetchone()[0]
        assert n == 1
        con.close()

    @staticmethod
    def test_init_db_failure_raises(tmp_path: Path):
        """source opencode.db 不存在 → _init_fork_db_from_source 返回 False → transcriber 抛 RuntimeError。"""
        target_cwd = tmp_path / "target"
        target_cwd.mkdir()
        transcriber = L2Transcriber(
            target_cwd=target_cwd,
            origin_cwd="/x",
            source_opencode_db=tmp_path / "nonexistent.db",
        )
        meta = _meta("ses_x", origin_cwd="/x")
        with pytest.raises(RuntimeError, match=r"source opencode\.db not found"):
            transcriber.transcribe([], meta, "ses_x")

    @staticmethod
    def test_reimport_same_target_cwd_overwrites(tmp_path: Path):
        """同 target_cwd 重复导入：第二次 transcribe 成功且整库覆盖。

        schema 回放要求空 dest；dest 已存在（上次成功导入）时首个 CREATE TABLE
        即撞 table already exists。覆盖语义：dest 只含最新导入的 session。
        """
        src_db = build_source_opencode_db(tmp_path)
        target_cwd = tmp_path / "target"
        target_cwd.mkdir()

        def _transcribe(session_id: str):
            transcriber = L2Transcriber(
                target_cwd=target_cwd,
                origin_cwd="/x",
                source_opencode_db=src_db,
            )
            return transcriber.transcribe([], _meta(session_id, origin_cwd="/x"), session_id)

        _transcribe("ses_main_first")
        result = _transcribe("ses_main_second")
        con = sqlite3.connect(result.opencode_db_path)
        ids = sorted(r[0] for r in con.execute("SELECT id FROM session").fetchall())
        con.close()
        assert ids == ["ses_main_second"]

    @staticmethod
    def test_sample_db_transcribe_main_only():
        """真实样例 db：转写主 agent，校验产出 db 可被 read_main_session_parts 读回。

        只校验主 agent 转写流程端到端跑通（不深究内容保真度）。
        通过环境变量 ``WORKFLOW_CONTROLLER_TEST_SAMPLE_CANNBOT_DB`` 指向本地样例 db
        （cannbot session 导出的脱敏副本）；未设置时 skip，不可读
        （macOS TCC/隔离属性）时同样 skip。宿主还需有
        ~/.local/share/opencode/opencode.db 做 schema 模板，否则 skip。
        """
        sample_env = os.environ.get("WORKFLOW_CONTROLLER_TEST_SAMPLE_CANNBOT_DB", "")
        if not sample_env:
            pytest.skip("WORKFLOW_CONTROLLER_TEST_SAMPLE_CANNBOT_DB not set (optional sample db)")
        sample = Path(sample_env)
        if not sample.is_file():
            pytest.skip("sample cannbot db not available")
        if not _can_open_readonly(sample):
            pytest.skip("sample cannbot db not readable (TCC/quarantine?)")
        # 用宿主默认 source opencode.db 做 schema-only 初始化
        host_db = Path.home() / ".local" / "share" / "opencode" / "opencode.db"
        if not host_db.is_file():
            pytest.skip("host opencode.db not available for schema template")

        import tempfile

        from workflow_controller.plugins.agent_traceback.l2_reader import (
            find_main_part_id_for_subagent,
            read_main_session_parts,
        )
        from workflow_controller.plugins.agent_traceback.session_reconstructor.db_reader import (
            CannbotDbReader,
        )

        with tempfile.TemporaryDirectory() as td:
            target_cwd = Path(td) / "target"
            target_cwd.mkdir()
            with CannbotDbReader(sample) as r:
                meta = r.read_session()
                tl = r.read_timeline()
                bridges = r.read_bridges()
            transcriber = L2Transcriber(
                target_cwd=target_cwd,
                origin_cwd=meta.origin_cwd,
                source_opencode_db=host_db,
            )
            result = transcriber.transcribe(tl, meta, meta.native_session_id, bridges)
            # 用 read_main_session_parts 读回
            env = {"XDG_DATA_HOME": str(target_cwd / ".workflow-controller")}
            parts = read_main_session_parts(meta.native_session_id, env)
            assert len(parts) > 0
            # 找一个 task part 验证 find_main_part_id_for_subagent 能定位
            # 从 bridge 取一个子 session id
            assert bridges
            sub_sid = str(bridges[0].get("subagentSessionId") or "")
            assert sub_sid
            task_pid = find_main_part_id_for_subagent(meta.native_session_id, sub_sid, env)
            assert task_pid is not None
            # 与转写器记录的 task_part_ids 对比
            assert sub_sid in result.task_part_ids
            assert result.task_part_ids[sub_sid] == task_pid

    @staticmethod
    def test_task_part_resolved_via_bridge_row_id(tmp_path: Path):
        """bridge.dispatchToolCallId = ToolCall.id（cpx 形态）也能定位 sub_sid。

        cpx 的 bridge 记录制器 cuid（ToolCall.id），与框架原生 toolCallId
        （toolu_…）不同命名空间；转写器须先按行 id 查 bridge。resultJson 为
        空 → state.output 强制注入 task_id 前缀。
        """
        transcriber, _target = _transcriber_at(tmp_path)
        meta = _meta("ses_main_004", framework="claude-code-proxy")
        tl = [
            turn_timeline_entry(1, "t1", content="do X"),
            turn_timeline_entry(
                2, "t2", idx=1, role="assistant", finish="tool-calls", content="ok"
            ),
            tool_timeline_entry(
                3,
                "tc_row_1",
                "t2",
                "toolu_01Sc3INBKrxD6z1",
                "task",
                {"description": "design", "subagent_type": "designer"},
                start="2026-10-28T13:00:01.500Z",
                end="2026-10-28T15:26:38.108Z",
            ),
        ]
        # bridge 键 = ToolCall.id（行 id），非 toolCallId
        bridges = [
            cannbot_bridge_row("tc_row_1", "sub-146cd4ba4af9", "designer"),
        ]
        result = transcriber.transcribe(tl, meta, "ses_main_004", bridges)
        # task part 落在 part_id 索引 3 的槽位（user text、step-start、text 之后）
        assert result.task_part_ids == {
            "sub-146cd4ba4af9": expect_part_id("ses_main_004", 3),
        }
        con = sqlite3.connect(result.opencode_db_path)
        parts = con.execute('SELECT data FROM part WHERE data LIKE \'%"tool": "task"%\'').fetchall()
        assert parts
        p = json.loads(parts[0][0])
        assert p["state"]["output"].startswith("task_id: sub-146cd4ba4af9")
        con.close()


def expect_part_id(sid: str, idx: int) -> str:
    """生成期望的 part id（与 l2_transcriber 内部一致）。"""
    return f"prt_rc_{sid[:8]}_{idx:04d}"
