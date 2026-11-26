#!/usr/bin/env python3
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

"""生成一个合成 cannbot session db 文件，供导入测试用。

构造一条已完成的三阶段 session（设计 → 开发 → 测试），录制在 cannbot db
格式里：3 个 subagent（designer / developer / tester），每个写一个小文件 +
git 提交。schema（Session/Turn/ToolCall/Execution/InteractionBridge）与
真实 cannbot 录制一致——``ReconstructOrchestrator.import_session`` 可直接读取。

用法::

    python gen_db.py          # 生成 cannbot_session.db
    python gen_db.py -o /tmp/xxx.db   # 指定输出路径

本模块也作为 demo.py 的共享 db 工厂。
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))

from opencode_demo_lib import configure_demo_logging

HERE = Path(__file__).resolve().parent

logger = logging.getLogger(__name__)


# ── 文件内容常量（subagent / 主 agent 的写入产物） ──────────────────

DESIGN_MD = "# API 设计\n\nadd(a, b) -> a + b\n"
IMPL_PY = "def add(a, b):\n    return a + b\n"
TEST_PY = "from impl import add\nassert add(1, 2) == 3\nprint('ok')\n"
SUMMARY_MD = "# 交付总结\n\n三阶段（设计/开发/测试）完成。\n"

# ── session id 常量 ────────────────────────────────────────────────

ORIGIN_SDK = "ses_recon_main"
S1 = "ses_recon_designer"  # designer subagent
S2 = "ses_recon_developer"  # developer subagent
S3 = "ses_recon_tester"  # tester subagent

# ── 重放时登记的 developer 补充指令（拨转 developer 行为：加法 → 乘法） ──

HINT_TEXT = (
    "重放补充指令：本次 impl.py 的 add(a,b) 应返回 a*b（乘法，覆盖原加法）。"
    "标记 REPLAY_HINT_MARKER。"
)
MARKERS = ["a*b", "REPLAY_HINT_MARKER", "乘法"]

DESIGNER_TYPE = "designer"
DEVELOPER_TYPE = "developer"
TESTER_TYPE = "tester"

ORIGIN_CWD = "/workspace/origin"

# 时间戳模板：T+{seconds}.{ms}Z
_T = "2026-07-20T12:00:{:02d}.{:03d}Z"

# 从 workflow-controller.yaml 读 starting_prompt——首个 user turn 须含完整 workflow 计划，
# 重建后 fork worker 从 L2 首条 user message 读到完整计划。yaml 不存在时用兜底。
_STARTING_PROMPT = """任务：按三阶段（设计 → 开发 → 测试）交付 add 的 impl.py + test.py。
一气走完三阶段再收尾写 summary.md，中途不要停下汇报、不要等指令、不要在中途产生无 tool 调用的结束回合。

阶段一（设计）：用 task 工具起 designer（subagent_type: designer）写 design.md
（含 add(a,b)->a+b）。等待 designer 返回后再进入阶段二。

阶段二（开发）：前置条件——design.md 已落盘。用 task 工具起 developer
（subagent_type: developer）读 design 实现 impl.py。

阶段三（测试）：用 task 工具起 tester（subagent_type: tester），读 impl.py 写 test.py 并自测。
收到 tester 完成摘要后，写 summary.md 汇总（文件 / 自测结果）。

【反循环约束】
- 每个文件只写一次。若 design.md 已存在且内容正确，不要再派 designer，直接进入下一阶段。
- 不要在阶段之间插入"已完成，等待指示"之类的回合——继续推进到 summary.md。"""


def build_cannbot_db(db_path: Path) -> Path:
    """构造一个 3-subagent cannbot db：主 3 task + 子 write/git 命令 + 3 bridge。

    时间窗交错::

        主 task#1  [01.5 .. 10]  ← designer  工具 [04 .. 06]
        主 task#2  [11.5 .. 20]  ← developer 工具 [14 .. 16]
        主 task#3  [21.5 .. 30]  ← tester    工具 [24 .. 26]

    检查点（subagent_start）时刻的文件态::

        cp#1 (task#1 起) = 空（designer 未跑）
        cp#2 (task#2 起) = design.md（designer 已跑）
        cp#3 (task#3 起) = design.md + impl.py（developer 已跑）
        session_complete = + test.py（tester 已跑）
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    con = sqlite3.connect(db_path)
    _create_tables(con)
    _insert_session(con)
    _insert_turns(con)
    _insert_tool_calls(con)
    _insert_executions(con)
    _insert_bridges(con)
    con.commit()
    con.close()
    return db_path


def _create_tables(con: sqlite3.Connection) -> None:
    con.execute(
        "CREATE TABLE Session (id TEXT PRIMARY KEY, taskId TEXT, query TEXT, "
        "framework TEXT, model TEXT, directory TEXT, sourcePath TEXT, "
        "rootExecutionId TEXT, createdAt TEXT)"
    )
    con.execute(
        "CREATE TABLE Turn (id TEXT PRIMARY KEY, sessionId TEXT, turnIndex INTEGER, "
        "role TEXT, content TEXT, contentJson TEXT, contentSummary TEXT, "
        "agentName TEXT, subagentName TEXT, subagentSessionId TEXT, "
        "parentExecutionId TEXT, finishReason TEXT, isSubagent BOOLEAN, "
        "createdAt TEXT, completedAt TEXT, model TEXT)"
    )
    con.execute(
        "CREATE TABLE ToolCall (id TEXT PRIMARY KEY, turnId TEXT, toolCallId TEXT, "
        "toolName TEXT, argsJson TEXT, resultJson TEXT, state TEXT, "
        "errorType TEXT, errorMessage TEXT, startedAt TEXT, completedAt TEXT, "
        "durationMs INTEGER, dispatchBridgeId TEXT, isSkillRelated BOOLEAN, "
        "createdAt TEXT)"
    )
    con.execute(
        "CREATE TABLE Execution (id TEXT PRIMARY KEY, sessionId TEXT, "
        "agentName TEXT, agentSessionId TEXT, isSubagent BOOLEAN, "
        "subagentType TEXT, subagentName TEXT, parentExecutionId TEXT, "
        "rootExecutionId TEXT, depth INTEGER)"
    )
    con.execute(
        "CREATE TABLE InteractionBridge (id TEXT PRIMARY KEY, sessionId TEXT, "
        "dispatchExecutionId TEXT, dispatchTurnId TEXT, dispatchToolCallId TEXT, "
        "dispatchContent TEXT, dispatchTimestamp TEXT, responseExecutionId TEXT, "
        "responseTurnId TEXT, responseContent TEXT, responseTimestamp TEXT, "
        "subagentSessionId TEXT, subagentType TEXT, subagentName TEXT, status TEXT)"
    )


def _insert_session(con: sqlite3.Connection) -> None:
    con.execute(
        "INSERT INTO Session (id, taskId, query, framework, model, directory, "
        "sourcePath, rootExecutionId, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "cmr_recon",
            ORIGIN_SDK,
            "按三阶段（设计 → 开发 → 测试）交付 add 的 impl.py + test.py",
            "opencode",
            "bailian-payg/glm-5.2",
            ORIGIN_CWD,
            "",
            "exec_root",
            _T.format(0, 0),
        ),
    )


def _design_turns() -> list[dict[str, Any]]:
    """主 agent 设计阶段 turns：起始 prompt → 派发 designer → 结果回填。"""
    return [
        {
            "id": "t1",
            "turnIndex": 0,
            "role": "user",
            "isSubagent": False,
            "content": _STARTING_PROMPT,
            "createdAt": _T.format(0, 0),
        },
        {
            "id": "t2",
            "turnIndex": 1,
            "role": "assistant",
            "isSubagent": False,
            "finishReason": "tool-calls",
            "content": "<thinking>start with design</thinking>Dispatching designer.",
            "createdAt": _T.format(1, 0),
        },
        # driver 注入的 task result（designer 完成）——镜像真实 opencode 会话结构
        {
            "id": "t2r",
            "turnIndex": 2,
            "role": "user",
            "isSubagent": False,
            "content": "task_id: ses_recon_designer\n<task_result>ok</task_result>\ndesign.md written successfully.",
            "createdAt": _T.format(10, 500),
        },
    ]


def _develop_turns() -> list[dict[str, Any]]:
    """主 agent 开发阶段 turns：派发 developer → 结果回填。"""
    return [
        {
            "id": "t3",
            "turnIndex": 3,
            "role": "assistant",
            "isSubagent": False,
            "finishReason": "tool-calls",
            "content": "<thinking>now develop</thinking>Dispatching developer.",
            "createdAt": _T.format(11, 0),
        },
        # driver 注入的 task result（developer 完成）
        {
            "id": "t3r",
            "turnIndex": 4,
            "role": "user",
            "isSubagent": False,
            "content": "task_id: ses_recon_developer\n<task_result>ok</task_result>\nimpl.py written successfully.",
            "createdAt": _T.format(20, 500),
        },
    ]


def _test_turns() -> list[dict[str, Any]]:
    """主 agent 测试阶段与收尾 turns：派发 tester → 结果回填 → 终轮。"""
    return [
        {
            "id": "t4",
            "turnIndex": 5,
            "role": "assistant",
            "isSubagent": False,
            "finishReason": "tool-calls",
            "content": "<thinking>now test</thinking>Dispatching tester.",
            "createdAt": _T.format(21, 0),
        },
        # driver 注入的 task result（tester 完成）
        {
            "id": "t4r",
            "turnIndex": 6,
            "role": "user",
            "isSubagent": False,
            "content": "task_id: ses_recon_tester\n<task_result>ok</task_result>\ntest.py written and test passes.",
            "createdAt": _T.format(30, 500),
        },
        {
            "id": "t5",
            "turnIndex": 7,
            "role": "assistant",
            "isSubagent": False,
            "finishReason": "stop",
            "content": "All done.",
            "createdAt": _T.format(31, 0),
        },
    ]


def _subagent_turns() -> list[dict[str, Any]]:
    """三名 subagent 的成对 user/assistant turns（s{n}_t1 / s{n}_t2）。"""
    specs = [
        (S1, "sub prompt 1", "writing design.md", 2),
        (S2, "sub prompt 2", "writing impl.py", 12),
        (S3, "sub prompt 3", "writing test.py", 22),
    ]
    rows: list[dict[str, Any]] = []
    for num, (sid, prompt, note, t0) in enumerate(specs, start=1):
        rows.append(
            {
                "id": f"s{num}_t1",
                "turnIndex": 0,
                "role": "user",
                "isSubagent": True,
                "subagentSessionId": sid,
                "content": prompt,
                "createdAt": _T.format(t0, 0),
            }
        )
        rows.append(
            {
                "id": f"s{num}_t2",
                "turnIndex": 1,
                "role": "assistant",
                "isSubagent": True,
                "subagentSessionId": sid,
                "finishReason": "tool-calls",
                "content": note,
                "createdAt": _T.format(t0 + 1, 0),
            }
        )
    return rows


def _insert_turns(con: sqlite3.Connection) -> None:
    turns = _design_turns() + _develop_turns() + _test_turns() + _subagent_turns()
    for t in turns:
        con.execute(
            "INSERT INTO Turn (id, sessionId, turnIndex, role, content, "
            "contentJson, contentSummary, agentName, subagentName, "
            "subagentSessionId, parentExecutionId, finishReason, isSubagent, "
            "createdAt, completedAt, model) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                t["id"],
                "cmr_recon",
                int(t.get("turnIndex", 0)),
                t.get("role", "user"),
                t.get("content", ""),
                "",
                "",
                "build",
                t.get("subagentName"),
                t.get("subagentSessionId"),
                t.get("parentExecutionId"),
                t.get("finishReason"),
                1 if t.get("isSubagent") else 0,
                t.get("createdAt"),
                t.get("completedAt"),
                t.get("model"),
            ),
        )


def _task_call(
    cid: str, turn_id: str, sub_sid: str, sub_type: str, ts_start: str, ts_end: str
) -> dict[str, Any]:
    return {
        "id": f"tc_{cid}",
        "turnId": turn_id,
        "toolCallId": cid,
        "toolName": "task",
        "argsJson": json.dumps(
            {
                "description": f"do {sub_sid}",
                "prompt": "be X",
                "subagent_type": sub_type,
                "subagent_session_id": sub_sid,
            }
        ),
        "resultJson": f"task_id: {sub_sid}\n<task_result>ok</task_result>",
        "startedAt": ts_start,
        "completedAt": ts_end,
    }


def _write_call(cid: str, turn_id: str, fp: str, content: str, ts: str) -> dict[str, Any]:
    return {
        "id": cid,
        "turnId": turn_id,
        "toolCallId": cid,
        "toolName": "write",
        "argsJson": json.dumps({"filePath": fp, "content": content}),
        "startedAt": ts,
        "completedAt": ts,
    }


def _bash_call(cid: str, turn_id: str, cmd: str, ts: str, result: str = "") -> dict[str, Any]:
    return {
        "id": cid,
        "turnId": turn_id,
        "toolCallId": cid,
        "toolName": "bash",
        "argsJson": json.dumps({"command": cmd, "description": ""}),
        "resultJson": result,
        "startedAt": ts,
        "completedAt": ts,
    }


def _tool_call_rows() -> list[dict[str, Any]]:
    """全部 ToolCall 行：主 task 派发 + 三名 subagent 的 write/git 命令。"""
    return [
        # ── 主 task 派发 ──
        _task_call(
            "call_main_task_1", "t2", S1, DESIGNER_TYPE, _T.format(1, 500), _T.format(10, 0)
        ),
        _task_call(
            "call_main_task_2", "t3", S2, DEVELOPER_TYPE, _T.format(11, 500), _T.format(20, 0)
        ),
        _task_call("call_main_task_3", "t4", S3, TESTER_TYPE, _T.format(21, 500), _T.format(30, 0)),
        # ── designer：write design.md + git init/add/commit ──
        _write_call("s1_w1", "s1_t2", f"{ORIGIN_CWD}/design.md", DESIGN_MD, _T.format(4, 0)),
        _bash_call("s1_g0", "s1_t2", "git init", _T.format(4, 500)),
        _bash_call("s1_g1", "s1_t2", "git add -A", _T.format(5, 0)),
        _bash_call(
            "s1_g2",
            "s1_t2",
            'git commit -m "init design"',
            _T.format(6, 0),
            # 含原生 git 输出（带 Author）——测 hash 保真 author 解析路径
            "[main abc1234] init design\n Author: Dev One <dev1@example.com>\n"
            " 1 file changed, 1 insertion(+)\n",
        ),
        # ── developer：write impl.py + git add/commit ──
        _write_call("s2_w1", "s2_t2", f"{ORIGIN_CWD}/impl.py", IMPL_PY, _T.format(14, 0)),
        _bash_call("s2_g1", "s2_t2", "git add -A", _T.format(15, 0)),
        _bash_call(
            "s2_g2",
            "s2_t2",
            'git commit -m "init impl"',
            _T.format(16, 0),
            # cannbot 包装格式（无原生 Author）——测降级路径
            f"task_id: {S2}\n<task_result>ok</task_result>",
        ),
        # ── tester：write test.py + git add/commit ──
        _write_call("s3_w1", "s3_t2", f"{ORIGIN_CWD}/test.py", TEST_PY, _T.format(24, 0)),
        _bash_call("s3_g1", "s3_t2", "git add -A", _T.format(25, 0)),
        _bash_call("s3_g2", "s3_t2", 'git commit -m "add test"', _T.format(26, 0)),
    ]


def _insert_tool_calls(con: sqlite3.Connection) -> None:
    for tc in _tool_call_rows():
        con.execute(
            "INSERT INTO ToolCall (id, turnId, toolCallId, toolName, argsJson, "
            "resultJson, state, errorType, errorMessage, startedAt, completedAt, "
            "durationMs, dispatchBridgeId, isSkillRelated, createdAt) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                tc["id"],
                tc["turnId"],
                tc["toolCallId"],
                tc["toolName"],
                tc.get("argsJson", ""),
                tc.get("resultJson", ""),
                "completed",
                None,
                None,
                tc.get("startedAt"),
                tc.get("completedAt"),
                0,
                None,
                0,
                tc.get("startedAt"),
            ),
        )


def _execution_row(exec_id: str, agent_sid: str, sub_type: str | None) -> dict[str, Any]:
    """构造一条 Execution 行：sub_type 为 None 表示主执行，否则为 subagent。"""
    return {
        "id": exec_id,
        "sessionId": "cmr_recon",
        "agentName": "build",
        "agentSessionId": agent_sid,
        "isSubagent": sub_type is not None,
        "subagentType": sub_type,
        "subagentName": sub_type,
        "parentExecutionId": None if sub_type is None else "exec_root",
        "rootExecutionId": "exec_root",
        "depth": 1 if sub_type is not None else 0,
    }


def _insert_executions(con: sqlite3.Connection) -> None:
    executions = [
        _execution_row("exec_root", ORIGIN_SDK, None),
        _execution_row("exec_s1", S1, DESIGNER_TYPE),
        _execution_row("exec_s2", S2, DEVELOPER_TYPE),
        _execution_row("exec_s3", S3, TESTER_TYPE),
    ]
    for e in executions:
        con.execute(
            "INSERT INTO Execution (id, sessionId, agentName, agentSessionId, "
            "isSubagent, subagentType, subagentName, parentExecutionId, "
            "rootExecutionId, depth) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                e["id"],
                e["sessionId"],
                e["agentName"],
                e["agentSessionId"],
                1 if e["isSubagent"] else 0,
                e["subagentType"],
                e["subagentName"],
                e["parentExecutionId"],
                e["rootExecutionId"],
                e["depth"],
            ),
        )


def _insert_bridges(con: sqlite3.Connection) -> None:
    bridges = [
        {
            "id": "b1",
            "dispatchToolCallId": "call_main_task_1",
            "subagentSessionId": S1,
            "subagentType": DESIGNER_TYPE,
        },
        {
            "id": "b2",
            "dispatchToolCallId": "call_main_task_2",
            "subagentSessionId": S2,
            "subagentType": DEVELOPER_TYPE,
        },
        {
            "id": "b3",
            "dispatchToolCallId": "call_main_task_3",
            "subagentSessionId": S3,
            "subagentType": TESTER_TYPE,
        },
    ]
    for b in bridges:
        con.execute(
            "INSERT INTO InteractionBridge (id, sessionId, dispatchToolCallId, "
            "subagentSessionId, subagentType, status) VALUES (?, ?, ?, ?, ?, ?)",
            (
                b["id"],
                "cmr_recon",
                b["dispatchToolCallId"],
                b["subagentSessionId"],
                b["subagentType"],
                "completed",
            ),
        )


def main() -> int:
    configure_demo_logging(logger)
    import argparse

    parser = argparse.ArgumentParser(
        description="生成合成 cannbot session db 供 Workflow Controller 导入重建测试",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=str(HERE / "cannbot_session.db"),
        help="输出 db 路径（默认：./cannbot_session.db）",
    )
    args = parser.parse_args()

    out = Path(args.output)
    build_cannbot_db(out)
    logger.info(f"✓ 生成 cannbot session db: {out}")
    logger.info(f"  native_session_id : {ORIGIN_SDK}")
    logger.info(
        f"  subagents      : {S1} ({DESIGNER_TYPE}), {S2} ({DEVELOPER_TYPE}), {S3} ({TESTER_TYPE})"
    )
    logger.info(f"  origin_cwd     : {ORIGIN_CWD}")
    logger.info("")
    logger.info("导入方法见 README.md → Live 跑法")
    return 0


if __name__ == "__main__":
    sys.exit(main())
