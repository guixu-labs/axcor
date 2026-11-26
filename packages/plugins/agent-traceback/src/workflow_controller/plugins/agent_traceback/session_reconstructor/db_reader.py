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

"""cannbot session db 读取器 + 统一时间线构建。

读 cannbot 录制的 SQLite session db（``Session``/``Turn``/``ToolCall``/
``Execution``/``InteractionBridge`` 表），按时间序把主 agent 的 task tool call
与子 agent 的 turn/toolcall 交错排序成统一 ``list[TimelineEntry]``。

读取时做两类清洗（框架方言适配）：
  - 工具名归一化（``_TOOL_NAME_ALIASES``）：claude-code-proxy 的
    ``Agent``/``Write``/``Edit``/``Bash`` → opencode 的 ``task``/``write``/
    ``edit``/``bash``，下游按 opencode 方言消费；
  - ``role='system'`` Turn 丢弃：cpx 导出 db 的注入噪声（数万行共享导出
    时刻的 createdAt），非对话内容。

时间线构建算法：
  1. 取主 agent 所有 Turn（``isSubagent=0``）+ 其 ToolCall，按 ``turnIndex``/
     ``startedAt`` 排序；
  2. 对每个主 agent 的 ``task`` tool call，从 ``InteractionBridge`` 查
     ``subagentSessionId``（键 = ``ToolCall.id``，见 ``_lookup_bridge_sub``）；
  3. 取该子 agent 的所有 Turn + ToolCall，按 ``startedAt`` 排序，插入到主
     agent ``task`` 的 ``startedAt``↔``completedAt`` 窗口内；子 agent 自己
     的 ``task`` 派发递归 interleave 孙 agent（``seen`` 防环）；
  4. 输出统一 ``list[TimelineEntry]``。

NULL ``startedAt`` 兜底：用 ``completedAt`` / ``createdAt`` 回退；若仍为 NULL，
子 agent 的 turn/toolcall 紧跟主 agent 的 task call 之后。
"""

from __future__ import annotations

__all__ = [
    "CannbotDbReader",
    "SessionMeta",
    "TimelineEntry",
]

import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# cannbot session db 必含的表（校验用）
_REQUIRED_TABLES: frozenset[str] = frozenset(
    {
        "Session",
        "Turn",
        "ToolCall",
        "Execution",
        "InteractionBridge",
    }
)

# 框架方言 → opencode 方言的工具名映射。claude-code-proxy 录制的是 Claude
# 原生工具名（Agent/Write/Edit/Bash），下游四处消费点（时间线 interleaving、
# L2 转写、文件重放、事件合成）都按 opencode 方言匹配，在读取处统一归一化，
# 未知工具名原样透传。
_TOOL_NAME_ALIASES: dict[str, str] = {
    "Agent": "task",
    "Write": "write",
    "Edit": "edit",
    "Bash": "bash",
    "Read": "read",
    "Glob": "glob",
    "Grep": "grep",
    "MultiEdit": "multi_edit",
}


def normalize_tool_name(name: str) -> str:
    """框架方言工具名 → opencode 方言；未知名称原样返回。"""
    return _TOOL_NAME_ALIASES.get(name, name)


# ISO-8601 时间字符串 → 毫秒 epoch 的正则（兼容有无毫秒两种形式）
_ISO_MS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(?P<frac>\d+))?Z?$",
)


def _iso_to_ms(s: str | None) -> int:
    """ISO-8601 字符串 → 毫秒 epoch；解析失败返回 0。"""
    if not s:
        return 0
    m = _ISO_MS_RE.match(s.strip())
    if not m:
        # 兜底：尝试 Python fromisoformat（去 Z 后）
        try:
            from datetime import datetime

            t = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return int(t.timestamp() * 1000)
        except (ValueError, TypeError):
            return 0
    # 手算避免时区歧义（样例全是 UTC）
    try:
        date_part, time_part = s.strip().rstrip("Z").split("T", 1)
        y, mo, d = (int(x) for x in date_part.split("-"))
        if "." in time_part:
            hms, frac = time_part.split(".", 1)
        else:
            hms, frac = time_part, "0"
        h, mi, se = (int(x) for x in hms.split(":"))
        # 补齐到毫秒（3 位）
        frac_ms = int((frac + "000")[:3]) if frac else 0
    except (ValueError, TypeError):
        return 0
    # 用 calendar 公式（UTC）避免 datetime 时区差异
    import calendar

    return calendar.timegm((y, mo, d, h, mi, se, 0, 0, 0)) * 1000 + frac_ms


@dataclass(frozen=True)
class TimelineEntry:
    """统一时间线条目（主/子 tool call 或 turn 边界）。

    把主 agent 的 task tool call 与子 agent 的 turn/toolcall 按时间交错
    排序，供后续重放器逐条重放。``role`` 区分主/子。
    """

    ts: int  # startedAt / createdAt (ms)
    role: str  # "main" | "subagent"
    kind: str  # "turn" | "tool_call"
    turn: dict[str, Any] | None  # Turn 行（kind=turn 时非空）
    tool_call: dict[str, Any] | None  # ToolCall 行（kind=tool_call 时非空）
    subagent_session_id: str  # 子 agent sdk session id
    parent_task_call_id: str | None  # 子 agent 工具指向主 task tool call id


@dataclass(frozen=True)
class SessionMeta:
    """``Session`` 表顶层元数据。"""

    native_session_id: str  # Session.taskId（ses_xxx）
    initial_prompt: str  # Session.query
    origin_cwd: str  # Session.directory（原 cwd）
    model: str  # Session.model
    framework: str  # Session.framework（opencode / claude-code-proxy）
    source_path: str  # Session.sourcePath（仅 debug 用）
    root_execution_id: str  # Session.rootExecutionId


class CannbotDbReader:
    """读 cannbot session db + 构建统一时间线。所有公开方法只读。"""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self._con: sqlite3.Connection | None = None

    # ── 连接管理 ──────────────────────────────────────────────────────

    def __enter__(self) -> CannbotDbReader:
        self._connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def validate(self) -> bool:
        """校验是否 cannbot session db：文件存在、是 SQLite、含必填表。"""
        try:
            con = self._connect()
            tables = set()
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'",
            ).fetchall():
                tables.add(str(r[0]))
        except (sqlite3.Error, OSError):
            return False
        return _REQUIRED_TABLES.issubset(tables)

    # ── 校验 ────────────────────────────────────────────────────────

    def read_session(self) -> SessionMeta:
        """读 ``Session`` 行（取 ``createdAt`` 最早的）；无行抛 ``ValueError``。"""
        con = self._connect()
        rows = con.execute(
            "SELECT id, taskId, query, framework, model, directory, "
            "sourcePath, rootExecutionId FROM Session "
            "ORDER BY createdAt ASC",
        ).fetchall()
        if not rows:
            raise ValueError(f"Session table empty in {self.db_path}")
        r = rows[0]
        return SessionMeta(
            native_session_id=str(r["taskId"] or ""),
            initial_prompt=str(r["query"] or ""),
            origin_cwd=str(r["directory"] or ""),
            model=str(r["model"] or ""),
            framework=str(r["framework"] or "unknown"),
            source_path=str(r["sourcePath"] or ""),
            root_execution_id=str(r["rootExecutionId"] or ""),
        )

    # ── 顶层元数据 ──────────────────────────────────────────────────

    def read_executions(self) -> list[dict[str, Any]]:
        """读 ``Execution`` 表（建执行树，含主 depth=0 + 子 depth=1）。"""
        con = self._connect()
        rows = con.execute(
            "SELECT id, sessionId, agentName, agentSessionId, isSubagent, "
            "subagentType, subagentName, parentExecutionId, rootExecutionId, "
            "depth FROM Execution",
        ).fetchall()
        return [dict(r) for r in rows]

    # ── 子表读取 ─────────────────────────────────────────────────────

    def read_bridges(self) -> list[dict[str, Any]]:
        """读 ``InteractionBridge`` 表（主→子派发/响应映射）。

        旧版 recorder 的表可能还没有 ``subagentTokens`` 列；缺失时以 0
        回退读取，避免导入在 SELECT 上整体崩掉（``validate`` 只校验表）。
        """
        con = self._connect()
        cols = {str(r[1]) for r in con.execute("PRAGMA table_info(InteractionBridge)").fetchall()}
        tokens_expr = "subagentTokens" if "subagentTokens" in cols else "0 AS subagentTokens"
        rows = con.execute(
            "SELECT id, sessionId, dispatchExecutionId, dispatchTurnId, "
            "dispatchToolCallId, dispatchContent, dispatchTimestamp, "
            "responseExecutionId, responseTurnId, responseContent, "
            "responseTimestamp, subagentSessionId, subagentType, "
            f"subagentName, {tokens_expr}, status FROM InteractionBridge",
        ).fetchall()
        return [dict(r) for r in rows]

    def read_timeline(self) -> list[TimelineEntry]:
        """构建统一时间线：主 + 子交错排序。

        算法见模块 docstring。时间戳 NULL 时退到 ``completedAt``/``createdAt``。
        """
        bridges = self.read_bridges()
        bridge_map = _build_bridge_map(bridges)
        turns = self._read_turn_rows()
        tool_calls = self._read_tool_call_rows()
        tcs_by_turn = _group_tool_calls_by_turn(tool_calls)
        sub_turns = _group_subagent_turns(turns)
        main_turns = _sorted_main_turns(turns)

        timeline: list[TimelineEntry] = []
        # 全局已 emit 子会话集合：主循环派发、孤儿补齐与递归层共用同一
        # 实例。Turn 行序不保证祖先先于后代，多个派发点也可能指向同一
        # 子会话——任一入口 emit 过即登记，防止重复 interleave
        emitted_sub_sids: set[str] = set()
        _emit_main_turns(main_turns, tcs_by_turn, sub_turns, timeline, bridge_map, emitted_sub_sids)

        # 末尾若有孤儿子 turn（无对应主 task call），按时间序追加
        _append_orphan_subagents(sub_turns, tcs_by_turn, timeline, bridge_map, emitted_sub_sids)

        # 全局按 ts 稳定排序（同 ts 保持插入顺序）
        timeline.sort(key=lambda e: e.ts)
        return timeline

    def _connect(self) -> sqlite3.Connection:
        """打开只读连接（按需初始化）。"""
        if self._con is None:
            if not self.db_path.is_file():
                raise FileNotFoundError(f"cannbot session db not found: {self.db_path}")
            self._con = sqlite3.connect(
                f"file:{self.db_path}?mode=ro",
                uri=True,
                timeout=10.0,
            )
            self._con.execute("PRAGMA busy_timeout = 10000")
            self._con.row_factory = sqlite3.Row
        return self._con

    def _read_turn_rows(self) -> list[dict[str, Any]]:
        """读全量 ``Turn`` 行（不排序）。

        ``role='system'`` 行直接丢弃：cpx 导出的 db 把 per-request 注入的
        系统上下文（environment 描述、token 计数器）录成 system turn——
        实测单 session 可达数万行且共享导出时刻的 createdAt，不是对话
        内容；进时间线会污染 L2 转写与主 turn 计数。opencode 方言本就
        不用 Turn 存 system prompt，双方言下丢弃均安全。``role`` 为 NULL
        的行不是 system，COALESCE 显式保留（``!=`` 的三值逻辑会误滤）。
        """
        con = self._connect()
        rows = con.execute(
            "SELECT id, sessionId, turnIndex, role, content, contentJson, "
            "contentSummary, agentName, subagentName, subagentSessionId, "
            "parentExecutionId, finishReason, isSubagent, createdAt, "
            "completedAt, model FROM Turn WHERE COALESCE(role, '') != 'system'",
        ).fetchall()
        return [dict(r) for r in rows]

    # ── 时间线构建 ───────────────────────────────────────────────────

    def _read_tool_call_rows(self) -> list[dict[str, Any]]:
        """读全量 ``ToolCall`` 行（不排序），工具名做方言归一化。"""
        con = self._connect()
        rows = con.execute(
            "SELECT id, turnId, toolCallId, toolName, argsJson, resultJson, "
            "state, errorType, errorMessage, startedAt, completedAt, "
            "durationMs, dispatchBridgeId, isSkillRelated, createdAt "
            "FROM ToolCall",
        ).fetchall()
        result: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["toolName"] = normalize_tool_name(str(d.get("toolName") or ""))
            result.append(d)
        return result


def _extract_subagent_sid_from_args(tc: dict[str, Any]) -> str:
    """从 task tool_call 的 argsJson 抽 subagent_session_id（兜底）。"""
    import json

    raw = tc.get("argsJson") or ""
    if not raw:
        return ""
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return ""
    return str(obj.get("subagent_session_id") or "")


def _lookup_bridge_sub(bridge_map: dict[str, str], tc: dict[str, Any]) -> str:
    """按派发 tool call 查 bridge_map 得子 agent session id。

    ``InteractionBridge.dispatchToolCallId`` 的命名空间是 ``ToolCall.id``
    （录制器 cuid），不是 ``ToolCall.toolCallId``（框架原生 call id，如
    ``toolu_…``）——先按行 id 查，再按原生 call id 回退（旧样例 db 存在
    把原生 call id 写进 dispatchToolCallId 的形态）。
    """
    return (
        bridge_map.get(str(tc.get("id") or ""))
        or bridge_map.get(str(tc.get("toolCallId") or ""))
        or ""
    )


def _build_bridge_map(bridges: list[dict[str, Any]]) -> dict[str, str]:
    """dispatchToolCallId → subagentSessionId。"""
    bridge_map: dict[str, str] = {}
    for b in bridges:
        tc_id = str(b.get("dispatchToolCallId") or "")
        sub_sid = str(b.get("subagentSessionId") or "")
        if tc_id and sub_sid:
            bridge_map[tc_id] = sub_sid
    return bridge_map


def _group_tool_calls_by_turn(tool_calls: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """turn_id → 该 turn 的 tool_calls（按 startedAt 回退 completedAt 排序）。"""
    tcs_by_turn: dict[str, list[dict[str, Any]]] = {}
    for tc in tool_calls:
        tcs_by_turn.setdefault(str(tc["turnId"]), []).append(tc)
    for tcs in tcs_by_turn.values():
        tcs.sort(key=lambda t: _iso_to_ms(t.get("startedAt")) or _iso_to_ms(t.get("completedAt")))
    return tcs_by_turn


def _group_subagent_turns(turns: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """subagentSessionId → 子 agent 的 turns（按 createdAt 排序）。"""
    sub_turns: dict[str, list[dict[str, Any]]] = {}
    for t in turns:
        if t.get("isSubagent"):
            sid = str(t.get("subagentSessionId") or "")
            if sid:
                sub_turns.setdefault(sid, []).append(t)
    for t_list in sub_turns.values():
        t_list.sort(key=lambda t: _iso_to_ms(t.get("createdAt")))
    return sub_turns


def _sorted_main_turns(turns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """主 agent 的 turns（isSubagent falsy），按 turnIndex/createdAt 排序。"""
    main_turns = [t for t in turns if not t.get("isSubagent")]
    main_turns.sort(key=lambda t: (int(t.get("turnIndex") or 0), _iso_to_ms(t.get("createdAt"))))
    return main_turns


def _emit_turn(
    timeline: list[TimelineEntry], role: str, t: dict[str, Any], sub_sid: str, parent_tc: str | None
) -> None:
    """把 turn 行 emit 到时间线（ts 取 createdAt 回退 completedAt）。"""
    timeline.append(
        TimelineEntry(
            ts=_iso_to_ms(t.get("createdAt")) or _iso_to_ms(t.get("completedAt")),
            role=role,
            kind="turn",
            turn=t,
            tool_call=None,
            subagent_session_id=sub_sid,
            parent_task_call_id=parent_tc,
        )
    )


def _emit_tool_call(
    timeline: list[TimelineEntry],
    role: str,
    tc: dict[str, Any],
    sub_sid: str,
    parent_tc: str | None,
) -> None:
    """把 tool_call 行 emit 到时间线（ts 取 startedAt 回退 completedAt）。"""
    timeline.append(
        TimelineEntry(
            ts=_iso_to_ms(tc.get("startedAt")) or _iso_to_ms(tc.get("completedAt")),
            role=role,
            kind="tool_call",
            turn=None,
            tool_call=tc,
            subagent_session_id=sub_sid,
            parent_task_call_id=parent_tc,
        )
    )


def _emit_main_turns(
    main_turns: list[dict[str, Any]],
    tcs_by_turn: dict[str, list[dict[str, Any]]],
    sub_turns: dict[str, list[dict[str, Any]]],
    timeline: list[TimelineEntry],
    bridge_map: dict[str, str],
    emitted_sub_sids: set[str],
) -> None:
    """按主 agent turn 顺序 emit；task tool call 后 interleave 子 agent。"""
    for mt in main_turns:
        mt_id = str(mt["id"])
        _emit_turn(timeline, "main", mt, "", None)
        for tc in tcs_by_turn.get(mt_id, []):
            tool_name = str(tc.get("toolName") or "")
            if tool_name != "task":
                _emit_tool_call(timeline, "main", tc, "", None)
                continue
            # task 条目须携带解析后的子会话 id 再 emit：检查点链路只读
            # entry.subagent_session_id（args 回退），cpx 的 args 无该字段
            sub_sid = _lookup_bridge_sub(bridge_map, tc) or _extract_subagent_sid_from_args(tc)
            _emit_tool_call(timeline, "main", tc, sub_sid, None)
            if not sub_sid:
                logger.warning(
                    "timeline: task tool_call %s has no subagent session id; "
                    "skipping subagent timeline interleave",
                    str(tc.get("toolCallId") or ""),
                )
                continue
            _interleave_subagent(
                sub_sid,
                sub_turns,
                tcs_by_turn,
                timeline,
                parent_tc=str(tc.get("toolCallId") or ""),
                bridge_map=bridge_map,
                seen=emitted_sub_sids,
            )


def _append_orphan_subagents(
    sub_turns: dict[str, list[dict[str, Any]]],
    tcs_by_turn: dict[str, list[dict[str, Any]]],
    timeline: list[TimelineEntry],
    bridge_map: dict[str, str],
    emitted_sub_sids: set[str],
) -> None:
    """把无对应主 task call 的孤儿子会话按时间序追加到时间线末尾。

    判重用全局 emitted 集合——bridge 值只是代理，bridge 存在但其派发
    tool call 行缺失时子 agent 并未被 interleave，仍需在此补齐。
    """
    for sid in sub_turns:
        if sid in emitted_sub_sids:
            continue
        logger.warning(
            "timeline: subagent %s has no matching main task call; appending at end",
            sid,
        )
        _interleave_subagent(
            sid,
            sub_turns,
            tcs_by_turn,
            timeline,
            bridge_map=bridge_map,
            seen=emitted_sub_sids,
        )


def _interleave_task_children(
    tc: dict[str, Any],
    sub_turns: dict[str, list[dict[str, Any]]],
    tcs_by_turn: dict[str, list[dict[str, Any]]],
    timeline: list[TimelineEntry],
    bmap: dict[str, str],
    seen: set[str],
) -> None:
    """task 派发行的孙 agent 递归 interleave（无 child sid 时跳过）。"""
    child_sid = _lookup_bridge_sub(bmap, tc) or _extract_subagent_sid_from_args(tc)
    if not child_sid:
        return
    _interleave_subagent(
        child_sid,
        sub_turns,
        tcs_by_turn,
        timeline,
        parent_tc=str(tc.get("toolCallId") or ""),
        bridge_map=bmap,
        seen=seen,
    )


def _interleave_subagent(
    sub_sid: str,
    sub_turns: dict[str, list[dict[str, Any]]],
    tcs_by_turn: dict[str, list[dict[str, Any]]],
    timeline: list[TimelineEntry],
    parent_tc: str | None = None,
    bridge_map: dict[str, str] | None = None,
    seen: set[str] | None = None,
) -> None:
    """按时间序把子 agent 的 turn + tool_call emit 到时间线。

    嵌套派发（子 agent 自己再派 task）递归 interleave 孙 agent——否则孙
    agent 的 turn 会因无人认领被整体丢弃。``seen`` 由调用方传入，是贯穿
    所有入口的全局已 emit 集合：既防 bridge 数据成环，也让同一子会话被
    不同派发点/孤儿补齐重复指向时只 emit 一次。
    """
    if seen is None:
        seen = set()
    if sub_sid in seen:
        return
    seen.add(sub_sid)
    bmap = bridge_map if bridge_map is not None else {}
    t_list = sub_turns.get(sub_sid, [])
    for t in t_list:
        t_id = str(t["id"])
        _emit_turn(timeline, "subagent", t, sub_sid, parent_tc)
        for tc in tcs_by_turn.get(t_id, []):
            _emit_tool_call(timeline, "subagent", tc, sub_sid, parent_tc)
            if str(tc.get("toolName") or "") == "task":
                _interleave_task_children(tc, sub_turns, tcs_by_turn, timeline, bmap, seen)
