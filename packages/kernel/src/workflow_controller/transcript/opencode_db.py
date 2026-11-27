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

"""opencode.db 读取轨：v1 三表（session/message/part）与 v2 双表
（session_v2/session_message，opencode ≥2.0.26）同构产出 RawInteraction。

按会话落表探测：v2 命中优先，否则回落 v1 三表；两轨先归一为同构消息行
再共用装配。
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, cast

from .types import RawInteraction, SessionMeta, TokenUsage, ToolCallInfo

if TYPE_CHECKING:
    import sqlite3

logger = logging.getLogger(__name__)


def _task_part_session_id(r: sqlite3.Row) -> str | None:
    """task 工具 part 行里的 subagent 会话 id（task_id: 模式提取）；无则 None。"""
    import re

    pd = json.loads(r["data"])
    output = pd.get("output") or pd.get("state", {}).get("output")
    if not output:
        return None
    s = output if isinstance(output, str) else json.dumps(output)
    # match "task_id: <id>" pattern
    m = re.search(r"task_id:\s*(\S+)", s)
    return m.group(1).strip() if m else None


def list_subagent_sessions(db: sqlite3.Connection, parent_session_id: str) -> list[str]:
    """Find subagent session ids by scanning task tool results for task_id:."""
    rows = db.execute(
        """SELECT data FROM part WHERE json_extract(data, '$.type') = 'tool'
           AND json_extract(data, '$.tool') IN ('task', 'Agent')"""
    ).fetchall()
    ids: list[str] = []
    seen: set[str] = set()
    for r in rows:
        try:
            sid = _task_part_session_id(r)
        except Exception:
            logger.debug("task 工具结果解析失败，跳过该 part")
            continue
        if sid and sid not in seen:
            seen.add(sid)
            ids.append(sid)
    return ids


def read_session_meta(db: sqlite3.Connection, session_id: str) -> SessionMeta:
    row = _v2_session_row(db, session_id)
    if row is not None:
        return SessionMeta(
            parent_id=row["parent_id"] if row["parent_id"] else None,
            version=row["version"],
            directory=row["directory"],
            summary_additions=row["summary_additions"] or 0,
            summary_deletions=row["summary_deletions"] or 0,
            summary_files=row["summary_files"] or 0,
        )
    if not _table_exists(db, "session"):
        return SessionMeta()
    row = db.execute(
        "SELECT parent_id, version, directory, summary_additions, summary_deletions, summary_files "
        "FROM session WHERE id = ?",
        (session_id,),
    ).fetchone()
    if not row:
        return SessionMeta()
    return SessionMeta(
        parent_id=row["parent_id"] if row["parent_id"] else None,
        version=row["version"],
        directory=row["directory"],
        summary_additions=row["summary_additions"] or 0,
        summary_deletions=row["summary_deletions"] or 0,
        summary_files=row["summary_files"] or 0,
    )


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    return (
        db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()
        is not None
    )


def _v2_session_row(db: sqlite3.Connection, session_id: str) -> sqlite3.Row | None:
    """opencode ≥2.0.26 的会话行（session_v2）；表不存在返回 None。"""
    if not _table_exists(db, "session_v2"):
        return None
    return db.execute(
        "SELECT parent_id, title, version, directory, "
        "summary_additions, summary_deletions, summary_files "
        "FROM session_v2 WHERE id = ?",
        (session_id,),
    ).fetchone()


def _subagent_name_from_title(title: str | None) -> str | None:
    if not title:
        return None
    import re

    m = re.match(r"@\((\w+)\)\s+subagent", title)
    if m:
        return m.group(1)
    m2 = re.match(r"@(\w+)", title)
    return m2.group(1) if m2 else None


# ── v1/v2 双轨：先归一为同构的消息行，再共用装配 ──────────────────


def read_session(db: sqlite3.Connection, session_id: str) -> list[RawInteraction]:
    """读会话消息 → RawInteraction[]。

    v2 优先：opencode ≥2.0.26 的 --standalone 会话只写
    session_v2/session_message（内容内嵌 data JSON），会话不在 v2 表时回落
    v1 三表（session/message/part）——两轨产出同构。
    """
    v2_row = _v2_session_row(db, session_id)
    if v2_row is not None:
        is_subagent, subagent_name = (
            bool(v2_row["parent_id"]),
            _subagent_name_from_title(v2_row["title"]),
        )
        rows = _v2_message_rows(db, session_id)
    else:
        if not _table_exists(db, "session"):
            return []
        v1_row = db.execute(
            "SELECT parent_id, title FROM session WHERE id = ?",
            (session_id,),
        ).fetchone()
        if v1_row is None:
            return []
        is_subagent, subagent_name = (
            bool(v1_row["parent_id"]),
            _subagent_name_from_title(v1_row["title"]),
        )
        rows = _v1_message_rows(db, session_id)
    return _to_interactions(rows, session_id, is_subagent, subagent_name)


def _norm_time_info(data_time: dict[str, Any] | None, row_created: int) -> dict[str, Any]:
    """created 缺省回落行时间戳；completed 仅在非 None 时携带。"""
    if not data_time:
        return {"created": row_created}
    info = {"created": data_time.get("created", row_created)}
    if data_time.get("completed") is not None:
        info["completed"] = data_time["completed"]
    return info


def _fetch_parts_by_msg(
    db: sqlite3.Connection,
    msg_ids: list[str],
    placeholders: str,
    part_type: str,
) -> dict[str, list[dict[str, Any]]]:
    """按 part.type 批读 part 行 → message_id → data dict 列表。"""
    rows = db.execute(
        f"SELECT message_id, data FROM part WHERE message_id IN ({placeholders}) "
        f"AND json_extract(data, '$.type') = ? ORDER BY time_created",
        (*msg_ids, part_type),
    ).fetchall()
    by_msg: dict[str, list[dict[str, Any]]] = {}
    for p in rows:
        try:
            pd = json.loads(p["data"])
            by_msg.setdefault(p["message_id"], []).append(pd)
        except (TypeError, ValueError):
            continue
    return by_msg


def _v1_norm_row(
    msg: sqlite3.Row,
    text_by_msg: dict[str, list[dict[str, Any]]],
    reasoning_by_msg: dict[str, list[dict[str, Any]]],
    tool_calls_by_msg: dict[str, list[ToolCallInfo]],
) -> dict[str, Any]:
    """v1 message 行 + 各类 part → 归一消息行。"""
    try:
        msg_data = json.loads(msg["data"])
    except Exception:
        msg_data = {}
    direct = msg_data.get("content")
    return {
        "id": msg["id"],
        "role": msg_data.get("role", "unknown"),
        "time_created": int(msg["time_created"]),
        "content_direct": direct if isinstance(direct, str) else None,
        "text_parts": [p.get("text", "") for p in text_by_msg.get(msg["id"], []) if p.get("text")],
        "reasoning_parts": [
            p.get("text", "") for p in reasoning_by_msg.get(msg["id"], []) if p.get("text")
        ],
        "tool_calls": tool_calls_by_msg.get(msg["id"]),
        "time_info": _norm_time_info(msg_data.get("time"), int(msg["time_created"])),
        "tokens": msg_data.get("tokens"),
        "cost": msg_data.get("cost", 0),
        "model_id": msg_data.get("modelID"),
        "provider_id": msg_data.get("providerID"),
        "model_obj": msg_data.get("model") if isinstance(msg_data.get("model"), dict) else None,
        "finish": msg_data.get("finish"),
        "agent": msg_data.get("agent"),
    }


def _v1_message_rows(
    db: sqlite3.Connection,
    session_id: str,
) -> list[dict]:
    """v1 三表 → 归一消息行（msg_data.content 字符串直通，parts 按类型批读）。"""
    messages = db.execute(
        "SELECT id, time_created, data FROM message WHERE session_id = ? ORDER BY time_created",
        (session_id,),
    ).fetchall()
    msg_ids = [m["id"] for m in messages]
    if not msg_ids:
        return []

    placeholders = ",".join("?" * len(msg_ids))
    text_by_msg = _fetch_parts_by_msg(db, msg_ids, placeholders, "text")
    reasoning_by_msg = _fetch_parts_by_msg(db, msg_ids, placeholders, "reasoning")
    tool_calls_by_msg = _tool_calls_v1(_fetch_parts_by_msg(db, msg_ids, placeholders, "tool"))

    out: list[dict] = []
    for msg in messages:
        out.append(_v1_norm_row(msg, text_by_msg, reasoning_by_msg, tool_calls_by_msg))
    return out


def _v1_args_json(pd: dict[str, Any]) -> str | None:
    """v1 tool part 的参数 JSON：input 直取，否则从 state.input 合并 subagent 元数据。"""
    if pd.get("input"):
        return json.dumps(pd["input"])
    if not pd.get("state", {}).get("input"):
        return None
    merged: dict = dict(pd["state"]["input"])
    meta = pd["state"].get("metadata", {})
    if meta.get("sessionId"):
        merged["subagent_session_id"] = meta["sessionId"]
    if meta.get("model"):
        merged["subagent_model"] = meta["model"]
    if pd["state"].get("title"):
        merged["summary"] = pd["state"]["title"]
    return json.dumps(merged)


def _v1_result_json(pd: dict[str, Any]) -> str | None:
    """v1 tool part 的结果 JSON：output 直取，否则读 state.output。"""
    if pd.get("output"):
        out = pd["output"]
        return out if isinstance(out, str) else json.dumps(out)
    if pd.get("state", {}).get("output"):
        out = pd["state"]["output"]
        return out if isinstance(out, str) else json.dumps(out)
    return None


def _tool_calls_v1(tool_parts_by_msg: dict[str, list[dict]]) -> dict[str, list[ToolCallInfo]]:
    tool_calls_by_msg: dict[str, list[ToolCallInfo]] = {}
    for msg_id, parts in tool_parts_by_msg.items():
        for pd in parts:
            call_id = pd.get("callID", "")
            tool_name = pd.get("tool", "")
            state = "unknown"
            if pd.get("state"):
                state = pd["state"].get("status", "unknown")

            args_json = _v1_args_json(pd)
            result_json = _v1_result_json(pd)

            part_time = (pd.get("state") or {}).get("time")
            started_ms = part_time.get("created") if isinstance(part_time, dict) else None

            tool_calls_by_msg.setdefault(msg_id, []).append(
                ToolCallInfo(
                    tool_call_id=call_id,
                    tool_name=tool_name,
                    args_json=args_json,
                    result_json=result_json,
                    state=state,
                    started_ms=started_ms,
                )
            )
    return tool_calls_by_msg


def _v2_norm_row(row: sqlite3.Row) -> dict[str, Any] | None:
    """v2 session_message 行 → 归一消息行；解析失败或无转录语义行返回 None。"""
    try:
        d = json.loads(row["data"])
    except (TypeError, ValueError):
        return None
    mtype = row["type"]
    if mtype not in ("user", "assistant", "system"):
        return None
    norm = {
        "id": row["id"],
        "role": mtype,
        "time_created": int(row["time_created"]),
        "content_direct": None,
        "text_parts": [],
        "reasoning_parts": [],
        "tool_calls": None,
        "time_info": _norm_time_info(d.get("time"), int(row["time_created"])),
        "tokens": d.get("tokens"),
        "cost": d.get("cost", 0),
        "model_id": None,
        "provider_id": None,
        "model_obj": None,
        "finish": d.get("finish"),
        "agent": d.get("agent"),
    }
    if mtype == "assistant":
        blocks = d.get("content") or []
        norm["text_parts"] = [
            b.get("text", "") for b in blocks if b.get("type") == "text" and b.get("text")
        ]
        norm["reasoning_parts"] = [
            b.get("text", "") for b in blocks if b.get("type") == "reasoning" and b.get("text")
        ]
        calls = [_tool_call_v2(b) for b in blocks if b.get("type") == "tool"]
        norm["tool_calls"] = calls or None
        model = d.get("model")
        if isinstance(model, dict):
            norm["model_id"] = model.get("id") or model.get("modelID")
            norm["provider_id"] = model.get("providerID")
    else:
        text = d.get("text")
        norm["content_direct"] = text if isinstance(text, str) else None
    return norm


def _v2_message_rows(
    db: sqlite3.Connection,
    session_id: str,
) -> list[dict]:
    """v2 session_message → 归一消息行（块全部内嵌 data JSON，part 表零行）。

    agent-switched/idle 是信封行（无转录语义）跳过；seq 为 TEXT，按数值序。
    """
    rows = db.execute(
        "SELECT id, type, seq, time_created, data FROM session_message "
        "WHERE session_id = ? ORDER BY CAST(seq AS INTEGER), time_created",
        (session_id,),
    ).fetchall()
    out: list[dict] = []
    for row in rows:
        norm = _v2_norm_row(row)
        if norm is not None:
            out.append(norm)
    return out


def _tool_call_v2(block: dict) -> ToolCallInfo:
    """v2 tool 块 → ToolCallInfo（callID=块 id、输出=state.content 文本块、
    subagent 链接=state.metadata.sessionId）。
    """
    state = block.get("state") or {}
    meta = state.get("metadata") or {}
    args_json: str | None = None
    if state.get("input") is not None:
        merged: dict = dict(state["input"])
        if meta.get("sessionId"):
            merged["subagent_session_id"] = meta["sessionId"]
        model = meta.get("model")
        if isinstance(model, dict):
            merged["subagent_model"] = (
                f"{model.get('providerID')}/{model.get('modelID')}"
                if model.get("providerID") and model.get("modelID")
                else model.get("modelID")
            )
        elif model:
            merged["subagent_model"] = model
        if meta.get("title"):
            merged["summary"] = meta["title"]
        args_json = json.dumps(merged)

    result = state.get("content")
    result_json: str | None = None
    if isinstance(result, str):
        result_json = result
    elif isinstance(result, list):
        texts = [
            b.get("text", "") for b in result if isinstance(b, dict) and b.get("type") == "text"
        ]
        result_json = "\n".join(t for t in texts if t) or None

    block_time = block.get("time") or {}
    return ToolCallInfo(
        tool_call_id=block.get("id") or block.get("callID", ""),
        tool_name=block.get("name") or block.get("tool", ""),
        args_json=args_json,
        result_json=result_json,
        state=state.get("status", "unknown"),
        started_ms=block_time.get("created") if isinstance(block_time, dict) else None,
    )


def _row_usage(row: dict[str, Any]) -> TokenUsage | None:
    """行 tokens → TokenUsage（cache 字段容错为 dict）。"""
    tk = row["tokens"]
    if not tk:
        return None
    cache = tk.get("cache", {}) if isinstance(tk.get("cache"), dict) else {}
    return TokenUsage(
        total=tk.get("total", 0),
        input=tk.get("input", 0),
        output=tk.get("output", 0),
        reasoning=tk.get("reasoning", 0),
        cache_read=cache.get("read", 0),
        cache_write=cache.get("write", 0),
        cost=row["cost"] or 0,
    )


def _row_model_ids(row: dict[str, Any]) -> tuple[str | None, str | None]:
    """(provider_id, model_id)：v1 平铺键优先，缺失再读 model 对象。"""
    provider_id = cast("str | None", row["provider_id"])
    model_id = cast("str | None", row["model_id"])
    if not model_id and isinstance(row["model_obj"], dict):
        provider_id = cast("str | None", row["model_obj"].get("providerID"))
        model_id = cast("str | None", row["model_obj"].get("modelID") or row["model_obj"].get("id"))
    return provider_id, model_id


def _row_content(row: dict[str, Any]) -> str | None:
    """正文：直通 content_direct，否则拼 reasoning/text parts（DC4 分隔）。"""
    content = cast("str | None", row["content_direct"])
    if content is not None:
        return content
    parts: list[str] = []
    if row["reasoning_parts"]:
        parts.append(f"<thinking>{chr(10).join(row['reasoning_parts'])}</thinking>")
    if row["text_parts"]:
        parts.append(chr(10).join(row["text_parts"]))
    return chr(10 + 10).join(parts) if parts else None


def _row_to_interaction(
    row: dict[str, Any],
    session_id: str,
    is_subagent: bool,
    subagent_name: str | None,
) -> RawInteraction:
    """归一消息行 → RawInteraction（v1/v2 共用装配）。"""
    time_info = row["time_info"]
    latency: float | None = None
    if time_info.get("completed") and time_info.get("created"):
        latency = time_info["completed"] - time_info["created"]
    provider_id, model_id = _row_model_ids(row)
    composed = f"{provider_id}/{model_id}" if provider_id and model_id else model_id
    return RawInteraction(
        role=row["role"],
        content=_row_content(row),
        timestamp=_to_iso(row["time_created"]),
        time_info=time_info,
        id=row["id"],
        reasoning="\n".join(row["reasoning_parts"]) if row["reasoning_parts"] else None,
        agent=row["agent"],
        subagent_name=subagent_name if is_subagent else None,
        subagent_session_id=session_id if is_subagent else None,
        tool_calls=row["tool_calls"],
        usage=_row_usage(row),
        model=composed,
        model_id=model_id,
        provider_id=provider_id,
        latency=latency,
        finish_reason=row["finish"],
    )


def _to_interactions(
    rows: list[dict],
    session_id: str,
    is_subagent: bool,
    subagent_name: str | None,
) -> list[RawInteraction]:
    """归一消息行 → RawInteraction[]（v1/v2 共用装配，保证两轨同构）。"""
    result: list[RawInteraction] = []
    for row in rows:
        try:
            result.append(_row_to_interaction(row, session_id, is_subagent, subagent_name))
        except Exception:
            result.append(
                RawInteraction(
                    role="unknown",
                    content=None,
                    timestamp=_to_iso(row["time_created"]),
                    time_info={"created": row["time_created"]},
                )
            )
    return result


def _to_iso(ts: int) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(ts / 1000, tz=UTC).isoformat()
