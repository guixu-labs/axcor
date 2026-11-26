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

"""cannbot Turn/ToolCall → opencode.db session/message/part 转写器。

把主 agent 的 Turn+ToolCall 转写成 opencode.db 的 ``session``/``message``/
``part`` 行，供 ``TracebackRunner`` 重放时截断 L2 + 定位 ``task`` part。

只转写主 agent（``Turn.isSubagent=0``）——``TracebackRunner`` 重放时只读主
session 的 part 流；子 agent 由主 agent 重新派发。

关键：``task`` part 的 ``state.output`` 必须含子 agent 的 sdk session id——
这是 ``find_main_part_id_for_subagent`` 定位截断点的依据。
"""

from __future__ import annotations

__all__ = [
    "L2Transcriber",
    "TranscribeResult",
]

import contextlib
import json
import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from workflow_controller.plugins.agent_traceback.l2_reader import _init_fork_db_from_source
from workflow_controller.plugins.agent_traceback.session_reconstructor.db_reader import (
    SessionMeta,
    TimelineEntry,
    _extract_subagent_sid_from_args,
    _iso_to_ms,
    _lookup_bridge_sub,
)
from workflow_controller.transcript.opencode_store import BUSY_TIMEOUT_MS, opencode_db_path

logger = logging.getLogger(__name__)

# Turn.content 里 <thinking>...</thinking> 块的正则
_THINKING_RE = re.compile(r"<thinking>(?P<text>.*?)</thinking>", re.DOTALL | re.IGNORECASE)


def _is_task_result_content(content: str) -> bool:
    """判断 user turn 是否为 driver 注入的 task result（非真实用户消息）。

    opencode 把 task result 放在 tool part 的 state.output，不产生独立
    user message，故转写时跳过。
    """
    return content.startswith("task_id:") or "<task_result>" in content


def _split_model(model: str) -> tuple[str, str]:
    """拆分 'provider/modelID' → ('provider', 'modelID')。"""
    if "/" in model:
        pid, _, mid = model.partition("/")
        return pid, mid
    return "", model


@dataclass
class TranscribeResult:
    """转写结果。``task_part_ids`` 供检查点推导预填 ``main_part_id``。"""

    task_part_ids: dict[str, str]  # {subagent_native_session_id: main_task_part_id}
    opencode_db_path: Path
    message_count: int
    part_count: int


class L2Transcriber:
    """把 cannbot 主 agent Turn/ToolCall 转写为 opencode.db 行。"""

    def __init__(
        self,
        target_cwd: Path,
        origin_cwd: str,
        source_opencode_db: Path | None = None,
    ) -> None:
        self.target_cwd = Path(target_cwd).expanduser().resolve()
        self.origin_cwd = origin_cwd
        # source opencode.db（schema 模板）。None → 宿主默认
        self.source_opencode_db = (
            Path(source_opencode_db).expanduser().resolve()
            if source_opencode_db is not None
            else opencode_db_path()
        )
        # 输出：<target_cwd>/.workflow-controller/opencode/opencode.db
        self.dest_opencode_db = (
            self.target_cwd / ".workflow-controller" / "opencode" / "opencode.db"
        )
        # project_id：session.project_id NOT NULL，须指向真实 project 行。
        # 用 target_cwd 的 hash 做稳定 id，在 _write_rows 里 upsert 占位行。
        import hashlib

        self._project_id = (
            "rc_proj_"
            + hashlib.sha1(
                str(self.target_cwd).encode(),
                usedforsecurity=False,
            ).hexdigest()[:12]
        )

    # ── 入口 ──────────────────────────────────────────────────────────

    @staticmethod
    def _resolve_turn_ts(
        turn_e: TimelineEntry,
        tool_call_entries: list[TimelineEntry],
    ) -> int:
        """解析 turn 的有效时间戳，让同 message 的所有 part 共享一致值。

        cannbot db 的 ``Turn.createdAt`` 可能是 session 导出时间（所有 turn 共享
        同一值），非真实 turn 起始。assistant turn 用首个 tool call 的
        ``startedAt``（真实时间）；user turn 无 tool call 时用 ``createdAt``。
        避免按 time_created 排序时非 tool part 被排到截断点之后丢弃。
        """
        for e in tool_call_entries:
            if e.tool_call is not None:
                ts = _iso_to_ms(e.tool_call.get("startedAt"))
                if ts:
                    return ts
        # 无 tool call：user turn 用 createdAt（非 completedAt，避免打字延迟偏移）
        return turn_e.ts

    # ── 行构造 ────────────────────────────────────────────────────────

    @staticmethod
    def _format_model_json(model: str | None) -> str | None:
        """'provider/modelID' → opencode 的 JSON model 格式。"""
        if not model:
            return None
        pid, mid = _split_model(model)
        return json.dumps(
            {"id": mid, "providerID": pid, "variant": "default"},
            ensure_ascii=False,
        )

    @staticmethod
    def _insert_row(con: sqlite3.Connection, table: str, row: dict[str, Any]) -> None:
        """动态列发现 + INSERT OR REPLACE。"""
        cols = [c[1] for c in con.execute(f"PRAGMA table_info({table})").fetchall()]
        if not cols:
            return
        present = [c for c in cols if c in row]
        if not present:
            return
        placeholders = ",".join("?" for _ in present)
        col_list = ",".join(present)
        con.execute(
            f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})",
            [row[c] for c in present],
        )

    def transcribe(
        self,
        timeline: list[TimelineEntry],
        session_meta: SessionMeta,
        native_session_id: str,
        bridges: list[dict[str, Any]] | None = None,
    ) -> TranscribeResult:
        """转写主 agent Turn+ToolCall → opencode.db。

        返回 ``TranscribeResult``（含 ``task_part_ids`` 映射）。副作用：
        在 ``<target_cwd>/.workflow-controller/opencode/opencode.db`` 写出新 db。
        """
        # dispatchToolCallId → subagentSessionId 映射（查找语义见 _lookup_bridge_sub）
        bridge_map: dict[str, str] = {}
        for b in bridges or []:
            tc_id = str(b.get("dispatchToolCallId") or "")
            sub_sid = str(b.get("subagentSessionId") or "")
            if tc_id and sub_sid:
                bridge_map[tc_id] = sub_sid

        # 初始化 fork opencode.db（source 校验 + 空 dest 整库覆盖）
        self._init_dest_db()

        # 构造 session/message/part 行
        sid = native_session_id
        session_row = self._build_session_row(session_meta, sid)
        main_turns, main_tcs_by_turn = _group_main_entries(timeline)
        messages, parts, task_part_ids = self._transcribe_main_turns(
            main_turns, main_tcs_by_turn, sid, session_meta, bridge_map
        )

        # 写入 opencode.db（失败清理半成品）
        try:
            self._write_rows(session_row, messages, parts)
        except Exception:
            self._remove_partial_dest()
            raise

        logger.info(
            "transcribe: wrote %d messages + %d parts for session %s at %s (task_part_ids=%d)",
            len(messages),
            len(parts),
            sid[:12],
            self.dest_opencode_db,
            len(task_part_ids),
        )
        return TranscribeResult(
            task_part_ids=task_part_ids,
            opencode_db_path=self.dest_opencode_db,
            message_count=len(messages),
            part_count=len(parts),
        )

    def rewrite_prompt_paths(self, text: str) -> str:
        """重写文本内嵌的 origin_cwd 路径（模块级转写 helper 经此公开面调用）。"""
        return self._rewrite_prompt_paths(text)

    def rewrite_obj_paths(self, obj: Any) -> Any:
        """递归重写 JSON 树中的 origin_cwd 路径（模块级转写 helper 经此公开面调用）。"""
        return self._rewrite_obj_paths(obj)

    def _init_dest_db(self) -> None:
        """校验 source 存在并整库覆盖初始化 dest fork db（失败清理半成品）。"""
        # 校验 source 存在：sqlite3.connect 对不存在的文件会建空 db，但缺 schema
        # ——下游写入会抛 OperationalError。早抛 RuntimeError 让 orchestrator 上报根因。
        if not self.source_opencode_db.is_file():
            raise RuntimeError(
                f"source opencode.db not found: {self.source_opencode_db} "
                f"(set config reconstruct.source_opencode_db or ensure host default exists)"
            )
        self.dest_opencode_db.parent.mkdir(parents=True, exist_ok=True)
        # schema 回放要求空 dest：同 target_cwd 重复导入（或 kill -9 残骸）时
        # 整库覆盖，否则 CREATE TABLE 撞已存在表失败还会误删旧库
        self._remove_partial_dest()
        try:
            if not _init_fork_db_from_source(self.dest_opencode_db, self.source_opencode_db):
                raise RuntimeError(
                    f"failed to init fork opencode.db from {self.source_opencode_db} "
                    f"to {self.dest_opencode_db}"
                )
        except Exception:
            self._remove_partial_dest()
            raise

    def _transcribe_main_turns(
        self,
        main_turns: list[TimelineEntry],
        main_tcs_by_turn: dict[str, list[TimelineEntry]],
        sid: str,
        session_meta: SessionMeta,
        bridge_map: dict[str, str],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
        """逐 main turn 构造 message/part 行，返回 (messages, parts, task_part_ids)。"""
        messages: list[dict[str, Any]] = []
        parts: list[dict[str, Any]] = []
        task_part_ids: dict[str, str] = {}
        # opencode 的 message parentID：同一轮多个 assistant step 指向同一 user
        # message；task result 是 driver 注入的 user message，跳过避免 resume 异常
        last_user_msg_id: str | None = None
        part_idx = 0
        for msg_idx, turn_e in enumerate(main_turns):
            t = turn_e.turn
            if t is None:
                raise RuntimeError("turn 条目缺少 turn 数据")
            role = str(t.get("role") or "assistant")
            content = str(t.get("content") or "")
            # 跳过 task-result user messages
            if role == "user" and _is_task_result_content(content):
                continue
            tcs = main_tcs_by_turn.get(str(t["id"]), [])
            msg_id, effective_ts, finish_reason, msg_data = self._build_turn_message(
                t, turn_e, msg_idx, sid, role, session_meta.model, last_user_msg_id, tcs
            )
            messages.append(_message_row(msg_id, sid, effective_ts, msg_data))
            if role == "user":
                last_user_msg_id = msg_id
                part_idx = self._append_user_part(
                    content, sid, msg_id, effective_ts, parts, part_idx
                )
            else:
                rows, n = self._build_assistant_parts(
                    content,
                    tcs,
                    sid,
                    msg_id,
                    effective_ts,
                    part_idx,
                    bridge_map,
                    task_part_ids,
                    finish_reason=finish_reason,
                )
                parts.extend(rows)
                part_idx += n
        return messages, parts, task_part_ids

    def _build_turn_message(
        self,
        t: dict[str, Any],
        turn_e: TimelineEntry,
        msg_idx: int,
        sid: str,
        role: str,
        model: str,
        last_user_msg_id: str | None,
        tcs: list[TimelineEntry],
    ) -> tuple[str, int, str, dict[str, Any]]:
        """构造单个 turn 的 message 上下文：(msg_id, effective_ts, finish_reason, msg_data)。"""
        # opencode MessageV2 schema requires message id to start with "msg".
        msg_id = f"msg_rc_{sid[:8]}_{msg_idx:04d}"
        finish_reason = str(t.get("finishReason") or "")
        # 时间戳语义见 _resolve_turn_ts：同 message 的所有 part 共享一致时间戳
        effective_ts = self._resolve_turn_ts(turn_e, tcs)
        msg_data = self._build_message_data(
            role, finish_reason, effective_ts, model, last_user_msg_id
        )
        return msg_id, effective_ts, finish_reason, msg_data

    def _build_message_data(
        self,
        role: str,
        finish_reason: str,
        effective_ts: int,
        model: str,
        last_user_msg_id: str | None,
    ) -> dict[str, Any]:
        """构造 message 的 data JSON（user / assistant 两态）。"""
        msg_data: dict[str, Any] = {"role": role, "time": {"created": effective_ts}}
        if role == "user":
            msg_data["agent"] = "build"
            if model:
                pid, mid = _split_model(model)
                msg_data["model"] = {"providerID": pid, "modelID": mid}
            msg_data["summary"] = {"diffs": []}
            return msg_data
        if last_user_msg_id is not None:
            msg_data["parentID"] = last_user_msg_id
        msg_data["mode"] = "build"
        msg_data["agent"] = "build"
        msg_data["path"] = {"cwd": str(self.target_cwd), "root": "/"}
        msg_data["cost"] = 0
        msg_data["tokens"] = {
            "total": 0,
            "input": 0,
            "output": 0,
            "reasoning": 0,
            "cache": {"write": 0, "read": 0},
        }
        msg_data["finish"] = finish_reason
        if model:
            pid, mid = _split_model(model)
            msg_data["modelID"] = mid
            msg_data["providerID"] = pid
        msg_data["time"]["completed"] = effective_ts
        return msg_data

    def _append_user_part(
        self,
        content: str,
        sid: str,
        msg_id: str,
        effective_ts: int,
        parts: list[dict[str, Any]],
        part_idx: int,
    ) -> int:
        """追加 user turn 的正文 text part，返回下一 part 序号。"""
        part_id = f"prt_rc_{sid[:8]}_{part_idx:04d}"
        user_text = self._rewrite_prompt_paths(content)
        parts.append(
            _part_row(
                part_id,
                msg_id,
                sid,
                effective_ts,
                effective_ts,
                {"type": "text", "text": user_text},
            )
        )
        return part_idx + 1

    def _build_session_row(
        self,
        meta: SessionMeta,
        sid: str,
    ) -> dict[str, Any]:
        """构造 opencode.db 的 ``session`` 行。

        parent_id 置空（顶层 session）；directory 覆盖到 target_cwd；
        title 加 ``[reconstructed]`` 前缀。必填列填值。
        """
        now_ms = int(_now_ms())
        # slug 用 sid 前缀保证唯一
        slug = f"reconstructed-{sid[:12]}"
        return {
            "id": sid,
            "project_id": self._project_id,
            "parent_id": None,
            "slug": slug,
            "directory": str(self.target_cwd),
            "title": f"[reconstructed] {meta.initial_prompt[:60]}"
            if meta.initial_prompt
            else "[reconstructed]",
            "version": "0.0.1-reconstructed",
            "share_url": None,
            "summary_additions": 0,
            "summary_deletions": 0,
            "summary_files": 0,
            "summary_diffs": None,
            "revert": None,
            "permission": None,
            "time_created": now_ms,
            "time_updated": now_ms,
            "time_compacting": None,
            "time_archived": None,
            "workspace_id": None,
            "path": "",
            "agent": "build",
            "model": self._format_model_json(meta.model),
            "cost": 0,
            "tokens_input": 0,
            "tokens_output": 0,
            "tokens_reasoning": 0,
            "tokens_cache_read": 0,
            "tokens_cache_write": 0,
            "metadata": None,
        }

    # ── 路径重写 ─────────────────────────────────────────────────────

    def _build_assistant_parts(
        self,
        content: str,
        tool_call_entries: list[TimelineEntry],
        sid: str,
        msg_id: str,
        turn_ts: int,
        part_idx_start: int,
        bridge_map: dict[str, str],
        task_part_ids: dict[str, str],
        finish_reason: str = "",
    ) -> tuple[list[dict[str, Any]], int]:
        """构造 assistant turn 的 parts：step-start + reasoning + text + tools + step-finish。

        opencode 的 --session resume 需要 step-start/step-finish 界定每个 LLM
        step 边界，缺它们 resume 会静默失败。
        """
        rows, idx = _pre_tool_parts(self, content, sid, msg_id, turn_ts, part_idx_start)
        parts: list[dict[str, Any]] = list(rows)

        # 每个 ToolCall → 一个 tool part
        for e in tool_call_entries:
            part_id = f"prt_rc_{sid[:8]}_{idx:04d}"
            idx += 1
            row, sub_sid = _build_tool_part_row(self, e, sid, msg_id, part_id, bridge_map)
            parts.append(row)
            # 记录 task part id
            if sub_sid:
                task_part_ids[sub_sid] = part_id

        # step-finish part，time_created 须 >= 所有 tool part
        reason = finish_reason or "stop"
        part_id = f"prt_rc_{sid[:8]}_{idx:04d}"
        idx += 1
        parts.append(
            _step_finish_part(part_id, msg_id, sid, _max_part_time_created(parts, turn_ts), reason)
        )
        return parts, idx - part_idx_start

    def _rewrite_prompt_paths(self, text: str) -> str:
        """重写文本内嵌的 origin_cwd 绝对路径 → target_cwd。

        task 工具 prompt / 工具参数 / 正文都可能内嵌工作目录绝对路径，
        转写时须替换为 target_cwd，否则 fork resume 时会读写原目录。
        负向前瞻防 ``/foo/bar`` 误匹配 ``/foo/barbaz``。
        """
        # origin_cwd 为空时跳过，否则正则匹配每个位置导致文本被插入 target_cwd
        if not text or not self.origin_cwd or self.origin_cwd == str(self.target_cwd):
            return text
        pattern = re.escape(self.origin_cwd) + r"(?![a-zA-Z0-9_.-])"
        return re.sub(pattern, str(self.target_cwd), text)

    # ── 写入 ──────────────────────────────────────────────────────────

    def _rewrite_obj_paths(self, obj: Any) -> Any:
        """递归重写 JSON 树中的 origin_cwd 路径 → target_cwd。

        覆盖所有字符串值（bash.command / edit.filePath / read.filePath 等），
        防止 worker 读写原目录（fs 未隔离 origin cwd）。
        """
        if obj is None or not self.origin_cwd or self.origin_cwd == str(self.target_cwd):
            return obj
        if isinstance(obj, str):
            return self._rewrite_prompt_paths(obj)
        if isinstance(obj, dict):
            for k, v in list(obj.items()):
                obj[k] = self._rewrite_obj_paths(v)
            return obj
        if isinstance(obj, list):
            for i, v in enumerate(obj):
                obj[i] = self._rewrite_obj_paths(v)
            return obj
        return obj

    def _remove_partial_dest(self) -> None:
        """清理半成品 fork db（含 ``-wal``/``-shm``）。"""
        for suffix in ("", "-wal", "-shm"):
            p = Path(f"{self.dest_opencode_db}{suffix}")
            try:
                p.unlink(missing_ok=True)
            except OSError:
                logger.debug("transcribe: cleanup partial %s failed (non-fatal)", p)

    def _write_rows(
        self,
        session_row: dict[str, Any],
        messages: list[dict[str, Any]],
        parts: list[dict[str, Any]],
    ) -> None:
        """把 session/message/part 行写入 fork opencode.db。

        列名动态发现（``PRAGMA table_info``），按 INSERT OR REPLACE 写入。
        先 upsert 占位 ``project`` 行。
        """
        try:
            con = sqlite3.connect(str(self.dest_opencode_db), timeout=BUSY_TIMEOUT_MS / 1000)
            con.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        except sqlite3.Error as exc:
            raise RuntimeError(f"failed to open fork opencode.db: {exc}") from exc
        try:
            self._ensure_project_row(con)
            self._insert_row(con, "session", session_row)
            for m in messages:
                self._insert_row(con, "message", m)
            for p in parts:
                self._insert_row(con, "part", p)
            con.commit()
        except sqlite3.Error as exc:
            with contextlib.suppress(sqlite3.Error):
                con.rollback()
            raise RuntimeError(f"failed to write rows to fork opencode.db: {exc}") from exc
        finally:
            con.close()

    def _ensure_project_row(self, con: sqlite3.Connection) -> None:
        """upsert 占位 project 行（让 session.project_id FK 有效）。"""
        # 检查 project 表是否存在
        try:
            cols = [c[1] for c in con.execute("PRAGMA table_info(project)").fetchall()]
        except sqlite3.Error:
            return
        if not cols:
            return
        now_ms = int(_now_ms())
        # worktree / time_created / time_updated 在真实 schema 是 NOT NULL
        row: dict[str, Any] = {
            "id": self._project_id,
            "worktree": str(self.target_cwd),
            "vcs": None,
            "name": f"[reconstructed] {self.target_cwd.name}",
            "icon_url": None,
            "icon_color": None,
            "time_created": now_ms,
            "time_updated": now_ms,
            "time_initialized": None,
            "sandboxes": "[]",
            "commands": None,
            "icon_url_override": None,
        }
        self._insert_row(con, "project", row)


def _now_ms() -> float:
    """当前时间毫秒 epoch。"""
    from datetime import UTC, datetime

    return datetime.now(UTC).timestamp() * 1000


def _group_main_entries(
    timeline: list[TimelineEntry],
) -> tuple[list[TimelineEntry], dict[str, list[TimelineEntry]]]:
    """取主 agent entries，按「turn → 其 tool_calls」分组。"""
    main_entries = [e for e in timeline if e.role == "main"]
    main_turns = [e for e in main_entries if e.kind == "turn" and e.turn is not None]
    main_tcs_by_turn: dict[str, list[TimelineEntry]] = {}
    for e in main_entries:
        if e.kind == "tool_call" and e.tool_call is not None:
            tid = str(e.tool_call.get("turnId") or "")
            main_tcs_by_turn.setdefault(tid, []).append(e)
    return main_turns, main_tcs_by_turn


def _message_row(
    msg_id: str,
    sid: str,
    effective_ts: int,
    msg_data: dict[str, Any],
) -> dict[str, Any]:
    """构造 message 表行。"""
    return {
        "id": msg_id,
        "session_id": sid,
        "time_created": effective_ts,
        "time_updated": effective_ts,
        "data": json.dumps(msg_data, ensure_ascii=False),
    }


def _part_row(
    part_id: str,
    msg_id: str,
    sid: str,
    time_created: int,
    time_updated: int,
    data: dict[str, Any],
) -> dict[str, Any]:
    """构造 part 表行；``data`` dict 序列化为 JSON 文本存储。"""
    return {
        "id": part_id,
        "message_id": msg_id,
        "session_id": sid,
        "time_created": time_created,
        "time_updated": time_updated,
        "data": json.dumps(data, ensure_ascii=False),
    }


def _parse_thinking(content: str) -> tuple[list[str], str]:
    """解析 <thinking> 块，返回 (reasoning 文本列表, 去块后的正文)。"""
    thinking_texts: list[str] = []
    for m in _THINKING_RE.finditer(content):
        txt = m.group("text").strip()
        if txt:
            thinking_texts.append(txt)
    # 正文 = 去掉 thinking 块后的剩余
    return thinking_texts, _THINKING_RE.sub("", content).strip()


def _pre_tool_parts(
    tr: L2Transcriber,
    content: str,
    sid: str,
    msg_id: str,
    turn_ts: int,
    idx: int,
) -> tuple[list[dict[str, Any]], int]:
    """构造 step-start + reasoning + 正文 text parts，返回 (rows, 下一序号)。"""
    rows: list[dict[str, Any]] = []
    part_id = f"prt_rc_{sid[:8]}_{idx:04d}"
    idx += 1
    rows.append(_part_row(part_id, msg_id, sid, turn_ts, turn_ts, {"type": "step-start"}))
    thinking_texts, body = _parse_thinking(content)
    for ttext in thinking_texts:
        part_id = f"prt_rc_{sid[:8]}_{idx:04d}"
        idx += 1
        ttext = tr.rewrite_prompt_paths(ttext)
        rows.append(
            _part_row(part_id, msg_id, sid, turn_ts, turn_ts, {"type": "reasoning", "text": ttext})
        )
    # 正文 text part（若非空）
    if body:
        part_id = f"prt_rc_{sid[:8]}_{idx:04d}"
        idx += 1
        body = tr.rewrite_prompt_paths(body)
        rows.append(
            _part_row(part_id, msg_id, sid, turn_ts, turn_ts, {"type": "text", "text": body})
        )
    return rows, idx


def _parse_tool_args(tr: L2Transcriber, args_raw: str) -> Any:
    """解析 argsJson；dict 形态递归重写其中的 origin_cwd 路径。"""
    args_obj: Any
    try:
        args_obj = json.loads(args_raw) if args_raw else {}
    except (json.JSONDecodeError, TypeError):
        args_obj = {"_raw": args_raw}
    # Rewrite origin_cwd paths in all tool inputs (recursively), so worker
    # doesn't read/write the origin directory.
    if isinstance(args_obj, dict):
        tr.rewrite_obj_paths(args_obj)
    return args_obj


def _tool_part_data(
    tool_name: str,
    call_id: str,
    status: str,
    args_obj: Any,
    output_str: str,
    start_ms: int,
    end_ms: int,
) -> dict[str, Any]:
    """构造 tool part 的 data JSON（state.input/output/time）。"""
    return {
        "type": "tool",
        "tool": tool_name,
        "callID": call_id,
        "state": {
            "status": status,
            "input": args_obj,
            "output": output_str,
            "time": {"start": start_ms, "end": end_ms},
        },
    }


def _build_tool_part_row(
    tr: L2Transcriber,
    e: TimelineEntry,
    sid: str,
    msg_id: str,
    part_id: str,
    bridge_map: dict[str, str],
) -> tuple[dict[str, Any], str]:
    """构造单个 tool part 行；task 工具另返回子会话 id（供 task_part_ids 登记）。"""
    tc = e.tool_call
    if tc is None:
        raise RuntimeError("tool_call 条目缺少 tool_call 数据")
    tool_name = str(tc.get("toolName") or "")
    call_id = str(tc.get("toolCallId") or "")
    args_obj = _parse_tool_args(tr, str(tc.get("argsJson") or ""))
    # state.status: 'completed' | 'error'
    state_str = str(tc.get("state") or "completed")
    status = "error" if state_str in ("error", "failed") else "completed"
    # task tool：state.output 须含子 session id（find_main_part_id_for_subagent 的依据）
    sub_sid = ""
    if tool_name == "task":
        sub_sid = _lookup_bridge_sub(bridge_map, tc) or _extract_subagent_sid_from_args(tc)
    result_raw = str(tc.get("resultJson") or "")
    output_str = tr.rewrite_prompt_paths(result_raw)
    if sub_sid and sub_sid not in output_str:
        output_str = f"task_id: {sub_sid}\n{output_str}" if result_raw else f"task_id: {sub_sid}"
    start_ms = _iso_to_ms(tc.get("startedAt")) or e.ts
    end_ms = _iso_to_ms(tc.get("completedAt")) or e.ts
    data = _tool_part_data(tool_name, call_id, status, args_obj, output_str, start_ms, end_ms)
    return _part_row(part_id, msg_id, sid, start_ms, end_ms, data), sub_sid


def _max_part_time_created(parts: list[dict[str, Any]], floor_ts: int) -> int:
    """取 parts 里最大 time_created（不低于 floor_ts）。"""
    ts = floor_ts
    for p in parts:
        tc_val = p.get("time_created")
        if tc_val is not None and tc_val > ts:
            ts = tc_val
    return ts


def _step_finish_part(
    part_id: str,
    msg_id: str,
    sid: str,
    step_finish_ts: int,
    reason: str,
) -> dict[str, Any]:
    """构造 step-finish part 行。"""
    return _part_row(
        part_id,
        msg_id,
        sid,
        step_finish_ts,
        step_finish_ts,
        {
            "reason": reason,
            "type": "step-finish",
            "tokens": {
                "total": 0,
                "input": 0,
                "output": 0,
                "reasoning": 0,
                "cache": {"write": 0, "read": 0},
            },
            "cost": 0,
        },
    )
