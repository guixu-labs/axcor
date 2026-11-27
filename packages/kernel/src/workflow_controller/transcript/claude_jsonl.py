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

"""claude-jsonl 读取轨：读 Claude Code 会话 .jsonl 文件产出 RawInteraction。

处理 assistant 行按 message.id 分组、tool_result 归并、usage 合并与技能
注入识别。
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC
from pathlib import Path
from typing import Any, NamedTuple

from .types import RawInteraction, TokenUsage, ToolCallInfo

logger = logging.getLogger(__name__)


def _is_valid_iso(ts: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", ts))


def _extract_text_content(content) -> str | None:
    if not content:
        return None
    if isinstance(content, str):
        return content
    parts: list[str] = []
    for block in content:
        if block.get("type") == "text" and block.get("text"):
            parts.append(block["text"])
        elif block.get("type") == "thinking" and block.get("thinking"):
            parts.append(f"<thinking>{block['thinking']}</thinking>")
    return "\n".join(parts) if parts else None


def _extract_tool_calls(content: list) -> list[ToolCallInfo] | None:
    tool_use = [b for b in content if b.get("type") == "tool_use"]
    if not tool_use:
        return None
    result_map: dict[str, str] = {}
    for r in content:
        if r.get("type") == "tool_result" and r.get("tool_use_id") and r.get("content"):
            val = r["content"]
            result_map[r["tool_use_id"]] = val if isinstance(val, str) else json.dumps(val)
    return [
        ToolCallInfo(
            tool_call_id=b.get("id", ""),
            tool_name=b.get("name", ""),
            args_json=json.dumps(b["input"]) if b.get("input") else None,
            result_json=result_map.get(b.get("id", "")),
            state="completed",
        )
        for b in tool_use
    ]


def _parse_jsonl(file_path: str) -> list[dict]:
    try:
        content = Path(file_path).read_text(encoding="utf-8")
        if not content.strip():
            return []
        return [
            json.loads(line)
            for line in content.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
    except Exception:
        return []


def _collect_block_results(content: list[dict[str, Any]], result_map: dict[str, str]) -> None:
    """content 块里的 tool_result 就地归并进 result_map。"""
    for block in content:
        if block.get("type") == "tool_result" and block.get("tool_use_id") and block.get("content"):
            val = block["content"]
            result_map[block["tool_use_id"]] = val if isinstance(val, str) else json.dumps(val)


def _collect_tool_results(lines: list[dict]) -> dict[str, str]:
    result_map: dict[str, str] = {}
    for line in lines:
        content = line.get("message", {}).get("content")
        if line.get("type") == "user" and isinstance(content, list):
            _collect_block_results(content, result_map)
    return result_map


def _group_assistant_lines(lines: list[dict]) -> list[tuple[int, list[dict]]]:
    """Group consecutive assistant lines that share the same message.id."""
    non_breaking = {"summary", "system"}
    groups: list[tuple[int, list[dict]]] = []
    current: list[dict] = []
    current_start: int = -1
    current_ids: set[str] = set()
    pending_results: list[dict] = []

    for i, line in enumerate(lines):
        is_tool_result_user_line = (
            current
            and line.get("type") == "user"
            and line.get("message", {}).get("content")
            and isinstance(line["message"]["content"], list)
            and _extract_text_content(line["message"]["content"]) is None
        )
        if line.get("type") == "assistant" and line.get("message"):
            msg_id = line["message"].get("id")
            is_repeated_message = bool(
                current and pending_results and msg_id and msg_id in current_ids
            )
            if is_repeated_message:
                pending_results = []
            elif pending_results:
                groups.append((current_start, current))
                current = []
                current_ids = set()
                pending_results = []

            if not current:
                current_start = i
            current.append(line)
            if msg_id:
                current_ids.add(msg_id)
        elif line.get("type") in non_breaking:
            continue
        elif is_tool_result_user_line:
            pending_results.append(line)
            continue
        else:
            if current:
                groups.append((current_start, current))
                current = []
                current_ids = set()
                pending_results = []

    if current:
        groups.append((current_start, current))
    return groups


def _line_timestamp(line: dict[str, Any], file_mtime: float, i: int) -> tuple[str, float]:
    """非 assistant 行的 (timestamp, created_ms)：无有效时间戳按文件 mtime + 行号推算。"""
    from datetime import datetime

    real_ts = line.get("timestamp")
    timestamp = real_ts if (real_ts and _is_valid_iso(real_ts)) else None
    time_created = None
    if real_ts and _is_valid_iso(real_ts):
        time_created = datetime.fromisoformat(real_ts).timestamp() * 1000
    if time_created is None:
        time_created = file_mtime + i * 1000
    if timestamp is None:
        timestamp = datetime.fromtimestamp(time_created / 1000, tz=UTC).isoformat()
    return timestamp, time_created


def _non_assistant_interaction(
    line: dict[str, Any], timestamp: str, time_created: float
) -> RawInteraction | None:
    """user/result 行 → RawInteraction；无正文或其余类型返回 None。"""
    if line.get("type") == "user" and line.get("message"):
        content = _extract_text_content(line["message"].get("content"))
        if not content:
            return None
        is_skill_inj = (
            "Base directory for this skill" in content
            or "<skill-format>" in content
            or content.startswith("Launching skill:")
            or content.startswith("Launching skill ")
        )
        return RawInteraction(
            role="system" if is_skill_inj else "user",
            content=content,
            timestamp=timestamp,
            time_info={"created": int(time_created)},
        )
    if line.get("type") == "result":
        cost = line.get("cost_usd", 0)
        result_usage = TokenUsage(cost=cost) if cost else None
        return RawInteraction(
            role="result",
            content=line.get("result"),
            timestamp=timestamp,
            time_info={"created": int(time_created)},
            usage=result_usage,
            latency=line.get("duration_ms"),
            finish_reason=line.get("subtype"),
        )
    return None


def _infer_latencies(result: list[RawInteraction]) -> None:
    """按相邻 created 间隔补 latency/completed（无显式 latency 的行）。"""
    for i, r in enumerate(result):
        if r.latency is None and r.time_info and r.time_info.get("created"):
            next_ts = (
                result[i + 1].time_info.get("created")
                if i + 1 < len(result) and result[i + 1].time_info
                else None
            )
            if next_ts and next_ts > r.time_info["created"]:
                r.latency = next_ts - r.time_info["created"]
                r.time_info["completed"] = next_ts


def read_session(file_path: str, session_id: str) -> list[RawInteraction]:
    """Read a claude-code .jsonl session file → RawInteraction[]."""
    if not file_path or not Path(file_path).exists():
        return []

    lines = _parse_jsonl(file_path)
    if not lines:
        return []

    all_tool_results = _collect_tool_results(lines)
    file_mtime = Path(file_path).stat().st_mtime * 1000
    groups = _group_assistant_lines(lines)

    # Build line-index → group mapping
    line_to_group: dict[int, int] = {}
    for gi, (start, group_lines) in enumerate(groups):
        for j in range(len(group_lines)):
            line_to_group[start + j] = gi
    emitted: set[int] = set()

    result: list[RawInteraction] = []
    for i, line in enumerate(lines):
        gi = line_to_group.get(i)
        if gi is not None:
            if gi in emitted:
                continue
            emitted.add(gi)
            start, group_lines = groups[gi]
            interaction = _merge_assistant_group(group_lines, all_tool_results, file_mtime, i)
            if interaction:
                result.append(interaction)
            continue

        timestamp, time_created = _line_timestamp(line, file_mtime, i)
        interaction = _non_assistant_interaction(line, timestamp, time_created)
        if interaction is not None:
            result.append(interaction)

    # Infer latencies from gaps
    _infer_latencies(result)
    return result


class _GroupScan(NamedTuple):
    """assistant 行组扫描结果（_scan_group_lines 产出）。"""

    text_parts: list[str]
    all_tool_calls: list[ToolCallInfo]
    merged_model: str | None
    first_ts: str | None
    first_created: float | None
    last_created: float | None
    merged_finish: str | None
    explicit_duration: float | None
    merged_usage: TokenUsage | None


def _line_blocks(
    content_blocks: list[Any], all_tool_results: dict[str, str]
) -> tuple[str | None, list[ToolCallInfo]]:
    """单行 content 块 → (文本, 补全结果后的工具调用列表)。"""
    text = _extract_text_content(content_blocks)
    tcs: list[ToolCallInfo] = []
    for tc in _extract_tool_calls(content_blocks) or []:
        v = all_tool_results.get(tc.tool_call_id) or tc.result_json
        tcs.append(
            ToolCallInfo(
                tool_call_id=tc.tool_call_id,
                tool_name=tc.tool_name,
                args_json=tc.args_json,
                result_json=v if isinstance(v, str) else (json.dumps(v) if v else None),
                state=tc.state,
            )
        )
    return text, tcs


def _fold_usage_line(
    usage: dict[str, Any], max_raw_input: int, final_usage_line: dict[str, Any] | None
) -> tuple[int, dict[str, Any] | None]:
    """usage 行归并：刷新最大原始 input，判定是否替换终选 usage 行。"""
    if usage.get("input_tokens", 0) > max_raw_input:
        max_raw_input = usage["input_tokens"]
    should_replace = (
        usage.get("cache_read_input_tokens") is not None
        or usage.get("cache_creation_input_tokens") is not None
        or (
            usage.get("output_tokens", 0) > 0
            and usage.get("input_tokens", 0)
            < (final_usage_line or {}).get("input_tokens", float("inf"))
        )
    )
    if should_replace:
        final_usage_line = usage
    return max_raw_input, final_usage_line


def _line_created(
    al: dict[str, Any], file_mtime: float, start_idx: int, li: int
) -> tuple[float, str]:
    """单行 (created_ms, 时间戳)：无效时间戳按文件 mtime + 行号推算。"""
    from datetime import datetime

    real_ts = al.get("timestamp")
    if real_ts and _is_valid_iso(real_ts):
        tc_created = datetime.fromisoformat(real_ts).timestamp() * 1000
        return tc_created, real_ts
    tc_created = file_mtime + (start_idx + li) * 1000
    return tc_created, datetime.fromtimestamp(tc_created / 1000, tz=UTC).isoformat()


def _fold_created(
    first_created: float | None,
    first_ts: str | None,
    last_created: float | None,
    tc_created: float,
    ts: str,
) -> tuple[float | None, str | None, float | None]:
    """按当前行刷新组内最早/最晚时间（最早行携带其时间戳）。"""
    if first_created is None or tc_created < first_created:
        first_created = tc_created
        first_ts = ts
    if last_created is None or tc_created > last_created:
        last_created = tc_created
    return first_created, first_ts, last_created


def _fold_tail_meta(
    al: dict[str, Any],
    merged_model: str | None,
    merged_finish: str | None,
    explicit_duration: float | None,
) -> tuple[str | None, str | None, float | None]:
    """刷新首个 model、最新 subtype 与显式 duration。"""
    if not merged_model and al.get("message", {}).get("model"):
        merged_model = al["message"]["model"]
    if al.get("subtype"):
        merged_finish = al["subtype"]
    if al.get("duration_ms") is not None:
        explicit_duration = al["duration_ms"]
    return merged_model, merged_finish, explicit_duration


def _final_usage(
    group_lines: list[dict[str, Any]],
    final_usage_line: dict[str, Any] | None,
    max_raw_input: int,
) -> TokenUsage | None:
    """终选 usage 行 → 合并 TokenUsage；无终选行回落组首行 usage。"""
    if final_usage_line:
        fu = final_usage_line
        inc_input = fu.get("input_tokens", 0)
        cache_read = fu.get("cache_read_input_tokens", 0)
        cache_write = fu.get("cache_creation_input_tokens", 0)
        input_msgs = max(max_raw_input, inc_input + cache_read + cache_write)
        return TokenUsage(
            total=input_msgs + fu.get("output_tokens", 0),
            input=inc_input,
            output=fu.get("output_tokens", 0),
            reasoning=0,
            cache_read=cache_read,
            cache_write=cache_write,
            cost=0,
            input_messages_tokens=input_msgs,
        )
    if group_lines[0].get("message", {}).get("usage"):
        u0 = group_lines[0]["message"]["usage"]
        return TokenUsage(
            total=(u0.get("input_tokens", 0)) + (u0.get("output_tokens", 0)),
            input=u0.get("input_tokens", 0),
            output=u0.get("output_tokens", 0),
        )
    return None


def _scan_group_lines(
    group_lines: list[dict[str, Any]],
    all_tool_results: dict[str, str],
    file_mtime: float,
    start_idx: int,
) -> _GroupScan:
    """扫 assistant 行组：文本/工具调用/usage 终选/首末时间/finish 归一。"""
    text_parts: list[str] = []
    all_tool_calls: list[ToolCallInfo] = []
    merged_model: str | None = None
    first_ts: str | None = None
    first_created: float | None = None
    last_created: float | None = None
    merged_finish: str | None = None
    explicit_duration: float | None = None
    max_raw_input = 0
    final_usage_line: dict | None = None

    for li, al in enumerate(group_lines):
        content_blocks = al.get("message", {}).get("content", [])
        if isinstance(content_blocks, list):
            text, tcs = _line_blocks(content_blocks, all_tool_results)
            if text:
                text_parts.append(text)
            all_tool_calls.extend(tcs)
        usage = al.get("message", {}).get("usage")
        if usage:
            max_raw_input, final_usage_line = _fold_usage_line(
                usage, max_raw_input, final_usage_line
            )
        tc_created, ts = _line_created(al, file_mtime, start_idx, li)
        first_created, first_ts, last_created = _fold_created(
            first_created, first_ts, last_created, tc_created, ts
        )
        merged_model, merged_finish, explicit_duration = _fold_tail_meta(
            al, merged_model, merged_finish, explicit_duration
        )

    return _GroupScan(
        text_parts=text_parts,
        all_tool_calls=all_tool_calls,
        merged_model=merged_model,
        first_ts=first_ts,
        first_created=first_created,
        last_created=last_created,
        merged_finish=merged_finish,
        explicit_duration=explicit_duration,
        merged_usage=_final_usage(group_lines, final_usage_line, max_raw_input),
    )


def _skill_invocation_from_read(tc: ToolCallInfo) -> ToolCallInfo | None:
    """Read SKILL.md 的 tool_use → 合成的 skill/<name> 调用；其余返回 None。"""
    if tc.tool_name != "Read" or not tc.args_json:
        return None
    try:
        args = json.loads(tc.args_json)
        fp = str(args.get("file_path", ""))
        if not fp.endswith("/SKILL.md"):
            return None
        parts = fp.split("/")
        skill_name = parts[-2] if len(parts) >= 2 else ""
        return ToolCallInfo(
            tool_call_id=f"skill-{tc.tool_call_id}",
            tool_name=f"skill/{skill_name}",
            args_json=json.dumps({"skill": skill_name, "file_path": fp}),
            state="completed",
        )
    except Exception:
        logger.debug("skill 调用识别失败（toolCall %s），跳过", tc.tool_call_id)
        return None


def _detect_skill_invocations(all_tool_calls: list[ToolCallInfo]) -> list[ToolCallInfo]:
    """从 Read SKILL.md 的 tool_use 识别技能调用并合成 skill/<name> 调用。"""
    skill_invs: list[ToolCallInfo] = []
    for tc in all_tool_calls:
        inv = _skill_invocation_from_read(tc)
        if inv is not None:
            skill_invs.append(inv)
    return skill_invs


def _merge_assistant_group(
    group_lines: list[dict],
    all_tool_results: dict[str, str],
    file_mtime: float,
    start_idx: int,
) -> RawInteraction | None:
    scan = _scan_group_lines(group_lines, all_tool_results, file_mtime, start_idx)
    skill_invs = _detect_skill_invocations(scan.all_tool_calls)
    final_tcs = scan.all_tool_calls + skill_invs if skill_invs else scan.all_tool_calls

    merged_content = "\n".join(scan.text_parts) if scan.text_parts else None
    if not merged_content and not final_tcs:
        return None

    latency = scan.explicit_duration or (
        scan.last_created - scan.first_created if scan.last_created and scan.first_created else None
    )

    from datetime import datetime

    return RawInteraction(
        role="assistant",
        content=merged_content,
        timestamp=scan.first_ts or datetime.fromtimestamp(file_mtime / 1000, tz=UTC).isoformat(),
        time_info={
            "created": int(scan.first_created or file_mtime),
            **({"completed": int(scan.last_created)} if scan.last_created is not None else {}),
        },
        tool_calls=final_tcs or None,
        usage=scan.merged_usage,
        model=scan.merged_model,
        latency=latency,
        finish_reason=scan.merged_finish,
    )


def list_subagent_sessions(file_path: str, session_id: str) -> list[tuple[str, str]]:
    """Find subagent .jsonl files at <parentDir>/<sessionId>/subagents/."""
    parent = Path(file_path).parent if Path(file_path).is_file() else Path(file_path)
    sub_dir = parent / session_id / "subagents"
    if not sub_dir.is_dir():
        return []
    return [(f.stem, str(f)) for f in sorted(sub_dir.glob("*.jsonl"))]


def collect_tool_use_mappings(file_path: str, session_id: str) -> dict[str, str]:
    """从 subagent meta.json 建 toolUseId → subagentSessionId 映射（文件名即会话 id）。"""
    parent = Path(file_path).parent if Path(file_path).is_file() else Path(file_path)
    sub_dir = parent / session_id / "subagents"
    mapping: dict[str, str] = {}
    if not sub_dir.is_dir():
        return mapping
    for meta_file in sub_dir.glob("*.meta.json"):
        try:
            data = json.loads(meta_file.read_text(encoding="utf-8"))
            tool_use_id = data.get("toolUseId")
            subagent_sid = meta_file.stem  # filename without .meta.json
            if tool_use_id and subagent_sid:
                mapping[tool_use_id] = subagent_sid
        except Exception:
            logger.debug("subagent meta 文件 %s 解析失败，跳过", meta_file.name)
    return mapping
