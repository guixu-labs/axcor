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

"""Claude 系 transcript 的重放种子（自插件 claude_session_seed 迁移提炼）。

按检查点时间戳定位最近的子代理派发调用作为截断边界，把前缀 JSONL 写进
fork 独立配置目录；另含源 runtime 探测（重放表单与前置校验共用）。
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from workflow_controller.core.restore.path_rewrite import rewrite_paths
from workflow_controller.transcript.sources import (
    claude_project_key,
    resolve_claude_session_path,
)

__all__ = [
    "DEFAULT_MATCH_WINDOW_MARGIN",
    "detect_replay_runtime",
    "seed_claude_session",
    "source_transcript_path",
]

# 检查点 created_at 在快照完成后才写入，比触发它的派发调用晚出「快照耗时 +
# LLM 延迟」。窗口只作拒绝明显无关的更早同型调用的宽松护栏，精确边界由
# 顺序约束 + 取最近一条保证。
DEFAULT_MATCH_WINDOW_MARGIN = 30.0


def source_transcript_path(
    source_session_id: str,
    source_cwd: str,
    source_config_dir: str | None = None,
) -> Path:
    """解析源 transcript JSONL 路径（配置目录可覆盖，缺省回落 ``~/.claude``）。"""
    env = {"CLAUDE_CONFIG_DIR": source_config_dir} if source_config_dir else None
    found = resolve_claude_session_path(source_session_id, source_cwd, env)
    if found is not None:
        return found
    config = Path(
        source_config_dir or os.environ.get("CLAUDE_CONFIG_DIR") or "~/.claude",
    ).expanduser()
    return config / "projects" / claude_project_key(source_cwd) / f"{source_session_id}.jsonl"


def detect_replay_runtime(
    *,
    sdk_session_id: str,
    cwd: str,
    configured_runtime: str | None,
    default_runtime: str,
    source_config_dir: str | None = None,
) -> str:
    """探测源会话的 runtime：显式配置 > id 形态 > transcript 命中 > 默认。"""
    if configured_runtime:
        return configured_runtime
    if sdk_session_id.startswith("ses_"):
        return "opencode"
    if source_transcript_path(sdk_session_id, cwd, source_config_dir).is_file():
        return "claude_cli"
    # transcript 未命中：可能是被移走的 claude_cli 会话，也可能是继承默认
    # runtime 的会话——按默认兜底让上层报出对应错误，不误标。
    return default_runtime if default_runtime else "claude_cli"


def _is_agent_dispatch(block: dict, agent_type: str) -> bool:
    """是否为派发到该类型子代理的工具调用块（工具名/类型键宽容匹配）。"""
    if not isinstance(block, dict) or block.get("type") != "tool_use":
        return False
    if (block.get("name") or "").lower() not in ("agent", "task"):
        return False
    tool_input = block.get("input") or {}
    if not isinstance(tool_input, dict):
        return False
    subagent_type = (
        tool_input.get("subagent_type") or tool_input.get("mode") or tool_input.get("subagent_name")
    )
    return subagent_type == agent_type


def _load_transcript_rows(source: Path) -> tuple[list[dict[str, Any]], str]:
    """读源 JSONL 为行列表；坏行（崩溃半写）及其后丢弃；空/不可读报错。"""
    if not source.is_file():
        return [], f"Claude CLI source transcript not found: {source}"
    try:
        raw_lines = source.read_text().split("\n")
    except OSError as exc:
        return [], f"Cannot read Claude CLI source transcript: {exc}"
    rows: list[dict[str, Any]] = []
    for line in raw_lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            break
    if not rows:
        return [], "Claude CLI source transcript is empty"
    return rows, ""


def _find_agent_dispatch_cutoff(
    rows: list[dict[str, Any]],
    checkpoint_at: str,
    agent_type: str,
    snapshot_timeout_seconds: float,
) -> tuple[int, str]:
    """定位检查点前最近的该类型派发调用行号（截断点）。"""
    try:
        local_tz = datetime.now(tz=UTC).astimezone().tzinfo
        checkpoint_time = datetime.fromisoformat(checkpoint_at.replace("Z", "+00:00"))
        if checkpoint_time.tzinfo is None:
            checkpoint_time = checkpoint_time.replace(tzinfo=local_tz)
    except ValueError:
        return 0, f"Invalid checkpoint timestamp: {checkpoint_at}"
    match_window = timedelta(seconds=snapshot_timeout_seconds + DEFAULT_MATCH_WINDOW_MARGIN)
    candidates: list[tuple[datetime, int]] = []
    for index, row in enumerate(rows):
        if row.get("type") != "assistant":
            continue
        content = (row.get("message") or {}).get("content") or []
        if not isinstance(content, list) or not any(
            _is_agent_dispatch(block, agent_type) for block in content
        ):
            continue
        try:
            at = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
            if at.tzinfo is None:
                at = at.replace(tzinfo=local_tz)
        except (KeyError, ValueError):
            continue
        if at <= checkpoint_time and checkpoint_time - at <= match_window:
            candidates.append((at, index))
    if not candidates:
        return 0, (
            f"Claude CLI Agent call for {agent_type!r} was not found "
            f"before checkpoint {checkpoint_at}"
        )
    return max(candidates)[1], ""


def _copy_cli_config_files(source_config: Path, fork_config: Path) -> None:
    """把 CLI 配置/凭证文件（存在才拷）拷进 fork 配置目录。"""
    fork_config.mkdir(parents=True, exist_ok=True)
    for name in ("settings.json", "settings.local.json", ".credentials.json"):
        source_file = source_config / name
        if source_file.is_file():
            shutil.copy2(source_file, fork_config / name)


def _write_fork_transcript(
    target: Path,
    rows: list[dict[str, Any]],
    source_cwd: str,
    fork_cwd: Path,
    new_session_id: str,
) -> None:
    """写 fork transcript JSONL（逐行改 sessionId + 路径重写）。"""
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as out:
        for row in rows:
            row["sessionId"] = new_session_id
            rewrite_paths(row, source_cwd, str(fork_cwd))
            out.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def seed_claude_session(
    *,
    source_session_id: str,
    source_cwd: str,
    new_session_id: str,
    fork_cwd: Path,
    checkpoint_at: str,
    agent_type: str,
    final_checkpoint: bool,
    source_config_dir: str | None = None,
    snapshot_timeout_seconds: float = 30.0,
) -> tuple[Path | None, str]:
    """把主 JSONL 前缀拷进 fork 局部 ``CLAUDE_CONFIG_DIR``，返回配置目录。

    检查点前最近的派发调用是截断边界，其类型与时间须与检查点吻合——迟到
    或无关的调用不会静默产出错误的重放历史。终态检查点保留完整对话。
    """
    source = source_transcript_path(
        source_session_id,
        source_cwd,
        source_config_dir,
    )
    rows, load_error = _load_transcript_rows(source)
    if load_error:
        return None, load_error
    source_config = source.parent.parent.parent
    cutoff = len(rows)
    if not final_checkpoint:
        cutoff, cutoff_error = _find_agent_dispatch_cutoff(
            rows,
            checkpoint_at,
            agent_type,
            snapshot_timeout_seconds,
        )
        if cutoff_error:
            return None, cutoff_error
    if not any(row.get("type") == "user" for row in rows[:cutoff]):
        return None, "Claude CLI transcript has no user message before checkpoint"
    fork_config = fork_cwd / ".workflow-controller" / "claude"
    target = (
        fork_config / "projects" / claude_project_key(str(fork_cwd)) / f"{new_session_id}.jsonl"
    )
    try:
        _copy_cli_config_files(source_config, fork_config)
        _write_fork_transcript(target, rows[:cutoff], source_cwd, fork_cwd, new_session_id)
    except (OSError, TypeError, ValueError) as exc:
        return None, f"Cannot seed Claude CLI transcript: {exc}"
    return fork_config, ""
