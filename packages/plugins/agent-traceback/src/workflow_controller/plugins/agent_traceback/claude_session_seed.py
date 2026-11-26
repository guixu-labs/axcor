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

"""Seed a private Claude CLI transcript at an agent traceback checkpoint."""

from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from workflow_controller.plugins.agent_traceback.l2_reader import _rewrite_paths
from workflow_controller.plugins.agent_traceback.snapshot_engine import (
    DEFAULT_SNAPSHOT_TIMEOUT_SECONDS,
)
from workflow_controller.transcript.sources import claude_project_key, resolve_claude_session_path

# 检查点 created_at 在 snapshot 完成后才写入（plugin.py），比触发它的 Agent 调用
# 时间戳晚出「快照耗时 + LLM 延迟」。匹配窗口取「实际生效的快照硬超时 + 固定余量」：
# 硬超时由插件配置 traceback.snapshot_timeout_seconds（默认 30s，见 config.yaml）决定，
# 经 seed_claude_session 的 snapshot_timeout_seconds 参数传入——不能锚定
# DEFAULT_SNAPSHOT_TIMEOUT_SECONDS，它只是 SnapshotEngine 构造器的兜底默认，在此不生效
# （用户把超时调大后窗口须同步放大，否则慢快照的首个检查点会全部匹配失败）。
# 该窗口只作「拒绝明显无关的更早同型 Agent 调用」的宽松护栏，不做精确边界——
# 精确边界由 `at <= cp_time` 的顺序约束 + 取最近一条（max）保证。
_CHECKPOINT_MATCH_WINDOW_MARGIN = 30.0


def _project_key(cwd: str) -> str:
    return claude_project_key(cwd)


def _is_agent_dispatch(block: dict, agent_type: str) -> bool:
    """是否为派发到 *agent_type* 的 subagent 工具调用块。

    与 context_bay_exporter 既有解析器（turn_split.py / bridge_builder.py）对齐：
    tool_use 名称大小写不敏感地接受 ``agent``/``task``，subagent 类型同时接受
    ``subagent_type``/``mode``/``subagent_name`` 键——历史 transcript 里存在用
    ``Task`` 工具名、``mode`` 键的 CLI 版本，严格匹配会让这些检查点重放全部失败。
    """
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


def source_transcript_path(
    source_session_id: str,
    source_cwd: str,
    source_config_dir: str | None = None,
) -> Path:
    env = {"CLAUDE_CONFIG_DIR": source_config_dir} if source_config_dir else None
    found = resolve_claude_session_path(source_session_id, source_cwd, env)
    if found is not None:
        return found
    config = Path(
        source_config_dir or os.environ.get("CLAUDE_CONFIG_DIR") or "~/.claude",
    ).expanduser()
    return config / "projects" / _project_key(source_cwd) / f"{source_session_id}.jsonl"


def detect_replay_runtime(
    *,
    native_session_id: str,
    cwd: str,
    configured_runtime: str | None,
    default_runtime: str,
    source_config_dir: str | None = None,
) -> str:
    if configured_runtime:
        return configured_runtime
    if native_session_id.startswith("ses_"):
        return "opencode"
    if source_transcript_path(native_session_id, cwd, source_config_dir).is_file():
        return "claude_cli"
    # 无 transcript 命中的会话：可能是 transcript 被删/移走的 claude_cli 会话，也可能
    # 本就是继承全局默认 runtime 的会话（默认 claude_sdk，其 session id 同为 UUID 形态）。
    # 按继承的默认 runtime 兜底——默认是 claude_cli 时维持原来的「transcript not found」
    # 报错；是 claude_sdk 等其它 runtime 时让上层报出「不支持该来源会话」，避免把
    # claude_sdk 会话误标成 claude_cli。
    return default_runtime if default_runtime else "claude_cli"


def _load_transcript_rows(source: Path) -> tuple[list[dict[str, Any]], str]:
    """读源 JSONL 为行列表；空文件/不可读返回错误串。"""
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
            # CLI 崩溃可能留下半行的尾部记录（JSONL 逐行写入，坏行只会在尾部）；
            # 跳过坏行及其后，保留可用前缀，让 final-checkpoint 重放仍可用。
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
    """定位检查点前最近的 *agent_type* 派发调用行号（截断点）。"""
    try:
        local_tz = datetime.now(tz=UTC).astimezone().tzinfo
        cp_time = datetime.fromisoformat(checkpoint_at.replace("Z", "+00:00"))
        if cp_time.tzinfo is None:
            cp_time = cp_time.replace(tzinfo=local_tz)
    except ValueError:
        return 0, f"Invalid checkpoint timestamp: {checkpoint_at}"
    match_window = timedelta(seconds=snapshot_timeout_seconds + _CHECKPOINT_MATCH_WINDOW_MARGIN)
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
        if at <= cp_time and cp_time - at <= match_window:
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
    with target.open("w") as out:
        for row in rows:
            row["sessionId"] = new_session_id
            _rewrite_paths(row, source_cwd, str(fork_cwd))
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
    snapshot_timeout_seconds: float = DEFAULT_SNAPSHOT_TIMEOUT_SECONDS,
) -> tuple[Path | None, str]:
    """Copy the main JSONL prefix into a fork-local CLAUDE_CONFIG_DIR.

    The Agent call closest before the checkpoint is the truncation boundary.
    Its type and timestamp must agree with the checkpoint, so a delayed or
    unrelated call cannot silently produce the wrong replay history.

    ``snapshot_timeout_seconds`` 是实际生效的快照硬超时（插件配置），用于推导
    检查点时间戳与 Agent 调用之间的匹配窗口；仅匹配窗口宽度受其影响。
    """
    source = source_transcript_path(
        source_session_id,
        source_cwd,
        source_config_dir,
    )
    rows, load_err = _load_transcript_rows(source)
    if load_err:
        return None, load_err
    source_config = source.parent.parent.parent
    cutoff = len(rows)
    if not final_checkpoint:
        cutoff, cutoff_err = _find_agent_dispatch_cutoff(
            rows,
            checkpoint_at,
            agent_type,
            snapshot_timeout_seconds,
        )
        if cutoff_err:
            return None, cutoff_err
    if not any(row.get("type") == "user" for row in rows[:cutoff]):
        return None, "Claude CLI transcript has no user message before checkpoint"

    fork_config = fork_cwd / ".workflow-controller" / "claude"
    target = fork_config / "projects" / _project_key(str(fork_cwd)) / f"{new_session_id}.jsonl"
    try:
        _copy_cli_config_files(source_config, fork_config)
        _write_fork_transcript(target, rows[:cutoff], source_cwd, fork_cwd, new_session_id)
    except (OSError, TypeError, ValueError) as exc:
        return None, f"Cannot seed Claude CLI transcript: {exc}"
    return fork_config, ""
