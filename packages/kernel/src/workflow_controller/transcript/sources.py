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

"""会话转录源定位：按 runtime 解析源会话的转录落点。

claude 系 runtime 为原生 jsonl（~/.claude/projects/<cwd 编码>/
<native_session_id>.jsonl），codex 为 workspace rollout，opencode 为
opencode.db（定位优先级见 find_opencode_db）。
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)

_CLAUDE_RUNTIMES = frozenset({"claude_sdk", "claude_code", "claude_cli"})
_USER_HOME_ROOTS = (Path("/home"), Path("/Users"))


@dataclass(frozen=True)
class TranscriptSource:
    """定位到的转录源（framework 值供 meta.json 元数据使用）。"""

    framework: str  # "claude-code" | "codex" | "opencode"
    kind: str  # "claude-jsonl" | "codex-jsonl" | "opencode-db"
    path: Path


def claude_config_dirs(
    env: Mapping[str, str] | None = None,
    *,
    scan_user_homes: bool = False,
) -> list[Path]:
    """Find Claude config directories used by the CLI and local users.

    ``scan_user_homes`` 控制是否额外扫描 ``/home``、``/Users`` 下其他用户的
    ``~/.claude``——共享主机上这会暴露其他 Unix 用户的会话，默认关闭，须由部署方
    显式开启（见 ``AppConfig.claude_scan_user_homes``）。
    """
    merged = {**os.environ, **(env or {})}
    candidates = []
    claude_config_dir = merged.get("CLAUDE_CONFIG_DIR")
    if claude_config_dir:
        candidates.append(Path(claude_config_dir).expanduser())
    home = merged.get("HOME")
    if home:
        candidates.append(Path(home) / ".claude")
    candidates.append(Path.home() / ".claude")
    if scan_user_homes:
        for root in _USER_HOME_ROOTS:
            try:
                candidates.extend(home / ".claude" for home in root.iterdir() if home.is_dir())
            except OSError:
                continue
    unique: dict[Path, Path] = {}
    for path in candidates:
        unique.setdefault(path.resolve(), path)
    return list(unique.values())


def _is_file(path: Path) -> bool:
    """``Path.is_file()`` 只吞 ENOENT，EACCES 会抛；探查他域目录须连带兜底。"""
    try:
        return path.is_file()
    except OSError:
        return False


def _is_safe_session_id(session_id: str) -> bool:
    """会话 id 仅作单段文件名：拒绝空值、路径穿越与 glob 元字符。

    glob 元字符（``*`` / ``?`` / ``[``）会被后续兜底 glob 解释，须在此拦截，
    避免 ``session_id`` 成为带通配的匹配模式命中非目标会话。
    """
    if not session_id or session_id in {".", ".."}:
        return False
    if Path(session_id).name != session_id:
        return False
    return not any(ch in session_id for ch in "*?[")


def claude_project_key(directory: str) -> str:
    """Encode a project path using Claude CLI's projects directory naming."""
    return re.sub(r"[^A-Za-z0-9]", "-", str(Path(directory).expanduser().resolve()))


def _glob_claude_sessions_in(config_dir: Path, session_id: str) -> set[Path]:
    """单个配置目录下跨项目 glob（*/<session_id>.jsonl）命中的会话文件（resolved 集合）。"""
    matches: set[Path] = set()
    try:
        for path in (config_dir / "projects").glob(f"*/{session_id}.jsonl"):
            if _is_file(path):
                matches.add(path.resolve())
    except OSError:
        pass
    return matches


def resolve_claude_session_path(
    session_id: str,
    directory: str,
    env: Mapping[str, str] | None = None,
    *,
    scan_user_homes: bool = False,
) -> Path | None:
    """Prefer the requested project; accept a legacy path only when unique."""
    if not _is_safe_session_id(session_id):
        return None
    config_dirs = claude_config_dirs(env, scan_user_homes=scan_user_homes)
    encoded = claude_project_key(directory)
    for config_dir in config_dirs:
        path = config_dir / "projects" / encoded / f"{session_id}.jsonl"
        if _is_file(path):
            return path
    matches: set[Path] = set()
    for config_dir in config_dirs:
        matches.update(_glob_claude_sessions_in(config_dir, session_id))
    return next(iter(matches)) if len(matches) == 1 else None


def _opencode_db_from_env(merged: Mapping[str, str]) -> Path | None:
    """环境变量 OPENCODE_DB_PATH/OPENCODE_DB 指定的数据库；未设或不存在返回 None。"""
    for key in ("OPENCODE_DB_PATH", "OPENCODE_DB"):
        env_value = merged.get(key)
        if env_value:
            path = Path(env_value)
            if path.exists():
                logger.debug("opencode 数据库来自环境变量 %s：%s", key, path)
                return path
    return None


def _opencode_db_from_global_config() -> Path | None:
    """全局配置 ~/.config/opencode/config.json 的 db_path；未配置或无效返回 None。"""
    global_config = Path.home() / ".config/opencode/config.json"
    if not global_config.exists():
        return None
    try:
        cfg = json.loads(global_config.read_text())
        if "db_path" in cfg:
            path = Path(cfg["db_path"]).expanduser()
            if path.exists():
                logger.debug("opencode 数据库来自全局配置：%s", path)
                return path
    except Exception as ex:
        logger.debug("读取 opencode 全局配置失败：%s", ex)
    return None


def _opencode_db_from_project_config(project_dir: str) -> Path | None:
    """项目配置 <project_dir>/.opencode/config.json 的 db_path（相对路径按项目目录解析）。"""
    project_config = Path(project_dir) / ".opencode/config.json"
    if not project_config.exists():
        return None
    try:
        cfg = json.loads(project_config.read_text())
        if "db_path" in cfg:
            path = Path(cfg["db_path"]).expanduser()
            if not path.is_absolute():
                path = (Path(project_dir) / path).resolve()
            if path.exists():
                logger.debug("opencode 数据库来自项目配置：%s", path)
                return path
    except Exception as ex:
        logger.debug("读取 opencode 项目配置失败：%s", ex)
    return None


def find_opencode_db(env: Mapping[str, str] | None = None) -> Path:
    """按优先级定位 OpenCode 数据库：环境变量 → 全局/项目配置 → XDG 数据目录。"""
    merged = {**os.environ, **(env or {})}

    env_db = _opencode_db_from_env(merged)
    if env_db is not None:
        return env_db

    global_db = _opencode_db_from_global_config()
    if global_db is not None:
        return global_db

    project_dir = merged.get("OPENCODE_PROJECT_DIR") or merged.get("PWD") or os.getcwd()
    project_db = _opencode_db_from_project_config(project_dir)
    if project_db is not None:
        return project_db

    xdg = merged.get("XDG_DATA_HOME")
    if xdg:
        path = Path(xdg) / "opencode/opencode.db"
        if path.exists():
            logger.debug("opencode 数据库来自 XDG_DATA_HOME：%s", path)
            return path

    default_path = Path.home() / ".local/share/opencode/opencode.db"
    logger.debug("opencode 数据库用默认路径：%s", default_path)
    return default_path


def _rollout_main_mtime(path: Path, session_id: str) -> int | None:
    """rollout 文件是会话主线程（首行 session_meta 且 id 匹配、非 subagent）时
    返回其 mtime_ns；读失败/不匹配/subagent 线程返回 None。
    """
    try:
        with path.open("r", encoding="utf-8") as stream:
            row = json.loads(stream.readline())
        modified_at = path.stat().st_mtime_ns
        payload = row.get("payload") if isinstance(row, dict) else None
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    is_unrelated_session = (
        not isinstance(row, dict)
        or row.get("type") != "session_meta"
        or not isinstance(payload, dict)
        or payload.get("id") != session_id
    )
    if is_unrelated_session:
        return None
    thread_source = payload.get("thread_source")
    if thread_source == "subagent" or (
        isinstance(thread_source, dict) and "subagent" in thread_source
    ):
        return None
    return modified_at


def _find_codex_rollout(session_id: str, cwd: str) -> Path:
    """Find the newest main Codex rollout in the workspace-local Codex home."""
    if not _is_safe_session_id(session_id):
        raise ValueError(f"Invalid Codex session id: {session_id}")
    sessions = Path(cwd).expanduser().resolve() / ".codex" / "sessions"
    matches: list[tuple[Path, int]] = []
    try:
        for path in sessions.rglob("rollout-*.jsonl"):
            if session_id not in path.name:
                continue
            modified_at = _rollout_main_mtime(path, session_id)
            if modified_at is not None:
                matches.append((path, modified_at))
    except OSError:
        pass
    if not matches:
        raise ValueError(
            f"Codex rollout not found for session {session_id} under {sessions}",
        )
    return max(matches, key=lambda match: match[1])[0]


def resolve_transcript_source(
    native_session_id: str | None,
    *,
    runtime: str,
    cwd: str = ".",
    env: Mapping[str, str] | None = None,
    scan_user_homes: bool = False,
) -> TranscriptSource | None:
    """按 runtime 定位转录源；runtime 不明或无 sdk 会话 id 返回 None。

    claude 系空 cwd 抛 ValueError：转录落点按 cwd 编码，"." 默认值只会
    拼出错误路径的误导性 file-not-found。
    """
    if not native_session_id:
        return None
    if runtime in _CLAUDE_RUNTIMES:
        if not cwd:
            raise ValueError("Session has no cwd")
        path = resolve_claude_session_path(
            native_session_id,
            cwd,
            env,
            scan_user_homes=scan_user_homes,
        )
        if path is None:
            if not _is_safe_session_id(native_session_id):
                return None
            path = (
                claude_config_dirs(
                    env,
                    scan_user_homes=scan_user_homes,
                )[0]
                / "projects"
                / claude_project_key(cwd)
                / f"{native_session_id}.jsonl"
            )
        return TranscriptSource("claude-code", "claude-jsonl", path)
    if runtime == "opencode":
        return TranscriptSource("opencode", "opencode-db", find_opencode_db(env))
    if runtime == "codex_cli":
        if not cwd:
            raise ValueError("Session has no cwd")
        return TranscriptSource(
            "codex",
            "codex-jsonl",
            _find_codex_rollout(native_session_id, cwd),
        )
    return None


def resolve_roster_source(
    roster_row: Mapping[str, Any],
    default_runtime: str = "",
    *,
    scan_user_homes: bool = False,
) -> TranscriptSource | None:
    """按会话记录行定位：config_json 的 runtime/cwd/env_overrides，缺省回落全局。"""
    try:
        cfg = json.loads(roster_row.get("config_json") or "{}")
    except (TypeError, ValueError):
        cfg = {}
    if not isinstance(cfg, dict):
        cfg = {}
    return resolve_transcript_source(
        roster_row.get("native_session_id"),
        runtime=str(cfg.get("runtime") or default_runtime),
        cwd=str(cfg.get("cwd") or "."),
        env=cfg.get("env_overrides") or None,
        scan_user_homes=scan_user_homes,
    )
