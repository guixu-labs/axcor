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

"""文件态重建器：重放 write/edit/multi_edit/bash + git 历史 + 快照仓（自插件迁移）。

在空目录 ``target_cwd`` 里前向逐字节重放时间线上的工具调用（内容来自 db
的 argsJson，非 LLM 重生成）；项目 ``.git`` 与快照仓两仓严格分离，git 命令
始终重放（GIT_AUTHOR_DATE 注入保 hash 保真），只读探查跳过、mutating 全量
重放（每命令超时杀整树，失败只记日志不中断）。
"""

from __future__ import annotations

__all__ = [
    "CheckpointFileState",
    "FileStateRebuilder",
    "LogCallback",
]

import contextlib
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path
from typing import Any, NamedTuple

from workflow_controller.core.restore.cannbot_reader import (
    TimelineEntry,
    extract_subagent_sid_from_args,
    extract_subagent_type_from_args,
    iso_to_ms,
)
from workflow_controller.core.restore.snapshot_mirror import (
    DEFAULT_IGNORE_PATTERNS,
    build_snapshot_commit_message,
    mirror_inner_git,
    python_sync_mirror,
    rsync_mirror,
)
from workflow_controller.core.workspace.repository import (
    INNER_GIT_RESERVED_PATH,
    run_snapshot_git_sync,
)

logger = logging.getLogger(__name__)

# git 可执行名（可用性探测用；实际子进程调用一律经私有仓模块同步出口）
_GIT_EXECUTABLE = "git"

# 单行日志回调类型：每步/每阶段发一行人类可读文本
LogCallback = Callable[[str], None]

# 快照仓 commit 的 author 身份（与 SnapshotEngine._GIT_ENV 一致）
_SNAPSHOT_GIT_ENV: dict[str, str] = {
    "GIT_AUTHOR_NAME": "workflow-controller-snapshot",
    "GIT_AUTHOR_EMAIL": "workflow-controller-snapshot@local",
    "GIT_COMMITTER_NAME": "workflow-controller-snapshot",
    "GIT_COMMITTER_EMAIL": "workflow-controller-snapshot@local",
}


@dataclass(frozen=True)
class CheckpointFileState:
    """某个 task 边界处快照仓 commit 的元数据。``git_ref`` 指向快照仓 sha。"""

    checkpoint_index: int  # 1-based 序号
    git_ref: str | None  # 快照仓 commit sha
    file_count: int
    byte_size: int
    trigger: str  # "subagent_start" | "session_complete"
    subagent_session_id: str  # task 派发的子 agent sdk id（session_complete 时为空）
    parent_task_call_id: str | None  # 主 task tool call 的 toolCallId
    status: str = "ok"
    inner_git: str = ""  # 边界时刻内部 git 捕获结果（captured/absent/...）


class SnapshotCommit(NamedTuple):
    """``_do_snapshot_commit`` 结果：字段名访问（``result.inner_git``）替代下标。"""

    git_ref: str | None
    file_count: int
    byte_size: int
    inner_git: str


class FileStateRebuilder:
    """逐字节重放 tool calls + 在 task 边界打快照仓 commit。"""

    def __init__(
        self,
        target_cwd: Path,
        origin_cwd: str,
        ignore_patterns: list[str] | None = None,
        bash_replay_mode: str = "all",
        git_hash_fidelity: bool = True,
        bash_timeout_seconds: float | None = 30.0,
        log_cb: LogCallback | None = None,
        snapshot_git_state: bool = True,
        snapshot_git_state_max_mb: int = 200,
        max_replay_tool_calls: int | None = None,
    ) -> None:
        self.target_cwd = Path(target_cwd).expanduser().resolve()
        self.origin_cwd = origin_cwd
        self.ignore_patterns = list(
            ignore_patterns if ignore_patterns is not None else DEFAULT_IGNORE_PATTERNS
        )
        # 与 SnapshotEngine 一致：保留路径排除，防镜像趟删掉上一检查点的内部 git 镜像
        if INNER_GIT_RESERVED_PATH not in self.ignore_patterns:
            self.ignore_patterns.append(INNER_GIT_RESERVED_PATH)
        # 全量重放：只读探查跳过，其余每命令超时（``bash_timeout`` 为 None 时不超时）。
        self.bash_replay_mode = bash_replay_mode
        self.git_hash_fidelity = git_hash_fidelity
        # None → 透传给 subprocess.communicate(timeout=None)，命令跑到自然结束。
        self.bash_timeout = bash_timeout_seconds
        # 边界快照镜像 target_cwd/.git（重建出的历史逐边界定格进检查点）
        self.snapshot_git_state = snapshot_git_state
        self.snapshot_git_state_max_mb = snapshot_git_state_max_mb
        # 快照仓目录：与 SnapshotEngine 布局一致
        self.snapshot_dir = self.target_cwd / ".workflow-controller" / "snapshots"
        self.working_dir = self.snapshot_dir / "working"
        self._checkpoints: list[CheckpointFileState] = []
        self._snapshot_inited = False
        # 上一个快照的 git_ref（文件未变时可复用）
        self._last_snapshot_ref: str | None = None
        # 日志回调（可空）：每步/每阶段一行文本，best-effort，回调内异常被吞掉
        self._log_cb = log_cb
        # 重放上限：tool_call 数超限只重放时间线前缀。None = 不限制
        self.max_replay_tool_calls = max_replay_tool_calls

    # ── 入口 ──────────────────────────────────────────────────────────

    # ── 快照仓 + 项目 .git 初始化 ────────────────────────────────────

    @staticmethod
    def _parse_args(tc: dict[str, Any]) -> dict[str, Any]:
        """解析 ToolCall.argsJson → dict。失败返回 {}。"""
        raw = str(tc.get("argsJson") or "")
        if not raw:
            return {}
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else {"_raw": obj}
        except (json.JSONDecodeError, TypeError):
            return {}

    @staticmethod
    def _arg(args: dict[str, Any], *keys: str) -> Any:
        """按优先级取首个非空参数键（opencode camelCase 优先，兼容 Claude Code snake_case）。"""
        for k in keys:
            if args.get(k) is not None:
                return args[k]
        return None

    # ── 重放原语 ─────────────────────────────────────────────────────

    @staticmethod
    def _run_git(args: list[str], cwd: Path, timeout: float = 30.0) -> Any:
        """快照仓 git 子进程（经私有仓模块同步出口，快照身份），失败不抛。"""
        return run_snapshot_git_sync(cwd, args, timeout=timeout)

    def replay(self, timeline: list[TimelineEntry]) -> list[CheckpointFileState]:
        """逐字节回放全量工具调用，每遇 task 边界打一次快照仓 commit。

        返回 ``list[CheckpointFileState]``（含末尾 ``session_complete``）。
        """
        # 初始化快照仓 + 项目 .git
        self._ensure_snapshot_repo()
        # session 未跑 git init 时 best-effort 自建空仓
        self._ensure_project_git_if_needed()

        self._log_line("phase=replay_start")

        # 超限截断：只重放时间线前缀
        entries = _truncate_timeline_prefix(timeline, self.max_replay_tool_calls, self._log_line)

        # 遍历 timeline 按序重放
        for entry in entries:
            if entry.kind != "tool_call" or entry.tool_call is None:
                continue
            tc = entry.tool_call
            tool_name = str(tc.get("toolName") or "")
            arg = self._summarize_arg(tc, tool_name)
            t0 = time.monotonic()
            try:
                status = self._replay_status(tool_name, tc, entry)
                self._log_step(tool_name, arg, status, t0)
            except Exception as exc:
                # best-effort：单条失败不中断整体
                self._log_step(tool_name, arg, "error", t0, err=str(exc))
                logger.warning(
                    "replay: %s tool_call %s failed (non-fatal): %s",
                    tool_name,
                    str(tc.get("toolCallId") or "")[:16],
                    exc,
                )

        # 末尾 session_complete 检查点
        self._snapshot_session_complete()
        self._log_line(f"phase=replay_end checkpoints={len(self._checkpoints)}")
        return list(self._checkpoints)

    def _replay_status(self, tool_name: str, tc: dict[str, Any], entry: TimelineEntry) -> str:
        """单条 tool_call 的重放分发，返回本步状态（ok / error / skipped）。"""
        if tool_name == "write":
            self._apply_write(tc)
            return "ok"
        if tool_name == "edit":
            self._apply_edit(tc)
            return "ok"
        if tool_name == "multi_edit":
            self._apply_multi_edit(tc)
            return "ok"
        if tool_name == "bash":
            return self._apply_bash(tc)
        if tool_name == "task":
            # task 边界打快照；返回本步状态（ok/error）。仅主 agent 的派发
            # （entry.role == "main"）打检查点——检查点语义 = 主 agent 派发
            # 边界（重放截断点是其主 session 的 task part）。孙 agent 的派发
            # part 在子会话里，主 session 无锚，打出的检查点不可重放，还污染
            # deriver 的边界配对与 max_iterations 推导。
            if entry.role == "subagent":
                return "skipped"
            return self._snapshot_at_boundary(tc, entry)
        # read/glob/grep/skill: 跳过（不改文件态）
        return "skipped"

    def _ensure_snapshot_repo(self) -> None:
        """初始化快照仓（``<cwd>/.workflow-controller/snapshots/.git``）。"""
        if self._snapshot_inited:
            return
        if shutil.which(_GIT_EXECUTABLE) is None:
            logger.warning("replay: git not available — snapshot repo disabled")
            return
        try:
            self.snapshot_dir.mkdir(parents=True, exist_ok=True)
            self.working_dir.mkdir(parents=True, exist_ok=True)
            git_dir = self.snapshot_dir / ".git"
            if not git_dir.exists():
                r = self._run_git(["init"], cwd=self.snapshot_dir)
                if r.returncode != 0:
                    logger.warning("replay: snapshot git init failed: %s", r.stderr.strip()[:200])
                    return
            # 空 baseline commit（让首次快照有 parent）
            self._run_git(["add", "-A", "--force"], cwd=self.snapshot_dir)
            msg = (
                "workflow-controller snapshot baseline\n\n"
                "session: reconstructed\n"
                "checkpoint: baseline\n"
            )
            self._run_git(["commit", "-m", msg, "--allow-empty"], cwd=self.snapshot_dir)
            self._snapshot_inited = True
        except Exception:
            logger.exception("replay: ensure_snapshot_repo failed")

    def _ensure_project_git_if_needed(self) -> None:
        """若 target_cwd 不是 git 仓，建空仓让后续 git add/commit 可用。"""
        if shutil.which(_GIT_EXECUTABLE) is None:
            return
        if (self.target_cwd / ".git").exists():
            return
        r = self._run_git(["init"], cwd=self.target_cwd)
        if r.returncode != 0:
            logger.debug(
                "replay: project git init failed (non-fatal): %s",
                r.stderr.strip()[:200],
            )

    def _apply_write(self, tc: dict[str, Any]) -> None:
        """逐字节回放 write：解析 filePath + content，path-rewrite，写盘。"""
        args = self._parse_args(tc)
        fp = str(self._arg(args, "filePath", "file_path") or "")
        content = str(args.get("content") or "")
        if not fp:
            return
        dest = self._rewrite_path(fp)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content.encode("utf-8"))

    def _apply_edit(self, tc: dict[str, Any]) -> None:
        """逐字节回放 edit：filePath + oldString/newString 字符串替换。

        oldString 须精确匹配；不匹配则 best-effort 跳过。
        """
        args = self._parse_args(tc)
        fp = str(self._arg(args, "filePath", "file_path") or "")
        old_s = str(self._arg(args, "oldString", "old_string") or "")
        new_s = str(self._arg(args, "newString", "new_string") or "")
        if not fp or not old_s:
            return
        dest = self._rewrite_path(fp)
        if not dest.is_file():
            logger.warning("replay: edit target not exists: %s — skipped", dest)
            return
        try:
            content = dest.read_bytes().decode("utf-8", errors="replace")
        except OSError:
            logger.warning("replay: edit target unreadable: %s — skipped", dest)
            return
        # 精确匹配替换（支持多次出现 → 全替换，对齐 opencode Edit 的 replaceAll 语义）
        if old_s not in content:
            logger.warning(
                "replay: edit oldString not matched in %s — skipped (file state may be partial)",
                dest,
            )
            return
        new_content = content.replace(old_s, new_s)
        dest.write_bytes(new_content.encode("utf-8"))

    def _apply_multi_edit(self, tc: dict[str, Any]) -> None:
        """逐字节回放 multi_edit：同一文件按序应用多组精确替换。

        归一化自 Claude Code 的 MultiEdit
        （``edits: [{old_string, new_string, replace_all}]``）。
        任一条不匹配 best-effort 跳过该条，其余条继续。
        """
        args = self._parse_args(tc)
        fp = str(self._arg(args, "filePath", "file_path") or "")
        if not fp:
            return
        dest = self._rewrite_path(fp)
        if not dest.is_file():
            logger.warning("replay: multi_edit target not exists: %s — skipped", dest)
            return
        try:
            content = dest.read_bytes().decode("utf-8", errors="replace")
        except OSError:
            logger.warning("replay: multi_edit target unreadable: %s — skipped", dest)
            return
        for i, e in enumerate(args.get("edits") or []):
            if not isinstance(e, dict):
                continue
            old_s = str(self._arg(e, "oldString", "old_string") or "")
            new_s = str(self._arg(e, "newString", "new_string") or "")
            if not old_s:
                continue
            if old_s not in content:
                logger.warning(
                    "replay: multi_edit edit #%d old not matched in %s — "
                    "skipped (file state may be partial)",
                    i,
                    dest,
                )
                continue
            # Claude MultiEdit 的 replace_all 默认 false（仅首处），
            # 不可沿用 _apply_edit 的恒全替换（那是 opencode Edit 语义）
            if self._arg(e, "replace_all", "replaceAll"):
                content = content.replace(old_s, new_s)
            else:
                content = content.replace(old_s, new_s, 1)
        dest.write_bytes(content.encode("utf-8"))

    # ── 快照仓 commit ────────────────────────────────────────────────

    def _apply_bash(self, tc: dict[str, Any]) -> str:
        """重放 bash：只读探查跳过，mutating 全量重放，超时杀树。

        - ``read_only_probe``：grep/ls/find/echo/cat/... 对文件态零贡献，跳过。
        - ``mutating``：mkdir/cp/python3/make/... 全量重放，失败/超时只记日志（best-effort）。
        - ``git``：mutating git 走 ``_apply_git_command``（保 GIT_AUTHOR_DATE 注入）；
          只读 git（status/diff/log/show）归入 read_only_probe 跳过。

        cwd 取 argsJson 的 ``workdir``（path-rewrite origin→target），无则回退
        target_cwd——相对路径脚本须从录制的 workdir 跑才能正确解析，否则输出
        落不到目标路径。

        返回本步状态（ok/skipped/timeout/error）供日志记录。
        """
        args = self._parse_args(tc)
        command = str(args.get("command") or "")
        if not command:
            return "ok"
        workdir = self._resolve_workdir(args)
        category = _classify_bash_command(command)
        if category == "read_only_probe":
            logger.debug("replay: read_only_probe skipped: %s", command[:80])
            return "skipped"
        if category == "git":
            return self._apply_git_command(command, tc, cwd=workdir)
        # mutating 非 git：重放，超时杀树（timeout=None 时不超时，跑到自然结束）
        return self._run_bash_subprocess(
            command,
            tc,
            allow_fail=True,
            timeout=self.bash_timeout,
            cwd=workdir,
        )

    def _resolve_workdir(self, args: dict[str, Any]) -> Path:
        """从 argsJson.workdir 解析子进程 cwd（path-rewrite origin→target）。

        绝对路径且前缀匹配 origin_cwd → 重写到 target_cwd 下对应子目录；
        其余回退 target_cwd。子目录不存在则 best-effort mkdir -p。
        """
        raw = str(args.get("workdir") or "").strip()
        if not raw:
            return self.target_cwd
        rewritten = self._rewrite_path(raw)
        if rewritten == self.target_cwd or not rewritten.is_absolute():
            return self.target_cwd
        # workdir 可能在前序 mkdir 之前被引用（如 build/Ascend950 仓内命令
        # 早于 mkdir build）——best-effort 建出目录让脚本 cwd 存在。
        try:
            rewritten.mkdir(parents=True, exist_ok=True)
        except OSError:
            logger.debug("replay: workdir mkdir failed (non-fatal): %s", rewritten)
        return rewritten

    def _apply_git_command(
        self,
        command: str,
        tc: dict[str, Any],
        *,
        cwd: Path | None = None,
    ) -> str:
        """在 cwd（默认 target_cwd）执行 git 命令，重建项目 .git 历史。

        hash 保真靠 tree + parent + author/committer timestamp + message。
        timestamp 用 ``ToolCall.startedAt`` 设 GIT_AUTHOR_DATE/COMMITTER_DATE，
        author name/email 从 ``git commit`` 的 resultJson 解析回填。

        cwd 取录制 workdir（path-rewrite）——git 命令同样可能有 workdir
        （如 ``cd operators/xlogy && git ...``），用 target_cwd 会找不到仓。

        返回本步状态（ok/timeout/error）。
        """
        # 提取 startedAt 设时间戳
        started_at_ms = iso_to_ms(tc.get("startedAt"))
        env = dict(os.environ)
        env.update(_SNAPSHOT_GIT_ENV)
        if self.git_hash_fidelity and started_at_ms:
            iso_ts = _ms_to_git_iso(started_at_ms)
            env["GIT_AUTHOR_DATE"] = iso_ts
            env["GIT_COMMITTER_DATE"] = iso_ts
            # 从 resultJson 解析 author name/email 回填
            author_name, author_email = _parse_git_author_from_result(
                str(tc.get("resultJson") or ""),
            )
            if author_name:
                env["GIT_AUTHOR_NAME"] = author_name
            if author_email:
                env["GIT_AUTHOR_EMAIL"] = author_email
        # path rewrite 命令内绝对路径；process_group=0 便于超时杀整树
        return self._run_shell_subprocess(
            command,
            env=env,
            timeout=self.bash_timeout,
            cwd=cwd,
            log_tag="replay: git command",
            log_fn=logger.warning,
        )

    def _run_shell_subprocess(
        self,
        command: str,
        *,
        env: dict[str, str],
        timeout: float | None,
        cwd: Path | None,
        log_tag: str,
        log_fn: Any,
        allow_fail: bool = True,
    ) -> str:
        """跑单条 bash 子进程（git/普通命令两轨共用），返回 ok/timeout/error。

        固定经 /bin/bash -c 执行，保证 brace 展开/$(...) 等 bashism 语义与录制时一致；
        text=False（二进制）避免输出含非 UTF-8 字节时 communicate() 抛 UnicodeDecodeError。
        """
        rewritten = self._rewrite_command_paths(command)
        run_cwd = str(cwd) if cwd is not None else str(self.target_cwd)
        proc: subprocess.Popen[bytes] | None = None
        try:
            proc = subprocess.Popen(
                ["/bin/bash", "-c", rewritten],
                cwd=run_cwd,
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
                env=env,
                process_group=0,
            )
            proc.communicate(timeout=timeout)
            return "ok"
        except subprocess.TimeoutExpired:
            if proc is not None:
                _kill_process_group(proc)
                _drain_after_kill(proc)
            log_fn("%s timeout (non-fatal): %s", log_tag, rewritten[:80])
            return "timeout"
        except OSError as exc:
            if proc is not None:
                _kill_process_group(proc)
            if not allow_fail:
                raise
            log_fn("%s failed (non-fatal): %s | err=%s", log_tag, rewritten[:80], str(exc)[:80])
            return "error"

    def _run_bash_subprocess(
        self,
        command: str,
        tc: dict[str, Any],
        *,
        allow_fail: bool = True,
        timeout: float | None = None,
        cwd: Path | None = None,
    ) -> str:
        """执行非 git 的 bash 命令（文件操作 / build / test），best-effort。

        ``process_group=0`` 起新进程组，超时 ``os.killpg`` 杀整个进程树（含管道/
        后台派生的孙进程），避免只杀 shell 父进程留下孤儿进程。

        cwd 取录制 workdir（path-rewrite），让相对路径脚本在正确子目录里跑。

        返回本步状态（ok/timeout/error）。
        """
        return self._run_shell_subprocess(
            command,
            env=dict(os.environ),
            timeout=timeout,
            cwd=cwd,
            log_tag="replay: bash",
            log_fn=logger.debug,
            allow_fail=allow_fail,
        )

    def _snapshot_at_boundary(
        self,
        tc: dict[str, Any],
        entry: TimelineEntry,
    ) -> str:
        """在 task 边界打快照仓 commit，返回本步状态（``ok`` | ``error``）。

        与 SnapshotEngine.snapshot 同构：镜像 → git add → git commit → rev-parse HEAD。
        """
        sub_sid = str(entry.subagent_session_id or "") or extract_subagent_sid_from_args(tc)
        parent_tc_id = str(tc.get("toolCallId") or "")
        cp_index = len(self._checkpoints) + 1
        result = self._do_snapshot_commit(
            checkpoint_id=f"cp_rc_{sub_sid[:8] or 'main'}_{cp_index:03d}",
            session_id="reconstructed",
            agent_id=sub_sid,
            agent_type=extract_subagent_type_from_args(tc),
            iteration=cp_index,
            trigger="subagent_start",
            parent_checkpoint_id=(self._checkpoints[-1].git_ref if self._checkpoints else None),
            main_part_id=None,
        )
        status = "ok" if result.git_ref else "error"
        self._checkpoints.append(
            CheckpointFileState(
                checkpoint_index=cp_index,
                git_ref=result.git_ref,
                file_count=result.file_count,
                byte_size=result.byte_size,
                trigger="subagent_start",
                subagent_session_id=sub_sid,
                parent_task_call_id=parent_tc_id,
                status=status,
                inner_git=result.inner_git,
            )
        )
        return status

    # ── 日志回调（best-effort，异常被吞） ──────────────────────────────

    def _snapshot_session_complete(self) -> None:
        """末尾追加 session_complete 终态检查点。"""
        cp_index = len(self._checkpoints) + 1
        result = self._do_snapshot_commit(
            checkpoint_id=f"cp_rc_main_{cp_index:03d}",
            session_id="reconstructed",
            agent_id="",
            agent_type="main",
            iteration=cp_index,
            trigger="session_complete",
            parent_checkpoint_id=(self._checkpoints[-1].git_ref if self._checkpoints else None),
            main_part_id=None,
        )
        self._checkpoints.append(
            CheckpointFileState(
                checkpoint_index=cp_index,
                git_ref=result.git_ref,
                file_count=result.file_count,
                byte_size=result.byte_size,
                trigger="session_complete",
                subagent_session_id="",
                parent_task_call_id=None,
                status="ok" if result.git_ref else "error",
                inner_git=result.inner_git,
            )
        )

    def _do_snapshot_commit(
        self,
        *,
        checkpoint_id: str,
        session_id: str,
        agent_id: str,
        agent_type: str,
        iteration: int,
        trigger: str,
        parent_checkpoint_id: str | None,
        main_part_id: str | None,
    ) -> SnapshotCommit:
        """打一次快照仓 commit，返回 SnapshotCommit(git_ref, file_count, byte_size, inner_git)。"""
        inner_git = ""
        if not self._snapshot_inited:
            return SnapshotCommit(None, 0, 0, inner_git)
        # 镜像 target_cwd → working/ + 内部 git → working/.workflow-controller-inner-git/
        file_count, byte_size, inner_git = self._mirror_for_snapshot()
        # git add -A --force（快照仓独立 repo）
        r = self._run_git(["add", "-A", "--force"], cwd=self.snapshot_dir)
        if r.returncode != 0:
            logger.warning("replay: snapshot git add failed: %s", r.stderr.strip()[:200])
            return SnapshotCommit(None, file_count, byte_size, inner_git)
        # 构造 commit message（结构化 trailer，与 SnapshotEngine 同构）
        msg = build_snapshot_commit_message(
            session_id=session_id,
            checkpoint_id=checkpoint_id,
            agent_id=agent_id,
            agent_type=agent_type,
            iteration=iteration,
            trigger=trigger,
            event_rowid=None,
            parent_checkpoint_id=parent_checkpoint_id,
            main_part_id=main_part_id,
        )
        git_ref = self._commit_snapshot_head(msg)
        if git_ref is None:
            return SnapshotCommit(None, file_count, byte_size, inner_git)
        self._last_snapshot_ref = git_ref
        return SnapshotCommit(git_ref, file_count, byte_size, inner_git)

    def _mirror_for_snapshot(self) -> tuple[int, int, str]:
        """镜像 target_cwd → working/ 并捕获内部 git，返回 (file_count, byte_size, inner_git)。"""
        file_count, byte_size = self._sync_mirror()
        inner_git = ""
        if self.snapshot_git_state:
            inner_git = mirror_inner_git(
                self.target_cwd,
                self.working_dir,
                max_mb=self.snapshot_git_state_max_mb,
            )
        return file_count, byte_size, inner_git

    def _commit_snapshot_head(self, msg: str) -> str | None:
        """快照仓 commit → rev-parse HEAD；任一步失败返回 None。"""
        r = self._run_git(["commit", "-m", msg, "--allow-empty"], cwd=self.snapshot_dir)
        if r.returncode != 0:
            logger.warning("replay: snapshot git commit failed: %s", r.stderr.strip()[:200])
            return None
        # 取 HEAD sha
        r = self._run_git(["rev-parse", "HEAD"], cwd=self.snapshot_dir)
        if r.returncode != 0 or not r.stdout.strip():
            return None
        return r.stdout.strip()

    def _sync_mirror(self) -> tuple[int, int]:
        """镜像 target_cwd → working/（rsync 优先，不可用降级纯 Python）。"""
        result = rsync_mirror(self.target_cwd, self.working_dir, self.ignore_patterns)
        if result is not None:
            return result
        return python_sync_mirror(self.target_cwd, self.working_dir, self.ignore_patterns)

    # ── 路径重写 ─────────────────────────────────────────────────────

    def _log_line(self, line: str) -> None:
        """发一行日志给回调（阶段标记等）。回调内异常吞掉，不影响重建主流程。"""
        if self._log_cb is None:
            return
        try:
            self._log_cb(line)
        except Exception:
            logger.debug("replay: log_cb raised (swallowed)", exc_info=True)

    def _log_step(
        self,
        tool: str,
        arg: str,
        status: str,
        t0: float,
        *,
        err: str = "",
    ) -> None:
        """记一行重放步：工具/参数摘要/状态/耗时（失败时附 err）。"""
        dur = int((time.monotonic() - t0) * 1000)
        line = f"STEP {tool} {arg[:80]} status={status} dur={dur}ms"
        if err:
            line += f" err={err}"
        self._log_line(line)

    # ── 工具 ──────────────────────────────────────────────────────────

    def _summarize_arg(self, tc: dict[str, Any], tool_name: str) -> str:
        """从 argsJson 抽命令/路径摘要，作为日志的参数段。"""
        args = self._parse_args(tc)
        if tool_name == "bash":
            return str(args.get("command") or "")[:200]
        if tool_name in ("write", "edit", "multi_edit"):
            return str(self._arg(args, "filePath", "file_path") or "")[:200]
        if tool_name == "task":
            sub = extract_subagent_type_from_args(tc)
            return f"task({sub})" if sub else "task"
        return ""

    def _rewrite_path(self, p: str) -> Path:
        """绝对路径 prefix 匹配：origin_cwd → target_cwd/rel。

        相对路径或非 origin_cwd 绝对路径不动。
        """
        if not p:
            return self.target_cwd
        try:
            rp = Path(p).expanduser()
            if not rp.is_absolute():
                # 相对路径：相对 target_cwd 解析
                return (self.target_cwd / rp).resolve(strict=False)
            rp = rp.resolve(strict=False)
            origin_resolved = Path(self.origin_cwd).expanduser().resolve(strict=False)
            if rp.is_relative_to(origin_resolved):
                rel = rp.relative_to(origin_resolved)
                return Path(self.target_cwd) / rel
        except (ValueError, OSError):
            pass
        # 非 origin_cwd 绝对路径，返回原值（best-effort）
        return Path(p)

    def _rewrite_command_paths(self, command: str) -> str:
        """重写 bash 命令文本里的绝对路径（path-prefix-aware）。"""
        # origin_cwd 为空时跳过：空串正则会零宽匹配每个位置，把 target 路径
        # 插进命令里几乎所有非词边界处（l2_transcriber 的文本重写同此守卫）
        if not command or not self.origin_cwd or self.origin_cwd == str(self.target_cwd):
            return command
        pattern = re.escape(self.origin_cwd) + r"(?![a-zA-Z0-9_.-])"
        return re.sub(pattern, str(self.target_cwd), command)


# ── 模块级辅助函数 ─────────────────────────────────────────────────


def _kill_process_group(proc: subprocess.Popen[bytes]) -> None:
    """杀掉进程组（含管道/后台派生的孙进程），best-effort 不抛。

    依赖 ``process_group=0`` 使子进程成独立进程组（pgid == proc.pid）。必须按
    启动时已知的 pgid 杀：主进程可能已退出成 zombie，macOS 上对 zombie 调
    ``os.getpgid`` 抛 ESRCH，被 suppress 吞掉后 killpg 根本不执行——后台循环
    孙进程存活持有管道，后续排空 communicate() 会永久卡死重放。
    """
    try:
        pgid = os.getpgid(proc.pid)
    except OSError:
        pgid = proc.pid  # process_group=0 ⇒ pgid == pid
    with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
        os.killpg(pgid, signal.SIGKILL)


# 杀组后排空管道的时限：脱离进程组的孙进程持有管道时不能无界等待
_POST_KILL_DRAIN_SECONDS = 2.0


def _truncate_timeline_prefix(
    timeline: list[TimelineEntry],
    max_replay_tool_calls: int | None,
    log_line: LogCallback,
) -> list[TimelineEntry]:
    """超限截断：只重放时间线前缀（前 max 个 tool_call 所在位置之前）。"""
    if max_replay_tool_calls is None:
        return timeline
    n_tool_calls = sum(1 for e in timeline if e.kind == "tool_call")
    if n_tool_calls <= max_replay_tool_calls:
        return timeline
    kept = 0
    cut = len(timeline)
    for i, e in enumerate(timeline):
        if e.kind == "tool_call":
            kept += 1
            if kept > max_replay_tool_calls:
                cut = i
                break
    log_line(
        f"phase=replay_truncated max_tool_calls={max_replay_tool_calls} "
        f"total={n_tool_calls} dropped={n_tool_calls - max_replay_tool_calls}"
    )
    return timeline[:cut]


def _close_proc_streams(proc: subprocess.Popen[bytes]) -> None:
    """关闭子进程 stdout/stderr 管道（drain 超时后断开 read 端再收割用）。"""
    for stream in (proc.stdout, proc.stderr):
        if stream is not None:
            with contextlib.suppress(OSError, ValueError):
                stream.close()


def _drain_after_kill(proc: subprocess.Popen[bytes]) -> None:
    """杀组后排空管道并收割子进程，有界防吊死（best-effort 不抛）。

    killpg 后管道仍可能被脱离进程组的孙进程（setsid/nohup 双 fork 形态）持有，
    无界 communicate() 会卡死整条重放。超时后关闭管道断开 read 端再收割；
    残留进程由系统回收——单条命令的清理不能吊死整个重建。
    """
    try:
        proc.communicate(timeout=_POST_KILL_DRAIN_SECONDS)
    except subprocess.TimeoutExpired:
        _close_proc_streams(proc)
        with contextlib.suppress(subprocess.TimeoutExpired, OSError):
            proc.wait(timeout=_POST_KILL_DRAIN_SECONDS)


# 复合命令运算符：&& || ; |
_COMPOUND_SPLIT_RE = re.compile(r"&&|\|\||;|\|")

# git 只读子命令（status/diff/log/show/...）→ read_only_probe；其余走 git 重放轨
_GIT_READONLY_SUBCMDS: frozenset[str] = frozenset(
    {
        "status",
        "diff",
        "log",
        "show",
        "rev-parse",
        "ls-files",
        "ls-remote",
        "describe",
        "shortlog",
        "blame",
        "cat-file",
        "name-rev",
        "for-each-ref",
    }
)

# 只读探查类命令（首位命令名）——对文件态零贡献，跳过不重放
_READ_ONLY_CMD_NAMES: frozenset[str] = frozenset(
    {
        "grep",
        "rg",
        "ack",
        "ls",
        "find",
        "echo",
        "cat",
        "head",
        "tail",
        "wc",
        "tree",
        "stat",
        "file",
        "du",
        "df",
        "env",
        "printenv",
        "which",
        "type",
        "command",
        "source",
        ".",
        "true",
        "false",
        "history",
        "jobs",
        "bg",
        "fg",
    }
)


def _classify_bash_command(command: str) -> str:
    """分类 bash 命令：``read_only_probe`` | ``git`` | ``mutating``。

    复合命令（含 ``&&``/``||``/``;``/``|``）按运算符切段逐段判定：全只读才
    ``read_only_probe``；任一段 ``git`` 则 ``git``（保 GIT_AUTHOR_DATE 注入）；
    否则 ``mutating``。单段分类见 ``_classify_single``。

    ``config``/``branch``/``reflog`` 等同名既可读又可写的 git 子命令一律归
    ``git``/``mutating`` 重放，不进只读白名单。
    """
    cmd = command.strip()
    parts = [p for p in _COMPOUND_SPLIT_RE.split(cmd) if p.strip()]
    if len(parts) > 1:
        cats = [_classify_single(p) for p in parts]
        if all(c == "read_only_probe" for c in cats):
            return "read_only_probe"
        if any(c == "git" for c in cats):
            return "git"
        return "mutating"
    return _classify_single(cmd)


def _classify_single(command: str) -> str:
    """分类单条命令段（不含复合运算符）。"""
    cmd = command.strip()
    first_token_match = re.match(r"^\s*(\S+)", cmd)
    first_token = first_token_match.group(1) if first_token_match else ""
    # git：只读子命令 → read_only_probe；其余（add/commit/checkout/...）→ git 或
    # mutating（网络类 clone/push/pull/fetch 归 mutating）
    if _is_git_subcmd(cmd):
        matches = re.findall(r"(?:^|[\s;&|])git\s+([\w-]+)", cmd)
        network_subcmds = {"clone", "push", "pull", "fetch"}
        if matches and all(m in _GIT_READONLY_SUBCMDS for m in matches):
            return "read_only_probe"
        if any(m not in network_subcmds for m in matches):
            return "git"
        return "mutating"
    if first_token in _READ_ONLY_CMD_NAMES:
        # cat 有重定向（>）→ mutating
        if first_token == "cat" and ">" in cmd:
            return "mutating"
        # command -v/-V 只读；其余 command 执行 → mutating
        if first_token == "command" and not re.search(r"\s-[vV]\b", cmd):
            return "mutating"
        return "read_only_probe"
    if first_token == "sed":
        return "read_only_probe" if "-i" not in cmd else "mutating"
    if first_token == "awk":
        return "read_only_probe" if ">" not in cmd else "mutating"
    if first_token in {"[[", "test"}:
        return "read_only_probe"
    # 其余全部 mutating（mkdir/cp/mv/rm/touch/python3/make/cmake/./test_*...）
    return "mutating"


def _is_git_subcmd(cmd: str) -> bool:
    """判断是否含 git 命令（含网络类）。支持复合命令（``git status && ...``）。"""
    return bool(re.search(r"(?:^|[\s;&|])git\s+\w+", cmd))


def _parse_git_author_from_result(result_json: str) -> tuple[str, str]:
    """从 ``git commit`` 的 resultJson 解析 author name/email。"""
    if not result_json:
        return "", ""
    m = re.search(r"Author:\s*(?P<name>[^<]+?)\s*<(?P<email>[^>]+)>", result_json)
    if not m:
        return "", ""
    return m.group("name").strip(), m.group("email").strip()


def _ms_to_git_iso(ms: int) -> str:
    """毫秒 epoch → git 接受的 ``YYYY-MM-DDTHH:MM:SS+00:00`` 形式。"""
    from datetime import datetime

    dt = datetime.fromtimestamp(ms / 1000.0, tz=UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
