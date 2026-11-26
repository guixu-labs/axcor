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

"""检查点重放编排器（配置式重放）。

``TracebackRunner.run(origin_session_id, checkpoint_id, overrides=None)`` 编排一次检查点
重放：检查点在 subagent X 的 start 时刻，重放时按 **message 级**截断 L2（丢弃 X 所在
message 的全部 part 及其之后），把文件态恢复到 fork cwd，让主 agent 收到一个**与正常
pause→resume 完全一致**的通用「继续」信号后**自然续跑**——它从整体任务上下文（stages /
system_prompt / L2 第一条 user message）重新决定派 X、派 Y、派 Z、写 summary。

配置式重放原则（详见 ``agent-traceback-replay-config-dialog-design.md``）：
1. **文件态 / L2 / 检查点链忠实继承**——worker 看到的「检查点时刻」与 origin 一致；
2. **driver 层配置改为用户当前选择**——``overrides`` 覆盖 origin 继承的 model /
   system_prompt / loop.evaluator / loop.max_iterations / criteria.task_goal /
   initial_prompt；reviewer.model 由前端经 ``PUT /api/config`` 单独改（进程级服务）。
   **无 overrides 时行为等价旧的透明重放**（向后兼容）；
3. 首轮 prompt 默认通用「请继续你之前的工作。」——与正常 pause→resume 等价，不注入任何
   重放语义；用户可通过 ``overrides.initial_prompt`` 改写；
4. iteration>1 的 prompt 走 origin 的 Jinja 模板（完整继承 LoopConfig）；
5. 对话史写入 fork 独立存储：OpenCode 用 ``opencode.db``，Claude CLI 用 JSONL。

流程：
1. 查 Checkpoint X；
2. ``SnapshotEngine.restore(X.git_ref)`` 把文件态抽到 ``<fork_root>/cwd/``（独立临时目录，
   ``cwd/`` 是 fork_cwd，其父目录只有 ``cwd/``——防 ``ls ..`` 跨重放泄漏）；
3. 按源 runtime 读取并截断对话史：OpenCode 按 message 截断数据库，Claude CLI 按检查点前
   匹配的 Agent 调用截断 JSONL；最终状态检查点保留完整对话；
4. 把截断对话史写入 fork 独立存储，生成新原生 session ID 并 ``--resume``；
5. OpenCode 把宿主 ``auth.json`` best-effort 拷进 fork；Claude CLI 复制其配置文件；
6. ``base_config = origin.config.model_copy(deep=True)`` 后用 ``overrides`` 逐字段覆盖；
7. 能力端口起新 resume session（带 birth_traceback 溯源 +
   resume + per-fork env_overrides 让 worker 子进程读取 fork 独立的对话史）。

约束：仅对已整体跑完的 session 重放。重放产生独立 session（不复用原
native_session_id），fork 关系在拓扑图并行显示。
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from workflow_controller.config.settings import SessionConfig
from workflow_controller.plugins.agent_traceback.claude_session_seed import (
    detect_replay_runtime,
    seed_claude_session,
)
from workflow_controller.plugins.agent_traceback.l2_reader import (
    find_exclude_message_id,
    find_main_part_id_for_subagent,
    get_last_seed_error,
    seed_prebuilt_session,
)
from workflow_controller.plugins.agent_traceback.snapshot_engine import (
    DEFAULT_SNAPSHOT_TIMEOUT_SECONDS,
    remove_all_remotes,
)
from workflow_controller.transcript.opencode_store import opencode_db_path

logger = logging.getLogger(__name__)

# 透明重放首轮信号：与正常 pause→resume 完全一致的通用「继续」信号。worker 收到此信号
# 后从 L2 + system_prompt + stages + L2 第一条 user message（origin 的 starting_prompt）
# 自然续跑——从检查点时刻的对话史重新决定派 X、派 Y、派 Z、写 summary。
#
# 不注入任何重放专用语义（「re-call X」「磁盘已回到 X 前」「前次产物不在盘上」等）——
# 否则 LLM 会字面执行「重派 X」就停，破坏透明重放。
_TRACEBACK_CONTINUE_PROMPT = "请继续你之前的工作。"

# 重放从检查点续跑，给 max_iterations 留余量避免撞上限截断（用户未显式传 max 时生效）。
_TRACEBACK_MAX_ITER_BUFFER = 10


# ── run 的模块级 helper：前置校验 / fork 目录 / overrides 应用 ───────────


@dataclass
class _ReplayPreflight:
    """run 前置校验通过的产物：origin 解析结果 + 检查点 + 匹配的 runtime。

    runtime 为源与目标一致的最终选择（不一致在前置校验即被拒绝），既充当
    ``base_config.runtime``，也是种子分支的选择依据。
    """

    origin_session_id: str
    cp: Any
    origin: Any
    cwd: str
    native_session_id: str
    runtime: str
    base_config: SessionConfig


@dataclass
class _ForkSeed:
    """截断对话史写入 fork 独立存储后的产物：新 sdk session id + per-fork env。"""

    fork_id: str
    new_native_session_id: str
    fork_env: dict[str, str]


def _checkpoint_reject_reason(cp: Any, origin_session_id: str, checkpoint_id: str) -> str | None:
    """检查点不可重放的原因（None = 可用）。"""
    if cp is None:
        return f"checkpoint {checkpoint_id} not found"
    if cp.session_id != origin_session_id:
        return "checkpoint does not belong to this session"
    if not cp.git_ref:
        return (
            f"checkpoint {checkpoint_id} has no git_ref (status={cp.status}); "
            "file-state not snapshot-able, cannot replay"
        )
    return None


def _make_fork_cwd() -> tuple[Path, Path]:
    """建 per-fork 根目录（布局 ``fork_root/cwd/``，cwd 的父目录只有 ``cwd/``——防
    ``ls ..`` 跨重放泄漏）并返回 ``(fork_root, fork_cwd)``。

    Resolve symlinks（macOS /var → /private/var）：opencode 对 --dir 做 symlink 展开
    算 project worktree，未 resolve 会让 seeded session.directory / L2 路径与 project
    worktree 不一致 → 文件访问被判 external_directory → 弹权限 → headless 无用户 →
    工具调用悬死。Linux 无 /var symlink → no-op。
    """
    fork_root = Path(tempfile.mkdtemp(prefix="workflow-controller-tb-")).resolve()
    fork_cwd = fork_root / "cwd"
    fork_cwd.mkdir(parents=True, exist_ok=True)
    return fork_root, fork_cwd


_REVIEWER_OVERRIDE_KEYS = (
    "reviewer_enabled",
    "reviewer_model",
    "reviewer_provider_index",
    "reviewer_thinking_enabled",
    "reviewer_thinking_budget_tokens",
)


def _apply_reviewer_overrides(base_config: SessionConfig, ov: dict[str, Any]) -> None:
    """reviewer 覆盖（enabled/model/provider_index/thinking_*）落到 fork 的 reviewer 配置。"""
    if base_config.reviewer is None:
        if not any(ov.get(k) is not None for k in _REVIEWER_OVERRIDE_KEYS):
            return
        from workflow_controller.config.settings import SessionReviewerConfig

        # origin 无 reviewer 块时不硬编码默认 model（"claude-opus-4-6"），留空交给下游
        # _ensure_session_snapshots 按默认 reviewer provider/全局配置补齐，避免绕过全局 reviewer.model。
        base_config.reviewer = SessionReviewerConfig(model="")
    if (enabled := ov.get("reviewer_enabled")) is not None:
        base_config.reviewer.enabled = bool(enabled)
    if (rm := ov.get("reviewer_model")) is not None and str(rm) != "":
        base_config.reviewer.model = str(rm)
    if (rpi := ov.get("reviewer_provider_index")) is not None:
        with contextlib.suppress(TypeError, ValueError):
            base_config.reviewer.provider_index = int(rpi)
    if (thinking_enabled := ov.get("reviewer_thinking_enabled")) is not None:
        base_config.reviewer.thinking_mode = "enabled" if bool(thinking_enabled) else "disabled"
    if (budget := ov.get("reviewer_thinking_budget_tokens")) is not None:
        with contextlib.suppress(TypeError, ValueError):
            base_config.reviewer.thinking_budget_tokens = max(1024, int(budget))


def _apply_max_iterations_override(base_config: SessionConfig, ov: dict[str, Any]) -> bool:
    """应用 ``overrides.max_iterations``（origin 的 loop 可能为 None，按需补一棵 LoopConfig）。

    返回是否显式覆盖：显式传则完全用用户值（不加 buffer）；不传则保留 origin+buffer 行为。
    """
    if (mi := ov.get("max_iterations")) is None:
        return False
    try:
        mi_int = int(mi)
    except (TypeError, ValueError):
        return False
    if mi_int <= 0:
        return False
    if base_config.loop is None:
        from workflow_controller.config.settings import LoopConfig

        base_config.loop = LoopConfig()
    base_config.loop.max_iterations = mi_int
    return True


def _apply_overrides(base_config: SessionConfig, overrides: dict[str, Any] | None) -> bool:
    """应用 driver 层 overrides（赢过 origin 继承值）；文件态 / L2 / 检查点链 / stages 仍忠实继承。

    返回 max_iterations 是否被用户显式覆盖（调用方据此决定是否再加 buffer）。
    """
    ov = overrides or {}
    if (cli_wrapper := ov.get("cli_wrapper")) is not None:
        if cli_wrapper not in ("", "cpx"):
            raise ValueError(f"Unsupported CLI wrapper: {cli_wrapper!r}")
        base_config.cli_wrapper = str(cli_wrapper)
    if m := ov.get("model"):
        base_config.model = str(m)
    if (sp := ov.get("system_prompt")) is not None and str(sp) != "":
        base_config.system_prompt = str(sp)
    if (pi := ov.get("provider_index")) is not None:
        with contextlib.suppress(TypeError, ValueError):
            base_config.provider_index = int(pi)
    _apply_reviewer_overrides(base_config, ov)
    if (ip := ov.get("initial_prompt")) is not None and str(ip) != "":
        base_config.initial_prompt = str(ip)
    if (ev := ov.get("evaluator")) is not None and str(ev) != "":
        if base_config.loop is None:
            from workflow_controller.config.settings import LoopConfig

            base_config.loop = LoopConfig()
        base_config.loop.evaluator = str(ev)
    max_iter_overridden = _apply_max_iterations_override(base_config, ov)
    if (rtg := ov.get("reviewer_task_goal")) is not None and str(rtg) != "":
        # reviewer_task_goal 是 per-session 字段（feedback evaluator 据此判完成）。
        base_config.criteria = {**(base_config.criteria or {}), "task_goal": str(rtg)}
    return max_iter_overridden


def _build_birth_traceback(
    pre: _ReplayPreflight,
    fork_root: Path,
    fork_cwd: Path,
) -> dict[str, Any]:
    """构造 birth_traceback：fork 的逻辑归属固定为根 session，嵌套重放沿用根 origin
    的逻辑归属，避免临时 fork cwd 泄漏进项目侧边栏。
    """
    origin = pre.origin
    origin_birth = (
        {}
        if origin is None
        else (
            ((getattr(origin, "plugin_metadata", None) or {}).get("agent_traceback") or {}).get(
                "birth_traceback"
            )
            or {}
        )
    )
    if origin_birth.get("fork_root"):
        logical_sid = origin_birth.get("origin_session_id") or pre.origin_session_id
        logical_cwd = origin_birth.get("origin_cwd") or str(pre.cwd)
        logical_name = origin_birth.get("origin_name") or getattr(origin, "name", "")
    else:
        logical_sid = pre.origin_session_id
        logical_cwd = str(pre.cwd)
        logical_name = getattr(origin, "name", "") if origin is not None else ""

    return {
        "origin_session_id": logical_sid,
        "origin_checkpoint_id": pre.cp.checkpoint_id,
        "forked_at": datetime.now(tz=UTC).isoformat(),
        # fork_root / fork_cwd 持久化到 plugin_metadata：供 API/拓扑图展示 +
        # on_session_delete 读出清理（另有 in-memory map）。
        "fork_root": str(fork_root),
        "fork_cwd": str(fork_cwd),
        "origin_cwd": logical_cwd,
        "origin_name": logical_name,
    }


def _fork_success_result(
    session: Any,
    pre: _ReplayPreflight,
    seed: _ForkSeed,
    fork_root: Path,
    fork_cwd: Path,
) -> dict[str, Any]:
    """记录 fork 日志并构造成功返回（供 API / 拓扑图 / 清理使用）。"""
    logger.info(
        "traceback: forked %s from %s/%s (cwd=%s, resume=%s, xdg=%s)",
        session.id,
        pre.origin_session_id[:12],
        pre.cp.checkpoint_id,
        fork_cwd,
        seed.new_native_session_id,
        fork_cwd / ".workflow-controller",
    )
    return {
        "ok": True,
        "session_id": session.id,
        "fork_cwd": str(fork_cwd),
        "fork_root": str(fork_root),
        "resume_native_session_id": seed.new_native_session_id,
        "origin_checkpoint_id": pre.cp.checkpoint_id,
    }


def _file_size(path: str) -> int:
    """单文件字节数（取不到按 0）。"""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


class TracebackRunner:
    """编排检查点重放。依赖 AgentTracebackPlugin 注入（拿能力端口 + 快照引擎）。"""

    def __init__(self, plugin: Any) -> None:
        self._plugin = plugin
        self._services = plugin._services
        self._work_dir = plugin.work_dir

    @staticmethod
    def _resolve_origin_env(origin_cwd: str) -> dict[str, str] | None:
        """按 origin cwd 解析 origin 的 opencode.db 所在 env（支持嵌套重放）。

        布局约定：每层重放的 opencode.db 在 ``<cwd>/.workflow-controller/opencode/opencode.db``。
        ``.workflow-controller`` 在快照 ignore 列表 → 不进快照 → 每个 fork 的 ``.workflow-controller/`` 全新。故仅
        凭 origin 的 cwd 即可定位其 opencode.db，无需在 session / checkpoint 上存额外字段。

        - origin 是原始 session：``<origin_cwd>/.workflow-controller/opencode/opencode.db`` 不存在 →
          返回 ``None``（用宿主默认 ``~/.local/share/opencode/opencode.db``）。
        - origin 是重放 session（嵌套重放）：该路径存在 → 返回
          ``{"XDG_DATA_HOME": "<origin_cwd>/.workflow-controller"}``，让转录层的 ``opencode_db_path``
          解析到 origin 自己的 fork opencode.db。

        嵌套重放链路（每层 opencode.db 仅凭 cwd 定位）::

            原始     origin_cwd_0     宿主默认 (env=None)
            重放 #1  fork_cwd_1      <fork_cwd_1>/.workflow-controller/opencode/opencode.db
            重放 #2  fork_cwd_2      <fork_cwd_2>/.workflow-controller/opencode/opencode.db
                                 （#2 读 #1 的 L2: env=_resolve_origin_env(fork_cwd_1)）
        """
        if not origin_cwd:
            return None
        candidate = Path(origin_cwd) / ".workflow-controller" / "opencode" / "opencode.db"
        if candidate.is_file():
            return {"XDG_DATA_HOME": str(Path(origin_cwd) / ".workflow-controller")}
        return None

    @staticmethod
    def _init_git_sandbox(
        fork_cwd: Path,
        origin_cwd: str | None = None,
        *,
        git_history_max_mb: int = 0,
        inner_git_captured: bool = False,
    ) -> None:
        """在 fork_cwd 内建可用的 git 工作树，让 worker 的 git 命令可用且有历史可参照。

        分支优先级：**恢复出的内部 git > 复制 origin 当刻 .git > git init**。主路径
        （``_adopt_restored_inner_git``）原样采用检查点 .git；降级分支
        （``_copy_origin_git_history``）复制 origin 当刻 .git 或 git init。降级分支
        统一移除 remote（fork 的 ``.git/config`` 不应指向 origin 的 remote——worker
        ``git fetch`` 会尝试连外部 remote，隔离场景下不允许）并打 baseline commit。

        git 操作的 scope 天然被 git 限定在 fork_cwd 内——为隔离多加一层 git-scope 约束。
        best-effort：git 不可用或复制/commit 失败不阻塞重放（worker 仍可跑，仅 git 命令报错）。
        """
        if shutil.which("git") is None:
            return
        env = TracebackRunner._git_sandbox_env()
        try:
            if TracebackRunner._adopt_restored_inner_git(fork_cwd, env, inner_git_captured):
                return  # 检查点态原样，无 baseline commit
            copied = TracebackRunner._copy_origin_git_history(
                fork_cwd, origin_cwd, git_history_max_mb
            )
            if not copied:
                # origin 无 .git（不是 git 仓）/ 超阈值跳过 → 建一棵无历史工作树
                r = subprocess.run(
                    ["git", "init"],
                    cwd=str(fork_cwd),
                    capture_output=True,
                    text=True,
                    env=env,
                )
                if r.returncode != 0:
                    return
            # copy 后移除 remote：fork 不应带 origin 的 remote，防 worker git fetch
            # 误连外部 remote。best-effort：rm 失败不阻塞。
            if copied:
                remove_all_remotes(fork_cwd / ".git")
            TracebackRunner._commit_git_baseline(fork_cwd, env)
        except Exception:
            logger.debug("traceback: git sandbox init failed (non-fatal)", exc_info=True)

    @staticmethod
    def _git_sandbox_env() -> dict[str, str]:
        """继承宿主 env（含 PATH / HOME 等），再叠加 git author 身份。

        避免子进程找不到 git 或读不到全局 git config。与 SnapshotEngine._git_env 一致。
        """
        env = dict(os.environ)
        env.update(
            {
                "GIT_AUTHOR_NAME": "workflow-controller-tb",
                "GIT_AUTHOR_EMAIL": "workflow-controller-tb@local",
                "GIT_COMMITTER_NAME": "workflow-controller-tb",
                "GIT_COMMITTER_EMAIL": "workflow-controller-tb@local",
            }
        )
        return env

    @staticmethod
    def _adopt_restored_inner_git(
        fork_cwd: Path,
        env: dict[str, str],
        inner_git_captured: bool,
    ) -> bool:
        """处理恢复出的 fork_cwd/.git：可原样采用（检查点态）返回 True，否则走降级。

        - ``.git`` 是文件（linked worktree 的 gitdir 指针，指向 origin 的绝对路径，
          恢复后必失效）→ 删除，走降级分支。
        - 主路径（``inner_git_captured=True`` 且 ``.git`` 是目录）：restore 已从检查点
          带出该时刻的完整 ``.git``（objects/refs/index/logs），sanity 通过后仅移除
          remote，不打 baseline commit——index 已随 ``.git`` 恢复，``git status``/
          ``git log`` 与检查点时刻一致。sanity 用 ``rev-parse --git-dir`` 而非
          ``HEAD``：unborn HEAD（首个 commit 前）是合法状态，用 HEAD 会误判为未捕获、
          降级复制 origin 当刻 ``.git``，泄漏未来历史。sanity 失败视为未捕获，删掉
          走降级。
        - 未捕获（``inner_git_captured=False``）/ ``.git`` 不存在 → 走降级。
        """
        fork_git = fork_cwd / ".git"
        if fork_git.is_file():
            fork_git.unlink(missing_ok=True)
        if not inner_git_captured or not fork_git.is_dir():
            return False
        r = subprocess.run(
            ["git", "rev-parse", "--git-dir"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
            env=env,
        )
        if r.returncode != 0:
            shutil.rmtree(fork_git, ignore_errors=True)  # sanity 失败 → 降级
            return False
        remove_all_remotes(fork_git)
        # .git/config 若带 core.worktree（指向 origin 绝对路径），恢复后
        # 必失效——git 会去那个不存在的 worktree。best-effort 剥除
        # （未设置时 --unset 退出码非 0，静默忽略）。
        subprocess.run(
            ["git", "config", "--unset", "core.worktree"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
            env=env,
        )
        return True

    @staticmethod
    def _copy_origin_git_history(
        fork_cwd: Path,
        origin_cwd: str | None,
        git_history_max_mb: int,
    ) -> bool:
        """降级分支（旧检查点 / sanity 失败 / origin_cwd 给出）：体积阈值内复制 origin 当刻 ``.git``。

        origin 不是 git 仓 / 超阈值 → 返回 False（调用方 ``git init`` 空仓 + baseline）。
        体积阈值仅此分支生效，默认 0 = 不限制。
        """
        if not origin_cwd:
            return False
        src_git = Path(origin_cwd) / ".git"
        if not src_git.is_dir():
            return False
        if git_history_max_mb > 0:
            # 体积检测：超阈值跳过 copy，退化为 git init。默认 0 = 不限制（用户显式设
            # traceback.git_history_max_mb 才生效），避免静默改变行为——worker git log
            # 为空时会做不同决策，用户应知晓。
            size_mb = TracebackRunner._dir_size_mb(src_git)
            if size_mb > git_history_max_mb:
                logger.info(
                    "traceback: origin .git %.1fMB > %dMB threshold → skip "
                    "history copy, using git init (no history); set "
                    "traceback.git_history_max_mb=0 to disable the limit",
                    size_mb,
                    git_history_max_mb,
                )
                return False
        return TracebackRunner._copy_git_dir(src_git, fork_cwd / ".git")

    @staticmethod
    def _commit_git_baseline(fork_cwd: Path, env: dict[str, str]) -> None:
        """降级分支才打 baseline：文件态(T) 与 origin HEAD(T_final) 本就错位。

        baseline 让 ``git status`` 干净、历史连续。``--allow-empty`` 兼容空目录或
        文件态与 origin HEAD 完全一致的情形。
        """
        subprocess.run(
            ["git", "add", "-A"],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
            env=env,
        )
        subprocess.run(
            [
                "git",
                "commit",
                "-m",
                "workflow-controller traceback sandbox baseline",
                "--allow-empty",
            ],
            cwd=str(fork_cwd),
            capture_output=True,
            text=True,
            env=env,
        )

    @staticmethod
    def _dir_size_mb(path: Path) -> float:
        """Best-effort directory size in MB (walks the tree, sums file sizes)."""
        try:
            total = 0
            for root, _dirs, files in os.walk(str(path)):
                total += sum(_file_size(os.path.join(root, f)) for f in files)
            return total / (1024 * 1024)
        except OSError:
            return 0.0

    @staticmethod
    def _copy_git_dir(src_git: Path, dest_git: Path) -> bool:
        """复制 origin 的 .git（含完整历史：objects/refs/HEAD 等）。

        用 copytree(copy_function=shutil.copy) 走 file copy（而非 copy2 带 metadata）
        ——更快且对 .git 内大量小文件足够。返回是否成功。
        """
        try:
            shutil.copytree(src_git, dest_git, copy_function=shutil.copy)
            return True
        except (OSError, ValueError):
            logger.debug(
                "traceback: copy origin .git failed (non-fatal)",
                exc_info=True,
            )
            return False

    @staticmethod
    def _write_permission_config(fork_cwd: Path) -> None:
        """在 fork cwd 写 ``.opencode/opencode.json``，设 ``external_directory: allow``。

        seed 的 session 行从 origin 克隆，``project_id`` 继承自 origin → 指向 origin
        的 project worktree（如 ``/Users/.../project``），而非 fork cwd。opencode 的权限
        系统据此判断 fork cwd 内的文件访问为 ``external_directory`` → 默认 action ``ask``
        → 等用户确认 → headless 模式无用户 → **subagent 工具调用悬死**（工具 status 永远
        停在 running，无 completed 事件）。

        设 ``external_directory: allow`` 绕过权限询问。安全：fork cwd 是隔离 tempdir
        （快照恢复目标 + git 软沙箱 + 快照隔离），worker 无法逃逸到 origin 目录。
        resolve fork_root（上方）修了 subagent 的 tool call 路径不匹配；本配置修 main
        agent 继承的 project_id 不匹配——两者结合消除悬死。

        opencode 合并多个配置源（``~/.config/opencode/opencode.json`` 全局 + ``<cwd>/
        .opencode/opencode.json`` 项目级），本文件只加 permission 段，不影响全局 provider
        配置。best-effort：写入失败不阻塞重放。
        """
        config_path = fork_cwd / ".opencode" / "opencode.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        # 若已有项目级 opencode.json（用户手动放或快照恢复带过来），读出合并；
        # 否则新建。只覆盖/设置 permission 段，保留其他字段（provider/model/agent 等）。
        existing: dict[str, Any] = {}
        if config_path.is_file():
            try:
                existing = json.loads(config_path.read_text())
            except (OSError, json.JSONDecodeError):
                existing = {}
        existing["$schema"] = "https://opencode.ai/config.json"
        existing["permission"] = {"external_directory": "allow"}
        try:
            config_path.write_text(json.dumps(existing, indent=2))
        except OSError:
            logger.debug(
                "traceback: write permission config failed (non-fatal)",
                exc_info=True,
            )

    @staticmethod
    def _seed_host_auth(fork_cwd: Path) -> None:
        """Best-effort 把宿主 ``auth.json`` 拷进 fork 的 XDG opencode 目录。

        seed 只拷 opencode.db 不带 ``auth.json``，fork 无凭证时需鉴权的 provider 不可用。
        auth 是宿主凭证（非 session 范围数据），须独立继承。best-effort：缺失 / 复制失败
        不阻塞（内联 apiKey 的 provider 仍可用）。

        始终从宿主拷（不从 origin fork），保证嵌套重放时每层 fork 都拿到当前宿主凭证。
        """
        try:
            src = opencode_db_path().parent / "auth.json"
            if not src.is_file():
                return
            dest_dir = fork_cwd / ".workflow-controller" / "opencode"
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / "auth.json"
            # 仅当目标不存在或内容不同才拷，避免无谓 IO + 覆盖用户手改。
            need_copy = True
            if dest.is_file():
                try:
                    need_copy = src.read_bytes() != dest.read_bytes()
                except OSError:
                    need_copy = True
            if need_copy:
                shutil.copy2(src, dest)
                logger.info("traceback: seeded host auth.json into fork (%s)", dest.parent)
        except Exception:
            logger.debug(
                "traceback: seed host auth.json failed (non-fatal)",
                exc_info=True,
            )

    @staticmethod
    def _build_fork_config(
        pre: _ReplayPreflight,
        seed: _ForkSeed,
        fork_cwd: Path,
        overrides: dict[str, Any] | None,
    ) -> SessionConfig:
        """构造 fork SessionConfig：完整继承 origin + 重放必需字段 + overrides。"""
        base_config = pre.base_config
        # 首轮 prompt 用通用续跑信号「请继续你之前的工作。」，不注入重放专用语义；
        # 用户可通过 overrides.initial_prompt 改写。
        base_config.name = f"replay-{seed.fork_id[:8]}"
        base_config.role = "worker"
        base_config.cwd = str(fork_cwd)
        base_config.initial_prompt = _TRACEBACK_CONTINUE_PROMPT
        base_config.runtime = pre.runtime
        # 用户级实验 fork 不进源 run 账（谱系经 birth_traceback 表达）：置空归属，
        # 由 session 创建路径生成独立隐式 run。
        base_config.run_id = None
        # env_overrides：driver 子进程读取 fork 独立的 OpenCode DB 或 Claude JSONL。
        base_config.env_overrides = dict(seed.fork_env)
        # 应用 driver 层 overrides（赢过 origin 继承值）；文件态 / L2 / 检查点链 /
        # stages 仍忠实继承。用户未显式传 max 时，在 origin max 基础上加 buffer
        # 避免撞上限截断。
        max_iter_overridden = _apply_overrides(base_config, overrides)
        if base_config.loop and not max_iter_overridden:
            base_config.loop.max_iterations = (
                int(base_config.loop.max_iterations) + _TRACEBACK_MAX_ITER_BUFFER
            )
        return base_config

    async def run(
        self,
        origin_session_id: str,
        checkpoint_id: str,
        *,
        runtime: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """从检查点起重放，返回新 session 信息。

        重文件态 / L2 重建等同步阻塞调用一律经 _offload 下放到线程池，不阻塞事件循环，
        保证重放期间 SSE 持续推送、其它 session 的实时更新不被冻住。

        Args:
            origin_session_id: 源 session id。
            checkpoint_id: 检查点 id。
            runtime: 可选的 driver runtime 覆盖（claude_sdk / claude_cli / opencode）。
            overrides: driver 层配置覆盖，逐字段赢过 origin 继承值。支持键：
                ``model`` / ``system_prompt`` / ``evaluator`` / ``max_iterations`` /
                ``reviewer_task_goal``（写到 ``criteria["task_goal"]``）/ ``initial_prompt``；
                以及 reviewer 覆盖（``reviewer_enabled`` / ``reviewer_model`` /
                ``reviewer_provider_index`` / ``reviewer_thinking_enabled`` /
                ``reviewer_thinking_budget_tokens``），落到 fork session 的
                ``reviewer`` 配置。

        Returns ``{"ok": bool, "session_id": str, "error": str}``。
        """
        # 1+2. 前置校验：查 Checkpoint / 解析 origin session / runtime 匹配
        pre = await self._preflight(origin_session_id, checkpoint_id, runtime)
        if isinstance(pre, dict):
            return pre
        # 3-7. 建 fork 目录 → restore + git 软沙箱 + 截断种子对话史 → fork config →
        #      起新 session；失败（含异常）时 finally 清理 fork_root
        return await self._execute_replay(pre, overrides)

    async def _preflight(
        self,
        origin_session_id: str,
        checkpoint_id: str,
        runtime: str | None,
    ) -> _ReplayPreflight | dict[str, Any]:
        """重放前置校验；不可重放时返回 error dict（含原因文案）。"""
        # 1. 查 Checkpoint（插件 own 的 checkpoints 表，经 CheckpointStore）
        cp = self._plugin.get_checkpoint(origin_session_id, checkpoint_id)
        reason = _checkpoint_reject_reason(cp, origin_session_id, checkpoint_id)
        if reason is not None:
            return {"ok": False, "error": reason}
        # 2. 解析 origin session（取 cwd + native_session_id + 完整 SessionConfig）
        ctx = await self._resolve_origin_context(origin_session_id)
        if ctx is None:
            return {"ok": False, "error": "origin session not found"}
        origin, cwd, native_session_id, origin_runtime, base_config = ctx
        if not native_session_id:
            return {
                "ok": False,
                "error": "origin session has no native_session_id; cannot replay its conversation",
            }

        # 仅对已整体跑完的 session 重放
        if origin is not None and origin.loop_status == "yes" and origin.state.value == "running":
            return {
                "ok": False,
                "error": "origin session still running — traceback only on finished sessions",
            }
        rt = await self._resolve_replay_runtime(
            native_session_id,
            cwd,
            origin_runtime,
            base_config,
            runtime,
        )
        if isinstance(rt, dict):
            return rt
        return _ReplayPreflight(
            origin_session_id=origin_session_id,
            cp=cp,
            origin=origin,
            cwd=cwd,
            native_session_id=native_session_id,
            runtime=rt,
            base_config=base_config,
        )

    async def _resolve_origin_context(
        self, origin_session_id: str
    ) -> tuple[Any, str, str | None, str | None, SessionConfig] | None:
        """解析 origin：pool 命中 → 深拷贝 config；重启后 → 读 roster 行反序列化。

        返回 ``(origin, cwd, native_session_id, origin_runtime, base_config)``；找不到返回 None。
        """
        origin = self._services.find_session(origin_session_id)
        if origin is not None:
            cwd = getattr(origin.config, "cwd", "") or str(self._work_dir)
            native_session_id = origin.native_session_id
            origin_runtime = getattr(origin.config, "runtime", None)
            # deep copy origin config 作为基底（完整继承 loop/system_prompt/stages/criteria/
            # model/permission_mode/allowed_tools/max_turns/max_budget_usd/skills_path/
            # agents_path/extra_dirs/hooks/handoff/continuation_prompt_path 等）
            base_config = origin.config.model_copy(deep=True)
        else:
            # 重启后 pool 无此 session → 读持久化记录 + 反序列化 config
            sess_row = await self._services.load_roster_session(origin_session_id)
            if sess_row is None:
                return None
            try:
                cfg_dict = json.loads(sess_row.get("config_json") or "{}")
            except json.JSONDecodeError:
                cfg_dict = {}
            cwd = cfg_dict.get("cwd", "") or str(self._work_dir)
            native_session_id = sess_row.get("native_session_id")
            origin_runtime = cfg_dict.get("runtime")
            # 从持久化 config 反序列化 base_config（完整继承 origin 配置）
            try:
                base_config = SessionConfig(**cfg_dict)
            except Exception:
                base_config = SessionConfig(name="origin", cwd=cwd)
        return origin, cwd, native_session_id, origin_runtime, base_config

    async def _resolve_replay_runtime(
        self,
        native_session_id: str,
        cwd: str,
        origin_runtime: str | None,
        base_config: SessionConfig,
        runtime: str | None,
    ) -> str | dict[str, Any]:
        """探测源 runtime 并解析目标 runtime；不支持 / 不匹配返回 error dict。

        成功时源与目标 runtime 必一致（重放不换 runtime），直接返回该值。
        """
        # detect_replay_runtime 内含同步 FS 探测（is_file / glob 扫描），与其它阻塞 IO
        # 一样 offload 到线程池，避免重放期间阻塞事件循环（SSE 推送依赖它）。
        # _offload 返回 Any，此处按 detect_replay_runtime 的实际返回类型收窄为 str。
        source_runtime: str = await self._offload(
            detect_replay_runtime,
            native_session_id=native_session_id,
            cwd=cwd,
            configured_runtime=origin_runtime,
            default_runtime=self._services.default_runtime() or "claude_sdk",
            source_config_dir=(base_config.env_overrides or {}).get("CLAUDE_CONFIG_DIR"),
        )
        chosen_runtime: str = self._services.resolve_runtime(runtime or source_runtime)
        if source_runtime not in ("opencode", "claude_cli"):
            return {
                "ok": False,
                "error": f"traceback does not support {source_runtime} source sessions",
            }
        if chosen_runtime != source_runtime:
            # WORKFLOW_CONTROLLER_DRIVER 环境变量优先于显式入参（driver/factory.py:resolve_runtime），
            # 若其导致不匹配，用户在弹窗里选什么都没用——报错须点明来源，否则修复指引失效。
            env_driver = os.environ.get("WORKFLOW_CONTROLLER_DRIVER")
            hint = f" (WORKFLOW_CONTROLLER_DRIVER={env_driver} is set)" if env_driver else ""
            return {
                "ok": False,
                "error": (
                    f"Cannot replay {source_runtime} conversation with {chosen_runtime}; "
                    f"select the source runtime{hint}"
                ),
            }
        return source_runtime

    async def _execute_replay(
        self, pre: _ReplayPreflight, overrides: dict[str, Any] | None
    ) -> dict[str, Any]:
        """执行重放主体；失败（含异常）时 finally 清理 fork_root。"""
        # 3. restore 文件态 → fork cwd（per-fork 根目录布局：fork_root/cwd/）
        engine = await self._offload(self._plugin.resolve_engine, pre.cwd)
        fork_root, fork_cwd = _make_fork_cwd()
        # session 生命周期绑定清理：注册成功后把 fork_root 登记到 plugin._fork_roots，
        # 由 on_session_delete 在 session 删除时 rmtree；崩溃残留由启动期 reaper 清理。
        fork_registered = False
        try:
            # 3b+4. git 软沙箱 + 读 L2 截断种子对话史（claude_cli / opencode 两路）
            seed = await self._restore_and_seed(pre, engine, fork_cwd)
            if isinstance(seed, dict):
                return seed
            # 6. 构造 SessionConfig（完整继承 origin，覆盖重放必需字段 + 应用 overrides）
            config = self._build_fork_config(pre, seed, fork_cwd, overrides)
            birth = _build_birth_traceback(pre, fork_root, fork_cwd)
            # hook config（worker 的 PreToolUse/Stop 要能接上）+ opencode 权限配置
            await self._prepare_fork_runtime(pre, fork_cwd)
            # 7. 起新 session（带 plugin_metadata 溯源 + resume + per-fork env_overrides +
            #    iteration 对齐）。inherit origin 的 session 级 overlay（subagent hint /
            #    全局指令），让首轮 query 前到位。initial_current_iteration = cp.iteration：
            #    让 Jinja {{ iteration }} 与 LLM 自感知进度对齐。
            session = await self._services.start_resumed_session(
                config,
                resume_native_session_id=seed.new_native_session_id,
                plugin_metadata={"agent_traceback": {"birth_traceback": birth}},
                start_action="traceback_session_started",
                inherit_overlays_from=pre.origin_session_id,
                initial_current_iteration=int(pre.cp.iteration) if pre.cp.iteration else None,
            )
            # session 注册成功 → fork_root 清理责任移交 on_session_delete。
            self._plugin.register_fork_root(session.id, str(fork_root))
            fork_registered = True
            return _fork_success_result(session, pre, seed, fork_root, fork_cwd)
        finally:
            if not fork_registered:
                await self._offload(
                    shutil.rmtree,
                    fork_root,
                    ignore_errors=True,
                )

    async def _restore_and_seed(
        self, pre: _ReplayPreflight, engine: Any, fork_cwd: Path
    ) -> _ForkSeed | dict[str, Any]:
        """restore 文件态 + git 软沙箱 + 按源 runtime 截断种子对话史。"""
        restore_result = await self._offload(engine.restore, pre.cp.git_ref, fork_cwd)
        if not restore_result.ok:
            return {
                "ok": False,
                "error": f"restore snapshot {pre.cp.git_ref[:8]} to {fork_cwd} failed",
            }
        # 3b. git 软沙箱：恢复出的内部 git（检查点时刻原样）> 复制 origin 当刻
        #     .git + baseline（降级）> git init。git scope 天然限定在 fork_cwd 内。
        #     best-effort。
        git_max_mb = int(self._plugin.cfg("git_history_max_mb", 0) or 0)
        await self._offload(
            self._init_git_sandbox,
            fork_cwd,
            pre.cwd,
            git_history_max_mb=git_max_mb,
            inner_git_captured=restore_result.inner_git_captured,
        )
        # 4. 读 L2 + 定位截断点（透明重放：按 message 级截断，丢弃 X 所在 message 全部
        #    part 及其之后，消除「说了要派 X 但没调用」的矛盾状态）。最终状态检查点
        #    （trigger=session_complete / 无 subagent）不截断，保留完整对话史。
        fork_id = uuid4().hex[:12]
        is_final = not pre.cp.agent_id or pre.cp.trigger == "session_complete"
        if pre.runtime == "claude_cli":
            return await self._seed_claude_cli_fork(pre, fork_cwd, fork_id, is_final)
        return await self._seed_opencode_fork(pre, fork_cwd, fork_id, is_final)

    async def _seed_claude_cli_fork(
        self,
        pre: _ReplayPreflight,
        fork_cwd: Path,
        fork_id: str,
        is_final: bool,
    ) -> _ForkSeed | dict[str, Any]:
        """Claude CLI 源：按检查点截断 JSONL 对话史，写入 fork 独立配置目录。"""
        new_native_session_id = str(uuid4())
        claude_config, seed_error = await self._offload(
            seed_claude_session,
            source_session_id=pre.native_session_id,
            source_cwd=pre.cwd,
            new_session_id=new_native_session_id,
            fork_cwd=fork_cwd,
            checkpoint_at=pre.cp.created_at,
            agent_type=pre.cp.agent_type,
            final_checkpoint=is_final,
            source_config_dir=(pre.base_config.env_overrides or {}).get("CLAUDE_CONFIG_DIR"),
            snapshot_timeout_seconds=float(
                self._plugin.cfg(
                    "snapshot_timeout_seconds",
                    DEFAULT_SNAPSHOT_TIMEOUT_SECONDS,
                )
            ),
        )
        if claude_config is None:
            return {"ok": False, "error": seed_error}
        return _ForkSeed(
            fork_id=fork_id,
            new_native_session_id=new_native_session_id,
            fork_env={"CLAUDE_CONFIG_DIR": str(claude_config)},
        )

    async def _seed_opencode_fork(
        self,
        pre: _ReplayPreflight,
        fork_cwd: Path,
        fork_id: str,
        is_final: bool,
    ) -> _ForkSeed | dict[str, Any]:
        """OpenCode 源：按 message 级截断 opencode.db 对话史，种子进 fork 独立 db。"""
        origin_env = self._resolve_origin_env(pre.cwd)
        exclude_part_id, exclude_message_id = await self._find_exclude_ids(
            pre, is_final, origin_env
        )
        new_native_session_id = f"ses_tb_{fork_id}"
        fork_env = {"XDG_DATA_HOME": str(fork_cwd / ".workflow-controller")}
        seeded = await self._offload(
            seed_prebuilt_session,
            source_session_id=pre.native_session_id,
            new_session_id=new_native_session_id,
            up_to_part_id=None,
            title=f"[traceback] {pre.origin_session_id[:8]} {pre.cp.checkpoint_id}",
            env=fork_env,
            source_env=origin_env,
            exclude_part_id=exclude_part_id,
            exclude_message_id=exclude_message_id,
            directory=str(fork_cwd),
        )
        if not seeded:
            return {
                "ok": False,
                "error": (
                    f"seed_prebuilt_session failed: "
                    f"{get_last_seed_error() or 'source OpenCode conversation unavailable'}"
                ),
            }
        await self._offload(self._seed_host_auth, fork_cwd)
        return _ForkSeed(
            fork_id=fork_id, new_native_session_id=new_native_session_id, fork_env=fork_env
        )

    async def _find_exclude_ids(
        self, pre: _ReplayPreflight, is_final: bool, origin_env: dict[str, str] | None
    ) -> tuple[str | None, str | None]:
        """定位 message 级截断点，返回 ``(exclude_part_id, exclude_message_id)``。"""
        if is_final:
            return None, None
        task_pid = pre.cp.main_part_id or await self._offload(
            find_main_part_id_for_subagent,
            pre.native_session_id,
            pre.cp.agent_id,
            origin_env,
        )
        if not task_pid:
            return None, None
        exclude_message_id = await self._offload(
            find_exclude_message_id,
            pre.native_session_id,
            task_pid,
            origin_env,
        )
        return (None if exclude_message_id else task_pid), exclude_message_id

    async def _prepare_fork_runtime(self, pre: _ReplayPreflight, fork_cwd: Path) -> None:
        """写 hook config；opencode 源再写权限配置（防工具调用悬死）。"""
        try:
            await self._offload(self._services.write_hook_config, fork_cwd)
        except Exception:
            logger.debug("traceback: hook config write failed (non-fatal)", exc_info=True)
        # 写 .opencode/opencode.json 设 external_directory: allow：seeded session 的
        # project_id 继承自 origin 指向 origin worktree，fork cwd 内文件访问会被判
        # external_directory → 弹权限 → headless 无用户 → 工具调用悬死。allow 绕过。
        if pre.runtime == "opencode":
            await self._offload(self._write_permission_config, fork_cwd)

    async def _offload(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        """把同步阻塞调用丢到线程池，避免阻塞事件循环（SSE 实时推送依赖它）。"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))
