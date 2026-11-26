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

"""OpenCodeDriver — spawns the `opencode` CLI subprocess and parses its
`run --format json` event stream into Workflow Controller Events.

Stage 2 thin slice: mirrors ClaudeCLIDriver's subprocess pattern; translates
opencode JSON events. Hook routing (control plane) is deferred to Stage 3.

Spike-derived facts:
- prompt is a positional arg to `opencode run`; stdin must be CLOSED (non-TTY
  hang), same pattern as ClaudeCLIDriver.
- opencode's `tool_use` event combines the call AND its result into one
  `part` (state.input + state.output); we split it into TOOL_USE + TOOL_RESULT.
- env: opencode's @ai-sdk/anthropic reads ANTHROPIC_API_KEY only, so we alias
  ANTHROPIC_AUTH_TOKEN -> ANTHROPIC_API_KEY when the key is absent.

opencode v2 行为适配（实测 v2.0.20）：
- `run` 默认复用**后台服务**（按服务启动时的环境装配配置）——每次调用传入的
  `OPENCODE_CONFIG_CONTENT` 等环境注入被整体忽略。必须 `--standalone` 起私有
  服务，配置注入（agent/permission/mcp）才对本调用生效；进程组 kill 也因此
  重新与运行对齐（后台服务模式下杀 CLI 杀不掉在途运行）。
- 审批旗标用 `--auto`（自动批准未被显式拒绝的操作）：`--dangerously-skip-
  permissions` 在 v2 下对显式 deny 也跳过、或悬挂在无终端的确认上；
  `--auto` 保留显式 deny 的强制性（restrict 沙箱依赖）。
- `--dir` 旗标已被移除，工作目录经子进程 cwd 传入。
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import logging
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

from workflow_controller.core._subagent import truncate_subagent_content
from workflow_controller.driver.cli_base import (
    CLIDriverBase,
    QueryOptions,
    drain_proc_stderr,
    error_end_events,
    iter_proc_line_batches,
    kill_proc_group,
    spawn_failure_events,
)
from workflow_controller.driver.opencode_gate import WORKER_AGENT_NAME, build_config_content
from workflow_controller.driver.opencode_session import (
    list_descendant_sessions,
    lookup_subagent_output_tokens,
    read_subagent_messages,
    read_subagent_parts,
    read_subagent_type,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from workflow_controller.driver.hooks import DriverHooks
    from workflow_controller.driver.opencode_gate import PerQueryGate
    from workflow_controller.session.events import Event

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=8)
def _detect_run_flags(bin_path: str) -> frozenset[str]:
    """探测 `opencode run` 支持的旗标集（版本适配；进程内缓存一次）。

    v1/v2 旗标面不同（--standalone/--auto 为 v2 新增、--dir 已移除）：按
    help 文本实测决定用哪组旗标，探测失败按空集回退保守面。
    """
    try:
        out = subprocess.run(
            [bin_path, "run", "--help"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        return frozenset(out.stdout.split())
    except Exception:
        return frozenset()


# ── opencode sqlite DB helpers ────────────────────────────────────────
# opencode-DB knowledge lives here; the orchestrator/API layer goes through
# these / update_session_title instead of touching opencode's storage directly.

_OPENCODE_DB_PATH: Path | None = None


def _opencode_db_path_from_bin(opencode_bin: str) -> Path | None:
    """问 opencode 二进制要它的 sqlite DB 路径；失败或路径不存在返回 None。"""
    try:
        result = subprocess.run(
            [opencode_bin, "db", "path"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            p = Path(result.stdout.strip())
            if p.exists():
                return p
    except Exception:
        logger.debug("opencode db path 探测失败，回落 XDG 路径")
    return None


def _resolve_opencode_db_path() -> Path | None:
    """Resolve the opencode sqlite DB path (cached). None if not found."""
    global _OPENCODE_DB_PATH
    if _OPENCODE_DB_PATH is not None:
        return _OPENCODE_DB_PATH
    opencode_bin = shutil.which("opencode")
    if opencode_bin:
        p = _opencode_db_path_from_bin(opencode_bin)
        if p is not None:
            _OPENCODE_DB_PATH = p
            return p
    xdg = os.environ.get("XDG_DATA_HOME", "")
    if xdg:
        p = Path(xdg) / "opencode" / "opencode.db"
        if p.exists():
            _OPENCODE_DB_PATH = p
            return p
    p = Path.home() / ".local" / "share" / "opencode" / "opencode.db"
    if p.exists():
        _OPENCODE_DB_PATH = p
        return p
    return None


class _RunOverlays(NamedTuple):
    """hooks 侧的 per-query 注入物（overlay 提示、全局指令与文件变更插件）。"""

    subagent_hints: dict[str, list[str]] | None
    instructions: list[str] | None
    auto_compaction: bool
    agents_dir: Path | None
    provenance_plugin: str | None
    hook_url: str


class _PreparedRun(NamedTuple):
    """一次 opencode run 的 spawn 输入与随行资源（gate 需在 query 收尾时停止）。"""

    args: list[str]
    proc_env: dict[str, str]
    cwd: str | None
    session_id: str
    iteration: int
    gate: PerQueryGate | None


def _collect_run_overlays(hooks: DriverHooks | None, cwd: str | None) -> _RunOverlays:
    """归集 hooks 侧的 per-query 注入（overlay 提示 / 全局指令 / 文件变更插件）。"""
    # overlay (opencode-only): one read of get_overlays(); empty → no bake.
    subagent_hints: dict[str, list[str]] | None = None
    session_instructions: list[str] | None = None
    if hooks is not None and hooks.get_overlays is not None:
        overlays = hooks.get_overlays() or {}
        subagent_hints = overlays.get("subagent_hints") or None
        session_instructions = overlays.get("instructions") or None
    auto_compaction = hooks.auto_compaction if hooks is not None else False
    agents_dir = Path(cwd) / ".opencode" / "agents" if cwd else None
    provenance_plugin: str | None = None
    hook_url = ""
    file_changes_active = (
        hooks is not None
        and hooks.on_post_tool_use is not None
        and hooks.file_changes_enabled is not None
        and hooks.file_changes_enabled()
    )
    if hooks is not None and file_changes_active:
        provenance_plugin = str(Path(__file__).with_name("opencode_file_changes.ts"))
        hook_url = hooks.file_change_hook_url or "http://localhost:18765"
    return _RunOverlays(
        subagent_hints=subagent_hints,
        instructions=session_instructions,
        auto_compaction=auto_compaction,
        agents_dir=agents_dir,
        provenance_plugin=provenance_plugin,
        hook_url=hook_url,
    )


class OpenCodeDriver(CLIDriverBase):
    """Runs the opencode agent via the `opencode run` CLI (subprocess)."""

    def __init__(
        self,
        *,
        permission_mode: str = "bypassPermissions",
        add_dirs: list[str] | None = None,
        resume: str | None = None,
        fork_session: bool = False,
        opencode_bin: str = "opencode",
        command_prefix: list[str] | None = None,
    ) -> None:
        super().__init__(permission_mode=permission_mode)
        # add_dirs / fork_session accepted for constructor parity with
        # ClaudeCLIDriver: reviewer_runner passes add_dirs and breather's
        # context probe passes fork_session=True via create_driver(**),
        # so __init__ must accept both to avoid a TypeError. Neither is emitted
        # here — opencode `run` has no multi-dir flag (子进程 cwd 即项目根), and
        # the probe is dead on opencode: breather reads recorded usage events
        # first (check_recorded), which carry real tokens, so the probe never
        # fires for an opencode session. (claude_cli emits --fork-session for
        # the cc path.)
        self._add_dirs: list[str] = add_dirs or []
        self._resume: str | None = resume
        self._fork_session: bool = fork_session
        self._opencode_bin = opencode_bin
        self._command_prefix: list[str] = command_prefix or []
        self._last_assistant_text = ""

    async def stop(self) -> None:
        self._stop_event.set()
        if self._proc is not None:
            proc = self._proc
            kill_proc_group(proc)
            # 不手动 feed_eof：进程被 kill 后 transport 仍可能投递内核里已缓冲的
            # 尾部数据，提前 feed_eof 会让 StreamReader.feed_data 抛
            # "feed_data after feed_eof" 并丢弃尾部输出。等待进程退出即可——
            # 内核随之关闭管道，stdout/stderr reader 自然收到 EOF，query() 解除阻塞。
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            stderr_task = self._stderr_task
            if stderr_task is not None:
                stderr_task.cancel()
                with contextlib.suppress(BaseException):
                    await stderr_task

    def update_session_title(self, sdk_session_id: str, name: str) -> bool:
        """Overrides ``Driver.update_session_title``: UPDATEs opencode's sqlite DB
        directly (orchestrator must not ``sqlite3.connect`` here). Returns False
        when the DB / sdk_id is missing or the row isn't written yet — rowcount
        folds the old existence SELECT into the UPDATE. Never raises.
        """
        if not sdk_session_id or not name:
            return False
        db_path = _resolve_opencode_db_path()
        if not db_path:
            return False
        import sqlite3

        try:
            # closing() ensures conn.close() on both paths (sqlite3's own
            # __exit__ only commits/rolls back, no close).
            with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
                cur = conn.execute(
                    "UPDATE session SET title = ? WHERE id = ?",
                    (name, sdk_session_id),
                )
                conn.commit()
                return cur.rowcount > 0
        except Exception:
            logger.warning(
                "update_session_title failed for %s",
                sdk_session_id[:12],
                exc_info=True,
            )
            return False

    def _query_events(self, prompt: str, opts: QueryOptions) -> AsyncIterator[Event]:
        """返回 opencode run 查询事件流。"""
        return self._query_event_stream(prompt, opts)

    async def _query_event_stream(self, prompt: str, opts: QueryOptions) -> AsyncIterator[Event]:
        """执行一次 opencode run 查询（行解析与 hook 派发见模块级函数）。"""
        from workflow_controller.session.events import Event, EventType

        iteration = opts.iteration
        session_id = opts.session_id
        self._last_assistant_text = ""
        # opencode 把 prompt 作为 argv 传入、stdout 不回显 user 消息，
        # 主动 emit USER_MESSAGE，与 claude_sdk/cli 对齐，前端能渲染用户输入。
        if prompt:
            yield Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.USER_MESSAGE,
                data={"content": [{"type": "text", "text": prompt}]},
            )
        run = await self._prepare_run(prompt, opts)
        try:
            proc = await self._spawn_proc(run.args, env=run.proc_env, cwd=run.cwd)
        except OSError as e:
            for event in spawn_failure_events(
                e,
                args=run.args,
                default_command=self._opencode_bin,
                command_prefix=self._command_prefix,
                log_label="opencode CLI",
                session_id=session_id,
                iteration=iteration,
                end_reason="internal_error",
            ):
                yield event
            if run.gate is not None:
                await run.gate.stop()
            return

        self._proc = proc
        self._arm_cleanup(proc)
        async for event in self._run_stream(proc, run, opts.hooks):
            yield event

    async def _prepare_run(self, prompt: str, opts: QueryOptions) -> _PreparedRun:
        """装配 argv / 子进程环境与 OPENCODE_CONFIG_CONTENT，并启动 gate（如接线）。"""
        # Read-only sandbox: translate the caller's allowed_tools (cc PascalCase)
        # into an opencode restricted-agent permission block (see
        # build_restricted_agent_config). None when the caller imposes no
        # restriction (the worker) -> default agent, full toolset.
        restricted_agent = build_restricted_agent_config(opts.allowed_tools, opts.disallowed_tools)
        # opencode 的 -m 参数要求 provider/model 格式。
        # 裸 model 名会被 opencode 跳过（_build_args 里检查 "/" in model），
        # 走 opencode 默认 provider。用户需在模型配置里写完整 provider/model。
        # 探测是同步 subprocess（首查最长 15s）：经 to_thread 免吊事件循环，
        # 命中缓存后这里只是一次线程跳的零头
        run_flags = await asyncio.to_thread(_detect_run_flags, self._opencode_bin)
        # 只读沙箱（allowlist 面）才切换受限 agent；deny-only（严格
        # traceback 的 task 拒绝）走顶层 permission，不夺走系统提示词通道
        allowlist_restricted = bool(opts.allowed_tools) and restricted_agent is not None
        args = _build_args(
            bin_path=self._opencode_bin,
            prompt=prompt,
            model=opts.model,
            cwd=opts.cwd,
            command_prefix=self._command_prefix,
            resume=self._resume,
            permission_mode=opts.permission_mode or self._permission_mode,
            restricted=allowlist_restricted,
            worker_agent=not allowlist_restricted and bool(opts.system_prompt),
            agent_name=opts.agent_name,
            run_flags=run_flags,
        )
        proc_env = _compose_proc_env(opts.env, opts.cwd)
        ov = _collect_run_overlays(opts.hooks, opts.cwd)
        if ov.provenance_plugin:
            proc_env["WORKFLOW_CONTROLLER_SESSION_ID"] = opts.session_id
            proc_env["WORKFLOW_CONTROLLER_HOOK_URL"] = ov.hook_url
        gate = await self._apply_config_content(opts, proc_env, restricted_agent, ov)
        return _PreparedRun(
            args=args,
            proc_env=proc_env,
            cwd=opts.cwd,
            session_id=opts.session_id,
            iteration=opts.iteration,
            gate=gate,
        )

    async def _apply_config_content(
        self,
        opts: QueryOptions,
        proc_env: dict[str, str],
        restricted_agent: dict[str, Any] | None,
        ov: _RunOverlays,
    ) -> Any:
        """启动 gate（如接线）并装配 OPENCODE_CONFIG_CONTENT（有任一注入段时）。"""
        gate = await self._maybe_start_gate(opts.hooks, opts.session_id)
        has_config_overrides = (
            gate is not None
            or restricted_agent is not None
            or opts.system_prompt
            or ov.subagent_hints
            or ov.instructions
            or ov.provenance_plugin
        )
        if has_config_overrides:
            proc_env["OPENCODE_CONFIG_CONTENT"] = build_config_content(
                gate_url=gate.url if gate is not None else None,
                restricted_agent=restricted_agent,
                system_prompt=opts.system_prompt,
                subagent_hints=ov.subagent_hints,
                agents_dir=ov.agents_dir,
                instructions=ov.instructions,
                session_id=opts.session_id,
                plugins=[ov.provenance_plugin] if ov.provenance_plugin else None,
                base_content=proc_env.get("OPENCODE_CONFIG_CONTENT", ""),
                auto_compaction=ov.auto_compaction,
            )
        return gate

    async def _maybe_start_gate(
        self,
        hooks: DriverHooks | None,
        session_id: str,
    ) -> Any:
        """Start a per-query MCP gate if the orchestrator wired on_gate_question.

        Returns the gate (to be stopped in ``finally``) or None. Never raises —
        a gate start failure degrades gracefully (opencode runs without a gate).
        """
        if hooks is None or hooks.on_gate_question is None:
            return None
        from workflow_controller.driver.opencode_gate import PerQueryGate

        try:
            gate = PerQueryGate(hooks=hooks, session_id=session_id)
            await gate.start()
            return gate
        except Exception:
            logger.exception("opencode gate failed to start; continuing without gate")
            return None

    async def _run_stream(
        self,
        proc: asyncio.subprocess.Process,
        run: _PreparedRun,
        hooks: DriverHooks | None,
    ) -> AsyncIterator[Event]:
        """流式消费 opencode stdout、发收尾事件并清理 poller / 进程 / gate。"""
        from workflow_controller.session.events import Event, EventType

        session_id = run.session_id
        iteration = run.iteration
        # poller 由 _stream_line_events 创建并经此容器交回；取消推迟到
        # proc.wait 与 ITERATION_END 之后——stdout 关闭到进程退出之间的
        # 尾窗内 subagent 进度事件仍需上报。
        pollers: list[asyncio.Task[None]] = []
        try:
            if proc.stdin is None:
                raise RuntimeError("opencode 子进程 stdin 未创建")
            # prompt travels as a positional argv; closing stdin signals EOF and
            # avoids opencode's non-TTY stdin hang (spike finding).
            proc.stdin.close()
            self._stderr_task = asyncio.ensure_future(_drain_stderr(proc))
            async for event in self._stream_line_events(proc, run, hooks, pollers):
                yield event
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except TimeoutError:
                kill_proc_group(proc)
                await proc.wait()
            yield Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.ITERATION_END,
                data={"reason": "proc_exit"},
            )
        except asyncio.CancelledError:
            kill_proc_group(proc)
            raise
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e!s}"
            logger.exception("opencode driver query error session=%s", session_id)
            kill_proc_group(proc)
            # 异常路径收尾标记：事件流消费者（如前端拓扑归档）依赖
            # iteration_end 判定 query 边界；不进 finally（aclose 风险）。
            for event in error_end_events(
                f"opencode driver internal error: {error_msg}",
                session_id=session_id,
                iteration=iteration,
                end_reason="internal_error",
            ):
                yield event
        finally:
            for poller in pollers:
                poller.cancel()
                with contextlib.suppress(BaseException):
                    await poller
            await self._teardown_run(proc, run)

    async def _stream_line_events(
        self,
        proc: asyncio.subprocess.Process,
        run: _PreparedRun,
        hooks: DriverHooks | None,
        poller_out: list[asyncio.Task[None]],
    ) -> AsyncIterator[Event]:
        """逐行转事件并派发 hook；subagent poller 创建后经 poller_out 交出所有权。"""
        # 主会话 id 单槽存放：批次解析方法写入，poller 闭包读取
        main_oc_id: list[str] = [""]
        id_ready = asyncio.Event()
        # Background poller: reads the session db for subagent parts in
        # real time (opencode only reports task on stdout post-completion).
        subagent_started: set[str] = set()
        # Shared with _dispatch_hook: a sub-session id lands here once its
        # stop fires on stdout, so the poller stops emitting late progress
        # for it (avoids re-pushing the final message after stop).
        subagent_stopped: set[str] = set()
        poller: asyncio.Task[None] | None = None
        if hooks is not None and hooks.on_subagent_progress is not None:
            poller = asyncio.ensure_future(
                _poll_subagent_stream(
                    run.proc_env,
                    hooks,
                    id_ready,
                    lambda: main_oc_id[0],
                    subagent_started,
                    subagent_stopped,
                )
            )
        try:
            # 分块切行读 stdout（绕开 readline 64KB 行上限），逐行：捕获主会话
            # id、转事件、派发 hook。stop 只在批间检查：已读 chunk 与 EOF 尾行
            # 批次必须完整过 _dispatch_hook——尾行 task tool_use 触发
            # on_subagent_stop 及其检查点快照，不因 stop 置位而丢弃。
            async for lines in iter_proc_line_batches(proc):
                async for event in self._oc_batch_events(
                    lines, run, hooks, main_oc_id, id_ready, subagent_stopped
                ):
                    yield event
                if self._stop_event.is_set():
                    return
        finally:
            if poller is not None:
                poller_out.append(poller)

    async def _oc_batch_events(
        self,
        lines: list[str],
        run: _PreparedRun,
        hooks: DriverHooks | None,
        main_oc_id: list[str],
        id_ready: asyncio.Event,
        subagent_stopped: set[str],
    ) -> AsyncIterator[Event]:
        """一个 stdout 行批次的事件产出：首次捕获主会话 id 发 ITERATION_START，
        逐行转事件并派发 hook（更新 _last_assistant_text）。"""
        session_id = run.session_id
        iteration = run.iteration
        for line in lines:
            # Capture the main session id (needed by the poller to
            # exclude the main session from subagent discovery).
            if not main_oc_id[0]:
                oc_sid = _extract_line_session_id(line)
                if oc_sid:
                    main_oc_id[0] = oc_sid
                    id_ready.set()
                    yield _iteration_start_event(oc_sid, session_id, iteration)
            for event in _opencode_line_to_event(line, session_id, iteration):
                yield event
            self._last_assistant_text = await _dispatch_hook(
                line, hooks, self._last_assistant_text, run.proc_env, subagent_stopped
            )

    async def _teardown_run(self, proc: asyncio.subprocess.Process, run: _PreparedRun) -> None:
        """query 收尾：进程组击杀、等死透、解除清理钩子并停 gate。"""
        self._stderr_task = None
        kill_proc_group(proc)
        # SIGKILL 后必须等 opencode 彻底退出：它的 MCP 客户端还挂着到 gate
        # 的 SSE 长连接，进程死透后内核才关 socket。紧接着让出一个事件循环
        # tick，让 gate 的 uvicorn 处理 FIN→disconnect，SSE 请求自然收尾。
        # 否则 gate.stop() 会在连接尚未断开时强杀 in-flight SSE，刷出
        # "ASGI callable returned without completing response"。
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(proc.wait(), timeout=5.0)
            await asyncio.sleep(0)
        self._disarm_cleanup()
        self._proc = None
        if run.gate is not None:
            await run.gate.stop()


def _compose_proc_env(env: dict[str, str] | None, cwd: str | None) -> dict[str, str]:
    """组装子进程环境：继承 + 调用方覆盖 + API key 别名 + PWD 对齐 cwd。

    opencode（bun）按 **PWD 环境变量**（而非 getcwd）做项目发现——spawn 只
    设 cwd 不改 PWD 时，子进程拿到的是守护进程的陈旧 PWD，会在错误的
    目录里找 .opencode/agents（现象即 `Agent not found`）。PWD 是 POSIX
    语义上「当前工作目录的绝对路径」，spawn 指定 cwd 时必须同步重写。
    """
    merged = _ensure_api_key({**os.environ, **(env or {})})
    if cwd:
        merged["PWD"] = cwd
    return merged


def _ensure_api_key(env: dict[str, str]) -> dict[str, str]:
    """opencode's @ai-sdk/anthropic reads ANTHROPIC_API_KEY; alias it from
    ANTHROPIC_AUTH_TOKEN (the GLM Coding Plan var) when absent.
    Also alias ANTHROPIC_API_KEY -> ALIBABA_API_KEY for opencode's built-in
    alibaba-cn provider (reads ALIBABA_API_KEY, not ANTHROPIC_API_KEY).
    """
    if not env.get("ANTHROPIC_API_KEY") and env.get("ANTHROPIC_AUTH_TOKEN"):
        env["ANTHROPIC_API_KEY"] = env["ANTHROPIC_AUTH_TOKEN"]
    if env.get("ANTHROPIC_API_KEY") and not env.get("ALIBABA_API_KEY"):
        env["ALIBABA_API_KEY"] = env["ANTHROPIC_API_KEY"]
    return env


# ── read-only sandbox: allowed_tools → opencode permission.deny ─────────────

# cc PascalCase tool name → opencode permission category (lowercase). opencode
# keys permissions by tool category, not exact tool name.
_CC_TO_OPENCODE_TOOL = {
    "Read": "read",
    "Glob": "glob",
    "Grep": "grep",
    "List": "list",
    "Edit": "edit",
    "Write": "write",
    "Bash": "bash",
    "Task": "task",
    "Agent": "task",  # cc Agent ≈ opencode task (subagent)
    "WebFetch": "webfetch",
    "WebSearch": "websearch",
    "TodoWrite": "todowrite",
    "Skill": "skill",
}

# opencode tool categories that mutate state or escape the sandbox. Denied
# unless allowed_tools explicitly maps to them; read-only categories
# (read/glob/grep/list/lsp) are never denied.
_OPENCODE_MUTATING_TOOLS = frozenset(
    {
        "edit",
        "write",
        "bash",
        "task",
        "webfetch",
        "websearch",
        "todowrite",
        "skill",
    }
)

_RESTRICTED_AGENT_NAME = "workflow-controller-restricted"


def build_restricted_agent_config(
    allowed_tools: list[str] | None,
    disallowed_tools: list[str] | None = None,
) -> dict[str, Any] | None:
    """Restrict an opencode run's toolset to ``allowed_tools`` (cc PascalCase).

    Reproduces cc's read-only reviewer sandbox on opencode. The opencode driver
    otherwise silently drops ``allowed_tools``/``disallowed_tools`` (it has no
    matching CLI flag); instead we deny mutating categories the caller omitted
    via an injected agent's ``permission`` block. opencode's
    ``--dangerously-skip-permissions`` auto-approves only what is *not explicitly
    denied, so an explicit ``deny`` holds even in headless bypass mode (verified
    by the readonly spike: edit/write/bash = deny blocked a file creation that
    the unrestricted default agent performed).

    ``disallowed_tools`` denies the named categories even without an
    allowlist — the strict-traceback writer rejection relies on this
    (deny-only: task 拒绝 = 子代理不得启动). The allowlist, when present,
    remains the stricter model and governs everything else.

    Returns ``None`` when nothing is denied (no restriction → the default
    agent with its full toolset, i.e. the worker). Otherwise an
    ``{"agent": {<name>: {"permission": {<cat>: "deny", ...}}}}`` dict to
    merge into ``OPENCODE_CONFIG_CONTENT`` and select with ``--agent <name>``.
    """
    allowed = (
        {_CC_TO_OPENCODE_TOOL.get(t, t.lower()) for t in allowed_tools} if allowed_tools else None
    )
    deny = (_OPENCODE_MUTATING_TOOLS - allowed) if allowed is not None else set()
    for tool in disallowed_tools or ():
        deny.add(_CC_TO_OPENCODE_TOOL.get(tool, tool.lower()))
    if not deny:
        return None
    deny_map = {c: "deny" for c in sorted(deny)}
    return {
        # mode=primary：v2 起 run --agent 只对 primary 模式的 agent 生效
        "agent": {_RESTRICTED_AGENT_NAME: {"mode": "primary", "permission": dict(deny_map)}},
        # v2 实测：agent 级 permission 的 deny 不被执行，顶层 permission 的
        # 显式 deny 在 --auto 下有强制力——顶层对该次 standalone 调用整体
        # 生效，正是 restrict 沙箱的语义；agent 级保留给 v1 语义
        "permission": dict(deny_map),
    }


def _build_args(
    *,
    bin_path: str,
    prompt: str,
    model: str,
    cwd: str | None,
    command_prefix: list[str] | None = None,
    resume: str | None,
    permission_mode: str = "bypassPermissions",
    restricted: bool = False,
    worker_agent: bool = False,
    agent_name: str = "",
    run_flags: frozenset[str],
) -> list[str]:
    # run_flags 无默认口：探测是同步 subprocess，必须由调用方在 to_thread
    # 内取得后显式传入（见 query），否则会在事件循环上隐式拉起子进程
    args = [*(command_prefix or []), bin_path, "run", "--format", "json"]
    # v2 起后台服务会吞掉每次调用的环境注入（OPENCODE_CONFIG_CONTENT），
    # 必须独立私有服务；v1 无此旗标则沿用旧进程模型
    if "--standalone" in run_flags:
        args.append("--standalone")
    # opencode has no --permission-mode; headless runs must auto-approve.
    # v2 用 --auto（保留显式 deny）；v1 沿用全跳过旗标
    if permission_mode in ("", "bypassPermissions"):
        args.append("--auto" if "--auto" in run_flags else "--dangerously-skip-permissions")
    # 工作目录经子进程 cwd 传入（opencode 按 cwd 做项目发现）。不能用
    # `--dir` 旗标：opencode v2 的 run 子命令已移除该旗标，传入直接报
    # "Unrecognized flag" 拒绝执行。
    if resume:
        args.extend(["--session", resume])
    # opencode wants provider/model; a bare name (e.g. glm-5.2[1m]) is invalid
    # at the API and is skipped so opencode.json's default model governs.
    if model and "/" in model:
        args.extend(["-m", model])
    if restricted:
        # select the read-only agent whose permission.deny was injected via
        # OPENCODE_CONFIG_CONTENT (see build_restricted_agent_config).
        args.extend(["--agent", _RESTRICTED_AGENT_NAME])
    elif agent_name:
        # graph plugin: 用 yaml executor 名（如 developer），从 cwd/.opencode/agents/<name>.md 加载
        args.extend(["--agent", agent_name])
    elif worker_agent:
        # select the worker agent whose `prompt` (system_prompt) was injected
        # via OPENCODE_CONFIG_CONTENT — opencode's only system-prompt channel.
        args.extend(["--agent", WORKER_AGENT_NAME])
    args.append(prompt)
    return args


async def _drain_stderr(proc: asyncio.subprocess.Process) -> None:
    """泵干 opencode stderr：非空行统一告警（无分级提示场景）。"""

    def _handle(line: str) -> None:
        logger.warning("opencode stderr: %s", line[:300])

    await drain_proc_stderr(proc, _handle)


# opencode wraps a task subagent's return text in a <task id="ses_…" state="…">
# <task_result>…</task_result></task> envelope (captured from opencode 1.17.11 +
# GLM). We surface only the inner text — the envelope's <task id="ses_…"> also
# carries the sub-session id, used elsewhere for token lookup.
_TASK_RESULT_RE = re.compile(r"<task_result>\s*(.*?)\s*</task_result>", re.DOTALL)
_TASK_ID_RE = re.compile(r'<task\s+id="([^"]+)"')


def _extract_line_session_id(line: str) -> str:
    """Pull the ``sessionID`` from one opencode json line (the main session).
    Returns "" on any parse error or when the field is absent.
    """
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return ""
    if not isinstance(data, dict):
        return ""
    return str(data.get("sessionID") or "")


def _iteration_start_event(oc_sid: str, session_id: str, iteration: int) -> Event:
    """opencode 主会话 id 首次出现时发的 ITERATION_START 事件。"""
    from workflow_controller.session.events import Event, EventType

    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.ITERATION_START,
        data={
            "session_id": oc_sid,
            "subtype": "init",
            "model": "",
            "cwd": "",
        },
    )


# Poll cadence (seconds). 400 ms feels live without hammering sqlite.
_SUBAGENT_POLL_INTERVAL = 0.4


# Background poller: polls opencode's session db for subagent parts and emits
# incremental ``SUBAGENT`` progress events via hooks.
#
# opencode writes parts to the db as the subagent runs; the poller discovers
# new sub-sessions (firing ``on_subagent_start``) and diffs by ``part.id``
# (firing ``on_subagent_progress`` with only the new messages). Discovery is
# **recursive** (``list_descendant_sessions``): 孙 agent（子代理再派发的
# agent）的 ``parent_id`` 指向子会话而非 main——start 事件带
# ``parent_agent_id``（直接子代为空串）供前端拓扑层级化。孙的 task 完成
# 只出现在其父会话的 part 流里（main stdout 只报 main 自己的 task），在
# part diff 中检测完成的 task tool part（``state.output`` 携带
# ``<task id="ses_…">`` 完成标记）为孙补发 ``on_subagent_stop``；task
# part 先以 pending 态落行、完成时原地更新（part id 不变），pending 的
# task part 不进 seen 去重、每轮复查直到完成标记出现。补发前校验孙已
# start：首个 part 与完成标记同轮到达时留 pending 下轮再触发，确保
# start 先于 stop（无 start 的 stop 会被前端拓扑丢弃）。Best-effort: any
# db error is swallowed; the stdout ``task`` line still fires
# ``on_subagent_stop`` with a full read, so the panel degrades to the
# post-completion view, never worse.
async def _poll_subagent_stream(
    env: dict[str, str] | None,
    hooks: DriverHooks,
    id_ready: asyncio.Event,
    get_main_id: Callable[[], str],
    started: set[str],
    stopped: set[str],
) -> None:
    """轮询 opencode session db，增量上报子代理进度（设计见上方注释）。"""
    await id_ready.wait()
    main_id = get_main_id()
    if not main_id:
        return
    # Snapshot sub-sessions (all descendant levels) that already exist in the
    # db before this run so we don't replay stale parts from an earlier
    # opencode session.
    baseline: set[str] = set()
    for sid, _parent in await asyncio.to_thread(
        list_descendant_sessions,
        main_id,
        env=env,
    ):
        baseline.add(sid)
    seen_parts: dict[str, set[str]] = {}
    started_local: set[str] = set()
    # pending task part → 属主会话：task part 完成标记未出现时每轮复查
    pending_task_owner: dict[str, str] = {}
    # 已由 poller 补发过 stop 的孙，防同一完成标记重复触发
    grandson_stopped: set[str] = set()
    try:
        while True:
            await asyncio.sleep(_SUBAGENT_POLL_INTERVAL)
            await _poll_subagent_round(
                env,
                hooks,
                main_id,
                baseline,
                started,
                stopped,
                started_local,
                seen_parts,
                pending_task_owner,
                grandson_stopped,
            )
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.debug("subagent stream poller ended", exc_info=True)


async def _poll_subagent_round(
    env: dict[str, str] | None,
    hooks: DriverHooks,
    main_id: str,
    baseline: set[str],
    started: set[str],
    stopped: set[str],
    started_local: set[str],
    seen_parts: dict[str, set[str]],
    pending_task_owner: dict[str, str],
    grandson_stopped: set[str],
) -> None:
    """一轮轮询：发现新子会话、diff part 进度并补发孙完成 stop。"""
    descendants = await asyncio.to_thread(list_descendant_sessions, main_id, env=env)
    for sid, parent_sid in descendants:
        if sid in baseline:
            continue
        # stop 已触发：跳过以免 start 晚于 stop、tracker 永久 in-flight
        if sid in stopped:
            continue
        if sid not in started_local:
            started_local.add(sid)
            started.add(sid)
            seen_parts[sid] = set()
            await _fire_subagent_start(hooks, env, sid, parent_sid, main_id)
        parts = await asyncio.to_thread(read_subagent_parts, sid, env=env)
        new_msgs = await _diff_subagent_parts(
            hooks,
            env,
            sid,
            parts,
            seen_parts[sid],
            pending_task_owner,
            started,
            stopped,
            grandson_stopped,
        )
        if new_msgs and hooks.on_subagent_progress is not None:
            await hooks.on_subagent_progress(
                hooks.session_id,
                {"agent_id": sid, "messages": new_msgs},
            )


async def _fire_subagent_start(
    hooks: DriverHooks,
    env: dict[str, str] | None,
    sid: str,
    parent_sid: str,
    main_id: str,
) -> None:
    """发现新子会话：发 on_subagent_start（parent_agent_id 区分直接子代/孙）。"""
    if hooks.on_subagent_start is None:
        return
    await hooks.on_subagent_start(
        hooks.session_id,
        {
            "agent_id": sid,
            "agent_type": await asyncio.to_thread(read_subagent_type, sid, env=env),
            # 直接子代（parent = main）为空串挂 main；孙 agent = 派发者子会话 id
            "parent_agent_id": (parent_sid if parent_sid != main_id else ""),
        },
    )


async def _diff_subagent_parts(
    hooks: DriverHooks,
    env: dict[str, str] | None,
    sid: str,
    parts: list[tuple[str, list[dict[str, Any]]]],
    seen: set[str],
    pending_task_owner: dict[str, str],
    started: set[str],
    stopped: set[str],
    grandson_stopped: set[str],
) -> list[dict[str, Any]]:
    """diff 一轮新 part 消息并检测孙 task 完成标记；返回本轮新消息。"""
    new_msgs: list[dict[str, Any]] = []
    for pid, msgs in parts:
        if pid in seen:
            # task part 可能 pending 时已消费过：不重发消息，但每轮复查等完成标记
            if pid not in pending_task_owner:
                continue
        else:
            seen.add(pid)
            new_msgs.extend(msgs)
        if not _is_task_part(msgs):
            continue
        child_sid = _task_part_sid(msgs)
        if not child_sid:
            pending_task_owner[pid] = sid
            continue
        # 孙尚未 start（首个 part 与完成标记同轮到达，父会话先被处理）：
        # 留 pending 下轮复查，待 start 发出后再补发 stop
        if child_sid not in started:
            pending_task_owner[pid] = sid
            continue
        pending_task_owner.pop(pid, None)
        if child_sid in stopped or child_sid in grandson_stopped:
            continue
        grandson_stopped.add(child_sid)
        # 同步进共享 stopped：poller 后续轮次不再为其发 progress
        stopped.add(child_sid)
        await _fire_grandson_stop(hooks, child_sid, msgs, env=env)
    return new_msgs


def _is_task_part(msgs: list[dict[str, Any]]) -> bool:
    """归一后的 part 消息组是否是 task 工具（tool_use + tool_result 对）。"""
    return bool(msgs and msgs[0].get("type") == "tool_use" and msgs[0].get("name") == "task")


def _task_part_sid(msgs: list[dict[str, Any]]) -> str:
    """完成的 task part 里的子会话 id（``ses_…``，来自 ``<task id=…>`` 完成
    标记）；pending / 非任务为空。
    """
    for m in msgs[1:]:
        if m.get("type") == "tool_result":
            return _extract_subagent_session_id(str(m.get("content") or ""))
    return ""


async def _fire_grandson_stop(
    hooks: DriverHooks,
    child_sid: str,
    task_msgs: list[dict[str, Any]],
    *,
    env: dict[str, str] | None,
) -> None:
    """为孙 agent 补发 stop，payload 与 stdout 路径（``_dispatch_hook``）同构。"""
    if hooks.on_subagent_stop is None:
        return
    output = ""
    for m in task_msgs:
        if m.get("type") == "tool_result":
            output = str(m.get("content") or "")
            break
    agent_type = await asyncio.to_thread(read_subagent_type, child_sid, env=env)
    total_tokens = await asyncio.to_thread(lookup_subagent_output_tokens, child_sid, env=env)
    # 与 _dispatch_hook 同构：孙的完整消息流在它自己的 part 表里
    messages = await asyncio.to_thread(read_subagent_messages, child_sid, env=env)
    await hooks.on_subagent_stop(
        hooks.session_id,
        {
            "agent_id": child_sid,
            "agent_type": agent_type,
            "content": truncate_subagent_content(_extract_subagent_content(output)),
            "total_tokens": total_tokens,
            "messages": messages,
        },
    )


def _extract_subagent_content(output: Any) -> str:
    """Return a task subagent's return text from opencode's ``state.output``.

    Strips the ``<task_result>…</task_result>`` envelope opencode wraps around
    the subagent's text. Falls back to the raw output (stringified) when the
    envelope is absent or the output isn't a string, so this never raises.
    """
    text = output if isinstance(output, str) else str(output or "")
    match = _TASK_RESULT_RE.search(text)
    return match.group(1) if match else text


def _extract_subagent_session_id(output: Any) -> str:
    """Return the sub-session id (``ses_…``) from a task result envelope, or "".
    Used to look up token usage and messages in opencode's session store.
    """
    text = output if isinstance(output, str) else str(output or "")
    match = _TASK_ID_RE.search(text)
    return match.group(1) if match else ""


async def _dispatch_hook(
    line: str,
    hooks: DriverHooks | None,
    last_assistant_text: str,
    env: dict[str, str] | None = None,
    stopped: set[str] | None = None,
) -> str:
    """Dispatch DriverHooks signaling for one opencode event line; return the
    updated ``last_assistant_text``.

    ``env`` is the subprocess environment so db lookups resolve the same db
    opencode wrote (custom ``XDG_DATA_HOME`` / ``HOME``).
    """
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return last_assistant_text
    if not isinstance(data, dict):
        return last_assistant_text
    msg_type = data.get("type", "")
    if msg_type == "text":
        return last_assistant_text + (data.get("part", {}).get("text") or "")
    # task 工具 = subagent：opencode 的 task 在一个已完成 tool_use 事件里（call +
    # result 合并）。stdout 路径只发 stop——start/progress 是后台 poller 的
    # 职责；即便 poller 漏了 start，这里也不补迟到的假 start，避免与 poller 的真
    # start 撞出两条 START 行。agent_type 透传 input.subagent_type；content 从
    # state.output 抽出，随 stop 内联透传。
    if msg_type == "tool_use" and data.get("part", {}).get("tool") == "task":
        return await _dispatch_task_stop(
            hooks, data.get("part", {}), last_assistant_text, env, stopped
        )
    is_final_stop_step = (
        msg_type == "step_finish"
        and hooks is not None
        and hooks.on_stop is not None
        and data.get("part", {}).get("reason") in ("stop", "end_turn")
    )
    if hooks is not None and is_final_stop_step and hooks.on_stop is not None:
        await hooks.on_stop(hooks.session_id, {"last_assistant_message": last_assistant_text})
    return last_assistant_text


async def _dispatch_task_stop(
    hooks: DriverHooks | None,
    part: dict[str, Any],
    last_assistant_text: str,
    env: dict[str, str] | None,
    stopped: set[str] | None,
) -> str:
    """task 工具行：抽子会话信息并发 on_subagent_stop，返回原 last_assistant_text。"""
    state = part.get("state", {}) or {}
    raw_input = state.get("input", {}) or {}
    agent_type = raw_input.get("subagent_type", "") if isinstance(raw_input, dict) else ""
    content = truncate_subagent_content(_extract_subagent_content(state.get("output", "")))
    # Forward the subprocess env so a relocated data dir resolves the
    # db opencode wrote.
    sub_session_id = _extract_subagent_session_id(state.get("output", ""))
    if state.get("status") == "error" and not sub_session_id:
        return last_assistant_text
    # Signal the background poller to stop emitting progress for this
    # sub-session: its next 0.4s cycle could otherwise re-push final parts
    # that this stop's full read already includes.
    if stopped is not None and sub_session_id:
        stopped.add(sub_session_id)
    # Use the sub-session id as agent_id so progress and stop events share
    # the same key; fall back to the tool part id when there's no envelope.
    agent_id = sub_session_id or part.get("id", "")
    total_tokens = await asyncio.to_thread(lookup_subagent_output_tokens, sub_session_id, env=env)
    # Read the full message stream from the db (stdout only reports task
    # post-completion; the subagent's thinking lives in the part table).
    # to_thread 同 poller：busy_timeout 下的同步读不吊事件循环。
    messages = await asyncio.to_thread(read_subagent_messages, sub_session_id, env=env)
    if hooks is not None and hooks.on_subagent_stop is not None:
        await hooks.on_subagent_stop(
            hooks.session_id,
            {
                "agent_id": agent_id,
                "agent_type": agent_type,
                "content": content,
                "total_tokens": total_tokens,
                "messages": messages,
            },
        )
    return last_assistant_text


# ── JSON line → Event conversion (module-level, driver-internal) ───────────


def _opencode_line_to_event(line: str, session_id: str, iteration: int) -> list[Event]:
    """Translate one opencode `--format json` line into 0..N Workflow Controller Events."""
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        logger.debug("opencode driver: failed to parse JSON line: %s", line[:200])
        return []
    if not isinstance(data, dict):
        return []

    msg_type = data.get("type", "")
    if msg_type == "text":
        return _parse_text(data, session_id, iteration)
    if msg_type == "tool_use":
        return _parse_tool(data, session_id, iteration)
    if msg_type == "step_finish":
        return _parse_step_finish(data, session_id, iteration)
    if msg_type == "error":
        return _parse_error(data, session_id, iteration)
    # step_start / session lifecycle / unknown -> ignored (not surfaced to Workflow Controller)
    return []


def _parse_text(data: dict[str, Any], session_id: str, iteration: int) -> list[Event]:
    from workflow_controller.session.events import Event, EventType

    text = data.get("part", {}).get("text", "")
    if not text:
        return []
    return [
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.ASSISTANT_MESSAGE,
            data={"content": [{"type": "text", "text": text}], "model": ""},
        )
    ]


def _parse_tool(data: dict[str, Any], session_id: str, iteration: int) -> list[Event]:
    """opencode combines a tool call and its result in one event; emit TOOL_USE + TOOL_RESULT + TOOL_CALL.

    `TOOL_RESULT.data.tool_name` 取自 `part.tool`，供 `breather._on_notify_skill_hook`
    据此匹配 `notify_skill` 配置项。
    """
    from workflow_controller.session.events import (
        Event,
        EventType,
        extract_tool_arg,
        extract_tool_error,
    )

    part = data.get("part", {})
    state = part.get("state", {}) or {}
    metadata = state.get("metadata", {}) or {}
    tool_id = part.get("id", "")
    tool_name = part.get("tool", "")
    tool_input = state.get("input", {})
    is_error = state.get("status") == "error" or metadata.get("exit", 0) != 0
    events = _tool_use_result_events(
        tool_id=tool_id,
        tool_name=tool_name,
        tool_input=tool_input,
        output=state.get("output", ""),
        is_error=is_error,
        session_id=session_id,
        iteration=iteration,
    )
    tool_call_data: dict[str, Any] = {
        "name": tool_name,
        "arg": extract_tool_arg(tool_input),
        "tool_use_id": tool_id,
        "is_error": is_error,
    }
    if is_error:
        error_reason = extract_tool_error(state.get("error") or state.get("output", ""))
        if error_reason:
            tool_call_data["error"] = error_reason
    events.append(
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.TOOL_CALL,
            data=tool_call_data,
        )
    )
    return events


def _tool_use_result_events(
    *,
    tool_id: str,
    tool_name: str,
    tool_input: dict[str, Any],
    output: object,
    is_error: bool,
    session_id: str,
    iteration: int,
) -> list[Event]:
    """TOOL_USE + TOOL_RESULT 事件对（opencode 单事件里合并了调用与结果）。"""
    from workflow_controller.session.events import Event, EventType

    use = Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.TOOL_USE,
        data={"id": tool_id, "name": tool_name, "input": tool_input},
    )
    result = Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.TOOL_RESULT,
        data={
            "tool_use_id": tool_id,
            "tool_name": tool_name,
            "content": output,
            "is_error": is_error,
        },
    )
    return [use, result]


def _parse_step_finish(data: dict[str, Any], session_id: str, iteration: int) -> list[Event]:
    from workflow_controller.session.events import Event, EventType

    part = data.get("part", {})
    reason = part.get("reason")
    # Normalize opencode's native token shape into the Anthropic usage shape
    # every downstream consumer reads (breather's context check sums
    # input_tokens + cache_creation_input_tokens + cache_read_input_tokens;
    # usage_api / feedback_evaluator / Dashboard token display read the same).
    # Verbatim opencode tokens {input, output, cache:{write, read}} would leave
    # all those fields at 0 → breather never fires on opencode (same failure
    # mode as claude_cli) and the Dashboard shows 0 tokens.
    tok = part.get("tokens") or {}
    cache = tok.get("cache") or {}
    sf_data = {
        "session_id": session_id,
        "duration_ms": 0,
        "num_turns": 1,
        "total_cost_usd": float(part.get("cost", 0) or 0),
        "is_error": reason == "error",
        "usage": {
            "input_tokens": tok.get("input", 0),
            "output_tokens": tok.get("output", 0),
            "cache_creation_input_tokens": cache.get("write", 0),
            "cache_read_input_tokens": cache.get("read", 0),
        },
        "modelUsage": {},
    }
    events = [
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.STEP_FINISH,
            data=dict(sf_data),
        ),
    ]
    return events


def _parse_error(data: dict[str, Any], session_id: str, iteration: int) -> list[Event]:
    """opencode error event → RATE_LIMIT (control signal) + ERROR (log).

    opencode emits errors as a nested dict `{"name","data":{"statusCode",…}}`.
    Translate 401/403 → auth_error (mark INVALID + rotate), 429 → rejected
    rate_limit (mark EXHAUSTED + rotate) into a normalized limit_signal BEFORE
    the ERROR observability log. Other statuses / non-dict errors only emit
    ERROR (observability; the detector does not act on them).
    """
    from workflow_controller.session.events import Event, EventType

    err = data.get("error", {})
    events: list[Event] = []

    status: Any = None
    name = ""
    if isinstance(err, dict):
        inner = err.get("data")
        if isinstance(inner, dict):
            status = inner.get("statusCode")
        name = err.get("name", "") or ""

    if status in (401, 403):
        events.append(
            _rate_limit_event(
                "auth_error",
                "auth_error",
                f"opencode:{name}:{status}",
                err,
                session_id,
                iteration,
            )
        )
    elif status == 429:
        events.append(
            _rate_limit_event("rejected", "5h", f"opencode:{name}:429", err, session_id, iteration)
        )

    events.append(
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.ERROR,
            data={"error": err},
        )
    )
    return events


def _rate_limit_event(
    kind: Literal["auth_error", "rejected"],
    scope: str,
    source: str,
    raw: object,
    session_id: str,
    iteration: int,
) -> Event:
    """限流/鉴权错误 → RATE_LIMIT 控制信号事件（携带归一化的 limit_signal）。"""
    from workflow_controller.core.auth_rotator.signal import LimitSignal
    from workflow_controller.session.events import Event, EventType

    limit_signal = LimitSignal(
        kind=kind,
        scope=scope,
        reset_at=None,
        utilization=None,
        source=source,
    )
    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.RATE_LIMIT,
        data=limit_signal.to_event_data(raw=raw),
    )
