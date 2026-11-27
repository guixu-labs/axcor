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

"""ClaudeCLIDriver — spawns claude binary subprocess and parses stream-json output."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import stat
import tempfile
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any

from workflow_controller.core.auth_rotator.signal import LimitSignal
from workflow_controller.driver._content import extract_user_message_blocks
from workflow_controller.driver._permission import effective_permission_mode
from workflow_controller.driver.claude_events import tool_pair_events
from workflow_controller.driver.claude_lineage import ClaudeSubagentLineage
from workflow_controller.driver.cli_base import (
    CLIDriverBase,
    QueryOptions,
    error_end_events,
    is_stderr_error_line,
    kill_proc_group,
    spawn_failure_events,
)
from workflow_controller.session.usage import context_input_tokens

if TYPE_CHECKING:
    from asyncio import StreamReader
    from collections.abc import AsyncIterator, Callable
    from datetime import datetime

    from workflow_controller.driver.hooks import DriverHooks
    from workflow_controller.session.events import Event

logger = logging.getLogger(__name__)

# 收尾时排空 stderr 的上限：CLI 派生的分离孙进程可能持有 stderr 写端，管道永不
# EOF，`_drain_stderr` 会无限挂起。超过该值即带已收集到的 stderr 行继续收尾。
_STDERR_DRAIN_TIMEOUT_SECONDS = 5.0
_STDERR_LINE_LOG_CHARS = 300
_STDERR_TAIL_LINES = 20
_STDERR_STATUS_PREFIXES = (
    "Reading additional input from stdin...",
    "[cpx-cli]",
)


class ClaudeCLIExitError(RuntimeError):
    """Claude CLI ended unsuccessfully before completing the query."""


class ClaudeCLIDriver(CLIDriverBase):
    """Runs Claude via the `claude` CLI binary (subprocess)."""

    evaluation_default_model = "claude-sonnet-4-6"

    def __init__(
        self,
        *,
        permission_mode: str = "bypassPermissions",
        add_dirs: list[str] | None = None,
        resume: str | None = None,
        fork_session: bool = False,
        claude_bin: str = "claude",
        command_prefix: list[str] | None = None,
    ) -> None:
        super().__init__(permission_mode=permission_mode)
        self._add_dirs: list[str] = add_dirs or []
        self._resume: str | None = resume
        self._fork_session: bool = fork_session
        self._claude_bin: str = claude_bin
        self._command_prefix: list[str] = command_prefix or []

    @property
    def claude_bin(self) -> str:
        """本 driver 启动的 claude 可执行文件。"""
        return self._claude_bin

    @property
    def command_prefix(self) -> list[str]:
        """claude 子进程的命令前缀（如包装器/容器）。"""
        return self._command_prefix

    @property
    def permission_mode(self) -> str:
        """构造时给定的默认权限模式。"""
        return self._permission_mode

    @property
    def add_dirs(self) -> list[str]:
        """claude 查询可访问的附加目录。"""
        return self._add_dirs

    @property
    def resume_ref(self) -> str | None:
        """默认续接的 claude 会话 id。"""
        return self._resume

    @property
    def fork_session(self) -> bool:
        """续接时是否派生新会话而非原会话。"""
        return self._fork_session

    @staticmethod
    async def _notify_subagent_starts(
        line_bytes: bytes,
        lineage: ClaudeSubagentLineage,
        session_id: str,
        hooks: DriverHooks | None,
    ) -> None:
        if hooks is None or hooks.on_subagent_start is None:
            return
        for start in lineage.observe_raw(line_bytes):
            if start["agent_id"]:
                await hooks.on_subagent_start(session_id, start)

    def query(
        self,
        prompt: str,
        *,
        system_prompt: str = "",
        session_id: str = "",
        permission_mode: str = "",
        model: str = "",
        max_turns: int | None = None,
        iteration: int = 1,
        hooks: DriverHooks | None = None,
        env: dict[str, str] | None = None,
        disallowed_tools: list[str] | None = None,
        allowed_tools: list[str] | None = None,
        cwd: str | None = None,
        agent_name: str = "",
        pre_trust: bool = True,
    ) -> AsyncIterator[Event]:
        """claude_cli 专属覆写：比基类多 ``pre_trust``（workspace 信任预写开关）。"""
        self._stop_event.clear()
        opts = QueryOptions(
            iteration=iteration,
            session_id=session_id,
            model=model,
            cwd=cwd,
            max_turns=max_turns,
            allowed_tools=allowed_tools,
            disallowed_tools=disallowed_tools,
            permission_mode=permission_mode,
            env=env,
            hooks=hooks,
            system_prompt=system_prompt,
            agent_name=agent_name,
        )
        return self._query_events(prompt, opts, pre_trust=pre_trust)

    def _query_events(
        self,
        prompt: str,
        opts: QueryOptions,
        *,
        pre_trust: bool = True,
    ) -> AsyncIterator[Event]:
        """返回 claude CLI 查询事件流。"""
        return self._query_event_stream(prompt, opts, pre_trust=pre_trust)

    async def _query_event_stream(
        self,
        prompt: str,
        opts: QueryOptions,
        *,
        pre_trust: bool = True,
    ) -> AsyncIterator[Event]:
        """执行一次 claude CLI 查询（stream-json 行解析见模块级函数）。"""
        iteration = opts.iteration
        session_id = opts.session_id
        cwd = opts.cwd
        proc_env = {**os.environ, **(opts.env or {})}
        bad_cwd_events = _cwd_missing_events(
            cwd, claude_bin=self._claude_bin, session_id=session_id, iteration=iteration
        )
        if bad_cwd_events is not None:
            for event in bad_cwd_events:
                yield event
            return
        args = _cli_query_args(prompt, opts, self)
        preexisting_children: set[str] = set()
        if self._resume and not self._fork_session and cwd:
            preexisting_children = set(_child_transcripts(cwd, self._resume, proc_env))

        # claude -p 在未信任 workspace 会忽略项目 settings 权限与 MCP，spawn 前预置
        # 信任；探测等以内部 scratch 目录为 cwd 的调用用 pre_trust=False 关闭
        if pre_trust:
            ensure_workspace_trusted(
                cwd,
                home=proc_env.get("HOME"),
                claude_config_dir=proc_env.get("CLAUDE_CONFIG_DIR"),
            )

        proc, spawn_events = await self._spawn_cli_proc(args, opts, proc_env=proc_env)
        for event in spawn_events:
            yield event
        if proc is None:
            return

        self._proc = proc
        self._arm_cleanup(proc)

        async for event in self._run_cli_proc(
            proc, opts, proc_env=proc_env, preexisting_children=preexisting_children
        ):
            yield event

    async def _spawn_cli_proc(
        self,
        args: list[str],
        opts: QueryOptions,
        *,
        proc_env: dict[str, str],
    ) -> tuple[asyncio.subprocess.Process | None, list[Event]]:
        """spawn claude 子进程；失败时返回 (None, 失败事件对)。"""
        try:
            return await self._spawn_proc(args, env=proc_env, cwd=opts.cwd), []
        except OSError as e:
            return None, spawn_failure_events(
                e,
                args=args,
                default_command=self._claude_bin,
                command_prefix=self._command_prefix,
                log_label="claude CLI",
                session_id=opts.session_id,
                iteration=opts.iteration,
                end_reason="internal_error",
            )

    async def _run_cli_proc(
        self,
        proc: asyncio.subprocess.Process,
        opts: QueryOptions,
        *,
        proc_env: dict[str, str],
        preexisting_children: set[str],
    ) -> AsyncIterator[Event]:
        """消费 claude CLI stdout 事件流并收尾（退出码校验、异常与清理路径）。"""
        iteration = opts.iteration
        session_id = opts.session_id
        run_state: dict[str, Any] = {"native_id": "", "result_received": False}
        try:
            self._start_stderr_and_poller(
                proc,
                opts,
                proc_env=proc_env,
                run_state=run_state,
                preexisting_children=preexisting_children,
            )
            async for event in self._stream_cli_lines(
                proc, session_id, iteration, run_state, opts.hooks
            ):
                yield event
            yield await self._await_cli_exit(
                proc, run_state, session_id=session_id, iteration=iteration
            )
        except asyncio.CancelledError:
            kill_proc_group(proc)
            raise
        except ClaudeCLIExitError:
            raise
        except Exception:
            logger.exception("CLI driver query error session=%s", session_id)
            kill_proc_group(proc)
            # 异常路径收尾标记：事件流消费者（如前端拓扑归档）依赖
            # iteration_end 判定 query 边界；不进 finally（aclose 风险）。
            for event in error_end_events(
                "CLI driver internal error",
                session_id=session_id,
                iteration=iteration,
                end_reason="internal_error",
            ):
                yield event
        finally:
            await self._teardown_cli_proc(proc, run_state.get("child_poller"))

    async def _stream_cli_lines(
        self,
        proc: asyncio.subprocess.Process,
        session_id: str,
        iteration: int,
        run_state: dict[str, Any],
        hooks: DriverHooks | None,
    ) -> AsyncIterator[Event]:
        """分块读 stdout 逐行转事件并补充子代理父级。"""
        from workflow_controller.session.events import EventType

        if proc.stdout is None:
            raise RuntimeError("claude 子进程 stdout 未创建")
        usage_tracker: dict[str, Any] = {}
        lineage = ClaudeSubagentLineage()
        async for line_bytes in _split_lines(proc.stdout):
            if self._stop_event.is_set():
                return
            await self._notify_subagent_starts(line_bytes, lineage, session_id, hooks)
            for event in _line_events(line_bytes, session_id, iteration, usage_tracker):
                if event.event_type == EventType.ITERATION_START and event.data.get("session_id"):
                    run_state["native_id"] = event.data["session_id"]
                    run_state["id_ready"].set()
                if event.event_type == EventType.STEP_FINISH and not event.data.get("is_error"):
                    run_state["result_received"] = True
                yield event

    def _start_stderr_and_poller(
        self,
        proc: asyncio.subprocess.Process,
        opts: QueryOptions,
        *,
        proc_env: dict[str, str],
        run_state: dict[str, Any],
        preexisting_children: set[str],
    ) -> None:
        """关 stdin、起 stderr 泵与子代理轮询；产物写入 run_state 供后续阶段使用。"""
        if proc.stdin is None:
            raise RuntimeError("claude 子进程 stdin 未创建")
        proc.stdin.close()
        run_state["stderr_lines"] = deque(maxlen=_STDERR_TAIL_LINES)
        run_state["stderr_error_lines"] = deque(maxlen=_STDERR_TAIL_LINES)
        self._stderr_task = asyncio.ensure_future(
            _drain_stderr(
                proc,
                run_state["stderr_lines"],
                run_state["stderr_error_lines"],
            )
        )
        id_ready = asyncio.Event()
        run_state["id_ready"] = id_ready
        run_state["child_poller"] = _start_child_poller(
            opts.hooks,
            opts.cwd or os.getcwd(),
            proc_env,
            id_ready,
            lambda: str(run_state["native_id"]),
            preexisting_children,
        )

    async def _await_cli_exit(
        self,
        proc: asyncio.subprocess.Process,
        run_state: dict[str, Any],
        *,
        session_id: str,
        iteration: int,
    ) -> Event:
        """等 CLI 退出并排空 stderr；非零退出且无成功 result 时抛 ClaudeCLIExitError。"""
        from workflow_controller.session.events import Event, EventType

        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except TimeoutError:
            kill_proc_group(proc)
            await proc.wait()
        if self._stderr_task is not None:
            try:
                await asyncio.wait_for(
                    self._stderr_task,
                    timeout=_STDERR_DRAIN_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                # stderr 管道被分离的孙进程持有写端时不会 EOF：超时放弃，
                # 带着已收集到的 stderr 行继续收尾，避免 query 挂死。
                self._stderr_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._stderr_task
        # 已产出「成功」result 行的工作视为完成：CLI 慢退被 SIGKILL（code -9）不算失败，
        # 否则「工作实际已完成」的会话会被误报为失败。error 子类型的 result 行不视为
        # 完成——CLI 产出 error result 后非零退出仍上报退出码，不吞掉失败详情。
        if proc.returncode and not self._stop_event.is_set() and not run_state["result_received"]:
            reason = _stderr_error_reason(
                run_state["stderr_lines"], run_state["stderr_error_lines"]
            )
            raise ClaudeCLIExitError(f"Claude CLI exited with code {proc.returncode}: {reason}")
        return Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.ITERATION_END,
            data={"reason": "proc_exit"},
        )

    async def _teardown_cli_proc(
        self,
        proc: asyncio.subprocess.Process,
        child_poller: asyncio.Task[None] | None,
    ) -> None:
        """query 收尾：停 poller 与 stderr 泵标记，击杀进程组并解除清理钩子。"""
        if child_poller is not None:
            child_poller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await child_poller
        self._stderr_task = None
        kill_proc_group(proc)
        self._disarm_cleanup()
        self._proc = None


def _build_args(
    *,
    bin_path: str,
    prompt: str,
    model: str,
    cwd: str | None,
    command_prefix: list[str] | None = None,
    max_turns: int | None,
    allowed_tools: list[str] | None,
    disallowed_tools: list[str] | None,
    permission_mode: str,
    add_dirs: list[str],
    resume: str | None,
    fork_session: bool = False,
    system_prompt: str = "",
) -> list[str]:
    args = [
        *(command_prefix or []),
        bin_path,
        "-p",
        prompt,
        "--output-format",
        "stream-json",
        "--verbose",
        "--permission-mode",
        permission_mode,
    ]
    if model:
        args.extend(["--model", model])
    if cwd:
        args.extend(["--add-dir", cwd])
    for d in add_dirs:
        if d and d != cwd:
            args.extend(["--add-dir", d])
    if max_turns:
        args.extend(["--max-turns", str(max_turns)])
    if allowed_tools:
        args.extend(["--allowed-tools", ",".join(allowed_tools)])
    if disallowed_tools:
        args.extend(["--disallowed-tools", ",".join(disallowed_tools)])
    if resume:
        args.extend(["--resume", resume])
    if fork_session:
        args.append("--fork-session")
    if system_prompt:
        args.extend(["--system-prompt", system_prompt])
    return args


def _cli_query_args(prompt: str, opts: QueryOptions, driver: ClaudeCLIDriver) -> list[str]:
    """装配一次 claude CLI 查询的 argv（权限模式与模型/目录旗标规整）。"""
    (
        _iteration,
        _session_id,
        model,
        cwd,
        max_turns,
        allowed_tools,
        disallowed_tools,
        permission_mode,
        _env,
        _hooks,
        system_prompt,
    ) = opts[:11]
    return _build_args(
        bin_path=driver.claude_bin,
        prompt=prompt,
        command_prefix=driver.command_prefix,
        model=model,
        cwd=cwd,
        max_turns=max_turns,
        allowed_tools=allowed_tools,
        disallowed_tools=disallowed_tools,
        permission_mode=effective_permission_mode(permission_mode or driver.permission_mode),
        add_dirs=driver.add_dirs,
        resume=driver.resume_ref,
        fork_session=driver.fork_session,
        system_prompt=system_prompt,
    )


def _cwd_missing_events(
    cwd: str | None,
    *,
    claude_bin: str,
    session_id: str,
    iteration: int,
) -> list[Event] | None:
    """cwd 不存在时的 ERROR + ITERATION_END 事件对；cwd 有效返回 None。"""
    if not (cwd and not os.path.isdir(cwd)):
        return None
    logger.error("Session cwd does not exist: %s (bin=%s)", cwd, claude_bin)
    return error_end_events(
        f"project directory not found: {cwd}",
        session_id=session_id,
        iteration=iteration,
        end_reason="internal_error",
    )


def ensure_workspace_trusted(
    cwd: str | None,
    *,
    home: str | None = None,
    claude_config_dir: str | None = None,
) -> None:
    """spawn 前把 cwd 幂等预标记为已信任写入 Claude 配置（代用户接受信任弹窗），best-effort：失败只告警不阻断。"""
    # cwd=None/""（未显式选定 workspace）时跳过：Path("") 会解析为守护进程自身
    # cwd，把非用户选定目录写入全局信任配置，且 Claude Code 信任沿父目录继承、
    # 覆盖其整棵子树
    if not cwd:
        return
    workspace = Path(cwd).expanduser().resolve()
    key = str(workspace)
    if claude_config_dir:
        # 子进程以 workspace 为 cwd，相对 CLAUDE_CONFIG_DIR 由它按自身 cwd 解析；
        # 预写须以同一基准拼接，才落在子进程实际读取的配置文件上
        cfg_dir = Path(claude_config_dir).expanduser()
        base = cfg_dir if cfg_dir.is_absolute() else workspace / cfg_dir
    else:
        base = Path(home or os.path.expanduser("~"))
    config_path = (base / ".claude.json").resolve()
    try:
        data: dict[str, Any] = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        data = {}
    except OSError:
        logger.warning("workspace trust: cannot read %s", config_path)
        return
    except json.JSONDecodeError:
        logger.warning("workspace trust: %s is not valid JSON; leaving it alone", config_path)
        return
    except UnicodeDecodeError:
        logger.warning("workspace trust: %s is not valid UTF-8; leaving it alone", config_path)
        return
    if not isinstance(data, dict):
        logger.warning("workspace trust: %s is not a JSON object; leaving it alone", config_path)
        return
    projects = data.setdefault("projects", {})
    if not isinstance(projects, dict):
        logger.warning("workspace trust: projects entry in %s is not an object", config_path)
        return
    entry = projects.setdefault(key, {})
    if not isinstance(entry, dict):
        entry = {}
        projects[key] = entry
    if entry.get("hasTrustDialogAccepted") is True:
        return
    entry["hasTrustDialogAccepted"] = True
    try:
        _atomic_write_json(config_path, data)
    except (OSError, UnicodeError):
        # json.loads 接受孤代理项转义（\ud800 等），ensure_ascii=False 写回时无法
        # 按 UTF-8 编码，UnicodeEncodeError 不是 OSError，需同样按 best-effort 跳过
        logger.warning("workspace trust: cannot write %s", config_path, exc_info=True)
    else:
        logger.info("workspace trust: marked %s as trusted in %s", key, config_path)


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    """同目录临时文件 + os.replace 原子写 JSON，保留原文件权限。"""
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError:
        mode = 0o600
    # 相对 CLAUDE_CONFIG_DIR 拼进 workspace 时，配置目录可能尚未创建
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".claude.json.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


async def _drain_stderr(
    proc: asyncio.subprocess.Process,
    lines: deque[str],
    error_lines: deque[str],
) -> None:
    """泵干 claude stderr：状态/普通行降 debug，错误行告警并优先保留。"""
    try:
        if proc.stderr is None:
            raise RuntimeError("claude 子进程 stderr 未创建")
        # 与 stdout 一致的分块切行，避免 LimitOverrunError 提前终止 drain。
        async for line_bytes in _split_lines(proc.stderr):
            _log_stderr_line(line_bytes, lines, error_lines)
    except Exception:
        logger.debug("stderr drain ended", exc_info=True)


async def _split_lines(reader: StreamReader) -> AsyncIterator[bytes]:
    """Read 64KB chunks from *reader* and yield newline-terminated lines,
    buffering across chunk boundaries; a trailing line without a newline at
    EOF is yielded too.

    用 bytearray 累积避免 bytes 的整块重分配 + 全量拷贝——大 JSON 单行下
    `buf += chunk` 每次把已累积的几 MB 全拷一遍会退化成 O(n²)。
    """
    buf = bytearray()
    while True:
        chunk = await reader.read(65536)
        if not chunk:
            break
        buf += chunk
        while b"\n" in buf:
            line_bytes, buf = buf.split(b"\n", 1)
            yield bytes(line_bytes)
    if buf:
        yield bytes(buf)


def _log_stderr_line(
    line_bytes: bytes,
    lines: deque[str],
    error_lines: deque[str],
) -> None:
    line = line_bytes.decode("utf-8", errors="replace").strip()
    if not line:
        return
    if is_stderr_error_line(line):
        bounded_line = line[:_STDERR_LINE_LOG_CHARS]
        error_lines.append(bounded_line)
        logger.error("claude stderr: %s", bounded_line)
    elif line.startswith(_STDERR_STATUS_PREFIXES):
        bounded_line = line[:_STDERR_LINE_LOG_CHARS]
        lines.append(bounded_line)
        logger.debug("claude stderr: %s", bounded_line)
    else:
        bounded_line = line[:_STDERR_LINE_LOG_CHARS]
        lines.append(bounded_line)
        logger.debug("claude stderr: %s", bounded_line)


def _stderr_error_reason(lines: deque[str], error_lines: deque[str]) -> str:
    """错误行优先，避免长段噪声把真实异常挤出退出详情。"""
    return next(
        (
            line
            for line in [*reversed(error_lines), *reversed(lines)]
            if "SessionEnd hook" not in line
        ),
        "no error detail from Claude CLI",
    )


def _child_transcripts(cwd: str, parent_id: str, env: dict[str, str]) -> dict[str, Path]:
    project = str(Path(cwd).resolve()).replace("/", "-")
    projects = Path(env.get("HOME") or Path.home()) / ".claude" / "projects"
    directory = projects / project / parent_id / "subagents"
    if not directory.is_dir():
        directory = next(projects.glob(f"*/{parent_id}/subagents"), directory)
    try:
        return {
            path.stem.removeprefix("agent-"): path
            for path in directory.glob("agent-*.jsonl")
            if path.is_file()
        }
    except OSError:
        return {}


def _child_transcript_complete(path: Path) -> bool:
    try:
        with path.open("rb") as stream:
            size = stream.seek(0, 2)
            stream.seek(max(size - 64 * 1024, 0))
            lines = stream.read().decode("utf-8", errors="replace").splitlines()
    except OSError:
        return False
    for line in reversed(lines):
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("type") == "assistant":
            return (row.get("message") or {}).get("stop_reason") == "end_turn"
    return False


def _child_agent_type(path: Path) -> str:
    try:
        with path.open("rb") as stream:
            lines = stream.read(64 * 1024).decode("utf-8", errors="replace").splitlines()
    except OSError:
        return "general-purpose"
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        value = row.get("attributionAgent")
        if isinstance(value, str) and value:
            return value
    return "general-purpose"


def _start_child_poller(
    hooks: DriverHooks | None,
    cwd: str,
    proc_env: dict[str, str],
    id_ready: asyncio.Event,
    get_native_id: Callable[[], str],
    preexisting_children: set[str],
) -> asyncio.Task[None] | None:
    """hooks 挂了子代理回调时启动 CLI 子代理轮询任务，否则返回 None。"""
    if not (hooks and (hooks.on_subagent_start or hooks.on_subagent_stop)):
        return None
    return asyncio.create_task(
        _poll_cli_subagents(
            hooks,
            cwd,
            proc_env,
            id_ready,
            get_native_id,
            preexisting_children,
        )
    )


async def _poll_cli_children(
    hooks: DriverHooks,
    paths: dict[str, Path],
    preexisting: set[str],
    started: set[str],
    stopped: set[str],
    agent_types: dict[str, str],
) -> None:
    """一轮 transcript 扫描：跳过 resume 前已存在的子进程，其余逐个推进。"""
    for agent_id, path in paths.items():
        if agent_id in preexisting:
            continue
        await _poll_cli_child_agent(hooks, agent_id, path, started, stopped, agent_types)


async def _poll_cli_subagents(
    hooks: DriverHooks,
    cwd: str,
    env: dict[str, str],
    id_ready: asyncio.Event,
    get_parent_id: Any,
    preexisting: set[str],
) -> None:
    await id_ready.wait()
    parent_id = get_parent_id()
    started: set[str] = set()
    stopped: set[str] = set()
    agent_types: dict[str, str] = {}
    try:
        while True:
            paths = await asyncio.to_thread(_child_transcripts, cwd, parent_id, env)
            await _poll_cli_children(hooks, paths, preexisting, started, stopped, agent_types)
            await asyncio.sleep(0.4)
    finally:
        await _fire_interrupted_stops(hooks, started, stopped, agent_types)


async def _poll_cli_child_agent(
    hooks: DriverHooks,
    agent_id: str,
    path: Path,
    started: set[str],
    stopped: set[str],
    agent_types: dict[str, str],
) -> None:
    """单个 CLI 子代理的一轮轮询：未见过的发 start，transcript 完成的发 stop。"""
    from workflow_controller.core._subagent import summarize_subagent_transcript

    if agent_id not in started:
        started.add(agent_id)
        agent_types[agent_id] = await asyncio.to_thread(_child_agent_type, path)
        if hooks.on_subagent_start:
            await hooks.on_subagent_start(
                hooks.session_id,
                {
                    "agent_id": agent_id,
                    "agent_type": agent_types[agent_id],
                },
            )
    elif agent_types[agent_id] == "general-purpose":
        discovered = await asyncio.to_thread(_child_agent_type, path)
        if discovered != "general-purpose":
            agent_types[agent_id] = discovered
            if hooks.on_subagent_start:
                await hooks.on_subagent_start(
                    hooks.session_id,
                    {"agent_id": agent_id, "agent_type": discovered},
                )
    if agent_id not in stopped and await asyncio.to_thread(_child_transcript_complete, path):
        stopped.add(agent_id)
        if hooks.on_subagent_stop:
            total_tokens, content = await asyncio.to_thread(
                summarize_subagent_transcript,
                str(path),
            )
            await hooks.on_subagent_stop(
                hooks.session_id,
                {
                    "agent_id": agent_id,
                    "agent_type": agent_types[agent_id],
                    "total_tokens": total_tokens,
                    "content": content,
                },
            )


async def _fire_interrupted_stops(
    hooks: DriverHooks,
    started: set[str],
    stopped: set[str],
    agent_types: dict[str, str],
) -> None:
    """主会话中断后在途子代理补发 interrupted stop（见 _poll_cli_subagents）。"""
    # 主会话被暂停/停止/崩溃时本 poller 被 cancel，仍在途的子代理收不到
    # end_turn 的 stop；这里补发 interrupted stop，避免 SubagentTracker
    # 永久滞留（阻塞单交接路径与 on_stop_hook 的 pending 判定）。
    if hooks.on_subagent_stop is None:
        return
    for agent_id in sorted(started - stopped):
        try:
            await hooks.on_subagent_stop(
                hooks.session_id,
                {
                    "agent_id": agent_id,
                    "agent_type": agent_types[agent_id],
                    "status": "interrupted",
                    "total_tokens": 0,
                    "content": "主会话中断时子代理仍在途",
                },
            )
        except Exception:
            logger.exception("subagent interrupted stop failed for %s", agent_id)


# ── JSON line → Event conversion (module-level, driver-internal) ───────────


def _line_events(
    line_bytes: bytes, session_id: str, iteration: int, usage_tracker: dict[str, Any] | None = None
) -> list[Event]:
    """Decode one stdout line and convert it to events (empty line → [])."""
    line = line_bytes.decode("utf-8", errors="replace").strip()
    if not line:
        return []
    return _cli_line_to_events(line, session_id, iteration, usage_tracker)


def _cli_line_to_events(
    line: str, session_id: str, iteration: int, usage_tracker: dict[str, Any] | None = None
) -> list[Event]:
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        logger.debug("CLI driver: failed to parse JSON line: %s", line[:200])
        return []

    if not isinstance(data, dict):
        return []

    msg_type = data.get("type", "")

    if msg_type == "assistant":
        from workflow_controller.session.events import Event, EventType

        events = _parse_assistant(data, session_id, iteration)
        msg = data.get("message") or {}
        # 子代理消息（sidechain）不代表主会话占用；无效读数不覆盖上一个有效快照
        msg_usage = msg.get("usage")
        if not data.get("parent_tool_use_id") and context_input_tokens(msg_usage) is not None:
            if usage_tracker is not None:
                usage_tracker["usage"] = dict(msg_usage)
            events.append(
                Event.create(
                    session_id=session_id,
                    iteration=iteration,
                    event_type=EventType.CONTEXT_USAGE,
                    data={"context_usage": dict(msg_usage), "model": msg.get("model", "")},
                )
            )
        return events
    if msg_type == "system":
        return [_parse_system(data, session_id, iteration)]
    if msg_type == "result":
        return _parse_result(
            data,
            session_id,
            iteration,
            context_usage=usage_tracker.get("usage") if usage_tracker else None,
        )
    if msg_type == "user":
        return [_parse_user(data, session_id, iteration)]
    if msg_type == "rate_limit_event":
        return [_parse_rate_limit(data, session_id, iteration)]
    return []


def _parse_assistant(data: dict[str, Any], session_id: str, iteration: int) -> list[Event]:
    from workflow_controller.session.events import Event, EventType

    msg = data.get("message", {})
    msg_blocks: list[dict[str, Any]] = []
    tool_uses: dict[str, dict[str, Any]] = {}
    tool_results: dict[str, dict[str, Any]] = {}
    for b in msg.get("content", []):
        bt = b.get("type", "")
        if bt == "text":
            text = b.get("text", "")
            if text.strip():
                msg_blocks.append({"type": "text", "text": text})
        elif bt == "tool_use":
            block = {
                "type": "tool_use",
                "id": b.get("id", ""),
                "name": b.get("name", ""),
                "input": b.get("input", {}),
            }
            tool_uses[block["id"]] = block
        elif bt == "tool_result":
            block = {
                "type": "tool_result",
                "tool_use_id": b.get("tool_use_id", ""),
                "content": b.get("content"),
                "is_error": b.get("is_error", False),
            }
            tool_results[block["tool_use_id"]] = block
        elif bt == "thinking":
            thinking = b.get("thinking", "")
            if thinking.strip():
                msg_blocks.append({"type": "thinking", "thinking": thinking})
    events: list[Event] = []
    if msg_blocks:
        events.append(
            Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.ASSISTANT_MESSAGE,
                data={"content": msg_blocks, "model": msg.get("model", "")},
            )
        )
    return events + tool_pair_events(
        tool_uses, tool_results, session_id=session_id, iteration=iteration
    )


def _parse_system(data: dict[str, Any], session_id: str, iteration: int) -> Event:
    from workflow_controller.session.events import Event, EventType

    # CLI stdout `type=system` 行按 subtype 分流：
    # - subtype=init（带 session_id 的起始消息）→ ITERATION_START（resume 前置，
    #   _register_session observe 此类型存 native_session_id）
    # - 其余（thinking_tokens 等纯展示性 system 消息）→ NOTIFICATION 拋余桶
    subtype = data.get("subtype", "")
    if subtype == "init" and data.get("session_id"):
        return Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.ITERATION_START,
            data=dict(data),
        )
    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.NOTIFICATION,
        data=dict(data),
    )


def _parse_result(
    data: dict[str, Any],
    session_id: str,
    iteration: int,
    context_usage: dict[str, Any] | None = None,
) -> list[Event]:
    from workflow_controller.session.events import Event, EventType

    sf_data = {
        "session_id": data.get("session_id", ""),
        "duration_ms": data.get("duration_ms", 0),
        "num_turns": data.get("num_turns", 0),
        "total_cost_usd": float(data.get("total_cost_usd", 0) or 0),
        "is_error": data.get("is_error", False),
        "usage": data.get("usage", {}),
        "context_usage": context_usage or data.get("usage", {}),
        "modelUsage": data.get("modelUsage", {}),
    }
    return [
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.STEP_FINISH,
            data=sf_data,
        ),
    ]


def _parse_user(data: dict[str, Any], session_id: str, iteration: int) -> Event:
    from workflow_controller.session.events import Event, EventType

    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.USER_MESSAGE,
        data={"content": extract_user_message_blocks(data.get("message", {}).get("content", []))},
    )


def _parse_rate_limit(data: dict[str, Any], session_id: str, iteration: int) -> Event:
    from workflow_controller.session.events import Event, EventType

    info = data.get("rate_limit_info", {})
    limit_signal = _build_signal_from_rate_limit_info(info)
    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.RATE_LIMIT,
        data=limit_signal.to_event_data(raw=info),
    )


def _build_signal_from_rate_limit_info(info: dict[str, Any]) -> LimitSignal:
    """Translate a claude_cli structured `rate_limit_info` payload into a
    signal. Field names mirror the Anthropic CLI rate_limit_event line:
    `status` ∈ {allowed, allowed_warning, rejected, …}, `rateLimitType` ∈
    {five_hour, seven_day}, `resetsAt` (epoch s), `utilization` (0..1).

    The utilization-warn threshold is enforced in the detector (policy), so
    any allowed_warning becomes kind=warning; the detector drops low-util
    ones. Unknown `status`/`rateLimitType` map to unknown / 5h-fallback
    matching the prior detector behavior.
    """
    from datetime import UTC, datetime

    status = info.get("status")
    rate_limit_type = info.get("rateLimitType")
    resets_at_epoch = info.get("resetsAt")
    utilization = info.get("utilization")

    reset_dt = datetime.fromtimestamp(resets_at_epoch, tz=UTC) if resets_at_epoch else None

    if status == "allowed":
        return LimitSignal(
            kind="allowed",
            scope=rate_limit_type or None,
            reset_at=reset_dt,
            utilization=utilization,
            source="cli:rate_limit_event:allowed",
        )
    if status == "allowed_warning":
        return LimitSignal(
            kind="warning",
            scope=rate_limit_type or "unknown",
            reset_at=reset_dt,
            utilization=utilization,
            source="cli:rate_limit_event:allowed_warning",
        )
    if status == "rejected":
        return _rejected_signal(rate_limit_type, reset_dt, utilization)
    return LimitSignal(
        kind="unknown",
        scope=rate_limit_type,
        reset_at=reset_dt,
        utilization=utilization,
        source=f"cli:rate_limit_event:{status}",
    )


def _rejected_signal(
    rate_limit_type: Any,
    reset_dt: datetime | None,
    utilization: Any,
) -> LimitSignal:
    """rejected 状态按 rateLimitType 分流（seven_day→weekly，未知按 5h 回退）。"""
    if rate_limit_type == "seven_day":
        return LimitSignal(
            kind="rejected",
            scope="weekly",
            reset_at=None,
            utilization=utilization,
            source="cli:rate_limit_event:rejected:seven_day",
        )
    if rate_limit_type == "five_hour":
        return LimitSignal(
            kind="rejected",
            scope="5h",
            reset_at=reset_dt,
            utilization=utilization,
            source="cli:rate_limit_event:rejected:five_hour",
        )
    return LimitSignal(  # unknown rateLimitType — treat as 5h (prior fallback)
        kind="rejected",
        scope="5h",
        reset_at=reset_dt,
        utilization=utilization,
        source=f"cli:rate_limit_event:rejected:{rate_limit_type}",
    )
