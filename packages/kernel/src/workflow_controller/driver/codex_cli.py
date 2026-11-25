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

"""CodexCLIDriver — spawns `codex exec --json` and parses JSONL events."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

from workflow_controller.driver.cli_base import (
    CLIDriverBase,
    QueryOptions,
    drain_proc_stderr,
    error_end_events,
    is_stderr_error_line,
    iter_proc_line_batches,
    kill_proc_group,
    spawn_failure_events,
)
from workflow_controller.driver.codex_catalog import (
    INVALID_MODEL_CATALOG_ERROR,
    validate_provider_catalog,
)
from workflow_controller.driver.codex_store import SubagentProgressMonitor

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from workflow_controller.driver.hooks import DriverHooks
    from workflow_controller.driver.runtime_gate import PerQueryGate
    from workflow_controller.session.events import Event

logger = logging.getLogger(__name__)

_KNOWN_SANDBOX_MODES = {"read-only", "workspace-write", "danger-full-access"}
_STDERR_LINE_LOG_CHARS = 300
_STDERR_TAIL_LINES = 20
_STDERR_ERROR_TAIL_LINES = 20
_STDERR_STATUS_PREFIXES = (
    "Reading additional input from stdin...",
    "[cpx-cli]",
)

_CODEX_SECRET_PREFIXES = (
    "ANTHROPIC_",
    "OPENCODE_",
    "CLAUDE_CODE_",
)
_CODEX_PROVIDER_SECRET_PREFIXES = ("WORKFLOW_CONTROLLER_CODEX_",)
_CODEX_PROVIDER_SECRET_KEYS = {"OPENAI_API_KEY"}
_CODEX_INTERNAL_PROVIDER_FIELDS = {"model_catalog_path"}
_CODEX_HOOK_ENV_KEYS = {
    "WORKFLOW_CONTROLLER_HOOK_SERVER_URL",
}
_HOOK_TRUST_WARNING_MARKERS = (
    "--dangerously-bypass-hook-trust",
    "Enabled hooks may run without review for this invocation.",
)
_RATE_LIMIT_ERROR_MARKERS = ("rate limit", "too many requests")
_OPENAI_RESPONSES_PROTOCOL_MARKERS = (
    "body.messages: field required",
    "body.tool_choice: input should be a valid dict",
    "missing `messages` parameter",
)
_GATEWAY_RESPONSES_MODEL_UNSUPPORTED_ERROR = (
    "gateway does not support this model with OpenAI Responses format"
)
_AUTH_STATUS_RE = re.compile(r"\b(?:401|403)\b")
_RATE_LIMIT_STATUS_RE = re.compile(r"\b429\b")
GATE_TOOL_TIMEOUT_SECONDS = 24 * 60 * 60


def _responses_base_url(base_url: str) -> str:
    """Normalize a gateway root URL to Codex's versioned Responses root."""
    parts = urlsplit(base_url)
    if parts.path in {"", "/"}:
        return urlunsplit((parts.scheme, parts.netloc, "/v1", parts.query, parts.fragment))
    return base_url


def _codex_config_overrides(
    base_url: str,
    model_catalog: Path | None = None,
    gate_url: str | None = None,
) -> list[str]:
    if not base_url:
        raise ValueError("codex_cli requires an explicit provider base_url")

    overrides = [
        "-c",
        'model_provider="workflow_controller"',
        "-c",
        f"model_providers.workflow_controller.name={json.dumps('Workflow Controller provider')}",
        "-c",
        f"model_providers.workflow_controller.base_url={json.dumps(_responses_base_url(base_url))}",
        "-c",
        'model_providers.workflow_controller.env_key="WORKFLOW_CONTROLLER_CODEX_API_KEY"',
        "-c",
        'model_providers.workflow_controller.wire_api="responses"',
    ]
    if model_catalog is not None:
        overrides.extend(
            [
                "-c",
                f"model_catalog_json={json.dumps(str(model_catalog))}",
                "-c",
                'web_search="disabled"',
            ]
        )
    if gate_url:
        overrides.extend(
            [
                "-c",
                f"mcp_servers.workflow_controller_gate.url={json.dumps(gate_url)}",
                "-c",
                "mcp_servers.workflow_controller_gate.required=true",
                "-c",
                "mcp_servers.workflow_controller_gate.tool_timeout_sec="
                f"{GATE_TOOL_TIMEOUT_SECONDS}",
                "-c",
                "mcp_servers.workflow_controller_gate.tools."
                'workflow_controller_ask.approval_mode="approve"',
            ]
        )
    return overrides


def _resolve_model_catalog(
    provider_env: dict[str, str] | None,
    model: str,
) -> Path | None:
    """Resolve the provider-bound Codex model catalog."""
    model_catalog_path = str(
        (provider_env or {}).get(
            "model_catalog_path",
            "",
        )
    ).strip()
    if not model_catalog_path:
        return None

    path = validate_provider_catalog(model_catalog_path, model)
    if path is None:
        raise ValueError(INVALID_MODEL_CATALOG_ERROR)
    return path


def _codex_proc_env(
    env: dict[str, str] | None,
    cwd: str | None = None,
) -> dict[str, str]:
    """Build a process-private Codex environment over a shared config home."""
    provider_env = env or {}
    api_key = str(provider_env.get("ANTHROPIC_API_KEY", "")).strip()
    if not api_key:
        raise ValueError("codex_cli requires a provider API key")

    merged = {**os.environ, **(env or {})}
    projected = {}
    for key, value in merged.items():
        is_secret_key = key.startswith(_CODEX_SECRET_PREFIXES) or key.startswith(
            _CODEX_PROVIDER_SECRET_PREFIXES
        )
        is_internal_key = (
            key in _CODEX_PROVIDER_SECRET_KEYS
            or key in _CODEX_INTERNAL_PROVIDER_FIELDS
            or key in _CODEX_HOOK_ENV_KEYS
        )
        if not is_secret_key and not is_internal_key:
            projected[key] = value

    base_url = str(provider_env.get("ANTHROPIC_BASE_URL", "")).strip()
    if not base_url:
        raise ValueError("codex_cli requires an explicit provider base_url")

    codex_home = (Path(cwd) if cwd else Path.cwd()).expanduser().resolve() / ".codex"
    codex_home.mkdir(parents=True, exist_ok=True)
    projected["CODEX_HOME"] = str(codex_home)
    projected["WORKFLOW_CONTROLLER_CODEX_API_KEY"] = api_key
    return projected


def _codex_hook_invocation(
    hooks: DriverHooks | None,
) -> dict[str, str]:
    """构造受控 Codex hook 环境。"""
    validator = hooks.runtime_hook_config_valid if hooks else None
    if validator is None:
        return {}
    if not validator():
        raise ValueError("codex_cli hooks must stay managed")
    server_url = hooks.file_change_hook_url if hooks else None
    if not server_url:
        raise ValueError("codex_cli hooks require HookServer url")
    return {
        "WORKFLOW_CONTROLLER_HOOK_SERVER_URL": server_url,
    }


def _validate_codex_run(
    opts: QueryOptions,
) -> tuple[str, Path | None, dict[str, str], dict[str, str]]:
    """校验调用参数并返回 provider 与子进程环境。"""
    # 当前稳定版 Codex 无 --system-prompt，暂显式拒绝非空 system_prompt；
    # 待官方提供该 flag 或验证 instructions 注入语义后再支持转发（届时再进入 _build_args）。
    if opts.system_prompt:
        raise ValueError("codex_cli does not yet support system_prompt")
    # Codex 无稳定 turn-cap flag，也没有通用工具名 allow/deny 等价物，
    # 非空即显式拒绝（fail-closed），避免悄悄放宽；待目标版本提供可映射 flag 再透传。
    if opts.max_turns:
        raise ValueError("codex_cli does not yet support max_turns")
    if opts.allowed_tools:
        raise ValueError("codex_cli does not yet support allowed_tools")
    if opts.disallowed_tools:
        raise ValueError("codex_cli does not yet support disallowed_tools")
    if not opts.model:
        raise ValueError("codex_cli requires an explicit model")
    provider_env = opts.env or {}
    base_url = str(provider_env.get("ANTHROPIC_BASE_URL", "")).strip()
    model_catalog = _resolve_model_catalog(provider_env, opts.model)
    proc_env = _codex_proc_env(provider_env, opts.cwd)
    hook_env = _codex_hook_invocation(opts.hooks)
    proc_env.update(hook_env)
    return base_url, model_catalog, proc_env, hook_env


def _validated_codex_run(
    opts: QueryOptions,
    session_id: str,
    iteration: int,
) -> tuple[str, Path | None, dict[str, str], dict[str, str]] | list[Event]:
    try:
        return _validate_codex_run(opts)
    except ValueError as e:
        return error_end_events(
            str(e),
            session_id=session_id,
            iteration=iteration,
            end_reason="rejected_params",
        )


def _create_codex_gate(opts: QueryOptions, session_id: str) -> PerQueryGate | None:
    hooks = opts.hooks
    if hooks is None or hooks.on_gate_question is None:
        return None

    from workflow_controller.driver.runtime_gate import PerQueryGate

    return PerQueryGate(
        hooks=hooks,
        session_id=session_id,
        require_callback=True,
    )


async def _start_codex_gate(
    gate: PerQueryGate | None,
    session_id: str,
    iteration: int,
) -> list[Event]:
    if gate is None:
        return []
    try:
        await gate.start()
    except Exception as exc:
        logger.exception("Codex gate failed to start session=%s", session_id)
        return error_end_events(
            str(exc),
            session_id=session_id,
            iteration=iteration,
            end_reason="gate_failed",
        )
    return []


def _prepare_codex_run(
    prompt: str,
    opts: QueryOptions,
    driver: CodexCLIDriver,
    prepared: tuple[str, Path | None, dict[str, str], dict[str, str]],
    gate_url: str | None = None,
) -> tuple[list[str], dict[str, str], dict[str, str]]:
    """装配 argv 与子进程环境。"""
    base_url, model_catalog, proc_env, hook_env = prepared
    config_overrides = _codex_config_overrides(base_url, model_catalog, gate_url)
    args = _build_args(
        bin_path=driver.codex_bin,
        prompt=prompt,
        model=opts.model,
        cwd=opts.cwd,
        permission_mode=opts.permission_mode or driver.permission_mode,
        add_dirs=driver.add_dirs,
        resume=driver.resume_ref,
        fork_session=driver.fork_session,
        config_overrides=config_overrides,
        bypass_hook_trust=bool(hook_env),
        command_prefix=driver.command_prefix,
    )
    return args, proc_env, hook_env


class CodexCLIDriver(CLIDriverBase):
    """Runs Codex via the `codex exec` CLI (subprocess)."""

    def __init__(
        self,
        *,
        permission_mode: str = "bypassPermissions",
        add_dirs: list[str] | None = None,
        resume: str | None = None,
        fork_session: bool = False,
        codex_bin: str = "codex",
        command_prefix: list[str] | None = None,
    ) -> None:
        super().__init__(permission_mode=permission_mode)
        self._add_dirs: list[str] = add_dirs or []
        self._resume: str | None = resume
        self._fork_session: bool = fork_session
        self._codex_bin: str = codex_bin
        self._command_prefix: list[str] = command_prefix or []

    @property
    def codex_bin(self) -> str:
        """本 driver 启动的 codex 可执行文件。"""
        return self._codex_bin

    @property
    def add_dirs(self) -> list[str]:
        """codex 查询可访问的附加目录。"""
        return self._add_dirs

    @property
    def command_prefix(self) -> list[str]:
        """codex 子进程的命令前缀（如包装器/容器）。"""
        return self._command_prefix

    @property
    def fork_session(self) -> bool:
        """续接时是否派生新会话而非原会话。"""
        return self._fork_session

    @property
    def resume_ref(self) -> str | None:
        """默认续接的 codex 会话 id。"""
        return self._resume

    @property
    def permission_mode(self) -> str:
        """构造时给定的默认权限模式（沙箱模式）。"""
        return self._permission_mode

    @staticmethod
    async def _notify_subagent_starts(
        data: dict[str, Any] | None,
        agent_by_thread: dict[str, str],
        session_id: str,
        hooks: DriverHooks | None,
    ) -> None:
        """通知原生子代理启动事件。"""
        if hooks is None or hooks.on_subagent_start is None:
            return
        for start in _codex_subagent_starts(data, agent_by_thread):
            await hooks.on_subagent_start(session_id, start)

    def _query_events(self, prompt: str, opts: QueryOptions) -> AsyncIterator[Event]:
        """返回 codex exec 查询事件流。"""
        return self._query_event_stream(prompt, opts)

    async def _query_event_stream(self, prompt: str, opts: QueryOptions) -> AsyncIterator[Event]:
        """执行一次 codex exec 查询（JSONL 行解析见模块级函数）。"""
        from workflow_controller.session.events import Event, EventType

        iteration = opts.iteration
        session_id = opts.session_id
        if prompt:
            yield Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.USER_MESSAGE,
                data={"content": [{"type": "text", "text": prompt}]},
            )

        gate = None
        try:
            prepared = _validated_codex_run(opts, session_id, iteration)
            if isinstance(prepared, list):
                for event in prepared:
                    yield event
                return

            gate = _create_codex_gate(opts, session_id)
            gate_events = await _start_codex_gate(gate, session_id, iteration)
            for event in gate_events:
                yield event
            if gate_events:
                return

            args, proc_env, hook_env = _prepare_codex_run(
                prompt, opts, self, prepared, gate.url if gate is not None else None
            )
            proc, spawn_events = await self._spawn_codex_proc(args, proc_env, opts)
            for event in spawn_events:
                yield event
            if proc is None:
                return

            if hook_env:
                logger.warning("codex hook trust bypass enabled session=%s", session_id)
            self._proc = proc
            self._arm_cleanup(proc)

            async for event in self._run_proc(
                proc,
                session_id,
                iteration,
                opts.hooks,
                proc_env,
            ):
                yield event
        finally:
            if gate is not None:
                await gate.stop()

    async def _spawn_codex_proc(
        self,
        args: list[str],
        proc_env: dict[str, str],
        opts: QueryOptions,
    ) -> tuple[asyncio.subprocess.Process | None, list[Event]]:
        try:
            return await self._spawn_proc(args, env=proc_env, cwd=opts.cwd), []
        except OSError as e:
            return None, spawn_failure_events(
                e,
                args=args,
                default_command=self._codex_bin,
                command_prefix=self._command_prefix,
                log_label="codex CLI",
                session_id=opts.session_id,
                iteration=opts.iteration,
                end_reason="spawn_failed",
            )

    async def _run_proc(
        self,
        proc: asyncio.subprocess.Process,
        session_id: str,
        iteration: int,
        hooks: DriverHooks | None,
        proc_env: dict[str, str],
    ) -> AsyncIterator[Event]:
        """流式消费 codex stdout 事件流并收尾（异常退出补 ERROR、清理进程）。"""
        from workflow_controller.session.events import Event, EventType

        subagent_monitor = None
        subagent_bridge = None
        try:
            subagent_monitor = self._start_subagent_monitor(hooks, proc_env)
            if subagent_monitor is not None and hooks is not None:
                subagent_bridge = hooks.subagent_progress_bridge
                if subagent_bridge is not None:
                    subagent_bridge.register(subagent_monitor)
            async for event in self._stream_proc_events(
                proc, session_id, iteration, hooks, subagent_monitor
            ):
                yield event
        except asyncio.CancelledError:
            kill_proc_group(proc)
            raise
        except Exception:
            logger.exception("codex driver query error session=%s", session_id)
            kill_proc_group(proc)
            yield Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.ERROR,
                data={"error": "codex driver internal error"},
            )
            yield Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.ITERATION_END,
                data={"reason": "internal_error"},
            )
        finally:
            if subagent_monitor is not None:
                await subagent_monitor.finish()
            if subagent_bridge is not None:
                subagent_bridge.unregister(subagent_monitor)
            await self._teardown_proc(proc)

    async def _stream_proc_events(
        self,
        proc: asyncio.subprocess.Process,
        session_id: str,
        iteration: int,
        hooks: DriverHooks | None,
        subagent_monitor: Any | None,
    ) -> AsyncIterator[Event]:
        """关闭 stdin、起 stderr 抽取任务，驱动 stdout 事件流至进程退出收尾。"""
        if proc.stdin is None:
            raise RuntimeError("codex 子进程 stdin 未创建")
        proc.stdin.close()
        stderr_tail: deque[str] = deque(maxlen=_STDERR_TAIL_LINES)
        stderr_error_tail: deque[str] = deque(maxlen=_STDERR_ERROR_TAIL_LINES)
        self._stderr_task = asyncio.ensure_future(
            _drain_stderr(proc, stderr_tail, stderr_error_tail)
        )

        flags: dict[str, bool] = {"saw_error": False, "turn_completed": False}
        async for event in self._stream_line_events(
            proc,
            session_id,
            iteration,
            flags,
            hooks,
            subagent_monitor,
        ):
            yield event
        async for event in self._proc_exit_events(
            proc, stderr_tail, stderr_error_tail, flags, session_id, iteration
        ):
            yield event

    async def _stream_line_events(
        self,
        proc: asyncio.subprocess.Process,
        session_id: str,
        iteration: int,
        flags: dict[str, bool],
        hooks: DriverHooks | None,
        subagent_monitor: Any | None = None,
    ) -> AsyncIterator[Event]:
        """逐行产出 stdout 事件并登记原生子代理线程。"""
        agent_by_thread: dict[str, str] = {}
        async for lines in iter_proc_line_batches(proc):
            for line in lines:
                data = _parse_codex_json_object(line)
                await self._notify_subagent_starts(data, agent_by_thread, session_id, hooks)
                await _handle_subagent_fact(data, subagent_monitor)
                for event in _codex_data_events(data, session_id, iteration, flags):
                    yield event
            if flags["turn_completed"] or self._stop_event.is_set():
                return

    async def _proc_exit_events(
        self,
        proc: asyncio.subprocess.Process,
        stderr_tail: deque[str],
        stderr_error_tail: deque[str],
        flags: dict[str, bool],
        session_id: str,
        iteration: int,
    ) -> AsyncIterator[Event]:
        """等 codex 退出并排空 stderr；异常退出（无 turn 完成、无已报错）补 ERROR。"""
        from workflow_controller.session.events import Event, EventType

        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except TimeoutError:
            kill_proc_group(proc)
            await proc.wait()
        if self._stderr_task is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._stderr_task, timeout=2.0)

        is_abnormal_exit = (
            proc.returncode != 0
            and not flags["turn_completed"]
            and not self._stop_event.is_set()
            and not flags["saw_error"]
        )
        if is_abnormal_exit:
            tail_text = _stderr_tail_text(stderr_tail, stderr_error_tail)
            error = f"codex exited with code {proc.returncode}"
            if tail_text:
                error += f": {tail_text}"
            yield Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.ERROR,
                data={"error": error},
            )
        yield Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.ITERATION_END,
            data={"reason": "proc_exit"},
        )

    async def _teardown_proc(self, proc: asyncio.subprocess.Process) -> None:
        """query 收尾：停 stderr 泵，击杀进程组并解除清理钩子。"""
        stderr_task = self._stderr_task
        self._stderr_task = None
        if stderr_task is not None:
            stderr_task.cancel()
            with contextlib.suppress(BaseException):
                await stderr_task
        kill_proc_group(proc)
        self._disarm_cleanup()
        self._proc = None

    def _start_subagent_monitor(
        self,
        hooks: DriverHooks | None,
        proc_env: dict[str, str],
    ) -> Any | None:
        """启动子代理进度监视器。"""
        if hooks is None or hooks.on_subagent_progress is None:
            return None
        try:
            return SubagentProgressMonitor(
                hooks=hooks,
                session_id=hooks.session_id,
                codex_home=Path(str(proc_env.get("CODEX_HOME", ""))),
            )
        except Exception:
            logger.debug("子代理进度监视器启动失败", exc_info=True)
            return None


def _build_args(
    *,
    bin_path: str,
    prompt: str,
    model: str,
    cwd: str | None,
    permission_mode: str,
    add_dirs: list[str],
    resume: str | None,
    fork_session: bool = False,
    config_overrides: list[str] | None = None,
    bypass_hook_trust: bool = False,
    command_prefix: list[str] | None = None,
) -> list[str]:
    args = [*(command_prefix or []), bin_path, "exec", "--json", "--skip-git-repo-check"]
    if bypass_hook_trust:
        args.append("--dangerously-bypass-hook-trust")
    if cwd:
        args.extend(["--cd", cwd])
    args.extend(config_overrides or [])
    for d in add_dirs:
        if d and d != cwd:
            args.extend(["--add-dir", d])
    if model:
        args.extend(["--model", model])
    args.extend(_sandbox_args(permission_mode))

    if resume:
        if fork_session:
            args.extend(["fork", resume])
        else:
            args.extend(["resume", resume])
    elif fork_session:
        logger.warning("codex fork requested without a session id; ignoring fork")
    args.extend(["--", prompt])
    return args


def _sandbox_args(permission_mode: str) -> list[str]:
    """Translate Claude-style permission modes into Codex sandbox flags.

    Unknown modes intentionally fall back to read-only instead of widening
    permissions. Workflow Controller runs Codex non-interactively, so approval-gated actions
    fail closed rather than waiting for a human or an unavailable reviewer.
    """
    mode = (permission_mode or "").strip()
    if mode == "bypassPermissions":
        return ["--sandbox", "danger-full-access"]
    if mode == "acceptEdits":
        return ["--sandbox", "workspace-write"]
    if mode in ("", "default", "read-only"):
        return ["--sandbox", "read-only"]
    if mode in _KNOWN_SANDBOX_MODES:
        return ["--sandbox", mode]
    if mode == "plan":
        return ["--sandbox", "read-only"]
    logger.warning("unknown codex permission_mode %r; using read-only", permission_mode)
    return ["--sandbox", "read-only"]


async def _drain_stderr(
    proc: asyncio.subprocess.Process,
    tail: deque[str],
    error_tail: deque[str],
) -> None:
    """泵干 codex stderr：状态/普通行降 debug，错误行记错误日志并优先保留。"""

    def _handle(line: str) -> None:
        bounded_line = line[:_STDERR_LINE_LOG_CHARS]
        if is_stderr_error_line(line):
            error_tail.append(bounded_line)
            logger.error("codex stderr: %s", bounded_line)
        elif all(marker in line for marker in _HOOK_TRUST_WARNING_MARKERS) or line.startswith(
            _STDERR_STATUS_PREFIXES
        ):
            tail.append(bounded_line)
            logger.debug("codex stderr: %s", bounded_line)
        else:
            tail.append(bounded_line)
            logger.debug("codex stderr: %s", bounded_line)

    await drain_proc_stderr(proc, _handle)


def _stderr_tail_text(tail: deque[str], error_tail: deque[str]) -> str:
    """错误行优先，普通行限量；异常退出时避免长段噪声挤掉真实诊断。"""
    return "\n".join([*error_tail, *tail]).strip()


# ── JSONL line → Event conversion (module-level, driver-internal) ──────────


def _codex_subagent_starts(
    data: dict[str, Any] | None,
    agent_by_thread: dict[str, str],
) -> list[dict[str, str]]:
    """从已解析线程事件提取子代理启动信息和直接派发者。"""
    if data is None:
        return []
    if data.get("type") == "thread.started":
        agent_by_thread.setdefault(str(data.get("thread_id") or ""), "")
        return []
    if data.get("type") != "item.completed":
        return []
    item = data.get("item") or {}
    if not isinstance(item, dict) or item.get("type") != "collab_tool_call":
        return []
    if item.get("tool") != "spawn_agent":
        return []
    sender_id = str(item.get("sender_thread_id") or "")
    parent_id = agent_by_thread.get(sender_id, "")
    starts = []
    for receiver_id in item.get("receiver_thread_ids") or []:
        agent_id = str(receiver_id or "")
        if not agent_id:
            continue
        agent_by_thread[agent_id] = agent_id
        starts.append(
            {
                "agent_id": agent_id,
                "agent_type": str(item.get("agent_type") or "general-purpose"),
                "parent_agent_id": parent_id,
                "session_id": sender_id,
            }
        )
    return starts


def _codex_line_to_events(line: str, session_id: str, iteration: int) -> list[Event]:
    data = _parse_codex_json_object(line)
    if data is None:
        return []
    return _codex_data_to_events(data, session_id, iteration)


def _parse_codex_json_object(line: str) -> dict[str, Any] | None:
    """解析单行 JSON 对象，失败返回 None。"""
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        logger.debug("codex driver: failed to parse JSON line: %s", line[:200])
        return None
    return data if isinstance(data, dict) else None


def _codex_data_to_events(
    data: dict[str, Any] | None,
    session_id: str,
    iteration: int,
) -> list[Event]:
    """把已解析的 JSON 对象翻译为事件。"""
    if data is None:
        return []

    msg_type = data.get("type", "")
    if msg_type == "thread.started":
        return [_parse_thread_started(data, session_id, iteration)]
    if msg_type == "turn.completed":
        return [_parse_turn_completed(data, session_id, iteration)]
    if msg_type == "turn.failed":
        return _parse_turn_failed(data, session_id, iteration)
    if msg_type == "item.completed":
        return _parse_item_completed(data, session_id, iteration)
    if msg_type == "error":
        return _parse_top_level_error(data, session_id, iteration)
    # turn.started / item.started / item.updated / unknown are intentionally ignored.
    return []


def _codex_data_events(
    data: dict[str, Any] | None,
    session_id: str,
    iteration: int,
    flags: dict[str, bool],
) -> Iterator[Event]:
    """单行事件的产出与 flags 维护；STEP_FINISH 后不再产出该行剩余事件。"""
    from workflow_controller.session.events import EventType

    for event in _codex_data_to_events(data, session_id, iteration):
        if event.event_type == EventType.ERROR:
            flags["saw_error"] = True
        yield event
        if event.event_type == EventType.STEP_FINISH:
            flags["turn_completed"] = True
            return


_COLLAB_ITEM_TYPES = ("CollabAgentToolCall", "collab_agent_tool_call", "collab_tool_call")


def _collab_state_text(value: Any) -> str:
    """归一协作状态：stdout 为 status 字段，rollout 为状态名作键。"""
    if isinstance(value, dict):
        status = value.get("status")
        if isinstance(status, str):
            return status
        return next((key for key in value if isinstance(key, str)), "")
    return str(value)


def _codex_subagent_fact(data: dict[str, Any]) -> dict[str, Any] | None:
    """从父 stdout 行提取子代理协作事实。"""
    msg_type = data.get("type", "")
    if msg_type == "thread.started":
        parent_id = str(data.get("thread_id") or "")
        return {"kind": "parent_started", "parent_id": parent_id} if parent_id else None
    if msg_type != "item.completed":
        return None
    item = data.get("item")
    if not isinstance(item, dict) or item.get("type") not in _COLLAB_ITEM_TYPES:
        return None
    child_ids = [str(value) for value in item.get("receiver_thread_ids") or [] if value]
    states = item.get("agents_states")
    return {
        "kind": "collab",
        "tool": str(item.get("tool") or ""),
        "child_ids": child_ids,
        "states": (
            {str(key): _collab_state_text(value) for key, value in states.items()}
            if isinstance(states, dict)
            else {}
        ),
    }


async def _handle_subagent_fact(
    data: dict[str, Any] | None,
    monitor: Any | None,
) -> None:
    """把父流协作事实交给监视器。"""
    if data is None or monitor is None:
        return
    fact = _codex_subagent_fact(data)
    if fact is not None:
        await monitor.handle_fact(fact)


def _parse_thread_started(data: dict[str, Any], session_id: str, iteration: int) -> Event:
    from workflow_controller.session.events import Event, EventType

    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.ITERATION_START,
        data={"session_id": data.get("thread_id", ""), "subtype": "init", "model": "", "cwd": ""},
    )


def _parse_turn_completed(data: dict[str, Any], session_id: str, iteration: int) -> Event:
    from workflow_controller.session.events import Event, EventType

    usage = data.get("usage") or {}
    if not isinstance(usage, dict):
        usage = {}
    normalized = {
        "input_tokens": _as_int(usage.get("input_tokens")),
        "output_tokens": _as_int(usage.get("output_tokens")),
        "cache_creation_input_tokens": _as_int(
            usage.get("cache_write_input_tokens", usage.get("cache_creation_input_tokens"))
        ),
        "cache_read_input_tokens": _as_int(
            usage.get("cached_input_tokens", usage.get("cache_read_input_tokens"))
        ),
    }
    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.STEP_FINISH,
        data={
            "session_id": session_id,
            "duration_ms": 0,
            "num_turns": 1,
            "total_cost_usd": 0.0,
            "is_error": False,
            "usage": normalized,
            "modelUsage": {},
        },
    )


def _error_events(error: str, session_id: str, iteration: int) -> list[Event]:
    """把错误文本归一为限流事件和可观测错误事件。"""
    from typing import Literal

    from workflow_controller.core.auth_rotator.signal import LimitSignal
    from workflow_controller.session.events import Event, EventType

    if all(marker in error for marker in _HOOK_TRUST_WARNING_MARKERS):
        logger.debug("codex hook trust warning: %s", error[:300])
        return []

    lower_error = error.lower()
    if "convert_request_failed" in lower_error or lower_error.strip() == "not implemented":
        error = _GATEWAY_RESPONSES_MODEL_UNSUPPORTED_ERROR
    if any(marker in lower_error for marker in _OPENAI_RESPONSES_PROTOCOL_MARKERS):
        error = "response is not OpenAI Responses format"
    kind: Literal["auth_error", "rejected"]
    scope: Literal["auth_error", "rate_limit"]
    if _AUTH_STATUS_RE.search(lower_error) or "authentication failed" in lower_error:
        kind, scope = "auth_error", "auth_error"
    elif _RATE_LIMIT_STATUS_RE.search(lower_error) or any(
        marker in lower_error for marker in _RATE_LIMIT_ERROR_MARKERS
    ):
        kind, scope = "rejected", "rate_limit"
    else:
        return [
            Event.create(
                session_id=session_id,
                iteration=iteration,
                event_type=EventType.ERROR,
                data={"error": error},
            )
        ]

    limit_signal = LimitSignal(
        kind=kind,
        scope=scope,
        reset_at=None,
        utilization=None,
        source="codex_cli:error:text",
    )
    return [
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.RATE_LIMIT,
            data=limit_signal.to_event_data(raw=error),
        ),
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.ERROR,
            data={"error": error},
        ),
    ]


def _parse_turn_failed(data: dict[str, Any], session_id: str, iteration: int) -> list[Event]:
    return _error_events(_error_text(data.get("error")), session_id, iteration)


def _parse_item_completed(data: dict[str, Any], session_id: str, iteration: int) -> list[Event]:
    item = data.get("item") or {}
    if not isinstance(item, dict):
        return []

    item_type = item.get("type", "")
    if item_type in ("agent_message", "reasoning"):
        return _item_text_events(item_type, item, session_id, iteration)
    if item_type == "command_execution":
        return _command_execution_events(item, session_id, iteration)
    if item_type == "mcp_tool_call":
        return _parse_mcp_tool_call(item, session_id, iteration)
    if item_type in _ITEM_NOTIFICATION_FIELDS:
        return [_item_notification_event(item_type, item, session_id, iteration)]
    if item_type == "error":
        return _error_events(
            str(item.get("message", "") or "codex item error"),
            session_id,
            iteration,
        )
    return []


def _item_text_events(
    item_type: str,
    item: dict[str, Any],
    session_id: str,
    iteration: int,
) -> list[Event]:
    """agent_message / reasoning → ASSISTANT_MESSAGE（text / thinking block，空文本不发）。"""
    from workflow_controller.session.events import Event, EventType

    text = str(item.get("text", "") or "")
    if not text.strip():
        return []
    block = (
        {"type": "text", "text": text}
        if item_type == "agent_message"
        else {"type": "thinking", "thinking": text}
    )
    return [
        Event.create(
            session_id=session_id,
            iteration=iteration,
            event_type=EventType.ASSISTANT_MESSAGE,
            data={"content": [block], "model": ""},
        )
    ]


def _command_execution_events(
    item: dict[str, Any],
    session_id: str,
    iteration: int,
) -> list[Event]:
    """command_execution item → Bash 工具三事件（use / result / call）。"""
    tool_input = {"command": str(item.get("command", "") or "")}
    output = str(item.get("aggregated_output", "") or "")
    exit_code = item.get("exit_code")
    is_error = item.get("status") == "failed" or exit_code not in (None, 0, "0")
    return _tool_events(
        tool_id=str(item.get("id", "") or ""),
        tool_name="Bash",
        tool_input=tool_input,
        content=output,
        is_error=bool(is_error),
        session_id=session_id,
        iteration=iteration,
    )


# file_change / web_search / todo_list → NOTIFICATION；value 为 (字段名, 缺省值)
_ITEM_NOTIFICATION_FIELDS: dict[str, tuple[str, object]] = {
    "file_change": ("changes", []),
    "web_search": ("query", ""),
    "todo_list": ("items", []),
}


def _item_notification_event(
    item_type: str,
    item: dict[str, Any],
    session_id: str,
    iteration: int,
) -> Event:
    """file_change / web_search / todo_list → NOTIFICATION（data.type 即 item 类型）。"""
    from workflow_controller.session.events import Event, EventType

    field, default = _ITEM_NOTIFICATION_FIELDS[item_type]
    return Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.NOTIFICATION,
        data={"type": item_type, field: item.get(field, default)},
    )


def _parse_mcp_tool_call(item: dict[str, Any], session_id: str, iteration: int) -> list[Event]:
    tool_name = str(item.get("tool", "") or "")
    if not tool_name:
        server = str(item.get("server", "") or "")
        tool_name = f"{server}/tool" if server else "mcp_tool"

    tool_input = item.get("arguments") or {}
    if not isinstance(tool_input, dict):
        tool_input = {"arguments": tool_input}

    result = item.get("result") or {}
    error = item.get("error") or {}
    status = item.get("status")
    is_error = status == "failed" or bool(error)
    content = _error_text(error) or "MCP tool call failed" if is_error else _result_text(result)

    return _tool_events(
        tool_id=str(item.get("id", "") or ""),
        tool_name=tool_name,
        tool_input=tool_input,
        content=content,
        is_error=bool(is_error),
        session_id=session_id,
        iteration=iteration,
    )


def _parse_top_level_error(data: dict[str, Any], session_id: str, iteration: int) -> list[Event]:
    error = _error_text(data.get("message", data.get("error")))
    return _error_events(error, session_id, iteration)


def _tool_events(
    *,
    tool_id: str,
    tool_name: str,
    tool_input: dict[str, Any],
    content: str,
    is_error: bool,
    session_id: str,
    iteration: int,
) -> list[Event]:
    from workflow_controller.session.events import (
        Event,
        EventType,
        extract_tool_arg,
        extract_tool_error,
    )

    use_event = Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.TOOL_USE,
        data={"id": tool_id, "name": tool_name, "input": tool_input},
    )
    result_event = Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.TOOL_RESULT,
        data={
            "tool_use_id": tool_id,
            "tool_name": tool_name,
            "content": content,
            "is_error": is_error,
        },
    )
    call_data: dict[str, Any] = {
        "name": tool_name,
        "arg": extract_tool_arg(tool_input),
        "is_error": is_error,
    }
    if is_error:
        error_reason = extract_tool_error(content)
        if error_reason:
            call_data["error"] = error_reason
    call_event = Event.create(
        session_id=session_id,
        iteration=iteration,
        event_type=EventType.TOOL_CALL,
        data=call_data,
    )
    return [use_event, result_event, call_event]


def _result_text(result: object) -> str:
    if isinstance(result, str):
        return result
    if isinstance(result, dict):
        content = result.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return _content_list_text(content)
        structured = result.get("structured_content")
        if structured is not None:
            return json.dumps(structured, ensure_ascii=False, default=str)
        return ""
    if result is None:
        return ""
    return str(result)


def _content_list_text(content: list[Any]) -> str:
    parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict):
            value = block.get("text") or block.get("content") or block.get("value")
            if value:
                parts.append(str(value))
        else:
            parts.append(str(block))
    return "\n".join(part for part in parts if part)


def _error_text(error: object) -> str:
    if isinstance(error, dict):
        message = error.get("message")
        if message:
            return str(message)
        return json.dumps(error, ensure_ascii=False, default=str)
    if error is None:
        return ""
    return str(error)


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return 0
