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

"""Monitor child contexts and hand off an entire in-flight group."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple
from uuid import uuid4

from workflow_controller.plugins.breather.handoff import (
    HANDOFF_AGENT_PROMPT,
    HandoffGenerator,
    build_birth_handoff_doc_section,
    handoff_template_for_mode,
    handoff_template_is_sectioned,
    handoff_template_section,
)
from workflow_controller.session.events import Event, EventType
from workflow_controller.session.session import SessionState

if TYPE_CHECKING:
    from workflow_controller.plugins.breather.plugin import BreatherPlugin

logger = logging.getLogger(__name__)

GROUP_HANDOFF_PROMPT = (
    "你是独立的子代理整体交接 session。主 session 和所有在途子代理已暂停。"
    "先读取 {manifest_path}，再逐个读取其中的 source 文件并核对项目实际文件。"
    "每个 entry 必须在 handoff 路径写一份 Markdown 文档，记录原任务、"
    "已完成且核实的工作、尚未完成的工作、下一步和文件状态；"
    "下一步要写出新子代理读完该文档后可执行的具体断点步骤。"
    "若 main_handoff 非空，还要读取 main_source、main_events 并在 main_handoff 写主代理交接摘要；"
    "摘要须逐项列出每个 entry 的 handoff 路径与 dispatch_prompt，"
    "明确要求主代理把各自的 dispatch_prompt 传入对应新子代理。"
    "若 birth_handoff_doc 非空，先读取上一份主代理交接文档，继承已有发现、决策和踩过的坑。"
    "source 为空时根据项目文件和主 session 事件尽力盘点，并标记证据缺口。"
    "不要把中断中的工具调用视为已完成。"
    "源文件可能很大，先读开头的任务和最近的消息，再按需检索中间记录。"
    "全部文档写完并自检后，最后写入 {done_path}，内容为 done。"
    "可选的后续任务与待决事项写在交接文档中，不在最终回复中征询用户是否继续。"
    "若确有阻塞且必须由用户决定，明确提出具体问题；否则最终回复只写："
    "整组交接文档已写入清单指定路径，完成信号已写入 {done_path}。"
)


class _MonitorTargets(NamedTuple):
    """单轮 group 监控的目标与监控参数。"""

    session: Any
    agents: dict[str, Any]
    parent_id: str
    runtime: str
    env: dict[str, str]
    window: int


class _MainSnapshot(NamedTuple):
    """主 session 的事件与源码快照落盘结果。"""

    events: Path
    source: Path
    found: bool


class _HandoffSetup(NamedTuple):
    """整体交接前半程的结果（目录布局与主代理用量判定）。"""

    agents: dict[str, Any]
    root: Path
    batch: str
    handoff_number: int
    main_handoff: Path
    done_path: Path
    parent_id: str
    main_over: bool


class _HandoffFiles(NamedTuple):
    """落盘的交接产物：manifest entry 列表、manifest 与交接 prompt 文件。"""

    entries: list[dict[str, Any]]
    manifest: Path
    prompt_file: Path


@dataclass
class _HandoffOutcome:
    """失败恢复所需的进度标记（断在哪一步决定恢复动作）。"""

    paused: bool = False
    successor_started: bool = False
    parent_stopped: bool = False


def _manually_stopped(services: Any, session_id: str) -> bool:
    session = services.find_session(session_id)
    return session is None or session.pause_requested or session.stop_reason == "user_pause"


def _naturally_ended(services: Any, session_id: str) -> bool:
    session = services.find_session(session_id)
    if session is None:
        return False
    if session.pause_requested or session.stop_reason == "user_pause":
        return False
    if getattr(session, "loop_status", None) == "no":
        return True
    return session.state in (
        SessionState.STOPPED,
        SessionState.COMPLETED,
        SessionState.FAILED,
    )


def _new_subagent_ids(events: list[Any], known_ids: set[str]) -> set[str]:
    new_ids: set[str] = set()
    for e in events:
        if e.event_type == EventType.SUBAGENT_START and e.data.get("agent_id") not in known_ids:
            new_ids.add(e.data.get("agent_id"))
    return new_ids


def _dispatch_prompts(events: list[Any]) -> list[str]:
    prompts: list[str] = []
    for e in events:
        if not (
            e.event_type == EventType.TOOL_USE
            and str(e.data.get("name", "")).lower() in {"agent", "task"}
            and isinstance(e.data.get("input"), dict)
        ):
            continue
        prompts.append(str(e.data["input"].get("prompt", "")))
    return prompts


def _paths_missing_from_prompts(handoff_paths: list[str], prompts: list[str]) -> list[str]:
    missing: list[str] = []
    for path in handoff_paths:
        path_dispatched = False
        for prompt in prompts:
            if path in prompt and all(
                other not in prompt for other in handoff_paths if other != path
            ):
                path_dispatched = True
                break
        if not path_dispatched:
            missing.append(path)
    return missing


def _dispatch_error_event(session_id: str, data: dict[str, Any]) -> Event:
    return Event.create(
        session_id=session_id,
        iteration=0,
        event_type=EventType.ERROR,
        data=data,
    )


def _monitor_targets(
    plugin: BreatherPlugin, session_id: str, active: set[str]
) -> _MonitorTargets | None:
    """取被监控 session 与监控参数；不可监控或无在途子代理时返回 None。"""
    session = plugin.services.find_session(session_id)
    if session is None or session.pause_requested or session_id in active:
        return None
    agents = dict(plugin.services.in_flight_subagents(session_id))
    if not agents:
        return None
    runtime = session.config.runtime or plugin.services.default_runtime()
    if runtime not in {"opencode", "claude_sdk", "claude_cli", "claude_code"}:
        return None
    env = {**os.environ, **session.config.env_overrides}
    window = int(plugin.context_window(session) or 0)
    if window <= 0:
        window = plugin.check_context_from_events(session).max_tokens or 200_000
    return _MonitorTargets(
        session=session,
        agents=agents,
        parent_id=session.native_session_id or "",
        runtime=runtime,
        env=env,
        window=window,
    )


async def _track_main_context(
    plugin: BreatherPlugin,
    session_id: str,
    session: Any,
    runtime: str,
    env: dict[str, str],
    parent_id: str,
    window: int,
    previous: tuple[int, int] | None,
) -> tuple[int, int] | None:
    """读主代理用量，有新读数时发 MAIN_CONTEXT；返回最新 (tokens, window)。"""
    main_tokens = await asyncio.to_thread(
        plugin.services.read_main_context,
        runtime,
        session.config.cwd,
        parent_id,
        env,
    )
    if main_tokens and previous != (main_tokens, window):
        await plugin.ctx.bus.emit(
            Event.create(
                session_id=session_id,
                iteration=session.current_iteration,
                event_type=EventType.MAIN_CONTEXT,
                data={
                    "current_tokens": main_tokens,
                    "max_tokens": window,
                    "percentage": round(main_tokens / window * 100, 1),
                },
            )
        )
        return (main_tokens, window)
    return previous


async def _check_subagent_usage(
    plugin: BreatherPlugin,
    session_id: str,
    session: Any,
    runtime: str,
    env: dict[str, str],
    parent_id: str,
    agents: dict[str, Any],
    window: int,
    previous: dict[str, tuple[int, int]],
    active: set[str],
) -> str | None:
    """逐个子代理读用量并上报；返回超阈值的 agent_id（无则 None）。"""
    for agent_id in agents:
        tokens = await asyncio.to_thread(
            plugin.services.read_subagent_context,
            runtime,
            session.config.cwd,
            parent_id,
            agent_id,
            env,
        )
        if not tokens or previous.get(agent_id) == (tokens, window):
            continue
        previous[agent_id] = (tokens, window)
        pct = round(tokens / window * 100, 1)
        await plugin.ctx.bus.emit(
            Event.create(
                session_id=session_id,
                iteration=session.current_iteration,
                event_type=EventType.SUBAGENT_CONTEXT,
                data={
                    "agent_id": agent_id,
                    "agent_type": agents[agent_id].get("agent_type", ""),
                    "current_tokens": tokens,
                    "max_tokens": window,
                    "percentage": pct,
                },
            )
        )
        child_threshold = plugin.subagent_threshold_pct(session)
        if child_threshold < 100 and pct > child_threshold and session_id not in active:
            return agent_id
    return None


def _try_takeover_handoff(plugin: BreatherPlugin, session: Any, sid: str) -> bool:
    """占用交接状态机；已有交接在途或用户已请求暂停时不接管。"""
    state = plugin.state_for(sid)
    if state.pending or state.finalizing or session.pause_requested:
        return False
    state.pending = True
    state.finalizing = True
    state.session_id = sid
    state.started_at = time.time()
    return True


def _handoff_root(services: Any, session: Any, sid: str) -> tuple[Path, str, int]:
    """创建本轮交接的输出目录，返回 (root, batch, handoff_number)。"""
    batch = uuid4().hex[:12]
    handoff_number = len(services.list_session_successors(sid)) + 1
    root = Path(session.config.cwd).resolve() / ".workflow-controller" / "handoffs" / sid / batch
    root.mkdir(parents=True, exist_ok=True)
    return root, batch, handoff_number


async def _main_over_threshold(
    plugin: BreatherPlugin,
    session: Any,
    runtime: str,
    parent_id: str,
    env: dict[str, str],
) -> bool:
    """主代理实时用量是否超阈值；读不到实时读数时按事件快照百分比判断。"""
    window = int(plugin.context_window(session) or 0)
    main_context = plugin.check_context_from_events(session)
    window = window or main_context.max_tokens or 200_000
    live_main = await asyncio.to_thread(
        plugin.services.read_main_context,
        runtime,
        session.config.cwd,
        parent_id,
        env,
    )
    main_pct = live_main / window * 100 if live_main else (main_context.percentage or 0.0)
    return plugin.threshold_pct(session) < 100 and main_pct > plugin.threshold_pct(session)


def _refresh_agents(services: Any, sid: str, agents: dict[str, Any]) -> dict[str, Any]:
    """合并暂停后仍在途的子代理，并用 SUBAGENT_START 事件修正 agent_type。"""
    agents.update(services.in_flight_subagents(sid))
    for event in services.session_events(sid):
        if event.event_type != EventType.SUBAGENT_START:
            continue
        agent_id = event.data.get("agent_id")
        agent_type = event.data.get("agent_type")
        if agent_id in agents and agent_type and agent_type != "general-purpose":
            agents[agent_id]["agent_type"] = agent_type
    return agents


async def _begin_group_handoff(
    plugin: BreatherPlugin,
    services: Any,
    session: Any,
    sid: str,
    initial_agents: dict[str, Any],
    runtime: str,
    env: dict[str, str],
    outcome: _HandoffOutcome,
) -> _HandoffSetup:
    """交接前半程：暂停主 session、等排空、中断全部在途子代理。"""
    agents = {**initial_agents, **services.in_flight_subagents(sid)}
    root, batch, handoff_number = _handoff_root(services, session, sid)
    main_handoff = root / HandoffGenerator.handoff_doc_name(sid[:8], handoff_number)
    done_path = root / HandoffGenerator.done_file_name(sid[:8], handoff_number)
    parent_id = session.native_session_id or ""
    main_over = await _main_over_threshold(plugin, session, runtime, parent_id, env)
    await services.pause_session(sid)
    outcome.paused = await services.wait_session_done(sid, wait_timeout=10.0)
    if not outcome.paused:
        raise RuntimeError(
            "parent session runtime did not drain within timeout; aborting group handoff"
        )
    agents = _refresh_agents(services, sid, agents)
    await services.interrupt_subagents(sid, agents)
    return _HandoffSetup(
        agents=agents,
        root=root,
        batch=batch,
        handoff_number=handoff_number,
        main_handoff=main_handoff,
        done_path=done_path,
        parent_id=parent_id,
        main_over=main_over,
    )


async def _snapshot_main(
    services: Any,
    sid: str,
    runtime: str,
    cwd: str,
    parent_id: str,
    env: dict[str, str],
    root: Path,
    main_over: bool,
) -> _MainSnapshot:
    """落盘主 session 事件尾部与（超阈值时的）主源码快照。"""
    main_events = root / "main-events.jsonl"
    with main_events.open("w", encoding="utf-8") as out:
        for event in services.session_events(sid)[-200:]:
            out.write(json.dumps(event.to_dict(), ensure_ascii=False, default=str) + "\n")
    main_source = root / "main-source.jsonl"
    found = main_over and await asyncio.to_thread(
        services.snapshot_main_source,
        runtime,
        cwd,
        parent_id,
        main_source,
        env,
    )
    return _MainSnapshot(events=main_events, source=main_source, found=found)


def _dispatch_prompt(agent_id: str, handoff_path: Path, source: Path, found: bool, cwd: str) -> str:
    """构造交接后重新派发的子代理 dispatch prompt。"""
    return (
        f"你是交接后重新派发的子代理，接替旧子代理 {agent_id} 的任务。"
        f"第一步必须用 Read 工具完整读取你自己的交接文档：{handoff_path}。"
        + (
            f"若交接文档缺失，读取源快照 {source} 并说明证据缺口。"
            if found
            else "若交接文档缺失，核对项目文件并说明证据缺口。"
        )
        + f"工作目录是 {cwd}。"
        "根据交接文档中的原任务、已完成工作和未完成步骤，先核对磁盘产物，"
        "再从上次中断的步骤继续执行。不要等待旧子代理，"
        "不要重复已经验证完成的写入，也不要把中断中的工具调用当作完成。"
        "完成后报告实际产物与验证结果。"
    )


async def _build_entries(
    services: Any,
    session: Any,
    runtime: str,
    parent_id: str,
    env: dict[str, str],
    root: Path,
    agents: dict[str, Any],
    handoff_number: int,
) -> list[dict[str, Any]]:
    """逐个子代理落盘源快照并组装 manifest entry。"""
    entries: list[dict[str, Any]] = []
    for agent_id, info in agents.items():
        safe_id = "".join(c for c in agent_id if c.isalnum() or c in "-_")
        source = root / f"source-{safe_id}.jsonl"
        found = await asyncio.to_thread(
            services.snapshot_subagent_source,
            runtime,
            session.config.cwd,
            parent_id,
            agent_id,
            source,
            env,
        )
        handoff_path = root / HandoffGenerator.handoff_doc_name(safe_id, handoff_number)
        dispatch_prompt = _dispatch_prompt(
            agent_id, handoff_path, source, found, session.config.cwd
        )
        entries.append(
            {
                "old_agent_id": agent_id,
                "agent_type": info.get("agent_type", ""),
                "source": str(source) if found else "",
                "handoff": str(handoff_path),
                "dispatch_prompt": dispatch_prompt,
            }
        )
    return entries


def _manifest_payload(
    session: Any,
    sid: str,
    trigger_id: str,
    setup: _HandoffSetup,
    entries: list[dict[str, Any]],
    main_snapshot: _MainSnapshot,
) -> dict[str, Any]:
    """manifest.json 的完整内容。"""
    return {
        "parent_session_id": sid,
        "trigger_agent_id": trigger_id,
        "handoff_number": setup.handoff_number,
        "done": str(setup.done_path),
        "birth_handoff_doc": getattr(session, "birth_handoff_doc", None) or "",
        "main_initial_prompt": getattr(session.config, "initial_prompt", ""),
        "entries": entries,
        "main_events": str(main_snapshot.events),
        "main_source": str(main_snapshot.source) if main_snapshot.found else "",
        "main_handoff": str(setup.main_handoff) if setup.main_over else "",
    }


async def _prepare_handoff_files(
    services: Any,
    session: Any,
    sid: str,
    trigger_id: str,
    runtime: str,
    env: dict[str, str],
    setup: _HandoffSetup,
    prompt_template: str,
) -> _HandoffFiles:
    """写 manifest.json 与 handoff-prompt.md，返回产物路径。"""
    main_snapshot = await _snapshot_main(
        services,
        sid,
        runtime,
        session.config.cwd,
        setup.parent_id,
        env,
        setup.root,
        setup.main_over,
    )
    entries = await _build_entries(
        services,
        session,
        runtime,
        setup.parent_id,
        env,
        setup.root,
        setup.agents,
        setup.handoff_number,
    )
    manifest = setup.root / "manifest.json"
    manifest.write_text(
        json.dumps(
            _manifest_payload(session, sid, trigger_id, setup, entries, main_snapshot),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    prompt_file = setup.root / "handoff-prompt.md"
    prompt_file.write_text(
        prompt_template.replace("{manifest_path}", str(manifest)).replace(
            "{done_path}", str(setup.done_path)
        ),
        encoding="utf-8",
    )
    return _HandoffFiles(entries=entries, manifest=manifest, prompt_file=prompt_file)


def _handoff_agent_config(session: Any, batch: str, prompt_file: Path) -> Any:
    """构造整体交接 agent 的 SessionConfig（单轮、干净上下文）。"""
    from workflow_controller.config.settings import LoopConfig, SessionConfig

    return SessionConfig(
        name=f"{session.name}-subagent-handoff-{batch}",
        role="handoff_agent",
        cwd=session.config.cwd,
        initial_prompt=f"读取 {prompt_file} 并严格执行。",
        max_turns=100,
        model=session.config.model,
        provider_index=session.config.provider_index,
        runtime=session.config.runtime,
        run_id=session.run_id,
        cli_wrapper=getattr(session.config, "cli_wrapper", ""),
        reviewer=(
            session.config.reviewer.model_copy(deep=True)
            if getattr(session.config, "reviewer", None)
            else None
        ),
        env_overrides=dict(session.config.env_overrides),
        loop=LoopConfig(max_iterations=1, evaluator="always_complete"),
    )


async def _run_handoff_agent(
    services: Any,
    plugin: BreatherPlugin,
    session: Any,
    sid: str,
    setup: _HandoffSetup,
    files: _HandoffFiles,
) -> tuple[Any, bool]:
    """起交接 agent、登记关系并等它跑完；返回 (agent, 是否自然完成)。"""
    agent_config = _handoff_agent_config(session, setup.batch, files.prompt_file)
    agent = await services.start_session(agent_config)
    try:
        await services.add_handoff_relation(
            from_session_id=sid,
            to_session_id=agent.id,
        )
    except Exception:
        logger.exception("group handoff relation could not be recorded")
    timeout = float(plugin.cfg("handoff_agent_timeout", 600))
    completed = await services.wait_session_done(
        agent.id,
        wait_timeout=timeout if timeout > 0 else None,
    )
    if not completed:
        await services.pause_session(agent.id)
        if not await services.wait_session_done(agent.id, wait_timeout=10.0):
            raise RuntimeError(
                "handoff agent runtime did not drain within timeout; aborting group handoff"
            )
    return agent, completed


def _group_ready(setup: _HandoffSetup, files: _HandoffFiles, completed: bool) -> bool:
    """交接 agent 是否产出全部产物（done 信号、每 entry 文档、主交接文档）。"""
    return (
        completed
        and setup.done_path.is_file()
        and all(Path(item["handoff"]).is_file() for item in files.entries)
        and (not setup.main_over or setup.main_handoff.is_file())
    )


def _parent_instruction(manifest: Path, ready: bool, main_over: bool) -> str:
    """构造交回主代理的派发指令（成功与降级两种话术）。"""
    return (
        f"子代理整体交接已触发。先读取 {manifest}。"
        + (
            "逐一读取每个 entry 的 handoff 文档。"
            if ready
            else "交接 agent 未完成，请从 source 快照和项目文件恢复各任务，并报告缺失证据。"
        )
        + ("先读取 main_handoff，恢复主代理的整体任务目标。" if main_over and ready else "")
        + ("先读取 main_events，核对主代理的原任务和当前进度。" if main_over and not ready else "")
        + "旧子代理均已中断，不要等待或恢复旧 ID。"
        "读取清单后，在同一条回复中并行调用 Agent/Task，为每个 entry 派发一个同类型的新子代理；"
        "必须把该 entry 的 dispatch_prompt 原文放进对应 Agent/Task 调用的 prompt 参数，"
        "不要只概括为“重新派发”。可追加具体任务细节，"
        "但不能删去先读取该子代理自己的 handoff 文档、核对磁盘、"
        "从未完成步骤继续的要求。"
    )


async def _start_successor_session(
    plugin: BreatherPlugin,
    services: Any,
    session: Any,
    sid: str,
    agent: Any,
    setup: _HandoffSetup,
    files: _HandoffFiles,
    ready: bool,
    instruction: str,
    outcome: _HandoffOutcome,
) -> str:
    """主代理超阈值：起接续 session、停旧主 session，返回新 session id。"""
    gen = plugin.effective_generator(session)
    continuation_name = gen.continuation_session_name(sid[:8], setup.handoff_number)
    successor = gen.build_new_session_config(
        session,
        setup.main_handoff if ready else files.manifest,
        continuation_name,
    )
    successor.name = continuation_name
    successor.initial_prompt = instruction
    successor.env_overrides = dict(session.config.env_overrides)
    new_session = await services.start_session(successor)
    outcome.successor_started = True
    new_session.birth_handoff_doc = str(setup.main_handoff) if ready else str(files.manifest)
    await services.persist_session(new_session)
    try:
        await services.add_handoff_relation(
            from_session_id=agent.id,
            to_session_id=new_session.id,
        )
    except Exception:
        logger.exception("group handoff relation could not be recorded")
    await services.stop_session(sid)
    outcome.parent_stopped = True
    return str(new_session.id)


def _known_subagent_ids(services: Any, sid: str) -> set[str]:
    """收集主 session 已派发过的子代理 ID。"""
    return {
        e.data.get("agent_id")
        for e in services.session_events(sid)
        if e.event_type == EventType.SUBAGENT_START and e.data.get("agent_id")
    }


async def _resume_parent_session(services: Any, sid: str, agent: Any, instruction: str) -> None:
    """主代理未超阈值：注入派发指令、恢复主 session 并在交接 agent 记录接续关系。"""
    await services.inject_prompt(sid, instruction)
    await services.resume_session(sid)
    metadata = getattr(agent, "plugin_metadata", None)
    if metadata is None:
        metadata = {}
        agent.plugin_metadata = metadata
    metadata.setdefault("breather", {})["continuation_session_id"] = sid


async def _recover_failed_handoff(services: Any, sid: str, successor_started: bool) -> None:
    """交接失败后的恢复：已起接续则停旧主 session，否则注入失败说明并恢复。"""
    if successor_started:
        try:
            await services.stop_session(sid)
        except Exception:
            logger.exception("could not stop parent after group handoff failure")
        return
    try:
        await services.inject_prompt(
            sid,
            "上次子代理整体交接失败。旧子代理已中断；检查 .workflow-controller/handoffs "
            "下最新快照及项目文件，重新派发所有未完成的子任务。",
        )
        await services.resume_session(sid)
    except Exception:
        logger.exception("could not resume parent after group handoff failure")


def _main_over_template_section(text: str, session: Any, birth_section: str) -> str:
    """main 段（主代理交接指令）替换占位符后的追加文本；无定制段返回空串。"""
    main = handoff_template_section(text, "main")
    if not main or main == HANDOFF_AGENT_PROMPT.strip():
        return ""
    main = (
        main.replace("{handoff_filename}.done", "{done_path}")
        .replace("{handoff_filename}", "清单中的 main_handoff")
        .replace("{worker_session_id}", str(getattr(session, "id", "")))
        .replace("{birth_handoff_doc_section}", birth_section)
    )
    return (
        "\n\n## 主代理交接指令\n"
        "若清单中的 main_handoff 非空，将以下要求用于主代理摘要；"
        "写入路径和完成信号以整组交接执行要求为准。\n" + main
    )


def _sectioned_group_template(text: str, session: Any, main_over: bool) -> str:
    """分段 handoff 模板的 group 模式渲染（旧式占位符替换 + 整组执行要求收尾）。"""
    instructions = handoff_template_for_mode(text, "group")
    # common/group 段同样替换旧式占位符：整组交接用 {done_path} 作完成信号、
    # 各 entry 的 handoff 路径在清单里，worker_session_id 即当前 session。
    # 否则旧式 {handoff_filename}.done 会以字面量交给交接 agent，
    # 写出与清单要求的 {done_path} 相冲突的完成信号。
    birth_section = build_birth_handoff_doc_section(
        getattr(session, "birth_handoff_doc", None) or ""
    )
    instructions = (
        instructions.replace("{handoff_filename}.done", "{done_path}")
        .replace("{handoff_filename}", "清单中对应 entry 的 handoff 路径")
        .replace("{worker_session_id}", str(getattr(session, "id", "")))
        .replace("{birth_handoff_doc_section}", birth_section)
    )
    if main_over:
        instructions += _main_over_template_section(text, session, birth_section)
    if GROUP_HANDOFF_PROMPT in instructions:
        return instructions
    return (
        instructions + "\n\n## 整组交接执行要求\n" if instructions else ""
    ) + GROUP_HANDOFF_PROMPT


async def _monitor_probe(
    plugin: BreatherPlugin,
    group: GroupHandoff,
    session_id: str,
    previous: dict[str, tuple[int, int]],
    main_carry: list[tuple[int, int] | None],
) -> bool:
    """单轮用量探测；返回是否继续监控（False = 结束，含已发起整体交接）。"""
    targets = _monitor_targets(plugin, session_id, group.active)
    if targets is None:
        return False
    session, agents, parent_id, runtime, env, window = targets
    if not parent_id:
        await asyncio.sleep(1)
        return True
    main_carry[0] = await _track_main_context(
        plugin, session_id, session, runtime, env, parent_id, window, main_carry[0]
    )
    hot_agent = await _check_subagent_usage(
        plugin,
        session_id,
        session,
        runtime,
        env,
        parent_id,
        agents,
        window,
        previous,
        group.active,
    )
    if hot_agent is not None:
        group.active.add(session_id)
        await group.trigger_handoff(session, hot_agent, agents, runtime, env)
        return False
    await asyncio.sleep(1)
    return True


async def _monitor_round(
    plugin: BreatherPlugin,
    group: GroupHandoff,
    session_id: str,
    previous: dict[str, tuple[int, int]],
    main_carry: list[tuple[int, int] | None],
) -> bool:
    """跑一轮监控并消化单轮异常；异常记日志后继续下一轮。"""
    try:
        return await _monitor_probe(plugin, group, session_id, previous, main_carry)
    except Exception:
        logger.exception("subagent context monitor iteration failed for %s", session_id)
        await asyncio.sleep(1)
        return True


class GroupHandoff:
    def __init__(self, plugin: BreatherPlugin) -> None:
        self.plugin = plugin
        self.monitors: dict[str, asyncio.Task[None]] = {}
        self.active: set[str] = set()
        self.verifiers: set[asyncio.Task[None]] = set()

    def start_monitor(self, session_id: str) -> None:
        task = self.monitors.get(session_id)
        if task is None or task.done():
            self.monitors[session_id] = asyncio.create_task(self._monitor(session_id))

    def close(self) -> None:
        for task in self.monitors.values():
            task.cancel()
        self.monitors.clear()
        for task in self.verifiers:
            task.cancel()
        self.verifiers.clear()

    async def trigger_handoff(
        self,
        session: Any,
        trigger_id: str,
        initial_agents: dict[str, Any],
        runtime: str,
        env: dict[str, str],
    ) -> None:
        """对超阈值子代理发起整体交接（_monitor 的模块级轮函数经此入口调用）。"""
        await self._handoff(session, trigger_id, initial_agents, runtime, env)

    def _prompt_template(self, session: Any, *, main_over: bool = False) -> str:
        path = getattr(session.config, "handoff_prompt_path", None) or self.plugin.cfg(
            "handoff_prompt_path", None
        )
        if path:
            template = Path(path)
            if not template.is_absolute():
                template = Path(session.config.cwd) / template
            try:
                text = template.read_text(encoding="utf-8")
                if not handoff_template_is_sectioned(text):
                    # 未分段的旧文件只指导普通主代理交接；整组交接继续走旧 group 模板。
                    logger.warning(
                        "handoff_prompt_path %s 未分段，整组交接忽略该文件并沿用旧 group 模板",
                        template,
                    )
                else:
                    return _sectioned_group_template(text, session, main_over)
            except (OSError, UnicodeDecodeError):
                logger.warning("handoff_prompt_path %s 不可读，回落内置 prompt", template)
                return GROUP_HANDOFF_PROMPT
        # Existing project configs retain their previous group template until migrated.
        path = getattr(session.config, "group_handoff_prompt_path", None) or self.plugin.cfg(
            "group_handoff_prompt_path", None
        )
        if not path:
            return GROUP_HANDOFF_PROMPT
        template = Path(path)
        if not template.is_absolute():
            template = Path(session.config.cwd) / template
        try:
            return template.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            logger.warning("group_handoff_prompt_path %s 不可读，回落内置 prompt", template)
            return GROUP_HANDOFF_PROMPT

    def _watch_dispatch(
        self, session_id: str, known_ids: set[str], handoff_paths: list[str]
    ) -> None:
        task = asyncio.create_task(self._verify_dispatch(session_id, known_ids, handoff_paths))
        self.verifiers.add(task)
        task.add_done_callback(self.verifiers.discard)

    async def _verify_dispatch(
        self, session_id: str, known_ids: set[str], handoff_paths: list[str]
    ) -> None:
        services = self.plugin.services
        expected = len(handoff_paths)
        new_ids: set[str] = set()
        missing: list[str] = []
        while not _manually_stopped(services, session_id):
            if _naturally_ended(services, session_id):
                break
            events = await asyncio.to_thread(services.session_events, session_id)
            if _manually_stopped(services, session_id):
                return
            new_ids = _new_subagent_ids(events, known_ids)
            if len(new_ids) >= expected:
                prompts = _dispatch_prompts(events)
                missing = _paths_missing_from_prompts(handoff_paths, prompts)
                if not missing:
                    return
            await asyncio.sleep(3)
        if _manually_stopped(services, session_id):
            return
        if len(new_ids) < expected:
            await self.plugin.ctx.bus.emit(
                _dispatch_error_event(
                    session_id,
                    {
                        "error": "子代理交接后重新派发数量不足",
                        "expected": expected,
                        "observed": len(new_ids),
                    },
                )
            )
            return
        await self.plugin.ctx.bus.emit(
            _dispatch_error_event(
                session_id,
                {
                    "error": "子代理交接后派发 prompt 缺少对应交接文档路径",
                    "missing_handoff_paths": missing,
                },
            )
        )

    async def _monitor(self, session_id: str) -> None:
        plugin = self.plugin
        previous: dict[str, tuple[int, int]] = {}
        # 单元素列表承载最近一次主代理读数：异常轮次也能保留已更新的值，
        # 与原循环内局部变量的存活语义一致。
        main_carry: list[tuple[int, int] | None] = [None]
        try:
            while plugin.installed:
                if not await _monitor_round(plugin, self, session_id, previous, main_carry):
                    return
        finally:
            if self.monitors.get(session_id) is asyncio.current_task():
                self.monitors.pop(session_id, None)

    async def _handoff(
        self,
        session: Any,
        trigger_id: str,
        initial_agents: dict[str, Any],
        runtime: str,
        env: dict[str, str],
    ) -> None:
        plugin = self.plugin
        services = plugin.services
        sid = session.id
        outcome = _HandoffOutcome()
        took_over = False
        try:
            took_over = _try_takeover_handoff(plugin, session, sid)
            if not took_over:
                return
            setup = await _begin_group_handoff(
                plugin, services, session, sid, initial_agents, runtime, env, outcome
            )
            files = await _prepare_handoff_files(
                services,
                session,
                sid,
                trigger_id,
                runtime,
                env,
                setup,
                self._prompt_template(session, main_over=setup.main_over),
            )
            agent, completed = await _run_handoff_agent(
                services, plugin, session, sid, setup, files
            )
            await self._finish_group_handoff(session, sid, agent, setup, files, completed, outcome)
        except Exception:
            logger.exception("subagent group handoff failed for %s", sid)
            if outcome.paused and not outcome.parent_stopped:
                await _recover_failed_handoff(services, sid, outcome.successor_started)
        finally:
            if took_over:
                plugin.reset_handoff_state(sid)
            self.active.discard(sid)

    async def _finish_group_handoff(
        self,
        session: Any,
        sid: str,
        agent: Any,
        setup: _HandoffSetup,
        files: _HandoffFiles,
        completed: bool,
        outcome: _HandoffOutcome,
    ) -> None:
        """按交接产物完备性收尾：起接续或恢复主 session，并挂派发核验。"""
        plugin = self.plugin
        services = plugin.services
        ready = _group_ready(setup, files, completed)
        instruction = _parent_instruction(files.manifest, ready, setup.main_over)
        handoff_paths = [item["handoff"] for item in files.entries]
        if setup.main_over:
            new_session_id = await _start_successor_session(
                plugin,
                services,
                session,
                sid,
                agent,
                setup,
                files,
                ready,
                instruction,
                outcome,
            )
            self._watch_dispatch(new_session_id, set(), handoff_paths)
        else:
            known_ids = _known_subagent_ids(services, sid)
            await _resume_parent_session(services, sid, agent, instruction)
            self._watch_dispatch(sid, known_ids, handoff_paths)
        if ready:
            agent.result = "success"
        if ready or not setup.main_over:
            await services.persist_session(agent)
