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

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from workflow_controller.event.event_bus import EventBus
    from workflow_controller.event.type_registry import EventTypeRegistry
    from workflow_controller.hooks.server import HookServer
    from workflow_controller.plugin.base import Plugin
    from workflow_controller.plugin.file_changes import FileChangeCapture


@dataclass
class PluginServices:
    """宿主向 plugin 暴露的能力端口，plugin 不持有宿主对象。

    plugin 需要穿透到宿主内部（event_router / pool）的操作经此暴露；
    由 :class:`PluginRegistry` 从 orchestrator 装配，plugin 只调端口，
    绕过端口的宿主成员直访会触发属性边界门禁。
    """

    get_session_events: Callable[[str], list[Any]] | None = None
    get_session_metadata: Callable[[str], dict[str, Any] | None] | None = None
    get_events_project_dir: Callable[[], str] | None = None
    # 稳定能力端口：由组合根装配 driver 创建能力，插件不依赖具体 driver 工厂模块。
    create_driver: Callable[..., Any] | None = None
    # 驱动工厂按名解析当前活跃插件实例（toggle 重载换实例所迫）
    find_plugin: Callable[[str], Plugin | None] | None = None
    get_provider_model: Callable[[int | None], str | None] | None = None
    reviewer_thinking_resolver: (
        Callable[[str, bool | None, int | None], tuple[bool, int]] | None
    ) = None
    # 跨 session 流程编排端口：插件（如 breather 交接）需要操作宿主里的其他
    # session——启停、等结束、名册落盘、查在途 subagent。这些能力住在
    # orchestrator，插件不得直访其私有成员，一律经此端口委托；调用走下方
    # 同名动词方法，未装配时抛 RuntimeError
    session_starter: Callable[..., Awaitable[Any]] | None = None
    session_stopper: Callable[..., Awaitable[None]] | None = None
    session_pauser: Callable[[str], Awaitable[None]] | None = None
    session_resumer: Callable[[str], Awaitable[None]] | None = None
    prompt_injector: Callable[[str, str], Awaitable[None]] | None = None
    session_done_waiter: Callable[..., Awaitable[bool]] | None = None
    session_persister: Callable[..., Awaitable[None]] | None = None
    subagent_tracker: Callable[[str], dict[str, Any]] | None = None
    subagent_interrupter: Callable[..., Awaitable[None]] | None = None
    main_context_reader: Callable[..., int] | None = None
    subagent_context_reader: Callable[..., int] | None = None
    main_source_snapshotter: Callable[..., bool] | None = None
    subagent_source_snapshotter: Callable[..., bool] | None = None
    # session/事件面端口：查宿主 session、批量写事件——经端口逐能力委托，
    # 不直访 orchestrator
    session_finder: Callable[[str], Any] | None = None
    session_lister: Callable[[], list[Any]] | None = None
    event_storer: Callable[[list[Any]], Awaitable[None]] | None = None
    # 全局默认 runtime；未装配时 default_runtime() 回落 None
    default_runtime_getter: Callable[[], str | None] | None = None
    # 交接链端口：handoff 谱系读写、CLI session ID 反解与全局起始 prompt
    cli_session_id_resolver: Callable[[str], str | None] | None = None
    session_successor_lister: Callable[[str], list[dict[str, Any]]] | None = None
    session_predecessor_finder: Callable[[str], dict[str, Any] | None] | None = None
    handoff_relation_adder: Callable[[str, str], Awaitable[None]] | None = None
    loop_status_updater: Callable[..., Awaitable[None]] | None = None
    starting_prompt_getter: Callable[[], str | None] | None = None

    def session_events(self, session_id: str) -> list[Any]:
        if self.get_session_events is None:
            raise RuntimeError("session event service is not available")
        return self.get_session_events(session_id)

    def session_metadata(self, session_id: str) -> dict[str, Any] | None:
        if self.get_session_metadata is None:
            raise RuntimeError("session metadata service is not available")
        return self.get_session_metadata(session_id)

    def events_project_dir(self) -> str | None:
        """守护进程事件库所在目录；未装配返回 None，调用方按无注入回落。"""
        if self.get_events_project_dir is None:
            return None
        return self.get_events_project_dir()

    def plugin_by_name(self, name: str) -> Plugin | None:
        """按名取当前活跃插件实例（驱动工厂的活跃实例解析通道）。"""
        if self.find_plugin is None:
            return None
        return self.find_plugin(name)

    def provider_model(self, provider_index: int | None) -> str | None:
        return self.get_provider_model(provider_index) if self.get_provider_model else None

    def reviewer_thinking(
        self, project_dir: str, enabled: bool | None, budget: int | None
    ) -> tuple[bool, int]:
        if self.reviewer_thinking_resolver is None:
            return (
                enabled if enabled is not None else False,
                budget if budget is not None else 2048,
            )
        return self.reviewer_thinking_resolver(project_dir, enabled, budget)

    async def start_session(self, config: Any, *, compose_prompt: bool = True) -> Any:
        if self.session_starter is None:
            raise RuntimeError("session start service is not available")
        return await self.session_starter(config, compose_prompt=compose_prompt)

    async def stop_session(
        self,
        session_id: str,
        reason: str = "user_stop",
        *,
        persist_stopped_state: bool = True,
    ) -> None:
        if self.session_stopper is None:
            raise RuntimeError("session stop service is not available")
        await self.session_stopper(
            session_id,
            reason=reason,
            persist_stopped_state=persist_stopped_state,
        )

    async def pause_session(self, session_id: str) -> None:
        if self.session_pauser is None:
            raise RuntimeError("session pause service is not available")
        await self.session_pauser(session_id)

    async def resume_session(self, session_id: str) -> None:
        if self.session_resumer is None:
            raise RuntimeError("session resume service is not available")
        await self.session_resumer(session_id)

    async def inject_prompt(self, session_id: str, prompt: str) -> None:
        if self.prompt_injector is None:
            raise RuntimeError("prompt injection service is not available")
        await self.prompt_injector(session_id, prompt)

    async def wait_session_done(
        self,
        session_id: str,
        timeout: float | None = None,
        *,
        wait_timeout: float | None = None,
    ) -> bool:
        if self.session_done_waiter is None:
            raise RuntimeError("session wait service is not available")
        if wait_timeout is None:
            return await self.session_done_waiter(session_id, timeout=timeout)
        return await self.session_done_waiter(
            session_id,
            timeout=timeout,
            wait_timeout=wait_timeout,
        )

    async def persist_session(self, session: Any) -> None:
        if self.session_persister is None:
            raise RuntimeError("session persist service is not available")
        await self.session_persister(session)

    def in_flight_subagents(self, session_id: str) -> dict[str, Any]:
        if self.subagent_tracker is None:
            raise RuntimeError("subagent tracking service is not available")
        return self.subagent_tracker(session_id)

    async def interrupt_subagents(
        self,
        session_id: str,
        snapshot: dict[str, dict[str, Any]],
    ) -> None:
        if self.subagent_interrupter is None:
            raise RuntimeError("subagent interruption service is not available")
        await self.subagent_interrupter(session_id, snapshot)

    def read_main_context(
        self, runtime: str, cwd: str, session_id: str, env: dict[str, str]
    ) -> int:
        if self.main_context_reader is None:
            raise RuntimeError("main context reader is not available")
        return self.main_context_reader(runtime, cwd, session_id, env)

    def read_subagent_context(
        self,
        runtime: str,
        cwd: str,
        parent_id: str,
        agent_id: str,
        env: dict[str, str],
    ) -> int:
        if self.subagent_context_reader is None:
            raise RuntimeError("subagent context reader is not available")
        return self.subagent_context_reader(runtime, cwd, parent_id, agent_id, env)

    def snapshot_main_source(
        self,
        runtime: str,
        cwd: str,
        session_id: str,
        target: Path,
        env: dict[str, str],
    ) -> bool:
        if self.main_source_snapshotter is None:
            raise RuntimeError("main source snapshotter is not available")
        return self.main_source_snapshotter(runtime, cwd, session_id, target, env)

    def snapshot_subagent_source(
        self,
        runtime: str,
        cwd: str,
        parent_id: str,
        agent_id: str,
        target: Path,
        env: dict[str, str],
    ) -> bool:
        if self.subagent_source_snapshotter is None:
            raise RuntimeError("subagent source snapshotter is not available")
        return self.subagent_source_snapshotter(runtime, cwd, parent_id, agent_id, target, env)

    def find_session(self, session_id: str) -> Any | None:
        """按 id 取内存池中的 session 对象；未命中返回 None。"""
        if self.session_finder is None:
            raise RuntimeError("session lookup service is not available")
        return self.session_finder(session_id)

    def list_sessions(self) -> list[Any]:
        """列出内存池全部 session 对象（按 SDK session ID 反查等只读遍历）。

        未装配视作空池：全集枚举的空答案良定义（与 find_session 的未命中
        语义不同），不因端口缺位抛错。
        """
        if self.session_lister is None:
            return []
        return self.session_lister()

    async def store_events(self, events: list[Any]) -> None:
        """批量写事件到事件库（单条事件用单元素列表）。"""
        if self.event_storer is None:
            raise RuntimeError("event store service is not available")
        await self.event_storer(events)

    def default_runtime(self) -> str | None:
        """全局默认 runtime；未装配返回 None，调用方按无注入回落。"""
        if self.default_runtime_getter is None:
            return None
        return self.default_runtime_getter()

    def resolve_cli_session_id(self, cli_session_id: str) -> str | None:
        """把 CLI/SDK session ID 反解为宿主 session ID；未命中返回 None。"""
        if self.cli_session_id_resolver is None:
            raise RuntimeError("cli session id service is not available")
        return self.cli_session_id_resolver(cli_session_id)

    def list_session_successors(self, session_id: str) -> list[dict[str, Any]]:
        """列 session 的交接后继关系（交接轮次计数用）。"""
        if self.session_successor_lister is None:
            raise RuntimeError("session successor service is not available")
        return self.session_successor_lister(session_id)

    def find_session_predecessor(self, session_id: str) -> dict[str, Any] | None:
        """查 session 的交接前驱关系；链头返回 None。"""
        if self.session_predecessor_finder is None:
            raise RuntimeError("session predecessor service is not available")
        return self.session_predecessor_finder(session_id)

    async def add_handoff_relation(
        self,
        from_session_id: str,
        to_session_id: str,
    ) -> None:
        """记录一条交接关系（前端进度与谱系回溯用）。"""
        if self.handoff_relation_adder is None:
            raise RuntimeError("handoff relation service is not available")
        await self.handoff_relation_adder(from_session_id, to_session_id)

    async def update_loop_status(
        self,
        session_id: str,
        loop_status: str,
        stop_reason: str | None = None,
    ) -> None:
        """更新持久化的 loop 状态记录（交接完成恢复可复用 / 超时落终态）。"""
        if self.loop_status_updater is None:
            raise RuntimeError("loop status service is not available")
        await self.loop_status_updater(session_id, loop_status, stop_reason=stop_reason)

    def starting_prompt(self) -> str | None:
        """全局起始 prompt；未装配返回 None，调用方按空串回落。"""
        if self.starting_prompt_getter is None:
            return None
        return self.starting_prompt_getter()


@dataclass
class PluginContext:
    bus: EventBus
    type_registry: EventTypeRegistry
    hook_server: HookServer | None
    file_changes: FileChangeCapture | None = None
    # 启动期 Workflow Controller 的工作目录（供 plugin 不穿越 orchestrator.config 用，二审 #11）
    # 由 PluginRegistry.load 注入，默认 = Path.cwd()
    work_dir: Path = field(default_factory=Path.cwd)
    services: PluginServices = field(default_factory=PluginServices)
