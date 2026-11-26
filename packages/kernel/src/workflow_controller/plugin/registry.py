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

import importlib
import importlib.metadata
import importlib.resources
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from workflow_controller.plugin.base import FormatInterpreter, Plugin, PluginConfig
from workflow_controller.plugin.context import PluginServices

if TYPE_CHECKING:
    from workflow_controller.hooks.server import HookServer
    from workflow_controller.plugin.run_driver import RunDriverFactory

logger = logging.getLogger(__name__)

_PLUGINS_DIR = Path(__file__).resolve().parent.parent / "plugins"
_EP_GROUP = "workflow_controller.plugins"


def default_user_plugin_config_dir() -> Path:
    """用户级插件覆盖配置根目录（与 auth_state.json 同根）。"""
    from platformdirs import user_config_dir

    return Path(user_config_dir("workflow-controller", appauthor=False)) / "plugins"


def _event_query_factories(orchestrator: Any) -> dict[str, Any]:
    """事件与会话元数据查询端口实现（闭包捕获 orchestrator）。"""

    def get_session_events(session_id: str) -> list[Any]:
        return list(orchestrator.event_router.get_events_sync(session_id, iteration=None))

    def get_session_metadata(session_id: str) -> dict[str, Any] | None:
        session = orchestrator.pool.get(session_id)
        if session is None:
            return None
        config = session.config.model_dump(mode="json")
        return {
            "id": session.id,
            "name": session.name,
            "role": session.role,
            "state": getattr(session.state, "value", str(session.state)),
            "result": session.result,
            "native_session_id": session.native_session_id,
            "created_at": session.created_at.isoformat(),
            "last_activity": session.last_activity.isoformat(),
            "current_iteration": session.current_iteration,
            "project_dir": config.get("cwd"),
            "runtime": config.get("runtime"),
            "model": config.get("model"),
            "agent_name": config.get("agent_name"),
            "initial_prompt": config.get("initial_prompt"),
        }

    def get_events_project_dir() -> str:
        return str(orchestrator.event_router.project_dir)

    return {
        "get_session_events": get_session_events,
        "get_session_metadata": get_session_metadata,
        "get_events_project_dir": get_events_project_dir,
    }


def _driver_factories(orchestrator: Any) -> dict[str, Any]:
    """driver 创建/解析与 reviewer 配置端口实现。"""

    def create_driver(*args: Any, **kwargs: Any) -> Any:
        from workflow_controller.driver.factory import create_driver as _create_driver

        return _create_driver(*args, **kwargs)

    def resolve_runtime(runtime: str | None) -> str:
        from workflow_controller.driver.factory import resolve_runtime as _resolve_runtime

        return _resolve_runtime(runtime)

    def find_plugin(name: str) -> Plugin | None:
        # 驱动工厂经端口解析当前活跃实例——toggle 重载换新实例后，构造期
        # 引用会留在已 uninstall 的旧实例上。按名扫描单一实现收敛在
        # PluginRegistry.find_plugin，端口侧委托过去，避免与 CoreAPI 的
        # plugin_by_name 各自维护一份实现造成语义漂移。
        return orchestrator.plugin_registry.find_plugin(name)

    def get_provider_model(provider_index: int | None) -> str | None:
        return orchestrator.auth_rotator.get_provider(provider_index).model

    def reviewer_thinking(
        project_dir: str, enabled: bool | None, budget: int | None
    ) -> tuple[bool, int]:
        from workflow_controller.config.reviewer import resolve_reviewer_thinking

        values = resolve_reviewer_thinking(
            project_dir,
            orchestrator.config.reviewer,
            budget_override=budget,
            mode_override=(None if enabled is None else "enabled" if enabled else "disabled"),
        )
        return (values.mode == "enabled", values.budget_tokens)

    return {
        "create_driver": create_driver,
        "runtime_resolver": resolve_runtime,
        "find_plugin": find_plugin,
        "get_provider_model": get_provider_model,
        "reviewer_thinking_resolver": reviewer_thinking,
    }


def _session_flow_factories(orchestrator: Any) -> dict[str, Any]:
    """跨 session 流程编排端口：委托 orchestrator 公开原语，插件不摸宿主私有成员。"""

    async def start_session(config: Any, *, compose_prompt: bool = True) -> Any:
        return await orchestrator.start_session_with_config(
            config,
            compose_prompt=compose_prompt,
        )

    async def stop_session(
        session_id: str,
        reason: str = "user_stop",
        *,
        persist_stopped_state: bool = True,
    ) -> None:
        await orchestrator.stop_session(
            session_id,
            reason=reason,
            persist_stopped_state=persist_stopped_state,
        )

    async def pause_session(session_id: str) -> None:
        await orchestrator.pause_session(session_id)

    async def resume_session(session_id: str) -> None:
        await orchestrator.resume_session(session_id)

    async def inject_prompt(session_id: str, prompt: str) -> None:
        await orchestrator.inject_prompt(session_id, prompt)

    async def wait_session_done(
        session_id: str,
        timeout: float | None = None,
        *,
        wait_timeout: float | None = None,
    ) -> bool:
        if wait_timeout is None:
            return await orchestrator.wait_session_done(session_id, timeout=timeout)
        return await orchestrator.wait_session_done(
            session_id,
            timeout=timeout,
            wait_timeout=wait_timeout,
        )

    async def persist_session(session: Any) -> None:
        await orchestrator.pool.persist(session)

    return {
        "session_starter": start_session,
        "session_stopper": stop_session,
        "session_pauser": pause_session,
        "session_resumer": resume_session,
        "prompt_injector": inject_prompt,
        "session_done_waiter": wait_session_done,
        "session_persister": persist_session,
    }


def _session_lookup_factories(orchestrator: Any) -> dict[str, Any]:
    """session/事件面端口（agent_traceback 检查点重放与重建消费）与 subagent 端口。"""

    def find_session(session_id: str) -> Any:
        return orchestrator.pool.get(session_id)

    def list_sessions() -> list[Any]:
        return orchestrator.pool.list_sessions()

    async def load_roster_session(session_id: str) -> dict[str, Any] | None:
        return await orchestrator.event_router.get_session(session_id)

    def resolve_events_db(session_id: str) -> Any:
        return orchestrator.event_router.db_path_for(session_id)

    async def store_events(events: list[Any]) -> None:
        await orchestrator.event_router.store_many(events)

    async def create_session(config: Any, **kwargs: Any) -> Any:
        return await orchestrator.pool.create(config, **kwargs)

    async def start_resumed_session(config: Any, **kwargs: Any) -> Any:
        return await orchestrator.start_session_with_resume(config, **kwargs)

    def in_flight_subagents(session_id: str) -> dict[str, Any]:
        return orchestrator.subagent_status(session_id)

    async def interrupt_subagents(
        session_id: str,
        snapshot: dict[str, dict[str, Any]],
    ) -> None:
        await orchestrator.interrupt_subagents(session_id, snapshot)

    return {
        "session_finder": find_session,
        "session_lister": list_sessions,
        "roster_session_loader": load_roster_session,
        "events_db_resolver": resolve_events_db,
        "event_storer": store_events,
        "session_creator": create_session,
        "resume_session_starter": start_resumed_session,
        "subagent_tracker": in_flight_subagents,
        "subagent_interrupter": interrupt_subagents,
    }


def _config_hook_factories(orchestrator: Any) -> dict[str, Any]:
    """hook 配置、默认 runtime 与全局配置类端口实现。"""

    def write_hook_config(project_dir: Any) -> None:
        orchestrator.generate_hook_config(project_dir)

    def get_default_runtime() -> str | None:
        return orchestrator.config.runtime

    def remove_recent_dir(path: str) -> None:
        # config_api 由组合根在插件装载后注入，调用期惰性读取；
        # 未注入（boot 中途窗口/测试）时跳过——recent_dirs 同步本就 best-effort
        ca = getattr(orchestrator, "config_api", None)
        if ca is None:
            return
        ca.remove_recent_dir(path)

    def get_starting_prompt() -> str | None:
        return orchestrator.config.starting_prompt

    return {
        "hook_config_writer": write_hook_config,
        "default_runtime_getter": get_default_runtime,
        "recent_dir_remover": remove_recent_dir,
        "starting_prompt_getter": get_starting_prompt,
    }


def _handoff_factories(orchestrator: Any) -> dict[str, Any]:
    """交接链端口：委托事件库谱系原语与宿主 CLI session 路由表。"""

    def resolve_cli_session_id(cli_session_id: str) -> str | None:
        return orchestrator.resolve_cli_session_id(cli_session_id)

    def list_session_successors(session_id: str) -> list[dict[str, Any]]:
        return orchestrator.event_router.get_successors_sync(session_id)

    def find_session_predecessor(session_id: str) -> dict[str, Any] | None:
        return orchestrator.event_router.get_predecessor_sync(session_id)

    async def add_handoff_relation(
        from_session_id: str,
        to_session_id: str,
    ) -> None:
        await orchestrator.event_router.add_handoff_relation(
            from_session_id=from_session_id,
            to_session_id=to_session_id,
        )

    async def update_loop_status(
        session_id: str,
        loop_status: str,
        stop_reason: str | None = None,
    ) -> None:
        await orchestrator.event_router.update_loop_status(
            session_id,
            loop_status,
            stop_reason=stop_reason,
        )

    return {
        "cli_session_id_resolver": resolve_cli_session_id,
        "session_successor_lister": list_session_successors,
        "session_predecessor_finder": find_session_predecessor,
        "handoff_relation_adder": add_handoff_relation,
        "loop_status_updater": update_loop_status,
    }


def _build_plugin_services(orchestrator: Any) -> PluginServices:
    """从 orchestrator 装配 plugin 能力端口。

    端口实现侧访问 ``event_router.default_store`` / ``pool`` 属于宿主内部，
    不在 plugin 源码扫描范围，故不触发属性边界门禁。plugin 侧只调端口。
    """
    services: dict[str, Any] = {}
    for factories in (
        _event_query_factories,
        _driver_factories,
        _session_flow_factories,
        _session_lookup_factories,
        _config_hook_factories,
        _handoff_factories,
    ):
        services.update(factories(orchestrator))
    services["main_context_reader"] = orchestrator.read_main_context
    services["subagent_context_reader"] = orchestrator.read_subagent_context
    services["main_source_snapshotter"] = orchestrator.snapshot_main_source
    services["subagent_source_snapshotter"] = orchestrator.snapshot_subagent_source
    return PluginServices(**services)


class PluginRegistry:
    def __init__(
        self,
        plugins_dir: Path | None = None,
        user_config_dir: Path | None = None,
    ) -> None:
        self._plugins_dir = plugins_dir or _PLUGINS_DIR
        self._user_config_dir = user_config_dir
        self._plugins: list[Plugin] = []

    @property
    def plugins(self) -> list[Plugin]:
        return list(self._plugins)

    @property
    def plugins_dir(self) -> Path:
        return self._plugins_dir

    @property
    def user_config_dir(self) -> Path:
        """用户级插件覆盖配置目录；未显式指定时取默认目录。"""
        return self._user_config_dir or default_user_plugin_config_dir()

    @user_config_dir.setter
    def user_config_dir(self, value: Path) -> None:
        self._user_config_dir = value

    @staticmethod
    def _plugin_package_dir(plugin_cls: type[Plugin]) -> Path | None:
        """插件类所在包的源码目录；定位不到（frozen/动态生成）返回 None。

        用 import_module 而非查 sys.modules：对调用方不预设「模块已随
        ep.load() 导入」的顺序，尚未导入时按需补导入（幂等）。
        """
        try:
            module = importlib.import_module(plugin_cls.__module__)
        except Exception:
            return None
        module_file = getattr(module, "__file__", None)
        if not module_file:
            return None
        return Path(module_file).resolve().parent

    @staticmethod
    def _entry_points() -> list[importlib.metadata.EntryPoint]:
        """已安装发行包登记的 workflow_controller.plugins 入口点（环境元数据坏掉当没有）。"""
        try:
            return sorted(importlib.metadata.entry_points(group=_EP_GROUP), key=lambda e: e.name)
        except Exception:
            logger.exception("entry points scan failed, skipping installed plugins")
            return []

    @staticmethod
    def _package_config_defaults(plugin_cls: type[Plugin]) -> dict[str, Any]:
        """读插件类所在包内的 config.yaml 只读默认（缺文件返回空表）。"""
        try:
            module = importlib.import_module(plugin_cls.__module__)
            package_name = module.__package__ or module.__name__
            resource = importlib.resources.files(package_name).joinpath("config.yaml")
            if resource.is_file():
                data = yaml.safe_load(resource.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
        except Exception:
            logger.warning(
                "failed to read packaged config.yaml of %s", plugin_cls.__module__, exc_info=True
            )
        return {}

    @staticmethod
    def _read_yaml_dict(path: Path) -> dict[str, Any]:
        """读 YAML 文件为 dict；缺失或坏文件返回空表。"""
        if not path.is_file():
            return {}
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            return {}
        return data if isinstance(data, dict) else {}

    def _inherit_legacy_config(
        self, name: str, user_path: Path, plugin_cls: type[Plugin]
    ) -> dict[str, Any]:
        """旧布局配置一次性迁移：把本地 plugins/<name>/config.yaml 的启停与
        自定义字段固化到用户配置文件，旧文件后续清理不丢配置。

        插件包随发行安装后，plugins/<name>/ 即插件类所在包目录，其
        config.yaml 是包内默认而非用户旧配置，不作为迁移源（否则默认值
        会被固化成用户覆盖，升级后的新默认不再生效）。
        """
        legacy_file = self._plugins_dir / name / "config.yaml"
        if not legacy_file.is_file():
            return {}
        package_dir = self._plugin_package_dir(plugin_cls)
        if package_dir is not None and package_dir == legacy_file.resolve().parent:
            return {}
        legacy_data = self._read_yaml_dict(legacy_file)
        payload = {k: v for k, v in legacy_data.items() if k not in ("name", "version")}
        if payload:
            try:
                user_path.parent.mkdir(parents=True, exist_ok=True)
                user_path.write_text(
                    yaml.dump(
                        payload, default_flow_style=False, sort_keys=False, allow_unicode=True
                    ),
                    encoding="utf-8",
                )
            except OSError:
                logger.warning("failed to persist inherited config of %s", name, exc_info=True)
        return payload

    def _entry_point_config(
        self, ep: importlib.metadata.EntryPoint, plugin_cls: type[Plugin]
    ) -> tuple[PluginConfig, Path]:
        """入口点插件配置：包内默认合并用户覆盖，返回 (配置, 用户配置文件路径)。

        版本取入口点所属 distribution；包内未携带 config.yaml 时默认停用
        （与本地目录插件缺配置一致）；用户覆盖缺失时先做旧配置迁移。
        """
        version = str(getattr(getattr(ep, "dist", None), "version", "") or "0.0.0")
        merged = dict(self._package_config_defaults(plugin_cls))
        user_path = self.user_config_dir / ep.name / "config.yaml"
        if user_path.is_file():
            override = self._read_yaml_dict(user_path)
        else:
            override = self._inherit_legacy_config(ep.name, user_path, plugin_cls)
        merged.update(override)
        extra = {k: v for k, v in merged.items() if k not in ("name", "version", "enabled")}
        return (
            PluginConfig(
                name=ep.name,
                version=version,
                enabled=bool(merged.get("enabled", False)),
                extra=extra or None,
            ),
            user_path,
        )

    @staticmethod
    def _load_entry_point_class(ep: importlib.metadata.EntryPoint) -> type[Plugin] | None:
        try:
            attr = ep.load()
        except Exception:
            logger.exception("failed to load entry point plugin %s", ep.name)
            return None
        if isinstance(attr, type) and issubclass(attr, Plugin) and attr is not Plugin:
            return attr
        logger.warning("entry point %s did not resolve to a Plugin subclass", ep.name)
        return None

    @staticmethod
    def _load_config(config_file: Path) -> PluginConfig:
        if not config_file.is_file():
            return PluginConfig(
                name=config_file.parent.name,
                version="0.0.0",
                enabled=False,
            )

        data = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
        extra = {k: v for k, v in data.items() if k not in ("name", "version", "enabled")}
        return PluginConfig(
            name=data.get("name", config_file.parent.name),
            version=str(data.get("version", "0.0.0")),
            enabled=bool(data.get("enabled", False)),
            extra=extra if extra else None,
        )

    @staticmethod
    def _load_plugin_class(plugin_file: Path, pkg_dir: Path) -> type[Plugin] | None:
        module_name = f"workflow_controller.plugins.{pkg_dir.name}.plugin"
        try:
            module = importlib.import_module(module_name)
        except Exception:
            logger.exception("failed to import plugin %s", pkg_dir.name)
            return None

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and issubclass(attr, Plugin) and attr is not Plugin:
                return attr

        return None

    @staticmethod
    def _build_plugin_context(orchestrator: Any, hook_server: HookServer | None) -> Any:
        """装配插件上下文（初始加载与运行时加载共用）。"""
        from workflow_controller.plugin.context import PluginContext

        services = _build_plugin_services(orchestrator)
        return PluginContext(
            bus=orchestrator.bus,
            type_registry=orchestrator.bus.type_registry,
            hook_server=hook_server,
            file_changes=orchestrator.file_changes,
            work_dir=Path(getattr(orchestrator.config, "project_dir", ".") or ".").expanduser(),
            services=services,
        )

    def find_plugin(self, name: str) -> Plugin | None:
        """按名返回当前已加载插件；唯一按名扫描实现，插件服务端口与
        CoreAPI ``plugin_by_name`` 端口均委托此处，避免两份相同扫描漂移。
        """
        return next(
            (p for p in self._plugins if p.config.name == name),
            None,
        )

    def format_interpreters(self) -> dict[str, FormatInterpreter]:
        """全部已装载插件的格式解释器合并表（ARCHITECTURE.md「Run 身份」）。

        同一格式被多个插件登记 = 派发不明确（配置错误），当场报而不是静默
        取一家——start_run 的去向不能取决于插件装载顺序。
        """
        merged: dict[str, FormatInterpreter] = {}
        owner: dict[str, str] = {}
        for p in self._plugins:
            for fmt, interpreter in p.get_format_interpreters().items():
                if fmt in merged:
                    raise ValueError(
                        f"格式 {fmt} 被多个插件登记（{owner[fmt]} 与 {p.config.name}），派发不明确"
                    )
                merged[fmt] = interpreter
                owner[fmt] = p.config.name
        return merged

    def run_driver_factories(self) -> dict[str, RunDriverFactory]:
        """驱动工厂独立派发面：与解释器注册面分离，同款重名报错纪律。"""
        merged: dict[str, RunDriverFactory] = {}
        owner: dict[str, str] = {}
        for p in self._plugins:
            for fmt, factory in p.get_run_driver_factories().items():
                if fmt in merged:
                    raise ValueError(
                        f"驱动格式 {fmt} 被多个插件登记"
                        f"（{owner[fmt]} 与 {p.config.name}），派发不明确"
                    )
                merged[fmt] = factory
                owner[fmt] = p.config.name
        return merged

    def get_handoff_states(self) -> dict[str, dict[str, Any]]:
        """聚合各插件经能力端口上报的 handoff 状态（SSE handoff_update 数据源）。

        单个插件查询失败只跳过该插件，不杀整段推送。
        """
        merged: dict[str, dict[str, Any]] = {}
        for p in self.plugins:
            try:
                states = p.get_handoff_states()
            except Exception:
                logger.debug(
                    "plugin %s get_handoff_states failed",
                    p.config.name,
                    exc_info=True,
                )
                continue
            if states:
                merged.update(states)
        return merged

    def discover(self) -> list[tuple[PluginConfig, Path | None, type[Plugin]]]:
        found: list[tuple[PluginConfig, Path | None, type[Plugin]]] = []
        local_names: set[str] = set()
        # 只枚举一次入口点：本地扫描的 shadow 判断与下方装载循环必须看到
        # 同一份清单，两次独立枚举在安装元数据变动间隙可能不一致。
        eps = self._entry_points()
        ep_by_name: dict[str, importlib.metadata.EntryPoint] = {}
        for ep in eps:
            ep_by_name.setdefault(ep.name, ep)
        if self._plugins_dir.is_dir():
            for sub in sorted(self._plugins_dir.iterdir()):
                if not sub.is_dir() or sub.name.startswith("_") or sub.name.startswith("."):
                    continue
                plugin_file = sub / "plugin.py"
                if not plugin_file.is_file():
                    continue
                config_file = sub / "config.yaml"
                cfg = self._load_config(config_file)
                # 插件发行包把各插件装进共享的 plugins 命名空间目录，本地扫描
                # 会撞见入口点装进来的同一实现；此时走入口点通道发现，保留
                # 包内默认与用户覆盖合并语义，仅真正的开发期本地副本才覆盖。
                shadow_ep = ep_by_name.get(cfg.name)
                if shadow_ep is not None:
                    shadow_cls = self._load_entry_point_class(shadow_ep)
                    shadow_dir = (
                        self._plugin_package_dir(shadow_cls) if shadow_cls is not None else None
                    )
                    if shadow_dir is not None and shadow_dir == sub.resolve():
                        continue
                plugin_cls = self._load_plugin_class(plugin_file, sub)
                if plugin_cls is not None:
                    found.append((cfg, config_file, plugin_cls))
                    local_names.add(cfg.name)

        ep_names: set[str] = set()
        for ep in eps:
            if ep.name in local_names:
                logger.warning(
                    "entry point plugin %s shadowed by local plugins/ directory", ep.name
                )
                continue
            if ep.name in ep_names:
                logger.warning("duplicate entry point plugin %s, first one wins", ep.name)
                continue
            plugin_cls = self._load_entry_point_class(ep)
            if plugin_cls is not None:
                cfg, user_cfg_path = self._entry_point_config(ep, plugin_cls)
                found.append((cfg, user_cfg_path, plugin_cls))
                ep_names.add(ep.name)

        return found

    def load(self, orchestrator: Any, hook_server: HookServer | None) -> None:
        discovered = self.discover()
        ctx = self._build_plugin_context(orchestrator, hook_server)
        for cfg, cfg_path, plugin_cls in discovered:
            if not cfg.enabled:
                logger.info("plugin %s is disabled, skipping", cfg.name)
                continue
            logger.info("loading plugin %s v%s", cfg.name, cfg.version)
            try:
                instance = plugin_cls(cfg, cfg_path)
                instance.install(ctx)
                self._plugins.append(instance)
            except Exception:
                logger.exception("plugin %s failed to load", cfg.name)

    def reload_plugin(self, name: str, orchestrator: Any, hook_server: HookServer | None) -> bool:
        """Enable/disable a plugin at runtime without restart."""
        for i, p in enumerate(self._plugins):
            if p.config.name == name:
                logger.info("plugin %s unloading", name)
                try:
                    p.uninstall()
                except Exception:
                    logger.exception("plugin %s failed to uninstall", name)
                self._plugins.pop(i)
                if not p.config.enabled:
                    return True
                break

        ctx = self._build_plugin_context(orchestrator, hook_server)
        discovered = self.discover()
        for cfg, cfg_path, plugin_cls in discovered:
            if cfg.name == name and cfg.enabled:
                logger.info("plugin %s loading at runtime", name)
                try:
                    instance = plugin_cls(cfg, cfg_path)
                    # Single lifecycle entry. register_hook_routes is
                    # frozen-aware (skips + warns if the aiohttp router is
                    # already frozen after the first request), so this is
                    # safe at any lifecycle stage — bus interceptors /
                    # observers only need ``ctx`` and always register,
                    # fixing the old "runtime reload drops interceptors" bug.
                    instance.install(ctx)
                    self._plugins.append(instance)
                    return True
                except Exception:
                    logger.exception("plugin %s failed to load at runtime", name)
        return False

    def unload_all(self) -> None:
        for p in reversed(self._plugins):
            try:
                p.uninstall()
            except Exception:
                logger.exception("plugin %s failed to uninstall", p.config.name)
        self._plugins.clear()
