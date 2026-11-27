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

"""Core API 聚合入口 — Web UI 和 Headless 共用。

所有子 API 返回纯数据结构，不依赖任何 UI 框架。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from workflow_controller.api.auth_api import AuthAPI
from workflow_controller.api.config_api import ConfigAPI
from workflow_controller.api.event_api import EventAPI
from workflow_controller.api.pipeline_api import PipelineAPI
from workflow_controller.api.plugin_api import PluginAPI
from workflow_controller.api.run_api import RunAPI
from workflow_controller.api.session_api import SessionAPI
from workflow_controller.api.usage_api import UsageAPI
from workflow_controller.api.watchdog_api import WatchdogAPI
from workflow_controller.core.run_coordinator import RunCoordinator
from workflow_controller.hooks.server import HookServerStartupError
from workflow_controller.store.run_events_cursor import RunEventsCursor

if TYPE_CHECKING:
    from collections.abc import Callable

    from workflow_controller.config.settings import AppConfig
    from workflow_controller.core.orchestrator import Orchestrator
    from workflow_controller.plugin.base import Plugin


class BootError(RuntimeError):
    """共享启动流程未能完成。"""


@dataclass
class CoreAPI:
    """Web 和 Headless 共用的统一 API 入口。"""

    session: SessionAPI
    event: EventAPI
    config: ConfigAPI
    pipeline: PipelineAPI
    run: RunAPI
    usage: UsageAPI
    auth: AuthAPI
    plugin: PluginAPI
    watchdog: WatchdogAPI
    plugin_by_name: Callable[[str], Plugin | None]


def create_core_api(
    orchestrator: Orchestrator,
    config_path: Path,
    run_coordinator: RunCoordinator,
) -> CoreAPI:
    """从 Orchestrator 创建 CoreAPI；run 平面由调用方（组合根）装配传入。"""
    session = SessionAPI(orchestrator, config_path)
    event = EventAPI(orchestrator.event_router)
    config = ConfigAPI(orchestrator, config_path)
    pipeline = PipelineAPI(orchestrator)
    run = RunAPI(
        run_coordinator,
        RunEventsCursor(orchestrator.event_router.default_store),
    )
    usage = UsageAPI(orchestrator.event_router)
    auth = AuthAPI(orchestrator)
    plugin = PluginAPI(orchestrator)
    watchdog = WatchdogAPI(orchestrator.event_router)

    def plugin_by_name(name: str) -> Plugin | None:
        # 按名扫描单一实现收敛在 PluginRegistry.find_plugin（与插件服务端口
        # 共用），避免两份相同扫描逻辑在 reload 换实例、别名、大小写归一等
        # 语义变化时只改一处造成漂移。
        return orchestrator.plugin_registry.find_plugin(name)

    return CoreAPI(
        session=session,
        event=event,
        config=config,
        pipeline=pipeline,
        run=run,
        usage=usage,
        auth=auth,
        plugin=plugin,
        watchdog=watchdog,
        plugin_by_name=plugin_by_name,
    )


async def boot(
    config: AppConfig,
    config_path: Path,
    settings_dir: Path | None = None,
    *,
    hook_port: int | None = None,
) -> tuple[Orchestrator, CoreAPI]:
    """统一入口 — Web / Headless 共用。

    创建 Orchestrator, 启动 hooked services, 返回 (orch, api)。
    """
    from workflow_controller.core.auth_rotator import AuthRotator, default_config_dir
    from workflow_controller.core.orchestrator import Orchestrator as _Orchestrator
    from workflow_controller.driver.factory import set_config_runtime

    set_config_runtime(config.runtime)

    # 存储目录解耦：sessions.json / 默认 events.db 绑定到启动期 cwd，不再随
    # 可变的 config.project_dir 漂移。切项目后 yaml 指针变了，但运行期存储位置
    # 不动，重启也从同一固定位置加载，根治「重启后 session 闪现/丢失」。
    # 测试与 demo 可显式传 default_project_dir 覆盖，不受影响。
    orch = _Orchestrator(
        config,
        default_project_dir=Path.cwd(),
        hook_server_port_override=hook_port,
    )
    orch.config_path = config_path  # type: ignore[attr-defined]
    cfg_dir = settings_dir or default_config_dir()
    orch.cfg_dir = cfg_dir  # type: ignore[attr-defined]  # recent_dirs.json 等主机本地状态存此（与 auth_state.json 同目录）
    # 插件用户覆盖配置与主机本地状态同根；kernel 侧 registry 不 import core，
    # 由组合根在此重定向（settings-dir 覆盖随之生效）
    orch.plugin_registry.user_config_dir = cfg_dir / "plugins"
    orch.auth_rotator = AuthRotator(
        state_path=cfg_dir / "auth_state.json",
        env_path=cfg_dir / ".env",
        runtime=config.runtime,
    )
    try:
        await orch.start()
    except HookServerStartupError as exc:
        raise BootError(str(exc)) from exc
    # boot 恢复序（组合根单处排序）：归属恢复（缓存重建+补铸，须先于一切
    # session 活动；名册空不触账面）→ run 平面装配 → run 恢复
    await orch.affiliation.recover_at_boot(orch.pool)
    # 驱动工厂聚合一次冻结注入：工厂集在插件装载后不可变，重名注册在此 raise
    coordinator = RunCoordinator(
        orchestrator=orch,
        run_store=orch.run_store,
        bus=orch.bus,
        driver_factories=orch.plugin_registry.run_driver_factories(),
        affiliation=orch.affiliation,
    )
    await coordinator.recover_at_boot()
    # run 平面关停接入 orchestrator 关停序（session 运行时与池停止之前）
    orch.register_shutdown_step(coordinator.shutdown)
    api = create_core_api(orch, config_path, coordinator)
    # 把 ConfigAPI 注入 orchestrator，供插件按需取用（如删除重建 session 时
    # 同步 recent_dirs.json）——与 cfg_dir / auth_rotator 同款注入方式。
    orch.config_api = api.config  # type: ignore[attr-defined]
    return orch, api
