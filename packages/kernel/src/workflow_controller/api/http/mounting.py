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

"""HTTP 路由挂载：内核路由域与插件路由的注册，内核独立应用与控制台应用共用。"""

from __future__ import annotations

import functools
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI

    from workflow_controller.api.core_api import CoreAPI

log = logging.getLogger(__name__)

# ── 内核路由域与 Swagger 分组（单一来源）────────────────────────────
# URL 前缀、Swagger tag 与中文描述集中在此表；/docs 与 /openapi.json 的
# 展示顺序、KERNEL_OPENAPI_TAGS 与挂载顺序均由它派生。路由模块只声明局部
# 路径与 handler，不声明前缀与 tags；新增路由域只改这里一行并在下方登记模块：
# 漏登或拼错会在装配期立即报错，而不是静默漂移。
# (模块名, 前缀, tag, 中文描述) —— 模块名为 workflow_controller.api.http.routes.<名>
KERNEL_DOMAIN_ROUTES: tuple[tuple[str, str, str, str], ...] = (
    ("sessions_routes", "/api/v1", "sessions", "Session 启动、控制、导入与提示注入"),
    ("runs_routes", "/api/v1/runs", "runs", "Run 启动、组控、成员操作、事件与导出"),
    ("overview_routes", "/api/v1", "overview", "健康检查、流水线进度、用量与事件轮询"),
    ("config_routes", "/api/v1", "config", "全局与项目配置、最近项目与目录浏览"),
    ("auth_routes", "/api/v1", "auth", "Provider 池、密钥状态与全局认证设置"),
    ("plugins_routes", "/api/v1", "plugins", "插件清单、启停、安装与配置"),
    ("sse_routes", "/api/v1", "sse", "实时事件流（Server-Sent Events）"),
    ("watchdog_routes", "/api/v1", "watchdog", "告警查询"),
)

# Swagger 分组（/docs 与 /openapi.json）：顺序与描述由 KERNEL_DOMAIN_ROUTES 派生，
# 不另行维护第二份，新增/改名 tag 只改上面一张表。
KERNEL_OPENAPI_TAGS: list[dict[str, str]] = [
    {"name": tag, "description": description} for _, _, tag, description in KERNEL_DOMAIN_ROUTES
]

# 模块名 → (前缀, tag)，装配期查表；重复登记在导入期即报错。
_MOUNT_BY_MODULE: dict[str, tuple[str, str]] = {}
for _name, _prefix, _tag, _description in KERNEL_DOMAIN_ROUTES:
    if _name in _MOUNT_BY_MODULE:
        raise ValueError(f"KERNEL_DOMAIN_ROUTES 重复登记模块：{_name}")
    _MOUNT_BY_MODULE[_name] = (_prefix, _tag)


def register_kernel_routes(app: FastAPI) -> None:
    """把内核路由域按 KERNEL_DOMAIN_ROUTES 登记的前缀与分组挂到应用上。

    路由模块只声明局部路径与 handler；改版本号、换分组只改路由域表。
    """
    from workflow_controller.api.http.routes import (
        auth_routes,
        config_routes,
        overview_routes,
        plugins_routes,
        runs_routes,
        sessions_routes,
        sse_routes,
        watchdog_routes,
    )

    route_modules = {
        "sessions_routes": sessions_routes,
        "runs_routes": runs_routes,
        "overview_routes": overview_routes,
        "config_routes": config_routes,
        "auth_routes": auth_routes,
        "plugins_routes": plugins_routes,
        "sse_routes": sse_routes,
        "watchdog_routes": watchdog_routes,
    }
    unregistered = set(_MOUNT_BY_MODULE) - set(route_modules)
    if unregistered:
        raise ValueError(f"KERNEL_DOMAIN_ROUTES 未登记装配的领域模块：{sorted(unregistered)}")
    for name, module in route_modules.items():
        prefix, tag = _MOUNT_BY_MODULE[name]
        app.include_router(module.router, prefix=prefix, tags=[tag])


def register_plugin_routers(app: FastAPI, api: CoreAPI) -> None:
    """挂载已装载插件的 web router，并安装运行期注册回调。

    须在任何 catch-all 路由挂载之前调用。插件的枚举只经 ``CoreAPI.plugin``：
    入口层不触碰 orchestrator，也不把它放上 ``app.state``。
    """
    app.state.plugin_routers: set[str] = set()
    app.state.register_runtime_plugin_router = functools.partial(
        register_runtime_plugin_router, app
    )
    for entry in api.plugin.list_plugins():
        name = entry["name"]
        try:
            router = api.plugin.get_web_router(name)
            if router is not None:
                app.include_router(router)
                app.state.plugin_routers.add(name)
        except Exception:
            log.exception("Failed to register web router for plugin '%s'", name)


def register_runtime_plugin_router(app: FastAPI, name: str, router: Any) -> None:
    """运行期注册插件 router：同名只注册一次。

    内核独立应用没有 catch-all，``include_router`` 追加到路由表末尾即可；
    有 catch-all 的宿主（控制台）自行替换 ``app.state`` 上的回调。
    """
    if name in app.state.plugin_routers:
        return
    app.include_router(router)
    app.state.plugin_routers.add(name)
