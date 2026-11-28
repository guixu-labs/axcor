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

"""路由挂载测试：插件 router 的启动期挂载、运行期注册回调与宿主约定。"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from workflow_controller.api.http.mounting import (
    register_kernel_routes,
    register_plugin_routers,
)
from workflow_controller.plugin.web_router import plugin_router


def _plugin_router(name: str) -> Any:
    """按插件 SDK 约定构造的 router：处理器经 app.state.api.plugin_by_name 找回插件。"""
    holder = plugin_router(name, prefix=f"/api/{name}")

    @holder.router.get("/state")
    async def state(request: Request) -> dict[str, str]:
        return {"plugin": holder.get_plugin(request).config.name}

    return holder.router


def _core_api(routers: dict[str, Any], *, failing: set[str] = frozenset()) -> SimpleNamespace:
    """CoreAPI 桩：list_plugins 给发现面名称，get_web_router 给 router（None 表示无 router）。"""
    discovered = [*routers, *sorted(failing - routers.keys())]

    def get_web_router(name: str) -> Any:
        if name in failing:
            raise RuntimeError(f"router of {name} failed")
        return routers.get(name)

    return SimpleNamespace(
        session=SimpleNamespace(health_snapshot=lambda: {}),
        plugin=SimpleNamespace(
            list_plugins=lambda: [{"name": name} for name in discovered],
            get_web_router=get_web_router,
        ),
        plugin_by_name=lambda name: SimpleNamespace(config=SimpleNamespace(name=name)),
    )


def test_plugin_routers_mounted_at_startup_without_touching_orchestrator() -> None:
    """已装载且带 router 的插件在启动期挂载并可请求；无 router 的插件不登记。"""
    app = FastAPI()
    api = _core_api({"alpha": _plugin_router("alpha"), "beta": None})

    register_plugin_routers(app, api)
    app.state.api = api

    assert app.state.plugin_routers == {"alpha"}
    response = TestClient(app).get("/api/alpha/state")
    assert response.status_code == 200
    assert response.json() == {"plugin": "alpha"}
    assert TestClient(app).get("/api/beta/state").status_code == 404


def test_failing_plugin_router_is_isolated_from_siblings(caplog) -> None:
    """单个插件的 router 取不到或挂不上只记日志，不影响其余插件。"""
    app = FastAPI()
    api = _core_api({"alpha": _plugin_router("alpha")}, failing={"broken"})

    with caplog.at_level(logging.ERROR, logger="workflow_controller.api.http.mounting"):
        register_plugin_routers(app, api)

    assert app.state.plugin_routers == {"alpha"}
    assert "broken" in caplog.text


def test_runtime_plugin_router_appends_and_dedupes() -> None:
    """运行期回调：无 catch-all 的应用直接追加，同名插件不重复注册。"""
    app = FastAPI()
    api = _core_api({})
    register_kernel_routes(app)
    register_plugin_routers(app, api)
    app.state.api = api
    register = app.state.register_runtime_plugin_router

    register("demo", _plugin_router("demo"))
    register("demo", _plugin_router("demo-again"))

    assert app.state.plugin_routers == {"demo"}
    client = TestClient(app)
    assert client.get("/api/demo/state").status_code == 200
    assert client.get("/api/demo-again/state").status_code == 404
    assert client.get("/api/v1/health").status_code == 200  # 运行期追加不影响既有内核路由
