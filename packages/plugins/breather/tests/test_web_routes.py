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

"""breather web 路由约定：插件 router 经宿主挂载后三条端点的应答，与内置指令的下载内容。"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from workflow_controller.api.http.mounting import register_plugin_routers
from workflow_controller.api.http.routes.plugins_routes import router as plugin_routes
from workflow_controller.api.plugin_api import PluginAPI
from workflow_controller.plugins.breather.group_handoff import GROUP_HANDOFF_PROMPT, GroupHandoff
from workflow_controller.plugins.breather.handoff import (
    HANDOFF_AGENT_PROMPT,
    RESUME_PROMPT,
    HandoffGenerator,
)
from workflow_controller.plugins.breather.plugin import BreatherPlugin

from ._helpers import breather_plugin

if TYPE_CHECKING:
    from pathlib import Path

# 插件空闲时的 /api/breather/state 应答，逐键锁定
IDLE_STATE = {
    "pending": False,
    "finalizing": False,
    "stop_armed": False,
    "force_pending": False,
    "pending_session_id": "",
    "active_subagents": [],
}

_BUILTIN_INSTRUCTIONS = "/api/v1/plugins/breather/builtin-instructions"


def _idle_plugin() -> BreatherPlugin:
    """按生产装配路径构造已安装的 breather 插件（orchestrator 为没有运行中成员的 mock）。"""
    orch = MagicMock()
    getattr(orch, "_subagents").in_flight.return_value = {}
    orch.pool.get.return_value = None
    orch.resolve_cli_session_id.return_value = None
    orch.pool.list_sessions.return_value = []
    return breather_plugin(orch)


def _app_with_plugin_router(plugin: BreatherPlugin) -> FastAPI:
    """按宿主启动期挂载方式装上插件 router（CoreAPI.plugin 用桩）。"""
    api = SimpleNamespace(
        plugin=SimpleNamespace(
            list_plugins=lambda: [{"name": "breather"}],
            get_web_router=lambda name: plugin.get_web_router(),
        )
    )
    app = FastAPI()
    register_plugin_routers(app, api)
    app.state.api = api
    return app


def _breather_client() -> tuple[TestClient, BreatherPlugin]:
    plugin = _idle_plugin()
    return TestClient(_app_with_plugin_router(plugin)), plugin


def _arm_force_pending(plugin: BreatherPlugin, session_id: str) -> None:
    """构造一个待决强制交接的 session 状态。"""
    getattr(plugin, "_select_state")(session_id)
    setattr(plugin, "_handoff_force_pending", True)


def test_state_idle_shape() -> None:
    client, _ = _breather_client()

    resp = client.get("/api/breather/state")

    assert resp.status_code == 200
    assert resp.json() == IDLE_STATE


def test_state_pending_reports_scoped_session() -> None:
    client, plugin = _breather_client()
    _arm_force_pending(plugin, "s1")

    resp = client.get("/api/breather/state", params={"session_id": "s1"})

    assert resp.status_code == 200
    assert resp.json() == {**IDLE_STATE, "force_pending": True, "pending_session_id": "s1"}


def test_cancel_without_session_id_rejected() -> None:
    client, _ = _breather_client()

    resp = client.post("/api/breather/cancel", json={})

    assert resp.status_code == 400
    assert resp.json() == {"detail": "session_id is required"}


def test_cancel_resets_pending_state() -> None:
    client, plugin = _breather_client()
    _arm_force_pending(plugin, "s1")

    resp = client.post("/api/breather/cancel", json={"session_id": "s1"})

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    assert "s1" not in getattr(plugin, "_handoff_states")


def test_handoff_without_active_session_reports_reason() -> None:
    client, _ = _breather_client()

    resp = client.post("/api/breather/handoff", json={"session_id": "s1", "force": False})

    assert resp.status_code == 200
    assert resp.json() == {"handoff_triggered": False, "reason": "no active session"}


def test_builtin_instructions_download_content_and_reupload_without_duplicate_group_rules(
    tmp_path: Path, monkeypatch
) -> None:
    """内置指令下载头、分段标记与 prompt 全文；下载件再上传不会重复注入整组交接规则。"""
    monkeypatch.chdir(tmp_path)
    app = FastAPI()
    app.include_router(plugin_routes, prefix="/api/v1")
    app.state.api = SimpleNamespace(
        plugin=PluginAPI(
            SimpleNamespace(
                discover_plugins=lambda: [
                    (SimpleNamespace(name="breather", enabled=False), None, BreatherPlugin)
                ],
            )
        )
    )
    client = TestClient(app)

    handoff = client.get(f"{_BUILTIN_INSTRUCTIONS}/handoff")
    continuation = client.get(f"{_BUILTIN_INSTRUCTIONS}/continuation")

    assert handoff.status_code == continuation.status_code == 200
    assert (
        handoff.headers["content-disposition"]
        == 'attachment; filename="workflow-handoff-instructions.md"'
    )
    assert (
        continuation.headers["content-disposition"]
        == 'attachment; filename="workflow-continuation-instructions.md"'
    )
    # 分段标记、编辑说明与三段内置 prompt 全文
    assert "<!-- workflow:main -->" in handoff.text
    assert "<!-- workflow:group -->" in handoff.text
    assert "主代理如何交接自己的工作" in handoff.text
    assert "每名子代理如何交接自己的工作" in handoff.text
    assert "交接文档名称由系统按会话 ID 和轮次生成" in handoff.text
    assert HANDOFF_AGENT_PROMPT.strip() in handoff.text
    assert GROUP_HANDOFF_PROMPT in handoff.text
    assert continuation.text == RESUME_PROMPT
    assert client.get(f"{_BUILTIN_INSTRUCTIONS}/unknown").status_code == 404

    uploaded = client.post(
        "/api/v1/plugins/breather/template?filename=workflow-handoff-instructions.md",
        content=handoff.content,
    )
    assert uploaded.status_code == 200
    path = uploaded.json()["path"]
    main = getattr(
        HandoffGenerator(skill="handoff", handoff_filename="handoff.md", handoff_prompt_path=path),
        "_load_template",
    )(str(tmp_path))
    session = SimpleNamespace(
        config=SimpleNamespace(
            cwd=str(tmp_path), handoff_prompt_path=path, group_handoff_prompt_path=None
        )
    )
    plugin = SimpleNamespace(_cfg=lambda key, default=None: default)
    group = getattr(GroupHandoff(plugin), "_prompt_template")(session)
    assert HANDOFF_AGENT_PROMPT.strip() in main
    assert GROUP_HANDOFF_PROMPT not in main
    assert group.count(GROUP_HANDOFF_PROMPT) == 1
    assert (
        getattr(GroupHandoff(plugin), "_prompt_template")(session, main_over=True).count(
            GROUP_HANDOFF_PROMPT
        )
        == 1
    )
