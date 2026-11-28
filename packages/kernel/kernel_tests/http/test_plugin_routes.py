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

"""plugins 路由：模板上传只写入启动目录，内置指令经 PluginAPI 取内容。"""

from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from workflow_controller.api.http.routes.plugins_routes import router
from workflow_controller.api.plugin_api import PluginAPI

_BUILTIN_INSTRUCTIONS = "/api/v1/plugins/breather/builtin-instructions"


class _InstructionPlugin:
    """声明内置指令文件的插件类型桩（不依赖具体插件）。"""

    @staticmethod
    def builtin_instruction_files() -> dict[str, tuple[str, str]]:
        return {"demo": ("workflow-demo-instructions.md", "# demo instructions\n")}


def _plugins_app() -> FastAPI:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return app


def _template_client(tmp_path, monkeypatch) -> TestClient:
    """chdir 到临时目录并挂载 plugins_routes 的测试客户端。"""
    monkeypatch.chdir(tmp_path)
    return TestClient(_plugins_app())


def test_upload_template_to_startup_directory(tmp_path, monkeypatch) -> None:
    """上传校验：纯文件名、.md/.txt、UTF-8；允许覆盖同名文件；返回绝对路径。"""
    client = _template_client(tmp_path, monkeypatch)

    response = client.post(
        "/api/v1/plugins/breather/template?filename=handoff.md",
        content="交接说明".encode(),
    )

    assert response.status_code == 200
    assert response.json() == {"path": str(tmp_path / "handoff.md")}
    assert (tmp_path / "handoff.md").read_text() == "交接说明"
    assert (
        client.post(
            "/api/v1/plugins/breather/template?filename=handoff.md", content=b"new"
        ).status_code
        == 200
    )
    assert (tmp_path / "handoff.md").read_text() == "new"
    assert (
        client.post(
            "/api/v1/plugins/breather/template?filename=..%2Fescape.md", content=b"x"
        ).status_code
        == 400
    )
    assert (
        client.post("/api/v1/plugins/breather/template?filename=bad.pdf", content=b"x").status_code
        == 400
    )
    assert (
        client.post(
            "/api/v1/plugins/breather/template?filename=bad.txt", content=b"\xff"
        ).status_code
        == 400
    )
    assert not (tmp_path / "escape.md").exists()


def test_upload_template_accepts_10_mib_and_rejects_larger_file(tmp_path, monkeypatch) -> None:
    client = _template_client(tmp_path, monkeypatch)

    limit = 10 * 1024 * 1024
    accepted = client.post(
        "/api/v1/plugins/breather/template?filename=limit.md", content=b"a" * limit
    )
    assert accepted.status_code == 200
    assert (tmp_path / "limit.md").stat().st_size == limit

    rejected = client.post(
        "/api/v1/plugins/breather/template?filename=too-large.txt", content=b"a" * (limit + 1)
    )
    assert rejected.status_code == 400
    assert "10 MiB" in rejected.json()["detail"]
    assert not (tmp_path / "too-large.txt").exists()


def test_builtin_instructions_download_and_unknown_kind(tmp_path, monkeypatch) -> None:
    """下载头是附件、内容取自插件类型声明；未知指令类型 404。"""
    app = _plugins_app()
    app.state.api = SimpleNamespace(
        plugin=PluginAPI(
            SimpleNamespace(
                discover_plugins=lambda: [
                    (SimpleNamespace(name="breather", enabled=False), None, _InstructionPlugin)
                ],
            )
        )
    )
    client = TestClient(app)

    demo = client.get(f"{_BUILTIN_INSTRUCTIONS}/demo")

    assert demo.status_code == 200
    assert (
        demo.headers["content-disposition"]
        == 'attachment; filename="workflow-demo-instructions.md"'
    )
    assert demo.headers["content-type"].startswith("text/markdown")
    assert demo.text == "# demo instructions\n"
    assert client.get(f"{_BUILTIN_INSTRUCTIONS}/unknown").status_code == 404
