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

"""``POST /api/sessions/import-db`` 与 ``GET /import-db/log`` web 路由测试。

用 FastAPI + httpx AsyncClient 跑路由——校验：plugin 未加载时返错；
缺参数返错；正常路径同步调 plugin.import_session 并带 log_path；
log 端点按 target_cwd 定位日志，非法/缺失返 404。

路由用 ``/import-db`` 而非 ``/import``——后者已被
``sessions_routes.import_session``（opencode session 导入）占用。

注意：路由对 ``target_cwd`` 做 ``resolve()`` 后才传入 plugin（macOS 上
``/tmp`` → ``/private/tmp``），期望值须动态 resolve。
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from workflow_controller.plugins.agent_traceback.web_api import build_router


class _StubOrch:
    """最小 stub：plugins 列表持有可替换的 plugin，供按名解析回调扫描。"""

    def __init__(self, config: Any = None, cfg_dir: Path | None = None) -> None:
        self.plugins: list[Any] = []
        self.config = config
        self.cfg_dir = cfg_dir


def _plugin_finder(orch: _StubOrch) -> Any:
    """按名解析当前活跃插件的回调（对齐插件能力端口的解析语义）。"""
    return lambda name: next(
        (p for p in orch.plugins if p.config.name == name),
        None,
    )


class _StubPlugin:
    """Stub plugin：记录 import_session 调用，返回可配置结果。"""

    config_name = "agent_traceback"

    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.traceback_calls: list[dict[str, Any]] = []
        self._result = result or {
            "ok": True,
            "session_id": "stub_session_001",
            "checkpoints": [],
        }

    @property
    def config(self) -> Any:
        class _C:
            name = "agent_traceback"

        return _C()

    async def import_session(
        self,
        db_path: str,
        target_cwd: str,
        *,
        log_path: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "db_path": db_path,
                "target_cwd": target_cwd,
                "log_path": log_path,
            }
        )
        return dict(self._result)

    async def run_traceback(
        self,
        session_id: str,
        checkpoint_id: str,
        *,
        runtime: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.traceback_calls.append(
            {
                "session_id": session_id,
                "checkpoint_id": checkpoint_id,
                "runtime": runtime,
                "overrides": overrides,
            }
        )
        return dict(self._result)

    async def replay_runtime(self, session_id: str) -> str:
        return "claude_cli"


@pytest.fixture
def stub_orch_with_plugin() -> tuple[Any, _StubPlugin]:
    orch = _StubOrch()
    plugin = _StubPlugin()
    orch.plugins = [plugin]
    return orch, plugin


@pytest.fixture
def stub_orch_no_plugin() -> Any:
    return _StubOrch()


def _orch_with_persisted_config(tmp_path) -> tuple[Any, _StubPlugin, Any]:
    """带真实 ConfigAPI 的 stub orchestrator（recent_dirs 落盘到 tmp_path）。"""
    from workflow_controller.api.config_api import ConfigAPI
    from workflow_controller.config.settings import AppConfig

    orch = _StubOrch(config=AppConfig(project_dir=str(tmp_path)), cfg_dir=tmp_path)
    plugin = _StubPlugin()
    orch.plugins = [plugin]
    api_obj = SimpleNamespace(config=ConfigAPI(orch, tmp_path / "saved.yaml"))
    return orch, plugin, api_obj


class TestImportSessionRoute:
    @pytest.mark.asyncio
    async def test_no_plugin_loaded(self, stub_orch_no_plugin):
        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        app = FastAPI()
        app.include_router(build_router(_plugin_finder(stub_orch_no_plugin)))
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post(
                "/api/sessions/import-db",
                json={
                    "db_path": "/tmp/x.db",
                    "target_cwd": "/tmp/x",
                },
            )
        data = r.json()
        assert r.status_code == 200
        assert data["ok"] is False
        assert "not loaded" in data["error"]
        assert data["log_path"] == ""

    @pytest.mark.asyncio
    async def test_missing_params(self, stub_orch_with_plugin):
        orch, _plugin = stub_orch_with_plugin
        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        app = FastAPI()
        app.include_router(build_router(_plugin_finder(orch)))
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/api/sessions/import-db", json={"db_path": "/tmp/x.db"})
        data = r.json()
        assert r.status_code == 200
        assert data["ok"] is False
        assert "required" in data["error"]
        assert data["log_path"] == ""

    @pytest.mark.asyncio
    async def test_sync_import_passes_log_path(self, stub_orch_with_plugin):
        """import-db 同步调 plugin.import_session，传入构造的 log_path 并回填响应。"""
        orch, plugin = stub_orch_with_plugin
        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        app = FastAPI()
        app.include_router(build_router(_plugin_finder(orch)))
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post(
                "/api/sessions/import-db",
                json={
                    "db_path": "/tmp/cannbot.db",
                    "target_cwd": "/tmp/target",
                },
            )
        data = r.json()
        assert r.status_code == 200
        assert data["ok"] is True
        assert data["session_id"] == "stub_session_001"
        assert data["log_path"].endswith("/.workflow-controller/reconstruct/reconstruction.log")
        # 路由会 resolve target_cwd，期望值须同步 resolve。
        assert plugin.calls == [
            {
                "db_path": "/tmp/cannbot.db",
                "target_cwd": str(Path("/tmp/target").resolve()),
                "log_path": data["log_path"],
            }
        ]

    @pytest.mark.asyncio
    async def test_import_registers_target_cwd_in_recent_dirs(self, tmp_path, monkeypatch):
        """重建成功后把 target_cwd 登记进 recent_dirs.json，使其跨重启在侧边栏可见。"""
        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        from workflow_controller.api import config_api as config_api_module

        # 禁用临时区判定：沙盒 tmp_path 位于真实系统临时区下，登记应成功
        # （「登记成功」语义与目录卫生正交，卫生契约由下一条用例覆盖）。
        monkeypatch.setattr(config_api_module, "_transient_roots", lambda: ())

        orch, _plugin, api_obj = _orch_with_persisted_config(tmp_path)

        # 真实 ConfigAPI（落盘到 tmp_path/recent_dirs.json）挂到 app.state.api.config
        app = FastAPI()
        app.state.api = api_obj
        app.include_router(build_router(_plugin_finder(orch)))
        transport = ASGITransport(app=app)
        target = tmp_path / "target"
        target.mkdir()
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post(
                "/api/sessions/import-db",
                json={
                    "db_path": "/tmp/cannbot.db",
                    "target_cwd": str(target),
                },
            )
        data = r.json()
        assert r.status_code == 200
        assert data["ok"] is True
        # recent_dirs.json 落盘含 target_cwd（resolve 后）
        saved = json.loads((tmp_path / "recent_dirs.json").read_text(encoding="utf-8"))
        assert str(target.resolve()) in saved

    async def test_import_skips_transient_target_in_recent_dirs(self, tmp_path):
        """瞬态 target（系统临时区）不登记进 recent_dirs——重建到 tempdir 的项目
        不再作为「最近项目」常驻侧边栏（历史噪声 `workflow-controller-recon-*/project` 的回归守护）。
        """
        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        orch, _plugin, api_obj = _orch_with_persisted_config(tmp_path)

        # 不 monkeypatch：tmp_path 位于真实系统临时区下（本仓 hermetic 环境外亦是）
        app = FastAPI()
        app.state.api = api_obj
        app.include_router(build_router(_plugin_finder(orch)))
        # 不 monkeypatch：瞬态判定生效，登记被卫生谓词拒绝
        target = tmp_path / "target"
        target.mkdir()
        payload = {"db_path": "/tmp/cannbot.db", "target_cwd": str(target)}
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.post("/api/sessions/import-db", json=payload)
        assert r.status_code == 200
        assert r.json()["ok"] is True
        # 重建本身成功，但 recent_dirs.json 不落盘（登记被卫生谓词拒绝）
        assert not (tmp_path / "recent_dirs.json").exists()
        assert str(target) not in api_obj.config.get_recent_dirs()


@pytest.mark.parametrize("cli_wrapper", ["cpx", ""])
@pytest.mark.asyncio
async def test_traceback_route_passes_cli_wrapper_override(
    stub_orch_with_plugin,
    cli_wrapper: str,
):
    """验证重放路由透传 cpx 与显式关闭。"""
    orch, plugin = stub_orch_with_plugin
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    app = FastAPI()
    app.include_router(build_router(_plugin_finder(orch)))
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/api/sessions/s1/traceback",
            json={
                "checkpoint_id": "cp-1",
                "runtime": "claude_cli",
                "overrides": {"cli_wrapper": cli_wrapper, "model": ""},
            },
        )

    assert plugin.traceback_calls[0]["overrides"] == {"cli_wrapper": cli_wrapper}


@pytest.mark.asyncio
async def test_traceback_route_passes_reviewer_thinking_overrides(stub_orch_with_plugin):
    orch, plugin = stub_orch_with_plugin
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    app = FastAPI()
    app.include_router(build_router(_plugin_finder(orch)))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.post(
            "/api/sessions/s1/traceback",
            json={
                "checkpoint_id": "cp-1",
                "overrides": {
                    "reviewer_thinking_enabled": False,
                    "reviewer_thinking_budget_tokens": 4096,
                    "reviewer_max_tokens": 8192,
                },
            },
        )
    assert plugin.traceback_calls[0]["overrides"] == {
        "reviewer_thinking_enabled": False,
        "reviewer_thinking_budget_tokens": 4096,
        "reviewer_max_tokens": 8192,
    }


@pytest.mark.asyncio
async def test_traceback_runtime_route_uses_active_plugin(stub_orch_with_plugin):
    orch, _ = stub_orch_with_plugin
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    app = FastAPI()
    app.include_router(build_router(_plugin_finder(orch)))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/sessions/s1/traceback/runtime")
    assert response.json() == {"runtime": "claude_cli"}


class TestImportLogRoute:
    """``GET /api/sessions/import-db/log?target_cwd=`` 端点测试。"""

    @pytest.mark.asyncio
    async def test_missing_target_cwd_returns_404(self):
        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        app = FastAPI()
        app.include_router(build_router(_plugin_finder(_StubOrch())))
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/api/sessions/import-db/log")
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_log_not_found_returns_404(self, tmp_path):
        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        app = FastAPI()
        app.include_router(build_router(_plugin_finder(_StubOrch())))
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get(
                "/api/sessions/import-db/log",
                params={"target_cwd": str(tmp_path)},
            )
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_log_file(self, tmp_path):
        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        log_dir = tmp_path / ".workflow-controller" / "reconstruct"
        log_dir.mkdir(parents=True)
        log_file = log_dir / "reconstruction.log"
        log_file.write_text("phase=db_read\nphase=complete\n", encoding="utf-8")

        app = FastAPI()
        app.include_router(build_router(_plugin_finder(_StubOrch())))
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get(
                "/api/sessions/import-db/log",
                params={"target_cwd": str(tmp_path)},
            )
        assert r.status_code == 200
        assert "phase=complete" in r.text
        assert "reconstruction.log" in r.headers.get("content-disposition", "")


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    async def _run():
        orch, plugin = _StubOrch(), _StubPlugin()
        orch.plugins = [plugin]
        result = await plugin.import_session("/tmp/x.db", "/tmp/y", log_path="/tmp/y.log")
        assert result["ok"] is True
        assert plugin.calls == [
            {
                "db_path": "/tmp/x.db",
                "target_cwd": "/tmp/y",
                "log_path": "/tmp/y.log",
            }
        ]
        logging.info("ok")

    asyncio.run(_run())
