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

"""内核独立 HTTP 应用的组合根测试：启动输入解析、关停与真实 boot 的生命周期。"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import workflow_controller.api.http.app as http_app
from workflow_controller.api.core_api import CoreAPI
from workflow_controller.api.http.app import (
    ORCHESTRATOR_SHUTDOWN_TIMEOUT_S,
    hook_port_from_environment,
    load_boot_context,
    reset_sse_exit_flag,
    shutdown_orchestrator,
)
from workflow_controller.config.settings import AppConfig
from workflow_controller.core.orchestrator import SESSION_DRAIN_TIMEOUT_S
from workflow_controller.core.run_coordinator import (
    KILL_GRACE_S,
    STOP_DRAIN_TIMEOUT_S,
    SURROGATE_SNAPSHOT_TIMEOUT_S,
    SURROGATE_THREAD_DRAIN_S,
    TASK_CANCEL_EXIT_TIMEOUT_S,
)

_RUN_WAIT_TIMEOUT_S = 30.0


def test_hook_port_environment_is_optional(monkeypatch) -> None:
    monkeypatch.delenv("WORKFLOW_CONTROLLER_HOOK_PORT", raising=False)
    assert hook_port_from_environment() is None


def test_hook_port_environment_is_parsed(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_CONTROLLER_HOOK_PORT", "18775")
    assert hook_port_from_environment() == 18775


def test_hook_port_environment_rejects_non_integer(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_CONTROLLER_HOOK_PORT", "invalid")
    with pytest.raises(RuntimeError, match="WORKFLOW_CONTROLLER_HOOK_PORT"):
        hook_port_from_environment()


def test_load_boot_context_prefers_environment_over_cwd_config(tmp_path, monkeypatch) -> None:
    """启动输入：配置读启动目录 yaml，端口/设置目录/日志级别以 CLI 经环境传入的值为准。"""
    (tmp_path / "workflow-controller-config.yaml").write_text(
        "brand_name: Ascendbot\nlog_level: DEBUG\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WORKFLOW_CONTROLLER_HOOK_PORT", "18775")
    monkeypatch.setenv("WORKFLOW_CONTROLLER_SETTINGS_DIR", str(tmp_path / "settings"))
    monkeypatch.delenv("WORKFLOW_CONTROLLER_LOG_LEVEL", raising=False)

    context = load_boot_context()

    assert context.config.brand_name == "Ascendbot"
    assert context.config_path == Path(
        "workflow-controller-config.yaml"
    )  # 相对启动目录，与 web 一致
    assert context.settings_dir == tmp_path / "settings"
    assert context.hook_port == 18775
    assert context.log_level == "DEBUG"  # CLI 未给级别时用 yaml 的
    monkeypatch.setenv("WORKFLOW_CONTROLLER_LOG_LEVEL", "WARNING")
    assert load_boot_context().log_level == "WARNING"  # CLI 给了以 CLI 为准


def test_load_boot_context_falls_back_to_defaults(tmp_path, monkeypatch) -> None:
    """启动目录没有配置文件、CLI 也没传覆盖：全部回落默认。"""
    monkeypatch.chdir(tmp_path)
    for name in ("HOOK_PORT", "SETTINGS_DIR", "LOG_LEVEL"):
        monkeypatch.delenv(f"WORKFLOW_CONTROLLER_{name}", raising=False)

    context = load_boot_context()

    assert context.config == AppConfig()
    assert context.settings_dir is None
    assert context.hook_port is None
    assert context.log_level == AppConfig().log_level


def test_reset_sse_exit_flag_clears_process_singleton(monkeypatch) -> None:
    from sse_starlette.sse import AppStatus

    monkeypatch.setattr(AppStatus, "should_exit", True)
    reset_sse_exit_flag()
    assert AppStatus.should_exit is False


async def test_shutdown_orchestrator_gives_up_after_timeout(monkeypatch, caplog) -> None:
    """关停超过上限不再等待：记告警后返回，由调用方强制退出。"""

    async def never_finishes() -> None:
        await asyncio.sleep(60)

    monkeypatch.setattr(http_app, "ORCHESTRATOR_SHUTDOWN_TIMEOUT_S", 0.01)
    orchestrator = SimpleNamespace(shutdown=never_finishes)

    started = time.monotonic()
    with caplog.at_level("WARNING", logger="workflow_controller.api.http.app"):
        await shutdown_orchestrator(orchestrator)

    assert time.monotonic() - started < 5
    assert "Orchestrator shutdown timed out" in caplog.text


def test_orchestrator_shutdown_timeout_covers_run_plane_worst() -> None:
    # 关停并发收尾：run 平面的耗时上界 = 单个卡死 run 的完整收尾（成员
    # 数按下界 5 计）。常量被调到低于该上界时测试必然失败——否则 run
    # 平面之后的 pool/插件/bus 收尾步骤会被 lifespan 的 wait_for 中途取消
    single_run_worst = (
        4 * STOP_DRAIN_TIMEOUT_S  # 排空窗 + 终态扫掠出生/spawn/kill 内三次排空
        + TASK_CANCEL_EXIT_TIMEOUT_S  # cancel 后任务退出等待（取消可被收尾路径吸收）
        + SURROGATE_THREAD_DRAIN_S  # 代办快照前运行中的驱动线程退出等待
        + SURROGATE_SNAPSHOT_TIMEOUT_S  # 代办快照
        + 5 * SESSION_DRAIN_TIMEOUT_S  # 逐成员 stop_session
        + KILL_GRACE_S  # 进程组 SIGTERM 宽限
    )
    assert single_run_worst < ORCHESTRATOR_SHUTDOWN_TIMEOUT_S


def test_kernel_app_boots_and_completes_fake_run(tmp_path, monkeypatch) -> None:
    """lifespan 真实 boot：health 可用，经 POST /api/v1/runs 创建并跑完一个 fake run，退出关停。"""
    from workflow_controller.plugins.fake_driver.plugin import FAKE_FORMAT, FAKE_PROGRESS_EVENT

    (tmp_path / "workflow-controller-config.yaml").write_text(
        "brand_name: KernelBot\nproject_dir: .\nhook_server:\n  enabled: true\n"
        "reviewer:\n  enabled: false\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WORKFLOW_CONTROLLER_HOOK_PORT", "0")
    monkeypatch.setenv("WORKFLOW_CONTROLLER_SETTINGS_DIR", str(tmp_path / "settings"))

    app = http_app.create_app()
    with TestClient(app) as client:
        assert client.get("/api/v1/health").status_code == 200
        # Swagger 标题随启动目录配置的 brand_name
        assert client.get("/openapi.json").json()["info"]["title"] == "KernelBot"

        program = tmp_path / "fake-program.yaml"
        program.write_text(f"format: {FAKE_FORMAT}\n", encoding="utf-8")
        created = client.post(
            "/api/v1/runs",
            json={
                "format": FAKE_FORMAT,
                "program_path": str(program),
                "work_dir": str(tmp_path / "work"),
            },
        )
        assert created.status_code == 201, created.text
        run_id = created.json()["run_id"]

        deadline = time.monotonic() + _RUN_WAIT_TIMEOUT_S
        while True:
            run = client.get(f"/api/v1/runs/{run_id}").json()["run"]
            if run["finished_at"] is not None or time.monotonic() >= deadline:
                break
            time.sleep(0.05)
        assert run["status"] == "completed", run
        events = client.get(f"/api/v1/runs/{run_id}/events").json()["events"]
        assert FAKE_PROGRESS_EVENT in {event["event_type"] for event in events}

        # 宿主约定：路由经 app.state.api 取内核门面；fake-driver 没有 web router
        assert isinstance(app.state.api, CoreAPI)
        assert app.state.plugin_routers == set()

    # 日志按启动目录落盘，与 CLI 报告的路径一致
    assert (tmp_path / ".workflow-controller" / "workflow-controller.log").exists()
