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

"""serve 子命令：选项经环境穿透到 lifespan，uvicorn 以工厂方式启动内核应用。"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import uvicorn
from click.testing import CliRunner

if TYPE_CHECKING:
    from pathlib import Path

    from click.testing import Result

_ENV_KEYS = (
    "WORKFLOW_CONTROLLER_HOOK_PORT",
    "WORKFLOW_CONTROLLER_SETTINGS_DIR",
    "WORKFLOW_CONTROLLER_LOG_LEVEL",
)


def _forget_env(monkeypatch, *keys: str) -> None:
    """让键在测试期间不存在、收尾时还原原值。

    monkeypatch.delenv 只登记删除时已存在的键；先 setenv 再 delenv 才能把
    「原本不存在」也登记进还原栈，命令随后新增的键才会被收尾清掉。
    """
    for key in keys:
        monkeypatch.setenv(key, "")
        monkeypatch.delenv(key)


def _prepare(monkeypatch, tmp_path: Path, log_level: str) -> dict[str, object]:
    """启动目录落配置文件、清掉同族环境变量、截获 uvicorn.run。"""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "workflow-controller-config.yaml").write_text(
        f"log_level: {log_level}\n", encoding="utf-8"
    )
    _forget_env(monkeypatch, *_ENV_KEYS)
    captured: dict[str, object] = {}
    monkeypatch.setattr(uvicorn, "run", lambda app, **kwargs: captured.update(app=app, **kwargs))
    return captured


def _invoke_serve(monkeypatch, *args: str) -> tuple[Result, dict[str, str]]:
    """执行 serve，返回结果与命令写入的环境变量快照（命令直接写 os.environ）。"""
    from workflow_controller.__main__ import cli

    result = CliRunner().invoke(cli, ["serve", *args])
    snapshot = {key: os.environ[key] for key in _ENV_KEYS if key in os.environ}
    _forget_env(monkeypatch, *_ENV_KEYS)
    return result, snapshot


def test_cmd_serve_passes_options_through_environment(tmp_path: Path, monkeypatch) -> None:
    """CLI 给出的端口、设置目录与日志级别经环境传给 lifespan；uvicorn 以工厂方式启动内核应用。"""
    captured = _prepare(monkeypatch, tmp_path, log_level="INFO")
    settings_dir = tmp_path / "settings"

    result, env = _invoke_serve(
        monkeypatch,
        "--port",
        "18700",
        "--host",
        "127.0.0.1",
        "--hook-port",
        "18775",
        "--settings-dir",
        str(settings_dir),
        "--log-level",
        "DEBUG",
    )

    assert result.exit_code == 0, result.output
    log_path = tmp_path / ".workflow-controller" / "workflow-controller.log"
    assert json.loads(result.output.splitlines()[-1]) == {
        "event": "info",
        "msg": f"Logging to {log_path}",
    }
    assert captured == {
        "app": "workflow_controller.api.http.app:create_app",
        "host": "127.0.0.1",
        "port": 18700,
        "factory": True,
        "timeout_graceful_shutdown": 3,
    }
    assert env == {
        "WORKFLOW_CONTROLLER_HOOK_PORT": "18775",
        "WORKFLOW_CONTROLLER_SETTINGS_DIR": str(settings_dir.resolve()),
        "WORKFLOW_CONTROLLER_LOG_LEVEL": "DEBUG",
    }
    assert log_path.exists()


def test_cmd_serve_without_overrides_uses_config_and_clears_hook_port(
    tmp_path: Path, monkeypatch
) -> None:
    """未给选项：默认端口 18600，清除继承的 hook 端口，日志级别取启动目录配置。"""
    captured = _prepare(monkeypatch, tmp_path, log_level="WARNING")
    monkeypatch.setenv("WORKFLOW_CONTROLLER_HOOK_PORT", "18775")

    result, env = _invoke_serve(monkeypatch)

    assert result.exit_code == 0, result.output
    assert captured["port"] == 18600
    assert captured["app"] == "workflow_controller.api.http.app:create_app"
    assert "WORKFLOW_CONTROLLER_HOOK_PORT" not in env
    assert "WORKFLOW_CONTROLLER_SETTINGS_DIR" not in env
    assert env["WORKFLOW_CONTROLLER_LOG_LEVEL"] == "WARNING"
