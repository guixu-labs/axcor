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

"""headless CLI 日志：hook 启动失败按 JSON 事件流报告，日志配置不被外部 dictConfig 禁用。"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

from click.testing import CliRunner

if TYPE_CHECKING:
    from pathlib import Path


def test_cmd_run_reports_hook_startup_failure_as_json(tmp_path: Path, monkeypatch) -> None:
    from workflow_controller.__main__ import cli
    from workflow_controller.hooks.server import HookServer

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WORKFLOW_CONTROLLER_HOOK_PORT", "18765")
    start = AsyncMock(side_effect=OSError("Address already in use"))
    monkeypatch.setattr(HookServer, "start", start)

    result = CliRunner().invoke(cli, ["run", "--headless", "--settings-dir", str(tmp_path)])

    assert result.exit_code == 3, result.output
    events = [json.loads(line) for line in result.stdout.splitlines()]
    assert [event["event"] for event in events] == ["info", "error"]
    assert "localhost:18765" in events[-1]["msg"]
    assert "Address already in use" in events[-1]["msg"]
    assert result.stderr == ""
    start.assert_awaited_once()


def test_configure_logging_unblocks_disabled_logger(tmp_path: Path) -> None:
    """uvicorn 启动跑 dictConfig(disable_existing_loggers=True)，会把 workflow_controller
    logger 的 ``.disabled`` 置 True。configure_logging 必须清掉它——否则 web
    模式 lifespan 里即使重新加了 handler，日志仍被 logger 自身拦截（实测
    现象：web 的 workflow-controller.log 0 行，plugin/门禁/reviewer 一条都没写）。

    cmd_run（headless）不经 uvicorn 所以没暴露；web 端到端才撞到。
    """
    import logging

    from workflow_controller.logging_config import configure_logging

    ops = logging.getLogger("workflow_controller")
    ops.disabled = True  # 模拟 uvicorn dictConfig 的 disable_existing_loggers

    log_path = configure_logging(tmp_path, level="INFO")

    assert ops.disabled is False, "configure_logging 必须清掉 .disabled"
    logging.getLogger("workflow_controller.web.lifespan").info("web-lifespan-marker")
    assert "web-lifespan-marker" in log_path.read_text(encoding="utf-8")
