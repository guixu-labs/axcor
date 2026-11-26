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

"""Web CLI --hook-port 参数的运行期覆盖传递测试。"""

from __future__ import annotations

import os
import sys
from types import ModuleType

from click.testing import CliRunner


def _install_fake_web_cli(monkeypatch) -> dict[str, object]:
    """注入 fake web.cli 并记录 run_web_command 调用时的环境变量。"""
    captured: dict[str, object] = {}

    def fake_run_web_command(*args: object, **kwargs: object) -> None:
        captured["args"] = args
        captured["kwargs"] = kwargs

    web_module = ModuleType("workflow_controller.web")
    cli_module = ModuleType("workflow_controller.web.cli")
    cli_module.run_web_command = fake_run_web_command  # type: ignore[attr-defined]
    web_module.cli = cli_module  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "workflow_controller.web", web_module)
    monkeypatch.setitem(sys.modules, "workflow_controller.web.cli", cli_module)
    return captured


def test_cmd_web_hook_port_sets_environment(monkeypatch) -> None:
    captured = _install_fake_web_cli(monkeypatch)
    monkeypatch.setenv("WORKFLOW_CONTROLLER_HOOK_PORT", "18765")

    from workflow_controller.__main__ import cli

    result = CliRunner().invoke(cli, ["web", "--hook-port", "18775"])

    assert result.exit_code == 0, result.output
    assert os.environ["WORKFLOW_CONTROLLER_HOOK_PORT"] == "18775"
    assert captured["args"][0] is not None


def test_cmd_web_without_hook_port_clears_environment(monkeypatch) -> None:
    captured = _install_fake_web_cli(monkeypatch)
    monkeypatch.setenv("WORKFLOW_CONTROLLER_HOOK_PORT", "18775")

    from workflow_controller.__main__ import cli

    result = CliRunner().invoke(cli, ["web"])

    assert result.exit_code == 0, result.output
    assert "WORKFLOW_CONTROLLER_HOOK_PORT" not in os.environ
    assert captured["args"][0] is not None
