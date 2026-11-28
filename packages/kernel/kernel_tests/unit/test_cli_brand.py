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

"""CLI --help 头部品牌随全局配置 brand_name（与 Web 品牌同源）。"""

from __future__ import annotations

import sys

import click
from click.testing import CliRunner

from workflow_controller.__main__ import main


def _run_help(monkeypatch) -> str:
    # 固定 main 内层 cli() 读取的 argv，由 CliRunner 捕获退出码与输出。
    monkeypatch.setattr(sys, "argv", ["workflow-controller", "--help"])
    command = click.Command(
        "workflow-controller",
        callback=main,
        add_help_option=False,
        context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    )
    result = CliRunner().invoke(command, ["--help"])
    assert result.exit_code == 0, result.output
    return result.output


def test_cli_help_shows_configured_brand(tmp_path, monkeypatch):
    """启动目录 workflow-controller-config.yaml 配了 brand_name → help 头部跟随。"""
    (tmp_path / "workflow-controller-config.yaml").write_text(
        "brand_name: Ascendbot\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    assert "Ascendbot — headless runner + HTTP API + Web UI." in _run_help(monkeypatch)


def test_cli_help_defaults_to_default_brand(tmp_path, monkeypatch):
    """无 workflow-controller-config.yaml → 回落默认品牌。"""
    monkeypatch.chdir(tmp_path)
    assert "Workflow Controller — headless runner + HTTP API + Web UI." in _run_help(monkeypatch)
