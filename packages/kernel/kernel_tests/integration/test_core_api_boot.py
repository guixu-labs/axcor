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

"""CoreAPI boot() 对 Web CLI Hook Server 端口覆盖的集成测试。"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from workflow_controller.api.core_api import boot
from workflow_controller.config.settings import (
    AppConfig,
    HookServerConfig,
    ReviewerConfig,
)
from workflow_controller.core.orchestrator import Orchestrator
from workflow_controller.core.run_coordinator import RunCoordinator
from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService


@pytest.mark.asyncio
async def test_boot_consumes_hook_port_environment(tmp_path: Path, monkeypatch) -> None:
    """boot() 读取 Web CLI 传入的 Hook Server 端口覆盖。"""
    from workflow_controller.hooks.server import HookServer

    monkeypatch.setenv("WORKFLOW_CONTROLLER_HOOK_PORT", "18999")
    monkeypatch.setattr(
        SessionRunAffiliationService,
        "recover_at_boot",
        AsyncMock(),
    )
    monkeypatch.setattr(RunCoordinator, "recover_at_boot", AsyncMock())
    monkeypatch.setattr(Orchestrator, "shutdown", AsyncMock())
    config = AppConfig(
        project_dir=str(tmp_path),
        runtime="claude_cli",
        hook_server=HookServerConfig(enabled=True, host="127.0.0.1"),
        reviewer=ReviewerConfig(enabled=False),
    )
    monkeypatch.setattr(HookServer, "start", AsyncMock())

    orchestrator, _api = await boot(
        config,
        tmp_path / "workflow-controller-config.yaml",
        settings_dir=tmp_path,
    )
    try:
        assert orchestrator.effective_hook_server_port == 18999
        assert orchestrator.hook_server_base_url == "http://127.0.0.1:18999"
        assert not hasattr(config.hook_server, "port")

        settings_file = tmp_path / ".claude" / "settings.json"
        data = json.loads(settings_file.read_text(encoding="utf-8"))
        hook = data["hooks"]["Stop"][0]["hooks"][0]
        assert hook["url"] == "http://127.0.0.1:18999/hooks/stop"
    finally:
        await orchestrator.shutdown()
