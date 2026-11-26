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

"""编排器侧的单会话工作区门控测试：开关、归属与屏障接线零变化。"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from workflow_controller.config.settings import SessionConfig, SessionTracebackConfig
from workflow_controller.core.orchestrator import Orchestrator
from workflow_controller.core.runs import RunKind, RunStatus

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.config.settings import AppConfig

pytestmark = pytest.mark.asyncio


@pytest.fixture
def base_config(tmp_path: Path) -> AppConfig:
    """测试用最小应用配置（复用钩子处理器测试的构造形态）。"""
    from workflow_controller.config.settings import (
        AppConfig,
        HookServerConfig,
        ReviewerConfig,
    )

    return AppConfig(
        project_dir=str(tmp_path),
        harmony_output_path=str(tmp_path),
        hook_server=HookServerConfig(enabled=False),
        reviewer=ReviewerConfig(enabled=False),
    )


async def _make_session(orch: Orchestrator, *, traceback: SessionTracebackConfig | None):
    """池内建一个会话并返回（隐式归属随创建铸造）。"""
    config = SessionConfig(name="s1", cwd=".", traceback=traceback)
    return await orch.pool.create(config)


async def test_untraced_session_gate_off_and_hooks_unchanged(tmp_path, base_config):
    """未开启 traceback：门控关闭、屏障回调不接线（行为零变化）。"""
    orch = Orchestrator(base_config, default_project_dir=tmp_path / "gate.db")
    try:
        session = await _make_session(orch, traceback=None)

        assert await orch._session_workspace_enabled(session) is False
        hooks = orch.make_driver_hooks(session.id)
        assert hooks.before_side_effect_tool is None
        assert hooks.before_subagent_dispatch is None
        assert not orch.session_workspaces.owns(session.id)
    finally:
        await orch.shutdown()


async def test_traceback_standalone_session_gate_on(tmp_path, base_config):
    """开启 traceback 的独立会话（隐式归属）：门控放行。"""
    orch = Orchestrator(base_config, default_project_dir=tmp_path / "gate.db")
    try:
        session = await _make_session(orch, traceback=SessionTracebackConfig())

        assert await orch._session_workspace_enabled(session) is True
    finally:
        await orch.shutdown()


async def test_traceback_run_member_gate_off(tmp_path, base_config):
    """开启 traceback 但归属显式 run（如容器成员）：run 平面管辖，门控关闭。"""
    orch = Orchestrator(base_config, default_project_dir=tmp_path / "gate.db")
    try:
        run = await orch.run_store.create(
            "",
            "",
            labels={"name": "container"},
            status=RunStatus.RUNNING,
            kind=RunKind.CONTAINER,
        )
        config = SessionConfig(
            name="s1",
            cwd=".",
            traceback=SessionTracebackConfig(),
            run_id=run.run_id,
        )
        session = await orch.pool.create(config)
        await orch.affiliation.attach(session.id, run.run_id, role="worker")

        assert await orch._session_workspace_enabled(session) is False
    finally:
        await orch.shutdown()
