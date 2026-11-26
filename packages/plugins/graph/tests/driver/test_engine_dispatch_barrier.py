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

"""严格档下成员出生规格的 writer 子代理拒绝测试（能力矩阵出生侧落点）。

runtime 缺子代理 cwd 注入能力（②）且 traceback 工作区严格档开启时，
成员规格禁用 task 派发工具——writer 子代理拒绝启动的出生侧表现。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from workflow_controller.plugin.run_driver import StartSession
from workflow_controller.plugin.workspace import PrepareScopeWorkspace, ScopeWorkspacePrepared

from .test_engine import build_driver, decide, pick

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.plugins.graph.driver.engine import GraphDriver

_TRACEBACK_CFG: dict[str, Any] = {"enabled": True}


def _runtime_driver(tmp_path: Path, runtime: str, traceback: dict | None) -> GraphDriver:
    """按 runtime 运行项构造驱动（yaml 落盘走生产翻译路径）。"""
    cfg: dict[str, Any] = {"_runner_options": {"runtime": runtime}} if runtime else {}
    if traceback is not None:
        cfg["traceback"] = traceback
    return build_driver(
        tmp_path,
        [{"id": "a", "task_type": "agent", "prompt": "go"}],
        **cfg,
    )


def _spawn_spec(driver: GraphDriver) -> dict[str, Any]:
    """驱动到成员出生并返回其 config_overrides。"""
    actions = decide(driver)
    if not pick(actions, StartSession):
        prepared = pick(actions, PrepareScopeWorkspace)
        assert prepared
        note = ScopeWorkspacePrepared(
            scope_id="a-w1",
            scope_epoch=1,
            base_commit="b" * 40,
            integration_base_commit="i" * 40,
            workspace_path="/tmp/ws-a",
            context=prepared[0].context,
        )
        actions = decide(driver, (note,))
    starts = pick(actions, StartSession)
    assert len(starts) == 1
    return dict(starts[0].spec.config_overrides)


def test_strict_traceback_denies_dispatch_tools_on_barrierless_runtime(
    tmp_path: Path,
) -> None:
    """严格档 + opencode：成员规格禁 task/Agent，writer 子代理拒绝启动。"""
    driver = _runtime_driver(tmp_path, "opencode", _TRACEBACK_CFG)
    overrides = _spawn_spec(driver)
    assert overrides["disallowed_tools"] == ["Task", "Agent"]
    assert overrides["runtime"] == "opencode"


def test_strict_traceback_keeps_claude_sdk_untouched(tmp_path: Path) -> None:
    """严格档 + claude SDK：五项能力齐备，不禁派发工具。"""
    driver = _runtime_driver(tmp_path, "claude_sdk", _TRACEBACK_CFG)
    overrides = _spawn_spec(driver)
    assert "disallowed_tools" not in overrides


def test_compat_mode_does_not_deny(tmp_path: Path) -> None:
    """显式弱化档（compat_late_barrier）不禁派发工具。"""
    driver = _runtime_driver(
        tmp_path,
        "opencode",
        {"enabled": True, "on_no_dispatch_barrier": "compat_late_barrier"},
    )
    overrides = _spawn_spec(driver)
    assert "disallowed_tools" not in overrides


def test_traceback_off_does_not_deny(tmp_path: Path) -> None:
    """traceback 未开启时不禁派发工具（旧路径零变化）。"""
    driver = _runtime_driver(tmp_path, "opencode", None)
    overrides = _spawn_spec(driver)
    assert "disallowed_tools" not in overrides
