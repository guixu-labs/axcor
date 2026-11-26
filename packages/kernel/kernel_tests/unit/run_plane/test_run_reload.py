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

"""重载当前工作流定义的 run 平面入口测试：准入门、工厂载荷与换装 op 通道。

驱动为静默玩具（不派发不收口，run 停在 running 等控制）；工厂可选
实现 reload_definition（翻译侧 I/O 替身），换装经 op 应答通道回执。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from workflow_controller.core.run_coordinator import RunConfig, RunCoordinator
from workflow_controller.core.runs import RunStore
from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService

from ._run_plane_stubs import StubBus
from .test_run_coordinator import FakeSO


class IdleDriver:
    """静默驱动：不派发不收口；记录收到的 op 供断言。"""

    def __init__(self) -> None:
        self.ops: list[tuple[str, dict]] = []

    def decide(self, world, notifications):
        return []

    def on_control(self, control):
        return []

    def projection(self):
        return {"schema_version": 1}

    def snapshot(self):
        return {"schema_version": 1, "state": {}, "pending_timers": []}

    def handle_op(self, op, args):
        self.ops.append((op, dict(args)))
        return {"echo": op}


class ReloadFactory:
    """支持重载的工厂：翻译侧返回合成载荷并留档调用。"""

    def __init__(self, error: Exception | None = None) -> None:
        self.format = "fake@1"
        self.driver = IdleDriver()
        self.error = error
        self.reload_calls: list[tuple[str, Path]] = []

    def create(self, run, program_path, params):
        return self.driver

    def restore(self, run, snapshot, sessions):
        return self.driver

    def reload_definition(self, run, driver, source):
        if self.error is not None:
            raise self.error
        self.reload_calls.append((run.run_id, Path(source)))
        return {"program": object(), "subgraph_plans": []}


class BareFactory:
    """无重载能力的工厂（仅 create/restore）。"""

    def __init__(self) -> None:
        self.format = "bare@1"
        self.driver = IdleDriver()

    def create(self, run, program_path, params):
        return self.driver

    def restore(self, run, snapshot, sessions):
        return self.driver


class ReloadHarness:
    """run 平面装配（静默驱动 + 可选重载工厂）。"""

    def __init__(self, tmp_path: Path, factory: ReloadFactory | BareFactory) -> None:
        self.store = RunStore(tmp_path / "events.db")
        self.affiliation = SessionRunAffiliationService(self.store)
        self.bus = StubBus()
        self.so = FakeSO(self.affiliation, self.bus, tmp_path)
        self.factory = factory
        self.coord = RunCoordinator(
            orchestrator=self.so,
            run_store=self.store,
            bus=self.bus,
            driver_factories={factory.format: factory},
            affiliation=self.affiliation,
        )
        self.program = tmp_path / "prog.txt"
        self.program.write_text("format: fake\n", encoding="utf-8")

    async def start(self) -> str:
        run = await self.coord.start_run(
            RunConfig(format=self.factory.format, program_ref=str(self.program)),
            self.program,
        )
        return run.run_id


async def test_reload_routes_through_factory_and_driver_op(tmp_path: Path) -> None:
    """暂停态受理全链路：工厂翻译 → 冻结随行 → 换装 op 在 run 任务内执行。"""
    harness = ReloadHarness(tmp_path, ReloadFactory())
    run_id = await harness.start()
    await harness.coord.pause_run(run_id)

    result = await harness.coord.reload_run_definition(run_id)

    assert result == {"echo": "reload_definition"}
    assert harness.factory.reload_calls == [(run_id, harness.program)]
    op, args = harness.factory.driver.ops[-1]
    assert op == "reload_definition"
    frozen = Path(args["yaml_path"])
    assert frozen.is_file()
    assert frozen.read_text(encoding="utf-8") == harness.program.read_text(encoding="utf-8")
    assert args["agents_dir_base"] == str(harness.program.parent)
    await harness.coord.stop_run(run_id)


async def test_reload_rejected_while_running(tmp_path: Path) -> None:
    """运行中（非暂停）重载被拒绝，工厂不被触达。"""
    factory = ReloadFactory()
    harness = ReloadHarness(tmp_path, factory)
    run_id = await harness.start()

    with pytest.raises(ValueError, match="仅暂停态受理"):
        await harness.coord.reload_run_definition(run_id)

    assert factory.reload_calls == []
    await harness.coord.stop_run(run_id)


async def test_reload_rejected_after_terminal(tmp_path: Path) -> None:
    """终态 run 重载被拒绝（恢复前置不满足）。"""
    harness = ReloadHarness(tmp_path, ReloadFactory())
    run_id = await harness.start()
    await harness.coord.stop_run(run_id)

    with pytest.raises(ValueError, match="已终态"):
        await harness.coord.reload_run_definition(run_id)


async def test_reload_rejected_for_container_run(tmp_path: Path) -> None:
    """非 workflow 形态（纯容器）不支持重载。"""
    harness = ReloadHarness(tmp_path, ReloadFactory())
    run = await harness.coord.create_run("box")

    with pytest.raises(ValueError, match="不支持重载定义"):
        await harness.coord.reload_run_definition(run.run_id)


async def test_reload_rejected_for_factory_without_capability(tmp_path: Path) -> None:
    """驱动工厂未实现重载能力时明确拒绝。"""
    harness = ReloadHarness(tmp_path, BareFactory())
    run_id = await harness.start()
    await harness.coord.pause_run(run_id)

    with pytest.raises(ValueError, match="不支持重载定义"):
        await harness.coord.reload_run_definition(run_id)

    await harness.coord.stop_run(run_id)


async def test_reload_rejected_for_missing_source(tmp_path: Path) -> None:
    """源文件不存在明确拒绝（可读错误带路径）。"""
    harness = ReloadHarness(tmp_path, ReloadFactory())
    run_id = await harness.start()
    await harness.coord.pause_run(run_id)

    missing = tmp_path / "nope.yaml"
    with pytest.raises(ValueError, match=f"源文件不存在.*{missing.name}"):
        await harness.coord.reload_run_definition(run_id, missing)

    await harness.coord.stop_run(run_id)


async def test_reload_wraps_translation_failure(tmp_path: Path) -> None:
    """翻译失败包装为可读错误；run 保持暂停态。"""
    harness = ReloadHarness(tmp_path, ReloadFactory(error=RuntimeError("yaml 坏了")))
    run_id = await harness.start()
    await harness.coord.pause_run(run_id)

    with pytest.raises(ValueError, match=r"工作流定义重读失败.*yaml 坏了"):
        await harness.coord.reload_run_definition(run_id)

    assert (await harness.store.get(run_id)).status == "paused"
    await harness.coord.stop_run(run_id)
