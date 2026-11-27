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

"""headless 验收：干净解释器内经 boot() 驱动 fake run，全程禁止导入 workflow_controller.web。

用法：python -m tests.support.headless_acceptance <project_dir> <settings_dir> [scenario]
scenario：complete（默认）| fail | crash | recover；结果以单行 JSON 打到 stdout，
退出码 0 表示验收通过。crash 在 run 留痕可查且驱动快照落盘后以非零码硬退出
（跳过关停），由 recover 在第二个进程内验证恢复链把崩溃遗留的 run 续跑到终态。
"""

from __future__ import annotations

import asyncio
import importlib.abc
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from workflow_controller.api.core_api import CoreAPI
    from workflow_controller.core.orchestrator import Orchestrator
    from workflow_controller.core.runs import RunRecord

logger = logging.getLogger(__name__)
report_logger = logging.getLogger(f"{__name__}.report")
report_logger.propagate = False
report_logger.setLevel(logging.INFO)
_report_handler = logging.StreamHandler(sys.stdout)
_report_handler.setFormatter(logging.Formatter("%(message)s"))
report_logger.addHandler(_report_handler)

RUN_WAIT_TIMEOUT_S = 30.0
# 崩溃场景退出码：非零且避开 main 的 0/1/2，验收侧据此区分硬崩与正常完成
CRASH_EXIT_CODE = 70


class WebImportBlocker(importlib.abc.MetaPathFinder):
    """meta-path 守卫：workflow_controller.web 的导入尝试当场失败。"""

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> None:
        if fullname == "workflow_controller.web" or fullname.startswith("workflow_controller.web."):
            raise ImportError(f"headless 验收禁止导入 {fullname}")
        return None


async def boot_fake_kernel(project_dir: Path, settings_dir: Path) -> tuple[Orchestrator, CoreAPI]:
    """以最小配置 boot 内核：hook server 承载插件装载，reviewer 关闭。"""
    from workflow_controller.api.core_api import boot
    from workflow_controller.config.settings import AppConfig, HookServerConfig, ReviewerConfig

    config = AppConfig(
        project_dir=str(project_dir),
        hook_server=HookServerConfig(enabled=True),
        reviewer=ReviewerConfig(enabled=False),
    )
    return await boot(
        config,
        project_dir / "workflow-controller-config.yaml",
        settings_dir=settings_dir,
        hook_port=0,
    )


async def start_fake_run(
    api: CoreAPI, project_dir: Path, params: dict[str, str] | None
) -> RunRecord:
    """落程序文件并启动 fake@1 run。"""
    from workflow_controller.plugins.fake_driver.plugin import FAKE_FORMAT

    program = project_dir / "fake-program.yaml"
    program.write_text(f"format: {FAKE_FORMAT}\n", encoding="utf-8")
    return await api.run.start("headless-acceptance", program, fmt=FAKE_FORMAT, params=params)


async def wait_run_finished(api: CoreAPI, run_id: str) -> RunRecord:
    """轮询 run 至终态并返回终态记录。"""
    deadline = time.monotonic() + RUN_WAIT_TIMEOUT_S
    while time.monotonic() < deadline:
        record = await api.run.get_run(run_id)
        if record.finished_at is not None:
            return record
        await asyncio.sleep(0.05)
    raise TimeoutError(f"run {run_id} 未在 {RUN_WAIT_TIMEOUT_S}s 内到终态")


async def run_event_types(api: CoreAPI, run_id: str) -> list[str]:
    """run 全量事件类型去重排序。"""
    events, _, _ = await api.run.fetch_events(run_id)
    return sorted({e["event_type"] for e in events})


async def wait_run_event(api: CoreAPI, run_id: str, event_type: str) -> None:
    """轮询 run 事件直到指定类型出现。"""
    deadline = time.monotonic() + RUN_WAIT_TIMEOUT_S
    while time.monotonic() < deadline:
        events, _, _ = await api.run.fetch_events(run_id)
        if any(e["event_type"] == event_type for e in events):
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"run {run_id} 未在 {RUN_WAIT_TIMEOUT_S}s 内出现 {event_type} 事件")


async def wait_driver_snapshot(api: CoreAPI, run_id: str) -> None:
    """轮询至驱动快照落盘：事件可见先于快照落库，恢复链只认快照。"""
    deadline = time.monotonic() + RUN_WAIT_TIMEOUT_S
    while time.monotonic() < deadline:
        if await api.run.driver_snapshot(run_id) is not None:
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"run {run_id} 未在 {RUN_WAIT_TIMEOUT_S}s 内落驱动快照")


async def scenario_complete(project_dir: Path, settings_dir: Path) -> dict[str, Any]:
    """完成路径：fake run 经 boot() 到 completed 并留痕。"""
    from workflow_controller.plugins.fake_driver.plugin import FAKE_PROGRESS_EVENT

    orch, api = await boot_fake_kernel(project_dir, settings_dir)
    try:
        run = await start_fake_run(api, project_dir, params=None)
        record = await wait_run_finished(api, run.run_id)
        event_types = await run_event_types(api, run.run_id)
        status = str(record.status)
        return {
            "run_id": run.run_id,
            "status": status,
            "event_types": event_types,
            "ok": status == "completed" and FAKE_PROGRESS_EVENT in event_types,
        }
    finally:
        await orch.shutdown()


async def scenario_fail(project_dir: Path, settings_dir: Path) -> dict[str, Any]:
    """失败路径：驱动判定失败，run 到 failed 且错误已记录。"""
    from workflow_controller.plugins.fake_driver.plugin import (
        BEHAVIOR_PARAM,
        FAIL_BEHAVIOR,
        FAKE_PROGRESS_EVENT,
    )

    orch, api = await boot_fake_kernel(project_dir, settings_dir)
    try:
        run = await start_fake_run(api, project_dir, params={BEHAVIOR_PARAM: FAIL_BEHAVIOR})
        record = await wait_run_finished(api, run.run_id)
        event_types = await run_event_types(api, run.run_id)
        status = str(record.status)
        return {
            "run_id": run.run_id,
            "status": status,
            "error": record.error,
            "event_types": event_types,
            "ok": status == "failed" and bool(record.error) and FAKE_PROGRESS_EVENT in event_types,
        }
    finally:
        await orch.shutdown()


async def scenario_crash(project_dir: Path, settings_dir: Path) -> dict[str, Any]:
    """崩溃路径：resumable run 留痕可查且快照落盘后硬退出，模拟内核进程崩溃。"""
    from workflow_controller.plugins.fake_driver.plugin import (
        BEHAVIOR_PARAM,
        FAKE_PROGRESS_EVENT,
        RESUMABLE_BEHAVIOR,
    )

    # 崩溃场景不持内核句柄关停：留痕可查且快照落盘后退出
    _orch, api = await boot_fake_kernel(project_dir, settings_dir)
    run = await start_fake_run(api, project_dir, params={BEHAVIOR_PARAM: RESUMABLE_BEHAVIOR})
    await wait_run_event(api, run.run_id, FAKE_PROGRESS_EVENT)
    await wait_driver_snapshot(api, run.run_id)
    # 崩溃前读账面事实：run 必须仍在 running 且无终态时间，报告不硬编码
    record = await api.run.get_run(run.run_id)
    status = str(record.status)
    if status != "running" or record.finished_at is not None:
        raise AssertionError(
            f"崩溃前 run 已离开 running：status={status} finished_at={record.finished_at}"
        )
    report_logger.info(
        json.dumps({"run_id": run.run_id, "status": status, "ok": True}, ensure_ascii=False)
    )
    sys.stdout.flush()
    # 跳过显式 orch.shutdown，以专用非零码退出，run 停在非终态
    raise _CrashExitError(CRASH_EXIT_CODE)


async def scenario_recover(project_dir: Path, settings_dir: Path) -> dict[str, Any]:
    """恢复路径：boot() 恢复链把崩溃遗留的 run 续跑到 completed。"""
    from workflow_controller.plugins.fake_driver.plugin import (
        FAKE_FORMAT,
        FAKE_PROGRESS_EVENT,
        FAKE_RESUMED_EVENT,
    )

    orch, api = await boot_fake_kernel(project_dir, settings_dir)
    try:
        fake_runs = [
            r
            for r in await api.run.list_runs()
            if r["format"] == FAKE_FORMAT and r["kind"] == "workflow"
        ]
        if not fake_runs:
            raise LookupError("没有可恢复的 fake run")
        run_id = max(fake_runs, key=lambda r: r["created_at"])["run_id"]
        record = await wait_run_finished(api, run_id)
        event_types = await run_event_types(api, run_id)
        status = str(record.status)
        return {
            "run_id": run_id,
            "status": status,
            "event_types": event_types,
            "ok": status == "completed"
            and FAKE_PROGRESS_EVENT in event_types
            and FAKE_RESUMED_EVENT in event_types,
        }
    finally:
        await orch.shutdown()


class _CrashExitError(Exception):
    """崩溃场景专用退出信号，由验收入口转换为原非零退出码。"""


SCENARIOS = {
    "complete": scenario_complete,
    "fail": scenario_fail,
    "crash": scenario_crash,
    "recover": scenario_recover,
}


def main(argv: list[str]) -> int:
    """验收入口：装守卫、跑场景、单行 JSON 报告结果。"""
    project_dir = Path(argv[0]).resolve()
    settings_dir = Path(argv[1]).resolve()
    scenario_name = argv[2] if len(argv) > 2 else "complete"
    if scenario_name not in SCENARIOS:
        logger.error("未知场景：%s（可选 %s）", scenario_name, sorted(SCENARIOS))
        return 2
    settings_dir.mkdir(parents=True, exist_ok=True)
    # boot() 以 cwd 为存储锚点，先落位再装导入守卫
    os.chdir(project_dir)
    sys.meta_path.insert(0, WebImportBlocker())
    try:
        result = asyncio.run(SCENARIOS[scenario_name](project_dir, settings_dir))
    except _CrashExitError as exc:
        return int(str(exc))
    report_logger.info(json.dumps(result, ensure_ascii=False))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
