#!/usr/bin/env python
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

"""headless fake run 冒烟：验证仅安装发行 wheel 的环境可完成完整 run。

工作目录落在临时目录，不依赖仓内源码与测试基建；供 kernel-only
安装矩阵（内核 + fake-driver wheel 的干净环境）使用。
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from workflow_controller.api.core_api import boot
from workflow_controller.config.settings import AppConfig, HookServerConfig, ReviewerConfig


async def run_fake_program(workdir: Path) -> str:
    """boot 内核并跑一个 fake@1 程序到终态，返回 run 状态。"""
    program = workdir / "fake-program.yaml"
    program.write_text("format: fake@1\n", encoding="utf-8")
    config = AppConfig(
        project_dir=str(workdir),
        hook_server=HookServerConfig(enabled=True),
        reviewer=ReviewerConfig(enabled=False),
    )
    orchestrator, api = await boot(config, workdir / "workflow-controller-config.yaml", hook_port=0)
    try:
        run = await api.run.start("fake-smoke", program, fmt="fake@1")
        while run.status.value not in ("completed", "failed"):
            await asyncio.sleep(0.1)
            run = await api.run.get_run(run.run_id)
        return run.status.value
    finally:
        await orchestrator.shutdown()


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="kernel-only-smoke.") as tmp:
        status = asyncio.run(run_fake_program(Path(tmp)))
    print(f"fake run 终态：{status}")
    return 0 if status == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
