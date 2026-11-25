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

"""根 conftest 与 tests/conftest 共享的 autouse 夹具实现（两处 conftest 只留薄包装）。"""

import asyncio
import logging


def capture_workflow_controller_logger_state() -> tuple[bool, bool, int, list[logging.Handler]]:
    """读取 ``workflow_controller`` logger 的全局状态（propagate/disabled/level/handlers）。"""
    logger = logging.getLogger("workflow_controller")
    return (logger.propagate, logger.disabled, logger.level, list(logger.handlers))


def restore_workflow_controller_logger_state(
    saved: tuple[bool, bool, int, list[logging.Handler]],
) -> None:
    """还原 logger 全局状态：configure_logging 关 propagate、装文件 handler，

    测试不还原时同进程后续用例的日志（caplog 经 root 捕获）会静默收不到。
    """
    logger = logging.getLogger("workflow_controller")
    logger.propagate, logger.disabled, logger.level = saved[:3]
    for handler in list(logger.handlers):
        if handler not in saved[3]:
            handler.close()
            logger.removeHandler(handler)


async def cancel_spawned_background_tasks() -> None:
    """cancel 并收拢 spawn() 留下的 fire-and-forget 后台 task。

    sync_session_status / runner.resume 等路径会 spawn(runner.run())——
    run() 是 while True 主循环、_run_one 会阻塞在 _pause_event.wait()，
    这些 task 永不自然结束。不 cancel 的话 pytest 关 event loop 时，
    pending task 会导致进程挂起（EXIT=124）。
    """
    from workflow_controller.support import tasks

    spawned = list(getattr(tasks, "_background_tasks"))
    for task in spawned:
        if not task.done():
            task.cancel()
    if spawned:
        await asyncio.gather(*spawned, return_exceptions=True)
