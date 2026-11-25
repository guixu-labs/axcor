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

"""仓根共享 pytest 配置：环境隔离插件注册与对全部测试树生效的通用收尾夹具。"""

import pytest

from tests.support.pytest_fixtures import (
    cancel_spawned_background_tasks,
    capture_workflow_controller_logger_state,
    restore_workflow_controller_logger_state,
)

pytest_plugins = ("tests.support.pytest_hermetic", "pytester")


@pytest.fixture(autouse=True)
def _restore_workflow_controller_logger_state():
    """还原 logger 全局状态；实现见 tests.support.pytest_fixtures。"""
    saved = capture_workflow_controller_logger_state()
    yield
    restore_workflow_controller_logger_state(saved)


@pytest.fixture(autouse=True)
async def _cancel_background_tasks():
    """收尾 cancel 后台 task；实现见 tests.support.pytest_fixtures。"""
    yield
    await cancel_spawned_background_tasks()
