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

"""共享 pytest 配置."""

import sys
from pathlib import Path

import pytest

from tests.support.pytest_fixtures import (
    cancel_spawned_background_tasks,
    capture_workflow_controller_logger_state,
    restore_workflow_controller_logger_state,
)

pytest_plugins = ("tests.support.pytest_hermetic", "pytester")

_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


@pytest.fixture(autouse=True)
async def _cancel_background_tasks():
    """测试收尾统一收拢 spawn() 遗留的后台 task，防进程挂起。"""
    yield
    await cancel_spawned_background_tasks()


@pytest.fixture(autouse=True)
def _restore_workflow_controller_logger_state():
    """configure_logging 改过的 logger 全局状态逐用例还原。"""
    saved = capture_workflow_controller_logger_state()
    yield
    restore_workflow_controller_logger_state(saved)
