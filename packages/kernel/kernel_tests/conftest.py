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

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# 裸 pytest 直跑（未安装包）时回退包内 src（向上探测内核包根）
_src_root = next(
    (
        p / "src"
        for p in Path(__file__).resolve().parents
        if (p / "src" / "workflow_controller").is_dir()
    ),
    None,
)
if _src_root is not None and str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))


@pytest.fixture
def tmp_store(tmp_path: Path):
    from workflow_controller.store.event_store import EventStore

    store = EventStore(tmp_path / "test.db")
    yield store


@pytest.fixture
def mock_session():
    from unittest.mock import AsyncMock

    from workflow_controller.session.session import Session

    session = Session(name="test-worker", role="worker")
    session.query = AsyncMock()
    return session


@pytest.fixture
def mock_evaluator():
    from unittest.mock import AsyncMock

    from workflow_controller.core.evaluator import EvaluationResult

    evaluator = AsyncMock()
    evaluator.evaluate = AsyncMock(
        return_value=EvaluationResult(
            is_complete=False,
            progress=0.5,
            summary="Mock evaluation",
            feedback="Keep going",
        )
    )
    return evaluator
