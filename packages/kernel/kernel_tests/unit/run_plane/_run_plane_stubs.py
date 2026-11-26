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

"""run 平面单测的装配替身：成员/池/总线与 pool 谓词面。

与 graph 侧联合装配（tests/plugins/graph/run_plane_helpers.py 的
SwitchBus/FakeRunSO）刻意保持不同形态与命名，避免重复代码检查
互相判重；此处只承载 unit 各测试共用的最小事实面。
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from workflow_controller.session.events import SessionOutcome


@dataclass
class StubSession:
    """成员 session 的最小事实（状态/停因/循环位，可选归属 run）。"""

    id: str
    name: str
    state: str = "running"
    stop_reason: str | None = None
    loop_status: str = "yes"
    run_id: str | None = None


class StubPool:
    """内存成员池：出生登记、按 sid 查询与全量列举。"""

    def __init__(self) -> None:
        self.sessions: dict[str, StubSession] = {}

    def get(self, sid: str) -> StubSession | None:
        return self.sessions.get(sid)

    def list_sessions(self) -> list[StubSession]:
        return list(self.sessions.values())


class StubBus:
    """测试总线：观察者登记 + outcome/state 回投；broadcast/ephemeral 仅留档。"""

    def __init__(self) -> None:
        self.observers: dict[str, list] = {}
        self.broadcasted: list = []
        self.ephemeral: list = []

    def observe(self, event_type, handler) -> None:
        self.observers.setdefault(getattr(event_type, "value", str(event_type)), []).append(handler)

    async def emit_outcome(self, sid: str, payload: dict) -> None:
        event = SimpleNamespace(session_id=sid, data=payload)
        for handler in self.observers.get("session_outcome", []):
            await handler(event)

    async def emit_state(self, sid: str) -> None:
        for handler in self.observers.get("session_state_changed", []):
            await handler(SimpleNamespace(session_id=sid))

    async def broadcast(self, event: Any) -> None:
        self.broadcasted.append(event)

    async def emit_ephemeral(self, event: Any) -> None:
        self.ephemeral.append(event)


class PoolFactsMixin:
    """pool 谓词面：协调器消费的 get_session_facts 与三判定。

    宿主类需提供 self.pool（StubPool 或同形池）。
    """

    pool: StubPool

    def get_session_facts(self, sid: str):
        info = self.pool.get(sid)
        if info is None:
            return None
        return SimpleNamespace(
            state=info.state,
            stop_reason=info.stop_reason,
            loop_status=info.loop_status,
        )

    def is_pausable(self, sid: str) -> bool:
        info = self.get_session_facts(sid)
        return info is not None and info.loop_status == "yes"

    def is_resumable(self, sid: str) -> bool:
        info = self.get_session_facts(sid)
        return info is not None and info.state == "idle" and info.stop_reason == "user_pause"

    def is_terminal(self, sid: str) -> bool:
        info = self.get_session_facts(sid)
        if info is None:
            return True
        paused = info.state == "idle" and info.stop_reason == "user_pause"
        return info.loop_status == "no" and not paused


def member_outcome_payload(
    sid: str,
    kind: str,
    stop_reason: str | None,
    digest: str = "digest",
) -> dict:
    """成员结局事件 payload：unit 各 SO 替身统一回投的公共形状。"""
    return SessionOutcome(
        session_id=sid,
        kind=kind,
        stop_reason=stop_reason,
        last_error=None,
        result_digest=digest,
        iteration=1,
        context=None,
    ).to_payload()
