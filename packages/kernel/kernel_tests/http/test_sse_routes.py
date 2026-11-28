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

"""SSE 流行为断言（一轮节拍的 topic payload + usage_update 活通道回归）+
源码 grep 检查（无插件名反射、无 loop 私有字段直摸）。
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import anyio


class _FakeEventAPI:
    @staticmethod
    def snapshot_cursor() -> str:
        return "cur-0"

    @staticmethod
    def get_recent_at_cursor(cursor: str, limit: int) -> list:
        return []

    @staticmethod
    def get_since_cursor(cursor: str):
        return (cursor, [])

    @staticmethod
    def snapshot_run_event_cursor() -> str:
        return "3"

    @staticmethod
    def get_run_events_since_cursor(cursor: str):
        # 一轮内回报一条 run 键事件（游标推进）
        return (
            "6",
            [
                {
                    "id": 6,
                    "run_id": "run-1",
                    "context": "node=b",
                    "timestamp": "2026-10-14T10:00:00",
                    "event_type": "process_output",
                    "data": {"proc_id": "p1", "line": "hi"},
                },
            ],
        )


class _FakeAPI:
    def __init__(self) -> None:
        self.event = _FakeEventAPI()
        self.session = SimpleNamespace(
            list_sessions=lambda: [],
            running_loop_sessions=lambda: [{"id": "s1", "name": "w1"}],
        )
        self.plugin = SimpleNamespace(
            get_handoff_states=lambda: {"s1": {"pending": True}},
        )
        self.pipeline = SimpleNamespace(all_snapshots=lambda: {})
        self.usage = SimpleNamespace(
            snapshot=lambda: SimpleNamespace(
                turns=1,
                tokens_in=10,
                tokens_out=20,
                cache_read=0,
                cache_created=0,
                cost=0.01,
            )
        )
        self.config = SimpleNamespace(get_config_dict=lambda: {"k": "v"})
        self.auth = SimpleNamespace(key_status=lambda: {"mode": "API", "providers": []})


class _FakeRequest:
    """is_disconnected 第二次起返回 True：恰好收集一轮完整节拍后断开。"""

    def __init__(self) -> None:
        self.app = SimpleNamespace(
            state=SimpleNamespace(
                api=_FakeAPI(),
                config_section_providers={
                    "context_bay": SimpleNamespace(
                        read_config_section=lambda: {"repo_url": "https://example.com/r.git"},
                    ),
                },
            )
        )
        self._rounds = 0

    async def is_disconnected(self) -> bool:
        self._rounds += 1
        return self._rounds > 1


async def test_one_round_topic_payloads_with_usage_update_regression() -> None:
    from workflow_controller.api.http.routes.sse_routes import event_stream

    events: dict[str, list] = {}
    async for ev in event_stream(_FakeRequest(), anyio.Event()):
        events.setdefault(ev["event"], []).append(json.loads(ev["data"]))

    assert events.get("usage_update") == [
        {
            "turns": 1,
            "tokens_in": 10,
            "tokens_out": 20,
            "cache_read": 0,
            "cache_created": 0,
            "cost": 0.01,
        }
    ]
    assert events.get("loop_update") == [
        {
            "running_sessions": [{"id": "s1", "name": "w1"}],
            "watching_session_id": "s1",
            "watching_session_name": "w1",
        }
    ]
    assert events.get("handoff_update") == [{"states": {"s1": {"pending": True}}}]
    assert events.get("auth_update") == [{"mode": "API", "providers": []}]
    # config_update 段组合装配期注入的领域配置段
    assert events.get("config_update") == [
        {"k": "v", "context_bay": {"repo_url": "https://example.com/r.git"}}
    ]


def test_snapshots_are_shared_per_app_and_expire(monkeypatch) -> None:
    from workflow_controller.api.http.routes import sse_routes

    now = [100.0]
    monkeypatch.setattr(sse_routes.time, "monotonic", lambda: now[0])
    first = _FakeRequest()
    second = _FakeRequest()
    second.app = first.app
    calls = [0]

    def build() -> int:
        calls[0] += 1
        return calls[0]

    assert getattr(sse_routes, "_cached_snap")(first, "sessions", build) == 1
    assert getattr(sse_routes, "_cached_snap")(second, "sessions", build) == 1
    assert calls[0] == 1
    now[0] += 0.6
    assert getattr(sse_routes, "_cached_snap")(second, "sessions", build) == 2
    assert getattr(sse_routes, "_cached_snap")(_FakeRequest(), "sessions", build) == 3


async def test_one_round_pushes_run_key_events_via_claude_event() -> None:
    from workflow_controller.api.http.routes.sse_routes import event_stream

    claude_events: list[dict] = []
    async for ev in event_stream(_FakeRequest(), anyio.Event()):
        if ev["event"] == "claude_event":
            claude_events.append(json.loads(ev["data"]))

    # run 键事件经 claude_event 通道推送：带 run_id、session_id 为空（前端据此分 run 桶）
    assert {
        "rowid": 6,
        "session_id": "",
        "run_id": "run-1",
        "context": "node=b",
        "event_type": "process_output",
        "iteration": 0,
        "timestamp": "2026-10-14T10:00:00",
        "data": {"proc_id": "p1", "line": "hi"},
    } in claude_events


async def test_run_event_cursor_snapshot_failure_skips_replay() -> None:
    from workflow_controller.api.http.routes.sse_routes import event_stream

    class _FailRunSnapshotEventAPI(_FakeEventAPI):
        def __init__(self) -> None:
            self.calls = 0

        def snapshot_run_event_cursor(self) -> str:
            self.calls += 1
            raise RuntimeError("db busy")

    request = _FakeRequest()
    request.app.state.api.event = _FailRunSnapshotEventAPI()

    claude_events: list[dict] = []
    async for ev in event_stream(request, anyio.Event()):
        if ev["event"] == "claude_event":
            claude_events.append(json.loads(ev["data"]))

    # 快照失败重试一次仍失败：跳过 run_events 段，不回退到 0 解成整表全量回放
    event_api = request.app.state.api.event
    assert event_api.calls == 2
    assert claude_events == []


def test_sse_routes_source_has_no_plugin_name_reflection_or_loop_private_access() -> None:
    import workflow_controller.api.http.routes.sse_routes as sse_routes

    source = Path(sse_routes.__file__).read_text(encoding="utf-8")

    assert "breather" not in source
    assert "_loops" not in source
    assert "_current_session" not in source
