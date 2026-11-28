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

"""会话内更新 reviewer 任务目标：两处落点、在跑 reviewer、持久化与审计事件。"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from workflow_controller.config.settings import AppConfig, SessionConfig, SessionReviewerConfig
from workflow_controller.core.orchestrator import Orchestrator
from workflow_controller.session.events import EventType

if TYPE_CHECKING:
    from pathlib import Path

_UPDATE_ACTION = "reviewer_task_goal_updated"


def _orchestrator(tmp_path: Path) -> Orchestrator:
    """会话记录落 tmp_path 的 orchestrator（不启动，事件面已接线）。"""
    return Orchestrator(AppConfig(project_dir=str(tmp_path)), default_project_dir=tmp_path)


def _worker_config(task_goal: str) -> SessionConfig:
    """带 reviewer 快照与 criteria 的 worker 配置（两处 task_goal 同源）。"""
    return SessionConfig(
        name="worker",
        role="worker",
        cwd=".",
        initial_prompt="test",
        reviewer=SessionReviewerConfig(enabled=True, task_goal=task_goal),
        criteria={"task_goal": task_goal},
    )


def _live_task_goal(orchestrator: Orchestrator, session_id: str) -> str:
    """在跑 ReviewerService 运行时配置里的任务目标。"""
    reviewer = orchestrator.reviewer_for(session_id)
    return getattr(reviewer, "_config").task_goal


def _roster_task_goals(orchestrator: Orchestrator, session_id: str) -> tuple[str, str]:
    """sessions 表行里的 (reviewer.task_goal, criteria.task_goal)。"""
    row = orchestrator.event_router.get_session_sync(session_id)
    config = json.loads(row["config_json"])
    return config["reviewer"]["task_goal"], config["criteria"]["task_goal"]


async def _audit_events(orchestrator: Orchestrator, session_id: str) -> list[dict[str, Any]]:
    """事件记录里的目标更新审计事件 data。"""
    events = await orchestrator.event_router.get_events(session_id)
    return [
        event.data
        for event in events
        if event.event_type == EventType.LOG_AUTO and event.data.get("action") == _UPDATE_ACTION
    ]


async def test_update_rewrites_both_goals_live_reviewer_and_ledger(tmp_path: Path) -> None:
    """更新同时改写两处落点、在跑 reviewer 与 sessions 表，并落一条含前后值的审计事件。"""
    orchestrator = _orchestrator(tmp_path)
    session = await orchestrator.pool.create(_worker_config("旧目标"))
    await orchestrator.ensure_reviewer_snapshot(session.id)

    stored = await orchestrator.set_session_reviewer_task_goal(session.id, "  新目标\n")

    assert stored == "新目标"
    assert session.config.reviewer.task_goal == "新目标"
    assert session.config.criteria == {"task_goal": "新目标"}
    assert _live_task_goal(orchestrator, session.id) == "新目标"
    assert _roster_task_goals(orchestrator, session.id) == ("新目标", "新目标")
    assert await _audit_events(orchestrator, session.id) == [
        {"action": _UPDATE_ACTION, "previous_task_goal": "旧目标", "task_goal": "新目标"}
    ]


async def test_rebuilt_reviewer_snapshot_picks_up_updated_goal(tmp_path: Path) -> None:
    """会话没有在跑 reviewer 时只改配置，之后重建的 reviewer 快照用新目标。"""
    orchestrator = _orchestrator(tmp_path)
    session = await orchestrator.pool.create(_worker_config("旧目标"))

    await orchestrator.set_session_reviewer_task_goal(session.id, "新目标")
    await orchestrator.ensure_reviewer_snapshot(session.id)

    assert _live_task_goal(orchestrator, session.id) == "新目标"


async def test_update_restores_evicted_session_without_keeping_it_resident(tmp_path: Path) -> None:
    """已离开内存池的会话按需恢复后更新，写入 sessions 表且不改变池驻留。"""
    orchestrator = _orchestrator(tmp_path)
    session = await orchestrator.pool.create(_worker_config("旧目标"))
    orchestrator.pool.discard(session.id)

    stored = await orchestrator.set_session_reviewer_task_goal(session.id, "新目标")

    assert stored == "新目标"
    assert orchestrator.pool.get(session.id) is None
    assert _roster_task_goals(orchestrator, session.id) == ("新目标", "新目标")
    assert len(await _audit_events(orchestrator, session.id)) == 1


async def test_update_rejects_blank_or_unknown_and_skips_unchanged_goal(tmp_path: Path) -> None:
    """空白目标与不存在的会话被拒绝；值未变化时不写入也不发事件。"""
    orchestrator = _orchestrator(tmp_path)
    session = await orchestrator.pool.create(_worker_config("旧目标"))
    await orchestrator.ensure_reviewer_snapshot(session.id)

    with pytest.raises(ValueError, match="empty"):
        await orchestrator.set_session_reviewer_task_goal(session.id, " \n")
    with pytest.raises(ValueError, match="not found"):
        await orchestrator.set_session_reviewer_task_goal("missing", "新目标")
    assert await orchestrator.set_session_reviewer_task_goal(session.id, "旧目标") == "旧目标"

    assert _live_task_goal(orchestrator, session.id) == "旧目标"
    assert _roster_task_goals(orchestrator, session.id) == ("旧目标", "旧目标")
    assert await _audit_events(orchestrator, session.id) == []
