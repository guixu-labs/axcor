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

"""检查点推导器：从 task 调用边界推导检查点元数据。

遍历统一时间线，每个主 agent 的 ``task`` tool call 对应一个检查点；
末尾追加 ``session_complete`` 终态检查点。元数据对齐 ``Checkpoint``
dataclass，可经 ``CheckpointStore.store_sync`` 批量写入 events.db。

重建检查点 id 用 ``cp_rc_`` 前缀（rc=reconstructed），与实时的
``cp_<sid>_<n>`` 区分，便于 Dashboard 标识与溯源。
"""

from __future__ import annotations

__all__ = [
    "CheckpointDeriver",
]

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from workflow_controller.plugins.agent_traceback.checkpoint_store import Checkpoint
from workflow_controller.plugins.agent_traceback.session_reconstructor.db_reader import (
    TimelineEntry,
    _extract_subagent_sid_from_args,
)

if TYPE_CHECKING:
    from workflow_controller.plugins.agent_traceback.session_reconstructor.file_state_rebuilder import (
        CheckpointFileState,
    )

logger = logging.getLogger(__name__)


@dataclass
class _TaskBoundary:
    """主 agent 的一个 task 边界（补充检查点元数据）。"""

    iteration: int  # task 序号（1-based）
    subagent_session_id: str  # 子 agent sdk session id
    agent_type: str  # 子 agent 类型


class CheckpointDeriver:
    """从 task 调用边界推导检查点元数据。

    输入：统一时间线 + 重放产出的 ``CheckpointFileState`` 列表（含 git_ref）+
    转写器产出的 ``task_part_ids`` 映射（子 session id → task part id）+
    重建 session id。输出：``list[Checkpoint]``。
    """

    @staticmethod
    def derive(
        timeline: list[TimelineEntry],
        file_states: list[CheckpointFileState],
        task_part_ids: dict[str, str],
        session_id: str,
        native_session_id: str,
        bridges: list[dict[str, Any]] | None = None,
    ) -> list[Checkpoint]:
        """推导检查点列表。

        ``session_id`` 是 Workflow Controller 内部 session id；``native_session_id`` 是
        opencode 的 ``ses_xxx``，用于 checkpoint_id 前缀。``bridges`` 查 subagent_type。
        """
        # 子 session id → subagent_type（从 InteractionBridge 查）
        sub_type_map = _build_sub_type_map(bridges)
        checkpoints = _derive_subagent_checkpoints(
            timeline=timeline,
            file_states=file_states,
            task_part_ids=task_part_ids,
            session_id=session_id,
            native_session_id=native_session_id,
            sub_type_map=sub_type_map,
        )
        # 末尾 session_complete 检查点
        complete_cp = _derive_session_complete_checkpoint(
            file_states=file_states,
            session_id=session_id,
            native_session_id=native_session_id,
            parent_checkpoint_id=(checkpoints[-1].checkpoint_id if checkpoints else None),
        )
        if complete_cp is not None:
            checkpoints.append(complete_cp)
        return checkpoints


def _build_checkpoint(
    *,
    checkpoint_id: str,
    session_id: str,
    iteration: int,
    agent_id: str,
    agent_type: str,
    trigger: str,
    git_ref: str | None,
    main_part_id: str | None,
    parent_checkpoint_id: str | None,
    fs: CheckpointFileState,
) -> Checkpoint:
    """构造 ``Checkpoint`` 行，``created_at`` 用当前时间。"""
    return Checkpoint(
        checkpoint_id=checkpoint_id,
        session_id=session_id,
        iteration=iteration,
        agent_id=agent_id,
        agent_type=agent_type,
        trigger=trigger,
        git_ref=git_ref,
        event_rowid=None,  # 重建 session 无 events 表事件可链接
        main_part_id=main_part_id,
        parent_checkpoint_id=parent_checkpoint_id,
        created_at=datetime.now(UTC).isoformat(),
        file_count=fs.file_count,
        byte_size=fs.byte_size,
        status=fs.status,
        inner_git=fs.inner_git,
    )


def _collect_main_task_boundaries(
    timeline: list[TimelineEntry],
) -> list[_TaskBoundary]:
    """从 timeline 收集主 agent 的 task 边界（按时间序，1-based）。"""
    boundaries: list[_TaskBoundary] = []
    iteration = 0
    for entry in timeline:
        if entry.role != "main" or entry.kind != "tool_call":
            continue
        tc = entry.tool_call
        if tc is None or str(tc.get("toolName") or "") != "task":
            continue
        iteration += 1
        sub_sid = entry.subagent_session_id or _extract_subagent_sid_from_args(tc)
        agent_type = _extract_subagent_type_from_args(tc)
        boundaries.append(
            _TaskBoundary(
                iteration=iteration,
                subagent_session_id=sub_sid,
                agent_type=agent_type,
            )
        )
    return boundaries


def _build_sub_type_map(bridges: list[dict[str, Any]] | None) -> dict[str, str]:
    """子 session id → subagent_type（从 InteractionBridge 查）。"""
    sub_type_map: dict[str, str] = {}
    for b in bridges or []:
        sid = str(b.get("subagentSessionId") or "")
        st = str(b.get("subagentType") or "")
        if sid and st:
            sub_type_map[sid] = st
    return sub_type_map


def _derive_subagent_checkpoints(
    timeline: list[TimelineEntry],
    file_states: list[CheckpointFileState],
    task_part_ids: dict[str, str],
    session_id: str,
    native_session_id: str,
    sub_type_map: dict[str, str],
) -> list[Checkpoint]:
    """按 subagent_start 的 file_states 逐个构造检查点。"""
    # 按 checkpoint_index 对齐 file_states（1-based）
    # file_states 已按时间序：前 N 个是 subagent_start，最后一个是 session_complete
    file_state_by_index: dict[int, CheckpointFileState] = {
        fs.checkpoint_index: fs for fs in file_states
    }

    checkpoints: list[Checkpoint] = []
    main_task_boundaries = _collect_main_task_boundaries(timeline)
    # 以 file_states 为准（重放是真相），避免 git_ref 错配
    n_subagent_starts = sum(1 for fs in file_states if fs.trigger == "subagent_start")
    if len(main_task_boundaries) != n_subagent_starts:
        logger.warning(
            "derive: timeline main task boundaries (%d) != file_states "
            "subagent_start count (%d) — aligning to file_states",
            len(main_task_boundaries),
            n_subagent_starts,
        )

    # 每个 subagent_start 对应一个 file_state（按序号顺序）
    sub_indices = sorted(
        fs.checkpoint_index for fs in file_states if fs.trigger == "subagent_start"
    )
    for i, cp_idx in enumerate(sub_indices):
        fs = file_state_by_index[cp_idx]
        boundary = main_task_boundaries[i] if i < len(main_task_boundaries) else None
        sub_sid = fs.subagent_session_id or (boundary.subagent_session_id if boundary else "")
        agent_type = sub_type_map.get(sub_sid) or (boundary.agent_type if boundary else "")
        iteration = boundary.iteration if boundary else (i + 1)
        cp = _build_checkpoint(
            checkpoint_id=f"cp_rc_{native_session_id[:8]}_{cp_idx:03d}",
            session_id=session_id,
            iteration=iteration,
            agent_id=sub_sid,
            agent_type=agent_type,
            trigger="subagent_start",
            git_ref=fs.git_ref,
            main_part_id=task_part_ids.get(sub_sid),
            parent_checkpoint_id=(checkpoints[-1].checkpoint_id if checkpoints else None),
            fs=fs,
        )
        checkpoints.append(cp)
    return checkpoints


def _derive_session_complete_checkpoint(
    file_states: list[CheckpointFileState],
    session_id: str,
    native_session_id: str,
    parent_checkpoint_id: str | None,
) -> Checkpoint | None:
    """构造末尾 session_complete 终态检查点；无终态 file_state 时返回 None。"""
    complete_fs = next(
        (fs for fs in file_states if fs.trigger == "session_complete"),
        None,
    )
    if complete_fs is None:
        return None
    cp_idx = complete_fs.checkpoint_index
    return _build_checkpoint(
        checkpoint_id=f"cp_rc_{native_session_id[:8]}_{cp_idx:03d}",
        session_id=session_id,
        iteration=complete_fs.checkpoint_index,
        agent_id="",  # 终态无 subagent
        agent_type="main",
        trigger="session_complete",
        git_ref=complete_fs.git_ref,
        main_part_id=None,
        parent_checkpoint_id=parent_checkpoint_id,
        fs=complete_fs,
    )


def _extract_subagent_type_from_args(tc: dict[str, Any]) -> str:
    """从 task tool_call 的 argsJson 抽 subagent_type（兜底）。"""
    raw = tc.get("argsJson") or ""
    if not raw:
        return ""
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return ""
    return str(obj.get("subagent_type") or "")
