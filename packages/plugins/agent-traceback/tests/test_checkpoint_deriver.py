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

"""``checkpoint_deriver.py`` 单元测试 — task 边界检查点推导。

Hermetic：构造最小 timeline + file_states + task_part_ids，跑
CheckpointDeriver.derive，校验产出的 list[Checkpoint] 字段对齐设计 §4.4。
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from workflow_controller.plugins.agent_traceback.session_reconstructor.checkpoint_deriver import (
    CheckpointDeriver,
)
from workflow_controller.plugins.agent_traceback.session_reconstructor.file_state_rebuilder import (
    CheckpointFileState,
)

from ._fixtures import (
    task_dispatch_timeline_entry as _make_task_entry,
)

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.plugins.agent_traceback.session_reconstructor.db_reader import (
        TimelineEntry,
    )


def _make_file_state(
    cp_index: int,
    trigger: str,
    sub_sid: str = "",
    git_ref: str | None = "abc1234",
    parent_tc: str | None = None,
) -> CheckpointFileState:
    return CheckpointFileState(
        checkpoint_index=cp_index,
        git_ref=git_ref,
        file_count=10,
        byte_size=1024,
        trigger=trigger,
        subagent_session_id=sub_sid,
        parent_task_call_id=parent_tc,
        status="ok",
    )


# ── derive ──────────────────────────────────────────────────────────


class TestDerive:
    @staticmethod
    def test_single_task_boundary_plus_complete():
        """1 个 task 边界 + 1 个 session_complete = 2 检查点。"""
        tl = [
            _make_task_entry(1, sub_sid="ses_sub_001", call_id="call_t1"),
        ]
        file_states = [
            _make_file_state(1, "subagent_start", "ses_sub_001", parent_tc="call_t1"),
            _make_file_state(2, "session_complete", ""),
        ]
        task_part_ids = {"ses_sub_001": "prt_rc_ses_0001_0003"}
        bridges = [
            {
                "dispatchToolCallId": "call_t1",
                "subagentSessionId": "ses_sub_001",
                "subagentType": "developer",
                "status": "completed",
            }
        ]
        cps = CheckpointDeriver().derive(
            tl,
            file_states,
            task_part_ids,
            session_id="ops_sess_001",
            native_session_id="ses_094ff8ee7ffe",
            bridges=bridges,
        )
        assert len(cps) == 2
        # 第一个检查点：subagent_start
        cp0 = cps[0]
        assert cp0.checkpoint_id == "cp_rc_ses_094f_001"
        assert cp0.session_id == "ops_sess_001"
        assert cp0.iteration == 1
        assert cp0.agent_id == "ses_sub_001"
        assert cp0.agent_type == "developer"
        assert cp0.trigger == "subagent_start"
        assert cp0.git_ref == "abc1234"
        assert cp0.event_rowid is None  # 重建无 events 链接
        assert cp0.main_part_id == "prt_rc_ses_0001_0003"  # 预填
        assert cp0.parent_checkpoint_id is None  # 第一个无 parent
        assert cp0.file_count == 10
        assert cp0.byte_size == 1024
        assert cp0.status == "ok"
        # 第二个：session_complete
        cp1 = cps[1]
        assert cp1.checkpoint_id == "cp_rc_ses_094f_002"
        assert cp1.trigger == "session_complete"
        assert cp1.agent_id == ""  # 终态无 subagent
        assert cp1.agent_type == "main"
        assert cp1.parent_checkpoint_id == cp0.checkpoint_id  # 链接前一个
        assert cp1.main_part_id is None  # 终态无 task part

    @staticmethod
    def test_multiple_task_boundaries_chain():
        """多个 task 边界 → 检查点链 parent_checkpoint_id 串联。"""
        tl = [
            _make_task_entry(1, sub_sid="ses_sub_A", call_id="call_tA"),
            _make_task_entry(2, sub_sid="ses_sub_B", call_id="call_tB"),
            _make_task_entry(3, sub_sid="ses_sub_C", call_id="call_tC"),
        ]
        file_states = [
            _make_file_state(1, "subagent_start", "ses_sub_A", "ref1", "call_tA"),
            _make_file_state(2, "subagent_start", "ses_sub_B", "ref2", "call_tB"),
            _make_file_state(3, "subagent_start", "ses_sub_C", "ref3", "call_tC"),
            _make_file_state(4, "session_complete", "", "ref_final"),
        ]
        task_part_ids = {
            "ses_sub_A": "prt_A",
            "ses_sub_B": "prt_B",
            "ses_sub_C": "prt_C",
        }
        cps = CheckpointDeriver().derive(
            tl,
            file_states,
            task_part_ids,
            session_id="ops_sess_002",
            native_session_id="ses_main002",
            bridges=[],
        )
        assert len(cps) == 4
        # 检查 parent_checkpoint_id 链
        assert cps[0].parent_checkpoint_id is None
        assert cps[1].parent_checkpoint_id == cps[0].checkpoint_id
        assert cps[2].parent_checkpoint_id == cps[1].checkpoint_id
        assert cps[3].parent_checkpoint_id == cps[2].checkpoint_id
        # main_part_id 全部预填
        assert cps[0].main_part_id == "prt_A"
        assert cps[1].main_part_id == "prt_B"
        assert cps[2].main_part_id == "prt_C"
        assert cps[3].main_part_id is None  # 终态
        # subagent_session_id 按序
        assert [cp.agent_id for cp in cps[:3]] == ["ses_sub_A", "ses_sub_B", "ses_sub_C"]

    @staticmethod
    def test_missing_bridge_uses_argsjson_fallback():
        """bridge 缺失时 subagent_type 从 argsJson 抽（兜底）。"""
        tl = [
            _make_task_entry(1, sub_sid="ses_sub_X", call_id="call_tX"),
        ]
        file_states = [
            _make_file_state(1, "subagent_start", "ses_sub_X"),
            _make_file_state(2, "session_complete", ""),
        ]
        cps = CheckpointDeriver().derive(
            tl,
            file_states,
            {},
            session_id="ops_sess_003",
            native_session_id="ses_main003",
            bridges=None,  # 无 bridge
        )
        # subagent_type 从 argsJson 取（developer）
        assert cps[0].agent_type == "developer"

    @staticmethod
    def test_checkpoint_id_namespace_isolation():
        """重建检查点 id 用 cp_rc_ 前缀，与实时的 cp_ 区分（§6.8）。"""
        tl = [_make_task_entry(1, "ses_sub_001")]
        file_states = [
            _make_file_state(1, "subagent_start", "ses_sub_001"),
            _make_file_state(2, "session_complete"),
        ]
        cps = CheckpointDeriver().derive(
            tl,
            file_states,
            {},
            session_id="ops",
            native_session_id="ses_xyz12345",
        )
        for cp in cps:
            # cp_rc_<native_session_id[:8]>_<n:03d>，与实时的 cp_<sid>_<n> 区分（§6.8）
            assert cp.checkpoint_id.startswith("cp_rc_ses_xyz1_")
            assert cp.checkpoint_id != "cp_ses_xyz12_001"  # 与实时格式区分

    @staticmethod
    def test_empty_timeline_no_checkpoints():
        """空 timeline → 仅 session_complete 检查点（如果 file_states 有）。"""
        tl: list[TimelineEntry] = []
        file_states = [_make_file_state(1, "session_complete", "")]
        cps = CheckpointDeriver().derive(
            tl,
            file_states,
            {},
            session_id="ops",
            native_session_id="ses_main004",
        )
        assert len(cps) == 1
        assert cps[0].trigger == "session_complete"

    @staticmethod
    def test_store_to_checkpoint_store(tmp_path: Path):
        """产出的 Checkpoint 行可被 CheckpointStore.store_sync 写入。"""
        from workflow_controller.plugins.agent_traceback.checkpoint_store import CheckpointStore

        tl = [
            _make_task_entry(1, "ses_sub_001"),
            _make_task_entry(2, "ses_sub_002"),
        ]
        file_states = [
            _make_file_state(1, "subagent_start", "ses_sub_001", "ref1"),
            _make_file_state(2, "subagent_start", "ses_sub_002", "ref2"),
            _make_file_state(3, "session_complete", "", "ref_final"),
        ]
        cps = CheckpointDeriver().derive(
            tl,
            file_states,
            {"ses_sub_001": "prt_1", "ses_sub_002": "prt_2"},
            session_id="ops_sess_005",
            native_session_id="ses_main005",
        )
        # 写入 events.db
        db_path = tmp_path / ".workflow-controller" / "events.db"
        store = CheckpointStore(db_path)
        for cp in cps:
            store.store_sync(cp)
        # 读回校验
        read_back = store.list_sync("ops_sess_005")
        assert len(read_back) == 3
        # 顺序应按 created_at（store_sync 写入时序）
        assert read_back[0].trigger == "subagent_start"
        assert read_back[1].trigger == "subagent_start"
        assert read_back[2].trigger == "session_complete"
        # main_part_id 预填正确
        assert read_back[0].main_part_id == "prt_1"
        assert read_back[1].main_part_id == "prt_2"
        assert read_back[2].main_part_id is None

    @staticmethod
    def test_timeline_file_states_misalign_warns(caplog):
        """timeline 边界数 ≠ file_states subagent_start 数时记 warning + 以 file_states 对齐。"""
        tl = [_make_task_entry(1, "ses_sub_001")]  # 1 个边界
        file_states = [
            _make_file_state(1, "subagent_start", "ses_sub_001"),
            _make_file_state(2, "subagent_start", "ses_sub_002"),  # file_states 多一个
            _make_file_state(3, "session_complete", ""),
        ]
        logger = logging.getLogger(
            "workflow_controller.plugins.agent_traceback.session_reconstructor.checkpoint_deriver"
        )
        logger.addHandler(caplog.handler)
        try:
            # 应记 warning 且按 file_states 对齐——产出 3 个检查点
            cps = CheckpointDeriver().derive(
                tl,
                file_states,
                {},
                session_id="ops",
                native_session_id="ses_main006",
            )
            assert len(cps) == 3
            # 第二个检查点的 subagent_session_id 来自 file_state（timeline 边界缺）
            assert cps[1].agent_id == "ses_sub_002"
            # 应有 warning 日志
            assert any("timeline main task boundaries" in r.message for r in caplog.records)
        finally:
            logger.removeHandler(caplog.handler)
