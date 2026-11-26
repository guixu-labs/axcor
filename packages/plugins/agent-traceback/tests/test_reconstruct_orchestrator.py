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

"""``orchestrator.py`` 集成测试 — 从 cannbot db 重建 Workflow Controller session。

Hermetic：构造一个最小 cannbot db（5 表）+ mock Orchestrator，跑
ReconstructOrchestrator.import_session，校验：
  - 产出 target_cwd 含 .git（项目仓）+ .workflow-controller/opencode/opencode.db + .workflow-controller/snapshots/.git
  - events.db 的 checkpoints 表写入检查点
  - 返回的 dict 含正确字段

不依赖真实 Orchestrator——用 FakeOrch 测编排骨架。
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from workflow_controller.plugins.agent_traceback.checkpoint_store import CheckpointStore
from workflow_controller.plugins.agent_traceback.session_reconstructor.db_reader import (
    TimelineEntry,
    _iso_to_ms,
)
from workflow_controller.plugins.agent_traceback.session_reconstructor.orchestrator import (
    ReconstructOrchestrator,
    _infer_origin_cwd,
)

from ._fixtures import (
    build_cannbot_db,
    build_source_opencode_db,
    cannbot_bridge_row,
    cannbot_task_call_row,
    cannbot_tool_call_row,
    cannbot_turn_row,
    tool_timeline_entry,
    turn_timeline_entry,
)


def _has_git() -> bool:
    return shutil.which("git") is not None


pytestmark = pytest.mark.skipif(not _has_git(), reason="git not installed")


# ── fake objects（避开真实 Orchestrator 的复杂依赖） ─────────────


class FakePool:
    """模拟 SessionPool：create() 返回一个最小 session 对象。"""

    def __init__(self) -> None:
        self.created: list[Any] = []

    async def create(self, config: Any) -> Any:
        from workflow_controller.session.session import Session

        sess = Session(name=config.name, role=config.role, config=config)
        sess.id = uuid4().hex  # 测试用随机 id
        self.created.append(sess)
        return sess

    async def _persist_session(self, session: Any) -> None:
        """与真实 SessionPool 对齐：重建路径在设置 metadata 后会调此方法持久化。"""
        pass


class FakeEventRouter:
    """模拟 EventStoreRouter：db_path_for() 返回守护进程库路径，事件经端口记录。"""

    def __init__(self, project_dir: Path) -> None:
        self.project_dir = project_dir
        self.stored_events: list[Any] = []

    async def store_many(self, events: list[Any]) -> None:
        self.stored_events.extend(events)

    def db_path_for(self, session_id: str) -> Path:
        return self.project_dir / ".workflow-controller" / "events.db"


class FakeOrch:
    """模拟 Orchestrator，仅暴露 import_session 用到的属性。"""

    def __init__(self, project_dir: Path) -> None:
        from workflow_controller.config.settings import AppConfig

        self.config = AppConfig(project_dir=str(project_dir))
        self.pool = FakePool()
        self.event_router = FakeEventRouter(project_dir)


class FakePlugin:
    """模拟 AgentTracebackPlugin，仅暴露能力端口 + _cfg + config.extra。"""

    def __init__(self, orch: FakeOrch) -> None:
        from workflow_controller.plugin.base import PluginConfig
        from workflow_controller.plugin.context import PluginServices

        self._services = PluginServices(
            session_creator=orch.pool.create,
            session_persister=getattr(orch.pool, "_persist_session"),
            event_storer=orch.event_router.store_many,
            events_db_resolver=orch.event_router.db_path_for,
        )
        # 用真实 PluginConfig 让 _reconstruct_cfg 能读到嵌套 reconstruct 段
        self.config = PluginConfig(name="agent_traceback", version="0.1.0", enabled=True)
        # 顶层 extra 配置（如 ignore）—— _cfg 直接读
        self._cfg_values: dict[str, Any] = {}
        # reconstruct 段（嵌套）—— _reconstruct_cfg 读
        self._reconstruct_values: dict[str, Any] = {}

    def _cfg(self, key: str, default: Any = None) -> Any:
        # 先从顶层 _cfg_values 读，再回退到 config.extra
        if key in self._cfg_values:
            return self._cfg_values[key]
        extra = self.config.extra or {}
        return extra.get(key, default)

    def _refresh_extra(self) -> None:
        """把 _cfg_values + _reconstruct_values 合并到 config.extra（让 _cfg 也能读 reconstruct）。"""
        extra: dict[str, Any] = dict(self._cfg_values)
        if self._reconstruct_values:
            extra["reconstruct"] = dict(self._reconstruct_values)
        self.config.extra = extra


# ── cannbot db 构造（复用 _fixtures 的行字典构造器） ─────────────────


_ORCH_SESSION_DEFAULTS = {
    "id": "cmr_test",
    "taskId": "ses_main_001",
    "query": "do X",
    "framework": "opencode",
    "model": "m",
    "directory": "/workspace/orig",
    "sourcePath": "",
    "rootExecutionId": "exec_root",
    "createdAt": "2026-07-16T12:00:00.000Z",
}


def _make_cannbot_db(
    tmp_path: Path,
    *,
    session_row: dict | None = None,
    turns: list[dict] | None = None,
    tool_calls: list[dict] | None = None,
    bridges: list[dict] | None = None,
) -> Path:
    """建一个 cannbot-style SQLite db（本文件统一 Session 默认行 + 默认 sessionId）。"""
    return build_cannbot_db(
        tmp_path,
        session_row=session_row,
        turns=turns,
        tool_calls=tool_calls,
        bridges=bridges,
        session_defaults=_ORCH_SESSION_DEFAULTS,
        default_session_id="cmr_test",
    )


def _make_source_opencode_db(tmp_path: Path) -> Path:
    """建一个最小 source opencode.db（schema 模板，含完整 project 表）。"""
    return build_source_opencode_db(
        tmp_path,
        migrations_pk=False,
        migrations_hash="fake",
        full_project_table=True,
    )


def _make_minimal_cannbot_db(
    tmp_path: Path,
    *,
    framework: str = "opencode",
) -> Path:
    """建一个最小可重建的 cannbot db：1 user + 1 assistant(task) + 子 write。"""
    turns = [
        cannbot_turn_row("t1", content="do X", at="2026-07-16T12:00:00.000Z"),
        cannbot_turn_row(
            "t2",
            idx=1,
            role="assistant",
            finish="tool-calls",
            content="<thinking>ok</thinking>Dispatching.",
            at="2026-07-16T12:00:01.000Z",
        ),
        # 子 agent turn
        cannbot_turn_row(
            "t3",
            content="sub prompt",
            sub_sid="ses_sub_001",
            at="2026-07-16T12:00:02.000Z",
        ),
        cannbot_turn_row(
            "t4",
            idx=1,
            role="assistant",
            finish="tool-calls",
            content="writing",
            sub_sid="ses_sub_001",
            at="2026-07-16T12:00:03.000Z",
        ),
    ]
    tool_calls = [
        _main_task_dispatch("task_id: ses_sub_001\n<task_result>ok</task_result>"),
        _sub_write_call(),
    ]
    bridges = [cannbot_bridge_row("call_main_task_1", "ses_sub_001", "developer")]
    return _make_cannbot_db(
        tmp_path,
        turns=turns,
        tool_calls=tool_calls,
        bridges=bridges,
        session_row={"framework": framework},
    )


def _main_task_dispatch(result: str) -> dict:
    """主 task 派发 ToolCall 行（tc1/t2/call_main_task_1/ses_sub_001，时间窗固定）。"""
    return cannbot_task_call_row(
        "tc1",
        "t2",
        "call_main_task_1",
        "ses_sub_001",
        result=result,
        start="2026-07-16T12:00:01.500Z",
        end="2026-07-16T12:00:10.000Z",
    )


def _sub_write_call() -> dict:
    """子 agent 的 write ToolCall 行（tc2/t4/call_sub_write_1，时间窗固定）。"""
    return cannbot_tool_call_row(
        "tc2",
        "t4",
        "call_sub_write_1",
        "write",
        {"filePath": "/workspace/orig/sub/a.txt", "content": "hello"},
        start="2026-07-16T12:00:03.500Z",
        end="2026-07-16T12:00:03.600Z",
    )


def _orch_and_plugin(project_dir: Path, source_db: Path) -> tuple[FakeOrch, FakePlugin]:
    """标准重建装配：FakeOrch + FakePlugin + source db / git_hash 配置。"""
    orch = FakeOrch(project_dir=project_dir)
    plugin = FakePlugin(orch)
    getattr(plugin, "_reconstruct_values")["source_opencode_db"] = str(source_db)
    getattr(plugin, "_reconstruct_values")["git_hash_fidelity"] = False
    getattr(plugin, "_refresh_extra")()
    return orch, plugin


def _main_dispatch_bridge() -> dict:
    """main → Designer 的标准 bridge（timeline 内有对应派发 tool call tc1）。"""
    return cannbot_bridge_row(
        "tc1",
        "sub-1",
        "designer",
        sub_name="Designer",
        tokens=100,
        at_dispatch="2026-10-28T13:00:01.500Z",
        at_response="2026-10-28T15:26:38.108Z",
    )


def _import_inputs(tmp_path: Path, *, framework: str = "opencode") -> tuple[Path, Path, Path]:
    """标准导入输入：最小 cannbot db + source opencode.db + target 目录。"""
    cannbot_db = _make_minimal_cannbot_db(tmp_path, framework=framework)
    source_db = _make_source_opencode_db(tmp_path)
    target_cwd = tmp_path / "target"
    target_cwd.mkdir()
    return cannbot_db, source_db, target_cwd


def _run_import(
    project_dir: Path, source_db: Path, cannbot_db: Path, target_cwd: Path
) -> tuple[FakeOrch, dict]:
    """标准导入路径：装配 FakeOrch/插件并执行 import_session。"""
    orch, plugin = _orch_and_plugin(project_dir, source_db)
    runner = ReconstructOrchestrator(plugin)
    result = asyncio.run(runner.import_session(cannbot_db, target_cwd))
    return orch, result


# ── 集成测试 ───────────────────────────────────────────────────────


class TestImportSession:
    @staticmethod
    def test_import_derives_loop_from_checkpoints(tmp_path: Path):
        """无 workflow-controller.yaml 时，loop 从 checkpoints 推导，永不为 None。"""
        cannbot_db, source_db, target_cwd = _import_inputs(tmp_path)
        orch, result = _run_import(tmp_path, source_db, cannbot_db, target_cwd)
        assert result["ok"]

        # 单个 task 检查点：subagent_start 之后 session_complete；loop 上限
        # 在检查点推导（取 2）与项目配置（取 10）之间取较大者，得 10
        sess = orch.pool.created[0]
        assert sess.config.loop is not None
        assert sess.config.loop.max_iterations == 10

    @staticmethod
    def test_import_yaml_loop_overrides_checkpoints(tmp_path: Path):
        """workflow-controller.yaml 配了 loop.max_iterations 时，yaml 优先于 checkpoints 推导。"""
        cannbot_db, source_db, target_cwd = _import_inputs(tmp_path)
        # 写 workflow-controller.yaml，配 max_iterations=50（大于 checkpoints 推导的 10）
        (target_cwd / "workflow-controller.yaml").write_text(
            "loop:\n  max_iterations: 50\n",
            encoding="utf-8",
        )
        orch, result = _run_import(tmp_path, source_db, cannbot_db, target_cwd)
        assert result["ok"]

        sess = orch.pool.created[0]
        assert sess.config.loop is not None
        assert sess.config.loop.max_iterations == 50

    @staticmethod
    def test_invalid_db_returns_error(tmp_path: Path):
        """非 cannbot db → 返回 ok=False + error。"""
        # 建一个缺表的 db
        bad_db = tmp_path / "bad.db"
        con = sqlite3.connect(bad_db)
        con.execute("CREATE TABLE foo (id TEXT)")
        con.commit()
        con.close()

        orch = FakeOrch(project_dir=tmp_path)
        plugin = FakePlugin(orch)
        runner = ReconstructOrchestrator(plugin)
        result = asyncio.run(runner.import_session(bad_db, tmp_path / "target"))
        assert result["ok"] is False
        assert "not a cannbot session db" in result["error"]

    @staticmethod
    def test_unsupported_framework_returns_error(tmp_path: Path):
        """framework 不在支持集合 → 返回 ok=False，错误信息列出支持集合。"""
        db = _make_cannbot_db(
            tmp_path,
            session_row={"framework": "claude_sdk"},
        )
        orch = FakeOrch(project_dir=tmp_path)
        plugin = FakePlugin(orch)
        runner = ReconstructOrchestrator(plugin)
        result = asyncio.run(runner.import_session(db, tmp_path / "target"))
        assert result["ok"] is False
        assert "unsupported framework" in result["error"]
        assert "claude-code-proxy" in result["error"]

    @staticmethod
    def test_claude_code_proxy_framework_imports(tmp_path: Path):
        """framework=claude-code-proxy（经方言归一化）可导入。"""
        cannbot_db, source_db, target_cwd = _import_inputs(tmp_path, framework="claude-code-proxy")
        _orch, result = _run_import(tmp_path, source_db, cannbot_db, target_cwd)
        assert result["ok"], f"import failed: {result.get('error')}"
        assert result["native_session_id"] == "ses_main_001"

    @staticmethod
    def test_reconstructed_session_id_logged_in_metadata(tmp_path: Path):
        """session.plugin_metadata 含 birth_traceback.reconstructed_from（§4.5 第 4 步）。"""
        cannbot_db, source_db, target_cwd = _import_inputs(tmp_path)
        orch, result = _run_import(tmp_path, source_db, cannbot_db, target_cwd)
        assert result["ok"]

        # 检查 session.plugin_metadata
        sess = orch.pool.created[0]
        bt = sess.plugin_metadata.get("agent_traceback", {}).get("birth_traceback", {})
        assert bt.get("reconstructed_from") == str(cannbot_db)
        assert bt.get("origin_native_session_id") == "ses_main_001"
        assert bt.get("fork_cwd") == str(target_cwd.resolve())
        assert bt.get("checkpoints") == 2
        # session.native_session_id 设为原 sdk id（让 TracebackRunner 能查 opencode.db）
        assert sess.native_session_id == "ses_main_001"
        # loop_status="yes"（已跑完态，不进运行队列）
        assert sess.loop_status == "yes"

    @staticmethod
    def test_birth_traceback_records_created_git_dir(tmp_path: Path):
        """created_git_dir 记录 .git 是否由重建产生。

        最小 cannbot db（含 write，无 git init）→ 重建会自建空 .git → created_git_dir=True。
        删除清理据此判定 .git 可清；旧 session 无此字段 → 保守不清。
        """
        cannbot_db, source_db, target_cwd = _import_inputs(tmp_path)
        orch, result = _run_import(tmp_path, source_db, cannbot_db, target_cwd)
        assert result["ok"]

        sess = orch.pool.created[0]
        bt = sess.plugin_metadata.get("agent_traceback", {}).get("birth_traceback", {})
        # created_git_dir 必须存在且为 bool
        assert "created_git_dir" in bt
        assert isinstance(bt["created_git_dir"], bool)
        assert isinstance(bt["created_opencode_dir"], bool)
        # 重建自建了空 .git → True
        assert bt["created_git_dir"] is True
        # 当前最小重建只创建 .workflow-controller/opencode，不创建项目级 .opencode。
        assert bt["created_opencode_dir"] is False
        assert (target_cwd.resolve() / ".git").is_dir()

    @staticmethod
    def test_birth_traceback_created_git_dir_false_when_user_git_preexisted(tmp_path: Path):
        """target_cwd 已有用户 .git → created_git_dir=False（重建不应声称「我建的」）。"""
        cannbot_db, source_db, target_cwd = _import_inputs(tmp_path)
        # 用户预置 .git（重建前已存在）
        import subprocess as _sp

        _sp.run(["git", "init"], cwd=str(target_cwd), capture_output=True)
        (target_cwd / ".opencode").mkdir()
        (target_cwd / ".opencode" / "user-config.json").write_text("{}")
        orch, result = _run_import(tmp_path, source_db, cannbot_db, target_cwd)
        assert result["ok"]

        sess = orch.pool.created[0]
        bt = sess.plugin_metadata.get("agent_traceback", {}).get("birth_traceback", {})
        # 用户已有 .git → 重建不应声称自己建的 → False（删除时不清 .git）
        assert bt["created_git_dir"] is False
        assert bt["created_opencode_dir"] is False

    @staticmethod
    def test_checkpoints_written_to_daemon_db_not_target_cwd(tmp_path: Path):
        """target_cwd ≠ 启动目录时，检查点须落守护进程库（读路径同源）。"""
        cannbot_db, source_db, target_cwd = _import_inputs(tmp_path)
        daemon_dir = tmp_path / "daemon"
        daemon_dir.mkdir()
        orch, result = _run_import(daemon_dir, source_db, cannbot_db, target_cwd)
        assert result["ok"]

        sess = orch.pool.created[0]
        listed = CheckpointStore(orch.event_router.db_path_for(sess.id)).list_sync(sess.id)
        assert len(listed) == 2
        assert not (target_cwd / ".workflow-controller" / "events.db").exists()


# ── _synthesize_historical_events（bridge 驱动） ─────────────────────


class TestSynthesizeHistoricalEvents:
    """合成 SUBAGENT/STEP_FINISH 事件：bridge 字段优先，timeline 回退。"""

    @staticmethod
    def _timeline_main_only() -> list[TimelineEntry]:
        """最小主 agent 时间线：2 turns + 1 task 派发（无子 agent interleave）。

        主 agent 的 task 条目携带**被派发子会话** id（db_reader 的检查点
        链路语义——`_emit_tool_call("main", tc, sub_sid, None)`），与子
        agent 条目（携带所属会话 = 派发者）语义相反，fixture 须如实还原。
        """
        return [
            turn_timeline_entry(
                _iso_to_ms("2026-10-28T13:00:00.000Z"),
                "t1",
                content="do X",
                at="2026-10-28T13:00:00.000Z",
            ),
            turn_timeline_entry(
                _iso_to_ms("2026-10-28T13:00:01.000Z"),
                "t2",
                idx=1,
                role="assistant",
                content="dispatch",
                at="2026-10-28T13:00:01.000Z",
            ),
            # cpx 方言：completedAt 被钉到 query 结束（不可靠，bridge 优先）；
            # 主派发条目携带被派发子会话 id（bridge 解析结果），
            # 不是派发者——parent 解析须按 role 区分
            tool_timeline_entry(
                _iso_to_ms("2026-10-28T13:00:01.500Z"),
                "tc1",
                "t2",
                "toolu_01",
                "task",
                {"subagent_type": "designer"},
                start="2026-10-28T13:00:01.500Z",
                end="2026-10-29T01:02:36.935Z",
                entry_sub_sid="sub-1",
            ),
        ]

    @staticmethod
    def _run(timeline: list[TimelineEntry], bridges: list[dict]) -> FakeEventRouter:
        orch = FakeOrch(project_dir=Path("/tmp/synth-test"))
        runner = ReconstructOrchestrator(FakePlugin(orch))
        asyncio.run(
            getattr(runner, "_synthesize_historical_events")(
                session_id="s_synth",
                timeline=timeline,
                bridges=bridges,
            )
        )
        return orch.event_router

    @staticmethod
    def _timeline_null_ts_dispatch() -> list[TimelineEntry]:
        """派发 ToolCall 两列时间戳均 NULL 的旧 db 形态（args 携带子会话 id）。"""
        tl = getattr(TestSynthesizeHistoricalEvents, "_timeline_main_only")()
        tc_entry = next(e for e in tl if e.kind == "tool_call")
        tc_entry.tool_call.update(
            {
                "argsJson": json.dumps(
                    {
                        "subagent_type": "designer",
                        "subagent_session_id": "sub-9",
                    }
                ),
                "startedAt": None,
                "completedAt": None,
            }
        )
        tl.append(
            turn_timeline_entry(
                _iso_to_ms("2026-10-28T13:00:04.000Z"),
                "t9",
                content="sub",
                sub_sid="sub-9",
                at="2026-10-28T13:00:04.000Z",
                parent_call="toolu_01",
            )
        )
        return tl

    @staticmethod
    def _timeline_nested_dispatch() -> list[TimelineEntry]:
        """嵌套派发时间线：main 派 Designer（tc1），Designer 派 Explore（tc2）。"""
        tl = getattr(TestSynthesizeHistoricalEvents, "_timeline_main_only")()
        # Designer 的 turn + 派发 tool call（timeline 条目的 subagent_session_id
        # = 派发者，孙 agent 层级真相）
        tl.append(
            turn_timeline_entry(
                _iso_to_ms("2026-10-28T13:02:00.000Z"),
                "t_sub1",
                idx=0,
                role="assistant",
                content="explore",
                sub_sid="sub-1",
                at="2026-10-28T13:02:00.000Z",
                parent_call="toolu_01",
            )
        )
        tl.append(
            tool_timeline_entry(
                _iso_to_ms("2026-10-28T13:02:01.000Z"),
                "tc2",
                "t_sub1",
                "toolu_02",
                "task",
                {"subagent_type": "Explore"},
                start="2026-10-28T13:02:01.000Z",
                end="2026-10-28T14:08:39.845Z",
                entry_role="subagent",
                entry_sub_sid="sub-1",
                parent_call="toolu_01",
            )
        )
        return tl

    def test_bridge_fields_drive_events(self):
        """时间戳/token/name/type 取 bridge 字段，不信任 ToolCall 的完成时刻。"""
        bridges = [
            cannbot_bridge_row(
                "tc1",
                "sub-1",
                "designer",
                sub_name="Designer: X",
                tokens=63034753,
                at_dispatch="2026-10-28T13:00:01.500Z",
                at_response="2026-10-28T15:26:38.108Z",
            )
        ]
        router = self._run(self._timeline_main_only(), bridges)
        events = router.stored_events
        starts = [e for e in events if e.event_type.value == "subagent_start"]
        stops = [e for e in events if e.event_type.value == "subagent_stop"]
        assert len(starts) == 1 and len(stops) == 1
        assert starts[0].data["agent_id"] == "sub-1"
        assert starts[0].data["agent_name"] == "Designer: X"
        assert stops[0].data["total_tokens"] == 63034753
        # stop 时间戳 = bridge.responseTimestamp，而非 ToolCall.completedAt
        assert stops[0].timestamp == datetime(2026, 10, 28, 15, 26, 38, 108000, tzinfo=UTC)
        assert starts[0].timestamp <= stops[0].timestamp

    def test_bridge_without_response_timestamp_start_only(self):
        """子代理未返回（无 responseTimestamp）→ 只发 start，stop 由前端归档。"""
        bridges = [
            cannbot_bridge_row(
                "tc1",
                "sub-1",
                "designer",
                sub_name="Designer: X",
                status="dispatched",
                at_dispatch="2026-10-28T13:00:01.500Z",
            )
        ]
        router = self._run(self._timeline_main_only(), bridges)
        types = [e.event_type.value for e in router.stored_events]
        assert types.count("subagent_start") == 1
        assert types.count("subagent_stop") == 0

    def test_bridge_only_nesting_covered_without_timeline_dispatch(self):
        """嵌套派发（派发 tool call 不在主时间线里）也能从 bridge 合成。"""
        bridges = [
            # 主派 Designer（timeline 里有对应 tool call）
            _main_dispatch_bridge(),
            # Designer 派 Explore（timeline 主时间线里无对应 tool call）
            cannbot_bridge_row(
                "tc_missing",
                "sub-2",
                "Explore",
                sub_name="Explore Y",
                tokens=4921254,
                at_dispatch="2026-10-28T13:05:00.000Z",
                at_response="2026-10-28T14:08:39.845Z",
            ),
        ]
        router = self._run(self._timeline_main_only(), bridges)
        events = router.stored_events
        start_sids = {e.data["agent_id"] for e in events if e.event_type.value == "subagent_start"}
        assert start_sids == {"sub-1", "sub-2"}
        stop2 = next(
            e
            for e in events
            if e.event_type.value == "subagent_stop" and e.data["agent_id"] == "sub-2"
        )
        assert stop2.data["total_tokens"] == 4921254
        assert stop2.data["agent_name"] == "Explore Y"

    def test_no_bridges_falls_back_to_timeline(self):
        """bridge 表缺失/为空 → 退化为纯 timeline 驱动（tc.startedAt/completedAt）。"""
        router = self._run(self._timeline_main_only(), [])
        events = router.stored_events
        starts = [e for e in events if e.event_type.value == "subagent_start"]
        stops = [e for e in events if e.event_type.value == "subagent_stop"]
        # 派发 tool call 的 argsJson 无 subagent_session_id、bridge 为空 → 无 SUBAGENT 事件
        assert not starts and not stops
        # 但 STEP_FINISH 仍按主 turn 数合成
        sf = [e for e in events if e.event_type.value == "step_finish"]
        assert len(sf) == 1 and sf[0].data["num_turns"] == 2

    def test_null_dispatch_timestamps_fall_back_to_sub_turn_ts(self):
        """派发行时间戳全 NULL → start 退子 agent 首条 entry 的 ts，节点不丢失。"""
        router = self._run(self._timeline_null_ts_dispatch(), [])
        events = router.stored_events
        starts = [e for e in events if e.event_type.value == "subagent_start"]
        stops = [e for e in events if e.event_type.value == "subagent_stop"]
        assert len(starts) == 1 and len(stops) == 1
        assert starts[0].data["agent_id"] == "sub-9"
        # start = 子 agent 首条 timeline entry 的 ts（子 turn createdAt）
        assert starts[0].timestamp == datetime(2026, 10, 28, 13, 0, 4, tzinfo=UTC)
        # completedAt 也 NULL → stop 回退 start_ms，不残留 running 节点
        assert stops[0].timestamp == starts[0].timestamp

    def test_no_bridge_stop_falls_back_to_start_ms(self):
        """无 bridge 且 completedAt NULL：stop 回退 start_ms（start+stop 成对）。"""
        tl = self._timeline_null_ts_dispatch()
        tc_entry = next(e for e in tl if e.kind == "tool_call")
        tc_entry.tool_call["startedAt"] = "2026-10-28T13:00:01.500Z"
        router = self._run(tl, [])
        events = router.stored_events
        starts = [e for e in events if e.event_type.value == "subagent_start"]
        stops = [e for e in events if e.event_type.value == "subagent_stop"]
        assert len(starts) == 1 and len(stops) == 1
        assert starts[0].timestamp == datetime(2026, 10, 28, 13, 0, 1, 500000, tzinfo=UTC)
        assert stops[0].timestamp == starts[0].timestamp

    def test_parent_agent_id_from_timeline_dispatch(self):
        """孙 agent start 事件携带 parent_agent_id=派发者；主派发为空串。"""
        bridges = [
            # main → Designer
            _main_dispatch_bridge(),
            # Designer → Explore（bridge 命名空间 = ToolCall.id）
            cannbot_bridge_row(
                "tc2",
                "sub-2",
                "Explore",
                sub_name="Explore Y",
                tokens=4921254,
                at_dispatch="2026-10-28T13:02:01.000Z",
                at_response="2026-10-28T13:58:20.150Z",
            ),
        ]
        router = self._run(self._timeline_nested_dispatch(), bridges)
        starts = {
            e.data["agent_id"]: e
            for e in router.stored_events
            if e.event_type.value == "subagent_start"
        }
        assert starts["sub-1"].data["parent_agent_id"] == ""
        assert starts["sub-2"].data["parent_agent_id"] == "sub-1"

    def test_parent_agent_id_bridge_only_via_dispatch_turn(self):
        """bridge-only 孙（派发 tool call 不在 timeline）：父级回退
        dispatchTurnId → Turn.subagentSessionId 索引。
        """
        bridges = [
            # main → Designer（timeline 有 tc1）
            _main_dispatch_bridge(),
            # Designer → Explore：派发 tool call 行缺失，只有 dispatchTurnId
            cannbot_bridge_row(
                "tc_missing",
                "sub-2",
                "Explore",
                sub_name="Explore Y",
                tokens=4921254,
                at_dispatch="2026-10-28T13:02:01.000Z",
                at_response="2026-10-28T13:58:20.150Z",
                dispatch_turn="t_sub1",
            ),
        ]
        # 嵌套时间线里 Designer 的 turn（id=t_sub1, subagentSessionId=sub-1）
        # 提供父级索引；但派发 tool call（tc2）不在 → bridge-only 场景
        tl = self._timeline_nested_dispatch()
        filtered_tl = []
        for e in tl:
            if e.kind == "tool_call" and e.tool_call is not None and e.tool_call.get("id") == "tc2":
                continue
            filtered_tl.append(e)
        tl = filtered_tl
        router = self._run(tl, bridges)
        starts = {
            e.data["agent_id"]: e
            for e in router.stored_events
            if e.event_type.value == "subagent_start"
        }
        assert starts["sub-2"].data["parent_agent_id"] == "sub-1"

    def test_parent_agent_id_defaults_empty_when_unresolvable(self):
        """两级父级来源均无：parent_agent_id 空串（前端挂 main）。"""
        bridges = [
            cannbot_bridge_row(
                "tc1",
                "sub-1",
                "designer",
                sub_name="Designer",
                tokens=100,
                at_dispatch="2026-10-28T13:00:01.500Z",
                at_response="2026-10-28T15:26:38.108Z",
            )
        ]
        router = self._run(self._timeline_main_only(), bridges)
        starts = [e for e in router.stored_events if e.event_type.value == "subagent_start"]
        assert starts[0].data["parent_agent_id"] == ""

    def test_step_finish_counts_main_turns_only(self):
        """num_turns 只数主 agent turn（system 噪声已在读取层丢弃）。"""
        sub_turn = turn_timeline_entry(
            _iso_to_ms("2026-10-28T13:00:04.000Z"),
            "t5",
            content="sub",
            sub_sid="sub-2",
            at="2026-10-28T13:00:04.000Z",
        )
        tl = [*self._timeline_main_only(), sub_turn]
        router = self._run(tl, [])
        sf = [e for e in router.stored_events if e.event_type.value == "step_finish"]
        assert len(sf) == 1 and sf[0].data["num_turns"] == 2


class TestInferOriginCwd:
    """Session.directory 为空时从文件工具目标路径推断项目根。"""

    @staticmethod
    def test_no_file_tools_returns_empty():
        tl = [
            TimelineEntry(
                ts=1,
                role="main",
                kind="turn",
                turn={"id": "t1", "role": "user", "content": "x"},
                tool_call=None,
                subagent_session_id="",
                parent_task_call_id=None,
            )
        ]
        assert _infer_origin_cwd(tl) == ""

    @staticmethod
    def _file_tool_entry(file_path: str) -> TimelineEntry:
        return TimelineEntry(
            ts=1,
            role="subagent",
            kind="tool_call",
            turn=None,
            tool_call={
                "id": "tc_w",
                "turnId": "t1",
                "toolCallId": "c_w",
                "toolName": "write",
                "argsJson": json.dumps({"file_path": file_path, "content": "x"}),
                "resultJson": "",
                "state": "completed",
                "startedAt": "2026-10-28T13:00:01.000Z",
            },
            subagent_session_id="sub-1",
            parent_task_call_id=None,
        )

    def test_common_parent_of_absolute_paths(self):
        tl = [
            self._file_tool_entry("/home/u/proj/vllm/models/qsa.py"),
            self._file_tool_entry("/home/u/proj/.day0/tracker.md"),
            self._file_tool_entry("/home/u/proj/vllm/worker/runner.py"),
        ]
        assert _infer_origin_cwd(tl) == "/home/u/proj"

    def test_snake_and_camel_arg_keys_both_read(self):
        e1 = self._file_tool_entry("/home/u/proj/src/a.py")
        e2 = self._file_tool_entry("/home/u/proj/docs/b.py")
        e2.tool_call["argsJson"] = json.dumps({"filePath": "/home/u/proj/docs/b.py"})
        assert _infer_origin_cwd([e1, e2]) == "/home/u/proj"

    def test_degenerate_common_root_returns_empty(self):
        tl = [
            self._file_tool_entry("/a/f.txt"),
            self._file_tool_entry("/b/f.txt"),
        ]
        assert _infer_origin_cwd(tl) == ""

    def test_single_file_returns_empty(self):
        """单文件写入时公共父目录就是该文件所在目录，无法与子目录区分。"""
        tl = [self._file_tool_entry("/home/u/proj/src/a.py")]
        assert _infer_origin_cwd(tl) == ""

    def test_same_subdir_only_returns_empty(self):
        """全部写入落在同一子目录时，公共父目录退化为该子目录而非项目根。"""
        tl = [
            self._file_tool_entry("/home/u/proj/src/a.py"),
            self._file_tool_entry("/home/u/proj/src/b.py"),
        ]
        assert _infer_origin_cwd(tl) == ""
