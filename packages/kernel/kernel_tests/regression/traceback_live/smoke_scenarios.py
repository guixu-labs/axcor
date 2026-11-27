# Copyright (c) 2026 Workflow Controller Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

"""冒烟场景：提示词、假模型脚本与断言，真实模型与离线用例共用同一份。

断言只盯结构事实（检查点行、作用域工作区、根检出、故障事件）与时序，
每个场景返回证据字典。真实模型用例与离线用例跑的是同一个函数，差别
只在谁来应答模型请求。
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path
from typing import Any

import pytest

from workflow_controller.core.context_reader import claude_main_transcript_path
from workflow_controller.session.events import EventType

from .live_plane import (
    MEMBER_TIMEOUT_S,
    RUN_TIMEOUT_S,
    HostHookTrace,
    LivePlane,
    assert_no_fault_events,
    assert_workspaces_released,
    live_plane,
    result_recorded_at,
    trace_host_hooks,
    transcript_rows,
    wait_member,
    wait_tool_use,
    write_workflow,
)
from .scripted_gateway import Answer, Script, ToolCall

# 提示词里的独特片段：假模型据此认出对话。捆绑 CLI 会把提示词里的引号、
# 尖括号和 & 转义后再发给模型，标记里不能带这些字符
_EXECUTOR_WRITE = "mkdir -p out"
_VERIFIER_READ = "cat out/a.txt"
_GENERATOR_WRITE = "创建 sub.yaml"
_WORKER_SLEEP = "time.sleep(30)"
_SUBAGENT_TASK = "SUBAGENT-WRITER"
_SUBAGENT_PARENT = "派一个子代理"
# 强杀前多等一会儿，确保落在长眠期间
KILL_SETTLE_S = 3.0
# 收尾入口注入的等待：超过捆绑 CLI 在查询被中断后的收尾等待（5 秒）
SLOW_SETTLE_S = 8.0

_SUBGRAPH_YAML = (
    "workflow: sub\n"
    "max_parallel: 1\n"
    "nodes:\n"
    "  - id: s1\n"
    "    task_type: bash\n"
    "    command: echo sub-ok > sub-out.txt\n"
    "    acceptance:\n"
    "      enabled: false\n"
)


def _write_then_done(write: ToolCall) -> Script:
    """先执行一次写工具，再回复 DONE。"""
    return lambda text, results: write if not results else Answer("DONE")


def _subagent_script(text: str, results: list[str]) -> ToolCall | Answer:
    """父代理先派子代理、等它返回后读文件；子代理在自己的当前目录写文件。

    父代理与子代理的任务文本里都有子代理标记，父代理的提示里另有「派一个子代理」。
    """
    if _SUBAGENT_PARENT not in text:
        return _write_then_done(
            ToolCall("Bash", {"command": "printf 'from-child\\n' > from-child.txt"})
        )(text, results)
    if not results:
        return ToolCall(
            "Task",
            {
                "description": "writer",
                "prompt": f"{_SUBAGENT_TASK}：在你自己的当前工作目录下用 Bash 执行 "
                "printf 'from-child\\n' > from-child.txt，成功后回复 done。",
                "subagent_type": "general-purpose",
            },
        )
    if len(results) == 1:
        return ToolCall("Bash", {"command": "cat from-child.txt"})
    return Answer(f"读取结果：\n{results[-1]}")


# 假模型的脚本表：标记 → 该对话的应答
SCRIPTS: dict[str, Script] = {
    _EXECUTOR_WRITE: _write_then_done(
        ToolCall("Bash", {"command": "mkdir -p out && printf 'smoke-a\\n' > out/a.txt"})
    ),
    _VERIFIER_READ: lambda text, results: (
        ToolCall("Bash", {"command": "cat out/a.txt"}) if not results else Answer("内容正确\nPASS")
    ),
    _GENERATOR_WRITE: _write_then_done(
        ToolCall("Write", {"file_path": "sub.yaml", "content": _SUBGRAPH_YAML})
    ),
    _WORKER_SLEEP: lambda text, results: [
        ToolCall("Bash", {"command": "printf 'first\\n' > first.txt"}),
        # 捆绑 CLI 对独立的 sleep 做短路、对链式的直接拦截，长眠用 Python
        ToolCall("Bash", {"command": 'python3 -c "import time; time.sleep(30)"'}),
        ToolCall("Bash", {"command": "printf 'second\\n' > second.txt"}),
        Answer("DONE"),
    ][min(len(results), 3)],
    _SUBAGENT_TASK: _subagent_script,
}

# ── 工作流节点 ──────────────────────────────────────────────────────────

EXECUTOR_WITH_AGENT_ACCEPTANCE = {
    "id": "writer",
    "task_type": "agent",
    "prompt": (
        "用 Bash 工具执行：mkdir -p out && printf 'smoke-a\\n' > out/a.txt。"
        "成功后回复 DONE。不要做别的事，不要提问。"
    ),
    "acceptance": {
        "enabled": True,
        "type": "agent",
        "verifier_prompt": (
            "用 Bash 工具执行 cat out/a.txt（不要用 Read 工具）。"
            "文件存在且内容是 smoke-a 就通过。最后一行只写 PASS 或 FAILED。"
        ),
    },
}

GENERATOR_NODE = {
    "id": "gen",
    "task_type": "generator",
    "generator_type": "agent",
    "subgraph": {"output": "sub.yaml"},
    "prompt": (
        "用 Write 工具在当前工作目录创建 sub.yaml，内容逐字如下，不要改动：\n\n"
        f"{_SUBGRAPH_YAML}\n"
        "写完后回复 DONE。不要执行子图里的节点，不要提问。"
    ),
    "acceptance": {"enabled": False},
}

WORKER_WITH_SLEEP_NODE = {
    "id": "worker",
    "task_type": "agent",
    "prompt": (
        "依次用 Bash 工具执行下面三条命令，每条单独一次调用，顺序不变，不要合并：\n"
        "1. printf 'first\\n' > first.txt\n"
        '2. python3 -c "import time; time.sleep(30)"\n'
        "3. printf 'second\\n' > second.txt\n"
        "全部完成后回复 DONE。不要提问。"
    ),
    "acceptance": {
        "enabled": True,
        "type": "script",
        "script": 'test "$(cat first.txt)" = first && test "$(cat second.txt)" = second',
    },
}

SUBAGENT_DISPATCH_NODE = {
    "id": "orchestrator",
    "task_type": "agent",
    "prompt": (
        "严格按两步做，每步只调用一次工具：\n"
        "第 1 步：用 Task 工具派一个子代理，subagent_type 填 general-purpose，"
        "prompt 参数原样填下面这一行：\n"
        f"{_SUBAGENT_TASK}：在你自己的当前工作目录下用 Bash 执行 "
        "printf 'from-child\\n' > from-child.txt，成功后回复 done。\n"
        "不要传 cwd 参数，也不要自己执行这条命令。\n"
        "第 2 步：等子代理返回后，用 Bash 执行 cat from-child.txt，"
        "把它的输出原样放进你的最终回复。不要提问。"
    ),
    "acceptance": {"enabled": False},
}


# ── 取数 ────────────────────────────────────────────────────────────────


def scope_rows(rows: list[dict[str, Any]], node_id: str) -> list[dict[str, Any]]:
    """节点作用域（键形如「节点 id-w序号」，不含其下子代理作用域）的检查点行。"""
    pattern = re.compile(rf"{re.escape(node_id)}-w\d+")
    return [row for row in rows if pattern.fullmatch(row["scope_id"])]


async def tool_result_of(
    plane: LivePlane, session_id: str, tool_name: str, command_fragment: str = ""
) -> dict[str, Any]:
    """成员会话里指定工具那次调用的结果块（可按命令片段挑选），不存在时断言失败。

    子代理的工具调用也记在父会话名下，要挑父代理自己的调用时按命令片段区分。
    """
    events = await plane.session_events(session_id)
    tool_use_id = next(
        event.data["id"]
        for event in events
        if event.event_type is EventType.TOOL_USE
        and event.data.get("name") == tool_name
        and command_fragment in str((event.data.get("input") or {}).get("command") or "")
    )
    for event in events:
        if event.event_type is not EventType.USER_MESSAGE:
            continue
        for block in event.data.get("content") or []:
            if block.get("tool_use_id") == tool_use_id:
                return dict(block)
    pytest.fail(f"会话 {session_id} 里没有 {tool_name} 的工具结果")


# ── 场景 ────────────────────────────────────────────────────────────────


async def executor_with_agent_acceptance(
    plane: LivePlane, wait_s: float = RUN_TIMEOUT_S
) -> dict[str, Any]:
    """agent 执行器用 Bash 写文件、agent 验收用 Bash 读它：两侧屏障放行，产出经集成进根检出。"""
    started = time.monotonic()
    yaml_path = write_workflow(plane.project_dir / "smoke-a.yaml", [EXECUTOR_WITH_AGENT_ACCEPTANCE])
    run_id = await plane.start_run(yaml_path, "smoke-a")
    run = await plane.wait_run_finished(run_id, wait_s)

    assert run.status == "completed", await plane.describe(run_id)
    nodes = await plane.node_states(run_id)
    assert nodes["writer"]["status"] == "passed", await plane.describe(run_id)
    assert (plane.work_dir / "out" / "a.txt").read_text(encoding="utf-8") == "smoke-a\n"
    await assert_no_fault_events(plane, run_id)

    rows = await plane.checkpoints(run_id)
    node_rows = scope_rows(rows, "writer")
    assert {row["trigger"] for row in node_rows} >= {"scope_start", "terminal"}
    # 执行会话与验收会话都拿到了原生会话标识：init 帧的标识被驱动捕获并入账
    executor = plane.member(run_id, "node=writer")
    verifier = plane.member(run_id, "node=writer/verify")
    assert executor["native_session_id"] and verifier["native_session_id"], (executor, verifier)
    # 两侧的副作用屏障都放行，各落下锚定到自己会话的组检查点行
    group_anchors = {
        row["dispatch_anchor"]["parent_native_session_id"]
        for row in node_rows
        if row["trigger"] == "before_child"
    }
    assert group_anchors == {executor["native_session_id"], verifier["native_session_id"]}
    assert_workspaces_released(plane.repository(run_id), node_rows[0]["scope_id"])
    return {"scenario": "A", "elapsed_s": round(time.monotonic() - started, 1)}


async def generator_writes_subgraph(
    plane: LivePlane, wait_s: float = RUN_TIMEOUT_S
) -> dict[str, Any]:
    """agent 型 generator 用 Write 写出子图 yaml，子图节点被执行，整图完成。"""
    started = time.monotonic()
    yaml_path = write_workflow(plane.project_dir / "smoke-c.yaml", [GENERATOR_NODE])
    run_id = await plane.start_run(yaml_path, "smoke-c")
    run = await plane.wait_run_finished(run_id, wait_s)

    assert run.status == "completed", await plane.describe(run_id)
    nodes = await plane.node_states(run_id)
    assert nodes["gen"]["status"] == "passed", await plane.describe(run_id)
    assert nodes["gen:s1"]["status"] == "passed", await plane.describe(run_id)
    assert (plane.work_dir / "sub-out.txt").read_text(encoding="utf-8") == "sub-ok\n"
    await assert_no_fault_events(plane, run_id)
    return {"scenario": "C", "elapsed_s": round(time.monotonic() - started, 1)}


async def restart_reuses_executor_session(
    project_dir: Path, wait_s: float = RUN_TIMEOUT_S
) -> dict[str, Any]:
    """执行器写第一个文件后长眠，期间进程被强杀；重启后续用原会话跑到 passed。"""
    started = time.monotonic()
    member_wait_s = min(wait_s, MEMBER_TIMEOUT_S)
    yaml_path = write_workflow(project_dir / "smoke-d.yaml", [WORKER_WITH_SLEEP_NODE])
    async with live_plane(project_dir) as crashed:
        run_id = await crashed.start_run(yaml_path, "smoke-d")
        executor = await wait_member(crashed, run_id, "node=worker", member_wait_s)
        await wait_tool_use(crashed, executor["session_id"], "Bash", "time.sleep", member_wait_s)
        archive = await crashed.node_archive(run_id, "worker")
        first_file = Path(archive["workspace_path"]) / "first.txt"
        assert first_file.read_text(encoding="utf-8") == "first\n", "长眠前第一个文件未落盘"
        await asyncio.sleep(KILL_SETTLE_S)
        still_running = await crashed.api.run.get_run(run_id)
        assert still_running.finished_at is None, "长眠命令没有阻塞，强杀窗口不存在"
        await crashed.abandon_as_killed()

    async with live_plane(project_dir) as rebooted:
        run = await rebooted.wait_run_finished(run_id, wait_s)

        assert run.status == "completed", await rebooted.describe(run_id)
        nodes = await rebooted.node_states(run_id)
        assert nodes["worker"]["status"] == "passed", await rebooted.describe(run_id)
        # 复用崩溃前的执行会话与作用域：经终态提交与集成后 passed
        assert nodes["worker"]["workflow_controller_session_id"] == executor["session_id"]
        assert (
            rebooted.member(run_id, "node=worker")["native_session_id"]
            == executor["native_session_id"]
        )
        assert (rebooted.work_dir / "first.txt").read_text(encoding="utf-8") == "first\n"
        assert (rebooted.work_dir / "second.txt").read_text(encoding="utf-8") == "second\n"
        rows = await rebooted.checkpoints(run_id)
        node_rows = scope_rows(rows, "worker")
        assert {row["scope_id"] for row in node_rows} == {archive["workspace_scope_id"]}
        assert {row["trigger"] for row in node_rows} >= {"scope_start", "terminal"}
        assert_workspaces_released(rebooted.repository(run_id), archive["workspace_scope_id"])
        await assert_no_fault_events(rebooted, run_id)
        # 重启后续跑同一会话：发了续跑提示，而不是从头重发任务
        queries = [
            event.data.get("prompt") or ""
            for event in await rebooted.session_events(executor["session_id"])
            if event.event_type is EventType.LOG_QUERY
        ]
        assert any("请继续你之前的工作" in prompt for prompt in queries), queries
        return {"scenario": "D", "elapsed_s": round(time.monotonic() - started, 1)}


async def compat_subagent(
    plane: LivePlane,
    monkeypatch: pytest.MonkeyPatch,
    *,
    settle_delay_s: float = 0.0,
    wait_s: float = RUN_TIMEOUT_S,
) -> dict[str, Any]:
    """弱化档下执行器派 writer 子代理：父会话阻塞到子代理完成，收尾先于结果交付。

    弱化档不提供隔离（子代理运行在父工作区），这里如实断言；收尾入口可
    注入等待，用来证明 CLI 等收尾回调返回后才交付结果。子代理自身的调用
    经同一个父会话进入宿主，不进父作用域的组仲裁。
    """
    started = time.monotonic()
    trace = HostHookTrace()
    trace_host_hooks(monkeypatch, plane, trace, settle_delay_s=settle_delay_s)
    yaml_path = write_workflow(
        plane.project_dir / "smoke-b.yaml", [SUBAGENT_DISPATCH_NODE], compat=True
    )
    run_id = await plane.start_run(yaml_path, "smoke-b")
    run = await plane.wait_run_finished(run_id, wait_s)

    assert run.status == "completed", await plane.describe(run_id)
    nodes = await plane.node_states(run_id)
    assert nodes["orchestrator"]["status"] == "passed", await plane.describe(run_id)
    await assert_no_fault_events(plane, run_id)

    rows = await plane.checkpoints(run_id)
    node_rows = scope_rows(rows, "orchestrator")
    node_scope = node_rows[0]["scope_id"]
    child_rows = [
        row
        for row in rows
        if row["scope_kind"] == "subagent" and row["parent_scope_id"] == node_scope
    ]
    assert [row["trigger"] for row in child_rows] == ["terminal"], rows
    child_scope = child_rows[0]["scope_id"]
    dispatch_rows = [
        row
        for row in node_rows
        if row["trigger"] == "before_child"
        and (row["dispatch_anchor"] or {}).get("preallocated_scope_id") == child_scope
    ]
    assert len(dispatch_rows) == 1
    # 弱化档的屏障质量：组检查点行标注 late_barrier
    assert dispatch_rows[0]["barrier_quality"] == "late_barrier"
    child_session_map = dispatch_rows[0]["child_session_map"] or {}
    assert child_scope in child_session_map

    # 子代理自身的调用不进父作用域的组仲裁：节点作用域上没有子代理调用建出的组检查点行，
    # 父作用域的组台账里也没有子代理的工具调用号
    repo = plane.repository(run_id)
    child_tool_ids = {
        b["tool_use_id"] for b in trace.barriers if b["agent_id"] or b["parent_tool_use_id"]
    }
    assert child_tool_ids, trace.barriers
    group_tool_ids = {
        (row["dispatch_anchor"] or {}).get("tool_use_id")
        for row in node_rows
        if row["trigger"] == "before_child"
    }
    assert not group_tool_ids & child_tool_ids, (group_tool_ids, child_tool_ids)
    group_id = dispatch_rows[0]["dispatch_anchor"]["dispatch_group_id"]
    ledger = json.loads(
        (repo.tree_dir / "dispatch" / f"{group_id}.json").read_text(encoding="utf-8")
    )
    assert not child_tool_ids & set(ledger["side_effects"]), (child_tool_ids, ledger)

    # 弱化档不提供隔离：子代理直接写进了父工作区，子作用域终态提交里没有它的文件
    assert await repo.blob_content(child_rows[0]["workspace_commit"], "from-child.txt") is None
    node_terminal = next(row for row in node_rows if row["trigger"] == "terminal")
    assert await repo.blob_content(node_terminal["workspace_commit"], "from-child.txt") == (
        b"from-child\n"
    )
    assert (plane.work_dir / "from-child.txt").read_text(encoding="utf-8") == "from-child\n"
    assert_workspaces_released(repo, node_scope, child_scope)

    # 时序：收尾返回之后，父代理才发出下一次工具调用
    executor = plane.member(run_id, "node=orchestrator")
    archive = await plane.node_archive(run_id, "orchestrator")
    dispatch_tool_use_id = dispatch_rows[0]["dispatch_anchor"]["tool_use_id"]
    child_agent_id = child_session_map[child_scope]
    settle = next(s for s in trace.settles if s["agent_id"] == child_agent_id)
    dispatch_at = next(b["at"] for b in trace.barriers if b["tool_use_id"] == dispatch_tool_use_id)
    # 父会话转录里只有父代理自己的工具调用，据此认出它在派发之后的下一次调用
    transcript = claude_main_transcript_path(
        archive["workspace_path"], executor["native_session_id"]
    )
    parent_tool_ids = {
        block["id"]
        for row in transcript_rows(transcript)
        for block in ((row.get("message") or {}).get("content") or [])
        if isinstance(block, dict) and block.get("type") == "tool_use"
    }
    parent_read_at = min(
        b["at"]
        for b in trace.barriers
        if b["at"] > dispatch_at and b["tool_use_id"] in parent_tool_ids
    )
    # 父会话阻塞在子代理上：子代理完成后父才读到文件
    assert settle["started_at"] < parent_read_at
    assert settle["finished_at"] <= parent_read_at, "父代理在收尾返回前就发了下一次工具调用"
    read_result = await tool_result_of(plane, executor["session_id"], "Bash", "cat from-child.txt")
    assert not read_result["is_error"] and "from-child" in read_result["content"]
    # 结果交付给父代理的时刻不早于收尾返回（CLI 等到了宿主回调）
    delivered_at = result_recorded_at(transcript, dispatch_tool_use_id)
    assert delivered_at is not None, f"父会话转录里找不到 Task 结果：{transcript}"
    assert delivered_at >= settle["finished_at"], "CLI 没等收尾回调返回就交付了结果"
    return {
        "scenario": "弱化档子代理",
        "settle_delay_s": settle_delay_s,
        "child_side_effect_barriers": len(child_tool_ids),
        "elapsed_s": round(time.monotonic() - started, 1),
        "settle_s": round(settle["finished_at"] - settle["started_at"], 2),
        "delivered_after_settle_s": round(delivered_at - settle["finished_at"], 2),
        "parent_read_after_settle_s": round(parent_read_at - settle["finished_at"], 2),
    }


async def strict_writer_dispatch_denied(
    plane: LivePlane, *, declare_runtime: bool, wait_s: float = RUN_TIMEOUT_S
) -> dict[str, Any]:
    """严格档下脚本化模型发起 Task 调用被拒：不建子作用域，父工作区里没有子代理写入。

    run 参数声明 runtime 时由出生侧禁用 task 工具挡住；不声明时由屏障的
    能力门挡住。两条路都要成立。
    """
    started = time.monotonic()
    yaml_path = write_workflow(plane.project_dir / "smoke-b.yaml", [SUBAGENT_DISPATCH_NODE])
    run_id = await plane.start_run(yaml_path, "smoke-b", declare_runtime=declare_runtime)
    run = await plane.wait_run_finished(run_id, wait_s)

    assert run.status == "completed", await plane.describe(run_id)
    nodes = await plane.node_states(run_id)
    assert nodes["orchestrator"]["status"] == "passed", await plane.describe(run_id)
    await assert_no_fault_events(plane, run_id)

    rows = await plane.checkpoints(run_id)
    assert not [row for row in rows if row["scope_kind"] == "subagent"], rows
    assert not [
        row for row in rows if (row["dispatch_anchor"] or {}).get("preallocated_scope_id")
    ], rows
    node_rows = scope_rows(rows, "orchestrator")
    node_terminal = next(row for row in node_rows if row["trigger"] == "terminal")
    # 父工作区里没有子代理写入：节点终态提交与根检出都没有该文件
    assert (
        await plane.repository(run_id).blob_content(
            node_terminal["workspace_commit"], "from-child.txt"
        )
        is None
    )
    assert not (plane.work_dir / "from-child.txt").exists()
    assert_workspaces_released(plane.repository(run_id), node_rows[0]["scope_id"])

    # Task 调用确实被拒（以错误结果交回模型），拒绝来自预期的那一层
    executor = plane.member(run_id, "node=orchestrator")
    task_result = await tool_result_of(plane, executor["session_id"], "Task")
    assert task_result["is_error"], task_result
    expected = "No such tool available" if declare_runtime else "cwd 注入"
    assert expected in str(task_result["content"]), task_result
    return {
        "scenario": "严格档拒绝 writer 派发",
        "declare_runtime": declare_runtime,
        "elapsed_s": round(time.monotonic() - started, 1),
        "task_result": str(task_result["content"])[:160],
    }
