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

import json

from workflow_controller.driver.claude_lineage import ClaudeSubagentLineage


class ToolUseBlock:
    """模拟 SDK 的 Agent 工具调用块。"""

    def __init__(self, block_id: str) -> None:
        self.id = block_id
        self.name = "Agent"
        self.input = {}


class AssistantMessage:
    """模拟带父任务 ID 的 SDK 助手消息。"""

    def __init__(self, block_id: str, parent_tool_use_id: str = "") -> None:
        self.content = [ToolUseBlock(block_id)]
        self.parent_tool_use_id = parent_tool_use_id


class TaskStartedMessage:
    """模拟 SDK 的任务启动消息。"""

    def __init__(self, task_id: str, tool_use_id: str, agent_type: str) -> None:
        self.subtype = "task_started"
        self.session_id = "native-main"
        self.data = {
            "task_id": task_id,
            "tool_use_id": tool_use_id,
            "subagent_type": agent_type,
        }


def _raw_assistant(
    tool_use_id: str, parent_tool_use_id: str = "", tool_name: str = "Agent"
) -> bytes:
    """构造一条 CLI 工具调用消息。"""
    return json.dumps(
        {
            "type": "assistant",
            "parent_tool_use_id": parent_tool_use_id or None,
            "message": {"content": [{"type": "tool_use", "id": tool_use_id, "name": tool_name}]},
        }
    ).encode()


def _raw_task_started(task_id: str, tool_use_id: str, agent_type: str) -> bytes:
    """构造一条 CLI 任务启动消息。"""
    return json.dumps(
        {
            "type": "system",
            "subtype": "task_started",
            "session_id": "native-main",
            "task_id": task_id,
            "tool_use_id": tool_use_id,
            "subagent_type": agent_type,
        }
    ).encode()


def _hook_input(agent_id: str, agent_type: str = "Explore", session_id: str = "root-session"):
    """构造与 SubagentStart hook 输入同形的字典。"""
    return {
        "hook_event_name": "SubagentStart",
        "session_id": session_id,
        "transcript_path": "/tmp/transcript.jsonl",
        "cwd": "/tmp",
        "agent_id": agent_id,
        "agent_type": agent_type,
    }


def test_cli_task_chain_restores_nested_parent() -> None:
    """CLI 任务链把 Explore 挂到 designer 而不是 Main Agent。"""
    lineage = ClaudeSubagentLineage()
    assert lineage.observe_raw(_raw_assistant("call-designer")) == []
    designer = lineage.observe_raw(_raw_task_started("designer-id", "call-designer", "designer"))[0]
    assert lineage.observe_raw(_raw_assistant("call-explore", "call-designer")) == []
    explore = lineage.observe_raw(_raw_task_started("explore-id", "call-explore", "Explore"))[0]

    assert designer["parent_agent_id"] == ""
    assert explore["parent_agent_id"] == "designer-id"


def test_sdk_task_chain_restores_nested_parent() -> None:
    """SDK 与 CLI 使用相同的嵌套父级语义。"""
    lineage = ClaudeSubagentLineage()
    lineage.observe_sdk_message(AssistantMessage("call-designer"))
    lineage.observe_sdk_message(TaskStartedMessage("designer-id", "call-designer", "designer"))
    lineage.observe_sdk_message(AssistantMessage("call-explore", "call-designer"))
    explore = lineage.observe_sdk_message(
        TaskStartedMessage("explore-id", "call-explore", "Explore")
    )[0]

    assert explore["parent_agent_id"] == "designer-id"


def test_cli_ignores_regular_tool_and_unmatched_task() -> None:
    """普通工具和未匹配调用不产生子代理启动信息。"""
    lineage = ClaudeSubagentLineage()
    lineage.observe_raw(_raw_assistant("call-bash", tool_name="Bash"))

    assert lineage.observe_raw(_raw_task_started("bash-task", "call-bash", "Bash")) == []
    assert lineage.observe_raw(_raw_task_started("orphan-task", "missing-call", "Explore")) == []


def test_sdk_ignores_task_started_for_regular_tool() -> None:
    """SDK 普通工具的 task_started 不产生子代理启动信息。"""
    lineage = ClaudeSubagentLineage()
    message = AssistantMessage("call-bash")
    message.content[0].name = "Bash"
    lineage.observe_sdk_message(message)

    assert lineage.observe_sdk_message(TaskStartedMessage("bash-task", "call-bash", "Bash")) == []


def test_task_started_without_task_id_is_ignored() -> None:
    """缺少 task_id 时不使用 agent_id 回退。"""
    lineage = ClaudeSubagentLineage()
    lineage.observe_raw(_raw_assistant("call-agent"))
    payload = json.loads(_raw_task_started("", "call-agent", "Explore"))
    payload["agent_id"] = "unexpected-agent-id"
    del payload["task_id"]

    assert lineage.observe_raw(json.dumps(payload).encode()) == []


def test_hook_payload_carries_no_parent() -> None:
    """SubagentStart hook 输入不含派发关系，父级恒为空（派发号原样透传）。"""
    lineage = ClaudeSubagentLineage()
    start = lineage.hook_payload(_hook_input("explore-id"))

    assert start == {
        "agent_id": "explore-id",
        "agent_type": "Explore",
        "parent_agent_id": "",
        "session_id": "root-session",
        "parent_tool_use_id": "",
    }


def test_hook_payload_never_infers_parent_from_session_id() -> None:
    """session_id 是派发会话 ID：与已知 agent ID 同值也不当作父级。"""
    lineage = ClaudeSubagentLineage()
    lineage.observe_sdk_message(AssistantMessage("call-designer"))
    lineage.observe_sdk_message(TaskStartedMessage("designer-id", "call-designer", "designer"))

    start = lineage.hook_payload(_hook_input("explore-id", session_id="designer-id"))

    assert start["parent_agent_id"] == ""


def test_hook_start_then_task_started_supplements_parent() -> None:
    """hook 先以空父级注册，后到的 task_started 补充真实父级。"""
    lineage = ClaudeSubagentLineage()
    hooked = lineage.hook_payload(_hook_input("explore-id"))
    assert hooked["parent_agent_id"] == ""
    lineage.observe_sdk_message(AssistantMessage("call-designer"))
    lineage.observe_sdk_message(TaskStartedMessage("designer-id", "call-designer", "designer"))
    lineage.observe_sdk_message(AssistantMessage("call-explore", "call-designer"))
    started = lineage.observe_sdk_message(
        TaskStartedMessage("explore-id", "call-explore", "Explore")
    )[0]

    assert started["agent_id"] == hooked["agent_id"]
    assert started["parent_agent_id"] == "designer-id"
