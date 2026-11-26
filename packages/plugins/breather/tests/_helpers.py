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

"""breather 测试共享夹具：插件装配 / 事件 / 会话 query 替身 / CLI 子代理 transcript 构造。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from workflow_controller.event.type_registry import EventTypeRegistry
from workflow_controller.plugin.base import PluginConfig
from workflow_controller.plugin.context import PluginContext
from workflow_controller.plugin.registry import _build_plugin_services
from workflow_controller.plugins.breather.plugin import BreatherPlugin
from workflow_controller.session.events import Event, EventType


def plugin_context(orch: Any, hook_server: Any = None) -> PluginContext:
    """构造装好的 PluginContext——services 走生产装配函数。

    Mock orchestrator 同样适用：端口闭包委托到 mock 属性上。
    """
    return PluginContext(
        bus=orch.bus,
        type_registry=getattr(orch.bus, "type_registry", None) or EventTypeRegistry(),
        hook_server=hook_server if hook_server is not None else MagicMock(),
        services=_build_plugin_services(orch),
    )


def breather_plugin(orch: Any) -> BreatherPlugin:
    """返回装好的 breather 插件。"""
    plugin = BreatherPlugin(
        PluginConfig(name="breather", version="0", enabled=True),
        Path("."),
    )
    plugin.install(plugin_context(orch))
    return plugin


def assistant_message_query(session):
    """替身 session.query：产出一条 ASSISTANT_MESSAGE 事件（final_text 捕获路径）。"""

    async def _assistant_query(prompt, iteration=1):
        yield Event.create(
            session_id=session.id, iteration=0, event_type=EventType.ASSISTANT_MESSAGE
        )

    return _assistant_query


def subagent_stop_event(session, agent_id="a1", agent_type="dev", status=None):
    """SUBAGENT_STOP 事件（子代理返回点信号）；status 可标记 interrupted。"""
    data = {"agent_id": agent_id, "agent_type": agent_type}
    if status is not None:
        data["status"] = status
    return Event.create(
        session_id=session.native_session_id,
        iteration=0,
        event_type=EventType.SUBAGENT_STOP,
        data=data,
    )


def claude_child_transcript(tmp_path):
    """CLI 子代理 fake transcript 路径（.claude/projects/<slug>/native/subagents）。"""
    project = str(tmp_path.resolve()).replace("/", "-")
    directory = tmp_path / ".claude" / "projects" / project / "native" / "subagents"
    directory.mkdir(parents=True)
    return directory / "agent-child.jsonl"


def append_transcript_row(transcript, row):
    """向 jsonl transcript 追加一行 JSON。"""
    with transcript.open("a", encoding="utf-8") as out:
        out.write(json.dumps(row) + "\n")


def cli_child_running_row(agent_type="explore", input_tokens=30):
    """子代理“运行中”的 assistant transcript 行（tool_use，未结束）。"""
    return {
        "type": "assistant",
        "attributionAgent": agent_type,
        "message": {"usage": {"input_tokens": input_tokens}, "stop_reason": "tool_use"},
    }


def cli_child_done_row(input_tokens=40):
    """子代理结束的 assistant transcript 行（end_turn）。"""
    return {
        "type": "assistant",
        "message": {"usage": {"input_tokens": input_tokens}, "stop_reason": "end_turn"},
    }
