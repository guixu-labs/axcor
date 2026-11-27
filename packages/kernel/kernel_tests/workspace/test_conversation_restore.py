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

"""对话截断与种子单测：message 级边界、回退边界与共用恢复边界。"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from workflow_controller.core.checkpoints import DispatchAnchor
from workflow_controller.core.restore.conversation import (
    CONTINUE_SIGNAL,
    ConversationRestorePlan,
    ConversationRestorer,
    TranscriptPart,
    resolve_truncation,
)

if TYPE_CHECKING:
    from pathlib import Path


def _anchor(**overrides: object) -> DispatchAnchor:
    """默认锚点（可按用例覆盖字段）。"""
    fields: dict[str, object] = {
        "parent_native_session_id": "ses-parent",
        "parent_message_id": "turn-1",
        "tool_use_id": "toolu-b",
        "preallocated_scope_id": "child-b",
        "exclude_message_id": "turn-1",
        "dispatch_group_id": "node-a-g1",
    }
    fields.update(overrides)
    return DispatchAnchor(**fields)  # type: ignore[arg-type]


def test_message_level_truncation_drops_whole_message() -> None:
    """message 级截断：边界消息的全部 part 及其后丢弃，无孤儿 task 文本。"""
    parts = [
        TranscriptPart(message_id="msg-user", role="user", turn_key="turn-1"),
        TranscriptPart(
            message_id="msg-a", role="assistant", turn_key="turn-1", tool_use_ids=("toolu-a",)
        ),
        TranscriptPart(message_id="msg-b", role="assistant", turn_key="turn-1"),
        TranscriptPart(
            message_id="msg-b", role="assistant", turn_key="turn-1", tool_use_ids=("toolu-b",)
        ),
        TranscriptPart(message_id="msg-c", role="assistant", turn_key="turn-2"),
    ]
    truncation = resolve_truncation(parts, tool_use_id="toolu-b")
    assert truncation.boundary_message_id == "msg-b"
    assert not truncation.fell_back
    # 保留前缀只到 msg-a：msg-b 的前置文本 part 与 task part 一并丢弃
    assert [parts[i].message_id for i in truncation.kept] == ["msg-user", "msg-a"]


def test_unpersisted_message_falls_back_to_previous_boundary() -> None:
    """锚点消息未完整持久化：边界回退上一完整 message。"""
    parts = [
        TranscriptPart(message_id="msg-user", role="user", turn_key="turn-1"),
        TranscriptPart(message_id="msg-a", role="assistant", turn_key="turn-1"),
    ]
    # task part 尚未落库：工具调用与消息标识都找不到边界
    truncation = resolve_truncation(parts, tool_use_id="toolu-b", exclude_message_id="msg-b")
    assert truncation.fell_back
    assert truncation.boundary_message_id is None
    assert [parts[i].message_id for i in truncation.kept] == ["msg-user", "msg-a"]
    # 部分持久化（同消息仅文本 part 落库）：按消息标识命中边界，部分行一并丢弃
    partial = [
        TranscriptPart(message_id="msg-user", role="user"),
        TranscriptPart(message_id="msg-b", role="assistant"),
    ]
    partial_truncation = resolve_truncation(partial, exclude_message_id="msg-b")
    assert partial_truncation.boundary_message_id == "msg-b"
    assert [partial[i].message_id for i in partial_truncation.kept] == ["msg-user"]


def test_same_message_multiple_tasks_share_boundary() -> None:
    """同 message 多 task：两个锚点共用同一恢复边界。"""
    parts = [
        TranscriptPart(message_id="msg-user", role="user", turn_key="turn-1"),
        TranscriptPart(
            message_id="msg-b",
            role="assistant",
            turn_key="turn-1",
            tool_use_ids=("toolu-a", "toolu-b"),
        ),
    ]
    first = resolve_truncation(parts, tool_use_id="toolu-a")
    second = resolve_truncation(parts, tool_use_id="toolu-b")
    assert first.boundary_message_id == second.boundary_message_id == "msg-b"
    assert first.kept == second.kept == (0,)


def test_turn_key_falls_back_to_first_assistant_message() -> None:
    """回合键兜底：截到回合内首条助手消息，保留同回合用户消息。"""
    parts = [
        TranscriptPart(message_id="msg-user", role="user", turn_key="turn-1"),
        TranscriptPart(message_id="msg-b", role="assistant", turn_key="turn-1"),
    ]
    truncation = resolve_truncation(parts, turn_key="turn-1")
    assert truncation.boundary_message_id == "msg-b"
    assert [parts[i].message_id for i in truncation.kept] == ["msg-user"]


def _write_source_transcript(path: Path) -> None:
    """写合成父会话 JSONL：用户消息 + 含 task 调用的助手消息 + 其后一条。"""
    rows = [
        {"type": "user", "uuid": "msg-user", "promptId": "turn-1", "message": {"content": []}},
        {
            "type": "assistant",
            "uuid": "msg-b",
            "promptId": "turn-1",
            "message": {
                "content": [
                    {"type": "text", "text": "我将调用子代理"},
                    {"type": "tool_use", "id": "toolu-b", "name": "Task"},
                ]
            },
        },
        {"type": "assistant", "uuid": "msg-c", "promptId": "turn-2", "message": {"content": []}},
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as out:
        for row in rows:
            out.write(json.dumps(row) + "\n")


def _make_restorer(source: Path, started: list[dict[str, Any]]) -> ConversationRestorer:
    """装配可注入定位器与会话入口的恢复器（记录创建调用）。"""

    def _locate(native_session_id: str, workspace_path: str) -> Path | None:
        return source if native_session_id == "ses-parent" else None

    async def _start(config: Any, **kwargs: Any) -> Any:
        class _Session:
            id = f"ses-restore-{len(started) + 1}"

        started.append({"config": config, **kwargs})
        return _Session()

    return ConversationRestorer(locate_transcript=_locate, start_resumed_session=_start)


def _plan(tmp_path: Path) -> ConversationRestorePlan:
    """默认恢复输入。"""
    return ConversationRestorePlan(
        run_id="run-1",
        scope_id="node-a",
        scope_epoch=2,
        checkpoint_id="node-a:before_child:node-a-g1",
        anchor=_anchor(),
        workspace_path=str(tmp_path / "ws"),
        seed_dir=tmp_path / "restore" / "op-1",
        session_runtime="claude_sdk",
    )


def _project_key_of(workspace: Path) -> str:
    """claude 项目键（直接用 transcript 面的实现）。"""
    from workflow_controller.transcript.sources import claude_project_key

    return claude_project_key(str(workspace))


async def test_restorer_prepare_truncates_and_seeds(tmp_path: Path) -> None:
    """prepare：按锚点截断源 JSONL 并落种子，不启动新会话。"""
    source = tmp_path / "projects" / "key" / "ses-parent.jsonl"
    _write_source_transcript(source)
    started: list[dict[str, Any]] = []
    restorer = _make_restorer(source, started)
    data = await restorer.prepare(_plan(tmp_path))
    assert data.fell_back is False
    assert data.boundary_message_id == "msg-b"
    assert data.first_signal == CONTINUE_SIGNAL
    seed = (
        tmp_path
        / "restore"
        / "op-1"
        / "claude"
        / "projects"
        / _project_key_of(tmp_path / "ws")
        / f"{data.resume_sdk_session_id}.jsonl"
    )
    rows = [json.loads(line) for line in seed.read_text(encoding="utf-8").splitlines()]
    assert [row["uuid"] for row in rows] == ["msg-user"]
    assert all(row["sessionId"] == data.resume_sdk_session_id for row in rows)
    assert started == []


async def test_restorer_bind_creates_session_with_binding_metadata(tmp_path: Path) -> None:
    """bind：经注入入口创建会话并携带绑定四元组与 resume 标识。"""
    source = tmp_path / "ses-parent.jsonl"
    _write_source_transcript(source)
    started: list[dict[str, Any]] = []
    restorer = _make_restorer(source, started)
    data = await restorer.prepare(_plan(tmp_path))
    binding = await restorer.bind(data)
    assert binding.session_id == "ses-restore-1"
    assert (binding.run_id, binding.scope_id, binding.scope_epoch, binding.checkpoint_id) == (
        "run-1",
        "node-a",
        2,
        "node-a:before_child:node-a-g1",
    )
    created = started[0]
    assert created["resume_sdk_session_id"] == data.resume_sdk_session_id
    assert created["plugin_metadata"]["workspace_restore"] == {
        "run_id": "run-1",
        "scope_id": "node-a",
        "scope_epoch": 2,
        "checkpoint_id": "node-a:before_child:node-a-g1",
    }
    config = created["config"]
    assert config.cwd == str(tmp_path / "ws")
    assert config.run_id == "run-1"
    assert config.env_overrides["CLAUDE_CONFIG_DIR"].endswith("claude")
