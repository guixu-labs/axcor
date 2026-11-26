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

"""对话态重建编排——子代理回滚链的截断、种子与新会话绑定。

过渡期平铺于 core/（P5.2 吸收 session_reconstructor 时升包为 core/restore/）。
只做编排：message 级截断与种子在本模块，L2 定位经 transcript/ 现有面，
会话创建经注入的 start_session_with_resume 入口（session/ 现有面），均不
自行实现。opencode 源的 fork-db 种子随会话重建能力迁移（S5b）落地，当前
仅支持 claude 系 JSONL 源。
"""

from __future__ import annotations

import asyncio
import json
import shutil
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from workflow_controller.transcript.sources import claude_project_key

if TYPE_CHECKING:
    from workflow_controller.core.checkpoints import DispatchAnchor

__all__ = [
    "CONTINUE_SIGNAL",
    "ConversationRestoreData",
    "ConversationRestorePlan",
    "ConversationRestorer",
    "RestoreSessionBinding",
    "TranscriptPart",
    "Truncation",
    "resolve_truncation",
]

# 自然续跑的首轮信号：不注入重放语义，由恢复后的父对话自行决定下一步
CONTINUE_SIGNAL = "请继续你之前的工作。"

# 源 transcript 定位器：按原生会话标识与工作目录解析 JSONL 路径
TranscriptLocator = Callable[[str, str], Path | None]

# 会话创建入口：编排器的 start_session_with_resume 形态
ResumedSessionStarter = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class TranscriptPart:
    """L2 单条消息事实：标识、回合键、角色与所含工具调用。"""

    message_id: str
    role: str = ""
    turn_key: str = ""
    tool_use_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class Truncation:
    """截断结果：保留行下标、命中边界与是否回退。"""

    kept: tuple[int, ...]
    boundary_message_id: str | None
    fell_back: bool


@dataclass(frozen=True)
class ConversationRestorePlan:
    """恢复输入：锚点、作用域事实与绑定/首轮信号选择。"""

    run_id: str
    scope_id: str
    scope_epoch: int
    checkpoint_id: str
    anchor: DispatchAnchor
    workspace_path: str
    seed_dir: Path
    session_runtime: str | None = None
    replay_tool_name: str = ""
    replay_tool_input: Mapping[str, Any] | None = None
    # 单会话工作区模式的配置透传：恢复会话须接续同一棵托管作用域树
    traceback: Any | None = None


@dataclass(frozen=True)
class ConversationRestoreData:
    """恢复数据（恢复动作阶段产出，不含新会话）。"""

    plan: ConversationRestorePlan
    resume_sdk_session_id: str
    fork_env: dict[str, str] = field(default_factory=dict)
    first_signal: str = CONTINUE_SIGNAL
    kept_messages: int = 0
    boundary_message_id: str | None = None
    fell_back: bool = False


@dataclass(frozen=True)
class RestoreSessionBinding:
    """绑定结果：新会话标识与溯源四元组。"""

    session_id: str
    run_id: str
    scope_id: str
    scope_epoch: int
    checkpoint_id: str


def resolve_truncation(
    parts: list[TranscriptPart],
    *,
    tool_use_id: str = "",
    exclude_message_id: str = "",
    turn_key: str = "",
) -> Truncation:
    """按锚点解析 message 级截断边界：工具调用 > 精确消息 > 回合首条助手消息。

    边界消息未持久化（含仅有部分 part 落库的形态之外的全缺失）时回退
    上一完整 message 边界——保留全部已落库前缀（其末尾即上一完整边界）。
    """

    def first_match(predicate: Callable[[TranscriptPart], bool]) -> str | None:
        for part in parts:
            if predicate(part):
                return part.message_id
        return None

    boundary = None
    if tool_use_id:
        boundary = first_match(lambda p: tool_use_id in p.tool_use_ids)
    if boundary is None and exclude_message_id:
        boundary = first_match(lambda p: p.message_id == exclude_message_id)
    if boundary is None and turn_key:
        # 回合粒度兜底：回合内首条助手消息（保留同回合的用户消息）
        boundary = first_match(lambda p: p.turn_key == turn_key and p.role == "assistant")
    if boundary is None:
        return Truncation(tuple(range(len(parts))), None, True)
    kept: list[int] = []
    for index, part in enumerate(parts):
        if part.message_id == boundary:
            break
        kept.append(index)
    return Truncation(tuple(kept), boundary, False)


def _load_claude_rows(source: Path) -> list[dict[str, Any]]:
    """读源 JSONL；坏行（崩溃半写）及其后丢弃，保留可用前缀。"""
    rows: list[dict[str, Any]] = []
    for line in source.read_text(encoding="utf-8").split("\n"):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            break
    return rows


def _row_to_part(row: dict[str, Any]) -> TranscriptPart | None:
    """JSONL 行 → 消息事实；非对话行返回 None。"""
    role = str(row.get("type") or "")
    if role not in ("user", "assistant"):
        return None
    content = (row.get("message") or {}).get("content")
    tool_use_ids: tuple[str, ...] = ()
    if isinstance(content, list):
        tool_use_ids = tuple(
            str(block.get("id"))
            for block in content
            if isinstance(block, dict) and block.get("type") == "tool_use" and block.get("id")
        )
    return TranscriptPart(
        message_id=str(row.get("uuid") or ""),
        role=role,
        turn_key=str(row.get("promptId") or row.get("requestId") or ""),
        tool_use_ids=tool_use_ids,
    )


def _copy_cli_config_files(source_config: Path, fork_config: Path) -> None:
    """把 CLI 配置/凭证文件（存在才拷）拷进恢复配置目录。"""
    fork_config.mkdir(parents=True, exist_ok=True)
    for name in ("settings.json", "settings.local.json", ".credentials.json"):
        source_file = source_config / name
        if source_file.is_file():
            shutil.copy2(source_file, fork_config / name)


def _replay_signal(tool_name: str, tool_input: Mapping[str, Any]) -> str:
    """确定性重派信号：携带组检查点留存的原始 task 调用参数。"""
    payload = json.dumps(dict(tool_input), ensure_ascii=False)
    return f"请重新派发子代理任务（与回滚前调用参数一致）：\n工具 {tool_name}，参数 {payload}"


class ConversationRestorer:
    """对话态重建编排：prepare 截断并落种子，bind 创建并绑定恢复会话。"""

    def __init__(
        self,
        *,
        locate_transcript: TranscriptLocator,
        start_resumed_session: ResumedSessionStarter,
    ) -> None:
        self._locate = locate_transcript
        self._start = start_resumed_session

    async def prepare(self, plan: ConversationRestorePlan) -> ConversationRestoreData:
        """截断父对话并落种子：不启动新会话（定序保证的前半步）。"""
        source = await asyncio.to_thread(
            self._locate, plan.anchor.parent_native_session_id, plan.workspace_path
        )
        if source is None or not source.is_file():
            raise FileNotFoundError(
                "未找到父会话 transcript，无法准备对话恢复数据："
                f"session={plan.anchor.parent_native_session_id} cwd={plan.workspace_path}"
            )
        rows = await asyncio.to_thread(_load_claude_rows, source)
        indexed = [
            (index, part) for index, part in enumerate(map(_row_to_part, rows)) if part is not None
        ]
        truncation = resolve_truncation(
            [part for _, part in indexed],
            tool_use_id=plan.anchor.tool_use_id,
            exclude_message_id=plan.anchor.exclude_message_id,
            turn_key=plan.anchor.parent_message_id,
        )
        kept_rows = [rows[indexed[i][0]] for i in truncation.kept]
        if not any(str(row.get("type")) == "user" for row in kept_rows):
            raise ValueError("截断后对话无用户消息，恢复会话无法续跑")
        # 恢复会话与父会话共用同一工作区目录，种子行无需路径重写
        new_sdk_session_id = str(uuid.uuid4())
        fork_config = plan.seed_dir / "claude"
        target = (
            fork_config
            / "projects"
            / claude_project_key(plan.workspace_path)
            / f"{new_sdk_session_id}.jsonl"
        )
        await asyncio.to_thread(self._write_seed, source, target, kept_rows, new_sdk_session_id)
        first_signal = CONTINUE_SIGNAL
        if plan.replay_tool_input is not None:
            first_signal = _replay_signal(plan.replay_tool_name, plan.replay_tool_input)
        return ConversationRestoreData(
            plan=plan,
            resume_sdk_session_id=new_sdk_session_id,
            fork_env={"CLAUDE_CONFIG_DIR": str(fork_config)},
            first_signal=first_signal,
            kept_messages=len({indexed[i][1].message_id for i in truncation.kept}),
            boundary_message_id=truncation.boundary_message_id,
            fell_back=truncation.fell_back,
        )

    async def bind(self, data: ConversationRestoreData) -> RestoreSessionBinding:
        """创建并绑定恢复会话：绑定原 run/作用域/新代次与恢复点。"""
        from workflow_controller.config.settings import SessionConfig

        plan = data.plan
        config = SessionConfig(
            name=f"restore-{plan.scope_id}-e{plan.scope_epoch}",
            role="executor",
            cwd=plan.workspace_path,
            initial_prompt=data.first_signal,
            runtime=plan.session_runtime,
            env_overrides=dict(data.fork_env),
            run_id=plan.run_id or None,
            traceback=plan.traceback,
        )
        metadata = {
            "workspace_restore": {
                "run_id": plan.run_id,
                "scope_id": plan.scope_id,
                "scope_epoch": plan.scope_epoch,
                "checkpoint_id": plan.checkpoint_id,
            }
        }
        session = await self._start(
            config,
            resume_sdk_session_id=data.resume_sdk_session_id,
            plugin_metadata=metadata,
            start_action="restore_session_started",
        )
        return RestoreSessionBinding(
            session_id=str(session.id),
            run_id=plan.run_id,
            scope_id=plan.scope_id,
            scope_epoch=plan.scope_epoch,
            checkpoint_id=plan.checkpoint_id,
        )

    @staticmethod
    def _write_seed(
        source: Path, target: Path, rows: list[dict[str, Any]], new_session_id: str
    ) -> None:
        """写种子 JSONL 与恢复配置目录（sessionId 改写为新会话）。"""
        _copy_cli_config_files(source.parent.parent.parent, target.parent.parent)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as out:
            for row in rows:
                row["sessionId"] = new_session_id
                out.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
