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

"""Codex 子代理 rollout 只读进度层。"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from workflow_controller.driver.hooks import DriverHooks

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 0.2
DISCOVERY_TIMEOUT_SECONDS = 30.0
PRE_START_BUFFER_LIMIT = 200
OUTPUT_TEXT_LIMIT = 4000

_CHILD_DISCOVERED = "discovered"
_CHILD_ACTIVE = "active"
_CHILD_COMPLETED = "completed"
_COMPLETED_STATES = {
    "completed",
    "complete",
    "finished",
    "interrupted",
    "errored",
    "shutdown",
    "not_found",
}


def find_rollout_file(codex_home: Path, child_thread_id: str) -> Path | None:
    """按子线程 id 精确匹配 rollout 文件，多文件时取最新。"""
    if not child_thread_id:
        return None
    try:
        matches = sorted(codex_home.glob(f"sessions/**/rollout-*-{child_thread_id}.jsonl"))
    except OSError:
        return None
    return matches[-1] if matches else None


def rollout_meta_matches(
    path: Path,
    child_thread_id: str,
    parent_thread_id: str,
) -> bool:
    """校验首行会话归属，避免并发与历史 rollout 误配。"""
    try:
        with path.open("rb") as file:
            first_line = file.readline()
        record = json.loads(first_line)
    except (OSError, json.JSONDecodeError):
        return False
    payload = record.get("payload", {})
    return (
        record.get("type") == "session_meta"
        and isinstance(payload, dict)
        and payload.get("id") == child_thread_id
        and payload.get("parent_thread_id") == parent_thread_id
    )


def truncate_text(text: str) -> str:
    """截断进入事件流的长输出。"""
    if len(text) <= OUTPUT_TEXT_LIMIT:
        return text
    return text[:OUTPUT_TEXT_LIMIT] + "…[截断]"


def _record_payload(record: dict[str, Any]) -> dict[str, Any]:
    """读取记录 payload，非字典时返回空。"""
    payload = record.get("payload", {})
    return payload if isinstance(payload, dict) else {}


def _content_blocks_text(content: Any) -> str:
    """拼接内容块里的文本。"""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if isinstance(block, dict) and block.get("text"):
            parts.append(str(block["text"]))
    return "\n".join(parts)


def _function_output_status(text: str, *, is_command: bool) -> bool | None:
    """从真实输出内容推断失败态，无法判定时返回未知。"""
    if is_command:
        match = re.search(r"Process exited with code (-?\d+)", text)
        if match is not None:
            return int(match.group(1)) != 0
    for line in text.splitlines():
        try:
            output = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(output, dict) and output.get("error") is not None:
            return True
    return None


class RolloutProgressReader:
    """把 rollout 增量行归一为子代理进度消息。"""

    def __init__(self) -> None:
        self.last_ordinal = -1
        self.task_completed = False
        self._result_ids: set[str] = set()
        self._command_call_ids: set[str] = set()
        self._pending_command_outputs: dict[str, dict[str, Any]] = {}

    def read_messages(self, lines: list[bytes]) -> list[dict[str, Any]]:
        """按序消费完整 JSONL 行并去重，返回新增进度消息。"""
        messages: list[dict[str, Any]] = []
        for raw_line in lines:
            try:
                record = json.loads(raw_line)
            except json.JSONDecodeError:
                logger.debug("忽略无法解析的 rollout 行: %r", raw_line[:120])
                continue
            if not isinstance(record, dict) or self._seen(record):
                continue
            messages.extend(self._record_messages(record))
        if self.task_completed and self._pending_command_outputs:
            self._result_ids.update(self._pending_command_outputs)
            messages.extend(self._pending_command_outputs.values())
            self._pending_command_outputs.clear()
        return messages

    def _seen(self, record: dict[str, Any]) -> bool:
        """按 ordinal 去重历史记录。"""
        ordinal = record.get("ordinal")
        if not isinstance(ordinal, int):
            return False
        if ordinal <= self.last_ordinal:
            return True
        self.last_ordinal = ordinal
        return False

    def _record_messages(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        """按记录类型归一为进度消息。"""
        record_type = str(record.get("type") or "")
        payload = _record_payload(record)
        if record_type == "response_item":
            return self._response_item_messages(payload)
        if record_type == "event_msg":
            return self._event_messages(payload)
        return []

    def _response_item_messages(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """归一 response_item 记录。"""
        payload_type = str(payload.get("type") or "")
        if payload_type == "function_call":
            call_id = str(payload.get("call_id") or payload.get("id") or "")
            if str(payload.get("name") or "") == "exec_command":
                self._command_call_ids.add(call_id)
            tool_input: Any = payload.get("arguments")
            if isinstance(tool_input, str):
                try:
                    tool_input = json.loads(tool_input)
                except json.JSONDecodeError:
                    tool_input = {"arguments": tool_input}
            if not isinstance(tool_input, dict):
                tool_input = {"arguments": tool_input}
            return [
                {
                    "type": "tool_use",
                    "name": str(payload.get("name") or ""),
                    "id": call_id,
                    "input": tool_input,
                }
            ]
        if payload_type == "function_call_output":
            call_id = str(payload.get("call_id") or payload.get("id") or "")
            is_command = call_id in self._command_call_ids
            output = payload.get("output")
            content = output if isinstance(output, str) else _content_blocks_text(output)
            is_error = _function_output_status(content, is_command=is_command)
            if call_id in self._command_call_ids:
                if call_id not in self._result_ids:
                    self._pending_command_outputs[call_id] = {
                        "type": "tool_result",
                        "tool_use_id": call_id,
                        "content": truncate_text(content),
                        "is_error": is_error,
                    }
                return []
            return self._tool_result(
                call_id,
                truncate_text(content),
                is_error=is_error,
            )
        if payload_type == "reasoning":
            text = _content_blocks_text(payload.get("content"))
            if not text.strip():
                text = _content_blocks_text(payload.get("summary"))
            if text.strip():
                return [{"type": "thinking", "text": truncate_text(text)}]
        return []

    def _event_messages(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """归一 event_msg 记录。"""
        payload_type = str(payload.get("type") or "")
        if payload_type == "task_complete":
            self.task_completed = True
            return []
        if payload_type != "item_completed":
            return []
        item = payload.get("item", {})
        if not isinstance(item, dict):
            return []
        item_type = str(item.get("type") or "")
        if item_type == "CommandExecution":
            exit_code = item.get("exit_code")
            status = str(item.get("status") or "").lower()
            if status in {"failed", "declined"} or exit_code not in (None, 0, "0"):
                is_error = True
            elif status == "completed" or exit_code in (0, "0"):
                is_error = False
            else:
                is_error = None
            content = str(
                item.get("aggregated_output") or item.get("stdout") or item.get("stderr") or ""
            )
            call_id = str(item.get("id") or "")
            messages = self._tool_result(
                call_id,
                truncate_text(content),
                is_error=is_error,
            )
            self._pending_command_outputs.pop(call_id, None)
            return messages
        if item_type == "AgentMessage":
            text = _content_blocks_text(item.get("content"))
            if text.strip():
                return [{"type": "text", "text": truncate_text(text)}]
        return []

    def _tool_result(
        self,
        call_id: str,
        content: str,
        *,
        is_error: bool | None,
    ) -> list[dict[str, Any]]:
        """按 call_id 去重工具结果。"""
        if not call_id or call_id in self._result_ids:
            return []
        self._result_ids.add(call_id)
        return [
            {
                "type": "tool_result",
                "tool_use_id": call_id,
                "content": content,
                "is_error": is_error,
            }
        ]


class RolloutFileTail:
    """按字节偏移尾读文件，只消费完整换行。"""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._offset = 0
        self._buffer = b""

    def read_lines(self) -> list[bytes]:
        """返回自上次调用后新增的完整行。"""
        try:
            with self._path.open("rb") as file:
                file.seek(self._offset)
                chunk = file.read()
        except OSError:
            return []
        self._buffer += chunk
        parts = self._buffer.split(b"\n")
        self._buffer = parts[-1]
        complete_lines = parts[:-1]
        self._offset += len(chunk)
        return [line for line in complete_lines if line.strip()]


class ChildProgressState:
    """单个子代理的进度状态。"""

    def __init__(self, child_thread_id: str) -> None:
        self.child_thread_id = child_thread_id
        self.state = _CHILD_DISCOVERED
        self.stop_requested = False
        self.rollout_completed = False
        self.wake_up = asyncio.Event()
        self.buffer: list[dict[str, Any]] = []
        self.task: asyncio.Task[None] | None = None
        self.reader = RolloutProgressReader()

    @property
    def is_completed(self) -> bool:
        """子代理是否已完成。"""
        return self.state == _CHILD_COMPLETED


class SubagentProgressMonitor:
    """按父流事实编排子 rollout 进度。"""

    def __init__(
        self,
        *,
        hooks: DriverHooks,
        session_id: str,
        codex_home: str | Path,
        poll_interval: float = POLL_INTERVAL_SECONDS,
        discovery_timeout: float = DISCOVERY_TIMEOUT_SECONDS,
        buffer_limit: int = PRE_START_BUFFER_LIMIT,
    ) -> None:
        self._hooks = hooks
        self._session_id = session_id
        self._codex_home = Path(codex_home)
        self._poll_interval = poll_interval
        self._discovery_timeout = discovery_timeout
        self._buffer_limit = buffer_limit
        self._parent_thread_id = ""
        self._children: dict[str, ChildProgressState] = {}
        self._started_children: set[str] = set()
        self._finished = False

    async def handle_fact(self, fact: dict[str, Any]) -> None:
        """消费父 stdout 提取的协作事实；异常不中断主查询。"""
        try:
            kind = str(fact.get("kind") or "")
            if kind == "parent_started":
                parent_id = str(fact.get("parent_id") or "")
                if parent_id and not self._parent_thread_id:
                    self._parent_thread_id = parent_id
            elif kind == "collab":
                self._handle_collab_fact(fact)
        except Exception:
            logger.debug("处理子代理协作事实失败", exc_info=True)

    async def notify_subagent_started(self, child_thread_id: str) -> None:
        """SubagentStart 对账：激活已发现的子代理并冲刷缓冲。"""
        self._started_children.add(child_thread_id)
        child = self._children.get(child_thread_id)
        if child is None:
            return
        if child.state != _CHILD_DISCOVERED:
            if not (child.is_completed and child.rollout_completed):
                return
        else:
            child.state = _CHILD_ACTIVE
        buffered = child.buffer
        child.buffer = []
        if buffered:
            await self._send_progress(child_thread_id, buffered)

    async def notify_subagent_stopped(self, child_thread_id: str) -> None:
        """SubagentStop 对账：请求尾读最终批次。"""
        self._request_child_completion(child_thread_id)

    async def finish(self) -> None:
        """查询结束：停止全部子代理任务，不伪造子代理事件。"""
        self._finished = True
        tasks = [
            child.task
            for child in self._children.values()
            if child.task is not None and not child.task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _handle_collab_fact(self, fact: dict[str, Any]) -> None:
        """处理 spawn/wait/close 协作事实。"""
        tool = str(fact.get("tool") or "")
        if tool.startswith("spawn"):
            for raw_child_id in fact.get("child_ids") or []:
                self._spawn_child(str(raw_child_id))
            return
        if not (tool.startswith("wait") or tool.startswith("close")):
            return
        states = fact.get("states")
        if not isinstance(states, dict):
            return
        for raw_child_id, raw_state in states.items():
            if str(raw_state).lower() in _COMPLETED_STATES:
                self._request_child_completion(str(raw_child_id))

    def _spawn_child(self, child_thread_id: str) -> None:
        """注册子代理并启动专属尾读任务。"""
        if not child_thread_id or child_thread_id in self._children:
            return
        child = ChildProgressState(child_thread_id)
        self._children[child_thread_id] = child
        if child_thread_id in self._started_children:
            child.state = _CHILD_ACTIVE
        child.task = asyncio.create_task(
            self._run_child(child),
            name=f"codex-subagent-{child_thread_id[:8]}",
        )

    def _complete_child(self, child_thread_id: str, *, preserve_buffer: bool = False) -> None:
        """标记子代理完成并清空缓冲。"""
        child = self._children.get(child_thread_id)
        if child is not None:
            child.state = _CHILD_COMPLETED
            if not preserve_buffer:
                child.buffer.clear()

    def _request_child_completion(self, child_thread_id: str) -> None:
        """请求子代理尾读最终批次后释放。"""
        child = self._children.get(child_thread_id)
        if child is not None:
            child.stop_requested = True
            child.wake_up.set()

    async def _run_child(self, child: ChildProgressState) -> None:
        """发现并持续尾读单个子 rollout；异常只终止该子代理。"""
        try:
            rollout_path = await self._discover_rollout(child)
            if rollout_path is None:
                if child.stop_requested:
                    self._complete_child(child.child_thread_id)
                return
            tail = RolloutFileTail(rollout_path)
            while not self._finished and not child.stop_requested:
                if await self._read_child_progress(child, tail):
                    child.rollout_completed = True
                    self._complete_child(child.child_thread_id, preserve_buffer=True)
                    return
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(child.wake_up.wait(), self._poll_interval)
                child.wake_up.clear()
            if child.stop_requested and not self._finished:
                await self._read_child_progress(child, tail)
                self._complete_child(child.child_thread_id)
        except Exception:
            logger.debug(
                "子代理进度任务失败: child=%s",
                child.child_thread_id[:12],
                exc_info=True,
            )

    async def _read_child_progress(
        self,
        child: ChildProgressState,
        tail: RolloutFileTail,
    ) -> bool:
        """读取并转发一轮 rollout 进度，返回是否读到任务完成。"""
        lines = await asyncio.to_thread(tail.read_lines)
        messages = child.reader.read_messages(lines)
        if messages:
            await self._emit_child_progress(child, messages)
        return child.reader.task_completed

    async def _discover_rollout(self, child: ChildProgressState) -> Path | None:
        """按轮询间隔重试查找并校验 rollout。"""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._discovery_timeout
        while not self._finished and loop.time() < deadline:
            path = await self._find_validated_rollout(child)
            if path is not None:
                return path
            if child.stop_requested:
                break
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(child.wake_up.wait(), self._poll_interval)
            child.wake_up.clear()
            if child.stop_requested:
                path = await self._find_validated_rollout(child)
                if path is not None:
                    return path
                break
        logger.debug(
            "子代理 rollout 未找到或校验失败: child=%s",
            child.child_thread_id[:12],
        )
        return None

    async def _find_validated_rollout(self, child: ChildProgressState) -> Path | None:
        """查找并校验一个子 rollout 文件。"""
        path = await asyncio.to_thread(
            find_rollout_file,
            self._codex_home,
            child.child_thread_id,
        )
        if path is None:
            return None
        meta_matches = await asyncio.to_thread(
            rollout_meta_matches,
            path,
            child.child_thread_id,
            self._parent_thread_id,
        )
        if not meta_matches:
            logger.debug(
                "子代理 rollout 归属校验失败: child=%s parent=%s path=%s",
                child.child_thread_id[:12],
                self._parent_thread_id[:12],
                path,
            )
            return None
        return path

    async def _emit_child_progress(
        self,
        child: ChildProgressState,
        messages: list[dict[str, Any]],
    ) -> None:
        """激活前缓冲、激活后直发、完成后丢弃。"""
        if child.state == _CHILD_ACTIVE:
            await self._send_progress(child.child_thread_id, messages)
            return
        if child.state != _CHILD_DISCOVERED:
            return
        remaining = self._buffer_limit - len(child.buffer)
        if remaining > 0:
            child.buffer.extend(messages[:remaining])

    async def _send_progress(
        self,
        child_thread_id: str,
        messages: list[dict[str, Any]],
    ) -> None:
        """调用子代理进度回调。"""
        callback = self._hooks.on_subagent_progress
        if callback is None:
            return
        await callback(
            self._session_id,
            {
                "agent_id": child_thread_id,
                "messages": messages,
            },
        )
