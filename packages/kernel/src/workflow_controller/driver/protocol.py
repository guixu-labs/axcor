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

"""Driver protocol — abstract interface for AI agent runtimes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from workflow_controller.driver.hooks import DriverHooks
    from workflow_controller.session.events import Event


# 只读评估/探测禁用的工具基表：驱动默认评估查询与 auth 探测共用。
READ_ONLY_BASE_DISALLOWED_TOOLS = [
    "Bash",
    "Write",
    "Edit",
    "Skill",
    "Task",
    "TodoWrite",
    "AskUserQuestion",
    "WebFetch",
    "WebSearch",
    "NotebookEdit",
    "Glob",
    "Grep",
]


class Driver(ABC):
    """Abstract driver for running an AI coding agent."""

    #: 反馈评估的默认模型；基类保持运行时中立（空串交各驱动自决），
    #: Claude 驱动覆写为各自默认模型，opencode 走其配置默认。
    evaluation_default_model: str = ""

    def __init__(self, *, permission_mode: str = "bypassPermissions") -> None:
        self._permission_mode = permission_mode

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """True while agent is active."""
        ...

    @abstractmethod
    def query(
        self,
        prompt: str,
        *,
        iteration: int = 1,
        session_id: str = "",
        model: str = "",
        cwd: str | None = None,
        max_turns: int | None = None,
        allowed_tools: list[str] | None = None,
        disallowed_tools: list[str] | None = None,
        permission_mode: str = "",
        env: dict[str, str] | None = None,
        hooks: DriverHooks | None = None,
        system_prompt: str = "",
        agent_name: str = "",
    ) -> AsyncIterator[Event]:
        """Run the agent and stream Event objects.

        ``hooks`` lets in-process runtimes (SDK/opencode) route tool-use / stop
        interception to the reviewer; the CLI runtime ignores it (it uses the
        HTTP hook server configured via .claude/settings.json).
        """
        ...

    def query_for_evaluation(
        self, prompt: str, *, model: str | None, env: dict[str, str]
    ) -> AsyncIterator[Event]:
        """Query once for a feedback evaluation with the evaluation tool restrictions.

        模型缺省时回退到各驱动自带的 ``evaluation_default_model``，
        基类不声明 Claude 专属默认。
        """
        return self.query(
            prompt,
            model=model or self.evaluation_default_model,
            max_turns=1,
            allowed_tools=[],
            disallowed_tools=[*READ_ONLY_BASE_DISALLOWED_TOOLS, "NotebookRead", "Read"],
            env=env,
        )

    @abstractmethod
    async def stop(self) -> None:
        """Kill the running agent."""
        ...

    async def kill(self) -> None:  # noqa: B027
        """Force-reap the agent's process tree after ``stop()``.

        ``stop()`` asks the runtime to shut down gracefully (lets the SDK
        flush session files etc). ``kill()`` is the backstop: ensure the
        whole process group — including grandchildren the SDK's SIGTERM may
        miss (claude_agent_sdk 的 SubprocessCLITransport 没 setsid,close 的
        SIGTERM 打不到 fork 出来的孙进程 → 孤儿) — is reaped. Default is a
        no-op; runtimes that spawn subprocesses (claude_sdk / claude_cli)
        override.
        """
        ...

    def update_session_title(self, native_session_id: str, name: str) -> bool:
        """Driver-agnostic title seam: opencode overrides (sqlite UPDATE), SDK/CLI
        inherit this noop. Orchestrator delegates here (no direct runtime-DB
        access). Returns False if no row was renamed (no DB / not written yet).
        """
        return False
