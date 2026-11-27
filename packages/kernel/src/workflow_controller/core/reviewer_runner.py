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

from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import TYPE_CHECKING, NamedTuple, Protocol

from workflow_controller.config.settings import MIN_REVIEWER_MAX_TOKENS

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    from workflow_controller.core.auth_rotator import AuthRotator
    from workflow_controller.driver.anthropic_http import AnthropicHttpClient

logger = logging.getLogger(__name__)


class _ReviewRequest(NamedTuple):
    prompt: str
    sandbox_root: Path
    model: str
    timeout_seconds: float
    thinking_budget_tokens: int
    max_tokens: int
    thinking_mode: str


class ReviewerRunner(Protocol):
    async def run(
        self,
        prompt: str,
        sandbox_root: Path,
        model: str,
        timeout_seconds: float,
        *,
        thinking_budget_tokens: int = 2048,
        max_tokens: int = MIN_REVIEWER_MAX_TOKENS,
        thinking_mode: str = "disabled",
    ) -> str:
        request = _ReviewRequest(
            prompt,
            sandbox_root,
            model,
            timeout_seconds,
            thinking_budget_tokens,
            max_tokens,
            thinking_mode,
        )
        return await self._run(request)

    async def _run(self, request: _ReviewRequest) -> str:
        raise NotImplementedError


class DriverReviewerRunner(ReviewerRunner):
    """Reviewer runner backed by the deterministic AnthropicHttpClient.

    The reviewer is a single-call LLM judge, so it runs through the raw-HTTP
    client (one ``/v1/messages`` POST, no sampling parameter, no agent loop /
    tools / CC system-prompt framing) instead of the worker's agent-loop
    driver. That removes the run-to-run variance (tool exploration + CC
    framing) that made reviewer decisions flaky.
    """

    def __init__(
        self,
        provider_index: int | None = None,
        on_provider_change: Callable[[int, str], Awaitable[None]] | None = None,
        on_provider_exhausted: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._auth_rotator: AuthRotator | None = None
        self._provider_index = provider_index
        self._on_provider_change = on_provider_change
        self._on_provider_exhausted = on_provider_exhausted

    @staticmethod
    def _classify_review_error(exc: RuntimeError) -> tuple[bool, bool]:
        """错误分类：返回 (是否限流, 是否认证失败)。"""
        message = str(exc).lower()
        is_limit = any(
            marker in message
            for marker in ("429", "rate limit", "rate_limit", "too many requests", "quota")
        )
        is_auth_error = re.search(r"\b(?:401|403)\b", message) is not None
        return is_limit, is_auth_error

    def set_auth_rotator(self, rotator: AuthRotator) -> None:
        self._auth_rotator = rotator

    async def _run(self, request: _ReviewRequest) -> str:
        from workflow_controller.driver.anthropic_http import AnthropicHttpClient

        call_model = await self._resolve_call_model(request.model)
        client = AnthropicHttpClient(timeout=request.timeout_seconds)
        for attempt in range(2):
            try:
                return await asyncio.wait_for(
                    self._call_review(client, request, call_model),
                    timeout=request.timeout_seconds,
                )
            except TimeoutError:
                # Only provider-scoped rate-limit failures rotate and retry.
                # Replaying an indeterminate timed-out review can duplicate
                # cost and still target the same healthy provider.
                raise TimeoutError(f"reviewer timed out after {request.timeout_seconds}s") from None
            except RuntimeError as exc:
                is_limit, is_auth_error = self._classify_review_error(exc)
                skip_retry = attempt > 0 or not is_limit
                rotator = self._auth_rotator
                provider_index = self._provider_index
                if skip_retry or rotator is None or provider_index is None:
                    self._invalidate_review_provider(exc, is_auth_error)
                    raise
                call_model = await self._rotate_review_provider(
                    rotator, provider_index, exc, request.model
                )
        raise RuntimeError("reviewer request failed")

    async def _call_review(
        self, client: AnthropicHttpClient, request: _ReviewRequest, call_model: str
    ) -> str:
        # Whitelisted ANTHROPIC_* env when a rotator is present (production);
        # otherwise fall back to the full os.environ (the test env carries the
        # GLM creds there). The HttpClient reads base_url + token from this.
        if self._auth_rotator is not None:
            sdk_env = self._auth_rotator.env_for_driver(
                dict(os.environ),
                self._provider_index,
            )
            thinking_type = (
                self._auth_rotator.reviewer_thinking_type(
                    self._provider_index,
                    requested_mode=request.thinking_mode,
                )
                or "omitted"
            )
        else:
            sdk_env = dict(os.environ)
            thinking_type = request.thinking_mode
        result = await client.request(
            prompt=request.prompt,
            model=call_model,
            env=sdk_env,
            thinking_budget_tokens=request.thinking_budget_tokens,
            max_tokens=max(MIN_REVIEWER_MAX_TOKENS, request.max_tokens),
            thinking_type=thinking_type,
        )
        if result.is_error:
            raise RuntimeError(f"reviewer HTTP error: {result.error[:200]}")
        text = result.text.strip()
        if not text:
            raise RuntimeError("reviewer HTTP error: response has no text content")
        return text

    async def _resolve_call_model(self, model: str) -> str:
        """按 rotator 可用性解析本轮调用的模型（含 provider 换绑回调）。"""
        call_model = model
        if self._auth_rotator is not None and self._provider_index is not None:
            replacement = self._auth_rotator.resolve_available_provider(
                self._provider_index,
                capability="reviewer",
            )
            if replacement is None:
                if self._on_provider_exhausted is not None:
                    await self._on_provider_exhausted()
                raise RuntimeError("reviewer has no available provider")
            if replacement != self._provider_index:
                self._provider_index = replacement
                call_model = self._auth_rotator.reviewer_model(replacement, model)
                if self._on_provider_change is not None:
                    await self._on_provider_change(replacement, call_model)
            else:
                call_model = self._auth_rotator.reviewer_model(
                    self._provider_index,
                    model,
                )
        return call_model

    def _invalidate_review_provider(self, exc: RuntimeError, is_auth_error: bool) -> None:
        """认证失败时吊销当前 provider 的 reviewer 能力（下次调用换路）。"""
        if is_auth_error and self._auth_rotator is not None and self._provider_index is not None:
            self._auth_rotator.invalidate_provider_capability(
                self._provider_index,
                "reviewer",
                str(exc),
            )

    async def _rotate_review_provider(
        self, rotator: AuthRotator, provider_index: int, exc: RuntimeError, model: str
    ) -> str:
        """限流失败轮换 provider 并通知换绑；无可用 provider 时抛错。"""
        replacement = rotator.rotate_bound_provider(
            provider_index,
            scope="rate_limit",
            capability="reviewer",
        )
        if replacement is None:
            if self._on_provider_exhausted is not None:
                await self._on_provider_exhausted()
            raise RuntimeError("reviewer has no available provider") from exc
        self._provider_index = replacement
        call_model = rotator.reviewer_model(replacement, model)
        if self._on_provider_change is not None:
            await self._on_provider_change(replacement, call_model)
        return call_model


class FakeReviewerRunner(ReviewerRunner):
    def __init__(self) -> None:
        self.responses: dict[str, str] = {}
        self.call_log: list[tuple[str, Path, str, float]] = []
        self.thinking_calls: list[dict[str, int | str]] = []

    async def _run(self, request: _ReviewRequest) -> str:
        self.call_log.append(
            (request.prompt, request.sandbox_root, request.model, request.timeout_seconds)
        )
        self.thinking_calls.append(
            {
                "thinking_budget_tokens": request.thinking_budget_tokens,
                "max_tokens": request.max_tokens,
                "thinking_mode": request.thinking_mode,
            }
        )
        for substring, response in self.responses.items():
            if substring in request.prompt:
                return response
        raise TimeoutError("FakeReviewerRunner: no response programmed for this prompt")
