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

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from workflow_controller.core.auth_rotator import AuthRotator
from workflow_controller.core.auth_rotator.persistence import (
    AuthProvider,
    AuthProviderType,
    KeyStatus,
)
from workflow_controller.core.reviewer_runner import (
    DriverReviewerRunner,
    FakeReviewerRunner,
)
from workflow_controller.driver.anthropic_http import AnthropicHttpClient, HttpResult

if TYPE_CHECKING:
    from pathlib import Path


class TestFakeReviewerRunner:
    async def test_returns_response_for_matching_substring(self, tmp_path: Path) -> None:
        runner = FakeReviewerRunner()
        runner.responses["Phase B"] = '{"decision":"APPROVE","feedback_to_worker":"ok"}'

        result = await runner.run(
            prompt="reviewing Phase B output",
            sandbox_root=tmp_path,
            model="claude-sonnet-4-6",
            timeout_seconds=10,
        )

        assert "APPROVE" in result
        assert len(runner.call_log) == 1
        prompt, root, model, timeout = runner.call_log[0]
        assert prompt == "reviewing Phase B output"
        assert root == tmp_path
        assert model == "claude-sonnet-4-6"
        assert timeout == 10

    async def test_no_match_raises_timeout(self, tmp_path: Path) -> None:
        runner = FakeReviewerRunner()

        with pytest.raises(TimeoutError):
            await runner.run(
                prompt="something else",
                sandbox_root=tmp_path,
                model="x",
                timeout_seconds=1,
            )


@pytest.mark.asyncio
async def test_http_failure_invalidates_only_reviewer_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failed_request(self, prompt, **kwargs):
        return HttpResult(text="", is_error=True, error="HTTP 403: invalid key")

    monkeypatch.setattr(AnthropicHttpClient, "request", failed_request)
    rotator = MagicMock()
    rotator.resolve_available_provider.return_value = 0
    rotator.reviewer_model.return_value = "model"
    rotator.env_for_driver.return_value = {"ANTHROPIC_API_KEY": "sk"}
    runner = DriverReviewerRunner(provider_index=0)
    runner.set_auth_rotator(rotator)

    with pytest.raises(RuntimeError, match="HTTP 403"):
        await runner.run("review", tmp_path, "model", 1)

    rotator.invalidate_provider_capability.assert_called_once_with(
        0,
        "reviewer",
        "reviewer HTTP error: HTTP 403: invalid key",
    )


@pytest.mark.asyncio
async def test_transient_http_failure_does_not_invalidate_reviewer_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failed_request(self, prompt, **kwargs):
        return HttpResult(text="", is_error=True, error="HTTP 529: overloaded")

    monkeypatch.setattr(AnthropicHttpClient, "request", failed_request)
    rotator = MagicMock()
    rotator.resolve_available_provider.return_value = 0
    rotator.reviewer_model.return_value = "model"
    rotator.env_for_driver.return_value = {"ANTHROPIC_API_KEY": "sk"}
    runner = DriverReviewerRunner(provider_index=0)
    runner.set_auth_rotator(rotator)

    with pytest.raises(RuntimeError, match="HTTP 529"):
        await runner.run("review", tmp_path, "model", 1)

    rotator.invalidate_provider_capability.assert_not_called()


@pytest.mark.asyncio
async def test_empty_reviewer_response_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def empty_request(self, prompt, **kwargs):
        return HttpResult(text=" ", is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", empty_request)

    with pytest.raises(RuntimeError, match="response has no text content"):
        await DriverReviewerRunner().run("review", tmp_path, "model", 1)


@pytest.mark.asyncio
async def test_runner_passes_thinking_mode_to_http(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = {}

    async def request(self, prompt, **kwargs):
        captured.update(kwargs)
        return HttpResult(text='{"action":"let_exit"}', is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", request)
    runner = DriverReviewerRunner()
    await runner.run(
        "review",
        tmp_path,
        "model",
        1,
        thinking_mode="enabled",
        thinking_budget_tokens=4096,
        max_tokens=8192,
    )
    assert captured["thinking_type"] == "enabled"
    assert captured["thinking_budget_tokens"] == 4096
    assert captured["max_tokens"] == 8192
    assert "temperature" not in captured


@pytest.mark.asyncio
async def test_runner_clamps_low_max_tokens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = {}

    async def request(self, prompt, **kwargs):
        captured.update(kwargs)
        return HttpResult(text='{"action":"let_exit"}', is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", request)
    await DriverReviewerRunner().run(
        "review",
        tmp_path,
        "model",
        1,
        max_tokens=512,
    )
    assert captured["max_tokens"] == 2048


@pytest.mark.asyncio
async def test_runner_uses_provider_thinking_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = {}

    async def request(self, prompt, **kwargs):
        captured.update(kwargs)
        return HttpResult(text='{"action":"let_exit"}', is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", request)
    rotator = MagicMock()
    rotator.resolve_available_provider.return_value = 0
    rotator.reviewer_model.return_value = "model"
    rotator.env_for_driver.return_value = {"ANTHROPIC_API_KEY": "sk"}
    rotator.reviewer_thinking_type.return_value = "adaptive"
    runner = DriverReviewerRunner(provider_index=0)
    runner.set_auth_rotator(rotator)

    await runner.run("review", tmp_path, "model", 1, thinking_mode="enabled")

    assert captured["thinking_type"] == "adaptive"
    rotator.reviewer_thinking_type.assert_called_once_with(
        0,
        requested_mode="enabled",
    )


@pytest.mark.asyncio
async def test_runner_default_disables_thinking(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}

    async def request(self, prompt, **kwargs):
        captured.update(kwargs)
        return HttpResult(text='{"action":"let_exit"}', is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", request)
    await DriverReviewerRunner().run("review", tmp_path, "model", 1)
    assert captured["thinking_type"] == "disabled"


@pytest.mark.asyncio
async def test_reviewer_rebuilds_anthropic_env_from_rotator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex Worker 过滤环境不影响 Reviewer 重建 Anthropic 配置。"""
    captured: dict[str, object] = {}

    async def fake_post(base_url, auth_headers, body, timeout):
        captured.update(
            base_url=base_url,
            auth_headers=auth_headers,
            body=body,
            timeout=timeout,
        )
        return HttpResult(text="OK", is_error=False, error="")

    monkeypatch.setattr(
        "workflow_controller.driver.anthropic_http._default_post",
        fake_post,
    )
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "https://filtered.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "filtered-key")
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "filtered-token")
    monkeypatch.setenv("ANTHROPIC_MODEL", "filtered-model")
    rotator = AuthRotator(
        state_path=tmp_path / "auth_state.json",
        env_path=tmp_path / ".env",
    )
    getattr(rotator, "_providers").append(
        AuthProvider(
            provider_type=AuthProviderType.API_KEY,
            status=KeyStatus.UNUSED,
            key="sk-reviewer",
            model="glm-5.3",
            base_url="https://anthropic.example",
            runtime="codex_cli",
        )
    )
    runner = DriverReviewerRunner(provider_index=0)
    runner.set_auth_rotator(rotator)

    result = await runner.run("review", tmp_path, "glm-5.3", 8)

    assert result == "OK"
    assert captured["base_url"] == "https://anthropic.example"
    assert captured["auth_headers"] == {
        "Authorization": "Bearer sk-reviewer",
    }
    assert captured["body"]["model"] == "glm-5.3"
