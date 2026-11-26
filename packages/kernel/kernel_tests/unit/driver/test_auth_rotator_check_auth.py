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

"""check_auth — lightweight API-key health probe.

check_auth tests the active key via AnthropicHttpClient (a single raw
POST /v1/messages), NOT the worker's agent-loop driver — keeps /auth/test
fast and runtime-agnostic (same path for cc and opencode, native key or
third-party endpoint).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.auth_rotator import AuthRotator
from workflow_controller.core.auth_rotator.persistence import KeyStatus
from workflow_controller.driver.anthropic_http import AnthropicHttpClient, HttpResult

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def rotator(tmp_path: Path) -> AuthRotator:
    env_file = tmp_path / ".env"
    env_file.write_text("ANTHROPIC_API_KEY=sk-test\n", encoding="utf-8")
    return AuthRotator(state_path=tmp_path / "state.json", env_path=env_file)


@pytest.mark.asyncio
async def test_check_auth_valid_key_uses_anthropic_http(
    rotator: AuthRotator,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """check_auth 的 API key 测试走 AnthropicHttpClient（轻量裸 POST），不走 worker driver。"""
    from workflow_controller.driver import factory as driver_factory

    async def fake_request(self, prompt, *, env=None, **kw):
        return HttpResult(text="ok", is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", fake_request)

    # 守卫：API key 测试必须走 AnthropicHttpClient；若误用 create_driver（worker
    # driver），boom 让测试显式失败。
    def boom(runtime, **kw):
        raise AssertionError("check_auth should use AnthropicHttpClient, not create_driver")

    monkeypatch.setattr(driver_factory, "create_driver", boom)

    ok, err = await rotator.check_auth()
    assert ok is True
    assert err is None


@pytest.mark.asyncio
async def test_check_auth_invalid_key_reports_error(
    rotator: AuthRotator,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AnthropicHttpClient 返 error HttpResult（如 401）→ check_auth 报失败 + 错误文本。"""

    async def fake_request(self, prompt, *, env=None, **kw):
        return HttpResult(text="", is_error=True, error="HTTP 401: Unauthorized")

    monkeypatch.setattr(AnthropicHttpClient, "request", fake_request)

    ok, err = await rotator.check_auth()
    assert ok is False
    assert "401" in (err or "")


@pytest.mark.asyncio
async def test_check_auth_success_restores_auth_error_invalid(
    rotator: AuthRotator,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """check_auth 成功后清除 auth_error 造成的全局 INVALID 状态。"""
    (rotator._config_dir / "settings.conf").write_text("api_keys:\n  - sk-test\n", encoding="utf-8")
    rotator.sync_providers_from_config()
    provider = rotator.get_provider(0)
    provider.status = KeyStatus.INVALID
    provider.exhausted_at = datetime.now(tz=UTC)
    provider.reset_at = datetime.now(tz=UTC) + timedelta(hours=1)
    provider.scope = "auth_error"

    async def fake_request(self, prompt, *, env=None, **kwargs):
        return HttpResult(text="ok", is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", fake_request)

    ok, err = await rotator.check_auth()

    assert ok is True
    assert err is None
    assert provider.status is KeyStatus.ACTIVE
    assert provider.exhausted_at is None
    assert provider.reset_at is None
    assert provider.scope is None


@pytest.mark.asyncio
async def test_check_auth_no_key_returns_no_api_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """无 provider 且无 env key → 'No API key configured'（不进 driver 测试）。

    清 host env 的 ANTHROPIC_*：本机带 GLM token 会污染 check_auth 的 has-key 判断。
    """
    for k in ("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "ANTHROPIC_MODEL"):
        monkeypatch.delenv(k, raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    rotator = AuthRotator(state_path=tmp_path / "state.json", env_path=env_file)

    ok, err = await rotator.check_auth()
    assert ok is False
    assert "No API key" in (err or "")


def _capture_model(monkeypatch: pytest.MonkeyPatch, captured: dict) -> None:
    """把 AnthropicHttpClient.request 收到的 model 参数捕获进 captured。"""

    async def fake_request(self, prompt, *, model="", env=None, **kw):
        captured["model"] = model
        return HttpResult(text="ok", is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", fake_request)


def _capture_model_rotator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runtime: str = "claude_sdk",
) -> tuple[AuthRotator, dict]:
    """建空 .env 的 rotator 并挂模型捕获桩；返回 (rotator, captured)。"""
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    rotator = AuthRotator(
        state_path=tmp_path / "state.json",
        env_path=env_file,
        runtime=runtime,
    )
    captured: dict = {}
    _capture_model(monkeypatch, captured)
    return rotator, captured


@pytest.mark.asyncio
async def test_check_auth_opencode_runtime_strips_provider_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """runtime=opencode：provider/model 的前缀是 opencode CLI 路由标记，直连探测剥离。"""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("ANTHROPIC_MODEL", "zhipuai-coding-plan/glm-5.2")
    rotator, captured = _capture_model_rotator(tmp_path, monkeypatch, runtime="opencode")

    ok, _ = await rotator.check_auth()
    assert ok is True
    assert captured["model"] == "glm-5.2"


@pytest.mark.asyncio
async def test_check_auth_non_opencode_runtime_keeps_slashed_model(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """claude_sdk 等 runtime：合法含 / 的模型 ID（OpenRouter 风格）原样直测。"""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("ANTHROPIC_MODEL", "anthropic/claude-sonnet-4")
    rotator, captured = _capture_model_rotator(tmp_path, monkeypatch)

    ok, _ = await rotator.check_auth()
    assert ok is True
    assert captured["model"] == "anthropic/claude-sonnet-4"


@pytest.mark.asyncio
async def test_test_key_opencode_runtime_strips_provider_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """test_key 同 check_auth：仅 opencode runtime 剥 provider 路由前缀。"""
    rotator, captured = _capture_model_rotator(tmp_path, monkeypatch, runtime="opencode")

    ok, _, _ = await rotator.test_key(
        "sk-test",
        "zhipuai-coding-plan/glm-5.2",
        "https://open.bigmodel.cn/api/anthropic",
    )
    assert ok is True
    assert captured["model"] == "glm-5.2"


@pytest.mark.asyncio
async def test_test_key_non_opencode_runtime_keeps_slashed_model(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """test_key：非 opencode runtime 的含 / 模型 ID 原样直测。"""
    rotator, captured = _capture_model_rotator(tmp_path, monkeypatch)

    ok, _, _ = await rotator.test_key(
        "sk-test",
        "anthropic/claude-sonnet-4",
        "https://openrouter.example/api",
    )
    assert ok is True
    assert captured["model"] == "anthropic/claude-sonnet-4"


@pytest.mark.asyncio
async def test_test_key_explicit_opencode_runtime_overrides_global(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """显式 runtime=opencode 覆盖启动时全局 claude_sdk——「默认 SDK、单会话
    OpenCode」配置下 /auth/test-key 按用户所选 runtime 剥前缀，而非全局值。
    """
    rotator, captured = _capture_model_rotator(tmp_path, monkeypatch)

    ok, _, _ = await rotator.test_key(
        "sk-test",
        "zhipuai-coding-plan/glm-5.2",
        "https://open.bigmodel.cn/api/anthropic",
        runtime="opencode",
    )
    assert ok is True
    assert captured["model"] == "glm-5.2"


@pytest.mark.asyncio
async def test_test_key_explicit_non_opencode_runtime_overrides_global(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """显式 claude_sdk 覆盖启动时全局 opencode——含 / 模型 ID 原样直测。"""
    rotator, captured = _capture_model_rotator(tmp_path, monkeypatch, runtime="opencode")

    ok, _, _ = await rotator.test_key(
        "sk-test",
        "anthropic/claude-sonnet-4",
        "https://openrouter.example/api",
        runtime="claude_sdk",
    )
    assert ok is True
    assert captured["model"] == "anthropic/claude-sonnet-4"


@pytest.mark.asyncio
async def test_check_auth_explicit_runtime_overrides_global(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """check_auth 同 test_key：显式 runtime 优先于启动时全局值。"""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("ANTHROPIC_MODEL", "zhipuai-coding-plan/glm-5.2")
    rotator, captured = _capture_model_rotator(tmp_path, monkeypatch)

    ok, _ = await rotator.check_auth(runtime="opencode")
    assert ok is True
    assert captured["model"] == "glm-5.2"


def _rotator_with_saved_runtime(tmp_path: Path, runtime: str | None) -> AuthRotator:
    """全局 claude_sdk 的 rotator，settings.conf 存一条带 runtime 的 provider。"""
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    rotator = AuthRotator(state_path=tmp_path / "state.json", env_path=env_file)
    entry: dict[str, str] = {
        "key": "sk-test",
        "model": "zhipuai-coding-plan/glm-5.2",
        "base_url": "https://open.bigmodel.cn/api/anthropic",
    }
    if runtime:
        entry["runtime"] = runtime
    rotator.set_keys([entry])
    return rotator


@pytest.mark.asyncio
async def test_check_auth_provider_saved_runtime_strips_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """runtime 随密钥保存后，不显式传 runtime 的健康检查也按它剥前缀。

    「默认全局 claude_sdk、provider 只给 opencode 会话用」——App mount 的
    /auth/test 空 POST 不带 runtime，靠 provider 持久化的 runtime 剥前缀，
    否则前缀模型直发 API 失败 → 误标 exhausted → 禁用新建会话。
    """
    rotator = _rotator_with_saved_runtime(tmp_path, "opencode")
    captured: dict = {}
    _capture_model(monkeypatch, captured)

    ok, _ = await rotator.check_auth()
    assert ok is True
    assert captured["model"] == "glm-5.2"


@pytest.mark.asyncio
async def test_check_auth_provider_saved_runtime_beats_global(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """provider 保存的 runtime=claude_sdk 优先于启动时全局 opencode——模型
    ID 格式归 provider 自己定，含 / 的 claude 侧 ID 原样直测。
    """
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    rotator = AuthRotator(
        state_path=tmp_path / "state.json",
        env_path=env_file,
        runtime="opencode",
    )
    rotator.set_keys(
        [
            {
                "key": "sk-test",
                "model": "anthropic/claude-sonnet-4",
                "base_url": "https://openrouter.example/api",
                "runtime": "claude_sdk",
            }
        ]
    )
    captured: dict = {}
    _capture_model(monkeypatch, captured)

    ok, _ = await rotator.check_auth()
    assert ok is True
    assert captured["model"] == "anthropic/claude-sonnet-4"


@pytest.mark.asyncio
async def test_check_auth_provider_saved_runtime_beats_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """provider 保存的 runtime 优先于调用方显式传入的冲突 runtime。"""
    rotator = _rotator_with_saved_runtime(tmp_path, "opencode")
    captured: dict = {}
    _capture_model(monkeypatch, captured)

    ok, _ = await rotator.check_auth(runtime="claude_sdk")
    assert ok is True
    assert captured["model"] == "glm-5.2"


@pytest.mark.asyncio
async def test_check_auth_provider_without_runtime_falls_back_to_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """未保存 runtime 的 provider：显式传入（会话上下文）仍生效。"""
    rotator = _rotator_with_saved_runtime(tmp_path, None)
    captured: dict = {}
    _capture_model(monkeypatch, captured)

    ok, _ = await rotator.check_auth(runtime="opencode")
    assert ok is True
    assert captured["model"] == "glm-5.2"


@pytest.mark.asyncio
async def test_check_auth_provider_without_runtime_keeps_slashed_model(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """未保存 runtime 的 provider：无显式 runtime → 回退全局 claude_sdk，
    含 / 模型 ID 原样直测（不剥）。
    """
    rotator = _rotator_with_saved_runtime(tmp_path, None)
    captured: dict = {}
    _capture_model(monkeypatch, captured)

    ok, _ = await rotator.check_auth()
    assert ok is True
    assert captured["model"] == "zhipuai-coding-plan/glm-5.2"


def test_get_api_keys_for_ui_includes_runtime(tmp_path: Path) -> None:
    """GET /auth/keys 回传 runtime，前端 loadAuth/syncKeys 才能完整回环。"""
    rotator = _rotator_with_saved_runtime(tmp_path, "opencode")
    keys = rotator.get_api_keys_for_ui()
    assert keys[0]["runtime"] == "opencode"
    assert keys[0]["provider_index"] == 0


def test_get_api_keys_for_ui_skips_blank_key_before_indexing(tmp_path: Path) -> None:
    """空白 key 不进入 provider 序号，避免状态表与编辑表错位。"""
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    (tmp_path / "settings.conf").write_text(
        'api_keys:\n- key: " "\n- key: sk-valid\n',
        encoding="utf-8",
    )
    rotator = AuthRotator(state_path=tmp_path / "state.json", env_path=env_file)
    keys = rotator.get_api_keys_for_ui()
    assert len(keys) == 1
    assert keys[0]["key"] == "sk-valid"
    assert keys[0]["provider_index"] == 0
