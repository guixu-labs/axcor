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

import json
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.auth_rotator import (
    AuthProvider,
    AuthProviderType,
    AuthRotator,
    KeyStatus,
    ProviderValidation,
    ValidationStatus,
)
from workflow_controller.driver.anthropic_http import AnthropicHttpClient, HttpResult
from workflow_controller.session.events import Event, EventType

if TYPE_CHECKING:
    from pathlib import Path


class _ProbeDriver:
    def __init__(self, captured: dict, event_type: EventType = EventType.STEP_FINISH) -> None:
        self.captured = captured
        self.event_type = event_type

    async def query(self, prompt: str, **kwargs):
        self.captured["query"] = kwargs
        if self.event_type == EventType.STEP_FINISH:
            yield Event.create(
                session_id="probe",
                iteration=0,
                event_type=EventType.STEP_FINISH,
                data={"is_error": False, "result": "OK"},
            )
        else:
            yield Event.create(
                session_id="probe",
                iteration=0,
                event_type=self.event_type,
                data={"error": {"data": {"statusCode": 401}}},
            )

    async def stop(self) -> None:
        self.captured["stopped"] = True

    async def kill(self) -> None:
        self.captured["killed"] = True


class _NoTerminalDriver(_ProbeDriver):
    async def query(self, prompt: str, **kwargs):
        self.captured["query"] = kwargs
        if False:
            yield


class _CodexProbeDriver:
    def __init__(self, captured: dict) -> None:
        self.captured = captured

    async def query(self, prompt: str, **kwargs):
        self.captured["query"] = kwargs
        for event_type in (
            EventType.ASSISTANT_MESSAGE,
            EventType.STEP_FINISH,
            EventType.ITERATION_END,
        ):
            self.captured.setdefault("events", []).append(event_type)
            data = {"is_error": False} if event_type == EventType.STEP_FINISH else {}
            yield Event.create(
                session_id="probe",
                iteration=0,
                event_type=event_type,
                data=data,
            )

    async def stop(self) -> None:
        self.captured["stopped"] = True

    async def kill(self) -> None:
        self.captured["killed"] = True


def _rotator(tmp_path: Path) -> AuthRotator:
    return AuthRotator(
        state_path=tmp_path / "auth_state.json",
        env_path=tmp_path / ".env",
    )


def _codex_rotator(tmp_path: Path) -> AuthRotator:
    """runtime=codex_cli 的独立 AuthRotator（codex 目录注入路径用）。"""
    return AuthRotator(
        state_path=tmp_path / "auth_state.json",
        env_path=tmp_path / ".env",
        runtime="codex_cli",
    )


async def _set_custom_codex_key(rotator: AuthRotator) -> None:
    await rotator.set_keys_validated(
        [
            {
                "key": "sk",
                "model": "custom-model",
                "base_url": "https://relay.test/v1",
                "runtime": "codex_cli",
            }
        ]
    )


def _counting_probe_env(monkeypatch: pytest.MonkeyPatch, http_ok: bool = True) -> dict:
    """打上计数 worker/reviewer 探活的桩；返回计数器 calls。

    http_ok=False 时 reviewer 探活改返回 403 失败。
    """
    calls = {"worker": 0, "reviewer": 0}

    def fake_create(runtime, **kwargs):
        calls["worker"] += 1
        return _ProbeDriver({})

    async def fake_http(self, prompt, **kwargs):
        calls["reviewer"] += 1
        if http_ok:
            return HttpResult(text="OK", is_error=False, error="")
        return HttpResult(text="", is_error=True, error="HTTP 403")

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AnthropicHttpClient, "request", fake_http)
    return calls


def _codex_preset_probe_env(monkeypatch: pytest.MonkeyPatch) -> dict:
    """清空 bundled slugs 并注入 _CodexProbeDriver；返回 captured。"""
    captured: dict = {}
    monkeypatch.setattr(
        "workflow_controller.driver.codex_catalog._bundled_model_slugs",
        lambda: frozenset(),
    )
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: _CodexProbeDriver(captured),
    )
    return captured


def _opencode_override_probe_env(monkeypatch: pytest.MonkeyPatch) -> tuple[dict, dict]:
    """WORKFLOW_CONTROLLER_DRIVER=opencode 覆盖 + kind 探测桩；返回 (captured, calls)。"""
    captured: dict = {}
    calls = {"probe": 0}

    def fake_create(runtime, **kwargs):
        captured["runtime"] = runtime
        return _ProbeDriver(captured)

    async def fake_probe(self, key, base_url, model=""):
        calls["probe"] += 1
        return "@ai-sdk/openai-compatible"

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AuthRotator, "probe_provider_kind", fake_probe)
    monkeypatch.setenv("WORKFLOW_CONTROLLER_DRIVER", "opencode")
    return captured, calls


def _kind_probing_env(monkeypatch: pytest.MonkeyPatch, probe_kind) -> dict:
    """打上 worker 计数 driver 与 probe_kind(base_url)->kind 探测桩；返回 calls。"""
    calls = {"worker": 0, "probe": 0}

    def fake_create(runtime, **kwargs):
        calls["worker"] += 1
        return _ProbeDriver({})

    async def fake_probe(self, key, base_url, model=""):
        calls["probe"] += 1
        return probe_kind(base_url)

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AuthRotator, "probe_provider_kind", fake_probe)
    return calls


@pytest.fixture(autouse=True)
def _reviewer_probe_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_http(self, prompt, **kwargs):
        return HttpResult(text="OK", is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", fake_http)


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime", ["claude_sdk", "claude_cli", "opencode"])
async def test_candidate_probe_uses_selected_real_driver_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runtime: str,
) -> None:
    captured: dict = {}

    def fake_create(selected: str, **kwargs):
        captured["runtime"] = selected
        return _ProbeDriver(captured)

    async def fake_http(self, prompt, **kwargs):
        captured["reviewer_model"] = kwargs.get("model")
        return HttpResult(text="OK", is_error=False, error="")

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AnthropicHttpClient, "request", fake_http)

    model = "route/glm-5.3" if runtime == "opencode" else "glm-5.3"
    result = await _rotator(tmp_path).validate_candidate(
        "sk-test",
        model,
        "https://example.test/v1",
        runtime,
    )

    assert result["success"] is True
    assert result["worker"]["status"] == "valid"
    assert result["reviewer"]["status"] == "valid"
    assert captured["runtime"] == runtime
    assert captured["query"]["model"] == model
    assert captured["query"]["allowed_tools"] == ["Read"]
    assert captured["reviewer_model"] == ("glm-5.3" if runtime == "opencode" else model)
    assert captured["stopped"] and captured["killed"]


@pytest.mark.asyncio
async def test_worker_probe_disables_claude_workspace_trust_pre_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """claude_cli worker 探测的 cwd 是内部 scratch 临时目录而非用户选定的
    workspace：query 显式关闭信任预写，避免每次校验把临时目录条目累积
    写进用户全局 Claude 配置（探测后目录即删，条目永久残留）。
    """
    captured: dict = {}

    def fake_create(runtime, **_kwargs):
        return _ProbeDriver(captured)

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    result = await _rotator(tmp_path).validate_candidate(
        "sk",
        "m",
        "",
        "claude_cli",
    )

    assert result["success"] is True
    assert captured["query"]["pre_trust"] is False


@pytest.mark.asyncio
async def test_probe_payload_omits_none_optional_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """非 codex/opencode 候选：catalog/kind 可选字段为 None 时不返回 key，
    供 typed 响应（exclude_unset）真正省略这些字段。"""
    _counting_probe_env(monkeypatch)
    result = await _rotator(tmp_path).validate_candidate("sk", "m", "", "claude_sdk")

    assert "catalog_status" not in result
    assert "model_catalog_name" not in result
    assert "model_catalog_path" not in result
    assert "provider_kind" not in result
    assert result["worker"]["status"] == "valid"


@pytest.mark.asyncio
async def test_codex_worker_probe_waits_for_iteration_end(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex 探测等待子进程收尾，避免 scratch 目录与 rollout 写入竞争。"""
    captured: dict = {}
    monkeypatch.setattr(
        "workflow_controller.driver.codex_catalog._bundled_model_slugs",
        lambda: {"gpt-test"},
    )

    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: _CodexProbeDriver(captured),
    )
    result = await _rotator(tmp_path).validate_candidate(
        "sk-test",
        "gpt-test",
        "https://example.test/v1",
        "codex_cli",
    )

    assert result["success"] is True
    assert captured["events"] == [
        EventType.ASSISTANT_MESSAGE,
        EventType.STEP_FINISH,
        EventType.ITERATION_END,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["glm-5.3", "glm-5.2"])
async def test_codex_preset_catalog_is_installed_and_injected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model: str,
) -> None:
    captured = _codex_preset_probe_env(monkeypatch)

    async def fake_reviewer_request(self, prompt: str, **kwargs) -> HttpResult:
        """Return a successful reviewer probe without network access."""

        return HttpResult(text="OK", is_error=False, error="")

    monkeypatch.setattr(
        AnthropicHttpClient,
        "request",
        fake_reviewer_request,
    )
    rotator = _rotator(tmp_path)

    result = await rotator.validate_candidate(
        "sk",
        model,
        "https://open.bigmodel.cn/api/v1",
        "codex_cli",
        force=True,
    )

    assert result["catalog_status"] == "preset"
    assert result["model_catalog_name"] == "Zhipu GLM"
    from pathlib import Path

    catalog_path = Path(result["model_catalog_path"])
    assert catalog_path.is_file()
    assert captured["query"]["env"]["model_catalog_path"] == str(catalog_path)


@pytest.mark.asyncio
async def test_set_keys_validated_persists_preset_catalog_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _codex_preset_probe_env(monkeypatch)
    rotator = _codex_rotator(tmp_path)

    await rotator.set_keys_validated(
        [
            {
                "key": "sk",
                "model": "glm-5.3",
                "base_url": "https://open.bigmodel.cn/api/v1",
                "runtime": "codex_cli",
            }
        ]
    )

    path = rotator.get_provider(0).model_catalog_path
    assert path is not None and path.endswith(".json")
    from workflow_controller.core.auth_rotator.settings import GlobalSettings

    entry = GlobalSettings.load(tmp_path / "settings.conf").api_keys[0]
    assert entry["model_catalog_path"] == path


@pytest.mark.asyncio
async def test_unknown_codex_provider_requires_catalog_before_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_create(*_args, **_kwargs):
        raise AssertionError("unknown Codex provider must not spawn")

    monkeypatch.setattr(
        "workflow_controller.driver.codex_catalog._bundled_model_slugs",
        lambda: frozenset(),
    )
    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fail_create)
    rotator = _rotator(tmp_path)

    result = await rotator.validate_candidate(
        "sk",
        "custom-model",
        "https://relay.test/v1",
        "codex_cli",
        force=True,
    )

    assert result["catalog_status"] == "custom_required"
    assert result["success"] is True
    assert result["worker"]["status"] == "invalid"
    assert result["reviewer"]["status"] == "valid"
    assert result["error"] == (
        "请配置第三方 provider 官方推荐的 model catalog JSON 文件，并将所在绝对路径输入设置中"
    )


@pytest.mark.asyncio
async def test_invalid_codex_catalog_fails_preflight_before_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_create(*_args, **_kwargs):
        raise AssertionError("invalid Codex catalog must not spawn")

    monkeypatch.setattr(
        "workflow_controller.driver.codex_catalog._bundled_model_slugs",
        lambda: frozenset(),
    )
    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fail_create)
    catalog = tmp_path / "models.json"
    catalog.write_text('{"models":[]}', encoding="utf-8")

    result = await _rotator(tmp_path).validate_candidate(
        "sk",
        "custom-model",
        "https://relay.test/v1",
        "codex_cli",
        model_catalog_path=str(catalog),
        force=True,
    )

    assert result["catalog_status"] == "custom_invalid"
    assert result["success"] is True
    assert result["worker"]["status"] == "invalid"
    assert result["reviewer"]["status"] == "valid"
    assert result["error"] == ("请输入合法且包含正确模型名的 model catalog JSON 文件路径")


@pytest.mark.asyncio
async def test_codex_catalog_preflight_runs_off_event_loop_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from workflow_controller.driver.codex_catalog import CodexCatalogResolution

    resolver_thread: threading.Thread | None = None

    def fake_resolve(**_kwargs):
        nonlocal resolver_thread
        resolver_thread = threading.current_thread()
        return CodexCatalogResolution(status="custom_required")

    monkeypatch.setattr(
        "workflow_controller.driver.codex_catalog.resolve_codex_catalog",
        fake_resolve,
    )

    result = await _rotator(tmp_path).validate_candidate(
        "sk",
        "custom-model",
        "https://relay.test/v1",
        "codex_cli",
    )

    assert result["catalog_status"] == "custom_required"
    assert resolver_thread is not None
    assert resolver_thread is not threading.main_thread()


@pytest.mark.asyncio
async def test_codex_gateway_model_conversion_failure_reaches_validation_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from workflow_controller.driver.codex_cli import _codex_line_to_events

    class GatewayUnsupportedDriver:
        async def query(self, prompt: str, **kwargs):
            line = json.dumps(
                {
                    "type": "turn.failed",
                    "error": {
                        "message": (
                            '{"error":{"message":"not implemented",'
                            '"type":"new_api_error","code":"convert_request_failed"}}'
                        )
                    },
                }
            )
            for event in _codex_line_to_events(line, "probe", 0):
                yield event

        async def stop(self) -> None:
            return None

        async def kill(self) -> None:
            return None

    catalog = tmp_path / "codex-models.json"
    catalog.write_text('{"models":[{"slug":"claude-opus-5-5"}]}', encoding="utf-8")

    async def resolve_catalog(**_kwargs):
        return "preset", str(catalog), "YibuAPI Claude Opus"

    monkeypatch.setattr(
        "workflow_controller.core.auth_rotator._resolve_codex_catalog_entry",
        resolve_catalog,
    )
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: GatewayUnsupportedDriver(),
    )

    async def reviewer_ok(self, prompt: str, **kwargs):
        return HttpResult(text="OK", is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", reviewer_ok)

    result = await _rotator(tmp_path).validate_candidate(
        "sk-test",
        "claude-opus-5-5",
        "https://yibuapi.com",
        "codex_cli",
        force=True,
    )

    assert result["success"] is True
    assert result["worker"]["status"] == "invalid"
    assert result["worker"]["error"] == (
        "gateway does not support this model with OpenAI Responses format"
    )
    assert result["reviewer"]["status"] == "valid"


@pytest.mark.asyncio
async def test_codex_candidate_does_not_use_global_default_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_create(*_args, **_kwargs):
        raise AssertionError("Codex without provider base_url must not spawn")

    monkeypatch.setattr(
        "workflow_controller.driver.codex_catalog._bundled_model_slugs",
        lambda: frozenset(),
    )
    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fail_create)
    rotator = _rotator(tmp_path)
    rotator.set_global_settings(
        default_url="https://open.bigmodel.cn/api/v1",
    )

    result = await rotator.validate_candidate(
        "sk",
        "custom-model",
        "",
        "codex_cli",
        force=True,
    )

    assert result["catalog_status"] == "custom_required"
    assert result["success"] is True


@pytest.mark.asyncio
async def test_reviewer_failure_does_not_fail_worker_save_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: _ProbeDriver({}),
    )

    async def failed_http(self, prompt, **kwargs):
        return HttpResult(text="", is_error=True, error="HTTP 403: invalid key")

    monkeypatch.setattr(AnthropicHttpClient, "request", failed_http)
    result = await _rotator(tmp_path).validate_candidate(
        "sk-test",
        "glm-5.3",
        "https://example.test/v1",
        "claude_sdk",
    )

    assert result["success"] is True
    assert result["worker"]["status"] == "valid"
    assert result["reviewer"]["status"] == "invalid"


@pytest.mark.asyncio
async def test_reviewer_empty_response_is_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: _ProbeDriver({}, EventType.ERROR),
    )

    async def empty_http(self, prompt, **kwargs):
        return HttpResult(text="  ", is_error=False, error="")

    monkeypatch.setattr(AnthropicHttpClient, "request", empty_http)
    result = await _rotator(tmp_path).validate_candidate(
        "sk",
        "m",
        "https://example.test/v1",
        "claude_sdk",
    )

    assert result["success"] is False
    assert result["reviewer"]["status"] == "invalid"
    assert result["reviewer"]["error"] == "reviewer response has no text content"


@pytest.mark.asyncio
async def test_reviewer_probe_error_is_normalized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: _ProbeDriver({}),
    )
    long_error = f"HTTP 400: sk-secret {'x' * 600}"

    async def failed_http(self, prompt, **kwargs):
        return HttpResult(text="", is_error=True, error=long_error)

    monkeypatch.setattr(AnthropicHttpClient, "request", failed_http)
    result = await _rotator(tmp_path).validate_candidate(
        "sk-secret",
        "glm-5.3",
        "https://example.test/v1",
        "claude_sdk",
    )

    assert result["reviewer"]["error"] == "Reviewer request rejected (HTTP 400)."


@pytest.mark.asyncio
async def test_probe_cache_is_keyed_by_configuration_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _counting_probe_env(monkeypatch)

    rotator = _rotator(tmp_path)

    await rotator.validate_candidate("sk", "m1", "https://example.test/v1", "claude_sdk")
    await rotator.validate_candidate("sk", "m1", "https://example.test/v1", "claude_sdk")
    assert calls == {"worker": 1, "reviewer": 4}

    await rotator.validate_candidate("sk", "m2", "https://example.test/v1", "claude_sdk")
    assert calls == {"worker": 2, "reviewer": 8}

    await rotator.validate_candidate("sk-2", "m2", "https://example.test/v1", "claude_sdk")
    await rotator.validate_candidate("sk-2", "m2", "https://other.test/v1", "claude_sdk")
    await rotator.validate_candidate("sk-2", "m2", "https://other.test/v1", "opencode")
    assert calls == {"worker": 5, "reviewer": 20}


@pytest.mark.asyncio
async def test_reviewer_probe_tests_all_thinking_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    modes: list[str] = []
    enabled_budgets: list[int] = []
    requests: list[dict[str, object]] = []

    def fake_create(runtime, **kwargs):
        return _ProbeDriver({})

    async def fake_http(self, prompt, **kwargs):
        mode = str(kwargs.get("thinking_type"))
        modes.append(mode)
        requests.append(kwargs)
        if mode == "enabled":
            enabled_budgets.append(int(kwargs.get("thinking_budget_tokens", 0)))
        if mode in ("disabled", "enabled", "adaptive"):
            return HttpResult(text="", is_error=True, error=f"{mode} unsupported")
        return HttpResult(text="OK", is_error=False, error="")

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AnthropicHttpClient, "request", fake_http)
    rotator = _rotator(tmp_path)

    result = await rotator.validate_candidate(
        "sk",
        "m",
        "https://example.test/v1",
        "claude_sdk",
    )

    assert result["success"] is True
    assert modes == ["disabled", "enabled", "adaptive", "omitted"]
    assert enabled_budgets == [2048]
    assert all("temperature" not in request for request in requests)
    assert result["reviewer"]["thinking_mode"] == "omitted"
    assert result["reviewer"]["thinking_modes"] == ["omitted"]


def test_reviewer_thinking_type_maps_provider_capability(tmp_path: Path) -> None:
    rotator = _rotator(tmp_path)
    getattr(rotator, "_providers").append(
        AuthProvider(
            provider_type=AuthProviderType.API_KEY,
            key="sk",
            model="m",
            runtime="claude_sdk",
        )
    )
    validation = rotator.get_provider(0).reviewer_validation

    validation.thinking_mode = "disabled"
    validation.thinking_modes = ["disabled", "enabled", "adaptive", "omitted"]
    assert rotator.reviewer_thinking_type(0, requested_mode="disabled") == "disabled"
    assert rotator.reviewer_thinking_type(0, requested_mode="enabled") == "enabled"
    assert rotator.reviewer_thinking_type(0, requested_mode="missing") == "disabled"

    validation.thinking_mode = "adaptive"
    validation.thinking_modes = ["adaptive", "omitted"]
    assert rotator.reviewer_thinking_type(0, requested_mode="enabled") == "adaptive"
    assert rotator.reviewer_thinking_type(0, requested_mode="omitted") == "omitted"

    validation.thinking_mode = ""
    validation.thinking_modes = []
    assert rotator.reviewer_thinking_type(0, requested_mode="enabled") == "enabled"
    assert rotator.reviewer_thinking_type(0, requested_mode="omitted") == "omitted"


@pytest.mark.asyncio
async def test_reviewer_failure_prefers_protocol_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: _ProbeDriver({}),
    )

    async def failed_http(self, prompt, **kwargs):
        mode = str(kwargs.get("thinking_type"))
        if mode == "disabled":
            return HttpResult(
                text="",
                is_error=True,
                error="response is not Anthropic Messages format",
            )
        return HttpResult(
            text="",
            is_error=True,
            error=f"HTTP 400: {mode} invalid",
        )

    monkeypatch.setattr(AnthropicHttpClient, "request", failed_http)
    result = await _rotator(tmp_path).validate_candidate(
        "sk",
        "m",
        "https://example.test/v1",
        "claude_sdk",
    )

    assert result["reviewer"]["status"] == "invalid"
    assert result["reviewer"]["error"] == "response is not Anthropic Messages format"


@pytest.mark.asyncio
async def test_runtime_invalidation_evicts_successful_probe_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _counting_probe_env(monkeypatch)
    rotator = _rotator(tmp_path)
    key = {"key": "sk", "model": "m", "runtime": "claude_sdk"}

    await rotator.set_keys_validated([key])
    rotator.invalidate_provider_capability(0, "worker", "HTTP 401")
    result = await rotator.validate_candidate("sk", "m", "", "claude_sdk")

    assert result["success"] is True
    assert calls == {"worker": 2, "reviewer": 8}


@pytest.mark.asyncio
async def test_runtime_auth_rotation_evicts_successful_probe_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _counting_probe_env(monkeypatch)
    rotator = _rotator(tmp_path)
    key = {"key": "sk", "model": "m", "runtime": "claude_sdk"}

    await rotator.set_keys_validated([key])
    rotator.rotate_bound_provider(0, scope="auth_error", runtime="claude_sdk")
    result = await rotator.validate_candidate("sk", "m", "", "claude_sdk")

    assert result["success"] is True
    assert calls == {"worker": 2, "reviewer": 8}


@pytest.mark.asyncio
async def test_runtime_invalidation_caps_persisted_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rotator = _rotator(tmp_path)
    await rotator.set_keys_validated([{"key": "sk", "model": "m", "runtime": "claude_sdk"}])

    rotator.invalidate_provider_capability(0, "worker", "sk " + "x" * 20_000)

    saved = rotator.get_providers()[0].worker_validation.error
    assert saved is not None
    expected = "*** " + "x" * 20_000
    assert saved == expected[:10_000] + "..."

    rotator.rotate_bound_provider(
        0,
        scope="auth_error",
        capability="reviewer",
        error="sk " + "y" * 20_000,
    )
    reviewer_error = rotator.get_providers()[0].reviewer_validation.error
    assert reviewer_error is not None
    assert reviewer_error.startswith("*** ")
    assert len(reviewer_error) == 10_000 + 3


@pytest.mark.asyncio
async def test_worker_probe_error_is_capped_only_in_persisted_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = "sk-secret"
    worker_error = f"{key} " + "w" * 20_000

    class ErrorProbeDriver(_ProbeDriver):
        async def query(self, prompt: str, **kwargs):
            yield Event.create(
                session_id="probe",
                iteration=0,
                event_type=EventType.ERROR,
                data={"message": worker_error},
            )

    async def successful_http(self, prompt, **kwargs):
        return HttpResult(text="OK", is_error=False, error="")

    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: ErrorProbeDriver({}),
    )
    monkeypatch.setattr(AnthropicHttpClient, "request", successful_http)
    rotator = _rotator(tmp_path)
    entry = {"key": key, "model": "m", "runtime": "claude_sdk"}
    rotator.set_keys([entry])
    result = await rotator.validate_provider(0)
    full_error = result["worker"]["error"]

    saved = json.loads((tmp_path / "auth_state.json").read_text())
    assert len(full_error) > 20_000
    assert key not in full_error
    assert saved["providers"][0]["worker_validation"]["error"] == full_error[:10_000] + "..."


@pytest.mark.asyncio
async def test_failed_probe_is_retried_instead_of_cached(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def failed_driver(runtime: str, **kwargs):
        nonlocal calls
        calls += 1
        raise FileNotFoundError("driver missing")

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", failed_driver)
    rotator = _rotator(tmp_path)

    await rotator.validate_candidate("sk", "m", "", "claude_sdk")
    await rotator.validate_candidate("sk", "m", "", "claude_sdk")

    assert calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime", ["claude_sdk", "claude_cli", "opencode"])
@pytest.mark.parametrize("event_type", [EventType.ERROR, EventType.RATE_LIMIT])
async def test_worker_probe_rejects_driver_error_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runtime: str,
    event_type: EventType,
) -> None:
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda selected, **kwargs: _ProbeDriver({}, event_type),
    )
    result = await _rotator(tmp_path).validate_candidate("sk", "m", "", runtime)

    assert result["success"] is True
    assert result["worker"]["status"] == "invalid"
    assert "401" in result["worker"]["error"]


@pytest.mark.asyncio
async def test_worker_probe_rejects_missing_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_driver(runtime: str, **kwargs):
        raise FileNotFoundError("opencode executable not found")

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", missing_driver)
    result = await _rotator(tmp_path).validate_candidate("sk", "route/m", "", "opencode")

    assert result["success"] is True
    assert "executable not found" in result["worker"]["error"]


@pytest.mark.asyncio
async def test_worker_probe_requires_successful_terminal_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: _NoTerminalDriver({}),
    )
    result = await _rotator(tmp_path).validate_candidate("sk", "m", "", "claude_sdk")

    assert result["success"] is True
    assert "without a successful step" in result["worker"]["error"]


@pytest.mark.asyncio
async def test_worker_probe_timeout_is_invalid_and_cleans_up(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    async def immediate_timeout(awaitable, timeout):
        awaitable.close()
        raise TimeoutError

    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: _ProbeDriver(captured),
    )
    monkeypatch.setattr(
        "workflow_controller.core.auth_rotator.asyncio.wait_for",
        immediate_timeout,
    )
    result = await _rotator(tmp_path).validate_candidate("sk", "m", "", "claude_sdk")

    assert result["success"] is True
    assert "timed out" in result["worker"]["error"]


@pytest.mark.asyncio
async def test_set_keys_accepts_reviewer_failure_and_reuses_worker_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _counting_probe_env(monkeypatch, http_ok=False)
    rotator = _rotator(tmp_path)
    keys = [{"key": "sk", "model": "m", "runtime": "claude_sdk"}]

    await rotator.set_keys_validated(keys)
    await rotator.set_keys_validated(keys)

    provider = rotator.get_provider(0)
    assert calls == {"worker": 1, "reviewer": 4}
    assert provider.worker_validation.status == ValidationStatus.VALID
    assert provider.reviewer_validation.status == ValidationStatus.INVALID


@pytest.mark.asyncio
async def test_set_keys_reuses_validations_after_provider_reordering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _counting_probe_env(monkeypatch)
    rotator = _rotator(tmp_path)
    first = {"key": "sk-1", "model": "m1", "runtime": "claude_sdk"}
    second = {"key": "sk-2", "model": "m2", "runtime": "opencode"}

    await rotator.set_keys_validated([first, second])
    await rotator.set_keys_validated([second, first])

    assert calls == {"worker": 2, "reviewer": 8}
    assert [provider.key for provider in rotator.get_providers()] == ["sk-2", "sk-1"]


@pytest.mark.asyncio
async def test_set_keys_reuses_single_valid_capability_after_provider_reordering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """已有 Provider 只要一侧 VALID 且配置未变，保存整池时不重测。"""
    calls = _counting_probe_env(monkeypatch, http_ok=False)
    rotator = _rotator(tmp_path)
    first = {"key": "sk-1", "model": "m1", "runtime": "claude_sdk"}
    second = {"key": "sk-2", "model": "m2", "runtime": "claude_sdk"}

    await rotator.set_keys_validated([first, second])
    await rotator.set_keys_validated([second, first])

    assert calls == {"worker": 2, "reviewer": 8}
    assert [provider.key for provider in rotator.get_providers()] == ["sk-2", "sk-1"]
    assert rotator.get_provider(1).reviewer_validation.status == ValidationStatus.INVALID


@pytest.mark.asyncio
async def test_set_keys_saves_failed_existing_provider_and_adds_new_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_calls = 0
    reviewer_calls = 0

    def fake_create(runtime: str, **kwargs):
        nonlocal worker_calls
        worker_calls += 1
        event_type = EventType.ERROR if worker_calls == 1 else EventType.STEP_FINISH
        return _ProbeDriver({}, event_type)

    async def fake_http(self, prompt: str, **kwargs):
        nonlocal reviewer_calls
        reviewer_calls += 1
        if reviewer_calls <= 4:
            return HttpResult(text="", is_error=True, error="HTTP 403")
        return HttpResult(text="OK", is_error=False, error="")

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AnthropicHttpClient, "request", fake_http)
    rotator = _rotator(tmp_path)
    failed = {"key": "sk-failed", "model": "m-failed", "runtime": "claude_sdk"}
    added = {"key": "sk-new", "model": "m-new", "runtime": "claude_sdk"}
    rotator.set_keys([failed])

    await rotator.set_keys_validated([failed, added])

    assert worker_calls == 2
    assert reviewer_calls == 8
    assert [provider.key for provider in rotator.get_providers()] == ["sk-failed", "sk-new"]
    failed_provider = rotator.get_provider(0)
    assert failed_provider.worker_validation.status == ValidationStatus.INVALID
    assert failed_provider.reviewer_validation.status == ValidationStatus.INVALID
    assert rotator.get_provider(1).worker_validation.status == ValidationStatus.VALID


@pytest.mark.asyncio
async def test_failed_worker_validation_saves_configuration_and_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: _ProbeDriver({}, EventType.ERROR),
    )
    rotator = _rotator(tmp_path)
    await rotator.set_keys_validated(
        [
            {
                "key": "sk",
                "model": "m",
                "runtime": "claude_sdk",
            }
        ]
    )
    provider = rotator.get_provider(0)
    assert provider.key == "sk"
    assert provider.worker_validation.status == ValidationStatus.INVALID
    assert rotator.get_provider_count() == 1


@pytest.mark.asyncio
async def test_set_keys_rejects_duplicate_provider_fingerprints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "workflow_controller.driver.factory.create_driver",
        lambda runtime, **kwargs: _ProbeDriver({}),
    )
    rotator = _rotator(tmp_path)
    duplicate = {
        "key": "sk",
        "model": "m",
        "base_url": "https://example.test/v1",
        "runtime": "claude_sdk",
    }

    with pytest.raises(ValueError, match="duplicate provider configuration"):
        await rotator.set_keys_validated([duplicate, duplicate.copy()])

    assert rotator.get_provider_count() == 0

    different_model = {**duplicate, "model": "m2"}
    await rotator.set_keys_validated([duplicate, different_model])
    assert rotator.get_provider_count() == 2


@pytest.mark.asyncio
async def test_persisted_invalid_validation_retries_after_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"worker": 0, "reviewer": 0}

    def fake_create(runtime: str, **kwargs):
        calls["worker"] += 1
        return _ProbeDriver({})

    async def failed_http(self, prompt, **kwargs):
        calls["reviewer"] += 1
        return HttpResult(text="", is_error=True, error="HTTP 403")

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AnthropicHttpClient, "request", failed_http)
    rotator = _rotator(tmp_path)
    rotator.set_keys([{"key": "sk", "model": "m", "runtime": "claude_sdk"}])

    await rotator.validate_provider(0)
    await rotator.validate_provider(0)
    assert calls == {"worker": 1, "reviewer": 4}

    provider = rotator.get_provider(0)
    provider.reviewer_validation.checked_at = datetime.now(tz=UTC) - timedelta(minutes=2)
    await rotator.validate_provider(0)

    assert calls == {"worker": 2, "reviewer": 8}


@pytest.mark.asyncio
async def test_legacy_provider_validates_on_first_use_and_reuses_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _counting_probe_env(monkeypatch)
    rotator = _rotator(tmp_path)
    rotator.set_keys([{"key": "sk-legacy", "model": "m"}])
    assert rotator.get_provider(0).runtime is None

    await rotator.ensure_provider_capability(0, "worker", runtime="claude_sdk")
    await rotator.ensure_provider_capability(0, "worker", runtime="claude_sdk")
    restarted = _rotator(tmp_path)
    await restarted.ensure_provider_capability(0, "worker", runtime="claude_sdk")

    assert calls == {"worker": 1, "reviewer": 4}
    assert restarted.get_provider(0).runtime == "claude_sdk"


@pytest.mark.asyncio
async def test_probe_uses_default_url_when_entry_base_url_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """条目依赖 settings.default_url 时，协议探测用 effective base_url
    （与 _build_api_env 同规则），否则默认 URL 条目探测被跳过、注入失效。
    """
    captured: dict = {}

    def fake_create(runtime, **kwargs):
        return _ProbeDriver({})

    async def fake_http(self, prompt, **kwargs):
        return HttpResult(text="OK", is_error=False, error="")

    async def fake_probe(self, key, base_url, model=""):
        captured["probe_url"] = base_url
        return "@ai-sdk/openai-compatible"

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AnthropicHttpClient, "request", fake_http)
    monkeypatch.setattr(AuthRotator, "probe_provider_kind", fake_probe)
    rotator = _rotator(tmp_path)
    rotator.set_global_settings(default_url="https://gw.example.com/v1")

    result = await rotator.validate_candidate("sk", "m", "", "opencode")

    assert captured["probe_url"] == "https://gw.example.com/v1"
    assert result["provider_kind"] == "@ai-sdk/openai-compatible"
    assert result["success"] is True


def _counting_rotator(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict, AuthRotator]:
    """装 create_driver 计数桩的 rotator：fake_create 每次出生 _ProbeDriver。"""
    calls = {"worker": 0}

    def fake_create(runtime, **kwargs):
        calls["worker"] += 1
        return _ProbeDriver({})

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    return calls, _rotator(tmp_path)


@pytest.mark.asyncio
async def test_probe_cache_separates_explicit_provider_kinds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """kind 改变 opencode 实际加载的 ai-sdk 包，显式换 kind 不能命中旧缓存。"""
    calls, rotator = _counting_rotator(tmp_path, monkeypatch)
    url = "https://example.test/v1"

    await rotator.validate_candidate(
        "sk", "m", url, "opencode", provider_kind="@ai-sdk/openai-compatible"
    )
    await rotator.validate_candidate(
        "sk", "m", url, "opencode", provider_kind="@ai-sdk/openai-compatible"
    )
    assert calls["worker"] == 1

    await rotator.validate_candidate("sk", "m", url, "opencode", provider_kind="@ai-sdk/anthropic")
    assert calls["worker"] == 2


@pytest.mark.asyncio
async def test_probe_cache_hits_after_kind_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """未显式给 kind 的候选：每次先实测协议再查缓存，实测结果一致即命中。"""
    calls = {"worker": 0}

    def fake_create(runtime, **kwargs):
        calls["worker"] += 1
        return _ProbeDriver({})

    async def fake_probe(self, key, base_url, model=""):
        return "@ai-sdk/openai-compatible"

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AuthRotator, "probe_provider_kind", fake_probe)
    rotator = _rotator(tmp_path)

    await rotator.validate_candidate("sk", "m", "https://example.test/v1", "opencode")
    await rotator.validate_candidate("sk", "m", "https://example.test/v1", "opencode")
    assert calls["worker"] == 1


@pytest.mark.asyncio
async def test_workflow_controller_driver_override_candidate_probes_and_injects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """WORKFLOW_CONTROLLER_DRIVER=opencode 覆盖全局 claude_sdk、条目无显式 kind：
    协议探测按 effective runtime 判定（与 create_driver/_build_api_env
    同规则）——否则 probe 被跳过、kind=None 不注入、model 保持裸名，
    driver 实跑 opencode 却回落默认 provider。
    """
    captured, calls = _opencode_override_probe_env(monkeypatch)
    rotator = _rotator(tmp_path)

    result = await rotator.validate_candidate(
        "sk",
        "glm-5.2",
        "https://example.test/v1",
        None,
    )

    assert calls["probe"] == 1
    assert result["runtime"] == "opencode"
    assert result["provider_kind"] == "@ai-sdk/openai-compatible"
    assert captured["runtime"] == "opencode"
    assert captured["query"]["model"] == "workflow-controller-pool/glm-5.2"
    assert "OPENCODE_CONFIG_CONTENT" in captured["query"]["env"]


@pytest.mark.asyncio
async def test_workflow_controller_driver_override_rekeys_probe_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """指纹记录 effective runtime：claude_sdk 下验证的 VALID 缓存在
    WORKFLOW_CONTROLLER_DRIVER=opencode 覆盖下不得复用（worker 实际 runtime 已变），
    覆盖移除后回到原指纹才命中。kind 显式且不变，隔离出 runtime 维度。
    """
    calls, rotator = _counting_rotator(tmp_path, monkeypatch)
    url = "https://example.test/v1"
    kind = "@ai-sdk/anthropic"

    await rotator.validate_candidate(
        "sk",
        "m",
        url,
        "claude_sdk",
        provider_kind=kind,
    )
    monkeypatch.setenv("WORKFLOW_CONTROLLER_DRIVER", "opencode")
    await rotator.validate_candidate(
        "sk",
        "m",
        url,
        "claude_sdk",
        provider_kind=kind,
    )
    assert calls["worker"] == 2

    monkeypatch.delenv("WORKFLOW_CONTROLLER_DRIVER", raising=False)
    await rotator.validate_candidate(
        "sk",
        "m",
        url,
        "claude_sdk",
        provider_kind=kind,
    )
    assert calls["worker"] == 2


@pytest.mark.asyncio
async def test_workflow_controller_driver_override_save_probes_kind_and_keeps_conf_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """覆盖 + 无显式 kind 的新增/保存路径：set_keys_validated 按 effective
    runtime 实测协议并把 kind 写回 provider（auth_state.json），settings.conf
    的条目 runtime 保持配置值——WORKFLOW_CONTROLLER_DRIVER 是临时环境覆盖，不落盘。
    """
    captured, calls = _opencode_override_probe_env(monkeypatch)
    rotator = _rotator(tmp_path)

    await rotator.set_keys_validated(
        [
            {
                "key": "sk",
                "model": "glm-5.2",
                "base_url": "https://example.test/v1",
                "runtime": "claude_sdk",
            }
        ]
    )

    assert calls["probe"] == 1
    assert captured["runtime"] == "opencode"
    assert captured["query"]["model"] == "workflow-controller-pool/glm-5.2"
    provider = rotator.get_provider(0)
    assert provider.provider_kind == "@ai-sdk/openai-compatible"
    assert provider.provider_kind_source == "probe"
    assert provider.worker_validation.status == ValidationStatus.VALID
    assert provider.worker_validation.fingerprint == getattr(rotator, "_provider_fingerprint")(
        provider
    )
    from workflow_controller.core.auth_rotator.settings import GlobalSettings

    conf_entry = GlobalSettings.load(tmp_path / "settings.conf").api_keys[0]
    assert conf_entry["runtime"] == "claude_sdk"
    assert "provider_kind" not in conf_entry


@pytest.mark.asyncio
async def test_set_keys_validated_persists_probed_provider_kind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """探测出的 kind 落到 provider（auth_state.json）而非 settings.conf 条目，
    且裸 model 的 worker 探测用合成路由。探测值写进 conf 会与显式配置无法
    区分——换 URL 时被当显式值重新绑定到新端点而不是清空重探测。
    """
    captured: dict = {}

    def fake_create(runtime, **kwargs):
        return _ProbeDriver(captured)

    async def fake_http(self, prompt, **kwargs):
        return HttpResult(text="OK", is_error=False, error="")

    async def fake_probe(self, key, base_url, model=""):
        return "@ai-sdk/openai-compatible"

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AnthropicHttpClient, "request", fake_http)
    monkeypatch.setattr(AuthRotator, "probe_provider_kind", fake_probe)
    rotator = _rotator(tmp_path)

    await rotator.set_keys_validated(
        [
            {
                "key": "sk",
                "model": "glm-5.2",
                "base_url": "https://example.test/v1",
                "runtime": "opencode",
            }
        ]
    )

    assert captured["query"]["model"] == "workflow-controller-pool/glm-5.2"
    provider = rotator.get_provider(0)
    assert provider.provider_kind == "@ai-sdk/openai-compatible"
    assert provider.provider_kind_url == "https://example.test/v1"
    assert provider.worker_validation.status == ValidationStatus.VALID
    assert provider.worker_validation.fingerprint == getattr(rotator, "_provider_fingerprint")(
        provider
    )
    from workflow_controller.core.auth_rotator.settings import GlobalSettings

    conf_entry = GlobalSettings.load(tmp_path / "settings.conf").api_keys[0]
    assert "provider_kind" not in conf_entry


@pytest.mark.asyncio
async def test_set_keys_validated_passes_internal_codex_catalog_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    catalog = home / "models.json"
    catalog.write_text(
        '{"models": [{"slug": "glm-5.3"}]}',
        encoding="utf-8",
    )
    monkeypatch.setenv("HOME", str(home))
    captured: dict = {}

    def fake_create(runtime, **kwargs):
        captured["runtime"] = runtime
        return _CodexProbeDriver(captured)

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    rotator = _codex_rotator(tmp_path)

    await rotator.set_keys_validated(
        [
            {
                "key": "sk",
                "model": "glm-5.3",
                "base_url": "https://example.test/v1",
                "runtime": "codex_cli",
                "model_catalog_path": "~/models.json",
            }
        ]
    )

    assert captured["runtime"] == "codex_cli"
    assert captured["query"]["env"]["model_catalog_path"] == str(catalog.resolve())
    assert rotator.get_provider(0).model_catalog_path == str(catalog.resolve())
    from workflow_controller.core.auth_rotator.settings import GlobalSettings

    entry = GlobalSettings.load(tmp_path / "settings.conf").api_keys[0]
    assert entry["model_catalog_path"] == str(catalog.resolve())
    reloaded = _codex_rotator(tmp_path)
    assert reloaded.get_provider(0).model_catalog_path == str(catalog.resolve())


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (
            "custom_required",
            "请配置第三方 provider 官方推荐的 model catalog JSON 文件，并将所在绝对路径输入设置中",
        ),
        (
            "custom_invalid",
            "请输入合法且包含正确模型名的 model catalog JSON 文件路径",
        ),
    ],
)
@pytest.mark.asyncio
async def test_set_keys_rejects_codex_catalog_failure_when_reviewer_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    message: str,
) -> None:
    from workflow_controller.driver.anthropic_http import (
        AnthropicHttpClient as ReviewerHttpClient,
    )
    from workflow_controller.driver.anthropic_http import (
        HttpResult as ReviewerHttpResult,
    )
    from workflow_controller.driver.codex_catalog import CodexCatalogResolution

    async def failed_http(self, prompt, **kwargs):
        return ReviewerHttpResult(text="", is_error=True, error="HTTP 403")

    monkeypatch.setattr(
        "workflow_controller.driver.codex_catalog.resolve_codex_catalog",
        lambda **_kwargs: CodexCatalogResolution(status=status),
    )
    monkeypatch.setattr(ReviewerHttpClient, "request", failed_http)
    rotator = _codex_rotator(tmp_path)

    with pytest.raises(ValueError, match=message):
        await _set_custom_codex_key(rotator)

    assert rotator.get_provider_count() == 0
    assert not (tmp_path / "settings.conf").exists()


@pytest.mark.asyncio
async def test_set_keys_saves_reviewer_only_codex_catalog_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from workflow_controller.driver.anthropic_http import (
        AnthropicHttpClient as ReviewerHttpClient,
    )
    from workflow_controller.driver.anthropic_http import (
        HttpResult as ReviewerHttpResult,
    )
    from workflow_controller.driver.codex_catalog import CodexCatalogResolution

    async def successful_http(self, prompt, **kwargs):
        return ReviewerHttpResult(text="OK", is_error=False, error="")

    monkeypatch.setattr(
        "workflow_controller.driver.codex_catalog.resolve_codex_catalog",
        lambda **_kwargs: CodexCatalogResolution(status="custom_required"),
    )
    monkeypatch.setattr(ReviewerHttpClient, "request", successful_http)
    rotator = _codex_rotator(tmp_path)

    await _set_custom_codex_key(rotator)

    provider = rotator.get_provider(0)
    assert provider.worker_validation.status == ValidationStatus.INVALID
    assert "model catalog JSON" in provider.worker_validation.error
    assert provider.reviewer_validation.status == ValidationStatus.VALID
    assert provider.model_catalog_path is None


@pytest.mark.asyncio
async def test_invalid_catalog_preserves_provider_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from workflow_controller.core.auth_rotator.settings import GlobalSettings
    from workflow_controller.driver.codex_catalog import CodexCatalogResolution

    catalog_path = "/invalid/models.json"
    GlobalSettings(
        api_keys=[
            {
                "key": "sk",
                "model": "custom-model",
                "base_url": "https://relay.test/v1",
                "runtime": "codex_cli",
                "model_catalog_path": catalog_path,
            }
        ]
    ).save(tmp_path / "settings.conf")
    monkeypatch.setattr(
        "workflow_controller.driver.codex_catalog.resolve_codex_catalog",
        lambda **_kwargs: CodexCatalogResolution(status="custom_invalid"),
    )
    rotator = AuthRotator(
        state_path=tmp_path / "auth_state.json",
        env_path=tmp_path / ".env",
        runtime="codex_cli",
    )

    result = await rotator.validate_provider(0, force=True)
    provider = rotator.get_provider(0)

    assert provider.model_catalog_path == catalog_path
    assert provider.worker_validation.status == ValidationStatus.INVALID
    assert provider.reviewer_validation.status == ValidationStatus.VALID
    assert result["fingerprint"] == getattr(rotator, "_provider_fingerprint")(provider)
    assert provider.reviewer_validation.fingerprint == getattr(rotator, "_provider_fingerprint")(
        provider
    )

    reloaded = AuthRotator(
        state_path=tmp_path / "auth_state.json",
        env_path=tmp_path / ".env",
        runtime="codex_cli",
    )
    assert reloaded.get_provider(0).reviewer_validation.status == ValidationStatus.VALID


def test_provider_fingerprint_upgrade_clears_cached_validations(
    tmp_path: Path,
) -> None:
    from workflow_controller.core.auth_rotator.persistence import AuthStateSnapshot, AuthStateStore
    from workflow_controller.core.auth_rotator.settings import GlobalSettings

    entry = {
        "key": "sk",
        "model": "m",
        "base_url": "https://a.test/v1",
        "runtime": "claude_sdk",
    }
    GlobalSettings(api_keys=[entry]).save(tmp_path / "settings.conf")
    provider = AuthProvider(
        provider_type=AuthProviderType.API_KEY,
        key="sk",
        model="m",
        base_url="https://a.test/v1",
        runtime="claude_sdk",
    )
    provider.worker_validation = ProviderValidation(
        capability="worker/claude_sdk",
        fingerprint="legacy",
        status=ValidationStatus.VALID,
    )
    provider.reviewer_validation = ProviderValidation(
        capability="anthropic_http",
        fingerprint="legacy",
        status=ValidationStatus.VALID,
    )
    AuthStateStore(tmp_path / "auth_state.json").save(
        AuthStateSnapshot(providers=[provider]),
    )

    reloaded = _rotator(tmp_path)

    assert reloaded.get_provider(0).worker_validation.status != ValidationStatus.VALID
    assert reloaded.get_provider(0).reviewer_validation.status != ValidationStatus.VALID


def test_auth_state_round_trips_codex_catalog_path(tmp_path: Path) -> None:
    from workflow_controller.core.auth_rotator import AuthMode
    from workflow_controller.core.auth_rotator.persistence import (
        AuthStateSnapshot,
        AuthStateStore,
    )

    provider = AuthProvider(
        provider_type=AuthProviderType.API_KEY,
        key="sk",
        model="m",
        base_url="https://a.test/v1",
        runtime="codex_cli",
        model_catalog_path="~/models.json",
    )
    provider.reviewer_validation.thinking_mode = "adaptive"
    store = AuthStateStore(tmp_path / "auth_state.json")
    store.save(AuthStateSnapshot(mode=AuthMode.API, providers=[provider]))

    restored = store.load().providers[0]

    assert restored.model_catalog_path == "~/models.json"
    assert restored.reviewer_validation.thinking_mode == "adaptive"


@pytest.mark.asyncio
async def test_validate_provider_persists_resolved_codex_catalog_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from workflow_controller.core.auth_rotator.settings import GlobalSettings

    catalog = tmp_path / "models.json"
    catalog.write_text('{"models":[{"slug":"m"}]}', encoding="utf-8")
    GlobalSettings(
        api_keys=[
            {
                "key": "sk",
                "model": "m",
                "base_url": "https://a.test/v1",
                "runtime": "codex_cli",
            }
        ]
    ).save(tmp_path / "settings.conf")

    async def fake_validate_candidate(self, *args, **kwargs):
        return {
            "provider_kind": None,
            "model_catalog_path": str(catalog.resolve()),
            "worker": ProviderValidation(
                capability="worker/codex_cli",
                status=ValidationStatus.VALID,
            ).to_dict(),
            "reviewer": ProviderValidation(
                capability="anthropic_http",
                status=ValidationStatus.VALID,
            ).to_dict(),
        }

    monkeypatch.setattr(
        AuthRotator,
        "validate_candidate",
        fake_validate_candidate,
    )
    rotator = _codex_rotator(tmp_path)

    await rotator.validate_provider(0, force=True)

    assert GlobalSettings.load(tmp_path / "settings.conf").api_keys[0]["model_catalog_path"] == str(
        catalog.resolve()
    )
    state = json.loads((tmp_path / "auth_state.json").read_text(encoding="utf-8"))
    assert state["providers"][0]["model_catalog_path"] == str(catalog.resolve())
    reloaded = _codex_rotator(tmp_path)
    assert reloaded.get_provider(0).model_catalog_path == str(catalog.resolve())


def test_env_for_driver_ignores_host_model_catalog_payload(
    tmp_path: Path,
) -> None:
    rotator = _codex_rotator(tmp_path)
    getattr(rotator, "_providers").extend(
        [
            AuthProvider(
                provider_type=AuthProviderType.API_KEY,
                key="sk",
                model="m",
                base_url="https://a.test/v1",
                runtime="codex_cli",
                model_catalog_path="~/a-models.json",
            ),
            AuthProvider(
                provider_type=AuthProviderType.API_KEY,
                key="sk-2",
                model="m",
                base_url="https://b.test/v1",
                runtime="codex_cli",
            ),
        ]
    )

    first = rotator.env_for_driver(
        {"model_catalog_path": "/host/models.json"},
        0,
    )
    second = rotator.env_for_driver(
        {"model_catalog_path": "/host/models.json"},
        1,
    )

    assert first["model_catalog_path"] == "~/a-models.json"
    assert "model_catalog_path" not in second


def test_model_catalog_path_is_dormant_for_non_codex_runtime(
    tmp_path: Path,
) -> None:
    from workflow_controller.core.auth_rotator.key_store import parse_key_entry
    from workflow_controller.core.auth_rotator.settings import GlobalSettings

    entry = {
        "key": "sk",
        "model": "m",
        "base_url": "https://a.test/v1",
        "runtime": "claude_cli",
        "model_catalog_path": "~/models.json",
    }
    assert parse_key_entry(entry).model_catalog_path == "~/models.json"
    settings_path = tmp_path / "settings.conf"
    GlobalSettings(api_keys=[entry]).save(settings_path)
    assert GlobalSettings.load(settings_path).api_keys[0]["model_catalog_path"] == ("~/models.json")

    provider = AuthProvider(
        provider_type=AuthProviderType.API_KEY,
        key="sk",
        model="m",
        base_url="https://a.test/v1",
        runtime="claude_cli",
        model_catalog_path="~/models.json",
    )
    provider_without_catalog = AuthProvider(
        provider_type=AuthProviderType.API_KEY,
        key="sk",
        model="m",
        base_url="https://a.test/v1",
        runtime="claude_cli",
    )
    rotator = AuthRotator(
        state_path=tmp_path / "auth_state.json",
        env_path=tmp_path / ".env",
        runtime="claude_cli",
    )
    getattr(rotator, "_providers").append(provider)

    assert "model_catalog_path" not in rotator.env_for_driver({}, 0)
    assert getattr(rotator, "_provider_fingerprint")(provider) == getattr(
        rotator, "_provider_fingerprint"
    )(
        provider_without_catalog,
    )


def test_parse_key_entry_preserves_catalog_without_runtime() -> None:
    from workflow_controller.core.auth_rotator.key_store import parse_key_entry

    parsed = parse_key_entry(
        {
            "key": "sk",
            "model_catalog_path": "~/models.json",
        }
    )
    assert parsed.runtime is None
    assert parsed.model_catalog_path == "~/models.json"


@pytest.mark.asyncio
async def test_set_keys_validated_reuses_fresh_probed_kind_without_reprobe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """配置未变的保存（UI 回传条目不带探测 kind）不重新探测/验证：按不含
    kind 的身份匹配已有条目，继承仍新鲜（provider_kind_url == 当前
    effective URL）的探测缓存并复用其 VALID 验证。
    """
    calls = _kind_probing_env(monkeypatch, lambda base_url: "@ai-sdk/openai-compatible")
    rotator = _rotator(tmp_path)

    entry = {
        "key": "sk",
        "model": "m",
        "base_url": "https://example.test/v1",
        "runtime": "opencode",
    }

    await rotator.set_keys_validated([entry])
    assert (calls["worker"], calls["probe"]) == (1, 1)
    provider = rotator.get_provider(0)
    assert provider.provider_kind == "@ai-sdk/openai-compatible"

    await rotator.set_keys_validated([entry])
    assert (calls["worker"], calls["probe"]) == (1, 1)
    assert rotator.get_provider(0).provider_kind == "@ai-sdk/openai-compatible"


@pytest.mark.asyncio
async def test_set_keys_validated_removed_explicit_kind_reprobes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """同一条目先带显式 kind 保存、再不带 kind 保存（显式纠偏被删除）：
    旧显式 kind 不能被当探测缓存继承而跳过重探测，保存后按新实测结果
    生效——否则错误的人工纠偏无法经保存撤销。
    """
    calls = _kind_probing_env(monkeypatch, lambda base_url: "@ai-sdk/openai-compatible")
    rotator = _rotator(tmp_path)

    await rotator.set_keys_validated(
        [
            {
                "key": "sk",
                "model": "m",
                "base_url": "https://example.test/v1",
                "runtime": "opencode",
                "provider_kind": "@ai-sdk/anthropic",
            }
        ]
    )
    provider = rotator.get_provider(0)
    assert provider.provider_kind == "@ai-sdk/anthropic"
    assert provider.provider_kind_source == "explicit"
    assert calls["probe"] == 0

    await rotator.set_keys_validated(
        [
            {
                "key": "sk",
                "model": "m",
                "base_url": "https://example.test/v1",
                "runtime": "opencode",
            }
        ]
    )

    assert calls["probe"] == 1
    provider = rotator.get_provider(0)
    assert provider.provider_kind == "@ai-sdk/openai-compatible"
    assert provider.provider_kind_source == "probe"
    assert provider.provider_kind_url == "https://example.test/v1"


@pytest.mark.asyncio
async def test_default_url_change_resave_reprobes_new_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """换 default_url 后重新保存（UI 条目不带 kind）：旧探测值已被 sync 清空
    且 settings.conf 无 kind 复活它，保存对新 URL 重新实测协议并成功——
    不会因旧协议被当显式值跳过探测而失败/注入错包。
    """
    calls = _kind_probing_env(
        monkeypatch,
        lambda base_url: (
            "@ai-sdk/openai-compatible"
            if base_url == "https://new.example/v1"
            else "@ai-sdk/anthropic"
        ),
    )
    rotator = _rotator(tmp_path)
    rotator.set_global_settings(default_url="https://old.example/v1")

    entry = {"key": "sk", "model": "m", "runtime": "opencode"}

    await rotator.set_keys_validated([entry])
    provider = rotator.get_provider(0)
    assert provider.provider_kind == "@ai-sdk/anthropic"
    assert provider.provider_kind_url == "https://old.example/v1"

    rotator.set_global_settings(default_url="https://new.example/v1")
    assert rotator.get_provider(0).provider_kind is None

    await rotator.set_keys_validated([entry])

    provider = rotator.get_provider(0)
    assert calls["probe"] == 2
    assert provider.provider_kind == "@ai-sdk/openai-compatible"
    assert provider.provider_kind_url == "https://new.example/v1"
    assert provider.worker_validation.status == ValidationStatus.VALID


@pytest.mark.asyncio
async def test_validate_provider_probes_and_persists_kind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """validate_provider 探测到的 kind 写回持久化 provider（否则 session 注入跳过）。"""

    def fake_create(runtime, **kwargs):
        return _ProbeDriver({})

    async def fake_http(self, prompt, **kwargs):
        return HttpResult(text="OK", is_error=False, error="")

    async def fake_probe(self, key, base_url, model=""):
        return "@ai-sdk/anthropic"

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AnthropicHttpClient, "request", fake_http)
    monkeypatch.setattr(AuthRotator, "probe_provider_kind", fake_probe)
    rotator = _rotator(tmp_path)
    rotator.set_keys(
        [
            {
                "key": "sk",
                "model": "route/m",
                "base_url": "https://example.test/v1",
                "runtime": "opencode",
            }
        ]
    )
    assert rotator.get_provider(0).provider_kind is None

    await rotator.validate_provider(0)
    assert rotator.get_provider(0).provider_kind == "@ai-sdk/anthropic"


@pytest.mark.asyncio
async def test_default_url_change_revalidates_and_reprobes_kind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """改全局 default_url 后，依赖默认 URL 的条目：effective URL 变化 →
    指纹变化（旧 VALID 不复用）、旧 kind 清空（不当显式值跳过重探测），
    重探测必须打到新 URL。
    """
    calls = {"worker": 0, "probe": 0}

    def fake_create(runtime, **kwargs):
        calls["worker"] += 1
        return _ProbeDriver({})

    async def fake_probe(self, key, base_url, model=""):
        calls["probe"] += 1
        calls["probe_url"] = base_url
        return "@ai-sdk/anthropic"

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    monkeypatch.setattr(AuthRotator, "probe_provider_kind", fake_probe)
    rotator = _rotator(tmp_path)
    rotator.set_global_settings(default_url="https://old.example/v1")
    rotator.set_keys([{"key": "sk", "model": "route/m", "runtime": "opencode"}])

    await rotator.validate_provider(0)
    provider = rotator.get_provider(0)
    assert provider.provider_kind == "@ai-sdk/anthropic"
    assert provider.provider_kind_url == "https://old.example/v1"
    assert (calls["worker"], calls["probe"]) == (1, 1)

    rotator.set_global_settings(default_url="https://new.example/v1")
    assert rotator.get_provider(0).provider_kind is None

    result = await rotator.validate_provider(0)
    assert (calls["worker"], calls["probe"]) == (2, 2)
    assert calls["probe_url"] == "https://new.example/v1"
    assert result["provider_kind"] == "@ai-sdk/anthropic"
    provider = rotator.get_provider(0)
    assert provider.provider_kind == "@ai-sdk/anthropic"
    assert provider.provider_kind_url == "https://new.example/v1"


def test_runtime_auth_failure_is_isolated_from_reviewer_and_global_status(tmp_path: Path) -> None:
    rotator = _rotator(tmp_path)
    rotator.set_keys(
        [
            {"key": "sk-1", "model": "route/m1", "runtime": "opencode"},
            {"key": "sk-2", "model": "route/m2", "runtime": "opencode"},
            {"key": "sk-3", "model": "m3", "runtime": "claude_sdk"},
        ]
    )
    for provider in rotator.get_providers():
        provider.worker_validation = ProviderValidation(
            capability=f"worker/{provider.runtime}",
            status=ValidationStatus.VALID,
        )
        provider.reviewer_validation = ProviderValidation(
            capability="anthropic_http",
            status=ValidationStatus.VALID,
        )

    replacement = rotator.rotate_bound_provider(
        0,
        scope="auth_error",
        runtime="opencode",
    )

    failed = rotator.get_provider(0)
    assert failed.status != KeyStatus.INVALID
    assert failed.worker_validation.status == ValidationStatus.INVALID
    assert failed.reviewer_validation.status == ValidationStatus.VALID
    assert replacement == 1


class _FakeProbeResponse:
    def __init__(self, status_code: int, url: str = "") -> None:
        self.status_code = status_code
        self._url = url

    def json(self) -> dict:
        """按探测路径给协议形状正确的成功体（200 时）。

        probe_provider_kind 对 2xx 应答做响应体形状核对（网关会把错误包成
        200 + 错误 JSON），假响应需携带可识别的成功载荷。
        """
        if self._url.endswith("/chat/completions"):
            return {
                "id": "c1",
                "object": "chat.completion",
                "choices": [{"message": {"role": "assistant", "content": "1"}}],
            }
        return {
            "id": "m1",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "1"}],
        }


def _fake_probe_http(
    status_by_call: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, int]:
    """把 probe_provider_kind 的外网探测换成假 httpx，按调用序返回状态码。"""
    import httpx

    calls = {"probe_http": 0}

    class _Client:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self) -> _Client:
            return self

        async def __aexit__(self, *args) -> None:
            return None

        async def post(self, url: str, **kwargs) -> _FakeProbeResponse:
            index = calls["probe_http"]
            calls["probe_http"] += 1
            status = status_by_call[min(index, len(status_by_call) - 1)]
            return _FakeProbeResponse(status, url)

    monkeypatch.setattr(httpx, "AsyncClient", _Client)
    return calls


@pytest.mark.asyncio
async def test_test_key_probed_kind_reused_by_save_without_reprobe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """test-key 刚实测的协议由后端短 TTL 缓存：随后保存同一条目（无显式
    kind）直接复用，不再二次外网探测——二次探测瞬时失败（429）会让刚
    测试通过的条目无法保存（kind=None 不注入 → worker 验证失败）。
    """
    # 第 1 次探测 200（test-key），之后的探测请求一律 429（模拟瞬时失败）
    calls = _fake_probe_http([200, 429], monkeypatch)
    calls["worker"] = 0

    def fake_create(runtime, **kwargs):
        calls["worker"] += 1
        return _ProbeDriver({})

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    rotator = _rotator(tmp_path)
    url = "https://example.test/v1"

    result = await rotator.validate_candidate("sk", "m", url, "opencode", force=True)
    assert result["success"] is True
    assert result["provider_kind"] == "@ai-sdk/openai-compatible"
    assert (calls["probe_http"], calls["worker"]) == (1, 1)

    await rotator.set_keys_validated(
        [
            {
                "key": "sk",
                "model": "m",
                "base_url": url,
                "runtime": "opencode",
            }
        ]
    )

    # 保存路径 0 次外网探测、0 次 worker 重测：缓存 kind 使指纹与 test-key
    # 一致，VALID 探测缓存随之复用
    assert (calls["probe_http"], calls["worker"]) == (1, 1)
    provider = rotator.get_provider(0)
    assert provider.provider_kind == "@ai-sdk/openai-compatible"
    assert provider.provider_kind_source == "probe"
    assert provider.worker_validation.status == ValidationStatus.VALID


@pytest.mark.asyncio
async def test_probed_kind_cache_expires_and_reprobes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TTL 过期后重新实测：首轮 OpenAI 风格 200，过期后首轮 429、Anthropic
    路径 200 → kind 切换为 @ai-sdk/anthropic，证明缓存没有无限期固定协议。
    """
    calls = _fake_probe_http([200, 429, 200], monkeypatch)

    def fake_create(runtime, **kwargs):
        return _ProbeDriver({})

    monkeypatch.setattr("workflow_controller.driver.factory.create_driver", fake_create)
    rotator = _rotator(tmp_path)
    url = "https://example.test/v1"

    first = await rotator.validate_candidate("sk", "m", url, "opencode", force=True)
    assert first["provider_kind"] == "@ai-sdk/openai-compatible"
    assert calls["probe_http"] == 1

    for cache_key, (kind, _) in list(getattr(rotator, "_probed_kinds").items()):
        getattr(rotator, "_probed_kinds")[cache_key] = (kind, time.time() - 1)

    second = await rotator.validate_candidate("sk", "m", url, "opencode", force=True)
    assert calls["probe_http"] == 3
    assert second["provider_kind"] == "@ai-sdk/anthropic"


@pytest.mark.asyncio
async def test_probe_kind_rejects_error_body_with_http_200(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """网关把错误包成 HTTP 200 + 错误 JSON（如 bigmodel 的 404-in-200）时，
    状态码不代言协议：形状核对不过 → 不确认该协议（返回 None，不注入）。
    """
    import httpx

    calls = {"n": 0}

    class _Resp:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {"code": 500, "msg": "404 NOT_FOUND", "success": False}

    class _Client:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self) -> _Client:
            return self

        async def __aexit__(self, *args) -> None:
            return None

        async def post(self, url: str, **kwargs) -> _Resp:
            calls["n"] += 1
            return _Resp()

    monkeypatch.setattr(httpx, "AsyncClient", _Client)
    rotator = _rotator(tmp_path)
    kind = await rotator.probe_provider_kind("sk", "https://gw.example.com/v1", "m")
    assert kind is None
    assert calls["n"] == 2  # 两种协议形状都试过，均因响应体形状不符被拒


@pytest.mark.asyncio
async def test_probed_kind_failure_is_not_cached(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """全部探测请求失败（429）→ None；失败不缓存，重试会再次外网实测。"""
    calls = _fake_probe_http([429], monkeypatch)

    rotator = _rotator(tmp_path)
    url = "https://example.test/v1"

    assert await rotator.probe_provider_kind("sk", url, "m") is None
    assert calls["probe_http"] == 2
    assert await rotator.probe_provider_kind("sk", url, "m") is None
    assert calls["probe_http"] == 4
