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

"""API key rotation."""

from __future__ import annotations

__all__ = [
    "AuthMode",
    "AuthProvider",
    "AuthProviderType",
    "AuthRotator",
    "AuthStateSnapshot",
    "AuthStateStore",
    "KeyStatus",
    "ProviderValidation",
    "ValidationStatus",
    "build_default_rotator",
    "default_config_dir",
]

import asyncio
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from workflow_controller.core.auth_rotator.persistence import (
    MAX_PERSISTED_ERROR_CHARS,
    APIKeyStore,
    AuthMode,
    AuthProvider,
    AuthProviderType,
    AuthStateSnapshot,
    AuthStateStore,
    KeyStatus,
    ProviderValidation,
    ValidationStatus,
)

logger = logging.getLogger(__name__)

_RESET_BUFFER_SECONDS: float = 60.0
_INVALID_VALIDATION_RETRY_SECONDS: float = 60.0
_PROBE_KIND_TTL_SECONDS: float = 300.0
# 已知模型的 opencode context/output 上限（models.dev 官方值）。注入 opencode
# provider 定义时写入 model.limit —— opencode 的自动 compaction（compaction.auto）
# 以 model.limit.context 判断上下文是否溢出，limit.context 缺省/为 0 时其触发
# 判断被短路、永不 compact（会话 token 只增不减）。未列出的模型不注入 limit，
# 避免编造未知窗口，保持既往行为。
_KNOWN_MODEL_LIMITS: dict[str, dict[str, int]] = {
    "deepseek-v4-pro": {"context": 1048576, "output": 16384},
}

if TYPE_CHECKING:
    from collections.abc import Callable

    from workflow_controller.core.auth_rotator.key_store import ParsedKey
    from workflow_controller.core.auth_rotator.settings import GlobalSettings
    from workflow_controller.core.auth_rotator.signal import LimitMatch
    from workflow_controller.session.events import Event


#: 只读探针的禁用工具基表（auth 探测与反馈评估两轨共用，按需追加）
PROBE_DISALLOWED_TOOLS = [
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


class AuthRotator:
    """Multi-provider auth state manager.

    Builds env for claude CLI subprocesses based on current active provider.
    Rotates providers on rate limit. Persists everything to auth_state.json.
    """

    # Probe semantics are part of the fingerprint so persisted caches do not outlive changes.
    _FINGERPRINT_VERSION = "provider-capability-v4"

    def __init__(
        self,
        state_path: Path,
        env_path: Path,
        *,
        runtime: str = "claude_sdk",
    ) -> None:
        self._runtime = runtime
        self._state_store = AuthStateStore(state_path)
        self._key_store = APIKeyStore(env_path)
        self._config_dir = state_path.parent

        self._quota_warning_callbacks: list[Callable[..., None]] = []
        self._loop_engine_hook: Callable[[], None] | None = None

        snapshot = self._state_store.load()
        self._mode: AuthMode = snapshot.mode
        self._reset_at: datetime | None = snapshot.reset_at
        self._active_index: int = snapshot.active_provider_index
        self._providers: list[AuthProvider] = snapshot.providers or []
        self._reset_task: asyncio.Task[None] | None = None
        self._last_limit_event_at: float = -999.0
        self._dedup_window_seconds: float = 60.0
        self._last_auth_check_ok: bool = True
        self._last_auth_check_error: str = ""
        self._last_oauth_ok: bool = False
        self._last_oauth_error: str = "not checked"
        self._probe_cache: dict[str, tuple[ProviderValidation, ProviderValidation]] = {}
        # probe_provider_kind 成功结果的短 TTL 缓存（key/base_url/model 绑定）
        self._probed_kinds: dict[str, tuple[str, float]] = {}

        # Load settings.conf keys into providers (merges with persisted state)
        self.sync_providers_from_config()

    @property
    def _active_provider(self) -> AuthProvider:
        if 0 <= self._active_index < len(self._providers):
            return self._providers[self._active_index]
        return AuthProvider(
            provider_type=AuthProviderType.API_KEY, key=None, status=KeyStatus.UNUSED
        )

    # --- Public API --------------------------------------------------------

    @staticmethod
    def _inject_opencode_provider(
        out: dict[str, str],
        provider: AuthProvider,
        base_url: str,
        npm: str,
        model: str,
    ) -> str:
        """把池条目序列化为 opencode provider 定义，并入 OPENCODE_CONFIG_CONTENT。

        model 为已解析的 effective model（provider.model 或 settings.default_model
        回退），不是 provider.model 本身。已有 OPENCODE_CONFIG_CONTENT（driver 的
        build_config_content 也会再合并一次）按 JSON object 合并注入；provider 名
        取 model 的 provider 前缀（volc-gateway/glm-5.2 → volc-gateway），裸 model
        名用 workflow-controller-pool。npm（协议包）由调用方给事实值（探测写回或显式配置），
        本方法不猜。返回注入后的 opencode 路由名（prov/model）：裸 model 合成
        workflow-controller-pool 时与传入 model 不同，调用方需把 -m/ANTHROPIC_MODEL 切到该
        路由，否则 opencode 会回落到默认 provider。
        """
        prov_name = model.split("/", 1)[0] if "/" in model else "workflow-controller-pool"
        model_name = model.split("/", 1)[1] if "/" in model else model
        model_def: dict[str, Any] = {"name": model_name}
        limit = _KNOWN_MODEL_LIMITS.get(model_name)
        if limit is not None:
            model_def["limit"] = limit
        prov_def: dict[str, Any] = {
            "npm": npm,
            "options": {"baseURL": base_url, "apiKey": provider.key},
            "models": {model_name: model_def},
        }
        try:
            existing = json.loads(out.get("OPENCODE_CONFIG_CONTENT", "") or "{}")
            if not isinstance(existing, dict):
                existing = {}
        except ValueError:
            existing = {}
        existing.setdefault("provider", {})
        existing["provider"][prov_name] = prov_def
        out["OPENCODE_CONFIG_CONTENT"] = json.dumps(existing, ensure_ascii=False)
        return f"{prov_name}/{model_name}"

    @staticmethod
    def _validation_cache_fresh(validation: ProviderValidation) -> bool:
        if validation.status == ValidationStatus.VALID:
            return True
        if validation.status != ValidationStatus.INVALID or validation.checked_at is None:
            return False
        return (time.time() - validation.checked_at.timestamp()) < _INVALID_VALIDATION_RETRY_SECONDS

    @staticmethod
    def _sanitize_probe_error(raw: Any, key: str | None, max_chars: int | None = None) -> str:
        if isinstance(raw, str):
            text = raw
        else:
            try:
                text = json.dumps(raw, ensure_ascii=False, default=str)
            except Exception:
                text = str(raw)
        if key:
            text = text.replace(key, "***")
        text = " ".join(text.split())
        if max_chars is not None and len(text) > max_chars:
            text = text[:max_chars] + "..."
        return text

    @staticmethod
    def _normalize_reviewer_probe_error(raw_error: str) -> str:
        from workflow_controller.driver.anthropic_http import (
            ANTHROPIC_MESSAGES_FORMAT_ERROR,
        )

        known_errors = {
            ANTHROPIC_MESSAGES_FORMAT_ERROR,
            "thinking output reached max_tokens",
            "thinking response has no text block",
            "response has no text content",
            "reviewer response has no text content",
        }
        if raw_error in known_errors:
            return raw_error
        status_match = re.search(r"\bHTTP (\d{3})\b", raw_error)
        if status_match:
            status = int(status_match.group(1))
            if status in (401, 403):
                return f"Reviewer authentication failed (HTTP {status})."
            if status == 404:
                return "Reviewer Anthropic Messages endpoint not found (HTTP 404)."
            if status == 429:
                return "Reviewer rate limited (HTTP 429)."
            if status >= 500:
                return f"Reviewer provider error (HTTP {status})."
            return f"Reviewer request rejected (HTTP {status})."
        lowered = raw_error.lower()
        if "token" in lowered and "required" in lowered:
            return "Reviewer credentials are required."
        if any(
            marker in lowered for marker in ("timeout", "timed out", "connecterror", "connection")
        ):
            return "Reviewer provider unreachable."
        return "Reviewer request failed."

    def schedule_pending_resets(self) -> None:
        """Re-schedule reset tasks for providers whose rate limit hasn't expired.

        Called after the asyncio event loop is running to restore reset
        tasks that were lost on server restart.
        """
        now = datetime.now(tz=UTC)
        for p in self._providers:
            if (
                p.status in (KeyStatus.EXHAUSTED, KeyStatus.INVALID)
                and p.reset_at
                and p.reset_at > now
            ):
                self._schedule_reset_task(p.reset_at)
                logger.info(
                    "AuthRotator: re-scheduled reset for %s (reset_at=%s)",
                    p.provider_type.value,
                    p.reset_at.isoformat(),
                )

    def get_mode(self) -> AuthMode:
        return self._mode

    def get_reset_at(self) -> datetime | None:
        return self._reset_at

    def get_key_store_path(self) -> Path:
        return self._key_store.path

    def has_key(self) -> bool:
        """Backward compat: check if any key is available via legacy store or providers."""
        return self.has_available_provider()

    def has_available_provider(self) -> bool:
        """Check if any provider is available (not exhausted/invalid)."""
        return any(p.status in (KeyStatus.UNUSED, KeyStatus.ACTIVE) for p in self._providers)

    def get_active_provider(self) -> AuthProvider:
        if 0 <= self._active_index < len(self._providers):
            return self._providers[self._active_index]
        return AuthProvider(
            provider_type=AuthProviderType.API_KEY, key=None, status=KeyStatus.UNUSED
        )

    def get_active_provider_index(self) -> int:
        return self._active_index

    def get_default_provider_index(
        self,
        runtime: str | None = None,
        capability: str = "worker",
    ) -> int | None:
        candidates: list[int] = [self._active_index, *range(len(self._providers))]
        seen: set[int] = set()
        for index in candidates:
            if index in seen or not (0 <= index < len(self._providers)):
                continue
            seen.add(index)
            if self._provider_usable(self._providers[index], runtime, capability):
                return index
        return None

    def get_provider_count(self) -> int:
        return len(self._providers)

    def get_available_count(self) -> int:
        """Count available providers."""
        return sum(1 for p in self._providers if p.status in (KeyStatus.UNUSED, KeyStatus.ACTIVE))

    def get_exhausted_count(self) -> int:
        return sum(
            1 for p in self._providers if p.status in (KeyStatus.EXHAUSTED, KeyStatus.INVALID)
        )

    def get_providers(self) -> list[AuthProvider]:
        return list(self._providers)

    def get_key_status_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable status dict for API/UI consumption."""
        providers_info = []
        for i, p in enumerate(self._providers):
            info = {
                "index": i,
                "type": p.provider_type.value,
                "status": p.status.value,
                "model": p.model,
                "base_url": p.base_url,
                "runtime": p.runtime or self._runtime,
                "custom_name": p.custom_name,
                "model_catalog_path": p.model_catalog_path,
                "worker_validation": p.worker_validation.to_dict(),
                "reviewer_validation": p.reviewer_validation.to_dict(),
                "is_active": i == self._active_index,
            }
            if p.provider_type == AuthProviderType.API_KEY:
                info["key"] = _mask_key(p.key) if p.key else None
            if p.exhausted_at:
                info["exhausted_at"] = p.exhausted_at.isoformat()
            if p.reset_at:
                info["reset_at"] = p.reset_at.isoformat()
            if p.scope:
                info["scope"] = p.scope
            providers_info.append(info)
        return {
            "mode": self._mode.value,
            "reset_at": self._reset_at.isoformat() if self._reset_at else None,
            "active_provider_index": self._active_index,
            "provider_count": len(self._providers),
            "available_count": self.get_available_count(),
            "exhausted_count": self.get_exhausted_count(),
            "has_api_key": self.has_available_provider(),
            "auth_ok": self._last_auth_check_ok,
            "auth_error": self._last_auth_check_error,
            "oauth_ok": self._last_oauth_ok,
            "oauth_error": self._last_oauth_error,
            "providers": providers_info,
        }

    def build_env(
        self,
        base: dict[str, str],
        provider_index: int | None = None,
    ) -> dict[str, str]:
        """Build env with an explicitly bound provider or the UI default."""
        out = dict(base)
        provider = self.get_provider(provider_index)
        return self._build_api_env(out, provider)

    def get_provider(self, provider_index: int | None = None) -> AuthProvider:
        if provider_index is not None and 0 <= provider_index < len(self._providers):
            return self._providers[provider_index]
        return self._active_provider

    def rotate_bound_provider(
        self,
        provider_index: int,
        *,
        scope: str,
        reset_at: datetime | None = None,
        runtime: str | None = None,
        capability: str = "worker",
        error: str = "",
    ) -> int | None:
        """Exhaust one bound provider and return a replacement index.

        The UI active provider is only changed when it points at the exhausted
        entry. Other session bindings remain untouched.
        """
        if not (0 <= provider_index < len(self._providers)):
            return None
        provider = self._providers[provider_index]
        if scope == "auth_error":
            self._probe_cache.pop(self._provider_fingerprint(provider), None)
            validation = (
                provider.worker_validation
                if capability == "worker"
                else provider.reviewer_validation
            )
            validation.status = ValidationStatus.INVALID
            validation.checked_at = datetime.now(tz=UTC)
            validation.error = self._sanitize_probe_error(
                error or "authentication failed during runtime execution",
                provider.key,
                MAX_PERSISTED_ERROR_CHARS,
            )
        else:
            provider.status = KeyStatus.EXHAUSTED
            provider.exhausted_at = datetime.now(tz=UTC)
            provider.reset_at = reset_at
            provider.scope = scope
        replacement = next(
            (
                idx
                for idx, candidate in enumerate(self._providers)
                if idx != provider_index and self._provider_usable(candidate, runtime, capability)
            ),
            None,
        )
        if replacement is not None and self._active_index == provider_index:
            self._set_active(replacement)
        else:
            self._persist()
        return replacement

    def invalidate_provider_capability(
        self,
        provider_index: int,
        capability: str,
        error: str,
    ) -> None:
        """Invalidate one execution capability without changing quota state."""
        if not (0 <= provider_index < len(self._providers)):
            return
        provider = self._providers[provider_index]
        self._probe_cache.pop(self._provider_fingerprint(provider), None)
        validation = (
            provider.worker_validation if capability == "worker" else provider.reviewer_validation
        )
        validation.status = ValidationStatus.INVALID
        validation.checked_at = datetime.now(tz=UTC)
        validation.error = self._sanitize_probe_error(
            error,
            provider.key,
            MAX_PERSISTED_ERROR_CHARS,
        )
        self._persist()

    def resolve_available_provider(
        self,
        provider_index: int,
        runtime: str | None = None,
        capability: str = "worker",
    ) -> int | None:
        """Return the binding when usable, otherwise select an available replacement."""
        if 0 <= provider_index < len(self._providers):
            provider = self._providers[provider_index]
            if self._provider_usable(provider, runtime, capability):
                return provider_index
        replacement = next(
            (
                idx
                for idx, candidate in enumerate(self._providers)
                if self._provider_usable(candidate, runtime, capability)
            ),
            None,
        )
        if replacement is not None and self._active_index == provider_index:
            self._set_active(replacement)
        return replacement

    def find_alternative_provider(
        self,
        provider_index: int,
        runtime: str | None = None,
        capability: str = "worker",
    ) -> int | None:
        """Find another usable provider without changing global defaults or status."""
        return next(
            (
                idx
                for idx, candidate in enumerate(self._providers)
                if idx != provider_index and self._provider_usable(candidate, runtime, capability)
            ),
            None,
        )

    def env_for_driver(
        self,
        base: dict[str, str],
        provider_index: int | None = None,
    ) -> dict[str, str]:
        """SDK env for a driver query — the four ANTHROPIC_* keys.

        SDK runtime uses this dict as a REPLACEMENT env (no os.environ
        inheritance), so this whitelist is the ONLY path keys travel to
        the SDK. GLM / third-party endpoints authenticate via
        ANTHROPIC_AUTH_TOKEN (Bearer) rather than ANTHROPIC_API_KEY
        (x-api-key); that token is populated by _build_api_env only when a
        custom base_url is set (so native Anthropic keys keep using
        x-api-key). ANTHROPIC_MODEL rides along for CLI/GUI drivers.
        OPENCODE_CONFIG_CONTENT 与 Codex 的 model_catalog_path 是内部 provider
        字段而非进程环境变量，也经此透传；对应 driver 负责消费并在 spawn 前剥离。
        """
        provider_base = {key: value for key, value in base.items() if key != "model_catalog_path"}
        full = self.build_env(provider_base, provider_index)
        sdk_env = {}
        for k in (
            "ANTHROPIC_AUTH_TOKEN",
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_BASE_URL",
            "ANTHROPIC_MODEL",
            "OPENCODE_CONFIG_CONTENT",
            "model_catalog_path",
        ):
            if full.get(k):
                sdk_env[k] = str(full[k])
        return sdk_env

    async def probe_provider_kind(
        self,
        key: str,
        base_url: str,
        model: str = "",
    ) -> str | None:
        """对 base_url 实测协议，返回 opencode 应用的 ai-sdk 包名。

        两种协议打不同路径（OpenAI 兼容 → POST {base}/chat/completions，
        Bearer；Anthropic 兼容 → POST {base}/messages，x-api-key——base_url
        惯例已含版本段 /v1，直接拼 /messages，与 @ai-sdk/anthropic 的拼接
        规则一致）。各发一个 1-token 最小请求，先试 OpenAI 风格再试
        Anthropic（自建网关多为 OpenAI 风格，先试可省一次往返）；2xx 须响应体
        形状与协议匹配（网关把错误包成 200 + 错误 JSON 时状态码不代言协议），
        401/403（端点在、key 问题）即判定协议成立，401 场景注入后 opencode
        行为一致。都失败返回 None（不猜，调用方不注入/报错）。

        成功结果按 key/base_url/model 短 TTL 缓存：test-key 刚实测的协议
        在随后保存同一条目（无显式 kind 的候选）时直接复用，不再二次外网
        探测——否则瞬时失败（429/超时）会让刚测试通过的条目无法保存
        （kind=None 不注入 → worker 验证失败）。失败不缓存，可立即重试。
        """
        import httpx

        cache_key = hashlib.sha256(
            "\0".join([key, base_url, model]).encode("utf-8"),
        ).hexdigest()
        now = time.time()
        cached_kind = _fresh_probed_kind(self._probed_kinds, cache_key, now)
        if cached_kind is not None:
            return cached_kind

        bare_model = model.split("/", 1)[-1] if "/" in model else (model or "gpt-4o-mini")
        probes = _build_kind_probes(base_url, key, bare_model)

        for npm, url, headers, body in probes:
            try:
                async with httpx.AsyncClient(timeout=10.0, trust_env=False) as client:
                    resp = await client.post(url, json=body, headers=headers)
                # 401/403 = 端点存在但鉴权失败 → 端点在，协议对，key 问题
                # 仍视为协议判定成功（注入后 opencode 会带同样的 key，行为一致）
                confirmed = resp.status_code in (401, 403) or (
                    200 <= resp.status_code < 300 and _probe_body_confirms(npm, resp)
                )
                if confirmed:
                    self._probed_kinds[cache_key] = (
                        npm,
                        now + _PROBE_KIND_TTL_SECONDS,
                    )
                    return npm
            except Exception:
                logger.debug("provider kind 探测失败（npm 包 %s），尝试下一候选", npm)
        return None

    async def check_auth(self, runtime: str | None = None) -> tuple[bool, str | None]:
        """Health check. Tests API key (always) and OAuth (CLI only)."""
        self._last_auth_check_ok = False
        self._last_auth_check_error = ""
        self._last_oauth_ok = False
        self._last_oauth_error = ""

        provider = self._active_provider
        settings = self._load_global_settings()
        # The provider's own persisted runtime describes its model format, so it
        # wins over the caller-provided session runtime; both fall back to the
        # boot-time global when absent (legacy entries / env-only setups).
        effective_runtime = provider.runtime or runtime or self._runtime

        await self._check_auth_api_key(provider, settings, effective_runtime)

        # --- test OAuth (CLI only) ---
        if effective_runtime == "claude_cli":
            await self._check_auth_oauth_cli(provider, settings)

        return self._last_auth_check_ok, self._last_auth_check_error or None

    async def test_key(
        self,
        key: str,
        model: str,
        base_url: str,
        runtime: str | None = None,
    ) -> tuple[bool, str | None, str]:
        """Test a specific key/model/base_url combination without affecting providers.

        Returns (success, error_message, response_text).
        """
        from workflow_controller.driver.anthropic_http import AnthropicHttpClient

        if not key:
            return False, "Key is required", ""

        # Build environment for this specific key
        sdk_env: dict[str, str] = {"ANTHROPIC_API_KEY": key}
        if base_url:
            sdk_env["ANTHROPIC_BASE_URL"] = base_url
            sdk_env["ANTHROPIC_AUTH_TOKEN"] = key

        # Use provided model or fall back to default
        settings = self._load_global_settings()
        test_model = self._direct_test_model(
            model or settings.default_model or "claude-sonnet-4-6", runtime
        )

        test_prompt = "你好，请直接回答：你的模型名称、版本、开发公司分别是什么？"

        client = AnthropicHttpClient()
        try:
            result = await client.request(prompt=test_prompt, model=test_model, env=sdk_env)
            if result.is_error:
                return False, result.error, ""
            return True, None, result.text.strip()
        except Exception as e:
            return False, str(e), ""

    async def validate_candidate(
        self,
        key: str,
        model: str,
        base_url: str,
        runtime: str | None,
        *,
        force: bool = False,
        cwd: str | None = None,
        provider_kind: str | None = None,
        model_catalog_path: str | None = None,
    ) -> dict[str, Any]:
        """Validate the two execution paths used by workers and reviewers."""
        from workflow_controller.driver.factory import resolve_runtime

        # 探测与缓存判定用 effective runtime（resolve_runtime，WORKFLOW_CONTROLLER_DRIVER
        # 覆盖同样生效，与 create_driver/_build_api_env 同规则）：全局
        # claude_sdk + 覆盖 opencode 时 worker 实跑 opencode，按原始 runtime
        # 判定会跳过无显式 kind 候选的协议探测（kind=None 不注入、driver
        # 回落默认 provider）。
        effective_runtime = resolve_runtime(runtime, default=self._runtime)
        provider = _candidate_provider(
            key, model, base_url, effective_runtime, provider_kind, model_catalog_path
        )
        # opencode 运行时先实测端点协议（OpenAI 兼容 vs Anthropic），写回
        # provider_kind——注入只信这个事实，不做 URL 猜测。探测失败不拦
        # 整体验证（端点可能只支持其中一种协议而模型探测走另一路），
        # kind=None 时 _build_api_env 会跳过注入并在日志提示。
        # 探测先于 fingerprint/缓存：kind 参与 fingerprint，未显式给 kind 的
        # 候选每次先实测再查缓存，协议不同即指纹不同、VALID 缓存不串。
        # base_url 与 _build_api_env 同规则解析 effective 值；Codex 必须显式
        # 绑定 provider base_url，不继承全局默认 URL。
        effective_base_url = provider.base_url or (
            "" if effective_runtime == "codex_cli" else self._load_global_settings().default_url
        )
        catalog_status: str | None = None
        catalog_name: str | None = None
        if effective_runtime == "codex_cli":
            catalog_status, catalog_path, catalog_display = await _resolve_codex_catalog_entry(
                model=model, config_dir=self._config_dir, provided_path=model_catalog_path or ""
            )
            if catalog_status in ("custom_required", "custom_invalid"):
                return await self._catalog_invalid_payload(
                    provider,
                    effective_runtime,
                    catalog_status,
                )
            if catalog_path is not None:
                provider.model_catalog_path = catalog_path
                catalog_name = catalog_display
        if effective_runtime == "opencode":
            await self._maybe_probe_kind(
                provider, key=key, model=model, base_url=effective_base_url
            )
        return await self._probe_validations(
            provider,
            runtime=effective_runtime,
            catalog_status=catalog_status,
            catalog_name=catalog_name,
            force=force,
            cwd=cwd,
        )

    async def validate_provider(
        self,
        provider_index: int,
        *,
        force: bool = False,
        cwd: str | None = None,
    ) -> dict[str, Any]:
        if not (0 <= provider_index < len(self._providers)):
            raise ValueError(f"invalid provider index: {provider_index}")
        provider = self._providers[provider_index]
        provider.runtime = provider.runtime or self._runtime
        fingerprint = self._provider_fingerprint(provider)
        cached_current = (
            provider.worker_validation.fingerprint == fingerprint
            and provider.reviewer_validation.fingerprint == fingerprint
            and self._validation_cache_fresh(provider.worker_validation)
            and self._validation_cache_fresh(provider.reviewer_validation)
        )
        if cached_current and not force:
            return self._validation_payload(provider, fingerprint)
        entry_kind = provider.provider_kind
        result = await self.validate_candidate(
            provider.key or "",
            provider.model or "",
            provider.base_url or "",
            provider.runtime,
            force=force,
            cwd=cwd,
            provider_kind=entry_kind,
            model_catalog_path=provider.model_catalog_path,
        )
        # 探测写回持久化 provider：本次 worker 验证依赖的临时 kind 必须落到
        # provider 上，否则后续 opencode session 的注入会因无 kind 被跳过；
        # kind 只对该次验证解析的 effective URL 有效，一并记录。入参无 kind
        # 时结果是本次实测所得，来源记 "probe"——sync 据此把它当探测缓存
        # （显式配置被删除时可清掉）而非不可撤销的显式值。
        self._apply_candidate_result(provider_index, provider, result, entry_kind)
        self._persist()
        return result

    async def ensure_provider_capability(
        self,
        provider_index: int,
        capability: str,
        *,
        runtime: str | None = None,
        cwd: str | None = None,
    ) -> None:
        provider = self.get_provider(provider_index)
        if capability == "worker" and runtime and (provider.runtime or self._runtime) != runtime:
            raise ValueError(
                f"provider {provider_index} is bound to {provider.runtime or self._runtime}, not {runtime}"
            )
        result = await self.validate_provider(provider_index, cwd=cwd)
        validation = result["worker" if capability == "worker" else "reviewer"]
        if validation.get("status") != ValidationStatus.VALID.value:
            error = validation.get("error") or f"{capability} validation failed"
            raise ValueError(f"provider {provider_index} {capability} validation failed: {error}")

    async def set_keys_validated(self, keys: list[dict[str, str]]) -> None:
        """Validate entries independently before replacing settings.conf."""
        validated: list[dict[str, Any]] = []
        normalized: list[dict[str, str]] = []
        kind_sources: list[str | None] = []
        used_existing: set[int] = set()
        seen_fingerprints: set[str] = set()
        claimed_custom_names: set[str] = set()
        for position, raw in enumerate(keys):
            resolved = await _resolve_settings_entry(
                raw, config_dir=self._config_dir, default_runtime=self._runtime
            )
            if resolved is None:
                continue
            _, entry, candidate = resolved
            existing, kind_source, fingerprint = self._entry_kind_context(
                candidate, entry, used_existing, seen_fingerprints
            )
            custom_name = entry["custom_name"]
            if custom_name:
                if custom_name in claimed_custom_names:
                    raise ValueError(f"自定义名重复：{custom_name}")
            else:
                # 整池提交：已有条目优先继承旧名（旧客户端未回传时不重测不重名）；
                # 新条目模型名未被任何自定义名占用即自动命名，否则要求手动输入。
                # 模型是可选字段：空模型以条目序号命名，不强制手动输入。
                inherited = (existing.custom_name or "").strip() if existing else ""
                auto_name = entry["model"].strip() or f"provider-{position + 1}"
                if inherited and inherited not in claimed_custom_names:
                    custom_name = inherited
                elif auto_name not in claimed_custom_names:
                    custom_name = auto_name
                else:
                    raise ValueError(f"模型名 {auto_name} 已被自定义名占用，请输入自定义名")
            claimed_custom_names.add(custom_name)
            entry["custom_name"] = custom_name
            if existing is not None and _existing_validations_match(existing, fingerprint):
                result = self._validation_payload(existing, fingerprint)
            else:
                result = await self.validate_candidate(
                    entry["key"],
                    entry["model"],
                    entry["base_url"],
                    entry["runtime"],
                    provider_kind=candidate.provider_kind,
                    model_catalog_path=candidate.model_catalog_path,
                )
            if result.get("provider_kind") and kind_source is None:
                # 候选验证时无 kind，结果里的 kind 是本次实测探测所得
                kind_source = "probe"
            if not result.get("success"):
                error = (
                    result["worker"].get("error")
                    or result["reviewer"].get("error")
                    or "no usable worker or reviewer capability"
                )
                if existing is None:
                    raise ValueError(f"provider validation failed: {error}")
                logger.warning(
                    "existing provider %s validation failed (saved state): %s",
                    entry["model"] or "provider",
                    error,
                )
            elif result["worker"].get("status") != ValidationStatus.VALID.value:
                logger.warning(
                    "provider %s worker validation failed (saved reviewer-only): %s",
                    entry["model"] or "provider",
                    result.get("error") or "unknown error",
                )
            validated.append(result)
            kind_sources.append(kind_source)
            normalized.append({k: v for k, v in entry.items() if v})

        self.set_keys(normalized)
        self._writeback_probed_kinds(validated, kind_sources)
        self._persist()

    def activate_provider(self, index: int) -> bool:
        """Directly activate the provider at the given index.

        Only providers with status UNUSED or ACTIVE can be activated.
        Returns True if the switch was successful.
        """
        if not (0 <= index < len(self._providers)):
            return False
        p = self._providers[index]
        if p.status not in (KeyStatus.UNUSED, KeyStatus.ACTIVE):
            return False
        if index == self._active_index:
            return True  # already active
        self._set_active(index)
        logger.info(
            "activate_provider: switched to %d (%s | %s)",
            index,
            p.provider_type.value,
            p.model or "(default)",
        )
        return True

    def rotate_to_first_available(self) -> bool:
        """Scan all providers, skip current, switch to the first available one.

        Scans from index 0 upwards. Skips the active index. Any provider
        with status UNUSED or ACTIVE qualifies.
        Returns True if a rotation occurred, False if no other provider
        is available (stuck on current).
        """
        if not self._providers:
            return False
        current = self._active_index
        for i, p in enumerate(self._providers):
            if i == current:
                continue
            if p.status in (KeyStatus.UNUSED, KeyStatus.ACTIVE):
                self._set_active(i)
                logger.info(
                    "rotate_to_first_available: %d (%s) → %d (%s)",
                    current,
                    self._providers[current].provider_type.value,
                    i,
                    p.provider_type.value,
                )
                return True
        logger.warning(
            "rotate_to_first_available: %d stuck — no other provider available",
            current,
        )
        return False

    def sync_providers_from_config(self) -> None:
        """Rebuild the providers list from settings.conf (the single source of
        truth for keys/models), preserving per-key runtime status.

        Keys absent from settings.conf are dropped — auth_state.json never
        revives a key the user has removed. First-run migration of a legacy
        .env key is handled once by GlobalSettings.load().
        """
        from workflow_controller.core.auth_rotator.key_store import parse_keys
        from workflow_controller.core.auth_rotator.settings import GlobalSettings

        settings = GlobalSettings.load(self._config_dir / "settings.conf")
        parsed = parse_keys(settings.api_keys)

        old_providers = list(self._providers)
        used_old: set[int] = set()
        new_providers: list[AuthProvider] = []

        for position, pk in enumerate(parsed):
            existing_index = self._match_old_provider(old_providers, pk, used_old, position)
            existing = old_providers[existing_index] if existing_index is not None else None
            kind_url = (pk.base_url or settings.default_url) or None
            if existing is not None and existing_index is not None:
                new_providers.append(self._synced_provider(existing, pk, kind_url))
            else:
                new_providers.append(_new_provider_from_entry(pk, kind_url))

        self._providers = new_providers
        self._ensure_unique_custom_names()
        self._reconcile_after_sync()

    def delete_provider(self, provider_index: int) -> bool:
        """Remove one configured provider without probing the remaining pool."""
        if not 0 <= provider_index < len(self._providers):
            return False
        from workflow_controller.core.auth_rotator.settings import GlobalSettings

        settings = GlobalSettings.load(self._config_dir / "settings.conf")
        if provider_index >= len(settings.api_keys):
            return False
        entry = settings.api_keys[provider_index]
        provider = self._providers[provider_index]
        entry_matches = (
            entry.get("key") == provider.key
            and (entry.get("model") or "") == (provider.model or "")
            and (entry.get("base_url") or "") == (provider.base_url or "")
            and (entry.get("runtime") or self._runtime) == (provider.runtime or self._runtime)
            and (entry.get("provider_kind") or None)
            == (provider.provider_kind if provider.provider_kind_source == "explicit" else None)
        )
        if not entry_matches:
            return False
        settings.api_keys.pop(provider_index)
        settings.save(self._config_dir / "settings.conf")
        if provider_index < self._active_index:
            self._active_index -= 1
        elif provider_index == self._active_index:
            self._active_index = 0
        self.sync_providers_from_config()
        return True

    def rotate_to_next_key(self) -> bool:
        """Rotate to the next available provider. Returns False if none available."""
        # Forward from current+1 to end
        for i in range(self._active_index + 1, len(self._providers)):
            p = self._providers[i]
            if p.status in (KeyStatus.UNUSED,):
                self._set_active(i)
                return True

        # Wrap from 0 back to current
        for i in range(0, self._active_index):
            p = self._providers[i]
            if p.status in (KeyStatus.UNUSED,):
                self._set_active(i)
                return True

        # Stale exhausted with passed reset_at
        for i, p in enumerate(self._providers):
            if p.status == KeyStatus.EXHAUSTED and p.reset_at and p.reset_at < datetime.now(tz=UTC):
                p.scope = None
                p.reset_at = None
                self._set_active(i)
                return True

        return False

    def set_keys(self, keys: list[dict[str, str]]) -> None:
        """Save new key list, reset exhausted state.

        Each entry is a dict with 'key' (required), 'model' (required), 'base_url' (required).
        """
        from workflow_controller.core.auth_rotator.settings import GlobalSettings

        settings = GlobalSettings.load(self._config_dir / "settings.conf")
        settings.api_keys = keys
        settings.save(self._config_dir / "settings.conf")
        if not keys:
            self._key_store.set("")
            self._key_store.set_model(None)
        self.sync_providers_from_config()

    def set_api_key(self, key: str) -> None:
        """Persist a single key (backward compat)."""
        self._key_store.set(key)
        self.sync_providers_from_config()

    # --- Key / provider management -----------------------------------------

    def set_global_settings(self, **kwargs: Any) -> None:
        """Update global settings.conf fields."""
        from workflow_controller.core.auth_rotator.settings import GlobalSettings

        settings = GlobalSettings.load(self._config_dir / "settings.conf")
        for k, v in kwargs.items():
            if hasattr(settings, k) and v is not None:
                setattr(settings, k, v)
        settings.save(self._config_dir / "settings.conf")
        if "default_url" in kwargs:
            # default_url 是省略 base_url 条目的 effective URL 组成部分：
            # 变了即可能换端点/换协议——sync 按 provider_kind_url 清失效
            # kind，指纹含 effective URL 使旧 VALID 验证不再复用。
            self.sync_providers_from_config()

    def get_global_settings_dict(self) -> dict[str, Any]:
        s = self._load_global_settings()
        masked_keys = []
        provider_index = 0
        for entry in s.api_keys:
            key = entry.get("key", "")
            masked_entry: dict[str, str] = {"key": _mask_key_for_display(key)}
            if entry.get("model"):
                masked_entry["model"] = entry["model"]
            if entry.get("base_url"):
                masked_entry["base_url"] = entry["base_url"]
            if entry.get("runtime"):
                masked_entry["runtime"] = entry["runtime"]
            custom_name = entry.get("custom_name")
            if not custom_name and key.strip():
                provider = (
                    self._providers[provider_index]
                    if provider_index < len(self._providers)
                    else None
                )
                custom_name = provider.custom_name if provider is not None else None
                provider_index += 1
            if custom_name:
                masked_entry["custom_name"] = custom_name
            masked_keys.append(masked_entry)
        return {
            "api_keys": masked_keys,
            "default_model": s.default_model,
            "default_url": s.default_url,
        }

    def get_api_keys_for_ui(self) -> list[dict[str, Any]]:
        """Return structured key list for settings UI."""
        from workflow_controller.core.auth_rotator.settings import GlobalSettings

        settings = GlobalSettings.load(self._config_dir / "settings.conf")
        result = []
        provider_index = 0
        for entry in settings.api_keys:
            key = entry.get("key", "")
            if not key.strip():
                continue
            item: dict[str, Any] = {
                "key": key,
                "masked": _mask_key_for_display(key),
                "provider_index": provider_index,
            }
            if entry.get("model"):
                item["model"] = entry["model"]
            if entry.get("base_url"):
                item["base_url"] = entry["base_url"]
            if entry.get("runtime"):
                item["runtime"] = entry["runtime"]
            if entry.get("model_catalog_path"):
                item["model_catalog_path"] = entry["model_catalog_path"]
            if entry.get("provider_kind"):
                item["provider_kind"] = entry["provider_kind"]
            provider = (
                self._providers[provider_index] if provider_index < len(self._providers) else None
            )
            # conf 未回写回填名时以 provider 侧（auth_state.json）为准。
            if entry.get("custom_name") or (provider is not None and provider.custom_name):
                item["custom_name"] = entry.get("custom_name") or provider.custom_name
            if provider is not None:
                item["worker_validation"] = provider.worker_validation.to_dict()
                item["reviewer_validation"] = provider.reviewer_validation.to_dict()
            result.append(item)
            provider_index += 1
        return result

    def rotate_key(self) -> bool:
        """Rotate to next available API key. Returns True if rotated."""
        self._cancel_reset_task()
        self._reset_at = None
        for i in range(1, len(self._providers)):
            idx = (self._active_index + i) % len(self._providers)
            if self._providers[idx].status in (KeyStatus.UNUSED, KeyStatus.ACTIVE):
                self._set_active(idx)
                self._fire_loop_engine_hook()
                return True
        return False

    def set_loop_engine_hook(self, hook: Callable[[], None] | None) -> None:
        self._loop_engine_hook = hook

    def set_quota_warning_callback(self, cb: Callable[..., None]) -> None:
        self._quota_warning_callbacks.append(cb)

    def on_quota_warning(
        self, *, utilization: float, scope: str, reset_at: datetime | None
    ) -> None:
        for cb in list(self._quota_warning_callbacks):
            try:
                cb(utilization=utilization, scope=scope, reset_at=reset_at)
            except Exception:
                logger.exception("quota_warning_callback failed")

    def on_limit_detected(self, match: LimitMatch, *, reset_at: datetime | None) -> None:
        now = time.monotonic()
        if now - self._last_limit_event_at < self._dedup_window_seconds:
            logger.info("AuthRotator: limit dedup window active — ignoring")
            return
        self._last_limit_event_at = now

        if match.scope == "rate_limit":
            logger.warning("AuthRotator: API rate_limit hit — staying on %s", self._mode.value)
            return

        if match.scope == "auth_error":
            self.rotate_bound_provider(
                self._active_index,
                scope="auth_error",
                runtime=self._active_provider.runtime or self._runtime,
                error=match.source,
            )
            return

        if match.scope in ("weekly", "5h"):
            self._handle_limit_detected(scope=match.scope, reset_at=reset_at)
            return

    # --- Mode control ------------------------------------------------------

    def on_rate_limit(self, *, scope: str, reset_at: datetime | None) -> None:
        now = time.monotonic()
        if now - self._last_limit_event_at < self._dedup_window_seconds:
            return
        self._last_limit_event_at = now

        if scope in ("5h", "weekly"):
            self._handle_limit_detected(scope=scope, reset_at=reset_at)
            return

        logger.warning("AuthRotator.on_rate_limit: unknown scope=%r", scope)

    def reviewer_model(self, provider_index: int | None, fallback: str = "") -> str:
        provider = self.get_provider(provider_index)
        return self._direct_test_model(
            provider.model or fallback,
            provider.runtime or self._runtime,
        )

    def reviewer_thinking_type(
        self,
        provider_index: int | None,
        *,
        requested_mode: str = "disabled",
    ) -> str | None:
        """Resolve a requested thinking shape against the provider capability set."""
        validation = self.get_provider(provider_index).reviewer_validation
        modes = validation.thinking_modes
        if not modes:
            return (
                requested_mode
                if requested_mode
                in (
                    "disabled",
                    "enabled",
                    "adaptive",
                    "omitted",
                )
                else "disabled"
            )
        requested = requested_mode or "disabled"
        if requested in modes:
            return requested
        preferences = {
            "enabled": ("enabled", "adaptive", "omitted", "disabled"),
            "adaptive": ("adaptive", "omitted", "enabled", "disabled"),
            "omitted": ("omitted", "adaptive", "disabled", "enabled"),
            "disabled": ("disabled", "omitted", "adaptive", "enabled"),
        }
        preference = preferences.get(requested, preferences["disabled"])
        return next(mode for mode in preference if mode in modes)

    def _build_api_env(self, out: dict[str, str], provider: AuthProvider) -> dict[str, str]:
        from workflow_controller.driver.factory import resolve_runtime

        settings = self._load_global_settings()
        key = provider.key
        if not key:
            return out
        out["ANTHROPIC_API_KEY"] = key
        runtime = resolve_runtime(provider.runtime, default=self._runtime)
        base_url = provider.base_url
        if runtime != "codex_cli":
            base_url = base_url or settings.default_url
        if base_url:
            out["ANTHROPIC_BASE_URL"] = base_url
            out["ANTHROPIC_AUTH_TOKEN"] = key
        elif runtime == "codex_cli":
            out.pop("ANTHROPIC_BASE_URL", None)
            out.pop("ANTHROPIC_AUTH_TOKEN", None)
        model = (
            provider.model if runtime == "codex_cli" else provider.model or settings.default_model
        )
        if model:
            out["ANTHROPIC_MODEL"] = model
        elif runtime == "codex_cli":
            out.pop("ANTHROPIC_MODEL", None)
        if runtime == "codex_cli" and provider.model_catalog_path:
            out["model_catalog_path"] = provider.model_catalog_path
        # opencode 运行时：把本条 provider 同时注入 OPENCODE_CONFIG_CONTENT——
        # opencode 子进程不读密钥池的 ANTHROPIC_* env，其 provider 定义来自项目
        # .opencode/*.json；经此注入后池条目与项目配置合并生效（不同名并存，
        # 同名项目优先），任意 project dir 下池条目模型均可被 -m 选用。
        # 协议（npm 包）只信两个事实来源：探测写回的 provider_kind，或
        # settings.conf 显式配置——不做 URL 猜测（猜错协议注入即坏）。
        # 定义用已解析的 effective model（provider.model 或 settings.default_model
        # 回退）；裸 model 合成 workflow-controller-pool 路由后 ANTHROPIC_MODEL 同步改写，
        # -m 才能命中注入的 provider。
        # 缺省 runtime 与本类其余路径同语义（_provider_usable/_probe_worker/
        # check_auth 均按 self._runtime 回退）：全局 runtime=opencode 时，省略
        # runtime 的条目同样注入。effective runtime 经 resolve_runtime 与
        # create_driver/session 判定同规则（WORKFLOW_CONTROLLER_DRIVER 覆盖同样生效）：
        # 否则全局 claude_sdk + WORKFLOW_CONTROLLER_DRIVER=opencode 时 driver 已切 opencode
        # 而 env 仍按 claude_sdk 生成，裸 model 丢注入定义、回落默认 provider。
        if runtime == "opencode" and base_url and model:
            # kind 只对 provider_kind_url（实测/显式配置时的 effective URL）
            # 有效：URL 已变而 sync 未及清理时（如运行中手改 settings.conf 的
            # default_url），不把旧协议注入新端点。
            kind = provider.provider_kind
            if kind and provider.provider_kind_url not in (None, base_url):
                kind = None
            if kind:
                route = self._inject_opencode_provider(
                    out,
                    provider,
                    base_url,
                    kind,
                    model,
                )
                if route != model:
                    out["ANTHROPIC_MODEL"] = route
            else:
                logger.debug(
                    "opencode provider %s: no probed provider_kind, skipping "
                    "OPENCODE_CONFIG_CONTENT injection (run test-key to probe)",
                    model,
                )
        return out

    def _persist_provider_catalog_path(
        self,
        provider_index: int,
        model_catalog_path: str,
    ) -> None:
        from workflow_controller.core.auth_rotator.settings import GlobalSettings

        settings_path = self._config_dir / "settings.conf"
        settings = GlobalSettings.load(settings_path)
        if not 0 <= provider_index < len(settings.api_keys):
            logger.warning(
                "cannot persist Codex catalog path for missing provider %s",
                provider_index,
            )
            return
        entry = settings.api_keys[provider_index]
        if entry.get("model_catalog_path") == model_catalog_path:
            return
        entry["model_catalog_path"] = model_catalog_path
        settings.save(settings_path)

    # --- Rate limit handling -----------------------------------------------

    async def _probe_worker(
        self,
        provider: AuthProvider,
        *,
        cwd: str | None = None,
    ) -> ProviderValidation:
        fingerprint = self._provider_fingerprint(provider)
        capability = f"worker/{provider.runtime or self._runtime}"
        checked_at = datetime.now(tz=UTC)
        # driver/error/saw_success 经 dict 传出：超时取消协程后宿主仍能拿到
        # 已创建的 driver 做清理、保留取消前已记录的成功信号
        state: dict[str, Any] = {"driver": None, "error": "", "saw_success": False}

        try:
            if cwd:
                await asyncio.wait_for(self._consume_worker_probe(provider, cwd, state), timeout=45)
            else:
                with tempfile.TemporaryDirectory(
                    prefix="workflow-controller-provider-probe-",
                    ignore_cleanup_errors=True,
                ) as tmp:
                    await asyncio.wait_for(
                        self._consume_worker_probe(provider, tmp, state), timeout=45
                    )
        except TimeoutError:
            state["error"] = "worker validation timed out after 45s"
        except Exception as exc:
            state["error"] = self._sanitize_probe_error(str(exc), provider.key)
        finally:
            driver = state["driver"]
            if driver is not None:
                try:
                    await driver.stop()
                    await driver.kill()
                except Exception:
                    logger.debug("provider probe cleanup failed", exc_info=True)

        if not state["saw_success"] and not state["error"]:
            state["error"] = "worker validation ended without a successful step"
        return ProviderValidation(
            capability=capability,
            status=ValidationStatus.VALID if state["saw_success"] else ValidationStatus.INVALID,
            fingerprint=fingerprint,
            checked_at=checked_at,
            error=state["error"],
        )

    async def _consume_worker_probe(
        self,
        provider: AuthProvider,
        probe_cwd: str,
        state: dict[str, Any],
    ) -> None:
        """执行一次 worker 探测查询；进度经 state 传出（超时取消后宿主仍可见）。"""
        from workflow_controller.driver.factory import create_driver, resolve_runtime

        runtime = resolve_runtime(provider.runtime, default=self._runtime)
        driver = create_driver(runtime)
        state["driver"] = driver
        # 探测目录是内部 scratch 目录而非用户选定的 workspace：claude_cli 的
        # 信任预写在此关闭，避免每次校验把临时目录累积写进用户全局 Claude 配置
        query_kwargs: dict[str, Any] = {"pre_trust": False} if runtime == "claude_cli" else {}
        env = self._build_api_env({}, provider)
        probe = _probe_settings(runtime, self._runtime)
        async for event in driver.query(
            "Reply with exactly OK. Do not use tools.",
            # env 的 ANTHROPIC_MODEL 已解析 default_model 回退与裸 model 的
            # workflow-controller-pool 合成路由，优先于 provider.model，-m 才命中注入的
            # provider（否则测的是 opencode 默认 provider）
            model=env.get("ANTHROPIC_MODEL") or provider.model or "",
            cwd=probe_cwd,
            max_turns=probe.max_turns,
            allowed_tools=probe.allowed_tools,
            disallowed_tools=probe.disallowed_tools,
            permission_mode=probe.permission_mode,
            env=env,
            **query_kwargs,
        ):
            if self._consume_probe_event(event, provider, probe, state):
                break

    def _consume_probe_event(
        self,
        event: Event,
        provider: AuthProvider,
        probe: _ProbeSettings,
        state: dict[str, Any],
    ) -> bool:
        """消费单个探测事件：进度写 state，返回是否终止探测流。"""
        from workflow_controller.session.events import EventType

        if event.event_type in (EventType.ERROR, EventType.RATE_LIMIT):
            raw_error = event.data.get("error", event.data)
            state["error"] = self._sanitize_probe_error(raw_error, provider.key)
            return True
        if event.event_type == EventType.STEP_FINISH:
            if event.data.get("is_error"):
                state["error"] = self._sanitize_probe_error(event.data, provider.key)
                return True
            state["saw_success"] = True
            return not probe.codex_probe
        if event.event_type == EventType.ASSISTANT_MESSAGE:
            # opencode v2 的纯文本回合不再发 step_finish（带工具的
            # 回合才有）：模型的文本回复即 provider 可用的成功信号
            state["saw_success"] = True
            return not probe.codex_probe
        return False

    async def _probe_reviewer(
        self,
        provider: AuthProvider,
    ) -> ProviderValidation:
        from workflow_controller.driver.anthropic_http import ANTHROPIC_MESSAGES_FORMAT_ERROR

        fingerprint = self._provider_fingerprint(provider)
        checked_at = datetime.now(tz=UTC)
        env = self._build_api_env({}, provider)
        model = self._direct_test_model(
            provider.model or env.get("ANTHROPIC_MODEL", ""),
            provider.runtime,
        )
        errors: list[str] = []
        supported_modes: list[str] = []
        for mode in ("disabled", "enabled", "adaptive", "omitted"):
            raw_error = await self._probe_reviewer_mode(provider, env, model, mode)
            if not raw_error:
                supported_modes.append(mode)
                continue
            error = self._normalize_reviewer_probe_error(raw_error)
            logger.debug(
                "reviewer probe mode=%s failed: %s",
                mode,
                raw_error,
            )
            errors.append(error)
        error = errors[-1] if errors else ""
        if (
            error.startswith("Reviewer request rejected")
            and ANTHROPIC_MESSAGES_FORMAT_ERROR in errors
        ):
            error = ANTHROPIC_MESSAGES_FORMAT_ERROR
        if supported_modes:
            error = ""
        return ProviderValidation(
            capability="anthropic_http",
            status=ValidationStatus.VALID if supported_modes else ValidationStatus.INVALID,
            fingerprint=fingerprint,
            checked_at=checked_at,
            error=error,
            thinking_mode=supported_modes[0] if supported_modes else "",
            thinking_modes=supported_modes,
        )

    async def _probe_reviewer_mode(
        self,
        provider: AuthProvider,
        env: dict[str, str],
        model: str,
        mode: str,
    ) -> str:
        from workflow_controller.driver.anthropic_http import AnthropicHttpClient

        try:
            result = await AnthropicHttpClient(timeout=45).request(
                prompt="Reply with exactly OK.",
                model=model,
                env=env,
                thinking_type=mode,
                thinking_budget_tokens=2048,
            )
            if result.is_error:
                return self._sanitize_probe_error(result.error, provider.key)
            if not result.text.strip():
                return "reviewer response has no text content"
        except Exception as exc:
            return self._sanitize_probe_error(str(exc), provider.key)
        return ""

    def _provider_fingerprint(
        self,
        provider: AuthProvider,
        *,
        include_kind: bool = True,
    ) -> str:
        # provider_kind 决定 opencode 实际加载的 ai-sdk 包（npm/provider 行为
        # 不同），必须入指纹：显式改 kind 不能命中旧 VALID 缓存。base_url 与
        # _build_api_env 同规则取 effective 值：依赖默认 URL 的非 Codex 条目
        # 在 default_url 变化后指纹必须变化，否则旧验证跨端点复用。
        # runtime 同经 resolve_runtime 取 effective 值（WORKFLOW_CONTROLLER_DRIVER 覆盖
        # 生效）：覆盖切换改变 worker 实际 runtime，claude 的 VALID 缓存不得
        # 在 opencode 覆盖下复用，反之亦然。
        # include_kind=False 供「配置身份」匹配（set_keys_validated 复用
        # 已有条目）：探测 kind 只存 auth_state，条目回传不带它。
        from workflow_controller.driver.factory import resolve_runtime

        runtime = resolve_runtime(provider.runtime, default=self._runtime)
        base_url = provider.base_url or (
            "" if runtime == "codex_cli" else self._load_global_settings().default_url
        )
        model_catalog_path = provider.model_catalog_path or "" if runtime == "codex_cli" else ""
        parts = [
            provider.key or "",
            provider.model or "",
            base_url,
            runtime,
            model_catalog_path,
        ]
        if include_kind:
            parts.append(provider.provider_kind or "")
        material = [self._FINGERPRINT_VERSION, *parts]
        return hashlib.sha256("\0".join(material).encode("utf-8")).hexdigest()

    def _validation_payload(
        self,
        provider: AuthProvider,
        fingerprint: str,
    ) -> dict[str, Any]:
        success = (
            provider.worker_validation.status == ValidationStatus.VALID
            or provider.reviewer_validation.status == ValidationStatus.VALID
        )
        return {
            "success": success,
            "runtime": provider.runtime or self._runtime,
            "fingerprint": fingerprint,
            "worker": provider.worker_validation.to_dict(),
            "reviewer": provider.reviewer_validation.to_dict(),
            "provider_kind": provider.provider_kind,
            "error": provider.worker_validation.error,
        }

    async def _check_auth_api_key(
        self,
        provider: AuthProvider,
        settings: GlobalSettings,
        effective_runtime: str,
    ) -> None:
        """Test the API key path of check_auth; results land on the _last_auth_check_* flags."""
        from workflow_controller.driver.anthropic_http import AnthropicHttpClient

        # --- test API key ---
        # Providers (settings.conf) OR env credentials (ANTHROPIC_AUTH_TOKEN /
        # ANTHROPIC_API_KEY) both qualify — env_for_driver passes the latter to
        # the SDK, so a host with only GLM/third-party env set (no settings.conf)
        # still gets tested here instead of being short-circuited to "No API key".
        if (
            self._providers
            or os.environ.get("ANTHROPIC_AUTH_TOKEN")
            or os.environ.get("ANTHROPIC_API_KEY")
        ):
            env = dict(os.environ)
            built = self.build_env(env)
            sdk_env: dict[str, str] = {}
            for k in (
                "ANTHROPIC_AUTH_TOKEN",
                "ANTHROPIC_API_KEY",
                "ANTHROPIC_BASE_URL",
                "ANTHROPIC_MODEL",
            ):
                if built.get(k):
                    sdk_env[k] = built[k]
            model = (
                provider.model or settings.default_model or os.environ.get("ANTHROPIC_MODEL") or ""
            )
            model = self._direct_test_model(model, effective_runtime)
            client = AnthropicHttpClient()
            try:
                result = await client.request(prompt="hi", model=model, env=sdk_env)
                if result.is_error:
                    self._last_auth_check_error = result.error
                else:
                    self._last_auth_check_ok = True
                    self._last_auth_check_error = ""
                    self._reactivate_exhausted_active()
            except Exception as e:
                self._last_auth_check_error = str(e)
        else:
            self._last_auth_check_error = "No API key configured"

    async def _check_auth_oauth_cli(
        self,
        provider: AuthProvider,
        settings: GlobalSettings,
    ) -> None:
        """claude_cli runtime 的 OAuth 健康探测：一次 hi 查询按 STEP_FINISH 判定。"""
        from workflow_controller.driver.factory import create_driver
        from workflow_controller.session.events import EventType

        cli_driver = create_driver("claude_cli", permission_mode="bypassPermissions")
        model = provider.model or settings.default_model or "claude-sonnet-4-6"
        try:
            async for event in cli_driver.query(prompt="hi", model=model, max_turns=1, env={}):
                if event.event_type == EventType.STEP_FINISH:
                    self._record_oauth_step(event)
                    break
        except Exception as e:
            self._last_oauth_error = str(e)

    def _record_oauth_step(self, event: Event) -> None:
        """按 STEP_FINISH 事件的 is_error 记录 OAuth 探测结果。"""
        if event.data.get("is_error"):
            self._last_oauth_error = str(event.data.get("result") or "unknown")
        else:
            self._last_oauth_ok = True

    def _reactivate_exhausted_active(self) -> None:
        """Reset the active provider to ACTIVE after a successful key check."""
        if self._providers and 0 <= self._active_index < len(self._providers):
            p = self._providers[self._active_index]
            if p.status in (KeyStatus.EXHAUSTED, KeyStatus.INVALID):
                p.status = KeyStatus.ACTIVE
                p.exhausted_at = None
                p.reset_at = None
                p.scope = None
                self._persist()

    async def _maybe_probe_kind(
        self,
        provider: AuthProvider,
        *,
        key: str,
        model: str,
        base_url: str,
    ) -> None:
        """opencode 候选无显式 kind 且有 base_url 时实测端点协议并写回。"""
        if not base_url or provider.provider_kind:
            return
        probed = await self.probe_provider_kind(key, base_url, model)
        if probed:
            provider.provider_kind = probed
            logger.info("probed provider_kind for %s: %s", base_url, probed)

    async def _probe_validations(
        self,
        provider: AuthProvider,
        *,
        runtime: str,
        catalog_status: str | None,
        catalog_name: str | None,
        force: bool,
        cwd: str | None,
    ) -> dict[str, Any]:
        """按指纹取缓存或并发实测 worker/reviewer，组装候选验证结果。"""
        fingerprint = self._provider_fingerprint(provider)
        cached = self._probe_cache.get(fingerprint)
        if cached is not None and not force:
            worker, reviewer = cached
        else:
            worker, reviewer = await asyncio.gather(
                self._probe_worker(provider, cwd=cwd),
                self._probe_reviewer(provider),
            )
            if (
                worker.status == ValidationStatus.VALID
                and reviewer.status == ValidationStatus.VALID
            ):
                self._probe_cache[fingerprint] = (worker, reviewer)
        payload = {
            "success": (
                worker.status == ValidationStatus.VALID or reviewer.status == ValidationStatus.VALID
            ),
            "runtime": runtime,
            "catalog_status": catalog_status,
            "model_catalog_name": catalog_name,
            "model_catalog_path": provider.model_catalog_path,
            "fingerprint": fingerprint,
            "worker": worker.to_dict(),
            "reviewer": reviewer.to_dict(),
            "provider_kind": provider.provider_kind,
            "error": worker.error,
        }
        # 未设置的可选字段（非 codex/opencode 时为 None）不返回 key：typed 响应的
        # exclude_unset 才能真正省略；能力项内部的显式 null（如 checked_at）仍保留。
        return {key: value for key, value in payload.items() if value is not None}

    async def _catalog_invalid_payload(
        self,
        provider: AuthProvider,
        effective_runtime: str,
        catalog_status: str,
    ) -> dict[str, Any]:
        """Codex catalog preflight 失败时仍独立给出 Reviewer 探测结果。"""
        from workflow_controller.driver.codex_catalog import (
            INVALID_MODEL_CATALOG_ERROR,
            MODEL_CATALOG_ERROR,
        )

        catalog_error = (
            MODEL_CATALOG_ERROR
            if catalog_status == "custom_required"
            else INVALID_MODEL_CATALOG_ERROR
        )
        reviewer = await self._probe_reviewer(provider)
        worker = ProviderValidation(
            capability=f"worker/{effective_runtime}",
            status=ValidationStatus.INVALID,
            fingerprint=reviewer.fingerprint,
            checked_at=reviewer.checked_at,
            error=catalog_error,
        )
        payload = {
            "success": reviewer.status == ValidationStatus.VALID,
            "runtime": effective_runtime,
            "catalog_status": catalog_status,
            "fingerprint": reviewer.fingerprint,
            "worker": worker.to_dict(),
            "reviewer": reviewer.to_dict(),
            "provider_kind": provider.provider_kind,
            "error": catalog_error,
        }
        return {key: value for key, value in payload.items() if value is not None}

    def _apply_candidate_result(
        self,
        provider_index: int,
        provider: AuthProvider,
        result: dict[str, Any],
        entry_kind: str | None,
    ) -> None:
        """把 validate_candidate 结果写回 provider（kind/catalog 路径/两项验证）。"""
        if result.get("provider_kind"):
            provider.provider_kind = result["provider_kind"]
            provider.provider_kind_url = (
                provider.base_url or self._load_global_settings().default_url
            ) or None
            if not entry_kind:
                provider.provider_kind_source = "probe"
        if result.get("model_catalog_path"):
            provider.model_catalog_path = str(result["model_catalog_path"])
            self._persist_provider_catalog_path(
                provider_index,
                provider.model_catalog_path,
            )
        provider.worker_validation = ProviderValidation.from_dict(
            result["worker"],
            f"worker/{provider.runtime}",
        )
        provider.reviewer_validation = ProviderValidation.from_dict(
            result["reviewer"],
            "anthropic_http",
        )

    def _entry_kind_context(
        self,
        candidate: AuthProvider,
        entry: dict[str, str],
        used_existing: set[int],
        seen_fingerprints: set[str],
    ) -> tuple[AuthProvider | None, str | None, str]:
        """匹配已有条目、继承仍有效的探测 kind 并登记候选指纹。"""
        # 已有条目按不含 kind 的配置身份匹配：探测 kind 只存 auth_state，
        # UI 回传条目不带它，若按含 kind 的指纹匹配，每次保存都会因
        # 「条目无 kind vs provider 有 kind」触发无谓的重新探测。
        existing_index = None
        for index, provider in enumerate(self._providers):
            if index not in used_existing and self._provider_fingerprint(
                provider, include_kind=False
            ) == self._provider_fingerprint(candidate, include_kind=False):
                existing_index = index
                break
        existing = self._providers[existing_index] if existing_index is not None else None
        if existing_index is not None:
            used_existing.add(existing_index)
        # 无显式 kind 时继承仍新鲜（provider_kind_url == 当前
        # effective URL）的探测缓存：kind 对该 URL 依旧是实测事实，
        # 继承后即可复用其 VALID 验证；URL 已变则不继承，
        # validate_candidate 对新 URL 重新实测协议。显式来源
        # （provider_kind_source == "explicit"）的旧 kind 不继承——
        # 条目不带 kind 保存即用户删除了显式纠偏，须重新探测而非沿用。
        inherited = False
        candidate_needs_kind = not candidate.provider_kind
        existing_kind_inheritable = (
            existing is not None
            and existing.provider_kind
            and existing.provider_kind_source != "explicit"
            and (existing.provider_kind_url or "")
            == ((entry["base_url"] or self._load_global_settings().default_url) or "")
        )
        if candidate_needs_kind and existing is not None and existing_kind_inheritable:
            candidate.provider_kind = existing.provider_kind
            inherited = True
        if entry["provider_kind"]:
            kind_source: str | None = "explicit"
        elif inherited:
            kind_source = (existing.provider_kind_source if existing else None) or "probe"
        else:
            kind_source = None
        fingerprint = self._provider_fingerprint(candidate)
        if fingerprint in seen_fingerprints:
            raise ValueError(
                "duplicate provider configuration: key, model, base_url and runtime must be unique"
            )
        seen_fingerprints.add(fingerprint)
        return existing, kind_source, fingerprint

    def _writeback_probed_kinds(
        self,
        validated: list[dict[str, Any]],
        kind_sources: list[str | None],
    ) -> None:
        """把探测/继承出的 kind 与两项验证结果写回保存后的 provider。"""
        # 探测/继承出的 kind 写回 provider（auth_state.json，含其生效
        # URL 与来源）：后续 opencode 注入与指纹以 provider 上的值为准，
        # settings.conf 条目保持无 kind（显式配置除外）。无 kind 事实
        # 来源时清空旧值：显式纠偏被删除且未重新探测出 kind，残留会被
        # validate_provider 当已知协议跳过 probe。
        for provider, saved_result, kind_source in zip(
            self._providers,
            validated,
            kind_sources,
            strict=True,
        ):
            if saved_result.get("provider_kind"):
                provider.provider_kind = saved_result["provider_kind"]
                provider.provider_kind_url = (
                    provider.base_url or self._load_global_settings().default_url
                ) or None
                provider.provider_kind_source = kind_source
            else:
                provider.provider_kind = None
                provider.provider_kind_url = None
                provider.provider_kind_source = None
            provider.worker_validation = ProviderValidation.from_dict(
                saved_result["worker"],
                f"worker/{provider.runtime}",
            )
            provider.reviewer_validation = ProviderValidation.from_dict(
                saved_result["reviewer"],
                "anthropic_http",
            )

    def _match_old_provider(
        self,
        old_providers: list[AuthProvider],
        pk: ParsedKey,
        used_old: set[int],
        position: int,
    ) -> int | None:
        """按下标优先、其次配置形状匹配旧条目，命中即登记 used_old。"""
        for index, provider in enumerate(old_providers):
            same_shape = (
                provider.model == pk.model
                and provider.base_url == pk.base_url
                and (provider.runtime or self._runtime) == (pk.runtime or self._runtime)
                and provider.model_catalog_path == pk.model_catalog_path
            )
            slot_unused = index not in used_old
            same_key = provider.key == pk.key
            position_or_shape = index == position or same_shape
            if slot_unused and same_key and position_or_shape:
                used_old.add(index)
                return index
        return None

    def _synced_provider(
        self,
        existing: AuthProvider,
        pk: ParsedKey,
        kind_url: str | None,
    ) -> AuthProvider:
        """把 settings.conf 条目合并进匹配到的旧 provider，保留探测 kind 缓存语义。"""
        old_fingerprint = self._provider_fingerprint(existing)
        worker_fingerprint_current = existing.worker_validation.fingerprint == old_fingerprint
        reviewer_fingerprint_current = existing.reviewer_validation.fingerprint == old_fingerprint
        existing.model = pk.model
        existing.base_url = pk.base_url
        existing.runtime = pk.runtime or existing.runtime
        existing.model_catalog_path = pk.model_catalog_path
        # conf 条目未写自定义名时保留旧值（存量回填/手动命名不因 sync 丢失）。
        existing.custom_name = pk.custom_name or existing.custom_name
        # settings.conf 里的 kind 一律是显式配置（探测值只存
        # auth_state.json 的 provider，set_keys_validated 不写回
        # conf）：显式配置直接按当前 effective URL 生效。conf 未写
        # kind 时保留 provider 上已探测的值，避免一次 sync 把探测
        # 结果抹掉；但 kind 是对特定 effective URL（条目 base_url
        # 或全局 default_url）的实测事实：URL 变了旧 kind 即失效——
        # 保留它会让 validate_provider 把非空旧 kind 当显式值跳过重
        # 探测，新 URL 被注入成旧 npm 包。provider_kind_url 记录
        # kind 生效时的 URL，据此比较即可同时覆盖省略 base_url、
        # 改 default_url 的条目。此时清空，等验证重新探测。
        # URL 未变也可能要清：provider_kind_source == "explicit"
        # 说明旧 kind 来自显式配置，conf 不再写它即配置被删除——
        # 不能当探测缓存保留（否则错误的人工纠偏删不掉，验证会把
        # 非空旧 kind 当已知协议跳过 probe）。只有来源为探测
        # （或历史未记录来源）且 URL 未变的 kind 才作为缓存继承。
        if pk.provider_kind:
            existing.provider_kind = pk.provider_kind
            existing.provider_kind_url = kind_url
            existing.provider_kind_source = "explicit"
        elif (existing.provider_kind_url or "") != (
            kind_url or ""
        ) or existing.provider_kind_source == "explicit":
            existing.provider_kind = None
            existing.provider_kind_url = None
            existing.provider_kind_source = None
        if (
            self._provider_fingerprint(existing) != old_fingerprint
            or not worker_fingerprint_current
        ):
            existing.worker_validation = ProviderValidation(
                capability=f"worker/{existing.runtime}",
            )
        if (
            self._provider_fingerprint(existing) != old_fingerprint
            or not reviewer_fingerprint_current
        ):
            existing.reviewer_validation = ProviderValidation(
                capability="anthropic_http",
            )
        if existing.status == KeyStatus.INVALID and existing.scope == "auth_error":
            existing.status = KeyStatus.UNUSED
            existing.exhausted_at = None
            existing.scope = None
        return existing

    def _reconcile_after_sync(self) -> None:
        """清过期重置、收敛 ACTIVE 唯一性并按需轮转激活条目。"""
        if self._active_index >= len(self._providers):
            self._active_index = 0
        # Clear stale exhaustion for all providers whose reset_at has passed
        now = datetime.now(tz=UTC)
        for p in self._providers:
            if p.reset_at and p.reset_at < now:
                if p.status == KeyStatus.EXHAUSTED:
                    p.status = KeyStatus.UNUSED
                p.reset_at = None
                p.scope = None
        # Ensure exactly one provider is ACTIVE
        for i, p in enumerate(self._providers):
            if p.status == KeyStatus.ACTIVE and i != self._active_index:
                p.status = KeyStatus.UNUSED

        if not self._providers:
            self._persist()
            return

        active = self._providers[self._active_index]
        if active.status in (KeyStatus.EXHAUSTED, KeyStatus.INVALID):
            # Active provider is exhausted/invalid — try to rotate to an available one
            found = False
            for i, p in enumerate(self._providers):
                if i != self._active_index and p.status == KeyStatus.UNUSED:
                    self._set_active(i)
                    found = True
                    break
            if not found:
                # No available provider; leave the exhausted one as-is and persist
                self._persist()
        else:
            active.status = KeyStatus.ACTIVE
            self._persist()

    def _ensure_unique_custom_names(self) -> None:
        """加载期回填/收敛自定义名，保证池内全局唯一。

        显式名先到先得；缺失时取模型名，被占用则取第一个空闲的
        <模型名>-backup-<N>（N 从 2 递增），模型也缺失时以
        provider-<序号> 为基底。按池内顺序确定性分配，多次加载结果
        一致；结果随 _persist 写 auth_state.json，settings.conf 不回写，
        下次 sync 由同规则重现（显式名仍从条目带出）。
        """
        used: set[str] = set()
        for index, provider in enumerate(self._providers):
            requested = (provider.custom_name or "").strip()
            if requested and requested not in used:
                name = requested
            else:
                base = (provider.model or "").strip() or f"provider-{index + 1}"
                name = base if base not in used else _free_backup_name(base, used)
            used.add(name)
            provider.custom_name = name

    # --- Internal ----------------------------------------------------------

    def _handle_limit_detected(self, *, scope: str, reset_at: datetime | None) -> None:
        """Handle 5h or weekly limit — mark exhausted, rotate, or fallback."""
        logger.info(
            "AuthRotator: limit detected scope=%s reset=%s active=%d mode=%s",
            scope,
            reset_at,
            self._active_index,
            self._mode.value,
        )
        self._mark_exhausted(scope=scope, reset_at=reset_at)
        if scope == "5h":
            self._reset_at = reset_at
            self._schedule_reset_task(reset_at)
        else:
            self._cancel_reset_task()
            self._reset_at = None

        if not self._try_rotate_or_fallback():
            return

    def _try_rotate_or_fallback(self) -> bool:
        """Try rotate; returns True if key was rotated."""
        return self._try_rotate()

    def _direct_test_model(self, model: str, runtime: str | None = None) -> str:
        """Drop the opencode provider route prefix for direct API probes.

        provider/model（如 zhipuai-coding-plan/glm-5.2）的前缀只是给 opencode
        CLI 的路由标记，仅 opencode runtime 采用该约定；其他 runtime 的模型
        ID 可能合法含 /（如 OpenRouter 的 anthropic/claude-sonnet-4），
        必须原样直传，否则探测打到错误模型上、误标 provider exhausted。

        runtime 优先级：provider 持久化 runtime（模型格式归 provider 自己
        定，check_auth 按 active provider 取）> 显式传入（web 侧按用户/会话
        选择的运行时，test_key 用）> 启动时的全局 runtime——会话 runtime
        逐会话可选，与全局值无关。
        """
        if (runtime or self._runtime) == "opencode" and "/" in model:
            return model.split("/", 1)[1]
        return model

    def _set_active(self, index: int) -> None:
        if self._active_index < len(self._providers):
            old = self._providers[self._active_index]
            if old.status == KeyStatus.ACTIVE:
                old.status = KeyStatus.UNUSED
        self._active_index = index
        if 0 <= index < len(self._providers):
            self._providers[index].status = KeyStatus.ACTIVE
        self._persist()

    def _try_rotate(self) -> bool:
        """Try to rotate to next key. If Plan index 0, try API keys first."""
        return self.rotate_to_next_key()

    def _mark_exhausted(
        self,
        *,
        scope: str,
        reset_at: datetime | None = None,
    ) -> None:
        p = self._active_provider
        p.status = KeyStatus.EXHAUSTED if scope != "auth_error" else KeyStatus.INVALID
        p.exhausted_at = datetime.now(tz=UTC)
        p.reset_at = reset_at
        p.scope = scope
        self._persist()

    def _load_global_settings(self) -> GlobalSettings:
        from workflow_controller.core.auth_rotator.settings import GlobalSettings

        return GlobalSettings.load(self._config_dir / "settings.conf")

    def _schedule_reset_task(self, reset_at: datetime | None) -> None:
        self._cancel_reset_task()
        if reset_at is None:
            return
        delay_seconds = (reset_at - datetime.now(tz=UTC)).total_seconds() + _RESET_BUFFER_SECONDS
        if delay_seconds <= 0:
            self._reset_at = None
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._reset_task = loop.create_task(self._sleep_and_reset(delay_seconds))

    async def _sleep_and_reset(self, delay_seconds: float) -> None:
        try:
            await asyncio.sleep(delay_seconds)
        except asyncio.CancelledError:
            return
        if self._mode == AuthMode.API and self._reset_at is not None:
            self._reset_at = None
        self._persist()

    def _cancel_reset_task(self) -> None:
        if self._reset_task is not None and not self._reset_task.done():
            self._reset_task.cancel()
        self._reset_task = None

    def _persist(self) -> None:
        snap = AuthStateSnapshot(
            mode=self._mode,
            reset_at=self._reset_at,
            active_provider_index=self._active_index,
            providers=self._providers,
        )
        try:
            self._state_store.save(snap)
        except OSError as exc:
            logger.warning("Failed to persist auth_state: %s", exc)

    def _provider_usable(
        self,
        provider: AuthProvider,
        runtime: str | None,
        capability: str,
    ) -> bool:
        if provider.status not in (KeyStatus.UNUSED, KeyStatus.ACTIVE):
            return False
        if capability == "worker":
            effective_runtime = runtime or self._runtime
            if (provider.runtime or self._runtime) != effective_runtime:
                return False
            return provider.worker_validation.status != ValidationStatus.INVALID
        return provider.reviewer_validation.status != ValidationStatus.INVALID

    def _fire_loop_engine_hook(self) -> None:
        """轮换生效后触发 LoopEngine 回调（回调失败只记日志不阻断）。"""
        if self._loop_engine_hook is not None:
            try:
                self._loop_engine_hook()
            except Exception:
                logger.exception("LoopEngine hook failed")


class _ProbeSettings(NamedTuple):
    """worker 探测查询的 runtime 差异参数（codex 与其余 runtime 探测面不同）。"""

    codex_probe: bool
    max_turns: int | None
    allowed_tools: list[str]
    disallowed_tools: list[str]
    permission_mode: str


def _probe_settings(runtime: str, default_runtime: str) -> _ProbeSettings:
    """按 effective runtime 一次判定探测查询参数。"""
    from workflow_controller.driver.factory import resolve_runtime

    codex_probe = resolve_runtime(runtime, default=default_runtime) == "codex_cli"
    if codex_probe:
        return _ProbeSettings(
            codex_probe=True,
            max_turns=None,
            allowed_tools=[],
            disallowed_tools=[],
            permission_mode="read-only",
        )
    return _ProbeSettings(
        codex_probe=False,
        max_turns=1,
        allowed_tools=["Read"],
        disallowed_tools=list(PROBE_DISALLOWED_TOOLS),
        permission_mode="",
    )


def _fresh_probed_kind(
    cache: dict[str, tuple[str, float]],
    cache_key: str,
    now: float,
) -> str | None:
    """命中未过期缓存时返回 kind；过期则清掉该缓存项。"""
    cached = cache.get(cache_key)
    if cached is None:
        return None
    kind, expires_at = cached
    if expires_at > now:
        return kind
    cache.pop(cache_key, None)
    return None


def _build_kind_probes(
    base_url: str,
    key: str,
    bare_model: str,
) -> list[tuple[str, str, dict[str, str], dict[str, Any]]]:
    """两种协议的最小探测请求定义（OpenAI 兼容在前）。"""
    return [
        (
            "@ai-sdk/openai-compatible",
            f"{base_url.rstrip('/')}/chat/completions",
            {"Authorization": f"Bearer {key}"},
            {
                "model": bare_model,
                "max_tokens": 1,
                "messages": [{"role": "user", "content": "1"}],
            },
        ),
        (
            "@ai-sdk/anthropic",
            f"{base_url.rstrip('/')}/messages",
            {"x-api-key": key, "anthropic-version": "2023-06-01"},
            {
                "model": bare_model,
                "max_tokens": 1,
                "messages": [{"role": "user", "content": "1"}],
            },
        ),
    ]


def _probe_body_confirms(npm: str, resp: Any) -> bool:
    """2xx 应答的响应体形状核对。

    部分网关把路由/鉴权错误包成 HTTP 200 + 错误 JSON（如
    bigmodel 的 ``{"code":500,"msg":"404 NOT_FOUND"}``）——只看状态码
    会把任意路径都判成「协议成立」，选错 npm 注入即坏。按各自协议
    的成功响应体特征核对：OpenAI 兼容有 ``choices``，Anthropic 有
    ``content``/``type=="message"``。
    """
    try:
        payload = resp.json()
    except ValueError:
        return False
    if not isinstance(payload, dict):
        return False
    if npm == "@ai-sdk/openai-compatible":
        return isinstance(payload.get("choices"), list)
    return payload.get("type") == "message" or "content" in payload


def _candidate_provider(
    key: str,
    model: str,
    base_url: str,
    runtime: str,
    provider_kind: str | None,
    model_catalog_path: str | None,
) -> AuthProvider:
    """Build the candidate AuthProvider for a validation round."""
    return AuthProvider(
        provider_type=AuthProviderType.API_KEY,
        key=key,
        model=model or None,
        base_url=base_url or None,
        runtime=runtime,
        provider_kind=provider_kind or None,
        model_catalog_path=model_catalog_path or None,
    )


async def _resolve_codex_catalog_entry(
    *,
    model: str,
    config_dir: Path,
    provided_path: str,
) -> tuple[str, str | None, str | None]:
    """to_thread 解析 Codex model catalog，返回 (status, 解析路径, preset 名)。"""
    from workflow_controller.driver.codex_catalog import resolve_codex_catalog

    resolution = await asyncio.to_thread(
        resolve_codex_catalog,
        model=model,
        config_dir=config_dir,
        provided_path=provided_path,
    )
    resolved_path = (
        str(resolution.model_catalog_path) if resolution.model_catalog_path is not None else None
    )
    return resolution.status, resolved_path, resolution.preset_display_name


async def _resolve_settings_entry(
    raw: dict[str, str],
    *,
    config_dir: Path,
    default_runtime: str,
) -> tuple[str, dict[str, str], AuthProvider] | None:
    """解析一条待保存条目：catalog 路径、settings.conf entry 与候选 provider。"""
    key = str(raw.get("key") or "").strip()
    if not key:
        return None
    runtime = str(raw.get("runtime") or default_runtime).strip()
    model_catalog_path = (
        str(raw.get("model_catalog_path") or "").strip() if runtime == "codex_cli" else ""
    )
    if runtime == "codex_cli":
        _catalog_status, resolved_path, _ = await _resolve_codex_catalog_entry(
            model=str(raw.get("model") or "").strip(),
            config_dir=config_dir,
            provided_path=model_catalog_path,
        )
        model_catalog_path = resolved_path or ""
    entry = {
        "key": key,
        "model": str(raw.get("model") or "").strip(),
        "base_url": str(raw.get("base_url") or "").strip(),
        "runtime": runtime,
        "custom_name": str(raw.get("custom_name") or "").strip(),
        "model_catalog_path": model_catalog_path,
        # 条目里的 kind 只应是 settings.conf 的显式配置（端点风格
        # 特殊时手工纠偏），原样持久化；探测出的 kind 只落
        # auth_state.json 的 provider（保存后写回），不写
        # settings.conf——探测值混进 conf 后与显式配置无法区分，
        # 换 URL 时会被 sync 当显式值重新绑定到新端点而不是清空
        # 重探测（Anthropic→OpenAI 可稳定复现注入错包）。
        "provider_kind": str(raw.get("provider_kind") or "").strip(),
    }
    candidate = _candidate_provider(
        key,
        entry["model"],
        entry["base_url"],
        runtime,
        entry["provider_kind"],
        model_catalog_path,
    )
    return key, entry, candidate


def _existing_validations_match(existing: AuthProvider, fingerprint: str) -> bool:
    """已有条目任一可用能力的验证指纹与候选一致时可复用其结果。

    保存入口会整池提交（新增条目也会携带全部已有条目）。若只有双 VALID
    才复用，历史上一侧失败的条目在每次保存新增 Provider 时都会被重测；
    重测失败会中断整池保存，令用户误以为正在添加的新条目不可用。历史
    至少一侧 VALID 已证明该条目具备可用能力，且配置指纹未变，复用其
    结果；需要重新检测时用户可用行级「重新测试」强制执行。
    """
    worker = existing.worker_validation
    reviewer = existing.reviewer_validation
    return (worker.fingerprint == fingerprint and worker.status == ValidationStatus.VALID) or (
        reviewer.fingerprint == fingerprint and reviewer.status == ValidationStatus.VALID
    )


def _new_provider_from_entry(pk: ParsedKey, kind_url: str | None) -> AuthProvider:
    """settings.conf 新条目对应的初始 UNUSED provider。"""
    return AuthProvider(
        provider_type=AuthProviderType.API_KEY,
        key=pk.key,
        model=pk.model,
        base_url=pk.base_url,
        runtime=pk.runtime,
        custom_name=pk.custom_name,
        model_catalog_path=pk.model_catalog_path,
        provider_kind=pk.provider_kind,
        provider_kind_url=kind_url if pk.provider_kind else None,
        provider_kind_source="explicit" if pk.provider_kind else None,
        status=KeyStatus.UNUSED,
    )


def _free_backup_name(base: str, used: set[str]) -> str:
    """取第一个空闲的 <base>-backup-<N>（N 从 2 递增）。"""
    n = 2
    while f"{base}-backup-{n}" in used:
        n += 1
    return f"{base}-backup-{n}"


def _mask_key(key: str | None) -> str | None:
    if not key:
        return None
    if len(key) <= 8:
        return "****"
    return f"{key[:4]}...{key[-4:]}"


def _mask_key_for_display(raw: str) -> str:
    """Mask the key portion of a 'key | model' line for UI display."""
    key_part = raw.split("|")[0].strip()
    masked = _mask_key(key_part)
    if "|" in raw:
        return f"{masked} | {raw.split('|', 1)[1].strip()}"
    return masked or ""


def default_config_dir() -> Path:
    from platformdirs import user_config_dir

    return Path(user_config_dir("workflow-controller", appauthor=False))


def build_default_rotator() -> AuthRotator:
    cfg = default_config_dir()
    rotator = AuthRotator(state_path=cfg / "auth_state.json", env_path=cfg / ".env")
    rotator.sync_providers_from_config()
    return rotator
