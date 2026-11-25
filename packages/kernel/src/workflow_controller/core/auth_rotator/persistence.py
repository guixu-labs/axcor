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

"""APIKeyStore + AuthStateStore — load/save key and mode state from disk.

Updated for multi-provider: AuthStateSnapshot now carries a provider list
instead of a single key + exhausted_keys dict.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

MAX_PERSISTED_ERROR_CHARS = 10_000


_KEY_LINE_RE = re.compile(r"^\s*ANTHROPIC_API_KEY\s*=\s*[\"']?(?P<value>[^\"'\s]+)[\"']?\s*$")
_URL_LINE_RE = re.compile(r"^\s*ANTHROPIC_BASE_URL\s*=\s*[\"']?(?P<value>[^\"'\s]+)[\"']?\s*$")
_MODEL_LINE_RE = re.compile(r"^\s*ANTHROPIC_MODEL\s*=\s*[\"']?(?P<value>[^\"'\s]+)[\"']?\s*$")


class AuthMode(Enum):
    """鉴权模式枚举。历史上定义在包 ``__init__``，为解开 ``__init__`` 顶部
    re-export 与本模块间的循环依赖下沉至此（``__init__`` 再 re-export 回去，
    外部 ``from workflow_controller.core.auth_rotator import AuthMode`` 路径不变）。
    """

    API = "API"


class APIKeyStore:
    """Legacy single-key store — reads/writes one key in a .env file.
    Kept for backward compatibility during migration.
    """

    def __init__(self, env_path: Path) -> None:
        self._path = env_path

    @property
    def path(self) -> Path:
        return self._path

    def get(self) -> str | None:
        return self._get(_KEY_LINE_RE)

    def set(self, key: str) -> None:
        self._set("ANTHROPIC_API_KEY", _KEY_LINE_RE, key)

    def get_base_url(self) -> str | None:
        return self._get(_URL_LINE_RE)

    def set_base_url(self, url: str | None) -> None:
        self._set("ANTHROPIC_BASE_URL", _URL_LINE_RE, url)

    def get_model(self) -> str | None:
        return self._get(_MODEL_LINE_RE)

    def set_model(self, model: str | None) -> None:
        self._set("ANTHROPIC_MODEL", _MODEL_LINE_RE, model)

    def _get(self, regex: re.Pattern[str]) -> str | None:
        if not self._path.exists():
            return None
        try:
            content = self._path.read_text(encoding="utf-8")
        except OSError:
            return None
        for line in content.splitlines():
            m = regex.match(line)
            if m:
                return m.group("value")
        return None

    def _set(self, varname: str, regex: re.Pattern[str], value: str | None) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []
        if self._path.exists():
            for line in self._path.read_text(encoding="utf-8").splitlines():
                if regex.match(line):
                    continue
                lines.append(line)
        if value:
            lines.append(f"{varname}={value}")
        self._path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        with contextlib.suppress(OSError):
            self._path.chmod(0o600)


# ---------------------------------------------------------------------------
# AuthProvider & AuthStateStore
# ---------------------------------------------------------------------------


class AuthProviderType(Enum):
    API_KEY = "api_key"


class KeyStatus(Enum):
    UNUSED = "unused"
    ACTIVE = "active"
    EXHAUSTED = "exhausted"
    INVALID = "invalid"


class ValidationStatus(Enum):
    UNTESTED = "untested"
    VALID = "valid"
    INVALID = "invalid"


@dataclass
class ProviderValidation:
    capability: str
    status: ValidationStatus = ValidationStatus.UNTESTED

    fingerprint: str = ""

    checked_at: datetime | None = None

    error: str = ""
    thinking_mode: str = ""
    thinking_modes: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Any, capability: str) -> ProviderValidation:
        if not isinstance(raw, dict):
            return cls(capability=capability)
        try:
            status = ValidationStatus(raw.get("status", "untested"))
        except ValueError:
            status = ValidationStatus.UNTESTED
        thinking_mode = str(raw.get("thinking_mode") or "")
        raw_modes = raw.get("thinking_modes")
        thinking_modes = (
            [str(mode) for mode in raw_modes if isinstance(mode, str)]
            if isinstance(raw_modes, list)
            else []
        )
        if not thinking_modes and thinking_mode:
            thinking_modes = [thinking_mode]
        return cls(
            capability=str(raw.get("capability") or capability),
            status=status,
            fingerprint=str(raw.get("fingerprint") or ""),
            checked_at=_parse_dt(raw.get("checked_at")),
            error=str(raw.get("error") or ""),
            thinking_mode=thinking_mode,
            thinking_modes=thinking_modes,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "capability": self.capability,
            "status": self.status.value,
            "fingerprint": self.fingerprint,
            "checked_at": self.checked_at.isoformat() if self.checked_at else None,
            "error": self.error,
            "thinking_mode": self.thinking_mode,
            "thinking_modes": self.thinking_modes,
        }


@dataclass
class AuthProvider:
    provider_type: AuthProviderType

    status: KeyStatus = KeyStatus.UNUSED

    key: str | None = None

    model: str | None = None

    base_url: str | None = None

    runtime: str | None = None

    # 全局唯一的显示名（新建会话下拉/Provider 列表用），空值在加载时回填。
    custom_name: str | None = None

    # Codex CLI 专用的内部模型 catalog 路径；不进入 provider/子进程环境。
    model_catalog_path: str | None = None

    # opencode 注入时显式指定 ai-sdk 包（@ai-sdk/openai-compatible 或
    # @ai-sdk/anthropic），覆盖 _opencode_provider_npm 的 URL 特征判定；
    # 端点风格特殊时在 settings.conf 条目上配 provider_kind 纠偏。
    provider_kind: str | None = None

    # kind 的来源与生效 URL：显式配置（settings.conf）或探测写回（本文件），
    # 记录探测/配置时的 effective base_url（条目 base_url 或全局
    # default_url）——sync/注入据此判定改 URL 后旧 kind 是否失效，协议是
    # 对特定端点的实测事实，不能跟着 key 跨端点复用。
    provider_kind_url: str | None = None

    # kind 的来源："explicit"（settings.conf 显式配置）或 "probe"（协议实测
    # 写回）。显式配置被删除（conf 不再写 kind）时 sync 必须清掉旧值而非当
    # 探测缓存保留——否则错误的人工纠偏无法撤销（验证把非空旧 kind 当已知
    # 协议跳过重探测）。None 为历史状态，按探测缓存处理。
    provider_kind_source: str | None = None

    exhausted_at: datetime | None = None

    reset_at: datetime | None = None

    scope: str | None = None

    worker_validation: ProviderValidation = field(
        default_factory=lambda: ProviderValidation(capability="worker"),
    )

    reviewer_validation: ProviderValidation = field(
        default_factory=lambda: ProviderValidation(capability="anthropic_http"),
    )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> AuthProvider:
        try:
            provider_type = AuthProviderType(d.get("type", "api_key"))
        except ValueError:
            provider_type = AuthProviderType.API_KEY
        return cls(
            provider_type=provider_type,
            status=KeyStatus(d.get("status", "unused")),
            key=d.get("key"),
            model=d.get("model"),
            base_url=d.get("base_url"),
            runtime=d.get("runtime"),
            custom_name=d.get("custom_name"),
            model_catalog_path=d.get("model_catalog_path"),
            provider_kind=d.get("provider_kind"),
            provider_kind_url=d.get("provider_kind_url"),
            provider_kind_source=d.get("provider_kind_source"),
            exhausted_at=_parse_dt(d.get("exhausted_at")),
            reset_at=_parse_dt(d.get("reset_at")),
            scope=d.get("scope"),
            worker_validation=ProviderValidation.from_dict(
                d.get("worker_validation"),
                "worker",
            ),
            reviewer_validation=ProviderValidation.from_dict(
                d.get("reviewer_validation"),
                "anthropic_http",
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.provider_type.value,
            "status": self.status.value,
            "key": self.key,
            "model": self.model,
            "base_url": self.base_url,
            "runtime": self.runtime,
            "custom_name": self.custom_name,
            "model_catalog_path": self.model_catalog_path,
            "provider_kind": self.provider_kind,
            "provider_kind_url": self.provider_kind_url,
            "provider_kind_source": self.provider_kind_source,
            "exhausted_at": self.exhausted_at.isoformat() if self.exhausted_at else None,
            "reset_at": self.reset_at.isoformat() if self.reset_at else None,
            "scope": self.scope,
            "worker_validation": self.worker_validation.to_dict(),
            "reviewer_validation": self.reviewer_validation.to_dict(),
        }


def _parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    # 历史版本写入 naive 本地时间串，按本地时区补全；全量 aware 写入一个版本后可删除
    return parsed if parsed.tzinfo is not None else parsed.astimezone()


@dataclass
class AuthStateSnapshot:
    mode: AuthMode = AuthMode.API
    reset_at: datetime | None = None
    active_provider_index: int = 0
    providers: list[AuthProvider] = field(default_factory=list)
    last_limit_event: dict[str, Any] | None = None


class AuthStateStore:
    """Persists auth state to a json file."""

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> AuthStateSnapshot:
        if not self._path.exists():
            return AuthStateSnapshot()
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("auth_state.json corrupted (%s), defaulting", exc)
            return AuthStateSnapshot()

        mode_str = data.get("mode", "API")
        legacy_map = {"FORCED_PLAN": "API", "FORCED_API": "API"}
        mode_str = legacy_map.get(mode_str, mode_str)
        try:
            mode = AuthMode(mode_str)
        except ValueError:
            logger.warning("auth_state.json unknown mode %r, defaulting", mode_str)
            mode = AuthMode.API

        reset_at_str = data.get("reset_at")
        reset_at = None
        if reset_at_str:
            try:
                reset_at = datetime.fromisoformat(reset_at_str)
            except ValueError:
                logger.warning("auth_state.json unparseable reset_at %r", reset_at_str)
            else:
                if reset_at is not None and reset_at.tzinfo is None:
                    # 历史版本写入 naive 本地时间串，按本地时区补全；全量 aware 写入一个版本后可删除
                    reset_at = reset_at.astimezone()

        providers = []
        raw_providers = data.get("providers")
        if isinstance(raw_providers, list) and raw_providers:
            providers = [AuthProvider.from_dict(p) for p in raw_providers]
        elif "exhausted_keys" in data:
            # Migrate from old exhausted_keys format
            providers = _migrate_from_exhausted_keys(data)

        return AuthStateSnapshot(
            mode=mode,
            reset_at=reset_at,
            active_provider_index=data.get("active_provider_index", 0),
            providers=providers,
            last_limit_event=data.get("last_limit_event"),
        )

    def save(self, snapshot: AuthStateSnapshot) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        providers = [p.to_dict() for p in snapshot.providers]
        for provider in providers:
            for capability in ("worker_validation", "reviewer_validation"):
                validation = provider[capability]
                error = validation["error"]
                if len(error) > MAX_PERSISTED_ERROR_CHARS:
                    validation["error"] = error[:MAX_PERSISTED_ERROR_CHARS] + "..."
        payload = {
            "mode": snapshot.mode.value,
            "reset_at": snapshot.reset_at.isoformat() if snapshot.reset_at else None,
            "active_provider_index": snapshot.active_provider_index,
            "providers": providers,
            "last_limit_event": snapshot.last_limit_event,
        }
        self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _migrate_from_exhausted_keys(data: dict[str, Any]) -> list[AuthProvider]:
    """Convert old exhausted_keys format to providers list."""
    providers = []
    # Old format didn't track API keys in state, just exhaustion
    exhausted: dict[str, Any] = data.get("exhausted_keys", {})
    old_active = data.get("active_key_index", 0)
    # We can't recover the actual key values, so create placeholder providers
    # The GlobalSettings.load will provide the real keys
    for i_str in sorted(exhausted.keys(), key=int):
        ts = exhausted.get(i_str, "")
        providers.append(
            AuthProvider(
                provider_type=AuthProviderType.API_KEY,
                status=KeyStatus.EXHAUSTED,
                exhausted_at=_parse_dt(ts),
            )
        )
    providers.append(
        AuthProvider(
            provider_type=AuthProviderType.API_KEY,
            status=KeyStatus.ACTIVE if old_active == len(exhausted) else KeyStatus.UNUSED,
        )
    )
    return providers
