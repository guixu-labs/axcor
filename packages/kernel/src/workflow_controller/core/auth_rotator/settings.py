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

"""GlobalSettings — user-managed auth configuration (settings.conf).

Stored at ~/.config/workflow-controller/settings.conf in YAML format.
Replaces the single-key .env approach with multi-provider support.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class GlobalSettings:
    """User-editable global auth configuration.

    Read from and written to ``settings.conf`` via YAML.

    Global defaults:
      - default_model: str (optional, fallback for keys without model)
      - default_url: str (optional, fallback for keys without base_url)

    Each entry in ``api_keys`` is a dict with:
      - key: str (required)
      - model: str (optional, falls back to default_model)
      - base_url: str (optional, falls back to default_url)
      - runtime: str (optional, e.g. "opencode" — prefix-stripping rule for probes)
      - custom_name: str (optional, globally unique display name; backfilled on load)
      - model_catalog_path: str (optional provider metadata; effective runtime decides usage)
      - provider_kind: str (optional, opencode 的 ai-sdk 包名；仅显式配置——探测值只存 auth_state.json)
    """

    api_keys: list[dict[str, str]] = field(default_factory=list)

    default_model: str = ""

    default_url: str = ""

    @classmethod
    def load(cls, path: Path) -> GlobalSettings:
        if not path.exists():
            return cls._migrate_from_env(path) or cls()
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            logger.warning("settings.conf unreadable, using defaults")
            return cls()
        if not isinstance(data, dict):
            return cls()
        # YAML `default_url:`（显式 null）时 data.get(k, "") 返回 None，
        # 违反 str 字段契约：_provider_fingerprint 的 "\0".join 会抛
        # TypeError 使启动失败。or "" 把 null 归一化为空串。
        return cls(
            api_keys=_normalize_keys(data.get("api_keys", [])),
            default_model=data.get("default_model") or "",
            default_url=data.get("default_url") or "",
        )

    @classmethod
    def _migrate_from_env(cls, config_path: Path) -> GlobalSettings | None:
        """One-shot migration: read legacy ANTHROPIC_API_KEY from .env."""
        env_path = config_path.parent / ".env"
        if not env_path.exists():
            return None
        key = _read_dotenv_key(env_path, "ANTHROPIC_API_KEY")
        base_url = _read_dotenv_key(env_path, "ANTHROPIC_BASE_URL")
        model = _read_dotenv_key(env_path, "ANTHROPIC_MODEL")
        if not key:
            return None
        logger.info("migrating legacy .env key to settings.conf")
        entry: dict[str, str] = {"key": key}
        if model:
            entry["model"] = model
        if base_url:
            entry["base_url"] = base_url
        return cls(
            api_keys=[entry],
            default_model=model or "",
            default_url=base_url or "",
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "api_keys": self.api_keys,
            "default_model": self.default_model,
            "default_url": self.default_url,
        }
        path.write_text(
            yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )


def _normalize_key_entry(item: dict[str, Any]) -> dict[str, str] | None:
    """dict 条目规整：key 必填，其余字段非空才保留；无 key 返回 None。"""
    if not item.get("key"):
        return None
    entry: dict[str, str] = {"key": str(item["key"]).strip()}
    if item.get("model"):
        entry["model"] = str(item["model"]).strip()
    if item.get("base_url"):
        entry["base_url"] = str(item["base_url"]).strip()
    if item.get("runtime"):
        entry["runtime"] = str(item["runtime"]).strip()
    if item.get("custom_name"):
        entry["custom_name"] = str(item["custom_name"]).strip()
    if item.get("model_catalog_path"):
        entry["model_catalog_path"] = str(item["model_catalog_path"]).strip()
    # provider_kind（opencode 协议包）必须随条目保留：conf 里的
    # kind 只来自显式配置，丢掉即抹去用户的纠偏意图
    if item.get("provider_kind"):
        entry["provider_kind"] = str(item["provider_kind"]).strip()
    return entry


def _normalize_key_line(item: str) -> dict[str, str] | None:
    """legacy 字符串条目（"sk-…" 或 "sk-… | model"）规整；空白/注释行返回 None。"""
    line = item.strip()
    if not line or line.startswith("#"):
        return None
    if "|" not in line:
        return {"key": line}
    parts = line.split("|", 1)
    entry = {"key": parts[0].strip()}
    if parts[1].strip():
        entry["model"] = parts[1].strip()
    return entry


def _normalize_keys(raw: list) -> list[dict[str, str]]:
    """Normalize api_keys entries to dict format.

    Accepts:
      - dict with 'key' field: {"key": "...", "model": "...", "base_url": "..."}
      - legacy string format: "sk-ant-..." or "sk-ant-... | model"
    """
    result: list[dict[str, str]] = []
    for item in raw:
        if isinstance(item, dict):
            entry = _normalize_key_entry(item)
            if entry is not None:
                result.append(entry)
        elif isinstance(item, str):
            line_entry = _normalize_key_line(item)
            if line_entry is not None:
                result.append(line_entry)
    return result


def _read_dotenv_key(path: Path, varname: str) -> str | None:
    """Read a single KEY=value from a .env file."""
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"{varname}="):
                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                return val or None
    except OSError:
        pass
    return None
