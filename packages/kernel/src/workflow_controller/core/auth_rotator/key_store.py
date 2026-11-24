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

"""MultiKeyStore — parses key entries from settings.conf.

Each entry is a dict with:
  - key: str (required)
  - model: str (required)
  - base_url: str (required)
  - runtime: str (optional, e.g. "opencode" — prefix-stripping rule for probes)
  - model_catalog_path: str (optional provider metadata; effective runtime decides usage)
  - provider_kind: str (optional, opencode 的 ai-sdk 包名——仅显式配置；探测值只存 auth_state.json)

No filesystem I/O here — GlobalSettings handles that.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ParsedKey:
    """A single parsed API key entry."""

    key: str
    model: str | None = None
    base_url: str | None = None
    runtime: str | None = None
    custom_name: str | None = None
    model_catalog_path: str | None = None
    provider_kind: str | None = None


def parse_key_entry(entry: dict[str, str]) -> ParsedKey | None:
    """Parse a dict entry into a ParsedKey. Returns None if key is missing."""
    key = entry.get("key", "").strip()
    if not key:
        return None
    model = entry.get("model", "").strip() or None
    base_url = entry.get("base_url", "").strip() or None
    runtime = entry.get("runtime", "").strip() or None
    custom_name = entry.get("custom_name", "").strip() or None
    model_catalog_path = entry.get("model_catalog_path", "").strip() or None
    provider_kind = entry.get("provider_kind", "").strip() or None
    return ParsedKey(
        key=key,
        model=model,
        base_url=base_url,
        runtime=runtime,
        custom_name=custom_name,
        model_catalog_path=model_catalog_path,
        provider_kind=provider_kind,
    )


def parse_keys(raw_entries: list[dict[str, str]]) -> list[ParsedKey]:
    """Parse a list of dict entries into ParsedKey entries."""
    result: list[ParsedKey] = []
    for entry in raw_entries:
        parsed = parse_key_entry(entry)
        if parsed:
            result.append(parsed)
    return result
