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
from typing import TYPE_CHECKING

from workflow_controller.driver import codex_catalog

if TYPE_CHECKING:
    from pathlib import Path


def test_resolves_builtin_codex_model(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        codex_catalog,
        "_bundled_model_slugs",
        lambda: {"gpt-5.5"},
    )

    result = codex_catalog.resolve_codex_catalog(
        model="gpt-5.5",
        config_dir=tmp_path,
    )

    assert result.status == "builtin"
    assert result.model_catalog_path is None


def test_installs_matching_preset(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        codex_catalog,
        "_bundled_model_slugs",
        lambda: frozenset(),
    )

    result = codex_catalog.resolve_codex_catalog(
        model="glm-5.3",
        config_dir=tmp_path,
    )

    assert result.status == "preset"
    catalog = json.loads(result.model_catalog_path.read_text(encoding="utf-8"))
    assert catalog["models"][0]["slug"] == "glm-5.3"


def test_installs_glm_5_2_preset(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        codex_catalog,
        "_bundled_model_slugs",
        lambda: frozenset(),
    )

    result = codex_catalog.resolve_codex_catalog(
        model="glm-5.2",
        config_dir=tmp_path,
    )

    assert result.status == "preset"
    catalog = json.loads(result.model_catalog_path.read_text(encoding="utf-8"))
    models = {model["slug"]: model for model in catalog["models"]}
    assert set(models) == {"glm-5.3", "glm-5.2"}
    assert [level["effort"] for level in models["glm-5.3"]["supported_reasoning_levels"]] == [
        "low",
        "high",
        "max",
    ]
    assert models["glm-5.2"]["context_window"] == 1048576
    assert models["glm-5.2"]["default_reasoning_level"] == "max"
    assert [level["effort"] for level in models["glm-5.2"]["supported_reasoning_levels"]] == [
        "none",
        "minimal",
        "low",
        "medium",
        "high",
        "xhigh",
        "max",
    ]


def test_unknown_third_party_requires_custom_catalog(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        codex_catalog,
        "_bundled_model_slugs",
        lambda: frozenset(),
    )

    result = codex_catalog.resolve_codex_catalog(
        model="unknown-model",
        config_dir=tmp_path,
    )

    assert result.status == "custom_required"
    assert result.model_catalog_path is None


def test_installs_deepseek_preset(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        codex_catalog,
        "_bundled_model_slugs",
        lambda: frozenset(),
    )

    result = codex_catalog.resolve_codex_catalog(
        model="deepseek-v4-flash",
        config_dir=tmp_path,
    )

    assert result.status == "preset"
    catalog = json.loads(result.model_catalog_path.read_text(encoding="utf-8"))
    assert {model["slug"] for model in catalog["models"]} == {
        "deepseek-v4-flash",
        "deepseek-v4-pro",
    }


def test_installs_yibuapi_opus_preset(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        codex_catalog,
        "_bundled_model_slugs",
        lambda: frozenset(),
    )

    result = codex_catalog.resolve_codex_catalog(
        model="claude-opus-5-5",
        config_dir=tmp_path,
    )

    assert result.status == "preset"
    catalog = json.loads(result.model_catalog_path.read_text(encoding="utf-8"))
    model = catalog["models"][0]
    assert model["slug"] == "claude-opus-5-5"
    assert model["base_instructions"] == ""
    assert model["truncation_policy"] == {"mode": "bytes", "limit": 10000}
    assert model["context_window"] == 1048576
    assert model["input_modalities"] == ["text", "image"]
    assert model["supports_parallel_tool_calls"] is True
    assert [level["effort"] for level in model["supported_reasoning_levels"]] == [
        "low",
        "medium",
        "high",
        "max",
    ]
    assert model["support_verbosity"] is False
    assert model["experimental_supported_tools"] == []


def test_custom_path_overrides_preset(
    tmp_path: Path,
    monkeypatch,
) -> None:
    catalog = tmp_path / "models.json"
    catalog.write_text(
        '{"models":[{"slug":"glm-5.3"}]}',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        codex_catalog,
        "_bundled_model_slugs",
        lambda: frozenset(),
    )

    result = codex_catalog.resolve_codex_catalog(
        model="glm-5.3",
        config_dir=tmp_path,
        provided_path=str(catalog),
    )

    assert result.status == "custom"
    assert result.model_catalog_path == catalog.resolve()
    assert not (tmp_path / "codex-models").exists()


def test_invalid_custom_path_returns_custom_invalid(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        codex_catalog,
        "_bundled_model_slugs",
        lambda: frozenset(),
    )

    result = codex_catalog.resolve_codex_catalog(
        model="glm-5.3",
        config_dir=tmp_path,
        provided_path=str(tmp_path / "missing.json"),
    )

    assert result.status == "custom_invalid"
    assert result.model_catalog_path is None
