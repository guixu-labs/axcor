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

"""PluginAPI 的清单聚合、启停写盘、router 透传与配置读写。"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from workflow_controller.api.plugin_api import PluginAPI
from workflow_controller.plugin.base import PluginConfig, RunPanelDef


class _Rediscovery:
    """每次发现都按 config.yaml 现值重建 PluginConfig（模拟 registry 行为）。"""

    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path

    def __call__(self) -> list[tuple[PluginConfig, Path, None]]:
        data = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        extra = {k: v for k, v in data.items() if k not in ("name", "version", "enabled")}
        return [
            (
                PluginConfig(
                    name=data.get("name", self.config_path.parent.name),
                    version=str(data.get("version", "0.0.0")),
                    enabled=bool(data.get("enabled", False)),
                    extra=extra or None,
                ),
                self.config_path,
                None,
            )
        ]


def _write_plugin_config(plugins_dir: Path, text: str) -> Path:
    plugin_dir = plugins_dir / "demo"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    config_path = plugin_dir / "config.yaml"
    config_path.write_text(text, encoding="utf-8")
    return config_path


def _orch(
    config_path: Path | None = None,
    plugins: list[Any] | None = None,
    plugins_dir: Path | None = None,
) -> SimpleNamespace:
    reload_calls: list[str] = []
    orch = SimpleNamespace(
        discover_plugins=_Rediscovery(config_path) if config_path else lambda: [],
        plugins=plugins or [],
        reload_plugin=reload_calls.append,
        plugins_dir=plugins_dir or Path("/unused"),
    )
    orch.reload_calls = reload_calls
    return orch


def test_toggle_flips_enabled_preserving_comments_and_reloads(tmp_path: Path) -> None:
    config_path = _write_plugin_config(tmp_path, "enabled: true\n# 说明注释\nname: demo\n")
    orch = _orch(config_path, plugins_dir=tmp_path)

    result = PluginAPI(orch).toggle("demo")

    assert result == {"name": "demo", "enabled": False}
    assert orch.reload_calls == ["demo"]
    text = config_path.read_text(encoding="utf-8")
    assert "enabled: false" in text
    assert "# 说明注释" in text


def test_toggle_unknown_plugin_returns_none(tmp_path: Path) -> None:
    config_path = _write_plugin_config(tmp_path, "name: demo\nenabled: true\n")
    assert PluginAPI(_orch(config_path, plugins_dir=tmp_path)).toggle("other") is None


def test_toggle_seeds_missing_config_file(tmp_path: Path) -> None:
    """配置文件尚不存在（入口点插件未写过覆盖）时，toggle 落最小覆盖种子。"""
    cfg_path = tmp_path / "user-config" / "demo" / "config.yaml"
    orch = SimpleNamespace(
        discover_plugins=lambda: [
            (PluginConfig(name="demo", version="0.1.0", enabled=True), cfg_path, None)
        ],
        plugins=[],
        reload_plugin=lambda name: None,
        plugins_dir=tmp_path,
    )

    result = PluginAPI(orch).toggle("demo")

    assert result == {"name": "demo", "enabled": False}
    assert yaml.safe_load(cfg_path.read_text(encoding="utf-8")) == {"enabled": False}


def test_write_config_coerces_form_strings_to_original_types(tmp_path: Path) -> None:
    config_path = _write_plugin_config(
        tmp_path,
        "name: demo\nenabled: true\nretries: 3\nratio: 0.5\nflag: yes\nlabel: demo\n",
    )

    result = PluginAPI(_orch(config_path, plugins_dir=tmp_path)).write_config(
        "demo",
        {"retries": "5", "ratio": "0.25", "flag": "false", "label": "new", "added": "x"},
        enabled=False,
    )

    assert result == {"ok": True, "name": "demo", "enabled": False}
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert data["retries"] == 5
    assert data["ratio"] == 0.25
    assert data["flag"] is False
    assert data["label"] == "new"
    assert data["added"] == "x"
    assert data["name"] == "demo"


def test_write_config_coerces_with_effective_type_hints_for_entry_point_plugin(
    tmp_path: Path,
) -> None:
    """入口点插件的覆盖文件只存增量：缺键的类型从发现面有效配置取。"""
    cfg_path = tmp_path / "user-config" / "demo" / "config.yaml"
    cfg_path.parent.mkdir(parents=True)
    cfg_path.write_text("enabled: true\n", encoding="utf-8")
    discovered = [
        (
            PluginConfig(name="demo", version="0.1.0", enabled=True, extra={"retries": 3}),
            cfg_path,
            None,
        )
    ]
    orch = SimpleNamespace(
        discover_plugins=lambda: discovered,
        plugins=[],
        reload_plugin=lambda name: None,
        plugins_dir=tmp_path,
    )

    result = PluginAPI(orch).write_config("demo", {"retries": "5"}, None)

    assert result == {"ok": True, "name": "demo", "enabled": True}
    assert yaml.safe_load(cfg_path.read_text(encoding="utf-8")) == {
        "enabled": True,
        "retries": 5,
    }


def test_write_config_without_enabled_key_returns_effective_state(tmp_path: Path) -> None:
    """覆盖文件尚无 enabled 键时，返回值取发现面有效态而非误报停用。"""
    cfg_path = tmp_path / "user-config" / "demo" / "config.yaml"
    discovered = [
        (
            PluginConfig(name="demo", version="0.1.0", enabled=True, extra={"retries": 3}),
            cfg_path,
            None,
        )
    ]
    orch = SimpleNamespace(
        discover_plugins=lambda: discovered,
        plugins=[],
        reload_plugin=lambda name: None,
        plugins_dir=tmp_path,
    )

    result = PluginAPI(orch).write_config("demo", {"retries": "5"}, None)

    assert result == {"ok": True, "name": "demo", "enabled": True}
    assert yaml.safe_load(cfg_path.read_text(encoding="utf-8")) == {"retries": 5}


def test_write_config_syncs_merged_view_to_loaded_plugin(tmp_path: Path) -> None:
    """覆盖文件只存增量：同步已装载插件时用合并视图，包内默认字段不丢。"""
    cfg_path = tmp_path / "user-config" / "demo" / "config.yaml"
    discovered = [
        (
            PluginConfig(
                name="demo", version="0.1.0", enabled=True, extra={"threshold": 7, "timeout": 30}
            ),
            cfg_path,
            None,
        )
    ]
    applied: list[dict[str, Any]] = []

    class _Plugin:
        def __init__(self) -> None:
            self.config = SimpleNamespace(name="demo", extra={"threshold": 7, "timeout": 30})

        def apply_config(self) -> None:
            applied.append(dict(self.config.extra))

    plugin = _Plugin()
    orch = SimpleNamespace(
        discover_plugins=lambda: discovered,
        plugins=[plugin],
        reload_plugin=lambda name: None,
        plugins_dir=tmp_path,
    )

    result = PluginAPI(orch).write_config("demo", {"timeout": 3}, None)

    assert result == {"ok": True, "name": "demo", "enabled": True}
    assert yaml.safe_load(cfg_path.read_text(encoding="utf-8")) == {"timeout": 3}
    assert plugin.config.extra == {"threshold": 7, "timeout": 3}
    assert applied == [{"threshold": 7, "timeout": 3}]


def test_read_config_returns_fields_and_none_for_unknown(tmp_path: Path) -> None:
    config_path = _write_plugin_config(
        tmp_path, "name: demo\nversion: 1.2.0\nenabled: true\nkey: value\n"
    )
    orch = _orch(config_path, plugins_dir=tmp_path)

    assert PluginAPI(orch).read_config("demo") == {
        "name": "demo",
        "version": "1.2.0",
        "enabled": True,
        "path": str(config_path.parent),
        "fields": {"key": "value"},
    }
    assert PluginAPI(orch).read_config("other") is None


def test_get_web_router_passthrough_for_loaded_plugin() -> None:
    sentinel = object()
    plugin = SimpleNamespace(
        config=SimpleNamespace(name="demo"),
        get_web_router=lambda: sentinel,
    )
    api = PluginAPI(_orch(plugins=[plugin]))
    assert api.get_web_router("demo") is sentinel
    assert api.get_web_router("other") is None


def test_list_plugins_attaches_run_panels_for_loaded_plugin(tmp_path: Path) -> None:
    class _PanelPlugin:
        """贡献 run 面板声明的最小桩插件。"""

        config = SimpleNamespace(name="demo")

        def get_web_router(self):
            return None

        @staticmethod
        def get_run_panels() -> list[RunPanelDef]:
            return [
                RunPanelDef(format="graph-yaml@1", component="graph.RunDagPanel", schema_version=2)
            ]

    config_path = _write_plugin_config(tmp_path, "name: demo\nversion: 0.1.0\nenabled: true\n")
    plugin = _PanelPlugin()

    entries = PluginAPI(_orch(config_path, plugins=[plugin], plugins_dir=tmp_path)).list_plugins()
    entry = next(e for e in entries if e["name"] == "demo")
    # manifest 键是控制台判定「已装载」的标记：已装载插件恒带空列表
    assert entry["manifest"] == []
    assert entry["run_panels"] == [
        {
            "format": "graph-yaml@1",
            "component": "graph.RunDagPanel",
            "schema_version": 2,
        }
    ]


def test_install_copies_dir_and_rejects_incomplete_source(tmp_path: Path) -> None:
    source = tmp_path / "incoming" / "demo"
    source.mkdir(parents=True)
    (source / "config.yaml").write_text("name: demo\n", encoding="utf-8")
    orch = _orch(plugins_dir=tmp_path / "plugins")

    assert PluginAPI(orch).install(str(source)) == {"error": "missing plugin.py"}

    (source / "plugin.py").write_text("", encoding="utf-8")
    result = PluginAPI(orch).install(str(source))
    assert result["ok"] is True
    assert (tmp_path / "plugins" / "demo" / "plugin.py").exists()
