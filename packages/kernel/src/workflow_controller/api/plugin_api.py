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

"""Plugin API — 插件管理面门面。

只返回纯数据与 router 透传对象，不做任何路由注册；注册动作由控制台完成。
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from workflow_controller.core.orchestrator import Orchestrator


def _flip_enabled_in_text(text: str, new_value: bool) -> str:
    """定点改顶层 ``enabled:`` 行的标量值，保留其余所有内容（注释、结构、顺序）。

    替代 ``yaml.dump`` 整体重写——后者会丢光 config.yaml 里的注释。匹配首个
    顶层 ``enabled:`` 行（缩进的嵌套键、注释行都不动）；找不到则在最前补一行。
    """
    scalar = "true" if new_value else "false"
    new_text, n = re.subn(
        r"^(enabled:)\s*\S+(.*)$",
        rf"\g<1> {scalar}\g<2>",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    if n == 0:
        return f"enabled: {scalar}\n{text}"
    return new_text


def _coerce_form_value(raw_value: Any, text: str) -> Any:
    """HTML 表单字符串按原 YAML 值类型还原（bool/数字），其余原样保留。"""
    if isinstance(raw_value, bool):
        return text.lower() in ("true", "1", "yes", "on")
    if isinstance(raw_value, (int, float)):
        try:
            return float(text) if isinstance(raw_value, float) or "." in text else int(text)
        except ValueError:
            return text
    if raw_value is None and text.lower() in ("true", "false"):
        return text.lower() == "true"
    return text


def _load_plugin_yaml_raw(cfg_path: Path | None) -> dict[str, Any]:
    """读插件 config.yaml 原始 dict；路径缺失/不可读/非 dict 时从空配置起步。"""
    raw: dict[str, Any] = {}
    if cfg_path is None or not cfg_path.exists():
        return raw
    try:
        loaded = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            raw = loaded
    except (OSError, yaml.YAMLError):
        # 现存配置不可读时从空配置起步
        pass
    return raw


def _sync_loaded_plugin_config(plugins: list[Any], name: str, raw: dict[str, Any]) -> None:
    """把写回后的配置同步进同名已装载插件（extra 更新 + 触发 apply_config）。"""
    for plugin in plugins:
        if plugin.config.name != name:
            continue
        plugin.config.extra = {
            key: value for key, value in raw.items() if key not in ("name", "version", "enabled")
        }
        apply_config = getattr(plugin, "apply_config", None)
        if callable(apply_config):
            apply_config()
        break


class PluginAPI:
    """收口插件清单、启停、安装与配置读写。"""

    def __init__(self, orchestrator: Orchestrator) -> None:
        self._orch = orchestrator

    def builtin_breather_instructions(self, kind: str) -> tuple[str, str] | None:
        for cfg, _, plugin_type in self._orch.discover_plugins():
            if cfg.name == "breather":
                return plugin_type.builtin_instruction_files().get(kind)
        return None

    def list_plugins(self) -> list[dict[str, Any]]:
        """发现面清单；已装载插件附装载标记与 run 面板贡献。"""
        try:
            discovered = self._orch.discover_plugins()
        except Exception:
            return []

        entries: list[dict[str, Any]] = []
        for cfg, cfg_path, _ in discovered:
            entries.append(
                {
                    "name": cfg.name,
                    "version": cfg.version,
                    "enabled": cfg.enabled,
                    "path": str(cfg_path.parent),
                    "extra": dict(cfg.extra or {}),
                }
            )
        for plugin in self._orch.plugins:
            for entry in entries:
                if entry["name"] == plugin.config.name:
                    # 过渡：manifest 不再承载内容，仅作控制台判定「插件已装载」的
                    # 标记键（控制台按键在场判定）；控制台换用其他判定后删除
                    entry["manifest"] = []
                    # run 驱动面板贡献声明（面板槽位路由用；仅活插件有条目）
                    entry["run_panels"] = [
                        {
                            "format": p.format,
                            "component": p.component,
                            "schema_version": p.schema_version,
                        }
                        for p in plugin.get_run_panels()
                    ]
                    break
        return entries

    def toggle(self, name: str) -> dict[str, Any] | None:
        """翻转 enabled、保留注释写盘并重载；未发现插件时返回 None。"""
        found = self._find_config(name)
        if found is None:
            return None
        cfg, cfg_path = found
        cfg.enabled = not cfg.enabled
        if cfg_path is not None:
            if cfg_path.is_file():
                text = _flip_enabled_in_text(cfg_path.read_text(encoding="utf-8"), cfg.enabled)
            else:
                # 配置文件尚不存在（入口点插件未写过覆盖）：落最小覆盖种子
                text = f"enabled: {'true' if cfg.enabled else 'false'}\n"
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(text, encoding="utf-8")
        self._orch.reload_plugin(name)
        return {"name": name, "enabled": cfg.enabled}

    def get_handoff_states(self) -> dict[str, dict[str, Any]]:
        """聚合各插件上报的交接状态（SSE handoff_update 数据源）。"""
        return self._orch.plugin_registry.get_handoff_states()

    def get_web_router(self, name: str) -> Any:
        """取已装载插件的 web router 对象；注册动作由控制台完成。"""
        for plugin in self._orch.plugins:
            if plugin.config.name == name:
                return plugin.get_web_router()
        return None

    def install(self, source_dir: str) -> dict[str, Any]:
        """把本地插件目录复制进插件目录（缺 config.yaml/plugin.py 拒绝）。"""
        src_path = Path(source_dir)
        if not (src_path / "config.yaml").exists():
            return {"error": "missing config.yaml"}
        if not (src_path / "plugin.py").exists():
            return {"error": "missing plugin.py"}

        dst = self._orch.plugins_dir / src_path.name
        if dst.exists():
            return {"error": f"already exists: {dst}"}

        shutil.copytree(src_path, dst, ignore_dangling_symlinks=True)
        return {"ok": True, "path": str(dst)}

    def read_config(self, name: str) -> dict[str, Any] | None:
        """读插件配置字段；未发现返回 None。"""
        for cfg, cfg_path, _ in self._orch.discover_plugins():
            if cfg.name == name:
                return {
                    "name": cfg.name,
                    "version": cfg.version,
                    "enabled": cfg.enabled,
                    "path": str(cfg_path.parent),
                    "fields": dict(cfg.extra or {}),
                }
        return None

    def write_config(
        self,
        name: str,
        fields: dict[str, Any],
        enabled: bool | None,
    ) -> dict[str, Any] | None:
        """写插件配置：表单字符串按原 YAML 类型还原，未触及字段保持原值。

        ``enabled`` 为 None 时不改动该项。返回写回后的状态；未发现返回 None。
        """
        for cfg, cfg_path, _ in self._orch.discover_plugins():
            if cfg.name != name:
                continue

            raw = _load_plugin_yaml_raw(cfg_path)
            # 类型提示取合并视图：入口点插件的覆盖文件只存增量，缺键的
            # 原类型要从发现面的有效配置（包内默认+用户覆盖）取
            type_hints = {**(cfg.extra or {}), **raw}

            if isinstance(fields, dict):
                for key, value in fields.items():
                    raw[key] = (
                        _coerce_form_value(type_hints.get(key), value)
                        if isinstance(value, str)
                        else value
                    )
            if enabled is not None:
                raw["enabled"] = enabled

            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(
                yaml.dump(raw, default_flow_style=False, sort_keys=False, allow_unicode=True),
                encoding="utf-8",
            )
            # 同步同样传合并视图：raw 只存覆盖增量，整体替换 extra 会丢包内默认字段
            _sync_loaded_plugin_config(self._orch.plugins, name, {**(cfg.extra or {}), **raw})
            # 覆盖文件只存增量：无 enabled 键时回落到发现面有效态
            return {"ok": True, "name": name, "enabled": raw.get("enabled", cfg.enabled)}
        return None

    def _find_config(self, name: str) -> tuple[Any, Path | None] | None:
        """按名找插件配置：先查已装载插件，再查发现面。"""
        for plugin in self._orch.plugins:
            if plugin.config.name == name:
                return plugin.config, plugin.config_path
        for cfg, cfg_path, _ in self._orch.discover_plugins():
            if cfg.name == name:
                return cfg, cfg_path
        return None
