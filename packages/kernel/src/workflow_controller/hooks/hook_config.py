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
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from pathlib import Path


class HookConfigGenerator:
    def __init__(self, port: int = 18765, host: str = "localhost") -> None:
        self.base_url = f"http://{host}:{port}"

    def generate_settings(self) -> dict[str, Any]:
        hook_events = [
            "PostToolUse",
            "PostToolUseFailure",
            "PreToolUse",
            "Stop",
            "Notification",
            "SessionStart",
            "SessionEnd",
            "SubagentStart",
            "SubagentStop",
        ]
        hooks: dict[str, list[dict[str, Any]]] = {}
        for event in hook_events:
            path = (
                event.lower()
                .replace("sessionstart", "session-start")
                .replace("sessionend", "session-end")
                .replace("posttooluse", "post-tool-use")
                .replace("post-tool-usefailure", "post-tool-use-failure")
                .replace("pretooluse", "pre-tool-use")
                .replace("subagentstart", "subagent-start")
                .replace("subagentstop", "subagent-stop")
            )
            hooks[event] = [
                {
                    "matcher": "*",
                    "hooks": [
                        {
                            "type": "http",
                            "url": f"{self.base_url}/hooks/{path}",
                            "timeout": 300,
                        }
                    ],
                }
            ]
        return {"hooks": hooks}

    def write_settings(self, project_dir: Path) -> None:
        self.write_settings_to_file(project_dir / ".claude" / "settings.json")

    def write_settings_to_file(self, settings_file: Path) -> None:
        """按事件合并写入：只替换 controller 自己的旧条目，保留项目原有的 hook。"""
        settings = self.generate_settings()
        settings_file.parent.mkdir(parents=True, exist_ok=True)

        existing: dict[str, Any] = {}
        if settings_file.exists():
            existing = json.loads(settings_file.read_text())

        managed_paths = _managed_hook_paths(settings["hooks"])
        hooks = existing.get("hooks")
        if not isinstance(hooks, dict):
            hooks = {}
        for event, generated in settings["hooks"].items():
            old = hooks.get(event)
            if not isinstance(old, list):
                old = []
            hooks[event] = [g for g in old if not _is_managed(g, managed_paths)] + generated
        existing["hooks"] = hooks
        settings_file.write_text(json.dumps(existing, indent=2))


def _managed_hook_paths(hooks: dict[str, Any]) -> frozenset[str]:
    """取 controller 生成的 HTTP hook 端点路径，作为认领旧条目的依据。"""
    return frozenset(
        urlsplit(hook["url"]).path
        for groups in hooks.values()
        for group in groups
        for hook in group["hooks"]
    )


def _is_managed(group: Any, managed_paths: frozenset[str]) -> bool:
    """判断事件下的 matcher 条目是否全部由 controller 的 HTTP hook 组成。"""
    hooks = group.get("hooks") if isinstance(group, dict) else None
    if not isinstance(hooks, list) or not hooks:
        return False
    return all(
        isinstance(hook, dict)
        and hook.get("type") == "http"
        and urlsplit(str(hook.get("url", ""))).path in managed_paths
        for hook in hooks
    )
