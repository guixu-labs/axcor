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

import pytest

from workflow_controller.hooks.hook_config import HookConfigGenerator

if TYPE_CHECKING:
    from pathlib import Path

# 与 AscendBot harness 同形的 command hook 条目
COMMAND_HOOK = {
    "hooks": [
        {
            "type": "command",
            "command": 'bash "$CLAUDE_PROJECT_DIR"/.claude/hooks/day0-wake-check.sh',
            "timeout": 10,
        }
    ]
}


class TestWriteSettings:
    @staticmethod
    def test_write_settings_creates_dot_claude_settings_json_in_project_dir(
        tmp_path: Path,
    ) -> None:
        gen = HookConfigGenerator(port=18999, host="127.0.0.1")
        gen.write_settings(tmp_path)

        expected = tmp_path / ".claude" / "settings.json"
        assert expected.exists()
        data = json.loads(expected.read_text())
        assert "hooks" in data
        assert "PreToolUse" in data["hooks"]

    @staticmethod
    def test_write_settings_preserves_non_hook_keys_and_unmanaged_events(
        tmp_path: Path,
    ) -> None:
        claude_dir = tmp_path / ".claude"
        claude_dir.mkdir()
        settings_file = claude_dir / "settings.json"
        settings_file.write_text(
            json.dumps(
                {
                    "permissions": {"allow": ["Bash(*)"]},
                    "hooks": {"StopFailure": [COMMAND_HOOK]},
                }
            )
        )

        gen = HookConfigGenerator()
        gen.write_settings(tmp_path)

        data = json.loads(settings_file.read_text())
        assert data["permissions"] == {"allow": ["Bash(*)"]}
        assert data["hooks"]["StopFailure"] == [COMMAND_HOOK]
        assert "PreToolUse" in data["hooks"]


class TestWriteSettingsToFile:
    @staticmethod
    def test_writes_to_exact_path_no_double_dot_claude(
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "any" / "place" / "settings.json"

        gen = HookConfigGenerator()
        gen.write_settings_to_file(target)

        assert target.exists()
        assert not (target.parent / ".claude").exists()
        data = json.loads(target.read_text())
        assert "hooks" in data
        assert "Stop" in data["hooks"]

    @staticmethod
    def test_creates_parent_dirs_if_missing(tmp_path: Path) -> None:
        target = tmp_path / "nested" / "dirs" / "settings.json"

        gen = HookConfigGenerator()
        gen.write_settings_to_file(target)

        assert target.exists()

    @staticmethod
    def test_preserves_existing_keys_at_target_path(
        tmp_path: Path,
    ) -> None:
        target = tmp_path / ".claude" / "settings.json"
        target.parent.mkdir()
        target.write_text(
            json.dumps(
                {
                    "permissions": {"allow": ["x"]},
                }
            )
        )

        gen = HookConfigGenerator()
        gen.write_settings_to_file(target)

        data = json.loads(target.read_text())
        assert data["permissions"] == {"allow": ["x"]}
        assert "PreToolUse" in data["hooks"]

    @staticmethod
    def test_keeps_existing_command_hooks_under_managed_events(tmp_path: Path) -> None:
        target = tmp_path / ".claude" / "settings.json"
        target.parent.mkdir()
        target.write_text(json.dumps({"hooks": {"Stop": [COMMAND_HOOK]}}))

        gen = HookConfigGenerator()
        gen.write_settings_to_file(target)

        hooks = json.loads(target.read_text())["hooks"]
        assert hooks["Stop"] == [COMMAND_HOOK, *gen.generate_settings()["hooks"]["Stop"]]

    @staticmethod
    def test_replaces_stale_controller_hooks(tmp_path: Path) -> None:
        target = tmp_path / ".claude" / "settings.json"
        HookConfigGenerator(port=18999).write_settings_to_file(target)

        gen = HookConfigGenerator(port=18765)
        gen.write_settings_to_file(target)

        assert json.loads(target.read_text()) == gen.generate_settings()

    @staticmethod
    def test_repeated_write_leaves_file_unchanged(tmp_path: Path) -> None:
        target = tmp_path / ".claude" / "settings.json"
        target.parent.mkdir()
        target.write_text(json.dumps({"hooks": {"Stop": [COMMAND_HOOK]}}))
        gen = HookConfigGenerator()
        gen.write_settings_to_file(target)
        first = target.read_text()

        gen.write_settings_to_file(target)

        assert target.read_text() == first

    @staticmethod
    @pytest.mark.parametrize(
        "malformed_hooks",
        [None, {"Stop": None}],
        ids=["hooks-not-object", "event-not-array"],
    )
    def test_replaces_malformed_hooks(tmp_path: Path, malformed_hooks: Any) -> None:
        target = tmp_path / ".claude" / "settings.json"
        target.parent.mkdir()
        target.write_text(json.dumps({"permissions": {"allow": ["x"]}, "hooks": malformed_hooks}))

        gen = HookConfigGenerator()
        gen.write_settings_to_file(target)

        data = json.loads(target.read_text())
        assert data["permissions"] == {"allow": ["x"]}
        assert data["hooks"] == gen.generate_settings()["hooks"]

    @staticmethod
    def test_emitted_hook_timeout_is_5_minutes(tmp_path: Path) -> None:
        gen = HookConfigGenerator()
        target = tmp_path / ".claude" / "settings.json"
        gen.write_settings_to_file(target)

        data = json.loads(target.read_text())
        for hook_event in ("PreToolUse", "Stop", "PostToolUse"):
            timeout = data["hooks"][hook_event][0]["hooks"][0]["timeout"]
            assert timeout >= 120, f"{hook_event} hook timeout {timeout} too short for LLM mode"
