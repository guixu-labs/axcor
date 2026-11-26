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

from typing import TYPE_CHECKING

import pytest

from workflow_controller.config.bootstrap import (
    DEFAULT_TASK_GOAL,
    ConfigFields,
    EnvFields,
    build_yaml_dict,
    copy_overlay,
    sync_worker_discovery_dirs,
    write_env,
    write_initial_config,
    write_yaml,
)
from workflow_controller.config.settings import AppConfig

if TYPE_CHECKING:
    from pathlib import Path


class TestBuildYamlDict:
    @staticmethod
    def test_default_fields_produces_complete_schema() -> None:
        data = build_yaml_dict(ConfigFields())

        assert data["project_dir"] == "."
        assert "reviewer" in data
        assert "hook_server" in data
        assert "mode" not in data["reviewer"], (
            "reviewer.mode is deleted from schema; the regex/auto_continue "
            "vs llm distinction no longer exists"
        )
        assert data["reviewer"]["enabled"] is True
        assert "port" not in data["hook_server"]

    @staticmethod
    def test_skills_path_omitted_when_blank() -> None:
        data = build_yaml_dict(ConfigFields(skills_path=""))
        assert "skills_path" not in data

    @staticmethod
    def test_skills_path_included_when_provided() -> None:
        data = build_yaml_dict(ConfigFields(skills_path="D:\\my\\skills"))
        assert data["skills_path"] == "D:\\my\\skills"


class TestWriteYaml:
    @staticmethod
    def test_default_yaml_roundtrips_through_app_config(tmp_path: Path) -> None:
        path = tmp_path / "workflow-controller.yaml"
        write_yaml(path, ConfigFields())

        cfg = AppConfig.from_yaml(path)
        assert cfg.reviewer.enabled is True
        assert not hasattr(cfg.reviewer, "mode")
        assert not hasattr(cfg.hook_server, "port")

    @staticmethod
    def test_chinese_task_goal_preserved(tmp_path: Path) -> None:
        path = tmp_path / "workflow-controller.yaml"
        write_yaml(path, ConfigFields())

        cfg = AppConfig.from_yaml(path)
        # Neutral generic task goal: domain-neutral wording + never-stop-early directive
        assert "代码与产物" in cfg.reviewer.task_goal
        assert "永远不要选" in cfg.reviewer.task_goal

    @staticmethod
    def test_custom_task_goal_chinese_roundtrips(tmp_path: Path) -> None:
        path = tmp_path / "workflow-controller.yaml"
        write_yaml(
            path,
            ConfigFields(reviewer_task_goal="完整跑完算子生成，包含中文标点。"),
        )
        cfg = AppConfig.from_yaml(path)
        assert cfg.reviewer.task_goal == "完整跑完算子生成，包含中文标点。"

    @staticmethod
    def test_windows_backslash_paths_survive_roundtrip(tmp_path: Path) -> None:
        path = tmp_path / "workflow-controller.yaml"
        write_yaml(
            path,
            ConfigFields(
                project_dir="D:\\Code\\my-project",
                skills_path="D:\\my\\skills",
            ),
        )
        cfg = AppConfig.from_yaml(path)
        assert cfg.project_dir == "D:\\Code\\my-project"
        assert cfg.skills_path == "D:\\my\\skills"

    @staticmethod
    def test_raises_when_file_exists(tmp_path: Path) -> None:
        path = tmp_path / "workflow-controller.yaml"
        path.write_text("existing", encoding="utf-8")

        with pytest.raises(FileExistsError):
            write_yaml(path, ConfigFields())

    @staticmethod
    def test_overwrite_flag_replaces_existing(tmp_path: Path) -> None:
        path = tmp_path / "workflow-controller.yaml"
        path.write_text("garbage content", encoding="utf-8")

        write_yaml(path, ConfigFields(), overwrite=True)

        cfg = AppConfig.from_yaml(path)
        assert cfg.reviewer.enabled is True

    @staticmethod
    def test_creates_parent_directory(tmp_path: Path) -> None:
        path = tmp_path / "nested" / "dir" / "workflow-controller.yaml"
        write_yaml(path, ConfigFields())
        assert path.exists()

    @staticmethod
    def test_written_file_is_utf8(tmp_path: Path) -> None:
        path = tmp_path / "workflow-controller.yaml"
        write_yaml(
            path,
            ConfigFields(reviewer_task_goal="中文 emoji ✓"),
        )
        # explicitly decode as utf-8 to verify encoding
        content = path.read_bytes().decode("utf-8")
        assert "中文" in content


class TestWriteEnv:
    @staticmethod
    def test_basic_write_creates_both_keys(tmp_path: Path) -> None:
        path = tmp_path / ".env"
        write_env(path, EnvFields(deepseek_api_key="sk-deepseek", anthropic_api_key="sk-claude"))

        content = path.read_text(encoding="utf-8")
        assert "DEEPSEEK_API_KEY=sk-deepseek" in content
        assert "ANTHROPIC_API_KEY=sk-claude" in content

    @staticmethod
    def test_blank_keys_still_emit_empty_placeholders(tmp_path: Path) -> None:
        path = tmp_path / ".env"
        write_env(path, EnvFields())
        content = path.read_text(encoding="utf-8")
        assert "DEEPSEEK_API_KEY=" in content
        assert "ANTHROPIC_API_KEY=" in content

    @staticmethod
    def test_raises_when_file_exists_without_flag(tmp_path: Path) -> None:
        path = tmp_path / ".env"
        path.write_text("EXISTING_KEY=foo\n", encoding="utf-8")

        with pytest.raises(FileExistsError):
            write_env(path, EnvFields())

    @staticmethod
    def test_overwrite_flag_replaces_existing(tmp_path: Path) -> None:
        path = tmp_path / ".env"
        path.write_text("EXISTING_KEY=foo\nDEEPSEEK_API_KEY=old\n", encoding="utf-8")

        write_env(
            path,
            EnvFields(deepseek_api_key="new"),
            overwrite=True,
        )

        content = path.read_text(encoding="utf-8")
        assert "DEEPSEEK_API_KEY=new" in content
        assert "EXISTING_KEY" not in content  # full replacement

    @staticmethod
    def test_merge_preserves_existing_custom_keys(tmp_path: Path) -> None:
        path = tmp_path / ".env"
        path.write_text(
            "MY_CUSTOM_KEY=xxx\nDEEPSEEK_API_KEY=old\n",
            encoding="utf-8",
        )

        write_env(
            path,
            EnvFields(deepseek_api_key="new"),
            merge=True,
        )

        content = path.read_text(encoding="utf-8")
        assert "MY_CUSTOM_KEY=xxx" in content
        assert "DEEPSEEK_API_KEY=new" in content

    @staticmethod
    def test_merge_does_not_override_with_blank(tmp_path: Path) -> None:
        """If user previously set a key and wizard form is blank, keep old."""
        path = tmp_path / ".env"
        path.write_text("DEEPSEEK_API_KEY=existing-key\n", encoding="utf-8")

        write_env(
            path,
            EnvFields(deepseek_api_key=""),  # blank
            merge=True,
        )

        content = path.read_text(encoding="utf-8")
        assert "DEEPSEEK_API_KEY=existing-key" in content

    @staticmethod
    def test_merge_skips_comment_lines(tmp_path: Path) -> None:
        path = tmp_path / ".env"
        path.write_text(
            "# This is a comment\nDEEPSEEK_API_KEY=old\n",
            encoding="utf-8",
        )

        write_env(
            path,
            EnvFields(deepseek_api_key="new"),
            merge=True,
        )
        content = path.read_text(encoding="utf-8")
        assert "DEEPSEEK_API_KEY=new" in content

    @staticmethod
    def test_creates_parent_directory(tmp_path: Path) -> None:
        path = tmp_path / "nested" / "dir" / ".env"
        write_env(path, EnvFields())
        assert path.exists()


class TestWriteInitialConfig:
    @staticmethod
    def test_yaml_is_loadable_as_app_config(tmp_path: Path) -> None:
        yaml_path, _ = write_initial_config(tmp_path)
        cfg = AppConfig.from_yaml(yaml_path)
        assert cfg.reviewer.enabled is True
        assert cfg.reviewer.model

    @staticmethod
    def test_custom_filenames(tmp_path: Path) -> None:
        yaml_path, env_path = write_initial_config(
            tmp_path,
            config_filename="my-config.yaml",
            env_filename="secrets.env",
        )
        assert yaml_path.name == "my-config.yaml"
        assert env_path.name == "secrets.env"

    @staticmethod
    def test_custom_fields_propagate(tmp_path: Path) -> None:
        yaml_path, _ = write_initial_config(
            tmp_path,
            config_fields=ConfigFields(
                skills_path="D:\\my\\skills",
                reviewer_model="claude-haiku-4-5",
            ),
        )
        cfg = AppConfig.from_yaml(yaml_path)
        assert cfg.skills_path == "D:\\my\\skills"
        assert cfg.reviewer.model == "claude-haiku-4-5"

    @staticmethod
    def test_creates_target_dir_if_missing(tmp_path: Path) -> None:
        target = tmp_path / "new-project"
        assert not target.exists()
        yaml_path, _ = write_initial_config(target)
        assert yaml_path.exists()


class TestCopyOverlay:
    @staticmethod
    def test_copies_into_empty_destination(tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        (src / "a.md").write_text("A", encoding="utf-8")
        (src / "nested").mkdir()
        (src / "nested" / "b.md").write_text("B", encoding="utf-8")

        dst = tmp_path / "dst"
        count = copy_overlay(src, dst)

        assert count == 2
        assert (dst / "a.md").read_text(encoding="utf-8") == "A"
        assert (dst / "nested" / "b.md").read_text(encoding="utf-8") == "B"

    @staticmethod
    def test_overwrites_same_named_files(tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        (src / "shared.md").write_text("from-src", encoding="utf-8")

        dst = tmp_path / "dst"
        dst.mkdir()
        (dst / "shared.md").write_text("stale-from-dst", encoding="utf-8")

        copy_overlay(src, dst)
        assert (dst / "shared.md").read_text(encoding="utf-8") == "from-src"

    @staticmethod
    def test_preserves_dst_only_files(tmp_path: Path) -> None:
        """Files in dst that don't exist in src must NOT be deleted."""
        src = tmp_path / "src"
        src.mkdir()
        (src / "from-src.md").write_text("X", encoding="utf-8")

        dst = tmp_path / "dst"
        dst.mkdir()
        (dst / "user-added.md").write_text("user-content", encoding="utf-8")
        (dst / "subdir").mkdir()
        (dst / "subdir" / "user-deep.md").write_text("deep", encoding="utf-8")

        copy_overlay(src, dst)

        # New file copied in
        assert (dst / "from-src.md").exists()
        # User files survive
        assert (dst / "user-added.md").read_text(encoding="utf-8") == "user-content"
        assert (dst / "subdir" / "user-deep.md").exists()

    @staticmethod
    def test_creates_destination_if_missing(tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        (src / "x.md").write_text("X", encoding="utf-8")

        dst = tmp_path / "does" / "not" / "exist" / "yet"
        assert not dst.exists()

        copy_overlay(src, dst)
        assert (dst / "x.md").exists()

    @staticmethod
    def test_raises_when_src_missing(tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            copy_overlay(tmp_path / "nope", tmp_path / "dst")

    @staticmethod
    def test_raises_when_src_is_file(tmp_path: Path) -> None:
        src = tmp_path / "src.txt"
        src.write_text("file not dir", encoding="utf-8")
        with pytest.raises(NotADirectoryError):
            copy_overlay(src, tmp_path / "dst")

    @staticmethod
    def test_empty_src_is_no_op(tmp_path: Path) -> None:
        src = tmp_path / "empty-src"
        src.mkdir()
        dst = tmp_path / "dst"

        count = copy_overlay(src, dst)
        assert count == 0
        assert dst.is_dir()


class TestSyncWorkerDiscoveryDirs:
    @staticmethod
    def test_sync_cc_lands_in_claude_dir(tmp_path: Path) -> None:
        skills = tmp_path / "skills-src"
        (skills / "my-skill").mkdir(parents=True)
        (skills / "my-skill" / "SKILL.md").write_text("name: my-skill", encoding="utf-8")
        agents = tmp_path / "agents-src"
        agents.mkdir()
        (agents / "bot.md").write_text("---\nname: bot\n---\nhi", encoding="utf-8")
        project = tmp_path / "proj"
        project.mkdir()

        synced = sync_worker_discovery_dirs(
            project_dir=project,
            runtime="claude_sdk",
            skills_path=str(skills),
            agents_path=str(agents),
        )

        assert (project / ".claude" / "skills" / "my-skill" / "SKILL.md").exists()
        assert (project / ".claude" / "agents" / "bot.md").exists()
        assert len(synced) == 2

    @staticmethod
    def test_sync_opencode_lands_in_opencode_dir(tmp_path: Path) -> None:
        skills = tmp_path / "skills-src"
        (skills / "s").mkdir(parents=True)
        (skills / "s" / "SKILL.md").write_text("x", encoding="utf-8")
        project = tmp_path / "proj"
        project.mkdir()

        sync_worker_discovery_dirs(
            project_dir=project,
            runtime="opencode",
            skills_path=str(skills),
        )

        assert (project / ".opencode" / "skills" / "s" / "SKILL.md").exists()
        # opencode runtime must NOT touch the CC discovery dir
        assert not (project / ".claude").exists()

    @staticmethod
    def test_sync_skips_empty_and_missing_source(tmp_path: Path) -> None:
        project = tmp_path / "proj"
        project.mkdir()

        # both paths empty → no-op, no dirs created
        synced = sync_worker_discovery_dirs(
            project_dir=project,
            runtime="claude_sdk",
            skills_path="",
            agents_path="",
        )
        assert synced == {}
        assert not (project / ".claude").exists()

        # missing source dir → skipped, no crash
        synced2 = sync_worker_discovery_dirs(
            project_dir=project,
            runtime="claude_sdk",
            skills_path=str(tmp_path / "does-not-exist"),
        )
        assert synced2 == {}

    @staticmethod
    def test_sync_overlay_preserves_local(tmp_path: Path) -> None:
        skills = tmp_path / "skills-src"
        skills.mkdir()
        (skills / "shared.md").write_text("from-src", encoding="utf-8")
        project = tmp_path / "proj"
        local_root = project / ".claude" / "skills"
        local_root.mkdir(parents=True)
        (local_root / "local-only.md").write_text("keep-me", encoding="utf-8")
        (local_root / "shared.md").write_text("stale", encoding="utf-8")

        sync_worker_discovery_dirs(
            project_dir=project,
            runtime="claude_sdk",
            skills_path=str(skills),
        )

        assert (local_root / "shared.md").read_text(encoding="utf-8") == "from-src"
        assert (local_root / "local-only.md").read_text(encoding="utf-8") == "keep-me"

    @staticmethod
    def test_sync_unknown_runtime_noop(tmp_path: Path) -> None:
        skills = tmp_path / "skills-src"
        skills.mkdir()
        (skills / "x.md").write_text("x", encoding="utf-8")
        project = tmp_path / "proj"
        project.mkdir()

        synced = sync_worker_discovery_dirs(
            project_dir=project,
            runtime="weird-runtime",
            skills_path=str(skills),
        )
        assert synced == {}
        assert not (project / ".claude").exists()
        assert not (project / ".opencode").exists()


class TestAgentsPath:
    @staticmethod
    def test_agents_path_omitted_when_blank() -> None:
        data = build_yaml_dict(ConfigFields(agents_path=""))
        assert "agents_path" not in data

    @staticmethod
    def test_agents_path_included_when_provided() -> None:
        data = build_yaml_dict(ConfigFields(agents_path="D:\\my\\agents"))
        assert data["agents_path"] == "D:\\my\\agents"


class TestDefaults:
    @staticmethod
    def test_default_task_goal_never_stops_early() -> None:
        # The default task goal must be domain-neutral (no operator wording —
        # operator framing is injected per-project/plugin, not shipped as a
        # framework default) AND instruct the reviewer to keep the worker going
        # until the task is actually complete.
        assert "代码与产物" in DEFAULT_TASK_GOAL
        assert "永远不要选" in DEFAULT_TASK_GOAL
        assert "算子" not in DEFAULT_TASK_GOAL
