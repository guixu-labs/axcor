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
import yaml
from pydantic import ValidationError

from workflow_controller.config.settings import AppConfig

if TYPE_CHECKING:
    from pathlib import Path


class TestFromYamlEncoding:
    @staticmethod
    def test_utf8_chinese_content_loads_on_windows(tmp_path: Path) -> None:
        path = tmp_path / "config.yaml"
        path.write_text(
            "project_dir: D:\\test\n"
            "reviewer:\n"
            "  enabled: true\n"
            "  task_goal: |\n"
            "    跑完 a2h pipeline 5 步骤：spec / plan / execute / verify / retrospect\n"
            '    所有步骤完成前永远不要选"结束"选项\n',
            encoding="utf-8",
        )

        cfg = AppConfig.from_yaml(path)

        assert cfg.reviewer.enabled is True
        assert "a2h" in cfg.reviewer.task_goal
        assert "永远不要选" in cfg.reviewer.task_goal

    @staticmethod
    def test_loop_block_loads_from_yaml(tmp_path: Path) -> None:
        path = tmp_path / "config.yaml"
        path.write_text(
            "loop:\n  evaluator: feedback\n  max_iterations: 8\n",
            encoding="utf-8",
        )
        cfg = AppConfig.from_yaml(path)
        assert cfg.loop is not None
        assert cfg.loop.evaluator == "feedback"
        assert cfg.loop.max_iterations == 8

    @staticmethod
    def test_system_prompt_loads_from_yaml(tmp_path: Path) -> None:
        path = tmp_path / "config.yaml"
        path.write_text(
            "system_prompt: |\n  你是 worker，严格遵守阶段门禁纪律。\n",
            encoding="utf-8",
        )
        cfg = AppConfig.from_yaml(path)
        assert "阶段门禁纪律" in cfg.system_prompt

    @staticmethod
    def test_to_yaml_then_from_yaml_roundtrip_utf8(tmp_path: Path) -> None:
        path = tmp_path / "roundtrip.yaml"
        original = AppConfig(
            project_dir="D:\\test",
        )
        original.reviewer.task_goal = "完整跑完 5 步骤，包含中文"
        original.reviewer.model = "claude-haiku-4-5"

        original.to_yaml(path)
        loaded = AppConfig.from_yaml(path)

        assert loaded.reviewer.task_goal == "完整跑完 5 步骤，包含中文"
        assert loaded.reviewer.model == "claude-haiku-4-5"

    @staticmethod
    def test_to_yaml_writes_full_schema_even_at_defaults(
        tmp_path: Path,
    ) -> None:
        """Settings → Save used to call model_dump(exclude_defaults=True),
        silently stripping entire blocks (e.g. reviewer when all fields were
        at pydantic defaults). Now we write the full schema so users can see
        and edit every field directly in the yaml.
        """
        import yaml as yaml_mod

        path = tmp_path / "full.yaml"
        # All-defaults AppConfig: would previously produce nearly-empty yaml
        AppConfig().to_yaml(path)

        # Reload as raw dict and verify the reviewer block is present
        raw = yaml_mod.safe_load(path.read_text(encoding="utf-8"))
        assert "reviewer" in raw, "reviewer block missing — to_yaml is still stripping defaults"
        assert raw["reviewer"]["enabled"] is True
        assert "mode" not in raw["reviewer"], "reviewer.mode field is deleted from schema"
        assert raw["reviewer"]["model"]

        # hook_server, project_dir, etc. should also show up
        assert "hook_server" in raw
        assert "port" not in raw["hook_server"]

    @staticmethod
    def test_yaml_rejects_hook_server_port(tmp_path: Path) -> None:
        path = tmp_path / "legacy.yaml"
        path.write_text("hook_server:\n  port: 18765\n", encoding="utf-8")
        with pytest.raises(ValidationError, match="port"):
            AppConfig.from_yaml(path)


class TestUnknownSectionRoundTrip:
    """顶层未知配置段（如领域自有段）round-trip 不丢字段；段内模型不校验。"""

    @staticmethod
    def test_unknown_section_survives_yaml_roundtrip(tmp_path: Path) -> None:
        path = tmp_path / "workflow-controller.yaml"
        path.write_text(
            "context_bay:\n"
            "  repo_url: https://example.com/repo.git\n"
            "  snapshot_paths:\n    - operators/x/\n",
            encoding="utf-8",
        )
        cfg = AppConfig.from_yaml(path)
        assert cfg.model_extra["context_bay"]["repo_url"] == "https://example.com/repo.git"

        cfg.to_yaml(path)
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert raw["context_bay"]["repo_url"] == "https://example.com/repo.git"
        assert raw["context_bay"]["snapshot_paths"] == ["operators/x/"]


class TestLoopConfigPromptTemplate:
    """LoopConfig.prompt_template default was rewritten to be resume-aware:
    iter>1 without queued feedback renders a self-recovery prompt that has
    the worker inspect on-disk artifacts before continuing. Useful both for
    cross-iter handoff and for Stop-hook block_count safety-net restarts.
    """

    @staticmethod
    def test_iter_1_uses_initial_prompt() -> None:
        from jinja2 import Template

        from workflow_controller.config.settings import LoopConfig

        t = Template(LoopConfig().prompt_template)
        out = t.render(iteration=1, initial_prompt="kick off", feedback=None)
        assert out.strip() == "kick off"

    @staticmethod
    def test_iter_2_with_feedback_uses_feedback() -> None:
        from jinja2 import Template

        from workflow_controller.config.settings import LoopConfig

        t = Template(LoopConfig().prompt_template)
        out = t.render(
            iteration=2,
            initial_prompt="kick off",
            feedback="do plan",
        )
        assert out.strip() == "do plan"

    @staticmethod
    def test_iter_2_without_feedback_uses_resume_prompt() -> None:
        from jinja2 import Template

        from workflow_controller.config.settings import LoopConfig

        t = Template(LoopConfig().prompt_template)
        out = t.render(iteration=2, initial_prompt="kick off", feedback=None)
        # Neutral resume prompt: instructs the worker to continue unfinished
        # work and inspect existing artifacts/progress before acting.
        assert "继续" in out
        assert "产物" in out
