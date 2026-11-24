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

"""translator 单元测试：IR 快照（golden file）+ work_dir 回退链 + 校验与变量渲染。"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml

from workflow_controller.plugins.graph import translator


SCENARIOS_DIR = Path(__file__).parent / "fixtures" / "graph-test"
EXAMPLE_YAML = SCENARIOS_DIR / "graph-workflow.yaml"
GOLDEN_FILE = Path(__file__).parent / "golden" / "graph_workflow_ir.json"


def _write_yaml(tmp_path: Path, config: dict) -> Path:
    p = tmp_path / "wf.yaml"
    p.write_text(yaml.safe_dump(config), encoding="utf-8")
    return p


def _write_single_node_yaml(tmp_path: Path, node: dict) -> Path:
    """单节点 workflow 落盘（on_exhaust 词表与归一化用例的公共形状）。"""
    return _write_yaml(tmp_path, {"workflow": "w", "nodes": [node]})


def _ir_projection(program: translator.GraphProgram) -> dict:
    """IR 的可比较投影：绝对路径转相对，剔除环境差异。"""
    agents_dir = None
    if program.agents_dir is not None:
        agents_dir = str(program.agents_dir.relative_to(EXAMPLE_YAML.parent))
    return {
        "workflow": program.workflow,
        "description": program.description,
        "variables": program.variables,
        "max_parallel": program.max_parallel,
        "rollback_budget": program.rollback_budget,
        "agents_dir": agents_dir,
        "nodes": program.dag.nodes,
        "edges": sorted(f"{src}->{dst}" for src, dst in program.dag.edges),
    }


class TestProgramIrSnapshot:
    @staticmethod
    def test_example_workflow_ir_matches_golden():
        program = translator.translate_program(EXAMPLE_YAML)
        actual = _ir_projection(program)
        if os.environ.get("UPDATE_GOLDEN"):
            GOLDEN_FILE.parent.mkdir(parents=True, exist_ok=True)
            GOLDEN_FILE.write_text(
                json.dumps(actual, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        golden = json.loads(GOLDEN_FILE.read_text(encoding="utf-8"))
        assert actual == golden

    @pytest.mark.parametrize(
        "filename, workflow, node_ids",
        [
            (
                "test-all-scenarios.yaml",
                "test-all-scenarios",
                {
                    "fail-node",
                    "exec-node",
                    "verify-node",
                },
            ),
            ("test-pause-resume.yaml", "test-pause-resume", {"A", "B", "C"}),
            (
                "test-pause-resume-agent.yaml",
                "test-pause-resume-agent",
                {"A", "B", "C"},
            ),
        ],
    )
    def test_pause_resume_examples_translate_on_run_driver(
        self,
        filename: str,
        workflow: str,
        node_ids: set[str],
    ):
        """源提交新增的场景 YAML 走当前 RunDriver 翻译器，而非旧 Graph API。"""
        program = translator.translate_program(
            SCENARIOS_DIR / filename,
        )
        assert program.workflow == workflow
        assert set(program.dag.all_ids()) == node_ids


class TestResolveWorkDir:
    @staticmethod
    def test_yaml_work_dir_rendered_with_merged_variables(tmp_path):
        cfg = {"work_dir": "{{base}}/out", "variables": {"base": "unused"}}
        wd = translator.resolve_work_dir(
            cfg,
            {"base": str(tmp_path)},
            None,
            tmp_path / "wf.yaml",
            None,
        )
        assert wd == (tmp_path / "out").resolve()

    @staticmethod
    def test_relative_yaml_work_dir_uses_external_base(tmp_path):
        wd = translator.resolve_work_dir(
            {"work_dir": "out"},
            None,
            str(tmp_path / "proj"),
            tmp_path / "y" / "wf.yaml",
            None,
        )
        assert wd == (tmp_path / "proj" / "out").resolve()

    @staticmethod
    def test_external_work_dir_fallback(tmp_path):
        wd = translator.resolve_work_dir(
            {},
            None,
            str(tmp_path / "ext"),
            tmp_path / "wf.yaml",
            None,
        )
        assert wd == (tmp_path / "ext").resolve()

    @staticmethod
    def test_project_dir_fallback(tmp_path):
        wd = translator.resolve_work_dir(
            {},
            None,
            None,
            tmp_path / "wf.yaml",
            str(tmp_path / "proj"),
        )
        assert wd == (tmp_path / "proj").resolve()


class TestTranslateProgram:
    @staticmethod
    def test_variables_merge_external_wins_and_nested_render(tmp_path):
        p = _write_yaml(
            tmp_path,
            {
                "workflow": "w",
                "variables": {"a": "{{b}}-x", "b": "base"},
                "nodes": [{"id": "n1", "task_type": "bash", "command": "true"}],
            },
        )
        program = translator.translate_program(p, {"b": "ext"})
        assert program.variables == {"a": "ext-x", "b": "ext"}

    @staticmethod
    def test_rollback_target_must_exist(tmp_path):
        p = _write_yaml(
            tmp_path,
            {
                "workflow": "w",
                "nodes": [
                    {
                        "id": "n1",
                        "task_type": "bash",
                        "command": "true",
                        "acceptance": {"on_exhaust": "rollback", "rollback_to": "ghost"},
                    }
                ],
            },
        )
        with pytest.raises(ValueError, match="does not exist"):
            translator.translate_program(p)

    @staticmethod
    def test_rollback_target_must_be_ancestor(tmp_path):
        p = _write_yaml(
            tmp_path,
            {
                "workflow": "w",
                "nodes": [
                    {"id": "a", "task_type": "bash", "command": "true"},
                    {
                        "id": "b",
                        "task_type": "bash",
                        "command": "true",
                        "depends_on": ["a"],
                        "acceptance": {"on_exhaust": "rollback", "rollback_to": "b"},
                    },
                ],
            },
        )
        with pytest.raises(ValueError, match="must be an ancestor"):
            translator.translate_program(p)

    @staticmethod
    def test_on_exhaust_out_of_vocab_rejected(tmp_path):
        p = _write_yaml(
            tmp_path,
            {
                "workflow": "w",
                "nodes": [
                    {
                        "id": "n1",
                        "task_type": "bash",
                        "command": "true",
                        "acceptance": {"on_exhaust": "nope"},
                    }
                ],
            },
        )
        with pytest.raises(ValueError, match="不在词表"):
            translator.translate_program(p)

    @staticmethod
    def test_on_exhaust_top_level_with_dict_acceptance_rejected(tmp_path):
        p = _write_single_node_yaml(
            tmp_path,
            {
                "id": "n1",
                "task_type": "bash",
                "command": "true",
                "on_exhaust": "halt",
                "acceptance": {"type": "script", "script": "true"},
            },
        )
        with pytest.raises(ValueError, match="不在词表"):
            translator.translate_program(p)

    @staticmethod
    def test_on_exhaust_top_level_with_dict_acceptance_normalizes_legacy(tmp_path):
        p = _write_single_node_yaml(
            tmp_path,
            {
                "id": "n1",
                "task_type": "bash",
                "command": "true",
                "on_exhaust": "fail",
                "acceptance": {"type": "script", "script": "true"},
            },
        )
        program = translator.translate_program(p)
        assert program.dag.nodes[0]["acceptance"]["on_exhaust"] == "stop"

    @staticmethod
    def test_on_exhaust_thin_node_rejected(tmp_path):
        p = _write_single_node_yaml(
            tmp_path,
            {"id": "n1", "task_type": "bash", "command": "true", "on_exhaust": "halt"},
        )
        with pytest.raises(ValueError, match="不在词表"):
            translator.translate_program(p)

    @staticmethod
    def test_on_exhaust_thin_node_legacy_accepted(tmp_path):
        p = _write_single_node_yaml(
            tmp_path,
            {"id": "n1", "task_type": "bash", "command": "true", "on_exhaust": "fail"},
        )
        translator.translate_program(p)

    @staticmethod
    def test_agents_dir_resolved_relative_to_yaml(tmp_path):
        (tmp_path / "agents").mkdir()
        p = _write_yaml(
            tmp_path,
            {
                "workflow": "w",
                "agents_dir": "./agents",
                "nodes": [{"id": "n1", "task_type": "bash", "command": "true"}],
            },
        )
        program = translator.translate_program(p)
        assert program.agents_dir == tmp_path / "agents"

    @staticmethod
    def test_agents_dir_missing_directory_yields_none(tmp_path):
        p = _write_yaml(
            tmp_path,
            {
                "workflow": "w",
                "agents_dir": "./nope",
                "nodes": [{"id": "n1", "task_type": "bash", "command": "true"}],
            },
        )
        assert translator.translate_program(p).agents_dir is None

    @pytest.mark.parametrize("legacy", ["exit", "fail"])
    def test_on_exhaust_legacy_values_normalize_to_stop(self, tmp_path, legacy):
        p = _write_yaml(
            tmp_path,
            {
                "workflow": "w",
                "nodes": [
                    {
                        "id": "n1",
                        "task_type": "bash",
                        "command": "true",
                        "acceptance": {"on_exhaust": legacy},
                    }
                ],
            },
        )
        program = translator.translate_program(p)
        assert program.dag.nodes[0]["acceptance"]["on_exhaust"] == "stop"


class TestLoadSubgraph:
    @staticmethod
    def test_prefixed_subgraph_nodes_and_rollback_target(tmp_path):
        sub = tmp_path / "sub.yaml"
        sub.write_text(
            yaml.safe_dump(
                {
                    "nodes": [
                        {"id": "s1", "task_type": "bash", "command": "true"},
                        {
                            "id": "s2",
                            "task_type": "bash",
                            "command": "true",
                            "depends_on": ["s1"],
                            "acceptance": {"on_exhaust": "rollback", "rollback_to": "s1"},
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )
        prefixed = translator.load_prefixed_subgraph(sub, "gen")
        assert [n["id"] for n in prefixed] == ["gen:s1", "gen:s2"]
        assert prefixed[1]["depends_on"] == ["gen:s1"]
        assert prefixed[1]["acceptance"]["rollback_to"] == "gen:s1"

    @staticmethod
    def test_subgraph_config_normalized_from_xlsx_format(tmp_path):
        sub = tmp_path / "sub.yaml"
        sub.write_text(
            yaml.safe_dump(
                {
                    "nodes": [
                        {
                            "id": "s1",
                            "task_type": "normal",
                            "executor": "w",
                            "acceptance": ["test -f out.txt"],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        config = translator.load_subgraph_config(sub)
        node = config["nodes"][0]
        assert node["task_type"] == "agent"
        assert node["acceptance"]["script"] == "test -f out.txt"
