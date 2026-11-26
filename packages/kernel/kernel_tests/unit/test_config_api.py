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

from unittest.mock import MagicMock

import pytest
import yaml
from pydantic import ValidationError

from workflow_controller.api.config_api import ConfigAPI
from workflow_controller.config.settings import AppConfig, WatchdogConfig


def _orch(cfg: AppConfig) -> MagicMock:
    orch = MagicMock()
    orch.config = cfg
    return orch


# ── load_from_dir:只更 project_dir 指针,不把项目配置灌进全局 ──────────────────


def test_load_from_dir_only_sets_project_dir_pointer(tmp_path):
    """选目录只在全局写 project_dir 指针,**不**把项目 runtime/system_prompt/loop
    灌进全局 config——项目配置唯一来源是项目 yaml,session 启动现读它,全局里的项目字段
    是冗余且会被 save 污染(修「Settings save 对项目设置无效」的根因)。
    """
    (tmp_path / "workflow-controller.yaml").write_text(
        "runtime: claude_cli\n"
        "system_prompt: 'AUQ gate'\n"
        "loop: {evaluator: passthrough, max_iterations: 5}\n",
        encoding="utf-8",
    )
    orch = _orch(AppConfig(project_dir=".", runtime="claude_sdk", system_prompt="GLOBAL"))
    api = ConfigAPI(orch, tmp_path / "saved.yaml")
    api.load_from_dir(str(tmp_path))
    assert orch.config.project_dir == str(tmp_path)  # 指针更新
    assert orch.config.runtime == "claude_sdk"  # 全局 runtime 不被项目覆盖
    assert orch.config.system_prompt == "GLOBAL"  # 全局 system_prompt 维持
    assert orch.config.loop.evaluator is None  # 全局 loop 不被项目覆盖


def test_load_from_dir_no_yaml_only_updates_project_dir(tmp_path):
    """目录下无 workflow-controller.yaml → 只更新 project_dir,维持其余全局 config。"""
    orch = _orch(AppConfig(project_dir=".", runtime="claude_sdk", system_prompt="GLOBAL"))
    api = ConfigAPI(orch, tmp_path / "saved.yaml")
    api.load_from_dir(str(tmp_path))  # tmp_path 无 yaml
    assert orch.config.runtime == "claude_sdk"  # 维持全局
    assert orch.config.system_prompt == "GLOBAL"  # 维持全局
    assert orch.config.project_dir == str(tmp_path)  # 只更新 project_dir


# ── get_project_config:读项目 yaml 标量字段(不含 stages) ─────────────────────


def test_get_project_config_returns_scalar_fields(tmp_path):
    """get_project_config 读项目 yaml,返回项目标量字段(runtime/starting_prompt/
    system_prompt/loop/reviewer.task_goal);**不含 stages**(stages 不进 UI,手改 yaml)。
    """
    (tmp_path / "workflow-controller.yaml").write_text(
        "runtime: opencode\n"
        "starting_prompt: 'do task'\n"
        "system_prompt: 'gate'\n"
        "loop: {evaluator: feedback, max_iterations: 8}\n"
        "reviewer: {task_goal: 'ship it'}\n"
        "stages:\n  - {name: spec, artifact: spec.md}\n",
        encoding="utf-8",
    )
    orch = _orch(AppConfig(project_dir=str(tmp_path)))
    api = ConfigAPI(orch, tmp_path / "saved.yaml")
    result = api.get_project_config(str(tmp_path))
    assert result["runtime"] == "opencode"
    assert result["starting_prompt"] == "do task"
    assert result["system_prompt"] == "gate"
    assert result["loop"] == {"evaluator": "feedback", "max_iterations": 8}
    assert result["reviewer"]["task_goal"] == "ship it"
    assert result["reviewer"]["enabled"] is None
    assert "stages" not in result  # stages 不进 UI


def test_get_project_config_defaults_when_no_yaml(tmp_path):
    """项目目录无 workflow-controller.yaml → 返回默认项目配置(不崩)。"""
    orch = _orch(AppConfig(project_dir="."))
    api = ConfigAPI(orch, tmp_path / "saved.yaml")
    result = api.get_project_config(str(tmp_path))  # tmp_path 无 yaml
    assert result["runtime"] == AppConfig().runtime  # 默认 claude_sdk
    assert result["starting_prompt"] == AppConfig().starting_prompt
    assert result["loop"] == {"evaluator": None, "max_iterations": None}
    assert result["reviewer"]["enabled"] is None


def test_get_project_config_preserves_explicit_false_and_partial_loop(tmp_path):
    (tmp_path / "workflow-controller.yaml").write_text(
        "reviewer: {enabled: false}\nloop: {max_iterations: 4}\n",
        encoding="utf-8",
    )
    api = ConfigAPI(_orch(AppConfig(project_dir=str(tmp_path))), tmp_path / "saved.yaml")
    result = api.get_project_config(str(tmp_path))
    assert result["reviewer"]["enabled"] is False
    assert result["loop"] == {"evaluator": None, "max_iterations": 4}


def test_reviewer_thinking_config_api_keeps_inheritance_and_other_keys(tmp_path):
    yaml_path = tmp_path / "workflow-controller.yaml"
    yaml_path.write_text("reviewer: {model: old}\nstages: [{name: a}]\n", encoding="utf-8")
    api = ConfigAPI(_orch(AppConfig(project_dir=str(tmp_path))), tmp_path / "saved.yaml")
    assert api.get_project_config(str(tmp_path))["reviewer"]["thinking_mode"] is None
    api.update_config({"reviewer": {"thinking_mode": "enabled", "thinking_budget_tokens": 4096}})
    assert api.get_config_dict()["reviewer"]["thinking_budget_tokens"] == 4096
    result = api.update_project_config(
        str(tmp_path),
        {
            "reviewer": {"thinking_mode": "disabled", "thinking_budget_tokens": 3072},
        },
    )
    assert result["reviewer"]["thinking_mode"] == "disabled"
    assert result["reviewer"]["thinking_budget_tokens"] == 3072
    saved = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    assert saved["reviewer"]["model"] == "old"
    assert saved["stages"] == [{"name": "a"}]


def test_reviewer_thinking_legacy_false_maps_to_disabled(tmp_path):
    api = ConfigAPI(_orch(AppConfig(project_dir=str(tmp_path))), tmp_path / "saved.yaml")

    api.update_config({"reviewer": {"thinking_enabled": False, "thinking_budget_tokens": 4096}})
    assert api.get_config_dict()["reviewer"]["thinking_mode"] == "disabled"

    result = api.update_project_config(
        str(tmp_path),
        {"reviewer": {"thinking_enabled": False, "thinking_budget_tokens": 3072}},
    )
    assert result["reviewer"]["thinking_mode"] == "disabled"


# ── update_project_config:写回项目 yaml,保键合并(不动 stages 等) ─────────────


def test_update_project_config_writes_back_preserving_stages(tmp_path):
    """update_project_config 写回项目 yaml:更新传入的项目标量键,**保留** stages 等
    未传键(保键合并——load dict → 更新已知键 → dump,不丢任何已有键)。
    """
    (tmp_path / "workflow-controller.yaml").write_text(
        "runtime: claude_sdk\n"
        "stages:\n  - {name: spec, artifact: spec.md}\n  - {name: plan, artifact: plan.md}\n"
        "reviewer: {task_goal: 'old goal'}\n",
        encoding="utf-8",
    )
    orch = _orch(AppConfig(project_dir=str(tmp_path)))
    api = ConfigAPI(orch, tmp_path / "saved.yaml")
    result = api.update_project_config(
        str(tmp_path),
        {
            "runtime": "opencode",
            "loop": {"max_iterations": 12, "evaluator": "feedback"},
            "reviewer": {"task_goal": "new goal"},
        },
    )
    data = yaml.safe_load((tmp_path / "workflow-controller.yaml").read_text(encoding="utf-8"))
    assert data["runtime"] == "opencode"  # 更新了
    assert data["loop"] == {"max_iterations": 12, "evaluator": "feedback"}  # 新增(合并)
    assert data["reviewer"]["task_goal"] == "new goal"  # 更新了
    assert data["stages"] == [  # stages 原样保留
        {"name": "spec", "artifact": "spec.md"},
        {"name": "plan", "artifact": "plan.md"},
    ]
    # 返回值 = 更新后的项目配置
    assert result["runtime"] == "opencode"
    assert result["reviewer"]["task_goal"] == "new goal"


def test_update_project_config_creates_yaml_when_missing(tmp_path):
    """项目目录无 workflow-controller.yaml → update 时新建一个(只写编辑过的键)。"""
    orch = _orch(AppConfig(project_dir=str(tmp_path)))
    api = ConfigAPI(orch, tmp_path / "saved.yaml")
    api.update_project_config(str(tmp_path), {"runtime": "opencode"})
    data = yaml.safe_load((tmp_path / "workflow-controller.yaml").read_text(encoding="utf-8"))
    assert data["runtime"] == "opencode"


# ── update_config(全局):只管 web 字段,补 project_dir(latent bug 修复) ────────


def test_update_config_ignores_project_fields_handles_project_dir(tmp_path):
    """update_config(全局)只管 web 字段:忽略项目字段(runtime/starting_prompt/...——
    那些归项目设置接口),但 project_dir 能被设置(修 latent bug——以前 PUT {project_dir}
    被 update_config 静默丢)。
    """
    orch = _orch(AppConfig(project_dir="/old", runtime="claude_sdk", starting_prompt="GLOBAL"))
    api = ConfigAPI(orch, tmp_path / "saved.yaml")
    api.update_config(
        {
            "project_dir": "/new/project",
            "runtime": "opencode",  # 项目字段 → 应被忽略
            "starting_prompt": "X",  # 项目字段 → 应被忽略
            "skills_path": "/skills",  # web 字段 → 应生效
        }
    )
    assert orch.config.project_dir == "/new/project"  # project_dir 现在能设
    assert orch.config.runtime == "claude_sdk"  # 项目字段被忽略
    assert orch.config.starting_prompt == "GLOBAL"  # 项目字段被忽略
    assert orch.config.skills_path == "/skills"  # web 字段生效


@pytest.mark.parametrize("enabled", [False, True])
def test_update_config_persists_hook_server_enabled(tmp_path, enabled):
    config_path = tmp_path / "workflow-controller-config.yaml"
    cfg = AppConfig()
    cfg.hook_server.enabled = not enabled
    api = ConfigAPI(_orch(cfg), config_path)

    updated = api.update_config({"hook_server": {"enabled": enabled}})

    assert updated["hook_server"]["enabled"] is enabled
    assert AppConfig.from_yaml(config_path).hook_server.enabled is enabled

    with pytest.raises(ValueError, match=r"hook_server\.port 不再支持配置"):
        api.update_config({"hook_server": {"port": 18766}})
    assert AppConfig.from_yaml(config_path).hook_server.enabled is enabled
    assert not hasattr(AppConfig.from_yaml(config_path).hook_server, "port")


# ── watchdog 配置：闭合模型，仅检测字段、仅全局 ─────────────────────────────


def test_update_config_watchdog_known_fields_only(tmp_path):
    """watchdog 只认模型声明的检测字段：未知键忽略（不再透传领域键），None 视为未配置跳过。"""
    cfg = AppConfig()
    api = ConfigAPI(_orch(cfg), tmp_path / "saved.yaml")
    api.update_config(
        {
            "watchdog": {
                "track_keyword": "oom",
                "subagent_timeout_seconds": None,
                "context_bay_repo_url": "https://example.com/r.git",
            }
        }
    )
    assert cfg.watchdog.track_keyword == "oom"
    assert cfg.watchdog.subagent_timeout_seconds == 1800  # None 跳过、保留原值
    assert not hasattr(cfg.watchdog, "context_bay_repo_url")  # 未知键不落入模型


def test_update_config_watchdog_bad_value_raises(tmp_path):
    """检测字段坏值由赋值校验直接抛错，不再静默跳过。"""
    api = ConfigAPI(_orch(AppConfig()), tmp_path / "saved.yaml")
    with pytest.raises(ValidationError):
        api.update_config({"watchdog": {"subagent_timeout_seconds": "abc"}})


def test_get_config_dict_watchdog_is_detection_fields_only(tmp_path):
    """配置视图只含检测字段，不含领域上传键。"""
    api = ConfigAPI(_orch(AppConfig()), tmp_path / "saved.yaml")
    wd = api.get_config_dict()["watchdog"]
    assert set(wd) == set(WatchdogConfig.model_fields)


# ── 模型外配置段存储端口（不透明透传，段含义归段所有者）──────────────────


def test_section_roundtrip_through_yaml(tmp_path):
    """段写入经 update_section 落盘，重读经 get_section 取回，round-trip 不丢。"""
    cfg_path = tmp_path / "saved.yaml"
    api = ConfigAPI(_orch(AppConfig()), cfg_path)
    api.update_section("somedomain", {"repo_url": "https://example.com/r.git"})
    assert api.get_section("somedomain") == {"repo_url": "https://example.com/r.git"}
    assert yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["somedomain"] == {
        "repo_url": "https://example.com/r.git",
    }
    assert ConfigAPI(_orch(AppConfig.from_yaml(cfg_path)), cfg_path).get_section("somedomain") == {
        "repo_url": "https://example.com/r.git"
    }


def test_update_section_refuses_kernel_owned_section(tmp_path):
    """内核模型持有的段不可经段端口写入，防止把模型字段覆盖成裸 dict。"""
    api = ConfigAPI(_orch(AppConfig()), tmp_path / "saved.yaml")
    with pytest.raises(ValueError, match="kernel-owned"):
        api.update_section("watchdog", {"enabled": False})
    assert api.get_section("watchdog") is None


# ── brand_name：用户可见品牌随 /config 读写 ─────────────────────────────────


def test_get_config_dict_returns_default_brand_name(tmp_path):
    """未配置时配置视图返回默认品牌。"""
    api = ConfigAPI(_orch(AppConfig(project_dir=".")), tmp_path / "saved.yaml")
    assert api.get_config_dict()["brand_name"] == "Workflow Controller"


def test_update_config_sets_and_persists_brand_name(tmp_path):
    """update_config 改品牌名并持久化到全局 yaml。"""
    orch = _orch(AppConfig(project_dir="."))
    path = tmp_path / "saved.yaml"
    api = ConfigAPI(orch, path)
    assert api.update_config({"brand_name": "Ascendbot"})["brand_name"] == "Ascendbot"
    assert orch.config.brand_name == "Ascendbot"
    assert yaml.safe_load(path.read_text(encoding="utf-8"))["brand_name"] == "Ascendbot"


def test_update_config_blank_brand_name_falls_back_to_default(tmp_path):
    """空/纯空白品牌名回落默认，避免界面出现无标题品牌。"""
    orch = _orch(AppConfig(project_dir=".", brand_name="Ascendbot"))
    api = ConfigAPI(orch, tmp_path / "saved.yaml")
    assert api.update_config({"brand_name": "   "})["brand_name"] == "Workflow Controller"
    assert api.update_config({"brand_name": ""})["brand_name"] == "Workflow Controller"


def test_get_config_dict_blank_brand_name_falls_back_to_default(tmp_path):
    """手改 yaml 置空品牌名绕过写入口，读侧在 API 边界回落默认。"""
    orch = _orch(AppConfig(project_dir=".", brand_name="  "))
    api = ConfigAPI(orch, tmp_path / "saved.yaml")
    assert api.get_config_dict()["brand_name"] == "Workflow Controller"


def test_brand_name_yaml_round_trip(tmp_path):
    """from_yaml / to_yaml 品牌名 round-trip——手改 yaml 的主路径。"""
    path = tmp_path / "workflow-controller-config.yaml"
    path.write_text("brand_name: Ascendbot\n", encoding="utf-8")
    cfg = AppConfig.from_yaml(path)
    assert cfg.brand_name == "Ascendbot"
    cfg.to_yaml(path)
    assert AppConfig.from_yaml(path).brand_name == "Ascendbot"
