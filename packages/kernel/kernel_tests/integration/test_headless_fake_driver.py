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

"""headless-only 验收：干净子进程内驱动 fake run 的完成、失败与崩溃恢复，全程不导入 workflow_controller.web。"""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.support import find_upward, subprocess_python_paths

_REPO_ROOT = find_upward(Path(__file__).resolve().parent, "tests/support")
ACCEPTANCE_TIMEOUT_S = 120.0


def run_acceptance(
    project_dir: Path,
    settings_dir: Path,
    scenario: str,
    expected_returncode: int = 0,
) -> dict:
    """子进程执行 headless 验收场景，断言通过并返回结果 JSON。

    expected_returncode 区分正常完成（0）与 crash 场景的刻意硬崩退出码。
    """
    env = dict(os.environ)
    python_path = subprocess_python_paths(Path(__file__).resolve().parent)
    if inherited := env.get("PYTHONPATH"):
        python_path.append(inherited)
    env["PYTHONPATH"] = os.pathsep.join(python_path)
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.support.headless_acceptance",
            str(project_dir),
            str(settings_dir),
            scenario,
        ],
        capture_output=True,
        text=True,
        timeout=ACCEPTANCE_TIMEOUT_S,
        env=env,
    )
    assert completed.returncode == expected_returncode, completed.stderr
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_fake_run_completes_headless_without_web(tmp_path: Path) -> None:
    """完成路径：干净子进程经 boot() 完成 fake run，且 meta-path 守卫未触发。"""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    result = run_acceptance(project_dir, tmp_path / "settings", "complete")
    assert result["status"] == "completed"
    assert "fake_progress" in result["event_types"]


def test_fake_run_fails_headless_without_web(tmp_path: Path) -> None:
    """失败路径：驱动判定失败并记录错误，run 到 failed 终态。"""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    result = run_acceptance(project_dir, tmp_path / "settings", "fail")
    assert result["status"] == "failed"
    assert result["error"]
    assert "fake_progress" in result["event_types"]


def test_fake_run_resumes_after_hard_crash_headless(tmp_path: Path) -> None:
    """恢复路径：子进程硬崩后，第二个进程 boot() 恢复链续跑至 completed。"""
    from tests.support.headless_acceptance import CRASH_EXIT_CODE

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    settings_dir = tmp_path / "settings"
    crash = run_acceptance(project_dir, settings_dir, "crash", expected_returncode=CRASH_EXIT_CODE)
    assert crash["status"] == "running"
    recover = run_acceptance(project_dir, settings_dir, "recover")
    assert recover["status"] == "completed"
    assert "fake_progress" in recover["event_types"]
    assert "fake_resumed" in recover["event_types"]


def test_http_layer_import_blocker_blocks_only_http_entry_modules() -> None:
    """负向验收：守卫自身必须拦下控制台 web 与内核 HTTP 表现层的导入，其余模块名放行。"""
    from tests.support.headless_acceptance import HttpLayerImportBlocker

    # 已缓存的模块会绕过 meta_path，先摘除再恢复，保证断言命中守卫本身
    guarded = ("workflow_controller.web", "workflow_controller.api.http")
    cached = {
        name: module
        for name, module in sys.modules.items()
        if any(name == pkg or name.startswith(f"{pkg}.") for pkg in guarded)
    }
    for name in cached:
        del sys.modules[name]
    blocker = HttpLayerImportBlocker()
    sys.meta_path.insert(0, blocker)
    try:
        for module_name in (
            "workflow_controller.web",
            "workflow_controller.web.routes",
            "workflow_controller.api.http",
            "workflow_controller.api.http.routes",
        ):
            with pytest.raises(ImportError, match="禁止导入"):
                importlib.import_module(module_name)
    finally:
        sys.meta_path.remove(blocker)
        sys.modules.update(cached)
    assert blocker.find_spec("workflow_controller.core") is None
    assert blocker.find_spec("workflow_controller.hooks") is None
    assert blocker.find_spec("os") is None
