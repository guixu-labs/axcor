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

"""spike 脚本冒烟：最小规模合成目录完整跑通五项测量。"""

import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "workspace_scope_spike.py"

# 子集导出布局不含 scripts/，脚本缺席时按缺席面跳过
pytestmark = pytest.mark.skipif(
    not SCRIPT.exists(), reason="本布局无 scripts/workspace_scope_spike.py"
)


def test_minimal_synthetic_spike_completes(tmp_path: Path) -> None:
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--work-root",
            str(tmp_path / "spike"),
            "--build-total-mb",
            "4",
            "--build-files",
            "12",
            "--source-files",
            "16",
            "--source-file-kb",
            "2",
            "--inner-git-mb",
            "1",
            "--checkpoint-rounds",
            "1",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    for section in (
        "一、初始基线导入",
        "二、单节点 worktree 检出",
        "三、构建产物继承（首层建区）",
        "四、检查点提交（构建产物纳入 checkpointed）",
        "五、下游继承构建产物",
        "六、回滚恢复",
    ):
        assert section in proc.stdout
    assert not (tmp_path / "spike").exists(), "默认应清理测量现场"
