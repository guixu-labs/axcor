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

"""export_subset cannbot 档裁剪单测：冻结清单剔除与互锁校验。"""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[4] / "scripts" / "export_subset.py"
if not SCRIPT.is_file():
    # 子集布局不含 scripts/，缺席时跳过（与 sync_dual_repo 同口径）
    pytest.skip("export_subset.py 不在本布局", allow_module_level=True)

_SPEC = importlib.util.spec_from_file_location("export_subset_under_test", SCRIPT)
export_subset = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(export_subset)

# 冻结清单外的邻近保留面：unit/driver 共享单测、unit 顶层与 integration 用例、根测试基建
KEPT_PATHS = (
    "packages/kernel/kernel_tests/unit/driver/test_session.py",
    "packages/kernel/kernel_tests/unit/driver/test_gate.py",
    "packages/kernel/kernel_tests/unit/test_session_api.py",
    "packages/kernel/kernel_tests/integration/test_orchestrator.py",
    "packages/kernel/kernel_tests/workspace/test_repository.py",
    "tests/conftest.py",
    "tests/support/pytest_hermetic.py",
)


def _build_subset_tree(root: Path) -> None:
    """按冻结清单与保留面搭建最小子集树（目录条目带一个内部文件）。"""
    for relative in export_subset.CANNBOT_EXCLUDED_DIRS:
        path = root / relative / "test_sample.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")
    for relative in export_subset.CANNBOT_EXCLUDED_FILES:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")
    for relative in KEPT_PATHS:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")


def test_cannbot_trim_removes_frozen_paths_and_keeps_neighbors(tmp_path: Path) -> None:
    _build_subset_tree(tmp_path)

    export_subset.apply_cannbot_profile(tmp_path)

    trimmed = (*export_subset.CANNBOT_EXCLUDED_DIRS, *export_subset.CANNBOT_EXCLUDED_FILES)
    assert not any((tmp_path / relative).exists() for relative in trimmed)
    assert all((tmp_path / relative).is_file() for relative in KEPT_PATHS)


def test_cannbot_trim_fails_when_frozen_path_missing(tmp_path: Path) -> None:
    _build_subset_tree(tmp_path)
    (tmp_path / export_subset.CANNBOT_EXCLUDED_FILES[0]).unlink()

    with pytest.raises(export_subset.ExportError, match="缺席"):
        export_subset.apply_cannbot_profile(tmp_path)


def test_cannbot_naming_rejects_internal_repo_name(tmp_path: Path) -> None:
    _build_subset_tree(tmp_path)
    leak = tmp_path / "packages/kernel/kernel_tests/unit/test_leak.py"
    leak.write_text('ROUTE = "https://example.invalid/axcor"\n', encoding="utf-8")

    with pytest.raises(export_subset.ExportError, match="内部仓名"):
        export_subset.validate_cannbot_naming(tmp_path)


def test_cannbot_naming_passes_on_clean_tree(tmp_path: Path) -> None:
    _build_subset_tree(tmp_path)

    export_subset.validate_cannbot_naming(tmp_path)


@pytest.mark.parametrize(
    "leftover",
    [
        "packages/kernel/kernel_tests/unit/driver/test_codex_extra.py",
        "packages/kernel/kernel_tests/unit/test_provider_extra.py",
    ],
)
def test_cannbot_trim_fails_on_same_category_leftover(tmp_path: Path, leftover: str) -> None:
    _build_subset_tree(tmp_path)
    path = tmp_path / leftover
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x\n", encoding="utf-8")

    with pytest.raises(export_subset.ExportError, match="须并入冻结清单"):
        export_subset.apply_cannbot_profile(tmp_path)
