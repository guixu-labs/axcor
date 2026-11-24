#!/usr/bin/env python
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

"""「内核 + 指定插件」自治子集导出工具。

把根 workspace 配置（裁剪 members / dev 依赖 / uv.sources）、kernel 包、
选中插件包与根测试基建拷贝到目标目录，重生成 lock 并写自说明 README，
导出结果可直接 ``uv sync && uv run pytest``。fake-driver 是内核声明的
测试依赖（headless 验收驱动），对所有子集强制随附；hookserver 与 npm
子包不入子集。可选 ``--profile cannbot``：按冻结裁剪清单剔除 axcor 开发
门禁与深度回归测试、不带开发工具脚本，产物即 cannbot 导入源。
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KERNEL_MEMBER = "packages/kernel"
FAKE_DRIVER = "fake-driver"
# 可选入子集的交付插件（fake-driver 之外）
DELIVERABLE_PLUGINS = (
    "agent-traceback",
    "breather",
    "file-provenance",
    "graph",
    "staged-pipeline",
)
DIST_PREFIX = "workflow-controller-plugin-"
CANNBOT_PROFILE = "cannbot"
# 拷贝时排除的本地构建产物与缓存
COPY_IGNORE = shutil.ignore_patterns(
    "__pycache__", ".pytest_cache", ".ruff_cache", "build", "dist", "*.egg-info"
)
# 随子集带走的根级文件（.gitignore 防快照线误收构建产物）；插件测试的场景
# 输入内聚在各自包内（tests/fixtures），examples 不再随子集
ROOT_FILES = (
    Path("LICENSE"),
    Path(".gitignore"),
    Path("conftest.py"),
)
# default 档额外随附的开发工具（kernel_tests/tooling 黑盒测试依赖词检工具与
# 词表）；cannbot 档不带 scripts/
TOOLING_FILES = (
    Path("scripts/check_ai_words.py"),
    Path("scripts/ai-words.txt"),
)
# cannbot 档剔除的测试目录（随 cannbot 初始 PR 冻结，只减不增；调整前先改
# docs/local/cannbot-minimal-initial-pr-analysis.md §4.2 再同步此处）
CANNBOT_EXCLUDED_DIRS = (
    "tests/architecture",  # axcor 开发期架构门禁，cannbot 有自家检查体系
    "packages/kernel/kernel_tests/tooling",  # 开发工具（词检/双仓同步）测试
)
# cannbot 档剔除的测试文件：逐 runtime 深度单测（守护由 axcor 全量门禁承担，
# 共享面单测与 integration e2e 保留）+ provider 验证矩阵 + spike 脚本冒烟
CANNBOT_EXCLUDED_FILES = (
    "packages/kernel/kernel_tests/unit/driver/test_claude_cli.py",
    "packages/kernel/kernel_tests/unit/driver/test_claude_cli_cleanup.py",
    "packages/kernel/kernel_tests/unit/driver/test_claude_lineage.py",
    "packages/kernel/kernel_tests/unit/driver/test_claude_sdk.py",
    "packages/kernel/kernel_tests/unit/driver/test_codex_catalog.py",
    "packages/kernel/kernel_tests/unit/driver/test_codex_cli.py",
    "packages/kernel/kernel_tests/unit/driver/test_codex_hooks.py",
    "packages/kernel/kernel_tests/unit/driver/test_codex_lineage.py",
    "packages/kernel/kernel_tests/unit/driver/test_codex_store.py",
    "packages/kernel/kernel_tests/unit/driver/test_opencode.py",
    "packages/kernel/kernel_tests/unit/driver/test_opencode_provider_injection.py",
    "packages/kernel/kernel_tests/unit/driver/test_opencode_session.py",
    "packages/kernel/kernel_tests/unit/test_provider_delete.py",
    "packages/kernel/kernel_tests/unit/test_provider_validation.py",
    "tests/test_workspace_scope_spike.py",
)
# 裁剪后按类别复扫的残留判定：unit/driver 不得有逐 runtime 深度单测、unit
# 顶层不得有 provider 矩阵用例（防根树新增同类文件绕过冻结清单静默漏裁）
DEEP_TEST_PREFIXES = ("test_claude", "test_codex", "test_opencode")


class ExportError(Exception):
    """导出参数或内容不符合预期。"""


def resolve_members(selected: list[str]) -> list[str]:
    """导出成员路径：kernel 在前，fake-driver 强制随附，插件按名序。"""
    members = [KERNEL_MEMBER, f"packages/plugins/{FAKE_DRIVER}"]
    members += [f"packages/plugins/{name}" for name in selected]
    return members


def copy_subset(target: Path, members: list[str], profile: str | None) -> None:
    """拷贝成员包、根测试树与根级文件到目标目录（cannbot 档不带开发工具）。"""
    for member in members:
        source = ROOT / member
        if not source.is_dir():
            raise ExportError(f"成员目录不存在：{member}")
        shutil.copytree(source, target / member, ignore=COPY_IGNORE)
    shutil.copytree(ROOT / "tests", target / "tests", ignore=COPY_IGNORE)
    root_files = ROOT_FILES if profile == CANNBOT_PROFILE else (*ROOT_FILES, *TOOLING_FILES)
    for relative in root_files:
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, destination)


def apply_cannbot_profile(target: Path) -> None:
    """按冻结清单剔除 cannbot 档不含的测试内容，并做互锁校验。"""
    for relative in (*CANNBOT_EXCLUDED_DIRS, *CANNBOT_EXCLUDED_FILES):
        path = target / relative
        if not path.exists():
            raise ExportError(f"裁剪清单路径在子集中缺席（根树漂移，须先同步冻结清单）：{relative}")
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    _validate_cannbot_trim(target)


def _validate_cannbot_trim(target: Path) -> None:
    """裁剪互锁：冻结路径零残留、类别模式零命中，防根树漂移静默漏裁。"""
    for relative in (*CANNBOT_EXCLUDED_DIRS, *CANNBOT_EXCLUDED_FILES):
        if (target / relative).exists():
            raise ExportError(f"裁剪后仍存在：{relative}")
    driver_dir = target / "packages/kernel/kernel_tests/unit/driver"
    deep_leftover = sorted(
        path.name
        for path in driver_dir.glob("test_*.py")
        if path.name.startswith(DEEP_TEST_PREFIXES)
    )
    if deep_leftover:
        raise ExportError(f"unit/driver 残留逐 runtime 深度单测（须并入冻结清单）：{deep_leftover}")
    unit_dir = target / "packages/kernel/kernel_tests/unit"
    provider_leftover = sorted(path.name for path in unit_dir.glob("test_provider_*.py"))
    if provider_leftover:
        raise ExportError(f"unit 残留 provider 深度用例（须并入冻结清单）：{provider_leftover}")


def validate_cannbot_naming(target: Path) -> None:
    """命名互锁：cannbot 只认可 workflow-controller 名字，全树零上游内部仓名命中。"""
    offenders = [
        path.relative_to(target).as_posix()
        for path in sorted(target.rglob("*"))
        if path.is_file() and b"axcor" in path.read_bytes()
    ]
    if offenders:
        raise ExportError(f"子集含上游内部仓名（cannbot 档不允许）：{offenders}")


def trim_pyproject(target: Path, members: list[str], profile: str | None) -> None:
    """按成员清单裁剪根 pyproject 的 workspace members、dev 依赖与 uv.sources。"""
    member_set = set(members)
    plugin_dist_set = {
        DIST_PREFIX + Path(member).name for member in members if member != KERNEL_MEMBER
    }
    source_key_set = {"workflow-controller-kernel"} | plugin_dist_set
    renamed_root = False
    kept_lines: list[str] = []
    for line in (ROOT / "pyproject.toml").read_text(encoding="utf-8").splitlines(keepends=True):
        stripped = line.strip()
        if profile == CANNBOT_PROFILE and stripped == 'name = "axcor"':
            # cannbot 模块不带上游内部仓名，根虚拟项目名中性化
            kept_lines.append('name = "workflow-controller"\n')
            renamed_root = True
            continue
        if stripped.startswith('"packages/') and stripped.endswith('",'):
            if stripped[1:-2] not in member_set:
                continue
        elif stripped.startswith('"' + DIST_PREFIX) and stripped.endswith('",'):
            if stripped[1:-2] not in plugin_dist_set:
                continue
        elif (
            stripped.startswith(DIST_PREFIX)
            and stripped.endswith("{ workspace = true }")
            and stripped.split(" =")[0] not in source_key_set
        ):
            continue
        kept_lines.append(line)
    if profile == CANNBOT_PROFILE and not renamed_root:
        raise ExportError('未找到根项目名 "axcor"（根 pyproject 漂移，须同步 cannbot 档改名逻辑）')
    (target / "pyproject.toml").write_text("".join(kept_lines), encoding="utf-8")
    validate_trimmed_pyproject(target, member_set, plugin_dist_set, source_key_set)


def validate_trimmed_pyproject(
    target: Path, member_set: set[str], plugin_dist_set: set[str], source_key_set: set[str]
) -> None:
    """裁剪结果与成员清单互锁，防根 pyproject 格式漂移导致静默漏裁剪。"""
    config = tomllib.loads((target / "pyproject.toml").read_text(encoding="utf-8"))
    members = set(config["tool"]["uv"]["workspace"]["members"])
    if members != member_set:
        raise ExportError(f"workspace members 裁剪不符：{sorted(members)}")
    dev_plugins = {dep for dep in config["dependency-groups"]["dev"] if dep.startswith(DIST_PREFIX)}
    if dev_plugins != plugin_dist_set:
        raise ExportError(f"dev 插件依赖裁剪不符：{sorted(dev_plugins)}")
    sources = set(config["tool"]["uv"]["sources"])
    if sources != source_key_set:
        raise ExportError(f"uv.sources 裁剪不符：{sorted(sources)}")


def write_readme(target: Path, selected: list[str], profile: str | None) -> None:
    """写子集自说明 README：来源提交、内容清单与用法。"""
    try:
        source_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise ExportError(f"读取来源提交失败（须在 git 仓内导出）：{error}") from error
    plugin_lines = "\n".join(f"- `{DIST_PREFIX}{name}`" for name in selected) or "（无）"
    if profile == CANNBOT_PROFILE:
        content = f"""# Workflow Controller

由上游源仓的子集导出工具生成（来源提交 `{source_sha}`，cannbot 档），经 subtree
同步维护；除许可证表述外的修改一律先落上游源仓，再同步到本模块。

## 内容

- `workflow-controller-kernel`：内核全量源码与必要档测试（claude_sdk /
  claude_cli / opencode / codex_cli 四 runtime driver、多 provider 轮换、
  run 编排、事件库与转录层）
- `workflow-controller-plugin-fake-driver`（headless 验收驱动，测试依赖强制随附）
{plugin_lines}
- 交付插件测试全量随包；内核测试取必要档（开发门禁与逐 runtime 深度单测
  留在上游，由上游全量门禁守护）

## 本地验证

```bash
uv sync
uv run pytest -q
```
"""
    else:
        content = f"""# Workflow Controller 内核子集

由 [axcor](https://github.com/guixu-labs/axcor) 的 `scripts/export_subset.py`
导出，来源提交 `{source_sha}`。

## 内容

- `workflow-controller-kernel`（内核全量，测试随包）
- `workflow-controller-plugin-fake-driver`（headless 验收驱动，测试依赖强制随附）
{plugin_lines}
- 根测试基建（架构门禁与共享 support）与词检工具（check_ai_words）

## 用法

```bash
uv sync
uv run pytest -q
```
"""
    (target / "README.md").write_text(content, encoding="utf-8")


def run_uv_lock(target: Path) -> None:
    """在子集内重生成 uv.lock（依赖解析与全仓一致，仅成员裁剪）。"""
    try:
        subprocess.run(["uv", "lock"], cwd=target, check=True)
    except subprocess.CalledProcessError as error:
        raise ExportError(f"uv lock 失败（返回码 {error.returncode}）") from error


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="导出「内核 + 指定插件」自治子集")
    parser.add_argument("--output", type=Path, required=True, help="目标目录（须不存在或为空）")
    parser.add_argument(
        "--plugin",
        action="append",
        default=[],
        metavar="NAME",
        help=f"交付插件名（可重复）；可用：{', '.join(DELIVERABLE_PLUGINS)}",
    )
    parser.add_argument(
        "--profile",
        choices=(CANNBOT_PROFILE,),
        default=None,
        help="cannbot：按冻结清单剔除开发门禁与深度回归测试，产物即 cannbot 导入源",
    )
    args = parser.parse_args(argv)

    unknown = [name for name in args.plugin if name not in DELIVERABLE_PLUGINS]
    if unknown:
        print(f"未知插件：{unknown}；可用：{list(DELIVERABLE_PLUGINS)}", file=sys.stderr)
        return 2
    selected = sorted(dict.fromkeys(args.plugin))

    if args.output.exists() and any(args.output.iterdir()):
        print(f"目标目录非空：{args.output}", file=sys.stderr)
        return 2
    args.output.mkdir(parents=True, exist_ok=True)

    members = resolve_members(selected)
    try:
        copy_subset(args.output, members, args.profile)
        trim_pyproject(args.output, members, args.profile)
        if args.profile == CANNBOT_PROFILE:
            apply_cannbot_profile(args.output)
        write_readme(args.output, selected, args.profile)
        run_uv_lock(args.output)
        if args.profile == CANNBOT_PROFILE:
            validate_cannbot_naming(args.output)
    except ExportError as error:
        print(f"导出失败：{error}", file=sys.stderr)
        return 1

    print(f"子集已导出：{args.output}")
    print("成员：", ", ".join(members))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
