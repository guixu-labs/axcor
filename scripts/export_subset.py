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
子包不入子集。
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
    "breather",
    "file-provenance",
    "graph",
    "staged-pipeline",
)
DIST_PREFIX = "workflow-controller-plugin-"
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
    Path("scripts/check_ai_words.py"),
    Path("scripts/ai-words.txt"),
)


class ExportError(Exception):
    """导出参数或内容不符合预期。"""


def resolve_members(selected: list[str]) -> list[str]:
    """导出成员路径：kernel 在前，fake-driver 强制随附，插件按名序。"""
    members = [KERNEL_MEMBER, f"packages/plugins/{FAKE_DRIVER}"]
    members += [f"packages/plugins/{name}" for name in selected]
    return members


def copy_subset(target: Path, members: list[str]) -> None:
    """拷贝成员包、根测试树与根级文件到目标目录。"""
    for member in members:
        source = ROOT / member
        if not source.is_dir():
            raise ExportError(f"成员目录不存在：{member}")
        shutil.copytree(source, target / member, ignore=COPY_IGNORE)
    shutil.copytree(ROOT / "tests", target / "tests", ignore=COPY_IGNORE)
    for relative in ROOT_FILES:
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, destination)


def trim_pyproject(target: Path, members: list[str]) -> None:
    """按成员清单裁剪根 pyproject 的 workspace members、dev 依赖与 uv.sources。"""
    member_set = set(members)
    plugin_dist_set = {
        DIST_PREFIX + Path(member).name for member in members if member != KERNEL_MEMBER
    }
    source_key_set = {"workflow-controller-kernel"} | plugin_dist_set
    kept_lines: list[str] = []
    for line in (ROOT / "pyproject.toml").read_text(encoding="utf-8").splitlines(keepends=True):
        stripped = line.strip()
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


def write_readme(target: Path, selected: list[str]) -> None:
    """写子集自说明 README：来源提交、插件清单与用法。"""
    try:
        source_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise ExportError(f"读取来源提交失败（须在 git 仓内导出）：{error}") from error
    plugin_lines = "\n".join(f"- `{DIST_PREFIX}{name}`" for name in selected) or "（无）"
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
        copy_subset(args.output, members)
        trim_pyproject(args.output, members)
        write_readme(args.output, selected)
        run_uv_lock(args.output)
    except ExportError as error:
        print(f"导出失败：{error}", file=sys.stderr)
        return 1

    print(f"子集已导出：{args.output}")
    print("成员：", ", ".join(members))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
