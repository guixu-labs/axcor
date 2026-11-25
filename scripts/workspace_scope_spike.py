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

"""工作区作用域量化 spike。

以 git CLI 模拟未来内核工作区管理的核心操作——私有仓基线导入、作用域
worktree 建区、构建产物物化、检查点提交、回滚恢复——在合成目录上测量
耗时与磁盘成本，输出 markdown 报告，为构建产物路径策略取值提供数据。

合成目录模拟自定义算子开发的目录画像：四个源码子目录、二进制构建产物
目录（不可压缩）、带历史的用户 git 仓（含未提交与未跟踪状态）。
"""

from __future__ import annotations

import argparse
import os
import platform
import random
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

# 私有仓与保留路径布局
SCOPES_ROOT = ".workflow-controller/scopes/spike-run"
INNER_GIT_MIRROR = ".workflow-controller-inner-git"
BUILD_DIR = "op/build"
SOURCE_DIRS = ("op/op_host", "op/op_kernel", "op/op_api", "op/op_graph")


@dataclass
class SpikeConfig:
    """一次测量的全部参数。"""

    work_root: Path
    build_total_mb: int
    build_files: int
    source_files: int
    source_file_kb: int
    inner_git_mb: int
    checkpoint_rounds: int
    refresh_fraction: float
    random_seed: int


def run_git(
    args: Sequence[str],
    git_dir: Path | None = None,
    work_tree: Path | None = None,
    cwd: Path | None = None,
) -> str:
    """执行 git 子进程；禁用后台自动整理保证测量确定性，分离环境显式传入。"""
    env = dict(os.environ)
    if git_dir is not None:
        env["GIT_DIR"] = str(git_dir)
    else:
        env.pop("GIT_DIR", None)
    if work_tree is not None:
        env["GIT_WORK_TREE"] = str(work_tree)
    else:
        env.pop("GIT_WORK_TREE", None)
    proc = subprocess.run(
        [
            "git",
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
            *args,
        ],
        env=env,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} 失败：{proc.stderr.strip()}")
    return proc.stdout


def git_version() -> str:
    """返回本机 git 版本描述。"""
    return subprocess.run(
        ["git", "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()


def tree_stats(path: Path) -> tuple[int, int]:
    """统计目录文件数与磁盘块占用，返回（文件数，字节数）。"""
    files = 0
    total = 0
    if not path.exists():
        return 0, 0
    for entry in path.rglob("*"):
        if entry.is_file() and not entry.is_symlink():
            files += 1
            total += entry.stat().st_blocks * 512
    return files, total


def mb(size_bytes: int) -> str:
    """字节数格式化为 MB 字符串。"""
    return f"{size_bytes / 1024 / 1024:.1f} MB"


def sec(seconds: float) -> str:
    """秒数格式化为易读字符串。"""
    return f"{seconds:.2f} s"


def generate_source_tree(root: Path, rng: random.Random, files: int, file_kb: int) -> None:
    """生成四个源码子目录的文本文件与根级说明文件。"""
    for i in range(files):
        sub = SOURCE_DIRS[i % len(SOURCE_DIRS)]
        target = root / sub / f"module_{i // len(SOURCE_DIRS):04d}"
        target.mkdir(parents=True, exist_ok=True)
        lines = [
            f"// module {i} line {j} salt {rng.randrange(1 << 30):08x}\n"
            for j in range(file_kb * 16)
        ]
        (target / f"unit_{i:04d}.cc").write_text("".join(lines), encoding="utf-8")
    (root / "CMakeLists.txt").write_text(
        "cmake_minimum_required(VERSION 3.16)\nproject(operator)\n", encoding="utf-8"
    )
    (root / "CODE_REVIEW.md").write_text("# 评审记录\n", encoding="utf-8")


def generate_build_tree(root: Path, total_mb: int, files: int) -> None:
    """生成构建产物目录：三个子目录均分随机二进制与少量时间戳文本。"""
    if files <= 0 or total_mb <= 0:
        return
    per_file = max(1, total_mb * 1024 * 1024 // files)
    subdirs = ("autogen", "out", "kernel")
    for i in range(files):
        target = root / BUILD_DIR / subdirs[i % len(subdirs)]
        target.mkdir(parents=True, exist_ok=True)
        (target / f"artifact_{i:05d}.bin").write_bytes(os.urandom(per_file))
    autogen = root / BUILD_DIR / "autogen"
    autogen.mkdir(parents=True, exist_ok=True)
    (autogen / "copy_kernel_src.timestamp").write_text("20261101\n", encoding="utf-8")


def generate_user_git_repo(root: Path, total_mb: int) -> None:
    """制造带历史的用户 git 仓：大对象提交后删除，历史留对象、工作树轻量。"""
    run_git(["init", "-q"], cwd=root)
    run_git(["config", "user.name", "spike"], cwd=root)
    run_git(["config", "user.email", "spike@example.com"], cwd=root)
    (root / "README.md").write_text("operator workspace\n", encoding="utf-8")
    run_git(["add", "README.md"], cwd=root)
    run_git(["commit", "-qm", "init"], cwd=root)
    chunks = 4
    per_blob_mb = max(1, total_mb // chunks) if total_mb > 0 else 0
    for i in range(chunks if per_blob_mb > 0 else 0):
        blob = root / f".history-blob-{i}.bin"
        blob.write_bytes(os.urandom(per_blob_mb * 1024 * 1024))
        run_git(["add", "-f", blob.name], cwd=root)
        run_git(["commit", "-qm", f"history {i}"], cwd=root)
        run_git(["rm", "-q", blob.name], cwd=root)
        run_git(["commit", "-qm", f"drop {i}"], cwd=root)
    with (root / "README.md").open("a", encoding="utf-8") as readme:
        readme.write("local draft\n")
    (root / "notes-untracked.md").write_text("未跟踪笔记\n", encoding="utf-8")


def generate_synthetic_workspace(cfg: SpikeConfig) -> Path:
    """生成合成工作目录并返回其路径。"""
    rng = random.Random(cfg.random_seed)
    root = cfg.work_root / "operator-workspace"
    root.mkdir(parents=True)
    generate_source_tree(root, rng, cfg.source_files, cfg.source_file_kb)
    generate_build_tree(root, cfg.build_total_mb, cfg.build_files)
    generate_user_git_repo(root, cfg.inner_git_mb)
    return root


def import_baseline(root: Path, repo: Path) -> dict[str, object]:
    """建立私有仓并导入基线：工作树全量、用户仓镜像保留路径、单基线提交。"""
    timings: dict[str, float] = {}

    def timed(label: str, action) -> None:
        start = time.perf_counter()
        action()
        timings[label] = time.perf_counter() - start

    repo.mkdir(parents=True)
    run_git(["init", "-q", "--bare", str(repo)])
    run_git(["config", "core.hooksPath", ""], git_dir=repo)
    run_git(["config", "user.name", "spike"], git_dir=repo)
    run_git(["config", "user.email", "spike@example.com"], git_dir=repo)
    run_git(["symbolic-ref", "HEAD", "refs/heads/integration/line"], git_dir=repo)

    def add_worktree_state() -> None:
        run_git(
            [
                "add",
                "-A",
                "-f",
                "--",
                ".",
                ":(exclude).git",
                f":(exclude){INNER_GIT_MIRROR}",
                ":(exclude).workflow-controller",
            ],
            git_dir=repo,
            work_tree=root,
            cwd=root,
        )

    def mirror_user_git() -> None:
        if (root / ".git").is_dir():
            shutil.copytree(root / ".git", root / INNER_GIT_MIRROR)

    def strip_build_from_line() -> None:
        run_git(["rm", "-r", "-q", "--cached", BUILD_DIR], git_dir=repo, work_tree=root, cwd=root)
        run_git(["commit", "-qm", "integration line init"], git_dir=repo, work_tree=root, cwd=root)

    timed("工作树全量 add（含未提交/未跟踪）", add_worktree_state)
    timed("用户仓镜像拷贝到保留路径", mirror_user_git)
    timed(
        "保留路径独立趟 add",
        lambda: run_git(
            ["add", "-f", "--", INNER_GIT_MIRROR], git_dir=repo, work_tree=root, cwd=root
        ),
    )
    timed(
        "单基线提交",
        lambda: run_git(["commit", "-qm", "baseline seed"], git_dir=repo, work_tree=root, cwd=root),
    )
    seed = run_git(["rev-parse", "HEAD"], git_dir=repo).strip()
    run_git(["update-ref", "refs/checkpoints/root-seed", seed], git_dir=repo)
    timed("集成头剥离构建路径", strip_build_from_line)
    line_head = run_git(["rev-parse", "HEAD"], git_dir=repo).strip()
    return {"timings": timings, "seed": seed, "integration_head": line_head}


def create_scope_worktree(repo: Path, workspace: Path, branch: str, base_commit: str) -> float:
    """从指定提交建作用域 worktree 分支，返回建区耗时。"""
    start = time.perf_counter()
    run_git(["worktree", "add", "-q", "-b", branch, str(workspace), base_commit], git_dir=repo)
    return time.perf_counter() - start


def materialize_build(worktree: Path, from_commit: str) -> float:
    """从指定提交检出构建产物路径（文件与暂存区），返回耗时。"""
    start = time.perf_counter()
    run_git(["checkout", "-q", from_commit, "--", BUILD_DIR], cwd=worktree)
    return time.perf_counter() - start


def commit_scope_state(worktree: Path, message: str) -> tuple[float, str]:
    """全量暂存并提交作用域当前状态，返回耗时与提交号。"""
    start = time.perf_counter()
    run_git(["add", "-A", "-f"], cwd=worktree)
    run_git(["commit", "-qm", message], cwd=worktree)
    return time.perf_counter() - start, run_git(["rev-parse", "HEAD"], cwd=worktree).strip()


def refresh_build_artifacts(worktree: Path, rng: random.Random, fraction: float) -> dict[str, int]:
    """模拟构建刷新：重写部分产物、少量删除与新增，返回变更计数。"""
    artifacts = sorted((worktree / BUILD_DIR).rglob("*.bin"))
    rewrite = [path for path in artifacts if rng.random() < fraction]
    for path in rewrite:
        path.write_bytes(os.urandom(path.stat().st_size))
    removed = 0
    for path in rewrite[: max(1, len(rewrite) // 10)]:
        path.unlink(missing_ok=True)
        removed += 1
    added = 0
    out_dir = worktree / BUILD_DIR / "out"
    for _ in range(max(1, len(rewrite) // 10)):
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"fresh_{rng.randrange(1 << 32):08x}.bin").write_bytes(os.urandom(1 << 18))
        added += 1
    return {"rewrite": len(rewrite), "removed": removed, "added": added}


def edit_source_files(worktree: Path, rng: random.Random, count: int, tag: str) -> int:
    """模拟节点内源码修改，返回实际修改文件数。"""
    sources = [path for sub in SOURCE_DIRS for path in (worktree / sub).rglob("*.cc")]
    if not sources:
        return 0
    picks = rng.sample(sources, min(count, len(sources)))
    for path in picks:
        with path.open("a", encoding="utf-8") as source:
            source.write(f"// spike edit {tag}\n")
    return len(picks)


def checkpoint_round(
    worktree: Path, repo: Path, rng: random.Random, cfg: SpikeConfig, round_no: int
) -> dict[str, object]:
    """执行一轮打点：刷新产物、改源码、全量提交，记录耗时与磁盘增量。"""
    repo_before = tree_stats(repo)[1]
    changes = refresh_build_artifacts(worktree, rng, cfg.refresh_fraction)
    edited = edit_source_files(worktree, rng, 8, f"round-{round_no}")
    elapsed, commit_id = commit_scope_state(worktree, f"checkpoint {round_no}")
    return {
        "轮次": round_no,
        "重写产物数": changes["rewrite"],
        "删除/新增产物数": (changes["removed"], changes["added"]),
        "源码修改数": edited,
        "add+commit 耗时": elapsed,
        "私有仓磁盘增量": tree_stats(repo)[1] - repo_before,
        "提交": commit_id[:12],
    }


def control_round(worktree: Path, repo: Path, rng: random.Random) -> dict[str, object]:
    """对照打点：只提交源码修改（构建产物不进检查点的口径），记录耗时与增量。"""
    repo_before = tree_stats(repo)[1]
    edited = edit_source_files(worktree, rng, 8, "control")
    start = time.perf_counter()
    run_git(["add", "-f", "--", *SOURCE_DIRS], cwd=worktree)
    run_git(["commit", "-qm", "checkpoint control"], cwd=worktree)
    elapsed = time.perf_counter() - start
    return {
        "源码修改数": edited,
        "add+commit 耗时": elapsed,
        "私有仓磁盘增量": tree_stats(repo)[1] - repo_before,
    }


def rollback_by_reset(worktree: Path, scope_base: str) -> float:
    """回滚恢复：作用域分支 reset 到建区检查点并清理，返回耗时。"""
    start = time.perf_counter()
    run_git(["reset", "-q", "--hard", scope_base], cwd=worktree)
    run_git(["clean", "-fdq"], cwd=worktree)
    return time.perf_counter() - start


def rebuild_scope(
    repo: Path, workspace: Path, branch: str, base_commit: str, seed: str
) -> dict[str, float]:
    """废弃重建：删 worktree 后从集成头重建并物化构建产物，返回分段耗时。"""
    timings: dict[str, float] = {}
    start = time.perf_counter()
    if workspace.exists():
        run_git(["worktree", "remove", "--force", str(workspace)], git_dir=repo)
    timings["废弃旧工作区"] = time.perf_counter() - start
    timings["重建 worktree"] = create_scope_worktree(repo, workspace, branch, base_commit)
    timings["物化构建产物"] = materialize_build(workspace, seed)
    elapsed, _ = commit_scope_state(workspace, "scope base after rebuild")
    timings["提交建区基线"] = elapsed
    return timings


def run_measurements(cfg: SpikeConfig) -> dict[str, object]:
    """执行五项指标测量，返回报告数据结构。"""
    free_before = shutil.disk_usage(cfg.work_root).free
    gen_start = time.perf_counter()
    root = generate_synthetic_workspace(cfg)
    generation_seconds = time.perf_counter() - gen_start

    source_files, source_bytes = tree_stats(root / "op")
    build_files, build_bytes = tree_stats(root / BUILD_DIR)
    _, inner_git_bytes = tree_stats(root / ".git")
    profile = {
        "合成目录生成": sec(generation_seconds),
        "源码+构建文件数": source_files,
        "源码+构建磁盘占用": mb(source_bytes),
        "构建产物文件数": build_files,
        "构建产物磁盘占用": mb(build_bytes),
        "用户仓 .git 磁盘占用": mb(inner_git_bytes),
    }

    repo = cfg.work_root / SCOPES_ROOT / "repo.git"
    baseline = import_baseline(root, repo)
    baseline["私有仓磁盘占用"] = mb(tree_stats(repo)[1])

    workspaces = cfg.work_root / SCOPES_ROOT / "workspaces"
    node_a = workspaces / "node-a"
    checkout_seconds = create_scope_worktree(
        repo, node_a, "scope/node-a", str(baseline["integration_head"])
    )
    worktree_checkout = {
        "建区耗时（检出集成头，不含构建产物）": sec(checkout_seconds),
        "worktree 磁盘占用（不含构建产物）": mb(tree_stats(node_a)[1]),
    }

    materialize_seconds = materialize_build(node_a, str(baseline["seed"]))
    commit_seconds, scope_base = commit_scope_state(node_a, "scope base")
    inherit_first = {
        "物化耗时（自基线提交检出构建产物）": sec(materialize_seconds),
        "建区基线提交耗时": sec(commit_seconds),
        "物化后 worktree 磁盘占用": mb(tree_stats(node_a)[1]),
    }

    rounds = [
        checkpoint_round(node_a, repo, random.Random(cfg.random_seed * 100 + i), cfg, i + 1)
        for i in range(cfg.checkpoint_rounds)
    ]
    control = control_round(node_a, repo, random.Random(cfg.random_seed * 100 + 99))

    node_b = workspaces / "node-b"
    downstream_checkout = create_scope_worktree(
        repo, node_b, "scope/node-b", str(baseline["integration_head"])
    )
    upstream_head = run_git(["rev-parse", "HEAD"], cwd=node_a).strip()
    downstream_materialize = materialize_build(node_b, upstream_head)
    downstream_commit, _ = commit_scope_state(node_b, "scope base inherited")
    inherit_downstream = {
        "建区耗时": sec(downstream_checkout),
        "物化耗时（自上游终态检出构建产物）": sec(downstream_materialize),
        "提交耗时": sec(downstream_commit),
        "下游 worktree 磁盘占用": mb(tree_stats(node_b)[1]),
    }

    reset_seconds = rollback_by_reset(node_a, scope_base)
    rebuild = rebuild_scope(
        repo,
        node_a,
        "scope/node-a-rebuild",
        str(baseline["integration_head"]),
        str(baseline["seed"]),
    )

    free_after = shutil.disk_usage(cfg.work_root).free
    repo_before_gc = tree_stats(repo)[1]
    gc_start = time.perf_counter()
    run_git(["gc", "-q", "--prune=now"], git_dir=repo)
    gc_seconds = time.perf_counter() - gc_start
    return {
        "环境": {
            "日期": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "平台": platform.platform(),
            "Python": sys.version.split()[0],
            "git": git_version(),
            "参数": (
                f"构建产物 {cfg.build_total_mb} MB / {cfg.build_files} 文件、"
                f"源码 {cfg.source_files} 文件 × {cfg.source_file_kb} KB、"
                f"用户仓 {cfg.inner_git_mb} MB、刷新比例 {cfg.refresh_fraction:.0%}、"
                f"打点 {cfg.checkpoint_rounds} 轮、随机种子 {cfg.random_seed}"
            ),
        },
        "目录画像": profile,
        "初始基线导入": baseline,
        "worktree 检出": worktree_checkout,
        "构建产物继承（首层）": inherit_first,
        "检查点提交（构建产物纳入）": rounds,
        "检查点提交（对照，构建产物排除）": control,
        "下游继承构建产物": inherit_downstream,
        "回滚恢复": {
            "reset+clean 恢复耗时": sec(reset_seconds),
            **{f"废弃重建·{label}": sec(value) for label, value in rebuild.items()},
        },
        "磁盘总账": {
            "测量前可用": mb(free_before),
            "测量后可用": mb(free_after),
            "本次净占用": mb(free_before - free_after),
            "私有仓最终大小": mb(repo_before_gc),
            "gc 压实后私有仓大小（立即回收不可达对象）": mb(tree_stats(repo)[1]),
            "gc 压实耗时": sec(gc_seconds),
        },
    }


def render_timings(title: str, entries: dict[str, object]) -> list[str]:
    """把一个计时小节渲染为 markdown 行。"""
    lines = [f"### {title}", "", "| 项 | 值 |", "|---|---|"]
    for label, value in entries.items():
        lines.append(f"| {label} | {value} |")
    lines.append("")
    return lines


def render_markdown(report: dict[str, object]) -> str:
    """把报告数据渲染为 markdown 文本。"""
    lines = ["# 工作区作用域量化 spike 报告", ""]
    for label, value in report["环境"].items():
        lines.append(f"- {label}：{value}")
    lines.extend(render_timings("目录画像", report["目录画像"]))
    baseline = report["初始基线导入"]
    baseline_view = {
        label: (value[:12] if label in ("seed", "integration_head") else value)
        for label, value in baseline.items()
        if label != "timings"
    }
    baseline_view |= {label: sec(value) for label, value in baseline["timings"].items()}
    lines.extend(render_timings("一、初始基线导入", baseline_view))
    lines.extend(render_timings("二、单节点 worktree 检出", report["worktree 检出"]))
    lines.extend(render_timings("三、构建产物继承（首层建区）", report["构建产物继承（首层）"]))
    rounds = report["检查点提交（构建产物纳入）"]
    lines.append("### 四、检查点提交（构建产物纳入 checkpointed）")
    lines.append("")
    lines.append("| 轮 | 重写产物 | 删除/新增 | 源码修改 | add+commit 耗时 | 私有仓增量 |")
    lines.append("|---|---|---|---|---|---|")
    for round_info in rounds:
        lines.append(
            "| {轮次} | {重写产物数} | {删除/新增产物数} | {源码修改数} | {耗时} | {增量} |".format(
                耗时=sec(round_info["add+commit 耗时"]),
                增量=mb(round_info["私有仓磁盘增量"]),
                **round_info,
            )
        )
    control = report["检查点提交（对照，构建产物排除）"]
    lines.append("")
    lines.append(
        f"对照（构建产物排除、仅源码进检查点）：耗时 {sec(control['add+commit 耗时'])}、"
        f"私有仓增量 {mb(control['私有仓磁盘增量'])}、源码修改 {control['源码修改数']} 文件"
    )
    lines.append("")
    lines.extend(render_timings("五、下游继承构建产物", report["下游继承构建产物"]))
    lines.extend(render_timings("六、回滚恢复", report["回滚恢复"]))
    lines.extend(render_timings("七、磁盘总账", report["磁盘总账"]))
    return "\n".join(lines)


def ensure_free_space(work_root: Path, cfg: SpikeConfig) -> None:
    """测量前检查可用磁盘，避免中途写满。"""
    logical_mb = cfg.build_total_mb + cfg.source_files * cfg.source_file_kb // 1024
    logical_mb += cfg.inner_git_mb + 64
    required = (logical_mb * 4 + 2048) * 1024 * 1024
    free = shutil.disk_usage(work_root).free
    if free < required:
        raise SystemExit(
            f"可用磁盘不足：约需 {required / 2**30:.1f} GiB，当前 {free / 2**30:.1f} GiB"
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="工作区作用域量化 spike")
    parser.add_argument("--work-root", type=Path, help="测量工作目录（默认临时目录）")
    parser.add_argument("--keep", action="store_true", help="测量后保留现场")
    parser.add_argument("--report", type=Path, help="报告输出文件（markdown）")
    parser.add_argument("--build-total-mb", type=int, default=1024, help="构建产物总大小（MB）")
    parser.add_argument("--build-files", type=int, default=2000, help="构建产物文件数")
    parser.add_argument("--source-files", type=int, default=4000, help="源码文件数")
    parser.add_argument("--source-file-kb", type=int, default=16, help="单个源码文件大小（KB）")
    parser.add_argument("--inner-git-mb", type=int, default=64, help="用户仓历史体积（MB）")
    parser.add_argument("--checkpoint-rounds", type=int, default=3, help="打点轮数")
    parser.add_argument("--refresh-fraction", type=float, default=0.25, help="每轮产物重写比例")
    parser.add_argument("--seed", type=int, default=20261124, help="随机种子")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """入口：准备现场、执行测量、输出报告。"""
    args = parse_args(argv)
    if args.work_root is None:
        work_root = Path(tempfile.mkdtemp(prefix="workspace-spike."))
    else:
        work_root = args.work_root.resolve()
        if work_root.exists() and any(work_root.iterdir()):
            raise SystemExit(f"--work-root 已非空，拒绝使用：{work_root}")
    work_root.mkdir(parents=True, exist_ok=True)
    cleanup = not args.keep
    if args.report is not None:
        report_path = args.report.resolve()
        if report_path.is_relative_to(work_root) and cleanup:
            raise SystemExit("--report 不能落在即将清理的工作目录内")
    cfg = SpikeConfig(
        work_root=work_root,
        build_total_mb=args.build_total_mb,
        build_files=args.build_files,
        source_files=args.source_files,
        source_file_kb=args.source_file_kb,
        inner_git_mb=args.inner_git_mb,
        checkpoint_rounds=args.checkpoint_rounds,
        refresh_fraction=args.refresh_fraction,
        random_seed=args.seed,
    )
    ensure_free_space(work_root, cfg)
    report = run_measurements(cfg)
    text = render_markdown(report)
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text, encoding="utf-8")
    print(text)
    if cleanup:
        shutil.rmtree(work_root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
