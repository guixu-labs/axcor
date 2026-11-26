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

"""集成协调器：串行集成、按路径构造集成树与两档冲突判定。

本模块是私有仓之外唯一的 git 子进程出口：求限定路径集的变更清单、
三方合并判定与内容生成、把 merge 路径 diff 应用到父树并提交，以及
工作区释放。判定以写者建区时的集成头为公共基准、限定 merge 路径集：
双方变更路径集不相交则按路径构造集成树；相交时按档位处理——可合并
档走三方合并（干净则并入集成树）、严格档直接判冲突。冲突结果携带
冲突路径与双方身份。集成线只前进不回退；已合并回滚的集成线回退与
保留者换序重放也经本模块：回退链先在对象库内构造完成、再一次性
推进根引用与根检出，失败时引用不动。
"""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from workflow_controller.core.workspace.oplog import CommitFacts, operation_marker
from workflow_controller.core.workspace.repository import ROOT_INTEGRATION_REF

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from workflow_controller.core.workspace.repository import WorkspaceRepository

__all__ = [
    "MERGE_TREE_MIN_GIT_VERSION",
    "ConflictPolicy",
    "IntegrationConflict",
    "IntegrationCoordinator",
    "IntegrationCoordinatorError",
    "IntegrationSuccess",
    "ReplayStep",
    "WorkspaceTarget",
]

# 新式三方合并写法要求的最低 git 版本
MERGE_TREE_MIN_GIT_VERSION = (2, 38)

# 根检出状态提交的固定排除：用户仓与托管目录（私有仓住在其中，吞入会
# 反噬索引与对象库）
_ROOT_RESERVED_EXCLUDES = (":(exclude).git", ":(exclude).workflow-controller")

# 全部 git 调用共带的开关与提交身份（与私有仓出口保持一致）
_GIT_COMMON_ARGS = (
    "-c",
    "gc.auto=0",
    "-c",
    "maintenance.auto=false",
    "-c",
    "commit.gpgsign=false",
)
_GIT_IDENTITY_ENV = {
    "GIT_AUTHOR_NAME": "workflow-controller-workspace",
    "GIT_AUTHOR_EMAIL": "workflow-controller-workspace@local",
    "GIT_COMMITTER_NAME": "workflow-controller-workspace",
    "GIT_COMMITTER_EMAIL": "workflow-controller-workspace@local",
}
_SEPARATED_GIT_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE")

_VERSION_PATTERN = re.compile(r"git version (\d+)\.(\d+)")


class IntegrationCoordinatorError(RuntimeError):
    """集成操作失败：一律显式失败，不自动降级。"""


class ConflictPolicy(StrEnum):
    """冲突检测档位：可合并性为准或路径集重叠即违规。"""

    GIT_MERGE = "git_merge"
    STRICT = "strict"


@dataclass(frozen=True)
class WorkspaceTarget:
    """一个可提交的 git 工作区定位：根检出或作用域工作区。"""

    path: Path
    # 根检出无 .git 指针，须经分离环境变量定位私有仓
    separated_env: bool


@dataclass(frozen=True)
class IntegrationConflict:
    """一次集成冲突的完整载荷：路径与双方身份、双侧提交与共同基准。"""

    conflict_paths: tuple[str, ...]
    writer_scope_id: str
    parent_scope_id: str
    base_commit: str
    writer_head: str
    parent_head: str


@dataclass(frozen=True)
class IntegrationSuccess:
    """一次成功集成的结果：父线新头与写者进入父树的路径清单。"""

    new_head: str
    integrated_paths: tuple[str, ...]


@dataclass(frozen=True)
class ReplayStep:
    """换序重放的一个保留者：建区基线与其终态提交。"""

    scope_id: str
    base_commit: str
    head_commit: str


class IntegrationCoordinator:
    """一棵作用域树的集成引擎：无账本状态，全部参数由调用方显式给出。"""

    def __init__(
        self,
        repository: WorkspaceRepository,
        *,
        conflict_policy: ConflictPolicy = ConflictPolicy.GIT_MERGE,
    ) -> None:
        """绑定私有仓与冲突档位，并核验 git 版本满足三方合并写法。"""
        self._repository = repository
        self._conflict_policy = conflict_policy
        self._require_merge_tree_capable_git()

    async def integrate_writer(
        self,
        *,
        writer_scope_id: str,
        parent_scope_id: str,
        base_commit: str,
        writer_head: str,
        parent_head: str,
        parent_target: WorkspaceTarget,
        merge_pathspecs: Sequence[str],
        commit_message: str,
    ) -> IntegrationSuccess | IntegrationConflict:
        """按两档判定把写者的 merge 路径 diff 并入父树。

        公式：以写者建区基线为公共基准求双方限定路径集，不相交则构造
        集成树；相交时严格档判冲突、可合并档走三方合并。写者无 merge
        路径变更时为空集成，父线不动。
        """
        writer_paths = await self.changed_paths(base_commit, writer_head, merge_pathspecs)
        if not writer_paths:
            return IntegrationSuccess(new_head=parent_head, integrated_paths=())
        parent_paths = await self.changed_paths(base_commit, parent_head, merge_pathspecs)
        overlap = sorted(set(writer_paths) & set(parent_paths))
        if overlap and self._conflict_policy is ConflictPolicy.STRICT:
            return self._conflict(
                tuple(overlap),
                writer_scope_id,
                parent_scope_id,
                base_commit,
                writer_head,
                parent_head,
            )
        # 构造只含 merge 路径变更的写者侧树，再以公共基准做三方合并：
        # 自建与共享类路径既不在基准也不在父侧，完整合并会把它们带进父树，
        # 先行限定是集成线不被污染的前提
        writer_restricted = await self._build_restricted_commit(
            base_commit=base_commit,
            head=writer_head,
            merge_pathspecs=merge_pathspecs,
            message=f"merge-path delta of {writer_scope_id}",
        )
        merged_tree, conflict_paths = await self._merge_trees(
            base=base_commit, ours=parent_head, theirs=writer_restricted
        )
        if merged_tree is None:
            return self._conflict(
                tuple(conflict_paths),
                writer_scope_id,
                parent_scope_id,
                base_commit,
                writer_head,
                parent_head,
            )
        new_head = await self._advance_target_to_tree(parent_target, merged_tree, commit_message)
        return IntegrationSuccess(new_head=new_head, integrated_paths=tuple(writer_paths))

    def _conflict(
        self,
        conflict_paths: tuple[str, ...],
        writer_scope_id: str,
        parent_scope_id: str,
        base_commit: str,
        writer_head: str,
        parent_head: str,
    ) -> IntegrationConflict:
        """组装冲突载荷：路径、双方身份、双侧提交与共同基准。"""
        return IntegrationConflict(
            conflict_paths=conflict_paths,
            writer_scope_id=writer_scope_id,
            parent_scope_id=parent_scope_id,
            base_commit=base_commit,
            writer_head=writer_head,
            parent_head=parent_head,
        )

    async def changed_paths(self, base: str, head: str, pathspecs: Sequence[str]) -> list[str]:
        """求 base..head 限定路径集的变更路径（重命名按旧删新增计）。"""
        out = await self._run_repo_git(
            ["diff", "--name-only", "-z", "--no-renames", base, head, "--", *pathspecs]
        )
        return [path for path in out.split("\0") if path]

    async def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        """谱系判定：ancestor 是否为 descendant 的祖先（含相等）。"""
        proc = await self._start_repo_git(["merge-base", "--is-ancestor", ancestor, descendant])
        await proc.communicate()
        if proc.returncode == 0:
            return True
        if proc.returncode == 1:
            return False
        raise IntegrationCoordinatorError(
            f"git merge-base --is-ancestor 失败（rc={proc.returncode}）：{ancestor} → {descendant}"
        )

    async def commit_exists(self, commit: str) -> bool:
        """提交对象是否存在于私有仓。"""
        proc = await self._start_repo_git(
            ["rev-parse", "--verify", "--quiet", f"{commit}^{{commit}}"]
        )
        await proc.communicate()
        return proc.returncode == 0

    async def read_commit_facts(self, commit: str) -> CommitFacts | None:
        """读一条提交的父提交与提交信息（操作日志判据核验的事实源）。"""
        out = await self._run_repo_git(["log", "-1", "--pretty=%P%x00%B", commit])
        parent, _, message = out.partition("\0")
        first_parent = parent.strip().split(" ", 1)[0] if parent.strip() else ""
        return CommitFacts(parent=first_parent, message=message) if first_parent else None

    async def line_integration_order(self, head: str) -> list[tuple[str, str]]:
        """沿集成线首父链解析集成序：[(作用域, 集成提交)]，按时间正序。

        集成与保留者重放的线提交信息都以「integrate <作用域>」开头，
        换序重放按此序进行可复现的冲突检查序列。
        """
        subjects = await self._first_parent_subjects(head)
        reversed_pairs: list[tuple[str, str]] = []
        for commit, subject in reversed(subjects):
            if subject.startswith("integrate "):
                reversed_pairs.append((subject.removeprefix("integrate ").strip(), commit))
        return reversed_pairs

    async def first_parent_commits(self, head: str) -> set[str]:
        """集成线首父链上的全部提交（含 head 与基线种子）。"""
        return {commit for commit, _subject in await self._first_parent_subjects(head)}

    async def earliest_line_commit(self, head: str, commits: Sequence[str]) -> str | None:
        """求一组提交在集成线首父链上的最早者（离头最远）；缺席任一返回 None。"""
        order = {
            commit: index
            for index, (commit, _subject) in enumerate(await self._first_parent_subjects(head))
        }
        positions = [order.get(commit) for commit in commits]
        if any(position is None for position in positions):
            return None
        return commits[max(range(len(commits)), key=lambda index: positions[index])]

    async def _first_parent_subjects(self, head: str) -> list[tuple[str, str]]:
        """首父链的 (提交, 主题) 对，按时间倒序（新在前）。"""
        out = await self._run_repo_git(["log", "--first-parent", "--pretty=%H%x00%s", head])
        pairs: list[tuple[str, str]] = []
        for line in out.splitlines():
            commit, _, subject = line.partition("\0")
            if commit:
                pairs.append((commit, subject))
        return pairs

    async def root_status_clean(self, exclude_pathspecs: Sequence[str]) -> bool:
        """根检出在集成线路径面上是否存在未提交变化（自建产物不在此面）。"""
        target = WorkspaceTarget(path=self._repository.root_dir, separated_env=True)
        restriction = [".", *_ROOT_RESERVED_EXCLUDES, *exclude_pathspecs]
        status = await self._run_target_git(target, ["status", "--porcelain", "--", *restriction])
        return not status.strip()

    async def retract_line_to(
        self,
        *,
        target_commit: str,
        replay: Sequence[ReplayStep],
        merge_pathspecs: Sequence[str],
        client_op_id: str,
    ) -> str:
        """在对象库内构造回退后的新线：reset 到目标后按序重放保留者 diff。

        只产生提交对象、不动任何引用——失败（重放冲突，构造上不应发生）
        时旧线完好；无保留者时新线即目标提交本身。
        """
        new_head = target_commit
        for step in replay:
            restricted = await self._build_restricted_commit(
                base_commit=step.base_commit,
                head=step.head_commit,
                merge_pathspecs=merge_pathspecs,
                message=f"merge-path delta of {step.scope_id}",
            )
            merged_tree, conflict_paths = await self._merge_trees(
                base=step.base_commit, ours=new_head, theirs=restricted
            )
            if merged_tree is None:
                raise IntegrationCoordinatorError(
                    f"保留者重放出现合并冲突（构造上不应发生）：{step.scope_id}"
                    f" 冲突路径 {conflict_paths}"
                )
            message = f"integrate {step.scope_id}\n\n{operation_marker(client_op_id)}"
            new_head = (
                await self._run_repo_git(
                    ["commit-tree", merged_tree, "-p", new_head, "-m", message]
                )
            ).strip()
        return new_head

    async def advance_root_checkout(self, new_head: str) -> None:
        """把集成线引用与根检出推进到指定提交（reset 落地步，幂等）。"""
        await self._run_repo_git(["update-ref", ROOT_INTEGRATION_REF, new_head])
        target = WorkspaceTarget(path=self._repository.root_dir, separated_env=True)
        await self._run_target_git(target, ["read-tree", "-u", "--reset", new_head])

    async def populate_checkout(self, work_tree: Path, commit: str) -> None:
        """把指定提交检出到独立工作目录（分离环境，目录内不留 git 元数据）。

        单会话树的内部集成线检出用：原目录保持基线不动，线状态落在
        树目录里的内部目录上。
        """
        work_tree.mkdir(parents=True, exist_ok=True)
        target = WorkspaceTarget(path=work_tree, separated_env=True)
        await self._run_target_git(target, ["read-tree", "-u", "--reset", commit])

    async def commit_pending_state(
        self,
        target: WorkspaceTarget,
        *,
        exclude_pathspecs: Sequence[str],
        message: str,
    ) -> str:
        """把目标工作区未提交状态按排除规格落成提交，返回当前头。

        排除规格须为 git 就绪形态（带魔法前缀）；清单外的内容全部视
        为该工作区的真实写入并落账，无未提交变更时不产生空提交。根
        检出另固定排除用户仓与托管目录。
        """
        restriction = [".", *exclude_pathspecs]
        if target.separated_env:
            restriction = [".", *_ROOT_RESERVED_EXCLUDES, *exclude_pathspecs]
        status = await self._run_target_git(target, ["status", "--porcelain", "--", *restriction])
        if not status.strip():
            return (await self._run_target_git(target, ["rev-parse", "HEAD"])).strip()
        await self._run_target_git(target, ["add", "-A", "--force", "--", *restriction])
        await self._run_target_git(target, ["commit", "--quiet", "--message", message])
        return (await self._run_target_git(target, ["rev-parse", "HEAD"])).strip()

    async def release_worktree(self, workspace_path: Path) -> None:
        """释放作用域工作区：移除工作树与登记元数据，分支保留。"""
        await self._run_repo_git(["worktree", "remove", "--force", str(workspace_path)])

    def _require_merge_tree_capable_git(self) -> None:
        """版本探测：低于三方合并写法要求的 git 直接拒绝使用。"""
        try:
            out = subprocess.run(
                ["git", "--version"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout
        except (OSError, subprocess.CalledProcessError) as exc:
            raise IntegrationCoordinatorError("git 不可用") from exc
        matched = _VERSION_PATTERN.search(out)
        if matched is None:
            raise IntegrationCoordinatorError(f"无法解析 git 版本输出：{out.strip()}")
        version = (int(matched.group(1)), int(matched.group(2)))
        if version < MERGE_TREE_MIN_GIT_VERSION:
            raise IntegrationCoordinatorError(
                f"git 版本过低，无法使用三方合并写法：{version} < {MERGE_TREE_MIN_GIT_VERSION}"
            )

    async def _build_restricted_commit(
        self, *, base_commit: str, head: str, merge_pathspecs: Sequence[str], message: str
    ) -> str:
        """构造只含限定路径变更的提交：基准树应用限定 diff 后落提交对象。"""
        patch = await self._run_repo_git_bytes(
            ["diff", "--binary", base_commit, head, "--", *merge_pathspecs]
        )
        temp_index = self._repository.repo_dir / f"index.restrict-{uuid.uuid4().hex}"
        try:
            await self._run_repo_git(["read-tree", base_commit], index_file=temp_index)
            await self._run_repo_git(
                ["apply", "--cached", "--binary", "-"],
                index_file=temp_index,
                input_bytes=patch,
            )
            tree = (await self._run_repo_git(["write-tree"], index_file=temp_index)).strip()
        finally:
            temp_index.unlink(missing_ok=True)
        return (
            await self._run_repo_git(["commit-tree", tree, "-p", base_commit, "-m", message])
        ).strip()

    async def _merge_trees(
        self, *, base: str, ours: str, theirs: str
    ) -> tuple[str | None, list[str]]:
        """三方合并判定：干净返回 (树, [])，冲突返回 (None, 冲突路径)。"""
        proc = await self._start_repo_git(
            [
                "merge-tree",
                "--write-tree",
                "-z",
                "--name-only",
                f"--merge-base={base}",
                ours,
                theirs,
            ]
        )
        stdout_bytes, _ = await proc.communicate()
        stdout = stdout_bytes.decode("utf-8", "surrogateescape")
        if proc.returncode not in (0, 1):
            raise IntegrationCoordinatorError(
                f"git merge-tree 失败（rc={proc.returncode}）：{stdout[-500:]}"
            )
        tokens = stdout.split("\0")
        if not tokens or not tokens[0]:
            raise IntegrationCoordinatorError(f"merge-tree 输出缺失树标识：{stdout!r}")
        tree = tokens[0]
        conflict_paths: list[str] = []
        for token in tokens[1:]:
            if not token:
                break
            conflict_paths.append(token)
        if proc.returncode == 1:
            return None, conflict_paths
        return tree, []

    async def _advance_target_to_tree(
        self, target: WorkspaceTarget, tree: str, message: str
    ) -> str:
        """把目标工作区推进到指定树并提交：索引与工作树一并同步。"""
        await self._run_target_git(target, ["read-tree", "-u", "--reset", tree])
        await self._run_target_git(
            target, ["commit", "--quiet", "--allow-empty", "--message", message]
        )
        return (await self._run_target_git(target, ["rev-parse", "HEAD"])).strip()

    async def _run_repo_git(
        self,
        args: Sequence[str],
        *,
        index_file: Path | None = None,
        input_bytes: bytes | None = None,
    ) -> str:
        """私有仓级 git 调用，成功返回 stdout 文本。"""
        return (
            await self._run_git(
                args,
                git_dir=self._repository.repo_dir,
                index_file=index_file,
                input_bytes=input_bytes,
            )
        ).decode("utf-8", "surrogateescape")

    async def _run_repo_git_bytes(
        self, args: Sequence[str], *, index_file: Path | None = None
    ) -> bytes:
        """私有仓级 git 调用，成功返回 stdout 原始字节（补丁等二进制）。"""
        return await self._run_git(args, git_dir=self._repository.repo_dir, index_file=index_file)

    async def _run_target_git(self, target: WorkspaceTarget, args: Sequence[str]) -> str:
        """目标工作区级 git 调用：根检出走分离环境，工作区走 .git 指针。"""
        raw = await self._run_git(
            args,
            git_dir=self._repository.repo_dir if target.separated_env else None,
            work_tree=target.path if target.separated_env else None,
            cwd=None if target.separated_env else target.path,
        )
        return raw.decode("utf-8", "surrogateescape")

    async def _start_repo_git(self, args: Sequence[str]) -> asyncio.subprocess.Process:
        """启动私有仓级 git 子进程（冲突判定需读退出码，不走统一封装）。"""
        return await self._start_git(args, git_dir=self._repository.repo_dir)

    async def _run_git(
        self,
        args: Sequence[str],
        *,
        git_dir: Path | None = None,
        work_tree: Path | None = None,
        index_file: Path | None = None,
        cwd: Path | None = None,
        input_bytes: bytes | None = None,
    ) -> bytes:
        """执行 git 子进程：成功返回 stdout 字节，失败显式抛错。"""
        proc = await self._start_git(
            args, git_dir=git_dir, work_tree=work_tree, index_file=index_file, cwd=cwd
        )
        stdout, stderr = await proc.communicate(input_bytes)
        if proc.returncode != 0:
            detail = stderr.decode("utf-8", "replace").strip()[-500:]
            raise IntegrationCoordinatorError(
                f"git {' '.join(args)} 失败（rc={proc.returncode}）：{detail}"
            )
        return stdout

    async def _start_git(
        self,
        args: Sequence[str],
        *,
        git_dir: Path | None = None,
        work_tree: Path | None = None,
        index_file: Path | None = None,
        cwd: Path | None = None,
    ) -> asyncio.subprocess.Process:
        """按分离环境约定启动 git 子进程。"""
        env = dict(os.environ)
        for var in _SEPARATED_GIT_VARS:
            env.pop(var, None)
        if git_dir is not None:
            env["GIT_DIR"] = str(git_dir)
        if work_tree is not None:
            env["GIT_WORK_TREE"] = str(work_tree)
        if index_file is not None:
            env["GIT_INDEX_FILE"] = str(index_file)
        env.update(_GIT_IDENTITY_ENV)
        try:
            return await asyncio.create_subprocess_exec(
                "git",
                *_GIT_COMMON_ARGS,
                *args,
                cwd=str(cwd) if cwd is not None else None,
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise IntegrationCoordinatorError("git 不可用") from exc
