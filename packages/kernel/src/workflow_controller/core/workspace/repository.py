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

"""私有仓：作用域树的 git 文件态基座。

私有仓住 ``<root>/.workflow-controller/scopes/<tree_id>/repo.git``，对象库与
refs 完全独立于用户仓；根检出以 ``GIT_DIR``/``GIT_WORK_TREE`` 分离操作，根目录
不出现任何内核 git 元数据，用户仓自身零改动。基线导入捕获工作树实际状态
（脏文件、未跟踪文件、用户 ``.git`` 的保留路径镜像），产出根双引用：seed 为
全量不可变基线，集成线头只含 merge 路径状态。首层作用域自集成头检出、从
seed 物化 writer_local 路径后提交建区基线。
"""

from __future__ import annotations

import asyncio
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

__all__ = [
    "INNER_GIT_RESERVED_PATH",
    "ROOT_INTEGRATION_REF",
    "ROOT_SEED_REF",
    "RootRefs",
    "ScopeWorkspace",
    "WorkspaceRepository",
    "WorkspaceRepositoryError",
]

# 用户 .git 在基线树中的保留路径：避开 git 保留名，恢复端可按需还原
INNER_GIT_RESERVED_PATH = ".workflow-controller-inner-git"

# 根双引用：seed = 原目录完整初始状态（不可变）；integration = merge 路径集成线
ROOT_SEED_REF = "refs/checkpoints/root-seed"
ROOT_INTEGRATION_REF = "refs/heads/integration/line"

# 基线主趟的固定排除：用户仓（另走保留路径镜像）与私有仓所在目录（防递归）
_BASELINE_EXCLUDE_PATHSPECS = (
    ":(exclude).git",
    ":(exclude).workflow-controller",
)

# 作用域分支前缀，作用域工作区与检查点提交一律落该命名空间
_SCOPE_BRANCH_PREFIX = "scope/"

# 每次 checkout 传入的路径数上限，避免超长命令行
_CHECKOUT_PATH_CHUNK = 200

# 私有仓统一提交身份（经环境变量注入，不落任何全局配置）
_GIT_IDENTITY_ENV = {
    "GIT_AUTHOR_NAME": "workflow-controller-workspace",
    "GIT_AUTHOR_EMAIL": "workflow-controller-workspace@local",
    "GIT_COMMITTER_NAME": "workflow-controller-workspace",
    "GIT_COMMITTER_EMAIL": "workflow-controller-workspace@local",
}

# 全部 git 调用共带的开关：禁自动整理（压实耗时高、不宜异步路径内触发）与签名
_GIT_COMMON_ARGS = (
    "-c",
    "gc.auto=0",
    "-c",
    "maintenance.auto=false",
    "-c",
    "commit.gpgsign=false",
)

# 需要显式管控的分离环境变量（先剥掉环境残留再按需注入）
_SEPARATED_GIT_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE")


class WorkspaceRepositoryError(RuntimeError):
    """私有仓操作失败：一律显式失败，不自动降级；git 子进程失败时 returncode 为其退出码。"""

    def __init__(self, message: str, *, returncode: int | None = None) -> None:
        super().__init__(message)
        self.returncode = returncode


@dataclass(frozen=True)
class RootRefs:
    """根双引用：seed 检查点与集成线头。"""

    seed_checkpoint: str
    integration_head: str


@dataclass(frozen=True)
class ScopeWorkspace:
    """作用域工作区建区结果。"""

    scope_key: str
    path: Path
    branch: str
    base_commit: str


def _chunked(items: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    """按块切分序列，避免超长命令行。"""
    for start in range(0, len(items), size):
        yield items[start : start + size]


class WorkspaceRepository:
    """一棵作用域树的私有仓。"""

    def __init__(self, root_dir: Path, tree_id: str) -> None:
        """绑定根目录与树标识（run_id 或 session_id），派生私有仓布局。"""
        if not tree_id:
            raise WorkspaceRepositoryError("tree_id 不能为空")
        self.root_dir = Path(root_dir).expanduser().resolve()
        self.tree_id = tree_id
        self.tree_dir = self.root_dir / ".workflow-controller" / "scopes" / tree_id
        self.repo_dir = self.tree_dir / "repo.git"
        self.workspaces_dir = self.tree_dir / "workspaces"

    async def import_baseline(self, *, writer_local_pathspecs: Sequence[str]) -> RootRefs:
        """导入基线并建立根双引用；私有仓已存在或 git 缺席时拒绝。"""
        if shutil.which("git") is None:
            raise WorkspaceRepositoryError("git 不可用")
        if self.repo_dir.exists():
            raise WorkspaceRepositoryError(f"私有仓已存在：{self.repo_dir}")
        if not self.root_dir.is_dir():
            raise WorkspaceRepositoryError(f"根目录不存在：{self.root_dir}")
        self.repo_dir.mkdir(parents=True)
        await self._init_bare_repo()
        await self._add_root_worktree_state()
        staging_dir = await asyncio.to_thread(self._stage_inner_git_mirror)
        try:
            if staging_dir is not None:
                await self._add_staged_mirror(staging_dir)
            seed = await self._commit_on_integration_line("workspace baseline seed")
            await self._run_repo_git(["update-ref", ROOT_SEED_REF, seed])
            integration_head = await self._strip_integration_line(seed, writer_local_pathspecs)
        finally:
            if staging_dir is not None:
                await asyncio.to_thread(shutil.rmtree, staging_dir, ignore_errors=True)
        return RootRefs(seed_checkpoint=seed, integration_head=integration_head)

    async def create_scope_workspace(
        self,
        scope_key: str,
        *,
        base_commit: str,
        materialize_from: str | None = None,
        materialize_pathspecs: Sequence[str] = (),
    ) -> ScopeWorkspace:
        """建作用域工作区：自 base_commit 检出、按需物化路径后提交建区基线。"""
        if not scope_key or "/" in scope_key:
            raise WorkspaceRepositoryError(f"非法 scope_key：{scope_key!r}")
        branch = f"{_SCOPE_BRANCH_PREFIX}{scope_key}"
        workspace_path = self.workspaces_dir / scope_key
        if workspace_path.exists():
            raise WorkspaceRepositoryError(f"作用域工作区已存在：{workspace_path}")
        await self._run_repo_git(
            ["worktree", "add", "--quiet", "-b", branch, str(workspace_path), base_commit]
        )
        if materialize_from is not None:
            paths = await self._enumerate_commit_paths(materialize_from, materialize_pathspecs)
            for chunk in _chunked(paths, _CHECKOUT_PATH_CHUNK):
                await self._run_workspace_git(
                    workspace_path, ["checkout", "--quiet", materialize_from, "--", *chunk]
                )
        base_commit_after_materialize = await self.commit_workspace_state(
            workspace_path, f"scope {scope_key} base"
        )
        return ScopeWorkspace(
            scope_key=scope_key,
            path=workspace_path,
            branch=branch,
            base_commit=base_commit_after_materialize,
        )

    async def commit_workspace_state(self, workspace_path: Path, message: str) -> str:
        """全量暂存并提交工作区当前状态，返回提交号。"""
        await self._run_workspace_git(workspace_path, ["add", "-A", "--force"])
        await self._run_workspace_git(
            workspace_path, ["commit", "--quiet", "--allow-empty", "--message", message]
        )
        return (await self._run_workspace_git(workspace_path, ["rev-parse", "HEAD"])).strip()

    async def resolve_ref(self, ref: str) -> str | None:
        """解析完整引用名到提交号；引用不存在返回 None。"""
        if not ref.startswith("refs/"):
            raise WorkspaceRepositoryError(f"须传完整引用名：{ref!r}")
        try:
            return (await self._run_repo_git(["rev-parse", "--verify", "--quiet", ref])).strip()
        except WorkspaceRepositoryError as exc:
            # rev-parse --verify --quiet 仅在引用缺席时退出 1；其余退出码（如仓损坏 128）是真实故障
            if exc.returncode == 1:
                return None
            raise

    async def _init_bare_repo(self) -> None:
        """初始化裸私有仓：禁 hooks、关换行归一、HEAD 指向集成线。"""
        await self._run_git(["init", "--quiet", "--bare", str(self.repo_dir)])
        await self._run_repo_git(["config", "core.hooksPath", ""])
        await self._run_repo_git(["config", "core.autocrlf", "false"])
        info_dir = self.repo_dir / "info"
        info_dir.mkdir(parents=True, exist_ok=True)
        # 基线必须逐字节一致：置空 text/filter 属性，压制树内 .gitattributes 的
        # 换行归一与外部 clean 过滤（LFS 等）
        (info_dir / "attributes").write_text("* -text -filter\n", encoding="utf-8")
        await self._run_repo_git(["symbolic-ref", "HEAD", ROOT_INTEGRATION_REF])

    async def _add_root_worktree_state(self) -> None:
        """主趟导入根目录全量状态（含脏文件与未跟踪文件）。"""
        await self._run_repo_git(
            ["add", "-A", "--force", "--", ".", *_BASELINE_EXCLUDE_PATHSPECS],
            work_tree=self.root_dir,
            cwd=self.root_dir,
        )

    def _stage_inner_git_mirror(self) -> Path | None:
        """把用户 .git 复制到根检出之外的暂存目录（保留路径名）。"""
        user_git_dir = self.root_dir / ".git"
        # .git 为文件（linked worktree 的 gitdir 指针）时不可镜像，基线不含内部 git 状态
        if not user_git_dir.is_dir():
            return None
        staging_dir = self.tree_dir / "baseline-stage"
        shutil.rmtree(staging_dir, ignore_errors=True)
        shutil.copytree(user_git_dir, staging_dir / INNER_GIT_RESERVED_PATH, symlinks=True)
        return staging_dir

    async def _add_staged_mirror(self, staging_dir: Path) -> None:
        """独立趟把保留路径镜像加进索引（不受主趟排除约束）。"""
        await self._run_repo_git(
            ["add", "--force", "--", INNER_GIT_RESERVED_PATH],
            work_tree=staging_dir,
            cwd=staging_dir,
        )

    async def _commit_on_integration_line(self, message: str) -> str:
        """把当前索引提交到集成线（HEAD 符号引用），返回提交号。"""
        await self._run_repo_git(
            ["commit", "--quiet", "--allow-empty", "--message", message],
            work_tree=self.root_dir,
            cwd=self.root_dir,
        )
        return (await self._run_repo_git(["rev-parse", "HEAD"])).strip()

    async def _strip_integration_line(
        self, seed: str, writer_local_pathspecs: Sequence[str]
    ) -> str:
        """自 seed 剥除 writer_local 路径与保留路径后提交集成线头。"""
        strip_paths = await self._list_index_paths(
            [*writer_local_pathspecs, INNER_GIT_RESERVED_PATH]
        )
        if not strip_paths:
            return seed
        payload = "\0".join(strip_paths) + "\0"
        await self._run_repo_git(
            ["update-index", "-z", "--force-remove", "--stdin"],
            work_tree=self.root_dir,
            cwd=self.root_dir,
            input=payload,
        )
        return await self._commit_on_integration_line("workspace integration line init")

    async def _list_index_paths(self, pathspecs: Sequence[str]) -> list[str]:
        """按路径规格列出当前索引中的文件。"""
        if not pathspecs:
            return []
        out = await self._run_repo_git(
            ["ls-files", "-z", "--", *pathspecs],
            work_tree=self.root_dir,
            cwd=self.root_dir,
        )
        return [path for path in out.split("\0") if path]

    async def _enumerate_commit_paths(self, commit: str, pathspecs: Sequence[str]) -> list[str]:
        """枚举某提交中命中路径规格的文件（临时索引，不动共享索引）。"""
        if not pathspecs:
            return []
        temp_index = self.repo_dir / f"index.enumerate-{uuid.uuid4().hex}"
        try:
            await self._run_repo_git(["read-tree", commit], index_file=temp_index)
            out = await self._run_repo_git(
                ["ls-files", "-z", "--", *pathspecs],
                work_tree=self.root_dir,
                cwd=self.root_dir,
                index_file=temp_index,
            )
        finally:
            temp_index.unlink(missing_ok=True)
        return [path for path in out.split("\0") if path]

    async def _run_repo_git(
        self,
        args: Sequence[str],
        *,
        work_tree: Path | None = None,
        index_file: Path | None = None,
        cwd: Path | None = None,
        input: str | None = None,
    ) -> str:
        """私有仓级 git 调用（GIT_DIR 固定指向 repo.git）。"""
        return await self._run_git(
            args,
            git_dir=self.repo_dir,
            work_tree=work_tree,
            index_file=index_file,
            cwd=cwd,
            input=input,
        )

    async def _run_workspace_git(self, workspace_path: Path, args: Sequence[str]) -> str:
        """作用域工作区内 git 调用（环境不带分离变量，由 .git 指针解析）。"""
        return await self._run_git(args, cwd=workspace_path)

    async def _run_git(
        self,
        args: Sequence[str],
        *,
        git_dir: Path | None = None,
        work_tree: Path | None = None,
        index_file: Path | None = None,
        cwd: Path | None = None,
        input: str | None = None,
    ) -> str:
        """执行 git 子进程：分离环境显式注入，成功返回 stdout。"""
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
            proc = await asyncio.create_subprocess_exec(
                "git",
                *_GIT_COMMON_ARGS,
                *args,
                cwd=str(cwd) if cwd is not None else None,
                env=env,
                stdin=asyncio.subprocess.PIPE if input is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise WorkspaceRepositoryError("git 不可用") from exc
        stdout_bytes, stderr_bytes = await proc.communicate(
            input.encode("utf-8", "surrogateescape") if input is not None else None
        )
        # stdout 可能被回灌给 git（ls-files 输出进 update-index/checkout）：surrogateescape
        # 保证非 UTF-8 路径字节可往返；stderr 仅供诊断，替换非法字节即可
        stdout = stdout_bytes.decode("utf-8", "surrogateescape")
        stderr = stderr_bytes.decode("utf-8", "replace")
        if proc.returncode != 0:
            raise WorkspaceRepositoryError(
                f"git {' '.join(args)} 失败（rc={proc.returncode}）：{stderr.strip()[-500:]}",
                returncode=proc.returncode,
            )
        return stdout
