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
import contextlib
import logging
import os
import re
import shutil
import string
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

logger = logging.getLogger(__name__)

__all__ = [
    "INNER_GIT_RESERVED_PATH",
    "ROOT_INTEGRATION_REF",
    "ROOT_SEED_REF",
    "RootRefs",
    "ScopeWorkspace",
    "WorkspaceRepository",
    "WorkspaceRepositoryError",
    "init_replay_git_sandbox",
    "legacy_snapshot_dir_for",
    "legacy_snapshot_file",
    "legacy_snapshot_list",
    "legacy_snapshot_restore",
    "remove_all_remotes",
    "run_git_sync",
    "run_snapshot_git_sync",
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

# 作用域键进入分支名/目录名时原样保留的字符；其余字符（含 % 自身）按 UTF-8 逐字节 %XX 编码
_SCOPE_KEY_SAFE_CHARS = frozenset(string.ascii_letters + string.digits + "_-")


def _escape_scope_key(scope_key: str) -> str:
    """作用域键转义为合法且互不冲突的分支名/目录名组件。"""
    return "".join(
        char
        if char in _SCOPE_KEY_SAFE_CHARS
        else "".join(f"%{byte:02X}" for byte in char.encode("utf-8", "surrogatepass"))
        for char in scope_key
    )


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

    def workspace_dir_for(self, scope_key: str) -> Path:
        """作用域键的默认工作区目录（目录名取转义键，作用域键原样保留）。"""
        return self.workspaces_dir / _escape_scope_key(scope_key)

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
        workspace_dir: Path | None = None,
    ) -> ScopeWorkspace:
        """建作用域工作区：自 base_commit 检出、按需物化路径后提交建区基线。

        workspace_dir 显式指定落点（默认 ``workspaces/<转义作用域键>``）；
        分支名与默认目录名均取转义作用域键——单会话主写者的固定
        ``root/`` 落点用它保住建区代次命名的分支空间。
        """
        if not scope_key or "/" in scope_key:
            raise WorkspaceRepositoryError(f"非法 scope_key：{scope_key!r}")
        branch = f"{_SCOPE_BRANCH_PREFIX}{_escape_scope_key(scope_key)}"
        workspace_path = (
            workspace_dir if workspace_dir is not None else self.workspace_dir_for(scope_key)
        )
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

    async def create_reader_workspace(
        self,
        scope_key: str,
        *,
        commit: str,
    ) -> Path:
        """建 reader 子代理的 detached worktree 只读快照：无分支、不建基线提交。

        严格模式下 reader 的写入不构成保证：本仓不做只读挂载强制，按
        审计约定处理（越界写由 file_provenance 审计与对账发现）。
        """
        if not scope_key or "/" in scope_key:
            raise WorkspaceRepositoryError(f"非法 scope_key：{scope_key!r}")
        workspace_path = self.workspace_dir_for(scope_key)
        if workspace_path.exists():
            raise WorkspaceRepositoryError(f"作用域工作区已存在：{workspace_path}")
        await self._run_repo_git(
            ["worktree", "add", "--quiet", "--detach", str(workspace_path), commit]
        )
        return workspace_path

    async def reset_workspace_state(self, workspace_path: Path, commit: str) -> None:
        """把工作区文件态硬恢复到目标提交：reset 后清掉全部未跟踪残留。

        clean 带 -x：被忽略路径中未跟踪的构建产物同属检查点之后的
        副作用，一并撤回（可重建路径由重建脚本重新生成）。
        """
        await self._run_workspace_git(workspace_path, ["reset", "--hard", commit])
        await self._run_workspace_git(workspace_path, ["clean", "-fdx", "--quiet"])

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

    async def tree_blob_entries(self, commit: str) -> dict[str, str]:
        """列出一个提交树的全部 blob：路径 → blob 哈希（子模块等非 blob 项跳过）。"""
        out = await self._run_repo_git(["ls-tree", "-r", "-z", commit])
        entries: dict[str, str] = {}
        for record in out.split("\0"):
            if not record:
                continue
            meta, _, path = record.partition("\t")
            fields = meta.split()
            if len(fields) >= 3 and fields[1] == "blob":
                entries[path] = fields[2]
        return entries

    async def blob_size(self, commit: str, path: str) -> int | None:
        """提交内一个文件的大小；路径不在树内返回 None。"""
        try:
            out = await self._run_repo_git(["cat-file", "-s", f"{commit}:{path}"])
        except WorkspaceRepositoryError:
            return None
        try:
            return int(out.strip())
        except ValueError:
            return None

    async def blob_content(self, commit: str, path: str) -> bytes | None:
        """提交内一个文件的内容；路径不在树内返回 None。"""
        try:
            return await self._run_repo_git_bytes(["cat-file", "blob", f"{commit}:{path}"])
        except WorkspaceRepositoryError:
            return None

    async def prune_scope_residue(self, workspace_dir: Path, scope_key: str) -> None:
        """清理无账的半成品建区残留：工作树与同名分支（含代次后缀）一并移除。"""
        if workspace_dir.exists():
            with contextlib.suppress(WorkspaceRepositoryError):
                await self._run_repo_git(["worktree", "remove", "--force", str(workspace_dir)])
        prefix = f"{_SCOPE_BRANCH_PREFIX}{_escape_scope_key(scope_key)}"
        out = await self._run_repo_git(
            ["for-each-ref", "--format=%(refname:short)", f"refs/heads/{prefix}"]
        )
        pattern = re.compile(rf"^{re.escape(prefix)}(-e\d+)?$")
        for branch in (line.strip() for line in out.splitlines()):
            if branch and pattern.match(branch):
                await self._run_repo_git(["branch", "-D", branch])

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

    async def _run_repo_git_bytes(self, args: Sequence[str]) -> bytes:
        """私有仓级 git 调用，返回原始字节（文件内容读取用）。"""
        return await self._run_git_bytes(args, git_dir=self.repo_dir)

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

    async def _run_git_bytes(
        self,
        args: Sequence[str],
        *,
        git_dir: Path | None = None,
    ) -> bytes:
        """执行 git 子进程并返回原始 stdout 字节（内容读取面）。"""
        env = dict(os.environ)
        for var in _SEPARATED_GIT_VARS:
            env.pop(var, None)
        if git_dir is not None:
            env["GIT_DIR"] = str(git_dir)
        env.update(_GIT_IDENTITY_ENV)
        try:
            proc = await asyncio.create_subprocess_exec(
                "git",
                *_GIT_COMMON_ARGS,
                *args,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise WorkspaceRepositoryError("git 不可用") from exc
        stdout_bytes, stderr_bytes = await proc.communicate()
        if proc.returncode != 0:
            stderr = stderr_bytes.decode("utf-8", "replace")
            raise WorkspaceRepositoryError(
                f"git {' '.join(args)} 失败（rc={proc.returncode}）：{stderr.strip()[-500:]}",
                returncode=proc.returncode,
            )
        return stdout_bytes


# ── 快照兼容仓与重放沙箱的同步 git 出口（线程内消费，勿在事件循环直调） ──


def _sync_git_env() -> dict[str, str]:
    """同步 git 调用的环境：剥离分离变量并叠加重放身份。"""
    env = dict(os.environ)
    for var in _SEPARATED_GIT_VARS:
        env.pop(var, None)
    env.update(
        {
            "GIT_AUTHOR_NAME": "workflow-controller-tb",
            "GIT_AUTHOR_EMAIL": "workflow-controller-tb@local",
            "GIT_COMMITTER_NAME": "workflow-controller-tb",
            "GIT_COMMITTER_EMAIL": "workflow-controller-tb@local",
        }
    )
    return env


def run_git_sync(
    cwd: Path,
    args: Sequence[str],
    *,
    timeout: float = 10.0,
) -> subprocess.CompletedProcess[str]:
    """在指定目录同步执行 git（快照兼容仓/重放沙箱的唯一出口，参数不含 git 词）。"""
    return subprocess.run(
        ["git", "-c", "core.hooksPath=", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_sync_git_env(),
    )


def run_snapshot_git_sync(
    cwd: Path,
    args: Sequence[str],
    *,
    timeout: float = 30.0,
) -> subprocess.CompletedProcess[str]:
    """快照仓身份的同步 git 调用（导入重建轨沿用历史快照仓的 commit 作者）。"""
    env = _sync_git_env()
    env.update(
        {
            "GIT_AUTHOR_NAME": "workflow-controller-snapshot",
            "GIT_AUTHOR_EMAIL": "workflow-controller-snapshot@local",
            "GIT_COMMITTER_NAME": "workflow-controller-snapshot",
            "GIT_COMMITTER_EMAIL": "workflow-controller-snapshot@local",
        }
    )
    return subprocess.run(
        ["git", "-c", "core.hooksPath=", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


def remove_all_remotes(git_dir: Path) -> None:
    """剥除指定 ``.git`` 目录的全部 remote（防外连与 token 持久化，best-effort）。"""
    result = subprocess.run(
        ["git", "--git-dir", str(git_dir), "remote"],
        capture_output=True,
        text=True,
        timeout=10,
        env=_sync_git_env(),
    )
    if result.returncode != 0:
        return
    for name in result.stdout.split():
        subprocess.run(
            ["git", "--git-dir", str(git_dir), "remote", "rm", name],
            capture_output=True,
            text=True,
            timeout=10,
            env=_sync_git_env(),
        )


def legacy_snapshot_dir_for(cwd: Path) -> Path:
    """会话原目录对应的历史快照仓目录（兼容读取的寻址约定）。"""
    return Path(cwd) / ".workflow-controller" / "snapshots"


def _strip_rel(path: str) -> str:
    """清理预览路径：去首尾斜杠与 ``./`` 前缀。"""
    return path.strip("/").lstrip("./")


def legacy_snapshot_list(
    snapshot_dir: Path, git_ref: str, subpath: str = ""
) -> list[dict[str, Any]]:
    """列旧检查点文件态直接子项（快照树根为 ``working/``）。

    返回 ``[{"name", "path", "is_dir"}]``（目录在前），对齐既有插件预览口径；
    顶层隐藏内部 git 保留目录。仓或引用缺席返回空。
    """
    if not git_ref or not Path(snapshot_dir).is_dir():
        return []
    sub = _strip_rel(subpath)
    tree_ish = f"{git_ref}:working" if not sub else f"{git_ref}:working/{sub}"
    result = run_git_sync(Path(snapshot_dir), ["ls-tree", tree_ish], timeout=5.0)
    if result.returncode != 0:
        return []
    entries: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        if "\t" not in line:
            continue
        meta, name = line.split("\t", 1)
        fields = meta.split()
        if len(fields) < 2 or not name:
            continue
        if not sub and name == INNER_GIT_RESERVED_PATH:
            continue
        entries.append(
            {
                "name": name,
                "path": f"{sub}/{name}".strip("/") if sub else name,
                "is_dir": fields[1] == "tree",
            }
        )
    entries.sort(key=lambda e: (not e["is_dir"], e["name"]))
    return entries


def legacy_snapshot_file(snapshot_dir: Path, git_ref: str, path: str) -> dict[str, Any] | None:
    """读旧检查点的一个文件内容（>512KB 只报大小不读全量）。"""
    if not git_ref or not Path(snapshot_dir).is_dir():
        return None
    rel = _strip_rel(path)
    if not rel:
        return None
    obj = f"working/{rel}"
    size_result = run_git_sync(
        Path(snapshot_dir), ["cat-file", "-s", f"{git_ref}:{obj}"], timeout=5.0
    )
    size = 0
    if size_result.returncode == 0:
        try:
            size = int(size_result.stdout.strip())
        except ValueError:
            size = 0
    if size > 512 * 1024:
        return {
            "name": rel.rsplit("/", 1)[-1],
            "path": rel,
            "size": size,
            "content": "(file too large to preview)",
            "truncated": True,
        }
    show = subprocess.run(
        ["git", "-c", "core.hooksPath=", "show", f"{git_ref}:{obj}"],
        cwd=str(Path(snapshot_dir)),
        capture_output=True,
        timeout=5.0,
        env=_sync_git_env(),
    )
    if show.returncode != 0:
        return None
    return {
        "name": rel.rsplit("/", 1)[-1],
        "path": rel,
        "size": size,
        "content": show.stdout.decode("utf-8", "replace"),
        "truncated": False,
    }


def _restore_inner_git(dest_dir: Path) -> bool:
    """把保留路径还原为 ``.git``（恢复端专用，失败降级返回 False）。"""
    reserved = dest_dir / INNER_GIT_RESERVED_PATH
    git_dir = dest_dir / ".git"
    if not reserved.is_dir():
        return False
    shutil.rmtree(git_dir, ignore_errors=True)
    try:
        reserved.rename(git_dir)
    except OSError:
        shutil.rmtree(reserved, ignore_errors=True)
        return False
    return True


def legacy_snapshot_restore(snapshot_dir: Path, git_ref: str, dest_dir: Path) -> tuple[bool, bool]:
    """把旧检查点文件态抽到目标目录，返回 ``(是否成功, 是否带内部 git)``。

    ``git archive`` 管道解包 ``working/`` 前缀；空检查点（树不含 working
    条目）按成功恢复空文件态处理（对齐既有快照引擎口径）。
    """
    snapshot_dir = Path(snapshot_dir)
    dest_dir = Path(dest_dir)
    if not snapshot_dir.is_dir() or not git_ref:
        return False, False
    probe = run_git_sync(snapshot_dir, ["ls-tree", git_ref, "--", "working"], timeout=5.0)
    if probe.returncode != 0 or not probe.stdout.strip():
        dest_dir.mkdir(parents=True, exist_ok=True)
        return True, False
    reserved = run_git_sync(
        snapshot_dir,
        ["ls-tree", git_ref, "--", f"working/{INNER_GIT_RESERVED_PATH}"],
        timeout=5.0,
    )
    inner_captured = bool(reserved.returncode == 0 and reserved.stdout.strip())
    dest_dir.mkdir(parents=True, exist_ok=True)
    archive = subprocess.Popen(
        ["git", "archive", "--format=tar", git_ref, "--", "working"],
        cwd=str(snapshot_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=_sync_git_env(),
    )
    extract = subprocess.Popen(
        ["tar", "-x", "--strip-components=1", "-C", str(dest_dir)],
        stdin=archive.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    archive.stdout.close()
    _extract_out, extract_err = extract.communicate()
    archive.wait()
    if extract.returncode != 0:
        logger.warning("快照恢复解包失败：%s", extract_err.decode("utf-8", "replace")[-300:])
        return False, False
    if inner_captured:
        inner_captured = _restore_inner_git(dest_dir)
    return True, inner_captured


def _remove_all_remotes(git_dir: Path) -> None:
    """移除仓内全部 remote 配置段（fork 不连外部 remote，best-effort）。"""
    result = run_git_sync(git_dir.parent, ["remote"], timeout=5.0)
    if result.returncode != 0:
        return
    for name in result.stdout.split():
        run_git_sync(git_dir.parent, ["remote", "remove", name], timeout=5.0)


def _dir_size_mb(path: Path) -> float:
    """目录体积（MB，best-effort）。"""
    total = 0
    for root, _dirs, files in os.walk(str(path)):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                continue
    return total / (1024 * 1024)


def _adopt_restored_inner_git(fork_cwd: Path, inner_git_captured: bool) -> bool:
    """采用恢复出的内部 git（检查点时刻原样）返回 True，否则走降级。"""
    fork_git = fork_cwd / ".git"
    if fork_git.is_file():
        fork_git.unlink(missing_ok=True)
    if not inner_git_captured or not fork_git.is_dir():
        return False
    # sanity 用 rev-parse --git-dir 而非 HEAD：unborn HEAD 是合法状态
    sanity = run_git_sync(fork_cwd, ["rev-parse", "--git-dir"], timeout=10.0)
    if sanity.returncode != 0:
        shutil.rmtree(fork_git, ignore_errors=True)
        return False
    _remove_all_remotes(fork_cwd)
    run_git_sync(fork_cwd, ["config", "--unset", "core.worktree"], timeout=5.0)
    return True


def _copy_origin_git_history(fork_cwd: Path, origin_cwd: str | None, max_mb: int) -> bool:
    """降级分支：体积阈值内复制 origin 当刻 ``.git``；否则返回 False。"""
    if not origin_cwd:
        return False
    src_git = Path(origin_cwd) / ".git"
    if not src_git.is_dir():
        return False
    if max_mb > 0 and _dir_size_mb(src_git) > max_mb:
        return False
    try:
        shutil.copytree(src_git, fork_cwd / ".git", copy_function=shutil.copy)
        return True
    except (OSError, ValueError):
        return False


def init_replay_git_sandbox(
    fork_cwd: Path,
    origin_cwd: str | None,
    *,
    git_history_max_mb: int = 0,
    inner_git_captured: bool = False,
) -> None:
    """在重放目录建可用 git 工作树：恢复内部 git > 复制 origin > git init。

    best-effort：git 不可用或失败不阻塞重放（仅 git 命令报错）。
    """
    if shutil.which("git") is None:
        return
    try:
        if _adopt_restored_inner_git(fork_cwd, inner_git_captured):
            return
        copied = _copy_origin_git_history(fork_cwd, origin_cwd, git_history_max_mb)
        if not copied:
            if run_git_sync(fork_cwd, ["init"], timeout=10.0).returncode != 0:
                return
        else:
            _remove_all_remotes(fork_cwd)
        run_git_sync(fork_cwd, ["add", "-A"], timeout=30.0)
        run_git_sync(
            fork_cwd,
            ["commit", "-m", "workflow-controller traceback sandbox baseline", "--allow-empty"],
            timeout=30.0,
        )
    except (OSError, subprocess.SubprocessError):
        logger.debug("重放 git 沙箱初始化失败（不阻塞重放）", exc_info=True)
