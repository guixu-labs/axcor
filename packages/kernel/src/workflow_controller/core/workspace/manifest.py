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

"""两维路径策略清单：集成策略 × 回滚策略的路径声明与判定。

路径按声明桶分为四类集成策略（进集成线 / 每工作区自建 / 共享只读 /
共享独占），回滚策略三值（随检查点 / 确定性重建 / 不在恢复范围）。
未声明的路径默认「进集成线 + 随检查点」；标记可重建而无确定性脚本
与验收的路径一律按随检查点处理；共享类与外部路径不进检查点、不进
集成线，租约与只读强制不在本版。inherit_from 的声明与运行时绑定类
型也住本模块，第一版只做单上游绑定。

路径规格统一按 glob 语义解释：``*`` 不跨目录、``**`` 跨任意层目录、
字面规格按目录前缀命中其下全部内容；对外输出的规格一律已带 git 魔
法前缀，可直接拼进 git 命令的路径实参。
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "DEFAULT_WRITER_LOCAL_PATHSPECS",
    "CheckpointKind",
    "InheritBinding",
    "InheritDeclaration",
    "IntegrationPolicy",
    "PathPolicy",
    "RebuildScript",
    "RollbackPolicy",
    "WorkspaceManifest",
    "WorkspaceManifestError",
    "pathspec_matches",
]

# 每工作区自建路径的默认声明：构建产物、字节码缓存与日志（任意深度）
DEFAULT_WRITER_LOCAL_PATHSPECS = ("**/build/**", "**/__pycache__/**", "**/*.log")

# 路径规格中的通配字符：命中即按 glob 匹配，否则按字面目录前缀
_GLOB_CHARS = "*?["


class WorkspaceManifestError(RuntimeError):
    """路径策略声明非法：桶重叠、规格缺件或绑定缺件。"""


class IntegrationPolicy(StrEnum):
    """集成策略：路径如何进入（或不进入）集成线。"""

    MERGE = "merge"
    WRITER_LOCAL = "writer_local"
    SHARED_READONLY = "shared_readonly"
    SHARED_EXCLUSIVE = "shared_exclusive"


class RollbackPolicy(StrEnum):
    """回滚策略：路径内容随何种机制恢复。"""

    CHECKPOINTED = "checkpointed"
    REBUILDABLE = "rebuildable"
    EXTERNAL = "external"


class CheckpointKind(StrEnum):
    """继承来源的检查点类型。"""

    SCOPE_START = "scope_start"
    TERMINAL = "terminal"


@dataclass(frozen=True)
class RebuildScript:
    """确定性重建脚本与其验收命令。"""

    cmd: str
    verify: str


@dataclass(frozen=True)
class InheritDeclaration:
    """静态声明层的继承来源：生产者节点与检查点类型，不含运行时身份。"""

    producer_node: str
    checkpoint: CheckpointKind


@dataclass(frozen=True)
class InheritBinding:
    """运行时解析后的继承绑定：核心只接受精确三元组，第一版单上游。"""

    producer_scope_id: str
    producer_scope_epoch: int
    checkpoint_id: str


@dataclass(frozen=True)
class PathPolicy:
    """单条路径的两维策略。"""

    integration: IntegrationPolicy
    rollback: RollbackPolicy


def pathspec_matches(pathspec: str, path: str) -> bool:
    """按 glob 语义匹配路径：字面规格按目录前缀，通配按组件匹配。

    ``**`` 组件匹配零个及以上路径层，``*``/``?``/``[...]`` 只在单组件
    内匹配；无通配的字面规格命中自身与其目录下全部内容。
    """
    if not any(char in pathspec for char in _GLOB_CHARS):
        literal = pathspec.rstrip("/")
        return path == literal or path.startswith(f"{literal}/")
    pattern_parts = pathspec.strip("/").split("/")
    path_parts = path.split("/")
    return _match_components(pattern_parts, path_parts)


def _match_components(pattern_parts: list[str], path_parts: list[str]) -> bool:
    """逐组件匹配：``**`` 吸收零个及以上层，其余按 fnmatch 单层匹配。"""
    if not pattern_parts:
        return not path_parts
    head = pattern_parts[0]
    if head == "**":
        return any(
            _match_components(pattern_parts[1:], path_parts[offset:])
            for offset in range(len(path_parts) + 1)
        )
    if not path_parts:
        return False
    return fnmatch.fnmatchcase(path_parts[0], head) and _match_components(
        pattern_parts[1:], path_parts[1:]
    )


def _git_glob(pathspec: str) -> str:
    """给路径规格加 glob 魔法前缀。"""
    return f":(glob){pathspec}"


def _git_exclude(pathspec: str) -> str:
    """给路径规格加排除 + glob 魔法前缀。"""
    return f":(exclude,glob){pathspec}"


@dataclass(frozen=True)
class WorkspaceManifest:
    """一棵作用域树共用的路径策略清单。"""

    merge: tuple[str, ...] = ()
    writer_local: tuple[str, ...] = DEFAULT_WRITER_LOCAL_PATHSPECS
    shared_readonly: tuple[str, ...] = ()
    shared_exclusive: tuple[str, ...] = ()
    external: tuple[str, ...] = ()
    sparse: tuple[str, ...] = ()
    rebuild_scripts: Mapping[str, RebuildScript] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """声明校验：桶内规格非空、跨桶不重叠、重建脚本完整。

        脚本或验收缺失的重建声明无法确定性重建，按铁律降级为随检
        查点：此类键在构造时剔除，不视为错误。
        """
        buckets = {
            IntegrationPolicy.MERGE: self.merge,
            IntegrationPolicy.WRITER_LOCAL: self.writer_local,
            IntegrationPolicy.SHARED_READONLY: self.shared_readonly,
            IntegrationPolicy.SHARED_EXCLUSIVE: self.shared_exclusive,
            RollbackPolicy.EXTERNAL: self.external,
        }
        seen: dict[str, str] = {}
        for bucket_name, pathspecs in buckets.items():
            for pathspec in pathspecs:
                if not pathspec or not isinstance(pathspec, str):
                    raise WorkspaceManifestError(f"非法路径规格：{pathspec!r}")
                owner = seen.get(pathspec)
                if owner is not None:
                    raise WorkspaceManifestError(
                        f"路径规格同时声明在多个桶：{pathspec!r}（{owner} 与 {bucket_name.value}）"
                    )
                seen[pathspec] = bucket_name.value
        object.__setattr__(
            self,
            "rebuild_scripts",
            {
                pathspec: script
                for pathspec, script in self.rebuild_scripts.items()
                if pathspec and script.cmd and script.verify
            },
        )

    @property
    def merge_pathspecs(self) -> tuple[str, ...]:
        """显式声明进集成线的路径规格（未声明路径同样默认进线）。"""
        return self.merge

    @property
    def writer_local_pathspecs(self) -> tuple[str, ...]:
        """全部每工作区自建路径规格（含可重建与随检查点两类）。"""
        return self.writer_local

    @property
    def rebuildable_pathspecs(self) -> tuple[str, ...]:
        """给出确定性脚本的自建路径：建区留空、作用域提交排除。"""
        return tuple(self.rebuild_scripts)

    def git_writer_local_specs(self) -> tuple[str, ...]:
        """自建路径的 git 正向规格：根基线剥除与建区物化共用。"""
        return tuple(_git_glob(pathspec) for pathspec in self.writer_local)

    def git_materialize_specs(self) -> tuple[str, ...]:
        """首层建区的物化规格：自建路径中排除可重建部分。"""
        positives = self.git_writer_local_specs()
        excludes = tuple(_git_exclude(pathspec) for pathspec in self.rebuild_scripts)
        return (*positives, *excludes)

    def git_scope_commit_exclude_specs(self) -> tuple[str, ...]:
        """作用域分支提交须排除的 git 规格：不进检查点的一切路径。"""
        return tuple(
            _git_exclude(pathspec)
            for pathspec in (
                *self.rebuildable_pathspecs,
                *self.shared_readonly,
                *self.shared_exclusive,
                *self.external,
            )
        )

    def git_line_commit_exclude_specs(self) -> tuple[str, ...]:
        """根检出状态提交须排除的 git 规格：不进集成线的一切路径。"""
        return tuple(
            _git_exclude(pathspec)
            for pathspec in (
                *self.writer_local,
                *self.shared_readonly,
                *self.shared_exclusive,
                *self.external,
            )
        )

    def git_diff_restriction_specs(self) -> tuple[str, ...]:
        """集成 diff 的 git 限定规格：除自建与共享类路径外全部纳入。"""
        return (".", *self.git_line_commit_exclude_specs())

    def classify_path(self, path: str) -> PathPolicy:
        """判定单条路径的两维策略。

        优先级为外部 > 共享独占 > 共享只读 > 自建（可重建优先于随检
        查点）> 默认进线；路径同时命中多层声明时按此序取先，声明桶本
        身的字符串级互斥见构造校验。
        """
        for pathspec in self.external:
            if pathspec_matches(pathspec, path):
                return PathPolicy(IntegrationPolicy.SHARED_READONLY, RollbackPolicy.EXTERNAL)
        for pathspec in self.shared_exclusive:
            if pathspec_matches(pathspec, path):
                return PathPolicy(IntegrationPolicy.SHARED_EXCLUSIVE, RollbackPolicy.EXTERNAL)
        for pathspec in self.shared_readonly:
            if pathspec_matches(pathspec, path):
                return PathPolicy(IntegrationPolicy.SHARED_READONLY, RollbackPolicy.EXTERNAL)
        for pathspec in self.writer_local:
            if pathspec_matches(pathspec, path):
                rollback = (
                    RollbackPolicy.REBUILDABLE
                    if any(pathspec_matches(key, path) for key in self.rebuild_scripts)
                    else RollbackPolicy.CHECKPOINTED
                )
                return PathPolicy(IntegrationPolicy.WRITER_LOCAL, rollback)
        for script_key in self.rebuild_scripts:
            if pathspec_matches(script_key, path):
                return PathPolicy(IntegrationPolicy.WRITER_LOCAL, RollbackPolicy.REBUILDABLE)
        return PathPolicy(IntegrationPolicy.MERGE, RollbackPolicy.CHECKPOINTED)
