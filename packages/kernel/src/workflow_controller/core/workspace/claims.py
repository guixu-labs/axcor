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

"""claims 软预检：单元出生时的声明写路径登记与相交判定。

声明（``writes`` glob 清单）只在出生期做软预检：强制档下与在册存活
声明相交即拒绝后出生单元；默认只登记，集成期冲突判定兜底。声明与
实际写入不符不在此层拦截。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from workflow_controller.core.workspace.manifest import pathspec_matches

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "WORKSPACE_CLAIM_REJECTED_EVENT",
    "ClaimRejectedError",
    "ClaimsRegistry",
    "claims_overlap",
]

# 出生期 claims 相交拒绝的回执事件类型（run 键事件）
WORKSPACE_CLAIM_REJECTED_EVENT = "workspace.claim_rejected"

# 路径规格中的通配字符：命中即按 glob 解释，否则按字面前缀
_GLOB_CHARS = "*?["


def _literal_stem(pathspec: str) -> str:
    """取规格的字面前缀：首个含通配组件之前的完整路径。"""
    parts = pathspec.strip("/").split("/")
    stem: list[str] = []
    for part in parts:
        if any(char in part for char in _GLOB_CHARS):
            break
        stem.append(part)
    return "/".join(stem)


def claims_overlap(left: str, right: str) -> bool:
    """判定两条声明规格可能覆盖同一路径。

    以对方的字面前缀为本方规格的探测路径做双向匹配：前缀是声明覆盖
    面的骨架，命中即视为相交。该判定对「目录桶与桶内字面」「同桶前后
    缀」精确，对个别双方皆通配的形态保守（宁可误拒，集成判定兜底）。
    """
    if left == right:
        return True
    right_stem = _literal_stem(right)
    if right_stem and pathspec_matches(left, right_stem):
        return True
    left_stem = _literal_stem(left)
    return bool(left_stem and pathspec_matches(right, left_stem))


class ClaimRejectedError(RuntimeError):
    """出生期 claims 相交拒绝：携带相交声明与持有者供事件载荷。"""

    def __init__(
        self,
        scope_key: str,
        claims: tuple[str, ...],
        conflicts: tuple[tuple[str, tuple[str, ...]], ...],
    ) -> None:
        """组装拒绝载荷：被拒键、其声明与各持有者相交的声明。"""
        self.scope_key = scope_key
        self.claims = claims
        self.conflicts = conflicts
        holders = ", ".join(holder for holder, _ in conflicts)
        super().__init__(f"claims 相交拒绝建区：{scope_key} 声明与在册单元（{holders}）相交")


class ClaimsRegistry:
    """按作用域登记声明写路径，供出生期相交判定。"""

    def __init__(self) -> None:
        """空登记表：作用域标识 → 声明清单。"""
        self._claims: dict[str, tuple[str, ...]] = {}

    def register(self, scope_id: str, claims: tuple[str, ...]) -> None:
        """登记（覆盖）作用域声明；空声明不入册。"""
        if claims:
            self._claims[scope_id] = claims

    def release(self, scope_id: str) -> None:
        """移除作用域声明（集成、冲突、废弃或回收后调用）。"""
        self._claims.pop(scope_id, None)

    def conflicts_for(
        self, scope_id: str, claims: tuple[str, ...]
    ) -> tuple[tuple[str, tuple[str, ...]], ...]:
        """求声明与在册存活声明的相交：返回（持有者, 相交规格）清单。

        自身键的既有登记不计（废弃作用域重建场景）。
        """
        conflicts: list[tuple[str, tuple[str, ...]]] = []
        for holder, held in sorted(self._claims.items()):
            if holder == scope_id:
                continue
            overlapped = tuple(
                sorted(
                    claim for claim in claims if any(claims_overlap(claim, other) for other in held)
                )
            )
            if overlapped:
                conflicts.append((holder, overlapped))
        return tuple(conflicts)

    def snapshot(self) -> Mapping[str, tuple[str, ...]]:
        """当前登记视图（测试与审计用）。"""
        return dict(self._claims)
