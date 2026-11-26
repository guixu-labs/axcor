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

"""作用域树账本：写者执行树的代次、状态与引用记录。

核心只认识作用域：集成根、主写者、子代理三类节点构成一棵树。每个作用域
携带两层代次——自身代次（废弃或重建时递增，旧代次回执一律拒绝）与建区
时的集成线代次（集成线回退后，基于旧基线的请求拒绝）；生命周期状态、
闸门状态与不透明编排引用随行记录。本账本只在内存维护，持久化由检查点
账本另行接管。
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = [
    "INITIAL_EPOCH",
    "AbandonedScope",
    "ScopeGateState",
    "ScopeKind",
    "ScopeRecord",
    "ScopeState",
    "ScopeTree",
    "ScopeTreeError",
]

# 起始代次取 1：0 恒为非法值，便于暴露未初始化的调用方
INITIAL_EPOCH = 1


class ScopeTreeError(RuntimeError):
    """账本拒绝：拒绝旧代次请求时携带当前代次，供调用方回传现状。"""

    def __init__(
        self,
        message: str,
        *,
        current_scope_epoch: int | None = None,
        current_root_epoch: int | None = None,
    ) -> None:
        super().__init__(message)
        self.current_scope_epoch = current_scope_epoch
        self.current_root_epoch = current_root_epoch


class ScopeKind(StrEnum):
    """作用域种类：集成根 / 主写者 / 子代理。"""

    ROOT = "root"
    MAIN_WRITER = "main_writer"
    SUBAGENT = "subagent"


class ScopeState(StrEnum):
    """作用域生命周期状态：验收通过与集成完成分离。"""

    PREPARING = "preparing"
    ACTIVE = "active"
    AWAITING_INTEGRATION = "awaiting_integration"
    INTEGRATED = "integrated"
    CONFLICTED = "conflicted"
    DISCARDED = "discarded"
    RELEASED = "released"


class ScopeGateState(StrEnum):
    """账本登记的闸门状态三值子集：静止失败为闸门机内部瞬态，不落账本。"""

    RUNNING = "running"
    QUIESCING = "quiescing"
    QUIESCED = "quiesced"


@dataclass(frozen=True)
class ScopeRecord:
    """作用域账本行：字段与落库列一一对应。"""

    scope_id: str
    parent_scope_id: str | None
    scope_kind: ScopeKind
    owner_ref: str | None
    workspace_ref: str | None
    scope_base_commit: str | None
    integration_base_commit: str | None
    scope_epoch: int
    root_epoch: int
    gate_state: ScopeGateState
    state: ScopeState
    created_at: str


@dataclass(frozen=True)
class AbandonedScope:
    """一次废弃换代的作用域及其新代次。"""

    scope_id: str
    new_scope_epoch: int


# 生命周期合法迁移：废弃换代与重建走专门方法，不经此表
_ALLOWED_STATE_TRANSITIONS: dict[ScopeState, frozenset[ScopeState]] = {
    ScopeState.PREPARING: frozenset({ScopeState.ACTIVE}),
    ScopeState.ACTIVE: frozenset({ScopeState.AWAITING_INTEGRATION}),
    ScopeState.AWAITING_INTEGRATION: frozenset({ScopeState.INTEGRATED, ScopeState.CONFLICTED}),
    ScopeState.INTEGRATED: frozenset({ScopeState.RELEASED}),
    ScopeState.RELEASED: frozenset(),
    ScopeState.CONFLICTED: frozenset(),
    ScopeState.DISCARDED: frozenset(),
}

# 闸门三值合法环：静止中可回运行（延迟分支），静止失败瞬态不经过账本
_ALLOWED_GATE_TRANSITIONS: dict[ScopeGateState, frozenset[ScopeGateState]] = {
    ScopeGateState.RUNNING: frozenset({ScopeGateState.QUIESCING}),
    ScopeGateState.QUIESCING: frozenset({ScopeGateState.QUIESCED, ScopeGateState.RUNNING}),
    ScopeGateState.QUIESCED: frozenset({ScopeGateState.RUNNING}),
}


def _now_iso() -> str:
    """当前时刻的带时区 ISO-8601 字符串。"""
    return datetime.now(UTC).isoformat()


def _require_valid_parenting(parent: ScopeRecord, child_kind: ScopeKind) -> None:
    """父子种类约束：主写者只挂根下，子代理挂主写者或子代理下。"""
    if child_kind is ScopeKind.MAIN_WRITER and parent.scope_kind is not ScopeKind.ROOT:
        raise ScopeTreeError(
            f"主写者作用域只能挂在根作用域下：{parent.scope_id}（{parent.scope_kind.value}）"
        )
    if child_kind is ScopeKind.SUBAGENT and parent.scope_kind is ScopeKind.ROOT:
        raise ScopeTreeError("子代理作用域不能直接挂在根作用域下")


class ScopeTree:
    """一棵作用域树的内存账本。"""

    def __init__(self) -> None:
        """空账本：建立根作用域后成树。"""
        self._scopes: dict[str, ScopeRecord] = {}
        self._children: dict[str, list[str]] = {}
        self._root_scope_id: str | None = None

    @property
    def current_root_epoch(self) -> int:
        """树当前集成线代次。"""
        return self._require_root().root_epoch

    @property
    def root_scope_id(self) -> str | None:
        """根作用域标识；未建根为 None。"""
        return self._root_scope_id

    async def create_root_scope(
        self,
        *,
        scope_id: str,
        owner_ref: str | None = None,
        workspace_ref: str | None = None,
        scope_base_commit: str | None = None,
        integration_base_commit: str | None = None,
    ) -> ScopeRecord:
        """建立集成根作用域：每棵树唯一，根检出即工作区，出生即活动。"""
        self._require_unused_id(scope_id)
        if self._root_scope_id is not None:
            raise ScopeTreeError(f"树已有根作用域：{self._root_scope_id}")
        record = ScopeRecord(
            scope_id=scope_id,
            parent_scope_id=None,
            scope_kind=ScopeKind.ROOT,
            owner_ref=owner_ref,
            workspace_ref=workspace_ref,
            scope_base_commit=scope_base_commit,
            integration_base_commit=integration_base_commit,
            scope_epoch=INITIAL_EPOCH,
            root_epoch=INITIAL_EPOCH,
            gate_state=ScopeGateState.RUNNING,
            state=ScopeState.ACTIVE,
            created_at=_now_iso(),
        )
        self._scopes[scope_id] = record
        self._root_scope_id = scope_id
        return record

    async def load_records(self, records: Sequence[ScopeRecord]) -> None:
        """崩溃恢复的整账装载：替换全部行并重建父子索引。

        只接受恰好一个根作用域的行集；父子引用须闭合（父在集合内或
        为根），否则对不上账、显式失败。
        """
        if self._root_scope_id is not None or self._scopes:
            raise ScopeTreeError("账本非空，不能整账装载")
        roots = [r for r in records if r.scope_kind is ScopeKind.ROOT]
        if len(roots) != 1:
            raise ScopeTreeError(f"整账装载须恰好一个根作用域，实得 {len(roots)}")
        root = roots[0]
        if root.parent_scope_id is not None:
            raise ScopeTreeError(f"根作用域不得携带父引用：{root.scope_id}")
        known = {r.scope_id for r in records}
        children: dict[str, list[str]] = {}
        for record in records:
            self._scopes[record.scope_id] = record
            if record.scope_id == root.scope_id:
                continue
            parent_id = record.parent_scope_id
            if not parent_id or parent_id not in known:
                raise ScopeTreeError(f"作用域的父引用不在集合内：{record.scope_id}（{parent_id}）")
            children.setdefault(parent_id, []).append(record.scope_id)
        self._children = children
        self._root_scope_id = root.scope_id

    async def create_child_scope(
        self,
        *,
        scope_id: str,
        parent_scope_id: str,
        scope_kind: ScopeKind,
        integration_base_commit: str,
        owner_ref: str | None = None,
        workspace_ref: str | None = None,
        scope_base_commit: str | None = None,
    ) -> ScopeRecord:
        """建子作用域：出生为建区中，登记建区时的父集成头并继承树根代次。"""
        self._require_unused_id(scope_id)
        if not isinstance(scope_kind, ScopeKind):
            raise ScopeTreeError("scope_kind 必须是作用域种类枚举")
        if not integration_base_commit:
            raise ScopeTreeError("子作用域必须携带建区时的父集成头")
        if scope_kind is ScopeKind.ROOT:
            raise ScopeTreeError("根作用域须经建根方法建立")
        parent = self._require(parent_scope_id)
        _require_valid_parenting(parent, scope_kind)
        if parent.state is not ScopeState.ACTIVE:
            raise ScopeTreeError(
                f"父作用域非活动态，不能建子：{parent.scope_id}（{parent.state.value}）"
            )
        record = ScopeRecord(
            scope_id=scope_id,
            parent_scope_id=parent.scope_id,
            scope_kind=scope_kind,
            owner_ref=owner_ref,
            workspace_ref=workspace_ref,
            scope_base_commit=scope_base_commit,
            integration_base_commit=integration_base_commit,
            scope_epoch=INITIAL_EPOCH,
            root_epoch=self.current_root_epoch,
            gate_state=ScopeGateState.RUNNING,
            state=ScopeState.PREPARING,
            created_at=_now_iso(),
        )
        self._scopes[scope_id] = record
        self._children.setdefault(parent.scope_id, []).append(scope_id)
        return record

    async def get_scope(self, scope_id: str) -> ScopeRecord | None:
        """查询作用域；不存在返回 None。"""
        return self._scopes.get(scope_id)

    async def list_children(self, scope_id: str) -> list[ScopeRecord]:
        """列出直接子作用域（建序）。"""
        self._require(scope_id)
        return [self._scopes[child_id] for child_id in self._children.get(scope_id, [])]

    async def list_descendants(self, scope_id: str) -> list[ScopeRecord]:
        """深度优先列出全部后代（父先子后），不含自身。"""
        self._require(scope_id)
        collected: list[ScopeRecord] = []
        pending = list(reversed(self._children.get(scope_id, [])))
        while pending:
            current = self._scopes[pending.pop()]
            collected.append(current)
            pending.extend(reversed(self._children.get(current.scope_id, [])))
        return collected

    async def list_all_scopes(self) -> list[ScopeRecord]:
        """根起深度优先列出全部作用域（根在前）；空树返回空。"""
        if self._root_scope_id is None:
            return []
        root_id = self._root_scope_id
        return [self._scopes[root_id], *await self.list_descendants(root_id)]

    async def activate_scope(
        self, scope_id: str, *, workspace_ref: str, scope_base_commit: str
    ) -> ScopeRecord:
        """建区成功转活动：登记工作区引用与文件恢复基线。"""
        self._require_transition(scope_id, ScopeState.ACTIVE)
        return self._mutate(
            scope_id,
            state=ScopeState.ACTIVE,
            workspace_ref=workspace_ref,
            scope_base_commit=scope_base_commit,
        )

    async def set_state(self, scope_id: str, new_state: ScopeState) -> ScopeRecord:
        """按生命周期表迁移状态；废弃换代与重建走专门方法，不经此入口。"""
        if not isinstance(new_state, ScopeState):
            raise ScopeTreeError("new_state 必须是作用域状态枚举")
        self._require_transition(scope_id, new_state)
        return self._mutate(scope_id, state=new_state)

    async def begin_attempt(
        self,
        scope_id: str,
        *,
        workspace_ref: str,
        scope_base_commit: str,
        integration_base_commit: str,
    ) -> ScopeRecord:
        """废弃作用域在新基线重建：回建区中，根代次取当前值（基线重新有效）。"""
        record = self._require(scope_id)
        if record.state is not ScopeState.DISCARDED:
            raise ScopeTreeError(f"只有已废弃作用域能重建：{scope_id}（{record.state.value}）")
        return self._mutate(
            scope_id,
            state=ScopeState.PREPARING,
            workspace_ref=workspace_ref,
            scope_base_commit=scope_base_commit,
            integration_base_commit=integration_base_commit,
            gate_state=ScopeGateState.RUNNING,
            root_epoch=self.current_root_epoch,
        )

    async def abandon_scope(
        self, scope_id: str, *, expected_scope_epoch: int
    ) -> list[AbandonedScope]:
        """废弃作用域及其全部后代：各自换代一次，旧代次回执随之全部失效。"""
        target = await self.require_scope_epoch(scope_id, expected_scope_epoch)
        abandoned = [
            AbandonedScope(scope_id=target.scope_id, new_scope_epoch=target.scope_epoch + 1)
        ]
        for descendant in await self.list_descendants(target.scope_id):
            abandoned.append(
                AbandonedScope(
                    scope_id=descendant.scope_id,
                    new_scope_epoch=descendant.scope_epoch + 1,
                )
            )
        for item in abandoned:
            self._mutate(
                item.scope_id,
                scope_epoch=item.new_scope_epoch,
                state=ScopeState.DISCARDED,
                gate_state=ScopeGateState.RUNNING,
            )
        return abandoned

    async def restore_scope(
        self, scope_id: str, *, expected_scope_epoch: int
    ) -> tuple[AbandonedScope, list[AbandonedScope]]:
        """组检查点恢复的账面步：目标换代但保持活动，后代废弃换代。"""
        target = await self.require_scope_epoch(scope_id, expected_scope_epoch)
        restored = AbandonedScope(scope_id=target.scope_id, new_scope_epoch=target.scope_epoch + 1)
        abandoned: list[AbandonedScope] = []
        for descendant in await self.list_descendants(target.scope_id):
            abandoned.append(
                AbandonedScope(
                    scope_id=descendant.scope_id,
                    new_scope_epoch=descendant.scope_epoch + 1,
                )
            )
        for item in abandoned:
            self._mutate(
                item.scope_id,
                scope_epoch=item.new_scope_epoch,
                state=ScopeState.DISCARDED,
                gate_state=ScopeGateState.RUNNING,
            )
        self._mutate(target.scope_id, scope_epoch=restored.new_scope_epoch)
        return restored, abandoned

    async def advance_root_epoch(self, *, expected_root_epoch: int) -> int:
        """集成线回退时根代次 +1：基于旧根代次的请求此后一律拒绝。"""
        root = self._require_root()
        if expected_root_epoch != root.root_epoch:
            raise ScopeTreeError(
                f"根代次不匹配：expected={expected_root_epoch}",
                current_root_epoch=root.root_epoch,
            )
        new_epoch = root.root_epoch + 1
        self._mutate(root.scope_id, root_epoch=new_epoch)
        return new_epoch

    async def require_scope_epoch(self, scope_id: str, expected_scope_epoch: int) -> ScopeRecord:
        """校验回执或请求携带的作用域代次：旧值拒绝并附当前值。"""
        record = self._require(scope_id)
        if expected_scope_epoch != record.scope_epoch:
            raise ScopeTreeError(
                f"作用域代次不匹配：{scope_id} expected={expected_scope_epoch}",
                current_scope_epoch=record.scope_epoch,
            )
        return record

    async def require_root_epoch(self, expected_root_epoch: int) -> None:
        """校验请求携带的根代次：旧值拒绝并附当前值。"""
        root = self._require_root()
        if expected_root_epoch != root.root_epoch:
            raise ScopeTreeError(
                f"根代次不匹配：expected={expected_root_epoch}",
                current_root_epoch=root.root_epoch,
            )

    async def require_baseline_current(self, scope_id: str) -> None:
        """校验作用域建区基线仍属当前根代次：集成线已回退则拒绝。"""
        record = self._require(scope_id)
        current = self.current_root_epoch
        if record.root_epoch != current:
            raise ScopeTreeError(
                f"作用域基线基于旧根代次：{scope_id}（{record.root_epoch}）",
                current_root_epoch=current,
            )

    async def set_gate_state(self, scope_id: str, gate_state: ScopeGateState) -> ScopeRecord:
        """登记闸门状态：只接受账本三值环，静止失败瞬态不落账本。"""
        if not isinstance(gate_state, ScopeGateState):
            raise ScopeTreeError("gate_state 必须是账本闸门状态枚举")
        record = self._require(scope_id)
        if gate_state not in _ALLOWED_GATE_TRANSITIONS[record.gate_state]:
            raise ScopeTreeError(
                f"非法闸门状态迁移：{record.gate_state.value} → {gate_state.value}（{scope_id}）"
            )
        return self._mutate(scope_id, gate_state=gate_state)

    def _require(self, scope_id: str) -> ScopeRecord:
        """取作用域行；不存在即报错。"""
        record = self._scopes.get(scope_id)
        if record is None:
            raise ScopeTreeError(f"作用域不存在：{scope_id}")
        return record

    def _require_root(self) -> ScopeRecord:
        """取根作用域行；未建根即报错。"""
        if self._root_scope_id is None:
            raise ScopeTreeError("尚未建立根作用域")
        return self._scopes[self._root_scope_id]

    def _require_unused_id(self, scope_id: str) -> None:
        """作用域标识不得复用：新代次共用同一标识。"""
        if not scope_id:
            raise ScopeTreeError("scope_id 不能为空")
        if scope_id in self._scopes:
            raise ScopeTreeError(f"作用域已存在：{scope_id}")

    def _require_transition(self, scope_id: str, new_state: ScopeState) -> ScopeRecord:
        """校验生命周期迁移合法性并返回现值。"""
        record = self._require(scope_id)
        if new_state not in _ALLOWED_STATE_TRANSITIONS[record.state]:
            raise ScopeTreeError(
                f"非法状态迁移：{record.state.value} → {new_state.value}（{scope_id}）"
            )
        return record

    def _mutate(self, scope_id: str, **changes: object) -> ScopeRecord:
        """替换作用域行字段并返回新快照。"""
        updated = replace(self._scopes[scope_id], **changes)
        self._scopes[scope_id] = updated
        return updated
