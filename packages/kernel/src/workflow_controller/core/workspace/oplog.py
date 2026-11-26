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

"""横切操作日志：多步操作的三阶段记账与崩溃恢复判定。

跨工作区、检查点、会话替换的多步操作都经此记账：意图先行（携带精确
旧头、两层代次与操作号），git 提交成功后立即落实际新头（先于一切落账），
全部落账完成才收口。操作号全链路透传并内嵌进提交信息；恢复时按实测头
四分支判定——未生效可重试或放弃、已写只补账、判据核验通过补记新头、
其余一律停止并报告分叉，绝不自动回退。git 的读写都在出口模块完成，本
模块只做记账与判定；账本为单写者设计（协调器是唯一执行手），不加锁。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

__all__ = [
    "OPLOG_FILENAME",
    "CommitFacts",
    "OperationPhase",
    "OplogError",
    "RecoveryAction",
    "RecoveryDecision",
    "SagaOperation",
    "SagaOplog",
    "operation_marker",
]

# 操作日志文件名（住仓树目录内）
OPLOG_FILENAME = "oplog.json"


def operation_marker(client_op_id: str) -> str:
    """提交信息内嵌的操作标识片段：崩溃后判据核验按它唯一匹配。"""
    return f"[client-op:{client_op_id}]"


def _now_iso() -> str:
    """当前时刻的带时区 ISO-8601 字符串。"""
    return datetime.now(UTC).isoformat()


class OplogError(RuntimeError):
    """操作日志非法使用：幂等键冲突或阶段跳跃。"""


class OperationPhase(StrEnum):
    """操作阶段：意图 → 已写 → 已收口，另含未生效即放弃的终态。"""

    PREPARED = "prepared"
    GIT_WRITTEN = "git_written"
    COMMITTED = "committed"
    ABANDONED = "abandoned"


class RecoveryAction(StrEnum):
    """恢复动作：分叉即停，绝不自动回退。"""

    RETRY_SAFE = "retry_safe"
    COMPLETE_LEDGER = "complete_ledger"
    FORK_REPORTED = "fork_reported"


@dataclass(frozen=True)
class CommitFacts:
    """判据核验所需的最小提交事实。"""

    parent: str
    message: str


@dataclass(frozen=True)
class SagaOperation:
    """一条多步操作的记账行。"""

    client_op_id: str
    op_kind: str
    expected_old_head: str
    scope_epoch: int
    root_epoch: int
    metadata: dict[str, object]
    phase: OperationPhase
    actual_new_head: str | None
    result: object
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class RecoveryDecision:
    """一次恢复判定的结论。"""

    action: RecoveryAction
    client_op_id: str
    detail: str
    actual_new_head: str | None = None


class SagaOplog:
    """三阶段操作日志：JSON 文件持久化，重启后加载继续恢复判定。"""

    def __init__(self, tree_dir: Path) -> None:
        """绑定仓树目录并加载既有操作账；文件缺席视为空账本。"""
        self._path = Path(tree_dir) / OPLOG_FILENAME
        self._operations: dict[str, SagaOperation] = {}
        self._load()

    async def begin(
        self,
        *,
        client_op_id: str,
        op_kind: str,
        expected_old_head: str,
        scope_epoch: int,
        root_epoch: int,
        metadata: Mapping[str, object] | None = None,
    ) -> SagaOperation:
        """登记操作意图：同操作号重复登记幂等，参数不一致按冲突拒绝。"""
        self._require_text(client_op_id, "client_op_id")
        self._require_text(op_kind, "op_kind")
        self._require_text(expected_old_head, "expected_old_head")
        normalized_metadata: dict[str, object] = (
            json.loads(json.dumps(dict(metadata), ensure_ascii=False)) if metadata else {}
        )
        existing = self._operations.get(client_op_id)
        if existing is not None:
            parameters_match = (
                existing.op_kind == op_kind
                and existing.expected_old_head == expected_old_head
                and existing.scope_epoch == scope_epoch
                and existing.root_epoch == root_epoch
                and existing.metadata == normalized_metadata
            )
            if not parameters_match:
                raise OplogError(f"操作号重复登记且参数不一致：{client_op_id}")
            return existing
        now = _now_iso()
        operation = SagaOperation(
            client_op_id=client_op_id,
            op_kind=op_kind,
            expected_old_head=expected_old_head,
            scope_epoch=scope_epoch,
            root_epoch=root_epoch,
            metadata=normalized_metadata,
            phase=OperationPhase.PREPARED,
            actual_new_head=None,
            result=None,
            created_at=now,
            updated_at=now,
        )
        self._operations[client_op_id] = operation
        self._persist()
        return operation

    async def record_new_head(self, client_op_id: str, actual_new_head: str) -> SagaOperation:
        """git 提交成功后立即记实际新头（先于一切落账）；同值重记幂等。"""
        operation = self._require_operation(client_op_id)
        self._require_text(actual_new_head, "actual_new_head")
        if operation.phase is OperationPhase.GIT_WRITTEN:
            if operation.actual_new_head != actual_new_head:
                raise OplogError(
                    f"实际新头与已记值不符：{client_op_id}（recorded={operation.actual_new_head}）"
                )
            return operation
        if operation.phase is not OperationPhase.PREPARED:
            raise OplogError(f"该阶段不可记新头：{client_op_id}（{operation.phase.value}）")
        return self._replace_operation(
            operation, phase=OperationPhase.GIT_WRITTEN, actual_new_head=actual_new_head
        )

    async def commit(self, client_op_id: str, *, result: object = None) -> SagaOperation:
        """落账完成后收口：结果随行保存，同操作号重复请求原样返回。"""
        operation = self._require_operation(client_op_id)
        if operation.phase is not OperationPhase.GIT_WRITTEN:
            raise OplogError(f"未记新头不可收口：{client_op_id}（{operation.phase.value}）")
        return self._replace_operation(
            operation,
            phase=OperationPhase.COMMITTED,
            result=json.loads(json.dumps(result, ensure_ascii=False))
            if result is not None
            else None,
        )

    async def abandon(self, client_op_id: str) -> SagaOperation:
        """放弃未生效操作：仅意图阶段（实测头仍为预期旧头）可放弃。"""
        operation = self._require_operation(client_op_id)
        if operation.phase is not OperationPhase.PREPARED:
            raise OplogError(f"已生效操作不可放弃：{client_op_id}（{operation.phase.value}）")
        return self._replace_operation(operation, phase=OperationPhase.ABANDONED)

    async def get(self, client_op_id: str) -> SagaOperation | None:
        """查询操作；不存在返回 None。"""
        return self._operations.get(client_op_id)

    async def pending_operations(self) -> list[SagaOperation]:
        """列出未收口操作（意图或已写阶段），建账序。"""
        return [
            operation
            for operation in self._operations.values()
            if operation.phase in (OperationPhase.PREPARED, OperationPhase.GIT_WRITTEN)
        ]

    def all_operations(self) -> list[SagaOperation]:
        """全部操作行（建账序）；深度恢复的代次推断与对账用。"""
        return list(self._operations.values())

    async def resolve(
        self,
        client_op_id: str,
        *,
        current_head: str,
        read_commit: Callable[[str], Awaitable[CommitFacts | None]],
    ) -> RecoveryDecision:
        """按实测头四分支判定恢复动作；判据核验通过时补记新头继续落账。"""
        operation = self._require_operation(client_op_id)
        self._require_text(current_head, "current_head")
        if operation.phase not in (OperationPhase.PREPARED, OperationPhase.GIT_WRITTEN):
            raise OplogError(f"操作已收口，无需恢复：{client_op_id}")
        if operation.phase is OperationPhase.GIT_WRITTEN:
            if current_head == operation.actual_new_head:
                return RecoveryDecision(
                    action=RecoveryAction.COMPLETE_LEDGER,
                    client_op_id=client_op_id,
                    detail="实测头即已记新头：只补账，不再执行 git 写",
                    actual_new_head=current_head,
                )
            return RecoveryDecision(
                action=RecoveryAction.FORK_REPORTED,
                client_op_id=client_op_id,
                detail=(
                    f"实测头不等于已记新头（head={current_head}，"
                    f"recorded={operation.actual_new_head}）：停止并报告分叉"
                ),
            )
        if current_head == operation.expected_old_head:
            return RecoveryDecision(
                action=RecoveryAction.RETRY_SAFE,
                client_op_id=client_op_id,
                detail="实测头仍为预期旧头：操作未生效，可安全重试或放弃",
            )
        facts = await read_commit(current_head)
        criteria_matched = (
            facts is not None
            and facts.parent == operation.expected_old_head
            and operation_marker(client_op_id) in facts.message
        )
        if not criteria_matched:
            return RecoveryDecision(
                action=RecoveryAction.FORK_REPORTED,
                client_op_id=client_op_id,
                detail="提交后未记新头，且无法唯一确认实测头属于本操作：停止并报告分叉",
            )
        self._replace_operation(
            operation, phase=OperationPhase.GIT_WRITTEN, actual_new_head=current_head
        )
        return RecoveryDecision(
            action=RecoveryAction.COMPLETE_LEDGER,
            client_op_id=client_op_id,
            detail="判据核验唯一匹配：已补记新头，继续落账",
            actual_new_head=current_head,
        )

    def _require_operation(self, client_op_id: str) -> SagaOperation:
        """取操作行；不存在即报错。"""
        operation = self._operations.get(client_op_id)
        if operation is None:
            raise OplogError(f"操作不存在：{client_op_id}")
        return operation

    @staticmethod
    def _require_text(value: str, field_name: str) -> None:
        """必填字符串不得为空。"""
        if not value:
            raise OplogError(f"{field_name} 不能为空")

    def _replace_operation(self, operation: SagaOperation, **changes: object) -> SagaOperation:
        """替换操作行字段、落盘并返回新快照。"""
        updated = replace(operation, updated_at=_now_iso(), **changes)
        self._operations[operation.client_op_id] = updated
        self._persist()
        return updated

    def _persist(self) -> None:
        """整账原子落盘：临时文件写满同步后改名替换。"""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.parent / f"{self._path.name}.tmp"
        payload = {
            "operations": [self._serialize(operation) for operation in self._operations.values()]
        }
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        temp_path.replace(self._path)

    def _load(self) -> None:
        """加载既有操作账。"""
        if not self._path.is_file():
            return
        payload = json.loads(self._path.read_text(encoding="utf-8"))
        for item in payload["operations"]:
            operation = SagaOperation(
                client_op_id=item["client_op_id"],
                op_kind=item["op_kind"],
                expected_old_head=item["expected_old_head"],
                scope_epoch=item["scope_epoch"],
                root_epoch=item["root_epoch"],
                metadata=item.get("metadata", {}),
                phase=OperationPhase(item["phase"]),
                actual_new_head=item.get("actual_new_head"),
                result=item.get("result"),
                created_at=item["created_at"],
                updated_at=item["updated_at"],
            )
            self._operations[operation.client_op_id] = operation

    @staticmethod
    def _serialize(operation: SagaOperation) -> dict[str, object]:
        """操作行转为可 JSON 落盘的字典。"""
        return {
            "client_op_id": operation.client_op_id,
            "op_kind": operation.op_kind,
            "expected_old_head": operation.expected_old_head,
            "scope_epoch": operation.scope_epoch,
            "root_epoch": operation.root_epoch,
            "metadata": operation.metadata,
            "phase": operation.phase.value,
            "actual_new_head": operation.actual_new_head,
            "result": operation.result,
            "created_at": operation.created_at,
            "updated_at": operation.updated_at,
        }
