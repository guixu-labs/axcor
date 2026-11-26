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

"""节点状态机 + 重试账 + 回滚账（纯内存，无持久化）。

restore 时以 snapshot 载荷整体重建；持久化由宿主经驱动快照边界承接。
graph 状态与节点状态的两层语义都在本模块：节点档位
pending/running/passed/failed/stopped/skipped，graph 档位由调度器按
节点状态推导。
"""

from __future__ import annotations

import contextlib
import copy
import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

# 节点终态集：依赖满足判定的基准（stopped 不在其中——暂停语义，续跑可恢复）
_TERMINAL = {"passed", "failed", "skipped"}

# 节点失败原因持久化时的最大保留字符数（保持源提交的前缀截取行为）
MAX_FAILURE_REASON_CHARS = 5000


def default_node() -> dict[str, Any]:
    """节点状态档案的初始值。"""
    return {
        "status": "pending",
        "attempts": 0,
        "retries": 0,
        "workflow_controller_session_id": None,
        "verify_session_id": None,
        "started_at": None,
        "finished_at": None,
        "failure_reason": None,
        "output": None,
        "gen": 0,
        "subgraph_generated": False,
        "subgraph_node_ids": None,
        "subgraph_nodes": None,
        "subgraph_output": None,
        # 节点作用域工作区映射：恢复后续跑与重试废弃的账面依据
        "workspace_scope_id": None,
        "workspace_scope_epoch": 0,
        "workspace_path": None,
    }


class GraphState:
    """graph 状态 + 逐节点档案 + 回滚账（纯内存，snapshot 即持久化载荷）。"""

    def __init__(
        self,
        graph_id: str,
        workflow: str,
        node_ids: set[str],
        initial_data: dict[str, Any] | None = None,
    ) -> None:
        self._graph_id = graph_id
        self._workflow = workflow
        self._lock = threading.Lock()
        self._checkpoint_hook: Callable[[], None] | None = None
        self._batch_depth = 0
        self._batch_dirty = False
        self._interrupted_nodes: frozenset[str] = frozenset()

        if initial_data is not None:
            self._data = initial_data
            # 档案缺键补默认：快照载荷随版本演进时旧 blob 恢复不丢字段语义
            for nid, info in self._data.get("nodes", {}).items():
                filled = default_node()
                filled.update(info if isinstance(info, dict) else {})
                self._data["nodes"][nid] = filled
            # 中断时的 session 可能仍可续跑；是否复用由恢复后的成员事实决定。
            # 先保留断点，避免在事实对账之前重新出生第二个 session。
            self._interrupted_nodes = frozenset(
                nid for nid, info in self._data["nodes"].items() if info.get("status") == "running"
            )
            for info in self._data["nodes"].values():
                if info.get("status") == "running":
                    info["status"] = "stopped"
            # yaml 变更新增的节点补默认档案
            for nid in node_ids:
                if nid not in self._data["nodes"]:
                    self._data["nodes"][nid] = default_node()
        else:
            self._data = {
                "graph_id": graph_id,
                "status": "pending",
                "nodes": {nid: default_node() for nid in node_ids},
            }

    @property
    def interrupted_nodes(self) -> frozenset[str]:
        return self._interrupted_nodes

    @property
    def checkpoint_bound(self) -> bool:
        """checkpoint 钩子是否在位（轮末提交走钩子还是动作通道的分野）。"""
        return self._checkpoint_hook is not None

    @property
    def graph_id(self) -> str:
        return self._graph_id

    @property
    def workflow(self) -> str:
        return self._workflow

    def bind_checkpoint(self, hook: Callable[[], None] | None) -> None:
        """Run 宿主注入同步持久化钩子；状态对象不持有数据库引用。"""
        self._checkpoint_hook = hook

    @contextlib.contextmanager
    def batch(self) -> Iterator[None]:
        """轮内批量变更：批内的多次 checkpoint 合并为退出时的一次提交。

        decide 一轮会连改多个字段（调度启动 N 节点、消费逐行输出），
        逐字段全量快照会把 decide 延迟放大到超时；退出时同步提交一次，
        「动作派发前落盘」的边界语义不变。批外仍逐次落盘（SIGKILL
        安全性测试要求的语义）。批内异常也提交已发生的变更。
        """
        with self._lock:
            self._batch_depth += 1
        try:
            yield
        finally:
            with self._lock:
                self._batch_depth -= 1
                flush = self._batch_depth == 0 and self._batch_dirty
                self._batch_dirty = False
            if flush:
                self._checkpoint()

    # ── Node status ───────────────────────────────────────────────

    def status(self, node_id: str) -> str:
        return self._data["nodes"][node_id]["status"]

    def attempts(self, node_id: str) -> int:
        return self._data["nodes"][node_id].get("attempts", 0)

    def set_node_status(
        self,
        node_id: str,
        status: str,
        session_id: str | None = None,
    ) -> None:
        with self._lock:
            self._data["nodes"][node_id]["status"] = status
            now = datetime.now(tz=UTC).isoformat()
            if status == "running":
                self._data["nodes"][node_id]["started_at"] = now
                # 重跑节点时清除上一次的失败原因
                self._data["nodes"][node_id]["failure_reason"] = None
            elif status in _TERMINAL or status == "stopped":
                self._data["nodes"][node_id]["finished_at"] = now
            if session_id is not None:
                self._data["nodes"][node_id]["workflow_controller_session_id"] = session_id
        self._checkpoint()

    def set_graph_status(self, status: str) -> None:
        with self._lock:
            self._data["status"] = status
        self._checkpoint()

    def increment_attempts(self, node_id: str) -> None:
        with self._lock:
            info = self._data["nodes"][node_id]
            info["attempts"] = info.get("attempts", 0) + 1
        self._checkpoint()

    def reset_attempts(self, node_id: str) -> None:
        self._set_node_field(node_id, "attempts", 0)

    def retries(self, node_id: str) -> int:
        """当前续跑周期内已消耗的重试次数（重试预算的展示计数无关账单）。"""
        return self._data["nodes"][node_id].get("retries", 0)

    def increment_retries(self, node_id: str) -> None:
        with self._lock:
            info = self._data["nodes"][node_id]
            info["retries"] = info.get("retries", 0) + 1
        self._checkpoint()

    def reset_retries(self, node_id: str) -> None:
        self._set_node_field(node_id, "retries", 0)

    def set_failure_reason(self, node_id: str, reason: str) -> None:
        self._set_node_field(
            node_id,
            "failure_reason",
            reason[:MAX_FAILURE_REASON_CHARS],
        )

    def clear_failure_reason(self, node_id: str) -> None:
        self._set_node_field(node_id, "failure_reason", None)

    def clear_session_id(self, node_id: str) -> None:
        self._set_node_field(node_id, "workflow_controller_session_id", None)

    def node_session_id(self, node_id: str) -> str | None:
        """节点档案的执行会话绑定；未绑定为 None。"""
        return self._data["nodes"][node_id].get("workflow_controller_session_id")

    def set_node_session_id(self, node_id: str, session_id: str) -> None:
        """回填节点执行会话绑定（恢复会话到达时，不改节点状态）。"""
        self._set_node_field(node_id, "workflow_controller_session_id", session_id)

    def set_verify_session_id(self, node_id: str, session_id: str | None) -> None:
        self._set_node_field(node_id, "verify_session_id", session_id)

    def verify_session_id(self, node_id: str) -> str | None:
        return self._data["nodes"][node_id].get("verify_session_id")

    def clear_verify_session_id(self, node_id: str) -> None:
        self._set_node_field(node_id, "verify_session_id", None)

    def set_node_workspace(
        self,
        node_id: str,
        scope_id: str,
        scope_epoch: int,
        workspace_path: str,
    ) -> None:
        """登记节点的作用域工作区映射（建区回执到达时写入）。"""
        with self._lock:
            info = self._data["nodes"][node_id]
            info["workspace_scope_id"] = scope_id
            info["workspace_scope_epoch"] = scope_epoch
            info["workspace_path"] = workspace_path
        self._checkpoint()

    def clear_node_workspace(self, node_id: str) -> None:
        """清节点的作用域映射（旧作用域废弃后不再可用）。"""
        with self._lock:
            info = self._data["nodes"][node_id]
            info["workspace_scope_id"] = None
            info["workspace_scope_epoch"] = 0
            info["workspace_path"] = None
        self._checkpoint()

    def node_workspace(self, node_id: str) -> tuple[str, int, str] | None:
        """节点档案的作用域映射 (scope_id, epoch, path)；未建区为 None。"""
        info = self._data["nodes"][node_id]
        scope_id = info.get("workspace_scope_id")
        if not scope_id:
            return None
        return (
            scope_id,
            int(info.get("workspace_scope_epoch", 0)),
            info.get("workspace_path") or "",
        )

    def clear_output(self, node_id: str) -> None:
        self._set_node_field(node_id, "output", None)

    def subgraph_generated(self, node_id: str) -> bool:
        return bool(self._data["nodes"][node_id].get("subgraph_generated", False))

    def set_subgraph_generated(self, node_id: str, value: bool) -> None:
        self._set_node_field(node_id, "subgraph_generated", value)

    def add_node(self, node_id: str, gen: int = 0) -> None:
        """运行时动态加入子图节点（子图节点按普通 graph node 调度）。

        gen 继承父 generator 当前的 gen：主图回滚到 generator 后 generator 已被
        reset 到新的回滚代，重新生成的子图节点必须同代，否则子图弹窗按父 gen
        过滤会为空、历史记录也会混代。
        """
        with self._lock:
            if node_id not in self._data["nodes"]:
                info = default_node()
                info["gen"] = gen
                self._data["nodes"][node_id] = info
        self._checkpoint()

    def subgraph_node_ids(self, node_id: str) -> list[str]:
        """generator 节点合并的子图节点 id 列表（带前缀）。"""
        return list(self._data["nodes"][node_id].get("subgraph_node_ids") or [])

    def subgraph_nodes(self, node_id: str) -> list[dict]:
        """generator 合并的子图节点拓扑快照（id/depends_on/task_type 等）。

        供前端还原历史子图；归档的 generator 实例（retired_instances）会随
        reset_rollback 一起复制该字段，从而保留各 generation 的子图拓扑。
        """
        return list(self._data["nodes"][node_id].get("subgraph_nodes") or [])

    def set_subgraph_nodes(self, node_id: str, nodes: list[dict]) -> None:
        self._set_node_field(node_id, "subgraph_nodes", nodes)

    def subgraph_parent(self, node_id: str) -> str | None:
        """返回 node_id 所属的 generator 父节点 id（若不是子图节点则 None）。

        用显式归属（subgraph_node_ids）判断，而不是 node id 里的 ':'——主图节点
        id 允许含 ':'（如 prep:db），不能靠分隔符推断子图归属。
        """
        for gid, info in self._data["nodes"].items():
            if node_id in (info.get("subgraph_node_ids") or []):
                return gid
        return None

    def set_subgraph_node_ids(self, node_id: str, node_ids: list[str]) -> None:
        self._set_node_field(node_id, "subgraph_node_ids", node_ids)

    def subgraph_output(self, node_id: str) -> str:
        """generator 实际解析出的子 yaml 相对路径（供恢复复用）。"""
        return self._data["nodes"][node_id].get("subgraph_output") or ""

    def set_subgraph_output(self, node_id: str, output_rel: str) -> None:
        self._set_node_field(node_id, "subgraph_output", output_rel)

    def remove_nodes(self, node_ids: list[str]) -> None:
        """移除动态加入的子图节点（回滚 generator 时废弃旧子图节点）。"""
        with self._lock:
            for nid in node_ids:
                self._data["nodes"].pop(nid, None)
        self._checkpoint()

    def append_output(self, node_id: str, line: str) -> None:
        with self._lock:
            info = self._data["nodes"][node_id]
            info["output"] = ((info.get("output") or "") + line + "\n")[-10000:]
        self._checkpoint()

    # ── Rollback ───────────────────────────────────────────────────

    def gen(self, node_id: str) -> int:
        return self._data["nodes"][node_id].get("gen", 0)

    def rollback_left(self, node_id: str, rollback_to: str, budget: int) -> int:
        key = f"{node_id}:{rollback_to}"
        used = (self._data.get("rollbacks") or {}).get(key, 0)
        return budget - used

    def increment_rollback(self, node_id: str, rollback_to: str) -> None:
        with self._lock:
            key = f"{node_id}:{rollback_to}"
            rollbacks = self._data.setdefault("rollbacks", {})
            rollbacks[key] = rollbacks.get(key, 0) + 1
        self._checkpoint()

    def max_gen(self) -> int:
        """当前所有实例（含 retired）的最大 gen，即全局回滚代。"""
        max_gen = 0
        for info in self._data["nodes"].values():
            max_gen = max(max_gen, info.get("gen", 0))
        for info in self._data.get("retired_instances", {}).values():
            max_gen = max(max_gen, info.get("gen", 0))
        return max_gen

    def reset_rollback(self, node_id: str, new_gen: int) -> int:
        """回滚重置：旧实例归档（供前端继续显示），gen 设为 new_gen（全局回滚代）。
        同一批回滚的节点共用同一个 new_gen，保证「回滚 #N」栏内节点对齐。
        """
        with self._lock:
            info = self._data["nodes"][node_id]
            old_gen = info.get("gen", 0)
            retired = self._data.setdefault("retired_instances", {})
            archived = dict(info)
            archived["id"] = node_id
            retired[f"{node_id}#{old_gen}"] = archived
            info["gen"] = new_gen
            info["status"] = "pending"
            info["attempts"] = 0
            info["retries"] = 0
            info["workflow_controller_session_id"] = None
            info["verify_session_id"] = None
            info["failure_reason"] = None
            info["started_at"] = None
            info["finished_at"] = None
            info["output"] = None
            # 清除 subgraph_generated：回滚的 generator 节点重新生成子 yaml
            # （崩溃恢复不清理，那是 resume 不是 redo）。
            info["subgraph_generated"] = False
        self._checkpoint()
        return new_gen

    def add_rollback_edge(self, from_node: str, from_gen: int, to_node: str, to_gen: int) -> None:
        """记录回滚边（供前端画红线）。"""
        with self._lock:
            edges = self._data.setdefault("rollback_edges", [])
            edges.append(
                {
                    "from": from_node,
                    "from_gen": from_gen,
                    "to": to_node,
                    "to_gen": to_gen,
                }
            )
        self._checkpoint()

    # ── 回滚操作账（异步确认链的状态源，随快照持久化）──────────

    def operations(self) -> dict[str, dict[str, Any]]:
        """全部回滚操作记录（深拷贝；变更经 upsert_operation 回写）。"""
        with self._lock:
            return copy.deepcopy(self._data.get("operations") or {})

    def operation(self, operation_id: str) -> dict[str, Any] | None:
        """按标识取单条操作记录（深拷贝）；不存在返回 None。"""
        record = (self._data.get("operations") or {}).get(operation_id)
        return copy.deepcopy(record) if record is not None else None

    def operation_by_client(self, client_op_id: str) -> dict[str, Any] | None:
        """按客户端幂等键取操作记录（同键冲突请求在受理入口拒绝）。"""
        for record in (self._data.get("operations") or {}).values():
            if record.get("client_op_id") == client_op_id:
                return copy.deepcopy(record)
        return None

    def upsert_operation(self, record: Mapping[str, Any]) -> None:
        """写入/覆盖一条操作记录（按 operation_id 幂等）。"""
        with self._lock:
            self._data.setdefault("operations", {})[str(record["operation_id"])] = dict(record)
        self._checkpoint()

    # ── Queries ───────────────────────────────────────────────────

    def terminal_ids(self) -> set[str]:
        return {nid for nid in self._data["nodes"] if self.status(nid) in _TERMINAL}

    def has_failed(self) -> bool:
        return any(self.status(nid) == "failed" for nid in self._data["nodes"])

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._data)

    def reset_for_new_execution(self) -> None:
        """全新执行的整体重置：节点回初始值，回滚/归档档案清零。

        与「重开续跑」的局部复活不同，这是全新 run 的从头执行——上一轮的
        完成态不能让新 run 零执行空转。已合并的子图节点由调用方先从 DAG
        与 state 一起废弃，单侧残留会让重新合并或调度出错。
        """
        with self._lock:
            self._data = {
                "graph_id": self._graph_id,
                "status": "pending",
                "nodes": {nid: default_node() for nid in self._data["nodes"]},
            }
        self._checkpoint()

    def _set_node_field(self, node_id: str, key: str, value: Any) -> None:
        """单字段变更（锁内置值）。"""
        with self._lock:
            self._data["nodes"][node_id][key] = value
        self._checkpoint()

    def _checkpoint(self) -> None:
        if self._checkpoint_hook is None:
            return
        with self._lock:
            if self._batch_depth > 0:
                self._batch_dirty = True
                return
        self._checkpoint_hook()
