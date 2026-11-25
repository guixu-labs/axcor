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

"""graph 方言导出贡献者：三源事实 → workflow bundle 的成员与关系字段。

三源对账（权威序：node_history 事件 > 驱动快照 > 冻结程序）：事件给
attempt 级事实（session/验收/时间/输出），快照给当前代档案与回滚账
（retired/rollback_edges/子图拓扑），冻结程序给静态定义（依赖边/
acceptance/run_on_failure）。成员判定纯以实例存在性为据：代集内每代
每个已知节点都有成员（实例逐 attempt / carried / 占位）。
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from workflow_controller.plugin.run_export import (
    RunExportContribution,
    WorkflowMemberContribution,
    WorkflowRelationContribution,
    synthetic_member_id,
)
from workflow_controller.session.events import COMPLETED_STOP_REASONS, FAILED_STOP_REASONS

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.plugin.run_export import RunExportRequest

logger = logging.getLogger(__name__)

_KIND_RANK = {"session": 0, "process": 1, "workflow": 2}
_REL_RANK = {"depends_on": 0, "rollback": 1, "gen-sub-graph": 2}
_TERMINAL_STATUSES = frozenset({"passed", "failed", "skipped"})
_ENTRY_MARKER = ".workflow-controller-entry"

_SortedMembers = list[tuple[tuple, WorkflowMemberContribution]]
_SortedRelations = list[tuple[tuple, WorkflowRelationContribution]]


class _NodeAttempts:
    """单节点单代的 attempt 事实（事件流权威，快照档案补缺）。"""

    def __init__(self) -> None:
        self.facts: dict[int, dict[str, Any]] = {}

    def note_event(self, data: Mapping[str, Any]) -> None:
        attempt = int(data.get("attempt") or 0)
        if attempt <= 0:
            return
        fact = self.facts.setdefault(attempt, {})
        if data.get("workflow_controller_session_id"):
            fact["workflow_controller_session_id"] = data["workflow_controller_session_id"]
        for key in ("started_at", "finished_at", "output"):
            if data.get(key) is not None:
                fact[key] = data[key]
        if data.get("status") is not None:
            fact["status"] = data["status"]
        verify = {
            key: data.get(key)
            for key in ("verify_type", "verify_session_id", "verify_reason")
            if data.get(key) is not None
        }
        if verify:
            # 逐事件合并：结论事件可能只带 sid/reason（type 为空），
            # 覆盖式写入会丢掉起步事件给的验收方式
            fact["verify"] = {**fact.get("verify", {}), **verify}

    def note_record(self, record: Mapping[str, Any]) -> None:
        """快照档案只补事件没给的当前 attempt 事实。"""
        attempt = int(record.get("attempts") or 0)
        if attempt <= 0:
            return
        fact = self.facts.setdefault(attempt, {})
        for key in ("started_at", "finished_at", "output"):
            if fact.get(key) is None and record.get(key) is not None:
                fact[key] = record[key]
        if fact.get("workflow_controller_session_id") is None and record.get(
            "workflow_controller_session_id"
        ):
            fact["workflow_controller_session_id"] = record["workflow_controller_session_id"]
        status = record.get("status")
        # 与前端 projectionToRunIndex 同序：快照的 running/stopped 可覆盖事件的
        # 非终态状态（暂停/停止的尝试在导出件中也显示为已停止），终态结论优先。
        active_state = status in ("running", "stopped")
        status_missing = fact.get("status") is None
        status_overridable = active_state and fact.get("status") not in _TERMINAL_STATUSES
        if status is not None and (status_missing or status_overridable):
            fact["status"] = status
        if fact.get("verify") is None and record.get("verify_session_id"):
            fact["verify"] = {
                "verify_type": "agent",
                "verify_session_id": record["verify_session_id"],
            }


def build_contribution(request: RunExportRequest) -> RunExportContribution:
    """组装 graph 方言贡献（纯函数；读冻结程序是唯一文件访问）。"""
    return _ContributionBuilder(request).build()


class _ContributionBuilder:
    """贡献构建序：全量成员 → DAG/回滚/子图关系 → 排序装配。"""

    def __init__(self, request: RunExportRequest) -> None:
        self.request = request
        self.run_id = request.run.run_id
        state = (request.driver_snapshot or {}).get("state") or {}
        self.workflow_name = (request.driver_snapshot or {}).get("workflow")
        self.records: dict[tuple[str, int], dict[str, Any]] = {}
        for nid, info in (state.get("nodes") or {}).items():
            self.records[(nid, int(info.get("gen") or 0))] = dict(info)
        for key, info in (state.get("retired_instances") or {}).items():
            nid = info.get("id") or key.split("#")[0]
            self.records[(nid, int(info.get("gen") or 0))] = dict(info)
        self.rollback_edges = [dict(e) for e in (state.get("rollback_edges") or [])]
        self.attempts: dict[tuple[str, int], _NodeAttempts] = {}
        self.session_outcomes: dict[str, tuple[str, str]] = {}
        for ev in request.events:
            if ev.get("source") == "events" and ev.get("event_type") == "session_outcome":
                sid = ev.get("session_id")
                kind = (ev.get("data") or {}).get("kind")
                if sid and kind:
                    self.session_outcomes[sid] = (kind, ev["timestamp"])
            if ev.get("source") != "run_events" or ev.get("event_type") != "node_history":
                continue
            data = ev.get("data") or {}
            if data.get("node_id"):
                self._node_attempts(
                    data["node_id"],
                    int(data.get("gen") or 0),
                ).note_event(data)
        for (nid, gen), record in self.records.items():
            self._node_attempts(nid, gen).note_record(record)
        self.main_nodes, self.main_edges = _load_program(request.program_dir)
        self.generators = {
            nid for nid, node in self.main_nodes.items() if node.get("task_type") == "generator"
        }
        self.subgraph_nodes = {
            nid for nid in self.generators if self.main_nodes[nid].get("_xlsx_subgraph")
        }
        # 子图拓扑：generator → {gen → 拓扑快照}；子节点首代已知代
        self.sub_topos: dict[str, dict[int, list[dict[str, Any]]]] = {}
        self.sub_known_from: dict[str, dict[str, int]] = {}
        for (nid, gen), record in self.records.items():
            if nid not in self.generators:
                continue
            if record.get("subgraph_nodes"):
                self.sub_topos.setdefault(nid, {})[gen] = list(record["subgraph_nodes"])
            known = self.sub_known_from.setdefault(nid, {})
            for sub_id in record.get("subgraph_node_ids") or []:
                known[sub_id] = min(known.get(sub_id, gen), gen)
        self.gens = self._generation_set()
        self.member_ids: dict[tuple[str, int, int, str], str] = {}
        self.dynamic_workflow_ids: dict[tuple[str, int], str] = {}

    # ── 基础查询 ────────────────────────────────────────────────

    @staticmethod
    def _execution_state(status: Any) -> str:
        return "finished" if status == "stopped" or status in _TERMINAL_STATUSES else "in_progress"

    @staticmethod
    def _depends_on_relation(
        from_id: str,
        to_id: str,
    ) -> tuple[tuple[int, str, str], WorkflowRelationContribution]:
        """depends_on 关系条目（排序键 + 贡献体）。"""
        return (
            (_REL_RANK["depends_on"], from_id, to_id),
            WorkflowRelationContribution(
                from_id=from_id,
                to_id=to_id,
                fields={"type": "depends_on"},
            ),
        )

    @staticmethod
    def _assemble_nested(
        sub_members: dict[str, _SortedMembers],
        sub_relations: dict[str, _SortedRelations],
        top_members: _SortedMembers,
    ) -> dict[str, RunExportContribution]:
        """嵌套子图工作流（按容器成员 id 归拢成员与关系）。"""
        nested: dict[str, RunExportContribution] = {}
        for _, member in top_members:
            if member.kind != "workflow":
                continue
            wf_id = member.id
            nested[wf_id] = RunExportContribution(
                members=tuple(m for _, m in sorted(sub_members.get(wf_id, []))),
                relations=tuple(r for _, r in sorted(sub_relations.get(wf_id, []))),
            )
        return nested

    def build(self) -> RunExportContribution:
        top_members: _SortedMembers = []
        top_relations: _SortedRelations = []
        sub_members: dict[str, _SortedMembers] = {}
        sub_relations: dict[str, _SortedRelations] = {}
        child_relations: dict[str, _SortedRelations] = {}
        child_generations: dict[str, int] = {}
        self._collect_node_members(
            top_members,
            top_relations,
            sub_members,
            child_relations,
            child_generations,
        )
        self._append_dynamic_workflows(top_members, top_relations)
        scoped_relations = {
            None: top_relations,
            **{
                generator: child_relations.setdefault(generator, [])
                for generator in self.sub_known_from
            },
        }
        self._build_dag_edges(scoped_relations)
        self._build_rollback_edges(top_relations)
        self._bind_child_relations(
            child_relations,
            child_generations,
            sub_relations,
            top_members,
        )
        nested = self._assemble_nested(sub_members, sub_relations, top_members)
        top_members.sort(key=lambda item: item[0])
        top_relations.sort(key=lambda item: item[0])
        self._decorate_retry_policies(
            [m for _, m in top_members] + [m for sub in nested.values() for m in sub.members],
        )
        return RunExportContribution(
            name=self.workflow_name,
            members=tuple(m for _, m in top_members),
            relations=tuple(r for _, r in top_relations),
            nested=nested,
        )

    def _collect_node_members(
        self,
        top_members: _SortedMembers,
        top_relations: _SortedRelations,
        sub_members: dict[str, _SortedMembers],
        child_relations: dict[str, _SortedRelations],
        child_generations: dict[str, int],
    ) -> None:
        """全量节点成员：主图直挂顶层，子图挂到所在代的工作流容器。"""
        for nid in sorted(self._all_known_nodes()):
            generator = self._parent_generator(nid)
            if generator:
                continue
            self._build_node_members(
                nid,
                top_members,
                top_relations,
            )
        for nid in sorted(self._all_known_nodes()):
            generator = self._parent_generator(nid)
            if not generator:
                continue
            members: _SortedMembers = []
            relations: _SortedRelations = []
            self._build_node_members(nid, members, relations)
            for item in members:
                member = item[1]
                gen = member.fields["generation"]
                wf_id = self._subgraph_workflow_id(generator, gen, top_members)
                sub_members.setdefault(wf_id, []).append(item)
                child_generations[member.id] = gen
            child_relations.setdefault(generator, []).extend(relations)

    def _append_dynamic_workflows(
        self,
        top_members: _SortedMembers,
        top_relations: _SortedRelations,
    ) -> None:
        """动态子图容器成员与其 gen-sub-graph 关系挂顶层。"""
        for (generator, gen), wf_id in sorted(self.dynamic_workflow_ids.items()):
            top_members.append(
                (
                    (_KIND_RANK["workflow"], generator, gen, 0, 0, wf_id),
                    WorkflowMemberContribution(
                        id=wf_id,
                        kind="workflow",
                        name=f"{generator}：第 {gen} 代生成的子图",
                    ),
                )
            )
            source_id = self._downstream_id(generator, gen)
            if source_id:
                top_relations.append(
                    (
                        (_REL_RANK["gen-sub-graph"], source_id, wf_id),
                        WorkflowRelationContribution(
                            from_id=source_id,
                            to_id=wf_id,
                            fields={"type": "gen-sub-graph"},
                        ),
                    )
                )

    def _bind_child_relations(
        self,
        child_relations: dict[str, _SortedRelations],
        child_generations: dict[str, int],
        sub_relations: dict[str, _SortedRelations],
        top_members: _SortedMembers,
    ) -> None:
        """子图内部关系按代分拣进对应的嵌套工作流。"""
        for generator, relations in child_relations.items():
            for item in relations:
                relation = item[1]
                gen = child_generations.get(relation.from_id)
                if gen is None:
                    logger.warning(
                        "子图关系来源实例不在子图作用域内，跳过该边：%s → %s",
                        relation.from_id,
                        relation.to_id,
                    )
                    continue
                if child_generations.get(relation.to_id) != gen:
                    raise ValueError(f"子图关系跨代：{relation.from_id} → {relation.to_id}")
                wf_id = self._subgraph_workflow_id(generator, gen, top_members)
                sub_relations.setdefault(wf_id, []).append(item)

    def _node_attempts(self, nid: str, gen: int) -> _NodeAttempts:
        if (nid, gen) not in self.attempts:
            self.attempts[(nid, gen)] = _NodeAttempts()
        return self.attempts[(nid, gen)]

    def _generation_set(self) -> set[int]:
        gens = {0} | {to_gen for (_, to_gen) in self.records}
        gens |= {
            to_gen for (nid, to_gen), attempts in self.attempts.items() if nid and attempts.facts
        }
        gens |= {int(e.get("to_gen") or 0) for e in self.rollback_edges}
        return gens

    def _instance_gens(self, nid: str) -> list[int]:
        return sorted(
            gen for (n, gen), attempts in self.attempts.items() if n == nid and attempts.facts
        )

    def _attempt_numbers(self, nid: str, gen: int) -> list[int]:
        node = self.attempts.get((nid, gen))
        return sorted(node.facts) if node else []

    def _known_from(self, nid: str) -> int | None:
        """节点首代已知代：主图节点恒 0，子图节点按生成代。"""
        if nid in self.main_nodes:
            return 0
        for known in self.sub_known_from.values():
            if nid in known:
                return known[nid]
        return None

    # ── 成员派生 ────────────────────────────────────────────────

    def _parent_generator(self, nid: str) -> str | None:
        for generator, known in self.sub_known_from.items():
            if nid in known:
                return generator
        return None

    def _is_bash_node(self, nid: str) -> bool:
        node = self.main_nodes.get(nid) or {}
        if node:
            task_type = node.get("task_type")
            if task_type == "generator":
                return node.get("generator_type", "agent") == "bash"
            return task_type == "bash"
        # 定义缺席（程序漂移/归档代子节点）：以首事实有无会话判定
        for gen in self._instance_gens(nid):
            for attempt in self._attempt_numbers(nid, gen):
                if self.attempts[(nid, gen)].facts[attempt].get("workflow_controller_session_id"):
                    return False
        return True

    def _all_known_nodes(self) -> set[str]:
        nodes = set(self.main_nodes)
        for (nid, _), attempts in self.attempts.items():
            if nid and attempts.facts:
                nodes.add(nid)
        for known in self.sub_known_from.values():
            nodes.update(known)
        return nodes

    def _build_node_members(
        self,
        nid: str,
        members: _SortedMembers,
        relations: _SortedRelations,
    ) -> None:
        """单节点跨代成员（实例逐 attempt / carried / 占位）与同节点验收边。"""
        known_from = self._known_from(nid) or 0
        instance_gens = self._instance_gens(nid)
        for gen in sorted(g for g in self.gens if g >= known_from):
            attempt_numbers = self._attempt_numbers(nid, gen)
            if not attempt_numbers:
                self._emit_vacant_member(nid, gen, instance_gens, members)
                continue
            for attempt in attempt_numbers:
                fact = self.attempts[(nid, gen)].facts[attempt]
                exec_id = self._emit_execute_member(nid, gen, attempt, fact, members)
                verify = fact.get("verify")
                if isinstance(verify, Mapping) and (
                    verify.get("verify_type") or verify.get("verify_session_id")
                ):
                    verify_id = self._emit_verify_member(
                        nid,
                        gen,
                        attempt,
                        fact,
                        verify,
                        members,
                    )
                    relations.append(
                        (
                            (_REL_RANK["depends_on"], exec_id, verify_id),
                            WorkflowRelationContribution(
                                from_id=exec_id,
                                to_id=verify_id,
                                fields={"type": "depends_on"},
                            ),
                        )
                    )

    def _emit_execute_member(
        self,
        nid: str,
        gen: int,
        attempt: int,
        fact: Mapping[str, Any],
        members: _SortedMembers,
    ) -> str:
        status = fact.get("status")
        fields: dict[str, Any] = {
            "nodeId": nid,
            "generation": gen,
            "attempt": attempt,
            "executionState": self._execution_state(status),
            "nodeStatus": status,
        }
        if status == "passed":
            fields["attemptResult"] = "succeeded"
        elif status == "failed":
            fields["attemptResult"] = "failed"
        sid = fact.get("workflow_controller_session_id")
        if nid in self.subgraph_nodes:
            member_id, member = self._execute_local_member(
                nid, gen, attempt, fact, fields, "workflow", None
            )
        elif self._is_bash_node(nid):
            member_id, member = self._execute_local_member(
                nid,
                gen,
                attempt,
                fact,
                fields,
                "process",
                self._bash_output(nid, gen, attempt, fact),
            )
        else:
            member_id, member = self._execute_session_member(nid, gen, attempt, sid, status, fields)
        self.member_ids[(nid, gen, attempt, "execute")] = member_id
        members.append(
            (
                (_KIND_RANK[member.kind], nid, gen, attempt, 0, member_id),
                member,
            )
        )
        return member_id

    def _execute_local_member(
        self,
        nid: str,
        gen: int,
        attempt: int,
        fact: Mapping[str, Any],
        fields: dict[str, Any],
        kind: str,
        output_text: str | None,
    ) -> tuple[str, WorkflowMemberContribution]:
        """执行成员的非 session 形态（子图容器 / bash 进程）。"""
        member_id = synthetic_member_id(self.run_id, nid, gen, attempt, "execute")
        member = WorkflowMemberContribution(
            id=member_id,
            kind=kind,
            name=f"{nid}：第 {gen} 代第 {attempt} 次执行",
            started_at=fact.get("started_at"),
            finished_at=fact.get("finished_at"),
            output_text=output_text,
            annotation={"role": "execute"},
            fields=fields,
        )
        return member_id, member

    def _execute_session_member(
        self,
        nid: str,
        gen: int,
        attempt: int,
        sid: str | None,
        status: Any,
        fields: dict[str, Any],
    ) -> tuple[str, WorkflowMemberContribution]:
        """执行成员的 session 形态（agent 执行器）。"""
        member_id = (
            f"s_{sid}"
            if sid
            else synthetic_member_id(
                self.run_id,
                nid,
                gen,
                attempt,
                "execute",
            )
        )
        if sid:
            fields["executionState"] = self._session_execution_state(sid, status)
        row = self.request.roster.get(sid) or {}
        member = WorkflowMemberContribution(
            id=member_id,
            kind="session",
            name=f"{nid}：第 {gen} 代第 {attempt} 次执行",
            started_at=row.get("created_at"),
            finished_at=(
                self._session_finished_at(sid)
                or (row.get("last_activity") if status in _TERMINAL_STATUSES else None)
            ),
            session_id=sid,
            annotation={"role": "execute"},
            fields=fields,
        )
        return member_id, member

    def _emit_verify_member(
        self,
        nid: str,
        gen: int,
        attempt: int,
        fact: Mapping[str, Any],
        verify: Mapping[str, Any],
        members: _SortedMembers,
    ) -> str:
        status = fact.get("status")
        verdict = (
            "passed"
            if status == "passed"
            else "failed"
            if status == "failed"
            else "stopped"
            if status == "stopped"
            else "running"
        )
        verification: dict[str, Any] = {
            "type": verify.get("verify_type") or "agent",
            "status": verdict,
        }
        if status in ("passed", "failed") and verify.get("verify_reason"):
            verification["reason"] = verify["verify_reason"]
        fields: dict[str, Any] = {
            "nodeId": nid,
            "generation": gen,
            "attempt": attempt,
            "executionState": self._execution_state(status),
            "verification": verification,
        }
        member_id, member = self._verify_member_shape(nid, gen, attempt, status, verify, fields)
        self.member_ids[(nid, gen, attempt, "verify")] = member_id
        members.append(
            (
                (_KIND_RANK[member.kind], nid, gen, attempt, 1, member_id),
                member,
            )
        )
        return member_id

    def _verify_member_shape(
        self,
        nid: str,
        gen: int,
        attempt: int,
        status: Any,
        verify: Mapping[str, Any],
        fields: dict[str, Any],
    ) -> tuple[str, WorkflowMemberContribution]:
        """验收成员形态：有 sid 是 session 成员，否则脚本进程。"""
        verify_sid = verify.get("verify_session_id")
        if verify_sid:
            member_id = f"s_{verify_sid}"
            verify_row = self.request.roster.get(verify_sid) or {}
            fields["executionState"] = self._session_execution_state(verify_sid, status)
            member = WorkflowMemberContribution(
                id=member_id,
                kind="session",
                name=f"{nid}：第 {gen} 代第 {attempt} 次验收",
                started_at=verify_row.get("created_at"),
                finished_at=(
                    self._session_finished_at(verify_sid)
                    or (verify_row.get("last_activity") if status in _TERMINAL_STATUSES else None)
                ),
                session_id=verify_sid,
                annotation={"role": "verify"},
                fields=fields,
            )
        else:
            member_id = synthetic_member_id(self.run_id, nid, gen, attempt, "verify")
            member = WorkflowMemberContribution(
                id=member_id,
                kind="process",
                name=f"{nid}：第 {gen} 代第 {attempt} 次验收",
                annotation={"role": "verify"},
                fields=fields,
            )
        return member_id, member

    def _emit_vacant_member(
        self,
        nid: str,
        gen: int,
        instance_gens: list[int],
        members: _SortedMembers,
    ) -> None:
        """代内无实例：最近实例已终态成 carried，否则未运行占位。"""
        earlier = [g for g in instance_gens if g < gen]
        if earlier:
            self._emit_carried_member(nid, gen, earlier[-1], members)
            return
        kind = (
            "workflow"
            if nid in self.subgraph_nodes
            else "process"
            if self._is_bash_node(nid)
            else "session"
        )
        member, member_id = self._pending_placeholder(nid, gen, kind)
        members.append(((_KIND_RANK[kind], nid, gen, 0, 0, member_id), member))

    def _emit_carried_member(
        self,
        nid: str,
        gen: int,
        source_gen: int,
        members: _SortedMembers,
    ) -> None:
        """沿用最近已终态代的执行结果（carried 成员）。"""
        record = self.records.get((nid, source_gen), {})
        node = self.attempts.get((nid, source_gen))
        last_fact = node.facts[max(node.facts)] if node and node.facts else {}
        status = last_fact.get("status") or record.get("status")
        if status not in _TERMINAL_STATUSES:
            return
        member_id = synthetic_member_id(self.run_id, nid, gen, 0, "execute")
        self.member_ids[(nid, gen, 0, "execute")] = member_id
        members.append(
            (
                (
                    _KIND_RANK["workflow" if nid in self.subgraph_nodes else "process"],
                    nid,
                    gen,
                    0,
                    0,
                    member_id,
                ),
                WorkflowMemberContribution(
                    id=member_id,
                    kind="workflow" if nid in self.subgraph_nodes else "process",
                    name=f"{nid}：第 {gen} 代未执行（沿用第 {source_gen} 代结果）",
                    annotation={"role": "execute"},
                    fields={
                        "nodeId": nid,
                        "generation": gen,
                        "attempt": 0,
                        "executionState": "carried",
                        "resultFromGeneration": source_gen,
                    },
                ),
            )
        )

    def _pending_placeholder(
        self,
        nid: str,
        gen: int,
        kind: str,
    ) -> tuple[WorkflowMemberContribution, str]:
        """待执行占位成员（attempt 0，executionState pending）。"""
        member_id = synthetic_member_id(self.run_id, nid, gen, 0, "execute")
        self.member_ids[(nid, gen, 0, "execute")] = member_id
        member = WorkflowMemberContribution(
            id=member_id,
            kind=kind,
            name=f"{nid}：第 {gen} 代待执行",
            annotation={"role": "execute"},
            fields={
                "nodeId": nid,
                "generation": gen,
                "attempt": 0,
                "executionState": "pending",
            },
        )
        return member, member_id

    def _bash_output(
        self,
        nid: str,
        gen: int,
        attempt: int,
        fact: Mapping[str, Any],
    ) -> str | None:
        """attempt 级尾部输出：node_history 优先，回退进程退出 tail。"""
        if fact.get("output"):
            return str(fact["output"])
        started, finished = fact.get("started_at"), fact.get("finished_at")
        for ev in reversed(self.request.events):
            if (
                ev.get("source") == "run_events"
                and ev.get("event_type") == "process_exit"
                and ev.get("context") == f"node={nid}"
            ):
                if started and finished and not started <= ev["timestamp"] <= finished:
                    continue
                tail = "\n".join((ev.get("data") or {}).get("tail") or []).strip()
                return tail or None
        return None

    def _session_execution_state(self, sid: str, status: Any = None) -> str:
        outcome = self.session_outcomes.get(sid)
        if outcome:
            return "finished" if outcome[0] in {"completed", "failed", "stopped"} else "in_progress"
        row = self.request.roster.get(sid) or {}
        if row.get("loop_status") == "no" and row.get("stop_reason") in (
            COMPLETED_STOP_REASONS | FAILED_STOP_REASONS
        ):
            return "finished"
        # 名册查无终态证据（如恢复库会话 stop_reason=user_pause）：按节点状态
        # 收口，避免已结束会话泄漏成 in_progress 被下游当成失败。
        return self._execution_state(status)

    def _session_finished_at(self, sid: str | None) -> str | None:
        outcome = self.session_outcomes.get(sid or "")
        return outcome[1] if outcome and outcome[0] in {"completed", "failed"} else None

    # ── 关系派生 ────────────────────────────────────────────────

    def _build_dag_edges(
        self,
        relations_by_scope: Mapping[str | None, _SortedRelations],
    ) -> None:
        """依赖边：主图静态边 × 各代；子图内部边按代取拓扑快照进嵌套层。"""
        for gen in sorted(self.gens):
            self._append_main_edges(relations_by_scope[None], gen)
            self._append_subgraph_edges(relations_by_scope, gen)

    def _append_main_edges(self, relations: _SortedRelations, gen: int) -> None:
        """单代主图静态依赖边（A/B 端按代解析，任一端缺失即跳过）。"""
        for source, target in self.main_edges:
            from_id = self._upstream_id(source, gen)
            to_id = self._downstream_id(target, gen)
            if from_id and to_id:
                relations.append(self._depends_on_relation(from_id, to_id))

    def _append_subgraph_edges(
        self,
        relations_by_scope: Mapping[str | None, _SortedRelations],
        gen: int,
    ) -> None:
        """单代子图内部依赖边：按代取拓扑快照写入各 generator 的嵌套层。"""
        for generator in self.sub_topos:
            topo = self._topology_at(generator, gen)
            if topo is None:
                continue
            relations = relations_by_scope[generator]
            for sn in topo:
                self._append_node_deps(relations, sn, gen)

    def _append_node_deps(
        self,
        relations: _SortedRelations,
        sn: Mapping[str, Any],
        gen: int,
    ) -> None:
        """单个子图节点的全部依赖边（A/B 端任一缺失即跳过该边）。"""
        for dep in sn.get("depends_on") or []:
            from_id = self._upstream_id(dep, gen)
            to_id = self._downstream_id(sn["id"], gen)
            if from_id and to_id:
                relations.append(self._depends_on_relation(from_id, to_id))

    def _upstream_id(self, nid: str, gen: int) -> str | None:
        """依赖边 A 端：末次验收、末次执行或占位。"""
        attempts = self._attempt_numbers(nid, gen)
        if not attempts:
            return self.member_ids.get((nid, gen, 0, "execute"))
        verify_attempts = [a for a in attempts if (nid, gen, a, "verify") in self.member_ids]
        if verify_attempts:
            return self.member_ids[(nid, gen, verify_attempts[-1], "verify")]
        if (nid, gen) in self.dynamic_workflow_ids:
            return self.dynamic_workflow_ids[(nid, gen)]
        return self.member_ids[(nid, gen, attempts[-1], "execute")]

    def _downstream_id(self, nid: str, gen: int) -> str | None:
        """依赖边 B 端：执行成员（末次 attempt），无实例回退 carried/占位。"""
        attempts = self._attempt_numbers(nid, gen)
        attempt = attempts[-1] if attempts else 0
        return self.member_ids.get((nid, gen, attempt, "execute"))

    def _build_rollback_edges(self, top_relations: _SortedRelations) -> None:
        for edge in self.rollback_edges:
            failed, from_gen = edge.get("from"), int(edge.get("from_gen") or 0)
            target, to_gen = edge.get("to"), int(edge.get("to_gen") or 0)
            if not failed or not target:
                continue
            from_id = self._rollback_source_id(failed, from_gen)
            to_id = self._downstream_id(target, to_gen)
            if not from_id or not to_id:
                continue
            top_relations.append(
                (
                    (_REL_RANK["rollback"], from_id, to_id),
                    WorkflowRelationContribution(
                        from_id=from_id,
                        to_id=to_id,
                        fields={"type": "rollback"},
                        annotation={"note": "回滚后重新执行"},
                    ),
                )
            )

    def _rollback_source_id(self, nid: str, gen: int) -> str | None:
        """回滚边起点：失败验收成员，无验收回退执行成员/占位。"""
        attempts = self._attempt_numbers(nid, gen)
        verify_attempts = [a for a in attempts if (nid, gen, a, "verify") in self.member_ids]
        if verify_attempts:
            return self.member_ids[(nid, gen, verify_attempts[-1], "verify")]
        if attempts:
            return self.member_ids[(nid, gen, attempts[-1], "execute")]
        return self.member_ids.get((nid, gen, 0, "execute"))

    # ── 子图嵌套 ────────────────────────────────────────────────

    def _subgraph_workflow_id(
        self,
        generator: str,
        gen: int,
        top_members: _SortedMembers,
    ) -> str:
        attempts = self._attempt_numbers(generator, gen)
        successful = [
            attempt
            for attempt in attempts
            if self.attempts[(generator, gen)].facts[attempt].get("status") == "passed"
        ]
        selected = (successful or attempts or [0])[-1]
        if generator in self.subgraph_nodes:
            member_id = self.member_ids.get((generator, gen, selected, "execute"))
            if member_id is not None:
                return member_id
            return self._emit_subgraph_placeholder(generator, gen, top_members)
        return self.dynamic_workflow_ids.setdefault(
            (generator, gen),
            "w"
            + synthetic_member_id(
                self.run_id,
                generator,
                gen,
                selected,
                "subgraph",
            )[1:],
        )

    def _emit_subgraph_placeholder(
        self,
        generator: str,
        gen: int,
        top_members: _SortedMembers,
    ) -> str:
        """generator 该代无成员（最近实例非终态跳过生成）时补占位 workflow 容器。

        子节点成员与关系都挂在该容器下，否则会随缺失成员一起静默消失。
        """
        member, member_id = self._pending_placeholder(generator, gen, "workflow")
        top_members.append(
            (
                (_KIND_RANK["workflow"], generator, gen, 0, 0, member_id),
                member,
            )
        )
        return member_id

    def _topology_at(
        self,
        generator: str,
        gen: int,
    ) -> list[dict[str, Any]] | None:
        """gen 代生效的子图拓扑快照（最近代 ≤ gen）。"""
        topos = self.sub_topos.get(generator)
        if not topos:
            return None
        available = [g for g in topos if g <= gen]
        return topos[max(available)] if available else None

    # ── 节点定义装饰 ────────────────────────────────────────────

    def _decorate_retry_policies(
        self,
        members: list[WorkflowMemberContribution],
    ) -> None:
        """retryPolicy/runOnFailure 只挂各 nodeId 首条实际执行成员（attempt ≥ 1，不含 carried 与待执行占位）。"""
        seen: set[str] = set()
        for member in members:
            graph = member.fields
            if (member.annotation or {}).get("role") != "execute" or (graph.get("attempt", 0) == 0):
                continue
            nid = graph.get("nodeId")
            if nid in seen or nid not in self.main_nodes:
                continue
            seen.add(nid)
            node = self.main_nodes[nid]
            acceptance = node.get("acceptance")
            if isinstance(acceptance, dict) and acceptance:
                graph["retryPolicy"] = {
                    "maxRetries": acceptance.get("max_retries", 0),
                    "onExhaust": acceptance.get("on_exhaust") or "stop",
                }
                if acceptance.get("rollback_to"):
                    graph["retryPolicy"]["rollbackToNodeId"] = acceptance["rollback_to"]
            if node.get("run_on_failure") is not None:
                graph["runOnFailure"] = node["run_on_failure"]


def _load_program(
    program_dir: Path,
) -> tuple[dict[str, dict[str, Any]], list[tuple[str, str]]]:
    """冻结程序重翻译：主图节点定义与依赖边（外部 yaml 可能已漂移）。"""
    from workflow_controller.plugins.graph import translator

    entry = program_dir / _ENTRY_MARKER
    if not entry.is_file():
        raise ValueError(f"run 根缺冻结程序清单：{entry}")
    entry_path = program_dir / entry.read_text(encoding="utf-8").strip()
    if not entry_path.is_file():
        raise ValueError(f"冻结程序入口不在：{entry_path}")
    config = translator.normalize_config(translator.parse_yaml_file(entry_path) or {})
    nodes = {node["id"]: node for node in (config.get("nodes") or [])}
    edges = sorted(
        {
            (dep, node["id"])
            for node in (config.get("nodes") or [])
            for dep in (node.get("depends_on") or [])
        }
    )
    return nodes, edges
