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

"""Run 导出编排者——workflow bundle 的装配、校验与确定性写包。

核心只出账面与搬运：从守护进程单库取齐 run 记录、成员名册、两表事件、
驱动快照与冻结程序，经 SDK 贡献协议把 format 方言翻译权交给驱动工厂；
无贡献者时走通用兜底（run_sessions + 进程聚合 + handoff 边）。会话单元
由转录源定位与 claude-jsonl 转换生产；校验失败不产残包（临时目录 + 备份
换名发布）。同一 run 状态未变时两次导出字节一致；同 run 并发导出串行。
"""

from __future__ import annotations

import asyncio
import json
import logging
import posixpath
import shutil
import threading
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import jsonschema

from workflow_controller.core.runs import RunKind, RunStatus
from workflow_controller.core.schemas import WORKFLOW_BUNDLE_SCHEMA, WORKFLOW_BUNDLE_SCHEMA_TEXT
from workflow_controller.plugin.run_export import (
    RunExportContribution,
    RunExportRequest,
    WorkflowMemberContribution,
    WorkflowRelationContribution,
    synthetic_member_id,
)
from workflow_controller.store.run_events_cursor import RunEventsCursor
from workflow_controller.transcript import (
    resolve_roster_source,
    write_opencode_session,
    write_opencode_subagents,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from workflow_controller.core.runs import RunRecord, RunStore
    from workflow_controller.store.event_store import EventStore
    from workflow_controller.transcript import TranscriptSource

logger = logging.getLogger(__name__)

BUNDLE_SCHEMA = "workflow-bundle"
BUNDLE_VERSION = 1
_MEMBER_KINDS = frozenset({"session", "process", "workflow"})
_RESERVED_EXPORT_FIELDS = frozenset(
    {
        "schema",
        "version",
        "id",
        "kind",
        "name",
        "startedAt",
        "finishedAt",
        "data",
        "sessionId",
        "location",
        "output",
        "annotation",
        "from",
        "to",
        "exportDataComplete",
        "role",
    }
)
_SOURCE_RANK = {"run_events": 0, "events": 1}


def _iso_utc(ts: Any) -> str | None:
    """把 naive/aware 时间统一成带 UTC 时区的 ISO 时间戳。

    导出的 startedAt/finishedAt 须带时区，否则下游在非 UTC 机器上会把
    naive 时间读成本地时间（时区漂移数小时）。naive 值按本地时区换算成
    UTC：现役写入方均已 aware（datetime.now(tz=UTC)），naive 分支
    仅回退历史存量串（旧版本按本地墙上时间写入）；无法解析的字符串原样
    透传（避免抛错中断导出）。
    """
    if ts is None:
        return None
    if isinstance(ts, datetime):
        dt = ts
    else:
        try:
            dt = datetime.fromisoformat(str(ts))
        except ValueError:
            return str(ts)
    return dt.astimezone(UTC).isoformat()


class RunExportRefusedError(ValueError):
    """run 形态不接受导出（隐式 run / pending 无事实）。"""


class RunExportFailedError(RuntimeError):
    """导出失败（贡献者异常 / 校验拒绝 / 单元生产意外错误）。"""


def export_refusal_reason(run: RunRecord) -> str | None:
    """run 形态不接受导出时的拒绝原因（None = 可导出）。

    路由层同步门与 ``_assemble_request`` 共用的单一策略——形态拒绝的判定
    只在此处维护，避免两处实现漂移（core 侧新增/调整拒绝形态时路由门
    自动跟随）。
    """
    if run.kind == RunKind.IMPLICIT:
        return "隐式 run 不支持导出"
    if run.status == RunStatus.PENDING:
        return "pending run 无事实可导"
    return None


@dataclass(frozen=True)
class _ExportOutcome:
    """导出结果摘要（端点应答与日志用）。"""

    bundle_dir: Path
    archive: Path | None
    members: int
    relations: int
    sessions: int
    complete: bool
    missing_sessions: list[str]


@dataclass
class _ExportLockSlot:
    """run 级导出锁槽：inflight 计在途（含排队）导出，归零即可回收条目。

    lock 用 threading.Lock 而非 asyncio.Lock：导出经路由后台线程 + 各自
    事件循环执行，asyncio.Lock 绑定创建它的 loop，跨 loop 等锁会永久挂起
    （另一 loop 上的 release 无法唤醒它）。threading.Lock 进程级串行，
    跨线程/跨 loop 都成立。
    """

    lock: threading.Lock = field(default_factory=threading.Lock)
    inflight: int = 0


class RunExportService:
    """导出编排者：装配请求、调贡献者、产单元、校验、写包。"""

    def __init__(
        self,
        *,
        run_store: RunStore,
        event_store: EventStore,
        run_root: Callable[[str], Path],
        factory_lookup: Callable[[str], Any],
        guarded_call: Callable[..., Any],
        default_runtime: str = "",
        scan_user_homes: bool = False,
    ) -> None:
        self._run_store = run_store
        self._event_store = event_store
        self._run_root = run_root
        self._factory_lookup = factory_lookup
        self._guarded_call = guarded_call
        self._default_runtime = default_runtime
        self._scan_user_homes = scan_user_homes
        self._cursor = RunEventsCursor(event_store)
        self._export_locks: dict[str, _ExportLockSlot] = {}
        self._export_locks_guard = threading.Lock()

    @staticmethod
    def _process_members(
        request: RunExportRequest,
    ) -> list[WorkflowMemberContribution]:
        exits: dict[str, dict[str, Any]] = {}
        first_output: dict[str, str] = {}
        for ev in request.events:
            if ev.get("source") != "run_events":
                continue
            data = ev.get("data") or {}
            proc_id = data.get("proc_id")
            if not proc_id:
                continue
            if ev.get("event_type") == "process_exit":
                exits[proc_id] = ev
            elif ev.get("event_type") == "process_output" and proc_id not in first_output:
                first_output[proc_id] = ev["timestamp"]
        out: list[WorkflowMemberContribution] = []
        for proc_id in sorted(exits):
            ev = exits[proc_id]
            tail = "\n".join((ev.get("data") or {}).get("tail") or []).strip()
            context = ev.get("context")
            out.append(
                WorkflowMemberContribution(
                    id=synthetic_member_id(request.run.run_id, proc_id, 0, 0, "process"),
                    kind="process",
                    name=context or proc_id,
                    started_at=first_output.get(proc_id),
                    finished_at=ev.get("timestamp"),
                    output_text=tail or None,
                    annotation={"workflow_controller": {"proc_id": proc_id}},
                )
            )
        return out

    @staticmethod
    def _relation_doc(r: WorkflowRelationContribution) -> dict[str, Any]:
        doc: dict[str, Any] = {"from": r.from_id, "to": r.to_id}
        RunExportService._add_fields(doc, r.fields)
        if r.annotation is not None:
            doc["annotation"] = json.loads(json.dumps(r.annotation, default=str))
        return doc

    @staticmethod
    def _add_fields(doc: dict[str, Any], fields: Mapping[str, Any]) -> None:
        conflicts = [key for key in fields if key in _RESERVED_EXPORT_FIELDS]
        if conflicts:
            identity = doc.get("id") or f"{doc.get('from')}→{doc.get('to')}"
            raise RunExportFailedError(
                f"贡献字段不得覆盖保留字段：{identity} 冲突 {sorted(conflicts)}"
            )
        doc.update(json.loads(json.dumps(fields, default=str)))

    # ── 请求装配 ────────────────────────────────────────────────

    @staticmethod
    def _write_session_unit(
        native: str,
        source: TranscriptSource,
        sessions_root: Path,
    ) -> bool:
        unit_dir = sessions_root / f"{native}.native"
        unit_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = unit_dir / f"{native}.jsonl"
        if source.kind in {"claude-jsonl", "codex-jsonl"}:
            written = source.path.is_file()
            if written:
                shutil.copyfile(source.path, jsonl_path)
        else:
            written = write_opencode_session(source.path, native, jsonl_path)
        if not written:
            # 缺席不留空单元目录（完整性降级由调用方置 location=null）
            shutil.rmtree(unit_dir, ignore_errors=True)
            return False
        meta = {
            "x_context_bay": {
                "schema": "cc-session-meta",
                "version": 1,
                "data": {
                    "producer": "workflow-controller-run-export",
                    "framework": source.framework,
                    "sid": native,
                },
            }
        }
        (unit_dir / f"{native}.meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        if source.kind == "opencode-db":
            # 子代理会话（task 派发的 subagent）随父会话一并落 subagents/，
            # 供 Insight 关联「执行正文」——否则 task 调用指向的子会话 ID 无处可寻
            write_opencode_subagents(source.path, native, unit_dir)
        return True

    async def export_run(
        self,
        run_id: str,
        *,
        archive: bool = False,
        progress_cb: Callable[[int, int], None] | None = None,
        archive_name: str | None = None,
    ) -> dict[str, Any]:
        """导出 run 为 workflow bundle 目录（可选 zip），幂等覆盖重写。

        progress_cb(done, total)：逐会话单元生产进度回调（后台导出线程
        逐条上报，供前端进度条轮询）。done/total 均按 session 单元计数。
        archive_name：zip 文件名（相对 export/ 目录）；缺省按 run 固定命名
        ``{run_id}-bundle.zip``。后台异步导出按任务传入唯一名，使每次导出的
        zip 归属独立，下载字节与统计头不会串到同 run 的更新导出。
        """
        # 同 run 串行：临时/目标目录按 run 固定命名，并发写互毁。
        # 锁经 to_thread 在副线程等待——同步 acquire 会卡死单 loop 下的
        # asyncio.gather 调用方（首任务持锁后其余任务阻塞事件循环）。
        # 注意本等待不可取消：排队协程被 cancel 后副线程仍会替它完成
        # acquire 并退出而不释放，搁浅一个排队线程。RunAPI.export_run 是
        # 公共面，调用方不得 cancel 本协程（路由后台线程 run_until_complete
        # 跑满全程，当前无取消方）。
        slot = self._lock_slot_for(run_id)
        acquired = False
        try:
            await asyncio.to_thread(slot.lock.acquire)
            acquired = True
            outcome = await self._export_locked(run_id, archive, progress_cb, archive_name)
        finally:
            if acquired:
                slot.lock.release()
            self._release_lock_slot(run_id, slot)
        logger.info(
            "run %s 导出完成：%s（members=%s relations=%s complete=%s）",
            run_id,
            outcome.bundle_dir,
            outcome.members,
            outcome.relations,
            outcome.complete,
        )
        return {
            "ok": True,
            "path": str(outcome.bundle_dir),
            "archive": str(outcome.archive) if outcome.archive else None,
            "members": outcome.members,
            "relations": outcome.relations,
            "sessions": outcome.sessions,
            "missing_sessions": outcome.missing_sessions,
            "exportDataComplete": outcome.complete,
        }

    async def _export_locked(
        self,
        run_id: str,
        archive: bool,
        progress_cb: Callable[[int, int], None] | None,
        archive_name: str | None,
    ) -> _ExportOutcome:
        """持锁导出主体：组装请求/贡献/文档后写包。"""
        request = await self._assemble_request(run_id)
        contribution, dialect = await self._contribution(request)
        document, native_roster, unresolved = self._compose_document(
            request,
            contribution,
        )
        complete = (
            request.run.kind != RunKind.WORKFLOW
            or (dialect and request.driver_snapshot is not None)
        ) and not unresolved
        return await self._write_bundle(
            request.run,
            document,
            native_roster,
            complete,
            archive,
            progress_cb,
            archive_name,
        )

    def _lock_slot_for(self, run_id: str) -> _ExportLockSlot:
        # 懒建 + inflight 累加：后台线程并发出现在多 loop，须以线程锁守卫
        with self._export_locks_guard:
            slot = self._export_locks.get(run_id)
            if slot is None:
                slot = _ExportLockSlot()
                self._export_locks[run_id] = slot
            slot.inflight += 1
            return slot

    # ── 贡献者派发 ──────────────────────────────────────────────

    def _release_lock_slot(self, run_id: str, slot: _ExportLockSlot) -> None:
        # 排队中的导出已计入 inflight，归零移除不会与等待者失联
        with self._export_locks_guard:
            slot.inflight -= 1
            if not slot.inflight:
                self._export_locks.pop(run_id, None)

    async def _assemble_request(self, run_id: str) -> RunExportRequest:
        run = await self._run_store.get(run_id)
        refusal = export_refusal_reason(run)
        if refusal is not None:
            raise RunExportRefusedError(f"{refusal}：{run_id}")
        members = await self._run_store.list_run_sessions(run_id)
        roster, snapshot, projection = await asyncio.to_thread(
            self._read_facts_sync,
            run_id,
            [m.session_id for m in members],
        )
        events = await asyncio.to_thread(self._read_events, run_id)
        return RunExportRequest(
            run=run,
            members=tuple(members),
            roster=roster,
            events=events,
            driver_snapshot=snapshot,
            driver_projection=projection,
            program_dir=self._run_root(run_id) / "program",
        )

    def _read_facts_sync(
        self,
        run_id: str,
        session_ids: list[str],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any] | None, dict[str, Any] | None]:
        """名册/快照/投影的同步批量读（与账面读同一时点窗内取齐）。"""
        roster = {}
        for sid in session_ids:
            row = self._event_store.get_session_sync(sid)
            if row is not None:
                roster[sid] = row
        snapshot = self._run_store.load_driver_state_sync(run_id)
        projection = self._run_store.load_driver_projection_sync(run_id)
        return roster, snapshot, projection

    def _read_events(self, run_id: str) -> list[dict[str, Any]]:
        """两表事件全量翻页后按 (timestamp, source, id) 终排。"""
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page, cursor, limit = self._cursor.fetch(run_id, cursor, limit=1000)
            rows.extend(page)
            if len(page) < limit:
                break
        rows.sort(key=lambda r: (r["timestamp"], _SOURCE_RANK[r["source"]], r["id"]))
        return rows

    # ── 文档组装 ────────────────────────────────────────────────

    async def _contribution(
        self,
        request: RunExportRequest,
    ) -> tuple[RunExportContribution, bool]:
        """返回贡献与是否走了方言贡献者（完整性评定的输入之一）。"""
        factory = self._factory_lookup(request.run.format) if request.run.format else None
        contributor = getattr(factory, "export_contribution", None)
        if contributor is None:
            if request.run.kind == RunKind.WORKFLOW:
                # 方言面缺席不伪装完整：兜底包无可辨认的工作流语义，
                # 版本偏斜/贡献者丢失须在日志可见（complete 由调用方置 false）
                logger.warning(
                    "run %s 的 format %r 无导出贡献者，产出通用兜底包（方言面缺席）",
                    request.run.run_id,
                    request.run.format,
                )
            return self._fallback_contribution(request), False
        try:
            contribution = await self._guarded_call(
                request.run.format,
                contributor,
                request,
            )
        except Exception as e:
            raise RunExportFailedError(f"导出贡献者执行失败：{e}") from e
        if not isinstance(contribution, RunExportContribution):
            raise RunExportFailedError("导出贡献者返回类型不合法")
        return contribution, True

    def _fallback_contribution(self, request: RunExportRequest) -> RunExportContribution:
        """通用兜底：run_sessions 成员 + 进程聚合 + handoff 边。"""
        members: list[WorkflowMemberContribution] = []
        member_sids = set()
        for m in request.members:
            member_sids.add(m.session_id)
            workflow_controller_ann: dict[str, Any] = {}
            if m.context:
                workflow_controller_ann["context"] = m.context
            members.append(
                WorkflowMemberContribution(
                    id=f"s_{m.session_id}",
                    kind="session",
                    name=(request.roster.get(m.session_id) or {}).get("name") or m.session_id,
                    session_id=m.session_id,
                    annotation={
                        "role": m.role,
                        **(
                            {"workflow_controller": workflow_controller_ann}
                            if workflow_controller_ann
                            else {}
                        ),
                    },
                )
            )
        members.extend(self._process_members(request))
        relations = self._handoff_relations(request, member_sids)
        return RunExportContribution(members=tuple(members), relations=tuple(relations))

    def _handoff_relations(
        self,
        request: RunExportRequest,
        member_sids: set[str],
    ) -> list[WorkflowRelationContribution]:
        edges: list[tuple[str, str]] = []
        for m in request.members:
            for succ in self._event_store.get_successors_sync(m.session_id):
                to = succ.get("to_session_id")
                if to in member_sids and to != m.session_id:
                    edges.append((m.session_id, to))
        return [
            WorkflowRelationContribution(
                from_id=f"s_{f}",
                to_id=f"s_{t}",
                fields={"type": "handoff"},
            )
            for f, t in sorted(set(edges))
        ]

    def _compose_document(
        self,
        request: RunExportRequest,
        contribution: RunExportContribution,
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]], bool]:
        """贡献 → workflow.json 文档；返回文档、native→roster 行索引、
        与是否有源会话 id 无法解析到 native 的降级事实。
        """
        native_of: dict[str, str] = {}
        for m in request.members:
            if m.native_session_id:
                native_of[m.session_id] = m.native_session_id
        for sid, row in request.roster.items():
            if row.get("native_session_id"):
                native_of[sid] = row["native_session_id"]
        native_roster = {
            native_of[sid]: row for sid, row in request.roster.items() if sid in native_of
        }
        unresolved = _has_unresolved_sessions(contribution, native_of)
        run = request.run
        name = contribution.name or run.labels.get("name") or run.run_id
        document = {
            "schema": BUNDLE_SCHEMA,
            "version": BUNDLE_VERSION,
            "id": run.run_id,
            "kind": "workflow",
            "name": name,
            "startedAt": _iso_utc(run.created_at),
            "finishedAt": _iso_utc(run.finished_at),
            "data": {
                "exportDataComplete": True,
                "members": [
                    self._member_doc(m, contribution.nested, native_of)
                    for m in contribution.members
                ],
                "relations": [self._relation_doc(r) for r in contribution.relations],
            },
        }
        return document, native_roster, unresolved

    # ── 单元生产与写包 ──────────────────────────────────────────

    def _member_doc(
        self,
        m: WorkflowMemberContribution,
        nested: Mapping[str, RunExportContribution],
        native_of: Mapping[str, str],
    ) -> dict[str, Any]:
        if m.kind not in _MEMBER_KINDS:
            raise RunExportFailedError(f"成员 kind 不在词表：{m.kind!r}（{m.id}）")
        doc: dict[str, Any] = {
            "id": m.id,
            "kind": m.kind,
            "name": m.name,
            "startedAt": _iso_utc(m.started_at),
            "finishedAt": _iso_utc(m.finished_at),
        }
        if m.kind == "workflow":
            if m.session_id is not None or m.output_text is not None:
                raise RunExportFailedError(f"workflow 成员不得直接携带会话或输出：{m.id}")
            sub = nested.get(m.id)
            if sub is None:
                raise RunExportFailedError(f"嵌套 workflow 成员缺子贡献：{m.id}")
            doc["data"] = {
                "members": [self._member_doc(sm, sub.nested, native_of) for sm in sub.members],
                "relations": [self._relation_doc(r) for r in sub.relations],
            }
        elif m.kind == "session":
            native = native_of.get(m.session_id) if m.session_id else None
            doc["sessionId"] = native
            doc["location"] = f"sessions/{native}.native" if native else None
        else:
            if m.session_id is not None:
                raise RunExportFailedError(f"process 成员不得携带会话：{m.id}")
            if m.output_text is not None:
                doc["output"] = {"text": m.output_text}
        self._add_fields(doc, m.fields)
        annotation = (
            json.loads(json.dumps(m.annotation, default=str)) if m.annotation is not None else {}
        )
        if annotation or m.annotation is not None:
            doc["annotation"] = annotation
        return doc

    async def _write_bundle(
        self,
        run: RunRecord,
        document: dict[str, Any],
        native_roster: Mapping[str, Mapping[str, Any]],
        complete: bool,
        archive: bool,
        progress_cb: Callable[[int, int], None] | None = None,
        archive_name: str | None = None,
    ) -> _ExportOutcome:
        export_root = self._run_root(run.run_id) / "export"
        bundle_dir = export_root / f"{run.run_id}-bundle"
        tmp_dir = export_root / f".tmp-{run.run_id}-bundle"
        await asyncio.to_thread(shutil.rmtree, tmp_dir, True)
        complete, missing_sessions = await self._stage_bundle(
            run, document, native_roster, complete, progress_cb, tmp_dir, bundle_dir, export_root
        )
        archive_path = await self._write_archive(
            run.run_id, bundle_dir, export_root, archive, archive_name
        )
        members, relations, sessions = _tree_counts(document)
        return _ExportOutcome(
            bundle_dir,
            archive_path,
            members,
            relations,
            sessions,
            complete,
            missing_sessions,
        )

    async def _stage_bundle(
        self,
        run: RunRecord,
        document: dict[str, Any],
        native_roster: Mapping[str, Mapping[str, Any]],
        complete: bool,
        progress_cb: Callable[[int, int], None] | None,
        tmp_dir: Path,
        bundle_dir: Path,
        export_root: Path,
    ) -> tuple[bool, list[str]]:
        """暂存目录内生产会话单元与文档、校验后换名发布；返回 (完整性, 缺席清单)。"""
        try:
            (tmp_dir / "sessions").mkdir(parents=True, exist_ok=True)
            produced_ok, missing_sessions = await asyncio.to_thread(
                self._produce_session_units,
                document,
                native_roster,
                tmp_dir / "sessions",
                progress_cb,
            )
            complete = produced_ok and complete
            document["data"]["exportDataComplete"] = complete
            (tmp_dir / "workflow.json").write_text(
                json.dumps(document, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            (tmp_dir / "workflow.schema.json").write_text(
                WORKFLOW_BUNDLE_SCHEMA_TEXT,
                encoding="utf-8",
            )
            await asyncio.to_thread(validate_bundle, tmp_dir)
            await asyncio.to_thread(
                _publish_bundle,
                tmp_dir,
                bundle_dir,
                export_root / f".old-{run.run_id}-bundle",
            )
        except OSError as e:
            # 与 zip 路径同约：写盘类失败走 503 契约，不裸 500
            await asyncio.to_thread(shutil.rmtree, tmp_dir, True)
            raise RunExportFailedError(f"暂存或会话单元生产写盘失败：{e}") from e
        except Exception:
            # 不捕取消/退出类异常：本清理分支含 await，被取消的任务里会再次中断
            await asyncio.to_thread(shutil.rmtree, tmp_dir, True)
            raise
        return complete, missing_sessions

    async def _write_archive(
        self,
        run_id: str,
        bundle_dir: Path,
        export_root: Path,
        archive: bool,
        archive_name: str | None,
    ) -> Path | None:
        """目录包发布后写 zip；失败抛 503 约定错误（目录包不回滚）。"""
        if not archive:
            return None
        archive_path = export_root / (archive_name or f"{run_id}-bundle.zip")
        try:
            await asyncio.to_thread(write_bundle_zip, bundle_dir, archive_path)
        except OSError as e:
            # 目录包已发布不回滚：zip 是附属产物，失败走 503 约定并注明落点
            raise RunExportFailedError(f"zip 写入失败（目录包已落 {bundle_dir}）：{e}") from e
        return archive_path

    def _produce_session_units(
        self,
        document: dict[str, Any],
        native_roster: Mapping[str, Mapping[str, Any]],
        sessions_root: Path,
        progress_cb: Callable[[int, int], None] | None = None,
    ) -> tuple[bool, list[str]]:
        """逐会话单元生产；缺席置 location=null 并返回 (完整性, 缺失会话列表)。

        progress_cb(done, total) 每产出一个会话单元回调一次（从 0 到
        total），供后台导出线程驱动前端进度条。
        """
        session_members = [
            m for m in _walk_members(document) if m["kind"] == "session" and m.get("sessionId")
        ]
        total = len(session_members)
        if progress_cb is not None:
            progress_cb(0, total)
        ok = True
        missing: list[str] = []
        for done, member in enumerate(session_members, start=1):
            native = member["sessionId"]
            row = native_roster.get(native)
            try:
                source = (
                    resolve_roster_source(
                        row,
                        self._default_runtime,
                        scan_user_homes=self._scan_user_homes,
                    )
                    if row
                    else None
                )
            except ValueError:
                source = None
            if source is None or not self._write_session_unit(native, source, sessions_root):
                member["location"] = None
                missing.append(native)
                ok = False
            if progress_cb is not None:
                progress_cb(done, total)
        return ok, missing


# ── 校验器（结构级 + 通用语义级，零方言知识）──────────────────────


def _has_unresolved_sessions(
    contribution: RunExportContribution,
    native_of: Mapping[str, str],
) -> bool:
    """贡献声明的源会话解析不到 native id → 事实缺失的降级信号。"""
    for m in contribution.members:
        if m.kind == "session" and m.session_id and m.session_id not in native_of:
            return True
    return any(_has_unresolved_sessions(sub, native_of) for sub in contribution.nested.values())


def _walk_members(document: dict[str, Any]) -> list[dict[str, Any]]:
    """整棵树的成员列表（含嵌套 workflow 内层）。"""
    out: list[dict[str, Any]] = []
    for m in document["data"].get("members", []):
        out.append(m)
        if m["kind"] == "workflow":
            out.extend(_walk_members(m))
    return out


def _walk_relations(document: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for m in document["data"].get("members", []):
        if m["kind"] == "workflow":
            out.extend(_walk_relations(m))
    out.extend(document["data"].get("relations", []))
    return out


def _tree_counts(document: dict[str, Any]) -> tuple[int, int, int]:
    members = _walk_members(document)
    return (
        len(members),
        len(_walk_relations(document)),
        sum(1 for m in members if m.get("kind") == "session"),
    )


def validate_bundle(bundle_dir: Path) -> None:
    """对既有包目录独立校验 workflow.json；失败 raise（不产残包防线）。

    结构/类型/枚举/条件必填（members·relations 可选、kind 枚举、
    根信封 const 等）恒由内置权威 schema 校验（包内 workflow.schema.json 与其
    保证逐字一致，缺 schema 的旧包同样用内置权威）；跨节点与文件系统不变量
    （成员 id 全树唯一、关系端点可解析、location 规范化/不逃逸、会话单元文件
    存在）由下方代码校验。
    """
    manifest = bundle_dir / "workflow.json"
    if not manifest.is_file():
        raise RunExportFailedError(f"包缺 workflow.json：{bundle_dir}")
    schema_file = bundle_dir / "workflow.schema.json"
    # 包内 schema 必须与内置权威逐字一致，防打包错 schema 版本导致校验漂移
    try:
        schema_mismatch = (
            schema_file.is_file()
            and schema_file.read_text(encoding="utf-8") != WORKFLOW_BUNDLE_SCHEMA_TEXT
        )
    except (OSError, UnicodeError) as e:
        raise RunExportFailedError(f"包内 workflow.schema.json 不可读：{e}") from e
    if schema_mismatch:
        raise RunExportFailedError("包内 workflow.schema.json 与内置权威不一致")
    try:
        document = json.loads(manifest.read_text(encoding="utf-8"))
    except ValueError as e:
        raise RunExportFailedError(f"workflow.json 不可解析：{e}") from e
    if not isinstance(document, dict):
        raise RunExportFailedError("workflow.json 根需为 JSON 对象")
    _validate_against_schema(document)
    ids: set[str] = set()
    _validate_workflow_invariants(document, True, ids, bundle_dir)
    member_ids = {m["id"] for m in _walk_members(document)}
    for rel in _walk_relations(document):
        for end in (rel["from"], rel["to"]):
            if end not in member_ids:
                raise RunExportFailedError(f"关系端点不可解析：{end}")
    degraded = [
        m
        for m in _walk_members(document)
        if m["kind"] == "session" and m.get("sessionId") and m.get("location") is None
    ]
    if degraded and document["data"]["exportDataComplete"] is not False:
        raise RunExportFailedError(
            f"会话单元缺席但 exportDataComplete 非 false：{[m['id'] for m in degraded]}"
        )


def _validate_against_schema(document: dict[str, Any]) -> None:
    """结构校验：单一权威是随包 schema（此处用内置同款文本）。"""
    try:
        jsonschema.validate(document, WORKFLOW_BUNDLE_SCHEMA)
    except jsonschema.ValidationError as e:
        raise RunExportFailedError(_schema_error_text(e)) from e


def _schema_error_text(error: jsonschema.ValidationError) -> str:
    """把 oneOf/allOf 的嵌套失败下钻到叶子，去重后按路径深度排序呈现。

    jsonschema 顶层报错常折叠成「is not valid under any of the given schemas」，
    真正原因在 error.context 里。这里递归取叶子、去重，避免丢失定位信息。
    """
    leaves: list[tuple[str, str]] = []

    def walk(err: jsonschema.ValidationError) -> None:
        if err.context:
            for child in err.context:
                walk(child)
            return
        leaves.append((err.json_path, err.message))

    walk(error)
    uniq: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for path, message in leaves:
        key = (path, message)
        if key not in seen:
            seen.add(key)
            uniq.append((path, message))
    uniq.sort(key=lambda pm: len(pm[0]), reverse=True)
    details = "；".join(f"{p}: {m}" for p, m in uniq[:5])
    return f"workflow.json 结构不符 schema：{details}"


def _validate_workflow_invariants(
    node: dict[str, Any],
    is_root: bool,
    ids: set[str],
    bundle_dir: Path,
) -> None:
    """跨节点不变量：递归遍历，校验成员 id 唯一 + 根/嵌套专属字段 + 会话单元。"""
    if is_root:
        if not isinstance(node["data"].get("exportDataComplete"), bool):
            raise RunExportFailedError("根缺布尔 exportDataComplete")
    elif "schema" in node or "exportDataComplete" in node.get("data", {}):
        raise RunExportFailedError(f"嵌套 workflow 带根专属字段：{node.get('id')}")
    for member in node["data"].get("members", []):
        _validate_member_invariants(member, ids, bundle_dir)


def _validate_member_invariants(
    member: dict[str, Any],
    ids: set[str],
    bundle_dir: Path,
) -> None:
    if member["id"] in ids:
        raise RunExportFailedError(f"成员 id 重复：{member['id']}")
    ids.add(member["id"])
    carried = member.get("executionState") == "carried"
    has_source_generation = "resultFromGeneration" in member
    if carried != has_source_generation:
        raise RunExportFailedError(
            f"resultFromGeneration 仅随 carried 出现，且 carried 必须提供来源代：{member['id']}"
        )
    if member["kind"] == "workflow":
        _validate_workflow_invariants(member, False, ids, bundle_dir)
        return
    session_id, location = member.get("sessionId"), member.get("location")
    # (sessionId 非空, location=null) 是已发生会话的单元缺席降级形态，
    # 由根 exportDataComplete=false 兜底；(null, 非空) 与空串一律非法
    if session_id is None and location is not None:
        raise RunExportFailedError(f"成员缺 sessionId 却有 location：{member['id']}")
    if session_id is not None and not session_id:
        raise RunExportFailedError(f"成员 sessionId 为空串：{member['id']}")
    if location is None:
        return
    if not isinstance(location, str) or not location:
        raise RunExportFailedError(f"成员 location 非法：{location!r}")
    if posixpath.isabs(location) or ".." in posixpath.normpath(location).split("/"):
        raise RunExportFailedError(f"成员 location 逃逸或非相对路径：{location}")
    if posixpath.normpath(location) != location:
        raise RunExportFailedError(f"成员 location 非规范化路径：{location}")
    unit_dir = bundle_dir / location
    jsonl = unit_dir / f"{session_id}.jsonl"
    meta = unit_dir / f"{session_id}.meta.json"
    if not jsonl.is_file() or not meta.is_file():
        raise RunExportFailedError(f"会话单元文件缺失：{location}")
    try:
        json.loads(meta.read_text(encoding="utf-8"))
    except ValueError as e:
        raise RunExportFailedError(f"meta.json 不可解析：{location}") from e


def _publish_bundle(tmp_dir: Path, bundle_dir: Path, backup_dir: Path) -> None:
    """备份换名发布：rename 失败回滚旧包，目标位任意时刻保留最近可用包。"""
    shutil.rmtree(backup_dir, ignore_errors=True)
    if bundle_dir.exists():
        bundle_dir.rename(backup_dir)
    try:
        tmp_dir.rename(bundle_dir)
    except Exception:
        if not bundle_dir.exists() and backup_dir.exists():
            backup_dir.rename(bundle_dir)
        raise
    shutil.rmtree(backup_dir, ignore_errors=True)


def _zip_entry_info(path: Path, bundle_dir: Path) -> zipfile.ZipInfo:
    """固定时间戳与权限位的 zip 条目元信息（确定性压缩的解压一致前提）。"""
    info = zipfile.ZipInfo(
        path.relative_to(bundle_dir).as_posix(),
        date_time=(1980, 1, 1, 0, 0, 0),
    )
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    return info


def write_bundle_zip(bundle_dir: Path, archive_path: Path) -> None:
    """确定性压缩（固定时间戳）；解压后与目录产物逐字节一致。

    先写同目录临时文件再原子换名：并发导出的下载方只会看到旧的或新的完整
    包，不会读到写一半被截断的 zip（与 _publish_bundle 的发布语义一致）。
    """
    tmp_path = archive_path.with_name(archive_path.name + ".tmp")
    try:
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(p for p in bundle_dir.rglob("*") if p.is_file()):
                zf.writestr(_zip_entry_info(path, bundle_dir), path.read_bytes())
        tmp_path.replace(archive_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
