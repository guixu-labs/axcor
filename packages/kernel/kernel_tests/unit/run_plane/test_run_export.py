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

"""run 导出核心测试：确定性、诚实降级、校验器、通用兜底、贡献协议中立性。

不依赖协调器：RunExportService 直接以 RunStore/EventStore 装配，贡献者
经 factory_lookup 注入——覆盖无贡献者兜底与玩具方言两条链路。
"""

from __future__ import annotations

import asyncio
import json
import zipfile
from datetime import UTC
from pathlib import Path
from typing import Any

import pytest

from workflow_controller.core.run_export import (
    BUNDLE_SCHEMA,
    BUNDLE_VERSION,
    RunExportFailedError,
    RunExportRefusedError,
    RunExportService,
    _iso_utc,
    _publish_bundle,
    validate_bundle,
)
from workflow_controller.core.runs import RunKind, RunStatus, RunStore
from workflow_controller.core.schemas import WORKFLOW_BUNDLE_SCHEMA, WORKFLOW_BUNDLE_SCHEMA_TEXT
from workflow_controller.plugin.run_export import (
    RunExportContribution,
    RunExportRequest,
    WorkflowMemberContribution,
    WorkflowRelationContribution,
)
from workflow_controller.transcript import TranscriptSource

from tests.support import find_upward

FIXTURE = (
    find_upward(Path(__file__).resolve().parent, "fixtures/workflow-bundle-minimal.json")
    / "fixtures"
    / "workflow-bundle-minimal.json"
)


def _make_service(
    tmp_path: Path,
    *,
    factory: Any = None,
    default_runtime: str = "opencode",
) -> tuple[RunExportService, RunStore, Any]:
    from workflow_controller.store.event_store import EventStore

    store = RunStore(tmp_path / ".workflow-controller" / "events.db")
    event_store = EventStore(tmp_path / ".workflow-controller" / "events.db")
    service = RunExportService(
        run_store=store,
        event_store=event_store,
        run_root=lambda run_id: tmp_path / ".workflow-controller" / "runs" / run_id,
        factory_lookup=lambda fmt: factory,
        guarded_call=_sync_call,
        default_runtime=default_runtime,
    )
    return service, store, event_store


async def _sync_call(fmt: str, fn: Any, *args: Any) -> Any:
    return fn(*args)


async def _add_session_roster(
    event_store: Any,
    sid: str,
    native: str,
    name: str = "",
    config_json: str = "{}",
) -> None:
    await event_store.upsert_roster(
        {
            "id": sid,
            "name": name or sid,
            "role": "executor",
            "state": "idle",
            "native_session_id": native,
            "config_json": config_json,
            "created_at": "2026-10-19T10:00:00",
            "last_activity": "2026-10-19T10:00:00",
        }
    )


def test_iso_utc_interprets_naive_as_local_and_converts() -> None:
    """naive 时间按宿主本地时间换算成 UTC（非贴标签）；aware 时间做时区换算。"""
    from datetime import datetime

    expected = datetime(2026, 10, 14, 22, 16, 37, 850412).astimezone(UTC).isoformat()
    assert _iso_utc("2026-10-14T22:16:37.850412") == expected
    assert _iso_utc(datetime(2026, 10, 14, 22, 16, 37, 850412)) == expected
    assert _iso_utc("2026-10-14T22:16:37+09:00") == "2026-10-14T13:16:37+00:00"
    assert _iso_utc(None) is None
    assert _iso_utc("garbage") == "garbage"


async def test_export_deterministic_and_honest(tmp_path: Path) -> None:
    """两次导出字节一致；转录缺席/会话不可解析时成员保留、complete=false。"""
    service, store, event_store = _make_service(tmp_path)
    run = await store.create("", "", kind=RunKind.CONTAINER)
    await store.attach_session(run.run_id, "s1", role="executor")
    await store.attach_session(run.run_id, "s2", role="executor")  # 无名册行的成员
    await event_store.store_run_events_batch(
        run.run_id,
        [
            ("process_exit", {"proc_id": "p1", "returncode": 0, "tail": ["done"]}),
        ],
        context="node=x",
    )
    await _add_session_roster(event_store, "s1", "ses_native1", name="worker")

    first = await service.export_run(run.run_id, archive=True)
    second = await service.export_run(run.run_id)
    bundle = Path(first["path"])
    assert first["exportDataComplete"] is False
    assert (bundle / "workflow.json").read_bytes() == (
        Path(second["path"]) / "workflow.json"
    ).read_bytes()
    with zipfile.ZipFile(first["archive"]) as zf:
        assert "workflow.json" in zf.namelist()
    doc = json.loads((bundle / "workflow.json").read_text(encoding="utf-8"))
    members = {m["id"]: m for m in doc["data"]["members"]}
    # 转录源缺席：sessionId 已知、location 降级为 null
    assert members["s_s1"]["sessionId"] == "ses_native1"
    assert members["s_s1"]["location"] is None
    # 名册缺行：native 不可解析，成员保留
    assert members["s_s2"]["sessionId"] is None
    process = next(m for m in members.values() if m["kind"] == "process")
    assert process["output"] == {"text": "done"}
    assert "sessionId" not in process and "location" not in process


async def test_fallback_export_includes_handoff_relations(tmp_path: Path) -> None:
    service, store, event_store = _make_service(tmp_path)
    run = await store.create("", "", kind=RunKind.CONTAINER)
    await store.attach_session(run.run_id, "s1", role="executor")
    await store.attach_session(run.run_id, "s2", role="executor")
    await event_store.add_handoff_relation("s1", "s2")
    assert event_store.get_successors_sync("s1")

    result = await service.export_run(run.run_id)
    document = json.loads((Path(result["path"]) / "workflow.json").read_text())
    assert result["relations"] == 1
    assert document["data"]["relations"] == [
        {
            "from": "s_s1",
            "to": "s_s2",
            "type": "handoff",
        }
    ]


async def test_codex_missing_rollout_degrades_to_missing_session(
    tmp_path: Path,
) -> None:
    service, store, event_store = _make_service(tmp_path, default_runtime="codex_cli")
    run = await store.create("", "", kind=RunKind.CONTAINER)
    await store.attach_session(run.run_id, "s1", role="executor")
    await _add_session_roster(
        event_store,
        "s1",
        "thread-1",
        config_json=json.dumps({"runtime": "codex_cli", "cwd": str(tmp_path)}),
    )

    result = await service.export_run(run.run_id)

    assert result["missing_sessions"] == ["thread-1"]
    assert result["exportDataComplete"] is False
    document = json.loads(
        (Path(result["path"]) / "workflow.json").read_text(encoding="utf-8"),
    )
    assert document["data"]["members"][0]["location"] is None


def test_codex_rollout_is_copied_without_sqlite(tmp_path: Path) -> None:
    rollout = tmp_path / "rollout-thread-1.jsonl"
    rollout.write_text('{"type":"session_meta"}\n', encoding="utf-8")

    written = getattr(RunExportService, "_write_session_unit")(
        "thread-1",
        TranscriptSource("codex", "codex-jsonl", rollout),
        tmp_path / "sessions",
    )

    assert written is True
    output = tmp_path / "sessions" / "thread-1.native" / "thread-1.jsonl"
    assert output.read_text(encoding="utf-8") == '{"type":"session_meta"}\n'


async def test_export_bundles_schema(tmp_path: Path) -> None:
    """导出包带 workflow.schema.json（与内置权威逐字一致），zip 也带上。"""
    service, store, _ = _make_service(tmp_path)
    run = await store.create("", "", kind=RunKind.CONTAINER)
    result = await service.export_run(run.run_id, archive=True)
    bundle = Path(result["path"])
    schema_path = bundle / "workflow.schema.json"
    assert schema_path.is_file()
    assert schema_path.read_text(encoding="utf-8") == WORKFLOW_BUNDLE_SCHEMA_TEXT
    with zipfile.ZipFile(result["archive"]) as zf:
        assert "workflow.schema.json" in zf.namelist()


async def test_export_refuses_implicit_and_pending(tmp_path: Path) -> None:
    service, store, _ = _make_service(tmp_path)
    run = await store.create("", "", kind=RunKind.CONTAINER)
    await store.update_status(run.run_id, RunStatus.PENDING)
    with pytest.raises(RunExportRefusedError):
        await service.export_run(run.run_id)
    implicit = await store.mint_implicit(session_id="sx", role="executor", label="l")
    with pytest.raises(RunExportRefusedError):
        await service.export_run(implicit)


async def test_validator_accepts_minimal_bundle(tmp_path: Path) -> None:
    """极简结构样例（含嵌套/跨层端点/双会话单元）通过校验。"""
    doc = json.loads(FIXTURE.read_text(encoding="utf-8"))
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "workflow.json").write_text(
        json.dumps(doc, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_bundle_schema(bundle)
    for location in _collect_locations(doc):
        sid = location.split("/")[-1].removesuffix(".native")
        unit = bundle / location
        unit.mkdir(parents=True)
        (unit / f"{sid}.jsonl").write_text("", encoding="utf-8")
        (unit / f"{sid}.meta.json").write_text("{}", encoding="utf-8")
    validate_bundle(bundle)


def test_validator_accepts_omitted_workflow_collections(tmp_path: Path) -> None:
    base = {
        "schema": "workflow-bundle",
        "version": 1,
        "id": "r1",
        "kind": "workflow",
        "name": "root",
        "startedAt": None,
        "finishedAt": None,
        "data": {"exportDataComplete": True},
    }
    nested = {
        "id": "w1",
        "kind": "workflow",
        "name": "sub",
        "startedAt": None,
        "finishedAt": None,
        "data": {},
    }
    for i, data in enumerate(
        (
            {"exportDataComplete": True},
            {"exportDataComplete": True, "members": [nested]},
            {"exportDataComplete": True, "relations": []},
        )
    ):
        bundle = tmp_path / f"bundle-{i}"
        bundle.mkdir()
        (bundle / "workflow.json").write_text(
            json.dumps({**base, "data": data}),
            encoding="utf-8",
        )
        _write_bundle_schema(bundle)
        validate_bundle(bundle)


def test_validator_accepts_omitted_names(tmp_path: Path) -> None:
    doc = {
        "schema": "workflow-bundle",
        "version": 1,
        "id": "r1",
        "kind": "workflow",
        "startedAt": None,
        "finishedAt": None,
        "data": {
            "exportDataComplete": True,
            "members": [
                {"id": "p1", "kind": "process", "startedAt": None, "finishedAt": None},
                {
                    "id": "s1",
                    "kind": "session",
                    "startedAt": None,
                    "finishedAt": None,
                    "sessionId": None,
                    "location": None,
                },
                {"id": "w1", "kind": "workflow", "startedAt": None, "finishedAt": None, "data": {}},
            ],
            "relations": [],
        },
    }
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "workflow.json").write_text(json.dumps(doc), encoding="utf-8")
    _write_bundle_schema(bundle)
    validate_bundle(bundle)


def _collect_locations(doc: dict[str, Any]) -> set[str]:
    locations = {m.get("location") for m in doc["data"]["members"] if m.get("location")}
    for m in doc["data"]["members"]:
        if m["kind"] == "workflow":
            locations |= _collect_locations(m)
    return locations


def _write_bundle_schema(bundle_dir: Path) -> None:
    """随包写权威 schema 副本（手动构造 bundle 的测试夹具用）。"""
    (bundle_dir / "workflow.schema.json").write_text(
        WORKFLOW_BUNDLE_SCHEMA_TEXT,
        encoding="utf-8",
    )


def _bundle_base_doc() -> dict[str, Any]:
    """最小合法 workflow bundle 顶层文档（调用方自行改写 data.members）。"""
    return {
        "schema": "workflow-bundle",
        "version": 1,
        "id": "r1",
        "kind": "workflow",
        "name": "n",
        "startedAt": None,
        "finishedAt": None,
        "data": {"exportDataComplete": True, "members": [], "relations": []},
    }


def _write_schemaless_bundle(tmp_path: Path, doc: dict[str, Any]) -> Path:
    """按 locations 写出不带 schema 副本的旧式 bundle（doc 为完整 workflow.json 内容）。"""
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "workflow.json").write_text(
        json.dumps(doc, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_locations_bundle_units(bundle, doc)
    return bundle


def _write_locations_bundle_units(bundle: Path, doc: dict[str, Any]) -> None:
    """按 doc 中出现的 location 逐个补齐会话单元（jsonl + meta）。"""
    for location in _collect_locations(doc):
        sid = location.split("/")[-1].removesuffix(".native")
        unit = bundle / location
        unit.mkdir(parents=True)
        (unit / f"{sid}.jsonl").write_text("", encoding="utf-8")
        (unit / f"{sid}.meta.json").write_text("{}", encoding="utf-8")


def _write_fixture_bundle(tmp_path: Path, doc: dict[str, Any]) -> Path:
    """写出带 schema 副本与全部 location 单元的 fixture bundle。"""
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "workflow.json").write_text(json.dumps(doc), encoding="utf-8")
    _write_bundle_schema(bundle)
    _write_locations_bundle_units(bundle, doc)
    return bundle


def test_validator_falls_back_to_builtin_schema_when_missing(tmp_path: Path) -> None:
    """旧包（schema 引入前）缺 workflow.schema.json：回退内置权威校验，不硬拒。"""
    doc = json.loads(FIXTURE.read_text(encoding="utf-8"))
    validate_bundle(_write_schemaless_bundle(tmp_path, doc))


def test_validator_accepts_legacy_on_exhaust_in_old_bundle(tmp_path: Path) -> None:
    """旧包缺 schema 且 retryPolicy.onExhaust 含历史值 fail：回退内置权威仍通过。"""
    doc = json.loads(FIXTURE.read_text(encoding="utf-8"))
    doc["data"]["members"].append(
        {
            "id": "m_legacy",
            "kind": "process",
            "name": "legacy",
            "startedAt": None,
            "finishedAt": None,
            "nodeId": "n",
            "generation": 0,
            "attempt": 0,
            "annotation": {"role": "execute"},
            "retryPolicy": {"maxRetries": 2, "onExhaust": "fail"},
        }
    )
    validate_bundle(_write_schemaless_bundle(tmp_path, doc))


def _member(kind: str, **fields: Any) -> dict[str, Any]:
    """最小成员（id=m1、name=x）；kind 之外的差异字段经 fields 并入。"""
    return {
        "id": "m1",
        "kind": kind,
        "name": "x",
        "startedAt": None,
        "finishedAt": None,
    } | fields


def _doc_with_single_member(base: dict[str, Any], member: dict[str, Any]) -> dict[str, Any]:
    """base 深拷贝后把 data.members 换成单个给定成员。"""
    doc = json.loads(json.dumps(base))
    doc["data"]["members"] = [member]
    return doc


async def test_validator_rejects_broken_bundles(tmp_path: Path) -> None:
    base = _bundle_base_doc()
    _expect_invalid(tmp_path, json.loads(json.dumps(base)) | {"id": None})
    dangling = json.loads(json.dumps(base))
    dangling["data"]["relations"] = [{"from": "ghost", "to": "m1"}]
    _expect_invalid(tmp_path, dangling)
    dup_id = json.loads(json.dumps(base))
    dup_id["data"]["members"] = [_member("process"), _member("process")]
    _expect_invalid(tmp_path, dup_id)
    degraded = _doc_with_single_member(base, _member("session", sessionId="s", location=None))
    _expect_invalid(tmp_path, degraded)  # 单元缺席时 complete 须为 false
    escape = _doc_with_single_member(
        base, _member("session", sessionId="s", location="../escape.native")
    )
    _expect_invalid(tmp_path, escape)
    invalid_state = _doc_with_single_member(base, _member("process", executionState="unknown"))
    _expect_invalid(tmp_path, invalid_state)
    old_running_state = json.loads(json.dumps(invalid_state))
    old_running_state["data"]["members"][0]["executionState"] = "running"
    _expect_invalid(tmp_path, old_running_state)
    carried = _doc_with_single_member(base, _member("process", executionState="carried"))
    _expect_invalid(tmp_path, carried)
    no_carried = _doc_with_single_member(
        base, _member("process", executionState="finished", resultFromGeneration=0)
    )
    _expect_invalid(tmp_path, no_carried)
    bad_verification = _doc_with_single_member(
        base, _member("process", verification={"type": "manual"})
    )
    _expect_invalid(tmp_path, bad_verification)
    invalid_relation = json.loads(json.dumps(dup_id))
    invalid_relation["data"]["members"][1]["id"] = "m2"
    invalid_relation["data"]["relations"] = [
        {"from": "m1", "to": "m2", "type": "custom-transition"}
    ]
    _expect_invalid(tmp_path, invalid_relation)
    for field in ("members", "relations"):
        invalid_collection = json.loads(json.dumps(base))
        invalid_collection["data"][field] = None
        _expect_invalid(tmp_path, invalid_collection)
    missing_complete = json.loads(json.dumps(base))
    del missing_complete["data"]["exportDataComplete"]
    _expect_invalid(tmp_path, missing_complete)  # 根缺 exportDataComplete → 代码拒绝


def _expect_invalid(tmp_path: Path, doc: dict[str, Any]) -> None:
    bundle = tmp_path / f"broken-{abs(hash(json.dumps(doc, sort_keys=True)))}"
    bundle.mkdir()
    (bundle / "workflow.json").write_text(
        json.dumps(doc, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_bundle_schema(bundle)
    with pytest.raises(RunExportFailedError):
        validate_bundle(bundle)


def test_schema_const_matches_code_constants() -> None:
    """schema 文件里 const 锁的 schema/version 与导出代码常量一致。"""
    envelope = next(
        branch
        for branch in WORKFLOW_BUNDLE_SCHEMA["allOf"]
        if "schema" in (branch.get("properties") or {})
    )
    assert envelope["properties"]["schema"]["const"] == BUNDLE_SCHEMA
    assert envelope["properties"]["version"]["const"] == BUNDLE_VERSION


def test_validator_rejects_mismatched_bundled_schema(tmp_path: Path) -> None:
    """包内 schema 与内置权威不一致时拒绝（防打包错 schema 版本）。"""
    doc = json.loads(FIXTURE.read_text(encoding="utf-8"))
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "workflow.json").write_text(
        json.dumps(doc, ensure_ascii=False),
        encoding="utf-8",
    )
    (bundle / "workflow.schema.json").write_text("{}", encoding="utf-8")
    with pytest.raises(RunExportFailedError, match="不一致"):
        validate_bundle(bundle)


def test_validator_accepts_free_annotation_and_optional_graph_fields(tmp_path: Path) -> None:
    """通用 schema 不从 annotation 推断 graph 字段或 role 必填。"""
    base = _bundle_base_doc()
    base["data"]["members"] = [
        {
            "id": "m1",
            "kind": "session",
            "name": "x",
            "startedAt": None,
            "finishedAt": None,
            "sessionId": "s",
            "location": "sessions/s.native",
            "annotation": {"graph": {"nodeId": 123}, "type": [1, None]},
        }
    ]
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "workflow.json").write_text(
        json.dumps(base, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_bundle_schema(bundle)
    (bundle / "sessions" / "s.native").mkdir(parents=True)
    (bundle / "sessions" / "s.native" / "s.jsonl").write_text("", encoding="utf-8")
    (bundle / "sessions" / "s.native" / "s.meta.json").write_text("{}", encoding="utf-8")
    validate_bundle(bundle)


@pytest.mark.parametrize(
    "field,value",
    [
        ("nodeId", "build"),
        ("generation", 0),
        ("attempt", 1),
        ("attemptResult", "succeeded"),
        ("verification", {"type": "agent"}),
        ("verification", {"type": "script"}),
        ("retryPolicy", {}),
        ("runOnFailure", False),
        ("executionState", "in_progress"),
        ("executionState", "finished"),
        ("executionState", "carried"),
        ("executionState", "pending"),
        ("resultFromGeneration", 0),
    ],
)
def test_workflow_member_accepts_graph_fields(
    tmp_path: Path,
    field: str,
    value: Any,
) -> None:
    doc = json.loads(FIXTURE.read_text(encoding="utf-8"))
    doc["data"]["members"][3][field] = value
    if field == "executionState" and value == "carried":
        doc["data"]["members"][3]["resultFromGeneration"] = 0
    elif field == "resultFromGeneration":
        doc["data"]["members"][3]["executionState"] = "carried"
    validate_bundle(_write_fixture_bundle(tmp_path, doc))


def test_workflow_member_annotation_remains_free(tmp_path: Path) -> None:
    doc = json.loads(FIXTURE.read_text(encoding="utf-8"))
    workflow = doc["data"]["members"][3]
    workflow["annotation"] = {
        "role": ["any", None],
        "nodeId": {"arbitrary": True},
    }
    workflow["name"] = None
    validate_bundle(_write_fixture_bundle(tmp_path, doc))


@pytest.mark.parametrize(
    "field,value",
    [
        ("role", "execute"),
        ("sessionId", "ses_gen"),
        ("location", "sessions/ses_gen.native"),
        ("output", {"text": "subgraph loaded"}),
    ],
)
def test_workflow_schema_allows_extension_fields(
    tmp_path: Path,
    field: str,
    value: Any,
) -> None:
    doc = json.loads(FIXTURE.read_text(encoding="utf-8"))
    doc["data"]["members"][3][field] = value
    validate_bundle(_write_fixture_bundle(tmp_path, doc))


def test_workflow_contribution_rejects_own_session(tmp_path: Path) -> None:
    service, _, _ = _make_service(tmp_path)
    member = WorkflowMemberContribution(
        id="sub",
        kind="workflow",
        name="sub",
        session_id="source-session",
    )
    with pytest.raises(RunExportFailedError, match="不得直接携带会话或输出"):
        getattr(service, "_member_doc")(
            member,
            {"sub": RunExportContribution()},
            {"source-session": "native-session"},
        )


def test_contribution_fields_cannot_override_structural_fields() -> None:
    with pytest.raises(RunExportFailedError, match="保留字段"):
        getattr(RunExportService, "_add_fields")({"id": "m1"}, {"data": {}})
    with pytest.raises(RunExportFailedError, match="保留字段"):
        getattr(RunExportService, "_add_fields")({"from": "m1", "to": "m2"}, {"annotation": {}})
    with pytest.raises(RunExportFailedError, match="保留字段"):
        getattr(RunExportService, "_add_fields")({"id": "m1"}, {"role": "worker"})


def test_validator_rejects_null_rollback_to_node_id(tmp_path: Path) -> None:
    """没有回滚目标时省略字段；显式 null 不符合契约。"""
    base = _bundle_base_doc()
    base["data"]["members"] = [
        {
            "id": "m1",
            "kind": "process",
            "name": "x",
            "startedAt": None,
            "finishedAt": None,
            "nodeId": "n",
            "generation": 0,
            "attempt": 1,
            "annotation": {"role": "execute"},
            "retryPolicy": {
                "maxRetries": 2,
                "onExhaust": "stop",
                "rollbackToNodeId": None,
            },
        }
    ]
    _expect_invalid(tmp_path, base)
    del base["data"]["members"][0]["retryPolicy"]["rollbackToNodeId"]
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "workflow.json").write_text(
        json.dumps(base, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_bundle_schema(bundle)
    validate_bundle(bundle)


async def test_workflow_run_without_contributor_marks_incomplete(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """workflow run 无贡献者时兜底包不得伪装完整：complete=false。"""
    service, store, _ = _make_service(tmp_path)

    class BareFactory:  # 注册了 format 但无导出面（版本偏斜形态）
        format = "graph-yaml@1"

    def _factory_lookup(fmt):
        return BareFactory() if fmt else None

    monkeypatch.setattr(service, "_factory_lookup", _factory_lookup)
    run = await store.create("wf.yaml", "graph-yaml@1", kind=RunKind.WORKFLOW)
    await store.attach_session(run.run_id, "s1", role="executor")
    await store.save_driver_state(run.run_id, {"workflow": "demo", "state": {}})

    result = await service.export_run(run.run_id)
    assert result["ok"] and result["exportDataComplete"] is False
    doc = json.loads(
        (Path(result["path"]) / "workflow.json").read_text(encoding="utf-8"),
    )
    assert doc["data"]["members"][0]["annotation"]["role"] == "executor"
    assert "role" not in doc["data"]["members"][0]


async def test_toy_contributor_neutral_protocol(tmp_path: Path) -> None:
    """玩具方言（annotation.pipeline）走全链路：核心写包零方言知识。"""

    class ToyFactory:
        format = "pipeline@1"

        @staticmethod
        def export_contribution(request: RunExportRequest) -> RunExportContribution:
            members = tuple(
                WorkflowMemberContribution(
                    id=f"s_{m.session_id}",
                    kind="session",
                    name=m.session_id,
                    session_id=m.session_id,
                    fields={"priority": i},
                    annotation={"role": "worker", "pipeline": {"step": i}},
                )
                for i, m in enumerate(request.members)
            )
            relations = tuple(
                WorkflowRelationContribution(
                    from_id=members[i].id,
                    to_id=members[i + 1].id,
                    annotation={"pipeline": {"edge": i, "kind": "custom-transition"}},
                )
                for i in range(len(members) - 1)
            )
            return RunExportContribution(name="toy", members=members, relations=relations)

    service, store, event_store = _make_service(tmp_path, factory=ToyFactory())
    run = await store.create("p.yaml", "pipeline@1", kind=RunKind.WORKFLOW)
    await store.attach_session(run.run_id, "s1", role="executor")
    await store.attach_session(run.run_id, "s2", role="executor")
    await _add_session_roster(event_store, "s1", "ses_a", name="a")
    await _add_session_roster(event_store, "s2", "ses_b", name="b")
    result = await service.export_run(run.run_id)
    doc = json.loads(
        (Path(result["path"]) / "workflow.json").read_text(encoding="utf-8"),
    )
    assert doc["name"] == "toy"
    assert doc["data"]["exportDataComplete"] is False  # 快照缺席的降级面
    assert [m["annotation"]["pipeline"]["step"] for m in doc["data"]["members"]] == [0, 1]
    assert [m["annotation"]["role"] for m in doc["data"]["members"]] == [
        "worker",
        "worker",
    ]
    assert [m["priority"] for m in doc["data"]["members"]] == [0, 1]
    assert "type" not in doc["data"]["relations"][0]
    assert doc["data"]["relations"][0]["annotation"] == {
        "pipeline": {"edge": 0, "kind": "custom-transition"},
    }


async def test_concurrent_exports_same_run_serialize(tmp_path: Path) -> None:
    """同 run 并发导出被 run 级锁串行：不互毁产物、不留中间残目录。"""
    service, store, event_store = _make_service(tmp_path)
    run = await store.create("", "", kind=RunKind.CONTAINER)
    await store.attach_session(run.run_id, "s1", role="executor")
    await _add_session_roster(event_store, "s1", "ses_c1", name="c")
    results = await asyncio.gather(
        *[service.export_run(run.run_id, archive=i % 2 == 0) for i in range(6)]
    )
    assert all(r["ok"] for r in results)
    bundle = Path(results[0]["path"])
    validate_bundle(bundle)
    export_root = bundle.parent
    assert not list(export_root.glob(".tmp-*"))
    assert not list(export_root.glob(".old-*"))
    assert getattr(service, "_export_locks") == {}


async def test_cancelled_export_propagates_and_next_export_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """取消不进清理分支：CancelledError 原样透出，残留 tmp 由下次导出清理。"""
    service, store, _ = _make_service(tmp_path)
    run = await store.create("", "", kind=RunKind.CONTAINER)

    def _cancelled(_bundle_dir: Path) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(
        "workflow_controller.core.run_export.validate_bundle",
        _cancelled,
    )
    with pytest.raises(asyncio.CancelledError):
        await service.export_run(run.run_id)
    stale = (
        tmp_path
        / ".workflow-controller"
        / "runs"
        / run.run_id
        / "export"
        / f".tmp-{run.run_id}-bundle"
    )
    assert stale.exists()

    monkeypatch.undo()
    result = await service.export_run(run.run_id)
    assert result["ok"]
    assert not stale.exists()


def test_publish_bundle_rollback_keeps_old_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """发布 rename 失败回滚：旧包回位，目标位不留空窗。"""
    tmp_dir = tmp_path / "tmp"
    (tmp_dir / "sessions").mkdir(parents=True)
    (tmp_dir / "workflow.json").write_text("new", encoding="utf-8")
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "workflow.json").write_text("old", encoding="utf-8")
    backup_dir = tmp_path / "old"

    real_rename = Path.rename

    def _rename(self: Path, target: str | Path) -> Path:
        if self == tmp_dir:
            raise OSError("rename boom")
        return real_rename(self, target)

    monkeypatch.setattr(Path, "rename", _rename)
    with pytest.raises(OSError):
        _publish_bundle(tmp_dir, bundle_dir, backup_dir)
    monkeypatch.undo()
    assert (bundle_dir / "workflow.json").read_text(encoding="utf-8") == "old"
    assert not backup_dir.exists()
    assert tmp_dir.exists()  # 未就位的 tmp 交调用方清理


async def test_zip_failure_maps_to_export_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """zip 写失败映射 RunExportFailedError（路由 503），已发布目录包保留。"""

    def _boom(_bundle_dir: Path, _archive_path: Path) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("workflow_controller.core.run_export.write_bundle_zip", _boom)
    service, store, _ = _make_service(tmp_path)
    run = await store.create("", "", kind=RunKind.CONTAINER)
    with pytest.raises(RunExportFailedError, match="目录包已落"):
        await service.export_run(run.run_id, archive=True)
    bundle = (
        tmp_path / ".workflow-controller" / "runs" / run.run_id / "export" / f"{run.run_id}-bundle"
    )
    assert (bundle / "workflow.json").is_file()
    assert not list(bundle.parent.glob(".tmp-*"))


async def test_export_lock_slots_reclaimed_across_runs(tmp_path: Path) -> None:
    """锁条目不随 run 数单调积压：导出结束（含失败）即回收，仅在途时保留。"""
    service, store, _ = _make_service(tmp_path)
    for _ in range(3):
        run = await store.create("", "", kind=RunKind.CONTAINER)
        assert (await service.export_run(run.run_id))["ok"]
    pending = await store.create("", "", kind=RunKind.CONTAINER)
    await store.update_status(pending.run_id, RunStatus.PENDING)
    with pytest.raises(RunExportRefusedError):
        await service.export_run(pending.run_id)
    assert getattr(service, "_export_locks") == {}


async def test_session_unit_oserror_maps_to_export_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """单元生产写盘 OSError 包装 RunExportFailedError（路由 503），tmp 照常清理。"""

    def _boom(*_args: Any) -> bool:
        raise PermissionError("copy denied")

    monkeypatch.setattr(RunExportService, "_write_session_unit", _boom)
    service, store, event_store = _make_service(tmp_path)
    run = await store.create("", "", kind=RunKind.CONTAINER)
    await store.attach_session(run.run_id, "s1", role="executor")
    await _add_session_roster(event_store, "s1", "ses_c1", name="c")
    with pytest.raises(RunExportFailedError, match="写盘失败"):
        await service.export_run(run.run_id)
    export_root = tmp_path / ".workflow-controller" / "runs" / run.run_id / "export"
    assert not list(export_root.glob(".tmp-*"))
    assert not (export_root / f"{run.run_id}-bundle").exists()


def test_tree_counts_sessions() -> None:
    """_tree_counts 的 sessions 计数：只数 session 成员（含嵌套 workflow）。"""
    from workflow_controller.core.run_export import _tree_counts

    doc = {
        "data": {
            "members": [
                {"id": "s1", "kind": "session"},
                {"id": "p1", "kind": "process"},
                {
                    "id": "w1",
                    "kind": "workflow",
                    "data": {
                        "members": [
                            {"id": "s2", "kind": "session"},
                            {"id": "s3", "kind": "session"},
                        ],
                        "relations": [],
                    },
                },
            ],
            "relations": [{"from": "s1", "to": "p1"}],
        }
    }
    members, relations, sessions = _tree_counts(doc)
    assert members == 5  # workflow 成员自身也计入（s1/p1/w1/s2/s3）
    assert relations == 1
    assert sessions == 3
