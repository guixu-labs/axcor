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

"""RunsAPI 全端点约定：FakeDriver 全链路、错误码与事件游标。"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from workflow_controller.api.http.routes import runs_routes
from workflow_controller.api.run_api import RunAPI
from workflow_controller.core.run_coordinator import RunConfig, RunCoordinator
from workflow_controller.core.runs import RunStore
from workflow_controller.core.session_run_affiliation import SessionRunAffiliationService
from workflow_controller.plugin.run_driver import SessionSpec, StartSession
from workflow_controller.session.events import Event, EventType
from workflow_controller.store.event_store import EventStore
from workflow_controller.store.run_events_cursor import RunEventsCursor

from ._runs_api_stubs import (
    PoolFactsMixin,
    StubBus,
    StubPool,
    StubSession,
)

# ── 测试替身 ──────────────────────────────────────────────────────


class FakeSO(PoolFactsMixin):
    """SO 替身：慢成员常驻不结束；控制原语记录并翻态。"""

    def __init__(self, affiliation, bus, project_dir, event_store) -> None:
        self.pool = StubPool()
        self.affiliation = affiliation
        self.bus = bus
        self.default_project_dir = project_dir
        self.event_router = SimpleNamespace(default_store=event_store)
        self.started: list = []
        self.paused: list[str] = []
        self.resumed: list[str] = []
        self.stopped: list[str] = []
        self.injected: list[tuple[str, str]] = []

    async def start_session_with_config(self, cfg):
        s = StubSession(f"s{len(self.started) + 1}", cfg.name)
        self.pool.sessions[s.id] = s
        self.started.append(cfg)
        await self.affiliation.attach(s.id, cfg.run_id, role=cfg.role)
        return s

    async def pause_session(self, sid: str) -> None:
        self._flip(sid, "idle", "no", "user_pause")
        self.paused.append(sid)

    async def resume_session(self, sid: str) -> None:
        self._flip(sid, "running", "yes", None)
        self.resumed.append(sid)

    async def stop_session(self, sid: str) -> None:
        self._flip(sid, "stopped", "no", "user_stop")
        self.stopped.append(sid)

    async def inject_prompt(self, sid: str, prompt: str) -> None:
        self.injected.append((sid, prompt))

    def _flip(self, sid: str, state: str, loop: str, reason: str | None) -> None:
        s = self.pool.get(sid)
        if s is not None:
            s.state, s.loop_status, s.stop_reason = state, loop, reason


class ApiDriver:
    """起一个慢成员常驻；记录控制通知与驱动操作。"""

    def __init__(self) -> None:
        self.controls: list = []
        self.ops: list[str] = []

    @staticmethod
    def decide(world, notifications):
        if not world.sessions and world.in_flight == 0:
            return [
                StartSession(
                    spec=SessionSpec(name="slow", prompt="p", cwd="/tmp"),
                    context="node=a",
                )
            ]
        return []

    @staticmethod
    def projection():
        return {"schema_version": 1, "units": [{"id": "a", "state": "running"}]}

    @staticmethod
    def snapshot():
        return {"schema_version": 1, "state": {}, "pending_timers": []}

    def on_control(self, control) -> None:
        self.controls.append(control)

    def handle_op(self, op, args):
        self.ops.append(op)
        return {"echo": op, "args": dict(args)}


class ApiFactory:
    format = "fake@1"

    def __init__(self, driver: ApiDriver) -> None:
        self.driver = driver

    def create(self, run, program_path, params):
        return self.driver

    def restore(self, run, snapshot, sessions):
        return self.driver


class Harness:
    def __init__(self, tmp_path) -> None:
        db = tmp_path / "events.db"
        self.store = RunStore(db)
        self.event_store = EventStore(db)
        self.affiliation = SessionRunAffiliationService(self.store)
        self.bus = StubBus()
        self.so = FakeSO(self.affiliation, self.bus, tmp_path, self.event_store)
        self.driver = ApiDriver()
        self.coord = RunCoordinator(
            orchestrator=self.so,
            run_store=self.store,
            bus=self.bus,
            driver_factories={"fake@1": ApiFactory(self.driver)},
            affiliation=self.affiliation,
        )
        self.program = tmp_path / "prog.yaml"
        self.program.write_text("format: fake@1\n", encoding="utf-8")
        app = FastAPI()
        app.state.api = SimpleNamespace(run=RunAPI(self.coord, RunEventsCursor(self.event_store)))
        app.include_router(runs_routes.router, prefix="/api/v1/runs")
        self.client = AsyncClient(transport=ASGITransport(app=app), base_url="http://t")

    async def start_workflow(self) -> str:
        run = await self.coord.start_run(RunConfig(format="fake@1"), self.program)
        return run.run_id

    async def eventually(self, pred, timeout_s: float = 5.0) -> None:
        deadline = time.monotonic() + timeout_s
        while True:
            ok = pred()
            if asyncio.iscoroutine(ok):
                ok = await ok
            if ok:
                return
            if time.monotonic() >= deadline:
                raise AssertionError("条件等待超时")
            await asyncio.sleep(0.01)

    async def first_session_id(self, run_id: str) -> str:
        ids = await self.store.list_session_ids(run_id)
        assert ids, "成员尚未出生"
        return ids[0]

    async def wait_member_context(self, run_id: str, context: str) -> None:
        """等待成员出生且 context 落账：归属行先挂、分派方随后补记，慢机有窗口。"""

        async def ready() -> bool:
            sessions = await self.coord.list_run_sessions(run_id)
            return any(s.context == context for s in sessions)

        await self.eventually(ready)


# ── 启动与查询 ────────────────────────────────────────────────────


# run 详情与成员摘要的一级字段形态（结构快照，防字段意外增删）
_RUN_DETAIL_KEYS = frozenset(
    {
        "run_id",
        "kind",
        "format",
        "status",
        "program_ref",
        "params",
        "label",
        "work_dir",
        "max_parallel",
        "pause_reason",
        "created_at",
        "finished_at",
        "reopened_at",
        "error",
        "member_count",
        "running_count",
    }
)
_SESSION_DETAIL_KEYS = frozenset(
    {"session_id", "role", "context", "state", "stop_reason", "in_flight"}
)


class TestStartAndQuery:
    async def test_start_missing_program_returns_readable_error(self, tmp_path):
        h = Harness(tmp_path)
        resp = await h.client.post(
            "/api/v1/runs",
            json={
                "format": "fake@1",
                "program_path": "missing.yaml",
                "work_dir": str(tmp_path),
            },
        )
        assert resp.status_code == 422
        assert "程序文件不存在" in resp.json()["detail"]

    async def test_start_workflow_contract(self, tmp_path):
        h = Harness(tmp_path)
        resp = await h.client.post(
            "/api/v1/runs",
            json={
                "format": "fake@1",
                "program_ref": "p@1",
                "program_path": str(h.program),
                "work_dir": str(tmp_path / "wd"),
                "label": "demo",
                "max_parallel": 4,
            },
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["kind"] == "workflow" and body["status"] == "running"
        assert body["format"] == "fake@1"
        run_id = body["run_id"]

        await h.eventually(lambda: h.so.started)  # 成员出生
        await h.wait_member_context(run_id, "node=a")  # context 补记完成后再取详情
        resp = await h.client.get(f"/api/v1/runs/{run_id}")
        assert resp.status_code == 200
        got = resp.json()
        assert set(got) == {"run", "sessions"}
        run = got["run"]
        assert set(run) == _RUN_DETAIL_KEYS
        assert run["label"] == "demo" and run["max_parallel"] == 4
        assert run["program_ref"] == "p@1" and run["params"] == {}
        assert run["status"] == "running" and run["kind"] == "workflow"
        [session] = got["sessions"]
        assert set(session) == _SESSION_DETAIL_KEYS
        assert session["role"] == "executor" and session["context"] == "node=a"
        assert session["state"] == "running" and not session["in_flight"]

    async def test_start_non_json_body_422(self, tmp_path):
        """空 body / 非 JSON body 统一 422，不冒 500（请求体解析公共路径）。"""
        h = Harness(tmp_path)
        r = await h.client.post(
            "/api/v1/runs", content=b"", headers={"content-type": "application/json"}
        )
        assert r.status_code == 422
        r = await h.client.post(
            "/api/v1/runs", content=b"{bad", headers={"content-type": "application/json"}
        )
        assert r.status_code == 422
        r = await h.client.post("/api/v1/runs/x/steer", content=b"nope")
        assert r.status_code == 422

    async def test_start_without_format_creates_container(self, tmp_path):
        h = Harness(tmp_path)
        resp = await h.client.post("/api/v1/runs", json={"label": "box"})
        assert resp.status_code == 201
        body = resp.json()
        assert body["kind"] == "container" and body["format"] == ""
        assert body["status"] == "running"

    async def test_start_input_errors(self, tmp_path):
        h = Harness(tmp_path)
        r = await h.client.post(
            "/api/v1/runs",
            json={
                "format": "ghost@1",
                "program_path": str(h.program),
                "work_dir": str(tmp_path / "wd"),
            },
        )
        assert r.status_code == 422 and "没有驱动工厂" in r.json()["detail"]
        r = await h.client.post("/api/v1/runs", json={"format": "fake@1"})
        assert r.status_code == 422  # 缺 program_path
        r = await h.client.post("/api/v1/runs", json={"program_path": str(h.program)})
        assert r.status_code == 422 and "work_dir" in r.json()["detail"]
        r = await h.client.post(
            "/api/v1/runs",
            json={
                "program_ref": "p",
                "program_path": str(h.program),
                "work_dir": str(tmp_path / "wd"),
            },
        )
        assert r.status_code == 201  # format 从程序文件头读出

    async def test_start_rejects_work_dir_inside_run_data_root(self, tmp_path):
        h = Harness(tmp_path)
        data_root = tmp_path / ".workflow-controller" / "runs"
        r = await h.client.post(
            "/api/v1/runs",
            json={
                "format": "fake@1",
                "program_path": str(h.program),
                "work_dir": str(data_root / "r1" / "work"),
            },
        )
        assert r.status_code == 422 and "run 数据根" in r.json()["detail"]
        r = await h.client.post(
            "/api/v1/runs",
            json={"format": "fake@1", "program_path": str(h.program), "work_dir": str(data_root)},
        )
        assert r.status_code == 422  # 数据根本身同样拒绝
        r = await h.client.post(
            "/api/v1/runs",
            json={"label": "box", "work_dir": str(data_root / "w")},
        )
        assert r.status_code == 422  # 容器 run 同样拒绝
        # 数据根之外的合法路径不受影响
        r = await h.client.post(
            "/api/v1/runs",
            json={
                "program_ref": "p",
                "program_path": str(h.program),
                "work_dir": str(tmp_path / "wd"),
            },
        )
        assert r.status_code == 201

    async def test_list_filters_implicit_and_status(self, tmp_path):
        h = Harness(tmp_path)
        await h.store.mint_implicit(session_id="s-imp", role="interactive")
        box = (await h.client.post("/api/v1/runs", json={"label": "box"})).json()

        resp = await h.client.get("/api/v1/runs")
        ids = [r["run_id"] for r in resp.json()["runs"]]
        assert box["run_id"] in ids  # 默认过滤隐式

        resp = await h.client.get("/api/v1/runs", params={"include_implicit": "true"})
        body = resp.json()
        ids = [r["run_id"] for r in body["runs"]]
        assert box["run_id"] in ids and len(ids) == 2
        assert all("member_count" in r and "running_count" in r for r in body["runs"])

        resp = await h.client.get("/api/v1/runs", params={"status": "completed"})
        assert resp.json()["runs"] == []

    async def test_state_transparent_driver_projection(self, tmp_path):
        h = Harness(tmp_path)
        run_id = await h.start_workflow()
        resp = await h.client.get(f"/api/v1/runs/{run_id}/state")
        assert resp.status_code == 200
        body = resp.json()
        assert body["driver"]["schema_version"] == 1
        assert body["driver"]["units"] == [{"id": "a", "state": "running"}]
        assert body["run_id"] == run_id


# ── 组控与转向 ────────────────────────────────────────────────────


class TestControlEndpoints:
    async def test_full_control_cycle(self, tmp_path):
        h = Harness(tmp_path)
        run_id = await h.start_workflow()
        await h.eventually(lambda: h.so.started)

        r = await h.client.post(f"/api/v1/runs/{run_id}/steer", json={"message": "先做文档"})
        assert r.status_code == 202
        await h.eventually(lambda: h.driver.controls)

        r = await h.client.post(f"/api/v1/runs/{run_id}/pause")
        assert r.status_code == 202 and r.json()["status"] == "paused"
        r = await h.client.post(f"/api/v1/runs/{run_id}/pause")  # 幂等
        assert r.status_code == 202 and r.json()["status"] == "paused"
        assert h.so.paused  # 成员已被组暂停

        r = await h.client.post(f"/api/v1/runs/{run_id}/resume")
        assert r.status_code == 202 and r.json()["status"] == "running"
        assert h.so.resumed

        r = await h.client.post(f"/api/v1/runs/{run_id}/stop")
        assert r.status_code == 202 and r.json()["status"] == "stopped"
        r = await h.client.post(f"/api/v1/runs/{run_id}/stop")  # 终态重复控制 no-op
        assert r.status_code == 202

        r = await h.client.post(f"/api/v1/runs/{run_id}/restart")
        assert r.status_code == 202 and r.json()["status"] == "running"

    async def test_control_rejects_implicit_and_unknown(self, tmp_path):
        h = Harness(tmp_path)
        implicit = await h.store.mint_implicit(session_id="s-imp", role="interactive")
        r = await h.client.post(f"/api/v1/runs/{implicit}/pause")
        assert r.status_code == 409
        r = await h.client.post("/api/v1/runs/ghost/pause")
        assert r.status_code == 404
        r = await h.client.get("/api/v1/runs/ghost")
        assert r.status_code == 404


class TestResumePayload:
    """resume 的可选 payload：路由请求体约定与 payload 透传。"""

    async def test_resume_endpoint_optional_body(self, tmp_path):
        """请求体可选：带 payload 受理且通知带 payload；缺键当 None；空 body 与不带 body 照常受理。"""
        h = Harness(tmp_path)
        run_id = await h.start_workflow()
        await h.eventually(lambda: h.so.started)

        r = await h.client.post(f"/api/v1/runs/{run_id}/pause")
        assert r.status_code == 202
        r = await h.client.post(f"/api/v1/runs/{run_id}/resume", json={"payload": {"answer": "B"}})
        assert r.status_code == 202 and r.json()["status"] == "running"
        await h.eventually(lambda: any(c.kind == "resumed" for c in h.driver.controls))
        resumed = next(c for c in h.driver.controls if c.kind == "resumed")
        assert resumed.payload == {"answer": "B"}

        # body 有但缺 payload 键：当 None 照常受理
        r = await h.client.post(f"/api/v1/runs/{run_id}/pause")
        assert r.status_code == 202
        r = await h.client.post(f"/api/v1/runs/{run_id}/resume", json={"other": 1})
        assert r.status_code == 202
        await h.eventually(lambda: sum(c.kind == "resumed" for c in h.driver.controls) >= 2)
        bare = [c for c in h.driver.controls if c.kind == "resumed"][-1]
        assert bare.payload is None

        # 不带 body 的既有调用方（httpx 无 body）照常受理
        r = await h.client.post(f"/api/v1/runs/{run_id}/pause")
        assert r.status_code == 202
        r = await h.client.post(f"/api/v1/runs/{run_id}/resume")
        assert r.status_code == 202 and r.json()["status"] == "running"
        await h.coord.stop_run(run_id)

    async def test_resume_endpoint_bad_body_422(self, tmp_path):
        """坏 JSON 与 payload 非对象报 422（与 steer 同规则）；422 不改 run 记录、可再恢复。"""
        h = Harness(tmp_path)
        run_id = await h.start_workflow()
        await h.eventually(lambda: h.so.started)
        await h.client.post(f"/api/v1/runs/{run_id}/pause")

        r = await h.client.post(
            f"/api/v1/runs/{run_id}/resume",
            content=b"{bad",
            headers={"content-type": "application/json"},
        )
        assert r.status_code == 422
        r = await h.client.post(f"/api/v1/runs/{run_id}/resume", json={"payload": "x"})
        assert r.status_code == 422
        assert (await h.store.get(run_id)).status == "paused"  # 受理前拒绝，run 记录不动

        r = await h.client.post(f"/api/v1/runs/{run_id}/resume")
        assert r.status_code == 202 and r.json()["status"] == "running"
        await h.coord.stop_run(run_id)


# ── 成员操作 ──────────────────────────────────────────────────────


class TestSessionEndpoints:
    async def test_workflow_member_resume_requires_active_run_and_user_pause(self, tmp_path):
        h = Harness(tmp_path)
        run_id = await h.start_workflow()
        await h.eventually(lambda: h.so.started)
        sid = await h.first_session_id(run_id)
        url = f"/api/v1/runs/{run_id}/sessions/{sid}/resume"

        r = await h.client.post(url)
        assert r.status_code == 409
        assert sid not in h.so.resumed

        await h.client.post(f"/api/v1/runs/{run_id}/sessions/{sid}/pause")
        r = await h.client.post(url)
        assert r.status_code == 202
        assert sid in h.so.resumed

        await h.coord.stop_run(run_id)
        count = len(h.so.resumed)
        r = await h.client.post(url)
        assert r.status_code == 409
        assert len(h.so.resumed) == count

    async def test_session_inject_and_pause(self, tmp_path):
        h = Harness(tmp_path)
        run_id = await h.start_workflow()
        await h.eventually(lambda: h.so.started)
        sid = await h.first_session_id(run_id)

        r = await h.client.post(
            f"/api/v1/runs/{run_id}/sessions/{sid}/inject", json={"prompt": "hi"}
        )
        assert r.status_code == 202
        assert h.so.injected == [(sid, "hi")]

        r = await h.client.post(f"/api/v1/runs/{run_id}/sessions/{sid}/pause")
        assert r.status_code == 202

        r = await h.client.post(f"/api/v1/runs/{run_id}/sessions/ghost/pause")
        assert r.status_code == 404  # 非成员
        assert "不隶属" in r.json()["detail"]  # 404 细分：非成员文案区别于 run 不在账
        r = await h.client.post(f"/api/v1/runs/ghost/sessions/{sid}/pause")
        assert r.status_code == 404
        assert "不存在" in r.json()["detail"]


# ── 驱动通道 ──────────────────────────────────────────────────────


class TestDriverOp:
    async def test_driver_op_roundtrip(self, tmp_path):
        h = Harness(tmp_path)
        run_id = await h.start_workflow()
        r = await h.client.post(
            f"/api/v1/runs/{run_id}/driver/op",
            json={"op": "reload", "args": {"model": "m2"}},
        )
        assert r.status_code == 200
        assert r.json() == {"echo": "reload", "args": {"model": "m2"}}
        assert h.driver.ops == ["reload"]

    async def test_driver_op_input_and_not_found(self, tmp_path):
        h = Harness(tmp_path)
        r = await h.client.post("/api/v1/runs/x/driver/op", json={"args": {}})
        assert r.status_code == 422  # 缺 op
        r = await h.client.post("/api/v1/runs/x/driver/op", json={"op": "__projection__"})
        assert r.status_code == 422  # 保留名
        r = await h.client.post("/api/v1/runs/x/driver/op", json={"op": "reload"})
        assert r.status_code == 404

    async def test_driver_op_unavailable_503(self, tmp_path):
        h = Harness(tmp_path)
        box = (await h.client.post("/api/v1/runs", json={"label": "box"})).json()
        r = await h.client.post(f"/api/v1/runs/{box['run_id']}/driver/op", json={"op": "reload"})
        assert r.status_code == 503  # 无驱动运行时

    async def test_driver_op_arbitrary_error_503(self, tmp_path, monkeypatch):
        """驱动缺 handle_op（getattr 失败不经 _DriverError 转译）也回退 503 而非 500。"""
        h = Harness(tmp_path)
        run_id = await h.start_workflow()
        monkeypatch.delattr(type(h.driver), "handle_op")
        r = await h.client.post(f"/api/v1/runs/{run_id}/driver/op", json={"op": "reload"})
        assert r.status_code == 503


# ── 事件游标 ──────────────────────────────────────────────────────


async def _store_disordered_events(h, run_id, t0):
    """铺乱序事件：events 表 uuid 重放改写时间戳，run_events 表后写行持更早时间戳。"""
    for i in range(3):
        await h.event_store.store(
            Event(
                id=f"u{i}",
                session_id="s1",
                iteration=0,
                timestamp=t0 + timedelta(seconds=i + 1),
                event_type=EventType.USER_MESSAGE,
                data={"i": i},
                run_id=run_id,
            )
        )
    # uuid 重放：首行主键不变、timestamp 改写为全表最晚
    await h.event_store.store(
        Event(
            id="u0",
            session_id="s1",
            iteration=0,
            timestamp=t0 + timedelta(seconds=30),
            event_type=EventType.USER_MESSAGE,
            data={"i": 0},
            run_id=run_id,
        )
    )
    # run_events 乱序落库：后写行持更早时间戳
    await h.event_store.store_run_event(
        run_id, "process_output", {"i": 1}, timestamp=t0 + timedelta(seconds=4)
    )
    await h.event_store.store_run_event(
        run_id, "process_output", {"i": 2}, timestamp=t0 + timedelta(seconds=2)
    )


class TestEventsAndDelete:
    async def test_events_merge_and_pagination(self, tmp_path):
        h = Harness(tmp_path)
        box = (await h.client.post("/api/v1/runs", json={"label": "box"})).json()
        run_id = box["run_id"]
        await h.client.post(f"/api/v1/runs/{run_id}/steer", json={"message": "go"})
        await h.event_store.store(
            Event(
                id="u1",
                session_id="s1",
                iteration=0,
                timestamp=datetime.now(tz=UTC),
                event_type=EventType.USER_MESSAGE,
                data={"q": 1},
                run_id=run_id,
            )
        )

        r = await h.client.get(f"/api/v1/runs/{run_id}/events")
        assert r.status_code == 200
        body = r.json()
        assert body["limit"] == 200  # 默认页长随应答回传
        sources = {e["source"] for e in body["events"]}
        assert sources == {"run_events", "events"}  # 两表合并在一个流
        kinds = {(e["source"], e["event_type"]) for e in body["events"]}
        assert ("run_events", "run_steered") in kinds
        assert ("events", "user_message") in kinds

        r = await h.client.get(f"/api/v1/runs/{run_id}/events", params={"limit": 1})
        first = r.json()
        assert len(first["events"]) == 1
        assert first["limit"] == 1
        r = await h.client.get(
            f"/api/v1/runs/{run_id}/events",
            params={"limit": 1, "cursor": first["next_cursor"]},
        )
        second = r.json()
        assert len(second["events"]) == 1
        r = await h.client.get(
            f"/api/v1/runs/{run_id}/events", params={"cursor": second["next_cursor"]}
        )
        assert r.json()["events"] == []  # 游标推进无重复无遗漏
        # 请求页长超上限被钳制：应答回传实际生效值，翻页终止判据不依赖请求值
        r = await h.client.get(f"/api/v1/runs/{run_id}/events", params={"limit": 5000})
        assert r.json()["limit"] == 1000

    async def test_events_timestamp_disorder_no_loss(self, tmp_path):
        """timestamp 乱序（uuid 重放改写/乱序落库）下翻页不漏行（回归）。"""
        h = Harness(tmp_path)
        box = (await h.client.post("/api/v1/runs", json={"label": "box"})).json()
        run_id = box["run_id"]
        t0 = datetime(2026, 9, 1, 10, 0, 0)
        await _store_disordered_events(h, run_id, t0)

        seen: list[tuple[str, int]] = []
        cursor = None
        for _ in range(20):
            params: dict = {"limit": 2}
            if cursor:
                params["cursor"] = cursor
            body = (await h.client.get(f"/api/v1/runs/{run_id}/events", params=params)).json()
            if not body["events"]:
                break
            seen.extend((e["source"], e["id"]) for e in body["events"])
            cursor = body["next_cursor"]
        assert len(seen) == len(set(seen))  # 无重复
        assert set(seen) == {
            ("events", 1),
            ("events", 2),
            ("events", 3),
            ("run_events", 1),
            ("run_events", 2),
        }

    async def test_delete_idempotent_200_always(self, tmp_path):
        """DELETE 幂等 200-always：重删/不在账同判已删，不产 404。"""
        h = Harness(tmp_path)
        box = (await h.client.post("/api/v1/runs", json={"label": "box"})).json()
        run_id = box["run_id"]

        r = await h.client.delete(f"/api/v1/runs/{run_id}")
        assert r.status_code == 200
        assert r.json() == {"run_id": run_id, "deleted": True}

        r = await h.client.delete(f"/api/v1/runs/{run_id}")  # 重删同判已删
        assert r.status_code == 200 and r.json()["deleted"] is True

        r = await h.client.delete("/api/v1/runs/ghost")  # 从未存在也 200
        assert r.status_code == 200 and r.json()["deleted"] is True

        r = await h.client.get(f"/api/v1/runs/{run_id}")  # 其他端点照常 404
        assert r.status_code == 404
