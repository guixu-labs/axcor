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

"""Auth 路由经 AuthAPI 门面的应答形状断言。"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from workflow_controller.api.auth_api import AuthAPI
from workflow_controller.api.http.routes import auth_routes
from workflow_controller.session.session import SessionState


class _RotatorStub:
    """覆盖 auth 路由触达的轮换器方法面。"""

    def __init__(self, provider_count: int = 1) -> None:
        self._provider_count = provider_count
        self.model_catalog_path = None
        self.candidate_args = None
        self.saved = None

    @staticmethod
    def get_key_status_dict():
        return {
            "mode": "API",
            "reset_at": None,
            "providers": [{"index": 0}],
            "active_provider_index": 0,
        }

    @staticmethod
    def get_api_keys_for_ui():
        return [{"label": "k1"}]

    @staticmethod
    async def set_keys_validated(keys):
        if keys == ["bad"]:
            raise ValueError("invalid key entry")

    @staticmethod
    def activate_provider(index):
        return index == 0

    @staticmethod
    def get_active_provider_index():
        return 0

    def delete_provider(self, index):
        return 0 <= index < self._provider_count

    @staticmethod
    def get_global_settings_dict():
        return {"auto_switch": True}

    def get_provider_count(self):
        return self._provider_count

    async def validate_provider(self, index, force):
        return {"success": True, "validated": [index], "force": force}

    async def validate_candidate(
        self,
        key,
        model,
        base_url,
        runtime,
        *,
        force,
        model_catalog_path=None,
    ):
        self.model_catalog_path = model_catalog_path
        self.candidate_args = (key, model, base_url, runtime, force)
        return {"success": True, "runtime": runtime, "provider_kind": None}

    def set_global_settings(self, **kwargs):
        self.saved = kwargs


def _request(rotator, sessions=None) -> SimpleNamespace:
    # 哑桩：路由层只断言应答形状，pool 回写细节由单元层测试负责
    async def persist(session) -> None:
        pass

    pool = SimpleNamespace(list_sessions=lambda: sessions or [], persist=persist)
    api = SimpleNamespace(auth=AuthAPI(SimpleNamespace(auth_rotator=rotator, pool=pool)))
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(api=api)))


def _auth_app(rotator) -> FastAPI:
    """生产装配的 auth 路由挂到真实 FastAPI 应用上，走框架序列化层。"""
    app = FastAPI()
    app.state.api = SimpleNamespace(
        auth=AuthAPI(
            SimpleNamespace(
                auth_rotator=rotator,
                pool=SimpleNamespace(list_sessions=lambda: []),
            )
        )
    )
    app.include_router(auth_routes.router, prefix="/api/v1")
    return app


async def test_status_and_keys_passthrough() -> None:
    request = _request(_RotatorStub())
    assert await auth_routes.auth_status(request) == {
        "mode": "API",
        "reset_at": None,
        "providers": [{"index": 0}],
        "active_provider_index": 0,
    }
    assert await auth_routes.auth_get_keys(request) == {"keys": [{"label": "k1"}]}


async def test_rotator_absent_returns_legacy_empty_payloads() -> None:
    request = _request(None)
    assert await auth_routes.auth_status(request) == {
        "mode": "API",
        "reset_at": None,
        "providers": [],
        "active_provider_index": 0,
    }
    assert await auth_routes.auth_get_keys(request) == {"keys": []}
    assert await auth_routes.auth_get_settings(request) == {}
    assert await auth_routes.auth_set_keys({"keys": []}, request) == {
        "error": "auth rotator not available",
    }


async def test_set_keys_maps_value_error_to_400() -> None:
    request = _request(_RotatorStub())
    assert await auth_routes.auth_set_keys({"keys": ["k"]}, request) == {"ok": True}
    with pytest.raises(HTTPException) as ei:
        await auth_routes.auth_set_keys({"keys": ["bad"]}, request)
    assert ei.value.status_code == 400
    assert ei.value.detail == "invalid key entry"


async def test_delete_key_maps_missing_and_unavailable() -> None:
    assert await auth_routes.auth_delete_key(0, _request(_RotatorStub())) == {"ok": True}
    with pytest.raises(HTTPException) as missing:
        await auth_routes.auth_delete_key(1, _request(_RotatorStub()))
    assert missing.value.status_code == 404
    with pytest.raises(HTTPException) as unavailable:
        await auth_routes.auth_delete_key(0, _request(None))
    assert unavailable.value.status_code == 503


async def test_delete_key_returns_conflict_for_running_session() -> None:
    session = SimpleNamespace(
        id="s1",
        state=SessionState.RUNNING,
        config=SimpleNamespace(provider_index=0, reviewer=None),
    )
    with pytest.raises(HTTPException) as conflict:
        await auth_routes.auth_delete_key(0, _request(_RotatorStub(), [session]))
    assert conflict.value.status_code == 409


async def test_delete_key_returns_conflict_for_running_session_index_shift() -> None:
    """活跃会话引用更高索引：删除低位使其下移，同样 409。"""
    session = SimpleNamespace(
        id="s1",
        state=SessionState.RUNNING,
        config=SimpleNamespace(provider_index=1, reviewer=None),
    )
    with pytest.raises(HTTPException) as conflict:
        await auth_routes.auth_delete_key(0, _request(_RotatorStub(provider_count=2), [session]))
    assert conflict.value.status_code == 409


async def test_delete_key_allows_running_session_lower_index() -> None:
    """活跃会话引用低于被删索引：不受移位影响，放行。"""
    session = SimpleNamespace(
        id="s1",
        state=SessionState.RUNNING,
        config=SimpleNamespace(provider_index=0, reviewer=None),
    )
    assert await auth_routes.auth_delete_key(
        1, _request(_RotatorStub(provider_count=2), [session])
    ) == {"ok": True}


async def test_delete_key_allows_idle_session_reference() -> None:
    session = SimpleNamespace(
        id="s1",
        state=SessionState.IDLE,
        config=SimpleNamespace(provider_index=0, reviewer=None),
    )
    assert await auth_routes.auth_delete_key(0, _request(_RotatorStub(), [session])) == {"ok": True}


async def test_activate_rejects_invalid_index() -> None:
    request = _request(_RotatorStub())
    assert await auth_routes.auth_activate({"index": -1}, request) == {
        "ok": False,
        "error": "invalid index",
    }
    assert await auth_routes.auth_activate({"index": "x"}, request) == {
        "ok": False,
        "error": "invalid index",
    }
    assert await auth_routes.auth_activate({"index": 0}, request) == {
        "ok": True,
        "active_index": 0,
    }


async def test_test_reports_no_provider_configured() -> None:
    request = _request(_RotatorStub(provider_count=0))
    assert await auth_routes.auth_test(request) == {
        "success": False,
        "error": "No provider configured",
    }


async def test_test_validates_active_provider_with_force() -> None:
    rotator = _RotatorStub()
    assert await auth_routes.auth_test(_request(rotator)) == {
        "success": True,
        "validated": [0],
        "force": True,
    }


async def test_retest_validates_selected_provider_and_rejects_missing_index() -> None:
    request = _request(_RotatorStub(provider_count=2))
    assert await auth_routes.auth_test_provider(1, request) == {
        "success": True,
        "validated": [1],
        "force": True,
    }
    with pytest.raises(HTTPException) as ei:
        await auth_routes.auth_test_provider(2, request)
    assert ei.value.status_code == 404


async def test_test_key_requires_key_and_runtime() -> None:
    request = _request(_RotatorStub())
    result = await auth_routes.auth_test_key(
        auth_routes.AuthTestKeyRequest(model="m", base_url="b", runtime="opencode"),
        request,
    )
    assert result.model_dump(exclude_unset=True) == {"success": False, "error": "Key is required"}
    with pytest.raises(HTTPException) as ei:
        await auth_routes.auth_test_key(
            auth_routes.AuthTestKeyRequest(key="k", model="m", base_url="b", runtime=""),
            request,
        )
    assert ei.value.status_code == 400
    assert ei.value.detail == "runtime is required"


async def test_test_key_delegates_candidate_without_persisting() -> None:
    rotator = _RotatorStub()
    request = _request(rotator)
    result = await auth_routes.auth_test_key(
        auth_routes.AuthTestKeyRequest(
            key=" k ",
            model=" m ",
            base_url=" b ",
            runtime=" opencode ",
            model_catalog_path=" ~/m.json ",
        ),
        request,
    )
    assert result.model_dump(exclude_unset=True) == {
        "success": True,
        "runtime": "opencode",
        "provider_kind": None,
    }
    assert rotator.candidate_args == ("k", "m", "b", "opencode", True)
    assert rotator.model_catalog_path == "~/m.json"


async def test_settings_roundtrip() -> None:
    rotator = _RotatorStub()
    request = _request(rotator)
    assert await auth_routes.auth_get_settings(request) == {"auto_switch": True}
    assert await auth_routes.auth_set_settings({"auto_switch": False}, request) == {
        "ok": True,
    }
    assert rotator.saved == {"auto_switch": False}


class _FullPayloadRotatorStub(_RotatorStub):
    """返回与真实 _probe_validations 相同的 payload（可选字段已省略）。"""

    async def validate_candidate(
        self,
        key,
        model,
        base_url,
        runtime,
        *,
        force,
        model_catalog_path=None,
    ):
        return {
            "success": True,
            "runtime": runtime or "claude_sdk",
            "fingerprint": "example-fingerprint",
            "worker": {
                "capability": "worker/claude_sdk",
                "status": "valid",
                "fingerprint": "example-fingerprint",
                "checked_at": "2026-11-18T01:00:00Z",
                "error": "",
                "thinking_mode": "",
                "thinking_modes": [],
            },
            "reviewer": {
                "capability": "reviewer",
                "status": "invalid",
                "fingerprint": "example-fingerprint",
                "checked_at": None,
                "error": "Reviewer probe failed",
                "thinking_mode": "",
                "thinking_modes": [],
            },
            "error": "",
        }


class _SchemaDriftRotatorStub(_RotatorStub):
    """返回偏离响应结构的内部 payload（新增枚举值且缺必填字段）。"""

    async def validate_candidate(
        self,
        key,
        model,
        base_url,
        runtime,
        *,
        force,
        model_catalog_path=None,
    ):
        return {"success": True, "runtime": runtime, "worker": {"status": "unexpected"}}


def test_test_key_http_body_omits_unset_optional_fields() -> None:
    """真实 HTTP 响应经框架序列化：未设置的可选字段省略，能力项显式 null 保留。"""
    response = TestClient(_auth_app(_FullPayloadRotatorStub())).post(
        "/api/v1/auth/test-key",
        json={"key": "k", "model": "m", "base_url": "b", "runtime": "claude_sdk"},
    )
    assert response.status_code == 200
    body = response.json()
    assert "catalog_status" not in body
    assert "model_catalog_name" not in body
    assert "model_catalog_path" not in body
    assert "provider_kind" not in body
    assert body["worker"]["checked_at"] == "2026-11-18T01:00:00Z"
    assert body["reviewer"]["checked_at"] is None


def test_test_key_maps_schema_drift_to_explicit_500() -> None:
    """内部校验结果偏离响应结构时映射为明确 500，而非未处理的异常。"""
    response = TestClient(_auth_app(_SchemaDriftRotatorStub())).post(
        "/api/v1/auth/test-key",
        json={"key": "k", "model": "m", "base_url": "b", "runtime": "claude_sdk"},
    )
    assert response.status_code == 500
    assert "响应结构" in response.json()["detail"]
