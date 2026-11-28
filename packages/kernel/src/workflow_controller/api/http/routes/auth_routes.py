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

"""Auth 领域路由 — key 状态、激活、验证与全局认证设置。"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from workflow_controller.api.auth_api import ProviderInUseError
from workflow_controller.api.http.routes.common import get_core_api

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/auth/status")
async def auth_status(request: Request) -> dict[str, Any]:
    return get_core_api(request).auth.key_status()


@router.get("/auth/keys")
async def auth_get_keys(request: Request) -> dict[str, Any]:
    return get_core_api(request).auth.keys_for_ui()


@router.post("/auth/keys")
async def auth_set_keys(body: dict[str, Any], request: Request) -> dict[str, Any]:
    try:
        return await get_core_api(request).auth.set_keys_validated(body.get("keys", []))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/auth/keys/{provider_index}")
async def auth_delete_key(provider_index: int, request: Request) -> dict[str, Any]:
    try:
        deleted = await get_core_api(request).auth.delete_provider(provider_index)
    except ProviderInUseError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail="provider not found or configuration changed")
    return {"ok": True}


@router.post("/auth/activate")
async def auth_activate(body: dict[str, Any], request: Request) -> dict[str, Any]:
    """按序号直接激活 provider。"""
    return get_core_api(request).auth.activate_provider(body.get("index", -1))


@router.post("/auth/test")
async def auth_test(request: Request) -> dict[str, Any]:
    """重跑当前 provider 的 worker/reviewer 校验。"""
    return await get_core_api(request).auth.test_active()


@router.post("/auth/keys/{provider_index}/test")
async def auth_test_provider(provider_index: int, request: Request) -> dict[str, Any]:
    """强制重测一行 Provider。"""
    try:
        return await get_core_api(request).auth.test_provider(provider_index)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


class AuthTestKeyRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "key": "your-provider-key",
                    "model": "claude-opus-5-5",
                    "base_url": "https://provider.example.com/anthropic",
                    "runtime": "claude_cli",
                }
            ]
        }
    )

    key: str = Field("", description="供应商 API 密钥，执行探测时必须提供。")
    model: str = Field("", description="供应商模型 ID。")
    base_url: str = Field("", description="供应商 API 基础地址，Codex CLI 必须显式填写。")
    runtime: str | None = Field(
        None, description="Worker 运行时，例如 claude_cli。探测时必须提供。"
    )
    model_catalog_path: str | None = Field(
        None, description="Codex CLI 自定义模型目录 JSON 的绝对路径。"
    )


class AuthTestCapability(BaseModel):
    capability: str = Field(examples=["worker/claude_cli"])
    status: Literal["untested", "valid", "invalid"]
    fingerprint: str
    checked_at: str | None = Field(json_schema_extra={"format": "date-time"})
    error: str
    thinking_mode: str
    thinking_modes: list[str]


class AuthTestKeyResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "success": True,
                    "runtime": "claude_cli",
                    "worker": {
                        "capability": "worker/claude_cli",
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
                        "checked_at": "2026-11-18T01:00:00Z",
                        "error": "Reviewer probe failed",
                        "thinking_mode": "",
                        "thinking_modes": [],
                    },
                    "error": "",
                }
            ]
        }
    )

    success: bool | None = Field(None, description="Worker 或 Reviewer 任一通过即为 true。")
    runtime: str | None = Field(None, description="实际用于 Worker 探测的运行时。")
    worker: AuthTestCapability | None = Field(None, description="Worker 探测结果。")
    reviewer: AuthTestCapability | None = Field(None, description="Reviewer 探测结果。")
    error: str = Field("", description="Worker 错误或探测无法执行的原因。")
    fingerprint: str | None = None
    provider_kind: str | None = None
    catalog_status: (
        Literal["builtin", "preset", "custom_required", "custom_invalid", "custom"] | None
    ) = None
    model_catalog_name: str | None = None
    model_catalog_path: str | None = None


@router.post(
    "/auth/test-key",
    response_model_exclude_unset=True,
    response_description="供应商测试结果",
    responses={
        400: {"description": "测试失败"},
    },
)
async def auth_test_key(body: AuthTestKeyRequest, request: Request) -> AuthTestKeyResponse:
    """试测 key/model/base_url 组合，不写入 provider 列表。"""
    try:
        result = await get_core_api(request).auth.validate_candidate(
            body.key.strip(),
            body.model.strip(),
            body.base_url.strip(),
            (body.runtime or "").strip() or None,
            model_catalog_path=(body.model_catalog_path or "").strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        return AuthTestKeyResponse.model_validate(result)
    except ValidationError as exc:
        logger.exception("auth test-key response contract validation failed")
        raise HTTPException(
            status_code=500,
            detail="供应商试测结果与响应结构不匹配，请查看服务端日志。",
        ) from exc


@router.get("/auth/settings")
async def auth_get_settings(request: Request) -> dict[str, Any]:
    return get_core_api(request).auth.global_settings()


@router.put("/auth/settings")
async def auth_set_settings(body: dict[str, Any], request: Request) -> dict[str, Any]:
    return get_core_api(request).auth.set_global_settings(body)
