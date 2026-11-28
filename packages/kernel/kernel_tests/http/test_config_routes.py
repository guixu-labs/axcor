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

"""config 路由响应组合：内核配置段 + 装配期注入的领域配置段。"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from workflow_controller.api.http.routes.config_routes import get_config, update_config
from workflow_controller.config.settings import WatchdogConfig

if TYPE_CHECKING:
    from collections.abc import Callable


def _provider(reads: list[dict], saves: list[dict]) -> SimpleNamespace:
    """领域段服务桩：记录读写调用。"""
    return SimpleNamespace(
        read_config_section=lambda: (
            reads.append({}),
            {"enabled": True, "snapshot_paths": []},
        )[1],
        validate_config_section=lambda partial: None,
        save_config_section=lambda partial: (
            saves.append(partial),
            {"enabled": partial.get("enabled", ""), "snapshot_paths": []},
        )[1],
    )


def _config_update_request(
    update: Callable[[dict], dict], provider: SimpleNamespace
) -> SimpleNamespace:
    """构造带领域配置段的 PUT /config 请求桩。"""
    api = SimpleNamespace(
        config=SimpleNamespace(update_config=update, restart_reviewer=AsyncMock(return_value=None))
    )
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(api=api, config_section_providers={"sample_domain": provider})
        )
    )


async def test_config_response_composes_domain_sections() -> None:
    """GET /config 在内核配置上组合领域段；无注入时不多出键。"""
    api = SimpleNamespace(config=SimpleNamespace(get_config_dict=lambda: {"runtime": "opencode"}))
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(api=api)))
    resp = await get_config(request)
    assert resp == {"runtime": "opencode"}  # 无注入段：原样返回

    request.app.state.config_section_providers = {"sample_domain": _provider([], [])}
    resp = await get_config(request)
    assert resp["sample_domain"] == {"enabled": True, "snapshot_paths": []}


async def test_config_update_dispatches_domain_section() -> None:
    """PUT /config 把领域段分派给段所有者保存，内核更新不再收到领域段。"""
    saves: list[dict] = []
    updates: list[dict] = []
    request = _config_update_request(
        lambda body: (updates.append(body), {"watchdog": {"enabled": True}})[1],
        _provider([], saves),
    )

    resp = await update_config(
        {"watchdog": {"enabled": True}, "sample_domain": {"enabled": True}},
        request,
    )
    assert saves == [{"enabled": True}]  # 领域段已分派
    assert updates == [{"watchdog": {"enabled": True}}]  # 内核更新不再收到领域段
    assert resp["sample_domain"]["enabled"] is True


async def test_config_update_brand_name_syncs_swagger_title() -> None:
    """PUT /config 改品牌名：app.title 同步并作废 openapi 缓存，Swagger 标题免重启生效。"""
    api = SimpleNamespace(
        config=SimpleNamespace(
            update_config=lambda body: {"brand_name": "Ascendbot"},
            restart_reviewer=AsyncMock(return_value=None),
        )
    )
    app = SimpleNamespace(
        state=SimpleNamespace(api=api),
        title="Workflow Controller",
        openapi_schema={"info": {"title": "Workflow Controller"}},
    )

    await update_config({"brand_name": "Ascendbot"}, SimpleNamespace(app=app))
    assert app.title == "Ascendbot"
    assert app.openapi_schema is None  # 缓存不作废则 /openapi.json 仍是旧标题

    # 非 brand 请求不触碰标题
    app2 = SimpleNamespace(
        state=SimpleNamespace(
            api=SimpleNamespace(
                config=SimpleNamespace(
                    update_config=lambda body: {"watchdog": {"enabled": True}},
                    restart_reviewer=AsyncMock(return_value=None),
                )
            )
        ),
        title="Workflow Controller",
        openapi_schema={"info": {"title": "Workflow Controller"}},
    )
    await update_config({"watchdog": {"enabled": True}}, SimpleNamespace(app=app2))
    assert app2.title == "Workflow Controller"
    assert app2.openapi_schema == {"info": {"title": "Workflow Controller"}}


async def test_config_update_domain_validation_error_rejects_before_kernel_persist() -> None:
    """PUT /config 领域段坏值：在内核落盘前拒绝，update_config（内含 save）不执行。"""
    saves: list[dict] = []
    updates: list[dict] = []
    api = SimpleNamespace(
        config=SimpleNamespace(
            update_config=lambda body: (updates.append(body), {"watchdog": {"enabled": True}})[1],
            restart_reviewer=AsyncMock(return_value=None),
        )
    )
    bad_provider = SimpleNamespace(
        read_config_section=lambda: {},
        validate_config_section=lambda partial: (_ for _ in ()).throw(
            ValueError("领域配置校验失败")
        ),
        save_config_section=lambda partial: (saves.append(partial), {})[1],
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(api=api, config_section_providers={"sample_domain": bad_provider})
        )
    )

    with pytest.raises(HTTPException) as exc_info:
        await update_config(
            {"watchdog": {"enabled": True}, "sample_domain": {"typo": 1}},
            request,
        )
    assert exc_info.value.status_code == 400
    assert "领域配置校验失败" in exc_info.value.detail
    assert updates == []  # 内核未更新（未落盘）
    assert saves == []  # 领域段未保存


async def test_config_update_kernel_validation_error_returns_400_and_skips_domain_save() -> None:
    """PUT /config 内核段坏值（赋值校验的 ValidationError）转 400，领域段不保存。"""
    saves: list[dict] = []

    def _kernel_rejects(body: dict) -> dict:
        # 与内核 update_config 同源：坏值触发赋值校验抛 pydantic ValidationError
        return WatchdogConfig.model_validate(body["watchdog"])

    request = _config_update_request(_kernel_rejects, _provider([], saves))

    with pytest.raises(HTTPException) as exc_info:
        await update_config(
            {
                "watchdog": {"subagent_timeout_seconds": "abc"},
                "sample_domain": {"enabled": True},
            },
            request,
        )
    assert exc_info.value.status_code == 400
    assert saves == []  # 内核校验失败在先，领域段不落盘
