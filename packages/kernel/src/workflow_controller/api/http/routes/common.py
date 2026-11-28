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

"""领域路由共用的小工具：CoreAPI 获取与配置响应组合。"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, cast

if TYPE_CHECKING:
    from fastapi import Request

    from workflow_controller.api.core_api import CoreAPI


def get_core_api(request: Request) -> CoreAPI:
    """从 app.state 取内核门面。"""
    return cast("CoreAPI", request.app.state.api)


class ConfigSectionProvider(Protocol):
    """领域配置段服务：由领域模块实现、装配期注入，路由不直接依赖领域模块。"""

    def read_config_section(self) -> dict[str, Any]:
        raise NotImplementedError

    def validate_config_section(self, partial: dict[str, Any]) -> None:
        raise NotImplementedError

    def save_config_section(self, partial: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


def get_config_section_providers(request: Request) -> dict[str, ConfigSectionProvider]:
    """取装配期注入的领域配置段服务（段名 → 服务）。"""
    return getattr(request.app.state, "config_section_providers", {})


def compose_config_dict(request: Request, config_dict: dict[str, Any]) -> dict[str, Any]:
    """组合内核配置与领域配置段：GET/PUT /config 与 SSE config_update 共用。"""
    for section_name, provider in get_config_section_providers(request).items():
        config_dict[section_name] = provider.read_config_section()
    return config_dict
