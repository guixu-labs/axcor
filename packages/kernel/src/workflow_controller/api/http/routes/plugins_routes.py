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

"""插件领域路由 — 清单、启停、安装与配置读写。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from workflow_controller.api.http.routes.common import get_core_api

router = APIRouter()

log = logging.getLogger(__name__)


@router.get("/plugins")
async def list_plugins(request: Request) -> dict[str, Any]:
    return {"plugins": get_core_api(request).plugin.list_plugins()}


@router.post("/plugins/{name}/toggle")
async def toggle_plugin(name: str, request: Request) -> dict[str, Any]:
    """翻转插件启停；启用后把插件 router 交运行期注册回调挂载。"""
    plugin_api = get_core_api(request).plugin
    result = plugin_api.toggle(name)
    if result is None:
        return {"error": "plugin not found"}
    if result["enabled"]:
        router_obj = plugin_api.get_web_router(name)
        if router_obj is not None:
            # 注册回调由应用的装配层注入（见 mounting.register_plugin_routers），
            # 路由层不反向依赖装配层
            register = getattr(request.app.state, "register_runtime_plugin_router", None)
            if register is not None:
                register(name, router_obj)
            else:
                # 未经 register_plugin_routers 装配（如单测自建 app）时缺回调：插件已启用
                # 但 router 挂不上，必须留痕，不能静默吞掉
                log.warning(
                    "plugin '%s' enabled but register_runtime_plugin_router is missing "
                    "on app.state (app not assembled via register_plugin_routers); "
                    "plugin web router not mounted",
                    name,
                )
    return result


@router.post("/plugins/install")
async def install_plugin(body: dict[str, Any], request: Request) -> dict[str, Any]:
    source_dir = (body.get("path") or "").strip()
    if not source_dir:
        return {"error": "no path provided"}
    return get_core_api(request).plugin.install(source_dir)


@router.get("/plugins/{name}/config")
async def get_plugin_config(name: str, request: Request) -> dict[str, Any]:
    try:
        cfg_data = get_core_api(request).plugin.read_config(name)
    except Exception:
        return {"error": "discovery failed"}
    return cfg_data if cfg_data is not None else {"error": "plugin not found"}


@router.put("/plugins/{name}/config")
async def save_plugin_config(name: str, body: dict[str, Any], request: Request) -> dict[str, Any]:
    enabled = bool(body["enabled"]) if "enabled" in body else None
    try:
        result = get_core_api(request).plugin.write_config(name, body.get("fields", {}), enabled)
    except Exception:
        return {"error": "discovery failed"}
    return result if result is not None else {"error": "plugin not found"}


@router.post("/plugins/breather/template")
async def upload_breather_template(request: Request, filename: str) -> dict[str, str]:
    """将 UTF-8 模板写入守护进程启动目录，返回可直接用于配置的路径。"""
    if (
        filename != Path(filename).name
        or filename in (".", "..")
        or Path(filename).suffix.lower() not in (".md", ".txt")
    ):
        raise HTTPException(status_code=400, detail="只接受 .md 或 .txt 文件名")
    limit = 10 * 1024 * 1024
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > limit:
                raise HTTPException(status_code=400, detail="指令文件必须非空且不超过 10 MiB")
        except ValueError:
            pass
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            raise HTTPException(status_code=400, detail="指令文件必须非空且不超过 10 MiB")
    if not body:
        raise HTTPException(status_code=400, detail="指令文件必须非空且不超过 10 MiB")
    try:
        body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="模板必须采用 UTF-8 编码") from exc
    destination = Path.cwd() / filename
    # 允许覆盖同名文件：内置指令下载文件名固定（workflow-*.md），
    # 先下载→编辑→再次上传同名文件必须能落盘，否则用户会被 409 卡住。
    with destination.open("wb") as target:
        target.write(body)
    return {"path": str(destination.resolve())}


@router.get("/plugins/breather/builtin-instructions/{kind}")
async def download_breather_instructions(kind: str, request: Request) -> Response:
    instructions = get_core_api(request).plugin.builtin_breather_instructions(kind)
    if instructions is None:
        raise HTTPException(status_code=404, detail="未知指令类型")
    filename, content = instructions
    return Response(
        content=content,
        media_type="text/markdown; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
