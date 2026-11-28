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

"""配置领域路由 — 全局/项目配置、最近项目与目录选择器。"""

from __future__ import annotations

import os
import string
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from workflow_controller.api.http.routes.common import (
    compose_config_dict,
    get_config_section_providers,
    get_core_api,
)

router = APIRouter()


@router.get("/config")
async def get_config(request: Request) -> dict[str, Any]:
    return compose_config_dict(request, get_core_api(request).config.get_config_dict())


@router.put("/config")
async def update_config(body: dict[str, Any], request: Request) -> dict[str, Any]:
    api = get_core_api(request)
    payload = dict(body)
    # 领域段先弹出：与内核字段一并校验通过后才落盘，坏值请求不落任何段
    pending_sections = [
        (provider, payload.pop(name))
        for name, provider in get_config_section_providers(request).items()
        if isinstance(payload.get(name), dict)
    ]
    try:
        # 领域段先校验（只校验不落盘）：内核 update_config 内含落盘，
        # 任一段坏值都要在首次落盘前拒绝，请求零持久化
        for provider, partial in pending_sections:
            provider.validate_config_section(partial)
        # 内核赋值校验抛的 ValidationError 是 ValueError 子类，一并转 400
        result = api.config.update_config(payload)
        for provider, partial in pending_sections:
            provider.save_config_section(partial)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if "reviewer" in payload:
        await api.config.restart_reviewer()
    if "brand_name" in payload:
        # openapi schema 首次生成后缓存，作废才按新 title 重生成（/docs、/openapi.json 免重启）
        request.app.title = result["brand_name"]
        request.app.openapi_schema = None
    return compose_config_dict(request, result)


@router.post("/config/load-from-dir")
async def load_config_from_dir(body: dict[str, Any], request: Request) -> dict[str, Any]:
    """选目录:只在全局 config 记下 project_dir 指针(不再把项目 yaml 灌进全局)。
    项目配置由 /api/project-config 读写项目 yaml。
    """
    api = get_core_api(request)
    result = api.config.load_from_dir(body.get("dir", ""))
    await api.config.restart_reviewer()
    return compose_config_dict(request, result)


@router.post("/config/save")
async def save_config(request: Request) -> dict[str, Any]:
    get_core_api(request).config.save()
    return {"ok": True}


@router.get("/project-config")
async def get_project_config(
    request: Request, directory: str | None = Query(default=None, alias="dir")
) -> dict[str, Any]:
    """读当前项目(或 ?dir= 指定)workflow-controller.yaml 的项目配置(runtime/starting_prompt/
    system_prompt/loop/reviewer.task_goal;不含 stages)。
    """
    return get_core_api(request).config.get_project_config(directory)


@router.put("/project-config")
async def update_project_config(
    body: dict[str, Any],
    request: Request,
    directory: str | None = Query(default=None, alias="dir"),
) -> dict[str, Any]:
    """写回当前项目(或 ?dir= 指定)workflow-controller.yaml 的项目配置(保键合并,不动 stages)。
    body 里 reviewer.task_goal 变了 → 重启 reviewer。
    """
    api = get_core_api(request)
    result = api.config.update_project_config(directory, body)
    if "reviewer" in body:
        await api.config.restart_reviewer()
    return result


# ── 最近项目（后端单一来源，前端只读 SSE config.recent_dirs）──────────────────


@router.post("/recent-dirs")
async def add_recent_dir(body: dict[str, Any], request: Request) -> dict[str, Any]:
    """添加目录到最近项目列表（写 recent_dirs.json）。

    校验单点在 ConfigAPI.add_recent_dir（存在性/绝对性/系统临时区），route 只
    透传结果：error 值 ``path not found``（前端 toast「该路径在当前主机不存在」）
    与 ``transient dir``（临时区目录，前端 toast 说明原因）。
    """
    path = (body.get("dir") or body.get("path") or "").strip()
    if not path:
        return {
            "error": "dir is required",
            "recent_dirs": get_core_api(request).config.get_recent_dirs(),
        }
    dirs, err = get_core_api(request).config.add_recent_dir(path)
    if err:
        return {"error": err, "recent_dirs": dirs}
    return {"ok": True, "recent_dirs": dirs}


@router.delete("/recent-dirs")
async def remove_recent_dir(body: dict[str, Any], request: Request) -> dict[str, Any]:
    """从最近项目列表移除目录（真删，即时生效，无 SSE 闪回）。"""
    path = (body.get("dir") or body.get("path") or "").strip()
    if not path:
        return {
            "error": "dir is required",
            "recent_dirs": get_core_api(request).config.get_recent_dirs(),
        }
    return {"ok": True, "recent_dirs": get_core_api(request).config.remove_recent_dir(path)}


# Sentinel path representing the Windows "drive list" layer (above any drive
# root). Lets the directory picker climb past C:\ to pick another drive.
_DRIVES_SENTINEL = "::drives"


def _windows_drives() -> list[str]:
    """Existing Windows drive roots, e.g. ['C:\\\\', 'D:\\\\']."""
    return [f"{c}:\\" for c in string.ascii_uppercase if Path(f"{c}:\\").exists()]


def _parent_of(p: Path) -> str | None:
    """Parent path for the picker's 'Up' button.

    A drive/filesystem root has ``p.parent == p``. On Windows that root is a
    drive (C:\\) whose parent is the synthetic drive-list layer; on POSIX the
    root is ``/`` and there is nothing above it.
    """
    if p.parent != p:
        return str(p.parent)
    return _DRIVES_SENTINEL if os.name == "nt" else None


@router.get("/list-directory")
async def list_directory(path: str = "", show_hidden: str = "false") -> dict[str, Any]:
    """Return directory contents for the given path."""
    from os.path import expanduser
    from pathlib import Path as _Path

    _show = show_hidden.lower() == "true"

    # Roots layer: Windows -> drive list (C:, D:); POSIX -> the single root "/".
    if path == _DRIVES_SENTINEL:
        if os.name == "nt":
            entries = [{"name": d, "is_dir": True, "path": d} for d in _windows_drives()]
            return {"path": _DRIVES_SENTINEL, "parent": None, "entries": entries}
        path = "/"  # POSIX: fall through and list "/" normally

    if not path:
        path = expanduser("~")

    p = _Path(path).expanduser().resolve()
    if not p.exists():
        return {"path": str(p), "parent": None, "entries": [], "error": "not found"}
    if not p.is_dir():
        p = p.parent

    entries = []
    try:
        for child in sorted(p.iterdir(), key=lambda c: (not c.is_dir(), c.name.lower())):
            if not _show and child.name.startswith("."):
                continue
            entries.append(
                {
                    "name": child.name,
                    "is_dir": child.is_dir(),
                    "path": str(child),
                }
            )
    except PermissionError:
        return {
            "path": str(p),
            "parent": _parent_of(p),
            "entries": [],
            "error": "permission denied",
        }

    return {"path": str(p), "parent": _parent_of(p), "entries": entries}
