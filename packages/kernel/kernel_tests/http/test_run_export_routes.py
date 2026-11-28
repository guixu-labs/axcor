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

"""run 导出路由面：错误码映射、后台任务登记与进度/取消/下载约定。"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from fastapi import HTTPException

if TYPE_CHECKING:
    from pathlib import Path

from workflow_controller.api.http.routes import runs_routes as rr
from workflow_controller.api.http.routes.runs_routes import (
    export_cancel,
    export_download,
    export_progress,
    export_run,
)
from workflow_controller.api.run_api import RunAPI, RunApiNotFoundError
from workflow_controller.core.runs import RunKind, RunStatus


def _export_task_row(
    run_id: str, *, status: str, total: int, zip_path: str | None = None, result: Any = None
) -> dict[str, Any]:
    """异步导出任务注册表的最小行；total/zip_path/result 表差异。"""
    return {
        "run_id": run_id,
        "status": status,
        "done": 1,
        "total": total,
        "error": None,
        "result": result,
        "zip_path": zip_path,
        "cancel": False,
        "done_at": None,
    }


def _make_export_zip(tmp_path: Path) -> tuple[Path, Path]:
    """export 目录下铺 zip 与同目录 keep 文件；返回 (zip, keep)。"""
    export_root = tmp_path / "export"
    zip_path = export_root / "r1-bundle.zip"
    zip_path.parent.mkdir(parents=True)
    zip_path.write_bytes(b"PK")
    keep = zip_path.parent / "keep.txt"
    keep.write_text("keep", encoding="utf-8")
    return zip_path, keep


async def test_export_endpoint_rejection_mapping() -> None:
    """路由面：404 不在账 / 409 形态拒绝（复用 core 拒绝策略而非字符串比对）。"""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    def _invoke(fake_api):
        request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(api=fake_api)),
        )
        return export_run(request, "r1")

    missing_api = SimpleNamespace(
        run=SimpleNamespace(
            get_run=AsyncMock(side_effect=RunApiNotFoundError("r1")),
        )
    )
    with pytest.raises(HTTPException) as ei:
        await _invoke(missing_api)
    assert ei.value.status_code == 404

    for kind, status in (
        (RunKind.IMPLICIT, RunStatus.COMPLETED),
        (RunKind.WORKFLOW, RunStatus.PENDING),
    ):
        refused_api = SimpleNamespace(
            run=SimpleNamespace(
                get_run=AsyncMock(return_value=SimpleNamespace(kind=kind, status=status)),
                export_refusal_reason=RunAPI.export_refusal_reason,
            )
        )
        with pytest.raises(HTTPException) as ei:
            await _invoke(refused_api)
        assert ei.value.status_code == 409


async def test_export_endpoint_starts_background_task(monkeypatch) -> None:
    """路由面：合法 run 返回 task_id 并登记异步任务（不触真实导出线程）。"""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    monkeypatch.setattr(rr, "_run_export_task", lambda *a, **k: None)
    monkeypatch.setattr(rr, "_start_exports_janitor_locked", lambda: None)
    ok_api = SimpleNamespace(
        run=SimpleNamespace(
            get_run=AsyncMock(
                return_value=SimpleNamespace(
                    kind=RunKind.WORKFLOW,
                    status=RunStatus.COMPLETED,
                )
            ),
            export_refusal_reason=RunAPI.export_refusal_reason,
        )
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(api=ok_api)))
    try:
        resp = await export_run(request, "r1")
        assert "task_id" in resp
        assert getattr(rr, "_EXPORT_TASKS")[resp["task_id"]]["run_id"] == "r1"
        assert getattr(rr, "_EXPORT_TASKS")[resp["task_id"]]["status"] == "running"
    finally:
        getattr(rr, "_EXPORT_TASKS").clear()


async def test_export_progress_cancel_download_contract(tmp_path, monkeypatch) -> None:
    """异步导出子端点：404 未登记 / 409 未就绪 / 下载头与注册表回收 / 取消不误删目录。"""
    from fastapi.responses import FileResponse

    with pytest.raises(HTTPException) as ei:
        await export_progress("r1", "missing")
    assert ei.value.status_code == 404
    with pytest.raises(HTTPException) as ei:
        await export_cancel("r1", "missing")
    assert ei.value.status_code == 404
    with pytest.raises(HTTPException) as ei:
        await export_download("r1", "missing")
    assert ei.value.status_code == 404

    getattr(rr, "_EXPORT_TASKS")["t1"] = _export_task_row("r1", status="running", total=3)
    try:
        with pytest.raises(HTTPException) as ei:
            await export_download("r1", "t1")
        assert ei.value.status_code == 409

        zip_path, keep = _make_export_zip(tmp_path)

        getattr(rr, "_EXPORT_TASKS")["t1"]["status"] = "done"
        getattr(rr, "_EXPORT_TASKS")["t1"]["zip_path"] = str(zip_path)
        getattr(rr, "_EXPORT_TASKS")["t1"]["result"] = {"members": 7, "relations": 3, "sessions": 2}

        resp = await export_download("r1", "t1")
        assert isinstance(resp, FileResponse)
        assert resp.headers["X-Export-Members"] == "7"
        assert resp.headers["X-Export-Relations"] == "3"
        assert resp.headers["X-Export-Sessions"] == "2"
        assert resp.headers["X-Export-Complete"] == "false"
        assert "t1" not in getattr(rr, "_EXPORT_TASKS")  # 下载即回收注册表项
        # 单测不发送响应、BackgroundTask 不执行，生产路径下载后会删 zip；
        # 这里只断言不误删整个 export 目录（keep 仍保留）
        assert zip_path.is_file() and keep.is_file()

        # 取消已产出任务：只删 zip 文件，不动同目录其他文件
        getattr(rr, "_EXPORT_TASKS")["t2"] = _export_task_row(
            "r1", status="done", total=1, zip_path=str(zip_path)
        )
        await export_cancel("r1", "t2")
        assert not zip_path.is_file()
        assert keep.is_file()
    finally:
        getattr(rr, "_EXPORT_TASKS").clear()
