#!/usr/bin/env python
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

"""serve 冒烟：验证仅安装发行 wheel 的环境可经 workflow-controller serve 用 HTTP 控制 run。

工作目录落在临时目录，不依赖仓内源码与测试基建；供 kernel-only
安装矩阵（内核 + fake-driver wheel 的干净环境）使用。启动 serve 子进程后：
GET /api/v1/health 返回 200 → POST /api/v1/runs 创建 fake@1 run → 轮询至
终态 → SIGTERM 后 lifespan 走完优雅关停。
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

READY_TIMEOUT_S = 30.0
RUN_TIMEOUT_S = 30.0
SHUTDOWN_TIMEOUT_S = 30.0
_LOG_TAIL_LINES = 40


def _free_port() -> int:
    """取一个当前可绑定的回环端口。"""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _request(base_url: str, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    """发 JSON 请求并解析 JSON 应答；连接失败与非 2xx 抛异常由调用方处理。"""
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        f"{base_url}{path}", data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read() or b"null")


def _wait_for_ready(base_url: str, process: subprocess.Popen[str]) -> None:
    """等 health 返回 200；服务提前退出或超时即报错。"""
    deadline = time.monotonic() + READY_TIMEOUT_S
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"serve 提前退出（退出码 {process.returncode}）")
        try:
            health = _request(base_url, "GET", "/api/v1/health")
        except (urllib.error.URLError, OSError, ValueError) as exc:
            last_error = exc
            time.sleep(0.2)
            continue
        if health.get("ok") is True:
            return
        raise RuntimeError(f"health 应答异常：{health}")
    raise RuntimeError(f"serve 未在 {READY_TIMEOUT_S}s 内就绪：{last_error}")


def _wait_run_finished(base_url: str, run_id: str) -> dict[str, Any]:
    """轮询 run 详情至终态并返回 run 记录。"""
    deadline = time.monotonic() + RUN_TIMEOUT_S
    while True:
        run = _request(base_url, "GET", f"/api/v1/runs/{run_id}")["run"]
        if run["finished_at"] is not None:
            return run
        if time.monotonic() >= deadline:
            raise RuntimeError(f"run {run_id} 未在 {RUN_TIMEOUT_S}s 内到终态：{run}")
        time.sleep(0.1)


def _cli_command() -> list[str]:
    """定位已安装的 workflow-controller 入口；退回模块方式。"""
    console_script = Path(sys.executable).with_name("workflow-controller")
    if console_script.exists():
        return [str(console_script)]
    return [sys.executable, "-m", "workflow_controller"]


def serve_fake_run(workdir: Path, settings_dir: Path) -> dict[str, Any]:
    """启动 serve、经 HTTP 创建并跑完一个 fake@1 run、SIGTERM 关停，返回摘要。"""
    (workdir / "workflow-controller-config.yaml").write_text(
        "project_dir: .\nhook_server:\n  enabled: true\nreviewer:\n  enabled: false\n",
        encoding="utf-8",
    )
    (workdir / "fake-program.yaml").write_text("format: fake@1\n", encoding="utf-8")
    port, hook_port = _free_port(), _free_port()
    base_url = f"http://127.0.0.1:{port}"
    log_path = workdir / "serve.log"
    summary: dict[str, Any] = {}
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen[str](
            [
                *_cli_command(),
                "serve",
                "--port",
                str(port),
                "--host",
                "127.0.0.1",
                "--hook-port",
                str(hook_port),
                "--settings-dir",
                str(settings_dir),
            ],
            cwd=workdir,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            _wait_for_ready(base_url, process)
            created = _request(
                base_url,
                "POST",
                "/api/v1/runs",
                {
                    "format": "fake@1",
                    "program_path": str(workdir / "fake-program.yaml"),
                    "work_dir": str(workdir / "work"),
                },
            )
            run = _wait_run_finished(base_url, created["run_id"])
            summary = {"run_id": created["run_id"], "status": run["status"]}
        except RuntimeError as exc:
            raise RuntimeError(f"{exc}\n--- serve.log 末尾 ---\n{_log_tail(log_path)}") from None
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                process.wait(timeout=SHUTDOWN_TIMEOUT_S)
    summary["exit_code"] = process.returncode
    # uvicorn 在信号触发的优雅关停完成后重新抛出该信号，进程以 -SIGTERM 结束；
    # lifespan 是否走完只能由日志判定
    summary["graceful_shutdown"] = "Application shutdown complete" in _log_tail(
        log_path, limit=None
    )
    return summary


def _log_tail(log_path: Path, limit: int | None = _LOG_TAIL_LINES) -> str:
    """读服务日志的末尾若干行（limit 为 None 取全文）。"""
    lines = log_path.read_text(encoding="utf-8").splitlines(keepends=True)
    return "".join(lines if limit is None else lines[-limit:])


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="kernel-only-serve-smoke.") as tmp:
        summary = serve_fake_run(Path(tmp), Path(tmp) / "settings")
    # 正常退出到 0；SIGTERM 优雅关停后 uvicorn 重抛信号，退出码为负的信号编号
    expected_exit_codes = (0, -int(signal.SIGTERM)) if os.name != "nt" else (0,)
    ok = (
        summary["status"] == "completed"
        and summary["graceful_shutdown"]
        and summary["exit_code"] in expected_exit_codes
    )
    print(f"serve fake run 终态：{summary['status']}；关停：{summary}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
