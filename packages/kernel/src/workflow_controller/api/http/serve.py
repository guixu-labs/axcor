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

"""serve 子命令执行体：日志、环境穿透与 uvicorn 工厂启动（由内核 CLI 懒装配调用）。"""

from __future__ import annotations

import json
import os
from pathlib import Path

import click

from workflow_controller.api.http.app import load_boot_context, prepare_logging

# uvicorn 以工厂方式导入应用；lifespan 在 uvicorn 配置日志之后重新配置内核日志
APP_FACTORY = "workflow_controller.api.http.app:create_app"
# 须大于 sse_routes 的 shutdown_grace_period（2s），否则 SSE 告别帧发不出去
GRACEFUL_SHUTDOWN_TIMEOUT_S = 3


def run_serve_command(
    port: int, host: str, settings_dir: str | None, log_level: str | None
) -> None:
    """serve 子命令执行体：日志落启动目录，再以 uvicorn 工厂方式起内核应用。"""
    # settings 目录与日志级别经 env 穿透到 lifespan 的 load_boot_context
    if settings_dir:
        os.environ["WORKFLOW_CONTROLLER_SETTINGS_DIR"] = str(
            Path(settings_dir).expanduser().resolve()
        )
    if log_level is not None:
        os.environ["WORKFLOW_CONTROLLER_LOG_LEVEL"] = log_level

    context = load_boot_context()
    log_path = prepare_logging(context)
    click.echo(json.dumps({"event": "info", "msg": f"Logging to {log_path}"}))
    os.environ["WORKFLOW_CONTROLLER_LOG_LEVEL"] = context.log_level

    import uvicorn

    uvicorn.run(
        APP_FACTORY,
        host=host,
        port=port,
        factory=True,
        timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_TIMEOUT_S,
    )
