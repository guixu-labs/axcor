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

"""内核独立 HTTP 应用的组合根：启动输入、内核 boot、路由挂载与关停。

控制台应用复用这里的启动输入解析、日志准备、SSE 退出标志复位与关停，
但保留自己的组合根与 ``boot()`` 调用。
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import FastAPI

from workflow_controller.api.http.mounting import (
    KERNEL_OPENAPI_TAGS,
    register_kernel_routes,
    register_plugin_routers,
)
from workflow_controller.config.settings import DEFAULT_BRAND_NAME, AppConfig
from workflow_controller.logging_config import configure_logging

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from workflow_controller.core.orchestrator import Orchestrator

log = logging.getLogger(__name__)

# 启动目录下的全局配置文件，与 `web` 子命令同一份
_CONFIG_PATH = Path("workflow-controller-config.yaml")

# 关停超时须覆盖单个卡死 run 的全路径最坏耗时（关停并发收尾，多 run 不
# 串行累加）：排空窗与终态扫掠的四段排空等待、任务/线程退出等待、代办
# 快照、逐成员停（成员数按下界 5 计）与 SIGTERM 宽限 ≈ 84s，再加其后
# pool/插件/bus 收尾余量。各段上界只有卡死才会占满，正常关停远小于此值。
ORCHESTRATOR_SHUTDOWN_TIMEOUT_S = 120.0


@dataclass(frozen=True)
class HttpBootContext:
    """HTTP 组合根的启动输入：启动目录的配置文件 + CLI 经环境变量传入的覆盖。"""

    config: AppConfig
    config_path: Path
    settings_dir: Path | None
    hook_port: int | None
    log_level: str


def hook_port_from_environment() -> int | None:
    """读取 CLI 传入的 Hook Server 端口（uvicorn 子进程经环境继承）。"""
    raw_port = os.environ.get("WORKFLOW_CONTROLLER_HOOK_PORT")
    if not raw_port:
        return None
    try:
        return int(raw_port)
    except ValueError as exc:
        raise RuntimeError("WORKFLOW_CONTROLLER_HOOK_PORT must be an integer") from exc


def load_boot_context() -> HttpBootContext:
    """组装启动输入：配置读启动目录的 yaml（缺失取默认），端口与设置目录以环境覆盖为准。"""
    config = AppConfig.from_yaml(_CONFIG_PATH) if _CONFIG_PATH.exists() else AppConfig()
    # CLI --settings-dir 经 env 穿透（uvicorn reload 子进程会重新 import 本模块，
    # 模块级状态不可靠）；空值回落 platformdirs 默认（与 core_api.boot 一致）。
    raw_settings_dir = os.environ.get("WORKFLOW_CONTROLLER_SETTINGS_DIR")
    return HttpBootContext(
        config=config,
        config_path=_CONFIG_PATH,
        settings_dir=Path(raw_settings_dir) if raw_settings_dir else None,
        hook_port=hook_port_from_environment(),
        # CLI --log-level 同样经 env 穿透；未给时用 yaml 的 log_level
        log_level=os.environ.get("WORKFLOW_CONTROLLER_LOG_LEVEL") or config.log_level,
    )


def prepare_logging(context: HttpBootContext) -> Path:
    """日志写启动目录的 .workflow-controller/，返回日志路径。

    不随 yaml 的 project_dir / 各 session 的 cwd 变——否则 CLI 报的路径 ≠ 实际
    写的路径，用户 grep 不到日志（多 session 的日志该在一个固定位置）。
    """
    return configure_logging(Path.cwd(), level=context.log_level)


def reset_sse_exit_flag() -> None:
    """复位 sse_starlette 的进程级退出标志。

    sse_starlette 的 AppStatus.should_exit 是模块级单例，SIGTERM 置 True 后
    永不复位。uvicorn reload 竞态下 worker 收到 shutdown 信号却活了下来
    （连续文件变化触发重入 reload），残留 True 的 worker 此后所有 SSE 连接
    被 _listen_for_exit_signal 立即取消 → "ASGI callable returned without
    completing response"、页面有框架无数据。lifespan startup 完成意味着本
    worker 正式服务，exit 标志必须复位。
    """
    try:
        from sse_starlette.sse import AppStatus

        AppStatus.should_exit = False
    except Exception:
        log.warning("sse_starlette AppStatus 复位失败（残留 worker 的 SSE 可能被立即取消）")


async def shutdown_orchestrator(orchestrator: Orchestrator) -> None:
    """关停内核：超过 ORCHESTRATOR_SHUTDOWN_TIMEOUT_S 不再等待，并留出被取消任务的清理时间。"""
    try:
        await asyncio.wait_for(orchestrator.shutdown(), timeout=ORCHESTRATOR_SHUTDOWN_TIMEOUT_S)
    except TimeoutError:
        log.warning("Orchestrator shutdown timed out — forcing exit")
    await asyncio.sleep(0.5)  # allow cancelled tasks to clean up


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """FastAPI 生命周期：启动内核并挂载插件路由，退出关停内核。"""
    from workflow_controller.api.core_api import boot

    context = load_boot_context()
    # Swagger /docs 与 /openapi.json 标题随部署品牌（openapi 按请求惰性生成，
    # 启动期赋值即对首个请求生效）；空/纯空白回落默认，与配置写入时一致。
    app.title = context.config.brand_name.strip() or DEFAULT_BRAND_NAME
    # uvicorn 启动会禁用既有 logger，须在其后重新配置内核日志
    prepare_logging(context)
    reset_sse_exit_flag()

    orchestrator, api = await boot(
        context.config,
        context.config_path,
        settings_dir=context.settings_dir,
        hook_port=context.hook_port,
    )
    app.state.api = api
    register_plugin_routers(app, api)

    log.info("%s HTTP API started on project_dir=%s", app.title, context.config.project_dir)
    try:
        yield
    finally:
        await shutdown_orchestrator(orchestrator)


def create_app() -> FastAPI:
    """创建内核 HTTP 应用：内核路由、插件路由与生命周期；不含 CORS、SPA 与反代。"""
    app = FastAPI(
        # 启动后由 lifespan 按配置的 brand_name 覆盖，此处仅覆盖启动间隙。
        title=DEFAULT_BRAND_NAME,
        version="0.1.0",
        description="Workflow Controller 内核 HTTP API",
        openapi_tags=KERNEL_OPENAPI_TAGS,
        lifespan=lifespan,
    )
    register_kernel_routes(app)
    return app
