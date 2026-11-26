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

"""Breather 全流程演示 — 通过 Orchestrator.start() 启动 HookServer。

用法:
    python3 tests/breather/demo_breather.py              # 超阈值场景
    python3 tests/breather/demo_breather.py --below       # 未超阈值场景
    python3 tests/breather/demo_breather.py --threshold 60 --strategy auto
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import uuid
from pathlib import Path

import aiohttp

from workflow_controller.config.settings import AppConfig, HookServerConfig, SessionConfig
from workflow_controller.core.orchestrator import Orchestrator
from workflow_controller.session.events import Event, EventType

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def banner(msg: str) -> None:
    logger.info(f"\n{'=' * 60}\n  {msg}\n{'=' * 60}")


def _build_orchestrator(args: argparse.Namespace) -> Orchestrator:
    """装配 Orchestrator：手动挂 HookServer 并安装 Breather 插件（Step 1）。"""
    config = AppConfig(
        hook_server=HookServerConfig(port=args.port, host="localhost"),
    )
    orch = Orchestrator(config, default_project_dir=Path("/tmp/breather-demo.db"))

    # 手动启动 HookServer（跳过 reviewer，demo 不需要实际模型）
    from workflow_controller.hooks.adapter import HookPayloadAdapter
    from workflow_controller.hooks.server import HookServer

    setattr(
        orch,
        "_hook_server",
        HookServer(
            port=args.port,
            host="localhost",
            on_event=HookPayloadAdapter(orch.bus),
        ),
    )
    # 手动启插件（绕开 registry 的 enabled 检查，方便演示）
    from workflow_controller.plugin.base import PluginConfig
    from workflow_controller.plugin.context import PluginContext
    from workflow_controller.plugin.registry import _build_plugin_services
    from workflow_controller.plugins.breather.plugin import BreatherPlugin

    plugin_cfg = PluginConfig(
        name="breather",
        version="0.1",
        enabled=True,
        extra={
            "strategy": args.strategy,
            "context_threshold_pct": args.threshold,
            "handoff_skill": "a2h-handoff",
            "handoff_filename": "handoff.md",
        },
    )
    _plugin = BreatherPlugin(plugin_cfg, Path("/dev/null"))
    _plugin.install(
        PluginContext(
            bus=orch.bus,
            type_registry=orch.bus.type_registry,
            hook_server=getattr(orch, "_hook_server"),
            services=_build_plugin_services(orch),
        )
    )
    logger.info(f"  HookServer: http://localhost:{args.port}")
    logger.info("  BreatherPlugin installed")
    return orch


async def _create_worker_session(orch: Orchestrator, args: argparse.Namespace):
    """创建 worker session 并注入上下文使用量事件（Step 2）。"""
    logger.info("\n[Step 2] start_session_with_config()")
    cfg = SessionConfig(
        name="android-to-harmony",
        role="worker",
        cwd="/tmp/breather-demo",
        initial_prompt="run a2h pipeline",
    )
    s = await orch.pool.create(cfg)
    s.native_session_id = "cc-" + uuid.uuid4().hex[:12]
    logger.info(f"  name={s.name}  sdk_session={s.native_session_id}")

    # 注入上下文事件
    input_tokens = 20_000 if args.below else 150_000
    window = 200_000
    pct = input_tokens / window * 100
    await orch.event_router.store(
        Event.create(
            session_id=s.id,
            iteration=5,
            event_type=EventType.STEP_FINISH,
            data={
                "usage": {
                    "input_tokens": input_tokens,
                    "output_tokens": 15_000,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                },
                "modelUsage": {"claude-opus": {"contextWindow": window}},
            },
        )
    )
    logger.info(
        f"  上下文: {input_tokens}/{window} = {pct}%  "
        f"(阈值 {args.threshold}% → {'会' if pct > args.threshold else '不会'} 触发)"
    )
    return s


def _install_mock_runtime(orch: Orchestrator, s) -> None:
    """铺 mock query、handoff 文件、mock start/stop 与 hook 总线（Step 3~5）。"""

    # ── 3. mock query ───────────────────────────────────────
    async def mock_query(prompt, iteration=1):
        logger.info(f"\n  [mock] Claude Code query(iter={iteration})")
        logger.info(f"         prompt: {prompt[:150]}...")
        yield Event.create(session_id=s.id, iteration=0, event_type=EventType.ASSISTANT_MESSAGE)

    s.query = mock_query

    # ── 4. handoff.md ───────────────────────────────────────
    handoff_file = Path("/tmp/breather-demo") / "handoff.md"
    handoff_file.parent.mkdir(parents=True, exist_ok=True)
    handoff_file.write_text(
        "# Handoff\nspec done, plan done, execute done\nverify pending\n",
        encoding="utf-8",
    )

    # ── 5. mock start/stop & bus ──────────────────────────
    new_s = type("MockSession", (), {})()
    new_s.name = "android-to-harmony-h2"
    new_s.id = uuid.uuid4().hex[:12]
    orch.start_session_with_config = _make_async_return(new_s, orch.pool, new_s.id)
    orch.stop_session = _make_async_pop(orch.pool)

    # Mock bus for hook-driven handoff
    from unittest.mock import MagicMock

    orch.bus = MagicMock()
    orch.bus.observe = MagicMock()

    # Capture observe callback and trigger it after mock_query runs
    hook_cbs = []

    def fake_sub(event_type, cb):
        hook_cbs.append(cb)

    orch.bus.observe = fake_sub

    # Replace mock_query to also trigger the hook
    _original_query = s.query

    async def hooked_query(prompt, iteration=1):
        async for event in _original_query(prompt, iteration=iteration):
            yield event
        await asyncio.sleep(0.05)
        if hook_cbs:
            await hook_cbs[0](
                Event.create(
                    session_id=s.native_session_id,
                    iteration=0,
                    event_type=EventType.TOOL_RESULT,
                    data={"tool_name": "a2h-handoff"},
                )
            )

    s.query = hooked_query


def _log_result(data: dict, threshold: float) -> None:
    """按 /breather 应答打印结果（触发/未触发/失败三分支）。"""
    if data["handoff_triggered"]:
        logger.info("  ✅ handoff 已触发")
        logger.info(f"     旧 session: {data['old_session_name']}")
        logger.info(f"     新 session: {data['new_session_name']}")
        logger.info(f"     交接文件:   {data['handoff_file']}")
    elif "context" in data:
        c = data["context"]
        logger.info(f"  ⏸️  handoff 未触发（{c['percentage']}% < {threshold}%）")
        logger.info(f"     tokens: {c['current_tokens']}/{c['max_tokens']}")
    else:
        logger.info(f"  ❌ 失败: {data.get('reason', 'unknown')}")


async def main(args: argparse.Namespace) -> None:
    banner("Breather Demo")

    # ── 1. Orchestrator.start() → HookServer + 插件 ─────────
    logger.info("\n[Step 1] Orchestrator.start()")
    orch = _build_orchestrator(args)

    await getattr(orch, "_hook_server").start()

    # ── 2~5. worker session + mock 铺设 ─────────────────────
    s = await _create_worker_session(orch, args)
    _install_mock_runtime(orch, s)

    # ── 6. POST /breather ───────────────────────────────────
    logger.info("\n[Step 3] POST /breather")
    async with (
        aiohttp.ClientSession() as client,
        client.post(
            f"http://localhost:{args.port}/breather",
            json={},
        ) as resp,
    ):
        data = await resp.json()

    # ── 7. 结果 ─────────────────────────────────────────────
    banner("结果")
    _log_result(data, args.threshold)

    logger.info("\n  SessionPool:")
    for ss in orch.pool.list_sessions():
        logger.info(f"    {getattr(ss, 'name', ss.__class__.__name__)}")

    await getattr(orch, "_hook_server").stop_server()
    logger.info(f"\n{'=' * 60}\n  Demo 完成\n{'=' * 60}")


def _make_async_return(value, pool, sid):
    async def _fn(cfg):
        getattr(pool, "_sessions")[sid] = value
        return value

    return _fn


def _make_async_pop(pool):
    async def _fn(sid):
        getattr(pool, "_sessions").pop(sid, None)

    return _fn


if __name__ == "__main__":
    _configure_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=18766)
    ap.add_argument("--strategy", default="token")
    ap.add_argument("--threshold", type=float, default=50.0)
    ap.add_argument("--below", action="store_true", help="模拟未超阈值场景")
    asyncio.run(main(ap.parse_args()))
