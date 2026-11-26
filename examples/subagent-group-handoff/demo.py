#!/usr/bin/env python3
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

"""Exercise Claude CLI group handoff through the real Orchestrator."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from workflow_controller.config.settings import (
    AppConfig,
    HookServerConfig,
    LoopConfig,
    ReviewerConfig,
    SessionConfig,
)
from workflow_controller.core.orchestrator import Orchestrator
from workflow_controller.driver.factory import set_config_runtime
from workflow_controller.plugin.base import PluginConfig
from workflow_controller.plugin.context import PluginContext
from workflow_controller.plugin.registry import _build_plugin_services
from workflow_controller.plugins.breather.plugin import BreatherPlugin
from workflow_controller.session.events import EventType

logger = logging.getLogger(__name__)


class DemoCheckError(RuntimeError):
    """演示断言失败：_check 抛出，main 捕获后以退出码 1 结束。"""


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


AGENT_TYPES = {"researcher", "checker", "writer"}


def _check(condition: bool, description: str) -> None:
    """校验演示产物；失败抛 DemoCheckError（main 捕获后以退出码 1 结束）。"""
    if not condition:
        raise DemoCheckError(f"校验失败：{description}")


def _install_breather_plugin(
    orch: Orchestrator, project: Path, window: int, main_threshold: int, child_threshold: int
) -> None:
    """手动安装 breather 插件（hook_server 关闭时 registry 不自动加载）。"""
    plugin = BreatherPlugin(
        PluginConfig(
            name="breather",
            version="demo",
            enabled=True,
            extra={
                "context_window": window,
                "context_threshold_pct": main_threshold,
                "subagent_context_threshold_pct": child_threshold,
                "handoff_agent_timeout": 20,
            },
        ),
        None,
    )
    hook_server = SimpleNamespace(register_route=lambda *_args: None)
    plugin.install(
        PluginContext(
            bus=orch.bus,
            type_registry=orch.bus.type_registry,
            hook_server=hook_server,
            file_changes=orch.file_changes,
            work_dir=project,
            services=_build_plugin_services(orch),
        )
    )
    getattr(orch.plugin_registry, "_plugins").append(plugin)


async def _start_demo_session(orch: Orchestrator, recipe: AppConfig, project: Path):
    """以 YAML 中的阈值启动初始 worker session。"""
    return await orch.start_session_with_config(
        SessionConfig(
            name="claude-cli-group-demo",
            role="worker",
            runtime="claude_cli",
            cwd=str(project),
            initial_prompt=recipe.starting_prompt,
            context_window=recipe.context_window,
            context_threshold_pct=recipe.context_threshold_pct,
            subagent_context_threshold_pct=recipe.subagent_context_threshold_pct,
            handoff_prompt_path=recipe.handoff_prompt_path,
            model="demo",
            loop=LoopConfig(max_iterations=1, evaluator="always_complete"),
        )
    )


async def _wait_for_completion(orch: Orchestrator, session, project: Path):
    """等三名旧子代理在途 → 写启动信号 → 等交接与续跑完成；返回 successor 会话。"""
    start_deadline = asyncio.get_running_loop().time() + 10
    expected_old_ids = {f"old-{kind}" for kind in AGENT_TYPES}
    while not expected_old_ids.issubset(orch.subagent_status(session.id)):
        if asyncio.get_running_loop().time() >= start_deadline:
            raise RuntimeError("10 秒内未观察到三名在途子代理")
        await asyncio.sleep(0.05)
    (project / "demo-all-started.signal").write_text("ready", encoding="utf-8")
    deadline = asyncio.get_running_loop().time() + 35
    completion = project / "demo-complete.json"
    while asyncio.get_running_loop().time() < deadline:
        workers = [s for s in orch.pool.list_sessions() if s.role == "worker"]
        successors = [s for s in workers if s.id != session.id]
        if completion.is_file() and successors and successors[0].loop_status == "no":
            return successors[0]
        await asyncio.sleep(0.2)
    raise RuntimeError("35 秒内未完成交接与续跑")


def _collect_handoff_artifacts(
    orch: Orchestrator, project: Path, session, successor
) -> SimpleNamespace:
    """收集交接产物：manifest、调用记录、双方事件流与完成标记。"""
    batch = next((project / ".workflow-controller" / "handoffs" / session.id).iterdir())
    manifest = json.loads((batch / "manifest.json").read_text(encoding="utf-8"))
    invocations = [
        json.loads(line)
        for line in (project / "invocations.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    events = orch.event_router.get_events_sync(session.id)
    successor_events = orch.event_router.get_events_sync(successor.id)
    new_starts = [
        e.data["agent_id"] for e in successor_events if e.event_type == EventType.SUBAGENT_START
    ]
    agent_prompts = [
        e.data.get("input", {}).get("prompt", "")
        for e in successor_events
        if e.event_type == EventType.TOOL_USE and e.data.get("name") == "Agent"
    ]
    stopped = [
        e.data["agent_id"]
        for e in events
        if e.event_type == EventType.SUBAGENT_STOP and e.data.get("status") == "interrupted"
    ]
    contexts = {
        e.data["agent_id"]: e.data for e in events if e.event_type == EventType.SUBAGENT_CONTEXT
    }
    main_contexts = [e.data for e in events if e.event_type == EventType.MAIN_CONTEXT]
    kinds = [item["kind"] for item in invocations]
    result = json.loads((project / "demo-complete.json").read_text(encoding="utf-8"))
    return SimpleNamespace(
        manifest=manifest,
        invocations=invocations,
        kinds=kinds,
        agent_prompts=agent_prompts,
        new_starts=new_starts,
        stopped=stopped,
        contexts=contexts,
        main_contexts=main_contexts,
        result=result,
    )


def _check_handoff_manifest(art: SimpleNamespace) -> None:
    """校验调用顺序、清单条目、主摘要引用与派发指令。"""
    manifest = art.manifest
    _check(
        art.kinds == ["initial_worker", "handoff_agent", "successor_worker"],
        f"invocation 顺序异常：{art.kinds}",
    )
    _check(len(manifest["entries"]) == 3, "handoff 条目应为 3 份")
    _check(manifest["trigger_agent_id"] == "old-checker", "触发者应为 old-checker")
    _check(bool(manifest["main_handoff"]), "manifest 缺 main_handoff")
    _check(Path(manifest["main_source"]).is_file(), "main_source 文件缺失")
    _check(Path(manifest["main_events"]).is_file(), "main_events 文件缺失")
    main_doc = Path(manifest["main_handoff"])
    _check(main_doc.is_file(), "main_handoff 文件缺失")
    main_text = main_doc.read_text(encoding="utf-8")
    _check(
        all(
            item["handoff"] in main_text and item["dispatch_prompt"] in main_text
            for item in manifest["entries"]
        ),
        "main_handoff 应引用全部条目的文档与派发指令",
    )
    _check(
        {item["agent_type"] for item in manifest["entries"]} == AGENT_TYPES,
        "条目 agent_type 集合不符",
    )
    _check(
        all(
            item["handoff"] in item["dispatch_prompt"]
            and "从上次中断的步骤继续执行" in item["dispatch_prompt"]
            for item in manifest["entries"]
        ),
        "派发指令应包含 handoff 文档与继续执行提示",
    )
    _check(
        all(art.agent_prompts.count(item["dispatch_prompt"]) == 1 for item in manifest["entries"]),
        "每条派发指令应恰好派发一次",
    )
    _check(all(Path(item["handoff"]).is_file() for item in manifest["entries"]), "handoff 文件缺失")
    _check(all(Path(item["source"]).is_file() for item in manifest["entries"]), "source 文件缺失")
    _check(Path(manifest["done"]).is_file(), "done 标记文件缺失")


def _check_handoff_events(art: SimpleNamespace, main_threshold: int, child_threshold: int) -> None:
    """校验新旧子代理启停与上下文占比事件。"""
    _check(set(art.stopped) == {f"old-{kind}" for kind in AGENT_TYPES}, "旧子代理停止集合不符")
    _check(
        {f"new-{kind}" for kind in AGENT_TYPES} <= set(art.new_starts),
        "新子代理应全部启动",
    )
    _check(art.contexts["old-checker"]["percentage"] == 1.6, "old-checker 上下文占比应为 1.6%")
    _check(
        all(
            art.contexts[f"old-{kind}"]["percentage"] < child_threshold
            for kind in AGENT_TYPES - {"checker"}
            if f"old-{kind}" in art.contexts
        ),
        "旧子代理上下文占比应低于阈值",
    )
    _check(
        max(item["percentage"] for item in art.main_contexts) > main_threshold,
        "主代理上下文占比应超过阈值",
    )
    _check(
        {agent_id for agent_id in art.contexts if agent_id.startswith("old-")}
        <= {f"old-{kind}" for kind in AGENT_TYPES},
        "上下文事件不应包含未知旧代理",
    )


def _check_successor_outcome(
    art: SimpleNamespace, orch: Orchestrator, project: Path, session, successor
) -> None:
    """校验 successor 会话身份、交付文件与 worker 会话数。"""
    result = art.result
    _check(result["successor_native_id"] == "demo-native-successor", "successor 原生会话 id 不符")
    _check(successor.native_session_id == "demo-native-successor", "successor 原生会话 id 不符")
    _check(
        set(result["new_agents"]) == {f"new-{kind}" for kind in AGENT_TYPES},
        "结果 new_agents 集合不符",
    )
    _check(
        all((project / name).is_file() for name in ("research.md", "checklist.md", "outline.md")),
        "交付文件缺失（research/checklist/outline）",
    )
    _check(
        len([s for s in orch.pool.list_sessions() if s.role == "worker"]) == 2,
        "worker 会话数应为 2",
    )
    _check(session.id != successor.id, "新主会话应与 successor 不同")


async def run_demo(project: Path) -> None:
    recipe = AppConfig.from_yaml(Path(__file__).with_name("workflow-controller.yaml"))
    window = recipe.context_window
    main_threshold = recipe.context_threshold_pct
    child_threshold = recipe.subagent_context_threshold_pct
    _check(
        window == 1_000_000 and main_threshold == child_threshold == 1,
        "YAML 阈值与演示假设不符（window=1_000_000，两阈值均为 1）",
    )
    config = AppConfig(
        project_dir=str(project),
        runtime="claude_cli",
        hook_server=HookServerConfig(enabled=False),
        reviewer=ReviewerConfig(enabled=False),
        loop=LoopConfig(max_iterations=1, evaluator="always_complete"),
    )
    set_config_runtime("claude_cli")
    orch = Orchestrator(config, default_project_dir=project)
    await orch.start()
    _install_breather_plugin(orch, project, window, main_threshold, child_threshold)

    try:
        session = await _start_demo_session(orch, recipe, project)
        successor = await _wait_for_completion(orch, session, project)
        art = _collect_handoff_artifacts(orch, project, session, successor)
        _check_handoff_manifest(art)
        _check_handoff_events(art, main_threshold, child_threshold)
        _check_successor_outcome(art, orch, project, session, successor)
        logger.info("PASS Claude CLI：主代理与 checker 均超过 YAML 的 1% 阈值")
        logger.info("PASS 一个 handoff session 写主代理和三名旧子代理共四份文档")
        logger.info("PASS 旧主 session 停止，创建全新主 session")
        logger.info("PASS 新主 session 读取清单并重新派发三名子代理")
    finally:
        await orch.shutdown()


async def _run_in(root: Path) -> None:
    project = root / "project"
    project.mkdir()
    shutil.copyfile(
        Path(__file__).with_name("handoff-instructions.md"), project / "handoff-instructions.md"
    )
    bin_dir = root / "bin"
    bin_dir.mkdir()
    fake_claude = bin_dir / "claude"
    shutil.copyfile(Path(__file__).with_name("fake_claude.py"), fake_claude)
    fake_claude.chmod(0o755)
    original = {}
    for key in (
        "PATH",
        "HOME",
        "WORKFLOW_CONTROLLER_DEMO_PROJECT",
        "WORKFLOW_CONTROLLER_DEMO_STATE",
    ):
        original[key] = os.environ.get(key)
    os.environ.update(
        {
            "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
            "HOME": str(root),
            "WORKFLOW_CONTROLLER_DEMO_PROJECT": str(project),
            "WORKFLOW_CONTROLLER_DEMO_STATE": str(project),
        }
    )
    try:
        await run_demo(project)
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep", action="store_true", help="保留临时项目和交接产物")
    args = parser.parse_args()
    try:
        if args.keep:
            root = Path(tempfile.mkdtemp(prefix="workflow-controller-claude-group-"))
            try:
                await _run_in(root)
            finally:
                logger.info(f"ARTIFACTS {root / 'project'}")
        else:
            with tempfile.TemporaryDirectory(prefix="workflow-controller-claude-group-") as tmp:
                await _run_in(Path(tmp))
    except DemoCheckError as error:
        logger.error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    _configure_logging()
    raise SystemExit(asyncio.run(main()))
