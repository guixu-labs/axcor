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

"""graph 插件：注册 graph-yaml@1 驱动工厂 + 前端 RunDagPanel 面板声明。

graph 状态经驱动快照（run_driver_state）持久化、节点历史经事件流
（run_events）承载；boot 恢复与重开续跑由 run 平面按账面驱动。决策与
执行全部经正规 RunDriver 承载（decide 步进）。插件侧只持有 run_id 键的
驱动内存注册表（create/restore 共用）；graph_id 是驱动内标签（label
或程序位置摘要），不参与任何寻址。
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from workflow_controller.plugin.sdk import Plugin, RunPanelDef
from workflow_controller.plugins.graph import exporter, translator
from workflow_controller.plugins.graph.driver import scheduler
from workflow_controller.plugins.graph.driver.engine import (
    GraphDriver,
    pick_runner_options,
)
from workflow_controller.plugins.graph.projection import PROJECTION_SCHEMA_VERSION

if TYPE_CHECKING:
    from collections.abc import Mapping

    from workflow_controller.plugin.base import PluginConfig
    from workflow_controller.plugin.context import PluginContext

logger = logging.getLogger(__name__)

# 驱动格式串：工厂注册、run 侧识别与面板声明共用，散写易漂移
GRAPH_FORMAT = "graph-yaml@1"

# 驱动注册表容量上界：终态/已删 run 的条目不再被触达（run_id 不复用），
# 按插入序保留最近若干个防无界增长
_DRIVER_CACHE_LIMIT = 128


def _decode_driver_params(params: dict[str, str]) -> dict[str, Any]:
    """run 参数反串行化：JSON 可解析则还原，否则按原字符串。"""
    decoded: dict[str, Any] = {}
    for key, raw in params.items():
        try:
            decoded[key] = json.loads(raw)
        except (TypeError, ValueError):
            decoded[key] = raw
    return decoded


def _reconcile_existing_status(target: Path, values: dict[str, str], temp: Path) -> None:
    """回读已存在的 status.json：三键全匹配则复用，键冲突报错，否则覆盖重发。"""
    try:
        existing = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        existing = None
    if isinstance(existing, dict) and any(key in existing for key in values):
        if not all(key in existing for key in values) or any(
            existing.get(key) != value for key, value in values.items()
        ):
            raise ValueError(
                f"{target} 已属于其他工作流或运行，请为这次运行选择独立的工作目录"
            ) from None
        return
    # 三键全缺失或文件损坏：旧版工作流整体重写的陈旧残留，覆盖重发
    shutil.copyfile(temp, target)


def _publish_workflow_status(work_dir: Path, workflow: Path, user_prompt: Path) -> None:
    """发布旧版工作流读取的路径信息，保留工作目录中已有的状态文件。

    只核对本函数发布的三个键（workflow/work_dir/user_prompt）：旧版工作流
    运行时可能往 status.json 追加自己的字段，恢复/重启时不能因这些无关字段
    变化就误判为工作目录冲突。三键全缺失（旧版整体重写或文件损坏）视为陈旧
    残留并重新发布；只要出现任一已发布键但值不符（或键不全）就报告冲突。
    硬链接不可用的文件系统（部分网络盘/FAT）回退为普通复制。
    """
    target = work_dir / ".workflow" / "status.json"
    values = {
        "workflow": str(workflow.absolute()),
        "work_dir": str(work_dir.absolute()),
        "user_prompt": str(user_prompt.absolute()),
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=target.parent, prefix=".graph-status-") as tmp_dir:
        temp = Path(tmp_dir) / target.name
        temp.write_text(json.dumps(values, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        try:
            target.hardlink_to(temp)
            return
        except OSError:
            pass
        if target.exists():
            _reconcile_existing_status(target, values, temp)
            return
        # 目标不存在：排他创建；并发竞态下已被其他进程创建则回读比对，
        # 内容一致即复用而非误报冲突
        content = temp.read_text(encoding="utf-8")
        try:
            with target.open("x", encoding="utf-8") as fh:
                fh.write(content)
        except FileExistsError:
            _reconcile_existing_status(target, values, temp)


class GraphDriverFactory:
    """graph-yaml@1 驱动工厂：经插件供给/恢复正规驱动。"""

    discard_terminal_sessions_at_boot = True

    def __init__(self, plugin_lookup: Any) -> None:
        # 协调器在 boot 期冻结工厂集，插件 toggle 重载换新实例后构造期
        # 引用会落在已 uninstall 的旧实例上：create/restore 时按名解析
        # 当前活跃实例
        self._plugin_lookup = plugin_lookup

    @property
    def format(self) -> str:
        """带版本格式串（驱动工厂派发键）。"""
        return GRAPH_FORMAT

    @staticmethod
    def export_contribution(request: Any) -> Any:
        """导出贡献：快照+事件+冻结程序翻译为 workflow bundle 方言。"""
        return exporter.build_contribution(request)

    def create(
        self,
        run: Any,
        program_path: Path,
        params: Mapping[str, str],
    ) -> GraphDriver:
        """新建驱动：经插件供给（run_id 键实例复用）。"""
        return self._plugin().provide_driver(run, Path(program_path), dict(params))

    def restore(
        self,
        run: Any,
        snapshot: Mapping[str, Any],
        sessions: Any,
    ) -> GraphDriver:
        """从快照恢复驱动。"""
        return self._plugin().restore_driver(run, snapshot)

    def reload_definition(self, run: Any, driver: Any, source: Path) -> dict[str, Any]:
        """重读当前工作流定义（工厂侧 I/O）：翻译与子图预案，不换驱动实例。"""
        return self._plugin().reload_definition(run, driver, Path(source))

    def _plugin(self) -> Any:
        """解析当前活跃插件实例。"""
        plugin = self._plugin_lookup()
        if plugin is None:
            raise RuntimeError("graph 插件未装载，graph-yaml@1 驱动不可用")
        return plugin


class GraphPlugin(Plugin):
    def __init__(self, config: PluginConfig, config_path: Path) -> None:
        super().__init__(config, config_path)
        self._ctx: PluginContext | None = None
        # run_id 键的驱动内存注册表：create/restore 写入（重开续跑的实例
        # 复用捷径），uninstall 清空。run 生命周期归 run 平面——toggle 卸载
        # 不停 run，在跑驱动的实例引用由协调器持有，这里只清插件侧索引
        self._drivers: dict[str, GraphDriver] = {}

    @staticmethod
    def _load_subgraph_yaml(work_dir: Path, generator_id: str, output_rel: str) -> Any:
        """读取子图 yaml 配置；缺失/不可解析返回 None（日志留痕）。"""
        if not output_rel:
            return None
        subgraph_path = work_dir / output_rel
        if not subgraph_path.is_file():
            return None
        try:
            return translator.load_subgraph_config(subgraph_path)
        except Exception:
            logger.warning(
                "restore subgraph yaml parse failed for %s: %s",
                generator_id,
                subgraph_path,
                exc_info=True,
            )
            return None

    @staticmethod
    def _write_user_prompt(user_prompt_path: Path, content: str) -> None:
        """落 user_prompt.md：有内容写内容，无内容保底写空行。"""
        try:
            user_prompt_path.parent.mkdir(parents=True, exist_ok=True)
            if content:
                user_prompt_path.write_text(f"{content}\n", encoding="utf-8")
            elif not user_prompt_path.exists():
                user_prompt_path.write_text("\n", encoding="utf-8")
        except OSError:
            logger.warning("write user_prompt.md failed", exc_info=True)

    def install(self, ctx: PluginContext) -> None:
        """插件安装钩子。"""
        super().install(ctx)
        self._ctx = ctx
        logger.info("graph plugin installed")

    def uninstall(self) -> None:
        """插件卸载：清内存驱动索引（名下 run 由 run 平面继续持有）。"""
        self._drivers.clear()
        logger.info("graph plugin uninstalled")

    def get_run_driver_factories(self) -> dict[str, Any]:
        """登记 graph-yaml@1 驱动工厂（run 平面的派发入口）。"""
        plugin_name = self.config.name

        def _active_plugin() -> GraphPlugin | None:
            """按名解析当前活跃实例。"""
            if self._ctx is None:
                return None
            plugin = self._ctx.services.plugin_by_name(plugin_name)
            return plugin if isinstance(plugin, GraphPlugin) else None

        return {GRAPH_FORMAT: GraphDriverFactory(_active_plugin)}

    def get_run_panels(self) -> list[RunPanelDef]:
        """面板贡献声明：graph-yaml@1 的 DAG 视图（消费驱动投影渲染）。"""
        return [
            RunPanelDef(
                format=GRAPH_FORMAT,
                component="graph.RunDagPanel",
                schema_version=PROJECTION_SCHEMA_VERSION,
            )
        ]

    def provide_driver(
        self,
        run: Any,
        program_path: Path,
        params: dict[str, str],
    ) -> GraphDriver:
        """工厂 create 供给：重开续跑复用内存实例（run_id 键），否则按
        冻结程序与 run 参数现建。
        """
        driver = self._drivers.get(run.run_id)
        if driver is None:
            decoded = _decode_driver_params(params)
            opts = pick_runner_options(decoded)
            self._apply_provider_overrides(opts)
            variables = dict(decoded.get("variables") or {})
            # 冻结副本目录下没有相对 agents 资源：agents_dir 的相对基准
            # 改用原始 yaml 目录（program_ref 记录）
            agents_dir_base = Path(run.program_ref).parent if run.program_ref else None
            work_dir = self._resolve_work_dir(run, Path(program_path), variables)
            self._apply_thinking_overrides(opts, str(work_dir))
            user_prompt_path = work_dir / ".workflow" / run.run_id / "user_prompt.md"
            driver = self._build_driver(
                graph_id=self._derive_graph_id(run, fallback=str(program_path)),
                yaml_path=Path(program_path),
                agents_dir_base=agents_dir_base,
                work_dir=work_dir,
                variables=variables,
                starting_prompt=decoded.get("starting_prompt"),
                user_prompt_path=user_prompt_path,
                workflow_source=Path(run.program_ref) if run.program_ref else None,
                **opts,
            )
            self._write_user_prompt(
                user_prompt_path,
                decoded.get("starting_prompt") or driver.prompt_fallback,
            )
        self._register_driver(run.run_id, driver)
        return driver

    def restore_driver(
        self,
        run: Any,
        snapshot: Mapping[str, Any] | None = None,
    ) -> GraphDriver:
        """重开/恢复时取回驱动：内存实例优先（run_id 键），缺席按快照重建。"""
        driver = self._drivers.get(run.run_id)
        if driver is None:
            if not snapshot:
                raise ValueError(f"run {run.run_id} 无法恢复（无快照且内存无实例）")
            graph_id = str(snapshot.get("graph_id") or "") or self._derive_graph_id(run)
            driver = self._driver_from_blob(run, graph_id, snapshot)
            logger.info("run %s rebuilt from driver snapshot", run.run_id)
        driver.prepare_recovery(
            running=run.status == "running",
            reopened_at=(run.reopened_at.isoformat() if run.reopened_at else None),
        )
        self._register_driver(run.run_id, driver)
        return driver

    def _register_driver(self, run_id: str, driver: GraphDriver) -> None:
        """登记驱动并按插入序裁剪超出容量的陈旧条目。"""
        self._drivers[run_id] = driver
        if len(self._drivers) > _DRIVER_CACHE_LIMIT:
            for stale in list(self._drivers)[:-_DRIVER_CACHE_LIMIT]:
                del self._drivers[stale]

    def reload_definition(self, run: Any, driver: Any, source: Path) -> dict[str, Any]:
        """重读工作流源并翻译为换装载荷：读文件在本层，换装决策在驱动侧。

        翻译沿用驱动快照里的原始入参变量与工作目录注入，等价于按当前
        文件内容重走一遍创建期翻译；已合并子图的重建预案一并备好。
        """
        snap = driver.snapshot()
        render_vars = dict(snap.get("variables") or {})
        user_prompt = snap.get("user_prompt_path")
        work_dir = snap.get("work_dir")
        if user_prompt and work_dir:
            render_vars.update(WORK_DIR=work_dir, USER_PROMPT=user_prompt)
        program = translator.translate_program(source, render_vars, source.parent)
        return {
            "program": program,
            "subgraph_plans": self._reload_subgraph_plans(driver, program),
        }

    def _reload_subgraph_plans(self, driver: Any, program: Any) -> list[tuple[str, list[dict]]]:
        """已合并子图的重建预案：按新程序 DAG 定位生成器并校验子图 yaml 一致。

        子图 yaml 缺失或与已合并节点不一致时显式拒绝重载——恢复路径允许
        降级失败续跑，重载不允许（半新半旧的定义现场比失败更糟）。
        """
        plans: list[tuple[str, list[dict]]] = []
        for spec in scheduler.subgraph_restore_specs(program.dag, driver.state, driver.variables):
            sub_config = self._load_subgraph_yaml(
                driver.work_dir,
                spec.node["id"],
                spec.output_rel,
            )
            prefixed = (
                scheduler.restored_subgraph_plan(spec.node["id"], spec.sub_ids, sub_config)
                if sub_config is not None
                else None
            )
            if prefixed is None:
                raise ValueError(f"子图 yaml 缺失或与已合并节点不一致，拒绝重载：{spec.node['id']}")
            plans.append((spec.node["id"], prefixed))
        return plans

    def _apply_provider_overrides(self, opts: dict[str, Any]) -> None:
        """create 供给的配置修正：provider_index 整数化、model_pinned 缺省推导。"""
        provider_index = opts.get("provider_index")
        if provider_index is not None and not isinstance(provider_index, int):
            try:
                opts["provider_index"] = int(provider_index)
            except (TypeError, ValueError):
                raise ValueError(f"provider_index 需为整数：{provider_index!r}") from None
        model = opts.get("model")
        if model and "model_pinned" not in opts:
            bound_model = (
                self._ctx.services.provider_model(opts.get("provider_index")) if self._ctx else None
            )
            opts["model_pinned"] = bound_model is not None and model != bound_model

    def _apply_thinking_overrides(self, opts: dict[str, Any], work_dir: str) -> None:
        """reviewer thinking 配置按 ctx 服务解析后回填。"""
        if self._ctx is None:
            return
        thinking_enabled, thinking_budget = self._ctx.services.reviewer_thinking(
            work_dir,
            opts.get("reviewer_thinking_enabled"),
            opts.get("reviewer_thinking_budget_tokens"),
        )
        opts["reviewer_thinking_enabled"] = thinking_enabled
        opts["reviewer_thinking_budget_tokens"] = thinking_budget

    def _build_driver(
        self,
        graph_id: str,
        yaml_path: Path,
        work_dir: Path,
        variables: dict[str, Any],
        agents_dir_base: Path | None = None,
        initial_payload: dict[str, Any] | None = None,
        starting_prompt: str | None = None,
        user_prompt_path: Path | None = None,
        applied_reopen_at: str | None = None,
        workflow_source: Path | None = None,
        **opts: Any,
    ) -> GraphDriver:
        """组装正规驱动（create/快照恢复共用的唯一构造点）。"""
        ctx = self._ctx
        if ctx is None:
            raise RuntimeError("GraphPlugin 未安装（缺少 PluginContext）")
        render_vars = dict(variables)
        if user_prompt_path is not None:
            render_vars.update(WORK_DIR=str(work_dir), USER_PROMPT=str(user_prompt_path))
        program = translator.translate_program(yaml_path, render_vars, agents_dir_base)
        if user_prompt_path is not None and any(
            ".workflow/status.json" in str(node) for node in program.dag.nodes
        ):
            _publish_workflow_status(work_dir, workflow_source or yaml_path, user_prompt_path)
        state = _build_graph_state(graph_id, program, initial_payload)
        return GraphDriver(
            graph_id=graph_id,
            program=program,
            state=state,
            work_dir=work_dir,
            yaml_path=yaml_path,
            input_variables=variables,
            agents_dir_base=agents_dir_base,
            starting_prompt=starting_prompt,
            user_prompt_path=user_prompt_path,
            applied_reopen_at=applied_reopen_at,
            options=opts,
        )

    def _derive_graph_id(self, run: Any, fallback: str = "") -> str:
        """graph_id 派生（驱动内展示标签，不参与寻址）：label 优先；
        无 label 按程序原始位置摘要——同一程序重复直连派生同一标签。
        """
        label = (run.labels or {}).get("name") or ""
        if label:
            return label
        source = run.program_ref or fallback
        if not source:
            return ""
        digest = hashlib.sha256(source.encode()).hexdigest()[:8]
        return f"{Path(source).stem}_{digest}"

    def _resolve_work_dir(
        self,
        run: Any,
        program_path: Path,
        variables: dict[str, Any],
    ) -> Path:
        """yaml 声明 work_dir（变量渲染）→ run.work_dir → 当前目录。

        相对声明以 run.work_dir 为基准（外部回退值即项目目录）；run 记录行
        的 work_dir 不改写，仅驱动执行面取解析结果。
        """
        cfg = translator.parse_yaml_file(program_path) or {}
        yaml_path = Path(run.program_ref) if run.program_ref else program_path
        work_dir = translator.resolve_work_dir(
            cfg,
            variables,
            run.work_dir,
            yaml_path,
            None,
        )
        work_dir.mkdir(parents=True, exist_ok=True)
        return work_dir

    def _driver_from_blob(
        self,
        run: Any,
        graph_id: str,
        blob: Mapping[str, Any],
    ) -> GraphDriver:
        """按驱动快照重建驱动（恢复链的共用工事；yaml 缺失即抛错）。"""
        run_id = run.run_id
        yaml_path = Path(str(blob.get("yaml_path") or ""))
        if not yaml_path.is_file():
            raise FileNotFoundError(f"graph {graph_id} 程序档案缺失: {yaml_path}")
        agents_base = blob.get("agents_dir_base")
        work_dir = Path(str(blob.get("work_dir") or Path.cwd()))
        user_prompt_path = Path(
            str(
                blob.get("user_prompt_path") or (work_dir / ".workflow" / run_id / "user_prompt.md")
            )
        )
        driver = self._build_driver(
            graph_id=graph_id,
            yaml_path=yaml_path,
            agents_dir_base=Path(agents_base) if agents_base else None,
            work_dir=work_dir,
            variables=dict(blob.get("variables") or {}),
            initial_payload=_state_payload(graph_id, blob),
            starting_prompt=blob.get("starting_prompt"),
            user_prompt_path=user_prompt_path,
            applied_reopen_at=blob.get("applied_reopen_at"),
            workflow_source=Path(run.program_ref) if run.program_ref else None,
            **pick_runner_options(dict(blob.get("options") or {})),
        )
        driver.resume_workspace_seq(int(blob.get("workspace_seq", 0) or 0))
        if not user_prompt_path.exists():
            self._recreate_user_prompt(
                run_id,
                user_prompt_path,
                blob.get("starting_prompt") or driver.prompt_fallback,
            )
        self._restore_subgraph_dag(driver)
        return driver

    def _recreate_user_prompt(self, run_id: str, user_prompt_path: Path, content: str) -> None:
        """恢复后补建缺失的 user_prompt.md（成败都日志留痕）。"""
        try:
            user_prompt_path.parent.mkdir(parents=True, exist_ok=True)
            user_prompt_path.write_text(f"{content}\n", encoding="utf-8")
            logger.info(
                "run %s: recreated missing user prompt file after restore: %s",
                run_id,
                user_prompt_path,
            )
        except OSError:
            logger.warning(
                "run %s: user prompt file missing after restore: %s",
                run_id,
                user_prompt_path,
            )

    def _restore_subgraph_dag(self, driver: GraphDriver) -> None:
        """快照恢复后重建子图节点的 DAG 拓扑（读文件在本层，合并决策在 scheduler）。

        子图节点在运行时动态 add_nodes 合并进 dag，持久化只存
        subgraph_node_ids（id 列表，不含 depends_on）——重启后 dag 只有
        主图节点，须从子图 yaml 重新构造带前缀节点补回。
        """
        state = driver.state
        for spec in scheduler.subgraph_restore_specs(
            driver.dag,
            state,
            driver.variables,
        ):
            sub_config = self._load_subgraph_yaml(
                driver.work_dir,
                spec.node["id"],
                spec.output_rel,
            )
            prefixed = scheduler.restored_subgraph_plan(
                spec.node["id"],
                spec.sub_ids,
                sub_config,
            )
            if prefixed is None:
                scheduler.fail_unrestorable_subgraph(
                    state,
                    driver.dag,
                    spec.node["id"],
                    spec.sub_ids,
                )
                continue
            scheduler.merge_restored_subgraph(
                state,
                driver.dag,
                spec.node["id"],
                prefixed,
            )


def _state_payload(graph_id: str, blob: Mapping[str, Any]) -> dict[str, Any]:
    """驱动快照 → GraphState 的 initial_payload 构造数据。"""
    state = blob.get("state") or {}
    return {
        "graph_id": graph_id,
        "status": blob.get("status", "pending"),
        "nodes": state.get("nodes", {}),
        "rollbacks": state.get("rollbacks", {}),
        "rollback_edges": state.get("rollback_edges", []),
        "retired_instances": state.get("retired_instances", {}),
        "operations": state.get("operations", {}),
    }


def _build_graph_state(
    graph_id: str,
    program: Any,
    initial_payload: dict[str, Any] | None,
) -> Any:
    """驱动状态的构造薄层（全新态/快照载荷整体重建）。"""
    from workflow_controller.plugins.graph.driver.state import GraphState

    return GraphState(
        graph_id=graph_id,
        workflow=program.workflow,
        node_ids=program.dag.all_ids(),
        initial_data=initial_payload,
    )
