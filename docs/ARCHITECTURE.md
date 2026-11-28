# Workflow Controller 架构

## 概览

Workflow Controller 是一个**领域无关的 AI Agent 调度框架**，核心是 **Loop Engine + LLM Reviewer** 反馈回路（Ralph Loop：Worker → Reviewer → feedback → Worker）。它通过 **Driver 抽象层**运行 agent（Claude Code SDK 进程内 / Claude CLI 子进程 / opencode CLI 子进程 / Codex CLI 子进程；CLI 类 runtime 支持 GLM 等第三方端点），并在其上提供**可接管控制面**（实时看 / 暂停 / 改 / 继续）。

> `ops` = operations（调度执行），非 operator。框架不绑定任何领域语义阶段；领域知识靠 plugin / skill / agent / prompt 注入。算子生成是其首个应用场景，但同一个框架可驱动代码迁移、文档工程等任意长 horizon agent 任务。

两个 UI 前端 —— **Web**（FastAPI + Vue 3 SPA）、**Headless**（JSON→stdout）—— 共享同一个 CoreAPI 层，后者委托给 Core orchestrator 与 Driver 层。

## 架构图

架构图源文件：`docs/imgs/ARCHITECTURE.drawio` — 用 [draw.io](https://app.diagrams.net) 打开编辑后可导出图片本地查看；导出图片不入库。

## 统一入口：`boot()`

两个 UI 都调用同一个函数启动：

```python
# api/core_api.py
async def boot(
    config: AppConfig,
    config_path: Path,
    settings_dir: Path | None = None,
) -> tuple[Orchestrator, CoreAPI]:
    ...
    # 把 yaml 的 runtime 注入 factory 全局，使 worker/reviewer/feedback/breather
    # 所有 create_driver(None) 调用点都遵守 workflow-controller.yaml（默认 claude_sdk，GLM/第三方 claude_cli）
    set_config_runtime(config.runtime)

    db_path = Path(config.project_dir) / ".workflow-controller" / "events.db"
    orch = Orchestrator(config, db_path)
    orch.config_path = config_path
    cfg_dir = settings_dir or default_config_dir()
    orch.auth_rotator = AuthRotator(
        state_path=cfg_dir / "auth_state.json",
        env_path=cfg_dir / ".env",
        runtime=config.runtime,
    )
    await orch.start()
    # 归属恢复 → run 平面装配 → run 恢复链 → 关停步骤登记（组合根单处排序）
    await orch.affiliation.recover_at_boot(orch.pool)
    coordinator = RunCoordinator(
        orchestrator=orch, run_store=orch.run_store, bus=orch.bus,
        driver_factories=orch.plugin_registry.run_driver_factories(),
        affiliation=orch.affiliation,
    )
    await coordinator.recover_at_boot()
    orch.register_shutdown_step(coordinator.shutdown)  # 先于 session 运行时停止
    api = create_core_api(orch, config_path, coordinator)
    return orch, api
```

> `settings_dir` 让 Headless 注入自定义配置目录；`set_config_runtime` 把 `workflow-controller.yaml` 的 `runtime` 字段注入 `driver/factory.py` 全局，使 web/headless 两路的所有 `create_driver(None)` 调用点（worker/reviewer/feedback/breather）都遵守 yaml 配置——否则 Headless 会卡在默认 `claude_sdk`（见 factory.py 的三级优先级：`WORKFLOW_CONTROLLER_DRIVER` env > 显式参数 > yaml runtime > 默认）。

| UI | 文件 | 调用 |
|----|------|------|
| Web | `web/app.py:lifespan` | `await boot(cfg, _config_path)` |
| Headless | `headless.py:run()` | `await boot(self._config, Path(self._config_path), settings_dir=sd)` |

## 内核与控制台边界

> 本仓只承载内核与内置插件；下述控制台（web）实现住控制台发行包，本节保留其边界契约作为内核侧的约束。

内核（core / api / session / store / plugin SDK / driver / event / hooks / watchdog / config / headless）可以在不装配控制台的情况下启动和运行：Headless 与 API 模式只经 `boot()` + CoreAPI 完成 Run 的创建、观察、控制与崩溃恢复。headless 验收在干净子进程内覆盖 Run 的完成、失败与崩溃恢复路径，并以 meta-path 导入守卫阻断 `workflow_controller.web` 的任何导入，锁定「没有控制台也能运行内核」的验收结果。

控制台（`web/`）是内核的可选客户端与装配方式：`app.py` 只承担组合根（配置、日志、boot、装配调用、关停）；`console.py` 聚合领域路由与插件路由，并挂载 Insight 反代与 SPA；`insight.py` 与 `spa.py` 不依赖内核。路由处理器只通过 CoreAPI 访问内核，不读取 orchestrator。自有 API 统一挂 `/api/v1` 正式路径（旧 `/api` 兼容别名已删除）；Insight 反代路径与插件贡献路由不参与前缀切换。

Breather 插件贡献 `/api/breather/state|handoff|cancel`，与 Insight 的 `/api/breather/*` 反代共用前缀。运行时启用的插件路由须插在 Insight 反代之前；禁用时这些专有路径返回明确的禁用状态，不转发给 Insight。插件配置页面可将单文件不超过 10 MiB 的 UTF-8 `.md`/`.txt` 指令上传到守护进程启动目录；文件名只接受纯文件名、允许覆盖同名文件（内置指令下载文件名固定，先下载→编辑→再次上传同名文件必须能落盘），配置仍保存路径字符串。

Breather 交接配置使用单一 `handoff_prompt_path` 指令文件：`<!-- workflow:main -->` 写主代理怎样交接自身工作，以及交接后怎样重新派发仍在途的子代理（子代理自动交接关闭时无需此项）；`<!-- workflow:group -->` 写每名子代理怎样交接自己的工作；可选 `<!-- workflow:common -->` 写共同要求。普通主代理交接读取 main 段；子代理触发整组交接时读取 group 段，若主代理也越线，额外读取 main 段指导主代理摘要。未分段的旧文件保持原样。内置下载名采用 `workflow-handoff-instructions.md` 与 `workflow-continuation-instructions.md`。整组交接仍附加清单及完成信号的固定流程，旧 `group_handoff_prompt_path` 仅在统一路径未设置时兼容读取。内置交接与接续指令可从设置页下载为 Markdown；交接文件在正文前带有供用户编辑的注释，设置页只保留简短引导。插件通过 `Plugin.builtin_instruction_files()` 扩展点声明文件，`CoreAPI.plugin` 从已发现的插件类型取得内容，Web 路由只调用 CoreAPI。源文本分别位于 `handoff.py` 的 `HANDOFF_AGENT_PROMPT`、`RESUME_PROMPT` 和 `group_handoff.py` 的 `GROUP_HANDOFF_PROMPT`。主代理或子代理的有效阈值为 100% 时，该侧只采集用量，不自动触发交接；手动强制交接仍可用。Claude CLI 子代理转录文件可能先出现无类型的用户行；轮询器在后续助手行给出 `attributionAgent` 后更新子代理类型，供界面和整组交接清单使用。

## CoreAPI — 共享服务层

```python
@dataclass
class CoreAPI:
    session: SessionAPI     # start/stop/pause/play/restart/delete sessions + loop 监视概要
    event:   EventAPI       # incremental event query (cursor-based) + handoff chain
    config:  ConfigAPI      # read/write AppConfig + hooks + sync
    pipeline: PipelineAPI   # multi-stage progress snapshot
    run:     RunAPI         # run 生命周期、组控、成员操作、驱动通道与事件游标
    usage:   UsageAPI       # token/cost aggregation
    auth:    AuthAPI        # key 状态/激活/验证与全局认证设置
    plugin:  PluginAPI      # 插件清单/启停/安装/配置读写、router 获取与交接状态聚合
    watchdog: WatchdogAPI   # 告警历史查询（直读事件库筛 fault.*）
```

所有 UI 都通过这九个子 API 操作。没有任何 UI 直接访问 `orchestrator` 或 `event_store`。

## 包角色与依赖门禁

Workflow Controller 的运行时对象存在必要的双向协作（例如 `session/core/driver/store`），因此不强行把源码压成线性 DAG，而是通过 Import Linter 与架构测试共同实施“禁止依赖矩阵”：

| 源包 | 禁止依赖 | 说明 |
|---|---|---|
| `web/routes`、`headless` | 除 `api` 外的 `workflow_controller.*` 后端内部包 | 路由只能额外依赖其他 `web/routes` 模块；`web/app.py` 是唯一 Web 组合根 |
| `web/context_bay`（CANN 领域暂存） | 除 `api`、`context_bay_exporter`、`transcript`、包内互引外的 `workflow_controller.*` 后端内部包 | 领域暂存模块；`config.settings` 常量边为符号级登记债务，S2b 清理，门禁盯防回归 |
| `transcript`（中性转录层） | 一切 `workflow_controller.*` 包（含 `__main__`），只依赖标准库 | 转录源定位与 claude-jsonl / opencode.db 编解码；只做格式翻译，不进控制面 |
| 内核各包（`api`、`core`、`transcript`、`plugins`、`__main__`、`headless` 等） | `context_bay_exporter`（领域上传包） | CANN 上传面只由 Console/领域组件消费，内核导入链不触及 |
| `api`、`core` | 具体 `workflow_controller.plugins.*` | 公共计算放入中立约定模块，插件状态通过公开 API/端口读取 |
| `plugin` SDK | `core`、`api`、`web`、具体 `plugins.*` | SDK 只定义契约，不反向绑定宿主实现 |
| `plugins.<name>` | 其他 `plugins.<name>` 以及 `core`、`driver`、`store`、`api`、`web` | 内置插件只依赖 SDK、服务端口与公共基础包；宿主私有实现必须通过端口访问 |
| `driver`、`event`、`hooks`、`store`、`watchdog` | `api`、`web`、具体 `plugins.*` | 底层运行时不得反向进入控制面或领域插件 |

Import Linter 对当前无违规的边界实施零容忍检查：`plugin` SDK 不反向依赖宿主或具体插件，内置插件不依赖兄弟插件，低层运行时不反向进入控制面或领域插件，Console 静态模块不依赖内核，路由不回导组合根，中性转录层零 workflow_controller 依赖，内核各包不导入领域上传包。它检查源码中的直接导入并覆盖各包的子模块；经中立协议、事件或注入端口形成的间接依赖不作为违规。

架构测试覆盖完整矩阵，并显式注册顶层包角色；新增 `src/workflow_controller/<package>` 必须先登记，否则门禁失败。子包继承所属顶层包的角色，不需要单独登记。类型检查专用的纯 `if TYPE_CHECKING:` 或 `if typing.TYPE_CHECKING:` 分支不属于运行时依赖，两个门禁均忽略；但与条件组合的表达式仍按运行时依赖检查。模块边界匹配同时覆盖包本身及其子模块，不能通过把导入移动到 `__init__.py` 绕过。

架构扫描器只分析源码中可静态确定的 `import`、`from ... import ...` 以及参数为字符串字面量的 `__import__` / `importlib.import_module`；运行时拼装的模块名由运行时测试负责。扫描器按 Python 源文件编码声明读取文件，不导入被扫描模块，也不使用模块发现或加载 API。扫描记录的源路径固定为相对仓库根的 `src/...` 格式，历史债务基线依赖这一格式。

迁移期间只允许 `tests/architecture/dependency_debt.py` 和 `tests/architecture/boundary_debt.py` 中逐条登记的历史债务。每条债务精确到源文件、访问目标、符号和预期次数；新增违规、次数增加、次数减少或债务已经消失但基线没有同步更新，都会失败。失败诊断只列出新增或增加、移除或减少的变化项，不重复输出未变化的完整基线。最终目标是 UI 绕过 CoreAPI、Core 依赖具体插件以及内置插件访问宿主私有实现三类债务均归零。

导入门禁的规则定义、违规模型和失败诊断统一维护在 `tests/architecture/rules.py`，属性边界门禁的对应定义与诊断维护在 `tests/architecture/boundaries.py`。两者都输出违规位置、规则含义、实际与允许次数及首选修复方式，完整诊断文本作为用户可见接口由精确断言保护；源码扫描与历史债务基线分别由 `scanner.py` / `boundaries.py` 和 `dependency_debt.py` / `boundary_debt.py` 负责。

两个门禁均由版本化的 pre-push 质量门禁在普通后端测试之前运行，也可以分别通过 `uv run lint-imports --no-cache` 与 `uv run pytest tests/architecture -q` 独立执行。属性边界门禁按 AST 属性段区分 `ctx.orchestrator` / `self._ctx.orchestrator` 公开入口直连与 `self._orchestrator` / `plugin._orchestrator` 私有兼容 shim；宿主私有成员只沿这些已知入口或显式 `orchestrator` 参数继续匹配，无关接收者不算违规。`app.state.orchestrator` 的拦截覆盖属性链、字符串 `getattr`、先赋别名再访问的 `app.state` 别名链与属性名非字面量的 `getattr` 变量形态（可静态归一的常量名按值判定，其余取值无法排除 `orchestrator`，一律计违规）。别名链的赋值形态覆盖普通赋值、带类型注解的赋值与 walrus 绑定，别名再赋给新名字（链式别名）沿值链继续传播，赋值表达式直接串接的属性访问也按其值链判定；别名与常量名均按词法作用域解析——绑定只统计所在作用域及其外层作用域，本作用域同名重绑遮蔽外层绑定，兄弟函数之间的同名互不串扰；别名与常量均按同一作用域内源码顺序的末次绑定判定，末次赋值不再是 `app.state` 链（或字符串字面量）时不再沿用早前绑定，常量名末次赋值为非字符串后按变量形态 `getattr` 处理。同一行上的每个访问节点都单独计入债务次数。它与导入扫描器一样遵循 Python 源文件编码声明，且每条规则都必须有合成违规与允许访问覆盖。属性访问、环境隔离和运行时生命周期不属于导入图约束，应由各自的专属测试覆盖。运行时动态导入（`__import__` / `import_module`，含运行时拼装模块名）由白名单门禁约束，只允许出现在插件装载器（`plugin/registry.py`）单点。

测试环境隔离由 `tests/architecture/hygiene.py` 的源码门禁负责：测试不得直接给从 `workflow_controller` 根包或子模块导入的全局对象属性赋值，也不得通过 `os.environ`、`os.putenv` 或 `os.unsetenv` 直接修改进程环境；其中环境映射的直接变更包括条目赋值、删除、变更方法和 `|=` 增强赋值。统一使用 Pytest `monkeypatch`，确保测试结束后自动恢复状态。扫描器按 Python 源文件编码声明读取测试源码，覆盖带别名和不带别名的 `import` / `from ... import ...`，包名只匹配精确的 `workflow_controller` 根包及其子模块，并跟踪 `os` 模块及其 `environ`、`putenv`、`unsetenv` 成员的本地导入名，避免导入写法绕过门禁或相似包名前缀造成误报。

运行期隔离由根 `tests/conftest.py` 加载的 `tests/support/pytest_hermetic.py` 统一实施。普通测试会在收集阶段前把 `HOME` 与 `XDG_CONFIG_HOME` 切换到会话级临时目录，并禁止外部网络连接及 `getaddrinfo`、`gethostbyname`、`gethostbyname_ex`、`gethostbyaddr`、`getfqdn` 等公开 DNS 查询入口；loopback、Unix Socket 等进程内集成测试仍可使用。每个测试结束时校验进程环境并在报告泄漏前恢复现场，测试会话结束后再恢复启动 Pytest 前的环境、网络函数与临时目录。需要真实 LLM 或外部服务的测试必须标记为 `live_llm` / `reviewer_recall`，默认跳过，只有显式传入 `--live` 才解除外部网络限制。配置隔离完全由测试进程环境实现，生产配置解析不读取测试专用变量，避免测试访问用户真实 Workflow Controller 配置的同时不改变生产行为。

### CI 用词检查

GitHub Actions 的 `forbidden-word-check` job 与 `backend` 并行并独立报告结果。它拉取完整 Git 历史，通过 `scripts/check_ai_words.py --pr` 扫描 base 与 head 的共同祖先到 head 之间的最终差异。PR 使用自身的 base 和 head，合并队列使用 `merge_group` 的 base 和 head，发布流程调用 CI 时使用推送前后的提交。base/head 为全零（如新建分支推送）或 base 提交不可获取时，job 以带指引的 `::error` 失败，不静默放行。

拆分后 `backend` 不再承载词检拦截，仓库分支保护（含合并队列的检查清单）必须把 `forbidden-word-check` 登记为必需检查，否则合并前不再强制词检。发布流程中词检失败发生在 `version` job 推送标签之后，会留下已有标签但没有 Release 的中间态；恢复方式与发布流程中其他 CI 检查失败相同（继续向前 bump 版本重发）。

## Session 生命周期

### OpenCode 文件归因失败收尾

独立 OpenCode 文件归因插件在 `tool.execute.before` 建立同步 capture 窗口，并同时订阅
工具失败事件。`session.tool.failed` 按 `callID` 立即以错误 outcome 收尾失败或中止的工具，
避免等待 stale TTL 期间与后续窗口重叠；正常 after hook 仍根据工具 metadata（包括非零退出码）
记录最终结果。OpenCode 的 `filePath`/`filepath` 参数与 Python 侧命名一并提取为绝对路径提示，
确保 edit/write 的行归因覆盖实际目标文件。

```
                      ┌─▶ PAUSED ─▶─┐
CREATING → IDLE → RUNNING            COMPLETED/STOPPED/FAILED
                      ├─▶ (stop) ───┘
                      └─▶ WAITING_RETRY ─▶ (wake) ─▶ RUNNING
```

- **SessionAPI.start(project_dir)**：`generate_hook_config` → 读 `workspace.json` 判断是否同项目 → 写 `workspace.json`（记录 project_dir；事件与列表恒守护进程库，换项目不清空）→ 优先用该目录的 `workflow-controller.yaml`（无则全局 config）构造 worker `SessionConfig`（`loop`/`runtime`/`system_prompt`/`starting_prompt` 等取自 yaml）→ `start_session_with_config`
- **Claude CLI hook 前置**：默认 runtime 为 `claude_cli` 时，controller 启动 Hook Server 后即按当前 endpoint 刷新项目 `.claude/settings.json`；激活任意 `claude_cli` session 前再次刷新目标 cwd 的 hooks，并要求 Hook Server 已在役。刷新失败或 Hook Server 缺失时 activation fail-fast，防止旧 session 继续访问已漂移的历史端口。
- **CLI driver hook 归属**：Orchestrator 经 driver factory 创建具体 runtime driver 并调用其 `refresh_hooks`；runtime 分派只存在于 factory，CLI driver 不比较 runtime 字符串。
- **Hook Server 实际端口**：controller 实例维护 `effective_hook_port`，端口只来自启动参数 CLI `--hook-port`，未指定时使用默认 `18765`；YAML 不再支持 `hook_server.port`。Hook Server 绑定、Claude `.claude/settings.json` 与 Codex `WORKFLOW_CONTROLLER_HOOK_SERVER_URL` 都以 `effective_hook_port` 为唯一事实源。完整设计见 [hook-server-port.md](specs/hook-server-port.md)。
- **Hook 启动事务回滚**：Hook Server 绑定成功后的任何启动步骤失败（如 Claude hooks 写入失败）都必须先停止已绑定的 Hook Server，再卸载插件并停事件总线；不能只丢弃对象引用，留下仍监听端口的 TCPSite。
- **SessionAPI.delete()**：统一执行 `stop_session → pool.remove → bus.emit(session_delete) → event_router.delete_session`；Web DELETE 路由只做参数校验和 HTTP 错误映射，不另行实现删除链路
- **SessionAPI.inject_prompt()**：`pool.inject_prompt()` + `event_store.store(LOG_MANUAL)`
- **Claude Stop 后台任务判定**：CLI 与 SDK 保留 Stop hook 请求里的 `background_tasks` 快照；运行中任务非空时放行 Stop，不调用 reviewer。快照为空时照常审查；旧版运行时没有该字段时，沿用 SubagentStart/Stop 运行中名单。事件记录只存任务 ID、类型与状态，不存命令正文。该快照覆盖后台 shell 与子代理；Bash 超时转后台若被 Claude 登记为运行中 shell，也走同一判断。子代理跟踪仍供其他生命周期消费者使用。
- **Stop HTTP 集成测试隔离**：测试启动 Orchestrator 时关闭其常驻 Hook Server，再用 aiohttp TestServer 为同一 Stop 回调分配临时端口，避免占用控制器默认端口。
- **发送人工消息**：直连 session 由 SessionAPI 单请求完成「恢复池外历史对象 → 注入并留痕 → 若可续跑则恢复」；workflow 成员只经 RunCoordinator 的成员操作面完成同一请求内的顺序操作，由它检查 run 状态与成员资格，再调用 Session Orchestrator 原语。前端不能拆成 inject、resume 两次请求，避免两次请求之间成员出池或状态变化而丢失输入。
- **恢复与超时**：崩溃遗留的 running 会话恢复成 idle/user_pause，保留 SDK 会话身份供 Run 驱动复用；Graph 成员显式 `timeout <= 0` 表示不挂超时定时器，未配置则沿用 7200 秒默认值。复用成员时由 GraphDriver 在 RestartSession 动作中提交当前模型配置，RunHost 在续跑前应用。若恢复时发现成员已自然结束，GraphDriver 请求 Run 平面回放该成员的持久结局，再据此推进执行或验收，不重建验收 session。通用 resume 状态机测试不得触发 OpenCode 宿主 DB 兜底，应预置 SDK/native session ID 或显式隔离恢复入口；OpenCode 恢复启发式由专用测试守护。
- **上下文管理**：插件以通用能力声明是否管理上下文窗口；Orchestrator 将布尔策略交给 driver，OpenCode driver 据此配置自动 compaction，核心不依赖插件名称。事件读连接按次关闭，SSE 的共享快照限定在单个 Web 应用实例内，事件回放与最近活动通过 SQL LIMIT 限量读取。

> **WAITING_RETRY**：reviewer 返回 `wait_and_retry` 时设置（10 分钟退避窗口），窗口结束自动唤醒回 `RUNNING`。
> **终态映射**：`LoopStatus`（`COMPLETED` / `MAX_ITERATIONS` / `BLOCKED` / `ERROR` / `MANUAL_STOP`）映射到 `SessionState` 时，**`MAX_ITERATIONS` 也坍缩为 `COMPLETED`** —— 真实终止原因看 `final_loop_status` 字段（SessionInfo 已暴露）。

### Claude CLI 会话文件定位

Claude CLI 子进程以 Session 的项目目录为 cwd；`--add-dir` 只授予额外目录访问权限，不改变会话文件的项目归属。会话列表和导入按 `CLAUDE_CONFIG_DIR`、环境中的 `HOME/.claude`、进程用户目录查找 JSONL；额外扫描 `/home`、`/Users` 下其他用户的 `~/.claude` 由配置项 `claude_scan_user_homes` 显式开启（默认关闭，共享主机上无条件扫描会暴露其他 Unix 用户的会话，可经 Web 端口导入）。所有探测点统一 `except OSError` 兜底，不可读主目录（如 macOS `/Users/Guest`）只被跳过，不会让列表/导入整体报错。导入优先匹配请求的项目目录；兼容旧驱动写入其他项目目录的历史文件时，仅接受唯一会话 ID 命中，避免导错会话。

### 多 Session 并发运行时

Orchestrator 维护 `session_id -> SessionRuntime(engine, task)` 注册表。每个 running
session 拥有独立的 loop、停止/重试/auth-switch 事件和人工输入队列，因此启动、暂停、
恢复、停止、Reviewer 放行及 subagent 唤醒只影响目标 session。完成后的 runtime task 会被
回收，Session 与事件元数据继续保留；shutdown 并发停止并等待所有 runtime。

暂停和恢复保留 Session 的当前轮次。循环引擎为恢复后发出的查询分配下一轮编号；第 3 轮暂停后从第 4 轮继续，首轮开始前暂停则从第 1 轮发送初始提示。

Session 创建与 runtime 激活构成原子生命周期：`pool.create` 会先写入 session 元数据，
但后续配置快照、Reviewer 或 loop 初始化任一步失败（包括请求被取消）时，Orchestrator 必须
停止已部分创建的 Reviewer/runtime、清除 overlay，并从 pool 与 events.db 回滚该 session。
只有激活成功返回后，调用方才能把临时工作目录的清理责任移交给 session 删除链路。

Provider 池每个条目带全局唯一的 `custom_name` 显示名（随 `settings.conf` 条目持久化，
`auth_state.json` 同步冗余）：保存整池时，留空条目在模型名未被任何自定义名占用的前提下
自动取模型名，被占用则要求调用方显式命名，显式名重复以 400 拒绝；空模型条目按序号
`provider-<N>` 自动命名。加载无该字段的存量数据时按池内顺序确定性回填——模型名优先，
冲突取第一个空闲的 `<模型名>-backup-<N>`（N 从 2 递增）；回填值随加载写入 auth_state.json，settings.conf
待下一次整池保存时写入。
自定义名只用于 UI 展示与
Provider 身份区分，`provider_index`/model 绑定语义不受影响。

宿主不做目录级写入互斥：任意 session（含跨 run 的成员）可并发操作同一工作目录，
启动、恢复、播放或重启均不检查目录占用。并发写同一目录的协调（任务划分、文件分工、
git 兜底）属调用方责任。SessionConfig 的 `provider_index`/`model`
保存 Worker 绑定，嵌套的 `reviewer` 快照保存 Reviewer 的 enabled、mode、provider/model、
timeout 和 task goal。全局配置只作为新会话默认值；旧 `config_json` 缺少字段时，
`_ensure_session_snapshots` 在下次激活前补齐并通过 `_persist_session_provider` 等路径持久化。
运行期 Reviewer 必须从 `session_id -> ReviewerService` 注册表解析，不回退到全局实例。

provider 轮换通过 session runtime 的专用 auth-switch 中断完成：限流检测先按事件的
`session_id` 找到实际 Worker 或 Reviewer provider，只停止该 Session 当前 driver/query，
取消正在等待的本轮并立即进入使用新 provider 的下一轮，不调用会写入 STOPPED/`loop_status=no`
的 Session 终止接口。若该 session 已无可用 provider，则先停止 driver、排空其 runtime，再将
状态统一持久化为 FAILED、`stop_reason=error` 和对应 `last_error`。Reviewer 使用自己的绑定，
限流时只轮换并重试该 Reviewer 一次；其他 Session 和其他 provider 的 runtime 不受影响。
auth-switch 恢复前必须同时检查 loop stop event 与 session 的 `_stop_requested`：若用户停止或
限流耗尽停止已先发生，停止请求优先，恢复路径只消费 auth-switch 信号，不得把 Session 从
STOPPED 重新写回 RUNNING。

handoff 停止旧 worker 时以 runtime task 完成作为异步屏障；无 runtime 但已是
`loop_status=no` 的 STOPPED Session 也视为已停止。排空后由 breather 插件显式将 worker
转为 IDLE/`handoff_triggered` 并持久化，不轮询等待 loop 隐式改写 SessionState。
等待旧 worker 退出使用本次调用起算的固定窗口，不消耗会话执行预算；执行预算仍由
累计活跃时长驱动。两种时钟参数互斥；固定窗口覆盖 runtime 延迟注册。等待超时或
等待调用自身被取消都只放弃本次等待，不得取消 worker runtime。Plugin 端口仅在显式
启用固定窗口时下发 `wait_timeout`；默认调用保持旧 waiter 的参数形状。
Breather 的普通 pending 路径和 subagent 返回点主动 arm 路径都必须为目标 session 建立
独立的阶段一 watchdog；若 worker 未到达 iteration boundary、异常退出或被外部停止，
watchdog 会按 session id 重置 pending/finalizing/stop-armed 状态，不影响其他 session。
状态重置调用必须显式携带 session id，ContextVar 仅用于同一异步调用链内的属性访问。

## Run 身份（workflow run 记账层）

> 本节描述**现状**（骨架账面，生产不可达；runs 族已并入项目库 events.db，RunStore 含生命周期 API 与扩列）。目标形态：graph 并入 run，控制面见下文[两层协调器章程](#两层协调器章程目标架构)。

执行主体仍是 session（真 driver 子进程）与裸进程（bash 类节点）；run 是加在其上的
**宿主级记账与派发实体**——身份、账面、公共 API 锚，不是执行实体。两类执行单元都
可能没有 session（bash 直跑），所以 run 归属塞不进 session 体系，必须自己当主体。

```python
@dataclass
class RunRecord:
    run_id: str               # 短 uuid，同 session id 风格
    program_ref: str          # "release-notes@0.1.0" / "file:/path" / "skill-md:…"
    format: str               # "dag-yaml@1" / "skill-md" / …——start_run 按它查格式派发表
    status: str               # RunStatus StrEnum：pending|running|paused|completed|failed|stopped
                              # （未知值读回原样透传不抛）
    kind: str                 # RunKind StrEnum：workflow | container | implicit
    work_dir: str | None      # run 工作目录（显式 run 必填——直连启动在 API 层强制；None 仅 implicit run）
    labels: dict[str, str]
    params: dict[str, str]    # 起 run 时的实参（审计 / 复现）
    max_parallel: int | None      # 并发准入上限（None = 不限）
    pause_reason: str | None      # paused 细因：user / auth / projection（落终态即清）
    created_at: datetime
    finished_at: datetime | None
    reopened_at: datetime | None  # reopen 审计戳
    error: str | None
```

- **生命周期**：`Orchestrator.start_run(program_ref, program_path, *, params, labels)`
  mint run_id → 按程序 format 查格式派发表 → 拉起对应解释器 → 立即返回（不等跑完）；
  `finish_run(run_id, status, error=None)` 是 run 终态回写的**唯一通道**（解释器收尾都走
  它；stop 路由与崩溃恢复也复用），同态幂等、换态拒绝、对 kind=implicit 拒绝服务；
  `RunStore.update_status(run_id, status)` 只做非终态迁移（pending/running/paused），
  `RunStore.reopen(run_id)` 仅终态可重开——清 finished_at/error、留 reopened_at 审计，
  同样拒绝 implicit。`attach_to_run(session_id, run_id)` 是显式挂靠入口（新落行
  才发审计事件，幂等 no-op 不重发）；改挂已归属 session 走 adopt/detach 的原子
  转移。解释器不直写
  runs 表——跨插件不耦合 schema。
- **归属**：`SessionConfig.run_id / labels` 照存不解析（argv 类比）；`Session.run_id`
  创建时从 config 继承。`run_id=None` 由 pool 创建咽喉铸隐式 run（kind=implicit、
  label=session 名、状态=成员投影，消费面默认不可见——语义为「None → 铸隐式」，
  新 session 恒有归属）；**unknown run_id 一律拒绝**
  （不兜底自造 run——账面多出调用方没意识到的条目）。run_sessions 行带
  role/context/native_session_id（不透明归属标签），`UNIQUE(session_id)` 在存储层强制
  「每个 session 恰好属于一个 run」。
- **落点**：runs 族表与一切 run/session 事件、名册同库——**守护进程级单库**
  （物理为启动目录 `.workflow-controller/events.db`，语义归守护进程全局，session 事件不经 cwd 路由）。建库为
  `CREATE TABLE IF NOT EXISTS` 与 user_version 盖戳的同一显式事务，幂等可重放；
  schema 不齐一律 fail-fast（零存量无数据可失，删库重建，不做原地补列）；
  WAL 每次启动无条件核对（journal_mode 切换不抛错，
  须核对返回值）；RunStore 是 events.db user_version 阶梯的**唯一写者**，
  每档迁移整体（全部 DDL + 盖戳）由 RunStore 单显式事务执行，EventStore 维持幂等
  DDL、永不触碰；名册写权分层——普通写（单行独立事务）走 EventStore 写连接，
  事务内嵌写（mint 随铸 / discard·delete 链删除）走 RunStore 连接（账面原子的
  连接前提）；双写连接均 WAL + busy_timeout，SQLite 文件级写串行兜底。run 数据根
  `{project}/.workflow-controller/runs/{run_id}/`——锚 `_default_project_dir` 于 boot 解析定格，
  数据根与账本同做全路径 resolve（symlink/相对锚不产生第二真相，不随运行期 cwd
  重锚）；work_dir 显式解析传入（必填——直连 workflow 启动在 API 层强制，graph
  shim 显式解析）且不设在数据根内——数据根只装 Workflow Controller 记录
  （program/export），agent 文件产物（work_dir 内）的删除不归宿主管，
  delete_run 删除面 = 账面 + 数据根。
- **格式派发表**：`FormatInterpreter` 协议住 plugin SDK（`start(run, program_path, params)`
  / `stop(run_id)`），Plugin 子类可选覆写 `get_format_interpreters()` 登记
  `{format: 解释器}`；core 经 SDK 协议查注册表，不 import 具体插件（依赖门禁）。
  未登记格式 start_run 明确报错（格式是开放集合，定制格式的解释器走同一接口注册）。
- **公共 API**：`POST /api/v1/runs`（201 + run_id）、`GET /api/v1/runs/{id}`（状态 +
  sessions[]）、`GET /api/v1/runs/{id}/state`（过程详情：节点状态 + 全节点依赖表 edges + 产物文件清单 + 数据根绝对路径
  root——读 run 数据根的 run-state.json 与目录列表，那是解释器写盘的持久真相（断点
  重跑同源），目录列表不含宿主自身的 .workflow-controller/ 目录（不是产物也不是程序状态）；
  edges 同样透传 run-state.json 里的
  值（解释器写入的全节点 depends 表，供客户端按拓扑序渲染执行进度；旧数据没有则
  null）；非 dag 格式或未落盘时 nodes 为 null；
  run 不在账面 404）、
  `POST /api/v1/runs/{id}/stop`（202 已受理；经解释器 stop 停名下节点，
  解释器停完调 finish_run(run_id, "stopped") 回写终态）、
  `POST /api/v1/runs/{id}/export[?archive=zip]`（导出 cannbot workflow bundle：
  `RunExportService`（`core/run_export.py`）从守护进程单库与 run 根取齐事实，
  经驱动工厂的可选方法 `export_contribution`（SDK 协议 `plugin/run_export.py`）
  把工作流方言翻译交给 format 所有者，核心只做账面装配、会话单元生产、
  结构校验与确定性写包（同 run 并发导出经进程内 run 级锁串行，发布走备份换名——
  任意失败点保留最近一次可用包）；会话单元格式统一 claude-jsonl——claude 直拷、
  opencode 经 `workflow_controller.transcript` 转换轨（读取面 v1/v2 双轨：按会话落表探测
  `session_v2`/`session_message`，miss 回落旧三表，多 runtime 演进见设计稿 §16）；
  产物落 run 根 `export/{run_id}-bundle/`，任意非
  pending 状态可导（时点快照），隐式 run 拒绝；贡献者超时/异常与 zip 写入失败映射
  503，不落 run 任何状态）。路由经 CoreAPI 新增的 RunAPI
  子 API 操作，不直摸 orchestrator（依赖门禁）。
- **与 Graph Runner 的关系**：graph 插件的 `graph_id` 维持插件级身份不动；runs 是宿主级
  公共记账（多解释器共享同一账面，消费者按 run_id 聚合）。目标形态是 graph 并入 run
  （graph_id 降级为 run 上的 label），见下文[两层协调器章程](#两层协调器章程目标架构)。

## 两层协调器章程（目标架构）

> 本节是**目标架构章程**：控制面定型为两层协调器——RunCoordinator（run 平面）与 Session Orchestrator（session 平面）；主结构只有 run 与 session 两个实体（外加 run 键进程这一执行原语），工作流的一切语义（DAG/节点/验收/重试/回滚）由插件以「驱动（RunDriver）」身份注入 run 的决策；事件流（event bus）与轨迹流（file change path）留在主干。上文各节描述现状（As-Is），向本章程演进。

### 章程（正面清单 + 负面清单）

| 层 | 组件 | 职责 | 不做什么 |
|---|---|---|---|
| run 平面 | RunCoordinator（`core/run_coordinator.py` 新——**协作组件簇**而非单类：本体（装配+决策循环+任务注册表）+ RunHost（成员执行，公共命名 + 显式构造注入的服务面）+ `_GroupControlEngine`（组控）/`_AdmissionGate`（并发准入）/`_TerminalSequencer`（终态收尾）/`_RecoveryService`（boot 恢复）——四者为模块内部实现类（`_` 前缀私有命名，无模块外消费者，可见性与 `coord._` 耦合棘轮由架构测试钉死），按变更轴拆分；导出不设组件类——`RunExportService`（`core/run_export.py`）由 `export_run` 惰性装配） | run 生命周期（mint/finish/删除/恢复）；成员编排（决策循环、驱动宿主、动作执行）；run 级控制聚合（暂停/恢复/停止/重启/steer 对任意驱动通用）；run 键进程（RunHost）；run 并发治理（max_parallel 准入；run 级无预算闸）；驱动状态存储；恢复链 | 不直接摸 pool/session 内部（只经 SO 原语）；不解析驱动 blob；不含工作流语义（DAG/节点/验收/重试/回滚/子图）；对无驱动 run 不进决策循环 |
| session 平面 | Session Orchestrator（`orchestrator.py` 进化） | session 激活/去激活与 loop 运行时；reviewer 判定点；per-session auth/limit；hook server 与 native 路由；session 控制原语与**谓词原语**（is_pausable/is_resumable/is_terminal——run 平面只消费谓词、不消费 session 字段值语义）；三个公共钩子（激活后挂靠 / 全路径状态迁移事件 / shutdown 步骤位） | 不含 run 编排语义（run_id 只作归属戳透传，无行为分叉）；不编排多 session；不持有驱动；不做目录占用判定 |
| 共享基础设施 | 组合根装配（`core_api.boot()`） | EventBus · EventStoreRouter · SessionPool · PluginRegistry（驱动工厂注册表）· WatchdogCore · FileChangeRegistry · OverlayRegistry · **SessionRunAffiliationService（归属服务——中立基础设施：打戳热路径 resolve、pool 创建咽喉 mint、显式 attach 写者三方共用，不归 run 平面所有）** | 不属于任何一层 |

### 装配序（组合根成文）

- 装配权归 `core_api.boot()`：RunCoordinator 在 `await orch.start()` 之后、`create_core_api` 之前构造（两者之间是天然装配位）。
- RunCoordinator 显式持有五引用：`orchestrator`（session 平面）、`run_store`（账面）、`bus`（事件）、`plugin_registry`（驱动工厂表——经**独立派发面** `get_run_driver_factories()` 获取，与 interpreter 启动器契约（`get_format_interpreters()`）分离、同一注册键不承载两种语义；core 不 import plugins，依赖门禁不破）、`affiliation`（归属服务，中立基础设施）。
- orchestrator 仍可代为构造 session 侧服务（构造顺序便利），但章程上它只是持有者不是所有者；boot 恢复链由 RunCoordinator 而非 orchestrator 执行（插件装载完成后调用 `run_coordinator.recover_at_boot()`——此时驱动工厂才存在）。
- boot 恢复序（组合根单处排序）：`orch.start()`（名册装载）→ 归属服务自有恢复（缓存重建 + 崩溃窗口补铸 + 正向孤儿回收）→ RunCoordinator 构造 → `recover_at_boot()`。恢复服务按步拆分（名册成员对账 / 分类判定 / 驱动重建 / 续跑拉起），每步按名可跳过（`skip` 名单，启动路径排障用；无驱动 run 的状态重算随 `restore` 步名跳过）；恢复扫描含隐式 run（崩溃后 ghost 投影要重算）；无驱动 run 的容器重估在 boot 形态用 ghost 谓词（ghost 无活 runtime，陈旧 loop_status=yes 不保非终态；idle+user_pause 可恢复断点豁免——与隐式投影的 state 判据同源）；名册成员对账先于账面守卫（runs 族表缺失的升级库不漏 reviewer 重建，恢复仍不造库）；run 级与成员级对账循环共用同一幂等重入骨架（逐项隔离失败，整轮可重放）。
- 关停走同一条装配通道：组合根把协调器关停登记为 orchestrator 的 shutdown 步骤（`register_shutdown_step`），在 session 运行时与池停止之前执行；关停不落终态，非终态 run 由下次 boot 恢复链接续。

### 控制分层（三问三答）

1. **run 在 session 之上还是之间？** 之上（包含关系），不在 session 与 orchestrator「之间」。orchestrator 仍是唯一的 session 运行时；session 平面不感知 run（除归属戳与被动钩子）。
2. **run 与 orchestrator 谁管谁？** RunCoordinator **使用** Session Orchestrator 的原语，从不绕过它直接摸 pool 内部；且对有驱动的 run（kind=workflow）的成员是**唯一写者**（成员 pause/resume/stop/inject 一律经 run 平面；无驱动 run（kind=implicit/container）的成员操作直通 session 平面热路径）。方向恒为 `RunCoordinator → Session Orchestrator → Session`。
3. **驱动坐在哪？** RunCoordinator 的侧翼，经 SDK 协议（`decide(world, notifications) → actions`，默认纯决策：无直接 I/O、无 asyncio、无墙钟）交互，永不直接触碰 orchestrator/pool/event_router——只产出动作，机制全归核心。需逐次同步落盘的驱动可选用 `RunCheckpointDriver` 端口，在决策中途发出 `PersistDriverState` 数据动作并等待宿主提交（graph 驱动将 decide 轮内的多次变更合并为轮末一次提交——轮内崩溃等同崩溃发生在动作派发前，恢复按成员事实对账）；这是批量返回协议的显式例外，存储和成员执行仍由 Run 平面负责。

### 不变量

**RunCoordinator（run 平面）14 条**：

1. run 任务唯一 spawn 口——`_tasks` 注册表之外的 run 任务不存在，shutdown 可 cancel/collect 全部；
2. 决策纯度——驱动 decide 与 on_control、handle_op、projection、snapshot、restore 全部发生在 run 任务（或恢复路径的守护包裹）内，控制/op 通知经 FIFO 投递、不跨协程直调（op/projection 经 OpRequest 应答通道回传结果）；全部经超时守护，超时按驱动异常收尾；
3. 终态唯一写口——`RunStore.finish` 的唯一生产调用者是 RunCoordinator（FinishResult.was_applied 显式表达终态裁决权三态）；implicit run 永不 finish/reopen；stopped 控制受理后拥有终态优先权；
4. 组操作幂等与序——pause 跳过已结束成员绝不覆盖 stop_reason；stop 先杀进程组后停 session；AwaitSessions 超时返回前必已停该 session；
5. 准入公平且事件驱动——Start 类动作超 max_parallel 并发进挂起 FIFO，驱动不感知（world in-flight 计数一致，以激活回执——含失败回执——为界回收）；挂起等待以容量释放事件唤醒，不忙等；终态收尾轮不 pump、清空挂起队列（冻结新出生）；组控/终态受理在成员扫掠/落终态前排空在途激活回执、用户暂停的冻结谓词下沉到每个派发点（批内派发循环/挂起泵对 Start 类与 StartProcess（仅暂停窗）复检 paused——挂起 FIFO、resume 后 pump），迟到出生不逃逸组控与终态；pause_run 杀 run 键进程前、run 任务退出后置杀前均排空在途 spawn 任务（注册表对已派发 StartProcess 全量，无「已派发未落定」窗口）——**手动 pause_run 窗内 run 键进程零存活（终止不是挂起）**，迟到进程 spawn 不逃逸、孤儿零残留；驱动自暂停（`PauseRun`→`driver_pause_run`）为不杀在途的受理分道——成员不挂起、run 键进程放行跑完、回执照常入队滞留 FIFO 到恢复轮，消费方不得由 paused(user) 推断「无在途」；
6. 快照边界——动作批后/控制后/收尾前核心自动落盘驱动快照（最坏崩溃丢失 = 最后一批动作之后）；终态控制受理后驱动裁决 FinishRun 的直退路径不落账但快照照落（终态优先权在控制侧，最终驱动状态仍须落盘）；
7. 单一写者——workflow 成员的 pause/resume/stop/inject 只经 RunCoordinator；RunCoordinator 对成员的一切操作只经 SO 原语；成员级 resume 仅受理正在运行的 workflow Run 中处于用户暂停断点的成员，整组暂停或终态需先经 Run 级恢复/重开，让驱动决定下一步；`SessionAPI.stop_all()` 对每个会话调用 `stop()`，等待本批操作结束后以 `ExceptionGroup` 聚合传播全部停止错误（不丢弃后续失败）；`SessionAPI.delete()` 在停止会话或清理数据之前检查归属，拒绝单独删除 workflow 成员。整 Run 删除由 `RunAPI.delete()` 级联处理。
8. 数据恒守护进程级单库——run_events/run_driver_state/runs 族/events/名册同库不分裂、不分库；session 事件不经 cwd 路由；
9. 打戳唯一入口——事件 run_id/context 维度只从归属缓存来（attach/mint 维护，boot 双源重建）；
10. boot 恢复单路径——runs 表驱动 + 驱动 restore，无第二条恢复链；
11. 恢复不造库——boot 扫描在无账面库的环境零写入（名册驱动判档）；
12. 目录写入无互斥——宿主不做跨 session/跨 run 的 cwd 占用检查，并发操作同一目录的协调属调用方责任；
13. 锁层级——显式挂靠在 SO `_runtime_lock` 临界区内落账（激活成功才记账，不给「已运行不在账面」留窗口；此处对 RunStore 的短写嵌套可接受——RunStore 从不反向获取 SO 锁，无死锁环）；组操作对两层锁先取后释、不交叉持有；
14. implicit 投影单写者——implicit run 的 status/pause_reason 唯一写者 = RunCoordinator 的成员状态事件回调（boot 重算兜底），读写同源不漂移。

**Session Orchestrator（session 平面）10 条**：

1. 行为不变——loop/reviewer/auth/事件内容/交互语义逐字节不变（既有 session 测试全绿即守护）；
2. 不因 run 分叉——无 `if run_id` 行为分支（归属戳透传与钩子回调除外）；
3. 激活唯一入口——一切运行时 session 经 `_activate_session`（锁内：幂等/冲突复查/记账钩子/激活回执）；
4. 终态事件完备——五类终态路径各有 SessionOutcome；引擎旁路与编排/插件层的直写终态一律在直写现场补发 session_outcome，旁路清零；
5. 三字段推断退役——`state+stop_reason+pause_requested` 组合推断由 SessionOutcome 替代；
6. reviewer 快照语义——None=disabled（核心不给默认值）；ghost 恢复重建快照；
7. 钩子最小面——对 RunCoordinator 只暴露三个钩子位 + 原语，不暴露 pool/loop 内部；
8. native 路由权威源——hook 事件路由按 `run_sessions.native_session_id` 列优先；
9. 等人口径不变——manual 队列/gate_question/NL 启发式三路径行为不动；
10. 名册纪律——sessions 名册恒守护进程库；`_persist_session` 字段面是 ghost 恢复的唯一依据。

## Event Store

- **SQLite WAL 模式** — 允许 Web、Headless 并发读写
- **五张表**：
  - `events (id, uuid, run_id, context, session_id, iteration, timestamp, event_type, data, created_at)` — `id` 为持久自增主键（排序/游标键），`uuid` 为客户端幂等键（对外暴露为事件 id），`run_id`/`context` 为 run 维度打戳列
  - `run_events (id, run_id, context, timestamp, event_type, data, created_at)` — run 键事件（bash 输出/驱动事件/状态迁移），按 run_id 归属
  - `evaluations (id, session_id, iteration, is_complete, progress, summary, feedback, artifacts, created_at)` — reviewer 每轮评估
  - `loop_results (id, session_id, iterations, status, error, final_evaluation_id, started_at, completed_at, created_at)` — 每次 loop 终态 + 最终评估引用
  - `sessions (id, name, role, state, loop_status, stop_reason, loop_hold, config_json, native_session_id, created_at, last_activity, current_iteration, birth_handoff_doc, result, last_error, plugin_metadata, final_loop_status, successor_name, successor_id, last_stop_reason, retry_count)` — session 元数据快照（`_persist_session` 每次状态变更 UPSERT 一行）
- **增量查询**：cursor 是 events 表持久主键高水位的字符串形式。SSE 建连先
  `snapshot_event_cursor()` 取当前最大 id 形成快照，经 `get_recent_at_cursor` 回放快照前的
  最近事件；回放结束后用 `get_events_since_cursor` 执行 `id > cursor` 增量查询，并在每批
  响应后推进位置。游标不依赖事件 timestamp，因此即使提交顺序与事件时间不同，也不会漏读或跳跃。

> **Session 元数据持久化（events.db sessions 表，单一来源）**：
> - `native_session_id` 是运行时分配的会话 ID，用于续跑和定位转录。EventStore 和 RunStore 初始化时会在事务内将旧 `sessions.sdk_session_id` 列原地改名，保留已有 ID。Session API 和新写入的事件及插件元数据使用新字段名，历史事件保留原始 payload。
> - `SessionPool._persist_session`（经 `event_router.store_session`）每次状态变更（capture native_session_id / breather 设 result / stop/pause/resume）实时 UPSERT 一行到 events.db 的 sessions 表。SQLite WAL 模式下进程被 kill 也不丢（自动 checkpoint）。
> - 重启时 `load_sessions()` 扫描守护进程库的 sessions 表，还原 ghost session 到内存 pool。
> - `resume` 使用当前 Session 已记录的 `native_session_id` 接续运行时会话。缺少该 ID 时，下次 query 发送初始提示并启动新的运行时会话，Workflow Controller Session 的轮次继续递增。此规则适用于 OpenCode、Claude 和 Codex。
>
> **注入关系**：`EventStoreRouter` 构造早于 `SessionPool`（router 需要 config，pool 还没建）。orchestrator 先建 router，再建 pool：`router.set_run_stamper(stamper)` 让事件写库前按归属缓存补 run 维度戳；sessions 表普通写由 router 直达 EventStore 写连接（store 层对 core 零运行时引用，残留仅 RunStamper 签名的 TYPE_CHECKING 类型引用；mint/discard 的事务内嵌名册写在 RunStore 连接内自持）；`pool.set_event_router(router)` 让 pool 的 `_persist_session` 能调到 `store_session`（守护进程单库直写，不按 session cwd 路由）。缺任一注入，对应方向的功能就空跑。
- **事件类型**（23 个）：`tool_use`、`tool_result`、`tool_call`、`assistant_message`、`notification`、`iteration_start`、`iteration_end`、`user_message`、`result`、`error`、`rate_limit_event`、`subagent_start`、`subagent_progress`、`subagent_stop`、`main_context`、`subagent_context`、`prompt_overlay_registered`、`prompt_overlay_cleared`、`log_auto`、`log_query`、`log_eval`、`log_loop`、`log_manual`

> **子代理整组交接（PR #311）**：Workflow Controller 核心层负责从 Claude Code 转录与 OpenCode session 库读取主代理、子代理的最新输入上下文，`SubagentTracker` 维护在途 ID；Breather 通过 `PluginServices` 查询 session、在途表、事件与交接链，按主/子代理各自阈值决定是否交接，不直接访问 orchestrator/pool/event_router 的私有成员。任一在途子代理越线即暂停主 session，独立 handoff session 为所有在途子代理写各自文档；主代理未越线则恢复原 session，主代理也越线则额外写主代理文档并创建新 session。接续 prompt 逐一携带子代理文档路径和原类型，要求主代理读取清单后在同一条回复中并行派发所有子代理，再验证派发数量与对应 prompt，避免只派一名就再次越线；用户主动暂停/停止不算派发失败。上下文事件供 Web 分别显示主代理和在途子代理用量；交接文档与清单按源 session 和批次隔离。主代理文档由系统命名为 `<源 session ID 前 8 位>-handoff-<轮次>-doc`，子代理文档以对应的子代理 ID 替换此前缀；主代理完成信号以 `-done` 结尾，设置页不允许指定产物名。主代理自身越线时保留现有 Breather 交接边界行为。

> **三个工具事件 `TOOL_USE` / `TOOL_RESULT` / `TOOL_CALL` 的对齐**：四路 driver（`claude_sdk` / `claude_cli` / `opencode` / `codex_cli`）+ HTTP hook server + `session_api` 重放路径统一产出这三个独立事件，data shape 与对齐设计见 [事件系统文档 §1.3](event-system.md#13-工具事件对齐设计)。`TOOL_CALL` 是归一化的「一次工具调用 + 结果」摘要事件，`data = {tool_use_id, name, arg, is_error, error?}`；三路 driver 与 HookServer 的 `tool_use_id` 对应同次 `TOOL_USE` / `TOOL_RESULT`，可供消费方关联。`arg` 由 `extract_tool_arg` 从 input 抽取代表性参数摘要（file_path/command/pattern/query/url），`error` 仅在 `is_error=true` 时填充，由 `extract_tool_error` 直接取对应 tool_result content 的原始字符串（不做额外解析/截断，长内容折叠交给前端）；前端 VirtualLog 据此在失败行下新增续行展示具体原因，并显式跳过 `tool_use`/`tool_result` 事件。`TOOL_RESULT.data` 必含 `tool_name`（供 `breather._on_notify_skill_hook` 四路命中）。

> `subagent_start` / `subagent_progress` / `subagent_stop` 是 sub-agent 跟踪的三个事件（worker 派生的 subagent 经它们可观测）——按生命周期阶段拆成独立 event_type，消费者按类型订阅；`rate_limit_event` 是**driver 无关的归一化限流信号**——四路 driver（claude_cli 结构化 `rate_limit_info` / claude_sdk 异常文本正则 / opencode error dict `statusCode` / codex_cli 错误文本）各自把上游限流·鉴权错误翻译成统一的 `data.limit_signal`（`{kind, scope, reset_at, utilization, source}`）并 emit 该事件，`data.raw` 保留原生负载供观测；Codex 错误文本无结构化恢复字段，能识别 `resets at HH:MM` / `try again at HH:MM` 时解析为当日或次日时间，否则按 5 小时保守窗口回退。`LimitDetector` 单路径消费该事件触发 key 轮换，不再按事件类型 / error 是 str 还是 dict 分流。撞限流时 driver **同时** emit `ERROR`（可观测日志，前端 VirtualLog 照常渲染）+ `RATE_LIMIT`（控制信号），把「日志看到什么」与「触发轮换」解耦。`delete_session()` 会同时清这三张表（events / evaluations / loop_results + sessions + handoff_relations）。

## 数据流（Start Session）

```
UI (click "Start Session")
  │
  ▼
SessionAPI.start(project_dir)
  │
  ├─ generate_hook_config(project_dir) → <project_dir>/.claude/settings.json
  ├─ write workspace.json            (records project_dir; no clear_all — events live in the daemon-level library)
  ├─ orchestrator.start_session_with_config()
  │   ├─ pool.create_and_run()
  │   │   └─ _run_session()
  │   │       └─ LoopEngine.run_loop(initial_prompt)
  │   │           ├─ iter 1..N:
  │   │           │   └─ session.query(prompt)
  │   │           │       └─ driver.query(prompt)  (SDK or CLI subprocess)
  │   │           │           └─ yields Event objects → event_store.store()
  │   │           └─ evaluator → COMPLETED / MAX_ITERATIONS / BLOCKED / ERROR / MANUAL_STOP
  │   └─ event_store.store(LOG_AUTO: "session_started")
  │
  ▼
Events streamed to UI:
  Web:      SSE event_stream() → EventAPI.get_since()
  Headless: poll loop → EventAPI.get_since() → stdout JSON
```

> **SSE 稳定性硬化**（能耗报告 §八）：`sse_routes.event_stream` 的 `while` 循环内每个 yield 段（增量事件 / 会话 / 流水线 / 用量 / 配置 / auth / handoff / loop）独立 `try/except`，单段异常只 `continue` 跳下一轮、不杀整个连接；`event_store` 所有查询连接 `sqlite3.connect` 处统一 `PRAGMA busy_timeout=5000`，避免多生成器并发读时 WAL checkpoint 立即抛 `database is locked`。前端 `useSSE` 改模块级单例（全局只一条 EventSource，由 `App.vue` 根组件托管生命周期）+ 指数退避重连，根治「断开/重连 toast 死循环」与连接泄漏。服务端 shutdown（uvicorn reload 的 SIGTERM）用 sse-starlette 协作式 shutdown：`EventSourceResponse` 带 `shutdown_event` + `shutdown_grace_period=2`（须小于 uvicorn `timeout_graceful_shutdown=3`），生成器在 `while` 循环头检查 event、break 后发出 `server_reload` farewell 再退出——不能在 `CancelledError` 处理器里补发（任务组已取消，`send` 立即再次被取消，事件到不了浏览器）。

## Driver 层

所有 AI 调用都经过统一的 **Driver 抽象层**（`driver/protocol.py` 的 `Driver` 是 **ABC**，非 typing.Protocol）：

```
Driver (ABC)
  ├── __init__(*, permission_mode="bypassPermissions")   # common fallback state
  ├── query(prompt, *, iteration, session_id, model, cwd, max_turns,
  │         allowed_tools, disallowed_tools, permission_mode="", env, hooks)
  │         → AsyncIterator[Event]
  ├── query_for_evaluation(prompt, *, model, env) → AsyncIterator[Event]
  ├── stop() → None
  ├── update_session_title(native_session_id, name) → bool   # concrete noop
  │         default; opencode overrides (sqlite UPDATE), SDK/CLI inherit noop
  └── is_running: bool        # abstract property
```

反馈评估通过 `Driver.query_for_evaluation()` 传入提示词、模型和 provider 环境。Driver 负责模型回退、轮数及工具权限。基类默认实现沿用单轮和工具禁用列表且模型回退保持运行时中立（各驱动自带 `evaluation_default_model`：Claude 驱动沿用 `claude-sonnet-4-6`，opencode 走其配置默认）；Codex 使用 provider 模型和只读沙箱。

四个实现：

| Driver | 方式 | 适用 |
|--------|------|------|
| `ClaudeSDKDriver` | 经 `claude_agent_sdk` Python 库进程内调用 | 默认 runtime（`claude_sdk`）；经 `env_for_driver` 白名单也能走第三方 |
| `ClaudeCLIDriver` | 子进程 `claude` 二进制 + JSON-stream I/O | GLM / 第三方端点（合并式 env） |
| `OpenCodeDriver` | 子进程 `opencode` 二进制（`--format json`）+ JSON 事件流 | GLM / 第三方端点（provider-agnostic，GLM 经自定义 provider 成一等公民；自带 agent / MCP 抽象） |
| `CodexCLIDriver` | 子进程 `codex exec --json` + JSONL 事件流 | OpenAI Codex CLI；prompt 走 argv，`resume <ID>` / `fork` 由 driver 压入 `Driver.query()` 参数 |

> **root 用户的 Claude 权限模式**：Claude Code 会拒绝 root/sudo 进程使用 `bypassPermissions`（底层等价于 `--dangerously-skip-permissions`），子进程会立即退出。所有 driver 对 per-call `permission_mode` 采用同一短路优先级：非空 per-call 值优先，否则回退构造参数（`per_call or constructor_default`）。默认 runtime 的 `ClaudeSDKDriver` 与 `ClaudeCLIDriver` 随后经 `effective_permission_mode()` 检测到有效用户为 root 且请求该模式时，自动降级为 `acceptEdits` 并**每个进程只告警一次**，避免子进程立即退出；文件编辑仍自动批准，shell 等工具继续由 Claude Code 的权限规则控制。headless（`-p`）模式下无交互式批准路径，未被 `--allowed-tools` / settings allow 规则覆盖的工具（Bash、WebFetch 等）会被直接拒绝而非执行——仍严格优于之前子进程立即退出的硬失败。非 root 用户和显式指定的其他权限模式保持原样。若部署在**真正隔离的沙箱容器**中、确需在 root 下保留完整自主权，可设置 `WORKFLOW_CONTROLLER_CLAUDE_KEEP_BYPASS_PERMISSIONS=1` 跳过降级；默认不启用 Claude Code 的 `IS_SANDBOX=1` 逃生通道，以免在非沙箱环境下让 root 进程重新获得无限制的 shell 权限。

> **CLI workspace 信任预置**：Claude Code headless（`claude -p`）在从未被交互式信任过的 workspace 里会忽略 `.claude/settings.json` 的 `permissions.allow` 条目与项目级 MCP 服务（"this workspace has not been trusted"），导致 allowedTools、hooks（reviewer 的 stop hook 通道）与 MCP 全部失效。worker 目录由用户显式选定，`ClaudeCLIDriver.query()` 在 spawn 前把解析后的 cwd 以 `projects[<cwd>].hasTrustDialogAccepted: true` 预写入 `~/.claude.json`（等价于代用户接受一次信任弹窗），settings.json / hooks / MCP 立即生效；仅显式传入 cwd 时预写，cwd 为 None 或空串（auth 探活、feedback 评估等未选定 workspace 的调用）时跳过——若回退守护进程自身 cwd，Claude Code 的信任判定沿父目录继承，等效把该目录整棵子树静默写入用户全局信任配置。provider worker 探测虽显式传入 cwd，但那是内部 scratch 临时目录而非用户选定 workspace，探测路径以 `pre_trust=False` 关闭同一预写，避免临时目录条目在用户全局配置中逐次累积残留。写入幂等且 best-effort：已有信任标记直接跳过；文件缺失则创建最小结构；JSON 损坏或读写失败只告警、不阻断会话启动；原子替换（同目录临时文件 + `os.replace`，保留原文件 mode）避免半写；并发会话写同一文件的最坏情况是某个标记丢失，由该会话下一轮 query 自愈重写。HOME 取子进程生效环境（env 可覆盖），`CLAUDE_CONFIG_DIR` 存在时配置文件改落该目录；相对的 `CLAUDE_CONFIG_DIR` 按解析后的 workspace cwd 拼接（子进程以该 cwd 运行、按同基准解析相对路径），预写才落在子进程实际读取的位置。该修复仅覆盖 claude_cli runtime，其他 runtime 无此文件语义。

运行时通过 `driver/factory.py` 的 `create_driver(runtime=...)` 选择（支持 `claude_sdk` / `claude_cli` / `opencode` / `codex_cli`）。`WORKFLOW_CONTROLLER_DRIVER` env 可在启动时覆盖。`driver/` 之外的代码不直接 import `claude_agent_sdk` / spawn `claude` / spawn `opencode` / spawn `codex`。

> **`query` 的 `hooks: DriverHooks`（`driver/hooks.py`）是 in-process 通道**：SDK/opencode 驱动经它把 tool-use / stop 拦截路由给 reviewer；CLI 驱动（`claude_cli` / `codex_cli`）忽略它（claude_cli 走 HTTP hook server，codex_cli 当前无同步 hook/approval 回调入口，权限由 `--sandbox` 表达：`bypassPermissions` → `danger-full-access`，`acceptEdits` → `workspace-write`，`default` / `read-only` / `plan` → `read-only`；`codex exec` 强制 `approval_policy: never`，审批门禁动作 fail-closed，不依赖 `--approve-for-me`）。两条通道最终都汇入 `orchestrator._decide_pre_tool_use` / `_decide_stop`。`DriverHooks.get_overlays()`（**单一**回调，返回 `{"subagent_hints": {type: [hint,...]}, "instructions": [...]}`）是 prompt overlay 的拉取通道——仅 opencode driver 消费（bake 进 system prompt / `instructions` config），SDK/CLI 拿到空 dict。旧 `get_subagent_hints` / `get_session_instructions` 退化为 deprecated 转发属性（`__post_init__` 双向 shim：构造时只给了 `get_overlays` 则自动转发两个旧的；只给了旧的则合成 `get_overlays`），保旧调用方/测试不改。reviewer 侧的 4 个控制事件拦截器提取到 `core/reviewer.py:ReviewerInterceptor`（priority=0），orchestrator 经 `ReviewerHost` 协议供其读取 live reviewer / mode / loop 状态（`restart_reviewer` / `set_reviewer_mode` 无需重新注册）。

> **自动回复转人工**：自动模式的会话在轮次结束后遇到尚未回答的问题时，循环引擎临时将该会话的有效回复模式切为手动。收到回复后，下一轮以自动模式处理回复。暂停或中断等待时解除临时切换。会话快照向 Web 提供当前有效模式和等待标记，已保存的模式配置保持原值。

> **Hook Server 启动**：启动时从命令运行目录下的 `workflow-controller-config.yaml` 读取 `hook_server` 的启用与监听地址配置；端口由 CLI `--hook-port` 指定，未指定时使用默认 `18765`，YAML 不再支持端口配置。`hook_server.enabled` 默认为 `true`，监听地址默认为 `localhost`。启用后首次绑定失败即终止 Workflow Controller 启动。`boot()` 在共享 API 边界把 Hook Server 绑定错误转换为 `BootError`，Web 和 headless 各自呈现启动失败；CLI `main()` 不感知 Hook Server 的底层异常。Web 和 headless 均以退出码 3 退出并报告监听地址和系统错误，headless 在标准输出写入 JSON 错误事件。显式设为 `false` 时可跳过监听，启动时也会跳过插件装载。Claude CLI 的 Stop 审核和工具提问经 HTTP 回调进入控制器。Claude SDK 和 OpenCode 的 reviewer 回调走进程内通道。

> **overlay 注册表外溢剥离**（阶段 A）：opencode-only 的 overlay 状态（per-session + per-project 的 subagent hints + 全局 instructions）原活在 `orchestrator` 的 4 个 dict + 14 个方法里（SDK/CLI 永远空跑）。现抽到 `core/overlay_registry.py` 的 `OverlayRegistry`：orchestrator 持 `self._overlays`，公开方法降为一行转发、新增 `overlays` 属性供直接访问；`start_session_with_config` / `start_session_with_resume` 经 `transfer_project_overlays` / `transfer_session_overlays` 注入。opencode driver 的 session 标题更新（原 `orchestrator._update_opencode_title` 直接 `sqlite3.connect` opencode DB）下沉为 driver-agnostic 的 `Driver.update_session_title(sdk_id, name)`——orchestrator 只轮询 `session.native_session_id` 后委托 driver（SDK/CLI noop），不再触碰 opencode 的 sqlite3。opencode DB 路径解析（`_resolve_opencode_db_path`）随之归入 `driver/opencode.py`，`session_api` 的只读/导入 helper 改从 driver 包 import。

> **subagent 跟踪剥离**（阶段 B）：在途子代理追踪（`_subagent_status` dict）+ `_on_subagent_start/progress/stop` 三个 `DriverHooks` callback + `SUBAGENT_*` 事件发射 + WAITING_RETRY 精确唤醒（在途归零即 `wake_retry()` 提前唤醒，不等满 600s）原活在 `orchestrator`。现抽到 `core/subagent_tracker.py` 的 `SubagentTracker`（OverlayRegistry 风格：构造注入 `bus`/`pool`/`loop_getter`；`on_start/on_progress/on_stop` **即** `DriverHooks` 的 `on_subagent_*` callback——发射事件 + 维护在途状态，非 bus observer；`make_driver_hooks` 直接绑 tracker 方法）。reviewer 经 `in_flight(session_id)` 读在途状态：Stop 时在途子代理非空则直接放行本次暂停（`action="stop"`，不调 reviewer LLM），原查询继续等子代理结果；在途清空后下一次 Stop 才恢复 LLM 判定。CLI↔Workflow Controller session 解析器（`_cli_to_workflow_controller_session` / `_register_session` / `resolve_cli_session_id`）**留 orchestrator**——被 stop-hook、AUQ pre-tool-use 等非 subagent 路径共用，且 `_register_session` 缠 `native_session_id` 持久化 + 是 `ITERATION_START` 的 bus observer，非 subagent 专属。

> **query 开始清理**：`SubagentTracker` 的在途记录在下一轮 `session.query()` 开始前清理。`SingleLoopEngine` 在 `_run_iteration` 调用 `session.query()` 前调用 `on_query_start(session.id)`，orchestrator 将其接到 `SubagentTracker.clear_session`。清理点选在 query **开始**而非**结束**：query 结束后到 iteration 边界门禁之间仍可能有子代理在途（reviewer 的 `wait_and_retry` 会在子代理在途时结束 query），breather 的 `on_iteration_boundary` / `_on_context_event` 需靠 `in_flight` 判断「子代理仍在跑」以推迟 handoff——在 query 结束清空会让这些延迟分支读不到在途记录而提前 handoff。既保住边界门禁的延迟语义，又确保即使上游未发送 `SubagentStop`，下一轮 query 也读不到上轮残留。
>
> **嵌套派发关系**：`subagent_start.data.parent_agent_id` 统一表示直接派发者，空串表示 Main Agent。OpenCode 从子会话 `parent_id` 取得；Claude SDK/CLI 仅在 `task_started.tool_use_id` 匹配已记录的 Agent / Task 调用时生成子代理启动事件，并用 Assistant 消息的 `parent_tool_use_id` 与 `task_started` 的 `tool_use_id → task_id` 关联取得父级；`task_started` 只以 `task_id` 作为子代理 ID，缺失时忽略；普通工具及无法匹配调用的任务事件不生成子代理启动事件。后到的 `task_started` 补充先前 hook 未携带的父级（SubagentStart hook 输入只有 `agent_id`/`agent_type`/`session_id`，其 `agent_id` 与 task 事件的 `task_id` 同值可去重，`session_id` 是派发会话 ID、与 agent ID 不同空间，不参与父级解析），task 生命周期消息不再误作 query 边界；Codex 用 `collab_tool_call` 的 `sender_thread_id` / `receiver_thread_ids` 取得，hook 则沿 `session_id` / `agent_id` 的原生线程映射补充，嵌套线程复用所属 Workflow Controller session 路由。`SubagentTracker` 保留首个非空父级并登记原生 agent ID，重复 start 只补充类型或父级，不用空值覆盖。无法确认父级的历史事件仍回退 Main Agent，不按名称或时间猜测。

```
Core (Orchestrator)
        │
   Driver Layer
        │
   ┌────┬────┬────┐
   ▼    ▼    ▼    ▼
ClaudeSDKDriver  ClaudeCLIDriver  OpenCodeDriver  CodexCLIDriver
(in-process SDK) (subprocess CLI) (subprocess CLI) (subprocess CLI)
```

`cli_wrapper` 是 CLI runtime 的可选透明命令前缀，当前只支持 `cpx`；空值表示直接执行原生命令。配置从项目配置进入会话快照，可被启动请求、workflow run 选项和重放覆盖覆盖；handoff 接续会话继承原会话取值。Claude CLI 生成 `cpx claude ...`，OpenCode 生成 `cpx opencode ...`，Codex CLI 生成 `cpx codex ...`。SDK 不接受该配置；Provider 匹配与 driver 事件语义仍按原生 runtime 判定。

启用 cpx 的 Claude 会话在 Insight 导入和 Context Bay 上传时读取 `CANNBOT_PROXY_DIR` 或 `~/.cannbot-insight/proxy/`、`~/.context-insight/proxy/` 下的 `cpx-<session_id>.jsonl`；压缩后的 `.jsonl.gz` / `.jsonl.7z` 仅支持 Insight 导入，上传会明确拒绝，且不回退到原生 Claude JSONL。Codex Insight 导入优先读取 workspace `.codex/sessions` 下的原生 rollout（`codex-jsonl`），不读取 cpx 捕获；Context Bay 上传暂不支持 Codex。

CLI 子进程启动失败时按实际执行的第一个命令报错；配置了 `cpx` 且其命令缺失时，错误明确指出 `cpx`，并提示安装后加入 `PATH` 或关闭 cpx wrapper，避免误报为 Claude/OpenCode CLI 缺失。

> **GLM / 第三方端点**有两条路：`claude_cli`（合并式 env，继承 `os.environ`，对 `ANTHROPIC_BASE_URL` 重定向与 `ANTHROPIC_AUTH_TOKEN` Bearer 原生支持）或 `opencode`（provider-agnostic，GLM 经自定义 provider 成一等公民，不用 ANTHROPIC env 把戏）。SDK 是替换式 env（依赖 Workflow Controller 白名单透传）。`codex_cli` 复用 `OPENAI_API_KEY` / access token 等 Codex 自有认证入口。详见 README「Driver 与 GLM 支持」。

> **CLI stdout 读取**：`claude_cli` 对子进程 stdout/stderr 用**分块读取自行切行**（`read(64K)` + `\n` 切分 + 余量缓冲），不依赖 asyncio `StreamReader.readline`——后者 64KB 的行上限会被长 JSON 行（大工具结果）击穿，抛 `LimitOverrunError` 导致 query 崩溃。切行后的每一行经 `_line_events` 解码并转换为事件；EOF 时残余无换行尾串同样处理。`codex_cli` / `opencode` 经 `cli_base` 共享读取器走同一分块策略，但**按 chunk 批量产出**（一批 = 一个已读 chunk 切出的全部行；EOF 尾行单独一批）：消费方在**批间**而非行间检查 stop——已读入 chunk 的行必须完整转换并派发，EOF 尾行无条件处理（`opencode` 尾行可能是 `task` tool_use，须照常触发 `on_subagent_stop` 与其检查点快照）。stop/kill 竞态下保持块级消费语义，已缓冲输出不得因 stop 置位而丢弃。

> **实时上下文与累计用量**：Claude CLI 每收到主会话 assistant 消息中的有效 `message.usage`，立即发布独立 `context_usage` 事件，携带单请求快照与 model；子代理消息（带 `parent_tool_use_id`）不覆盖主会话快照，无效读数不覆盖上一个有效快照。事件经正常事件存储与 SSE 到达页面，不携带费用与轮次，不参与累计用量统计。最终 `step_finish.usage` 保留 result 的一次 query 内多次 API 调用累计量，`step_finish.context_usage` 保留最后一个有效主会话快照——累计量不能当作当前上下文，实时快照也不能计入累计计费。旧事件缺少快照时展示侧才回退到累计量。UsageAPI 现有的短期快照缓存仅服务聚合查询，不参与此事件口径转换。

> **交接检查与未知用量**：breather 在 subagent 全部返回、worker Stop hook（reviewer 之前）与 iteration 边界读取最新读数——`context_usage` 事件优先，`step_finish` 与历史 `result` 兼容读 `usage`；输入占用为 input、cache creation、cache read 三项之和，窗口依次取配置 `context_window`、事件 `modelUsage` 最大有效 `contextWindow`、200000。缺失或非法读数不是零（明确上报的全零输入仍是有效读数）：无有效读数时检查结果为未知（`percentage=null`），自动检查跳过本次判断、手动交接返回 `reason=context_usage_unknown`，不得声称低于阈值；强制交接不依赖用量。有快照即免 fork；无快照才回退探测——按原会话 runtime / cli_wrapper / cwd 起 `--resume` fork 查询，model 复用 `Session.query` 的 `resolve_query_model` 同一口径（opencode 裸名改写为 env 已解析路由、非 codex 兜底 `ANTHROPIC_MODEL`），fork 才会命中原会话 provider，限时 15 秒并保证 driver 清理，探测失败或无有效结果同样返回未知。Stop hook 只读已记录快照，不启动探测子进程。

> **opencode 的「问/拦截」窗口**：opencode 无 AskUserQuestion、且事件是 worker 跑完后才到（post-completion，无法中途 deny 工具）。Workflow Controller 经 driver 每次 query 拉起的**进程内 per-query MCP gate**（`driver/opencode_gate.py`，暴露 `workflow_controller_ask` 工具）当触发点——worker 调它即拦截，决策作 tool 返回值回流同 run；经 `DriverHooks.on_gate_question` 汇入与 SDK/CLI 同一套 `_resolve_gate_question`（auto→reviewer / manual→人工队列）。其余控制面（看 / 完成 / 暂停-继续）三 runtime 一致。

> **hook 事件的 native id 路由**：HookServer 收到的是 CLI/SDK 侧的原生 session id，
> 解析顺序为内存映射 dict → `run_sessions.native_session_id` 列（权威源）→ 池内
> 直查未命中时，按 `native_session_id` 线性查找。native id 在 ITERATION_START 捕获
> `native_session_id` 的同一写点记账进 run_sessions 列，写序为列先落、映射随后
> （列是重启后的唯一权威源，换绑两段间崩溃不把路由回退到旧持有）；attach
> 带 native id 的换绑/重挂写点在落账后同款同步映射——全部列写点的映射不
> 旧于列。写入语义
> 是「最新绑定唯一持有人」——先清同 id 的旧持有行再落新行，两个 session 先后
> 复用同一 native id 时以最新绑定为准，线性扫第一个命中的路由串台不再发生。

## Plugin 系统

插件在不修改核心代码的前提下扩展 Workflow Controller。每个插件位于 `plugins/<name>/`，含 `plugin.py`（继承 `Plugin`）与 `config.yaml`。

### 插件发现（两条通道）

| 通道 | 形态 | 适用 |
|---|---|---|
| 目录扫描 | `plugins/<name>/plugin.py` + `config.yaml` | 宿主内置插件、本地开发 |
| entry_points | 发行包登记 `workflow_controller.plugins` 组，value = `模块:类`（继承 `Plugin`） | 外部插件，pip 装进环境即被发现，免拷贝目录 |

entry_points 插件没有本地 `config.yaml`：`PluginConfig` 由注册表构造——name 取入口点名、version 取同名发行包版本（找不到记 0.0.0）、`enabled=True`（装进环境即表态启用，不想要就卸载包）、`config_path=None`。同名规则：目录插件压过 entry_points（本地开发覆盖安装版，注册表记日志）；两个 entry_points 同名取先扫到的。解析失败（目标不是 `Plugin` 子类 / import 不动）的入口点跳过并记日志，不拦其他插件装载。

### Plugin SDK（`plugin/sdk.py`）

```python
from workflow_controller.plugin.sdk import Plugin, PluginTabDef, StopHookResult, IterationGateResult, plugin_router
```

### PluginContext

`PluginContext`（`plugin/context.py`）是 plugin 唯一的宿主接入点，由 `PluginRegistry.load` / `reload_plugin` 构造并注入。字段：`bus` / `type_registry` / `hook_server` / `work_dir` / `services`。`services` 是宿主向 plugin 暴露的能力端口对象，plugin 经它调用需要穿透到宿主内部（`event_router` / `pool` 等）的操作——plugin 不持有宿主对象，绕过端口直访宿主成员会触发属性边界门禁（direct-orchestrator-access / no-host-private-access），新代码必须经 `services` 端口。端口族覆盖：session 查询/枚举/启停/等待/落盘、事件读取与批量写入、事件库寻址、在途 subagent 查询、驱动创建、插件实例解析、交接链谱系读写（后继/前驱/交接关系/loop 状态记录）、CLI session ID 反解、hook 配置写入、全局 runtime 与起始 prompt、最近项目清单。`PluginRegistry` 在构造 ctx 时从 orchestrator 装配 services；SDK 基类的拦截翻译助手（stop / iteration 边界）也经 `services.find_session` 取 session，不触碰 orchestrator 对象。守护进程事件库所在目录同样经端口暴露（`services.events_project_dir()`，指向 `event_router.project_dir`）——plugin 侧的外部读库方（如 breather 注入 handoff prompt 的 narrative 读库路径）经此寻址。

### Plugin 能力

| 能力 | 方法 | 服务 |
|---|---|---|
| aiohttp 路由（CC hooks） | `register_hook_routes(hook_server, routes)` | :默认 18765 |
| FastAPI 路由（浏览器） | `get_web_router()` | :18600 |
| Stop-hook 拦截 | `on_stop_hook(session_id, session) → StopHookResult` | — |
| Iteration-boundary gate | async `on_iteration_boundary(session_id, session, iter) → IterationGateResult` | loop 边界 |
| Subagent-start 拦截 | `bus.intercept(SUBAGENT_START, handler)`（阻塞冻文件态） | — |
| Session-delete 拦截 | `bus.intercept("session_delete", handler)`（inline，sync 清理） | — |
| Session-complete 观察者 | `bus.observe("session_complete", handler)`（fire-and-forget） | — |
| 观察者 hook（TOOL_RESULT/NOTIFICATION/ITERATION_START/ITERATION_END 等） | `bus.observe(event_type, handler)` 自注册（无基类 `on_*` 观察方法） | — |
| Web tab + actions | `get_web_manifest()` | Web |
| run 驱动面板贡献 | `get_run_panels()`（format → 前端组件 id + 投影 schema_version 上限，经 /api/plugins 下发） | Web |

### hook 自注册（拦截器 + 观察者，不由 orchestrator 集中代理）

> **reviewer 拦截器不是 plugin**：reviewer 的 4 个控制事件拦截器（`stop_requested` / `pre_tool_use` / `iteration_boundary` / `gate_question`）由 orchestrator 在 `start()` 构造 `ReviewerInterceptor`（`core/reviewer.py`）并注册（priority=0）；orchestrator 另注册一个 priority=5 的 `_on_stop_pre` 拦截器处理 staged deferral 短路（交接类收尾由 plugin 自带的 let_exit 回调承担）。三层 priority band：plugin ≥ 10 / orchestrator pre-stop = 5 / reviewer = 0。

EventBus 提供三种互不混用的投递语义：`intercept` 是按优先级串行执行、允许返回控制结果并短路的决策链；`observe_awaited` / `observe_all_awaited` 是由发布者等待全部 handler 完成的 inline 广播；`observe` / `observe_all` 是入队后由后台 processor 消费的异步广播。两类 observer 都先按注册顺序执行事件类型对应的 handler，再执行通配 handler；它们都没有控制权，单个 handler 的普通异常只记录日志并继续广播，调用任务的取消不被吞掉。`emit` 的固定顺序为持久化事件、执行 interceptor、执行 awaited observer、将事件加入后台 observer 队列，最后向发布者返回拦截结果。

插件**自行**把覆盖的 `on_stop_hook` / `on_pre_tool_use` / `on_iteration_boundary` 注册成 `EventBus` 控制事件**拦截器**，把观察者 hook（TOOL_RESULT / NOTIFICATION / ITERATION_START / ITERATION_END / session_complete 等）注册成 `EventBus` 类型化**观察者**——**不再由 orchestrator 集中遍历 `_plugin_registry` 代注册/派发**。落点在 `Plugin` 基类的**唯一**生命周期入口 `install(ctx)`：它是 concrete 方法，默认 body 捕获 `self._ctx = ctx` 并派发两个 opt-in 扩展点 `_register_interceptors(ctx)` / `_register_observers(ctx)`（默认空）。只做 bus 接线、不需 setup 的插件只 override 相关 helper、**不** override `install`；需要 setup（hook 路由 / 状态 / 类型化 `bus.observe` 订阅）的插件 override `install`，调 `super().install(ctx)` 保留 bus 接线，再做自己的 setup。基类不再提供旧式 `install(orchestrator, hook_server)` 入口——`install` / `install_ctx` 双入口已合并成一个 `install(ctx)`：

```python
# plugin/base.py — install 是唯一生命周期入口（concrete，默认派发 helpers）
def install(self, ctx: PluginContext) -> None:
    self._ctx = ctx                          # 翻译 helper 运行时要拿 orchestrator.pool
    self._register_interceptors(ctx)         # 拦截器：子类按需 override 显式声明
    self._register_observers(ctx)            # 观察者：子类按需 override 显式声明

def _register_interceptors(self, ctx: PluginContext) -> None:
    """默认空：不拦截。需要拦截的 plugin override 此方法、显式声明。

    不再用反射扫「子类覆盖了哪个 on_*」自动注册——那既违反 OCP（基类枚举
    hook 名）又隐式难调试。改为显式：谁要拦谁写。
    """
    return

def _register_observers(self, ctx: PluginContext) -> None:
    """默认空：不观察。需要观察的 plugin override 此方法、显式声明。

    与 _register_interceptors 对称：插件写自己的 async handler 并
    ctx.bus.observe(event_type, handler) 做类型化订阅，避免「收所有事件
    再手动过滤」。基类不提供 on_* 观察方法也不提供翻译 helper——观察者
    一律是插件自有的 async handler（拦截器侧才有兼容翻译 helper，因其
    结果类型 StopHookResult→InterceptResult 非平凡）。
    """
    return
```

插件示例：

```python
# breather（有 setup：路由；拦 stop_requested / iteration_boundary；类型化 observe 订阅）
def install(self, ctx):
    super().install(ctx)                       # _ctx + _register_interceptors + _register_observers
    self._hook_server = ctx.hook_server
    self.register_hook_routes(ctx.hook_server, [("POST", "/breather", self._handle_breather)])

def _register_interceptors(self, ctx):
    ctx.bus.intercept("stop_requested", self._on_stop_intercept, priority=10)
    ctx.bus.intercept("iteration_boundary", self._on_iteration_boundary_intercept, priority=10)

def _register_observers(self, ctx):
    ctx.bus.observe(EventType.SUBAGENT_STOP, self._on_context_event)        # subagent 结束 → handoff check
    if self._cfg("notify_skill", ""):
        ctx.bus.observe(EventType.TOOL_RESULT, self._on_notify_skill_hook)

# staged_pipeline
def _register_interceptors(self, ctx):
    ctx.bus.intercept("iteration_boundary", self._on_iteration_boundary_intercept, priority=10)

# agent_traceback
def install(self, ctx):
    super().install(ctx)                       # _ctx + helpers（宿主能力端口经 ctx.services 取）
    self._reap_orphan_fork_roots()             # 启动期一次性扫 tempdir 清孤儿（跳过活动 fork_root）

def uninstall(self):
    self._detach_bus_handlers()                # 从 bus 注销 interceptor/observer（reload 不泄漏）

def _register_interceptors(self, ctx):
    ctx.bus.intercept(EventType.SUBAGENT_START, self._on_subagent_start, priority=10)  # subagent 启动前冻文件态
    ctx.bus.intercept("session_delete", self._on_session_delete, priority=10)   # inline 清理
def _register_observers(self, ctx):
    ctx.bus.observe("session_complete", self._on_session_complete)  # fire-and-forget
```

> **Claude CLI 检查点重放**：`agent_traceback` 先根据源会话配置、原生 session ID 和 JSONL 文件判定源 runtime；通过 `PluginServices` 的 runtime 解析端口校验只使用相同 runtime 续跑。Claude CLI 源会话从原项目的 JSONL 找到检查点前匹配的 Agent 调用，截断后改写 session ID 与路径，写入 fork 独立的 `CLAUDE_CONFIG_DIR`；最终状态检查点保留完整对话。OpenCode 仍走隔离 `opencode.db`。重放弹窗查询源 runtime 作默认值，后端再次校验，防止用另一 runtime 读取错误格式的对话。Claude CLI 非正常退出应带退出码与 stderr 原因上报；用户主动停止时不报 CLI 失败。

> **session 删除清理链路**：`SessionAPI.delete` 在 `pool.remove` 前捕获 `birth_traceback` 透传进 `session_delete` 事件 data，插件 `_on_session_delete` 据此即时清理——
> - **重放 fork session**（`birth_traceback.fork_root`）：先无条件 pop in-memory `_fork_roots`，再按事件 data → in-memory 值 → `_read_birth_traceback` 全库扫描选择目录；rmtree 隔离 tempdir（~2GB）前做路径安全校验（须在系统 tempdir 且 `workflow-controller-tb-*` 前缀），实际删除 offload 到工作线程，避免阻塞事件循环。重放在 Session 注册前失败时立即清除尚未移交生命周期管理的 fork_root。
> - **重建 session**（`birth_traceback.reconstructed_from`，无 `fork_root`）：按 `clean_reconstruct`（`"workflow_controller"` 清产物子集 / `"all"` rmtree 整个 target_cwd）清磁盘 + 经 `ConfigAPI.remove_recent_dir` 同步 `recent_dirs.json`（消除侧边栏幽灵）。默认模式只有 `created_git_dir`/`created_opencode_dir` 标记为真时才删除对应目录，保护用户预先存在的配置；aggressive 模式拒绝根目录/过浅路径。
>
> `delete` 返回 `{ok, deleted_session_ids}`。`SessionAPI._delete_one` 顺序为 `pool.remove → reclaim_deleted_session_affiliation → bus.emit(session_delete) → event_router.delete_session`——emit 在前使事件落正确项目库，delete_session 随即清掉它，避免孤儿事件行；归属回收与名册删除同点（隐式归属连行带 run 幂等回收，显式 run 的成员行是调用方的记录、不连坐删——与 boot 正向孤儿回收同一判定），删除即清账、不等 boot 兜底（名册全删空时 boot 兜底不触发）。`EventStoreRouter.delete_session` 同时删 default DB（metadata owner）+ 项目 DB（事件），防止重启后 `load_sessions` 从残留 metadata 行复活已删 session。reaper 仅启动期跑一次，活动集 = pool 中且 `last_activity` 在 TTL 窗口内的 session（liveness = `now - last_activity < TTL`），崩溃后 IDLE ghost 超 TTL 即可回收。`uninstall` 经 `EventBus.detach_interceptor/detach_observer` 注销 handler，防 reload 后旧实例与新实例同时触发。

**翻译归插件、副作用归框架**——这是本重构的核心边界，避免 orchestrator 反向 import plugin SDK 的结果类型：

- **翻译层（`Plugin._on_*_intercept`，plugin 层 owns）**：把 `StopHookResult` / `PreToolUseResult` / `IterationGateResult` 翻成 `InterceptResult` + 调度插件自己的 `on_let_exit` 回调 + 日志。**纯翻译，不碰框架内部状态**。
- **副作用层（orchestrator 的控制事件消费点 owns）**：`_decide_stop` / `_decide_pre_tool_use` / `_run_iteration_boundary_gates` 拿到 `InterceptResult` 后做框架级反应——经 `HookDecisionEmitter`（`core/reviewer.py`）发 `LOG_AUTO` 决策事件、`SingleLoopEngine.mark_reviewer_let_exit`。reviewer 的 LLM 判定编排（`_run_llm_stop_review` / `_run_nl_question_review`）移入 `ReviewerInterceptor`，副作用（`mark_reviewer_let_exit` / `schedule_retry` / `inject_prompt`）仍经 `ReviewerHost` 回调 orchestrator。插件不再伸手进 `single_loop`。

通用 Stop Hook 的 reviewer 根据配置的任务目标、worker 末条回复和磁盘事实判定完成。放行需要末条回复提供覆盖整个任务范围的明确完成依据，并满足已知的验证要求。磁盘事实只证明所列文件的落盘和占位状态。若回复只汇报一组路径检查、报告更新或提交结果，且无法确认这些工作覆盖完整目标，返回 `block_with_feedback`，要求核对其余必需项。若已知任务的完整范围就是这些检查，且回复逐项给出结果，可以放行。

`InterceptResult` 的 action 语义（控制事件链的通用契约，plugin 与 reviewer 共用）：

| action | 含义 | 框架反应 |
|---|---|---|
| `block` | 拦下、注入 feedback 重做 | 发决策事件 + 把 feedback 回灌 worker |
| `stop` | 放行本轮结束、loop 自行判断是否结束 | 仅发决策事件（若 `source` 非空） |
| `let_exit` | 放行结束 **且** 强制收尾（bypass evaluator，`stop_reason=reviewer_complete`） | 发决策事件 + `mark_reviewer_let_exit` |

`InterceptResult.source`（可选）= 决策来源标识：插件拦截器填 `plugin.config.name`，reviewer 留空。`_decide_stop` 见 `source` 非空才发 `stop_hook_plugin` 决策事件（避免与 reviewer 自带的 `pretool_hook_llm` 决策事件重复）。

- **何时注册**：`PluginRegistry.load()` / `reload_plugin()` 都走 `install`，故加载即注册。`reload_plugin` 运行时分支（aiohttp router 已冻结）也直接调 `install(ctx)`——`register_hook_routes` 内部检测 router 冻结即 skip + warn（路由加不进是运行时 reload 的固有约束，bus 拦截器 / 观察者只依赖 `ctx`，照常注册），修掉旧实现「reload 后拦截器丢失」的隐患。
- 路由注册的冻结感知在 `Plugin.register_hook_routes`：`hook_server.app.router.frozen` 为真时跳过 `register_route` 并 warn，让 `install` 在任意生命周期阶段（启动期 / 运行时 reload）都可安全调用。

### 内置插件

| Plugin | 位置 | 用途 |
|--------|----------|---------|
| Breather | `plugins/breather/` | 上下文窗口 handoff 管理 |
| staged_pipeline | `plugins/staged_pipeline/` | 门禁执行器：读项目 workflow-controller.yaml 的 `stages` 强制顺序 + 完成门禁（默认关；session 无 stages 则惰性） |
| agent_traceback | `plugins/agent_traceback/` | 检查点快照 / 重放 + 从 cannbot db 重建会话（默认关） |
| graph | `plugins/graph/` | DAG 工作流：读 yaml DAG 调度 bash/agent 节点并行执行 + 暂停 / 续跑 / 崩溃恢复（默认关） |
| file_provenance | `plugins/file_provenance/` | 文件变化证据、逐行归因与 Git authorship（默认关） |
| fake_driver | `plugins/fake_driver/` | 内置验收插件：注册 fake@1 run 驱动工厂，headless 无外部运行时验收用（默认开） |



> **agent_traceback 内部 git 状态捕获**：workflow 在 project_dir 内自建的 `.git` 仓（objects/refs/index/logs）也是「文件态」的一部分——它决定 worker 在检查点时刻通过 git 命令能看到什么。检查点快照时 `SnapshotEngine` 把 `project_dir/.git` 整目录**增量**镜像到快照仓保留路径 `working/.workflow-controller-inner-git/`（避开 git 保留名，随工作树同一 commit 原子落库；工作树镜像趟把保留路径排除以防 `--delete` 误删上一检查点镜像；rsync 两趟——固定名小文件 `--checksum` 防「同秒同长漏传」、objects/ 走 quick-check 防白读 GB 级 packfile；镜像后剥除 remote 配置，防含 token 的 URL 在快照仓历史里持久化。受 `snapshot_git_state` / `snapshot_git_state_max_mb` 配置约束，超限标 `skipped_size` 降级；rsync 不可用时降级纯 Python 镜像——size+mtime_ns 判定，不保证捕获同秒同长的 ref 重写，需保真场景应安装 rsync，降级镜像残缺时标 `absent`）；重放 `restore` 用 `git archive` 抽出后改回 `fork_cwd/.git`（unborn/空仓的 objects、refs 空目录抽不出，补齐骨架后 git 才认作有效仓；改回失败时降级 `inner_git_captured=False`，不让可选增强项拉垮整次文件态 restore），`_init_git_sandbox` 主路径以 `git rev-parse --git-dir` 做仓健全性 sanity（`rev-parse HEAD` 会把 unborn HEAD——`git init` 后首个 commit 前——误判为未捕获而降级复制 origin 当刻 `.git`，造成未来历史泄漏），通过后仅移除 remote、**不打 baseline commit**（HEAD/index/status 与检查点时刻一致，unborn 仓忠实保持 unborn）。旧检查点（树里无保留目录）零迁移自动降级为旧行为（复制 origin 当刻 `.git` + baseline）。重建路径（`FileStateRebuilder`）在 task 边界快照时同样镜像 `target_cwd/.git`。

#### Graph Runner（`plugins/graph/`）

把「确定性工作流 yaml」（DAG 节点）作为 run 的驱动跑在 Workflow Controller 里：插件注册 `graph-yaml@1` 驱动工厂（SDK `get_run_driver_factories` 派发面）+ 前端 RunDagPanel 面板声明，一切执行机制（成员 session 出生、bash 进程组、事件、快照持久化、恢复）归 run 平面。**归属**：graph 不进 `SessionPool`；agent 节点 = run 成员 session（role=executor，`plugin_metadata.graph` 携带 node_id 溯源）；bash 节点 = run 键进程；节点/验收/重试/回滚语义全部住在驱动决策里（`plugins/graph/driver/`，纯决策无 I/O，门禁守护）。

- **生命周期**：启动即 `POST /api/v1/runs`（format=graph-yaml@1、label=yaml 文件名 stem、params 携带 variables 与运行配置）→ `RunCoordinator.start_run` 冻结程序进 run 根 → 工厂 `create` 组装驱动 → 决策循环拉起。插件侧内存注册表按 **run_id** 键持有驱动实例（create/restore 共用；跨 run 同程序并发互不顶占），graph_id 降级为驱动内标签（label 或程序位置摘要，仅投影/展示用）。
- **调度**：驱动 `decide(world, notifications)` 按就绪集产出 StartSession/StartProcess/AwaitSessions 动作；并发准入（max_parallel）归 run 平面准入门。bash 节点经 RunHost 进程原语；agent 节点经 SO 成员原语。
- **组控（run 平面）**：启停/暂停/续跑/重开全部经 RunCoordinator 组控引擎；驱动 `on_control` 承接调度反应面（组暂停冻结待启节点、组恢复重置 failed/stopped 可续跑节点）。成员级 pause/resume/inject 经 run 会话端点（A8 单一写者），回执以 `session_control_applied` 通知进决策。
- **崩溃恢复**：run 平面恢复链按驱动快照（run_driver_state）restore；`GraphState` 构造把 running 降级为 stopped（可续跑断点）、子图节点按归档代恢复；终态 run 经 `restart_run` 重开续跑。
- **删除**：`DELETE /api/v1/runs/{id}` 级联（账面/快照/事件/run 根/成员数据单事务）；graphs/graph_node_history 旧表已退役（残表 DROP 收官）。
- **插件卸载**：toggle 卸载不停名下 run（run 归 run 平面所有，驱动实例继续跑）；重启后插件停用则恢复按「格式无驱动工厂」判 failed。
- **前端**：RunView 的 RunDagPanel 面板（消费驱动投影渲染 DAG、节点历史走 run 事件回放、模型切换走 driver op）；Session Detail 对 workflow 成员经 Run 徽章跳 run 面；旧 `/graph/*` 路由与 `/api/graph/*` shim 已退役。
- **用户要求传递**：Runs API 创建 workflow 时以当前项目 `work_dir` 解析相对程序路径；Graph 插件在驱动工厂的 I/O 层把 `starting_prompt`（缺省回退 `OP`）写入 `<work_dir>/.workflow/<run_id>/user_prompt.md`，以 run_id 隔离同目录并发工作流。恢复驱动只复用文件路径，不覆盖用户在两次运行间对文件的修改。`WORK_DIR` / `USER_PROMPT` 在 YAML 变量渲染前注入，并由 GraphDriver 通过 `SessionSpec.env_overrides` 和 `StartProcess.env` 传给执行/验收成员及进程；驱动只决定路径和动作，不直接写文件或访问宿主私有对象。
- **启动失败**：相对程序路径不存在、逃出当前项目与用户目录，或 YAML 无法读取时，Runs API 返回可读错误；RunView 启动表单显示该错误。相对路径的项目目录取本次请求的 `work_dir`，不使用插件安装时的目录快照；已有的绝对程序路径入口保持原样。

### 源提交 `1aa0c65` 的通用修复落点

- SSE 的 UsageAPI 快照允许短期缓存以避免每 250ms 全量扫描历史事件；显式 `/usage` 和 Headless 收尾强制重算，保证按需查询的新鲜度。
- provider 连接探测失败写入验证结果但不阻断密钥列表保存；身份重复仍拒绝保存。该判断属于 AuthRotator，Web 路由不复制验证逻辑。
- Session 每轮向驱动发请求前核对当前 provider 的 model；未 pin 的旧 model 随同索引的 provider 重配更新，显式指定且不同于 provider 的 model 保持 pin。SessionAPI 在创建时固化 pin 语义；Graph 插件经 `PluginServices.provider_model` 在 Run 创建时计算并快照 pin，再通过 `SessionSpec.config_overrides` 传给宿主组装成员，恢复时不按已重配的 provider 重算。

## Watchdog（通用能力）

Watchdog 是 Workflow Controller 的通用能力（**非 plugin**），检测规则以事件库订阅者形态各自订阅 SUBAGENT_* 事件，命中条件时发 `fault.<rule>` 开放词表告警事件（字符串事件类型，session 键落 events 表、按归属打 run 戳）。

> **依赖单向**：Watchdog 只接收三个独立参数（`config.watchdog` / `bus` / `pool`），不 import Orchestrator，故 `orchestrator → watchdog` 顶层 import 无循环依赖。测试也直接传入三对象，无需构造完整 Orchestrator。

```
SUBAGENT_{START,PROGRESS,STOP} 事件 ──> EventBus ──> 三条规则订阅者（各自 observe 所需事件）
                                     │
                                     ├── 规则 1: subagent 超时（PROGRESS/STOP 时检查 elapsed）
                                     ├── 规则 2: 文件不变（每次 STOP 读文件 hash 比对）
                                     └── 规则 3: 关键词检测（每次 STOP 查文件中是否含关键词）
                                              │
                                              ▼
                                     fault.<rule> 告警事件 → events 表 + SSE → 前端告警徽标
```

**与 plugin 的区别**：
- 不继承 Plugin ABC，不被 plugin registry 管理
- 配置在 `AppConfig.watchdog`（全局 workflow-controller-config.yaml），仅全局生效；项目 yaml 不参与检测配置
- 生命周期：`orchestrator.start()` 里接入事件总线，进程常驻

三条规则各有独立的关闭方式（阈值=0 或字段为空），去重逻辑保证不刷屏。
（run 级停滞规则挂起未实现。）

告警事件的类型词表与 payload 编解码住 `watchdog/fault.py`（检测与消费共用）。告警历史查询经 `CoreAPI.watchdog`（`api/watchdog_api.py` 门面，直读事件库筛 `fault.*` 并解码，不依赖检测本体）；告警查询路由住控制台侧（经 `CoreAPI.watchdog`），内核 watchdog 包不承载 web 路由。前端实时告警走 SSE 透传的 `fault.*` 事件（按前缀过滤）。

详见 [watchdog 设计文档](watchdog.md)。

## Hook 系统

```
Claude Code subprocess (cwd=project_dir)
  │  reads .claude/settings.json
  │  hooks fire on: PreToolUse, PostToolUse, Stop, Notification, SessionStart/End, SubagentStart/Stop
  ▼
HTTP POST → HookServer (localhost:<effective_hook_port>)
  │  hook handlers:
  │    PreToolUse → plugin interceptors (10) → reviewer (0) decides allow/deny
  │    Stop      → plugin interceptors (10, e.g. breather handoff) → orchestrator pre-stop (5, staged) → reviewer (0, manual/LLM)
  │    PostToolUse → event bridge → plugin observers
  │    SessionStart/End → event bridge → plugin observers
  ▼
Orchestrator processes decision → returns JSON response to Claude Code
```

> **领域门禁的落点**：框架层提供的是通用的 `AskUserQuestion` pre-tool-use hook 拦截（driver-agnostic，SDK / CLI 双入口汇入 `orchestrator._decide_pre_tool_use`）。框架不「懂」任何领域的门禁语义（如算子的 CP1）—— 何时触发门禁由 plugin / skill / prompt 表达，框架只提供「工具调用拦截 + 人机交互 surface」的通道。

## Auth 系统

Reviewer thinking mode 采用请求值、项目 `workflow-controller.yaml`、全局配置三级优先级；未配置的字段以 `None` 表示并继续向下回退，显式 `False` 不被覆盖。会话创建时将解析值写入 `SessionReviewerConfig`，ReviewerService 传给 DriverReviewerRunner；Codex Worker 的 Reviewer 同样必须使用 Anthropic Messages base URL。Provider 测试完整探测 `disabled`、带预算的 `enabled`、`adaptive`、省略字段四种形状并持久化成功集合；新建会话只允许选择当前 Provider 支持的模式，`enabled` 模式使用 thinking 预算，其余模式不使用预算字段。`max_tokens` 保持既有调用语义，`enabled` 仍按 thinking 预算额外预留 1024 token 给判定文本。全部失败时只保留最终归一化诊断，不透传多次 fallback 的上游原始错误。共享 Anthropic HTTP client 只把非 Anthropic Messages 形状视为请求错误；健康检查允许形状合法但无 text 的 always-thinking 响应，Reviewer runner 与 Provider Reviewer 探测另行要求非空文本。全局和项目配置 API 暴露这两个字段，项目读取保留未配置的 `null` 语义。

### Auth Rotator

`AuthMode` 枚举仅有 `API`（无 OAuth/Plan 模式）；`AuthProviderType` 仅有 `API_KEY`。

```
AuthRotator
├── providers[0..N]: API keys（from settings.conf），解析 "key | model" 行（key_store.py）
│   每个 provider 绑定单一 worker runtime；worker_validation 与
│   reviewer_validation 分别记录真实 driver / Anthropic HTTP 检测结果、
│   配置指纹、检查时间和脱敏错误。探测错误先替换密钥、规整空白；内存及即时验证结果保留全文供诊断。所有 auth_state.json 写入路径统一将 Worker/Reviewer 错误限制在 10K 字符，避免上游大响应使状态文件无界膨胀；重新加载的超限错误在下一次保存时按上限截断。两种能力互不连坐。
│
├── 查询：get_mode / get_active_provider / get_provider_count /
│         get_available_count / get_exhausted_count / get_providers /
│         get_key_status_dict / has_key / has_available_provider
├── env 透传：build_env / env_for_driver（SDK 白名单四 key，见 Driver 段）
├── 健康检查：validate_provider —— worker 通过绑定 runtime 的真实 driver
│           执行隔离、只读、单轮 smoke test；reviewer 通过实际使用的
│           AnthropicHttpClient 直连检测。OpenCode 的 provider/model 只在
│           reviewer 路径剥路由前缀。配置指纹未变时复用持久化结果；旧配置在
│           第一次使用前检测，不在进程启动时批量调用模型。
│           设置页按行重测通过独立接口指定 provider 序号并以 force=True
│           跳过验证缓存；现有 /auth/test 仍检测当前 active provider。重测
│           返回 success 时，EXHAUSTED 的全局配额状态恢复为 ACTIVE/UNUSED，
│           并清理 exhausted_at、reset_at、scope 与该 provider 的恢复任务；
│           check_auth 成功还会将 auth_error 造成的全局 INVALID 恢复为
│           ACTIVE，避免轻量健康检查无法解除认证错误轮换状态。
├── 轮转：rotate_to_first_available / rotate_to_next_key / rotate_key /
│         schedule_pending_resets（按 provider 调度 reset；到期把该
│         provider 从 EXHAUSTED 恢复为 ACTIVE/UNUSED；若 reset_at 已过期而立即
│         恢复，全局 reset_at 同步清空，只在该 active provider 仍为 EXHAUSTED
│         时保留）
├── 写入：set_keys / delete_provider / set_api_key / set_api_config / set_global_settings /
│         sync_providers_from_config
├── 限流回调：on_limit_detected / on_rate_limit / on_quota_warning /
│            set_loop_engine_hook / set_quota_warning_callback
│
├── LimitDetector → 订阅 EventBus（bus.observe_all），**单路径**消费 RATE_LIMIT
│   的归一化 `limit_signal`（kind: allowed/warning/rejected/auth_error/unknown →
│   quota 回调 / on_rate_limit / on_limit_detected / no-op）；driver 负责把
│   上游错误翻译成 signal，detector 不再做 driver 专属解析（文本正则 / dict
│   statusCode 路由已下沉到 claude_sdk / opencode driver）。ERROR 事件为纯可观测
│   日志，detector 不消费——限流处置链为两段回落：per-session 轮换
│   （`_handle_session_limit`）→ 全局 active provider 轮换（旧行为）；
│   run 平面不做 auth 协调（session key 轮转去留未决，'auth' 暂停档休眠
│   预留、无生产写者）
│
└── Persistence: auth_state.json（mode, active_index, providers, reset_at）
                 · settings.conf 读写（settings.py）· 序列化（persistence.py）
```

Provider 的全局 `status` 只表达配额可用性；限流轮换与 per-provider reset task
都会持久化 `reset_at`，到期或行内重测成功即恢复该 provider。driver 的 401/403、路由错误、缺少
可执行文件或超时只使对应 `worker_validation` 失效，Reviewer 只有 HTTP 401/403
才使 `reviewer_validation` 失效，瞬时 5xx 等请求错误仅向上抛出。失败探测不写
进程内缓存；持久化的 INVALID 结果只在短退避窗口内复用，窗口结束后首次使用会
重测；运行期无论通过显式能力失效还是 auth_error 轮换将能力标记为 INVALID，
都同步驱逐相同配置指纹的成功探测缓存，避免重测复用失效前的结果。协议探测
（`probe_provider_kind`）的成功结果另存一份按 key、base URL、model 绑定的短
TTL 进程内缓存：test-key 刚实测出的协议在随后的保存（无显式 kind 的候选）
中直接复用，不再二次外网探测——否则二次探测的瞬时失败（429/超时）会让刚
测试通过的条目无法保存（kind=None 不注入 `OPENCODE_CONFIG_CONTENT` → worker
验证失败），成功路径也固定多一次外网往返。失败结果不缓存（可立即重试），TTL
过期后重新实测；换 key/URL 即换缓存键，不跨端点复用。Worker 与
Reviewer 探测并行执行，配置保存按 key、model、base URL、runtime（Codex 另含
provider-bound catalog 路径）整体身份跨
位置复用验证结果（runtime 与协议探测判定同取 `resolve_runtime` 的 effective
值，`WORKFLOW_CONTROLLER_DRIVER` 覆盖同样生效：覆盖切换即指纹变化，claude 的 VALID 缓存
不会在 opencode 覆盖下复用，反之亦然），拒绝完整身份重复的 Provider；删除或
重排 Provider 不触发无关重测。删除走专用 AuthAPI 路径：先拒绝会话快照仍引用目标索引或更高索引的请求（删除会使后续索引前移，历史会话仍可重启），再核对内存 Provider 与 `settings.conf` 对应条目的身份并移除。仅同步剩余条目的内存状态，不重新探测 Worker/Reviewer；配置或索引不匹配则拒绝，避免误删。Worker 候选
解析按 session runtime 过滤绑定 runtime 及验证结果；存在 Provider 但无 runtime
匹配时会话启动直接失败，只有 Provider 列表为空的 env-only 模式允许继续。429
先走 session-bound provider 轮换；无替代时保留原绑定并进入 EXHAUSTED，同时
调度 `reset_at` 对应的恢复任务（Codex 无恢复时间则用 5 小时保守窗口）。
auth-switch 切换 Provider 后下一轮冷启动 driver，
不 resume 旧 runtime session，避免旧路由状态污染替换 Provider。

> **OAuth 并行通道**：`check_auth()` 始终测 API key；**仅在 `runtime=claude_cli` 时**额外跑一次空 env 的 CLI 探针测 OAuth token（`_last_oauth_ok`）。SDK runtime 不测 OAuth。GLM 等第三方端点走 API key（Bearer `ANTHROPIC_AUTH_TOKEN`），不依赖 OAuth。

### Key 配置（`~/.config/workflow-controller/settings.conf`）

```yaml
# Global defaults (used when a key doesn't specify its own model/base_url)
default_model: "claude-sonnet-4-6"
default_url: ""  # Leave empty for native Anthropic endpoint

api_keys:
  - key: "sk-ant-key1"
    # model and base_url omitted → uses global defaults
  - key: "sk-ant-key2"
    model: "deepseek-chat"
    base_url: "https://api.deepseek.com/anthropic"
```

## 关闭与清理

```
orch.shutdown()
├── run 平面关停步骤（register_shutdown_step 登记的回调，逐个隔离执行——
│     单步失败留痕不阻断后续关停序；先于 session 运行时停止）
│     RunCoordinator.shutdown()：run 任务消费 stopped 自行收尾（10s 排空窗）→
│     超时先 cancel 等任务退场、再代办快照 → 终态序列停成员/置杀 run 键进程；账面保持非终态，
│     下次 boot 恢复链接续
├── pool.stop_all()               # 停所有 worker session
├── plugin_registry.unload_all()  # 卸载插件（cancel refine task，stop flags）
├── reviewer.stop()
├── hook_server.stop_server()
└── bridge.stop()
```

启动时 `pool.load_sessions()`（`orchestrator.py` 的 `start()`）从守护进程库的 sessions 表扫描全部历史 session 加载进池并强制置为 `SessionState.IDLE`（`manager.py` 不恢复运行态，仅恢复元数据 + config_json）。事件与名册恒守护进程库（启动目录 `events.db`），**不跨重启清空**（换项目时也不 clear，仅写 `workspace.json` 记录当前 project_dir）。

> 注：当前**没有** psutil 扫描或杀孤儿 claude 进程的逻辑 —— 崩溃遗留的 claude 子进程需手动清理（如 `pkill -f claude`）。
>
> 存储目录解耦：event_router 的 `_default_project_dir` 绑定到**启动期 cwd**（`core_api.boot` 传 `default_project_dir=Path.cwd()`），运行期不变。`config.project_dir` 仅作「当前 worker cwd 指针」，不再决定全局 session/events 存哪 —— 切项目后 yaml 指针变了，但运行期存储位置不动，重启也从同一固定位置加载，根治「重启后 session 闪现/丢失」。

> **主机本地目录（`cfg_dir`，与 `auth_state.json` 同目录，`platformdirs.user_config_dir("workflow-controller")`）**：存放与「本机」绑定的状态：
> - `auth_state.json` — auth 轮换状态（mode / active_index / providers / reset_at）；
> - `recent_dirs.json` — 侧边栏「最近项目」列表来源（recency 排序、上限 10）。`ConfigAPI.get_recent_dirs()` 合并 `recent_dirs.json` + pool 中非 fork session 的 cwd（去重、recency 优先），随 `config_update` SSE 推送，前端只读；重启不丢（json 落盘），SSH 同 origin 时各主机独立。只有「浏览/添加项目」（DirPicker）或 import-db 登记才写 json；点击目录只切换 `project_dir` 指针，不落 json。
>
> **「最近项目」的 cwd 来源**（`ConfigAPI._collect_session_cwds` 收集 pool 中**非 fork session** 的 `config.cwd`，`.` 哨兵除外）：
> - **跳过 fork、并入普通/重建 session**：fork session 的 cwd 是 `workflow-controller-tb-*` 临时目录（`<fork_root>/cwd`），不进侧边栏（在 Dashboard 按 `logicalProjectDir` 归到 origin 项目下）。普通/重建 session 的 cwd 是真实目录，进侧边栏列表。条目 ✕ 只 `remove_recent_dir`；若该 cwd 下仍有 session，`_collect_session_cwds` 会推回（防误删有会话的项目）。
> - **fork 按逻辑归属归组**（`DashboardView.logicalProjectDir`）：fork 的 `config.cwd` 是 `workflow-controller-tb-*` 隔离 tempdir，前端按 `origin_session_id` 在 sessions 列表查根 origin 的 `project_dir` 作分组（`origin_session_id` 在 fork 创建时已解析到根）。
> - **侧边栏扁平一层**（`AppSidebar`）：只列真实项目目录（key=`dir:<cwd>`），点击切换 `project_dir` 指针；fork session 在 Dashboard 展示。
> - **Dashboard 过滤改用 `logicalProjectDir`**（`DashboardView`）：`sessions.filter(s => logicalProjectDir(s) === projectDir)`，点进 origin 项目能看到其全部 fork。
> - **fork / 重建 session 删除对称清理 `recent_dirs.json`**（`agent_traceback._on_session_delete`）：fork tempdir cwd 若被误登记进 json（旧版残留 / 用户手选），删 session 时一并 `remove_recent_dir`（与重建对称），否则侧边栏留指向已 rmtree 的 tempdir 的幽灵条目。
> - **从 cannbot db 重建的 session 目录持久登记**：`POST /api/sessions/import-db` 重建成功后把 `target_cwd` 写入 `recent_dirs.json`。重建 session 的账面与检查点均落守护进程库（重启可还原），`target_cwd` 只承载会话态/文件态产物；session 删除后其 cwd 失去收集来源 → 侧边栏丢失该项目，显式登记兜底运行期可见。

### Insight 启动命令（控制台侧）

命令装配实现住控制台发行包；内核 CLI 只保留 `web` 子命令的选项声明与缺席时的可诊断错误。

`workflow-controller web --insight` 启动 Insight 服务；传入命令时执行指定可执行文件，省略值时执行 PATH 中的 `cannbot-insight` 命令。命令位于 `node_modules/.bin` 时，子进程工作目录改为对应包目录，避免 Next.js 依赖解析失败。子进程环境包含端口和浏览器控制变量；CLI 启动后把 Workflow Controller 端口写入进程环境，Web worker 只以环境端口为准确认服务可用。

本地质量基线和推送策略属于工程治理。

## 目录结构

本仓是 uv workspace：内核与各内置插件各自成包，共享 `workflow_controller` PEP 420 命名空间（`workflow_controller/`、`workflow_controller/plugins/` 不携带 `__init__.py`）。

```text
axcor/
├── packages/
│   ├── kernel/                        # workflow-controller-kernel
│   │   ├── src/workflow_controller/   # api core driver event hooks plugin(SDK) session
│   │   │                              # store support transcript watchdog config resources
│   │   │                              # __main__.py headless.py logging_config.py
│   │   ├── kernel_tests/              # 内核测试（unit/integration/transcript/watchdog/
│   │   │                              # regression/tooling/fixtures）
│   │   └── pyproject.toml
│   ├── hookserver/                    # workflow-controller-hookserver（独立子包）
│   ├── opencode-file-provenance/      # OpenCode 宿主 npm 子包
│   └── plugins/
│       └── fake-driver/               # workflow-controller-plugin-<name>；其余内置插件
│                                      # 逐包迁入，目录形如 src/workflow_controller/plugins/<name>
├── tests/                             # 跨包架构门禁（architecture）与共享测试基建（support）
├── examples/  scripts/  conf/
└── docs/
```

内核各包职责（api/core/…）与单仓布局一致；控制台（Web UI、路由装配、Insight 反代）不在本仓，作为控制台发行包消费内核稳定 API。

## File Provenance（V1）

`file_provenance` 是独立内置插件，负责把 Agent/自动化的文件变化转换为可查证的逐行归因。插件默认关闭，通过 `PluginContext.file_changes` 订阅 capture capability；`agent_traceback` 仍只负责会话检查点与重放，不改名、不承载提交归因。

文件变化捕获通过 EventBus awaited observer 与插件 capability 两层边界提供：公开的 `FileChangeContext`、`FileChangeOutcome` 与 `FileChangeCapture` 接口位于 `workflow_controller.plugin`，核心层的 `FileChangeRegistry` 实现该接口，并作为 `tool_will_execute` / `tool_execution_finished` / `tool_execution_aborted` 的 `observe_awaited` handler。各 runtime 与 Graph 只发布工具生命周期事件，不直接调用 Registry；EventBus 保证 handler 在发布者继续执行前完成且不拥有控制权。内置插件不得反向导入 `workflow_controller.core`：`PluginRegistry` 将 Orchestrator 持有的 Registry 注入 `PluginContext.file_changes`，归因插件再订阅这个 capability。

- Registry 由 Orchestrator 单例持有并绑定 EventBus，通过 `PluginContext.file_changes` 暴露；插件在 install/uninstall 显式订阅/取消订阅。
- 工具生命周期事件使用 `EventType` 枚举成员并携带可序列化的 context/outcome；`tool_will_execute` 在工具运行前发布，完成、失败或取消分别以 finished/aborted 闭合，事件可由 replay 与 hook adapter 正常反序列化。
- `before` 回调必须在工具执行前 await 完成，`after` 在成功或失败后都执行；无订阅者时不扫描文件。
- 捕获错误为 fail-open：记录日志，不改变工具决策或返回值。
- 工具任务被取消时仍须同步等待已开始的 `after` 清理完成，再向上游传播取消；子进程 capture 记录终止后的真实 returncode 并标记 incomplete。Graph 验收脚本被取消或超时时同样属于未自然完成，必须以 incomplete/aborted 闭合。capture 在清理完成前保持 closing 状态，避免重复 post 或 SQLite 活动窗口、内存快照泄漏。
- Registry 按 capture ID 固定保存成功完成 `before` 的订阅者；插件 reload 后，已有 capture 仍交回原实例闭合，缺失前置快照时只记录 incomplete/conflict，绝不以空快照推导归因。
- Session 结束时回收未收到 post hook 的 capture，并保守降级为 unknown/conflict。
- 公开边界类型为 `FileChangeContext` / `FileChangeOutcome`；一个 context 允许最终无变化。

三路 runtime 的同步边界：Claude SDK 用 PreToolUse/PostToolUse，Claude CLI 用 HookServer 的 pre/post tool 路由，并兼容从顶层 `is_error` 或 `tool_response.is_error` 读取失败状态；OpenCode 通过进程级 `OPENCODE_CONFIG_CONTENT.plugin` 合并注入随包 TypeScript adapter，并调用只触发 file-change 回调、不重复生成 `tool_use` / `tool_result` / `tool_call` 观察事件的专用 HookServer 路由；已有进程级配置（包括 plugin、agent、MCP 和 instructions）必须保留，不改用户的全局或项目 OpenCode 配置。OpenCode adapter 只匹配官方可变工具名 `edit`、`write`、`apply_patch` 和 `bash`，不维护其他 runtime 的兼容别名。各 adapter 透传原生 sub-agent ID/type，避免同一父 Session 下的子代理被折叠为同一个 actor。Graph Agent 节点沿用 runtime 链路；Graph 直接 Bash 和验收脚本以 `automation` actor 单独捕获。

### Standalone OpenCode Plugin

文件归因的 OpenCode 插件实现不依赖 Workflow Controller Web 或 Orchestrator；直接运行 OpenCode 时，`@workflow-controller/file-provenance` 复用同一份证据格式。TypeScript 插件订阅 `chat.message`、`tool.execute.before` 与 `tool.execute.after`：前者缓存 session 的模型、agent、初始/当前文本 prompt，后两者为 `edit`、`write`、`apply_patch`、`bash` 建立同步 capture 窗口。插件不复制 Python 归因算法，而是自动拉起用户已安装的 `workflow-controller provenance serve --project <worktree>`，通过 versioned JSONL stdio 协议调用 `ProvenanceEngine` 与 `ProvenanceStore`。

sidecar 固定服务一个 project root，只接受 root 内的 cwd；stdout 只承载 `ping` / `before` / `after` / `shutdown` 响应，诊断写 stderr。before 必须等基线快照完成才放行工具；协议超时、后端缺失或崩溃均 fail-open，客户端终止失效 sidecar 并在后续工具调用时惰性重启。未收到 after 的窗口由超时回收或 shutdown 以 incomplete/conflict 闭合；进程异常退出留下的活动窗口仍由 Store 的 owner PID 清理恢复。after 先完整校验 outcome，再移除活动窗口；畸形 payload 不得丢失可重试的 capture 状态。

Standalone 模式只维护 `.workflow-controller/file-provenance/` 的 blob、capture、mutation 与当前工作树逐行 attribution，不启动 `CommitFinalizer`，因此不写 Git notes、commit 指标，也不提供 OpenCode 查询工具或 UI。若 Workflow Controller 托管的 OpenCode 进程已经设置 `WORKFLOW_CONTROLLER_HOOK_URL` 与 `WORKFLOW_CONTROLLER_SESSION_ID`，npm 插件必须停用 standalone capture，把唯一写入权交给 Workflow Controller 注入 adapter，避免同一工具调用被重复归因。首版只承诺稳定版 OpenCode hooks；v2 plugin API 另行适配。

### Evidence Store

`workflow_controller.plugins.file_provenance` 在每个 worktree 的 `.workflow-controller/file-provenance/` 下维护独立证据库：SQLite 元数据写入 `provenance.db`，文件内容按 SHA-256 写入 `blobs/<前两位>/<其余位>`。插件从 capture 的 cwd 向上定位最近的 Git worktree；非 Git 项目使用 cwd。同步的 Git、SQLite 与文件操作从 awaited observer 卸载到工作线程，per-worktree Store/Engine 也在共享锁外初始化，锁内只发布已构造实例。blob 先写同目录临时文件并 fsync，再原子替换并 fsync 父目录，保证文件内容和目录项都可持久恢复；数据库只引用完整 blob，进程中断最多留下可回收的孤立 blob，不会引用半写内容。已验证且文件签名未变化的 blob 可在同一 Store 进程内复用完整性校验结果，外部变化会使缓存失效。数据库连接启用 WAL、foreign keys 与 5 秒 busy timeout，schema 以 `PRAGMA user_version` 显式版本化，不与核心 `events.db` 混用。

store 基础层只负责可验证证据，不推导逐行归因。`captures` 保存 actor、工具、输入/输出、起止时间、完成状态与前后 HEAD；`capture_files` 保存 before/after 快照到 blob 的引用；`mutations` 保存路径、前后 blob、增删行数与冲突标记；`active_captures` 保存跨进程活动窗口；`conflicts` 保存重叠、跨提交等保守降级原因。对外方法仅保留有调用方的 begin/complete capture 生命周期，以及紧随其后的 attribution 与查询层确定需要的快照、mutation 和 capture 读取；blob 读写属于 store 内部细节，冲突也随 capture 聚合返回，不再提供独立的无消费者查询。查询层把 SQLite 的布尔语义列还原为 `bool`（未完成 capture 的可空结果仍为 `None`），避免调用方依赖 0/1 存储表示。逐行状态、commit 映射、Git notes、人工认领和 blame 查询由后续层建立在这些证据上。

`begin_capture` 在 `BEGIN IMMEDIATE` 事务中清理 owner PID 已不存在的活动窗口：遗留 capture 同时以 incomplete/conflict 闭合，不能永久停留在运行中；发生重叠的所有窗口均标为 conflicted。after 扫描完成后，`end_capture_window` 先在事务中闭合活动窗口并返回冲突状态，attribution 层依此决定能否精确归因；`finish_capture` 在推导与证据入库后持久化 outcome，前后 HEAD 不一致时记录 `capture_crossed_commit`。重复结束保持幂等，缺失活动窗口按 conflict 处理，避免把不完整链路声明为精确证据。所有入库路径必须是规范化的 worktree 相对 POSIX 路径；绝对路径、`.`、`..` 和反斜杠均拒绝。

### Attribution Engine

attribution 层在 before callback 中扫描 worktree 并持久化 before 快照；工具执行期间内存只保留已准备完成的 capture ID，after callback 从证据库批量加载基线并再次扫描，按路径比较新增、修改、删除和唯一的纯重命名。扫描在目录遍历阶段直接裁剪 `ignore` 规则命中的目录，永远不进入 `.git` 和 `.workflow-controller`，默认也忽略常见虚拟环境、依赖缓存和构建输出目录，并跳过符号链接、匹配规则的文件与超过 `max_file_bytes` 的文件。内置默认规则以 attribution engine 为唯一事实来源，插件配置中的 `ignore` 仅追加项目特有规则，不能移除内置默认。快照持久化在同一数据库连接中按 SQLite 参数上限分批读取 `file_state`；没有匹配状态的文件以空 attribution 数组保存，查询时再按文本行数补齐 unknown，避免连接数、工具执行期间的常驻内存和证据体积随文件总行数无谓增长。before 快照未完成时不得运行工具；若 after 阶段读取 before 快照或扫描 worktree 失败，则以准确描述失败阶段的 incomplete/conflict 闭合，不得留下活动窗口。

文本文件通过 `SequenceMatcher` 把未变行的 attribution 传递到新版本，新增或替换行归当前 actor；无法 UTF-8 解码的内容仍保留 blob/mutation 证据，但不伪造逐行结果。基线内容与 `file_state` 不一致时，旧行一律视为 unknown。重复内容导致无法唯一映射、capture 窗口重叠、outcome incomplete 或跨进程重叠时，受影响的新 attribution 必须降级为 unknown/conflict。只有内容完全相同且旧、新路径各唯一时才按重命名传递旧 attribution。

`file_state` 保存 worktree 当前文件内容 blob 及与文本行等长的 attribution 数组；每个 attribution 包含 actor kind/ID、capture/session、runtime/model 和 agent 标识。Agent actor ID 由 runtime、session 与 native agent ID 稳定派生，同一 session 中的不同 sub-agent 不得被折叠；automation 使用固定 ID。并发 capture 可以各自保留证据，但不能声称精确逐行归因。

### Git Authorship Finalization

Git worktree 的 finalizer 在插件初次见到项目时把当前 HEAD 记为游标，不回填 Workflow Controller 启动前的历史；之后在有 mutation 的 capture 闭合后立即检查，并以可配置周期轮询 HEAD，以捕获稍后发生的人工 commit。游标到 HEAD 的多个 commit 沿 first-parent 链从旧到新逐个处理；capture 仅能归入其 `head_before=head_after=parent` 的下一个 commit，conflicted、incomplete 或跨 commit capture 不得进入精确指标。如果检测到游标已不是 HEAD 祖先，记录 `history_rewritten` 冲突，从 merge-base 重放当前链；无 merge-base 时只从新 HEAD 恢复，不把旧链证据嫁接到新历史。

每个 commit 的新增行与该 parent 边界上最后一个合格 after 快照比较，只有内容唯一匹配的行才携带 attribution；结果写入 `refs/notes/ai` 的 `authorship/3.0.0` note，并把 commit 时点的逐行状态落到 `commit_lines`。已有合法 note 的行区间优先，Workflow Controller 只补充未被占用的新增行；已有 note 非法时原样保留并记录冲突，不强制覆盖。单进程使用 per-repository async lock，跨进程在 Git common dir 使用文件锁，防止 worktree 共用 notes ref 时互相覆盖。`commits` 记录 AI/总新增行、capture 生成行与最终采纳行，重复 finalization 必须幂等。人工认领只允许在 finalizer 游标已经等于 HEAD 时重新 finalization；游标落后时必须保留原值，由常规 finalization 沿 first-parent 链补齐全部 commit。非 Git 目录仍保留 worktree attribution，但不启动 finalizer。

### Query、人工认领与删除脱敏

插件通过自己的 Web router 暴露文件列表、当前/指定 commit 的 blame、commit 指标、capture 与快照证据；capture 查询返回 Store 已持久化的会话与原生标识、runtime/model/agent、工具输入输出、上下文 attributes、执行状态、Git 边界、mutation 和冲突，不从核心事件库推测 Store 未记录的轮次等字段。同步 Store 查询统一卸载到工作线程。完整文件归因视图只在 Session Detail 的「文件归因」tab 中提供，固定使用当前 session 项目且不改变当前会话路由；Dashboard 主侧栏不提供独立入口。API 只能访问插件启动目录或已经实际收到 capture 的 worktree，未知路径返回 403，不能借查询请求在任意目录创建 `.workflow-controller`。显式查询不存在的 commit 或证据返回 404；请求字段和认领范围等输入校验失败返回 422；只有 attribution 或工作树内容在 compare-and-swap 期间发生变化才返回 409。Store 提供明确的只读查询方法，Web 层不得访问 `_connect`、blob 路径或插件内部缓存。

当前工作树允许把明确选中的 unknown 行认领为 human：请求必须携带规范化相对路径、Git committer 格式的 author、1-based 闭区间和读取时的内容 SHA-256。Store 在单个事务中同时校验数据库快照与磁盘内容，使用旧 attribution 做 compare-and-swap，只修改 unknown 行并同步相同 after 快照；越界、二进制、陈旧内容或没有可认领行均拒绝。若内容正好等于 HEAD，插件重新 finalization 当前 commit，只补充尚未被已有合法 note 占用的 human 区间；历史 commit 不允许直接认领。

删除 session 时插件保留 mutation、内容 hash、commit 指标等审计证据，但清除本地 Store 中的 session/native ID、原始工具输入输出及 attribution 内的直接 session 标识。`refs/notes/ai` 属于 Git 仓库历史，不随 Workflow Controller session 删除自动改写。

### Sift Audit Evidence Export

`file_provenance` 提供 session 级只读导出，把核心 session 事件与归因 Store 中的 capture/mutation 合并为 `sift-records` v2 JSON。插件不得直接读取 Orchestrator 私有成员：宿主通过 `PluginServices` 提供只读的 session metadata/events 端口，Store 只暴露按 session 查询 capture 的公开方法。导出以事件流为主、通过 capture lifecycle 中的 `capture_id` 或唯一 `tool_use_id` 关联文件证据；关联歧义和未匹配 capture 必须保留为独立记录并产生 warning。

协议保持 `format: "sift-records"`，以 `version: 2` 区分增强字段。根对象包含 session、records、captures、content-addressed blobs、coverage 与 warnings；只有 mutation 涉及路径的 before/after 内容进入 blobs，UTF-8 原文存储，二进制 base64，SHA-256 去重。capture 质量固定为 `observed / incomplete / conflicted / ambiguous`，只有完整且无冲突的 capture 可作为精确文件事实。导出接口为 `GET /api/provenance/sessions/{session_id}/audit-evidence?project_dir=...`，响应作为 attachment 且禁止缓存；未知项目/session 和插件禁用沿用 403/404/503 边界。用户消息从 `content` 文本块拼接，事件查询放入线程池避免阻塞事件循环。缺失快照只降低对应 effect 的证据质量并产生 warning，摘要校验失败仍拒绝生成不完整证据包；SUBAGENT 控制记录的 `parent_tool_use_id` 仅表示真实 tool-use 关联，不以 agent/session 标识替代。

Standalone OpenCode 使用同一协议的离线入口：`workflow-controller provenance export-audit-evidence --project <worktree> --session <native-session-id> [-o <file>]`。命令不启动 Web/Orchestrator，也不调用 `opencode export`；它用只读 SQLite 连接读取 OpenCode DB 的 `session/message/part` 最终状态，从根 session 沿 `parent_id` 递归包含全部子 session，再以 `native_session_id + callID/tool_use_id` 合并该 worktree 已存在的 provenance capture。OpenCode DB 可由 `--opencode-db` 显式指定，否则遵循 `OPENCODE_DB_PATH`、XDG data dir 与默认路径发现顺序。

capture 的 `tool_use_id` 歧义判定保持全局作用域：即使 capture 带有不同的 `native_session_id`，同一非空 tool-use ID 的多个 capture 仍标记为 `ambiguous`（除非事件通过明确的 `capture_id` 关联），避免消费方把跨 session 的重复 ID 当作精确事实。`tool_use_id` 缺失或为空的 capture 不参与全局歧义判定——它们只是没有可对齐的 tool-use，不应因共享「空 ID」而被集体降级。事件匹配仍优先使用 `(native_session_id, tool_use_id)`，仅在事件缺失原生 session ID 时回退到全局 ID。

离线主轨迹保留用户/助手文本、reasoning、工具调用与结果、step/error 以及主子 session lineage；父 session 中 task 工具的 metadata `sessionId` 用于恢复 child session 对应的 parent tool-use。当 task part 缺少 metadata 时，仅在工具名为 `task` 的 part 上回退解析其 `<task id="ses_…">` 输出信封，不接受正文中的裸 `task_id:` 文本，也不得在其它工具输出上匹配该正则，避免把 parent tool-use 错误归因到非 subagent 调用。用户消息统一编码为 `content: [{type: "text", text: ...}]`，确保多轮输入经过审计记录构造时保留完整文本及事件元数据。控制事件的展示文本只包含语义数据，agent 与 parent/native session 等 scope 字段单独结构化落盘；时间戳缺失或类型异常而回退到 Unix epoch 时必须写入 coverage warning，避免审计消费者误判记录顺序。OpenCode append-only `event` 表包含大量流式重复快照，首版不把它当作规范轨迹，只在 coverage 中声明未包含瞬时/已删除事件。文件证据 Store 也以只读模式打开，缺少 provenance DB、未知 session、schema 不兼容、blob 缺失或 SHA-256 损坏均明确失败，不生成无文件证据或部分可信包；损坏或被锁定的 SQLite provenance DB 错误统一转换为用户可读的导出失败信息，不泄露 traceback。

输出先写同目录临时文件并 fsync，再发布到目标；默认拒绝覆盖已存在目标，优先使用不覆盖的原子硬链接语义，在不支持硬链接的文件系统上回退到 `O_CREAT|O_EXCL` 独占创建并 fsync 内容，只有显式 `--force` 才允许替换。发布前临时文件统一 chmod 为 `0o644`，使硬链接、`O_EXCL` 回退与 `--force` 原子替换三条路径产出的审计包文件权限一致，不因文件系统能力不同而漂移。若 task metadata 引用的子 session 不在 `parent_id` 导出树中，coverage 必须产生 warning，不能静默遗漏。包内记录 OpenCode DB 路径、根/子 session 数、轨迹覆盖 warning 和稳定原生 ID，但不复制 OpenCode `event` 原始快照或整个工作树。
