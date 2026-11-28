---
type: adr
title: "ADR-0002：内核面 HTTP 路由下沉内核包，新增 workflow-controller serve"
status: Accepted
owner: "chenyangwang"
created: 2026-11-28
related_docs:
  - docs/ARCHITECTURE.md
  - HEADLESS.md
---

# ADR-0002：内核面 HTTP 路由下沉内核包，新增 `workflow-controller serve`

> 本决策把只依赖 `CoreAPI` 的 HTTP 路由从控制台发行包（opsbot）迁入内核包 `workflow_controller.api.http`，并让内核在仅安装内核包时就能独立接收控制类 HTTP 请求。本文记录决策、被否决的方案与后果；路由面的逐条约定以 `packages/kernel/kernel_tests/contract/manifest.yaml` 为准。

## 状态

Accepted（2026-11-28）。施工分两步：axcor 纯新增（本 ADR 所在改动），opsbot 切换为复用内核路由（独立 PR，依赖 axcor 发版）。

## 背景

axcor 被定位为后端，但仓内没有任何一条控制类 HTTP 路由。内核的 HTTP 监听只有接收宿主回调的 hook server（aiohttp）、驱动内部临时启动的 MCP 服务，以及只定义不挂载的插件 router SDK。今天要让 axcor 接收一次「创建 Run」的请求，只有进程内调用 `CoreAPI` 这一条路。

路由住在控制台仓不是一次「HTTP 该归谁」的决策，而是拆仓时 `web/` 目录按映射表整体搬移的结果。拆分之后暴露出四个问题：

1. **axcor 无法独立提供控制 API**：仅装内核时 `workflow-controller web` 报「Web UI is not available in this kernel-only install」。
2. **内核的消费者被迫拉上控制台**：需要经 HTTP 控制内核的调用方（如 RSI 在线修复闭环）必须安装带前端和 Insight 的完整包。
3. **内核能力与它的 HTTP 表现分属两仓**：内核每增加一个 `CoreAPI` 能力，都要去另一个仓补路由；traceback 与 RSI 计划里的回滚查询/回滚端点因此登记为跨仓待办（S2w、S6w）。
4. **约定与实现分家**：约定清单与互锁测试住控制台仓，被约定的端点、SSE 事件与告警词表却全部实现在内核。

控制台正式路由面共 95 个 `/api/v1` 端点（stable 58、experimental 28、internal 9），分布在 9 个路由域。其中 8 个域的 91 个端点只经 `CoreAPI` 访问内核，对 `workflow_controller` 的导入只有 `api` 包与同目录的 `common.py`；剩下 1 个域（`context_bay`，4 个端点）依赖领域包 `context_bay_exporter` 与 Insight。

| 路由模块 | 前缀 | 端点 | Swagger tag | 归类 |
|---|---|---|---|---|
| `sessions_routes` | `/api/v1` | 37 | `sessions` | 内核面 |
| `runs_routes` | `/api/v1/runs` | 21 | `runs` | 内核面 |
| `auth_routes` | `/api/v1` | 10 | `auth` | 内核面 |
| `config_routes` | `/api/v1` | 9 | `config` | 内核面 |
| `plugins_routes` | `/api/v1` | 7 | `plugins` | 内核面 |
| `overview_routes` | `/api/v1` | 5 | `overview` | 内核面 |
| `sse_routes` | `/api/v1` | 1 | `sse` | 内核面 |
| `watchdog_routes` | `/api/v1` | 1 | `watchdog` | 内核面 |
| `context_bay` | `/api/v1` | 4 | `context_bay` | 控制台领域面 |

内核包的依赖清单早已含 `fastapi` 与 `uvicorn`，`api` 包的模块说明也一直自称「HTTP API 层」。

## 决策

把内核面的 HTTP 表现下沉到内核包，内核新增 `serve` 子命令独立监听；控制台改为在内核路由之上叠加 UI、Insight 反代与 Context Bay。单进程部署与「`boot()` 是唯一内核启动入口」不变。

### 目标结构

```text
axcor packages/kernel/src/workflow_controller/
├── api/
│   ├── …_api.py              不变：进程内门面
│   └── http/                 新增：CoreAPI 的 HTTP 表现
│       ├── app.py            create_app()：boot、内核路由、插件路由、关停
│       ├── mounting.py       register_kernel_routes、register_plugin_routers，控制台复用
│       └── routes/           8 个路由模块与 common.py
└── __main__.py               新增 serve 子命令，函数内懒装配；web 子命令不变

opsbot src/workflow_controller/web/
├── app.py                    组合根不变，仍由它调用 boot()
├── console.py                内核路由、插件路由（复用 axcor）、context_bay、Insight、SPA
└── context_bay/              唯一留下的自有路由
```

### 归属划分

- **判定规则**：处理器只需要 `CoreAPI` 的进内核；需要领域包、Insight 或浏览器页面资源的留控制台。
- **进内核**：上表 8 个路由模块与 `common.py`；插件路由的启动期挂载与运行期注册回调；内核独立应用的工厂与 `serve` 子命令；内核面的约定清单与约定测试。
- **留控制台**：`context_bay` 路由与服务、`context_bay_exporter`、Insight 反代、SPA 与前端源码、`web` 子命令（Vite 与 Insight 子进程管理）、CORS 配置、运行期路由的 catch-all 顺序修正。

### 组合方式

- **内核独立应用** `create_app()`：lifespan 里调用 `boot()`、设置 `app.state.api`、挂载插件路由，退出时关停 orchestrator。`app.state.register_runtime_plugin_router` 默认直接 `include_router`——内核应用没有 catch-all，追加到末尾即可。
- **控制台应用**保留自己的组合根：`create_app()` 调用内核的 `register_kernel_routes`，lifespan 仍由控制台调用 `boot()`，再依次挂插件路由（复用内核函数，随后把注册回调换成 catch-all 之前的版本）、`context_bay`、Insight、SPA。`boot()` 的生产调用点因此由 2 处变 3 处：`api/http/app.py`、控制台 `web/app.py`、`headless.py`。
- **插件路由的枚举**经 `CoreAPI.plugin`：`list_plugins()` 取名称，`get_web_router(name)` 取 router，未装载或没有 router 时返回 `None`。HTTP 入口层因此不触碰 orchestrator，也不把 orchestrator 放上 `app.state`。
- **路由表拆分**：路由域表拆成内核的 8 行与控制台的 1 行，Swagger 分组取两者之和；挂载顺序不变（内核路由 → 插件路由 → `context_bay` → Insight → SPA）。
- **领域配置段扩展点**：`ConfigSectionProvider` 协议随 `common.py` 进内核，控制台的 `context_bay` 服务继续实现它并在装配期注册。内核独立应用没有领域段，`GET /config` 只返回内核配置。
- **内核生命周期的归属**：关停超时（120 秒）与 SSE 退出标志复位随应用工厂进内核，控制台复用。
- **宿主约定不变**：两种应用都先设置 `app.state.api`，插件 router 的 `plugin_by_name` 查找照常工作。
- **依赖**：内核包新增显式依赖 `sse-starlette`（此前只是 `mcp` 的传递依赖）。

### `serve` 子命令

`workflow-controller serve` 用 uvicorn 以工厂方式启动内核应用，不启动 Vite 与 Insight。选项为 `--port`、`--host`、`--hook-port`、`--settings-dir`、`--log-level`，与 `web` 子命令同名选项语义一致。CLI 在函数内懒装配：导入 `workflow_controller.__main__`、运行 `run` / `web` 以及 headless 验收的导入链都不得引入 `workflow_controller.api.http`。

### 产品决策

| 决策 | 选择 |
|---|---|
| D1 模块路径 | **`api/http` 子包**，不新设顶层包：`api` 包本来就自称 HTTP API 层；子包按需导入，进程内嵌入方导入 `api.core_api` 时不会加载它；不需要在架构门禁登记新包 |
| D2 控制台的 `boot()` 调用 | **保留自己的组合根**（3 处调用）：改动小，控制台的日志、品牌标题与 SSE 复位逻辑不用重排 |
| D3 `serve` 的默认端口 | **18600**，与 `web` 相同：两者是替代关系，各自启动内核，不会同时运行 |
| D4 两个 breather 专属端点 | **随 `plugins_routes.py` 原样搬移**，之后另议是否交给 breather 插件自己的 router |
| D5 鉴权 | **不做，仅本机**：默认绑定 `127.0.0.1`；远程调用需求明确后先做 token 鉴权，作为独立决策 |
| D6 进程拓扑 | **不变**：仍是单进程，控制台与内核同进程；本决策不引入跨进程 RPC |

## 被否决的方案

| 方案 | 否决理由 |
|---|---|
| B. 独立适配包（同样的代码做成 axcor 仓内单独的发行包） | 与 A 只差包边界。`fastapi` 与 `uvicorn` 仍是内核的硬依赖，隔离只是名义上的，却要多维护一个包的 pyproject、lock、CI 矩阵与子集导出 |
| C. 维持现状（axcor 只做库和 headless，宿主自行 `boot()` 并编写适配层） | 每个消费者各写一套路由，约定漂移，跨仓待办持续累积；与 RSI 和 traceback 的现有 HTTP 需求矛盾 |
| D. 进程拆分（axcor 常驻服务，控制台变成纯前端加瘦代理） | 现在没有必要：A 完成后代码不需要再搬就能演进到 D；而 D 要解决跨进程鉴权、SSE 和 Insight 子进程管理 |
| E. 并入 hook server（把控制端点加到现有 aiohttp 端口） | 数据面与控制面共用端口和信任模型；与「hook server 改用 fastapi」的评审方向相反 |

## 后果

### 正面

- 仅安装内核包即可经 HTTP 创建、观察、暂停、恢复、停止 Run；内核的新增 HTTP 面在本仓一次完成，不再产生跨仓路由待办。
- 约定清单与被约定的实现同仓：91 条内核面端点、9 个 SSE 事件名与 3 个告警类型的互锁测试随路由住进 `kernel_tests/contract/`。
- 内核在 axcor 锁定的依赖版本下跑全量路由测试，路由面的兼容性第一次有了版本基线。
- 控制台不再维护路由模块，只叠加领域面；控制台路由面与内核路由面由结构比较测试互锁。

### 代价与风险

- **fastapi 版本差**：路由模块此前只在控制台锁定的 0.136.3 下运行过。搬入后须在 axcor 锁定版本下跑全量路由测试，并据此决定两仓的版本约束如何统一。
- **门禁改指**：「路由只能导入 `api`」的检查必须同 PR 改指 `api/http/routes`，否则对搬走的文件静默失效。
- **headless 回归**：headless 与 CLI 导入链不得带进 HTTP 层，由 meta-path 守卫与 CLI 隔离断言锁定。
- **双份路由的窗口期**：axcor 合入到控制台切换之间两仓各有一份路由模块，窗口期内路由改动要么两处同改，要么推迟到控制台切换之后。
- **无鉴权**：`serve` 一旦绑到非回环地址，任何人都可以控制内核。默认只绑回环地址。
- **子集导出**：导出子集随之多出 HTTP 表现与一个依赖，默认随附。

### 后续

- 新的内核面路由直接写进 `api/http/routes/` 并登记内核约定清单；traceback 与 RSI 计划中登记为「控制台仓路由装配」的 S2w、S6w 以及 scopes、checkpoints、operations 查询路由改在本仓完成。
- 需要常驻后端的调用方可以改用 `workflow-controller serve`；RSI 规格里「最小实现用 `web --no-open` 启动后端」的表述随之修订。
- 「hook server 改用 fastapi、`/hooks/*` 挂到同一个应用」在本决策之后具备条件，但它会改变 hook 端口、headless 下的启动方式与写进 `.claude/settings.json` 的 URL，另开决策。

## 备注

- 删除 `packages/plugins/agent-traceback/` 的工作住在尚未合入 `main` 的 traceback 分支；本决策以 `main` 为基线，不涉及控制台对该插件的依赖。
- **fastapi 版本差**：axcor 锁定 0.163.2，控制台锁定 0.136.3（`<0.137`）。搬入的 8 个路由模块与约定测试在两个版本下都通过——axcor 全量测试在 0.163.2 下运行，另在只装 fastapi 0.136.3 的临时环境里跑通 `kernel_tests/http` 与 `kernel_tests/contract`。两版的差别在路由树形态：0.141 起 `include_router` 把子 router 包成单个 `_IncludedRouter`，`app.router.routes` 不再是扁平的 `APIRoute`；控制台的 catch-all 前插逻辑与其测试建立在扁平路由上。因此本次不统一两仓的 fastapi 约束，内核侧的路由面枚举改用两版形态一致的 OpenAPI 路径表；统一版本需要控制台侧同步改造，另议。
- **搬移带来的两处对外文案变化**：`runs` 路由的 404 detail 改为「run 不存在：{run_id}」，`auth/test-key` 的 500 detail 改为「供应商试测结果与响应结构不匹配」。axcor 的 PR 用词门禁对全部新增行生效，原有文案不能原样带入；状态码与应答结构不变，控制台前端只按状态码判定，不依赖这两段文案。
- **与单仓布局清理的重叠**：原先恒跳过的两个 web 路由门禁用例改指 `api/http/routes`——保留私有符号检查；orchestrator 访问检查由基线已清空的精确基线测试覆盖，不再单独保留。指向不存在的 `web/app.py` 的边界债务同时清空。
