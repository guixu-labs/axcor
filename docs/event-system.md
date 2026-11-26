# Workflow Controller 事件系统

> 面向 Workflow Controller 开发者的事件系统参考文档。读完本文应能理解：事件从哪产生、怎么持久化、
> 怎么分发到观察者/拦截器、怎么推到前端，以及每类事件的语义和适用场景。

## 1. 核心数据结构

### 1.1 Event

`src/workflow_controller/session/events.py:36`

```python
@dataclass
class Event:
    id: str               # uuid4().hex，32 字符，PRIMARY KEY
    session_id: str       # Workflow Controller session id（不是 SDK/CLI session id）
    iteration: int        # loop 轮次（非 loop 场景为 0）
    timestamp: datetime   # datetime.now()，Event.create 时刻
    event_type: EventType | str # 枚举外的开放词表事件（如 fault.*）以字符串承载
    data: dict[str, Any]  # 任意 JSON 可序列化 dict
```

- `Event.create(session_id, iteration, event_type, data)`：工厂方法，自动生成 id 和 timestamp。
- `to_dict()` / `from_dict()`：序列化时 `event_type` 存 `.value` 字符串（如 `"tool_use"`；字符串事件类型原样往返，`from_dict` 对枚举外的字符串回落原值），timestamp 存 ISO 字符串。
- **id 是 Python `uuid4().hex`**，不是数据库自增——幂等写入靠 `INSERT OR REPLACE`。

### 1.2 EventType 枚举

`src/workflow_controller/session/events.py:11`

21 个枚举值（另有 6 个字符串型控制事件，不经枚举），按用途分 5 类：

| 类别 | 标识 | 语义 | 谁发 | 谁消费 |
|------|------|------|------|--------|
| **工具** | `TOOL_USE` | 工具调用（post-hoc 观察）| 三路 driver + HookServer（post-tool-use）+ session_api replay | `feedback_evaluator`、`question_payload`（抽工具调用 / AUQ）|
| | `TOOL_RESULT` | 工具调用结果 | 三路 driver + HookServer + session_api replay | `breather._on_notify_skill_hook`（`bus.observe(TOOL_RESULT)` 后查 `tool_name`）、插件在 `_register_observers` 里 `bus.observe(TOOL_RESULT, handler)` 自注册 |
| | `TOOL_CALL` | 工具调用摘要（tool_use_id+name+arg+is_error）| 三路 driver + HookServer `_emit_tool_call` | 前端展示（VirtualLog 唯一渲染入口）|
| **消息** | `ASSISTANT_MESSAGE` | LLM 文本输出 | driver | loop 疑问检测、reviewer、前端 |
| | `NOTIFICATION` | 系统消息（**纯展示性 notification 拋余桶**：CLI `/hooks/notification` payload、SDK `thinking_tokens` 遥测等；不再承载 iteration 起停——生命周期已拆出 ITERATION_START/END）| driver、HookServer `/hooks/notification` | 前端 |
| | `ITERATION_START` | driver 单次 query() 调用开始、首次给出 sdk/cli session id（resume 前置）| driver（opencode `id_ready.set()` 时刻、SDK SystemMessage 带 session_id 子类、CLI stdout `subtype=init`）、HookServer `/hooks/session-start` | `_register_session`（类型化 observe 此类型存 sdk_session_id） |
| | `ITERATION_END` | driver 单次 query() 调用结束、sdk session id 不再可用（driver 层、per-iteration；与编排层 `session_complete` 区分）| driver（opencode/SDK/CLI 的 query 末尾、异常路径）、HookServer `/hooks/session-end` | 插件 `bus.observe(ITERATION_END, handler)` |
| | `USER_MESSAGE` | 用户输入 | driver（opencode 不回显用户输入，主动发）| 前端展示 |
| | `STEP_FINISH` | step/turn 结果（含 usage）| driver | auth_rotator、breather（抽 usage）、reviewer |
| | `ERROR` | 错误（**可观测日志**）| driver | 前端展示；LimitDetector 不消费（限流信号已由 driver 翻成 `RATE_LIMIT`，ERROR 不再承载轮换决策）|
| | `RATE_LIMIT` | **driver 无关的归一化限流事件**（`data.limit_signal={kind,scope,reset_at,utilization,source}` + `data.raw` 原生负载）| 三路 driver（claude_cli 结构化 `rate_limit_info` / claude_sdk 异常文本正则 / opencode error dict `statusCode`）| LimitDetector（on_rate_limit / on_limit_detected / on_quota_warning）|
| **子代理** | `SUBAGENT_START` | 子代理启动 | orchestrator | watchdog、agent_traceback（**可被拦截**——冻文件态快照，阻塞串行）、前端 |
| | `SUBAGENT_PROGRESS` | 子代理增量消息流（opencode 轮询、claude 暂未支持）| orchestrator | watchdog、前端 |
| | `SUBAGENT_STOP` | 子代理结束（含 token/content/messages）| orchestrator | watchdog、breather（handoff 检查时机）、前端 |
| | `PROMPT_OVERLAY_REGISTERED` | 用户注册 prompt overlay（per-subagent-type 或 global）| session_api（`bus.emit`，回流总线）| 审计追踪 + 可 `bus.observe` 订阅；overlay 注册表状态**同步写**（不进 observer，保「注册→下一轮生效」契约，见重构设计附录 C.5）|
| | `PROMPT_OVERLAY_CLEARED` | 用户清除 prompt overlay（按 index 或全清）| session_api（`bus.emit`）| 审计追踪 + 可 `bus.observe` 订阅 |
| **控制事件（字符串型，不经 EventType 枚举）** | `"stop_requested"` | worker 触发 stop hook（CC Stop / opencode `step_finish` end_turn） | orchestrator（`_decide_stop`） | 插件 `bus.intercept("stop_requested", ...)`（priority=10）+ reviewer 拦截器（priority=0），**inline 返回 InterceptResult**（block/stop/let_exit）|
| | `"pre_tool_use"` | AskUserQuestion pre-tool-use 拦截 | orchestrator（`_decide_pre_tool_use`） | 插件 `bus.intercept("pre_tool_use", ...)` + reviewer 拦截器，inline 返回 InterceptResult |
| | `"iteration_boundary"` | loop iteration 边界门禁 | orchestrator（`_run_iteration_boundary_gates`） | 插件 `bus.intercept("iteration_boundary", ...)`（staged_pipeline/breather）+ reviewer 拦截器，inline 返回 InterceptResult |
| | `"gate_question"` | opencode per-query MCP gate 触发的问答 | orchestrator（`_resolve_gate_question`） | reviewer 拦截器（auto→reviewer AUQ / manual→人工队列），inline 返回 InterceptResult |
| | `"session_complete"` | session loop 跑完（`loop_status=no`+完成 stop_reason）| orchestrator（loop 回调）| 插件 `bus.observe("session_complete", ...)`（fire-and-forget，终态快照/清理）|
| | `"session_delete"` | session 被删除（核心表清完之后）| orchestrator（`SessionAPI.delete`）| 插件 `bus.intercept("session_delete", ...)`（**inline 拦截器**，sync 清理如 rmtree fork_root）|
| **系统日志** | `LOG_AUTO` | orchestrator 自动审计（session 启停、hook 决策）| orchestrator | 前端展示 |
| | `LOG_QUERY` | loop prompt 渲染 | loop engine | 前端展示 |
| | `LOG_EVAL` | evaluator 结果 | loop engine | 前端展示 |
| | `LOG_LOOP` | loop 决策（stop/block/continue）| loop engine（10 处）| 前端展示 |
| | `LOG_MANUAL` | 用户手动输入 | session_api | 前端展示 |

> **拦截器 vs 观察者——两套派发通道**
> EventBus 把事件分成两条正交通道（详见 §2）：
> - **控制事件**（上表字符串型 + `SUBAGENT_START`）走 **inline 拦截器链**：`emit()` 时
>   按 priority 逐个 await，首个非 `pass` 的 `InterceptResult` 短路并回流给调用方
>   （`_decide_stop` / `_decide_pre_tool_use` 等据此决定 allow/deny）。**同步阻塞**，因为
>   决策结果要回灌 driver/loop。
> - **观察事件**（其余 EventType）走 **async 观察者队列**：`emit()` 把事件丢进
>   `asyncio.Queue` 立即返回，后台单 task `_process_observers` 按 type 串行派发。
>   观察者只读不决、不影响控制流，故不阻塞 emit。
> `EventType` 枚举的成员全是观察事件（除 `SUBAGENT_START` 同时被拦截与观察，`SUBAGENT_PROGRESS`/`SUBAGENT_STOP` 只观察）；控制事件不经枚举，直接用字符串键。

> **watchdog 告警走 `fault.*` 开放词表（字符串事件类型，不经枚举）**
> 告警类型按检测规则名生成（`fault.subagent_timeout` / `fault.file_stall` /
> `fault.keyword_detected`），检测规则即事件库订阅者，各自订阅 SUBAGENT_* 事件、
> 命中条件自行发告警。查询按 `event_type` 前缀 `fault.` 过滤，不用 json_extract hack，
> 也不污染 `LOG_AUTO`（那是 orchestrator 自己的 action 审计）。词表与 payload 编解码
> 唯一实现在 `watchdog/fault.py`，详见 [watchdog 设计文档](watchdog.md)。

> **ITERATION_START / ITERATION_END vs `session_complete` 的层级区分**
> 三个事件表达不同层级的生命周期：
> - `ITERATION_START` / `ITERATION_END` — **driver 层、per-iteration**：单次 driver
>   `query()` 调用的起止 + sdk session id 颁发/失效。一个 session 在 loop 模式下
>   会有 N 对 ITERATION_START/END（每 iteration 一对），resume 前置由
>   ITERATION_START 携带的 `session_id` 字段承担（`_register_session` 类型化
>   observe 此类型）。命名取「iteration」而非「session」是为了与下面的
>   `session_complete` 区分清楚——两者都带 "session" 容易混淆。
> - `"session_complete"` — **orchestration 层、per-loop**：loop 跑完的终态通知，
>   整个 loop 一次。插件用 fire-and-forget observe 做终态快照/清理。
> 两者不可互相替代：driver 层的「这次 iteration 的 driver 调用结束了」≠
> 编排层的「loop 完成」。

### 1.3 工具事件对齐设计

> 三个工具事件 `TOOL_USE` / `TOOL_RESULT` / `TOOL_CALL` 的产出与消费曾长期不对齐：
> - `claude_sdk` / `claude_cli` 把 `tool_use` / `tool_result` 作为 content block 嵌在 `ASSISTANT_MESSAGE.data.content` 里，**不**发独立事件，只发 `TOOL_CALL`；
> - `opencode` 把 call+result 拆三条独立事件（`_parse_tool`），但 `TOOL_RESULT.data` 无 `tool_name` 字段；
> - `HookServer` 的 post-tool-use 发 `TOOL_RESULT`（带 `tool_name`）+ `TOOL_CALL`，pre-tool-use 又发一条 `TOOL_USE`（时序在前、语义是"即将调用"而非"已调用"）；
> - `session_api` 重放路径两处发 `TOOL_USE` + `TOOL_RESULT`，data shape 互不一致（其一缺 `is_error`）。
>
> 后果：`breather._on_notify_skill_hook`（`plugins/breather/plugin.py:326`）查 `TOOL_RESULT.data.tool_name` 仅 CLI/HTTP hook 命中；当时 orchestrator 集中遍历 `_plugin_registry` 派发 `on_post_tool_use`（`_dispatch_plugin_observer_hooks`，现已移除——插件改为在 `_register_observers` 里 `bus.observe(TOOL_RESULT)` 自注册）在 SDK 路径根本不触发；前端 `SessionDetailView.vue:318,386-387` 显式跳过 `tool_use`/`tool_result` 事件与内嵌 block，只渲染 `TOOL_CALL`。

#### 1.3.1 统一 data shape

所有产出点必须按下表归一：

| 事件 | `data` 字段 | 说明 |
|------|------------|------|
| `TOOL_USE` | `{id, name, input}` | `id` = tool_use_id（SDK/CLI 取 `ToolUseBlock.id`，opencode 取 `part.id`，HookServer 取 `call_id`） |
| `TOOL_RESULT` | `{tool_use_id, tool_name, content, is_error}` | **`tool_name` 必填**——`breather._on_notify_skill_hook` 据此匹配 `notify_skill`，三路 driver 必须从配对的 `TOOL_USE.name` 回填 |
| `TOOL_CALL` | `{tool_use_id, name, arg, is_error, error?}` | 摘要事件；`tool_use_id` 与对应的 `TOOL_USE.id`、`TOOL_RESULT.tool_use_id` 相同（HookServer 取 post-tool-use 的 ID），供消费方关联；`arg` 由 `extract_tool_arg`、`error` 由 `extract_tool_error` 生成；前端 VirtualLog 唯一渲染入口 |

> `HookServer._emit_event` 给 `TOOL_RESULT.data` 塞 `source` / `tool_name` + 原始 hook payload，需收敛为上表 shape（`tool_name` 保留，`source` 对消费方无用——`breather._on_notify_skill_hook` 据 `tool_name` 匹配 `notify_skill`，不读 `source`；类型化派发按 `event_type` 走，也不读该字段）。`hook_type` 字段已移除（session_start/session_end 升级为独立 EventType 后判别职责由类型本身承担）。

#### 1.3.2 三路 driver 统一产出

| 产出点 | `TOOL_USE` | `TOOL_RESULT` | `TOOL_CALL` | 备注 |
|--------|:---:|:---:|:---:|------|
| `claude_sdk._sdk_to_events` | ✓ 新增 | ✓ 新增 | ✓ 已有 | 从 `AssistantMessage` 的 `ToolUseBlock` / `ToolResultBlock` 配对发独立事件 |
| `claude_cli._parse_assistant` | ✓ 新增 | ✓ 新增 | ✓ 已有 | 同上，从 `message.content` blocks 配对 |
| `opencode._parse_tool` | ✓ 已有 | ✓ 已有（补 `tool_name`） | ✓ 已有 | `TOOL_RESULT.data` 补 `tool_name`（取 `part.tool`） |
| `HookServer._handle_post_tool_use` | — | ✓ 已有（shape 收敛） | ✓ 已有 | post-tool-use 发 `TOOL_RESULT` + `TOOL_CALL` |
| `session_api` 重放（:655/:702、:1006/:1017） | ✓ 已有（shape 收敛） | ✓ 已有（补 `tool_name`+`is_error`） | — 待补 | 重放路径补发 `TOOL_CALL`，统一 shape |

#### 1.3.3 时序语义：观察 vs 控制

`TOOL_USE` 作为**观察事件**统一是 **post-hoc**（"已调用"），由三路 driver 在拿到完整 `AssistantMessage`（SDK/CLI）或合并 part（opencode）后发出。

`HookServer._handle_pre_tool_use` 当前在工具执行**前**发一条 `TOOL_USE`——这是**控制决策点**（可 deny），语义是"即将调用"，与观察事件冲突。目标：pre-tool-use 决策改走 `pre_tool_use` **控制事件**，不再发 `TOOL_USE` 观察事件。过渡期保留现状但标注为已知偏差（§7.6）。

#### 1.3.4 与 `ASSISTANT_MESSAGE` 内嵌 block 的关系

SDK/CLI 的 `ASSISTANT_MESSAGE.data.content` **只保留 `text` / `thinking` 块**——`tool_use` / `tool_result` 块已剥离到独立 `TOOL_USE` / `TOOL_RESULT` 事件（与 opencode `_parse_text` 同形：opencode 本来就只有 text 块）。三路 driver 同形，工具调用信息统一走独立事件。

- **独立事件** = 工具调用信息的唯一通道，供 `breather._on_notify_skill_hook`（查 `TOOL_RESULT.tool_name`）、`feedback_evaluator._build_event_summary`（读 `TOOL_USE.data.{name, input}` 抽工具列表/file_path）、`question_payload.extract_questions`（读 `TOOL_USE.data.{name, input}` 抽 `AskUserQuestion`）、未来插件按事件流消费。
- **`ASSISTANT_MESSAGE.content`** = LLM 文本输出 + thinking，供 loop 疑问检测、reviewer、前端展示。**不再嵌入工具块**。

> 已知缺口：opencode 的 `ASSISTANT_MESSAGE` 只含 text block（`_parse_text`），**没有 thinking 块**——opencode stream-json 把 thinking 放在 driver 未捕获的流里。三路 driver 在 text/tool 维度已同形，thinking 维度的对齐属后续工作。

## 2. EventBus — 事件总线

`src/workflow_controller/event/event_bus.py:51`

EventBus 是事件系统的枢纽：所有事件经它持久化 + 派发。它有**两条正交通道**——
**拦截器（interceptor）**处理控制事件、**观察者（observer）**处理观察事件，取代了
旧的 EventBridge「单队列 + catch-all subscribe」模型。

### 2.1 架构

EventBus 内部是 **拦截器链（inline）+ 观察者队列（async）** 的双通道发布-订阅：

**设计要点**：

- **两条通道、职责分离**：
  - **拦截器通道**：控制事件（`stop_requested`/`pre_tool_use`/`iteration_boundary`/`gate_question`/`session_delete`/`SUBAGENT_START`）走 `emit()` 时的 **inline 链**，按 priority 降序逐个 await，首个返回非 `pass` 的 `InterceptResult` 即短路，`emit()` 把它返回给调用方。用于"可否决"的决策点，**同步阻塞调用方**——决策结果要回流给 driver/loop（如 stop hook 的 allow/deny）。
  - **观察者通道**：观察事件（TOOL_RESULT/SUBAGENT_{START,PROGRESS,STOP}/ITERATION_START/RATE_LIMIT/LOG_* 等）走 `asyncio.Queue` + 单后台 task `_process_observers` **串行派发**。观察者只读不决、不影响控制流，故**异步、不阻塞 emit**。
- **`emit()` 的三步**（`event_bus.py:98`）：(1) `store_router.store` 持久化 → (2) `_run_interceptors` 跑拦截器链（inline，可能返回 `InterceptResult`）→ (3) 事件入 `_observer_queue`。故观察者拿到的事件一定已落盘且拦截器已跑完。
- **类型化订阅**：`observe(event_type, handler)` 按 event_type 字符串键登记（`EventType` 取 `.value`，控制事件直接用字符串）；**无 catch-all `subscribe`**——需要全量订阅用 `observe_all(handler)`（`"*"` 键）。旧 EventBridge 的 `subscribe(callback)` 已移除。
- **拦截器 priority**：`intercept(event_type, handler, *, priority=0)`，priority 大者先跑。插件拦截器 priority=10、reviewer 拦截器 priority=0——插件优先于 reviewer。
- **无 unsubscribe**：`observe`/`intercept` 只 append，无移除 API。插件卸载后回调留在列表里，但回调内部检查 `_orchestrator is None` 后早返回，不会崩溃。
- **不 flush 观察者队列**：`stop()` 直接 cancel `_process_observers` task，队列里残留事件丢弃。

```
                    ┌──────────────────────────────────────────────┐
  emit(event) ──────>│  EventBus                                     │
                     │  1. store_router.store (持久化, inline)        │
                     │  2. _run_interceptors (inline, 返回 InterceptResult)│
                     │  3. _observer_queue.put (asyncio.Queue, 无界) │
                     └──────────────────────────────────────────────┘
            inline ↑              │  async ↓
                  │                ▼
          InterceptResult    _process_observers (单 task 串行)
          (回流调用方)         for cb in observers[type]: await cb(event)
                              for cb in observers["*"]:     await cb(event)
                                │      │      │      │
                                ▼      ▼      ▼      ▼
                          _track_  watch-  breather  Limit
                          session   dog    _on_      Detector
                          (ITER_    (SUB_  context   (RATE_
                          START)    AGENT) _event    LIMIT)
                                    +      +_on_
                                    breather notify_
                                    _on_     skill
                                    context  _hook
                                    _event
```

**共 5 个观察者**（拦截器不计入），在 `orchestrator.start()`（:427-429）里注册前 3 个，breather 在插件 `_register_observers` 里自己注册第 4/5 个。每个观察者只注册自己关心的 type，不相关的事件根本不被遍历（类型化派发），不像旧 `subscribe` 那样每个事件都调一遍再早返回。

> 插件的观察者 hook 不再由 orchestrator 集中遍历 `_plugin_registry` 派发——基类也没有 `on_post_tool_use`/`on_session_start` 等观察方法。插件在 `_register_observers`（由 `install` 默认 body 派发）里写自己的 async handler 并 `ctx.bus.observe(event_type, handler)` 做类型化订阅，与拦截器的 `_register_interceptors` 对称。详见 [插件系统架构](ARCHITECTURE.md#plugin-系统)。

**观察者各自做什么**：

| # | 观察者 | 注册位置 | 关心的事件 | 作用 |
|---|--------|---------|-----------|------|
| 1 | `_register_session` | orchestrator.py:428（`bus.observe(ITERATION_START, ...)`） | `ITERATION_START` | 从 data 抽 SDK/CLI session id，建"SDK session id → Workflow Controller session id"映射，写到 session 对象供 resume 用。跨 driver（claude_cli / opencode）追踪子进程 session 的关键。 |
| 2 | `watchdog.on_start` / `on_progress` / `on_stop` | `watchdog.start()`（`bus.observe(SUBAGENT_{START,PROGRESS,STOP})`，orchestrator.py:429 调） | `SUBAGENT_START` / `SUBAGENT_PROGRESS` / `SUBAGENT_STOP` | subagent 启动建账 / 运行中与结束时做规则检测，命中条件发 `fault.<rule>` 告警（检测规则即订阅者）。详见 [watchdog 设计文档](watchdog.md)。 |
| 3 | `LimitDetector._handle_signal` | detector `__init__`（`bus.observe(RATE_LIMIT, ...)`，orchestrator.py:431 构造） | `RATE_LIMIT`（单路径） | 读 `limit_signal.kind` 单点派发：rejected→on_rate_limit、auth_error→on_limit_detected、warning→on_quota_warning、allowed/unknown→no-op。driver 负责把上游错误翻成 signal，detector 不再做 driver 专属解析，ERROR 不消费。 |
| 4 | `breather._on_context_event` | breather `_register_observers`（`bus.observe(SUBAGENT_STOP)`） | `SUBAGENT_STOP` | subagent 返回点是检查主 worker context 占用的时机，超阈值则 arm handoff。**插件自己 observe，不走 orchestrator 统一注册**。 |
| 5 | `breather._on_notify_skill_hook` | breather `_register_observers`（`bus.observe(TOOL_RESULT)`，仅 `notify_skill` 配置时） | `TOOL_RESULT` | 查 `data.tool_name` 匹配 `notify_skill`；工具事件对齐后（§1.3）三路 driver + HookServer 均命中。 |

**拦截器（inline，控制事件）**：

| event_type | 注册位置 | priority | 作用 |
|------------|---------|----------|------|
| `stop_requested` | reviewer `_register_reviewer_interceptors`（:1280）+ 插件（breather） | 0 / 插件 10 | stop hook 决策：插件先跑、reviewer 兜底，返回 InterceptResult(block/stop/let_exit) |
| `pre_tool_use` | reviewer（:1285）+ 插件 | 0 / 插件 10 | AskUserQuestion 拦截决策 |
| `iteration_boundary` | reviewer（:1290）+ 插件（staged_pipeline/breather） | 0 / 插件 10 | loop 边界门禁 |
| `gate_question` | reviewer（:1295） | 0 | opencode MCP gate 问答：auto→reviewer AUQ、manual→人工队列 |
| `session_delete` | 插件（agent_traceback） | 插件 10 | inline sync 清理（rmtree fork_root 等） |
| `SUBAGENT_START` | 插件（agent_traceback） | 插件 10 | 冻文件态快照、阻塞串行 |

**为什么拦截器与两类观察者采用不同投递方式**：拦截器要回流决策结果给调用方（`_decide_stop` 拿 InterceptResult 决定 allow/deny 并回 driver），因此 inline 执行并允许短路。普通 `observe` 观察者只读、不影响控制流，放后台队列让 `emit()` 尽快返回，避免 worker loop 被慢观察者拖住；`observe_awaited` 也只读、没有控制权，但用于发布者返回前必须完成的副作用，因此在 interceptor 之后 inline 串行执行。两类 observer 都先执行事件类型对应的 handler，再执行 `"*"` 通配 handler。持久化先于所有派发，观察者不需要自己再查库确认事件已落盘。普通观察者的代价是有队列调度延迟（通常 <1ms）；awaited observer 的代价是其耗时直接增加到 `emit()` 延迟。

### 2.2 核心方法

| 方法 | 行 | 作用 |
|------|----|------|
| `emit(event)` | — | **主入口**：store 持久化 → 跑拦截器链 → 跑 awaited observer → 入普通观察者队列；返回 `InterceptResult \| None`（仅控制事件非空） |
| `observe(event_type, handler)` | — | 普通观察者类型化订阅：append 到 `_observers[type_key]` |
| `observe_all(handler)` | — | 普通观察者通配订阅：append 到 `_observers["*"]`，收到所有事件 |
| `observe_awaited(event_type, handler)` | — | awaited observer 类型化订阅：append 到 `_awaited_observers[type_key]` |
| `observe_all_awaited(handler)` | — | awaited observer 通配订阅：append 到 `_awaited_observers["*"]`，在 `emit()` 返回前收到所有事件 |
| `detach_observer(event_type, handler)` | — | 移除指定类型（包括 `"*"`）的普通观察者 |
| `detach_awaited_observer(event_type, handler)` | — | 移除指定类型（包括 `"*"`）的 awaited observer |
| `intercept(event_type, handler, *, priority=0)` | — | 拦截器订阅：append 到 `_interceptors[type_key]` 并按 priority 降序排序 |
| `detach_interceptor(event_type, handler)` | — | 移除指定类型的拦截器 |
| `register_type(td)` | — | 注册自定义事件类型到 `EventTypeRegistry`（内置类型已预注册） |
| `start()` | — | 创建 `_process_observers` task（仅普通观察者派发） |
| `stop()` | — | cancel processor task——**不 flush 队列，残留普通观察者事件丢弃** |

### 2.3 拦截器链（inline，控制事件）

`event_bus.py:104`

```python
chain = self._interceptors.get(self._type_key(event.event_type))
if not chain:
    return None
for _priority, handler in chain:          # priority 降序
    try:
        r = await handler(event)
    except Exception:
        logger.exception(...); continue
    if r is not None and r.action != "pass":
        return r                          # 首个非 pass 即短路
return None
```

拦截器异常被 log + `continue`（不短路、不中断链），避免一个坏插件让控制事件永远 fall-through。`action="pass"` 表示"弃权"，交给链中下一个。`emit()` 把短路结果回流给调用方（`_decide_stop` / `_decide_pre_tool_use` 等据此决策）。

### 2.4 观察者派发循环（async，观察事件）

`event_bus.py:118`

```python
while True:
    event = await self._observer_queue.get()
    key = self._type_key(event.event_type)
    for handler in self._observers.get(key, []):     # 类型化
        try:
            await handler(event)
        except Exception:
            logger.exception(...)
    for handler in self._observers.get("*", []):      # 通配
        try:
            await handler(event)
        except Exception:
            logger.exception(...)
```

**顺序保证**：`emit()` 里持久化 + 拦截器先于 `queue.put`，故观察者拿到的事件一定已落盘且拦截器已跑完。同 type 观察者按注册顺序串行 await，不是并发；异常被 log + 吞，不中断后续。**持久化不在这一环**——`emit()` 已 inline 存过，观察者环只派发。

**串行阻塞**：单 task，一个慢观察者会阻塞后续事件（含其他观察者的事件）。内置观察者（`_register_session`、watchdog、breather、LimitDetector）都是纯内存操作，不会慢；自定义插件需注意。

## 3. 持久化 — EventStore + EventStoreRouter

### 3.1 events 表

`src/workflow_controller/store/event_store.py:27`

```sql
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    iteration INTEGER NOT NULL,
    timestamp TEXT NOT NULL,      -- ISO 字符串
    event_type TEXT NOT NULL,     -- .value 字符串（如 "tool_use"）
    data TEXT NOT NULL,            -- JSON 字符串
    created_at TEXT DEFAULT (datetime('now'))
)
```

- **`event_type` 列存 `.value` 字符串**，不是 enum 对象。
- **`data` 列存 JSON 字符串**，`json.dumps(event.data, default=str)`。
- **唯一索引**：`(session_id, iteration)`。**没有 timestamp/event_type 索引**——按 timestamp 查全表扫。
- PRAGMA：`journal_mode=WAL`，`busy_timeout=5000`。
- 每次 `_write_event` 开新 `sqlite3.connect`，**无连接池**。async 方法用 `asyncio.Lock` 串行化。

### 3.2 多 db 路由

`src/workflow_controller/store/event_router.py`

**为什么多 db**：per-project 隔离——事件落在 session 自己目录（`{cwd}/.workflow-controller/events.db`），换 project 不丢历史、breather handoff 能读、重启不清全局。

**路由依据**：仅 `session.cwd`，与 session_id 无关。

`_resolve_db_path`（event_router.py:55）三级回退：
1. pool 内存查 session.cwd
2. 扫所有已知库的 sessions 表找 config_json 解析 cwd
3. default 库（`{default_project_dir}/.workflow-controller/events.db`）

**跨库聚合读**（`get_events_since`，event_router.py:164）：
- `session_id` 非空 → 只查该 session 所在库（定位准确）
- `session_id` 为空 → 遍历所有已知库，按 event.id 去重，按 timestamp 排序
- **游标是 timestamp 高水位字符串**（非 rowid——多库 rowid 空间独立，不全局有效）

**session 元数据不分库**：`sessions`/`evaluations`/`loop_results`/`handoff_relations` 表全在 default 库。只有 `events` 表 per-project。

### 3.3 EventAPI

`src/workflow_controller/api/event_api.py:38`

纯代理 EventStoreRouter，提供两个方法供 SSE 和 Web API（控制台侧）调用：

- `get_since(last_ts, session_id="")` → `(new_ts, list[EventInfo])`：timestamp 高水位增量查询
- `get_recent(limit=50)` → `list[EventInfo]`：跨库最近 N 条

`EventInfo`（event_api.py:15）是 Event 的扁平化 dataclass，`event_type` 是字符串（非 enum），`rowid` 字段保留但始终为 0（多 db 路由后无意义）。

## 4. 事件来源 — 3 条路径

### 4.1 路径 A：Claude Code HTTP HookServer

`src/workflow_controller/hooks/server.py`

8 个 hook 路由（默认 port 18765，可用 `--hook-port` 指定），handler → event 映射：

| HTTP 路由 | 发 event_type | 备注 |
|-----------|---------------|------|
| `POST /hooks/post-tool-use` | `TOOL_RESULT` + `TOOL_CALL` | 一个请求发两条 |
| `POST /hooks/pre-tool-use` | `TOOL_USE` | + 调 `on_pre_tool_use_request` 决策 |
| `POST /hooks/stop` | **不发 event** | 只调 `on_stop_request` 决策 |
| `POST /hooks/notification` | `NOTIFICATION` | 纯展示性系统消息（残余桶） |
| `POST /hooks/session-start` | `ITERATION_START` | driver 层 iteration 启动；`_register_session` 类型化 observe 此类型。HTTP 路径名保留（Claude Code hook 协议规定），但 emit 的 event_type 是 `iteration_start` |
| `POST /hooks/session-end` | `ITERATION_END` | driver 层 iteration 结束；与编排层 `session_complete` 区分。HTTP 路径名保留，emit 的 event_type 是 `iteration_end` |
| `POST /hooks/subagent-start` | **不发 event** | 只调 `on_subagent_start_request` |
| `POST /hooks/subagent-stop` | **不发 event** | 只调 `on_subagent_stop_request` |

handler 通过 `on_event` 回调（orchestrator 注入的 `HookPayloadAdapter(self.bus)`，见 `hooks/adapter.py`）→ `Event.create` → `bus.emit`。

> **`hook_type` 字段已移除**：iteration_start / iteration_end 升级为独立 EventType 后，
> 判别职责由类型本身承担，`server.py:_emit_event` 不再注入 `hook_type` 字符串字段。
> 旧 row 若含 `hook_type`，读侧兼容查询用 `json_extract(data,'$.hook_type')`。

### 4.2 路径 B：opencode driver

`src/workflow_controller/driver/opencode.py`

两条子路径：

**(1) stdout JSON 行 → Event**（`_opencode_line_to_event`，:592）

opencode 子进程 stdout 按 `\n` 分行读（65536 字节 chunk buffer），每行 JSON 按_msg_type 分发：

| msg_type | 产出 | 备注 |
|----------|------|------|
| `text` | `ASSISTANT_MESSAGE` | |
| `tool_use` | `TOOL_USE` + `TOOL_RESULT` + `TOOL_CALL` | opencode 把 call+result 合并，Workflow Controller 拆三条 |
| `step_finish` | `STEP_FINISH` | init NOTIFICATION 派生已移除——ITERATION_START 改在 driver 首次拿到 session id 时（`id_ready.set()`）emit，关闭第一轮崩溃导致 resume 不可用的窗口 |
| `error` | `ERROR` | |
| `step_start`/其他 | 丢弃 | |

> **ITERATION_START / ITERATION_END 在 opencode driver 的位置**：
> - ITERATION_START：在 stdout 主循环里 driver **首次**通过 `_extract_line_session_id` 抓到
>   `main_oc_id` 时（`opencode.py:id_ready.set()`）emit 一条，带 `session_id`/`subtype=init`。
>   这关闭了原 parity 文档 §5 描述的「driver 内部早有 id 对外晚发」窗口——resume 在第一轮
>   崩溃时不再失效。
> - ITERATION_END：OpenCode、Claude CLI 和 Claude SDK 的 query 正常结束时各 emit 一次；
>   出错时先 emit ERROR，再 emit 一次 `ITERATION_END`（`reason=internal_error`），
>   包括启动失败以及由 session 层接住的 driver 异常。取消 query 不补发结束事件。
>   session 层捕获异常并停止 loop 时，loop 仍须把该路径的 ERROR 和 ITERATION_END
>   转发到事件总线，供前端消费。
>   结束事件不能放在 async generator 的 `finally` 中：消费者调用 `aclose()` 时，
>   `finally` 里的 `yield` 会抛 RuntimeError；清理代码仍放在 `finally`。

**(2) db poller → progress 事件**（`_poll_subagent_stream`，:443）

后台 task，每 `_SUBAGENT_POLL_INTERVAL=0.4` 秒轮询 opencode 的 session db：
- 发现新 sub-session → 调 `on_subagent_start`
- diff `part.id` 有新 messages → 调 `on_subagent_progress`

> **注意**：poller 400ms 轮询 ≠ progress 事件每 400ms 一条。opencode 按 LLM 流式输出块边界
> 写 part（text block / thinking block / tool_use block 等），subagent 思考期间没新 part
> 写入，poller 没东西可推。所以 progress 间隔 = subagent 产出 block 的间隔，可能几十秒
> 甚至更长。**不能依赖 progress 做心跳**。

**(3) stdout task 行 → subagent stop**（`_dispatch_hook`，:522）

opencode 的 task 工具在一个已完成 tool_use 事件里（call + result 合并），`_dispatch_hook` 解析后调 `on_subagent_stop`（带 content、total_tokens、messages）。

### 4.3 路径 C：orchestrator 自发

`src/workflow_controller/core/orchestrator.py`

| 场景 | event_type | 行 |
|------|-----------|-----|
| session 启动 | `LOG_AUTO`（`action=session_started`）| 574 |
| subagent start | `SUBAGENT_START` | 872 |
| subagent progress | `SUBAGENT_PROGRESS` | 889 |
| subagent stop | `SUBAGENT_STOP` | 917 |
| hook 决策审计 | `LOG_AUTO` | 1865（11 个调用点）|

`SubagentTracker.on_start` / `on_progress` / `on_stop` 是 driver hooks 的接收端——driver（opencode 或 claude_sdk/cli）通过 `DriverHooks` 回调这些方法，它们再 `bus.emit` 发对应类型的 SUBAGENT_* 事件。

## 5. 事件消费者 — EventBus 观察者

### 5.1 orchestrator 内置观察者

在 `orchestrator.start()`（:423）注册：

| 观察者 | 关心的事件 | 作用 |
|--------|-----------|------|
| `_register_session` | `ITERATION_START`（类型化 observe，非 observe_all+内部过滤）| 从 data 抽 SDK/CLI session id，建"SDK session id → Workflow Controller session id"映射，存到 session 对象 |
| `watchdog.on_start` / `on_progress` / `on_stop` | `SUBAGENT_START` / `SUBAGENT_PROGRESS` / `SUBAGENT_STOP` | subagent 超时检测 |
| `LimitDetector._handle_signal` | `RATE_LIMIT`（单路径） | auth/限流检测：读 `limit_signal.kind` 派发 rotator 回调（ERROR 不消费）|

> 插件观察者 hook 不再由 orchestrator 集中派发，基类也无 `on_*` 观察方法——插件在 `_register_observers`（由 `install` 默认 body 派发）里写自己的 async handler 并 `ctx.bus.observe(event_type, handler)` 自注册，与拦截器的 `_register_interceptors` 对称，orchestrator 不再遍历 `_plugin_registry`。

### 5.2 插件观察者

- **breather**（`plugin.py`）：`_on_context_event` 只关心 `SUBAGENT_STOP`——subagent 返回点是 handoff 检查时机。在 `_register_observers` 里通过 `bus.observe(SUBAGENT_STOP)` 注册。
- **breather**（`plugin.py`）：`_on_notify_skill_hook` 查 `TOOL_RESULT` 的 `tool_name` 匹配 `notify_skill`；工具事件对齐后（§1.3）三路 driver + HookServer 均命中（此前仅 CLI/HTTP hook）。在 `_register_observers` 里通过 `bus.observe(TOOL_RESULT)` 注册（`notify_skill` 配置时）。
- **agent_traceback / staged_pipeline**：不 observe bus；拦截器/观察者一律在 `_register_interceptors` / `_register_observers`（由 `install` 默认 body 派发）里 `ctx.bus.intercept(...)` / `ctx.bus.observe(...)` 自注册到 `EventBus`。

### 5.3 SSE 推送

`web/routes/sse_routes.py`（控制台侧）

SSE 不走 EventBus 观察者，而是 250ms 轮询 `api.event.get_since(last_event_ts)`：

1. **首次连接回放**：`api.event.get_recent(50)` 跨库最近 50 条
2. **增量推送**：每 250ms 一轮，`get_since(last_event_ts)` 取 timestamp 高水位后的新事件，推 `claude_event` SSE 事件
3. **每轮还推**：`session_update`/`pipeline_update`/`usage_update`/`config_update`/`auth_update`/插件 SSE/`handoff_update`/`loop_update` 快照

### 5.4 前端消费

前端 `composables/useSSE.ts`（控制台侧）

```typescript
eventSource.addEventListener('claude_event', (e) => {
  const ev = JSON.parse(e.data)
  if (isNoiseEvent(ev)) return          // 过滤 notification + subtype=thinking_tokens
  events.value.push(ev)                  // O(1) push
  if (events.value.length > MAX_EVENTS)  // 环形，上限 2000
    events.value.splice(0, events.value.length - MAX_EVENTS)
})
```

- **MAX_EVENTS = 2000**：防无限增长，超出丢最旧。
- **isNoiseEvent**：只过滤 `notification + subtype=thinking_tokens`（高频遥测）。
- 重连：`onerror` → close + 2 秒后重连。

## 6. 完整数据流

以 opencode 一个 tool_use 事件为例：

```
opencode 子进程 stdout: {"type":"tool_use","part":{...}}\n
        │
        ▼
OpenCodeDriver.query (opencode.py:189)
  读 65536 chunk, 按 \n 切行
  _opencode_line_to_event → _parse_tool
  产出 [TOOL_USE, TOOL_RESULT, TOOL_CALL] 三个 Event, yield
        │
        ▼
SingleLoopEngine._run_iteration (loop.py:362)
  async for event in session.query(...):
    bus.emit(event)          ← 内联于 _run_iteration; EventBus.emit:
         1. store_router.store (INSERT OR REPLACE; 入库 {cwd}/.workflow-controller/events.db, 单次)
         2. _run_interceptors (TOOL_USE 无拦截器 → None)
         3. _observer_queue.put
        │
        ▼ (后台单 task, async)
EventBus._process_observers (event_bus.py:118)
  key = "tool_result"
  for cb in observers["tool_result"]:        # 类型化, 只调注册过该 type 的
       breather._on_notify_skill_hook → 匹配 data.tool_name
  (observers["iteration_start"]/["subagent_start"]/["subagent_progress"]/["subagent_stop"]/["rate_limit_event"] 不被遍历)
        │
        ▼ (并行, 250ms 轮询)
SSE event_stream (sse_routes.py:48)
  api.event.get_since(last_event_ts)
  → event_router.get_events_since (跨库聚合)
  → yield {"event":"claude_event", "data": json.dumps(event.__dict__)}
        │
        ▼
前端 useSSE.ts:63
  JSON.parse → isNoiseEvent 过滤 → events.value.push → Vue 响应式
  → VirtualLog / SessionDetailView / WatchdogAlerts 渲染
```

## 7. 已知问题与注意事项

1. **EventBus 无 unsubscribe**：`observe`/`intercept` 只 append，无移除 API。插件卸载后回调留在列表里，但回调内部检查 `_orchestrator is None` 后早返回，不会崩溃。
2. **观察者串行阻塞**：`_process_observers` 单 task，同 type 观察者按注册顺序串行 await，一个慢观察者会阻塞后续事件（含其他观察者的事件）。watchdog/breather 的 `on_event` 都是纯内存操作，不会慢；自定义插件需注意。拦截器通道 inline 阻塞调用方，但控制事件频率低、链短。
 3. **入库路径**：`_run_iteration` 内联 `bus.emit`——而 `bus.emit` 内部第一步就是 `store_router.store`，且 bus 的 `store_router` 就是 loop 的 `event_router`（`orchestrator.py:110-111`），故 driver 事件单次入库。`SingleLoopEngine.bus` 为必填（非 `None`）——`None` 默认值与 `_store_event` 包装方法都是已移除的 per-session `LoopEngine` API 的死脚手架，已删。orchestrator 自发 / HookServer 路径只经 `bus.emit` 一次，同样不重复。> 历史上 `_store_event` 曾先 `event_router.store` 再 `bus.emit`，导致同 id 事件 `INSERT OR REPLACE` 写两次（幂等覆盖、轻微开销），已修正为单写。
4. **events 表无 timestamp 索引**：`get_events_since_ts` 全表扫。大库可能慢。
5. **progress 不规律**：opencode 按 block 边界写 part，subagent 思考期间无 progress。不能依赖 progress 做心跳，超时检测需在 stop 时兜底或用定时器。
6. **工具事件曾不对齐**（见 §1.3）：三路 driver（`claude_sdk._sdk_to_events` / `claude_cli._parse_assistant` / `opencode._parse_tool`）已按 §1.3.2 统一产出 `TOOL_USE` + `TOOL_RESULT` + `TOOL_CALL` 三个独立事件，data shape 见 §1.3.1（`TOOL_RESULT.data.tool_name` 必填，三路均从配对的 `TOOL_USE.name` / opencode 的 `part.tool` 回填）。§1.3.4 peel 已落地：SDK/CLI 的 `ASSISTANT_MESSAGE.content` 剥离 `tool_use`/`tool_result` 块，只保留 `text`/`thinking`，与 opencode 同形；消费方（`feedback_evaluator._build_event_summary` / `question_payload.extract_questions`）已迁到读独立 `TOOL_USE` 事件。`RESULT` 事件已重命名为 `STEP_FINISH`（值 `step_finish`，旧 `"result"` 字符串读侧仍兼容映射）。**仍待补**：`HookServer._handle_post_tool_use` 的 `TOOL_RESULT.data` 仍混 `source`/原始 payload（需收敛为 §1.3.1 shape；`hook_type` 已移除）；`session_api` 重放路径仍缺 `TOOL_CALL`；`HookServer._handle_pre_tool_use` 的 `TOOL_USE` 时序在前（§1.3.3 控制事件迁移未落地，过渡期保留为已知偏差）；opencode 的 `ASSISTANT_MESSAGE` 无 `thinking` 块（stream 未捕获）。
