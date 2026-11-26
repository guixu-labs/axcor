# Watchdog 设计文档

> 面向 Workflow Controller 开发者。本文描述 watchdog 的已实现部分（subagent 超时、文件不变、关键词、节点累计超时）和扩展指南。

## 1. 定位

Watchdog 是 Workflow Controller 的**通用能力**（不是 plugin），用于实时检测运行中 session 的异常并通知用户。

- **代码位置**：`src/workflow_controller/watchdog/`（与 `core/`、`plugins/` 并列）
- **配置位置**：`workflow-controller-config.yaml` 的 `watchdog` 字段（`AppConfig.watchdog: WatchdogConfig`），全局配置非 plugin 配置；节点累计超时的时限属工作流级，住工作流 YAML 的 `timeouts` 段（见 §2.4）
- **不是 plugin**：不实现 Plugin ABC，不被 plugin registry 管理，不参与 plugin 生命周期

## 2. 已实现规则：subagent 超时

### 2.1 规则定义

单个 subagent 运行时间超过配置阈值 → 发 WARNING 告警。

- **触发时机**：SUBAGENT_* 事件回调（PROGRESS 和 STOP 时检查）+ 周期巡检（见 §2.2）
- **判断逻辑**：`elapsed = now - started_at`，超过 `subagent_timeout_seconds` 即告警
- **去重**：同一 call（session_id + agent_id）对同一规则只发一次（`CallRecord.fired_alerts` 集合）
- **报告内容**：agent_type、agent_id、run_id、node_id、started_at、elapsed_seconds、threshold_seconds、人类可读 message（run/node 取自事件打戳：session 事件写库前按归属缓存补 run 维度，graph 成员 context 形如 `node=<id>/execute`，可得范围内补齐，不可得为空）

### 2.2 为什么不能只靠 progress 检测

opencode 的 subagent progress 事件来自 db poller（每 400ms 轮询 opencode session db 的 parts 表），但 **progress 事件不规律**：

- poller 400ms 轮询一次，但 opencode 按 LLM 流式输出块边界（text block / thinking block / tool_use block）写 part
- subagent 思考期间没新 part 写入 → poller 没东西可推 → 不发 progress
- progress 间隔 = subagent 产出 block 的间隔，可能几十秒甚至更长

实测数据（`examples/opencode-subagent` 的一个 subagent）：
```
start@0s → progress@0s → progress@1.4s → [22秒空档] → progress@22s → stop@22s
```

所以：
- **progress 检查**：有机会在运行中实时告警（但可能错过，如果 progress 长时间不来）
- **stop 检查**：兜底，subagent 结束时一定能检查到实际 elapsed
- **周期巡检**：`WatchdogCore` 启动后持有单一巡检任务（默认 15s 一拍），逐规则调 `sweep()`——完全不依赖事件的到期检查（静默卡死的 subagent 与无事件的节点都按时告警）。巡检时先做终态检查：父 session 已终态（completed/stopped/failed）或已不在池内的 call 记录直接清掉，不再告警

三者互补，同一 call 去重保证不重复发。

### 2.3 stop 后迟到的 progress

opencode 的 poller 是独立 task，subagent stop 后 poller 可能还在推最后一批 parts → stop 之后才到 progress。

**处理方式**：`_on_stop` 已经从 `_active_calls` 清掉 rec，后续 progress 找不到 rec → 直接忽略。不兜底建账（否则用 progress.timestamp 当 started_at，elapsed 永远≈0，测不到超时）。

## 3. 实现结构

### 3.1 文件清单

| 文件 | 作用 |
|------|------|
| `src/workflow_controller/watchdog/__init__.py` | 包标记 |
| `src/workflow_controller/watchdog/state.py` | 数据结构：Alert / Severity / CallRecord / NodeClock / make_alert_id |
| `src/workflow_controller/watchdog/fault.py` | `fault.*` 告警事件编解码（类型词表 + payload 解码，检测与消费共用） |
| `src/workflow_controller/watchdog/core.py` | 规则订阅者（四条检测规则）+ WatchdogCore 组合根（含周期巡检任务） |
| `src/workflow_controller/watchdog/ledger.py` | 节点累计时钟账本（`watchdog_node_clocks` 表读写面；建表归 run 记录库统一迁移） |
| `src/workflow_controller/api/watchdog_api.py` | WatchdogAPI：告警查询公开门面（`CoreAPI.watchdog`，直读事件库） |
| `web/routes/watchdog_routes.py`（控制台侧） | web 端点：`GET /api/v1/sessions/{id}/watchdog/alerts` 查历史告警 |
| `src/workflow_controller/config/settings.py` | `WatchdogConfig`（在 AppConfig 上） |
| `src/workflow_controller/api/config_api.py` | `get_config_dict` / `update_config` 暴露 watchdog 字段 |
| `src/workflow_controller/core/orchestrator.py` | `__init__` 构造 `self.watchdog`（账本锚守护进程库 events.db），`start()` 里接入事件总线，`shutdown()` 停巡检 |
| `src/workflow_controller/core/run_coordinator.py` | 驱动 `EmitEvent` 动作派发时把 run 键事件同流总线分发（见 §3.7） |
| 控制台路由聚合（控制台侧） | 领域路由统一注册（含告警查询路由） |

### 3.2 数据结构（state.py）

```python
class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

@dataclass
class Alert:
    alert_id: str           # alert_<uuid hex 12>
    rule: str                # "subagent_timeout"
    severity: Severity
    session_id: str
    message: str            # 人类可读
    agent_id: str = ""
    agent_type: str = ""
    run_id: str = ""        # 事件打戳携带的 run 归属（可得范围内）
    node_id: str = ""       # graph 成员 context（node=<id>/…）解析（可得范围内）
    started_at: str = ""    # ISO
    elapsed_seconds: int = 0
    threshold_seconds: int = 0
    timestamp: str = ""     # ISO，告警产生时刻

@dataclass
class CallRecord:
    session_id: str
    agent_id: str
    agent_type: str
    started_at: datetime    # SUBAGENT start 事件的 timestamp
    run_id: str = ""        # start 事件打戳快照
    node_id: str = ""       # start 事件 context 解析
    fired_alerts: set[str]  # 已触发的 rule 集合，去重用

@dataclass
class NodeClock:
    run_id: str             # 记账键：(run_id, node_id, gen)
    node_id: str
    gen: int                # 节点代次（回滚递增；新代重新计时）
    threshold_seconds: int  # 本节点时限（工作流 YAML 解析，随事件透传）
    accumulated_seconds: float  # 已累计用时（暂停/终态冻结）
    running_since: datetime | None  # 当前计时段起点；None = 冻结
    started_at: datetime    # 本代首次开始时刻（告警载荷）
    fired: bool             # 同代同调用只报一次
    attempt: int            # 最近 attempt 号（告警载荷）
    session_id: str         # 执行/验收 session（告警载荷，可得范围内）
    verify_session_id: str = ""
```

`Alert.to_dict()` / `Alert.from_dict()` 用于 fault.* 告警事件 data 字段序列化。

### 3.3 核心逻辑（core.py）

Watchdog 不依赖具体的 `Orchestrator`，而是按需接收独立参数——直接传入真正用到的对象。依赖单向：``orchestrator → watchdog``，watchdog 不反向 import / 类型引用 ``Orchestrator``。

检测规则是事件库订阅者：每条规则一个类，各自订阅所需事件、命中条件时经 ``watchdog/fault.py`` 编码发 ``fault.<rule>`` 告警事件。

```
WatchdogCore.__init__(config, bus, pool, db_path=None)   # 组合根：构造四条规则；db_path 给定时挂节点时钟账本
  SubagentTimeoutRule   订阅 SUBAGENT_{START,PROGRESS,STOP}（规则 1）
  FileStallRule         订阅 SUBAGENT_{START,STOP}（规则 2）
  KeywordRule           订阅 SUBAGENT_STOP（规则 3）
  NodeTimeoutRule       订阅 node_history / graph_paused / graph_{completed,failed}（规则 4）

WatchdogCore.start()  ← orchestrator.start() 调用
  if not config.enabled: return
  各规则向 bus.observe 注册回调，加载节点时钟账本存留行（开段冻结，见 §3.6），起周期巡检任务
  配置热更新靠每次回调现读 config

WatchdogCore.stop()   ← orchestrator.shutdown() 调用：停巡检任务、关账本连接

巡检循环（每 15s 一拍）
  for rule in rules: await rule.sweep(now)   # 不依赖事件的到期检查 + 节点开段落账

规则回调（以超时规则为例）
  on_start(event):    建 CallRecord(started_at=event.timestamp, run_id/node_id 取事件打戳与 context)
  on_progress(event): 查 rec, _check_timeout(event.timestamp)；rec 已清（stop 后迟到 progress）直接忽略
  on_stop(event):     查 rec, _check_timeout(event.timestamp), 清 rec

  sweep(now):         逐 active call：父 session 已终态/不在池内 → 清记录；否则 _check_timeout(now)

_check_timeout(rec, now)
  threshold = config.subagent_timeout_seconds
  if threshold <= 0: return          # 0 = 关闭
  if rule in rec.fired_alerts: return # 去重
  elapsed = (now - rec.started_at).total_seconds()
  if elapsed < threshold: return
  → 构造 Alert, emit fault.<rule> 事件（fault.py 编码）
  → rec.fired_alerts.add("subagent_timeout")
```

### 3.4 告警事件：fault.* 开放词表

告警事件类型是 ``fault.<rule>`` 开放词表（字符串事件类型，同 ``stop_requested`` 等内核字符串事件的落库方式），当前四条：

| event_type | 规则 | 寻址 |
|-----------|------|------|
| `fault.subagent_timeout` | subagent 运行超时 | session 键，payload 为 `Alert.to_dict()` |
| `fault.file_stall` | 连续 N 个 subagent 结束后追踪文件无变化 | session 键 |
| `fault.keyword_detected` | 追踪文件中出现关键词 | session 键 |
| `fault.node_timeout` | 节点本代累计超时 | **run 键**（节点无 session 身份），payload 为领域事实（run/节点/代次/attempt/session/已用时间/时限），非 Alert 序列化 |

事件按 session 寻址落 events 表，写库路径按归属缓存打 run 戳（run/session 双维度可查）；run 键事件（`fault.node_timeout`）落 run_events 表。两类都在 `GET /api/v1/runs/{id}/events` 合并流里可见（RSI 适配器轮询面）。前端按 `event_type` 前缀 `fault.` 过滤。

词表与编解码住 `watchdog/fault.py`：检测规则发事件、查询面解事件共用同一实现，检测本体不独占编码；新增规则只需给出规则名，事件类型随规则名生成。

### 3.5 告警持久化与查询

告警事件走 `bus.emit(Event.create(event_type="fault.<rule>", ...))`：
1. EventBus `emit` 先 `event_router.store` → 进 events 表持久化（打 run 戳）
2. 再分发给订阅者（SSE 不走订阅，但 250ms 轮询 `get_since` 会取到）
3. session 删除时 `delete_session` 清掉该 session 的所有 events（包括告警）

**历史告警查询**：`GET /api/v1/sessions/{session_id}/watchdog/alerts`
- 路由住控制台侧，处理器经 `CoreAPI.watchdog.list_alerts` 查询，不触碰内核内部对象
- `WatchdogAPI` 直读事件库（`get_events_sync`）筛 `fault.*` 事件并解码，不依赖检测本体与内存态
- session 已结束、进程重启、watchdog 被禁用——照样能查到历史告警

### 3.6 规则 4：节点累计超时（fault.node_timeout）

检测「本代节点从开始执行到验收结束」的累计耗时超过时限。节点是 graph 编排概念，watchdog 不 import graph——节点身份完全经事件载荷透传（泛化为「执行单元」事实）。

**事实源：run 键事件进总线分发面（§3.7）**。规则订阅四类驱动事件（字符串事件类型）：

| 事件 | 用途 |
|------|------|
| `node_history`（载荷含 node_id/gen/attempt/status/session/timeout_seconds） | 计时段迁移：`running` 起步/续跑，`stopped/passed/failed/skipped` 冻结；同节点出现更大 gen 时冻结旧代（回滚重新计时） |
| `graph_paused` | 该 run 全部节点时钟冻结（暂停不计时） |
| `graph_completed` / `graph_failed` | 该 run 全部节点时钟冻结（终态不再报） |

**时钟语义**（键 = run_id + node_id + gen）：

- 从本代节点首次 `running` 起累计，`running` 段计时、其余状态冻结；暂停（graph_paused / 节点 stopped）冻结，续跑（再次 `running`）不清零继续累计；
- 回滚产生新代次：新 `running` 事件携带更大 gen → 新键从零计时，旧代时钟冻结；
- 同代同调用只报一次（fired 标记持久化，重启不重报）；
- 告警后时钟冻结（ breach 已表达，后续累计无消费方）。

**时限配置（工作流 YAML 顶层 `timeouts` 段，graph 引擎解析后随 `node_history` 载荷透传 `timeout_seconds`）**：

```yaml
timeouts:
  node_seconds: 7200     # 整图默认节点累计时限；缺省或 0 = 该图不启用本规则
  nodes:                 # 单节点覆盖（0 = 该节点关闭）
    fix-findings: 5400
```

**告警载荷**（run 键、领域事实）：`run_id / node_id / gen / attempt / session_id / verify_session_id / started_at / elapsed_seconds / threshold_seconds`。

**持久化**：`watchdog_node_clocks` 表住守护进程库 events.db（建表归 run 记录库统一迁移阶梯管理），读写面 `watchdog/ledger.py`。每次状态迁移与巡检拍落账；**服务重启保留已用时间**——启动加载存留行，`running_since` 非空的行视为崩溃残留、按冻结收段（开段最多损失一个巡检周期，宕机时长不计入，对齐暂停语义）；run 删除时随 run_events 一并级联清理。

### 3.7 run 键事件进总线分发面

驱动的 `EmitEvent` 动作（node_history / graph_* 等编排事件）由 RunCoordinator 落 run_events 表后，把同一内容以 run 键临时事件（`bus.emit_ephemeral`，只分发不落库）分发进总线。watchdog 等内核订阅者由此观察节点/图生命周期；节点身份经事件数据透传，watchdog 与 graph 插件零依赖。

### 3.8 配置

`workflow-controller-config.yaml`：
```yaml
watchdog:
  enabled: true                    # 总开关
  subagent_timeout_seconds: 1800   # 超时阈值（秒），0 = 关闭该规则，默认 30 分钟
```

通过 `ConfigAPI.get_config_dict` / `update_config` 读写，前端 Settings 页可配。热更新：`WatchdogCore` 每次回调现读 `self._config`（直接引用 `orchestrator.config.watchdog`，update_config 改的是同一对象）。检测配置仅全局 workflow-controller-config.yaml 生效；告警触发的「上传现场到 Context Bay」能力属领域暂存模块，其配置（仓地址、切片目录）在独立的 `context_bay:` 段，不在 watchdog 段。

## 4. 前端展示

### 4.1 SessionDetailView 告警徽标

前端 SessionDetailView（控制台侧）

头部加一个三角形告警按钮 + NPopover 弹层：
- 无告警：按钮灰色
- 有告警：按钮变红 + 显示告警计数
- 点击 → NPopover 弹层显示 `WatchdogAlerts` 组件

### 4.2 WatchdogAlerts 组件

前端 WatchdogAlerts（控制台侧）

- 从 `sessionEvents` props 实时过滤 `fault.` 前缀事件（`event_type.startsWith('fault.')`）
- 按时间排序，新告警自动追加到列表底部
- 弹窗内 `overflow-y: auto` 滚动浏览所有告警
- 每条告警：Severity tag + rule + agent_type + message + elapsed/threshold/started_at

### 4.3 SettingsModal 配置

前端 SettingsModal（控制台侧）

全局设置加 "Watchdog" 卡片：
- 启用开关
- 超时阈值（秒，`n-input-number`，min=0，step=60）

### 4.4 SSE 透传

SSE（`sse_routes.py`）250ms 轮询 `api.event.get_since`，所有 events（含 `fault.*`）通过 `claude_event` SSE 事件推到前端。前端 `useSSE.ts` 的 `events` ref 自动收到，`WatchdogAlerts` 的 computed 自动响应。**无需改 SSE**。

## 5. 扩展指南

### 5.1 加新规则

watchdog 设计预留了多规则扩展。以"subagent 返回空结果"为例：

1. **core.py**：新增规则订阅者类（如 `EmptyResultRule`），类属性 `rule = "empty_result"`，订阅 SUBAGENT_STOP 并实现检查逻辑；同一 call 去重靠 `rec.fired_alerts.add("empty_result")`
2. **WatchdogCore**：规则清单加入新规则
3. **Alert**：复用现有结构，`rule` 字段区分，`severity` 可升级为 ERROR；事件类型随规则名生成（`fault.empty_result`），无需改编解码
4. **前端**：`WatchdogAlerts.vue` 的 `timelineType` / `severityTag` 已按 severity 分色，无需改

### 5.2 周期巡检（已实施）

「即使 progress 不来也能实时告警」由 `WatchdogCore` 的单一巡检任务承担（15s 一拍、逐规则 `sweep(now)`）：生命周期集中管理（start 起、shutdown 停），规则侧只实现无副作用的 `sweep`，不自持 task。子代理超时规则与节点累计超时规则都靠它兜底静默卡死场景；事件回调内的检查仍是机会性快路径。

## 6. fault.* 事件迁移（已实施）

watchdog 已完成事件库化改造，检测与消费解耦：

- **告警事件类型**：告警从 `LOG_HOOK`（已删除的预留枚举值）迁移为 `fault.*` 开放词表事件（`fault.subagent_timeout`、`fault.file_stall`、`fault.keyword_detected`），session 键落 events 表、写库路径按归属打 run 戳；
- **检测规则订阅者化**：三条规则各自成为事件库订阅者（独立订阅 subagent_* 事件、自行发 `fault.*`），告警事件编解码独立为 `watchdog/fault.py` 公共模块，检测本体不再独占编码；
- **消费解耦**：前端展示按 `fault.` 前缀过滤 SSE 透传事件，历史查询经 `CoreAPI.watchdog`（直读事件库解码），均不依赖检测实现；历史 `log_hook` 告警行为干净切换、不再展示。
