# Watchdog 设计文档

> 面向 Workflow Controller 开发者。本文描述 watchdog 的已实现部分（subagent 超时检测）和扩展指南。

## 1. 定位

Watchdog 是 Workflow Controller 的**通用能力**（不是 plugin），用于实时检测运行中 session 的异常并通知用户。

- **代码位置**：`src/workflow_controller/watchdog/`（与 `core/`、`plugins/` 并列）
- **配置位置**：`workflow-controller-config.yaml` 的 `watchdog` 字段（`AppConfig.watchdog: WatchdogConfig`），全局配置非 plugin 配置
- **不是 plugin**：不实现 Plugin ABC，不被 plugin registry 管理，不参与 plugin 生命周期

## 2. 已实现规则：subagent 超时

### 2.1 规则定义

单个 subagent 运行时间超过配置阈值 → 发 WARNING 告警。

- **触发时机**：SUBAGENT_* 事件回调（PROGRESS 和 STOP 时检查）
- **判断逻辑**：`elapsed = now - started_at`，超过 `subagent_timeout_seconds` 即告警
- **去重**：同一 call（session_id + agent_id）对同一规则只发一次（`CallRecord.fired_alerts` 集合）
- **报告内容**：agent_type、agent_id、started_at、elapsed_seconds、threshold_seconds、人类可读 message

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

两者互补，同一 call 去重保证不重复发。

### 2.3 stop 后迟到的 progress

opencode 的 poller 是独立 task，subagent stop 后 poller 可能还在推最后一批 parts → stop 之后才到 progress。

**处理方式**：`_on_stop` 已经从 `_active_calls` 清掉 rec，后续 progress 找不到 rec → 直接忽略。不兜底建账（否则用 progress.timestamp 当 started_at，elapsed 永远≈0，测不到超时）。

## 3. 实现结构

### 3.1 文件清单

| 文件 | 作用 |
|------|------|
| `src/workflow_controller/watchdog/__init__.py` | 包标记 |
| `src/workflow_controller/watchdog/state.py` | 数据结构：Alert / Severity / CallRecord / make_alert_id |
| `src/workflow_controller/watchdog/fault.py` | `fault.*` 告警事件编解码（类型词表 + payload 解码，检测与消费共用） |
| `src/workflow_controller/watchdog/core.py` | 规则订阅者（三条检测规则）+ WatchdogCore 组合根 |
| `src/workflow_controller/api/watchdog_api.py` | WatchdogAPI：告警查询公开门面（`CoreAPI.watchdog`，直读事件库） |
| `src/workflow_controller/api/http/routes/watchdog_routes.py` | HTTP 端点：`GET /api/v1/sessions/{id}/watchdog/alerts` 查历史告警 |
| `src/workflow_controller/config/settings.py` | `WatchdogConfig`（在 AppConfig 上） |
| `src/workflow_controller/api/config_api.py` | `get_config_dict` / `update_config` 暴露 watchdog 字段 |
| `src/workflow_controller/core/orchestrator.py` | `__init__` 构造 `self.watchdog`，`start()` 里接入事件总线 |
| `src/workflow_controller/api/http/mounting.py` | 内核路由域表与挂载（含告警查询路由） |

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
    fired_alerts: set[str]  # 已触发的 rule 集合，去重用
```

`Alert.to_dict()` / `Alert.from_dict()` 用于 fault.* 告警事件 data 字段序列化。

### 3.3 核心逻辑（core.py）

Watchdog 不依赖具体的 `Orchestrator`，而是按需接收独立参数——直接传入真正用到的对象。依赖单向：``orchestrator → watchdog``，watchdog 不反向 import / 类型引用 ``Orchestrator``。

检测规则是事件库订阅者：每条规则一个类，各自订阅所需的 SUBAGENT_* 事件、命中条件时经 ``watchdog/fault.py`` 编码发 ``fault.<rule>`` 告警事件。

```
WatchdogCore.__init__(config, bus, pool)   # 组合根：构造三条规则
  SubagentTimeoutRule   订阅 SUBAGENT_{START,PROGRESS,STOP}（规则 1）
  FileStallRule         订阅 SUBAGENT_{START,STOP}（规则 2）
  KeywordRule           订阅 SUBAGENT_STOP（规则 3）

WatchdogCore.start()  ← orchestrator.start() 调用
  if not config.enabled: return
  各规则向 bus.observe 注册回调，配置热更新靠每次回调现读 config

规则回调（以超时规则为例）
  on_start(event):    建 CallRecord(started_at=event.timestamp)
  on_progress(event): 查 rec, _check_timeout(event.timestamp)；rec 已清（stop 后迟到 progress）直接忽略
  on_stop(event):     查 rec, _check_timeout(event.timestamp), 清 rec

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

告警事件类型是 ``fault.<rule>`` 开放词表（字符串事件类型，同 ``stop_requested`` 等内核字符串事件的落库方式），当前三条：

| event_type | 规则 |
|-----------|------|
| `fault.subagent_timeout` | subagent 运行超时 |
| `fault.file_stall` | 连续 N 个 subagent 结束后追踪文件无变化 |
| `fault.keyword_detected` | 追踪文件中出现关键词 |

事件按 session 寻址落 events 表，写库路径按归属缓存打 run 戳（run/session 双维度可查）。payload 是 `Alert.to_dict()`（字段见 3.2 节 Alert 结构），前端按 `event_type` 前缀 `fault.` 过滤。

词表与编解码住 `watchdog/fault.py`：检测规则发事件、查询面解事件共用同一实现，检测本体不独占编码；新增规则只需给出规则名，事件类型随规则名生成。

### 3.5 告警持久化与查询

告警事件走 `bus.emit(Event.create(event_type="fault.<rule>", ...))`：
1. EventBus `emit` 先 `event_router.store` → 进 events 表持久化（打 run 戳）
2. 再分发给订阅者（SSE 不走订阅，但 250ms 轮询 `get_since` 会取到）
3. session 删除时 `delete_session` 清掉该 session 的所有 events（包括告警）

**历史告警查询**：`GET /api/v1/sessions/{session_id}/watchdog/alerts`
- 路由住 `src/workflow_controller/api/http/routes/watchdog_routes.py`，处理器经 `CoreAPI.watchdog.list_alerts` 查询，不触碰内核内部对象
- `WatchdogAPI` 直读事件库（`get_events_sync`）筛 `fault.*` 事件并解码，不依赖检测本体与内存态
- session 已结束、进程重启、watchdog 被禁用——照样能查到历史告警

### 3.6 配置

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

### 5.2 加定时器规则（不依赖事件触发）

如果需要"即使 progress 不来也能实时告警"：

```python
# 超时规则订阅者内：
async def _timer(rec: CallRecord):
    while True:
        await asyncio.sleep(30)
        if rec fired: break
        await self._check_timeout(rec, datetime.now())

# on_start:
asyncio.create_task(_timer(rec))
# on_stop:
rec.fired = True  # 让 timer 退出
```

当前实现没做定时器，因为：
- stop 事件一定有，stop 时检查是可靠兜底
- 定时器要管理 task 生命周期（cancel/异常/进程退出），复杂度高
- 最小实现优先简单

## 6. fault.* 事件迁移（已实施）

watchdog 已完成事件库化改造，检测与消费解耦：

- **告警事件类型**：告警从 `LOG_HOOK`（已删除的预留枚举值）迁移为 `fault.*` 开放词表事件（`fault.subagent_timeout`、`fault.file_stall`、`fault.keyword_detected`），session 键落 events 表、写库路径按归属打 run 戳；
- **检测规则订阅者化**：三条规则各自成为事件库订阅者（独立订阅 subagent_* 事件、自行发 `fault.*`），告警事件编解码独立为 `watchdog/fault.py` 公共模块，检测本体不再独占编码；
- **消费解耦**：前端展示按 `fault.` 前缀过滤 SSE 透传事件，历史查询经 `CoreAPI.watchdog`（直读事件库解码），均不依赖检测实现；历史 `log_hook` 告警行为干净切换、不再展示。
