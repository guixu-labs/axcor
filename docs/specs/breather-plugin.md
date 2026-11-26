# Breather Plugin

> 项目 `opsbot.yaml` 可用 `context_threshold_pct` 和 `subagent_context_threshold_pct` 分别覆盖主、子代理阈值，用 `context_window` 覆盖窗口；子代理阈值按项目值 > 全局子代理值 > 有效主代理阈值回落。任一运行中子代理超过其阈值时，Breather 暂停主 session 与全部在途子代理，由一个独立 handoff session 为每个子代理写文档；主 session 未超阈值时恢复原主 session 派发新子代理，主 session 也超阈值时同批总结主代理并创建新主 session。页面分别展示主、子代理最近一次模型输入占用和各自阈值。子代理流程见下文「子代理整组交接」。

长会话上下文管理插件。在 worker session 的上下文使用率超过阈值时**主动 handoff**：停掉上下文已满的旧 session，由一个干净上下文的 handoff agent 盘点进度、写出交接文档，再以该文档为初始 prompt 启动新 session 接力——避免上下文窗口耗尽导致任务卡死或质量崩塌。

breather 是 opsbot 框架级的「通用 handoff 工具」：默认在 loop 边界自动检查触发（无需项目侧 skill 配合），交接内容可按领域定制（模板文件）；也支持通过 `/breather` 端点按需触发（人工或 skill 均可，见 §3）。

---

## 1. 它解决什么问题

Claude Code 是**无状态**的——每一轮请求都要把从 session 启动到现在的完整对话历史带上去。所以一个 session 的「累计上下文」只增不减：

```
累计上下文 = input_tokens + cache_creation_input_tokens + cache_read_input_tokens
```

长任务（如算子生成：调研 → 设计 → 实现 → 验证，常跨数十轮、派大批 sub-agent）会逐渐撑满上下文窗口。窗口一旦接近上限，模型可用「思考预算」被历史挤占，输出质量下降；撑爆则直接报错。breather 在**到达阈值（默认 70%）时**主动切片：旧 session 带着它的全部产物停在磁盘上，新 session 读交接文档从断点继续——相当于「翻篇」，把上下文压回接近 0。

### 为什么不直接靠模型自带的上下文压缩（auto-compact / auto-summarize）？

Claude Code 和 opencode 都自带「上下文压缩」：context 涨到阈值（cc 约 92%、opencode 约 90%）时，模型把**整段对话就地总结**、同一个 session 继续。听起来这也解决了「撑爆」，那为什么还要 breather？因为两者**都会丢信息**，差别在「谁决定丢什么、丢得透不透明、是不是个检查点」：

| | auto-compact / auto-summarize | breather handoff |
|---|---|---|
| 谁决定留什么 | 模型（opaque summary，看不见留了啥） | **你**：handoff agent 按 template 结构化提取，template 可配 |
| 透不透明 | session 内部静默压缩，看不到 / 改不了 | 交接文档是磁盘文件，**可读 / 可改 / 控制面可见 / reviewer 能看** |
| 检查点 | 原地继续，无边界 | **显式边界**：结老 session、起新 session、successor 追踪，可暂停 review |
| 文件（地面真相） | 跟对话一起被压 | **不丢**——新 session 重新 Read 真实文件，full fidelity |

关键：新 session 是干净 context + 读交接文档 **+ 读真实文件**，所以**文件本身一点不丢**；被「curate」的只是对话里的推理过程（worker 试过啥、死胡同、为什么），而那是按你的 template 提取、你能看见的。auto-compact 则是把**整段对话（含推理）**一股脑压成 summary，丢得又不均匀。还有长期效应：**auto-compact 多次压缩会累积失真**（summary of summary…），breather 每次是 fresh context + curated 文档，干净重置。

一句话：breather 不是「不丢信息」（无限上下文塞不进一个文档），而是「**丢在明处、丢在你的规矩里、且是个你能拦住的检查点**」。auto-compact 够用的场景：短任务轻微超 context、只想「别崩」、不要 ops 介入；长 horizon + 要 oversight 的任务（Workflow Controller 的定位）用 breather。

---

## 2. 设计思路与抉择（给开发者）

理解这几个抉择，就理解了 breather 为什么长这样。

### 抉择 A：触发解耦——在 loop 边界检查，不依赖 stop hook

breather 最早的自动触发依赖 Claude Code 的 stop hook（worker 停下时回调）。但实测发现：**`claude -p`（CLI 的非交互/print 模式）不触发 `/hooks/stop`**——只有 SDK runtime（进程内 `_stop_cb`）会触发。这意味着 breather 的自动 handoff 在 CLI runtime 下失效。

于是触发逻辑上移到 **LoopEngine 的 loop 边界**：每个 iteration 的 worker 子进程自然 EOF 后，`LoopEngine` 经通用的 iteration-boundary gate 调一次 `breather.on_iteration_boundary(session)`，由它检查上下文、决定是否 handoff（超阈值返回 `IterationGateResult(action="stop")`）。这条路径是 **driver-agnostic** 的——**SDK / CLI / opencode 三种 runtime 都自动触发**，因为「一轮 worker 轮完」这个事件三种 runtime 都会发生，与 hook 机制无关（opencode 的完成信号经 `step_finish → on_stop` 副作用，同样在 worker EOF 后落地）。

> stop hook 路径仍保留，但现在主要服务于 `notify_skill` 和手动 `/breather` 这两条请求式触发；另在 worker 自然停止时补读一次实时快照（见 §5 检查方式）。**自动**触发除 loop 边界外，还在 **subagent 返回点**（主 worker 收 subagent 结果的干净 turn 边界）轮内 check——这是为长轮任务（如算子生成 primary+subagent，主 worker 一口气跑完才自然 EOF）补的检测点，否则 context 在 subagent 返回点累积却没被 check、爆了才到 loop 边界，handoff 永远迟到。见 §3。

### 抉择 B：handoff 用独立 agent 盘点，不让 worker 自己写交接文档

为什么不直接让快溢出的 worker 自己写一份交接文档再退出？因为它的上下文**已经满了**——让它再产出高质量、结构化的盘点，既容易写不完（剩余预算不够），也容易遗漏/失真。

breather 起一个**全新、干净上下文的 handoff agent**（`role=handoff_agent`），它的唯一任务就是盘点旧 session 留在磁盘上的产物与进度，写出交接文档。新 agent 上下文近乎为零，有充足预算把盘点做扎实。

### 抉择 C：确定性工具作为「地面真相」，不让 LLM 用眼睛数

handoff agent 盘点时，关键数字（产物有几个、任务清单完成几项）必须**准确**——这些数字决定下一 worker 是否重跑已完成的工作。让 LLM 自己用 Read/Glob 去数文件、数 checkbox，会数错（漏文件、数错勾选状态）。

breather 提供 3 个**纯代码、无 LLM** 的确定性工具（`handoff_tools.py`），handoff agent 通过 Bash 调用它们拿到「地面真相」，再据此写文档。工具与 LLM 推理交叉验证，冲突时以工具为准。

这 3 个工具**零 opsbot 依赖**（只用标准库），因为 finalize 时会把脚本复制到目标工程的 `.opsbot/` 下，由 handoff agent 在那个 cwd（PYTHONPATH 上没有 workflow_controller 包）里 `python .opsbot/handoff_tools.py <cmd>` 调用。

### 抉择 D：先起新 session，成功后再停旧 session（避免接力断档）

handoff 的最后两步顺序很关键：**先启动 worker-h2（继任），确认起来了，再停旧 worker**。

如果反过来（先停旧再起新），一旦起 h2 失败，就会出现「旧已停 + 新没起」的**接力断档**——任务彻底丢失。先起新后停旧，则任何一步失败时旧 worker 都还在，是天然的降级。

### 抉择 E：graceful 降级——任何失败都退回「旧 worker 继续」

handoff 全流程包了多层降级：

| 失败点 | 行为 |
|--------|------|
| handoff agent 超时没写完文档 | 不停旧 worker，重置状态，worker 继续 |
| 文件没被本轮（重新）写入（旧残留） | 不停旧 worker，重置状态，worker 继续 |
| 起 worker-h2 失败 | 不停旧 worker，重置状态，worker 继续 |
| 部署 handoff_tools.py 失败 | 只 warn，agent 降级用内置 Read/Glob 手动盘点 |
| 自定义模板读不到 | warn，回落内置默认模板 |

核心原则：**breather 永远不应该因为自己的故障把用户任务弄丢**。降级时旧 worker 仍在跑，最坏情况是这次没 handoff 成功（下次 loop 边界再试）。

---

## 3. 触发方式

breather 有三个触发入口，**默认都受 `context_threshold_pct` 阈值约束**——上下文超阈值才 handoff，没超只读当前占比；`/breather` 支持 `force=true` 跳过阈值、在语义交接点主动切（见下）。三者区别只在**何时检查**：

| 入口 | 何时检查上下文 | 谁触发 |
|------|--------------|--------|
| **loop 边界** | 每个 iteration worker 自然 EOF 后 | 自动（driver-agnostic，SDK / CLI / opencode 都行）|
| **subagent 返回点** | 主 worker 收 subagent 结果后（干净 turn 边界：disk 产物 + git tag + 无 in-flight）| 自动（无条件订阅 `SUBAGENT` stop event）|
| **notify_skill** | 配置的 skill 产出 `tool_result` 后 | 自动（配 `notify_skill`）|
| **/breather 端点** | 被调用的那一刻 | 人工 curl / 外部脚本 / skill 驱动的 worker（按需）|

三者最终都触发 `finalize_handoff`（§4）：**loop 边界**直接 spawn；**`notify_skill`** 与 **`/breather`** 经 `_mark_handoff_pending` 标 pending、再等 worker 自然 stop 触发（SDK runtime 下 worker 停下 → `on_stop_hook` 捕获 final_text → `finalize_handoff`）。

绝大多数场景**只需依赖 loop 边界自动触发**，零配置。`/breather` 端点主要用于**按需立即检查**（不必等下个 iter 边界）或拿到当前上下文占比。

### `/breather` 端点

注册在 hook server（默认 `127.0.0.1:18765/breather`），可被任何能访问该端口的调用方触发——人工终端、外部脚本、或 worker（由项目 skill 的指令驱动 curl）。三者命中的是同一个端点，区别只在发起方：

```bash
curl -s -X POST http://127.0.0.1:18765/breather \
  -H "Content-Type: application/json" -d '{}'
# 可选字段：
#   "session_id": "<claude-session-uuid>"  精确指定 session（省略则取当前 worker）
#   "force": true                          跳过阈值检查，立即进入 pending（见下）
```

响应：

```jsonc
{"handoff_triggered": true,  "phase": "pending", "session": "worker"}    // 超阈值 或 force=true，进入 pending
{"handoff_triggered": false, "context": {"current_tokens": 5000, "max_tokens": 1000000, "percentage": 0.5}}  // 未超阈值且非 force，只报占比
{"handoff_triggered": false, "reason": "no active session"}              // 无 worker
{"handoff_triggered": false, "reason": "handoff already pending"}        // 并发重复调用
```

### `force` 选项：跳过阈值，在语义交接点主动切片

默认 `/breather` 受 `context_threshold_pct` 约束——上下文没到阈值就不切。但有时领域想在自己选的**语义交接点**（如「阶段 2 验证完」「一批产物落盘后」）主动切片，那时上下文可能才 40%，远没到阈值。`force=true` 跳过这个门：

```bash
curl -s -X POST http://127.0.0.1:18765/breather \
  -H "Content-Type: application/json" -d '{"force": true}'
```

**等 subagent 返回点切片**：force 设 `_handoff_force_pending`（"想切"），**不立即切**，等**下一个 subagent 返回点**（`_on_context_event` 在那 check force → 主动停 worker 在干净 turn 边界 → finalize）。force 跳阈值检查，但切片位置仍统一在 subagent 返回点（合适停点）——和阈值检测走同一条路。无 subagent 的纯长轮任务无返回点可等 → force 不切（loose end；算子生成 primary+subagent 有 subagent 覆盖）。

> 这意味着 skill 调 force 时应放在**接近自然停止点的位置**（如某阶段产物的最后一步之后）——worker 随即停下，handoff 很快落地。若 worker 调完 force 还继续干别的，handoff 会推迟到它下次自然停止（watchdog 30min 兜底）。force 不改阈值门以外任何行为，复用全部既有降级逻辑（§4）。

> **web UI 入口**：Dashboard 每个 running session 有 **Handoff** / **Force** 两个按钮（§7），分别走阈值门和 force，命中的就是本端点（`/api/breather/handoff`，breather 经 `get_web_router` 暴露——它是首个用此机制的插件）。


---

## 4. Handoff 流程（`finalize_handoff` 五步）

```
触发（两路，都最终进 finalize_handoff）：
  • loop 边界（自动）：worker 自然 EOF → on_iteration_boundary 超阈值
                       → 设 _handoff_pending + 抓 final_text → spawn(finalize)
  • 手动 / skill（/breather, notify_skill）：_request_handoff(force?)
                       → _mark_handoff_pending 设 pending + watchdog
                       → 等 worker 自然 stop → on_stop_hook 抓 final_text → finalize
      ▼
finalize_handoff(session)：
  1. 部署确定性工具 → {cwd}/.opsbot/handoff_tools.py
  2. 构建 handoff agent prompt（模板 + final_text 写入 .opsbot/handoff-agent-prompt.md）
     → 起 handoff agent session（role=handoff_agent，干净上下文，max_turns=100）
  3. 等 agent 跑完（wait_session_done，默认 10min 超时）
     → 校验交接文档是「本轮新写」的（mtime ≥ handoff 起点，防旧残留冒充）
  4. 以交接文档为初始 prompt 构建 worker-h2 配置 → start_session_with_config
  5. worker-h2 起来了 → 停旧 worker → 标记 successor（UI 显示接力链）

任一步失败 → 旧 worker 不停，重置状态，天然降级。
```

**新鲜度校验**（步骤 3）是个易错的点：交接文档被设计成**跨轮增量更新**（下一轮继承上一轮的「核心发现/踩过的坑」），所以旧文件一直都在。光判 `exists()` 不够——若 agent 本轮崩了，`exists()` 仍为 True，会把上一轮的旧文档当本轮交接，导致本轮工作全丢。breather 改判 **mtime 必须不早于本轮 handoff 起点**（`_pending_handoff_started_at`），本轮新写的才认。

### 交接文档装了什么（三个信息源）

handoff agent 写 `handoff.md` 时，内容来自三个来源（可靠性高 → 低）：

| 来源 | 是什么 | 哪来 | 当前状态 |
|------|--------|------|---------|
| **磁盘文件清单 + 内容** | 工作目录里实际产物的状态（哪些文件、多大、stub 还是完整） | 确定性工具 `list_generated_files`（agent 直接盘点磁盘） | ✅ **永不丢**——文件就在磁盘上（所有 runtime）|
| **worker 最后一条消息**（`final_text`） | worker 停下前说的最后一句话（常带当前进度 / 下一步意图） | worker stop 时从 event store 捕获 | ✅ **永不丢**（所有 runtime）|
| **worker 的 reasoning trace（推理轨迹）** | worker 这一轮**想过啥、试过啥、踩过啥坑、为啥这么决定** | 确定性工具 `extract_worker_narrative` 从事件库（`events.db`）抽 assistant 文本块 + reviewer 逐轮 summary | ✅ **三 runtime 都有**——所有 driver 都把 `ASSISTANT_MESSAGE`/`LOG_EVAL` 事件写进事件库 |

前两个是「地面真相 + 最后状态」，永远在。第三个是「过程推理」，把交接文档升级成 **rich briefing**——新 worker 还知道**之前为啥走到这**，避免重走死胡同、复用已验证的决策。三 runtime 一致可用（数据源是通用事件库，非任一 runtime 的私有日志）。

> **🔔 rich briefing（已实现）**：第三个来源经 `extract_worker_narrative` 从 Workflow Controller 守护进程事件库重建——单库化（B1）后 worker 事件统一落守护进程启动目录的 `.opsbot/events.db`，Workflow Controller 生成 handoff prompt 时把该目录的绝对路径注入调用参数（worker cwd ≠ 守护进程目录时同样可读，不会读到 worker 工程下的旧库）。三个 runtime（claude_sdk / claude_cli / opencode）都把 worker 的 `ASSISTANT_MESSAGE`（assistant 文本块）与 `LOG_EVAL`（reviewer 逐轮 summary）事件写进同一张 `events` 表，工具按 session 过滤、按时间序抽取，跳过 thinking / tool_use / tool_result 等原始数据。所以 narrative 对所有 runtime 都有料，handoff 文档是 rich briefing。

> 历史注记：旧实现读 `iter_*_worker.stdout.log`（migbot 时代的 CC 流式 JSONL 日志），但 opsbot 三个 driver 都不写该文件——sdk 进程内跑无 stdout，cli / opencode 用 `stdout=PIPE` 逐行解析只读不落盘——narrative 恒返回 `[空]`。改读事件库（数据早已存在、三 runtime 通用）后缺口关闭，无需 driver 层加 stdout tee。

> 任一来源缺失仍是降级、不是故障（见 §2 抉择 E）：事件库里没有该 session 的事件时（极端情况），narrative 返回 `[空]` 标记，handoff agent 回退到只用前两个来源（薄 briefing），任务仍能正确完成。

### 子代理整组交接

运行中的子代理分别拥有自己的模型上下文。Breather 从 OpenCode 的子 session 本地库、Claude Code 的 `subagents/agent-<id>.jsonl` 转录读取最近一次完整 assistant 输入；同一采样循环也读取主代理上下文。采样不到时显示未知，不按猜测值触发。任一子代理超过有效阈值，就以主 session 为单位暂停全部在途子代理；仅主代理越线则等子代理结束后按原主代理流程交接。

整组交接在项目 `.opsbot/handoffs/<主 session id>/<批次>/` 保存 `manifest.json`、主代理事件摘要、每名子代理的源转录快照和交接文档。一个独立 `handoff_agent` 按 `manifest.json` 为所有在途子代理写文档；`main_handoff` 非空时还须写主代理摘要，最后写清单指定的完成信号。文档使用 `<代理 id>-handoff-<轮次>-doc` 命名，批次目录避免后续交接覆盖前一批。

清单中每名子代理的 `dispatch_prompt` 要求新子代理先读取**自己的**交接文档、核对磁盘产物，再从未完成步骤继续。仅子代理越线时，opsbot 恢复原主 session 并注入清单路径；主代理也越线时创建新主 session。两条路径都要求主代理读取清单后在同一条回复中并行重新派发所有被中断的子代理，避免只派一名就再次越线；接续校验检查新派发数量和 Agent/Task prompt 中对应的文档路径。用户主动暂停接续会话时，校验退出；自然结束而未派发仍报错。文档不完整时保留源快照并标记交接失败，供恢复流程说明证据缺口。

主代理与子代理阈值可在项目 `opsbot.yaml` 中分别设置 `context_threshold_pct` 与 `subagent_context_threshold_pct`。整组交接由在途子代理越线启动，暂停主会话后再读取主代理用量：主代理也越线时，清单的 `main_handoff` 非空，交接会话须写主代理摘要及所有在途子代理摘要，新建主会话；仅子代理越线时，`main_handoff` 为空，恢复原主会话。两个阈值都设为 1% 只适合同时保持子代理在途的示例；主代理初始上下文可能已经超过 1%，若子代理提前返回，会先走普通主代理交接。

整组 handoff session 使用内置 `GROUP_HANDOFF_PROMPT` 执行清单和完成信号要求；若设置 `handoff_prompt_path`，会先读取同一份交接指令，再附加整组执行要求。旧 `group_handoff_prompt_path` 仅在统一模板未设置时兼容读取。 Claude CLI 子代理类型初次未知时，转录的后续 `attributionAgent` 会更新在途记录，整组清单采用更新后的实际类型。`handoff_agent_timeout: 0` 表示等待交接文档时不设超时。可运行示例及首次交接验收步骤见 [确定性模拟](../../examples/subagent-group-handoff/README.md)和 [真实 Claude CLI 试跑](../../examples/subagent-only-handoff/README.md)。

---

## 5. 上下文占比怎么算

```
占比% = 累计上下文 / 上下文窗口 × 100
```

**累计上下文**：从最新 `step_finish` 事件的 `usage` 读 `input_tokens + cache_creation_input_tokens + cache_read_input_tokens`（= session 启动至今带上去的全部历史，非本轮增量）。> Phase 1.7 前读 RESULT 事件，历史会话兼容读旧库 RESULT；详见 RESULT 拆分（原链接目标未入库）。

**上下文窗口**优先级（高 → 低）：

1. `context_window` 配置项（显式指定）
2. Claude 上报的 `modelUsage.contextWindow`（会话内最新一条上报，`step_finish` / 历史 `result` 携带。`context_usage` 事件本身不带窗口，读数为 `context_usage` 时回溯此前最近一条上报，轮内检查点与 loop 边界同分母）
3. 默认 `200000`

> **关于 `[1m]` 后缀**：模型名带 `[1m]` 后缀时（如 `GLM-5.2[1m]`），Claude Code 按 1M 窗口跑并**如实上报**；不带 `[1m]` 则按 200k。也就是说 cc 按模型变体正确上报窗口，通常无需显式配 `context_window`。该配置项主要作 web 可见可改的显式声明，或用于强制一个与 cc 上报不同的窗口。

> **opencode runtime**：三处 runtime 差异已在 driver 边界抹平，breather 行为与 cc 一致——
> - **token 归一**：opencode 原生 usage 是 `{input, output, cache:{write, read}}`，driver 把它归一成上面那套 Anthropic 形（`input/output → *_tokens`、`cache.write/read → cache_creation/read_input_tokens`），占比计算读同样的字段。
> - **关掉 opencode 自带 compaction**：opencode 默认开 `compaction.auto`（~90% 就地总结，与 §1 讲的 auto-summarize 同类）。opsbot 经 `OPENCODE_CONFIG_CONTENT` 把它关掉——否则它会抢在 breather 阈值前压缩、并留下失真的压缩后 token 计数干扰检测。关掉后 breather 独占上下文管理。
> - **`context_window` 对 opencode 是权威值（无 auto-detect 兜底）**：opencode 的 `modelUsage` 恒空（`context.py` 里 config 设了就用 config、没设才去读 modelUsage，而 opencode 的 `modelUsage` 永远是 `{}`），占比分母拿不到 cc 那种上报值。**cc** 清空 `context_window` 还能回落到它上报的窗口，**opencode** 清空只会扁平回落 200000、不会自动探测。所以这个值要**与你 opencode.json 实际跑的模型窗口对齐**：shipped 默认 `1000000`（按 1M 模型给）；换非 1M 模型时，在 web UI（Settings → Plugins → breather → context_window）或 `config.yaml` 改成实际窗口，否则占比按错的窗口算、breather 在错的点触发。

**检查方式**（`ContextTracker.check()`）：

- 优先 `check_recorded`：按时间倒序读最新一条有效读数——`context_usage` 事件（Claude CLI 每条主会话 assistant 消息即时发布，subagent 返回点 / Stop hook / loop 边界都能读到新鲜快照）、`step_finish`（优先 `context_usage` 快照，兼容历史库 RESULT）。**免 fork**、零子进程开销。输入占用为 input、cache creation、cache read 三项之和；缺失或非法读数不算零（明确上报的全零仍是有效读数），无效读数不覆盖上一条有效快照。占比分母沿用会话已知窗口：读数事件自带 `modelUsage` 用之，否则回溯最近一条上报（`context_usage` 事件不带窗口），都没有才回落 `200000`。
- 无任何有效读数时 fallback `_check_from_command`：fork 一个 `--resume` 的 driver 查询拿 usage，取 fork 流上最新一条有效读数与窗口（`step_finish` 自带 `modelUsage`，`context_usage` 事件沿用流上已知窗口）。探测按原会话的 runtime / cli_wrapper / cwd 起 fork，model 走 `Session.query` 同一解析口径（`resolve_query_model`：opencode 裸名改写为 env 已解析路由、非 codex 回退 `ANTHROPIC_MODEL`），保证 fork 命中原会话注入的 provider 而非默认 provider，限时 15 秒，结束后清理 driver；读数逐事件即时落账——流中途失败或超时前已得的有效读数照常返回，不因收尾异常作废（慢/半挂死 driver 也能拿到读数触发交接）；仅全程无有效读数（探测失败、超时且一无所获）→ 返回未知（`percentage=null`），自动检查跳过本次判断、手动交接返回 `context_usage_unknown`，不得当作 0% 低于阈值。强制交接不读用量、不探测。
- worker Stop hook（reviewer 之前）只读已记录快照、不 fork 活跃会话：无在途 subagent 且快照超阈值时直接进入交接，等价于阈值检测的后备停点。

---

## 6. 配置

`src/workflow_controller/plugins/breather/config.yaml`（当前默认）：

```yaml
name: breather
enabled: false
context_threshold_pct: 70
subagent_context_threshold_pct: null  # 留空沿用有效主代理阈值
context_window: 1000000        # 带宽，见 §5；留空=用 cc 上报值
handoff_prompt_path: null    # handoff agent 的 prompt 模板（给 agent 的指令，非产物；见 §8）
continuation_prompt_path: null  # continuation worker 的 prompt 模板（接续指令；见 §8）
handoff_wait_stop_timeout: 1800  # 阶段一：等 worker stop hook（秒）
handoff_agent_timeout: 600       # 阶段二：等 handoff agent 写完（秒）
```

> Web 的「设置 → 插件 → breather → 配置」分别提供全局主代理和子代理阈值输入；其余字段使用通用输入框。项目级覆盖值在项目 `opsbot.yaml` 中配置，只影响新建会话。

完整字段表：

| 配置项 | 默认 | 说明 |
|--------|------|------|
| `context_threshold_pct` | `70` | 主代理上下文占比阈值（%），超过触发 handoff |
| `subagent_context_threshold_pct` | `null` | 子代理阈值（%）；未设置时沿用该会话的有效主代理阈值；100% 时只采集用量，不自动交接 |
| `context_window` | `1000000` | 上下文窗口（tokens）。优先级见 §5；清空回落 cc 上报值 |
| `handoff_prompt_path` | `null` | handoff agent 的**指令文件**（告诉 agent 如何盘点和写交接文档），不是交接产物。见 §8 |
| `continuation_prompt_path` | `null` | continuation（接续 worker）的 **prompt 模板**（给下一 worker 的指令：读交接文档后怎么接续）。默认内置 `RESUME_PROMPT`，设了就指向你自己的 `.md`。见 §8 |
| `group_handoff_prompt_path` | `null` | 旧配置兼容字段；仅当 `handoff_prompt_path` 未设置时读取，不再出现在设置页 |
| `handoff_filename` | `handoff.md` | 旧配置兼容字段；运行时交接产物均按 `<主代理/子代理 ID>-handoff-<轮次>-doc` 命名，设置页不提供修改入口 |
| `handoff_wait_stop_timeout` | `1800` | 阶段一超时：等 worker 发 stop hook（worker 派大批 sub-agent 常十几分钟才停，故给 30min）。`0`=禁用 watchdog（无限等） |
| `handoff_agent_timeout` | `600` | 阶段二超时：等 handoff agent 写完文档（实测约 4min，给 10min 余量）。`0`=无限等 |
| `notify_skill` | `""`（关） | 配置后，该 skill 的 `tool_result` 事件触发自动检查上下文（§3） |
| `strategy` | `auto` | **当前未使用**（保留字段）。检查方式由 `check()` 内部按有无 step_finish（Phase 1.7 前为 RESULT）自动决定 |
| `handoff_skill` | `operator-handoff` | **当前未使用**（保留字段）。早期设计引用的 skill 名，现 prompt 不再引用 |
| `handoff_timeout` | `600` | **已废弃**别名，向后兼容 `handoff_wait_stop_timeout`/`handoff_agent_timeout` |

> `handoff_prompt_path` 指向给 handoff agent 的**输入指令**，默认使用内置 `HANDOFF_AGENT_PROMPT`。交接文档是 agent 写出的**输出产物**：主代理按 `<源 session ID 前 8 位>-handoff-<轮次>-doc` 命名，子代理以对应子代理 ID 为前缀；主代理完成信号以 `-done` 结尾。指令中的 `{handoff_filename}` 由系统填入实际产物名，用户无需指定文件名。

---

## 7. 使用（用户视角）

### 日常：什么都不用做

breather 默认启用，loop 边界自动检查。你正常跑长任务即可——上下文到 70% 时它会自动切片接力，新 session 在 UI 里显示为 `worker-h2`、`worker-h3`…（`-hN` 后缀递增），接力链可在 session 面板看到。

### 想立即检查 / 手动触发

两种方式：
- **web UI**：Dashboard 上 running session 的 **Handoff**（阈值门，超 `context_threshold_pct` 才切）/ **Force**（跳阈值，下个停止点切）按钮，点一下即可，toast 报结果（触发 / 未触发+占比）。
- **curl**：`POST /breather`（端点细节见 §3），body `{session_id?, force?}`。默认仍受阈值约束；想无视阈值用 `force=true`。

### 调阈值

上下文涨得快想早点切片？把 `context_threshold_pct` 调低（如 `50`）。窗口配错导致占比异常？检查 `context_window` 是否匹配你模型的实际窗口（见 §5 的 `[1m]` 说明）。

---

## 8. 特定领域扩展（开发者视角）

breather 的交接文档默认是一份**通用结构**（阻塞项 / 如何接续 / 进度 / 产物清单 / 待执行任务 / 核心发现 / 关键决策 / 踩过的坑 / 本轮变更 / 编译验证命令 / 项目坐标，共 11 节）。接续 worker 的 prompt 也是通用的（读交接文档 → 按「如何接续」执行）。不同领域（cannbot 的多个算子领域、鸿蒙迁移、其它长任务）往往想要**不同的交接结构或盘点步骤**——breather 通过 `handoff_prompt_path` 和 `continuation_prompt_path` 支持不改一行代码地注入领域模板。

默认模板以项目目录、原始任务和现有产物为坐标，不假定算子目录、算子名称或 `LOG.md`。算子等领域专用步骤应由项目模板提供。模板路径未配置或不可读时也回落到这个通用默认模板。

### 交接指令

| 指令 | 配置项 | 作用 | 内置默认 |
|------|--------|------|----------|
| handoff prompt | `handoff_prompt_path` | 给 handoff agent 的指令（盘点步骤/文档结构/原则） | `HANDOFF_AGENT_PROMPT` |
| continuation prompt | `continuation_prompt_path` | 给接续 worker 的指令（读交接文档后怎么接续） | `RESUME_PROMPT` |


交接指令只配置 `handoff_prompt_path` 一处；在同一文件中用 `<!-- workflow:main -->` 写主代理如何盘点自身工作、交接后如何重新派发仍在途的子代理（子代理自动交接关闭时无需此项），用 `<!-- workflow:group -->` 写每名子代理如何盘点自身工作；共同规则可放在 `<!-- workflow:common -->`。普通交接选共享段和 main 段；子代理整组交接选共享段和 group 段，若主代理同时越线再用 main 段指导主代理摘要。未分段的旧文件保持原样，只指导普通主代理交接，缺少主代理段时回退内置主代理指令；旧 `<!-- opsbot:... -->` 标记继续识别，已上传的旧文件无需重写。整组交接只在指令文件带 `<!-- workflow:... -->` 或旧标记分段时读取，未分段时沿用旧 `group_handoff_prompt_path` 或内置整组指令。文件里面向编辑者的 HTML 注释（`<!-- ... -->`）不会被注入交接 prompt。整组交接所需清单、文档路径和完成信号由系统追加，不要求用户在指令里重复描述。`continuation_prompt_path` 独立指导接续。设置页分别以 `workflow-handoff-instructions.md`、`workflow-continuation-instructions.md` 下载实际内置指令；源码在 `src/workflow_controller/plugins/breather/handoff.py`（`HANDOFF_AGENT_PROMPT`、`RESUME_PROMPT`）及 `group_handoff.py`（`GROUP_HANDOFF_PROMPT`）。

同一份指令文件可这样写：

```markdown
<!-- workflow:common -->
先核对项目文件与任务进度，未知状态写明证据缺口。
<!-- workflow:main -->
读取上一主代理的事件和项目产物，列出已完成工作、阻塞项、验证结果与下一步；写出 {handoff_filename}，自检后写完成信号。
<!-- workflow:group -->
逐一核对每名子代理的原任务、实际产物和未完成工作，给出新子代理可以执行的下一步。
```

`{handoff_filename}` 只在主代理段替换；整组交接使用 `{manifest_path}`、`{done_path}`，系统始终附加写文档与完成信号的约束。

### 配置位置

**两种方式**：

1. **全局**（breather `config.yaml`）——所有项目共用：

```yaml
handoff_prompt_path: templates/my-domain-handoff.md
continuation_prompt_path: templates/my-domain-continue.md
```

2. **per-project**（项目 `opsbot.yaml`）——只影响本项目，优先于全局：

```yaml
handoff_prompt_path: handoff-prompt.md
continuation_prompt_path: continuation-prompt.md
```

per-project 配置经 `SessionConfig` 透传到 breather：session 有自己的值就用 session 的，没有就用全局默认。交接与接续模板均随 handoff 接力传给 successor session，保证多轮交接沿用同一套模板。

Web 设置中的「选择文档」只暂存浏览器所选的文件；点击同一插件的「保存」才上传文件并把返回路径写入全局配置。清空路径并保存即恢复内置指令。项目 YAML 中显式配置的模板路径优先于这个全局路径；测试全局模板时须使用未配置相应路径的项目。整组子代理交接只取交接指令中的 group 段（主代理也越线时再取 main 段）；其接续提示由整组交接流程生成，不读取 `continuation_prompt_path`。

### 用法

1. 写一份领域自己的 handoff prompt 模板（`.md` 文件），描述你想要的盘点步骤、文档结构、原则。
2. 在 breather 配置里指向它：

```yaml
handoff_prompt_path: templates/my-domain-handoff.md   # 相对 project_dir，或绝对路径
```

3. handoff agent 起来时，会用你的模板代替内置默认 prompt。

### 模板写法

**handoff prompt 模板**——已知槽位会被自动填充：

| 槽位 | 填入 |
|------|------|
| `{handoff_filename}` | 交接文档文件名（`<session id 前 8 位>-handoff-<轮次>-doc`） |
| `{worker_session_id}` | 上一 worker 的 session id（用于抽 narrative） |

普通主代理交接的完成信号沿用 `<session id 前 8 位>-handoff-<轮次>-done` 命名；交接文档名以 `-doc` 结尾。自定义模板要求 agent 写完成信号时，应将填好的 `{handoff_filename}` 末尾 `-doc` 替换为 `-done`。

**continuation prompt 模板**——已知槽位：

| 槽位 | 填入 |
|------|------|
| `{handoff_path}` | 交接文档的完整路径（接续 worker 用 Read 读取） |

其它 `{占位}` **原样保留**不报错——你可以写自定义占位符给 agent 看，只有上面列出的会被替换。

**整组交接模板**使用 `{manifest_path}`（清单绝对路径）和 `{done_path}`（完成信号绝对路径）。清单中每个 `entries[].handoff` 都要写出文档；`main_handoff` 非空时也要写出主代理文档。完成所有文档并自检后写 `done_path`，内容为 `done`。`handoff_prompt_path` 对普通交接和整组交接均生效；整组交接时自动附加清单及完成条件，恢复 prompt 由流程根据主代理是否越线生成。

### 示例：一个精简领域的模板

`templates/my-domain-handoff.md`：

```markdown
你是 handoff agent。盘点上一 session 进度，写交接文档 `{handoff_filename}`。

## 信息源（优先级高 → 低）
1. 确定性工具（地面真相）：`python .opsbot/handoff_tools.py list_generated_files "." "src/**/*.cpp"`
2. worker narrative：`python .opsbot/handoff_tools.py extract_worker_narrative "." "{worker_session_id}"`
3. worker final_text（下方附带）

## 盘点步骤
1. 列出 src/ 下实际产物，区分「完成/进行中/stub」
2. 抽 worker narrative，提取本领域的阻塞点与决策
3. 写 {handoff_filename}，结构如下：

## 1. 阻塞项
## 2. 如何接续
## 3. 产物清单（按本领域模块组织：core/io/ui…）
## 4. 编译命令（逐字抄录）

## 原则
- 数字来自工具，不要自己数
- 磁盘与 narrative 冲突时以磁盘为准
```

领域想加自己的章节（如鸿蒙迁移的 Slice/Step 维度）、改盘点步骤、换原则，都在模板里写——完全不需要改 breather 代码或提 PR。

> `extract_worker_narrative` 的第一个参数在模板里写 `"."` 即可：生成 prompt 时 Workflow Controller 会把它替换为守护进程事件库所在目录的绝对路径（单库化后事件不在 worker 工程目录下），模板无需感知守护进程布局。

### 失败回落

模板路径设了但读不到（路径错、权限、编码问题）→ breather **warn 并回落内置默认模板**（handoff 回落 `HANDOFF_AGENT_PROMPT`，continuation 回落 `RESUME_PROMPT`），handoff 照常进行（只是用通用结构）。所以配错不会卡死任务。

### 未来：工具集可插拔（尚未实现）

当前 3 个确定性工具（见 §9）是固定的。如果某领域需要**自己的确定性工具**（例如验证特定产物清单是否齐全），未来会加一个工具注册接口让领域插件注册自定义工具、模板自动引用。**现阶段**：领域可在自己的模板里直接用 Bash 调自己工程里的脚本（只要自包含、结果打到 stdout），效果等同。

---

## 9. 确定性工具参考

`scripts/handoff_tools.py`，3 个纯代码命令，结果打到 stdout（结构化→JSON，narrative→纯文本）。finalize 时复制到 `{cwd}/.opsbot/handoff_tools.py`。

```
python .opsbot/handoff_tools.py count_checkboxes <plan_file>
python .opsbot/handoff_tools.py list_generated_files <base_dir> <glob>
python .opsbot/handoff_tools.py extract_worker_narrative <project_dir> <session_id>
```

| 命令 | 入参 | 输出 | 用途 |
|------|------|------|------|
| `count_checkboxes` | markdown 清单路径 | `{total, completed, pending}` JSON | 数 `- [x]`/`- [ ]` checkbox 状态。只数符号，不代表文件存在（要与 `list_generated_files` 交叉验证） |
| `list_generated_files` | base_dir + glob | `[{path, lines, size}, ...]` JSON | 列磁盘实际产物，含行数/大小。用于区分「已落盘/stub/缺失」 |
| `extract_worker_narrative` | project_dir + session_id | 纯文本 | 从事件库 `{project_dir}/.opsbot/events.db` 按 session 抽 `ASSISTANT_MESSAGE` 的 text block + `LOG_EVAL` 的 reviewer 逐轮 summary（跳过 thinking / tool_use / tool_result 等原始数据），按时间序拼接。三 runtime 通用（数据源是事件库，非 runtime 私有日志）。prompt 中 project_dir 由 opsbot 注入为守护进程库所在目录。事件库缺该 session 或无文本内容时返回 `[空]` 标记 |

这 3 个工具是 `opsbot.plugins.breather.handoff` 的 re-export 真相源——改逻辑改 `scripts/handoff_tools.py`，别在 re-export 处改。

---

## 10. 架构与文件

```
src/workflow_controller/plugins/breather/
├── config.yaml            插件配置（§6）
├── plugin.py              BreatherPlugin：触发入口 + finalize_handoff 主流程 + 状态机
├── context.py             ContextTracker：上下文占比计算（events 免 fork / command fallback）
├── handoff.py             HandoffGenerator：prompt 模板加载 + 槽填充 + 新 session 配置生成
│                          + HANDOFF_AGENT_PROMPT（内置默认模板）
└── scripts/
    └── handoff_tools.py   3 个确定性盘点工具（自包含 CLI，零 opsbot 依赖）
```

关键集成点：

- **LoopEngine**（`core/loop.py`）：每个 iteration worker EOF 后经通用 `iteration_gate` 调 breather 的 `on_iteration_boundary`。返回 `IterationGateResult(action="stop")` = 已 handoff，loop 以 MANUAL_STOP 退出（`continue`/`None` = 继续）。
- **orchestrator**（`core/orchestrator.py`）：`_run_session_loop` 构造 LoopEngine 时注入通用 iteration-boundary 派发器（`_run_iteration_boundary_gates`，遍历所有 plugin 的 `on_iteration_boundary`，不再按名字找 breather）；`stop_session` / `start_session_with_config` / `wait_session_done` 供 finalize 调用——插件一律经 `PluginContext.services` 能力端口触达这些原语，不持有 orchestrator 对象。
- **两个 HTTP 入口**：hook server（默认 18765）的 `POST /breather`（CC hook / curl / 脚本）；web app 的 `POST /api/breather/handoff`（浏览器，经 `get_web_router`）。两者共享核心 `_request_handoff`。

---

## 11. 故障排查

| 现象 | 排查 |
|------|------|
| 上下文很高但 breather 不触发 | 看日志 `breather: ouro_id=… context=X/Y (Z%)` 确认占比计算（用量未知时显示 `unknown`，等模型上报后再判）；`context_window` 是否配错（如把 200k 模型配成 1M → 占比永远低）；`context_threshold_pct` 是否设太高 |
| handoff 进了 pending 但没完成 | 看是否 `handoff_wait_stop_timeout`（阶段一）或 `handoff_agent_timeout`（阶段二）太短；查 `{project_dir}/.opsbot/opsbot.log` 里 finalize 的降级日志 |
| 交接文档内容陈旧/丢失本轮工作 | 检查新鲜度校验日志 `未在本轮更新 …（exists=…, started_at=…）`——agent 可能超时没写完，或写到了错路径 |
| CLI runtime 下完全不触发 | 已由 loop 边界 check 解决（driver-agnostic）。若仍不触发，确认 breather 插件 enabled、orchestrator 注入了 hook（日志 `loading plugin breather`） |
| 起 worker-h2 失败反复降级 | 看 `起 worker-h2 失败 — 不停旧 worker` 日志的异常栈；通常是 session 配置（runtime/auth/skills_path）问题 |
| 调了 `force=true` 但没立刻 handoff | force 沿用等 stop 模式——只在 worker **下个自然停止点**才切，不是立即中断。看日志是否进了 pending（`force handoff for …`）；若 worker 调完 force 还在跑，handoff 推迟到它下次自然停止（watchdog 30min 兜底）。想尽快切就把 force 调用放在接近停止点的位置（如某步产物落盘后） |
```
