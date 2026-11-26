# 阶段门禁设计（staged_pipeline）

> 本文是 opsbot「阶段门禁」机制 + `staged_pipeline` plugin 的设计文档，自洽可读，不依赖任何本地稿。reviewer 的内部机制见 [架构文档](../ARCHITECTURE.md)，本文只在两层关系处引用。

## 0. 概览

每个领域把**阶段顺序 + 每阶段门禁**声明在**项目 `opsbot.yaml`** 里（`stages:`），由一个通用的 `staged_pipeline` plugin 在 iteration 边界**确定性强制**——worker 跳步就被拦回、阶段没全过门禁就不许结束；项目没声明 stages 则门禁惰性、不套任何阶段。**核心仍是 iteration 粒度**（Workflow Controller 既定原则不动），「阶段」是项目配置概念。

**它怎么跑**：worker 每轮做一个阶段、停下 → 边界门禁从磁盘现推「已过的阶段数」→ 跳步/未做则 `block` 回注、某阶段刚过则交 reviewer per-stage 语义审、全过则放行完成。一次边界的完整走查（含 `block` 跨 iteration 与两检查点事件时序）见 §4.4。

**两层判定**（核心模型，§3 详述）：门禁查**结构**（产物在不在 / 非占位 / 命令 rc==0，确定性，iteration 边界、reviewer 之前）= 硬地板、必要条件；reviewer 查**语义**（产物真满足 `task_goal` 吗，LLM，门禁 defer 时 per-stage 跑）= 判 approve/reject。两者查不同问题、用不同料、在不同时机，互补不重叠。

**移动部件**：项目 `opsbot.yaml` 的 `stages:`（领域声明，喂门禁）+ `staged_pipeline` plugin（一道门，`on_iteration_boundary`）+ orchestrator 派发器（first block/stop wins）+ boundary reviewer（`review_stage`，per-stage 语义审）。stages 归项目，没声明则门禁惰性；加新领域 = 新项目 yaml 写 stages，零动核心、零复制 plugin。

## 1. 背景与动机

Workflow Controller 是通用 AI-agent 调度框架（Loop Engine + Reviewer + 控制面 + Driver 抽象），领域无关。算子生成是首个 use case，但其领域逻辑原本散在 prompt/skill 里（`4495d71` 已把默认 `task_goal`/`starting_prompt` 中性化）。要支持**多个特定领域**，需要一条干净的路：每个领域把「worker 该干什么 + 什么时候算干完 + 阶段顺序」收拢成**项目配置 + 一个通用门禁 plugin**，加新领域不动核心、不动别的领域（甚至不复制 plugin）。

CLAUDE.md 早就把「per-plugin 阶段门禁」标为未来方向。本文把它落地。

### 1.1 关键澄清：我们要「硬」，不是 migbot 的「软」

参考的 migbot 5 阶段（`spec→plan→execute→verify→retrospect`）顺序**只写在 reviewer 提示词里**，代码无阶段路由；它的「强制」靠另一个 reviewer LLM **主观判**「5 阶段是否按序完成」——不可靠（worker 能用漂亮叙述骗过 LLM）。我们要的**强制顺序是硬的、确定性的**：查磁盘产物存在性 + 非占位，不靠 LLM 判。比 migbot 强，也呼应「客观信号优先、主观环节才用 reviewer」的工程论点。

### 1.2 底子已有 ~80%

- Plugin 系统完整（base ABC + registry + SDK + hook 拦截器 + 观察者）。breather 是唯一真实 plugin，是任何 domain plugin 的模板。
- **完成门禁在本次改动前就零改能做**：`on_stop_hook` 返回 `block` + reason，orchestrator 已翻成「喂 feedback 让 worker 继续」；plugin 拦截器在 reviewer **之前**跑、first-non-None 短路 reviewer。

真正缺的一处：breather 影响 loop 走向的 loop-boundary 门禁，在 orchestrator 里是**硬编码按名字找 `"breather"`** 的——不在 Plugin 接口里，别的插件复用不了。**把它提升成通用 hook，是核心侧唯一要动的中枢。**

### 1.3 目标 / 非目标

**目标**：
- 把「阶段顺序 + 每阶段门禁」收拢成**项目配置（`stages:`）+ 一个通用门禁 plugin**，加新领域零动核心、零复制 plugin。
- **确定性硬强制**：顺序与完成门禁查磁盘产物（存在 / 非占位 / 命令 rc==0），不靠 LLM 主观判（§1.1）。
- **driver-agnostic**：CC / opencode / CLI 三个 runtime 行为一致（§4.4）。
- 逐阶段检查点（结构门禁 + reviewer 语义），面板可见。

**非目标**：
- workflow 的**丰富内部**（并行子任务、迭代重试 ≤N、条件分支、subagent 调度）——归 worker 加载的**领域 skill**，不进 stages（§5.4 关键分工）。
- **每次文件写即判**的细粒度门禁（需 PostToolUse 观察者，即 `bus.observe(TOOL_RESULT, ...)`）——不做，代价大于收益（§7）。
- **per-stage 选择性人审**（某些阶段人审、其余自动）——未来 `gate: human`，不在 reviewer 模式里（§4.4）。
- 非线性 stages（DAG / 条件跳过 N/A）——stages 是线性序，可选分支靠「不列该 stage」表达（§5.4）。

## 2. 核心机制：通用 iteration-boundary hook

### 2.1 新增结果类型 `IterationGateResult`

`packages/kernel/src/workflow_controller/plugin/base.py`：

```python
@dataclass
class IterationGateResult:
    action: Literal["pass", "block", "stop"]
    feedback: str = ""   # action="block" 时，作为下轮 user 消息回注
    reason: str = ""     # 日志 / LOG_LOOP 事件的短标签
```

三个动作：

| action | 含义 | loop 后果 |
|---|---|---|
| `pass` | 无意见（默认返回值） | 正常往下走（stop 检查 / WAITING_RETRY / reviewer / evaluator） |
| `block` | 拦住，回注 feedback | **跳过 reviewer + evaluator**，带 feedback 强制再跑一轮（阶段重定向） |
| `stop` | 结束 loop | 返回 `LoopStatus.MANUAL_STOP`（如 breather handoff） |

### 2.2 新增 async hook `on_iteration_boundary`

`Plugin` 基类新增可选 override（默认返回 `IterationGateResult(action="pass")`）：

```python
async def on_iteration_boundary(
    self, session_id: str, session: Any, iteration: int,
) -> IterationGateResult: ...
```

`async` 是因为 loop 在 async 上下文里 `await` 它（且 breather 的等价逻辑本就是 async——要做上下文检查/handoff）。worker 子进程每轮自然 EOF 后、reviewer/evaluator 之前触发。

### 2.3 orchestrator 派发器

`_run_iteration_boundary_gates(mig_session_id, session_obj, iteration)`（`orchestrator.py`）镜像已有的 `_run_plugin_stop_interceptors`：遍历 `self._plugin_registry.plugins`，`await` 每个 `on_iteration_boundary`，**first block/stop wins**（`pass` 跳过），吞掉单个 plugin 的异常（一个坏 plugin 不影响别的）。派发器自身返回 `IterationGateResult | None`——`None` 表示所有 plugin 都 `pass` 且无 pending boundary 工作（loop 据此跳过 block/stop 分支、正常往下走）。注意「移除 `| None`」只发生在 hook 层（plugin 的 `on_iteration_boundary` 非 Optional、无意见用 `pass`），派发器层仍保留 `None` 这一"无工作"信号。

**关键**：派发器取代了旧的 `p.config.name == "breather"` 名字查找——核心从此**不认识任何具体 plugin**。

### 2.4 loop 消费

`LoopEngine` 的 `breather_check` 参数改为通用的 `iteration_gate`。iteration 边界（worker EOF 后）调用派发器：

- `stop` → 发 `LOG_LOOP`（`reason: gate.reason`）+ `return LoopResult(MANUAL_STOP)`。
- `block` → `feedback = gate.feedback` + 发 `LOG_LOOP` + 进下一轮（跳过 reviewer/evaluator，带 feedback）。
- `pass` → 正常往下。

### 2.5 breather 适配

breather 实现 `on_iteration_boundary`：上下文超阈值 → 发起 handoff + 返回 `IterationGateResult(action="stop", reason="breather_handoff")`，否则 `IterationGateResult(action="pass")`。handoff 时发 `LOG_LOOP {reason:"breather_handoff"}` + `LoopStatus.MANUAL_STOP`（见 §2.4 stop 分支）。

## 3. 完成判定：门禁 + reviewer（+ evaluator 的位置）

完成判定有多个判定环节，各答不同的问题、用不同的料、在不同的时机跑。核心两层:

| | 答的问题 | 料 | 时机 | 能做什么 |
|---|---|---|---|---|
| **门禁**（确定性） | 领域**要求**的产物齐了吗？顺序对吗？（**结构**） | 项目 `stages`（`on_iteration_boundary` 读 `session.config.stages`） | **iteration 边界**（worker 每轮 EOF 后），reviewer/evaluator **之前** | 只能 **block**（拦）或 defer，不会自己放行 |
| **reviewer**（LLM） | worker 说「做完了」这话**真**吗？（**语义**） | 项目 `task_goal` + artifact-facts 真值表 + worker 汇报 | 门禁 defer（0<k<总）时做 per-stage；全过时 let_exit | 判完成：let_exit / 拦回重做 |

**组合规则**：门禁是**必要条件**（硬地板）。门禁 `block`（跳步 / k==0）→ reviewer 和 evaluator **都不跑**（短路，省 token），带 feedback 进下一轮；门禁 defer（0<k<总，某阶段刚过结构门禁）→ 交 boundary reviewer 做 per-stage 语义评判（approve→推进下一阶段 / reject→重做），仍是 `block`；全过（k==总）→ 置 `stages_complete` → auto 模式 `mark_reviewer_let_exit` → loop COMPLETED。

**为什么不冗余**：门禁查**结构**（文件在不在、非占位、按序，确定性），reviewer 查**真相**（活儿真满足目标吗，概率性）。worker 能过一个 fail 另一个——产物齐但 spec 写错了（门禁过、reviewer 拦）；或嘴上说完了但没产出 `spec.md`（reviewer 可能被漂亮叙述忽悠，门禁拦死）。**互补，不重叠**。所以 reviewer **不看 stages**——门禁已经确定性查过，reviewer 再查是拿概率重做确定性的事，只会更差。`stages` 喂门禁、`task_goal` 喂 reviewer，是干净的职责切线。

**「两层」是职责层，不是时序层**（staged 里尤其要厘清）：通用 loop 有 turn、无阶段，reviewer 判每轮 turn 完成没——这时 reviewer 的 turn 级角色清晰。但 staged 把「一个阶段」绑到「一个 iteration」（worker 一轮一阶段），所以**阶段边界就是 turn 边界**——门禁和 reviewer **收敛到同一边界**，是同一轮的两个判定（结构 + 语义），不是两个先后跑的时序层。也因此 reviewer 在 staged 里**自己降级**成 per-stage 语义审（`review_stage`，判单个阶段），不再判「整体 turn 完成」（那是门禁 + `stages_complete` 干的）。

reviewer 的内部机制（裸 HTTP / 固定请求形状 / 与 runtime 解耦）见 [架构文档](../ARCHITECTURE.md)。

### 3.1 evaluator 在 staged 场景的位置（三者优先级链）

Workflow Controller 的 loop 在每个 iteration 末尾有一条**判定优先级链**（`loop.py:run_loop`，worker 自然 EOF 之后），evaluator 排在**最后**:

| 顺序 | 环节 | 短路 evaluator? |
|---|---|---|
| ① | **iteration_gate**（`on_iteration_boundary`，driver-agnostic） | `block` → 进下一轮，跳过下面全部 |
| ② | stop_event / WAITING_RETRY / manual mode | manual 跳过 |
| ③ | **reviewer_let_exit**（auto 模式 reviewer 判完成） | COMPLETED，跳过 evaluator |
| ④ | **evaluator**（`evaluator.evaluate`） | — 它自己 |

**staged 场景下 evaluator 几乎从不被触及**——① 和 ③ 把它短路了。worker 每轮做一个阶段、停下 → ① 算 `k = completed_prefix`：`k==0` / 跳步 → `block`（短路 ④）；`0<k<总` → defer 给 boundary reviewer per-stage → 仍 `block`（短路 ④）；全过 → 置 `stages_complete` → ③ `let_exit` → COMPLETED（短路 ④）。

所以 staged-pipeline 的完成语义由**门禁（结构）+ reviewer（语义）**接管，evaluator 退居后备——只在门禁和 reviewer 都放行的边界情况、或最终 `max_iterations` 硬封顶时间接起作用。

**为什么这样设计**：evaluator 是扁平的「这一轮完没完」判定（一个 `is_complete` bool），**它不懂阶段**；staged 的完成语义是「分阶段、每阶段都得过语义审」——evaluator 的模型表达不了。所以完成判定责任从 evaluator 转移到门禁+reviewer，否则一个 `PassThroughEvaluator`（`is_complete=False`）会让 loop 只在 `max_iterations` 停，把 reviewer 的判定整个吞掉（见 `loop.py` reviewer_let_exit 分支注释）。

**反过来，通用 loop（无 stages）里 evaluator 是主裁判**：① 返回 None（无 staged plugin block）、③ 不触发（无 stages_complete），完成判定落到 ④ evaluator（passthrough / feedback / always_complete / max_iteration 四种）。这正是 evaluator 的本职——staged 场景只是被 plugin 接管了。

## 4. `staged_pipeline` 门禁执行器 plugin

`packages/plugins/staged-pipeline/src/workflow_controller/plugins/staged_pipeline/`——一个**通用**门禁执行器：读 session 带来的领域 stages，确定性强制顺序 + 完成门禁。**stages 不在 plugin 里**——在项目 `opsbot.yaml`（经 session 传进来，§5）；plugin 只提供门禁逻辑。默认 `enabled: false`。

```
staged_pipeline/
├── config.yaml     # 只有 name/version/enabled（stages 不在这）
└── plugin.py       # StagedPipelinePlugin：一道门(on_iteration_boundary)，stages 从 session 读
```

阶段约定的求值函数（artifact 规范化/展示、门禁求值、阶段派生 + 跳步检测）住 SDK 中立模块 `packages/kernel/src/workflow_controller/plugin/stages.py`——stages 是项目 `opsbot.yaml` 约定、非插件私有，orchestrator 的边界 review 文案、reviewer 的产物真值表、进度 API 与本 plugin 同源求值（核心与插件 import SDK 均合法）。求值函数全是纯函数（无副作用、易单测），`plugin.py` 只做编排（何时 block/defer + 纪律 prompt 注入）——匹配 breather 的分层风格。

### 4.1 门禁三模式（config 数据驱动）

每个 stage 在 `artifacts:` 下挂**一个或多个 artifact entry**，每条 `{artifact, gate, command}` 独立设门禁；stage 的**全部** artifact entry 都过门禁才算该 stage 过（`evaluate_gate` 全过才 True）。`artifact` 字段还支持**字符串列表缩略写法**——多个路径共享同一 `gate`/`command`，展开为多条 entry。门禁三模式（写在**项目 opsbot.yaml 的 `stages:`**）：

| 模式 | 判定 |
|---|---|
| `exists` | artifact 路径存在 |
| `non_placeholder` | 存在 + 非占位（size < 32B 或整段是 stub token 如 `todo`/`占位`/`stub` → 视为占位空壳） |
| `command` | 在 cwd 跑 `command`，exit 0 = 通过 |

- **样例项目**（`examples/staged-pipeline/opsbot.yaml`）全用 `non_placeholder`（领域无关、能端到端跑）。
- **`command` 是真实领域接客观信号的口子**：operator-gen 的 verify 会写 `gate: command, command: "pytest -q && python check_precision.py"`。同一逻辑适用任意阶段（真实领域可给 `spec`/`plan` 也加 `command`）。`command` 来自项目作者写的 opsbot.yaml（可信源，非 worker/LLM 输入）；实现以 `shell=False` 调用固定 `/bin/bash -c` 参数数组，在保留 bashism 的同时避免 shell 选项注入面。**完整算子实例**（把 spec 9-stage / cp2 / cp3 三道 validator 接成 `command` 门）见 [`examples/ascendc-operator/`](../../examples/ascendc-operator/)。

领域表达「我的阶段怎么算完」**改项目 opsbot.yaml、不写代码、不复制 plugin**。

### 4.2 阶段派生（无状态、确定性）

阶段派生求值（`opsbot.plugin.stages`）每次从磁盘现推，不存「当前第几阶段」——重启不丢、无状态可腐化：

- `completed_prefix(stages, cwd)` = 从头连续满足门禁的阶段数（遇第一个未满足即停）。
- `detect_jump(stages, cwd, prefix=None)` = current 阶段门禁未过、但**更后面**阶段的产物已出现 → `(current, jumped)`；否则 `None`。用 artifact「存在」（非门禁通过）判定跳步——后置产物提前落盘本身就是顺序违规信号。`prefix` 复用调用方已算好的 `completed_prefix`，门禁单次求值（`command` 门要起 shell，逐分支重复求值代价高）。

### 4.3 一道门（`on_iteration_boundary`，stages 从 session 读）

**一道门，长在 iteration boundary**（driver-agnostic，框架唯一控制点）。`on_iteration_boundary` 开头先 `_stages_for(session)` 从 `session.config.stages` 读阶段声明；**没声明 stages（`[]`）→ 直接 `return IterationGateResult(action="pass")`（惰性，不套任何阶段）**——这是「stages 归项目」的核心保证，plugin 不会把任何阶段强加到没声明的 session（别的项目不受影响）。

stages 非空时，算 `k = completed_prefix(stages, cwd)`，按阶段状态决定（确定性结构判定；语义判定交 reviewer）：

| 情况 | 动作 |
|---|---|
| **全过**（k==总） | 置 `session.stages_complete = True`、返回 `pass` → orchestrator boundary 闭包看到信号，`mark_reviewer_let_exit` → loop COMPLETED（auto/manual 均如此——结构门禁是客观的，无需人工判断，不等 manual prompt）。确定性完成，不依赖 evaluator 配置（避免 PassThrough 下 k==总 死循环）。 |
| **跳步**（`detect_jump`：后置阶段产物已落盘而前置门禁未过） | `block` + 回注「先完成 `{current}`，你跳到了 `{later}`」。 |
| **一个阶段都没过**（k==0） | `block` + 回注「先做 stage 0（产出 `{artifact}`），做完停下汇报」。 |
| **某阶段刚完成、还没全过**（0<k<总） | 把 `{done: stages[k-1], next: stages[k], done_index: k-1}` 写到 `session.pending_stage_review`、**返回 `pass`**（defer）→ orchestrator 的 `_run_stage_review_at_boundary` 跑 per-stage reviewer 语义评判：approve → `block` 回注「做下一阶段」、reject → `block` 回注「重做该阶段」。见 §4.4。 |

只有结构判定会 `block`（确定性硬地板）；语义判定（该阶段产物真不真）交给 reviewer。门禁查结构、reviewer 查语义，互补不冗余（§3）。

### 4.4 逐阶段检查点怎么跑（+ 为什么统一到 iteration boundary）

**一次 iteration boundary 的走查**（block 跨 iteration、下一轮 fresh subprocess `--resume`）：

```
worker subprocess (iter N) ── 写阶段产物、停下、EOF ──┐
                                                      ▼
LoopEngine._run_iteration (worker EOF 后)
  │
  └─► gate = orchestrator._run_iteration_boundary_gates(session, N)
        │
        ├─► 每个 plugin: await on_iteration_boundary(session)
        │     └─► StagedPipelinePlugin: k = completed_prefix(stages, cwd)
        │          ├─ k==总(全过)  → stages_complete=True, return pass
        │          ├─ detect_jump → return block("先做 {current}")
        │          ├─ k==0        → return block("先做 stage 0")
        │          └─ 0<k<总      → set pending_stage_review, return pass(defer)
        │
        ├─ 有 plugin block/stop? → first wins → return 该 result
        └─ 全 pass → _resolve_boundary_pending:
             ├─ pending_stage_review 非空 → _run_stage_review_at_boundary
             │    ├─ manual / 无 reviewer → feedback=「做下一阶段」推进文案
             │    └─ auto → await review_stage(...)        ◀ ~LLM 时长
             │    └─ emit gate_pass + emit reviewer verdict → return block(feedback)
             └─ stages_complete → mark_reviewer_let_exit → return None (→ COMPLETED)
  │
  ├─ gate == None  → 正常往下 (stop_event / WAITING_RETRY / manual / let_exit / evaluator)
  ├─ gate.stop     → LOG_LOOP, MANUAL_STOP, 结束
  └─ gate.block    → _pending_gate_feedback=feedback; LOG_LOOP("continue")
                       │
                       ▼
          下一轮 iter N+1: fresh worker subprocess --resume native_session_id
                           feedback 当本轮 prompt（程序化，无 "User intervention:" 前缀）
```

**一轮一阶段**：`starting_prompt`（+ plugin 注入的逐阶段工作纪律）让 worker **每完成一个阶段就停下汇报**，不连做下一个。worker 自然 EOF → loop 到 iteration boundary → 门禁算 `k`：

- 跳步 / k==0 → `block` 回注，loop 带 feedback 进下一轮（fresh `--resume` subprocess，续同一 SDK session）。
- 0<k<总 → defer，boundary reviewer per-stage 审（结构门禁已过 → 发 `gate_pass` 检查点 → reviewer `review_stage`）→ `block` 回注推进/重做。
- 全过 → `stages_complete` → reviewer `let_exit` → COMPLETED。

N 个阶段 = N 个 iteration，每个 iteration 的 boundary 产出**两个检查点事件**（门禁与 reviewer 是两个阶段，不是一个同步成对的检查点）：

- **门禁结构**（`stop_hook_plugin`，cyan）：worker EOF、算完 `completed_prefix`、结构门禁过的**真实时刻**就 emit（微秒级磁盘判定）。与 gate-block（跳步/k==0）的 emit 时机对称——门禁过与不过同等对待，都不 defer。
- **reviewer 语义**（`stop_hook_llm`，indigo；manual 模式为 `stop_hook_plugin`）：`review_stage` LLM 跑完的**真实时刻**才 emit，auto 模式下晚于门禁事件约一个 LLM 调用时长（timeout 默认 300s、算子场景 600s 都可能）。

两事件**不靠时间戳同步**：门禁事件不 defer 到 reviewer 之后才发。配对靠事件携带的 `stage_name`/`stage_index`——前端据此归成一张「阶段检查点卡」；门禁事件先到、reviewer 判定未到时卡显「reviewer 思考中…」态，把 LLM 等待这段静默变可见进度（否则慢审一个阶段 600s 期间面板全空）。取舍：不为「让两事件看起来成对」而伪造门禁事件的时间戳——① 事件流/replay/调试依赖真实时间，defer 会让「门禁何时过」晚报一个 LLM 时长；② LLM 等待本身是信号（reviewer 在想），藏起来反而损信息；③ 与 gate-block 已有的真实时刻 emit 对称（门禁过 / 不过应同等对待）。

> **检查点 emit 的分工**（`_run_iteration_boundary_gates` 派发器 + reviewer 路径内部，对齐 [ARCHITECTURE.md §3 翻译/副作用分层](../ARCHITECTURE.md)）：`InterceptResult.source`（插件名）是派发键——插件拦截器 `_on_iteration_boundary_intercept` 给 block/stop 结果带 `source=self.config.name`，派发器见 `source` 非空才 `emit` 门禁检查点（jump/k==0 等插件 gate）；reviewer 拦截器结果不带 `source`，派发器跳过、由 `_run_stage_review_at_boundary` 内部自己 `emit` 其检查点（skip 的「已审继续等」、advance 的 `gate_pass` 门禁结构 + `stop_hook_llm` reviewer 语义判定）。两路互斥、不重入——否则同一 decision 落库两条 `log_auto`，前端看到重复行。

> 实现待对齐：`_run_stage_review_at_boundary` 当前仍把 `gate_pass` defer 到 `review_stage` **之后**才 emit（代码注释里「成对 timestamp」是旧设计，本节推翻它）。按本节应改为：结构门禁一过即 emit `gate_pass`，reviewer 判定在其真实完成时刻 emit，两事件加 `stage_name`/`stage_index` linking 字段供前端配对。

**reviewer 模式三分支**（0<k<总、结构门禁过之后，[orchestrator.py:`_run_stage_review_at_boundary`](../../packages/kernel/src/workflow_controller/core/orchestrator.py)）：staged 里 turn=阶段，所以「人审 turn」=「人审阶段」，per-stage 判定由全局 reviewer 模式决定：

- **auto**：reviewer `review_stage` 语义审 → approve（block 推进下一阶段）/ reject（block 重做）。
- **manual**：**不**自动推进——emit「⏸ 阶段 X 已过门禁，等人工审查」+ `return None` → 落 loop 的 manual 分支等面板输入；人输入「通过」推进 / 指出问题重做 → feedback → 下一轮。让人逐阶段把关（替代 auto 的 LLM 审）。
- **reviewer.enabled=false**（无 reviewer）：门禁结构自动驾驶，block 推进下一阶段（无语义审）。

**去抖（跳过重审）**：boundary 记 `session._last_reviewed_stage_index`（最近审完的阶段 idx）。worker 被推进后本轮若还没产出下一阶段产物、disk 上 `k` 不变 → 下次 boundary 会再次落到同阶段的 `pending_stage_review`；此时 `done_index <= _last_reviewed_stage_index`，直接 block「继续等下一阶段产出」、**不重复调 reviewer**——避免 worker 还在做时反复触发同阶段语义审烧 token。首次审某阶段时才把 `_last_reviewed_stage_index` 置为该 `done_index`。

manual 是**全局**（所有阶段都停等审）；若想要 per-stage 选择性人审（某些阶段人审、其余自动），那是未来的 `gate: human`（human gate）的事，不在 reviewer 模式里。

**block 跨 iteration**（关键设计）：每个 `block` 经 loop 的 feedback 注入（`{{feedback or '继续。'}}`）成为下一轮 prompt，下一轮 spawn fresh worker subprocess `--resume` 同一 `native_session_id`——所以 N 个检查点跨 N 个 iteration、N 次 subprocess 调用，**不是**同一 turn 内的连续 block。

**driver-agnostic（CC / opencode / CLI 一致）**：这是统一到 iteration boundary 的根本原因。早期版本门禁用 `on_stop_hook`：CC 的 Stop-hook `block` 返回值能重定向**活 worker**（把 decision 当「带 reason 继续本轮」，5 检查点全在同 turn）；但 **opencode 的 stop 返回值被丢弃**（worker 已退出，事件到达在退出之后）→ stop-hook 门禁**对 opencode 无效**。统一到 iteration boundary 后，三个 runtime 都走「block → 下一轮新 subprocess」，行为一致。

**stop-hook 对 staged session 退化为「让 turn 结束」**：orchestrator 的 `_decide_stop` 对 staged session 不再自己判完成，而是顺手把 worker 汇报捕获到 `session.last_worker_text` 供 boundary reviewer 当 context；stop-hook 返回值只让当前 turn 结束，完成判定交给下一轮 boundary。这样 stop-hook 不再承担门禁职责，CC/opencode 行为统一。

**仍不保证的**（诚实标注）：逐阶段检查点是「每阶段停一次」粒度，不是「每次文件写即判」。若 worker 不听 prompt、一次 query 里连做多阶段再停，中间阶段会跳过 per-stage reviewer——但完成门禁 + 跳步检测仍保证「不能带着跳步/未完成退出」。要真正「每次文件写即判」需 PostToolUse 观察者（`bus.observe(TOOL_RESULT, ...)`），仍不做（代价大于收益）。

**对通用 workflow 零影响**：`on_iteration_boundary` stages 空 → 惰性 `pass`；`pending_stage_review` 只在 stages 非空且 0<k<总时置位；orchestrator boundary 闭包（`_resolve_boundary_pending`）是纯加法；reviewer 的 `review_stage` 是扩展，现有 `decide_stop` 不动。无 stages 的 session 逐阶段机制整个是 no-op。

## 5. 使用

### 5.1 启用门禁执行器（全局，一次）

staged_pipeline 默认 `enabled: false`，**不启用就不加载、门禁不生效**。在 web Settings → Plugins 找到 `staged_pipeline` 点 **Enable**（toggle 热加载、免重启）；或手编 `packages/plugins/staged-pipeline/src/workflow_controller/plugins/staged_pipeline/config.yaml` 的 `enabled: true` + 重启 web。

启用是**全局**的——但只对**声明了 stages 的项目**生效（没声明的 session 门禁惰性，§4.3）。所以启用一次即可，不用按项目开关。

### 5.2 配项目（stages + starting_prompt + task_goal，同在 opsbot.yaml）

阶段顺序的**指引**（prompt）和**门禁**（stages）是两套东西，但都写在项目 `opsbot.yaml` 里——天然自洽（门禁查的结构 = prompt 要 worker 做的阶段）。完整可跑样例见 `examples/staged-pipeline/`（README 讲运行/观察、`sample-project/` 给具体任务、`opsbot.yaml` 是门禁 + 任务声明）；最小 stages 片段：

```yaml
runtime: claude_sdk
starting_prompt: |
  按 spec → plan → execute → verify → retrospect 的顺序推进……（见下面 stages）
reviewer:
  task_goal: <这个项目的总体目标>
stages:
  - name: spec
    artifacts:
      - {artifact: spec.md, gate: non_placeholder}
  - name: plan
    artifacts:
      - {artifact: plan.md, gate: non_placeholder}
  - name: verify
    artifacts:
      - {artifact: verify.md, gate: command, command: "pytest -q"}
  # …
```

- `stages` → 喂**门禁**（结构：产物在 / 非占位 / 按序）。
- `task_goal` → 喂 **reviewer**（语义：做完了这话真不真）。
- `starting_prompt` → 告诉 worker 按阶段顺序推进；通用的「逐阶段工作纪律」（一轮一阶段/写完停/跨阶段=跳步白做）由 plugin 经 `prompt_prefix` 自动 prepend 到本 prompt 前，domain 的 `starting_prompt` 只写领域内容（任务 + 每阶段产物该装什么）。
- `cwd` = web 里选的项目目录，`spec.md` / `plan.md` / `impl/` … 写在那儿。

### 5.3 跑 & 在 web 里看什么

主面板**选这个项目目录**（web 会读它的 `opsbot.yaml`，把 `stages` 注入 session）→ Start。loop 在**每轮 iteration 边界**（worker 每轮做完一个阶段、停下后）查门禁——顺序 + 完成都在这一个 hook（§4.3）：

- **跳步**（如直接写 execute 产物但 plan 没过门禁）→ `block`，回注「先完成 `plan`，你跳到了 `execute`」，loop 带 feedback 再跑一轮——在 web 的 **Claude Output / Interactions** 能看到这条回注。
- **某阶段刚完成（0<k<总）** → 结构门禁过，交 boundary reviewer per-stage 语义审：过 → `block` 回注「做下一阶段」；不过 → `block` 回注「重做该阶段」。
- **产物全过门禁（k==总）** → 置完成信号 → auto 模式 reviewer `let_exit` → loop `COMPLETED`。

选**别的项目**（没声明 stages）→ 门禁惰性，完成纯靠 reviewer，**不会被强行套阶段**。

> 门禁只在 **iteration 边界**生效：单次 agent-loop 内的跳步要等下一轮边界才发现（§4.4 / §7）。

### 5.4 加一个新领域

**配方**：新项目 `opsbot.yaml` 写三件——`stages`（喂门禁）+ `starting_prompt`（喂 worker）+ `reviewer.task_goal`（喂 reviewer）。零动核心、零 plugin 复制，SDK 阶段求值函数领域无关原样复用，换阶段名 / artifact / 门禁模式全在 yaml。

**关键分工**（复杂领域要厘清）：opsbot 只承载**线性门禁骨架**（顺序 + 机器门 + per-stage reviewer）；workflow 的**丰富内部**（并行子任务、迭代重试 ≤N、条件分支、subagent 调度）由 worker 加载的**领域 skill** 承载，不进 stages。所以 stages 的粒度 = workflow 里「禁止进入 X」的**硬门界**，不必是 macro 阶段的 1:1——一个 macro 阶段内部若有硬门（如「spec 未过禁止进入 design」），就拆成两个 stage。

**实例**：复杂领域样板（AscendC 算子，11 阶段镜像 cannbot registry workflow + `gate: command` 硬化 spec 9-stage / cp2 / cp3 validator）见 [`examples/ascendc-operator/`](../../examples/ascendc-operator/)——`opsbot.yaml`（可直接拷贝到算子工作目录改占位）+ `README.md`（用法 / 前置 / 取舍）。

## 6. 测试覆盖

| 测试文件 | 数 | 覆盖 |
|---|---|---|
| `test_stages.py` | 44 | SDK 求值纯函数：stages 声明校验（`declared_stages` 滤缺 name / 缺空 artifacts / 非 dict 条目，全无效 → 空）、artifact 规范化（多 artifact 展开、`artifact` 列表缩略写法）与展示、门禁 exists（文件 + 目录）/ non_placeholder（缺失 / 占位 token / 实质 + 目录）/ command / `evaluate_gate` 派发、阶段派生 completed_prefix / detect_jump（含 prefix 复用） |
| `test_staged_pipeline_plugin.py` | 12 | 门禁（`on_iteration_boundary`）从 `session.config.stages` 读；**无 stages → 惰性**（含 session 无该属性的防御）；`prompt_prefix` 纪律注入与 compose prepend |
| `test_stages_config.py` | 6 | config plumbing：`AppConfig.from_yaml` 解析 stages、`SessionConfig` 携带、默认空 |
| `test_iteration_boundary_gate.py` | 2 | SDK 约定回退：`IterationGateResult` 字段默认值、基类 `on_iteration_boundary` 未覆写时 `pass` |
| `test_plugin_toggle.py` | 6 | web toggle 定点改 `enabled:` 行保注释（嵌套 / 注释行里的 enabled 不被动） |

> 历史注：3 例端到端组合测试 `test_staged_pipeline_integration.py`（真 plugin + 真 dispatcher + 真 LoopEngine + fake worker）并非随本次重构移除——先行合入的 !176（commit `b1efc80`，清理 outdated iteration-boundary dispatcher 测试）已删除该文件；其组合的各环节部分由上表单测分摊覆盖（求值 = `test_stages.py`、plugin 门 = `test_staged_pipeline_plugin.py`、SDK 约定 = `test_iteration_boundary_gate.py`）；派发器与 LoopEngine 的串联消费暂无单测覆盖。

> 想不耗 LLM 看门禁的确定性证明：`pytest packages/kernel/kernel_tests/unit/run_plane/test_stages.py packages/plugins/staged-pipeline/tests/test_gates.py packages/kernel/kernel_tests/unit/run_plane/test_iteration_boundary_gate.py -q`。真实多阶段跑见 [`examples/staged-pipeline/`](../../examples/staged-pipeline/)（README 讲运行 / 观察）。

## 7. 限制与已知边界

- **iteration 粒度强制**（§4.4）：单次 agent-loop 内的跳步要到下次边界才发现；门禁保证「退不出去」，不保证「每次工具调用合规」。
- **plugin 注入通用工作纪律（非领域知识）**：`Plugin.prompt_prefix` 是一等 hook，`compose_initial_prompt` 在 session 启动时把各 plugin 的 prefix（load order）prepend 到项目 `starting_prompt` 前。`StagedPipelinePlugin` 注入「逐阶段工作纪律」（一轮一阶段/写完停/跨阶段=跳步白做/不要占位）——这套描述的是 stage 机制本身、不是领域知识，归 plugin 拥有；domain 的 `starting_prompt` 只写领域内容（任务 + 每阶段产物该装什么），不必复制纪律。stages 未声明时 `prompt_prefix` 返回 None（compose 跳过，零影响）。
- **PostToolUse 细粒度跳步检测**（每次文件写即判，`bus.observe(TOOL_RESULT, ...)`）：本次不做。
- **样例项目的真实客观 verify**（真跑测试框架）：样例用 `non_placeholder`，`command` 留口子（`examples/staged-pipeline/opsbot.yaml` 注释里给了换法）。
- **多 plugin 组合优先级**：first block/stop wins（`pass` 跳过，按 load order），机制与 stop 拦截器的 first-non-None 同形；breather handoff 的 `stop` 与 staged 的 `block` 谁先由 load order 定，不特殊处理。
- **`block` 无内部计数上限**：误写的 plugin（总返回 block）会把 loop 困到 `max_iterations`。`max_iterations` 是后备。breather 的 stop 有 `_decide_stop` 的 block 上限，iteration-gate 的 block 没有——因为「该不该限」是 plugin 策略，loop 不该持有。

## 8. 关键文件

| 文件 | 职责 |
|---|---|
| `packages/kernel/src/workflow_controller/plugin/base.py` | `IterationGateResult` + `on_iteration_boundary` async hook + `prompt_prefix` hook + `compose_initial_prompt` |
| `packages/kernel/src/workflow_controller/plugin/sdk.py` | re-export `IterationGateResult` |
| `packages/kernel/src/workflow_controller/core/orchestrator.py` | `_run_iteration_boundary_gates` 派发器；`_resolve_boundary_pending` / `_run_stage_review_at_boundary` 闭包；注入闭包（取代 name-coupled breather 查找） |
| `packages/kernel/src/workflow_controller/core/loop.py` | `iteration_gate` 参数；消费 `block`/`stop`/`pass` |
| `packages/plugins/breather/src/workflow_controller/plugins/breather/plugin.py` | `on_iteration_boundary`（handoff→stop）；旧 `loop_boundary_check` 已删 |
| `packages/kernel/src/workflow_controller/plugin/stages.py` | 阶段约定共享求值面：artifact 规范化 + 展示（多 artifact 展开、`artifact` 列表缩略写法）、门禁求值（exists/non_placeholder/command；stage 全 artifact 过才 True）、阶段派生 + 跳步检测、stages 声明校验——核心（边界 review 文案、reviewer 产物真值表、进度 API）与 staged_pipeline plugin 同源消费 |
| `packages/kernel/src/workflow_controller/config/settings.py` | `AppConfig.stages` / `SessionConfig.stages`（per-project 阶段声明） |
| `packages/plugins/staged-pipeline/src/workflow_controller/plugins/staged_pipeline/plugin.py` | `StagedPipelinePlugin`：一道门 `on_iteration_boundary`（顺序+完成+defer per-stage）+ `prompt_prefix` 注入通用工作纪律；stages 从 `session.config.stages` 读（无则惰性） |
| `packages/plugins/staged-pipeline/src/workflow_controller/plugins/staged_pipeline/config.yaml` | name/version/enabled（默认 `false`；stages 归项目 opsbot.yaml） |
| `packages/kernel/src/workflow_controller/core/reviewer_service.py` | `review_stage`：per-stage 语义审（approve→推进 / reject→重做），总是 block_with_feedback |
| `examples/staged-pipeline/opsbot.yaml` | migbot 5 阶段可跑样例（stages + starting_prompt + task_goal） |
| `examples/ascendc-operator/` | AscendC 算子复杂领域样例（11 阶段镜像 cannbot registry workflow + `gate: command` 硬化 spec/cp2/cp3 validator；非自包含，需 NPU+cannbot） |
