---
type: design
title: "Codex CLI Driver 设计"
status: Draft
created: 2026-10-09
---

# Codex CLI Driver 设计

> 从零实现 Workflow Controller 的 Codex CLI runtime（`codex exec --json`）。主目标：完成最小闭环——单 agent 会话端到端跑通；后续目标：完善控制面——stop / ask / subagent 进度。steer 与 graph / breather / traceback 一样属上层能力，不在 driver 范围。

## 1. 目标与非目标

目标：

- Codex CLI 达到与 opencode 同等的控制面完整度：stop 决策可回流、ask 提问确认、subagent 进度。
- Codex 会话详情页的 Insight 入口使用原生 rollout 导入。
- 模块边界清晰、低耦合，每个模块可独立评审、合入、回滚。

非目标：

- 不做 Codex `PreToolUse` 工具级 deny（本期用 `workflow_controller_ask` MCP gate 覆盖提问/确认门禁）。
- 不做 Codex session DB 解析。
- 不扩展 CANNBay exporter；Codex 上传继续显式报暂不支持。
- 不支持 Anthropic 原生 Messages 协议；Claude 模型仅允许经 OpenAI Responses 兼容网关和 provider 绑定的 model catalog 接入。
- 不做 mid-flight steer（暂停 / 改 / 继续）：driver 只提供 Stop hook / resume / fork 原语，实时改属上层会话能力，不在 driver 控制面。

## 2. 约束与假设

约束：

- 遵循既有 Driver 协议与 Event 模型；Codex 专用凭证、config 与内部调用参数留在 Codex Driver / 调用侧，不扩展通用 runtime adapter 协议。
- 通用 Driver 协议不感知 runtime 专属控制面；B/D 以「数据 + 回调」注入，C 由 Codex runtime 在 query 内组装。
- 复用已有 `resolve_runtime()`、HookServer、opencode gate、auth rotator，不重复实现。
- 范围不含 graph / breather / traceback 等上层插件。
- C 的 gate 审批失败时，不得以 `--dangerously-bypass-approvals-and-sandbox` 回退（该 flag 会同时关闭 sandbox）；应固定已验证的 Codex 修复版本，或改用非 MCP 提问通道。

假设：

- `codex exec --json` 的 stdout JSONL 是稳定、可逐行解析的协议。
- Codex 无 HTTP hook / in-process DriverHooks，因此 Stop / Subagent 走 command hook 回流机制，Ask 等价物走 MCP gate。
- 有可用的 Codex/OpenAI 兼容 provider；Codex worker 与 Claude / opencode 一样必须绑定 provider，不回退本机登录态。
- 当前 Session 层假设同一 Workflow Controller session 任意时刻最多只有一个 active query；重叠 query 是既有 Session/driver/manual answer 管理问题，不属于本设计修复范围。调用方必须串行同一 session 的 query；不同 session 之间可以并发。
- D 与 Insight 导入依赖 `<workspace>/.codex/sessions/...` rollout 文件存在且 schema 稳定；schema 变化时 D fail-open，Insight 导入返回可诊断错误。
- 目标 Codex 版本在 `codex exec` 无人值守下会执行 `workflow_controller_ask` 的 per-tool `approval_mode="approve"`，且 streamable HTTP MCP 连接与 `workflow_controller_ask` 长阻塞调用不被客户端超时自动取消；接入真实环境前需验证/固定版本，并规避 openai/codex#29857 的自动取消行为。

## 3. 模块划分

| ID | 模块 | 目标 | 职责 |
|---|---|---|---|
| A | Codex 基础驱动 | 最小闭环 | 拉起 `codex exec --json`，JSONL → `Event`，管理生命周期，并完成 Codex 专用凭证 / config / 默认值装配 |
| B | 控制面 command hooks | 控制面 | `.codex/hooks.json` + 本地 command 回流 Stop / Subagent |
| C | MCP gate | 控制面 | per-query gate，注入 `workflow_controller_ask` 门禁 |
| D | subagent 进度 | 控制面 | rollout poller 补 subagent 增量进度 |

## 4. 依赖与独立性

依赖方向（只允许上依赖下，不反向）：

```text
A Codex worker runtime
   ├─ B 控制面 hooks（依赖 A）
   │    └─ D subagent 进度（依赖 A + B）
   └─ C MCP gate（runtime 无关实现）
```

原则：

- A 保持 argv、进程生命周期与事件翻译等基础能力，并作为 Codex worker runtime 组装 C；B/D 由调用方以「数据 + 回调」注入。
- C 归 Codex runtime 内部组装：`Session` 只传递既有 `hooks`，不传递 gate URL；Codex runtime 在 query 内根据 `on_gate_question` 启动可选 gate。`Driver.query()` 不为 runtime 专属控制面增加参数。
- B/C 缺少时 Codex 基础事件流仍可运行（graceful degradation）。
- D 依赖 B 的 SubagentStart/Stop 分工，只补 progress、不重复 START/STOP。

## 5. 模块 A：Codex 基础驱动

职责：`packages/kernel/src/workflow_controller/driver/codex_cli.py` spawn `codex exec --json`，逐行解析 stdout JSONL，翻译为 Workflow Controller `Event`，管理子进程生命周期、argv/env/sandbox 映射，完成 Codex 专用凭证、进程级 config 覆盖与默认值装配，并在 worker query 内组装 per-query MCP gate；在 `packages/kernel/src/workflow_controller/driver/factory.py` 注册 `codex_cli` 分支。

关键点：

- argv：`exec --json --skip-git-repo-check --cd <cwd> ... -- <prompt>`，prompt 前加 `--` 终止符防注入；启用 CLI wrapper 时在完整 Codex 命令前追加透明前缀，如 `cpx codex exec ...`。
- 权限模式决策：不再传 `--ask-for-approval`。本地 Codex CLI `0.165.0` 实测 `codex exec` 即使收到全局 `--ask-for-approval on-request`，session rollout 仍记录 `approval_policy: "never"`；无该 flag 时同样为 `never`。因此该参数对非交互 exec 是冗余的，安全边界统一由 `--sandbox` 决定，审批门禁动作按 fail-closed 处理。
- per-call `permission_mode` 优先于构造参数；Driver 协议与其实现用空字符串表示未指定，各 driver 统一以 `per_call or constructor_default` 回退。Codex worker 构造默认与其它 runtime 一样保持 `bypassPermissions`，未显式降权时映射 `--sandbox danger-full-access`；provider probe 与 feedback 评估属于内部调用，必须显式传 `read-only`。

  Claude mode 与 Codex 非交互参数只做近似映射，不宣称语义等价：

  | Workflow Controller / Claude mode | 非交互语义 | Codex 近似参数 |
  | --- | --- | --- |
  | `plan` | 只读、规划，不产生本地变更 | `--sandbox read-only` |
  | `default` / `read-only` | 读文件；本地写入与越界动作失败 | `--sandbox read-only` |
  | `acceptEdits` | 允许在预先声明的工作区内自动读写和执行常规命令 | `--sandbox workspace-write`（需要额外目录时用 `--add-dir`） |
  | `bypassPermissions` | 无人值守且外层环境已完全受控时允许全量访问 | `--sandbox danger-full-access` |

  `plan` 并不等价于 Codex `read-only`：Claude 的 `plan` 是产品级“规划模式”，而 Codex `read-only` 是本地命令沙箱。`read-only` 能阻止模型生成命令对本地文件写入，但依赖沙箱 runtime 可用；它不是“无副作用”的普适保证，MCP / app 工具仍可能按自身配置产生外部副作用，session rollout 也可能落盘。需要更强隔离时可叠加 `--ephemeral` 避免持久化 session 文件。
- 事件翻译：`thread.started` → `ITERATION_START`；`turn.completed` → `STEP_FINISH`（usage 归一）；`turn.failed` / `error` → `ERROR`；`item.completed` 展开为 `ASSISTANT_MESSAGE / TOOL_USE / TOOL_RESULT / TOOL_CALL / NOTIFICATION`。`codex exec --json` 顶层没有 `thread.completed`；本次 one-shot invocation 以首个 `turn.completed` 为终态，不再等待后续 stdout。
- 参数兜底：非空 `max_turns`、`allowed_tools` / `disallowed_tools` 直接 `ValueError` fail-closed——当前 `codex exec` 没有稳定的 turn-cap 等价 flag（`--num-turns` 不存在；`--max-turns` 与 `--max-steps` / `--max-agent-turns` 分别停留在 openai/codex#12336、#33294，均未合入），且 Workflow Controller 工具名集合无法与 Codex 工具集一一映射（Codex 仅有 MCP `enabled_tools` 与 execpolicy shell 前缀规则，无通用工具名 allow/deny flag），因此拒绝而不是悄悄放宽；若目标版本合入可映射的 turn-cap flag，再改为透传并验证计数口径。
- 错误/限流归一：`turn.failed` 与顶层 `error` 均只有 `error.message` 文本（无结构化 statusCode），driver 从文本识别——`401` / `403` 用词边界匹配，或 `authentication failed` → `RATE_LIMIT(kind=auth_error)` + `ERROR`；`429` 用词边界匹配，或 `rate limit` / `too many requests` → `RATE_LIMIT(kind=rejected, scope=rate_limit)` + `ERROR`。限流文本可解析 `resets at HH:MM` / `try again at HH:MM` 时按当日或次日时间写入 `reset_at`；无恢复提示则写入当前时间加 5 小时的保守回退窗口，供 AuthRotator 的 per-provider 自动恢复调度使用；其余只 `ERROR`。
- 进程管理：`setsid` 建进程组，`stop()/kill()` 用 `killpg(SIGKILL)`，`weakref.finalize` 兜底；未收到 `turn.completed` 且 `returncode != 0` 时仅在流内无 `ERROR` 时补一条兜底 `ERROR`，失败路径末尾仍发 `ITERATION_END`。
- stderr 语义：stdout JSONL 仍是唯一 Codex 协议事件来源；stderr 只作诊断 / 状态流，不按物理流推断 query 终态。`cpx` 的状态日志用 `console.error()` 输出（Node.js 中写 stderr 的常规做法，不是错误），Codex 也可能输出 `Reading additional input from stdin...` 与 multiline 诊断 continuation。driver 只把明确的错误标记行记为日志 `ERROR`：行首或空白后的 `ERROR` 后接空白/冒号/等号、行首或单个前缀词后的 `Error:` / `error:`、行首或空白后的 `[ERROR]`、`level=ERROR`。hook trust、`Reading additional input...`、`[cpx-cli]` 状态行和未分类普通行降为 `DEBUG`；本分类不使用 `INFO` / `WARNING`。兜底 `ERROR` 的 stderr 尾部采用「错误行优先 + 普通行限量」两段保留，避免长段正常输出淹没早期真实错误。完整消息形态、日志级别与 tail 映射见 [CLI stderr 观测语义统一设计](./cli-stderr-observability.md)。
- 子进程工作目录与 `--cd` 一致：`CodexCLIDriver.query()` spawn 时传 `cwd=opts.cwd`，与 claude_cli / opencode 相同；Codex CLI 同时经 argv `--cd <cwd>` 声明工作区。CLI 自身或 hook/子工具依赖进程 cwd 时不得落到 daemon cwd。

### 运行时装配

- Codex provider 查询由 `CodexCLIDriver.query()` 根据通用 provider env 和 `cwd` 完成专用投影：只过滤通用 provider 协议相关的 Anthropic / opencode 凭证；并在注入前清除宿主机 `OPENAI_API_KEY` 与 `WORKFLOW_CONTROLLER_CODEX_*` 凭证残留，仅注入本次 provider 的 `WORKFLOW_CONTROLLER_CODEX_API_KEY`。`CODEX_HOME` 指向 `<workspace>/.codex`；driver 只确保该目录存在，不创建或改写 `config.toml`，已有配置只作为共享基础配置读取。
- Codex provider 必须显式配置非空 `base_url`；AuthRotator 的 Codex 分支不把全局 `settings.conf default_url` 或宿主 env 中的 `ANTHROPIC_BASE_URL` 提升为 provider URL。缺失或空白 `base_url` 时，`CodexCLIDriver.query()` 在 spawn 前以 rejected_params fail-closed，不回退 Codex 内置 OpenAI provider。显式 `base_url` 经每次 invocation 的 `-c/--config` 覆盖 `model_provider` 与 `model_providers.workflow_controller.*`，并通过其 `env_key` 指定的进程 env 传递密钥；这些覆盖只作用于当前 Codex 子进程，不落盘，也不影响同工作区其他会话或 provider 探测。
- Codex Worker 要求 base_url 提供 OpenAI Responses 协议。若服务端按 Anthropic/Chat 协议校验请求并返回 `body.messages` / `tool_choice` 类字段错误，或明确报告缺少 `messages` 参数，driver 将错误归一为 `response is not OpenAI Responses format`，与 Reviewer 的 Anthropic Messages 协议不匹配提示保持一致。
- Responses 网关若明确返回模型转换未实现（如 `convert_request_failed`，或错误正文精确为 `not implemented`），Codex Worker 错误归一为 `gateway does not support this model with OpenAI Responses format`，提示问题在该网关的 OpenAI Responses 模型转换能力而非本地 catalog；Reviewer 仍独立探测。泛化的流断开错误不据此归类。
- Codex 自定义模型需要模型 catalog 提供上下文窗口、reasoning、工具形态等 metadata。Workflow Controller 不从宿主机当前 Codex 配置推断 catalog：driver 不读取宿主 `CODEX_HOME/config.toml`、`CODEX_HOME/models.json`、`~/.codex/config.toml` 或 `~/.codex/models.json`。catalog 判定分为：Codex 内置 metadata 已覆盖的 OpenAI/GPT 模型不注入 `model_catalog_json`；Workflow Controller 内置 preset 覆盖的 model slug 自动复制并绑定对应 catalog；未收录模型必须由用户提供官方推荐 catalog 的绝对路径（支持 `~` 展开，不推导相对路径）。未提供路径时 preflight 返回 `custom_required`，显示「请配置第三方 provider 官方推荐的 model catalog JSON 文件，并将所在绝对路径输入设置中」，且不启动 smoke test；已填写路径在 preflight 中校验，不可访问、相对路径、JSON 非法、缺少顶层 `models` 数组或当前模型不在 catalog 中时返回 `custom_invalid`，显示「请输入合法且包含正确模型名的 model catalog JSON 文件路径」，且不启动 smoke test。校验通过的自定义路径返回 `custom`，driver 仍在 spawn 前复验并注入。
- Codex 内置模型检测可能执行 `codex debug models --bundled` 子进程；AuthRotator 的 async 路径通过 `asyncio.to_thread()` 调用 catalog 解析，避免首次检测阻塞 event loop。选择线程池而非 asyncio 子进程是为了保留同步 catalog 解析函数及其直接单测，同时一并移出 JSON 读取和 preset 安装等少量文件 I/O。
- Codex catalog preset 是 Workflow Controller 随包发布的官方 catalog 快照，资源文件位于 `packages/kernel/src/workflow_controller/resources/codex_catalog_presets.json`，包含匹配用的 model 集合与可直接被 Codex 读取的 `models` JSON 内容。preset 只作为 provider 添加/测试阶段的自动匹配来源；按精确 model slug 匹配，不做名称相似或前缀推断，命中后复制到 `<workflow-controller-config-dir>/codex-models/`，provider entry 只保存解析后的绝对路径，不内嵌 JSON 内容。preset 匹配与调度前校验复用同一套 catalog JSON / model slug 解析规则；同一 slug 命中多个 preset 时使用资源文件中的首个 preset。当前包含 Zhipu GLM（`glm-5.3` / `glm-5.2`）、DeepSeek，以及 YibuAPI 网关上的 Claude Opus。
- GLM-5.2 的 reasoning 档位按 Z.ai 官方兼容集声明：`none`、`minimal`、`low`、`medium`、`high`、`xhigh`、`max`。其中 `none` / `minimal` 由服务端跳过 thinking，`low` / `medium` 映射为 `high`，`xhigh` 映射为 `max`；preset 保留这些请求值，不在本地改写成少数“有效档位”。
- YibuAPI Claude Opus preset 按 `claude-opus-5-5` 精确匹配，保留 context 窗口、输入模态、parallel tool calls、reasoning level 等完整模型 metadata；为兼容第三方 Responses 网关，显式关闭 verbosity 并声明无实验工具。catalog 快照必须满足当前 Codex `ModelInfo` 必填约束：显式提供 `truncation_policy`，并提供 `base_instructions` 或 `model_messages.instructions_template` 之一；本 preset 使用空 `base_instructions`，与 Zhipu preset 的最小 catalog 口径一致。
- Provider 表单的 base URL 保持网关官方根地址。Codex 投影只对仅有 host、没有 API path 的根地址补 `/v1`，显式版本化地址保持原样；因此 YibuAPI 的 `https://yibuapi.com` 会按官方 Codex 配置调用 `/v1/responses`，不影响 Anthropic Messages 对同一根地址的推导。catalog 只解决本地模型 metadata，不能替网关补齐特定模型的 Responses 转换能力；若网关对该模型明确返回 `convert_request_failed`，Worker 验证失败并按上文规则归一化错误提示。
- 第三方 Responses 网关对 Codex 工具形态的支持不一。Workflow Controller preset 采用 function 工具兼容口径，不声明 freeform apply-patch 与 verbosity；注入第三方 catalog 时同时关闭 Codex web_search，避免网关拒绝 `custom` / `web_search` 工具或 `text.verbosity`。Codex 内置 OpenAI 模型仍使用原生内置 metadata 与默认 web_search 行为。
- 解析后的 catalog 路径以 `settings.conf` provider entry 为配置源；行内重测得到 preset/自定义路径时写回对应 entry，并在 `auth_state.json` 的 provider 状态中同步序列化，避免重启后绑定丢失或验证指纹漂移。catalog preflight 失败不隐式清空已绑定路径；Worker/Reviewer 验证指纹始终按 provider 当前配置计算。
- Provider 测试与保存使用同一 catalog fail-closed 语义：`custom_required` / `custom_invalid` 只判定 Codex Worker 不可用，Reviewer 仍独立探测；Reviewer 通过时可保存 reviewer-only provider，两者都失败才拒绝保存。
- Web「设置 → 模型供应商」的 Codex CLI catalog 输入采用延迟显示：非 `Codex CLI` runtime 不显示；初始 Codex 表单仍只呈现 key/model/base URL/runtime，用户点击「测试」后先做 catalog preflight。命中 Codex 内置模型时显示「使用 Codex 内置模型 metadata」；命中内置 preset 时显示「已自动匹配 <provider> 官方 model catalog」；未识别且非内置模型时不启动 Codex smoke test，显示自定义 `model catalog JSON` 路径输入框与统一提示，用户填写后重新测试。provider entry 的内部 `model_catalog_path` 参与 provider 测试与 Worker 调度；Codex driver 消费该字段并在投影环境前剥离，解析后的路径经每次 invocation 的 `-c model_catalog_json=<path>` 传给 Codex，不进入任何进程环境，也不复制或改写宿主/workspace 的 Codex 配置。
- Web 的重复 provider 检测使用 preflight 返回且已解析的 catalog 路径比较；空表单路径不代表任意已绑定路径，避免把自动 preset 与自定义 catalog 混为同一配置。
- `settings.conf` 的通用解析层只保留 `model_catalog_path` 字段，不按 runtime 过滤；该字段是否生效由 AuthRotator 的 effective runtime 判定。非 Codex runtime 下它不进入 driver env、不参与 provider 指纹，也不会被任何非 Codex driver 消费。
- Codex 模型只来自会话显式配置或其显式绑定 provider 的 `model`；两者为空时 fail-closed 拒绝发起查询，且不继承全局 `default_model`、宿主机 `ANTHROPIC_MODEL` 或 `config.toml` 的残留 `model`。provider 轮换到无 `model` 的条目时，未 pinned 的继承模型同步清空；用户显式 pinned 的模型保留。非空模型始终经 `--model` 显式传递。
- 保留工作区内与 B 无关的 Codex 基础配置，同一工作区继续共享一个 `CODEX_HOME`，不创建按 provider / session 变化的临时 `CODEX_HOME`。
- worker 未绑定 provider 时直接报错；Codex 不在 Web API 或 orchestrator 快照阶段读取 UI active provider / 池内首个可用 provider 作为隐式绑定，只接受调用方显式传入的 provider index。该约束以 driver 为最终 enforcement point：provider env 缺少或只含空白 `ANTHROPIC_API_KEY`，或已填写的 provider 专属模型 catalog 非法时，`CodexCLIDriver.query()` 必须在 spawn 前以 rejected_params 拒绝，不读取宿主机 `~/.codex` / `CODEX_HOME` 中的登录态、默认 provider、`OPENAI_API_KEY` 或模型 catalog 兜底。后续 C 控制面配置仍写入同一个 `<workspace>/.codex/config.toml`。
- provider 探测在 auth rotator 的 Codex 分支使用只读、无 turn 上限、无工具限制参数，并等待 Codex 事件流到 `ITERATION_END`；探测 scratch 目录清理容忍 Codex SQLite 历史文件的瞬时竞态，不把清理噪声误报为 provider 失败。`CodexCLIDriver.query_for_evaluation()` 使用相同的查询限制，模型优先取评估器显式配置，其次取 provider 环境中的模型。工作区配置由 `CodexCLIDriver.query()` 统一维护。
- Web 设置的「模型供应商 → 添加新供应商 → Worker 运行时」下拉提供 `Codex CLI`，已配置供应商列表使用同一显示名。Codex provider 同时支持 Worker 与 Reviewer：Worker 探测使用 Codex CLI 对应的 OpenAI Responses base URL，并沿用内置 metadata 或 catalog 的默认 reasoning 配置；Reviewer 探测与调度使用 Anthropic Messages base URL，不把 OpenAI Responses base URL 改写成 Anthropic 协议复用。Worker 与 Reviewer 是独立能力，任一通过即可保存 provider；Codex catalog preflight 失败只判定 Worker 失败，Reviewer 仍独立探测。
- 新建会话的「代理框架」下拉提供 `Codex CLI`；选择后 Worker 模型列表只显示 runtime 匹配且 Worker 检测通过的 provider，Reviewer 模型列表只显示 Reviewer 检测通过的 provider；Worker 与 Reviewer 可绑定不同 provider entry。

## 6. 控制面（B / C / D）

### 模块 B：控制面 command hooks

- B 控制面 hooks：启用 B 的 workspace 为 Workflow Controller 专用，用户不应在其中手动
  运行 Codex。`hooks.json` 不存在或已是 Workflow Controller managed 格式时才生成/复用；
  存在未知 hooks 时拒绝启用，不覆盖或混合执行；配置只在 effective
  runtime 为 Codex 且 HookServer 已在役的 session 激活时准备，不进入通用
  hook 配置生成。
  运行期 hooks 定义保持 immutable，`query()` 不按查询重写配置。本地
  command 回流器把 Stop / SubagentStart / SubagentStop 回流 HookServer，
  query/session 相关信息只通过
  hook payload 或 invocation env 传递；`codex_cli.query()` 只接收注入的
  hooks 配置，A 不 import B。
  跨重启或升级导致解释器路径变化时，旧 hooks 会按未知配置拒绝；确认工作区
  无需保留后删除 `.codex/hooks.json` 并重启 Workflow Controller 重建。
- B 的 hook 信任策略：Workflow Controller 发起的 Codex CLI invocation 必须显式传入
  `--dangerously-bypass-hook-trust`。Codex 对非 managed hooks 按当前定义
  hash 做持久信任，B 启用时生成的 relay hook 不能依赖用户交互执行 `/hooks`
  完成信任。该 flag 只允许作用于本次 Workflow Controller 受控 invocation，不得写入用户
  alias、全局配置或影响用户手工运行的 Codex；同时需在日志中标记 hook trust
  bypass，Codex 自身的重复提示降级为 debug。该 flag 会放行本次所有 active
  的 non-managed hooks，因此 B 的启用
  检查必须拒绝未知 hooks。

#### Stop 协议映射

Stop 链路固定为：
`Codex Stop stdin → local command relay → POST /hooks/stop → on_stop_request → relay stdout`。

- 请求映射：relay 把 Codex stdin 的 `session_id` 原样作为 `session_id`
  路由，`stop_hook_active`、`last_assistant_message` 原样透传，完整 stdin
  JSON 保留到 `codex_input`。
- 响应映射：`None`、`{}` 或 `decision != "block"` 都输出 `{}` 并允许停止；
  `decision: "block"` 时输出 `{"decision":"block","reason":"..."}` 让 Codex
  继续；reason 缺失、空白或非字符串时记录错误并使用
  `"Workflow Controller reviewer requested continuation."`。HTTP 失败、非 200 或 JSON
  非法时先做有限快速重试；仍失败则输出 `{}` 放行停止，避免 HookServer
  故障导致 worker 无限继续。

#### Hook 失败与超时

- hooks 配置显式设置 `timeout`（单位秒），不依赖 Codex 默认 600 秒：
  Stop 为 `330`（沿用 Claude 生产配置的 5 分钟量级，并覆盖默认 reviewer
  300 秒加本地 relay 余量），SubagentStart / SubagentStop 为 `15`。
- relay 内部 HTTP 超时分别为 Stop `320` 秒、Subagent `10` 秒。到时或请求
  失败时 relay 记录错误并以 exit 0 结束：Stop 按失败策略有限重试后放行，
  Subagent 不产生额外 stdout。外层 Codex hook timeout 仅作最后保护。

#### Hook 并发路由

- Codex relay 直接复用现有 Claude `/hooks/stop`、`/hooks/subagent-start`
  和 `/hooks/subagent-stop` 端点，不新增专用路由或鉴权状态。
- 多 session 共用同一 HookServer，沿用现有 native `session_id` →
  Workflow Controller session 映射；不使用可变的“当前 session”状态。

### 模块 C：MCP gate

- C MCP gate：从 opencode 抽 runtime 无关的 `runtime_gate.py`，复用 per-query 生命周期语义——在“同一 session 同时最多一个 query”的既有假设下，Codex runtime 在每次 `query()` 内启动 gate，取得本次 OS 分配端口对应的 streamable-http URL，并在 query 结束（含正常结束、失败、取消、异常）后有界停止；URL 不跨 invocation 复用，并经 `hooks` / `session_id` 闭包路由。不同 session 的 gate 与回调天然隔离。
- 配置注入：gate URL 是 Codex runtime 的 invocation 内局部数据，不进入通用 `Driver.query()` 协议；runtime 把它翻译成 per-invocation `-c` 覆盖——`mcp_servers.workflow_controller_gate.url`、`required=true`、`tool_timeout_sec`，以及 `workflow_controller_ask.approval_mode="approve"`——与 provider override 同通道，只作用于当前子进程、不落盘。不写共享 `<workspace>/.codex/config.toml`，避免共享 `CODEX_HOME` 的并发 session 互相覆盖。
- A/C 边界：Codex runtime 负责 per-query gate 生命周期，并在同一 invocation 内把 URL 翻译为 `-c` 配置；`Session` 不启动、不停止 gate，也不知道 gate URL 或实现。argv/env 与事件翻译等基础能力仍保持在 `codex_cli.py`，runtime 无关 gate 实现保持在 `runtime_gate.py`。
- 抽取边界：`PerQueryGate`、`_GateServer`、`workflow_controller_ask` tool 注册、`answer_via_gate_with_keepalive` 及 MCP context 保活逻辑迁入 `runtime_gate.py`；`build_config_content`、`WORKER_AGENT_NAME`、agents / instructions 等 opencode 专属逻辑留在 `opencode_gate.py`。opencode 侧通过 import / re-export 保持现有入口稳定，抽取后既有 opencode gate 单测不回归。
- 启用范围：仅 worker session 的正常 `query()` 启用 gate（条件：effective runtime 为 Codex 且 `on_gate_question` 已接线）；auth rotator 的 provider 探测与 feedback evaluator 的内部调用不启动 gate，继续保持只读、可自动退出的保守参数。
- 失败语义：C 显式启用但 gate 启动失败时 fail-closed——Codex runtime 不 spawn Codex 子进程，向会话产出 `ERROR` + `ITERATION_END(reason=gate_failed)`，不得降级为无 gate 继续运行；`required=true` 必须使 Codex 对 gate MCP 初始化失败直接失败。运行中 gate 崩溃或 MCP 断连按 Codex 事件流的 `turn.failed` / `error` 处理，Codex runtime 在 finally 停止 gate。任何 gate 失败路径都不得回退 `--dangerously-bypass-approvals-and-sandbox`。
- 实现链路：`CodexCLIDriver.query()` 在参数和 provider 环境校验后、spawn Codex 前检查 `hooks.on_gate_question`；存在则创建 `PerQueryGate(require_callback=True)` 并启动，随后把本次局部 `gate.url` 传给既有 `_codex_config_overrides()` 生成 `-c` 参数。query 的正常、失败、取消和异常路径都停止 gate；未接线 callback 或内部 provider 探测 / feedback 调用则不创建 gate。
- 终态记账：`Session.query()` 转发 driver 事件时以通用规则记录本 invocation 的末个 `ERROR` 与 `ITERATION_END.reason`；当 reason 为 `gate_failed` 时设置 `state=IDLE`、`last_error`、`loop_status=no`、`stop_reason=gate_failed`，并发送一次 failed outcome。`SingleLoopEngine` 将 `gate_failed` 与 `error` 一样按失败终态放行，保证 `ERROR` 与 `ITERATION_END` 都进入事件总线；该逻辑不 import gate 实现。
- 并发边界：本设计只承诺不同 session 并发隔离；同一 session 的重叠 query 由上层串行避免。当前 PR 不新增 invocation registry 或问题路由表；未来若要支持同 session 重叠 query，需另行引入 invocation id、pending answer 索引和按 invocation 的 stop 控制。
- 版本验证清单：模块 C 代码实现通过单测 / 轻量集成不等于真实环境验收完成。接入目标 Codex 版本（约定不低于 `0.166.0`，或用户显式指定的更新版本）时，必须实测 per-tool `approval_mode="approve"` 生效、streamable HTTP MCP server 可连接、`required=true` 初始化失败 fail-closed、`tool_timeout_sec` 覆盖有效、`workflow_controller_ask` 阻塞后能按答案继续；可用短超时探针验证 timeout 机制，生产仍固定 `86400` 秒。验证结论记录到 PR / 环境验收记录；未通过时该版本视为不支持，不得降级运行。

### 模块 D：subagent 进度

- 引入原因：Codex 原生 hooks 只提供 `SubagentStart` / `SubagentStop` 生命周期信号，不提供子代理增量进度；thinking、工具调用、工具结果与最终文本需从子 rollout 读取。
- D subagent 进度采用「父 stdout 发现 + 子 rollout tail」两层结构：父事件流只在 `item.completed` 中识别 `collab_tool_call.spawn_agent`，用其中的 `receiver_thread_ids` 精确注册子 agent；子进度只读对应 `rollout-*-{child_thread_id}.jsonl` 的追加行。D 不按 `parent_thread_id` 盲扫整个 sessions 目录，避免共享 `CODEX_HOME` 的并发会话与 resume 旧文件误配。
- 生命周期分工保持不变：B 的 `SubagentStart` / `SubagentStop` 是唯一 `SUBAGENT_START` / `SUBAGENT_STOP` 来源；D 只发 `on_subagent_progress`。父流中的 spawn / wait / close 事实用于注册 monitor、对账和提前释放 tail 资源，不直接产生主会话 tool 事件，也不重复发子代理生命周期。
- 启用范围：仅 worker 正常 `query()` 且 `on_subagent_progress` 已接线时由 Codex driver 创建监视器；provider 探测与 feedback 内部调用不启用，未接线时事件流不变。

#### D 的数据协议

- A（基础 driver）解析父 stdout 时保留 `thread.started.thread_id` 作为 parent id，并把 `collab_tool_call` 归一为内部事实回调给调用方：`spawn(child_ids, parent_id)`、`wait(states)`、`close(states)`。该回调只传数据，也不把 collab tool 混入主 transcript。首个 `thread.started` 只用于初始化 parent id，后续子线程事件不得覆盖它。
- D 根据本次 invocation 的 `CODEX_HOME` 查找 `sessions/**/rollout-*-{child_thread_id}.jsonl`。文件在 spawn 事件后可能延迟创建，monitor 需按短间隔重试并设置上限；找到后必须读取首行 `session_meta`，校验 `payload.id == child_thread_id` 且 `payload.parent_thread_id == parent_thread_id`。
- rollout 读取维护每个文件的字节 offset，只消费完整换行，用 `ordinal` 去重；长工具输出不得走 `StreamReader.readline()`，必须沿用 chunk + 手工切行策略。
- 进度块归一为现有 subagent messages 协议：`response_item.function_call` → `tool_use`；`event_msg.item_completed.CommandExecution` 与 `response_item.function_call_output` → `tool_result`（按调用 id 去重：`function_call_output` 用 `call_id` 字段，`CommandExecution` 用其 `id` 字段——当前版本即调用 id；命令的 `failed` / `declined` / 非零退出码为失败，状态缺失时为未知；命令输出缺席时按输出内容兜底，其他工具可识别结构化 `error`，其余失败态标记为未知）；`AgentMessage` → `text`；reasoning 的 content 缺失时用 summary → `thinking`。所有输出进入事件流前截断，避免大命令结果污染 Dashboard 和 EventStore。

#### D 的状态机与并发

- B/D 对账经 `DriverHooks.subagent_progress_bridge` 会话级桥完成：每个会话的 `DriverHooks` 持有一个单槽 bridge，monitor 在 query 开始时注册、`finish()` 时按身份注销；B 的 hook 回调经 Orchestrator 从会话 hooks 取 bridge 转发，D 不维护进程级全局注册表，core 也不 import D。
- 每个 child monitor 的状态为 `discovered → active → completed`：父流 spawn 后进入 `discovered` 并开始找 rollout；B 的 SubagentStart 与同一 child id 对账后进入 `active`；B 的 SubagentStop、父流 wait/close 的 completed 等终态（`interrupted` / `errored` / `shutdown` / `not_found` 同样释放）、或子 rollout `task_complete` 进入 `completed`。只有 `active` 状态发送 progress；start 前读到的进度先进入有界缓冲，对账成功后按 `ordinal` flush，避免事件早于 start 被前端拓扑丢弃；`task_complete` 先到达时保留缓冲，等待延迟 Start 补发。Stop / wait / close 终态先唤醒尾读任务并消费最终批次，再释放该 child；若此时仍在发现 rollout，则先做一次最终查找与归属校验；查询取消或进程组被杀时不补造最终批次。
- `task_complete` 只用于停止 tail 并进入 completed，不调用 `on_subagent_stop`，也不保留最终摘要；最终文本经 `AgentMessage` 记录照常进入进度流。query 正常结束、取消或进程组被杀时，monitor 只停止尾读任务，不伪造 stop；在途清理继续由现有 `SubagentTracker` / 交接中断语义负责。
- 多 session 共享同一 workspace `.codex` 时，D 只信任本次父流给出的 child id，并以 `session_meta` 双重校验；不得把同 parent 的历史 rollout 当成本次进度。
- D 的 child 注册与尾读仅覆盖本 invocation 父流声明的 spawn；跨 resume invocation 仍存活的子代理不续读，待真实环境验证确有需求后再扩展。
- B 未启用或 start 对账未达时，child 停在 `discovered`：进度只进有界缓冲、不发送，rollout 发现超时后放弃（fail-open，不影响主 query）。
- 文件查找、meta 校验与 tail 读取全部经 `asyncio.to_thread` 执行；每个 child 独立任务，单 child 异常只终止该 child 的进度。
- D 显式启用时禁用 `--ephemeral`。当前 driver 不传该 flag；后续若暴露该能力，两者同时启用必须 fail-closed，因为 ephemeral 模式破坏 rollout 进度来源。

#### D 的施工路线

1. 固化协议 fixture：把 Codex CLI `0.166.0` 实测的父 `collab_tool_call` 事件、子 `session_meta`、function call、CommandExecution、function output、AgentMessage 与 `task_complete` 样例固化为内联测试夹具；解析器对 schema 变化 fail-open 并记录 debug，不让主 query 失败。
2. 实现 `codex_store.py` 纯读层：rollout 精确查找、首行 meta 校验、字节 offset tail、完整 JSONL 行解析、`ordinal/call_id` 去重与进度块归一；不写、不删除、不移动 Codex 文件。
3. 扩展 A 的数据回调：父 stdout parser 识别 `thread.started` 与 `collab_tool_call`，把 parent id 和 spawn/wait/close 事实交给注入的 observer；observer 缺失时行为保持现状，主事件翻译不回归。
4. 实现 monitor 编排：按 spawn child id 启动文件查找与 tail task，维护 `discovered/active/completed` 状态、B start 对账缓冲、stop 对账和 query 结束清理；progress 只调用 `DriverHooks.on_subagent_progress`。
5. 补齐并发与生命周期测试：两个 Codex session 共享 `CODEX_HOME` 时进度不串线；rollout 延迟创建、start 晚于进度、stop 晚于 `task_complete`、resume 历史文件、大输出截断、query cancel 均有断言。
6. 端到端验收：一次父会话派发两个只读子 agent，Dashboard 能实时看到两个 agent 的 thinking、tool use、tool result 与最终汇总；B start/stop 各只出现一次，D 不重复发生命周期事件。

### Insight 导入

- `resolve_transcript_source()` 为 `codex_cli` 返回 `kind="codex-jsonl"`：在既有安全校验下扫描 `<cwd>/.codex/sessions/**`，用首行 `session_meta.payload.id` 匹配 `native_session_id`，排除 subagent rollout；多分片时取最新主线程文件，候选并发消失时跳过，缺失时明确报错。
- 配置 `cpx` 的 Codex 会话在 Insight 导入时优先读取 `~/.context-insight/proxy/cpx-<session_id>.jsonl`（cpx 的实际捕获落点；`CANNBOT_PROXY_DIR` 显式配置仍最高，`~/.cannbot-insight/proxy/` 仅作为存量 fallback）。压缩捕获仅支持 Insight 导入。捕获缺失时不回退原生 rollout，返回错误「Codex cpx 捕获文件不存在（未回退原生 rollout）: <session_id>」。cpx 捕获的 `meta.json` 标记 `framework="claude-code"`，JSONL 首行为 `{"type":"user","message":...}` 的 Claude JSONL 方言；导入服务因此透传 `source="claude-jsonl"`。未启用 cpx 的会话仍使用 `resolve_transcript_source()` 的原生 rollout，现有导入服务向 Insight 透传 `source="codex-jsonl"`、`sessionId` 与 `filePath`，详情页 URL 使用源会话 `taskId`（即 Codex thread id），而非 Insight 数据库内部行 id。Run 导出不受 cpx 影响，继续复用原生 rollout 定位结果：rollout 存在时按 JSONL 文件复制，缺失时按会话成员降级为 `location=null/missing`，不影响整个 run 导出。CANNBay exporter 暂不支持 Codex，上传继续显式拒绝。

## 7. 实施顺序

- 最小闭环（M1）：A。
- 完善控制面（M2）：B、C（可并行）→ D（D 依赖 B 的 SubagentStart/Stop 分工）。

## 8. 用例与成功标准

最小闭环（A）：

- 单 agent 会话端到端跑通：`WORKFLOW_CONTROLLER_DRIVER=codex_cli` 启动 session，prompt 经 `codex exec` 执行，结果以 `Event` 流回，会话正常结束。
- 事件翻译正确：`thread.started` / `turn.completed` / `turn.failed` / `item.completed` 等正确映射为 `Event`，usage 归一无误；`turn.completed` 是本次 invocation 终态，输出事件后等待退出最多 5 秒，未退出则终止进程组并产出一次 `ITERATION_END`，不把该终止码报为错误。
- 生命周期可终止：`stop()/kill()` 能终止进程组；未收到 `turn.completed` 时 `returncode != 0` 产出 `ERROR`。

控制面（B / C / D）：

- reviewer stop 回流：Stop hook 经本地 command 回流器回流 HookServer，block / approve 决策影响继续或停止。
- 并发最小闭环：两个 session 并发回流时，事件经现有 native session 映射
  路由到各自 Workflow Controller session，不串线。
- ask 提问确认：agent 经 `workflow_controller_ask` gate 阻塞提问，用户答复后继续。
- subagent 进度：能看到增量 progress，start/stop 由 B 负责、不与 D 重复。
- Insight 导入：未配置 cpx 的 Codex 会话按原生 rollout 以 `codex-jsonl` 导入；配置 cpx 时按上节捕获优先规则导入，CANNBay 上传继续显式拒绝。

## 9. 验收门槛

- 单 agent 会话端到端跑通：`WORKFLOW_CONTROLLER_DRIVER=codex_cli` 跑一个单 agent session。
- 模块边界：通用 `Driver` 协议不感知 gate；`Session` 不 import gate 实现、不知道 gate URL；`codex_cli` runtime 是 C 的唯一组装点，`runtime_gate.py` 不反向 import Codex/OpenCode 专属配置逻辑。
- gate 注入正确：启用 C 时每次 invocation 的 `-c` 参数包含 gate URL 与 per-tool `approval_mode="approve"`；未启用时无相关覆盖，事件流与不启用时一致。
- gate 失败 fail-closed：gate 启动失败时不发起 Codex 子进程，会话收到 `ERROR` + `ITERATION_END(reason=gate_failed)`。
- D 目标单测全绿：不重复 START/STOP、meta 双校验防串线、start 先于 progress、共享 `CODEX_HOME` 隔离、query 取消清理监视器。
- opencode 抽取无回归：`runtime_gate.py` 抽取后既有 opencode gate 单测全绿。
- Insight 能按 `codex-jsonl` 导入原生 rollout；CANNBay 上传仍明确拒绝。
- 目标单测全绿 + 全量单测不回归。
