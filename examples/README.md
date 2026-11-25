# Workflow Controller examples

Workflow Controller 的每个能力点都有一个最小可跑的示例。每个示例是一个目录，含 `workflow-controller.yaml`（runtime
+ 任务 + reviewer），web 主面板选目录 → Start 即跑。

## 怎么跑（通用）

1. `.venv/bin/workflow-controller web` 起好（端口 18600）。
2. web 主面板选下表某个 example 目录 → **Start**。
3. Dashboard 看实时输出 / 控制（reviewer 决策 / handoff / 状态机）。
4. 产物落 example 目录；历史事件在 `{example}/.workflow-controller/events.db`（per-project 分库）。

> 各 example 的前置（runtime 特定 env / agent 安装）见各自 README。

## 示例一览

| 示例 | runtime | 演示 / 验证什么 |
|---|---|---|
| [getting-started](getting-started/) | claude_sdk | **入门**：领域无关轻量任务（wordstat 词频工具），跑一遍 Workflow Controller 全能力——实时观测 / 控制（暂停·改·继续）/ reviewer 反馈 / 产物落盘 |
| [multi-iteration](multi-iteration/) | claude_cli | **多轮 loop**：从零写计算器 CLI（4 阶段），验证 iteration 边界 / reviewer 真审查 / feedback 衔接 / checkpoint 接管。含 AUQ 门禁（需 CLI runtime） |
| [stop-hook-iter](stop-hook-iter/) | claude_sdk | **stop hook + reviewer decide_stop**（不用 AUQ）：多 iters 收尾路径，与 multi-iteration 对照 |
| [staged-pipeline](staged-pipeline/) | claude_sdk | **阶段门禁**：项目 `workflow-controller.yaml` 声明 `stages`，`staged_pipeline` plugin 确定性强制阶段顺序 + 完成（worker 跳步被拦回 / 没收齐产物不许结束）。罗马转换 5 阶段 |
| [opencode-subagent](opencode-subagent/) | opencode | **opencode + subagent**：primary 用 `task` 起 `researcher` subagent，验证 opencode subagent 接线（task→SUBAGENT event）+ breather subagent 返回点 handoff |
| [subagent-group-handoff](subagent-group-handoff/) | claude_cli | **确定性验证第一次整组交接**：`demo.py` 用可控 Claude CLI 替身断言一名子代理越线、三份文档、原主 session 重新派发；本目录的真实 Web 试跑只适合看首次派发 |
| [subagent-only-handoff](subagent-only-handoff/) | claude_cli | **Web 真模型首次交接试跑**：项目主代理阈值 10%、子代理阈值 5%；首次交接成功即可暂停，不要求完整任务跑通 |
| [codex-subagent-progress](codex-subagent-progress/) | codex_cli | **Codex 子代理实时进度**：父 agent 并行 spawn 两个只读子代理，验证 rollout tail → Dashboard 的 thinking / tool use / tool result 增量与 start/stop 对账 |
| [file-provenance](file-provenance/) | opencode | **文件归因**：从 Web 启动真实 LLM session，验证 runtime capture → 逐行 attribution → Git AI note；示例含独立 Git 项目模板和手工验收步骤 |

根 [`workflow-controller.yaml`](workflow-controller.yaml) 是 **cannbot 算子生成**的配置示例（`project_dir` 指向装好 cannbot
plugin 的工程，runtime 注释了 SDK / CLI / GLM 三种接法）。

## 按需求选

- **第一次跑 Workflow Controller** → [getting-started](getting-started/)（最轻、领域无关）。
- **看 loop 多轮 + reviewer 审查** → [multi-iteration](multi-iteration/)（含 AUQ）或 [stop-hook-iter](stop-hook-iter/)（无 AUQ）。
- **看阶段门禁（stages）** → [staged-pipeline](staged-pipeline/)。
- **看 opencode + subagent + breather handoff** → [opencode-subagent](opencode-subagent/)。
- **确定性验证三个并行子代理的首次整组交接和重新派发** → [subagent-group-handoff](subagent-group-handoff/)（运行无密钥 demo）。
- **在 Web 里用真实 Claude CLI 验证第一次子代理交接** → [subagent-only-handoff](subagent-only-handoff/)（首次交接成功即可暂停）。
- **看 Codex 子代理实时进度（thinking / tool use / tool result）** → [codex-subagent-progress](codex-subagent-progress/)（需 Codex CLI + 已验证 provider）。
- **看真实 LLM 的逐行文件归因和 Git note** → [file-provenance](file-provenance/)（需要可用 Provider/API key）。
- **配 cannbot 算子生成** → 根 [workflow-controller.yaml](workflow-controller.yaml)（指向 cannbot 工程）。

## 各 runtime 前置

| runtime | 前置 | 典型用途 |
|---|---|---|
| `claude_sdk` | 原生 Anthropic（env `ANTHROPIC_API_KEY` / OAuth） | 入门、阶段门禁、stop hook（in-process SDK） |
| `claude_cli` | 第三方 / GLM 端点（env `ANTHROPIC_BASE_URL` / `ANTHROPIC_AUTH_TOKEN` / `ANTHROPIC_MODEL`） | AUQ 门禁（CLI 有 AskUserQuestion 工具）、GLM |
| `opencode` | opencode CLI + provider（全局 `~/.config/opencode/opencode.json`，如 zai-anthropic + glm-5.2） | opencode runtime、subagent、GLM |
| `codex_cli` | Codex CLI（不低于 `0.166.0`）+ Web 设置中的 Codex provider（base URL / API key / model 必填） | Codex runtime、子代理实时进度 |

详见各 example README。
