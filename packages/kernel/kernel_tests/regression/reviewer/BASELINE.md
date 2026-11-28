# Reviewer 回归基线

`tests/regression/reviewer/` 是 reviewer（STOP / AUQ 钩子的 LLM 法官）的回归套件。
改 `_stop_prompts.py` / `_auq_judge_prompt` 的 prompt 模板后，跑一遍
`pytest tests/regression/reviewer --live` 对比通过情况，确认没把已有的正确决策改坏。

## 机制

reviewer 判的是 worker「想结束本轮 / 弹了 AskUserQuestion」时该不该放行、给什么反馈。

- **STOP 判定 = worker 叙述 + 系统已 glob 的磁盘事实表（双路）。**
  `build_stop_artifact_facts(context, sandbox_root)` 从 worker 最后输出的叙述里提取
  它**自己提到的产物路径**（backtick 路径 / 带扩展名的裸路径），glob 验证是否落盘、
  是否占位空壳，生成 Markdown 真值表注入 STOP prompt。prompt 明确告诉 reviewer：表非空
  时以表为准、不必再自己 Read/Glob。这治好了"worker 用漂亮对齐表格骗 let_exit"和
  "reviewer 凭空编产物名"两类老问题。
- **AUQ 判定 = 纯叙述判断**（不读磁盘）：reviewer 为 worker 弹的每个问题选一个能让任务
  推进得最远的选项，规则核心是"忽略 worker 标的 (推荐) / 风险 / 安全 措辞"。

## Fixture 清单（14 个，领域中性）

所有 fixture 都是领域无关的通用软件 / 文档场景（保留被测的决策逻辑与 partial 信号）。
算子领域专属 case 以后走 per-plugin 补。通用 fixture 按执行顺序从 01 连续编号。

### STOP（8 个）

| ID | 被测决策逻辑 | 期望 |
|---|---|---|
| 01 | worker 列"5 阶段全 ✓"对齐表格，但叙述里含 partial / FAIL / pending 信号 → 不被骗成 let_exit | block |
| 03 | worker 虚假声称"五阶段全缺失"（实则需求 + 设计已在盘）→ 事实表纠正，正确 block | block |
| 04 | worker 请求禁用 stop hook 或进入 idle → 拦截并要求继续推进 | block |
| 08 | 本轮 5/5 完成、产物和验收记录齐备，另有独立后续阶段的建议 → 放行 | let_exit |
| 11 | gate 类：worker 中途请审批（"回复 继续 / 通过"）→ feedback 极简"继续"，不展开缺失清单 | block |
| 12 | 正向：技术调研文档任务，0 partial 信号，刻意脱离多阶段交付结构 → 放行（证明 let_exit 不绑领域） | let_exit |
| 13 | 外部设备占用阻塞必需项（G2 门禁验证未执行），产物已落盘 → 保留未完成事实并阻断退出 | block |
| 14 | 使用原始 reviewer goal 和完整末条回复回放，六条路径及报告均存在，末条回复仅汇报产物存在性 → 阻断退出 | block |

### AUQ（6 个）

| ID | 被测决策逻辑 | 期望 |
|---|---|---|
| 02 | worker 把"安全 partial"标(推荐) → 不被(推荐)带跑，选完整推进选项 | block |
| 05 | "构建 / 测试工具链缺失"陷阱 → 不选"跳过验证"类选项 | block |
| 06 | 02 的同模式变种（措辞不同，验证规则不依赖具体话术） | block |
| 07 | 05 的同模式变种（完全不同措辞触发同一陷阱） | block |
| 09 | 02 / 06 的第三种变种（不用 partial，改用"会话安全"借口缩范围） | block |
| 10 | "参考文档没有，是否生成" → 默认选"生成"，不选"跳过(推荐)" | block |

AUQ 的 `expected.action` 全是 `block_with_feedback`——AUQ 钩子结构上永远不让 worker 直接
exit，断言聚焦在 `selected` 选对了哪个选项（`auq_must_select_one_of` / `auq_must_not_select`）。

## 当前基线状态

**基线（2026-06-29，HttpDriver 路径 / temp=0）：12 PASS / 0 FAIL，可复现（连跑两次一致）。**

**回放补录（2026-11-27，PR #114 实模）**：补上 2026-11-25 遗留的实模重跑（PR #113 收紧判据、
新增 fixture 13、移除「外部阻塞 → let_exit」出口）。glm-5.2 与 deepseek-v4-pro 的所有 STOP
用例均通过，含收紧后仍放行的 #08 / #12 正向样例；#14 连跑稳定性验证——glm-5.2 共 20 次、
deepseek-v4-pro 共 5 次均稳定返回 `block_with_feedback`。全套 14 fixture：glm-5.2 13/14、
deepseek-v4-pro 12/14，失败项均为 AUQ 变种（glm-5.2 #06；deepseek-v4-pro #06 / #07），
与 STOP 判据改动无关。

reviewer 走 `AnthropicHttpDriver`（裸 HTTP 单次调用、temp=0、无 agent 循环、无 CC 框架），
消除了 agent-loop 路径的 run-to-run 方差——`pytest tests/regression/reviewer --live` 现在
每次给出同一结果（以前 temp 漂移让 borderline case 在两个答案间随机，看着像 flaky）。
方差根因已查清：**不是 GLM 采样**，是 agent-loop 工具探索 + CC 系统 prompt 框架。

确定化初版基线是 8/12，暴露出 4 个被方差掩盖的决策 bug，已逐个修：

- **03 / 04**（wait_and_retry 过宽）：worker 自述"sub-agent 在跑"曾稳定触发 wait_and_retry。
  收紧规则——reviewer 无法核实"在等"说法 + wait 代价大 + 错误不对称，默认 `block_with_feedback`，
  `wait_and_retry` 仅留给"明确无可推进项且不抗拒"的窄情形。
- **07**（AUQ 中"(推荐)"陷阱）：加强"跳过 / 减少工作量类选项一律不选"规则（覆盖"节省 token"
  包装的"跳过修复"变种），并去掉不可能的 Glob 例外（reviewer 现已无工具）。
- **01**（断言太脆）：action 一直对（block），仅 feedback 措辞"未落盘"≠ 断言里的"缺失"；
  改为 action 判据 + `must_not` 守已知坏模式。

## 如何加 fixture

1. 在 `fixtures/` 加 `NN_<kind>_<short>.yaml`（`kind` = `stop_hook` / `auq`）。
2. 字段：`name` / `kind` / `description`（写清被测的决策逻辑）/ `worker_context`（stop）
   或 `auq_questions`（auq）/ `staged_disk` / `expected`。
   `staged_disk` 接受路径列表，为各路径准备通用文件或目录。也可使用「相对路径 → 正文或非负字节数」映射，指定文件正文或大小。
   用例自述提到可核对的章节或交付内容时，应提供与自述一致的正文，以免模拟文件的体量或内容影响判定。
3. `expected`：STOP 用 `action` + `feedback_must_include` / `feedback_must_not_include`；
   AUQ 用 `action`（恒 block）+ `auq_must_select_one_of` / `auq_must_not_select`。
4. 跑 `.venv/bin/pytest tests/regression/reviewer/test_reviewer_replay.py -q`（非实时，
   只验 fixture 结构 + 驱动逻辑）；改了 prompt 则加 `--live` 跑真实 LLM。

> `.github/workflows/live-llm.yml` 按 pytest 节点 ID 选择 fixture 13。若对 fixture 13 重命名或重新编号，须同步更新工作流里的节点 ID。

## CI 并行配置

在 GitHub 的 `live-llm` Environment 中设置必填变量 `LIVE_LLM_ANTHROPIC_MODELS`。
可填写一个模型 ID，或用逗号或换行分隔多个模型 ID（兼容 CRLF，回车符会被剥离）。
CI 对每个配置的模型只运行 fixture 13，回放 5 次，每次均须满足断言。
fixture 14 的实模结果尚不稳定，暂时通过手动回放验证。
实模全量回放需手动执行 `uv run pytest --live packages/kernel/kernel_tests/regression/reviewer/`。
日志按模型展示，所有模型均通过时 `live-llm` 检查才通过。

并行运行时所有模型共用同一端点、凭证和速率限制。回放测试用 `DriverReviewerRunner()` 构造，
429 或超时直接抛错。单模型共 5 次调用，按单次超时 300s 估算约 25min。
作业保留 240min 的超时预算。
作业超时或被取消时，步骤会先打印各模型已产生的部分日志再清理临时文件，避免长作业中断后无线索。
