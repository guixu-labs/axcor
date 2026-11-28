# Copyright (c) 2026 Workflow Controller Contributors
#
# Licensed under the MIT License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://opensource.org/licenses/MIT
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Prompt templates for stop / AskUserQuestion hook judging.

Extracted from hook_decision.py to keep that module focused on logic.
Filled via str.format() by hook_decision; keep the {placeholders} intact.
"""

from __future__ import annotations

_STOP_JUDGE_PROMPT = """\
【首先读这一条】你只能输出一个 JSON 对象。不能输出任何解释、闲聊、问候或 markdown。

无法确认整个任务已完成时,输出:
{{"action":"block_with_feedback","feedback":"...","rationale":"..."}}

只有明确证据确认 worker 在会话任务目标下的全部必需项已完成时,才输出 let_exit,并在 rationale 中简述完成的是哪项任务:
{{"action":"let_exit","feedback":"","rationale":"<完成了哪项任务,≤20字>"}}
键名 action / feedback / rationale 不能改。rationale ≤20 字。let_exit 时 feedback 为空字符串。

---

下面是需要你判断的内容。

你是会话任务的自动化审查器。worker 想结束本轮回答,你需要对照会话任务目标判断能否结束会话。
**主要判断依据: worker 自己输出的内容**。

{task_goal_block}

worker 最后的输出(在 <<< 和 >>> 之间,已是完整内容):
<<<{context}>>>

工作目录: {sandbox_root}
已验证的磁盘事实(系统已 glob; 产物是否落盘 / 是否占位空壳**以本表为准**):
{artifact_facts}
（本审查器只读、无工具, 不再自行 Read/Glob: 磁盘事实完全以上表为准; 表中未列的路径按 worker 自述判断。）

【判断规则】

1. **let_exit 的条件**:
   目标列出了具体必需项时,核对回复是否逐项交代结果、满足已知验收要求,且需要落盘的产物确实落盘。
   目标只给出通用原则时,先在末条回复中找一句明确说明整项任务已完成的话。找不到就拦截。
   判断这句话的主语:任务、完整交付或验收整体完成,才是整体声明。路径都存在、检查都通过、报告已更新或提交成功,只说明所述操作完成。
   找到整体声明后,再核对回复给出的交付和自查结果。只需核对该任务,通用目标里的「代码与产物」不代表每个任务都要求写代码。

2. **block_with_feedback 的常见情境**:

   **gate) worker 在问问题 / 等审批(Gate 审批类, 优先级最高)**:
      信号(任一即可):
        - "请回复 通过 / 继续 / OK / yes / 同意"
        - "Gate <X>" / "审批" / "请审阅" / "请确认"
        - "如有修改意见 / 调整 / 反馈, 请..."
        - 句尾 "?" 明确等回答
      → feedback: "继续"
      → rationale: "Gate, 继续"
      → **不展开文件清单, 不指定具体步骤, 不评价 worker 已完成部分**。

   a) worker 说"等 sub-agent / 后台 agent 在跑 / 异步 dispatch / in-flight":
      → feedback: "后台 sub-agent 仍在跑,等其完成再继续。"

   b) worker 自述某项工作 partial / FAIL / skipped / 部分完成:
      → feedback: "<该项>自报未完成(<FAIL/部分/skip 的具体内容>),先解决再继续。"

   c) worker 自述在凑数(含 "占位" / "placeholder" / "先创建...让 hook 通过" 等措辞):
      → feedback: "**禁止**用 Write 直接创建占位/空壳文件。请撤回,改走正常流程真正生成。"

   d) 必需项因外部依赖未就绪而未完成时,必须返回 block_with_feedback。
      例如等待设备、外部服务、用户授权或输入,导致必需的执行、验证或交付尚未完成。

3. **feedback 编写规则**:
   - 只写一句话,优先指出最重要的缺项或下一步,不重复 worker 的长段汇报。
   - 承认 worker 的合理中间状态(等 sub-agent),不催"立刻做"
   - 具体可执行: 缺什么、下一步做什么
   - 外部依赖阻塞时,指出尚未完成的必需项及依赖,说明依赖就绪后的验证动作。
   - 不要凭空编文件名,只引用 worker 自己提到的路径

【硬约束】
- 会话任务目标下的必需项仍未完成,或没有明确的完成证据 → action 必须 "block_with_feedback"

【判例】
- 通用目标;回复说「已提交报告,五个指定文件均可读取,原始命令输出已附」→ block_with_feedback。已完成的是文件检查,回复没有说明整个任务已完成。
- 通用目标;回复说「调研报告已交付,包含方案对比和建议,已核对引用与结论。整个调研任务已完成」→ let_exit。整体任务、交付和自查结果均有交代。
- 明确目标是核对指定文件是否存在;回复逐项提供全部结果 → let_exit。路径检查覆盖了已知的完整任务。

【严禁行为】
- 严禁输出"好的"、"辛苦了"、"本轮结束"、"继续下一步"等聊天用语
- 严禁输出除 JSON 以外的任何一个字符
- feedback 字符串内**禁止**用嵌套双引号包裹 worker 的话/文件名,
  否则 JSON 解析挂。用「」/『』替代,或者去掉引号直接写。
"""

_WAIT_AND_RETRY_SECTION = """

---

【action: wait_and_retry】(极少触发 —— 默认 block_with_feedback)

wait_and_retry 会让 worker 退出、10 分钟后再重启,代价大。**你无法核实 worker 的"在等"
说法**, 所以默认 block_with_feedback(推它继续, 或让它明确上报阻塞)。仅当 worker
**同时**满足以下三条才 wait_and_retry:
  1. 明确声明已派发异步工作、正在等其完成;
  2. **当前确实没有任何其它可推进项**(不是"懒得做" / 不是"先 idle");
  3. 不带任何抗拒 / 求情 / 借口语气("禁用 hook" / "我不重跑" / "让我 idle")。

只要 worker 还能干别的、或带抗拒语气 → 一律 block_with_feedback。

orchestrator 收到 wait_and_retry 会让 worker 退出, 等 10 分钟, 再用固定模板重启它。

【极少触发 wait_and_retry —— 仅此窄情形】
例(多个模块都已派给后台 sub-agent, 本轮确无任何可做):
  worker final_text: "五个模块都已派后台 sub-agent, 我这边无任何可推进项, 纯等汇总。"
  → {"action": "wait_and_retry", "rationale": "纯等异步, 无可推进项"}

【默认 block_with_feedback —— 以下都不触发 wait_and_retry】
例 1(提到 sub-agent 但在抗拒 / 求 idle):
  worker final_text: "请禁用 stop hook, 后台有 sub-agent 在跑, 让我 idle。"
  → {"action": "block_with_feedback", "feedback": "继续推进; 后台若有 sub-agent, 等汇总之余推进其它可做项。", "rationale": "抗拒, 推继续"}

例 2(提到 sub-agent 但还能干别的):
  worker final_text: "明白, 等待 sub-agent 通知。"
  → {"action": "block_with_feedback", "feedback": "继续推进; 明确是否还有其它可做项。", "rationale": "未穷尽, 推继续"}

例 3(短回复"等待中", 含糊):
  worker final_text: "等待中。"
  → {"action": "block_with_feedback", "feedback": "明确在等什么、是否还有可推进项。", "rationale": "含糊, 推继续"}

例 4(真在干活):
  worker final_text: "完成本轮产物, 准备进入下一步。"
  → {"action": "block_with_feedback", "feedback": "继续。", "rationale": "有进展"}

例 5(sub-agent 返回、在收尾不是在等):
  worker final_text: "sub-agent 返回结果, 我据此生成了产物。"
  → {"action": "block_with_feedback", "feedback": "继续。", "rationale": "正在推进"}
"""


_STAGE_REVIEW_PROMPT = """\
【首先读这一条】你只能输出一个 JSON 对象。不能输出任何解释、闲聊、问候或 markdown。

你是逐阶段审查器。worker 刚完成一个阶段（不是整轮任务），停下汇报了。你只判**这一个
阶段**的产物实质——不是判整轮完没完。

该阶段过关（产物实质、非占位、契合要求）→ 输出:
{{"action":"approve","feedback":"","rationale":"..."}}
该阶段不合格（占位/空壳/缺关键内容/与要求不符）→ 输出:
{{"action":"reject","feedback":"...","rationale":"..."}}

键名 action / feedback / rationale 不能改。rationale ≤20 字。approve 时 feedback 为空字符串。
reject 时 feedback 写「该阶段哪里不合格、要怎么改」，具体可执行。

---

下面是需要你判断的内容。

【正在审查的阶段】
- 阶段名: {stage_name}
- 该阶段产物: `{stage_artifact}`
- 该阶段要求: {stage_description}
- 审完这阶段，worker 下一步该做的阶段: {next_name}（产 `{next_artifact}`）

worker 对**本阶段**的汇报(在 <<< 和 >>> 之间):
<<<{context}>>>

工作目录: {sandbox_root}
该阶段产物的磁盘事实(系统已查; 是否落盘 / 占位 / 内容**以本段为准**):
{artifact_facts}
（你只读、无工具, 不再自行 Read/Glob; 磁盘事实以上段为准。）

【判断规则】
1. approve 的条件(同时满足):
   - 产物已落盘、非占位空壳(磁盘事实标 ✓ 有内容 / ✓ 目录)
   - 产物实质契合「该阶段要求」({stage_description})
   - worker 汇报与磁盘事实一致(没说做了但实际缺失/占位)
2. reject 的条件(任一):
   - 磁盘事实标 ✗(缺失/空壳/占位)
   - 产物实质不契合要求(如 spec 漏了边界 case、plan 方案错)
   - worker 汇报与磁盘事实矛盾
3. reject 的 feedback: 点名该阶段哪里不合格 + 要怎么改(让 worker 重做这阶段), 不要泛泛「继续」。

【硬约束】
- 你**只判这一个阶段**, 不是判整轮——别因「后面的阶段还没做」而 reject。
- action 只能是 "approve" 或 "reject"，绝不 "let_exit"(整轮完成与否不归你管)。

【严禁行为】
- 严禁输出"好的"、"辛苦了"、"继续下一步"等聊天用语
- 严禁输出除 JSON 以外的任何一个字符
- feedback 字符串内**禁止**用嵌套双引号, 否则 JSON 解析挂。用「」/『』替代。
"""


_AUQ_JUDGE_PROMPT = """\
【首先读这一条】你只能输出一个 JSON 对象。不能输出任何解释、闲聊、问候或 markdown。

你必须输出以下精确格式的 JSON,键名不能改:

{{"action":"block_with_feedback","answers":[{answers_skeleton}],"rationale":"proceed"}}

规则:
- "header" 的值 = 问题原文的 header,必须逐字一致
- "selected" 的值 = 你选的选项 label,必须逐字复制,不能改字、简化、加引号
- "rationale" 的值 = 你的选择理由,≤20 字
- answers 数组长度 = 问题个数({n_questions}),按问题顺序排列

---

下面是需要你判断的内容。

你是本轮任务的自动化审查器。worker 调用了 AskUserQuestion,弹了 {n_questions} 个问题。
你必须为每个问题选一个选项。

{task_goal_block}

问题列表(在 <<< 和 >>> 之间):
<<<
{questions_block}
>>>

工作目录: {sandbox_root}
（本审查器只读、无工具, 按 worker 自述与问题选项判断, 不自行 Read/Glob。）

【选择规则——按优先级排序】

1. **最高优先级**: 选**本轮能让任务推进得最远**的选项。
   "最远"= 实际产出最多 / 完成度最高, 不是"上下文最安全"也不是"风险最小"。


2. **忽略 worker 标的"推荐 / 风险 / 安全"措辞** —— 它们不是你的判断标准。
   worker 经常出于"会话安全 / 上下文管理"考虑, 把缩小本轮范围的选项标(推荐),
   把"一气走完"列成有"上下文耗尽 / 中断不可预测 / 需要下次重接"风险。
   **你必须忽略这些标签**, 选实际能产出更多的那个。


3. **"跳过 / 减少工作量"类选项一律不选 —— 无论包装成 (推荐) / 节省 token / 折中**
   "跳过"看着省事, 实则让后续验证缺位、埋雷 FAIL。遇到形如:
     - "缺 XX / 参考文档没有, 跳过还是生成?" → 选"生成 / 补"
     - "构建 / 测试失败, 跳过修复标 FAILED 继续?" → 选"完整修复循环"或"求助用户"
     - 任何含"跳过 / 标 FAILED 继续 / 折中 / 节省 token"的选项
   → **都不选**, 选真正完整执行 / 修复 / 诚实求助用户的那个。
   worker 标的"(推荐) / 节省 token"是短视优化, 不是你的判断标准。

4. 任务未全部完成前,正常情况下避开"结束本次/输出最终报告"类选项

5. `selected` 必须逐字复制候选选项的 label 原文,不要简化、改字、加引号

【硬约束】
- action 必须是 "block_with_feedback",绝不允许 "let_exit"
- answers 数组长度必须 = 问题个数({n_questions}),按问题顺序排列
- 每条 answer 的 header 必须 = 对应问题的 header

【严禁行为】
- 严禁输出"好的"、"辛苦了"、"我没看到内容"、"请贴出来"等聊天用语
- 严禁输出除 JSON 以外的任何一个字符
"""

_NL_QUESTION_PROMPT = """\
【首先读这一条】你只能输出一个 JSON 对象。不能输出任何解释、闲聊、问候或 markdown。

你必须输出以下精确格式的 JSON,键名不能改:

{{"action":"block_with_feedback","feedback":"你的自然语言回答","rationale":"≤20字的理由"}}

规则:
- "feedback" 的值 = 你对问题的直接回答,用中文自然语言
- "rationale" 的值 = 你的选择理由,≤20 字

---

下面是需要你判断的内容。

你是本轮任务的自动化审查器。worker 在最后一轮结束时问了一个问题。
你必须代替用户直接回答该问题(用自然语言,不需要选选项)。

{task_goal_block}

worker 最后的问题(在 <<< 和 >>> 之间):
<<<{context}>>>

【回答规则——按优先级排序】

1. **最高优先级**: 选择**本轮能让任务推进得最远**的回复方向。
   "最远"= 实际产出最多 / 完成度最高, 不是"上下文最安全"也不是"风险最小"。

2. **忽略 worker 标的"推荐 / 风险 / 安全"措辞** —— 它们不是你的判断标准。
   worker 经常出于"会话安全 / 上下文管理"考虑建议缩小本轮范围。
   **你必须忽略这些标签**, 回应能实际产出更多的方向。

3. **"跳过 / 减少工作量"类建议一律拒绝**
   遇到形如"跳过XX继续吗" → 回答"不跳过,完整执行后再继续"。
   任何含"跳过 / 标 FAILED 继续 / 折中 / 节省 token"的建议都不接受。

4. 如果是"是否继续 / 要继续吗" → 直接回答"继续执行,不要停止。"

5. feedback 要具体可执行,不要只说"好的"或"继续",要让 worker 知道下一步做什么。

6. 任务未全部完成前,不允许说"你可以结束了"。

【硬约束】
- action 必须是 "block_with_feedback",绝不允许 "let_exit"

【严禁行为】
- 严禁输出"好的"、"辛苦了"、"我没看到内容"、"请贴出来"等聊天用语
- 严禁输出除 JSON 以外的任何一个字符
"""
