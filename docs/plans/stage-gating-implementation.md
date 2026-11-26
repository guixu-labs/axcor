# 阶段门禁实现日志（staged_pipeline）

> 本文是 `staged_pipeline` 的**实现 / 演进历史**（commit 序列、评审记录、refactor 史），从 [stage-gating-design.md](../designs/stage-gating.md) 拆出——后者只保留设计本身。设计内容以设计文档为准，本文不重复。

## 初版：10 commit（Phase A/B/C）

本 feature 初版由 10 个 commit 实现（Phase A/B/C，分三阶段）。每步 TDD（红→绿→commit），每个 Task 过两阶段 subagent 评审（先 spec 合规、再 code quality）+ 最终整体评审。

### Phase A：核心 seam 通用化（先铺通用机制，再让 plugin 用）

为什么要 core-first：通用 hook 必须先存在，plugin 才能实现它；且核心改动（动 loop/orchestrator/breather）风险最高，先独立验证。

| commit | 做什么 | 为什么这个顺序 |
|---|---|---|
| `feat(plugin): 加 IterationGateResult 与 on_iteration_boundary 迭代边界 hook` | base.py 加数据类 + async hook + SDK re-export + 修 stale docstring | 地基。此时没人调用 hook，纯增量、不破坏现状 |
| `feat(orchestrator): 加 on_iteration_boundary 派发器（first block/stop wins）` | orchestrator 加 `_run_iteration_boundary_gates`，镜像 stop 拦截器 | 派发器独立可测，暂不接线（接线是下一步） |
| `feat(breather): 实现 on_iteration_boundary（迁移准备，保留旧方法）` | breather 加 `on_iteration_boundary`，**保留**旧 `loop_boundary_check` | 让 breather 有新方法但 loop 仍走旧路——避免中间断 |
| `refactor(loop): iteration gate 取代 breather 名字硬编码，消费 block/stop/continue` | **原子切换**：orchestrator 注入改派发器 + loop 改名/消费 block/stop/continue + 删 breather 旧方法 + 迁 3 个旧 loop 测试 | 三处同改、中间状态会断，故**一个 commit**。切换后 breather handoff 经新路径，行为不变 |

A3「保留旧方法」+ A4「原子切换删旧」是有意的：避免任何一个中间 commit 让 live loop 断掉（breather handoff 是生产路径）。

### Phase B：`staged_pipeline` 参考 plugin（纯函数先行，再编排）

为什么 pure-functions-first：`gates`/`phases` 是确定性的核心逻辑，先 TDD 锁死，`plugin.py` 就只是对已测原语的编排，降低风险。

| commit | 做什么 |
|---|---|
| `feat(plugin/staged_pipeline): 门禁求值 exists/non_placeholder/command` | `gates.py`（4 纯函数）+ `__init__.py` |
| `feat(plugin/staged_pipeline): 阶段派生与跳步检测纯函数` | `phases.py`（completed_prefix / current_stage / all_complete / detect_jump） |
| `feat(plugin/staged_pipeline): 5 阶段骨架 + 顺序门（跳步检测）` | `config.yaml`（5 阶段）+ `plugin.py` 骨架 + `on_iteration_boundary` 顺序门 |
| `feat(plugin/staged_pipeline): 完成门 on_stop_hook（门禁全过才放行）` | `on_stop_hook` 完成门——两道门齐 |

### Phase C：验证 + 文档

| commit | 做什么 |
|---|---|
| `test(staged_pipeline): 端到端接线集成测试（跳步→block→补全→放行）` | 真 plugin 经真 dispatcher 被真 LoopEngine 驱动的组合测试（单测各自隔离，此测试锁组合） |
| `docs(plugin/staged_pipeline): starting_prompt 模板 + command 门禁信任注释` | 模板可用性（docstring 给 prompt 模板）+ `command` 可信输入说明；2026-11-13 起实现改为 `shell=False` + 固定 `/bin/bash -c` |

## 评审与验证（初版）

- 每 Task：implementer subagent（TDD）→ spec-compliance reviewer（独立验代码，不信报告）→ code-quality reviewer。发现的问题由 implementer 修后重审。
- 最终：整体 reviewer 跨 Phase A/B/C 的全部实现 commit 评审——架构、跨切面、breather 行为保留（byte-equivalent 确认）、两层模型接线、模板质量。结论 ready to merge。

## 后续 refactor：stages 归项目 config

初版 stages 写在 plugin 的 config.yaml（全局写死）。refactor（`refactor(staged_pipeline): stages 归项目 opsbot.yaml`）把它挪到 per-project：

- `AppConfig` / `SessionConfig` 加 `stages` 字段；`AppConfig.from_yaml` 解析项目 `opsbot.yaml` 的 `stages:`；`session_api` 选目录起 session 时注入 `session.config.stages`（镜像 `reviewer.task_goal` 走 `criteria` 的套路）。
- plugin 删 `__init__` / `_load_stages` / `self._stages`，改 `_stages_for(session)` 在门禁时从 session 读；没声明 stages → 门禁惰性 `pass`。
- plugin config.yaml 去 stages 块；migbot 5 阶段样例挪到 `examples/staged-pipeline/opsbot.yaml`。

**动机**：初版「一启用就套到每个 session」与「选别的项目不该被套」矛盾——文档讲死也救不了运行时行为。per-project 后，没声明的项目门禁惰性（= 现在的默认 reviewer-only 行为，零回归），声明了才多一道确定性硬地板。加新领域从「复制 plugin 改 config」简化成「新项目 opsbot.yaml 写 stages」。另：toggle 写回 config.yaml 改定点行编辑保注释（`fix(web): plugin toggle 定点改 enabled 行`）。

## 后续演进（stages 归项目之后）

设计文档随实现继续往前，以下 commit 在「stages 归项目」之后落地，设计文档已同步：

| commit | 做什么 | 设计文档落点 |
|---|---|---|
| `feat(staged-pipeline): 逐阶段门禁统一到 iteration boundary (CC+opencode)` | on_stop_hook → `on_iteration_boundary` 迁移（driver-agnostic，opencode stop 返回值被丢弃的问题根治）；顺手加 `prompt_prefix` 注入通用逐阶段工作纪律 | §4.3 / §4.4 / §7 / §5.2 |
| `feat(staged_pipeline): 每个 stage 支持多 artifact，各自独立设置 gate` | stage 从单 `artifact`+`gate` 平铺改为 `artifacts:` 列表，每条 `{artifact, gate, command}` 独立门禁；新增 `artifacts.py`（规范化 + 展示）；支持 `artifact` 字符串列表缩略写法 | §4.1 / §5.2 / §5.4 / §8 |
| `refactor(plugin): IterationGateResult 移除 \| None，action 精简为 pass/block/stop` | hook 返回类型 `IterationGateResult \| None` → `IterationGateResult`（非 Optional），无意见动作 `continue` → `pass`（默认返回值） | §2.1 / §2.2 / §2.3 / §2.4 / §3.1 |
| `fix: 修复stage pipeline在opencode的调用问题` | boundary 加 `_last_reviewed_stage_index` 去抖：worker 被推进后本轮还没产出下一阶段产物（disk k 不变）时，跳过同阶段 reviewer 重审，直接 block「继续等下一阶段产出」 | §4.4 |

## 求值函数整族搬入 SDK 中立模块（反向 import 清偿）

`artifacts.py` / `gates.py` / `phases.py` 整族搬入 `src/workflow_controller/plugin/stages.py` 并合并为一个模块，插件目录只剩 `plugin.py`（策略层）+ `config.yaml`。

**动机**：orchestrator（边界 review 文案）、reviewer 产物真值表、进度 API 三处核心消费方反向 import 插件模块，架构门禁带红。否决端口注入：被 import 的是项目 opsbot.yaml 契约上的纯函数（stages 归项目、非插件私有），且 staged_pipeline 默认 `enabled: false`——端口未注册时进度 API（纯读盘视图）会行为回退。搬入 SDK 后核心与插件两个方向的 import 都合法，行为零变化；顺带把四处重复的 stages 声明校验（orchestrator `_session_has_stages` / plugin `_stages_for` / `prompt_prefix` / pipeline_api `_stages_from`）收敛为模块内共享 helper。三个纯函数测试文件合并为 `test_stages.py`。

**后续收敛（同 PR）**：`on_iteration_boundary` 原先 `all_complete` → `detect_jump` → `current_stage`/`completed_prefix` 逐分支各算一遍前缀（`command` 门每次求值都起 shell）——改为开头单次 `completed_prefix`，`detect_jump` 增 `prefix` 参数复用；`current_stage` / `all_complete` 随之失去全部生产消费者，连同用例删除（`k == len(stages)` / `stages[k]` 即其语义）。

## 初版测试快照

staged-pipeline 相关合计 42 个；初版全量 `pytest tests/ -q`：**671 passed / 12 skipped**；`ruff check src/workflow_controller/` + `mypy src/workflow_controller/`（80 文件）干净。（point-in-time 快照，非持续更新；当前测试覆盖见设计文档 §6。）
