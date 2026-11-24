---
type: implementation-plan
title: "RSI 在线时长修复闭环与工作区隔离回滚实施计划"
status: Planned
owner: "chenyangwang"
created: 2026-11-23
updated: 2026-11-24
related_design: docs/specs/traceback-workspace-rollback.md
related_adrs:
  - docs/adr/0001-worktree-scope-tree-unified-rollback.md
related_specs:
  - docs/specs/rsi-online-repair-loop.md
  - docs/specs/traceback-workspace-rollback.md
---

# RSI 在线时长修复闭环与工作区隔离回滚实施计划

> 本计划覆盖两个文档界定的全部工作：[traceback 工作区隔离与统一回滚技术规格](../specs/traceback-workspace-rollback.md)（本仓，OpsBot 侧）与 [RSI 在线时长修复闭环规格](../specs/rsi-online-repair-loop.md)（跨仓：本仓 + RSI 仓 + 接入仓 + 控制台发行包）。施工顺序按规格与 [ADR-0001](../adr/0001-worktree-scope-tree-unified-rollback.md) 已确认的决策执行：**Graph 节点级垂直切片与 RSI fix-findings 闭环先行（P0–P3），dispatch_group + 子代理回滚 + 已合并回滚其次（P4），单 session 迁移与插件退出最后（P5）**。首期 fix-findings 节点无内部子代理，不需要先实现 P4 能力。架构方案变更时先改设计文档再动代码。

## 1. 目标与范围

- 目标：OpsBot 支持「节点/子代理超时 → 暂停 → 导出证据 → RSI 诊断修复 → 应用 patch → Graph 回滚 → resume」的在线闭环，并以 fix-findings 案例端到端验证；traceback 能力迁入内核 `core/`，`agent_traceback` 插件在能力迁移完成后删除。
- 不在范围：跨机部署、对抗性安全沙箱（D8 为协作式隔离）、嵌套（孙）子代理的精确恢复、`file_provenance` 的重构。
- 依据：[ADR-0001](../adr/0001-worktree-scope-tree-unified-rollback.md)、[traceback 规格](../specs/traceback-workspace-rollback.md)、[RSI 闭环规格](../specs/rsi-online-repair-loop.md)。

## 2. 前置条件

- 本仓现有测试全绿（`uv sync && uv run pytest -q`），ruff / lint-imports / codespell / 架构门禁通过。
- fix-findings 首次实验现场已按 [RSI 闭环规格 §7.3](../specs/rsi-online-repair-loop.md) 准备（起始源码、节点开始前产物、可执行环境、耗时判据、知识卡片）。
- RSI 侧与接入仓的改动开在各自仓库（RSI 仓、`glacier-rsi` 接入仓），本计划只登记接口与联调顺序，其 PR 不占本仓 PR 编号预算。
- Web 查询/回滚端点的 HTTP 路由住控制台发行包（不在本仓，见 ARCHITECTURE「内核与控制台边界」）：S2c 的内核侧 CoreAPI 扩展（`api/run_api.py`）在本仓，路由装配 PR 开在控制台仓，同样只登记不占编号预算。
- 每个阶段保持 Web、Headless 和现有插件行为兼容；旧单 session 与插件路径在 P5 之前完全不动。

## 3. 阶段与任务

### P0：协议冻结 + 量化 spike

| ID | 任务 | 产出 | 依赖 | 验收 |
|---|---|---|---|---|
| P0.1 | 本文档集四份（ADR-0001 + 双 spec + 本计划，分居 `docs/adr`、`docs/specs`、`docs/plans`）评审定稿，协议以冻结版进入实现 | 本文档集 | 无 | 评审通过，后续改动走设计评审 |
| P0.2 | 量化 spike：用 fix-findings 真实目录测五项指标——初始基线导入耗时、单节点 worktree 检出耗时、`op/build` 纳入 checkpointed 后每次打点的提交耗时与磁盘增量、回滚恢复耗时、下游继承构建产物的成本 | spike 脚本与测量报告 | P0.1 | 指标可接受，或路径策略调整结论回填 ADR-0001 备注；GB 级产物超标路径改 `rebuildable` 或评估 reflink/内容寻址仓 |

### P1：私有仓 + 作用域核心

| ID | 任务 | 产出 | 依赖 | 验收 |
|---|---|---|---|---|
| P1.1 | `core/workspace/repository.py`：私有仓布局、基线导入（脏文件+未跟踪+内部 git 保留路径+排除清单防递归）、首层物化与 `scope_base_commit`、`root_seed_checkpoint`/`root_integration_head` | Repository 模块 + 单测 | P0.2 | 含未提交/未跟踪/用户 `.git` 的目录建区基线逐字节一致；用户仓 refs/HEAD/index/hooks 未改；根目录无 OpsBot git 元数据 |
| P1.2 | `core/workspace/scope_tree.py`（作用域账本 + 两层 epoch）+ `core/workspace/gate.py`（闸门状态机）+ `core/workspace/oplog.py`（三阶段 saga） | 账本/闸门/oplog + 单测 | P1.1 | 作用域 CRUD/epoch 幂等单测；闸门状态机与降级分支单测；saga 三阶段崩溃注入（含 GIT_WRITTEN 前判据、分叉即停不 reset） |
| P1.3 | `core/workspace/manager.py`（worktree 生命周期、物化、GC）+ `core/workspace/integration.py`（串行集成 + 按路径集成树，相交路径按 `conflict_policy` 档位处理：git_merge 走 merge-tree 三方合并、strict 在 P2.2 拒绝）+ 通用动作/通知协议类型 + `core/workspace/manifest.py`（两维路径策略） | 工作区/集成/manifest + 单测 | P1.2 | fake 编排经协议建区/集成；四组合路径策略行为正确；writer_local 不误报冲突 |
| P1.4 | `core/checkpoints.py` 迁移（自插件 CheckpointStore，平铺单文件对偶 `core/runs.py`）：核心新建 `scope_checkpoints` 表、schema 经 RunStore 统一迁移管理；插件旧 `checkpoints` 表并存至 P5 零改动（同表加 NOT NULL 列会撞插件并存写入，spec §7）；检查点类型 `scope_start`/`terminal` 与持久化顺序（编排状态→文件 commit→检查点行→发布）的 store 侧支持，生命周期触发接线在 P2.1 | CheckpointStore + 迁移单测 | P1.2 | schema 由 RunStore 迁移管理；并存期插件旧形态写入不受新 schema 影响；旧表留存零改动（历史行兼容读取器归 P5.2） |

### P2：Graph 节点级垂直切片（旧单 session 与插件路径不动）

| ID | 任务 | 产出 | 依赖 | 验收 |
|---|---|---|---|---|
| P2.1 | graph engine cwd 注入（7 处 `cwd=self._work_dir` 改为作用域工作区 + `WORKSPACE_DIR` 注入）；节点 executor/verifier 出生映射 `PrepareScopeWorkspace`；`awaiting_integration` 定序 + executor→verifier 闸门；`scope_start`/`terminal` 打点接线；**RunCoordinator 生产执行桥**——`PrepareScopeWorkspace`/`IntegrateScope` 等动作的生产派发接线（spec §5.1：协调器是唯一 I/O 手；P1.3 的 fake 编排只覆盖测试侧） | Graph 接线 + 协调器执行桥 + 集成测试 | P1.3、P1.4 | 双节点并行写不相交路径隔离 + 按序集成；验收通过 ≠ 集成完成（`awaiting_integration` 居中）；executor 后台写未静止前 verifier 不得启动 |
| P2.2 | 两档冲突检测（`conflict_policy`：`git_merge` 默认 = 以 `integration_base_commit` 为基准、限定 merge 路径集求交后走 merge-tree 可合并性判定；`strict` = 路径集重叠即拒绝，覆盖全部事件对）+ claims 软预检 + `fault.workspace_conflict` 入 watchdog 词表 | 冲突检测 + 单测/集成测试 | P2.1 | git_merge 档同文件不相交 hunk 并行写自动合并放行、真实合并冲突检出；strict 档同文件并行写（含 Git 可自动合并）检出；writer 修改 writer_local 路径、父树无变化时不误报；载荷含冲突路径与双方身份 |
| P2.3 | 未合并节点回滚异步状态机：通用 `plan_restore`（既有 `plan_rollback` 变为其调用方之一，并保留动态子图特例——target 为子图节点时父 generator 进新回滚代但立即置回 passed、不重新编排、其子图不废弃、回滚边记在 generator）、`AbandonScope`/`RestoreScope`/`PrepareScopeWorkspace` 动作、driver/op 异步化（op 只登记 `pending_rollback` 返回 operation_id）、内核 CoreAPI 增加 scopes/checkpoints/operations 查询与回滚方法（HTTP 路由 `GET .../scopes`、`GET .../checkpoints`、`GET .../operations/{id}` 的装配 PR 开在控制台发行包） | 异步回滚 + CoreAPI 扩展 + 集成测试（控制台路由 PR 登记在控制台仓） | P2.2 | op 立即返回 operation_id；回滚幂等、旧 epoch 回执被拒、失效集外零变化；rollback_to 验收失败回滚按 §9.1 编排语义执行（目标既有集成成果保留、不做文件撤回，spec §9）；兄弟已集成时新基线含其成果（redo on latest integration head）；generator 特例符合 spec §9.1.1 |

### P3：RSI fix-findings 闭环

| ID | 任务 | 产出 | 依赖 | 验收 |
|---|---|---|---|---|
| P3.1 | OpsBot 超时能力：子代理超时独立计时（`fault.subagent_timeout`，无新事件也触发）+ 节点累计超时规则（`fault.node_timeout`，整图默认 + 单节点覆盖，暂停不计时、续跑不清零、回滚重新计时、重启保留已用时间、同代只报一次） | watchdog 超时能力 + 单测 | P0.1（与 P1/P2 并行） | 持续输出日志的慢任务与完全卡住的任务都按时告警；告警含 Run/节点/代次/session/已用时间/时限 |
| P3.2 | 当前文件加载：复用现有加载/解析代码重新读取当前 YAML、角色、Skill，更新现有 Run 的任务定义，保留节点状态与产物；加载入口补「不创建新 Run」的调用方式 | 加载当前定义能力 + 单测 | P2.3 | 修复后原 Run 中加载新定义并继续，不新建 Run |
| P3.3 | 接入仓：适配器 `adapter.mjs` + `opsbot_bridge.py` + 启动脚本 `start_online.py`（启动后端、等待 API 就绪、`loop start`、定时 `loop tick`、退出清理、重启恢复核对） | 接入仓代码（PR 开在接入仓） | P2.3、P3.1 | 重启后从持久化账本恢复原任务，不重复创建 loop 或 Graph |
| P3.4 | RSI 仓：`budget.exceeded` 分支 + 暂停回调 + `policy` 中断（supervisor.ts / families.ts / loop.mjs）；证据导出与知识卡片接通；验证流程经适配器调用 OpsBot 隔离测试 Run；本地自动应用候选 | RSI 侧改动（PR 开在 RSI 仓） | P3.3 | 收到 fault 后远端确实暂停并进入原生诊断；时限与验收不被修复 agent 修改 |
| P3.5 | e2e：fix-findings 超时 → 暂停 → 导出 → 修复 → 异步回滚（operation_id 轮询）→ resume → 下游继续；整图完成后按配置从头开始下一轮 | e2e 验收记录 | P3.2、P3.4 | 见 §7 验收标准；据此评估 worktree 实际成本，再决定 P4/P5 节奏 |

### P4：dispatch_group + 子代理作用域 + 已合并回滚

| ID | 任务 | 产出 | 依赖 | 验收 |
|---|---|---|---|---|
| P4.1 | dispatch_group 一致性先行：⑤ 副作用工具前置拦截 + message 级组仲裁（`before_side_effect_tool`）、组检查点/`child_base_commit` 分离、屏障调用链（hook → 宿主 → RunCoordinator → 核心动作，有界等待）；`before_subagent_dispatch` 契约与五项能力矩阵落地（claude SDK 核验先行，opencode writer 子代理 fail-closed） | 派发屏障 + 组仲裁 + 单测 | P3.5 | 组检查点位于 message 内首个副作用之前；组恢复点不可变；无 cwd 注入 runtime 上 writer 子代理被拒绝启动 |
| P4.2 | writer/reader 子代理作用域 + 子代理回滚（§9.4 异步确认链，Graph 与单 session 共用）+ `core/restore/` ConversationRestorer 迁移提炼（message 级截断 + seed + `start_session_with_resume` + Run/作用域/epoch 绑定）；`replay_dispatch` 仅限单副作用组 | 子代理回滚 + 集成测试 | P4.1 | 新 session 在 Graph 新代状态持久化之前不得创建（崩溃注入验证）；失效范围 = 组检查点之后全部；多副作用组 replay 请求被拒绝 |
| P4.3 | 已合并节点回滚：集成线回退（先校验后 reset：已登记/属当前集成线/是当前 HEAD 祖先/不含 writer_local）+ 谱系失效集计算 + 保留者按原集成序换序重放 + root epoch 传播 | 已合并回滚 + 集成测试 | P4.1 | 旧 root epoch 请求被拒；非法 reset 目标拒绝后根 HEAD/工作树/epoch/Graph 状态不变；基线谱系不含目标的写者保留不重跑、含目标的失效重跑 |

### P5：单 session 迁移 + 插件退出

| ID | 任务 | 产出 | 依赖 | 验收 |
|---|---|---|---|---|
| P5.1 | 资产下沉：单 session 作用域树接入 + 终态三方同步（基线参照、含删除生效、外部变化停止覆盖并报告冲突）+ live view 端点 | 单 session 迁移 + 单测 | P4.2 | D5 行为正确；agent 删除基线文件终态同步生效；基线外用户新增文件不被误删 |
| P5.2 | 能力迁移：对话恢复/文件预览/历史检查点读取/清理（reaper）并入核心与主 Web（控制台发行包，跨仓 PR）；`import-db` 独立为 session 导入能力；历史快照兼容读取器 | 迁移后的能力 + 回归 | P5.1 | 全部现有对外端点有主 Web 等价物；单 session 重放/import 在迁移前后两点不回退 |
| P5.3 | 删除 `packages/plugins/agent-traceback/`；snapshot 兼容模式收尾 | 插件删除 | P5.2 且全部开发完成 | 全量测试与 e2e 全部通过后插件目录出仓；`file_provenance` 不受影响 |

## 4. PR 级施工顺序

每个 PR 单一主题；变更量建议 1000 行以内，无其他硬约束；行为变化不与结构搬移混排。S 编号即建议的 PR 顺序；跨仓 PR（S2w 控制台路由、S3c、S3d）开在各所属仓库，此处登记以保持全局顺序可见。体量预案：S1a/S1b/S2a/S4a 单笔可能超过建议值，施工时可按子主题再拆（如 S2a 拆「执行桥+出生映射+cwd 注入」与「集成定序+闸门+打点」两笔——出生映射依赖执行桥提供工作区，顺序不可倒），拆分不改变依赖关系。

| 序号 | 标识 | 内容 | 对应任务 | 前置 | 出口检查 | 完成 |
|---:|---|---|---|---|---|---|
| 1 | S0a | 文档集评审定稿（ADR-0001 + 双 spec + 本计划），协议冻结 | P0.1 | — | 评审通过 | ☑ |
| 2 | S0b | 量化 spike 脚本与执行：fix-findings 真实目录五项指标测量，结论回填 ADR-0001 | P0.2 | S0a | 指标可接受或路径策略调整结论落档 | ☐ |
| 3 | S1a | `core/workspace/repository.py`：私有仓 + 基线导入（保留路径/排除清单）+ 首层物化与 `scope_base_commit` + 根双引用 | P1.1 | S0b | 基线完整性验收（含用户 `.git`、脏文件、未跟踪文件） | ☐ |
| 4 | S1b | `core/workspace/scope_tree.py` + `gate.py` + `oplog.py`：作用域账本、两层 epoch、闸门状态机、三阶段 saga | P1.2 | S1a | saga 崩溃注入用例全过（含 GIT_WRITTEN 前判据、分叉即停） | ☐ |
| 5 | S1c | `core/workspace/manager.py` + `integration.py` + `manifest.py`：worktree 生命周期/GC、串行集成、按路径集成树、两维路径策略；动作/通知协议类型 | P1.3 | S1b | fake 编排经协议建区/集成；四组合路径策略正确 | ☐ |
| 6 | S1d | `core/checkpoints.py` 迁移（平铺单文件对偶 `core/runs.py`）+ RunStore schema 迁移接线：核心新建 `scope_checkpoints` 表与插件旧表并存（spec §7），检查点类型与持久化顺序的 store 侧支持（生命周期触发接线在 S2a；S1c 与本行并行、不触发尚不存在的 store） | P1.4 | S1b | schema 经 RunStore 管理；并存期插件写入互不干扰；旧表留存零改动（历史行兼容读取器归 S5b） | ☐ |
| 7 | S2a | Graph 接线：engine cwd/`WORKSPACE_DIR` 注入、executor/verifier 映射 `PrepareScopeWorkspace`、`awaiting_integration` 定序、executor→verifier 闸门、RunCoordinator 生产执行桥（动作协议的生产派发接线） | P2.1 | S1c、S1d | 双节点并行写不相交路径隔离 + 按序集成；verifier 时序正确 | ☐ |
| 8 | S2b | 两档冲突检测（`git_merge` 默认 / `strict`）+ claims 软预检 + `fault.workspace_conflict`/`fault.scope_quiesce_failed` 入 watchdog 词表 | P2.2 | S2a | git_merge 档可合并放行/真冲突拒绝用例；strict 档全部路径事件对拒绝用例；writer_local 不误报 | ☐ |
| 9 | S2c | 未合并节点回滚异步状态机：`plan_restore`（含动态子图特例）、作用域动作、driver/op 异步化与 operation_id、内核 CoreAPI scopes/checkpoints/operations 查询与回滚方法（HTTP 路由装配在 S2w，控制台发行包） | P2.3 | S2b | 幂等、旧 epoch 拒绝、失效集外零变化、redo-on-latest-head 语义；rollback_to 验收失败回滚按 §9.1 编排语义执行（目标既有集成成果保留、不撤文件）；target 为子图节点时 generator 置回 passed 不重跑、其子图不废弃、回滚边记在 generator | ☐ |
| 10 | S2w | 控制台发行包：scopes/checkpoints/operations 查询与 driver/op 异步回滚的 HTTP 路由装配（薄 handler 走 CoreAPI） | P2.3 | S2c | 端点可达；适配器可经 `GET .../operations/{id}` 轮询回滚状态 | ☐ |
| 11 | S3a | watchdog 超时能力：子代理独立计时 + 节点累计超时 `fault.node_timeout`（与 P1/P2 可并行，此处按合入序排列） | P3.1 | S0a | 慢任务与卡死任务均按时告警；暂停不计时、重启保留已用时间 | ☐ |
| 12 | S3b | 当前文件加载：更新现有 Run 任务定义、保留节点状态与产物、不新建 Run | P3.2 | S2c | 修复后原 Run 加载新定义继续 | ☐ |
| 13 | S3c | 接入仓：adapter.mjs + opsbot_bridge.py + start_online.py（含定时 tick 与重启恢复核对） | P3.3 | S2w、S3a | 重启恢复原任务，不重复创建 loop/Graph | ☐ |
| 14 | S3d | RSI 仓：budget.exceeded 分支 + 暂停回调 + policy 中断 + 证据/知识卡片/验证经适配器 + 本地应用候选 | P3.4 | S3c | fault 后远端真实暂停并进入原生诊断 | ☐ |
| 15 | S3e | e2e 联调与验收：fix-findings 全链路 + 整图完成后从头新一轮（e2e 测试代码在本仓 `tests/e2e/`，联调跨仓） | P3.5 | S3b、S3d | §7 验收标准全过；worktree 成本实测回填 | ☐ |
| 16 | S4a | dispatch_group + ⑤ 副作用屏障 + 屏障调用链 + 能力矩阵落地（claude SDK 核验，opencode fail-closed） | P4.1 | S3e | 组检查点先于首个副作用；无 cwd 注入拒绝 writer 子代理 | ☐ |
| 17 | S4b | writer/reader 子代理作用域 + 子代理回滚异步确认链 + ConversationRestorer 迁移提炼 + replay_dispatch 单组限制 | P4.2 | S4a | 定序崩溃注入通过；多副作用组 replay 拒绝 | ☐ |
| 18 | S4c | 已合并回滚：先校验后 reset + 谱系失效集计算 + 保留者按原集成序换序重放 + root epoch 传播 | P4.3 | S4a | 非法目标拒绝后零改动；旧 root epoch 请求被拒；谱系不含目标的保留者不重跑、含目标的失效重跑 | ☐ |
| 19 | S5a | 单 session 迁移：作用域树接入 + 终态三方同步 + live view 端点 | P5.1 | S4b | D5 行为与三方同步验收 | ☐ |
| 20 | S5b | 能力迁移：对话恢复/文件预览/历史检查点/清理/import-db 独立 + 兼容读取器（旧 `checkpoints` 表历史行读取在此实现，S1d 只保证旧表留存） | P5.2 | S5a | 对外端点等价；重放/import 不回退 | ☐ |
| 21 | S5c | 删除 `agent_traceback` 插件，snapshot 兼容模式收尾 | P5.3 | S5b 且全部开发完成 | 全量测试与 e2e 通过后插件目录出仓 | ☐ |

### 4.1 里程碑出口

- **M0（P0）**：协议冻结；spike 五项指标有结论，路径策略最终取值落档。
- **M1（P1）**：临时仓上基线含未提交+未跟踪+内部 git 保留路径；作用域 CRUD/epoch/幂等/闸门单测通过；fake 编排经协议建区/集成；saga 三阶段崩溃注入通过。
- **M2（P2）**：双节点并行写不相交路径隔离+按序集成；git_merge 档同文件不相交 hunk 并行写自动合并放行、真实合并冲突检出，strict 档同文件并行写（含 Git 可自动合并）检出；writer_local 修改不误报冲突；异步回滚幂等、旧 epoch 回执被拒、失效集外零变化；验收失败回滚不撤已集成成果（spec §9）；动态子图回滚特例符合 spec §9.1.1；旧单 session 与插件路径零改动。
- **M3（P3）**：fix-findings e2e 跑通（超时→暂停→导出→修复→异步回滚→resume→下游继续）；重复回滚幂等；worktree 实际成本评估完成，P4/P5 节奏据此确认。
- **M4（P4）**：规格 §17.1 第 8/23/24/28/31–35 项验收通过；无 cwd 注入 runtime 上 writer 子代理被拒绝启动；已合并回滚后旧 root epoch 请求被拒、reset 目标为集成线祖先、谱系失效集正确（不含目标的保留者不重跑）；失效范围 = 组检查点之后全部。
- **M5（P5）**：全部现有对外端点有主 Web 等价物；单 session 重放/import 迁移前后两点不回退；全部开发完成且全量测试与 e2e 通过后，`agent_traceback` 目录删除退役。

### 4.2 新增门禁

| 门禁 | 检查方式 | 生效 |
|---|---|---|
| `core/workspace`、`core/checkpoints`、`core/restore` 不 import 插件与 Graph；插件不 import core（SDK 端口） | import-linter + 架构测试 | S1a |
| git 子进程调用只允许出现在 `core/workspace/repository.py` 与 `core/workspace/integration.py`（其余模块经二者 API） | tests/architecture grep 型测试 | S1a |
| 核心动作执行只经 RunCoordinator/会话宿主（唯一 I/O 手），hook 不直写核心 | 架构测试 | S2a |
| 集成与冲突检测公共基准为 `integration_base_commit` 且限定 merge 路径集 | 针对 #31 的回归用例 | S2b |
| 回滚 API 幂等（client_op_id + expected_epoch），同键不同参数返回冲突错误 | 契约测试 | S2c |
| 无检查点/traceback 关闭时文件回滚明确拒绝 | 契约测试 | S2c |
| 屏障能力缺失时 fail-closed（不静默降级保证） | 集成测试 | S4a |

## 5. 依赖与关键路径

```text
S0a 协议冻结 ─────────────────────────────────────→ S3a 超时能力（与 P1/P2 并行）
    ↓                                                             │
S0b 量化 spike                                                    │
    ↓                                                             │
S1a 私有仓/基线 → S1b 账本/闸门/oplog → S1c 工作区/集成 → S1d 检查点迁移
                                                            ↓
                                          S2a Graph 接线 → S2b 冲突检测 → S2c 异步回滚
                                                                          ↓     │
                                            ┌─────────────────────────────┤     │
                                            ↓                             ↓     ↓
                                          S3b 当前文件加载          S2w 控制台路由 → S3c 适配器/启动脚本
                                            │                                        ↓
                                            │                                  S3d RSI 侧信号与验证
                                            └─────────────┬──────────────────────────┘
                                                          ↓
                                          S3e fix-findings e2e（M3，本仓 tests/e2e/）
                                                          ↓
                                          S4a dispatch_group+屏障 ──────→ S4c 已合并回滚
                                                          ↓
                                          S4b 子代理回滚
                                                          ↓
                                          S5a 单 session 迁移 → S5b 能力迁移 → S5c 删插件
```

D7 双轨（opencode 上游推动 + 宿主接管 task 工具 spike）与 P4 并行启动，不阻塞 P1–P3。

## 6. 风险与应对

| 风险 | 触发信号 | 影响 | 应对/回滚 |
|---|---|---|---|
| spike 指标超标（GB 级产物频繁打点） | 提交分钟级或磁盘翻倍 | 高 | 相应路径改 `rebuildable` 或评估 reflink/内容寻址仓，结论回填 ADR；不强制入 Git |
| opencode 屏障与 cwd 注入缺位 | 能力矩阵核验失败 | 中 | 严格模式 writer 子代理 fail-closed；claude SDK 路径先行；路线 B spike |
| 闸门后台进程不可静止形成等待环 | 父 agent 长时间等 task result | 高 | 父阻塞时有界等待后 `fault.scope_quiesce_failed` 收口；父未阻塞才允许延迟 |
| 多步操作中途崩溃 | 协调器进程被杀 | 高 | 三阶段 saga + 三态核对；分叉即停绝不自动 reset |
| 既有单 session 行为回退 | 迁移前后重放/import 回归失败 | 高 | P5 才动旧路径；迁移期插件经 SDK 端口；两点不回退为 M5 硬验收 |
| 双基线字段误用（scope_base 当冲突基准） | fix-findings 集成误报 conflict | 中 | §8.1/§9.3 强制 integration_base/integration_commit；验收 #31/#32 兜底 |
| 跨仓联调顺序错位（RSI/接入仓先于本仓能力） | e2e 接口不匹配 | 中 | S3c/S3d 以 S2w/S3a 出口为前置；接口契约以 RSI 闭环规格 §8 与 traceback 规格 §9.1/§14 为准 |

## 7. 验收标准

- [ ] 规格 §17.1 关键验收清单第 1–38 项全部通过（各阶段对应项随里程碑验收）。
- [ ] fix-findings e2e：超时 → 暂停 → 导出 → 修复 → 回滚 → resume → 下游继续；候选在同时限内完成原验收；整图完成后用修复后源码从头开始下一轮。
- [ ] 重复事件不重复回滚；旧代事件不改变新代；恢复文件后节点状态与文件一致；候选测试通过后主图真实继续。
- [ ] 关闭 traceback 时拒绝文件回滚；开启后可恢复纯节点起点；并行任务按检查点重置；存在中间产物（writer_local/rebuildable）时按 traceback 规格 §6.2 路径策略恢复，无法确定性重建则明确拒绝。
- [ ] 每个阶段都有可回滚路径，旧单 session 与插件路径在 P5 前零改动。

## 8. 进度记录

| 日期 | 进展 | 证据/链接 | 变更或阻塞 |
|---|---|---|---|
| 2026-11-23 | 创建 ADR-0001、双 spec 与本计划 | `docs/adr`、`docs/specs`、`docs/plans` 下四份文档 | S0a 评审中 |
| 2026-11-24 | S0a 首轮评审意见落档：检查点核心/插件分表（spec §7）、控制台发行包登记（§2/P2.3/S2c）、RunCoordinator 生产执行桥归属（P2.1/S2a）、引用与状态口径修订（B1–B4）；冲突检测档位化 `conflict_policy`（ADR D9 + spec §8） | 同上 | 等待评审关闭 |
| 2026-11-24 | S0a 二轮评审落档：N1 验收失败回滚语义明写（spec §9 + 验收 #38）；S2w 控制台路由行登记（S3c 前置改为 S2w）；依赖图修正（S0a→S3a、S4c 并立 S4a）；`strict` 档更名 `path_set`（后于三轮回退）；claims 与 git_merge 倒挂消除（§8.3）；体量约束放宽为建议 1000 行以内；RSI spec 三处旧口径同步 | 同上 | 等待评审关闭 |
| 2026-11-24 | 单值配置项移除（需求方意见）：`on_unsafe_workspace`、`single_session.root_sync` 出配置面（连同 `single_session:` 节），D2/D5 改为 spec §15 明文固定行为 | 同上 | 等待评审关闭 |
| 2026-11-24 | S0a 三轮落档：`path_set` 档名回退为 `strict`（需求方意见）；§4 表 S2b/S2c 并行修复；§7 验收项计数 1–38；风险表 S3c 前置改 S2w；P1.4 验收与 S1d 口径统一；动作词表自洽（§9.1 改用 AbandonScope→ScopeAbandoned，RestoreCommitted 入 §5.2 动作表）；§6.1 状态机补 Integrated/Released→Discarded；显式已合并回滚 P4.3 前明确拒绝；claims×档位 2×2 写死；目录评审四微调落档（`core/checkpoints.py` 平铺、restore/ 边界、oplog 横切声明、git 出口收敛 + 新门禁） | 同上 | 等待评审关闭 |
| 2026-11-24 | 补选择性保留（换序重放）安全性论证与边界（spec §9.2.1，回应「并行写者同基线、回滚先到者是否拖累后到者」）；ADR D3 行注明升级路径 | 同上 | 等待评审关闭 |
| 2026-11-24 | D3 改写（需求方确认）：第一版即选择性保留——失效集按基线谱系计算、保留者按原集成序换序重放；spec §9 表/§9.2 流程/§9.2.1 转为正式机制、验收 #7，ADR D3 与主决策，plan P4.3/S4c/M4 同步 | 同上 | 等待评审关闭 |
| 2026-11-24 | **S0a 关闭**：三轮评审落档，ADR/双 spec 状态推进 Frozen，协议进入实现阶段；下一笔 S0b 量化 spike | 同上 | S0b 待 fix-findings 真实目录 |
