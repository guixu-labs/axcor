# Reviewer 任务目标会话内更新设计

## 状态

- 设计阶段：已评审
- 作用范围：内核（axcor）`SessionAPI` / `Orchestrator`、Web 控制台（opsbot）的会话路由与 Session Detail

## 背景

reviewer 的 prompt 由固定模板加「会话任务目标」段组成，可变部分只有 `task_goal`。它目前在会话启动时一次性写入两处，之后没有任何更新入口：

- `SessionConfig.reviewer.task_goal`：reviewer 的四类评审（Stop、AskUserQuestion、阶段审查、自然语言提问）渲染 prompt 时读取；
- `SessionConfig.criteria["task_goal"]`：feedback evaluator 每轮判定完成度时读取。

会话跑起来后才发现目标写宽、写窄或写偏，只能改项目 yaml 后另建会话。

检查点重放的 `reviewer_task_goal` 覆盖项也只写 `criteria.task_goal`：origin 自带 reviewer 快照时，深拷贝带来的旧目标会让覆盖项到不了 reviewer。

Web 控制台的 Agent 调用树里，Reviewer 节点由 reviewer hook 事件推导，第一次评审发生前节点不存在；节点详情里的「Reviewer 目标」是只读的，含义是「启动时冻结」。

## 目标

1. reviewer 开启的会话，Agent 调用树自创建起常驻显示 Reviewer 节点。
2. 点击 Reviewer 节点可查看并编辑当前会话的 `task_goal`，点击「更新」后保存。
3. 更新对运行中的会话自下一次评审起生效，不需要重启会话或 controller。
4. 更新随会话持久化：controller 重启、会话再次激活、交接接续、检查点重放继承时都沿用新值。
5. `reviewer.task_goal` 与 `criteria.task_goal` 始终一致。
6. 任何未删除的会话都能更新，不按会话状态或 reviewer 状态设限。

## 非目标

1. 不回写项目 `workflow-controller.yaml` 或全局配置；新建会话仍取项目配置。
2. 不提供 run 级批量更新。graph 成员被 retry / rollback 重新派发为新 session 时，仍取 run 级 reviewer 配置。
3. 不改 reviewer 的 prompt 模板与其余 reviewer 参数（model、thinking、`max_tokens` 等）。
4. 不打断、不重跑进行中的评审。
5. 不在时间线展示更新记录，不提供更新历史视图。

## 设计要点

### 写入口（axcor）

1. `Orchestrator.set_session_reviewer_task_goal(session_id, task_goal)` 是会话内更新 `task_goal` 的唯一入口，一次写入 `reviewer.task_goal` 与 `criteria.task_goal` 两处。
2. 入参去除首尾空白后为空则抛 `ValueError`，不回落 `DEFAULT_TASK_GOAL`：空目标会让 reviewer 失去判定依据。
3. 新值与当前值相同时直接返回，不写入、不发事件。
4. 写入顺序：定位会话 → `_ensure_session_snapshots` 补齐快照 → 写两处 → 同步在跑的 reviewer → `pool.persist` → 发审计事件。
5. `SessionAPI.set_session_reviewer_task_goal(session_id, task_goal)` 转调并返回保存后的值；会话不存在抛 `ValueError`，与 `set_session_reviewer_mode` 同形。
6. 检查点重放的 `reviewer_task_goal` 覆盖项（agent-traceback 的 `_apply_overrides`）遵循同一规则：写 `criteria.task_goal`，origin 自带 reviewer 快照时一并改写 `reviewer.task_goal`；快照为 None 时留给激活时的 `_ensure_session_snapshots` 按 `criteria` 补建。

### 生效时机

1. reviewer 的四类评审入口都在调用时读 `ReviewerService` 的运行时配置，目标不固化在 prompt 里。
2. 会话运行期间，`Orchestrator._session_reviewers` 持有该会话的 `ReviewerService`。更新经公有方法 `ReviewerService.set_task_goal()` 改写其运行时配置（不跨类访问 `_config`），下一次评审调用即用新目标。
3. 已开始的评审不受影响，仍按开始时的目标出结论。
4. 会话没有在跑的 `ReviewerService` 时（从未激活、已结束、已离开内存池），只改会话配置并持久化；下次激活时 `_ensure_session_reviewer` 按会话配置重建，自然得到新目标。
5. feedback evaluator 每轮现读 `session.config.criteria`，下一轮即用新目标。
6. manual 模式下同样可更新，切回 auto 后按新目标评审。

### 不按会话状态设限

1. 能否更新不依赖会话状态，也不依赖 reviewer 节点状态（如 `let_exit` 之后的 completed）。判定「会话是否结束」没有可靠的判定依据，而收窄验收标准后再次判定正是本能力要支撑的场景。
2. 会话在内存池内：直接更新。
3. 会话已离开内存池：沿用 `SessionAPI.get_session` 的按需恢复方式，即 `pool.restore_from_store` → 更新 → `pool.persist` → `pool.discard`，不改变池驻留。
4. 会话记录不存在（已删除或从未存在）：抛 `ValueError`，不做任何写入。
5. workflow（graph）成员会话走同一入口，不套用 pause / stop / inject 的 409 拦截，与 `reviewer-mode` 一致。run 平面独占写的是成员的 pause / resume / stop / inject，reviewer 设置不在其列。

### 继承

1. 交接（breather）产生的接续会话深拷贝原会话的 `reviewer` 与 `criteria`，继承更新后的目标，无需改动。
2. 检查点重放深拷贝 origin 的会话配置，继承 origin 当前的 `task_goal`。
3. graph 成员 retry 会清 session id 重新派发，新成员取 run 级 reviewer 配置，不继承被替换成员的会话内更新。

### 审计

1. 每次成功更新发一条 `LOG_AUTO` 事件，`action=reviewer_task_goal_updated`，`data` 含 `previous_task_goal` 与 `task_goal` 全文。评审结论取决于当时的目标，事件记录要能对上「哪次判定用的是哪版目标」。
2. 不用 `LOG_MANUAL`：前端的等待人工回复判定把 `log_manual` 当作人工回复，混入会误清等待状态。
3. 时间线不展示该事件。现有过滤器已隐藏不带 `hook` 字段的 `log_auto`，前端无需为此改动。

### HTTP 接口（opsbot）

1. `PUT /api/v1/sessions/{session_id}/reviewer-task-goal`，请求体 `{"task_goal": "<非空字符串>"}`。
2. 成功返回 `{"ok": true, "task_goal": "<保存后的值>"}`。
3. `task_goal` 缺失、不是字符串、去除首尾空白后为空，或会话不存在：返回 400，`detail` 为错误信息。
4. 路由不挂 `_session_op`，与 `reviewer-mode` 同形；登记进 `tests/contract/manifest.yaml` 的 stable 段，登记状态码 400。
5. `SessionInfo.reviewer_task_goal` 字段名不变，语义从「启动时冻结的值」改为「会话当前生效的值」。

### Agent 调用树（opsbot）

1. `reviewerEnabled` 为真时，`deriveTopology` 直接创建 Reviewer 节点与指向 main 的 feedback 边，不再等 reviewer hook 事件。节点初始状态 `idle`，`startedAt` 为空。
2. 首个 reviewer hook 事件到达时补 `startedAt` 并转 `running`。之后的迁移不变：`let_exit` 转 `completed`，`block` 转 `running`，会话离开 running 时 `running` 转 `idle`。
3. reviewer 开启的会话因此始终有非空拓扑：没有子代理的会话和历史会话也会显示「Agent 调用树」卡片。reviewer 关闭的会话行为不变。
4. `AgentTopologyGraph` 不改：Reviewer 仍在首行，点击即 `selectAgent`。

### Reviewer 节点详情（opsbot）

1. 「Reviewer 目标」区域始终渲染，从只读展开改为直接可编辑：textarea 绑定草稿，草稿在弹窗打开或切换节点时初始化为 `session.reviewer_task_goal`，保留换行。
2. textarea 下方一行说明「更新自下一次评审起生效」，再下方是「更新」按钮。
3. 按钮在以下任一情况下禁用：草稿去除首尾空白后为空、与已生效的值相同、请求进行中。
4. 点击「更新」调用上述接口。成功后 toast 提示，以响应中的 `task_goal` 同步草稿并让按钮立即回到禁用态，再重拉 `GET /api/v1/sessions/{id}`；失败时 toast 展示后端 `detail`。
5. 编辑器不按会话状态或节点状态禁用。
6. 关闭弹窗即丢弃未保存的草稿，不弹确认。
7. 弹窗打开期间，其他端保存的新值不覆盖正在编辑的草稿；两端都保存时以最后一次为准。
8. 删除 `showReviewerTaskGoal` 与只读 `<pre>`；`types/api.ts` 中 `reviewer_task_goal` 的注释同步修改。

### 失败语义

1. `pool.persist` 沿用既有语义：落库失败只记日志、不抛出。此时内存中的会话配置与在跑的 reviewer 已是新值，接口返回成功，但 controller 重启后会回到旧值。
2. 除此之外，接口返回 400 时不产生任何写入与审计事件。

### 版本与发版

1. 本能力新增内核 API，opsbot 依赖它。axcor 的功能 PR 内 bump 根 `pyproject.toml` 版本（`0.2.2` → `0.2.3`，以合入时的下一个未占用版本为准），用 `uv version` 同步 `uv.lock`，合入 main 后由 release 流水线发布。
2. Release 发布后，opsbot 升级 `pyproject.toml` 的 axcor Git 标签与 `axcor.lock` 的 `axcor_sha`，执行 `uv lock` 并确认解析出的提交与 `axcor_sha` 一致；路由与前端改动随同一 PR 合入。
3. 本地联调时 opsbot 以本地路径依赖指向 axcor 工作区，合入前切回标签。

## 施工计划

axcor：

1. `ReviewerService.set_task_goal()`；`Orchestrator.set_session_reviewer_task_goal()` 与审计事件构造函数（`orchestrator_helpers.py`）；`SessionAPI.set_session_reviewer_task_goal()`。
2. `SessionInfo.reviewer_task_goal` 的注释改为「会话当前生效的值」。
3. agent-traceback：`_apply_overrides` 的 `reviewer_task_goal` 覆盖项两处同写；测试改用自带 reviewer 快照的 origin，断言 `criteria`、快照与重建的 reviewer 运行时配置。
4. 内核测试：运行中会话更新后两处落点、在跑 reviewer、持久化与审计事件一致；空目标与不存在的会话被拒绝且无副作用；池外会话更新后不改变池驻留；workflow 成员会话不被拒。
5. `docs/ARCHITECTURE.md` Session Orchestrator 第 6 条补充 `task_goal` 可会话内更新；`docs/event-system.md` 的 `LOG_AUTO` 发射点表补一行。
6. 根 `pyproject.toml` 版本 bump 并发布。

opsbot：

7. `web/routes/sessions_routes.py` 增加路由；`tests/contract/manifest.yaml` 登记后运行 `python scripts/generate_api_contract_doc.py` 重新生成 `docs/api-contract.md`；`tests/contract/test_stable_api_contract.py` 与路由测试补对应用例。
8. `useTopology.ts` 常驻 Reviewer 节点；`SessionDetailView.vue` 的 Reviewer 目标编辑器与更新逻辑；`types/api.ts` 注释；更新 `useTopology.test.ts` 与 `SessionDetailView.test.ts`。
9. `docs/Web-UI.md`「Agent 调用树」条目中关于 Reviewer 目标的描述改写。
10. 升级 axcor 标签与 `axcor.lock`，执行 `uv lock`。

## 验收标准

1. reviewer 开启的会话创建后（尚无评审、无子代理），调用树即显示 Reviewer（idle）与 Main Agent。
2. reviewer 关闭的会话，调用树行为与改动前一致。
3. 点击 Reviewer 节点，弹窗显示当前 `task_goal` 的可编辑文本框与「更新」按钮；内容未改动或为空白时按钮禁用。
4. 修改并点击「更新」后接口返回 200，随后 `GET /api/v1/sessions/{id}` 的 `reviewer_task_goal` 为新值。
5. 运行中的会话，更新后的下一次评审（Stop、AskUserQuestion、阶段审查、自然语言提问）所用 prompt 的「会话任务目标」段为新值；更新前已开始的评审仍按旧目标出结论。
6. 更新后 `criteria.task_goal` 与 `reviewer.task_goal` 相同，feedback evaluator 下一轮读到新值。
7. 已结束的会话与已离开内存池的历史会话都能更新；重启 controller 后 `GET /api/v1/sessions/{id}` 仍返回新值；会话再次激活后 reviewer 按新目标评审。
8. reviewer 已 `let_exit` 的会话仍可更新，恢复会话后按新目标重新评审。
9. workflow 成员会话可直接更新，不被 409 拒绝；节点 retry 新派的成员使用 run 级配置。
10. 交接产生的接续会话，其 `task_goal` 与原会话更新后的值一致。
11. 检查点重放带 `reviewer_task_goal` 覆盖项、origin 自带 reviewer 快照时，fork 的 `reviewer.task_goal`、`criteria.task_goal` 与重建的 reviewer 运行时配置都是覆盖值。
12. 空白目标与会话不存在均返回 400，且不改变会话配置、不发审计事件。
13. 每次成功更新落一条 `action=reviewer_task_goal_updated` 的 `log_auto` 事件，含前后值；时间线不展示。
14. 更新不写项目 `workflow-controller.yaml` 与全局配置。
15. `tests/contract/manifest.yaml` 与 `docs/api-contract.md` 包含新端点；相关内核测试、路由测试与前端测试通过。
16. axcor 新版本发布后，opsbot 的 `pyproject.toml` 标签与 `axcor.lock` 指向该发布。
