# Agent Traceback 插件开发者指南

> 面向**想改 `packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback` 的开发者**。讲清两件事：
>
> 1. **代码做了什么**——每个模块职责、关键调用链、不可破坏的不变量；
> 2. **为什么这样写**——背后的设计权衡，便于改进时不踩坑。
>
> 阅读前置：了解 Workflow Controller 的 orchestrator / driver / event_router / plugin 概念即可。深逻辑细节可对照 深逻辑解析（实时）（原链接目标未入库） 与 深逻辑解析（db 重建）（原链接目标未入库）。

---

## 0. 一句话

本插件干两件事，**共享同一套重放底层**：

- **实时检查点重放**：每个**主 agent 直接子代**的 subagent start 时刻在独立 git 仓给整个工程目录打一次文件态快照，session 跑完后用户可任选一个检查点发起「透明重放」——把文件态 restore 到隔离 tempdir、按 message 级截断主 agent 对话史、写回该 tempdir 内的独立 `opencode.db` 成预制 session、起新 worker `--resume` 它续跑。孙 agent（嵌套派发，事件带非空 `parent_agent_id`）不打检查点——检查点语义 = 主 agent 派发边界，重放截断点是其主 session 的 task part（`find_main_part_id_for_subagent`），孙的派发 part 在子会话里、主 session 无锚。
- **从 cannbot session db 文件重建会话**：把一个只含 session 历史的 cannbot SQLite db 喂进来——按时间序逐字节回放所有 `write`/`edit`/`bash`（含 git）工具调用，重建每个检查点时刻的文件态（含项目 `.git` 历史），把主 agent 的 `Turn`/`ToolCall` 转写成 opencode.db 的 `session`/`message`/`part` 行，按 `task` 调用边界推导检查点——产出物与实时跑完的 session **结构完全一致**，于是 `TracebackRunner` 零改动复用。

核心承诺：**重建产物与实时快照同构**。任何对实时链路的修改，都要同步考虑重建链路能否产出同构产物；反之亦然。

---

## 1. 插件文件结构

```
packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/
├── __init__.py            # 空
├── config.yaml            # 插件配置（见 §7）
├── plugin.py              # AgentTracebackPlugin：钩子入口 + 引擎/存储 per-project 缓存 + 重放/重建入口
├── checkpoint_store.py    # Checkpoint dataclass + checkpoints 表 DDL/CRUD（与 events 同库）
├── snapshot_engine.py     # SnapshotEngine：rsync→git add→commit，restore/preview 文件态
├── l2_reader.py           # 读 opencode.db 主 session part 流、按 message 级截断、seed 预制 session
├── traceback_runner.py    # TracebackRunner：编排一次透明重放的 7 步流程
├── web_api.py             # FastAPI router：列检查点 / 发起重放 / 预览文件态 / 导入 db
└── session_reconstructor/ # 从 cannbot db 重建会话的子包
    ├── __init__.py
    ├── db_reader.py            # cannbot db 读取 + 统一时间线交错排序
    ├── l2_transcriber.py      # Turn/ToolCall → opencode.db part/message 转写
    ├── file_state_rebuilder.py # 重放 write/edit/bash + git 历史 + 快照仓
    ├── checkpoint_deriver.py  # task 边界推导检查点
    └── orchestrator.py        # 编排上述四步 + 注册 Workflow Controller session
```

每个模块都遵循「**best-effort、绝不抛异常打断 workflow**」的总原则。快照失败、超时、git 不可用、db 写不进……全部降级为 `status=timeout/unavailable/error` 落库或返回 `ok=False`。改进时**必须保持这条底线**——插件永远不应该因为自己的故障把用户任务弄丢。

---

## 2. 数据流全景

### 2.1 实时检查点 + 透明重放

```
┌───────────────────────── 检查点写入（session 跑时） ─────────────────────────┐
│ worker：「我要派 subagent X」                                                  │
│   ↓ opencode poller 发现 opencode.db 新冒出 ses_X                              │
│   ↓ driver hooks.on_subagent_start(sid, {agent_id: ses_X, ...})               │
│   ↓ SubagentTracker.on_start → await plugin.on_subagent_start(...)     │
│   ↓    AgentTracebackPlugin._snapshot_subagent_start                          │
│   ↓       ├─ _resolve_engine(cwd)              [per-project 缓存]              │
│   ↓       ├─ cp_store.list_sync               [算 next_n / parent]            │
│   ↓       └─ await engine.snapshot(...)       [阻塞 worker，管道背压]           │
│   ↓            ├─ async with self._lock       [串行化]                          │
│   ↓            ├─ asyncio.wait_for(_do_snapshot, 30s)                          │
│   ↓            │    ├─ _sync_mirror()         [rsync 或 Python]                │
│   ↓            │    ├─ git add -A --force                                      │
│   ↓            │    ├─ git commit --allow-empty  [结构化 trailer]              │
│   ↓            │    └─ git rev-parse HEAD     → git_ref                        │
│   ↓            └─ await cp_store.store(cp)     [INSERT OR REPLACE]            │
│   ↓                                                                          │
│   钩子返回，worker 继续，subagent X 真正动手写文件                              │
└──────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────── 透明重放（用户主动） ───────────────────────────────┐
│ 用户在 web 点「从 checkpoint cp_xxx_003 重放」                                  │
│   ↓ POST /api/sessions/{sid}/traceback  body={checkpoint_id}                  │
│   ↓ plugin.run_traceback → TracebackRunner.run                                │
│   ↓   1. cp_store.get_sync(checkpoint_id) → 拿 git_ref + main_part_id + agent_id │
│   ↓   2. 解析 origin session（pool 或 event_router 的 config_json）             │
│   ↓   3. engine.restore(cp.git_ref, fork_cwd)  [git archive | tar -x]        │
│   ↓      + _init_git_sandbox(fork_cwd, origin_cwd)  [复制 .git + remote rm]   │
│   ↓   4. _resolve_origin_env(origin_cwd) → 命中 origin 的 opencode.db          │
│   ↓      + find_main_part_id_for_subagent → 定位 task part                    │
│   ↓      + find_exclude_message_id → message 级截断                          │
│   ↓   5. seed_prebuilt_session(...)  [截断 part 流写回 fork 独立 opencode.db]  │
│   ↓   6. 构造 base_config + _write_permission_config (external_directory: allow) │
│   ↓   7. orch.start_session_with_resume(...)  [带 birth_traceback 溯源]        │
│   ↓      └─ worker 收到首轮 prompt「请继续你之前的工作。」→ 从 L2 自然续跑       │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 从 cannbot db 重建会话

```
POST /api/sessions/import-db {db_path, target_cwd}
  ↓ plugin.import_session → ReconstructOrchestrator.import_session
  ↓   0. CannbotDbReader.validate + read_session + read_timeline + read_bridges
  ↓        └─ framework 校验（opencode / claude-code-proxy，见 §4.8.1 方言归一化）
  ↓   1. L2Transcriber.transcribe(timeline, meta, sdk_sid, bridges)
  ↓        → <target_cwd>/.opsbot/opencode/opencode.db
  ↓   2. FileStateRebuilder.replay(timeline)
  ↓        → <target_cwd>/.opsbot/snapshots/.git + <target_cwd>/.git + list[CheckpointFileState]
  ↓   3. CheckpointDeriver.derive(...)
  ↓        → list[Checkpoint]（用临时 session_id 占位）
  ↓   4. pool.create(SessionConfig) → session（持久化到 events.db sessions 表）
  ↓        ├─ session.sdk_session_id = sdk_session_id
  ↓        ├─ session.loop_status = "yes"（已跑完态，不进运行队列）
  ↓        ├─ session.plugin_metadata["agent_traceback"]["birth_traceback"] = {...}
  ↓        └─ event_router.store(Event(reconstructed_session_imported))
  ↓   5. dataclasses.replace(cp, session_id=session.id) → CheckpointStore.store_sync
  ↓        → 守护进程库（event_router.db_path_for(session.id)）的 checkpoints 表
  ↓   6. _synthesize_historical_events → 合成 SUBAGENT/STEP_FINISH 事件供 Dashboard 展示（Phase 1.7 后；之前为 RESULT）
  ↓
  返回 {ok, session_id, sdk_session_id, checkpoints[], fork_cwd, opencode_db_path}
```

重建完成后，**重放复用 `TracebackRunner.run`，零改动**——这是「同构承诺」的直接收益。

---

## 3. 核心概念与不变量

改进任何一处前，先确认没有破坏下面这些不变量。

### 3.1 四类存储

| 存储 | 路径 | 谁建/写 | 谁读 |
|---|---|---|---|
| `events.db` 的 `events` 表 | 守护进程库（启动目录 `.opsbot/events.db`） | 核心 `EventStore` | web、traceback 反查 `event_rowid` |
| `events.db` 的 `checkpoints` 表 | 同上（同库） | **插件 `CheckpointStore`** | web 列检查点、重放查 cp |
| 快照仓 git repo | `<cwd>/.opsbot/snapshots/.git` | **`SnapshotEngine`** | restore / list_files / read_file |
| origin 的 `opencode.db` | `~/.local/share/opencode/` 或 `<fork_cwd>/.opsbot/opencode/` | opencode 子进程 | `l2_reader` 读 L2 |
| fork 的 `opencode.db` | `<fork_cwd>/.opsbot/opencode/opencode.db` | **`l2_reader.seed_prebuilt_session`** | fork 的 worker 子进程 |

**关键约束**：

- `checkpoints` 表**物理上**留在 `events.db`（与 `events` 同库，便于 `event_rowid` 双向可查、备份/迁移一致），但 **DDL + CRUD 全部归插件**——核心 `EventStore` 不含 checkpoint 概念。插件经 `PluginServices` 能力端口（`events_db_path`）拿 db 路径后**自己开连接**操作。
- `.opsbot` 在快照 ignore 列表（`snapshot_engine.py:34-47`）——快照仓**不会**把自己镜像进 `working/`，每个 fork tempdir 的 `.opsbot/` 全新（无 origin 的快照仓残留）。**这对嵌套重放至关重要**：每层 opencode.db 仅凭 cwd 定位，无需在 session/checkpoint 上存额外字段。
- fork opencode.db **必须从 origin 整库 backup**（`l2_reader._init_fork_db_from_source`），保留 schema + `__drizzle_migrations` + `project` 元数据，使真实 opencode 打开 fork db 时**不重跑 CREATE TABLE**；然后清空所有 session 范围表行，仅含本 fork 的 seeded session，实现隔离。

### 3.2 透明重放首轮信号

首轮 prompt **必须**是通用「请继续你之前的工作。」（`traceback_runner.py:64`）——不注入任何「re-call X」「磁盘已回到 X 前」「前次产物不在盘上」之类暴露重放语义的指令。否则 LLM 会字面执行「重派 X」就停，破坏透明重放。worker 收到此信号后从 L2 + system_prompt + stages + L2 第一条 user message（origin 的 starting_prompt）自然续跑——从检查点时刻的对话史重新决定派 X、派 Y、派 Z、写 summary。

### 3.3 message 级截断（不可退回 part 级）

opencode 一条 assistant 消息可含多 part（reasoning + text + task）共享同一 `message_id`。part 级截断（只丢 task part）会留下同 message 的「我将调用 X」前置 text/reasoning——形成「孤儿文本」，worker 误以为自己已派过 X → 跳过 X。

**message 级截断**（`l2_reader.read_main_session_parts` 的 `exclude_message_id`）丢弃该 message 的全部 part 及其之后，worker 看不到「我将调用 X」的声明，从上一条完整 message 自然续跑。`exclude_part_id` 仅作为 schema 漂移/旧 db 无 `message_id` 时的退化回退。

### 3.4 task part 的 `state.output` 必须含子 session id

`l2_reader.find_main_part_id_for_subagent` 的匹配逻辑（`l2_reader.py:135`）：

```python
if sub_agent_id in str((part.get("state") or {}).get("output", "")):
    return str(pid)   # ← 定位到截断点
```

这是整个重放链路**最关键的一条不变量**：子 session id 必须出现在 task part 的 `state.output` 字符串里。`l2_transcriber` 对 `task` 工具显式回退（`l2_transcriber.py:374-381`）——若 `resultJson` 不含子 session id，强制构造 `task_id: <sub_sid>\n<result>` 注入。

### 3.5 两个 git 仓严格分离

重建时最容易踩坑。**项目 `.git` 与快照仓互不干扰**：

| | 项目 `.git`（`<cwd>/.git`） | 快照仓（`<cwd>/.opsbot/snapshots/.git`） |
|---|---|---|
| 创建者 | session 的 git 命令重放 | `SnapshotEngine._ensure_repo` / `FileStateRebuilder._ensure_snapshot_repo` |
| 内容 | 项目真实历史 | 检查点文件态快照（每检查点一 commit） |
| 是否镜像进 `working/` | **否**（`.git` 在 `DEFAULT_IGNORE_PATTERNS`） | 自身即仓 |
| 消费者 | `TracebackRunner._init_git_sandbox` 复制到 fork | `SnapshotEngine.restore` 抽文件态到 fork |

**快照不捕获项目 `.git`**，与实时 traceback 行为完全一致（实时时项目 `.git` 也被快照排除，fork 时由 `_init_git_sandbox` 从 origin 复制）。

### 3.6 best-effort 约定

| 钩子 / 入口 | 调用方 | async? | 阻塞 worker? | 失败影响 |
|---|---|---|---|---|
| `on_subagent_start` | driver hook 调用栈 | 是 | **是**（管道背压） | 快照超时/失败 → `status=timeout`，仍继续 workflow |
| `on_session_complete` | loop 完成回调 | 是 | 否（best-effort） | 落 `status=error`，不阻塞 loop |
| `on_session_delete` | event_router 删除回调 | **否** | — | try/except 回退，绝不抛 |
| `run_traceback` | web API `POST /traceback` | 是 | 起 fork session 后即返回 | 失败返回 `{ok:false, error}` |
| `import_session` | web API `POST /import-db` | 是 | 同步走完后返回 | 失败返回 `{ok:false, error}` |

注意 `on_subagent_start` 是 `await`，**不是 fire-and-forget**——driver 端 poller/解析逻辑在等这个 await 返回前不会继续推进，worker 子进程的 stdout 管道会被背压暂停。于是 `engine.snapshot(...)` 有窗口把文件态冻结在「subagent X 真正动手写之前」。

---

## 4. 模块详解

### 4.1 `plugin.py` — 插件入口

`AgentTracebackPlugin`（`plugin.py:41`）实现三个生命周期钩子 + 缓存 + 重放/重建入口。

#### 钩子

- `on_subagent_start`（`plugin.py:120`，**异步、阻塞型**）：转发到 `_snapshot_subagent_start`。调用链见 §2.1。
- `on_session_complete`（`plugin.py:136`，异步、非阻塞）：打终态检查点 `trigger=session_complete`、`agent_id=""`，让用户能从「workflow 跑完后的最终文件态」继续。幂等保护见 `plugin.py:312`：已有 session_complete 检查点直接 return。
- `on_session_delete`（`plugin.py:142`，**同步**）：删 `checkpoints` 表里该 session 的所有行；若该 session 是重放产生的 fork（`_fork_roots` map 里有登记），`rmtree` 掉整个 fork_root（含 ~2GB 的 fork opencode.db + .git 副本 + 恢复的文件态）。同步是有意为之——删除 session 是同步 API，插件清理也必须同步完成。

#### 缓存

- `_engines: dict[str, SnapshotEngine]`（`plugin.py:47`）：按 `Path(cwd).resolve()` 做 key 缓存 `SnapshotEngine`——与 `EventStoreRouter` 的 per-project 路由对称：同一 cwd 的 session 共用一个 engine（共用一个快照仓）。`_resolve_engine` 见 `plugin.py:189-210`。
- `_cp_stores: dict[str, CheckpointStore]`（`plugin.py:50`）：按 db 路径缓存 `CheckpointStore`。
- `_fork_roots: dict[str, str]`（`plugin.py:55`）：fork session_id → fork_root path 的 in-memory 生命周期追踪。`TracebackRunner.run()` 注册成功后登记；`on_session_delete` 取出 rmtree。**进程崩溃时此 map 丢失**——由启动期 `_reap_orphan_fork_roots`（`plugin.py:78-116`）按 TTL（默认 7 天）扫描 host tempdir 里的 `opsbot-tb-*` 目录后备清理。活动集保护：reaper 跳过 pool 中且 `last_activity` 在 TTL 窗口内的 session 引用的 fork_root（liveness = `now - last_activity < TTL`），崩溃后 IDLE ghost 超 TTL 即回收。活动集不可判定（pool 枚举端口未装配或枚举失败）时该次启动整体跳过回收——liveness 保护不 fail-open，宁可孤儿目录堆积等下次可判定的启动再清。

#### 重放 / 重建入口

- `run_traceback`（`plugin.py:418`）：构造 `TracebackRunner` 转发。
- `import_session`（`plugin.py:427`）：构造 `ReconstructOrchestrator` 转发，产出物落到 `target_cwd/.opsbot/` 下。

### 4.2 `config.yaml` — 插件配置

见 §7 详述。关键字段：`enabled`（默认 `false`，加载即启用才生效）、`snapshot_dir`、`snapshot_timeout_seconds`（30s，须远大于 git 本身耗时）、`reconstruct` 段。

### 4.3 `checkpoint_store.py` — 检查点表

`Checkpoint` dataclass（`checkpoint_store.py:21-43`）+ `checkpoints` 表 DDL/CRUD。

#### 表结构（`checkpoint_store.py:97-122`）

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id    TEXT PRIMARY KEY,        -- cp_<sid[:8]>_001 / cp_rc_<sid[:8]>_001
    session_id       TEXT NOT NULL,
    iteration        INTEGER NOT NULL,
    agent_id         TEXT,                    -- subagent 的 ses_xxx；终态检查点为空
    agent_type       TEXT,
    trigger          TEXT NOT NULL,           -- "subagent_start" / "session_complete"
    git_ref          TEXT,                   -- 快照仓 HEAD sha（核心字段）
    event_rowid      INTEGER,                -- 反向链接到 events 表
    main_part_id     TEXT,                   -- 重放时由 find_main_part_id_for_subagent 查
    parent_checkpoint_id TEXT,              -- 检查点链：上一条 cp_id
    created_at       TEXT NOT NULL,
    file_count       INTEGER DEFAULT 0,
    byte_size        INTEGER DEFAULT 0,
    status           TEXT DEFAULT 'ok'       -- ok/timeout/unavailable/skipped/error
);
CREATE INDEX idx_cp_session ON checkpoints (session_id);
CREATE INDEX idx_cp_parent  ON checkpoints (parent_checkpoint_id);
```

#### 双形式 API（sync + async）

```python
async def store(self, cp):     async with self._lock: self.store_sync(cp)
def store_sync(self, cp):     with self._connect() as conn: conn.execute("INSERT OR REPLACE ...", (...))
```

`asyncio.Lock` 只保护 async 入口防协程级重入；sync 入口直接走 sqlite（sqlite 自身 WAL + busy_timeout 保证并发安全）。两类调用方：

- async 钩子内（`_snapshot_subagent_start` / `_snapshot_session_complete`）：用 async `store`；
- 同步路径（web handler 列检查点、`on_session_delete` 删行、`TracebackRunner.run` 查 cp）：用 sync `get_sync` / `list_sync` / `delete_for_session`。

`get_latest_subagent_stop_rowid_sync`（`checkpoint_store.py:177-201`）查的是核心 `events` 表（同库）的 SUBAGENT stop 事件 rowid，给 checkpoint 补 `event_rowid` 反向链接。`except sqlite3.OperationalError` 回退处理 events 表尚未创建的场景。

### 4.4 `snapshot_engine.py` — 文件态快照引擎

`SnapshotEngine` 在独立快照仓 `<cwd>/.opsbot/snapshots/` 里为每个检查点打一次快照。

#### `snapshot()` 的两层保护（`snapshot_engine.py:344-395`）

1. `asyncio.Lock` 串行化（同一 project 的多个 subagent 排队打快照，git 不并发安全）；
2. `asyncio.wait_for` 30 秒硬超时——超时直接 `status=timeout` 跳过该检查点（无 `git_ref`，不可重放），不阻塞 workflow。

**为什么 30s 而不是 git 本身耗时的 0.1s？** 见 `snapshot_engine.py:49-58`：opencode 驱动的后台 poller 会对体量可达数 GB 的 opencode.db 做同步全索引扫描（`list_subagent_session_ids`），冷缓存时阻塞事件循环 1-2s。2s 旧值在「冷缓存 + 并发 subagent」场景会被吃光 → 检查点 timeout 落库。30s 留足余量。

#### `_do_snapshot()` 四步（`snapshot_engine.py:397-457`）

```python
# 1. 镜像 project_dir → working/（返回 (file_count, byte_size)）
file_count, byte_size = await self._sync_mirror()
# 2. git add -A --force（独立仓，须收全部镜像文件，不受项目 .gitignore 影响）
rc, _out, err = await _run_async(["git", "add", "-A", "--force"], cwd=self.snapshot_dir)
# 3. commit（结构化 trailer，--allow-empty 让无变更也产生独立 commit）
msg = self._build_commit_message(...)
rc, _out, err = await _run_async(["git", "commit", "-m", msg, "--allow-empty"], cwd=self.snapshot_dir)
# 4. 取 HEAD sha 作为 git_ref
rc, out, err = await _run_async(["git", "rev-parse", "HEAD"], cwd=self.snapshot_dir)
git_ref = out.strip()
```

`_sync_mirror`（`snapshot_engine.py:459-471`）先试 rsync（`--checksum` 而非 size+mtime，避免「同秒同尺寸不同内容」漏掉——subagent 改一个配置数字常见此情形），rsync 不可用降级纯 Python `_python_sync_mirror`（`snapshot_engine.py:173-240`，按 size+mtime_ns 增量、删源已删文件、清空目录）。

**关键约束**（`snapshot_engine.py:8-10` docstring）：`snapshot` 是 `async def`，调用方**必须** `await`——绝不能 `spawn`/`create_task` fire-and-forget，否则 worker 在后台快照期间继续写文件，捕获的就不是检查点时刻的文件态。这正是 §3.6 说「阻塞型钩子」的根因。

#### `restore()` — 重放时抽文件态（`snapshot_engine.py:497-543`）

```python
def restore(self, git_ref: str, dest_dir: Path) -> bool:
    # 空检查点回退：git ls-tree 前置回退，无 working/ 条目 = 空文件态
    # git archive <ref> working | tar -x --strip-components=1 -C dest_dir
```

用管道 `git archive | tar -x` 而不是 `git checkout`：**不污染快照仓的工作树**，直接抽出某 ref 的整树到任意 dest_dir。`--strip-components=1` 剥掉 `working/` 前缀。空检查点回退（`snapshot_engine.py:510-516`）是实时/重建共享 fix：首个 `subagent_start` 前若无 write，commit 不含 `working` tree 条目 → `git archive` 会失败，此情况视为「成功恢复空文件态」返回 True。

#### 预览（`list_files` / `read_file`）

`snapshot_engine.py:547-610`。用 `git ls-tree` / `git cat-file -s` / `git show`，不 restore 直接看。供前端在重放前浏览文件态决定从哪重放。

### 4.5 `l2_reader.py` — L2 读取 + 预制 session seed

#### `read_main_session_parts`（`l2_reader.py:36-82`）

读 opencode.db 主 session 的 part 流，按 `ORDER BY time_created ASC, id ASC` 排序。截断三选一（优先级 `exclude_message_id` > `exclude_part_id` > `up_to_part_id`）——见 §3.3。

#### `find_main_part_id_for_subagent` / `find_exclude_message_id`

`l2_reader.py:108-165`。前者定位「`state.output` 含子 session id 的 `task` part」（§3.4 硬约束），后者取该 part 所属的 `message_id` 供 message 级截断。`find_exclude_message_id` 失败时返回 `None`，调用方退化为 part 级截断。

#### `_init_fork_db_from_source`（`l2_reader.py:196-267`）

```python
src.backup(dest)   # SQLite backup API，含 WAL 数据，比 shutil.copyfile 更可靠
# 按 FK 依赖反序清空 session 范围表行（_FORK_DB_CLEANUP_TABLES）
# VACUUM 回收 free page（~2GB host db 清空后仍是 ~2GB 空洞）
# schema 自检：发现未知表时 warning（防 opencode schema 漂移新增 session 范围表残留）
```

清理表清单**硬编码**（`l2_reader.py:182-188`），不动态发现全删——未来 opencode 可能新增非 session 范围的系统表（缓存/开关/全局配置），全删会破坏功能。未知表改用 schema 自检 + warning（`l2_reader.py:248-263`），让开发者评估是否加进清理清单。

#### `seed_prebuilt_session`（`l2_reader.py:270-514`）

整个重放里最复杂的函数，干 6 件事：

1. **读写分离**：`source_env`（读 origin 的 db）与 `env`（写 fork 的 db）可不同；`env≠source_env` 且 fork db 不存在时调 `_init_fork_db_from_source`。
2. **读源 session 行**（动态适配 schema 漂移）：`PRAGMA table_info(session)` 拿列名列表，`SELECT <cols> FROM session WHERE id=?`。
3. **override 字段**：新 id、新 title、`parent_id=NULL`（重放 session 是顶层 workflow，不是源 session 的子 agent——否则 opencode 的 `task` 工具按 `parentID` 链算 subagent depth，重放 session 被算作 depth 1 → `subagent_depth` 默认 1 已满 → 无法再派 subagent）、fresh timestamps、`directory=path=fork_cwd`。
4. **读截断后的 part 流**（message 级优先）+ 读 kept parts 所属的 message 行（role/parentID 链）。**必须复制 message 行**——否则 parts 的 `message_id` 悬空，opencode `--session` resume 无法重建对话 → 子进程活但零输出悬死。
5. **路径重写**（`l2_reader.py:531-616`）：part/message 的 `data` JSON 树里，路径字段（`_PATH_FIELDS = filePath/path/filepath/directory/cwd/absPath/workdir`）做 `Path.resolve` + `is_relative_to(origin_cwd)` prefix 比对，命中替换为 `fork_cwd/rel`；**所有**其他字符串值做 path-prefix-aware 字符串替换（负向先行断言 `(?![a-zA-Z0-9_.-])`，消除 `/foo/bar` 匹配 `/foo/bar-baz` 的子串误伤）。**`origin_cwd` 是 source session 行的 `directory` 字段**——重建 session 的 `directory` 已被 `L2Transcriber` 改写为 `target_cwd`（见 §5.3），所以 seed 期 rewrite 只能把 `target_cwd/*` → `fork_cwd/*`；要消除原始 `origin_cwd/*`（cannbot 真实 cwd）路径，**必须由 `L2Transcriber` 在转写时把所有 `cannbot_origin_cwd/*` 字符串重写到 `target_cwd/*`**（见 §5.3 「全字段路径重写」）。若 transcriber 漏改字段，seed 期 rewrite 无法补救——worker 会在 L2 里看到原始 `cannbot_origin_cwd/*` 路径并直接读写原目录（**fs 并未隔离原目录**：origin cwd 是普通目录，worker 进程有权限访问），污染 ground truth。若不 rewrite，resume 后主 agent 沿用旧绝对路径读写 → 文件落到源目录而非 fork cwd → 快照引擎镜像 fork cwd 捕获不到 subagent 新产物 → 重放 session 的检查点不含新文件。
6. **写入 fork db**：session 行（`INSERT OR REPLACE`）、part 行（新 part id + 新 session_id，保留 message_id/time/data）、message 行（保留原 id 使 parts 引用有效，改 session_id 指向新 session）。写入失败 best-effort 回滚（删可能写入的新行），记 `_last_seed_error`。

### 4.6 `traceback_runner.py` — 透明重放编排

`TracebackRunner.run`（`traceback_runner.py:105-334`）7 步：

#### Step 1-2：查检查点 + 解析 origin session

```python
cp = self._plugin._cp_store_for_session(origin_session_id).get_sync(checkpoint_id)
if cp is None: return {"ok": False, "error": "checkpoint not found"}
if not cp.git_ref: return {"ok": False, "error": "no git_ref ... cannot replay"}
```

`git_ref` 为空（`status=timeout/unavailable`）的检查点不可重放——文件态没存住。解析 origin session（`traceback_runner.py:131-156`）两条路径：在 pool 里直接拿 `origin.config` `model_copy(deep=True)`；重启后不在 pool 从 `event_router.get_session()` 读持久化的 `config_json`，反序列化 `SessionConfig(**cfg_dict)`。**完整继承** origin 的 `loop` / `system_prompt` / `stages` / `criteria` / `model` / `permission_mode` / `allowed_tools` / `skills_path` / `agents_path` / `extra_dirs` / `hooks` / `handoff_prompt_path` / `continuation_prompt_path`——不硬编码，iteration>1 的 prompt 走 origin 的 Jinja 模板。约束：仅对已整体跑完的 session 重放（`traceback_runner.py:166-167`）。

#### Step 3：restore 文件态 → fork cwd + git 软沙箱

布局约定（`traceback_runner.py:172-182`）：

```
/var/folders/.../T/opsbot-tb-<id>/   ← fork_root（tempfile.mkdtemp）
└── cwd/                              ← fork_cwd（snapshot restore 目标 + worker --dir）
    ├── .opsbot/opencode/opencode.db  ← 本 fork 独立 opencode.db（seed 写入）
    ├── .opsbot/snapshots/            ← 本 fork 独立快照仓（fork 的 fork 用）
    ├── .opencode/                     ← 随快照恢复（含 opencode.json 权限配置）
    └── <工程文件>                     ← 检查点时刻的文件态
```

- **为什么 `fork_root/cwd/` 而不是直接 `fork_root/`？** 防 worker `ls ..` 看到 host tempdir 下兄弟 fork 的目录（跨重放泄漏）。`cwd/` 的父目录只有 `cwd/` 自己。
- **为什么 `resolve()`？** 见 `traceback_runner.py:185-190`：opencode 对 `--dir` 做 symlink 展开（macOS `/var`→`/private/var`）算 project worktree。若 fork_cwd 未 resolve，seeded `session.directory` 与 L2 路径不匹配 opencode 的 resolved project worktree → fork cwd 内的文件访问被判 `external_directory` → 弹权限询问 → headless 无用户 → 工具调用悬死。

Step 3b `_init_git_sandbox`（`traceback_runner.py:336-433`）在 fork cwd 内建可用的 git 工作树——`.git` 在快照 ignore 列表 → 快照恢复后 fork_cwd 无 git 历史 → worker 的 `git log`/`git diff`/`git commit` 失败。处理：

- 若 origin `.git` 存在且不超 `git_history_max_mb` 阈值：复制到 `fork_cwd/.git`，让 worker 继承完整 git 历史；
- 超阈值：跳过 copy，退化为 `git init`（无历史空树）；
- copy 后 `git remote rm` 逐一移除 remote，防 worker `git fetch` 误连外部；
- `git add -A && git commit --allow-empty` 把检查点时刻的文件态作为 baseline commit，让 `git status` 干净、历史连续。

#### Step 4：读 L2 + 定位 message 级截断点

`_resolve_origin_env`（`traceback_runner.py:78-103`）按 origin cwd 定位 origin 的 opencode.db 所在 env——支持嵌套重放。终态检查点（`is_final=True`）不排除任何 part，保留完整对话史 → 让重放 session 从原始 workflow 跑完后的最终文件态继续。

#### Step 5：seed 预制 session → fork 独立 opencode.db

见 §4.5。fork_env = `{"XDG_DATA_HOME": str(fork_cwd / ".opsbot")}` 注入 seed，让 worker 子进程的 opencode.db 指向 fork 独立路径。

#### Step 6：构造 SessionConfig + 权限配置

```python
base_config.initial_prompt = _TRACEBACK_CONTINUE_PROMPT   # "请继续你之前的工作。"
base_config.env_overrides = dict(fork_env)
# loop / max_iterations —— replay 直接继承 import 时固化的 LoopConfig，不做任何修改。
# import 时从 checkpoints 推导 max_iterations（见 §4.5），replay 无需再处理。
```

**权限配置**（`_write_permission_config`，`traceback_runner.py:466-502`）：seed 的 session 行从 origin 克隆，`project_id` 继承自 origin → 指向 origin 的 project worktree，非 fork cwd。opencode 据此把 fork cwd 内的文件访问判为 `external_directory` → 默认 action `ask` → 等用户确认 → headless 无用户 → **subagent 工具调用悬死**（工具 status 永远停在 running）。在 `<fork_cwd>/.opencode/opencode.json` 写 `{"permission": {"external_directory": "allow"}}` 绕过。**安全**：fork cwd 是隔离 tempdir（快照恢复目标 + git 软沙箱 + 快照隔离），worker 无法逃逸。这条配置修 main agent 继承的 project_id 不匹配；上方的 `resolve(fork_root)` 修 subagent 的 tool call 路径不匹配——两者结合消除悬死。

#### Step 7：起新 session

```python
session = await orch.start_session_with_resume(
    config,
    resume_sdk_session_id=resume_id,                       # seeded 的 ses_tb_xxx
    plugin_metadata={"agent_traceback": {"birth_traceback": birth}},
    start_action="traceback_session_started",
    inherit_overlays_from=origin_session_id,                # 继承 origin 的 subagent hints + 全局指令
    initial_current_iteration=int(cp.iteration) if cp.iteration else None,
)
self._plugin.register_fork_root(session.id, str(fork_root))  # 清理责任移交 on_session_delete
```

`inherit_overlays_from`：继承 origin session 的 session-scoped subagent hints + 全局 instructions——否则在 origin 重放前注册的 hint 到不了新 replay session，re-called subagent 的 system prompt 会丢用户的诊断。成功后 `register_fork_root` 把 `fork_session_id → fork_root` 登记到 in-memory map，由 `on_session_delete` 在 fork session 被显式删除时 rmtree。进程崩溃残留由启动期 `_reap_orphan_fork_roots` 按 TTL 后备清理。

### 4.7 `web_api.py` — FastAPI router

`web_api.py:37-107`，5 个端点：

| Method | Path | 用途 |
|---|---|---|
| GET | `/api/sessions/{sid}/checkpoints` | 列检查点（供前端拓扑图节点点选） |
| POST | `/api/sessions/{sid}/traceback` | 发起重放，body `{checkpoint_id, runtime?}` |
| GET | `/api/sessions/{sid}/checkpoints/{cpid}/files` | 列某检查点文件态直接子项（`?path=<sub>` 下钻） |
| GET | `/api/sessions/{sid}/checkpoints/{cpid}/file` | 读某检查点单文件内容（`?path=<rel>`） |
| POST | `/api/sessions/import-db` | 从 cannbot db 重建，body `{db_path, target_cwd}` |

**插件 reload 友好**（`web_api.py`）：路由在 app 启动时注册一次，闭包持有**按名解析插件的回调**（`PluginServices.plugin_by_name`）而非插件实例。每个请求经该回调解析当前已加载的 `agent_traceback` 实例——Settings 页 runtime `reload_plugin`（卸载旧、装入新）后，API 端点仍然落到**活跃**实例，不会指向已卸载的实例。

> **运行期 toggle 启用时的路由注册位置**（设计补充）：当 plugin 启动时为 disabled（不在 `orchestrator.plugins` 中）、用户在 Settings 里运行期 toggle 启用时，插件管理路由（`web/routes/plugins_routes.py` 的 toggle handler）经 `CoreAPI.plugin` 完成翻转与重载后，把 plugin router 交给 Console 装配器注入的注册回调补挂载。但 `app.include_router` 把 router **追加**到 `app.router.routes` 末尾——排在 SPA 挂载（`web/spa.py`）已注册的 catch-all `@app.get("/{full_path:path}")` 之后。Starlette 按注册顺序匹配，GET 请求会被 catch-all 截胡返回 `{"error":"not found"}` 404，永远到不了 plugin router（catch-all 只注册了 GET，故 POST 不受影响——这是诊断该问题的鉴别性特征）。因此运行期补注册时，必须把 plugin router 插到 catch-all **之前**的位置，而非 append 到末尾。

### 4.8 `session_reconstructor/` — db 重建子包

挂在现有 `agent_traceback` 插件下，与 traceback 共享 `SnapshotEngine` / `CheckpointStore` / `l2_reader`。

#### 4.8.1 `db_reader.py` — 时间线交错排序

设计文档 §2.2 实测样例 db：主 agent（`isSubagent=0`）只有 43 个 tool call（多为 `task` 派发 + 少量 `read`），**2143 个 tool call 在子 agent 里**——文件态变更几乎全发生在子 agent 内。若只重放主 agent 的工具，重建出来的工程目录几乎是空的。因此必须**跨主/子统一按 `startedAt` 排序重放**。

**框架方言归一化（读取时一次性完成）**：不同 framework 录制的 db 工具名不一致——opencode 用 `task`/`write`/`edit`/`bash`（小写），claude-code-proxy 用 `Agent`/`Write`/`Edit`/`Bash`（Claude 原生大写）。下游四处消费点（时间线 interleaving、L2 转写、文件重放、事件合成）都需要 opencode 方言，故在 `_read_tool_call_rows` 读取处经 `_TOOL_NAME_ALIASES` 统一映射（`Agent→task`、`Write→write`、`Edit→edit`、`Bash→bash`、`Read→read`、`MultiEdit→multi_edit`…），未知工具名原样透传。单一归一化点让所有下游零改动获得多框架支持；转写进 fork opencode.db 的 tool part 也自然用 opencode 原生名，resume 语义正确。工具**参数键**同样有方言差异：opencode 是 camelCase（`filePath`/`oldString`/`newString`），Claude Code 原生是 snake_case（`file_path`/`old_string`/`new_string`）——文件态重放对两种键形都接受（见 §4.8.3）。`NotebookEdit`/`Patch` 等其余 Claude 原生工具暂无归一化与重放支持，按未知工具透传并在重放时跳过。

**`role='system'` turn 过滤**：cpx 导出的 db 会把 per-request 注入的系统上下文（environment 描述、token 计数器）录成 `role='system'` 的 Turn——实测一个 session 可达 7.6 万行、全部共享导出时刻的 `createdAt`。它们不是对话内容，进时间线会污染 L2 转写与主 turn 计数，`_read_turn_rows` 读取时直接丢弃（opencode 方言本就不用 Turn 存 system prompt，双方言下丢弃均安全）。过滤条件是 `COALESCE(role,'') != 'system'`——只丢显式 system 行；`role IS NULL` 的行在 SQLite 三值逻辑下会被 `!=` 一并滤掉，故显式保留（转写时按 assistant 处理）。

交错算法（`db_reader.py:275-381`）：

1. 从 `InteractionBridge` 建 `bridge_map: {dispatchToolCallId → subagentSessionId}`。**`dispatchToolCallId` 的命名空间是 `ToolCall.id`（录制器 cuid），不是 `ToolCall.toolCallId`（框架原生 call id，如 `toolu_…`）**——查找时先按行 id、再按 `toolCallId` 回退查找，两个键都查；
2. 读全量 Turn + ToolCall，索引：`tcs_by_turn`、`sub_turns`；
3. 主 agent turn（`isSubagent=0`）按 `(turnIndex, createdAt)` 排序；
4. 遍历主 turn，每个 turn：emit turn entry → 对该 turn 的每个 tool_call（按 `startedAt`）：emit tool_call entry → 若是 `task` tool_call，**先解析子会话 id（bridge 优先、args 回退）再 emit**——task 条目的 `subagent_session_id` 必须携带解析结果：检查点链路只读 `entry.subagent_session_id`、args 抽取仅作回退，而 cpx 的 task args 不含 `subagent_session_id`（子会话 id 只在 bridge 里，键 = `ToolCall.id`），emit 后再解析会把空值钉进条目——检查点 `agent_id` 全空、`main_part_id` 查不到、拓扑节点不可重放；随后 `_interleave_subagent` 把子 agent 的全部 turn + tool_call 按 `turn.createdAt` 序交错 emit；
5. **嵌套派发递归 interleave**：子 agent 自己也可能派 task（实测 cpx db：Designer 派 4 个 Explore、Developer 派 2 个）。`_interleave_subagent` 遇到子 agent 的 `task` tool_call 时递归 interleave 其孙 agent 的 turns——否则孙 agent 的 turn 被整体丢弃，文件态重放缺失；
6. 末尾若有孤儿子 turn（无对应主 task call），按时间序追加 + warning。判重用**全局已 emit sid 集合**：同一集合实例贯穿主循环派发、孤儿补齐与 `_interleave_subagent` 递归层（兼作防环 `seen`），子会话从任一入口 emit 过一次即登记、后续各入口直接跳过。Turn 行序不保证祖先先于后代（时间线依赖末尾全局 ts 排序，行序不被信任）：循环先补齐后代、再补齐孤儿祖先时，祖先的递归层看不到已 emit 的后代，会再 emit 一次；主循环两个 task 派发指向同一子会话同理。重复 Turn/ToolCall 会进 `FileStateRebuilder` 被重复重放（write/edit/multi_edit 二次改写、bash 追加型命令二次执行），文件态失真；
7. 全局按 ts 稳定排序（Python list.sort 稳定，同 ts 保持插入顺序——让主 turn 在其 task call 前）。

缺失数据的回退：

- **`startedAt` 为 NULL**（旧 db）：`_iso_to_ms(tc.get("startedAt")) or _iso_to_ms(tc.get("completedAt"))` 回退到 `completedAt`，再退到 turn 的 `createdAt`。
- **`InteractionBridge` 缺失**：`_extract_subagent_sid_from_args`（`db_reader.py:384-394`）从 task 的 `argsJson.subagent_session_id` 抽——cannbot 录制时确实把子 session id 写进了 task 参数。
- **`InteractionBridge` 缺 `subagentTokens` 列**（旧版 recorder，`validate()` 只校验表不校验列）：`read_bridges` 先 `PRAGMA table_info` 探测，缺失时以 `0 AS subagentTokens` 回退读取，避免导入在 SELECT 上整体崩掉；其余列仍视为必备。
- **`Session.directory` 为空**（cpx 导出的常态，origin cwd 缺失）：orchestrator 从时间线上全部 write/edit/multi_edit 绝对目标路径的公共父目录推断项目根。公共父目录恰为某条目标路径的父目录（单文件或全部同目录）时视为退化——该目录更可能是项目根下的子目录而非根本身，过深的 origin 会让兄弟目录的绝对路径免于重写、或把文件拍平到 target 根，保守回退空串。推断失败（无绝对路径、公共目录退化到文件系统根）同样保持空——空 origin 下文件写入按原样绝对路径 best-effort、bash 命令与文本路径不做前缀重写（空串正则会退化为零宽匹配，把 target 路径插进每个非词边界位置，l2 转写与 bash 重放两侧同此守卫）。

`_iso_to_ms`（`db_reader.py:69-97`）**手算** UTC 而非用 `datetime.fromisoformat`——样例 db 全部以 `Z` 结尾（UTC），用 `calendar.timegm` 公式避免 `datetime` 时区设置差异，毫秒部分补齐到 3 位（`frac + "000")[:3]`）。失败返回 0（排序时退到最早），保证不抛。

#### 4.8.2 `l2_transcriber.py` — 转写对话态

**只转写主 agent**（`l2_transcriber.py:137`）——`TracebackRunner` 重放时只读主 session 的 part 流，子 agent 由主 agent 重新派发（`task` part 的 `state.output` 引用子 session id，重放时主 agent 自然 re-call）。

转写映射（`l2_transcriber.py:147-187`）：

| cannbot Turn | opencode message + part |
|---|---|
| `Turn(role=user, content=prompt)` | 1 message(role=user) + 1 part(type=text) |
| `Turn(role=assistant, content="<thinking>…</thinking>正文")` | 1 message(role=assistant) + 若干 part：每个 `<thinking>` 块一个 `reasoning` part + 去掉 thinking 的正文一个 `text` part（非空才发） |
| `Turn(role=assistant)` + 关联 ToolCall 列表 | 上述 reasoning/text part + 每个 ToolCall 一个 `tool` part |

`<thinking>` 块用正则 `_THINKING_RE = re.compile(r"<thinking>(?P<text>.*?)</thinking>", re.DOTALL | re.IGNORECASE)`（`l2_transcriber.py:34`）非贪婪匹配，跨行覆盖。正文 = 去掉所有 thinking 块后 `.strip()`，空正文不发 text part 避免噪声。

**task part 的 `state.output` 必须含子 session id**（§3.4 硬约束）显式回退（`l2_transcriber.py:327-336`）。同时把 `sub_sid → part_id` 记进 `task_part_ids` 映射，供检查点推导**预填** `main_part_id`——免得重放时再 `find_main_part_id_for_subagent` 查一次。

**全字段路径重写**：`L2Transcriber` 在转写时**必须把 `cannbot_origin_cwd/*` 字符串重写到 `target_cwd/*`**，覆盖**所有可能被 worker LLM 看到并复用的字段**——不止 `task` 工具的 `prompt`/`summary`/`output`，还包括：
- `text` / `reasoning` part 的正文（worker 推理时会引用这些路径）
- **所有** tool 的 `state.input`（如 `bash.command`、`edit.filePath`/`oldString`/`newString`、`write.path`、`read.filePath` 等）
- **所有** tool 的 `state.output`（如 `bash` 的 stdout、`read` 的文件内容——其中常含绝对路径）

实现：递归 walk `args_obj` / `output_str` / `body` / `ttext` 的 JSON 树（dict+list+str），对**每个字符串值**做 path-prefix-aware 替换（与 `l2_reader._rewrite_content_string` 同算法：负向先行断言 `(?![a-zA-Z0-9_.-])` 防子串误伤）。`_rewrite_prompt_paths` 是字符串级 helper；`_rewrite_obj_paths` 递归到字符串级。

**为什么 seed 期的 rewrite 不能补救**：`l2_reader.seed_prebuilt_session` 的 rewrite 以 source session 行的 `directory` 为 `origin_cwd`——重建 session 的 `directory` 已是 `target_cwd`（如 `/root/.../ops3_reconstruct`），所以 seed 只能把 `target_cwd/*` → `fork_cwd/*`。原始 `cannbot_origin_cwd/*`（如 `/root/.../ops1`）路径若残留，seed 无法识别。**fs 也未隔离**：origin cwd 是普通目录，worker 进程对其有读写权限——若 transcriber 漏改，worker 会直接读写 origin cwd 的真实文件（**污染 ground truth**）。因此 transcriber 是唯一能彻底消除 origin 路径的环节。

**opencode.db 初始化复用 `_init_fork_db_from_source`**（`l2_transcriber.py:119-131`）：不能裸建 `CREATE TABLE session (...)`——真实 opencode 打开 fork db 时不重跑 CREATE TABLE（依赖 `__drizzle_migrations` 版本号）；裸建表会让 opencode 认为是旧版 db 触发迁移，破坏 schema。**显式校验 source 存在**：`sqlite3.connect` 对不存在文件会建空 db，让 backup 看似成功但产出缺 schema——transcriber 早抛 `RuntimeError` 让 orchestrator 上报根因。**重复导入幂等（整库覆盖）**：schema 回放要求空 dest，而 `target_cwd` 由用户传入、可重复指向同一目录（`/api/sessions/import-db`），故 `transcribe()` 在 init 前先删旧 dest（含 `-wal`/`-shm`）——否则同 `target_cwd` 二次导入时首个 `CREATE TABLE` 撞 `table already exists` 失败，且失败清理会把**上次成功导入的有效 dest 一并误删**（events.db 注册项悬空指向不存在的 db）。先删后建等价恢复旧 `src.backup(dest)` 的覆盖语义：transcribe 本就写全量新 session，dest 最终只含本次导入的 session；kill -9 中断残骸同样被清掉。

写入用动态列发现（`_insert_row`，`l2_transcriber.py:494-507`）：`PRAGMA table_info(<table>)` 拿列名 → `INSERT OR REPLACE`，schema 漂移友好。

**占位 project 行**（`l2_transcriber.py:466-491`）：opencode 真实 schema 的 `session.project_id` 是 NOT NULL FK 指向 `project.id`。转写器 upsert 一行占位 project（`worktree=target_cwd`、`name="[reconstructed] <basename>"`），让 session 行能 INSERT。

**LoopConfig 从 checkpoints 推导**（`orchestrator._build_session_config`）：cannbot db 不存 opsbot `LoopConfig`，若仅依赖 `target_cwd/opsbot.yaml`，yaml 不存在时 `loop=None` → replay 时 `loop.py` 默认 `max_iterations=10`，而检查点 `cp.iteration` 可能远大于 10（如 cp_025 iter=25）→ 首轮即触发 `max_iterations` 停。**解法**：import 时 `checkpoints` 已在手上（每个 `cp.iteration` 是主 agent 第 N 次 task 派发，最大值即原始 session 实际总轮数），`_build_session_config` 取 `max(最大 cp.iteration, 10)` 作为 `max_iterations`，构造非 None 的 `LoopConfig`。yaml 配了 `loop.max_iterations` 时 yaml 优先（用户显式覆盖）。replay 直接继承固化的 `config_json`，不再做任何修改。

#### 4.8.3 `file_state_rebuilder.py` — 逐字节重放

**重放 = 逐字节回放 `argsJson`，非 LLM 重生成**（保真的根基）。`write` 的 `content`、`edit` 的 `oldString`/`newString` 都是 db 里原样录制的完整文本，重放时直接写盘——不经过任何 LLM。因此 `write`/`edit` 产出的文件与原始 session **逐字节一致**。

`_apply_write`（`file_state_rebuilder.py:247-256`）用 `write_bytes(content.encode("utf-8"))` 而非 `write_text`——不转码、不加尾换行，逐字节一致（含中文「hello\n世界」等）。`_apply_edit`（`:258-286`）精确匹配替换；oldString 不匹配 best-effort 跳过 + warning。

**参数键双方言 + `multi_edit`**：`_apply_write`/`_apply_edit` 对 opencode 的 camelCase（`filePath`/`oldString`/`newString`）与 Claude Code 原生 snake_case（`file_path`/`old_string`/`new_string`）都接受——cpx 方言录制的参数键是后者。`MultiEdit`（Claude Code 同文件多组替换，归一化为 `multi_edit`）由 `_apply_multi_edit` 按序逐条精确匹配应用，任一条不匹配 best-effort 跳过该条 + warning，其余条继续。每条 edit 按 Claude 语义读 `replace_all`：缺省/false 仅替换首处匹配，true 全替换——区别于 `_apply_edit` 恒全替换（后者对齐 opencode Edit 的 replaceAll 语义）。

`bash` 三类分类（`_classify_bash_command`，`file_state_rebuilder.py:508-535`）：

| 类别 | 示例 | 重放策略 |
|---|---|---|
| `git`（非 clone/push/pull/fetch） | `git init`/`add`/`commit`/`checkout <branch>` | **始终重放**（项目 `.git` 历史重建硬需求） |
| `file_op` | `mkdir`/`cp`/`rm`/`mv`/`touch`/`chmod`/`sed -i`/`cat >`/`tee` | `git_and_file_ops` 模式重放 |
| `build_test_network` | `./build.sh`/`make`/`pytest`/`git clone`/`curl` | `git_and_file_ops` 模式跳过 / `all` 模式 best-effort |

**网络类 git（clone/push/pull/fetch）优先判别**（`_GIT_NETWORK_RE`）——归入 `build_test_network` 而非 `git`，因为它们不可重放（依赖网络/外部仓）。**无论哪种模式，git 命令始终重放**——项目 `.git` 历史重建是硬需求。

**进程组终止的有界性**（`_kill_process_group` + `_drain_after_kill`）：重放命令后台派生的循环孙进程（如 `for i in $(seq…); do …; sleep 30; done &`）会持有 stdout/stderr 管道，主 shell 退出后 `communicate(timeout)` 仍要等到管道 EOF 才返回。超时杀组必须按**启动时已知的 pgid**（`process_group=0` ⇒ pgid == `proc.pid`）发 `os.killpg`——主进程此时常已退出成 zombie，macOS 对 zombie 调 `os.getpgid` 抛 ESRCH，被 best-effort suppress 吞掉后 killpg 根本不执行；杀组后的排空 `communicate()` 也要有界（`_POST_KILL_DRAIN_SECONDS`），超时则关闭管道断开 read 端 + 有界 `wait()` 收割——脱离进程组的孙进程（setsid/nohup 双 fork）持有管道时，无界等待会把整条重放永久吊死在单条命令上。

git commit hash 保真（`file_state_rebuilder.py:255-290`）：git commit 的 hash 由五元组决定（`tree + parent + author(name/email/timestamp) + committer(name/email/timestamp) + message`）。

- **tree**：由 `write`/`edit` 逐字节重放产出 → tree hash 一致 ✅
- **parent**：首条 hash 一致则链路一致 ✅
- **message**：从 `git commit -m "..."` 命令文本提取 ✅
- **timestamp**：`ToolCall.startedAt` 设 `GIT_AUTHOR_DATE`/`GIT_COMMITTER_DATE` ✅
- **author name/email**：从 `git commit` 的 `resultJson` 解析 `Author: Name <email>` 回填 ✅

五元组全对齐 → commit hash 逐位一致。**但**若 cannbot 把 `git commit` 的 resultJson 包装过（形如 `task_id: ses_xxx\n<task_result>...`），不含原生 git 输出——hash 保真度会降级（hash 不一致，但逻辑历史在），符合设计 §7.2 的降级路径。

`_snapshot_at_boundary`（`file_state_rebuilder.py:318-349`）每遇**主 agent** `task` tool call（`entry.role == "main"`，孙 agent 的嵌套派发条目跳过——理由同 §2.1：孙的派发 part 在子会话，主 session 无重放截断点，打出的检查点不可重放且污染 deriver 边界配对）：mirror → git add → git commit（结构化 trailer，与 `SnapshotEngine._build_commit_message` 同构）→ rev-parse HEAD。commit message 带结构化 trailer（`file_state_rebuilder.py:425-445`），与实时 `SnapshotEngine` 同构——`git log --grep "checkpoint: cp_rc_xxx"` 能反查。末尾 `_snapshot_session_complete` 追加 `trigger=session_complete` 终态检查点。

#### 4.8.4 `checkpoint_deriver.py` — 检查点推导

`CheckpointDeriver.derive`（`checkpoint_deriver.py:49-146`）输入：`timeline` / `file_states`（重放产出的 `CheckpointFileState` 列表，含 git_ref）/ `task_part_ids`（转写器产出的 `{subagent_sdk_id → main_task_part_id}`）/ `session_id` / `sdk_session_id` / `bridges`。

按 `file_states` 里 `trigger=subagent_start` 的 `checkpoint_index` 顺序遍历，每条构造一个 `Checkpoint` 行；末尾追加 `trigger=session_complete` 终态检查点。**关键回退**（`checkpoint_deriver.py:90-98`）：若 timeline 主 task 边界数 ≠ file_states 的 subagent_start 数（重放与转写偶尔对不齐），**以 file_states 为准**（重放才是真相）+ warning——避免 git_ref 错配。

字段取值见 `checkpoint_deriver.py:148-171`。`checkpoint_id` 用 `cp_rc_` 前缀（rc=reconstructed），与实时的 `cp_<sid>_<n>` 命名空间隔离——便于 Dashboard 标识「这是重建出的检查点」+ 溯源 `birth_traceback.reconstructed_from`。`main_part_id` 预填（`task_part_ids[subagent_sdk_id]`）——免得重放时再 `find_main_part_id_for_subagent` 查一次。

#### 4.8.5 `orchestrator.py` — 编排 + 注册

`ReconstructOrchestrator.import_session`（`orchestrator.py:56-228`）编排五步，外加合成历史事件。`import_session` 整体 try/except，失败返回 `{"ok": False, "error": "..."}` 不抛——不让重建崩掉调用方。

**临时 session_id → 真实 session.id 的回填**（`orchestrator.py:136-147, 185-191`）：`Checkpoint` 是 `@dataclass(frozen=True)`，无法原地改字段。检查点推导时 `session_id` 还未知（`pool.create` 后才知道）——先用 `rc_temp_<sdk[:12]>` 占位，`pool.create` 拿到真实 `session.id` 后用 `dataclasses.replace` 重建指向真实 session.id（`_replace_session_id`，`orchestrator.py:443-446`）。这避免了「先 pool.create 再 derive」的循环依赖。

**events.db 路径与插件读路径同源**（`orchestrator.py`）：

```python
events_db_path = self._orch.event_router.db_path_for(session.id)
cp_store = CheckpointStore(events_db_path)
```

`db_path_for` 恒解析守护进程库，检查点写入与插件读路径（`plugin.py` 的
`_cp_store_for_session`）同源。`opencode.db` / `snapshots/.git` / 项目 `.git`
仍在 `<target_cwd>/.opsbot/` 下——会话态与文件态跟 cwd 走，会话记录与检查点跟
守护进程库走。

**`_synthesize_historical_events`**（`orchestrator.py:363-538`）：重建 session 无实时 driver 事件流，但 front-end「概览」tab 从 events 表的 subagent/step_finish 事件派生数据。**以 InteractionBridge 为驱动源**（bridge 是派发关系的完备真相，覆盖主/子/孙全层级）：每条含 `subagentSessionId` 的 bridge → 一对 SUBAGENT 事件（start/stop），时间戳用 bridge 的 `dispatchTimestamp`/`responseTimestamp`（比派发 ToolCall 的 `startedAt`/`completedAt` 可靠——cpx db 的 ToolCall 完成时刻会被钉到 query 结束或塌缩成毫秒级），token 用 `subagentTokens`，类型/名称用 `subagentType`/`subagentName`（`agent_name` 进事件 data，前端拓扑 label 优先用它）；start 事件另携带 `parent_agent_id`（派发者 agent id，主 agent 派发为空串）供前端拓扑边层级化，父级真相取 timeline 派发条目的 `subagent_session_id`——其语义随 role 而异（子 agent 的 task 条目携带所属会话 id = 派发者；主 agent 的 task 条目携带被派发子会话 id，是检查点链路语义，不可当父级），bridge-only 场景（派发 ToolCall 行缺失）回退 `dispatchTurnId → Turn.subagentSessionId` 索引，两级均无时回退空串（挂到 main）——**不可用 `bridge.dispatchExecutionId` 判父级**（cpx 录制形态下恒指根 execution，孙 agent 的 bridge 也指向 main 的 execution）；timeline 里能定位到的派发 ToolCall 提供后备时间戳，start 的完整回退链为 `dispatchTimestamp → startedAt → completedAt → 子 agent 首条 timeline entry 的 ts`（子 turn `createdAt`——旧 db 的派发 ToolCall 两列时间戳可能均为 NULL，缺这级回退会让子代理节点整条从拓扑消失）。stop 的回退：bridge `status=completed` 或无 bridge（纯 timeline 驱动）时回退 `completedAt`、再退 start_ms（重建 session 是已跑完态，stop 缺失会让节点一直显示 running）；仅 bridge 声明未返回（`status=dispatched` 且无 `responseTimestamp`）→ 只发 start，节点由前端按 session 终态归档为 stopped。bridge 表整体缺失时回退纯 timeline 驱动（主 agent 每个 `task` tool call → 一对事件）。末尾一个 `STEP_FINISH` 事件，`num_turns` = 主 agent turn 数，`usage` 留 0。Phase 1.7 前合成的是 RESULT 事件；Phase 1.7 后改合成 `STEP_FINISH`，与 driver 产出对齐。

> **拓扑层级（孙 agent 边层级化）**：嵌套派发（子 agent 派孙 agent）时，孙 agent 的 start 事件携带 `parent_agent_id`（见上），前端 `useTopology` 以它决定 dispatch 边的 `fromId`（字段缺失或父节点未注册时回退 `main`，兼容实时 driver 旧事件），`AgentTopologyGraph` 按 dispatch 边构建层级树：深度分行、tidy-tree 叶子槽位定位（父节点居中于子节点之上），孙 agent 渲染为其派发者的直接子节点，dispatch 边自真实父节点底部引出。实时路径（opencode runtime）由 driver 轮询器递归发现全层级后代并在 start 事件携带 `parent_agent_id`（`SubagentTracker` 透传），孙 agent 的 stop 在其父会话 part 流里检测补发——见 `../../../docs/ARCHITECTURE.md` §4.7；claude_sdk 实时事件暂无 parent 信息，孙 agent 仍挂 main（前端回退兼容）。

---

## 5. 重建产物与实时 traceback 的同构性（核心承诺）

这是整个设计**最关键的原则**。重建完成后，产出的（a）git 快照仓、（b）opencode.db、（c）`checkpoints` 表行，与一条实时跑完的 session 产生的三者**结构完全一致**：

| 产物 | 路径 | 与实时同构的体现 |
|---|---|---|
| 快照仓 | `<target_cwd>/.opsbot/snapshots/.git` | `working/` 镜像 + 每 task 边界一 commit + 结构化 trailer message；`.git` 被 `DEFAULT_IGNORE_PATTERNS` 排除 |
| opencode.db | `<target_cwd>/.opsbot/opencode/opencode.db` | session/message/part 行 + 占位 project；task part 的 `state.output` 含子 session id（§3.4） |
| checkpoints 表 | 守护进程库（`event_router.db_path_for`） | `cp_rc_` 前缀 + git_ref + main_part_id 预填 + parent_checkpoint_id 链 |
| 项目 `.git` | `<target_cwd>/.git` | session 内 git 命令重放重建（含 hash 保真） |

因此 `TracebackRunner`、`web_api`、`list_checkpoint_files` 等下游链路**零改动复用**：

```
用户点检查点「重放」
  → TracebackRunner.run(origin_session_id, checkpoint_id)   ← 零改动
    1. cp = CheckpointStore.get(checkpoint_id) → git_ref + main_part_id + agent_id
    2. SnapshotEngine.restore(git_ref, fork_cwd) → 从重建快照仓抽文件态
    3. _resolve_origin_env(target_cwd) → 命中 <target_cwd>/.opsbot/opencode/opencode.db（重建 L2）
    4. find_main_part_id_for_subagent / find_exclude_message_id → 在转写 part 流里定位截断点
    5. seed_prebuilt_session → 截断 part 流写回 fork 独立 opencode.db
    6. start_session_with_resume → 起 fork session，首轮 prompt = "请继续你之前的工作。"
```

`_resolve_origin_env`（`traceback_runner.py:78-103`）按 origin cwd 定位 opencode.db：若 `<origin_cwd>/.opsbot/opencode/opencode.db` 存在 → 视为重建/重放 session → 返回 `{"XDG_DATA_HOME":"<origin_cwd>/.opsbot"}`。重建产出的 opencode.db 恰在此路径——**天然被当作「origin」**。

---

## 6. 关键设计决策小结（供改进者参考）

| 决策 | 理由 | 出处 |
|---|---|---|
| `on_subagent_start` 是 `await` 而非 fire-and-forget | 文件态须冻结在 subagent 真正动手写之前；管道背压暂停 worker | `plugin.py:120` |
| 检查点表物理在 events.db、逻辑归插件 | `event_rowid` 双向可查、备份/迁移一致；核心 EventStore 不含 checkpoint 概念 | `checkpoint_store.py:73-91` |
| 快照仓是独立 git repo，不是项目 `.git` | 不污染项目 git 历史；`--allow-empty` 让无变更也产生独立 commit 便于 `git log --grep` 反查 | `snapshot_engine.py:284-395` |
| rsync 用 `--checksum` 而非 size+mtime | 避免同秒同尺寸不同内容漏掉（subagent 改一个数字常见） | `snapshot_engine.py:243-281` |
| 30s 硬超时而非 2s | opencode poller 冷缓存时阻塞事件循环 1-2s，2s 在并发 subagent 场景会被吃光 | `snapshot_engine.py:49-58` |
| restore 用 `git archive \| tar -x` 而非 `git checkout` | 不污染快照仓工作树；直接抽整树到任意 dest_dir | `snapshot_engine.py:497-543` |
| 重放用 message 级截断而非 part 级 | 消除「我将调用 X」的孤儿文本，worker 不会误以为自己已派过 X | `l2_reader.py:36-82` |
| 首轮 prompt 是通用「请继续你之前的工作」 | 透明重放：不注入重放语义，主 agent 自然续跑 | `traceback_runner.py:64` |
| `parent_id=NULL` on seeded session | 避免被 opencode task 工具算作 depth 1 → subagent_depth 已满 → 无法再派 subagent | `l2_reader.py:375-379` |
| fork_cwd = `fork_root/cwd/` 而非 `fork_root/` | 防 worker `ls ..` 看到 host tempdir 下兄弟 fork 目录 | `traceback_runner.py:172-182` |
| `resolve(fork_root)` | macOS `/var→/private/var` symlink 展开，匹配 opencode 的 resolved project worktree，避免工具调用悬死 | `traceback_runner.py:185-190` |
| 写 `.opencode/opencode.json` 设 `external_directory: allow` | seeded session 继承 origin 的 project_id → fork cwd 内访问被判 external → headless 无用户 → 悬死 | `traceback_runner.py:466-502` |
| fork opencode.db 从 origin 整库 backup + 清行 | 保留 schema + migrations，opencode 打开 fork db 不重跑 CREATE TABLE；仅含本 fork seeded session 实现隔离 | `l2_reader.py:196-267` |
| 路径重写：结构化 JSON walk + path-prefix-aware 字符串替换 | 路径字段做 resolve+prefix-match（防子串误伤）；所有字符串值覆盖（不限字段名）；非 origin_cwd 绝对路径显式不重写 | `l2_reader.py:531-616` |
| `.opsbot` 在快照 ignore 列表 | 每个 fork 的 `.opsbot/` 全新 → opencode.db 独立 + 嵌套重放干净 | `snapshot_engine.py:34-47` |
| `_init_git_sandbox` 复制 origin `.git` 后 `git remote rm` | 让 worker 继承完整 git 历史（log/diff 可用），但不带 remote（防 fetch 误连外部） | `traceback_runner.py:336-433` |
| `_reap_orphan_fork_roots` 启动期按 TTL 扫 tempdir | 清理进程崩溃/重启后 in-memory `_fork_roots` map 丢失的 fork_root 残留（每个 ~2GB） | `plugin.py:78-116` |
| 双形式 `store` / `store_sync` | async 钩子内 + 同步 web/删除路径都要用同一张表 | `checkpoint_store.py:124-175` |
| `_active_plugin` per-request 解析 | 插件 runtime reload 后 API 端点仍落到活跃实例 | `web_api.py:22-35` |
| 重放 = 逐字节回放 `argsJson`，非 LLM 重生成 | `write`/`edit` 内容与原始逐位一致——保真的根基 | `file_state_rebuilder.py:182-221` |
| 跨主/子统一按 `startedAt` 排序重放 | 文件态变更几乎全在子 agent 内，只重放主 agent 会丢文件 | `db_reader.py:248-317` |
| 两个 git 仓严格分离 | 项目 `.git`（真实历史）与快照仓（检查点快照）互不干扰；快照不捕获 `.git` | `file_state_rebuilder.py:7-15` |
| git 命令始终重放（不受 `bash_replay_mode` 影响） | 项目 `.git` 历史重建是硬需求 | `file_state_rebuilder.py:223-253` |
| `cp_rc_` 前缀命名空间隔离 | 与实时 `cp_<sid>_<n>` 区分，便于 Dashboard 标识 + 溯源 | `checkpoint_deriver.py:108-110` |
| `main_part_id` 预填 | 免得重放时再 `find_main_part_id_for_subagent` 查，加速冷启动 | `checkpoint_deriver.py:117` |
| 占位 project 行 upsert | `session.project_id` NOT NULL FK 须指向真实 project 行 | `l2_transcriber.py:466-491` |
| 检查点写守护进程库（`db_path_for`） | 与插件读路径同源，列表/回放才查得到 | `orchestrator.py` |
| `env_overrides={XDG_DATA_HOME: target_cwd/.opsbot}` | 让 fork worker 子进程的 opencode.db 指向重建路径 | `orchestrator.py:360` |
| 临时 session_id 占位 + `dataclasses.replace` 回填 | 避免「先 pool.create 再 derive」的循环依赖 | `orchestrator.py:136-147, 443-446` |
| timeline 边界数 ≠ file_states 数时以 file_states 为准 | 重放才是真相，避免 git_ref 错配 | `checkpoint_deriver.py:90-98` |
| `SnapshotEngine.restore` 空检查点回退 | 首个检查点常为空（subagent 前 no write），实时/重建共享 fix | `snapshot_engine.py:510-516` |
| 重建产物与实时快照同构 | `TracebackRunner`/`web_api`/`CheckpointStore` 零改动复用 | `session_reconstructor/__init__.py` |

---

## 7. 配置项参考

`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/config.yaml`：

```yaml
name: agent_traceback
version: 0.1.0
enabled: false                 # 设为 true 才会触发 on_subagent_start 快照
snapshot_dir: .opsbot/snapshots # 相对 project_dir
ignore: []                     # 留空则用 DEFAULT_IGNORE_PATTERNS
max_checkpoints: 200           # 单 session 检查点上限，超出滚降最早的
snapshot_timeout_seconds: 30.0  # 单次快照硬超时（须远大于 git 本身耗时）

# ── 从 cannbot session db 文件重建会话 ──
reconstruct:
  enabled: true
  bash_timeout_seconds: 30      # 重放 bash 单命令超时
  bash_replay_mode: all         # all | git_and_file_ops
  git_hash_fidelity: true       # 回填 GIT_AUTHOR_DATE/author 保 hash
  max_replay_tool_calls: 5000   # 重放上限（防巨型 db 卡死）
  source_opencode_db: ""        # 空 → 用宿主默认 ~/.local/share/opencode/opencode.db
```

`bash_replay_mode` 两种模式：

- `all`（默认，当前阶段推荐）：全量重放——不跳过任何 bash/cd/其它命令（含 build/test/网络/python3 脚本包裹），失败/超时只记日志不中断。**git 命令无论何种模式始终重放**（`git clone/push/pull/fetch` 除外，属网络类）。
- `git_and_file_ops`：git + 文件操作类执行，build/test/网络跳过。

> **若宿主没有 `~/.local/share/opencode/opencode.db`**：把任意一台跑过 opencode 的机器的 opencode.db 拷过来，填到 `reconstruct.source_opencode_db`（只需 schema + migrations，体积无关——backup 后会清空 session 表）。

---

## 8. 测试套件

测试在 `packages/plugins/agent-traceback/tests/`，13 个文件覆盖全部模块：

```bash
# 全套（含重建子包）
.venv/bin/python -m pytest packages/plugins/agent-traceback/tests/ -q
# 期望：145 passed, 2 skipped（skip 的是需真实样例 db 的 test_sample_db_*）

# 只跑重建相关
.venv/bin/python -m pytest packages/plugins/agent-traceback/tests/test_db_reader.py \
  packages/plugins/agent-traceback/tests/test_l2_transcriber.py \
  packages/plugins/agent-traceback/tests/test_file_state_rebuilder.py \
  packages/plugins/agent-traceback/tests/test_checkpoint_deriver.py \
  packages/plugins/agent-traceback/tests/test_reconstruct_orchestrator.py \
  packages/plugins/agent-traceback/tests/test_web_api_import.py \

# Lint + Typecheck（提交前必跑）
.venv/bin/ruff check packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/ \
  packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/session_reconstructor/
.venv/bin/mypy packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/ \
  packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/session_reconstructor/
```


改任何模块后**必须**：

1. 跑该模块对应 test 与 `test_reconstruct_e2e.py`（验证同构性未破坏）；
2. 跑 `test_traceback_runner.py`（验证重放链路未受影响）；
3. ruff + mypy 通过。

---

## 9. 常见改进点与已知风险

改进前请先读 风险与改进清单（原链接目标未入库）。下面是几个高频改动方向的提示：

### 9.1 bash 命令分类的盲区

`_classify_bash_command`（`file_state_rebuilder.py:508-535`）的设计假设是「**命令首 token 足以判断命令性质**」——这对裸 git 命令（`git add`）、简单文件操作（`mkdir`/`cp`）成立，但失败于：

1. **脚本包裹**：`python3 script.py` 把真实操作（git/文件产出）藏在脚本内部，首 token `python3` 掩断了判断。例如 `python3 git-checkpoint.py`（内部执行 `git checkout -b`/`git add`/`git commit`/`git tag`）会被跳过 → 项目 `.git` 缺所有 checkpoint commit + tag。
2. **`cd` 前缀遮蔽**：`cd X && <cmd>` 是 shell 惯用法，但首 token `cd` 不在任何分类集合 → 整条命令跳过，即使 `&&` 后是 `git add .` 或 `mkdir build/`。
3. **分类粒度不足**：`build_test_network` 是个大杂烩——把 `python3`/`make`/`pytest`/`curl`/`bash`/`cd` 统一全扫进去，但 `python3 git-checkpoint.py`（产出 git 历史）与 `python3 -m pytest`（产出测试结果）性质完全不同。

改进方向（详见深逻辑解析 §12）：

- **方案 A（推荐短期）**：在 `config.yaml` 的 `reconstruct` 段加 `script_overrides`（正则 `pattern` + `classify_as`），分类器在首 token 匹配前先按 override 列表匹配。
- **方案 B（中短期）**：`cd` 剥离 + 复合命令按 `&&`/`;`/`|` 切分逐段分类。
- **方案 C（中期补充）**：重建后做「缺口检测」——把 db 里所有 `read`/`edit` tool call 引用过的文件路径与重建目录实际文件对比，缺失的标记 `partial` + 日志列出。
- **方案 D（长期根治）**：对每条 `build_test_network` 类 bash 命令做重放前后目录差分，新增/修改的文件即该命令的「产出」。

### 9.2 连接管理与并发

- `checkpoint_store._connect`（`checkpoint_store.py:78-91`）每次操作开短连接、`PRAGMA busy_timeout=5000`、`journal_mode=WAL` 持久在 db header 故无需每次重设。busy_timeout 让插件写 checkpoints 时若与 EventStore 写 events 抢锁，会等最多 5 秒而不是立刻 `database is locked`。
- 改进风险：若把连接改成长连接复用，需注意 sqlite 连接不能跨线程/协程共享，且 WAL 模式下长写事务会阻塞 reader。

### 9.3 fork db 清理表清单

`_FORK_DB_CLEANUP_TABLES`（`l2_reader.py:182-188`）是**硬编码**。opencode schema 漂移新增 session 范围表时，新表的 stale 数据不会被清理——靠 schema 自检 warning（`l2_reader.py:248-263`）提醒开发者评估是否加进清理清单。改 opencode 版本后请检查此清单是否需更新。

### 9.4 30s 超时与孤儿 mirror 线程

`_sync_mirror` 经 `loop.run_in_executor` 把 rsync/Python 镜像丢到线程池——`asyncio.wait_for` 超时后**无法中断已运行的线程**（线程不可强制取消），rsync/`os.walk` 仍会跑完，只是结果被丢弃。30s 已留足余量，但极端场景（巨型 repo + 慢磁盘）仍可能残留孤儿线程。改进方向：把镜像改用 `asyncio.create_subprocess_exec`（kill 子进程能真正中断）或加全局 rate limit。

### 9.5 `created_at` 时区

`checkpoint_store` 的 `created_at` 用 `datetime.now().isoformat()`（`plugin.py:279`，naive 本地时区），而 `checkpoint_deriver` 用 `datetime.now(UTC).isoformat()`（`checkpoint_deriver.py:167`，UTC aware）。`list` 的 `ORDER BY created_at`（`checkpoint_store.py:172`）在跨时区混合时会错位。改进：统一用 UTC aware。

### 9.6 `permission` 覆盖与 project_id

`_write_permission_config` 设 `external_directory: allow` 是**全局覆盖**——若用户在项目级 `opencode.json` 已有 `permission` 段，会被整段替换（`traceback_runner.py:489-496` 实际是 dict update 仅覆盖 `permission` 键，但子键会整体替换）。更稳妥的改法是 merge 子键。

### 9.7 路径重写的子串假阳性

`_rewrite_content_string`（`l2_reader.py:603-616`）用 `re.escape(origin_cwd) + r"(?![a-zA-Z0-9_.-])"` 负向先行断言消除 `/foo/bar` 匹配 `/foo/bar-baz` 的子串误伤。但若 origin_cwd 本身是另一路径的前缀（如 `/foo/bar` 是 `/foo/barge` 的前缀，但 `barge` 含 `g` 不在断言排除集），会误命中。当前集合 `[a-zA-Z0-9_.-]` 已覆盖常见文件名字符，但极端命名（如含中文/unicode 的目录名）需评估。

---

## 10. 进一步阅读

- 深逻辑解析：实时检查点重放（原链接目标未入库）：本文的深度展开版，含端到端时序图与每条调用链的源码索引
- 深逻辑解析：从 db 文件重建会话（原链接目标未入库）：重建链路的深度展开版，含脚本包裹命令盲区（§12）与全量重放替代路径（§12.8）
- 检查点设计（原链接目标未入库）：需求与设计原理
- 重放隔离修复日志（原链接目标未入库）：fork 隔离演进历史
- 重放完整性分析（原链接目标未入库）：截断/路径/权限的完整论证
- 风险与改进清单（原链接目标未入库）：R1–R8 待审风险 + I1–I19 新增问题 + F1–F8 重构建议（含状态跟踪表，**改代码前必读**）
- 从 db 重建设计（原链接目标未入库）：重建功能的设计与推进计划
- 从 db 重建开发日志（原链接目标未入库）：实现过程中的踩坑记录

代码主入口：

- 钩子与引擎/存储缓存：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/plugin.py`
- 快照引擎：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/snapshot_engine.py`
- 重放编排：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/traceback_runner.py`
- L2 读取与 seed：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/l2_reader.py`
- 检查点表：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/checkpoint_store.py`
- 重建编排：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/session_reconstructor/orchestrator.py`
- db 读取与时间线：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/session_reconstructor/db_reader.py`
- 对话态转写：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/session_reconstructor/l2_transcriber.py`
- 文件态重建：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/session_reconstructor/file_state_rebuilder.py`
- 检查点推导：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/session_reconstructor/checkpoint_deriver.py`
- Web API：`packages/plugins/agent-traceback/src/workflow_controller/plugins/agent_traceback/web_api.py`
