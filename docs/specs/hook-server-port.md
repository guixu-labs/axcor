# Hook Server 端口 CLI 设计

## 状态

- 设计阶段：待评审
- 作用范围：CLI 启动、Web 与 headless 控制器

## 背景

在 `docker run --network=host` 场景下，多个 Workflow Controller 实例共享宿主机网络命名空间。固定 Hook Server 端口容易冲突，且 `workflow-controller web --port` 语义是 Web UI 端口，不是 Hook Server 端口。

Hook Server 端口不再属于 YAML 配置。`workflow-controller-config.yaml` 中的 `hook_server.port` 不再支持；端口由启动命令的 `--hook-port` 传入，未指定时使用默认值 `18765`。

同时，`workflow-controller codex-hook` 是内部转发命令，不是 Hook Server 本体。帮助文案必须明确区分二者，避免用户误以为它也接受端口参数。

## 目标

1. Hook Server 端口由启动参数 `--hook-port <port>` 指定。
2. 未指定 `--hook-port` 时使用默认端口 `18765`。
3. YAML 不再提供 `hook_server.port` 配置入口。
4. 指定端口被占用时显式启动失败，不自动换端口。
5. `--dev` 模式下参数能穿透 uvicorn reload 的子进程边界。
6. 帮助文案与文档明确区分：
   - `--port`：Web UI 端口
   - `--hook-port`：Hook Server 端口
   - `codex-hook`：内部转发命令，不是 Hook Server
7. controller 实例维护 `effective_hook_port`，作为 Hook Server、Claude hooks 与 Codex hooks 的唯一端口事实源。

## 非目标

1. 不自动选择空闲端口。
2. 不做端口步长扫描。
3. 不把端口写回 `workflow-controller-config.yaml`。
4. 不调整 Web UI 端口 `--port` 的语义。
5. 不改变 `codex-hook` 的转发协议，只补齐端口一致性校验。

## 设计要点

### 端口来源

1. `--hook-port` 是唯一的显式端口入口。
2. 端口来源只有两类取值：
   - 指定 `--hook-port` 时使用该值
   - 未指定时使用默认 `18765`
3. `HookServerConfig` 不包含 `port` 字段；旧 YAML 中的 `hook_server.port` 会在加载时被拒绝。
4. `ConfigAPI.update_config()` 不接受 `hook_server.port`，设置页也不展示或编辑该字段。
5. 端口变更只能通过重启 controller 并传入新的 `--hook-port` 完成。

### `boot()` 与 `effective_hook_port`

1. controller 实例在启动时固化 `effective_hook_port`。
2. 它来自 CLI 参数或默认值，不落盘、不写回 YAML。
3. HookServer 绑定、Claude `.claude/settings.json` 与 DriverHooks 中的通用
   Hook Server URL / OpenCode file-change URL 全部从这里取值。
4. `boot()` 暴露 `hook_port` 显式入参；headless 与直接调用方传入该参数，
   不再通过 `WORKFLOW_CONTROLLER_HOOK_PORT` 传递。
5. `WORKFLOW_CONTROLLER_HOOK_PORT` 仅作为 Web CLI 与其 Web 组合根 / uvicorn
   reload 子进程之间的边界适配，尤其是 `--dev` 模式。

### Claude CLI

1. Claude CLI session 激活前刷新 `.claude/settings.json`。
2. 刷新时使用 `effective_hook_port`。
3. Hook Server 不可用时拒绝激活，避免旧 session 访问漂移端口。
4. 刷新按事件合并，不整段覆盖 `hooks`：只替换 controller 自己的条目（`type` 为 `http` 且 URL
   路径为 `/hooks/<event>` 端点之一的 matcher 条目，含端口过期的旧条目），保留项目原有的
   其他 hook 条目与事件；`hooks` 以外的顶层键照旧保留。

### Codex CLI

1. Codex hook 通过 `WORKFLOW_CONTROLLER_HOOK_SERVER_URL` 获得转发地址。
2. 该环境变量由 `effective_hook_port` 生成。
3. Codex session 激活前由 driver 的 `refresh_hooks` 校验：
   - Hook Server 可用；
   - managed Codex hooks 配置仍然有效。
4. `DriverHooks.hook_server_base_url` 是通用的 runtime Hook Server 地址；
   Codex 读取它生成 `WORKFLOW_CONTROLLER_HOOK_SERVER_URL`。
5. `DriverHooks.file_change_hook_url` 仅保留 OpenCode 文件归因语义；
   OpenCode 继续读取它注入 file-provenance 插件。
6. 构造 Codex 子进程环境前，`_codex_hook_invocation` 校验该 URL 非空且为带
   host/port 的 `http` URL，再写入 `WORKFLOW_CONTROLLER_HOOK_SERVER_URL`。
7. 校验失败时拒绝激活或继续运行，而不是让 hook 请求打到错误端口后超时。

## 施工计划

1. CLI 增加或统一 `--hook-port` 参数。
2. headless 入口把端口显式传入 `HeadlessRunner` 与 `boot()`。
3. Web CLI 在进程边界内使用 `WORKFLOW_CONTROLLER_HOOK_PORT`，供 Web 组合根与
   `--dev` 下的 uvicorn reload 子进程读取。
4. 从配置模型、ConfigAPI 与设置页移除 `hook_server.port`。
5. HookServer 绑定与全部 hook URL 消费方统一使用 `effective_hook_port`。
6. 补齐 Claude 与 Codex 激活前的一致性校验；Codex URL 一致性由单一构造源
   与测试守护，运行前校验位于 `_codex_hook_invocation`。
7. 更新 `--help`、README 和 Web UI 文档。

## 验收标准

1. `workflow-controller web --hook-port 18775` 启动后，Hook Server 监听 `18775`。
2. 未指定 `--hook-port` 时，Hook Server 监听默认端口 `18765`。
3. YAML 中存在 `hook_server.port` 时启动失败，配置接口也不接受该字段。
4. 指定端口被占用时，启动失败并输出可诊断错误。
5. `--dev` 模式下，uvicorn reload 子进程仍能读取到 `--hook-port`。
6. 在 UI 中保存全局配置时，不生成或修改 `hook_server.port`。
7. `workflow-controller web --help` 能明确区分 Web UI 端口与 Hook Server 端口。
8. `workflow-controller codex-hook --help` 不与 Hook Server 端口参数混淆。
9. Claude CLI 激活前，`.claude/settings.json` 中 controller 写入的 hook URL 端口与 Hook Server 实际监听端口一致；项目原有的非 controller hook 条目保持不变。
10. Codex CLI 子进程的 `WORKFLOW_CONTROLLER_HOOK_SERVER_URL` 来自
    `DriverHooks.hook_server_base_url`；测试断言该 URL 与 Hook Server 实际监听端口一致。
11. 相关单元测试与集成测试全部通过。
