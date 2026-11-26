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

### `effective_hook_port`

1. controller 实例在启动时固化 `effective_hook_port`。
2. 它来自 CLI 参数或默认值，不落盘、不写回 YAML。
3. HookServer 绑定、Claude `.claude/settings.json`、Codex `WORKFLOW_CONTROLLER_HOOK_SERVER_URL` 与 DriverHooks 的 file-change hook URL 全部从这里取值。

### Claude CLI

1. Claude CLI session 激活前刷新 `.claude/settings.json`。
2. 刷新时使用 `effective_hook_port`。
3. Hook Server 不可用时拒绝激活，避免旧 session 访问漂移端口。

### Codex CLI

1. Codex hook 通过 `WORKFLOW_CONTROLLER_HOOK_SERVER_URL` 获得转发地址。
2. 该环境变量由 `effective_hook_port` 生成。
3. Codex session 激活前校验：
   - Hook Server 可用；
   - `WORKFLOW_CONTROLLER_HOOK_SERVER_URL` 的端口等于 `effective_hook_port`；
   - managed Codex hooks 配置仍然有效。
4. 校验失败时拒绝激活，而不是让 hook 请求打到错误端口后超时。

## 施工计划

1. CLI 增加或统一 `--hook-port` 参数，写入内部环境变量 `WORKFLOW_CONTROLLER_HOOK_PORT`。
2. Web 与 headless 组合根读取该环境变量并传入 controller。
3. 从配置模型、ConfigAPI 与设置页移除 `hook_server.port`。
4. HookServer 绑定与全部 hook URL 消费方统一使用 `effective_hook_port`。
5. 补齐 Claude 与 Codex 激活前的一致性校验。
6. 更新 `--help`、README 和 Web UI 文档。

## 验收标准

1. `workflow-controller web --hook-port 18775` 启动后，Hook Server 监听 `18775`。
2. 未指定 `--hook-port` 时，Hook Server 监听默认端口 `18765`。
3. YAML 中存在 `hook_server.port` 时启动失败，配置接口也不接受该字段。
4. 指定端口被占用时，启动失败并输出可诊断错误。
5. `--dev` 模式下，uvicorn reload 子进程仍能读取到 `--hook-port`。
6. 在 UI 中保存全局配置时，不生成或修改 `hook_server.port`。
7. `workflow-controller web --help` 能明确区分 Web UI 端口与 Hook Server 端口。
8. `workflow-controller codex-hook --help` 不与 Hook Server 端口参数混淆。
9. Claude CLI 激活前，`.claude/settings.json` 中的 hook URL 端口与 Hook Server 实际监听端口一致。
10. Codex CLI 激活前，`WORKFLOW_CONTROLLER_HOOK_SERVER_URL` 的端口与 Hook Server 实际监听端口一致。
11. 相关单元测试与集成测试全部通过。
