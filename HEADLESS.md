# Workflow Controller Headless Mode

无交互、全配置驱动的 agent 任务流水线。  
适用于 CI/CD、脚本自动化、批量任务场景。

## 配置文件一览

| 文件 | 用途 | 默认路径 |
|------|------|----------|
| `workflow-controller.yaml` | 项目配置：project_dir、reviewer、hook_server、runtime | 项目根目录 |
| `settings.conf` | 全局认证：API keys、模型、base URL | `~/.config/workflow-controller/settings.conf` |
| `auth_state.json` | 认证状态持久化：provider 轮转、reset 时间 | `~/.config/workflow-controller/auth_state.json` |

本目录 (`conf/`) 提供完整 `.example` 模板，复制后修改即可使用。

## 快速开始

### 1. 准备配置文件

```bash
# 从模板创建
cp conf/workflow-controller.yaml.example workflow-controller.yaml
cp conf/settings.conf.example ~/.config/workflow-controller/settings.conf

# 编辑 workflow-controller.yaml — 填入目标工程路径
vim workflow-controller.yaml
  # project_dir: /path/to/your/project   # 目标工程（worker cwd）

# 编辑 settings.conf — 填入 API key
vim ~/.config/workflow-controller/settings.conf
  # api_keys:
  #   - sk-ant-api03-your-key-here
```

### 2. 执行

```bash
# 最简
workflow-controller run --headless

# 指定配置文件
workflow-controller run --headless -c /path/to/workflow-controller.yaml

# 禁用 reviewer
workflow-controller run --headless --no-reviewer

# GLM / 第三方端点（走 claude_cli runtime + env）
ANTHROPIC_BASE_URL=https://open.bigmodel.cn/api/anthropic \
ANTHROPIC_AUTH_TOKEN=<glm token> \
ANTHROPIC_MODEL=glm-5.2 \
workflow-controller run --headless -c workflow-controller.yaml
```

### 3. 输出

所有输出为 JSON Lines (一行一条 JSON)，通过 stdout 输出，方便脚本解析：

```json
{"event": "auth", "mode": "api", "api_keys": 2}
{"event": "info", "msg": "Starting session: /path/to/project"}
{"event": "session_started", "id": "abc123-..."}
{"event": "pipeline", "progress": 0.0, "stages": []}
{"event": "session_update", "sessions": [{"id": "abc123", "state": "running", "iter": 3, "progress": 0, "elapsed": 120}]}
{"event": "assistant_message", "ts": "2026-06-24T...", "session": "abc123", "data": {"text": "..."}}
...
{"event": "usage", "turns": 42, "tokens_in": 123456, "tokens_out": 65432, "cost": 1.23}
{"event": "done", "msg": "All done"}
```

## CLI 完整参数

```
Usage: workflow-controller run [OPTIONS]

Options:
  -c, --config PATH         Path to workflow-controller.yaml (default: workflow-controller.yaml)
  --settings-dir PATH       Dir with settings.conf + auth_state.json
                            (default: ~/.config/workflow-controller/)
  -p, --port INTEGER        Hook server port (default: 18765)
  --headless                (default) JSON lines to stdout (CI/scripts)
  --no-reviewer             Disable reviewer
  --help                    Show this message and exit.
```

> `workflow-controller run` 默认即 headless（所有输出为 JSON lines）；交互式 UI 请用 `workflow-controller web`（需另装控制台发行包，内核-only 安装时该子命令给出可诊断错误）；只需要 HTTP 接口时用内核自带的 `workflow-controller serve`（见下文「HTTP API 模式」）。

## 认证配置

- `settings.conf` → API keys、模型、base URL
- `auth_state.json` → provider 状态、reset 时间
- 环境变量 → 作为 fallback

### GLM / 第三方端点

`runtime: claude_cli` 时，Workflow Controller 把以下 env 透传给 `claude` 子进程：

| env | 说明 |
|-----|------|
| `ANTHROPIC_BASE_URL` | 第三方端点（GLM: `https://open.bigmodel.cn/api/anthropic`） |
| `ANTHROPIC_AUTH_TOKEN` | Bearer token（GLM 鉴权走它，非 `x-api-key`） |
| `ANTHROPIC_MODEL` | 模型名（如 `glm-5.2`） |

## 适用于 CI/CD

```bash
#!/bin/bash
# 1. 在 CI 环境中准备配置
export WORKFLOW_CONTROLLER_CFG_DIR=/tmp/workflow-controller-ci-conf
mkdir -p "$WORKFLOW_CONTROLLER_CFG_DIR"

# 2. 写入 settings.conf (secrets from CI env vars)
cat > "$WORKFLOW_CONTROLLER_CFG_DIR/settings.conf" <<EOF
api_keys:
  - $ANTHROPIC_API_KEY
api_base_url: $ANTHROPIC_BASE_URL
api_model: $ANTHROPIC_MODEL
EOF

# 3. 执行任务
workflow-controller run \
  --config /path/to/workflow-controller.yaml \
  --settings-dir "$WORKFLOW_CONTROLLER_CFG_DIR" \
  --headless \
  > workflow-controller-output.jsonl 2> workflow-controller-errors.log
```

## 事件输出说明

| event | 说明 |
|-------|------|
| `auth` | 认证状态：mode, api_keys 数量, plan 是否可用 |
| `info` | 普通信息消息 |
| `error` | 错误消息 |
| `session_started` | session 启动，含 session id |
| `session_update` | session 状态变化：state, iter, progress, elapsed |
| `assistant_message` | worker agent 输出消息 |
| `pipeline` | 流水线进度：progress, stages |
| `usage` | Token/cost 统计 (最后 emit) |
| `interrupted` | 用户 Ctrl+C |
| `done` | 全部完成 |

## HTTP API 模式：`workflow-controller serve`

需要常驻 HTTP 接口而不需要 Web UI 时，用内核自带的 `serve`：仅安装内核包即可用，不启动 Vite 与 Insight。

```bash
# 默认 127.0.0.1:18600；hook server 端口默认 18765
workflow-controller serve

# 指定端口、设置目录与日志级别
workflow-controller serve --port 18700 --hook-port 18775 \
  --settings-dir ~/.config/workflow-controller --log-level DEBUG
```

启动后 `GET /api/v1/health` 返回 200，`/docs` 为 Swagger，`/api/v1` 提供与 Web 控制台相同的内核面端点（清单见 [docs/api-contract.md](docs/api-contract.md)）：

```bash
curl -s http://127.0.0.1:18600/api/v1/health
curl -s -X POST http://127.0.0.1:18600/api/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{"format": "fake@1", "program_path": "/abs/path/fake-program.yaml", "work_dir": "/abs/path/work"}'
```

- 配置读启动目录的 `workflow-controller-config.yaml`，与 `web` 子命令同一份。
- 默认只绑定回环地址且没有鉴权；绑定到其他地址之前先加访问控制。
- hook server 端口被占用时以退出码 3 退出，并在标准错误报告监听地址与系统错误。

## 注意事项

- **headless 模式下 reviewer 自动设为 `auto`**：reviewer 自动执行，不需要手动交互
- **`workflow-controller run` 始终为 headless**：所有输出为 JSON lines；交互式 UI 请用 `workflow-controller web`（需另装控制台发行包），只要 HTTP 接口用 `workflow-controller serve`
- **日志文件**：仍写入项目 `.workflow-controller/` 目录下的日志文件
- **auth_state.json**：自动创建和维护，记录 provider 轮转状态
