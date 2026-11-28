# axcor — Workflow Controller 内核与内置插件

axcor（Agent eXecution Core Orchestration Runtime）是 Workflow Controller 的内核与内置插件仓，以 uv workspace 组织多个发行包，共享 `workflow_controller` PEP 420 命名空间：

| 发行包 | 内容 |
|---|---|
| `workflow-controller-kernel` | 内核：headless runner、内核 HTTP API（`serve`）、Run 记录、事件存储、Session 生命周期、预算与门禁强制、插件 SDK、稳定控制 API |
| `workflow-controller-plugin-<name>` | 内置插件：`agent-traceback`（检查点回放）、`breather`（会话交接）、`fake-driver`（headless 验收驱动）、`file-provenance`（文件归因）、`graph`（graph-yaml@1 驱动与 DAG 面板）、`staged-pipeline`（阶段门禁） |
| `workflow-controller-hookserver` | 独立 Claude Code / opencode hook server |
| `@workflow-controller/file-provenance`（npm） | OpenCode 宿主的文件归因插件 |

内核可在不装配控制台的情况下启动和运行（Headless / HTTP API 模式）；Web 控制台、Insight 反代与领域导出住控制台发行包，在内核的 HTTP 路由之上叠加，只经 `CoreAPI` 与稳定 API 使用内核。

## 快速开始

```bash
# 克隆后安装（Python >= 3.12）
uv sync

# 全部测试与门禁
uv run pytest -q
uv run lint-imports --no-cache
uv run ruff check . && uv run ruff format --check .
```

仅安装内核（不含开发依赖）：

```bash
uv pip install workflow-controller-kernel
workflow-controller --help
```

headless 跑一个 fake run（不依赖任何外部 agent 运行时，先安装 `workflow-controller-plugin-fake-driver`）：

```python
import asyncio
from pathlib import Path

from workflow_controller.api.core_api import boot
from workflow_controller.config.settings import AppConfig, HookServerConfig, ReviewerConfig

async def main() -> None:
    workdir = Path("fake-demo").resolve()
    workdir.mkdir(exist_ok=True)
    program = workdir / "fake-program.yaml"
    program.write_text("format: fake@1\n", encoding="utf-8")
    config = AppConfig(
        project_dir=str(workdir),
        hook_server=HookServerConfig(enabled=True),
        reviewer=ReviewerConfig(enabled=False),
    )
    orchestrator, api = await boot(config, workdir / "workflow-controller-config.yaml")
    run = await api.run.start("fake-demo", program, fmt="fake@1")
    while run.status.value not in ("completed", "failed"):
        await asyncio.sleep(0.1)
        run = await api.run.get_run(run.run_id)
    print("run:", run.run_id, run.status)
    await orchestrator.shutdown()

asyncio.run(main())
```

运行手册见 [HEADLESS.md](HEADLESS.md)。安装插件即自动发现（entry point + 包内默认 `config.yaml`，用户可写覆盖住 `<用户配置根>/plugins/<name>/config.yaml`）。

只装内核也能经 HTTP 控制 run：`workflow-controller serve` 起常驻服务（不启动 Vite 与 Insight），默认只绑定 `127.0.0.1:18600`，`/docs` 为 Swagger：

```bash
workflow-controller serve --port 18600 --hook-port 18765
curl -s http://127.0.0.1:18600/api/v1/health
```

内核面 HTTP API 与 SSE 事件清单见 [docs/api-contract.md](docs/api-contract.md)，由 `packages/kernel/kernel_tests/contract/manifest.yaml` 生成。

## 子集导出

按插件清单导出「内核 + 指定插件」的自治子集（拷贝即用，测试随包）：

```bash
uv run python scripts/export_subset.py --output /tmp/subset --plugin breather --plugin graph
cd /tmp/subset && uv sync && uv run pytest -q
```

不指定 `--plugin` 时仅导出内核与验收驱动；hookserver 与 npm 子包不入子集。导出会同步裁剪 workspace 成员、path 依赖与 lock，并生成记录来源提交的 README。

## 仓库布局

```text
packages/
├── kernel/                  # 内核源码与 kernel_tests/
├── hookserver/              # 独立 hook server 子包
├── opencode-file-provenance/  # npm 子包（OpenCode 宿主集成）
└── plugins/<name>/          # 插件包：src + tests + pyproject（entry point 声明）
tests/                       # 跨包架构门禁（architecture）与共享测试基建（support）
examples/  conf/  docs/
```

## 文档

- [HEADLESS.md](HEADLESS.md) — headless 运行手册与 `serve` HTTP API 模式
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — 架构事实来源
- [docs/api-contract.md](docs/api-contract.md) — 内核面 HTTP API 与事件约定清单
- [docs/adr/0002-kernel-http-api-ownership.md](docs/adr/0002-kernel-http-api-ownership.md) — 内核面 HTTP 路由归属决策
- [docs/event-system.md](docs/event-system.md) — 事件系统
- [docs/watchdog.md](docs/watchdog.md) — 故障检测
- [docs/develop-workflows-guide.md](docs/develop-workflows-guide.md) — 工作流开发指南
- [docs/specs/hook-server-port.md](docs/specs/hook-server-port.md) — Hook Server 端口 CLI 设计
- [docs/release-process.md](docs/release-process.md) — 发布流程与版本策略
- [docs/opencode-file-provenance.md](docs/opencode-file-provenance.md) — OpenCode 文件归因指南

## 许可证

[MIT](LICENSE)
