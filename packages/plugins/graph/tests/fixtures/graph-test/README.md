# Workflow Controller graph-test

> 本目录原住仓根 `examples/graph-test`，现为 graph 插件翻译测试的场景输入
> （`test_translator.py` 包内定位）；场景 YAML 仍可按下表手工试跑。

用最小 DAG 演示 graph 工作流驱动的核心能力：**串行/并行调度、重试、回滚、子图、
验收脚本、run_on_failure 收尾**。

目录里的 workflow（都是 `work_dir: .`、`agents_dir: ./agents`）：

| workflow | 文件 | 演示什么 | 预期终态 |
|---|---|---|---|
| `graph-demo` | `graph-workflow.yaml` | 全语义大图：串行 DAG + 并行 + 重试 + 回滚 + 子图 + run_on_failure | **failed（设计语义）**——`always-fail` 节点耗尽重试即停图，`final-report` 照常收尾 |
| `parallel-test` | `parallel-test.yaml` | 最小 agent 暂停演示：`prep → (agent-1 ∥ agent-2)`，3 节点 | completed |
| `agent-retry-test.yaml` | agent 重试 | 成员按重试上下文在第 2 次尝试写对产物 | completed |
| `retry-rollback-test.yaml` | 重试 + 回滚 + agent 验收 | bad-node 失败回滚祖先、计数器跨回滚保留 | completed |
| `subgraph-test.yaml` | 子图 + agent 验收 | generator 产出子图 yaml 并合并执行 | completed |
| `run-on-failure-test.yaml` | run_on_failure | 纯 bash + 失败收尾报告 | failed（设计语义） |
| `test-all-scenarios.yaml` | 失败、执行暂停、验收暂停 | 三个并行节点分别演示失败与暂停恢复 | failed（含失败节点） |
| `test-pause-resume.yaml` | bash 执行暂停与并行恢复 | 两个根节点完成后再运行依赖节点 | completed |
| `test-pause-resume-agent.yaml` | agent 验收暂停与并行恢复 | 两个执行节点分别启动 verifier session | completed |

> agent 节点按 `max_parallel` **真并行执行**（run 平面并发准入）；不设置时不限。
> 上述 workflow 里「并行」即真实并发。

## 前置

- 装好 Workflow Controller 并起 web（`pip install -e ".[dev]"` → `.venv/bin/workflow-controller web`，端口 18600）。
- graph plugin 默认启用；若曾关闭，Settings 里重新打开。
- agent 节点走 `opencode` runtime，需配好全局 opencode provider
  （`~/.config/opencode/opencode.json`）。

## 怎么跑

入口（「导入」弹窗**不含**工作流分支，别找它）：

1. **新建会话入口**：Dashboard → 「+ 新建会话」→ 「YAML 路径」填本目录下某个
   workflow 的绝对路径（项目配置 `workflow-controller.yaml` 的 `graph_yaml:` 会预填
   `graph-workflow.yaml`）→ 「启动工作流」→ 跳转 `/run/:run_id`。

之后：

2. Run 详情页看 DAG 实时着色与成员表；
   点 bash 节点看运行历史与 stdout，点 agent 节点看历史并可「查看会话」跳
   session 详情页看 LLM 输出。验收 agent 是独立的 verifier session，暂停/恢复时与执行 session
   一起由 RunCoordinator 处理。
3. 通过 Run 控制接口（或驱动自暂停）阻塞下一轮并暂停在跑的 agent session；「恢复」唤醒并
   resume（恢复轮会自动重发任务 prompt，早暂停丢上下文已不构成问题）；
   终态后「重启」续跑（failed/stopped 节点重置重跑）。
4. 运行过程中可在 RunView 观察 `running → stopped/paused → running` 的断点恢复；失败节点重开
   会新建执行成员，暂停节点若成员仍存在则复用原 session。
5. 终态后 `graph-workflow` 显示 failed 属正常（见上表）；`output/report.txt` 是
   run_on_failure 的收尾报告。历史 run 在 Dashboard 主列表（run 折叠卡片）可查、可删。

产物落在各 workflow 所在目录的 `output/`；验收脚本逐节点校验产物文件是否齐备。
