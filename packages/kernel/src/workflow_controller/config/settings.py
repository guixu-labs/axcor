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

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from workflow_controller.config.bootstrap import DEFAULT_STARTING_PROMPT, DEFAULT_TASK_GOAL

if TYPE_CHECKING:
    from pathlib import Path


class HookConfig(BaseModel):
    type: str = "session"
    session: str | None = None
    prompt_template: str = ""
    when: str | None = None


ThinkingMode = Literal["disabled", "enabled", "adaptive", "omitted"]


def _migrate_thinking_enabled(data: Any) -> Any:
    """Translate legacy boolean reviewer settings into the mode representation."""
    if isinstance(data, dict) and not data.get("thinking_mode"):
        if data.get("thinking_enabled") is True:
            data["thinking_mode"] = "enabled"
        elif data.get("thinking_enabled") is False:
            data["thinking_mode"] = "disabled"
    return data


class LoopConfig(BaseModel):
    max_iterations: int = 10
    # iter 1: kick off with the user-supplied initial_prompt.
    # iter >1 with queued feedback: feed the feedback string verbatim.
    # iter >1 without feedback (normal case once async reviewer was removed,
    #   or when Stop hook hit block_count fallback): give a self-recovery
    #   prompt that makes worker check what's already on disk before doing
    #   anything else. Avoids "continue what?" confusion when the new CC
    #   subprocess has no memory of previous iters.
    prompt_template: str = (
        "{% if iteration == 1 %}"
        "{% if feedback %}{{ feedback }}{% else %}{{ initial_prompt }}{% endif %}"
        "{% else %}"
        "{% if feedback %}{{ feedback }}{% else %}"
        "继续本轮未完成的工作。先检查工作目录下已有的产物与进度,"
        "从断点继续推进。"
        "{% endif %}"
        "{% endif %}"
    )
    evaluator: str | None = None
    # 问句检测开关：None=沿现状（每轮检测，命中即等人工）；False=跳过（无人值守成员防挂死）
    question_detect: bool | None = None


class SessionReviewerConfig(BaseModel):
    """Reviewer settings captured when a session is created."""

    enabled: bool = True
    mode: str = "auto"
    model: str = "claude-opus-4-6"
    provider_index: int | None = None
    timeout_seconds: float = 300.0
    thinking_mode: ThinkingMode = "disabled"
    thinking_budget_tokens: int = 2048
    task_goal: str = DEFAULT_TASK_GOAL

    @model_validator(mode="before")
    @classmethod
    def migrate_thinking_enabled(cls, data: Any) -> Any:
        """Translate pre-matrix session snapshots into the mode representation."""
        return _migrate_thinking_enabled(data)


class SessionTracebackConfig(BaseModel):
    """单会话 traceback 工作区模式配置（内核侧配置通道）。

    词面与工作流侧 traceback 段对齐：未携带或 ``enabled`` 关闭的会话
    行为不变；snapshot 兼容模式已随旧插件退役移除，本通道只管
    workspace 模式。
    """

    enabled: bool = True
    # 模式仅支持 workspace：snapshot_compatible 已随旧插件退役，显式拒绝防旧配置误配
    mode: str = "workspace"
    # 派发屏障弱化档：fail（默认，严格）| compat_late_barrier（显式弱化）
    on_no_dispatch_barrier: str = "fail"
    conflict_policy: str = "git_merge"
    retain_after_merge: bool = False
    background_drain_timeout_s: float = 60.0
    merge: list[str] = []
    writer_local: list[str] = []
    shared_readonly: list[str] = []
    shared_exclusive: list[str] = []
    external: list[str] = []


class SessionConfig(BaseModel):
    name: str
    role: str = "worker"
    cwd: str = "."
    extra_dirs: list[str] = []
    skills_path: str | None = None
    agents_path: str | None = None
    initial_prompt: str = ""
    permission_mode: str = "bypassPermissions"
    allowed_tools: list[str] | None = None
    # 禁用工具（cc PascalCase 词面）：严格 traceback 下缺屏障能力的 runtime
    # 借此拒绝 writer 子代理派发（opencode 顶层 permission deny / cc 原生禁用）
    disallowed_tools: list[str] | None = None
    max_turns: int | None = None
    max_budget_usd: float | None = None
    model: str | None = None
    provider_index: int | None = None
    model_pinned: bool = False
    reviewer: SessionReviewerConfig | None = None
    runtime: str | None = (
        None  # per-session driver runtime（覆盖全局）；None → 用全局 config runtime
    )
    cli_wrapper: str = ""  # 可选命令前缀，当前仅支持 cpx
    loop: LoopConfig | None = None
    system_prompt: str = ""
    agent_name: str = (
        ""  # opencode: 传给 --agent <name>，从 .opencode/agents/<name>.md 加载 agent 定义
    )
    hooks: dict[str, list[HookConfig]] = {}
    criteria: dict[str, Any] = {}
    # 领域阶段声明（per-project，来自 workflow-controller.yaml 的 stages:）。staged_pipeline 门禁
    # plugin 在 on_iteration_boundary / on_stop_hook 时从这里读；空 = 该 session 不套阶段。
    stages: list[dict[str, Any]] = []
    # 可选：breather handoff / continuation（接续 worker）prompt 模板文件路径（per-project，
    # 来自 workflow-controller.yaml）。None = 用 breather 内置默认模板（老项目零变化）。相对路径相对
    # 项目目录解析。随 handoff 接力传给 successor session（见 build_new_session_config）。
    handoff_prompt_path: str | None = None
    continuation_prompt_path: str | None = None
    group_handoff_prompt_path: str | None = None
    context_threshold_pct: float | None = Field(default=None, gt=0, le=100)
    subagent_context_threshold_pct: float | None = Field(default=None, gt=0, le=100)
    context_window: int | None = Field(default=None, gt=0)
    # 子进程环境变量覆盖（per-session）：driver 起 opencode/claude 子进程时把这些 key
    # 注入 proc_env（覆盖 os.environ）。子代理回滚的恢复会话靠它把 ``CLAUDE_CONFIG_DIR``
    # 指向恢复种子所在的配置目录（core/restore/conversation.py）；其余场景留空。
    env_overrides: dict[str, str] = {}
    # run 归属（宿主级 run 记账层，ARCHITECTURE.md「Run 身份」）：解释器起节点
    # session 时传入；内核照存不解析（argv 类比）。None = 不归属任何 run；
    # labels 里的 run_id 是给消费者免 join 的冗余，归属判定以 run_id 为准。
    run_id: str | None = None
    labels: dict[str, str] = {}
    # 单会话 traceback 工作区模式开关（None = 未开启，行为逐字不变）：
    # 开启后原目录只做基线导入，会话工作在托管工作区，终态经三方同步写回
    traceback: SessionTracebackConfig | None = None


class HookServerConfig(BaseModel):
    enabled: bool = True
    port: int = 18765
    host: str = "localhost"


class ReviewerConfig(BaseModel):
    enabled: bool = True
    model: str = "claude-opus-4-6"
    timeout_seconds: float = 300.0
    thinking_mode: ThinkingMode = "disabled"
    thinking_budget_tokens: int = 2048
    # Default to the project-wide DEFAULT_TASK_GOAL so reviewer is never
    # initialised with an empty grounding section, even if the yaml is
    # missing the `reviewer:` block entirely (e.g. created by Save Path
    # alone, or by an older code path that didn't write the section).
    task_goal: str = DEFAULT_TASK_GOAL

    @model_validator(mode="before")
    @classmethod
    def migrate_thinking_enabled(cls, data: Any) -> Any:
        """Translate legacy global reviewer configs onto the mode field."""
        return _migrate_thinking_enabled(data)


class WatchdogConfig(BaseModel):
    """Watchdog（subagent 异常检测）配置：仅检测字段，仅全局生效。"""

    # 就地赋值时按字段类型校验：设置页热更新写入坏值立即暴露
    model_config = ConfigDict(validate_assignment=True)

    enabled: bool = True
    # subagent 运行超过该秒数 → 发告警。0 = 关闭该规则。
    subagent_timeout_seconds: int = 1800  # 30 分钟
    # 追踪单个文件：连续 N 个 subagent stop 后该文件内容无变化 → 告警。
    # 空字符串 = 关闭该规则。路径相对 session cwd 解析。
    track_file: str = ""
    file_stall_threshold: int = 3
    # 关键词检测：每次 subagent stop 后检查指定文件中是否包含该关键词 → 告警。
    # 空字符串 = 关闭该规则。大小写不敏感。
    track_keyword_file: str = ""
    track_keyword: str = ""


# 用户可见品牌名默认值：侧边栏标识、浏览器标签标题、Swagger 标题与 CLI help 头共用。
DEFAULT_BRAND_NAME = "Workflow Controller"


class AppConfig(BaseModel):
    """全局配置模型：顶层未知段原样读入落盘（段内容归段所有者定义）。"""

    # 顶层 extra=allow：内核模型外的配置段（如领域自有段）round-trip 不丢
    model_config = ConfigDict(extra="allow")

    sessions: list[SessionConfig] = []
    hook_server: HookServerConfig = HookServerConfig()
    reviewer: ReviewerConfig = ReviewerConfig()
    starting_prompt: str = DEFAULT_STARTING_PROMPT
    project_dir: str = "."
    skills_path: str = ""
    agents_path: str = ""
    # 用户可见品牌名（侧边栏标识、浏览器标签标题、Swagger 标题、CLI help 头），空值回落默认。
    brand_name: str = DEFAULT_BRAND_NAME
    runtime: str = "claude_sdk"  # claude_sdk (SDK) | claude_cli | opencode | codex_cli
    cli_wrapper: str = ""  # Claude CLI / OpenCode 可选命令前缀
    model: str = ""  # model name (e.g. "alibaba-cn/deepseek-v4-pro" for opencode)
    loop: LoopConfig = LoopConfig()  # 顶层 loop（evaluator / max_iterations，yaml 可配）
    system_prompt: str = (
        ""  # worker system 级指令：claude_sdk/cli 替换 driver 的 system message（内核级）；
    )
    # opencode 当 worker agent prompt。CLAUDE.md/AGENTS.md 是独立 project context、始终加载不受影响。yaml 可配
    # 领域阶段声明（per-project）：选目录起 session 时注入到 SessionConfig.stages，
    # 供 staged_pipeline 门禁 plugin 读。空 = 不套阶段（默认）。
    stages: list[dict[str, Any]] = []
    # 可选：breather handoff / continuation prompt 模板文件路径（per-project）。选目录起
    # session 时注入到 SessionConfig；None = 用 breather 内置默认模板（老项目零变化）。
    handoff_prompt_path: str | None = None
    continuation_prompt_path: str | None = None
    group_handoff_prompt_path: str | None = None
    context_threshold_pct: float | None = Field(default=None, gt=0, le=100)
    subagent_context_threshold_pct: float | None = Field(default=None, gt=0, le=100)
    context_window: int | None = Field(default=None, gt=0)
    # 日志级别：DEBUG, INFO, WARNING, ERROR, CRITICAL（默认 INFO）
    log_level: str = "INFO"
    # Claude 会话定位是否额外扫描 /home、/Users 下其他用户的 ~/.claude。
    # 默认关闭：共享主机上无条件扫描会暴露其他 Unix 用户的会话（可经 Web 端口导入），
    # 仅当 Web 与 CLI 以不同用户运行时才需显式开启。
    claude_scan_user_homes: bool = False
    # watchdog 通用能力配置（subagent 超时检测）
    watchdog: WatchdogConfig = WatchdogConfig()
    # graph 模式默认 yaml 路径（per-project）：新建会话弹窗自动填充
    graph_yaml: str = ""

    @staticmethod
    def from_yaml(path: Path) -> AppConfig:
        import yaml

        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return AppConfig(**data)

    def to_yaml(self, path: Path) -> None:
        import yaml

        # Write the full schema (no exclude_defaults) so users can see every
        # field and tweak it without having to know which keys to add. Matches
        # the verbose-by-default behavior of ``bootstrap.write_yaml``,
        # eliminating the prior path-dependent surprise where Settings → Save
        # would strip out fields like the entire reviewer block if they
        # happened to all be at their pydantic defaults.
        data = self.model_dump()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
