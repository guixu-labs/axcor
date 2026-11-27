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

__all__ = [
    "HandoffGenerator",
    "count_checkboxes",
    "extract_worker_narrative",
    "list_generated_files",
]

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

# ── 确定性工具（handoff agent 专用）──────────────────────────
#
# 真相源在 scripts/handoff_tools.py（自包含、零 workflow_controller 依赖，可被复制到
# 目标工程 .workflow-controller/ 下用 `python .workflow-controller/handoff_tools.py <cmd>` 独立运行）。
# 这里 re-export，保持 `from workflow_controller.plugins.breather.handoff import
# count_checkboxes` 等历史 import 路径不破。改逻辑请改 handoff_tools.py。
from workflow_controller.plugins.breather.scripts.handoff_tools import (
    count_checkboxes,
    extract_worker_narrative,
    list_generated_files,
)

if TYPE_CHECKING:
    from workflow_controller.session.session import Session

logger = logging.getLogger(__name__)

# Keep this as a single line: embedded newlines in a `-p` value passed to
# claude.CMD truncate cmd.exe's `%*` forwarding, dropping every flag after
# the prompt (including `--output-format stream-json`). See
# tests/plugins/test_handoff_cmd_argv.py for the repro.
RESUME_PROMPT = """这是一个从上一个 session 交接过来的任务。请按顺序继续：

1. 用 Read 工具完整读取交接文档 `{handoff_path}`，确认「项目坐标」「阻塞项」「如何接续」和「待执行任务」。
2. 核对交接文档列出的项目路径与磁盘产物。已有且验证通过的工作不重做；状态不明的工作先检查，再决定是否继续。
3. 如果阻塞项明确要求用户确认，先向用户提出问题，收到回答后再推进相关工作。
4. 按「如何接续」读取必要输入文件，执行下一步。若交接文档记录了中断的子代理，按原任务和类型重新派发新的子代理，并让它们从各自的断点继续。
5. 按项目原有约定更新任务清单或进度文件；不要凭空创建领域专用文件。完成后报告实际产物与验证结果。
"""

HANDOFF_AGENT_PROMPT = """你是一个交接文档组装器。盘点上一 session 的实际进度，写出 `{handoff_filename}`，让一个零记忆的新 session 能从断点继续。

## 信息源

以项目磁盘文件和确定性工具结果为准；worker narrative、final_text 用来解释意图和决策。资料冲突时记录冲突，不猜测。

## 工作步骤

1. 以当前项目目录为工作范围，阅读下方附带的初始任务、上一份交接文档（如有）及现有任务清单。不要推断特定领域目录或要求项目不存在的文件。
2. 用 Bash 运行 `{worker_session_id}` 对应的 `extract_worker_narrative`，并在项目确有任务清单或产物时运行 `count_checkboxes`、`list_generated_files`：

```bash
python .workflow-controller/handoff_tools.py extract_worker_narrative "." "{worker_session_id}"
python .workflow-controller/handoff_tools.py count_checkboxes "<实际存在的任务清单路径>"
python .workflow-controller/handoff_tools.py list_generated_files "." "<项目产物 glob>"
```

若 `python` 不可用，使用 `python3`。不要对不存在的任务清单编造统计。
3. 继承历史信息：{birth_handoff_doc_section}
4. 核对磁盘现状、已完成任务、正在进行的工作、阻塞及验证结果，然后写文档。

## 文档结构

## 0. 项目坐标
记录项目目录、任务目标和相关工作目录。仅写有证据的路径。

## 1. 阻塞项
写明真实阻塞、需要谁提供什么；等待用户答复的标为「⚠️ 需用户确认」。没有阻塞则明确写「无」。

## 2. 如何接续
写可立即执行的下一步、所需输入文件和先后顺序。若有中断的子代理，逐一记录其类型、原任务、进度和重新派发指令。列出已经验证完成、不必重跑的工作。

## 3. 进度
列出已完成、进行中、待执行的工作及其证据。数字仅来自确定性工具或磁盘记录。

## 4. 产物清单
逐项列出路径、存在性和验证状态。

## 5. 待执行任务分解
给出每项任务的输入、输出、前置条件与验收方式。

## 6. 核心发现
保留已有发现，补充本轮得到的关键事实；只增不删。

## 7. 关键决策
保留已有决策，并说明本轮决策的原因；只增不删。

## 8. 踩过的坑
保留已有问题、根因和解决办法；只增不删。

## 9. 本轮变更
只记录本 session 相对上一轮的实际变化。

## 10. 编译 / 验证命令
逐字记录实际运行的命令、结果和未执行的验证。不要把未运行的命令写成通过。

## 完成条件

通读文档，核对文件路径和状态，确保下一 session 无需猜测就能开始。把文档写到 `{handoff_filename}`，最后写 `{handoff_filename}.done`，内容为 `done`。完成信号必须在文档落盘并自检后写。

不确定的内容写 `UNKNOWN` 并说明缺少什么证据；不得编造。
"""

# Trailing `-hN` suffix produced by next_handoff_name (N ≥ 2). Only matches
# at end-of-name so middle-of-name patterns like "foo-h2-bar" are untouched.
_HANDOFF_SUFFIX_RE = re.compile(r"-h(\d+)$")
_HANDOFF_SECTION_RE = re.compile(r"^<!-- workflow:(common|main|group) -->$")


def _handoff_sections(template: str) -> tuple[dict[str, list[str]], bool]:
    sections: dict[str, list[str]] = {"common": [], "main": [], "group": []}
    current = "common"
    marked = False
    in_comment = False
    for line in template.splitlines(keepends=True):
        stripped = line.strip()
        if in_comment:
            if "-->" in line:
                in_comment = False
            continue
        if _HANDOFF_SECTION_RE.fullmatch(stripped):
            current = _HANDOFF_SECTION_RE.fullmatch(stripped).group(1)
            marked = True
            continue
        if stripped.startswith("<!--"):
            if "-->" not in line:
                in_comment = True
            continue
        sections[current].append(line)
    return sections, marked


def handoff_template_is_sectioned(template: str) -> bool:
    """Return True if the template carries recognized section markers."""
    _, marked = _handoff_sections(template)
    return marked


def handoff_template_section(template: str, mode: str) -> str:
    """Return one explicitly marked section, without shared or fallback text."""
    sections, marked = _handoff_sections(template)
    return "".join(sections[mode]).strip() if marked else ""


def handoff_template_for_mode(template: str, mode: str) -> str:
    """Choose the shared and mode-specific paragraphs of one handoff file."""
    sections, marked = _handoff_sections(template)
    if not marked:
        return template
    if mode == "main" and not "".join(sections["main"]).strip():
        return "".join(sections["common"]).strip() + "\n\n" + HANDOFF_AGENT_PROMPT
    return "".join(sections["common"] + sections[mode]).strip()


def build_birth_handoff_doc_section(birth_handoff_doc: str) -> str:
    """生成「继承历史信息」段落，供主代理与整组交接模板共用。

    模板里的 `{birth_handoff_doc_section}` 占位符在两种流程里都要被填成
    具体指引，不能以字面量交给交接 agent。
    """
    if birth_handoff_doc:
        return (
            f"你正在为其写交接文档的前序 session 是从 handoff 文档 `{birth_handoff_doc}` 创建的 continuation session。"
            f"先用 Read 工具读取 `{birth_handoff_doc}` 的第 6、7、8 节（核心发现、关键决策、踩过的坑），"
            f"把所有已有条目**继承到本次文档的对应节**（只增不删）。新提取的内容追加到已有条目后面。"
            f"其余节用本次最新数据。"
        )
    return "这是第一次 handoff，无需继承历史文档。"


class _SafeSlotDict(dict[str, str]):
    """str.format_map 辅助：模板里未知的 {slot} 原样保留、不抛 KeyError。

    领域自定义模板可写任意 {占位}，只有已知槽（handoff_filename /
    worker_session_id）被替换，其余照字面输出——避免领域作者误写未知槽位
    导致整个 handoff 崩。
    """

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


class HandoffGenerator:
    def __init__(
        self,
        skill: str,
        handoff_filename: str,
        handoff_prompt_path: str | None = None,
        continuation_prompt_path: str | None = None,
    ) -> None:
        self.skill = skill
        self.handoff_filename = handoff_filename
        # 可选：领域自定义 handoff prompt 模板文件。设了且可读 → 用它代替内置
        # HANDOFF_AGENT_PROMPT；未设或读不到 → 内置（老用户零变化）。相对路径相对
        # project_dir 解析。
        self.handoff_prompt_path = handoff_prompt_path
        # 可选：领域自定义 continuation（接续 worker）prompt 模板文件。设了且可读 →
        # 用它代替内置 RESUME_PROMPT；未设或读不到 → 内置（老用户零变化）。相对路径
        # 相对 project_dir 解析。模板里 {handoff_path} 会被填，其他 {占位} 原样保留。
        self.continuation_prompt_path = continuation_prompt_path

    @staticmethod
    def next_handoff_name(name: str) -> str:
        """Compute the post-handoff session name from `name`.

        First handoff:    foo            → foo-h2
        Subsequent:       foo-h2         → foo-h3
                          foo-h3         → foo-h4
        Legacy chained:   foo-h2-h2      → foo-h2-h3  (only the trailing
                                            -hN segment is incremented, so
                                            sessions named under the old
                                            scheme keep growing linearly
                                            instead of doubling suffixes)
        """
        m = _HANDOFF_SUFFIX_RE.search(name)
        if m:
            n = int(m.group(1))
            return _HANDOFF_SUFFIX_RE.sub(f"-h{n + 1}", name)
        return f"{name}-h2"

    @staticmethod
    def handoff_session_name(session_id_short: str, n: int) -> str:
        """生成交接 session 名：{id}-handoff-{n}。

        Pattern: {id}-handoff-{n}
        Example: 12345678-handoff-1
        """
        return f"{session_id_short}-handoff-{n}"

    @staticmethod
    def continuation_session_name(session_id_short: str, n: int) -> str:
        """生成接续 session 名：{id}-continue-{n}。

        Pattern: {id}-continue-{n}
        Example: 12345678-continue-1
        """
        return f"{session_id_short}-continue-{n}"

    @staticmethod
    def handoff_doc_name(session_id_short: str, n: int) -> str:
        """生成交接文档文件名：{id}-handoff-{n}-doc。

        Pattern: {id}-handoff-{n}-doc
        Example: 12345678-handoff-1-doc
        """
        return f"{session_id_short}-handoff-{n}-doc"

    @staticmethod
    def done_file_name(session_id_short: str, n: int) -> str:
        """生成完成信号文件名：{id}-handoff-{n}-done。

        Pattern: {id}-handoff-{n}-done
        Example: 12345678-handoff-1-done
        """
        return f"{session_id_short}-handoff-{n}-done"

    def build_handoff_agent_prompt(
        self,
        final_text: str,
        project_dir: str,
        worker_session_id: str = "",
        handoff_doc_name: str | None = None,
        initial_prompt: str = "",
        birth_handoff_doc: str = "",
        events_project_dir: str = "",
    ) -> str:
        """构建 handoff agent session 的初始 prompt。

        完整指令（模板 + final_text + initial_prompt + birth_handoff_doc）写到 `.workflow-controller/handoff-agent-prompt.md`，
        本方法返回一句短引用 prompt。这样 `-p` 参数极短，避开 Windows
        cmd.exe 8191 字符命令行长度限制（见 build_resume_prompt 相同约束）。
        每次调用都重写该文件——worker_session_id 与 final_text 每轮不同。

        handoff_doc_name: 命名规则的交接文档名（如 xlogy-123-handoff-1-doc）。
        如果提供，覆盖 self.handoff_filename。

        initial_prompt: 初始 session 的 starting_prompt，用于保留原始任务背景。

        birth_handoff_doc: 上一 worker session 的 birth handoff doc 路径，用于继承历史上下文。

        events_project_dir: 守护进程事件库所在目录（单库化后的唯一事件落盘
        位置）。提供时把 prompt 中 extract_worker_narrative 的 "." 参数替换
        为该目录；不提供则保留模板原文（兼容旧调用方）。
        """
        doc_name = handoff_doc_name or self.handoff_filename
        template = self._load_template(project_dir)

        birth_handoff_doc_section = build_birth_handoff_doc_section(birth_handoff_doc)

        full_prompt = template.format_map(
            _SafeSlotDict(
                handoff_filename=doc_name,
                worker_session_id=worker_session_id,
                birth_handoff_doc_section=birth_handoff_doc_section,
            )
        )
        tools_dir = Path(project_dir) / ".workflow-controller"
        full_prompt = full_prompt.replace(
            ".workflow-controller/handoff_tools.py",
            str(tools_dir / "handoff_tools.py"),
        )
        if events_project_dir:
            # worker 事件只落守护进程库；agent cwd（worker 工程）下没有该库，
            # 静态库还可能是旧库，narrative 的读库目录必须显式改指守护进程侧
            full_prompt = full_prompt.replace(
                'extract_worker_narrative "."',
                f'extract_worker_narrative "{events_project_dir}"',
            )
        if final_text:
            full_prompt += f"\n\n## Worker 的 final_text（辅助参考，优先级低于磁盘 + narrative）\n\n{final_text}"
        else:
            full_prompt += "\n\n## Worker 的 final_text\n\n（本次未捕获到 worker 的输出。）"

        if initial_prompt:
            full_prompt += f"\n\n## 初始 session 的 starting_prompt（任务背景）\n\n{initial_prompt}"

        prompt_file = Path(project_dir) / ".workflow-controller" / "handoff-agent-prompt.md"
        prompt_file.parent.mkdir(parents=True, exist_ok=True)
        prompt_file.write_text(full_prompt, encoding="utf-8")

        return (
            f"你是 handoff agent。请先用 Read 工具读取 `{prompt_file}` "
            f"的完整内容，然后严格按其中的指令执行，产出交接文档 `{doc_name}`。"
        )

    def handoff_path(self, session: Session, filename: str | None = None) -> Path:
        """返回交接文档的完整路径。

        filename: 命名规则的交接文档名（如 xlogy-123-handoff-1-doc）。
        如果提供，覆盖 self.handoff_filename。
        """
        cwd = Path(session.config.cwd) if session.config.cwd else Path(".")
        doc_name = filename or self.handoff_filename
        p = Path(doc_name)
        if str(p).startswith("~"):
            p = p.expanduser()
        if not p.is_absolute():
            p = cwd / p
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def build_resume_prompt(
        self,
        handoff_file: Path,
        continuation_name: str = "",
        project_dir: str = ".",
    ) -> str:
        # Reference the file path instead of inlining the content. The full
        # file content can easily exceed Windows cmd.exe's 8191-char command
        # line limit when passed as a `-p` argument to the claude.CMD npm
        # wrapper, which spawns the worker subprocess with returncode=1
        # (stderr: "The command line is too long."). The new worker has the
        # same cwd as the old session, so the path is accessible via Read.
        # Prepend continuation_name to prompt so opencode auto-generates the
        # correct session title（见命名规则）.
        #
        # 模板来自 _load_continuation_template（continuation_prompt_path 配了就用
        # 领域自定义，否则内置 RESUME_PROMPT）。用 format_map + _SafeSlotDict：
        # {handoff_path} 被填，其余未知 {占位} 原样保留（领域模板可写任意占位）。
        template = self._load_continuation_template(project_dir)
        body = template.format_map(_SafeSlotDict(handoff_path=str(handoff_file)))
        if continuation_name:
            return f"{continuation_name}\n\n{body}"
        return body

    def build_new_session_config(
        self,
        session: Session,
        handoff_file: Path,
        continuation_name: str = "",
    ) -> Any:
        from workflow_controller.config.settings import SessionConfig

        old = session.config
        new_name = self.next_handoff_name(old.name)
        new_prompt = self.build_resume_prompt(
            handoff_file,
            continuation_name,
            project_dir=old.cwd or ".",
        )

        return SessionConfig(
            name=new_name,
            role=old.role,
            cwd=old.cwd,
            extra_dirs=list(old.extra_dirs),
            skills_path=old.skills_path,
            agents_path=old.agents_path,
            initial_prompt=new_prompt,
            permission_mode=old.permission_mode,
            allowed_tools=list(old.allowed_tools) if old.allowed_tools is not None else None,
            max_turns=old.max_turns,
            max_budget_usd=old.max_budget_usd,
            model=old.model,
            provider_index=old.provider_index,
            reviewer=old.reviewer.model_copy(deep=True) if old.reviewer else None,
            loop=old.loop,
            system_prompt=old.system_prompt,
            runtime=old.runtime,
            cli_wrapper=old.cli_wrapper,
            hooks=dict(old.hooks),
            criteria=dict(old.criteria),
            # run 归属随谱系透传：接续 session 与 worker 同 run
            run_id=getattr(old, "run_id", None),
            # 把 per-project 的 handoff/continuation 模板路径带给接续 session，
            # 后续轮次（h3、h4…）才继续用同一套领域模板。
            handoff_prompt_path=getattr(old, "handoff_prompt_path", None),
            continuation_prompt_path=getattr(old, "continuation_prompt_path", None),
            group_handoff_prompt_path=getattr(old, "group_handoff_prompt_path", None),
            context_threshold_pct=getattr(old, "context_threshold_pct", None),
            subagent_context_threshold_pct=getattr(old, "subagent_context_threshold_pct", None),
            context_window=getattr(old, "context_window", None),
        )

    def _load_template(self, project_dir: str) -> str:
        """解析并读取 handoff prompt 模板。读不到回落内置（graceful 降级）。"""
        if not self.handoff_prompt_path:
            return HANDOFF_AGENT_PROMPT
        p = Path(self.handoff_prompt_path)
        if not p.is_absolute():
            p = Path(project_dir) / p
        try:
            return (
                handoff_template_for_mode(p.read_text(encoding="utf-8"), "main")
                or HANDOFF_AGENT_PROMPT
            )
        except (OSError, UnicodeDecodeError):
            logger.warning(
                "handoff_prompt_path %s 不可读，回落内置 prompt",
                p,
            )
            return HANDOFF_AGENT_PROMPT

    def _load_continuation_template(self, project_dir: str) -> str:
        """解析并读取 continuation prompt 模板。读不到回落内置 RESUME_PROMPT。"""
        if not self.continuation_prompt_path:
            return RESUME_PROMPT
        p = Path(self.continuation_prompt_path)
        if not p.is_absolute():
            p = Path(project_dir) / p
        try:
            return p.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            logger.warning(
                "continuation_prompt_path %s 不可读，回落内置 RESUME_PROMPT",
                p,
            )
            return RESUME_PROMPT
