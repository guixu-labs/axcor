#!/usr/bin/env python3
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

"""扫描推送或 PR 新增行中的黑名单词，并给出替换建议。

由 .githooks/pre-push 调用，Git 把待推送 refs 写到 stdin，每行四个字段：
<local ref> <local sha> <remote ref> <remote sha>。只检查分支推送新增的行，
tag 与分支删除跳过。词表默认是同目录 ai-words.txt（可用 --config 覆盖），
格式「词 => 建议」，`#` 注释，`!` 前缀行是豁免路径（fnmatch 匹配仓库相对
路径）；词表文件自身不参与扫描。词按子串匹配；纯英文条目暂不支持
（配置时报错）。

CI 通过 --pr BASE HEAD 扫描 PR 共同祖先到最新提交的最终差异。
`not in_hunk` 守卫只在 hunk 外把 `+++ ` 当文件头：新增行以 `++ ` 开头时
渲染成 `+++ …`，旧代码会误当文件头消费（跳过该行、清空路径状态、该 hunk
后续新增行被静默漏检），该守卫顺带修复了 pre-push 路径的这一既有误解析。

仅依赖标准库、兼容 Python 3.9：hook 运行在 .venv 创建之前。

退出码：1 = 命中黑名单词，中止 push；2 = 词表缺失或格式错误。
"""

from __future__ import annotations

import fnmatch
import logging
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, NoReturn

if TYPE_CHECKING:
    from collections.abc import Iterator

EXIT_BLOCKED = 1
EXIT_BAD_CONFIG = 2

CONFIG_NAME = "ai-words.txt"

ZERO_SHA_RE = re.compile(r"^0+$")
COMMIT_RE = re.compile(r"^COMMIT ([0-9a-f]{40})(?: (.*))?$")
HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")
PATCH_OPTIONS = (
    "-U0",
    "--no-color",
    "--no-ext-diff",
    "--no-textconv",
    "--src-prefix=a/",
    "--dst-prefix=b/",
)


class ConfigError(Exception):
    """词表内容无法解析。"""


class CheckFailureError(Exception):
    """词检终止：携带进程退出码，由 main 统一转为返回值。"""

    def __init__(self, exit_code: int) -> None:
        super().__init__(exit_code)
        self.exit_code = exit_code


logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class Rule(NamedTuple):
    word: str
    pattern: re.Pattern[str]
    suggestion: str


class Hit(NamedTuple):
    sha: str
    subject: str
    path: str
    line: int
    word: str
    suggestion: str


def fail(message: str, exit_code: int = EXIT_BLOCKED) -> NoReturn:
    logger.error("词检: %s", message)
    raise CheckFailureError(exit_code)


def _git_executable() -> str:
    executable = shutil.which("git")
    if executable is None:
        fail("找不到 git 可执行文件，请安装 git 并加入 PATH", EXIT_BAD_CONFIG)
    return executable


def git_lines(*args: str) -> Iterator[str]:
    """逐行产出 git 命令的 stdout；stderr 落临时文件，避免管道满时互相阻塞。"""
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace") as stderr_file:
        process = subprocess.Popen(
            [_git_executable(), *args],
            stdout=subprocess.PIPE,
            stderr=stderr_file,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if process.stdout is None:
            raise RuntimeError("git 子进程 stdout 管道未创建")
        for line in process.stdout:
            yield line.rstrip("\n")
        return_code = process.wait()
        stderr_file.seek(0)
        stderr_text = stderr_file.read().strip()
    if return_code != 0:
        fail(f"git {' '.join(args)} 失败: {stderr_text or '无错误输出'}")


def load_rules(config_path: Path) -> tuple[list[Rule], list[str]]:
    rules: list[Rule] = []
    excludes: list[str] = []
    for lineno, raw in enumerate(config_path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("!"):
            pattern = line[1:].strip()
            if not pattern:
                raise ConfigError(f"{config_path}:{lineno}: `!` 后缺少路径")
            excludes.append(pattern)
            continue
        if "=>" not in line:
            raise ConfigError(f"{config_path}:{lineno}: 缺少「=>」分隔符: {raw}")
        word, suggestion = (part.strip() for part in line.split("=>", 1))
        if not word:
            raise ConfigError(f"{config_path}:{lineno}: 词为空: {raw}")
        if not suggestion:
            raise ConfigError(f"{config_path}:{lineno}: 缺少建议: {raw}")
        if word.isascii():
            raise ConfigError(f"{config_path}:{lineno}: 暂不支持英文条目: {raw}")
        pattern = re.compile(re.escape(word))
        rules.append(Rule(word, pattern, suggestion))
    return rules, excludes


def ref_expressions(refs: list[str]) -> list[list[str]]:
    """把 stdin 的 ref 行转成 git log 修订表达式，每行一个 ref。"""
    expressions: list[list[str]] = []
    for line in refs:
        fields = line.split()
        if len(fields) < 4:
            continue
        local_ref, local_sha, _remote_ref, remote_sha = fields[:4]
        if not local_ref.startswith("refs/heads/"):
            continue  # tag 等非分支推送，内容已随分支 push 检查过
        if ZERO_SHA_RE.match(local_sha):
            continue  # 分支删除
        if ZERO_SHA_RE.match(remote_sha):
            # 新分支：git 不提供精确上游，退化为本地 remote-tracking ref 近似
            # （local --not --remotes）。若 origin/* 落后于远端（未 fetch），
            # 会把已推入远端的旧提交也纳入扫描，历史里含黑名单词的旧提交会
            # 造成误报；推送前 `git fetch` 可消除。见 docs/designs/
            # ai-word-push-check.md 第 4 节与 docs/CI.md 词检门禁。
            expressions.append([local_sha, "--not", "--remotes"])
        else:
            expressions.append([f"{remote_sha}..{local_sha}"])
    return expressions


_C_ESCAPES = {
    "a": "\a",
    "b": "\b",
    "t": "\t",
    "n": "\n",
    "v": "\v",
    "f": "\f",
    "r": "\r",
    '"': '"',
    "\\": "\\",
}


def _octal_digits(header: str, start: int) -> tuple[str, int]:
    """从 start 起收集最多 3 位八进制数字；返回 (数字串, 下一位置)。"""
    digits = ""
    pos = start
    while pos < len(header) and len(digits) < 3 and header[pos] in "01234567":
        digits += header[pos]
        pos += 1
    return digits, pos


def _decode_c_quoted(header: str) -> str:
    """还原 git C 引号包裹形态的字符串（转义序列与八进制）。"""
    chars: list[str] = []
    i = 1
    while i < len(header):
        ch = header[i]
        if ch == '"':
            break  # 未转义的收尾引号
        if ch == "\\" and i + 1 < len(header):
            nxt = header[i + 1]
            escaped = _C_ESCAPES.get(nxt)
            if escaped is not None:
                chars.append(escaped)
                i += 2
                continue
            if nxt in "01234567":
                digits, j = _octal_digits(header, i + 1)
                chars.append(chr(int(digits, 8)))
                i = j
                continue
        chars.append(ch)
        i += 1
    return "".join(chars)


def parse_path(header: str) -> str | None:
    """解析 `+++ ` 后的目标路径（`b/<path>` 或 git 引号包裹形态）。

    git 对含空格等特殊字符的路径会在末尾追加一个 `\\t` 作为「未加引号」的
    边界标记；对含 tab/引号等字符的路径则整体 C 引号包裹（`"b/<path>"`）。
    两者都要还原成真实路径，否则报告里的文件:行号会带上多余字符、`!` 豁免
    规则的 fnmatch 也会静默失效。
    """
    if header == "/dev/null":
        return None
    if header.startswith('"'):
        raw = _decode_c_quoted(header)
        return raw[2:] if raw.startswith("b/") else raw
    if header.startswith("b/"):
        path = header[2:]
        return path[:-1] if path.endswith("\t") else path
    return None


def is_excluded(path: str, excludes: list[str], config_rel: str | None) -> bool:
    return (config_rel is not None and path == config_rel) or any(
        fnmatch.fnmatch(path, pattern) for pattern in excludes
    )


def repo_relative(config_path: Path) -> str | None:
    """词表在仓库内的相对路径；词表在仓库外（如 --config 临时词表）返回 None。"""
    top_result = subprocess.run(
        [_git_executable(), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if top_result.returncode != 0:
        return None
    try:
        return str(config_path.resolve().relative_to(Path(top_result.stdout.strip()).resolve()))
    except ValueError:
        return None


def _log_args(expr: list[str]) -> list[str]:
    """组装 git log -p 参数（前缀与输出格式设定服务于路径还原与行号定位）。"""
    return [
        "-c",
        "core.quotepath=false",
        "log",
        "-p",
        *PATCH_OPTIONS,
        "--format=COMMIT %H %s",
        *expr,
    ]


def _line_hits(
    content: str,
    rules: list[Rule],
    sha: str,
    subject: str,
    path: str,
    line_number: int,
) -> list[Hit]:
    """一行新增文本命中的全部词表规则。"""
    return [
        Hit(sha, subject, path, line_number, rule.word, rule.suggestion)
        for rule in rules
        if rule.pattern.search(content)
    ]


def scan_patch(
    lines: Iterator[str], rules: list[Rule], excludes: list[str], config_rel: str | None
) -> list[Hit]:
    """扫描 Git 补丁中的新增行，返回命中列表。"""
    hits: list[Hit] = []
    current_sha = ""
    current_subject = ""
    current_path: str | None = None
    line_number = 0
    in_hunk = False
    for line in lines:
        if line.startswith("COMMIT "):
            match = COMMIT_RE.match(line)
            if match:
                current_sha = match.group(1)
                current_subject = (match.group(2) or "").strip()
            current_path = None
            in_hunk = False
            continue
        if line.startswith("diff --git "):
            current_path = None
            in_hunk = False
            continue
        if line.startswith("Binary files "):
            current_path = None
            in_hunk = False
            continue
        if not in_hunk and line.startswith("+++ "):
            current_path = parse_path(line[4:])
            in_hunk = False
            continue
        match = HUNK_RE.match(line)
        if match:
            line_number = int(match.group(1))
            in_hunk = True
            continue
        if line.startswith("+") and in_hunk:
            if current_path is not None and not is_excluded(current_path, excludes, config_rel):
                hits.extend(
                    _line_hits(
                        line[1:], rules, current_sha, current_subject, current_path, line_number
                    )
                )
            line_number += 1
    return hits


def print_report(hits: list[Hit], *, pr_mode: bool) -> None:
    heading = (
        "PR 词检：新增行命中黑名单词。"
        if pr_mode
        else "pre-push 词检：本次推送的新增行命中黑名单词，push 已打回。"
    )
    out = [heading, ""]
    grouped: dict = {}
    for hit in hits:
        grouped.setdefault((hit.sha, hit.subject), []).append(hit)
    for (sha, subject), group in grouped.items():
        if sha:
            out.append(f"  {sha[:12]} {subject}")
        for hit in group:
            out.append(f"    {hit.path}:{hit.line}  命中「{hit.word}」→ {hit.suggestion}")
    out.append("")
    out.append(f"共 {len(hits)} 处命中。请按建议修改后重新提交并推送。")
    out.append("词表：scripts/ai-words.txt（可增删条目；`!` 前缀行可豁免路径）。")
    logger.error("%s", "\n".join(out))


def main(argv: list[str]) -> int:
    _configure_logging()
    try:
        return _run(argv)
    except CheckFailureError as failure:
        return failure.exit_code


def _run(argv: list[str]) -> int:
    config_path = Path(__file__).resolve().parent / CONFIG_NAME
    pr_mode = len(argv) == 4 and argv[1] == "--pr"
    if len(argv) == 3 and argv[1] == "--config":
        config_path = Path(argv[2])
    elif len(argv) == 2 and argv[1].startswith("--config="):
        config_path = Path(argv[1].split("=", 1)[1])
    elif len(argv) != 1 and not pr_mode:
        fail("用法: check_ai_words.py [--config PATH | --pr BASE HEAD]")
    if not config_path.is_file():
        fail(f"词表缺失: {config_path}", EXIT_BAD_CONFIG)
    try:
        rules, excludes = load_rules(config_path)
    except ConfigError as error:
        fail(str(error), EXIT_BAD_CONFIG)
    if not rules:
        return 0
    if pr_mode:
        base, head = argv[2:]
        patches = [["-c", "core.quotepath=false", "diff", *PATCH_OPTIONS, f"{base}...{head}", "--"]]
    else:
        refs = [line.strip() for line in sys.stdin if line.strip()]
        patches = [_log_args(expr) for expr in ref_expressions(refs)]
    hits: list[Hit] = []
    config_rel = repo_relative(config_path)
    for args in patches:
        hits.extend(scan_patch(git_lines(*args), rules, excludes, config_rel))
    seen = set()
    unique_hits: list[Hit] = []
    for hit in hits:
        key = (hit.sha, hit.path, hit.line, hit.word)
        if key not in seen:
            seen.add(key)
            unique_hits.append(hit)
    if unique_hits:
        print_report(unique_hits, pr_mode=pr_mode)
        return EXIT_BLOCKED
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
