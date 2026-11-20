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

"""scripts/check_ai_words.py 黑盒测试：在临时 git 仓库中模拟 push refs。

测试内不硬编码黑名单词（否则本仓库 push 时会被自己的门禁打回）。
中文词与建议从 scripts/ai-words.txt 读取；英文条目拒绝路径用与词表
无关的占位词经 --config 注入验证。
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

from tests.support import find_upward

REPO_ROOT = find_upward(Path(__file__).resolve().parent, "scripts/check_ai_words.py")
SCRIPT = REPO_ROOT / "scripts" / "check_ai_words.py"
CONFIG = REPO_ROOT / "scripts" / "ai-words.txt"
GIT_EXECUTABLE = shutil.which("git")
assert GIT_EXECUTABLE is not None, "git 必须可用于测试真实提交与差异"

ZERO_SHA = "0" * 40


def load_entries() -> list:
    entries = []
    for line in CONFIG.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("!"):
            continue
        word, suggestion = (part.strip() for part in line.split("=>", 1))
        entries.append((word, suggestion))
    return entries


ENTRIES = load_entries()
assert ENTRIES, "ai-words.txt 需要至少一条条目"
CHINESE_WORD, CHINESE_SUGGESTION = ENTRIES[0]


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        [GIT_EXECUTABLE, "-C", str(repo), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    ).stdout


def make_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.name", "tester")
    git(repo, "config", "user.email", "tester@example.com")
    return repo


def commit(repo: Path, message: str) -> str:
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD").strip()


def run_check(repo: Path, refs: str, *extra_args: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run(
        [sys.executable, str(SCRIPT), *extra_args],
        input=refs,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        cwd=str(repo),
        env=env,
    )


def new_branch_refs(head: str) -> str:
    """remote_sha 全零：新分支首次推送。"""
    return f"refs/heads/main {head} refs/heads/main {ZERO_SHA}\n"


def test_clean_commit_passes(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    docs.mkdir()
    (docs / "notes.md").write_text(
        "本文档描述任务调度流程。\n每个 run 有独立的工作目录。\n", encoding="utf-8"
    )
    head = commit(repo, "docs: add notes")
    result = run_check(repo, new_branch_refs(head))
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""


def test_chinese_word_blocks_with_location_and_suggestion(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    docs.mkdir()
    (docs / "guide.md").write_text(
        "第一行是背景说明。\n" + CHINESE_WORD + "相关流程如下。\n", encoding="utf-8"
    )
    head = commit(repo, "docs: add guide")
    result = run_check(repo, new_branch_refs(head))
    assert result.returncode == 1
    assert "guide.md:2" in result.stderr
    assert CHINESE_WORD in result.stderr
    assert CHINESE_SUGGESTION in result.stderr


def test_pr_scans_only_final_additions_since_merge_base(tmp_path: Path) -> None:
    """目标分支前进和 PR 中间提交不会造成误报。"""
    repo = make_repo(tmp_path)
    existing = repo / "existing.md"
    existing.write_text(CHINESE_WORD + "\n", encoding="utf-8")
    commit(repo, "base commit")
    git(repo, "branch", "feature")
    existing.unlink()
    base = commit(repo, "advance target branch")
    git(repo, "switch", "feature")

    guide = repo / "guide.md"
    guide.write_text("++ " + CHINESE_WORD + "\n", encoding="utf-8")
    head = commit(repo, "add guide")
    result = run_check(repo, "", "--pr", base, head)
    assert result.returncode == 1
    assert "guide.md:1" in result.stderr
    assert CHINESE_SUGGESTION in result.stderr
    assert "existing.md" not in result.stderr

    guide.write_text("本文档描述任务调度流程。\n", encoding="utf-8")
    head = commit(repo, "revise guide")
    result = run_check(repo, "", "--pr", base, head)
    assert result.returncode == 0, result.stderr


def test_ascii_word_rejected(tmp_path: Path) -> None:
    """英文条目暂不支持：配置时报错，而不是按子串误匹配。"""
    config = tmp_path / "words.txt"
    config.write_text("wibble => wobble\n", encoding="utf-8")
    repo = make_repo(tmp_path)
    result = run_check(repo, "", "--config", str(config))
    assert result.returncode == 2
    assert "英文" in result.stderr


def test_remote_range_ignores_already_pushed_commits(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    docs.mkdir()
    (docs / "old.md").write_text(CHINESE_WORD + "出现在已推送的旧提交。\n", encoding="utf-8")
    old_head = commit(repo, "old commit with word")
    (docs / "new.md").write_text("干净的新增内容。\n", encoding="utf-8")
    new_head = commit(repo, "clean new commit")
    refs = f"refs/heads/main {new_head} refs/heads/main {old_head}\n"
    result = run_check(repo, refs)
    assert result.returncode == 0, result.stderr


def test_remote_range_flags_new_commit(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    docs.mkdir()
    (docs / "old.md").write_text("干净的旧提交。\n", encoding="utf-8")
    old_head = commit(repo, "clean old commit")
    (docs / "new.md").write_text(CHINESE_WORD + "出现在新提交。\n", encoding="utf-8")
    new_head = commit(repo, "new commit with word")
    refs = f"refs/heads/main {new_head} refs/heads/main {old_head}\n"
    result = run_check(repo, refs)
    assert result.returncode == 1
    assert "new.md:1" in result.stderr
    assert "old.md" not in result.stderr


def test_delete_and_tag_refs_are_skipped(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    docs.mkdir()
    (docs / "notes.md").write_text("干净的内容。\n", encoding="utf-8")
    head = commit(repo, "docs: add notes")
    refs = (
        f"refs/heads/gone {ZERO_SHA} refs/heads/gone {head}\n"
        f"refs/tags/v1 {head} refs/tags/v1 {ZERO_SHA}\n"
    )
    result = run_check(repo, refs)
    assert result.returncode == 0, result.stderr


def test_malformed_config_exits_2(tmp_path: Path) -> None:
    bad_config = tmp_path / "bad-words.txt"
    bad_config.write_text("缺少分隔符的一行\n", encoding="utf-8")
    repo = make_repo(tmp_path)
    result = run_check(repo, "", "--config", str(bad_config))
    assert result.returncode == 2
    assert "分隔符" in result.stderr


def test_empty_wordlist_passes(tmp_path: Path) -> None:
    empty_config = tmp_path / "empty-words.txt"
    empty_config.write_text("# 只有注释\n", encoding="utf-8")
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    docs.mkdir()
    (docs / "notes.md").write_text(CHINESE_WORD + "在词表为空时不打回。\n", encoding="utf-8")
    head = commit(repo, "docs: add notes")
    result = run_check(repo, new_branch_refs(head), "--config", str(empty_config))
    assert result.returncode == 0, result.stderr


def test_exclusion_pattern_skips_matching_file(tmp_path: Path) -> None:
    config = tmp_path / "words.txt"
    config.write_text(f"{CHINESE_WORD} => 换个说法\n!docs/allowed.md\n", encoding="utf-8")
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    docs.mkdir()
    (docs / "allowed.md").write_text(CHINESE_WORD + "出现在豁免文件。\n", encoding="utf-8")
    (docs / "other.md").write_text(CHINESE_WORD + "出现在未豁免文件。\n", encoding="utf-8")
    head = commit(repo, "docs: two files")
    result = run_check(repo, new_branch_refs(head), "--config", str(config))
    assert result.returncode == 1
    assert "other.md:1" in result.stderr
    assert "allowed.md" not in result.stderr


def test_path_with_spaces_in_report_and_exclusion(tmp_path: Path) -> None:
    """含空格路径：git 在 `+++` 行末尾追加 `\\t` 边界标记，解析须剥掉，
    否则报告带多余 tab、`!` 豁免规则静默失效。
    """
    config = tmp_path / "words.txt"
    config.write_text(f"{CHINESE_WORD} => 换个说法\n!docs/my dir/notes.md\n", encoding="utf-8")
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    (docs / "my dir").mkdir(parents=True)
    (docs / "my dir" / "notes.md").write_text(
        CHINESE_WORD + " 出现在豁免文件。\n", encoding="utf-8"
    )
    (docs / "my dir" / "other.md").write_text(
        CHINESE_WORD + " 出现在未豁免文件。\n", encoding="utf-8"
    )
    head = commit(repo, "docs: files with spaces")
    result = run_check(repo, new_branch_refs(head), "--config", str(config))
    assert result.returncode == 1
    assert "docs/my dir/other.md:1" in result.stderr
    assert "docs/my dir/notes.md" not in result.stderr


def test_tab_in_path_is_quoted_and_decoded(tmp_path: Path) -> None:
    """含 tab 的路径被 git 引号包裹：解析须去掉 b/ 前缀并解码 C 转义，
    报告路径与 `!` 豁免匹配才正确。
    """
    config = tmp_path / "words.txt"
    config.write_text(f"{CHINESE_WORD} => 换个说法\n!docs/we\tird.md\n", encoding="utf-8")
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    docs.mkdir()
    (docs / "we\tird.md").write_text(CHINESE_WORD + " 出现在含 tab 的文件。\n", encoding="utf-8")
    (docs / "plain.md").write_text(CHINESE_WORD + " 出现在普通文件。\n", encoding="utf-8")
    head = commit(repo, "docs: tab file")
    result = run_check(repo, new_branch_refs(head), "--config", str(config))
    assert result.returncode == 1
    assert "b/docs" not in result.stderr  # 不带 b/ 前缀
    assert "docs/we\tird.md" not in result.stderr  # 豁免路径不再被误报
    assert "docs/plain.md:1" in result.stderr


def test_config_file_itself_is_skipped(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    scripts_dir = repo / "scripts"
    scripts_dir.mkdir()
    words_file = scripts_dir / "ai-words.txt"
    words_file.write_text(f"{CHINESE_WORD} => 换个说法\n", encoding="utf-8")
    head = commit(repo, "add word list")
    result = run_check(repo, new_branch_refs(head), "--config", str(words_file))
    assert result.returncode == 0, result.stderr


def test_binary_file_is_skipped(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    docs.mkdir()
    (docs / "ok.md").write_text("干净的文本。\n", encoding="utf-8")
    (repo / "img.bin").write_bytes(b"\x00\x01\x02\x03" * 10)
    head = commit(repo, "add binary asset")
    result = run_check(repo, new_branch_refs(head))
    assert result.returncode == 0, result.stderr


def test_multi_ref_hits_are_deduped(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    docs = repo / "docs"
    docs.mkdir()
    (docs / "guide.md").write_text(CHINESE_WORD + "出现在提交。\n", encoding="utf-8")
    head = commit(repo, "docs: add guide")
    refs = (
        f"refs/heads/main {head} refs/heads/main {ZERO_SHA}\n"
        f"refs/heads/dev {head} refs/heads/dev {ZERO_SHA}\n"
    )
    result = run_check(repo, refs)
    assert result.returncode == 1
    assert result.stderr.count(CHINESE_WORD) == 1  # 同一处命中不因多 ref 重复报告
    assert "共 1 处命中" in result.stderr
