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

"""Claude CLI checkpoint replay must preserve only the pre-checkpoint dialogue."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from workflow_controller.plugins.agent_traceback.claude_session_seed import (
    _project_key,
    detect_replay_runtime,
    seed_claude_session,
)


def _dialogue_row(kind: str, cwd: Path, when: datetime, message) -> dict:
    """origin transcript 单行（user / assistant 对话记录）。"""
    return {
        "type": kind,
        "sessionId": "origin-uuid",
        "cwd": str(cwd),
        "timestamp": when.isoformat(),
        "message": {"content": message},
    }


def _write_origin_transcript(
    config: Path, cwd: Path, now: datetime, user_age_s: int, agent_age_s: int
) -> None:
    """写 origin transcript：user 请求 → Agent 派发 → 检查点后的未来结果。"""
    transcript = config / "projects" / _project_key(str(cwd)) / "origin-uuid.jsonl"
    transcript.parent.mkdir(parents=True)
    rows = [
        _dialogue_row("user", cwd, now - timedelta(seconds=user_age_s), "build app"),
        _dialogue_row(
            "assistant",
            cwd,
            now - timedelta(seconds=agent_age_s),
            [
                {
                    "type": "tool_use",
                    "name": "Agent",
                    "input": {"subagent_type": "developer"},
                }
            ],
        ),
        _dialogue_row("user", cwd, now, "future result"),
    ]
    transcript.write_text("".join(json.dumps(row) + "\n" for row in rows))
    (config / "settings.json").write_text('{"theme":"dark"}')


def _source(tmp_path: Path, *, user_age_s: int = 4, agent_age_s: int = 1) -> tuple[Path, Path, str]:
    """标准 origin 环境：claude-home transcript + project cwd + 检查点时刻。"""
    config = tmp_path / "claude-home"
    cwd = tmp_path / "project"
    cwd.mkdir()
    now = datetime.now(tz=UTC).astimezone()
    _write_origin_transcript(config, cwd, now, user_age_s, agent_age_s)
    checkpoint_at = (now + timedelta(milliseconds=100)).replace(tzinfo=None).isoformat()
    return config, cwd, checkpoint_at


def _origin_transcript_path(config: Path, cwd: Path) -> Path:
    """origin transcript 文件路径（读取 / 改写用）。"""
    return config / "projects" / _project_key(str(cwd)) / "origin-uuid.jsonl"


def _seed_fork_call(
    tmp_path: Path,
    config: Path,
    cwd: Path,
    checkpoint_at: str,
    agent_type: str,
    final_checkpoint: bool,
):
    """seed_claude_session 的统一封装（fork cwd 固定 tmp/fork，新 sdk id fork-uuid）。"""
    fork_cwd = tmp_path / "fork"
    fork_cwd.mkdir()
    fork_config, error = seed_claude_session(
        source_session_id="origin-uuid",
        source_cwd=str(cwd),
        new_session_id="fork-uuid",
        fork_cwd=fork_cwd,
        checkpoint_at=checkpoint_at,
        agent_type=agent_type,
        final_checkpoint=final_checkpoint,
        source_config_dir=str(config),
    )
    return fork_config, error, fork_cwd


def _fork_before_agent(tmp_path, config, cwd, checkpoint_at, agent_type):
    """final_checkpoint=False：重放到 Agent 派发前（只保留前置对话）。"""
    return _seed_fork_call(tmp_path, config, cwd, checkpoint_at, agent_type, False)


def _fork_with_history(tmp_path, config, cwd, checkpoint_at, agent_type="main"):
    """final_checkpoint=True：终态检查点，保留全部历史。"""
    return _seed_fork_call(tmp_path, config, cwd, checkpoint_at, agent_type, True)


def _checkpoint_slightly_ahead() -> str:
    """比当前时刻晚 1 秒的检查点时刻（origin transcript 全部行都在其之前）。"""
    return (
        (datetime.now(tz=UTC).astimezone() + timedelta(seconds=1)).replace(tzinfo=None).isoformat()
    )


def test_project_key_matches_claude_cli_for_underscores(tmp_path):
    cwd = tmp_path / "segment_with_underscore" / "cwd"
    assert "_" not in _project_key(str(cwd))
    assert _project_key(str(cwd)).endswith("segment-with-underscore-cwd")


def test_detect_replay_runtime_prefers_source_evidence(tmp_path):
    config, cwd, _ = _source(tmp_path)
    base = dict(
        cwd=str(cwd),
        configured_runtime=None,
        default_runtime="claude_sdk",
        source_config_dir=str(config),
    )
    assert detect_replay_runtime(native_session_id="origin-uuid", **base) == "claude_cli"
    assert detect_replay_runtime(native_session_id="ses_abc", **base) == "opencode"
    assert (
        detect_replay_runtime(
            native_session_id="ses_abc",
            **{**base, "configured_runtime": "claude_cli"},
        )
        == "claude_cli"
    )


def test_detect_replay_runtime_falls_back_to_default_when_no_transcript(tmp_path):
    cwd = tmp_path / "project"
    cwd.mkdir()
    config = tmp_path / "claude-home"
    config.mkdir()
    base = dict(cwd=str(cwd), configured_runtime=None, source_config_dir=str(config))
    assert (
        detect_replay_runtime(
            native_session_id="origin-uuid",
            **{**base, "default_runtime": "claude_sdk"},
        )
        == "claude_sdk"
    )
    assert (
        detect_replay_runtime(
            native_session_id="origin-uuid",
            **{**base, "default_runtime": "claude_cli"},
        )
        == "claude_cli"
    )


def test_seed_subagent_checkpoint_excludes_agent_call_and_future(tmp_path):
    config, cwd, checkpoint_at = _source(tmp_path)
    fork_config, error, fork_cwd = _fork_before_agent(
        tmp_path, config, cwd, checkpoint_at, "developer"
    )
    assert error == ""
    assert fork_config == fork_cwd / ".workflow-controller" / "claude"
    target = fork_config / "projects" / _project_key(str(fork_cwd)) / "fork-uuid.jsonl"
    rows = [json.loads(line) for line in target.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["sessionId"] == "fork-uuid"
    assert rows[0]["cwd"] == str(fork_cwd)
    assert (fork_config / "settings.json").read_text() == '{"theme":"dark"}'
    assert len(_origin_transcript_path(config, cwd).read_text().splitlines()) == 3


def test_seed_final_checkpoint_keeps_history(tmp_path):
    config, cwd, checkpoint_at = _source(tmp_path)
    fork_config, error, fork_cwd = _fork_with_history(tmp_path, config, cwd, checkpoint_at, "main")
    assert error == ""
    target = fork_config / "projects" / _project_key(str(fork_cwd)) / "fork-uuid.jsonl"
    assert len(target.read_text().splitlines()) == 3


def test_seed_rejects_missing_agent_call(tmp_path):
    config, cwd, checkpoint_at = _source(tmp_path)
    fork_config, error, _fork_cwd = _fork_before_agent(
        tmp_path, config, cwd, checkpoint_at, "tester"
    )
    assert fork_config is None
    assert "Agent call for 'tester' was not found" in error


def test_seed_matches_agent_call_delayed_by_snapshot(tmp_path):
    # 检查点 created_at 在 snapshot 完成后才写入，可能晚于 Agent 调用远超 5s；
    # 匹配窗口须容纳快照耗时，否则大工作树上的检查点不可重放。
    config, cwd, checkpoint_at = _source(tmp_path, user_age_s=50, agent_age_s=40)
    fork_config, error, fork_cwd = _fork_before_agent(
        tmp_path, config, cwd, checkpoint_at, "developer"
    )
    assert error == ""
    target = fork_config / "projects" / _project_key(str(fork_cwd)) / "fork-uuid.jsonl"
    assert len(target.read_text().splitlines()) == 1


def test_seed_accepts_task_tool_name_and_mode_key(tmp_path):
    # 历史 CLI 版本用 `Task` 工具名 + `mode` 键派发（与 context_bay_exporter 兼容一致），
    # 严格匹配 `Agent`+`subagent_type` 会让这类检查点重放全部失败。
    config, cwd, checkpoint_at = _source(tmp_path)
    transcript = _origin_transcript_path(config, cwd)
    rows = [json.loads(line) for line in transcript.read_text().splitlines()]
    rows[1]["message"]["content"] = [
        {"type": "tool_use", "name": "Task", "input": {"mode": "developer"}},
    ]
    transcript.write_text("".join(json.dumps(row) + "\n" for row in rows))
    fork_config, error, fork_cwd = _fork_before_agent(
        tmp_path, config, cwd, checkpoint_at, "developer"
    )
    assert error == ""
    target = fork_config / "projects" / _project_key(str(fork_cwd)) / "fork-uuid.jsonl"
    assert len(target.read_text().splitlines()) == 1


def test_seed_skips_trailing_partial_line(tmp_path):
    # CLI 崩溃留下的半行尾部记录不得让整条 transcript 不可重放。
    config, cwd, _checkpoint_at = _source(tmp_path)
    transcript = _origin_transcript_path(config, cwd)
    with transcript.open("a") as f:
        f.write('{"type": "user", "sessionId": "origin-uuid", "cwd": "trunc')
    fork_config, error, fork_cwd = _fork_with_history(
        tmp_path, config, cwd, _checkpoint_slightly_ahead(), "main"
    )
    assert error == ""
    target = fork_config / "projects" / _project_key(str(fork_cwd)) / "fork-uuid.jsonl"
    assert len(target.read_text().splitlines()) == 3


def test_seed_skips_interior_blank_lines(tmp_path):
    # 某些工具会写空行分隔；空行不应被当成损坏行而静默截断重放历史。
    config, cwd, _checkpoint_at = _source(tmp_path)
    transcript = _origin_transcript_path(config, cwd)
    rows = [json.loads(line) for line in transcript.read_text().splitlines()]
    transcript.write_text(
        "".join(json.dumps(row) + "\n" for row in rows[:1])
        + "\n"
        + "".join(json.dumps(row) + "\n" for row in rows[1:]),
    )
    fork_config, error, fork_cwd = _fork_with_history(
        tmp_path, config, cwd, _checkpoint_slightly_ahead(), "main"
    )
    assert error == ""
    target = fork_config / "projects" / _project_key(str(fork_cwd)) / "fork-uuid.jsonl"
    out_rows = [line for line in target.read_text().split("\n") if line.strip()]
    assert len(out_rows) == 3


def test_seed_preserves_line_separator_chars_in_json_string(tmp_path):
    # U+2028 / U+2029 在 JSON 字符串值内可不转义出现（Node 的 JSON.stringify 不转义），
    # str.splitlines() 会把它们当行界切分，静默截断 transcript；按 "\n" 切分则不受影响。
    config, cwd, _checkpoint_at = _source(tmp_path)
    transcript = _origin_transcript_path(config, cwd)
    rows = [json.loads(line) for line in transcript.read_text().splitlines()]
    rows[2]["message"]["content"] = "past\u2028result\u2029end"
    transcript.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
    )
    fork_config, error, fork_cwd = _fork_with_history(
        tmp_path, config, cwd, _checkpoint_slightly_ahead(), "main"
    )
    assert error == ""
    target = fork_config / "projects" / _project_key(str(fork_cwd)) / "fork-uuid.jsonl"
    out_rows = [json.loads(line) for line in target.read_text().split("\n") if line.strip()]
    assert len(out_rows) == 3
    assert out_rows[2]["message"]["content"] == "past\u2028result\u2029end"
