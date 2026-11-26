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

"""Bootstrap config generation — pure functions, no UI / no click.

Used by:
- ``workflow-controller init`` CLI (legacy, will be removed once SetupWizardScreen lands)
- ``SetupWizardScreen`` Save handler (Phase 3)
- Any other entry point that needs to drop a starting ``workflow-controller.yaml`` + ``.env``

Design choices:
- ``ConfigFields`` and ``EnvFields`` are dataclasses with sensible defaults so
  callers only fill the fields they actually have.
- ``write_yaml`` emits via ``yaml.dump(dict)`` rather than string templating —
  keeps output schema-compliant with ``AppConfig`` and avoids quoting/escaping
  bugs on paths with backslashes (Windows).
- All file ops raise ``FileExistsError`` unless ``overwrite=True``. Caller
  decides whether to clobber.
- Files are always written ``encoding="utf-8"`` (matches the rest of the codebase
  and the TODO #13 fix for HookConfigGenerator).
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

DEFAULT_TASK_GOAL = """\
完成既定任务：按目标在工作目录中生成/补全代码与产物，
并确保可验证。
在任务全部完成前永远不要选"结束本轮/输出报告/收尾"类选项；
优先选"继续/推进下一步"。"""

DEFAULT_STARTING_PROMPT = "请阅读工作目录中的项目说明与目标，按既定计划推进任务。"


@dataclass
class ConfigFields:
    """All knobs that go into ``workflow-controller.yaml``. Every field is optional."""

    # ── Paths ──────────────────────────────────────────────────────────
    project_dir: str = "."
    skills_path: str = ""
    agents_path: str = ""

    # ── Reviewer ──────────────────────────────────────────────────────
    reviewer_enabled: bool = True
    reviewer_model: str = "claude-opus-4-6"
    reviewer_task_goal: str = DEFAULT_TASK_GOAL

    # ── Hook server ───────────────────────────────────────────────────
    hook_server_enabled: bool = True

    # ── Starting prompt ───────────────────────────────────────────────
    starting_prompt: str = DEFAULT_STARTING_PROMPT


@dataclass
class EnvFields:
    """API keys that go into ``.env`` (not yaml)."""

    deepseek_api_key: str = ""
    anthropic_api_key: str = ""


def build_yaml_dict(fields: ConfigFields) -> dict[str, Any]:
    """Serialize ``ConfigFields`` into a dict ready for ``yaml.dump``.

    Kept separate from ``write_yaml`` so callers (e.g. a preview pane) can
    inspect the rendered structure without writing to disk.
    """
    out: dict[str, Any] = {
        "project_dir": fields.project_dir,
    }
    if fields.skills_path:
        out["skills_path"] = fields.skills_path
    if fields.agents_path:
        out["agents_path"] = fields.agents_path
    if fields.starting_prompt:
        out["starting_prompt"] = fields.starting_prompt

    out["reviewer"] = {
        "enabled": fields.reviewer_enabled,
        "model": fields.reviewer_model,
        "task_goal": fields.reviewer_task_goal,
    }

    out["hook_server"] = {
        "enabled": fields.hook_server_enabled,
    }

    return out


def write_yaml(
    path: Path,
    fields: ConfigFields,
    *,
    overwrite: bool = False,
) -> Path:
    """Render fields into yaml and write to ``path``.

    Raises ``FileExistsError`` if path exists and ``overwrite=False``.
    Always UTF-8.
    """
    if path.exists() and not overwrite:
        raise FileExistsError(
            f"{path} already exists; pass overwrite=True to replace",
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    data = build_yaml_dict(fields)
    serialized = yaml.dump(
        data,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )
    path.write_text(serialized, encoding="utf-8")
    return path


def write_env(
    path: Path,
    fields: EnvFields,
    *,
    overwrite: bool = False,
    merge: bool = False,
) -> Path:
    """Write ``.env`` with API keys.

    Args:
        merge: If True and ``path`` exists, preserve existing keys; only
            override with non-empty new values. Useful when re-running the
            setup wizard so user-added custom keys aren't wiped.
    """
    if path.exists() and not overwrite and not merge:
        raise FileExistsError(
            f"{path} already exists; pass overwrite=True or merge=True",
        )

    existing: dict[str, str] = {}
    if merge and path.exists():
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            existing[k.strip()] = v.strip()

    new_values = {
        "DEEPSEEK_API_KEY": fields.deepseek_api_key,
        "ANTHROPIC_API_KEY": fields.anthropic_api_key,
    }

    out_keys: dict[str, str] = dict(existing)
    for k, v in new_values.items():
        if merge:
            if v:
                out_keys[k] = v
            elif k not in out_keys:
                out_keys[k] = ""
        else:
            out_keys[k] = v

    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(f"{k}={v}" for k, v in out_keys.items()) + "\n"
    path.write_text(body, encoding="utf-8")
    return path


def write_initial_config(
    target_dir: Path,
    config_fields: ConfigFields | None = None,
    env_fields: EnvFields | None = None,
    *,
    overwrite: bool = False,
    config_filename: str = "workflow-controller-config.yaml",
    env_filename: str = ".env",
) -> tuple[Path, Path]:
    """Drop a starting ``workflow-controller.yaml`` + ``.env`` into ``target_dir``.

    Returns ``(yaml_path, env_path)``.
    """
    target_dir.mkdir(parents=True, exist_ok=True)

    yaml_path = write_yaml(
        target_dir / config_filename,
        config_fields or ConfigFields(),
        overwrite=overwrite,
    )
    env_path = write_env(
        target_dir / env_filename,
        env_fields or EnvFields(),
        overwrite=overwrite,
    )
    return yaml_path, env_path


def copy_overlay(src: Path, dst: Path) -> int:
    """Overlay-copy every file under ``src`` into ``dst``.

    Semantics:
    - Same-named files in ``dst`` are **overwritten** by ``src`` versions.
    - Files in ``dst`` that don't exist in ``src`` are **preserved**.
    - Sub-directories are created as needed in ``dst``.

    Returns the count of files copied. Raises ``FileNotFoundError`` if
    ``src`` doesn't exist.

    Use case: user has a curated ``<skills_path>/`` and wants to sync it
    into the target project's ``.claude/skills/`` without nuking any local
    edits the worker made (e.g. memory files generated mid-run).
    """
    if not src.exists():
        raise FileNotFoundError(f"source dir not found: {src}")
    if not src.is_dir():
        raise NotADirectoryError(f"source is not a directory: {src}")

    dst.mkdir(parents=True, exist_ok=True)
    count = 0
    for src_file in src.rglob("*"):
        if not src_file.is_file():
            continue
        rel = src_file.relative_to(src)
        out = dst / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_file, out)
        count += 1
    return count


# worker runtime → 其原生 skill/agent 发现目录名（CC 扫 <cwd>/.claude/、opencode 扫 <cwd>/.opencode/）。
# session 启动时按此把配置的 skills_path/agents_path overlay-sync 进去，字段才真正生效。
_RUNTIME_DISCOVERY_ROOT: dict[str, str] = {
    "claude_sdk": ".claude",
    "claude_cli": ".claude",
    "opencode": ".opencode",
}


def sync_worker_discovery_dirs(
    *,
    project_dir: Path,
    runtime: str,
    skills_path: str = "",
    agents_path: str = "",
) -> dict[str, int]:
    """把配置的 skills/agents 源 overlay-sync 进 worker runtime 的原生发现目录。

    两个 runtime 都按**目录约定**发现 skill/agent（CC 扫 ``<cwd>/.claude/{skills,agents}/``、
    opencode 扫 ``<cwd>/.opencode/{skills,agents}/``），源 ``.md``（YAML frontmatter + body）两边
    直接 drop-in——所以是纯 overlay 拷贝（复用 :func:`copy_overlay`：保留 dst-only 文件、同名覆盖）。
    空路径或源不存在 → 静默跳过；未知 runtime → no-op + warning。

    Returns ``{dst_dir: files_copied}``（只含真正跑了的同步），便于测试与日志。
    """
    root_name = _RUNTIME_DISCOVERY_ROOT.get(runtime)
    if root_name is None:
        logger.warning(
            "sync_worker_discovery_dirs: unknown runtime %r, skipping skill/agent sync",
            runtime,
        )
        return {}

    synced: dict[str, int] = {}
    root = Path(project_dir) / root_name
    for src_path, sub in ((skills_path, "skills"), (agents_path, "agents")):
        if not src_path:
            continue
        src_dir = Path(src_path)
        if not src_dir.exists():
            logger.warning(
                "sync_worker_discovery_dirs: %s source %s not found, skipping",
                sub,
                src_path,
            )
            continue
        dst = root / sub
        count = copy_overlay(src_dir, dst)
        synced[str(dst)] = count
        logger.info(
            "synced %d %s file(s): %s → %s (runtime=%s)",
            count,
            sub,
            src_path,
            dst,
            runtime,
        )
    return synced
