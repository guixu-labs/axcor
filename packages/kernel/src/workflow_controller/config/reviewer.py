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

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from workflow_controller.config.settings import ReviewerConfig, ThinkingMode

from workflow_controller.config.settings import MIN_REVIEWER_MAX_TOKENS


@dataclass(frozen=True)
class ReviewerThinkingOverride:
    mode: ThinkingMode | None = None
    budget_tokens: int | None = None
    max_tokens: int | None = None


@dataclass(frozen=True)
class ReviewerThinkingValues:
    mode: ThinkingMode = "disabled"
    budget_tokens: int = 2048
    max_tokens: int = MIN_REVIEWER_MAX_TOKENS


def explicit_project_reviewer_thinking(
    project_dir: Path | str,
    raw: dict[str, Any] | None = None,
) -> ReviewerThinkingOverride:
    if raw is None:
        yaml_path = Path(project_dir) / "workflow-controller.yaml"
        if not yaml_path.is_file():
            return ReviewerThinkingOverride()
        try:
            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        except Exception:
            return ReviewerThinkingOverride()
    reviewer = raw.get("reviewer") if isinstance(raw, dict) else None
    if not isinstance(reviewer, dict):
        return ReviewerThinkingOverride()

    mode = reviewer.get("thinking_mode")
    budget = reviewer.get("thinking_budget_tokens")
    max_tokens = reviewer.get("max_tokens")
    try:
        budget_tokens = int(budget) if budget is not None else None
    except (TypeError, ValueError):
        budget_tokens = None
    try:
        resolved_max_tokens = int(max_tokens) if max_tokens is not None else None
    except (TypeError, ValueError):
        resolved_max_tokens = None
    resolved_mode: ThinkingMode | None = (
        mode
        if mode
        in (
            "disabled",
            "enabled",
            "adaptive",
            "omitted",
        )
        else None
    )
    if resolved_mode is None:
        enabled = reviewer.get("thinking_enabled")
        if isinstance(enabled, bool):
            resolved_mode = "enabled" if enabled else "disabled"
    return ReviewerThinkingOverride(
        mode=resolved_mode,
        budget_tokens=budget_tokens,
        max_tokens=resolved_max_tokens,
    )


def resolve_reviewer_thinking(
    project_dir: Path | str,
    global_reviewer: ReviewerConfig,
    *,
    budget_override: int | None = None,
    max_tokens_override: int | None = None,
    mode_override: ThinkingMode | str | None = None,
) -> ReviewerThinkingValues:
    project = explicit_project_reviewer_thinking(project_dir)
    budget = budget_override if budget_override is not None else project.budget_tokens
    if budget is None:
        budget = global_reviewer.thinking_budget_tokens
    try:
        budget = int(budget)
    except (TypeError, ValueError):
        budget = 2048

    resolved_max_tokens = (
        max_tokens_override if max_tokens_override is not None else project.max_tokens
    )
    if resolved_max_tokens is None:
        resolved_max_tokens = global_reviewer.max_tokens
    try:
        resolved_max_tokens = int(resolved_max_tokens)
    except (TypeError, ValueError):
        resolved_max_tokens = MIN_REVIEWER_MAX_TOKENS
    resolved_max_tokens = max(MIN_REVIEWER_MAX_TOKENS, resolved_max_tokens)

    mode: ThinkingMode
    if mode_override in ("disabled", "enabled", "adaptive", "omitted"):
        mode = mode_override
    elif project.mode is not None:
        mode = project.mode
    elif global_reviewer.thinking_mode:
        mode = global_reviewer.thinking_mode
    else:
        mode = "disabled"
    return ReviewerThinkingValues(
        mode=mode,
        budget_tokens=budget,
        max_tokens=resolved_max_tokens,
    )
