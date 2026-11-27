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

import pytest

from workflow_controller.config.reviewer import (
    explicit_project_reviewer_thinking,
    resolve_reviewer_thinking,
)
from workflow_controller.config.settings import ReviewerConfig, SessionReviewerConfig


def test_missing_project_fields_inherit_global(tmp_path):
    (tmp_path / "workflow-controller.yaml").write_text(
        "reviewer: {model: test}\n", encoding="utf-8"
    )
    global_config = ReviewerConfig(
        thinking_mode="enabled",
        thinking_budget_tokens=4096,
        max_tokens=8192,
    )
    assert explicit_project_reviewer_thinking(tmp_path).mode is None
    resolved = resolve_reviewer_thinking(tmp_path, global_config)
    assert (resolved.mode, resolved.budget_tokens, resolved.max_tokens) == (
        "enabled",
        4096,
        8192,
    )


@pytest.mark.parametrize(
    ("legacy_enabled", "expected_mode"),
    [(True, "enabled"), (False, "disabled")],
)
def test_legacy_boolean_thinking_preserves_wire_semantics(
    tmp_path,
    legacy_enabled,
    expected_mode,
):
    (tmp_path / "workflow-controller.yaml").write_text(
        f"reviewer: {{thinking_enabled: {str(legacy_enabled).lower()}, "
        "thinking_budget_tokens: 3072}\n",
        encoding="utf-8",
    )
    global_config = ReviewerConfig(thinking_mode="adaptive")

    assert explicit_project_reviewer_thinking(tmp_path).mode == expected_mode
    assert resolve_reviewer_thinking(tmp_path, global_config).mode == expected_mode
    assert ReviewerConfig(thinking_enabled=legacy_enabled).thinking_mode == expected_mode
    assert SessionReviewerConfig(thinking_enabled=legacy_enabled).thinking_mode == (expected_mode)


def test_missing_legacy_configuration_defaults_to_disabled(tmp_path):
    assert resolve_reviewer_thinking(tmp_path, ReviewerConfig()).mode == "disabled"
    assert SessionReviewerConfig().thinking_mode == "disabled"


def test_request_overrides_project_and_explicit_false(tmp_path):
    (tmp_path / "workflow-controller.yaml").write_text(
        "reviewer: {thinking_mode: disabled, thinking_budget_tokens: 3072}\n",
        encoding="utf-8",
    )
    global_config = ReviewerConfig(
        thinking_mode="enabled",
        thinking_budget_tokens=4096,
        max_tokens=8192,
    )
    project = resolve_reviewer_thinking(tmp_path, global_config)
    assert (project.mode, project.budget_tokens, project.max_tokens) == (
        "disabled",
        3072,
        8192,
    )
    request = resolve_reviewer_thinking(
        tmp_path,
        global_config,
        mode_override="enabled",
        budget_override=8192,
        max_tokens_override=16384,
    )
    assert (request.mode, request.budget_tokens, request.max_tokens) == (
        "enabled",
        8192,
        16384,
    )


def test_max_tokens_out_of_range_is_clamped(tmp_path):
    resolved = resolve_reviewer_thinking(
        tmp_path,
        ReviewerConfig(),
        max_tokens_override=512,
    )
    assert resolved.max_tokens == 2048


def test_low_max_tokens_is_loaded_and_clamped_at_resolution(tmp_path):
    global_reviewer = ReviewerConfig(max_tokens=512)
    assert global_reviewer.max_tokens == 512
    assert SessionReviewerConfig(max_tokens=512).max_tokens == 512
    assert resolve_reviewer_thinking(tmp_path, global_reviewer).max_tokens == 2048


def test_string_overrides_are_not_coerced_as_truthy(tmp_path):
    (tmp_path / "workflow-controller.yaml").write_text(
        "reviewer: {thinking_mode: enabled, thinking_budget_tokens: 3072}\n",
        encoding="utf-8",
    )
    global_config = ReviewerConfig(thinking_mode="disabled", thinking_budget_tokens=4096)
    # 未知 mode 与非法 budget 都收敛到安全默认值，不让配置错误进入运行期。
    resolved = resolve_reviewer_thinking(
        tmp_path,
        global_config,
        mode_override="false",
        budget_override="abc",
    )
    assert resolved.mode == "enabled"
    assert resolved.budget_tokens == 2048
