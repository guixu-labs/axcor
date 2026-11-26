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

"""claims 软预检测试：声明相交判定、出生期拒绝与登记生命周期。"""

from __future__ import annotations

from pathlib import Path

import pytest

from workflow_controller.core.workspace.claims import (
    ClaimRejectedError,
    ClaimsRegistry,
    claims_overlap,
)

from ._support import FakeOrchestrator, build_workspace


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        ("CODE_REVIEW.md", "CODE_REVIEW.md", True),
        ("CODE_REVIEW.md", "docs/other.md", False),
        ("op/op_kernel/**", "op/op_kernel/autogen/x.py", True),
        ("op/op_kernel/**", "op/build/autogen/**", False),
        ("op/op_kernel/**", "op/op_kernel/**", True),
        ("design/*.md", "design/sub/**", False),
        ("*.md", "*.txt", False),
        ("**/*.log", "**/__pycache__/**", False),
        ("design", "design/notes.txt", True),
    ],
)
def test_claims_overlap(left: str, right: str, expected: bool) -> None:
    """声明相交判定：字面互含、桶嵌套相交，跨桶与异后缀不相交。"""
    assert claims_overlap(left, right) is expected
    assert claims_overlap(right, left) is expected


def test_registry_conflicts_exclude_self_key() -> None:
    """登记表求交：自身键不计（废弃重建场景），他键按相交聚合。"""
    registry = ClaimsRegistry()
    registry.register("node-a", ("op/src/**", "docs/**"))
    registry.register("node-b", ("op/build/**",))

    # 自身键的既有登记不计：node-a 与自己、与不相交的 node-b 都不冲突
    assert registry.conflicts_for("node-a", ("op/src/gen/**", "op/other/**")) == ()
    # node-b 的声明与 node-a 的 op/src/** 相交
    assert registry.conflicts_for("node-b", ("op/src/**", "op/other/**")) == (
        ("node-a", ("op/src/**",)),
    )
    registry.release("node-a")
    assert registry.conflicts_for("node-b", ("op/src/**", "docs/**")) == ()


async def test_enforce_rejects_second_birth_on_overlapping_claims(tmp_path: Path) -> None:
    """强制档：出生期声明相交即拒绝第二个单元，先到者不受影响。"""
    _root, _repository, manager = build_workspace(tmp_path, claims_enforce=True)
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared = await orchestrator.prepare("node-a", claims=("op/op_kernel/**",))
    assert prepared.scope_id == "node-a"

    with pytest.raises(ClaimRejectedError) as caught:
        await orchestrator.prepare("node-b", claims=("op/op_kernel/autogen/**",))
    assert caught.value.conflicts == (("node-a", ("op/op_kernel/autogen/**",)),)


async def test_default_tier_registers_without_rejection(tmp_path: Path) -> None:
    """默认档：声明相交只登记不拒绝，冲突判定留待集成期兜底。"""
    _root, _repository, manager = build_workspace(tmp_path)
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared_a = await orchestrator.prepare("node-a", claims=("op/op_kernel/**",))
    prepared_b = await orchestrator.prepare("node-b", claims=("op/op_kernel/**",))
    assert prepared_a.scope_id != prepared_b.scope_id


async def test_claims_released_after_integration(tmp_path: Path) -> None:
    """集成完成后声明释放：后续单元同声明可再出生。"""
    _root, _repository, manager = build_workspace(tmp_path, claims_enforce=True)
    orchestrator = FakeOrchestrator(manager)
    await orchestrator.bootstrap_root()

    prepared = await orchestrator.prepare("node-a", claims=("op/src/**",))
    src_dir = Path(prepared.workspace_path) / "op" / "src"
    src_dir.mkdir(parents=True)
    (src_dir / "a.py").write_text("a\n", encoding="utf-8")
    await orchestrator.integrate("node-a")

    prepared_b = await orchestrator.prepare("node-b", claims=("op/src/**",))
    assert prepared_b.scope_id == "node-b"
