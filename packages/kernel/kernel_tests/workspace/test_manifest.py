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

"""两维路径策略清单测试。"""

from __future__ import annotations

import pytest

from workflow_controller.core.workspace.manifest import (
    DEFAULT_WRITER_LOCAL_PATHSPECS,
    IntegrationPolicy,
    PathPolicy,
    RebuildScript,
    RollbackPolicy,
    WorkspaceManifest,
    WorkspaceManifestError,
)


def test_default_policy_is_merge_with_checkpoint() -> None:
    """未声明路径默认进集成线并随检查点流动。"""
    manifest = WorkspaceManifest(writer_local=())

    assert manifest.classify_path("design/a.md") == PathPolicy(
        IntegrationPolicy.MERGE, RollbackPolicy.CHECKPOINTED
    )


def test_default_writer_local_buckets_match_at_any_depth() -> None:
    """默认自建声明在顶层与嵌套目录都命中，含字面目录前缀形态。"""
    manifest = WorkspaceManifest(writer_local=(*DEFAULT_WRITER_LOCAL_PATHSPECS, "dist"))

    assert manifest.classify_path("run.log").rollback is RollbackPolicy.CHECKPOINTED
    assert manifest.classify_path("sub/deep/x.log").integration is IntegrationPolicy.WRITER_LOCAL
    assert manifest.classify_path("op/build/out.bin").integration is IntegrationPolicy.WRITER_LOCAL
    assert manifest.classify_path("build/top.bin").integration is IntegrationPolicy.WRITER_LOCAL
    assert manifest.classify_path("dist/pack.tgz").integration is IntegrationPolicy.WRITER_LOCAL
    assert manifest.classify_path("op/src/main.py").integration is IntegrationPolicy.MERGE


def test_rebuild_script_upgrades_path_to_rebuildable() -> None:
    """给出确定性脚本的自建路径按可重建处理，不受桶声明形态限制。"""
    manifest = WorkspaceManifest(
        rebuild_scripts={"op/build/**": RebuildScript(cmd="make build", verify="make check")}
    )

    assert manifest.classify_path("op/build/out.bin") == PathPolicy(
        IntegrationPolicy.WRITER_LOCAL, RollbackPolicy.REBUILDABLE
    )
    assert manifest.rebuildable_pathspecs == ("op/build/**",)
    specs = manifest.git_materialize_specs()
    assert ":(glob)**/build/**" in specs
    assert ":(exclude,glob)op/build/**" in specs


def test_incomplete_rebuild_script_falls_back_to_checkpointed() -> None:
    """脚本或验收缺失的重建声明按铁律降级为随检查点。"""
    manifest = WorkspaceManifest(
        rebuild_scripts={
            "op/build/**": RebuildScript(cmd="make build", verify=""),
            "cache/**": RebuildScript(cmd="", verify="make check"),
        }
    )

    assert manifest.rebuild_scripts == {}
    assert manifest.classify_path("op/build/out.bin").rollback is RollbackPolicy.CHECKPOINTED
    assert manifest.classify_path("cache/blob").rollback is RollbackPolicy.CHECKPOINTED


def test_shared_and_external_paths_never_enter_line_or_checkpoint() -> None:
    """共享类与外部路径：不进集成线、不进检查点。"""
    manifest = WorkspaceManifest(
        writer_local=(),
        shared_readonly=("third_party/**",),
        shared_exclusive=("device/socket",),
        external=("vendor/**",),
    )

    assert manifest.classify_path("third_party/lib.h") == PathPolicy(
        IntegrationPolicy.SHARED_READONLY, RollbackPolicy.EXTERNAL
    )
    assert manifest.classify_path("device/socket") == PathPolicy(
        IntegrationPolicy.SHARED_EXCLUSIVE, RollbackPolicy.EXTERNAL
    )
    assert manifest.classify_path("vendor/pkg/data.bin").rollback is RollbackPolicy.EXTERNAL
    assert manifest.git_scope_commit_exclude_specs() == (
        ":(exclude,glob)third_party/**",
        ":(exclude,glob)device/socket",
        ":(exclude,glob)vendor/**",
    )


def test_diff_restriction_excludes_every_non_merge_bucket() -> None:
    """集成 diff 限定规格覆盖除进线路径外的全部桶。"""
    manifest = WorkspaceManifest(external=("vendor/**",))

    restriction = manifest.git_diff_restriction_specs()

    assert restriction[0] == "."
    assert ":(exclude,glob)**/build/**" in restriction
    assert ":(exclude,glob)**/*.log" in restriction
    assert ":(exclude,glob)vendor/**" in restriction


def test_pathspec_declared_in_two_buckets_rejected() -> None:
    """同一规格跨桶声明直接拒绝。"""
    with pytest.raises(WorkspaceManifestError, match="多个桶"):
        WorkspaceManifest(writer_local=("op/build",), external=("op/build",))


def test_empty_pathspec_rejected() -> None:
    """空路径规格拒绝。"""
    with pytest.raises(WorkspaceManifestError, match="非法路径规格"):
        WorkspaceManifest(writer_local=("",))
