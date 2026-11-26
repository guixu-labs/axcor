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

"""已合并回滚的集成线回退直测：先校验后 reset、谱系失效集与换序重放。

现场用真实 git 构造「同基线兄弟 + 下游 + 活动写者」的集成线，断言
回退后的线恰为保留者终态（逐字节）、失效集按基线谱系划分、根代次
传播与旧根代次请求拒绝、拒绝路径对根头/根工作树/代次/账本零改动。
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from workflow_controller.core.workspace.actions import ScopeIntegrated
from workflow_controller.core.workspace.manager import RetractTarget, WorkspaceManagerError
from workflow_controller.core.workspace.repository import ROOT_INTEGRATION_REF
from workflow_controller.core.workspace.scope_tree import ScopeState, ScopeTreeError

from ._support import FakeOrchestrator, build_workspace, run_git

if TYPE_CHECKING:
    from workflow_controller.core.workspace.actions import IntegrationLineRetracted


class _LineageSite:
    """谱系现场：同基线兄弟 a/b、下游 d、基线干净的活动写者 e、失效活动写者 f。"""

    def __init__(self, tmp_path: Path) -> None:
        self.root, self.repository, self.manager = build_workspace(tmp_path)
        self.orchestrator = FakeOrchestrator(self.manager)
        self.bases: dict[str, str] = {}

    async def prepare(self, key: str) -> Path:
        prepared = await self.orchestrator.prepare(key)
        self.bases[key] = prepared.integration_base_commit
        return Path(prepared.workspace_path)

    async def integrate(self, key: str) -> None:
        outcome = await self.orchestrator.integrate(key)
        assert isinstance(outcome, ScopeIntegrated)

    async def build(self) -> None:
        """a/b/e 同基线先行，a 集成后 d、f 建区（基线含 a 的集成）。"""
        await self.orchestrator.bootstrap_root()
        ws_a = await self.prepare("node-a")
        ws_b = await self.prepare("node-b")
        ws_e = await self.prepare("node-e")
        (ws_a / "design" / "a.md").write_text("from-a\n", encoding="utf-8")
        (ws_b / "design" / "b.md").write_text("from-b\n", encoding="utf-8")
        (ws_e / "design" / "e.md").write_text("from-e\n", encoding="utf-8")
        await self.integrate("node-a")
        ws_d = await self.prepare("node-d")
        ws_f = await self.prepare("node-f")
        (ws_d / "design" / "d.md").write_text("from-d\n", encoding="utf-8")
        (ws_d / "design" / "a.md").write_text("d-extends-a\n", encoding="utf-8")
        (ws_f / "design" / "f.md").write_text("from-f\n", encoding="utf-8")
        await self.integrate("node-b")
        await self.integrate("node-d")
        # e 保持活动：基线不含 a 的集成，回退后按旧根代次拒绝其集成

    async def build_empty_integration_scene(self) -> Path:
        """空集成现场：empty 无文件改动通过，a/c 各写一文件并集成，down 为下游活动写者。"""
        await self.orchestrator.bootstrap_root()
        ws_empty = await self.prepare("node-empty")
        await self.integrate("node-empty")  # 通过但未改动文件：线上无集成提交
        ws_a = await self.prepare("node-a")
        ws_c = await self.prepare("node-c")
        (ws_a / "design" / "a.md").write_text("from-a\n", encoding="utf-8")
        (ws_c / "design" / "c.md").write_text("from-c\n", encoding="utf-8")
        await self.integrate("node-a")
        await self.integrate("node-c")
        ws_down = await self.prepare("node-down")  # 下游活动写者：建区基线含 a/c 的集成
        (ws_down / "design" / "down.md").write_text("from-down\n", encoding="utf-8")
        return ws_empty

    async def retract_a(
        self, *, client_op_id: str = "retract-1", epoch: int = 1
    ) -> IntegrationLineRetracted:
        return await self.manager.retract_integration_line(
            targets=(RetractTarget(scope_id="node-a", base_commit=self.bases["node-a"]),),
            expected_root_epoch=epoch,
            client_op_id=client_op_id,
        )

    def pollute_line_with_writer_local(self) -> str:
        """在集成线上落一个含自建路径变更的提交（不动根工作树），返回提交。"""
        payload = self.repository.repo_dir / "pollute-test-blob"
        payload.write_bytes(b"polluted-log\n")
        blob = run_git(["hash-object", "-w", str(payload)], self.repository.repo_dir).strip()
        payload.unlink()
        head = run_git(["rev-parse", "HEAD"], self.repository.repo_dir).strip()
        tree = self._polluted_tree(head, blob)
        commit = run_git(
            ["commit-tree", tree, "-p", head, "-m", "pollute line with writer_local"],
            self.repository.repo_dir,
        ).strip()
        run_git(["update-ref", ROOT_INTEGRATION_REF, commit], self.repository.repo_dir)
        return commit

    def _polluted_tree(self, head: str, blob: str) -> str:
        """基准树加一个自建路径条目后写树（经临时索引）。"""
        import os
        import subprocess

        index = self.repository.repo_dir / "index.pollute-test"
        env = dict(os.environ, GIT_DIR=str(self.repository.repo_dir), GIT_INDEX_FILE=str(index))
        for args in (
            ["git", "read-tree", head],
            ["git", "update-index", "--add", "--cacheinfo", f"100644,{blob},run.log"],
            ["git", "write-tree"],
        ):
            proc = subprocess.run(args, capture_output=True, text=True, check=True, env=env)
        index.unlink(missing_ok=True)
        return proc.stdout.strip()

    def restore_line_ref(self, commit: str) -> None:
        """把集成线引用拨回指定提交（污染构造的还原）。"""
        run_git(["update-ref", ROOT_INTEGRATION_REF, commit], self.repository.repo_dir)


async def _zero_change_facts(site: _LineageSite) -> tuple:
    """根头、根工作树与账本快照（零改动断言用）。"""
    head = await site.manager.current_integration_head()
    tree = {
        str(p.relative_to(site.root)): p.read_bytes()
        for p in sorted(site.root.rglob("*"))
        if p.is_file() and ".workflow-controller" not in p.parts
    }
    records = await site.manager.list_scopes()
    ledger = [
        (r.scope_id, r.scope_epoch, r.root_epoch, r.state.value, r.integration_base_commit)
        for r in records
    ]
    return head, tree, ledger


async def test_retraction_lineage_replay_and_epoch_propagation(tmp_path: Path) -> None:
    """谱系失效集（同基线兄弟保留、下游与失效活动写者废弃）+ 重放逐字节 + 根代次传播。"""
    site = _LineageSite(tmp_path)
    await site.build()

    head_before = await site.manager.current_integration_head()
    b_scope = await site.manager.get_scope("node-b")
    assert b_scope is not None
    b_branch_head_before = run_git(["rev-parse", "scope/node-b"], site.repository.repo_dir).strip()
    result = await site.retract_a()

    # 根代次 +1、新头推进；失效 = 目标 + 下游 d + 活动写者 f；保留 = b
    assert result.new_root_epoch == 2
    assert result.new_root_head != head_before
    assert {scope for scope, _epoch in result.invalidated} == {"node-a", "node-d", "node-f"}
    assert result.preserved == ("node-b",)

    # 根检出：a/d 的产物撤回、b 的产出保留
    assert not (site.root / "design" / "a.md").exists()
    assert not (site.root / "design" / "d.md").exists()
    assert (site.root / "design" / "b.md").read_text(encoding="utf-8") == "from-b\n"

    # 重放后的线在 merge 路径上与保留者终态逐字节一致
    diff = run_git(
        ["diff", "--name-only", result.new_root_head, b_branch_head_before, "--", "design"],
        site.repository.repo_dir,
    )
    assert diff.strip() == ""

    # 保留者作用域与分支不动；失效者换代且工作区移除
    b_scope_after = await site.manager.get_scope("node-b")
    assert b_scope_after is not None
    assert b_scope_after.state is ScopeState.INTEGRATED
    assert b_scope_after.scope_epoch == b_scope.scope_epoch
    assert (
        run_git(["rev-parse", "scope/node-b"], site.repository.repo_dir).strip()
        == b_branch_head_before
    )
    for key in ("node-a", "node-d", "node-f"):
        record = await site.manager.get_scope(key)
        assert record is not None
        assert record.state is ScopeState.DISCARDED
        assert record.scope_epoch == 2
        assert not (site.repository.workspaces_dir / key).exists()

    # 旧根代次的集成请求一律拒绝：干净活动写者 e 的集成被代次纪律挡下
    with pytest.raises(ScopeTreeError, match="旧根代次"):
        await site.orchestrator.integrate("node-e")

    # 同操作号重放幂等：返回原结果，不产生第二次回退
    replay = await site.retract_a()
    assert replay.new_root_epoch == result.new_root_epoch
    assert replay.new_root_head == result.new_root_head
    assert {s for s, _e in replay.invalidated} == {s for s, _e in result.invalidated}


async def test_retraction_rejections_leave_zero_change(tmp_path: Path) -> None:
    """校验拒绝零改动：目标不在线上/目标为当前头/区间含自建变更/根检出脏。"""
    site = _LineageSite(tmp_path)
    await site.build()
    head_before = await site.manager.current_integration_head()
    facts = await _zero_change_facts(site)

    # ② 目标不属于当前集成线：作用域分支提交不在集成线首父链上
    b_branch_head = run_git(["rev-parse", "scope/node-b"], site.repository.repo_dir).strip()
    with pytest.raises(WorkspaceManagerError, match="不属于当前集成线"):
        await site.manager.retract_integration_line(
            targets=(RetractTarget(scope_id="node-a", base_commit=b_branch_head),),
            expected_root_epoch=1,
            client_op_id="retract-x1",
        )

    # ③ 目标基线即当前集成头：非严格祖先，拒绝
    with pytest.raises(WorkspaceManagerError, match="严格）祖先"):
        await site.manager.retract_integration_line(
            targets=(RetractTarget(scope_id="node-a", base_commit=head_before or ""),),
            expected_root_epoch=1,
            client_op_id="retract-x2",
        )

    # ④ 回退区间含自建路径变更：构造一个带自建路径的线提交后拒绝
    polluted = site.pollute_line_with_writer_local()
    try:
        with pytest.raises(WorkspaceManagerError, match="自建路径变更"):
            await site.manager.retract_integration_line(
                targets=(RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),),
                expected_root_epoch=1,
                client_op_id="retract-x3",
            )
    finally:
        site.restore_line_ref(head_before or "")

    # 根检出存在未提交变化：先经集成落账再回退
    (site.root / "design" / "loose.md").write_text("loose\n", encoding="utf-8")
    try:
        with pytest.raises(WorkspaceManagerError, match="未提交变化"):
            await site.manager.retract_integration_line(
                targets=(RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),),
                expected_root_epoch=1,
                client_op_id="retract-x4",
            )
    finally:
        (site.root / "design" / "loose.md").unlink()
    _ = polluted

    # 拒绝后零改动：根头、根工作树、账本与代次与回退前一致
    head_after, tree_after, ledger_after = await _zero_change_facts(site)
    assert head_after == facts[0]
    assert tree_after == facts[1]
    assert ledger_after == facts[2]


async def test_retraction_replays_multiple_preservers_in_original_order(
    tmp_path: Path,
) -> None:
    """多个保留者按原集成序重放：序取自集成线历史，内容收敛到保留者终态。"""
    site = _LineageSite(tmp_path)
    await site.orchestrator.bootstrap_root()
    ws_a = await site.prepare("node-a")
    ws_b = await site.prepare("node-b")
    ws_c = await site.prepare("node-c")
    (ws_a / "design" / "a.md").write_text("from-a\n", encoding="utf-8")
    (ws_b / "design" / "b.md").write_text("from-b\n", encoding="utf-8")
    (ws_c / "design" / "c.md").write_text("from-c\n", encoding="utf-8")
    # 集成序 b → a → c：回退 a 时保留者按原集成序为 (b, c)
    await site.integrate("node-b")
    await site.integrate("node-a")
    await site.integrate("node-c")

    result = await site.retract_a()
    assert result.preserved == ("node-b", "node-c")
    # 重放后的线沿原集成序：线信息含 b、c 的集成主题（可复现序列）
    log = run_git(
        ["log", "--first-parent", "--pretty=%s", result.new_root_head],
        site.repository.repo_dir,
    ).splitlines()
    integrated_subjects = [s for s in log if s.startswith("integrate ")]
    assert integrated_subjects == ["integrate node-c", "integrate node-b"]
    for name in ("b.md", "c.md"):
        assert (site.root / "design" / name).exists()
    assert not (site.root / "design" / "a.md").exists()


async def test_multi_target_rejection_leaves_zero_change(tmp_path: Path) -> None:
    """任一目标不合格即整批拒绝零改动：单独合法的 a 也不回退（全有或全无）。"""
    site = _LineageSite(tmp_path)
    await site.build()
    facts = await _zero_change_facts(site)

    # e 未集成：目标集第二个目标不合格，整批拒绝
    with pytest.raises(WorkspaceManagerError, match="未集成：node-e"):
        await site.manager.retract_integration_line(
            targets=(
                RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),
                RetractTarget(scope_id="node-e", base_commit=site.bases["node-e"]),
            ),
            expected_root_epoch=1,
            client_op_id="retract-multi-x1",
        )
    head_after, tree_after, ledger_after = await _zero_change_facts(site)
    assert head_after == facts[0]
    assert tree_after == facts[1]
    assert ledger_after == facts[2]


async def test_multi_target_rejection_on_duplicate_scope(tmp_path: Path) -> None:
    """目标集含重复作用域按非法输入拒绝。"""
    site = _LineageSite(tmp_path)
    await site.build()
    with pytest.raises(WorkspaceManagerError, match="重复作用域"):
        await site.manager.retract_integration_line(
            targets=(
                RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),
                RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),
            ),
            expected_root_epoch=1,
            client_op_id="retract-multi-x2",
        )


async def test_multi_target_retraction_advances_in_one_action(tmp_path: Path) -> None:
    """多目标一次动作完成回退：目标与谱系吸收写者全失效换代，根代次 +1。"""
    site = _LineageSite(tmp_path)
    await site.build()

    result = await site.manager.retract_integration_line(
        targets=(
            RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),
            RetractTarget(scope_id="node-b", base_commit=site.bases["node-b"]),
        ),
        expected_root_epoch=1,
        client_op_id="retract-multi",
    )
    # a/b 同基线：回退点即该基线，无保留者时新头就是回退点本身
    assert result.new_root_epoch == 2
    assert result.target_commit == site.bases["node-a"]
    assert result.new_root_head == site.bases["node-a"]
    # 失效集 = 目标集 ∪ 谱系吸收（下游 d 与失效活动写者 f）；基线干净的 e 不入失效面
    assert {scope for scope, _epoch in result.invalidated} == {
        "node-a",
        "node-b",
        "node-d",
        "node-f",
    }
    assert result.preserved == ()

    # 失效作用域各自换代并移除工作区；e 零触碰
    for key in ("node-a", "node-b", "node-d", "node-f"):
        record = await site.manager.get_scope(key)
        assert record is not None
        assert record.state is ScopeState.DISCARDED
        assert record.scope_epoch == 2
        assert not (site.repository.workspaces_dir / key).exists()
    e_scope = await site.manager.get_scope("node-e")
    assert e_scope is not None
    assert e_scope.state is ScopeState.ACTIVE
    assert e_scope.scope_epoch == 1
    assert (site.repository.workspaces_dir / "node-e").exists()

    # 根检出：目标与谱系吸收者的产物一并撤回，基线文件仍在
    for name in ("a.md", "b.md", "d.md"):
        assert not (site.root / "design" / name).exists()
    assert (site.root / "design" / "base.md").read_text(encoding="utf-8") == "base\n"


async def test_multi_target_retraction_point_is_earliest_baseline(tmp_path: Path) -> None:
    """回退点取各目标建区基线在首父链上的最早者：a 基线早于 b 基线 → T = a 的基线。"""
    site = _LineageSite(tmp_path)
    await site.orchestrator.bootstrap_root()
    ws_a = await site.prepare("node-a")
    ws_p = await site.prepare("node-p")
    (ws_a / "design" / "a.md").write_text("from-a\n", encoding="utf-8")
    (ws_p / "design" / "p.md").write_text("from-p\n", encoding="utf-8")
    await site.integrate("node-a")
    ws_b = await site.prepare("node-b")  # 建区基线已含 a 的集成
    (ws_b / "design" / "b.md").write_text("from-b\n", encoding="utf-8")
    await site.integrate("node-b")
    await site.integrate("node-p")  # 保留者最后集成

    result = await site.manager.retract_integration_line(
        targets=(
            RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),
            RetractTarget(scope_id="node-b", base_commit=site.bases["node-b"]),
        ),
        expected_root_epoch=1,
        client_op_id="retract-earliest",
    )
    # 回退点 = a 的建区基线（最早者）；b 的基线（更晚）不被采用
    assert site.bases["node-b"] != site.bases["node-a"]
    assert result.target_commit == site.bases["node-a"]
    assert {scope for scope, _epoch in result.invalidated} == {"node-a", "node-b"}
    # p 基线不含任一目标的集成提交：保留并按原集成序重放
    assert result.preserved == ("node-p",)
    assert (site.root / "design" / "p.md").read_text(encoding="utf-8") == "from-p\n"
    assert not (site.root / "design" / "a.md").exists()
    assert not (site.root / "design" / "b.md").exists()


async def test_multi_target_retraction_idempotent_replay_and_conflict(tmp_path: Path) -> None:
    """幂等缓存：同操作号同目标集（乱序重放含内）返回原结果；异目标集按冲突拒绝。"""
    site = _LineageSite(tmp_path)
    await site.build()
    targets = (
        RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),
        RetractTarget(scope_id="node-b", base_commit=site.bases["node-b"]),
    )
    result = await site.manager.retract_integration_line(
        targets=targets, expected_root_epoch=1, client_op_id="retract-multi"
    )

    replay = await site.manager.retract_integration_line(
        targets=targets, expected_root_epoch=1, client_op_id="retract-multi"
    )
    assert replay == result

    # 目标集相同但顺序不同：规范形一致，仍返回原结果
    permuted = await site.manager.retract_integration_line(
        targets=(targets[1], targets[0]), expected_root_epoch=1, client_op_id="retract-multi"
    )
    assert permuted == result

    # 同操作号携带不同目标集：按冲突拒绝，不返回旧结果
    with pytest.raises(WorkspaceManagerError, match="已绑定不同的回退请求"):
        await site.manager.retract_integration_line(
            targets=(targets[0],), expected_root_epoch=1, client_op_id="retract-multi"
        )


async def test_retraction_with_empty_integration_preserver(tmp_path: Path) -> None:
    """空集成保留者不阻塞回退：原样保留不重放，真实目标的产物照常撤回。"""
    site = _LineageSite(tmp_path)
    await site.build_empty_integration_scene()

    result = await site.manager.retract_integration_line(
        targets=(
            RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),
            RetractTarget(scope_id="node-c", base_commit=site.bases["node-c"]),
        ),
        expected_root_epoch=1,
        client_op_id="retract-empty-keep",
    )
    # 失效 = 真实目标 + 下游活动写者；空集成保留者不进失效面也不重放
    assert {scope for scope, _epoch in result.invalidated} == {"node-a", "node-c", "node-down"}
    assert result.preserved == ()
    assert result.new_root_epoch == 2
    # 无保留者可重放：新头就是回退点本身
    assert result.new_root_head == site.bases["node-a"]
    assert not (site.root / "design" / "a.md").exists()
    assert not (site.root / "design" / "c.md").exists()
    empty_record = await site.manager.get_scope("node-empty")
    assert empty_record is not None
    assert empty_record.state is ScopeState.INTEGRATED
    assert empty_record.scope_epoch == 1


async def test_retraction_with_empty_integration_target(tmp_path: Path) -> None:
    """空集成目标不阻塞回退：直接失效换代，回退点只由真实目标决定。"""
    site = _LineageSite(tmp_path)
    await site.build_empty_integration_scene()

    result = await site.manager.retract_integration_line(
        targets=(
            RetractTarget(scope_id="node-empty", base_commit=site.bases["node-empty"]),
            RetractTarget(scope_id="node-c", base_commit=site.bases["node-c"]),
        ),
        expected_root_epoch=1,
        client_op_id="retract-empty-target",
    )
    # 失效 = 空集成目标 + 真实目标 + 下游；真实保留者 a 按原集成序重放
    assert {scope for scope, _epoch in result.invalidated} == {"node-empty", "node-c", "node-down"}
    assert result.preserved == ("node-a",)
    assert result.new_root_epoch == 2
    # 根检出：a 的产物经重放保留，c 的撤回
    assert (site.root / "design" / "a.md").read_text(encoding="utf-8") == "from-a\n"
    assert not (site.root / "design" / "c.md").exists()
    empty_record = await site.manager.get_scope("node-empty")
    assert empty_record is not None
    assert empty_record.state is ScopeState.DISCARDED
    assert empty_record.scope_epoch == 2
    a_record = await site.manager.get_scope("node-a")
    assert a_record is not None
    assert a_record.state is ScopeState.INTEGRATED
    assert a_record.scope_epoch == 1


async def test_retraction_of_only_empty_target_keeps_line_untouched(tmp_path: Path) -> None:
    """目标集全部为空集成：线与根检出不动、根代次不推进，在途下游照常集成。"""
    site = _LineageSite(tmp_path)
    await site.build_empty_integration_scene()
    head_before = await site.manager.current_integration_head()

    result = await site.manager.retract_integration_line(
        targets=(RetractTarget(scope_id="node-empty", base_commit=site.bases["node-empty"]),),
        expected_root_epoch=1,
        client_op_id="retract-empty-only",
    )
    # 无集成提交可撤：回退点即当前头，线不动、保留者不重放、根代次不推进
    assert result.new_root_head == head_before
    assert result.target_commit == head_before
    assert result.preserved == ()
    assert result.new_root_epoch == 1
    # 线上无人消费过空集成的成果：真实写者与下游零触碰
    assert {scope for scope, _epoch in result.invalidated} == {"node-empty"}
    for key in ("node-a", "node-c", "node-down"):
        record = await site.manager.get_scope(key)
        assert record is not None
        assert record.scope_epoch == 1
    for name in ("a.md", "c.md"):
        assert (site.root / "design" / name).exists()
    # 基线仍属当前根代次的在途写者：回退后集成不被旧代次拒绝
    await site.integrate("node-down")
    assert (site.root / "design" / "down.md").read_text(encoding="utf-8") == "from-down\n"


async def test_retraction_of_empty_target_created_at_current_head(tmp_path: Path) -> None:
    """建区基线即当前头的空集成目标：免严格祖先校验，照常失效换代。"""
    site = _LineageSite(tmp_path)
    await site.orchestrator.bootstrap_root()
    await site.prepare("node-empty")
    await site.integrate("node-empty")  # 空集成：线未动，基线仍是当前头
    head = await site.manager.current_integration_head()
    assert head == site.bases["node-empty"]

    result = await site.manager.retract_integration_line(
        targets=(RetractTarget(scope_id="node-empty", base_commit=site.bases["node-empty"]),),
        expected_root_epoch=1,
        client_op_id="retract-empty-head",
    )
    assert result.new_root_head == head
    assert result.new_root_epoch == 1  # 线不动：根代次不推进
    record = await site.manager.get_scope("node-empty")
    assert record is not None
    assert record.state is ScopeState.DISCARDED
    assert record.scope_epoch == 2


async def test_retraction_with_reintegrated_preserver(tmp_path: Path) -> None:
    """同作用域两次集成、回退点落在两条集成提交之间：最新集成须重放。"""
    site = _LineageSite(tmp_path)
    await site.orchestrator.bootstrap_root()
    ws_s = await site.prepare("node-s")
    (ws_s / "design" / "s1.md").write_text("s1\n", encoding="utf-8")
    await site.integrate("node-s")  # 第一次集成
    ws_t = await site.prepare("node-t")  # T 建区基线即第一次集成提交
    (ws_t / "design" / "t.md").write_text("t\n", encoding="utf-8")
    await site.orchestrator.abandon("node-s", 1)  # S 废弃重建后再次集成
    ws_s2 = await site.prepare("node-s")
    (ws_s2 / "design" / "s2.md").write_text("s2\n", encoding="utf-8")
    await site.integrate("node-s")  # 第二次集成
    await site.integrate("node-t")

    result = await site.manager.retract_integration_line(
        targets=(RetractTarget(scope_id="node-t", base_commit=site.bases["node-t"]),),
        expected_root_epoch=1,
        client_op_id="retract-t",
    )
    # 重放判据取最晚一条集成提交：第二次集成被回退撤下，须重放回线
    assert result.preserved == ("node-s",)
    assert (site.root / "design" / "s2.md").exists()
    assert (site.root / "design" / "s1.md").exists()


async def test_retraction_rejects_scope_with_unparsed_line_changes(tmp_path: Path) -> None:
    """线上无集成提交但分支带集成路径变更：不按空集成跳过，目标与保留者双拒绝。"""
    site = _LineageSite(tmp_path)
    ws_empty = await site.build_empty_integration_scene()
    # 模拟提交信息解析错位：集成后向作用域分支补一条带集成路径变更的提交
    (ws_empty / "design" / "extra.md").write_text("extra\n", encoding="utf-8")
    run_git(["add", "design/extra.md"], ws_empty)
    run_git(["commit", "-m", "unparsed line change"], ws_empty)

    for client_op_id, targets in (
        (
            "retract-unparsed-target",
            (RetractTarget(scope_id="node-empty", base_commit=site.bases["node-empty"]),),
        ),
        (
            "retract-unparsed-preserver",
            (
                RetractTarget(scope_id="node-a", base_commit=site.bases["node-a"]),
                RetractTarget(scope_id="node-c", base_commit=site.bases["node-c"]),
            ),
        ),
    ):
        with pytest.raises(WorkspaceManagerError, match="分支带集成路径变更"):
            await site.manager.retract_integration_line(
                targets=targets,
                expected_root_epoch=1,
                client_op_id=client_op_id,
            )
