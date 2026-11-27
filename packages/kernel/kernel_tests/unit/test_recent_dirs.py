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

"""recent_dirs（最近项目）卫生机制测试。

设计要点：pytest 的 tmp_path 本身位于真实系统临时区下，因此除最后一个回归用例外，
全部用例 monkeypatch ``config_api._transient_roots`` 指向沙盒内伪临时区根，
「持久项目目录」用沙盒内真实 mkdir 的目录——谓词行为与宿主环境完全解耦。
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from workflow_controller.api import config_api
from workflow_controller.api.config_api import ConfigAPI
from workflow_controller.config.settings import AppConfig

if TYPE_CHECKING:
    import pytest

# ── 沙盒与 fixture ───────────────────────────────────────────────────────────


class Sandbox:
    """测试沙盒：真实目录树 + 伪系统临时区根（monkeypatch 解耦宿主 TMPDIR）。"""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        self.root = tmp_path
        self.transient = tmp_path / "T"
        self.transient.mkdir()
        monkeypatch.setattr(config_api, "_transient_roots", lambda: (self.transient,))

    def proj(self, rel: str) -> str:
        """真实持久项目目录（mkdir 返回绝对路径）。"""
        d = self.root / rel
        d.mkdir(parents=True, exist_ok=True)
        return str(d)

    def transient_dir(self, rel: str) -> str:
        """伪临时区内的真实目录（mkdir）——存在但瞬态，谓词必须拒绝。"""
        d = self.transient / rel
        d.mkdir(parents=True, exist_ok=True)
        return str(d)

    def write_json(self, dirs: list[str]) -> None:
        (self.root / "recent_dirs.json").write_text(
            json.dumps(dirs),
            encoding="utf-8",
        )

    def read_json(self) -> list[str]:
        return json.loads((self.root / "recent_dirs.json").read_text(encoding="utf-8"))


def _orch_with_cfg(cfg_dir: Path, sessions: list[SimpleNamespace] | None = None) -> MagicMock:
    """构造带 cfg_dir + pool.list_sessions 的 mock orchestrator。

    cfg_dir 指向 tmp_path，让 recent_dirs.json 落盘到测试沙盒（不碰真实主机目录）。
    sessions 为 SimpleNamespace 列表，每个含 .config.cwd，模拟 in-memory pool
    （含 load_sessions() 还原的 ghost session）。
    """
    orch = MagicMock()
    orch.config = AppConfig(project_dir=".")
    orch.cfg_dir = cfg_dir
    orch.pool.list_sessions.return_value = sessions or []
    return orch


def _sess(cwd: str) -> SimpleNamespace:
    """模拟一个 session：config.cwd = cwd（SessionInfo.project_dir 的来源）。"""
    return SimpleNamespace(config=SimpleNamespace(cwd=cwd))


# ── 卫生谓词 ────────────────────────────────────────────────────────────────


def test_is_persistent_project_dir_branches(tmp_path, monkeypatch):
    """谓词四分支：非空/绝对/真实目录/非临时区——缺一即假。"""
    sb = Sandbox(tmp_path, monkeypatch)
    real = sb.proj("work/proj-a")
    tdir = sb.transient_dir("opencode/realtest/target")

    assert getattr(config_api, "_is_persistent_project_dir")(real)
    assert not getattr(config_api, "_is_persistent_project_dir")("")  # 空
    assert not getattr(config_api, "_is_persistent_project_dir")(".")  # 相对路径哨兵
    assert not getattr(config_api, "_is_persistent_project_dir")(str(tmp_path / "nope"))  # 不存在
    assert not getattr(config_api, "_is_persistent_project_dir")(tdir)  # 瞬态区（即使真实存在）
    assert not getattr(config_api, "_is_persistent_project_dir")(f"{tdir}/deeper")  # 瞬态区深层


def test_is_transient_dir_real_gettempdir(tmp_path):
    """不 monkeypatch 的宿主回归：真实 tempfile.gettempdir() 下的目录判瞬态。"""
    real_t = Path(tempfile.gettempdir()).resolve() / "workflow-controller-hygiene-check"
    real_t.mkdir(exist_ok=True)
    try:
        assert getattr(config_api, "_is_transient_dir")(str(real_t))
        # pytest tmp_path 也在真实临时区下——同样判瞬态
        assert getattr(config_api, "_is_transient_dir")(str(tmp_path))
    finally:
        real_t.rmdir()
    # 持久区负例用根路径：本仓 hermetic 插件会把 HOME 重定向到 mkdtemp 临时目录，
    # 测试进程内 Path.home() 真的位于临时区（谓词判 True 是正确行为），不能当负例。
    assert not getattr(config_api, "_is_transient_dir")("/")


# ── get_recent_dirs: 合并 + 去重 + recency 优先 ──────────────────────────────


def test_get_recent_dirs_merges_user_and_session_cwds_dedup(tmp_path, monkeypatch):
    """用户手选 + session cwd 合并：用户目录在前、追加在后、去重。"""
    sb = Sandbox(tmp_path, monkeypatch)
    a, b = sb.proj("proj/a"), sb.proj("proj/b")
    s_only = sb.proj("session-only")
    sb.write_json([a, b])
    orch = _orch_with_cfg(
        tmp_path,
        sessions=[
            _sess(b),  # 与 json 的 b 去重
            _sess(s_only),  # session-only：追加在 json 之后
        ],
    )
    api = ConfigAPI(orch, tmp_path / "saved.yaml")

    assert api.get_recent_dirs() == [a, b, s_only]


# ── ghost session 的临时区与死 cwd 不进顶层 ──────────────────────────────────


def test_get_recent_dirs_excludes_dead_and_transient_ghost_cwds(tmp_path, monkeypatch):
    """ghost session 指向已删除/临时区目录 → 不并入「最近项目」。"""
    sb = Sandbox(tmp_path, monkeypatch)
    live = sb.proj("live/proj")
    dead = str(sb.root / "deleted/proj")  # 从未创建（模拟目录已被清理）
    tdir = sb.transient_dir("opencode/realtest/target")
    sb.write_json([live])
    orch = _orch_with_cfg(tmp_path, sessions=[_sess(dead), _sess(tdir), _sess(live)])
    api = ConfigAPI(orch, tmp_path / "saved.yaml")

    assert api.get_recent_dirs() == [live]


# ── 存量自清洁：json 读盘时剔除非法条目并回写 ────────────────────────────────


def test_load_recent_dirs_self_cleans_legacy_noise(tmp_path, monkeypatch):
    """存量 json 含临时区/死目录/相对路径 → 读取即剔除并回写，用户无需手动清理。"""
    sb = Sandbox(tmp_path, monkeypatch)
    keep = sb.proj("work/keep")
    tdir = sb.transient_dir("opencode/realtest/target")
    dead = str(sb.root / "gone")
    sb.write_json([tdir, keep, dead, "."])

    orch = _orch_with_cfg(tmp_path, sessions=[])
    result = ConfigAPI(orch, tmp_path / "saved.yaml").get_recent_dirs()

    assert result == [keep]
    assert sb.read_json() == [keep]  # 落盘同步收敛


# ── add_recent_dir: 校验单点 + 置顶 + 不重排 + 上限 ─────────────────────────


def test_add_recent_dir_new_prepends(tmp_path, monkeypatch):
    """新目录置顶（recency 最新在前），存 resolve() 规范绝对值。"""
    sb = Sandbox(tmp_path, monkeypatch)
    a, b, c = sb.proj("a"), sb.proj("b"), sb.proj("c")
    sb.write_json([a, b])
    api = ConfigAPI(_orch_with_cfg(tmp_path, sessions=[]), tmp_path / "saved.yaml")

    result, err = api.add_recent_dir(c)

    assert err is None
    assert result[:3] == [c, a, b]
    assert sb.read_json() == [c, a, b]


def test_add_recent_dir_existing_no_reorder(tmp_path, monkeypatch):
    """已在列表的目录不重排（避免点击切换项目时列表抖动，与旧 localStorage 语义一致）。"""
    sb = Sandbox(tmp_path, monkeypatch)
    a, b = sb.proj("a"), sb.proj("b")
    sb.write_json([a, b])
    api = ConfigAPI(_orch_with_cfg(tmp_path, sessions=[]), tmp_path / "saved.yaml")

    result, err = api.add_recent_dir(a)

    assert err is None
    assert result == [a, b]
    assert sb.read_json() == [a, b]  # 落盘文件不变


def test_add_recent_dir_respects_max(tmp_path, monkeypatch):
    """用户手选目录上限 10（RECENT_DIRS_MAX），超出的丢最旧。"""
    sb = Sandbox(tmp_path, monkeypatch)
    initial = [sb.proj(f"p{i}") for i in range(10)]
    new = sb.proj("new")
    sb.write_json(initial)
    api = ConfigAPI(_orch_with_cfg(tmp_path, sessions=[]), tmp_path / "saved.yaml")

    _, err = api.add_recent_dir(new)

    assert err is None
    assert sb.read_json() == [new, *initial[:9]]  # new 置顶，丢最旧的 p9
    assert len(sb.read_json()) == 10


def test_add_recent_dir_rejects_transient(tmp_path, monkeypatch):
    """系统临时区目录拒绝入库（transient dir），json 保持不变。"""
    sb = Sandbox(tmp_path, monkeypatch)
    keep = sb.proj("keep")
    tdir = sb.transient_dir("opencode/realtest/target")
    sb.write_json([keep])
    api = ConfigAPI(_orch_with_cfg(tmp_path, sessions=[]), tmp_path / "saved.yaml")

    result, err = api.add_recent_dir(tdir)

    assert err == "transient dir"
    assert result == [keep]
    assert sb.read_json() == [keep]


def test_add_recent_dir_rejects_dead_and_relative(tmp_path, monkeypatch):
    """不存在/非目录/相对路径一律拒绝（path not found），json 保持不变。"""
    sb = Sandbox(tmp_path, monkeypatch)
    keep = sb.proj("keep")
    sb.write_json([keep])
    api = ConfigAPI(_orch_with_cfg(tmp_path, sessions=[]), tmp_path / "saved.yaml")

    result_dead, err_dead = api.add_recent_dir(str(sb.root / "nope"))
    result_rel, err_rel = api.add_recent_dir(".")
    result_empty, err_empty = api.add_recent_dir("")

    assert err_dead == "path not found" and result_dead == [keep]
    assert err_rel == "path not found" and result_rel == [keep]
    assert err_empty == "dir is required" and result_empty == [keep]
    assert sb.read_json() == [keep]


# ── remove_recent_dir: 真删、落盘 ──────────────────────────────────────────


def test_remove_recent_dir_persists(tmp_path, monkeypatch):
    """移除目录真删 recent_dirs.json。pool 中的 session cwd 会推回。"""
    sb = Sandbox(tmp_path, monkeypatch)
    a, b, c = sb.proj("a"), sb.proj("b"), sb.proj("c")
    sb.write_json([a, b, c])
    orch = _orch_with_cfg(tmp_path, sessions=[_sess(c)])  # 普通 session 并入
    api = ConfigAPI(orch, tmp_path / "saved.yaml")

    result = api.remove_recent_dir(b)

    assert b not in result
    assert a in result and c in result  # c 仍在（json 留 + pool 推）
    assert sb.read_json() == [a, c]


# ── 落盘跨实例：重启后 recent_dirs 不丢 ──────────────────────────────────────


def test_recent_dirs_persist_across_instances(tmp_path, monkeypatch):
    """重启模拟：一个 ConfigAPI 写 recent_dirs.json，另一个新实例读回，列表不丢。"""
    sb = Sandbox(tmp_path, monkeypatch)
    old, new = sb.proj("old"), sb.proj("new")
    sb.write_json([old])
    ConfigAPI(_orch_with_cfg(tmp_path, sessions=[]), tmp_path / "saved.yaml").add_recent_dir(new)

    # 重启：新 orchestrator + 新 ConfigAPI，同一 cfg_dir
    api2 = ConfigAPI(_orch_with_cfg(tmp_path, sessions=[]), tmp_path / "saved.yaml")

    assert api2.get_recent_dirs() == [new, old]


# ── get_config_dict 包含 recent_dirs 字段（随 SSE 推送）──────────────────────


def test_get_config_dict_includes_recent_dirs(tmp_path, monkeypatch):
    """get_config_dict 返回 recent_dirs 字段，不含临时区/死目录条目。"""
    sb = Sandbox(tmp_path, monkeypatch)
    proj = sb.proj("proj/a")
    tdir = sb.transient_dir("workflow-controller-tb-z/cwd")
    sb.write_json([proj, tdir])
    orch = _orch_with_cfg(tmp_path, sessions=[_sess(tdir)])
    api = ConfigAPI(orch, tmp_path / "saved.yaml")

    d = api.get_config_dict()

    assert d["recent_dirs"] == [proj]
