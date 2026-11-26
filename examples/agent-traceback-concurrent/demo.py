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

"""agent-traceback 三阶段示例 —— 设计(并发)/开发/测试，重放到 developer 跑之前并注入指令。

一个**轻量但真实**的三阶段 workflow 端到端链路演示（CI 友好，无需 LLM / opencode 二进制）：
结构与 [ops-registry-invoke](https://gitcode.com/cann/skills.git) 对齐（设计 → 开发 → 测试），
但**极简**（4 个 subagent，每个只写一个小文件，省 token、短耗时）。在进程内起真实
Orchestrator + REST 路由（httpx ASGITransport，不起子进程 web），跑的是**真实代码路径**
（``SubagentTracker.on_start`` → ``SnapshotEngine.snapshot`` → checkpoints 表 → ``TracebackRunner.run``
→ seed 预制 session / overlay 注入 / 文件态预览 / 最终状态检查点）。

五个阶段，一条清晰的故事线——「跑三阶段 → 预览文件态 → 重放到 developer 跑之前
→ 注入 developer 指令 → 跳回最终状态」：

  * **Phase 1（三阶段 + 四个检查点）**：
    - 阶段一（设计，**并发**）：designer_api start → 检查点 #1（磁盘空）；designer_api 写
      design_api.md；designer_impl start → 检查点 #2（design_api.md 在、design_impl.md 不在）。
      此刻两个设计 subagent **同时在飞**（in-flight=2，并发）——对齐 ops-registry-invoke
      阶段一 1.3c 的并行分段生成。
    - 阶段二（开发）：developer start → 检查点 #3（两份 design 在、impl.py 不在）；
      developer 写 impl.py。
    - 阶段三（测试）：tester start → 检查点 #4（design + impl.py 在、test.py 不在）；
      tester 写 test.py。
    验证 trailer / 独立 ref / 排除 .workflow-controller / parent 链 / 并发 in-flight=2。
  * **Phase 2（预览文件态 → 决定从哪重放）**：``GET /checkpoints/{cp3}/files`` 列检查点 #3
    的文件 → 两份 design 在、impl.py 不在；``/file?path=design_api.md`` 读内容。不创建重放
    session 即可审视那一刻磁盘，决定是否 / 从哪重放。
  * **Phase 3（重放到 developer 跑之前）**：``POST /traceback`` 从检查点 #3 重放（re-call
    developer）→ re-call 语义移除 developer 的 task part **及其之后全部** → 重放 session 里
    developer 还没跑。验证：fork cwd 恢复两份 design、**impl.py 不在**（developer 没跑）、
    birth_traceback 溯源、seed 预制 session resume。
  * **Phase 4（注入 subagent 指令 → developer 系统 prompt）**：在重放 session 登记 developer
    补充指令（``POST /subagent-hint``）——developer 还没跑，下一轮它启动时指令已前置进 developer
    系统 prompt（overlay 前置、原 .md 正文保留、非侵入可逆）。这就是「重放到 developer 跑之前，
    注入 subagent 指令拨转重跑方向」（如把 add 改成乘法）。
  * **Phase 5（最终状态检查点 + 跳回）**：模拟 session 完成（写 summary.md）→
    ``_snapshot_session_complete`` 打最终状态检查点（trigger=session_complete）；预览最终文件态
    （design×2 + impl.py + test.py + summary.md 齐全）；从重放 session 读 origin 的检查点列表
    （``GET /sessions/{origin}/checkpoints``）——即「重放不满意时跳回原始 workflow 的最终状态
    或任一检查点重新重放」。

用法::

    cd examples/agent-traceback-concurrent
    python demo.py            # CI 友好，无需 API key

前置：Workflow Controller 已装（``pip install -e .``）+ git 已装。Live 跑法见 README。
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
from pathlib import Path
from typing import NamedTuple
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))

from opencode_demo_lib import (
    DemoOpencodeDb,
    attach_traceback_plugin,
    configure_demo_logging,
    demo_banner,
    demo_http_get,
    demo_http_post,
    mount_real_app,
    resolve_effective_subagent_agent,
    seed_agent_defs,
)

HERE = Path(__file__).resolve().parent

logger = logging.getLogger(__name__)


def banner(title: str) -> None:
    demo_banner(logger, title)


# 阶段一（设计）产物（极简，省 token / 短时间）。
DESIGN_API_MD = "# API 设计\n\nadd(a, b) -> a + b\n"
DESIGN_IMPL_MD = "# 实现设计\n\nreturn a + b\n"

# 阶段二（开发）产物。
IMPL_PY = "def add(a, b):\n    return a + b\n"

# 阶段三（测试）产物。
TEST_PY = "from impl import add\nassert add(1, 2) == 3\nprint('ok')\n"

# 主 agent 写的 summary.md（收尾）。
SUMMARY_MD = "# 交付总结\n\n三阶段（设计/开发/测试）完成。\n"

# 召回时登记的 developer 补充指令：拨转 developer 行为（加法 → 乘法）。
HINT_TEXT = (
    "重放补充指令：本次 impl.py 的 add(a,b) 应返回 a*b（乘法，覆盖原加法）。"
    "标记 REPLAY_HINT_MARKER。"
)
MARKERS = ["a*b", "REPLAY_HINT_MARKER", "乘法"]

DESIGNER_API_TYPE = "designer_api"
DESIGNER_IMPL_TYPE = "designer_impl"
DEVELOPER_TYPE = "developer"
TESTER_TYPE = "tester"

API_SUB_ID = "ses_sub_da"
IMPL_DESIGN_SUB_ID = "ses_sub_di"
DEV_SUB_ID = "ses_sub_dev"
TESTER_SUB_ID = "ses_sub_t"
ORIGIN_SDK_SESSION = "ses_orig_main"


def _effective_subagent_agent(subagent_type, hints, agents_dir):
    return resolve_effective_subagent_agent(subagent_type, hints, agents_dir)


async def _http_get(app, path: str) -> dict:
    return await demo_http_get(app, path)


async def _http_post(app, path: str, body: dict | None = None) -> dict:
    return await demo_http_post(app, path, body)


def _print_demo_header() -> None:
    """打印 demo 头部说明（配置、模式、任务与 hint 概览）。"""
    banner("agent-traceback 三阶段示例 (设计(并发) → 开发 → 测试 → 重放到 developer 跑之前 → 注入)")
    logger.info(
        f"  config : {HERE / 'workflow-controller.yaml'}  (三阶段: 设计(并发) / 开发 / 测试)"
    )
    logger.info("  mode   : in-process（不起子进程 web / LLM，CI 友好）")
    logger.info("  task   : designer_api+designer_impl(并发) → developer → tester")
    logger.info(f"  hint   : {HINT_TEXT[:40]}...")


def _make_sandbox() -> tuple[Path, Path, Path]:
    """建临时 project/xdg 目录并设 XDG_DATA_HOME；返回 (tmp, project, xdg)。"""
    import tempfile

    tmp = Path(tempfile.mkdtemp(prefix="workflow-controller-tb-3st-"))
    project_dir = tmp / "project"
    project_dir.mkdir(parents=True, exist_ok=True)
    xdg = tmp / "xdg"
    xdg.mkdir(parents=True, exist_ok=True)
    os.environ["XDG_DATA_HOME"] = str(xdg)
    logger.info(f"  tmp    : {tmp}")
    return tmp, project_dir, xdg


async def _create_origin_session(orch, project_dir: Path):
    """创建 worker session 并推进到第 2 轮（模拟进行中的 workflow）。"""
    from workflow_controller.config.settings import SessionConfig

    session = await orch.pool.create(
        SessionConfig(name="worker", role="worker", cwd=str(project_dir))
    )
    session.current_iteration = 2
    return session


async def _mount_demo_app(project_dir: Path, tmp: Path):
    """进程内起 Orchestrator + agent_traceback 插件 + REST 路由。"""
    from workflow_controller.config.settings import (
        AppConfig,
        HookServerConfig,
        ReviewerConfig,
    )
    from workflow_controller.core.orchestrator import Orchestrator

    # AppConfig 不设 traceback 配置 → traceback 由 agent_traceback 插件控制。
    cfg = AppConfig(
        project_dir=str(project_dir),
        hook_server=HookServerConfig(enabled=False),
        reviewer=ReviewerConfig(enabled=False),
    )
    orch = Orchestrator(cfg, default_project_dir=tmp / "test.db")
    await orch.start()
    setattr(
        orch, "_activate_session", AsyncMock()
    )  # 不 spawn driver    # hook_server 关闭 → plugin registry 不自动 load；手动安装 agent_traceback 插件。
    tb_plugin = attach_traceback_plugin(orch, HERE, project_dir, snapshot_timeout_seconds=10.0)
    getattr(orch.plugin_registry, "_plugins").append(tb_plugin)
    app = mount_real_app(orch, HERE / "workflow-controller.yaml")
    return orch, app, tb_plugin


async def _flush_events() -> None:
    """等事件总线排空（检查点异步落盘）。"""
    import asyncio

    for _ in range(20):
        await asyncio.sleep(0.01)


async def _start_and_checkpoint(orch, tb_plugin, origin_sid, sub_id, agent_type, cp_index):
    """on_start 一名 subagent 并读取检查点；返回 (该检查点, in-flight 数, 检查点总数)。"""
    await getattr(orch, "_subagents").on_start(
        origin_sid, {"agent_id": sub_id, "agent_type": agent_type}
    )
    await _flush_events()
    inflight = len(orch.subagent_status(origin_sid))
    cps = getattr(tb_plugin, "_cp_store_for_session")(origin_sid).list_sync(origin_sid)
    cp = cps[cp_index] if len(cps) > cp_index else None
    return cp, inflight, len(cps)


async def _stop_subagent(orch, origin_sid, sub_id, agent_type, content) -> None:
    """on_stop 一名 subagent（token 数按演示固定 10）。"""
    await getattr(orch, "_subagents").on_stop(
        origin_sid,
        {
            "agent_id": sub_id,
            "agent_type": agent_type,
            "content": content,
            "total_tokens": 10,
        },
    )


async def _phase_design(orch, tb_plugin, origin_sid: str, project_dir: Path):
    """阶段一（设计，并发）：两名 designer 的 start 检查点 + 产物 + 双 stop。"""
    # 阶段一（设计，并发）：designer_api start → cp#1（磁盘空）
    cp1, inflight_after_da, _ = await _start_and_checkpoint(
        orch, tb_plugin, origin_sid, API_SUB_ID, DESIGNER_API_TYPE, 0
    )
    # designer_api 写 design_api.md（designer_api 仍在飞）
    (project_dir / "design_api.md").write_text(DESIGN_API_MD)
    # designer_impl start → cp#2（design_api.md 在、design_impl.md 不在；并发 in-flight=2）
    cp2, inflight_concurrent, _ = await _start_and_checkpoint(
        orch, tb_plugin, origin_sid, IMPL_DESIGN_SUB_ID, DESIGNER_IMPL_TYPE, 1
    )
    # designer_impl 写 design_impl.md
    (project_dir / "design_impl.md").write_text(DESIGN_IMPL_MD)
    # 两个设计 subagent 都停止（结束阶段一 in-flight）
    await _stop_subagent(
        orch, origin_sid, API_SUB_ID, DESIGNER_API_TYPE, "designer_api wrote design_api.md"
    )
    await _stop_subagent(
        orch,
        origin_sid,
        IMPL_DESIGN_SUB_ID,
        DESIGNER_IMPL_TYPE,
        "designer_impl wrote design_impl.md",
    )
    return cp1, cp2, inflight_after_da, inflight_concurrent


async def _dispatch_stage(
    orch,
    tb_plugin,
    origin_sid,
    project_dir,
    sub_id,
    agent_type,
    cp_index,
    file_name,
    content,
    stop_content,
):
    """单个阶段的通用节奏：start 检查点 → 写产物 → stop；返回 (该阶段检查点, 检查点总数)。"""
    cp, _inflight, cp_total = await _start_and_checkpoint(
        orch, tb_plugin, origin_sid, sub_id, agent_type, cp_index
    )
    (project_dir / file_name).write_text(content)
    await _stop_subagent(orch, origin_sid, sub_id, agent_type, stop_content)
    return cp, cp_total


def _verify_phase1(
    tb_plugin,
    project_dir: Path,
    origin_sid: str,
    cps: list,
    cp_total: int,
    inflight_after_da: int,
    inflight_concurrent: int,
) -> tuple[bool, bool, bool]:
    """Phase 1 断言：trailer 结构、排除目录、四个检查点字段与并发计数。"""
    from workflow_controller.plugins.agent_traceback.snapshot_engine import _run_sync

    cp1, cp2, cp3, cp4 = cps
    eng = getattr(tb_plugin, "_resolve_engine")(str(project_dir))
    log1 = _run_sync(["git", "log", "--format=%B", "-1"], cwd=eng.snapshot_dir)
    trailer_ok = (
        "workflow-controller snapshot" in log1.stdout and f"session: {origin_sid}" in log1.stdout
    )
    exclude_ok = not (eng.working_dir / ".workflow-controller" / "junk.txt").exists()
    p1 = (
        cp_total == 4
        and cp1.status == "ok"
        and cp1.git_ref
        and cp1.agent_id == API_SUB_ID
        and cp1.trigger == "subagent_start"
        and cp1.parent_checkpoint_id is None
        and cp2.status == "ok"
        and cp2.git_ref
        and cp2.agent_id == IMPL_DESIGN_SUB_ID
        and cp2.trigger == "subagent_start"
        and cp2.parent_checkpoint_id == cp1.checkpoint_id
        and cp3.status == "ok"
        and cp3.git_ref
        and cp3.agent_id == DEV_SUB_ID
        and cp3.trigger == "subagent_start"
        and cp3.parent_checkpoint_id == cp2.checkpoint_id
        and cp4.status == "ok"
        and cp4.git_ref
        and cp4.agent_id == TESTER_SUB_ID
        and cp4.trigger == "subagent_start"
        and cp4.parent_checkpoint_id == cp3.checkpoint_id
        and trailer_ok
        and exclude_ok
        and inflight_concurrent == 2  # 并发：两个设计 subagent 同时在飞
        and inflight_after_da == 1
    )
    return p1, trailer_ok, exclude_ok


def _log_phase1(cps: list, trailer_ok: bool, exclude_ok: bool, inflight_concurrent: int) -> None:
    """打印 Phase 1 结果。"""
    cp1, cp2, cp3, cp4 = cps
    logger.info(
        f"  阶段一(设计,并发): cp#1(designer_api, ref={(cp1.git_ref or '')[:8] if cp1 else '?'})"
    )
    logger.info(
        f"                   cp#2(designer_impl, ref={(cp2.git_ref or '')[:8] if cp2 else '?'})  并发 in-flight=2"
    )
    logger.info(
        f"  阶段二(开发)    : cp#3(developer,    ref={(cp3.git_ref or '')[:8] if cp3 else '?'})"
    )
    logger.info(
        f"  阶段三(测试)    : cp#4(tester,       ref={(cp4.git_ref or '')[:8] if cp4 else '?'})"
    )
    parent_chain_ok = (
        cp3
        and cp3.parent_checkpoint_id == cp2.checkpoint_id
        and cp4
        and cp4.parent_checkpoint_id == cp3.checkpoint_id
    )
    logger.info(
        f"  parent 链 cp1←None, cp2←cp1, cp3←cp2, cp4←cp3 : {'✓' if parent_chain_ok else '✗'}"
    )
    logger.info(
        f"  并发 in-flight = 2（两个设计 subagent 同时在飞）: {'✓' if inflight_concurrent == 2 else '✗'}"
    )
    logger.info(
        f"  排除 .workflow-controller / trailer 结构化        : {'✓' if exclude_ok and trailer_ok else '✗'}"
    )


async def _run_phase1(orch, tb_plugin, origin_sid: str, project_dir: Path):
    """Phase 1：三阶段 + 四个检查点；返回 ([cp1..cp4], p1)。"""
    banner("[Phase 1] 三阶段：阶段一(设计,并发) → 阶段二(开发) → 阶段三(测试)，每 start 打一检查点")
    (project_dir / ".gitignore").write_text(
        "design_api.md\ndesign_impl.md\nimpl.py\ntest.py\nsummary.md\n"
    )
    (project_dir / ".workflow-controller").mkdir(exist_ok=True)
    (project_dir / ".workflow-controller" / "junk.txt").write_text("ignored")
    cp1, cp2, inflight_after_da, inflight_concurrent = await _phase_design(
        orch, tb_plugin, origin_sid, project_dir
    )
    # 阶段二（开发）：developer start → cp#3（两份 design 在、impl.py 不在）
    cp3, _ = await _dispatch_stage(
        orch,
        tb_plugin,
        origin_sid,
        project_dir,
        DEV_SUB_ID,
        DEVELOPER_TYPE,
        2,
        "impl.py",
        IMPL_PY,
        "developer wrote impl.py",
    )
    # 阶段三（测试）：tester start → cp#4（design + impl.py 在、test.py 不在）
    cp4, cp_total = await _dispatch_stage(
        orch,
        tb_plugin,
        origin_sid,
        project_dir,
        TESTER_SUB_ID,
        TESTER_TYPE,
        3,
        "test.py",
        TEST_PY,
        "tester wrote test.py",
    )
    await _flush_events()
    cps = [cp1, cp2, cp3, cp4]
    p1, trailer_ok, exclude_ok = _verify_phase1(
        tb_plugin, project_dir, origin_sid, cps, cp_total, inflight_after_da, inflight_concurrent
    )
    _log_phase1(cps, trailer_ok, exclude_ok, inflight_concurrent)
    return cps, p1


async def _run_phase2(app, origin_sid: str, cp3) -> bool:
    """Phase 2：GET /checkpoints/{cp3}/files 预览检查点 #3 文件态。"""
    banner(
        "[Phase 2] GET /checkpoints/{cp3}/files → 预览检查点 #3 文件态（两份 design 在、impl.py 不在）"
    )
    files = await _http_get(
        app,
        f"/api/sessions/{origin_sid}/checkpoints/{cp3.checkpoint_id}/files",
    )
    entries = files.get("entries", [])
    names = {e["name"]: e for e in entries}
    da_entry = names.get("design_api.md")
    # 读 design_api.md 内容
    da_content = await _http_get(
        app,
        f"/api/sessions/{origin_sid}/checkpoints/{cp3.checkpoint_id}/file?path=design_api.md",
    )
    p2 = (
        da_entry is not None
        and da_entry["is_dir"] is False
        and "design_impl.md" in names  # 另一份 design 也在
        and "impl.py" not in names  # developer 还没跑 → impl.py 不在
        and "test.py" not in names
        and "summary.md" not in names
        and da_content.get("content") == DESIGN_API_MD
        and da_content.get("truncated") is False
    )
    logger.info(f"  检查点 #3 文件 : {[e['name'] for e in entries]}")
    logger.info(f"  两份 design 在 / impl.py 不在       : {'✓' if p2 else '✗'}")
    logger.info(
        f"  读 design_api.md 内容一致          : {'✓' if da_content.get('content') == DESIGN_API_MD else '✗'}"
    )
    logger.info("  → 重放前即可审视那一刻磁盘，决定是否 / 从哪重放")
    return p2


def _seed_origin_db(xdg: Path, project_dir: Path) -> None:
    """构造假 opencode.db：主 session + part 流（按叙事顺序登记后一次性落盘）。"""
    # p5 是 developer 的 task part（完成，output 含 DEV_SUB_ID）→ re-call developer 排除它及之后全部。
    # p2/p3 是两份设计的 task part（保留）→ 主 agent 知道设计完成，只重新派 developer。
    # p1b 是 read tool part（含 project_dir 绝对路径）→ 验证 seed 把 origin cwd 路径 rewrite 为 fork cwd
    # （否则 resume 后主 agent 沿用旧绝对路径 → subagent 写文件到源目录而非 fork cwd → 快照缺新文件）。
    origin_db = DemoOpencodeDb(xdg)
    origin_db.session(ORIGIN_SDK_SESSION, directory=str(project_dir))
    origin_db.text_part(
        ORIGIN_SDK_SESSION, "p1", 1, "阶段一：并发派 designer_api 和 designer_impl 做设计"
    )
    origin_db.read_part(ORIGIN_SDK_SESSION, "p1b", 1, str(project_dir / "design_api.md"))
    origin_db.task_part(ORIGIN_SDK_SESSION, "p2", 2, API_SUB_ID)  # designer_api 完成（保留）
    origin_db.task_part(
        ORIGIN_SDK_SESSION, "p3", 3, IMPL_DESIGN_SUB_ID
    )  # designer_impl 完成（保留）
    origin_db.text_part(ORIGIN_SDK_SESSION, "p4", 4, "阶段二：派 developer 按 design 实现 impl.py")
    origin_db.task_part(ORIGIN_SDK_SESSION, "p5", 5, DEV_SUB_ID)  # developer 完成（被排除）
    origin_db.text_part(ORIGIN_SDK_SESSION, "p6", 6, "阶段三：派 tester 测试")
    origin_db.task_part(ORIGIN_SDK_SESSION, "p7", 7, TESTER_SUB_ID)  # tester 完成（被排除）
    origin_db.text_part(ORIGIN_SDK_SESSION, "p8", 8, "写 summary")
    origin_db.user_message(ORIGIN_SDK_SESSION, "p1", 1)
    origin_db.assistant_message(ORIGIN_SDK_SESSION, "p1b", 1, parent="m_p1")
    origin_db.assistant_message(ORIGIN_SDK_SESSION, "p2", 2, parent="m_p1b")
    origin_db.assistant_message(ORIGIN_SDK_SESSION, "p3", 3, parent="m_p2")
    origin_db.assistant_message(ORIGIN_SDK_SESSION, "p4", 4, parent="m_p3")
    origin_db.assistant_message(ORIGIN_SDK_SESSION, "p5", 5, parent="m_p4")
    origin_db.assistant_message(ORIGIN_SDK_SESSION, "p6", 6, parent="m_p5")
    origin_db.assistant_message(ORIGIN_SDK_SESSION, "p7", 7, parent="m_p6")
    origin_db.assistant_message(ORIGIN_SDK_SESSION, "p8", 8, parent="m_p7")
    origin_db.write()


def _fork_rows_point_at_fork_cwd(rows: list, origin_abs: str, fork_abs: str) -> bool:
    """seeded read part 行的路径检查：残留 origin 绝对路径即 False，见 fork 路径即 True。"""
    ok = False
    for (raw,) in rows:
        if origin_abs in raw:
            return False
        if fork_abs in raw:
            ok = True
    return ok


def _check_replay_isolation(
    fork_cwd: Path | None, project_dir: Path, resume_sid: str
) -> tuple[bool, bool, bool]:
    """验证 seed 路径 rewrite、fork db 隔离与兄弟 tempdir 隔离。"""
    import sqlite3 as _sq

    seeded_read_ok = False
    fork_db_isolated = False
    sibling_tempdir_safe = False
    if resume_sid and fork_cwd is not None:
        # fork 独立 db（透明重放：隔离 + 嵌套友好）
        fork_db = fork_cwd / ".workflow-controller" / "opencode" / "opencode.db"
        if fork_db.is_file():
            _con = _sq.connect(str(fork_db))
            _rows = _con.execute(
                "SELECT data FROM part WHERE session_id = ? AND data LIKE '%filePath%'",
                (resume_sid,),
            ).fetchall()
            # 隔离断言：fork db 只含本 fork 的 seeded session（不含 origin / 兄弟）
            _all_sids = set()
            for r in _con.execute(
                "SELECT id FROM session",
            ).fetchall():
                _all_sids.add(r[0])
            _con.close()
            seeded_read_ok = _fork_rows_point_at_fork_cwd(
                _rows, str(project_dir / "design_api.md"), str(fork_cwd / "design_api.md")
            )
            # fork db 仅含本 fork 的 seeded session 行（不含 origin 的 ses_orig_main）
            fork_db_isolated = _all_sids == {resume_sid}
        # 兄弟 tempdir 隔离：fork_cwd 的父目录下只有 cwd/（无兄弟 workflow-controller-tb-*）
        if fork_cwd.parent.is_dir():
            siblings = [p.name for p in fork_cwd.parent.iterdir() if p.name != ".DS_Store"]
            sibling_tempdir_safe = siblings == ["cwd"]
    return seeded_read_ok, fork_db_isolated, sibling_tempdir_safe


def _check_prompt_transparency(fork) -> tuple[bool, bool]:
    """透明重放 prompt 断言：首轮 prompt = 通用「请继续你之前的工作。」，不含任何重放语义。"""
    # 禁词表压成空格分隔字符串（豁免说明见下行 noqa），与 tests 的逐项元组区分
    _prompt = fork.config.initial_prompt or "" if fork else ""
    _leak_terms = (  # noqa: SIM905 —— 禁词表用空格分隔字符串，避免逐项枚举
        "重放 replay checkpoint fork 上次调用 前次产物 磁盘已回到 重新发起 不在盘上"
        " 最终状态 恢复 移除"
    ).split()
    _no_leak = all(t not in _prompt.lower() for t in _leak_terms)
    _is_continue = "请继续你之前的工作" in _prompt
    return _no_leak, _is_continue


class _Phase3Facts(NamedTuple):
    """Phase 3 断言输入：重放响应 + 会话/路径事实 + 隔离与透明性检查结论。"""

    tb: dict
    fork: object
    fork_cwd: Path | None
    origin_sid: str
    cp3: object
    project_dir: Path
    is_continue: bool
    no_leak: bool
    seeded_read_ok: bool
    fork_db_isolated: bool
    sibling_tempdir_safe: bool


def _verify_phase3(facts: _Phase3Facts) -> bool:
    """Phase 3 断言：seed resume、文件态恢复、溯源、隔离与 prompt 透明。"""
    tb = facts.tb
    fork = facts.fork
    fork_cwd = facts.fork_cwd
    origin_sid = facts.origin_sid
    cp3 = facts.cp3
    project_dir = facts.project_dir
    _is_continue = facts.is_continue
    _no_leak = facts.no_leak
    seeded_read_ok = facts.seeded_read_ok
    fork_db_isolated = facts.fork_db_isolated
    sibling_tempdir_safe = facts.sibling_tempdir_safe
    fork_bt = (
        (fork.plugin_metadata or {}).get("agent_traceback", {}).get("birth_traceback")
        if fork
        else None
    )
    return (
        tb.get("ok") is True
        and tb.get("resume_native_session_id")  # seed 预制 session + resume
        and fork is not None
        and fork_bt
        == {
            "origin_session_id": origin_sid,
            "origin_checkpoint_id": cp3.checkpoint_id,
            "forked_at": fork_bt["forked_at"],
            "fork_root": fork_bt["fork_root"],
            "fork_cwd": fork_bt["fork_cwd"],
        }
        and fork.native_session_id == tb.get("resume_native_session_id")  # sdk id = 预制 session
        and getattr(fork, "_resume_next", False) is True  # 首轮 --resume
        and _is_continue  # 透明续跑信号
        and _no_leak  # 无重放语义泄漏
        and fork_cwd is not None
        and (fork_cwd / "design_api.md").read_text() == DESIGN_API_MD  # 文件态恢复两份 design
        and (fork_cwd / "design_impl.md").read_text() == DESIGN_IMPL_MD
        and not (fork_cwd / "impl.py").exists()  # developer 没跑 → impl.py 不在
        and not (fork_cwd / "test.py").exists()  # tester 也没跑
        and not (fork_cwd / ".workflow-controller" / "junk.txt").exists()  # 排除生效
        and seeded_read_ok  # seed rewrite origin→fork 路径
        and fork_db_isolated  # fork db 仅含本 fork session（隔离）
        and sibling_tempdir_safe  # fork_cwd 父目录只有 cwd/（无兄弟）
        and not str(fork_cwd).startswith(
            str(project_dir.resolve()) + "/"
        )  # fork cwd 在 origin 之外
    )


def _log_phase3(
    tb: dict,
    fork_cwd: Path | None,
    project_dir: Path,
    _is_continue: bool,
    _no_leak: bool,
    fork_db_isolated: bool,
    sibling_tempdir_safe: bool,
    seeded_read_ok: bool,
) -> None:
    """打印 Phase 3 结果。"""
    logger.info(
        f"  POST /traceback → replay_session={(tb.get('session_id') or '')[:12]}"
        f"  resume={(tb.get('resume_native_session_id') or '')[:12]}"
    )
    restored_designs = bool(
        fork_cwd
        and (fork_cwd / "design_api.md").exists()
        and (fork_cwd / "design_impl.md").exists()
    )
    logger.info(f"  恢复两份 design（设计产物）      : {'✓' if restored_designs else '✗'}")
    logger.info(
        f"  impl.py 不在（developer 还没跑）    : {'✓' if fork_cwd and not (fork_cwd / 'impl.py').exists() else '✗'}"
    )
    fork_outside_origin = bool(
        fork_cwd
        and str(fork_cwd) != str(Path(project_dir))
        and not str(fork_cwd).startswith(str(Path(project_dir).resolve()) + "/")
    )
    logger.info(f"  fork cwd 在 origin 之外（防推导）  : {'✓' if fork_outside_origin else '✗'}")
    logger.info(
        f"  透明续跑 prompt（无重放语义泄漏）  : {'✓' if _is_continue and _no_leak else '✗'}"
    )
    logger.info(f"  fork db 仅含本 fork session（隔离）: {'✓' if fork_db_isolated else '✗'}")
    logger.info(f"  fork 父目录只有 cwd/（无兄弟泄漏） : {'✓' if sibling_tempdir_safe else '✗'}")
    logger.info(f"  seed rewrite origin→fork 路径     : {'✓' if seeded_read_ok else '✗'}")
    logger.info("  → 重放到 developer 跑之前：re-call developer 移除其 task part 及之后全部")


async def _run_phase3(app, orch, session, origin_sid: str, xdg: Path, project_dir: Path, cp3):
    """Phase 3：POST /traceback 从 cp#3 重放；返回 (p3, fork_cwd, fork_sid)。"""
    # 从检查点 #3（developer 的 start）重放：re-call 语义移除 developer 的 task part
    # 及其之后全部 → 重放 session 里 developer 还没跑。
    banner("[Phase 3] POST /traceback 从 cp#3 重放 → 重放 session 里 developer 还没跑")
    session.loop_status = "no"
    session.stop_reason = "reviewer_complete"
    session.native_session_id = ORIGIN_SDK_SESSION
    _seed_origin_db(xdg, project_dir)
    tb = await _http_post(
        app,
        f"/api/sessions/{origin_sid}/traceback",
        {"checkpoint_id": cp3.checkpoint_id, "runtime": "opencode"},
    )
    fork_sid = tb.get("session_id", "")
    fork = orch.pool.get(fork_sid)
    fork_cwd = Path(tb["fork_cwd"]) if tb.get("fork_cwd") else None
    resume_sid = tb.get("resume_native_session_id", "")
    # 验证 seed 把 part data 里 origin cwd 绝对路径 rewrite 为 fork cwd
    # （防止 resume 后主 agent 沿用旧绝对路径把文件写到源目录而非 fork cwd）
    # 透明重放隔离：seeded session 写到 fork 独立 db（<fork_cwd>/.workflow-controller/opencode/opencode.db），
    # 不在宿主 db——worker 读不到 origin 的 L2「未来」。
    seeded_read_ok, fork_db_isolated, sibling_tempdir_safe = _check_replay_isolation(
        fork_cwd, project_dir, resume_sid
    )
    _no_leak, _is_continue = _check_prompt_transparency(fork)
    p3 = _verify_phase3(
        _Phase3Facts(
            tb=tb,
            fork=fork,
            fork_cwd=fork_cwd,
            origin_sid=origin_sid,
            cp3=cp3,
            project_dir=project_dir,
            is_continue=_is_continue,
            no_leak=_no_leak,
            seeded_read_ok=seeded_read_ok,
            fork_db_isolated=fork_db_isolated,
            sibling_tempdir_safe=sibling_tempdir_safe,
        )
    )
    _log_phase3(
        tb,
        fork_cwd,
        project_dir,
        _is_continue,
        _no_leak,
        fork_db_isolated,
        sibling_tempdir_safe,
        seeded_read_ok,
    )
    return p3, fork_cwd, fork_sid


async def _run_phase4(app, orch, fork_sid: str, agents_dir: Path) -> bool:
    """Phase 4：在重放 session 登记 developer 补充指令（developer 还没跑）。"""
    banner(
        "[Phase 4] 在重放 session 登记 developer 补充指令 → developer 系统 prompt 含指令（developer 还没跑）"
    )
    r_post = await _http_post(
        app,
        f"/api/sessions/{fork_sid}/subagent-hint",
        {"subagent_type": DEVELOPER_TYPE, "hint": HINT_TEXT},
    )
    r_get = await _http_get(app, f"/api/sessions/{fork_sid}/subagent-hint")
    hooks = orch.make_driver_hooks(fork_sid)
    hints_back = hooks.get_overlays()["subagent_hints"]
    agent = _effective_subagent_agent(DEVELOPER_TYPE, hints_back, agents_dir)
    prompt = agent["prompt"] if agent else ""
    from workflow_controller.core._subagent import _HINT_FOOTER, _HINT_HEADER

    p4 = (
        r_post == {"ok": True}
        and r_get.get("hints", {}).get(DEVELOPER_TYPE) == [HINT_TEXT]
        and hints_back.get(DEVELOPER_TYPE) == [HINT_TEXT]
        and agent is not None
        and all(m in prompt for m in MARKERS)  # hint 进了 developer 系统 prompt
        and "你是**开发 subagent**" in prompt  # 原 .md 正文保留
        and prompt.index("REPLAY_HINT_MARKER") < prompt.index("你是**开发 subagent**")  # hint 前置
        and _HINT_HEADER in prompt
        and _HINT_FOOTER in prompt
    )
    logger.info(f"  POST /subagent-hint → {r_post}  (replay session {fork_sid[:12]})")
    logger.info(
        f"  developer 系统 prompt 含指令标记 {'✓' if all(m in prompt for m in MARKERS) else '✗'}  "
        f"原正文保留 {'✓' if '你是**开发 subagent**' in prompt else '✗'}"
    )
    logger.info("  → developer 还没跑，下一轮它启动时补充指令已前置进其系统 prompt（非侵入、可逆）")
    logger.info("    （指令把 add 从加法拨转为乘法 a*b，引导重跑方向）")
    return p4


def _verify_phase5(cps: list, cp_final, final_names: set, origin_cp_ids: set) -> bool:
    """Phase 5 断言：最终检查点字段、文件齐全与跳回数据源。"""
    cp1, cp2, cp3, cp4 = cps
    return (
        cp_final is not None
        and cp_final.status == "ok"
        and cp_final.git_ref
        and cp_final.agent_id == ""
        and cp_final.trigger == "session_complete"
        and cp_final.parent_checkpoint_id == cp4.checkpoint_id
        and {"design_api.md", "design_impl.md", "impl.py", "test.py", "summary.md"} <= final_names
        and {
            cp1.checkpoint_id,
            cp2.checkpoint_id,
            cp3.checkpoint_id,
            cp4.checkpoint_id,
            cp_final.checkpoint_id,
        }
        <= origin_cp_ids
    )


async def _run_phase5(app, tb_plugin, origin_sid: str, project_dir: Path, cps: list) -> bool:
    """Phase 5：最终状态检查点 + 预览最终文件态 + 跳回。"""
    # 模拟原始 workflow 跑完（summary.md 已写）→ on_session_complete 打最终状态检查点。
    banner("[Phase 5] 模拟 session 完成 → 最终状态检查点；预览最终文件态；跳回 origin 检查点")
    cp1, cp2, cp3, cp4 = cps
    (project_dir / "summary.md").write_text(SUMMARY_MD)
    await getattr(tb_plugin, "_snapshot_session_complete")(origin_sid)
    await _flush_events()
    cps_now = getattr(tb_plugin, "_cp_store_for_session")(origin_sid).list_sync(origin_sid)
    cp_final = next((c for c in cps_now if c.trigger == "session_complete"), None)
    # 预览最终状态文件态（design×2 + impl.py + test.py + summary.md 齐全）
    final_files = (
        await _http_get(
            app,
            f"/api/sessions/{origin_sid}/checkpoints/{cp_final.checkpoint_id}/files",
        )
        if cp_final
        else {"entries": []}
    )
    final_names = {e["name"] for e in final_files.get("entries", [])}
    # 跳回：从重放 session 读 origin 的检查点列表（jump-back 数据源）
    origin_cps_via_api = await _http_get(app, f"/api/sessions/{origin_sid}/checkpoints")
    origin_cp_ids = {c["checkpoint_id"] for c in origin_cps_via_api.get("checkpoints", [])}
    p5 = _verify_phase5(cps, cp_final, final_names, origin_cp_ids)
    logger.info(
        f"  最终状态检查点 cp#{cp_final.iteration} "
        f"(trigger=session_complete, ref={(cp_final.git_ref or '')[:8] if cp_final else '?'})"
    )
    logger.info(f"  最终文件态 : {sorted(final_names)}")
    all_cps_in_origin = {
        cp1.checkpoint_id,
        cp2.checkpoint_id,
        cp3.checkpoint_id,
        cp4.checkpoint_id,
        cp_final.checkpoint_id,
    } <= origin_cp_ids
    logger.info(
        f"  跳回 origin 检查点列表含 cp1/cp2/cp3/cp4/cp_final : {'✓' if all_cps_in_origin else '✗'}"
    )
    logger.info("  → 重放不满意时跳回原始最终状态或任一检查点重新重放")
    return p5


def _log_summary(results: dict[str, bool]) -> None:
    """打印各 Phase 结果汇总。"""
    logger.info(f"\n{'═' * 64}")
    labels = [
        ("Phase 1  三阶段(设计并发/开发/测试) + 四检查点 + parent 链", "p1_three_stages_cps"),
        ("Phase 2  预览检查点 #3 文件态（两份 design 在 / impl.py 不在）", "p2_preview_files"),
        ("Phase 3  重放到 developer 跑之前（re-call developer）", "p3_replay_before_developer"),
        (
            "Phase 4  注入 developer 指令 → 系统 prompt（developer 还没跑）",
            "p4_hint_inject_before_run",
        ),
        ("Phase 5  最终状态检查点 + 跳回 origin 检查点", "p5_final_state_jumpback"),
    ]
    for label, key in labels:
        logger.info(f"  {label} : {'✓' if results[key] else '✗'}")
    logger.info(f"{'═' * 64}")


async def _async_main() -> int:
    if shutil.which("git") is None:
        logger.info("git not installed — this demo requires git for file-state snapshots.")
        return 1

    logging.getLogger("workflow_controller").setLevel(logging.CRITICAL)
    _print_demo_header()
    tmp, project_dir, xdg = _make_sandbox()
    agents_dir = seed_agent_defs(HERE)
    orch, app, tb_plugin = await _mount_demo_app(project_dir, tmp)

    fork_cwd: Path | None = None  # tracked for cleanup (now outside tmp)
    try:
        results: dict[str, bool] = {}
        session = await _create_origin_session(orch, project_dir)
        origin_sid = session.id

        # ── Phase 1: 三阶段 + 四个检查点 ───────────────────────────────────
        # 检查点在每个 subagent **start 时刻**打（捕获该 subagent 开始工作前的磁盘文件态）。
        cps, p1 = await _run_phase1(orch, tb_plugin, origin_sid, project_dir)
        results["p1_three_stages_cps"] = p1

        # ── Phase 2: 预览文件态 → 决定从哪重放 ────────────────────────────
        p2 = await _run_phase2(app, origin_sid, cps[2])
        results["p2_preview_files"] = p2

        # ── Phase 3: 重放到 developer 跑之前 ───────────────────────────────
        p3, fork_cwd, fork_sid = await _run_phase3(
            app, orch, session, origin_sid, xdg, project_dir, cps[2]
        )
        results["p3_replay_before_developer"] = p3

        # ── Phase 4: 注入 subagent 指令 → developer 系统 prompt（developer 还没跑）──
        p4 = await _run_phase4(app, orch, fork_sid, agents_dir)
        results["p4_hint_inject_before_run"] = p4

        # ── Phase 5: 最终状态检查点 + 跳回 ────────────────────────────────
        p5 = await _run_phase5(app, tb_plugin, origin_sid, project_dir, cps)
        results["p5_final_state_jumpback"] = p5

        # ── 汇总 ─────────────────────────────────────────────────────────
        _log_summary(results)
        return 0 if all(results.values()) else 2
    finally:
        await orch.shutdown()
        # 透明重放布局：fork_root/cwd/——fork_cwd 是 fork_root 的子目录。清 fork_root
        # （父）才能把 cwd/ + .workflow-controller/ 一起清干净。fork_root 在系统 tempdir，不在 tmp 下。
        if fork_cwd is not None:
            shutil.rmtree(fork_cwd.parent, ignore_errors=True)
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    configure_demo_logging(logger)
    import asyncio

    try:
        return asyncio.run(_async_main())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
