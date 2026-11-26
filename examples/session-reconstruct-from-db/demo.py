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

"""从 cannbot session db 重建会话 + 检查点重放 —— 综合测试。

故事线——「生成 db → 导入重建 → 预览文件态 → 重放到 developer 跑之前
→ 注入 developer 指令 → 跳回」：

  * **Phase 1（从 db 导入重建）**：``POST /api/sessions/import-db`` 把合成
    cannbot db 导入 → 重建出带检查点的 Workflow Controller session（含对话态 + 文件态 +
    项目 ``.git`` 历史）。
  * **Phase 2（重建产物同构校验）**：4 检查点（3 ``subagent_start`` + 1
    ``session_complete``）+ parent 链 + 文件态逐字节 + 项目 ``.git`` 历史 +
    L2 可消费性 + 合成历史事件 + birth_traceback 溯源。
  * **Phase 3（重放：复用 TracebackRunner）**：从 cp#2（developer start）
    重放 → re-call developer 移除其 task part 及之后 → developer 还没跑。
  * **Phase 4（注入 subagent 指令 → developer 系统 prompt）**：登记 developer
    补充指令 → developer 系统 prompt 含指令。
  * **Phase 5（跳回原始检查点）**：从重放 session 读 origin 检查点列表。

两种模式::

    python demo.py          # 确定性（CI 友好，无需 LLM / opencode）
    python demo.py --live   # 真实 AI agent（需 opencode + API key + provider）

确定性模式在进程内起真实 Orchestrator + REST 路由（httpx ASGITransport），
mock ``_activate_session``（不 spawn driver）。``--live`` 模式不 mock，
fork session 被真实 opencode driver 激活，worker 用真实 LLM 续跑。

前置：Workflow Controller 已装（``pip install -e .``）+ git 已装。``--live`` 另需 opencode
CLI + provider 配置。
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock

HERE = Path(__file__).resolve().parent

sys.path.insert(0, str(HERE))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))

from gen_db import (
    DESIGN_MD,
    DEVELOPER_TYPE,
    HINT_TEXT,
    MARKERS,
    ORIGIN_SDK,
    S1,
    S2,
    S3,
    build_cannbot_db,
)
from opencode_demo_lib import (
    SourceOpencodeSchemaDb,
    attach_traceback_plugin,
    configure_demo_logging,
    demo_banner,
    demo_http_get,
    demo_http_post,
    mount_real_app,
    resolve_effective_subagent_agent,
    seed_agent_defs,
)

logger = logging.getLogger(__name__)


def banner(title: str) -> None:
    demo_banner(logger, title)


def _git(cwd: Path, *args: str) -> str:
    import subprocess

    r = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=10.0,
    )
    return r.stdout.strip()


def _print_demo_header(live: bool) -> None:
    """打印 demo 头部说明（模式与任务概览）。"""
    mode_label = "live（真实 AI agent）" if live else "deterministic（CI 友好，无需 LLM）"
    banner(f"从 cannbot db 重建会话 + 检查点重放 ({mode_label})")
    logger.info(f"  config : {HERE / 'workflow-controller.yaml'}  (三阶段: 设计 / 开发 / 测试)")
    logger.info(f"  mode   : {mode_label}")
    logger.info("  task   : designer → developer → tester（三阶段，每 stage 一检查点）")
    logger.info(f"  hint   : {HINT_TEXT[:40]}...")


def _make_sandbox() -> tuple[Path, Path, Path]:
    """建临时 project/xdg 目录并设 XDG_DATA_HOME；返回 (tmp, project, xdg)。"""
    import tempfile

    tmp = Path(tempfile.mkdtemp(prefix="workflow-controller-recon-"))
    project_dir = tmp / "project"  # target_cwd（重建目标目录）
    project_dir.mkdir(parents=True, exist_ok=True)
    xdg = tmp / "xdg"
    xdg.mkdir(parents=True, exist_ok=True)
    os.environ["XDG_DATA_HOME"] = str(xdg)
    logger.info(f"  tmp    : {tmp}")
    return tmp, project_dir, xdg


def _select_source_db(tmp: Path, live: bool) -> Path:
    """确定性模式用最小 source db；live 模式用宿主真实 opencode.db。"""
    source_db = SourceOpencodeSchemaDb(tmp).build()
    if live:
        host_db = Path.home() / ".local" / "share" / "opencode" / "opencode.db"
        if host_db.is_file():
            source_db = host_db
            logger.info(
                f"  source : {host_db} (host opencode.db, ~{host_db.stat().st_size // (1024 * 1024)}MB)"
            )
    return source_db


def _seed_project_agents(project_dir: Path, live: bool) -> Path:
    """铺 agent 定义与 workflow-controller.yaml；返回默认 agents 目录。"""
    # live 模式：把 agent 定义放入 target_cwd/.opencode/agents/——
    # 快照仓镜像时含这些文件，fork_cwd restore 后可被 opencode driver 发现。
    if live:
        seed_agent_defs(HERE, project_dir)
    agents_dir = seed_agent_defs(HERE)
    # 把 workflow-controller.yaml 复制到 project_dir——重建时从 target_cwd/workflow-controller.yaml 读
    # system_prompt / stages / loop / starting_prompt，让 fork worker 继承
    # 完整 workflow 上下文。
    shutil.copy2(HERE / "workflow-controller.yaml", project_dir / "workflow-controller.yaml")
    return agents_dir


async def _mount_demo_app(project_dir: Path, tmp: Path, source_db: Path, live: bool):
    """进程内起 Orchestrator + agent_traceback 插件 + REST 路由。"""
    from workflow_controller.config.settings import (
        AppConfig,
        HookServerConfig,
        ReviewerConfig,
    )
    from workflow_controller.core.orchestrator import Orchestrator

    # AppConfig：live 模式开 hook_server（opencode driver 的 hook 回调通道）。
    hook_cfg = HookServerConfig(enabled=live)
    cfg = AppConfig(
        project_dir=str(project_dir),
        hook_server=hook_cfg,
        reviewer=ReviewerConfig(enabled=False),
    )
    orch = Orchestrator(
        cfg,
        default_project_dir=tmp / "test.db",
        hook_server_port_override=18790 if live else None,
    )
    await orch.start()
    if not live:
        setattr(orch, "_activate_session", AsyncMock())  # 不 spawn driver
    tb_plugin = attach_traceback_plugin(
        orch,
        HERE,
        project_dir,
        snapshot_timeout_seconds=30.0,
        reconstruct={
            "enabled": True,
            "bash_timeout_seconds": 30,
            "bash_replay_mode": "git_and_file_ops",
            "git_hash_fidelity": True,
            "source_opencode_db": str(source_db),
        },
    )
    # live 模式下 orch.start() 已从 config 加载了一个 agent_traceback 插件实例。
    # 移除它，只用手动构造的 tb_plugin。
    plugins = getattr(orch.plugin_registry, "_plugins")
    existing = [p for p in plugins if p.config.name == "agent_traceback"]
    for p in existing:
        plugins.remove(p)
    plugins.append(tb_plugin)
    app = mount_real_app(orch, HERE / "workflow-controller.yaml")
    return orch, app, tb_plugin


async def _run_phase1(app, cannbot_db: Path, project_dir: Path):
    """Phase 1：POST /api/sessions/import-db 导入重建；返回 (p1, recon_sid, recon_sdk, cps)。"""
    banner(
        "[Phase 1] POST /api/sessions/import-db → 从 cannbot db 重建 Workflow Controller session"
    )
    imp = await demo_http_post(
        app,
        "/api/sessions/import-db",
        {
            "db_path": str(cannbot_db),
            "target_cwd": str(project_dir),
        },
    )
    recon_sid = imp.get("session_id", "")
    recon_sdk = imp.get("sdk_session_id", "")
    cps = imp.get("checkpoints", [])
    p1 = (
        imp.get("ok") is True
        and recon_sid
        and recon_sdk == ORIGIN_SDK
        and len(cps) == 4  # 3 subagent_start + 1 session_complete
        and Path(imp.get("fork_cwd", "")).resolve() == project_dir.resolve()
    )
    logger.info(
        f"  import result : ok={imp.get('ok')} session={recon_sid[:12]} sdk={recon_sdk[:16]}"
    )
    logger.info(f"  checkpoints   : {len(cps)} (triggers: {[c['trigger'] for c in cps]})")
    logger.info(f"  fork_cwd      : {imp.get('fork_cwd', '')[:40]}...")
    if imp.get("error"):
        logger.info(f"  ERROR         : {imp['error']}")
    return p1, recon_sid, recon_sdk, cps


def _check_checkpoint_chain(cps: list) -> bool:
    """2a. 检查点结构：trigger 序列、agent_id、parent 链与 main_part_id 预填。"""
    triggers = [c["trigger"] for c in cps]
    cp_chain_ok = (
        triggers == ["subagent_start", "subagent_start", "subagent_start", "session_complete"]
        and cps[0]["agent_id"] == S1
        and cps[1]["agent_id"] == S2
        and cps[2]["agent_id"] == S3
        and cps[3]["agent_id"] == ""
        and cps[0]["parent_checkpoint_id"] is None
        and cps[1]["parent_checkpoint_id"] == cps[0]["checkpoint_id"]
        and cps[2]["parent_checkpoint_id"] == cps[1]["checkpoint_id"]
        and cps[3]["parent_checkpoint_id"] == cps[2]["checkpoint_id"]
        and all(c["checkpoint_id"].startswith("cp_rc_") for c in cps)
        and all(c["main_part_id"] for c in cps[:3])  # 预填 main_part_id
    )
    logger.info(
        f"  检查点链 cp_rc_ 前缀 + parent 链 + main_part_id 预填: {'✓' if cp_chain_ok else '✗'}"
    )
    return cp_chain_ok


async def _check_file_states(app, recon_sid: str, cps: list) -> tuple[bool, bool]:
    """2b. 文件态逐字节（GET /checkpoints/{cp}/files）+ design.md 内容。"""
    cp_files = []
    for cp in cps:
        cp_files.append(
            await demo_http_get(
                app, f"/api/sessions/{recon_sid}/checkpoints/{cp['checkpoint_id']}/files"
            )
        )
    names = [{e["name"] for e in r.get("entries", [])} for r in cp_files]
    file_state_ok = (
        "design.md" not in names[0]  # cp#1: designer 未跑 → 空
        and "design.md" in names[1]  # cp#2: designer 已跑
        and "impl.py" not in names[1]  # developer 未跑
        and "design.md" in names[2]  # cp#3: developer 已跑
        and "impl.py" in names[2]
        and "test.py" not in names[2]  # tester 未跑
        and "design.md" in names[3]  # session_complete: 全齐
        and "impl.py" in names[3]
        and "test.py" in names[3]
    )
    # 读 design.md 内容
    da = await demo_http_get(
        app,
        f"/api/sessions/{recon_sid}/checkpoints/{cps[1]['checkpoint_id']}/file?path=design.md",
    )
    content_ok = da.get("content") == DESIGN_MD
    logger.info(
        f"  文件态逐字节 (cp#1空 → cp#2+design → cp#3+impl → complete+test): "
        f"{'✓' if file_state_ok else '✗'}"
    )
    logger.info(f"  design.md 内容一致 (cp#2): {'✓' if content_ok else '✗'}")
    return file_state_ok, content_ok


def _check_git_history(project_dir: Path) -> bool:
    """2c. 项目 .git 历史：三条提交且顺序正确。"""
    git_log = _git(project_dir, "log", "--format=%s")
    msgs = git_log.splitlines() if git_log else []
    git_ok = (
        msgs == ["add test", "init impl", "init design"]
        and int(_git(project_dir, "rev-list", "--count", "HEAD") or "0") == 3
    )
    logger.info(
        f"  项目 .git 历史 (3 commit: init design → init impl → add test): {'✓' if git_ok else '✗'}"
    )
    return git_ok


def _check_l2_consumability(recon_sdk: str, project_dir: Path) -> tuple[bool, str | None]:
    """2d. L2 可消费性（TracebackRunner 重放前的截断链路）。"""
    from workflow_controller.plugins.agent_traceback.l2_reader import (
        find_exclude_message_id,
        find_main_part_id_for_subagent,
        read_main_session_parts,
    )

    env_recon = {"XDG_DATA_HOME": str(project_dir / ".workflow-controller")}
    parts = read_main_session_parts(recon_sdk, env_recon)
    task_parts = [
        (pid, json.loads(raw))
        for pid, _tc, _mid, raw in parts
        if json.loads(raw).get("type") == "tool" and json.loads(raw).get("tool") == "task"
    ]
    task_pid_s2 = find_main_part_id_for_subagent(recon_sdk, S2, env_recon)
    excl_mid = find_exclude_message_id(recon_sdk, task_pid_s2, env_recon) if task_pid_s2 else None
    trunc = read_main_session_parts(recon_sdk, env_recon, exclude_message_id=excl_mid)
    task_pid_s1 = find_main_part_id_for_subagent(recon_sdk, S1, env_recon)
    l2_ok = (
        len(task_parts) == 3  # 3 个 task part
        and task_pid_s2 is not None
        and excl_mid is not None
        and task_pid_s2 not in {p[0] for p in trunc}  # task#2 被截断
        and task_pid_s1 in {p[0] for p in trunc}  # task#1 保留
    )
    logger.info(f"  L2 可消费性 (3 task part + message 级截断定位): {'✓' if l2_ok else '✗'}")
    return l2_ok, task_pid_s2


def _check_stored_events(orch, recon_sid: str, task_pid_s2: str | None) -> bool:
    """2e. 合成历史事件与 checkpoints 表（front-end「概览」tab 数据源）。"""
    from workflow_controller.plugins.agent_traceback.checkpoint_store import CheckpointStore

    events_db = orch.event_router.db_path_for(recon_sid)
    store = CheckpointStore(events_db)
    stored_cps = store.list_sync(recon_sid)
    cp_store_ok = len(stored_cps) == 4 and stored_cps[1].main_part_id == task_pid_s2
    # 事件：SUBAGENT pairs (3×2=6) + RESULT (1) + LOG_AUTO (1) = 8
    # best-effort: check sessions table has the recon session
    con_ev = sqlite3.connect(str(events_db))
    ev_count = con_ev.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    con_ev.close()
    events_ok = ev_count >= 8  # 至少 8 条事件
    logger.info(f"  checkpoints 表 (4 行, main_part_id 预填): {'✓' if cp_store_ok else '✗'}")
    logger.info(
        f"  合成历史事件 (≥8: 3 SUBAGENT pairs + RESULT + LOG_AUTO): "
        f"{'✓' if events_ok else '✗'} (got {ev_count})"
    )
    return cp_store_ok


def _check_birth_traceback(orch, recon_sid: str, cannbot_db: Path) -> bool:
    """2f. birth_traceback 溯源。"""
    recon_session = orch.pool.get(recon_sid)
    bt = (
        (recon_session.plugin_metadata or {}).get("agent_traceback", {}).get("birth_traceback")
        if recon_session
        else None
    )
    bt_ok = (
        recon_session is not None
        and recon_session.sdk_session_id == ORIGIN_SDK
        and recon_session.loop_status == "yes"
        and bt is not None
        and bt.get("reconstructed_from") == str(cannbot_db)
        and bt.get("origin_sdk_session_id") == ORIGIN_SDK
        and bt.get("checkpoints") == 4
    )
    logger.info(
        f"  birth_traceback 溯源 (reconstructed_from / sdk / loop_status=yes): "
        f"{'✓' if bt_ok else '✗'}"
    )
    return bt_ok


async def _run_phase2(
    app, orch, recon_sid: str, recon_sdk: str, cps: list, project_dir: Path, cannbot_db: Path
) -> bool:
    """Phase 2：重建产物同构校验。"""
    banner("[Phase 2] 校验：检查点链 + 文件态逐字节 + 项目 .git 历史 + L2 可消费性 + 合成事件")
    cp_chain_ok = _check_checkpoint_chain(cps)
    file_state_ok, content_ok = await _check_file_states(app, recon_sid, cps)
    git_ok = _check_git_history(project_dir)
    l2_ok, task_pid_s2 = _check_l2_consumability(recon_sdk, project_dir)
    cp_store_ok = _check_stored_events(orch, recon_sid, task_pid_s2)
    bt_ok = _check_birth_traceback(orch, recon_sid, cannbot_db)
    return (
        cp_chain_ok and file_state_ok and content_ok and git_ok and l2_ok and cp_store_ok and bt_ok
    )


def _check_prompt_transparency(fork) -> tuple[bool, bool]:
    """透明续跑 prompt 断言：通用「请继续你之前的工作。」、无重放语义。"""
    _prompt = fork.config.initial_prompt or "" if fork else ""
    _no_leak = all(
        t not in _prompt.lower() for t in ("重放", "replay", "checkpoint", "fork", "恢复", "移除")
    )
    _is_continue = "请继续你之前的工作" in _prompt
    return _no_leak, _is_continue


def _verify_phase3(
    tb: dict,
    fork,
    fork_cwd: Path | None,
    recon_sid: str,
    _is_continue: bool,
    _no_leak: bool,
) -> bool:
    """Phase 3 断言：seed resume、文件态恢复、溯源与 prompt 透明。"""
    fork_bt = (
        (fork.plugin_metadata or {}).get("agent_traceback", {}).get("birth_traceback")
        if fork
        else None
    )
    return (
        tb.get("ok") is True
        and tb.get("session_id")
        and tb.get("resume_sdk_session_id")
        and fork is not None
        and fork_cwd is not None
        and (fork_cwd / "design.md").read_text() == DESIGN_MD  # 文件态恢复
        and not (fork_cwd / "impl.py").exists()  # developer 没跑
        and not (fork_cwd / "test.py").exists()
        and _is_continue
        and _no_leak  # 透明续跑
        and fork_bt is not None
        and fork_bt.get("origin_session_id") == recon_sid
    )


def _log_phase3(tb: dict, fork_cwd: Path | None, _is_continue: bool, _no_leak: bool) -> None:
    """打印 Phase 3 结果。"""
    logger.info(
        f"  POST /traceback → fork={tb.get('session_id', '')[:12]}"
        f" resume={tb.get('resume_sdk_session_id', '')[:12]}"
    )
    logger.info(
        f"  恢复 design.md / impl.py 不在 (developer 还没跑): "
        f"{'✓' if fork_cwd and (fork_cwd / 'design.md').exists() and not (fork_cwd / 'impl.py').exists() else '✗'}"
    )
    logger.info(f"  透明续跑 prompt (无重放语义泄漏): {'✓' if _is_continue and _no_leak else '✗'}")


async def _run_live_agent(orch, fork_sid: str, fork_cwd: Path | None, fork) -> bool:
    """live 模式：等真实 developer subagent 写 impl.py 并汇报结果。"""
    import asyncio

    logger.info("\n  [live] 等待 developer subagent 写 impl.py (timeout=180s)...")
    deadline = time.time() + 180
    impl_written = False
    while time.time() < deadline:
        if (fork_cwd / "impl.py").exists():
            impl_written = True
            break
        if fork and fork.state.value == "idle" and fork.loop_status == "no":
            # session 已结束但 impl.py 未出现
            logger.info(
                f"  [live] session ended (state={fork.state.value}, "
                f"reason={fork.stop_reason}) — impl.py {'written' if impl_written else 'NOT written'}"
            )
            break
        await asyncio.sleep(2)
    if impl_written:
        return _report_live_impl(fork_cwd)
    return _report_live_failure(orch, fork_sid, fork_cwd, fork)


def _report_live_impl(fork_cwd: Path | None) -> bool:
    """live 模式 impl.py 已写出：汇报内容与 hint 痕迹。"""
    impl_content = (fork_cwd / "impl.py").read_text()
    has_add = "def add" in impl_content
    has_hint = any(m in impl_content for m in MARKERS)
    logger.info("  [live] impl.py written by real agent ✓")
    logger.info(f"  [live] contains def add: {'✓' if has_add else '✗'}")
    logger.info(
        f"  [live] hint markers (a*b): {'✓' if has_hint else '✗ (agent may not have followed hint)'}"
    )
    logger.info(f"  [live] impl.py content: {impl_content!r}")
    return has_add


def _report_live_failure(orch, fork_sid: str, fork_cwd: Path | None, fork) -> bool:
    """live 模式超时未写出 impl.py：dump 现场（文件、fork db、事件、session 状态）。"""
    logger.info("  [live] impl.py NOT written within timeout — agent may have taken different path")
    # Check what files exist in fork_cwd
    if fork_cwd:
        files = [f.name for f in fork_cwd.iterdir() if f.is_file()]
        logger.info(f"  [live] files in fork_cwd: {files}")
    # Debug: check fork db + events
    if fork_cwd and fork_cwd.is_dir():
        fork_db = fork_cwd / ".workflow-controller" / "opencode" / "opencode.db"
        if fork_db.is_file():
            _debug_dump_fork_db(fork_db)
        ev_db = orch.event_router.db_path_for(fork_sid)
        if ev_db.is_file():
            _debug_dump_events(ev_db, fork_sid)
    # Debug: session attributes
    if fork:
        logger.info(
            f"  [debug] session state={fork.state} loop={fork.loop_status} "
            f"stop_reason={fork.stop_reason} iter={fork.current_iteration}"
        )
    return False


def _debug_dump_fork_db(fork_db: Path) -> None:
    """dump fork 独立 db 的 session/part 概况。"""
    try:
        con = sqlite3.connect(str(fork_db))
        sess_count = con.execute("SELECT COUNT(*) FROM session").fetchone()[0]
        part_count = con.execute("SELECT COUNT(*) FROM part").fetchone()[0]
        logger.info(f"  [debug] fork db: {sess_count} sessions, {part_count} parts")
        if sess_count > 0:
            rows = con.execute("SELECT id, directory, title FROM session").fetchall()
            for r in rows:
                logger.info(f"  [debug] session: id={r[0][:16]} dir={r[1][:40]} title={r[2][:40]}")
        con.close()
    except Exception as e:
        logger.info(f"  [debug] fork db check failed: {e}")


def _debug_dump_events(ev_db: Path, fork_sid: str) -> None:
    """dump fork session 最近 10 条事件。"""
    try:
        con = sqlite3.connect(str(ev_db))
        ev_rows = con.execute(
            "SELECT event_type, data FROM events WHERE session_id = ? ORDER BY rowid DESC LIMIT 10",
            (fork_sid,),
        ).fetchall()
        logger.info(f"  [debug] last {len(ev_rows)} events:")
        for et, data in ev_rows:
            d = json.loads(data) if isinstance(data, str) else {}
            logger.info(f"  [debug]   {et}: {str(d)[:120]}")
        con.close()
    except Exception as e:
        logger.info(f"  [debug] events check failed: {e}")


async def _run_phase3(app, orch, recon_sid: str, cps: list, live: bool):
    """Phase 3：POST /traceback 从 cp#2 重放；返回 (p3, fork_cwd, fork_sid, p3_live)。"""
    banner(
        "[Phase 3] POST /traceback 从 cp#2 重放 → developer 还没跑"
        + ("（真实 AI agent 续跑）" if live else "")
    )

    # live 模式：replay 前在 origin 登记 developer 补充指令（fork 继承）
    if live:
        r_hint = await demo_http_post(
            app,
            f"/api/sessions/{recon_sid}/subagent-hint",
            {"subagent_type": DEVELOPER_TYPE, "hint": HINT_TEXT},
        )
        logger.info(f"  pre-replay hint registered on origin: {r_hint.get('ok')}")

    tb = await demo_http_post(
        app,
        f"/api/sessions/{recon_sid}/traceback",
        {"checkpoint_id": cps[1]["checkpoint_id"], "runtime": "opencode"},
    )
    fork_sid = tb.get("session_id", "")
    fork_cwd = Path(tb["fork_cwd"]) if tb.get("fork_cwd") else None
    fork = orch.pool.get(fork_sid)

    _no_leak, _is_continue = _check_prompt_transparency(fork)
    p3 = _verify_phase3(tb, fork, fork_cwd, recon_sid, _is_continue, _no_leak)
    _log_phase3(tb, fork_cwd, _is_continue, _no_leak)

    p3_live = None
    if live and p3:
        p3_live = await _run_live_agent(orch, fork_sid, fork_cwd, fork)
        p3 = p3 and p3_live
    return p3, fork_cwd, fork_sid, p3_live


async def _phase4_hint_check(app, orch, fork_sid: str, agents_dir: Path) -> bool:
    """确定性模式：replay 后在 fork session 登记 hint（developer 还没跑）。"""
    r_post = await demo_http_post(
        app,
        f"/api/sessions/{fork_sid}/subagent-hint",
        {"subagent_type": DEVELOPER_TYPE, "hint": HINT_TEXT},
    )
    r_get = await demo_http_get(app, f"/api/sessions/{fork_sid}/subagent-hint")
    hooks = orch.make_driver_hooks(fork_sid)
    hints_back = hooks.get_overlays()["subagent_hints"]
    agent = resolve_effective_subagent_agent(DEVELOPER_TYPE, hints_back, agents_dir)
    prompt = agent["prompt"] if agent else ""
    from workflow_controller.core._subagent import _HINT_FOOTER, _HINT_HEADER

    p4 = (
        r_post == {"ok": True}
        and r_get.get("hints", {}).get(DEVELOPER_TYPE) == [HINT_TEXT]
        and agent is not None
        and all(m in prompt for m in MARKERS)
        and "你是**开发 subagent**" in prompt  # 原 .md 正文保留
        and prompt.index("REPLAY_HINT_MARKER") < prompt.index("你是**开发 subagent**")
        and _HINT_HEADER in prompt
        and _HINT_FOOTER in prompt
    )
    logger.info(f"  POST /subagent-hint → {r_post}")
    logger.info(
        f"  developer 系统 prompt 含指令标记: {'✓' if all(m in prompt for m in MARKERS) else '✗'}"
    )
    logger.info(
        f"  原 .md 正文保留 + hint 前置: {'✓' if '你是**开发 subagent**' in prompt else '✗'}"
    )
    return p4


def _phase4_live_check(orch, fork_sid: str, agents_dir: Path) -> bool:
    """live 模式：hint 在 replay 前已登记在 origin，fork 继承。"""
    hooks = orch.make_driver_hooks(fork_sid)
    hints_back = hooks.get_overlays()["subagent_hints"]
    agent = resolve_effective_subagent_agent(DEVELOPER_TYPE, hints_back, agents_dir)
    prompt = agent["prompt"] if agent else ""
    p4 = (
        hints_back.get(DEVELOPER_TYPE) == [HINT_TEXT]
        and agent is not None
        and all(m in prompt for m in MARKERS)
    )
    logger.info(
        f"  hint inherited from origin: {'✓' if hints_back.get(DEVELOPER_TYPE) == [HINT_TEXT] else '✗'}"
    )
    logger.info(
        f"  developer 系统 prompt 含指令: {'✓' if agent and all(m in prompt for m in MARKERS) else '✗'}"
    )
    return p4


async def _run_phase4(app, orch, fork_sid: str, agents_dir: Path, live: bool) -> bool:
    """Phase 4：注入 developer 补充指令 → developer 系统 prompt 含指令。"""
    banner("[Phase 4] 注入 developer 补充指令 → developer 系统 prompt 含指令")
    if not live:
        return await _phase4_hint_check(app, orch, fork_sid, agents_dir)
    return _phase4_live_check(orch, fork_sid, agents_dir)


async def _run_phase5(app, recon_sid: str, cps: list) -> bool:
    """Phase 5：跳回 origin 检查点列表（jump-back 数据源）。"""
    banner("[Phase 5] 跳回 origin 检查点列表（jump-back 数据源）")
    origin_cps = await demo_http_get(app, f"/api/sessions/{recon_sid}/checkpoints")
    origin_cp_ids = {c["checkpoint_id"] for c in origin_cps.get("checkpoints", [])}
    p5 = {c["checkpoint_id"] for c in cps} <= origin_cp_ids
    logger.info(
        f"  origin 检查点列表含 cp1/cp2/cp3/session_complete: "
        f"{'✓' if p5 else '✗'} ({len(origin_cp_ids)} checkpoints)"
    )
    logger.info("  → 重放不满意时跳回原始检查点重新重放")
    return p5


def _log_summary(results: dict[str, bool], live: bool) -> None:
    """打印各 Phase 结果汇总。"""
    logger.info(f"\n{'═' * 64}")
    labels = [
        ("Phase 1  从 cannbot db 导入重建 session", "p1_import"),
        ("Phase 2  重建产物同构 (检查点/文件态/git/L2/事件/溯源)", "p2_verify_reconstruction"),
        ("Phase 3  重放到 developer 跑之前", "p3_replay_before_developer"),
        ("Phase 4  注入 developer 指令 → 系统 prompt", "p4_hint_inject"),
        ("Phase 5  跳回 origin 检查点列表", "p5_jumpback"),
    ]
    if live:
        labels.append(("Phase 3+ 真实 agent 写 impl.py", "p3_live_agent"))
    for label, key in labels:
        logger.info(f"  {label} : {'✓' if results.get(key) else '✗'}")
    logger.info(f"{'═' * 64}")


async def _async_main(live: bool = False) -> int:
    if shutil.which("git") is None:
        logger.info("git not installed — this demo requires git for file-state snapshots.")
        return 1

    if live and shutil.which("opencode") is None:
        logger.info(
            "opencode CLI not found — --live mode requires it. Install or run without --live."
        )
        return 1

    logging.getLogger("workflow_controller").setLevel(logging.DEBUG if live else logging.CRITICAL)

    _print_demo_header(live)
    tmp, project_dir, _xdg = _make_sandbox()
    source_db = _select_source_db(tmp, live)
    agents_dir = _seed_project_agents(project_dir, live)
    cannbot_db = build_cannbot_db(tmp / "cannbot.db")
    orch, app, _tb_plugin = await _mount_demo_app(project_dir, tmp, source_db, live)

    fork_cwd: Path | None = None
    try:
        results: dict[str, bool] = {}

        # ── Phase 1: 从 cannbot db 导入重建 ──────────────────────────────
        p1, recon_sid, recon_sdk, cps = await _run_phase1(app, cannbot_db, project_dir)
        results["p1_import"] = p1
        if not p1:
            logger.info("\nPhase 1 failed — aborting.")
            return 2

        # ── Phase 2: 重建产物同构校验 ─────────────────────────────────────
        p2 = await _run_phase2(app, orch, recon_sid, recon_sdk, cps, project_dir, cannbot_db)
        results["p2_verify_reconstruction"] = p2

        # ── Phase 3: 重放到 developer 跑之前 ─────────────────────────────
        p3, fork_cwd, fork_sid, p3_live = await _run_phase3(app, orch, recon_sid, cps, live)
        if p3_live is not None:
            results["p3_live_agent"] = p3_live
        results["p3_replay_before_developer"] = p3

        # ── Phase 4: 注入 developer 指令 → 系统 prompt ────────────────────
        p4 = await _run_phase4(app, orch, fork_sid, agents_dir, live)
        results["p4_hint_inject"] = p4

        # ── Phase 5: 跳回原始检查点 ───────────────────────────────────────
        p5 = await _run_phase5(app, recon_sid, cps)
        results["p5_jumpback"] = p5

        # ── 汇总 ──────────────────────────────────────────────────────────
        _log_summary(results, live)
        return 0 if all(results.values()) else 2
    finally:
        await orch.shutdown()
        if fork_cwd is not None:
            shutil.rmtree(fork_cwd.parent, ignore_errors=True)
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    configure_demo_logging(logger)
    import argparse

    parser = argparse.ArgumentParser(
        description="从 cannbot db 重建会话 + 检查点重放综合测试",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="使用真实 AI agent（需 opencode CLI + provider）",
    )
    args = parser.parse_args()
    import asyncio

    try:
        return asyncio.run(_async_main(live=args.live))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
