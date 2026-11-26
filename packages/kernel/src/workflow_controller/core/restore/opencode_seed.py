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

"""opencode 对话史读取与预制 session 种子（自插件 l2_reader 迁移提炼）。

按 message 级截断读取主会话 part 流，把截断前缀写进 fork 独立 db 成为新
session，供新 worker ``--resume`` 续跑（原生对话史保真）。低层只读原语来
自 ``transcript/opencode_store``——opencode.db 访问的单一定义处。
"""

from __future__ import annotations

import contextlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from workflow_controller.core.restore.path_rewrite import (
    rewrite_content_string,
    rewrite_part_data,
)
from workflow_controller.transcript.opencode_store import (
    BUSY_TIMEOUT_MS,
    fetch_rows,
    open_ro_db,
    opencode_db_path,
    parse_part,
)

__all__ = [
    "find_exclude_message_id",
    "find_main_part_id_for_subagent",
    "get_last_seed_error",
    "init_fork_db_from_source",
    "read_main_session_parts",
    "seed_prebuilt_session",
]

logger = logging.getLogger(__name__)

# 最近一次种子的失败原因（诊断用，调用方在失败时读取上报）
_last_seed_error: list[str] = [""]


def get_last_seed_error() -> str:
    """返回最近一次种子失败的原因串（无失败为空串）。"""
    return _last_seed_error[0]


def read_main_session_parts(
    main_session_id: str,
    env: dict[str, str] | None = None,
    up_to_part_id: str | None = None,
    max_parts: int = 2000,
    exclude_part_id: str | None = None,
    exclude_message_id: str | None = None,
) -> list[tuple[str, int, str, str]]:
    """读主会话 part 流为 ``(part_id, time_created, message_id, data_json)``。

    截断优先级 ``exclude_message_id``（message 级，消除「说了要派 X 但没有
    调用」的孤儿文本）> ``exclude_part_id``（part 级降级）> ``up_to_part_id``
    （含端点）。best-effort，任何错误返回空。
    """
    if not main_session_id:
        return []
    rows = fetch_rows(
        "SELECT id, time_created, message_id, data FROM part "
        "WHERE session_id = ? ORDER BY time_created ASC, id ASC",
        (main_session_id,),
        env,
    )
    if not rows:
        return []
    out: list[tuple[str, int, str, str]] = []
    for pid, tc, mid, raw in rows:
        if exclude_message_id and mid and str(mid) == exclude_message_id:
            break
        if exclude_part_id and str(pid) == exclude_part_id:
            break
        out.append((str(pid), int(tc) if tc else 0, str(mid or ""), str(raw)))
        if up_to_part_id and str(pid) == up_to_part_id:
            break
        if len(out) >= max_parts:
            break
    return out


def find_main_part_id_for_subagent(
    main_session_id: str,
    sub_agent_id: str,
    env: dict[str, str] | None = None,
) -> str | None:
    """主会话里输出引用了该子会话的 task part 标识（截断定位起点）。"""
    if not main_session_id or not sub_agent_id:
        return None
    rows = fetch_rows(
        "SELECT id, data FROM part WHERE session_id = ? ORDER BY time_created ASC, id ASC",
        (main_session_id,),
        env,
    )
    if not rows:
        return None
    for pid, raw in rows:
        part = parse_part(raw)
        if part is None:
            continue
        if part.get("type") != "tool" or part.get("tool") != "task":
            continue
        if sub_agent_id in str((part.get("state") or {}).get("output", "")):
            return str(pid)
    return None


def find_exclude_message_id(
    main_session_id: str,
    part_id: str,
    env: dict[str, str] | None = None,
) -> str | None:
    """取该 part 所属的 message_id（message 级截断用）；不可得返回 None。"""
    if not main_session_id or not part_id:
        return None
    rows = fetch_rows(
        "SELECT id, message_id FROM part WHERE session_id = ? AND id = ?",
        (main_session_id, part_id),
        env,
    )
    if not rows or not rows[0][1]:
        return None
    return str(rows[0][1])


# fork db 初始化时拷数据行的全局小表：迁移版本表 + project/agent 元数据；
# session 范围表只建 schema，行由种子/转写显式写入；敏感表刻意不拷
_FORK_DB_COPY_DATA_TABLES: tuple[str, ...] = (
    "__drizzle_migrations",
    "migration",
    "data_migration",
    "project",
    "project_directory",
    "agent",
)

# 已知的全部表，schema 自检据此发现未知漂移
_FORK_DB_KNOWN_TABLES: frozenset[str] = frozenset(
    {
        "part",
        "message",
        "session",
        "todo",
        "event",
        "event_sequence",
        "session_input",
        "session_message",
        "session_share",
        "session_context_epoch",
        "sqlite_sequence",
        "account",
        "account_state",
        "control_account",
        "credential",
        "permission",
        "workspace",
    }
) | frozenset(_FORK_DB_COPY_DATA_TABLES)


def _copy_schema_statements(src: sqlite3.Connection, dest: sqlite3.Connection) -> None:
    """按 table → view → index → trigger 依赖序回放 CREATE 语句。"""
    schema_rows = src.execute(
        "SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL",
    ).fetchall()
    type_order = {"table": 0, "view": 1, "index": 2, "trigger": 3}
    schema_rows.sort(key=lambda r: type_order.get(str(r[0]), 4))
    for _type, name, sql in schema_rows:
        if str(name).startswith("sqlite_"):
            continue
        dest.execute(str(sql))


def _copy_user_version(src: sqlite3.Connection, dest: sqlite3.Connection) -> None:
    """复制 ``PRAGMA user_version``（保真零成本）。"""
    user_version = int(src.execute("PRAGMA user_version").fetchone()[0] or 0)
    if user_version:
        dest.execute(f"PRAGMA user_version = {user_version}")


def _copy_global_table_rows(src: sqlite3.Connection, dest: sqlite3.Connection) -> None:
    """拷全局小表数据行（动态列发现，缺表/空表跳过）。"""
    for table in _FORK_DB_COPY_DATA_TABLES:
        cols = [column[1] for column in src.execute(f'PRAGMA table_info("{table}")').fetchall()]
        if not cols:
            continue
        data_rows = src.execute(
            f'SELECT {",".join(chr(34) + c + chr(34) for c in cols)} FROM "{table}"'
        ).fetchall()
        if not data_rows:
            continue
        placeholders = ",".join("?" for _ in cols)
        dest.executemany(
            f'INSERT OR REPLACE INTO "{table}" '
            f"({','.join(chr(34) + c + chr(34) for c in cols)}) "
            f"VALUES ({placeholders})",
            data_rows,
        )


def _warn_unknown_tables(src: sqlite3.Connection) -> None:
    """schema 漂移自检：未知表告警，提醒评估是否需要拷其数据。"""
    try:
        names = [
            str(name)
            for (name,) in src.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        unknown = [name for name in names if name not in _FORK_DB_KNOWN_TABLES]
        if unknown:
            logger.warning(
                "fork db 存在 %d 张未知表（%s）——评估种子是否需要拷其数据行",
                len(unknown),
                ", ".join(unknown),
            )
    except sqlite3.Error:
        pass


def init_fork_db_from_source(dest_path: Path, source_path: Path) -> bool:
    """初始化 fork db：从 source 做 schema-only 拷贝（不拷 session 数据行）。

    失败时清理半成品 dest 文件（含 ``-wal``/``-shm``）。
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    src: sqlite3.Connection | None = None
    dest: sqlite3.Connection | None = None
    ok = True
    try:
        src = sqlite3.connect(
            f"file:{source_path}?mode=ro",
            uri=True,
            timeout=BUSY_TIMEOUT_MS / 1000,
        )
        src.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        dest = sqlite3.connect(str(dest_path), timeout=BUSY_TIMEOUT_MS / 1000)
        dest.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        with contextlib.suppress(sqlite3.Error):
            dest.execute("PRAGMA journal_mode=WAL")
        _copy_schema_statements(src, dest)
        _copy_user_version(src, dest)
        _copy_global_table_rows(src, dest)
        dest.commit()
        _warn_unknown_tables(src)
    except (sqlite3.Error, OSError):
        ok = False
    finally:
        for conn in (dest, src):
            if conn is not None:
                with contextlib.suppress(sqlite3.Error):
                    conn.close()
        if not ok:
            for suffix in ("", "-wal", "-shm"):
                with contextlib.suppress(OSError):
                    Path(f"{dest_path}{suffix}").unlink(missing_ok=True)
    return ok


@dataclass
class _SeedSource:
    """种子的源侧输入快照：session 行（已覆盖）+ 截断 part 流 + message 行。"""

    cols: list[str]
    src_dict: dict[str, Any]
    origin_cwd: str
    now: int
    parts: list[tuple[str, int, str, str]]
    msg_cols: list[str]
    msg_rows: list[tuple[Any, ...]]
    msg_id_remap: dict[str, str]


def _load_source_session_row(
    source_path: Path,
    source_session_id: str,
) -> tuple[dict[str, Any], list[str]] | None:
    """读源 session 行（全列，动态适配 schema 漂移）；失败返回 None。"""
    try:
        con_ro = open_ro_db(source_path)
        if con_ro is None:
            return None
        cols = [column[1] for column in con_ro.execute("PRAGMA table_info(session)").fetchall()]
        if not cols or "id" not in cols:
            con_ro.close()
            return None
        src_row = con_ro.execute(
            f"SELECT {','.join(cols)} FROM session WHERE id = ?",
            (source_session_id,),
        ).fetchone()
        con_ro.close()
    except sqlite3.Error:
        return None
    if not src_row:
        return None
    return dict(zip(cols, src_row, strict=False)), cols


def _override_session_row(
    src_dict: dict[str, Any],
    *,
    new_session_id: str,
    title: str,
    directory: str | None,
    now: int,
) -> str:
    """按种子需求覆盖 session 行字段；返回改写前保存的源 cwd。"""
    origin_cwd: str = src_dict.get("directory") or ""
    src_dict["id"] = new_session_id
    if "title" in src_dict:
        src_dict["title"] = title
    if "parent_id" in src_dict:
        # 置空：重放会话是顶层任务而非源会话的子代理（父链会让派生深度提前占满）
        src_dict["parent_id"] = None
    for time_col in ("time_created", "time_updated"):
        if time_col in src_dict:
            src_dict[time_col] = now
    if "time_compacting" in src_dict:
        src_dict["time_compacting"] = None
    if directory:
        if "directory" in src_dict:
            src_dict["directory"] = directory
        if "path" in src_dict:
            src_dict["path"] = ""
    return origin_cwd


def _load_message_rows(
    source_path: Path,
    source_session_id: str,
    msg_ids: set[str],
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """读保留 parts 所属的 message 行（role/parentID 链）；失败返回空。"""
    cols: list[str] = []
    rows: list[tuple[Any, ...]] = []
    if not msg_ids:
        return cols, rows
    try:
        con_ro = open_ro_db(source_path)
        if con_ro is not None:
            msg_cols = [
                column[1] for column in con_ro.execute("PRAGMA table_info(message)").fetchall()
            ]
            if msg_cols and "id" in msg_cols and "session_id" in msg_cols:
                all_msgs = con_ro.execute(
                    f"SELECT {','.join(msg_cols)} FROM message "
                    "WHERE session_id = ? ORDER BY time_created ASC",
                    (source_session_id,),
                ).fetchall()
                id_index = msg_cols.index("id")
                rows = [row for row in all_msgs if row[id_index] in msg_ids]
                cols = msg_cols
            con_ro.close()
    except sqlite3.Error:
        pass
    return cols, rows


def _build_msg_id_remap(
    parts: list[tuple[str, int, str, str]],
    msg_cols: list[str],
    msg_rows: list[tuple[Any, ...]],
) -> dict[str, str]:
    """把非 ``msg`` 前缀的 message id 重映射为 schema 要求的前缀（幂等）。"""
    remap: dict[str, str] = {}
    for _pid, _tc, mid, _raw in parts:
        if mid and mid not in remap and not str(mid).startswith("msg"):
            remap[str(mid)] = "msg" + str(mid)[1:]
    if msg_cols:
        id_index = msg_cols.index("id")
        for row in msg_rows:
            mid = row[id_index]
            if mid and str(mid) not in remap and not str(mid).startswith("msg"):
                remap[str(mid)] = "msg" + str(mid)[1:]
    return remap


def _remap_msg_ids_in_data(raw: str, remap: dict[str, str]) -> str:
    """Remap message data JSON 内的 ``id`` / ``parentID``，保持链一致。"""
    if not raw or not remap:
        return raw
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw
    if not isinstance(data, dict):
        return raw
    changed = False
    for key in ("id", "parentID"):
        value = data.get(key)
        if value and str(value) in remap:
            data[key] = remap[str(value)]
            changed = True
    if not changed:
        return raw
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _load_seed_snapshot(
    source_session_id: str,
    read_env: dict[str, str] | None,
    source_path: Path,
    up_to_part_id: str | None,
    exclude_part_id: str | None,
    exclude_message_id: str | None,
    *,
    new_session_id: str,
    title: str,
    directory: str | None,
) -> _SeedSource | None:
    """读源 session 行（覆盖后）+ 截断 part 流 + message 行 + id 重映射。"""
    loaded = _load_source_session_row(source_path, source_session_id)
    if loaded is None:
        return None
    src_dict, cols = loaded
    now = int(datetime.now(tz=UTC).timestamp() * 1000)
    origin_cwd = _override_session_row(
        src_dict,
        new_session_id=new_session_id,
        title=title,
        directory=directory,
        now=now,
    )
    parts = read_main_session_parts(
        source_session_id,
        read_env,
        up_to_part_id,
        exclude_part_id=exclude_part_id,
        exclude_message_id=exclude_message_id,
    )
    if not parts and not exclude_part_id and not exclude_message_id:
        # 非 re-call 模式无 part 视为失败（源会话无对话可灌）；re-call 的首个
        # 子代理可能无前置 part（task part 即第一条），允许空流冷启
        return None
    msg_ids = {mid for _pid, _tc, mid, _raw in parts if mid}
    msg_cols, msg_rows = _load_message_rows(source_path, source_session_id, msg_ids)
    msg_id_remap = _build_msg_id_remap(parts, msg_cols, msg_rows)
    return _SeedSource(
        cols=cols,
        src_dict=src_dict,
        origin_cwd=origin_cwd,
        now=now,
        parts=parts,
        msg_cols=msg_cols,
        msg_rows=msg_rows,
        msg_id_remap=msg_id_remap,
    )


def _open_dest_db(dest_path: Path) -> sqlite3.Connection | None:
    """连接目标 db（busy timeout）；失败返回 None。"""
    try:
        conn = sqlite3.connect(str(dest_path), timeout=BUSY_TIMEOUT_MS / 1000)
        conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    except sqlite3.Error:
        return None
    return conn


def _insert_seeded_parts(
    conn: sqlite3.Connection,
    snapshot: _SeedSource,
    *,
    new_session_id: str,
    directory: str | None,
    rewrite: bool,
) -> None:
    """写 part 行：新 part id + 新 session_id，保留 message_id/time/data。"""
    for index, (_pid, tc, mid, raw) in enumerate(snapshot.parts):
        new_pid = f"prt_tb_{new_session_id[:8]}_{index:04d}"
        if rewrite and directory:
            raw = rewrite_part_data(raw, snapshot.origin_cwd, directory)
        effective_mid = (
            snapshot.msg_id_remap.get(mid, mid)
            if mid
            else f"msg_tb_{new_session_id[:8]}_{index:04d}"
        )
        conn.execute(
            "INSERT OR REPLACE INTO part "
            "(id, message_id, session_id, time_created, time_updated, data) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (new_pid, effective_mid, new_session_id, tc or snapshot.now, tc or snapshot.now, raw),
        )


def _insert_seeded_messages(
    conn: sqlite3.Connection,
    snapshot: _SeedSource,
    *,
    new_session_id: str,
    directory: str | None,
    rewrite: bool,
) -> None:
    """写 message 行：保留（重映射后的）原 id 使 parts 引用有效，改 session_id。"""
    if not snapshot.msg_rows or not snapshot.msg_cols:
        return
    placeholders = ",".join("?" for _ in snapshot.msg_cols)
    data_col_index = snapshot.msg_cols.index("data") if "data" in snapshot.msg_cols else -1
    for row in snapshot.msg_rows:
        mdict = dict(zip(snapshot.msg_cols, row, strict=False))
        mdict["session_id"] = new_session_id
        old_mid = str(mdict.get("id", ""))
        if old_mid in snapshot.msg_id_remap:
            mdict["id"] = snapshot.msg_id_remap[old_mid]
        if (
            rewrite
            and directory is not None
            and data_col_index >= 0
            and isinstance(mdict.get("data"), str)
        ):
            mdict["data"] = rewrite_content_string(
                mdict["data"],
                snapshot.origin_cwd,
                directory,
            )
        if data_col_index >= 0 and isinstance(mdict.get("data"), str):
            mdict["data"] = _remap_msg_ids_in_data(mdict["data"], snapshot.msg_id_remap)
        conn.execute(
            f"INSERT OR REPLACE INTO message ({','.join(snapshot.msg_cols)}) "
            f"VALUES ({placeholders})",
            [mdict[column] for column in snapshot.msg_cols],
        )


def _rollback_seeded_rows(conn: sqlite3.Connection, new_session_id: str) -> None:
    """写入失败——best-effort 删掉可能已写入的新 session/message/part 行。"""
    try:
        conn.execute("DELETE FROM session WHERE id = ?", (new_session_id,))
        conn.execute("DELETE FROM message WHERE session_id = ?", (new_session_id,))
        conn.execute("DELETE FROM part WHERE session_id = ?", (new_session_id,))
        conn.commit()
    except sqlite3.Error:
        pass


def _write_seed_snapshot(
    dest_path: Path,
    source_path: Path,
    snapshot: _SeedSource,
    *,
    new_session_id: str,
    directory: str | None,
) -> bool:
    """初始化（若需）并连接目标 db，写入 seeded session/parts/messages。"""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    need_init = not dest_path.is_file() or dest_path.stat().st_size == 0
    if need_init and not init_fork_db_from_source(dest_path, source_path):
        return False
    conn = _open_dest_db(dest_path)
    if conn is None:
        return False
    rewrite = bool(snapshot.origin_cwd and directory and snapshot.origin_cwd != directory)
    try:
        placeholders = ",".join("?" for _ in snapshot.cols)
        conn.execute(
            f"INSERT OR REPLACE INTO session ({','.join(snapshot.cols)}) VALUES ({placeholders})",
            [snapshot.src_dict[column] for column in snapshot.cols],
        )
        _insert_seeded_parts(
            conn,
            snapshot,
            new_session_id=new_session_id,
            directory=directory,
            rewrite=rewrite,
        )
        _insert_seeded_messages(
            conn,
            snapshot,
            new_session_id=new_session_id,
            directory=directory,
            rewrite=rewrite,
        )
        conn.commit()
    except sqlite3.Error as exc:
        _rollback_seeded_rows(conn, new_session_id)
        _last_seed_error[0] = f"{type(exc).__name__}: {exc}"
        conn.close()
        return False
    conn.close()
    _last_seed_error[0] = ""
    return True


def seed_prebuilt_session(
    *,
    source_session_id: str,
    new_session_id: str,
    up_to_part_id: str | None,
    title: str,
    env: dict[str, str] | None = None,
    source_env: dict[str, str] | None = None,
    exclude_part_id: str | None = None,
    exclude_message_id: str | None = None,
    directory: str | None = None,
) -> bool:
    """往 opencode.db 写一个预制 session + 截断 part 流 + message 行。

    读写分离：``source_env`` 读源 db、``env`` 写目标 db（均 None 走宿主默
    认）；两者不同且目标库不存在时从源库 schema-only 初始化——worker 读
    不到 origin/兄弟会话的「未来」（隔离）。防御性：失败返回 False 不抛。
    """
    if not source_session_id or not new_session_id:
        return False
    read_env = source_env
    source_path = opencode_db_path(read_env)
    if not source_path.is_file():
        return False
    dest_path = opencode_db_path(env)
    snapshot = _load_seed_snapshot(
        source_session_id,
        read_env,
        source_path,
        up_to_part_id,
        exclude_part_id,
        exclude_message_id,
        new_session_id=new_session_id,
        title=title,
        directory=directory,
    )
    if snapshot is None:
        return False
    return _write_seed_snapshot(
        dest_path, source_path, snapshot, new_session_id=new_session_id, directory=directory
    )
