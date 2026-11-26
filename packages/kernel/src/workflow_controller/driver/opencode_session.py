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

"""Best-effort lookup of an opencode subagent's token usage and message stream
from opencode's local session store.

opencode persists per-session token totals and message parts in a sqlite db at
``$XDG_DATA_HOME/opencode/opencode.db``. A ``task`` subagent runs in its own
sub-session (``ses_…`` from the task result envelope). Workflow Controller reads this db in
two phases: streaming (``list_subagent_session_ids`` + ``read_subagent_parts``
for the driver's background poller) and post-completion
(``read_subagent_messages`` for the SUBAGENT stop event).

opencode writes to this db *while* Workflow Controller reads it. SQLite guarantees
row-level atomicity (no torn reads), but a concurrent write can briefly lock
the db (``SQLITE_BUSY``). All connections are opened read-only with a
``busy_timeout`` so reads wait for opencode's write to finish rather than
failing immediately — best-effort: a missing db, schema drift, or a
persistent lock all return 0 / [] and never raise.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from workflow_controller.transcript.opencode_store import (
    BUSY_TIMEOUT_MS,
    normalize_part,
    open_ro_db,
    opencode_db_path,
)


def lookup_subagent_output_tokens(
    sub_session_id: str,
    env: dict[str, str] | None = None,
) -> int:
    """Return a task subagent's ``tokens_output`` from opencode's session db.

    Best-effort: 0 when the id is empty, the db is absent, the column is
    missing, or the read fails. Read-only; never raises.
    """
    if not sub_session_id:
        return 0
    path = opencode_db_path(env)
    if not path.is_file():
        return 0
    con = open_ro_db(path)
    if con is None:
        return 0
    try:
        row = con.execute(
            "SELECT tokens_output FROM session WHERE id = ?",
            (sub_session_id,),
        ).fetchone()
    except sqlite3.Error:
        return 0
    finally:
        con.close()
    return int(row[0]) if row and row[0] else 0


def read_subagent_type(
    sub_session_id: str,
    env: dict[str, str] | None = None,
) -> str:
    """Return a task subagent's ``agent`` (subagent_type) from the session db.

    Reads the ``agent`` column opencode writes when a ``task`` subagent is
    spawned (e.g. ``"explore"``, ``"general"``). Best-effort; ``""`` when the
    id is empty, the db is absent, the column is missing, or the read fails.
    Read-only; never raises.
    """
    if not sub_session_id:
        return ""
    path = opencode_db_path(env)
    if not path.is_file():
        return ""
    con = open_ro_db(path)
    if con is None:
        return ""
    try:
        row = con.execute(
            "SELECT agent FROM session WHERE id = ?",
            (sub_session_id,),
        ).fetchone()
    except sqlite3.Error:
        return ""
    finally:
        con.close()
    return str(row[0]) if row and row[0] else ""


def read_opencode_session_title(
    native_session_id: str,
    env: dict[str, str] | None = None,
) -> str | None:
    """Read the title of an opencode session. Best-effort; never raises."""
    if not native_session_id:
        return None
    path = opencode_db_path(env)
    if not path.is_file():
        return None
    con = open_ro_db(path)
    if con is None:
        return None
    try:
        row = con.execute(
            "SELECT title FROM session WHERE id = ?",
            (native_session_id,),
        ).fetchone()
    except sqlite3.Error:
        return None
    finally:
        con.close()
    return row[0] if row and row[0] else None


def update_opencode_session_title(
    native_session_id: str,
    title: str,
    env: dict[str, str] | None = None,
) -> bool:
    """Update the title of an opencode session. Best-effort; never raises."""
    if not native_session_id or not title:
        return False
    path = opencode_db_path(env)
    if not path.is_file():
        return False
    try:
        con = sqlite3.connect(str(path), timeout=BUSY_TIMEOUT_MS / 1000)
    except sqlite3.Error:
        return False
    try:
        con.execute(
            "UPDATE session SET title = ? WHERE id = ?",
            (title, native_session_id),
        )
        con.commit()
        return True
    except sqlite3.Error:
        return False
    finally:
        con.close()


def read_subagent_parts(
    sub_session_id: str,
    env: dict[str, str] | None = None,
    max_parts: int = 200,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Read a subagent's message-part stream as ``(part_id, [messages])`` pairs.

    Each entry carries the source ``part.id`` so the poller can diff by part
    id and emit only new messages between polls. A ``tool`` part yields one
    entry with two messages sharing the same ``part_id``.

    Capped at ``max_parts`` raw parts (tail kept — conclusion > preamble).
    Best-effort; never raises.
    """
    if not sub_session_id:
        return []
    path = opencode_db_path(env)
    if not path.is_file():
        return []
    con = open_ro_db(path)
    if con is None:
        return []
    try:
        rows = con.execute(
            "SELECT id, data FROM part WHERE session_id = ? ORDER BY time_created ASC, id ASC",
            (sub_session_id,),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        con.close()

    if not rows:
        return []

    if len(rows) > max_parts:
        rows = rows[-max_parts:]

    out: list[tuple[str, list[dict[str, Any]]]] = []
    for pid, raw_data in rows:
        try:
            part = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(part, dict):
            continue
        msgs = normalize_part(part)
        if msgs:
            out.append((str(pid), msgs))
    return out


def list_subagent_session_ids(
    env: dict[str, str] | None = None,
    exclude: set[str] | None = None,
    parent_id: str = "",
) -> list[str]:
    """Return sub-session ids parented to ``parent_id`` (the main opencode
    session), filtered via the ``session`` table's ``parent_id`` column.

    opencode.db is **shared across all opencode processes** on the host — a
    part-table-only scan would surface sub-sessions spawned by *other*
    opencode processes (parallel Workflow Controller workers, unrelated ``opencode`` runs,
    interactive TUI sessions) and leak them into the subagent tracker's in-flight set
    forever (their stop event never arrives on *this* worker's stdout).
    Filtering by ``session.parent_id`` keeps the poller scoped to sub-sessions
    the *current* worker actually spawned.

    When ``parent_id`` is empty (legacy callers), falls back to the old
    part-table scan with ``exclude`` applied.

    ``exclude`` holds stale baseline ids so already-known sub-sessions are
    not re-reported. Ordered by earliest ``time_created``.
    Best-effort; never raises.
    """
    path = opencode_db_path(env)
    if not path.is_file():
        return []
    con = open_ro_db(path)
    if con is None:
        return []
    try:
        if parent_id:
            rows = con.execute(
                "SELECT p.session_id, MIN(p.time_created) AS t "
                "FROM part p "
                "JOIN session s ON s.id = p.session_id "
                "WHERE s.parent_id = ? "
                "GROUP BY p.session_id ORDER BY t ASC",
                (parent_id,),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT session_id, MIN(time_created) AS t FROM part "
                "GROUP BY session_id ORDER BY t ASC",
            ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        con.close()

    skip = exclude or set()
    return [str(sid) for sid, _ in rows if str(sid) not in skip]


def list_descendant_sessions(
    root_id: str,
    env: dict[str, str] | None = None,
) -> list[tuple[str, str]]:
    """Return ``(session_id, parent_id)`` for ALL sub-sessions descending from
    ``root_id`` (children + grandchildren + …), ordered by earliest part time.

    递归发现：孙 agent（子代理再派发的 agent）的 ``session.parent_id`` 指向
    子会话而非 main，直接子代过滤（``list_subagent_session_ids`` 的
    ``parent_id=``）看不到它们。``WITH RECURSIVE`` 沿 ``parent_id`` 下探全
    层级，``UNION`` 去重防环。与直接子代过滤同因：opencode.db 跨进程共享，
    只认 ``root_id`` 的后代，别的 opencode 进程的子会话不进 poller（它们的
    stop 永远不会到本 worker）。只返回 part 表里有行的会话（poller 从 part
    表读流）。Best-effort; never raises.
    """
    if not root_id:
        return []
    path = opencode_db_path(env)
    if not path.is_file():
        return []
    con = open_ro_db(path)
    if con is None:
        return []
    try:
        rows = con.execute(
            "WITH RECURSIVE tree(id, parent) AS ( "
            "  SELECT id, parent_id FROM session WHERE parent_id = ? "
            "  UNION "
            "  SELECT s.id, s.parent_id FROM session s "
            "  JOIN tree t ON s.parent_id = t.id "
            ") "
            "SELECT tree.id, tree.parent, MIN(p.time_created) AS t "
            "FROM tree JOIN part p ON p.session_id = tree.id "
            "GROUP BY tree.id, tree.parent ORDER BY t ASC",
            (root_id,),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        con.close()
    return [(str(sid), str(parent)) for sid, parent, _t in rows]


def read_subagent_messages(
    sub_session_id: str,
    env: dict[str, str] | None = None,
    max_parts: int = 200,
) -> list[dict[str, Any]]:
    """Flatten :func:`read_subagent_parts` into a single message list — the
    shape the SUBAGENT stop event carries. Best-effort; never raises.

    Part types: ``text``→text, ``reasoning``→thinking, ``tool``→tool_use+
    tool_result, ``step-start``→step_start; others skipped.
    """
    messages: list[dict[str, Any]] = []
    for _pid, msgs in read_subagent_parts(sub_session_id, env, max_parts):
        messages.extend(msgs)
    return messages
