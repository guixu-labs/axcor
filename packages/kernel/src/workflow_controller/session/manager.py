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

import asyncio
import contextlib
import json
import logging
from typing import TYPE_CHECKING, Any

from workflow_controller.session.session import Session, SessionState

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from workflow_controller.config.settings import SessionConfig

logger = logging.getLogger(__name__)


class SessionPool:
    def __init__(self) -> None:
        self._sessions: dict[str, Session] = {}
        self._configs: dict[str, SessionConfig] = {}
        self._event_router: Any = None  # EventStoreRouter, set via set_event_router()
        # session 创建入口回调（run 归属铸造）：一切创建路径都过这里
        self._affiliation_hook: Callable[[Session, SessionConfig], Awaitable[None]] | None = None

    @staticmethod
    def _roster_fields(session: Session) -> dict[str, Any]:
        """sessions 表行字段（键为 sessions 表列名）。"""
        fls = session.final_loop_status.value if session.final_loop_status else None
        return {
            "id": session.id,
            "name": session.name,
            "role": session.role,
            "state": session.state.value,
            "loop_status": session.loop_status,
            "stop_reason": session.stop_reason,
            "loop_hold": session.loop_hold,
            # config_json 附带运行时长记账（非 SessionConfig 字段，恢复时读出）
            "config_json": json.dumps(
                {**session.config.model_dump(), "active_seconds": session.active_seconds},
                default=str,
            ),
            "sdk_session_id": session.sdk_session_id,
            "created_at": session.created_at.isoformat(),
            "last_activity": session.last_activity.isoformat(),
            "current_iteration": session.current_iteration,
            "birth_handoff_doc": session.birth_handoff_doc,
            "result": session.result,
            "last_error": session.last_error,
            "plugin_metadata": json.dumps(
                getattr(session, "plugin_metadata", {}),
                default=str,
            )
            if getattr(session, "plugin_metadata", None)
            else None,
            "final_loop_status": fls,
            "retry_count": session.retry_count,
        }

    def set_event_router(self, router: Any) -> None:
        """Inject EventStoreRouter for database persistence."""
        self._event_router = router

    def set_affiliation_hook(
        self,
        hook: Callable[[Session, SessionConfig, dict], Awaitable[bool]] | None,
    ) -> None:
        """挂 session 创建入口回调：隐式创建与 sessions 表写入单事务落账并回写 config.run_id。

        回调返回 True 表示 sessions 表行已随创建同事务写入（pool 不再单独写）。
        """
        self._affiliation_hook = hook

    async def persist(self, session: Session) -> None:
        """公开回写面：外部调用点（orchestrator 恢复链等）落 sessions 表投影。"""
        await self._persist_session(session)

    async def create(
        self,
        config: SessionConfig,
        *,
        plugin_metadata: dict[str, dict[str, Any]] | None = None,
    ) -> Session:
        """新建 session 并入 pool + 落库。

        plugin_metadata 在入 pool 前设好，避免读端看到空 metadata。
        隐式归属铸造与 sessions 表写入单事务落账（回调返回 True 时 sessions 表行已随 mint 写入）；
        记账写失败不阻断会话——恒等式由 boot 补铸兜底。
        """
        session = Session(
            name=config.name,
            role=config.role,
            config=config,
        )
        if plugin_metadata:
            session.plugin_metadata.update(plugin_metadata)
        self._sessions[session.id] = session
        self._configs[config.name] = config
        roster = self._roster_fields(session)
        roster_persisted = False
        if self._affiliation_hook is not None:
            try:
                roster_persisted = await self._affiliation_hook(session, config, roster)
            except Exception:
                logger.exception("run 归属铸造失败（session=%s）", session.id)
        if not roster_persisted:
            await self._persist_session(session)
        return session

    def get(self, session_id: str) -> Session | None:
        return self._sessions.get(session_id)

    def get_config(self, name: str) -> SessionConfig | None:
        return self._configs.get(name)

    def get_by_name(self, name: str) -> Session | None:
        return next(
            (s for s in self._sessions.values() if s.name == name),
            None,
        )

    async def stop(self, session_id: str) -> None:
        session = self._sessions.get(session_id)
        if session:
            await session.stop()
            await self._persist_session(session)

    async def pause(self, session_id: str) -> None:
        session = self._sessions.get(session_id)
        if session:
            await session.pause()
            await self._persist_session(session)

    async def play(self, session_id: str) -> None:
        session = self._sessions.get(session_id)
        if session:
            await session.play()
            await self._persist_session(session)

    async def resume(self, session_id: str) -> None:
        session = self._sessions.get(session_id)
        if session:
            await session.resume()
            await self._persist_session(session)

    async def inject_prompt(self, session_id: str, prompt: str) -> None:
        session = self._sessions.get(session_id)
        if session:
            await session.inject_prompt(prompt)

    async def stop_all(self) -> None:
        await asyncio.gather(*(self.stop(sid) for sid in list(self._sessions)))

    def list_sessions(self) -> list[Session]:
        return list(self._sessions.values())

    def remove(self, session_id: str) -> bool:
        """Remove a session from the pool (does not stop running processes)."""
        if session_id in self._sessions:
            del self._sessions[session_id]
            # Also delete from database if event_router is set
            if self._event_router is not None:
                try:
                    self._event_router.delete_session(session_id)
                except Exception:
                    logger.exception("Failed to delete session %s from database", session_id)
            return True
        return False

    def discard(self, session_id: str) -> bool:
        """只从内存移除 session，不改持久记录或 Run 归属。"""
        if session_id in self._sessions:
            del self._sessions[session_id]
            return True
        return False

    def restore_from_store(self, session_id: str) -> Session | None:
        """历史详情按需恢复单个已离开内存的 session。

        与 boot 批量恢复不同，这里保留持久化的终态：DiscardSession 清理
        出池的多为已结束成员，详情读应显示 completed/failed 而非 idle。
        """
        session = self._sessions.get(session_id)
        if session is not None or self._event_router is None:
            return session
        entry = self._event_router.get_session_sync(session_id)
        if entry is None or not self._restore_session_from_entry(
            entry,
            preserve_terminal_state=True,
        ):
            return None
        return self._sessions.get(session_id)

    def load_sessions(self) -> int:
        """Load ghost sessions from events.db sessions table.

        events.db 的 sessions 表是 session 元数据的唯一持久化来源
        （`_persist_session` 每次状态变更实时 UPSERT）。重启时从这里恢复。

        Returns count loaded.
        """
        seen_ids: set[str] = set(self._sessions.keys())
        count = self._restore_ghost_rows(seen_ids) if self._event_router is not None else 0
        if count:
            logger.info("Restored %d ghost session(s)", count)
        return count

    def _restore_ghost_rows(self, seen_ids: set[str]) -> int:
        """逐行恢复 events.db 里池外未见过的 ghost session，返回新增恢复数。"""
        count = 0
        try:
            for row_dict in self._event_router.list_sessions_sync():
                sid = row_dict.get("id")
                if not sid or sid in seen_ids:
                    continue
                if self._restore_session_from_entry(row_dict):
                    seen_ids.add(sid)
                    count += 1
        except Exception:
            logger.exception("Failed to load sessions from events.db")
        return count

    async def _persist_session(self, session: Session) -> None:
        """Persist session metadata to database if event_router is set."""
        if self._event_router is None:
            return
        try:
            await self._event_router.store_session(
                session.id,
                **self._roster_fields(session),
            )
        except Exception:
            logger.exception("Failed to persist session %s", session.id)

    def _restore_session_from_entry(
        self,
        entry: dict[str, Any],
        *,
        preserve_terminal_state: bool = False,
    ) -> bool:
        """从 dict（events.db sessions 行 或 sessions.json 条目）还原一个 ghost
        session 到内存 pool。返回是否成功。

        CRITICAL: 所有 ghost session 都加载为 IDLE —— Workflow Controller 重启时没有 loop
        在跑，没有任何 session 可以是 running。这是 redesign.md §2.1 G4:
        "所有 session 都 idle 是允许的，此时没有 loop"。
        例外：preserve_terminal_state（按需详情读）保留持久化的终态
        （completed/stopped/failed）——池外成员只会是已终结被清理的，
        终态无 loop 附着、显示为真相；非终态仍归一 IDLE。
        """
        sid = entry.get("id")
        if not sid or sid in self._sessions:
            return False
        try:
            session = _ghost_session_from_entry(
                entry, preserve_terminal_state=preserve_terminal_state
            )
            self._sessions[sid] = session
            self._configs[session.config.name] = session.config
            return True
        except Exception:
            logger.exception("Failed to restore ghost session %s", sid)
            return False


def _apply_entry_fields(session: Session, entry: dict[str, Any], active_seconds: float) -> None:
    """entry 的运行态字段回填到 ghost session。"""
    session.active_seconds = active_seconds
    session.sdk_session_id = entry.get("sdk_session_id")
    session.created_at = _parse_dt(entry.get("created_at"))
    session.last_activity = _parse_dt(entry.get("last_activity"))
    session.current_iteration = entry.get("current_iteration", 0)
    fls = entry.get("final_loop_status")
    if fls:
        from workflow_controller.core.loop import LoopStatus

        with contextlib.suppress(ValueError, KeyError):
            session.final_loop_status = LoopStatus(fls)
    session.last_stop_reason = entry.get("last_stop_reason") or "user_stop"
    session.retry_count = entry.get("retry_count") or 0
    # 崩溃遗留的 running 没有存活 runtime；归一为可恢复断点，
    # Run 驱动才能按原 sdk_session_id 续跑，而不会误生新成员。
    if (entry.get("state") or "").lower() == "running":
        session.loop_status = "no"
        session.stop_reason = "user_pause"
    else:
        session.loop_status = entry.get("loop_status") or "no"
        session.stop_reason = entry.get("stop_reason")
    session.loop_hold = entry.get("loop_hold")
    session.birth_handoff_doc = entry.get("birth_handoff_doc")
    session.result = entry.get("result")
    pm_raw = entry.get("plugin_metadata")
    pm: dict[str, dict[str, Any]] = {}
    if pm_raw:
        if isinstance(pm_raw, str):
            try:
                pm = json.loads(pm_raw) or {}
            except (json.JSONDecodeError, TypeError):
                pm = {}
        elif isinstance(pm_raw, dict):
            pm = pm_raw
    session.plugin_metadata = pm


def _ghost_session_from_entry(
    entry: dict[str, Any],
    *,
    preserve_terminal_state: bool = False,
) -> Session:
    """entry dict → ghost Session（config 解析、终态保留与字段回填）。"""
    from workflow_controller.config.settings import SessionConfig

    # events.db 存的是 config_json (str)，sessions.json 存的是 config (dict)
    cfg_raw = entry.get("config_json") or entry.get("config")
    if isinstance(cfg_raw, str):
        cfg_raw = json.loads(cfg_raw)
    # 运行时长记账随 config_json 存取（非 SessionConfig 字段）
    active_seconds = 0.0
    if isinstance(cfg_raw, dict):
        active_seconds = float(cfg_raw.pop("active_seconds", 0) or 0)
    config = SessionConfig(**cfg_raw)
    session = Session(
        session_id=entry["id"],
        name=entry["name"],
        role=entry["role"],
        config=config,
    )
    if preserve_terminal_state:
        try:
            persisted = SessionState(entry.get("state"))
        except ValueError:
            persisted = SessionState.IDLE
        session.state = (
            persisted
            if persisted.value in ("completed", "stopped", "failed")
            else SessionState.IDLE
        )
    else:
        session.state = SessionState.IDLE
    _apply_entry_fields(session, entry, active_seconds)
    return session


def _parse_dt(val: str | None) -> Any:
    from datetime import UTC, datetime

    if not val:
        return datetime.now(tz=UTC)
    try:
        parsed = datetime.fromisoformat(val)
    except (ValueError, TypeError):
        return datetime.now(tz=UTC)
    # 历史版本写入 naive 本地时间串，按本地时区补全；全量 aware 写入一个版本后可删除
    return parsed if parsed.tzinfo is not None else parsed.astimezone()
