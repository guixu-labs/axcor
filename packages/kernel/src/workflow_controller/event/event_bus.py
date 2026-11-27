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
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from workflow_controller.session.events import Event

if TYPE_CHECKING:
    from workflow_controller.event.type_registry import EventTypeRegistry
    from workflow_controller.store.event_router import EventStoreRouter

logger = logging.getLogger(__name__)


@dataclass
class InterceptResult:
    """A control-event interceptor's verdict.

    ``action`` semantics (shared by plugin and reviewer interceptors):

    - ``block`` — intercept: inject ``feedback`` and force another iteration.
    - ``stop`` — release the worker for this turn; the loop itself decides
      whether the session is complete (evaluator / boundary gate).
    - ``let_exit`` — release the worker **and** force-complete the loop:
      bypass the evaluator, end with ``stop_reason=reviewer_complete``.
      Used by plugin ``StopHookResult(action="let_exit")`` (e.g. breather
      handoff) and the reviewer's manual-reply branch.

    ``source`` optionally identifies the deciding interceptor (a plugin sets
    it to ``plugin.config.name``; the reviewer leaves it empty). The
    orchestrator's control-event consumer uses it to emit a
    ``stop_hook_plugin`` audit event only for plugin-sourced
    verdicts, avoiding double-emission with the reviewer's own
    ``pretool_hook_llm`` decisions.
    """

    action: str
    feedback: str = ""
    reason: str = ""
    source: str = ""


Observer = Callable[[Event], Awaitable[None]]

Interceptor = Callable[[Event], Awaitable[InterceptResult | None]]


class EventBus:
    def __init__(
        self,
        store_router: EventStoreRouter,
        type_registry: EventTypeRegistry,
    ) -> None:
        self._store = store_router
        self._registry = type_registry
        self._observers: dict[str, list[Observer]] = defaultdict(list)
        self._awaited_observers: dict[str, list[Observer]] = defaultdict(list)
        self._interceptors: dict[str, list[tuple[int, Interceptor]]] = defaultdict(list)
        self._observer_queue: asyncio.Queue[Event] = asyncio.Queue()
        self._processor: asyncio.Task[None] | None = None

    @property
    def type_registry(self) -> EventTypeRegistry:
        return self._registry

    @staticmethod
    def _type_key(event_type: Any) -> str:
        if isinstance(event_type, str):
            return event_type
        return getattr(event_type, "value", str(event_type))

    def start(self) -> None:
        self._processor = asyncio.create_task(self._process_observers())

    async def stop(self) -> None:
        processor, self._processor = self._processor, None
        if processor is not None:
            processor.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await processor

    def observe(self, event_type: Any, handler: Observer) -> None:
        self._observers[self._type_key(event_type)].append(handler)

    def observe_awaited(self, event_type: Any, handler: Observer) -> None:
        """Register an inline observer that completes before ``emit`` returns."""
        self._awaited_observers[self._type_key(event_type)].append(handler)

    def observe_all_awaited(self, handler: Observer) -> None:
        """Register an inline observer for every event type."""
        self._awaited_observers["*"].append(handler)

    def intercept(
        self,
        event_type: Any,
        handler: Interceptor,
        *,
        priority: int = 0,
    ) -> None:
        key = self._type_key(event_type)
        self._interceptors[key].append((priority, handler))
        self._interceptors[key].sort(key=lambda x: x[0], reverse=True)

    def detach_interceptor(self, event_type: Any, handler: Interceptor) -> None:
        """按 handler 匹配移除拦截器。

        插件在 ``uninstall`` 时调用，避免 reload 后旧实例的 handler 仍留在链中
        （否则旧、新实例都会触发）。按 ``==`` 匹配（绑定方法按
        ``__self__``/``__func__`` 比较）。
        """
        key = self._type_key(event_type)
        chain = self._interceptors.get(key)
        if not chain:
            return
        self._interceptors[key] = [(p, h) for p, h in chain if h != handler]

    def detach_awaited_observer(self, event_type: Any, handler: Observer) -> None:
        """按 handler 匹配移除 inline awaited 观察者。"""
        key = self._type_key(event_type)
        handlers = self._awaited_observers.get(key)
        if not handlers:
            return
        self._awaited_observers[key] = [h for h in handlers if h != handler]

    async def emit(self, event: Event) -> InterceptResult | None:
        # 单库双表分派：run 键（有 run_id 无 session 身份）→ run_events；
        # session 键 → events（打戳在存储门面的写路径）
        if event.run_id is not None and not event.session_id:
            await self._store.store_run_event(event)
        else:
            await self._store.store(event)
        return await self._dispatch(event)

    async def emit_ephemeral(self, event: Event) -> None:
        """只分发、不落库——事件数据行已由调用方清除或本就不该存在时使用。"""
        await self._dispatch(event)

    async def _dispatch(self, event: Event) -> InterceptResult | None:
        result = await self._run_interceptors(event)
        await self._run_awaited_observers(event)
        await self._observer_queue.put(event)
        return result

    async def _run_interceptors(self, event: Event) -> InterceptResult | None:
        chain = self._interceptors.get(self._type_key(event.event_type))
        if not chain:
            return None
        for _priority, handler in chain:
            try:
                r = await handler(event)
            except Exception:
                logger.exception("interceptor raised for %s", event.event_type)
                continue
            if r is not None and r.action != "pass":
                return r
        return None

    async def _run_awaited_observers(self, event: Event) -> None:
        key = self._type_key(event.event_type)
        handlers = (
            *self._awaited_observers.get(key, ()),
            *self._awaited_observers.get("*", ()),
        )
        for handler in handlers:
            try:
                await handler(event)
            except Exception:
                logger.exception("awaited observer raised for %s", event.event_type)

    async def _process_observers(self) -> None:
        while True:
            event = await self._observer_queue.get()
            key = self._type_key(event.event_type)
            for handler in self._observers.get(key, []):
                try:
                    await handler(event)
                except Exception:
                    logger.exception("observer raised for %s", event.event_type)
            for handler in self._observers.get("*", []):
                try:
                    await handler(event)
                except Exception:
                    logger.exception("wildcard observer raised")
