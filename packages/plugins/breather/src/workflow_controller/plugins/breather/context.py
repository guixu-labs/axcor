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
import logging
import math
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from workflow_controller.session.events import EventType
from workflow_controller.session.session import resolve_query_model
from workflow_controller.session.usage import context_input_tokens

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Callable

    from workflow_controller.session.session import Session


@dataclass
class ContextResult:
    current_tokens: int
    max_tokens: int
    percentage: float | None
    method: str


class ContextTracker:
    def __init__(
        self,
        events_reader: Callable[[str], list[Any]],
        session: Session,
        strategy: str,
        threshold_pct: float,
        context_window: int | None = None,
        driver_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.events_reader = events_reader
        self.session = session
        self.strategy = strategy
        self.threshold_pct = threshold_pct
        self.context_window = context_window
        self.driver_factory = driver_factory

    @staticmethod
    def _model_usage_window(data: dict) -> int | None:
        model_usage = data.get("modelUsage")
        windows = []
        for mu in (model_usage if isinstance(model_usage, dict) else {}).values():
            if not (
                isinstance(mu, dict)
                and isinstance(mu.get("contextWindow"), (int, float))
                and math.isfinite(mu["contextWindow"])
                and mu["contextWindow"] > 0
            ):
                continue
            windows.append(mu.get("contextWindow"))
        return max(windows, default=None)

    async def check(self) -> ContextResult:
        # 优先读已记录快照（context_usage/step_finish/result），免 fork；
        # 无有效读数才回退探测子进程。
        result = self.check_recorded()
        if result.percentage is not None:
            return result
        return await self._check_from_command()

    def check_recorded(self) -> ContextResult:
        """只读已记录读数，不探测子进程（Stop hook 等同步停点用）。

        context_usage 事件不带 modelUsage，读数为它时回溯最近一条上报窗口，
        保证轮内检查点与 loop 边界（step_finish 的 modelUsage）同分母。
        """
        events = self.events_reader(self.session.id)
        total: int | None = None
        window: int | None = None
        for event in reversed(events):
            et = getattr(event.event_type, "value", event.event_type)
            if et not in ("context_usage", "step_finish", "result"):
                continue
            data = event.data
            if total is None:
                total = context_input_tokens(data.get("context_usage"))
                if total is None:
                    total = context_input_tokens(data.get("usage"))
            if window is None:
                window = self._model_usage_window(data)
            if total is not None and (self.context_window is not None or window is not None):
                break
        if total is None:
            return ContextResult(0, 0, None, "token")
        resolved = self.context_window or window or 200_000
        return ContextResult(total, resolved, round(total / resolved * 100, 1), "token")

    def _from_data(
        self,
        data: dict,
        method: str,
        window_hint: int | None = None,
    ) -> ContextResult:
        total = context_input_tokens(data.get("context_usage"))
        if total is None:
            total = context_input_tokens(data.get("usage"))
        if total is None:
            return ContextResult(0, 0, None, method)
        window = self.context_window or self._model_usage_window(data) or window_hint or 200_000
        return ContextResult(total, window, round(total / window * 100, 1), method)

    async def _probe_latest_result(
        self,
        driver: Any,
        sdk_env: dict[str, str],
        config: Any,
        model: str,
        carry: list[ContextResult | None],
    ) -> None:
        """消费探测 driver 的事件流，最新有效读数经 ``carry`` 即时回填。

        读数逐条落账到可变容器而非返回值：流中途异常（如 15s 超时、
        driver 半挂死）时调用方仍保留已得读数，不因收尾异常作废。
        """
        # context_usage 不带窗口，先到先记，收尾的 step_finish 自带
        # modelUsage 会覆盖，与 loop 边界同分母。
        window_hint: int | None = None
        async for event in driver.query(
            prompt="hi",
            max_turns=1,
            env=sdk_env,
            cwd=config.cwd,
            model=model,
        ):
            if event.event_type in (EventType.CONTEXT_USAGE, EventType.STEP_FINISH):
                window_hint = window_hint or self._model_usage_window(event.data)
                result = self._from_data(event.data, "context_command", window_hint)
                if result.percentage is not None:
                    carry[0] = result

    async def _check_from_command(self) -> ContextResult:
        unknown = ContextResult(0, 0, None, "context_command")
        if not self.session.native_session_id:
            return unknown

        if self.driver_factory is None:
            # create_driver 由组合根经端口装配，走到这里是装配错误——静默返回
            # 会把上下文检测降级为永不触发 handoff，须显式暴露。
            raise RuntimeError(
                "_check_from_command: driver_factory is not configured; "
                "create_driver must be wired via PluginServices"
            )

        config = self.session.config
        rotator = getattr(self.session, "_auth_rotator", None)
        sdk_env = (
            rotator.env_for_driver(
                dict(os.environ),
                config.provider_index,
            )
            if rotator is not None
            else {}
        )
        # model 解析先于 env_overrides 合并（与 Session.query 顺序一致）：
        # model 只看 rotator 注入的路由，env_overrides 不参与解析。
        model = resolve_query_model(config, sdk_env)
        sdk_env = {**sdk_env, **(config.env_overrides or {})}

        driver = None
        carry: list[ContextResult | None] = [None]
        try:
            driver = self.driver_factory(
                config.runtime,
                permission_mode="bypassPermissions",
                cli_wrapper=config.cli_wrapper,
                resume=self.session.native_session_id,
                fork_session=True,
            )
            async with asyncio.timeout(15):
                await self._probe_latest_result(driver, sdk_env, config, model, carry)
        except Exception:
            logger.warning("Context probe failed for session %s", self.session.id, exc_info=True)
        finally:
            if driver is not None:
                await driver.stop()
        last = carry[0]
        return last if last is not None else unknown
