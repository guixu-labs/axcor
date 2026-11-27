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

"""fault.* 告警事件编解码 — 检测规则与查询消费共用。

告警事件类型是开放词表 ``fault.<rule>``（字符串事件类型），payload 为
``Alert.to_dict()``。本模块是词表与 payload 编解码的唯一实现，消费侧
（WatchdogAPI、前端过滤）不依赖检测规则实现。

词表成员按产生源分两类：检测规则（watchdog/core.py 的事件订阅者）与
run 平面故障（产生源在协调器工作区桥，不经检测规则、走 run 键事件通
道，payload 为领域事实而非 ``Alert`` 序列化——上层把它们与超时同级
映射为停止信号）。节点累计超时（``node_timeout``）是检测规则但 run 键
寻址——节点无 session 身份，事件经 ``emit_run_fault_event`` 落
run_events 表（RSI 适配器轮询的 run 事件面）。
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from workflow_controller.session.events import Event
from workflow_controller.watchdog.state import Alert

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from workflow_controller.event.event_bus import EventBus

log = logging.getLogger(__name__)

FAULT_TYPE_PREFIX = "fault."

# run 平面故障词目：工作区集成冲突与作用域闸门不可静止
WORKSPACE_CONFLICT_RULE = "workspace_conflict"
SCOPE_QUIESCE_FAILED_RULE = "scope_quiesce_failed"


def fault_event_type(rule: str) -> str:
    """规则名 → fault.* 事件类型。"""
    return FAULT_TYPE_PREFIX + rule


def is_fault_event_type(event_type: Any) -> bool:
    """判定事件类型是否属于 fault.* 开放词表。"""
    return isinstance(event_type, str) and event_type.startswith(FAULT_TYPE_PREFIX)


async def emit_fault_event(bus: EventBus, alert: Alert) -> None:
    """告警进事件库：fault.<rule> 类型、session 键、payload 为 Alert 序列化。"""
    await bus.emit(
        Event.create(
            session_id=alert.session_id,
            iteration=0,
            event_type=fault_event_type(alert.rule),
            data=alert.to_dict(),
        )
    )


async def emit_run_fault_event(
    bus: EventBus,
    rule: str,
    payload: Mapping[str, Any],
) -> None:
    """run 键告警进事件库：fault.<rule> 类型落 run_events 表（payload 为领域事实）。

    run_id 缺失视为调用方构造错误——run 键事件没有 run 归属会回落
    session 键写库（空 session 污染 events 表），直接拒绝。
    """
    run_id = str(payload.get("run_id", "") or "")
    if not run_id:
        raise ValueError("run 键告警事件缺少 run_id")
    await bus.emit(
        Event(
            id=uuid4().hex,
            session_id="",
            iteration=0,
            timestamp=datetime.now(tz=UTC),
            event_type=fault_event_type(rule),
            data=dict(payload),
            run_id=run_id,
        )
    )


def decode_alerts(events: Iterable[Event]) -> list[Alert]:
    """从事件行筛出 fault.* 告警并解码，坏行跳过。"""
    alerts: list[Alert] = []
    for ev in events:
        if not is_fault_event_type(ev.event_type):
            continue
        try:
            alerts.append(Alert.from_dict(ev.data))
        except Exception:
            log.exception("watchdog: skip malformed fault event %s", ev.id)
    return alerts
