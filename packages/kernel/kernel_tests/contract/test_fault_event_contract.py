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

"""fault.* 告警事件约定 — 词表与 payload 字段和 manifest 清单互锁。"""

from __future__ import annotations

from workflow_controller.watchdog.core import FileStallRule, KeywordRule, SubagentTimeoutRule
from workflow_controller.watchdog.fault import fault_event_type
from workflow_controller.watchdog.state import Alert, Severity, make_alert_id

from .manifest import event_payload_fields, fault_event_types

_RULES = (SubagentTimeoutRule, FileStallRule, KeywordRule)


def test_fault_vocabulary_matches_manifest() -> None:
    """规则产生的事件类型集合必须与清单登记词表一致。"""
    assert {fault_event_type(r.rule) for r in _RULES} == set(fault_event_types())


def test_fault_payload_fields_match_manifest() -> None:
    """告警 payload 字段必须与清单登记一致。"""
    alert = Alert(
        alert_id=make_alert_id(),
        rule="subagent_timeout",
        severity=Severity.WARNING,
        session_id="s",
        message="m",
    )
    assert set(alert.to_dict()) == set(event_payload_fields("fault_alert"))
