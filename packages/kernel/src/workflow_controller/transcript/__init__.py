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

"""中性转录层：转录源定位与 claude-jsonl / opencode.db 编解码。

只依赖标准库，供内核 run 导出与领域上传面共同消费，不反向依赖其他
workflow_controller 包。
"""

from workflow_controller.transcript.claude_jsonl_out import (
    write_opencode_session,
    write_opencode_subagents,
)
from workflow_controller.transcript.opencode_store import (
    BUSY_TIMEOUT_MS,
    normalize_part,
    open_ro_db,
    opencode_db_path,
)
from workflow_controller.transcript.sources import (
    TranscriptSource,
    find_opencode_db,
    resolve_roster_source,
    resolve_transcript_source,
)
from workflow_controller.transcript.types import (
    RawInteraction,
    SessionMeta,
    TokenUsage,
    ToolCallInfo,
)

__all__ = [
    "BUSY_TIMEOUT_MS",
    "RawInteraction",
    "SessionMeta",
    "TokenUsage",
    "ToolCallInfo",
    "TranscriptSource",
    "find_opencode_db",
    "normalize_part",
    "open_ro_db",
    "opencode_db_path",
    "resolve_roster_source",
    "resolve_transcript_source",
    "write_opencode_session",
    "write_opencode_subagents",
]
