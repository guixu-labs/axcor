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

"""对话态重建包：截断/种子/重放与 session 导入的内核能力集。

自 ``core/conversation_restore.py``（过渡期平铺）升包而来，吸收原插件的
对话种子与重放编排；L2 定位经 ``transcript/`` 现有面、会话创建经注入端口，
均不自行实现。
"""

from workflow_controller.core.restore.conversation import (
    CONTINUE_SIGNAL,
    ConversationRestoreData,
    ConversationRestorePlan,
    ConversationRestorer,
    RestoreSessionBinding,
    TranscriptPart,
    Truncation,
    resolve_truncation,
)

__all__ = [
    "CONTINUE_SIGNAL",
    "ConversationRestoreData",
    "ConversationRestorePlan",
    "ConversationRestorer",
    "RestoreSessionBinding",
    "TranscriptPart",
    "Truncation",
    "resolve_truncation",
]
