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

"""graph 驱动包：纯决策代码的归宿。

- engine：正规 RunDriver（decide 步进、阶段机与动作产出）
- dag：拓扑纯函数（就绪/祖先/子图合并）
- state：节点状态机 + 重试账 + 回滚账（纯内存）
- scheduler：就绪集/失败传播/终局清扫/回滚数学/子图恢复策略
- gates：验收判定协议与重试/回滚去向决策
- prompts：提示词与报告文本组装

决策代码不含任何 I/O 原语（纯度门禁逐行扫描本目录）；机制半边由宿主
执行服务承接。本包只依赖包内模块与 template 文本渲染。
"""
