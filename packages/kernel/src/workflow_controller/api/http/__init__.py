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

"""CoreAPI 的 HTTP 表现：内核路由、插件路由挂载与内核独立应用工厂。

子包按需导入：进程内嵌入方只导入 ``workflow_controller.api.core_api`` 时不会加载它。
决策见 ADR-0002。
"""
