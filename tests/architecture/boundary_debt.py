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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.architecture.dependency_debt import Debt

# Entries are deliberately exact. Add, remove, or update the expected count in
# the same change that alters the underlying boundary occurrences.
# 当前无边界债务：HTTP 组合根不把 orchestrator 放上 app.state，路由只经 CoreAPI 访问内核。
BOUNDARY_DEBT: tuple[Debt, ...] = ()
