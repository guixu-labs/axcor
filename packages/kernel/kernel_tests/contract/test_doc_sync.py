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

"""人读 API 文档同步校验 —— manifest 更新后未重跑生成脚本即红。"""

from __future__ import annotations

from .manifest import API_CONTRACT_DOC_PATH, render_manifest_markdown


def test_api_contract_doc_matches_manifest() -> None:
    """docs/api-contract.md 的内容必须与清单渲染结果一致。"""
    assert API_CONTRACT_DOC_PATH.read_text(encoding="utf-8") == render_manifest_markdown()
