#!/usr/bin/env python
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

"""API 文档生成器：由 packages/kernel/kernel_tests/contract/manifest.yaml 生成 docs/api-contract.md。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "packages" / "kernel"))

from kernel_tests.contract.manifest import API_CONTRACT_DOC_PATH, render_manifest_markdown


def main() -> int:
    API_CONTRACT_DOC_PATH.write_text(render_manifest_markdown(), encoding="utf-8")
    print(f"已生成 {API_CONTRACT_DOC_PATH.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
