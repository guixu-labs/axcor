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

"""约定清单加载与文档渲染 —— manifest.yaml 是内核面 HTTP API 与事件清单的唯一事实来源。"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import yaml

MANIFEST_PATH = Path(__file__).with_suffix(".yaml")
API_CONTRACT_DOC_PATH = Path(__file__).parents[4] / "docs" / "api-contract.md"
ROUTE_CLASSES = ("stable", "experimental", "internal")

_CLASS_TITLES = {
    "stable": "Stable API — 已承担兼容责任；只允许向后兼容地增加端点、字段与事件类型",
    "experimental": "Experimental API — 较新或后续迁移/删除的面，不承诺兼容",
    "internal": "Internal API — 内核/控制台内部装配面，不对消费者承诺",
}


@cache
def load_manifest() -> dict:
    """读入清单并做结构校验（未知键、路由行形状、重复端点、payload 行形状即报错）。"""
    with MANIFEST_PATH.open(encoding="utf-8") as stream:
        manifest = yaml.safe_load(stream)

    expected_keys = {"routes", "sse_event_names", "fault_event_types", "event_payload_fields"}
    unknown = set(manifest) - expected_keys
    if unknown:
        raise ValueError(f"清单出现未知顶层键：{sorted(unknown)}")

    seen_global: set[tuple[str, str]] = set()
    classes = manifest["routes"]
    if set(classes) != set(ROUTE_CLASSES):
        raise ValueError(f"路由分类必须恰为 {list(ROUTE_CLASSES)}")
    for class_name in ROUTE_CLASSES:
        seen_in_class: set[tuple[str, str]] = set()
        for row in classes[class_name]:
            if not (isinstance(row, list) and len(row) == 3):
                raise ValueError(f"{class_name} 路由行须为 [方法, 路径, [状态码]]：{row!r}")
            method, path, codes = row
            key = (method, path)
            if key in seen_in_class:
                raise ValueError(f"{class_name} 内重复端点：{key}")
            if key in seen_global:
                raise ValueError(f"跨分类重复端点：{key}")
            seen_in_class.add(key)
            if not all(isinstance(code, int) for code in codes):
                raise ValueError(f"{key} 状态码须为整数列表：{codes!r}")
            seen_global.add(key)

    for row_name, field_row in manifest["event_payload_fields"].items():
        if not isinstance(field_row, dict):
            raise ValueError(f"payload 字段行须为映射：{row_name} → {field_row!r}")
        if "fields" not in field_row:
            raise ValueError(f"payload 字段行缺少 fields：{row_name} → {field_row!r}")
        if "source" not in field_row:
            raise ValueError(f"payload 字段行缺少 source：{row_name} → {field_row!r}")
    return manifest


def routes_by_class() -> dict[str, dict[tuple[str, str], list[int]]]:
    """三分类内核面端点清单：{(方法, 路径): [登记状态码]}。"""
    return {
        class_name: {
            (method, path): list(codes)
            for method, path, codes in load_manifest()["routes"][class_name]
        }
        for class_name in ROUTE_CLASSES
    }


def kernel_routes() -> set[tuple[str, str]]:
    """三分类并集：内核面路由的全集。"""
    return {key for routes in routes_by_class().values() for key in routes}


def sse_event_names() -> list[str]:
    """SSE 单流推送的事件名清单。"""
    return list(load_manifest()["sse_event_names"])


def fault_event_types() -> list[str]:
    """watchdog 告警的 fault.* 词表。"""
    return list(load_manifest()["fault_event_types"])


def event_payload_fields(row_name: str) -> list[str]:
    """按行名取事件 payload 顶层字段清单。"""
    rows = load_manifest()["event_payload_fields"]
    if row_name not in rows:
        raise ValueError(f"payload 字段行未登记：{row_name}")
    return list(rows[row_name]["fields"])


def render_manifest_markdown() -> str:
    """由清单渲染人读 API 文档内容。"""
    lines = [
        "# 内核面 HTTP API 与事件约定",
        "",
        "> 本文档由 `packages/kernel/kernel_tests/contract/manifest.yaml` 生成；改动清单后运行",
        "> `uv run python scripts/generate_api_contract_doc.py` 再提交，生成校验测试会拦截未同步的漂移。",
        "",
        "> 控制台发行包自有的领域端点（如 Context Bay）由该仓另行登记，不在本清单内。",
        "",
    ]
    manifest = load_manifest()
    for class_name in ROUTE_CLASSES:
        routes = manifest["routes"][class_name]
        lines.append(f"## {_CLASS_TITLES[class_name]}（{len(routes)} 条）")
        lines.append("")
        lines.append("| 方法 | 路径 | 登记状态码 |")
        lines.append("|---|---|---|")
        for method, path, codes in routes:
            rendered = ", ".join(str(code) for code in codes) if codes else "—"
            lines.append(f"| {method} | `{path}` | {rendered} |")
        lines.append("")

    lines.append(f"## SSE 事件名（{len(manifest['sse_event_names'])} 个）")
    lines.append("")
    lines.append("、".join(f"`{name}`" for name in manifest["sse_event_names"]))
    lines.append("")

    lines.append(f"## 告警事件词表（{len(manifest['fault_event_types'])} 个）")
    lines.append("")
    lines.append("、".join(f"`{name}`" for name in manifest["fault_event_types"]))
    lines.append("")

    lines.append("## 事件行与 payload 字段")
    lines.append("")
    lines.append("| payload 行 | 顶层字段 | 来源 |")
    lines.append("|---|---|---|")
    for row_name, row in manifest["event_payload_fields"].items():
        fields = "、".join(f"`{field}`" for field in row["fields"])
        if note := row.get("note", ""):
            fields += f"；{note}"
        lines.append(f"| {row_name} | {fields} | {row['source']} |")
    lines.append("")
    return "\n".join(lines)
