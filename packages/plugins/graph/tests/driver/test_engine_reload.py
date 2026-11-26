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

"""当前定义换装的驱动侧测试：纯换装语义、状态保留与受理拒绝面。

载荷由测试侧翻译生成（等价工厂产物），换装经 reload_definition op
受理；暂停态以 PAUSED 控制通知置位，续跑以 RESUMED 控制通知复位。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
import yaml

from workflow_controller.plugin.run_driver import (
    ControlKind,
    ControlNotification,
    EmitEvent,
    RunWorld,
    StartProcess,
)
from workflow_controller.plugins.graph import translator

from .test_engine import _RUN, build_driver, decide, pick

if TYPE_CHECKING:
    from pathlib import Path

    from workflow_controller.plugins.graph.driver.engine import GraphDriver


def _translate(tmp_path: Path, name: str, nodes: list[dict], **cfg: object) -> Any:
    """翻译一份新定义（测试侧落盘后走生产翻译入口）。"""
    cfg.setdefault("workflow", "wf")
    cfg.setdefault("max_parallel", 2)
    yaml_path = tmp_path / name
    yaml_path.write_text(yaml.safe_dump({**cfg, "nodes": nodes}), encoding="utf-8")
    return translator.translate_program(yaml_path, {}, None)


def _payload(program: Any, **extra: object) -> dict[str, Any]:
    """换装 op 载荷（工厂侧产物 + 宿主侧补的冻结路径）。"""
    payload: dict[str, Any] = {"program": program, "subgraph_plans": []}
    payload.update(extra)
    return payload


def _pause(driver: GraphDriver) -> None:
    """驱动置暂停态（换装受理面前置）。"""
    driver.on_control(ControlNotification(kind=ControlKind.PAUSED))


def _bash(nid: str, command: str, **extra: object) -> dict:
    """bash 节点清单项。"""
    return {"id": nid, "task_type": "bash", "command": command, **extra}


def test_reload_swaps_definition_and_preserves_node_state(tmp_path: Path) -> None:
    """换装只换定义（命令/验收），节点状态/代次/工作区映射档案原样保留。"""
    driver = build_driver(tmp_path, [_bash("a", "echo old")])
    driver.state.set_node_status("a", "passed")
    driver.state.increment_attempts("a")
    driver.state.set_node_workspace("a", "a-w1", 1, "/tmp/ws-a")
    _pause(driver)

    program = _translate(
        tmp_path,
        "wf-new.yaml",
        [
            _bash(
                "a",
                "echo new",
                acceptance={"enabled": True, "type": "script", "script": "true"},
            )
        ],
    )
    result = driver.handle_op(
        "reload_definition", _payload(program, yaml_path="/tmp/frozen/wf-new.yaml")
    )

    assert result["reloaded"] is True
    assert result["added_nodes"] == []
    node = driver.dag.get_node("a")
    assert node["command"] == "echo new"
    assert node["acceptance"]["script"] == "true"
    info = driver.state.snapshot()["nodes"]["a"]
    assert info["status"] == "passed"
    assert info["attempts"] == 1
    assert driver.state.node_workspace("a") == ("a-w1", 1, "/tmp/ws-a")
    # 快照的程序定位指向新冻结入口（恢复据此重翻译）
    assert driver.snapshot()["yaml_path"] == "/tmp/frozen/wf-new.yaml"


def test_reload_added_node_scheduled_with_new_definition(tmp_path: Path) -> None:
    """新增节点补默认档案；恢复续跑后按新定义调度执行。"""
    driver = build_driver(tmp_path, [_bash("a", "echo old")])
    driver.state.set_node_status("a", "passed")
    _pause(driver)
    program = _translate(
        tmp_path,
        "wf-new.yaml",
        [_bash("a", "echo old"), _bash("b", "echo brand-new", depends_on=["a"])],
    )

    result = driver.handle_op("reload_definition", _payload(program))

    assert result["added_nodes"] == ["b"]
    assert driver.state.status("b") == "pending"

    driver.on_control(ControlNotification(kind=ControlKind.RESUMED))
    actions = decide(driver)
    spawns = pick(actions, StartProcess)
    assert any("brand-new" in a.cmd for a in spawns)


def test_reload_rejects_removed_existing_node(tmp_path: Path) -> None:
    """删除既有节点被拒：错误点名无法保留的节点，定义与状态零变化。"""
    driver = build_driver(tmp_path, [_bash("a", "echo old"), _bash("b", "echo keep")])
    driver.state.set_node_status("a", "passed")
    _pause(driver)
    program = _translate(tmp_path, "wf-new.yaml", [_bash("b", "echo keep")])

    with pytest.raises(ValueError, match="既有状态无法保留") as excinfo:
        driver.handle_op("reload_definition", _payload(program))

    assert "a" in str(excinfo.value)
    assert "a" in driver.dag.all_ids()
    assert driver.state.status("a") == "passed"


def test_reload_rejected_while_running(tmp_path: Path) -> None:
    """运行中（未暂停）换装被拒。"""
    driver = build_driver(tmp_path, [_bash("a", "echo old")])
    program = _translate(tmp_path, "wf-new.yaml", [_bash("a", "echo new")])

    with pytest.raises(ValueError, match="仅暂停态受理"):
        driver.handle_op("reload_definition", _payload(program))


def test_reload_remerges_merged_subgraph_topology(tmp_path: Path) -> None:
    """已合并子图节点按预案合回新 DAG：state 档案原样、不重复建账。"""
    driver = build_driver(tmp_path, [_bash("a", "echo old")])
    driver.state.set_subgraph_node_ids("a", ["a:x"])
    driver.state.add_node("a:x")
    driver.state.set_node_status("a:x", "passed")
    _pause(driver)
    program = _translate(tmp_path, "wf-new.yaml", [_bash("a", "echo new")])
    prefixed = [{"id": "a:x", "task_type": "bash", "command": "echo sub", "depends_on": []}]

    result = driver.handle_op(
        "reload_definition", _payload(program, subgraph_plans=[("a", prefixed)])
    )

    assert "a:x" in driver.dag.all_ids()
    assert driver.state.status("a:x") == "passed"
    assert result["added_nodes"] == []  # 子图节点已在册，不算新增


def test_reload_world_op_emits_event_and_marks_changed(tmp_path: Path) -> None:
    """应答通道换装后标记快照待落并发出 definition_reloaded 事件。"""
    driver = build_driver(tmp_path, [_bash("a", "echo old")])
    _pause(driver)
    program = _translate(tmp_path, "wf-new.yaml", [_bash("a", "echo new")])

    result, changed, events = driver.handle_world_op(
        RunWorld(run=_RUN), "reload_definition", _payload(program)
    )

    assert result["reloaded"] is True
    assert changed is True
    assert pick(events, EmitEvent)[0].event_type == "definition_reloaded"
