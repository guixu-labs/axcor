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

"""Auth API — 认证管理面门面。

所有方法返回纯数据结构；rotator 缺席时沿用各端点的空载形状。
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from workflow_controller.core.orchestrator import Orchestrator

_EMPTY_KEY_STATUS: dict[str, Any] = {
    "mode": "API",
    "reset_at": None,
    "providers": [],
    "active_provider_index": 0,
}


class ProviderInUseError(ValueError):
    """A running session still uses the provider being deleted."""


def _provider_shift_block_ids(pool: Any, provider_index: int, provider_count: int) -> list[str]:
    """引用被删 provider 移位区间的活跃/暂停会话 id 列表（删除拦截面）。"""
    from workflow_controller.session.session import SessionState

    active_states = {
        SessionState.CREATING,
        SessionState.RUNNING,
        SessionState.PAUSED,
        SessionState.WAITING_RETRY,
    }
    # Session.stop() 不清 pause_requested：先暂停再停止（含 stop-all）
    # 会留下 state=STOPPED + pause_requested=True 的终态。硬终态必须
    # 先排除，否则已停止会话被 pause_requested 误判为活跃，用户按
    # 「请先停止」提示操作也无法解除 409。
    terminal_states = {
        SessionState.STOPPED,
        SessionState.COMPLETED,
        SessionState.FAILED,
    }
    blocked_ids: list[str] = []
    for session in pool.list_sessions():
        if session.state in terminal_states:
            continue

        # 真实暂停路径（Session.pause）置 state=IDLE + pause_requested=True，
        # PAUSED 状态只在 wait_if_paused 竞态窗口短暂出现。必须按
        # pause_requested 判定，否则暂停会话被当历史会话跳过；恢复后
        # play() 置 RUNNING，下一次 query 用旧索引解析即热错绑。
        if not getattr(session, "pause_requested", False) and session.state not in active_states:
            continue
        config = session.config
        reviewer_index = config.reviewer.provider_index if config.reviewer is not None else None
        # 拦截移位区间 [被删索引, provider_count)：删除低位 provider 会让
        # 更高索引整体下移，运行中会话每轮 query 都拿旧索引解析 provider，
        # 同索引已指向另一个 key/model（模型相同时甚至无任何告警回调）。
        # 越界索引本就失效，不拦。
        shifted = [
            index
            for index in (config.provider_index, reviewer_index)
            if isinstance(index, int) and provider_index <= index < provider_count
        ]
        if shifted:
            blocked_ids.append(session.id)
    return blocked_ids


def _provider_inactive_sessions(pool: Any) -> list[Any]:
    """池内非活跃会话：硬终态，或无暂停标志的非活跃态（删除后清空绑定面）。"""
    from workflow_controller.session.session import SessionState

    terminal_states = {
        SessionState.STOPPED,
        SessionState.COMPLETED,
        SessionState.FAILED,
    }
    active_states = {
        SessionState.CREATING,
        SessionState.RUNNING,
        SessionState.PAUSED,
        SessionState.WAITING_RETRY,
    }
    inactive: list[Any] = []
    for session in pool.list_sessions():
        if session.state in terminal_states:
            inactive.append(session)
            continue
        if not getattr(session, "pause_requested", False) and session.state not in active_states:
            inactive.append(session)
    return inactive


async def _clear_provider_bindings(pool: Any, sessions: list[Any]) -> None:
    """清空所列会话的 provider 与 reviewer 绑定索引（置 None 并持久化）。"""
    for session in sessions:
        changed = False
        if session.config.provider_index is not None:
            session.config.provider_index = None
            changed = True
        if session.config.reviewer is not None and (
            session.config.reviewer.provider_index is not None
        ):
            session.config.reviewer.provider_index = None
            changed = True
        if changed:
            await pool.persist(session)


class AuthAPI:
    """收口 key 状态、激活、验证与全局认证设置。"""

    def __init__(self, orchestrator: Orchestrator) -> None:
        self._orch = orchestrator

    def key_status(self) -> dict[str, Any]:
        """key 轮换状态载荷。"""
        rotator = self._rotator()
        if rotator is None:
            return dict(_EMPTY_KEY_STATUS)
        return rotator.get_key_status_dict()

    def keys_for_ui(self) -> dict[str, Any]:
        """UI 展示用的 key 清单。"""
        rotator = self._rotator()
        if rotator is None:
            return {"keys": []}
        return {"keys": rotator.get_api_keys_for_ui()}

    async def set_keys_validated(self, keys: list[dict[str, str]]) -> dict[str, Any]:
        """批量写入已验证 key；非法输入抛 ValueError 由路由映射 400。"""
        rotator = self._rotator()
        if rotator is None:
            return {"error": "auth rotator not available"}
        await rotator.set_keys_validated(keys)
        return {"ok": True}

    async def delete_provider(self, provider_index: int) -> bool:
        """删除 provider；仅活跃会话阻塞，池内非活跃会话清空绑定。

        历史会话不阻塞删除，但其 provider 索引是删除前列表的位置坐标：
        删除低位会让高位索引静默指向另一个 provider。池内非活跃会话
        （Run 页重启会续跑）删除后统一置 None——重启时
        _ensure_session_snapshots 补绑当前默认，语义与「供应商已删」
        一致。池外持久化会话无运行时副本，其重启路径（restore →
        restart）以显式报错收场，不做全表改写。

        运行中（含暂停，恢复瞬间会拿旧索引热查）会话仍拒绝，避免热错绑。
        """
        rotator = self._rotator()
        if rotator is None:
            raise RuntimeError("auth rotator not available")
        if not 0 <= provider_index < rotator.get_provider_count():
            return False

        blocked_ids = _provider_shift_block_ids(
            self._orch.pool, provider_index, rotator.get_provider_count()
        )
        if blocked_ids:
            raise ProviderInUseError(
                f"以下会话正在使用该供应商：{', '.join(blocked_ids)}。请先停止后再删除。"
            )

        # 先快照池内「非活跃」会话再删除：删除失败（settings.conf 陈旧
        # 不一致）时零副作用。硬终态确定不可续跑，pause_requested 残留
        # （stop() 不复位）不影响判定；IDLE 仅无暂停标志才算非活跃。
        # 409 阶段已保证活跃/暂停会话不引用被删项；其余会话清空绑定后，
        # 重启时走 _ensure_session_snapshots 的默认 provider 补绑。
        inactive_sessions = _provider_inactive_sessions(self._orch.pool)

        # rotator.delete_provider 内部读写 settings.conf（阻塞磁盘 I/O）。本方法
        # 被 auth_delete_key 路由 await，必须放线程池执行；直接同步调用会阻塞
        # 事件循环（SSE 流、其他会话 query、并发请求全部卡住）。
        if not await asyncio.to_thread(rotator.delete_provider, provider_index):
            return False
        await _clear_provider_bindings(self._orch.pool, inactive_sessions)
        return True

    def activate_provider(self, index: Any) -> dict[str, Any]:
        """按序号激活 provider。"""
        rotator = self._rotator()
        if rotator is None:
            return {"error": "auth rotator not available"}
        if not isinstance(index, int) or index < 0:
            return {"ok": False, "error": "invalid index"}
        ok = rotator.activate_provider(index)
        return {"ok": ok, "active_index": rotator.get_active_provider_index()}

    async def test_active(self) -> dict[str, Any]:
        """重跑当前 provider 的 worker/reviewer 校验。"""
        rotator = self._rotator()
        if rotator is None:
            return {"error": "auth rotator not available"}
        if rotator.get_provider_count() == 0:
            return {"success": False, "error": "No provider configured"}
        return await rotator.validate_provider(
            rotator.get_active_provider_index(),
            force=True,
        )

    async def test_provider(self, provider_index: int) -> dict[str, Any]:
        """强制重测指定 provider 的两种能力。"""
        rotator = self._rotator()
        if rotator is None:
            return {"error": "auth rotator not available"}
        if not 0 <= provider_index < rotator.get_provider_count():
            raise ValueError(f"invalid provider index: {provider_index}")
        return await rotator.validate_provider(provider_index, force=True)

    async def validate_candidate(
        self,
        key: str,
        model: str,
        base_url: str,
        runtime: str | None,
        model_catalog_path: str = "",
    ) -> dict[str, Any]:
        """试测 key/model/base_url/runtime 组合，不写入 provider 列表。

        opencode 探测不依赖 project cwd：provider 定义经 OPENCODE_CONFIG_CONTENT
        注入子进程 env，临时目录即可测；探测到的 kind 由后端存 auth_state.json。
        """
        rotator = self._rotator()
        if rotator is None:
            return {"error": "auth rotator not available"}
        if not key:
            return {"success": False, "error": "Key is required"}
        if not runtime:
            raise ValueError("runtime is required")
        return await rotator.validate_candidate(
            key,
            model,
            base_url,
            runtime,
            model_catalog_path=model_catalog_path,
            force=True,
        )

    def global_settings(self) -> dict[str, Any]:
        """全局认证设置载荷。"""
        rotator = self._rotator()
        if rotator is None:
            return {}
        return rotator.get_global_settings_dict()

    def set_global_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        """更新全局认证设置。"""
        rotator = self._rotator()
        if rotator is None:
            return {"error": "auth rotator not available"}
        rotator.set_global_settings(**settings)
        return {"ok": True}

    def _rotator(self) -> Any:
        return getattr(self._orch, "auth_rotator", None)
