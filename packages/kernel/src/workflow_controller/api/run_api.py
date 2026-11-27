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

"""Run API — 宿主级 run 的操作面（run 平面协调器的消费侧薄层）。

Web / Headless 经 CoreAPI 到这里，不直摸协调器或账面；错误统一翻译成
本层类型（RunApiNotFoundError），路由层不需要 import core。成员操作前置
校验 run 在账且 session 是其成员。
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from workflow_controller.core.run_coordinator import RunConfig
from workflow_controller.core.run_export import (
    RunExportFailedError,
    RunExportRefusedError,
    export_refusal_reason,
)
from workflow_controller.core.runs import (
    MemberResumeDeniedError,
    RunNotFoundError,
    SessionNotInRunError,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from workflow_controller.core.run_coordinator import RunCoordinator
    from workflow_controller.core.runs import RunRecord
    from workflow_controller.plugin.run_driver import RunSessionInfo
    from workflow_controller.store.run_events_cursor import RunEventsCursor


class RunApiNotFoundError(LookupError):
    """run_id 不在账面（api 层翻译，路由层 404 用）。"""


class RunApiSessionNotFoundError(LookupError):
    """session 不隶属该 run（成员操作的 404 细分；与 RunApiNotFoundError 平级）。"""


class RunApiInvalidStateError(RuntimeError):
    """成员操作被 run 状态准入拒绝（成员 resume 的 409；非 ValueError——
    成员路径上的 ValueError 属 bug，应冒 500 而非误报冲突）。
    """


class RunApiExportFailedError(RuntimeError):
    """导出执行失败（贡献者异常 / 校验拒绝；后台导出转任务 error 状态）。"""


class RunApiExportRefusedError(ValueError):
    """导出形态拒绝（隐式 run / pending 无事实；路由层经同步拒绝门提前 409）。"""


# 兼容别名：N818 改名前的公共异常名，存量 import/except 仍依赖；
# 确认下游已全部迁移到 *Error 名后删除。
RunApiNotFound = RunApiNotFoundError
RunApiSessionNotFound = RunApiSessionNotFoundError
RunApiInvalidState = RunApiInvalidStateError
RunApiExportFailed = RunApiExportFailedError
RunApiExportRefused = RunApiExportRefusedError


def run_record_to_dict(run: RunRecord) -> dict[str, Any]:
    """账面行的对外形状（label 取 labels.name 键）。"""
    return {
        "run_id": run.run_id,
        "kind": run.kind,
        "format": run.format,
        "status": run.status,
        "program_ref": run.program_ref,
        "params": dict(run.params),
        "label": run.labels.get("name") or None,
        "work_dir": run.work_dir,
        "max_parallel": run.max_parallel,
        "pause_reason": run.pause_reason,
        "created_at": run.created_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "reopened_at": run.reopened_at.isoformat() if run.reopened_at else None,
        "error": run.error,
        "member_count": run.member_count,
        "running_count": run.running_count,
    }


def session_info_to_dict(run_session: RunSessionInfo) -> dict[str, Any]:
    """成员行的对外形状（role/context/实时态）。"""
    return {
        "session_id": run_session.session_id,
        "role": run_session.role,
        "context": run_session.context,
        "state": run_session.state,
        "stop_reason": run_session.stop_reason,
        "in_flight": run_session.in_flight,
    }


class RunAPI:
    """run 生命周期操作面：启停查、组控、转向、成员操作、驱动通道与事件游标。"""

    def __init__(
        self, coordinator: RunCoordinator, events_cursor: RunEventsCursor | None = None
    ) -> None:
        self._coord = coordinator
        self._events_cursor = events_cursor

    @staticmethod
    def export_refusal_reason(run: RunRecord) -> str | None:
        """run 形态拒绝的同步判定（路由层早期 409 用）；与 core 导出入口同一策略。"""
        return export_refusal_reason(run)

    @staticmethod
    def _read_format(program_path: Path | str) -> str:
        path = Path(program_path)
        try:
            head = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, yaml.YAMLError) as e:
            raise ValueError(f"程序文件读不出 format：{path}（{e}）") from e
        fmt = head.get("format") if isinstance(head, dict) else None
        if not isinstance(fmt, str) or not fmt:
            raise ValueError(f"程序文件缺 format 字段：{path}")
        return fmt

    async def start(
        self,
        program_ref: str,
        program_path: Path | str,
        *,
        params: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        fmt: str | None = None,
        work_dir: str | None = None,
        max_parallel: int | None = None,
    ) -> RunRecord:
        """启动 workflow run；fmt 显式给定优先，否则读程序文件头。"""
        self._check_work_dir_outside_runs_root(work_dir)
        path = Path(program_path).expanduser()
        relative_to_project = not path.is_absolute() and bool(work_dir)
        if relative_to_project:
            path = Path(work_dir).expanduser() / path
        path = path.resolve()
        if not path.is_file():
            raise ValueError(f"程序文件不存在：{path}")
        if relative_to_project:
            roots = (Path(work_dir).expanduser().resolve(), Path.home().resolve())
            if not any(path.is_relative_to(root) for root in roots):
                raise ValueError(f"程序文件不在当前项目或用户目录内：{path}")
        if not fmt:
            fmt = self._read_format(path)
        config = RunConfig(
            format=fmt,
            params=dict(params or {}),
            labels=dict(labels or {}),
            # 缺省回落原始程序路径：账面恒持原始位置，驱动侧的
            # agents_dir 解析基准与稳定 graph_id 都以它为据
            program_ref=program_ref or str(path),
            work_dir=work_dir,
            max_parallel=max_parallel,
        )
        return await self._coord.start_run(config, path)

    async def create(
        self,
        label: str,
        *,
        work_dir: str | None = None,
        max_parallel: int | None = None,
    ) -> RunRecord:
        """创建纯容器 run（无程序无驱动，成员全终态自动收尾）。"""
        self._check_work_dir_outside_runs_root(work_dir)
        return await self._coord.create_run(label, work_dir=work_dir, max_parallel=max_parallel)

    async def get_run(self, run_id: str) -> RunRecord:
        try:
            return await self._coord.get_run(run_id)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(run_id) from e

    async def list_runs(
        self, *, include_implicit: bool = False, status: str | None = None
    ) -> list[dict[str, Any]]:
        """run 列表（消费面默认过滤隐式）。"""
        records = await self._coord.list_runs(
            include_implicit=include_implicit,
            status=status,
        )
        return [run_record_to_dict(r) for r in records]

    async def sessions(self, run_id: str) -> list[str]:
        try:
            sessions = await self._coord.list_run_sessions(run_id)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(run_id) from e
        return [s.session_id for s in sessions]

    async def session_infos(self, run_id: str) -> list[dict[str, Any]]:
        """成员全行视图（role/context/实时态，详情端点用）。"""
        try:
            sessions = await self._coord.list_run_sessions(run_id)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(run_id) from e
        return [session_info_to_dict(s) for s in sessions]

    async def state(self, run_id: str) -> dict[str, Any]:
        try:
            return await self._coord.run_state(run_id)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(run_id) from e

    async def driver_snapshot(self, run_id: str) -> dict[str, Any] | None:
        """驱动状态快照（`RunDriver.snapshot()` 落盘值，`restore()` 的输入）；不是配置也不是驱动实例——驱动对象不出内核。未落盘为 None。"""
        return await self._coord.load_driver_snapshot(run_id)

    async def pause(self, run_id: str) -> RunRecord:
        """用户档暂停（隐式 run 等不可控形态以 ValueError 拒）。"""
        try:
            return await self._coord.pause_run(run_id)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(run_id) from e

    async def resume(self, run_id: str, payload: dict[str, Any] | None = None) -> RunRecord:
        """恢复（仅用户档可恢复；auth 档未恢复时以 ValueError 拒）。

        payload 随 RESUMED 通知透传给驱动（外部答复等恢复上下文）。
        """
        try:
            return await self._coord.resume_run(run_id, payload)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(run_id) from e

    async def stop(self, run_id: str) -> RunRecord:
        """终态控制受理：终态重复控制为 no-op（幂等）。"""
        try:
            return await self._coord.stop_run(run_id)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(run_id) from e

    async def restart(self, run_id: str) -> RunRecord:
        """终态 run 重开续跑（非终态/隐式以 ValueError 拒）。"""
        try:
            return await self._coord.restart_run(run_id)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(run_id) from e

    async def steer(self, run_id: str, message: str) -> None:
        """run 级转向注入：有驱动经通知（驱动决定转向谁），无驱动直播成员。"""
        try:
            await self._coord.steer_run(run_id, message)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(run_id) from e

    async def delete(self, run_id: str) -> None:
        """级联删除 run 的全部 Workflow Controller 记录（记录 + run 根），不碰 work_dir。

        run 不在账面（含重复删除）不报 404，一律当作已删。
        """
        await self._coord.delete_run(run_id)

    async def export_run(
        self,
        run_id: str,
        *,
        archive: bool = False,
        progress_cb: Callable[[int, int], None] | None = None,
        archive_name: str | None = None,
    ) -> dict[str, Any]:
        """导出 run 为 workflow bundle（幂等覆盖；形态拒绝为 409）。"""
        try:
            return await self._coord.export_run(
                run_id,
                archive=archive,
                progress_cb=progress_cb,
                archive_name=archive_name,
            )
        except RunNotFoundError as e:
            raise RunApiNotFoundError(run_id) from e
        except RunExportRefusedError as e:
            raise RunApiExportRefusedError(str(e)) from e
        except RunExportFailedError as e:
            raise RunApiExportFailedError(str(e)) from e

    async def pause_session(self, run_id: str, session_id: str) -> None:
        """成员级暂停（记账用户档）。"""
        try:
            await self._coord.pause_run_session(run_id, session_id)
        except SessionNotInRunError as e:
            raise RunApiSessionNotFoundError(e.args[0] if e.args else session_id) from e
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e

    # ── 成员操作（run 平面唯一写者通道；隶属校验在 coordinator）──

    async def resume_session(self, run_id: str, session_id: str) -> None:
        """成员级恢复。"""
        try:
            await self._coord.resume_run_session(run_id, session_id)
        except SessionNotInRunError as e:
            raise RunApiSessionNotFoundError(e.args[0] if e.args else session_id) from e
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e
        except MemberResumeDeniedError as e:
            raise RunApiInvalidStateError(str(e)) from e

    async def stop_session(self, run_id: str, session_id: str) -> None:
        """成员级停止。"""
        try:
            await self._coord.stop_run_session(run_id, session_id)
        except SessionNotInRunError as e:
            raise RunApiSessionNotFoundError(e.args[0] if e.args else session_id) from e
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e

    async def inject_session(self, run_id: str, session_id: str, prompt: str) -> None:
        """成员级定向注入。"""
        try:
            await self._coord.inject_run_session(run_id, session_id, prompt)
        except SessionNotInRunError as e:
            raise RunApiSessionNotFoundError(e.args[0] if e.args else session_id) from e
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e

    async def send_session(
        self,
        run_id: str,
        session_id: str,
        prompt: str,
        *,
        allow_resume: bool = True,
    ) -> bool:
        try:
            return await self._coord.send_run_session(
                run_id,
                session_id,
                prompt,
                allow_resume=allow_resume,
            )
        except SessionNotInRunError as e:
            raise RunApiSessionNotFoundError(e.args[0] if e.args else session_id) from e
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e
        except MemberResumeDeniedError as e:
            raise RunApiInvalidStateError(str(e)) from e

    async def request_driver_op(
        self, run_id: str, op: str, args: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """驱动自定义操作转发（应答通道内执行，API 不直调驱动）。"""
        try:
            return await self._coord.request_driver_op(run_id, op, args or {})
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e

    # ── 回滚查询与异步请求面 ────────────────────────────────────

    async def list_run_scopes(self, run_id: str) -> list[dict[str, Any]]:
        """run 的作用域查询（树不在内存时按盘上配置重建；未开启隔离或 run 已无运行时为空列表）。"""
        try:
            return await self._coord.list_run_scopes(run_id)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e

    async def list_run_checkpoints(
        self, run_id: str, *, scope_id: str | None = None
    ) -> list[dict[str, Any]]:
        """run 的检查点查询（scope_checkpoints 行；可按作用域过滤）。"""
        try:
            return await self._coord.list_run_checkpoints(run_id, scope_id=scope_id)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e

    async def get_run_operation(self, run_id: str, operation_id: str) -> dict[str, Any]:
        """异步操作状态查询（经驱动 get_operation op；run 无运行时为状态错误）。"""
        try:
            return await self._coord.request_driver_op(
                run_id, "get_operation", {"operation_id": operation_id}
            )
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e
        except LookupError as e:
            raise RunApiNotFoundError(str(e)) from e
        except RuntimeError as e:
            raise RunApiInvalidStateError(str(e)) from e

    async def request_run_rollback(
        self,
        run_id: str,
        *,
        node_id: str,
        client_op_id: str,
        expected_epoch: int,
        scope_id: str | None = None,
        checkpoint_id: str | None = None,
        expected_root_epoch: int | None = None,
        reason: str = "",
    ) -> dict[str, Any]:
        """节点回滚请求（异步）：转发驱动 rollback_node op，返回 operation_id。

        显式给出的 checkpoint_id 先对 run 的检查点账本核验存在（无有效
        检查点明确拒绝）；目标已集成（passed）时为已合并回滚，须另携带
        旧根代次 expected_root_epoch（取 run 作用域查询的根行值）；其余
        前置（traceback 开关、代次）由驱动在受理时校验。
        """
        if checkpoint_id:
            rows = await self.list_run_checkpoints(run_id)
            if not any(row["checkpoint_id"] == checkpoint_id for row in rows):
                raise ValueError(f"检查点不存在于该 run：{checkpoint_id}")
        args: dict[str, Any] = {
            "node_id": node_id,
            "client_op_id": client_op_id,
            "expected_epoch": expected_epoch,
        }
        if scope_id is not None:
            args["scope_id"] = scope_id
        if checkpoint_id is not None:
            args["checkpoint_id"] = checkpoint_id
        if expected_root_epoch is not None:
            args["expected_root_epoch"] = expected_root_epoch
        if reason:
            args["reason"] = reason
        try:
            return await self._coord.request_driver_op(run_id, "rollback_node", args)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e

    async def request_run_rollback_nodes(
        self,
        run_id: str,
        *,
        targets: list[dict[str, Any]],
        client_op_id: str,
        expected_root_epoch: int | None = None,
        reason: str = "",
    ) -> dict[str, Any]:
        """批量节点回滚请求（异步）：转发驱动 rollback_nodes op，返回 operation_id。

        targets 每项携带 node_id 与 expected_epoch（可选 scope_id 与
        checkpoint_id）；每个显式给出的 checkpoint_id 先对 run 的检查点
        账本核验存在（无有效检查点明确拒绝）；其余前置（traceback 开关、
        映射/代次、目标归并）由驱动在受理时校验。含已合并目标时须另携
        带旧根代次 expected_root_epoch。
        """
        if not isinstance(targets, list) or not targets:
            raise ValueError("targets 须为非空列表")
        explicit_checkpoints: list[str] = []
        for entry in targets:
            if not isinstance(entry, dict) or not entry.get("node_id"):
                raise ValueError("targets 每项须为携带 node_id 的对象")
            if entry.get("checkpoint_id"):
                explicit_checkpoints.append(str(entry["checkpoint_id"]))
        if explicit_checkpoints:
            rows = await self.list_run_checkpoints(run_id)
            known = {row["checkpoint_id"] for row in rows}
            missing = [cp for cp in explicit_checkpoints if cp not in known]
            if missing:
                raise ValueError(f"检查点不存在于该 run：{', '.join(missing)}")
        args: dict[str, Any] = {
            "targets": [
                {
                    "node_id": entry["node_id"],
                    "expected_epoch": entry.get("expected_epoch"),
                    **(
                        {"scope_id": entry["scope_id"]} if entry.get("scope_id") is not None else {}
                    ),
                    **(
                        {"checkpoint_id": entry["checkpoint_id"]}
                        if entry.get("checkpoint_id") is not None
                        else {}
                    ),
                }
                for entry in targets
            ],
            "client_op_id": client_op_id,
        }
        if expected_root_epoch is not None:
            args["expected_root_epoch"] = expected_root_epoch
        if reason:
            args["reason"] = reason
        try:
            return await self._coord.request_driver_op(run_id, "rollback_nodes", args)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e

    async def request_subagent_rollback(
        self,
        run_id: str,
        *,
        node_id: str,
        client_op_id: str,
        expected_epoch: int,
        checkpoint_id: str,
        scope_id: str | None = None,
        replay_dispatch: bool = False,
        expected_root_epoch: int | None = None,
        reason: str = "",
    ) -> dict[str, Any]:
        """子代理回滚请求（异步）：转发驱动 rollback_subagent op，返回 operation_id。

        检查点须为该 run 的派发组检查点行；replay_dispatch 开启时先做
        「单 task、无其他副作用」校验，多副作用组明确拒绝。目标已集成
        到根（节点 passed）时按已合并回滚传播，须另携带旧根代次
        expected_root_epoch。
        """
        rows = await self.list_run_checkpoints(run_id)
        row = next((r for r in rows if r["checkpoint_id"] == checkpoint_id), None)
        if row is None:
            raise ValueError(f"检查点不存在于该 run：{checkpoint_id}")
        if row.get("trigger") != "before_child" or not (row.get("dispatch_anchor") or {}).get(
            "dispatch_group_id"
        ):
            raise ValueError(f"回滚目标须为派发组检查点：{checkpoint_id}")
        if replay_dispatch:
            await self._coord.validate_dispatch_replay(run_id, checkpoint_id)
        args: dict[str, Any] = {
            "node_id": node_id,
            "client_op_id": client_op_id,
            "expected_epoch": expected_epoch,
            "checkpoint_id": checkpoint_id,
            "replay_dispatch": replay_dispatch,
        }
        if scope_id is not None:
            args["scope_id"] = scope_id
        if expected_root_epoch is not None:
            args["expected_root_epoch"] = expected_root_epoch
        if reason:
            args["reason"] = reason
        try:
            return await self._coord.request_driver_op(run_id, "rollback_subagent", args)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e

    # ── 单会话工作区查询与回滚面（HTTP 路由装配住控制台发行包）──

    async def get_session_live_view(
        self, session_id: str, path: str | None = None
    ) -> dict[str, Any]:
        """单会话托管工作区的只读实时视图（目录树或单文件内容）。"""
        try:
            return await self._coord.get_session_live_view(session_id, path)
        except LookupError as e:
            raise RunApiSessionNotFoundError(str(e)) from e

    async def list_session_workspace_checkpoints(self, session_id: str) -> list[dict[str, Any]]:
        """单会话树的检查点查询（子代理回滚的核对面）。"""
        try:
            return await self._coord.list_session_workspace_checkpoints(session_id)
        except LookupError as e:
            raise RunApiSessionNotFoundError(str(e)) from e

    async def request_session_subagent_rollback(
        self,
        session_id: str,
        *,
        checkpoint_id: str,
        expected_epoch: int,
        client_op_id: str,
        replay_dispatch: bool = False,
    ) -> dict[str, Any]:
        """单会话子代理回滚：组检查点恢复并绑定恢复会话，返回恢复结论。

        检查点先对会话树核验为派发组恢复点行；expected_epoch 为主写者
        作用域当前代次（取检查点行 scope_epoch 之外的实时值须先经查询面）。
        """
        rows = await self.list_session_workspace_checkpoints(session_id)
        row = next((r for r in rows if r["checkpoint_id"] == checkpoint_id), None)
        if row is None:
            raise ValueError(f"检查点不存在于该会话：{checkpoint_id}")
        if row.get("trigger") != "before_child" or not (row.get("dispatch_anchor") or {}).get(
            "dispatch_group_id"
        ):
            raise ValueError(f"回滚目标须为派发组检查点：{checkpoint_id}")
        try:
            return await self._coord.restore_session_subagent(
                session_id,
                checkpoint_id=checkpoint_id,
                expected_epoch=expected_epoch,
                client_op_id=client_op_id,
                replay_dispatch=replay_dispatch,
            )
        except LookupError as e:
            raise RunApiSessionNotFoundError(str(e)) from e

    # ── 工作区残留清理面（HTTP 路由装配住控制台发行包）──

    async def reap_workspace_residue(self) -> dict[str, Any]:
        """显式触发一轮工作区残留清理（崩溃残留树与过期账本行）。"""
        return await self._coord.reap_workspace_residue()

    async def reload_run_definition(
        self, run_id: str, program_path: str | Path | None = None
    ) -> dict[str, Any]:
        """重读当前工作流定义并更新现有 run 的任务定义。

        保留节点状态与产物、不新建 run；仅暂停态受理（运行中/终态拒绝）。
        program_path 缺省重读 run 的原始程序位置（RSI 应用 patch 后的同
        一文件），显式新路径按新位置重读。
        """
        try:
            return await self._coord.reload_run_definition(run_id, program_path)
        except RunNotFoundError as e:
            raise RunApiNotFoundError(e.args[0] if e.args else run_id) from e

    # ── 驱动通道与事件 ──────────────────────────────────────────

    async def fetch_events(
        self, run_id: str, cursor: str | None = None, limit: int | None = None
    ) -> tuple[list[dict[str, Any]], str, int]:
        """run 事件增量（run 键 + 成员打戳两表合并序）；limit 为钳制后实际生效页长。"""
        await self.get_run(run_id)
        if self._events_cursor is None:
            raise RuntimeError("run 事件游标未装配")
        return self._events_cursor.fetch(run_id, cursor, limit)

    def _check_work_dir_outside_runs_root(self, work_dir: str | None) -> None:
        """显式 work_dir 不得落进 run 数据根：delete_run 物理删除 run 根，
        位于其内的 agent 产物会被连带删掉。
        """
        if not work_dir:
            return
        data_root = self._coord.runs_data_root()
        wd = Path(work_dir).expanduser().resolve()
        if wd == data_root or data_root in wd.parents:
            raise ValueError(
                f"work_dir 不得位于 run 数据根内（{data_root}）——"
                "run 根删除时全量移除，agent 产物须放在数据根之外"
            )
