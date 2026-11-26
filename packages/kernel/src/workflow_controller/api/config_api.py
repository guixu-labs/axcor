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

"""Config API — 配置读写。

所有读写通过 Pydantic 验证, 不再绕过验证直接写 yaml。

**两类配置分离**(修「Settings save 对项目设置无效」):
- 全局 config(repo根 workflow-controller.yaml)= web 基础设施:hook_server / skills_path / agents_path /
  project_dir(指针)/ sessions / reviewer.{enabled,model,timeout_seconds,thinking_*}。get_config_dict /
  update_config / load_from_dir 只管这些。
- 项目 config(<project_dir>/workflow-controller.yaml)= 项目配置:runtime / starting_prompt / system_prompt /
  loop / reviewer.{task_goal,thinking_*} / stages。session 启动从项目 yaml 现读;项目设置面板
  经 get/update_project_config 读写项目 yaml。stages 不进 UI(手改 yaml)。
"""

from __future__ import annotations

import contextlib
import json
import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from workflow_controller.config.reviewer import explicit_project_reviewer_thinking
from workflow_controller.config.settings import DEFAULT_BRAND_NAME, AppConfig, WatchdogConfig

if TYPE_CHECKING:
    from workflow_controller.core.orchestrator import Orchestrator

logger = logging.getLogger(__name__)


def _str_representer(dumper: Any, data: Any) -> Any:
    """yaml representer:含换行的 str → block literal style(`|`);其余让 emitter 自动选。

    PyYAML safe_dump 默认对 multiline str 用单引号标量,而 YAML 单引号里空行=真换行、
    行内换行=折叠成空格 → 每个原换行被撑成空行(视觉「隔行」)。block literal 干净一行
    一行、可读、git diff 友好、round-trip 安全。
    """
    style = "|" if "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


class _BlockLiteralDumper(yaml.SafeDumper):
    """SafeDumper + multiline str → block literal representer。

    隔离子类,不污染全局 SafeDumper(进程内别处 safe_dump 行为不变)。
    """


_BlockLiteralDumper.add_representer(str, _str_representer)


def _is_traceback_fork_session(session: Any) -> bool:
    """是否为检查点重放 fork session（据 birth_traceback.fork_root 判定）。"""
    try:
        pm = getattr(session, "plugin_metadata", None) or {}
        bt = (pm.get("agent_traceback") or {}).get("birth_traceback") or {}
        return bool(bt.get("fork_root"))
    except Exception:
        return False


def _transient_roots() -> tuple[Path, ...]:
    """系统临时区根集合（resolved 去重）。

    环境约定下 OS/清理任务随时可能清空的目录树：``tempfile.gettempdir()``
    （macOS 即 ``/var/folders/…/T/``）补 ``/tmp`` 与 ``/var/tmp``。resolve 统一
    符号链接（macOS ``/var`` → ``/private/var``），前缀判定才可靠。
    """
    roots: list[Path] = []
    for cand in (tempfile.gettempdir(), "/tmp", "/var/tmp"):
        try:
            p = Path(cand).expanduser().resolve()
        except OSError:
            continue
        if p not in roots:
            roots.append(p)
    return tuple(roots)


def _is_transient_dir(raw: str) -> bool:
    """路径是否位于系统临时区（resolve 后前缀判定）。解析失败视为瞬态（fail-closed）。"""
    try:
        p = Path(raw).expanduser().resolve()
    except Exception:
        return True
    return any(p == r or r in p.parents for r in _transient_roots())


def _is_persistent_project_dir(raw: str) -> bool:
    """项目目录卫生谓词——入库与展示共用的唯一判定（单一 chokepoint）。

    非空、绝对路径（相对路径含 ``.`` 哨兵无法与 project_dir 精确匹配，不可作
    项目目录）、磁盘上的真实目录、且不在系统临时区（临时区目录随时被 OS 清理，
    不构成可持久的「项目」——测试遗留的 tempdir 从此不再挂进「最近项目」）。
    """
    raw = (raw or "").strip()
    if not raw:
        return False
    p = Path(raw)
    if not p.is_absolute():
        return False
    if _is_transient_dir(raw):
        return False
    try:
        return p.is_dir()
    except Exception:
        return False


def _explicit_project_yaml(project_dir: str) -> dict[str, Any]:
    """读项目 yaml 的原始 dict（显式配置判定用）；缺失/解析失败返回空 dict。"""
    explicit: dict[str, Any] = {}
    yaml_path = Path(project_dir) / "workflow-controller.yaml"
    if yaml_path.exists():
        try:
            loaded = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
            if isinstance(loaded, dict):
                explicit = loaded
        except Exception:
            logger.warning("项目 yaml %s 解析失败，忽略其中的显式配置", yaml_path)
    return explicit


def _merge_project_yaml_data(data: dict[str, Any], partial: dict[str, Any]) -> None:
    """把 partial 的项目字段合并进 yaml 读出的 data（保键合并，未触及的键不动）。"""
    # 顶层项目标量
    for k in ("model", "runtime", "cli_wrapper", "starting_prompt", "system_prompt"):
        if k in partial:
            data[k] = partial[k]
    # loop 合并(保留已有 loop 键)
    if isinstance(partial.get("loop"), dict):
        lp = data.get("loop")
        if not isinstance(lp, dict):
            lp = {}
        if "evaluator" in partial["loop"]:
            lp["evaluator"] = partial["loop"]["evaluator"]
        if "max_iterations" in partial["loop"]:
            lp["max_iterations"] = partial["loop"]["max_iterations"]
        data["loop"] = lp
    # reviewer 项目字段合并，保留未触及的键。
    if isinstance(partial.get("reviewer"), dict):
        rv = data.get("reviewer")
        if not isinstance(rv, dict):
            rv = {}
        touched = False
        if "task_goal" in partial["reviewer"]:
            rv["task_goal"] = partial["reviewer"]["task_goal"]
            touched = True
        thinking_mode = partial["reviewer"].get("thinking_mode")
        if thinking_mode in (None, "") and isinstance(
            partial["reviewer"].get("thinking_enabled"), bool
        ):
            thinking_mode = "enabled" if partial["reviewer"]["thinking_enabled"] else "disabled"
        if thinking_mode in ("disabled", "enabled", "adaptive", "omitted"):
            rv["thinking_mode"] = thinking_mode
            touched = True
        if "thinking_budget_tokens" in partial["reviewer"]:
            try:
                rv["thinking_budget_tokens"] = max(
                    1024, int(partial["reviewer"]["thinking_budget_tokens"])
                )
                touched = True
            except (TypeError, ValueError):
                pass
        if touched:
            data["reviewer"] = rv


def _apply_web_fields(cfg: AppConfig, partial: dict[str, Any]) -> None:
    """应用 web 基础字段（project_dir/skills_path/agents_path/brand_name）。"""
    if "project_dir" in partial:
        cfg.project_dir = partial["project_dir"]
    if "skills_path" in partial:
        cfg.skills_path = partial["skills_path"]
    if "agents_path" in partial:
        cfg.agents_path = partial["agents_path"]
    if "brand_name" in partial:
        # 空/纯空白回落默认品牌，避免界面出现无标题品牌
        cfg.brand_name = str(partial["brand_name"] or "").strip() or DEFAULT_BRAND_NAME


def _apply_hook_server(cfg: AppConfig, partial: dict[str, Any]) -> None:
    """应用 hook_server 段（enabled/host）；端口只归 CLI。"""
    hs = partial.get("hook_server")
    if not isinstance(hs, dict):
        return
    if "port" in hs:
        raise ValueError("hook_server.port 不再支持配置；请使用 --hook-port")
    if isinstance(hs.get("enabled"), bool):
        cfg.hook_server.enabled = hs["enabled"]
    if "host" in hs:
        cfg.hook_server.host = hs["host"]


def _apply_reviewer_fields(cfg: AppConfig, partial: dict[str, Any]) -> None:
    """应用 reviewer 段字段（model/timeout/thinking；task_goal 是项目字段不在此处理）。"""
    rv = partial.get("reviewer")
    if not isinstance(rv, dict):
        return
    if "model" in rv:
        cfg.reviewer.model = rv["model"]
    if "timeout_seconds" in rv:
        cfg.reviewer.timeout_seconds = rv["timeout_seconds"]
    thinking_mode = rv.get("thinking_mode")
    if thinking_mode in (None, "") and isinstance(rv.get("thinking_enabled"), bool):
        thinking_mode = "enabled" if rv["thinking_enabled"] else "disabled"
    if thinking_mode in ("disabled", "enabled", "adaptive", "omitted"):
        cfg.reviewer.thinking_mode = thinking_mode
    if "thinking_budget_tokens" in rv:
        with contextlib.suppress(TypeError, ValueError):
            cfg.reviewer.thinking_budget_tokens = max(1024, int(rv["thinking_budget_tokens"]))


def _apply_watchdog_key(cfg: AppConfig, wd: dict[str, Any], key: str) -> None:
    """应用单个 watchdog 键（None 视为未提供跳过）。"""
    if key in wd and wd[key] is not None:
        setattr(cfg.watchdog, key, wd[key])


def _apply_watchdog_fields(cfg: AppConfig, partial: dict[str, Any]) -> None:
    """就地写 watchdog 段保持热更新（检测核心持同一对象）；坏值由赋值校验抛错。"""
    wd = partial.get("watchdog")
    if not isinstance(wd, dict):
        return
    for key in WatchdogConfig.model_fields:
        _apply_watchdog_key(cfg, wd, key)


class ConfigAPI:
    """配置读写, 通过 Pydantic 验证保证一致性。"""

    def __init__(self, orchestrator: Orchestrator, config_path: Path) -> None:
        self._orch = orchestrator
        self._config_path = config_path

    # ── 全局(web)config ────────────────────────────────────────────────────

    @property
    def project_dir(self) -> str:
        """当前项目目录（全局配置指针）。"""
        return self._orch.config.project_dir

    @property
    def claude_scan_user_homes(self) -> bool:
        """Claude 会话定位是否扫描其他用户主目录。"""
        return bool(self._orch.config.claude_scan_user_homes)

    @property
    def _recent_dirs_path(self) -> Path:
        return self._cfg_dir() / "recent_dirs.json"

    @staticmethod
    def _load_project_appconfig(project_dir: str) -> AppConfig:
        """读项目 yaml 成 AppConfig(无/失败 → 默认)。"""
        yaml_path = Path(project_dir) / "workflow-controller.yaml"
        if yaml_path.exists():
            try:
                return AppConfig.from_yaml(yaml_path)
            except Exception:
                return AppConfig()
        return AppConfig()

    # ── 最近项目（后端单一来源，前端只读 SSE config.recent_dirs）──────────────

    #: recent_dirs.json 用户手选目录上限（recency 排序）。
    RECENT_DIRS_MAX = 10

    def get_section(self, name: str) -> dict[str, Any] | None:
        """读模型外配置段（不透明透传，段含义与校验归段所有者），无则 None。"""
        if name in AppConfig.model_fields:
            return None
        value = getattr(self._orch.config, name, None)
        return dict(value) if isinstance(value, dict) else None

    def update_section(self, name: str, mapping: dict[str, Any]) -> None:
        """写模型外配置段并落盘：键集合与取值由段所有者保证，内核不校验。"""
        if name in AppConfig.model_fields:
            raise ValueError(f"config section '{name}' is kernel-owned")
        setattr(self._orch.config, name, dict(mapping))
        self.save()

    def get_recent_dirs(self) -> list[str]:
        """合并 recent_dirs.json（用户手选，recency 优先）+ pool 中非 fork session 的
        ``config.cwd``，去重返回统一列表。

        用户手选目录在前、session 目录去重补充在后；session 目录不挤占
        ``RECENT_DIRS_MAX`` 上限。随 ``get_config_dict`` → ``config_update`` SSE 推送。
        """
        user_dirs = self._load_recent_dirs()
        session_dirs = self._collect_session_cwds()
        seen: set[str] = set()
        out: list[str] = []
        for d in user_dirs:
            if d and d not in seen:
                seen.add(d)
                out.append(d)
        for d in session_dirs:
            if d and d not in seen:
                seen.add(d)
                out.append(d)
        return out

    def add_recent_dir(self, path: str) -> tuple[list[str], str | None]:
        """添加目录到 recent_dirs.json（新目录置顶、已在列表不重排、上限 RECENT_DIRS_MAX）。

        校验单点在本方法（route 只透传结果）：相对路径/不存在/非目录拒绝为
        ``path not found``，系统临时区拒绝为 ``transient dir``（临时区随时被 OS
        清理，不构成可持久的「项目」）。合法项存 resolve() 后的规范绝对值。
        返回 (更新后的合并列表, 错误串|None)。
        """
        raw = (path or "").strip()
        if not raw:
            return self.get_recent_dirs(), "dir is required"
        p = Path(raw).expanduser()
        if not p.is_absolute() or not p.is_dir():
            return self.get_recent_dirs(), "path not found"
        if _is_transient_dir(raw):
            return self.get_recent_dirs(), "transient dir"
        norm = str(p.resolve())
        dirs = self._load_recent_dirs()
        if norm not in dirs:
            dirs = [norm, *dirs][: self.RECENT_DIRS_MAX]
            self._save_recent_dirs(dirs)
        return self.get_recent_dirs(), None

    def remove_recent_dir(self, path: str) -> list[str]:
        """从 recent_dirs.json 移除目录（真删，单一来源下即时生效，无 SSE 闪回）。

        session 的 cwd 不在此删（session 目录由 events.db 推，仍会出现在合并列表）。
        返回更新后的合并列表。
        """
        path = (path or "").strip()
        dirs = [d for d in self._load_recent_dirs() if d != path]
        self._save_recent_dirs(dirs)
        return self.get_recent_dirs()

    def get_config_dict(self) -> dict[str, Any]:
        """返回全局 web 配置(web 基础设施 + project_dir 指针 + sessions)。

        **不含**项目字段(runtime/starting_prompt/system_prompt/loop/reviewer.task_goal)——
        那些归 get_project_config(项目 yaml)。reviewer.mode 是运行态(orch.reviewer_mode)。
        """
        cfg = self._orch.config
        return {
            # 手改 yaml 置空会绕过写入口，读侧同样回落默认品牌
            "brand_name": cfg.brand_name.strip() or DEFAULT_BRAND_NAME,
            "project_dir": cfg.project_dir,
            "skills_path": cfg.skills_path,
            "agents_path": cfg.agents_path,
            "hook_server": {
                "enabled": cfg.hook_server.enabled,
                "host": cfg.hook_server.host,
            },
            "reviewer": {
                "enabled": cfg.reviewer.enabled,
                "model": cfg.reviewer.model,
                "timeout_seconds": cfg.reviewer.timeout_seconds,
                "thinking_mode": cfg.reviewer.thinking_mode,
                "thinking_budget_tokens": cfg.reviewer.thinking_budget_tokens,
                "mode": self._orch.reviewer_mode,
            },
            "sessions": [
                {
                    "name": s.name,
                    "role": s.role,
                    "cwd": s.cwd,
                    "model": s.model,
                    "permission_mode": s.permission_mode,
                    "max_turns": s.max_turns,
                    "max_budget_usd": s.max_budget_usd,
                    "loop": {
                        "max_iterations": s.loop.max_iterations if s.loop else 10,
                        "evaluator": s.loop.evaluator if s.loop else None,
                    }
                    if s.loop
                    else None,
                }
                for s in cfg.sessions
            ],
            "watchdog": cfg.watchdog.model_dump(),
            "recent_dirs": self.get_recent_dirs(),
        }

    def update_config(self, partial: dict[str, Any]) -> dict[str, Any]:
        """更新全局 web 配置并保存。只认 web 字段;**项目字段(runtime/starting_prompt/
        system_prompt/loop/reviewer.task_goal)在这里被忽略**——归 update_project_config。
        """
        cfg = self._orch.config
        _apply_web_fields(cfg, partial)
        _apply_hook_server(cfg, partial)
        _apply_reviewer_fields(cfg, partial)
        rv = partial.get("reviewer")
        if isinstance(rv, dict) and "mode" in rv:
            self._orch.set_reviewer_mode(rv["mode"])
        _apply_watchdog_fields(cfg, partial)
        self.save()
        return self.get_config_dict()

    def load_from_dir(self, project_dir: str) -> dict[str, Any]:
        """选目录:只在全局写 project_dir 指针 + save。

        项目级配置（含领域自有段的项目覆盖）由各段所有者在读取时现场合并，
        不灌进全局内存——避免切项目时旧配置残留。
        """
        self._orch.config.project_dir = project_dir
        self.save()
        return self.get_config_dict()

    async def restart_reviewer(self) -> None:
        """Stop and restart the reviewer subprocess with current config."""
        await self._orch.restart_reviewer()

    def save(self) -> None:
        """将全局 web 配置写回 workflow-controller-config.yaml——**只写 web 字段**。

        项目字段(runtime/starting_prompt/system_prompt/loop/stages/reviewer.task_goal)归项目
        workflow-controller.yaml(由项目设置面板读写),不进全局 yaml。全局 yaml 文件名是
        workflow-controller-config.yaml，和项目 workflow-controller.yaml 永远不重合，不会互相覆盖。
        """
        cfg = self._orch.config
        data = cfg.model_dump(mode="json", exclude_none=True)
        for k in ("runtime", "starting_prompt", "system_prompt", "loop", "stages"):
            data.pop(k, None)
        if isinstance(data.get("reviewer"), dict):
            data["reviewer"].pop("task_goal", None)

        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        self._config_path.write_text(
            yaml.dump(
                data,
                Dumper=_BlockLiteralDumper,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
            ),
            encoding="utf-8",
        )

    def get_project_config(self, project_dir: str | None = None) -> dict[str, Any]:
        """读项目 yaml,返回项目标量字段(runtime/starting_prompt/system_prompt/loop/
        reviewer/graph_yaml)。无 yaml/解析失败 → AppConfig 默认。project_dir 省略 → 用全局 config 的
        project_dir(当前项目)。未显式配置的 reviewer.enabled/loop 字段返回 None，
        供新建会话表单按插件状态选择默认值；其他字段由 pydantic 填默认值。
        graph_yaml 若为相对路径,基于 project_dir 解析为绝对路径返回,
        保证前端透传给 runs API 启动 workflow run 时程序路径可用。
        """
        project_dir = project_dir or self._orch.config.project_dir
        cfg = self._load_project_appconfig(project_dir)
        explicit = _explicit_project_yaml(project_dir)
        raw_loop = explicit.get("loop") if isinstance(explicit.get("loop"), dict) else {}
        raw_reviewer = (
            explicit.get("reviewer") if isinstance(explicit.get("reviewer"), dict) else {}
        )
        explicit_thinking = explicit_project_reviewer_thinking(project_dir, raw=explicit)
        graph_yaml = cfg.graph_yaml or ""
        if graph_yaml and not Path(graph_yaml).is_absolute():
            base = Path(project_dir) if project_dir else Path.cwd()
            graph_yaml = str((base / graph_yaml).resolve())
        return {
            "model": cfg.model,
            "runtime": cfg.runtime,
            "cli_wrapper": cfg.cli_wrapper,
            "starting_prompt": cfg.starting_prompt,
            "system_prompt": cfg.system_prompt,
            "loop": {
                "evaluator": cfg.loop.evaluator if raw_loop.get("evaluator") is not None else None,
                "max_iterations": (
                    cfg.loop.max_iterations if raw_loop.get("max_iterations") is not None else None
                ),
            },
            "reviewer": {
                "enabled": cfg.reviewer.enabled
                if raw_reviewer.get("enabled") is not None
                else None,
                "model": cfg.reviewer.model,
                "thinking_mode": explicit_thinking.mode,
                "thinking_budget_tokens": explicit_thinking.budget_tokens,
                "task_goal": cfg.reviewer.task_goal,
            },
            "graph_yaml": graph_yaml,
        }

    def update_project_config(
        self, project_dir: str | None = None, partial: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """写回项目 yaml:保键合并(load dict → 更新已知项目键 → dump),**不丢** stages 等
        任何已有键;无 yaml 则新建。partial 支持顶层 runtime/starting_prompt/system_prompt +
        嵌套 loop{evaluator,max_iterations} / reviewer{task_goal}。返回更新后的项目配置。
        project_dir 省略 → 用全局 config 的 project_dir(当前项目)。
        """
        project_dir = project_dir or self._orch.config.project_dir
        partial = partial or {}
        yaml_path = Path(project_dir) / "workflow-controller.yaml"
        data: dict[str, Any] = {}
        if yaml_path.exists():
            try:
                loaded = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
            except Exception:
                loaded = {}
            if isinstance(loaded, dict):
                data = loaded
        _merge_project_yaml_data(data, partial)
        yaml_path.parent.mkdir(parents=True, exist_ok=True)
        yaml_path.write_text(
            yaml.dump(
                data,
                Dumper=_BlockLiteralDumper,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        # Sync model change into memory so new sessions pick it up immediately
        if "model" in partial:
            self._orch.config.model = partial["model"]
        return self.get_project_config(project_dir)

    def _cfg_dir(self) -> Path:
        """recent_dirs.json 等主机本地状态的存放目录（与 auth_state.json 同目录）。

        ``boot()`` 把 cfg_dir 挂到 orch 上；未挂（如单元测试 mock）时回退
        ``default_config_dir()``。
        """
        cd = getattr(self._orch, "cfg_dir", None)
        if isinstance(cd, Path):
            return cd
        from workflow_controller.core.auth_rotator import default_config_dir

        return default_config_dir()

    # ── 项目 config(读写项目 <project_dir>/workflow-controller.yaml) ─────────────────────

    def _load_recent_dirs(self) -> list[str]:
        """读 recent_dirs.json（recency 排序，上限 RECENT_DIRS_MAX）。无/坏 → []。

        json.loads 原子性：异常时无半成品可清理，故异常分支返回 [] 不做 tempdir 回退。
        存量自清洁：卫生谓词不过关的条目（系统临时区/已不存在/相对路径）读盘时
        剔除并回写——用户无需手动逐个移除历史噪声；列表收敛后不再写盘。
        """
        try:
            p = self._recent_dirs_path
            if not p.exists():
                return []
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, list):
                dirs = [str(d) for d in data if isinstance(d, str)][: self.RECENT_DIRS_MAX]
            else:
                dirs = []
        except Exception:
            return []
        clean = [d for d in dirs if _is_persistent_project_dir(d)]
        if len(clean) != len(dirs):
            with contextlib.suppress(Exception):
                self._save_recent_dirs(clean)
        return clean

    def _save_recent_dirs(self, dirs: list[str]) -> None:
        p = self._recent_dirs_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps(dirs[: self.RECENT_DIRS_MAX], ensure_ascii=False),
            encoding="utf-8",
        )

    def _collect_session_cwds(self) -> list[str]:
        """收集 pool 中非 fork session 的持久项目目录 cwd（去重、保留顺序）。

        fork session（birth_traceback.fork_root）的隔离 cwd 不并入顶层——这是
        session 级语义判定；其余 cwd 一律经卫生谓词过滤（绝对、真实存在、非系统
        临时区——ghost session 指向已被删除/清理的目录时不再污染「最近项目」）。
        """
        out: list[str] = []
        seen: set[str] = set()
        try:
            for s in self._orch.pool.list_sessions():
                cwd = getattr(getattr(s, "config", None), "cwd", "") or ""
                if _is_traceback_fork_session(s) or not _is_persistent_project_dir(cwd):
                    continue
                if cwd not in seen:
                    seen.add(cwd)
                    out.append(cwd)
        except Exception:
            logger.debug("遍历 session 列表提取项目目录失败", exc_info=True)
        return out
