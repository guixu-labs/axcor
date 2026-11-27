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

"""Workflow Controller CLI — Headless runner + Web UI."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from workflow_controller.config.settings import AppConfig


def _load_app_config(config_path: str) -> AppConfig:
    from workflow_controller.config.settings import AppConfig

    p = Path(config_path)
    if p.exists():
        return AppConfig.from_yaml(p)
    return AppConfig()


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Workflow Controller — headless runner + Web UI."""
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.group("provenance")
def provenance() -> None:
    """Standalone file-provenance services."""


@provenance.command("serve")
@click.option(
    "--project",
    required=True,
    type=click.Path(path_type=Path, exists=True, file_okay=False),
)
@click.option("--ignore", multiple=True, help="Additional ignore pattern (repeatable)")
@click.option("--max-file-bytes", default=2 * 1024 * 1024, type=click.IntRange(min=1))
def cmd_provenance_serve(project: Path, ignore: tuple[str, ...], max_file_bytes: int) -> None:
    """Serve the versioned file-provenance JSONL protocol on stdio."""
    from workflow_controller.plugins.file_provenance.sidecar import run_sidecar

    run_sidecar(project, ignore=list(ignore), max_file_bytes=max_file_bytes)


@cli.command("codex-hook")
@click.argument(
    "event",
    type=click.Choice(["Stop", "SubagentStart", "SubagentStop"]),
)
@click.pass_context
def cmd_codex_hook(ctx: click.Context, event: str) -> None:
    """转发 Codex 原生 command hook（内部命令，不接受端口参数）。"""
    from workflow_controller.driver.hooks import run_codex_hook

    ctx.exit(run_codex_hook(event))


@provenance.command("export-audit-evidence")
@click.option(
    "--project",
    required=True,
    type=click.Path(path_type=Path, exists=True, file_okay=False),
)
@click.option("--session", "session_id", required=True, help="Root OpenCode session ID")
@click.option(
    "--opencode-db",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    default=None,
    help="OpenCode SQLite database (auto-detected by default)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path, dir_okay=False),
    default=None,
    help="Output JSON path (default: ./workflow-controller-audit-<session>.json)",
)
@click.option("--force", is_flag=True, help="Atomically replace an existing output file")
def cmd_provenance_export_audit_evidence(
    project: Path,
    session_id: str,
    opencode_db: Path | None,
    output: Path | None,
    force: bool,
) -> None:
    """Export OpenCode DB + file provenance as sift-records v2."""
    from workflow_controller.plugins.file_provenance.offline_export import (
        export_opencode_audit_evidence,
        write_audit_bundle,
    )
    from workflow_controller.transcript import find_opencode_db

    database = opencode_db or find_opencode_db()
    target = output or Path.cwd() / f"workflow-controller-audit-{session_id}.json"
    try:
        bundle = export_opencode_audit_evidence(project, session_id, database)
        written = write_audit_bundle(bundle, target, force=force)
    except (KeyError, OSError, RuntimeError, ValueError) as exc:
        message = exc.args[0] if isinstance(exc, KeyError) and exc.args else str(exc)
        raise click.ClickException(str(message)) from exc
    click.echo(str(written))


@cli.command("run")
@click.option(
    "--config",
    "-c",
    default="workflow-controller-config.yaml",
    help="Path to workflow-controller-config.yaml",
)
@click.option(
    "--settings-dir",
    default=None,
    help="Dir with settings.conf + auth_state.json (default: ~/.config/workflow-controller/)",
)
@click.option(
    "--hook-port",
    type=click.IntRange(min=1, max=65535),
    default=18765,
    help="Hook server port (default: 18765)",
)
@click.option("--headless", is_flag=True, help="(default) JSON lines to stdout (CI/scripts)")
@click.option("--no-reviewer", is_flag=True)
@click.option(
    "--log-level",
    default=None,
    help="Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)",
)
def cmd_run(
    config: str,
    settings_dir: str | None,
    hook_port: int,
    headless: bool,
    no_reviewer: bool,
    log_level: str | None,
) -> None:
    """Run a worker session headless (JSON lines to stdout). For the interactive UI use `workflow-controller web`."""
    from workflow_controller.logging_config import configure_logging

    app_cfg = _load_app_config(config)
    if no_reviewer:
        app_cfg.reviewer.enabled = False
    if log_level is not None:
        app_cfg.log_level = log_level

    log_path = configure_logging(Path(app_cfg.project_dir), level=app_cfg.log_level)

    import asyncio

    click.echo(json.dumps({"event": "info", "msg": f"Logging to {log_path}"}))
    from workflow_controller.headless import HeadlessRunner

    runner = HeadlessRunner(
        app_cfg,
        config,
        settings_dir=settings_dir,
        hook_port=hook_port,
    )
    try:
        asyncio.run(runner.run())
    finally:
        asyncio.run(runner.shutdown())


@cli.command("web")
@click.option("--port", default=18600, type=int, help="Web UI port")
@click.option("--host", default="127.0.0.1", help="Bind address")
@click.option(
    "--hook-port",
    type=click.IntRange(min=1, max=65535),
    default=None,
    help="Hook server port (default: 18765)",
)
@click.option("--no-open", is_flag=True, help="Don't open browser")
@click.option("--dev", is_flag=True, help="Dev mode: Vite HMR frontend + uvicorn reload")
@click.option(
    "--insight",
    is_flag=False,
    flag_value="default",
    default=None,
    help="Start Insight; optionally provide the command path",
)
@click.option("--insight-port", default=21025, type=int, help="Insight port (default: 21025)")
@click.option(
    "--settings-dir",
    default=None,
    help="Dir with settings.conf + auth_state.json (default: ~/.config/workflow-controller/)",
)
@click.option(
    "--log-level",
    default=None,
    help="Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)",
)
@click.pass_context
def cmd_web(
    ctx: click.Context,
    port: int,
    host: str,
    hook_port: int | None,
    no_open: bool,
    dev: bool,
    insight: str | None,
    insight_port: int,
    settings_dir: str | None,
    log_level: str | None,
) -> None:
    """Start Web UI — Vue 3 SPA + SSE real-time stream."""
    if hook_port is None:
        os.environ.pop("WORKFLOW_CONTROLLER_HOOK_PORT", None)
    else:
        os.environ["WORKFLOW_CONTROLLER_HOOK_PORT"] = str(hook_port)

    # Web/Insight 装配住完整发行包的 web 模块，函数内懒装配保持内核导入链干净
    try:
        from workflow_controller.web.cli import run_web_command
    except ImportError as exc:
        raise click.ClickException(
            "Web UI is not available in this kernel-only install: "
            "workflow_controller.web is missing. Install the full workflow-controller "
            "package, or use `workflow-controller run` for headless mode."
        ) from exc
    run_web_command(ctx, port, host, no_open, dev, insight, insight_port, settings_dir, log_level)


def main() -> int:
    # CLI help 头部品牌随全局配置的 brand_name——与 Web 品牌同源；
    # 配置缺失/损坏回落默认品牌，不阻塞 CLI。
    from workflow_controller.config.settings import DEFAULT_BRAND_NAME

    try:
        brand = _load_app_config("workflow-controller-config.yaml").brand_name.strip()
    except Exception:
        brand = ""
    cli.help = f"{brand or DEFAULT_BRAND_NAME} — headless runner + Web UI."
    cli()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
