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

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from workflow_controller.core.auth_rotator import AuthMode, AuthRotator
from workflow_controller.core.auth_rotator.persistence import KeyStatus
from workflow_controller.core.auth_rotator.signal import LimitMatch


@pytest.fixture
def rotator(tmp_path: Path) -> AuthRotator:
    env_file = tmp_path / ".env"
    env_file.write_text("ANTHROPIC_API_KEY=sk-ant-test\n", encoding="utf-8")
    return AuthRotator(
        state_path=tmp_path / "auth_state.json",
        env_path=env_file,
    )


def test_on_limit_detected_5h_stays_api(rotator: AuthRotator) -> None:
    reset_at = datetime.now(tz=UTC) + timedelta(hours=4)
    rotator.on_limit_detected(
        LimitMatch(scope="5h", source="with_reset_time", reset_hhmm="14:30"),
        reset_at=reset_at,
    )
    assert rotator.get_mode() == AuthMode.API
    assert rotator.get_reset_at() == reset_at


def test_on_limit_detected_weekly_switches_to_api_no_reset(
    rotator: AuthRotator,
) -> None:
    rotator.on_limit_detected(
        LimitMatch(scope="weekly", source="weekly_no_time"),
        reset_at=None,
    )
    # Weekly → API but no auto-back (reset_at=None means won't auto-revert)
    assert rotator.get_mode() == AuthMode.API
    assert rotator.get_reset_at() is None


def test_on_limit_detected_rate_limit_does_not_switch(rotator: AuthRotator) -> None:
    # rate_limit 属 API 侧限流：无备选可切，保持当前模式
    initial_mode = rotator.get_mode()
    rotator.on_limit_detected(
        LimitMatch(scope="rate_limit", source="api_rate_limit"),
        reset_at=None,
    )
    assert rotator.get_mode() == initial_mode


def test_legacy_forced_api_in_state_file_migrates_to_api(tmp_path: Path) -> None:
    """Persistence migration: FORCED_API → API on load (with reset_at=None,
    matching FORCED_API's no-auto-back semantics).
    """
    state_path = tmp_path / "auth_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        '{"mode": "FORCED_API", "reset_at": null}',
        encoding="utf-8",
    )
    env_file = tmp_path / ".env"
    env_file.write_text("ANTHROPIC_API_KEY=sk\n", encoding="utf-8")
    rotator = AuthRotator(state_path=state_path, env_path=env_file)
    assert rotator.get_mode() == AuthMode.API
    assert rotator.get_reset_at() is None


def test_auto_switch_does_not_invoke_loop_engine_hook(tmp_path: Path) -> None:
    """Detector-driven switches do NOT kill the worker — claude CLI is already
    dying. Only manual force_mode triggers mid-iter restart.
    """
    env_file = tmp_path / ".env"
    env_file.write_text("ANTHROPIC_API_KEY=sk\n", encoding="utf-8")
    rotator = AuthRotator(state_path=tmp_path / "state.json", env_path=env_file)
    called = []
    rotator.set_loop_engine_hook(lambda: called.append(True))
    rotator.on_limit_detected(
        LimitMatch(scope="5h", source="generic_5h"),
        reset_at=datetime.now(tz=UTC) + timedelta(hours=4),
    )
    assert called == []


def test_on_rate_limit_5h_switches_to_api_with_reset_at(tmp_path: Path) -> None:
    """5h 限流切 API 模式并记录恢复时间。"""
    env_file = tmp_path / ".env"
    env_file.write_text("ANTHROPIC_API_KEY=sk-test\n", encoding="utf-8")
    rotator = AuthRotator(state_path=tmp_path / "state.json", env_path=env_file)
    reset_dt = datetime.now(tz=UTC) + timedelta(hours=2)
    rotator.on_rate_limit(scope="5h", reset_at=reset_dt)
    assert rotator.get_mode() == AuthMode.API
    assert rotator.get_reset_at() == reset_dt


def test_on_rate_limit_weekly_switches_to_api_no_reset(tmp_path: Path) -> None:
    """weekly 限流切 API 模式，不记录恢复时间。"""
    env_file = tmp_path / ".env"
    env_file.write_text("ANTHROPIC_API_KEY=sk-test\n", encoding="utf-8")
    rotator = AuthRotator(state_path=tmp_path / "state.json", env_path=env_file)
    rotator.on_rate_limit(scope="weekly", reset_at=None)
    assert rotator.get_mode() == AuthMode.API
    assert rotator.get_reset_at() is None


@pytest.mark.asyncio
async def test_reset_task_fires_with_60s_buffer(tmp_path: Path) -> None:
    """Reset task delay = (reset_at - now) + 60s buffer. Schedule reset
    50ms in the future; with 60s buffer the task should NOT fire within 200ms.
    """
    env_file = tmp_path / ".env"
    env_file.write_text("ANTHROPIC_API_KEY=sk-test\n", encoding="utf-8")
    rotator = AuthRotator(state_path=tmp_path / "state.json", env_path=env_file)
    reset_dt = datetime.now(tz=UTC) + timedelta(milliseconds=50)
    rotator.on_rate_limit(scope="5h", reset_at=reset_dt)
    assert rotator.get_mode() == AuthMode.API
    # Without buffer, reset would fire by now (50ms passed). With 60s buffer,
    # we should still be in API mode after only 200ms.
    await asyncio.sleep(0.2)
    assert rotator.get_mode() == AuthMode.API, "Reset fired too early — 60s buffer not applied"


@pytest.mark.asyncio
async def test_bound_rate_limit_schedules_per_provider_reset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """session-bound 429 轮换也按 provider 调度恢复，而非仅启动时恢复。"""
    (tmp_path / "settings.conf").write_text("api_keys:\n  - sk\n", encoding="utf-8")
    (tmp_path / ".env").write_text("", encoding="utf-8")
    rotator = AuthRotator(
        state_path=tmp_path / "auth_state.json",
        env_path=tmp_path / ".env",
        runtime="codex_cli",
    )
    scheduled: list[tuple[int, float]] = []

    async def capture_reset(provider_index: int, delay_seconds: float) -> None:
        scheduled.append((provider_index, delay_seconds))

    monkeypatch.setattr(rotator, "_sleep_and_reset", capture_reset)
    reset_at = datetime.now(tz=UTC) + timedelta(hours=1)

    replacement = rotator.rotate_bound_provider(
        0,
        scope="rate_limit",
        reset_at=reset_at,
        runtime="codex_cli",
    )
    await asyncio.sleep(0)

    assert replacement is None
    assert rotator.get_provider(0).status is KeyStatus.EXHAUSTED
    assert rotator.get_provider(0).reset_at == reset_at
    assert scheduled == [(0, pytest.approx(3600 + 60))]
    assert set(rotator._reset_tasks) == {0}


@pytest.mark.asyncio
async def test_provider_reset_restores_exhausted_state(tmp_path: Path) -> None:
    """reset task 到期后恢复该 provider 的全局配额状态。"""
    (tmp_path / "settings.conf").write_text("api_keys:\n  - sk\n", encoding="utf-8")
    (tmp_path / ".env").write_text("", encoding="utf-8")
    rotator = AuthRotator(
        state_path=tmp_path / "auth_state.json",
        env_path=tmp_path / ".env",
        runtime="codex_cli",
    )
    provider = rotator.get_provider(0)
    provider.status = KeyStatus.EXHAUSTED
    provider.reset_at = datetime.now(tz=UTC)
    provider.scope = "rate_limit"

    await rotator._sleep_and_reset(0, 0)

    assert provider.status is KeyStatus.ACTIVE
    assert provider.exhausted_at is None
    assert provider.reset_at is None
    assert provider.scope is None


def test_on_quota_warning_invokes_registered_callback(tmp_path: Path) -> None:
    """set_quota_warning_callback + on_quota_warning → callback fires with
    utilization + scope + reset_at.
    """
    rotator = AuthRotator(
        state_path=tmp_path / "state.json",
        env_path=tmp_path / ".env",
    )
    received: list[tuple] = []

    def cb(*, utilization: float, scope: str, reset_at: datetime | None) -> None:
        received.append((utilization, scope, reset_at))

    rotator.set_quota_warning_callback(cb)
    reset_dt = datetime(2026, 5, 26, 18, 0)
    rotator.on_quota_warning(utilization=0.85, scope="seven_day", reset_at=reset_dt)
    assert received == [(0.85, "seven_day", reset_dt)]


def test_on_quota_warning_does_not_change_mode(tmp_path: Path) -> None:
    """Warning is informational; doesn't switch mode."""
    rotator = AuthRotator(
        state_path=tmp_path / "state.json",
        env_path=tmp_path / ".env",
    )
    initial_mode = rotator.get_mode()
    rotator.on_quota_warning(utilization=0.85, scope="five_hour", reset_at=None)
    assert rotator.get_mode() == initial_mode


def test_on_quota_warning_no_callback_is_noop(tmp_path: Path) -> None:
    """Without a registered callback, on_quota_warning is a no-op (no error)."""
    rotator = AuthRotator(
        state_path=tmp_path / "state.json",
        env_path=tmp_path / ".env",
    )
    # Should not raise
    rotator.on_quota_warning(utilization=0.9, scope="five_hour", reset_at=None)


def test_default_config_dir_returns_path() -> None:
    from workflow_controller.core.auth_rotator import default_config_dir

    p = default_config_dir()
    assert isinstance(p, Path)
    assert "workflow-controller" in str(p).lower()


def test_build_default_rotator_creates_rotator(tmp_path: Path, monkeypatch) -> None:
    from workflow_controller.core.auth_rotator import build_default_rotator

    def _fake_dir() -> Path:
        return tmp_path

    monkeypatch.setattr("workflow_controller.core.auth_rotator.default_config_dir", _fake_dir)
    (tmp_path / ".env").write_text("ANTHROPIC_API_KEY=sk-test\n")
    assert build_default_rotator().get_mode() is not None
