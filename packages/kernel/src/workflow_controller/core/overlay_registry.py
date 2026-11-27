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

"""Per-session + per-project prompt overlay registry.

Extracted from Orchestrator (阶段 A: driver overlay 外溢剥离). Holds the
opencode-only overlay bookkeeping the orchestrator used to own — subagent
system-prompt hints (per-type prepend) + global instructions (appended to ALL
agents via opencode's ``instructions`` config). Only the opencode driver
consumes them via ``DriverHooks.get_overlays()`` →
``{"subagent_hints", "instructions"}``; SDK/CLI get an empty snapshot.

Two scopes: **session**-scoped (keyed by Workflow Controller session id, live + lazily read
at query() time so an iteration-boundary registration lands next round) and
**project**-scoped (keyed by normalized project dir, registered before any
session exists; ``transfer_project_overlays`` copies it into a new session at
creation). Transfers deep-copy so the source bucket stays intact; empty buckets
are pruned.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


class OverlayRegistry:
    """Per-session + per-project prompt overlay registry (opencode-consumed)."""

    def __init__(self) -> None:
        # session_id -> {subagent_type: [hint, ...]} (per-type prepend; opencode-only)
        self._session_hints: dict[str, dict[str, list[str]]] = {}
        # project_dir (normalized) -> {subagent_type: [hint, ...]} (pre-session;
        # copied into _session_hints on transfer_project_overlays)
        self._project_hints: dict[str, dict[str, list[str]]] = {}
        # session_id -> [instruction, ...] (global; appended to ALL agents via
        # opencode `instructions` config)
        self._session_instructions: dict[str, list[str]] = {}
        # project_dir (normalized) -> [instruction, ...] (pre-session; copied
        # into _session_instructions on transfer_project_overlays)
        self._project_instructions: dict[str, list[str]] = {}

    # ── session-scoped subagent hints ───────────────────────────────────

    @staticmethod
    def _norm_project_dir(project_dir: str) -> str:
        """Normalize project_dir to a stable registry key."""
        try:
            return str(Path(project_dir).expanduser().resolve())
        except Exception:
            return str(project_dir)

    @staticmethod
    def _remove_hint_entries(
        store: dict[str, dict[str, list[str]]],
        key: str,
        subagent_type: str | None,
        hint_index: int | None,
    ) -> int:
        """按 type/index 档位移除提示并修剪空桶（session/project 两轨共用）。"""
        if subagent_type is None:
            removed = store.pop(key, {})
            return sum(len(v) for v in removed.values())
        bucket = store.get(key, {})
        hints_list = bucket.get(subagent_type)
        if not hints_list:
            return 0
        if hint_index is None:
            count = len(hints_list)
            del bucket[subagent_type]
        else:
            if hint_index < 0 or hint_index >= len(hints_list):
                return 0
            hints_list.pop(hint_index)
            count = 1
            if not hints_list:
                del bucket[subagent_type]
        if not bucket:
            store.pop(key, None)
        return count

    def register_subagent_hint(
        self,
        session_id: str,
        subagent_type: str,
        hint: str,
    ) -> None:
        """Append a hint for ``subagent_type`` (multiple accumulate, not overwrite)."""
        self._session_hints.setdefault(
            session_id,
            {},
        ).setdefault(subagent_type, []).append(hint)

    def clear_subagent_hint(
        self,
        session_id: str,
        subagent_type: str | None = None,
        hint_index: int | None = None,
    ) -> int:
        """Clear hints: all for the session (``type=None``) / all for a type
        (``index=None``) / one by 0-based ``index``. Returns count cleared;
        prunes empty buckets.
        """
        return self._remove_hint_entries(self._session_hints, session_id, subagent_type, hint_index)

    # ── session-scoped global instructions ─────────────────────────────
    # opencode's `instructions` config appends file content to ALL agents'
    # system prompts (worker + every subagent). Complementary to per-type hints.

    def get_subagent_hints(self, session_id: str) -> dict[str, list[str]]:
        """Copy of the hints for a session (empty if none)."""
        return {
            stype: list(hints) for stype, hints in self._session_hints.get(session_id, {}).items()
        }

    def register_session_instruction(
        self,
        session_id: str,
        instruction: str,
    ) -> None:
        """Append a global instruction (multiple accumulate)."""
        self._session_instructions.setdefault(
            session_id,
            [],
        ).append(instruction)

    def clear_session_instruction(
        self,
        session_id: str,
        instruction_index: int | None = None,
    ) -> int:
        """Clear instructions: all (``index=None``) / one by 0-based ``index``.
        Returns count cleared; prunes empty lists.
        """
        instructions = self._session_instructions.get(session_id)
        if not instructions:
            return 0
        if instruction_index is None:
            count = len(instructions)
            self._session_instructions.pop(session_id, None)
            return count
        if instruction_index < 0 or instruction_index >= len(instructions):
            return 0
        instructions.pop(instruction_index)
        if not instructions:
            self._session_instructions.pop(session_id, None)
        return 1

    # ── project-scoped (pre-session, Dashboard) ───────────────────────
    # Same semantics as the session-scoped registry above, but keyed by
    # project dir so overlays can be registered before any session exists.

    def get_session_instructions(self, session_id: str) -> list[str]:
        """Copy of the instructions for a session (empty if none)."""
        return list(self._session_instructions.get(session_id, []))

    def register_project_hint(
        self,
        project_dir: str,
        subagent_type: str,
        hint: str,
    ) -> None:
        """Append a hint for ``subagent_type`` on a project dir."""
        key = self._norm_project_dir(project_dir)
        self._project_hints.setdefault(
            key,
            {},
        ).setdefault(subagent_type, []).append(hint)

    def clear_project_hint(
        self,
        project_dir: str,
        subagent_type: str | None = None,
        hint_index: int | None = None,
    ) -> int:
        """Clear hints on a project dir: all / a type / one by index. See
        :meth:`clear_subagent_hint` for the mode contract. Returns count cleared.
        """
        key = self._norm_project_dir(project_dir)
        return self._remove_hint_entries(self._project_hints, key, subagent_type, hint_index)

    def get_project_hints(self, project_dir: str) -> dict[str, list[str]]:
        """Copy of the hints for a project dir (empty if none)."""
        hints_by_type = {}
        for stype, hints in self._project_hints.get(
            self._norm_project_dir(project_dir),
            {},
        ).items():
            hints_by_type[stype] = list(hints)
        return hints_by_type

    def register_project_instruction(
        self,
        project_dir: str,
        instruction: str,
    ) -> None:
        """Append a global instruction on a project dir."""
        key = self._norm_project_dir(project_dir)
        self._project_instructions.setdefault(
            key,
            [],
        ).append(instruction)

    def clear_project_instruction(
        self,
        project_dir: str,
        instruction_index: int | None = None,
    ) -> int:
        """Clear instructions on a project dir: all / one by index. Returns count."""
        key = self._norm_project_dir(project_dir)
        instructions = self._project_instructions.get(key)
        if not instructions:
            return 0
        if instruction_index is None:
            count = len(instructions)
            self._project_instructions.pop(key, None)
            return count
        if instruction_index < 0 or instruction_index >= len(instructions):
            return 0
        instructions.pop(instruction_index)
        if not instructions:
            self._project_instructions.pop(key, None)
        return 1

    def get_project_instructions(self, project_dir: str) -> list[str]:
        """Copy of the instructions for a project dir."""
        return list(
            self._project_instructions.get(
                self._norm_project_dir(project_dir),
                [],
            )
        )

    # ── transfers (session-creation wiring) ───────────────────────────

    def transfer_project_overlays(self, session_id: str, project_dir: str) -> None:
        """Copy a project's hints + instructions into a new session's buckets
        (deep-copied so the project bucket stays reusable; no-op if none).
        """
        key = self._norm_project_dir(project_dir)

        hint_bucket = self._project_hints.get(key)
        if hint_bucket:
            target = self._session_hints.setdefault(session_id, {})
            for stype, hints in hint_bucket.items():
                target.setdefault(stype, []).extend(hints)

        instructions = self._project_instructions.get(key)
        if instructions:
            self._session_instructions.setdefault(session_id, []).extend(instructions)

    # ── driver query seam ──────────────────────────────────────────────

    def get_overlays(self, session_id: str) -> dict[str, Any]:
        """Snapshot ``{"subagent_hints": {type: [...]}, "instructions": [...]}``
        a driver bakes into config (copies; empty if none — SDK/CLI ignore it).
        """
        return {
            "subagent_hints": self.get_subagent_hints(session_id),
            "instructions": self.get_session_instructions(session_id),
        }
