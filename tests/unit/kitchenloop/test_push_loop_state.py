"""Tests for push-loop-state.sh script argument handling and safety checks.

VIB-1803
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parent.parent.parent.parent / "scripts" / "kitchenloop" / "push-loop-state.sh"


class TestPushLoopStateScript:
    """Validate the push-loop-state.sh script exists and has correct structure."""

    def test_script_exists(self):
        assert SCRIPT_PATH.exists(), f"Script not found at {SCRIPT_PATH}"

    def test_script_is_executable(self):
        assert os.access(SCRIPT_PATH, os.X_OK), f"Script is not executable: {SCRIPT_PATH}"

    def test_script_has_set_euo_pipefail(self):
        """Safety: script must use strict mode."""
        content = SCRIPT_PATH.read_text()
        assert "set -euo pipefail" in content

    def test_script_does_not_force_push(self):
        """Safety: script must NEVER force push (append-only audit trail)."""
        content = SCRIPT_PATH.read_text()
        # Check that no git push line uses --force
        for line in content.splitlines():
            if "git push" in line:
                assert "--force" not in line, f"Force push found: {line}"

    def test_script_only_touches_loop_state(self):
        """Safety: only docs/internal/loop-state.md should flow through this path."""
        content = SCRIPT_PATH.read_text()
        assert 'LOOP_STATE_FILE="docs/internal/loop-state.md"' in content

    def test_script_uses_dedicated_branch(self):
        """Script must push to loop-state branch, not main."""
        content = SCRIPT_PATH.read_text()
        assert 'TRACKING_BRANCH="loop-state"' in content

    def test_script_exits_outside_git_repo(self):
        """Running outside a git repo should fail gracefully."""
        result = subprocess.run(
            ["bash", str(SCRIPT_PATH)],
            capture_output=True,
            text=True,
            cwd="/tmp",
            timeout=10,
        )
        assert result.returncode != 0
        assert "ERROR" in result.stderr or "not inside a git" in result.stderr
