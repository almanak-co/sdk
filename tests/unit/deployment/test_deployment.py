"""Unit tests for `almanak.framework.deployment` — VIB-3759.

Test IDs map to the test plan in `docs/internal/AccountingApril30-Tests.md`.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from almanak.framework.deployment import (
    agent_id,
    deployment_mode,
    is_hosted,
    is_local,
)


# --- T-3759-1 / T-3759-2: boolean behavior on unset / set --------------------


def test_t_3759_1_unset_is_local(monkeypatch):
    """T-3759-1: AGENT_ID unset → is_hosted() False."""
    monkeypatch.delenv("AGENT_ID", raising=False)
    assert is_hosted() is False


def test_t_3759_2_set_is_hosted(monkeypatch):
    """T-3759-2: AGENT_ID set → is_hosted() True."""
    monkeypatch.setenv("AGENT_ID", "agent-001")
    assert is_hosted() is True


# --- T-3759-3 / T-3759-4: empty / whitespace are not hosted ------------------


def test_t_3759_3_empty_string_is_local(monkeypatch):
    """T-3759-3: empty string AGENT_ID → is_hosted() False.

    Catches the `"AGENT_ID" in os.environ` shortcut, which would return True.
    """
    monkeypatch.setenv("AGENT_ID", "")
    assert is_hosted() is False


def test_t_3759_4_whitespace_only_is_local(monkeypatch):
    """T-3759-4: whitespace-only AGENT_ID → is_hosted() False.

    Forces `.strip()` semantics — matches legacy `_is_managed_deployment()`.
    """
    monkeypatch.setenv("AGENT_ID", "   ")
    assert is_hosted() is False


# --- T-3759-5 / T-3759-6: is_local() complements is_hosted() ----------------


def test_t_3759_5_is_local_false_when_hosted(monkeypatch):
    """T-3759-5: hosted → is_local() False."""
    monkeypatch.setenv("AGENT_ID", "agent-002")
    assert is_local() is False


def test_t_3759_6_is_local_true_when_local(monkeypatch):
    """T-3759-6: local → is_local() True."""
    monkeypatch.delenv("AGENT_ID", raising=False)
    assert is_local() is True


# --- T-3759-7 / T-3759-8: deployment_mode() string token ---------------------


def test_t_3759_7_deployment_mode_hosted(monkeypatch):
    """T-3759-7: hosted → deployment_mode() == 'hosted'."""
    monkeypatch.setenv("AGENT_ID", "agent-003")
    assert deployment_mode() == "hosted"


def test_t_3759_8_deployment_mode_local(monkeypatch):
    """T-3759-8: local → deployment_mode() == 'local'."""
    monkeypatch.delenv("AGENT_ID", raising=False)
    assert deployment_mode() == "local"


# --- agent_id() value-resolution tests ---------------------------------------


def test_agent_id_returns_stripped_value(monkeypatch):
    """agent_id() returns the env value, stripped, when set."""
    monkeypatch.setenv("AGENT_ID", "  agent-007  ")
    assert agent_id() == "agent-007"


def test_agent_id_returns_none_when_unset(monkeypatch):
    """agent_id() returns None when env is unset."""
    monkeypatch.delenv("AGENT_ID", raising=False)
    assert agent_id() is None


def test_agent_id_returns_none_when_empty(monkeypatch):
    """agent_id() returns None for empty/whitespace values."""
    monkeypatch.setenv("AGENT_ID", "  ")
    assert agent_id() is None


# --- T-3759-9: existing _is_managed_deployment delegates --------------------


def test_t_3759_9_strategy_runner_delegates(monkeypatch):
    """T-3759-9: StrategyRunner._is_managed_deployment delegates to is_hosted().

    Verifies the runner-side helper is wired through the central function,
    not duplicating its own AGENT_ID check.
    """
    from unittest.mock import MagicMock, patch

    monkeypatch.setenv("AGENT_ID", "agent-test-9")
    runner = MagicMock()
    from almanak.framework.runner.strategy_runner import StrategyRunner

    # Call the method directly on the class without going through __init__
    result = StrategyRunner._is_managed_deployment(runner)
    assert result is True

    monkeypatch.delenv("AGENT_ID", raising=False)
    result = StrategyRunner._is_managed_deployment(runner)
    assert result is False

    # And confirm: the helper imports `is_hosted` (not a copy of the env-read).
    with patch("almanak.framework.deployment.is_hosted", return_value=True) as mock_hosted:
        StrategyRunner._is_managed_deployment(runner)
        mock_hosted.assert_called_once()


# --- T-3759-10: codebase-wide callsite coverage guard -----------------------


def _repo_root() -> Path:
    """Resolve the repo root from this test file."""
    return Path(__file__).resolve().parents[3]


def test_t_3759_10_no_direct_agent_id_reads_outside_helper():
    """T-3759-10: zero direct `os.environ.get("AGENT_ID")` outside the helper.

    Anti-gaming guard: the whole point of §A is centralization. Without this
    test, future PRs can re-scatter the check. The only file permitted to
    read AGENT_ID directly is `almanak/framework/deployment.py`.

    Test fixtures, conftest, and tests themselves are excluded — they may
    legitimately need to set/inspect the env var.
    """
    root = _repo_root()
    pattern = re.compile(r'os\.environ\.get\(\s*[\'"]AGENT_ID[\'"]')

    # Use git ls-files to enumerate tracked Python files under production paths.
    completed = subprocess.run(
        ["git", "ls-files", "--", "*.py"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    candidate_paths = [Path(line) for line in completed.stdout.splitlines() if line.strip()]

    PRODUCTION_PREFIXES = ("almanak/", "platform-plugins/", "strategies/")
    EXEMPT_FILE = "almanak/framework/deployment/mode.py"

    offenders: list[str] = []
    for rel in candidate_paths:
        rel_str = str(rel)
        if not rel_str.startswith(PRODUCTION_PREFIXES):
            continue
        if rel_str == EXEMPT_FILE:
            continue
        try:
            content = (root / rel).read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if pattern.search(content):
            offenders.append(rel_str)

    assert not offenders, (
        "Found direct `os.environ.get(\"AGENT_ID\")` in production code outside "
        "`almanak/framework/deployment/mode.py`. Use `framework.deployment.is_hosted()` "
        "or `agent_id()` instead.\nOffenders:\n  - "
        + "\n  - ".join(offenders)
    )


# --- Sanity: the helper itself is allowed to read the env var --------------


def test_helper_module_is_the_one_that_reads_env_directly():
    """The helper module IS allowed to read the env var. Sanity-check the file
    contains exactly one such call so we don't accidentally regress to multiple
    reads even within the helper.
    """
    helper_path = _repo_root() / "almanak/framework/deployment/mode.py"
    content = helper_path.read_text(encoding="utf-8")
    matches = re.findall(r'os\.environ\.get\(\s*[\'"]AGENT_ID[\'"]', content)
    assert len(matches) == 1, (
        f"Expected exactly one direct AGENT_ID read in mode.py, found {len(matches)}"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
