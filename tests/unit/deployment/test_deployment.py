"""Unit tests for `almanak.framework.deployment` — VIB-4722.

`ALMANAK_IS_HOSTED` is the single deployment-mode signal; `ALMANAK_DEPLOYMENT_ID`
carries the id value within hosted mode. `mode.py` is the sole reader of both.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from almanak.framework.deployment import (
    FatalBootError,
    deployment_commit_sha,
    deployment_id,
    deployment_mode,
    deployment_sdk_version,
    deployment_strategy_name,
    deployment_strategy_version,
    is_hosted,
    is_local,
)


def _clear(monkeypatch):
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)


# --- is_hosted / is_local on the ALMANAK_IS_HOSTED signal -------------------


def test_unset_is_local(monkeypatch):
    """ALMANAK_IS_HOSTED unset → is_hosted() False."""
    _clear(monkeypatch)
    assert is_hosted() is False
    assert is_local() is True


@pytest.mark.parametrize("truthy", ["1", "true", "TRUE", "yes", "on", "  true  "])
def test_truthy_values_are_hosted(monkeypatch, truthy):
    """Truthy ALMANAK_IS_HOSTED → is_hosted() True."""
    _clear(monkeypatch)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", truthy)
    assert is_hosted() is True
    assert is_local() is False


@pytest.mark.parametrize("falsey", ["", "  ", "0", "false", "no", "off", "garbage"])
def test_falsey_values_are_local(monkeypatch, falsey):
    """Empty / whitespace / 0 / false / unknown ALMANAK_IS_HOSTED → local."""
    _clear(monkeypatch)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", falsey)
    assert is_hosted() is False
    assert is_local() is True


# --- deployment_mode() string token -----------------------------------------


def test_deployment_mode_hosted(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-3")
    assert deployment_mode() == "hosted"


def test_deployment_mode_local(monkeypatch):
    _clear(monkeypatch)
    assert deployment_mode() == "local"


# --- deployment_id() value resolution ---------------------------------------


def test_deployment_id_hosted_returns_value(monkeypatch):
    """Hosted: deployment_id() returns the stripped ALMANAK_DEPLOYMENT_ID."""
    _clear(monkeypatch)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "  agent-007  ")
    assert deployment_id() == "agent-007"


def test_deployment_id_hosted_blank_raises_fatal(monkeypatch):
    """Hosted + blank ALMANAK_DEPLOYMENT_ID ⇒ FatalBootError."""
    _clear(monkeypatch)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "   ")
    with pytest.raises(FatalBootError):
        deployment_id()


def test_deployment_id_hosted_unset_raises_fatal(monkeypatch):
    """Hosted + unset ALMANAK_DEPLOYMENT_ID ⇒ FatalBootError."""
    _clear(monkeypatch)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    with pytest.raises(FatalBootError):
        deployment_id()


def test_deployment_id_local_returns_none(monkeypatch):
    """Local: deployment_id() returns None (id derived from wallet+chain)."""
    _clear(monkeypatch)
    assert deployment_id() is None


def test_deployment_id_local_ignores_stray_id(monkeypatch):
    """Local: a stray ALMANAK_DEPLOYMENT_ID is ignored (returns None)."""
    _clear(monkeypatch)
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "platform-uuid-1234")
    assert deployment_id() is None


# --- StrategyRunner delegates to is_hosted() --------------------------------


def test_strategy_runner_delegates(monkeypatch):
    """StrategyRunner._is_managed_deployment delegates to is_hosted()."""
    from unittest.mock import MagicMock, patch

    _clear(monkeypatch)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-test-9")
    runner = MagicMock()
    from almanak.framework.runner.strategy_runner import StrategyRunner

    assert StrategyRunner._is_managed_deployment(runner) is True

    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    assert StrategyRunner._is_managed_deployment(runner) is False

    with patch("almanak.framework.deployment.is_hosted", return_value=True) as mock_hosted:
        StrategyRunner._is_managed_deployment(runner)
        mock_hosted.assert_called_once()


# --- codebase-wide callsite coverage guard ----------------------------------


def _repo_root() -> Path:
    """Resolve the repo root from this test file."""
    return Path(__file__).resolve().parents[3]


def test_no_direct_mode_env_reads_outside_helper():
    """Zero direct reads of the mode env vars outside `mode.py`.

    `ALMANAK_IS_HOSTED` and `ALMANAK_DEPLOYMENT_ID` are read only by
    `almanak/framework/deployment/mode.py`. Every other code path consumes
    `is_hosted()` / `is_local()` / `deployment_id()` / `deployment_mode()`.
    """
    root = _repo_root()
    pattern = re.compile(
        r'os\.environ(?:\.get)?\(\s*[\'"](?:ALMANAK_IS_HOSTED|ALMANAK_DEPLOYMENT_ID)[\'"]'
    )

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
        "Found direct reads of ALMANAK_IS_HOSTED / ALMANAK_DEPLOYMENT_ID in "
        "production code outside `almanak/framework/deployment/mode.py`. Use "
        "`framework.deployment.is_hosted()` / `deployment_id()` instead.\n"
        "Offenders:\n  - " + "\n  - ".join(offenders)
    )


def test_helper_module_reads_each_env_var_once():
    """`mode.py` reads each mode env var through the single `_raw` helper.

    `_raw` is the one `os.environ.get(...)` call; the var names appear as
    string literals passed to it.
    """
    helper_path = _repo_root() / "almanak/framework/deployment/mode.py"
    content = helper_path.read_text(encoding="utf-8")
    # Exactly one os.environ.get call (inside _raw).
    env_gets = re.findall(r"os\.environ\.get\(", content)
    assert len(env_gets) == 1, f"Expected one os.environ.get in mode.py, found {len(env_gets)}"
    assert '"ALMANAK_IS_HOSTED"' in content
    assert '"ALMANAK_DEPLOYMENT_ID"' in content


# --- Banner identity helpers (deployment_commit_sha / sdk_version / etc.) ---


@pytest.mark.parametrize(
    "helper,env_var",
    [
        (deployment_commit_sha, "ALMANAK_COMMIT_SHA"),
        (deployment_sdk_version, "ALMANAK_SDK_VERSION"),
        (deployment_strategy_name, "ALMANAK_STRATEGY_NAME"),
        (deployment_strategy_version, "ALMANAK_STRATEGY_VERSION"),
    ],
)
def test_banner_identity_helpers(monkeypatch, helper, env_var):
    """Each helper: unset → None; whitespace-only → None; set → stripped value."""
    monkeypatch.delenv(env_var, raising=False)
    assert helper() is None, f"{helper.__name__} should return None when {env_var} is unset"

    monkeypatch.setenv(env_var, "   ")
    assert helper() is None, f"{helper.__name__} should return None for whitespace-only {env_var}"

    monkeypatch.setenv(env_var, "  abc123  ")
    assert helper() == "abc123", f"{helper.__name__} should strip surrounding whitespace"

    monkeypatch.setenv(env_var, "v1.0.0")
    assert helper() == "v1.0.0"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
