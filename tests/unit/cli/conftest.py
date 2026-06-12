"""Shared fixtures for `tests/unit/cli/`.

Currently scoped to ContextVar hygiene around the test-only signing-key
plumb introduced in #2100. The `_runtime_private_key_override` ContextVar
is process-global by design (it carries the strat-test fallback key from
the CLI through to the framework), so tests that exercise `_setup_gateway`
or `_build_runtime_config` must start from a clean default. Without this
fixture, a value set by an earlier test would silently satisfy the kwarg-
fallback branch in a later test and produce confusing failures (e.g.
sidecar dispatch tests not raising the expected ClickException because
the contextvar happened to carry a valid key).
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def _reset_runtime_private_key_override() -> Iterator[None]:
    """Reset the test-only signing-key ContextVar before and after each test."""
    from almanak.framework.cli import run_helpers

    token = run_helpers._runtime_private_key_override.set(None)
    try:
        yield
    finally:
        run_helpers._runtime_private_key_override.reset(token)


@pytest.fixture(autouse=True)
def _enable_backtesting_feature_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """Opt the CLI test suite in to the gated backtesting feature.

    Backtesting is disabled by default behind ``ALMANAK_ENABLE_BACKTESTING``
    until VIB-5079 lands; the existing CLI tests exercise the commands behind
    the gate. The gate itself is covered by
    ``test_backtest_feature_flag.py``, which removes the variable again.
    """
    monkeypatch.setenv("ALMANAK_ENABLE_BACKTESTING", "1")
