"""Shared fixtures for `tests/unit/cli/`.

Covers two pieces of process-global hygiene:

* ContextVar reset around the test-only signing-key plumb introduced in
  #2100. The `_runtime_private_key_override` ContextVar is process-global
  by design (it carries the strat-test fallback key from the CLI through
  to the framework), so tests that exercise `_setup_gateway` or
  `_build_runtime_config` must start from a clean default. Without this
  fixture, a value set by an earlier test would silently satisfy the
  kwarg-fallback branch in a later test and produce confusing failures
  (e.g. sidecar dispatch tests not raising the expected ClickException
  because the contextvar happened to carry a valid key).

* Deterministic demo-strategy registration. Many CLI tests invoke
  backtest/paper/sweep commands with packaged demo names
  (``demo_uniswap_lp``, ``demo_aerodrome_paper_trade``, ...), but
  ``almanak/demo_strategies/`` is NOT auto-discovered the way
  ``./strategies/`` is (see ``framework/strategies/_auto_discover_strategies``).
  Registration only happens when a demo's ``strategy.py`` module is
  imported and its ``@almanak_strategy`` decorator runs. Before this
  fixture, those tests passed in full xdist runs only when an earlier
  test on the same worker happened to import a demo module - and failed
  with "Unknown strategy" when run in isolation or under an unlucky
  collection order.
"""

from __future__ import annotations

import importlib
import warnings
from collections.abc import Iterator

import pytest


@pytest.fixture(scope="session", autouse=True)
def _register_demo_strategies() -> None:
    """Import every packaged demo's ``strategy.py`` into STRATEGY_REGISTRY.

    Uses the same canonical module path as explicit test imports
    (``almanak.demo_strategies.<name>.strategy``), so ``sys.modules`` is
    shared with them and the ``@almanak_strategy`` decorator's
    already-registered branch keeps re-imports idempotent. All demos are
    imported (not just the ones referenced today) so a new CLI test using
    any demo name cannot reintroduce the order dependence; the marginal
    cost is ~1-2s once per pytest/xdist worker process.

    Lenient per-module: a demo that fails to import is reported as a
    warning rather than failing the whole suite; only the tests that
    reference that demo will then fail, with the warning pointing at the
    real cause.
    """
    from almanak.demo_strategies import DEMO_STRATEGY_NAMES

    for name in DEMO_STRATEGY_NAMES:
        try:
            importlib.import_module(f"almanak.demo_strategies.{name}.strategy")
        except Exception as exc:  # noqa: BLE001 - importing arbitrary demo code
            warnings.warn(
                f"Failed to import demo strategy '{name}' for registry setup: {exc!r}",
                RuntimeWarning,
                stacklevel=1,
            )


@pytest.fixture(autouse=True)
def _reset_runtime_private_key_override() -> Iterator[None]:
    """Reset the test-only signing-key ContextVar before and after each test."""
    from almanak.framework.cli import run_helpers

    token = run_helpers._runtime_private_key_override.set(None)
    try:
        yield
    finally:
        run_helpers._runtime_private_key_override.reset(token)
