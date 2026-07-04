"""Regression guard: importing the ``almanak`` CLI bootstrap must not pull
in heavy modules that the deployed strategy container has no need for at
startup.

The deployed V2 strategy image runs ``almanak strat run --no-gateway
--interval N`` as its entry point. That command goes through
``almanak/cli/cli.py``, which eagerly imports every framework CLI subgroup
(``backtest``, ``check``, ``demo``, ``ax``, ``run``, ``status``, …) so it
can register them on the top-level click group. If any of those subgroups
transitively pulls in ``almanak.framework.backtesting``, optuna,
sqlalchemy, plotly, matplotlib, streamlit, or other backtest-only / UI-only
deps, the strategy container pays the import cost on every startup and any
future ``Dockerfile.strategy`` strip of those packages crashes the pod
with ``ModuleNotFoundError``.

This test is the contract that lets ``Dockerfile.strategy`` strip those
packages safely. Companion tests:

- ``tests/framework/runner/test_imports_lean.py`` — same forbidden list
  for ``almanak.framework.runner.strategy_runner`` (the in-process entry
  point used by the runner harness, not the deployed container).
- ``tests/framework/dashboard/test_imports_lean.py`` — same pattern for
  the Streamlit dashboard.
- ``tests/gateway/test_imports_lean.py`` — same pattern for the gateway
  sidecar.

If a future PR adds a module-level import of one of the forbidden
packages anywhere on the CLI bootstrap path, this test fails in CI before
the strip can break a deploy. Either:
  - move the import to function scope (preferred for click commands —
    decorate the command, import inside the callback);
  - extend the lazy dispatch map in the affected ``__init__.py`` (the
    PEP 562 ``__getattr__`` pattern used by ``framework/__init__.py``,
    ``framework/cli/__init__.py``, and ``framework/cli/backtest/__init__.py``);
  - if the dep is genuinely needed at every CLI startup, also remove it
    from ``platform/packages/backend/src/templates/Dockerfile.strategy``
    so the package stays in the deployed image.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap

# UI / backtest / analytical packages — none of these belong in the
# strategy runtime, which talks to the gateway over gRPC and has no UI
# surface. Backtesting is offline-only (parameter sweeps, paper trading)
# and runs on the user's laptop, never inside a deployed strategy pod.
#
# ``pandas`` and ``pyarrow`` are paired here intentionally: pandas
# auto-loads pyarrow on import via ``pandas.compat.pyarrow``, so guarding
# only one would let a future eager ``import pandas as pd`` slip in
# without tripping the test.
_FORBIDDEN_THIRD_PARTY = (
    "pandas",
    "streamlit",
    "plotly",
    "matplotlib",
    "altair",
    "pyarrow",
    "optuna",
    "tqdm",
    "sqlalchemy",
    "alembic",
    "simple_term_menu",
)

# Framework subpackages the CLI bootstrap has no business loading at
# module level. ``backtesting`` is offline-only; ``dashboard`` is the
# Streamlit UI surface; ``deployment`` and ``testing`` are scaffolding
# code that the strategy runtime never touches.
_FORBIDDEN_FRAMEWORK_SUBPACKAGES = (
    "almanak.framework.backtesting",
    "almanak.framework.dashboard",
    "almanak.framework.deployment",
    "almanak.framework.testing",
)

# Gateway modules the strategy container image deletes outright
# (deploy/docker/Dockerfile.strategy removes server.py, services/, api/,
# and middleware/ — a strategy container must not be able to run its own
# gateway). If the CLI bootstrap eagerly reaches any of these, the
# ``almanak`` CLI dies with ModuleNotFoundError inside that container.
# Gateway *client* surfaces (proto stubs, data providers, settings) stay
# in the image and are fine to load.
_FORBIDDEN_GATEWAY_STRIPPED_SURFACE = (
    "almanak.gateway.server",
    "almanak.gateway.services",
    "almanak.gateway.api",
    "almanak.gateway.middleware",
)


def _import_cli_in_subprocess() -> set[str]:
    """Import the top-level ``almanak`` click group in a fresh subprocess
    and return ``sys.modules`` keys.

    A subprocess is required because pytest itself loads many modules
    (numpy / pandas via plugins, optuna via backtest tests) and we'd
    otherwise see false positives. ``ALMANAK_STRATEGIES_DIR`` is forced
    to a non-existent path so the ``_auto_discover_strategies`` side
    effect in ``framework/strategies/__init__.py`` is a no-op (mirrors
    the deployed strategy container, which has no ``./strategies``
    directory at SDK install time).

    Importing ``almanak.cli.cli`` reproduces what happens when
    ``almanak strat run`` boots inside the strategy container: the
    top-level group plus every framework CLI subgroup get registered.
    Click does not invoke any subgroup's command callbacks at import
    time; it only resolves them when an argv subcommand is dispatched.
    So the modules we forbid here are precisely the ones that should
    have no business loading at registration time.
    """
    script = textwrap.dedent(
        """
        import json
        import sys
        from almanak.cli.cli import almanak  # noqa: F401
        sys.stdout.write(json.dumps(sorted(sys.modules)))
        """
    )
    env = os.environ.copy()
    env["ALMANAK_STRATEGIES_DIR"] = "/nonexistent_strategies_dir_for_lean_import_test"
    # Generous timeout: the import normally takes ~1-2s; 60s covers a
    # cold-cache CI run while still failing fast if some future import
    # path hangs (e.g. a synchronous network call slipped into a
    # subgroup ``__init__`` — exactly the kind of regression the rest
    # of this test exists to catch). ``TimeoutExpired`` propagates as
    # a plain test failure with the script/argv attached.
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )
    return set(json.loads(result.stdout))


def _check_absent(loaded: set[str], forbidden: tuple[str, ...], category: str) -> list[str]:
    return [f"{category}: {mod} is in sys.modules" for mod in forbidden if mod in loaded]


def test_cli_import_does_not_pull_heavy_modules() -> None:
    loaded = _import_cli_in_subprocess()

    failures: list[str] = []
    failures.extend(_check_absent(loaded, _FORBIDDEN_THIRD_PARTY, "third-party"))
    failures.extend(_check_absent(loaded, _FORBIDDEN_FRAMEWORK_SUBPACKAGES, "framework subpackage"))
    failures.extend(_check_absent(loaded, _FORBIDDEN_GATEWAY_STRIPPED_SURFACE, "gateway stripped-surface"))

    if failures:
        msg_lines = [
            "Importing almanak.cli.cli pulled in modules the deployed strategy",
            "container does not need at startup. The CLI bootstrap eagerly",
            "registers every framework CLI subgroup; one of those subgroup",
            "modules is doing eager backtest / UI imports at module load.",
            "",
            "The most likely culprit is a new module-level import in a CLI",
            "subgroup ``__init__.py`` that re-exports types from a heavy",
            "dependency, or a new top-level ``from almanak.framework.cli.* ...``",
            "in ``almanak/cli/cli.py`` that lands on a non-lazy submodule.",
            "Either move the import to function scope, or extend the lazy",
            "dispatch map in the affected ``__init__.py``.",
            "",
            "If the new import is genuinely required at every CLI startup,",
            "also update platform/packages/backend/src/templates/Dockerfile.strategy",
            "so the package stays in the deployed image.",
            "",
            *failures,
        ]
        raise AssertionError("\n".join(msg_lines))


# ---------------------------------------------------------------------------
# Lazy-resolution smoke tests
#
# The absence check above proves the CLI bootstrap stays lean. It does NOT
# prove that the lazy maps actually resolve correctly — a typo in
# ``_LAZY_IMPORTS`` (``framework/cli/backtest/__init__.py``),
# ``_SUBCOMMAND_MODULES`` (``framework/cli/backtest/group.py``), or the
# ohlcv ``__getattr__`` would ship green and only fail when a user
# (or a backtest test) actually reaches for the symbol. These smoke
# tests exercise one resolution from each surface so a broken mapping
# fails in CI instead of at runtime.
# ---------------------------------------------------------------------------


def test_backtest_lazy_re_exports_resolve() -> None:
    """Resolve a representative lazy re-export from each source module
    in ``framework/cli/backtest/__init__._LAZY_IMPORTS``.

    We deliberately fetch the package object via
    ``importlib.import_module`` instead of ``import
    almanak.framework.cli.backtest as backtest_pkg``. The parent
    ``framework.cli/__init__.py``'s lazy ``__getattr__`` resolves
    ``backtest`` to the click *group* and caches that result in
    ``framework.cli.__dict__['backtest']``. After any test (or the
    CLI bootstrap) does ``from almanak.framework.cli import backtest``,
    attribute lookup ``almanak.framework.cli.backtest`` returns the
    cached click group rather than the submodule, even though the
    submodule itself sits in ``sys.modules`` correctly.
    ``importlib.import_module`` reads ``sys.modules`` directly and
    bypasses that attribute-lookup shadow.
    """
    import importlib

    backtest_pkg = importlib.import_module("almanak.framework.cli.backtest")

    # From ``framework.backtesting``.
    assert backtest_pkg.BacktestResult.__name__ == "BacktestResult"
    assert backtest_pkg.PaperTrader.__name__ == "PaperTrader"
    # From a deeper module under ``framework.backtesting``.
    assert backtest_pkg.PaperTraderState.__name__ == "PaperTraderState"
    # From a sibling top-level (``framework.strategies``).
    assert callable(backtest_pkg.list_strategies)
    # From a local backtest submodule.
    assert callable(backtest_pkg.pnl_backtest)


def test_backtest_lazy_subcommand_resolution() -> None:
    """Force ``LazyBacktestGroup`` to resolve every mapped subcommand.

    A typo in ``_SUBCOMMAND_MODULES`` (wrong submodule name, wrong
    command name) fails here instead of at the next ``almanak strat
    backtest <typo>`` invocation. ``list_commands`` triggers the
    ``_ensure_loaded`` path for every distinct submodule, and
    ``get_command`` then asserts each registered name resolves to a
    real click command.
    """
    import click

    from almanak.framework.cli.backtest.group import LazyBacktestGroup, backtest

    assert isinstance(backtest, LazyBacktestGroup)

    ctx = click.Context(backtest)
    declared = sorted(LazyBacktestGroup._SUBCOMMAND_MODULES)
    listed = sorted(backtest.list_commands(ctx))
    assert declared == listed, (
        f"_SUBCOMMAND_MODULES declares {declared} but click.list_commands "
        f"resolved {listed}; a submodule probably failed to register a "
        "@backtest.command(...) decorator."
    )

    for name in declared:
        cmd = backtest.get_command(ctx, name)
        assert cmd is not None, f"LazyBacktestGroup.get_command({name!r}) returned None"
        assert cmd.name == name, f"command registered as {cmd.name!r}, expected {name!r}"


def test_ohlcv_lazy_getattr_resolves() -> None:
    """``OHLCVModule`` and ``GapStrategy`` resolve via the lazy
    ``__getattr__`` in ``framework/data/ohlcv/__init__.py``."""
    from almanak.framework.data import ohlcv

    assert ohlcv.OHLCVModule.__name__ == "OHLCVModule"
    # ``GapStrategy`` is a typing.Literal alias, so check identity via
    # the source module rather than ``__name__``.
    from almanak.framework.data.ohlcv.module import GapStrategy as _Source

    assert ohlcv.GapStrategy is _Source

    # Any other attribute access must still raise AttributeError so
    # importers don't silently get None for a typo'd symbol.
    import pytest

    with pytest.raises(AttributeError):
        _ = ohlcv.NotAThing
