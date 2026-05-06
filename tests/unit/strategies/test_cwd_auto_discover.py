"""VIB-2917: strategies registered via `./strategy.py` in the current working directory.

Prior to this ticket, `almanak strat backtest pnl` silently fell back to a mock
strategy when run from a folder that did not contain a `strategies/<name>/` subtree.
The new helper `_auto_discover_cwd_strategy()` mirrors `almanak strat run`'s behavior
so the backtest CLIs see the local strategy by importing `./strategy.py` at
module load time.
"""

from __future__ import annotations

import sys
import textwrap

import pytest


STRATEGY_SOURCE = textwrap.dedent(
    '''
    """Fixture strategy used by test_cwd_auto_discover.py."""

    from decimal import Decimal

    from almanak.framework.intents import Intent
    from almanak.framework.market import MarketSnapshot
    from almanak.framework.strategies import IntentStrategy, almanak_strategy


    @almanak_strategy(name="{name}")
    class CwdFixtureStrategy(IntentStrategy):
        def decide(self, market: MarketSnapshot) -> Intent:
            return Intent.hold(reason="fixture")
    '''
).strip()


@pytest.fixture
def _isolated_cwd(tmp_path, monkeypatch):
    """Run the body with a clean cwd and no lingering strategy modules/registry entries."""
    monkeypatch.chdir(tmp_path)
    # Ensure ALMANAK_STRATEGIES_DIR doesn't point at the real repo.
    monkeypatch.delenv("ALMANAK_STRATEGIES_DIR", raising=False)
    yield tmp_path


def _run_cwd_autodiscover(strategy_name: str) -> None:
    # Imported lazily so the fixture's monkeypatch.chdir takes effect first.
    from almanak.framework.strategies import _auto_discover_cwd_strategy  # type: ignore[attr-defined]

    _auto_discover_cwd_strategy()
    _ = strategy_name  # kept for readability at call sites


class TestCwdAutoDiscover:
    def test_registers_strategy_from_cwd(self, _isolated_cwd) -> None:
        from almanak.framework.strategies import STRATEGY_REGISTRY, get_strategy, unregister_strategy

        strategy_name = "vib_2917_cwd_fixture_registers"
        (_isolated_cwd / "strategy.py").write_text(STRATEGY_SOURCE.format(name=strategy_name))

        try:
            assert strategy_name not in STRATEGY_REGISTRY
            _run_cwd_autodiscover(strategy_name)
            assert strategy_name in STRATEGY_REGISTRY, (
                "local ./strategy.py should register via @almanak_strategy (VIB-2917)"
            )
            # Round-trip through the public lookup used by the backtest CLI.
            assert get_strategy(strategy_name).__name__ == "CwdFixtureStrategy"
        finally:
            if strategy_name in STRATEGY_REGISTRY:
                unregister_strategy(strategy_name)
            # Remove any cwd module we inserted so repeated runs don't collide.
            for mod in [m for m in sys.modules if m.startswith("_cwd_strategy_")]:
                sys.modules.pop(mod, None)

    def test_noop_when_no_cwd_strategy(self, _isolated_cwd) -> None:
        from almanak.framework.strategies import STRATEGY_REGISTRY

        registry_before = dict(STRATEGY_REGISTRY)
        modules_before = set(sys.modules)

        _run_cwd_autodiscover("unused")

        assert dict(STRATEGY_REGISTRY) == registry_before
        assert {m for m in sys.modules if m.startswith("_cwd_strategy_")} == {
            m for m in modules_before if m.startswith("_cwd_strategy_")
        }
