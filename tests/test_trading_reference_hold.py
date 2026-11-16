"""Trading references time the hold from the fill with half a decision interval of slack."""

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).parents[1]
TRADING = ("uniswap_v3_base_trading", "uniswap_v3_arbitrum_trading")
INTERVAL = 3600
HOLD = 7200
FILLED = 1_000_000


@pytest.fixture(params=TRADING)
def trading(request):
    path = ROOT / "strategies" / request.param / "strategy.py"
    spec = importlib.util.spec_from_file_location(request.param, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def decided(module, *, at, bought=FILLED, teardown=False, entered=True, hold=HOLD):
    memory = module.State(bought_at={} if bought is None else {"trade": bought})
    context = SimpleNamespace(
        memory=memory,
        teardown=teardown,
        config=SimpleNamespace(decision_trigger=SimpleNamespace(seconds=INTERVAL)),
        actions=SimpleNamespace(exit=lambda name: ("exit", name), swap=lambda name: ("swap", name)),
    )
    allocation = SimpleNamespace(name="trade", hold_seconds=hold)
    observed = SimpleNamespace(
        effective_at=datetime.fromtimestamp(at, UTC),
        owned=SimpleNamespace(entry_complete=entered),
    )
    return module.decide_allocation(context, allocation, observed)


def test_the_decision_on_schedule_exits_although_the_fill_landed_after_it(trading):
    # The buy decided at FILLED - 12 s; the decision two intervals later is due.
    assert decided(trading, at=FILLED - 12 + HOLD) == ("exit", "trade")


def test_a_decision_a_whole_interval_early_never_exits(trading):
    assert decided(trading, at=FILLED + HOLD - INTERVAL) is None


def test_no_fill_or_no_hold_never_times_out(trading):
    assert decided(trading, at=FILLED + 10 * HOLD, bought=None, entered=False) == (
        "swap",
        "trade",
    )
    assert decided(trading, at=FILLED + 10 * HOLD, hold=None) is None


def test_teardown_exits_at_once(trading):
    assert decided(trading, at=FILLED, teardown=True) == ("exit", "trade")
