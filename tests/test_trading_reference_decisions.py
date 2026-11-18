"""Trading references buy once, hold for their stated time, then exit."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from tests.test_dx_schemas import ROOT
from tests.test_optional_hold_seconds import Actions, strategy_module

TRADING = sorted(path.parent.name for path in (ROOT / "strategies").glob("*_trading/strategy.py"))
# References that time the hold from the buy's fill allow half a decision interval of slack.
FILL_TIMED = [
    name
    for name in TRADING
    if "decision_trigger" in (ROOT / "strategies" / name / "strategy.py").read_text()
]
BOUGHT = 1_000
HOLD = 600
INTERVAL = 60


def decide(name, *, bought=None, elapsed=0, hold=HOLD, entered=True, teardown=False):
    strategy = strategy_module(name)
    memory = strategy.State(bought_at={} if bought is None else {"trade": bought})
    context = SimpleNamespace(
        memory=memory,
        teardown=teardown,
        actions=Actions(),
        config=SimpleNamespace(decision_trigger=SimpleNamespace(seconds=INTERVAL)),
    )
    allocation = SimpleNamespace(name="trade", hold_seconds=hold)
    observed = SimpleNamespace(
        effective_at=datetime.fromtimestamp(BOUGHT + elapsed, UTC),
        owned=SimpleNamespace(entry_complete=entered),
    )
    return strategy.decide_allocation(context, allocation, observed)


def test_every_trading_reference_is_covered():
    assert len(TRADING) == 25


@pytest.mark.parametrize("name", TRADING)
def test_an_unbought_allocation_swaps_until_its_entry_completes(name):
    assert decide(name, entered=False) == ("swap", "trade")
    assert decide(name) is None


@pytest.mark.parametrize("name", TRADING)
def test_a_bought_allocation_exits_once_its_hold_elapses(name):
    assert decide(name, bought=BOUGHT, elapsed=HOLD - INTERVAL) is None
    assert decide(name, bought=BOUGHT, elapsed=HOLD) == ("exit", "trade")


@pytest.mark.parametrize("name", FILL_TIMED)
def test_a_fill_timed_hold_fires_half_an_interval_early_and_no_earlier(name):
    early = HOLD - INTERVAL // 2
    assert decide(name, bought=BOUGHT, elapsed=early - 1) is None
    assert decide(name, bought=BOUGHT, elapsed=early) == ("exit", "trade")


@pytest.mark.parametrize("name", TRADING)
def test_a_bought_allocation_without_a_hold_waits_for_teardown(name):
    assert decide(name, bought=BOUGHT, elapsed=10**7, hold=None) is None
    assert decide(name, bought=BOUGHT, hold=None, teardown=True) == ("exit", "trade")


@pytest.mark.parametrize("name", TRADING)
def test_teardown_exits_before_any_entry(name):
    assert decide(name, entered=False, teardown=True) == ("exit", "trade")
