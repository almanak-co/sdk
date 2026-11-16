"""The RSI reference's decision: repeating buy-and-sell cycles on RSI-14 crossings."""

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.reference_parameters import authored_parameters

SOURCE = Path(__file__).parents[1] / "strategies/uniswap_v3_base_rsi/strategy.py"
HOLD = 7 * 86400
NOW = 1_800_000_000

KNOBS = authored_parameters("uniswap_v3_base_rsi")


@pytest.fixture(scope="module")
def rsi():
    spec = importlib.util.spec_from_file_location("uniswap_v3_base_rsi", SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def context(rsi, memory, *, values=(), complete=True, holding=False, teardown=False, exited=False):
    def decision(intent=None, memory=None, reason=None):
        return SimpleNamespace(intent=intent, memory=memory, reason=reason)

    observed = SimpleNamespace(
        effective_at=datetime.fromtimestamp(NOW, UTC),
        owned=SimpleNamespace(entry_complete=holding),
    )
    return SimpleNamespace(
        memory=memory,
        teardown=teardown,
        require_observation=lambda name: observed,
        config=SimpleNamespace(
            allocation=lambda name: SimpleNamespace(hold_seconds=HOLD),
            parameters=KNOBS,
        ),
        price=lambda asset: SimpleNamespace(complete=complete, values=values),
        actions=SimpleNamespace(
            swap=lambda name: ("swap", name), exit=lambda name: None if exited else ("exit", name)
        ),
        decision=decision,
    )


def falling():
    return [100.0 + i for i in range(100)] + [200.0 - 3 * i for i in range(20)]


def rising():
    return [200.0 - i for i in range(100)] + [100.0 + 3 * i for i in range(20)]


def test_teardown_and_an_expired_hold_exit_before_reading_prices(rsi):
    state = rsi.State()
    made = rsi.decide(context(rsi, state, teardown=True))
    assert made.intent == ("exit", "position") and made.memory.selling
    held = rsi.State(bought_at=NOW - HOLD)
    assert rsi.decide(context(rsi, held, holding=True)).intent == ("exit", "position")
    latched = rsi.State(selling=True)
    assert rsi.decide(context(rsi, latched, holding=True)).intent == ("exit", "position")


def test_a_pending_buy_is_retried_until_the_swap_completes(rsi):
    made = rsi.decide(context(rsi, rsi.State(buying=True), complete=False))
    assert made.intent == ("swap", "position")


def test_an_undefined_rsi_decides_nothing_and_keeps_memory(rsi):
    made = rsi.decide(context(rsi, rsi.State(last_rsi=50.0), values=[float("nan")] * 120))
    assert made.intent is None and made.memory is None


def test_an_incomplete_series_decides_nothing(rsi):
    made = rsi.decide(context(rsi, rsi.State(last_rsi=50.0), complete=False))
    assert made.intent is None and made.memory is None


def test_a_cross_below_oversold_buys_once_and_marks_the_buy_pending(rsi):
    first = rsi.decide(context(rsi, rsi.State(), values=falling()))
    assert first.intent is None and first.memory.last_rsi < KNOBS.oversold
    crossed = rsi.decide(context(rsi, rsi.State(last_rsi=40.0), values=falling()))
    assert crossed.intent == ("swap", "position") and crossed.memory.buying
    below = rsi.decide(context(rsi, rsi.State(last_rsi=20.0), values=falling()))
    assert below.intent is None


def test_a_cross_above_overbought_sells_while_holding(rsi):
    crossed = rsi.decide(context(rsi, rsi.State(last_rsi=60.0), values=rising(), holding=True))
    assert crossed.intent == ("exit", "position") and crossed.memory.selling
    flat = rsi.decide(context(rsi, rsi.State(last_rsi=60.0), values=rising()))
    assert flat.intent is None and flat.memory.last_rsi > KNOBS.overbought


def test_a_completed_funding_swap_records_the_entry(rsi):
    target = SimpleNamespace(funding_currency="usdc")
    result = SimpleNamespace(
        status="completed",
        effective_at=datetime.fromtimestamp(NOW, UTC),
        intent=SimpleNamespace(
            kind="swap_exact_input", amount=SimpleNamespace(asset="usdc"), target=target
        ),
    )
    memory = rsi.on_outcome(SimpleNamespace(memory=rsi.State(buying=True), result=result))
    assert memory.bought_at == NOW and not memory.buying
    result.intent.amount.asset = "weth"
    assert rsi.on_outcome(SimpleNamespace(memory=rsi.State(), result=result)).bought_at is None


def test_a_completed_exit_resets_the_cycle_so_the_next_cross_buys_again(rsi):
    closed = rsi.State(selling=True, bought_at=NOW - 3600, last_rsi=40.0)
    again = rsi.decide(context(rsi, closed, values=falling(), exited=True))
    assert again.intent == ("swap", "position")
    assert again.memory.buying and not again.memory.selling and again.memory.bought_at is None
    idle = rsi.decide(context(rsi, closed.updated(last_rsi=20.0), values=falling(), exited=True))
    assert idle.intent is None and not idle.memory.selling
    teardown = rsi.decide(context(rsi, closed, teardown=True, exited=True))
    assert teardown.intent is None and teardown.memory.selling


def test_each_decision_states_why_it_holds_or_trades(rsi):
    waiting = rsi.decide(context(rsi, rsi.State(), complete=False))
    assert waiting.reason == "Waiting for a full WETH price history"
    idle = rsi.decide(context(rsi, rsi.State(), values=falling()))
    assert idle.reason == f"RSI {idle.memory.last_rsi:.0f}; buying below {KNOBS.oversold:g}"
    crossed = rsi.decide(context(rsi, rsi.State(last_rsi=40.0), values=falling()))
    assert crossed.reason.startswith("RSI ") and crossed.reason.endswith(", buying")
    held = rsi.decide(context(rsi, rsi.State(last_rsi=50.0), values=falling(), holding=True))
    assert held.reason.endswith(f"; selling above {KNOBS.overbought:g}")
