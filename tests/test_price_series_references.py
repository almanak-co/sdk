"""Decisions of the price-series references: RSI-gated LP, SMA-gated loan, ATR breakout."""

import importlib.util
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.v3_pool_domain import TickBand
from tests.reference_parameters import authored_parameters

STRATEGIES = Path(__file__).parents[1] / "strategies"
NOW = 1_800_000_000
HOUR = 3600


def load(name):
    spec = importlib.util.spec_from_file_location(name, STRATEGIES / name / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def rsi_lp():
    return load("uniswap_v3_base_rsi_lp")


@pytest.fixture(scope="module")
def sma():
    return load("aave_v3_base_sma_borrow")


@pytest.fixture(scope="module")
def atr():
    return load("uniswap_v3_base_atr_breakout")


def decision(intent=None, memory=None, reason=None):
    return SimpleNamespace(intent=intent, memory=memory, reason=reason)


def series(closes, *, complete=True, highs=None, lows=None):
    points = tuple(
        SimpleNamespace(value=Decimal(str(close)), source_time=NOW - (len(closes) - i) * HOUR)
        for i, close in enumerate(closes)
    )
    return SimpleNamespace(
        complete=complete,
        values=tuple(p.value for p in points),
        points=points,
        highs=tuple(Decimal(str(v)) for v in (highs or [c * 1.002 for c in closes])),
        lows=tuple(Decimal(str(v)) for v in (lows or [c * 0.998 for c in closes])),
    )


def ranging():
    return [2500.0 + (10 if i % 2 else -10) for i in range(120)]


def trending_up():
    return [2000.0 + 5 * i for i in range(120)]


# RSI-gated LP


BAND = TickBand(lower=-1000, upper=1000, width=200)


def lp_context(
    rsi_lp, memory, prices, *, position=None, teardown=False, quote=True, tick=0, exited=False
):
    actions = SimpleNamespace(
        swap=lambda name, rebalance=False: ("swap", rebalance),
        open_liquidity=lambda name: ("open",),
        close_liquidity=lambda name: ("close",),
        exit=lambda name: None if exited else ("exit",),
    )
    observed = SimpleNamespace(
        effective_at=datetime.fromtimestamp(NOW, UTC),
        owned=SimpleNamespace(position=position),
        rebalance_quote=object() if quote else None,
        pool=SimpleNamespace(tick=tick),
        target=SimpleNamespace(band=BAND, pool=SimpleNamespace(tick_spacing=10)),
    )
    return SimpleNamespace(
        memory=memory,
        teardown=teardown,
        require_observation=lambda name: observed,
        config=SimpleNamespace(
            allocation=lambda name: SimpleNamespace(hold_seconds=7 * 86400),
            parameters=authored_parameters("uniswap_v3_base_rsi_lp"),
        ),
        price=lambda asset: prices,
        actions=actions,
        decision=decision,
    )


def test_the_lp_opens_only_while_rsi_is_ranging(rsi_lp):
    made = rsi_lp.decide(lp_context(rsi_lp, rsi_lp.State(), series(ranging())))
    assert made.intent == ("swap", True) and made.memory.entering
    trending = rsi_lp.decide(lp_context(rsi_lp, rsi_lp.State(), series(trending_up())))
    assert trending.intent is None
    short = rsi_lp.decide(lp_context(rsi_lp, rsi_lp.State(), series(ranging(), complete=False)))
    assert short.intent is None


def test_a_started_entry_finishes_regardless_of_the_next_reading(rsi_lp):
    memory = rsi_lp.State(entering=True, rebalanced=True)
    made = rsi_lp.decide(lp_context(rsi_lp, memory, series(trending_up())))
    assert made.intent == ("open",)


def test_a_trend_closes_the_position_and_the_cycle(rsi_lp):
    memory = rsi_lp.State(opened_at=NOW - HOUR)
    made = rsi_lp.decide(lp_context(rsi_lp, memory, series(trending_up()), position=object()))
    assert made.intent == ("close",) and made.memory.closing
    held = rsi_lp.decide(lp_context(rsi_lp, memory, series(ranging()), position=object()))
    assert held.intent is None and held.memory is None
    after = rsi_lp.decide(lp_context(rsi_lp, rsi_lp.State(closing=True), series(ranging())))
    assert after.intent == ("exit",)


def test_a_completed_close_resets_the_cycle_so_ranging_opens_again(rsi_lp):
    closed = rsi_lp.State(closing=True, opened_at=NOW - HOUR, rebalanced=True)
    again = rsi_lp.decide(lp_context(rsi_lp, closed, series(ranging()), exited=True))
    assert again.intent == ("swap", True)
    assert again.memory == rsi_lp.State(entering=True)
    torn = rsi_lp.decide(lp_context(rsi_lp, closed, None, teardown=True, exited=True))
    assert torn.intent is None and torn.memory.closing


def test_the_lp_waits_while_the_price_is_outside_the_envelope(rsi_lp):
    outside = rsi_lp.decide(lp_context(rsi_lp, rsi_lp.State(), series(ranging()), tick=1500))
    assert outside.intent is None and outside.memory is None
    entering = rsi_lp.State(entering=True)
    waiting = rsi_lp.decide(lp_context(rsi_lp, entering, series(ranging()), tick=-1500))
    assert waiting.intent is None


def test_lp_teardown_closes_before_reading_prices(rsi_lp):
    memory = rsi_lp.State(opened_at=NOW - HOUR)
    context = lp_context(rsi_lp, memory, None, position=object(), teardown=True)
    assert rsi_lp.decide(context).intent == ("close",)


def test_lp_outcomes_record_the_funding_swap_and_the_open(rsi_lp):
    def outcome(memory, kind):
        result = SimpleNamespace(
            status="completed",
            effective_at=datetime.fromtimestamp(NOW, UTC),
            intent=SimpleNamespace(kind=kind),
        )
        return rsi_lp.on_outcome(SimpleNamespace(memory=memory, result=result))

    swapped = outcome(rsi_lp.State(entering=True), "swap_exact_input")
    assert swapped.rebalanced
    opened = outcome(swapped, "open_liquidity")
    assert (opened.entering, opened.opened_at) == (False, NOW)


# SMA-gated loan


def loan_context(sma, memory, prices, *, debt=0, teardown=False, supplied=1):
    collateral = SimpleNamespace(
        position=SimpleNamespace(
            supplied_scaled=supplied, collateral_enabled=bool(supplied), debt_scaled=0
        ),
        allowance=0,
    )
    loan = SimpleNamespace(position=SimpleNamespace(debt_scaled=debt), allowance=0)
    usdc = "usdc"
    config = SimpleNamespace(
        allocation=lambda name: SimpleNamespace(
            capital=10**16, reserve=SimpleNamespace(asset=usdc)
        ),
        asset_metadata={usdc: SimpleNamespace(decimals=6)},
        parameters=authored_parameters("aave_v3_base_sma_borrow"),
    )
    actions = SimpleNamespace(
        supply=lambda name, amount: ("supply", amount),
        borrow=lambda name, amount: ("borrow", amount),
        repay=lambda name: ("repay",),
        withdraw=lambda name: ("withdraw",),
    )
    return SimpleNamespace(
        memory=memory,
        teardown=teardown,
        observation=lambda name: {"collateral": collateral, "loan": loan}[name],
        config=config,
        price=lambda asset: prices,
        actions=actions,
        decision=decision,
    )


def above_average():
    return [2000.0] * 59 + [2100.0]


def below_average():
    return [2000.0] * 59 + [1900.0]


def test_the_loan_supplies_first_then_borrows_above_the_average(sma):
    first = sma.decide(loan_context(sma, sma.State(), series(above_average(), complete=True)))
    assert first.intent == ("supply", 10**16)
    supplied = sma.State(supplied=True)
    made = sma.decide(loan_context(sma, supplied, series(above_average())))
    assert made.intent == ("borrow", 5_000_000)
    flat = sma.decide(loan_context(sma, supplied, series([2000.0] * 60)))
    assert flat.intent is None


def test_the_loan_is_repaid_below_the_average_and_taken_again_above_it(sma):
    borrowed = sma.State(supplied=True, borrowed=True)
    made = sma.decide(loan_context(sma, borrowed, series(below_average()), debt=5))
    assert made.intent == ("repay",)
    inside = sma.decide(loan_context(sma, borrowed, series([2000.0] * 59 + [1990.0]), debt=5))
    assert inside.intent is None  # within the margin: no whipsaw
    repaid = sma.State(supplied=True)
    assert sma.decide(loan_context(sma, repaid, series(above_average()))).intent == (
        "borrow",
        5_000_000,
    )


def test_loan_teardown_repays_then_withdraws(sma):
    memory = sma.State(supplied=True, borrowed=True)
    assert sma.decide(loan_context(sma, memory, None, debt=5, teardown=True)).intent == ("repay",)
    made = sma.decide(loan_context(sma, memory, None, teardown=True))
    assert made.intent == ("withdraw",)
    assert sma.decide(loan_context(sma, memory, None, teardown=True, supplied=0)).intent is None


def test_loan_outcomes_track_the_cycle(sma):
    def outcome(memory, kind, status="completed"):
        result = SimpleNamespace(status=status, intent=SimpleNamespace(kind=kind))
        return sma.on_outcome(SimpleNamespace(memory=memory, result=result))

    memory = outcome(sma.State(), "supply")
    memory = outcome(memory, "borrow")
    assert (memory.supplied, memory.borrowed) == (True, True)
    assert outcome(memory, "repay", status="failed") == memory
    memory = outcome(memory, "repay")
    assert (memory.supplied, memory.borrowed) == (True, False)


# ATR breakout


def atr_context(atr, memory, prices, *, holding=False, teardown=False, exited=False):
    observed = SimpleNamespace(
        effective_at=datetime.fromtimestamp(NOW, UTC),
        owned=SimpleNamespace(entry_complete=holding),
    )
    return SimpleNamespace(
        memory=memory,
        teardown=teardown,
        require_observation=lambda name: observed,
        config=SimpleNamespace(
            allocation=lambda name: SimpleNamespace(hold_seconds=7 * 86400),
            parameters=authored_parameters("uniswap_v3_base_atr_breakout"),
        ),
        price=lambda asset: prices,
        actions=SimpleNamespace(
            swap=lambda name: ("swap",), exit=lambda name: None if exited else ("exit",)
        ),
        decision=decision,
    )


def calm_rise():
    return [2400.0 + 2 * i for i in range(48)]


def test_the_breakout_buys_above_the_midline_in_a_calm_market(atr):
    made = atr.decide(atr_context(atr, atr.State(), series(calm_rise())))
    assert made.intent == ("swap",) and made.memory.buying
    falling = [2600.0 - 2 * i for i in range(48)]
    assert atr.decide(atr_context(atr, atr.State(), series(falling))).intent is None
    wild = series(calm_rise(), highs=[c * 1.05 for c in calm_rise()])
    assert atr.decide(atr_context(atr, atr.State(), wild)).intent is None


def test_the_trailing_stop_exits_below_the_highest_close_since_entry(atr):
    closes = [*calm_rise()[:-1], 2400.0]
    memory = atr.State(bought_at=NOW - 10 * HOUR)
    made = atr.decide(atr_context(atr, memory, series(closes), holding=True))
    assert made.intent == ("exit",) and made.memory.selling
    held = atr.decide(atr_context(atr, memory, series(calm_rise()), holding=True))
    assert held.intent is None and held.memory is None


def test_a_completed_exit_resets_the_cycle_so_the_next_signal_buys_again(atr):
    closed = atr.State(selling=True, bought_at=NOW - 10 * HOUR)
    again = atr.decide(atr_context(atr, closed, series(calm_rise()), exited=True))
    assert again.intent == ("swap",)
    assert again.memory.buying and not again.memory.selling and again.memory.bought_at is None


def test_a_pending_buy_is_retried_and_teardown_exits_first(atr):
    retried = atr.decide(atr_context(atr, atr.State(buying=True), series([], complete=False)))
    assert retried.intent == ("swap",)
    torn = atr.decide(atr_context(atr, atr.State(bought_at=NOW), None, teardown=True))
    assert torn.intent == ("exit",)


def test_an_incomplete_or_undefined_window_decides_nothing(atr):
    short = series(calm_rise(), complete=False)
    assert atr.decide(atr_context(atr, atr.State(), short)).intent is None
    undefined = series([float("nan")] * 48)
    assert atr.decide(atr_context(atr, atr.State(), undefined)).intent is None


# Configuration checks


def lending_config(collateral_asset, loan_asset, **overrides):
    def allocation(name, asset, capital, collateral, borrow):
        reserve = SimpleNamespace(asset=asset, pool="pool")
        return SimpleNamespace(
            name=name,
            account="fund",
            chain="base",
            reserve=reserve,
            capital=capital,
            collateral=collateral,
            borrow=borrow,
        )

    items = {
        "collateral": allocation("collateral", collateral_asset, 10**16, True, False),
        "loan": allocation("loan", loan_asset, 0, False, True),
    }
    for name, fields in overrides.items():
        for key, value in fields.items():
            setattr(items[name], key, value)
    return SimpleNamespace(
        positions=tuple(items.values()),
        allocation=items.__getitem__,
        parameters=authored_parameters("aave_v3_base_sma_borrow"),
        asset_metadata={loan_asset: SimpleNamespace(decimals=6)},
    )


def test_the_loan_configuration_requires_weth_collateral_and_a_usdc_loan(sma):
    sma.validate_config(lending_config(sma.WETH, sma.USDC))
    refusals = (
        (lending_config(sma.USDC, sma.USDC), "must be WETH"),
        (lending_config(sma.WETH, sma.WETH), "borrows USDC"),
        (lending_config(sma.WETH, sma.USDC, loan={"account": "other"}), "same fund"),
        (lending_config(sma.WETH, sma.USDC, collateral={"capital": 0}), "starting capital"),
        (lending_config(sma.WETH, sma.USDC, loan={"borrow": False}), "requires borrowing"),
    )
    for config, message in refusals:
        with pytest.raises(ValueError, match=message):
            sma.validate_config(config)
    one = SimpleNamespace(
        positions=(SimpleNamespace(name="collateral"),),
        parameters=authored_parameters("aave_v3_base_sma_borrow"),
    )
    with pytest.raises(ValueError, match="collateral and loan"):
        sma.validate_config(one)


def test_the_breakout_hold_must_fit_the_candle_window(atr):
    parameters = authored_parameters("uniswap_v3_base_atr_breakout")

    def config(hold):
        allocation = SimpleNamespace(range=None, band=None, hold_seconds=hold)
        return SimpleNamespace(allocations=(allocation,), parameters=parameters)

    # The hold limit is the window less the channel, in hours: 72 - 24 = 48.
    limit = (parameters.window - parameters.channel) * HOUR
    assert limit == parameters.hold_limit == 48 * HOUR
    atr.validate_config(config(limit))
    with pytest.raises(ValueError, match="candle window"):
        atr.validate_config(config(limit + 1))
    lp = SimpleNamespace(
        allocations=(SimpleNamespace(range=object(), band=None),), parameters=parameters
    )
    with pytest.raises(ValueError, match="liquidity"):
        atr.validate_config(lp)


def test_the_lp_configuration_requires_a_band(rsi_lp):
    parameters = authored_parameters("uniswap_v3_base_rsi_lp")

    def config(band):
        return SimpleNamespace(allocations=(SimpleNamespace(band=band),), parameters=parameters)

    rsi_lp.validate_config(config(object()))
    with pytest.raises(ValueError, match="band"):
        rsi_lp.validate_config(config(None))
