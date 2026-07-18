"""Phase-2 acceptance: a mixed strategy accrues BOTH categories (ALM-2943).

The whole-strategy classification this replaces sent every position through
one adapter, so a mixed lending+LP strategy silently lost one category's
accrual (an LP-classified run accrued zero lending interest). The router
dispatches per intent and per position; the differential against an
explicitly-forced single adapter isolates the recovered interest exactly.
"""

from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.calculators import InterestCalculator
from almanak.framework.intents.lending_intents import SupplyIntent
from almanak.framework.intents.vocabulary import LPOpenIntent
from tests.validation.backtesting.trust_matrix import (
    INITIAL_CAPITAL,
    TICK_SECONDS,
    ScriptedStrategy,
    flat_series,
    run_backtest,
)

SUPPLY_PRINCIPAL = Decimal("3000")


def _mixed_intents() -> list:
    return [
        SupplyIntent(protocol="aave_v3", token="USDC", amount=SUPPLY_PRINCIPAL),
        LPOpenIntent(
            pool="WETH/USDC",
            protocol="uniswap_v3",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("1000"),
            range_upper=Decimal("4000"),
        ),
    ]


def _run(strategy_type: str | None):
    return run_backtest(
        ScriptedStrategy(_mixed_intents()),
        flat_series(12),
        hours=8,
        strategy_type=strategy_type,
    )


@pytest.mark.filterwarnings("ignore")
def test_mixed_strategy_accrues_both_categories() -> None:
    routed = _run(None)  # per-intent router (the default)
    forced_lp = _run("lp")  # the old single-adapter world, forced

    assert routed.success and forced_lp.success
    assert all(t.success for t in routed.trades), [t.error for t in routed.trades]
    assert routed.metrics.total_trades == 2

    # Hand-computed. The generic lane creates the SUPPLY position (the
    # lending adapter declines creation) and stamps the connector-declared
    # default APY for the protocol — the ONE table the accrual lane and
    # market.lending_rate() share; the sub-adapter then accrues one compound
    # increment per mark, starting the tick AFTER the fill.
    calculator = InterestCalculator()
    hourly = calculator.calculate_interest(
        principal=SUPPLY_PRINCIPAL,
        apy=calculator.get_supply_apy_for_protocol("aave_v3"),
        time_delta=Decimal(TICK_SECONDS) / Decimal(86400),
        compound=True,
    ).interest
    supply_at = routed.trades[0].timestamp
    accrual_marks = int((routed.equity_curve[-1].timestamp - supply_at).total_seconds()) // TICK_SECONDS
    expected_interest = accrual_marks * hourly
    assert expected_interest > 0

    # Flat prices: the ONLY equity difference between the routed run and the
    # forced-single-adapter run is the lending interest the old world lost.
    # Dust bound absorbs 28-digit context rounding across per-mark accruals.
    recovered = routed.final_capital_usd - forced_lp.final_capital_usd
    assert abs(recovered - expected_interest) < Decimal("1e-18")
    assert abs(routed.final_capital_usd - (INITIAL_CAPITAL + expected_interest)) < Decimal("1e-18")


def test_forced_single_adapter_still_available_as_escape_hatch() -> None:
    forced = _run("lp")
    assert forced.success
    # The explicit path is the baseline escape hatch: it still runs, it just
    # cannot accrue the category its adapter does not own.
    assert forced.final_capital_usd == INITIAL_CAPITAL
