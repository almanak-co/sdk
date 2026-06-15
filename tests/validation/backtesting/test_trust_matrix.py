"""Backtest Trust Matrix cells (VIB-5081) - network-free tier.

Each test here is one cell of the matrix defined in ``trust_matrix.py``:
rows are the conservation/math invariants from blueprint 31 section 4.3,
columns are strategy types. All cells run the REAL engine/portfolio/adapter
code over synthetic price data - no network, no API keys, no mocks of the
code under test.

Run: ``uv run pytest tests/validation/backtesting -m "not validation"``

Known-bug cells are ``xfail(strict=True)`` with the tracking reference in
the reason. NEVER weaken an assertion to make a cell pass - the assertion
is the spec; xfail documents the gap. A PASS -> FAIL transition is a
stop-the-line event (blueprint 31 section 9).

Candidate stop-the-line findings encoded as strict xfails by this module
(discovered while building the matrix; see the VIB-5081 PR body):

1. FIXED (VIB-5096): LP positions minted value on open in BOTH lanes - the
   ``liquidity`` field held the USD notional but every valuation path
   interpreted it as Uniswap V3 L-units, so a $5K open marked at
   ~$131K-$452K. Producers now convert deposits into true L-units via
   ``ImpermanentLossCalculator.liquidity_for_target_value``; the three LP
   cells pass with their assertions unchanged.
2. FIXED (VIB-5097): Lending WITHDRAW used to never close the supply
   position, double-counting the principal on a SUPPLY -> WITHDRAW round
   trip; WITHDRAW now closes (or partially reduces) the matched SUPPLY
   position and realizes accrued interest as PnL.
"""

from __future__ import annotations

import inspect
import math
import sys
from datetime import timedelta
from decimal import Decimal

import pytest

from almanak.core.models.quote_asset import QuoteAsset
from almanak.framework.backtesting.adapters.lp_adapter import (
    LPBacktestAdapter,
    LPBacktestConfig,
)
from almanak.framework.backtesting.pnl.calculators.funding import FundingCalculator
from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    MAX_TICK,
    MIN_TICK,
    ImpermanentLossCalculator,
)
from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.metrics_calculator import (
    calculate_max_drawdown,
    calculate_returns,
    calculate_sharpe_ratio,
    calculate_volatility,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.intents.lending_intents import (
    BorrowIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent
from tests.validation.backtesting.trust_matrix import (
    CELLS_BY_ID,
    INITIAL_CAPITAL,
    START,
    TICK_SECONDS,
    LPOpenDuck,
    PerpCloseDuck,
    PerpOpenDuck,
    ScriptedStrategy,
    SupplyDuck,
    SwapDuck,
    flat_series,
    run_backtest,
)


def _market_state(hour: int, weth: str = "2000") -> MarketState:
    return MarketState(
        timestamp=START + timedelta(hours=hour),
        prices={"WETH": Decimal(weth), "USDC": Decimal("1")},
        chain="arbitrum",
        block_number=1_000_000 + hour,
        gas_price_gwei=Decimal("30"),
    )


# =============================================================================
# swap column - the generic engine lane (the default path the Feb 2026
# manual trust run missed)
# =============================================================================


@pytest.mark.trust_cell("swap:no_trade_conservation")
def test_swap_no_trade_conservation() -> None:
    """Hold-only run: final equity == initial capital, Decimal-exact."""
    result = run_backtest(ScriptedStrategy([None] * 10), flat_series(12), hours=9)

    assert result.success
    assert result.metrics.total_trades == 0
    assert result.final_capital_usd == INITIAL_CAPITAL
    assert all(point.value_usd == INITIAL_CAPITAL for point in result.equity_curve)


@pytest.mark.trust_cell("swap:single_trade_closed_form")
def test_swap_single_trade_closed_form() -> None:
    """Buy $5,000 of WETH at $2,000, mark at $2,500: equity == $11,250 exactly.

    Closed form: position = 5000 / 2000 = 2.5 WETH; delta = 2.5 x (2500 -
    2000) = +1250; zero costs. The buy is decided at t0 and executes at t1
    (the engine queues intents one tick even with inclusion_delay_blocks=0),
    so the price series holds 2000 through t1.
    """
    series = {
        "WETH": [Decimal("2000")] * 2 + [Decimal("2500")] * 10,
        "USDC": [Decimal("1")] * 12,
    }
    strategy = ScriptedStrategy([SwapDuck(amount_usd=Decimal("5000"))])

    result = run_backtest(strategy, series, hours=6)

    assert result.success
    assert result.metrics.total_trades == 1
    assert result.trades[0].success
    assert result.final_capital_usd == Decimal("11250")


@pytest.mark.trust_cell("swap:round_trip_conservation")
def test_swap_round_trip_conservation() -> None:
    """Buy then sell at the same price with zero costs returns initial capital.

    Uses the DEFAULT portfolio construction (initial capital as cash_usd,
    stables swept to cash) through the real engine loop - the exact path the
    Feb 2026 manual trust run missed and where VIB-5082 lived.
    """
    intents = [
        SwapDuck(amount_usd=Decimal("5000")),  # buy 2.5 WETH
        SwapDuck(from_token="WETH", to_token="USDC", amount_usd=Decimal("5000")),  # sell 2.5 WETH
    ]
    result = run_backtest(ScriptedStrategy(intents), flat_series(12), hours=8)

    assert result.success
    assert result.metrics.total_trades == 2
    assert all(trade.success for trade in result.trades)
    assert result.final_capital_usd == INITIAL_CAPITAL
    # Conservation must hold at every mark, not just the endpoint.
    assert all(point.value_usd == INITIAL_CAPITAL for point in result.equity_curve)


#: WETH on Arbitrum (the default backtest chain) -- the numeraire token used by
#: the conservation cell below. flat_series prices WETH at $2,000.
_WETH_ARBITRUM = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"


@pytest.mark.trust_cell("swap:round_trip_conservation_numeraire")
def test_swap_round_trip_conservation_numeraire() -> None:
    """Round trip with a WETH numeraire conserves value in WETH (VIB-5127).

    Same buy-then-sell round trip as the USD cell, but the strategy declares a
    token quote_asset (WETH on Arbitrum). The USD core is untouched (final USD
    equity == initial capital), and the additive numeraire projection values the
    portfolio at a flat 5 WETH (10,000 USD / 2,000 USD-per-WETH) at every mark.
    Decimal-exact because the WETH price is flat.
    """
    intents = [
        SwapDuck(amount_usd=Decimal("5000")),  # buy 2.5 WETH
        SwapDuck(from_token="WETH", to_token="USDC", amount_usd=Decimal("5000")),  # sell 2.5 WETH
    ]
    strategy = ScriptedStrategy(intents, quote_asset=QuoteAsset.token(42161, _WETH_ARBITRUM))
    result = run_backtest(strategy, flat_series(12), hours=8)

    assert result.success
    assert all(trade.success for trade in result.trades)
    # USD conservation core is unchanged.
    assert result.final_capital_usd == INITIAL_CAPITAL
    # Numeraire projection: 10,000 / 2,000 = 5 WETH, conserved at every mark.
    expected_weth = INITIAL_CAPITAL / Decimal("2000")
    assert result.numeraire == "WETH"
    assert result.initial_capital_numeraire == expected_weth
    assert result.final_capital_numeraire == expected_weth
    assert result.metrics.numeraire_metrics is not None
    assert result.metrics.numeraire_metrics.numeraire == "WETH"
    assert result.metrics.numeraire_metrics.total_pnl == Decimal("0")
    for point in result.equity_curve:
        assert point.numeraire_price_usd == Decimal("2000")
        assert point.value_usd / point.numeraire_price_usd == expected_weth


@pytest.mark.trust_cell("swap:fiat_usd_pin")
def test_swap_fiat_usd_byte_for_byte_pin() -> None:
    """A default (USD) strategy emits no numeraire fields -- byte-for-byte pin.

    Guards the additive contract (VIB-5127): the numeraire feature must never
    change a fiat_usd artifact. Any reviewer who reuses the _usd storage for
    the numeraire would grow numeraire* keys here and trip this cell.
    """
    intents = [
        SwapDuck(amount_usd=Decimal("5000")),
        SwapDuck(from_token="WETH", to_token="USDC", amount_usd=Decimal("5000")),
    ]
    result = run_backtest(ScriptedStrategy(intents), flat_series(12), hours=8)

    assert result.success
    assert result.numeraire is None
    assert result.initial_capital_numeraire is None
    assert result.final_capital_numeraire is None
    assert result.metrics.numeraire_metrics is None
    assert all(point.numeraire_price_usd is None for point in result.equity_curve)

    # No numeraire* keys leak into the serialized artifact.
    payload = result.to_dict()
    assert "numeraire" not in payload
    assert "initial_capital_numeraire" not in payload
    assert "final_capital_numeraire" not in payload
    assert "numeraire_metrics" not in payload["metrics"]
    assert all("numeraire_price_usd" not in point for point in payload["equity_curve"])


@pytest.mark.trust_cell("swap:rejection_no_state_change")
def test_swap_overspend_is_rejected_without_state_change() -> None:
    """Selling WETH the portfolio does not hold is a failed trade, zero mutation.

    Post-#2744 contract: the fill is recorded with success=False, its
    execution costs are zeroed (originals stashed as ``*_unapplied``), and
    no balance/cash/position state changes.
    """
    strategy = ScriptedStrategy([SwapDuck(from_token="WETH", to_token="USDC", amount_usd=Decimal("5000"))])
    result = run_backtest(
        strategy,
        flat_series(10),
        hours=6,
        fee_pct=Decimal("0.003"),
        slippage_pct=Decimal("0.001"),
    )

    assert result.success  # the backtest completes; the trade fails
    # Rejected fill is recorded but is NOT a trade: total_trades excludes
    # it; it surfaces as failed_trades (VIB-5083, CodeRabbit).
    assert result.metrics.total_trades == 0
    assert result.metrics.failed_trades == 1
    trade = result.trades[0]
    assert trade.success is False
    assert "insufficient" in trade.metadata.get("failure_reason", "")
    # No value moved and no costs were charged for the rejected fill.
    assert trade.fee_usd == Decimal("0")
    assert trade.slippage_usd == Decimal("0")
    assert trade.gas_cost_usd == Decimal("0")
    assert trade.metadata.get("fee_usd_unapplied") is not None
    assert result.metrics.total_fees_usd == Decimal("0")
    assert result.metrics.total_slippage_usd == Decimal("0")
    assert result.final_capital_usd == INITIAL_CAPITAL
    assert all(point.value_usd == INITIAL_CAPITAL for point in result.equity_curve)


@pytest.mark.trust_cell("swap:cost_accounting")
def test_swap_fee_accounting_is_exact() -> None:
    """N trades x fee model == total fees, exact; equity delta == cost sum.

    Trades: buy $5,000, sell $2,000, buy $1,000 at 0.3% fee + 0.1% slippage
    on flat prices. Expected: fees = 8000 x 0.003 = $24.00, slippage =
    8000 x 0.001 = $8.00, final equity = 10000 - 32 = $9,968 exactly.
    """
    intents = [
        SwapDuck(amount_usd=Decimal("5000")),
        SwapDuck(from_token="WETH", to_token="USDC", amount_usd=Decimal("2000")),
        SwapDuck(amount_usd=Decimal("1000")),
    ]
    result = run_backtest(
        ScriptedStrategy(intents),
        flat_series(12),
        hours=8,
        fee_pct=Decimal("0.003"),
        slippage_pct=Decimal("0.001"),
    )

    assert result.success
    assert result.metrics.total_trades == 3
    assert all(trade.success for trade in result.trades)
    assert result.metrics.total_fees_usd == Decimal("24")
    assert result.metrics.total_slippage_usd == Decimal("8")
    assert result.metrics.total_gas_usd == Decimal("0")
    assert result.final_capital_usd == INITIAL_CAPITAL - Decimal("32")


@pytest.mark.trust_cell("swap:trade_pnl_attribution")
def test_swap_profitable_close_records_positive_pnl() -> None:
    """A profitable closing swap must record positive per-trade pnl_usd.

    Buy 2.5 WETH at $2,000, sell at $2,500 with zero costs: the closing
    trade realizes +$1,250. The spec: pnl attribution ties to the realized
    economics, and win_rate counts this as a win.
    """
    series = {
        "WETH": [Decimal("2000")] * 2 + [Decimal("2500")] * 10,
        "USDC": [Decimal("1")] * 12,
    }
    intents = [
        SwapDuck(amount_usd=Decimal("5000")),  # exec t1 @2000 -> 2.5 WETH
        None,
        SwapDuck(from_token="WETH", to_token="USDC", amount_usd=Decimal("6250")),  # exec t3 @2500
    ]
    result = run_backtest(ScriptedStrategy(intents), series, hours=6)

    assert result.success
    assert result.metrics.total_trades == 2
    closing_trade = result.trades[-1]
    assert closing_trade.success
    # Economic reality: the round trip realized +$1,250 (equity proves it).
    assert result.final_capital_usd == INITIAL_CAPITAL + Decimal("1250")
    # The spec under test: attribution must reflect that realization.
    assert closing_trade.pnl_usd > Decimal("0")
    assert result.metrics.win_rate > Decimal("0")


@pytest.mark.trust_cell("swap:math_sharpe")
def test_sharpe_ratio_matches_closed_form() -> None:
    """Sharpe from the real calculator matches an independent computation.

    Protocol Phase 2.2 reference curve: [10000, 10100, 10200, 10150, 10250].
    """
    equity = [Decimal("10000"), Decimal("10100"), Decimal("10200"), Decimal("10150"), Decimal("10250")]

    returns = calculate_returns(equity)
    assert len(returns) == 4
    assert returns[0] == Decimal("0.01")  # exact: 100 / 10000

    volatility = calculate_volatility(returns, Decimal("365"))
    sharpe = calculate_sharpe_ratio(returns, volatility, Decimal("0"), Decimal("365"))

    # Independent float reference: annualized mean / annualized sample stdev.
    ref_returns = [100 / 10000, 100 / 10100, -50 / 10200, 100 / 10150]
    mean = sum(ref_returns) / 4
    variance = sum((r - mean) ** 2 for r in ref_returns) / 3  # sample variance (n-1)
    ref_volatility = math.sqrt(variance) * math.sqrt(365)
    ref_sharpe = (mean * 365) / ref_volatility

    assert abs(float(volatility) - ref_volatility) < 1e-9
    assert abs(float(sharpe) - ref_sharpe) < 1e-9


@pytest.mark.trust_cell("swap:math_max_drawdown")
def test_max_drawdown_matches_closed_form() -> None:
    """Protocol Phase 2.3: peak 12000 -> trough 9000 == exactly 25%."""
    equity = [
        Decimal("10000"),
        Decimal("11000"),
        Decimal("10500"),
        Decimal("12000"),
        Decimal("9000"),
        Decimal("10000"),
    ]
    assert calculate_max_drawdown(equity) == Decimal("0.25")
    # Degenerate inputs must not fabricate a drawdown.
    assert calculate_max_drawdown([]) == Decimal("0")
    assert calculate_max_drawdown([Decimal("10000")]) == Decimal("0")


# =============================================================================
# LP column
# =============================================================================


@pytest.mark.trust_cell("lp:math_il_closed_form")
def test_il_formula_matches_published_math() -> None:
    """Protocol Phase 2.1: full-range IL matches the V2 closed form.

    IL(k) = 1 - 2*sqrt(k)/(1+k): k=1.5 -> 2.02%, k=2.0 -> 5.72%, and the
    formula is ratio-symmetric (IL(0.5) == IL(2.0)).
    """
    calc = ImpermanentLossCalculator()
    tolerance = Decimal("0.001")  # 0.1% per the protocol acceptance criteria

    il_50_up = calc.calculate_il_for_price_change(Decimal("1.5"), MIN_TICK, MAX_TICK)
    assert abs(il_50_up - Decimal("0.0202")) < tolerance

    il_double = calc.calculate_il_for_price_change(Decimal("2.0"), MIN_TICK, MAX_TICK)
    assert abs(il_double - Decimal("0.05719")) < tolerance

    il_half = calc.calculate_il_for_price_change(Decimal("0.5"), MIN_TICK, MAX_TICK)
    assert abs(il_half - il_double) < Decimal("0.0001")

    assert calc.calculate_il_for_price_change(Decimal("1.0"), MIN_TICK, MAX_TICK) == Decimal("0")


def _lp_adapter_and_portfolio() -> tuple[LPBacktestAdapter, SimulatedPortfolio]:
    """Real LP adapter (zero measured pool volume -> zero fee accrual) + real portfolio."""
    adapter = LPBacktestAdapter(
        config=LPBacktestConfig(
            strategy_type="lp",
            explicit_pool_volume_usd_daily=Decimal("0"),
        )
    )
    portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)
    return adapter, portfolio


def _lp_open_intent() -> LPOpenIntent:
    # $5,000 position at WETH=$2,000: 1.25 WETH + 2,500 USDC, price range 1000-4000.
    return LPOpenIntent(
        pool="WETH/USDC",
        amount0=Decimal("1.25"),
        amount1=Decimal("2500"),
        range_lower=Decimal("1000"),
        range_upper=Decimal("4000"),
        protocol="uniswap_v3",
    )


@pytest.mark.trust_cell("lp:entry_value_neutral")
def test_lp_adapter_open_is_value_neutral() -> None:
    """Opening an LP position must not change equity beyond the gas charge.

    Drives the real LPBacktestAdapter against the real portfolio exactly as
    the engine loop does per tick (execute_intent -> apply_fill ->
    mark_to_market with the adapter). With zero pool volume and flat prices,
    equity at the open instant must equal initial capital minus the fill's
    execution costs - the $5,000 position must be WORTH $5,000.
    """
    adapter, portfolio = _lp_adapter_and_portfolio()
    state = _market_state(0)

    fill = adapter.execute_intent(_lp_open_intent(), portfolio, state)
    assert fill is not None and fill.success
    assert portfolio.apply_fill(fill, market_state=state)

    equity = portfolio.mark_to_market(state, state.timestamp, adapter=adapter)
    expected = INITIAL_CAPITAL - fill.fee_usd - fill.slippage_usd - fill.gas_cost_usd
    # Numeric dust bound only (Decimal sqrt round-trip), never economic tolerance.
    assert abs(equity - expected) <= expected * Decimal("1e-9")


@pytest.mark.trust_cell("lp:round_trip_conservation")
def test_lp_adapter_round_trip_conserves_value() -> None:
    """LP open -> close at flat price returns initial capital minus costs.

    Zero measured pool volume (no fee accrual), flat prices (no IL): the
    close must credit back exactly what the open deposited, so final equity
    == initial - sum of execution costs across both fills.
    """
    adapter, portfolio = _lp_adapter_and_portfolio()

    open_state = _market_state(0)
    open_fill = adapter.execute_intent(_lp_open_intent(), portfolio, open_state)
    assert open_fill is not None and open_fill.success
    assert portfolio.apply_fill(open_fill, market_state=open_state)
    portfolio.mark_to_market(open_state, open_state.timestamp, adapter=adapter)

    # One hour passes at flat prices (real per-tick adapter update).
    tick_state = _market_state(1)
    for position in portfolio.positions:
        adapter.update_position(position, tick_state, float(TICK_SECONDS), tick_state.timestamp)
    portfolio.mark_to_market(tick_state, tick_state.timestamp, adapter=adapter)

    close_state = _market_state(2)
    position_id = portfolio.positions[0].position_id
    close_fill = adapter.execute_intent(
        LPCloseIntent(position_id=position_id, protocol="uniswap_v3"), portfolio, close_state
    )
    assert close_fill is not None and close_fill.success
    assert portfolio.apply_fill(close_fill, market_state=close_state)

    final_equity = portfolio.mark_to_market(close_state, close_state.timestamp, adapter=adapter)
    total_costs = sum((f.fee_usd + f.slippage_usd + f.gas_cost_usd) for f in (open_fill, close_fill))
    expected = INITIAL_CAPITAL - total_costs
    assert len(portfolio.positions) == 0
    assert abs(final_equity - expected) <= expected * Decimal("1e-9")


@pytest.mark.trust_cell("lp:generic_lane_entry")
def test_lp_generic_lane_open_does_not_mint() -> None:
    """Generic-lane (no adapter) LP_OPEN through the engine loop must not mint.

    A $5,000 LP open on a $10,000 portfolio must leave equity at $10,000 at
    the execution tick. Before VIB-5096 the engine stored the USD notional
    in ``position.liquidity`` and the V3 marker valued it as L-units,
    marking at ~$452,000 (~90x).
    """
    result = run_backtest(ScriptedStrategy([LPOpenDuck()]), flat_series(8), hours=4)

    assert result.success
    assert result.metrics.total_trades == 1
    assert result.trades[0].success
    # Equity at the open tick (execution happens one tick after decide).
    assert result.equity_curve[1].value_usd == INITIAL_CAPITAL


@pytest.mark.trust_cell("lp:rejection_no_state_change")
def test_lp_open_beyond_cash_is_rejected() -> None:
    """An LP_OPEN larger than the portfolio is a failed trade, zero mutation."""
    result = run_backtest(
        ScriptedStrategy([LPOpenDuck(amount_usd=Decimal("20000"))]),
        flat_series(8),
        hours=4,
    )

    assert result.success
    # Rejected fill is recorded but is NOT a trade: total_trades excludes
    # it; it surfaces as failed_trades (VIB-5083, CodeRabbit).
    assert result.metrics.total_trades == 0
    assert result.metrics.failed_trades == 1
    trade = result.trades[0]
    assert trade.success is False
    assert "insufficient" in trade.metadata.get("failure_reason", "")
    assert result.final_capital_usd == INITIAL_CAPITAL
    assert all(point.value_usd == INITIAL_CAPITAL for point in result.equity_curve)


# =============================================================================
# lending column
# =============================================================================


@pytest.mark.trust_cell("lending:entry_value_neutral")
def test_supply_is_value_neutral_at_entry() -> None:
    """SUPPLY converts cash 1:1 into the position: equity unchanged at entry."""
    result = run_backtest(ScriptedStrategy([SupplyDuck()]), flat_series(8), hours=4)

    assert result.success
    assert result.metrics.total_trades == 1
    assert result.trades[0].success
    # Supply executes at t1; equity at that mark is exactly initial capital
    # (interest accrual starts the following tick).
    assert result.equity_curve[0].value_usd == INITIAL_CAPITAL
    assert result.equity_curve[1].value_usd == INITIAL_CAPITAL


@pytest.mark.trust_cell("lending:yield_tie_out")
def test_supply_equity_growth_ties_to_interest_accrual() -> None:
    """Equity growth of an open supply == the engine's own interest accrual, exact.

    The engine marks accrue compound interest per tick on the entry
    principal (InterestCalculator, 1/24-day periods). The books must tie:
    final equity == initial + N x the per-hour increment, with N derived
    from the execution and final mark timestamps. This pins the
    "interest is the ONLY equity source for an open supply" invariant.
    """
    apy = Decimal("0.05")
    principal = Decimal("5000")
    result = run_backtest(
        ScriptedStrategy([SupplyDuck(amount_usd=principal, apy=apy)]),
        flat_series(10),
        hours=6,
    )

    assert result.success
    assert result.trades[0].success

    executed_at = result.trades[0].timestamp
    last_mark = result.equity_curve[-1].timestamp
    accrual_hours = int((last_mark - executed_at).total_seconds()) // TICK_SECONDS
    assert accrual_hours == 5  # supply executes at t1, marks t2..t6 accrue

    hourly = (
        InterestCalculator()
        .calculate_interest(
            principal=principal,
            apy=apy,
            time_delta=Decimal(TICK_SECONDS) / Decimal(86400),
            compound=True,
        )
        .interest
    )
    expected = INITIAL_CAPITAL + accrual_hours * hourly

    assert result.final_capital_usd == expected
    # Monotone: interest only ever adds.
    values = [p.value_usd for p in result.equity_curve]
    assert all(b >= a for a, b in zip(values, values[1:], strict=False))


@pytest.mark.trust_cell("lending:round_trip_conservation")
def test_supply_withdraw_round_trip_conserves_value() -> None:
    """SUPPLY then WITHDRAW must return initial capital plus accrued interest.

    Real lending-lane intents through the real engine loop. The withdrawn
    principal comes OUT of the supply position (VIB-5097): the WITHDRAW
    resolves the matched SUPPLY position via position_close_id, credits
    principal + accrued interest, and realizes the interest as PnL.
    """
    intents = [
        SupplyIntent(protocol="aave_v3", token="USDC", amount=Decimal("5000")),
        None,
        WithdrawIntent(protocol="aave_v3", token="USDC", amount=Decimal("5000"), withdraw_all=True),
    ]
    result = run_backtest(
        ScriptedStrategy(intents),
        flat_series(10),
        hours=6,
        strategy_type="lending",
    )

    assert result.success
    assert result.metrics.total_trades == 2
    assert all(trade.success for trade in result.trades)
    # Interest for the few open hours is well under $1; the principal must
    # NOT be double-counted.
    drift = result.final_capital_usd - INITIAL_CAPITAL
    assert Decimal("0") <= drift < Decimal("1")


@pytest.mark.trust_cell("lending:borrow_repay_conservation")
def test_borrow_repay_round_trip_conserves_value() -> None:
    """BORROW opens debt + credits cash; REPAY extinguishes it (VIB-5098).

    Real lending-lane intents through the real engine loop. The borrow must
    be a real $2,000 fill (debt position + cash inflow, equity-neutral), and
    the repay must close the matched BORROW position -- pre-fix the borrow
    was a $0 no-op and the repay burned the $2,000 from cash.
    """
    intents = [
        SupplyIntent(protocol="aave_v3", token="USDC", amount=Decimal("5000")),
        BorrowIntent(
            protocol="aave_v3",
            collateral_token="USDC",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("2000"),
        ),
        None,
        RepayIntent(protocol="aave_v3", token="USDC", amount=Decimal("2000")),
    ]
    result = run_backtest(
        ScriptedStrategy(intents),
        flat_series(10),
        hours=6,
        strategy_type="lending",
    )

    assert result.success
    assert result.metrics.total_trades == 3
    assert all(trade.success for trade in result.trades)
    # The borrow is a real $2,000 fill, not a no-op.
    assert result.trades[1].amount_usd == Decimal("2000")
    # Drift = supply interest earned - borrow interest owed over a few open
    # hours: cents, never principal. Pre-fix this was ~-$2,000.
    drift = result.final_capital_usd - INITIAL_CAPITAL
    assert abs(drift) < Decimal("1")


@pytest.mark.trust_cell("lending:rejection_no_state_change")
def test_supply_beyond_cash_is_rejected() -> None:
    """A SUPPLY larger than available cash is a failed trade, zero mutation."""
    result = run_backtest(
        ScriptedStrategy([SupplyDuck(amount_usd=Decimal("20000"))]),
        flat_series(8),
        hours=4,
    )

    assert result.success
    # Rejected fill is recorded but is NOT a trade: total_trades excludes
    # it; it surfaces as failed_trades (VIB-5083, CodeRabbit).
    assert result.metrics.total_trades == 0
    assert result.metrics.failed_trades == 1
    trade = result.trades[0]
    assert trade.success is False
    assert "insufficient" in trade.metadata.get("failure_reason", "")
    assert result.final_capital_usd == INITIAL_CAPITAL
    assert all(point.value_usd == INITIAL_CAPITAL for point in result.equity_curve)


# =============================================================================
# perp column (v1 beta)
# =============================================================================


@pytest.mark.trust_cell("perp:entry_value_neutral")
def test_perp_open_is_value_neutral_at_entry() -> None:
    """PERP_OPEN moves collateral cash -> position without minting.

    Note: the VIB-5093 defect cited at scoping time (perp adapter reading
    nonexistent ``portfolio.cash_balance``) is no longer present on main
    (fixed by PR #2751); these perp cells assert the real invariants.
    """
    result = run_backtest(ScriptedStrategy([PerpOpenDuck()]), flat_series(8), hours=4)

    assert result.success
    assert result.trades[0].success
    # Open executes at t1: collateral ($1,000) moved from cash into the
    # position, equity unchanged.
    assert result.equity_curve[1].value_usd == INITIAL_CAPITAL


@pytest.mark.trust_cell("perp:round_trip_conservation")
def test_perp_round_trip_closed_form_funding_only() -> None:
    """Perp open -> close at flat price loses exactly the modeled funding.

    Choreography: open decided t0 (executes t1), close decided t2 (executes
    t3). Funding accrues only at marks while the position is open - exactly
    one hour (the t2 mark). Closed form: final == initial - rate x notional,
    with the rate read from the engine's own FundingCalculator default so
    the cell stays in lockstep with the modeled cost.
    """
    notional = Decimal("5000")
    intents = [
        PerpOpenDuck(size_usd=notional, collateral_usd=Decimal("1000")),
        None,
        PerpCloseDuck(),
    ]
    result = run_backtest(ScriptedStrategy(intents), flat_series(10), hours=5)

    assert result.success
    assert result.metrics.total_trades == 2
    assert all(trade.success for trade in result.trades)

    hourly_rate = FundingCalculator().get_funding_rate_for_protocol("gmx")
    expected_funding = hourly_rate * notional  # one funding hour
    assert result.final_capital_usd == INITIAL_CAPITAL - expected_funding
    # The close trade realizes exactly the accumulated funding (price PnL is 0).
    assert result.trades[-1].pnl_usd == -expected_funding


@pytest.mark.trust_cell("perp:rejection_no_state_change")
def test_perp_open_beyond_cash_is_rejected() -> None:
    """A PERP_OPEN whose collateral exceeds cash is rejected, zero mutation."""
    result = run_backtest(
        ScriptedStrategy([PerpOpenDuck(size_usd=Decimal("100000"), collateral_usd=Decimal("20000"))]),
        flat_series(8),
        hours=4,
    )

    assert result.success
    # Rejected fill is recorded but is NOT a trade: total_trades excludes
    # it; it surfaces as failed_trades (VIB-5083, CodeRabbit).
    assert result.metrics.total_trades == 0
    assert result.metrics.failed_trades == 1
    trade = result.trades[0]
    assert trade.success is False
    assert "insufficient" in trade.metadata.get("failure_reason", "")
    assert result.final_capital_usd == INITIAL_CAPITAL
    assert all(point.value_usd == INITIAL_CAPITAL for point in result.equity_curve)


# =============================================================================
# Matrix integrity meta-test
# =============================================================================


def test_every_registered_cell_has_exactly_one_test() -> None:
    """The registry in trust_matrix.py and the tests here stay in lockstep."""
    seen: dict[str, str] = {}
    module = sys.modules[__name__]
    for name, obj in inspect.getmembers(module, inspect.isfunction):
        if not name.startswith("test_"):
            continue
        for mark in getattr(obj, "pytestmark", []):
            if mark.name != "trust_cell":
                continue
            cell_id = mark.args[0]
            assert cell_id in CELLS_BY_ID, f"{name} references unregistered cell {cell_id!r}"
            assert cell_id not in seen, f"cell {cell_id!r} tested by both {seen[cell_id]} and {name}"
            seen[cell_id] = name

    missing = set(CELLS_BY_ID) - set(seen)
    assert not missing, f"registered cells without tests: {sorted(missing)}"

    # Known-bug cells must carry strict xfail; healthy cells must not.
    for cell_id, test_name in seen.items():
        cell = CELLS_BY_ID[cell_id]
        marks = getattr(getattr(module, test_name), "pytestmark", [])
        has_xfail = any(m.name == "xfail" for m in marks)
        if cell.xfail_ticket:
            assert has_xfail, f"{cell_id} declares xfail_ticket but {test_name} has no xfail mark"
            xfail = next(m for m in marks if m.name == "xfail")
            assert xfail.kwargs.get("strict") is True, f"{cell_id} xfail must be strict"
        else:
            assert not has_xfail, f"{test_name} has an xfail mark but {cell_id} declares no ticket"
