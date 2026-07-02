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
from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.pnl.calculators.funding import FundingCalculator
from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    MAX_TICK,
    MIN_TICK,
    ImpermanentLossCalculator,
)
from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.metrics_calculator import (
    calculate_max_drawdown,
    calculate_metrics,
    calculate_returns,
    calculate_sharpe_ratio,
    calculate_volatility,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.backtesting.pnl.position_models import PositionType, SimulatedPosition
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import FundingHistoryPoint
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
    USDC_ARBITRUM,
    FundingCoherenceProbeStrategy,
    FundingGatedPerpStrategy,
    LPOpenDuck,
    PerpCloseDuck,
    PerpOpenDuck,
    ScriptedStrategy,
    StakeDuck,
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


@pytest.mark.trust_cell("swap:unsupported_intent_refused")
def test_unsupported_intent_stops_the_run_without_state_change() -> None:
    """An intent outside the simulated envelope is fatal — never a costed no-op.

    Design decision 2026-07-02: pre-change, the generic lane recorded ANY
    intent type as a trade (fees/gas charged, zero token flows, no position),
    silently diverging the backtest from live behaviour. The engine now stops
    the run with UnsupportedIntentError; the equity that exists is untouched
    and no trade is recorded.
    """
    result = run_backtest(ScriptedStrategy([StakeDuck()]), flat_series(12), hours=9)

    assert not result.success
    assert "Unsupported intent" in str(result.error)
    assert "STAKE" in str(result.error)
    assert result.metrics.total_trades == 0
    assert all(point.value_usd == INITIAL_CAPITAL for point in result.equity_curve)


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


@pytest.mark.trust_cell("swap:gas_native_asset_pricing")
def test_swap_gas_cost_uses_chain_native_asset_price() -> None:
    """Polygon gas prices from MATIC, not tracked ETH/WETH.

    The synthetic provider only receives WETH/USDC as the configured token
    set. The engine must auto-add the Polygon native gas asset to the data
    fetch set, then value gas from that MATIC price. If the legacy ETH/WETH
    lookup returns, this cell overcharges the trade by 3000x and fails.
    """
    result = run_backtest(
        ScriptedStrategy([SwapDuck(amount_usd=Decimal("1000"))]),
        flat_series(8),
        hours=4,
        chain="polygon",
        include_gas_costs=True,
    )

    assert result.success
    assert result.metrics.total_trades == 1
    trade = result.trades[0]
    assert trade.success
    assert trade.gas_price_gwei == Decimal("30")
    # SyntheticPriceProvider assigns $1 to auto-added tokens without a
    # fixture series and supplies 30 gwei in MarketState. SWAP gas = 180,000;
    # 180000 * 30 gwei * $1 / 1e9.
    assert trade.gas_cost_usd == Decimal("0.005400000")
    assert result.metrics.total_gas_usd == Decimal("0.005400000")


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


@pytest.mark.trust_cell("lp:fungible_close_by_pool_id")
def test_lp_close_by_pool_descriptor_id_round_trips() -> None:
    """Fungible-LP LP_CLOSE by pool-descriptor id matches, closes, conserves.

    Aerodrome / Uniswap-V2-style strategies emit LP_CLOSE with a pool-string
    position_id ("WETH/USDC/volatile") -- what the LIVE compiler expects --
    which never equals the engine's synthetic open id
    ("LP_aerodrome_WETH_USDC_<ts>", VIB-2916). Before find_lp_close_position_id
    the LP adapter matched by exact id only, so every such close failed
    ("Position ... not found"), the position never closed, and the strategy
    was mis-simulated as "open and never exit". This drives the real adapter
    through open -> close exactly as the engine loop does and asserts the
    close succeeds, the position count returns to zero, and equity conserves.
    """
    adapter, portfolio = _lp_adapter_and_portfolio()

    open_intent = LPOpenIntent(
        pool="WETH/USDC/volatile",
        amount0=Decimal("1.25"),
        amount1=Decimal("2500"),
        range_lower=Decimal("1000"),
        range_upper=Decimal("4000"),
        protocol="aerodrome",
    )
    open_state = _market_state(0)
    open_fill = adapter.execute_intent(open_intent, portfolio, open_state)
    assert open_fill is not None and open_fill.success
    assert portfolio.apply_fill(open_fill, market_state=open_state)
    portfolio.mark_to_market(open_state, open_state.timestamp, adapter=adapter)

    # The synthetic open id is NOT the pool descriptor the close will carry --
    # this is the id-scheme mismatch the matcher must bridge.
    synthetic_id = portfolio.positions[0].position_id
    assert synthetic_id != "WETH/USDC/volatile"

    # One flat-price hour (real per-tick adapter update): no IL, no fees.
    tick_state = _market_state(1)
    for position in portfolio.positions:
        adapter.update_position(position, tick_state, float(TICK_SECONDS), tick_state.timestamp)
    portfolio.mark_to_market(tick_state, tick_state.timestamp, adapter=adapter)

    close_state = _market_state(2)
    close_intent = LPCloseIntent(
        position_id="WETH/USDC/volatile",
        pool="WETH/USDC/volatile",
        protocol="aerodrome",
    )
    close_fill = adapter.execute_intent(close_intent, portfolio, close_state)
    assert close_fill is not None and close_fill.success
    # The fill must target the matched SYNTHETIC id so apply_fill closes it.
    assert close_fill.position_close_id == synthetic_id
    assert portfolio.apply_fill(close_fill, market_state=close_state)

    final_equity = portfolio.mark_to_market(close_state, close_state.timestamp, adapter=adapter)
    total_costs = sum((f.fee_usd + f.slippage_usd + f.gas_cost_usd) for f in (open_fill, close_fill))
    expected = INITIAL_CAPITAL - total_costs
    assert len(portfolio.positions) == 0
    assert abs(final_equity - expected) <= expected * Decimal("1e-9")


@pytest.mark.trust_cell("lp:fee_reporting_tie_out")
def test_lp_fee_reporting_ties_out_to_per_trade() -> None:
    """Summary LP fee metrics must equal the per-trade fees they aggregate.

    The engine *result* is assembled from ``metrics_calculator.calculate_metrics``
    (``engine._calculate_metrics`` -> ``_engine_helpers.finalize_backtest_result``),
    which before VIB-5079 v1.1 never aggregated ``position.fees_earned`` -- so
    ``total_fees_earned_usd`` / ``fees_by_pool`` stayed at their dataclass defaults
    (``0`` / ``{}``) on EVERY LP backtest, even though per-trade ``fees_earned_usd``
    were correct and credited into equity at close. Surfaced after #2832
    (fungible-LP positions now close instead of accumulating, so the broken
    aggregate is the only place LP fees would show in the summary). A
    reporting/KPI bug only: conservation was always exact.

    Drives a real LP open -> accrue -> close round trip through the real adapter
    and portfolio (explicit volume + TVL make accrual deterministic and
    network-free), then asserts the engine-result metric path:

    1. Fees demonstrably accrued (``total_fees_earned_usd > 0``) -- otherwise the
       tie-out below is vacuous.
    2. ``total_fees_earned_usd`` == the sum of the per-trade ``fees_earned_usd``,
       Decimal-exact.
    3. ``fees_by_pool`` sums to the same total.
    4. The engine-result path agrees with ``SimulatedPortfolio.get_metrics()`` --
       both now share one aggregation helper and cannot drift apart again.
    """
    adapter = LPBacktestAdapter(
        config=LPBacktestConfig(
            strategy_type="lp",
            fee_tracking_enabled=True,
            explicit_pool_volume_usd_daily=Decimal("5000000"),  # $5M daily volume
            explicit_pool_liquidity_usd=Decimal("20000000"),  # $20M TVL
        )
    )
    portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)

    open_state = _market_state(0)
    open_fill = adapter.execute_intent(_lp_open_intent(), portfolio, open_state)
    assert open_fill is not None and open_fill.success
    assert portfolio.apply_fill(open_fill, market_state=open_state)
    portfolio.mark_to_market(open_state, open_state.timestamp, adapter=adapter)

    # One hour of flat-price fee accrual through the real adapter.
    tick_state = _market_state(1)
    for position in portfolio.positions:
        adapter.update_position(position, tick_state, float(TICK_SECONDS), tick_state.timestamp)
    portfolio.mark_to_market(tick_state, tick_state.timestamp, adapter=adapter)

    # Collect fees at close (the default) so the round trip fully realizes them.
    close_state = _market_state(2)
    position_id = portfolio.positions[0].position_id
    close_fill = adapter.execute_intent(
        LPCloseIntent(position_id=position_id, protocol="uniswap_v3"), portfolio, close_state
    )
    assert close_fill is not None and close_fill.success
    assert portfolio.apply_fill(close_fill, market_state=close_state)
    portfolio.mark_to_market(close_state, close_state.timestamp, adapter=adapter)
    assert len(portfolio.positions) == 0  # round trip complete

    config = PnLBacktestConfig(
        start_time=START,
        end_time=START + timedelta(hours=2),
        interval_seconds=TICK_SECONDS,
        token_funding=[
            {
                "symbol": "USDC",
                "address": USDC_ARBITRUM,
                "chain": "arbitrum",
                "amount": str(INITIAL_CAPITAL),
                "amount_type": "token",
            }
        ],
        tokens=["WETH", "USDC"],
    )
    metrics = calculate_metrics(portfolio, portfolio.trades, config)

    per_trade_fees = sum(
        (t.fees_earned_usd for t in portfolio.trades if t.fees_earned_usd is not None),
        Decimal("0"),
    )

    # (1) Fees actually accrued -- without this the tie-out below is vacuous and
    # the cell would have passed against the pre-fix all-zero behaviour.
    assert metrics.total_fees_earned_usd > Decimal("0")
    assert per_trade_fees > Decimal("0")

    # (2) The summary aggregate equals the per-trade fees it sums, Decimal-exact.
    assert metrics.total_fees_earned_usd == per_trade_fees

    # (3) fees_by_pool ties to the same total.
    assert sum(metrics.fees_by_pool.values(), Decimal("0")) == metrics.total_fees_earned_usd

    # (4) Engine-result path agrees with the portfolio-native path (shared helper).
    assert metrics.total_fees_earned_usd == portfolio.get_metrics().total_fees_earned_usd


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


def _accrue_one_day_of_fees(
    pool_tvl_usd: Decimal,
    *,
    volume_usd: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    """Open a ~$5k LP position in a pool of the given TVL, accrue one day of
    fees at flat prices through the REAL adapter, and return
    ``(position_value_used, accrued_fee_usd, effective_fee_tier)``.

    Explicit volume + explicit liquidity make fee accrual a deterministic,
    network-free closed form: ``fees = volume * fee_tier * (value / TVL) * days``
    with HIGH-confidence ``explicit_volume`` (no APR averaging). The fee tier is
    read back from the position the adapter actually built (``position.fee_tier``)
    rather than assumed, so the closed-form check stays exact even if the
    intent's default tier changes.
    """
    adapter = LPBacktestAdapter(
        config=LPBacktestConfig(
            strategy_type="lp",
            fee_tracking_enabled=True,
            explicit_pool_volume_usd_daily=volume_usd,
            explicit_pool_liquidity_usd=pool_tvl_usd,
        )
    )
    portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)

    open_state = _market_state(0)
    fill = adapter.execute_intent(_lp_open_intent(), portfolio, open_state)
    assert fill is not None and fill.success
    assert portfolio.apply_fill(fill, market_state=open_state)
    position = portfolio.positions[0]

    # Flat-price update one tick later. value_position() here (before any
    # accrual, so accumulated_fees == 0) is byte-identical to the
    # position_value _calculate_fee_accrual will divide by: both run the same
    # calculate_il_v3 with the same inputs.
    update_state = _market_state(1)
    position_value = adapter.value_position(position, update_state)
    fees_before = position.accumulated_fees_usd
    adapter.update_position(position, update_state, float(TICK_SECONDS) * 24, update_state.timestamp)
    return position_value, position.accumulated_fees_usd - fees_before, position.fee_tier


#: base_liquidity reference TVL hardcoded in SimulatedPortfolio._simulate_lp_fee_accrual.
_GENERIC_LANE_REF_TVL = Decimal("1000000")


def _accrue_generic_lane_lp_fee(position_value_usd: Decimal, *, fee_tier: Decimal) -> Decimal:
    """Accrue one day of LP fees through the generic/fallback portfolio lane.

    Drives ``SimulatedPortfolio._simulate_lp_fee_accrual`` directly - the path
    ``_mark_lp_position`` takes when no adapter is wired or adapter valuation
    raises. This lane carries its own copy of the liquidity-share model with a
    fixed ``$1M`` reference TVL (``_GENERIC_LANE_REF_TVL``), so it had the same
    10% floor as the adapter path and is fixed in lockstep.
    """
    portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)
    position = SimulatedPosition(
        position_type=PositionType.LP,
        protocol="uniswap_v3",
        tokens=["WETH", "USDC"],
        amounts={"WETH": Decimal("1.25"), "USDC": Decimal("2500")},
        entry_price=Decimal("2000"),
        entry_time=START,
        tick_lower=-887272,
        tick_upper=887272,
        liquidity=Decimal("1"),
        fee_tier=fee_tier,
    )
    # last_updated is None -> elapsed measured from entry_time == one day.
    return portfolio._simulate_lp_fee_accrual(position, position_value_usd, START + timedelta(days=1))


@pytest.mark.trust_cell("lp:fee_share_scaling")
def test_lp_fee_accrual_scales_with_liquidity_share() -> None:
    """LP fee accrual must scale with the REAL share of pool liquidity.

    The removed ``max(Decimal("0.1"), liquidity_share)`` floor (epic VIB-5079;
    blocked the VIB-5130 v1 flag removal) credited any sub-10% position - i.e.
    essentially every realistic position - with 10% of the ENTIRE pool's fee
    revenue, minting value on every LP fee backtest. The SAME floor lived in
    BOTH fee-accrual paths, so this cell pins both:

    - **Adapter lane** (``LPBacktestAdapter._calculate_fee_accrual``):
      1. **Closed form**: accrued fee == volume * fee_tier * (position_value /
         pool_TVL) * days, Decimal-exact. For a ~$5k position in a $20M pool
         that is ~$3.75/day, not the floored ~$1,500/day (~400x overstatement).
      2. **Inverse scaling**: 10x-ing pool TVL divides the fee by exactly 10.
         Under the floor both runs clamp to 10% and the ratio collapses to 1.0 -
         the exact symptom the bug report reproduced by 10x-ing
         ``--pool-liquidity-usd`` and observing identical accrual.
    - **Generic / fallback lane** (``SimulatedPortfolio._simulate_lp_fee_accrual``,
      used by ``_mark_lp_position`` when no adapter is wired or adapter
      valuation raises): closed-form check that a ~$5k position against the
      lane's fixed $1M reference TVL accrues at its true ~0.5% share, not the
      floored 10% (~20x on the volume term).
    """
    volume_usd = Decimal("5000000")  # $5M daily pool volume
    days_elapsed = Decimal("1")
    pool_small = Decimal("20000000")  # $20M TVL -> ~0.025% share for a $5k position
    pool_large = pool_small * 10  # $200M TVL

    value_small, fee_small, fee_tier = _accrue_one_day_of_fees(pool_small, volume_usd=volume_usd)
    value_large, fee_large, _ = _accrue_one_day_of_fees(pool_large, volume_usd=volume_usd)

    # (1) Closed form on the small pool: fees track the TRUE liquidity share.
    # fee_tier is the tier the adapter actually accrued at (read off the
    # position), so this stays exact regardless of the intent's default tier.
    expected_small = volume_usd * fee_tier * (value_small / pool_small) * days_elapsed
    # Numeric dust bound only (Decimal division round-trip), never economic tolerance.
    assert abs(fee_small - expected_small) <= expected_small * Decimal("1e-9")

    # The true share (~0.025%) is far below the old 10% floor, so the floored
    # fee would have been hundreds of times larger. Pin the accrual well under it.
    floored_fee = volume_usd * fee_tier * Decimal("0.1") * days_elapsed
    assert fee_small < floored_fee / 100  # ~$3.75 vs the floored ~$1,500

    # (2) Inverse scaling: identical $5k position, 10x the TVL -> exactly 1/10
    # the fee. The floor would clamp BOTH runs to the 10% share, collapsing
    # this ratio to 1.0 (the bug report's root-cause confirmation).
    assert value_large == value_small  # same intent, same flat prices
    expected_large = fee_small / 10
    assert abs(fee_large - expected_large) <= expected_large * Decimal("1e-9")

    # (3) Generic / fallback lane: _simulate_lp_fee_accrual must scale with the
    # real share too. A $5k position is ~0.5% of the lane's fixed $1M reference
    # TVL - far below the old 10% floor. The lane's 0.3%-tier branch uses
    # volume_multiplier=10 and base_apr=0.25, and averages volume- and APR-based
    # fees, so the no-floor closed form for one day is exact.
    generic_fee_tier = Decimal("0.003")  # 0.3% tier -> multiplier 10, apr 0.25
    pos_value = Decimal("5000")
    generic_fee = _accrue_generic_lane_lp_fee(pos_value, fee_tier=generic_fee_tier)

    real_share = pos_value / _GENERIC_LANE_REF_TVL  # min(1, ...) is a no-op here
    volume_based = pos_value * Decimal("10") * generic_fee_tier * real_share * days_elapsed
    apr_based = pos_value * (Decimal("0.25") / Decimal("365")) * days_elapsed
    generic_expected = (volume_based + apr_based) / Decimal("2")
    assert abs(generic_fee - generic_expected) <= generic_expected * Decimal("1e-9")

    # Under the old floor the share would clamp from ~0.5% to 10% (~20x on the
    # volume term), materially inflating the total. Pin it below the floored value.
    floored_volume = pos_value * Decimal("10") * generic_fee_tier * Decimal("0.1") * days_elapsed
    floored_total = (floored_volume + apr_based) / Decimal("2")
    assert generic_fee < floored_total


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


@pytest.mark.trust_cell("lending:snapshot_price_case_insensitive")
def test_snapshot_price_resolves_strategy_config_casing() -> None:
    """The engine seeds the snapshot with upper-cased symbols, but a strategy
    queries its config casing. The strategy-facing ``market.price()`` must
    resolve case-insensitively.

    Regression: ``create_market_snapshot_from_state`` seeds ``_prices`` from
    ``market_state.available_tokens`` (upper-cased), while a lending strategy
    queries ``market.price(self.supply_token)`` with its config casing (e.g.
    ``"wstETH"``). A case-sensitive lookup missed and fell through to an oracle
    that cannot resolve a non-native token, so the strategy got ``ValueError``
    every tick, executed zero intents, and the run still reported
    ``institutional_compliance=true`` / 100% coverage - a silent false-clean
    lending backtest where the engine HAD the price but never exposed it.
    """
    from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state

    state = MarketState(
        timestamp=START,
        prices={"WSTETH": Decimal("3965.76"), "USDC": Decimal("1")},
        chain="arbitrum",
        block_number=1_000_000,
        gas_price_gwei=Decimal("30"),
    )
    snapshot = create_market_snapshot_from_state(state, chain="arbitrum")

    # Engine seeds upper-cased; the strategy queries its config casing.
    assert snapshot.price("WSTETH") == Decimal("3965.76")  # exact
    assert snapshot.price("wstETH") == Decimal("3965.76")  # strategy config casing
    assert snapshot.price("wsteth") == Decimal("3965.76")  # lower

    # The case-insensitive fallback must NOT turn a genuinely-unknown token
    # into a hit (no oracle is wired on the backtest snapshot, so it raises).
    with pytest.raises(ValueError, match="Cannot determine price"):
        snapshot.price("ARB")


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


@pytest.mark.trust_cell("perp:funding_gated_entry")
def test_perp_funding_gated_strategy_can_enter() -> None:
    """A funding-gated strategy reads a rate from the snapshot and enters.

    Guards the unwired strategy-facing funding lane: the engine used to hand
    decide() a snapshot with no funding_rate_provider, so every
    market.funding_rate(...) read raised and funding-gated perp strategies
    (the gmx_v2_directional_perp entry gate) produced 0-trade backtests over
    any window. The fallback here (0.0002/h) sits below the 0.0005/h entry
    threshold, so a wired lane MUST admit the entry.
    """
    fallback = Decimal("0.0002")
    strategy = FundingGatedPerpStrategy()
    result = run_backtest(
        strategy,
        flat_series(8),
        hours=4,
        strategy_type="perp",
        # Network-free tier: the fixed lane serves funding_fallback_rate with
        # zero gateway traffic.
        data_config=BacktestDataConfig(use_historical_funding=False, funding_fallback_rate=fallback),
    )

    assert result.success
    assert strategy.rates_seen, "decide() never observed a funding rate - snapshot funding lane unwired"
    # The lane serves the ENGINE-CONFIGURED rate (the same knob the perp
    # adapter's historical-fallback lane charges), not a fabricated global.
    assert all(rate == fallback for rate in strategy.rates_seen)
    assert result.metrics.total_trades == 1
    assert result.trades[0].success


@pytest.mark.trust_cell("perp:funding_lane_coherence")
def test_perp_funding_lanes_agree_on_measured_rate(monkeypatch: pytest.MonkeyPatch) -> None:
    """decide()-visible funding and position-accrued funding share one source.

    With historical funding enabled, the strategy-facing snapshot lane and the
    perp adapter's position-evolution lane must resolve the SAME measured
    rate. The adapter used to skip historical fetches inside the engine's
    async task (fallback:async_context), so a strategy could enter on the
    measured rate while its open position accrued the fallback (PR #3153
    review). The gateway seam is stubbed with one measured point per window
    (synthetic data, not a mock of the code under test); the fallback is set
    to a DIFFERENT rate so any lane falling back breaks the closed form.

    Choreography mirrors perp:round_trip_conservation: open decided t0
    (executes t1), close decided t2 (executes t3). Funding applies at the t1
    and t2 marks (the adapter's first update after open charges the first
    elapsed hour) — two one-hour applications, both at the measured rate.
    """
    measured = Decimal("0.0004")

    def _fetch(**kwargs):
        return [FundingHistoryPoint(timestamp=kwargs["end_ts"] - 60, rate_hourly=measured)]

    # Both lanes' import-site bindings of the shared gateway seam.
    monkeypatch.setattr("almanak.framework.backtesting.pnl.providers.funding_rates.fetch_funding_points", _fetch)
    monkeypatch.setattr("almanak.connectors.gmx_v2.backtest_funding.fetch_funding_points", _fetch)

    notional = Decimal("5000")
    strategy = FundingCoherenceProbeStrategy(notional=notional)
    result = run_backtest(
        strategy,
        flat_series(10),
        hours=5,
        strategy_type="perp",
        data_config=BacktestDataConfig(
            use_historical_funding=True,
            funding_fallback_rate=Decimal("0.0007"),
        ),
    )

    assert result.success
    assert result.metrics.total_trades == 2
    assert all(trade.success for trade in result.trades)
    # decide() saw the measured rate on every tick...
    assert strategy.rates_seen, "decide() never observed a funding rate"
    assert all(rate == measured for rate in strategy.rates_seen)
    # ...the position's funding was stamped as measured history, not fallback...
    assert result.data_coverage_metrics.perp_metrics.data_sources == ["historical:gateway"]
    assert result.data_coverage_metrics.perp_metrics.funding_confidence_breakdown["high"] == 1
    # ...and the position accrued exactly that rate for its two funding hours.
    expected_funding = measured * notional * 2
    assert result.final_capital_usd == INITIAL_CAPITAL - expected_funding
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
