"""LP position conservation invariants (VIB-5096).

Characterization tests for the LP liquidity-units bug: ``SimulatedPosition.
liquidity`` is consumed as Uniswap V3 L-units by every valuation path
(``calculate_il_v3`` token-amount math), but both flow producers stored the
USD notional in it — the generic engine lane (``engine._lp_open_delta``) and
the adapter lane (``lp_adapter._execute_lp_open``). A $5,000 LP open on a
$10,000 portfolio marked the position at ~$452K (generic lane) / ~$136K
(adapter lane), and LP_CLOSE realized the phantom into cash.

Invariants pinned here (blueprint 31 section 4.3; Trust Matrix cells
``lp:entry_value_neutral``, ``lp:round_trip_conservation``,
``lp:generic_lane_entry`` in tests/validation/backtesting):

- Entry value-neutrality: equity after LP_OPEN == initial equity minus the
  fill's execution costs, in BOTH lanes.
- Round-trip conservation: OPEN then CLOSE at unchanged price returns the
  initial capital minus exactly the execution costs, in BOTH lanes.
- Post-move valuation: value after a price move follows the standard V3 LP
  math (full-range 50% move -> ~2.02% IL versus hold).

All tests drive the REAL engine/adapter/portfolio code over synthetic
prices — no network, no mocks of the code under test.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.adapters.lp_adapter import (
    LPBacktestAdapter,
    LPBacktestConfig,
)
from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    MAX_TICK,
    MIN_TICK,
    ImpermanentLossCalculator,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    HistoricalDataConfig,
    MarketState,
)
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio, SimulatedPosition
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

START = datetime(2024, 1, 1, tzinfo=UTC)
TICK_SECONDS = 3600
INITIAL_CAPITAL = Decimal("10000")
WETH_PRICE = Decimal("2000")
DEPOSIT_USD = Decimal("5000")

#: Numeric dust bound for Decimal round-trips in the V3 math; never an
#: economic tolerance (blueprint 31 section 4.2).
DUST = Decimal("1e-9")


def _market_state(hour: int, weth: Decimal = WETH_PRICE) -> MarketState:
    return MarketState(
        timestamp=START + timedelta(hours=hour),
        prices={"WETH": weth, "USDC": Decimal("1")},
        chain="arbitrum",
        block_number=1_000_000 + hour,
        gas_price_gwei=Decimal("30"),
    )


# =============================================================================
# Adapter lane (LPBacktestAdapter driven exactly as the engine loop does)
# =============================================================================


def _adapter_and_portfolio() -> tuple[LPBacktestAdapter, SimulatedPortfolio]:
    """Real LP adapter (zero measured pool volume -> zero fee accrual) + real portfolio."""
    adapter = LPBacktestAdapter(
        config=LPBacktestConfig(
            strategy_type="lp",
            explicit_pool_volume_usd_daily=Decimal("0"),
        )
    )
    portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)
    return adapter, portfolio


def _lp_open_intent(
    range_lower: Decimal = Decimal("1000"),
    range_upper: Decimal = Decimal("4000"),
) -> LPOpenIntent:
    # $5,000 position at WETH=$2,000: 1.25 WETH + 2,500 USDC.
    return LPOpenIntent(
        pool="WETH/USDC",
        amount0=Decimal("1.25"),
        amount1=Decimal("2500"),
        range_lower=range_lower,
        range_upper=range_upper,
        protocol="uniswap_v3",
    )


class TestAdapterLaneConservation:
    def test_open_is_value_neutral(self) -> None:
        """Equity after LP_OPEN == initial equity minus execution costs.

        On the buggy producer (liquidity = USD notional) the $5,000 position
        marked at ~$135,960 and equity jumped ~13.6x on open.
        """
        adapter, portfolio = _adapter_and_portfolio()
        state = _market_state(0)

        fill = adapter.execute_intent(_lp_open_intent(), portfolio, state)
        assert fill is not None and fill.success
        assert portfolio.apply_fill(fill, market_state=state)

        equity = portfolio.mark_to_market(state, state.timestamp, adapter=adapter)
        expected = INITIAL_CAPITAL - fill.fee_usd - fill.slippage_usd - fill.gas_cost_usd
        assert abs(equity - expected) <= expected * DUST

    def test_position_marks_at_deposit_value(self) -> None:
        """The $5,000 deposit must be WORTH $5,000 at the open instant."""
        adapter, portfolio = _adapter_and_portfolio()
        state = _market_state(0)

        fill = adapter.execute_intent(_lp_open_intent(), portfolio, state)
        assert fill is not None and fill.success
        assert portfolio.apply_fill(fill, market_state=state)

        value = adapter.value_position(portfolio.positions[0], state, state.timestamp)
        assert abs(value - DEPOSIT_USD) <= DEPOSIT_USD * DUST

    def test_liquidity_is_l_units_not_usd_notional(self) -> None:
        """liquidity * unit_position_value(entry) must equal the deposit.

        This is the producer contract: ``SimulatedPosition.liquidity`` holds
        true V3 L-units, so the per-unit value at the entry price times L
        recovers the deposited token1 value.
        """
        adapter, portfolio = _adapter_and_portfolio()
        state = _market_state(0)

        fill = adapter.execute_intent(_lp_open_intent(), portfolio, state)
        assert fill is not None and fill.success
        position = fill.position_delta
        assert position is not None

        calc = ImpermanentLossCalculator()
        unit_value = calc.unit_position_value(
            price=position.entry_price,
            tick_lower=position.tick_lower,
            tick_upper=position.tick_upper,
        )
        implied_value = position.liquidity * unit_value
        assert abs(implied_value - DEPOSIT_USD) <= DEPOSIT_USD * DUST

    def test_round_trip_at_flat_price_conserves_value(self) -> None:
        """OPEN -> tick -> CLOSE at unchanged price returns initial - costs.

        On the buggy producer the close credited ~$135,960 of tokens for a
        $5,000 deposit, realizing the phantom value into cash.
        """
        adapter, portfolio = _adapter_and_portfolio()

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
        assert abs(final_equity - expected) <= expected * DUST

    def test_value_after_price_move_matches_v3_closed_form(self) -> None:
        """Full-range LP value after a 50% move follows the V3 closed form.

        For a full-range position, value scales with sqrt(price ratio):
        value = deposit * sqrt(1.5), and the implied IL versus holding the
        entry amounts is 1 - 2*sqrt(1.5)/2.5 = ~2.0204%.
        """
        adapter, portfolio = _adapter_and_portfolio()
        state = _market_state(0)

        # Price bounds far beyond the V3 tick domain clamp to MIN/MAX tick.
        fill = adapter.execute_intent(
            _lp_open_intent(range_lower=Decimal("1E-39"), range_upper=Decimal("1E+39")),
            portfolio,
            state,
        )
        assert fill is not None and fill.success
        position = fill.position_delta
        assert position is not None
        assert (position.tick_lower, position.tick_upper) == (MIN_TICK, MAX_TICK)
        assert portfolio.apply_fill(fill, market_state=state)

        moved_state = _market_state(1, weth=WETH_PRICE * Decimal("1.5"))
        value = adapter.value_position(portfolio.positions[0], moved_state, moved_state.timestamp)

        sqrt_ratio = Decimal("1.5").sqrt()
        expected_value = DEPOSIT_USD * sqrt_ratio
        assert abs(value - expected_value) <= expected_value * Decimal("1e-6")

        # Hold value: the deposited 1.25 WETH + 2,500 USDC at the new price.
        hold_value = Decimal("1.25") * WETH_PRICE * Decimal("1.5") + Decimal("2500")
        implied_il = (hold_value - value) / hold_value
        closed_form_il = Decimal("1") - Decimal("2") * sqrt_ratio / Decimal("2.5")
        assert abs(implied_il - closed_form_il) <= Decimal("0.0001")

    def test_fee_share_uses_usd_value_not_l_units(self) -> None:
        """The liquidity-share heuristic divides USD by USD (VIB-5096).

        With pool TVL $10,000 and a $5,000 position, the share is 0.5 and
        explicit volume of $10,000/day at the 0.3% tier over one day yields
        exactly 10000 * 0.003 * 0.5 = $15. Feeding L-units (~55.9) into the
        share would clamp it to the 0.1 floor and yield $3.
        """
        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(
                strategy_type="lp",
                explicit_pool_volume_usd_daily=Decimal("10000"),
                explicit_pool_liquidity_usd=Decimal("10000"),
            )
        )
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)
        state = _market_state(0)

        fill = adapter.execute_intent(_lp_open_intent(), portfolio, state)
        assert fill is not None and fill.success
        assert portfolio.apply_fill(fill, market_state=state)

        day_state = _market_state(24)
        position = portfolio.positions[0]
        adapter.update_position(position, day_state, 86400.0, day_state.timestamp)

        assert abs(position.fees_earned - Decimal("15")) <= Decimal("15") * DUST


# =============================================================================
# Generic engine lane (no adapter: the default execution path)
# =============================================================================


class SyntheticPriceProvider:
    """Deterministic, network-free HistoricalDataProvider."""

    def __init__(self, price_series: dict[str, list[Decimal]]) -> None:
        self._series = {token.upper(): list(series) for token, series in price_series.items()}

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        current = config.start_time
        index = 0
        while current <= config.end_time:
            prices: dict[str, Decimal] = {}
            for token in config.tokens:
                series = self._series.get(token.upper())
                prices[token.upper()] = series[min(index, len(series) - 1)] if series else Decimal("1")
            yield (
                current,
                MarketState(
                    timestamp=current,
                    prices=prices,
                    chain=config.chains[0] if config.chains else "arbitrum",
                    block_number=1_000_000 + index,
                    gas_price_gwei=Decimal("30"),
                ),
            )
            index += 1
            current += timedelta(seconds=config.interval_seconds)

    @property
    def provider_name(self) -> str:
        return "lp-conservation-synthetic"

    @property
    def supported_tokens(self) -> list[str]:
        return list(self._series.keys())

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum"]

    @property
    def min_timestamp(self) -> datetime:
        return START

    @property
    def max_timestamp(self) -> datetime:
        n_points = max((len(s) for s in self._series.values()), default=1)
        return START + timedelta(seconds=(n_points - 1) * TICK_SECONDS)


class ScriptedStrategy:
    """Returns a fixed intent sequence, one per decide() call."""

    def __init__(self, intents: list[Any]) -> None:
        self._intents = list(intents)
        self._cursor = 0
        self.deployment_id = "lp-conservation"

    def decide(self, market: Any) -> Any:
        if self._cursor < len(self._intents):
            intent = self._intents[self._cursor]
            self._cursor += 1
            return intent
        return None


@dataclass
class LPOpenDuck:
    intent_type: str = "LP_OPEN"
    token0: str = "WETH"
    token1: str = "USDC"
    amount_usd: Decimal = DEPOSIT_USD
    protocol: str = "uniswap_v3"
    tick_lower: int = MIN_TICK
    tick_upper: int = MAX_TICK
    fee_tier: Decimal = Decimal("0.003")


@dataclass
class LPCloseDuck:
    position_id: str
    intent_type: str = "LP_CLOSE"
    token0: str = "WETH"
    token1: str = "USDC"
    amount_usd: Decimal = DEPOSIT_USD
    protocol: str = "uniswap_v3"


def _flat_series(n_ticks: int) -> dict[str, list[Decimal]]:
    return {"WETH": [WETH_PRICE] * n_ticks, "USDC": [Decimal("1")] * n_ticks}


def _run_backtest(strategy: Any, price_series: dict[str, list[Decimal]], hours: int) -> Any:
    """Run the REAL PnL engine loop over synthetic data (zero fees/slippage/gas)."""
    config = PnLBacktestConfig(
        start_time=START,
        end_time=START + timedelta(hours=hours),
        interval_seconds=TICK_SECONDS,
        initial_capital_usd=INITIAL_CAPITAL,
        tokens=sorted(price_series),
        include_gas_costs=False,
        inclusion_delay_blocks=0,
    )
    backtester = PnLBacktester(
        data_provider=SyntheticPriceProvider(price_series),
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )
    return asyncio.run(backtester.backtest(strategy, config))


def _expected_position_id(open_hour: int) -> str:
    """The deterministic id engine-created LP positions get (position_models)."""
    entry_time = START + timedelta(hours=open_hour)
    return f"LP_uniswap_v3_WETH_USDC_{entry_time.timestamp():.0f}"


class TestGenericLaneConservation:
    def test_open_holds_equity_at_initial_capital(self) -> None:
        """A $5,000 LP open on a $10,000 portfolio leaves equity at $10,000.

        On the buggy producer (liquidity = USD notional) the open tick marked
        equity at ~$452,000 (~90x mint). Decimal-exact, matching the
        ``lp:generic_lane_entry`` Trust Matrix cell.
        """
        result = _run_backtest(ScriptedStrategy([LPOpenDuck()]), _flat_series(8), hours=4)

        assert result.success
        assert result.metrics.total_trades == 1
        assert result.trades[0].success
        # Execution happens one tick after decide; equity at the open tick.
        assert result.equity_curve[1].value_usd == INITIAL_CAPITAL

    def test_round_trip_at_flat_price_conserves_capital(self) -> None:
        """OPEN then CLOSE at unchanged price keeps every equity point at $10,000.

        Zero fees/slippage/gas: nothing may be minted or destroyed at any
        tick — during the open window (the marks), at the close credit, or
        after.
        """
        # decide() at tick 0 queues the open (executes tick 1); decide() at
        # tick 1 queues the close (executes tick 2).
        strategy = ScriptedStrategy(
            [
                LPOpenDuck(),
                LPCloseDuck(position_id=_expected_position_id(open_hour=1)),
            ]
        )
        result = _run_backtest(strategy, _flat_series(8), hours=4)

        assert result.success
        assert result.metrics.total_trades == 2
        assert all(trade.success for trade in result.trades)
        assert all(point.value_usd == INITIAL_CAPITAL for point in result.equity_curve)


# =============================================================================
# Producer/valuer contract: liquidity_for_target_value
# =============================================================================


class TestLiquidityForTargetValue:
    @pytest.mark.parametrize(
        ("price", "tick_lower", "tick_upper"),
        [
            (Decimal("2000"), MIN_TICK, MAX_TICK),  # full range, in range
            (Decimal("2000"), 69081, 82944),  # ~1000-4000, in range
            (Decimal("500"), 69081, 82944),  # below range (all token0)
            (Decimal("9000"), 69081, 82944),  # above range (all token1)
            (Decimal("0.0005"), -887272, -69081),  # sub-$1 prices
        ],
    )
    def test_round_trips_through_v3_amount_math(self, price: Decimal, tick_lower: int, tick_upper: int) -> None:
        """L from a deposit value must value back to the deposit at entry."""
        calc = ImpermanentLossCalculator()
        target = Decimal("5000")

        liquidity = calc.liquidity_for_target_value(target, price, tick_lower, tick_upper)
        assert liquidity > 0

        _, token0, token1 = calc.calculate_il_v3(
            entry_price=price,
            current_price=price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=liquidity,
        )
        value = token0 * price + token1
        assert abs(value - target) <= target * DUST

    def test_degenerate_inputs_return_zero(self) -> None:
        calc = ImpermanentLossCalculator()
        assert calc.liquidity_for_target_value(Decimal("0"), Decimal("2000"), -100, 100) == 0
        assert calc.liquidity_for_target_value(Decimal("-1"), Decimal("2000"), -100, 100) == 0
        assert calc.liquidity_for_target_value(Decimal("5000"), Decimal("0"), -100, 100) == 0
        assert calc.liquidity_for_target_value(Decimal("5000"), Decimal("2000"), 100, 100) == 0
        assert calc.liquidity_for_target_value(Decimal("5000"), Decimal("2000"), 100, -100) == 0


def _producer_backtester() -> PnLBacktester:
    return PnLBacktester(
        data_provider=SyntheticPriceProvider({"WETH": [WETH_PRICE], "USDC": [Decimal("1")]}),
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )


class TestGeminiReviewRegressions:
    """Pins for the three Gemini findings on PR #2759."""

    def test_entry_value_prices_token1_leg_in_usd(self) -> None:
        """Unrealized PnL is zero at unchanged prices for a non-stable token1 pool.

        entry_price is the token0/token1 ratio, so the entry composition is
        token1-denominated and needs ONE conversion to USD. The pre-fix code
        added a token1-unit term to a USD term, fabricating ~+$2,499.95 of
        unrealized PnL on this exact fixture.
        """
        weth, wbtc = Decimal("2000"), Decimal("50000")
        position = SimulatedPosition.lp(
            token0="WETH",
            token1="WBTC",
            amount0=Decimal("1.25"),
            amount1=Decimal("0.05"),
            liquidity=Decimal("1"),
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
            fee_tier=Decimal("0.003"),
            entry_price=weth / wbtc,
            entry_time=START,
        )
        position.metadata["entry_amounts"] = {"WETH": "1.25", "WBTC": "0.05"}
        market_state = MarketState(
            timestamp=START,
            prices={"WETH": weth, "WBTC": wbtc},
            chain="arbitrum",
            block_number=1_000_000,
            gas_price_gwei=Decimal("30"),
        )
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CAPITAL)

        pnl = portfolio._calculate_lp_unrealized_pnl(position, market_state)

        assert abs(pnl) <= DUST * INITIAL_CAPITAL

    def test_generic_lane_degenerate_tick_range_widens_like_adapter(self) -> None:
        """tick_upper <= tick_lower widens by one tick (adapter-lane parity)."""
        duck = LPOpenDuck()
        duck.tick_lower = 1000
        duck.tick_upper = 1000

        position = _producer_backtester()._lp_open_delta(
            duck, "uniswap_v3", ["WETH", "USDC"], WETH_PRICE, START, _market_state(0), False
        )

        assert position.tick_lower == 1000
        assert position.tick_upper == 1001
        assert position.liquidity > Decimal("0")

    def test_generic_lane_non_positive_price_falls_back_instead_of_crashing(self) -> None:
        """A zero price from bad data must not raise ZeroDivisionError."""
        market_state = MarketState(
            timestamp=START,
            prices={"WETH": Decimal("0"), "USDC": Decimal("1")},
            chain="arbitrum",
            block_number=1_000_000,
            gas_price_gwei=Decimal("30"),
        )

        position = _producer_backtester()._lp_open_delta(
            LPOpenDuck(), "uniswap_v3", ["WETH", "USDC"], WETH_PRICE, START, market_state, False
        )

        assert position.liquidity > Decimal("0")


class TestAdapterLaneBranchCoverage:
    """Branch pins for _execute_lp_open's guard paths (CRAP gate: cc=26 needs
    the error/fallback branches exercised, not just the happy path)."""

    def test_wrong_intent_type_returns_failed_fill(self) -> None:
        adapter, portfolio = _adapter_and_portfolio()
        close_intent = LPCloseIntent(position_id="nope", protocol="uniswap_v3")

        # Call the LP_OPEN executor directly: the adapter's dispatcher would
        # route an LPCloseIntent elsewhere, but the isinstance guard is the
        # executor's own contract.
        fill = adapter._execute_lp_open(close_intent, portfolio, _market_state(0))

        assert fill.success is False
        assert fill.metadata["failure_reason"] == "Invalid intent type"

    def test_missing_prices_fall_back_to_one_dollar(self) -> None:
        adapter, portfolio = _adapter_and_portfolio()
        empty_market = MarketState(
            timestamp=START,
            prices={},
            chain="arbitrum",
            block_number=1_000_000,
            gas_price_gwei=Decimal("30"),
        )

        fill = adapter.execute_intent(_lp_open_intent(), portfolio, empty_market)

        # Both legs price at $1: 1.25 + 2500 in token units.
        assert fill.success is True
        assert fill.amount_usd == Decimal("1.25") + Decimal("2500")

    def test_degenerate_range_widens_by_one_tick(self) -> None:
        adapter, portfolio = _adapter_and_portfolio()

        fill = adapter.execute_intent(
            # Bounds must differ (pydantic validation) but floor to the same
            # tick: log(2000.1/2000)/log(1.0001) < 1 tick apart.
            _lp_open_intent(range_lower=Decimal("2000"), range_upper=Decimal("2000.1")),
            portfolio,
            _market_state(0),
        )

        assert fill.success is True
        position = fill.position_delta
        assert position.tick_upper == position.tick_lower + 1
        assert position.liquidity > Decimal("0")

    @pytest.mark.parametrize(
        ("protocol", "expected_fee_tier"),
        [
            ("uniswap_v3_0.01", Decimal("0.0001")),
            ("uniswap_v3_5bps", Decimal("0.0005")),
            ("uniswap_v3_1", Decimal("0.01")),
            ("uniswap_v3_30bps", Decimal("0.003")),
            ("uniswap_v3", Decimal("0.003")),
        ],
    )
    def test_fee_tier_parsed_from_protocol_suffix(self, protocol: str, expected_fee_tier: Decimal) -> None:
        adapter, portfolio = _adapter_and_portfolio()
        intent = LPOpenIntent(
            pool="WETH/USDC",
            amount0=Decimal("1.25"),
            amount1=Decimal("2500"),
            range_lower=Decimal("1000"),
            range_upper=Decimal("4000"),
            protocol=protocol,
        )

        fill = adapter.execute_intent(intent, portfolio, _market_state(0))

        assert fill.success is True
        assert fill.position_delta.fee_tier == expected_fee_tier
