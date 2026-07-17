"""LP vocabulary-intent resolution in the PnL backtester's generic lane (VIB-5096).

``LPOpenIntent`` declares its pair as a single ``pool`` string ("WETH/USDC")
and its size as per-leg token amounts -- it carries no token0/token1 or USD
attributes. Before the fix, the generic lane (no adapter):

- resolved the position tokens to ``["UNKNOWN"]`` (never price-tracked,
  equity frozen at the open value for the whole backtest),
- resolved the USD notional to zero (zero-size position, zero fees),
- and, once tokens resolve, the engine-side ``liquidity = amount_usd`` unit
  bug made ``_mark_lp_position`` (which feeds ``position.liquidity`` into
  ``calculate_il_v3`` as V3 liquidity L) mint value at the open tick.

These tests pin the fixed behaviour at each layer, ending with the real
engine driven by a real ``LPOpenIntent`` over a moving synthetic price
series: equity must track price, and the open tick must conserve value
(blueprint 31 section 4).
"""
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl._engine_helpers import _resolve_lp_tokens
from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (


    ImpermanentLossCalculator,
)
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktestConfig,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.intent_extraction import (
    find_lp_close_position_id,
    get_intent_amount_usd,
    get_intent_tokens,
    get_lp_tick_range,
    lp_pool_fee_units,
    lp_pool_tokens,
)
from almanak.framework.backtesting.pnl.position_models import PositionType, SimulatedPosition
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

START = datetime(2024, 1, 1, tzinfo=UTC)


def _lp_open_intent(pool: str = "WETH/USDC", protocol: str = "uniswap_v3") -> LPOpenIntent:
    return LPOpenIntent(
        pool=pool,
        amount0=Decimal("1"),
        amount1=Decimal("3000"),
        range_lower=Decimal("2000"),
        range_upper=Decimal("4000"),
        protocol=protocol,
    )


def _market_state(weth_price: Decimal = Decimal("3000")) -> MarketState:
    return MarketState(
        timestamp=START,
        prices={"WETH": weth_price, "USDC": Decimal("1")},
        chain="arbitrum",
    )


class TestLpPoolTokens:
    """lp_pool_tokens: symbolic pool identifier parsing."""

    def test_parses_symbolic_pair(self):
        assert lp_pool_tokens("WETH/USDC") == ("WETH", "USDC")

    def test_uppercases_and_strips(self):
        assert lp_pool_tokens(" weth / usdc ") == ("WETH", "USDC")

    def test_ignores_fee_tier_suffix(self):
        assert lp_pool_tokens("WETH/USDC/500") == ("WETH", "USDC")

    def test_address_pool_returns_none(self):
        assert lp_pool_tokens("0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640") is None

    def test_bare_symbol_returns_none(self):
        assert lp_pool_tokens("WETH") is None

    def test_empty_segment_returns_none(self):
        assert lp_pool_tokens("WETH/") is None

    def test_non_string_returns_none(self):
        assert lp_pool_tokens(None) is None
        assert lp_pool_tokens(123) is None


class TestLpPoolFeeUnits:
    """lp_pool_fee_units: the declared fee/step segment (ALM-2949 — this
    segment used to be silently discarded end-to-end)."""

    def test_parses_fee_segment(self):
        assert lp_pool_fee_units("WETH/USDC/3000") == 3000

    def test_strips_whitespace(self):
        assert lp_pool_fee_units(" weth / usdc / 500 ") == 500

    def test_pair_without_segment_returns_none(self):
        assert lp_pool_fee_units("WETH/USDC") is None

    def test_empty_segment_returns_none(self):
        assert lp_pool_fee_units("WETH/USDC/") is None

    def test_address_pool_returns_none(self):
        assert lp_pool_fee_units("0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640") is None

    def test_non_numeric_segment_returns_none(self):
        assert lp_pool_fee_units("WETH/USDC/0.3%") is None

    def test_multi_coin_pool_name_is_not_a_fee(self):
        # Curve tri-pool names put a TOKEN in the third segment — it must read
        # as "no declared fee", never as malformed.
        assert lp_pool_fee_units("DAI/USDC/USDT") is None

    def test_numeric_segments_return_raw_undomained(self):
        # Domain validation ((0, 1_000_000) for V3) is the consumer's job so a
        # malformed declaration can FAIL CLOSED instead of reading as absent.
        assert lp_pool_fee_units("WETH/USDC/0") == 0
        assert lp_pool_fee_units("WETH/USDC/1000000") == 1000000

    def test_non_string_returns_none(self):
        assert lp_pool_fee_units(None) is None
        assert lp_pool_fee_units(123) is None


class TestGetIntentTokensForLpIntents:
    """get_intent_tokens: real LPOpenIntent resolves its pair from ``pool``."""

    def test_real_lp_open_intent_resolves_pool_pair(self):
        assert get_intent_tokens(_lp_open_intent()) == ["WETH", "USDC"]

    def test_address_pool_keeps_unknown_sentinel(self):
        intent = _lp_open_intent(pool="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640")
        assert get_intent_tokens(intent) == ["UNKNOWN"]

    def test_explicit_token_attributes_win_over_pool(self):
        class _DuckLpIntent:
            pool = "WETH/USDC"
            token0 = "ARB"
            token1 = "USDT"

        assert get_intent_tokens(_DuckLpIntent()) == ["ARB", "USDT"]

    def test_token_a_token_b_aliases_resolve(self):
        class _DuckLpIntent:
            token_a = "ARB"
            token_b = "USDT"

        assert get_intent_tokens(_DuckLpIntent()) == ["ARB", "USDT"]

    def test_partial_explicit_pair_falls_back_to_pool(self):
        """A lone token0 must not split the pair across resolvers: both
        get_intent_tokens and _resolve_lp_tokens take the pool pair."""

        class _DuckLpIntent:
            pool = "ARB/USDT"
            token0 = "ARB"

        tokens = get_intent_tokens(_DuckLpIntent())
        assert tokens[:2] == ["ARB", "USDT"]
        assert _resolve_lp_tokens(_DuckLpIntent()) == ("ARB", "USDT")


class TestResolveLpTokensFlows:
    """_resolve_lp_tokens: token flows use the pool pair, not WETH/USDC defaults."""

    def test_real_lp_open_intent_parses_pool(self):
        assert _resolve_lp_tokens(_lp_open_intent(pool="ARB/USDT")) == ("ARB", "USDT")

    def test_explicit_attributes_win(self):
        class _DuckLpIntent:
            pool = "ARB/USDT"
            token0 = "weth"
            token1 = "usdc"

        assert _resolve_lp_tokens(_DuckLpIntent()) == ("WETH", "USDC")

    def test_address_pool_keeps_legacy_default(self):
        intent = _lp_open_intent(pool="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640")
        assert _resolve_lp_tokens(intent) == ("WETH", "USDC")


class TestGetIntentAmountUsdForLpIntents:
    """get_intent_amount_usd: LP per-leg amounts are priced into USD."""

    def test_real_lp_open_intent_prices_both_legs(self):
        amount = get_intent_amount_usd(_lp_open_intent(), _market_state())
        assert amount == Decimal("1") * Decimal("3000") + Decimal("3000") * Decimal("1")

    def test_zero_leg_needs_no_price(self):
        intent = LPOpenIntent(
            pool="WETH/RARE",
            amount0=Decimal("2"),
            amount1=Decimal("0"),
            range_lower=Decimal("2000"),
            range_upper=Decimal("4000"),
        )
        # RARE has no price, but its leg is zero -- the WETH leg prices alone.
        assert get_intent_amount_usd(intent, _market_state()) == Decimal("6000")

    def test_missing_leg_price_strict_raises(self):
        intent = _lp_open_intent(pool="ARB/USDT")
        with pytest.raises(ValueError, match="no positive price available for leg token"):
            get_intent_amount_usd(intent, _market_state(), strict_reproducibility=True)

    def test_missing_leg_price_nonstrict_falls_back_to_zero(self):
        intent = _lp_open_intent(pool="ARB/USDT")
        fallbacks: list[str] = []
        amount = get_intent_amount_usd(intent, _market_state(), track_fallback=fallbacks.append)
        assert amount == Decimal("0")
        assert fallbacks == ["default_usd_amount"]

    def test_zero_price_leg_is_unpriceable_strict(self):
        """Zero/negative quotes are bad data, not a $0 valuation."""
        state = MarketState(
            timestamp=START,
            prices={"WETH": Decimal("0"), "USDC": Decimal("1")},
            chain="arbitrum",
        )
        with pytest.raises(ValueError, match="no positive price available for leg token"):
            get_intent_amount_usd(_lp_open_intent(), state, strict_reproducibility=True)

    def test_zero_price_leg_nonstrict_falls_back_to_zero(self):
        state = MarketState(
            timestamp=START,
            prices={"WETH": Decimal("0"), "USDC": Decimal("1")},
            chain="arbitrum",
        )
        fallbacks: list[str] = []
        amount = get_intent_amount_usd(_lp_open_intent(), state, track_fallback=fallbacks.append)
        assert amount == Decimal("0")
        assert fallbacks == ["default_usd_amount"]

    def test_direct_usd_attribute_still_wins(self):
        class _DuckLpIntent:
            amount_usd = Decimal("1234")
            amount0 = Decimal("1")
            amount1 = Decimal("3000")

        assert get_intent_amount_usd(_DuckLpIntent(), _market_state()) == Decimal("1234")


class TestGetLpTickRange:
    """get_lp_tick_range: price bounds map to ticks; explicit ticks win."""

    def test_price_bounds_convert_to_ticks(self):
        calculator = ImpermanentLossCalculator()
        tick_lower, tick_upper = get_lp_tick_range(_lp_open_intent(), calculator.price_to_tick)
        # price = 1.0001^tick must round-trip the declared bounds closely.
        assert Decimal("1.0001") ** tick_lower == pytest.approx(Decimal("2000"), rel=Decimal("1e-4"))
        assert Decimal("1.0001") ** tick_upper == pytest.approx(Decimal("4000"), rel=Decimal("1e-4"))

    def test_explicit_tick_attributes_win(self):
        class _DuckLpIntent:
            tick_lower = -100
            tick_upper = 200

        calculator = ImpermanentLossCalculator()
        assert get_lp_tick_range(_DuckLpIntent(), calculator.price_to_tick) == (-100, 200)

    def test_tick_based_protocol_uses_raw_ticks(self):
        intent = LPOpenIntent(
            pool="WETH/USDC",
            amount0=Decimal("1"),
            amount1=Decimal("3000"),
            range_lower=Decimal("-200"),
            range_upper=Decimal("300"),
            protocol="aerodrome_slipstream",
        )
        calculator = ImpermanentLossCalculator()
        assert get_lp_tick_range(intent, calculator.price_to_tick) == (-200, 300)

    def test_no_bounds_falls_back_to_full_range(self):
        class _BareIntent:
            pass

        calculator = ImpermanentLossCalculator()
        assert get_lp_tick_range(_BareIntent(), calculator.price_to_tick) == (-887272, 887272)


class TestLiquidityForValue:
    """liquidity_for_target_value: the V3 liquidity solve round-trips through calculate_il_v3."""

    @pytest.mark.parametrize(
        ("price", "tick_lower", "tick_upper"),
        [
            (Decimal("3000"), -887272, 887272),  # full range
            (Decimal("3000"), 76012, 82944),  # in range (~2000-4000)
            (Decimal("1500"), 76012, 82944),  # below range: all token0
            (Decimal("5000"), 76012, 82944),  # above range: all token1
        ],
    )
    def test_value_round_trips_through_il_calculator(self, price, tick_lower, tick_upper):
        calculator = ImpermanentLossCalculator()
        target_value = Decimal("6000")

        liquidity = calculator.liquidity_for_target_value(target_value, price, tick_lower, tick_upper)
        assert liquidity > 0

        _, token0_amount, token1_amount = calculator.calculate_il_v3(
            entry_price=price,
            current_price=price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=liquidity,
        )
        assert token0_amount * price + token1_amount == pytest.approx(target_value)

    def test_degenerate_inputs_return_zero(self):
        calculator = ImpermanentLossCalculator()
        assert calculator.liquidity_for_target_value(Decimal("0"), Decimal("3000"), -100, 100) == 0
        assert calculator.liquidity_for_target_value(Decimal("6000"), Decimal("0"), -100, 100) == 0
        assert calculator.liquidity_for_target_value(Decimal("6000"), Decimal("3000"), 100, 100) == 0


class TestLpOpenEntryRatioStrictness:
    """_lp_open_delta: the entry price ratio is never silently fabricated.

    A single-sided open (zero leg) can pass USD sizing without the zero
    leg's quote, but the entry RATIO still needs both prices: strict mode
    must raise rather than synthesize a $1 pool price, and non-strict mode
    must record the hardcoded_price fallback for the compliance report.
    """

    def _delta(self, strict: bool):
        from almanak.framework.backtesting.models import IntentType

        engine = PnLBacktester(
            data_provider=_SyntheticPriceProvider(num_ticks=1, step=Decimal("0")),
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )
        intent = LPOpenIntent(
            pool="WETH/RARE",
            amount0=Decimal("2"),
            amount1=Decimal("0"),
            range_lower=Decimal("2000"),
            range_upper=Decimal("4000"),
        )
        position = engine._create_position_delta(
            intent=intent,
            intent_type=IntentType.LP_OPEN,
            protocol="uniswap_v3",
            tokens=["WETH", "RARE"],
            executed_price=Decimal("3000"),
            timestamp=START,
            market_state=_market_state(),
            strict_reproducibility=strict,
        )
        return engine, position

    def test_strict_mode_raises_on_unpriceable_ratio_leg(self):
        with pytest.raises(ValueError, match="no positive price available"):
            self._delta(strict=True)

    def test_nonstrict_mode_tracks_hardcoded_price_fallback(self):
        engine, position = self._delta(strict=False)
        assert position is not None
        assert position.entry_price == Decimal("3000")  # WETH priced, RARE at the $1 fallback
        assert engine._fallback_usage is not None
        assert engine._fallback_usage.get("hardcoded_price", 0) >= 1


class _SyntheticPriceProvider:
    """WETH moves by ``step`` USD per hourly tick; USDC stays at $1."""

    def __init__(self, num_ticks: int, step: Decimal):
        self.num_ticks = num_ticks
        self.step = step

    async def iterate(self, config: Any):
        for i in range(self.num_ticks):
            yield (
                START + timedelta(hours=i),
                MarketState(
                    timestamp=START + timedelta(hours=i),
                    prices={"WETH": Decimal("3000") + self.step * i, "USDC": Decimal("1")},
                    chain="arbitrum",
                ),
            )


class _LpOnceStrategy:
    """Opens one real LPOpenIntent on the first tick, then holds."""

    def __init__(self):
        self._opened = False

    @property
    def deployment_id(self) -> str:
        return "lp-pool-resolution-test"

    def decide(self, market):
        if self._opened:
            return None
        self._opened = True
        return _lp_open_intent()


async def _run_lp_backtest(num_ticks: int, step: Decimal):
    config = PnLBacktestConfig(
        start_time=START,
        end_time=START + timedelta(hours=num_ticks),
        token_funding=_pnl_token_funding(Decimal("10000")),
        tokens=["WETH", "USDC"],
        preflight_validation=False,
        inclusion_delay_blocks=0,
    )
    backtester = PnLBacktester(
        data_provider=_SyntheticPriceProvider(num_ticks=num_ticks, step=step),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )
    return await backtester.backtest(_LpOnceStrategy(), config)


class TestRealLpOpenIntentEndToEnd:
    """Real engine + real LPOpenIntent over a moving price series (the VIB-5096 repro)."""

    @pytest.mark.asyncio
    async def test_equity_tracks_rising_prices(self):
        result = await _run_lp_backtest(num_ticks=5, step=Decimal("100"))

        assert len(result.trades) == 1
        trade = result.trades[0]
        assert trade.tokens == ["WETH", "USDC"]
        # Executed at the open tick's prices: 1 WETH + 3000 USDC.
        assert trade.amount_usd > Decimal("5900")

        values = [point.value_usd for point in result.equity_curve]
        # The open pays execution costs out of the initial capital.
        open_index = next(i for i, value in enumerate(values) if value != values[0])
        assert values[open_index] < values[0]

        # The regression this guards against: equity frozen after the open.
        # ~1500-5500 of the position is WETH exposure inside the 2000-4000
        # range, so each +100 WETH tick must move equity up by a meaningful
        # fraction of +100, bounded above by full 1-WETH exposure plus fees.
        post_open = values[open_index:]
        assert len(post_open) >= 3
        for previous, current in zip(post_open, post_open[1:], strict=False):
            delta = current - previous
            assert Decimal("25") < delta < Decimal("100")

    @pytest.mark.asyncio
    async def test_equity_tracks_falling_prices(self):
        result = await _run_lp_backtest(num_ticks=5, step=Decimal("-100"))

        values = [point.value_usd for point in result.equity_curve]
        open_index = next(i for i, value in enumerate(values) if value != values[0])

        post_open = values[open_index:]
        assert len(post_open) >= 3
        # Falling into the range, the position's WETH exposure GROWS past the
        # initial 1 WETH (concentrated-liquidity IL mechanics), so per-tick
        # losses may exceed -100; bound only by a sanity cap.
        for previous, current in zip(post_open, post_open[1:], strict=False):
            delta = current - previous
            assert Decimal("-250") < delta < Decimal("-25")

    @pytest.mark.asyncio
    async def test_open_tick_conserves_value(self):
        """Equity delta at the open is execution costs only -- the position
        itself is worth exactly what was paid for it (blueprint 31 section 4)."""
        result = await _run_lp_backtest(num_ticks=2, step=Decimal("0"))

        assert len(result.trades) == 1
        trade = result.trades[0]
        # Gas meters to the operational tank, outside portfolio value.
        costs = trade.fee_usd + trade.slippage_usd

        values = [point.value_usd for point in result.equity_curve]
        final_delta = values[-1] - Decimal("10000")
        assert final_delta == pytest.approx(-costs, abs=Decimal("1e-9"))


def _lp_position(
    token0: str = "WETH",
    token1: str = "USDC",
    protocol: str = "aerodrome",
    entry_time: datetime = START,
) -> SimulatedPosition:
    """An open LP SimulatedPosition with an auto-generated synthetic id."""
    return SimulatedPosition(
        position_type=PositionType.LP,
        protocol=protocol,
        tokens=[token0, token1],
        amounts={token0: Decimal("1"), token1: Decimal("3000")},
        entry_price=Decimal("3000"),
        entry_time=entry_time,
    )


def _lp_close(position_id: str, pool: str | None = None, protocol: str = "aerodrome") -> LPCloseIntent:
    return LPCloseIntent(position_id=position_id, pool=pool, protocol=protocol)


class TestFindLpClosePositionId:
    """find_lp_close_position_id: fungible-LP close matching (sibling of VIB-5097).

    Fungible-LP LP_CLOSE carries a pool-descriptor id ("WETH/USDC/volatile")
    that never equals the synthetic open id ("LP_aerodrome_WETH_USDC_<ts>");
    the matcher resolves it by pair+protocol the way the perp matcher resolves
    venue ids, never minting on a no-match.
    """

    def test_matches_fungible_pool_descriptor_id(self):
        position = _lp_position()
        assert position.position_id != "WETH/USDC/volatile"  # the bug premise
        intent = _lp_close(position_id="WETH/USDC/volatile", pool="WETH/USDC/volatile")
        assert find_lp_close_position_id(intent, [position]) == position.position_id

    def test_matches_via_pool_attribute_when_id_opaque(self):
        # An NFT-style numeric id (no "/") cannot parse to a pair; the pool
        # attribute supplies it.
        position = _lp_position()
        intent = _lp_close(position_id="42", pool="WETH/USDC")
        assert find_lp_close_position_id(intent, [position]) == position.position_id

    def test_exact_id_match_takes_precedence(self):
        position = _lp_position()
        intent = _lp_close(position_id=position.position_id, pool="WETH/USDC/volatile")
        assert find_lp_close_position_id(intent, [position]) == position.position_id

    def test_unordered_pair_matches(self):
        # Position opened with tokens in the reverse order still matches: a
        # pool is identified by its unordered pair.
        position = _lp_position(token0="USDC", token1="WETH")
        intent = _lp_close(position_id="WETH/USDC/volatile", pool="WETH/USDC/volatile")
        assert find_lp_close_position_id(intent, [position]) == position.position_id

    def test_no_open_position_returns_none(self):
        intent = _lp_close(position_id="WETH/USDC/volatile", pool="WETH/USDC/volatile")
        assert find_lp_close_position_id(intent, []) is None

    def test_pair_mismatch_returns_none(self):
        position = _lp_position(token0="WETH", token1="USDC")
        intent = _lp_close(position_id="ARB/USDC/volatile", pool="ARB/USDC/volatile")
        assert find_lp_close_position_id(intent, [position]) is None

    def test_protocol_mismatch_returns_none(self):
        position = _lp_position(protocol="aerodrome")
        intent = _lp_close(position_id="WETH/USDC/volatile", pool="WETH/USDC/volatile", protocol="uniswap_v3")
        assert find_lp_close_position_id(intent, [position]) is None

    def test_ignores_non_lp_positions(self):
        supply = SimulatedPosition(
            position_type=PositionType.SUPPLY,
            protocol="aerodrome",
            tokens=["WETH", "USDC"],
            amounts={"WETH": Decimal("1")},
            entry_price=Decimal("3000"),
            entry_time=START,
        )
        intent = _lp_close(position_id="WETH/USDC/volatile", pool="WETH/USDC/volatile")
        assert find_lp_close_position_id(intent, [supply]) is None

    def test_fifo_oldest_wins_among_matching(self):
        oldest = _lp_position(entry_time=START)
        newest = _lp_position(entry_time=START + timedelta(hours=1))
        intent = _lp_close(position_id="WETH/USDC/volatile", pool="WETH/USDC/volatile")
        # Pass newest-first to prove the result is time-ordered, not list-ordered.
        assert find_lp_close_position_id(intent, [newest, oldest]) == oldest.position_id

    def test_unresolvable_descriptor_returns_none(self):
        # An address-style id with no pool attribute cannot resolve a pair, so
        # the close is rejected rather than matched ambiguously.
        position = _lp_position()
        intent = _lp_close(position_id="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640", pool=None)
        assert find_lp_close_position_id(intent, [position]) is None
