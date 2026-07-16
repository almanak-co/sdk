"""Slipstream price-band range resolution (VIB-5867 / ALM-2901).

Slipstream used to be the only concentrated-liquidity connector that took RAW
TICKS, so every strategy author and every codegen path had to hand-roll the
price->tick conversion the SDK already owns. ALM-2901 is what that produced: a
hand-rolled ``tick = log(price)/log(1.0001)`` with no decimals term, which is
wrong by ``|decimals0 - decimals1| * log_1.0001(10)`` ticks.

These tests pin the fix at the level the bug actually lived at -- the range
resolution -- against the REAL on-chain numbers from the ALM-2901 pool, and
assert that the tick escape hatch and the backtest lane stay in lockstep.

Ground truth (Base, ``uv run almanak ax pool USDC CBBTC --chain base
--protocol aerodrome_slipstream``): pool ``0x4e962bb3...``, token0=USDC(6),
token1=CBBTC(8), tick_spacing=100, live tick **-64744**, human price
**1.543048e-05**.
"""

from __future__ import annotations

import math
from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.cl_range import price_band_to_ticks
from almanak.connectors.aerodrome.compiler import _resolve_slipstream_ticks
from almanak.framework.backtesting.pnl.calculators.impermanent_loss import ImpermanentLossCalculator
from almanak.framework.backtesting.pnl.intent_extraction import _FULL_RANGE_TICKS, get_lp_tick_range
from almanak.framework.intents.compiler_models import CompilationResult
from almanak.framework.intents.vocabulary import (
    LPOpenIntent,
    PriceBand,
    TickBand,
    lp_range_bounds,
    lp_range_is_ticks,
)

# --- ALM-2901 ground truth ---------------------------------------------------
USDC_DECIMALS = 6
CBBTC_DECIMALS = 8
POOL_TICK_SPACING = 100
# The pool's live tick and human price at the time of the report.
ONCHAIN_TICK = -64744
POOL_PRICE = Decimal("1.543067e-05")
# The tick the user's strategy actually computed (strategy.py:352-356).
HAND_ROLLED_TICK = -110797
# |decimals0 - decimals1| * log_1.0001(10) == 2 * 23027
DECIMALS_TICK_ERROR = 46054


def _price_band_intent(lower: Decimal, upper: Decimal) -> LPOpenIntent:
    return LPOpenIntent(
        pool="USDC/CBBTC/100",
        amount0=Decimal("10"),
        amount1=Decimal("0.0001"),
        range_spec=PriceBand(lower=lower, upper=upper),
        protocol="aerodrome_slipstream",
    )


def _resolve(intent: LPOpenIntent, spacing: int = POOL_TICK_SPACING):
    return _resolve_slipstream_ticks(intent, spacing, USDC_DECIMALS, CBBTC_DECIMALS)


class TestPriceBandIsAccepted:
    """The fix: Slipstream takes a price band like every other CL connector."""

    def test_price_band_straddles_the_live_onchain_tick(self):
        # A +/-2% band around the pool's real price must contain the pool's real
        # tick. This is the assertion ALM-2901 failed: the minted range was
        # [-110400, -109900], ~46k ticks away, so it could only be one-sided.
        band = _resolve(_price_band_intent(POOL_PRICE * Decimal("0.98"), POOL_PRICE * Decimal("1.02")))
        assert not isinstance(band, CompilationResult), f"price band rejected: {band}"
        tick_lower, tick_upper = band
        assert tick_lower <= ONCHAIN_TICK < tick_upper, (
            f"band [{tick_lower}, {tick_upper}) must straddle the live tick {ONCHAIN_TICK}"
        )

    def test_bounds_are_aligned_to_tick_spacing(self):
        band = _resolve(_price_band_intent(POOL_PRICE * Decimal("0.9"), POOL_PRICE * Decimal("1.1")))
        assert not isinstance(band, CompilationResult)
        tick_lower, tick_upper = band
        assert tick_lower % POOL_TICK_SPACING == 0
        assert tick_upper % POOL_TICK_SPACING == 0

    def test_decimals_are_actually_applied(self):
        """The regression guard: the decimals term must be load-bearing here.

        Resolving the SAME price band while claiming symmetric decimals must move
        the ticks by exactly the ALM-2901 error. If this assertion ever reads
        "no difference", the decimals term has been dropped again.
        """
        intent = _price_band_intent(POOL_PRICE * Decimal("0.98"), POOL_PRICE * Decimal("1.02"))
        real = _resolve_slipstream_ticks(intent, POOL_TICK_SPACING, USDC_DECIMALS, CBBTC_DECIMALS)
        symmetric = _resolve_slipstream_ticks(intent, POOL_TICK_SPACING, 18, 18)
        assert not isinstance(real, CompilationResult)
        assert not isinstance(symmetric, CompilationResult)
        # 46,054 ticks, rounded to the 100-tick spacing grid.
        assert real[0] - symmetric[0] == pytest.approx(DECIMALS_TICK_ERROR, abs=POOL_TICK_SPACING)

    def test_hand_rolled_formula_reproduces_the_alm_2901_error(self):
        """Pins WHY the demo's hand-rolled math had to go (documentation test).

        The user's formula omitted the decimals term, which is arithmetically
        identical to decimals0 == decimals1 == 18.
        """
        hand_rolled = round(math.log(float(POOL_PRICE)) / math.log(1.0001))
        assert hand_rolled == pytest.approx(HAND_ROLLED_TICK, abs=1)
        assert abs(hand_rolled - ONCHAIN_TICK) == pytest.approx(DECIMALS_TICK_ERROR, abs=2)

    def test_non_positive_price_band_is_rejected_not_full_range(self):
        # The deleted demo helper returned MIN_TICK (a silent full-range open)
        # for price <= 0. The seam must fail closed instead.
        intent = LPOpenIntent.model_construct(
            pool="USDC/CBBTC/100",
            amount0=Decimal("10"),
            amount1=Decimal("0.0001"),
            range_lower=Decimal("-1.5"),
            range_upper=Decimal("2.5"),
            range_spec=PriceBand.model_construct(kind="price", lower=Decimal("-1.5"), upper=Decimal("2.5")),
            protocol="aerodrome_slipstream",
            intent_id="i",
        )
        assert isinstance(_resolve(intent), CompilationResult)

    def test_collapsed_band_is_rejected(self):
        # A band far narrower than one tick_spacing bucket cannot be minted.
        intent = _price_band_intent(POOL_PRICE, POOL_PRICE * Decimal("1.0000001"))
        assert isinstance(_resolve(intent), CompilationResult)


class TestTickBandEscapeHatchStillWorks:
    """Explicit raw ticks remain supported -- this fix is additive."""

    def test_explicit_tick_band_is_taken_literally(self):
        intent = LPOpenIntent(
            pool="USDC/CBBTC/100",
            amount0=Decimal("10"),
            amount1=Decimal("0.0001"),
            range_spec=TickBand(lower=-64800, upper=-64700),
            protocol="aerodrome_slipstream",
        )
        assert _resolve(intent) == (-64800, -64700)

    def test_legacy_negative_tick_bounds_still_resolve_as_ticks(self):
        # Deployed strategies pass bare negative ticks; the legacy bridge keeps
        # honouring them (with a DeprecationWarning) rather than silently
        # reinterpreting them as prices.
        with pytest.warns(DeprecationWarning):
            intent = LPOpenIntent(
                pool="USDC/CBBTC/100",
                amount0=Decimal("10"),
                amount1=Decimal("0.0001"),
                range_lower=Decimal("-64800"),
                range_upper=Decimal("-64700"),
                protocol="aerodrome_slipstream",
            )
        assert _resolve(intent) == (-64800, -64700)

    def test_unaligned_tick_band_is_rejected(self):
        intent = LPOpenIntent(
            pool="USDC/CBBTC/100",
            amount0=Decimal("10"),
            amount1=Decimal("0.0001"),
            range_spec=TickBand(lower=-64799, upper=-64700),
            protocol="aerodrome_slipstream",
        )
        assert isinstance(_resolve(intent), CompilationResult)


class TestAmbiguousLegacyBoundsAreRejected:
    """A bare whole-number pair on a tick-based protocol must never be guessed.

    ``[2000, 4000]`` is an entirely natural WETH/USDC PRICE band and an entirely
    valid TICK range. The Step-1 bridge used to silently read it as ticks, which
    means the meaning of a user's range flipped depending on whether the live
    price happened to be a whole number. Both readings mint real money; the only
    safe answer is to refuse and ask.
    """

    def test_positive_whole_number_bounds_are_rejected(self):
        with pytest.raises(ValueError, match="Ambiguous LP range"):
            LPOpenIntent(
                pool="WETH/USDC/200",
                amount0=Decimal("0.1"),
                amount1=Decimal("250"),
                range_lower=Decimal("2000"),
                range_upper=Decimal("4000"),
                protocol="aerodrome_slipstream",
            )

    def test_rejection_names_both_remedies(self):
        with pytest.raises(ValueError) as exc:
            LPOpenIntent(
                pool="WETH/USDC/200",
                amount0=Decimal("0.1"),
                amount1=Decimal("250"),
                range_lower=Decimal("2000"),
                range_upper=Decimal("4000"),
                protocol="aerodrome_slipstream",
            )
        message = str(exc.value)
        assert "PriceBand" in message and "TickBand" in message

    def test_explicit_price_band_resolves_the_ambiguity(self):
        intent = LPOpenIntent(
            pool="WETH/USDC/200",
            amount0=Decimal("0.1"),
            amount1=Decimal("250"),
            range_spec=PriceBand(lower=Decimal("2000"), upper=Decimal("4000")),
            protocol="aerodrome_slipstream",
        )
        assert lp_range_is_ticks(intent) is False

    def test_explicit_tick_band_resolves_the_ambiguity(self):
        intent = LPOpenIntent(
            pool="WETH/USDC/200",
            amount0=Decimal("0.1"),
            amount1=Decimal("250"),
            range_spec=TickBand(lower=2000, upper=4000),
            protocol="aerodrome_slipstream",
        )
        assert lp_range_is_ticks(intent) is True

    def test_unambiguous_forms_still_pass_unchanged(self):
        # Negative -> ticks (a price can never be negative).
        with pytest.warns(DeprecationWarning):
            ticks = LPOpenIntent(
                pool="USDC/CBBTC/100",
                amount0=Decimal("10"),
                amount1=Decimal("0.0001"),
                range_lower=Decimal("-64800"),
                range_upper=Decimal("-64700"),
                protocol="aerodrome_slipstream",
            )
        assert lp_range_is_ticks(ticks) is True

        # Positive fractional -> prices (a tick is always an integer).
        prices = LPOpenIntent(
            pool="USDC/CBBTC/100",
            amount0=Decimal("10"),
            amount1=Decimal("0.0001"),
            range_lower=Decimal("1.5e-05"),
            range_upper=Decimal("1.6e-05"),
            protocol="aerodrome_slipstream",
        )
        assert lp_range_is_ticks(prices) is False

    def test_price_based_protocols_are_unaffected(self):
        # uniswap_v3 has never been tick-based: whole-number prices are prices.
        intent = LPOpenIntent(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.1"),
            amount1=Decimal("250"),
            range_lower=Decimal("2000"),
            range_upper=Decimal("4000"),
            protocol="uniswap_v3",
        )
        assert lp_range_is_ticks(intent) is False


class TestDiscriminatorIsShared:
    """The compiler and the backtest extractor must agree on ticks-vs-prices.

    Two independent copies of this decision is how backtest/live desync gets
    introduced (design doc §Migration, "Required co-change").
    """

    def test_price_band_reads_as_prices(self):
        assert lp_range_is_ticks(_price_band_intent(POOL_PRICE * Decimal("0.98"), POOL_PRICE * Decimal("1.02"))) is False

    def test_tick_band_reads_as_ticks(self):
        intent = LPOpenIntent(
            pool="USDC/CBBTC/100",
            amount0=Decimal("10"),
            amount1=Decimal("0.0001"),
            range_spec=TickBand(lower=-64800, upper=-64700),
            protocol="aerodrome_slipstream",
        )
        assert lp_range_is_ticks(intent) is True

    def test_backtest_extractor_agrees_with_the_compiler_on_a_price_band(self):
        """The lockstep assertion.

        Before this fix the extractor branched on protocol alone ("every
        Slipstream range is ticks"), so a Slipstream price band of 1.54e-05 was
        truncated by int() to tick 0 -- a range of (0, 0) in the backtest while
        the chain minted around -64744.
        """
        from almanak.framework.backtesting.pnl.intent_extraction import get_lp_tick_range

        intent = _price_band_intent(POOL_PRICE * Decimal("0.98"), POOL_PRICE * Decimal("1.02"))
        compiled = _resolve(intent)
        assert not isinstance(compiled, CompilationResult)

        # The backtest's own converter, given the same decimals the compiler used.
        def price_to_tick(price: Decimal) -> int:
            from almanak.connectors._strategy_base.concentrated_liquidity_math import (
                price_to_tick as core,
            )

            return core(price, decimals0=USDC_DECIMALS, decimals1=CBBTC_DECIMALS)

        bt_lower, bt_upper = get_lp_tick_range(intent, price_to_tick)
        assert bt_lower != 0 and bt_upper != 0, "extractor truncated the price band to tick 0"
        # Same band, modulo the compiler's tick_spacing alignment.
        assert abs(bt_lower - compiled[0]) <= POOL_TICK_SPACING
        assert abs(bt_upper - compiled[1]) <= POOL_TICK_SPACING

    def test_backtest_extractor_uses_ticks_directly_for_a_tick_band(self):
        from almanak.framework.backtesting.pnl.intent_extraction import get_lp_tick_range

        intent = LPOpenIntent(
            pool="USDC/CBBTC/100",
            amount0=Decimal("10"),
            amount1=Decimal("0.0001"),
            range_spec=TickBand(lower=-64800, upper=-64700),
            protocol="aerodrome_slipstream",
        )

        def unreachable(price: Decimal) -> int:  # pragma: no cover - must not be called
            raise AssertionError("tick band must not go through price_to_tick")

        assert get_lp_tick_range(intent, unreachable) == (-64800, -64700)


class TestRangeSpecOnlyIntentsResolveEverywhere:
    """VIB-5867 review round 2: a ``range_spec``-only intent (no legacy fields).

    The fixed Slipstream demo builds its LP_OPEN from a ``PriceBand`` range_spec.
    A validated intent mirrors that onto ``range_lower``/``range_upper``, but a
    ``model_construct`` / duck-typed / deserialized intent — the shapes the
    compiler and the backtest extractor must tolerate — can carry ONLY the
    range_spec. Reading the legacy fields directly then throws (compile side) or
    silently collapses the band to the full V3 range (backtest side): a
    backtest/live desync. Both now resolve bounds through the shared
    ``lp_range_bounds``.
    """

    @staticmethod
    def _spec_only(lower, upper, *, kind="price"):
        """A Slipstream intent whose legacy range_* fields are absent."""
        spec = (
            PriceBand.model_construct(kind="price", lower=Decimal(str(lower)), upper=Decimal(str(upper)))
            if kind == "price"
            else TickBand.model_construct(kind="tick", lower=int(lower), upper=int(upper))
        )
        return LPOpenIntent.model_construct(
            pool="WETH/USDC/200",
            amount0=Decimal("0.1"),
            amount1=Decimal("250"),
            range_lower=None,
            range_upper=None,
            range_spec=spec,
            protocol="aerodrome_slipstream",
            intent_id="spec-only",
        )

    def test_lp_range_bounds_prefers_range_spec_when_legacy_absent(self):
        i = self._spec_only("1000", "12000")
        assert lp_range_bounds(i) == (Decimal("1000"), Decimal("12000"))

    def test_backtest_extractor_does_not_collapse_a_spec_only_band_to_full_range(self):
        # Finding #1: was _FULL_RANGE_TICKS because range_lower/upper were None.
        calc = ImpermanentLossCalculator()
        i = self._spec_only("1000", "12000")
        result = get_lp_tick_range(i, calc.price_to_tick)
        assert result != _FULL_RANGE_TICKS, "spec-only price band collapsed to full range"
        cur = calc.price_to_tick(Decimal("3000"))  # human WETH/USDC ratio
        assert result[0] <= cur < result[1], "backtest band must straddle the current price, not span full range"

    def test_compiler_resolves_a_spec_only_price_band(self):
        # Finding #3: was TypeError/ValueError reading intent.range_lower (None).
        i = self._spec_only("1000", "12000")
        result = _resolve_slipstream_ticks(i, 200, 18, 6)  # WETH(18)/USDC(6)
        assert not isinstance(result, CompilationResult), f"spec-only band rejected: {result}"
        assert result[0] < result[1] < 0  # WETH/USDC ticks are negative

    def test_compiler_resolves_a_spec_only_tick_band(self):
        i = self._spec_only(-207400, -182400, kind="tick")
        result = _resolve_slipstream_ticks(i, 200, 18, 6)
        assert result == (-207400, -182400)

    def test_compiler_fails_cleanly_when_no_range_at_all(self):
        i = LPOpenIntent.model_construct(
            pool="WETH/USDC/200",
            amount0=Decimal("0.1"),
            amount1=Decimal("250"),
            range_lower=None,
            range_upper=None,
            range_spec=None,
            protocol="aerodrome_slipstream",
            intent_id="no-range",
        )
        result = _resolve_slipstream_ticks(i, 200, 18, 6)
        assert isinstance(result, CompilationResult)  # clean FAILED, not a raw TypeError


class TestModelConstructFallbackDoesNotSneakAmbiguousTicks:
    """VIB-5867 review round 2, finding #4: the discriminator's legacy fallback.

    ``lp_range_is_ticks`` and ``_bridge_legacy_range`` MUST agree on the same
    pair. The bridge rejects an ambiguous positive-integer pair; the fallback
    used to return ``True`` (treat as ticks) for it. So a ``model_construct`` /
    duck-typed intent with legacy ``[2000, 4000]`` and no range_spec would be
    silently compiled as RAW TICKS — the ALM-2901 failure class through a side
    door. Both now route through ``_classify_legacy_bounds``; ambiguous is never
    ``True``.
    """

    @staticmethod
    def _legacy_only(lower, upper):
        return LPOpenIntent.model_construct(
            pool="WETH/USDC/200",
            amount0=Decimal("0.1"),
            amount1=Decimal("250"),
            range_lower=Decimal(str(lower)),
            range_upper=Decimal(str(upper)),
            range_spec=None,
            protocol="aerodrome_slipstream",
            intent_id="legacy-only",
        )

    def test_ambiguous_pair_is_not_classified_as_ticks(self):
        assert lp_range_is_ticks(self._legacy_only(2000, 4000)) is False

    def test_ambiguous_pair_is_not_compiled_as_raw_ticks(self):
        # The whole point: it must NOT be taken as ticks [2000, 4000). It routes
        # to the price path instead (where WETH/USDC prices 2000/4000 -> negative
        # ticks), so it can never silently mint the raw-tick position.
        result = _resolve_slipstream_ticks(self._legacy_only(2000, 4000), 200, 18, 6)
        if not isinstance(result, CompilationResult):
            assert result != (2000, 4000), "ambiguous pair was silently compiled as raw ticks 2000/4000"
            assert result[1] < 0, "ambiguous pair must be treated as prices (negative WETH/USDC ticks), not ticks"

    def test_negative_pair_is_still_ticks(self):
        assert lp_range_is_ticks(self._legacy_only(-207400, -182400)) is True

    def test_fractional_pair_is_still_prices(self):
        assert lp_range_is_ticks(self._legacy_only("2000.5", "4000.5")) is False


class TestBacktestLiveEquivalence:
    """VIB-5867 review round 2: prove the desync is CLOSED for the demo's form.

    The team lead asked for proof that a ``range_spec``-only Slipstream intent
    resolves to the SAME position in backtest and live -- specifically NOT
    full-range on one side. The two engines work in different tick SPACES by
    design (live = raw-price / decimals-aware, the chain's space; backtest =
    human-price, its self-consistent value model), so the absolute tick integers
    differ by the constant decimals shift and cannot be bit-identical without a
    backtest money-path rewrite. What must match -- and does -- is the ECONOMIC
    behaviour: both produce a real, finite band that straddles their own current
    price, and neither collapses to full-range.
    """

    def test_spec_only_band_straddles_current_price_in_BOTH_engines(self):
        # WETH(18)/USDC(6), a [1000, 12000] USDC/WETH price band. This is the
        # exact form the Base-fork intent test mints (live tick -201259).
        d0, d1, spacing = 18, 6, 200
        intent = LPOpenIntent(
            pool="WETH/USDC/200",
            amount0=Decimal("0.1"),
            amount1=Decimal("250"),
            range_spec=PriceBand(lower=Decimal("1000"), upper=Decimal("12000")),
            protocol="aerodrome_slipstream",
        )

        # LIVE: cl_range with the pool's real decimals -> raw-price-space ticks.
        live = price_band_to_ticks(
            range_lower=Decimal("1000"),
            range_upper=Decimal("12000"),
            token0_decimals=d0,
            token1_decimals=d1,
            tokens_swapped=False,
            tick_spacing=spacing,
            current_tick=None,
        )
        # The live current tick in raw-price space for a ~3000 USDC/WETH price.
        from almanak.connectors._strategy_base.concentrated_liquidity_math import price_to_tick as _core

        live_cur = _core(Decimal("3000"), decimals0=d0, decimals1=d1)
        assert live.tick_lower <= live_cur < live.tick_upper, "LIVE band must straddle the live-space current tick"

        # BACKTEST: get_lp_tick_range with the IL calc's human-space converter.
        calc = ImpermanentLossCalculator()
        bt_lower, bt_upper = get_lp_tick_range(intent, calc.price_to_tick)
        bt_cur = calc.price_to_tick(Decimal("3000"))
        assert (bt_lower, bt_upper) != _FULL_RANGE_TICKS, "BACKTEST band must NOT be full-range (the #1 desync)"
        assert bt_lower <= bt_cur < bt_upper, "BACKTEST band must straddle the human-space current tick"

        # The equivalence that matters: same in-range verdict, same ordering,
        # neither full-range. Absolute ticks differ by the decimals shift (spaces
        # differ by design) -- assert they are genuinely different spaces so a
        # future "unify the integers" change can't silently pass this as-is.
        assert live.tick_lower < 0 and bt_lower > 0, (
            "expected live (raw-price, negative) and backtest (human-price, positive) to be different spaces"
        )
