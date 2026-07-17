"""VIB-5559: backtest LP-range extraction stays in lockstep with the compilers.

``get_lp_tick_range`` and the connector compilers both dispatch on
``lp_range_is_ticks``; these tests pin that a given range produces identical
ticks whichever typed form (PriceBand/TickBand) carries it, so a backtest can
never re-interpret a range differently from live execution.
"""

from decimal import Decimal

from almanak.framework.backtesting.pnl.calculators import ImpermanentLossCalculator
from almanak.framework.backtesting.pnl.intent_extraction import get_lp_tick_range
from almanak.framework.intents.vocabulary import LPOpenIntent, PriceBand, TickBand

_price_to_tick = ImpermanentLossCalculator().price_to_tick


def _intent(range_spec) -> LPOpenIntent:
    return LPOpenIntent.model_construct(
        pool="WETH/USDC",
        protocol="uniswap_v3",
        amount_usd=Decimal("5000"),
        range_spec=range_spec,
    )


class TestRangeSpecParity:
    def test_price_band_and_equivalent_tick_band_extract_identically(self) -> None:
        lower_price, upper_price = Decimal("1000"), Decimal("4000")
        tick_band = TickBand(lower=_price_to_tick(lower_price), upper=_price_to_tick(upper_price))
        price_band = PriceBand(lower=lower_price, upper=upper_price)

        from_prices = get_lp_tick_range(_intent(price_band), _price_to_tick)
        from_ticks = get_lp_tick_range(_intent(tick_band), _price_to_tick)

        assert from_prices == from_ticks

    def test_discriminator_decides_not_protocol_lists(self) -> None:
        # The same ambiguous positive pair means PRICES under a PriceBand and
        # raw TICKS under a TickBand — the typed form is the only dispatcher.
        pair = (Decimal("1000"), Decimal("4000"))

        as_prices = get_lp_tick_range(_intent(PriceBand(lower=pair[0], upper=pair[1])), _price_to_tick)
        as_ticks = get_lp_tick_range(_intent(TickBand(lower=int(pair[0]), upper=int(pair[1]))), _price_to_tick)

        assert as_ticks == (1000, 4000)
        assert as_prices == (_price_to_tick(pair[0]), _price_to_tick(pair[1]))
        assert as_prices != as_ticks

    def test_slipstream_price_band_is_not_truncated_to_ticks(self) -> None:
        # The VIB-5867 regression shape: a tiny Slipstream price band must
        # convert via price_to_tick, never truncate through int().
        band = PriceBand(lower=Decimal("0.0000120"), upper=Decimal("0.0000180"))
        intent = LPOpenIntent.model_construct(
            pool="USDC/cbBTC",
            protocol="aerodrome_slipstream",
            amount_usd=Decimal("1000"),
            range_spec=band,
        )

        tick_lower, tick_upper = get_lp_tick_range(intent, _price_to_tick)

        assert (tick_lower, tick_upper) == (_price_to_tick(band.lower), _price_to_tick(band.upper))
        assert tick_lower < tick_upper < 0  # deep-negative tick space, not 0/0


class TestDecimalsAsymmetricTickPlane:
    """ALM-2948 backtest half: raw ticks convert through the shared kernel."""

    def test_raw_tick_band_matches_equivalent_price_band_on_18_6_pool(self) -> None:
        from almanak.connectors._strategy_base.concentrated_liquidity_math import (
            price_to_tick as kernel_price_to_tick,
        )

        decimals = (18, 6)  # WETH/USDC
        lower_price, upper_price = Decimal("1000"), Decimal("4000")
        raw_band = TickBand(
            lower=kernel_price_to_tick(lower_price, *decimals),
            upper=kernel_price_to_tick(upper_price, *decimals),
        )
        price_band = PriceBand(lower=lower_price, upper=upper_price)

        from_raw_ticks = get_lp_tick_range(_intent(raw_band), _price_to_tick, decimals=decimals)
        from_prices = get_lp_tick_range(_intent(price_band), _price_to_tick)

        # Same range, one plane: the raw on-chain band (deep-negative ticks on
        # an 18/6 pool) resolves to the SAME human-plane ticks as the price
        # band; pre-fix it passed through verbatim and read out-of-range.
        assert abs(from_raw_ticks[0] - from_prices[0]) <= 1
        assert abs(from_raw_ticks[1] - from_prices[1]) <= 1
        assert raw_band.lower < -200_000  # genuinely the other plane

    def test_unknown_decimals_keeps_the_documented_hatch(self) -> None:
        band = TickBand(lower=-1000, upper=1000)
        assert get_lp_tick_range(_intent(band), _price_to_tick) == (-1000, 1000)

    def test_pair_decimals_resolves_address_native_refs(self) -> None:
        # (chain, address) tuple refs resolve by address on the ref's own
        # chain — str(tuple) is never a resolvable key.
        from almanak.framework.backtesting.pnl.engine import _lp_pair_decimals

        weth = ("ethereum", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
        usdc = ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")

        assert _lp_pair_decimals(weth, usdc, "base") == (18, 6)  # ref chain wins
        assert _lp_pair_decimals("WETH", "USDC", "ethereum") == (18, 6)


class TestAdapterLaneUsesSharedExtractor:
    """The routed LP lane resolves ranges via the same extractor as the
    generic lane — a raw TickBand must not collapse through the price
    converter into a one-tick MIN_TICK range."""

    def test_adapter_open_range_matches_generic_extractor_for_raw_ticks(self) -> None:
        from almanak.connectors._strategy_base.concentrated_liquidity_math import (
            price_to_tick as kernel_price_to_tick,
        )
        from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter
        from almanak.framework.backtesting.pnl.engine import _lp_pair_decimals

        decimals = (18, 6)  # WETH/USDC
        raw_band = TickBand(
            lower=kernel_price_to_tick(Decimal("1000"), *decimals),
            upper=kernel_price_to_tick(Decimal("4000"), *decimals),
        )
        intent = _intent(raw_band)
        adapter = LPBacktestAdapter()

        _, _, tick_lower, tick_upper = adapter._lp_open_range(intent)
        expected = get_lp_tick_range(
            intent, adapter._price_to_tick_int, decimals=_lp_pair_decimals("WETH", "USDC", "ethereum")
        )

        assert (tick_lower, tick_upper) == expected
        assert tick_upper - tick_lower > 1  # not the collapsed MIN_TICK+1 shape
        assert tick_lower > -400_000  # converted into the human plane, not clamped
