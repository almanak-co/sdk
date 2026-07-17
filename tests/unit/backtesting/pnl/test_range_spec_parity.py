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
