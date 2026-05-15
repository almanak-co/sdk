"""VIB-4439 / MorphoMay15 §6.1 (F1) — PriceAggregator must fail closed when
exactly two sources disagree by more than the outlier threshold but less
than the magnitude_outlier_ratio.

Reproduces the wstETH-price corruption observed in the Morpho looping
fixture on Ethereum:

  • Source A (Chainlink direct WSTETH/USD):  ~$3500  (correct)
  • Source B (DexScreener wstETH/USD):       $97.31  (broken pool)

With these 2 inputs the current aggregator computes
``median([97.31, 3500]) = mean(...) = $1798.65`` and returns it silently.
The magnitude outlier ratio is 3500 / 97.31 ≈ 36×, **under** the 100×
fail-closed threshold; the 2% outlier-deviation threshold flags both
prices as outliers, code falls through to the "all flagged → use all"
path, and the midpoint ships downstream — silently corrupting every PnL
calculation that consumes the price.

With 3+ sources the median is robust (one bad apple cannot move it past
the outlier threshold). With exactly 2 sources, "median" = mean = the
worst possible answer when one source is wrong. The fix shape is
caller-controlled hardening: ``two_source_divergence_policy=fail_closed``
(default after VIB-4439) raises ``AllDataSourcesFailed`` instead of
returning the midpoint. Existing 1-source and 3+-source paths are
unchanged.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)
from almanak.gateway.data.price.aggregator import (
    AllDataSourcesFailed,
    PriceAggregator,
)


class _MockPriceSource(BasePriceSource):
    """Local copy of the mock used by ``test_price_aggregator_ceiling`` so
    this test file is self-contained on import order."""

    def __init__(self, name: str, price: Decimal | None = None, error: str | None = None) -> None:
        self._name = name
        self._price = price
        self._error = error

    @property
    def source_name(self) -> str:
        return self._name

    async def get_price(self, token: str, quote: str = "USD", **kwargs) -> PriceResult:
        if self._error:
            raise DataSourceUnavailable(source=self._name, reason=self._error)
        return PriceResult(
            price=self._price,
            source=self._name,
            timestamp=datetime.now(UTC),
            confidence=0.95,
        )

    async def close(self) -> None:  # pragma: no cover — required by the protocol
        pass


# ─── 1. The bug reproducer — RED until VIB-4439 lands ─────────────────────────


class TestTwoSourceDivergenceFailsClosed:
    """B3 — two-source aggregation must fail closed on divergence."""

    @pytest.mark.asyncio
    async def test_wsteth_divergent_pair_raises_instead_of_midpoint(self) -> None:
        """The Morpho fixture's wstETH bug, distilled to a unit test.

        Before VIB-4439: aggregator returns midpoint ≈ $1798.65 silently.
        After VIB-4439:  aggregator raises ``AllDataSourcesFailed`` because
                          the two sources disagree by ~36× (above the 2%
                          outlier threshold, below the 100× magnitude cap).
        """
        chainlink = _MockPriceSource("chainlink", price=Decimal("3500"))
        dexscreener = _MockPriceSource("dexscreener", price=Decimal("97.31"))
        aggregator = PriceAggregator(sources=[chainlink, dexscreener])

        with pytest.raises(AllDataSourcesFailed) as excinfo:
            await aggregator.get_aggregated_price("WSTETH")

        # The error should name BOTH sources so observability can trace
        # which feed disagreed (per AGENTS.md §Accounting "Empty ≠ Zero":
        # an erroneous price is worse than no price — the message must
        # make the disagreement loud, not invisible).
        errs = str(excinfo.value)
        assert "chainlink" in errs and "dexscreener" in errs, (
            f"AllDataSourcesFailed must reference both source names so a human "
            f"can identify the disagreement. Got: {errs!r}"
        )

    @pytest.mark.asyncio
    async def test_modest_two_source_divergence_below_threshold_passes(self) -> None:
        """Sanity guard: 2 sources within the outlier threshold (2% default)
        still aggregate normally — the fix must not break consensus pricing.

        Sources within 1.5% of each other should produce the midpoint
        (which is also their median for exactly 2 values).
        """
        chainlink = _MockPriceSource("chainlink", price=Decimal("3500"))
        coingecko = _MockPriceSource("coingecko", price=Decimal("3520"))  # 0.57% deviation
        aggregator = PriceAggregator(sources=[chainlink, coingecko])

        result = await aggregator.get_aggregated_price("WSTETH")
        # 2-source median = mean = midpoint = 3510
        assert result.price == Decimal("3510")

    @pytest.mark.asyncio
    async def test_single_source_path_unchanged(self) -> None:
        """Regression guard: 1-source aggregation is unchanged. The single-
        source ceiling check (`test_price_aggregator_ceiling`) is the
        existing safety net for this path; this test only confirms the
        normal-price single-source flow still returns the price."""
        chainlink = _MockPriceSource("chainlink", price=Decimal("3500"))
        aggregator = PriceAggregator(sources=[chainlink])

        result = await aggregator.get_aggregated_price("WSTETH")
        assert result.price == Decimal("3500")

    @pytest.mark.asyncio
    async def test_three_source_with_one_outlier_uses_median(self) -> None:
        """Regression guard: 3+ source paths are NOT affected by the
        2-source fail-closed semantic. With 3 sources and 1 outlier, the
        median is robust and the outlier is filtered.

        sources = [3500, 3505, 97.31]
        median = 3500 (the outlier is excluded by the 2% deviation filter)
        """
        chainlink = _MockPriceSource("chainlink", price=Decimal("3500"))
        coingecko = _MockPriceSource("coingecko", price=Decimal("3505"))
        dexscreener = _MockPriceSource("dexscreener", price=Decimal("97.31"))
        aggregator = PriceAggregator(sources=[chainlink, coingecko, dexscreener])

        result = await aggregator.get_aggregated_price("WSTETH")
        # After outlier filtering on 3 sources: median([3500, 3505]) = 3502.5
        # Either median or midpoint of the surviving pair is fine — what
        # matters is the bad source did NOT move the result more than 1%.
        assert Decimal("3490") < result.price < Decimal("3520"), (
            f"3-source median with one outlier must be near 3500. Got: {result.price}"
        )

    @pytest.mark.asyncio
    async def test_two_source_extreme_magnitude_still_raises(self) -> None:
        """Existing magnitude_outlier_ratio behaviour preserved: a 100×+
        divergence still raises (this path predates VIB-4439 — proves the
        new fail-closed logic does not bypass the existing safety net)."""
        chainlink = _MockPriceSource("chainlink", price=Decimal("3500"))
        bad_feed = _MockPriceSource("bad_feed", price=Decimal("0.01"))  # 350,000×
        aggregator = PriceAggregator(sources=[chainlink, bad_feed])

        with pytest.raises(AllDataSourcesFailed):
            await aggregator.get_aggregated_price("WSTETH")
