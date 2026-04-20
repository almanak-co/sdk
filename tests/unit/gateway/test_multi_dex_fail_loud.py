"""Tests for MultiDexPriceService fail-loud simulation pricing (VIB-3137).

The legacy ``_get_default_price`` helper shipped a hardcoded dict of approximate
prices (ETH=$2500, WBTC=$45000, MATIC=$0.50, ARB=$0.80, OP=$1.50, CRV=$0.40,
...). When quotes were requested for those tokens without a mock wired, the
service would silently fabricate a fake quote from those numbers, producing
divergent USD valuations downstream (the MATIC vs POL issue in VIB-3137 is
one surface).

The fix:

* Stablecoin <-> stablecoin pair: still priced 1:1 (peg is a design invariant,
  sourced from the canonical ``STABLECOINS`` constant — not a hardcoded dict).
* Everything else: fail loud via ``QuoteUnavailableError`` so the caller
  understands the DEX is unavailable in simulation and must provide a mock
  or wire a real oracle.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.gateway.data.price.multi_dex import (
    MultiDexPriceService,
    QuoteUnavailableError,
)


@pytest.fixture
def service():
    """Create a service on Ethereum (which supports all three DEXs)."""
    return MultiDexPriceService(chain="ethereum")


class TestFailLoudOnNonStablePairs:
    """Non-stable pairs must fail loud instead of fabricating quotes."""

    def test_get_default_price_raises_on_non_stable_pair(self, service):
        """VIB-3137: no hardcoded fallback for ETH/USDC or similar."""
        with pytest.raises(QuoteUnavailableError):
            service._get_default_price("USDC", "WETH")

    def test_get_default_price_raises_for_matic(self, service):
        """VIB-3137 root-cause: MATIC=$0.50 hardcoded fallback is gone."""
        with pytest.raises(QuoteUnavailableError) as exc_info:
            service._get_default_price("MATIC", "USDC")

        # The error message should mention the token and the simulation-only
        # nature so debuggers can trace the failure to its source.
        msg = str(exc_info.value)
        assert "MATIC" in msg
        assert "simulation" in msg.lower() or "mock" in msg.lower()

    def test_get_default_price_raises_for_pol(self, service):
        """POL must fail loud on the same path (no stale MATIC=$0.50 bleed)."""
        with pytest.raises(QuoteUnavailableError):
            service._get_default_price("POL", "USDC")

    def test_get_default_price_raises_for_wbtc(self, service):
        """WBTC=$45000 hardcoded fallback must also be gone."""
        with pytest.raises(QuoteUnavailableError):
            service._get_default_price("WBTC", "USDC")

    def test_get_default_price_raises_for_arb(self, service):
        """ARB=$0.80 hardcoded fallback must also be gone."""
        with pytest.raises(QuoteUnavailableError):
            service._get_default_price("ARB", "USDC")


class TestStablecoinPegPreserved:
    """Stable <-> stable pairs keep 1:1 behaviour (peg is a design invariant)."""

    def test_usdc_to_dai_is_one(self, service):
        assert service._get_default_price("USDC", "DAI") == Decimal("1")

    def test_usdc_to_usdt_is_one(self, service):
        assert service._get_default_price("USDC", "USDT") == Decimal("1")

    def test_case_insensitive_stablecoin_check(self, service):
        assert service._get_default_price("usdc", "dai") == Decimal("1")


class TestPartialFailureDoesNotBreakService:
    """get_prices_across_dexs must still return partial results on failure."""

    @pytest.mark.asyncio
    async def test_unmocked_non_stable_pair_returns_empty_result(self, service):
        """Without mocks, non-stable pairs produce zero quotes (not a crash)."""
        result = await service.get_prices_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
        )

        # All three DEXs raise QuoteUnavailableError (caught by fetch_quote),
        # so no quotes come back. Key assertion: the service does NOT fall
        # through to a fabricated hardcoded quote.
        assert result.token_in == "USDC"
        assert result.token_out == "WETH"
        assert len(result.quotes) == 0
