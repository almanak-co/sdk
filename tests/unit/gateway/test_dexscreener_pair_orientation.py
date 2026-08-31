"""Direct unit tests for ALM-3147: DexScreener pair-side orientation in
``_pick_best_pair`` and the on-demand identity gate in
``_resolve_token_for_pricing``. End-to-end coverage lives in
``tests/gateway/test_dexscreener_multichain.py`` and
``tests/gateway/test_market_service_unconfigured_price.py``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.data.price.dexscreener import DexScreenerPriceSource

USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
AERO = "0x940181a94A35A4569E4529A3CDfB74e38FD98631"
USDBC = "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA"


def _pair(
    price_usd: str,
    *,
    liquidity: float = 28_000_000,
    base: str = AERO,
    base_symbol: str = "AERO",
    quote: str = USDC,
    quote_symbol: str = "USDC",
    price_native: str | None = "0.48",
) -> dict:
    p = {
        "priceUsd": price_usd,
        "liquidity": {"usd": liquidity},
        "volume": {"h24": 1_000_000},
        "baseToken": {"address": base, "symbol": base_symbol},
        "quoteToken": {"address": quote, "symbol": quote_symbol},
    }
    if price_native is not None:
        p["priceNative"] = price_native
    return p


@pytest.fixture
def source() -> DexScreenerPriceSource:
    return DexScreenerPriceSource(cache_ttl=30, min_liquidity_usd=10_000)


class TestPickBestPairOrientation:
    def test_base_side_match_uses_price_usd(self, source):
        picked = source._pick_best_pair(
            [_pair("1.0001", base=USDC, base_symbol="USDC", quote=USDBC, quote_symbol="USDbC")],
            address=USDC,
            token=USDC,
        )
        assert picked is not None
        assert picked[1] == Decimal("1.0001")

    def test_base_side_wins_over_deeper_quote_side(self, source):
        """The launch-day shape: AERO/USDC at $28M must lose to a much
        smaller base-side USDC pair."""
        picked = source._pick_best_pair(
            [
                _pair("0.48", liquidity=28_000_000),
                _pair("1.0001", liquidity=180_000, base=USDC, base_symbol="USDC", quote=USDBC, quote_symbol="USDbC"),
            ],
            address=USDC,
            token=USDC,
        )
        assert picked is not None
        assert picked[1] == Decimal("1.0001")

    def test_quote_side_only_inverts_price_native(self, source):
        picked = source._pick_best_pair([_pair("0.48", price_native="0.48")], address=USDC, token=USDC)
        assert picked is not None
        assert picked[1] == Decimal("1")

    def test_unrelated_pair_is_discarded(self, source):
        picked = source._pick_best_pair(
            [_pair("5.23", base=AERO, base_symbol="AERO", quote=USDBC, quote_symbol="USDbC")],
            address=USDC,
            token=USDC,
        )
        assert picked is None

    def test_symbol_match_on_search_path(self, source):
        picked = source._pick_best_pair(
            [_pair("5.23", base="0xabc", base_symbol="FOO", quote="0xdef", quote_symbol="WETH")],
            address=None,
            token="FOO",
        )
        assert picked is not None
        assert picked[1] == Decimal("5.23")

    def test_quote_side_unusable_price_native_is_skipped(self, source):
        for bad_native in ("0", "-1", None, "NaN", "Infinity", "garbage"):
            assert source._pick_best_pair([_pair("0.48", price_native=bad_native)], address=USDC, token=USDC) is None


class TestPickBestPairMalformedValues:
    """CodeRabbit on #3822: NaN/Infinity from the API must be skipped, and a
    malformed pair must never prevent a later valid pair from winning."""

    def test_nan_price_does_not_abort_later_valid_pair(self, source):
        picked = source._pick_best_pair(
            [
                _pair("NaN", base=USDC, base_symbol="USDC", quote=USDBC, quote_symbol="USDbC"),
                _pair("1.0001", liquidity=50_000, base=USDC, base_symbol="USDC", quote=USDBC, quote_symbol="USDbC"),
            ],
            address=USDC,
            token=USDC,
        )
        assert picked is not None
        assert picked[1] == Decimal("1.0001")

    def test_infinite_price_is_skipped(self, source):
        assert (
            source._pick_best_pair(
                [_pair("Infinity", base=USDC, base_symbol="USDC", quote=USDBC, quote_symbol="USDbC")],
                address=USDC,
                token=USDC,
            )
            is None
        )

    def test_non_finite_liquidity_cannot_win_selection(self, source):
        picked = source._pick_best_pair(
            [
                _pair("9.99", liquidity=float("inf"), base=USDC, base_symbol="USDC", quote=USDBC, quote_symbol="USDbC"),
                _pair("7.77", liquidity=float("nan"), base=USDC, base_symbol="USDC", quote=USDBC, quote_symbol="USDbC"),
                _pair("1.0001", liquidity=50_000, base=USDC, base_symbol="USDC", quote=USDBC, quote_symbol="USDbC"),
            ],
            address=USDC,
            token=USDC,
        )
        assert picked is not None
        assert picked[1] == Decimal("1.0001")


class TestIdentityGateOnDemand:
    """ALM-3147 leg 2: the identity gate must use the same servable-chain
    predicate as GetPrice (on-demand mode for unconfigured gateways, hard
    allowlist once provisioned)."""

    @staticmethod
    def _servicer(chains):
        from almanak.gateway.services.market_service import MarketServiceServicer

        settings = MagicMock(spec=GatewaySettings)
        settings.chains = chains
        settings.network = "mainnet"
        settings.coingecko_api_key = None
        return MarketServiceServicer(settings=settings)

    @pytest.mark.asyncio
    async def test_unconfigured_gateway_resolves_identity(self):
        from almanak.framework.data.tokens.pegs import is_pegged

        resolved = await self._servicer([])._resolve_token_for_pricing(USDC, "base")

        assert resolved is not None
        assert resolved.symbol == "USDC"
        assert is_pegged(resolved.token_ref) is not None

    @pytest.mark.asyncio
    async def test_provisioned_gateway_keeps_hard_allowlist(self):
        assert await self._servicer(["arbitrum"])._resolve_token_for_pricing(USDC, "base") is None
