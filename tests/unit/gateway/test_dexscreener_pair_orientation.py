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
    volume: float = 1_000_000,
    base: str = AERO,
    base_symbol: str = "AERO",
    quote: str = USDC,
    quote_symbol: str = "USDC",
    price_native: str | None = "0.48",
) -> dict:
    p = {
        "priceUsd": price_usd,
        "liquidity": {"usd": liquidity},
        "volume": {"h24": volume},
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


USDE_RH = "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34"
USDG_RH = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"
USDB_RH = "0x0000000000000000000000000000000000000B0B"


class TestPickBestPairVolumeFloor:
    """A deep pool nobody trades in is not a price observation.

    Shape observed on Robinhood on 2026-09-14: the deepest USDe pair was a
    USDe/USDB pool with $191M liquidity, $0 24h volume and ``priceUsd`` 0.9604
    (USDB's stale valuation applied to a 1.002 pool ratio), while every
    USDe/USDG pool with real volume quoted 0.9997 and the Morpho market oracle
    priced 1.0. Liquidity-only selection returned 0.9604.
    """

    @staticmethod
    def _robinhood_usde_pairs() -> list[dict]:
        pairs = [
            _pair(
                "0.9604",
                liquidity=191_714_165,
                volume=0,
                base=USDE_RH,
                base_symbol="USDe",
                quote=USDB_RH,
                quote_symbol="USDB",
                price_native="1.002002",
            ),
            _pair(
                "0.9997",
                liquidity=1_013_146,
                volume=64_778,
                base=USDE_RH,
                base_symbol="USDe",
                quote=USDG_RH,
                quote_symbol="USDG",
                price_native="0.9997",
            ),
        ]
        for p in pairs:
            p["chainId"] = "robinhood"
        return pairs

    def test_dead_pool_loses_to_traded_pool(self, source):
        picked = source._pick_best_pair(self._robinhood_usde_pairs(), address=USDE_RH, token=USDE_RH)
        assert picked is not None
        assert picked[1] == Decimal("0.9997")

    def test_dead_pool_is_not_a_fallback_either(self, source):
        only_dead = self._robinhood_usde_pairs()[:1]
        assert source._pick_best_pair(only_dead, address=USDE_RH, token=USDE_RH) is None

    def test_missing_volume_is_rejected_as_unmeasured(self, source):
        pair = _pair("0.9604", liquidity=191_714_165, base=USDE_RH, base_symbol="USDe")
        del pair["volume"]
        assert source._pick_best_pair([pair], address=USDE_RH, token=USDE_RH) is None

    def test_non_finite_volume_cannot_win_selection(self, source):
        picked = source._pick_best_pair(
            [
                _pair("9.99", volume=float("inf"), base=USDE_RH, base_symbol="USDe"),
                _pair("7.77", volume=float("nan"), base=USDE_RH, base_symbol="USDe"),
                _pair("0.9997", liquidity=50_000, volume=5_000, base=USDE_RH, base_symbol="USDe"),
            ],
            address=USDE_RH,
            token=USDE_RH,
        )
        assert picked is not None
        assert picked[1] == Decimal("0.9997")

    def test_zero_floor_still_rejects_unmeasured_volume(self):
        zero_floor = DexScreenerPriceSource(cache_ttl=30, min_liquidity_usd=10_000, min_volume_usd=0)
        missing = _pair("0.9604", liquidity=191_714_165, base=USDE_RH, base_symbol="USDe")
        del missing["volume"]
        unparseable = _pair("0.9604", liquidity=191_714_165, volume="lots", base=USDE_RH, base_symbol="USDe")
        explicit_zero = _pair("0.9604", liquidity=191_714_165, volume=0, base=USDE_RH, base_symbol="USDe")
        assert zero_floor._pick_best_pair([missing], address=USDE_RH, token=USDE_RH) is None
        assert zero_floor._pick_best_pair([unparseable], address=USDE_RH, token=USDE_RH) is None
        picked = zero_floor._pick_best_pair([explicit_zero], address=USDE_RH, token=USDE_RH)
        assert picked is not None and picked[1] == Decimal("0.9604")

    def test_positional_constructor_slots_are_unchanged(self):
        resolver = MagicMock()
        source = DexScreenerPriceSource("base", 30, 10.0, 10_000, 0.6, resolver)
        assert source._token_resolver is resolver
        assert source._min_volume_usd == 1_000

    def test_survivors_still_rank_by_liquidity(self, source):
        busy_shallow = _pair("1.01", liquidity=50_000, volume=900_000, base=USDE_RH, base_symbol="USDe")
        quiet_deep = _pair("1.02", liquidity=5_000_000, volume=50_000, base=USDE_RH, base_symbol="USDe")
        picked = source._pick_best_pair([busy_shallow, quiet_deep], address=USDE_RH, token=USDE_RH)
        assert picked is not None
        assert picked[1] == Decimal("1.02")

    @pytest.mark.asyncio
    async def test_public_price_raises_when_every_pool_is_untraded(self, source):
        from unittest.mock import AsyncMock, patch

        from almanak.framework.data.interfaces import DataSourceUnavailable
        from almanak.framework.data.tokens.models import ResolvedToken

        usde = ResolvedToken(symbol="USDe", address=USDE_RH, decimals=18, chain="robinhood", chain_id=4663)
        dead = [self._robinhood_usde_pairs()[0]]
        with (
            patch.object(source, "_get_session", new_callable=AsyncMock, return_value=object()),
            patch.object(source, "_fetch_token_pairs", new_callable=AsyncMock, return_value=dead),
        ):
            with pytest.raises(DataSourceUnavailable, match="volume"):
                await source.get_price("USDE", "USD", resolved_token=usde)

    @pytest.mark.asyncio
    async def test_public_price_comes_from_the_traded_pool(self, source):
        from unittest.mock import AsyncMock, patch

        from almanak.framework.data.tokens.models import ResolvedToken

        usde = ResolvedToken(symbol="USDe", address=USDE_RH, decimals=18, chain="robinhood", chain_id=4663)
        with (
            patch.object(source, "_get_session", new_callable=AsyncMock, return_value=object()),
            patch.object(
                source, "_fetch_token_pairs", new_callable=AsyncMock, return_value=self._robinhood_usde_pairs()
            ),
        ):
            result = await source.get_price("USDE", "USD", resolved_token=usde)
        assert result.price == Decimal("0.9997")
        assert result.source == "dexscreener"

    def test_volume_floor_is_configurable(self):
        lenient = DexScreenerPriceSource(cache_ttl=30, min_liquidity_usd=10_000, min_volume_usd=0)
        only_dead = self._robinhood_usde_pairs()[:1]
        picked = lenient._pick_best_pair(only_dead, address=USDE_RH, token=USDE_RH)
        assert picked is not None
        assert picked[1] == Decimal("0.9604")


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
