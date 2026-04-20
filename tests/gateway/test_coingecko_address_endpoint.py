"""Tests for CoinGecko's contract-address price endpoint fallback.

When a token is not in the hardcoded symbol/address registry but the caller
supplies a ``ResolvedToken`` with chain + contract address, the CoinGecko
source should call ``/simple/token_price/{platform}?contract_addresses=...``
and return a price. This is what lets a fresh token like cbBTC be priced
without adding it to any hardcoded list.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.core.enums import Chain
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.tokens import ResolvedToken
from almanak.gateway.data.price.coingecko import (
    COINGECKO_PLATFORM_IDS,
    GLOBAL_TOKEN_IDS,
    CoinGeckoPriceSource,
)

# cbBTC on Base - intentionally chosen because it's NOT in the registry.
CBBTC_ADDRESS = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"


def _mock_http_json(source: CoinGeckoPriceSource, payload, status: int = 200):
    """Patch the source's HTTP session to return a single JSON response."""
    resp = MagicMock()
    resp.status = status
    resp.json = AsyncMock(return_value=payload)
    resp.text = AsyncMock(return_value="")

    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)

    session = MagicMock()
    session.get = MagicMock(return_value=cm)

    return patch.object(source, "_get_session", new_callable=AsyncMock, return_value=session)


@pytest.fixture
def source() -> CoinGeckoPriceSource:
    # Force free-tier API so we don't depend on env vars in CI.
    return CoinGeckoPriceSource(api_key="")


class TestCoinGeckoAddressEndpoint:
    """The contract-address fallback path."""

    def test_cbbtc_is_not_hardcoded_anywhere(self):
        """Regression guard: if someone adds cbBTC to the hardcoded list the
        other tests would silently pass via the symbol/ID path. Fail loudly
        instead so the test case still exercises the address fallback."""
        assert "CBBTC" not in GLOBAL_TOKEN_IDS, "cbBTC must not be hardcoded for this test"
        from almanak.framework.data.tokens.defaults import DEFAULT_TOKENS, get_coingecko_id

        assert get_coingecko_id("CBBTC") is None, "cbBTC symbol must not map to a CG id statically"
        for tok in DEFAULT_TOKENS:
            assert CBBTC_ADDRESS.lower() not in {a.lower() for a in (tok.addresses or {}).values()}, (
                f"cbBTC address found on token {tok.symbol}; remove it so this test stays meaningful"
            )

    def test_base_platform_is_mapped(self):
        """Platform map must contain 'base' so the Base address endpoint works."""
        assert COINGECKO_PLATFORM_IDS.get("base") == "base"

    @pytest.mark.asyncio
    async def test_address_endpoint_prices_unknown_token(self, source: CoinGeckoPriceSource):
        """Unknown symbol/ID but known chain+address returns a price via
        /simple/token_price/{platform}."""
        resolved = ResolvedToken(
            symbol="cbBTC",
            address=CBBTC_ADDRESS,
            decimals=8,
            chain=Chain.BASE,
            chain_id=8453,
            source="on_chain",
            is_verified=False,
        )

        payload = {CBBTC_ADDRESS.lower(): {"usd": 65000.12}}

        with _mock_http_json(source, payload):
            result = await source.get_price(CBBTC_ADDRESS, "USD", resolved_token=resolved)

        assert result.price == Decimal("65000.12")
        assert result.source == "coingecko"
        assert result.stale is False

    @pytest.mark.asyncio
    async def test_no_resolved_token_still_raises_unknown(self, source: CoinGeckoPriceSource):
        """Without a ResolvedToken the source has no chain context and must
        keep raising DataSourceUnavailable. That's what makes the MarketService
        resolution step (task #3) load-bearing."""
        with pytest.raises(DataSourceUnavailable, match="Unknown token"):
            await source.get_price(CBBTC_ADDRESS, "USD")

    @pytest.mark.asyncio
    async def test_address_endpoint_propagates_transient_errors(self, source: CoinGeckoPriceSource):
        """HTTP 5xx on the address endpoint is a transient CoinGecko outage,
        not a "token doesn't exist" result. It must surface as
        DataSourceUnavailable so the aggregator can fall over to another
        source, not silently become "Unknown token" which bypasses
        stale-cache fallback and other health tracking."""
        resolved = ResolvedToken(
            symbol="cbBTC",
            address=CBBTC_ADDRESS,
            decimals=8,
            chain=Chain.BASE,
            chain_id=8453,
            source="on_chain",
            is_verified=False,
        )

        with _mock_http_json(source, payload=None, status=500):
            with pytest.raises(DataSourceUnavailable, match="HTTP 500"):
                await source.get_price(CBBTC_ADDRESS, "USD", resolved_token=resolved)

    @pytest.mark.asyncio
    async def test_unknown_chain_skips_address_endpoint(self, source: CoinGeckoPriceSource):
        """If the chain has no CoinGecko platform mapping, we must not try the
        address endpoint - otherwise CoinGecko returns 404 and we log noise."""
        # Use a plausible ResolvedToken on a chain absent from the platform map.
        # We assert the source never calls HTTP at all.
        chain_without_platform = next(
            (c for c in Chain if c.value.lower() not in COINGECKO_PLATFORM_IDS),
            None,
        )
        if chain_without_platform is None:
            pytest.skip("Every Chain enum value currently has a CoinGecko platform mapping")

        resolved = ResolvedToken(
            symbol="XYZ",
            address=CBBTC_ADDRESS,  # address contents are irrelevant for this path
            decimals=18,
            chain=chain_without_platform,
            chain_id=0,
            source="on_chain",
            is_verified=False,
        )

        session = MagicMock()
        session.get = MagicMock()
        with patch.object(source, "_get_session", new_callable=AsyncMock, return_value=session):
            with pytest.raises(DataSourceUnavailable, match="Unknown token"):
                await source.get_price(CBBTC_ADDRESS, "USD", resolved_token=resolved)

        session.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_address_endpoint_uses_lowercased_address(self, source: CoinGeckoPriceSource):
        """CoinGecko returns lowercased addresses in keys; verify the request
        is sent lowercased so the response lookup matches."""
        resolved = ResolvedToken(
            symbol="cbBTC",
            address=CBBTC_ADDRESS,  # mixed-case input
            decimals=8,
            chain=Chain.BASE,
            chain_id=8453,
            source="on_chain",
            is_verified=False,
        )

        payload = {CBBTC_ADDRESS.lower(): {"usd": 65000.0}}
        captured: dict = {}

        resp = MagicMock()
        resp.status = 200
        resp.json = AsyncMock(return_value=payload)
        resp.text = AsyncMock(return_value="")

        cm = AsyncMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)

        def capture_get(url, params=None):
            captured["url"] = url
            captured["params"] = params
            return cm

        session = MagicMock()
        session.get = MagicMock(side_effect=capture_get)

        with patch.object(source, "_get_session", new_callable=AsyncMock, return_value=session):
            await source.get_price(CBBTC_ADDRESS, "USD", resolved_token=resolved)

        assert captured["url"].endswith("/simple/token_price/base")
        assert captured["params"]["contract_addresses"] == CBBTC_ADDRESS.lower()
        assert captured["params"]["vs_currencies"] == "usd"
