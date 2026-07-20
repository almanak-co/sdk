"""Gateway funding lanes accept every canonical perp-market spelling.

Campaign-50 s38: ``market="ETH/USD"`` (the SDK's documented slash form) could
not be mapped by the gateway funding lane — the Hyperliquid coin parse split on
``"-"`` only and the venue tables are keyed by the dash form — while
``"ETH-USD"`` worked end to end. Every gateway ingress and connector table
lookup now canonicalizes through ``almanak.core.perp_markets``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors.gmx_v2.gateway.provider import GmxV2GatewayConnector
from almanak.connectors.hyperliquid.gateway.provider import (
    _HYPERLIQUID_DEFAULT_RATES,
    _UNKNOWN_MARKET_DEFAULT,
    HyperliquidGatewayConnector,
    _hyperliquid_resolve_coin,
)
from almanak.core.perp_markets import perp_market_funding_key


class TestHyperliquidCoinResolution:
    @pytest.mark.parametrize("market", ["ETH-USD", "ETH/USD", "ETH", "eth/usd"])
    def test_every_spelling_resolves_the_same_coin(self, market: str) -> None:
        assert _hyperliquid_resolve_coin(market) == "ETH"

    def test_unknown_market_still_fails_closed(self) -> None:
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        with pytest.raises(RateHistoryUnavailable):
            _hyperliquid_resolve_coin("NOPE/USD")


class TestDefaultRateTables:
    def test_hyperliquid_default_rate_slash_equals_dash(self) -> None:
        connector = HyperliquidGatewayConnector()
        assert connector.default_funding_rate("ETH/USD") == _HYPERLIQUID_DEFAULT_RATES["ETH-USD"]
        assert connector.default_funding_rate("ETH/USD") != _UNKNOWN_MARKET_DEFAULT

    def test_gmx_default_rate_slash_equals_dash(self) -> None:
        connector = GmxV2GatewayConnector()
        assert connector.default_funding_rate("ETH/USD") == connector.default_funding_rate("ETH-USD")
        assert connector.default_funding_rate("ETH/USD") == Decimal("0.000012")


class TestIngressCanonicalization:
    """The market string the servicers key tables/responses by is the dash form."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("ETH/USD", "ETH-USD"),
            ("ETH-USD", "ETH-USD"),
            ("eth/usdc", "ETH-USD"),
            ("BTC", "BTC-USD"),
        ],
    )
    def test_funding_key_canonical_form(self, raw: str, expected: str) -> None:
        assert perp_market_funding_key(raw) == expected

    def test_unparseable_market_passes_through_upper(self) -> None:
        # The ingress fallback (`or request.market.upper()`) keeps address-style
        # identifiers intact so the venue's own unsupported-market error names
        # what the caller actually sent.
        raw = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
        assert perp_market_funding_key(raw) is None
