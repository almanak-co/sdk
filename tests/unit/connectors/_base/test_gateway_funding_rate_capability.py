"""``GatewayFundingRateCapability`` contract tests (VIB-4811 / Phase 3).

The funding-rate servicer dispatches by venue via the registry instead
of an ``if venue == "...":`` chain. Tests pin:

* ``isinstance(connector, GatewayFundingRateCapability)`` is True iff
  the connector defines ``venue`` and ``fetch_funding_rate``.
* GMX does not advertise a fabricated default rate; Hyperliquid retains its
  legacy fallback independently.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewayFundingRateCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _FundingRateImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("funding_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def venue(self) -> str:
        return "demo_venue"

    def default_funding_rate(self, market: str) -> Decimal:
        return Decimal("0.0001")

    async def fetch_funding_rate(
        self,
        servicer: Any,
        market: str,
        chain: str,
        market_address: str = "",
    ) -> Any:
        del servicer, market_address
        return ("fetched", market, chain)


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_funding_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP


def test_funding_rate_capability_runtime_isinstance() -> None:
    assert isinstance(_FundingRateImpl(), GatewayFundingRateCapability)
    assert not isinstance(_BareConnector(), GatewayFundingRateCapability)


def test_registered_gmx_v2_and_hyperliquid_advertise_capability() -> None:
    """The registered Phase-3 perp connectors expose the capability."""
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewayFundingRateCapability)
    venues = {p.venue() for p in providers}
    assert {"gmx_v2", "hyperliquid"}.issubset(venues)


def test_gmx_v2_has_no_fabricated_default_funding_rate() -> None:
    from almanak.connectors.gmx_v2.gateway.provider import GmxV2GatewayConnector

    connector = GmxV2GatewayConnector()
    assert not hasattr(connector, "default_funding_rate")


def test_hyperliquid_default_funding_rate_matches_legacy_dict() -> None:
    """Hyperliquid connector returns the legacy ``DEFAULT_RATES`` values."""
    from almanak.connectors.hyperliquid.gateway.provider import HyperliquidGatewayConnector

    connector = HyperliquidGatewayConnector()
    expected = {
        "ETH-USD": Decimal("0.000015"),
        "BTC-USD": Decimal("0.000011"),
        "ARB-USD": Decimal("0.000018"),
        "LINK-USD": Decimal("0.000009"),
        "SOL-USD": Decimal("0.000022"),
    }
    for market, rate in expected.items():
        assert connector.default_funding_rate(market) == rate
    assert connector.default_funding_rate("DOGE-USD") == Decimal("0.00001")


def test_servicer_default_lookup_is_not_used_as_a_gmx_capability() -> None:
    from almanak.gateway.services.funding_rate_service import (
        FundingRateServiceServicer,
    )

    servicer = FundingRateServiceServicer(settings=SimpleNamespace(network="mainnet"))
    with pytest.raises(ValueError, match="does not permit"):
        servicer._get_default_rate("gmx_v2", "ETH-USD")
    assert servicer._get_default_rate("hyperliquid", "ETH-USD") == Decimal("0.000015")


@pytest.mark.asyncio
async def test_capability_dispatch_routes_to_correct_connector() -> None:
    """``fetch_funding_rate`` is dispatched through the capability instance.

    Hyperliquid still delegates to the servicer's live implementation. GMX V2
    owns its exact-market native fetch connector-side; this dispatch test
    replaces that method at the capability seam so it performs no network.
    """
    from unittest.mock import AsyncMock

    from almanak.gateway.services.funding_rate_service import (
        FundingRateServiceServicer,
    )

    servicer = FundingRateServiceServicer(settings=SimpleNamespace(network="mainnet"))
    servicer._fetch_hyperliquid_rate = AsyncMock(return_value="hyperliquid-result")  # type: ignore[method-assign]

    hl = servicer._funding_rate_providers["hyperliquid"]
    gmx = servicer._funding_rate_providers["gmx_v2"]
    gmx.fetch_funding_rate = AsyncMock(return_value="gmx-result")  # type: ignore[method-assign]
    assert await hl.fetch_funding_rate(servicer, "ETH-USD", "arbitrum") == "hyperliquid-result"
    servicer._fetch_hyperliquid_rate.assert_awaited_once_with("ETH-USD")

    gmx_result = await gmx.fetch_funding_rate(servicer, "ETH-USD", "arbitrum")
    assert gmx_result == "gmx-result"
    gmx.fetch_funding_rate.assert_awaited_once_with(servicer, "ETH-USD", "arbitrum")
