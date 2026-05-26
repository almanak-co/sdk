"""``GatewayMarketLookupCapability`` contract tests (VIB-4810 / VIB-4817).

VIB-4810 introduced the capability; VIB-4817 collapsed the eight
hand-wired ``TokenService._get_<protocol>()`` accessor methods into a
single ``_get_lookup(protocol)`` that dispatches through
``GATEWAY_REGISTRY.get(ProtocolName(protocol))``. Tests pin:

* ``isinstance(connector, GatewayMarketLookupCapability)`` is True iff
  the connector defines ``market_lookup``.
* The eight market-lookup connectors (aave_v3, compound_v3, fluid,
  morpho_vault, pendle, jupiter, beefy, yearn) all advertise the
  capability via the registry.
* ``TokenService._get_lookup`` raises a clear error for unregistered
  or non-capable protocols.
"""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewayMarketLookupCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _MarketLookupImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("market_lookup_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def market_lookup(self) -> Any:
        return AsyncMock(return_value="demo-lookup")


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_market_lookup_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP


def test_market_lookup_capability_runtime_isinstance() -> None:
    assert isinstance(_MarketLookupImpl(), GatewayMarketLookupCapability)
    assert not isinstance(_BareConnector(), GatewayMarketLookupCapability)


def test_registered_market_lookup_connectors_advertise_capability() -> None:
    """All eight Phase-2 market-lookup connectors expose the capability."""
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewayMarketLookupCapability)
    protocols = {str(p.protocol) for p in providers}
    expected = {
        "aave_v3",
        "compound_v3",
        "fluid",
        "morpho_vault",
        "pendle",
        "jupiter",
        "beefy",
        "yearn",
    }
    assert expected.issubset(protocols)


@pytest.mark.asyncio
async def test_token_service_get_lookup_dispatches_via_registry() -> None:
    """``_get_lookup`` resolves through GATEWAY_REGISTRY and awaits the factory.

    VIB-4817 collapse: the previous ``_get_aave``/``_get_compound``/...
    methods are replaced by a single registry-driven dispatcher.
    """
    from almanak.connectors._base.gateway_connector import GatewayConnector
    from almanak.connectors._base.types import ProtocolKind, ProtocolName
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.token_service import TokenServiceServicer

    sentinel_lookup = object()

    class _Provider(GatewayConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("vib4817_demo")
        kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

        def market_lookup(self) -> Any:
            async def _factory() -> Any:
                return sentinel_lookup

            return _factory

    provider = _Provider()
    GATEWAY_REGISTRY.register(provider)
    try:
        service = TokenServiceServicer(GatewaySettings())
        result = await service._get_lookup("vib4817_demo")
        assert result is sentinel_lookup
    finally:
        # Manually unregister — the registry collision guard would
        # otherwise poison subsequent test runs in the same process.
        GATEWAY_REGISTRY._connectors.pop(provider.protocol, None)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_token_service_get_lookup_unknown_protocol_raises() -> None:
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.token_service import TokenServiceServicer

    service = TokenServiceServicer(GatewaySettings())
    with pytest.raises(KeyError, match="no gateway connector registered"):
        await service._get_lookup("does_not_exist_protocol")


@pytest.mark.asyncio
async def test_token_service_get_lookup_non_capable_connector_raises() -> None:
    """Connector exists but does not implement ``GatewayMarketLookupCapability``."""
    from almanak.connectors._base.gateway_connector import GatewayConnector
    from almanak.connectors._base.types import ProtocolKind, ProtocolName
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.token_service import TokenServiceServicer

    class _NotCapable(GatewayConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("vib4817_not_capable_demo")
        kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    provider = _NotCapable()
    GATEWAY_REGISTRY.register(provider)
    try:
        service = TokenServiceServicer(GatewaySettings())
        with pytest.raises(TypeError, match="does not implement GatewayMarketLookupCapability"):
            await service._get_lookup("vib4817_not_capable_demo")
    finally:
        GATEWAY_REGISTRY._connectors.pop(provider.protocol, None)  # type: ignore[attr-defined]


def test_registered_market_lookup_returns_callable() -> None:
    """Each registered provider returns a callable from ``market_lookup()``.

    Pins the contract VIB-4817 relies on: ``_get_lookup`` awaits the
    callable returned by ``market_lookup()``; if a provider were ever
    refactored to return an awaited result directly the dispatcher
    would break.
    """
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewayMarketLookupCapability)
    for provider in providers:
        factory = provider.market_lookup()
        assert callable(factory), f"{type(provider).__qualname__}.market_lookup() did not return a callable"
