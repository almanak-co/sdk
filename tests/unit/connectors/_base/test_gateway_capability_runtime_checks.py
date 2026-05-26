"""Each ``Gateway*Capability`` Protocol must be runtime-checkable.

The registry routes via ``isinstance(connector, Cap)``; if a capability
loses ``@runtime_checkable`` the registry silently stops routing to
connectors that implement it. These tests pin the contract.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayMarketLookupCapability,
    GatewayPoolKeyCacheCapability,
    GatewayServicerCapability,
    PoolKeyCacheProtocol,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _ServicerOnly(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("servicer_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PREDICTION_MARKET
    servicer: Any | None = None

    def register_servicers(self, server: Any, settings: Any) -> None:
        pass


class _LookupOnly(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("lookup_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def market_lookup(self) -> Any:
        return object()


class _CacheOnly(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("cache_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def build_cache(self, *, network: str) -> PoolKeyCacheProtocol:
        class _Cache:
            async def lookup(self, chain: str, pool_id: bytes) -> Any | None:
                return None

        return _Cache()


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP


def test_servicer_capability_runtime_isinstance() -> None:
    assert isinstance(_ServicerOnly(), GatewayServicerCapability)
    assert not isinstance(_LookupOnly(), GatewayServicerCapability)
    assert not isinstance(_BareConnector(), GatewayServicerCapability)


def test_lookup_capability_runtime_isinstance() -> None:
    assert isinstance(_LookupOnly(), GatewayMarketLookupCapability)
    assert not isinstance(_ServicerOnly(), GatewayMarketLookupCapability)


def test_pool_key_cache_capability_runtime_isinstance() -> None:
    assert isinstance(_CacheOnly(), GatewayPoolKeyCacheCapability)
    assert not isinstance(_LookupOnly(), GatewayPoolKeyCacheCapability)


def test_multi_capability_inheritance() -> None:
    class _DualCap(GatewayConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("dual_demo")
        kind: ClassVar[ProtocolKind] = ProtocolKind.LP

        def market_lookup(self) -> Any:
            return object()

        def build_cache(self, *, network: str) -> PoolKeyCacheProtocol:
            class _Cache:
                async def lookup(self, chain: str, pool_id: bytes) -> Any | None:
                    return None

            return _Cache()

    inst = _DualCap()
    assert isinstance(inst, GatewayMarketLookupCapability)
    assert isinstance(inst, GatewayPoolKeyCacheCapability)
    assert not isinstance(inst, GatewayServicerCapability)
