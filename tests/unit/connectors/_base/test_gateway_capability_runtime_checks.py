"""Each ``Gateway*Capability`` Protocol must be runtime-checkable.

The registry routes via ``isinstance(connector, Cap)``; if a capability
loses ``@runtime_checkable`` the registry silently stops routing to
connectors that implement it. These tests pin the contract.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayMarketLookupCapability,
    GatewayPoolKeySeedCapability,
    GatewayServicerCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _ServicerOnly(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("servicer_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PREDICTION_MARKET

    def register_servicers(self, server: Any, settings: Any) -> None:
        pass


class _LookupOnly(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("lookup_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def market_lookup(self) -> Any:
        return object()


class _SeedOnly(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("seed_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def seed_pool_keys(self, cache: Any) -> None:
        pass


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


def test_pool_key_seed_capability_runtime_isinstance() -> None:
    assert isinstance(_SeedOnly(), GatewayPoolKeySeedCapability)
    assert not isinstance(_LookupOnly(), GatewayPoolKeySeedCapability)


def test_multi_capability_inheritance() -> None:
    class _DualCap(GatewayConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("dual_demo")
        kind: ClassVar[ProtocolKind] = ProtocolKind.LP

        def market_lookup(self) -> Any:
            return object()

        def seed_pool_keys(self, cache: Any) -> None:
            pass

    inst = _DualCap()
    assert isinstance(inst, GatewayMarketLookupCapability)
    assert isinstance(inst, GatewayPoolKeySeedCapability)
    assert not isinstance(inst, GatewayServicerCapability)
