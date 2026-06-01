"""Each ``Gateway*Capability`` Protocol must be runtime-checkable.

The registry routes via ``isinstance(connector, Cap)``; if a capability
loses ``@runtime_checkable`` the registry silently stops routing to
connectors that implement it. These tests pin the contract.
"""

from __future__ import annotations

from typing import Any, ClassVar

from collections.abc import Mapping

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDexTwapCapability,
    GatewayDexVolumeCapability,
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


class _AddressOnly(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("address_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    _addresses: ClassVar[dict[str, dict[str, str]]] = {
        "ethereum": {"router": "0x" + "11" * 20, "factory": "0x" + "22" * 20},
        "arbitrum": {"router": "0x" + "33" * 20, "factory": "0x" + "44" * 20},
    }

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        return self._addresses.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        return frozenset(self._addresses.keys())


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


def test_address_capability_runtime_isinstance() -> None:
    """Connectors that declare ``addresses_for`` + ``address_supported_chains``
    structurally satisfy :class:`GatewayAddressCapability`."""
    assert isinstance(_AddressOnly(), GatewayAddressCapability)
    assert not isinstance(_ServicerOnly(), GatewayAddressCapability)
    assert not isinstance(_BareConnector(), GatewayAddressCapability)


def test_address_capability_contract_basics() -> None:
    """Basic contract surface: per-chain mapping + supported-chain set.

    * Registered chain returns its mapping with the connector's contract-kind
      keys; unregistered chain returns an empty mapping (callers must NOT
      assume any specific key is present).
    * ``address_supported_chains`` returns the keys of the underlying table
      as a ``frozenset`` (immutability + set semantics for membership tests).
    """
    inst = _AddressOnly()
    eth = inst.addresses_for("ethereum")
    assert eth["router"] == "0x" + "11" * 20
    assert eth["factory"] == "0x" + "22" * 20

    assert inst.addresses_for("solana") == {}
    assert inst.address_supported_chains() == frozenset({"ethereum", "arbitrum"})
    # Set semantics — frozenset, not a list/tuple that callers could mutate.
    assert isinstance(inst.address_supported_chains(), frozenset)


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


# =============================================================================
# DEX TWAP / Volume capabilities must enforce ``dex_name()`` (VIB-4859 re-review)
#
# RateHistoryService keys its TWAP/Volume dispatch tables by
# ``conn.dex_name()``. The capability protocols declare ``dex_name()`` so the
# registry's structural ``isinstance`` check excludes a provider that forgot
# it — otherwise such a provider would slip through registration and
# ``AttributeError`` at dispatch-table build time.
# =============================================================================


class _TwapWithDexName(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("twap_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def dex_name(self) -> str:
        return "twap_demo"

    def twap_supported_chains(self) -> frozenset[str]:
        return frozenset({"ethereum"})

    async def fetch_twap(self, servicer: Any, **kwargs: Any) -> Any:
        return None

    async def fetch_twap_series(self, servicer: Any, **kwargs: Any) -> Any:
        return []


class _TwapMissingDexName(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("twap_no_name")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    # Deliberately omits ``dex_name()``.
    def twap_supported_chains(self) -> frozenset[str]:
        return frozenset({"ethereum"})

    async def fetch_twap(self, servicer: Any, **kwargs: Any) -> Any:
        return None

    async def fetch_twap_series(self, servicer: Any, **kwargs: Any) -> Any:
        return []


class _VolumeWithDexName(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("volume_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def dex_name(self) -> str:
        return "volume_demo"

    def volume_supported_chains(self) -> frozenset[str]:
        return frozenset({"ethereum"})

    async def fetch_volume_history(self, servicer: Any, **kwargs: Any) -> Any:
        return []


class _VolumeMissingDexName(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("volume_no_name")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    # Deliberately omits ``dex_name()``.
    def volume_supported_chains(self) -> frozenset[str]:
        return frozenset({"ethereum"})

    async def fetch_volume_history(self, servicer: Any, **kwargs: Any) -> Any:
        return []


def test_dex_twap_capability_requires_dex_name() -> None:
    """A TWAP provider missing ``dex_name()`` fails the structural check."""
    assert isinstance(_TwapWithDexName(), GatewayDexTwapCapability)
    assert not isinstance(_TwapMissingDexName(), GatewayDexTwapCapability)


def test_dex_volume_capability_requires_dex_name() -> None:
    """A volume provider missing ``dex_name()`` fails the structural check."""
    assert isinstance(_VolumeWithDexName(), GatewayDexVolumeCapability)
    assert not isinstance(_VolumeMissingDexName(), GatewayDexVolumeCapability)
