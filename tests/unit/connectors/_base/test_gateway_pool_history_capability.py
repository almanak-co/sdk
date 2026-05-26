"""``GatewayPoolHistoryCapability`` contract tests (VIB-4811 / Phase 3).

The pool-history validator unions every registered connector's
``pool_history_supported_chains()`` into the live allowlist. Tests pin:

* ``isinstance(connector, GatewayPoolHistoryCapability)`` is True iff
  the connector defines ``pool_history_supported_chains``.
* The registered ``uniswap_v3`` + ``aerodrome`` connectors contribute the
  expected chains.
* The derived ``POOL_PROTOCOL_ALLOWLIST`` / ``SUPPORTED_POOL_PAIRS``
  tables in ``pool_history_service`` match what existed before the
  refactor (byte-identical dispatch behaviour).
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayPoolHistoryCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _PoolHistoryImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("pool_hist_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def pool_history_supported_chains(self) -> frozenset[str]:
        return frozenset({"ethereum", "base"})


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_pool_hist_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP


def test_pool_history_capability_runtime_isinstance() -> None:
    assert isinstance(_PoolHistoryImpl(), GatewayPoolHistoryCapability)
    assert not isinstance(_BareConnector(), GatewayPoolHistoryCapability)


def test_pool_history_capability_returns_frozenset() -> None:
    inst = _PoolHistoryImpl()
    result = inst.pool_history_supported_chains()
    assert isinstance(result, frozenset)
    assert result == frozenset({"ethereum", "base"})


def test_pool_history_capability_does_not_imply_other_caps() -> None:
    """A pool-history provider must not be picked up as e.g. a market-lookup."""
    from almanak.connectors._base.gateway_capabilities import (
        GatewayMarketLookupCapability,
        GatewayServicerCapability,
    )

    inst: Any = _PoolHistoryImpl()
    assert not isinstance(inst, GatewayMarketLookupCapability)
    assert not isinstance(inst, GatewayServicerCapability)


def test_pool_history_tables_match_legacy_set() -> None:
    """The registry-derived tables match the Phase-2 hardcoded sets.

    Locks the dispatch behaviour byte-identically across the refactor.
    """
    from almanak.gateway.services.pool_history_service import (
        POOL_PROTOCOL_ALLOWLIST,
        SUPPORTED_POOL_PAIRS,
    )

    assert POOL_PROTOCOL_ALLOWLIST == frozenset({"uniswap_v3", "aerodrome"})
    assert SUPPORTED_POOL_PAIRS == frozenset(
        {
            ("ethereum", "uniswap_v3"),
            ("arbitrum", "uniswap_v3"),
            ("base", "uniswap_v3"),
            ("optimism", "uniswap_v3"),
            ("polygon", "uniswap_v3"),
            ("base", "aerodrome"),
        }
    )


def test_registered_uniswap_v3_and_aerodrome_advertise_capability() -> None:
    """The registered connectors expose ``GatewayPoolHistoryCapability``."""
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewayPoolHistoryCapability)
    protocols = {str(p.protocol) for p in providers}
    assert "uniswap_v3" in protocols
    assert "aerodrome" in protocols
