"""Gateway-side connector binding for Agni Finance (VIB-4853 / W1).

Agni Finance is a Uniswap V3 fork on Mantle. There is no separate
``almanak/connectors/agni_finance/`` folder — Agni reuses the
Uniswap V3 connector's adapter / receipt parser, and the addresses sit
alongside Uniswap V3's in ``uniswap_v3/addresses.py``. This minimal
scaffold lets Agni be registered as its own protocol with the gateway
registry so non-connector callers can resolve its addresses through
:class:`GatewayAddressCapability` without importing the dict by name.

Contributes:

* ``GatewayAddressCapability`` — Agni's Mantle addresses, surfaced under
  ``ProtocolName("agni_finance")``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import AGNI_FINANCE


class AgniFinanceGatewayConnector(GatewayConnector, GatewayAddressCapability):
    """Gateway-side connector for Agni Finance (Uniswap V3 fork on Mantle)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("agni_finance")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Agni Finance contract addresses for ``chain`` (or empty)."""
        return AGNI_FINANCE.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Agni Finance addresses are registered."""
        return frozenset(AGNI_FINANCE.keys())


__all__ = ["AgniFinanceGatewayConnector"]
