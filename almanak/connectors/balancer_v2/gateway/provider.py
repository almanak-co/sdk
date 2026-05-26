"""Gateway-side connector binding for Balancer v2.

Phase 3 (VIB-4811) introduces a minimal Balancer v2 gateway-side
scaffold so the protocol can publish capability-keyed metadata without
the gateway carrying a hardcoded ``"balancer-v2-*"`` table.

Currently contributes:

* ``GatewaySubgraphCapability`` — TheGraph subgraph URLs (Ethereum,
  Arbitrum). Moved verbatim from
  ``almanak.gateway.integrations.thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.

Strategy-side Balancer code (intents, connectors, receipt parsing)
remains unchanged and continues to live wherever it lived previously —
this scaffolding only owns the gateway-side capability surface.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewaySubgraphCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

# Balancer v2 subgraph URLs. Moved verbatim from
# ``thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
_BALANCER_V2_SUBGRAPHS: dict[str, str] = {
    "balancer-v2-ethereum": "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-v2",
    "balancer-v2-arbitrum": "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-arbitrum-v2",
}


class BalancerV2GatewayConnector(GatewayConnector, GatewaySubgraphCapability):
    """Gateway-side connector for Balancer v2."""

    protocol: ClassVar[ProtocolName] = ProtocolName("balancer_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def subgraph_endpoints(self) -> dict[str, str]:
        """TheGraph subgraph URLs for Balancer v2 (one per supported chain)."""
        return dict(_BALANCER_V2_SUBGRAPHS)


__all__ = ["BalancerV2GatewayConnector"]
