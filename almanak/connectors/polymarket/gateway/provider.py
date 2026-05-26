"""Gateway-side connector binding for Polymarket (VIB-4810).

Declares the ``GatewayServicerCapability`` so the gateway boot loop can
register the Polymarket gRPC servicer via the connector registry instead
of hand-wiring it in :mod:`almanak.gateway.server`.

Polymarket is the highest-risk move in Phase 1+2: the servicer holds a
``SecretStr`` API key, builds ``ClobClient`` instances per wallet,
manages an LRU market-shape cache, and proxies a stateful EIP-712 /
HMAC auth stack. The connector provider only changes the construction
point — every existing credential and lifecycle invariant in
``PolymarketServiceServicer`` is preserved byte-identically.

Phase 1+2 — ``server.py`` switches to instantiating the servicer
through ``GATEWAY_REGISTRY``; the explicit
``polymarket_pb2_grpc.add_PolymarketServiceServicer_to_server`` call
stays here inside ``register_servicers``. Phase 4 collapses
``server.py`` to a loop over
``GATEWAY_REGISTRY.capability_providers(GatewayServicerCapability)``;
Phase 5 (VIB-4813) relocates the proto definition itself out of
``almanak/gateway/proto/`` into this connector's ``proto/`` module.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayServicerCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors.polymarket.proto import polymarket_pb2_grpc

from .service import PolymarketServiceServicer


class PolymarketGatewayConnector(GatewayConnector, GatewayServicerCapability):
    """Gateway-side connector for Polymarket."""

    protocol: ClassVar[ProtocolName] = ProtocolName("polymarket")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PREDICTION_MARKET

    def __init__(self) -> None:
        self._servicer: PolymarketServiceServicer | None = None

    @property
    def servicer(self) -> PolymarketServiceServicer | None:
        """Constructed servicer instance, populated by ``register_servicers``.

        ``server.py`` holds onto this reference for the shutdown path
        (which iterates servicers and calls ``close()`` on each).
        """
        return self._servicer

    def register_servicers(self, server: Any, settings: Any) -> None:
        """Construct the Polymarket servicer and bind it to ``server``.

        The servicer carries credential state (``SecretStr`` API keys,
        per-wallet ``ClobClient`` instances) loaded from ``settings``;
        construction is the only step where credentials transit.
        """
        self._servicer = PolymarketServiceServicer(settings)
        polymarket_pb2_grpc.add_PolymarketServiceServicer_to_server(self._servicer, server)


__all__ = ["PolymarketGatewayConnector"]
