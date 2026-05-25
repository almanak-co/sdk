"""Gateway-side connector binding for Enso (VIB-4810).

Declares the ``GatewayServicerCapability`` so the gateway boot loop can
register the Enso gRPC servicer via the connector registry instead of
hand-wiring it in :mod:`almanak.gateway.server`.

Phase 1+2 — ``gateway/server.py`` switches to instantiating the servicer
through ``GATEWAY_REGISTRY``; the explicit
``gateway_pb2_grpc.add_EnsoServiceServicer_to_server`` call stays here
inside ``register_servicers``. Phase 4 collapses ``server.py`` to a loop
over ``GATEWAY_REGISTRY.capability_providers(GatewayServicerCapability)``.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayServicerCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.gateway.proto import gateway_pb2_grpc

from .service import EnsoServiceServicer


class EnsoGatewayConnector(GatewayConnector, GatewayServicerCapability):
    """Gateway-side connector for Enso."""

    protocol: ClassVar[ProtocolName] = ProtocolName("enso")
    kind: ClassVar[ProtocolKind] = ProtocolKind.CROSS_CHAIN_SWAP

    def __init__(self) -> None:
        self._servicer: EnsoServiceServicer | None = None

    @property
    def servicer(self) -> EnsoServiceServicer | None:
        """Constructed servicer instance, populated by ``register_servicers``.

        ``server.py`` holds onto this reference for the shutdown path
        (which iterates servicers and calls ``close()`` on each).
        """
        return self._servicer

    def register_servicers(self, server: Any, settings: Any) -> None:
        """Construct the Enso servicer and bind it to ``server``.

        Stores the constructed servicer on the connector instance so
        ``gateway/server.py`` can wire shutdown cleanup against the
        same object it would have built directly.
        """
        self._servicer = EnsoServiceServicer(settings)
        gateway_pb2_grpc.add_EnsoServiceServicer_to_server(self._servicer, server)


__all__ = ["EnsoGatewayConnector"]
