"""Gateway-side connector binding for Aster Pro."""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import GatewayServicerCapability
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors.aster_perps.proto import aster_perps_pb2_grpc

from .service import AsterPerpsServiceServicer


class AsterPerpsGatewayConnector(GatewayConnector, GatewayServicerCapability):
    """Registers the Aster Pro gRPC servicer on the gateway."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aster_perps")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def __init__(self) -> None:
        self._servicer: AsterPerpsServiceServicer | None = None

    @property
    def servicer(self) -> AsterPerpsServiceServicer | None:
        return self._servicer

    def register_servicers(self, server: Any, settings: Any) -> None:
        self._servicer = AsterPerpsServiceServicer(settings)
        aster_perps_pb2_grpc.add_AsterPerpsServiceServicer_to_server(self._servicer, server)


__all__ = ["AsterPerpsGatewayConnector"]
