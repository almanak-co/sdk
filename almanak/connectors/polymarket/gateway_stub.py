"""Polymarket gateway gRPC client-stub spec (VIB-4989).

Publishes :data:`GATEWAY_STUB_SPEC` for
:class:`~almanak.connectors._strategy_base.gateway_stub_registry.GatewayStubRegistry`
so the framework ``GatewayClient`` builds the Polymarket service stub generically
at connect time, instead of importing ``polymarket_pb2_grpc`` itself.
"""

from almanak.connectors._strategy_base.gateway_stub_base import GatewayStubSpec
from almanak.connectors.polymarket.proto import polymarket_pb2_grpc

GATEWAY_STUB_SPEC = GatewayStubSpec(
    service_name="polymarket",
    stub_factory=lambda channel: polymarket_pb2_grpc.PolymarketServiceStub(channel),
)
