"""Aster Pro gateway gRPC client-stub spec, built generically by ``GatewayClient``."""

from almanak.connectors._strategy_base.gateway_stub_base import GatewayStubSpec
from almanak.connectors.aster_perps.gateway_client import SERVICE_NAME
from almanak.connectors.aster_perps.proto import aster_perps_pb2_grpc

GATEWAY_STUB_SPEC = GatewayStubSpec(
    service_name=SERVICE_NAME,
    stub_factory=lambda channel: aster_perps_pb2_grpc.AsterPerpsServiceStub(channel),
)
