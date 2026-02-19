"""Deterministic bridge smoke test via managed gateway startup.

This test validates end-to-end wiring for:
1. ManagedGateway startup
2. gRPC ExecutionService CompileIntent request path
3. Bridge intent type normalization + bridge payload handling

It stubs the compiler to avoid external RPC/API dependencies.
"""

import json
import socket

import grpc

from almanak.framework.intents.compiler import CompilationResult, CompilationStatus
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.managed import ManagedGateway
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.services.execution_service import ExecutionServiceServicer


def _free_port() -> int:
    """Get a free localhost TCP port allocated by the OS."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_managed_gateway_bridge_compile_smoke(monkeypatch):
    """Managed gateway serves bridge compile requests through gRPC."""

    class _FakeCompiler:
        def compile(self, intent):
            assert intent.__class__.__name__ == "BridgeIntent"
            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                action_bundle=ActionBundle(
                    intent_type="BRIDGE",
                    transactions=[
                        {
                            "to": "0x1111111111111111111111111111111111111111",
                            "value": "0",
                            "data": "0x",
                            "gas_estimate": 1,
                            "description": "bridge mock tx",
                            "tx_type": "bridge_deposit",
                        }
                    ],
                    metadata={
                        "from_chain": "base",
                        "to_chain": "arbitrum",
                        "token": "USDC",
                        "amount": "10",
                        "bridge": "Across",
                        "estimated_time": 60,
                        "fee": "0.01",
                        "is_cross_chain": True,
                    },
                ),
                intent_id="bridge-smoke",
            )

    monkeypatch.setattr(ExecutionServiceServicer, "_get_compiler", lambda *_args, **_kwargs: _FakeCompiler())

    grpc_port = _free_port()
    settings = GatewaySettings(
        grpc_port=grpc_port,
        metrics_enabled=False,
        audit_enabled=False,
        allow_insecure=True,
    )

    with ManagedGateway(settings):
        channel = grpc.insecure_channel(f"127.0.0.1:{grpc_port}")
        stub = gateway_pb2_grpc.ExecutionServiceStub(channel)

        request = gateway_pb2.CompileIntentRequest(
            intent_type="BRIDGE",
            intent_data=json.dumps(
                {
                    "token": "USDC",
                    "amount": "10",
                    "from_chain": "base",
                    "to_chain": "arbitrum",
                }
            ).encode("utf-8"),
            chain="base",
            wallet_address="0x1111111111111111111111111111111111111111",
        )

        response = stub.CompileIntent(request)
        assert response.success is True

        bundle = json.loads(response.action_bundle.decode("utf-8"))
        assert bundle["intent_type"] == "BRIDGE"
        assert bundle["metadata"]["bridge"] == "Across"

        channel.close()
