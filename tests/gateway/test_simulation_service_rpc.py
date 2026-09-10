"""Standalone simulation preserves sequential execution evidence over protobuf."""

import json
from types import SimpleNamespace
from unittest.mock import patch

import grpc
import pytest

from almanak.core.rpc_network import Network
from almanak.framework.execution.simulator.config import SimulationConfig
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.simulation_service import SimulationServiceServicer
from tests.gateway.grpc_harness import make_grpc_context
from tests.gateway.test_rpc_simulator import make_node


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "revert", "unsupported"])
async def test_rpc_service_preserves_measured_outcome_and_evidence(outcome):
    simulator, txs, output, requests = make_node()
    if outcome == "revert":
        output["block"]["calls"][1].update(status="0x0", error={"message": "reverted"})
    elif outcome == "unsupported":
        output["rpc_error"] = {"code": -32601, "message": "method not found"}
    config = SimulationConfig(enabled=True, backend="rpc")
    with patch.object(SimulationConfig, "from_env", return_value=config):
        service = SimulationServiceServicer(SimpleNamespace(network=Network.MAINNET))
    request = gateway_pb2.SimulateBundleRequest(
        chain="robinhood",
        transactions=[
            gateway_pb2.SimulateTransaction(
                from_address=tx.from_address, to_address=tx.to, data=tx.data, value=str(tx.value)
            )
            for tx in txs
        ],
    )
    context = make_grpc_context()
    with patch("almanak.gateway.services.simulation_service.GatewayRpcSimulator", return_value=simulator) as backend:
        response = await service.SimulateBundle(request, context)
    backend.assert_called_once_with(chain="robinhood", network=Network.MAINNET, timeout_seconds=config.timeout_seconds)
    wire = gateway_pb2.SimulateBundleResponse.FromString(response.SerializeToString())
    assert wire.success is (outcome == "success")
    assert wire.simulated is (outcome != "unsupported")
    if outcome == "unsupported":
        assert not wire.simulation_evidence_json
        assert "method not found" in wire.error
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
    else:
        evidence = json.loads(wire.simulation_evidence_json)
        assert evidence["evaluated_indices"] == [0, 1]
        assert evidence["calls"][1]["status"] == ("0x0" if outcome == "revert" else "0x1")
        assert wire.simulator_used == "rpc"
        assert requests[1][0] == "eth_simulateV1"


@pytest.mark.parametrize("chain,count", [("solana", 1), ("robinhood", 65)])
def test_rpc_service_refuses_unsupported_request_shape(chain, count):
    with patch.object(SimulationConfig, "from_env", return_value=SimulationConfig(backend="rpc")):
        service = SimulationServiceServicer(SimpleNamespace(network=Network.MAINNET))
    with pytest.raises(ValueError):
        service._select_simulator(chain, count, False, "")


@pytest.mark.asyncio
@pytest.mark.parametrize("preferred", ["typo", " rpc", "auto"])
@pytest.mark.parametrize("has_transactions", [False, True])
async def test_unknown_requested_simulator_never_falls_back(preferred, has_transactions):
    with patch.object(SimulationConfig, "from_env", return_value=SimulationConfig(backend="rpc")):
        service = SimulationServiceServicer(SimpleNamespace(network=Network.MAINNET, alchemy_api_key="configured"))
    request = gateway_pb2.SimulateBundleRequest(
        chain="base",
        simulator=preferred,
        transactions=[gateway_pb2.SimulateTransaction()] if has_transactions else [],
    )
    context = make_grpc_context()
    with patch.object(service, "_select_simulator") as select:
        response = await service.SimulateBundle(request, context)
    select.assert_not_called()
    assert not response.success and not response.simulated
    assert "Unsupported simulator" in response.error
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
