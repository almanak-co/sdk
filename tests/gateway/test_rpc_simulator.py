"""Sequential node results must cover the exact ordered bundle and parent."""

from copy import deepcopy
from dataclasses import replace
from threading import Event
from unittest.mock import MagicMock, patch

import pytest

from almanak.core.rpc_network import Network
from almanak.framework.execution.interfaces import SimulationError, SimulationResult
from almanak.gateway.services.rpc_simulator import GatewayRpcSimulator
from tests.unit.execution.test_local_simulator_approve import _make_approve_tx, _make_tx

HASH = "0x" + "a" * 64
OTHER_HASH = "0x" + "b" * 64


@pytest.fixture
def node():
    return make_node()


def make_node():
    header = {"number": "0x64", "hash": HASH, "timestamp": "0x6aa1a122"}
    block = {
        "parentHash": HASH,
        "calls": [
            {"status": "0x1", "gasUsed": "0xa000", "returnData": "0x", "logs": []},
            {"status": "0x1", "gasUsed": "0xb000", "returnData": "0x", "logs": []},
        ],
    }
    web3 = MagicMock()
    requests = []
    output = {"block": block, "closing_hash": HASH, "rpc_error": None}

    def rpc(method, params):
        requests.append((method, deepcopy(params)))
        if method == "eth_getBlockByNumber":
            value = dict(header)
            if params[0] != "latest":
                assert params[0] == "0x64"
                value["hash"] = output["closing_hash"]
            return {"result": value}
        assert method == "eth_simulateV1"
        if output["rpc_error"] is not None:
            return {"error": output["rpc_error"]}
        return {"result": [output["block"]]}

    web3.provider.make_request.side_effect = rpc
    with patch("almanak.gateway.services.rpc_simulator.get_cached_web3", return_value=web3):
        simulator = GatewayRpcSimulator(chain="robinhood", network=Network.MAINNET)
    txs = [replace(_make_approve_tx(), chain_id=4663), replace(_make_tx(), chain_id=4663)]
    return simulator, txs, output, requests


@pytest.mark.asyncio
async def test_ordered_calls_are_measured_and_round_trip_with_evidence(node):
    simulator, txs, output, requests = node
    before = deepcopy(txs)
    result = await simulator.simulate(txs, "robinhood")
    assert result.success and result.simulated
    assert result.gas_estimates == [40960, 45056]
    assert result.evidence["evaluated_indices"] == [0, 1]
    assert result.evidence["parent_hash"] == HASH
    assert SimulationResult.from_dict(result.to_dict()).evidence == result.evidence
    assert txs == before
    method, params = requests[1]
    assert method == "eth_simulateV1" and params[1] == "0x64"
    assert params[0]["blockStateCalls"][0]["calls"] == [
        {"from": tx.from_address, "to": tx.to, "value": hex(tx.value), "data": tx.data} for tx in txs
    ]
    assert [method for method, _ in requests] == ["eth_getBlockByNumber", "eth_simulateV1", "eth_getBlockByNumber"]


@pytest.mark.asyncio
async def test_a_reverting_dependent_call_is_measured_failure(node):
    simulator, txs, output, _ = node
    output["block"]["calls"][1].update(status="0x0", error={"code": 3, "message": "execution reverted"})
    result = await simulator.simulate(txs, "robinhood")
    assert result.simulated and not result.success
    assert "call 1" in result.revert_reason
    assert result.evidence["calls"][1]["status"] == "0x0"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "missing_call",
        "extra_call",
        "missing_status",
        "false_status",
        "missing_gas",
        "zero_gas",
        "wrong_parent",
        "reorg",
    ],
)
async def test_partial_or_incoherent_simulation_cannot_be_success(node, mutation):
    simulator, txs, output, _ = node
    calls = output["block"]["calls"]
    if mutation == "missing_call":
        calls.pop()
    elif mutation == "extra_call":
        calls.append(deepcopy(calls[0]))
    elif mutation == "missing_status":
        calls[1].pop("status")
    elif mutation == "false_status":
        calls[1]["status"] = True
    elif mutation == "missing_gas":
        calls[1].pop("gasUsed")
    elif mutation == "zero_gas":
        calls[1]["gasUsed"] = "0x0"
    elif mutation == "wrong_parent":
        output["block"]["parentHash"] = OTHER_HASH
    else:
        output["closing_hash"] = OTHER_HASH
    with pytest.raises(SimulationError, match="Incomplete sequential simulation"):
        await simulator.simulate(txs, "robinhood")


@pytest.mark.asyncio
async def test_unsupported_rpc_is_not_a_measured_contract_revert(node):
    simulator, txs, output, _ = node
    output["rpc_error"] = {"code": -32601, "message": "method not found"}
    with pytest.raises(SimulationError, match="method not found") as exc:
        await simulator.simulate(txs, "robinhood")
    assert exc.value.recoverable


@pytest.mark.asyncio
@pytest.mark.parametrize("chain,chain_id", [("base", 4663), ("robinhood", 8453)])
async def test_simulation_cannot_relabel_another_chain(node, chain, chain_id):
    simulator, txs, _, requests = node
    txs[0] = replace(txs[0], chain_id=chain_id)
    with pytest.raises(SimulationError, match="chain differs"):
        await simulator.simulate(txs, chain)
    assert requests == []


@pytest.mark.parametrize("timeout", [True, 0, -1, float("nan"), float("inf"), "40"])
def test_invalid_timeout_refuses_before_constructing_rpc_provider(timeout):
    with patch("almanak.gateway.services.rpc_simulator.get_cached_web3") as provider:
        with pytest.raises(ValueError, match="finite positive"):
            GatewayRpcSimulator(chain="robinhood", network=Network.MAINNET, timeout_seconds=timeout)
    provider.assert_not_called()


@pytest.mark.asyncio
async def test_timeout_does_not_allow_overlapping_rpc_workers(node):
    simulator, txs, _, requests = node
    simulator._timeout = 0.05
    entered, release, finished = Event(), Event(), Event()
    original = simulator._simulate

    def delayed(*args):
        entered.set()
        try:
            assert release.wait(timeout=5)
            return original(*args)
        finally:
            finished.set()

    with patch.object(simulator, "_simulate", side_effect=delayed) as worker:
        try:
            with pytest.raises(SimulationError, match="timed out; outcome is unmeasured"):
                await simulator.simulate(txs, "robinhood")
            assert entered.is_set()
            with pytest.raises(SimulationError, match="still running"):
                await simulator.simulate(txs, "robinhood")
            assert worker.call_count == 1
            assert requests == []
        finally:
            release.set()
            import asyncio

            assert await asyncio.to_thread(finished.wait, 5)
    simulator._timeout = 5
    result = await simulator.simulate(txs, "robinhood")
    assert result.success and result.simulated
