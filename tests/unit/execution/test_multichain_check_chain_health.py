"""Branch coverage for MultiChainOrchestrator.check_chain_health.

Covers both modes with mocked probes (no chain, no gateway):

- gateway mode: per-chain eth_blockNumber RPC through the gateway client,
  success-flag mapping, transport exception -> unhealthy, and the
  empty-chain-list edge;
- config mode: per-chain executor ``check_connection`` probes, plus
  executor-lookup and probe failures -> unhealthy.

Construction seams follow test_multichain_execute_sequence.py (MagicMock
config) and test_multichain_gateway.py (from_gateway with a mock client).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.multichain import MultiChainOrchestrator

WALLET = "0x1234567890abcdef1234567890abcdef12345678"


def _gateway_orchestrator(client, chains):
    return MultiChainOrchestrator.from_gateway(
        gateway_client=client,
        chains=chains,
        wallet_address=WALLET,
    )


class TestGatewayMode:
    def test_maps_rpc_success_flags_and_transport_errors(self):
        client = MagicMock()
        requests: list[tuple] = []

        def _call(request, timeout=None):
            requests.append((request, timeout))
            if request.chain == "base":
                return MagicMock(success=True)
            if request.chain == "arbitrum":
                return MagicMock(success=False)
            raise RuntimeError("rpc transport down")

        client.rpc.Call.side_effect = _call
        orchestrator = _gateway_orchestrator(client, ["Base", "ARBITRUM", "ethereum"])

        health = asyncio.run(orchestrator.check_chain_health())

        assert health == {"base": True, "arbitrum": False, "ethereum": False}
        # Chains are lower-cased at construction and probed in order.
        assert [request.chain for request, _ in requests] == ["base", "arbitrum", "ethereum"]
        for request, timeout in requests:
            assert request.method == "eth_blockNumber"
            assert request.params == "[]"
            assert timeout == 10.0

    def test_no_chains_yields_empty_health(self):
        client = MagicMock()
        orchestrator = _gateway_orchestrator(client, [])
        assert asyncio.run(orchestrator.check_chain_health()) == {}
        client.rpc.Call.assert_not_called()


class TestConfigMode:
    @pytest.fixture
    def orchestrator(self):
        config = MagicMock()
        config.chains = ["base", "arbitrum", "ethereum"]
        return MultiChainOrchestrator(config=config)

    def test_maps_executor_probe_results(self, orchestrator, monkeypatch):
        healthy = MagicMock()
        healthy.check_connection = AsyncMock(return_value=True)
        degraded = MagicMock()
        degraded.check_connection = AsyncMock(return_value=False)
        probe_fails = MagicMock()
        probe_fails.check_connection = AsyncMock(side_effect=ConnectionError("probe timeout"))
        executors = {"base": healthy, "arbitrum": degraded, "ethereum": probe_fails}
        monkeypatch.setattr(orchestrator, "_get_executor", lambda chain: executors[chain])

        health = asyncio.run(orchestrator.check_chain_health())

        assert health == {"base": True, "arbitrum": False, "ethereum": False}
        healthy.check_connection.assert_awaited_once()
        degraded.check_connection.assert_awaited_once()
        probe_fails.check_connection.assert_awaited_once()

    def test_executor_lookup_failure_marks_chain_unhealthy(self, orchestrator, monkeypatch):
        def _get_executor(chain):
            raise ValueError(f"no executor for {chain}")

        monkeypatch.setattr(orchestrator, "_get_executor", _get_executor)

        health = asyncio.run(orchestrator.check_chain_health())

        assert health == {"base": False, "arbitrum": False, "ethereum": False}
