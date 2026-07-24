"""Branch coverage for MultiChainOrchestrator.initialize.

Covers both modes with mocked probes (no chain, no gateway):

- gateway mode: health-check gate (False -> typed error, transport exception
  -> wrapped error), per-chain orchestrator creation failures aggregated
  into a single MultiChainExecutionError, and the success path;
- config mode: per-chain executor connection probes (success, connect
  failure, executor-lookup exception) and error aggregation;
- idempotency: a second initialize() call short-circuits without re-probing.

Construction seams follow test_multichain_check_chain_health.py (MagicMock
config in config mode, from_gateway with a mock client in gateway mode).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.multichain import (
    MultiChainExecutionError,
    MultiChainOrchestrator,
)

WALLET = "0x1234567890abcdef1234567890abcdef12345678"


def _gateway_orchestrator(client, chains):
    return MultiChainOrchestrator.from_gateway(
        gateway_client=client,
        chains=chains,
        wallet_address=WALLET,
    )


class TestGatewayMode:
    def test_health_check_false_raises_typed_error(self):
        client = MagicMock()
        client.health_check.return_value = False
        orchestrator = _gateway_orchestrator(client, ["base"])

        with pytest.raises(MultiChainExecutionError, match="Gateway health check failed"):
            asyncio.run(orchestrator.initialize())

        assert orchestrator._initialized is False

    def test_health_check_transport_error_is_wrapped(self):
        client = MagicMock()
        client.health_check.side_effect = ConnectionError("gateway unreachable")
        orchestrator = _gateway_orchestrator(client, ["base"])

        with pytest.raises(MultiChainExecutionError, match="Gateway connectivity check failed"):
            asyncio.run(orchestrator.initialize())

        assert orchestrator._initialized is False

    def test_orchestrator_creation_failures_are_aggregated(self, monkeypatch):
        client = MagicMock()
        client.health_check.return_value = True
        orchestrator = _gateway_orchestrator(client, ["base", "arbitrum", "ethereum"])

        def _get_gateway_orchestrator(chain):
            if chain == "base":
                return MagicMock()
            raise ValueError(f"no orchestrator for {chain}")

        monkeypatch.setattr(orchestrator, "_get_gateway_orchestrator", _get_gateway_orchestrator)

        with pytest.raises(MultiChainExecutionError) as exc_info:
            asyncio.run(orchestrator.initialize())

        message = str(exc_info.value)
        assert "Failed to initialize chains" in message
        assert "arbitrum: no orchestrator for arbitrum" in message
        assert "ethereum: no orchestrator for ethereum" in message
        assert "base" not in message.split("Failed to initialize chains:")[1].split("arbitrum")[0]
        assert orchestrator._initialized is False

    def test_success_marks_initialized(self, monkeypatch):
        client = MagicMock()
        client.health_check.return_value = True
        orchestrator = _gateway_orchestrator(client, ["base", "arbitrum"])
        created: list[str] = []
        monkeypatch.setattr(
            orchestrator,
            "_get_gateway_orchestrator",
            lambda chain: created.append(chain) or MagicMock(),
        )

        asyncio.run(orchestrator.initialize())

        assert orchestrator._initialized is True
        assert created == ["base", "arbitrum"]

    def test_second_call_short_circuits(self, monkeypatch):
        client = MagicMock()
        client.health_check.return_value = True
        orchestrator = _gateway_orchestrator(client, ["base"])
        monkeypatch.setattr(orchestrator, "_get_gateway_orchestrator", lambda chain: MagicMock())

        asyncio.run(orchestrator.initialize())
        asyncio.run(orchestrator.initialize())

        # Health check probed exactly once: second call early-returns.
        client.health_check.assert_called_once()


class TestConfigMode:
    @pytest.fixture
    def orchestrator(self):
        config = MagicMock()
        config.chains = ["base", "arbitrum", "ethereum"]
        return MultiChainOrchestrator(config=config)

    def test_all_chains_connect_marks_initialized(self, orchestrator, monkeypatch):
        executor = MagicMock()
        executor.check_connection = AsyncMock(return_value=True)
        monkeypatch.setattr(orchestrator, "_get_executor", lambda chain: executor)

        asyncio.run(orchestrator.initialize())

        assert orchestrator._initialized is True
        assert executor.check_connection.await_count == 3

    def test_failed_connection_and_lookup_error_are_aggregated(self, orchestrator, monkeypatch):
        healthy = MagicMock()
        healthy.check_connection = AsyncMock(return_value=True)
        degraded = MagicMock()
        degraded.check_connection = AsyncMock(return_value=False)

        def _get_executor(chain):
            if chain == "base":
                return healthy
            if chain == "arbitrum":
                return degraded
            raise RuntimeError("bad RPC config")

        monkeypatch.setattr(orchestrator, "_get_executor", _get_executor)

        with pytest.raises(MultiChainExecutionError) as exc_info:
            asyncio.run(orchestrator.initialize())

        message = str(exc_info.value)
        assert "arbitrum: Failed to connect to RPC" in message
        assert "ethereum: bad RPC config" in message
        assert orchestrator._initialized is False
