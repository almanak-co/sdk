"""Tests for GatewayExecutionOrchestrator execute timeout configuration.

Verifies that:
1. Execute calls use _execute_timeout (not _timeout) to give gas estimation + confirmation time
2. Default execute timeout is larger than the TX confirmation timeout per chain
3. Custom execute_timeout parameter is respected
4. CompileIntent still uses _timeout (not the larger execute timeout)
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.gas.constants import (
    CHAIN_GRPC_EXECUTE_TIMEOUTS,
    CHAIN_TX_TIMEOUTS,
    DEFAULT_GRPC_EXECUTE_TIMEOUT_SECONDS,
    DEFAULT_TX_TIMEOUT_SECONDS,
)
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionOrchestrator


def _make_client() -> MagicMock:
    """Create a minimal mock GatewayClient."""
    client = MagicMock()
    return client


class TestExecuteTimeoutDefaults:
    """Execute timeout defaults should be larger than TX confirmation timeouts."""

    def test_execute_timeout_larger_than_tx_timeout_for_avalanche(self):
        """Avalanche execute timeout must exceed TX confirmation timeout (was causing DEADLINE_EXCEEDED)."""
        orch = GatewayExecutionOrchestrator(_make_client(), chain="avalanche")
        assert orch._execute_timeout > orch._timeout
        # Specifically, execute timeout should be >= 300s for Avalanche
        assert orch._execute_timeout >= 300

    def test_execute_timeout_larger_than_tx_timeout_for_ethereum(self):
        """Ethereum execute timeout must exceed TX confirmation timeout."""
        orch = GatewayExecutionOrchestrator(_make_client(), chain="ethereum")
        assert orch._execute_timeout > orch._timeout

    def test_execute_timeout_larger_than_tx_timeout_for_arbitrum(self):
        """Arbitrum execute timeout must exceed TX confirmation timeout."""
        orch = GatewayExecutionOrchestrator(_make_client(), chain="arbitrum")
        assert orch._execute_timeout > orch._timeout

    def test_default_execute_timeout_is_configurable(self):
        """DEFAULT_GRPC_EXECUTE_TIMEOUT_SECONDS must exceed DEFAULT_TX_TIMEOUT_SECONDS."""
        assert DEFAULT_GRPC_EXECUTE_TIMEOUT_SECONDS > DEFAULT_TX_TIMEOUT_SECONDS

    def test_all_chain_execute_timeouts_exceed_tx_timeouts(self):
        """Every chain with an explicit execute timeout must exceed its TX confirmation timeout."""
        for chain, execute_timeout in CHAIN_GRPC_EXECUTE_TIMEOUTS.items():
            tx_timeout = CHAIN_TX_TIMEOUTS.get(chain, DEFAULT_TX_TIMEOUT_SECONDS)
            assert execute_timeout > tx_timeout, (
                f"Chain {chain}: execute_timeout={execute_timeout} must exceed tx_timeout={tx_timeout}"
            )


class TestCustomTimeoutParameter:
    """Custom execute_timeout parameter should be respected."""

    def test_custom_execute_timeout_is_used(self):
        """Explicitly passed execute_timeout should override the default."""
        orch = GatewayExecutionOrchestrator(_make_client(), chain="avalanche", execute_timeout=999.0)
        assert orch._execute_timeout == 999.0

    def test_custom_timeout_and_execute_timeout_are_independent(self):
        """timeout and execute_timeout parameters are independent."""
        orch = GatewayExecutionOrchestrator(
            _make_client(),
            chain="avalanche",
            timeout=50.0,
            execute_timeout=600.0,
        )
        assert orch._timeout == 50.0
        assert orch._execute_timeout == 600.0

    def test_default_timeout_unchanged(self):
        """The default compile/status timeout (_timeout) still uses CHAIN_TX_TIMEOUTS."""
        orch = GatewayExecutionOrchestrator(_make_client(), chain="avalanche")
        expected = CHAIN_TX_TIMEOUTS.get("avalanche", DEFAULT_TX_TIMEOUT_SECONDS)
        assert orch._timeout == expected


class TestExecuteUsesExecuteTimeout:
    """The Execute gRPC call must use _execute_timeout, not _timeout."""

    @pytest.mark.asyncio
    async def test_execute_call_uses_execute_timeout(self):
        """Execute gRPC call should be made with _execute_timeout."""
        client = _make_client()
        mock_response = MagicMock()
        mock_response.success = True
        mock_response.tx_hashes = []
        mock_response.total_gas_used = 0
        mock_response.receipts = b"[]"
        mock_response.execution_id = "test-id"
        mock_response.error = ""
        mock_response.error_code = ""
        mock_response.extraction_warnings = []
        client.execution.Execute.return_value = mock_response

        orch = GatewayExecutionOrchestrator(
            client,
            chain="avalanche",
            timeout=120.0,
            execute_timeout=600.0,
        )
        bundle = {"transactions": []}
        await orch.execute(bundle, strategy_id="test", intent_id="test", wallet_address="0x" + "a" * 40)

        # Verify Execute was called with execute_timeout (600), not timeout (120)
        call_kwargs = client.execution.Execute.call_args
        assert call_kwargs.kwargs.get("timeout") == 600.0 or (
            call_kwargs.args and call_kwargs.args[-1] == 600.0
        ), f"Expected timeout=600.0, got call args: {call_kwargs}"


class TestChainReceiptTimeouts:
    """Per-chain receipt timeout defaults (VIB-1580)."""

    def test_bsc_gets_longer_default_timeout(self):
        """BSC should default to 300s receipt timeout, not 120s."""
        from almanak.framework.execution.chain_executor import ChainExecutorConfig

        cfg = ChainExecutorConfig(
            chain="bsc",
            rpc_url="http://localhost:8545",
            private_key="0x" + "a" * 64,
        )
        assert cfg.tx_timeout_seconds == 300

    def test_avalanche_gets_longer_default_timeout(self):
        """Avalanche should default to 180s receipt timeout."""
        from almanak.framework.execution.chain_executor import ChainExecutorConfig

        cfg = ChainExecutorConfig(
            chain="avalanche",
            rpc_url="http://localhost:8545",
            private_key="0x" + "a" * 64,
        )
        assert cfg.tx_timeout_seconds == 180

    def test_arbitrum_keeps_default_timeout(self):
        """Fast chains should keep the default 120s timeout."""
        from almanak.framework.execution.chain_executor import ChainExecutorConfig

        cfg = ChainExecutorConfig(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            private_key="0x" + "a" * 64,
        )
        assert cfg.tx_timeout_seconds == 120

    def test_explicit_timeout_is_respected_for_bsc(self):
        """Explicit tx_timeout_seconds overrides the per-chain default."""
        from almanak.framework.execution.chain_executor import ChainExecutorConfig

        cfg = ChainExecutorConfig(
            chain="bsc",
            rpc_url="http://localhost:8545",
            private_key="0x" + "a" * 64,
            tx_timeout_seconds=60,
        )
        # User explicitly set 60s -- should NOT be overridden by the BSC default (300s)
        assert cfg.tx_timeout_seconds == 60
