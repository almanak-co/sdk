"""Tests for GatewayExecutionOrchestrator Solana tx-hash handling (VIB-369).

Verifies:
1. Solana chain preserves base58 signatures (no 0x prefix)
2. EVM chains still get 0x-prefixed hashes
3. to_outcome() works for Solana results
"""

import json
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.gateway_orchestrator import (
    GatewayExecutionOrchestrator,
    GatewayExecutionResult,
)
from almanak.gateway.proto import gateway_pb2


def _make_client():
    """Create a minimal mock GatewayClient."""
    return MagicMock()


# Solana base58 signature
SOLANA_SIG = "5VERv8NMHKRYsGeYfVb9oKzvoHvU9vE3yo9Xq2Gj8j3B8VeqiZLzQQDCbPVmXNgTjEFGdYkhNmj1PYqC7GsQzXvA"
# EVM tx hash without 0x
EVM_HASH_NO_PREFIX = "a1" * 32
# EVM tx hash with 0x
EVM_HASH_WITH_PREFIX = "0x" + EVM_HASH_NO_PREFIX


class TestSolanaTxHashNormalization:
    """Gateway orchestrator preserves Solana base58 signatures."""

    @pytest.mark.asyncio
    async def test_solana_chain_preserves_base58(self):
        """Solana chain should NOT add 0x prefix to base58 signatures."""
        client = _make_client()
        orch = GatewayExecutionOrchestrator(client, chain="solana")

        # Mock the gateway response
        mock_response = MagicMock()
        mock_response.success = True
        mock_response.tx_hashes = [SOLANA_SIG]
        mock_response.total_gas_used = 5000
        mock_response.receipts = b"[]"
        mock_response.execution_id = "test-exec-1"
        mock_response.error = ""
        mock_response.error_code = ""

        client.execution.Execute.return_value = mock_response

        result = await orch.execute(
            action_bundle={"actions": []},
            wallet_address="So11111111111111111111111111111111111111112",
        )

        assert result.success is True
        assert result.tx_hashes == [SOLANA_SIG]
        assert not result.tx_hashes[0].startswith("0x")

    @pytest.mark.asyncio
    async def test_evm_chain_adds_0x_prefix(self):
        """EVM chain should add 0x prefix to hashes that lack it."""
        client = _make_client()
        orch = GatewayExecutionOrchestrator(client, chain="arbitrum")

        mock_response = MagicMock()
        mock_response.success = True
        mock_response.tx_hashes = [EVM_HASH_NO_PREFIX]
        mock_response.total_gas_used = 21000
        mock_response.receipts = b"[]"
        mock_response.execution_id = "test-exec-2"
        mock_response.error = ""
        mock_response.error_code = ""

        client.execution.Execute.return_value = mock_response

        result = await orch.execute(
            action_bundle={"actions": []},
            wallet_address="0x" + "ab" * 20,
        )

        assert result.tx_hashes == [EVM_HASH_WITH_PREFIX]

    @pytest.mark.asyncio
    async def test_evm_chain_preserves_existing_0x(self):
        """EVM chain should not double-prefix hashes that already have 0x."""
        client = _make_client()
        orch = GatewayExecutionOrchestrator(client, chain="arbitrum")

        mock_response = MagicMock()
        mock_response.success = True
        mock_response.tx_hashes = [EVM_HASH_WITH_PREFIX]
        mock_response.total_gas_used = 21000
        mock_response.receipts = b"[]"
        mock_response.execution_id = "test-exec-3"
        mock_response.error = ""
        mock_response.error_code = ""

        client.execution.Execute.return_value = mock_response

        result = await orch.execute(
            action_bundle={"actions": []},
            wallet_address="0x" + "ab" * 20,
        )

        assert result.tx_hashes == [EVM_HASH_WITH_PREFIX]
        assert not result.tx_hashes[0].startswith("0x0x")


class TestGatewayExecutionResultSolana:
    """GatewayExecutionResult works correctly for Solana results."""

    def test_tx_hash_property_returns_solana_sig(self):
        """tx_hash property returns the first Solana signature."""
        result = GatewayExecutionResult(
            success=True,
            tx_hashes=[SOLANA_SIG],
            total_gas_used=5000,
            receipts=[],
            execution_id="test-1",
        )
        assert result.tx_hash == SOLANA_SIG

    def test_to_outcome_for_solana(self):
        """to_outcome() produces valid ExecutionOutcome for Solana."""
        result = GatewayExecutionResult(
            success=True,
            tx_hashes=[SOLANA_SIG],
            total_gas_used=5000,
            receipts=[{"slot": 12345}],
            execution_id="test-2",
        )
        outcome = result.to_outcome()

        assert outcome.success is True
        assert outcome.tx_ids == [SOLANA_SIG]
        assert outcome.chain_family == "EVM"  # Currently hardcoded; future: detect from chain

    def test_to_dict_includes_solana_hashes(self):
        """to_dict() serializes Solana signatures correctly."""
        result = GatewayExecutionResult(
            success=True,
            tx_hashes=[SOLANA_SIG],
            total_gas_used=5000,
            receipts=[],
            execution_id="test-3",
        )
        d = result.to_dict()

        assert d["tx_hashes"] == [SOLANA_SIG]
        assert d["success"] is True
