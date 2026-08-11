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
from almanak.framework.execution.submission import ReplayPolicy, TransactionRole, execution_plan_hash
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


def _solana_receipt(*, success: bool = True) -> dict:
    return {
        "signature": SOLANA_SIG,
        "slot": 12345,
        "block_time": 1_700_000_000,
        "fee_lamports": 5000,
        "success": success,
        "err": None if success else {"InstructionError": [0, "Custom"]},
        "logs": [],
        "pre_token_balances": [],
        "post_token_balances": [],
    }


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
        mock_response.receipts = json.dumps([_solana_receipt()]).encode()
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
        assert result.chain_family == "SOLANA"

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

    @pytest.mark.asyncio
    async def test_execution_evidence_survives_proto_client_boundary(self):
        client = _make_client()
        orch = GatewayExecutionOrchestrator(client, chain="arbitrum")
        action_bundle = {"transactions": []}
        plan_hash = execution_plan_hash(action_bundle)
        client.execution.Execute.return_value = gateway_pb2.ExecutionResult(
            success=False,
            tx_hashes=[EVM_HASH_WITH_PREFIX],
            error="reverted",
            execution_plan_hash=plan_hash,
            submission_transactions=[
                gateway_pb2.SubmissionTransactionEvidence(
                    tx_id=EVM_HASH_WITH_PREFIX,
                    role=gateway_pb2.EXECUTION_TRANSACTION_ROLE_SETUP_APPROVAL,
                    replay_policy=gateway_pb2.REPLAY_POLICY_RECOMPILE_ONLY,
                )
            ],
        )

        result = await orch.execute(
            action_bundle=action_bundle,
            wallet_address="0x" + "ab" * 20,
        )

        assert result.execution_plan_hash == plan_hash
        assert result.submission_transactions[0].tx_id == EVM_HASH_WITH_PREFIX
        assert result.submission_transactions[0].role is TransactionRole.SETUP_APPROVAL
        assert result.submission_transactions[0].replay_policy is ReplayPolicy.RECOMPILE_ONLY

    @pytest.mark.asyncio
    async def test_mismatched_plan_hash_discards_role_evidence(self):
        client = _make_client()
        orch = GatewayExecutionOrchestrator(client, chain="arbitrum")
        client.execution.Execute.return_value = gateway_pb2.ExecutionResult(
            success=False,
            tx_hashes=[EVM_HASH_WITH_PREFIX],
            error="reverted",
            execution_plan_hash="a" * 64,
            submission_transactions=[
                gateway_pb2.SubmissionTransactionEvidence(
                    tx_id=EVM_HASH_WITH_PREFIX,
                    role=gateway_pb2.EXECUTION_TRANSACTION_ROLE_SETUP_APPROVAL,
                    replay_policy=gateway_pb2.REPLAY_POLICY_RECOMPILE_ONLY,
                )
            ],
        )

        result = await orch.execute(
            action_bundle={"transactions": [{"tx_type": "swap", "data": "0x1234"}]},
            wallet_address="0x" + "ab" * 20,
        )

        assert result.execution_plan_hash == ""
        assert result.submission_transactions == []


class TestGatewayExecutionResultSolana:
    """GatewayExecutionResult works correctly for Solana results."""

    def test_tx_hash_property_returns_solana_sig(self):
        """tx_hash property returns the first Solana signature."""
        result = GatewayExecutionResult(
            success=True,
            tx_hashes=[SOLANA_SIG],
            total_gas_used=5000,
            receipts=[_solana_receipt()],
            execution_id="test-1",
            chain_family="SOLANA",
        )
        assert result.tx_hash == SOLANA_SIG

    def test_to_outcome_for_solana(self):
        """to_outcome() produces valid ExecutionOutcome for Solana."""
        result = GatewayExecutionResult(
            success=True,
            tx_hashes=[SOLANA_SIG],
            total_gas_used=5000,
            receipts=[_solana_receipt()],
            execution_id="test-2",
            chain_family="SOLANA",
        )
        outcome = result.to_outcome()

        assert outcome.success is True
        assert outcome.tx_ids == [SOLANA_SIG]
        assert outcome.chain_family == "SOLANA"

    def test_to_dict_includes_solana_hashes(self):
        """to_dict() serializes Solana signatures correctly."""
        result = GatewayExecutionResult(
            success=True,
            tx_hashes=[SOLANA_SIG],
            total_gas_used=5000,
            receipts=[_solana_receipt()],
            execution_id="test-3",
            chain_family="SOLANA",
        )
        d = result.to_dict()

        assert d["tx_hashes"] == [SOLANA_SIG]
        assert d["success"] is True

    def test_solana_receipt_identity_mismatch_fails_closed(self):
        receipt = _solana_receipt()
        receipt["signature"] = "different-signature"
        result = GatewayExecutionResult(
            success=True,
            tx_hashes=[SOLANA_SIG],
            total_gas_used=5000,
            receipts=[receipt],
            execution_id="test-mismatch",
            chain_family="SOLANA",
        )

        assert result.success is False
        assert result.error_code == "RECEIPT_SET_INCOMPLETE"

    def test_solana_receipt_converts_fee_lamports_for_ledger_evidence(self):
        result = GatewayExecutionResult(
            success=True,
            tx_hashes=[SOLANA_SIG],
            total_gas_used=5000,
            receipts=[_solana_receipt()],
            execution_id="test-fee",
            chain_family="SOLANA",
        )

        assert result.transaction_results[0].tx_hash == SOLANA_SIG
        assert result.transaction_results[0].gas_used == 5000
        assert result.transaction_results[0].receipt.block_number == 12345
