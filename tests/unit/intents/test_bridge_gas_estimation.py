"""Tests for bridge gas estimation (VIB-1885).

Verifies that:
1. The bridge_deposit default gas estimate is sufficient for real-world Across deposits
2. eth_estimateGas is attempted for all TXs in multi-TX bundles (not just the first)
"""

import pytest

from almanak.framework.intents.compiler import get_gas_estimate


class TestBridgeDepositGasDefault:
    """Verify bridge_deposit default gas estimate is realistic."""

    def test_bridge_deposit_default_covers_across_deposits(self):
        """Bridge deposit default must be >= 800K to cover Across deposit paths.

        Previous value of 450K (buffered to 675K with 1.5x) was insufficient
        for some Across deposit paths, causing out-of-gas reverts.
        """
        gas = get_gas_estimate("arbitrum", "bridge_deposit")
        assert gas >= 800000, (
            f"bridge_deposit default ({gas}) too low for Across deposits. "
            "Must be >= 800K (buffered to 1.2M with 1.5x). "
            "See VIB-1885 for real-world out-of-gas evidence."
        )

    def test_bridge_deposit_default_same_across_chains(self):
        """Bridge deposit gas should be consistent across all chains."""
        chains = ["arbitrum", "base", "optimism", "ethereum", "avalanche"]
        estimates = {chain: get_gas_estimate(chain, "bridge_deposit") for chain in chains}

        # All should be the same (no chain-specific override for bridge_deposit)
        values = set(estimates.values())
        assert len(values) == 1, (
            f"bridge_deposit gas should be consistent across chains: {estimates}"
        )

    def test_bridge_deposit_not_excessively_high(self):
        """Bridge deposit gas shouldn't be so high it wastes native tokens on gas."""
        gas = get_gas_estimate("arbitrum", "bridge_deposit")
        assert gas <= 2_000_000, (
            f"bridge_deposit default ({gas}) seems excessively high. "
            "Should be 800K-1.5M for most bridge operations."
        )


class TestMultiTxGasEstimation:
    """Verify orchestrator attempts gas estimation for all TXs in bundles."""

    @pytest.mark.asyncio
    async def test_gas_estimation_attempted_for_non_first_tx(self):
        """eth_estimateGas should be attempted for bridge deposit TX (idx > 0).

        Previously, the orchestrator skipped all non-first TXs in multi-TX
        bundles, so bridge deposits (approve + deposit) never got estimated.
        """
        from unittest.mock import AsyncMock, MagicMock, patch

        from almanak.framework.execution.interfaces import TransactionType, UnsignedTransaction
        from almanak.framework.execution.orchestrator import ExecutionOrchestrator

        # Create a 2-TX bundle: approve + bridge deposit
        txs = [
            UnsignedTransaction(
                to="0x" + "a1" * 20,
                value=0,
                data="0x095ea7b3" + "00" * 64,  # approve
                chain_id=42161,
                gas_limit=65000,
                tx_type=TransactionType.EIP_1559,
                from_address="0x" + "cd" * 20,
                max_fee_per_gas=1000000000,
                max_priority_fee_per_gas=0,
            ),
            UnsignedTransaction(
                to="0x" + "b2" * 20,
                value=0,
                data="0x12345678" + "00" * 64,  # bridge deposit
                chain_id=42161,
                gas_limit=800000,
                tx_type=TransactionType.EIP_1559,
                from_address="0x" + "cd" * 20,
                max_fee_per_gas=1000000000,
                max_priority_fee_per_gas=0,
            ),
        ]

        # Mock Web3 estimate_gas to return a higher value for the bridge TX
        mock_web3 = AsyncMock()
        mock_web3.to_checksum_address = MagicMock(side_effect=lambda x: x)
        mock_web3.eth.estimate_gas = AsyncMock(return_value=950000)

        mock_signer = MagicMock()
        mock_signer.address = "0x" + "cd" * 20

        orchestrator = ExecutionOrchestrator.__new__(ExecutionOrchestrator)
        orchestrator.rpc_url = "http://localhost:8545"
        orchestrator.signer = mock_signer
        orchestrator.gas_buffer_multiplier = 1.5
        orchestrator._get_web3 = AsyncMock(return_value=mock_web3)

        from almanak.framework.execution.orchestrator import ExecutionContext

        context = ExecutionContext(
            chain="arbitrum",
            wallet_address="0x" + "cd" * 20,
        )

        updated_txs, warnings = await orchestrator._maybe_estimate_gas_limits(txs, context)

        # Both TXs should have had gas estimation attempted
        assert mock_web3.eth.estimate_gas.call_count == 2, (
            f"Expected eth_estimateGas to be called for both TXs, "
            f"but was called {mock_web3.eth.estimate_gas.call_count} time(s)"
        )

        # Both should have updated gas estimates (950K * 1.5 = 1.425M > compiler limits)
        for i, tx in enumerate(updated_txs):
            assert tx.gas_limit == int(950000 * 1.5), (
                f"TX[{i}] gas_limit should be updated to {int(950000 * 1.5)}, got {tx.gas_limit}"
            )

    @pytest.mark.asyncio
    async def test_gas_estimation_fallback_on_revert(self):
        """When eth_estimateGas fails for a dependent TX, compiler gas is used."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from almanak.framework.execution.interfaces import TransactionType, UnsignedTransaction
        from almanak.framework.execution.orchestrator import ExecutionOrchestrator

        txs = [
            UnsignedTransaction(
                to="0x" + "a1" * 20,
                value=0,
                data="0x095ea7b3" + "00" * 64,
                chain_id=42161,
                gas_limit=65000,
                tx_type=TransactionType.EIP_1559,
                from_address="0x" + "cd" * 20,
                max_fee_per_gas=1000000000,
                max_priority_fee_per_gas=0,
            ),
            UnsignedTransaction(
                to="0x" + "b2" * 20,
                value=0,
                data="0x12345678" + "00" * 64,
                chain_id=42161,
                gas_limit=800000,
                tx_type=TransactionType.EIP_1559,
                from_address="0x" + "cd" * 20,
                max_fee_per_gas=1000000000,
                max_priority_fee_per_gas=0,
            ),
        ]

        # First TX succeeds, second TX fails (depends on approve state)
        mock_web3 = AsyncMock()
        mock_web3.to_checksum_address = MagicMock(side_effect=lambda x: x)
        mock_web3.eth.estimate_gas = AsyncMock(
            side_effect=[950000, Exception("STF: transfer from failed")]
        )

        mock_signer = MagicMock()
        mock_signer.address = "0x" + "cd" * 20

        orchestrator = ExecutionOrchestrator.__new__(ExecutionOrchestrator)
        orchestrator.rpc_url = "http://localhost:8545"
        orchestrator.signer = mock_signer
        orchestrator.gas_buffer_multiplier = 1.5
        orchestrator._get_web3 = AsyncMock(return_value=mock_web3)

        from almanak.framework.execution.orchestrator import ExecutionContext

        context = ExecutionContext(
            chain="arbitrum",
            wallet_address="0x" + "cd" * 20,
        )

        updated_txs, warnings = await orchestrator._maybe_estimate_gas_limits(txs, context)

        # First TX: updated from estimation (950K * 1.5 = 1.425M)
        assert updated_txs[0].gas_limit == int(950000 * 1.5)
        # Second TX: kept compiler gas limit (800K) due to STF revert
        assert updated_txs[1].gas_limit == 800000
        # Warning should be recorded
        assert len(warnings) == 1
        assert "STF" in warnings[0]
