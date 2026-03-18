"""Tests for nonce cache management in ExecutionOrchestrator.

Verifies that the local nonce cache does NOT drift ahead of the on-chain nonce
when transactions fail. Regression test for VIB-1449: `ax swap` nonce/gas
estimation failures caused by optimistic nonce caching.

Root cause: _assign_nonces() was updating _local_nonce before transactions
were confirmed on-chain. If the transaction failed, the cache kept the
inflated value, causing subsequent calls to use "nonce too high" values.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.interfaces import (
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
)


def _make_unsigned_tx(idx: int = 0, description: str = "test") -> UnsignedTransaction:
    """Create a minimal UnsignedTransaction for testing."""
    return UnsignedTransaction(
        to=f"0x{'0' * 39}{idx}",
        value=0,
        data="0x1234",
        chain_id=42161,
        gas_limit=100_000,
        max_fee_per_gas=1_000_000_000,
        max_priority_fee_per_gas=100_000_000,
        tx_type=TransactionType.EIP_1559,
        metadata={"description": description},
    )


def _make_context(wallet: str = "0x" + "a" * 40) -> ExecutionContext:
    return ExecutionContext(wallet_address=wallet, chain="arbitrum")


class TestNonceDrift:
    """Tests verifying nonce cache is NOT updated optimistically."""

    @pytest.fixture
    def orchestrator(self):
        """Create a minimal orchestrator with mocked dependencies."""
        orch = ExecutionOrchestrator.__new__(ExecutionOrchestrator)
        orch.rpc_url = "http://localhost:8545"
        orch.chain = "arbitrum"
        orch.signer = MagicMock()
        orch.signer.__class__.__name__ = "LocalKeySigner"
        orch.signer.address = "0x" + "a" * 40
        orch.gas_buffer_multiplier = 1.5
        orch._local_nonce = {}
        orch._web3 = None
        return orch

    @pytest.mark.asyncio
    async def test_assign_nonces_does_not_update_cache(self, orchestrator):
        """_assign_nonces() should NOT update _local_nonce.

        The cache should only be updated after confirmed on-chain success.
        """
        mock_web3 = AsyncMock()
        mock_web3.eth.get_transaction_count = AsyncMock(return_value=7)
        mock_web3.to_checksum_address = lambda addr: addr

        with patch.object(orchestrator, "_get_web3", return_value=mock_web3):
            txs = [_make_unsigned_tx(0, "approve"), _make_unsigned_tx(1, "swap")]
            context = _make_context()

            result = await orchestrator._assign_nonces(txs, context)

        # Nonces should be assigned sequentially
        assert result[0].nonce == 7
        assert result[1].nonce == 8

        # But _local_nonce should NOT have been updated
        wallet_key = context.wallet_address.lower()
        assert wallet_key not in orchestrator._local_nonce, (
            "_local_nonce should not be updated by _assign_nonces(). "
            "It should only be updated after confirmed on-chain success."
        )

    @pytest.mark.asyncio
    async def test_nonce_cache_not_inflated_after_failure(self, orchestrator):
        """After a failed TX, _local_nonce should NOT be inflated.

        This is the core regression: before the fix, _assign_nonces()
        set _local_nonce to current_nonce + len(txs), so after a failed
        2-TX bundle (nonces 7,8), _local_nonce would be 9. On retry,
        max(chain_nonce=7, local_nonce=9) = 9 -> "nonce too high".
        """
        wallet_key = ("0x" + "a" * 40).lower()

        # Simulate: first call assigns nonces 7,8 but they never confirm
        mock_web3 = AsyncMock()
        mock_web3.eth.get_transaction_count = AsyncMock(return_value=7)
        mock_web3.to_checksum_address = lambda addr: addr

        with patch.object(orchestrator, "_get_web3", return_value=mock_web3):
            txs = [_make_unsigned_tx(0, "approve"), _make_unsigned_tx(1, "swap")]
            context = _make_context()
            await orchestrator._assign_nonces(txs, context)

        # Cache should be clean (no optimistic update)
        assert wallet_key not in orchestrator._local_nonce

        # Second call should use fresh chain nonce (still 7)
        with patch.object(orchestrator, "_get_web3", return_value=mock_web3):
            txs2 = [_make_unsigned_tx(0, "approve"), _make_unsigned_tx(1, "swap")]
            result2 = await orchestrator._assign_nonces(txs2, context)

        # Should get nonces 7,8 again (not 9,10)
        assert result2[0].nonce == 7
        assert result2[1].nonce == 8

    def test_reset_nonce_cache_specific_address(self, orchestrator):
        """reset_nonce_cache() should clear a specific address."""
        wallet = "0x" + "a" * 40
        orchestrator._local_nonce[wallet.lower()] = 42

        orchestrator.reset_nonce_cache(wallet)
        assert wallet.lower() not in orchestrator._local_nonce

    def test_reset_nonce_cache_all(self, orchestrator):
        """reset_nonce_cache() without args should clear all addresses."""
        orchestrator._local_nonce["0x" + "a" * 40] = 42
        orchestrator._local_nonce["0x" + "b" * 40] = 99

        orchestrator.reset_nonce_cache()
        assert len(orchestrator._local_nonce) == 0

    def test_reset_nonce_cache_nonexistent_address(self, orchestrator):
        """reset_nonce_cache() should not raise for unknown addresses."""
        orchestrator.reset_nonce_cache("0x" + "f" * 40)  # Should not raise

    @pytest.mark.asyncio
    async def test_local_nonce_used_when_higher_than_chain(self, orchestrator):
        """When _local_nonce > chain nonce, use local (rapid sequential calls)."""
        wallet_key = ("0x" + "a" * 40).lower()

        # Simulate: a previous success already set _local_nonce to 10
        orchestrator._local_nonce[wallet_key] = 10

        mock_web3 = AsyncMock()
        mock_web3.eth.get_transaction_count = AsyncMock(return_value=8)  # Chain behind
        mock_web3.to_checksum_address = lambda addr: addr

        with patch.object(orchestrator, "_get_web3", return_value=mock_web3):
            txs = [_make_unsigned_tx(0, "swap")]
            context = _make_context()
            result = await orchestrator._assign_nonces(txs, context)

        # Should use max(8, 10) = 10
        assert result[0].nonce == 10
