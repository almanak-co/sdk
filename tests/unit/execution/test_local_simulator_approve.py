"""Tests for LocalSimulator approve gas estimation.

Verifies that:
1. Approve transactions use eth_estimateGas (not a hardcoded constant)
2. If eth_estimateGas fails for approve, falls back to connector-provided gas_limit
3. Non-approve transactions that fail still cause simulation failure
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.interfaces import (
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.simulator.local import LocalSimulator

# ERC20 approve(address,uint256) selector
APPROVE_SELECTOR = "0x095ea7b3"
# Random non-approve selector (e.g., transfer)
TRANSFER_SELECTOR = "0xa9059cbb"


def _make_tx(data: str = TRANSFER_SELECTOR + "0" * 56, gas_limit: int = 100000) -> UnsignedTransaction:
    """Create a test transaction with LEGACY type for simplicity."""
    return UnsignedTransaction(
        to="0x" + "a" * 40,
        value=0,
        data=data,
        chain_id=1,
        gas_limit=gas_limit,
        gas_price=1_000_000_000,
        tx_type=TransactionType.LEGACY,
        from_address="0x" + "b" * 40,
    )


def _make_approve_tx(gas_limit: int = 65000) -> UnsignedTransaction:
    """Create an ERC20 approve transaction."""
    return _make_tx(data=APPROVE_SELECTOR + "0" * 56, gas_limit=gas_limit)


class TestApproveUsesEstimateGas:
    """Approve transactions should use eth_estimateGas, not a hardcoded constant."""

    @pytest.mark.asyncio
    async def test_approve_calls_estimate_gas(self):
        """eth_estimateGas should be called for approve transactions."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=46000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_approve_tx(gas_limit=65000)
        result = await sim.simulate([tx], chain="ethereum")

        assert result.success
        # Should use the actual estimate (46000), not the old hardcoded 80000
        assert result.gas_estimates == [46000]
        mock_web3.eth.estimate_gas.assert_called_once()

    @pytest.mark.asyncio
    async def test_approve_no_longer_hardcoded_80000(self):
        """The old hardcoded APPROVE_GAS=80000 should not appear in results."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        # Return a value different from 80000 to prove it's not hardcoded
        mock_web3.eth.estimate_gas = AsyncMock(return_value=52000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_approve_tx(gas_limit=65000)
        result = await sim.simulate([tx], chain="ethereum")

        assert result.success
        assert result.gas_estimates == [52000]
        assert 80000 not in result.gas_estimates

    @pytest.mark.asyncio
    async def test_approve_returns_raw_estimate_even_with_buffer_arg(self):
        """LocalSimulator should return raw estimates; orchestrator applies the buffer."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.1)

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=50000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_approve_tx()
        result = await sim.simulate([tx], chain="ethereum")

        assert result.success
        assert result.gas_estimates == [50000]


class TestApproveFallback:
    """If eth_estimateGas fails for approve, fall back to connector-provided gas_limit."""

    @pytest.mark.asyncio
    async def test_approve_fallback_to_connector_gas_limit(self):
        """When eth_estimateGas fails for approve, use tx.gas_limit from the connector."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(side_effect=Exception("execution reverted"))
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        connector_gas_limit = 75000
        tx = _make_approve_tx(gas_limit=connector_gas_limit)
        result = await sim.simulate([tx], chain="avalanche")

        assert result.success
        # Should fall back to the connector-provided gas_limit
        assert result.gas_estimates == [connector_gas_limit]

    @pytest.mark.asyncio
    async def test_approve_fallback_uses_exact_connector_value(self):
        """Fallback should use the exact connector gas_limit, not 80000 or any hardcoded value."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(side_effect=Exception("proxy contract error"))
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        # Use a distinctive value to prove it comes from the connector
        connector_gas_limit = 123456
        tx = _make_approve_tx(gas_limit=connector_gas_limit)
        result = await sim.simulate([tx], chain="avalanche")

        assert result.success
        assert result.gas_estimates == [123456]

    @pytest.mark.asyncio
    async def test_non_approve_failure_still_fails(self):
        """Non-approve tx failures should NOT fall back -- they fail the simulation."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(side_effect=Exception("execution reverted"))
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=100000)
        result = await sim.simulate([tx], chain="ethereum")

        assert not result.success
        assert result.revert_reason is not None


class TestMixedBundle:
    """Test approve + swap bundles (the most common multi-tx pattern)."""

    @pytest.mark.asyncio
    async def test_approve_then_swap_both_estimated(self):
        """Both approve and swap should use eth_estimateGas in a bundle."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        # First call for approve, second for swap
        mock_web3.eth.estimate_gas = AsyncMock(side_effect=[46000, 180000])
        mock_web3.to_checksum_address = lambda x: x
        # Snapshot support for multi-tx
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        # Execute approve for state setup (not last tx)
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        approve_tx = _make_approve_tx(gas_limit=65000)
        swap_tx = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)

        result = await sim.simulate([approve_tx, swap_tx], chain="arbitrum")

        assert result.success
        assert result.gas_estimates == [46000, 180000]
        assert mock_web3.eth.estimate_gas.call_count == 2

    @pytest.mark.asyncio
    async def test_approve_fallback_then_swap_estimated(self):
        """Approve fallback + successful swap estimation should work together."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        # Approve fails eth_estimateGas, swap succeeds
        mock_web3.eth.estimate_gas = AsyncMock(
            side_effect=[Exception("proxy contract error"), 180000]
        )
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        # Execute approve for state setup using fallback gas_limit
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        approve_tx = _make_approve_tx(gas_limit=75000)
        swap_tx = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)

        result = await sim.simulate([approve_tx, swap_tx], chain="avalanche")

        assert result.success
        # Approve falls back to connector gas_limit (75000), swap is estimated (180000)
        assert result.gas_estimates == [75000, 180000]
