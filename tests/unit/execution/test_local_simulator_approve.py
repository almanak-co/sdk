"""Tests for LocalSimulator approve simulation skip.

Verifies that:
1. Approve transactions SKIP eth_estimateGas entirely when compiler gas_limit is available
   (prevents hangs on Anvil forks with problematic contract storage — VIB-422)
2. Approve transactions WITHOUT gas_limit fall through to eth_estimateGas (with warning)
3. Non-approve transactions that fail still cause simulation failure
4. All approve selectors (ERC20, ERC1155, TraderJoe V2) are detected and skipped
5. Approve TXs in multi-TX bundles are executed for state setup even when skipped
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.interfaces import (
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.simulator.local import LocalSimulator

# ERC20 approve(address,uint256) selector
APPROVE_SELECTOR = "0x095ea7b3"
# ERC1155 setApprovalForAll(address,bool) selector
SET_APPROVAL_FOR_ALL_SELECTOR = "0xa22cb465"
# TraderJoe V2 LBPair approveForAll(address,bool) selector
TRADERJOE_APPROVE_FOR_ALL_SELECTOR = "0xe584b654"
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


def _make_set_approval_for_all_tx(gas_limit: int = 65000) -> UnsignedTransaction:
    """Create an ERC1155 setApprovalForAll transaction (e.g. TraderJoe V2 LBPair)."""
    return _make_tx(data=SET_APPROVAL_FOR_ALL_SELECTOR + "0" * 56, gas_limit=gas_limit)


def _make_traderjoe_approve_for_all_tx(gas_limit: int = 50000) -> UnsignedTransaction:
    """Create a TraderJoe V2 LBPair approveForAll transaction."""
    return _make_tx(data=TRADERJOE_APPROVE_FOR_ALL_SELECTOR + "0" * 56, gas_limit=gas_limit)


class TestApproveSkipsSimulation:
    """Approve transactions with compiler gas_limit skip eth_estimateGas entirely.

    This prevents hangs caused by Anvil failing to fetch contract storage
    (e.g., TraderJoe V2 LBPair on Avalanche — approveForAll hangs indefinitely
    while Anvil tries to retrieve hundreds of bin storage slots).
    Approve gas is well-known (~30-55K) so compiler limits are safe.
    """

    @pytest.mark.asyncio
    async def test_approve_skips_estimate_gas_uses_compiler_limit(self):
        """Approve with gas_limit should skip eth_estimateGas entirely."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=46000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_approve_tx(gas_limit=65000)
        result = await sim.simulate([tx], chain="ethereum")

        assert result.success
        # Should use compiler gas_limit (65000), NOT the estimate (46000)
        assert result.gas_estimates == [65000]
        # eth_estimateGas should NOT be called
        mock_web3.eth.estimate_gas.assert_not_called()

    @pytest.mark.asyncio
    async def test_approve_skip_uses_exact_compiler_value(self):
        """Approve skip should use the exact compiler gas_limit value."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=52000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        # Use a distinctive value to prove it comes from the compiler
        tx = _make_approve_tx(gas_limit=123456)
        result = await sim.simulate([tx], chain="ethereum")

        assert result.success
        assert result.gas_estimates == [123456]
        mock_web3.eth.estimate_gas.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_approve_tx_not_skipped(self):
        """Non-approve TXs should NOT be skipped — estimation called normally."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=180000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)
        result = await sim.simulate([tx], chain="ethereum")

        assert result.success
        assert result.gas_estimates == [180000]
        mock_web3.eth.estimate_gas.assert_called_once()

    def test_short_data_not_detected_as_approve(self):
        """TX with data shorter than 10 chars should not be detected as approve."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")
        tx = _make_tx(data="0x1234", gas_limit=100000)
        assert not sim._is_approve_tx(tx)


class TestApproveFallback:
    """When approve has no gas_limit and eth_estimateGas also fails, fallback handling."""

    @pytest.mark.asyncio
    async def test_approve_with_gas_limit_never_needs_estimation_fallback(self):
        """Approve with gas_limit skips estimation entirely — no fallback needed."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        # Even if estimation would fail, it should never be called
        mock_web3.eth.estimate_gas = AsyncMock(side_effect=Exception("would hang"))
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        connector_gas_limit = 75000
        tx = _make_approve_tx(gas_limit=connector_gas_limit)
        result = await sim.simulate([tx], chain="avalanche")

        assert result.success
        assert result.gas_estimates == [connector_gas_limit]
        mock_web3.eth.estimate_gas.assert_not_called()

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
    """Test approve + swap bundles (the most common multi-tx pattern).

    Approve TXs skip estimation entirely (VIB-422). Non-first TXs also skip
    estimation because they depend on state changes from prior TXs.
    """

    @pytest.mark.asyncio
    async def test_approve_then_swap_no_estimation_calls(self):
        """In approve+swap bundle, approve skips estimation, swap skips as non-first."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=46000)
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        approve_tx = _make_approve_tx(gas_limit=65000)
        swap_tx = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)

        result = await sim.simulate([approve_tx, swap_tx], chain="arbitrum")

        assert result.success
        # Approve uses compiler gas_limit (65000), swap uses compiler gas_limit (200000)
        assert result.gas_estimates == [65000, 200000]
        # No RPC estimation calls — approve skipped, swap skipped as non-first
        mock_web3.eth.estimate_gas.assert_not_called()

    @pytest.mark.asyncio
    async def test_approve_executed_for_state_setup_in_bundle(self):
        """Approve TX in a bundle is executed for state setup even though estimation is skipped."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        approve_tx = _make_approve_tx(gas_limit=65000)
        swap_tx = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)

        result = await sim.simulate([approve_tx, swap_tx], chain="arbitrum")

        assert result.success
        # Approve TX should be executed for state setup (it's not the last TX)
        assert mock_web3.eth.send_transaction.call_count == 1


class TestERC1155SetApprovalForAll:
    """ERC1155 setApprovalForAll gets the same skip treatment as ERC20 approve."""

    @pytest.mark.asyncio
    async def test_set_approval_for_all_detected_as_approval(self):
        """setApprovalForAll (0xa22cb465) should be detected as an approval call."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")
        tx = _make_set_approval_for_all_tx(gas_limit=65000)
        assert sim._is_approve_tx(tx)

    @pytest.mark.asyncio
    async def test_set_approval_for_all_skips_estimation(self):
        """setApprovalForAll with gas_limit should skip eth_estimateGas."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=48000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_set_approval_for_all_tx(gas_limit=65000)
        result = await sim.simulate([tx], chain="avalanche")

        assert result.success
        assert result.gas_estimates == [65000]
        mock_web3.eth.estimate_gas.assert_not_called()

    @pytest.mark.asyncio
    async def test_set_approval_for_all_then_remove_liquidity(self):
        """setApprovalForAll + removeLiquidity: both use compiler gas_limits."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        approve_for_all_tx = _make_set_approval_for_all_tx(gas_limit=450_000)
        remove_liquidity_tx = _make_tx(
            data="0xc2e3140e" + "0" * 56, gas_limit=500_000
        )  # removeLiquidity

        result = await sim.simulate([approve_for_all_tx, remove_liquidity_tx], chain="avalanche")

        assert result.success
        assert result.gas_estimates == [450_000, 500_000]


class TestTraderJoeV2ApproveForAll:
    """TraderJoe V2 LBPair.approveForAll gets the same skip as ERC20 approve.

    This is the exact scenario from VIB-422: approveForAll on LBPair hangs during
    simulation because Anvil can't fetch the contract's storage from Avalanche RPC.
    Skipping simulation entirely prevents the hang.
    """

    @pytest.mark.asyncio
    async def test_traderjoe_approve_for_all_detected_as_approval(self):
        """approveForAll (0xe584b654) should be detected as an approval call."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")
        tx = _make_traderjoe_approve_for_all_tx(gas_limit=50000)
        assert sim._is_approve_tx(tx)

    @pytest.mark.asyncio
    async def test_traderjoe_approve_for_all_skips_estimation(self):
        """approveForAll with gas_limit skips eth_estimateGas (the VIB-422 fix)."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        # This would hang indefinitely on Avalanche — but it should never be called
        mock_web3.eth.estimate_gas = AsyncMock(side_effect=Exception("would hang"))
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_traderjoe_approve_for_all_tx(gas_limit=50_000)
        result = await sim.simulate([tx], chain="avalanche")

        assert result.success
        assert result.gas_estimates == [50_000]
        mock_web3.eth.estimate_gas.assert_not_called()

    @pytest.mark.asyncio
    async def test_traderjoe_approve_for_all_then_remove_liquidity(self):
        """approveForAll + removeLiquidity LP_CLOSE bundle works without hanging."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        approve_tx = _make_traderjoe_approve_for_all_tx(gas_limit=50_000)
        remove_liquidity_tx = _make_tx(
            data="0xc2e3140e" + "0" * 56, gas_limit=500_000
        )  # removeLiquidity

        result = await sim.simulate([approve_tx, remove_liquidity_tx], chain="avalanche")

        assert result.success
        assert result.gas_estimates == [50_000, 500_000]


class TestMultiTxSimulationSkip:
    """Multi-TX bundles skip estimation for non-first TXs.

    Combined with the approve skip (VIB-422), this means:
    - Approve TX at position 0: skipped (approve skip)
    - Non-approve TX at position 0: estimated normally
    - Any TX at position > 0: skipped (multi-TX skip)
    """

    @pytest.mark.asyncio
    async def test_multi_tx_approve_first_skips_all_estimation(self):
        """When first TX is approve, no estimation calls are made at all."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=46000)
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx1 = _make_approve_tx(gas_limit=65000)
        tx2 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=250000)

        result = await sim.simulate([tx1, tx2], chain="arbitrum")

        assert result.success
        # No estimation calls — approve skipped, swap skipped as non-first
        mock_web3.eth.estimate_gas.assert_not_called()

    @pytest.mark.asyncio
    async def test_multi_tx_non_approve_first_still_estimated(self):
        """When first TX is NOT approve, it should still be estimated normally."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=180000)
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx1 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)
        tx2 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=250000)

        result = await sim.simulate([tx1, tx2], chain="arbitrum")

        assert result.success
        # First TX estimated (180000), second uses compiler gas_limit (250000)
        assert result.gas_estimates == [180000, 250000]
        assert mock_web3.eth.estimate_gas.call_count == 1

    @pytest.mark.asyncio
    async def test_multi_tx_approve_uses_compiler_gas_limits(self):
        """Approve first TX uses compiler gas_limit, second TX also uses compiler gas_limit."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx1 = _make_approve_tx(gas_limit=65000)
        tx2 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=884_000)

        result = await sim.simulate([tx1, tx2], chain="arbitrum")

        assert result.success
        assert result.gas_estimates == [65000, 884_000]

    @pytest.mark.asyncio
    async def test_single_non_approve_tx_still_estimated(self):
        """Single non-approve TX bundles should still use eth_estimateGas normally."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=180000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)

        result = await sim.simulate([tx], chain="arbitrum")

        assert result.success
        assert result.gas_estimates == [180000]
        assert mock_web3.eth.estimate_gas.call_count == 1

    @pytest.mark.asyncio
    async def test_three_tx_bundle_approve_first_skips_all(self):
        """In a 3-TX bundle starting with approve, all use compiler gas_limits."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx1 = _make_approve_tx(gas_limit=65000)  # Approve token A
        tx2 = _make_approve_tx(gas_limit=65000)  # Approve token B
        tx3 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=400000)  # addLiquidity

        result = await sim.simulate([tx1, tx2, tx3], chain="base")

        assert result.success
        # All use compiler gas_limits (approve skip + multi-TX skip)
        assert result.gas_estimates == [65000, 65000, 400000]

    @pytest.mark.asyncio
    async def test_three_tx_bundle_non_last_txs_executed_for_state(self):
        """In a 3-TX bundle, non-last TXs are executed for state setup."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx1 = _make_approve_tx(gas_limit=65000)
        tx2 = _make_approve_tx(gas_limit=65000)
        tx3 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=400000)

        result = await sim.simulate([tx1, tx2, tx3], chain="base")

        assert result.success
        # TX 1 executed for state, TX 2 executed for state (both non-last), TX 3 is last (not executed)
        assert mock_web3.eth.send_transaction.call_count == 2

    @pytest.mark.asyncio
    async def test_approve_execution_failure_stops_bundle(self):
        """If approve TX execution fails during state setup, bundle simulation fails."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 0})  # Reverted
        sim._web3 = mock_web3

        tx1 = _make_approve_tx(gas_limit=65000)
        tx2 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)

        result = await sim.simulate([tx1, tx2], chain="arbitrum")

        assert not result.success
        assert "Approve transaction" in result.revert_reason


class TestStateSetupTimeout:
    """Tests for state setup transaction timeout (VIB-1842)."""

    def test_state_setup_timeout_is_at_least_30s(self):
        """State setup timeout must be >= 30s for slower chains (Avalanche, Ethereum)."""
        from almanak.framework.execution.simulator.local import _STATE_SETUP_TX_TIMEOUT

        assert _STATE_SETUP_TX_TIMEOUT >= 30, (
            f"State setup timeout is {_STATE_SETUP_TX_TIMEOUT}s, must be >= 30s "
            "to accommodate Avalanche/Ethereum Anvil forks (VIB-1842)"
        )


class TestExecuteTxGasPricing:
    """Tests for explicit gas pricing in _execute_tx (VIB-1831).

    _execute_tx must set gasPrice=0 to bypass web3.py's EIP-1559 middleware
    which calls eth_feeHistory and can hang on Anvil forks with high base fees.
    """

    @pytest.mark.asyncio
    async def test_execute_tx_sets_gas_price_fallback(self):
        """_execute_tx should fall back to gasPrice=1gwei when baseFee query fails (VIB-1831).

        Wei(0) is rejected by Anvil 0.3.x, so the fallback must be 1 gwei (safe for all versions).
        """
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x

        # Capture the tx_params passed to send_transaction
        captured_params = {}

        async def capture_send_tx(params):
            captured_params.update(params)
            return b"\x00" * 32

        mock_web3.eth.send_transaction = AsyncMock(side_effect=capture_send_tx)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)
        success, error = await sim._execute_tx(tx, gas_limit=200000)

        assert success
        assert error is None
        # gasPrice must be explicitly set to bypass EIP-1559 middleware (VIB-1831)
        assert "gasPrice" in captured_params, (
            "_execute_tx must set gasPrice to bypass EIP-1559 middleware (VIB-1831)"
        )
        # Fallback is 1 gwei (not 0) to avoid Anvil 0.3.x rejection
        assert captured_params["gasPrice"] == 1_000_000_000

    @pytest.mark.asyncio
    async def test_execute_tx_no_eip1559_fields(self):
        """_execute_tx should NOT set maxFeePerGas or maxPriorityFeePerGas."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x

        captured_params = {}

        async def capture_send_tx(params):
            captured_params.update(params)
            return b"\x00" * 32

        mock_web3.eth.send_transaction = AsyncMock(side_effect=capture_send_tx)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)
        await sim._execute_tx(tx, gas_limit=200000)

        # Legacy gasPrice=0 means EIP-1559 fields should not be present
        assert "maxFeePerGas" not in captured_params
        assert "maxPriorityFeePerGas" not in captured_params

    @pytest.mark.asyncio
    async def test_execute_tx_in_multi_tx_bundle_uses_gas_price(self):
        """In a 3-TX bundle, state-setup TXs use legacy gasPrice (VIB-1831 regression guard).

        When baseFee query fails, fallback is 1 gwei (not 0) to avoid Anvil 0.3.x rejection.
        """
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})

        sent_params_list = []

        async def capture_send_tx(params):
            sent_params_list.append(dict(params))
            return b"\x00" * 32

        mock_web3.eth.send_transaction = AsyncMock(side_effect=capture_send_tx)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx1 = _make_approve_tx(gas_limit=65000)
        tx2 = _make_approve_tx(gas_limit=65000)
        tx3 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=400000)

        result = await sim.simulate([tx1, tx2, tx3], chain="ethereum")

        assert result.success
        # TX 1 and TX 2 are executed for state setup (TX 3 is last, not executed)
        assert len(sent_params_list) == 2
        for i, params in enumerate(sent_params_list):
            assert "gasPrice" in params, (
                f"State-setup TX {i} must set gasPrice to bypass EIP-1559 (VIB-1831)"
            )
            # Fallback is 1 gwei when baseFee query fails
            assert params["gasPrice"] == 1_000_000_000


class TestStateSetupGasBuffer:
    """Tests for gas buffer applied during state-setup TX execution (VIB-572).

    eth_estimateGas returns the minimum gas at the current block, but
    send_transaction mines a new block which can shift opcode costs.
    _execute_tx applies a 20% buffer to prevent consistent reverts
    during multi-TX state setup (e.g., V3 LP_CLOSE decreaseLiquidity).
    """

    @pytest.mark.asyncio
    async def test_execute_tx_applies_gas_buffer(self):
        """_execute_tx should apply 20% gas buffer to the provided gas limit."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x

        captured_params = {}

        async def capture_send_tx(params):
            captured_params.update(params)
            return b"\x00" * 32

        mock_web3.eth.send_transaction = AsyncMock(side_effect=capture_send_tx)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=200000)
        success, error = await sim._execute_tx(tx, gas_limit=200000)

        assert success
        assert error is None
        # Gas limit should be 200000 * 1.2 = 240000 (20% buffer)
        assert captured_params["gas"] == 240000, (
            f"Expected gas={240000} (200000 * 1.2), got {captured_params['gas']}. "
            "State-setup TXs need a buffer because mining a new block can shift "
            "opcode costs vs eth_estimateGas (VIB-572)."
        )

    @pytest.mark.asyncio
    async def test_gas_buffer_in_multi_tx_bundle(self):
        """In a 3-TX bundle, state-setup TXs get buffered gas but returned estimates are raw."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})

        sent_params_list = []

        async def capture_send_tx(params):
            sent_params_list.append(dict(params))
            return b"\x00" * 32

        mock_web3.eth.send_transaction = AsyncMock(side_effect=capture_send_tx)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        mock_web3.eth.estimate_gas = AsyncMock(return_value=150000)
        sim._web3 = mock_web3

        # 3-TX bundle simulating V3 LP_CLOSE: decreaseLiquidity + collect + burn
        tx1 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=250000)
        tx2 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=100000)
        tx3 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=100000)

        result = await sim.simulate([tx1, tx2, tx3], chain="ethereum")

        assert result.success
        # TX1 is estimated via eth_estimateGas (first TX), then executed for state setup
        # TX2 uses compiler gas_limit (multi-TX dependent), then executed for state setup
        # TX3 is last (not executed for state setup)
        assert len(sent_params_list) == 2

        # TX1 gas: eth_estimateGas returned 150000, buffered to 180000
        assert sent_params_list[0]["gas"] == 180000, (
            f"TX1 state-setup gas should be 150000 * 1.2 = 180000, got {sent_params_list[0]['gas']}"
        )
        # TX2 gas: compiler gas_limit 100000, buffered to 120000
        assert sent_params_list[1]["gas"] == 120000, (
            f"TX2 state-setup gas should be 100000 * 1.2 = 120000, got {sent_params_list[1]['gas']}"
        )

        # Returned gas estimates should be RAW (unbuffered) for the orchestrator
        # TX1: estimated at 150000, TX2: compiler gas_limit 100000, TX3: compiler gas_limit 100000
        assert result.gas_estimates == [150000, 100000, 100000]

    @pytest.mark.asyncio
    async def test_gas_buffer_constant_value(self):
        """Verify the gas buffer constant is 1.2 (20%)."""
        from almanak.framework.execution.simulator.local import _STATE_SETUP_GAS_BUFFER

        assert _STATE_SETUP_GAS_BUFFER == 1.2, (
            f"Gas buffer should be 1.2 (20%), got {_STATE_SETUP_GAS_BUFFER}"
        )


class TestDecodeRevertPayload:
    """Pure-function tests for `_decode_revert_payload`.

    The decoder turns the raw `data` field from an eth_call revert into a
    human-readable reason. Each test pins one branch of the selector dispatch.
    """

    def _build_error_string_payload(self, message: str) -> str:
        """Build a real Error(string) ABI payload for the given message."""
        from eth_abi.abi import encode as abi_encode

        encoded = abi_encode(["string"], [message]).hex()
        return "0x08c379a0" + encoded

    def test_decodes_error_string(self):
        from almanak.framework.execution.simulator.local import _decode_revert_payload

        payload = self._build_error_string_payload("Blacklistable: account is blacklisted")
        assert _decode_revert_payload(payload) == "Blacklistable: account is blacklisted"

    def test_decodes_panic(self):
        from almanak.framework.execution.simulator.local import _decode_revert_payload

        # Panic(uint256) selector + code 0x11 (arithmetic overflow)
        panic_code = (0x11).to_bytes(32, "big").hex()
        payload = "0x4e487b71" + panic_code
        assert _decode_revert_payload(payload) == "Panic(0x11)"

    def test_unknown_selector_reports_selector_with_args_preview(self):
        from almanak.framework.execution.simulator.local import _decode_revert_payload

        # Synthetic custom error with one uint256 arg
        payload = "0xfe47b63c" + (42).to_bytes(32, "big").hex()
        decoded = _decode_revert_payload(payload)
        assert decoded is not None
        assert "0xfe47b63c" in decoded
        # First 32 bytes of args (the encoded 42) should appear in the preview
        assert "00000000000000000000000000000000000000000000000000000000000000002a"[-64:] in decoded

    def test_empty_and_malformed_inputs_return_none(self):
        from almanak.framework.execution.simulator.local import _decode_revert_payload

        # None of these should decode — empty, just-the-0x, truncated below 10 chars
        for bad in (None, "", "0x", "0x12"):
            assert _decode_revert_payload(bad) is None, f"Expected None for {bad!r}"


class TestExplainRevert:
    """Tests for `LocalSimulator._explain_revert` — the eth_call replay path
    that surfaces the on-chain revert reason instead of a bare 'Transaction
    reverted' string.
    """

    @pytest.mark.asyncio
    async def test_decodes_string_revert_from_data(self):
        """ContractLogicError with Error(string) `.data` should yield the decoded reason."""
        from eth_abi.abi import encode as abi_encode
        from web3.exceptions import ContractLogicError

        from almanak.framework.execution.simulator.local import LocalSimulator

        sim = LocalSimulator(rpc_url="http://localhost:8545")
        encoded = abi_encode(["string"], ["Blacklistable: account is blacklisted"]).hex()
        data = "0x08c379a0" + encoded

        mock_web3 = MagicMock()
        mock_web3.eth.call = AsyncMock(side_effect=ContractLogicError("execution reverted", data=data))

        result = await sim._explain_revert(mock_web3, {"from": "0xa", "to": "0xb", "data": "0x"})
        assert result == "Transaction reverted: Blacklistable: account is blacklisted"

    @pytest.mark.asyncio
    async def test_no_data_returns_no_reason(self):
        """ContractLogicError with empty `.data` should yield '(no reason)'."""
        from web3.exceptions import ContractLogicError

        from almanak.framework.execution.simulator.local import LocalSimulator

        sim = LocalSimulator(rpc_url="http://localhost:8545")
        mock_web3 = MagicMock()
        mock_web3.eth.call = AsyncMock(side_effect=ContractLogicError("execution reverted", data="0x"))

        result = await sim._explain_revert(mock_web3, {"from": "0xa", "to": "0xb", "data": "0x"})
        assert result == "Transaction reverted (no reason)"

    @pytest.mark.asyncio
    async def test_replay_not_reverting_is_reported(self):
        """If eth_call does NOT revert (state shifted), report that explicitly
        instead of pretending we have a reason.
        """
        from almanak.framework.execution.simulator.local import LocalSimulator

        sim = LocalSimulator(rpc_url="http://localhost:8545")
        mock_web3 = MagicMock()
        mock_web3.eth.call = AsyncMock(return_value=b"")  # call succeeds

        result = await sim._explain_revert(mock_web3, {"from": "0xa", "to": "0xb", "data": "0x"})
        assert "did not revert" in result


class TestEstimateGasDecodesCustomError:
    """Regression guard: gas estimation must decode known custom-error selectors
    to their friendly name via the shared submitter registry, not surface them
    as the raw 4-byte selector or web3.py's args-tuple repr.

    Without this, every Aave V3 health-factor revert surfaces as
    ``('0x6679996d', '0x6679996d')`` and forces callers to decode by hand.
    """

    @pytest.mark.asyncio
    async def test_aave_health_factor_revert_decodes_to_friendly_name(self):
        from web3.exceptions import ContractCustomError

        from almanak.framework.execution.simulator.local import LocalSimulator

        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.to_checksum_address = lambda x: x
        # Aave V3 Pool reverts with this selector when a withdraw would drop
        # health factor below the liquidation threshold.
        mock_web3.eth.estimate_gas = AsyncMock(side_effect=ContractCustomError("0x6679996d", data="0x6679996d"))
        sim._web3 = mock_web3

        tx = _make_tx(data="0x69328dec", gas_limit=200000)  # Aave withdraw selector
        gas, error = await sim._estimate_gas(tx)

        assert gas == 0
        assert error == "HealthFactorLowerThanLiquidationThreshold()", (
            f"Expected decoded Aave error, got {error!r}"
        )
