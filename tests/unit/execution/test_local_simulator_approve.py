"""Tests for LocalSimulator approve gas estimation.

Verifies that:
1. Approve transactions use eth_estimateGas (not a hardcoded constant)
2. If eth_estimateGas fails for approve, falls back to connector-provided gas_limit
3. Non-approve transactions that fail still cause simulation failure
4. ERC1155 setApprovalForAll (0xa22cb465) is treated the same as ERC20 approve
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
    """Test approve + swap bundles (the most common multi-tx pattern).

    For multi-TX bundles, only the first TX is estimated via eth_estimateGas.
    Non-first TXs use the compiler-provided gas_limit because they depend on
    state changes from prior TXs (e.g., approve must execute before swap).
    This mirrors the VIB-157 fix for _maybe_estimate_gas_limits().
    """

    @pytest.mark.asyncio
    async def test_approve_then_swap_only_first_estimated(self):
        """Only the first TX (approve) should call eth_estimateGas in a multi-TX bundle."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        # Only first call (approve) should be made
        mock_web3.eth.estimate_gas = AsyncMock(return_value=46000)
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
        # Approve estimated (46000), swap uses compiler gas_limit (200000)
        assert result.gas_estimates == [46000, 200000]
        # Only 1 RPC call - swap estimation skipped for multi-TX bundle
        assert mock_web3.eth.estimate_gas.call_count == 1

    @pytest.mark.asyncio
    async def test_approve_fallback_then_swap_uses_compiler_limit(self):
        """Approve fallback + swap using compiler gas_limit should work together."""
        sim = LocalSimulator(rpc_url="http://localhost:8545", gas_buffer=1.0)

        mock_web3 = MagicMock()
        # Approve fails eth_estimateGas
        mock_web3.eth.estimate_gas = AsyncMock(
            side_effect=Exception("proxy contract error")
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
        # Approve falls back to connector gas_limit (75000), swap uses compiler gas_limit (200000)
        assert result.gas_estimates == [75000, 200000]


class TestERC1155SetApprovalForAll:
    """ERC1155 setApprovalForAll gets the same fallback treatment as ERC20 approve.

    TraderJoe V2 LP_CLOSE uses setApprovalForAll on the LBPair contract to grant
    the router permission to remove liquidity. On Anvil forks, eth_estimateGas for
    this call can fail with "missing trie node" because the public RPC doesn't have
    state that was created locally by prior LP_OPEN transactions.

    The compiler always provides a safe gas_limit upper bound for these calls.
    """

    @pytest.mark.asyncio
    async def test_set_approval_for_all_detected_as_approval(self):
        """setApprovalForAll (0xa22cb465) should be detected as an approval call."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")
        tx = _make_set_approval_for_all_tx(gas_limit=65000)
        assert sim._is_approve_tx(tx)

    @pytest.mark.asyncio
    async def test_set_approval_for_all_uses_estimate_gas_when_available(self):
        """When eth_estimateGas succeeds for setApprovalForAll, use the estimate."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=48000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_set_approval_for_all_tx(gas_limit=65000)
        result = await sim.simulate([tx], chain="avalanche")

        assert result.success
        assert result.gas_estimates == [48000]

    @pytest.mark.asyncio
    async def test_set_approval_for_all_fallback_on_missing_trie_node(self):
        """When eth_estimateGas fails (e.g. missing trie node), use compiler gas_limit."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(
            side_effect=Exception("missing trie node 3b3aca51...")
        )
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        compiler_gas_limit = 450_000
        tx = _make_set_approval_for_all_tx(gas_limit=compiler_gas_limit)
        result = await sim.simulate([tx], chain="avalanche")

        assert result.success
        # Falls back to compiler gas limit, not a hard failure
        assert result.gas_estimates == [compiler_gas_limit]

    @pytest.mark.asyncio
    async def test_set_approval_for_all_then_remove_liquidity(self):
        """setApprovalForAll fallback + removeLiquidity uses compiler gas_limit."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        # setApprovalForAll fails eth_estimateGas
        mock_web3.eth.estimate_gas = AsyncMock(
            side_effect=Exception("missing trie node")
        )
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
        # setApprovalForAll uses fallback (450K), removeLiquidity uses compiler gas_limit (500K)
        assert result.gas_estimates == [450_000, 500_000]


# TraderJoe V2 LBPair approveForAll(address,bool) selector
TRADERJOE_APPROVE_FOR_ALL_SELECTOR = "0xe584b654"


def _make_traderjoe_approve_for_all_tx(gas_limit: int = 50000) -> UnsignedTransaction:
    """Create a TraderJoe V2 LBPair approveForAll transaction."""
    return _make_tx(data=TRADERJOE_APPROVE_FOR_ALL_SELECTOR + "0" * 56, gas_limit=gas_limit)


class TestTraderJoeV2ApproveForAll:
    """TraderJoe V2 LBPair.approveForAll gets the same fallback as ERC20 approve.

    TraderJoe V2 uses approveForAll(address,bool) (selector 0xe584b654) rather than
    the standard ERC1155 setApprovalForAll(address,bool) (selector 0xa22cb465).
    The VIB-282 fix incorrectly added only 0xa22cb465; VIB-283 adds the correct selector.

    Without this fix, if eth_estimateGas fails for approveForAll during LP_CLOSE
    gas estimation, the simulation fails and the TX is never submitted -- causing
    a permanent hang waiting for a receipt that never arrives.
    """

    @pytest.mark.asyncio
    async def test_traderjoe_approve_for_all_detected_as_approval(self):
        """approveForAll (0xe584b654) should be detected as an approval call."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")
        tx = _make_traderjoe_approve_for_all_tx(gas_limit=50000)
        assert sim._is_approve_tx(tx)

    @pytest.mark.asyncio
    async def test_traderjoe_approve_for_all_uses_estimate_gas_when_available(self):
        """When eth_estimateGas succeeds for approveForAll, use the estimate."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=32000)
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        tx = _make_traderjoe_approve_for_all_tx(gas_limit=50000)
        result = await sim.simulate([tx], chain="avalanche")

        assert result.success
        assert result.gas_estimates == [32000]

    @pytest.mark.asyncio
    async def test_traderjoe_approve_for_all_fallback_on_missing_trie_node(self):
        """When eth_estimateGas fails (e.g. missing trie node), use compiler gas_limit."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(
            side_effect=Exception("missing trie node 3b3aca51...")
        )
        mock_web3.to_checksum_address = lambda x: x
        sim._web3 = mock_web3

        compiler_gas_limit = 50_000
        tx = _make_traderjoe_approve_for_all_tx(gas_limit=compiler_gas_limit)
        result = await sim.simulate([tx], chain="avalanche")

        assert result.success
        assert result.gas_estimates == [compiler_gas_limit]

    @pytest.mark.asyncio
    async def test_traderjoe_approve_for_all_then_remove_liquidity(self):
        """approveForAll fallback + removeLiquidity uses compiler gas_limit (LP_CLOSE bundle)."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        # approveForAll fails eth_estimateGas
        mock_web3.eth.estimate_gas = AsyncMock(
            side_effect=Exception("missing trie node")
        )
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
        # approveForAll uses fallback (50K), removeLiquidity uses compiler gas_limit (500K)
        assert result.gas_estimates == [50_000, 500_000]


class TestMultiTxSimulationSkip:
    """Multi-TX bundles skip estimation for non-first TXs.

    This class tests the VIB-157-pattern fix for the simulation pre-check.
    When a bundle has multiple transactions (approve + operation), the non-first
    TXs depend on state changes from prior TXs. eth_estimateGas against the current
    chain state will revert for these TXs even though the bundle would succeed
    when executed sequentially. The fix: skip estimation for non-first TXs and
    use the compiler-provided gas_limit instead.
    """

    @pytest.mark.asyncio
    async def test_multi_tx_only_first_calls_estimate_gas(self):
        """Only the first TX in a multi-TX bundle should call eth_estimateGas."""
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
        assert mock_web3.eth.estimate_gas.call_count == 1

    @pytest.mark.asyncio
    async def test_multi_tx_non_first_uses_compiler_gas_limit(self):
        """Non-first TXs should use the compiler-provided gas_limit."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=46000)
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx1 = _make_approve_tx(gas_limit=65000)
        tx2 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=884_000)

        result = await sim.simulate([tx1, tx2], chain="arbitrum")

        assert result.success
        assert result.gas_estimates == [46000, 884_000]

    @pytest.mark.asyncio
    async def test_single_tx_still_estimated(self):
        """Single-TX bundles should still use eth_estimateGas normally."""
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
    async def test_multi_tx_non_first_uses_exact_compiler_gas_limit(self):
        """Non-first TX uses its exact compiler gas_limit, not an estimated value."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=46000)
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.provider.make_request = AsyncMock(return_value={"result": "0x1"})
        mock_web3.eth.send_transaction = AsyncMock(return_value=b"\x00" * 32)
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value={"status": 1})
        sim._web3 = mock_web3

        tx1 = _make_approve_tx(gas_limit=65000)
        # Use a distinctive gas_limit to prove it comes from the compiler
        tx2 = _make_tx(data=TRANSFER_SELECTOR + "0" * 56, gas_limit=777_777)

        result = await sim.simulate([tx1, tx2], chain="arbitrum")

        assert result.success
        assert result.gas_estimates == [46000, 777_777]

    @pytest.mark.asyncio
    async def test_three_tx_bundle_only_first_estimated(self):
        """In a 3-TX bundle (approve+approve+operation), only first is estimated."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=46000)
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
        # Only first TX estimated; TX 2 and TX 3 use compiler gas_limits
        assert result.gas_estimates == [46000, 65000, 400000]
        assert mock_web3.eth.estimate_gas.call_count == 1

    @pytest.mark.asyncio
    async def test_three_tx_bundle_middle_tx_executed_for_state(self):
        """In a 3-TX bundle, middle TXs are executed for state setup even though not estimated."""
        sim = LocalSimulator(rpc_url="http://localhost:8545")

        mock_web3 = MagicMock()
        mock_web3.eth.estimate_gas = AsyncMock(return_value=46000)
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
