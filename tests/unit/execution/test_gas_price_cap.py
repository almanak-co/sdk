"""Tests for gas price cap enforcement.

Verifies that max_gas_price_gwei is enforced by the orchestrator AFTER
gas prices are fetched from the network, not before (when they're placeholder 0s).

Regression test for the bug where gas price validation ran in _validate_transactions()
before _update_gas_prices() set real values, so the cap was never enforced.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.interfaces import (
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.orchestrator import (
    ExecutionOrchestrator,
    TransactionRiskConfig,
)


def _make_orchestrator(
    chain: str = "arbitrum",
    max_gas_price_gwei: int = 10,
) -> ExecutionOrchestrator:
    """Create an ExecutionOrchestrator with mocked dependencies and gas cap."""
    signer = MagicMock()
    signer.address = "0x1234567890abcdef1234567890abcdef12345678"
    submitter = MagicMock()
    simulator = MagicMock()

    tx_risk_config = TransactionRiskConfig.default()
    tx_risk_config.max_gas_price_gwei = max_gas_price_gwei

    return ExecutionOrchestrator(
        signer=signer,
        submitter=submitter,
        simulator=simulator,
        chain=chain,
        tx_risk_config=tx_risk_config,
    )


def _make_tx(
    gas_price: int = 0,
    max_fee_per_gas: int | None = None,
    max_priority_fee_per_gas: int | None = None,
) -> UnsignedTransaction:
    """Create a test transaction."""
    if max_fee_per_gas is not None:
        return UnsignedTransaction(
            to="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            value=0,
            data="0x",
            chain_id=42161,
            gas_limit=100_000,
            tx_type=TransactionType.EIP_1559,
            max_fee_per_gas=max_fee_per_gas,
            max_priority_fee_per_gas=max_priority_fee_per_gas or 1_000_000_000,
        )
    return UnsignedTransaction(
        to="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        value=0,
        data="0x",
        chain_id=42161,
        gas_limit=100_000,
        tx_type=TransactionType.LEGACY,
        gas_price=gas_price,
    )


# =============================================================================
# _validate_gas_prices: blocks when cap exceeded
# =============================================================================


class TestValidateGasPrices:
    """Test the _validate_gas_prices method directly."""

    def test_blocks_when_gas_price_exceeds_cap(self):
        """Gas price 50 gwei should be blocked when cap is 10 gwei."""
        orchestrator = _make_orchestrator(max_gas_price_gwei=10)

        # 50 gwei in wei
        gas_price_wei = 50 * 10**9
        txs = [_make_tx(max_fee_per_gas=gas_price_wei)]

        result = orchestrator._validate_gas_prices(txs)

        assert result.passed is False
        assert len(result.violations) == 1
        assert "50.0 gwei exceeds limit 10 gwei" in result.violations[0]

    def test_allows_when_gas_price_under_cap(self):
        """Gas price 5 gwei should pass when cap is 10 gwei."""
        orchestrator = _make_orchestrator(max_gas_price_gwei=10)

        gas_price_wei = 5 * 10**9
        txs = [_make_tx(max_fee_per_gas=gas_price_wei)]

        result = orchestrator._validate_gas_prices(txs)

        assert result.passed is True
        assert len(result.violations) == 0

    def test_allows_when_gas_price_equals_cap(self):
        """Gas price exactly at the cap should pass."""
        orchestrator = _make_orchestrator(max_gas_price_gwei=10)

        gas_price_wei = 10 * 10**9
        txs = [_make_tx(max_fee_per_gas=gas_price_wei)]

        result = orchestrator._validate_gas_prices(txs)

        assert result.passed is True

    def test_skips_check_when_cap_is_zero(self):
        """When max_gas_price_gwei=0, no limit is enforced."""
        orchestrator = _make_orchestrator(max_gas_price_gwei=0)
        # Override to 0 (constructor validates > 0 for TransactionRiskConfig.default())
        orchestrator.tx_risk_config.max_gas_price_gwei = 0

        gas_price_wei = 999 * 10**9
        txs = [_make_tx(max_fee_per_gas=gas_price_wei)]

        result = orchestrator._validate_gas_prices(txs)

        assert result.passed is True

    def test_blocks_multiple_txs(self):
        """Multiple transactions should each be checked against the cap."""
        orchestrator = _make_orchestrator(max_gas_price_gwei=10)

        txs = [
            _make_tx(max_fee_per_gas=5 * 10**9),   # under cap
            _make_tx(max_fee_per_gas=50 * 10**9),  # over cap
            _make_tx(max_fee_per_gas=20 * 10**9),  # over cap
        ]

        result = orchestrator._validate_gas_prices(txs)

        assert result.passed is False
        assert len(result.violations) == 2

    def test_checks_legacy_gas_price(self):
        """Legacy transactions should check gas_price field."""
        orchestrator = _make_orchestrator(max_gas_price_gwei=10)

        gas_price_wei = 50 * 10**9
        txs = [_make_tx(gas_price=gas_price_wei)]

        result = orchestrator._validate_gas_prices(txs)

        assert result.passed is False
        assert "50.0 gwei exceeds limit 10 gwei" in result.violations[0]


# =============================================================================
# _validate_transactions: no longer checks gas prices
# =============================================================================


class TestValidateTransactionsNoGasCheck:
    """Verify _validate_transactions does NOT check gas prices (moved to _validate_gas_prices)."""

    @pytest.mark.asyncio
    async def test_passes_with_high_gas_price(self):
        """_validate_transactions should pass even with high gas prices
        because gas price validation is now in _validate_gas_prices()."""
        orchestrator = _make_orchestrator(max_gas_price_gwei=10)

        # 50 gwei - would be blocked by old _validate_transactions, should pass now
        gas_price_wei = 50 * 10**9
        txs = [_make_tx(max_fee_per_gas=gas_price_wei)]

        from almanak.framework.execution.orchestrator import ExecutionContext

        context = ExecutionContext(
            strategy_id="test",
            intent_id="test",
            chain="arbitrum",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        )

        result = await orchestrator._validate_transactions(txs, context)

        # Should pass: gas price check is no longer in _validate_transactions
        assert result.passed is True


# =============================================================================
# End-to-end: orchestrator.execute() blocks on gas cap
# =============================================================================


class TestExecuteBlocksOnGasCap:
    """End-to-end test: orchestrator.execute() blocks when gas price exceeds cap."""

    @pytest.mark.asyncio
    async def test_execute_blocked_by_gas_price_cap(self):
        """Set max_gas_price_gwei=10, mock network gas at 50 gwei, verify blocked."""
        orchestrator = _make_orchestrator(max_gas_price_gwei=10)

        # Mock RPC for nonce queries
        mock_web3 = AsyncMock()
        mock_web3.eth.get_transaction_count = AsyncMock(return_value=0)
        mock_web3.to_checksum_address = lambda addr: addr
        orchestrator._web3 = mock_web3

        # Mock get_gas_price to return 50 gwei
        orchestrator.get_gas_price = AsyncMock(return_value={
            "max_fee_per_gas": 50 * 10**9,      # 50 gwei
            "max_priority_fee_per_gas": 2 * 10**9,  # 2 gwei
            "base_fee_per_gas": 24 * 10**9,
        })

        # Mock simulator
        from almanak.framework.execution.interfaces import SimulationResult

        orchestrator.simulator.simulate = AsyncMock(return_value=SimulationResult(
            success=True,
            simulated=True,
            gas_estimates=[100_000],
        ))

        # Create a simple action bundle
        from almanak.framework.models.reproduction_bundle import ActionBundle

        bundle = ActionBundle(
            intent_type="swap",
            transactions=[{
                "to": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                "data": "0x1234",
                "value": 0,
                "chain_id": 42161,
                "gas_limit": 200_000,
            }],
        )

        from almanak.framework.execution.orchestrator import ExecutionContext

        context = ExecutionContext(
            strategy_id="test",
            intent_id="test",
            chain="arbitrum",
            wallet_address=orchestrator.signer.address,
            simulation_enabled=False,
        )

        result = await orchestrator.execute(bundle, context)

        # Execution should fail due to gas price cap
        assert result.success is False
        assert "gas price" in result.error.lower()
        assert "exceeds limit" in result.error.lower()
        assert "10 gwei" in result.error

        # Verify no transactions were signed or submitted
        orchestrator.signer.sign_batch.assert_not_called()
        orchestrator.submitter.submit.assert_not_called()
