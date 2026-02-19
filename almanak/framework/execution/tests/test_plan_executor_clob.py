"""Tests for PlanExecutor CLOB Integration.

This module tests the integration between PlanExecutor and ClobActionHandler
for routing Polymarket prediction orders.

Tests cover:
- CLOB bundle detection (_is_clob_bundle)
- Routing CLOB orders to ClobActionHandler
- Routing redemption bundles to on-chain executor
- execute_bundle() routing logic
- Execution path logging
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.clob_handler import (
    ClobActionHandler,
    ClobExecutionResult,
    ClobOrderStatus,
)
from almanak.framework.execution.plan_executor import (
    ExecutionPath,
    PlanExecutor,
    PlanExecutorConfig,
    StepExecutionResult,
)
from almanak.framework.models.reproduction_bundle import ActionBundle

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_clob_client():
    """Create a mock ClobClient."""
    client = MagicMock()
    return client


@pytest.fixture
def mock_clob_handler(mock_clob_client):
    """Create a mock ClobActionHandler."""
    handler = MagicMock(spec=ClobActionHandler)
    return handler


@pytest.fixture
def executor_with_clob(mock_clob_handler):
    """Create a PlanExecutor with CLOB handler."""
    return PlanExecutor(
        config=PlanExecutorConfig(),
        clob_handler=mock_clob_handler,
    )


@pytest.fixture
def executor_no_clob():
    """Create a PlanExecutor without CLOB handler."""
    return PlanExecutor(config=PlanExecutorConfig())


@pytest.fixture
def clob_buy_bundle():
    """Create a valid CLOB buy order bundle."""
    return ActionBundle(
        intent_type="PREDICTION_BUY",
        transactions=[],  # Empty - CLOB orders are off-chain
        metadata={
            "protocol": "polymarket",
            "order_payload": {
                "order": {
                    "salt": 12345,
                    "maker": "0x1234567890123456789012345678901234567890",
                    "signer": "0x1234567890123456789012345678901234567890",
                    "taker": "0x0000000000000000000000000000000000000000",
                    "tokenId": "1234567890",
                    "makerAmount": "1000000000",
                    "takerAmount": "500000000",
                    "expiration": "0",
                    "nonce": "0",
                    "feeRateBps": "0",
                    "side": 0,
                    "signatureType": 0,
                },
                "signature": "0xabcdef1234567890",
                "orderType": "GTC",
            },
            "side": "BUY",
            "size": "100",
            "price": "0.50",
            "intent_id": "test-buy-intent-123",
            "market_id": "0x1234",
        },
    )


@pytest.fixture
def clob_sell_bundle():
    """Create a valid CLOB sell order bundle."""
    return ActionBundle(
        intent_type="PREDICTION_SELL",
        transactions=[],  # Empty - CLOB orders are off-chain
        metadata={
            "protocol": "polymarket",
            "order_payload": {
                "order": {"salt": 67890},
                "signature": "0xfedcba",
            },
            "side": "SELL",
            "size": "50",
            "price": "0.60",
            "intent_id": "test-sell-intent-456",
        },
    )


@pytest.fixture
def redemption_bundle():
    """Create a Polymarket redemption bundle (on-chain)."""
    return ActionBundle(
        intent_type="PREDICTION_REDEEM",
        transactions=[
            {
                "to": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",  # CTF Exchange
                "data": "0xredeempositions...",
                "value": "0",
            }
        ],
        metadata={
            "protocol": "polymarket",
            "condition_id": "0xabcd1234",
            "intent_id": "test-redeem-intent-789",
        },
    )


@pytest.fixture
def uniswap_swap_bundle():
    """Create a standard Uniswap swap bundle."""
    return ActionBundle(
        intent_type="SWAP",
        transactions=[
            {
                "to": "0xE592427A0AEce92De3Edee1F18E0157C05861564",  # Uniswap Router
                "data": "0xswap...",
                "value": "0",
            }
        ],
        metadata={
            "protocol": "uniswap_v3",
            "token_in": "USDC",
            "token_out": "ETH",
        },
    )


# =============================================================================
# Tests: _is_clob_bundle Detection
# =============================================================================


class TestIsClobBundle:
    """Tests for CLOB bundle detection logic."""

    def test_detects_valid_clob_buy_bundle(self, executor_with_clob, clob_buy_bundle):
        """Valid CLOB buy bundle should be detected."""
        assert executor_with_clob._is_clob_bundle(clob_buy_bundle) is True

    def test_detects_valid_clob_sell_bundle(self, executor_with_clob, clob_sell_bundle):
        """Valid CLOB sell bundle should be detected."""
        assert executor_with_clob._is_clob_bundle(clob_sell_bundle) is True

    def test_rejects_redemption_bundle(self, executor_with_clob, redemption_bundle):
        """Redemption bundle with transactions should go on-chain."""
        assert executor_with_clob._is_clob_bundle(redemption_bundle) is False

    def test_rejects_non_polymarket_bundle(self, executor_with_clob, uniswap_swap_bundle):
        """Non-Polymarket bundle should not be CLOB."""
        assert executor_with_clob._is_clob_bundle(uniswap_swap_bundle) is False

    def test_rejects_bundle_without_order_payload(self, executor_with_clob):
        """Bundle without order_payload should not be CLOB."""
        bundle = ActionBundle(
            intent_type="PREDICTION_BUY",
            transactions=[],
            metadata={"protocol": "polymarket"},  # Missing order_payload
        )
        assert executor_with_clob._is_clob_bundle(bundle) is False

    def test_rejects_bundle_with_transactions(self, executor_with_clob):
        """Polymarket bundle with transactions should go on-chain."""
        bundle = ActionBundle(
            intent_type="PREDICTION_BUY",
            transactions=[{"to": "0x123", "data": "0x"}],  # Has transactions
            metadata={
                "protocol": "polymarket",
                "order_payload": {"order": {}},
            },
        )
        assert executor_with_clob._is_clob_bundle(bundle) is False


# =============================================================================
# Tests: execute_bundle Routing
# =============================================================================


class TestExecuteBundleRouting:
    """Tests for execute_bundle routing logic."""

    def test_routes_clob_bundle_to_handler(self, executor_with_clob, mock_clob_handler, clob_buy_bundle):
        """CLOB bundle should be routed to ClobActionHandler."""
        # Setup mock response
        mock_clob_handler.execute = AsyncMock(
            return_value=ClobExecutionResult(
                success=True,
                order_id="order-123",
                status=ClobOrderStatus.LIVE,
            )
        )

        # Execute
        result = asyncio.run(executor_with_clob.execute_bundle(clob_buy_bundle))

        # Verify routing
        mock_clob_handler.execute.assert_called_once_with(clob_buy_bundle)
        assert result.execution_path == ExecutionPath.CLOB
        assert result.success is True
        assert result.order_id == "order-123"

    def test_routes_redemption_to_onchain(self, executor_with_clob, mock_clob_handler, redemption_bundle):
        """Redemption bundle should be routed to on-chain executor."""
        # Setup mock on-chain executor
        mock_onchain_executor = AsyncMock(return_value={"success": True, "tx_hash": "0xabc123"})

        # Execute
        result = asyncio.run(executor_with_clob.execute_bundle(redemption_bundle, mock_onchain_executor))

        # Verify routing - CLOB handler should NOT be called
        mock_clob_handler.execute.assert_not_called()
        mock_onchain_executor.assert_called_once_with(redemption_bundle)
        assert result.execution_path == ExecutionPath.ON_CHAIN
        assert result.success is True
        assert result.tx_hash == "0xabc123"

    def test_routes_uniswap_to_onchain(self, executor_with_clob, mock_clob_handler, uniswap_swap_bundle):
        """Non-Polymarket bundle should go to on-chain executor."""
        mock_onchain_executor = AsyncMock(return_value={"success": True, "tx_hash": "0xdef456"})

        result = asyncio.run(executor_with_clob.execute_bundle(uniswap_swap_bundle, mock_onchain_executor))

        mock_clob_handler.execute.assert_not_called()
        mock_onchain_executor.assert_called_once()
        assert result.execution_path == ExecutionPath.ON_CHAIN

    def test_clob_without_handler_fails(self, executor_no_clob, clob_buy_bundle):
        """CLOB bundle without handler should fail gracefully."""
        result = asyncio.run(executor_no_clob.execute_bundle(clob_buy_bundle))

        assert result.success is False
        assert result.execution_path == ExecutionPath.CLOB

    def test_simulates_without_executor(self, executor_with_clob, redemption_bundle):
        """On-chain bundle without executor should simulate success."""
        result = asyncio.run(executor_with_clob.execute_bundle(redemption_bundle, on_chain_executor=None))

        assert result.success is True
        assert result.execution_path == ExecutionPath.SIMULATED


# =============================================================================
# Tests: CLOB Execution Results
# =============================================================================


class TestClobExecutionResults:
    """Tests for CLOB execution result handling."""

    def test_successful_clob_execution(self, executor_with_clob, mock_clob_handler, clob_buy_bundle):
        """Successful CLOB execution should populate result correctly."""
        mock_clob_handler.execute = AsyncMock(
            return_value=ClobExecutionResult(
                success=True,
                order_id="order-success-456",
                status=ClobOrderStatus.LIVE,
            )
        )

        result = asyncio.run(executor_with_clob.execute_bundle(clob_buy_bundle))

        assert result.success is True
        assert result.order_id == "order-success-456"
        assert result.tx_hash is None  # CLOB orders have no tx_hash

    def test_failed_clob_execution(self, executor_with_clob, mock_clob_handler, clob_buy_bundle):
        """Failed CLOB execution should report error."""
        mock_clob_handler.execute = AsyncMock(
            return_value=ClobExecutionResult(
                success=False,
                status=ClobOrderStatus.FAILED,
                error="Insufficient balance",
            )
        )

        result = asyncio.run(executor_with_clob.execute_bundle(clob_buy_bundle))

        assert result.success is False
        assert result.order_id is None

    def test_clob_execution_exception(self, executor_with_clob, mock_clob_handler, clob_buy_bundle):
        """Exception during CLOB execution should be handled."""
        mock_clob_handler.execute = AsyncMock(side_effect=Exception("Network error"))

        result = asyncio.run(executor_with_clob.execute_bundle(clob_buy_bundle))

        assert result.success is False


# =============================================================================
# Tests: On-Chain Execution Results
# =============================================================================


class TestOnChainExecutionResults:
    """Tests for on-chain execution result handling."""

    def test_successful_onchain_execution(self, executor_with_clob, redemption_bundle):
        """Successful on-chain execution should populate result."""
        mock_executor = AsyncMock(
            return_value={
                "success": True,
                "tx_hash": "0xredemption_tx_hash",
            }
        )

        result = asyncio.run(executor_with_clob.execute_bundle(redemption_bundle, mock_executor))

        assert result.success is True
        assert result.tx_hash == "0xredemption_tx_hash"
        assert result.order_id is None  # On-chain has no order_id

    def test_failed_onchain_execution(self, executor_with_clob, redemption_bundle):
        """Failed on-chain execution should report failure."""
        mock_executor = AsyncMock(
            return_value={
                "success": False,
                "error": "Transaction reverted",
            }
        )

        result = asyncio.run(executor_with_clob.execute_bundle(redemption_bundle, mock_executor))

        assert result.success is False
        assert result.tx_hash is None

    def test_onchain_execution_exception(self, executor_with_clob, redemption_bundle):
        """Exception during on-chain execution should be handled."""
        mock_executor = AsyncMock(side_effect=Exception("RPC error"))

        result = asyncio.run(executor_with_clob.execute_bundle(redemption_bundle, mock_executor))

        assert result.success is False


# =============================================================================
# Tests: End-to-End Integration (Compile -> Execute -> Verify)
# =============================================================================


class TestEndToEndIntegration:
    """End-to-end integration tests for prediction intent pipeline."""

    def test_compile_buy_intent_and_execute(self, executor_with_clob, mock_clob_handler):
        """Test full pipeline: PredictionBuyIntent -> compile -> execute -> verify."""
        # This simulates what happens after IntentCompiler produces an ActionBundle
        # 1. IntentCompiler.compile(PredictionBuyIntent) -> ActionBundle
        # 2. PlanExecutor.execute_bundle(bundle) -> routes to CLOB handler
        # 3. ClobActionHandler.execute(bundle) -> order submitted

        bundle = ActionBundle(
            intent_type="PREDICTION_BUY",
            transactions=[],
            metadata={
                "protocol": "polymarket",
                "order_payload": {
                    "order": {"salt": 111, "tokenId": "token-yes"},
                    "signature": "0xsig",
                },
                "side": "BUY",
                "size": "50",
                "price": "0.65",
                "intent_id": "e2e-buy-intent",
                "market_id": "market-123",
            },
        )

        mock_clob_handler.execute = AsyncMock(
            return_value=ClobExecutionResult(
                success=True,
                order_id="e2e-order-001",
                status=ClobOrderStatus.LIVE,
            )
        )

        result = asyncio.run(executor_with_clob.execute_bundle(bundle))

        # Verify full pipeline
        assert result.execution_path == ExecutionPath.CLOB
        assert result.success is True
        assert result.order_id == "e2e-order-001"
        mock_clob_handler.execute.assert_called_once()

    def test_compile_sell_intent_and_execute(self, executor_with_clob, mock_clob_handler):
        """Test full pipeline: PredictionSellIntent -> compile -> execute -> verify."""
        bundle = ActionBundle(
            intent_type="PREDICTION_SELL",
            transactions=[],
            metadata={
                "protocol": "polymarket",
                "order_payload": {
                    "order": {"salt": 222},
                    "signature": "0xsig_sell",
                },
                "side": "SELL",
                "size": "25",
                "price": "0.70",
                "intent_id": "e2e-sell-intent",
            },
        )

        mock_clob_handler.execute = AsyncMock(
            return_value=ClobExecutionResult(
                success=True,
                order_id="e2e-order-002",
                status=ClobOrderStatus.MATCHED,
            )
        )

        result = asyncio.run(executor_with_clob.execute_bundle(bundle))

        assert result.execution_path == ExecutionPath.CLOB
        assert result.success is True
        assert result.order_id == "e2e-order-002"

    def test_compile_redeem_intent_and_execute_onchain(self, executor_with_clob, mock_clob_handler):
        """Test full pipeline: PredictionRedeemIntent -> compile -> execute on-chain."""
        # Redemption intents produce bundles with transactions (CTF contract calls)
        bundle = ActionBundle(
            intent_type="PREDICTION_REDEEM",
            transactions=[
                {
                    "to": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
                    "data": "0xredeem_call_data",
                    "value": "0",
                }
            ],
            metadata={
                "protocol": "polymarket",
                "condition_id": "0xcondition123",
                "intent_id": "e2e-redeem-intent",
            },
        )

        mock_onchain_executor = AsyncMock(
            return_value={
                "success": True,
                "tx_hash": "0xredeem_tx_hash_abc",
            }
        )

        result = asyncio.run(executor_with_clob.execute_bundle(bundle, mock_onchain_executor))

        # Verify redemption went on-chain, not CLOB
        assert result.execution_path == ExecutionPath.ON_CHAIN
        assert result.success is True
        assert result.tx_hash == "0xredeem_tx_hash_abc"
        mock_clob_handler.execute.assert_not_called()
        mock_onchain_executor.assert_called_once()


# =============================================================================
# Tests: Step Execution Result Serialization
# =============================================================================


class TestStepExecutionResultSerialization:
    """Tests for StepExecutionResult with new fields."""

    def test_serialization_includes_execution_path(self):
        """StepExecutionResult should serialize execution_path."""
        from almanak.framework.execution.plan_executor import PlanExecutionResult, PlanExecutionStatus

        result = StepExecutionResult(
            step_id="step-1",
            success=True,
            order_id="order-123",
            execution_path=ExecutionPath.CLOB,
        )

        plan_result = PlanExecutionResult(
            plan_id="plan-1",
            status=PlanExecutionStatus.COMPLETED,
            step_results=[result],
        )

        serialized = plan_result.to_dict()

        assert serialized["step_results"][0]["execution_path"] == "clob"
        assert serialized["step_results"][0]["order_id"] == "order-123"

    def test_serialization_with_tx_hash(self):
        """StepExecutionResult should serialize tx_hash for on-chain."""
        from almanak.framework.execution.plan_executor import PlanExecutionResult, PlanExecutionStatus

        result = StepExecutionResult(
            step_id="step-2",
            success=True,
            tx_hash="0xabc",
            execution_path=ExecutionPath.ON_CHAIN,
        )

        plan_result = PlanExecutionResult(
            plan_id="plan-2",
            status=PlanExecutionStatus.COMPLETED,
            step_results=[result],
        )

        serialized = plan_result.to_dict()

        assert serialized["step_results"][0]["execution_path"] == "on_chain"
        assert serialized["step_results"][0]["tx_hash"] == "0xabc"
        assert serialized["step_results"][0]["order_id"] is None
