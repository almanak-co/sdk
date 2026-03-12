"""Tests for ExecutionOutcome and to_outcome() on existing result types.

Verifies:
  - ExecutionOutcome construction and defaults
  - ExecutionResult.to_outcome() preserves all fields
  - GatewayExecutionResult.to_outcome() preserves all fields
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.extracted_data import SwapAmounts
from almanak.framework.execution.outcome import ExecutionOutcome


class TestExecutionOutcome:
    def test_defaults(self):
        outcome = ExecutionOutcome(success=True)
        assert outcome.success is True
        assert outcome.tx_ids == []
        assert outcome.receipts == []
        assert outcome.total_fee_native == Decimal(0)
        assert outcome.error is None
        assert outcome.chain_family == "EVM"
        assert outcome.position_id is None
        assert outcome.swap_amounts is None
        assert outcome.lp_close_data is None
        assert outcome.extracted_data == {}
        assert outcome.extraction_warnings == []

    def test_construction_full(self):
        swap = SwapAmounts(
            amount_in=1000000,
            amount_out=500000000000000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.0005"),
            effective_price=Decimal("0.0005"),
            token_in="USDC",
            token_out="WETH",
        )
        outcome = ExecutionOutcome(
            success=True,
            tx_ids=["0xabc", "0xdef"],
            receipts=[{"status": 1}],
            total_fee_native=Decimal("21000"),
            error=None,
            chain_family="SOLANA",
            position_id=42,
            swap_amounts=swap,
            extracted_data={"tick_lower": -100},
            extraction_warnings=["partial extraction"],
        )
        assert outcome.chain_family == "SOLANA"
        assert len(outcome.tx_ids) == 2
        assert outcome.position_id == 42
        assert outcome.swap_amounts.token_in == "USDC"
        assert outcome.extracted_data["tick_lower"] == -100

    def test_failed_outcome(self):
        outcome = ExecutionOutcome(
            success=False,
            error="Transaction reverted",
            chain_family="EVM",
        )
        assert outcome.success is False
        assert outcome.error == "Transaction reverted"


class TestExecutionResultToOutcome:
    def test_to_outcome_success(self):
        from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult, TransactionResult

        tx_result = TransactionResult(
            tx_hash="0x123abc",
            success=True,
            gas_used=21000,
            gas_cost_wei=2100000000000,
        )
        result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            transaction_results=[tx_result],
            total_gas_used=21000,
            total_gas_cost_wei=2100000000000,
            position_id=99,
            extracted_data={"liquidity": "12345"},
        )
        outcome = result.to_outcome()

        assert isinstance(outcome, ExecutionOutcome)
        assert outcome.success is True
        assert outcome.tx_ids == ["0x123abc"]
        assert outcome.total_fee_native == Decimal(2100000000000)
        assert outcome.chain_family == "EVM"
        assert outcome.position_id == 99
        assert outcome.extracted_data["liquidity"] == "12345"

    def test_to_outcome_failure(self):
        from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult

        result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.SIGNING,
            error="Signing failed",
        )
        outcome = result.to_outcome()

        assert outcome.success is False
        assert outcome.error == "Signing failed"
        assert outcome.tx_ids == []

    def test_to_outcome_with_swap_amounts(self):
        from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult

        swap = SwapAmounts(
            amount_in=1000000000000000000,
            amount_out=2000000000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("2000.0"),
            effective_price=Decimal("2000.0"),
            token_in="WETH",
            token_out="USDC",
        )
        result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            swap_amounts=swap,
        )
        outcome = result.to_outcome()
        assert outcome.swap_amounts is swap


class TestGatewayExecutionResultToOutcome:
    def test_to_outcome_success(self):
        from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult

        result = GatewayExecutionResult(
            success=True,
            tx_hashes=["0xaaa", "0xbbb"],
            total_gas_used=42000,
            receipts=[{"status": 1}, {"status": 1}],
            execution_id="exec-123",
            position_id=7,
            extracted_data={"bin_ids": [1, 2, 3]},
            extraction_warnings=["partial"],
        )
        outcome = result.to_outcome()

        assert isinstance(outcome, ExecutionOutcome)
        assert outcome.success is True
        assert outcome.tx_ids == ["0xaaa", "0xbbb"]
        assert outcome.total_fee_native == Decimal(42000)
        assert outcome.chain_family == "EVM"
        assert outcome.position_id == 7
        assert outcome.extracted_data["bin_ids"] == [1, 2, 3]
        assert outcome.extraction_warnings == ["partial"]

    def test_to_outcome_failure(self):
        from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult

        result = GatewayExecutionResult(
            success=False,
            tx_hashes=[],
            total_gas_used=0,
            receipts=[],
            execution_id="",
            error="Timeout",
        )
        outcome = result.to_outcome()

        assert outcome.success is False
        assert outcome.error == "Timeout"
        assert outcome.tx_ids == []
