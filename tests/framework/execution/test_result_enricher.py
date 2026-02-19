"""Tests for ResultEnricher component.

Tests the automatic receipt parsing and result enrichment functionality
that provides zero-cognitive-load data access for strategy authors.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from unittest.mock import Mock, patch

import pytest

from almanak.framework.execution.extracted_data import LPCloseData, SwapAmounts
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
)
from almanak.framework.execution.receipt_registry import ReceiptParserRegistry
from almanak.framework.execution.result_enricher import ResultEnricher


# =============================================================================
# Test Fixtures
# =============================================================================


@dataclass
class MockIntent:
    """Mock intent for testing."""

    intent_type: str = "LP_OPEN"
    protocol: str = "uniswap_v3"
    intent_id: str = "test-intent-123"


@dataclass
class MockSwapIntent:
    """Mock swap intent for testing."""

    intent_type: str = "SWAP"
    protocol: str = "uniswap_v3"
    intent_id: str = "test-swap-456"


@dataclass
class MockReceipt:
    """Mock transaction receipt."""

    logs: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"logs": self.logs}


def create_execution_result(
    success: bool = True,
    logs: list[dict[str, Any]] | None = None,
) -> ExecutionResult:
    """Create a mock ExecutionResult for testing."""
    receipt = MockReceipt(logs=logs or [])
    tx_result = TransactionResult(
        tx_hash="0x123abc",
        success=success,
        receipt=receipt,  # type: ignore
        gas_used=100000,
    )
    return ExecutionResult(
        success=success,
        phase=ExecutionPhase.COMPLETE,
        transaction_results=[tx_result],
        total_gas_used=100000,
    )


def create_context() -> ExecutionContext:
    """Create a mock ExecutionContext."""
    return ExecutionContext(
        strategy_id="test-strategy",
        chain="arbitrum",
        wallet_address="0x1234567890abcdef",
    )


# =============================================================================
# ResultEnricher Core Tests
# =============================================================================


class TestResultEnricherBasics:
    """Test basic ResultEnricher functionality."""

    def test_does_not_enrich_failed_result(self):
        """Failed executions should not be enriched."""
        enricher = ResultEnricher()
        result = create_execution_result(success=False)
        intent = MockIntent()
        context = create_context()

        # Should return unchanged result
        enriched = enricher.enrich(result, intent, context)

        assert enriched is result
        assert enriched.position_id is None
        assert enriched.extracted_data == {}

    def test_does_not_enrich_unknown_intent_type(self):
        """Unknown intent types should be skipped gracefully."""
        enricher = ResultEnricher()
        result = create_execution_result(success=True)
        intent = MockIntent()
        intent.intent_type = "UNKNOWN_INTENT_TYPE"
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        assert enriched.position_id is None
        assert len(enriched.extraction_warnings) == 0

    def test_does_not_enrich_without_protocol(self):
        """Intents without protocol should be skipped gracefully."""
        enricher = ResultEnricher()
        result = create_execution_result(success=True)
        intent = MockIntent()
        intent.protocol = None  # type: ignore
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        assert enriched.position_id is None

    def test_handles_missing_parser_gracefully(self):
        """Missing parser should log warning but not crash."""
        enricher = ResultEnricher()
        result = create_execution_result(success=True)
        intent = MockIntent()
        intent.protocol = "unknown_protocol"
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        assert enriched.position_id is None
        assert len(enriched.extraction_warnings) > 0
        assert "Parser not found" in enriched.extraction_warnings[0]

    def test_hold_intent_has_no_extraction(self):
        """HOLD intents have empty extraction spec."""
        enricher = ResultEnricher()

        assert "HOLD" in enricher.EXTRACTION_SPECS
        assert enricher.EXTRACTION_SPECS["HOLD"] == []


class TestResultEnricherPositionId:
    """Test position ID extraction via ResultEnricher."""

    def test_extracts_position_id_from_receipt(self):
        """Position ID should be extracted from LP_OPEN receipts."""
        # Create a mock parser that returns a position ID
        mock_parser = Mock()
        mock_parser.extract_position_id.return_value = 12345

        mock_registry = Mock(spec=ReceiptParserRegistry)
        mock_registry.get.return_value = mock_parser

        enricher = ResultEnricher(parser_registry=mock_registry)
        result = create_execution_result(success=True)
        intent = MockIntent()
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        # Should have extracted position_id
        assert enriched.position_id == 12345
        assert enriched.extracted_data["position_id"] == 12345

    def test_handles_extraction_exception(self):
        """Extraction exceptions should be logged but not crash."""
        mock_parser = Mock()
        mock_parser.extract_position_id.side_effect = ValueError("Test error")

        mock_registry = Mock(spec=ReceiptParserRegistry)
        mock_registry.get.return_value = mock_parser

        enricher = ResultEnricher(parser_registry=mock_registry)
        result = create_execution_result(success=True)
        intent = MockIntent()
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        # Should have warning but not crash
        assert enriched.position_id is None
        assert len(enriched.extraction_warnings) > 0
        assert "Test error" in enriched.extraction_warnings[0]


class TestResultEnricherSwapAmounts:
    """Test swap amounts extraction via ResultEnricher."""

    def test_extracts_swap_amounts(self):
        """Swap amounts should be extracted from SWAP receipts."""
        swap_amounts = SwapAmounts(
            amount_in=1000000,
            amount_out=500000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("0.5"),
            slippage_bps=10,
            token_in="USDC",
            token_out="WETH",
        )

        mock_parser = Mock()
        mock_parser.extract_swap_amounts.return_value = swap_amounts

        mock_registry = Mock(spec=ReceiptParserRegistry)
        mock_registry.get.return_value = mock_parser

        enricher = ResultEnricher(parser_registry=mock_registry)
        result = create_execution_result(success=True)
        intent = MockSwapIntent()
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        # Should have extracted swap_amounts
        assert enriched.swap_amounts is swap_amounts
        assert enriched.swap_amounts.amount_in == 1000000
        assert enriched.effective_price == Decimal("0.5")
        assert enriched.slippage_bps == 10

    def test_missing_swap_extraction_method(self):
        """Missing extraction method should be skipped silently."""
        # Parser without extract_swap_amounts
        mock_parser = Mock(spec=[])  # Empty spec = no attributes

        mock_registry = Mock(spec=ReceiptParserRegistry)
        mock_registry.get.return_value = mock_parser

        enricher = ResultEnricher(parser_registry=mock_registry)
        result = create_execution_result(success=True)
        intent = MockSwapIntent()
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        # Should not crash, swap_amounts should be None
        assert enriched.swap_amounts is None
        # No warning for missing method (it's expected)
        assert len(enriched.extraction_warnings) == 0


class TestExecutionResultAccessors:
    """Test convenience accessors on ExecutionResult."""

    def test_get_extracted_returns_value(self):
        """get_extracted should return stored value."""
        result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
        )
        result.extracted_data["tick_lower"] = -887220
        result.extracted_data["tick_upper"] = 887220

        assert result.get_extracted("tick_lower") == -887220
        assert result.get_extracted("tick_upper") == 887220

    def test_get_extracted_returns_default_for_missing(self):
        """get_extracted should return default for missing keys."""
        result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
        )

        assert result.get_extracted("missing") is None
        assert result.get_extracted("missing", default=0) == 0

    def test_get_extracted_type_check(self):
        """get_extracted should validate type when expected_type provided."""
        result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
        )
        result.extracted_data["tick_lower"] = -887220
        result.extracted_data["name"] = "test"

        # Correct type
        assert result.get_extracted("tick_lower", int) == -887220

        # Wrong type returns default
        assert result.get_extracted("name", int, default=0) == 0

    def test_effective_price_property(self):
        """effective_price property should return from swap_amounts."""
        result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
        )
        result.swap_amounts = SwapAmounts(
            amount_in=1000000,
            amount_out=500000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("0.5"),
        )

        assert result.effective_price == Decimal("0.5")

    def test_effective_price_none_without_swap(self):
        """effective_price should be None when no swap_amounts."""
        result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
        )

        assert result.effective_price is None

    def test_slippage_bps_property(self):
        """slippage_bps property should return from swap_amounts."""
        result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
        )
        result.swap_amounts = SwapAmounts(
            amount_in=1000000,
            amount_out=500000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("0.5"),
            slippage_bps=25,
        )

        assert result.slippage_bps == 25


class TestResultEnricherIntentTypeParsing:
    """Test intent type extraction from various intent formats."""

    def test_parses_intent_type_from_enum(self):
        """Should parse intent_type from enum-style attribute."""
        enricher = ResultEnricher()

        @dataclass
        class MockEnumIntent:
            class MockIntentType:
                value = "SWAP"

            intent_type: MockIntentType = field(default_factory=MockIntentType)

        intent = MockEnumIntent()
        result = enricher._get_intent_type(intent)

        assert result == "SWAP"

    def test_parses_intent_type_from_string(self):
        """Should parse intent_type from string attribute."""
        enricher = ResultEnricher()

        @dataclass
        class MockStringIntent:
            intent_type: str = "LP_CLOSE"

        intent = MockStringIntent()
        result = enricher._get_intent_type(intent)

        assert result == "LP_CLOSE"

    def test_derives_intent_type_from_class_name(self):
        """Should derive intent type from class name when no intent_type."""
        enricher = ResultEnricher()

        class SwapIntent:
            pass

        intent = SwapIntent()
        result = enricher._get_intent_type(intent)

        assert result == "SWAP"

    def test_derives_lp_open_from_class_name(self):
        """Should derive LP_OPEN from LPOpenIntent class name."""
        enricher = ResultEnricher()

        class LPOpenIntent:
            pass

        intent = LPOpenIntent()
        result = enricher._get_intent_type(intent)

        assert result == "LP_OPEN"  # Acronyms like LP stay together


class TestResultEnricherExtractionSpecs:
    """Test extraction specs cover all intent types."""

    def test_all_common_intent_types_have_specs(self):
        """All common intent types should have extraction specs."""
        enricher = ResultEnricher()

        expected_types = [
            "SWAP",
            "LP_OPEN",
            "LP_CLOSE",
            "BORROW",
            "REPAY",
            "SUPPLY",
            "WITHDRAW",
            "PERP_OPEN",
            "PERP_CLOSE",
            "STAKE",
            "UNSTAKE",
            "HOLD",
        ]

        for intent_type in expected_types:
            assert intent_type in enricher.EXTRACTION_SPECS, f"Missing spec for {intent_type}"

    def test_lp_open_spec_includes_position_id(self):
        """LP_OPEN spec should include position_id."""
        enricher = ResultEnricher()
        spec = enricher.EXTRACTION_SPECS["LP_OPEN"]

        assert "position_id" in spec

    def test_swap_spec_includes_swap_amounts(self):
        """SWAP spec should include swap_amounts."""
        enricher = ResultEnricher()
        spec = enricher.EXTRACTION_SPECS["SWAP"]

        assert "swap_amounts" in spec


# =============================================================================
# Integration with Real Parsers
# =============================================================================


class TestResultEnricherWithUniswapV3:
    """Integration tests with real UniswapV3ReceiptParser."""

    def test_enriches_lp_open_with_real_parser(self):
        """Should extract position_id using real Uniswap V3 parser."""
        # Create a receipt with ERC-721 Transfer event (mint)
        transfer_event = {
            "address": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",  # Position Manager
            "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # Transfer
                "0x0000000000000000000000000000000000000000000000000000000000000000",  # from=zero
                "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678",  # to
                "0x0000000000000000000000000000000000000000000000000000000000003039",  # tokenId=12345
            ],
            "data": "0x",
        }

        enricher = ResultEnricher()
        result = create_execution_result(success=True, logs=[transfer_event])
        intent = MockIntent()
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        assert enriched.position_id == 12345

    def test_enriches_swap_with_real_parser(self):
        """Should extract swap_amounts using real Uniswap V3 parser."""
        # Create a receipt with Swap event
        # Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
        swap_event = {
            "address": "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",  # Pool
            "topics": [
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",  # Swap
                "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678",  # sender
                "0x0000000000000000000000009876543210fedcba9876543210fedcba98765432",  # recipient
            ],
            "data": (
                "0x"
                "00000000000000000000000000000000000000000000000000000000000f4240"  # amount0 = 1000000 (positive, input)
                "ffffffffffffffffffffffffffffffffffffffffffffffffffff0000000f423f"  # amount1 = -1000000 (negative, output)
                "0000000000000000000000000000000000000001000000000000000000000000"  # sqrtPriceX96
                "00000000000000000000000000000000000000000000000000000000000186a0"  # liquidity
                "0000000000000000000000000000000000000000000000000000000000000000"  # tick
            ),
        }

        enricher = ResultEnricher()
        result = create_execution_result(success=True, logs=[swap_event])
        intent = MockSwapIntent()
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        # Swap amounts should be extracted (even without full token info)
        assert enriched.swap_amounts is not None
        assert enriched.swap_amounts.amount_in > 0
