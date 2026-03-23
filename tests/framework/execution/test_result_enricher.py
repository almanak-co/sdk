"""Tests for ResultEnricher component.

Tests the automatic receipt parsing and result enrichment functionality
that provides zero-cognitive-load data access for strategy authors.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from unittest.mock import Mock

from almanak.framework.execution.extracted_data import SwapAmounts
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
    from_address: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"logs": self.logs}
        if self.from_address:
            d["from"] = self.from_address
        return d


@dataclass
class LogsOnlyReceipt:
    """Mock receipt without to_dict() — exercises the _collect_receipts logs-attribute branch."""

    logs: list[dict[str, Any]] = field(default_factory=list)
    from_address: str | None = None
    status: int = 1


def create_execution_result_logs_only(
    success: bool = True,
    logs: list[dict[str, Any]] | None = None,
    from_address: str | None = None,
) -> ExecutionResult:
    """Create a mock ExecutionResult using LogsOnlyReceipt (no to_dict)."""
    receipt = LogsOnlyReceipt(logs=logs or [], from_address=from_address)
    tx_result = TransactionResult(
        tx_hash="0x456def",
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


def _make_transfer_log(token_address: str, from_addr: str, to_addr: str, amount: int) -> dict[str, Any]:
    """Create a mock ERC-20 Transfer log entry."""
    return {
        "address": token_address,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # Transfer
            "0x000000000000000000000000" + from_addr.lower().removeprefix("0x"),
            "0x000000000000000000000000" + to_addr.lower().removeprefix("0x"),
        ],
        "data": "0x" + hex(amount)[2:].zfill(64),
    }


def create_execution_result(
    success: bool = True,
    logs: list[dict[str, Any]] | None = None,
    from_address: str | None = None,
) -> ExecutionResult:
    """Create a mock ExecutionResult for testing."""
    receipt = MockReceipt(logs=logs or [], from_address=from_address)
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

    def test_does_not_enrich_without_protocol_anywhere(self):
        """Intents without protocol on intent OR context should be skipped gracefully."""
        enricher = ResultEnricher()
        result = create_execution_result(success=True)
        intent = MockIntent()
        intent.protocol = None  # type: ignore
        context = create_context()
        context.protocol = None  # No fallback either

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


class TestResultEnricherContextProtocolFallback:
    """Test that enrichment falls back to context.protocol when intent.protocol is None."""

    def test_swap_enrichment_uses_context_protocol_fallback(self):
        """Swap enrichment should work via context.protocol when intent.protocol is None (VIB-135)."""
        swap_amounts = SwapAmounts(
            amount_in=1000000,
            amount_out=500000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("0.5"),
        )

        mock_parser = Mock()
        mock_parser.extract_swap_amounts.return_value = swap_amounts

        mock_registry = Mock(spec=ReceiptParserRegistry)
        mock_registry.get.return_value = mock_parser

        enricher = ResultEnricher(parser_registry=mock_registry)
        result = create_execution_result(success=True)

        # Intent has protocol=None (common when strategy author doesn't specify)
        intent = MockSwapIntent()
        intent.protocol = None  # type: ignore

        # Context carries the resolved protocol from the compiler
        context = create_context()
        context.protocol = "uniswap_v3"

        enriched = enricher.enrich(result, intent, context)

        # Should have used context.protocol as fallback
        assert enriched.swap_amounts is swap_amounts
        assert enriched.swap_amounts.amount_out_decimal == Decimal("0.5")
        mock_registry.get.assert_called_once_with("uniswap_v3", chain="arbitrum")

    def test_lp_enrichment_uses_context_protocol_fallback(self):
        """LP enrichment should work via context.protocol when intent.protocol is None."""
        mock_parser = Mock()
        mock_parser.extract_position_id.return_value = 42

        mock_registry = Mock(spec=ReceiptParserRegistry)
        mock_registry.get.return_value = mock_parser

        enricher = ResultEnricher(parser_registry=mock_registry)
        result = create_execution_result(success=True)

        intent = MockIntent()
        intent.protocol = None  # type: ignore

        context = create_context()
        context.protocol = "aerodrome"

        enriched = enricher.enrich(result, intent, context)

        assert enriched.position_id == 42
        mock_registry.get.assert_called_once_with("aerodrome", chain="arbitrum")

    def test_intent_protocol_takes_precedence_over_context(self):
        """When both intent.protocol and context.protocol are set, intent wins."""
        mock_parser = Mock()
        mock_parser.extract_swap_amounts.return_value = SwapAmounts(
            amount_in=1, amount_out=1,
            amount_in_decimal=Decimal("1"), amount_out_decimal=Decimal("1"),
            effective_price=Decimal("1"),
        )

        mock_registry = Mock(spec=ReceiptParserRegistry)
        mock_registry.get.return_value = mock_parser

        enricher = ResultEnricher(parser_registry=mock_registry)
        result = create_execution_result(success=True)
        intent = MockSwapIntent()
        intent.protocol = "aerodrome"

        context = create_context()
        context.protocol = "uniswap_v3"  # Should be ignored

        enricher.enrich(result, intent, context)

        # Intent protocol should take precedence
        mock_registry.get.assert_called_once_with("aerodrome", chain="arbitrum")


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


class TestResultEnricherLogsOnlyReceipt:
    """Test enrichment via receipts that lack to_dict() (logs-attribute branch)."""

    def test_swap_enrichment_with_logs_only_receipt(self):
        """Swap enrichment should work when receipt has logs attribute but no to_dict()."""
        swap_amounts = SwapAmounts(
            amount_in=1000000,
            amount_out=500000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("0.5"),
        )

        mock_parser = Mock()
        mock_parser.extract_swap_amounts.return_value = swap_amounts

        mock_registry = Mock(spec=ReceiptParserRegistry)
        mock_registry.get.return_value = mock_parser

        enricher = ResultEnricher(parser_registry=mock_registry)
        result = create_execution_result_logs_only(
            success=True,
            logs=[{"topics": ["0xabc"], "data": "0x01"}],
            from_address="0xWallet",
        )
        intent = MockSwapIntent()
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        assert enriched.swap_amounts is swap_amounts
        # Verify receipt dict passed to parser contains from_address
        call_args = mock_parser.extract_swap_amounts.call_args
        receipt_dict = call_args[0][0]
        assert receipt_dict["from_address"] == "0xWallet"
        assert receipt_dict["logs"] == [{"topics": ["0xabc"], "data": "0x01"}]

    def test_lp_enrichment_with_logs_only_receipt(self):
        """LP position ID extraction should work with logs-only receipts."""
        mock_parser = Mock()
        mock_parser.extract_position_id.return_value = 99999

        mock_registry = Mock(spec=ReceiptParserRegistry)
        mock_registry.get.return_value = mock_parser

        enricher = ResultEnricher(parser_registry=mock_registry)
        result = create_execution_result_logs_only(success=True)
        intent = MockIntent()
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        assert enriched.position_id == 99999


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


class TestCollectReceiptsCamelCaseAliases:
    """Test that _collect_receipts adds camelCase aliases for receipt parsers (VIB-301)."""

    @dataclass
    class SnakeCaseReceipt:
        """Mimics TransactionReceipt.to_dict() output with snake_case keys."""

        tx_hash: str = "0xabc123"
        gas_used: int = 178803
        block_number: int = 12345678
        block_hash: str = "0xdef456"
        logs: list = field(default_factory=list)

        def to_dict(self) -> dict[str, Any]:
            return {
                "tx_hash": self.tx_hash,
                "block_number": self.block_number,
                "block_hash": self.block_hash,
                "gas_used": self.gas_used,
                "effective_gas_price": "50000000000",
                "status": 1,
                "logs": self.logs,
                "contract_address": None,
                "from_address": "0xsender",
                "to_address": "0xrecipient",
            }

    def _make_result_with_snake_receipt(self, logs=None):
        receipt = self.SnakeCaseReceipt(logs=logs or [])
        tx_result = TransactionResult(
            tx_hash="0xabc123",
            success=True,
            receipt=receipt,
            gas_used=178803,
        )
        return ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            transaction_results=[tx_result],
            total_gas_used=178803,
        )

    def test_camel_case_aliases_added(self):
        """Receipt dict should have camelCase aliases after _collect_receipts."""
        enricher = ResultEnricher()
        result = self._make_result_with_snake_receipt()

        receipts = enricher._collect_receipts(result)
        assert len(receipts) == 1

        r = receipts[0]
        # camelCase aliases should be present
        assert r["transactionHash"] == "0xabc123"
        assert r["gasUsed"] == 178803
        assert r["blockNumber"] == 12345678
        assert r["blockHash"] == "0xdef456"
        assert r["effectiveGasPrice"] == "50000000000"
        assert r["from"] == "0xsender"
        assert r["to"] == "0xrecipient"

    def test_snake_case_keys_preserved(self):
        """Original snake_case keys should still be present."""
        enricher = ResultEnricher()
        result = self._make_result_with_snake_receipt()

        receipts = enricher._collect_receipts(result)
        r = receipts[0]
        assert r["tx_hash"] == "0xabc123"
        assert r["gas_used"] == 178803
        assert r["block_number"] == 12345678

    def test_no_overwrite_when_camel_already_present(self):
        """If receipt already has camelCase keys, they should not be overwritten."""
        enricher = ResultEnricher()
        result = self._make_result_with_snake_receipt()

        # Manually add a camelCase key to the receipt dict
        tx_result = result.transaction_results[0]
        original_to_dict = tx_result.receipt.to_dict

        def patched_to_dict():
            d = original_to_dict()
            d["transactionHash"] = "0xoriginal_camel"
            return d

        tx_result.receipt.to_dict = patched_to_dict

        receipts = enricher._collect_receipts(result)
        r = receipts[0]
        # Should keep the existing camelCase value, not overwrite
        assert r["transactionHash"] == "0xoriginal_camel"

    def test_raw_dict_receipts_not_affected(self):
        """Raw dict receipts (already camelCase) should pass through unchanged."""
        enricher = ResultEnricher()

        raw_receipt = {
            "transactionHash": "0xfrom_web3",
            "gasUsed": 21000,
            "blockNumber": 999,
            "logs": [],
        }
        tx_result = TransactionResult(
            tx_hash="0xfrom_web3",
            success=True,
            receipt=raw_receipt,
            gas_used=21000,
        )
        result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            transaction_results=[tx_result],
            total_gas_used=21000,
        )

        receipts = enricher._collect_receipts(result)
        r = receipts[0]
        assert r["transactionHash"] == "0xfrom_web3"
        assert r["gasUsed"] == 21000
        # No snake_case keys should be added to a raw dict
        assert "tx_hash" not in r


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
        # Wallet address used as the swap sender
        wallet = "0x1234567890abcdef1234567890abcdef12345678"
        # Real Arbitrum token addresses so the resolver can find decimals
        usdc_addr = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # 6 decimals
        weth_addr = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"  # 18 decimals

        # Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
        swap_event = {
            "address": "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",  # Pool
            "topics": [
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",  # Swap
                "0x000000000000000000000000" + wallet.removeprefix("0x"),  # sender
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

        # ERC-20 Transfer events so _extract_swap_tokens_from_transfers can resolve decimals
        transfer_out = _make_transfer_log(usdc_addr, wallet, "0x9876543210fedcba9876543210fedcba98765432", 1000000)
        transfer_in = _make_transfer_log(weth_addr, "0x9876543210fedcba9876543210fedcba98765432", wallet, 1000000)

        enricher = ResultEnricher()
        result = create_execution_result(
            success=True,
            logs=[transfer_out, swap_event, transfer_in],
            from_address=wallet,
        )
        intent = MockSwapIntent()
        context = create_context()

        enriched = enricher.enrich(result, intent, context)

        assert enriched.swap_amounts is not None
        assert enriched.swap_amounts.amount_in > 0

    def test_enriches_agni_swap_on_mantle(self):
        """Agni Finance (V3 fork) swap enrichment on Mantle should work (VIB-1653).

        Agni Finance is registered as 'agni_finance' in the receipt registry,
        mapping to UniswapV3ReceiptParser. This test verifies the full enrichment
        pipeline works with:
        - protocol='agni_finance' on the intent
        - chain='mantle' on the context
        - A real Uniswap V3 Swap event receipt
        """
        wallet = "0x1111111111111111111111111111111111111111"
        # Real Mantle token addresses
        usdc_addr = "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9"  # USDC on Mantle
        weth_addr = "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111"  # WETH on Mantle

        swap_event = {
            "address": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "topics": [
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",  # Swap
                "0x000000000000000000000000" + wallet.removeprefix("0x"),  # sender
                "0x0000000000000000000000002222222222222222222222222222222222222222",  # recipient
            ],
            "data": (
                "0x"
                "00000000000000000000000000000000000000000000000000000000002faf08"  # amount0 = 3_125_000 (USDC in, 6 dec)
                "fffffffffffffffffffffffffffffffffffffffffffffffffff3e8948e1e0000"  # amount1 = negative (WETH out)
                "0000000000000000000000000000000000000001000000000000000000000000"  # sqrtPriceX96
                "00000000000000000000000000000000000000000000000000000000000186a0"  # liquidity
                "0000000000000000000000000000000000000000000000000000000000000000"  # tick
            ),
        }

        transfer_out = _make_transfer_log(usdc_addr, wallet, "0x2222222222222222222222222222222222222222", 3_125_000)
        transfer_in = _make_transfer_log(weth_addr, "0x2222222222222222222222222222222222222222", wallet, 3_400_000_000_000_000_000)

        enricher = ResultEnricher()
        result = create_execution_result(
            success=True,
            logs=[transfer_out, swap_event, transfer_in],
            from_address=wallet,
        )

        @dataclass
        class AgniSwapIntent:
            intent_type: str = "SWAP"
            protocol: str = "agni_finance"
            intent_id: str = "test-agni-swap"

        intent = AgniSwapIntent()
        context = ExecutionContext(
            strategy_id="test-strategy",
            chain="mantle",
            wallet_address=wallet,
            protocol="agni_finance",
        )

        enriched = enricher.enrich(result, intent, context)

        assert enriched.swap_amounts is not None, "swap_amounts should be enriched for agni_finance on Mantle"
        assert enriched.swap_amounts.amount_in > 0

    def test_enriches_agni_swap_via_context_protocol_fallback(self):
        """Agni swap enrichment should work when intent.protocol is None (VIB-1653).

        When strategy authors omit protocol on the intent, the runner sets
        context.protocol = compiler.default_protocol. On Mantle, this resolves
        to 'agni_finance'. Enrichment must still work via this fallback path.
        """
        wallet = "0x1111111111111111111111111111111111111111"
        usdc_addr = "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9"  # USDC on Mantle
        weth_addr = "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111"  # WETH on Mantle

        swap_event = {
            "address": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "topics": [
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                "0x000000000000000000000000" + wallet.removeprefix("0x"),
                "0x0000000000000000000000002222222222222222222222222222222222222222",
            ],
            "data": (
                "0x"
                "00000000000000000000000000000000000000000000000000000000000f4240"  # amount0 = 1_000_000
                "ffffffffffffffffffffffffffffffffffffffffffffffffffff0000000f423f"  # amount1 = negative
                "0000000000000000000000000000000000000001000000000000000000000000"
                "00000000000000000000000000000000000000000000000000000000000186a0"
                "0000000000000000000000000000000000000000000000000000000000000000"
            ),
        }

        transfer_out = _make_transfer_log(usdc_addr, wallet, "0x2222222222222222222222222222222222222222", 1_000_000)
        transfer_in = _make_transfer_log(weth_addr, "0x2222222222222222222222222222222222222222", wallet, 1_000_000)

        enricher = ResultEnricher()
        result = create_execution_result(
            success=True,
            logs=[transfer_out, swap_event, transfer_in],
            from_address=wallet,
        )

        @dataclass
        class NoProtocolIntent:
            intent_type: str = "SWAP"
            protocol: str | None = None
            intent_id: str = "test-no-protocol"

        intent = NoProtocolIntent()
        context = ExecutionContext(
            strategy_id="test-strategy",
            chain="mantle",
            wallet_address=wallet,
            protocol="agni_finance",  # Set by runner from compiler.default_protocol
        )

        enriched = enricher.enrich(result, intent, context)

        assert enriched.swap_amounts is not None, (
            "swap_amounts should be enriched via context.protocol fallback"
        )
        assert enriched.swap_amounts.amount_in > 0

    def test_enriches_swap_with_approve_plus_swap_receipts(self):
        """Enrichment should work with 2 TXs: approve (no Swap event) + swap (VIB-1653).

        Strategy executions on Agni often produce 2 transactions: an ERC-20
        approve followed by the actual swap. The enricher must try each receipt
        and find swap_amounts from the second one.
        """
        wallet = "0x1111111111111111111111111111111111111111"
        usdc_addr = "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9"  # USDC on Mantle
        weth_addr = "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111"  # WETH on Mantle

        approve_log = {
            "address": usdc_addr,
            "topics": [
                "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",  # Approval
                "0x000000000000000000000000" + wallet.removeprefix("0x"),
                "0x0000000000000000000000002222222222222222222222222222222222222222",
            ],
            "data": "0x00000000000000000000000000000000000000000000000000000000ffffffff",
        }

        swap_log = {
            "address": "0x3333333333333333333333333333333333333333",
            "topics": [
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                "0x000000000000000000000000" + wallet.removeprefix("0x"),
                "0x0000000000000000000000002222222222222222222222222222222222222222",
            ],
            "data": (
                "0x"
                "00000000000000000000000000000000000000000000000000000000002faf08"
                "fffffffffffffffffffffffffffffffffffffffffffffffffff3e8948e1e0000"
                "0000000000000000000000000000000000000001000000000000000000000000"
                "00000000000000000000000000000000000000000000000000000000000186a0"
                "0000000000000000000000000000000000000000000000000000000000000000"
            ),
        }

        # Transfer events in the swap receipt
        transfer_out = _make_transfer_log(usdc_addr, wallet, "0x2222222222222222222222222222222222222222", 3_125_000)
        transfer_in = _make_transfer_log(weth_addr, "0x2222222222222222222222222222222222222222", wallet, 3_400_000_000_000_000_000)

        # Create 2 transaction results: approve + swap
        approve_receipt = MockReceipt(logs=[approve_log], from_address=wallet)
        approve_tx = TransactionResult(tx_hash="0xapprove", success=True, receipt=approve_receipt, gas_used=46000)
        swap_receipt = MockReceipt(logs=[transfer_out, swap_log, transfer_in], from_address=wallet)
        swap_tx = TransactionResult(tx_hash="0xswap", success=True, receipt=swap_receipt, gas_used=180000)

        result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            transaction_results=[approve_tx, swap_tx],
            total_gas_used=226000,
        )

        enricher = ResultEnricher()
        intent = MockSwapIntent(protocol="agni_finance")
        context = ExecutionContext(
            strategy_id="test-strategy",
            chain="mantle",
            wallet_address=wallet,
            protocol="agni_finance",
        )

        enriched = enricher.enrich(result, intent, context)

        assert enriched.swap_amounts is not None, (
            "swap_amounts should be extracted from swap TX even when approve TX comes first"
        )
        assert enriched.swap_amounts.amount_in > 0
