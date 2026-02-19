"""Tests for Aerodrome Receipt Parser.

This test suite covers:
- Event parsing (Swap, Mint, Burn, Transfer)
- Receipt parsing
- Data extraction and conversion
- Symbol and decimal resolution
"""

from decimal import Decimal

import pytest

from ..receipt_parser import (
    BURN_EVENT_TOPIC,
    MINT_EVENT_TOPIC,
    SWAP_EVENT_TOPIC,
    TOKEN_ADDRESSES,
    TOKEN_DECIMALS,
    TOPIC_TO_EVENT,
    AerodromeEvent,
    AerodromeEventType,
    AerodromeReceiptParser,
    ParsedLiquidityResult,
    ParsedSwapResult,
    ParseResult,
    SwapEventData,
)

# =============================================================================
# Parser Initialization Tests
# =============================================================================


class TestParserInit:
    """Tests for parser initialization."""

    def test_parser_creation(self) -> None:
        """Test parser creation with default values."""
        parser = AerodromeReceiptParser(chain="base")

        assert parser.chain == "base"
        assert parser.stable is False

    def test_parser_with_token_info(self) -> None:
        """Test parser creation with token information."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC
            token1_address="0x4200000000000000000000000000000000000006",  # WETH
        )

        assert parser.token0_symbol == "USDC"
        assert parser.token1_symbol == "WETH"
        assert parser.token0_decimals == 6
        assert parser.token1_decimals == 18

    def test_parser_with_explicit_symbols(self) -> None:
        """Test parser with explicit token symbols.

        Note: Decimals are only overridden from defaults (18) if a symbol
        can be resolved. Explicit non-default decimals are preserved.
        """
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address="0x0000000000000000000000000000000000000001",
            token0_symbol="TOKEN0",
            token0_decimals=8,  # Non-default, will be preserved
            token1_address="0x0000000000000000000000000000000000000002",
            token1_symbol="TOKEN1",
            token1_decimals=12,  # Non-default, will be preserved
        )

        assert parser.token0_symbol == "TOKEN0"
        assert parser.token1_symbol == "TOKEN1"
        # Non-default decimals are preserved
        assert parser.token0_decimals == 8
        assert parser.token1_decimals == 12

    def test_parser_with_stable_flag(self) -> None:
        """Test parser with stable pool flag."""
        parser = AerodromeReceiptParser(chain="base", stable=True)

        assert parser.stable is True

    def test_parser_with_quoted_price(self) -> None:
        """Test parser with quoted price for slippage calculation."""
        parser = AerodromeReceiptParser(
            chain="base",
            quoted_price=Decimal("2000"),
        )

        assert parser.quoted_price == Decimal("2000")


# =============================================================================
# Event Topic Tests
# =============================================================================


class TestEventTopics:
    """Tests for event topics."""

    def test_swap_event_topic(self) -> None:
        """Test Swap event topic is defined."""
        assert SWAP_EVENT_TOPIC is not None
        assert SWAP_EVENT_TOPIC.startswith("0x")

    def test_mint_event_topic(self) -> None:
        """Test Mint event topic is defined."""
        assert MINT_EVENT_TOPIC is not None
        assert MINT_EVENT_TOPIC.startswith("0x")

    def test_burn_event_topic(self) -> None:
        """Test Burn event topic is defined."""
        assert BURN_EVENT_TOPIC is not None
        assert BURN_EVENT_TOPIC.startswith("0x")

    def test_topic_to_event_mapping(self) -> None:
        """Test topic to event mapping."""
        assert TOPIC_TO_EVENT[SWAP_EVENT_TOPIC] == "Swap"
        assert TOPIC_TO_EVENT[MINT_EVENT_TOPIC] == "Mint"
        assert TOPIC_TO_EVENT[BURN_EVENT_TOPIC] == "Burn"

    def test_event_type_detection(self) -> None:
        """Test event type detection from topic."""
        parser = AerodromeReceiptParser(chain="base")

        assert parser.get_event_type(SWAP_EVENT_TOPIC) == AerodromeEventType.SWAP
        assert parser.get_event_type(MINT_EVENT_TOPIC) == AerodromeEventType.MINT
        assert parser.get_event_type(BURN_EVENT_TOPIC) == AerodromeEventType.BURN

    def test_is_aerodrome_event(self) -> None:
        """Test Aerodrome event detection."""
        parser = AerodromeReceiptParser(chain="base")

        assert parser.is_aerodrome_event(SWAP_EVENT_TOPIC) is True
        assert parser.is_aerodrome_event(MINT_EVENT_TOPIC) is True
        assert parser.is_aerodrome_event("0x0000000000000000000000000000000000000000") is False


# =============================================================================
# Swap Event Data Tests
# =============================================================================


class TestSwapEventData:
    """Tests for SwapEventData."""

    def test_token0_is_input(self) -> None:
        """Test detecting token0 as input."""
        data = SwapEventData(
            sender="0x1234",
            to="0x5678",
            amount0_in=1000,
            amount1_in=0,
            amount0_out=0,
            amount1_out=500,
            pool_address="0xpool",
        )

        assert data.token0_is_input is True
        assert data.amount_in == 1000
        assert data.amount_out == 500

    def test_token1_is_input(self) -> None:
        """Test detecting token1 as input."""
        data = SwapEventData(
            sender="0x1234",
            to="0x5678",
            amount0_in=0,
            amount1_in=1000,
            amount0_out=500,
            amount1_out=0,
            pool_address="0xpool",
        )

        assert data.token0_is_input is False
        assert data.amount_in == 1000
        assert data.amount_out == 500

    def test_to_dict(self) -> None:
        """Test SwapEventData serialization."""
        data = SwapEventData(
            sender="0x1234",
            to="0x5678",
            amount0_in=1000,
            amount1_in=0,
            amount0_out=0,
            amount1_out=500,
            pool_address="0xpool",
        )

        result = data.to_dict()

        assert result["sender"] == "0x1234"
        assert result["to"] == "0x5678"
        assert result["amount0_in"] == "1000"
        assert result["amount1_out"] == "500"
        assert result["token0_is_input"] is True

    def test_from_dict(self) -> None:
        """Test SwapEventData deserialization."""
        dict_data = {
            "sender": "0x1234",
            "to": "0x5678",
            "amount0_in": "1000",
            "amount1_in": "0",
            "amount0_out": "0",
            "amount1_out": "500",
            "pool_address": "0xpool",
        }

        data = SwapEventData.from_dict(dict_data)

        assert data.sender == "0x1234"
        assert data.amount0_in == 1000
        assert data.amount1_out == 500


# =============================================================================
# Receipt Parsing Tests
# =============================================================================


class TestReceiptParsing:
    """Tests for receipt parsing."""

    @pytest.fixture
    def parser(self) -> AerodromeReceiptParser:
        """Create parser fixture."""
        return AerodromeReceiptParser(
            chain="base",
            token0_address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC
            token1_address="0x4200000000000000000000000000000000000006",  # WETH
        )

    def test_parse_empty_receipt(self, parser: AerodromeReceiptParser) -> None:
        """Test parsing empty receipt."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "status": 1,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash == "0x1234"
        assert result.block_number == 12345
        assert result.transaction_success is True
        assert len(result.events) == 0

    def test_parse_failed_transaction(self, parser: AerodromeReceiptParser) -> None:
        """Test parsing failed transaction."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "status": 0,  # Failed
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        assert result.error == "Transaction reverted"

    def test_parse_swap_receipt(self, parser: AerodromeReceiptParser) -> None:
        """Test parsing swap receipt with mock data."""
        # Create mock swap log
        swap_log = {
            "address": "0xPoolAddress",
            "topics": [
                SWAP_EVENT_TOPIC,
                "0x" + "00" * 12 + "1234567890123456789012345678901234567890",  # sender
                "0x" + "00" * 12 + "1234567890123456789012345678901234567890",  # to
            ],
            "data": (
                "0x"
                + "0000000000000000000000000000000000000000000000000000000000000001"  # amount0In = 1
                + "0000000000000000000000000000000000000000000000000000000000000000"  # amount1In = 0
                + "0000000000000000000000000000000000000000000000000000000000000000"  # amount0Out = 0
                + "0000000000000000000000000000000000000000000000000000000000000002"  # amount1Out = 2
            ),
            "logIndex": 0,
        }

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "status": 1,
            "logs": [swap_log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.swap_events) == 1
        assert result.swap_events[0].amount0_in == 1
        assert result.swap_events[0].amount1_out == 2

    def test_parse_receipt_with_bytes(self, parser: AerodromeReceiptParser) -> None:
        """Test parsing receipt with bytes values."""
        receipt = {
            "transactionHash": bytes.fromhex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
            "blockNumber": 12345,
            "status": 1,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash.startswith("0x")


# =============================================================================
# Parsed Result Tests
# =============================================================================


class TestParsedSwapResult:
    """Tests for ParsedSwapResult."""

    def test_parsed_swap_result_to_dict(self) -> None:
        """Test ParsedSwapResult serialization."""
        result = ParsedSwapResult(
            token_in="0x1234",
            token_out="0x5678",
            token_in_symbol="USDC",
            token_out_symbol="WETH",
            amount_in=1000000,
            amount_out=500,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.0005"),
            effective_price=Decimal("0.0005"),
            slippage_bps=10,
            pool_address="0xpool",
            stable=False,
        )

        dict_result = result.to_dict()

        assert dict_result["token_in_symbol"] == "USDC"
        assert dict_result["token_out_symbol"] == "WETH"
        assert dict_result["amount_in"] == "1000000"
        assert dict_result["stable"] is False

    def test_parsed_swap_result_from_dict(self) -> None:
        """Test ParsedSwapResult deserialization."""
        dict_data = {
            "token_in": "0x1234",
            "token_out": "0x5678",
            "token_in_symbol": "USDC",
            "token_out_symbol": "WETH",
            "amount_in": "1000000",
            "amount_out": "500",
            "amount_in_decimal": "1.0",
            "amount_out_decimal": "0.0005",
            "effective_price": "0.0005",
            "slippage_bps": 10,
            "pool_address": "0xpool",
            "stable": False,
        }

        result = ParsedSwapResult.from_dict(dict_data)

        assert result.token_in_symbol == "USDC"
        assert result.amount_in == 1000000
        assert result.effective_price == Decimal("0.0005")

    def test_to_swap_result_payload(self) -> None:
        """Test conversion to SwapResultPayload."""
        result = ParsedSwapResult(
            token_in="0x1234",
            token_out="0x5678",
            token_in_symbol="USDC",
            token_out_symbol="WETH",
            amount_in=1000000,
            amount_out=500,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.0005"),
            effective_price=Decimal("0.0005"),
            slippage_bps=10,
            pool_address="0xpool",
        )

        payload = result.to_swap_result_payload()

        assert payload.token_in == "USDC"
        assert payload.token_out == "WETH"
        assert payload.amount_in == Decimal("1.0")
        assert payload.slippage_bps == 10


# =============================================================================
# Liquidity Result Tests
# =============================================================================


class TestParsedLiquidityResult:
    """Tests for ParsedLiquidityResult."""

    def test_add_liquidity_result(self) -> None:
        """Test add liquidity result."""
        result = ParsedLiquidityResult(
            operation="add",
            token0="0x1234",
            token1="0x5678",
            token0_symbol="USDC",
            token1_symbol="WETH",
            amount0=1000000,
            amount1=500,
            liquidity=10000,
            pool_address="0xpool",
            stable=False,
        )

        assert result.operation == "add"
        assert result.liquidity == 10000

    def test_remove_liquidity_result(self) -> None:
        """Test remove liquidity result."""
        result = ParsedLiquidityResult(
            operation="remove",
            token0="0x1234",
            token1="0x5678",
            token0_symbol="USDC",
            token1_symbol="WETH",
            amount0=1000000,
            amount1=500,
            liquidity=10000,
            pool_address="0xpool",
            stable=True,
        )

        assert result.operation == "remove"
        assert result.stable is True

    def test_to_dict(self) -> None:
        """Test serialization."""
        result = ParsedLiquidityResult(
            operation="add",
            token0="0x1234",
            token1="0x5678",
            token0_symbol="USDC",
            token1_symbol="WETH",
            amount0=1000000,
            amount1=500,
            liquidity=10000,
            pool_address="0xpool",
            stable=False,
        )

        dict_result = result.to_dict()

        assert dict_result["operation"] == "add"
        assert dict_result["amount0"] == "1000000"
        assert dict_result["liquidity"] == "10000"


# =============================================================================
# Token Constants Tests
# =============================================================================


class TestTokenConstants:
    """Tests for token constants."""

    def test_token_addresses_has_base(self) -> None:
        """Test TOKEN_ADDRESSES includes Base chain."""
        assert "base" in TOKEN_ADDRESSES
        assert len(TOKEN_ADDRESSES["base"]) > 0

    def test_token_decimals_correct(self) -> None:
        """Test TOKEN_DECIMALS are correct (keys normalized to uppercase)."""
        assert TOKEN_DECIMALS["USDC"] == 6
        assert TOKEN_DECIMALS["USDBC"] == 6  # Bridged USDC, normalized to uppercase
        assert TOKEN_DECIMALS["WETH"] == 18
        assert TOKEN_DECIMALS["DAI"] == 18

    def test_symbol_resolution(self) -> None:
        """Test symbol resolution from address."""
        parser = AerodromeReceiptParser(chain="base")

        usdc_addr = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        symbol = parser._resolve_symbol(usdc_addr)

        assert symbol == "USDC"


# =============================================================================
# ParseResult Tests
# =============================================================================


class TestParseResult:
    """Tests for ParseResult."""

    def test_parse_result_to_dict(self) -> None:
        """Test ParseResult serialization."""
        result = ParseResult(
            success=True,
            transaction_hash="0x1234",
            block_number=12345,
            transaction_success=True,
        )

        dict_result = result.to_dict()

        assert dict_result["success"] is True
        assert dict_result["transaction_hash"] == "0x1234"
        assert dict_result["block_number"] == 12345

    def test_parse_result_with_events(self) -> None:
        """Test ParseResult with events."""
        event = AerodromeEvent(
            event_type=AerodromeEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="0x1234",
            block_number=12345,
            contract_address="0xpool",
            data={},
        )

        result = ParseResult(
            success=True,
            events=[event],
            transaction_hash="0x1234",
            block_number=12345,
            transaction_success=True,
        )

        assert len(result.events) == 1
        assert result.events[0].event_type == AerodromeEventType.SWAP


# =============================================================================
# AerodromeEvent Tests
# =============================================================================


class TestAerodromeEvent:
    """Tests for AerodromeEvent."""

    def test_event_to_dict(self) -> None:
        """Test AerodromeEvent serialization."""
        event = AerodromeEvent(
            event_type=AerodromeEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="0x1234",
            block_number=12345,
            contract_address="0xpool",
            data={"key": "value"},
            raw_topics=["0xtopic1", "0xtopic2"],
            raw_data="0xdata",
        )

        dict_event = event.to_dict()

        assert dict_event["event_type"] == "SWAP"
        assert dict_event["event_name"] == "Swap"
        assert dict_event["log_index"] == 0
        assert dict_event["data"]["key"] == "value"

    def test_event_from_dict(self) -> None:
        """Test AerodromeEvent deserialization."""
        dict_data = {
            "event_type": "SWAP",
            "event_name": "Swap",
            "log_index": 0,
            "transaction_hash": "0x1234",
            "block_number": 12345,
            "contract_address": "0xpool",
            "data": {"key": "value"},
            "raw_topics": ["0xtopic1"],
            "raw_data": "0xdata",
            "timestamp": "2024-01-01T00:00:00+00:00",
        }

        event = AerodromeEvent.from_dict(dict_data)

        assert event.event_type == AerodromeEventType.SWAP
        assert event.event_name == "Swap"
        assert event.data["key"] == "value"
