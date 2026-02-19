"""Tests for Uniswap V3 Receipt Parser.

This test suite covers:
- Event parsing from receipts
- Swap event parsing and amount extraction
- Transfer event parsing
- Effective price calculation
- Slippage calculation
- Event type detection
- Failed transaction handling
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from ..receipt_parser import (
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    SWAP_EVENT_TOPIC,
    TOKEN_ADDRESSES,
    TOKEN_DECIMALS,
    TOPIC_TO_EVENT,
    ParsedSwapResult,
    ParseResult,
    SwapEventData,
    UniswapV3Event,
    UniswapV3EventType,
    UniswapV3ReceiptParser,
)

# =============================================================================
# Test Constants
# =============================================================================

# Sample addresses
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
TEST_POOL_ADDRESS = "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443"
TEST_WALLET = "0x1234567890123456789012345678901234567890"
TEST_TX_HASH = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"


def encode_swap_data(
    amount0: int,
    amount1: int,
    sqrt_price_x96: int = 79228162514264337593543950336,  # ~1.0
    liquidity: int = 1000000000000000000,
    tick: int = 0,
) -> str:
    """Encode swap event data for testing."""

    def encode_int256(val: int) -> str:
        if val < 0:
            val = val + 2**256
        return hex(val)[2:].zfill(64)

    def encode_uint256(val: int) -> str:
        return hex(val)[2:].zfill(64)

    return (
        "0x"
        + encode_int256(amount0)
        + encode_int256(amount1)
        + encode_uint256(sqrt_price_x96)
        + encode_uint256(liquidity)
        + encode_int256(tick)
    )


def create_swap_log(
    amount0: int,
    amount1: int,
    sender: str = TEST_WALLET,
    recipient: str = TEST_WALLET,
    pool_address: str = TEST_POOL_ADDRESS,
    log_index: int = 0,
) -> dict:
    """Create a swap log for testing."""
    return {
        "topics": [
            SWAP_EVENT_TOPIC,
            sender.lower().replace("0x", "").zfill(64),
            recipient.lower().replace("0x", "").zfill(64),
        ],
        "data": encode_swap_data(amount0, amount1),
        "address": pool_address,
        "logIndex": log_index,
    }


def create_transfer_log(
    from_addr: str,
    to_addr: str,
    value: int,
    token_address: str,
    log_index: int = 0,
) -> dict:
    """Create a transfer log for testing."""
    return {
        "topics": [
            EVENT_TOPICS["Transfer"],
            from_addr.lower().replace("0x", "").zfill(64),
            to_addr.lower().replace("0x", "").zfill(64),
        ],
        "data": hex(value)[2:].zfill(64),
        "address": token_address,
        "logIndex": log_index,
    }


# =============================================================================
# UniswapV3Event Tests
# =============================================================================


class TestUniswapV3Event:
    """Tests for UniswapV3Event dataclass."""

    def test_event_creation(self) -> None:
        """Test event creation."""
        event = UniswapV3Event(
            event_type=UniswapV3EventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash=TEST_TX_HASH,
            block_number=12345678,
            contract_address=TEST_POOL_ADDRESS,
            data={"key": "value"},
        )

        assert event.event_type == UniswapV3EventType.SWAP
        assert event.event_name == "Swap"
        assert event.log_index == 0
        assert event.transaction_hash == TEST_TX_HASH
        assert event.block_number == 12345678

    def test_event_to_dict(self) -> None:
        """Test event serialization."""
        event = UniswapV3Event(
            event_type=UniswapV3EventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash=TEST_TX_HASH,
            block_number=12345678,
            contract_address=TEST_POOL_ADDRESS,
            data={"key": "value"},
        )

        event_dict = event.to_dict()

        assert event_dict["event_type"] == "SWAP"
        assert event_dict["event_name"] == "Swap"
        assert event_dict["data"] == {"key": "value"}

    def test_event_from_dict(self) -> None:
        """Test event deserialization."""
        data = {
            "event_type": "SWAP",
            "event_name": "Swap",
            "log_index": 0,
            "transaction_hash": TEST_TX_HASH,
            "block_number": 12345678,
            "contract_address": TEST_POOL_ADDRESS,
            "data": {"key": "value"},
            "timestamp": datetime.now(UTC).isoformat(),
        }

        event = UniswapV3Event.from_dict(data)

        assert event.event_type == UniswapV3EventType.SWAP
        assert event.event_name == "Swap"
        assert event.data == {"key": "value"}


# =============================================================================
# SwapEventData Tests
# =============================================================================


class TestSwapEventData:
    """Tests for SwapEventData dataclass."""

    def test_swap_data_creation_token0_input(self) -> None:
        """Test swap data creation when token0 is input."""
        data = SwapEventData(
            sender=TEST_WALLET,
            recipient=TEST_WALLET,
            amount0=1000000,  # Positive = input
            amount1=-2000000,  # Negative = output
            sqrt_price_x96=79228162514264337593543950336,
            liquidity=1000000000000000000,
            tick=0,
            pool_address=TEST_POOL_ADDRESS,
        )

        assert data.token0_is_input is True
        assert data.token1_is_input is False
        assert data.amount_in == 1000000
        assert data.amount_out == 2000000

    def test_swap_data_creation_token1_input(self) -> None:
        """Test swap data creation when token1 is input."""
        data = SwapEventData(
            sender=TEST_WALLET,
            recipient=TEST_WALLET,
            amount0=-1000000,  # Negative = output
            amount1=2000000,  # Positive = input
            sqrt_price_x96=79228162514264337593543950336,
            liquidity=1000000000000000000,
            tick=0,
            pool_address=TEST_POOL_ADDRESS,
        )

        assert data.token0_is_input is False
        assert data.token1_is_input is True
        assert data.amount_in == 2000000
        assert data.amount_out == 1000000

    def test_swap_data_to_dict(self) -> None:
        """Test swap data serialization."""
        data = SwapEventData(
            sender=TEST_WALLET,
            recipient=TEST_WALLET,
            amount0=1000000,
            amount1=-2000000,
            sqrt_price_x96=79228162514264337593543950336,
            liquidity=1000000000000000000,
            tick=100,
            pool_address=TEST_POOL_ADDRESS,
        )

        data_dict = data.to_dict()

        assert data_dict["amount0"] == "1000000"
        assert data_dict["amount1"] == "-2000000"
        assert data_dict["tick"] == 100
        assert data_dict["token0_is_input"] is True


# =============================================================================
# ParsedSwapResult Tests
# =============================================================================


class TestParsedSwapResult:
    """Tests for ParsedSwapResult dataclass."""

    def test_swap_result_creation(self) -> None:
        """Test swap result creation."""
        result = ParsedSwapResult(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            token_in_symbol="WETH",
            token_out_symbol="USDC",
            amount_in=1000000000000000000,  # 1 WETH
            amount_out=2000000000,  # 2000 USDC
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("2000.0"),
            effective_price=Decimal("2000.0"),
            slippage_bps=25,
            pool_address=TEST_POOL_ADDRESS,
        )

        assert result.token_in_symbol == "WETH"
        assert result.token_out_symbol == "USDC"
        assert result.effective_price == Decimal("2000.0")
        assert result.slippage_bps == 25

    def test_swap_result_to_dict(self) -> None:
        """Test swap result serialization."""
        result = ParsedSwapResult(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            token_in_symbol="WETH",
            token_out_symbol="USDC",
            amount_in=1000000000000000000,
            amount_out=2000000000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("2000.0"),
            effective_price=Decimal("2000.0"),
            slippage_bps=25,
            pool_address=TEST_POOL_ADDRESS,
        )

        data = result.to_dict()

        assert data["token_in"] == WETH_ADDRESS
        assert data["effective_price"] == "2000.0"
        assert data["slippage_bps"] == 25

    def test_swap_result_to_payload(self) -> None:
        """Test conversion to SwapResultPayload."""
        result = ParsedSwapResult(
            token_in=WETH_ADDRESS,
            token_out=USDC_ADDRESS,
            token_in_symbol="WETH",
            token_out_symbol="USDC",
            amount_in=1000000000000000000,
            amount_out=2000000000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("2000.0"),
            effective_price=Decimal("2000.0"),
            slippage_bps=25,
            pool_address=TEST_POOL_ADDRESS,
        )

        payload = result.to_swap_result_payload()

        assert payload.token_in == "WETH"
        assert payload.token_out == "USDC"
        assert payload.amount_in == Decimal("1.0")
        assert payload.amount_out == Decimal("2000.0")
        assert payload.slippage_bps == 25


# =============================================================================
# ParseResult Tests
# =============================================================================


class TestParseResult:
    """Tests for ParseResult dataclass."""

    def test_parse_result_success(self) -> None:
        """Test successful parse result."""
        result = ParseResult(
            success=True,
            transaction_hash=TEST_TX_HASH,
            block_number=12345678,
        )

        assert result.success is True
        assert result.error is None
        assert len(result.events) == 0

    def test_parse_result_failure(self) -> None:
        """Test failed parse result."""
        result = ParseResult(
            success=False,
            error="Parse error",
        )

        assert result.success is False
        assert result.error == "Parse error"

    def test_parse_result_with_events(self) -> None:
        """Test parse result with events."""
        event = UniswapV3Event(
            event_type=UniswapV3EventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash=TEST_TX_HASH,
            block_number=12345678,
            contract_address=TEST_POOL_ADDRESS,
            data={},
        )

        result = ParseResult(
            success=True,
            events=[event],
            transaction_hash=TEST_TX_HASH,
            block_number=12345678,
        )

        assert len(result.events) == 1
        assert result.events[0].event_type == UniswapV3EventType.SWAP

    def test_parse_result_to_dict(self) -> None:
        """Test parse result serialization."""
        result = ParseResult(
            success=True,
            transaction_hash=TEST_TX_HASH,
            block_number=12345678,
        )

        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert result_dict["transaction_hash"] == TEST_TX_HASH


# =============================================================================
# UniswapV3ReceiptParser Tests
# =============================================================================


class TestUniswapV3ReceiptParser:
    """Tests for UniswapV3ReceiptParser."""

    @pytest.fixture
    def parser(self) -> UniswapV3ReceiptParser:
        """Create parser for testing."""
        return UniswapV3ReceiptParser(
            chain="arbitrum",
            token0_address=WETH_ADDRESS,
            token1_address=USDC_ADDRESS,
            token0_symbol="WETH",
            token1_symbol="USDC",
            token0_decimals=18,
            token1_decimals=6,
        )

    def test_parser_creation(self, parser: UniswapV3ReceiptParser) -> None:
        """Test parser creation."""
        assert parser is not None
        assert parser.chain == "arbitrum"
        assert parser.token0_symbol == "WETH"
        assert parser.token1_symbol == "USDC"
        assert len(parser._known_topics) > 0

    def test_parser_resolves_symbols(self) -> None:
        """Test parser resolves symbols from addresses."""
        parser = UniswapV3ReceiptParser(
            chain="arbitrum",
            token0_address=WETH_ADDRESS,
            token1_address=USDC_ADDRESS,
        )

        assert parser.token0_symbol == "WETH"
        assert parser.token1_symbol == "USDC"
        assert parser.token0_decimals == 18
        assert parser.token1_decimals == 6

    def test_parse_empty_receipt(self, parser: UniswapV3ReceiptParser) -> None:
        """Test parsing empty receipt."""
        receipt = {
            "transactionHash": TEST_TX_HASH,
            "blockNumber": 12345678,
            "status": 1,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0
        assert result.swap_result is None

    def test_parse_failed_transaction(self, parser: UniswapV3ReceiptParser) -> None:
        """Test parsing failed transaction."""
        receipt = {
            "transactionHash": TEST_TX_HASH,
            "blockNumber": 12345678,
            "status": 0,  # Failed
            "logs": [create_swap_log(1000000, -2000000)],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        assert result.error == "Transaction reverted"
        assert result.swap_result is None

    def test_parse_receipt_with_swap_token0_input(self, parser: UniswapV3ReceiptParser) -> None:
        """Test parsing receipt with Swap event (token0 as input)."""
        # Token0 (WETH) input: amount0 positive, amount1 negative
        # 1 WETH -> 2000 USDC (after decimals adjustment)
        amount0 = 1000000000000000000  # 1e18 (1 WETH)
        amount1 = -2000000000  # -2e9 (2000 USDC, 6 decimals)

        receipt = {
            "transactionHash": TEST_TX_HASH,
            "blockNumber": 12345678,
            "status": 1,
            "logs": [create_swap_log(amount0, amount1)],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is True
        assert len(result.events) == 1
        assert len(result.swap_events) == 1
        assert result.events[0].event_type == UniswapV3EventType.SWAP

        # Check swap result
        assert result.swap_result is not None
        assert result.swap_result.token_in_symbol == "WETH"
        assert result.swap_result.token_out_symbol == "USDC"
        assert result.swap_result.amount_in == amount0
        assert result.swap_result.amount_out == abs(amount1)
        assert result.swap_result.amount_in_decimal == Decimal("1")
        assert result.swap_result.amount_out_decimal == Decimal("2000")
        assert result.swap_result.effective_price == Decimal("2000")

    def test_parse_receipt_with_swap_token1_input(self, parser: UniswapV3ReceiptParser) -> None:
        """Test parsing receipt with Swap event (token1 as input)."""
        # Token1 (USDC) input: amount1 positive, amount0 negative
        # 2000 USDC -> 1 WETH
        amount0 = -1000000000000000000  # -1e18 (1 WETH out)
        amount1 = 2000000000  # 2e9 (2000 USDC in)

        receipt = {
            "transactionHash": TEST_TX_HASH,
            "blockNumber": 12345678,
            "status": 1,
            "logs": [create_swap_log(amount0, amount1)],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.swap_result is not None
        assert result.swap_result.token_in_symbol == "USDC"
        assert result.swap_result.token_out_symbol == "WETH"
        assert result.swap_result.amount_in == amount1
        assert result.swap_result.amount_out == abs(amount0)

    def test_parse_receipt_with_slippage_calculation(self, parser: UniswapV3ReceiptParser) -> None:
        """Test slippage calculation from quoted amount."""
        amount0 = 1000000000000000000  # 1 WETH
        expected_out = 2050000000  # Expected 2050 USDC
        actual_out = -2000000000  # Got 2000 USDC (negative = out)

        receipt = {
            "transactionHash": TEST_TX_HASH,
            "blockNumber": 12345678,
            "status": 1,
            "logs": [create_swap_log(amount0, actual_out)],
        }

        result = parser.parse_receipt(receipt, quoted_amount_out=expected_out)

        assert result.success is True
        assert result.swap_result is not None
        # Slippage = (2050 - 2000) / 2050 * 10000 = ~244 bps
        assert result.swap_result.slippage_bps == 243  # ~2.43%

    def test_parse_receipt_with_transfer_events(self, parser: UniswapV3ReceiptParser) -> None:
        """Test parsing receipt with Transfer events."""
        swap_log = create_swap_log(1000000000000000000, -2000000000, log_index=2)
        transfer_in = create_transfer_log(
            TEST_WALLET,
            TEST_POOL_ADDRESS,
            1000000000000000000,
            WETH_ADDRESS,
            log_index=0,
        )
        transfer_out = create_transfer_log(
            TEST_POOL_ADDRESS,
            TEST_WALLET,
            2000000000,
            USDC_ADDRESS,
            log_index=1,
        )

        receipt = {
            "transactionHash": TEST_TX_HASH,
            "blockNumber": 12345678,
            "status": 1,
            "logs": [transfer_in, transfer_out, swap_log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 3
        assert len(result.transfer_events) == 2
        assert len(result.swap_events) == 1

    def test_parse_receipt_with_unknown_event(self, parser: UniswapV3ReceiptParser) -> None:
        """Test parsing receipt with unknown event."""
        log = {
            "topics": ["0xunknowntopic000000000000000000000000000000000000000000000000"],
            "data": "0x00",
            "address": TEST_POOL_ADDRESS,
            "logIndex": 0,
        }

        receipt = {
            "transactionHash": TEST_TX_HASH,
            "blockNumber": 12345678,
            "status": 1,
            "logs": [log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0  # Unknown events are skipped

    def test_parse_receipt_bytes_transaction_hash(self, parser: UniswapV3ReceiptParser) -> None:
        """Test parsing receipt with bytes transaction hash."""
        receipt = {
            "transactionHash": bytes.fromhex("1234567890abcdef" * 4),
            "blockNumber": 12345678,
            "status": 1,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash.startswith("0x")

    def test_parse_logs(self, parser: UniswapV3ReceiptParser) -> None:
        """Test parsing logs directly."""
        logs = [create_swap_log(1000000, -2000000)]

        events = parser.parse_logs(logs)

        assert len(events) == 1
        assert events[0].event_type == UniswapV3EventType.SWAP

    def test_is_uniswap_event(self, parser: UniswapV3ReceiptParser) -> None:
        """Test checking if topic is Uniswap V3 event."""
        assert parser.is_uniswap_event(SWAP_EVENT_TOPIC) is True
        assert parser.is_uniswap_event(EVENT_TOPICS["Transfer"]) is True
        assert parser.is_uniswap_event("0xunknown") is False

    def test_get_event_type(self, parser: UniswapV3ReceiptParser) -> None:
        """Test getting event type from topic."""
        assert parser.get_event_type(SWAP_EVENT_TOPIC) == UniswapV3EventType.SWAP
        assert parser.get_event_type(EVENT_TOPICS["Transfer"]) == UniswapV3EventType.TRANSFER
        assert parser.get_event_type("0xunknown") == UniswapV3EventType.UNKNOWN


# =============================================================================
# Real Transaction Receipt Tests
# =============================================================================


class TestRealReceiptSamples:
    """Tests using patterns from real transaction receipts."""

    @pytest.fixture
    def parser(self) -> UniswapV3ReceiptParser:
        """Create parser for testing."""
        return UniswapV3ReceiptParser(
            chain="arbitrum",
            token0_address=WETH_ADDRESS,
            token1_address=USDC_ADDRESS,
        )

    def test_realistic_weth_to_usdc_swap(self, parser: UniswapV3ReceiptParser) -> None:
        """Test with realistic WETH to USDC swap amounts."""
        # Realistic swap: 0.5 WETH -> ~1000 USDC at $2000/ETH
        amount0 = 500000000000000000  # 0.5 WETH (18 decimals)
        amount1 = -1000000000  # -1000 USDC (6 decimals)

        receipt = {
            "transactionHash": TEST_TX_HASH,
            "blockNumber": 12345678,
            "status": 1,
            "logs": [create_swap_log(amount0, amount1)],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.swap_result is not None
        assert result.swap_result.amount_in_decimal == Decimal("0.5")
        assert result.swap_result.amount_out_decimal == Decimal("1000")
        assert result.swap_result.effective_price == Decimal("2000")

    def test_realistic_usdc_to_weth_swap(self, parser: UniswapV3ReceiptParser) -> None:
        """Test with realistic USDC to WETH swap amounts."""
        # Realistic swap: 1000 USDC -> ~0.5 WETH at $2000/ETH
        amount0 = -500000000000000000  # -0.5 WETH (18 decimals)
        amount1 = 1000000000  # 1000 USDC (6 decimals)

        receipt = {
            "transactionHash": TEST_TX_HASH,
            "blockNumber": 12345678,
            "status": 1,
            "logs": [create_swap_log(amount0, amount1)],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.swap_result is not None
        assert result.swap_result.token_in_symbol == "USDC"
        assert result.swap_result.token_out_symbol == "WETH"
        assert result.swap_result.amount_in_decimal == Decimal("1000")
        assert result.swap_result.amount_out_decimal == Decimal("0.5")

    def test_negative_slippage_favorable(self, parser: UniswapV3ReceiptParser) -> None:
        """Test favorable slippage (got more than quoted)."""
        amount0 = 1000000000000000000  # 1 WETH
        actual_out = -2100000000  # Got 2100 USDC
        quoted_out = 2000000000  # Expected 2000 USDC

        receipt = {
            "transactionHash": TEST_TX_HASH,
            "blockNumber": 12345678,
            "status": 1,
            "logs": [create_swap_log(amount0, actual_out)],
        }

        result = parser.parse_receipt(receipt, quoted_amount_out=quoted_out)

        assert result.success is True
        assert result.swap_result is not None
        # Negative slippage = (2000 - 2100) / 2000 * 10000 = -500 bps
        assert result.swap_result.slippage_bps == -500


# =============================================================================
# Event Type Mapping Tests
# =============================================================================


class TestEventTypeMappings:
    """Tests for event type mappings."""

    def test_event_topics_exist(self) -> None:
        """Test that all expected event topics exist."""
        expected_events = [
            "Swap",
            "Mint",
            "Burn",
            "Collect",
            "Flash",
            "Transfer",
            "Approval",
        ]

        for event in expected_events:
            assert event in EVENT_TOPICS, f"Missing event topic: {event}"

    def test_topic_to_event_reverse_mapping(self) -> None:
        """Test reverse mapping from topic to event name."""
        for event_name, topic in EVENT_TOPICS.items():
            assert TOPIC_TO_EVENT.get(topic) == event_name

    def test_event_name_to_type_mapping(self) -> None:
        """Test mapping from event name to event type."""
        assert EVENT_NAME_TO_TYPE["Swap"] == UniswapV3EventType.SWAP
        assert EVENT_NAME_TO_TYPE["Mint"] == UniswapV3EventType.MINT
        assert EVENT_NAME_TO_TYPE["Burn"] == UniswapV3EventType.BURN
        assert EVENT_NAME_TO_TYPE["Transfer"] == UniswapV3EventType.TRANSFER

    def test_swap_event_topic_constant(self) -> None:
        """Test the SWAP_EVENT_TOPIC constant matches Swap topic."""
        assert SWAP_EVENT_TOPIC == EVENT_TOPICS["Swap"]
        assert SWAP_EVENT_TOPIC == "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


# =============================================================================
# Event Type Enum Tests
# =============================================================================


class TestUniswapV3EventType:
    """Tests for UniswapV3EventType enum."""

    def test_all_event_types_exist(self) -> None:
        """Test that all expected event types exist."""
        expected_types = [
            "SWAP",
            "MINT",
            "BURN",
            "COLLECT",
            "FLASH",
            "TRANSFER",
            "APPROVAL",
            "UNKNOWN",
        ]

        for type_name in expected_types:
            assert hasattr(UniswapV3EventType, type_name), f"Missing event type: {type_name}"

    def test_event_type_values(self) -> None:
        """Test event type enum values."""
        assert UniswapV3EventType.SWAP.value == "SWAP"
        assert UniswapV3EventType.TRANSFER.value == "TRANSFER"
        assert UniswapV3EventType.UNKNOWN.value == "UNKNOWN"


# =============================================================================
# Token Constants Tests
# =============================================================================


class TestTokenConstants:
    """Tests for token constants."""

    def test_arbitrum_tokens_exist(self) -> None:
        """Test that expected Arbitrum tokens exist."""
        arbitrum_tokens = TOKEN_ADDRESSES.get("arbitrum", {})

        assert WETH_ADDRESS.lower() in arbitrum_tokens
        assert USDC_ADDRESS.lower() in arbitrum_tokens
        assert arbitrum_tokens[WETH_ADDRESS.lower()] == "WETH"
        assert arbitrum_tokens[USDC_ADDRESS.lower()] == "USDC"

    def test_token_decimals_exist(self) -> None:
        """Test that token decimals are defined."""
        assert TOKEN_DECIMALS["WETH"] == 18
        assert TOKEN_DECIMALS["USDC"] == 6
        assert TOKEN_DECIMALS["WBTC"] == 8
        assert TOKEN_DECIMALS["DAI"] == 18
