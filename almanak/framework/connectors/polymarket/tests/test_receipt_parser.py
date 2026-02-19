"""Tests for Polymarket Receipt Parser.

Tests cover:
- CLOB order response parsing
- CLOB fill notification parsing
- CTF TransferSingle event parsing
- CTF TransferBatch event parsing
- CTF PayoutRedemption event parsing
- ERC-20 Transfer event parsing
- Full receipt parsing with multiple events
- Error handling
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.connectors.polymarket.models import (
    CONDITIONAL_TOKENS,
    CTF_EXCHANGE,
    NEG_RISK_ADAPTER,
    NEG_RISK_EXCHANGE,
    USDC_POLYGON,
)
from almanak.framework.connectors.polymarket.receipt_parser import (
    ERC20_TRANSFER_TOPIC,
    PAYOUT_REDEMPTION_TOPIC,
    POLYMARKET_CONTRACTS,
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
    PolymarketEventType,
    PolymarketReceiptParser,
    RedemptionResult,
    TradeResult,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def parser() -> PolymarketReceiptParser:
    """Create a receipt parser instance."""
    return PolymarketReceiptParser()


@pytest.fixture
def sample_order_response() -> dict:
    """Sample CLOB order submission response."""
    return {
        "orderID": "0x1234567890abcdef1234567890abcdef",
        "status": "LIVE",
        "owner": "0x742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
        "market": "19045189272319329424023217822141741659150265216200539353252147725932663608488",
        "side": "BUY",
        "price": "0.65",
        "size": "100",
        "filledSize": "0",
        "createdAt": "2025-01-15T10:30:00Z",
    }


@pytest.fixture
def sample_filled_order_response() -> dict:
    """Sample CLOB filled order response."""
    return {
        "orderID": "0xfedcba0987654321fedcba0987654321",
        "status": "MATCHED",
        "owner": "0x742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
        "market": "19045189272319329424023217822141741659150265216200539353252147725932663608488",
        "side": "BUY",
        "price": "0.65",
        "size": "100",
        "filledSize": "100",
        "avgPrice": "0.64",
        "fee": "0.05",
        "createdAt": "2025-01-15T10:30:00Z",
    }


@pytest.fixture
def sample_fill_notification() -> dict:
    """Sample CLOB fill notification."""
    return {
        "type": "fill",
        "orderId": "0x1234567890abcdef1234567890abcdef",
        "matchId": "0xabcdef1234567890abcdef1234567890",
        "fillSize": "50",
        "fillPrice": "0.65",
        "fee": "0.025",
        "timestamp": "2025-01-15T10:35:00Z",
    }


@pytest.fixture
def sample_transfer_single_log() -> dict:
    """Sample TransferSingle ERC-1155 log."""
    # TransferSingle(operator, from, to, id, value)
    # operator, from, to are indexed in topics
    # id, value are in data
    return {
        "address": CONDITIONAL_TOKENS,
        "topics": [
            TRANSFER_SINGLE_TOPIC,
            # operator (padded to 32 bytes)
            "0x0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
            # from
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            # to
            "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
        ],
        # id (token_id as uint256), value (amount as uint256)
        "data": (
            "0x"
            "0000000000000000000000000000000029a6f6f42f1b92b23c4e9a9b1f0d4c5e"  # token_id
            "0000000000000000000000000000000000000000000000000000000005f5e100"  # value (100000000)
        ),
        "logIndex": 0,
        "transactionHash": "0xabc123def456789",
        "blockNumber": 12345678,
    }


@pytest.fixture
def sample_transfer_batch_log() -> dict:
    """Sample TransferBatch ERC-1155 log."""
    return {
        "address": CONDITIONAL_TOKENS,
        "topics": [
            TRANSFER_BATCH_TOPIC,
            "0x0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",  # operator
            "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",  # from
            "0x000000000000000000000000C5d563A36AE78145C45a50134d48A1215220f80a",  # to
        ],
        # Dynamic array encoding: offset to ids, offset to values, then arrays
        "data": (
            "0x"
            "0000000000000000000000000000000000000000000000000000000000000040"  # offset to ids (64 bytes)
            "00000000000000000000000000000000000000000000000000000000000000a0"  # offset to values (160 bytes)
            "0000000000000000000000000000000000000000000000000000000000000002"  # ids length = 2
            "0000000000000000000000000000000000000000000000000000000000000001"  # id[0] = 1
            "0000000000000000000000000000000000000000000000000000000000000002"  # id[1] = 2
            "0000000000000000000000000000000000000000000000000000000000000002"  # values length = 2
            "0000000000000000000000000000000000000000000000000000000005f5e100"  # value[0]
            "0000000000000000000000000000000000000000000000000000000005f5e100"  # value[1]
        ),
        "logIndex": 1,
        "transactionHash": "0xdef456abc789012",
        "blockNumber": 12345680,
    }


@pytest.fixture
def sample_payout_redemption_log() -> dict:
    """Sample PayoutRedemption CTF log."""
    return {
        "address": CONDITIONAL_TOKENS,
        "topics": [
            PAYOUT_REDEMPTION_TOPIC,
            "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",  # redeemer
            "0x0000000000000000000000002791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # collateralToken (USDC)
            "0x9915bea232fa12b20058f9cea1187ea51366352bf833393676cd0db557a58249",  # conditionId
        ],
        # parentCollectionId, offset to indexSets, payout, indexSets array
        "data": (
            "0x"
            "0000000000000000000000000000000000000000000000000000000000000000"  # parentCollectionId (zero)
            "0000000000000000000000000000000000000000000000000000000000000060"  # offset to indexSets (96 bytes)
            "000000000000000000000000000000000000000000000000000000000098968a"  # payout (10001034 = ~10 USDC)
            "0000000000000000000000000000000000000000000000000000000000000002"  # indexSets length = 2
            "0000000000000000000000000000000000000000000000000000000000000001"  # indexSet[0] = 1 (YES)
            "0000000000000000000000000000000000000000000000000000000000000002"  # indexSet[1] = 2 (NO)
        ),
        "logIndex": 2,
        "transactionHash": "0xredeem12345678",
        "blockNumber": 12345690,
    }


@pytest.fixture
def sample_erc20_transfer_log() -> dict:
    """Sample ERC-20 Transfer log (USDC)."""
    return {
        "address": USDC_POLYGON,
        "topics": [
            ERC20_TRANSFER_TOPIC,
            "0x0000000000000000000000004D97DCd97eC945f40cF65F87097ACe5EA0476045",  # from (CTF contract)
            "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",  # to (user)
        ],
        # value (uint256)
        "data": "0x000000000000000000000000000000000000000000000000000000000098968a",  # 10001034
        "logIndex": 3,
        "transactionHash": "0xredeem12345678",
        "blockNumber": 12345690,
    }


@pytest.fixture
def sample_redemption_receipt(
    sample_payout_redemption_log: dict,
    sample_erc20_transfer_log: dict,
) -> dict:
    """Sample complete redemption transaction receipt."""
    return {
        "transactionHash": "0xredeem12345678901234567890123456789012345678901234567890123456",
        "blockNumber": 12345690,
        "status": 1,
        "gasUsed": 185000,
        "logs": [sample_payout_redemption_log, sample_erc20_transfer_log],
    }


# =============================================================================
# CLOB Response Parsing Tests
# =============================================================================


class TestParseOrderResponse:
    """Tests for parse_order_response."""

    def test_parse_live_order(self, parser: PolymarketReceiptParser, sample_order_response: dict) -> None:
        """Test parsing a live (unfilled) order response."""
        result = parser.parse_order_response(sample_order_response)

        assert result.success is True
        assert result.order_id == "0x1234567890abcdef1234567890abcdef"
        assert result.status == "LIVE"
        assert result.filled_size == Decimal("0")
        assert result.avg_price == Decimal("0.65")  # Falls back to order price
        assert result.side == "BUY"
        assert result.token_id == "19045189272319329424023217822141741659150265216200539353252147725932663608488"
        assert result.timestamp is not None
        assert result.is_filled is False
        assert result.is_complete is False

    def test_parse_filled_order(self, parser: PolymarketReceiptParser, sample_filled_order_response: dict) -> None:
        """Test parsing a fully filled order response."""
        result = parser.parse_order_response(sample_filled_order_response)

        assert result.success is True
        assert result.order_id == "0xfedcba0987654321fedcba0987654321"
        assert result.status == "MATCHED"
        assert result.filled_size == Decimal("100")
        assert result.avg_price == Decimal("0.64")
        assert result.fee == Decimal("0.05")
        assert result.is_filled is True
        assert result.is_complete is True

    def test_parse_order_alternative_field_names(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing with alternative field names."""
        response = {
            "orderId": "0x123",  # lowercase 'd'
            "status": "CANCELLED",
            "tokenId": "123456789",
            "price": "0.55",
        }
        result = parser.parse_order_response(response)

        assert result.success is True
        assert result.order_id == "0x123"
        assert result.status == "CANCELLED"
        assert result.token_id == "123456789"
        assert result.is_complete is True

    def test_parse_order_minimal_response(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing with minimal fields."""
        response = {
            "id": "0xminimal",
            "status": "LIVE",
        }
        result = parser.parse_order_response(response)

        assert result.success is True
        assert result.order_id == "0xminimal"
        assert result.filled_size == Decimal("0")
        assert result.avg_price == Decimal("0")

    def test_parse_order_expired(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing an expired order."""
        response = {
            "orderID": "0xexpired",
            "status": "EXPIRED",
            "filledSize": "50",
        }
        result = parser.parse_order_response(response)

        assert result.success is True
        assert result.status == "EXPIRED"
        assert result.is_complete is True

    def test_parse_order_invalid_response(self, parser: PolymarketReceiptParser) -> None:
        """Test error handling for invalid response."""
        # This shouldn't crash, just return empty values
        result = parser.parse_order_response({})

        assert result.success is True
        assert result.order_id is None
        assert result.status == "UNKNOWN"


class TestParseFillNotification:
    """Tests for parse_fill_notification."""

    def test_parse_fill(self, parser: PolymarketReceiptParser, sample_fill_notification: dict) -> None:
        """Test parsing a fill notification."""
        result = parser.parse_fill_notification(sample_fill_notification)

        assert result.success is True
        assert result.order_id == "0x1234567890abcdef1234567890abcdef"
        assert result.status == "MATCHED"
        assert result.filled_size == Decimal("50")
        assert result.avg_price == Decimal("0.65")
        assert result.fee == Decimal("0.025")
        assert result.tx_hash == "0xabcdef1234567890abcdef1234567890"
        assert result.timestamp is not None

    def test_parse_fill_with_tx_hash(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing fill with txHash field."""
        notification = {
            "orderId": "0x123",
            "fillSize": "100",
            "fillPrice": "0.70",
            "txHash": "0xsettlement123",
        }
        result = parser.parse_fill_notification(notification)

        assert result.success is True
        assert result.tx_hash == "0xsettlement123"


class TestParseOrderStatus:
    """Tests for parse_order_status."""

    def test_parse_status_delegates_to_order_response(
        self, parser: PolymarketReceiptParser, sample_order_response: dict
    ) -> None:
        """Test that parse_order_status delegates to parse_order_response."""
        result = parser.parse_order_status(sample_order_response)

        assert result.success is True
        assert result.order_id == sample_order_response["orderID"]


# =============================================================================
# CTF Receipt Parsing Tests
# =============================================================================


class TestParseCtfReceipt:
    """Tests for parse_ctf_receipt."""

    def test_parse_empty_receipt(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing receipt with no logs."""
        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 100,
            "status": 1,
            "logs": [],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is True
        assert len(result.events) == 0

    def test_parse_failed_transaction(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing a reverted transaction."""
        receipt = {
            "transactionHash": "0xfailed123",
            "blockNumber": 100,
            "status": 0,  # Failed
            "logs": [],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        assert result.error == "Transaction reverted"

    def test_parse_transfer_single_event(
        self, parser: PolymarketReceiptParser, sample_transfer_single_log: dict
    ) -> None:
        """Test parsing TransferSingle event."""
        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 12345678,
            "status": 1,
            "logs": [sample_transfer_single_log],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert len(result.transfer_singles) == 1

        transfer = result.transfer_singles[0]
        assert transfer.operator.lower() == "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
        assert transfer.from_addr == "0x0000000000000000000000000000000000000000"
        assert transfer.to_addr.lower() == "0x742d35cc6634c0532925a3b844bc9e7595f5abcd"
        assert transfer.value > 0

    def test_parse_transfer_batch_event(self, parser: PolymarketReceiptParser, sample_transfer_batch_log: dict) -> None:
        """Test parsing TransferBatch event."""
        receipt = {
            "transactionHash": "0x456",
            "blockNumber": 12345680,
            "status": 1,
            "logs": [sample_transfer_batch_log],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert len(result.transfer_batches) == 1

        batch = result.transfer_batches[0]
        assert len(batch.token_ids) == 2
        assert batch.token_ids[0] == 1
        assert batch.token_ids[1] == 2
        assert len(batch.values) == 2

    def test_parse_payout_redemption_event(
        self, parser: PolymarketReceiptParser, sample_payout_redemption_log: dict
    ) -> None:
        """Test parsing PayoutRedemption event."""
        receipt = {
            "transactionHash": "0xredeem123",
            "blockNumber": 12345690,
            "status": 1,
            "logs": [sample_payout_redemption_log],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert len(result.redemptions) == 1

        redemption = result.redemptions[0]
        assert redemption.redeemer.lower() == "0x742d35cc6634c0532925a3b844bc9e7595f5abcd"
        assert redemption.collateral_token.lower() == USDC_POLYGON.lower()
        assert "9915bea232fa12b20058f9cea1187ea51366352bf833393676cd0db557a58249" in redemption.condition_id.lower()
        assert 1 in redemption.index_sets
        assert 2 in redemption.index_sets
        assert redemption.payout > 0
        assert redemption.payout_decimal > 0

    def test_parse_erc20_transfer_event(self, parser: PolymarketReceiptParser, sample_erc20_transfer_log: dict) -> None:
        """Test parsing ERC-20 Transfer event."""
        receipt = {
            "transactionHash": "0xtransfer123",
            "blockNumber": 12345690,
            "status": 1,
            "logs": [sample_erc20_transfer_log],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert len(result.erc20_transfers) == 1

        transfer = result.erc20_transfers[0]
        assert transfer.to_addr.lower() == "0x742d35cc6634c0532925a3b844bc9e7595f5abcd"
        assert transfer.token_address.lower() == USDC_POLYGON.lower()
        assert transfer.value == 10000010  # 0x98968a = 10000010
        assert transfer.value_decimal == Decimal("10.00001")

    def test_parse_full_redemption_receipt(
        self, parser: PolymarketReceiptParser, sample_redemption_receipt: dict
    ) -> None:
        """Test parsing a complete redemption transaction."""
        result = parser.parse_ctf_receipt(sample_redemption_receipt)

        assert result.success is True
        assert result.transaction_success is True
        assert len(result.events) == 2
        assert len(result.redemptions) == 1
        assert len(result.erc20_transfers) == 1

        # Check redemption result
        assert result.redemption_result is not None
        assert result.redemption_result.success is True
        assert result.redemption_result.amount_redeemed > 0
        assert result.redemption_result.condition_id is not None
        assert len(result.redemption_result.index_sets) == 2
        assert result.redemption_result.gas_used == 185000

    def test_parse_receipt_with_bytes_hash(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing receipt with bytes transaction hash."""
        receipt = {
            "transactionHash": bytes.fromhex("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"),
            "blockNumber": 100,
            "status": 1,
            "logs": [],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash.startswith("0x")
        assert "abcdef" in result.transaction_hash.lower()

    def test_parse_receipt_ignores_unknown_events(self, parser: PolymarketReceiptParser) -> None:
        """Test that unknown events are ignored."""
        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 100,
            "status": 1,
            "logs": [
                {
                    "address": "0x1234567890123456789012345678901234567890",
                    "topics": ["0xunknowneventtopic12345678901234567890123456789012345678901234"],
                    "data": "0x",
                    "logIndex": 0,
                }
            ],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0


# =============================================================================
# Data Class Tests
# =============================================================================


class TestTradeResult:
    """Tests for TradeResult dataclass."""

    def test_is_filled_true(self) -> None:
        """Test is_filled property when order has fills."""
        result = TradeResult(success=True, filled_size=Decimal("50"))
        assert result.is_filled is True

    def test_is_filled_false(self) -> None:
        """Test is_filled property when order has no fills."""
        result = TradeResult(success=True, filled_size=Decimal("0"))
        assert result.is_filled is False

    def test_is_complete_matched(self) -> None:
        """Test is_complete for matched orders."""
        result = TradeResult(success=True, status="MATCHED")
        assert result.is_complete is True

    def test_is_complete_cancelled(self) -> None:
        """Test is_complete for cancelled orders."""
        result = TradeResult(success=True, status="CANCELLED")
        assert result.is_complete is True

    def test_is_complete_live(self) -> None:
        """Test is_complete for live orders."""
        result = TradeResult(success=True, status="LIVE")
        assert result.is_complete is False

    def test_to_dict(self) -> None:
        """Test TradeResult serialization."""
        result = TradeResult(
            success=True,
            order_id="0x123",
            status="MATCHED",
            filled_size=Decimal("100"),
            avg_price=Decimal("0.65"),
            fee=Decimal("0.05"),
            tx_hash="0xabc",
            side="BUY",
            token_id="12345",
            timestamp=datetime(2025, 1, 15, 10, 30, tzinfo=UTC),
        )
        d = result.to_dict()

        assert d["success"] is True
        assert d["order_id"] == "0x123"
        assert d["status"] == "MATCHED"
        assert d["filled_size"] == "100"
        assert d["avg_price"] == "0.65"
        assert d["fee"] == "0.05"
        assert d["tx_hash"] == "0xabc"
        assert d["side"] == "BUY"
        assert d["token_id"] == "12345"
        assert d["timestamp"] == "2025-01-15T10:30:00+00:00"


class TestRedemptionResult:
    """Tests for RedemptionResult dataclass."""

    def test_to_dict(self) -> None:
        """Test RedemptionResult serialization."""
        result = RedemptionResult(
            success=True,
            tx_hash="0xredeem123",
            amount_redeemed=Decimal("100.5"),
            condition_id="0xcondition",
            index_sets=[1, 2],
            payout_amounts=[Decimal("100.5")],
            redeemer="0xuser",
            gas_used=200000,
        )
        d = result.to_dict()

        assert d["success"] is True
        assert d["tx_hash"] == "0xredeem123"
        assert d["amount_redeemed"] == "100.5"
        assert d["condition_id"] == "0xcondition"
        assert d["index_sets"] == [1, 2]
        assert d["payout_amounts"] == ["100.5"]
        assert d["redeemer"] == "0xuser"
        assert d["gas_used"] == 200000


# =============================================================================
# Utility Method Tests
# =============================================================================


class TestUtilityMethods:
    """Tests for utility methods."""

    def test_is_polymarket_event_known(self, parser: PolymarketReceiptParser) -> None:
        """Test is_polymarket_event for known events."""
        assert parser.is_polymarket_event(TRANSFER_SINGLE_TOPIC) is True
        assert parser.is_polymarket_event(PAYOUT_REDEMPTION_TOPIC) is True
        assert parser.is_polymarket_event(ERC20_TRANSFER_TOPIC) is True

    def test_is_polymarket_event_unknown(self, parser: PolymarketReceiptParser) -> None:
        """Test is_polymarket_event for unknown events."""
        assert parser.is_polymarket_event("0xunknown") is False

    def test_is_polymarket_contract_known(self, parser: PolymarketReceiptParser) -> None:
        """Test is_polymarket_contract for known contracts."""
        assert parser.is_polymarket_contract(CONDITIONAL_TOKENS) is True
        assert parser.is_polymarket_contract(CTF_EXCHANGE) is True
        assert parser.is_polymarket_contract(USDC_POLYGON) is True
        # Test case insensitivity
        assert parser.is_polymarket_contract(CONDITIONAL_TOKENS.lower()) is True

    def test_is_polymarket_contract_unknown(self, parser: PolymarketReceiptParser) -> None:
        """Test is_polymarket_contract for unknown contracts."""
        assert parser.is_polymarket_contract("0x1234567890123456789012345678901234567890") is False

    def test_get_event_type_known(self, parser: PolymarketReceiptParser) -> None:
        """Test get_event_type for known topics."""
        assert parser.get_event_type(TRANSFER_SINGLE_TOPIC) == PolymarketEventType.TRANSFER_SINGLE
        assert parser.get_event_type(TRANSFER_BATCH_TOPIC) == PolymarketEventType.TRANSFER_BATCH
        assert parser.get_event_type(PAYOUT_REDEMPTION_TOPIC) == PolymarketEventType.PAYOUT_REDEMPTION
        assert parser.get_event_type(ERC20_TRANSFER_TOPIC) == PolymarketEventType.ERC20_TRANSFER

    def test_get_event_type_unknown(self, parser: PolymarketReceiptParser) -> None:
        """Test get_event_type for unknown topics."""
        assert parser.get_event_type("0xunknown") == PolymarketEventType.UNKNOWN


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_parse_order_response_with_none_values(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing response with None values."""
        response = {
            "orderID": None,
            "status": None,
            "filledSize": None,
            "price": None,
        }
        result = parser.parse_order_response(response)

        assert result.success is True
        assert result.order_id is None
        assert result.filled_size == Decimal("0")

    def test_parse_receipt_with_bytes_data(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing log with bytes data."""
        log = {
            "address": bytes.fromhex("4D97DCd97eC945f40cF65F87097ACe5EA0476045"),
            "topics": [
                bytes.fromhex("c3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"),
                bytes.fromhex("0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
                bytes.fromhex("0000000000000000000000000000000000000000000000000000000000000000"),
                bytes.fromhex("000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD"),
            ],
            "data": bytes.fromhex(
                "0000000000000000000000000000000029a6f6f42f1b92b23c4e9a9b1f0d4c5e0000000000000000000000000000000000000000000000000000000005f5e100"
            ),
            "logIndex": 0,
        }
        receipt = {
            "transactionHash": bytes.fromhex("1234567890abcdef" * 4),
            "blockNumber": 100,
            "status": 1,
            "logs": [log],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert len(result.transfer_singles) == 1

    def test_parse_receipt_malformed_log(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing receipt with malformed log data."""
        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 100,
            "status": 1,
            "logs": [
                {
                    "address": CONDITIONAL_TOKENS,
                    "topics": [TRANSFER_SINGLE_TOPIC],  # Missing indexed params
                    "data": "0x",  # Truncated data
                    "logIndex": 0,
                }
            ],
        }
        result = parser.parse_ctf_receipt(receipt)

        # Should not crash, but may have empty parsed data
        assert result.success is True

    def test_parse_fill_with_zero_values(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing fill with zero amounts."""
        notification = {
            "orderId": "0x123",
            "fillSize": "0",
            "fillPrice": "0",
            "fee": "0",
        }
        result = parser.parse_fill_notification(notification)

        assert result.success is True
        assert result.filled_size == Decimal("0")
        assert result.is_filled is False

    def test_parse_large_token_id(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing with large token IDs (uint256)."""
        # Polymarket token IDs can be very large
        large_token_id = "19045189272319329424023217822141741659150265216200539353252147725932663608488"
        response = {
            "orderID": "0x123",
            "status": "LIVE",
            "market": large_token_id,
        }
        result = parser.parse_order_response(response)

        assert result.success is True
        assert result.token_id == large_token_id


# =============================================================================
# Contract Address Filtering Tests
# =============================================================================


class TestContractAddressFiltering:
    """Tests for contract address filtering in receipt parsing."""

    def test_polymarket_contracts_contains_all_known_addresses(self) -> None:
        """Test that POLYMARKET_CONTRACTS contains all expected addresses."""
        # All contracts should be lowercase
        assert CONDITIONAL_TOKENS.lower() in POLYMARKET_CONTRACTS
        assert CTF_EXCHANGE.lower() in POLYMARKET_CONTRACTS
        assert NEG_RISK_EXCHANGE.lower() in POLYMARKET_CONTRACTS
        assert NEG_RISK_ADAPTER.lower() in POLYMARKET_CONTRACTS
        assert USDC_POLYGON.lower() in POLYMARKET_CONTRACTS
        # Should be exactly 5 addresses
        assert len(POLYMARKET_CONTRACTS) == 5

    def test_filter_by_contract_excludes_non_polymarket_logs(self, parser: PolymarketReceiptParser) -> None:
        """Test that logs from non-Polymarket contracts are filtered out."""
        # Create a log with a Polymarket event signature but from a random contract
        unrelated_contract = "0x1111111111111111111111111111111111111111"
        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 100,
            "status": 1,
            "logs": [
                {
                    "address": unrelated_contract,
                    "topics": [
                        TRANSFER_SINGLE_TOPIC,
                        "0x0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
                    ],
                    "data": (
                        "0x"
                        "0000000000000000000000000000000029a6f6f42f1b92b23c4e9a9b1f0d4c5e"
                        "0000000000000000000000000000000000000000000000000000000005f5e100"
                    ),
                    "logIndex": 0,
                }
            ],
        }
        # With filter_by_contract=True (default), the log should be filtered
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0
        assert len(result.transfer_singles) == 0

    def test_filter_by_contract_false_includes_all_logs(self, parser: PolymarketReceiptParser) -> None:
        """Test that filter_by_contract=False includes all matching logs."""
        unrelated_contract = "0x1111111111111111111111111111111111111111"
        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 100,
            "status": 1,
            "logs": [
                {
                    "address": unrelated_contract,
                    "topics": [
                        TRANSFER_SINGLE_TOPIC,
                        "0x0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
                    ],
                    "data": (
                        "0x"
                        "0000000000000000000000000000000029a6f6f42f1b92b23c4e9a9b1f0d4c5e"
                        "0000000000000000000000000000000000000000000000000000000005f5e100"
                    ),
                    "logIndex": 0,
                }
            ],
        }
        # With filter_by_contract=False, the log should be parsed
        result = parser.parse_ctf_receipt(receipt, filter_by_contract=False)

        assert result.success is True
        assert len(result.events) == 1
        assert len(result.transfer_singles) == 1

    def test_multi_contract_receipt_filters_correctly(self, parser: PolymarketReceiptParser) -> None:
        """Test parsing receipt with logs from multiple contracts."""
        unrelated_contract = "0x1111111111111111111111111111111111111111"
        receipt = {
            "transactionHash": "0x123456",
            "blockNumber": 100,
            "status": 1,
            "logs": [
                # Log from Polymarket CONDITIONAL_TOKENS - should be included
                {
                    "address": CONDITIONAL_TOKENS,
                    "topics": [
                        TRANSFER_SINGLE_TOPIC,
                        "0x0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
                    ],
                    "data": (
                        "0x"
                        "0000000000000000000000000000000029a6f6f42f1b92b23c4e9a9b1f0d4c5e"
                        "0000000000000000000000000000000000000000000000000000000005f5e100"
                    ),
                    "logIndex": 0,
                },
                # Log from unrelated contract - should be filtered out
                {
                    "address": unrelated_contract,
                    "topics": [
                        TRANSFER_SINGLE_TOPIC,
                        "0x0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
                    ],
                    "data": (
                        "0x"
                        "0000000000000000000000000000000029a6f6f42f1b92b23c4e9a9b1f0d4c5e"
                        "0000000000000000000000000000000000000000000000000000000005f5e100"
                    ),
                    "logIndex": 1,
                },
                # Log from USDC_POLYGON - should be included
                {
                    "address": USDC_POLYGON,
                    "topics": [
                        ERC20_TRANSFER_TOPIC,
                        "0x0000000000000000000000004D97DCd97eC945f40cF65F87097ACe5EA0476045",
                        "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
                    ],
                    "data": "0x000000000000000000000000000000000000000000000000000000000098968a",
                    "logIndex": 2,
                },
            ],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        # Only 2 logs should be parsed (CONDITIONAL_TOKENS and USDC_POLYGON)
        assert len(result.events) == 2
        assert len(result.transfer_singles) == 1
        assert len(result.erc20_transfers) == 1

    def test_filter_includes_all_polymarket_contracts(self, parser: PolymarketReceiptParser) -> None:
        """Test that all known Polymarket contract addresses pass the filter."""
        contracts_to_test = [
            CONDITIONAL_TOKENS,
            CTF_EXCHANGE,
            NEG_RISK_EXCHANGE,
            NEG_RISK_ADAPTER,
            USDC_POLYGON,
        ]

        for contract_address in contracts_to_test:
            receipt = {
                "transactionHash": "0x123",
                "blockNumber": 100,
                "status": 1,
                "logs": [
                    {
                        "address": contract_address,
                        "topics": [
                            TRANSFER_SINGLE_TOPIC,
                            "0x0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
                            "0x0000000000000000000000000000000000000000000000000000000000000000",
                            "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
                        ],
                        "data": (
                            "0x"
                            "0000000000000000000000000000000029a6f6f42f1b92b23c4e9a9b1f0d4c5e"
                            "0000000000000000000000000000000000000000000000000000000005f5e100"
                        ),
                        "logIndex": 0,
                    }
                ],
            }
            result = parser.parse_ctf_receipt(receipt)

            assert result.success is True, f"Failed for contract {contract_address}"
            assert len(result.events) == 1, f"No events for contract {contract_address}"

    def test_filter_case_insensitive(self, parser: PolymarketReceiptParser) -> None:
        """Test that contract address filtering is case-insensitive."""
        # Use lowercase contract address
        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 100,
            "status": 1,
            "logs": [
                {
                    "address": CONDITIONAL_TOKENS.lower(),  # Lowercase
                    "topics": [
                        TRANSFER_SINGLE_TOPIC,
                        "0x0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
                    ],
                    "data": (
                        "0x"
                        "0000000000000000000000000000000029a6f6f42f1b92b23c4e9a9b1f0d4c5e"
                        "0000000000000000000000000000000000000000000000000000000005f5e100"
                    ),
                    "logIndex": 0,
                }
            ],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1

    def test_filter_with_bytes_address(self, parser: PolymarketReceiptParser) -> None:
        """Test filtering when contract address is bytes."""
        # Convert CONDITIONAL_TOKENS to bytes (remove 0x prefix)
        address_bytes = bytes.fromhex(CONDITIONAL_TOKENS[2:])
        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 100,
            "status": 1,
            "logs": [
                {
                    "address": address_bytes,
                    "topics": [
                        TRANSFER_SINGLE_TOPIC,
                        "0x0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
                    ],
                    "data": (
                        "0x"
                        "0000000000000000000000000000000029a6f6f42f1b92b23c4e9a9b1f0d4c5e"
                        "0000000000000000000000000000000000000000000000000000000005f5e100"
                    ),
                    "logIndex": 0,
                }
            ],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1

    def test_filter_excludes_similar_events_from_other_protocols(self, parser: PolymarketReceiptParser) -> None:
        """Test that ERC-1155 events from other protocols are filtered out.

        This is a key use case - other DeFi protocols may emit the same
        ERC-1155 events, and we should ignore them.
        """
        # Simulate a receipt with events from multiple ERC-1155 contracts
        other_erc1155_contract = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        receipt = {
            "transactionHash": "0xmixed123",
            "blockNumber": 100,
            "status": 1,
            "logs": [
                # ERC-1155 TransferSingle from another NFT contract
                {
                    "address": other_erc1155_contract,
                    "topics": [
                        TRANSFER_SINGLE_TOPIC,
                        "0x000000000000000000000000SomeOtherOperator12345678901234567890",
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
                    ],
                    "data": (
                        "0x"
                        "0000000000000000000000000000000000000000000000000000000000000001"
                        "0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    "logIndex": 0,
                },
                # ERC-1155 TransferSingle from Polymarket CONDITIONAL_TOKENS
                {
                    "address": CONDITIONAL_TOKENS,
                    "topics": [
                        TRANSFER_SINGLE_TOPIC,
                        "0x0000000000000000000000004bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f5ABCD",
                    ],
                    "data": (
                        "0x"
                        "0000000000000000000000000000000029a6f6f42f1b92b23c4e9a9b1f0d4c5e"
                        "0000000000000000000000000000000000000000000000000000000005f5e100"
                    ),
                    "logIndex": 1,
                },
            ],
        }
        result = parser.parse_ctf_receipt(receipt)

        assert result.success is True
        # Only the Polymarket log should be parsed
        assert len(result.events) == 1
        assert len(result.transfer_singles) == 1
        # Verify it's the correct transfer (from Polymarket, not the other contract)
        transfer = result.transfer_singles[0]
        assert transfer.contract_address.lower() == CONDITIONAL_TOKENS.lower()

    def test_filter_preserves_redemption_result_building(
        self, parser: PolymarketReceiptParser, sample_redemption_receipt: dict
    ) -> None:
        """Test that filtering doesn't affect redemption result building."""
        result = parser.parse_ctf_receipt(sample_redemption_receipt)

        assert result.success is True
        assert result.redemption_result is not None
        assert result.redemption_result.success is True
        assert result.redemption_result.amount_redeemed > 0
