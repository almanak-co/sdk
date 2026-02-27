"""Tests for Compound V3 Receipt Parser (Refactored).

Tests for the refactored Compound V3 receipt parser using base infrastructure
(EventRegistry + HexDecoder).
"""

from decimal import Decimal

from almanak.framework.connectors.compound_v3.receipt_parser import (
    CompoundV3EventType,
    CompoundV3ReceiptParser,
)

# =============================================================================
# Test Data
# =============================================================================

COMET_ADDRESS = "0xc3d688b66703497daa19211eedff47f25384cdc3"
USER_ADDRESS = "0x742d35cc6634c0532925a3b844bc454e4438f44e"
COLLATERAL_ASSET = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # USDC


def create_supply_log(from_addr: str, dst: str, amount: int) -> dict:
    """Create a Supply event log."""
    return {
        "address": COMET_ADDRESS,
        "topics": [
            "0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e",  # Supply
            f"0x000000000000000000000000{from_addr[2:].lower()}",
            f"0x000000000000000000000000{dst[2:].lower()}",
        ],
        "data": f"0x{amount:064x}",
        "logIndex": 0,
    }


def create_withdraw_log(src: str, to: str, amount: int) -> dict:
    """Create a Withdraw event log."""
    return {
        "address": COMET_ADDRESS,
        "topics": [
            "0x9b1bfa7fa9ee420a16e124f794c35ac9f90472acc99140eb2f6447c714cad8eb",  # Withdraw
            f"0x000000000000000000000000{src[2:].lower()}",
            f"0x000000000000000000000000{to[2:].lower()}",
        ],
        "data": f"0x{amount:064x}",
        "logIndex": 1,
    }


def create_supply_collateral_log(from_addr: str, dst: str, asset: str, amount: int) -> dict:
    """Create a SupplyCollateral event log."""
    return {
        "address": COMET_ADDRESS,
        "topics": [
            "0xfa56f7b24f17183d81894d3ac2ee654e3c26388d17a28dbd9549b8114304e1f4",  # SupplyCollateral
            f"0x000000000000000000000000{from_addr[2:].lower()}",
            f"0x000000000000000000000000{dst[2:].lower()}",
            f"0x000000000000000000000000{asset[2:].lower()}",
        ],
        "data": f"0x{amount:064x}",
        "logIndex": 2,
    }


def create_withdraw_collateral_log(src: str, to: str, asset: str, amount: int) -> dict:
    """Create a WithdrawCollateral event log."""
    return {
        "address": COMET_ADDRESS,
        "topics": [
            "0xd6d480d5b3068db003533b170d67561494d72e3bf9fa40a266471351ebba9e16",  # WithdrawCollateral
            f"0x000000000000000000000000{src[2:].lower()}",
            f"0x000000000000000000000000{to[2:].lower()}",
            f"0x000000000000000000000000{asset[2:].lower()}",
        ],
        "data": f"0x{amount:064x}",
        "logIndex": 3,
    }


def create_transfer_log(from_addr: str, to: str, amount: int) -> dict:
    """Create a Transfer event log."""
    return {
        "address": COMET_ADDRESS,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # Transfer
            f"0x000000000000000000000000{from_addr[2:].lower()}",
            f"0x000000000000000000000000{to[2:].lower()}",
        ],
        "data": f"0x{amount:064x}",
        "logIndex": 4,
    }


def create_transfer_collateral_log(from_addr: str, to: str, asset: str, amount: int) -> dict:
    """Create a TransferCollateral event log."""
    return {
        "address": COMET_ADDRESS,
        "topics": [
            "0x29db89d45e1a802b4d55e202984fce9faf1d30aedf86503ff1ea0ed9ebb64201",  # TransferCollateral
            f"0x000000000000000000000000{from_addr[2:].lower()}",
            f"0x000000000000000000000000{to[2:].lower()}",
            f"0x000000000000000000000000{asset[2:].lower()}",
        ],
        "data": f"0x{amount:064x}",
        "logIndex": 5,
    }


def create_absorb_debt_log(absorber: str, borrower: str, base_paid_out: int, usd_value: int) -> dict:
    """Create an AbsorbDebt event log."""
    return {
        "address": COMET_ADDRESS,
        "topics": [
            "0x1547a878dc89ad3c367b6338b4be6a65a5dd74fb77ae044da1e8747ef1f4f62f",  # AbsorbDebt
            f"0x000000000000000000000000{absorber[2:].lower()}",
            f"0x000000000000000000000000{borrower[2:].lower()}",
        ],
        "data": f"0x{base_paid_out:064x}{usd_value:064x}",
        "logIndex": 6,
    }


def create_absorb_collateral_log(
    absorber: str, borrower: str, asset: str, collateral_absorbed: int, usd_value: int
) -> dict:
    """Create an AbsorbCollateral event log."""
    return {
        "address": COMET_ADDRESS,
        "topics": [
            "0x9850ab1af75177e4a9201c65a2cf7976d5d28e40ef63494b44366f86b2f9412e",  # AbsorbCollateral
            f"0x000000000000000000000000{absorber[2:].lower()}",
            f"0x000000000000000000000000{borrower[2:].lower()}",
            f"0x000000000000000000000000{asset[2:].lower()}",
        ],
        "data": f"0x{collateral_absorbed:064x}{usd_value:064x}",
        "logIndex": 7,
    }


def create_buy_collateral_log(buyer: str, asset: str, base_amount: int, collateral_amount: int) -> dict:
    """Create a BuyCollateral event log."""
    return {
        "address": COMET_ADDRESS,
        "topics": [
            "0xf891b2a411b0e66a5f0a6ff1368670fefa287a13f541eb633a386a1a9cc7046b",  # BuyCollateral
            f"0x000000000000000000000000{buyer[2:].lower()}",
            f"0x000000000000000000000000{asset[2:].lower()}",
        ],
        "data": f"0x{base_amount:064x}{collateral_amount:064x}",
        "logIndex": 8,
    }


# =============================================================================
# Tests
# =============================================================================


class TestCompoundV3ReceiptParser:
    """Tests for CompoundV3ReceiptParser."""

    def test_parse_supply_event(self):
        """Test parsing a Supply event."""
        parser = CompoundV3ReceiptParser()

        supply_amount = 1_000_000_000  # 1000 USDC (6 decimals)
        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 12345,
            "logs": [create_supply_log(USER_ADDRESS, USER_ADDRESS, supply_amount)],
            "gasUsed": 100000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CompoundV3EventType.SUPPLY
        assert result.events[0].data["from_address"] == USER_ADDRESS.lower()
        assert result.events[0].data["dst"] == USER_ADDRESS.lower()
        assert result.events[0].data["amount"] == Decimal(supply_amount)
        assert result.supply_amount == Decimal(supply_amount)
        assert result.withdraw_amount == Decimal("0")

    def test_parse_withdraw_event(self):
        """Test parsing a Withdraw event."""
        parser = CompoundV3ReceiptParser()

        withdraw_amount = 500_000_000  # 500 USDC
        receipt = {
            "transactionHash": "0x456",
            "blockNumber": 12346,
            "logs": [create_withdraw_log(USER_ADDRESS, USER_ADDRESS, withdraw_amount)],
            "gasUsed": 100000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CompoundV3EventType.WITHDRAW
        assert result.events[0].data["src"] == USER_ADDRESS.lower()
        assert result.events[0].data["to"] == USER_ADDRESS.lower()
        assert result.events[0].data["amount"] == Decimal(withdraw_amount)
        assert result.withdraw_amount == Decimal(withdraw_amount)
        assert result.supply_amount == Decimal("0")

    def test_parse_supply_collateral_event(self):
        """Test parsing a SupplyCollateral event."""
        parser = CompoundV3ReceiptParser()

        collateral_amount = 1_000_000_000  # 1000 WETH (18 decimals shown as raw)
        receipt = {
            "transactionHash": "0x789",
            "blockNumber": 12347,
            "logs": [create_supply_collateral_log(USER_ADDRESS, USER_ADDRESS, COLLATERAL_ASSET, collateral_amount)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CompoundV3EventType.SUPPLY_COLLATERAL
        assert result.events[0].data["from_address"] == USER_ADDRESS.lower()
        assert result.events[0].data["dst"] == USER_ADDRESS.lower()
        assert result.events[0].data["asset"] == COLLATERAL_ASSET.lower()
        assert result.events[0].data["amount"] == Decimal(collateral_amount)
        assert result.collateral_supplied[COLLATERAL_ASSET.lower()] == Decimal(collateral_amount)

    def test_parse_withdraw_collateral_event(self):
        """Test parsing a WithdrawCollateral event."""
        parser = CompoundV3ReceiptParser()

        collateral_amount = 500_000_000  # 500 WETH
        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 12348,
            "logs": [create_withdraw_collateral_log(USER_ADDRESS, USER_ADDRESS, COLLATERAL_ASSET, collateral_amount)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CompoundV3EventType.WITHDRAW_COLLATERAL
        assert result.events[0].data["src"] == USER_ADDRESS.lower()
        assert result.events[0].data["to"] == USER_ADDRESS.lower()
        assert result.events[0].data["asset"] == COLLATERAL_ASSET.lower()
        assert result.events[0].data["amount"] == Decimal(collateral_amount)
        assert result.collateral_withdrawn[COLLATERAL_ASSET.lower()] == Decimal(collateral_amount)

    def test_parse_transfer_event(self):
        """Test parsing a Transfer event."""
        parser = CompoundV3ReceiptParser()

        transfer_amount = 100_000_000  # 100 USDC
        receiver = "0x1234567890123456789012345678901234567890"
        receipt = {
            "transactionHash": "0xdef",
            "blockNumber": 12349,
            "logs": [create_transfer_log(USER_ADDRESS, receiver, transfer_amount)],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CompoundV3EventType.TRANSFER
        assert result.events[0].data["from_address"] == USER_ADDRESS.lower()
        assert result.events[0].data["to"] == receiver.lower()
        assert result.events[0].data["amount"] == Decimal(transfer_amount)

    def test_parse_transfer_collateral_event(self):
        """Test parsing a TransferCollateral event."""
        parser = CompoundV3ReceiptParser()

        transfer_amount = 100_000_000
        receiver = "0x1234567890123456789012345678901234567890"
        receipt = {
            "transactionHash": "0x111",
            "blockNumber": 12350,
            "logs": [create_transfer_collateral_log(USER_ADDRESS, receiver, COLLATERAL_ASSET, transfer_amount)],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CompoundV3EventType.TRANSFER_COLLATERAL
        assert result.events[0].data["from_address"] == USER_ADDRESS.lower()
        assert result.events[0].data["to"] == receiver.lower()
        assert result.events[0].data["asset"] == COLLATERAL_ASSET.lower()
        assert result.events[0].data["amount"] == Decimal(transfer_amount)

    def test_parse_absorb_debt_event(self):
        """Test parsing an AbsorbDebt event."""
        parser = CompoundV3ReceiptParser()

        absorber = "0xabcd1234abcd1234abcd1234abcd1234abcd1234"
        borrower = "0x5678efgh5678efgh5678efgh5678efgh5678efgh"
        base_paid_out = 100_000_000  # 100 USDC
        usd_value = 100_000_000  # $100

        receipt = {
            "transactionHash": "0x222",
            "blockNumber": 12351,
            "logs": [create_absorb_debt_log(absorber, borrower, base_paid_out, usd_value)],
            "gasUsed": 200000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CompoundV3EventType.ABSORB_DEBT
        assert result.events[0].data["absorber"] == absorber.lower()
        assert result.events[0].data["borrower"] == borrower.lower()
        assert result.events[0].data["base_paid_out"] == Decimal(base_paid_out)
        assert result.events[0].data["usd_value"] == Decimal(usd_value)

    def test_parse_absorb_collateral_event(self):
        """Test parsing an AbsorbCollateral event."""
        parser = CompoundV3ReceiptParser()

        absorber = "0xabcd1234abcd1234abcd1234abcd1234abcd1234"
        borrower = "0x5678efgh5678efgh5678efgh5678efgh5678efgh"
        collateral_absorbed = 1_000_000_000  # 1 ETH
        usd_value = 2_000_000_000  # $2000

        receipt = {
            "transactionHash": "0x333",
            "blockNumber": 12352,
            "logs": [
                create_absorb_collateral_log(absorber, borrower, COLLATERAL_ASSET, collateral_absorbed, usd_value)
            ],
            "gasUsed": 250000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CompoundV3EventType.ABSORB_COLLATERAL
        assert result.events[0].data["absorber"] == absorber.lower()
        assert result.events[0].data["borrower"] == borrower.lower()
        assert result.events[0].data["asset"] == COLLATERAL_ASSET.lower()
        assert result.events[0].data["collateral_absorbed"] == Decimal(collateral_absorbed)
        assert result.events[0].data["usd_value"] == Decimal(usd_value)

    def test_parse_buy_collateral_event(self):
        """Test parsing a BuyCollateral event."""
        parser = CompoundV3ReceiptParser()

        buyer = "0xabcd1234abcd1234abcd1234abcd1234abcd1234"
        base_amount = 1_000_000_000  # 1000 USDC
        collateral_amount = 500_000_000  # 0.5 ETH

        receipt = {
            "transactionHash": "0x444",
            "blockNumber": 12353,
            "logs": [create_buy_collateral_log(buyer, COLLATERAL_ASSET, base_amount, collateral_amount)],
            "gasUsed": 180000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CompoundV3EventType.BUY_COLLATERAL
        assert result.events[0].data["buyer"] == buyer.lower()
        assert result.events[0].data["asset"] == COLLATERAL_ASSET.lower()
        assert result.events[0].data["base_amount"] == Decimal(base_amount)
        assert result.events[0].data["collateral_amount"] == Decimal(collateral_amount)

    def test_amount_aggregation(self):
        """Test that amounts are aggregated correctly."""
        parser = CompoundV3ReceiptParser()

        supply_amount1 = 1_000_000_000
        supply_amount2 = 500_000_000
        withdraw_amount = 300_000_000
        collateral_amount1 = 2_000_000_000
        collateral_amount2 = 1_000_000_000
        collateral_withdraw = 500_000_000

        receipt = {
            "transactionHash": "0x555",
            "blockNumber": 12354,
            "logs": [
                create_supply_log(USER_ADDRESS, USER_ADDRESS, supply_amount1),
                create_supply_log(USER_ADDRESS, USER_ADDRESS, supply_amount2),
                create_withdraw_log(USER_ADDRESS, USER_ADDRESS, withdraw_amount),
                create_supply_collateral_log(USER_ADDRESS, USER_ADDRESS, COLLATERAL_ASSET, collateral_amount1),
                create_supply_collateral_log(USER_ADDRESS, USER_ADDRESS, COLLATERAL_ASSET, collateral_amount2),
                create_withdraw_collateral_log(USER_ADDRESS, USER_ADDRESS, COLLATERAL_ASSET, collateral_withdraw),
            ],
            "gasUsed": 300000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 6
        assert result.supply_amount == Decimal(supply_amount1 + supply_amount2)
        assert result.withdraw_amount == Decimal(withdraw_amount)
        assert result.collateral_supplied[COLLATERAL_ASSET.lower()] == Decimal(collateral_amount1 + collateral_amount2)
        assert result.collateral_withdrawn[COLLATERAL_ASSET.lower()] == Decimal(collateral_withdraw)

    def test_comet_address_filtering(self):
        """Test filtering by Comet contract address."""
        parser = CompoundV3ReceiptParser()

        other_address = "0x9999999999999999999999999999999999999999"
        supply_amount = 1_000_000_000

        # Create logs from both Comet and other address
        comet_log = create_supply_log(USER_ADDRESS, USER_ADDRESS, supply_amount)
        other_log = create_supply_log(USER_ADDRESS, USER_ADDRESS, supply_amount)
        other_log["address"] = other_address

        receipt = {
            "transactionHash": "0x666",
            "blockNumber": 12355,
            "logs": [comet_log, other_log],
            "gasUsed": 100000,
        }

        # Parse with comet_address filter
        result = parser.parse_receipt(receipt, comet_address=COMET_ADDRESS)

        # Should only parse the Comet log
        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].contract_address == COMET_ADDRESS

    def test_empty_logs(self):
        """Test parsing a receipt with no logs."""
        parser = CompoundV3ReceiptParser()

        receipt = {
            "transactionHash": "0x777",
            "blockNumber": 12356,
            "logs": [],
            "gasUsed": 21000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0
        assert result.supply_amount == Decimal("0")
        assert result.withdraw_amount == Decimal("0")

    def test_unknown_event(self):
        """Test that unknown events are ignored."""
        parser = CompoundV3ReceiptParser()

        unknown_log = {
            "address": COMET_ADDRESS,
            "topics": [
                "0x9999999999999999999999999999999999999999999999999999999999999999",  # Unknown
            ],
            "data": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "logIndex": 0,
        }

        receipt = {
            "transactionHash": "0x888",
            "blockNumber": 12357,
            "logs": [unknown_log],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0

    def test_bytes_transaction_hash(self):
        """Test handling bytes transaction hash."""
        parser = CompoundV3ReceiptParser()

        receipt = {
            "transactionHash": b"\x12\x34\x56\x78",
            "blockNumber": 12358,
            "logs": [create_supply_log(USER_ADDRESS, USER_ADDRESS, 1_000_000_000)],
            "gasUsed": 100000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].transaction_hash == "0x12345678"

    def test_to_dict_conversion(self):
        """Test converting ParseResult to dictionary."""
        parser = CompoundV3ReceiptParser()

        supply_amount = 1_000_000_000
        receipt = {
            "transactionHash": "0x999",
            "blockNumber": 12359,
            "logs": [
                create_supply_log(USER_ADDRESS, USER_ADDRESS, supply_amount),
                create_supply_collateral_log(USER_ADDRESS, USER_ADDRESS, COLLATERAL_ASSET, 500_000_000),
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)
        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert len(result_dict["events"]) == 2
        assert result_dict["supply_amount"] == str(supply_amount)
        assert COLLATERAL_ASSET.lower() in result_dict["collateral_supplied"]

    def test_backward_compatibility_methods(self):
        """Test backward compatibility methods."""
        parser = CompoundV3ReceiptParser()

        # Test is_compound_v3_event
        supply_topic = "0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e"
        assert parser.is_compound_v3_event(supply_topic) is True

        unknown_topic = "0x9999999999999999999999999999999999999999999999999999999999999999"
        assert parser.is_compound_v3_event(unknown_topic) is False

        # Test get_event_type
        event_type = parser.get_event_type(supply_topic)
        assert event_type == CompoundV3EventType.SUPPLY

        unknown_type = parser.get_event_type(unknown_topic)
        assert unknown_type == CompoundV3EventType.UNKNOWN

    def test_parse_logs_method(self):
        """Test parsing logs directly."""
        parser = CompoundV3ReceiptParser()

        logs = [
            create_supply_log(USER_ADDRESS, USER_ADDRESS, 1_000_000_000),
            create_withdraw_log(USER_ADDRESS, USER_ADDRESS, 500_000_000),
        ]

        events = parser.parse_logs(logs, tx_hash="0xaaa", block_number=12360)

        assert len(events) == 2
        assert events[0].event_type == CompoundV3EventType.SUPPLY
        assert events[1].event_type == CompoundV3EventType.WITHDRAW
        assert events[0].transaction_hash == "0xaaa"
        assert events[0].block_number == 12360
