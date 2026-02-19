"""Tests for Curve Receipt Parser (Refactored)."""

from almanak.framework.connectors.curve.receipt_parser import (
    CurveEventType,
    CurveReceiptParser,
)

# Test data
POOL_ADDRESS = "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7"
USER_ADDRESS = "0x742d35cc6634c0532925a3b844bc454e4438f44e"


def create_token_exchange_log(buyer, sold_id, tokens_sold, bought_id, tokens_bought):
    """Create TokenExchange log with int128 token IDs."""

    # Convert signed int128 to two's complement if negative
    def int128_to_hex(value):
        if value < 0:
            value = (1 << 128) + value
        return f"{value:064x}"

    data = int128_to_hex(sold_id) + f"{tokens_sold:064x}" + int128_to_hex(bought_id) + f"{tokens_bought:064x}"

    return {
        "address": POOL_ADDRESS,
        "topics": [
            "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140",
            f"0x000000000000000000000000{buyer[2:].lower()}",
        ],
        "data": f"0x{data}",
        "logIndex": 0,
    }


def create_add_liquidity_2_log(provider, amounts, fees, invariant, supply):
    """Create AddLiquidity log for 2-coin pool."""
    data = f"{amounts[0]:064x}{amounts[1]:064x}" + f"{fees[0]:064x}{fees[1]:064x}" + f"{invariant:064x}{supply:064x}"

    return {
        "address": POOL_ADDRESS,
        "topics": [
            "0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768",
            f"0x000000000000000000000000{provider[2:].lower()}",
        ],
        "data": f"0x{data}",
        "logIndex": 1,
    }


def create_remove_liquidity_2_log(provider, amounts, fees, supply):
    """Create RemoveLiquidity log for 2-coin pool."""
    data = f"{amounts[0]:064x}{amounts[1]:064x}" + f"{fees[0]:064x}{fees[1]:064x}" + f"{supply:064x}"

    return {
        "address": POOL_ADDRESS,
        "topics": [
            "0x7c363854ccf79623411f8995b362bce5eddff18c927edc6f5dbbb5e05819a82c",
            f"0x000000000000000000000000{provider[2:].lower()}",
        ],
        "data": f"0x{data}",
        "logIndex": 2,
    }


class TestCurveReceiptParser:
    """Tests for CurveReceiptParser."""

    def test_parse_token_exchange(self):
        """Test parsing TokenExchange event."""
        parser = CurveReceiptParser(chain="ethereum")

        sold_id = 0  # DAI
        bought_id = 1  # USDC
        tokens_sold = 1_000_000_000_000_000_000_000  # 1000 DAI
        tokens_bought = 999_000_000  # 999 USDC

        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [create_token_exchange_log(USER_ADDRESS, sold_id, tokens_sold, bought_id, tokens_bought)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CurveEventType.TOKEN_EXCHANGE
        assert result.events[0].data["buyer"] == USER_ADDRESS.lower()
        assert result.events[0].data["sold_id"] == sold_id
        assert result.events[0].data["tokens_sold"] == tokens_sold
        assert result.events[0].data["bought_id"] == bought_id
        assert result.events[0].data["tokens_bought"] == tokens_bought

        # Check swap_events
        assert len(result.swap_events) == 1
        assert result.swap_events[0].sold_id == sold_id
        assert result.swap_events[0].bought_id == bought_id

    def test_parse_token_exchange_3pool(self):
        """Test parsing TokenExchange for 3-coin pool."""
        parser = CurveReceiptParser()

        # 3-coin pool indices (DAI, USDC, USDT)
        sold_id = 1  # USDC
        bought_id = 2  # USDT
        tokens_sold = 1_000_000_000  # 1000 USDC
        tokens_bought = 999_500_000  # 999.5 USDT

        receipt = {
            "transactionHash": "0x456",
            "blockNumber": 12346,
            "status": 1,
            "logs": [create_token_exchange_log(USER_ADDRESS, sold_id, tokens_sold, bought_id, tokens_bought)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].data["sold_id"] == sold_id
        assert result.events[0].data["bought_id"] == bought_id
        assert result.swap_events[0].tokens_sold == tokens_sold
        assert result.swap_events[0].tokens_bought == tokens_bought

    def test_parse_token_exchange_underlying(self):
        """Test parsing TokenExchangeUnderlying event."""
        parser = CurveReceiptParser()

        # Define test values
        sold_id = 1
        bought_id = 2
        tokens_sold = 1_000_000_000_000_000_000_000  # 1000 tokens (18 decimals)
        tokens_bought = 999_000_000  # 999 tokens (6 decimals)

        # Convert signed int128 to two's complement if negative
        def int128_to_hex(value):
            if value < 0:
                value = (1 << 128) + value
            return f"{value:064x}"

        # Construct properly formatted data (exactly 256 hex chars)
        data = int128_to_hex(sold_id) + f"{tokens_sold:064x}" + int128_to_hex(bought_id) + f"{tokens_bought:064x}"

        receipt = {
            "transactionHash": "0x789",
            "blockNumber": 12347,
            "status": 1,
            "logs": [
                {
                    "address": POOL_ADDRESS,
                    "topics": [
                        "0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b",
                        f"0x000000000000000000000000{USER_ADDRESS[2:].lower()}",
                    ],
                    "data": f"0x{data}",
                    "logIndex": 0,
                }
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == CurveEventType.TOKEN_EXCHANGE_UNDERLYING
        assert result.events[0].data["buyer"] == USER_ADDRESS.lower()
        assert result.events[0].data["sold_id"] == sold_id
        assert result.events[0].data["tokens_sold"] == tokens_sold
        assert result.events[0].data["bought_id"] == bought_id
        assert result.events[0].data["tokens_bought"] == tokens_bought

    def test_parse_add_liquidity_2pool(self):
        """Test parsing AddLiquidity event for 2-coin pool."""
        parser = CurveReceiptParser()

        amounts = [1_000_000, 2_000_000]
        fees = [100, 200]
        invariant = 3_000_000
        supply = 2_900_000

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 12348,
            "status": 1,
            "logs": [create_add_liquidity_2_log(USER_ADDRESS, amounts, fees, invariant, supply)],
            "gasUsed": 200000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == CurveEventType.ADD_LIQUIDITY
        assert result.events[0].data["provider"] == USER_ADDRESS.lower()
        assert result.events[0].data["token_amounts"] == amounts
        assert result.events[0].data["fees"] == fees
        assert result.events[0].data["invariant"] == invariant
        assert result.events[0].data["token_supply"] == supply

    def test_parse_remove_liquidity_2pool(self):
        """Test parsing RemoveLiquidity event for 2-coin pool."""
        parser = CurveReceiptParser()

        amounts = [1_000_000, 2_000_000]
        fees = [100, 200]
        supply = 2_700_000

        receipt = {
            "transactionHash": "0xdef",
            "blockNumber": 12349,
            "status": 1,
            "logs": [create_remove_liquidity_2_log(USER_ADDRESS, amounts, fees, supply)],
            "gasUsed": 200000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == CurveEventType.REMOVE_LIQUIDITY
        assert result.events[0].data["token_amounts"] == amounts
        assert result.events[0].data["fees"] == fees
        assert result.events[0].data["token_supply"] == supply

    def test_failed_transaction(self):
        """Test handling failed transactions."""
        parser = CurveReceiptParser()

        receipt = {
            "transactionHash": "0x111",
            "blockNumber": 12350,
            "status": 0,
            "logs": [create_token_exchange_log(USER_ADDRESS, 0, 1000000, 1, 999000)],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        assert result.error == "Transaction reverted"

    def test_empty_logs(self):
        """Test parsing receipt with no logs."""
        parser = CurveReceiptParser()

        receipt = {
            "transactionHash": "0x222",
            "blockNumber": 12351,
            "status": 1,
            "logs": [],
            "gasUsed": 21000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0

    def test_backward_compatibility(self):
        """Test backward compatibility methods."""
        parser = CurveReceiptParser()

        token_exchange_topic = "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"
        assert parser.is_curve_event(token_exchange_topic) is True
        assert parser.get_event_type(token_exchange_topic) == CurveEventType.TOKEN_EXCHANGE

        unknown_topic = "0x9999999999999999999999999999999999999999999999999999999999999999"
        assert parser.is_curve_event(unknown_topic) is False
        assert parser.get_event_type(unknown_topic) == CurveEventType.UNKNOWN

    def test_bytes_transaction_hash(self):
        """Test handling bytes transaction hash."""
        parser = CurveReceiptParser()

        receipt = {
            "transactionHash": b"\x12\x34\x56\x78",
            "blockNumber": 12352,
            "status": 1,
            "logs": [create_token_exchange_log(USER_ADDRESS, 0, 1000000, 1, 999000)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash == "0x12345678"
