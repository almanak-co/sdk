"""Tests for Uniswap V3 Receipt Parser (Refactored).

Tests for the refactored Uniswap V3 receipt parser using base infrastructure
(EventRegistry + HexDecoder).
"""

from decimal import Decimal

from almanak.framework.connectors.uniswap_v3.receipt_parser import (
    UniswapV3EventType,
    UniswapV3ReceiptParser,
)

# =============================================================================
# Test Data
# =============================================================================

POOL_ADDRESS = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
USER_ADDRESS = "0x742d35cc6634c0532925a3b844bc454e4438f44e"
WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC_ADDRESS = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


def create_swap_log(
    sender: str,
    recipient: str,
    amount0: int,  # Can be negative
    amount1: int,  # Can be negative
    sqrt_price_x96: int,
    liquidity: int,
    tick: int,
) -> dict:
    """Create a Swap event log.

    Note: amount0 and amount1 are signed int256 values.
    Negative values are represented in two's complement.
    """

    # Convert signed integers to two's complement hex
    def int_to_hex(value: int, bits: int = 256) -> str:
        """Convert signed integer to hex (two's complement)."""
        if value < 0:
            value = (1 << bits) + value
        return f"{value:064x}"

    # Build data field
    data = (
        int_to_hex(amount0)
        + int_to_hex(amount1)
        + f"{sqrt_price_x96:064x}"
        + f"{liquidity:064x}"
        + int_to_hex(tick, 256)  # int24 stored as int256
    )

    return {
        "address": POOL_ADDRESS,
        "topics": [
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",  # Swap
            f"0x000000000000000000000000{sender[2:].lower()}",
            f"0x000000000000000000000000{recipient[2:].lower()}",
        ],
        "data": f"0x{data}",
        "logIndex": 0,
    }


def create_transfer_log(from_addr: str, to_addr: str, value: int, token_address: str) -> dict:
    """Create a Transfer event log."""
    return {
        "address": token_address,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # Transfer
            f"0x000000000000000000000000{from_addr[2:].lower()}",
            f"0x000000000000000000000000{to_addr[2:].lower()}",
        ],
        "data": f"0x{value:064x}",
        "logIndex": 1,
    }


# =============================================================================
# Tests
# =============================================================================


class TestUniswapV3ReceiptParser:
    """Tests for UniswapV3ReceiptParser."""

    def test_parse_swap_event_token0_input(self):
        """Test parsing a Swap event where token0 is input."""
        parser = UniswapV3ReceiptParser(
            chain="ethereum",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
            token0_symbol="USDC",
            token1_symbol="WETH",
            token0_decimals=6,
            token1_decimals=18,
        )

        # Swap 1000 USDC (positive amount0) for WETH (negative amount1)
        amount_in = 1_000_000_000  # 1000 USDC (6 decimals)
        amount_out = 500_000_000_000_000_000  # 0.5 WETH (18 decimals)

        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    amount_in,  # amount0 positive (in)
                    -amount_out,  # amount1 negative (out)
                    1461446703485210103287273052203988822378723970341,  # sqrtPriceX96
                    1000000000000000000,  # liquidity
                    0,  # tick
                )
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == UniswapV3EventType.SWAP
        assert result.events[0].data["sender"] == USER_ADDRESS.lower()
        assert result.events[0].data["recipient"] == USER_ADDRESS.lower()
        assert result.events[0].data["amount0"] == amount_in
        assert result.events[0].data["amount1"] == -amount_out
        assert result.events[0].data["sqrt_price_x96"] == 1461446703485210103287273052203988822378723970341
        assert result.events[0].data["liquidity"] == 1000000000000000000
        assert result.events[0].data["tick"] == 0

        # Check swap_events
        assert len(result.swap_events) == 1
        assert result.swap_events[0].amount0 == amount_in
        assert result.swap_events[0].amount1 == -amount_out
        assert result.swap_events[0].token0_is_input is True
        assert result.swap_events[0].amount_in == amount_in
        assert result.swap_events[0].amount_out == amount_out

        # Check swap_result
        assert result.swap_result is not None
        assert result.swap_result.token_in == USDC_ADDRESS.lower()
        assert result.swap_result.token_out == WETH_ADDRESS.lower()
        assert result.swap_result.token_in_symbol == "USDC"
        assert result.swap_result.token_out_symbol == "WETH"
        assert result.swap_result.amount_in == amount_in
        assert result.swap_result.amount_out == amount_out

    def test_parse_swap_event_token1_input(self):
        """Test parsing a Swap event where token1 is input."""
        parser = UniswapV3ReceiptParser(
            chain="ethereum",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
            token0_symbol="USDC",
            token1_symbol="WETH",
            token0_decimals=6,
            token1_decimals=18,
        )

        # Swap WETH (positive amount1) for 2000 USDC (negative amount0)
        amount_in = 1_000_000_000_000_000_000  # 1 WETH (18 decimals)
        amount_out = 2_000_000_000  # 2000 USDC (6 decimals)

        receipt = {
            "transactionHash": "0x456",
            "blockNumber": 12346,
            "status": 1,
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    -amount_out,  # amount0 negative (out)
                    amount_in,  # amount1 positive (in)
                    1461446703485210103287273052203988822378723970341,
                    1000000000000000000,
                    0,
                )
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.swap_events) == 1
        assert result.swap_events[0].amount0 == -amount_out
        assert result.swap_events[0].amount1 == amount_in
        assert result.swap_events[0].token1_is_input is True
        assert result.swap_events[0].amount_in == amount_in
        assert result.swap_events[0].amount_out == amount_out

        # Check swap_result
        assert result.swap_result is not None
        assert result.swap_result.token_in == WETH_ADDRESS.lower()
        assert result.swap_result.token_out == USDC_ADDRESS.lower()
        assert result.swap_result.token_in_symbol == "WETH"
        assert result.swap_result.token_out_symbol == "USDC"
        assert result.swap_result.amount_in == amount_in
        assert result.swap_result.amount_out == amount_out

    def test_signed_integer_decoding(self):
        """Test proper decoding of signed integers (int256, int24)."""
        parser = UniswapV3ReceiptParser()

        # Create swap with negative amounts and tick
        amount0 = -1000000  # Negative
        amount1 = 2000000  # Positive
        tick = -887272  # Negative tick (minimum tick)

        receipt = {
            "transactionHash": "0x789",
            "blockNumber": 12347,
            "status": 1,
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    amount0,
                    amount1,
                    1000000000000000000000000,
                    500000000000000000,
                    tick,
                )
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].data["amount0"] == amount0
        assert result.events[0].data["amount1"] == amount1
        assert result.events[0].data["tick"] == tick

    def test_parse_transfer_event(self):
        """Test parsing Transfer events."""
        parser = UniswapV3ReceiptParser()

        transfer_amount = 1_000_000_000_000_000_000  # 1 ETH
        receiver = "0x1234567890123456789012345678901234567890"

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 12348,
            "status": 1,
            "logs": [create_transfer_log(USER_ADDRESS, receiver, transfer_amount, WETH_ADDRESS)],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == UniswapV3EventType.TRANSFER
        assert result.events[0].data["from_addr"] == USER_ADDRESS.lower()
        assert result.events[0].data["to_addr"] == receiver.lower()
        assert result.events[0].data["value"] == transfer_amount
        assert result.events[0].data["token_address"] == WETH_ADDRESS.lower()

        # Check transfer_events
        assert len(result.transfer_events) == 1
        assert result.transfer_events[0].from_addr == USER_ADDRESS.lower()
        assert result.transfer_events[0].to_addr == receiver.lower()
        assert result.transfer_events[0].value == transfer_amount

    def test_slippage_calculation_with_quoted_amount(self):
        """Test slippage calculation using quoted amount."""
        parser = UniswapV3ReceiptParser(
            chain="ethereum",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
            token0_decimals=6,
            token1_decimals=18,
        )

        amount_in = 1_000_000_000  # 1000 USDC
        amount_out_actual = 490_000_000_000_000_000  # 0.49 WETH
        amount_out_quoted = 500_000_000_000_000_000  # 0.50 WETH (expected)

        receipt = {
            "transactionHash": "0xdef",
            "blockNumber": 12349,
            "status": 1,
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    amount_in,
                    -amount_out_actual,
                    1461446703485210103287273052203988822378723970341,
                    1000000000000000000,
                    0,
                )
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt, quoted_amount_out=amount_out_quoted)

        assert result.success is True
        assert result.swap_result is not None

        # Slippage = (expected - actual) / expected * 10000
        # (500000000000000000 - 490000000000000000) / 500000000000000000 * 10000 = 200 bps (2%)
        expected_slippage = int((amount_out_quoted - amount_out_actual) / amount_out_quoted * 10000)
        assert result.swap_result.slippage_bps == expected_slippage
        assert result.swap_result.slippage_bps == 200  # 2% slippage

    def test_token_symbol_resolution(self):
        """Test automatic token symbol resolution from addresses."""
        parser = UniswapV3ReceiptParser(
            chain="ethereum",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
        )

        # Symbols should be auto-resolved
        assert parser.token0_symbol == "USDC"
        assert parser.token1_symbol == "WETH"

        # Decimals should be auto-set
        assert parser.token0_decimals == 6
        assert parser.token1_decimals == 18

    def test_token_decimals_from_symbol(self):
        """Test setting decimals from token symbol."""
        parser = UniswapV3ReceiptParser(
            chain="arbitrum",
            token0_symbol="USDC",
            token1_symbol="WETH",
        )

        assert parser.token0_decimals == 6
        assert parser.token1_decimals == 18

    def test_swap_result_price_calculation(self):
        """Test effective price calculation in swap result."""
        parser = UniswapV3ReceiptParser(
            chain="ethereum",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
            token0_decimals=6,
            token1_decimals=18,
        )

        amount_in = 2_000_000_000  # 2000 USDC
        amount_out = 1_000_000_000_000_000_000  # 1 WETH

        receipt = {
            "transactionHash": "0x111",
            "blockNumber": 12350,
            "status": 1,
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    amount_in,
                    -amount_out,
                    1461446703485210103287273052203988822378723970341,
                    1000000000000000000,
                    0,
                )
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.swap_result is not None

        # Effective price = amount_out / amount_in = 1 WETH / 2000 USDC = 0.0005 WETH per USDC
        # Or viewed as 2000 USDC per WETH
        amount_in_decimal = Decimal("2000")
        amount_out_decimal = Decimal("1")
        expected_price = amount_out_decimal / amount_in_decimal

        assert result.swap_result.amount_in_decimal == amount_in_decimal
        assert result.swap_result.amount_out_decimal == amount_out_decimal
        assert result.swap_result.effective_price == expected_price

    def test_failed_transaction(self):
        """Test handling of failed transactions."""
        parser = UniswapV3ReceiptParser()

        receipt = {
            "transactionHash": "0x222",
            "blockNumber": 12351,
            "status": 0,  # Failed
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    1000000,
                    -500000,
                    1000000000000000000000000,
                    500000000000000000,
                    0,
                )
            ],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        assert result.error == "Transaction reverted"
        assert len(result.events) == 0

    def test_empty_logs(self):
        """Test parsing receipt with no logs."""
        parser = UniswapV3ReceiptParser()

        receipt = {
            "transactionHash": "0x333",
            "blockNumber": 12352,
            "status": 1,
            "logs": [],
            "gasUsed": 21000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0
        assert result.swap_result is None

    def test_unknown_event(self):
        """Test that unknown events are ignored."""
        parser = UniswapV3ReceiptParser()

        unknown_log = {
            "address": POOL_ADDRESS,
            "topics": [
                "0x9999999999999999999999999999999999999999999999999999999999999999",  # Unknown
            ],
            "data": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "logIndex": 0,
        }

        receipt = {
            "transactionHash": "0x444",
            "blockNumber": 12353,
            "status": 1,
            "logs": [unknown_log],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0

    def test_bytes_transaction_hash(self):
        """Test handling bytes transaction hash."""
        parser = UniswapV3ReceiptParser()

        receipt = {
            "transactionHash": b"\x12\x34\x56\x78",
            "blockNumber": 12354,
            "status": 1,
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    1000000,
                    -500000,
                    1000000000000000000000000,
                    500000000000000000,
                    0,
                )
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash == "0x12345678"

    def test_to_dict_conversion(self):
        """Test converting ParseResult to dictionary."""
        parser = UniswapV3ReceiptParser(
            chain="ethereum",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
        )

        receipt = {
            "transactionHash": "0x555",
            "blockNumber": 12355,
            "status": 1,
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    1000000000,
                    -500000000000000000,
                    1461446703485210103287273052203988822378723970341,
                    1000000000000000000,
                    0,
                )
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)
        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert len(result_dict["events"]) == 1
        assert result_dict["swap_result"] is not None
        assert result_dict["swap_result"]["token_in_symbol"] == "USDC"
        assert result_dict["swap_result"]["token_out_symbol"] == "WETH"

    def test_swap_result_payload_conversion(self):
        """Test converting ParsedSwapResult to SwapResultPayload."""
        parser = UniswapV3ReceiptParser(
            chain="ethereum",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
        )

        receipt = {
            "transactionHash": "0x666",
            "blockNumber": 12356,
            "status": 1,
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    1000000000,
                    -500000000000000000,
                    1461446703485210103287273052203988822378723970341,
                    1000000000000000000,
                    0,
                )
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.swap_result is not None
        payload = result.swap_result.to_swap_result_payload()

        assert payload.token_in == "USDC"
        assert payload.token_out == "WETH"
        assert payload.amount_in == Decimal("1000")
        assert payload.amount_out == Decimal("0.5")

    def test_backward_compatibility_methods(self):
        """Test backward compatibility methods."""
        parser = UniswapV3ReceiptParser()

        # Test is_uniswap_event
        swap_topic = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
        assert parser.is_uniswap_event(swap_topic) is True

        unknown_topic = "0x9999999999999999999999999999999999999999999999999999999999999999"
        assert parser.is_uniswap_event(unknown_topic) is False

        # Test get_event_type
        event_type = parser.get_event_type(swap_topic)
        assert event_type == UniswapV3EventType.SWAP

        unknown_type = parser.get_event_type(unknown_topic)
        assert unknown_type == UniswapV3EventType.UNKNOWN

    def test_parse_logs_method(self):
        """Test parsing logs directly."""
        parser = UniswapV3ReceiptParser()

        logs = [
            create_swap_log(
                USER_ADDRESS,
                USER_ADDRESS,
                1000000,
                -500000,
                1000000000000000000000000,
                500000000000000000,
                0,
            ),
            create_transfer_log(USER_ADDRESS, POOL_ADDRESS, 1000000, USDC_ADDRESS),
        ]

        events = parser.parse_logs(logs)

        assert len(events) == 2
        assert events[0].event_type == UniswapV3EventType.SWAP
        assert events[1].event_type == UniswapV3EventType.TRANSFER

    def test_uint160_and_uint128_decoding(self):
        """Test proper decoding of uint160 (sqrtPriceX96) and uint128 (liquidity)."""
        parser = UniswapV3ReceiptParser()

        # Use large values to test uint160 and uint128 specifically
        sqrt_price_x96 = 2**160 - 1  # Max uint160
        liquidity = 2**128 - 1  # Max uint128

        receipt = {
            "transactionHash": "0x777",
            "blockNumber": 12357,
            "status": 1,
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    1000000,
                    -500000,
                    sqrt_price_x96,
                    liquidity,
                    0,
                )
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].data["sqrt_price_x96"] == sqrt_price_x96
        assert result.events[0].data["liquidity"] == liquidity

    def test_extreme_negative_tick(self):
        """Test handling of extreme negative tick values."""
        parser = UniswapV3ReceiptParser()

        # Minimum tick in Uniswap V3
        min_tick = -887272

        receipt = {
            "transactionHash": "0x888",
            "blockNumber": 12358,
            "status": 1,
            "logs": [
                create_swap_log(
                    USER_ADDRESS,
                    USER_ADDRESS,
                    1000000,
                    -500000,
                    1000000000000000000000000,
                    500000000000000000,
                    min_tick,
                )
            ],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].data["tick"] == min_tick


# =============================================================================
# Position ID Extraction Tests
# =============================================================================


POSITION_MANAGER_ADDRESS = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"
ZERO_ADDRESS_PADDED = "0x" + "0" * 64


def create_erc721_transfer_log(from_addr: str, to_addr: str, token_id: int, contract_address: str) -> dict:
    """Create an ERC-721 Transfer event log.

    ERC-721 Transfer has indexed tokenId (in topics[3]), unlike ERC-20 which has value in data.
    Event signature: Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
    """
    return {
        "address": contract_address,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # Transfer
            f"0x000000000000000000000000{from_addr[2:].lower()}" if from_addr.startswith("0x") else ZERO_ADDRESS_PADDED,
            f"0x000000000000000000000000{to_addr[2:].lower()}",
            f"0x{token_id:064x}",  # tokenId in topics[3] for ERC-721
        ],
        "data": "0x",  # Empty data for ERC-721 Transfer
        "logIndex": 0,
    }


class TestPositionIdExtraction:
    """Tests for LP position ID extraction from receipts."""

    def test_extract_position_id_from_mint(self):
        """Test extracting position ID from a mint (LP open) transaction."""
        parser = UniswapV3ReceiptParser(chain="arbitrum")

        token_id = 123456
        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [
                # ERC-721 Transfer from zero address (mint)
                create_erc721_transfer_log(
                    "0x0000000000000000000000000000000000000000",
                    USER_ADDRESS,
                    token_id,
                    POSITION_MANAGER_ADDRESS,
                ),
            ],
            "gasUsed": 300000,
        }

        position_id = parser.extract_position_id(receipt)

        assert position_id == token_id

    def test_extract_position_id_large_token_id(self):
        """Test extracting a large position ID."""
        parser = UniswapV3ReceiptParser(chain="ethereum")

        # Large token ID (realistic for mature Uniswap deployment)
        token_id = 987654321

        receipt = {
            "transactionHash": "0xdef456",
            "blockNumber": 12346,
            "status": 1,
            "logs": [
                create_erc721_transfer_log(
                    "0x0000000000000000000000000000000000000000",
                    USER_ADDRESS,
                    token_id,
                    POSITION_MANAGER_ADDRESS,
                ),
            ],
            "gasUsed": 300000,
        }

        position_id = parser.extract_position_id(receipt)

        assert position_id == token_id

    def test_extract_position_id_no_mint_event(self):
        """Test that non-mint transfers don't return position ID."""
        parser = UniswapV3ReceiptParser(chain="arbitrum")

        # Transfer between two users (not a mint)
        receipt = {
            "transactionHash": "0x789",
            "blockNumber": 12347,
            "status": 1,
            "logs": [
                create_erc721_transfer_log(
                    USER_ADDRESS,  # From non-zero address
                    "0x1234567890123456789012345678901234567890",
                    12345,
                    POSITION_MANAGER_ADDRESS,
                ),
            ],
            "gasUsed": 100000,
        }

        position_id = parser.extract_position_id(receipt)

        assert position_id is None

    def test_extract_position_id_wrong_contract(self):
        """Test that transfers from wrong contract are ignored."""
        parser = UniswapV3ReceiptParser(chain="arbitrum")

        # Transfer from a different contract (not position manager)
        receipt = {
            "transactionHash": "0xaaa",
            "blockNumber": 12348,
            "status": 1,
            "logs": [
                create_erc721_transfer_log(
                    "0x0000000000000000000000000000000000000000",
                    USER_ADDRESS,
                    99999,
                    "0x1234567890123456789012345678901234567890",  # Wrong contract
                ),
            ],
            "gasUsed": 100000,
        }

        position_id = parser.extract_position_id(receipt)

        assert position_id is None

    def test_extract_position_id_empty_logs(self):
        """Test handling of receipt with no logs."""
        parser = UniswapV3ReceiptParser(chain="arbitrum")

        receipt = {
            "transactionHash": "0xbbb",
            "blockNumber": 12349,
            "status": 1,
            "logs": [],
            "gasUsed": 21000,
        }

        position_id = parser.extract_position_id(receipt)

        assert position_id is None

    def test_extract_position_id_with_other_events(self):
        """Test extracting position ID when receipt has multiple events."""
        parser = UniswapV3ReceiptParser(chain="arbitrum")

        token_id = 555555
        receipt = {
            "transactionHash": "0xccc",
            "blockNumber": 12350,
            "status": 1,
            "logs": [
                # Some other event first
                {
                    "address": USDC_ADDRESS,
                    "topics": [
                        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",  # Approval
                        f"0x000000000000000000000000{USER_ADDRESS[2:].lower()}",
                        f"0x000000000000000000000000{POSITION_MANAGER_ADDRESS[2:].lower()}",
                    ],
                    "data": f"0x{'f' * 64}",  # Max approval
                    "logIndex": 0,
                },
                # The mint event we're looking for
                create_erc721_transfer_log(
                    "0x0000000000000000000000000000000000000000",
                    USER_ADDRESS,
                    token_id,
                    POSITION_MANAGER_ADDRESS,
                ),
                # More events after
                create_transfer_log(USER_ADDRESS, POOL_ADDRESS, 1000000, USDC_ADDRESS),
            ],
            "gasUsed": 400000,
        }

        position_id = parser.extract_position_id(receipt)

        assert position_id == token_id

    def test_extract_position_id_static_method(self):
        """Test the static method for extracting position ID from logs."""
        token_id = 777777
        logs = [
            create_erc721_transfer_log(
                "0x0000000000000000000000000000000000000000",
                USER_ADDRESS,
                token_id,
                POSITION_MANAGER_ADDRESS,
            ),
        ]

        position_id = UniswapV3ReceiptParser.extract_position_id_from_logs(logs, chain="arbitrum")

        assert position_id == token_id

    def test_extract_position_id_base_chain(self):
        """Test position ID extraction on Base chain (different position manager)."""
        parser = UniswapV3ReceiptParser(chain="base")

        # Base has a different position manager address
        base_position_manager = "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1"
        token_id = 42

        receipt = {
            "transactionHash": "0xddd",
            "blockNumber": 12351,
            "status": 1,
            "logs": [
                create_erc721_transfer_log(
                    "0x0000000000000000000000000000000000000000",
                    USER_ADDRESS,
                    token_id,
                    base_position_manager,
                ),
            ],
            "gasUsed": 300000,
        }

        position_id = parser.extract_position_id(receipt)

        assert position_id == token_id
