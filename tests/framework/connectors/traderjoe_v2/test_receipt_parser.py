"""Tests for TraderJoe V2 receipt parser (refactored version)."""

import importlib.util

# Load the v2 module for testing
spec = importlib.util.spec_from_file_location(
    "traderjoe_v2_parser_v2", "almanak/framework/connectors/traderjoe_v2/receipt_parser.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

TraderJoeV2ReceiptParser = module.TraderJoeV2ReceiptParser
TraderJoeV2EventType = module.TraderJoeV2EventType
ParseResult = module.ParseResult
EVENT_TOPICS = module.EVENT_TOPICS


class TestTraderJoeV2ReceiptParserBasic:
    """Basic tests for TraderJoeV2ReceiptParser."""

    def test_parse_receipt_with_swap(self):
        """Test parsing receipt with Transfer events (swap)."""
        parser = TraderJoeV2ReceiptParser()

        receipt = {
            "transactionHash": "0xswap123",
            "blockNumber": 12345,
            "gasUsed": 100000,
            "status": 1,
            "logs": [
                # Transfer IN (user -> pool)
                {
                    "address": "0xtoken_in",
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x000000000000000000000000" + "a" * 40,  # from = user
                        "0x000000000000000000000000" + "b" * 40,  # to = pool
                    ],
                    "data": (
                        "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1e18
                    ),
                },
                # Transfer OUT (pool -> user)
                {
                    "address": "0xtoken_out",
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x000000000000000000000000" + "b" * 40,  # from = pool
                        "0x000000000000000000000000" + "a" * 40,  # to = user
                    ],
                    "data": (
                        "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1e18
                    ),
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 2
        assert result.swap_result is not None
        assert result.swap_result.success is True
        assert result.swap_result.amount_in == 1000000000000000000
        assert result.swap_result.amount_out == 1000000000000000000
        assert result.swap_result.token_in == "0xtoken_in"
        assert result.swap_result.token_out == "0xtoken_out"

    def test_parse_receipt_with_deposited_to_bins(self):
        """Test parsing receipt with DepositedToBins event."""
        parser = TraderJoeV2ReceiptParser()

        # Create data for DepositedToBins with dynamic arrays
        # Structure: offset_ids, offset_amounts, [ids array], [amounts array]
        # offset_ids = 0x40 (64 bytes) - points to ids array
        # offset_amounts = 0xc0 (192 bytes) - points to amounts array after ids
        # ids = [1, 2, 3] takes 4*32 = 128 bytes (length + 3 elements)
        # amounts = [100, 200, 300] (bytes32[])

        data_hex = (
            "0x"
            # Offset to ids array (64 bytes)
            + "0000000000000000000000000000000000000000000000000000000000000040"
            # Offset to amounts array (192 bytes = 64 + 128)
            + "00000000000000000000000000000000000000000000000000000000000000c0"
            # ids array: length = 3
            + "0000000000000000000000000000000000000000000000000000000000000003"
            # ids[0] = 1
            + "0000000000000000000000000000000000000000000000000000000000000001"
            # ids[1] = 2
            + "0000000000000000000000000000000000000000000000000000000000000002"
            # ids[2] = 3
            + "0000000000000000000000000000000000000000000000000000000000000003"
            # amounts array: length = 3
            + "0000000000000000000000000000000000000000000000000000000000000003"
            # amounts[0] = 100
            + "0000000000000000000000000000000000000000000000000000000000000064"
            # amounts[1] = 200
            + "00000000000000000000000000000000000000000000000000000000000000c8"
            # amounts[2] = 300
            + "000000000000000000000000000000000000000000000000000000000000012c"
        )

        receipt = {
            "transactionHash": "0xdeposit123",
            "blockNumber": 12345,
            "gasUsed": 200000,
            "status": 1,
            "logs": [
                {
                    "address": "0xpool_address",
                    "topics": [
                        EVENT_TOPICS["DepositedToBins"],
                        "0x000000000000000000000000" + "a" * 40,  # sender
                        "0x000000000000000000000000" + "b" * 40,  # to
                    ],
                    "data": data_hex,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == TraderJoeV2EventType.DEPOSITED_TO_BINS
        assert result.events[0].data["sender"] == "0x" + "a" * 40
        assert result.events[0].data["to"] == "0x" + "b" * 40
        # Note: The original parser didn't parse the dynamic arrays, just stored raw_data
        # We maintain the same behavior
        assert "raw_data" in result.events[0].data

        # Check liquidity result
        assert result.liquidity_result is not None
        assert result.liquidity_result.success is True
        assert result.liquidity_result.is_add is True

    def test_parse_receipt_with_withdrawn_from_bins(self):
        """Test parsing receipt with WithdrawnFromBins event."""
        parser = TraderJoeV2ReceiptParser()

        # Create data for WithdrawnFromBins with dynamic arrays
        # ids = [10, 20] takes 3*32 = 96 bytes (length + 2 elements)
        # amounts starts at 64 + 96 = 160 (0xa0)
        data_hex = (
            "0x"
            # Offset to ids array (64 bytes)
            + "0000000000000000000000000000000000000000000000000000000000000040"
            # Offset to amounts array (160 bytes = 64 + 96)
            + "00000000000000000000000000000000000000000000000000000000000000a0"
            # ids array: length = 2
            + "0000000000000000000000000000000000000000000000000000000000000002"
            # ids[0] = 10
            + "000000000000000000000000000000000000000000000000000000000000000a"
            # ids[1] = 20
            + "0000000000000000000000000000000000000000000000000000000000000014"
            # amounts array: length = 2
            + "0000000000000000000000000000000000000000000000000000000000000002"
            # amounts[0] = 500
            + "00000000000000000000000000000000000000000000000000000000000001f4"
            # amounts[1] = 600
            + "0000000000000000000000000000000000000000000000000000000000000258"
        )

        receipt = {
            "transactionHash": "0xwithdraw123",
            "blockNumber": 12345,
            "gasUsed": 150000,
            "status": 1,
            "logs": [
                {
                    "address": "0xpool_address",
                    "topics": [
                        EVENT_TOPICS["WithdrawnFromBins"],
                        "0x000000000000000000000000" + "c" * 40,  # sender
                        "0x000000000000000000000000" + "d" * 40,  # to
                    ],
                    "data": data_hex,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS
        assert result.events[0].data["sender"] == "0x" + "c" * 40
        assert result.events[0].data["to"] == "0x" + "d" * 40
        # Original parser didn't parse the arrays, just stored raw_data
        assert "raw_data" in result.events[0].data

        # Check liquidity result
        assert result.liquidity_result is not None
        assert result.liquidity_result.success is True
        assert result.liquidity_result.is_add is False

    def test_parse_receipt_with_approval(self):
        """Test parsing receipt with Approval event."""
        parser = TraderJoeV2ReceiptParser()

        receipt = {
            "transactionHash": "0xapproval123",
            "blockNumber": 12345,
            "gasUsed": 50000,
            "status": 1,
            "logs": [
                {
                    "address": "0xtoken",
                    "topics": [
                        EVENT_TOPICS["Approval"],
                        "0x000000000000000000000000" + "a" * 40,  # owner
                        "0x000000000000000000000000" + "b" * 40,  # spender
                    ],
                    "data": (
                        "0x" + "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"  # max approval
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == TraderJoeV2EventType.APPROVAL
        assert result.events[0].data["owner"] == "0x" + "a" * 40
        assert result.events[0].data["spender"] == "0x" + "b" * 40
        assert result.events[0].data["value"] == 2**256 - 1  # max uint256

    def test_parse_receipt_with_deposit(self):
        """Test parsing receipt with Deposit event (WAVAX wrap)."""
        parser = TraderJoeV2ReceiptParser()

        receipt = {
            "transactionHash": "0xdeposit123",
            "blockNumber": 12345,
            "gasUsed": 30000,
            "status": 1,
            "logs": [
                {
                    "address": "0xwavax",
                    "topics": [
                        EVENT_TOPICS["Deposit"],
                        "0x000000000000000000000000" + "a" * 40,  # dst
                    ],
                    "data": (
                        "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1 AVAX
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == TraderJoeV2EventType.DEPOSIT
        assert result.events[0].data["dst"] == "0x" + "a" * 40
        assert result.events[0].data["wad"] == 1000000000000000000

    def test_parse_receipt_with_withdrawal(self):
        """Test parsing receipt with Withdrawal event (WAVAX unwrap)."""
        parser = TraderJoeV2ReceiptParser()

        receipt = {
            "transactionHash": "0xwithdrawal123",
            "blockNumber": 12345,
            "gasUsed": 40000,
            "status": 1,
            "logs": [
                {
                    "address": "0xwavax",
                    "topics": [
                        EVENT_TOPICS["Withdrawal"],
                        "0x000000000000000000000000" + "a" * 40,  # src
                    ],
                    "data": (
                        "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1 AVAX
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == TraderJoeV2EventType.WITHDRAWAL
        assert result.events[0].data["src"] == "0x" + "a" * 40
        assert result.events[0].data["wad"] == 1000000000000000000

    def test_parse_receipt_failed_transaction(self):
        """Test parsing failed transaction."""
        parser = TraderJoeV2ReceiptParser()

        receipt = {
            "transactionHash": "0xfailed",
            "blockNumber": 123,
            "gasUsed": 100000,
            "status": 0,  # Failed
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is False
        assert result.error == "Transaction reverted"

    def test_parse_receipt_empty_logs(self):
        """Test parsing receipt with no logs."""
        parser = TraderJoeV2ReceiptParser()

        receipt = {
            "transactionHash": "0xempty",
            "blockNumber": 123,
            "gasUsed": 21000,
            "status": 1,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0
        assert result.swap_result is None
        assert result.liquidity_result is None

    def test_parse_receipt_filters_unknown_events(self):
        """Test that unknown events are filtered out."""
        parser = TraderJoeV2ReceiptParser()

        receipt = {
            "transactionHash": "0xunknown",
            "blockNumber": 123,
            "gasUsed": 50000,
            "status": 1,
            "logs": [
                {
                    "address": "0xtoken",
                    "topics": ["0xunknown_event"],
                    "data": "0x" + "00" * 64,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0

    def test_parse_swap_events_method(self):
        """Test parse_swap_events convenience method."""
        parser = TraderJoeV2ReceiptParser()

        receipt = {
            "transactionHash": "0xswap123",
            "blockNumber": 12345,
            "gasUsed": 100000,
            "status": 1,
            "logs": [
                # Transfer IN
                {
                    "address": "0xtoken_in",
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x" + "00" * 12 + "a" * 40,
                        "0x" + "00" * 12 + "b" * 40,
                    ],
                    "data": "0x" + "00" * 31 + "64",  # 100
                },
                # Transfer OUT
                {
                    "address": "0xtoken_out",
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x" + "00" * 12 + "b" * 40,
                        "0x" + "00" * 12 + "a" * 40,
                    ],
                    "data": "0x" + "00" * 31 + "5f",  # 95
                },
            ],
        }

        swaps = parser.parse_swap_events(receipt)

        assert len(swaps) == 1
        assert swaps[0].amount_in == 100
        assert swaps[0].amount_out == 95
        assert swaps[0].token_in == "0xtoken_in"
        assert swaps[0].token_out == "0xtoken_out"


class TestTraderJoeV2SwapExtraction:
    """Tests for swap result extraction."""

    def test_extract_swap_requires_two_transfers(self):
        """Test that swap extraction requires at least 2 Transfer events."""
        parser = TraderJoeV2ReceiptParser()

        receipt = {
            "transactionHash": "0xswap",
            "blockNumber": 123,
            "gasUsed": 100000,
            "status": 1,
            "logs": [
                # Only one Transfer
                {
                    "address": "0xtoken",
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x" + "00" * 12 + "a" * 40,
                        "0x" + "00" * 12 + "b" * 40,
                    ],
                    "data": "0x" + "00" * 64,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.swap_result is None  # Not enough transfers


class TestTraderJoeV2LiquidityExtraction:
    """Tests for liquidity result extraction."""

    def test_extract_liquidity_prioritizes_deposit(self):
        """Test that deposit events are prioritized over withdraw events."""
        parser = TraderJoeV2ReceiptParser()

        # Create minimal data for arrays
        # ids = [5] takes 2*32 = 64 bytes (length + 1 element)
        # amounts starts at 64 + 64 = 128 (0x80)
        data_hex = (
            "0x"
            + "0000000000000000000000000000000000000000000000000000000000000040"  # offset_ids = 64
            + "0000000000000000000000000000000000000000000000000000000000000080"  # offset_amounts = 128
            + "0000000000000000000000000000000000000000000000000000000000000001"  # length = 1
            + "0000000000000000000000000000000000000000000000000000000000000005"  # ids[0] = 5
            + "0000000000000000000000000000000000000000000000000000000000000001"  # amounts length = 1
            + "0000000000000000000000000000000000000000000000000000000000000064"  # amounts[0] = 100
        )

        receipt = {
            "transactionHash": "0xliq",
            "blockNumber": 123,
            "gasUsed": 200000,
            "status": 1,
            "logs": [
                # Both deposit and withdraw
                {
                    "address": "0xpool",
                    "topics": [
                        EVENT_TOPICS["DepositedToBins"],
                        "0x" + "00" * 12 + "a" * 40,
                        "0x" + "00" * 12 + "b" * 40,
                    ],
                    "data": data_hex,
                },
                {
                    "address": "0xpool",
                    "topics": [
                        EVENT_TOPICS["WithdrawnFromBins"],
                        "0x" + "00" * 12 + "c" * 40,
                        "0x" + "00" * 12 + "d" * 40,
                    ],
                    "data": data_hex,
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.liquidity_result is not None
        assert result.liquidity_result.is_add is True  # Deposit is prioritized
        assert result.liquidity_result.pool_address == "0xpool"
