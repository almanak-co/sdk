"""
Tests for Pendle Protocol Receipt Parser

These tests verify the receipt parser correctly extracts events
from Pendle transaction receipts.
"""

from decimal import Decimal

import pytest

from almanak.framework.connectors.pendle import (
    EVENT_TOPICS,
    PendleEventType,
    PendleReceiptParser,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def parser():
    """Create parser instance."""
    return PendleReceiptParser(chain="arbitrum")


@pytest.fixture
def parser_with_decimals():
    """Create parser with custom decimals."""
    return PendleReceiptParser(
        chain="arbitrum",
        token_in_decimals=18,
        token_out_decimals=18,
        quoted_price=Decimal("1.05"),
    )


# =============================================================================
# Helper Functions
# =============================================================================


def create_mock_receipt(
    logs: list | None = None,
    status: int = 1,
    tx_hash: str = "0x" + "ab" * 32,
    block_number: int = 12345678,
) -> dict:
    """Create a mock receipt for testing."""
    return {
        "transactionHash": tx_hash,
        "blockNumber": block_number,
        "status": status,
        "logs": logs or [],
        "gasUsed": 200000,
    }


def create_transfer_log(
    from_addr: str,
    to_addr: str,
    value: int,
    token_address: str,
    log_index: int = 0,
) -> dict:
    """Create a mock Transfer event log."""
    # Pad addresses to 32 bytes
    from_padded = "0x" + from_addr.lower().replace("0x", "").zfill(64)
    to_padded = "0x" + to_addr.lower().replace("0x", "").zfill(64)
    value_hex = "0x" + hex(value)[2:].zfill(64)

    return {
        "topics": [
            EVENT_TOPICS["Transfer"],
            from_padded,
            to_padded,
        ],
        "data": value_hex,
        "logIndex": log_index,
        "address": token_address,
    }


def create_swap_log(
    caller: str,
    receiver: str,
    pt_to_account: int,
    sy_to_account: int,
    market_address: str,
    log_index: int = 0,
) -> dict:
    """Create a mock Swap event log."""
    caller_padded = "0x" + caller.lower().replace("0x", "").zfill(64)
    receiver_padded = "0x" + receiver.lower().replace("0x", "").zfill(64)

    # Encode signed integers (int256)
    def encode_int256(val: int) -> str:
        if val >= 0:
            return hex(val)[2:].zfill(64)
        else:
            # Two's complement for negative
            return hex((1 << 256) + val)[2:]

    pt_hex = encode_int256(pt_to_account)
    sy_hex = encode_int256(sy_to_account)
    data = "0x" + pt_hex + sy_hex

    return {
        "topics": [
            EVENT_TOPICS["Swap"],
            caller_padded,
            receiver_padded,
        ],
        "data": data,
        "logIndex": log_index,
        "address": market_address,
    }


def create_mint_log(
    receiver: str,
    net_lp_minted: int,
    net_sy_used: int,
    net_pt_used: int,
    market_address: str,
    log_index: int = 0,
) -> dict:
    """Create a mock Mint (LP) event log."""
    receiver_padded = "0x" + receiver.lower().replace("0x", "").zfill(64)

    lp_hex = hex(net_lp_minted)[2:].zfill(64)
    sy_hex = hex(net_sy_used)[2:].zfill(64)
    pt_hex = hex(net_pt_used)[2:].zfill(64)
    data = "0x" + lp_hex + sy_hex + pt_hex

    return {
        "topics": [
            EVENT_TOPICS["Mint"],
            receiver_padded,
        ],
        "data": data,
        "logIndex": log_index,
        "address": market_address,
    }


def create_burn_log(
    receiver: str,
    net_lp_burned: int,
    net_sy_out: int,
    net_pt_out: int,
    market_address: str,
    log_index: int = 0,
) -> dict:
    """Create a mock Burn (LP removal) event log."""
    receiver_padded = "0x" + receiver.lower().replace("0x", "").zfill(64)

    lp_hex = hex(net_lp_burned)[2:].zfill(64)
    sy_hex = hex(net_sy_out)[2:].zfill(64)
    pt_hex = hex(net_pt_out)[2:].zfill(64)
    data = "0x" + lp_hex + sy_hex + pt_hex

    return {
        "topics": [
            EVENT_TOPICS["Burn"],
            receiver_padded,
        ],
        "data": data,
        "logIndex": log_index,
        "address": market_address,
    }


# =============================================================================
# Basic Parsing Tests
# =============================================================================


class TestBasicParsing:
    """Test basic receipt parsing."""

    def test_parse_empty_receipt(self, parser):
        """Parser should handle empty receipt."""
        receipt = create_mock_receipt(logs=[])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is True
        assert len(result.events) == 0

    def test_parse_failed_transaction(self, parser):
        """Parser should handle failed transaction."""
        receipt = create_mock_receipt(status=0)
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        # Note: When there are no logs, the parser returns early
        # The error message is only set when there are logs but tx failed

    def test_extract_transaction_hash(self, parser):
        """Parser should extract transaction hash."""
        tx_hash = "0x" + "cd" * 32
        receipt = create_mock_receipt(tx_hash=tx_hash)
        result = parser.parse_receipt(receipt)

        assert result.transaction_hash == tx_hash

    def test_extract_block_number(self, parser):
        """Parser should extract block number."""
        block = 99999999
        receipt = create_mock_receipt(block_number=block)
        result = parser.parse_receipt(receipt)

        assert result.block_number == block

    def test_to_dict_conversion(self, parser):
        """Parser result should convert to dict."""
        receipt = create_mock_receipt()
        result = parser.parse_receipt(receipt)

        result_dict = result.to_dict()
        assert "success" in result_dict
        assert "events" in result_dict
        assert "transaction_hash" in result_dict


# =============================================================================
# Transfer Event Tests
# =============================================================================


class TestTransferEventParsing:
    """Test Transfer event parsing."""

    def test_parse_single_transfer(self, parser):
        """Parser should parse single Transfer event."""
        from_addr = "0x1234567890123456789012345678901234567890"
        to_addr = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        value = 10**18
        token = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"

        log = create_transfer_log(from_addr, to_addr, value, token)
        receipt = create_mock_receipt(logs=[log])

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.transfer_events) == 1

        transfer = result.transfer_events[0]
        assert transfer.from_addr.lower() == from_addr.lower()
        assert transfer.to_addr.lower() == to_addr.lower()
        assert transfer.value == value
        assert transfer.token_address.lower() == token.lower()

    def test_parse_multiple_transfers(self, parser):
        """Parser should parse multiple Transfer events."""
        logs = [
            create_transfer_log(
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
                10**18,
                "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                log_index=0,
            ),
            create_transfer_log(
                "0x3333333333333333333333333333333333333333",
                "0x4444444444444444444444444444444444444444",
                5 * 10**17,
                "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                log_index=1,
            ),
        ]
        receipt = create_mock_receipt(logs=logs)

        result = parser.parse_receipt(receipt)

        assert len(result.transfer_events) == 2
        assert result.transfer_events[0].value == 10**18
        assert result.transfer_events[1].value == 5 * 10**17


# =============================================================================
# Swap Event Tests
# =============================================================================


class TestSwapEventParsing:
    """Test Swap event parsing."""

    def test_parse_buy_pt_swap(self, parser):
        """Parser should parse buy PT swap (SY -> PT)."""
        caller = "0x1234567890123456789012345678901234567890"
        receiver = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        market = "0x08a152834de126d2ef83D612ff36e4523FD0017F"

        # Positive PT means buying PT
        pt_to_account = 10**18  # Received 1 PT
        sy_to_account = -(10**18)  # Spent 1 SY (negative)

        log = create_swap_log(caller, receiver, pt_to_account, sy_to_account, market)
        receipt = create_mock_receipt(logs=[log])

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.swap_events) == 1

        swap = result.swap_events[0]
        assert swap.is_buy_pt is True
        assert swap.is_sell_pt is False
        assert swap.pt_amount == 10**18
        assert swap.sy_amount == 10**18

    def test_parse_sell_pt_swap(self, parser):
        """Parser should parse sell PT swap (PT -> SY)."""
        caller = "0x1234567890123456789012345678901234567890"
        receiver = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        market = "0x08a152834de126d2ef83D612ff36e4523FD0017F"

        # Negative PT means selling PT
        pt_to_account = -(10**18)  # Spent 1 PT (negative)
        sy_to_account = 10**18  # Received 1 SY

        log = create_swap_log(caller, receiver, pt_to_account, sy_to_account, market)
        receipt = create_mock_receipt(logs=[log])

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.swap_events) == 1

        swap = result.swap_events[0]
        assert swap.is_buy_pt is False
        assert swap.is_sell_pt is True

    def test_build_swap_result(self, parser_with_decimals):
        """Parser should build high-level swap result."""
        caller = "0x1234567890123456789012345678901234567890"
        receiver = caller
        market = "0x08a152834de126d2ef83D612ff36e4523FD0017F"

        pt_to_account = 10**18
        sy_to_account = -(10**18)

        log = create_swap_log(caller, receiver, pt_to_account, sy_to_account, market)
        receipt = create_mock_receipt(logs=[log])

        result = parser_with_decimals.parse_receipt(receipt)

        assert result.swap_result is not None
        assert result.swap_result.swap_type == "buy_pt"
        assert result.swap_result.amount_in == 10**18
        assert result.swap_result.amount_out == 10**18
        assert result.swap_result.market_address == market.lower()


# =============================================================================
# Mint/Burn Event Tests
# =============================================================================


class TestMintEventParsing:
    """Test Mint (LP add) event parsing."""

    def test_parse_mint_event(self, parser):
        """Parser should parse Mint event."""
        receiver = "0x1234567890123456789012345678901234567890"
        market = "0x08a152834de126d2ef83D612ff36e4523FD0017F"

        net_lp = 10**18
        net_sy = 5 * 10**17
        net_pt = 5 * 10**17

        log = create_mint_log(receiver, net_lp, net_sy, net_pt, market)
        receipt = create_mock_receipt(logs=[log])

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.mint_events) == 1

        mint = result.mint_events[0]
        assert mint.net_lp_minted == net_lp
        assert mint.net_sy_used == net_sy
        assert mint.net_pt_used == net_pt
        assert mint.receiver.lower() == receiver.lower()


class TestBurnEventParsing:
    """Test Burn (LP remove) event parsing."""

    def test_parse_burn_event(self, parser):
        """Parser should parse Burn event."""
        receiver = "0x1234567890123456789012345678901234567890"
        market = "0x08a152834de126d2ef83D612ff36e4523FD0017F"

        net_lp = 10**18
        net_sy = 5 * 10**17
        net_pt = 5 * 10**17

        log = create_burn_log(receiver, net_lp, net_sy, net_pt, market)
        receipt = create_mock_receipt(logs=[log])

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.burn_events) == 1

        burn = result.burn_events[0]
        assert burn.net_lp_burned == net_lp
        assert burn.net_sy_out == net_sy
        assert burn.net_pt_out == net_pt


# =============================================================================
# Extraction Method Tests
# =============================================================================


class TestExtractionMethods:
    """Test extraction methods for Result Enrichment."""

    def test_extract_swap_amounts(self, parser):
        """Test swap amounts extraction."""
        caller = "0x1234567890123456789012345678901234567890"
        receiver = caller
        market = "0x08a152834de126d2ef83D612ff36e4523FD0017F"

        pt_to_account = 10**18
        sy_to_account = -(10**18)

        log = create_swap_log(caller, receiver, pt_to_account, sy_to_account, market)
        receipt = create_mock_receipt(logs=[log])

        swap_amounts = parser.extract_swap_amounts(receipt)

        assert swap_amounts is not None
        assert swap_amounts["amount_in"] == 10**18
        assert swap_amounts["amount_out"] == 10**18

    def test_extract_lp_minted(self, parser):
        """Test LP minted extraction."""
        receiver = "0x1234567890123456789012345678901234567890"
        market = "0x08a152834de126d2ef83D612ff36e4523FD0017F"

        net_lp = 12345 * 10**14

        log = create_mint_log(receiver, net_lp, 10**18, 10**18, market)
        receipt = create_mock_receipt(logs=[log])

        lp_minted = parser.extract_lp_minted(receipt)

        assert lp_minted == net_lp

    def test_extract_lp_burned(self, parser):
        """Test LP burned extraction."""
        receiver = "0x1234567890123456789012345678901234567890"
        market = "0x08a152834de126d2ef83D612ff36e4523FD0017F"

        net_lp = 98765 * 10**14

        log = create_burn_log(receiver, net_lp, 10**18, 10**18, market)
        receipt = create_mock_receipt(logs=[log])

        lp_burned = parser.extract_lp_burned(receipt)

        assert lp_burned == net_lp

    def test_extraction_returns_none_for_missing_event(self, parser):
        """Extraction methods return None when event not found."""
        receipt = create_mock_receipt(logs=[])

        assert parser.extract_swap_amounts(receipt) is None
        assert parser.extract_lp_minted(receipt) is None
        assert parser.extract_lp_burned(receipt) is None


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_unknown_event_ignored(self, parser):
        """Parser should ignore unknown events."""
        unknown_log = {
            "topics": ["0x" + "00" * 32],  # Unknown topic
            "data": "0x",
            "logIndex": 0,
            "address": "0x1234567890123456789012345678901234567890",
        }
        receipt = create_mock_receipt(logs=[unknown_log])

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0

    def test_malformed_log_handled(self, parser):
        """Parser should handle malformed logs gracefully."""
        malformed_log = {
            "topics": [],  # No topics
            "data": "0x",
            "logIndex": 0,
            "address": "0x1234567890123456789012345678901234567890",
        }
        receipt = create_mock_receipt(logs=[malformed_log])

        result = parser.parse_receipt(receipt)

        assert result.success is True  # Should not crash

    def test_bytes_transaction_hash(self, parser):
        """Parser should handle bytes transaction hash."""
        tx_hash_bytes = bytes.fromhex("ab" * 32)
        receipt = {
            "transactionHash": tx_hash_bytes,
            "blockNumber": 12345,
            "status": 1,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash == "0x" + "ab" * 32


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
