"""Characterization tests for GimoReceiptParser.

These tests pin CURRENT behavior. They are the regression contract for any future
refactor of the parser. Do not change parser source in this PR.
"""

from decimal import Decimal

import pytest

from almanak.connectors.gimo.receipt_parser import (
    EVENT_TOPICS,
    GIMO_ADDRESSES,
    GimoReceiptParser,
)


# ---------------------------------------------------------------------------
# Hex helpers
# ---------------------------------------------------------------------------


def word(v: int) -> str:
    """One 32-byte ABI word as 64 hex chars (no 0x)."""
    return f"{v:064x}"


def addr_topic(a: str) -> str:
    """Address as an indexed topic (0x-prefixed 32-byte word)."""
    return "0x" + a.lower().replace("0x", "").zfill(64)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TRANSFER_TOPIC = EVENT_TOPICS["Transfer"]
ST0G = GIMO_ADDRESSES["zerog"]["st0g"]
OTHER_TOKEN = "0xCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCc"
USER = "0x1111111111111111111111111111111111111111"
OTHER_USER = "0x2222222222222222222222222222222222222222"
ZERO_ADDR = "0x0000000000000000000000000000000000000000"
TX_HASH = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
BLOCK_NUMBER = 55_000_000


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def parser():
    return GimoReceiptParser(chain="zerog")


def make_transfer_log(
    from_addr: str,
    to_addr: str,
    amount_wei: int,
    token_address: str = ST0G,
) -> dict:
    """ERC-20 Transfer(indexed from, indexed to, uint256 value)."""
    return {
        "address": token_address,
        "topics": [TRANSFER_TOPIC, addr_topic(from_addr), addr_topic(to_addr)],
        "data": "0x" + word(amount_wei),
        "logIndex": 0,
    }


def make_receipt(logs: list) -> dict:
    return {"transactionHash": TX_HASH, "blockNumber": BLOCK_NUMBER, "logs": logs}


# ---------------------------------------------------------------------------
# Stake (mint from zero address)
# ---------------------------------------------------------------------------


class TestStake:
    def test_stake_mint_from_zero_address(self, parser):
        receipt = make_receipt([make_transfer_log(ZERO_ADDR, USER, 15 * 10**17)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 1
        assert result.unstakes == []
        stake = result.stakes[0]
        assert stake.amount == Decimal("1.5")
        assert stake.to_address == USER.lower()
        assert stake.token == ST0G.lower()


# ---------------------------------------------------------------------------
# Unstake (burn to zero address)
# ---------------------------------------------------------------------------


class TestUnstake:
    def test_unstake_burn_to_zero_address(self, parser):
        receipt = make_receipt([make_transfer_log(USER, ZERO_ADDR, 5 * 10**18)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.unstakes) == 1
        assert result.stakes == []
        unstake = result.unstakes[0]
        assert unstake.amount == Decimal("5")
        assert unstake.from_address == USER.lower()
        assert unstake.token == ST0G.lower()


# ---------------------------------------------------------------------------
# Non-st0G token transfers
# ---------------------------------------------------------------------------


class TestNonST0GToken:
    def test_transfer_on_different_token_is_ignored(self, parser):
        """A Transfer on a different token address is not recognized."""
        receipt = make_receipt([make_transfer_log(ZERO_ADDR, USER, 10**18, token_address=OTHER_TOKEN)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.stakes == []
        assert result.unstakes == []

    def test_user_to_user_transfer_on_st0g_is_neither_stake_nor_unstake(self, parser):
        """A user-to-user Transfer on st0G (neither from nor to zero address) is ignored."""
        receipt = make_receipt([make_transfer_log(USER, OTHER_USER, 10**18)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.stakes == []
        assert result.unstakes == []


# ---------------------------------------------------------------------------
# Empty logs
# ---------------------------------------------------------------------------


class TestEmptyLogs:
    def test_empty_logs_returns_success_true_with_empty_lists(self, parser):
        result = parser.parse_receipt(make_receipt([]))

        assert result.success is True
        assert result.stakes == []
        assert result.unstakes == []

    def test_tx_hash_and_block_number_echoed(self, parser):
        result = parser.parse_receipt(make_receipt([]))

        assert result.transaction_hash == TX_HASH
        assert result.block_number == BLOCK_NUMBER


# ---------------------------------------------------------------------------
# Truncated topics
# ---------------------------------------------------------------------------


class TestTruncatedTopics:
    def test_log_with_only_topic0_nothing_recorded(self, parser):
        """_parse_transfer_log requires topics[1] and topics[2].
        A log with only topic0 cannot decode from/to -> nothing is recorded."""
        only_topic0 = {
            "address": ST0G,
            "topics": [TRANSFER_TOPIC],  # missing indexed from/to
            "data": "0x" + word(10**18),
            "logIndex": 0,
        }
        result = parser.parse_receipt(make_receipt([only_topic0]))

        assert result.success is True
        assert result.stakes == []
        assert result.unstakes == []


# ---------------------------------------------------------------------------
# Extraction methods
# ---------------------------------------------------------------------------


class TestExtractionMethods:
    def test_extract_stake_amount_returns_wei_integer(self, parser):
        receipt = make_receipt([make_transfer_log(ZERO_ADDR, USER, 15 * 10**17)])
        # amount is Decimal("1.5"), extract returns int(1.5 * 10**18) = 1_500_000_000_000_000_000
        assert parser.extract_stake_amount(receipt) == 1_500_000_000_000_000_000

    def test_extract_unstake_amount_none_when_no_unstake(self, parser):
        receipt = make_receipt([make_transfer_log(ZERO_ADDR, USER, 15 * 10**17)])
        assert parser.extract_unstake_amount(receipt) is None

    def test_extract_unstake_amount_returns_wei_integer(self, parser):
        receipt = make_receipt([make_transfer_log(USER, ZERO_ADDR, 3 * 10**18)])
        assert parser.extract_unstake_amount(receipt) == 3_000_000_000_000_000_000

    def test_extract_stake_amount_none_when_no_stake(self, parser):
        receipt = make_receipt([make_transfer_log(USER, ZERO_ADDR, 10**18)])
        assert parser.extract_stake_amount(receipt) is None
