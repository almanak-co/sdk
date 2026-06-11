"""Characterization tests for JoeLendReceiptParser.

These tests pin CURRENT behavior. They are the regression contract for any future
refactor of the parser.

The former KNOWN_BUG pins (extract_supply_amount and borrow/withdraw/repay
counterparts returning int(human_scaled_decimal)) were intentionally updated
alongside the parser fix: the hooks now return RAW smallest-unit integers
(token wei), matching the euler_v2 / silo_v2 / benqi convention for the same
ResultEnricher hook. Downstream accounting
(``lending_accounting._select_lending_raw_amount``) expects raw ints and
scales to human units via the token resolver. See TestExtractAmountsRawUnits.
"""

from decimal import Decimal

import pytest

from almanak.connectors.joelend.receipt_parser import (
    EVENT_TOPICS,
    JoeLendEventType,
    JoeLendReceiptParser,
)

# ---------------------------------------------------------------------------
# Hex helpers
# ---------------------------------------------------------------------------


def word(v: int) -> str:
    """One 32-byte ABI word as 64 hex chars (no 0x)."""
    return f"{v:064x}"


def addr_word(a: str) -> str:
    """Address left-padded to a 32-byte word (no 0x)."""
    return a.lower().replace("0x", "").zfill(64)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JTOKEN = "0x2222222222222222222222222222222222222222"
USER = "0x1111111111111111111111111111111111111111"
PAYER = "0x3333333333333333333333333333333333333333"
TX_HASH = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

MINT_TOPIC = EVENT_TOPICS["Mint"]
REDEEM_TOPIC = EVENT_TOPICS["Redeem"]
BORROW_TOPIC = EVENT_TOPICS["Borrow"]
REPAY_TOPIC = EVENT_TOPICS["RepayBorrow"]

UNDERLYING_DECIMALS = 18


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def parser():
    return JoeLendReceiptParser(underlying_decimals=UNDERLYING_DECIMALS)


def make_receipt(logs: list) -> dict:
    return {"transactionHash": TX_HASH, "blockNumber": 12345, "logs": logs}


def make_mint_log(mint_amount_raw: int, mint_tokens_raw: int, address: str = JTOKEN) -> dict:
    """Mint(address minter, uint256 mintAmount, uint256 mintTokens) — 3 data words."""
    return {
        "address": address,
        "topics": [MINT_TOPIC],
        "data": "0x" + addr_word(USER) + word(mint_amount_raw) + word(mint_tokens_raw),
        "logIndex": 0,
    }


def make_redeem_log(redeem_amount_raw: int, redeem_tokens_raw: int, address: str = JTOKEN) -> dict:
    """Redeem(address redeemer, uint256 redeemAmount, uint256 redeemTokens) — 3 data words."""
    return {
        "address": address,
        "topics": [REDEEM_TOPIC],
        "data": "0x" + addr_word(USER) + word(redeem_amount_raw) + word(redeem_tokens_raw),
        "logIndex": 1,
    }


def make_borrow_log(
    borrow_amount_raw: int,
    account_borrows_raw: int = 0,
    total_borrows_raw: int = 0,
    address: str = JTOKEN,
) -> dict:
    """Borrow(address borrower, uint256 borrowAmount, uint256 accountBorrows, uint256 totalBorrows) — 4 data words."""
    return {
        "address": address,
        "topics": [BORROW_TOPIC],
        "data": "0x" + addr_word(USER) + word(borrow_amount_raw) + word(account_borrows_raw) + word(total_borrows_raw),
        "logIndex": 2,
    }


def make_repay_log(
    repay_amount_raw: int,
    account_borrows_raw: int = 0,
    total_borrows_raw: int = 0,
    payer: str = USER,
    address: str = JTOKEN,
) -> dict:
    """RepayBorrow(address payer, address borrower, uint256 repayAmount, uint256 accountBorrows,
    uint256 totalBorrows) — 5 data words."""
    return {
        "address": address,
        "topics": [REPAY_TOPIC],
        "data": (
            "0x"
            + addr_word(payer)
            + addr_word(USER)
            + word(repay_amount_raw)
            + word(account_borrows_raw)
            + word(total_borrows_raw)
        ),
        "logIndex": 3,
    }


# ---------------------------------------------------------------------------
# Mint (supply)
# ---------------------------------------------------------------------------


class TestMint:
    def test_mint_parses_human_string_amounts(self, parser):
        """Mint amounts are stored as human-readable STRINGS in event.data."""
        receipt = make_receipt([make_mint_log(2 * 10**18, 100 * 10**8)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        ev = result.events[0]
        assert ev.event_type == JoeLendEventType.MINT
        assert ev.data["mint_amount"] == "2"
        assert ev.data["mint_tokens"] == "100"

    def test_mint_aggregates_supply_amount_as_decimal(self, parser):
        receipt = make_receipt([make_mint_log(2 * 10**18, 100 * 10**8)])
        result = parser.parse_receipt(receipt)

        assert result.supply_amount == Decimal("2")
        assert result.j_tokens_minted == Decimal("100")


# ---------------------------------------------------------------------------
# Redeem (withdraw)
# ---------------------------------------------------------------------------


class TestRedeem:
    def test_redeem_parses_human_string_amounts(self, parser):
        receipt = make_receipt([make_redeem_log(3 * 10**18, 290 * 10**8)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        ev = result.events[0]
        assert ev.event_type == JoeLendEventType.REDEEM
        assert ev.data["redeem_amount"] == "3"
        assert ev.data["redeem_tokens"] == "290"
        assert result.withdraw_amount == Decimal("3")
        assert result.j_tokens_redeemed == Decimal("290")


# ---------------------------------------------------------------------------
# Borrow
# ---------------------------------------------------------------------------


class TestBorrow:
    def test_borrow_parses_borrow_amount_and_account_borrows(self, parser):
        receipt = make_receipt([make_borrow_log(5 * 10**18, account_borrows_raw=5 * 10**18)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        ev = result.events[0]
        assert ev.event_type == JoeLendEventType.BORROW
        assert ev.data["borrow_amount"] == "5"
        assert ev.data["account_borrows"] == "5"
        assert result.borrow_amount == Decimal("5")


# ---------------------------------------------------------------------------
# RepayBorrow
# ---------------------------------------------------------------------------


class TestRepayBorrow:
    def test_repay_parses_repay_amount_and_payer_borrower(self, parser):
        receipt = make_receipt([make_repay_log(4 * 10**18, payer=PAYER)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        ev = result.events[0]
        assert ev.event_type == JoeLendEventType.REPAY_BORROW
        assert ev.data["repay_amount"] == "4"
        # payer and borrower are decoded from data
        assert ev.data["payer"].lower() == PAYER.lower()
        assert ev.data["borrower"].lower() == USER.lower()
        assert result.repay_amount == Decimal("4")


# ---------------------------------------------------------------------------
# j_token_address filter
# ---------------------------------------------------------------------------


class TestJTokenFilter:
    def test_matching_j_token_address_keeps_event(self, parser):
        receipt = make_receipt([make_mint_log(2 * 10**18, 100 * 10**8, address=JTOKEN)])
        result = parser.parse_receipt(receipt, j_token_address=JTOKEN)

        assert result.success is True
        assert len(result.events) == 1

    def test_non_matching_j_token_address_drops_event_no_retry(self, parser):
        """j_token_address filter: non-matching drops event, success stays True, never retries."""
        other_jtoken = "0x9999999999999999999999999999999999999999"
        receipt = make_receipt([make_mint_log(2 * 10**18, 100 * 10**8, address=JTOKEN)])
        result = parser.parse_receipt(receipt, j_token_address=other_jtoken)

        assert result.success is True
        assert result.events == []
        assert result.supply_amount == Decimal("0")


# ---------------------------------------------------------------------------
# Empty logs
# ---------------------------------------------------------------------------


class TestEmptyLogs:
    def test_empty_logs_returns_success_true(self, parser):
        """Unlike euler_v2/silo_v2, joelend returns success=True on empty logs."""
        result = parser.parse_receipt({"logs": []})

        assert result.success is True
        assert result.events == []
        assert result.supply_amount == Decimal("0")

    def test_missing_logs_key_returns_success_true(self, parser):
        result = parser.parse_receipt({})

        assert result.success is True
        assert result.events == []


# ---------------------------------------------------------------------------
# Malformed / edge cases
# ---------------------------------------------------------------------------


class TestMalformed:
    def test_bytes_topic0_silently_skipped(self, parser):
        """Topic matching uses exact string match (TOPIC_TO_EVENT.get(topic0)).
        A bytes object is not found in the dict -> event is silently skipped."""
        bytes_log = {
            "address": JTOKEN,
            "topics": [bytes.fromhex(MINT_TOPIC[2:])],  # bytes, not str
            "data": "0x" + addr_word(USER) + word(2 * 10**18) + word(100 * 10**8),
            "logIndex": 0,
        }
        receipt = make_receipt([bytes_log])
        result = parser.parse_receipt(receipt)

        assert result.events == []
        assert result.supply_amount == Decimal("0")

    def test_truncated_mint_data_appends_event_with_empty_data_dict(self, parser):
        """Truncated Mint data (< 192 hex chars) causes _decode_event_data to return {}.
        The event is still appended to the list, but with an empty data dict.
        supply_amount stays 0 because data.get('mint_amount', 0) == 0."""
        truncated_log = {
            "address": JTOKEN,
            "topics": [MINT_TOPIC],
            "data": "0x" + word(10**18),  # only 64 hex chars, < 192 required
            "logIndex": 0,
        }
        receipt = make_receipt([truncated_log])
        result = parser.parse_receipt(receipt)

        assert len(result.events) == 1
        assert result.events[0].data == {}
        assert result.supply_amount == Decimal("0")


# ---------------------------------------------------------------------------
# extract_* hooks return RAW smallest units (euler_v2 / silo_v2 / benqi convention)
# ---------------------------------------------------------------------------


class TestExtractAmountsRawUnits:
    def test_extract_supply_amount_returns_raw_smallest_units(self, parser):
        """extract_supply_amount returns the raw Mint ``mintAmount`` (token wei).

        For a Mint of 1.5 tokens (15 * 10**17 raw at 18 decimals), the human
        ParseResult aggregate stays Decimal('1.5'), while the enricher hook
        returns the raw 1_500_000_000_000_000_000 — downstream accounting
        scales raw ints via the token resolver.
        """
        raw_amount = 15 * 10**17
        receipt = make_receipt([make_mint_log(raw_amount, 100 * 10**8)])
        result = parser.parse_receipt(receipt)

        assert result.supply_amount == Decimal("1.5")
        assert parser.extract_supply_amount(receipt) == raw_amount

    def test_extract_borrow_amount_returns_raw_smallest_units(self, parser):
        raw_amount = 25 * 10**17  # 2.5 tokens
        receipt = make_receipt([make_borrow_log(raw_amount)])
        result = parser.parse_receipt(receipt)

        assert result.borrow_amount == Decimal("2.5")
        assert parser.extract_borrow_amount(receipt) == raw_amount

    def test_extract_withdraw_amount_returns_raw_smallest_units(self, parser):
        raw_amount = 35 * 10**17  # 3.5 tokens
        receipt = make_receipt([make_redeem_log(raw_amount, 300 * 10**8)])
        result = parser.parse_receipt(receipt)

        assert result.withdraw_amount == Decimal("3.5")
        assert parser.extract_withdraw_amount(receipt) == raw_amount

    def test_extract_repay_amount_returns_raw_smallest_units(self, parser):
        raw_amount = 45 * 10**17  # 4.5 tokens
        receipt = make_receipt([make_repay_log(raw_amount)])
        result = parser.parse_receipt(receipt)

        assert result.repay_amount == Decimal("4.5")
        assert parser.extract_repay_amount(receipt) == raw_amount

    def test_extract_is_decimals_agnostic(self):
        """The raw extraction must not depend on ``underlying_decimals`` — the
        ResultEnricher constructs parsers without it (only ``chain=`` is
        threaded through), so the default would silently apply to 6-decimal
        underlyings."""
        raw_amount = 15 * 10**5  # 1.5 USDC at 6 decimals
        receipt = make_receipt([make_mint_log(raw_amount, 100 * 10**8)])

        for decimals in (6, 8, 18):
            parser = JoeLendReceiptParser(underlying_decimals=decimals)
            assert parser.extract_supply_amount(receipt) == raw_amount

    def test_extract_sums_multiple_matching_events(self, parser):
        """Two Mint logs in one receipt sum their raw amounts."""
        receipt = make_receipt(
            [
                make_mint_log(1 * 10**18, 50 * 10**8),
                make_mint_log(5 * 10**17, 25 * 10**8),
            ]
        )
        assert parser.extract_supply_amount(receipt) == 15 * 10**17

    def test_extract_returns_none_when_no_matching_event(self, parser):
        """Empty ≠ Zero: a receipt with no matching event is unmeasured (None),
        never a fabricated 0. A Mint-only receipt has no Borrow to extract."""
        receipt = make_receipt([make_mint_log(2 * 10**18, 100 * 10**8)])

        assert parser.extract_borrow_amount(receipt) is None
        assert parser.extract_repay_amount(receipt) is None
        assert parser.extract_withdraw_amount(receipt) is None

    def test_extract_measured_zero_returns_zero(self, parser):
        """A zero-value Mint is a MEASURED zero (0), distinct from None."""
        receipt = make_receipt([make_mint_log(0, 0)])

        assert parser.extract_supply_amount(receipt) == 0

    def test_extract_truncated_event_data_returns_none(self, parser):
        """A matching topic with a too-short data field is a broken receipt —
        the raw-word decode fails closed and the hook returns None (logged),
        rather than under-counting."""
        truncated_log = {
            "address": JTOKEN,
            "topics": [MINT_TOPIC],
            "data": "0x" + word(10**18),  # only 1 word; mintAmount is word 1
            "logIndex": 0,
        }
        receipt = make_receipt([truncated_log])

        assert parser.extract_supply_amount(receipt) is None

    def test_extract_handles_bytes_topics_and_data(self, parser):
        """web3 providers may return topics and ``data`` as bytes/HexBytes
        rather than hex strings; the raw extraction normalizes both forms
        (HexDecoder.normalize_hex) instead of TypeError-ing into None."""
        raw_amount = 15 * 10**17
        str_log = make_mint_log(raw_amount, 100 * 10**8)
        bytes_log = {
            "address": JTOKEN,
            "topics": [bytes.fromhex(MINT_TOPIC[2:])],
            "data": bytes.fromhex(str_log["data"][2:]),
            "logIndex": 0,
        }
        receipt = make_receipt([bytes_log])

        assert parser.extract_supply_amount(receipt) == raw_amount
