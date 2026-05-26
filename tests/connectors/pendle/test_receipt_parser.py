"""
Tests for Pendle Protocol Receipt Parser

These tests verify the receipt parser correctly extracts events
from Pendle transaction receipts.
"""

from decimal import Decimal

import pytest

from almanak.connectors.pendle import (
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
    receiver_pt: str | None = None,
) -> dict:
    """Create a mock Burn (LP removal) event log matching PendleMarketV3 layout.

    Burn(address indexed receiverSy, address indexed receiverPt, uint256 netLpToBurn, uint256 netSyOut, uint256 netPtOut)
    Topics: [hash, receiverSy, receiverPt]   Data: [netLpToBurn, netSyOut, netPtOut]
    """
    receiver_sy_padded = "0x" + receiver.lower().replace("0x", "").zfill(64)
    receiver_pt_padded = "0x" + (receiver_pt or receiver).lower().replace("0x", "").zfill(64)

    lp_hex = hex(net_lp_burned)[2:].zfill(64)
    sy_hex = hex(net_sy_out)[2:].zfill(64)
    pt_hex = hex(net_pt_out)[2:].zfill(64)
    data = "0x" + lp_hex + sy_hex + pt_hex

    return {
        "topics": [
            EVENT_TOPICS["Burn"],
            receiver_sy_padded,
            receiver_pt_padded,
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
        """Parser should handle failed transaction.

        Regression for issue #2064: even when logs=[] (early revert), the
        parser must surface the revert via ``error`` instead of silently
        returning a successful empty-receipt result.
        """
        receipt = create_mock_receipt(status=0)
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        assert result.error == "Transaction reverted"

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
        """Parser should parse Burn event with both V3 indexed receivers."""
        receiver_sy = "0x1234567890123456789012345678901234567890"
        receiver_pt = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        market = "0x08a152834de126d2ef83D612ff36e4523FD0017F"

        net_lp = 10**18
        net_sy = 5 * 10**17
        net_pt = 5 * 10**17

        log = create_burn_log(receiver_sy, net_lp, net_sy, net_pt, market, receiver_pt=receiver_pt)
        receipt = create_mock_receipt(logs=[log])

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.burn_events) == 1

        burn = result.burn_events[0]
        assert burn.receiver_sy.lower() == receiver_sy.lower()
        assert burn.receiver_pt.lower() == receiver_pt.lower()
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
        assert swap_amounts.amount_in == 10**18
        assert swap_amounts.amount_out == 10**18

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


# =============================================================================
# YT Swap Tests (VIB-3751)
# =============================================================================
#
# YT swaps cannot be inferred from the Pendle Market Swap event alone — that
# event reflects an internal flash-mint+sell of PT that the router uses to
# synthesize YT exposure. The user-facing amounts (input token sent to the
# router, YT received) live only in Transfer events. The parser must be
# given the compiler's ``intent_swap_type`` plus token+wallet addresses to
# reconstruct the correct user-facing trade.


class TestYTSwapReconstruction:
    """VIB-3751: ensure YT swaps report user-facing amounts, not the
    Market Swap event's internal flash-mint values."""

    # Canonical sUSDe / YT-sUSDe-7MAY2026 / Pendle market addresses on
    # Ethereum. These map to the failing strategy in QA-Report-April29 B20.
    SUSDE = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
    YT_SUSDE = "0x30775B422b9c7415349855346352FAA61fD97E41"
    MARKET = "0x8dAe8ECe668cf80d348873F23D456448E8694883"
    ROUTER = "0x888888888889758F76e7103c6CbF23ABbF58F946"
    WALLET = "0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF"

    def _build_yt_buy_logs(
        self,
        *,
        sUSDe_in_wei: int,
        yt_out_wei: int,
        # Internal flash-mint values that the Market Swap event misleadingly
        # reports for the SY/PT leg of a YT buy. These are the WRONG values
        # if anyone naively uses the Market Swap event to value the trade.
        internal_pt_amount_wei: int,
        internal_sy_amount_wei: int,
    ) -> list[dict]:
        """Build a logs list mirroring the on-chain shape of a Pendle YT buy."""
        # 1) User -> Router: sends sUSDe (the input)
        user_to_router_susde = create_transfer_log(
            self.WALLET,
            self.ROUTER,
            sUSDe_in_wei,
            self.SUSDE,
            log_index=0,
        )
        # 2) Router internal: SY mint + various PT/YT/SY transfers (we
        # simulate the noisy real receipt — only the user-facing transfers
        # should drive the result).
        router_to_market_sy = create_transfer_log(
            self.ROUTER,
            self.MARKET,
            internal_sy_amount_wei,
            "0x" + "11" * 20,  # SY token (irrelevant address)
            log_index=1,
        )
        # 3) Market Swap event — reports the internal flash-mint+sell of PT.
        # Negative pt_to_account, positive sy_to_account: looks like
        # "PT -> SY" in the legacy reader, NOT the user's YT buy.
        market_swap = create_swap_log(
            self.ROUTER,
            self.ROUTER,
            -internal_pt_amount_wei,
            internal_sy_amount_wei,
            self.MARKET,
            log_index=2,
        )
        # 4) Market -> Router: YT minted to router during flash-mint
        market_to_router_yt = create_transfer_log(
            self.MARKET,
            self.ROUTER,
            yt_out_wei + 100,  # extra YT that gets refunded
            self.YT_SUSDE,
            log_index=3,
        )
        # 5) Router -> User: YT delivered (the user-facing output)
        router_to_user_yt = create_transfer_log(
            self.ROUTER,
            self.WALLET,
            yt_out_wei,
            self.YT_SUSDE,
            log_index=4,
        )
        return [
            user_to_router_susde,
            router_to_market_sy,
            market_swap,
            market_to_router_yt,
            router_to_user_yt,
        ]

    def test_yt_buy_uses_user_facing_amounts(self):
        """A YT buy must report sUSDe-in / YT-out, NOT the internal PT/SY."""
        parser = PendleReceiptParser(
            chain="ethereum",
            token_in_decimals=18,  # sUSDe
            token_out_decimals=18,  # YT-sUSDe
        )
        logs = self._build_yt_buy_logs(
            sUSDe_in_wei=50 * 10**18,
            yt_out_wei=60_971 * 10**18,
            internal_pt_amount_wei=60_898 * 10**18,
            internal_sy_amount_wei=49_476 * 10**18,
        )
        receipt = create_mock_receipt(logs=logs)

        amounts = parser.extract_swap_amounts(
            receipt,
            intent_swap_type="token_to_yt",
            token_in_address=self.SUSDE,
            token_out_address=self.YT_SUSDE,
            wallet_address=self.WALLET,
        )

        assert amounts is not None
        # The bug (VIB-3751) reported amount_in≈60898 (PT flash-mint amount)
        # and amount_out≈49476 (SY internal). With the fix, amount_in must
        # be the 50 sUSDe the user actually sent and amount_out must be the
        # 60971 YT they actually received.
        assert amounts.amount_in_decimal == Decimal("50")
        assert amounts.amount_out_decimal == Decimal("60971")
        assert amounts.token_in == "TOKEN"
        assert amounts.token_out == "YT"

    def test_yt_buy_no_decimals_double_application(self):
        """Decimals must be applied exactly once — NEVER 18 then 18 again."""
        parser = PendleReceiptParser(
            chain="ethereum",
            token_in_decimals=18,
            token_out_decimals=18,
        )
        logs = self._build_yt_buy_logs(
            sUSDe_in_wei=10 * 10**18,
            yt_out_wei=12_345 * 10**18,
            internal_pt_amount_wei=10**24,
            internal_sy_amount_wei=10**24,
        )
        receipt = create_mock_receipt(logs=logs)

        amounts = parser.extract_swap_amounts(
            receipt,
            intent_swap_type="token_to_yt",
            token_in_address=self.SUSDE,
            token_out_address=self.YT_SUSDE,
            wallet_address=self.WALLET,
        )
        assert amounts is not None
        # 10 * 10**18 wei / 10**18 = 10. Anything else (e.g., 10e-18 from
        # double-application, or 10e36 from no-application) is wrong.
        assert amounts.amount_in_decimal == Decimal("10")
        assert amounts.amount_out_decimal == Decimal("12345")

    def test_yt_sell_uses_user_facing_amounts(self):
        """A YT sell must report YT-in / sUSDe-out (mirror of buy path)."""
        parser = PendleReceiptParser(
            chain="ethereum",
            token_in_decimals=18,
            token_out_decimals=18,
        )
        # Sell flow: user sends YT to router, receives sUSDe
        logs = [
            # 1) User -> Router: sends YT
            create_transfer_log(self.WALLET, self.ROUTER, 60_971 * 10**18, self.YT_SUSDE, 0),
            # 2) Internal noise — flash-redeem and Market Swap
            create_swap_log(self.ROUTER, self.ROUTER, 60_898 * 10**18, -49_476 * 10**18, self.MARKET, 1),
            # 3) Router -> User: returns sUSDe
            create_transfer_log(self.ROUTER, self.WALLET, 50 * 10**18, self.SUSDE, 2),
        ]
        receipt = create_mock_receipt(logs=logs)

        amounts = parser.extract_swap_amounts(
            receipt,
            intent_swap_type="yt_to_token",
            token_in_address=self.YT_SUSDE,
            token_out_address=self.SUSDE,
            wallet_address=self.WALLET,
        )
        assert amounts is not None
        assert amounts.amount_in_decimal == Decimal("60971")
        assert amounts.amount_out_decimal == Decimal("50")
        assert amounts.token_in == "YT"
        assert amounts.token_out == "TOKEN"

    def test_yt_swap_falls_back_to_legacy_when_context_missing(self):
        """Without compiler context, the parser falls back to the legacy
        PT-direction inference and emits a WARNING. This is intentional
        back-compat for any caller that hasn't been updated to thread
        intent_swap_type through; the ENRICHER path always supplies it."""
        parser = PendleReceiptParser(
            chain="ethereum",
            token_in_decimals=18,
            token_out_decimals=18,
        )
        logs = self._build_yt_buy_logs(
            sUSDe_in_wei=50 * 10**18,
            yt_out_wei=60_971 * 10**18,
            internal_pt_amount_wei=60_898 * 10**18,
            internal_sy_amount_wei=49_476 * 10**18,
        )
        receipt = create_mock_receipt(logs=logs)

        # No intent_swap_type / addresses passed — legacy path runs.
        amounts = parser.extract_swap_amounts(receipt)
        assert amounts is not None
        # The legacy path produces the misleading internal values. We
        # don't endorse this behavior — the assertion only documents the
        # fallback shape so a future change is visible in tests.
        assert amounts.amount_in_decimal != Decimal("50")

    def test_yt_buy_with_input_refund_uses_net_flow(self):
        """If the router refunds part of the input back to the wallet,
        ``amount_in`` must report the NET flow (sent - refunded), not the
        gross sent amount. Same mirror logic guards ``amount_out`` against
        any output token leaking back from the wallet.
        """
        parser = PendleReceiptParser(
            chain="ethereum",
            token_in_decimals=18,
            token_out_decimals=18,
        )
        # User sends 60 sUSDe; router only spends 50 and refunds 10.
        # User-facing input is the NET 50 (matches the strategy intent).
        logs = [
            create_transfer_log(self.WALLET, self.ROUTER, 60 * 10**18, self.SUSDE, 0),
            create_swap_log(self.ROUTER, self.ROUTER, -(60 * 10**18), 50 * 10**18, self.MARKET, 1),
            create_transfer_log(self.ROUTER, self.WALLET, 10 * 10**18, self.SUSDE, 2),
            create_transfer_log(self.ROUTER, self.WALLET, 60_000 * 10**18, self.YT_SUSDE, 3),
        ]
        receipt = create_mock_receipt(logs=logs)

        amounts = parser.extract_swap_amounts(
            receipt,
            intent_swap_type="token_to_yt",
            token_in_address=self.SUSDE,
            token_out_address=self.YT_SUSDE,
            wallet_address=self.WALLET,
        )
        assert amounts is not None
        # NET sUSDe out = 60 sent - 10 refunded = 50 (user-facing trade)
        assert amounts.amount_in_decimal == Decimal("50")
        assert amounts.amount_out_decimal == Decimal("60000")

    def test_pt_swap_unaffected_by_yt_path(self):
        """PT swaps must continue to work via the SY/PT Swap-event reader."""
        parser = PendleReceiptParser(
            chain="ethereum",
            token_in_decimals=18,
            token_out_decimals=18,
        )
        # PT buy: pt_to_account > 0, sy_to_account < 0
        log = create_swap_log(
            self.WALLET,
            self.WALLET,
            10**18,        # +1 PT
            -(10**18),     # -1 SY
            self.MARKET,
        )
        receipt = create_mock_receipt(logs=[log])

        amounts = parser.extract_swap_amounts(
            receipt,
            intent_swap_type="token_to_pt",
            token_in_address=self.SUSDE,
            token_out_address="0x" + "AB" * 20,
            wallet_address=self.WALLET,
        )
        assert amounts is not None
        # PT path — Swap event drives the values, not Transfer events.
        assert amounts.amount_in_decimal == Decimal("1")
        assert amounts.amount_out_decimal == Decimal("1")

    def test_yt_buy_uses_compiler_decimals_for_non_18_markets(self):
        """For non-18-decimal markets (e.g., Plasma fUSDT0 = 6 decimals),
        the parser MUST honor compiler-supplied decimals. The enricher
        instantiates PendleReceiptParser with chain only, leaving the
        constructor decimals at their 18 fallbacks — without overrides
        the reconstructed YT amounts would be off by 10^12. (Codex P1.)
        """
        # Constructor decimals left at default 18; compiler hands in 6 via kwargs
        parser = PendleReceiptParser(chain="plasma")
        FUSDT0 = "0x1DD4b13fcAE900C60a350589BE8052959D2Ed27B"
        YT_FUSDT0 = "0x7B6aD25E30AB1E7F5393E26C3F6bF1f4e8C0138A"
        MARKET = "0x0cb289E9df2d0dCFe13732638C89655fb80C2bE2"

        # 1.5 fUSDT0 = 1_500_000 wei (6 decimals)
        # 12 YT-fUSDT0 = 12_000_000 wei (6 decimals)
        logs = [
            create_transfer_log(self.WALLET, self.ROUTER, 1_500_000, FUSDT0, 0),
            create_swap_log(self.ROUTER, self.ROUTER, -1_400_000, 1_400_000, MARKET, 1),
            create_transfer_log(self.ROUTER, self.WALLET, 12_000_000, YT_FUSDT0, 2),
        ]
        receipt = create_mock_receipt(logs=logs)

        amounts = parser.extract_swap_amounts(
            receipt,
            intent_swap_type="token_to_yt",
            token_in_address=FUSDT0,
            token_out_address=YT_FUSDT0,
            token_in_decimals=6,
            token_out_decimals=6,
            wallet_address=self.WALLET,
        )
        assert amounts is not None
        # Without the fix, default decimals=18 would report 1.5e-12 fUSDT0.
        assert amounts.amount_in_decimal == Decimal("1.5")
        assert amounts.amount_out_decimal == Decimal("12")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
