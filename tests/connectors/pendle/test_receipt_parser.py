"""
Tests for Pendle Protocol Receipt Parser

These tests verify the receipt parser correctly extracts events
from Pendle transaction receipts.
"""

from decimal import Decimal

import pytest

from almanak.connectors.pendle import (
    EVENT_TOPICS,
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
            10**18,  # +1 PT
            -(10**18),  # -1 SY
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

    # -------------------------------------------------------------------------
    # VIB-5301: YT swap reconstruction must NOT require a PendleMarket ``Swap``
    # event to be present.
    #
    # A YT trade's user-facing amounts live ONLY in Transfer events (input token
    # leaving the wallet, YT arriving — or the reverse on a sell). The internal
    # ``Swap`` event is the router's flash-mint of PT and is *not* a faithful
    # representation of the user trade. On real receipts that ``Swap`` event is
    # often absent entirely: limit-order fills and markets whose AMM curve is
    # never touched produce a successful YT buy with NO ``Swap`` log (the report
    # market 0x8dAe…883 emitted zero AMM Swap events across millions of blocks on
    # mainnet, yet YT was delivered to the wallet). The prior code gated the
    # entire swap_result behind ``if swap_events:`` so those receipts silently
    # lost the YT output amount, breaking ``amount="all"`` chaining (the runner
    # reads ``swap_amounts.amount_out_decimal``) and accounting.
    #
    # Empty != Zero: when the amounts genuinely cannot be reconstructed the
    # parser must still return None (unmeasured), never Decimal("0").
    # -------------------------------------------------------------------------

    def test_yt_entry_no_swap_event_reconstructs_from_transfers(self):
        """YT BUY receipt with input + YT Transfers but NO Swap event must
        still report the user-facing sUSDe-in / YT-out (VIB-5301).

        Before the fix this returned None ("no output amount extracted"),
        breaking amount='all' chaining into the exit swap.
        """
        parser = PendleReceiptParser(
            chain="ethereum",
            token_in_decimals=18,
            token_out_decimals=18,
        )
        # Real YT-entry shape WITHOUT an AMM Swap event: user sends sUSDe,
        # receives YT. No create_swap_log at all.
        logs = [
            create_transfer_log(self.WALLET, self.ROUTER, 50 * 10**18, self.SUSDE, 0),
            create_transfer_log(self.ROUTER, self.WALLET, 60_971 * 10**18, self.YT_SUSDE, 1),
        ]
        receipt = create_mock_receipt(logs=logs)

        amounts = parser.extract_swap_amounts(
            receipt,
            intent_swap_type="token_to_yt",
            token_in_address=self.SUSDE,
            token_out_address=self.YT_SUSDE,
            wallet_address=self.WALLET,
        )
        assert amounts is not None, "YT entry without a Swap event must still reconstruct from Transfers"
        assert amounts.amount_in_decimal == Decimal("50")
        assert amounts.amount_out_decimal == Decimal("60971")
        assert amounts.token_out == "YT"

    def test_yt_exit_no_swap_event_reconstructs_from_transfers(self):
        """YT SELL receipt with YT-in + token-out Transfers but NO Swap event
        must report the user-facing YT-in / sUSDe-out (VIB-5301 mirror)."""
        parser = PendleReceiptParser(
            chain="ethereum",
            token_in_decimals=18,
            token_out_decimals=18,
        )
        logs = [
            create_transfer_log(self.WALLET, self.ROUTER, 60_971 * 10**18, self.YT_SUSDE, 0),
            create_transfer_log(self.ROUTER, self.WALLET, 49 * 10**18, self.SUSDE, 1),
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
        assert amounts.amount_out_decimal == Decimal("49")
        assert amounts.token_in == "YT"

    def test_yt_entry_no_swap_event_unmeasured_when_yt_transfer_missing(self):
        """Empty != Zero: a YT entry with NO Swap event AND no matching YT
        Transfer to the wallet must return None (unmeasured), never a coerced
        Decimal('0'). Guards against the fix silently fabricating a zero
        output amount that would corrupt amount='all' chaining."""
        parser = PendleReceiptParser(
            chain="ethereum",
            token_in_decimals=18,
            token_out_decimals=18,
        )
        # Input leaves the wallet but the YT never arrives (truncated receipt).
        logs = [
            create_transfer_log(self.WALLET, self.ROUTER, 50 * 10**18, self.SUSDE, 0),
        ]
        receipt = create_mock_receipt(logs=logs)

        amounts = parser.extract_swap_amounts(
            receipt,
            intent_swap_type="token_to_yt",
            token_in_address=self.SUSDE,
            token_out_address=self.YT_SUSDE,
            wallet_address=self.WALLET,
        )
        assert amounts is None


class TestYTEntryEnricherSeam:
    """VIB-5301: prove the FULL seam end-to-end — the compiler's ActionBundle
    metadata for a ``token_to_yt`` swap, threaded through the framework
    ResultEnricher's ``build_extract_kwargs`` contract, reaches the parser and
    reconstructs the YT output amount from Transfer events on a receipt that has
    NO PendleMarket ``Swap`` event.

    This guards against an *inert* fix: the parser change is only meaningful if
    the enricher actually forwards ``intent_swap_type`` + token/wallet addresses
    to ``extract_swap_amounts``. The runner reads the resulting
    ``swap_amounts.amount_out_decimal`` for ``amount="all"`` chaining.
    """

    SUSDE = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
    YT_SUSDE = "0x30775B422b9c7415349855346352FAA61fD97E41"
    MARKET = "0x8dAe8ECe668cf80d348873F23D456448E8694883"
    ROUTER = "0x888888888889758F76e7103c6CbF23ABbF58F946"
    WALLET = "0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF"

    def test_yt_entry_seam_no_swap_event(self):
        from almanak.framework.execution.result_enricher import ResultEnricher

        # Mirrors compiler.py ``compile_pendle_swap`` ActionBundle.metadata for
        # a token_to_yt swap (from_token.to_dict(), to_token_address,
        # to_token_decimals, swap_type, wallet_address).
        bundle_metadata = {
            "from_token": {
                "symbol": "sUSDe",
                "address": self.SUSDE,
                "decimals": 18,
                "is_native": False,
            },
            "to_token": "YT-sUSDe-7MAY2026",
            "to_token_address": self.YT_SUSDE,
            "to_token_decimals": 18,
            "amount_in": str(50 * 10**18),
            "swap_type": "token_to_yt",
            "wallet_address": self.WALLET,
            "market": self.MARKET,
        }

        # Real YT-entry receipt shape with NO AMM Swap event.
        logs = [
            create_transfer_log(self.WALLET, self.ROUTER, 50 * 10**18, self.SUSDE, 0),
            create_transfer_log(self.ROUTER, self.WALLET, 60_971 * 10**18, self.YT_SUSDE, 1),
        ]
        receipt = create_mock_receipt(logs=logs)

        parser = PendleReceiptParser(chain="ethereum")
        enricher = ResultEnricher()
        kwargs = enricher._build_extract_kwargs_for_parser(parser, "swap_amounts", bundle_metadata)

        # The enricher must have derived the YT reconstruction context.
        assert kwargs.get("intent_swap_type") == "token_to_yt"
        assert kwargs.get("token_in_address") == self.SUSDE
        assert kwargs.get("token_out_address") == self.YT_SUSDE
        assert kwargs.get("wallet_address") == self.WALLET

        amounts = parser.extract_swap_amounts(receipt, **kwargs)
        assert amounts is not None, "full enricher seam must reconstruct YT entry without a Swap event"
        assert amounts.amount_out_decimal == Decimal("60971")
        assert amounts.amount_in_decimal == Decimal("50")


class TestPTSwapSymbolResolution:
    """G-PT0: PT swaps must stamp the FULL maturity-bearing PT symbol.

    The Pendle Market ``Swap`` event carries no symbol, so the legacy parser
    used generic ``"PT"`` / ``"SY"`` placeholders. Those flow to
    ``transaction_ledger.token_in`` / ``token_out`` and the accounting
    categorizer never claimed the trade (it needs ``token_out.startswith("PT-")``
    plus a parseable maturity). The parser now resolves the canonical
    maturity-bearing symbol from the compiler-supplied PT token address.
    """

    # Arbitrum PT-wstETH-25JUN2026 (PT token addr) + WSTETH underlying.
    PT_WSTETH = "0x71fBF40651E9D4278a74586AfC99F307f369Ce9A"
    WSTETH = "0x5979D7b546E38E414F7E9822514be443A4800529"
    MARKET = "0xf78452e0f5C0B95fc5dC8353B8CD1e06E53fa25B"
    WALLET = "0x1234567890123456789012345678901234567890"

    def _parser(self):
        return PendleReceiptParser(chain="arbitrum", token_in_decimals=18, token_out_decimals=18)

    def _buy_receipt(self):
        # buy_pt: Swap event is positive pt_to_account, negative sy_to_account.
        log = create_swap_log(self.WALLET, self.WALLET, 10**18, -(10**18), self.MARKET)
        return create_mock_receipt(logs=[log])

    def _sell_receipt(self):
        # sell_pt: Swap event is negative pt_to_account, positive sy_to_account.
        log = create_swap_log(self.WALLET, self.WALLET, -(10**18), 10**18, self.MARKET)
        return create_mock_receipt(logs=[log])

    def test_buy_pt_token_out_carries_full_symbol(self):
        """A PT buy must report token_out = the maturity-bearing PT symbol."""
        amounts = self._parser().extract_swap_amounts(
            self._buy_receipt(),
            intent_swap_type="token_to_pt",
            token_in_address=self.WSTETH,
            token_out_address=self.PT_WSTETH,
            wallet_address=self.WALLET,
        )
        assert amounts is not None
        assert amounts.token_out == "PT-wstETH-25JUN2026"
        assert amounts.token_out.startswith("PT-")
        assert amounts.token_in == "WSTETH"
        # Amounts (raw-18) are untouched by the symbol fix.
        assert amounts.amount_in == 10**18
        assert amounts.amount_out == 10**18

    def test_sell_pt_token_in_carries_full_symbol(self):
        """A PT sell must report token_in = the maturity-bearing PT symbol."""
        amounts = self._parser().extract_swap_amounts(
            self._sell_receipt(),
            intent_swap_type="pt_to_token",
            token_in_address=self.PT_WSTETH,
            token_out_address=self.WSTETH,
            wallet_address=self.WALLET,
        )
        assert amounts is not None
        assert amounts.token_in == "PT-wstETH-25JUN2026"
        assert amounts.token_in.startswith("PT-")
        assert amounts.token_out == "WSTETH"

    def test_full_symbol_parses_maturity(self):
        """The resolved symbol must feed _parse_pt_maturity (the whole point)."""
        from almanak.connectors.pendle.accounting_spec import _parse_pt_maturity

        amounts = self._parser().extract_swap_amounts(
            self._buy_receipt(),
            intent_swap_type="token_to_pt",
            token_in_address=self.WSTETH,
            token_out_address=self.PT_WSTETH,
            wallet_address=self.WALLET,
        )
        maturity = _parse_pt_maturity(amounts.token_out)
        assert maturity is not None
        assert (maturity.year, maturity.month, maturity.day) == (2026, 6, 25)

    def test_unknown_pt_address_degrades_to_generic_label(self):
        """Empty != Zero: an unknown PT address degrades to "PT", never guessed."""
        amounts = self._parser().extract_swap_amounts(
            self._buy_receipt(),
            intent_swap_type="token_to_pt",
            token_in_address=self.WSTETH,
            token_out_address="0x" + "de" * 20,  # not in PT_TOKEN_INFO
            wallet_address=self.WALLET,
        )
        assert amounts is not None
        assert amounts.token_out == "PT"

    def test_missing_pt_address_degrades_to_generic_label(self):
        """No compiler address supplied -> generic labels (legacy behaviour)."""
        amounts = self._parser().extract_swap_amounts(self._buy_receipt())
        assert amounts is not None
        assert amounts.token_out == "PT"
        assert amounts.token_in == "SY"


class TestPendleRedeemMoneyLegs:
    """G-PT (VIB-4988 part 2): a PT redeem (WITHDRAW) declares its money legs as a
    typed ``PrimitiveMoneyLegs`` (INPUT=canonical PT symbol resolved from the
    compiler-supplied ``pt_address``, OUTPUT=underlying) so the ledger row carries
    the maturity-bearing PT symbol on ``token_in`` instead of the lending guess.

    A redeem emits ``RedeemPY`` / ``RedeemSY``, never a Market ``Swap``, so the
    swap-path PT-symbol resolution never fires — these tests pin the redeem path.
    """

    # Arbitrum PT-wstETH-25JUN2026 (PT token addr) + WSTETH underlying + SY/YT.
    PT_WSTETH = "0x71fBF40651E9D4278a74586AfC99F307f369Ce9A"
    WSTETH = "0x5979D7b546E38E414F7E9822514be443A4800529"
    SY_ADDR = "0x" + "5a" * 20
    YT_ADDR = "0x25bda1edd6af17c61399aa0eb84b93daa3069764"
    WALLET = "0x1234567890123456789012345678901234567890"

    def _parser(self):
        return PendleReceiptParser(chain="arbitrum", token_in_decimals=18, token_out_decimals=18)

    @staticmethod
    def _topic_addr(addr: str) -> str:
        return "0x" + addr.lower().replace("0x", "").zfill(64)

    def _redeem_sy_log(self, amount_sy: int, amount_token_out: int) -> dict:
        # Redeem(caller, receiver, tokenOut indexed; amountSyToRedeem, amountTokenOut)
        data = "0x" + hex(amount_sy)[2:].zfill(64) + hex(amount_token_out)[2:].zfill(64)
        return {
            "topics": [
                EVENT_TOPICS["RedeemSY"],
                self._topic_addr(self.WALLET),
                self._topic_addr(self.WALLET),
                self._topic_addr(self.WSTETH),
            ],
            "data": data,
            "logIndex": 0,
            "address": self.SY_ADDR,
        }

    def _redeem_py_log(self, net_py: int, net_sy: int) -> dict:
        # RedeemPY(caller, receiver indexed; netPYRedeemed, netSYRedeemed); emitter=YT
        data = "0x" + hex(net_py)[2:].zfill(64) + hex(net_sy)[2:].zfill(64)
        return {
            "topics": [
                EVENT_TOPICS["RedeemPY"],
                self._topic_addr(self.WALLET),
                self._topic_addr(self.WALLET),
            ],
            "data": data,
            "logIndex": 0,
            "address": self.YT_ADDR,
        }

    def _pt_transfer_log(self, from_addr: str, to_addr: str, value: int, token_address: str | None = None) -> dict:
        # ERC20 Transfer(from indexed, to indexed; value) on the PT token.
        return {
            "topics": [
                EVENT_TOPICS["Transfer"],
                self._topic_addr(from_addr),
                self._topic_addr(to_addr),
            ],
            "data": "0x" + hex(value)[2:].zfill(64),
            "logIndex": 0,
            "address": token_address or self.PT_WSTETH,
        }

    # Captured real-fork basis (pendle_redeem_roundtrip_20260620.db): the PT_BUY
    # bought 0.012378419794380337 PT; the redeem burned the FULL PT balance, but
    # the SY ``Redeem`` reported 0.010002774988900641 SY-asset (PT × SY-rate).
    # PEN6 requires the INPUT leg = the PT COUNT (0.012378), NOT the SY amount.
    PT_COUNT_RAW = 12378419794380337  # 0.012378419794380337 PT (18 dec)
    SY_OUT_RAW = 10002774988900641  # 0.010002774988900641 wstETH out (18 dec)
    BURN = "0x0000000000000000000000000000000000000000"

    def test_post_maturity_redeem_pt_count_from_transfer_not_sy(self):
        """PEN6 basis: at/after maturity (RedeemSY only, NO RedeemPY) the PT INPUT
        leg MUST be the PT TOKEN COUNT from the PT ``Transfer`` — NOT the
        SY-asset ``amount_sy_to_redeem`` (the basis bug). OUTPUT = the measured
        underlying ``amount_token_out``.

        Uses the captured real-fork values so the SY≠PT-count mismatch is exercised
        (0.012378 PT burned vs 0.010002 wstETH out)."""
        receipt = create_mock_receipt(
            logs=[
                # PT burned: wallet -> 0x0 (full balance), token == pt_address.
                self._pt_transfer_log(self.WALLET, self.BURN, self.PT_COUNT_RAW),
                self._redeem_sy_log(self.SY_OUT_RAW, self.SY_OUT_RAW),
            ]
        )
        legs = self._parser().extract_primitive_money_legs(
            receipt,
            pt_address=self.PT_WSTETH,
            out_token_symbol="WSTETH",
            out_token_address=self.WSTETH,
            out_token_decimals=18,
        )
        assert legs is not None
        inputs = legs.input_legs
        outputs = legs.output_legs
        assert len(inputs) == 1
        assert len(outputs) == 1
        assert inputs[0].token == "PT-wstETH-25JUN2026"
        assert inputs[0].amount.is_measured
        # PT count, basis-identical to the PT_BUY's amount_out — NOT the SY amount.
        assert inputs[0].amount.value == Decimal("0.012378419794380337")
        assert inputs[0].amount.value != Decimal("0.010002774988900641")
        # OUTPUT = underlying received (the SY Redeem's amount_token_out).
        assert outputs[0].token == "WSTETH"
        assert outputs[0].amount.is_measured
        assert outputs[0].amount.value == Decimal("0.010002774988900641")

    def test_pre_maturity_redeem_uses_redeempy_pt_count(self):
        """Pre-maturity redeem: PT count from ``RedeemPY.net_py_redeemed`` (the PT
        count) — preferred over any PT Transfer; underlying from RedeemSY's
        ``amount_token_out``. RedeemPY's PT count is distinct from the SY amounts."""
        receipt = create_mock_receipt(
            logs=[
                self._redeem_py_log(self.PT_COUNT_RAW, self.SY_OUT_RAW),
                self._redeem_sy_log(self.SY_OUT_RAW, 21 * 10**17),
            ]
        )
        legs = self._parser().extract_primitive_money_legs(
            receipt,
            pt_address=self.PT_WSTETH,
            out_token_symbol="WSTETH",
            out_token_decimals=18,
        )
        assert legs is not None
        assert legs.input_legs[0].token == "PT-wstETH-25JUN2026"
        assert legs.input_legs[0].amount.value == Decimal("0.012378419794380337")
        assert legs.output_legs[0].token == "WSTETH"
        assert legs.output_legs[0].amount.value == Decimal("2.1")

    def test_pt_transfer_filtered_by_token_address(self):
        """A Transfer of a DIFFERENT token (e.g. the underlying out) must NOT be
        mistaken for the PT count — only ``token_address == pt_address`` counts."""
        receipt = create_mock_receipt(
            logs=[
                # Underlying transfer (router -> wallet) on a NON-PT token: must be ignored.
                self._pt_transfer_log(self.SY_ADDR, self.WALLET, self.SY_OUT_RAW, token_address=self.WSTETH),
                # The real PT burn.
                self._pt_transfer_log(self.WALLET, self.BURN, self.PT_COUNT_RAW),
                self._redeem_sy_log(self.SY_OUT_RAW, self.SY_OUT_RAW),
            ]
        )
        legs = self._parser().extract_primitive_money_legs(
            receipt, pt_address=self.PT_WSTETH, out_token_symbol="WSTETH"
        )
        assert legs is not None
        assert legs.input_legs[0].amount.value == Decimal("0.012378419794380337")

    def test_post_maturity_no_pt_transfer_degrades_not_sy(self):
        """At/after maturity with NO RedeemPY and NO PT Transfer, the PT count is
        unresolvable — return None (degrade) rather than booking the SY-asset
        amount as the PT count (Empty != Zero, the PEN6 guard)."""
        receipt = create_mock_receipt(logs=[self._redeem_sy_log(self.SY_OUT_RAW, self.SY_OUT_RAW)])
        legs = self._parser().extract_primitive_money_legs(
            receipt, pt_address=self.PT_WSTETH, out_token_symbol="WSTETH"
        )
        assert legs is None

    def test_unknown_pt_address_returns_none_no_fabrication(self):
        """An unknown catalogue PT address resolves no symbol -> return None so the
        dispatcher falls back (Empty != Zero: never fabricate a PT symbol)."""
        receipt = create_mock_receipt(
            logs=[
                self._pt_transfer_log(self.WALLET, self.BURN, self.PT_COUNT_RAW, token_address="0x" + "de" * 20),
                self._redeem_sy_log(self.SY_OUT_RAW, self.SY_OUT_RAW),
            ]
        )
        legs = self._parser().extract_primitive_money_legs(
            receipt,
            pt_address="0x" + "de" * 20,  # not in PT_TOKEN_INFO
            out_token_symbol="WSTETH",
        )
        assert legs is None

    def test_no_pt_address_returns_none(self):
        """No compiler ``pt_address`` threaded -> no PT symbol -> None (legacy path)."""
        receipt = create_mock_receipt(
            logs=[
                self._pt_transfer_log(self.WALLET, self.BURN, self.PT_COUNT_RAW),
                self._redeem_sy_log(self.SY_OUT_RAW, self.SY_OUT_RAW),
            ]
        )
        assert self._parser().extract_primitive_money_legs(receipt, out_token_symbol="WSTETH") is None

    def test_missing_token_out_yields_unmeasured_output(self):
        """No measured ``amount_token_out`` (degenerate receipt) -> OUTPUT amount is
        UNMEASURED, never a measured zero or an SY proxy (Empty != Zero). The PT
        count is still measured from the PT Transfer."""
        receipt = create_mock_receipt(
            logs=[
                self._pt_transfer_log(self.WALLET, self.BURN, self.PT_COUNT_RAW),
                self._redeem_sy_log(self.SY_OUT_RAW, 0),
            ]
        )
        legs = self._parser().extract_primitive_money_legs(
            receipt, pt_address=self.PT_WSTETH, out_token_symbol="WSTETH"
        )
        assert legs is not None
        assert legs.input_legs[0].amount.is_measured  # PT count still known
        assert legs.input_legs[0].amount.value == Decimal("0.012378419794380337")
        assert not legs.output_legs[0].amount.is_measured  # underlying unmeasured

    def test_no_redeem_event_no_pt_transfer_returns_none(self):
        """A receipt with no redeem event and no PT transfer is not a redeem -> None."""
        receipt = create_mock_receipt(logs=[])
        assert (
            self._parser().extract_primitive_money_legs(receipt, pt_address=self.PT_WSTETH, out_token_symbol="WSTETH")
            is None
        )

    def test_build_extract_kwargs_threads_redeem_context(self):
        """``build_extract_kwargs(field="primitive_money_legs")`` threads the
        compiler's redeem metadata (pt_address + out_token descriptor) into the
        extractor kwargs."""
        bundle_metadata = {
            "protocol": "pendle",
            "yt_address": self.YT_ADDR,
            "pt_address": self.PT_WSTETH,
            "out_token": {"symbol": "WSTETH", "address": self.WSTETH, "decimals": 18},
            "py_amount": str(10**18),
        }
        kwargs = self._parser().build_extract_kwargs(
            field="primitive_money_legs", bundle_metadata=bundle_metadata
        )
        assert kwargs["pt_address"] == self.PT_WSTETH
        assert kwargs["out_token_symbol"] == "WSTETH"
        assert kwargs["out_token_address"] == self.WSTETH
        assert kwargs["out_token_decimals"] == 18

    def test_build_extract_kwargs_redeem_missing_keys_degrade(self):
        """Missing redeem-metadata keys degrade (omitted from kwargs) rather than
        crashing — the extractor then returns None (Empty != Zero)."""
        # No pt_address / out_token at all → empty kwargs.
        assert self._parser().build_extract_kwargs(field="primitive_money_legs", bundle_metadata={}) == {}
        # Non-int decimals → out_token_decimals omitted, others still threaded.
        kwargs = self._parser().build_extract_kwargs(
            field="primitive_money_legs",
            bundle_metadata={"pt_address": self.PT_WSTETH, "out_token": {"symbol": "WSTETH", "decimals": "oops"}},
        )
        assert kwargs["pt_address"] == self.PT_WSTETH
        assert kwargs["out_token_symbol"] == "WSTETH"
        assert "out_token_decimals" not in kwargs

    def test_build_extract_kwargs_swap_field_unaffected(self):
        """The redeem branch must not change the swap_amounts kwargs contract."""
        kwargs = self._parser().build_extract_kwargs(
            field="swap_amounts",
            bundle_metadata={"swap_type": "token_to_pt", "to_token_address": self.PT_WSTETH},
        )
        assert kwargs.get("intent_swap_type") == "token_to_pt"
        assert "pt_address" not in kwargs  # swap path never threads pt_address


class TestPTSymbolResolverHelpers:
    """Direct mocked-catalogue coverage of the symbol-resolution helpers (CodeRabbit).

    ``TestPTSwapSymbolResolution`` exercises these through ``extract_swap_amounts``
    against the live catalogue; this class pins the helper branches the live tables
    can't easily reach — the ``_pt_symbol_rank`` tiebreak and the missing-maturity
    degrade — independently, with mocked ``PT_TOKEN_INFO`` / ``PENDLE_TOKENS``.
    """

    _ADDR = "0x" + "ab" * 20

    def test_pt_symbol_rank_orders_longest_then_mixed_case(self):
        from almanak.connectors.pendle.receipt_parser import _pt_symbol_rank

        # Longer alias wins.
        assert _pt_symbol_rank("PT-wstETH-25JUN2026") > _pt_symbol_rank("PT-25JUN2026")
        # Equal length: the mixed-case (canonical, human-readable) spelling wins.
        assert len("PT-wstETH-25JUN2026") == len("PT-WSTETH-25JUN2026")
        assert _pt_symbol_rank("PT-wstETH-25JUN2026") > _pt_symbol_rank("PT-WSTETH-25JUN2026")

    def test_resolve_pt_symbol_prefers_maturity_bearing_mixed_case(self, monkeypatch):
        from almanak.connectors.pendle import sdk

        monkeypatch.setattr(
            sdk,
            "PT_TOKEN_INFO",
            {
                "arbitrum": {
                    "PT": (self._ADDR, 18),  # maturity-less — must be ignored
                    "PT-WSTETH-25JUN2026": (self._ADDR, 18),  # all-caps maturity-bearing
                    "PT-wstETH-25JUN2026": (self._ADDR, 18),  # canonical mixed-case
                }
            },
        )
        from almanak.connectors.pendle.receipt_parser import _resolve_pt_symbol

        # Case-insensitive address match; ranking picks the canonical mixed-case alias.
        assert _resolve_pt_symbol("arbitrum", self._ADDR.upper()) == "PT-wstETH-25JUN2026"

    def test_resolve_pt_symbol_no_maturity_alias_degrades_none(self, monkeypatch):
        """An address present in the catalogue but with no maturity-bearing alias
        degrades to None — never a fabricated maturity-less symbol (Empty != Zero)."""
        from almanak.connectors.pendle import sdk

        monkeypatch.setattr(
            sdk,
            "PT_TOKEN_INFO",
            {"arbitrum": {"PT": (self._ADDR, 18), "PT-SOMETHING": (self._ADDR, 18)}},
        )
        from almanak.connectors.pendle.receipt_parser import _resolve_pt_symbol

        assert _resolve_pt_symbol("arbitrum", self._ADDR) is None

    def test_resolve_pt_symbol_empty_or_unknown_is_none(self):
        from almanak.connectors.pendle.receipt_parser import _resolve_pt_symbol

        assert _resolve_pt_symbol("arbitrum", None) is None
        assert _resolve_pt_symbol("arbitrum", "") is None
        assert _resolve_pt_symbol("nosuchchain", self._ADDR) is None

    def test_resolve_base_symbol_lookup_and_degrade(self, monkeypatch):
        from almanak.connectors.pendle import addresses

        monkeypatch.setattr(addresses, "PENDLE_TOKENS", {"arbitrum": {"WSTETH": self._ADDR}})
        from almanak.connectors.pendle.receipt_parser import _resolve_base_symbol

        # Case-insensitive match.
        assert _resolve_base_symbol("arbitrum", self._ADDR.upper()) == "WSTETH"
        # Unknown address / empty / unknown chain → None (degrade to "SY" upstream).
        assert _resolve_base_symbol("arbitrum", "0x" + "cd" * 20) is None
        assert _resolve_base_symbol("arbitrum", None) is None
        assert _resolve_base_symbol("nosuchchain", self._ADDR) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
