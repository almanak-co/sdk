"""Coverage-focused tests for AerodromeReceiptParser branches.

Targets uncovered branches in:
- to_dict / from_dict round trips for AerodromeEvent and other dataclasses
- Mint/Burn/Transfer event parsing & to_dict
- _decode_log_data: unknown event fallback
- _decode_swap_data exception path (malformed data)
- _decode_mint_data, _decode_burn_data, _decode_transfer_data exception paths
- _parse_swap_event, _parse_mint_event, _parse_burn_event, _parse_transfer_event exception paths
- parse_receipt: empty logs, failed status, exception in parser, log warning
- _build_swap_result: no-quote vs quoted_amount_out vs quoted_price branches
- _build_liquidity_result: missing decimals branch
- extract_lp_close_data: Burn fallback, Transfer fallback (path A & B)
- _extract_lp_close_from_transfers: filters
- extract_position_id: success + exception
- extract_liquidity: success + exception
- extract_protocol_fees
- is_aerodrome_event / get_event_type with bytes inputs
- AerodromeSlipstreamReceiptParser: extract_position_id, extract_liquidity, extract_lp_close_data
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.connectors.aerodrome.receipt_parser import (
    EVENT_TOPICS,
    AerodromeEvent,
    AerodromeEventType,
    AerodromeReceiptParser,
    AerodromeSlipstreamReceiptParser,
    BurnEventData,
    MintEventData,
    ParsedLiquidityResult,
    ParsedSwapResult,
    ParseResult,
    SwapEventData,
    TransferEventData,
)
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
)

USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
WETH = "0x4200000000000000000000000000000000000006"
POOL = "0x" + "cc" * 20
WALLET = "0x" + "aa" * 20
ZERO = "0x" + "00" * 20


def _pad32(val: int, signed: bool = False) -> str:
    if signed and val < 0:
        val = val + (1 << 256)
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _swap_log(amount0_in: int, amount1_in: int, amount0_out: int, amount1_out: int, pool: str = POOL, log_index: int = 0) -> dict:
    data = "0x" + _pad32(amount0_in) + _pad32(amount1_in) + _pad32(amount0_out) + _pad32(amount1_out)
    return {
        "address": pool,
        "topics": [EVENT_TOPICS["Swap"], _addr_topic(WALLET), _addr_topic(WALLET)],
        "data": data,
        "logIndex": log_index,
    }


def _mint_log(amount0: int, amount1: int, pool: str = POOL, log_index: int = 0) -> dict:
    data = "0x" + _pad32(amount0) + _pad32(amount1)
    return {
        "address": pool,
        "topics": [EVENT_TOPICS["Mint"], _addr_topic(WALLET)],
        "data": data,
        "logIndex": log_index,
    }


def _burn_log(amount0: int, amount1: int, pool: str = POOL, to: str = WALLET, log_index: int = 0) -> dict:
    data = "0x" + _pad32(amount0) + _pad32(amount1)
    return {
        "address": pool,
        "topics": [EVENT_TOPICS["Burn"], _addr_topic(WALLET), _addr_topic(to)],
        "data": data,
        "logIndex": log_index,
    }


def _transfer_log(token: str, frm: str, to: str, amount: int, log_index: int = 0) -> dict:
    return {
        "address": token,
        "topics": [EVENT_TOPICS["Transfer"], _addr_topic(frm), _addr_topic(to)],
        "data": "0x" + _pad32(amount),
        "logIndex": log_index,
    }


def _approval_log(token: str, log_index: int = 0) -> dict:
    return {
        "address": token,
        "topics": [EVENT_TOPICS["Approval"], _addr_topic(WALLET), _addr_topic(POOL)],
        "data": "0x" + _pad32(10**18),
        "logIndex": log_index,
    }


def _receipt(logs: list[dict], status: int = 1, wallet: str | None = None) -> dict:
    r: dict = {
        "transactionHash": "0x" + "11" * 32,
        "blockNumber": 100,
        "status": status,
        "gasUsed": 150_000,
        "logs": logs,
    }
    if wallet is not None:
        r["from"] = wallet
    return r


# =============================================================================
# Dataclass to_dict / from_dict
# =============================================================================


class TestDataClassesRoundTrip:
    def test_aerodrome_event_to_dict(self) -> None:
        ev = AerodromeEvent(
            event_type=AerodromeEventType.SWAP,
            event_name="Swap",
            log_index=1,
            transaction_hash="0xabc",
            block_number=10,
            contract_address="0xpool",
            data={"x": 1},
            raw_topics=["0xtopic"],
            raw_data="0xdata",
        )
        d = ev.to_dict()
        assert d["event_type"] == "SWAP"
        assert d["log_index"] == 1
        assert d["raw_topics"] == ["0xtopic"]

    def test_aerodrome_event_from_dict_with_timestamp(self) -> None:
        ts = datetime.now(UTC).isoformat()
        ev = AerodromeEvent.from_dict({
            "event_type": "SWAP",
            "event_name": "Swap",
            "log_index": 1,
            "transaction_hash": "0xa",
            "block_number": 10,
            "contract_address": "0xpool",
            "data": {},
            "timestamp": ts,
        })
        assert ev.event_type == AerodromeEventType.SWAP
        assert ev.timestamp.tzinfo is not None

    def test_aerodrome_event_from_dict_without_timestamp(self) -> None:
        ev = AerodromeEvent.from_dict({
            "event_type": "MINT",
            "event_name": "Mint",
            "log_index": 1,
            "transaction_hash": "0xa",
            "block_number": 10,
            "contract_address": "0xpool",
            "data": {},
        })
        assert ev.event_type == AerodromeEventType.MINT

    def test_swap_event_data_to_dict(self) -> None:
        d = SwapEventData(
            sender="0xa", to="0xb",
            amount0_in=10, amount1_in=0, amount0_out=0, amount1_out=20,
            pool_address="0xp",
        ).to_dict()
        assert d["amount_in"] == "10"
        assert d["amount_out"] == "20"
        assert d["token0_is_input"] is True

    def test_mint_event_data_to_dict(self) -> None:
        d = MintEventData(sender="0xa", amount0=10, amount1=20, pool_address="0xp").to_dict()
        assert d["amount0"] == "10"
        assert d["pool_address"] == "0xp"

    def test_burn_event_data_to_dict(self) -> None:
        d = BurnEventData(sender="0xa", amount0=10, amount1=20, to="0xb", pool_address="0xp").to_dict()
        assert d["to"] == "0xb"

    def test_transfer_event_data_to_dict(self) -> None:
        d = TransferEventData(from_addr="0xa", to_addr="0xb", value=100, token_address="0xt").to_dict()
        assert d["value"] == "100"
        assert d["token_address"] == "0xt"

    def test_parsed_swap_result_to_dict_and_payload(self) -> None:
        sr = ParsedSwapResult(
            token_in="0xa", token_out="0xb",
            token_in_symbol="USDC", token_out_symbol="WETH",
            amount_in=10, amount_out=20,
            amount_in_decimal=Decimal("0.00001"), amount_out_decimal=Decimal("0.00002"),
            effective_price=Decimal("2"), slippage_bps=50,
            pool_address="0xp",
        )
        d = sr.to_dict()
        assert d["slippage_bps"] == 50
        payload = sr.to_swap_result_payload()
        assert payload.token_in == "USDC"

    def test_parsed_swap_result_payload_falls_back_to_address(self) -> None:
        sr = ParsedSwapResult(
            token_in="0xa", token_out="0xb",
            token_in_symbol="", token_out_symbol="",
            amount_in=10, amount_out=20,
            amount_in_decimal=Decimal("0"), amount_out_decimal=Decimal("0"),
            effective_price=Decimal("0"), slippage_bps=0,
            pool_address="0xp",
        )
        payload = sr.to_swap_result_payload()
        assert payload.token_in == "0xa"
        assert payload.token_out == "0xb"

    def test_parsed_liquidity_result_to_dict(self) -> None:
        lr = ParsedLiquidityResult(
            operation="add", token0="0xa", token1="0xb",
            token0_symbol="USDC", token1_symbol="WETH",
            amount0=100, amount1=200,
            amount0_decimal=Decimal("1"), amount1_decimal=Decimal("2"),
            pool_address="0xp",
        )
        d = lr.to_dict()
        assert d["operation"] == "add"
        assert d["amount0_decimal"] == "1"

    def test_parse_result_to_dict(self) -> None:
        result = ParseResult(success=True, transaction_hash="0xabc", block_number=10)
        d = result.to_dict()
        assert d["success"] is True
        assert d["swap_result"] is None
        assert d["liquidity_result"] is None


# =============================================================================
# parse_receipt branches
# =============================================================================


class TestParseReceiptBranches:
    def test_empty_logs_returns_success(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        result = parser.parse_receipt({"logs": [], "status": 1, "blockNumber": 1, "transactionHash": "0xabc"})
        assert result.success
        assert result.events == []

    def test_failed_tx_status_returns_error(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        receipt = _receipt([_swap_log(10, 0, 0, 20)], status=0)
        result = parser.parse_receipt(receipt)
        assert result.success
        assert result.transaction_success is False
        assert "reverted" in (result.error or "").lower()

    def test_bytes_tx_hash_normalized(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        result = parser.parse_receipt({
            "transactionHash": bytes.fromhex("aa" * 32),
            "logs": [],
            "status": 1,
            "blockNumber": 5,
        })
        assert result.success
        assert result.transaction_hash.startswith("0x")

    def test_parse_receipt_handles_exception(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        # logs as a non-iterable int short-circuits to the outer try/except
        # via "for log in logs" → TypeError. (A string would iterate char-by-char
        # and only fail inside _parse_log, which is caught at log-level.)
        result = parser.parse_receipt({"logs": 12345, "status": 1, "blockNumber": 1})
        assert isinstance(result, ParseResult)
        assert result.success is False
        # The except branch returns ParseResult(success=False, error=str(e)),
        # where e is `TypeError("'int' object is not iterable")`.
        assert "not iterable" in (result.error or "").lower()

    def test_parses_full_swap_with_transfers(self) -> None:
        """Full happy path produces swap_events + swap_result + transfer_events log lines."""
        parser = AerodromeReceiptParser(
            chain="base", token0_address=USDC, token1_address=WETH,
            token0_decimals=6, token1_decimals=18,
            token0_symbol="USDC", token1_symbol="WETH",
        )
        result = parser.parse_receipt(
            _receipt([
                _transfer_log(USDC, WALLET, POOL, 3_000_000),
                _swap_log(3_000_000, 0, 0, 10**15),
                _transfer_log(WETH, POOL, WALLET, 10**15),
            ], wallet=WALLET)
        )
        assert result.success
        assert len(result.swap_events) == 1
        assert len(result.transfer_events) == 2
        assert result.swap_result is not None

    def test_parses_mint_event_full(self) -> None:
        """Mint event triggers mint_events list and liquidity_result."""
        parser = AerodromeReceiptParser(
            chain="base", token0_address=USDC, token1_address=WETH,
            token0_decimals=6, token1_decimals=18,
            token0_symbol="USDC", token1_symbol="WETH",
        )
        result = parser.parse_receipt(_receipt([_mint_log(1_000_000, 5 * 10**14)]))
        assert result.success
        assert len(result.mint_events) == 1
        assert result.liquidity_result is not None
        assert result.liquidity_result.operation == "add"

    def test_parses_burn_event_full(self) -> None:
        parser = AerodromeReceiptParser(
            chain="base", token0_address=USDC, token1_address=WETH,
            token0_decimals=6, token1_decimals=18,
            token0_symbol="USDC", token1_symbol="WETH",
        )
        result = parser.parse_receipt(_receipt([_burn_log(1_000_000, 5 * 10**14)]))
        assert result.success
        assert len(result.burn_events) == 1
        assert result.liquidity_result.operation == "remove"


class TestParseLogBranches:
    def test_log_with_no_topics_skipped(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        result = parser.parse_receipt(_receipt([{"topics": [], "data": "0x", "address": POOL}]))
        assert result.success
        assert result.events == []

    def test_unknown_topic_skipped(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        unknown_topic = "0x" + "ff" * 32
        result = parser.parse_receipt(_receipt([{"topics": [unknown_topic], "data": "0x", "address": POOL}]))
        assert result.success
        assert result.events == []

    def test_bytes_topic_normalized(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        topic_bytes = bytes.fromhex(EVENT_TOPICS["Approval"][2:])
        result = parser.parse_receipt(_receipt([{
            "topics": [topic_bytes, _addr_topic(WALLET), _addr_topic(POOL)],
            "data": "0x" + _pad32(10**18),
            "address": POOL,
        }]))
        assert result.success
        assert len(result.events) == 1

    def test_bytes_address_normalized(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        addr_bytes = bytes.fromhex(POOL[2:])
        result = parser.parse_receipt(_receipt([{
            "topics": [EVENT_TOPICS["Mint"], _addr_topic(WALLET)],
            "data": "0x" + _pad32(1) + _pad32(2),
            "address": addr_bytes,
        }]))
        assert result.success

    def test_approval_event_parsed_unknown_type(self) -> None:
        """Approval doesn't have a typed handler; falls into unknown branch."""
        parser = AerodromeReceiptParser(chain="base")
        result = parser.parse_receipt(_receipt([_approval_log(USDC)]))
        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == AerodromeEventType.APPROVAL


# =============================================================================
# _decode_*_data exception branches
# =============================================================================


class TestDecodeDataExceptions:
    def test_decode_swap_with_too_short_data_caught(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        # Data only 1 byte — indexing will raise
        result = parser.parse_receipt(_receipt([{
            "address": POOL,
            "topics": [EVENT_TOPICS["Swap"], _addr_topic(WALLET), _addr_topic(WALLET)],
            "data": "0x00",
        }]))
        # exception caught — log skipped, parse continues
        assert result.success

    def test_decode_mint_with_too_short_data_caught(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        result = parser.parse_receipt(_receipt([{
            "address": POOL,
            "topics": [EVENT_TOPICS["Mint"], _addr_topic(WALLET)],
            "data": "0x00",
        }]))
        assert result.success

    def test_decode_burn_with_too_short_data_caught(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        result = parser.parse_receipt(_receipt([{
            "address": POOL,
            "topics": [EVENT_TOPICS["Burn"], _addr_topic(WALLET), _addr_topic(WALLET)],
            "data": "0x00",
        }]))
        assert result.success

    def test_decode_transfer_with_too_short_data_caught(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        result = parser.parse_receipt(_receipt([{
            "address": USDC,
            "topics": [EVENT_TOPICS["Transfer"], _addr_topic(WALLET), _addr_topic(POOL)],
            "data": "0x00",
        }]))
        # Even with bad data, parse_receipt completes
        assert result.success


# =============================================================================
# _build_swap_result branches
# =============================================================================


class TestBuildSwapResultBranches:
    def test_token1_input_swap(self) -> None:
        """token1_is_input branch — flip token0/token1."""
        parser = AerodromeReceiptParser(
            chain="base", token0_address=USDC, token1_address=WETH,
            token0_decimals=6, token1_decimals=18,
            token0_symbol="USDC", token1_symbol="WETH",
        )
        result = parser.parse_receipt(_receipt([_swap_log(0, 10**18, 2_500_000_000, 0)]))
        assert result.swap_result.token_in_symbol == "WETH"
        assert result.swap_result.token_out_symbol == "USDC"

    def test_quoted_amount_out_overrides_slippage(self) -> None:
        parser = AerodromeReceiptParser(
            chain="base", token0_address=USDC, token1_address=WETH,
            token0_decimals=6, token1_decimals=18,
            token0_symbol="USDC", token1_symbol="WETH",
        )
        result = parser.parse_receipt(_receipt([_swap_log(3_000_000, 0, 0, 10**15)]), quoted_amount_out=10**15 + 100)
        assert result.swap_result is not None
        # slippage = (quoted - actual)/quoted ≈ small positive value
        assert result.swap_result.slippage_bps >= 0

    def test_quoted_price_overrides_slippage(self) -> None:
        parser = AerodromeReceiptParser(
            chain="base", token0_address=USDC, token1_address=WETH,
            token0_decimals=6, token1_decimals=18,
            token0_symbol="USDC", token1_symbol="WETH",
            quoted_price=Decimal("0.0005"),
        )
        result = parser.parse_receipt(_receipt([_swap_log(3_000_000, 0, 0, 10**15)]))
        assert result.swap_result is not None

    def test_unresolved_decimals_omits_swap_result(self) -> None:
        """When decimals are still None after resolver attempt, swap_result is None."""
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address="0x" + "01" * 20,  # synthetic, non-resolvable
            token1_address="0x" + "02" * 20,
        )
        # Build CL swap log so amount0/amount1 are signed; tokens not in resolver.
        result = parser.parse_receipt(_receipt([_swap_log(3_000_000, 0, 0, 10**15)]))
        assert result.success
        assert result.swap_result is None  # decimals unresolved

    def test_zero_amount_in_yields_zero_price(self) -> None:
        parser = AerodromeReceiptParser(
            chain="base", token0_address=USDC, token1_address=WETH,
            token0_decimals=6, token1_decimals=18,
            token0_symbol="USDC", token1_symbol="WETH",
        )
        # All zeros → effective_price = 0
        result = parser.parse_receipt(_receipt([_swap_log(0, 0, 0, 0)]))
        # For all-zero amounts, both are inputs equal to 0, so amount_in=0
        assert result.swap_result is not None
        assert result.swap_result.effective_price == Decimal("0")


# =============================================================================
# _build_liquidity_result missing-decimals branch
# =============================================================================


class TestBuildLiquidityResultMissingDecimals:
    def test_decimals_unresolved_sets_zero(self) -> None:
        """Without token addresses or decimals, amounts default to Decimal(0)."""
        parser = AerodromeReceiptParser(chain="base")
        result = parser.parse_receipt(_receipt([_mint_log(1_000_000, 5 * 10**14)]))
        # liquidity_result is built but with Decimal(0)
        assert result.liquidity_result is not None
        assert result.liquidity_result.amount0_decimal == Decimal(0)
        assert result.liquidity_result.amount1_decimal == Decimal(0)


# =============================================================================
# extract_lp_close_data
# =============================================================================


class TestExtractLpCloseData:
    def test_burn_event_path(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        # Multiple burn events accumulate
        receipt = _receipt([_burn_log(100, 200), _burn_log(50, 75)])
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        assert data.amount0_collected == 150
        assert data.amount1_collected == 275

    def test_transfer_fallback_path_a_known_tokens(self) -> None:
        parser = AerodromeReceiptParser(
            chain="base", token0_address=USDC, token1_address=WETH,
        )
        # No Burn event; only Transfer events from pool to wallet
        receipt = _receipt([
            _transfer_log(USDC, POOL, WALLET, 1_000_000),
            _transfer_log(WETH, POOL, WALLET, 5 * 10**14),
        ])
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        assert data.amount0_collected == 1_000_000
        assert data.amount1_collected == 5 * 10**14

    def test_transfer_fallback_path_b_grouping(self) -> None:
        """When token addresses are unknown but recipient gets 2+ tokens."""
        parser = AerodromeReceiptParser(chain="base")  # no token addresses
        receipt = _receipt([
            _transfer_log("0x" + "01" * 20, POOL, WALLET, 1_000_000),
            _transfer_log("0x" + "02" * 20, POOL, WALLET, 5 * 10**14),
        ])
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        # Sorted desc — bigger amount first
        assert data.amount0_collected >= data.amount1_collected

    def test_no_burn_no_transfers_returns_none(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        data = parser.extract_lp_close_data(_receipt([]))
        assert data is None

    def test_only_burns_to_zero_filtered(self) -> None:
        """Transfers to zero address (LP token burn) are filtered out."""
        parser = AerodromeReceiptParser(
            chain="base", token0_address=USDC, token1_address=WETH,
        )
        receipt = _receipt([
            _transfer_log(POOL, WALLET, ZERO, 10**18),  # LP burn
        ])
        data = parser.extract_lp_close_data(receipt)
        assert data is None


# =============================================================================
# extract_position_id, extract_liquidity, extract_protocol_fees
# =============================================================================


class TestExtractPositionAndLiquidity:
    def test_extract_position_id_returns_pool_address(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        receipt = _receipt([_mint_log(100, 200, pool=POOL)])
        out = parser.extract_position_id(receipt)
        assert out is not None
        assert out == POOL.lower()

    def test_extract_position_id_no_mint_returns_none(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        out = parser.extract_position_id(_receipt([]))
        assert out is None

    def test_extract_position_id_bytes_topic(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        topic_bytes = bytes.fromhex(EVENT_TOPICS["Mint"][2:])
        receipt = _receipt([{
            "address": POOL,
            "topics": [topic_bytes, _addr_topic(WALLET)],
            "data": "0x" + _pad32(100) + _pad32(200),
        }])
        out = parser.extract_position_id(receipt)
        assert out is not None

    def test_extract_position_id_bytes_address(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        addr_bytes = bytes.fromhex(POOL[2:])
        receipt = _receipt([{
            "address": addr_bytes,
            "topics": [EVENT_TOPICS["Mint"], _addr_topic(WALLET)],
            "data": "0x" + _pad32(100) + _pad32(200),
        }])
        out = parser.extract_position_id(receipt)
        assert out is not None
        assert out.startswith("0x")

    def test_extract_liquidity_via_transfer_from_zero(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        # LP token mint = transfer from zero address
        receipt = _receipt([_transfer_log(POOL, ZERO, WALLET, 10**18)])
        out = parser.extract_liquidity(receipt)
        assert out == 10**18

    def test_extract_liquidity_no_zero_transfer_returns_none(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        receipt = _receipt([_transfer_log(USDC, WALLET, POOL, 100)])
        out = parser.extract_liquidity(receipt)
        assert out is None

    def test_extract_protocol_fees_returns_unavailable(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        fees = parser.extract_protocol_fees({"logs": []})
        assert fees.total_usd is None
        assert fees.unavailable_reason == "protocol_fee_not_emitted_in_receipt"


# =============================================================================
# is_aerodrome_event / get_event_type
# =============================================================================


class TestIsAerodromeEvent:
    def test_str_topic_known(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        assert parser.is_aerodrome_event(EVENT_TOPICS["Swap"]) is True

    def test_str_topic_no_prefix(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        topic = EVENT_TOPICS["Swap"][2:]  # without 0x
        assert parser.is_aerodrome_event(topic) is True

    def test_bytes_topic_known(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        topic_bytes = bytes.fromhex(EVENT_TOPICS["Mint"][2:])
        assert parser.is_aerodrome_event(topic_bytes) is True

    def test_unknown_topic_returns_false(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        assert parser.is_aerodrome_event("0x" + "ff" * 32) is False

    def test_get_event_type_str(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        assert parser.get_event_type(EVENT_TOPICS["Mint"]) == AerodromeEventType.MINT

    def test_get_event_type_bytes(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        topic_bytes = bytes.fromhex(EVENT_TOPICS["Burn"][2:])
        assert parser.get_event_type(topic_bytes) == AerodromeEventType.BURN

    def test_get_event_type_unknown_returns_unknown(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        assert parser.get_event_type("0x" + "ee" * 32) == AerodromeEventType.UNKNOWN


# =============================================================================
# extract_*_result variants (covers the remaining branches via crash + ok)
# =============================================================================


class TestExtractResultVariants:
    def test_extract_swap_amounts_result_ok(self) -> None:
        parser = AerodromeReceiptParser(
            chain="base", token0_address=USDC, token1_address=WETH,
            token0_decimals=6, token1_decimals=18,
            token0_symbol="USDC", token1_symbol="WETH",
        )
        result = parser.extract_swap_amounts_result(
            _receipt([_swap_log(3_000_000, 0, 0, 10**15)]),
        )
        assert isinstance(result, ExtractOk)

    def test_extract_lp_close_data_result_ok(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        result = parser.extract_lp_close_data_result(_receipt([_burn_log(100, 200)]))
        assert isinstance(result, ExtractOk)

    def test_extract_position_id_result_ok(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        result = parser.extract_position_id_result(_receipt([_mint_log(100, 200)]))
        assert isinstance(result, ExtractOk)

    def test_extract_liquidity_result_ok(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        result = parser.extract_liquidity_result(_receipt([_transfer_log(POOL, ZERO, WALLET, 10**18)]))
        assert isinstance(result, ExtractOk)

    def test_extract_position_id_result_crash(self) -> None:
        parser = AerodromeReceiptParser(chain="base")

        def boom(_r: dict) -> None:
            raise RuntimeError("boom")

        parser.extract_position_id = boom  # type: ignore[method-assign]
        result = parser.extract_position_id_result(_receipt([]))
        assert isinstance(result, ExtractError)

    def test_extract_liquidity_result_crash(self) -> None:
        parser = AerodromeReceiptParser(chain="base")

        def boom(_r: dict) -> None:
            raise RuntimeError("boom")

        parser.extract_liquidity = boom  # type: ignore[method-assign]
        result = parser.extract_liquidity_result(_receipt([]))
        assert isinstance(result, ExtractError)


# =============================================================================
# AerodromeSlipstreamReceiptParser
# =============================================================================


def _erc721_transfer_mint_log(token_id: int, to: str = WALLET, log_index: int = 0) -> dict:
    """ERC-721 Transfer (mint) — 4 topics, from = zero address."""
    return {
        "address": "0x" + "ee" * 20,
        "topics": [
            EVENT_TOPICS["Transfer"],
            _addr_topic(ZERO),  # from = mint
            _addr_topic(to),
            _addr_topic("0x" + format(token_id, "040x")),  # tokenId in topic3
        ],
        "data": "0x",
        "logIndex": log_index,
    }


def _increase_liquidity_log(liquidity: int, amount0: int = 100, amount1: int = 200, log_index: int = 0) -> dict:
    return {
        "address": "0x" + "ee" * 20,
        "topics": [
            EVENT_TOPICS["IncreaseLiquidity"],
            _addr_topic("0x" + "1" * 40),  # tokenId indexed
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _decrease_liquidity_log(liquidity: int, amount0: int, amount1: int, log_index: int = 0) -> dict:
    return {
        "address": "0x" + "ee" * 20,
        "topics": [
            EVENT_TOPICS["DecreaseLiquidity"],
            _addr_topic("0x" + "1" * 40),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _collect_cl_log(amount0: int, amount1: int, log_index: int = 0) -> dict:
    return {
        "address": "0x" + "ee" * 20,
        "topics": [
            EVENT_TOPICS["CollectCL"],
            _addr_topic("0x" + "1" * 40),
        ],
        "data": "0x" + _pad32(0) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


class TestSlipstreamReceiptParser:
    def test_extract_position_id_from_mint(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt([_erc721_transfer_mint_log(token_id=12345)])
        out = parser.extract_position_id(receipt)
        assert out == "12345"

    def test_extract_position_id_no_mint_returns_none(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        out = parser.extract_position_id(_receipt([]))
        assert out is None

    def test_extract_position_id_skips_non_mint_transfers(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        # 4 topics ERC-721 transfer but from is not zero
        receipt = _receipt([{
            "address": "0x" + "ee" * 20,
            "topics": [
                EVENT_TOPICS["Transfer"],
                _addr_topic(WALLET),  # NOT zero — not a mint
                _addr_topic(POOL),
                _addr_topic("0x" + format(99, "040x")),
            ],
            "data": "0x",
        }])
        out = parser.extract_position_id(receipt)
        assert out is None

    def test_extract_position_id_skips_3_topic_transfers(self) -> None:
        """ERC-20 Transfer (3 topics) is not an ERC-721 mint."""
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        # ERC-20 transfer has 3 topics
        receipt = _receipt([_transfer_log(USDC, ZERO, WALLET, 10**18)])
        out = parser.extract_position_id(receipt)
        assert out is None

    def test_extract_position_id_bytes_topic(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt([{
            "address": "0x" + "ee" * 20,
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Transfer"][2:]),
                _addr_topic(ZERO),
                _addr_topic(WALLET),
                bytes.fromhex(format(7, "064x")),
            ],
            "data": "0x",
        }])
        out = parser.extract_position_id(receipt)
        assert out == "7"

    def test_extract_liquidity_from_increase_liquidity(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt([_increase_liquidity_log(liquidity=10**18)])
        out = parser.extract_liquidity(receipt)
        assert out == 10**18

    def test_extract_liquidity_no_increase_returns_none(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        out = parser.extract_liquidity(_receipt([]))
        assert out is None

    def test_extract_liquidity_bytes_topic(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt([{
            "address": "0x" + "ee" * 20,
            "topics": [bytes.fromhex(EVENT_TOPICS["IncreaseLiquidity"][2:]), _addr_topic("0x" + "1" * 40)],
            "data": "0x" + _pad32(10**18) + _pad32(100) + _pad32(200),
        }])
        out = parser.extract_liquidity(receipt)
        assert out == 10**18

    def test_extract_lp_close_data_via_collect(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt([_collect_cl_log(amount0=1_000_000, amount1=5 * 10**14)])
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        assert data.amount0_collected == 1_000_000
        assert data.amount1_collected == 5 * 10**14

    def test_extract_lp_close_data_via_decrease_fallback(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        # Only DecreaseLiquidity, no Collect — fall back to decrease
        receipt = _receipt([_decrease_liquidity_log(liquidity=10**18, amount0=100, amount1=200)])
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        assert data.amount0_collected == 100
        assert data.amount1_collected == 200
        assert data.liquidity_removed == 10**18

    def test_extract_lp_close_data_no_events_returns_none(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        data = parser.extract_lp_close_data(_receipt([]))
        assert data is None

    def test_extract_lp_close_data_bytes_topic_collect(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt([{
            "address": "0x" + "ee" * 20,
            "topics": [bytes.fromhex(EVENT_TOPICS["CollectCL"][2:]), _addr_topic("0x" + "1" * 40)],
            "data": "0x" + _pad32(0) + _pad32(100) + _pad32(200),
        }])
        data = parser.extract_lp_close_data(receipt)
        assert data is not None

    def test_extract_lp_close_data_result_ok(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        result = parser.extract_lp_close_data_result(_receipt([_collect_cl_log(100, 200)]))
        assert isinstance(result, ExtractOk)

    def test_extract_lp_close_data_result_missing(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        result = parser.extract_lp_close_data_result(_receipt([]))
        assert isinstance(result, ExtractMissing)

    def test_extract_lp_close_data_result_crash(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")

        def boom(_r: dict) -> None:
            raise RuntimeError("kaboom")

        parser.extract_lp_close_data = boom  # type: ignore[method-assign]
        result = parser.extract_lp_close_data_result(_receipt([]))
        assert isinstance(result, ExtractError)

    def test_extract_position_id_result_ok(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        result = parser.extract_position_id_result(_receipt([_erc721_transfer_mint_log(token_id=42)]))
        assert isinstance(result, ExtractOk)

    def test_extract_position_id_result_missing(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        result = parser.extract_position_id_result(_receipt([]))
        assert isinstance(result, ExtractMissing)

    def test_extract_position_id_result_crash(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")

        def boom(_r: dict) -> None:
            raise RuntimeError("k")

        parser.extract_position_id = boom  # type: ignore[method-assign]
        result = parser.extract_position_id_result(_receipt([]))
        assert isinstance(result, ExtractError)

    def test_extract_liquidity_result_ok(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        result = parser.extract_liquidity_result(_receipt([_increase_liquidity_log(10**18)]))
        assert isinstance(result, ExtractOk)

    def test_extract_liquidity_result_missing(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        result = parser.extract_liquidity_result(_receipt([]))
        assert isinstance(result, ExtractMissing)

    def test_extract_liquidity_result_crash(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")

        def boom(_r: dict) -> None:
            raise RuntimeError("k")

        parser.extract_liquidity = boom  # type: ignore[method-assign]
        result = parser.extract_liquidity_result(_receipt([]))
        assert isinstance(result, ExtractError)


# =============================================================================
# Resolution helpers — _resolve_token_info, _resolve_decimals
# =============================================================================


class TestResolveHelpers:
    def test_resolve_token_info_resolves_address(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        # USDC base address resolves via the singleton resolver
        sym, dec = parser._resolve_token_info(USDC)
        # Symbol may differ depending on registry but decimals should be 6
        assert dec == 6

    def test_resolve_decimals_returns_none_for_empty(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        assert parser._resolve_decimals("") is None

    def test_resolve_decimals_returns_none_for_synthetic(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        assert parser._resolve_decimals("0x" + "ab" * 20) is None


# =============================================================================
# Constructor with token symbols only (no address) — exercises symbol→decimals branch
# =============================================================================


class TestParserInitBranches:
    def test_constructor_with_symbols_only_resolves_decimals(self) -> None:
        # USDC and WETH symbols resolve via the resolver
        parser = AerodromeReceiptParser(
            chain="base",
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        # Decimals get resolved automatically
        assert parser.token0_decimals == 6
        assert parser.token1_decimals == 18

    def test_constructor_with_addresses_only_resolves_symbols(self) -> None:
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC,
            token1_address=WETH,
        )
        # Symbols get resolved
        assert parser.token0_symbol == "USDC"
        assert parser.token1_symbol == "WETH"


# =============================================================================
# Pool fallback test - covers the _extract_tokens_by_pool branch
# =============================================================================


class TestPoolFallback:
    def test_extract_tokens_by_pool_finds_in_and_out(self) -> None:
        parser = AerodromeReceiptParser(chain="base")
        # No wallet party to transfers; only pool↔router. Pool fallback wins.
        router = "0x" + "dd" * 20
        receipt = _receipt(
            [
                _transfer_log(USDC, router, POOL, 5_000_000),
                _swap_log(5_000_000, 0, 0, 10**15),
                _transfer_log(WETH, POOL, router, 10**15),
            ],
            wallet="0x" + "ff" * 20,
        )
        out = parser.extract_swap_amounts(receipt)
        assert out is not None
        assert out.amount_in == 5_000_000
        assert out.amount_out == 10**15
