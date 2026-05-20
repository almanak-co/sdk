"""Unit tests for SushiSwap V3 receipt parser core methods.

Covers:
- parse_receipt success path with Swap event
- parse_receipt with empty logs
- parse_receipt with failed transaction
- parse_receipt with bytes tx_hash and hex status
- parse_receipt exception handling
- _parse_log: bytes topics normalization, unknown event, exception
- _decode_swap_data + _decode_transfer_data
- _parse_swap_event / _parse_transfer_event
- _build_swap_result: token0_is_input branch, token1_is_input branch,
  decimals unresolved branch, slippage with quoted_amount_out & quoted_price
- extract_position_id: success, no logs, missing position_manager fall-through,
  bytes topics, wrong event, wrong contract, less than 4 topics, non-zero from
- extract_position_id_from_logs static method
- extract_tick_lower / extract_tick_upper / extract_liquidity success and miss paths
- extract_lp_close_data with Collect + Burn events
- extract_swap_amounts with both swap_result and fallback to swap_events
- _resolve_token_info success / failure
- is_sushiswap_event / get_event_type with bytes / no-prefix / mixed case
- SushiSwapV3Event.to_dict / from_dict round trip
- SwapEventData properties (token1_is_input, amount_in/out)
- ParsedSwapResult to_dict / from_dict / to_swap_result_payload
- ParseResult to_dict
- TransferEventData to_dict
- parse_logs convenience method
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

from almanak.framework.connectors.sushiswap_v3.receipt_parser import (
    EVENT_TOPICS,
    POSITION_MANAGER_ADDRESSES,
    SWAP_EVENT_TOPIC,
    ZERO_ADDRESS_PADDED,
    ParsedSwapResult,
    ParseResult,
    SushiSwapV3Event,
    SushiSwapV3EventType,
    SushiSwapV3ReceiptParser,
    SwapEventData,
    TransferEventData,
)

# Test addresses
WALLET = "0x1234567890123456789012345678901234567890"
USDC_ARB = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ARB = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
POOL_ADDRESS = "0xC6962004F452BE9203591991D15F6B388e09E8D0"
ZERO_ADDR = "0x0000000000000000000000000000000000000000"
NFT_TOKEN_ID = 12345


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _pad_addr(addr: str) -> str:
    """Pad address to 32-byte topic."""
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _enc_uint(value: int) -> str:
    """Encode uint as 32-byte hex (no 0x)."""
    return hex(value)[2:].zfill(64)


def _enc_int_signed(value: int) -> str:
    """Encode signed int as 32-byte two's complement hex."""
    if value >= 0:
        return hex(value)[2:].zfill(64)
    return hex((1 << 256) + value)[2:].zfill(64)


def _make_swap_log(
    pool: str = POOL_ADDRESS,
    sender: str = WALLET,
    recipient: str = WALLET,
    amount0: int = 1000 * 10**6,
    amount1: int = -3 * 10**17,  # negative = user receives
    sqrt_price_x96: int = 2**96,
    liquidity: int = 10**12,
    tick: int = 0,
    log_index: int = 0,
) -> dict[str, Any]:
    data = (
        _enc_int_signed(amount0)
        + _enc_int_signed(amount1)
        + _enc_uint(sqrt_price_x96)
        + _enc_uint(liquidity)
        + _enc_int_signed(tick)
    )
    return {
        "address": pool,
        "topics": [EVENT_TOPICS["Swap"], _pad_addr(sender), _pad_addr(recipient)],
        "data": "0x" + data,
        "logIndex": log_index,
    }


def _make_transfer_log(
    token: str = USDC_ARB,
    from_addr: str = WALLET,
    to_addr: str = POOL_ADDRESS,
    value: int = 1000,
    log_index: int = 0,
) -> dict[str, Any]:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"], _pad_addr(from_addr), _pad_addr(to_addr),
        ],
        "data": "0x" + _enc_uint(value),
        "logIndex": log_index,
    }


def _make_erc721_mint_log(
    token_id: int = NFT_TOKEN_ID,
    to: str = WALLET,
    chain: str = "arbitrum",
) -> dict[str, Any]:
    """ERC-721 Transfer with 4 topics (from=zero, to, tokenId)."""
    pm = POSITION_MANAGER_ADDRESSES[chain]
    return {
        "address": pm,
        "topics": [
            EVENT_TOPICS["Transfer"],
            ZERO_ADDRESS_PADDED,
            _pad_addr(to),
            "0x" + _enc_uint(token_id),
        ],
        "data": "0x",
    }


def _make_mint_log(
    pool: str = POOL_ADDRESS,
    owner: str = WALLET,
    tick_lower: int = -60,
    tick_upper: int = 60,
    liquidity: int = 10**12,
    amount0: int = 100,
    amount1: int = 200,
) -> dict[str, Any]:
    """Pool Mint event - 4 topics, complex data layout."""
    data = (
        _pad_addr(owner)[2:]  # sender (padded address)
        + _enc_uint(liquidity)  # amount (uint128)
        + _enc_uint(amount0)  # amount0
        + _enc_uint(amount1)  # amount1
    )
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Mint"],
            _pad_addr(owner),  # owner indexed
            "0x" + _enc_int_signed(tick_lower),  # tickLower indexed (int24)
            "0x" + _enc_int_signed(tick_upper),  # tickUpper indexed (int24)
        ],
        "data": "0x" + data,
    }


def _make_collect_log(
    pool: str = POOL_ADDRESS,
    owner: str = WALLET,
    recipient: str = WALLET,
    tick_lower: int = -60,
    tick_upper: int = 60,
    amount0: int = 500,
    amount1: int = 600,
) -> dict[str, Any]:
    """Pool Collect event."""
    # Data: recipient (padded address), amount0 (uint128), amount1 (uint128)
    data = (
        _pad_addr(recipient)[2:]
        + _enc_uint(amount0)
        + _enc_uint(amount1)
    )
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Collect"],
            _pad_addr(owner),
            "0x" + _enc_int_signed(tick_lower),
            "0x" + _enc_int_signed(tick_upper),
        ],
        "data": "0x" + data,
    }


def _make_burn_log(
    pool: str = POOL_ADDRESS,
    owner: str = WALLET,
    tick_lower: int = -60,
    tick_upper: int = 60,
    liquidity: int = 10**12,
    amount0: int = 100,
    amount1: int = 200,
) -> dict[str, Any]:
    data = (
        _enc_uint(liquidity)
        + _enc_uint(amount0)
        + _enc_uint(amount1)
    )
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Burn"],
            _pad_addr(owner),
            "0x" + _enc_int_signed(tick_lower),
            "0x" + _enc_int_signed(tick_upper),
        ],
        "data": "0x" + data,
    }


def _make_receipt(
    logs: list[dict],
    success: bool = True,
    tx_hash: str = "0x" + "ab" * 32,
    block_number: int = 12345,
    from_addr: str | None = WALLET,
    gas_used: int = 200000,
) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "transactionHash": tx_hash,
        "blockNumber": block_number,
        "status": 1 if success else 0,
        "logs": logs,
        "gasUsed": gas_used,
    }
    if from_addr is not None:
        receipt["from"] = from_addr
    return receipt


# --------------------------------------------------------------------------
# Initialization & token resolution
# --------------------------------------------------------------------------


class TestParserInit:
    def test_init_with_addresses_resolves_symbols(self):
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver"
        ) as mock_get:
            mock_token = MagicMock()
            mock_token.symbol = "USDC"
            mock_token.decimals = 6
            mock_resolver = MagicMock()
            mock_resolver.resolve.return_value = mock_token
            mock_get.return_value = mock_resolver

            parser = SushiSwapV3ReceiptParser(
                chain="arbitrum",
                token0_address=USDC_ARB,
                token1_address=WETH_ARB,
            )
            # Resolved symbols/decimals
            assert parser.token0_symbol == "USDC"
            assert parser.token0_decimals == 6

    def test_init_with_symbols_only_resolves_decimals(self):
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver"
        ) as mock_get:
            mock_token = MagicMock()
            mock_token.symbol = "USDC"
            mock_token.decimals = 6
            mock_resolver = MagicMock()
            mock_resolver.resolve.return_value = mock_token
            mock_get.return_value = mock_resolver

            parser = SushiSwapV3ReceiptParser(
                chain="arbitrum",
                token0_symbol="USDC",
                token1_symbol="WETH",
            )
            assert parser.token0_decimals == 6

    def test_init_resolver_exception_does_not_break(self):
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            side_effect=Exception("resolver missing"),
        ):
            parser = SushiSwapV3ReceiptParser(
                chain="arbitrum", token0_address=USDC_ARB,
            )
            # Doesn't raise; symbols/decimals stay None
            assert parser.token0_symbol is None or parser.token0_symbol == ""

    def test_resolve_token_info_failure_returns_empty_tuple(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            side_effect=Exception("missing"),
        ):
            assert parser._resolve_token_info("0xfoo") == ("", None)


# --------------------------------------------------------------------------
# parse_receipt
# --------------------------------------------------------------------------


class TestParseReceipt:
    def test_empty_logs_returns_success(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.parse_receipt(_make_receipt([]))
        assert result.success is True
        assert result.events == []
        assert result.transaction_success is True

    def test_failed_transaction(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.parse_receipt(_make_receipt([_make_swap_log()], success=False))
        assert result.success is True
        assert result.transaction_success is False
        assert "reverted" in (result.error or "").lower()

    def test_failed_transaction_with_empty_logs(self):
        """Regression for issue #2064: early-revert receipt (status=0, logs=[])
        must surface the revert via ``error``."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.parse_receipt(_make_receipt([], success=False))
        assert result.success is True
        assert result.transaction_success is False
        assert result.error == "Transaction reverted"

    def test_hex_status(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = _make_receipt([_make_swap_log()])
        receipt["status"] = "0x1"
        result = parser.parse_receipt(receipt)
        assert result.transaction_success is True

    def test_int_str_status(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = _make_receipt([_make_swap_log()])
        receipt["status"] = "1"
        result = parser.parse_receipt(receipt)
        assert result.transaction_success is True

    def test_bytes_tx_hash(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = _make_receipt([_make_swap_log()])
        receipt["transactionHash"] = bytes.fromhex("ab" * 32)
        result = parser.parse_receipt(receipt)
        assert result.transaction_hash == "0x" + "ab" * 32

    def test_with_swap_and_transfer(self):
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ARB,
            token1_address=WETH_ARB,
            token0_symbol="USDC",
            token1_symbol="WETH",
            token0_decimals=6,
            token1_decimals=18,
        )
        logs = [
            _make_transfer_log(token=USDC_ARB, log_index=0),
            _make_swap_log(log_index=1),
            _make_transfer_log(
                token=WETH_ARB, from_addr=POOL_ADDRESS, to_addr=WALLET,
                value=3 * 10**17, log_index=2,
            ),
        ]
        result = parser.parse_receipt(_make_receipt(logs))
        assert result.success is True
        assert len(result.swap_events) == 1
        assert len(result.transfer_events) == 2
        assert result.swap_result is not None
        assert result.swap_result.amount_in == 1000 * 10**6
        assert result.swap_result.token_in_symbol == "USDC"

    def test_quoted_amount_out_calculates_slippage(self):
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ARB, token1_address=WETH_ARB,
            token0_decimals=6, token1_decimals=18,
        )
        # amount0=1000 USDC in, amount1=-3e17 = 0.3 WETH out
        logs = [_make_swap_log(amount0=1000 * 10**6, amount1=-3 * 10**17)]
        result = parser.parse_receipt(_make_receipt(logs), quoted_amount_out=4 * 10**17)
        assert result.swap_result is not None
        # slippage_bps > 0 since actual < expected
        assert result.swap_result.slippage_bps > 0

    def test_quoted_price_calculates_slippage(self):
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ARB, token1_address=WETH_ARB,
            token0_decimals=6, token1_decimals=18,
            quoted_price=Decimal("0.0005"),  # 0.5 WETH per USDC (high)
        )
        logs = [_make_swap_log(amount0=1000 * 10**6, amount1=-3 * 10**17)]
        result = parser.parse_receipt(_make_receipt(logs))
        assert result.swap_result is not None
        # quoted_price > effective_price -> positive slippage
        assert result.swap_result.slippage_bps != 0

    def test_token1_input_branch(self):
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ARB, token1_address=WETH_ARB,
            token0_symbol="USDC", token1_symbol="WETH",
            token0_decimals=6, token1_decimals=18,
        )
        # token1 (WETH) is input: amount1 > 0, amount0 < 0
        logs = [_make_swap_log(amount0=-1000 * 10**6, amount1=3 * 10**17)]
        result = parser.parse_receipt(_make_receipt(logs))
        assert result.swap_result is not None
        # token_in should be WETH
        assert result.swap_result.token_in == WETH_ARB.lower()
        assert result.swap_result.token_in_symbol == "WETH"

    def test_unresolved_decimals_returns_no_swap_result(self):
        # No decimals resolved -> _build_swap_result returns None
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        # Force the resolver lookup to fail
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            side_effect=Exception("nope"),
        ):
            logs = [_make_swap_log()]
            result = parser.parse_receipt(_make_receipt(logs))
            assert result.swap_result is None
            # But events still present
            assert len(result.swap_events) == 1

    def test_exception_during_parse_returns_failure(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        # Pass non-dict so .get raises
        result = parser.parse_receipt(None)  # type: ignore[arg-type]
        assert result.success is False
        assert result.error is not None

    def test_logs_with_unknown_topic_skipped(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        unknown_log = {
            "address": POOL_ADDRESS,
            "topics": ["0x" + "ee" * 32],
            "data": "0x",
        }
        result = parser.parse_receipt(_make_receipt([unknown_log]))
        assert result.success is True
        assert len(result.events) == 0


# --------------------------------------------------------------------------
# _parse_log direct
# --------------------------------------------------------------------------


class TestParseLog:
    def test_no_topics_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser._parse_log({"topics": [], "data": "0x"}, "tx", 0) is None

    def test_bytes_first_topic_normalized(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        sig = bytes.fromhex(EVENT_TOPICS["Swap"][2:])
        log = {
            "address": POOL_ADDRESS,
            "topics": [sig, _pad_addr(WALLET), _pad_addr(WALLET)],
            "data": "0x" + (
                _enc_int_signed(100)
                + _enc_int_signed(-50)
                + _enc_uint(2**96)
                + _enc_uint(10**12)
                + _enc_int_signed(0)
            ),
        }
        event = parser._parse_log(log, "tx", 1)
        assert event is not None
        assert event.event_type == SushiSwapV3EventType.SWAP

    def test_bytes_address_normalized(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = {
            "address": bytes.fromhex(POOL_ADDRESS[2:]),
            "topics": [EVENT_TOPICS["Swap"], _pad_addr(WALLET), _pad_addr(WALLET)],
            "data": "0x" + (
                _enc_int_signed(100) + _enc_int_signed(-50)
                + _enc_uint(2**96) + _enc_uint(10**12) + _enc_int_signed(0)
            ),
        }
        event = parser._parse_log(log, "tx", 1)
        assert event is not None
        # bytes → "0x" + hex().  bytes.hex() is lowercase, so the round-tripped
        # address must equal POOL_ADDRESS.lower().
        assert event.contract_address == POOL_ADDRESS.lower()

    def test_bytes_topics_in_topics_str_list(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        topic1_bytes = bytes.fromhex(_pad_addr(WALLET)[2:])
        log = {
            "address": POOL_ADDRESS,
            "topics": [EVENT_TOPICS["Swap"], topic1_bytes, _pad_addr(WALLET)],
            "data": "0x" + (
                _enc_int_signed(100) + _enc_int_signed(-50)
                + _enc_uint(2**96) + _enc_uint(10**12) + _enc_int_signed(0)
            ),
        }
        event = parser._parse_log(log, "tx", 1)
        assert event is not None
        # topics_str must contain string-form
        assert isinstance(event.raw_topics[1], str)

    def test_unknown_topic_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = {"address": POOL_ADDRESS, "topics": ["0xdead"], "data": "0x"}
        assert parser._parse_log(log, "tx", 0) is None


# --------------------------------------------------------------------------
# _decode_swap_data / _decode_transfer_data
# --------------------------------------------------------------------------


class TestDecodeData:
    def test_decode_swap_data_full(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        data = "0x" + (
            _enc_int_signed(1000)
            + _enc_int_signed(-500)
            + _enc_uint(2**96)
            + _enc_uint(10**12)
            + _enc_int_signed(42)
        )
        topics = [EVENT_TOPICS["Swap"], _pad_addr(WALLET), _pad_addr(WALLET)]
        result = parser._decode_swap_data(topics, data, POOL_ADDRESS)
        assert result["amount0"] == 1000
        assert result["amount1"] == -500
        assert result["tick"] == 42

    def test_decode_swap_data_invalid(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        # malformed data
        result = parser._decode_swap_data([], "not-hex", POOL_ADDRESS)
        # Should fail gracefully and return raw_data fallback
        assert "raw_data" in result

    def test_decode_swap_data_bytes_address(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        data = "0x" + (
            _enc_int_signed(1) + _enc_int_signed(-1)
            + _enc_uint(2**96) + _enc_uint(0) + _enc_int_signed(0)
        )
        topics = [EVENT_TOPICS["Swap"], _pad_addr(WALLET), _pad_addr(WALLET)]
        result = parser._decode_swap_data(
            topics, data, bytes.fromhex(POOL_ADDRESS[2:]),
        )
        # bytes address branch must produce the canonical lowercase hex form
        assert result["pool_address"] == POOL_ADDRESS.lower()

    def test_decode_transfer_data(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        data = "0x" + _enc_uint(12345)
        topics = [EVENT_TOPICS["Transfer"], _pad_addr(WALLET), _pad_addr(POOL_ADDRESS)]
        result = parser._decode_transfer_data(topics, data, USDC_ARB)
        assert result["value"] == 12345

    def test_decode_transfer_data_invalid(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser._decode_transfer_data([], "not-hex", USDC_ARB)
        assert "raw_data" in result

    def test_decode_transfer_data_bytes_address(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        data = "0x" + _enc_uint(5)
        topics = [EVENT_TOPICS["Transfer"], _pad_addr(WALLET), _pad_addr(POOL_ADDRESS)]
        result = parser._decode_transfer_data(
            topics, data, bytes.fromhex(USDC_ARB[2:]),
        )
        assert result["token_address"] == USDC_ARB.lower()


# --------------------------------------------------------------------------
# _parse_swap_event / _parse_transfer_event
# --------------------------------------------------------------------------


class TestParseTypedEvent:
    def test_parse_swap_event(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        event = SushiSwapV3Event(
            event_type=SushiSwapV3EventType.SWAP,
            event_name="Swap", log_index=0, transaction_hash="0xtx",
            block_number=1, contract_address=POOL_ADDRESS,
            data={
                "sender": "0xa", "recipient": "0xb",
                "amount0": 100, "amount1": -50,
                "sqrt_price_x96": 2**96, "liquidity": 10, "tick": 5,
                "pool_address": POOL_ADDRESS,
            },
        )
        result = parser._parse_swap_event(event)
        assert result is not None
        assert result.amount0 == 100
        assert result.tick == 5

    def test_parse_swap_event_falls_back_to_event_address(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        event = SushiSwapV3Event(
            event_type=SushiSwapV3EventType.SWAP, event_name="Swap",
            log_index=0, transaction_hash="0xtx", block_number=1,
            contract_address=POOL_ADDRESS, data={},
        )
        result = parser._parse_swap_event(event)
        assert result is not None
        # falls back to event.contract_address
        assert result.pool_address == POOL_ADDRESS

    def test_parse_transfer_event(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        event = SushiSwapV3Event(
            event_type=SushiSwapV3EventType.TRANSFER, event_name="Transfer",
            log_index=0, transaction_hash="", block_number=0,
            contract_address=USDC_ARB,
            data={"from_addr": "0xa", "to_addr": "0xb", "value": 100},
        )
        result = parser._parse_transfer_event(event)
        assert result is not None
        assert result.value == 100
        assert result.token_address == USDC_ARB

    def test_parse_swap_event_exception(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        # Non-event input causes attribute access to fail
        bad = MagicMock()
        bad.data = MagicMock()
        bad.data.get.side_effect = RuntimeError("boom")
        result = parser._parse_swap_event(bad)
        assert result is None


# --------------------------------------------------------------------------
# parse_logs
# --------------------------------------------------------------------------


class TestParseLogs:
    def test_parse_logs_returns_events(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        events = parser.parse_logs([_make_swap_log()])
        assert len(events) == 1
        assert events[0].event_type == SushiSwapV3EventType.SWAP

    def test_parse_logs_skips_unknown(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        events = parser.parse_logs([{"topics": ["0xdead"], "data": "0x", "address": ""}])
        assert events == []


# --------------------------------------------------------------------------
# extract_position_id
# --------------------------------------------------------------------------


class TestExtractPositionId:
    def test_success(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = _make_receipt([_make_erc721_mint_log()])
        assert parser.extract_position_id(receipt) == NFT_TOKEN_ID

    def test_no_logs_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_position_id({"logs": []}) is None

    def test_unknown_chain_fails_loud(self, caplog):
        """Unknown chain returns None AND emits a fail-loud WARNING.

        Earlier behaviour silently defaulted to the Arbitrum NPM address,
        which would mis-attribute mints on a real chain and silently break
        LP accounting. The task spec for the SushiSwap V3 LP extraction work
        (mirrors AGENTS.md "Fail-loud on unknown chains") inverted that to
        return None + WARN. The previous assertion of the silent-default
        behaviour is preserved here as a fail-loud regression test.
        """
        import logging

        parser = SushiSwapV3ReceiptParser(chain="unknown_chain")
        receipt = _make_receipt([_make_erc721_mint_log(chain="arbitrum")])
        with caplog.at_level(
            logging.WARNING,
            logger="almanak.framework.connectors.sushiswap_v3.receipt_parser",
        ):
            result = parser.extract_position_id(receipt)
        assert result is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("unknown_chain" in r.getMessage() for r in warnings), (
            f"Expected a WARNING naming 'unknown_chain' but got "
            f"{[r.getMessage() for r in warnings]!r}"
        )
        assert any(
            "SushiSwap V3 NPM not registered" in r.getMessage() for r in warnings
        ), "Expected the fail-loud 'SushiSwap V3 NPM not registered' phrasing"

    def test_wrong_contract_skipped(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_erc721_mint_log()
        log["address"] = "0x9999999999999999999999999999999999999999"
        assert parser.extract_position_id(_make_receipt([log])) is None

    def test_fewer_than_4_topics(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = {
            "address": POSITION_MANAGER_ADDRESSES["arbitrum"],
            "topics": [EVENT_TOPICS["Transfer"], ZERO_ADDRESS_PADDED, _pad_addr(WALLET)],
            "data": "0x" + _enc_uint(NFT_TOKEN_ID),
        }
        assert parser.extract_position_id(_make_receipt([log])) is None

    def test_non_transfer_event(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = {
            "address": POSITION_MANAGER_ADDRESSES["arbitrum"],
            "topics": ["0xdead", ZERO_ADDRESS_PADDED, _pad_addr(WALLET), "0x" + _enc_uint(1)],
            "data": "0x",
        }
        assert parser.extract_position_id(_make_receipt([log])) is None

    def test_non_zero_from_address(self):
        # from != 0x0 means it's a non-mint Transfer
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = {
            "address": POSITION_MANAGER_ADDRESSES["arbitrum"],
            "topics": [
                EVENT_TOPICS["Transfer"], _pad_addr(WALLET), _pad_addr(POOL_ADDRESS),
                "0x" + _enc_uint(NFT_TOKEN_ID),
            ],
            "data": "0x",
        }
        assert parser.extract_position_id(_make_receipt([log])) is None

    def test_bytes_topics(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = {
            "address": POSITION_MANAGER_ADDRESSES["arbitrum"],
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Transfer"][2:]),
                bytes.fromhex(ZERO_ADDRESS_PADDED[2:]),
                bytes.fromhex(_pad_addr(WALLET)[2:]),
                bytes.fromhex(_enc_uint(NFT_TOKEN_ID)),
            ],
            "data": "0x",
        }
        assert parser.extract_position_id(_make_receipt([log])) == NFT_TOKEN_ID

    def test_object_style_log(self):
        """Log without .get() - uses getattr fallback."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")

        class _LogObj:
            address = POSITION_MANAGER_ADDRESSES["arbitrum"]
            topics = [
                EVENT_TOPICS["Transfer"],
                ZERO_ADDRESS_PADDED,
                _pad_addr(WALLET),
                "0x" + _enc_uint(NFT_TOKEN_ID),
            ]

        # Call extract_position_id_from_logs which iterates logs
        receipt = {"logs": [_LogObj()]}
        assert parser.extract_position_id(receipt) == NFT_TOKEN_ID

    def test_bytes_address(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_erc721_mint_log()
        log["address"] = bytes.fromhex(POSITION_MANAGER_ADDRESSES["arbitrum"][2:])
        assert parser.extract_position_id(_make_receipt([log])) == NFT_TOKEN_ID

    def test_extract_position_id_from_logs_static(self):
        result = SushiSwapV3ReceiptParser.extract_position_id_from_logs(
            [_make_erc721_mint_log()],
            chain="arbitrum",
        )
        assert result == NFT_TOKEN_ID

    def test_exception_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        # Make logs raise on iteration
        bad_receipt = MagicMock()
        bad_receipt.get.side_effect = RuntimeError("boom")
        assert parser.extract_position_id(bad_receipt) is None


# --------------------------------------------------------------------------
# extract_tick_lower / extract_tick_upper / extract_liquidity
# --------------------------------------------------------------------------


class TestExtractTickLower:
    def test_success(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.extract_tick_lower(_make_receipt([_make_mint_log(tick_lower=-120)]))
        assert result == -120

    def test_no_logs(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_tick_lower({"logs": []}) is None

    def test_no_mint_event(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_tick_lower(_make_receipt([_make_swap_log()])) is None

    def test_fewer_than_4_topics(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_mint_log()
        log["topics"] = log["topics"][:3]
        assert parser.extract_tick_lower(_make_receipt([log])) is None

    def test_bytes_first_topic(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_mint_log(tick_lower=-300)
        log["topics"][0] = bytes.fromhex(EVENT_TOPICS["Mint"][2:])
        assert parser.extract_tick_lower(_make_receipt([log])) == -300

    def test_bytes_tick_topic(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_mint_log(tick_lower=-180)
        log["topics"][2] = bytes.fromhex(_enc_int_signed(-180))
        assert parser.extract_tick_lower(_make_receipt([log])) == -180

    def test_exception_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        bad = MagicMock()
        bad.get.side_effect = RuntimeError("boom")
        assert parser.extract_tick_lower(bad) is None


class TestExtractTickUpper:
    def test_success(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.extract_tick_upper(_make_receipt([_make_mint_log(tick_upper=240)]))
        assert result == 240

    def test_no_logs(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_tick_upper({"logs": []}) is None

    def test_no_mint_event(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_tick_upper(_make_receipt([_make_swap_log()])) is None

    def test_fewer_than_4_topics(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_mint_log()
        log["topics"] = log["topics"][:2]
        assert parser.extract_tick_upper(_make_receipt([log])) is None

    def test_bytes_first_topic(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_mint_log(tick_upper=480)
        log["topics"][0] = bytes.fromhex(EVENT_TOPICS["Mint"][2:])
        assert parser.extract_tick_upper(_make_receipt([log])) == 480

    def test_bytes_tick_topic(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_mint_log(tick_upper=600)
        log["topics"][3] = bytes.fromhex(_enc_int_signed(600))
        assert parser.extract_tick_upper(_make_receipt([log])) == 600

    def test_exception_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        bad = MagicMock()
        bad.get.side_effect = RuntimeError("boom")
        assert parser.extract_tick_upper(bad) is None


class TestExtractLiquidity:
    def test_success(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.extract_liquidity(_make_receipt([_make_mint_log(liquidity=10**15)]))
        assert result == 10**15

    def test_no_logs(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_liquidity({"logs": []}) is None

    def test_no_mint(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_liquidity(_make_receipt([_make_swap_log()])) is None

    def test_fewer_than_4_topics(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_mint_log()
        log["topics"] = log["topics"][:2]
        assert parser.extract_liquidity(_make_receipt([log])) is None

    def test_empty_data(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_mint_log()
        log["data"] = "0x"
        assert parser.extract_liquidity(_make_receipt([log])) is None

    def test_bytes_first_topic(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_mint_log(liquidity=42)
        log["topics"][0] = bytes.fromhex(EVENT_TOPICS["Mint"][2:])
        assert parser.extract_liquidity(_make_receipt([log])) == 42

    def test_exception_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        bad = MagicMock()
        bad.get.side_effect = RuntimeError("boom")
        assert parser.extract_liquidity(bad) is None


# --------------------------------------------------------------------------
# extract_lp_close_data
# --------------------------------------------------------------------------


class TestExtractLPCloseData:
    def test_collect_only(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.extract_lp_close_data(
            _make_receipt([_make_collect_log(amount0=100, amount1=200)])
        )
        assert result is not None
        assert result.amount0_collected == 100
        assert result.amount1_collected == 200
        # lp-close-may20.md §6.3: parser cannot disambiguate
        # LP_COLLECT_FEES / no-liquidity-but-owed (collect-only legitimately
        # means "whole transfer is fees") from split-tx LP_CLOSE collect leg
        # (principal lives in a sibling receipt). Parser emits its best
        # single-receipt answer (``fees = collect_amount``) and tags
        # ``source="collect"``. The aggregator overrides fees when a
        # ``decrease_liquidity`` sibling exists.
        assert result.fees0 == 100
        assert result.fees1 == 200
        assert result.source == "collect"
        # No burn -> liquidity_removed=None
        assert result.liquidity_removed is None

    def test_collect_plus_burn(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.extract_lp_close_data(
            _make_receipt([
                _make_burn_log(liquidity=10**12),
                _make_collect_log(amount0=100, amount1=200),
            ])
        )
        assert result is not None
        assert result.liquidity_removed == 10**12

    def test_multiple_collects_summed(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.extract_lp_close_data(
            _make_receipt([
                _make_collect_log(amount0=100, amount1=200),
                _make_collect_log(amount0=50, amount1=75),
            ])
        )
        assert result.amount0_collected == 150
        assert result.amount1_collected == 275

    def test_no_logs(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_lp_close_data({"logs": []}) is None

    def test_no_collect_or_burn(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_lp_close_data(_make_receipt([_make_swap_log()])) is None

    def test_no_topics_skipped(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        result = parser.extract_lp_close_data(_make_receipt([
            {"address": POOL_ADDRESS, "topics": [], "data": "0x"},
            _make_collect_log(amount0=10, amount1=20),
        ]))
        assert result is not None
        assert result.amount0_collected == 10

    def test_bytes_topic(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_collect_log(amount0=99, amount1=1)
        log["topics"][0] = bytes.fromhex(EVENT_TOPICS["Collect"][2:])
        result = parser.extract_lp_close_data(_make_receipt([log]))
        assert result.amount0_collected == 99

    def test_collect_with_fewer_than_4_topics(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _make_collect_log()
        log["topics"] = log["topics"][:2]
        # No effect but should not raise
        assert parser.extract_lp_close_data(_make_receipt([log])) is None

    def test_exception_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        bad = MagicMock()
        bad.get.side_effect = RuntimeError("boom")
        assert parser.extract_lp_close_data(bad) is None


# --------------------------------------------------------------------------
# extract_swap_amounts
# --------------------------------------------------------------------------


class TestExtractSwapAmounts:
    def test_success_with_swap_result(self):
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ARB, token1_address=WETH_ARB,
            token0_symbol="USDC", token1_symbol="WETH",
            token0_decimals=6, token1_decimals=18,
        )
        # Swap and Transfer events
        logs = [
            _make_transfer_log(token=USDC_ARB, from_addr=WALLET, to_addr=POOL_ADDRESS, value=1000 * 10**6),
            _make_swap_log(amount0=1000 * 10**6, amount1=-3 * 10**17),
            _make_transfer_log(token=WETH_ARB, from_addr=POOL_ADDRESS, to_addr=WALLET, value=3 * 10**17),
        ]
        # Force decimals resolution to succeed
        with patch.object(parser, "_resolve_decimals") as mock_resolve:
            mock_resolve.side_effect = lambda addr: 6 if addr.lower() == USDC_ARB.lower() else 18
            amounts = parser.extract_swap_amounts(_make_receipt(logs))
            assert amounts is not None
            assert amounts.amount_in == 1000 * 10**6
            assert amounts.amount_out == 3 * 10**17

    def test_with_expected_out_overrides_slippage(self):
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ARB, token1_address=WETH_ARB,
            token0_symbol="USDC", token1_symbol="WETH",
            token0_decimals=6, token1_decimals=18,
        )
        logs = [
            _make_transfer_log(token=USDC_ARB, from_addr=WALLET, to_addr=POOL_ADDRESS, value=1000 * 10**6),
            _make_swap_log(amount0=1000 * 10**6, amount1=-3 * 10**17),
            _make_transfer_log(token=WETH_ARB, from_addr=POOL_ADDRESS, to_addr=WALLET, value=3 * 10**17),
        ]
        with patch.object(parser, "_resolve_decimals") as mock_resolve:
            mock_resolve.side_effect = lambda addr: 6 if addr.lower() == USDC_ARB.lower() else 18
            amounts = parser.extract_swap_amounts(
                _make_receipt(logs),
                expected_out=Decimal("0.4"),  # higher than realized 0.3
            )
            assert amounts is not None
            # expected_out=0.4, realized=0.3 -> slippage = 25%
            assert amounts.slippage_bps == 2500

    def test_no_decimals_returns_none(self):
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ARB, token1_address=WETH_ARB,
            token0_symbol="USDC", token1_symbol="WETH",
            token0_decimals=6, token1_decimals=18,
        )
        logs = [_make_swap_log()]
        # Force decimals resolution to fail
        with patch.object(parser, "_resolve_decimals", return_value=None):
            assert parser.extract_swap_amounts(_make_receipt(logs)) is None

    def test_no_swap_events_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        # No logs at all
        assert parser.extract_swap_amounts({"logs": []}) is None

    def test_fallback_to_swap_events_no_swap_result(self):
        # No token0_decimals -> swap_result is None, but swap_events present
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _make_transfer_log(token=USDC_ARB, from_addr=WALLET, to_addr=POOL_ADDRESS, value=1000 * 10**6),
            _make_swap_log(amount0=1000 * 10**6, amount1=-3 * 10**17),
            _make_transfer_log(token=WETH_ARB, from_addr=POOL_ADDRESS, to_addr=WALLET, value=3 * 10**17),
        ]
        # Provide decimals via the inline resolver mock
        with patch.object(parser, "_resolve_decimals") as mock_resolve:
            mock_resolve.side_effect = lambda addr: 6 if addr.lower() == USDC_ARB.lower() else 18
            amounts = parser.extract_swap_amounts(_make_receipt(logs))
            # Even without symbols, falls back to swap_events branch
            assert amounts is not None
            assert amounts.amount_in == 1000 * 10**6

    def test_exception_returns_none(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        with patch.object(parser, "parse_receipt", side_effect=Exception("boom")):
            assert parser.extract_swap_amounts({"logs": []}) is None

    def test_transfer_topic_uppercase_prefix_still_extracts_tokens(self):
        # Regression for issue #2065 (Transfer-loop site): the
        # _extract_swap_tokens_from_transfers path also normalizes topic
        # case, so an uppercase '0X' Transfer topic must still be
        # recognised and yield the correct token_in / token_out addresses.
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        upper_transfer = "0X" + EVENT_TOPICS["Transfer"][2:].upper()
        # Hand-crafted Transfer logs with an uppercase '0X' prefix on topic[0].
        logs = [
            {
                "address": USDC_ARB,
                "topics": [upper_transfer, _pad_addr(WALLET), _pad_addr(POOL_ADDRESS)],
                "data": "0x" + _enc_uint(1000 * 10**6),
                "logIndex": 0,
            },
            _make_swap_log(amount0=1000 * 10**6, amount1=-3 * 10**17, log_index=1),
            {
                "address": WETH_ARB,
                "topics": [upper_transfer, _pad_addr(POOL_ADDRESS), _pad_addr(WALLET)],
                "data": "0x" + _enc_uint(3 * 10**17),
                "logIndex": 2,
            },
        ]
        token_in, token_out, amt_in, amt_out = parser._extract_swap_tokens_from_transfers(_make_receipt(logs))
        assert token_in.lower() == USDC_ARB.lower()
        assert token_out.lower() == WETH_ARB.lower()
        assert amt_in == 1000 * 10**6
        assert amt_out == 3 * 10**17


# --------------------------------------------------------------------------
# is_sushiswap_event / get_event_type
# --------------------------------------------------------------------------


class TestIsSushiswapEvent:
    def test_known_topic_str(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.is_sushiswap_event(EVENT_TOPICS["Swap"]) is True

    def test_unknown_topic(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.is_sushiswap_event("0x" + "ee" * 32) is False

    def test_bytes_topic(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.is_sushiswap_event(bytes.fromhex(EVENT_TOPICS["Swap"][2:])) is True

    def test_topic_no_prefix(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        no_prefix = EVENT_TOPICS["Swap"][2:]
        assert parser.is_sushiswap_event(no_prefix) is True

    def test_uppercase_with_prefix(self):
        # Mixed-case body with lowercase '0x' prefix matches.
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        mixed = "0x" + EVENT_TOPICS["Swap"][2:].upper()
        assert parser.is_sushiswap_event(mixed) is True

    def test_uppercase_0x_prefix(self):
        # Regression for issue #2065: uppercase '0X' prefix must not be
        # treated as missing-prefix and double-prefixed into '0x0x...'.
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        upper_prefix = "0X" + EVENT_TOPICS["Swap"][2:]
        assert parser.is_sushiswap_event(upper_prefix) is True

    def test_fully_uppercase(self):
        # Whole topic uppercased, including the '0X' prefix.
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.is_sushiswap_event(EVENT_TOPICS["Swap"].upper()) is True

    def test_bytes_topic_with_uppercase_hex_input(self):
        # Pin the contract that bytes inputs come back as lowercase hex
        # regardless of how the caller staged them. Constructing from an
        # uppercase hex string still yields the same lowercase '0x...'.
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        upper_hex = EVENT_TOPICS["Swap"][2:].upper()
        assert parser.is_sushiswap_event(bytes.fromhex(upper_hex)) is True


class TestGetEventType:
    def test_known(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.get_event_type(EVENT_TOPICS["Swap"]) == SushiSwapV3EventType.SWAP

    def test_unknown_returns_unknown(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.get_event_type("0xdead") == SushiSwapV3EventType.UNKNOWN

    def test_bytes(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.get_event_type(bytes.fromhex(EVENT_TOPICS["Mint"][2:])) == SushiSwapV3EventType.MINT

    def test_no_prefix(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        no_prefix = EVENT_TOPICS["Burn"][2:]
        assert parser.get_event_type(no_prefix) == SushiSwapV3EventType.BURN

    def test_uppercase_0x_prefix(self):
        # Regression for issue #2065: uppercase '0X' prefix must resolve
        # to the correct event type, not UNKNOWN.
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        upper_prefix = "0X" + EVENT_TOPICS["Mint"][2:]
        assert parser.get_event_type(upper_prefix) == SushiSwapV3EventType.MINT

    def test_fully_uppercase(self):
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.get_event_type(EVENT_TOPICS["Burn"].upper()) == SushiSwapV3EventType.BURN


# --------------------------------------------------------------------------
# Dataclasses to_dict / from_dict round trips
# --------------------------------------------------------------------------


class TestDataclassRoundTrips:
    def test_swap_event_data_to_dict(self):
        ed = SwapEventData(
            sender="0xa", recipient="0xb",
            amount0=100, amount1=-50,
            sqrt_price_x96=2**96, liquidity=10, tick=5,
            pool_address=POOL_ADDRESS,
        )
        d = ed.to_dict()
        assert d["amount0"] == "100"
        assert d["token0_is_input"] is True
        assert d["amount_in"] == "100"
        assert d["amount_out"] == "50"

    def test_swap_event_data_token1_input(self):
        ed = SwapEventData(
            sender="0xa", recipient="0xb",
            amount0=-100, amount1=50,
            sqrt_price_x96=0, liquidity=0, tick=0,
            pool_address="",
        )
        assert ed.token0_is_input is False
        assert ed.token1_is_input is True
        assert ed.amount_in == 50
        assert ed.amount_out == 100

    def test_swap_event_data_from_dict(self):
        d = {
            "sender": "0xa", "recipient": "0xb",
            "amount0": "100", "amount1": "-50",
            "sqrt_price_x96": "0", "liquidity": "0", "tick": "0",
            "pool_address": "",
        }
        ed = SwapEventData.from_dict(d)
        assert ed.amount0 == 100
        assert ed.amount1 == -50

    def test_transfer_event_data_to_dict(self):
        td = TransferEventData(from_addr="0xa", to_addr="0xb", value=100, token_address=USDC_ARB)
        d = td.to_dict()
        assert d["value"] == "100"

    def test_parsed_swap_result_round_trip(self):
        psr = ParsedSwapResult(
            token_in=USDC_ARB, token_out=WETH_ARB,
            token_in_symbol="USDC", token_out_symbol="WETH",
            amount_in=1000, amount_out=2000,
            amount_in_decimal=Decimal("1.0"), amount_out_decimal=Decimal("2.0"),
            effective_price=Decimal("2.0"), slippage_bps=50,
            pool_address=POOL_ADDRESS, sqrt_price_x96_after=2**96, tick_after=5,
        )
        d = psr.to_dict()
        psr2 = ParsedSwapResult.from_dict(d)
        assert psr2.amount_in == psr.amount_in
        assert psr2.token_in_symbol == "USDC"

    def test_parsed_swap_result_to_swap_result_payload(self):
        psr = ParsedSwapResult(
            token_in=USDC_ARB, token_out=WETH_ARB,
            token_in_symbol="USDC", token_out_symbol="WETH",
            amount_in=1, amount_out=2,
            amount_in_decimal=Decimal("1"), amount_out_decimal=Decimal("2"),
            effective_price=Decimal("2"), slippage_bps=10,
            pool_address=POOL_ADDRESS,
        )
        payload = psr.to_swap_result_payload()
        assert payload.token_in == "USDC"

    def test_parsed_swap_result_payload_falls_back_to_addr(self):
        psr = ParsedSwapResult(
            token_in=USDC_ARB, token_out=WETH_ARB,
            token_in_symbol="", token_out_symbol="",
            amount_in=1, amount_out=2,
            amount_in_decimal=Decimal("1"), amount_out_decimal=Decimal("2"),
            effective_price=Decimal("2"), slippage_bps=10,
            pool_address=POOL_ADDRESS,
        )
        payload = psr.to_swap_result_payload()
        assert payload.token_in == USDC_ARB

    def test_parse_result_to_dict_with_swap_result(self):
        psr = ParsedSwapResult(
            token_in=USDC_ARB, token_out=WETH_ARB,
            token_in_symbol="USDC", token_out_symbol="WETH",
            amount_in=1, amount_out=2,
            amount_in_decimal=Decimal("1"), amount_out_decimal=Decimal("2"),
            effective_price=Decimal("2"), slippage_bps=10,
            pool_address=POOL_ADDRESS,
        )
        pr = ParseResult(success=True, swap_result=psr)
        d = pr.to_dict()
        assert d["swap_result"]["token_in_symbol"] == "USDC"

    def test_parse_result_to_dict_no_swap_result(self):
        pr = ParseResult(success=True)
        d = pr.to_dict()
        assert d["swap_result"] is None

    def test_event_to_dict_and_from_dict(self):
        evt = SushiSwapV3Event(
            event_type=SushiSwapV3EventType.SWAP, event_name="Swap",
            log_index=1, transaction_hash="0xabc", block_number=10,
            contract_address=POOL_ADDRESS,
            data={"k": "v"}, raw_topics=["0xt1"], raw_data="0xd",
        )
        d = evt.to_dict()
        evt2 = SushiSwapV3Event.from_dict(d)
        assert evt2.event_type == SushiSwapV3EventType.SWAP
        assert evt2.contract_address == POOL_ADDRESS

    def test_event_from_dict_no_timestamp(self):
        d = {
            "event_type": "SWAP", "event_name": "Swap", "log_index": 1,
            "transaction_hash": "0xabc", "block_number": 10,
            "contract_address": POOL_ADDRESS, "data": {}, "raw_topics": [],
            "raw_data": "",
        }
        evt = SushiSwapV3Event.from_dict(d)
        assert isinstance(evt.timestamp, datetime)


class TestLegacyExports:
    def test_swap_event_topic_exported(self):
        assert SWAP_EVENT_TOPIC == EVENT_TOPICS["Swap"]
