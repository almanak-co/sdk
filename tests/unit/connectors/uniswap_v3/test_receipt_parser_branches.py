"""Branch coverage for the Uniswap V3 receipt parser.

Targets uncovered paths in ``receipt_parser.py``:
- ``parse_receipt`` happy / failed-tx / empty-logs / parser-crash branches.
- ``_parse_log`` topic normalization (bytes vs str, with / without 0x prefix).
- Bytes-encoded address / topic handling in ``_decode_swap_data`` /
  ``_decode_transfer_data``.
- ``parse_logs`` convenience wrapper.
- ``extract_position_id`` (with object-style logs, missing topic, malformed
  hex, wrong-from address) + the static class helper variant.
- ``extract_tick_lower`` / ``extract_tick_upper`` / ``extract_liquidity`` event-
  matching paths.
- ``is_uniswap_event`` / ``get_event_type`` topic-shape variants.
- ``UniswapV3Event.from_dict`` and ``ParsedSwapResult.from_dict`` /
  ``to_swap_result_payload`` round-trips.
- ``extract_lp_close_data`` ``current_tick`` branch when the receipt
  contains both Burn (carries pool_address) and a Swap event on the same
  pool.

These are pure-data tests — no network, no Web3, no token resolver.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.connectors.uniswap_v3.receipt_parser import (
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    POSITION_MANAGER_ADDRESSES,
    SWAP_EVENT_TOPIC,
    TOPIC_TO_EVENT,
    ParsedSwapResult,
    ParseResult,
    SwapEventData,
    TransferEventData,
    UniswapV3Event,
    UniswapV3EventType,
    UniswapV3ReceiptParser,
)
from almanak.framework.execution.events import SwapResultPayload
from almanak.framework.execution.extracted_data import LPCloseData

ARBITRUM_NPM = POSITION_MANAGER_ADDRESSES["arbitrum"].lower()
WETH_ADDR = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC_ADDR = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
POOL_ADDR = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
WALLET = "0x1111111111111111111111111111111111111111"
ROUTER = "0x2222222222222222222222222222222222222222"


def _pad32(addr: str) -> str:
    return "0x" + addr.removeprefix("0x").lower().rjust(64, "0")


def _enc_int24_topic(value: int) -> str:
    return f"0x{value & ((1 << 256) - 1):064x}"


def _make_swap_log(
    *,
    sender: str = ROUTER,
    recipient: str = WALLET,
    amount0: int,
    amount1: int,
    sqrt_price: int = 0,
    liquidity: int = 0,
    tick: int = 0,
    pool: str = POOL_ADDR,
) -> dict[str, Any]:
    """Build an ABI-faithful Pool Swap log."""

    def to_int256(v: int) -> int:
        return v & ((1 << 256) - 1)

    data = (
        f"{to_int256(amount0):064x}"
        f"{to_int256(amount1):064x}"
        f"{sqrt_price:064x}"
        f"{liquidity:064x}"
        f"{to_int256(tick):064x}"
    )
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Swap"],
            _pad32(sender),
            _pad32(recipient),
        ],
        "data": "0x" + data,
        "logIndex": 0,
    }


def _make_transfer_log(
    *,
    from_addr: str,
    to_addr: str,
    value: int,
    token: str,
    log_index: int = 1,
) -> dict[str, Any]:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            _pad32(from_addr),
            _pad32(to_addr),
        ],
        "data": "0x" + f"{value:064x}",
        "logIndex": log_index,
    }


class _ObjectLog:
    """Object-style log (mimics web3.AttributeDict-like)."""

    def __init__(self, address: str, topics: list[Any], data: str = "", log_index: int = 0) -> None:
        self.address = address
        self.topics = topics
        self.data = data
        self.logIndex = log_index


# ---------------------------------------------------------------------------
# Constants reflection
# ---------------------------------------------------------------------------


class TestModuleConstants:
    def test_topic_to_event_round_trip(self) -> None:
        for name, topic in EVENT_TOPICS.items():
            assert TOPIC_TO_EVENT[topic] == name

    def test_swap_event_topic_alias(self) -> None:
        assert SWAP_EVENT_TOPIC == EVENT_TOPICS["Swap"]

    def test_event_name_mapping_complete(self) -> None:
        for name in ["Swap", "Mint", "Burn", "Collect", "Flash", "Transfer", "Approval"]:
            assert name in EVENT_NAME_TO_TYPE


# ---------------------------------------------------------------------------
# Dataclass round-trips
# ---------------------------------------------------------------------------


class TestUniswapV3EventRoundTrip:
    def test_from_dict_then_to_dict(self) -> None:
        ev = UniswapV3Event(
            event_type=UniswapV3EventType.SWAP,
            event_name="Swap",
            log_index=3,
            transaction_hash="0xabc",
            block_number=42,
            contract_address=POOL_ADDR,
            data={"x": 1},
            raw_topics=["0xtopic"],
            raw_data="0xdeadbeef",
        )
        d = ev.to_dict()
        assert d["event_type"] == "SWAP"
        assert d["event_name"] == "Swap"
        # Round-trip
        ev2 = UniswapV3Event.from_dict(d)
        assert ev2.event_type == UniswapV3EventType.SWAP
        assert ev2.contract_address == POOL_ADDR
        assert ev2.raw_topics == ["0xtopic"]
        assert ev2.raw_data == "0xdeadbeef"


class TestSwapEventDataAccessors:
    @pytest.mark.parametrize(
        "amount0,amount1,exp_in,exp_out,exp_token0_in",
        [
            (100, -90, 100, 90, True),
            (-90, 100, 100, 90, False),
        ],
    )
    def test_amount_in_and_out(self, amount0: int, amount1: int, exp_in: int, exp_out: int, exp_token0_in: bool) -> None:
        ev = SwapEventData(
            sender=ROUTER,
            recipient=ROUTER,
            amount0=amount0,
            amount1=amount1,
            sqrt_price_x96=0,
            liquidity=0,
            tick=0,
            pool_address=POOL_ADDR,
        )
        assert ev.amount_in == exp_in
        assert ev.amount_out == exp_out
        assert ev.token0_is_input is exp_token0_in
        assert ev.token1_is_input is (not exp_token0_in)

    def test_to_dict_includes_computed_fields(self) -> None:
        ev = SwapEventData(
            sender=ROUTER,
            recipient=ROUTER,
            amount0=100,
            amount1=-90,
            sqrt_price_x96=1234,
            liquidity=5678,
            tick=42,
            pool_address=POOL_ADDR,
        )
        d = ev.to_dict()
        assert d["amount0"] == "100"
        assert d["amount1"] == "-90"
        assert d["amount_in"] == "100"
        assert d["amount_out"] == "90"
        assert d["token0_is_input"] is True

    def test_from_dict(self) -> None:
        d = {
            "sender": ROUTER,
            "recipient": WALLET,
            "amount0": "100",
            "amount1": "-90",
            "sqrt_price_x96": "1234",
            "liquidity": "5678",
            "tick": "42",
            "pool_address": POOL_ADDR,
        }
        ev = SwapEventData.from_dict(d)
        assert ev.amount0 == 100
        assert ev.amount1 == -90
        assert ev.tick == 42


class TestTransferEventDataDict:
    def test_to_dict(self) -> None:
        t = TransferEventData(
            from_addr=WALLET,
            to_addr=ROUTER,
            value=1000,
            token_address=WETH_ADDR,
        )
        d = t.to_dict()
        assert d["from_addr"] == WALLET
        assert d["value"] == "1000"


class TestParsedSwapResultRoundtrip:
    def test_to_dict_then_from_dict(self) -> None:
        sr = ParsedSwapResult(
            token_in=USDC_ADDR,
            token_out=WETH_ADDR,
            token_in_symbol="USDC",
            token_out_symbol="WETH",
            amount_in=100_000_000,
            amount_out=10**17,
            amount_in_decimal=Decimal("100"),
            amount_out_decimal=Decimal("0.1"),
            effective_price=Decimal("0.001"),
            slippage_bps=10,
            pool_address=POOL_ADDR,
            sqrt_price_x96_after=1234,
            tick_after=42,
        )
        d = sr.to_dict()
        sr2 = ParsedSwapResult.from_dict(d)
        assert sr2.amount_in == 100_000_000
        assert sr2.token_in_symbol == "USDC"
        assert sr2.tick_after == 42

    def test_to_swap_result_payload(self) -> None:
        sr = ParsedSwapResult(
            token_in=USDC_ADDR,
            token_out=WETH_ADDR,
            token_in_symbol="USDC",
            token_out_symbol="WETH",
            amount_in=100,
            amount_out=99,
            amount_in_decimal=Decimal("100"),
            amount_out_decimal=Decimal("0.099"),
            effective_price=Decimal("0.00099"),
            slippage_bps=20,
            pool_address=POOL_ADDR,
        )
        payload = sr.to_swap_result_payload()
        assert isinstance(payload, SwapResultPayload)
        assert payload.token_in == "USDC"
        assert payload.token_out == "WETH"
        assert payload.slippage_bps == 20

    def test_to_swap_result_payload_falls_back_to_address(self) -> None:
        sr = ParsedSwapResult(
            token_in=USDC_ADDR,
            token_out=WETH_ADDR,
            token_in_symbol="",
            token_out_symbol="",
            amount_in=100,
            amount_out=99,
            amount_in_decimal=Decimal("100"),
            amount_out_decimal=Decimal("0.099"),
            effective_price=Decimal("0.00099"),
            slippage_bps=0,
            pool_address=POOL_ADDR,
        )
        payload = sr.to_swap_result_payload()
        assert payload.token_in == USDC_ADDR
        assert payload.token_out == WETH_ADDR


class TestParseResultDict:
    def test_to_dict_with_swap_result(self) -> None:
        sr = ParsedSwapResult(
            token_in=USDC_ADDR,
            token_out=WETH_ADDR,
            token_in_symbol="USDC",
            token_out_symbol="WETH",
            amount_in=1,
            amount_out=1,
            amount_in_decimal=Decimal("1"),
            amount_out_decimal=Decimal("1"),
            effective_price=Decimal("1"),
            slippage_bps=0,
            pool_address=POOL_ADDR,
        )
        pr = ParseResult(success=True, swap_result=sr, transaction_hash="0xabc", block_number=100)
        d = pr.to_dict()
        assert d["success"] is True
        assert d["swap_result"] is not None
        assert d["transaction_hash"] == "0xabc"

    def test_to_dict_no_swap_result(self) -> None:
        pr = ParseResult(success=True)
        assert pr.to_dict()["swap_result"] is None


# ---------------------------------------------------------------------------
# parse_receipt branch coverage
# ---------------------------------------------------------------------------


class TestParseReceiptBranches:
    def test_empty_logs_returns_success(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        out = parser.parse_receipt({"logs": [], "transactionHash": "0xabc", "blockNumber": 1, "status": 1})
        assert out.success is True
        assert out.transaction_hash == "0xabc"
        assert out.block_number == 1

    def test_failed_tx_marks_success_false(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        out = parser.parse_receipt(
            {
                "logs": [_make_swap_log(amount0=100, amount1=-99)],
                "status": 0,
                "transactionHash": "0xabc",
            }
        )
        assert out.success is True
        assert out.transaction_success is False
        assert out.error == "Transaction reverted"

    def test_bytes_tx_hash_normalized(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        out = parser.parse_receipt(
            {
                "logs": [],
                "transactionHash": bytes.fromhex("ab" * 32),
                "status": 1,
            }
        )
        assert out.transaction_hash.startswith("0x")
        assert len(out.transaction_hash) == 66

    def test_swap_log_yields_swap_result(self) -> None:
        # Provide token info so build_swap_result skips resolver
        parser = UniswapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ADDR,
            token1_address=WETH_ADDR,
            token0_symbol="USDC",
            token1_symbol="WETH",
            token0_decimals=6,
            token1_decimals=18,
        )
        receipt = {
            "logs": [
                _make_swap_log(amount0=100_000_000, amount1=-(10**17)),
            ],
            "status": 1,
            "transactionHash": "0xabc",
            "blockNumber": 100,
        }
        out = parser.parse_receipt(receipt)
        assert out.success is True
        assert out.swap_result is not None
        assert out.swap_result.token_in_symbol == "USDC"

    def test_unknown_topic_logs_skipped(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        receipt = {
            "logs": [
                {"topics": ["0x" + "ff" * 32], "data": "0x", "address": POOL_ADDR},
            ],
            "status": 1,
        }
        out = parser.parse_receipt(receipt)
        assert out.success is True
        assert out.events == []

    def test_log_with_no_topics_skipped(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        receipt = {
            "logs": [{"topics": [], "data": "0x", "address": POOL_ADDR}],
            "status": 1,
        }
        out = parser.parse_receipt(receipt)
        assert out.events == []

    def test_object_style_log_handled(self) -> None:
        parser = UniswapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ADDR,
            token1_address=WETH_ADDR,
            token0_decimals=6,
            token1_decimals=18,
        )
        # parse_log uses log.get(...) — we need a dict-style log; provide
        # a bytes topic to exercise the bytes-normalisation branch instead.
        bytes_swap_log = {
            "address": POOL_ADDR,
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Swap"][2:]),
                bytes.fromhex(_pad32(ROUTER)[2:]),
                bytes.fromhex(_pad32(WALLET)[2:]),
            ],
            "data": bytes.fromhex(
                f"{100:064x}"
                f"{((1 << 256) - 99):064x}"  # -99
                f"{0:064x}{0:064x}{0:064x}"
            ),
            "logIndex": 0,
        }
        receipt = {"logs": [bytes_swap_log], "status": 1}
        out = parser.parse_receipt(receipt)
        assert out.success is True
        assert len(out.swap_events) == 1

    def test_parse_receipt_crash_returns_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A genuine crash inside parse_receipt is caught and reported as a
        failed ParseResult — the outer try/except in parse_receipt converts
        ``Exception`` into ``success=False`` with an error message."""
        parser = UniswapV3ReceiptParser(chain="arbitrum")

        def boom(_a: Any, _b: Any, _c: Any) -> Any:
            raise RuntimeError("induced parse crash")

        monkeypatch.setattr(parser, "_parse_log", boom)
        out = parser.parse_receipt({"logs": [{"topics": ["0xabc"], "data": "0x"}]})
        assert out.success is False
        assert "induced parse crash" in (out.error or "")


# ---------------------------------------------------------------------------
# parse_logs convenience
# ---------------------------------------------------------------------------


class TestParseLogsHelper:
    def test_returns_parsed_events(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        events = parser.parse_logs([_make_swap_log(amount0=10, amount1=-9)])
        assert len(events) == 1
        assert events[0].event_type == UniswapV3EventType.SWAP


# ---------------------------------------------------------------------------
# extract_position_id branches
# ---------------------------------------------------------------------------


def _make_npm_transfer_log(
    *,
    from_addr_padded: str,
    to_addr_padded: str,
    token_id: int,
    address: str = ARBITRUM_NPM,
) -> dict[str, Any]:
    """ERC-721 Transfer with all 3 indexed params (4 topics)."""
    return {
        "address": address,
        "topics": [
            EVENT_TOPICS["Transfer"],
            from_addr_padded,
            to_addr_padded,
            f"0x{token_id:064x}",
        ],
        "data": "0x",
    }


class TestExtractPositionId:
    def test_finds_position_id_from_npm_mint(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = _make_npm_transfer_log(
            from_addr_padded=_pad32("0x" + "00" * 20),
            to_addr_padded=_pad32(WALLET),
            token_id=42,
        )
        out = parser.extract_position_id({"logs": [log]})
        assert out == 42

    def test_skips_non_npm_logs(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = _make_npm_transfer_log(
            from_addr_padded=_pad32("0x" + "00" * 20),
            to_addr_padded=_pad32(WALLET),
            token_id=42,
            address=POOL_ADDR,  # not the NPM
        )
        assert parser.extract_position_id({"logs": [log]}) is None

    def test_skips_non_mint_transfers(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = _make_npm_transfer_log(
            from_addr_padded=_pad32(WALLET),  # not zero
            to_addr_padded=_pad32(ROUTER),
            token_id=42,
        )
        assert parser.extract_position_id({"logs": [log]}) is None

    def test_handles_object_style_logs(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = _ObjectLog(
            address=ARBITRUM_NPM,
            topics=[
                EVENT_TOPICS["Transfer"],
                _pad32("0x" + "00" * 20),
                _pad32(WALLET),
                f"0x{99:064x}",
            ],
        )
        assert parser.extract_position_id({"logs": [log]}) == 99

    def test_handles_bytes_topics(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = {
            "address": ARBITRUM_NPM,
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Transfer"][2:]),
                bytes.fromhex(_pad32("0x" + "00" * 20)[2:]),
                bytes.fromhex(_pad32(WALLET)[2:]),
                bytes.fromhex(f"{77:064x}"),
            ],
            "data": "0x",
        }
        assert parser.extract_position_id({"logs": [log]}) == 77

    def test_handles_bytes_address(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = {
            "address": bytes.fromhex(ARBITRUM_NPM[2:]),
            "topics": [
                EVENT_TOPICS["Transfer"],
                _pad32("0x" + "00" * 20),
                _pad32(WALLET),
                f"0x{55:064x}",
            ],
            "data": "0x",
        }
        assert parser.extract_position_id({"logs": [log]}) == 55

    def test_skips_logs_with_too_few_topics(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = {
            "address": ARBITRUM_NPM,
            "topics": [EVENT_TOPICS["Transfer"], _pad32("0x" + "00" * 20)],
            "data": "0x",
        }
        assert parser.extract_position_id({"logs": [log]}) is None

    def test_unsupported_chain_falls_back_to_default_npm(self) -> None:
        parser = UniswapV3ReceiptParser(chain="unsupported_chain_xyz")
        # Default fallback is the canonical Uniswap V3 NPM
        log = _make_npm_transfer_log(
            from_addr_padded=_pad32("0x" + "00" * 20),
            to_addr_padded=_pad32(WALLET),
            token_id=11,
            address="0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        )
        assert parser.extract_position_id({"logs": [log]}) == 11

    def test_empty_logs_returns_none(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_position_id({"logs": []}) is None
        assert parser.extract_position_id({}) is None

    def test_static_helper(self) -> None:
        log = _make_npm_transfer_log(
            from_addr_padded=_pad32("0x" + "00" * 20),
            to_addr_padded=_pad32(WALLET),
            token_id=123,
        )
        assert UniswapV3ReceiptParser.extract_position_id_from_logs([log], chain="arbitrum") == 123


# ---------------------------------------------------------------------------
# Tick / liquidity extraction
# ---------------------------------------------------------------------------


def _make_pool_mint_log_for_ticks(
    *,
    tick_lower: int,
    tick_upper: int,
    pool: str = POOL_ADDR,
    owner_padded: str | None = None,
    liquidity: int = 1000,
    amount0: int = 100,
    amount1: int = 200,
) -> dict[str, Any]:
    if owner_padded is None:
        owner_padded = _pad32(ARBITRUM_NPM)
    data = (
        f"{liquidity:064x}"  # uint128 amount
        f"{amount0:064x}"  # uint256 amount0
        f"{amount1:064x}"  # uint256 amount1
    )
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Mint"],
            owner_padded,
            _enc_int24_topic(tick_lower),
            _enc_int24_topic(tick_upper),
        ],
        "data": "0x" + data,
    }


class TestExtractTicks:
    def test_tick_lower_and_upper(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = _make_pool_mint_log_for_ticks(tick_lower=-100, tick_upper=100)
        receipt = {"logs": [log], "status": 1}
        assert parser.extract_tick_lower(receipt) == -100
        assert parser.extract_tick_upper(receipt) == 100

    def test_tick_returns_none_without_logs(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_tick_lower({"logs": []}) is None
        assert parser.extract_tick_upper({"logs": []}) is None

    def test_tick_skips_non_mint_topic(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = {"topics": [EVENT_TOPICS["Swap"], "0x", "0x", "0x"], "data": "0x"}
        assert parser.extract_tick_lower({"logs": [log]}) is None
        assert parser.extract_tick_upper({"logs": [log]}) is None

    def test_tick_skips_short_topics(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = {"topics": [EVENT_TOPICS["Mint"]], "data": "0x"}
        assert parser.extract_tick_lower({"logs": [log]}) is None
        assert parser.extract_tick_upper({"logs": [log]}) is None

    def test_tick_handles_bytes_topics(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = {
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Mint"][2:]),
                bytes.fromhex(_pad32(ARBITRUM_NPM)[2:]),
                bytes.fromhex(_enc_int24_topic(-50)[2:]),
                bytes.fromhex(_enc_int24_topic(50)[2:]),
            ],
            "data": "0x" + "00" * 96,
        }
        assert parser.extract_tick_lower({"logs": [log]}) == -50
        assert parser.extract_tick_upper({"logs": [log]}) == 50


class TestExtractLiquidity:
    def test_returns_liquidity(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = _make_pool_mint_log_for_ticks(tick_lower=-100, tick_upper=100, liquidity=987_654)
        receipt = {"logs": [log], "status": 1}
        assert parser.extract_liquidity(receipt) == 987_654

    def test_returns_none_without_logs(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_liquidity({"logs": []}) is None

    def test_skips_non_mint(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = {"topics": [EVENT_TOPICS["Swap"], "0x", "0x", "0x"], "data": "0x"}
        assert parser.extract_liquidity({"logs": [log]}) is None

    def test_skips_empty_data(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        log = {
            "topics": [
                EVENT_TOPICS["Mint"],
                _pad32(ARBITRUM_NPM),
                _enc_int24_topic(-100),
                _enc_int24_topic(100),
            ],
            "data": "0x",
        }
        assert parser.extract_liquidity({"logs": [log]}) is None


# ---------------------------------------------------------------------------
# is_uniswap_event / get_event_type
# ---------------------------------------------------------------------------


class TestEventTypeAccessors:
    def test_is_uniswap_event_with_str(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        assert parser.is_uniswap_event(EVENT_TOPICS["Swap"]) is True
        assert parser.is_uniswap_event("0x" + "ff" * 32) is False

    def test_is_uniswap_event_with_bytes(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        assert parser.is_uniswap_event(bytes.fromhex(EVENT_TOPICS["Swap"][2:])) is True

    def test_is_uniswap_event_without_prefix(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        topic_no_prefix = EVENT_TOPICS["Mint"][2:]
        assert parser.is_uniswap_event(topic_no_prefix) is True

    def test_get_event_type_returns_known(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        assert parser.get_event_type(EVENT_TOPICS["Swap"]) == UniswapV3EventType.SWAP

    def test_get_event_type_unknown_returns_unknown(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        assert parser.get_event_type("0x" + "ff" * 32) == UniswapV3EventType.UNKNOWN

    def test_get_event_type_with_bytes(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        result = parser.get_event_type(bytes.fromhex(EVENT_TOPICS["Burn"][2:]))
        assert result == UniswapV3EventType.BURN


# ---------------------------------------------------------------------------
# extract_lp_close_data: current_tick from Swap event in the same receipt
# ---------------------------------------------------------------------------


def _make_burn_log(
    *,
    pool: str,
    amount: int,
    amount0: int,
    amount1: int,
    log_index: int = 1,
) -> dict[str, Any]:
    data = f"{amount:064x}{amount0:064x}{amount1:064x}"
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Burn"],
            _pad32("0x" + "11" * 20),
            _enc_int24_topic(-100),
            _enc_int24_topic(100),
        ],
        "data": "0x" + data,
        "logIndex": log_index,
    }


class TestLpCloseCurrentTickFromSwap:
    def test_current_tick_from_swap_in_close_receipt(self) -> None:
        """When a Burn AND a Swap on the same pool both appear (multicall
        close that bundles a router swap), parser pulls current_tick from
        the Swap event."""
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        receipt = {
            "logs": [
                _make_burn_log(pool=POOL_ADDR, amount=1000, amount0=500, amount1=200),
                _make_swap_log(amount0=10, amount1=-9, tick=-12345, pool=POOL_ADDR),
            ],
            "status": 1,
        }
        out = parser.extract_lp_close_data(receipt)
        assert isinstance(out, LPCloseData)
        assert out.current_tick == -12345
        assert out.pool_address == POOL_ADDR.lower()

    def test_current_tick_none_without_swap(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        receipt = {
            "logs": [_make_burn_log(pool=POOL_ADDR, amount=1000, amount0=500, amount1=200)],
            "status": 1,
        }
        out = parser.extract_lp_close_data(receipt)
        assert out is not None
        assert out.current_tick is None

    def test_burn_with_bytes_address_normalised(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        # Provide a bytes address on the burn log to exercise the bytes-norm
        # branch in pool_address capture.
        burn = _make_burn_log(pool=POOL_ADDR, amount=1000, amount0=500, amount1=200)
        burn["address"] = bytes.fromhex(POOL_ADDR[2:])
        receipt = {"logs": [burn], "status": 1}
        out = parser.extract_lp_close_data(receipt)
        assert out is not None
        assert out.pool_address == POOL_ADDR.lower()


# ---------------------------------------------------------------------------
# extract_lp_open_data with bytes / object-style logs
# ---------------------------------------------------------------------------


class TestLpOpenObjectLogs:
    def test_handles_object_style_logs(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        # Provide both pool Mint and IncreaseLiquidity as objects
        token_id = 7
        liquidity = 100
        amount0 = 50
        amount1 = 60

        mint = _ObjectLog(
            address=POOL_ADDR,
            topics=[
                EVENT_TOPICS["Mint"],
                _pad32(ARBITRUM_NPM),
                _enc_int24_topic(-50),
                _enc_int24_topic(50),
            ],
            data="0x" + "00" * 96,
        )
        inc_liquidity = _ObjectLog(
            address=ARBITRUM_NPM,
            topics=[
                EVENT_TOPICS["IncreaseLiquidity"],
                f"0x{token_id:064x}",
            ],
            data="0x" + f"{liquidity:064x}{amount0:064x}{amount1:064x}",
        )
        out = parser.extract_lp_open_data({"logs": [mint, inc_liquidity]})
        assert out is not None
        assert out.position_id == token_id
        assert out.tick_lower == -50
        assert out.tick_upper == 50

    def test_extract_lp_open_with_bytes_topics_and_address(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        mint = {
            "address": bytes.fromhex(POOL_ADDR[2:]),
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Mint"][2:]),
                bytes.fromhex(_pad32(ARBITRUM_NPM)[2:]),
                bytes.fromhex(_enc_int24_topic(-30)[2:]),
                bytes.fromhex(_enc_int24_topic(30)[2:]),
            ],
            "data": "0x" + "00" * 96,
        }
        inc_log = {
            "address": bytes.fromhex(ARBITRUM_NPM[2:]),
            "topics": [
                bytes.fromhex(EVENT_TOPICS["IncreaseLiquidity"][2:]),
                bytes.fromhex(f"{99:064x}"),
            ],
            "data": "0x" + f"{500:064x}{10:064x}{20:064x}",
        }
        out = parser.extract_lp_open_data({"logs": [mint, inc_log]})
        assert out is not None
        assert out.position_id == 99
        assert out.tick_lower == -30
        assert out.tick_upper == 30


# ---------------------------------------------------------------------------
# extract_swap_amounts with explicit expected_out
# ---------------------------------------------------------------------------


class TestExtractSwapAmounts:
    def test_with_expected_out_computes_slippage(self) -> None:
        parser = UniswapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ADDR,
            token1_address=WETH_ADDR,
            token0_decimals=6,
            token1_decimals=18,
        )
        receipt = {
            "logs": [
                _make_transfer_log(from_addr=WALLET, to_addr=POOL_ADDR, value=100_000_000, token=USDC_ADDR),
                _make_transfer_log(
                    from_addr=POOL_ADDR, to_addr=WALLET, value=10**17, token=WETH_ADDR, log_index=2
                ),
                _make_swap_log(amount0=100_000_000, amount1=-(10**17)),
            ],
            "status": 1,
        }
        # expected_out higher than actual -> positive slippage_bps
        amounts = parser.extract_swap_amounts(receipt, expected_out=Decimal("0.105"))
        assert amounts is not None
        assert amounts.slippage_bps is not None
        assert amounts.slippage_bps > 0

    def test_without_expected_out_returns_amounts(self) -> None:
        parser = UniswapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ADDR,
            token1_address=WETH_ADDR,
            token0_decimals=6,
            token1_decimals=18,
        )
        receipt = {
            "logs": [
                _make_transfer_log(from_addr=WALLET, to_addr=POOL_ADDR, value=100_000_000, token=USDC_ADDR),
                _make_swap_log(amount0=100_000_000, amount1=-(10**17)),
            ],
            "status": 1,
        }
        amounts = parser.extract_swap_amounts(receipt)
        assert amounts is not None

    def test_no_swap_returns_none(self) -> None:
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_swap_amounts({"logs": [], "status": 1}) is None

    def test_uses_quoted_price_when_set(self) -> None:
        """When constructed with quoted_price, slippage_bps in ParsedSwapResult
        gets populated from the price differential."""
        parser = UniswapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ADDR,
            token1_address=WETH_ADDR,
            token0_decimals=6,
            token1_decimals=18,
            quoted_price=Decimal("0.0011"),  # quoted higher than realized
        )
        receipt = {
            "logs": [
                _make_swap_log(amount0=100_000_000, amount1=-(10**17)),
            ],
            "status": 1,
        }
        out = parser.parse_receipt(receipt)
        assert out.swap_result is not None
        # quoted=0.0011, realized=0.001 → ~909 bps slippage
        assert out.swap_result.slippage_bps > 0


# ---------------------------------------------------------------------------
# extract_swap_amounts_result with expected_out
# ---------------------------------------------------------------------------


class TestExtractResultExpectedOut:
    def test_swap_amounts_result_ok_with_expected_out(self) -> None:
        from almanak.framework.execution.extract_result import ExtractOk

        parser = UniswapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ADDR,
            token1_address=WETH_ADDR,
            token0_decimals=6,
            token1_decimals=18,
        )
        receipt = {
            "logs": [_make_swap_log(amount0=100_000_000, amount1=-(10**17))],
            "status": 1,
        }
        out = parser.extract_swap_amounts_result(receipt, expected_out=Decimal("0.10"))
        assert isinstance(out, ExtractOk)


# ---------------------------------------------------------------------------
# parser._strict_parse: surface a ParseResult-marked-failure
# ---------------------------------------------------------------------------


class TestStrictParseFailure:
    def test_extract_methods_propagate_strict_parse_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.execution.extract_result import ExtractError

        parser = UniswapV3ReceiptParser(chain="arbitrum")

        def crash(_receipt: dict[str, Any]) -> ParseResult:
            raise RuntimeError("strict parse boom")

        monkeypatch.setattr(parser, "parse_receipt", crash)
        out = parser.extract_position_id_result({"logs": [{"topics": []}]})
        assert isinstance(out, ExtractError)
        assert "strict parse boom" in out.error
