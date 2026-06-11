"""Tests for PancakeSwapV3ReceiptParser compiler-metadata hint threading (VIB-3164).

Verifies that swap decimal resolution uses compiler-supplied token metadata when
the TokenResolver misses, without changing fail-closed (output) / fail-soft
(input) semantics for cases where hints are also absent.

NOTE: There is NO direction fallback for this parser. When _pick_swap_raw_amounts
returns None (no wallet-matched transfers), there are no raw amounts to scale —
hints cannot rescue that path. The address-keyed hint map only helps once wallet
Transfers have classified token_in / token_out addresses.

Test structure mirrors tests/unit/connectors/uniswap_v3/test_receipt_parser_decimals.py.
The char test pins (test_unresolved_output_decimals_returns_none,
test_unresolved_input_decimals_returns_unresolved_amounts) are NOT duplicated
here — they live in test_extract_swap_amounts_char.py and must not be edited.
"""

from __future__ import annotations

import inspect
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors.pancakeswap_v3.receipt_parser import (
    EVENT_TOPICS,
    PancakeSwapV3ReceiptParser,
)

# ---------------------------------------------------------------------------
# Constants — fake addresses the real TokenResolver does NOT know
# ---------------------------------------------------------------------------

FAKE_TOKEN_IN_ADDR = "0x" + "a1" * 20   # 6-decimal token (USDC-like)
FAKE_TOKEN_IN_DECIMALS = 6
FAKE_TOKEN_IN_SYMBOL = "FAKE6"

FAKE_TOKEN_OUT_ADDR = "0x" + "b2" * 20  # 18-decimal token (WETH-like)
FAKE_TOKEN_OUT_DECIMALS = 18
FAKE_TOKEN_OUT_SYMBOL = "FAKE18"

FAKE_POOL_ADDR = "0x" + "c3" * 20
FAKE_WALLET_ADDR = "0x" + "d4" * 20
FAKE_ROUTER_ADDR = "0x" + "e5" * 20

SWAP_TOPIC = EVENT_TOPICS["Swap"]
TRANSFER_TOPIC = EVENT_TOPICS["Transfer"]


# ---------------------------------------------------------------------------
# Receipt-building helpers (reuse pattern from char test)
# ---------------------------------------------------------------------------


def _pad32(val: int, signed: bool = False) -> str:
    if signed and val < 0:
        val += 1 << 256
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _pcs_swap_log(
    amount0: int = 100_000_000,
    amount1: int = -1_079_340_000_000_000_000_000,
    pool: str = FAKE_POOL_ADDR,
    sender: str = FAKE_ROUTER_ADDR,
    recipient: str = FAKE_WALLET_ADDR,
    log_index: int = 1,
) -> dict:
    """PCS V3 Swap event: 9 params (includes protocol fees as uint128)."""
    data = (
        "0x"
        + _pad32(amount0, signed=True)
        + _pad32(amount1, signed=True)
        + _pad32(2**96)   # sqrtPriceX96
        + _pad32(10**18)  # liquidity
        + _pad32(0, signed=True)  # tick
        + _pad32(0)       # protocolFeesToken0
        + _pad32(0)       # protocolFeesToken1
    )
    return {
        "address": pool,
        "topics": [SWAP_TOPIC, _addr_topic(sender), _addr_topic(recipient)],
        "data": data,
        "logIndex": log_index,
    }


def _transfer_log(token: str, frm: str, to: str, amount: int, log_index: int = 0) -> dict:
    return {
        "address": token,
        "topics": [TRANSFER_TOPIC, _addr_topic(frm), _addr_topic(to)],
        "data": "0x" + _pad32(amount),
        "logIndex": log_index,
    }


def _receipt(logs: list[dict], wallet: str = FAKE_WALLET_ADDR) -> dict:
    return {
        "transactionHash": "0x" + "ee" * 32,
        "blockNumber": 100,
        "status": 1,
        "gasUsed": 150_000,
        "from": wallet,
        "logs": logs,
    }


def _make_full_receipt(
    amount_in: int = 100_000_000,
    amount_out: int = 1_079_340_000_000_000_000_000,
    token_in: str = FAKE_TOKEN_IN_ADDR,
    token_out: str = FAKE_TOKEN_OUT_ADDR,
    pool: str = FAKE_POOL_ADDR,
    wallet: str = FAKE_WALLET_ADDR,
) -> dict:
    """Full receipt with Transfer-in + PCS Swap + Transfer-out."""
    return _receipt(
        wallet=wallet,
        logs=[
            _transfer_log(token_in, wallet, pool, amount_in, log_index=0),
            _pcs_swap_log(amount0=amount_in, amount1=-amount_out, pool=pool),
            _transfer_log(token_out, pool, wallet, amount_out, log_index=2),
        ],
    )


# ---------------------------------------------------------------------------
# Parser factory — resolver always misses
# ---------------------------------------------------------------------------


def _make_parser(chain: str = "bnb") -> PancakeSwapV3ReceiptParser:
    with patch.object(PancakeSwapV3ReceiptParser, "_resolve_decimals", return_value=None):
        return PancakeSwapV3ReceiptParser(chain=chain)


# ---------------------------------------------------------------------------
# Metadata fixtures
# ---------------------------------------------------------------------------

_FAKE6_META = {
    "address": FAKE_TOKEN_IN_ADDR,
    "symbol": FAKE_TOKEN_IN_SYMBOL,
    "decimals": FAKE_TOKEN_IN_DECIMALS,
}
_FAKE18_META = {
    "address": FAKE_TOKEN_OUT_ADDR,
    "symbol": FAKE_TOKEN_OUT_SYMBOL,
    "decimals": FAKE_TOKEN_OUT_DECIMALS,
}
_FULL_META = {"token_in": _FAKE6_META, "token_out": _FAKE18_META}


# ---------------------------------------------------------------------------
# Case 1: hint resolves 6-decimal input token on resolver miss
# ---------------------------------------------------------------------------


class TestHintResolvesInputToken:
    def test_hint_resolves_input_on_resolver_miss(self):
        """Hint resolves 6-decimal input when resolver returns None -> full SwapAmounts."""
        parser = _make_parser()
        receipt = _make_full_receipt()

        with patch.object(parser, "_resolve_decimals", return_value=None):
            result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_in_decimal_resolved is True


# ---------------------------------------------------------------------------
# Case 2: hint resolves output token on resolver miss
# ---------------------------------------------------------------------------


class TestHintResolvesOutputToken:
    def test_hint_resolves_output_on_resolver_miss(self):
        """Hint resolves output token -> row is emitted instead of dropped."""
        parser = _make_parser()
        receipt = _make_full_receipt()

        with patch.object(parser, "_resolve_decimals", return_value=None):
            result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        assert result.amount_out_decimal == Decimal("1079.34")
        assert result.amount_out_decimal_resolved is True


# ---------------------------------------------------------------------------
# Case 3: fail-closed output preserved (no hints, output resolver miss)
# ---------------------------------------------------------------------------


class TestFailClosedOutputPreserved:
    """No hints, output resolver miss -> None (mirrors char test pin)."""

    def test_no_hints_output_miss_returns_none(self, caplog):
        import logging

        parser = _make_parser()
        receipt = _make_full_receipt()

        caplog.set_level(logging.WARNING)
        with patch.object(parser, "_resolve_decimals", return_value=None):
            result = parser.extract_swap_amounts(receipt)  # no swap_token_meta

        assert result is None
        assert any("output token decimals unknown" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Case 4: fail-soft input preserved (no hints, input resolver miss)
# ---------------------------------------------------------------------------


class TestFailSoftInputPreserved:
    """No hints, input resolver miss -> SwapAmounts with None/False flags (char pin)."""

    def test_no_hints_input_miss_returns_partial(self):
        parser = _make_parser()
        receipt = _make_full_receipt()

        def resolve_side(addr: str) -> int | None:
            return None if addr.lower() == FAKE_TOKEN_IN_ADDR.lower() else FAKE_TOKEN_OUT_DECIMALS

        with patch.object(parser, "_resolve_decimals", side_effect=resolve_side):
            result = parser.extract_swap_amounts(receipt)  # no swap_token_meta

        assert result is not None
        assert result.amount_in_decimal is None
        assert result.effective_price is None
        assert result.amount_in_decimal_resolved is False


# ---------------------------------------------------------------------------
# Case 5: address-mismatch skip
# ---------------------------------------------------------------------------


class TestAddressMismatchSkip:
    def test_hints_for_unrelated_addresses_do_not_alter_resolution(self):
        """Hints for C/D do not affect resolution of A/B from wallet transfers."""
        parser = _make_parser()

        other_in = "0x" + "e5" * 20
        other_out = "0x" + "f6" * 20
        amount_in = 50_000_000
        amount_out = 500 * 10**18

        receipt = _receipt(
            wallet=FAKE_WALLET_ADDR,
            logs=[
                _transfer_log(other_in, FAKE_WALLET_ADDR, FAKE_POOL_ADDR, amount_in, 0),
                _pcs_swap_log(amount0=amount_in, amount1=-amount_out),
                _transfer_log(other_out, FAKE_POOL_ADDR, FAKE_WALLET_ADDR, amount_out, 2),
            ],
        )

        # Hints are for FAKE_TOKEN_IN/OUT — different addresses
        # Resolver should be called for other_in / other_out, not hint addresses
        call_log: list[str] = []

        def mock_resolve(addr: str) -> int | None:
            call_log.append(addr.lower())
            return 6 if addr.lower() == other_in.lower() else 18

        with patch.object(parser, "_resolve_decimals", side_effect=mock_resolve):
            result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        # Resolver was called for the actual transfer addresses, not hint addresses
        assert any(a in (other_in.lower(), other_out.lower()) for a in call_log)
        # Hint addresses were NOT resolved by resolver (they were not in transfers)
        assert FAKE_TOKEN_IN_ADDR.lower() not in call_log
        assert FAKE_TOKEN_OUT_ADDR.lower() not in call_log


# ---------------------------------------------------------------------------
# Case 6: hint wins over resolver for same address
# ---------------------------------------------------------------------------


class TestHintWinsOverResolver:
    def test_hint_decimals_win(self):
        receipt = _make_full_receipt()
        parser = PancakeSwapV3ReceiptParser(chain="bnb")

        # Resolver claims 18 for both (wrong for FAKE_TOKEN_IN)
        with patch.object(parser, "_resolve_decimals", return_value=18):
            result_no_hint = parser.extract_swap_amounts(receipt)

        with patch.object(parser, "_resolve_decimals", return_value=18):
            result_with_hint = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        # Without hint, 6-decimal token treated as 18 -> wrong amounts
        assert result_no_hint is not None
        assert result_no_hint.amount_in_decimal != Decimal("100")

        # With hint, 6-decimal token is correct -> 100
        assert result_with_hint is not None
        assert result_with_hint.amount_in_decimal == Decimal("100")


# ---------------------------------------------------------------------------
# Case 7: hook shape + disjointness
# ---------------------------------------------------------------------------


class TestBuildExtractKwargsHook:
    def test_returns_swap_token_meta(self):
        parser = PancakeSwapV3ReceiptParser(chain="bnb")
        bundle_metadata = {
            "from_token": {
                "symbol": FAKE_TOKEN_IN_SYMBOL,
                "address": FAKE_TOKEN_IN_ADDR.upper(),
                "decimals": FAKE_TOKEN_IN_DECIMALS,
                "is_native": False,
            },
            "to_token": {
                "symbol": FAKE_TOKEN_OUT_SYMBOL,
                "address": FAKE_TOKEN_OUT_ADDR,
                "decimals": FAKE_TOKEN_OUT_DECIMALS,
                "is_native": False,
            },
        }
        kwargs = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle_metadata)
        assert kwargs == {
            "swap_token_meta": {
                "token_in": {
                    "address": FAKE_TOKEN_IN_ADDR.lower(),
                    "symbol": FAKE_TOKEN_IN_SYMBOL,
                    "decimals": FAKE_TOKEN_IN_DECIMALS,
                },
                "token_out": {
                    "address": FAKE_TOKEN_OUT_ADDR.lower(),
                    "symbol": FAKE_TOKEN_OUT_SYMBOL,
                    "decimals": FAKE_TOKEN_OUT_DECIMALS,
                },
            }
        }

    def test_skips_native_entries(self):
        parser = PancakeSwapV3ReceiptParser(chain="bnb")
        bundle_metadata = {
            "from_token": {
                "symbol": "BNB",
                "address": FAKE_TOKEN_IN_ADDR,
                "decimals": 18,
                "is_native": True,
            },
        }
        result = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle_metadata)
        assert result == {}

    def test_coerces_string_decimals(self):
        parser = PancakeSwapV3ReceiptParser(chain="bnb")
        bundle_metadata = {
            "from_token": {
                "symbol": FAKE_TOKEN_IN_SYMBOL,
                "address": FAKE_TOKEN_IN_ADDR,
                "decimals": "6",
                "is_native": False,
            },
        }
        kwargs = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle_metadata)
        assert "swap_token_meta" in kwargs
        assert kwargs["swap_token_meta"]["token_in"]["decimals"] == 6

    def test_returns_empty_for_other_fields(self):
        parser = PancakeSwapV3ReceiptParser(chain="bnb")
        bundle_metadata = {
            "from_token": {
                "symbol": FAKE_TOKEN_IN_SYMBOL,
                "address": FAKE_TOKEN_IN_ADDR,
                "decimals": 6,
                "is_native": False,
            },
        }
        result = parser.build_extract_kwargs(field="position_id", bundle_metadata=bundle_metadata)
        assert result == {}

    def test_must_not_return_framework_owned_kwarg(self):
        """expected_out must never appear in the returned dict."""
        parser = PancakeSwapV3ReceiptParser(chain="bnb")
        bundle_metadata = {
            "from_token": {
                "symbol": FAKE_TOKEN_IN_SYMBOL,
                "address": FAKE_TOKEN_IN_ADDR,
                "decimals": 6,
                "is_native": False,
            },
            "to_token": {
                "symbol": FAKE_TOKEN_OUT_SYMBOL,
                "address": FAKE_TOKEN_OUT_ADDR,
                "decimals": 18,
                "is_native": False,
            },
        }
        kwargs = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle_metadata)
        assert "expected_out" not in kwargs


# ---------------------------------------------------------------------------
# Case 8: signature guard
# ---------------------------------------------------------------------------


class TestSignatureGuard:
    def test_extract_swap_amounts_has_swap_token_meta(self):
        sig = inspect.signature(PancakeSwapV3ReceiptParser.extract_swap_amounts)
        assert "swap_token_meta" in sig.parameters
        param = sig.parameters["swap_token_meta"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY

    def test_extract_swap_amounts_has_expected_out(self):
        sig = inspect.signature(PancakeSwapV3ReceiptParser.extract_swap_amounts)
        assert "expected_out" in sig.parameters
        param = sig.parameters["expected_out"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY


# ---------------------------------------------------------------------------
# Case 9: expected_out + swap_token_meta together
# ---------------------------------------------------------------------------


class TestEndToEndWithExpectedOut:
    def test_hints_and_expected_out_together(self):
        parser = PancakeSwapV3ReceiptParser(chain="bnb")
        receipt = _make_full_receipt(
            amount_in=100_000_000,
            amount_out=1_079_340_000_000_000_000_000,
        )

        with patch.object(parser, "_resolve_decimals", return_value=None):
            result = parser.extract_swap_amounts(
                receipt,
                expected_out=Decimal("1100"),
                swap_token_meta=_FULL_META,
            )

        assert result is not None
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        assert result.slippage_bps is not None
        # (1100 - 1079.34) / 1100 * 10000 ≈ 188 bps
        assert 180 < result.slippage_bps < 200
