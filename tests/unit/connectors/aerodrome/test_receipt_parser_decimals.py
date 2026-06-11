"""Tests for AerodromeReceiptParser / AerodromeSlipstreamReceiptParser
compiler-metadata hint threading (VIB-3164).

Verifies that swap decimal resolution uses compiler-supplied token metadata when
the TokenResolver misses, without changing fail-closed semantics for cases where
hints are also absent.

Test structure mirrors tests/unit/connectors/uniswap_v3/test_receipt_parser_decimals.py.
The ``test_unresolved_decimals_returns_none`` pin from char test line 346 is the
sentinel for the no-hint fail-closed path; do NOT modify it here.
"""

from __future__ import annotations

import inspect
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors.aerodrome.receipt_parser import (
    EVENT_TOPICS,
    AerodromeReceiptParser,
    AerodromeSlipstreamReceiptParser,
)
from almanak.framework.execution.extract_result import ExtractOk

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

V1_SWAP_TOPIC = EVENT_TOPICS["Swap"]
CL_SWAP_TOPIC = EVENT_TOPICS["SwapCL"]
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


def _transfer_log(token: str, frm: str, to: str, amount: int, log_index: int = 0) -> dict:
    return {
        "address": token,
        "topics": [TRANSFER_TOPIC, _addr_topic(frm), _addr_topic(to)],
        "data": "0x" + _pad32(amount),
        "logIndex": log_index,
    }


def _v1_swap_log(
    amount0_in: int = 0,
    amount1_in: int = 0,
    amount0_out: int = 0,
    amount1_out: int = 0,
    pool: str = FAKE_POOL_ADDR,
    log_index: int = 1,
) -> dict:
    data = "0x" + _pad32(amount0_in) + _pad32(amount1_in) + _pad32(amount0_out) + _pad32(amount1_out)
    return {
        "address": pool,
        "topics": [V1_SWAP_TOPIC, _addr_topic(FAKE_WALLET_ADDR), _addr_topic(FAKE_WALLET_ADDR)],
        "data": data,
        "logIndex": log_index,
    }


def _cl_swap_log(
    amount0: int = 100_000_000,
    amount1: int = -1_079_340_000_000_000_000_000,
    pool: str = FAKE_POOL_ADDR,
    log_index: int = 1,
) -> dict:
    data = (
        "0x"
        + _pad32(amount0, signed=True)
        + _pad32(amount1, signed=True)
        + _pad32(2**96)
        + _pad32(10**18)
        + _pad32(0, signed=True)
    )
    return {
        "address": pool,
        "topics": [CL_SWAP_TOPIC, _addr_topic(FAKE_WALLET_ADDR), _addr_topic(FAKE_WALLET_ADDR)],
        "data": data,
        "logIndex": log_index,
    }


def _receipt(logs: list[dict], wallet: str = FAKE_WALLET_ADDR) -> dict:
    return {
        "transactionHash": "0x" + "ee" * 32,
        "status": 1,
        "blockNumber": 100,
        "gasUsed": 200_000,
        "from": wallet,
        "logs": logs,
    }


def _make_v1_receipt(
    amount_in: int = 100_000_000,
    amount_out: int = 1_079_340_000_000_000_000_000,
    token_in: str = FAKE_TOKEN_IN_ADDR,
    token_out: str = FAKE_TOKEN_OUT_ADDR,
    pool: str = FAKE_POOL_ADDR,
    wallet: str = FAKE_WALLET_ADDR,
) -> dict:
    """V1 receipt: Transfer-in + V1 Swap + Transfer-out."""
    return _receipt(
        wallet=wallet,
        logs=[
            _transfer_log(token_in, wallet, pool, amount_in, log_index=0),
            _v1_swap_log(amount0_in=amount_in, amount1_out=amount_out, pool=pool),
            _transfer_log(token_out, pool, wallet, amount_out, log_index=2),
        ],
    )


def _make_cl_receipt(
    amount_in: int = 100_000_000,
    amount_out: int = 1_079_340_000_000_000_000_000,
    token_in: str = FAKE_TOKEN_IN_ADDR,
    token_out: str = FAKE_TOKEN_OUT_ADDR,
    pool: str = FAKE_POOL_ADDR,
    wallet: str = FAKE_WALLET_ADDR,
) -> dict:
    """CL/Slipstream receipt: Transfer-in + CL Swap + Transfer-out."""
    return _receipt(
        wallet=wallet,
        logs=[
            _transfer_log(token_in, wallet, pool, amount_in, log_index=0),
            _cl_swap_log(amount0=amount_in, amount1=-amount_out, pool=pool),
            _transfer_log(token_out, pool, wallet, amount_out, log_index=2),
        ],
    )


# ---------------------------------------------------------------------------
# Parser factory — resolver always misses
# ---------------------------------------------------------------------------


def _make_parser(chain: str = "base") -> AerodromeReceiptParser:
    with patch(
        "almanak.connectors.aerodrome.receipt_parser.AerodromeReceiptParser._resolve_token_info",
        return_value=("", None),
    ):
        return AerodromeReceiptParser(chain=chain)


def _make_slipstream_parser(chain: str = "base") -> AerodromeSlipstreamReceiptParser:
    with patch(
        "almanak.connectors.aerodrome.receipt_parser.AerodromeSlipstreamReceiptParser._resolve_token_info",
        return_value=("", None),
    ):
        return AerodromeSlipstreamReceiptParser(chain=chain)


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
# Case 1: hints resolve 6-decimal token on resolver miss (V1 receipt)
# ---------------------------------------------------------------------------


class TestHintResolvesSixDecimalToken:
    def test_v1_receipt_resolver_miss_hints_provide_decimals(self):
        parser = _make_parser()
        receipt = _make_v1_receipt()
        result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        assert result.amount_in_decimal_resolved is True
        assert result.amount_out_decimal_resolved is True


# ---------------------------------------------------------------------------
# Case 2: direction fallback when transfers unclassifiable
# ---------------------------------------------------------------------------


class TestDirectionFallback:
    """Single-swap receipts with no wallet-matched transfers resolve via hint slots."""

    def test_direction_fallback_single_swap_v1(self):
        """No wallet in transfers + single V1 Swap + hints -> resolves via fallback."""
        parser = _make_parser()
        receipt = _receipt(
            logs=[_v1_swap_log(amount0_in=100_000_000, amount1_out=1_079_340_000_000_000_000_000)],
            wallet=FAKE_WALLET_ADDR,
        )
        result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)
        assert result is not None
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")

    # Case 3: multi-swap gate
    def test_direction_fallback_skipped_for_multi_swap(self):
        """Two Swap events -> direction fallback does NOT fire; returns None (fail-closed)."""
        parser = _make_parser()
        receipt = _receipt(
            logs=[
                _v1_swap_log(amount0_in=100_000_000, amount1_out=10**18, log_index=0),
                _v1_swap_log(amount1_in=10**18, amount0_out=50_000_000, log_index=2),
            ],
            wallet=FAKE_WALLET_ADDR,
        )
        result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)
        # No wallet-matched transfers; two Swap events -> no direction fallback -> None
        assert result is None


# ---------------------------------------------------------------------------
# Case 4: address-mismatch skip
# ---------------------------------------------------------------------------


class TestAddressMismatchSkip:
    def test_hint_for_wrong_address_is_ignored(self):
        """Transfers classify A/B; hints carry C/D -> hints ignored; resolver used for A/B."""
        parser = _make_parser()

        other_in = "0x" + "e5" * 20
        other_out = "0x" + "f6" * 20
        amount_in = 50_000_000
        amount_out = 500 * 10**18

        receipt = _receipt(
            wallet=FAKE_WALLET_ADDR,
            logs=[
                _transfer_log(other_in, FAKE_WALLET_ADDR, FAKE_POOL_ADDR, amount_in, 0),
                _v1_swap_log(amount0_in=amount_in, amount1_out=amount_out),
                _transfer_log(other_out, FAKE_POOL_ADDR, FAKE_WALLET_ADDR, amount_out, 2),
            ],
        )

        with patch.object(parser, "_resolve_decimals") as mock_resolve:
            mock_resolve.side_effect = lambda addr: (6 if addr == other_in.lower() else 18)
            result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        called_addrs = [call.args[0] for call in mock_resolve.call_args_list]
        # Resolver was called for the actual transfer addresses, not the hint addresses
        assert any(a in (other_in.lower(), other_out.lower()) for a in called_addrs)


# ---------------------------------------------------------------------------
# Case 5: hint wins over resolver for same address
# ---------------------------------------------------------------------------


class TestHintWinsOverResolver:
    def test_hint_decimals_win(self):
        receipt = _make_v1_receipt()
        # Resolver claims 18 (wrong); hint says 6 (correct)
        with patch(
            "almanak.connectors.aerodrome.receipt_parser.AerodromeReceiptParser._resolve_token_info",
            return_value=("WRONG", 18),
        ):
            parser = AerodromeReceiptParser(chain="base")

        with patch.object(parser, "_resolve_decimals", return_value=18):
            result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        assert result.amount_in_decimal == Decimal("100")


# ---------------------------------------------------------------------------
# Case 6: no-hint fallback preserves fail-closed behaviour
# ---------------------------------------------------------------------------


class TestFallbackPreservesFailClosed:
    def test_no_hints_resolver_miss_returns_none(self, caplog):
        import logging

        parser = _make_parser()
        receipt = _make_v1_receipt()

        caplog.set_level(logging.WARNING)
        result = parser.extract_swap_amounts(receipt)  # no swap_token_meta

        assert result is None
        assert any("Cannot compute swap amounts" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Case 7: hook shape + disjointness
# ---------------------------------------------------------------------------


class TestBuildExtractKwargsHook:
    def test_returns_swap_token_meta(self):
        parser = _make_parser()
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
        parser = _make_parser()
        bundle_metadata = {
            "from_token": {
                "symbol": "ETH",
                "address": FAKE_TOKEN_IN_ADDR,
                "decimals": 18,
                "is_native": True,
            },
        }
        result = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle_metadata)
        assert result == {}

    def test_coerces_string_decimals(self):
        parser = _make_parser()
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
        parser = _make_parser()
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
        parser = _make_parser()
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
# Case 8: signature guards
# ---------------------------------------------------------------------------


class TestSignatureGuards:
    def test_extract_swap_amounts_result_has_swap_token_meta(self):
        sig = inspect.signature(AerodromeReceiptParser.extract_swap_amounts_result)
        assert "swap_token_meta" in sig.parameters
        param = sig.parameters["swap_token_meta"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY

    def test_extract_swap_amounts_has_swap_token_meta(self):
        sig = inspect.signature(AerodromeReceiptParser.extract_swap_amounts)
        assert "swap_token_meta" in sig.parameters
        param = sig.parameters["swap_token_meta"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY

    def test_extract_swap_amounts_has_expected_out(self):
        sig = inspect.signature(AerodromeReceiptParser.extract_swap_amounts)
        assert "expected_out" in sig.parameters


# ---------------------------------------------------------------------------
# _result-variant signature guard + end-to-end via extract_swap_amounts_result
# ---------------------------------------------------------------------------


class TestExtractSwapAmountsResultVariant:
    """The enricher calls extract_swap_amounts_result — it must accept swap_token_meta."""

    def test_result_variant_resolves_via_hints(self):
        """extract_swap_amounts_result(receipt, swap_token_meta=...) -> ExtractOk with hints."""
        parser = _make_parser()
        receipt = _make_v1_receipt()

        result = parser.extract_swap_amounts_result(receipt, swap_token_meta=_FULL_META)

        assert isinstance(result, ExtractOk)
        value = result.value
        assert value.amount_in_decimal == Decimal("100")
        assert value.amount_out_decimal == Decimal("1079.34")

    def test_slipstream_free_rider_inherits_fix(self):
        """AerodromeSlipstreamReceiptParser inherits the fix (free rider)."""
        parser = _make_slipstream_parser()
        # Use a CL receipt for Slipstream
        receipt = _make_cl_receipt()

        result = parser.extract_swap_amounts_result(receipt, swap_token_meta=_FULL_META)

        assert isinstance(result, ExtractOk)
        value = result.value
        assert value.amount_in_decimal == Decimal("100")
        assert value.amount_out_decimal == Decimal("1079.34")


# ---------------------------------------------------------------------------
# Case 9: expected_out + swap_token_meta together
# ---------------------------------------------------------------------------


class TestEndToEndWithExpectedOut:
    def test_hints_and_expected_out_together(self):
        parser = _make_parser()
        receipt = _make_v1_receipt(
            amount_in=100_000_000,
            amount_out=1_079_340_000_000_000_000_000,
        )
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
