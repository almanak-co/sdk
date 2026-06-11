"""Tests for UniswapV4ReceiptParser compiler-metadata hint threading (VIB-3164).

Verifies that swap decimal resolution uses compiler-supplied token metadata when
the TokenResolver misses, with the existing Decimal(0) + flags-False fallback
preserved exactly when hints are also absent.

NOTE: The Decimal(0) coercion (issue #1778 guardrail) must NOT be converted to
None — the ParsedSwapResult fields are typed as Decimal, not Decimal | None.
Flags (amount_in_decimal_resolved / amount_out_decimal_resolved) are the
signal for unresolved cases.

Test structure mirrors tests/unit/connectors/uniswap_v3/test_receipt_parser_decimals.py.
"""

from __future__ import annotations

import inspect
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
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

# The V4 PoolManager for Arbitrum
POOL_MANAGER_ARB = "0x360e68faccca8ca495c1b759fd9d707a1f2f8a0c"
FAKE_SENDER = "0x" + "cc" * 20
FAKE_POOL_ID = "0x" + "ab" * 32


# ---------------------------------------------------------------------------
# Receipt-building helpers (reuse pattern from existing V4 tests)
# ---------------------------------------------------------------------------


def _encode_int128(value: int) -> str:
    if value < 0:
        value = (1 << 256) + value
    return hex(value)[2:].zfill(64)


def _encode_uint(value: int) -> str:
    return hex(value)[2:].zfill(64)


def _build_swap_log(
    amount0: int = -100_000_000,           # swapper PAYS token0 (6-decimal in)
    amount1: int = 1_079_340_000_000_000_000_000,   # swapper RECEIVES token1 (18-decimal out)
    pool_id: str = FAKE_POOL_ID,
    sender: str = FAKE_SENDER,
) -> dict:
    data = (
        "0x"
        + _encode_int128(amount0)
        + _encode_int128(amount1)
        + _encode_uint(79228162514264337593543950336)  # sqrtPriceX96
        + _encode_uint(10**18)                         # liquidity
        + _encode_int128(0)                             # tick
        + _encode_uint(3000)                            # fee
    )
    return {
        "address": POOL_MANAGER_ARB,
        "topics": [EVENT_TOPICS["Swap"], pool_id, sender],
        "data": data,
    }


def _build_transfer_log(
    token: str,
    from_addr: str,
    to_addr: str,
    amount: int,
) -> dict:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + from_addr.replace("0x", "").zfill(64),
            "0x" + to_addr.replace("0x", "").zfill(64),
        ],
        "data": "0x" + _encode_uint(amount),
    }


def _make_receipt_with_transfers(
    amount_in: int = 100_000_000,
    amount_out: int = 1_079_340_000_000_000_000_000,
    token_in: str = FAKE_TOKEN_IN_ADDR,
    token_out: str = FAKE_TOKEN_OUT_ADDR,
) -> dict:
    """Receipt with Swap + Transfer-to-PoolManager + Transfer-from-PoolManager.

    V4 sign convention: swapper PAYS token_in (amount0 negative),
    RECEIVES token_out (amount1 positive).
    """
    return {
        "logs": [
            _build_swap_log(amount0=-amount_in, amount1=amount_out),
            # token_in -> PoolManager (input side)
            _build_transfer_log(token_in, FAKE_SENDER, POOL_MANAGER_ARB, amount_in),
            # token_out <- PoolManager (output side)
            _build_transfer_log(token_out, POOL_MANAGER_ARB, FAKE_SENDER, amount_out),
        ]
    }


def _make_receipt_swap_only(
    amount_in: int = 100_000_000,
    amount_out: int = 1_079_340_000_000_000_000_000,
) -> dict:
    """Receipt with only a Swap log — no Transfer events.

    Uses the V4 sign convention: amount0 negative = swapper pays token0,
    amount1 positive = swapper receives token1.
    """
    return {"logs": [_build_swap_log(amount0=-amount_in, amount1=amount_out)]}


# ---------------------------------------------------------------------------
# Parser factory — resolver always fails for fake addresses
# ---------------------------------------------------------------------------


def _make_failing_resolver() -> MagicMock:
    """Return a mock TokenResolver that raises for any address."""
    mock = MagicMock()
    mock.resolve.side_effect = Exception("unknown token")
    return mock


def _make_parser() -> UniswapV4ReceiptParser:
    return UniswapV4ReceiptParser(
        chain="arbitrum",
        token_resolver=_make_failing_resolver(),
    )


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
# Case 1: hints resolve 6-decimal token on resolver miss
# ---------------------------------------------------------------------------


class TestHintResolvesSixDecimalToken:
    def test_resolver_miss_hints_provide_decimals(self):
        """Parser resolves correct amounts via hints when resolver returns None."""
        parser = _make_parser()
        receipt = _make_receipt_with_transfers()

        result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        assert result.amount_in_decimal_resolved is True
        assert result.amount_out_decimal_resolved is True


# ---------------------------------------------------------------------------
# Case 2: direction fallback when _identify_swap_tokens yields None
# ---------------------------------------------------------------------------


class TestDirectionFallback:
    """No Transfer events -> _identify_swap_tokens returns None/None -> hints fill gaps."""

    def test_direction_fallback_single_swap(self):
        """No transfers + single Swap + hints -> resolved with flags True."""
        parser = _make_parser()
        receipt = _make_receipt_swap_only()

        result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        assert result.amount_in_decimal_resolved is True
        assert result.amount_out_decimal_resolved is True

    # Case 3: multi-swap gate
    def test_direction_fallback_skipped_for_multi_swap(self):
        """Two Swap events -> direction fallback does NOT fire; Decimal(0)+flags False."""
        parser = _make_parser()
        receipt = {
            "logs": [
                _build_swap_log(amount0=-100_000_000, amount1=1_079_340_000_000_000_000_000),
                _build_swap_log(amount0=-50_000_000, amount1=500_000_000_000_000_000_000),
            ]
        }
        result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        # Two Swap events: direction fallback not applied; resolver also misses
        assert result is not None
        # Addresses remain None; decimals unresolved -> Decimal(0) + flags False
        assert result.amount_in_decimal == Decimal(0)
        assert result.amount_in_decimal_resolved is False


# ---------------------------------------------------------------------------
# Case 4: address-mismatch skip
# ---------------------------------------------------------------------------


class TestAddressMismatchSkip:
    """Identified address differs from hint address -> hint not applied."""

    def test_hint_not_applied_when_address_mismatch(self):
        """Transfers classify A/B; hints carry C/D -> hints do not override decimals for A/B."""
        parser = _make_parser()

        other_in = "0x" + "e5" * 20
        other_out = "0x" + "f6" * 20
        amount_in = 50_000_000
        amount_out = 500 * 10**18

        receipt = {
            "logs": [
                # V4 sign convention: swapper PAYS other_in (amount0<0), RECEIVES other_out (amount1>0)
                _build_swap_log(amount0=-amount_in, amount1=amount_out),
                _build_transfer_log(other_in, FAKE_SENDER, POOL_MANAGER_ARB, amount_in),
                _build_transfer_log(other_out, POOL_MANAGER_ARB, FAKE_SENDER, amount_out),
            ]
        }

        # Resolver returns known decimals for other_in/other_out
        mock_resolver = MagicMock()
        def resolve_side(addr, chain):  # noqa: E306
            if addr.lower() == other_in.lower():
                t = MagicMock(); t.decimals = 6; return t
            elif addr.lower() == other_out.lower():
                t = MagicMock(); t.decimals = 18; return t
            raise Exception("unknown")
        mock_resolver.resolve.side_effect = resolve_side
        parser._token_resolver = mock_resolver

        # Hints are for FAKE_TOKEN_IN/OUT — different addresses from other_in/other_out
        result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        # Resolver was used for other_in/other_out, not overridden by hints for FAKE addresses
        assert result.amount_in_decimal_resolved is True
        assert result.amount_out_decimal_resolved is True
        # 50_000_000 / 10^6 = 50
        assert result.amount_in_decimal == Decimal("50")


# ---------------------------------------------------------------------------
# Case 4b: empty-string address treated as backfillable (falsy guard)
# ---------------------------------------------------------------------------


class TestApplyTokenMetaAddressesEmptyString:
    """An empty-string identified address is treated as backfillable (falsy guard).

    Mirrors the sibling-parser (aerodrome/sushiswap_v3) behaviour: `not addr`
    rather than `addr is None`, so a malformed log that sets addr="" still gets
    backfilled from the compiler hint instead of being preserved as "".
    """

    def test_empty_string_token_in_addr_backfilled_from_hint(self):
        """_apply_token_meta_addresses treats '' as backfillable."""
        result_in, result_out = UniswapV4ReceiptParser._apply_token_meta_addresses(
            token_in_addr="",
            token_out_addr=FAKE_TOKEN_OUT_ADDR,
            swap_token_meta=_FULL_META,
            single_swap=True,
        )
        assert result_in == FAKE_TOKEN_IN_ADDR.lower()
        # out was non-empty, not overwritten
        assert result_out == FAKE_TOKEN_OUT_ADDR

    def test_empty_string_token_out_addr_backfilled_from_hint(self):
        """_apply_token_meta_addresses treats '' as backfillable for token_out."""
        result_in, result_out = UniswapV4ReceiptParser._apply_token_meta_addresses(
            token_in_addr=FAKE_TOKEN_IN_ADDR,
            token_out_addr="",
            swap_token_meta=_FULL_META,
            single_swap=True,
        )
        assert result_out == FAKE_TOKEN_OUT_ADDR.lower()
        # in was non-empty, not overwritten
        assert result_in == FAKE_TOKEN_IN_ADDR


# ---------------------------------------------------------------------------
# Case 5: hint decimals win over resolver for same address
# ---------------------------------------------------------------------------


class TestHintWinsOverResolver:
    def test_hint_decimals_win(self):
        """Resolver claims 18 for a 6-decimal token; hint says 6 -> hint wins."""
        parser = _make_parser()
        receipt = _make_receipt_with_transfers()

        # Give the resolver wrong decimals (18 for the 6-decimal token)
        mock_resolver = MagicMock()
        def wrong_resolve(addr, chain):  # noqa: E306
            t = MagicMock(); t.decimals = 18; return t
        mock_resolver.resolve.side_effect = wrong_resolve
        parser._token_resolver = mock_resolver

        result_no_hint = parser.extract_swap_amounts(receipt)
        result_with_hint = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        # Without hint: resolver gives 18 -> wrong amount
        assert result_no_hint is not None
        assert result_no_hint.amount_in_decimal != Decimal("100")

        # With hint: 6 -> correct amount
        assert result_with_hint is not None
        assert result_with_hint.amount_in_decimal == Decimal("100")


# ---------------------------------------------------------------------------
# Case 6: fallback preserves legacy Decimal(0) + flags False
# ---------------------------------------------------------------------------


class TestFallbackPreservesLegacyValues:
    """No hints, resolver miss -> Decimal(0) + flags False (issue #1778 contract)."""

    def test_no_hints_resolver_miss_stamps_flags_false(self):
        parser = _make_parser()
        receipt = _make_receipt_swap_only()

        result = parser.extract_swap_amounts(receipt)  # no swap_token_meta

        assert result is not None
        # Decimal(0) coercion preserved (#1778 guardrail)
        assert result.amount_in_decimal == Decimal(0)
        assert result.amount_out_decimal == Decimal(0)
        assert result.amount_in_decimal_resolved is False
        assert result.amount_out_decimal_resolved is False


# ---------------------------------------------------------------------------
# Case 7: hook shape + framework-kwarg disjointness
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

    def test_skips_none_decimals(self):
        """Adapter may emit None for decimals when it missed — these are skipped."""
        parser = _make_parser()
        bundle_metadata = {
            "from_token": {
                "symbol": FAKE_TOKEN_IN_SYMBOL,
                "address": FAKE_TOKEN_IN_ADDR,
                "decimals": None,
                "is_native": False,
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
    def test_extract_swap_amounts_has_swap_token_meta(self):
        sig = inspect.signature(UniswapV4ReceiptParser.extract_swap_amounts)
        assert "swap_token_meta" in sig.parameters
        param = sig.parameters["swap_token_meta"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY

    def test_parse_receipt_has_swap_token_meta(self):
        sig = inspect.signature(UniswapV4ReceiptParser.parse_receipt)
        assert "swap_token_meta" in sig.parameters
        param = sig.parameters["swap_token_meta"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY

    def test_extract_swap_amounts_has_expected_out(self):
        sig = inspect.signature(UniswapV4ReceiptParser.extract_swap_amounts)
        assert "expected_out" in sig.parameters
        param = sig.parameters["expected_out"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY


# ---------------------------------------------------------------------------
# Case 9: expected_out + swap_token_meta together
# ---------------------------------------------------------------------------


class TestEndToEndWithExpectedOut:
    def test_hints_and_expected_out_together(self):
        """Hints resolve decimals AND slippage_bps is computed from expected_out."""
        parser = _make_parser()
        receipt = _make_receipt_with_transfers(
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
