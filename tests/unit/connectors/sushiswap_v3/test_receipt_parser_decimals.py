"""Tests for SushiSwapV3ReceiptParser compiler-metadata hint threading (VIB-3164).

Verifies that swap decimal resolution uses compiler-supplied token metadata when
the TokenResolver misses, without changing fail-closed semantics for cases where
hints are also absent.

Test structure mirrors tests/unit/connectors/uniswap_v3/test_receipt_parser_decimals.py.
"""

from __future__ import annotations

import inspect
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors.sushiswap_v3.receipt_parser import (
    EVENT_TOPICS,
    SushiSwapV3ReceiptParser,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Fake 6-decimal token (USDC-like) — not in real TokenResolver
FAKE_TOKEN_IN_ADDR = "0x" + "a1" * 20
FAKE_TOKEN_IN_DECIMALS = 6
FAKE_TOKEN_IN_SYMBOL = "FAKE6"

# Fake 18-decimal token (WETH-like) — not in real TokenResolver
FAKE_TOKEN_OUT_ADDR = "0x" + "b2" * 20
FAKE_TOKEN_OUT_DECIMALS = 18
FAKE_TOKEN_OUT_SYMBOL = "FAKE18"

FAKE_POOL_ADDR = "0x" + "c3" * 20
FAKE_WALLET_ADDR = "0x" + "d4" * 20

SWAP_TOPIC = EVENT_TOPICS["Swap"]
TRANSFER_TOPIC = EVENT_TOPICS["Transfer"]


# ---------------------------------------------------------------------------
# Receipt-building helpers
# ---------------------------------------------------------------------------


def _pad32(val: int, signed: bool = False) -> str:
    if signed and val < 0:
        val += 1 << 256
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _int256_hex(val: int) -> str:
    if val >= 0:
        return hex(val)[2:].zfill(64)
    return hex((1 << 256) + val)[2:].zfill(64)


def _make_swap_log(
    pool: str = FAKE_POOL_ADDR,
    sender: str = FAKE_WALLET_ADDR,
    recipient: str = FAKE_WALLET_ADDR,
    amount0: int = 100_000_000,   # positive -> token0 is input
    amount1: int = -1_079_340_000_000_000_000_000,  # negative -> token1 is output
) -> dict:
    data = "0x" + (
        _int256_hex(amount0)
        + _int256_hex(amount1)
        + _pad32(2**96)       # sqrtPriceX96
        + _pad32(10**18)      # liquidity
        + _int256_hex(0)      # tick
    )
    return {
        "address": pool,
        "topics": [SWAP_TOPIC, _addr_topic(sender), _addr_topic(recipient)],
        "data": data,
        "logIndex": 1,
    }


def _make_transfer_log(
    token: str,
    from_addr: str,
    to_addr: str,
    amount: int,
    log_index: int = 0,
) -> dict:
    return {
        "address": token,
        "topics": [TRANSFER_TOPIC, _addr_topic(from_addr), _addr_topic(to_addr)],
        "data": "0x" + _pad32(amount),
        "logIndex": log_index,
    }


def _make_receipt(
    logs: list[dict],
    wallet: str = FAKE_WALLET_ADDR,
) -> dict:
    return {
        "transactionHash": "0x" + "ee" * 32,
        "status": 1,
        "blockNumber": 100,
        "gasUsed": 200_000,
        "from": wallet,
        "logs": logs,
    }


def _make_full_receipt(
    amount_in: int = 100_000_000,       # 100 units at 6 decimals
    amount_out: int = 1_079_340_000_000_000_000_000,  # 1079.34 at 18 decimals
    token_in: str = FAKE_TOKEN_IN_ADDR,
    token_out: str = FAKE_TOKEN_OUT_ADDR,
    pool: str = FAKE_POOL_ADDR,
    wallet: str = FAKE_WALLET_ADDR,
) -> dict:
    """Build a receipt with Swap + Transfer logs for a single-hop swap."""
    return _make_receipt(
        wallet=wallet,
        logs=[
            _make_transfer_log(token_in, wallet, pool, amount_in, log_index=0),
            _make_swap_log(pool, wallet, wallet, amount_in, -amount_out),
            _make_transfer_log(token_out, pool, wallet, amount_out, log_index=2),
        ],
    )


# ---------------------------------------------------------------------------
# Minimal parser factory — resolver always misses
# ---------------------------------------------------------------------------


def _make_parser(chain: str = "arbitrum") -> SushiSwapV3ReceiptParser:
    """Construct a parser with no token info and a resolver that always misses."""
    with patch(
        "almanak.connectors.sushiswap_v3.receipt_parser.SushiSwapV3ReceiptParser._resolve_token_info",
        return_value=("", None),
    ):
        return SushiSwapV3ReceiptParser(chain=chain)


# ---------------------------------------------------------------------------
# Token metadata fixtures
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
    """Hint resolves correct decimals when TokenResolver returns None."""

    def test_resolver_miss_hints_provide_decimals(self):
        parser = _make_parser()
        receipt = _make_full_receipt()
        result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        # Fail-closed parser never emits unresolved values; defaults stay True
        assert result.amount_in_decimal_resolved is True
        assert result.amount_out_decimal_resolved is True


# ---------------------------------------------------------------------------
# Case 2: direction fallback when transfers unclassifiable (no wallet in transfers)
# ---------------------------------------------------------------------------


class TestDirectionFallback:
    """Single-swap receipts with no wallet-matched transfers resolve via hint slots."""

    def test_direction_fallback_single_swap(self):
        """No wallet in transfers + single swap event + hints -> resolves via fallback."""
        parser = _make_parser()
        # Receipt with only a Swap log; no wallet-matched Transfer events
        receipt = _make_receipt(
            logs=[_make_swap_log()],
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
        # Two separate Swap logs — sushiswap parser sees 2 swap_events -> single_swap=False
        swap_log_0 = _make_swap_log()
        swap_log_1 = dict(_make_swap_log())
        swap_log_1["logIndex"] = 2
        receipt = _make_receipt(
            logs=[swap_log_0, swap_log_1],
            wallet=FAKE_WALLET_ADDR,
        )
        result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)
        # No wallet-matched transfers and no direction fallback -> None
        assert result is None


# ---------------------------------------------------------------------------
# Case 4: address-mismatch skip
# ---------------------------------------------------------------------------


class TestAddressMismatchSkip:
    """Hints for unrelated addresses do not affect resolution of known addresses."""

    def test_hint_for_wrong_address_is_ignored(self):
        """Transfers classify addresses A/B; hints carry C/D -> hints ignored,
        resolver resolves A/B normally."""
        parser = _make_parser()

        other_in = "0x" + "e5" * 20
        other_out = "0x" + "f6" * 20
        amount_in = 50_000_000   # 50 at 6 dec
        amount_out = 500 * 10**18

        receipt = _make_receipt(
            wallet=FAKE_WALLET_ADDR,
            logs=[
                _make_transfer_log(other_in, FAKE_WALLET_ADDR, FAKE_POOL_ADDR, amount_in, 0),
                _make_swap_log(amount0=amount_in, amount1=-amount_out),
                _make_transfer_log(other_out, FAKE_POOL_ADDR, FAKE_WALLET_ADDR, amount_out, 2),
            ],
        )

        # Hints are for FAKE_TOKEN_IN / FAKE_TOKEN_OUT — different addresses
        # Resolver should be called for other_in / other_out
        with patch.object(parser, "_resolve_decimals") as mock_resolve:
            mock_resolve.side_effect = lambda addr: (6 if addr == other_in.lower() else 18)
            result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        # Hints not applied — resolver path used for other_in/other_out
        # Verify resolver was called for the actual addresses, not hint addresses
        called_addrs = [call.args[0] for call in mock_resolve.call_args_list]
        assert other_in.lower() in called_addrs or other_out.lower() in called_addrs


# ---------------------------------------------------------------------------
# Case 5: hint wins over resolver for same address
# ---------------------------------------------------------------------------


class TestHintWinsOverResolver:
    """Compiler hint takes precedence over TokenResolver for the same address."""

    def test_hint_decimals_win(self):
        receipt = _make_full_receipt()
        # Resolver claims 18 for the 6-decimal token (wrong)
        with patch(
            "almanak.connectors.sushiswap_v3.receipt_parser.SushiSwapV3ReceiptParser._resolve_token_info",
            return_value=("WRONG", 18),
        ):
            parser = SushiSwapV3ReceiptParser(chain="arbitrum")

        with patch.object(parser, "_resolve_decimals", return_value=18):
            result = parser.extract_swap_amounts(receipt, swap_token_meta=_FULL_META)

        assert result is not None
        # Hint says 6 for FAKE_TOKEN_IN -> 100 FAKE6, not 1e-10
        assert result.amount_in_decimal == Decimal("100")


# ---------------------------------------------------------------------------
# Case 6: no-hint fallback preserves legacy fail-closed behaviour
# ---------------------------------------------------------------------------


class TestFallbackPreservesFailClosed:
    """Without hints, resolver miss -> None and warning (existing behaviour)."""

    def test_no_hints_resolver_miss_returns_none(self, caplog):
        import logging

        parser = _make_parser()
        receipt = _make_full_receipt()

        caplog.set_level(logging.WARNING)
        result = parser.extract_swap_amounts(receipt)  # no swap_token_meta

        # Resolver returns None for fake addresses -> fail-closed
        assert result is None
        assert any(
            "Cannot compute swap amounts" in msg for msg in caplog.messages
        )


# ---------------------------------------------------------------------------
# Case 7: hook shape + framework-kwarg disjointness
# ---------------------------------------------------------------------------


class TestBuildExtractKwargsHook:
    """VIB-3164: parser-owned enricher hook returns swap_token_meta correctly."""

    def test_returns_swap_token_meta(self):
        parser = _make_parser()
        bundle_metadata = {
            "from_token": {
                "symbol": FAKE_TOKEN_IN_SYMBOL,
                "address": FAKE_TOKEN_IN_ADDR.upper(),  # upper -> should be lowercased
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
                "is_native": True,  # skipped
            },
        }
        result = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle_metadata)
        assert result == {}

    def test_skips_missing_decimals(self):
        parser = _make_parser()
        bundle_metadata = {
            "to_token": {"symbol": "WETH", "address": FAKE_TOKEN_OUT_ADDR},  # no decimals
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
        """expected_out must never appear in the returned dict (disjointness contract)."""
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
# Case 8: signature guard — anti-TypeError-fallback-regression
# ---------------------------------------------------------------------------


class TestSignatureGuard:
    """extract_swap_amounts must declare swap_token_meta to prevent enricher TypeError fallback."""

    def test_extract_swap_amounts_has_swap_token_meta_param(self):
        sig = inspect.signature(SushiSwapV3ReceiptParser.extract_swap_amounts)
        assert "swap_token_meta" in sig.parameters
        param = sig.parameters["swap_token_meta"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY

    def test_extract_swap_amounts_has_expected_out_param(self):
        sig = inspect.signature(SushiSwapV3ReceiptParser.extract_swap_amounts)
        assert "expected_out" in sig.parameters
        param = sig.parameters["expected_out"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY


# ---------------------------------------------------------------------------
# Case 9: end-to-end with expected_out — both kwargs coexist
# ---------------------------------------------------------------------------


class TestEndToEndWithExpectedOut:
    """swap_token_meta and expected_out coexist: decimals from hints + slippage computed."""

    def test_hints_and_expected_out_together(self):
        parser = _make_parser()
        receipt = _make_full_receipt(
            amount_in=100_000_000,    # 100.0 at 6 dec
            amount_out=1_079_340_000_000_000_000_000,  # 1079.34 at 18 dec
        )
        result = parser.extract_swap_amounts(
            receipt,
            expected_out=Decimal("1100"),
            swap_token_meta=_FULL_META,
        )
        assert result is not None
        assert result.amount_in_decimal == Decimal("100")
        assert result.amount_out_decimal == Decimal("1079.34")
        # slippage_bps computed from expected_out
        assert result.slippage_bps is not None
        # (1100 - 1079.34) / 1100 * 10000 ≈ 188 bps
        assert 180 < result.slippage_bps < 200
