"""Unit tests for the Hyperliquid perps read spec (structure + Empty≠Zero).

The valuation SCALES (entryNtl 1e6, szi 10**szDecimals) were CONFIRMED against a
live mainnet position on 2026-07-01 (see perps_read.py module docstring — raw
precompile fields cross-checked exactly against api.hyperliquid.xyz human units).
These tests pin the read STRUCTURE and the internal consistency of the math
(entry recovered from notional/size, sign-aware PnL, Empty≠Zero); the absolute
scale is now backed by the live cross-check rather than assumption. The one
residual is the isolated-margin ``isolatedRawUsd`` scale (no isolated position in
the cross-check account) — still assumed 1e6.
"""

from __future__ import annotations

from decimal import Decimal

from eth_abi import encode as abi_encode

from almanak.connectors._strategy_base.perps_read_base import PerpsPositionQuery
from almanak.connectors.hyperliquid import perps_read as pr
from almanak.connectors.hyperliquid.addresses import (
    PRECOMPILE_ACCOUNT_MARGIN_SUMMARY,
    PRECOMPILE_POSITION,
)

_WALLET = "0x" + "11" * 20


def _position_blob(szi: int, entry_ntl: int, isolated_raw_usd: int = 0, leverage: int = 0, isolated: bool = False):
    return "0x" + abi_encode(
        ["int64", "uint64", "int64", "uint32", "bool"], [szi, entry_ntl, isolated_raw_usd, leverage, isolated]
    ).hex()


def _margin_blob(account_value: int = 1_000_000, margin_used: int = 0, ntl_pos: int = 0, raw_usd: int = 0):
    """A well-formed 0x080F accountMarginSummary blob (measured cross account)."""
    return "0x" + abi_encode(
        ["int64", "uint64", "uint64", "int64"], [account_value, margin_used, ntl_pos, raw_usd]
    ).hex()


_DEFAULT_MARGIN = object()  # sentinel: "caller didn't specify" (≠ margin=None = failed read)


def _results(*position_blobs, margin=_DEFAULT_MARGIN):
    """Build a ``results`` list matching build_calls: position blobs + trailing margin.

    ``margin`` unspecified → a measured account (so cross collateral folds in).
    Pass ``margin=None`` to simulate a failed/unmeasured 0x080F read (the trailing
    result is genuinely ``None``); pass a hex string to inject a specific blob.
    """
    m = _margin_blob() if margin is _DEFAULT_MARGIN else margin
    return [*position_blobs, m]


def _q(markets: tuple[str, ...]) -> PerpsPositionQuery:
    return PerpsPositionQuery(chain="hyperevm", wallet_address=_WALLET, targets={}, markets=markets)


class TestBuildCalls:
    def test_one_position_call_per_market_plus_trailing_margin(self) -> None:
        # 2 position calls (BTC, ETH) + 1 trailing account-margin call.
        calls = pr._build_hyperliquid_calls(_q(("BTC", "ETH")))
        assert len(calls) == 3
        assert all(c.to == PRECOMPILE_POSITION for c in calls[:-1])
        assert calls[-1].to == PRECOMPILE_ACCOUNT_MARGIN_SUMMARY

    def test_margin_call_is_always_last_even_with_no_markets(self) -> None:
        # An unknown market emits no position call, but the account-margin call is
        # ALWAYS appended (it is per-wallet, not per-market).
        calls = pr._build_hyperliquid_calls(_q(("NOTACOIN",)))
        assert len(calls) == 1
        assert calls[0].to == PRECOMPILE_ACCOUNT_MARGIN_SUMMARY

    def test_markets_for_chain_only_hyperevm(self) -> None:
        assert pr._markets_for_chain("arbitrum") == ()
        assert "BTC" in pr._markets_for_chain("hyperevm")


class TestReduce:
    def test_decodes_active_long(self) -> None:
        res = pr._reduce_hyperliquid_positions(_q(("BTC",)), _results(_position_blob(1000, 600_000_000)))
        assert res.ok is True
        assert len(res.positions) == 1
        p = res.positions[0]
        assert (p.market, p.is_long, p.size_in_tokens, p.size_in_usd) == ("BTC", True, 1000, 600_000_000)

    def test_zero_size_is_measured_no_position(self) -> None:
        res = pr._reduce_hyperliquid_positions(_q(("BTC",)), _results(_position_blob(0, 0)))
        assert res.ok is True and res.positions == ()

    def test_all_reads_failed_is_unmeasured(self) -> None:
        # Empty≠Zero: a totally failed POSITION read is ok=False, not a fabricated
        # empty book. The account-margin blob is excluded from this test — even a
        # measured margin blob (trailing) does not rescue an all-None position book.
        res = pr._reduce_hyperliquid_positions(_q(("BTC", "ETH")), _results(None, None))
        assert res.ok is False

    def test_margin_blob_excluded_from_all_none_test(self) -> None:
        # A 0x080F revert (margin=None) on an OTHERWISE-good book must NOT mark the
        # whole read unmeasured — the account blob is not a position blob.
        res = pr._reduce_hyperliquid_positions(
            _q(("BTC",)), _results(_position_blob(1000, 600_000_000), margin=None)
        )
        assert res.ok is True
        assert len(res.positions) == 1

    def test_one_failed_market_does_not_blind_others(self) -> None:
        res = pr._reduce_hyperliquid_positions(
            _q(("BTC", "ETH")), _results(None, _position_blob(-500, 300_000_000))
        )
        assert res.ok is True
        assert len(res.positions) == 1
        assert res.positions[0].is_long is False  # szi negative → short

    def test_unresolvable_market_does_not_misalign_results(self) -> None:
        # _build_hyperliquid_calls SKIPS the unresolvable NOTACOIN, so the POSITION
        # results are 1:1 with the resolvable (BTC, ETH) calls (plus a trailing
        # margin blob). reduce must consume them against that same subset — a naive
        # zip(query.markets, results) would put ETH's blob onto BTC and drop ETH
        # entirely. Distinct sizes make any misalignment observable.
        btc = _position_blob(1000, 600_000_000)  # BTC long
        eth = _position_blob(-2000, 300_000_000)  # ETH short, distinct size
        res = pr._reduce_hyperliquid_positions(_q(("NOTACOIN", "BTC", "ETH")), _results(btc, eth))
        assert res.ok is True
        by_market = {p.market: (p.is_long, p.size_in_tokens) for p in res.positions}
        assert by_market == {"BTC": (True, 1000), "ETH": (False, 2000)}


class TestCrossMarginCollateral:
    """VIB-5596: a cross position folds entryNtl/leverage in as collateral, gated
    on a measured 0x080F account-margin read (Empty≠Zero preserved)."""

    def test_cross_collateral_is_entry_margin_when_measured(self) -> None:
        # Cross (isolated=False), entryNtl=600_000_000 (1e6 USD → $600), leverage=20
        # → initial margin = 600_000_000 // 20 = 30_000_000 (= $30). NOT PnL-only 0.
        blob = _position_blob(1000, 600_000_000, isolated_raw_usd=0, leverage=20, isolated=False)
        res = pr._reduce_hyperliquid_positions(_q(("BTC",)), _results(blob, margin=_margin_blob()))
        assert res.ok is True
        assert res.positions[0].collateral_amount == 30_000_000  # $30 at 1e6

    def test_cross_collateral_is_zero_when_margin_unmeasured(self) -> None:
        # Empty≠Zero: 0x080F failed (margin=None) → cross falls back to PnL-only
        # (collateral 0), NOT a fabricated entry-margin off a failed account read.
        blob = _position_blob(1000, 600_000_000, isolated_raw_usd=0, leverage=20, isolated=False)
        res = pr._reduce_hyperliquid_positions(_q(("BTC",)), _results(blob, margin=None))
        assert res.ok is True
        assert res.positions[0].collateral_amount == 0

    def test_cross_zero_leverage_never_divides_by_zero(self) -> None:
        # Malformed cross with leverage 0, margin measured → guard falls back to 0.
        blob = _position_blob(1000, 600_000_000, isolated_raw_usd=0, leverage=0, isolated=False)
        res = pr._reduce_hyperliquid_positions(_q(("BTC",)), _results(blob, margin=_margin_blob()))
        assert res.ok is True
        assert res.positions[0].collateral_amount == 0

    def test_isolated_collateral_is_isolated_raw_usd_and_not_gated(self) -> None:
        # Isolated position: collateral = isolatedRawUsd, independent of 0x080F.
        blob = _position_blob(1000, 600_000_000, isolated_raw_usd=25_000_000, leverage=5, isolated=True)
        res = pr._reduce_hyperliquid_positions(_q(("BTC",)), _results(blob, margin=None))
        assert res.ok is True
        assert res.positions[0].collateral_amount == 25_000_000  # $25 at 1e6, ungated

    def test_malformed_margin_blob_leaves_cross_pnl_only(self) -> None:
        # A non-empty but undecodable margin blob is treated as unmeasured (gate
        # False) → cross stays PnL-only, positions still measured.
        blob = _position_blob(1000, 600_000_000, isolated_raw_usd=0, leverage=20, isolated=False)
        res = pr._reduce_hyperliquid_positions(_q(("BTC",)), _results(blob, margin="0xdeadbeef"))
        assert res.ok is True
        assert res.positions[0].collateral_amount == 0


class TestValue:
    def test_entry_recovered_and_pnl_signed(self) -> None:
        # 0.01 BTC (szi 1000, szDecimals 5), entry notional 600 USD → entry 60000.
        val = pr.value_hyperliquid_position(
            size_in_usd=600_000_000,
            size_in_tokens=1000,
            collateral_amount=0,
            is_long=True,
            mark_price_usd=Decimal("66000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=5,
            market="BTC",
        )
        assert val.size_usd == Decimal("600")
        assert val.entry_price_usd == Decimal("60000")
        # long, mark 66000 > entry 60000 → +0.01 * 6000 = +60 uPnL.
        assert val.unrealized_pnl_usd == Decimal("60.00")

    def test_short_pnl_inverts(self) -> None:
        val = pr.value_hyperliquid_position(
            size_in_usd=600_000_000,
            size_in_tokens=1000,
            collateral_amount=0,
            is_long=False,
            mark_price_usd=Decimal("66000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=5,
        )
        assert val.unrealized_pnl_usd == Decimal("-60.00")

    def test_cross_margin_net_value_includes_account_margin_not_pnl_only(self) -> None:
        # VIB-5596: with the folded cross collateral (entryNtl/leverage = $30 at
        # 1e6 → collateral_amount 30_000_000), net_value = collateral + pnl, NOT
        # PnL-only. Same inputs as test_entry_recovered_and_pnl_signed (uPnL +$60).
        val = pr.value_hyperliquid_position(
            size_in_usd=600_000_000,
            size_in_tokens=1000,
            collateral_amount=30_000_000,  # $30 initial margin (entryNtl/leverage)
            is_long=True,
            mark_price_usd=Decimal("66000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=5,
            market="BTC",
        )
        assert val.collateral_value_usd == Decimal("30")
        assert val.unrealized_pnl_usd == Decimal("60.00")
        # net = collateral 30 + pnl 60 - 0 fees = 90; a PnL-only net would be 60.
        assert val.net_value_usd == Decimal("90.00")
        assert val.net_value_usd != val.unrealized_pnl_usd  # not PnL-only

    def test_cross_margin_pnl_only_when_collateral_zero(self) -> None:
        # The unmeasured-account fallback (collateral 0) keeps net = PnL-only.
        val = pr.value_hyperliquid_position(
            size_in_usd=600_000_000,
            size_in_tokens=1000,
            collateral_amount=0,
            is_long=True,
            mark_price_usd=Decimal("66000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=5,
            market="BTC",
        )
        assert val.net_value_usd == val.unrealized_pnl_usd == Decimal("60.00")
