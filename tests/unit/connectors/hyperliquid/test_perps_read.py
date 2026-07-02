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

from almanak.connectors.hyperliquid import perps_read as pr
from almanak.connectors.hyperliquid.addresses import PRECOMPILE_POSITION
from almanak.connectors._strategy_base.perps_read_base import PerpsPositionQuery

_WALLET = "0x" + "11" * 20


def _position_blob(szi: int, entry_ntl: int, isolated_raw_usd: int = 0, leverage: int = 0, isolated: bool = False):
    return "0x" + abi_encode(
        ["int64", "uint64", "int64", "uint32", "bool"], [szi, entry_ntl, isolated_raw_usd, leverage, isolated]
    ).hex()


def _q(markets: tuple[str, ...]) -> PerpsPositionQuery:
    return PerpsPositionQuery(chain="hyperevm", wallet_address=_WALLET, targets={}, markets=markets)


class TestBuildCalls:
    def test_one_position_call_per_market_to_precompile(self) -> None:
        calls = pr._build_hyperliquid_calls(_q(("BTC", "ETH")))
        assert len(calls) == 2
        assert all(c.to == PRECOMPILE_POSITION for c in calls)

    def test_unknown_market_skipped(self) -> None:
        assert pr._build_hyperliquid_calls(_q(("NOTACOIN",))) == []

    def test_markets_for_chain_only_hyperevm(self) -> None:
        assert pr._markets_for_chain("arbitrum") == ()
        assert "BTC" in pr._markets_for_chain("hyperevm")


class TestReduce:
    def test_decodes_active_long(self) -> None:
        res = pr._reduce_hyperliquid_positions(_q(("BTC",)), [_position_blob(1000, 600_000_000)])
        assert res.ok is True
        assert len(res.positions) == 1
        p = res.positions[0]
        assert (p.market, p.is_long, p.size_in_tokens, p.size_in_usd) == ("BTC", True, 1000, 600_000_000)

    def test_zero_size_is_measured_no_position(self) -> None:
        res = pr._reduce_hyperliquid_positions(_q(("BTC",)), [_position_blob(0, 0)])
        assert res.ok is True and res.positions == ()

    def test_all_reads_failed_is_unmeasured(self) -> None:
        # Empty≠Zero: a totally failed read is ok=False, not a fabricated empty book.
        res = pr._reduce_hyperliquid_positions(_q(("BTC", "ETH")), [None, None])
        assert res.ok is False

    def test_one_failed_market_does_not_blind_others(self) -> None:
        res = pr._reduce_hyperliquid_positions(_q(("BTC", "ETH")), [None, _position_blob(-500, 300_000_000)])
        assert res.ok is True
        assert len(res.positions) == 1
        assert res.positions[0].is_long is False  # szi negative → short

    def test_unresolvable_market_does_not_misalign_results(self) -> None:
        # _build_hyperliquid_calls SKIPS the unresolvable NOTACOIN, so results are
        # 1:1 with the resolvable (BTC, ETH) calls. reduce must consume them
        # against that same subset — a naive zip(query.markets, results) would put
        # ETH's blob onto BTC and drop ETH entirely. Distinct sizes make any
        # misalignment observable.
        btc = _position_blob(1000, 600_000_000)  # BTC long
        eth = _position_blob(-2000, 300_000_000)  # ETH short, distinct size
        res = pr._reduce_hyperliquid_positions(_q(("NOTACOIN", "BTC", "ETH")), [btc, eth])
        assert res.ok is True
        by_market = {p.market: (p.is_long, p.size_in_tokens) for p in res.positions}
        assert by_market == {"BTC": (True, 1000), "ETH": (False, 2000)}


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
