"""VIB-5316 — FIFOBasisStore.iter_open_pt_lots: held-PT inventory from lots.

A held Pendle PT is NOT a position_event (the ``PENDLE_PT`` PositionType was
removed in VIB-4931; ``SWAP`` is absent from ``INTENT_TO_EVENT_TYPE``), so the
unmatched ``PT_BUY`` residual — ``PT_BUY`` minus matched ``PT_SELL`` /
``PT_REDEEM`` — is the only durable record of a currently-held PT. This file
pins the open-inventory accessor that the dashboard/NAV path consumes
(design spine §2 VIB-5316).
"""

from __future__ import annotations

import json
from decimal import Decimal

from almanak.framework.accounting.basis import FIFOBasisStore

DEP = "deployment:abc123def456"
WALLET = "0x00000000000000000000000000000000000000bb"
PT = "PT-sUSDe-26DEC2024"
POS_KEY = "pt:ethereum"


def _pt_event(
    event_type: str,
    pt_amount: str,
    sy_amount: str,
    *,
    ts: str,
    pt_token: str = PT,
    sy_price: str | None = None,
) -> dict:
    payload = {"pt_token": pt_token, "pt_amount": pt_amount, "sy_amount": sy_amount}
    if sy_price is not None:
        payload["sy_price"] = sy_price
    return {
        "event_type": event_type,
        "deployment_id": DEP,
        "position_key": POS_KEY,
        "chain": "ethereum",
        "wallet_address": WALLET,
        "timestamp": ts,
        "payload_json": json.dumps(payload),
    }


def _open(store: FIFOBasisStore) -> list[tuple[str, str, Decimal, Decimal | None, Decimal | None]]:
    return list(store.iter_open_pt_lots())


def test_two_buys_one_sell_yields_remaining_qty_and_sy_cost():
    """Buy 2 PT lots, sell 1 → open inventory = remaining qty; SY cost FIFO-pro-rated."""
    store = FIFOBasisStore()
    store.reconstruct_from_events(
        [
            # Lot 1: 100 PT for 95 SY (cost_per_pt = 0.95).
            _pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00"),
            # Lot 2: 50 PT for 48 SY (cost_per_pt = 0.96).
            _pt_event("PT_BUY", "50", "48", ts="2026-06-02T00:00:00+00:00"),
            # Sell 100 PT (consumes all of lot 1 FIFO) for 99 SY proceeds.
            _pt_event("PT_SELL", "100", "99", ts="2026-06-03T00:00:00+00:00"),
        ]
    )
    rows = _open(store)
    assert len(rows) == 1, rows
    _position_key, pt_token, remaining, sy_cost, usd_cost = rows[0]
    assert pt_token == PT  # original case preserved (identity/join key, spine §3.1)
    assert remaining == Decimal("50")  # lot 1 fully consumed; lot 2's 50 PT remain
    # SY cost of the open portion = cost_per_pt(0.96) × 50 = 48 (lot 2 untouched).
    assert sy_cost == Decimal("48")
    # No sy_price stamped on these buys → USD cost is UNMEASURED (Empty ≠ Zero),
    # never re-marked at a current price.
    assert usd_cost is None


def test_fully_redeemed_has_no_open_inventory():
    store = FIFOBasisStore()
    store.reconstruct_from_events(
        [
            _pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00"),
            _pt_event("PT_REDEEM", "100", "101", ts="2026-12-26T00:00:00+00:00"),
        ]
    )
    assert _open(store) == []


def test_symbol_is_the_join_key_distinct_symbols_separate():
    other = "PT-wstETH-26JUN2025"
    store = FIFOBasisStore()
    store.reconstruct_from_events(
        [
            _pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00"),
            _pt_event("PT_BUY", "20", "19", ts="2026-06-02T00:00:00+00:00", pt_token=other),
        ]
    )
    rows = {tok: (rem, cost) for _pk, tok, rem, cost, _usd in _open(store)}
    assert rows[PT] == (Decimal("100"), Decimal("95"))
    assert rows[other] == (Decimal("20"), Decimal("19"))


def test_pt_lots_not_confused_with_other_lot_kinds():
    """The shared _lots dict also holds BORROW/SWAP lots — only PT lots yield."""
    store = FIFOBasisStore()
    store.record_borrow(DEP, "supply:ethereum", "USDC", Decimal("1000"))
    store.record_swap_acquisition(DEP, f"swap:ethereum:{WALLET}", "WETH", Decimal("2"), cost_usd=Decimal("5000"))
    store.record_pt_buy(DEP, POS_KEY, PT, Decimal("100"), Decimal("95"))
    rows = _open(store)
    assert len(rows) == 1
    assert rows[0][1] == PT
    assert rows[0][2] == Decimal("100")


def test_buy_time_usd_cost_uses_stamped_sy_price_pro_rated():
    """A PT_BUY carrying sy_price stamps buy-time USD cost, FIFO-pro-rated on partial sell."""
    store = FIFOBasisStore()
    store.reconstruct_from_events(
        [
            # 100 PT for 95 SY (cost_per_pt 0.95) at buy-time underlying $2000 (wstETH-like).
            _pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00", sy_price="2000"),
            # Sell 40 PT (FIFO consumes 40 of the 100) for 41 SY → 60 PT remain.
            _pt_event("PT_SELL", "40", "41", ts="2026-06-02T00:00:00+00:00"),
        ]
    )
    rows = _open(store)
    assert len(rows) == 1
    _pk, _tok, remaining, sy_cost, usd_cost = rows[0]
    assert remaining == Decimal("60")
    assert sy_cost == Decimal("57")  # 0.95 × 60
    # buy-time USD cost = cost_per_pt(0.95) × remaining(60) × underlying_at_buy(2000) = 114000.
    # Anchored to the $2000 BUY price, NOT any later/current price.
    assert usd_cost == Decimal("114000")


def test_buy_time_usd_cost_none_when_one_lot_missing_price():
    """Empty ≠ Zero at the lot level: a lot without sy_price yields usd_cost None."""
    store = FIFOBasisStore()
    store.reconstruct_from_events(
        [_pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00")]  # no sy_price
    )
    rows = _open(store)
    assert len(rows) == 1
    assert rows[0][4] is None  # usd_cost unmeasured — never 0, never a current-price re-mark


def test_record_pt_buy_stores_buy_time_price_on_lot():
    """The direct (non-replay) record path also stamps the buy-time underlying price."""
    store = FIFOBasisStore()
    store.record_pt_buy(DEP, POS_KEY, PT, Decimal("100"), Decimal("95"), sy_price=Decimal("1.5"))
    _pk, _tok, remaining, sy_cost, usd_cost = _open(store)[0]
    assert remaining == Decimal("100")
    assert sy_cost == Decimal("95")
    assert usd_cost == Decimal("142.5")  # 0.95 × 100 × 1.5
