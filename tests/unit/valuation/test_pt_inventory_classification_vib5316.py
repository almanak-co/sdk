"""VIB-5316 — held-PT inventory is the LIVE consumer of the gateway PT/USD price.

A held Pendle PT is NOT a position_event (PENDLE_PT PositionType removed in
VIB-4931; SWAP ∉ INTENT_TO_EVENT_TYPE), so the VIB-5313 reprice path is INERT
for a strategy that merely swapped into a PT. This file pins the path that makes
it LIVE: ``PortfolioValuer.value()`` synthesizes the open-PT inventory from FIFO
basis lots and values each symbol via ``MarketSnapshot.pt_price`` →
``value_principal_token_position`` (design spine §2 VIB-5316).

These drive the FULL ``value()`` path (events → FIFO replay → classify → snapshot
sums) so they fail if the gateway price never reaches a real NAV computation —
the inert-layer trap.

Money contract pinned:
  1. HIGH price       → PT NAV = qty × price into total_value_usd; SY cost marked
                        to USD into deployed_capital; unrealized PnL = mark − cost;
                        snapshot HIGH.
  2. UNAVAILABLE      → Empty ≠ Zero: mark/cost/PnL unmeasured (no_path), qty + SY
                        cost still shown, value_usd 0 books NO phantom NAV,
                        snapshot UNAVAILABLE.
  3. ESTIMATED/STALE  → valued but degraded: snapshot ESTIMATED (never HIGH).
  4. dedup            → a discovered PT position already covering the symbol →
                        FIFO defers (no double-count).
  5. zero PT lots     → byte-identical to the pre-VIB-5316 writer.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.market.models import PtPriceData
from almanak.framework.portfolio.models import PortfolioSnapshot, PositionValue, ValueConfidence
from almanak.framework.teardown.models import PositionType, TeardownPositionSummary
from almanak.framework.valuation.portfolio_valuer import (
    PortfolioValuer,
    _classify_pt_inventory,
)

DEP = "deployment:pt5316"
WALLET = "0x00000000000000000000000000000000000000cc"
PT = "PT-sUSDe-26DEC2024"
CHAIN = "ethereum"


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def make_strategy(tracked=("USDC",), positions=None) -> MagicMock:
    s = MagicMock()
    s.deployment_id = DEP
    s.chain = CHAIN
    s.wallet_address = WALLET
    s._get_tracked_tokens.return_value = list(tracked)
    s.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id=DEP, timestamp=datetime.now(UTC), positions=positions or []
    )
    return s


def make_market(*, pt_price: PtPriceData | None, balances=None) -> MagicMock:
    balances = balances or {"USDC": Decimal("100")}
    m = MagicMock()

    def _price(t: str, quote: str = "USD"):
        if t in ("ETH", "WETH"):
            return Decimal("2000")  # gas token priceable → snapshot stays HIGH
        if t == "USDC":
            return Decimal("1")
        raise ValueError(f"no price for {t}")

    def _bal(t: str):
        r = MagicMock()
        r.balance = balances.get(t, Decimal("0"))
        return r

    m.price = _price
    m.balance = _bal
    m.pt_price = MagicMock(return_value=pt_price)
    return m


def pt_event(event_type: str, pt_amount: str, sy_amount: str, *, ts: str, sy_price: str | None = None) -> dict:
    payload = {"pt_token": PT, "pt_amount": pt_amount, "sy_amount": sy_amount}
    if sy_price is not None:
        payload["sy_price"] = sy_price
    return {
        "event_type": event_type,
        "deployment_id": DEP,
        "position_key": "pt:ethereum",
        "chain": CHAIN,
        "wallet_address": WALLET,
        "timestamp": ts,
        "payload_json": json.dumps(payload),
    }


def make_store(events) -> MagicMock:
    st = MagicMock()
    st.get_accounting_events_sync = lambda dep, position_key=None: list(events)
    return st


def _pt_price(*, price, confidence, underlying=Decimal("1.0"), rate=Decimal("0.97")) -> PtPriceData:
    return PtPriceData(
        symbol=PT,
        chain=CHAIN,
        price=price,
        confidence=confidence,
        underlying_price=underlying,
        pt_to_asset_rate=rate,
        days_to_maturity=180,
        source="composition:getPtToAssetRate×oracle",
    )


# Buy 100 PT for 95 SY (cost_per_pt 0.95) at buy-time underlying $1.0, sell 40 for
# 39 SY → 60 PT open, 57 SY cost, 57 USD buy-time cost (0.95 × 60 × 1.0).
HELD_PT_EVENTS = [
    pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00", sy_price="1.0"),
    pt_event("PT_SELL", "40", "39", ts="2026-06-02T00:00:00+00:00"),
]
OPEN_QTY = Decimal("60")
OPEN_SY_COST = Decimal("57")  # 0.95 × 60
OPEN_USD_COST = Decimal("57.0")  # 0.95 × 60 × buy-time underlying 1.0


def run_value(events, market) -> PortfolioSnapshot:
    v = PortfolioValuer()
    if events is not None:
        v.set_accounting_context(make_store(events), DEP)
    return v.value(make_strategy(), market)


def pt_rows(snap):
    return [
        p
        for p in snap.positions
        if p.position_type == PositionType.TOKEN and (p.details or {}).get("source") == "pt_inventory_lots"
    ]


# ---------------------------------------------------------------------------
# 1. HIGH price → live NAV contribution
# ---------------------------------------------------------------------------


class TestHighPriceLiveNav:
    def test_held_pt_surfaces_with_usd_mark_cost_and_unrealized(self):
        market = make_market(pt_price=_pt_price(price=Decimal("0.97"), confidence=ValueConfidence.HIGH))
        snap = run_value(HELD_PT_EVENTS, market)

        rows = pt_rows(snap)
        assert len(rows) == 1, [str(p) for p in snap.positions]
        row = rows[0]
        # qty × price = 60 × 0.97 = 58.2
        assert row.value_usd == Decimal("58.20")
        # USD cost is the BUY-TIME-anchored cost (0.95 × 60 × 1.0 = 57), NOT a
        # current-price re-mark. (Buy-time underlying == current here, so 57 either way.)
        assert row.cost_basis_usd == Decimal("57.0")
        # unrealized = mark − cost = 1.20.
        assert row.unrealized_pnl_usd == Decimal("1.20")
        assert row.details["pt_symbol"] == PT  # original case preserved for display
        assert Decimal(row.details["quantity"]) == OPEN_QTY
        assert Decimal(row.details["sy_cost"]) == OPEN_SY_COST
        assert row.details["days_to_maturity"] == 180

    def test_pt_nav_flows_into_snapshot_sums(self):
        market = make_market(pt_price=_pt_price(price=Decimal("0.97"), confidence=ValueConfidence.HIGH))
        snap = run_value(HELD_PT_EVENTS, market)

        # PT is KNOWN_UNPRICEABLE → not in wallet value → an ordinary non-wallet
        # position: it ADDS to open-position NAV and deployed capital.
        assert snap.total_value_usd == Decimal("58.20")
        assert snap.deployed_capital_usd == Decimal("57")
        # Not subtracted from cash (a held PT is not wallet cash): wallet = USDC 100.
        assert snap.available_cash_usd == Decimal("100")
        assert snap.value_confidence == ValueConfidence.HIGH
        meta = (snap.snapshot_metadata or {}).get("pt_inventory")
        assert isinstance(meta, dict) and meta.get("status") == "applied", meta


# ---------------------------------------------------------------------------
# 2. UNAVAILABLE → Empty ≠ Zero
# ---------------------------------------------------------------------------


class TestUnavailablePriceEmptyNotZero:
    def test_unmeasured_mark_but_qty_and_cost_shown(self):
        market = make_market(pt_price=_pt_price(price=None, confidence=ValueConfidence.UNAVAILABLE, underlying=None))
        snap = run_value(HELD_PT_EVENTS, market)

        rows = pt_rows(snap)
        assert len(rows) == 1
        row = rows[0]
        # Mark/cost/PnL are UNMEASURED — placeholder 0 paired with explicit flags,
        # never a fabricated measured-zero.
        assert row.value_usd == Decimal("0")
        assert row.details["valuation_status"] == "no_path"
        assert row.details["mark_unmeasured"] is True
        assert row.details["cost_basis_unmeasured"] is True
        # Measured-from-ledger qty + SY cost STILL shown.
        assert Decimal(row.details["quantity"]) == OPEN_QTY
        assert Decimal(row.details["sy_cost"]) == OPEN_SY_COST

    def test_no_phantom_nav_and_snapshot_unavailable(self):
        market = make_market(pt_price=_pt_price(price=None, confidence=ValueConfidence.UNAVAILABLE, underlying=None))
        snap = run_value(HELD_PT_EVENTS, market)
        # value_usd 0 books NO phantom NAV; the unmeasured price drops the whole
        # snapshot to UNAVAILABLE (a reader cannot mistake it for a measured 0).
        assert snap.total_value_usd == Decimal("0")
        assert snap.value_confidence == ValueConfidence.UNAVAILABLE
        meta = (snap.snapshot_metadata or {}).get("pt_inventory")
        assert isinstance(meta, dict) and meta.get("status") == "unmeasured", meta


# ---------------------------------------------------------------------------
# 3. ESTIMATED / STALE → degraded, never folded to HIGH
# ---------------------------------------------------------------------------


class TestEstimatedDegrades:
    def test_estimated_price_marks_snapshot_estimated(self):
        market = make_market(pt_price=_pt_price(price=Decimal("0.97"), confidence=ValueConfidence.ESTIMATED))
        snap = run_value(HELD_PT_EVENTS, market)

        rows = pt_rows(snap)
        assert len(rows) == 1
        assert rows[0].value_usd == Decimal("58.20")  # still valued
        assert rows[0].details["valuation_status"] == "estimated"
        assert snap.value_confidence == ValueConfidence.ESTIMATED


# ---------------------------------------------------------------------------
# 4. dedup vs a discovered PT position (no double-count)
# ---------------------------------------------------------------------------


class TestDedupAgainstReportedPosition:
    def test_classifier_skips_symbols_already_reported(self):
        # A discovered position already covering PT (the VIB-5313 reprice path).
        reported = PositionValue(
            position_type=PositionType.SUPPLY,
            protocol="pt",
            chain=CHAIN,
            value_usd=Decimal("58"),
            label="reported PT",
            details={"pt_symbol": PT},
        )
        market = make_market(pt_price=_pt_price(price=Decimal("0.97"), confidence=ValueConfidence.HIGH))
        from almanak.framework.accounting.basis import canonical_symbol
        from almanak.framework.valuation.portfolio_valuer import _reported_pt_symbols

        canonical = canonical_symbol(PT)
        lot_totals = {canonical: (OPEN_QTY, OPEN_SY_COST, OPEN_USD_COST, PT)}
        result = _classify_pt_inventory(lot_totals, market, CHAIN, _reported_pt_symbols([reported]))
        assert result.rows == []  # FIFO defers to the reported position
        assert result.metadata["skipped"][canonical] == "reported_position_present"
        market.pt_price.assert_not_called()  # never even priced the dup


# ---------------------------------------------------------------------------
# 5. zero PT lots → byte-identical
# ---------------------------------------------------------------------------


class TestNoPtLotsByteIdentical:
    def test_no_pt_events_no_pt_inventory_stamp(self):
        market = make_market(pt_price=_pt_price(price=Decimal("0.97"), confidence=ValueConfidence.HIGH))
        baseline = run_value(None, market)
        s = run_value([], market)
        assert "pt_inventory" not in (s.snapshot_metadata or {})
        assert s.total_value_usd == baseline.total_value_usd
        assert s.available_cash_usd == baseline.available_cash_usd
        assert pt_rows(s) == []


# ---------------------------------------------------------------------------
# 6. VIB-5316 REGRESSION CONTRACT — cost basis is buy-time, never current-price.
# ---------------------------------------------------------------------------


class TestBuyTimeCostBasisRegression:
    """The bug: cost was ``sy_cost × CURRENT underlying``, which sign-flips PnL for
    volatile underlyings (PT-wstETH: a true +gain rendered as a loss). The fix
    anchors cost at the BUY-TIME underlying captured on the lot."""

    def test_volatile_underlying_cost_uses_buy_time_not_current_price(self):
        # Buy 100 PT for 95 SY (cost_per_pt 0.95) at buy-time underlying $2000
        # (wstETH-like). No sell → 100 PT open.
        events = [pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00", sy_price="2000")]
        # Underlying has since risen to $2200; current PT/USD price = $1910 →
        # mark = 100 × 1910 = 191,000.
        market = make_market(
            pt_price=_pt_price(
                price=Decimal("1910"), confidence=ValueConfidence.HIGH, underlying=Decimal("2200")
            )
        )
        snap = run_value(events, market)
        rows = pt_rows(snap)
        assert len(rows) == 1
        row = rows[0]

        assert row.value_usd == Decimal("191000")  # mark = qty × current price
        # Cost is BUY-TIME anchored: 0.95 × 100 × 2000 = 190,000.
        assert row.cost_basis_usd == Decimal("190000")
        # The BUGGY current-price re-mark would be 95 × 2200 = 209,000 → a phantom
        # −18,000 "loss". Assert we did NOT do that.
        assert row.cost_basis_usd != Decimal("209000")
        # True unrealized = 191,000 − 190,000 = +1,000 (a GAIN, correct sign).
        assert row.unrealized_pnl_usd == Decimal("1000")
        assert row.unrealized_pnl_usd > 0
        assert "cost_basis_unmeasured" not in row.details  # cost IS measured here

    def test_pre_fix_lot_without_buy_price_cost_unmeasured_mark_stands(self):
        # A pre-fix persisted lot / missing price_inputs → PT_BUY carries NO sy_price.
        events = [pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00")]  # no sy_price
        market = make_market(
            pt_price=_pt_price(
                price=Decimal("0.97"), confidence=ValueConfidence.HIGH, underlying=Decimal("2200")
            )
        )
        snap = run_value(events, market)
        rows = pt_rows(snap)
        assert len(rows) == 1
        row = rows[0]

        # Mark IS measured and STILL contributes to NAV.
        assert row.value_usd == Decimal("97.00")  # 100 × 0.97
        assert snap.total_value_usd == Decimal("97.00")
        # Cost/PnL are UNMEASURED (Empty ≠ Zero) — placeholder 0 paired with flags.
        assert row.cost_basis_usd == Decimal("0")
        assert row.unrealized_pnl_usd == Decimal("0")
        assert row.details["cost_basis_unmeasured"] is True
        assert row.details["unrealized_pnl_unmeasured"] is True
        # CRITICAL: it must NOT fall back to the current underlying (95 × 2200 = 209,000).
        assert row.cost_basis_usd != Decimal("209000")
        # Quantity + SY cost still shown (measured ledger primitives).
        assert Decimal(row.details["quantity"]) == Decimal("100")
        assert Decimal(row.details["sy_cost"]) == Decimal("95")


# ---------------------------------------------------------------------------
# 7. Dedup canonicalization + unpriceable-shape guard (VIB-5316 hardening).
# ---------------------------------------------------------------------------


class TestDedupAndShapeGuard:
    def test_mixed_case_buy_and_discovery_symbol_aggregate_to_one_row(self):
        """A buy stored mixed-case and a buy stored all-caps for the SAME PT
        aggregate to ONE inventory key (canonical join), never two."""
        from almanak.framework.valuation.portfolio_valuer import _aggregate_open_pt_lots

        mixed = "PT-sUSDe-13AUG2026"
        upper = "PT-SUSDE-13AUG2026"
        events = [
            {
                "event_type": "PT_BUY",
                "deployment_id": DEP,
                "position_key": "pt:ethereum",
                "chain": CHAIN,
                "wallet_address": WALLET,
                "timestamp": "2026-06-01T00:00:00+00:00",
                "payload_json": json.dumps(
                    {"pt_token": mixed, "pt_amount": "100", "sy_amount": "95", "sy_price": "1.0"}
                ),
            },
            {
                "event_type": "PT_BUY",
                "deployment_id": DEP,
                "position_key": "pt:ethereum",
                "chain": CHAIN,
                "wallet_address": WALLET,
                "timestamp": "2026-06-02T00:00:00+00:00",
                "payload_json": json.dumps(
                    {"pt_token": upper, "pt_amount": "50", "sy_amount": "48", "sy_price": "1.0"}
                ),
            },
        ]
        totals = _aggregate_open_pt_lots(events, DEP)
        assert len(totals) == 1, totals  # ONE canonical row, not two
        (remaining, sy_cost, usd_cost, _display) = next(iter(totals.values()))
        assert remaining == Decimal("150")  # 100 + 50 summed across both casings
        assert sy_cost == Decimal("143")  # 95 + 48
        assert usd_cost == Decimal("143")  # (0.95×100 + 0.96×50) × 1.0

    def test_discovery_position_dedups_against_mixed_case_fifo_lot(self):
        """The discovery (reported) symbol and the FIFO lot symbol canonicalize
        identically, so a different-cased reported PT defers the FIFO row."""
        from almanak.framework.accounting.basis import canonical_symbol
        from almanak.framework.valuation.portfolio_valuer import _reported_pt_symbols

        mixed = "PT-sUSDe-13AUG2026"
        reported = PositionValue(
            position_type=PositionType.SUPPLY,
            protocol="pt",
            chain=CHAIN,
            value_usd=Decimal("100"),
            label="reported PT",
            details={"pt_symbol": "PT-SUSDE-13AUG2026"},  # all-caps
        )
        market = make_market(pt_price=_pt_price(price=Decimal("0.97"), confidence=ValueConfidence.HIGH))
        lot_totals = {canonical_symbol(mixed): (Decimal("100"), Decimal("95"), Decimal("95"), mixed)}
        result = _classify_pt_inventory(lot_totals, market, CHAIN, _reported_pt_symbols([reported]))
        assert result.rows == []  # deferred — no double-count despite case mismatch
        market.pt_price.assert_not_called()

    def test_non_pt_shape_symbol_is_skipped_not_double_counted(self):
        """A symbol that lost the PT- shape must be skipped (it would otherwise
        double-count against a wallet TOKEN row), never priced as PT inventory."""
        from almanak.framework.accounting.basis import canonical_symbol

        bad = "WSTETH"  # no PT- prefix
        market = make_market(pt_price=_pt_price(price=Decimal("0.97"), confidence=ValueConfidence.HIGH))
        lot_totals = {canonical_symbol(bad): (Decimal("10"), Decimal("9"), Decimal("9"), bad)}
        result = _classify_pt_inventory(lot_totals, market, CHAIN, set())
        assert result.rows == []  # not surfaced as PT inventory
        assert result.metadata["skipped"][canonical_symbol(bad)] == "not_pt_shape"
        market.pt_price.assert_not_called()  # never priced
