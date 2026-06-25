"""VIB-5353 / VIB-5355 — one canonical PT identity across every surface.

A held Pendle PT is named in two forms across the stack:

* framework / ledger / accounting → MATURITY-BEARING ``PT-wstETH-25JUN2026``
  (receipt parser → SWAP ledger row → ``PT_BUY.pt_token`` → FIFO lot);
* strategy / config → MATURITY-LESS ``PT-wstETH``
  (``get_config("pt_token", "PT-wstETH")`` → teardown ``from_token`` +
  ``details["pt_token"]``).

Bare ``canonical_symbol`` (upper/strip) never joins the two, so:

* VIB-5353 — the teardown clamp keys the swap-back on the maturity-less symbol,
  finds no tracked inventory under it, and SKIPS the swap (``untracked_token``)
  → the swap-acquired PT is stranded in the wallet; AND a PT lot is structurally
  invisible to the wallet-basis accessor (``:swap:`` key + ``remaining`` field).
* VIB-5355 — the PortfolioValuer dedup skip-set is keyed maturity-less while the
  FIFO inventory aggregates maturity-bearing, so the skip misses and the same
  held PT is counted by BOTH the reprice path and the FIFO inventory path → ~2×
  NAV.

The fix: ``canonical_pt_symbol`` (maturity-insensitive for PTs, identical to
``canonical_symbol`` for every other token) is the shared cross-surface identity,
and held-PT lots are folded into the clamp's tracked inventory. This file pins
all three surfaces plus the durable PT_BUY → held → PT_SELL write-path
round-trip (the previously-missing Layer-5 coverage).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio

from almanak.framework.accounting.basis import (
    FIFOBasisStore,
    canonical_pt_symbol,
    canonical_symbol,
    sum_open_wallet_basis_by_token,
)
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    PendleAccountingEvent,
    PendleEventType,
)
from almanak.framework.accounting.writer import AccountingWriter
from almanak.framework.portfolio.models import PositionValue, ValueConfidence
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.teardown.models import PositionType
from almanak.framework.teardown.swap_clamp import decide_swap_clamp
from almanak.framework.valuation.portfolio_valuer import (
    _aggregate_open_pt_lots,
    _classify_pt_inventory,
    _reported_pt_symbols,
)

DEP = "deployment:pt5353"
WALLET = "0x00000000000000000000000000000000000000ab"
CHAIN = "arbitrum"
# The two forms of the SAME on-chain wstETH PT (PT_TOKEN_INFO maps both to
# 0x71fB…Ce9A): maturity-BEARING (ledger/accounting) and maturity-LESS (config).
PT_BEARING = "PT-wstETH-25JUN2026"
PT_LESS = "PT-wstETH"
POS_KEY = f"pendle_pt:{CHAIN}:{WALLET}:{PT_BEARING.lower()}"
# VIB-5413: the YT analogue — same two-form split (maturity-BEARING ledger vs
# maturity-LESS teardown from_token) that strands a swap-acquired YT.
YT_BEARING = "YT-wstETH-25JUN2026"
YT_LESS = "YT-wstETH"


# ──────────────────────────────────────────────────────────────────────────
# 1. canonical_pt_symbol — the shared cross-surface identity
# ──────────────────────────────────────────────────────────────────────────


class TestCanonicalPtSymbol:
    def test_maturity_bearing_strips_to_maturity_less_upper(self):
        assert canonical_pt_symbol(PT_BEARING) == "PT-WSTETH"

    def test_maturity_less_upper_unchanged(self):
        assert canonical_pt_symbol(PT_LESS) == "PT-WSTETH"

    def test_both_forms_converge_on_one_key(self):
        assert canonical_pt_symbol(PT_BEARING) == canonical_pt_symbol(PT_LESS)

    def test_mixed_case_both_forms_converge(self):
        assert canonical_pt_symbol("pt-WSTeth-25jun2026") == "PT-WSTETH"

    def test_underscore_separator_maturity_also_stripped(self):
        assert canonical_pt_symbol("PT-sUSDe_29MAY2025") == "PT-SUSDE"

    def test_distinct_underlyings_stay_distinct(self):
        assert canonical_pt_symbol("PT-sUSDe-29MAY2025") != canonical_pt_symbol("PT-wstETH-25JUN2026")

    @pytest.mark.parametrize("sym", ["USDC", "WETH", "USDC.e", "wstETH", "", None, "  weth  "])
    def test_non_pt_identical_to_canonical_symbol(self, sym):
        # Byte-identical to canonical_symbol for every non-PT token — non-Pendle
        # inventory is unaffected.
        assert canonical_pt_symbol(sym) == canonical_symbol(sym)

    def test_pt_without_parseable_maturity_left_intact(self):
        # No DDMONYYYY suffix → behaves like canonical_symbol (no spurious strip).
        assert canonical_pt_symbol("PT-wstETH") == "PT-WSTETH"
        assert canonical_pt_symbol("PT-foobar") == "PT-FOOBAR"


class TestCanonicalYtSymbol:
    """VIB-5413: ``canonical_pt_symbol`` strips the maturity suffix for ``YT-``
    symbols too (it previously only special-cased ``PT-``), so a swap-acquired
    YT named in its maturity-BEARING ledger form joins the maturity-LESS teardown
    ``from_token`` and is recognised as tracked inventory instead of being
    stranded as ``untracked_token``."""

    def test_yt_maturity_bearing_strips_to_maturity_less_upper(self):
        assert canonical_pt_symbol(YT_BEARING) == "YT-WSTETH"

    def test_yt_maturity_less_unchanged(self):
        assert canonical_pt_symbol(YT_LESS) == "YT-WSTETH"

    def test_yt_both_forms_converge_on_one_key(self):
        assert canonical_pt_symbol(YT_BEARING) == canonical_pt_symbol(YT_LESS)

    def test_yt_mixed_case_and_underscore_separator_converge(self):
        assert canonical_pt_symbol("yt-WSTeth-25jun2026") == "YT-WSTETH"
        assert canonical_pt_symbol("YT-sUSDe_29MAY2025") == "YT-SUSDE"

    def test_yt_without_parseable_maturity_left_intact(self):
        assert canonical_pt_symbol("YT-wstETH") == "YT-WSTETH"

    def test_yt_and_pt_of_same_underlying_stay_distinct(self):
        # YT and PT of the same maturity must NOT collide (distinct prefixes) —
        # otherwise a teardown YT swap-back could match a held-PT lot.
        assert canonical_pt_symbol(YT_BEARING) != canonical_pt_symbol(PT_BEARING)

    def test_yt_distinct_underlyings_stay_distinct(self):
        assert canonical_pt_symbol("YT-sUSDe-29MAY2025") != canonical_pt_symbol(YT_BEARING)


# ──────────────────────────────────────────────────────────────────────────
# 2. VIB-5353 — held PT is TRACKED inventory + clamp matches the config symbol
# ──────────────────────────────────────────────────────────────────────────


def _pt_event(event_type: str, pt_amount: str, sy_amount: str, *, ts: str) -> dict:
    return {
        "event_type": event_type,
        "deployment_id": DEP,
        "position_key": POS_KEY,
        "chain": CHAIN,
        "wallet_address": WALLET,
        "timestamp": ts,
        "payload_json": json.dumps({"pt_token": PT_BEARING, "pt_amount": pt_amount, "sy_amount": sy_amount}),
    }


class TestHeldPtIsTrackedInventory:
    def test_held_pt_surfaces_in_tracked_map_under_maturity_less_key(self):
        events = [_pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00")]
        tracked = sum_open_wallet_basis_by_token(events, DEP)
        assert tracked is not None
        # Keyed by the maturity-LESS canonical identity, not the maturity-bearing
        # ledger form — so the config-side teardown from_token matches.
        assert tracked == {"PT-WSTETH": Decimal("100")}
        assert PT_BEARING.upper() not in tracked

    def test_partial_sell_leaves_remaining_pt_tracked(self):
        events = [
            _pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00"),
            _pt_event("PT_SELL", "40", "41", ts="2026-06-02T00:00:00+00:00"),
        ]
        tracked = sum_open_wallet_basis_by_token(events, DEP)
        assert tracked == {"PT-WSTETH": Decimal("60")}

    def test_clamp_matches_maturity_less_from_token_against_bearing_lot(self):
        # The end-to-end VIB-5353 fix: the strategy emits a maturity-LESS
        # from_token; tracked inventory is keyed maturity-less from the
        # maturity-BEARING FIFO lot → CLAMPED, not stranded as untracked_token.
        events = [_pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00")]
        tracked = sum_open_wallet_basis_by_token(events, DEP)
        decision = decide_swap_clamp(live_balance=Decimal("100"), tracked_map=tracked, from_token=PT_LESS)
        assert decision.reason == "clamped"
        assert decision.skip is False
        assert decision.amount == Decimal("100")

    def test_clamp_also_matches_maturity_bearing_from_token(self):
        # A strategy that happens to emit the maturity-bearing form matches too.
        events = [_pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00")]
        tracked = sum_open_wallet_basis_by_token(events, DEP)
        decision = decide_swap_clamp(live_balance=Decimal("100"), tracked_map=tracked, from_token=PT_BEARING)
        assert decision.reason == "clamped"

    def test_clamp_caps_at_live_balance_never_oversweeps(self):
        # min(tracked, live) — a smaller live balance caps the swap; the clamp
        # can never sweep more PT than is actually in the wallet.
        events = [_pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00")]
        tracked = sum_open_wallet_basis_by_token(events, DEP)
        decision = decide_swap_clamp(live_balance=Decimal("30"), tracked_map=tracked, from_token=PT_LESS)
        assert decision.amount == Decimal("30")

    def test_fully_sold_pt_is_zero_tracked_not_phantom(self):
        events = [
            _pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00"),
            _pt_event("PT_SELL", "100", "99", ts="2026-06-02T00:00:00+00:00"),
        ]
        tracked = sum_open_wallet_basis_by_token(events, DEP)
        # No open PT remains → the symbol is simply absent (untracked), never a
        # fabricated zero row.
        assert "PT-WSTETH" not in tracked


# ──────────────────────────────────────────────────────────────────────────
# 3. VIB-5355 — valuer dedup across the maturity form (no 2× NAV)
# ──────────────────────────────────────────────────────────────────────────


class _MarketRaisesOnPrice:
    """A market double whose pt_price MUST NOT be called (the dedup must skip
    BEFORE pricing). Calling it fails the test loudly."""

    def pt_price(self, symbol, chain):  # noqa: ARG002
        raise AssertionError(f"pt_price({symbol}) called — dedup should have skipped")


class TestValuerDedupAcrossMaturity:
    def test_maturity_less_reported_position_dedups_bearing_fifo_lot(self):
        # Discovered position reports the maturity-LESS config symbol; the FIFO
        # inventory holds the maturity-BEARING ledger symbol. The skip must match
        # across the form difference so the PT is counted ONCE (reprice path),
        # not twice (reprice + FIFO).
        reported = PositionValue(
            position_type=PositionType.TOKEN,
            protocol="pt",
            chain=CHAIN,
            value_usd=Decimal("100"),
            label="reported PT",
            details={"pt_token": PT_LESS},
        )
        skip = _reported_pt_symbols([reported])
        assert skip == {"PT-WSTETH"}

        canonical = canonical_symbol(PT_BEARING)  # maturity-bearing aggregation key
        lot_totals = {canonical: (Decimal("100"), Decimal("95"), Decimal("190000"), PT_BEARING)}
        result = _classify_pt_inventory(lot_totals, _MarketRaisesOnPrice(), CHAIN, skip)

        assert result.rows == []  # FIFO defers → no double count
        assert result.metadata["skipped"][canonical] == "reported_position_present"

    def test_no_reported_position_fifo_still_surfaces(self):
        # Sanity: when nothing reports the PT, the FIFO inventory is NOT skipped
        # (the gap-fill path stays live for the swap-only case).
        from unittest.mock import MagicMock

        from almanak.framework.market.models import PtPriceData

        market = MagicMock()
        market.pt_price = MagicMock(
            return_value=PtPriceData(
                symbol=PT_BEARING,
                chain=CHAIN,
                price=Decimal("0.97"),
                confidence=ValueConfidence.HIGH,
                underlying_price=Decimal("1.0"),
                pt_to_asset_rate=Decimal("0.97"),
                days_to_maturity=180,
                source="test",
            )
        )
        canonical = canonical_symbol(PT_BEARING)
        lot_totals = {canonical: (Decimal("100"), Decimal("95"), Decimal("95"), PT_BEARING)}
        result = _classify_pt_inventory(lot_totals, market, CHAIN, set())
        assert len(result.rows) == 1
        market.pt_price.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────
# 4. Layer-5 — PT_BUY → held → PT_SELL through the REAL writer + SQLite, then
#    cross-surface identity (event ↔ FIFO ↔ tracked inventory ↔ valuer dedup).
# ──────────────────────────────────────────────────────────────────────────


def _pendle_event(
    event_type: PendleEventType,
    *,
    pt_amount: Decimal,
    sy_amount: Decimal,
    ts: datetime,
    sy_price: Decimal | None = None,
) -> PendleAccountingEvent:
    identity = AccountingIdentity(
        id=f"id-{event_type.value}-{ts.isoformat()}",
        deployment_id=DEP,
        cycle_id="cycle-1",
        execution_mode="live",
        timestamp=ts,
        chain=CHAIN,
        protocol="pendle",
        wallet_address=WALLET,
        tx_hash=f"0x{event_type.value.lower()}",
        ledger_entry_id=f"le-{event_type.value}",
    )
    return PendleAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=POS_KEY,
        market_id="0xmarket",
        pt_token=PT_BEARING,  # the durable accounting form is maturity-bearing
        maturity_timestamp=None,
        pt_amount=pt_amount,
        sy_amount=sy_amount,
        pt_price=(sy_amount / pt_amount if pt_amount else None),
        sy_price=sy_price,
        implied_apr_bps=None,
        days_to_maturity=None,
        realized_yield_usd=None,
        confidence=AccountingConfidence.HIGH,
        unavailable_reason="",
    )


@pytest_asyncio.fixture
async def sqlite_store(tmp_path: Path):
    db_path = tmp_path / "pt_canon.sqlite"
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()
    yield store, db_path


@pytest.mark.asyncio
async def test_pt_buy_persists_and_is_consistent_across_all_surfaces(sqlite_store):
    """PT_BUY through the REAL AccountingWriter → SQLite, then the SAME persisted
    rows drive FIFO inventory, teardown tracked inventory, the clamp, and the
    valuer dedup — proving one canonical PT identity end to end."""
    store, db_path = sqlite_store
    writer = AccountingWriter(store)

    ok = await writer.write(
        _pendle_event(
            PendleEventType.PT_BUY,
            pt_amount=Decimal("100"),
            sy_amount=Decimal("95"),
            ts=datetime(2026, 6, 1, tzinfo=UTC),
            sy_price=Decimal("2000"),
        )
    )
    assert ok is True

    # The row is durably persisted with the maturity-bearing pt_token.
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT event_type, json_extract(payload_json, '$.pt_token') FROM accounting_events"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("PT_BUY", PT_BEARING)]

    # Read the persisted rows back through the production accessor.
    events = store.get_accounting_events_sync(DEP)
    assert len(events) == 1

    # Surface A — FIFO open-PT inventory keeps the maturity-bearing identity.
    fifo = FIFOBasisStore()
    fifo.reconstruct_from_events(events)
    open_lots = list(fifo.iter_open_pt_lots())
    assert len(open_lots) == 1
    assert open_lots[0][1] == PT_BEARING
    assert open_lots[0][2] == Decimal("100")

    # Surface B — teardown tracked inventory keys it maturity-less.
    tracked = sum_open_wallet_basis_by_token(events, DEP)
    assert tracked == {"PT-WSTETH": Decimal("100")}

    # Surface C — the clamp matches the strategy's maturity-less from_token.
    decision = decide_swap_clamp(live_balance=Decimal("100"), tracked_map=tracked, from_token=PT_LESS)
    assert decision.reason == "clamped" and decision.amount == Decimal("100")

    # Surface D — valuer dedup: a maturity-less reported position skips the
    # maturity-bearing FIFO lot → counted once.
    lot_totals = _aggregate_open_pt_lots(events, DEP)
    reported = PositionValue(
        position_type=PositionType.TOKEN,
        protocol="pt",
        chain=CHAIN,
        value_usd=Decimal("100"),
        label="reported",
        details={"pt_token": PT_LESS},
    )
    result = _classify_pt_inventory(lot_totals, _MarketRaisesOnPrice(), CHAIN, _reported_pt_symbols([reported]))
    assert result.rows == []  # deduped, not double-counted


@pytest.mark.asyncio
async def test_pt_buy_then_sell_round_trip_empties_tracked_inventory(sqlite_store):
    """A full buy → held → sell round-trip through the writer leaves no tracked
    PT (the teardown swap-back unwound it), and the FIFO realized-yield match
    runs on the persisted rows."""
    store, _ = sqlite_store
    writer = AccountingWriter(store)

    assert await writer.write(
        _pendle_event(
            PendleEventType.PT_BUY,
            pt_amount=Decimal("100"),
            sy_amount=Decimal("95"),
            ts=datetime(2026, 6, 1, tzinfo=UTC),
        )
    )
    # While held, the PT is tracked.
    held = sum_open_wallet_basis_by_token(store.get_accounting_events_sync(DEP), DEP)
    assert held == {"PT-WSTETH": Decimal("100")}

    assert await writer.write(
        _pendle_event(
            PendleEventType.PT_SELL,
            pt_amount=Decimal("100"),
            sy_amount=Decimal("99"),
            ts=datetime(2026, 6, 2, tzinfo=UTC),
        )
    )
    # After the sell, no PT remains tracked (no phantom residual).
    after = sum_open_wallet_basis_by_token(store.get_accounting_events_sync(DEP), DEP)
    assert "PT-WSTETH" not in after

    # FIFO replay matched the sell against the buy lot (realized yield computed).
    fifo = FIFOBasisStore()
    fifo.reconstruct_from_events(store.get_accounting_events_sync(DEP))
    assert list(fifo.iter_open_pt_lots()) == []
