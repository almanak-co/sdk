"""VIB-5408 regression: the drawdown / high-watermark fold must SKIP UNAVAILABLE
``value_confidence`` snapshots (and ONLY UNAVAILABLE).

The lifetime/incremental drawdown fold (``get_nav_series`` →
``lifetime_drawdowns_from_nav_text`` → ``_wallet_navs_from_nav_text``) and the
recent-window fallback (``_drawdowns``) read ``portfolio_snapshots.total_value_usd``
to fold a running-peak drawdown. An ``UNAVAILABLE`` snapshot deliberately EXCLUDES an
unmeasured position from ``total_value_usd`` (e.g. a held PT that could not be priced —
``portfolio_valuer.py`` ``_pt_unmeasured_row`` — or VIB-5406's drain-barrier degrade),
so its NAV is *deflated*. Folding that deflated NAV as a genuine sample manufactures a
phantom drawdown dip and corrupts the displayed high-watermark / max-drawdown tiles
(display-correctness: these tiles feed dashboard display + an agent-tools report dict
only — no risk breaker / auto-teardown consumes them).

Gate scope — **UNAVAILABLE-only**, aligned with ``PortfolioSnapshot.is_valid`` (==
``value_confidence != UNAVAILABLE``). ``ESTIMATED`` (CEX / API estimates) and ``STALE``
(old-but-real) snapshots ARE valued — the position is priced, just imprecisely / late —
so they are NOT deflated and must be KEPT. Skipping them would remove legitimate NAV
samples and could MASK a real drawdown (a strategy legitimately ``ESTIMATED`` for its
whole life would otherwise show 0% max-DD forever). For a drawdown metric a masked-dip
false-negative is worse than an imprecise-but-real point.

Policy: **skip-carry-forward**. Dropping the ``UNAVAILABLE`` sample leaves the running
peak / latest_nav untouched, so the fold behaves as if the last measured point
persisted — no interpolation, no hard gap. Empty/None confidence is unmeasured-
*confidence* (not provably ``UNAVAILABLE``), so it falls through the gate (legacy
success path preserved).

These tests pin the core invariant from the ticket:

  *A series with an interleaved UNAVAILABLE snapshot yields the SAME max-drawdown and
   high-watermark as the same series without that row.*

across the raw text fold (``lifetime_drawdowns_from_nav_text`` / ``fold_nav_text``), the
typed AND dict recent-window fold (``_drawdowns``), and the real SQLite
``get_nav_series`` reader (which now projects the 6th ``value_confidence`` element); plus
ESTIMATED/STALE-NOT-skipped, degraded-terminal (carry-forward), all-skipped, first-row-
skipped, and Empty≠Zero legacy-path edges.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio

from almanak.framework.dashboard.quant_aggregations import _EMPTY_DRAWDOWN_STATE as EMPTY
from almanak.framework.dashboard.quant_aggregations import (
    _drawdown_stats,
    _drawdowns,
    fold_nav_text,
    lifetime_drawdowns_from_nav_text,
)
from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

_DEP = "deployment:vib5408-confidence-gate"
_BASE_TS = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)

# Healthy series: $100 peak → $90 → $110 (new peak) → $105. Max drawdown = 10% (peak
# 100, trough 90); after a new peak of 110 the current drawdown is (110-105)/110.
_HEALTHY_NAVS = [Decimal("100"), Decimal("90"), Decimal("110"), Decimal("105")]
# An UNAVAILABLE snapshot whose total_value_usd is deliberately deflated (an unmeasured
# position was dropped) — if folded it would read as a catastrophic 95% crash and a
# phantom drawdown / corrupted high-watermark.
_DEFLATED = Decimal("5")


def _text_row(i: int, total: Decimal, confidence: str | None) -> tuple:
    # (timestamp, total_value_usd_text, available_cash_usd_text, id, positions_json,
    #  value_confidence) — the 6-tuple shape get_nav_series now returns. cash 0 and
    # positions_json None so wallet-NAV == total and no debt netting.
    return (_BASE_TS + timedelta(minutes=i), str(total), "0", i, None, confidence)


# ---------------------------------------------------------------------------
# 1. Raw-text fold (lifetime path): interleaved UNAVAILABLE row is invariant.
# ---------------------------------------------------------------------------


def test_lifetime_fold_skips_unavailable_row_invariant() -> None:
    # Series WITHOUT the degraded row — all HIGH.
    clean_rows = [_text_row(i, nav, "HIGH") for i, nav in enumerate(_HEALTHY_NAVS)]

    # Same series WITH a deflated UNAVAILABLE row spliced into the middle (after the
    # $110 new-high, before the $105) — if it were folded it would crash the NAV to
    # $5 and report a ~95% drawdown.
    interleaved_rows = [
        _text_row(0, _HEALTHY_NAVS[0], "HIGH"),
        _text_row(1, _HEALTHY_NAVS[1], "HIGH"),
        _text_row(2, _HEALTHY_NAVS[2], "HIGH"),
        _text_row(3, _DEFLATED, "UNAVAILABLE"),  # the phantom dip
        _text_row(4, _HEALTHY_NAVS[3], "HIGH"),
    ]

    clean = lifetime_drawdowns_from_nav_text(clean_rows)
    interleaved = lifetime_drawdowns_from_nav_text(interleaved_rows)

    # THE invariant: the interleaved UNAVAILABLE snapshot must NOT change the computed
    # max-drawdown (nor the current drawdown / high-watermark) vs the same series
    # without it.
    assert interleaved == clean
    # And it is the HEALTHY 10% max drawdown, not the ~95% phantom the deflated NAV
    # would have produced if folded.
    max_dd, _current = interleaved
    assert max_dd == Decimal("10")  # (100 - 90) / 100 * 100


@pytest.mark.parametrize("kept_confidence", ["ESTIMATED", "STALE"])
def test_lifetime_fold_keeps_estimated_and_stale_rows(kept_confidence: str) -> None:
    # ESTIMATED / STALE are valued (priced, just imprecise / late), NOT deflated — they
    # must be KEPT. A genuine dip stamped ESTIMATED/STALE must STILL register as a
    # drawdown (skipping it would MASK a real drawdown — the over-broad-gate failure).
    rows = [
        _text_row(0, Decimal("100"), "HIGH"),  # peak
        _text_row(1, Decimal("70"), kept_confidence),  # a REAL 30% dip, just imprecise
        _text_row(2, Decimal("100"), "HIGH"),
    ]
    max_dd, _current = lifetime_drawdowns_from_nav_text(rows)
    # The ESTIMATED/STALE trough is folded → the real 30% drawdown is reported, not 0.
    assert max_dd == Decimal("30")  # (100 - 70) / 100 * 100


def test_lifetime_fold_high_water_mark_unmoved_by_unavailable_low_row() -> None:
    # An UNAVAILABLE row's deflated NAV must not lower the running peak / high-watermark.
    rows = [
        _text_row(0, Decimal("110"), "HIGH"),  # peak established
        _text_row(1, _DEFLATED, "UNAVAILABLE"),  # must be skipped
        _text_row(2, Decimal("108"), "HIGH"),
    ]
    state = fold_nav_text(EMPTY, rows)
    # Peak stays $110 (the deflated $5 neither lowered it nor was treated as a sample).
    assert state.running_peak == Decimal("110")
    # Current drawdown is the real (110 - 108) / 110, never the phantom (110 - 5) / 110.
    assert state.latest_nav == Decimal("108")


def test_lifetime_fold_degraded_terminal_row_carries_forward_last_measured() -> None:
    # The LAST row is UNAVAILABLE → current_drawdown must reflect the last MEASURED
    # latest_nav (carry-forward), not the deflated $5 (phantom) and not 0.
    rows = [
        _text_row(0, Decimal("100"), "HIGH"),  # peak
        _text_row(1, Decimal("96"), "HIGH"),  # last measured = $96
        _text_row(2, _DEFLATED, "UNAVAILABLE"),  # terminal, skipped
    ]
    state = fold_nav_text(EMPTY, rows)
    assert state.running_peak == Decimal("100")
    assert state.latest_nav == Decimal("96")  # carried forward, not $5, not None
    # Current drawdown = (100 - 96) / 100 = 4%, the real residual — not (100-5)/100.
    _max_dd, current = state.as_pcts()
    assert current == Decimal("4")


def test_lifetime_fold_all_rows_unavailable_yields_zero_zero() -> None:
    # Every row UNAVAILABLE → the filtered series is empty → (0, 0), identical to an
    # empty series fold (no peak, no drawdown). Empty≠Zero: no NAV is asserted as $0.
    rows = [_text_row(i, _DEFLATED, "UNAVAILABLE") for i in range(4)]
    assert lifetime_drawdowns_from_nav_text(rows) == _drawdown_stats([])
    assert lifetime_drawdowns_from_nav_text(rows) == (Decimal("0"), Decimal("0"))


def test_lifetime_fold_first_row_skipped_second_becomes_peak() -> None:
    # First row UNAVAILABLE (deflated) → it must NOT seed the peak; the first SURVIVING
    # row becomes the running peak. Else a deflated $5 first peak would invert the
    # drawdown sign for the rest of the series.
    rows = [
        _text_row(0, _DEFLATED, "UNAVAILABLE"),  # skipped — must not become peak $5
        _text_row(1, Decimal("100"), "HIGH"),  # first surviving → peak $100
        _text_row(2, Decimal("80"), "HIGH"),  # 20% real dip
    ]
    state = fold_nav_text(EMPTY, rows)
    assert state.running_peak == Decimal("100")
    max_dd, _current = state.as_pcts()
    assert max_dd == Decimal("20")  # (100 - 80) / 100, computed off the real peak


# ---------------------------------------------------------------------------
# 2. Recent-window typed/dict fold (_drawdowns): same invariant + ESTIMATED kept.
# ---------------------------------------------------------------------------


def _snap(i: int, total: Decimal, confidence: ValueConfidence) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=_BASE_TS + timedelta(minutes=i),
        deployment_id=_DEP,
        total_value_usd=total,
        available_cash_usd=Decimal("0"),
        deployed_capital_usd=total,
        wallet_total_value_usd=total,
        value_confidence=confidence,
        chain="arbitrum",
        iteration_number=i,
        cycle_id=f"iter-{i}",
    )


def test_recent_window_drawdowns_skips_unavailable_typed_snapshot_invariant() -> None:
    clean = [_snap(i, nav, ValueConfidence.HIGH) for i, nav in enumerate(_HEALTHY_NAVS)]
    interleaved = [
        _snap(0, _HEALTHY_NAVS[0], ValueConfidence.HIGH),
        _snap(1, _HEALTHY_NAVS[1], ValueConfidence.HIGH),
        _snap(2, _HEALTHY_NAVS[2], ValueConfidence.HIGH),
        _snap(3, _DEFLATED, ValueConfidence.UNAVAILABLE),
        _snap(4, _HEALTHY_NAVS[3], ValueConfidence.HIGH),
    ]
    assert _drawdowns(interleaved) == _drawdowns(clean)


def test_recent_window_drawdowns_keeps_estimated_typed_snapshot() -> None:
    # ESTIMATED is valued → a real dip stamped ESTIMATED must register as a drawdown.
    snaps = [
        _snap(0, Decimal("100"), ValueConfidence.HIGH),
        _snap(1, Decimal("70"), ValueConfidence.ESTIMATED),  # real 30% dip
        _snap(2, Decimal("100"), ValueConfidence.HIGH),
    ]
    max_dd, _current = _drawdowns(snaps)
    assert max_dd == Decimal("30")


def test_recent_window_drawdowns_skips_unavailable_dict_snapshot_invariant() -> None:
    # The dict shape (legacy / DB-text snapshots) gates identically.
    def _d(total: Decimal, confidence: str) -> dict:
        return {
            "total_value_usd": str(total),
            "available_cash_usd": "0",
            "value_confidence": confidence,
            "positions_json": "[]",
        }

    clean = [_d(nav, "HIGH") for nav in _HEALTHY_NAVS]
    interleaved = [
        _d(_HEALTHY_NAVS[0], "HIGH"),
        _d(_HEALTHY_NAVS[1], "HIGH"),
        _d(_HEALTHY_NAVS[2], "HIGH"),
        _d(_DEFLATED, "UNAVAILABLE"),
        _d(_HEALTHY_NAVS[3], "HIGH"),
    ]
    assert _drawdowns(interleaved) == _drawdowns(clean)


# ---------------------------------------------------------------------------
# 3. Empty≠Zero: empty/None/absent confidence is NOT skipped (legacy success path).
# ---------------------------------------------------------------------------


def test_empty_confidence_is_not_unavailable_legacy_path_preserved() -> None:
    # Empty-string / None confidence is unmeasured-confidence, not UNAVAILABLE: it must
    # NOT be skipped — the > 0 NAV filter still gates it. A 5-tuple (no confidence
    # element at all) is also the legacy path.
    rows_empty = [_text_row(i, nav, "") for i, nav in enumerate(_HEALTHY_NAVS)]
    rows_none = [_text_row(i, nav, None) for i, nav in enumerate(_HEALTHY_NAVS)]
    rows_legacy_5tuple = [
        (_BASE_TS + timedelta(minutes=i), str(nav), "0", i, None) for i, nav in enumerate(_HEALTHY_NAVS)
    ]
    rows_high = [_text_row(i, nav, "HIGH") for i, nav in enumerate(_HEALTHY_NAVS)]

    expected = lifetime_drawdowns_from_nav_text(rows_high)
    assert lifetime_drawdowns_from_nav_text(rows_empty) == expected
    assert lifetime_drawdowns_from_nav_text(rows_none) == expected
    assert lifetime_drawdowns_from_nav_text(rows_legacy_5tuple) == expected


# ---------------------------------------------------------------------------
# 4. Real SQLite reader: get_nav_series projects value_confidence (6th element)
#    and the end-to-end fold over a real backend is invariant to a degraded row.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def store(tmp_path: Path) -> SQLiteStore:
    s = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "vib5408.db")))
    await s.initialize()
    yield s
    await s.close()


@pytest.mark.asyncio
async def test_get_nav_series_projects_value_confidence_sixth_element(store: SQLiteStore) -> None:
    await store.save_portfolio_snapshot(_snap(0, Decimal("100"), ValueConfidence.HIGH))
    await store.save_portfolio_snapshot(_snap(1, _DEFLATED, ValueConfidence.UNAVAILABLE))

    rows, _truncated = await store.get_nav_series(_DEP)

    assert len(rows) == 2
    # 6-tuple: (..., positions_json[4], value_confidence[5]).
    assert all(len(r) == 6 for r in rows)
    assert rows[0][5] == "HIGH"
    assert rows[1][5] == "UNAVAILABLE"


@pytest.mark.asyncio
async def test_get_nav_series_end_to_end_fold_invariant_to_unavailable_row(store: SQLiteStore) -> None:
    # Seed the clean healthy series, read it back, fold the lifetime drawdown.
    for i, nav in enumerate(_HEALTHY_NAVS):
        await store.save_portfolio_snapshot(_snap(i, nav, ValueConfidence.HIGH))
    clean_rows, _ = await store.get_nav_series(_DEP)
    clean = lifetime_drawdowns_from_nav_text(clean_rows)

    # Splice a real UNAVAILABLE deflated snapshot into the persisted history.
    await store.save_portfolio_snapshot(_snap(len(_HEALTHY_NAVS), _DEFLATED, ValueConfidence.UNAVAILABLE))
    # And one more genuine HIGH point after it so the series isn't degraded-terminal.
    await store.save_portfolio_snapshot(_snap(len(_HEALTHY_NAVS) + 1, Decimal("105"), ValueConfidence.HIGH))

    interleaved_rows, _ = await store.get_nav_series(_DEP)
    interleaved = lifetime_drawdowns_from_nav_text(interleaved_rows)

    # The UNAVAILABLE $5 snapshot read back from the real DB must not change the
    # max-drawdown (the appended HIGH $105 already matches the clean tail $105, so the
    # only new fold input is the skipped UNAVAILABLE row).
    assert interleaved == clean
    max_dd, _current = interleaved
    assert max_dd == Decimal("10")  # healthy 10% dip, never the ~95% phantom
