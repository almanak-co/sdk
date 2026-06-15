"""VIB-5134: incremental "fetch since cursor" lifetime-drawdown fold.

VIB-5118 made the dashboard's lifetime drawdown correct by scanning the WHOLE
snapshot history every render. On a hosted multi-tenant gateway that O(history)
scan re-runs on the 5-second render cadence — wasteful for a slow-moving lifetime
max-drawdown. VIB-5134 keeps the figure correct while cutting the read cost:

  * the expensive full-history scan refreshes only every
    ``_LIFETIME_DRAWDOWN_TTL_SECONDS`` (max-drawdown is slow-moving);
  * between full scans the gateway advances an in-memory ``(running_peak, cursor)``
    checkpoint by folding ONLY the snapshots newer than the cursor — so
    current-drawdown stays live AND correct: a new high-water mark that lands
    after the last scan is folded into the peak BEFORE current-drawdown is
    computed (a plain "latest NAV vs. cached peak" would understate it).

These tests pin: (1) the running-peak fold is resumable — folding a suffix into a
prefix's state is byte-identical to one full recompute (golden-equivalence, the
VIB-5118 pattern); (2) the SQLite reader's ``since`` cursor returns only rows
strictly after it, oldest-first, with a correct identical-timestamp tiebreak and
gapless (oldest-kept) truncation; (3) end-to-end, a new-high-then-fall BETWEEN
full scans is reflected in current-drawdown on the next render via the cheap
incremental fold — the Codex finding-1 regression.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from almanak.framework.dashboard.quant_aggregations import (
    _EMPTY_DRAWDOWN_STATE,
    _drawdown_stats,
    fold_drawdowns,
    fold_nav_text,
    lifetime_drawdowns_from_nav_text,
)
from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

_DEP = "deployment:vib5134-incremental-dd"
_BASE_TS = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)


# ===========================================================================
# 1. The running-peak fold is resumable (golden-equivalence).
# ===========================================================================


def _navs(values: list[str]) -> list[Decimal]:
    return [Decimal(v) for v in values]


@pytest.mark.parametrize("split", [0, 1, 3, 5, 7, 8])
def test_fold_is_resumable_across_any_split(split: int) -> None:
    # A series with an early peak/crash and a later new high: folding [:split]
    # then [split:] must equal folding the whole series at once — that property
    # is what lets the gateway fold only the newest tail and still report the
    # lifetime figure a full recompute would.
    series = _navs(["100", "60", "80", "150", "90", "150", "120", "200", "140"])

    one_shot = fold_drawdowns(_EMPTY_DRAWDOWN_STATE, series)
    resumed = fold_drawdowns(fold_drawdowns(_EMPTY_DRAWDOWN_STATE, series[:split]), series[split:])

    assert resumed.running_peak == one_shot.running_peak
    assert resumed.max_drawdown == one_shot.max_drawdown
    assert resumed.latest_nav == one_shot.latest_nav
    assert resumed.as_pcts() == one_shot.as_pcts()


def test_full_recompute_equals_drawdown_stats_and_lifetime_text() -> None:
    # The fold front-ends (numeric / text / the named lifetime entry point) all
    # converge on the SAME recurrence as _drawdown_stats (the windowed path).
    series = _navs(["100", "60", "80", "150", "90", "120"])
    rows = [(_BASE_TS + timedelta(minutes=i), str(v), "0", i) for i, v in enumerate(series)]

    via_numeric = fold_drawdowns(_EMPTY_DRAWDOWN_STATE, series).as_pcts()
    via_text = fold_nav_text(_EMPTY_DRAWDOWN_STATE, rows).as_pcts()

    assert via_numeric == via_text
    assert via_text == lifetime_drawdowns_from_nav_text(rows)
    assert via_text == _drawdown_stats(series)


def test_empty_and_single_sample_states() -> None:
    # No positive sample folded yet → both drawdowns 0, peak unknown (None).
    assert _EMPTY_DRAWDOWN_STATE.running_peak is None
    assert _EMPTY_DRAWDOWN_STATE.as_pcts() == (Decimal("0"), Decimal("0"))

    # A single sample → peak == that sample, both drawdowns 0.
    one = fold_drawdowns(_EMPTY_DRAWDOWN_STATE, _navs(["123.45"]))
    assert one.running_peak == Decimal("123.45")
    assert one.as_pcts() == (Decimal("0"), Decimal("0"))


def test_fold_nav_text_applies_empty_not_zero_filter() -> None:
    # "", None and garbage are unmeasured (filtered), never a measured $0 that
    # would crater the running peak.
    rows: list[tuple[Any, str | None, str | None, int]] = [
        (_BASE_TS, "100", "0", 0),
        (_BASE_TS + timedelta(minutes=1), "", None, 1),
        (_BASE_TS + timedelta(minutes=2), "not-a-number", "x", 2),
        (_BASE_TS + timedelta(minutes=3), "60", "0", 3),
    ]
    state = fold_nav_text(_EMPTY_DRAWDOWN_STATE, rows)
    # Only 100 and 60 fold in → peak 100, current (100-60)/100 = 40%.
    assert state.running_peak == Decimal("100")
    assert state.latest_nav == Decimal("60")
    assert state.as_pcts() == (Decimal("40"), Decimal("40"))


# ===========================================================================
# 2. SQLite reader: the `since` cursor.
# ===========================================================================


def _snap(i: int, total: Decimal, ts: datetime) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=ts,
        deployment_id=_DEP,
        total_value_usd=total,
        available_cash_usd=Decimal("0"),
        deployed_capital_usd=total,
        wallet_total_value_usd=total,
        value_confidence=ValueConfidence.HIGH,
        chain="arbitrum",
        iteration_number=i,
        cycle_id=f"iter-{i}",
    )


@pytest_asyncio.fixture
async def store(tmp_path: Path) -> SQLiteStore:
    s = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "vib5134.db")))
    await s.initialize()
    yield s
    await s.close()


@pytest.mark.asyncio
async def test_since_returns_only_rows_after_cursor_oldest_first(store: SQLiteStore) -> None:
    for i in range(6):
        await store.save_portfolio_snapshot(_snap(i, Decimal(100 + i), _BASE_TS + timedelta(minutes=i)))

    full, _ = await store.get_nav_series(_DEP)
    assert len(full) == 6
    cursor = (full[2][0], full[2][3])  # (timestamp, id) of the 3rd-oldest row

    tail, truncated = await store.get_nav_series(_DEP, since=cursor)

    assert truncated is False
    # Strictly newer than the cursor → rows 3,4,5 only, still oldest-first.
    assert [r[1] for r in tail] == ["103", "104", "105"]
    assert [r[3] for r in tail] == [full[3][3], full[4][3], full[5][3]]

    # A cursor at the newest row → nothing left to fold.
    newest = (full[-1][0], full[-1][3])
    empty, _ = await store.get_nav_series(_DEP, since=newest)
    assert empty == []


@pytest.mark.asyncio
async def test_since_cursor_excludes_the_cursor_row_no_double_fold(store: SQLiteStore) -> None:
    # portfolio_snapshots is UNIQUE(deployment_id, timestamp) (the writer upserts on
    # conflict), so two rows can NEVER share a timestamp for one deployment. The id
    # half of the (timestamp, id) cursor therefore does one concrete job on SQLite:
    # at the boundary timestamp, ``id > last_id`` excludes the cursor row ITSELF so a
    # fold never double-counts the last row it already folded. (The full composite
    # predicate is still the correct, identical contract the Postgres twin defines —
    # see the PG parity test — and is defensive for any future schema that allows
    # same-timestamp rows.)
    for i in range(4):
        await store.save_portfolio_snapshot(_snap(i, Decimal(100 + i), _BASE_TS + timedelta(minutes=i)))

    full, _ = await store.get_nav_series(_DEP)

    # Cursor exactly AT row 1 (its own timestamp AND id): row 1 must be excluded,
    # and only the strictly-newer rows 2, 3 returned.
    tail, _ = await store.get_nav_series(_DEP, since=(full[1][0], full[1][3]))
    assert full[1][3] not in [r[3] for r in tail], "the cursor row must not be re-folded"
    assert [r[1] for r in tail] == ["102", "103"]


@pytest.mark.asyncio
async def test_since_truncation_keeps_oldest_after_cursor_for_gapless_paging(store: SQLiteStore) -> None:
    # Incremental truncation must keep the OLDEST scan_cap rows after the cursor
    # (contiguous from the cursor) so the fold has no gap and the caller catches up
    # on the next render — the opposite direction from the full scan's newest-kept.
    for i in range(6):
        await store.save_portfolio_snapshot(_snap(i, Decimal(100 + i), _BASE_TS + timedelta(minutes=i)))

    full, _ = await store.get_nav_series(_DEP)
    cursor = (full[0][0], full[0][3])  # after the oldest row → rows 1..5 remain

    tail, truncated = await store.get_nav_series(_DEP, since=cursor, scan_cap=2)

    assert truncated is True
    assert len(tail) == 2
    # Oldest two AFTER the cursor (rows 1,2), not the newest two.
    assert [r[1] for r in tail] == ["101", "102"]


@pytest.mark.asyncio
async def test_get_nav_series_read_serialized_under_db_lock(store: SQLiteStore) -> None:
    # CodeRabbit/VIB-5134: writers hold _db_lock across BEGIN IMMEDIATE … COMMIT on the
    # single shared connection (WAL isolates between connections, not within one), so the
    # read MUST take the same lock — otherwise an incremental cursor could advance past a
    # writer's uncommitted rows that later roll back, permanently skipping them.
    await store.save_portfolio_snapshot(_snap(0, Decimal("100"), _BASE_TS))
    await store.save_portfolio_snapshot(_snap(1, Decimal("90"), _BASE_TS + timedelta(minutes=1)))

    real_lock = store._db_lock
    entered = {"n": 0}

    class _SpyLock:
        def __enter__(self):
            entered["n"] += 1
            return real_lock.__enter__()

        def __exit__(self, *a):
            return real_lock.__exit__(*a)

    store._db_lock = _SpyLock()  # type: ignore[assignment]
    await store.get_nav_series(_DEP)  # full scan
    await store.get_nav_series(_DEP, since=(_BASE_TS, 1))  # incremental
    assert entered["n"] >= 2, "both the full-scan and incremental reads acquire _db_lock"


# ===========================================================================
# 3. Gateway end-to-end: current-drawdown stays live via the incremental fold
#    (Codex finding-1 regression).
# ===========================================================================


def _servicer_over(store: SQLiteStore) -> DashboardServiceServicer:
    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._warm = store
    sm._record_metrics = MagicMock()

    svc = DashboardServiceServicer.__new__(DashboardServiceServicer)
    svc.settings = SimpleNamespace()
    svc._state_manager = sm
    svc._initialized = True
    svc._strategies_root = None
    svc._cached_positions = {}
    return svc


@pytest.mark.asyncio
async def test_new_high_then_fall_between_scans_is_live_via_incremental_fold(store: SQLiteStore) -> None:
    # History is flat $100 → peak 100, current-drawdown 0. The first call full-scans
    # and seeds the checkpoint.
    await store.save_portfolio_snapshot(_snap(0, Decimal("100"), _BASE_TS))
    await store.save_portfolio_snapshot(_snap(1, Decimal("100"), _BASE_TS + timedelta(minutes=1)))

    svc = _servicer_over(store)
    svc._lifetime_dd_ttl_seconds = 1000.0  # keep the 2nd call inside the full-scan TTL

    full_scans = {"n": 0}
    real = svc._state_manager.get_nav_series

    async def _spy(*a: Any, **k: Any) -> Any:
        if k.get("since") is None:
            full_scans["n"] += 1
        return await real(*a, **k)

    svc._state_manager.get_nav_series = _spy  # type: ignore[method-assign]

    seeded = await svc._get_lifetime_drawdown(_DEP)
    assert seeded == (Decimal("0"), Decimal("0"))

    # A NEW high-water mark ($120) lands AFTER the cached scan, then NAV falls to
    # $90 — all before the full-scan TTL expires.
    await store.save_portfolio_snapshot(_snap(2, Decimal("120"), _BASE_TS + timedelta(minutes=2)))
    await store.save_portfolio_snapshot(_snap(3, Decimal("90"), _BASE_TS + timedelta(minutes=3)))

    live = await svc._get_lifetime_drawdown(_DEP)

    # The incremental fold raised the running peak to $120 BEFORE computing the
    # drawdown → current = (120-90)/120 = 25%. A plain "latest NAV vs cached
    # peak($100)" would have reported only (100-90)/100 = 10% (understated).
    assert live == (Decimal("25"), Decimal("25"))
    assert full_scans["n"] == 1, "the new high was captured WITHOUT a second full-history scan"


@pytest.mark.asyncio
async def test_no_new_snapshots_keeps_value_without_full_rescan(store: SQLiteStore) -> None:
    await store.save_portfolio_snapshot(_snap(0, Decimal("100"), _BASE_TS))
    await store.save_portfolio_snapshot(_snap(1, Decimal("80"), _BASE_TS + timedelta(minutes=1)))

    svc = _servicer_over(store)
    svc._lifetime_dd_ttl_seconds = 1000.0

    full_scans = {"n": 0}
    real = svc._state_manager.get_nav_series

    async def _spy(*a: Any, **k: Any) -> Any:
        if k.get("since") is None:
            full_scans["n"] += 1
        return await real(*a, **k)

    svc._state_manager.get_nav_series = _spy  # type: ignore[method-assign]

    first = await svc._get_lifetime_drawdown(_DEP)
    second = await svc._get_lifetime_drawdown(_DEP)

    assert first == second == (Decimal("20"), Decimal("20"))  # (100-80)/100
    assert full_scans["n"] == 1, "an unchanged history must not trigger a second full scan"


@pytest.mark.asyncio
async def test_full_scan_failure_preserves_last_known_good(store: SQLiteStore) -> None:
    # pr-auditor non-blocking item: a transient get_nav_series failure must NOT blank the
    # lifetime tile (degrade to the recent-window) for the full slow TTL. The last
    # successfully-computed lifetime is preserved; the full scan retries on a short backoff.
    await store.save_portfolio_snapshot(_snap(0, Decimal("100"), _BASE_TS))
    await store.save_portfolio_snapshot(_snap(1, Decimal("70"), _BASE_TS + timedelta(minutes=1)))

    svc = _servicer_over(store)
    svc._lifetime_dd_ttl_seconds = 0.0  # force every call onto the full-scan branch

    good = await svc._get_lifetime_drawdown(_DEP)
    assert good == (Decimal("30"), Decimal("30"))  # (100-70)/100

    # Backend now fails on the next (full-scan) call — last-known-good must survive.
    svc._state_manager.get_nav_series = AsyncMock(side_effect=RuntimeError("transient blip"))
    preserved = await svc._get_lifetime_drawdown(_DEP)
    assert preserved == (Decimal("30"), Decimal("30")), "last-known-good lifetime survives a transient failure"
