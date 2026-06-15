"""VIB-5118 regression: lifetime drawdown / high-watermark over FULL history.

The dashboard PnL loader feeds ``compute_pnl_summary`` the recent 168-row
snapshot window (``get_recent_snapshots``, ~14h at the 5-min cadence). The
drawdown recurrence (``_drawdowns``) therefore only ever saw the recent window,
so a lifetime peak or peak-to-trough drawdown OLDER than ~14h was silently
understated — a strategy that crashed 40% on day one then stabilised showed a
near-zero "max drawdown" once the crash scrolled out of the window.

The fix: the gateway computes the lifetime drawdown over the WHOLE snapshot
history via ``get_nav_series`` + ``lifetime_drawdowns_from_nav_text`` (the SAME
recurrence ``_drawdowns`` runs, just fed every row), and threads it into
``compute_pnl_summary`` which prefers it over the windowed value. When the full
series is unavailable the summary degrades to the recent-window drawdown.

These tests pin: (1) the pure recurrence captures an old peak; (2) the lifetime
text path is byte-for-byte equal to ``_drawdowns`` over the same series;
(3) ``compute_pnl_summary`` prefers the lifetime value and falls back without it;
(4) the SQLite ``get_nav_series`` reader returns the full series oldest-first as
raw text (Empty≠Zero) with newest-kept truncation; (5) the facade degrades
gracefully; (6) the loader computes the lifetime drawdown end-to-end over a real
backend where the recent window alone would understate it.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from almanak.framework.dashboard.quant_aggregations import (
    _drawdowns,
    compute_pnl_summary,
    lifetime_drawdowns_from_nav_text,
)
from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

_DEP = "deployment:vib5118-lifetime-dd"
_BASE_TS = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
_RECENT_CAP = 168


# ---------------------------------------------------------------------------
# A full history where the lifetime peak + crash live BEFORE the recent window.
# 250 snapshots: snap 0 = $100 peak, snaps 1..81 = $60 trough (40% drawdown),
# snaps 82..249 = flat $95. The recent 168 (snaps 82..249) is flat → windowed
# drawdown is 0%; the lifetime drawdown is 40% (peak $100 → trough $60), current
# drawdown 5% (peak $100 → last $95).
# ---------------------------------------------------------------------------
_TOTAL_COUNT = 250
_PEAK = Decimal("100")
_TROUGH = Decimal("60")
_RECENT = Decimal("95")


def _total_for(i: int) -> Decimal:
    if i == 0:
        return _PEAK
    if i < 82:
        return _TROUGH
    return _RECENT


def _nav_text_rows() -> list[tuple[datetime, str | None, str | None]]:
    # (timestamp, total_value_usd_text, available_cash_usd_text) oldest-first,
    # cash 0 so wallet-NAV == total.
    return [(_BASE_TS + timedelta(minutes=i), str(_total_for(i)), "0") for i in range(_TOTAL_COUNT)]


def _snap(i: int) -> PortfolioSnapshot:
    total = _total_for(i)
    return PortfolioSnapshot(
        timestamp=_BASE_TS + timedelta(minutes=i),
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


# ---------------------------------------------------------------------------
# 1. Pure recurrence: an old peak/crash is captured by the full series and lost
#    by the recent window.
# ---------------------------------------------------------------------------


def test_lifetime_text_path_captures_peak_older_than_recent_window() -> None:
    rows = _nav_text_rows()

    max_dd, current_dd = lifetime_drawdowns_from_nav_text(rows)
    assert max_dd == Decimal("40")  # ($100 - $60) / $100
    assert current_dd == Decimal("5")  # ($100 - $95) / $100

    # The recent-168 window (flat $95) understates it to zero — the bug.
    windowed_snaps = [_snap(i) for i in range(_TOTAL_COUNT)][-_RECENT_CAP:]
    win_max, win_current = _drawdowns(windowed_snaps)
    assert win_max == Decimal("0")
    assert win_current == Decimal("0")


# ---------------------------------------------------------------------------
# 2. Golden equivalence: the lifetime text path == _drawdowns over the SAME
#    series. The lifetime number is only "_drawdowns fed more rows".
# ---------------------------------------------------------------------------


def test_lifetime_text_path_matches_drawdowns_byte_for_byte() -> None:
    rows = _nav_text_rows()
    snaps = [_snap(i) for i in range(_TOTAL_COUNT)]

    assert lifetime_drawdowns_from_nav_text(rows) == _drawdowns(snaps)


def test_lifetime_text_path_empty_not_zero() -> None:
    # Empty≠Zero: "", None, and garbage parse to 0 and are filtered (<2 valid
    # samples → (0, 0)), exactly as _drawdowns treats unmeasured snapshots.
    rows: list[tuple[Any, str | None, str | None]] = [
        (_BASE_TS, "", None),
        (_BASE_TS, None, ""),
        (_BASE_TS, "not-a-number", "x"),
    ]
    assert lifetime_drawdowns_from_nav_text(rows) == (Decimal("0"), Decimal("0"))


# ---------------------------------------------------------------------------
# 3. compute_pnl_summary prefers the lifetime value; falls back without it.
# ---------------------------------------------------------------------------


def test_compute_pnl_summary_prefers_lifetime_drawdown() -> None:
    windowed_snaps = [_snap(i) for i in range(_TOTAL_COUNT)][-_RECENT_CAP:]

    with_lifetime = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=windowed_snaps,
        ledger_entries=[],
        accounting_events=[],
        lifetime_drawdown=(Decimal("40"), Decimal("5")),
    )
    assert with_lifetime.max_drawdown_pct == Decimal("40")
    assert with_lifetime.current_drawdown_pct == Decimal("5")

    # No lifetime value → windowed fallback (the documented graceful degrade),
    # byte-for-byte the legacy behaviour.
    without = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=windowed_snaps,
        ledger_entries=[],
        accounting_events=[],
    )
    assert without.max_drawdown_pct == Decimal("0")
    assert without.current_drawdown_pct == Decimal("0")


# ---------------------------------------------------------------------------
# 4. SQLite get_nav_series reader.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def store(tmp_path: Path) -> SQLiteStore:
    s = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "vib5118.db")))
    await s.initialize()
    yield s
    await s.close()


async def _seed(store: SQLiteStore, count: int) -> None:
    for i in range(count):
        await store.save_portfolio_snapshot(_snap(i))


@pytest.mark.asyncio
async def test_get_nav_series_returns_full_history_oldest_first(store: SQLiteStore) -> None:
    await _seed(store, _TOTAL_COUNT)

    rows, truncated = await store.get_nav_series(_DEP)

    assert truncated is False
    assert len(rows) == _TOTAL_COUNT  # the WHOLE history, not the 168 window
    # Oldest-first: the $100 peak is row 0, the flat-$95 tail is last.
    assert rows[0][1] == "100"
    assert rows[-1][1] == "95"
    assert [r[0] for r in rows] == sorted(r[0] for r in rows)


@pytest.mark.asyncio
async def test_get_nav_series_truncation_keeps_newest(store: SQLiteStore) -> None:
    await _seed(store, _TOTAL_COUNT)

    rows, truncated = await store.get_nav_series(_DEP, scan_cap=10)

    assert truncated is True
    assert len(rows) == 10
    # Newest scan_cap kept (right edge present for the current-drawdown term),
    # still emitted oldest-first.
    assert [r[0] for r in rows] == sorted(r[0] for r in rows)
    assert rows[-1][0] == _BASE_TS + timedelta(minutes=_TOTAL_COUNT - 1)

    with pytest.raises(ValueError):
        await store.get_nav_series(_DEP, scan_cap=0)


@pytest.mark.asyncio
async def test_get_nav_series_returns_raw_text_not_parsed(store: SQLiteStore) -> None:
    # A high-precision decimal must survive as its exact stored text — proof the
    # reader projects raw text and never round-trips through float.
    precise = Decimal("1.123456789012345")
    snap = _snap(0)
    snap.total_value_usd = precise
    snap.available_cash_usd = Decimal("0.000000000000001")
    await store.save_portfolio_snapshot(snap)

    rows, _ = await store.get_nav_series(_DEP)

    # "1.123456789012345" is not representable in float64 (which would round to
    # 1.1234567890123449…), so an exact-string round-trip proves no float hop.
    assert rows[0][1] == "1.123456789012345"
    assert Decimal(rows[0][1]) == precise
    assert Decimal(rows[0][2]) == Decimal("0.000000000000001")


# ---------------------------------------------------------------------------
# 5. Facade degrades gracefully (default header metric, not operator time-travel).
# ---------------------------------------------------------------------------


def _bare_state_manager(warm: Any) -> StateManager:
    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._metrics = []
    sm._config = SimpleNamespace(metrics_callback=None)
    sm._warm = warm
    sm._record_metrics = MagicMock()
    return sm


@pytest.mark.asyncio
async def test_facade_get_nav_series_empty_when_warm_lacks_method() -> None:
    sm = _bare_state_manager(SimpleNamespace())  # no get_nav_series attribute
    assert await sm.get_nav_series(_DEP) == ([], False)
    # The config-error branch records a failure metric too (honours the docstring
    # + parity with get_snapshots_in_window) — the degrade is observable, not silent.
    assert sm._record_metrics.called


@pytest.mark.asyncio
async def test_facade_get_nav_series_swallows_backend_error() -> None:
    class _Boom:
        async def get_nav_series(self, deployment_id: str, *, scan_cap: int = 200_000):
            raise RuntimeError("db down")

    sm = _bare_state_manager(_Boom())
    # Graceful: returns ([], False) rather than raising — the loader then leaves
    # lifetime_drawdown None and the summary falls back to the windowed value.
    assert await sm.get_nav_series(_DEP) == ([], False)
    assert sm._record_metrics.called


# ---------------------------------------------------------------------------
# 6. End-to-end loader integration over a REAL SQLite backend: the lifetime
#    drawdown is computed over the full history where the recent window alone
#    would report ~0.
# ---------------------------------------------------------------------------


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
async def test_get_lifetime_drawdown_over_full_history(store: SQLiteStore) -> None:
    await _seed(store, _TOTAL_COUNT)
    svc = _servicer_over(store)

    # The shared loader still fetches the recent 168-row window for the live tiles
    # and returns the 5-tuple — it does NOT carry the lifetime drawdown.
    (_metrics, snapshots, _ledger, _events, _pos) = await svc._load_quant_inputs(_DEP)
    assert len(snapshots) == _RECENT_CAP

    # The PnL-only accessor computes the lifetime drawdown over the WHOLE history.
    lifetime_drawdown = await svc._get_lifetime_drawdown(_DEP)
    assert lifetime_drawdown == (Decimal("40"), Decimal("5"))

    # Threading it into compute_pnl_summary surfaces the lifetime figure; the
    # windowed-only path (lifetime_drawdown=None) would have reported zero.
    fixed = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=snapshots,
        ledger_entries=_ledger,
        accounting_events=_events,
        lifetime_drawdown=lifetime_drawdown,
    )
    assert fixed.max_drawdown_pct == Decimal("40")
    assert fixed.current_drawdown_pct == Decimal("5")

    buggy = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=snapshots,
        ledger_entries=_ledger,
        accounting_events=_events,
    )
    assert buggy.max_drawdown_pct == Decimal("0")


@pytest.mark.asyncio
async def test_shared_loader_does_not_scan_nav_series_only_pnl_accessor_does(store: SQLiteStore) -> None:
    # Codex review of PR #2801: only GetPnLSummary surfaces drawdown, so the
    # expensive full-history get_nav_series scan must NOT fire from the shared
    # quant-input load (which GetCostStack / GetAuditPosture also use and which
    # would discard the result). It must fire only from the PnL-only accessor.
    await _seed(store, _TOTAL_COUNT)
    svc = _servicer_over(store)

    calls: list[str] = []
    real = svc._state_manager.get_nav_series

    async def _spy(*a: Any, **k: Any) -> Any:
        calls.append("get_nav_series")
        return await real(*a, **k)

    svc._state_manager.get_nav_series = _spy  # type: ignore[method-assign]

    await svc._load_quant_inputs(_DEP)
    assert calls == [], "the shared quant-input load must not scan the full NAV series"

    await svc._get_lifetime_drawdown(_DEP)
    assert calls == ["get_nav_series"], "the PnL-only accessor is the sole caller of get_nav_series"


@pytest.mark.asyncio
async def test_get_lifetime_drawdown_caches_within_ttl(store: SQLiteStore) -> None:
    # Rapid PnL polls within the TTL coalesce to a single scan (the bounded-load
    # discipline the gateway is designed around).
    await _seed(store, _TOTAL_COUNT)
    svc = _servicer_over(store)

    scans = {"n": 0}
    real = svc._state_manager.get_nav_series

    async def _count(*a: Any, **k: Any) -> Any:
        scans["n"] += 1
        return await real(*a, **k)

    svc._state_manager.get_nav_series = _count  # type: ignore[method-assign]

    first = await svc._get_lifetime_drawdown(_DEP)
    second = await svc._get_lifetime_drawdown(_DEP)
    assert first == second == (Decimal("40"), Decimal("5"))
    assert scans["n"] == 1, "second call within TTL must be served from cache"


@pytest.mark.asyncio
async def test_get_lifetime_drawdown_warns_when_nav_series_truncated(store: SQLiteStore, caplog) -> None:
    # When get_nav_series hits its scan_cap the lifetime drawdown is computed
    # over the newest window, NOT full history — the accessor MUST surface that
    # loudly (operator WARNING) so a truncated figure is never read as lifetime.
    await _seed(store, _TOTAL_COUNT)
    svc = _servicer_over(store)

    truncated_rows = _nav_text_rows()
    svc._state_manager.get_nav_series = AsyncMock(return_value=(truncated_rows, True))

    with caplog.at_level(logging.WARNING):
        lifetime_drawdown = await svc._get_lifetime_drawdown(_DEP)

    # Still computed from the (truncated) rows…
    assert lifetime_drawdown == (Decimal("40"), Decimal("5"))
    # …and the truncation is logged, not silent.
    assert any("truncat" in r.getMessage().lower() for r in caplog.records), (
        "accessor must emit an operator WARNING when the NAV series is truncated"
    )
