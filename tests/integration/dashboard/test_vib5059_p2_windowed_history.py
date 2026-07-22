"""Integration tests proving the VIB-5059 Phase 2 windowed / time-travel chart contract.

This module is the executable UAT card for VIB-5059 Phase 2 (see
``docs/internal/uat-cards/VIB-5059-p2.md``). It exercises the **real** production
read/transform stack — the pure ``chart_window`` module, the real on-disk
``SQLiteStore.get_snapshots_in_window``, the ``StateManager`` facade, and the
gateway ``DashboardServiceServicer`` methods — against a deterministically-seeded
SQLite state DB. No network, no Anvil, no live Postgres daemon (the PG path uses
the repo-standard ``_FakeConn`` recorder; live-PG execution is VIB-5099).

Determinism: every seed is built from a FIXED base timestamp (``_BASE``), fixed
values, a fixed spike index, a fixed flat-control index, and fixed boundary
timestamps. No wall-clock, no unseeded randomness. The heavy 60-day seed (17 280
snapshots) is built once per session via an async fixture and reused read-only.

HARD CONSTRAINT: this file asserts the contract; it never weakens an assertion to
make a failing production behaviour pass. A genuine production-bug failure is
reported, not patched here.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest
import pytest_asyncio

from almanak.framework.dashboard.chart_window import (
    DEFAULT_MAX_POINTS,
    MAX_POINTS_CEILING,
    MAX_POINTS_FLOOR,
    TIMEFRAME_SECONDS,
    NavPoint,
    clamp_max_points,
    decimate_nav,
    granularity_for_range,
    validate_window,
)
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager, StateManagerConfig
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

# ---------------------------------------------------------------------------
# Deterministic seed parameters (no wall-clock anywhere)
# ---------------------------------------------------------------------------

_BASE = datetime(2026, 1, 1, tzinfo=UTC)
"""Fixed inception timestamp. Day 0 == _BASE; day N == _BASE + N days."""

_DEPLOYMENT_ID = "deployment:vib5059p2test"

_CADENCE = timedelta(minutes=5)
_PER_DAY = 288  # 24h / 5min
_DAYS = 60
_TOTAL_SNAPSHOTS = _DAYS * _PER_DAY  # 17280

# A flat baseline NAV with one deep drawdown spike planted at a known index.
_FLAT_VALUE = Decimal("1000.00")
_SPIKE_INDEX = 8000  # somewhere in the interior, NOT an endpoint
_SPIKE_VALUE = Decimal("12.34")  # far below neighbours -> a per-bucket min
# A flat-control index in an interior bucket that decimation provably thins away
# (it is neither an anchor nor its bucket's min/max) — used to prove the series
# was thinned, not returned whole. Empirically absent from the decimated output
# for the (17280-row, 1500-budget) shape; the test re-derives it deterministically
# from the decimation policy so the proof never relies on a magic constant.
_FLAT_CONTROL_INDEX = 200


def _marker(text: str) -> None:
    """Emit a card marker to the test's stdout.

    The card runs these with ``-s``. The repo pins ``-n auto`` in ``pytest.ini``;
    under pytest-xdist the marker lands in the test's captured stdout, which the
    controller forwards in the per-test "Captured stdout call" report section
    (surfaced by ``-s`` / ``-rA``). The marker is therefore always present in the
    run output for the assertion the card makes on it; with xdist disabled
    (``-p no:xdist``) ``-s`` streams it live.
    """
    print(text)


def _snap_ts(index: int) -> datetime:
    """Timestamp of the i-th 5-minute snapshot from inception."""
    return _BASE + index * _CADENCE


def _snap_value(index: int) -> Decimal:
    """Deterministic NAV value for the i-th snapshot.

    Flat baseline with one planted drawdown spike. A tiny per-index ripple keeps
    most values distinct so the spike is an unambiguous global/bucket minimum and
    the endpoints are unique, while the spike stays the lone deep trough.
    """
    if index == _SPIKE_INDEX:
        return _SPIKE_VALUE
    # Small deterministic ripple in [+0.00, +2.87], never near the spike.
    return _FLAT_VALUE + Decimal(index % 288) / Decimal(100)


def _day_start(day: int) -> datetime:
    return _BASE + timedelta(days=day)


# ---------------------------------------------------------------------------
# Store / StateManager / servicer construction helpers
# ---------------------------------------------------------------------------


async def _make_store(db_path: str) -> SQLiteStore:
    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    await store.initialize()
    return store


def _make_state_manager(store: SQLiteStore) -> StateManager:
    """Real StateManager wrapping a real on-disk SQLite store as the WARM tier."""
    return StateManager(StateManagerConfig(), warm_backend=store)


async def _seed_snapshot(store: SQLiteStore, index: int, value: Decimal | str) -> None:
    """Persist one snapshot via the production writer at a fixed timestamp."""
    snap = PortfolioSnapshot(
        timestamp=_snap_ts(index),
        deployment_id=_DEPLOYMENT_ID,
        total_value_usd=Decimal(value) if isinstance(value, str) and value != "" else value,
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
        iteration_number=index,
    )
    await store.save_portfolio_snapshot(snap)


async def _table_count(store: SQLiteStore, table: str) -> int:
    cur = store._conn.execute(f"SELECT COUNT(*) AS n FROM {table}")  # noqa: SLF001 — test-only DB introspection
    return int(cur.fetchone()["n"])


def _make_servicer_with_sm(state_manager: StateManager, snapshot: PortfolioSnapshot) -> DashboardServiceServicer:
    """A servicer wired to a REAL StateManager, with the discovery cascade mocked.

    The discovery helpers (``_get_strategy_state_data`` / ``_get_latest_snapshot`` /
    ``_get_portfolio_value_and_pnl`` / ``_get_portfolio_metrics``) are servicer-level
    surfaces unrelated to the windowed NAV read; mocking them keeps the test off the
    filesystem/registry while leaving the real ``_state_manager`` (and therefore the
    real ``get_snapshots_in_window`` / ``get_recent_snapshots`` / ``get_ledger_entries``
    reads under test) intact. A non-None ``_get_latest_snapshot`` makes
    ``build_state_only_strategy_info`` return a real strategy_info so the call does not
    404 even with an empty registry.
    """
    svc = DashboardServiceServicer(GatewaySettings())
    svc._initialized = True
    svc._state_manager = state_manager
    svc._get_strategy_state_data = AsyncMock(return_value=None)
    svc._get_latest_snapshot = AsyncMock(return_value=snapshot)
    svc._get_portfolio_value_and_pnl = AsyncMock(return_value=("1000.00", "0"))
    svc._get_portfolio_metrics = AsyncMock(return_value=None)
    return svc


@asynccontextmanager
async def _discovery_patched():
    """Force the local discovery cascade to miss deterministically."""
    mock_registry = MagicMock()
    mock_registry.get.return_value = None
    with (
        patch(
            "almanak.gateway.services.dashboard_service.get_instance_registry",
            return_value=mock_registry,
        ),
    ):
        yield


def _latest_snapshot() -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=_snap_ts(_TOTAL_SNAPSHOTS - 1),
        deployment_id=_DEPLOYMENT_ID,
        total_value_usd=_snap_value(_TOTAL_SNAPSHOTS - 1),
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
    )


# ---------------------------------------------------------------------------
# Session-scoped heavy 60-day seed (built once, reused read-only)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="session")
async def big_store(tmp_path_factory) -> SQLiteStore:
    """17 280 snapshots (60d @ 5min) with one planted drawdown spike, built once."""
    db_path = str(tmp_path_factory.mktemp("vib5059p2_big") / "big.db")
    store = await _make_store(db_path)
    # Seed in one transaction-batched loop via the production writer.
    for i in range(_TOTAL_SNAPSHOTS):
        await _seed_snapshot(store, i, _snap_value(i))
    return store


@pytest_asyncio.fixture(scope="session")
async def big_sm(big_store: SQLiteStore) -> StateManager:
    return _make_state_manager(big_store)


# ---------------------------------------------------------------------------
# D1.S1 — Windowed NAV fetch: bounds, budget, ordering, anchors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nav_window_bounds_and_budget(big_sm: StateManager) -> None:
    from_dt = _day_start(10)
    to_dt = _day_start(17)
    max_points = 1500

    rows, truncated = await big_sm.get_snapshots_in_window(_DEPLOYMENT_ID, from_dt, to_dt)
    assert not truncated
    assert rows, "7-day window must contain snapshots"

    # Build NavPoints exactly as the gateway builder does, then decimate.
    points = [NavPoint(ts, Decimal(v)) for ts, v, _cash, _c, _pj in rows if v]
    out = decimate_nav(points, clamp_max_points(max_points))

    # Every returned point lies inside the requested window.
    for p in out:
        assert from_dt <= p.timestamp <= to_dt

    # Budget respected.
    assert len(out) <= max_points

    # Strictly ascending, no duplicate timestamps.
    times = [p.timestamp for p in out]
    assert times == sorted(times)
    assert len(set(times)) == len(times)

    # Endpoints are anchors — the window's earliest + latest raw rows verbatim.
    first_raw_ts, first_raw_val, _, _, _ = rows[0]
    last_raw_ts, last_raw_val, _, _, _ = rows[-1]
    assert out[0].timestamp == first_raw_ts
    assert str(out[0].value) == str(Decimal(first_raw_val))
    assert out[-1].timestamp == last_raw_ts
    assert str(out[-1].value) == str(Decimal(last_raw_val))

    _marker(f"NAV_WINDOW_OK points={len(out)} window=[{from_dt.isoformat()},{to_dt.isoformat()}]")


# ---------------------------------------------------------------------------
# D1.S2 — Decimation preserves a planted spike (POINTWISE)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nav_decimation_preserves_spike(big_sm: StateManager) -> None:
    rows, truncated = await big_sm.get_snapshots_in_window(_DEPLOYMENT_ID, None, None)
    assert not truncated
    assert len(rows) == _TOTAL_SNAPSHOTS

    points = [NavPoint(ts, Decimal(v)) for ts, v, _cash, _c, _pj in rows if v]
    out = decimate_nav(points, clamp_max_points(DEFAULT_MAX_POINTS))

    # Decimation actually occurred.
    assert len(out) <= DEFAULT_MAX_POINTS
    assert len(out) < _TOTAL_SNAPSHOTS

    spike_ts = _snap_ts(_SPIKE_INDEX)
    out_by_ts = {p.timestamp: p.value for p in out}

    # The spike survives POINTWISE: exact ts AND exact value string.
    if spike_ts not in out_by_ts or str(out_by_ts[spike_ts]) != str(_SPIKE_VALUE):
        _marker("SPIKE_LOST")
        raise AssertionError(
            f"planted spike not preserved pointwise: ts={spike_ts.isoformat()} "
            f"present={spike_ts in out_by_ts} value={out_by_ts.get(spike_ts)!r} expected={_SPIKE_VALUE!r}"
        )

    # The spike's bucket also yields its bucket-local maximum (both V shoulders).
    # Locate the bucket boundaries used by decimate_nav for this n/budget.
    n = len(points)
    budget = clamp_max_points(DEFAULT_MAX_POINTS)
    num_buckets = (budget - 2) // 3
    b = next(bi for bi in range(num_buckets) if (bi * n) // num_buckets <= _SPIKE_INDEX < ((bi + 1) * n) // num_buckets)
    lo = (b * n) // num_buckets
    hi = ((b + 1) * n) // num_buckets
    bucket_max_idx = max(range(lo, hi), key=lambda i: points[i].value)
    bucket_max_ts = points[bucket_max_idx].timestamp
    assert bucket_max_ts in out_by_ts, "bucket-local max must be retained alongside the spike"

    # A flat-control snapshot was thinned away — proving the series is not whole.
    # _FLAT_CONTROL_INDEX is a deterministically-absent interior point for this
    # (n, budget) shape; re-derive the absent set so the proof is self-checking and
    # not a brittle magic constant.
    flat_ts = _snap_ts(_FLAT_CONTROL_INDEX)
    absent_interior = [i for i in range(_PER_DAY, _SPIKE_INDEX) if _snap_ts(i) not in out_by_ts]
    assert absent_interior, "decimation must thin away some interior snapshot"
    assert flat_ts not in out_by_ts, (
        f"flat-control snapshot (index {_FLAT_CONTROL_INDEX}) must be absent (series was thinned); "
        f"e.g. these interior indices are absent: {absent_interior[:5]}"
    )

    _marker(f"SPIKE_PRESERVED spike_ts={spike_ts.isoformat()} value={_SPIKE_VALUE} out_points={len(out)}")


# ---------------------------------------------------------------------------
# D1.S3 — No-arg default equals a pinned golden baseline (back compat)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture()
async def golden_store(tmp_path) -> SQLiteStore:
    """200 snapshots at known timestamps with distinct values (oldest->newest)."""
    store = await _make_store(str(tmp_path / "golden.db"))
    for i in range(1, 201):  # T[1]..T[200]
        await _seed_snapshot(store, i, Decimal("500") + Decimal(i))  # distinct
    return store


@pytest.mark.asyncio
async def test_default_path_golden_baseline(golden_store: SQLiteStore) -> None:
    sm = _make_state_manager(golden_store)
    latest = PortfolioSnapshot(
        timestamp=_snap_ts(200),
        deployment_id=_DEPLOYMENT_ID,
        total_value_usd=Decimal("700"),
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
    )
    svc = _make_servicer_with_sm(sm, latest)

    async with _discovery_patched():
        # No window fields -> legacy recent-window path (max_points unset == 0).
        request = gateway_pb2.GetStrategyDetailsRequest(
            deployment_id=_DEPLOYMENT_ID, include_pnl_history=True, include_timeline=False
        )
        ctx = MagicMock(spec=grpc.aio.ServicerContext)
        resp = await svc.GetStrategyDetails(request, ctx)

    ctx.set_code.assert_not_called()
    pnl = list(resp.pnl_history)
    # Exactly 168 (the get_recent_snapshots default), oldest-first.
    assert len(pnl) == 168, f"expected 168 default points, got {len(pnl)}"

    # First == T[33] (200-168+1), last == T[200].
    assert pnl[0].timestamp == int(_snap_ts(33).timestamp())
    assert pnl[-1].timestamp == int(_snap_ts(200).timestamp())
    # Boundary values equal seeded values exactly.
    assert pnl[0].value_usd == str(Decimal("500") + Decimal(33))
    assert pnl[-1].value_usd == str(Decimal("500") + Decimal(200))
    # T[1]..T[32] absent.
    emitted_ts = {p.timestamp for p in pnl}
    for i in range(1, 33):
        assert int(_snap_ts(i).timestamp()) not in emitted_ts

    _marker(f"DEFAULT_GOLDEN_OK len={len(pnl)} first_ts={pnl[0].timestamp} last_ts={pnl[-1].timestamp}")


# ---------------------------------------------------------------------------
# D1.S4 — Trade markers fetched for the same window as the chart
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture()
async def markers_store(tmp_path) -> SQLiteStore:
    """Ledger with trades strictly interior to days {5, 12, 14, 30}."""
    store = await _make_store(str(tmp_path / "markers.db"))
    # Strictly-interior timestamps so the EXCLUSIVE lower bound on `since`
    # (timestamp > since) does not matter at the boundary.
    trade_days = {5: 1, 12: 1, 14: 2, 30: 1}  # day -> count
    seq = 0
    for day, count in trade_days.items():
        for k in range(count):
            ts = _day_start(day) + timedelta(hours=6 + k)  # interior of the day
            seq += 1
            entry = LedgerEntry(
                id=f"tx-{seq}",
                cycle_id=f"cycle-{seq}",
                deployment_id=_DEPLOYMENT_ID,
                execution_mode="live",
                timestamp=ts,
                intent_type="SWAP",
                chain="arbitrum",
                protocol="uniswap_v3",
                tx_hash=f"0x{seq:064x}",
                success=True,
                gas_usd="0.01",
                gas_used=100_000,
            )
            await store.save_ledger_entry(entry)
    return store


@pytest.mark.asyncio
async def test_trade_markers_windowed(markers_store: SQLiteStore) -> None:
    sm = _make_state_manager(markers_store)
    svc = DashboardServiceServicer(GatewaySettings())
    svc._initialized = True
    svc._state_manager = sm

    from_ts = int(_day_start(10).timestamp())
    before_ts = int(_day_start(17).timestamp())
    request = gateway_pb2.GetTradeTapeRequest(
        deployment_id=_DEPLOYMENT_ID, from_ts=from_ts, before_timestamp=before_ts, limit=100
    )
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    resp = await svc.GetTradeTape(request, ctx)

    ctx.set_code.assert_not_called()
    # In-window seeded trades: day12 (1) + day14 (2) = 3. day5 and day30 excluded.
    in_window = 3
    assert len(resp.rows) == in_window, f"expected {in_window} in-window markers, got {len(resp.rows)}"
    for row in resp.rows:
        assert from_ts <= row.timestamp < before_ts

    _marker(f"MARKERS_WINDOWED_OK count={len(resp.rows)} window=[{from_ts},{before_ts})")


# ---------------------------------------------------------------------------
# D1.S5 — OHLCV granularity ladder picks a bounded candle count
# ---------------------------------------------------------------------------


def test_granularity_ladder() -> None:
    candle_budget = 720
    ranges = {
        "1h": 3600,
        "24h": 86400,
        "7d": 7 * 86400,
        "30d": 30 * 86400,
        "365d": 365 * 86400,
    }
    for label, range_seconds in ranges.items():
        tf = granularity_for_range(range_seconds, candle_budget)
        secs = TIMEFRAME_SECONDS[tf]
        assert range_seconds / secs <= candle_budget, (
            f"{label}: tf={tf} would request {range_seconds / secs:.0f} candles > {candle_budget}"
        )
    # A 1-year window must NOT pick a 5-minute granularity.
    assert granularity_for_range(365 * 86400, candle_budget) == "1d"

    # Delegation preserved the legacy ohlcv count table byte-for-byte.
    from almanak.framework.dashboard.templates._ohlcv_window import ohlcv_limit_for_timeframe

    legacy = {"1m": 720, "5m": 720, "15m": 720, "1h": 168, "4h": 180, "1d": 120}
    for tf, expected in legacy.items():
        assert ohlcv_limit_for_timeframe(tf) == expected, f"legacy {tf} count drifted"

    _marker("LADDER_OK " + ",".join(f"{k}->{granularity_for_range(v, candle_budget)}" for k, v in ranges.items()))


# ---------------------------------------------------------------------------
# D2.M3 — Window-size sweep (minutes -> full lifetime)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_window_size_sweep(big_sm: StateManager) -> None:
    max_points = 1500
    # (label, from_dt, to_dt)
    full_from = None
    windows = [
        ("15m", _day_start(20), _day_start(20) + timedelta(minutes=15)),
        ("1h", _day_start(20), _day_start(20) + timedelta(hours=1)),
        ("24h", _day_start(20), _day_start(21)),
        ("7d", _day_start(10), _day_start(17)),
        ("30d", _day_start(5), _day_start(35)),
        ("full", full_from, None),
    ]
    for label, from_dt, to_dt in windows:
        rows, truncated = await big_sm.get_snapshots_in_window(_DEPLOYMENT_ID, from_dt, to_dt)
        assert not truncated
        raw_in_window = len(rows)
        assert raw_in_window >= 1, f"{label}: window must contain >=1 snapshot"

        points = [NavPoint(ts, Decimal(v)) for ts, v, _cash, _c, _pj in rows if v]
        out = decimate_nav(points, clamp_max_points(max_points))

        assert out, f"{label}: non-empty window must not decimate to empty"
        assert len(out) <= max_points
        if from_dt is not None:
            for p in out:
                assert from_dt <= p.timestamp
        if to_dt is not None:
            for p in out:
                assert p.timestamp <= to_dt
        times = [p.timestamp for p in out]
        assert times == sorted(times)
        assert len(set(times)) == len(times)

        if label == "15m":
            # Raw rows <= budget -> returned un-decimated (count == raw in-window).
            assert len(out) == raw_in_window, "small window must return raw rows un-decimated"
        if label == "full":
            assert len(out) < raw_in_window, "full lifetime must exercise decimation"

    _marker("SWEEP_OK windows=" + ",".join(w[0] for w in windows))


# ---------------------------------------------------------------------------
# D2.M2 — Postgres backend: SQL shape + row conversion + cross-backend parity
# ---------------------------------------------------------------------------


class _FakeConn:
    """Records (sql, args) of each call; returns canned rows (mirror of repo harness)."""

    def __init__(self, fetch_rows: list | None = None) -> None:
        self.fetch_rows = fetch_rows or []
        self.calls: list[tuple[str, str, tuple]] = []

    async def fetch(self, sql: str, *args):
        self.calls.append(("fetch", sql, args))
        return self.fetch_rows


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


def _make_pg_store(conn: _FakeConn):
    from almanak.framework.state.state_manager import PostgresStore

    store = PostgresStore.__new__(PostgresStore)
    store._pool = conn and _FakePool(conn)  # type: ignore[attr-defined]
    store._initialized = True  # type: ignore[attr-defined]
    return store


@pytest.mark.asyncio
async def test_postgres_window_sql_shape_and_parity(big_sm: StateManager) -> None:
    # --- (a) SQL shape: lower+upper bound, projection, ORDER BY + tiebreak, LIMIT.
    conn = _FakeConn(fetch_rows=[])
    store = _make_pg_store(conn)
    from_dt = _day_start(10)
    to_dt = _day_start(17)
    await store.get_snapshots_in_window(_DEPLOYMENT_ID, from_dt, to_dt)

    kind, sql, args = conn.calls[0]
    assert kind == "fetch"
    assert "FROM portfolio_snapshots" in sql
    assert "timestamp >=" in sql  # lower bound
    assert "timestamp <=" in sql  # upper bound
    assert "total_value_usd::text" in sql  # cast for Empty!=Zero ownership
    assert "available_cash_usd::text" in sql  # VIB-5942: wallet-NAV (total-debt+cash)
    assert "value_confidence" in sql
    # VIB-5170: positions_json::text projected for per-row debt netting; other JSON
    # blobs still excluded (transfer-size discipline).
    assert "positions_json::text" in sql
    assert "token_prices_json" not in sql
    assert "wallet_balances_json" not in sql
    # Deterministic ordering with an id tiebreak; LIMIT/scan-cap present.
    assert "ORDER BY timestamp DESC, id DESC" in sql
    assert "LIMIT $" in sql
    # deployment_id + from + to + (scan_cap+1) limit.
    assert args[0] == _DEPLOYMENT_ID
    assert args[1] == from_dt
    assert args[2] == to_dt
    _marker("PG_SQL_SHAPE_OK")

    # --- (b) cross-backend parity at the logic boundary the live DB would share.
    # Take the SAME logical series the SQLite path returns for a window and feed
    # it through the PG row->type conversion + decimate_nav. Output must match.
    sqlite_rows, _ = await big_sm.get_snapshots_in_window(_DEPLOYMENT_ID, from_dt, to_dt)

    # The PG method reverses a DESC fetch to oldest-first; emulate the DB by
    # handing the fake conn the same rows newest-first.
    pg_db_rows = [
        {
            "timestamp": ts,
            "total_value_text": val,
            "available_cash_text": cash,
            "value_confidence": conf,
            "positions_text": pj,
        }
        for ts, val, cash, conf, pj in reversed(sqlite_rows)
    ]
    conn2 = _FakeConn(fetch_rows=pg_db_rows)
    store2 = _make_pg_store(conn2)
    pg_rows, pg_trunc = await store2.get_snapshots_in_window(_DEPLOYMENT_ID, from_dt, to_dt)
    assert not pg_trunc

    def _to_points(rows):
        return [NavPoint(ts, Decimal(v)) for ts, v, _cash, _c, _pj in rows if v]

    sqlite_out = decimate_nav(_to_points(sqlite_rows), clamp_max_points(1500))
    pg_out = decimate_nav(_to_points(pg_rows), clamp_max_points(1500))
    assert [(p.timestamp, str(p.value)) for p in pg_out] == [(p.timestamp, str(p.value)) for p in sqlite_out]
    _marker(f"PG_PARITY_OK points={len(pg_out)}")

    # --- (c) Empty!=Zero: a PG row with total_value_usd text "" is unmeasured.
    empty_rows = [
        {
            "timestamp": _snap_ts(1),
            "total_value_text": "1000.00",
            "available_cash_text": "0",
            "value_confidence": "HIGH",
            "positions_text": "[]",
        },
        {
            "timestamp": _snap_ts(2),
            "total_value_text": "",
            "available_cash_text": "0",
            "value_confidence": "HIGH",
            "positions_text": "[]",
        },  # unmeasured
        {
            "timestamp": _snap_ts(3),
            "total_value_text": "1001.00",
            "available_cash_text": "0",
            "value_confidence": "HIGH",
            "positions_text": "[]",
        },
    ]
    conn3 = _FakeConn(fetch_rows=list(reversed(empty_rows)))  # DB hands DESC
    store3 = _make_pg_store(conn3)
    rows3, _ = await store3.get_snapshots_in_window(_DEPLOYMENT_ID, None, None)
    pts = _to_points(rows3)  # builder drops "" -> never Decimal("0")
    assert len(pts) == 2, "the empty-text row must be excluded, not parsed to 0"
    assert all(p.value != Decimal("0") for p in pts)
    assert Decimal("0") not in [p.value for p in pts]
    _marker("PG_EMPTY_NOT_ZERO_OK")


# ---------------------------------------------------------------------------
# D3.F1 — Backend read raises during windowed fetch (loud, not swallowed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_store_error_propagates(golden_store: SQLiteStore, caplog) -> None:
    sm = _make_state_manager(golden_store)
    latest = _latest_snapshot()
    svc = _make_servicer_with_sm(sm, latest)

    pre_snaps = await _table_count(golden_store, "portfolio_snapshots")
    pre_ledger = await _table_count(golden_store, "transaction_ledger")

    # Inject: the WARM backend's windowed read raises.
    async def _boom(*a, **k):
        raise RuntimeError("simulated backend failure")

    golden_store.get_snapshots_in_window = _boom  # type: ignore[assignment]

    from_ts = int(_day_start(0).timestamp())
    to_ts = int(_day_start(60).timestamp())
    request = gateway_pb2.GetStrategyDetailsRequest(
        deployment_id=_DEPLOYMENT_ID,
        include_pnl_history=True,
        include_timeline=False,
        from_ts=from_ts,
        to_ts=to_ts,
        max_points=1500,
    )
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    with caplog.at_level(logging.ERROR):
        resp = await svc.GetStrategyDetails(request, ctx)

    # Non-OK status set; NOT an OK response with empty history.
    codes = [c.args[0] for c in ctx.set_code.call_args_list]
    if grpc.StatusCode.UNAVAILABLE not in codes:
        _marker("STORE_ERROR_SWALLOWED")
        raise AssertionError(f"windowed backend error not surfaced as non-OK status; codes={codes}")
    assert len(resp.pnl_history) == 0  # empty StrategyDetails returned WITH the error code
    # ERROR-level log naming the failure.
    assert any(r.levelno >= logging.ERROR for r in caplog.records), "an ERROR log must name the failure"

    # Read path performed zero writes.
    assert await _table_count(golden_store, "portfolio_snapshots") == pre_snaps
    assert await _table_count(golden_store, "transaction_ledger") == pre_ledger

    _marker(f"STORE_ERROR_LOUD code={grpc.StatusCode.UNAVAILABLE.name}")


# ---------------------------------------------------------------------------
# D3.F2 — Inverted window (both bounds, from >= to) -> INVALID_ARGUMENT
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inverted_window_rejected(golden_store: SQLiteStore) -> None:
    sm = _make_state_manager(golden_store)
    latest = _latest_snapshot()
    svc = _make_servicer_with_sm(sm, latest)

    pre_snaps = await _table_count(golden_store, "portfolio_snapshots")
    pre_ledger = await _table_count(golden_store, "transaction_ledger")

    # Inverted: from = day 30, to = day 10, both non-zero, from > to.
    request = gateway_pb2.GetStrategyDetailsRequest(
        deployment_id=_DEPLOYMENT_ID,
        include_pnl_history=True,
        include_timeline=False,
        from_ts=int(_day_start(30).timestamp()),
        to_ts=int(_day_start(10).timestamp()),
        max_points=1500,
    )
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    resp = await svc.GetStrategyDetails(request, ctx)

    codes = [c.args[0] for c in ctx.set_code.call_args_list]
    assert grpc.StatusCode.INVALID_ARGUMENT in codes, f"inverted window must be INVALID_ARGUMENT; codes={codes}"
    assert len(resp.pnl_history) == 0

    # An OPEN bound (from set, to=0) is valid and NOT rejected — the pure
    # validator pins that the two are never conflated.
    validate_window(int(_day_start(10).timestamp()), 0)  # from-only open bound: no raise
    validate_window(0, int(_day_start(10).timestamp()))  # to-only open bound: no raise
    with pytest.raises(ValueError):
        validate_window(int(_day_start(30).timestamp()), int(_day_start(10).timestamp()))

    # Zero writes.
    assert await _table_count(golden_store, "portfolio_snapshots") == pre_snaps
    assert await _table_count(golden_store, "transaction_ledger") == pre_ledger

    _marker("INVERTED_REJECTED")


# ---------------------------------------------------------------------------
# D3.F3 — max_points is bounded: oversized clamps, never unbounded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_max_points_bounded(big_sm: StateManager) -> None:
    rows, _ = await big_sm.get_snapshots_in_window(_DEPLOYMENT_ID, None, None)
    points = [NavPoint(ts, Decimal(v)) for ts, v, _cash, _c, _pj in rows if v]

    # max_points = 10_000_000 -> clamped to CEILING.
    huge = clamp_max_points(10_000_000)
    assert huge == MAX_POINTS_CEILING
    out_huge = decimate_nav(points, huge)
    if len(out_huge) > MAX_POINTS_CEILING:
        _marker("BUDGET_UNBOUNDED")
        raise AssertionError(f"oversized budget not bounded: {len(out_huge)} > {MAX_POINTS_CEILING}")

    # max_points = 1 -> clamped up to the documented floor (2 anchors).
    assert clamp_max_points(1) == MAX_POINTS_FLOOR
    out_floor = decimate_nav(points, 1)
    assert len(out_floor) == MAX_POINTS_FLOOR, "tiny budget honored at the floor, not ignored"
    for p in out_floor:
        # In-window (full lifetime here) — endpoints are the global anchors.
        assert rows[0][0] <= p.timestamp <= rows[-1][0]
    assert out_floor[0].timestamp == rows[0][0]
    assert out_floor[-1].timestamp == rows[-1][0]

    # max_points = 0 -> default recent-window mode, NOT windowed-with-zero-budget.
    # The mode trigger lives at the request layer (max_points > 0). Pin the
    # contract: 0 means legacy default (the D1.S3 golden), 5000 is the ceiling.
    assert MAX_POINTS_CEILING == 5000
    assert DEFAULT_MAX_POINTS == 1500

    _marker(f"MAX_POINTS_BOUNDED_OK ceiling={MAX_POINTS_CEILING} floor={MAX_POINTS_FLOOR} huge_out={len(out_huge)}")


# ---------------------------------------------------------------------------
# D3.F4 — Row-cap truncation is surfaced, never silent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rowcap_truncation_flagged(big_store: SQLiteStore, caplog) -> None:
    # Set the scan-cap below the full in-window row count and request the full window.
    cap = 1000
    rows, truncated = await big_store.get_snapshots_in_window(_DEPLOYMENT_ID, None, None, scan_cap=cap)

    if not truncated:
        _marker("TRUNCATION_SILENT")
        raise AssertionError(f"scan_cap={cap} below {_TOTAL_SNAPSHOTS} rows must flag truncated=True")
    assert len(rows) == cap, "the newest scan_cap rows are returned on truncation"

    # The gateway builder logs a WARNING containing 'truncat' AND increments the metric.
    from almanak.gateway.metrics import DASHBOARD_NAV_HISTORY_TRUNCATED

    # Metric carries a deployment_id label (1 series per gateway instance).
    metric = DASHBOARD_NAV_HISTORY_TRUNCATED.labels(deployment_id=_DEPLOYMENT_ID)
    before_metric = metric._value.get()

    svc = DashboardServiceServicer(GatewaySettings())
    svc._state_manager = _make_state_manager(big_store)
    # Force the facade read to hit the cap by monkeypatching the warm read to
    # pass the low scan_cap (the builder calls it without scan_cap, so wrap it).
    real = big_store.get_snapshots_in_window

    async def _capped(deployment_id, from_ts, to_ts, *, scan_cap=cap):
        return await real(deployment_id, from_ts, to_ts, scan_cap=cap)

    big_store.get_snapshots_in_window = _capped  # type: ignore[assignment]
    try:
        with caplog.at_level(logging.WARNING):
            out = await svc._build_pnl_history(_DEPLOYMENT_ID, from_dt=None, to_dt=None, max_points=1500)
    finally:
        big_store.get_snapshots_in_window = real  # type: ignore[assignment]

    after_metric = metric._value.get()
    assert after_metric == before_metric + 1, "truncation metric must increment"
    assert any("truncat" in r.getMessage().lower() for r in caplog.records), "operator-visible WARNING required"

    # Returned series is still a valid decimation: <= budget, ascending.
    assert 0 < len(out) <= 1500
    out_ts = [p.timestamp for p in out]
    assert out_ts == sorted(out_ts)

    _marker(f"TRUNCATION_FLAGGED cap={cap} returned={len(rows)} metric_delta=1")


# ---------------------------------------------------------------------------
# D3.F5 — Non-empty window must never return empty output
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nonempty_window_never_empty(tmp_path) -> None:
    # 1- and 2-row cases: returned verbatim un-decimated.
    store1 = await _make_store(str(tmp_path / "one.db"))
    await _seed_snapshot(store1, 0, Decimal("100"))
    sm1 = _make_state_manager(store1)
    rows1, _ = await sm1.get_snapshots_in_window(_DEPLOYMENT_ID, None, None)
    out1 = decimate_nav([NavPoint(ts, Decimal(v)) for ts, v, _cash, _c, _pj in rows1 if v], clamp_max_points(1500))
    assert len(out1) == 1, "1-row window returns that row, never empty"

    store2 = await _make_store(str(tmp_path / "two.db"))
    await _seed_snapshot(store2, 0, Decimal("100"))
    await _seed_snapshot(store2, 1, Decimal("101"))
    sm2 = _make_state_manager(store2)
    rows2, _ = await sm2.get_snapshots_in_window(_DEPLOYMENT_ID, None, None)
    out2 = decimate_nav([NavPoint(ts, Decimal(v)) for ts, v, _cash, _c, _pj in rows2 if v], clamp_max_points(1500))
    assert len(out2) == 2, "2-row window returns both rows, never empty"

    # max_points+1 case (over budget): returns <= budget but >= 2 (anchors).
    budget = 4  # small budget so we can seed budget+1 rows cheaply
    storeN = await _make_store(str(tmp_path / "n.db"))
    for i in range(budget + 1):
        await _seed_snapshot(storeN, i, Decimal("100") + Decimal(i))
    smN = _make_state_manager(storeN)
    rowsN, _ = await smN.get_snapshots_in_window(_DEPLOYMENT_ID, None, None)
    assert len(rowsN) == budget + 1
    outN = decimate_nav([NavPoint(ts, Decimal(v)) for ts, v, _cash, _c, _pj in rowsN if v], budget)
    assert outN, "over-budget non-empty window must not decimate to empty"
    assert len(outN) <= budget
    assert len(outN) >= 2

    _marker(f"NONEMPTY_OK one={len(out1)} two={len(out2)} over_budget={len(outN)}/{budget}")


# ---------------------------------------------------------------------------
# D3.F6 — Empty != Zero silent-coercion guard (mandatory)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_not_zero_guard(tmp_path, caplog) -> None:
    store = await _make_store(str(tmp_path / "empty.db"))
    # Three measured rows + one with total_value_usd = "" (unmeasured).
    await _seed_snapshot(store, 0, Decimal("100"))
    await _seed_snapshot(store, 1, Decimal("101"))
    await _seed_snapshot(store, 2, Decimal("102"))

    # The typed model will not let us store "" via Decimal; insert raw SQL so the
    # TEXT NOT NULL column holds an empty string (the Empty!=Zero case).
    store._conn.execute(  # noqa: SLF001 — deliberately bypass the typed writer for "" seeding
        """
        INSERT INTO portfolio_snapshots (
            deployment_id, timestamp, iteration_number, total_value_usd,
            available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
            value_confidence, positions_json, token_prices_json,
            wallet_balances_json, chain, created_at, cycle_id, execution_mode
        ) VALUES (?, ?, ?, '', '0', '0', '0', 'HIGH', '[]', '{}', '[]', '', ?, '', '')
        """,
        (_DEPLOYMENT_ID, _snap_ts(3).isoformat(), 3, _snap_ts(3).isoformat()),
    )
    store._conn.commit()

    sm = _make_state_manager(store)
    pre_snaps = await _table_count(store, "portfolio_snapshots")
    pre_ledger = await _table_count(store, "transaction_ledger")
    assert pre_snaps == 4  # 3 measured + 1 empty

    svc = DashboardServiceServicer(GatewaySettings())
    svc._state_manager = sm

    with caplog.at_level(logging.WARNING):
        out = await svc._build_pnl_history(_DEPLOYMENT_ID, from_dt=None, to_dt=None, max_points=1500)

    # The unmeasured point is EXCLUDED (count drops by 1) — not coerced to 0.
    assert len(out) == 3, f"empty-text snapshot must be excluded; got {len(out)} points"
    emitted_values = [Decimal(p.value_usd) for p in out]
    if Decimal("0") in emitted_values or "0.0" in [p.value_usd for p in out] or "0" in [p.value_usd for p in out]:
        _marker("COERCED_TO_ZERO")
        raise AssertionError(f"empty NAV coerced to a fake $0 trough: values={[p.value_usd for p in out]}")
    # A WARNING naming the Empty!=Zero drop was logged.
    assert any("Empty!=Zero" in r.getMessage() for r in caplog.records), "Empty!=Zero WARNING required"

    # Zero writes.
    assert await _table_count(store, "portfolio_snapshots") == pre_snaps
    assert await _table_count(store, "transaction_ledger") == pre_ledger

    _marker(f"EMPTY_NOT_ZERO_OK kept={len(out)} excluded=1")


@pytest.mark.asyncio
async def test_scan_cap_must_be_positive(golden_store: SQLiteStore) -> None:
    # Defensive guard (CodeRabbit): a non-positive scan_cap is a caller error, not
    # an ambiguous SQL LIMIT — fail fast with a deterministic ValueError.
    for bad in (0, -1):
        with pytest.raises(ValueError, match="scan_cap"):
            await golden_store.get_snapshots_in_window(_DEPLOYMENT_ID, None, None, scan_cap=bad)
