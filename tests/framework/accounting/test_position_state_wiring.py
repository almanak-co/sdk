"""Track C SDK wiring tests (VIB-3891).

Covers:

* SQLite ``save_position_state_snapshots`` round-trip.
* ``materialise_position_state`` accepts ``PositionValue`` shape and returns
  a row keyed by ``details["position_id"]`` when the direct attribute is
  missing.
* The materializer's hosted-mode short-circuit fires; local mode produces
  rows.
* The runner caller ``_persist_position_state_snapshots`` is a no-op when
  the state manager doesn't expose ``save_position_state_snapshots``.
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.accounting.position_state import (
    PositionStateRow,
    materialise_position_state,
)
from almanak.framework.runner.runner_state import _persist_position_state_snapshots
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

# ─── SQLite save path ─────────────────────────────────────────────────────


def _insert_parent_snapshot(path: Path, snapshot_id: int) -> None:
    """Insert a minimal portfolio_snapshots row so Track C inserts can
    satisfy the FK constraint. Uses a direct sqlite3 connection so we
    bypass the SQLiteStore's higher-level write API (which expects a
    full PortfolioSnapshot dataclass)."""
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT OR REPLACE INTO portfolio_snapshots (id, deployment_id, "
        "execution_mode, timestamp, iteration_number, total_value_usd, "
        "available_cash_usd, deployed_capital_usd, value_confidence, "
        "positions_json, token_prices_json, wallet_balances_json, chain, created_at) "
        "VALUES (?, 'dep-A', 'live', '2026-05-01T00:00:00Z', 0, '10', '0', '10', "
        "'HIGH', '[]', '{}', '{}', 'arbitrum', '2026-05-01T00:00:00Z')",
        (snapshot_id,),
    )
    conn.commit()
    conn.close()


@pytest.fixture
def loop():
    """Explicit, function-scoped event loop shared across the ``sqlite_store``
    fixture (setup + teardown) and the test body. ``SQLiteStore`` opens its
    aiosqlite connection on one loop and every subsequent call must reuse it,
    so both the fixture and the test depend on this single loop object rather
    than ``asyncio.get_event_loop()`` (removed in 3.12 when no loop is set,
    which broke under pytest-xdist)."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        asyncio.set_event_loop(None)
        loop.close()


@pytest.fixture
def sqlite_store(loop):
    """Initialised SQLite store on a tmp file. Yields the store and path."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    path = Path(tmp.name)
    store = SQLiteStore(SQLiteConfig(db_path=str(path)))
    loop.run_until_complete(store.initialize())
    try:
        yield store, path
    finally:
        loop.run_until_complete(store.close())
        path.unlink(missing_ok=True)


def _row(**overrides) -> PositionStateRow:
    base = {
        "snapshot_id": None,
        "deployment_id": "dep-A",
        "cycle_id": "cyc-1",
        "timestamp": datetime(2026, 5, 1, tzinfo=UTC),
        "position_id": "pos-1",
        "position_type": "LP",
        "current_tick": 1234,
        "in_range": True,
        "liquidity": 999_999_999,
        "supply_balance": Decimal("100"),
        "borrow_balance": Decimal("50"),
        "health_factor": Decimal("1.85"),
        "supply_apy_pct": Decimal("3.5"),
        "borrow_apy_pct": Decimal("4.2"),
        "value_confidence": "HIGH",
    }
    base.update(overrides)
    return PositionStateRow(**base)


def test_sqlite_round_trip_single_row(loop, sqlite_store):
    store, path = sqlite_store
    _insert_parent_snapshot(path, snapshot_id=42)
    rows = [_row()]
    written = loop.run_until_complete(store.save_position_state_snapshots(snapshot_id=42, rows=rows))
    assert written == 1
    out = loop.run_until_complete(store.get_position_state_snapshots(snapshot_id=42))
    assert len(out) == 1
    r = out[0]
    assert r["position_id"] == "pos-1"
    assert r["position_type"] == "LP"
    # SQLite stores bools as 0/1 — the round-trip yields the int.
    assert r["in_range"] == 1
    assert r["liquidity"] == "999999999"
    assert r["health_factor"] == "1.85"
    assert r["value_confidence"] == "HIGH"


def test_sqlite_round_trip_preserves_null_distinction(loop, sqlite_store):
    """Null fields must come back as None, not as 0 / empty string —
    ESTIMATED-vs-unmeasured is a real distinction (CLAUDE.md "Empty ≠
    zero")."""
    store, path = sqlite_store
    _insert_parent_snapshot(path, snapshot_id=1)
    rows = [
        _row(
            in_range=None,
            health_factor=None,
            supply_apy_pct=None,
            current_tick=None,
        )
    ]
    loop.run_until_complete(store.save_position_state_snapshots(snapshot_id=1, rows=rows))
    out = loop.run_until_complete(store.get_position_state_snapshots(snapshot_id=1))
    assert out[0]["in_range"] is None
    assert out[0]["health_factor"] is None
    assert out[0]["supply_apy_pct"] is None
    assert out[0]["current_tick"] is None


def test_sqlite_save_empty_rows_returns_zero(loop, sqlite_store):
    """Empty input is a measured zero, not an error — strategies that
    hold only cash legitimately have zero open positions."""
    store, _ = sqlite_store
    written = loop.run_until_complete(store.save_position_state_snapshots(snapshot_id=99, rows=[]))
    assert written == 0


def test_sqlite_save_bulk_round_trip(loop, sqlite_store):
    """Bulk insert of 5 rows lands in one transaction; per-position
    filter works."""
    store, path = sqlite_store
    _insert_parent_snapshot(path, snapshot_id=10)
    rows = [_row(position_id=f"pos-{i}", in_range=(i % 2 == 0)) for i in range(5)]
    loop.run_until_complete(store.save_position_state_snapshots(snapshot_id=10, rows=rows))
    all_out = loop.run_until_complete(store.get_position_state_snapshots(snapshot_id=10))
    assert len(all_out) == 5
    one = loop.run_until_complete(store.get_position_state_snapshots(snapshot_id=10, position_id="pos-3"))
    assert len(one) == 1
    assert one[0]["position_id"] == "pos-3"


# ─── Materializer accepts PositionValue ───────────────────────────────────


class _PositionValueLike:
    """Mimics the shape of ``portfolio.models.PositionValue`` minimally."""

    def __init__(self, *, position_type, details, protocol="uniswap_v3", chain="arbitrum", label="WETH/USDC"):
        self.position_type = position_type
        self.details = details
        self.protocol = protocol
        self.chain = chain
        self.label = label


def test_materializer_extracts_position_id_from_details():
    """``PositionValue`` keeps protocol-specific identifiers under
    ``details`` (NFT id for Uniswap V3, market for GMX). The materializer
    must read those rather than expecting a top-level attribute."""
    pos = _PositionValueLike(
        position_type="LP",
        details={"position_id": "uniV3-nft-12345", "current_tick": 100, "in_range": True},
    )
    row = materialise_position_state(
        position=pos,
        market=None,
        prices=None,
        deployment_id="d",
        cycle_id="c",
        timestamp=datetime.now(UTC),
    )
    assert row is not None
    assert row.position_id == "uniV3-nft-12345"
    assert row.position_type == "LP"
    assert row.current_tick == 100


def test_materializer_falls_back_to_protocol_chain_label():
    """A lending PositionValue without a position_id field uses
    ``protocol:chain:label`` as last-resort identity."""
    pos = _PositionValueLike(
        position_type="SUPPLY",
        details={"health_factor": "1.9"},
        protocol="aave_v3",
        chain="arbitrum",
        label="WETH supply",
    )
    row = materialise_position_state(
        position=pos,
        market=None,
        prices=None,
        deployment_id="d",
        cycle_id="c",
        timestamp=datetime.now(UTC),
    )
    assert row is not None
    assert row.position_id == "aave_v3:arbitrum:WETH supply"
    # SUPPLY collapses to LENDING per VIB-3891 classifier update.
    assert row.position_type == "LENDING"
    assert row.health_factor == Decimal("1.9")


def test_materializer_returns_none_for_unrecognised_position_type():
    pos = _PositionValueLike(position_type="VAULT", details={"position_id": "v-1"})
    row = materialise_position_state(
        position=pos,
        market=None,
        prices=None,
        deployment_id="d",
        cycle_id="c",
        timestamp=datetime.now(UTC),
    )
    assert row is None


# ─── Hosted-mode short-circuit ────────────────────────────────────────────


def test_materializer_short_circuits_in_hosted_mode(monkeypatch):
    """VIB-3866 / Codex Finding 2: hosted mode returns None and fires
    the unavailability gauge — Track C cannot write until VIB-3871's
    metrics-database PR ships."""
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-test-hosted")
    pos = _PositionValueLike(
        position_type="LP",
        details={"position_id": "p", "current_tick": 1, "in_range": True},
    )
    row = materialise_position_state(
        position=pos,
        market=None,
        prices=None,
        deployment_id="d",
        cycle_id="c",
        timestamp=datetime.now(UTC),
    )
    assert row is None


def test_materializer_runs_in_local_mode(monkeypatch):
    """Local mode (no ALMANAK_IS_HOSTED env) must produce a row when the position
    is recognised — proves the hosted short-circuit doesn't fire on the
    happy path."""
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)
    pos = _PositionValueLike(
        position_type="LP",
        details={"position_id": "p", "current_tick": 1, "in_range": True},
    )
    row = materialise_position_state(
        position=pos,
        market=None,
        prices=None,
        deployment_id="d",
        cycle_id="c",
        timestamp=datetime.now(UTC),
    )
    assert row is not None
    assert row.in_range is True


# ─── Runner caller no-op when backend lacks the method ────────────────────


@pytest.mark.asyncio
async def test_runner_caller_noops_when_state_manager_lacks_method():
    """A state manager without ``save_position_state_snapshots`` returns
    0 — that's the "table absent" path for older backends. Caller must
    not crash, and the cell matrix stays at XFAIL by design."""
    runner = MagicMock()
    runner.state_manager = MagicMock(spec=[])  # no save_position_state_snapshots
    runner.deployment_id = "dep"
    runner._last_cycle_id = "cyc"
    runner._current_strategy = None

    snapshot = MagicMock()
    snapshot.timestamp = datetime.now(UTC)
    snapshot.deployment_id = "dep"
    snapshot.cycle_id = "cyc"
    snapshot.positions = []

    result = await _persist_position_state_snapshots(runner, snapshot, snapshot_id=1)
    assert result == 0


@pytest.mark.asyncio
async def test_runner_caller_noops_when_no_open_positions():
    """A snapshot with no positions writes nothing — measured zero, not
    an error."""
    runner = MagicMock()
    runner.state_manager = MagicMock()
    runner.state_manager.save_position_state_snapshots = AsyncMock(return_value=0)
    runner.deployment_id = "dep"
    runner._last_cycle_id = "cyc"

    snapshot = MagicMock()
    snapshot.timestamp = datetime.now(UTC)
    snapshot.deployment_id = "dep"
    snapshot.positions = []

    result = await _persist_position_state_snapshots(runner, snapshot, snapshot_id=1)
    assert result == 0
    runner.state_manager.save_position_state_snapshots.assert_not_called()


@pytest.mark.asyncio
async def test_runner_caller_writes_rows_for_open_positions(monkeypatch):
    """Happy path: state manager has the method, snapshot has 2
    positions, both materialize, one save call lands with both rows."""
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)

    saved_rows: list = []

    async def _save(snapshot_id, rows):
        saved_rows.extend(rows)
        return len(rows)

    runner = MagicMock()
    runner.state_manager = MagicMock()
    runner.state_manager.save_position_state_snapshots = _save
    runner.deployment_id = "dep"
    runner._last_cycle_id = "cyc"
    runner._current_strategy = None

    pos1 = _PositionValueLike(
        position_type="LP",
        details={"position_id": "p1", "current_tick": 100, "in_range": True},
    )
    pos2 = _PositionValueLike(
        position_type="SUPPLY",
        details={"health_factor": "1.5"},
        protocol="aave_v3",
        chain="arbitrum",
        label="WETH supply",
    )

    snapshot = MagicMock()
    snapshot.timestamp = datetime.now(UTC)
    snapshot.deployment_id = "dep"
    snapshot.positions = [pos1, pos2]

    result = await _persist_position_state_snapshots(runner, snapshot, snapshot_id=99)
    assert result == 2
    assert len(saved_rows) == 2
    assert {r.position_type for r in saved_rows} == {"LP", "LENDING"}


@pytest.mark.asyncio
async def test_runner_caller_swallows_save_errors(monkeypatch):
    """A Track C save failure must not regress the equity curve (the
    parent snapshot has already been persisted at this point). Caller
    logs and returns 0."""
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)

    async def _save(snapshot_id, rows):
        raise RuntimeError("disk full")

    runner = MagicMock()
    runner.state_manager = MagicMock()
    runner.state_manager.save_position_state_snapshots = _save
    runner.deployment_id = "dep"
    runner._last_cycle_id = "cyc"
    runner._current_strategy = None

    pos = _PositionValueLike(
        position_type="LP",
        details={"position_id": "p", "in_range": True},
    )
    snapshot = MagicMock()
    snapshot.timestamp = datetime.now(UTC)
    snapshot.deployment_id = "dep"
    snapshot.positions = [pos]

    # Must NOT raise.
    result = await _persist_position_state_snapshots(runner, snapshot, snapshot_id=1)
    assert result == 0
