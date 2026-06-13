"""Unit tests for ``PostgresStore`` reader methods (VIB-3933).

These tests stub the asyncpg pool with a recording fake so we can pin
down the SQL shape (column list, WHERE clause, ORDER BY) and confirm the
row → framework-type conversion matches the SQLite contract that
:class:`DashboardService` and the ``quant_aggregations`` builders
(``compute_pnl_summary`` / ``compute_cost_stack`` / etc.) already
consume.

What is intentionally NOT covered here:
  - End-to-end Postgres behaviour. That belongs in
    ``tests/integration/gateway/test_dashboard_postgres_parity.py``
    (Phase 3 of the VIB-3933 plan), which spins up a real DB.
  - Type validation of asyncpg's parameter binding. asyncpg does that.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.state.state_manager import (
    _PG_FINITE_NUMERIC_PATTERN,
    PostgresStore,
    _pg_row_to_accounting_event_dict,
    _pg_row_to_ledger_entry,
    _pg_row_to_portfolio_metrics,
    _pg_row_to_portfolio_snapshot,
    _pg_row_to_position_event_dict,
)

_DEPLOYMENT_ID = "AccountingQuantLPStrategy:abc123"


# =============================================================================
# Fake asyncpg pool
# =============================================================================


class _FakeConn:
    """Records the SQL + args of each call; returns canned rows."""

    def __init__(
        self,
        fetch_rows: list | None = None,
        fetchrow_row: dict | None = None,
        execute_result: str = "INSERT 0 1",
    ) -> None:
        self.fetch_rows = fetch_rows or []
        self.fetchrow_row = fetchrow_row
        # asyncpg returns a command tag string from ``execute`` — e.g.
        # "INSERT 0 1", "UPDATE 1", "UPDATE 0". Tests pin specific values
        # when asserting on rowcount-derived return values.
        self.execute_result = execute_result
        self.calls: list[tuple[str, str, tuple]] = []

    async def fetch(self, sql: str, *args):
        self.calls.append(("fetch", sql, args))
        return self.fetch_rows

    async def fetchrow(self, sql: str, *args):
        self.calls.append(("fetchrow", sql, args))
        return self.fetchrow_row

    async def execute(self, sql: str, *args):
        self.calls.append(("execute", sql, args))
        return self.execute_result


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


def _make_store(conn: _FakeConn) -> PostgresStore:
    """Return a PostgresStore wired to a fake pool, skipping initialize()."""
    store = PostgresStore(database_url="postgresql://fake/fake")
    store._pool = _FakePool(conn)  # type: ignore[assignment]
    store._initialized = True
    return store


# =============================================================================
# Snapshot readers
# =============================================================================


def _snapshot_row(**overrides):
    base = {
        "deployment_id": _DEPLOYMENT_ID,
        "timestamp": datetime(2026, 5, 4, 12, 0, 0, tzinfo=UTC),
        "iteration_number": 5,
        "total_value_usd": "1234.50",
        "available_cash_usd": "100.00",
        "deployed_capital_usd": "1100.00",
        "wallet_total_value_usd": "1234.50",
        "value_confidence": "HIGH",
        "positions_text": "[]",
        "token_prices_text": "{}",
        "wallet_balances_text": "[]",
        "chain": "arbitrum",
    }
    base.update(overrides)
    return _DictRow(base)


class _DictRow(dict):
    """asyncpg.Record-like — both [key] and .get(key) work, plus .keys()."""

    def __getitem__(self, k):
        return super().__getitem__(k)

    def get(self, k, default=None):
        return super().get(k, default)


@pytest.mark.asyncio
async def test_get_latest_snapshot_keys_on_deployment_id_orders_desc():
    conn = _FakeConn(fetchrow_row=_snapshot_row())
    store = _make_store(conn)

    snap = await store.get_latest_snapshot(_DEPLOYMENT_ID)

    assert snap is not None
    assert snap.deployment_id == _DEPLOYMENT_ID
    assert snap.total_value_usd == Decimal("1234.50")
    assert snap.deployed_capital_usd == Decimal("1100.00")
    assert snap.chain == "arbitrum"

    # SQL shape — deployment_id filter + DESC + LIMIT 1 (VIB-4721/4722:
    # portfolio_snapshots has a single identity column, deployment_id).
    assert len(conn.calls) == 1
    kind, sql, args = conn.calls[0]
    assert kind == "fetchrow"
    assert "FROM portfolio_snapshots" in sql
    assert "WHERE deployment_id = $1" in sql
    assert "agent_id" not in sql
    assert "ORDER BY timestamp DESC" in sql
    assert "LIMIT 1" in sql
    assert args == (_DEPLOYMENT_ID,)


@pytest.mark.asyncio
async def test_get_latest_snapshot_returns_none_on_empty():
    conn = _FakeConn(fetchrow_row=None)
    store = _make_store(conn)

    assert await store.get_latest_snapshot(_DEPLOYMENT_ID) is None


@pytest.mark.asyncio
async def test_get_snapshots_since_passes_since_and_limit():
    conn = _FakeConn(fetch_rows=[_snapshot_row(), _snapshot_row(iteration_number=6)])
    store = _make_store(conn)
    since = datetime(2026, 5, 1, tzinfo=UTC)

    snaps = await store.get_snapshots_since(_DEPLOYMENT_ID, since, limit=42)

    assert len(snaps) == 2
    kind, sql, args = conn.calls[0]
    assert kind == "fetch"
    assert "WHERE deployment_id = $1 AND timestamp >= $2" in sql
    assert "ORDER BY timestamp ASC" in sql
    assert "LIMIT $3" in sql
    assert args == (_DEPLOYMENT_ID, since, 42)


@pytest.mark.asyncio
async def test_get_recent_snapshots_orders_desc_and_reverses_to_oldest_first():
    """VIB-5026: the latest-window read must SELECT newest-first then reverse.

    Pairing ``get_snapshots_since`` (ASC from ``since``) with
    ``compute_pnl_summary``'s ``snapshots[-1]`` returned the 168th-OLDEST row
    once a deployment had >168 snapshots, freezing the dashboard money tiles
    ~14h after launch. ``get_recent_snapshots`` instead bounds to the most
    recent ``limit`` rows and hands them back oldest-first so ``[-1]`` is the
    true latest.
    """
    # asyncpg yields ``ORDER BY timestamp DESC`` rows newest-first; the fake
    # echoes whatever we hand it, so pass newest-first to mirror the DB.
    newest = _snapshot_row(
        iteration_number=200,
        timestamp=datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC),
        total_value_usd="4.93",
    )
    older_in_window = _snapshot_row(
        iteration_number=33,
        timestamp=datetime(2026, 5, 4, 1, 0, 0, tzinfo=UTC),
        total_value_usd="2.43",
    )
    conn = _FakeConn(fetch_rows=[newest, older_in_window])  # DESC order from DB
    store = _make_store(conn)

    snaps = await store.get_recent_snapshots(_DEPLOYMENT_ID, limit=168)

    # Reversed to oldest-first → [-1] is the true latest.
    assert [s.iteration_number for s in snaps] == [33, 200]
    assert snaps[-1].total_value_usd == Decimal("4.93")

    kind, sql, args = conn.calls[0]
    assert kind == "fetch"
    assert "FROM portfolio_snapshots" in sql
    assert "WHERE deployment_id = $1" in sql
    assert "timestamp >=" not in sql  # NOT a since-anchored query
    assert "ORDER BY timestamp DESC" in sql
    assert "LIMIT $2" in sql
    assert args == (_DEPLOYMENT_ID, 168)


@pytest.mark.asyncio
async def test_get_recent_snapshots_empty_limit_short_circuits():
    conn = _FakeConn(fetch_rows=[_snapshot_row()])
    store = _make_store(conn)

    assert await store.get_recent_snapshots(_DEPLOYMENT_ID, limit=0) == []
    assert conn.calls == []  # never touches the pool


@pytest.mark.asyncio
async def test_get_snapshot_at_uses_at_or_before_filter():
    conn = _FakeConn(fetchrow_row=_snapshot_row())
    store = _make_store(conn)
    ts = datetime(2026, 5, 4, 0, 0, 0, tzinfo=UTC)

    snap = await store.get_snapshot_at(_DEPLOYMENT_ID, ts)

    assert snap is not None
    _, sql, args = conn.calls[0]
    assert "WHERE deployment_id = $1 AND timestamp <= $2" in sql
    assert "ORDER BY timestamp DESC" in sql
    assert "LIMIT 1" in sql
    assert args == (_DEPLOYMENT_ID, ts)


# =============================================================================
# Portfolio metrics
# =============================================================================


def _metrics_row(**overrides):
    base = {
        "deployment_id": _DEPLOYMENT_ID,
        "initial_value_usd": "10000.00",
        "initial_timestamp": datetime(2026, 5, 1, tzinfo=UTC),
        "deposits_usd": "500.00",
        "withdrawals_usd": "100.00",
        "gas_spent_usd": "12.34",
        "total_value_usd": "11000.00",
        "positions_text": "[]",
        "cycle_id": "cycle-42",
        "execution_mode": "live",
        "is_complete": True,
        "updated_at": datetime(2026, 5, 4, tzinfo=UTC),
    }
    base.update(overrides)
    return _DictRow(base)


@pytest.mark.asyncio
async def test_get_portfolio_metrics_keys_on_deployment_id():
    conn = _FakeConn(fetchrow_row=_metrics_row())
    store = _make_store(conn)

    metrics = await store.get_portfolio_metrics(_DEPLOYMENT_ID)

    assert metrics is not None
    assert metrics.deployment_id == _DEPLOYMENT_ID
    assert metrics.initial_value_usd == Decimal("10000.00")
    assert metrics.execution_mode == "live"
    assert metrics.is_complete is True

    _, sql, args = conn.calls[0]
    assert "FROM portfolio_metrics" in sql
    assert "WHERE deployment_id = $1" in sql
    assert "agent_id" not in sql
    assert args == (_DEPLOYMENT_ID,)


@pytest.mark.asyncio
async def test_get_portfolio_metrics_returns_none_on_empty():
    conn = _FakeConn(fetchrow_row=None)
    store = _make_store(conn)

    assert await store.get_portfolio_metrics(_DEPLOYMENT_ID) is None


# =============================================================================
# Ledger entries
# =============================================================================


def _ledger_row(**overrides):
    base = {
        "id": "tx-1",
        "cycle_id": "cycle-1",
        "deployment_id": _DEPLOYMENT_ID,
        "execution_mode": "live",
        "timestamp": datetime(2026, 5, 4, 9, 0, 0, tzinfo=UTC),
        "intent_type": "LP_OPEN",
        "token_in": "USDC",
        "amount_in": "1000",
        "token_out": "WETH",
        "amount_out": "0.3",
        "effective_price": "3333.0",
        "slippage_bps": 12.5,
        "gas_used": 250000,
        "gas_usd": "1.50",
        "tx_hash": "0xabc",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "success": True,
        "error": "",
        "extracted_data_text": '{"k":"v"}',
        "price_inputs_text": "{}",
        "pre_state_text": "{}",
        "post_state_text": "{}",
    }
    base.update(overrides)
    return _DictRow(base)


@pytest.mark.asyncio
async def test_get_ledger_entries_minimal_filters():
    conn = _FakeConn(fetch_rows=[_ledger_row()])
    store = _make_store(conn)

    entries = await store.get_ledger_entries(_DEPLOYMENT_ID, limit=50)

    assert len(entries) == 1
    e = entries[0]
    assert e.id == "tx-1"
    assert e.intent_type == "LP_OPEN"
    assert e.slippage_bps == 12.5
    assert e.success is True
    assert e.extracted_data_json == '{"k":"v"}'

    _, sql, args = conn.calls[0]
    assert "FROM transaction_ledger" in sql
    assert "WHERE deployment_id = $1" in sql
    assert "agent_id" not in sql
    assert "LIMIT $2" in sql  # limit is the only extra param
    assert "ORDER BY timestamp DESC" in sql
    assert args == (_DEPLOYMENT_ID, 50)


@pytest.mark.asyncio
async def test_get_ledger_entries_with_all_filters():
    conn = _FakeConn(fetch_rows=[])
    store = _make_store(conn)
    since = datetime(2026, 5, 1, tzinfo=UTC)
    before = datetime(2026, 5, 4, tzinfo=UTC)

    await store.get_ledger_entries(
        _DEPLOYMENT_ID,
        since=since,
        intent_type="LP_OPEN",
        limit=100,
        before=before,
    )

    _, sql, args = conn.calls[0]
    assert "deployment_id = $1" in sql
    assert "agent_id" not in sql
    assert "timestamp > $2" in sql
    assert "timestamp < $3" in sql
    assert "intent_type = $4" in sql
    assert "LIMIT $5" in sql
    assert args == (_DEPLOYMENT_ID, since, before, "LP_OPEN", 100)


# =============================================================================
# Accounting events
# =============================================================================


def _ae_row(**overrides):
    base = {
        "id": "ae-1",
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": datetime(2026, 5, 4, 9, 0, 0, tzinfo=UTC),
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "wallet_address": "0xwallet",
        "event_type": "LP_OPEN",
        "position_key": "uniswap_v3:arbitrum:0xpool:0xnft#1234",
        "ledger_entry_id": "tx-1",
        "tx_hash": "0xabc",
        "confidence": "HIGH",
        "payload_text": '{"cost_basis_usd":"1000"}',
        "schema_version": 2,
    }
    base.update(overrides)
    return _DictRow(base)


@pytest.mark.asyncio
async def test_get_accounting_events_keys_on_deployment_id():
    conn = _FakeConn(fetch_rows=[_ae_row()])
    store = _make_store(conn)

    rows = await store.get_accounting_events(_DEPLOYMENT_ID)

    assert len(rows) == 1
    r = rows[0]
    assert r["id"] == "ae-1"
    assert r["event_type"] == "LP_OPEN"
    assert r["payload_json"] == '{"cost_basis_usd":"1000"}'
    assert r["timestamp"].startswith("2026-05-04T")
    # SQLite parity: deployment_id, agent_id, deployment_id all present in dict
    assert r["deployment_id"] == _DEPLOYMENT_ID
    assert r["deployment_id"] == _DEPLOYMENT_ID

    _, sql, args = conn.calls[0]
    assert "FROM accounting_events" in sql
    assert "WHERE deployment_id = $1" in sql
    assert "ORDER BY timestamp ASC" in sql
    assert args == (_DEPLOYMENT_ID, 500)


@pytest.mark.asyncio
async def test_get_accounting_events_with_filters():
    conn = _FakeConn(fetch_rows=[])
    store = _make_store(conn)

    await store.get_accounting_events(
        _DEPLOYMENT_ID,
        event_type="LP_OPEN",
        position_key="some_pos",
        limit=10,
    )

    _, sql, args = conn.calls[0]
    assert "deployment_id = $1" in sql
    assert "event_type = $2" in sql
    assert "position_key = $3" in sql
    assert "LIMIT $4" in sql
    assert args == (_DEPLOYMENT_ID, "LP_OPEN", "some_pos", 10)


# =============================================================================
# Position events
# =============================================================================


def _pe_row(**overrides):
    base = {
        "id": "pe-1",
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "position_id": "1234",
        "position_type": "LP",
        "event_type": "OPEN",
        "timestamp": datetime(2026, 5, 4, 9, 0, 0, tzinfo=UTC),
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token0": "USDC",
        "token1": "WETH",
        "amount0": "1000",
        "amount1": "0.3",
        "value_usd": "2000",
        "tick_lower": -100,
        "tick_upper": 100,
        "liquidity": "12345",
        "in_range": True,
        "fees_token0": "0",
        "fees_token1": "0",
        "leverage": "",
        "entry_price": "",
        "mark_price": "",
        "unrealized_pnl": "",
        "is_long": None,
        "tx_hash": "0xabc",
        "gas_usd": "1.50",
        "ledger_entry_id": "tx-1",
        "protocol_fees_usd": "0.0125",
        "attribution_text": "{}",
        "attribution_version": 1,
    }
    base.update(overrides)
    return _DictRow(base)


@pytest.mark.asyncio
async def test_get_position_events_dict_keys_on_deployment_id():
    conn = _FakeConn(fetch_rows=[_pe_row()])
    store = _make_store(conn)

    rows = await store.get_position_events_dict(_DEPLOYMENT_ID)

    assert len(rows) == 1
    r = rows[0]
    assert r["id"] == "pe-1"
    assert r["position_type"] == "LP"
    assert r["event_type"] == "OPEN"
    assert r["in_range"] is True
    assert r["timestamp"].startswith("2026-05-04T")
    # VIB-3966: column now exists on metrics_db, real value passes through.
    assert r["protocol_fees_usd"] == "0.0125"

    _, sql, args = conn.calls[0]
    assert "FROM position_events" in sql
    assert "WHERE deployment_id = $1" in sql
    assert "ORDER BY timestamp ASC" in sql
    # VIB-3966: protocol_fees_usd must be in the SELECT list now.
    assert "protocol_fees_usd" in sql
    assert args == (_DEPLOYMENT_ID,)


@pytest.mark.asyncio
async def test_get_position_events_dict_with_all_filters():
    conn = _FakeConn(fetch_rows=[])
    store = _make_store(conn)

    await store.get_position_events_dict(
        _DEPLOYMENT_ID,
        position_id="42",
        position_type="LP",
        event_type="CLOSE",
    )

    _, sql, args = conn.calls[0]
    assert "deployment_id = $1" in sql
    assert "position_id = $2" in sql
    assert "position_type = $3" in sql
    assert "event_type = $4" in sql
    assert args == (_DEPLOYMENT_ID, "42", "LP", "CLOSE")


# =============================================================================
# Position events — write/read parity (VIB-4315)
# =============================================================================


def _make_position_event(**overrides):
    """Build a minimal :class:`PositionEvent` for the writer tests.

    Defaults mirror ``_pe_row`` so the assertions can target the same shape
    that ``get_position_history`` would return after the round-trip.
    """
    from almanak.framework.observability.position_events import PositionEvent

    base = {
        "id": "pe-1",
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "position_id": "1234",
        "position_type": "LP",
        "event_type": "OPEN",
        "timestamp": datetime(2026, 5, 4, 9, 0, 0, tzinfo=UTC),
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token0": "USDC",
        "token1": "WETH",
        "amount0": "1000",
        "amount1": "0.3",
        "value_usd": "2000",
        "tick_lower": -100,
        "tick_upper": 100,
        "liquidity": "12345",
        "in_range": True,
        "fees_token0": "0",
        "fees_token1": "0",
        "leverage": "",
        "entry_price": "",
        "mark_price": "",
        "unrealized_pnl": "",
        "is_long": None,
        "tx_hash": "0xabc",
        "gas_usd": "1.50",
        "ledger_entry_id": "tx-1",
        "protocol_fees_usd": "0.0125",
        "attribution_json": "{}",
        "attribution_version": 1,
    }
    base.update(overrides)
    return PositionEvent(**base)


@pytest.mark.asyncio
async def test_save_position_event_writes_all_columns_from_deployment_id(monkeypatch):
    """SQL shape pin + one-identity binding (blueprint 29 §4).

    VIB-4721/4722: ``position_events`` has a single identity column,
    ``deployment_id`` (the legacy ``agent_id`` column was DROPPED).
    ``save_position_event`` stamps ``event.deployment_id`` directly with no
    gateway-side translation.
    """
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "hosted-agent-xyz")
    conn = _FakeConn()
    store = _make_store(conn)

    ok = await store.save_position_event(_make_position_event())

    assert ok is True
    assert len(conn.calls) == 1
    kind, sql, args = conn.calls[0]
    assert kind == "execute"
    assert "INSERT INTO position_events" in sql
    # Schema columns the SDK must persist; if a new column is added this
    # assertion fails loudly — same anti-drift contract as the reader test.
    for col in (
        "id",
        "deployment_id",
        "cycle_id",
        "execution_mode",
        "position_id",
        "position_type",
        "event_type",
        "timestamp",
        "protocol_fees_usd",
        "attribution_json",
        "attribution_version",
    ):
        assert col in sql, f"column {col!r} missing from save_position_event INSERT"
    # The legacy agent_id column is gone.
    assert "agent_id" not in sql
    # First-write-wins idempotency (matches SQLite INSERT OR IGNORE).
    assert "ON CONFLICT (id) DO NOTHING" in sql
    # JSONB column must be cast on the wire.
    assert "$31::jsonb" in sql

    # Blueprint 29 §4: one identity column — deployment_id gets
    # event.deployment_id, no hosted-env translation.
    assert args[0] == "pe-1"  # id
    assert args[1] == _DEPLOYMENT_ID  # canonical deployment_id column


@pytest.mark.asyncio
async def test_save_position_event_uses_deployment_id_in_local_mode(monkeypatch):
    """Local mode → the deployment_id column gets event.deployment_id."""
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)
    conn = _FakeConn()
    store = _make_store(conn)

    await store.save_position_event(_make_position_event())

    _, _, args = conn.calls[0]
    assert args[1] == _DEPLOYMENT_ID  # deployment_id column


@pytest.mark.asyncio
async def test_save_position_event_preserves_protocol_fees_empty_vs_zero(monkeypatch):
    """AGENTS.md "Empty ≠ Zero" — Decimal("0") must not collapse to "" on the wire."""
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "a")

    # Measured zero stays "0".
    conn = _FakeConn()
    store = _make_store(conn)
    await store.save_position_event(_make_position_event(protocol_fees_usd="0"))
    assert conn.calls[0][2][29] == "0"

    # Parser-did-not-emit stays "".
    conn = _FakeConn()
    store = _make_store(conn)
    await store.save_position_event(_make_position_event(protocol_fees_usd=""))
    assert conn.calls[0][2][29] == ""


@pytest.mark.asyncio
async def test_save_position_event_preserves_tri_state_optionals(monkeypatch):
    """tick_lower / tick_upper / in_range / is_long None must bind as None (NULL)."""
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "a")
    conn = _FakeConn()
    store = _make_store(conn)

    await store.save_position_event(_make_position_event(tick_lower=None, tick_upper=None, in_range=None, is_long=None))

    _, _, args = conn.calls[0]
    # Positional order matches the INSERT VALUES list (VIB-4721/4722: no
    # agent_id column, so indices shift down by one from the legacy shape):
    # ..., $16 tick_lower, $17 tick_upper, $18 liquidity, $19 in_range, ...
    # ..., $26 unrealized_pnl, $27 is_long, ...
    assert args[15] is None  # tick_lower
    assert args[16] is None  # tick_upper
    assert args[18] is None  # in_range
    assert args[25] is None  # is_long


@pytest.mark.asyncio
async def test_save_position_event_binds_datetime_not_string(monkeypatch):
    """asyncpg TIMESTAMPTZ codec rejects raw strings (VIB-4313 redux).

    PositionEvent.timestamp is already a tz-aware datetime in the dataclass;
    this pins it through the binding so a future refactor that hands a
    string to ``conn.execute`` would fail the test instead of crashing in
    hosted prod.
    """
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "a")
    conn = _FakeConn()
    store = _make_store(conn)

    ts = datetime(2026, 5, 4, 9, 0, 0, tzinfo=UTC)
    await store.save_position_event(_make_position_event(timestamp=ts))

    _, _, args = conn.calls[0]
    # $8 timestamp position (VIB-4721/4722: no agent_id column).
    assert args[7] == ts
    assert isinstance(args[7], datetime)
    assert args[7].tzinfo is not None


@pytest.mark.asyncio
async def test_get_position_history_keys_on_deployment_and_orders_asc():
    """SQL shape + asc ordering — pnl_attributor needs OPEN before CLOSE."""
    conn = _FakeConn(fetch_rows=[_pe_row(), _pe_row(id="pe-2", event_type="CLOSE")])
    store = _make_store(conn)

    rows = await store.get_position_history(_DEPLOYMENT_ID, "1234")

    assert len(rows) == 2
    assert rows[0]["event_type"] == "OPEN"
    assert rows[1]["event_type"] == "CLOSE"

    _, sql, args = conn.calls[0]
    assert "FROM position_events" in sql
    assert "WHERE deployment_id = $1 AND position_id = $2" in sql
    assert "ORDER BY timestamp ASC" in sql
    assert "protocol_fees_usd" in sql
    # JSONB needs to be selected as text for the dict converter.
    assert "attribution_json::text AS attribution_text" in sql
    assert args == (_DEPLOYMENT_ID, "1234")


@pytest.mark.asyncio
async def test_get_position_history_returns_empty_when_no_rows():
    conn = _FakeConn(fetch_rows=[])
    store = _make_store(conn)

    rows = await store.get_position_history(_DEPLOYMENT_ID, "missing")

    assert rows == []


@pytest.mark.asyncio
async def test_update_position_attribution_returns_true_on_match():
    """Match → True; runner stamps attribution_json on disk."""
    # fetchrow returns a single-column row (RETURNING id) when matched.
    conn = _FakeConn(fetchrow_row=_DictRow({"id": "pe-1"}))
    store = _make_store(conn)

    ok = await store.update_position_attribution(
        event_id="pe-1",
        attribution_json='{"realized_pnl_usd":"-1.23"}',
        attribution_version=2,
    )

    assert ok is True
    _, sql, args = conn.calls[0]
    assert "UPDATE position_events" in sql
    assert "SET attribution_json = $1::jsonb, attribution_version = $2" in sql
    assert "WHERE id = $3" in sql
    assert "RETURNING id" in sql
    assert args == ('{"realized_pnl_usd":"-1.23"}', 2, "pe-1")


@pytest.mark.asyncio
async def test_update_position_attribution_returns_false_on_missing_row():
    """No row matched → False; pnl_attributor logs warning, runner continues."""
    conn = _FakeConn(fetchrow_row=None)
    store = _make_store(conn)

    ok = await store.update_position_attribution(
        event_id="missing-id",
        attribution_json="{}",
        attribution_version=0,
    )

    assert ok is False


@pytest.mark.asyncio
async def test_update_position_attribution_scopes_by_deployment_id_when_provided():
    """Non-empty deployment_id → extra ``AND deployment_id = $4`` guard (multi-tenant defense-in-depth)."""
    conn = _FakeConn(fetchrow_row=_DictRow({"id": "pe-1"}))
    store = _make_store(conn)

    ok = await store.update_position_attribution(
        event_id="pe-1",
        attribution_json="{}",
        attribution_version=1,
        deployment_id=_DEPLOYMENT_ID,
    )

    assert ok is True
    _, sql, args = conn.calls[0]
    assert "WHERE id = $3 AND deployment_id = $4" in sql
    assert args == ("{}", 1, "pe-1", _DEPLOYMENT_ID)


@pytest.mark.asyncio
async def test_update_position_attribution_unscoped_when_deployment_id_empty():
    """Empty deployment_id (default) → single-clause WHERE id = $3 (parity with SQLite + GSM legacy callers)."""
    conn = _FakeConn(fetchrow_row=_DictRow({"id": "pe-1"}))
    store = _make_store(conn)

    ok = await store.update_position_attribution(
        event_id="pe-1",
        attribution_json="{}",
        attribution_version=1,
        # deployment_id omitted → defaults to ""
    )

    assert ok is True
    _, sql, args = conn.calls[0]
    assert "WHERE id = $3" in sql
    assert "deployment_id" not in sql.split("RETURNING")[0]
    assert args == ("{}", 1, "pe-1")


@pytest.mark.asyncio
async def test_update_position_attribution_scoped_returns_false_on_deployment_mismatch():
    """Wrong deployment_id → no row matched → False (defense-in-depth)."""
    conn = _FakeConn(fetchrow_row=None)  # PG returns no row when scope filter excludes it
    store = _make_store(conn)

    ok = await store.update_position_attribution(
        event_id="pe-1",
        attribution_json="{}",
        attribution_version=1,
        deployment_id="wrong-deployment",
    )

    assert ok is False
    _, sql, args = conn.calls[0]
    assert "AND deployment_id = $4" in sql
    assert args[3] == "wrong-deployment"


# =============================================================================
# Row-conversion parity (no DB needed)
# =============================================================================


def test_pg_row_to_portfolio_snapshot_handles_envelope_payload():
    payload = (
        '{"schema_version":1,"positions":[{"position_type":"LP","protocol":"u3",'
        '"chain":"arbitrum","value_usd":"1","label":"x"}],'
        '"metadata":{"source":"test"},"reconciliation":{"gap_usd":"0"}}'
    )
    row = _DictRow(
        {
            "deployment_id": _DEPLOYMENT_ID,
            "timestamp": datetime(2026, 5, 4, tzinfo=UTC),
            "iteration_number": 1,
            "total_value_usd": "1",
            "available_cash_usd": "0",
            "deployed_capital_usd": "1",
            "wallet_total_value_usd": "1",
            "value_confidence": "HIGH",
            "positions_text": payload,
            "token_prices_text": "{}",
            "wallet_balances_text": "[]",
            "chain": "arbitrum",
        }
    )
    snap = _pg_row_to_portfolio_snapshot(row)
    assert snap.deployment_id == _DEPLOYMENT_ID
    assert len(snap.positions) == 1
    # Envelope metadata round-trips into snapshot_metadata
    assert snap.snapshot_metadata.get("source") == "test"


def test_pg_row_to_portfolio_snapshot_hydrates_populated_wallet_columns():
    # VIB-5007 — the four wallet-side columns the PG WRITE path now binds must
    # round-trip on the READ path into the snapshot's typed fields. This is the
    # consumer half of the end-to-end chain: ``snapshot.wallet_balances`` is the
    # PRIMARY source for the dashboard "Current Position" panel
    # (``build_position_proto`` → ``token_balances``), and it is the per-snapshot
    # audit record of token composition. Both must be recoverable from the
    # dedicated columns alone — not only from the positions_json envelope.
    wallet_balances = [
        {
            "symbol": "WBTC",
            "balance": "0.00009864",
            "value_usd": "6.24",
            "address": "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
            "price_usd": "63251.0",
        },
        {
            "symbol": "USDC",
            "balance": "1.978059",
            "value_usd": "1.978059",
            "address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            "price_usd": "1.0",
        },
    ]
    token_prices = {
        "arbitrum:0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": "63251.0",
        "arbitrum:0xaf88d065e77c8cc2239327c5edb3a432268e5831": "1.0",
    }
    row = _snapshot_row(
        wallet_balances_text=json.dumps(wallet_balances),
        token_prices_text=json.dumps(token_prices),
        deployed_capital_usd="0",
        wallet_total_value_usd="8.218059",
    )

    snap = _pg_row_to_portfolio_snapshot(row)

    # Per-token composition recovered (not merely "non-empty") — this is what
    # makes snapshot↔trade-tape reconciliation a DB read instead of on-chain
    # forensics.
    assert [b.symbol for b in snap.wallet_balances] == ["WBTC", "USDC"]
    wbtc, usdc = snap.wallet_balances
    assert wbtc.balance == Decimal("0.00009864")
    assert wbtc.value_usd == Decimal("6.24")
    assert wbtc.price_usd == Decimal("63251.0")
    assert wbtc.address == "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f"
    assert usdc.balance == Decimal("1.978059")
    # Scalar + price-map columns hydrate too.
    assert snap.wallet_total_value_usd == Decimal("8.218059")
    assert snap.deployed_capital_usd == Decimal("0")
    assert snap.token_prices == token_prices


def test_pg_row_to_portfolio_metrics_defaults_missing_columns():
    row = _DictRow(
        {
            "initial_value_usd": "100",
            "initial_timestamp": datetime(2026, 5, 1, tzinfo=UTC),
            "deposits_usd": None,  # NULL on legacy row → defaults to "0"
            "withdrawals_usd": "0",
            "gas_spent_usd": "0",
            "total_value_usd": None,
            "positions_text": None,
            "cycle_id": None,
            "deployment_id": "",
            "execution_mode": "",
            "is_complete": None,
            "updated_at": datetime(2026, 5, 4, tzinfo=UTC),
        }
    )
    metrics = _pg_row_to_portfolio_metrics(row)
    assert metrics.deposits_usd == Decimal("0")
    assert metrics.total_value_usd == Decimal("0")
    assert metrics.positions_json == "[]"
    assert metrics.is_complete is True


def test_pg_row_to_ledger_entry_preserves_jsonb_text_passthrough():
    row = _DictRow(
        {
            "id": "tx-1",
            "cycle_id": "c1",
            "deployment_id": _DEPLOYMENT_ID,
            "execution_mode": "live",
            "timestamp": datetime(2026, 5, 4, tzinfo=UTC),
            "intent_type": "SUPPLY",
            "token_in": "USDC",
            "amount_in": "100",
            "token_out": "aUSDC",
            "amount_out": "100",
            "effective_price": "1",
            "slippage_bps": None,  # optional field
            "gas_used": 100000,
            "gas_usd": "0.50",
            "tx_hash": "0xa",
            "chain": "arbitrum",
            "protocol": "aave_v3",
            "success": True,
            "error": "",
            "extracted_data_text": '{"protocol_fees":{"total_usd":"0"}}',
            "price_inputs_text": '{"USDC":"1.0"}',
            "pre_state_text": "{}",
            "post_state_text": "{}",
        }
    )
    entry = _pg_row_to_ledger_entry(row)
    assert entry.id == "tx-1"
    assert entry.slippage_bps is None  # tri-state preserved
    assert entry.extracted_data_json.startswith("{")
    assert entry.price_inputs_json == '{"USDC":"1.0"}'


def test_pg_row_to_accounting_event_dict_matches_sqlite_keys():
    row = _ae_row()
    d = _pg_row_to_accounting_event_dict(row)
    # The SQLite version returns a dict with exactly these keys; consumer
    # parity is the contract.
    expected_keys = {
        "id",
        "deployment_id",
        "cycle_id",
        "execution_mode",
        "timestamp",
        "chain",
        "protocol",
        "wallet_address",
        "event_type",
        "position_key",
        "ledger_entry_id",
        "tx_hash",
        "confidence",
        "payload_json",
        "schema_version",
    }
    assert expected_keys.issubset(d.keys())
    assert d["payload_json"] == '{"cost_basis_usd":"1000"}'
    assert d["schema_version"] == 2


def test_pg_row_to_position_event_dict_preserves_tri_state_in_range():
    row = _pe_row(in_range=None)
    d = _pg_row_to_position_event_dict(row)
    # in_range tri-state matters for the dashboard's primary-risk gauge
    # (VIB-3893): None must not collapse to False.
    assert d["in_range"] is None


def test_pg_row_to_position_event_dict_passes_protocol_fees_through():
    """``protocol_fees_usd`` reads the real Postgres column post VIB-3966.

    The metrics-database migration (PR #27) added the column with
    ``TEXT NOT NULL DEFAULT ''``. The SDK converter now reads the value
    off the row instead of emitting the sentinel.
    """
    # Real value present on the row — round-trips.
    assert _pg_row_to_position_event_dict(_pe_row())["protocol_fees_usd"] == "0.0125"

    # Empty string from the DEFAULT survives as "" (parser-did-not-emit
    # semantic per AGENTS.md "Empty ≠ zero", distinct from "0").
    assert _pg_row_to_position_event_dict(_pe_row(protocol_fees_usd=""))["protocol_fees_usd"] == ""

    # Defensive: the row.get(...) or "" fallback collapses NULL legacy rows
    # (shouldn't happen with NOT NULL DEFAULT, but cheap to defend) to "".
    assert _pg_row_to_position_event_dict(_pe_row(protocol_fees_usd=None))["protocol_fees_usd"] == ""

    # Measured zero passes through without being collapsed to "" — important
    # because "0" and "" mean different things in the accounting contract.
    assert _pg_row_to_position_event_dict(_pe_row(protocol_fees_usd="0"))["protocol_fees_usd"] == "0"


def test_position_event_dict_keyset_parity_pg_vs_sqlite():
    """SQLite and Postgres position_event dicts must expose identical keys.

    Regression for review finding #3: a future schema-drift migration that
    adds a column to one backend but not the other (or to neither) must
    not silently produce different dict shapes downstream — that was
    exactly how protocol_fees_usd slipped between the SDK and metrics_db.

    Compares the field set produced by each converter against a fresh
    fixture row with all advertised columns populated.
    """
    pg_dict = _pg_row_to_position_event_dict(_pe_row())

    # Build a SQLite-row-shape dict to feed the SQLite converter. SQLite's
    # ``get_position_events_sync`` returns ``dict(sqlite3.Row)`` directly
    # — we mimic that here so the comparison is over public output keys.
    sqlite_row_dict = {
        "id": "pe-1",
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "position_id": "1234",
        "position_type": "LP",
        "event_type": "OPEN",
        "timestamp": "2026-05-04T09:00:00+00:00",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token0": "USDC",
        "token1": "WETH",
        "amount0": "1000",
        "amount1": "0.3",
        "value_usd": "2000",
        "tick_lower": -100,
        "tick_upper": 100,
        "liquidity": "12345",
        "in_range": True,
        "fees_token0": "0",
        "fees_token1": "0",
        "leverage": "",
        "entry_price": "",
        "mark_price": "",
        "unrealized_pnl": "",
        "is_long": None,
        "tx_hash": "0xabc",
        "gas_usd": "1.50",
        "ledger_entry_id": "tx-1",
        "protocol_fees_usd": "",
        "attribution_json": "{}",
        "attribution_version": 1,
    }
    # Every key SQLite returns must be present in the PG-converter output.
    # (PG output may carry extras like ``agent_id`` — those are additive,
    # not regressive — but it must not LOSE any SQLite key.)
    missing_on_pg = set(sqlite_row_dict.keys()) - set(pg_dict.keys())
    assert not missing_on_pg, (
        f"PG position_event dict is missing keys present on SQLite: {missing_on_pg}. "
        "If you added a column to one backend, add it (or its sentinel) to the other."
    )


# =============================================================================
# Dashboard dispatch — VIB-3933 review finding #2
# =============================================================================


@pytest.mark.asyncio
async def test_dashboard_dispatch_prefers_sqlite_sync_for_accounting_events():
    """SQLite-side dashboard reads must use the sync ASC/no-LIMIT contract.

    Regression for VIB-3933 review finding #2: an earlier dispatch
    preferred ``get_accounting_events`` (async DESC LIMIT 500) over the
    sync ASC unlimited path, which silently changed local-mode dashboard
    semantics. SQLite should hit the sync path.
    """
    from almanak.framework.state.state_manager import StateManager

    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._unimplemented_logged = set()

    sync_calls: list = []
    async_calls: list = []

    class _SQLiteLike:
        def get_accounting_events_sync(self, deployment_id, position_key=None):
            sync_calls.append((deployment_id, position_key))
            return [{"id": "from-sync"}]

        async def get_accounting_events(self, deployment_id, event_type=None, position_key=None, limit=500):
            async_calls.append((deployment_id, limit))
            return [{"id": "from-async"}]

    sm._warm = _SQLiteLike()  # type: ignore[assignment]

    rows = await sm.get_accounting_events_for_dashboard("dep-1")

    assert rows == [{"id": "from-sync"}]
    assert sync_calls == [("dep-1", None)]
    assert async_calls == []  # async path must NOT be reached on SQLite


@pytest.mark.asyncio
async def test_dashboard_dispatch_uses_async_when_only_async_available():
    """PostgresStore exposes only the async method; dispatch must use it."""
    from almanak.framework.state.state_manager import StateManager

    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._unimplemented_logged = set()

    async_calls: list = []

    class _PostgresLike:
        async def get_accounting_events(self, deployment_id, event_type=None, position_key=None, limit=500):
            async_calls.append((deployment_id, limit))
            return [{"id": "from-pg"}]

    sm._warm = _PostgresLike()  # type: ignore[assignment]

    rows = await sm.get_accounting_events_for_dashboard("dep-1")

    assert rows == [{"id": "from-pg"}]
    # Effectively-unbounded limit mirrors SQLite sync's no-LIMIT contract.
    assert async_calls == [("dep-1", 10**9)]


@pytest.mark.asyncio
async def test_dashboard_dispatch_prefers_sqlite_sync_for_position_events():
    """Same dispatch order for position events (VIB-3933 finding #2)."""
    from almanak.framework.state.state_manager import StateManager

    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._unimplemented_logged = set()

    sync_calls: list = []
    async_calls: list = []

    class _SQLiteLike:
        def get_position_events_sync(self, deployment_id, position_id=None, position_type=None, event_type=None):
            sync_calls.append(deployment_id)
            return [{"id": "from-sync"}]

        async def get_position_events_dict(self, deployment_id, **kwargs):
            async_calls.append(deployment_id)
            return [{"id": "from-async"}]

    sm._warm = _SQLiteLike()  # type: ignore[assignment]

    rows = await sm.get_position_events_for_dashboard("dep-1")

    assert rows == [{"id": "from-sync"}]
    assert async_calls == []


# =============================================================================
# PG metrics writer parity — VIB-3933 review finding #1
# =============================================================================


def test_pg_upsert_query_includes_total_value_and_positions_columns():
    """The PG UPSERT must persist total_value_usd and positions_json (VIB-3933 finding #1).

    Without these columns being written, the schema default '0' leaks
    through to GetPortfolioMetrics and dashboards render $0 NAV despite
    snapshots existing. Mirror of SQLite's writer at sqlite.py:2253.
    """
    from almanak.gateway.services._save_metrics_helpers import PG_UPSERT_QUERY

    # INSERT column list must include both fields.
    assert "total_value_usd" in PG_UPSERT_QUERY
    assert "positions_json" in PG_UPSERT_QUERY
    # On UPDATE conflict path, both must also be refreshed (otherwise the
    # second row a strategy ever writes still loses the value).
    assert "total_value_usd = EXCLUDED.total_value_usd" in PG_UPSERT_QUERY
    assert "positions_json = EXCLUDED.positions_json" in PG_UPSERT_QUERY


def test_build_pg_upsert_args_appends_total_value_and_positions():
    """Positional args must end with total_value_usd then positions_json."""
    from almanak.gateway.proto import gateway_pb2
    from almanak.gateway.services._save_metrics_helpers import (
        ParsedMetricsInputs,
        build_pg_upsert_args,
    )

    inputs = ParsedMetricsInputs(
        deployment_id=_DEPLOYMENT_ID,
        initial_value_usd=Decimal("100"),
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
        gas_spent_usd=Decimal("0"),
        timestamp=datetime(2026, 5, 1, tzinfo=UTC),
    )
    request = gateway_pb2.SaveMetricsRequest()
    now = datetime(2026, 5, 4, tzinfo=UTC)

    args = build_pg_upsert_args(inputs, request, now, Decimal("12345.67"))

    # Length matches $1..$12 placeholders in PG_UPSERT_QUERY (VIB-4721/4722:
    # portfolio_metrics has a single identity column, deployment_id — the
    # separate request.deployment_id arg was dropped).
    assert len(args) == 12
    assert args[0] == _DEPLOYMENT_ID  # deployment_id column (canonical wire id)
    assert args[10] == "12345.67"  # total_value_usd
    assert args[11] == "[]"  # positions_json default

    # Override positions_json explicitly.
    args2 = build_pg_upsert_args(inputs, request, now, Decimal("0"), positions_json='[{"x":1}]')
    assert args2[11] == '[{"x":1}]'


# =============================================================================
# position_registry readers + backfill writer (VIB-4794)
# =============================================================================


def _registry_row(**overrides):
    """Build an asyncpg-shaped row dict matching the SELECT column list in
    :meth:`PostgresStore.get_position_registry_open_rows`."""
    base = {
        "deployment_id": _DEPLOYMENT_ID,
        "chain": "arbitrum",
        "primitive": "lp",
        "accounting_category": "lp",
        "physical_identity_hash": "0xdeadbeef",
        "semantic_grouping_key": "arbitrum:0xpool",
        "grouping_policy_version": "univ3_lp@v1",
        "handle": None,
        "status": "open",
        "payload_text": '{"token_id": "5503050", "liquidity": "12345"}',
        "opened_at_block": 100,
        "opened_tx": "0xopen",
        "closed_at_block": None,
        "closed_tx": None,
        "last_reconciled_at_block": None,
        "matching_policy_version": 3,
    }
    base.update(overrides)
    return _DictRow(base)


@pytest.mark.asyncio
async def test_get_position_registry_open_rows_basic_select_and_order():
    """SQL keys on deployment_id + status=open with NULLS-FIRST ordering."""
    conn = _FakeConn(fetch_rows=[_registry_row()])
    store = _make_store(conn)

    rows = await store.get_position_registry_open_rows(_DEPLOYMENT_ID)

    assert len(rows) == 1
    row = rows[0]
    # payload_text alias is consumed by the parser and replaced with parsed
    # payload dict — caller never sees the raw text alias.
    assert "payload_text" not in row
    assert row["payload"] == {"token_id": "5503050", "liquidity": "12345"}
    assert row["deployment_id"] == _DEPLOYMENT_ID

    _, sql, args = conn.calls[0]
    assert "FROM position_registry" in sql
    assert "WHERE deployment_id = $1 AND status = 'open'" in sql
    # Cross-backend determinism contract: NULLS FIRST pinned explicitly
    # because Postgres ASC defaults to NULLS LAST while SQLite ASC
    # defaults to NULLS FIRST.
    assert "ORDER BY opened_at_block ASC NULLS FIRST, opened_tx ASC NULLS FIRST" in sql
    # JSONB column SELECT'd as text so the parser can mirror SQLite shape.
    assert "payload::text AS payload_text" in sql
    assert args == (_DEPLOYMENT_ID,)


@pytest.mark.asyncio
async def test_get_position_registry_open_rows_returns_empty_when_no_rows():
    conn = _FakeConn(fetch_rows=[])
    store = _make_store(conn)

    rows = await store.get_position_registry_open_rows(_DEPLOYMENT_ID)
    assert rows == []


@pytest.mark.asyncio
async def test_get_position_registry_open_rows_applies_optional_filters():
    """Each optional filter appends a placeholder and a parameter."""
    conn = _FakeConn(fetch_rows=[])
    store = _make_store(conn)

    await store.get_position_registry_open_rows(
        _DEPLOYMENT_ID,
        chain="arbitrum",
        primitive="lp",
        accounting_category="lp",
    )

    _, sql, args = conn.calls[0]
    assert "AND chain = $2" in sql
    assert "AND primitive = $3" in sql
    assert "AND accounting_category = $4" in sql
    assert args == (_DEPLOYMENT_ID, "arbitrum", "lp", "lp")


@pytest.mark.asyncio
async def test_get_position_registry_open_rows_handles_payload_decode_error():
    """Corrupt JSON payload is coerced to {} with diagnostic fields."""
    conn = _FakeConn(fetch_rows=[_registry_row(payload_text="{not json")])
    store = _make_store(conn)

    rows = await store.get_position_registry_open_rows(_DEPLOYMENT_ID)

    assert len(rows) == 1
    assert rows[0]["payload"] == {}
    assert rows[0]["payload_raw"] == "{not json"
    assert "payload_decode_error" in rows[0]


@pytest.mark.asyncio
async def test_get_position_registry_open_rows_handles_non_dict_payload():
    """Non-dict JSON (e.g. array) is coerced to {} with a shape-error field."""
    conn = _FakeConn(fetch_rows=[_registry_row(payload_text="[1, 2, 3]")])
    store = _make_store(conn)

    rows = await store.get_position_registry_open_rows(_DEPLOYMENT_ID)

    assert len(rows) == 1
    assert rows[0]["payload"] == {}
    assert rows[0]["payload_raw"] == "[1, 2, 3]"
    assert "payload_shape_error" in rows[0]


@pytest.mark.asyncio
async def test_get_position_registry_open_rows_preserves_multiple_rows_in_order():
    """fetch order is preserved (asyncpg respects ORDER BY at the SQL layer)."""
    conn = _FakeConn(
        fetch_rows=[
            _registry_row(physical_identity_hash="0xa", opened_at_block=10),
            _registry_row(physical_identity_hash="0xb", opened_at_block=20),
        ]
    )
    store = _make_store(conn)

    rows = await store.get_position_registry_open_rows(_DEPLOYMENT_ID)

    assert [r["physical_identity_hash"] for r in rows] == ["0xa", "0xb"]


class _FakeRegistryRow:
    """Stand-in for ``almanak.framework.accounting.commit.RegistryRow``.

    The real class is a frozen dataclass that requires enum-validated
    primitive / accounting_category values. The PostgresStore method only
    calls the three ``*_value()`` / ``payload_json()`` accessors and reads
    attributes — duck-typing here keeps the test free of import-side enum
    validation and matches the calling contract documented on the method.
    """

    def __init__(self, **kwargs):
        self.deployment_id = kwargs.get("deployment_id", _DEPLOYMENT_ID)
        self.chain = kwargs.get("chain", "arbitrum")
        self._primitive = kwargs.get("primitive", "lp")
        self._accounting_category = kwargs.get("accounting_category", "lp")
        self.physical_identity_hash = kwargs.get("physical_identity_hash", "0xhash")
        self.semantic_grouping_key = kwargs.get("semantic_grouping_key", "arbitrum:0xpool")
        self.grouping_policy_version = kwargs.get("grouping_policy_version", "univ3_lp@v1")
        self.handle = kwargs.get("handle")
        self.status = kwargs.get("status", "open")
        self._payload_json = kwargs.get("payload_json", '{"token_id": "5503050"}')
        self.opened_at_block = kwargs.get("opened_at_block", 100)
        self.opened_tx = kwargs.get("opened_tx", "0xopen")
        self.closed_at_block = kwargs.get("closed_at_block")
        self.closed_tx = kwargs.get("closed_tx")
        self.last_reconciled_at_block = kwargs.get("last_reconciled_at_block")
        self.matching_policy_version = kwargs.get("matching_policy_version", 3)

    def primitive_value(self) -> str:
        return self._primitive

    def accounting_category_value(self) -> str:
        return self._accounting_category

    def payload_json(self) -> str:
        return self._payload_json


@pytest.mark.asyncio
async def test_insert_position_registry_row_if_absent_inserts_new():
    """``INSERT 0 1`` command tag → True (new row inserted)."""
    conn = _FakeConn(execute_result="INSERT 0 1")
    store = _make_store(conn)

    inserted = await store.insert_position_registry_row_if_absent(row=_FakeRegistryRow())

    assert inserted is True
    _, sql, args = conn.calls[0]
    assert "INSERT INTO position_registry" in sql
    assert "ON CONFLICT (deployment_id, chain, primitive, physical_identity_hash)" in sql
    assert "DO NOTHING" in sql
    # 16 positional placeholders matching the SQLite column order.
    assert len(args) == 16
    assert args[0] == _DEPLOYMENT_ID  # deployment_id
    assert args[2] == "lp"  # primitive
    assert args[3] == "lp"  # accounting_category


@pytest.mark.asyncio
async def test_insert_position_registry_row_if_absent_returns_false_on_conflict():
    """``INSERT 0 0`` command tag → False (conflict skipped, no new row)."""
    conn = _FakeConn(execute_result="INSERT 0 0")
    store = _make_store(conn)

    inserted = await store.insert_position_registry_row_if_absent(row=_FakeRegistryRow())

    assert inserted is False


@pytest.mark.asyncio
async def test_insert_position_registry_row_if_absent_returns_false_on_unexpected_tag():
    """Unknown / malformed command tag → False (fail-closed)."""
    conn = _FakeConn(execute_result="")
    store = _make_store(conn)

    inserted = await store.insert_position_registry_row_if_absent(row=_FakeRegistryRow())

    assert inserted is False


# =============================================================================
# Ledger quant stats + anchor candidates (VIB-5059 Phase 1 — SQL half)
# =============================================================================
#
# UAT card D2.M2 (docs/internal/uat-cards/VIB-5059-p1sql.md): the aggregate
# SQL computes COUNT/SUM server-side, selects NO JSON-blob column, casts the
# text-numeric gas_usd to exact numeric ONLY behind a finite-numeric-literal
# guard (NULL / '' / garbage / NaN / Infinity contribute zero, never raise),
# and coalesces the zero-row sum to 0; the anchor SQL is ascending,
# LIMIT-bounded, and projects only the three columns the anchor walk reads.


def _quant_stats_row(**overrides):
    base = {
        "total": 13,
        "with_tx_hash": 12,
        "with_cycle_id": 12,
        "with_price_inputs": 3,
        "with_pre_post_state": 2,
        "with_positive_gas_usd": 8,
        "gas_usd_sum": "0.95",
    }
    base.update(overrides)
    return _DictRow(base)


@pytest.mark.asyncio
async def test_get_ledger_quant_stats_sql_shape_and_conversion():
    conn = _FakeConn(fetchrow_row=_quant_stats_row())
    store = _make_store(conn)

    stats = await store.get_ledger_quant_stats(_DEPLOYMENT_ID)

    # Row → stats conversion: counts as ints, sum as exact Decimal.
    assert stats.total == 13
    assert stats.with_tx_hash == 12
    assert stats.with_cycle_id == 12
    assert stats.with_price_inputs == 3
    assert stats.with_pre_post_state == 2
    assert stats.with_positive_gas_usd == 8
    assert isinstance(stats.gas_usd_sum, Decimal)
    assert stats.gas_usd_sum == Decimal("0.95")
    # The anchor is NEVER computed by the aggregate query (caller-owned walk).
    assert stats.first_action_wallet_value_usd is None

    assert len(conn.calls) == 1
    kind, sql, args = conn.calls[0]
    assert kind == "fetchrow"
    assert args == (_DEPLOYMENT_ID,)
    assert "FROM transaction_ledger" in sql
    assert "WHERE deployment_id = $1" in sql
    # Server-side aggregation, O(1) rows.
    assert "COUNT(*)" in sql
    assert "COUNT(*) FILTER" in sql
    assert "SUM(" in sql
    # No blob column VALUES selected; presence predicates only.
    assert "SELECT *" not in sql
    assert "extracted_data_json" not in sql
    assert "NULLIF(pre_state_json::text, '') IS NOT NULL" in sql
    assert "NULLIF(post_state_json::text, '') IS NOT NULL" in sql
    # Exact-numeric cast runs ONLY behind the finite-numeric-literal guard.
    assert f"gas_usd ~ '{_PG_FINITE_NUMERIC_PATTERN}'" in sql
    assert "CASE WHEN gas_usd ~" in sql
    assert "ELSE 0 END" in sql
    # Zero-row coalescing to 0 (text-roundtripped for exactness).
    assert "COALESCE(SUM(" in sql
    assert "0)::text" in sql


@pytest.mark.asyncio
async def test_get_ledger_quant_stats_zero_rows_documented_mapping():
    """NULL/zero-row mapping pinned per field: counts 0, sum 0, anchor None."""
    conn = _FakeConn(fetchrow_row=_quant_stats_row(
        total=0,
        with_tx_hash=0,
        with_cycle_id=0,
        with_price_inputs=0,
        with_pre_post_state=0,
        with_positive_gas_usd=0,
        gas_usd_sum="0",
    ))
    store = _make_store(conn)

    stats = await store.get_ledger_quant_stats(_DEPLOYMENT_ID)
    assert stats.total == 0
    assert stats.gas_usd_sum == Decimal("0")
    assert stats.first_action_wallet_value_usd is None

    # Defensive: a missing row degrades to the same documented zeros.
    conn_none = _FakeConn(fetchrow_row=None)
    store_none = _make_store(conn_none)
    stats_none = await store_none.get_ledger_quant_stats(_DEPLOYMENT_ID)
    assert stats_none.total == 0
    assert stats_none.gas_usd_sum == Decimal("0")
    assert stats_none.first_action_wallet_value_usd is None


def test_pg_finite_numeric_guard_semantics():
    """The guard pattern itself: finite literals pass, everything else is 0.

    PG's ``~`` operator is POSIX regex — semantics here are pinned with
    Python ``re`` over the SAME pattern string (no PCRE-only syntax is used).
    A guard that let 'NaN' / 'Infinity' through would poison the LTD SUM
    (both are valid PG ``numeric`` casts!); one that let garbage through
    would make the whole aggregate raise and zero every tile.
    """
    import re as _re

    matches = lambda s: _re.match(_PG_FINITE_NUMERIC_PATTERN, s) is not None  # noqa: E731

    for ok in ("0", "0.95", "-3", "+2.5", ".5", "2.", "1e-5", "3E+10", "-0.001"):
        assert matches(ok), f"finite literal must pass the guard: {ok!r}"
    for bad in ("NaN", "nan", "Infinity", "-Infinity", "inf", "garbage!!", "1.2.3", "", " 1", "0x10"):
        assert not matches(bad), f"non-finite/garbage must be excluded: {bad!r}"


@pytest.mark.asyncio
async def test_get_ledger_anchor_candidates_sql_shape_and_conversion():
    rows = [
        _DictRow({
            "id": "ledger-row-1",
            "timestamp": datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC),
            "pre_state_text": '{"wallet_balances": {"USDC": "1000"}}',
            "price_inputs_text": '{"USDC": {"price_usd": "1.0"}}',
        }),
    ]
    conn = _FakeConn(fetch_rows=rows)
    store = _make_store(conn)

    entries = await store.get_ledger_anchor_candidates(_DEPLOYMENT_ID, limit=64, offset=0)

    assert len(entries) == 1
    entry = entries[0]
    assert entry.id == "ledger-row-1"  # persisted identity, not a fabricated UUID
    assert entry.deployment_id == _DEPLOYMENT_ID
    assert entry.timestamp == datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    assert entry.pre_state_json == '{"wallet_balances": {"USDC": "1000"}}'
    assert entry.price_inputs_json == '{"USDC": {"price_usd": "1.0"}}'

    assert len(conn.calls) == 1
    kind, sql, args = conn.calls[0]
    assert kind == "fetch"
    assert args == (_DEPLOYMENT_ID, 64, 0)
    # Projection: row identity plus ONLY the three columns the anchor walk reads.
    select_clause = sql.split("FROM", 1)[0]
    assert "SELECT id," in sql
    assert "timestamp" in select_clause
    assert "pre_state_json::text" in select_clause
    assert "price_inputs_json::text" in select_clause
    assert "post_state_json" not in sql
    assert "extracted_data_json" not in sql
    assert "SELECT *" not in sql
    # Blob-presence filtered in SQL; ascending; mandatory LIMIT bound.
    assert "WHERE deployment_id = $1" in sql
    assert "NULLIF(pre_state_json::text, '') IS NOT NULL" in sql
    assert "NULLIF(price_inputs_json::text, '') IS NOT NULL" in sql
    assert "ORDER BY timestamp ASC NULLS FIRST, id ASC" in sql
    assert "LIMIT $2 OFFSET $3" in sql


@pytest.mark.asyncio
async def test_get_ledger_anchor_candidates_zero_limit_short_circuits():
    conn = _FakeConn()
    store = _make_store(conn)

    assert await store.get_ledger_anchor_candidates(_DEPLOYMENT_ID, limit=0) == []
    assert conn.calls == []
