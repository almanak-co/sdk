"""Schema-contract validator tests — VIB-3763.

Test IDs T-3763-1..T-3763-10. These pin the boot-time invariant that the
gateway refuses to start when the live state backend is missing any
column the SDK's accounting writers require.

Why this matters: the April 29 run had 8/10 strategies write zero
accounting rows with NO operator-visible signal. Catching schema drift at
boot is the single mechanism that turns silent first-iteration failures
into supervisor-visible refusals.
"""

from __future__ import annotations

import asyncio
import sqlite3
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.schema_contract import (
    ACCOUNTING_SCHEMA_CONTRACT,
    SchemaContractViolation,
    format_violations,
)
from almanak.framework.state.schema_validator import (
    validate_postgres_schema_or_raise,
    validate_sqlite_schema_or_raise,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def initialized_sqlite_db(tmp_path) -> str:
    """Create + initialize a fresh SQLite DB so all tables/columns exist."""
    db_path = str(tmp_path / "state.db")
    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    asyncio.get_event_loop().run_until_complete(store.initialize())
    asyncio.get_event_loop().run_until_complete(store.close())
    return db_path


# ---------------------------------------------------------------------------
# T-3763-1 / T-3763-2: contract is non-empty and self-consistent
# ---------------------------------------------------------------------------
def test_t_3763_1_contract_lists_all_accounting_tables() -> None:
    """T-3763-1: every accounting table the runner writes is in the contract.

    Anti-gaming guard: a future PR that adds a new accounting table but
    forgets to register it here would skip schema validation entirely.
    Pinning the table set forces explicit registration.
    """
    expected_tables = {
        "portfolio_snapshots",
        "portfolio_metrics",
        "transaction_ledger",
        "accounting_events",
        "accounting_outbox",
    }
    assert set(ACCOUNTING_SCHEMA_CONTRACT) == expected_tables


def test_t_3763_2_contract_columns_are_non_empty() -> None:
    """T-3763-2: each table contract names at least one required column."""
    for table, cols in ACCOUNTING_SCHEMA_CONTRACT.items():
        assert cols, f"contract for {table} is empty"
        assert all(isinstance(c, str) and c for c in cols), table


# ---------------------------------------------------------------------------
# T-3763-3: clean SQLite passes
# ---------------------------------------------------------------------------
def test_t_3763_3_clean_sqlite_passes(initialized_sqlite_db: str) -> None:
    """T-3763-3: a freshly-migrated SQLite DB passes the contract check."""
    # Should not raise.
    validate_sqlite_schema_or_raise(initialized_sqlite_db)


# ---------------------------------------------------------------------------
# T-3763-4: SQLite missing column → raise
# ---------------------------------------------------------------------------
def test_t_3763_4_sqlite_missing_column_raises(tmp_path) -> None:
    """T-3763-4: drop a column from a real DB; validator must refuse."""
    db_path = str(tmp_path / "drift.db")
    # Build a portfolio_snapshots table that omits a required column.
    with sqlite3.connect(db_path) as conn:
        # Snapshots table missing wallet_balances_json (required).
        conn.executescript(
            """
            CREATE TABLE portfolio_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                deployment_id TEXT DEFAULT '',
                cycle_id TEXT DEFAULT '',
                execution_mode TEXT DEFAULT '',
                timestamp TEXT NOT NULL,
                iteration_number INTEGER DEFAULT 0,
                total_value_usd TEXT NOT NULL,
                available_cash_usd TEXT NOT NULL,
                deployed_capital_usd TEXT DEFAULT '0',
                wallet_total_value_usd TEXT DEFAULT '0',
                value_confidence TEXT DEFAULT 'HIGH',
                positions_json TEXT NOT NULL,
                token_prices_json TEXT DEFAULT '{}',
                chain TEXT,
                created_at TEXT NOT NULL
            );
            """
        )

    with pytest.raises(SchemaContractViolation) as exc:
        validate_sqlite_schema_or_raise(db_path)
    msg = str(exc.value)
    assert "portfolio_snapshots.wallet_balances_json" in msg
    assert "Local SQLite" in msg


# ---------------------------------------------------------------------------
# T-3763-5: SQLite missing entire table → raise
# ---------------------------------------------------------------------------
def test_t_3763_5_sqlite_missing_table_raises(tmp_path) -> None:
    """T-3763-5: a missing table reports every required column as missing."""
    db_path = str(tmp_path / "no_outbox.db")
    # Empty DB: PRAGMA table_info returns no rows for any of the contract's
    # tables, so every required column is reported missing.
    sqlite3.connect(db_path).close()
    with pytest.raises(SchemaContractViolation) as exc:
        validate_sqlite_schema_or_raise(db_path)
    msg = str(exc.value)
    # Spot-check: a known column from each table appears.
    assert "portfolio_snapshots.id" in msg
    assert "accounting_outbox.id" in msg


# ---------------------------------------------------------------------------
# T-3763-6: format_violations is deterministic
# ---------------------------------------------------------------------------
def test_t_3763_6_format_violations_is_sorted() -> None:
    """T-3763-6: violation listing is sorted (table, then column).

    Stability matters because operator runbooks grep these messages.
    """
    violations = {
        "transaction_ledger": {"chain", "id"},
        "portfolio_snapshots": {"strategy_id", "id"},
    }
    rendered = format_violations("Test", violations)
    lines = rendered.split("\n")
    assert lines[0] == "Test schema is missing required accounting columns:"
    # portfolio_snapshots before transaction_ledger; columns sorted within.
    assert lines[1] == "  - portfolio_snapshots.id"
    assert lines[2] == "  - portfolio_snapshots.strategy_id"
    assert lines[3] == "  - transaction_ledger.chain"
    assert lines[4] == "  - transaction_ledger.id"


# ---------------------------------------------------------------------------
# Postgres path — mocked asyncpg
# ---------------------------------------------------------------------------
def _make_pg_mock(
    column_map: dict[str, list[str]],
) -> tuple[AsyncMock, MagicMock]:
    """Return a (connect_async_mock, conn_mock) pair where conn.fetch returns
    the columns declared by ``column_map[table]``.
    """
    fake_conn = MagicMock()

    async def _fetch(query: str, *args):
        # Last positional arg is always the table name.
        table = args[-1]
        return [{"column_name": c} for c in column_map.get(table, [])]

    fake_conn.fetch = _fetch
    fake_conn.close = AsyncMock()

    connect = AsyncMock(return_value=fake_conn)
    return connect, fake_conn


# ---------------------------------------------------------------------------
# T-3763-7: Postgres clean → passes
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_t_3763_7_postgres_clean_passes() -> None:
    """T-3763-7: PG with all required columns passes."""
    full_columns = {
        table: list(cols) for table, cols in ACCOUNTING_SCHEMA_CONTRACT.items()
    }
    connect, _ = _make_pg_mock(full_columns)
    with patch("asyncpg.connect", connect):
        await validate_postgres_schema_or_raise("postgres://user:pass@host/db")


# ---------------------------------------------------------------------------
# T-3763-8: Postgres missing column → raise
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_t_3763_8_postgres_missing_column_raises() -> None:
    """T-3763-8: PG missing a single column raises with that column named."""
    full_columns = {
        table: list(cols) for table, cols in ACCOUNTING_SCHEMA_CONTRACT.items()
    }
    full_columns["transaction_ledger"].remove("post_state_json")
    connect, _ = _make_pg_mock(full_columns)
    with patch("asyncpg.connect", connect):
        with pytest.raises(SchemaContractViolation) as exc:
            await validate_postgres_schema_or_raise("postgres://x/y")
    msg = str(exc.value)
    assert "transaction_ledger.post_state_json" in msg
    assert "metrics-database" in msg


# ---------------------------------------------------------------------------
# T-3763-9: Postgres validator does NOT issue DDL
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_t_3763_9_postgres_validator_never_issues_ddl() -> None:
    """T-3763-9: the PG validator must be read-only.

    CLAUDE.md hard rule: 'metrics_db schema is owned outside this repo'.
    A future refactor that tries to ALTER TABLE / CREATE TABLE from this
    process is a CLAUDE.md violation. This test pins it: every query the
    validator issues must be a SELECT.
    """
    issued_queries: list[str] = []

    async def _fetch(query: str, *args):
        issued_queries.append(query)
        # Return everything required so we don't raise.
        table = args[-1]
        return [
            {"column_name": c} for c in ACCOUNTING_SCHEMA_CONTRACT.get(table, set())
        ]

    fake_conn = MagicMock()
    fake_conn.fetch = _fetch
    fake_conn.close = AsyncMock()
    connect = AsyncMock(return_value=fake_conn)

    with patch("asyncpg.connect", connect):
        await validate_postgres_schema_or_raise("postgres://x/y")

    assert issued_queries, "validator did not issue any query"
    for q in issued_queries:
        normalized = q.strip().upper()
        assert normalized.startswith("SELECT "), (
            f"validator issued non-SELECT query (potential DDL): {q!r}"
        )
        for forbidden in ("ALTER", "CREATE", "DROP", "INSERT", "UPDATE", "DELETE", "TRUNCATE"):
            assert forbidden not in normalized.split(), (
                f"validator query contains forbidden DDL/DML token {forbidden!r}: {q!r}"
            )


# ---------------------------------------------------------------------------
# T-3763-10: Postgres missing table reports every column
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_t_3763_10_postgres_missing_table_reports_all_columns() -> None:
    """T-3763-10: a missing table is treated as 'every column missing'."""
    full_columns = {
        table: list(cols) for table, cols in ACCOUNTING_SCHEMA_CONTRACT.items()
    }
    full_columns["accounting_events"] = []  # table missing entirely
    connect, _ = _make_pg_mock(full_columns)
    with patch("asyncpg.connect", connect):
        with pytest.raises(SchemaContractViolation) as exc:
            await validate_postgres_schema_or_raise("postgres://x/y")
    msg = str(exc.value)
    # A few representative columns must be reported.
    assert "accounting_events.id" in msg
    assert "accounting_events.event_type" in msg
    assert "accounting_events.payload_json" in msg


# ---------------------------------------------------------------------------
# Sanity — boot wrapper branches on is_hosted() (the canonical mode signal)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_validate_state_schema_at_boot_routes_to_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Boot helper must call the PG validator in hosted mode (AGENT_ID set).

    Mode is now read from ``is_hosted()`` rather than ``settings.database_url``
    directly (CodeRabbit major fix on PR #1977) so a reordered boot path or
    direct unit call cannot drift from ``validate_deployment_invariants``.
    """
    from almanak.gateway._server_start_helpers import validate_state_schema_at_boot

    monkeypatch.setenv("AGENT_ID", "agent-test-pg")

    pg_mock = AsyncMock()
    sq_mock = MagicMock()
    monkeypatch.setattr(
        "almanak.framework.state.schema_validator.validate_postgres_schema_or_raise",
        pg_mock,
    )
    monkeypatch.setattr(
        "almanak.framework.state.schema_validator.validate_sqlite_schema_or_raise",
        sq_mock,
    )

    settings = MagicMock()
    settings.database_url = "postgres://x/y"
    await validate_state_schema_at_boot(settings)

    pg_mock.assert_awaited_once_with("postgres://x/y")
    sq_mock.assert_not_called()


@pytest.mark.asyncio
async def test_validate_state_schema_at_boot_hosted_without_database_url_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In hosted mode, an empty/whitespace ``database_url`` MUST fail loudly.

    Defence-in-depth check inside the schema validator itself for direct
    unit-test callers that build a ``GatewaySettings`` and skip Phase 0
    (``validate_deployment_invariants``).
    """
    from almanak.gateway._server_start_helpers import validate_state_schema_at_boot

    monkeypatch.setenv("AGENT_ID", "agent-test-pg-empty")

    settings = MagicMock()
    settings.database_url = "   "
    with pytest.raises(RuntimeError, match="ALMANAK_GATEWAY_DATABASE_URL is unset"):
        await validate_state_schema_at_boot(settings)


@pytest.mark.asyncio
async def test_validate_state_schema_at_boot_routes_to_sqlite(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Boot helper must call SQLite migrations + validator in local mode."""
    from almanak.gateway._server_start_helpers import validate_state_schema_at_boot

    monkeypatch.delenv("AGENT_ID", raising=False)
    db_path = str(tmp_path / "boot.db")
    monkeypatch.setenv("ALMANAK_STATE_DB", db_path)

    sq_validate = MagicMock()
    monkeypatch.setattr(
        "almanak.framework.state.schema_validator.validate_sqlite_schema_or_raise",
        sq_validate,
    )

    settings = MagicMock()
    settings.database_url = None
    await validate_state_schema_at_boot(settings)

    sq_validate.assert_called_once_with(db_path)
    # Migrations actually ran — file exists, tables present.
    assert sqlite3.connect(db_path).execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='portfolio_snapshots'"
    ).fetchone() is not None
