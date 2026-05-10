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
    ACCOUNTING_SCHEMA_CONTRACT_POSTGRES,
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

    VIB-4197 / T11 added ``position_registry`` (the atomic-commit primitive
    target) and ``migration_state`` (cutover-progress tracking) to the
    SQLite contract. The Postgres contract excludes both until VIB-4205 /
    T19 ships the hosted writer + ``metrics-database`` migration; that
    asymmetry is asserted in T-3763-2b (below).
    """
    expected_tables = {
        "portfolio_snapshots",
        "portfolio_metrics",
        "transaction_ledger",
        "accounting_events",
        "accounting_outbox",
        # T11 — local SQLite only until T19 lands hosted Postgres support.
        "position_registry",
        "migration_state",
    }
    assert set(ACCOUNTING_SCHEMA_CONTRACT) == expected_tables


def test_t_3763_2_contract_columns_are_non_empty() -> None:
    """T-3763-2: each table contract names at least one required column."""
    for table, cols in ACCOUNTING_SCHEMA_CONTRACT.items():
        assert cols, f"contract for {table} is empty"
        assert all(isinstance(c, str) and c for c in cols), table


def test_t_3763_2b_pg_contract_swaps_strategy_id_for_agent_id() -> None:
    """T-3763-2b (PR #2162 / Codex): the PG contract uses ``agent_id``
    everywhere the SQLite contract uses ``strategy_id`` — and ONLY there.

    Locks the rename invariant: a future drift between deployed
    metrics-database column names and ``ACCOUNTING_SCHEMA_CONTRACT_POSTGRES``
    will fail this test loudly rather than silently bricking the hosted
    validator.

    VIB-4197 / T11: the PG contract now legitimately excludes
    ``position_registry`` and ``migration_state`` (deferred until VIB-4205
    / T19 lands the hosted writer + ``metrics-database`` migration). The
    equality assertion below is therefore scoped to the table set the PG
    contract DOES cover; the deferred set is asserted separately so a
    future PR cannot silently add hosted boot requirements.

    VIB-4196 / T10 extended the deferral mechanism to per-column granularity
    via ``_POSTGRES_DEFERRED_COLUMNS`` (``accounting_events.position_reference``
    is local-only until T19). The column-level diff below subtracts the
    deferred columns before comparing — same intent as the table-level
    deferral, applied at column scope.
    """
    from almanak.framework.state.schema_contract import (
        _POSTGRES_DEFERRED_COLUMNS,
        _POSTGRES_DEFERRED_TABLES,
    )

    sqlite = ACCOUNTING_SCHEMA_CONTRACT  # alias to SQLite variant
    pg_tables = set(ACCOUNTING_SCHEMA_CONTRACT_POSTGRES)
    sqlite_tables = set(sqlite)
    assert pg_tables <= sqlite_tables, (
        f"PG contract has tables SQLite doesn't: {pg_tables - sqlite_tables}"
    )
    sqlite_only = sqlite_tables - pg_tables
    assert sqlite_only == _POSTGRES_DEFERRED_TABLES, (
        f"SQLite-only tables {sqlite_only} should equal "
        f"_POSTGRES_DEFERRED_TABLES {_POSTGRES_DEFERRED_TABLES}. If you added "
        "a new local-only table, list it in _POSTGRES_DEFERRED_TABLES; if "
        "you shipped a hosted Postgres writer, remove from the deferred set."
    )
    for table in pg_tables:
        sqlite_cols = sqlite[table]
        pg_cols = ACCOUNTING_SCHEMA_CONTRACT_POSTGRES[table]
        # ``strategy_id`` must NOT appear in any PG-shaped contract.
        assert "strategy_id" not in pg_cols, f"{table}: PG contract still lists 'strategy_id' — should be 'agent_id'"
        # If ``strategy_id`` was in the SQLite contract, ``agent_id``
        # must be in the PG contract for the same table.
        if "strategy_id" in sqlite_cols:
            assert "agent_id" in pg_cols, f"{table}: SQLite has strategy_id but PG is missing agent_id"
        # Every other column must be identical, modulo the per-column
        # Postgres deferrals (T10's mechanism for landing a column on
        # local SQLite while the metrics-database migration is in flight).
        deferred_cols = _POSTGRES_DEFERRED_COLUMNS.get(table, frozenset())
        sqlite_other = sqlite_cols - {"strategy_id"} - deferred_cols
        pg_other = pg_cols - {"agent_id"}
        assert sqlite_other == pg_other, (
            f"{table}: PG contract diverges from SQLite outside the "
            f"strategy_id↔agent_id rename and the deferred columns "
            f"{sorted(deferred_cols)}: "
            f"{(sqlite_other - pg_other) | (pg_other - sqlite_other)}"
        )
        # Deferred columns MUST appear in SQLite and MUST NOT appear in PG.
        for col in deferred_cols:
            assert col in sqlite_cols, (
                f"{table}: deferred column {col!r} is in "
                f"_POSTGRES_DEFERRED_COLUMNS but missing from SQLite contract"
            )
            assert col not in pg_cols, (
                f"{table}: deferred column {col!r} leaked into the PG "
                "contract before T19 / metrics-database migration"
            )


# ---------------------------------------------------------------------------
# VIB-4196 / T10: position_reference column is in the SQLite contract,
# defers from Postgres, and triggers the boot-guard when missing.
# ---------------------------------------------------------------------------
def test_vib_4196_position_reference_is_local_sqlite_only() -> None:
    """T10: position_reference must be SQLite-required and Postgres-deferred."""
    from almanak.framework.state.schema_contract import (
        _POSTGRES_DEFERRED_COLUMNS,
        ACCOUNTING_SCHEMA_CONTRACT_SQLITE,
        ACCOUNTING_SCHEMA_CONTRACT_POSTGRES,
    )

    assert (
        "position_reference"
        in ACCOUNTING_SCHEMA_CONTRACT_SQLITE["accounting_events"]
    ), "T10 column missing from SQLite contract"
    assert (
        "position_reference"
        not in ACCOUNTING_SCHEMA_CONTRACT_POSTGRES["accounting_events"]
    ), "T10 column leaked into Postgres contract before T19"
    deferred_for_ae = _POSTGRES_DEFERRED_COLUMNS.get("accounting_events", frozenset())
    assert "position_reference" in deferred_for_ae, (
        f"T10 column must be in _POSTGRES_DEFERRED_COLUMNS['accounting_events']; "
        f"got {deferred_for_ae}"
    )


def test_vib_4196_validator_refuses_when_position_reference_missing(tmp_path) -> None:
    """T10: drop position_reference from a real DB; validator must refuse to start."""
    db_path = str(tmp_path / "no-position-reference.db")

    # Build a fully-shaped DB, then drop the column.
    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    asyncio.get_event_loop().run_until_complete(store.initialize())
    asyncio.get_event_loop().run_until_complete(store.close())

    # SQLite supports DROP COLUMN since 3.35; if the runtime is older,
    # fall back to a rebuild that omits the column.
    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute("ALTER TABLE accounting_events DROP COLUMN position_reference")
            conn.commit()
        except sqlite3.OperationalError:
            # Older SQLite — recreate the table without the column.
            conn.executescript(
                """
                ALTER TABLE accounting_events RENAME TO _ae_old;
                CREATE TABLE accounting_events (
                    id TEXT PRIMARY KEY,
                    deployment_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    cycle_id TEXT NOT NULL,
                    execution_mode TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    chain TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    wallet_address TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    position_key TEXT NOT NULL,
                    ledger_entry_id TEXT,
                    tx_hash TEXT,
                    confidence TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    schema_version INTEGER NOT NULL DEFAULT 1
                );
                DROP TABLE _ae_old;
                """
            )
            conn.commit()

    with pytest.raises(SchemaContractViolation) as excinfo:
        validate_sqlite_schema_or_raise(db_path)
    msg = str(excinfo.value)
    assert "position_reference" in msg, (
        f"validator must name the missing column; got: {msg}"
    )


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
    """T-3763-7: PG with all required columns passes.

    Uses ``ACCOUNTING_SCHEMA_CONTRACT_POSTGRES`` (``agent_id`` instead of
    ``strategy_id``) — the deployed metrics-database shape, not the SDK
    SQLite shape (per Codex review on PR #2162).
    """
    full_columns = {table: list(cols) for table, cols in ACCOUNTING_SCHEMA_CONTRACT_POSTGRES.items()}
    connect, _ = _make_pg_mock(full_columns)
    with patch("asyncpg.connect", connect):
        await validate_postgres_schema_or_raise("postgres://user:pass@host/db")


# ---------------------------------------------------------------------------
# T-3763-8: Postgres missing column → raise
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_t_3763_8_postgres_missing_column_raises() -> None:
    """T-3763-8: PG missing a single column raises with that column named."""
    full_columns = {table: list(cols) for table, cols in ACCOUNTING_SCHEMA_CONTRACT_POSTGRES.items()}
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
        # Return everything required so we don't raise. Use the PG-shaped
        # contract since this exercises the hosted validator.
        table = args[-1]
        return [{"column_name": c} for c in ACCOUNTING_SCHEMA_CONTRACT_POSTGRES.get(table, set())]

    fake_conn = MagicMock()
    fake_conn.fetch = _fetch
    fake_conn.close = AsyncMock()
    connect = AsyncMock(return_value=fake_conn)

    with patch("asyncpg.connect", connect):
        await validate_postgres_schema_or_raise("postgres://x/y")

    assert issued_queries, "validator did not issue any query"
    for q in issued_queries:
        normalized = q.strip().upper()
        assert normalized.startswith("SELECT "), f"validator issued non-SELECT query (potential DDL): {q!r}"
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
    full_columns = {table: list(cols) for table, cols in ACCOUNTING_SCHEMA_CONTRACT_POSTGRES.items()}
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
async def test_validate_state_schema_at_boot_routes_to_sqlite(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
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
    assert (
        sqlite3.connect(db_path)
        .execute("SELECT name FROM sqlite_master WHERE type='table' AND name='portfolio_snapshots'")
        .fetchone()
        is not None
    )
