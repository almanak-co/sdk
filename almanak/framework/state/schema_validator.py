"""Boot-time validation that the live state backend matches the SDK's schema
contract — VIB-3763, plan §D.

Two paths, one principle: refuse to start when a write would silently lose
columns.

* **SQLite (local, SDK-owned)**: migrations are run first (they are
  idempotent and self-healing), then ``PRAGMA table_info`` is introspected.
  If a required column is missing AFTER migrations, the SDK build itself
  is broken — the migration helper shipped without one of the columns the
  contract demands. Refuse to start.
* **Postgres (hosted, owned by the metrics-database repo)**: introspect
  ``information_schema.columns`` and refuse on any missing column. **No
  DDL.** A mismatch here means the metrics-database migration has not been
  deployed; the fix lives in that repo, per CLAUDE.md.

Both paths raise :class:`SchemaContractViolation` with a deterministic,
operator-grep-friendly message.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import TYPE_CHECKING

from almanak.framework.state.schema_contract import (
    ACCOUNTING_SCHEMA_CONTRACT,
    TEARDOWN_SCHEMA_CONTRACT_POSTGRES,
    SchemaContractViolation,
    format_violations,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    pass

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQLite path — local, SDK-owned, migrate-then-validate
# ---------------------------------------------------------------------------
def _sqlite_columns_for_table(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return the column names declared on ``table`` in the open connection.

    Returns an empty set if the table itself does not exist; callers
    treat that as "all required columns are missing" (table-shaped drift).
    """
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def validate_sqlite_schema_or_raise(db_path: str) -> None:
    """Assert every required accounting column is present on the SQLite DB.

    Caller must ensure the SQLite store has already been initialized so that
    the latest in-code migrations have run; this function is read-only.

    Raises :class:`SchemaContractViolation` listing every missing column
    across every accounting table when the DB has drifted from the
    contract.
    """
    violations: dict[str, set[str]] = {}
    with sqlite3.connect(db_path) as conn:
        for table, required in ACCOUNTING_SCHEMA_CONTRACT.items():
            actual = _sqlite_columns_for_table(conn, table)
            missing = set(required) - actual
            if missing:
                violations[table] = missing

    if not violations:
        logger.info(
            "Local SQLite schema contract: all %d accounting tables OK at %s",
            len(ACCOUNTING_SCHEMA_CONTRACT),
            db_path,
        )
        return

    detail = format_violations(f"Local SQLite ({db_path})", violations)
    raise SchemaContractViolation(
        f"{detail}\n"
        "The SDK's in-code SQLite migrations did not produce the columns the "
        "accounting writers require. This is an SDK build error — "
        "almanak/framework/state/backends/sqlite.py must be updated to add the "
        "missing columns before the gateway can start."
    )


# ---------------------------------------------------------------------------
# Postgres path — hosted, validate-only, NO DDL
# ---------------------------------------------------------------------------
async def validate_postgres_schema_or_raise(database_url: str) -> None:
    """Assert every required accounting column exists in hosted Postgres.

    Read-only. The hosted Postgres schema is owned by the separate
    ``metrics-database`` repo (CLAUDE.md hard rule); this gateway must
    never mutate it. Drift means the latest Prisma migration has not been
    applied to the deployed database.

    Raises :class:`SchemaContractViolation` listing every missing column.
    """
    import asyncpg

    from almanak.gateway.database import _strip_schema_param

    url, schema = _strip_schema_param(database_url)
    # Hosted Postgres sits behind pgbouncer in transaction pooling mode, which
    # cannot support server-side prepared statements: asyncpg's auto-named
    # ``__asyncpg_stmt_N__`` statements collide across multiplexed backends and
    # raise ``DuplicatePreparedStatementError`` from this boot-time fetch,
    # crash-looping the gateway before it ever serves the strategy. Disabling the
    # statement cache makes asyncpg issue unprepared queries, matching every
    # other asyncpg connection in this codebase (timeline store, lifecycle store,
    # state manager pool — all ``statement_cache_size=0``).
    conn = await asyncpg.connect(url, statement_cache_size=0)
    try:
        violations: dict[str, set[str]] = {}
        # Accounting uses one shared contract across SQLite and Postgres; hosted
        # additionally validates teardown bridge tables owned by metrics-db.
        contract = {**ACCOUNTING_SCHEMA_CONTRACT, **TEARDOWN_SCHEMA_CONTRACT_POSTGRES}
        for table, required in contract.items():
            if schema:
                rows = await conn.fetch(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
                    schema,
                    table,
                )
            else:
                rows = await conn.fetch(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
                    table,
                )
            actual = {r["column_name"] for r in rows}
            if not actual:
                violations[table] = set(required)
                continue
            missing = set(required) - actual
            if missing:
                violations[table] = missing
    finally:
        await conn.close()

    if not violations:
        logger.info(
            "Hosted Postgres schema contract: all %d accounting tables OK",
            len(contract),
        )
        return

    detail = format_violations("Hosted Postgres (metrics-database)", violations)
    raise SchemaContractViolation(
        f"{detail}\n"
        "The metrics-database schema is out of date. Apply the latest "
        "Prisma migration in the metrics-database repo and redeploy "
        "before starting this gateway. The SDK does NOT mutate hosted "
        "Postgres at runtime.\n"
        "Schema dependency: VIB-4191 "
        "(https://linear.app/almanak/issue/VIB-4191) tracks the metrics-database "
        "Prisma migration that lands the T19 (VIB-4205) tables and columns "
        "(position_registry, migration_state, accounting_events.position_reference)."
    )
