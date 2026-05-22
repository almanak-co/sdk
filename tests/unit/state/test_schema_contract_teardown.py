"""VIB-4049 PR2 — teardown table presence/absence across the schema contracts.

Composition assertions for the post-PR2 contract layout:

- ``ACCOUNTING_SCHEMA_CONTRACT`` MUST NOT list any teardown table.
  Local SQLite creates ``teardown_requests`` / ``teardown_execution_state``
  / ``teardown_approvals`` lazily inside the teardown state managers' own
  ``_init_db`` methods. Listing them in the SQLite contract would require
  every local strategy folder to materialize the tables at gateway boot
  before any teardown work has occurred — a regression of the lazy-create
  pattern those managers were built around.

- ``TEARDOWN_SCHEMA_CONTRACT_POSTGRES`` MUST list every teardown table.
  Hosted Postgres is owned outside this repo (``metrics-database`` /
  CLAUDE.md "Database schema ownership"); the contract is the boot-time
  gate that fails closed when the operator hasn't applied the
  metrics-database#32 migration yet. Dropping a teardown table from this
  contract would silently re-enable a path where the gateway boots,
  accepts a teardown request, then crashes on the first INSERT.

VIB-4722 collapsed the split SQLite / Postgres contract dicts (and the
``_POSTGRES_DEFERRED_TABLES`` machinery) into a single accounting contract
keyed on ``deployment_id`` (blueprint 29 §3). The teardown bridge tables
remain Postgres-only; this file locks the SQLite-absent / Postgres-present
invariant for them.

Test added per CodeRabbit review on PR #2234; updated for VIB-4722.
"""

from __future__ import annotations

from almanak.framework.state.schema_contract import (
    ACCOUNTING_SCHEMA_CONTRACT,
    TEARDOWN_SCHEMA_CONTRACT_POSTGRES,
)

# The canonical teardown table names — pinned here so a rename in
# ``schema_contract.py`` must also touch this assertion (and the
# corresponding metrics-database migration).
_TEARDOWN_TABLES = frozenset(
    {
        "teardown_requests",
        "teardown_execution_state",
        "teardown_approvals",
    }
)


def test_teardown_tables_absent_from_sqlite_contract() -> None:
    """Local SQLite contract MUST NOT list teardown tables.

    Local teardown managers self-bootstrap inside their own ``_init_db``
    (``SQLiteTeardownStateManager``, ``SQLiteTeardownStateAdapter``). Adding
    them to the SQLite contract would force every fresh strategy folder to
    materialize teardown tables at gateway boot — before any teardown work
    has occurred — re-introducing the lazy-create regression those managers
    were built around.
    """
    overlap = _TEARDOWN_TABLES & set(ACCOUNTING_SCHEMA_CONTRACT)
    assert overlap == frozenset(), (
        f"Teardown tables leaked into the SQLite contract: {sorted(overlap)}. "
        "Local SQLite teardown managers self-bootstrap via ``_init_db`` — "
        "listing them in ``ACCOUNTING_SCHEMA_CONTRACT`` would force "
        "every strategy folder to materialize them at gateway boot. Move "
        "the entry to ``TEARDOWN_SCHEMA_CONTRACT_POSTGRES`` only."
    )


def test_teardown_tables_present_in_postgres_contract() -> None:
    """Hosted Postgres contract MUST list every teardown table.

    Hosted Postgres DDL is owned by the separate ``metrics-database`` repo
    (CLAUDE.md "Database schema ownership" — the SDK never creates Postgres
    tables at runtime). The Postgres contract is the boot-time gate that
    fails closed when the metrics-database migration hasn't been applied
    yet; dropping a teardown table from the contract would silently let the
    gateway boot and then crash on the first INSERT during a real teardown.
    """
    missing = _TEARDOWN_TABLES - set(TEARDOWN_SCHEMA_CONTRACT_POSTGRES)
    assert missing == frozenset(), (
        f"Teardown tables missing from the Postgres contract: {sorted(missing)}. "
        "Hosted Postgres relies on the schema-contract check at boot to "
        "refuse a stale metrics-database — without these entries the gateway "
        "would boot against a migration-less DB and crash on the first "
        "teardown INSERT. Add them via ``TEARDOWN_SCHEMA_CONTRACT_POSTGRES``."
    )
    # Column-level sanity: every hosted-only teardown table declares the
    # expected column shape directly in the teardown contract.
    for table, expected_cols in TEARDOWN_SCHEMA_CONTRACT_POSTGRES.items():
        assert expected_cols, f"Postgres teardown contract for {table} is empty"
        assert "deployment_id" in expected_cols, f"{table} missing deployment_id"
