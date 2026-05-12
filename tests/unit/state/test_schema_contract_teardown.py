"""VIB-4049 PR2 — teardown table presence/absence across the schema contracts.

Composition assertions for the post-PR2 contract layout:

- ``ACCOUNTING_SCHEMA_CONTRACT_SQLITE`` MUST NOT list any teardown table.
  Local SQLite creates ``teardown_requests`` / ``teardown_execution_state``
  / ``teardown_approvals`` lazily inside the teardown state managers' own
  ``_init_db`` methods. Listing them in the SQLite contract would require
  every local strategy folder to materialize the tables at gateway boot
  before any teardown work has occurred — a regression of the lazy-create
  pattern those managers were built around.

- ``ACCOUNTING_SCHEMA_CONTRACT_POSTGRES`` MUST list every teardown table.
  Hosted Postgres is owned outside this repo (``metrics-database`` /
  CLAUDE.md "Database schema ownership"); the contract is the boot-time
  gate that fails closed when the operator hasn't applied the
  metrics-database#32 migration yet. Dropping a teardown table from this
  contract would silently re-enable a path where the gateway boots,
  accepts a teardown request, then crashes on the first INSERT.

- ``_POSTGRES_DEFERRED_TABLES`` filtering MUST still apply after the
  teardown tables are merged in. The composition order in
  ``schema_contract.py`` is ``{<sqlite tables - deferred>, <teardown
  Postgres>}`` — a future contributor refactoring the merge could
  accidentally re-add a deferred table to the Postgres contract via the
  teardown branch. Lock the invariant explicitly.

Test added per CodeRabbit review on PR #2234.
"""

from __future__ import annotations

from almanak.framework.state.schema_contract import (
    ACCOUNTING_SCHEMA_CONTRACT_POSTGRES,
    ACCOUNTING_SCHEMA_CONTRACT_SQLITE,
    TEARDOWN_SCHEMA_CONTRACT_POSTGRES,
    _POSTGRES_DEFERRED_TABLES,
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
    overlap = _TEARDOWN_TABLES & set(ACCOUNTING_SCHEMA_CONTRACT_SQLITE)
    assert overlap == frozenset(), (
        f"Teardown tables leaked into the SQLite contract: {sorted(overlap)}. "
        "Local SQLite teardown managers self-bootstrap via ``_init_db`` — "
        "listing them in ``ACCOUNTING_SCHEMA_CONTRACT_SQLITE`` would force "
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
    missing = _TEARDOWN_TABLES - set(ACCOUNTING_SCHEMA_CONTRACT_POSTGRES)
    assert missing == frozenset(), (
        f"Teardown tables missing from the Postgres contract: {sorted(missing)}. "
        "Hosted Postgres relies on the schema-contract check at boot to "
        "refuse a stale metrics-database — without these entries the gateway "
        "would boot against a migration-less DB and crash on the first "
        "teardown INSERT. Add them via ``TEARDOWN_SCHEMA_CONTRACT_POSTGRES``."
    )
    # Column-level sanity: every column declared in
    # ``TEARDOWN_SCHEMA_CONTRACT_POSTGRES`` reaches the merged contract.
    for table, expected_cols in TEARDOWN_SCHEMA_CONTRACT_POSTGRES.items():
        assert ACCOUNTING_SCHEMA_CONTRACT_POSTGRES[table] == expected_cols, (
            f"Postgres contract column drift for {table}: "
            f"merged={sorted(ACCOUNTING_SCHEMA_CONTRACT_POSTGRES[table])} "
            f"vs source={sorted(expected_cols)}"
        )


def test_postgres_deferred_tables_filter_preserved_with_teardown_merge() -> None:
    """``_POSTGRES_DEFERRED_TABLES`` filtering still works post-merge.

    The Postgres contract is composed as
    ``{<sqlite - _POSTGRES_DEFERRED_TABLES>, <TEARDOWN_SCHEMA_CONTRACT_POSTGRES>}``.
    A future refactor of the merge could accidentally re-add a deferred
    table to the Postgres contract via the teardown branch (e.g. by
    spreading SQLite into the teardown source map). Lock the invariant
    explicitly: no deferred table is allowed in the merged Postgres
    contract, and no teardown table is in the deferred set.
    """
    leaked_deferred = _POSTGRES_DEFERRED_TABLES & set(ACCOUNTING_SCHEMA_CONTRACT_POSTGRES)
    assert leaked_deferred == frozenset(), (
        f"Deferred tables leaked into the Postgres contract: {sorted(leaked_deferred)}. "
        "Either remove them from ``_POSTGRES_DEFERRED_TABLES`` (and land the "
        "corresponding metrics-database migration) or fix the contract merge "
        "to keep them out — silent leakage would break hosted boot."
    )
    # And the inverse: teardown tables MUST NOT be in the deferred set —
    # they are the OPPOSITE of deferred (Postgres-only, no SQLite analogue).
    teardown_in_deferred = _TEARDOWN_TABLES & _POSTGRES_DEFERRED_TABLES
    assert teardown_in_deferred == frozenset(), (
        f"Teardown tables incorrectly listed as deferred: {sorted(teardown_in_deferred)}. "
        "``_POSTGRES_DEFERRED_TABLES`` is for SQLite tables awaiting their "
        "Postgres migration — teardown tables are Postgres-only by design."
    )
