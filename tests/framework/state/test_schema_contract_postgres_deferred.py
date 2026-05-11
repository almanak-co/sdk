"""D3.F4 regression — VIB-4208 / T22.

The hosted Postgres half of the cutover storage tables
(``position_registry``, ``migration_state``) is owned by T19 / VIB-4205
plus the cross-repo metrics-database migration. Until that lands,
``_POSTGRES_DEFERRED_TABLES`` MUST continue to exclude both tables so
the hosted gateway's boot-time schema validator does NOT refuse to
start when the deployed Postgres schema is missing them.

This test fires loud if a future PR accidentally removes either entry —
that would brick hosted gateway boot until the metrics-database
migration also lands.
"""

from __future__ import annotations

from almanak.framework.state.schema_contract import (
    ACCOUNTING_SCHEMA_CONTRACT_POSTGRES,
    ACCOUNTING_SCHEMA_CONTRACT_SQLITE,
    _POSTGRES_DEFERRED_TABLES,
)


def test_postgres_deferred_tables_excludes_migration_state_and_position_registry() -> None:
    """``_POSTGRES_DEFERRED_TABLES`` MUST contain both registry tables."""
    assert "migration_state" in _POSTGRES_DEFERRED_TABLES, (
        "VIB-4208 contract: migration_state MUST stay in _POSTGRES_DEFERRED_TABLES "
        "until T19 / VIB-4205 lands the Postgres half + metrics-database migration."
    )
    assert "position_registry" in _POSTGRES_DEFERRED_TABLES, (
        "VIB-4208 contract: position_registry MUST stay in _POSTGRES_DEFERRED_TABLES "
        "until T19 / VIB-4205 lands the Postgres half + metrics-database migration."
    )


def test_postgres_contract_does_not_carry_deferred_tables() -> None:
    """The derived ``ACCOUNTING_SCHEMA_CONTRACT_POSTGRES`` MUST NOT include them.

    A bug in ``_postgres_columns_for`` or the dict comprehension at the
    bottom of schema_contract.py could let a deferred table leak into
    the Postgres contract. This catches it.
    """
    for deferred in _POSTGRES_DEFERRED_TABLES:
        assert deferred not in ACCOUNTING_SCHEMA_CONTRACT_POSTGRES, (
            f"deferred table {deferred!r} unexpectedly present in the Postgres contract"
        )


def test_sqlite_contract_still_includes_both_tables() -> None:
    """The SQLite contract STILL covers both — local SDK is SDK-owned."""
    assert "migration_state" in ACCOUNTING_SCHEMA_CONTRACT_SQLITE, (
        "SQLite contract MUST include migration_state — local SDK is SDK-owned per AGENTS.md."
    )
    assert "position_registry" in ACCOUNTING_SCHEMA_CONTRACT_SQLITE, (
        "SQLite contract MUST include position_registry — local SDK is SDK-owned per AGENTS.md."
    )
