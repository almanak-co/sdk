"""D3.F4 regression — VIB-4205 / T19 (was VIB-4208 / T22).

T22 introduced ``position_registry`` and ``migration_state`` to the
SQLite contract while keeping the hosted Postgres contract deferred via
``_POSTGRES_DEFERRED_TABLES`` so the gateway could boot before the
metrics-database migration (VIB-4191) landed.

T19 (VIB-4205) inverts that posture: the Postgres writer paths now ship
in ``state_service.py``, and the deferral entries MUST be removed so the
boot-time schema validator fails loud when the deployed metrics-database
schema is missing either table or the new
``accounting_events.position_reference`` column.

This test fires loud if a future PR re-adds either table to the deferred
set — that would silently re-disable the fail-loud guard for hosted
column drift on the position_registry / migration_state tables.
"""

from __future__ import annotations

from almanak.framework.state.schema_contract import (
    ACCOUNTING_SCHEMA_CONTRACT_POSTGRES,
    ACCOUNTING_SCHEMA_CONTRACT_SQLITE,
    _POSTGRES_DEFERRED_COLUMNS,
    _POSTGRES_DEFERRED_TABLES,
)


def test_postgres_deferred_tables_no_longer_includes_t19_targets() -> None:
    """T19: ``_POSTGRES_DEFERRED_TABLES`` MUST NOT carry the T19 tables.

    Re-adding either name would silently re-disable the fail-loud boot
    guard for column drift on those tables. The Infra dependency is
    VIB-4191 (metrics-database Prisma migration); see
    ``schema_validator.validate_postgres_schema_or_raise`` for the
    operator-visible error message.
    """
    assert "migration_state" not in _POSTGRES_DEFERRED_TABLES, (
        "VIB-4205 / T19: migration_state was promoted out of "
        "_POSTGRES_DEFERRED_TABLES — re-adding it would silently re-disable "
        "the fail-loud boot guard for column drift on this hosted table."
    )
    assert "position_registry" not in _POSTGRES_DEFERRED_TABLES, (
        "VIB-4205 / T19: position_registry was promoted out of "
        "_POSTGRES_DEFERRED_TABLES — re-adding it would silently re-disable "
        "the fail-loud boot guard for column drift on this hosted table."
    )


def test_postgres_deferred_columns_no_longer_includes_position_reference() -> None:
    """T19: ``_POSTGRES_DEFERRED_COLUMNS`` MUST NOT carry the T10 column.

    The ``accounting_events.position_reference`` column (VIB-4196 / T10)
    is now required on hosted Postgres too. Re-adding it would silently
    re-disable the fail-loud guard for that column.
    """
    ae_deferred = _POSTGRES_DEFERRED_COLUMNS.get("accounting_events", frozenset())
    assert "position_reference" not in ae_deferred, (
        "VIB-4205 / T19: accounting_events.position_reference was promoted "
        "out of _POSTGRES_DEFERRED_COLUMNS — re-adding it would silently "
        "re-disable the fail-loud boot guard for that column."
    )


def test_postgres_contract_now_carries_both_t19_tables() -> None:
    """T19: both tables MUST flow through to the derived Postgres contract.

    With the deferral entries removed, the dict-comprehension at
    ``schema_contract.py`` builds the Postgres contract from the SQLite
    contract for these tables too. The fail-loud boot guard now covers
    them via ``validate_postgres_schema_or_raise``.
    """
    assert "position_registry" in ACCOUNTING_SCHEMA_CONTRACT_POSTGRES, (
        "T19 ships the Postgres writers; the Postgres contract MUST include "
        "position_registry so missing columns fail the boot validator."
    )
    assert "migration_state" in ACCOUNTING_SCHEMA_CONTRACT_POSTGRES, (
        "T19 ships the Postgres writers; the Postgres contract MUST include "
        "migration_state so missing columns fail the boot validator."
    )


def test_postgres_contract_carries_position_reference_on_accounting_events() -> None:
    """T19: ``accounting_events.position_reference`` is required on hosted PG."""
    pg_cols = ACCOUNTING_SCHEMA_CONTRACT_POSTGRES["accounting_events"]
    assert "position_reference" in pg_cols, (
        "T19 (VIB-4205) lifts the T10 deferral; the Postgres contract for "
        "accounting_events MUST require position_reference so VIB-4191 "
        "migration drift is caught at boot."
    )


def test_sqlite_contract_still_includes_both_tables() -> None:
    """The SQLite contract STILL covers both — local SDK is SDK-owned."""
    assert "migration_state" in ACCOUNTING_SCHEMA_CONTRACT_SQLITE, (
        "SQLite contract MUST include migration_state — local SDK is SDK-owned per AGENTS.md."
    )
    assert "position_registry" in ACCOUNTING_SCHEMA_CONTRACT_SQLITE, (
        "SQLite contract MUST include position_registry — local SDK is SDK-owned per AGENTS.md."
    )
