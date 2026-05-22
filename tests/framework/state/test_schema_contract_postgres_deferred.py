"""Schema-contract regression — VIB-4205 / T19, updated for VIB-4722.

VIB-4722 collapsed the split SQLite / Postgres contract dicts (and their
``_POSTGRES_DEFERRED_TABLES`` / ``_POSTGRES_DEFERRED_COLUMNS`` name-map
machinery) into a single contract keyed on the one identity column,
``deployment_id`` (blueprint 29 §3). The deferral mechanism no longer
exists; the behavioural guarantee it protected — that ``position_registry``,
``migration_state``, and ``accounting_events.position_reference`` are
required on hosted Postgres — is asserted directly below.
"""

from __future__ import annotations

from almanak.framework.state.schema_contract import (
    ACCOUNTING_SCHEMA_CONTRACT,
    TEARDOWN_SCHEMA_CONTRACT_POSTGRES,
)

HOSTED_SCHEMA_CONTRACT = {**ACCOUNTING_SCHEMA_CONTRACT, **TEARDOWN_SCHEMA_CONTRACT_POSTGRES}


def test_postgres_contract_carries_both_t19_tables() -> None:
    """Both T19 tables MUST be in the Postgres contract.

    The fail-loud boot guard (``validate_postgres_schema_or_raise``) covers
    them; dropping either entry would silently re-disable hosted column-drift
    detection.
    """
    assert "position_registry" in ACCOUNTING_SCHEMA_CONTRACT
    assert "migration_state" in ACCOUNTING_SCHEMA_CONTRACT


def test_postgres_contract_carries_position_reference_on_accounting_events() -> None:
    """``accounting_events.position_reference`` is required on hosted PG."""
    assert "position_reference" in ACCOUNTING_SCHEMA_CONTRACT["accounting_events"]


def test_sqlite_contract_still_includes_both_tables() -> None:
    """The SQLite contract STILL covers both — local SDK is SDK-owned."""
    assert "migration_state" in ACCOUNTING_SCHEMA_CONTRACT
    assert "position_registry" in ACCOUNTING_SCHEMA_CONTRACT


def test_one_identity_column_deployment_id_on_every_accounting_table() -> None:
    """Blueprint 29 §3: every accounting table keys on the single
    ``deployment_id`` column and no legacy hosted identity column.
    """
    for table, cols in ACCOUNTING_SCHEMA_CONTRACT.items():
        assert "deployment_id" in cols, f"{table} must require deployment_id"
        assert "agent_id" not in cols, f"{table} must not carry an agent_id identity column"


def test_sqlite_and_postgres_accounting_contracts_are_identical() -> None:
    """The accounting tables are identical on both backends — one contract.

    The Postgres contract additionally carries the Postgres-only teardown
    bridge tables, but every accounting table the SQLite contract lists
    must appear with the SAME column set on the Postgres side.
    """
    for table, cols in ACCOUNTING_SCHEMA_CONTRACT.items():
        assert HOSTED_SCHEMA_CONTRACT[table] == cols, (
            f"{table}: SQLite and Postgres contracts diverged — blueprint 29 "
            "requires one identity column on both backends."
        )
