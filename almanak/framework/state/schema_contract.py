"""Schema contract for accounting tables — VIB-3763, blueprint 29.

The SDK writes accounting rows whose columns are produced from in-code
dataclasses. The deployed backend (SQLite locally, Postgres hosted) must
have every column those writers reference, or every write silently fails
in a way the operator does not see.

This module is the **single source of truth** for which columns the SDK
requires on each accounting table. It is consumed at gateway boot by
``almanak.framework.state.schema_validator`` to refuse to start when the
live backend is missing any required column.

Adding a new accounting column?

  1. Add it to the SQLite ``CREATE TABLE`` / ``_run_migrations()`` block
     in ``almanak/framework/state/backends/sqlite.py``.
  2. Add the column name to the right ``frozenset`` below.
  3. File a ``metrics-database`` Prisma migration in that repo.

Step 3 is what hosted gateways will refuse to start without. The contract
in this file is what catches the mismatch instead of letting it land as a
silent first-iteration failure.

Per blueprint 27 §accounting and CLAUDE.md "metrics_db schema is owned
outside this repo".

> **One identity (blueprint 29 §3).** Every deployment-scoped table — local
> SQLite *and* hosted Postgres — carries exactly one identity column,
> ``deployment_id``. There is no separate legacy identity column
> identity column and no runtime translation. The contract below is a
> **single dict** used for both backends — the gateway's SQLite vs.
> Postgres branch exists only for SQL dialect, never for the identity
> column name.
"""

from __future__ import annotations

# Required columns per accounting table.
#
# "Required" means: the SDK's writer paths reference this column at INSERT
# or UPDATE time. A backend missing the column will fail every write.
#
# Optional/decorative columns (e.g., audit-only metadata that the SDK does
# not yet read or write) are intentionally NOT listed here so a new SDK
# build can be deployed before its companion metrics-database migration
# without bricking startup.
#
# Identity column convention (blueprint 29 §3): every deployment-scoped
# table keys on the single ``deployment_id`` column on BOTH backends. The
# legacy split between local and hosted identity names is gone —
# there is one column, one name, one contract.
ACCOUNTING_SCHEMA_CONTRACT: dict[str, frozenset[str]] = {
    "portfolio_snapshots": frozenset(
        {
            "id",
            "deployment_id",
            "cycle_id",
            "execution_mode",
            "timestamp",
            "iteration_number",
            "total_value_usd",
            "available_cash_usd",
            "deployed_capital_usd",
            "wallet_total_value_usd",
            "value_confidence",
            "positions_json",
            "token_prices_json",
            "wallet_balances_json",
            "chain",
            "created_at",
        }
    ),
    "portfolio_metrics": frozenset(
        {
            "deployment_id",
            "initial_value_usd",
            "initial_timestamp",
            "deposits_usd",
            "withdrawals_usd",
            "gas_spent_usd",
            "total_value_usd",
            "positions_json",
            "cycle_id",
            "execution_mode",
            "is_complete",
            "updated_at",
        }
    ),
    "transaction_ledger": frozenset(
        {
            "id",
            "cycle_id",
            "deployment_id",
            "execution_mode",
            "timestamp",
            "intent_type",
            "token_in",
            "amount_in",
            "token_out",
            "amount_out",
            "effective_price",
            "slippage_bps",
            "gas_used",
            "gas_usd",
            "tx_hash",
            "chain",
            "protocol",
            "success",
            "error",
            "extracted_data_json",
            "price_inputs_json",
            "pre_state_json",
            "post_state_json",
        }
    ),
    "accounting_events": frozenset(
        {
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
            # VIB-4196 / T10: forward-compat JSON pointer between
            # accounting_events and the position_registry. Required on
            # BOTH backends as of T19 (VIB-4205).
            "position_reference",
        }
    ),
    "accounting_outbox": frozenset(
        {
            "id",
            "deployment_id",
            "cycle_id",
            "ledger_entry_id",
            "intent_type",
            "wallet_address",
            "position_key",
            "market_id",
            "status",
            "attempts",
            "error",
            "created_at",
            "updated_at",
        }
    ),
    # VIB-4197 / T11 — atomic ledger+registry+handle commit primitive.
    # The 16-column shape ratified by PRD §Registry Data Shape and the
    # blueprint 28 §3 reference.
    "position_registry": frozenset(
        {
            "deployment_id",
            "chain",
            "primitive",
            "accounting_category",
            "physical_identity_hash",
            "semantic_grouping_key",
            "grouping_policy_version",
            "handle",
            "status",
            "payload",
            "opened_at_block",
            "opened_tx",
            "closed_at_block",
            "closed_tx",
            "last_reconciled_at_block",
            "matching_policy_version",
        }
    ),
    # VIB-4197 / T11 — per-(deployment_id, primitive, cutover_key) cutover
    # progress tracking.
    "migration_state": frozenset(
        {
            "deployment_id",
            "primitive",
            "cutover_key",
            "position_registry_backfill_complete",
            "backfill_started_at",
            "backfill_completed_at",
            "backfill_source_table",
            "backfill_reader_version",
            "rows_synthesized",
            "rows_skipped_already_present",
            "notes",
            "created_at",
            "updated_at",
        }
    ),
}


# VIB-4049 — hosted teardown bridge tables (Postgres-only).
#
# These three tables ride a different lifecycle from the accounting tables
# above: their DDL is owned by ``metrics-database`` PR #32 (Postgres), and on
# local SQLite they are created LAZILY by ``SQLiteTeardownStateManager.__init__``
# the first time a teardown request is written. The gateway's local boot
# path does NOT eagerly construct ``SQLiteTeardownStateManager`` (only
# ``SQLiteStore`` for accounting), so including these tables in the SQLite
# contract would crash a fresh local boot with ``SchemaContractViolation``
# every time before any teardown work has occurred.
#
# We therefore keep them out of ``ACCOUNTING_SCHEMA_CONTRACT`` and only
# require them at hosted Postgres boot — where the migration is the
# operative gate, and where lazy table creation is forbidden by
# ``CLAUDE.md`` "Database schema ownership". Hosted boot fails closed if
# metrics-database#32 hasn't been applied; local boot doesn't care because
# the teardown subsystem self-bootstraps.
#
# The ``teardown_*`` tables key on the canonical ``deployment_id`` (blueprint
# 29 §3). VIB-4721's metrics-database migration RENAMED the legacy hosted id
# column on these three tables to ``deployment_id``; the contract requires the
# post-migration name so the hosted boot-time schema validator matches the
# deployed schema.
TEARDOWN_SCHEMA_CONTRACT_POSTGRES: dict[str, frozenset[str]] = {
    "teardown_requests": frozenset(
        {
            "deployment_id",
            "mode",
            "asset_policy",
            "target_token",
            "reason",
            "requested_at",
            "requested_by",
            "status",
            "acknowledged_at",
            "started_at",
            "completed_at",
            "current_phase",
            "positions_total",
            "positions_closed",
            "positions_failed",
            "cancel_requested",
            "cancel_deadline",
            "error_message",
            "result_json",
            "updated_at",
        }
    ),
    "teardown_execution_state": frozenset(
        {
            "teardown_id",
            "deployment_id",
            "mode",
            "status",
            "total_intents",
            "completed_intents",
            "current_intent_index",
            "started_at",
            "updated_at",
            "completed_at",
            "pending_intents_json",
            "intent_results_json",
            "cancel_window_until",
            "config_json",
        }
    ),
    "teardown_approvals": frozenset(
        {
            "teardown_id",
            "level",
            "deployment_id",
            "request_json",
            "response_json",
            "created_at",
            "responded_at",
            "expires_at",
        }
    ),
}


class SchemaContractViolation(RuntimeError):
    """Backend schema is missing one or more columns the SDK requires.

    Raised at gateway boot by ``schema_validator``; never surfaced to a
    strategy iteration. The supervisor restart loop catches this in the
    boot phase and the operator sees a precise list of missing columns.
    """


def format_violations(backend_label: str, violations: dict[str, set[str]]) -> str:
    """Render a multi-line, deterministic violation report.

    Used by the validator and pinned in tests so error messages are stable
    enough for operators to grep.
    """
    lines = [f"{backend_label} schema is missing required accounting columns:"]
    for table in sorted(violations):
        for column in sorted(violations[table]):
            lines.append(f"  - {table}.{column}")
    return "\n".join(lines)
