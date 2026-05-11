"""Schema contract for accounting tables — VIB-3763, plan §D.

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
# Identity column convention (CodeRabbit + Codex on PR #2162):
#   - Local SQLite tables key on ``strategy_id`` (the SDK's wire-side
#     name; this is what every writer in ``backends/sqlite.py`` writes).
#   - Hosted Postgres tables key on ``agent_id`` (the deployed
#     metrics-database name; the gateway maps wire-side ``strategy_id``
#     → ``agent_id`` via ``resolve_agent_id`` before each PG INSERT /
#     SELECT — see ``state_service.py:610`` and the read sites in
#     ``state_manager.py:721/752/782``).
# We therefore expose two contract dicts and two name maps so the
# validator can introspect the right column names per backend. The
# legacy ``ACCOUNTING_SCHEMA_CONTRACT`` alias points at the SQLite
# variant for backwards compatibility with existing imports.
ACCOUNTING_SCHEMA_CONTRACT_SQLITE: dict[str, frozenset[str]] = {
    "portfolio_snapshots": frozenset(
        {
            "id",
            "strategy_id",
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
            "strategy_id",
            "initial_value_usd",
            "initial_timestamp",
            "deposits_usd",
            "withdrawals_usd",
            "gas_spent_usd",
            "total_value_usd",
            "positions_json",
            "cycle_id",
            "deployment_id",
            "execution_mode",
            "is_complete",
            "updated_at",
        }
    ),
    "transaction_ledger": frozenset(
        {
            "id",
            "cycle_id",
            "strategy_id",
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
            "strategy_id",
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
            # accounting_events and the position_registry (T11). Required
            # on BOTH backends as of T19 (VIB-4205) — the metrics-database
            # migration (VIB-4191) lands the column on hosted Postgres.
            "position_reference",
        }
    ),
    "accounting_outbox": frozenset(
        {
            "id",
            "deployment_id",
            "strategy_id",
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
    # blueprint 28 §3 reference. Adding the contract entry was deferred
    # from T05 (schema-only PR) per blueprint 28 §5.1; T11 adds it
    # atomically with the writer code so the LOCAL SQLite boot guard refuses
    # to start when the column shape is missing (VIB-3763 pattern).
    #
    # NOT in the hosted Postgres contract until T19 (VIB-4205) lands the
    # Postgres writer + corresponding ``metrics-database`` migration —
    # auto-flow from ``_SQLITE`` to ``_POSTGRES`` is suppressed via
    # ``_POSTGRES_DEFERRED_TABLES`` below. Including it earlier would block
    # hosted gateway boot for a table the hosted runtime can't use.
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
    # progress tracking. Schema lives in SCHEMA_SQL; per the cutover spec
    # §2.1 the SDK adds the contract entry with the writer (here) but the
    # boot-time guard / BackfillReader runner-side wiring lands in a
    # follow-up ticket. Hosted Postgres ships via metrics-database per
    # AGENTS.md "Database schema ownership"; the SDK does NOT apply
    # Postgres DDL.
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


# Postgres variant: same table set, ``strategy_id`` → ``agent_id`` per
# the deployed metrics-database schema. The transformation is
# table-by-table because some accounting tables (``accounting_events``,
# ``accounting_outbox``) historically carry both ``deployment_id`` AND
# the strategy/agent key.
def _swap_strategy_for_agent(cols: frozenset[str]) -> frozenset[str]:
    return frozenset({"agent_id" if c == "strategy_id" else c for c in cols})


# Tables introduced by VIB-4197 / T11 (``position_registry``) and the cutover
# infrastructure (``migration_state``). T19 (VIB-4205) shipped the Postgres
# writer paths in ``almanak/gateway/services/state_service.py``; the deployed
# metrics-database schema is owned outside this repo (VIB-4191). Once VIB-4191
# lands on production metrics-database, hosted gateway boot will validate-only
# both tables here — adding either name to this set would re-disable that
# fail-loud guard for new column drift.
#
# Empty by design as of T19. Future additions belong here ONLY when a new
# table lands on SQLite ahead of a still-pending metrics-database migration
# (the same forward-compat pattern this set has always served).
_POSTGRES_DEFERRED_TABLES: frozenset[str] = frozenset()

# Per-table column-level Postgres deferrals. Mirrors
# ``_POSTGRES_DEFERRED_TABLES`` for additions that introduce a new column
# on an EXISTING table — the SDK can land the column on local SQLite and
# require it via the SQLite contract while the corresponding metrics-database
# migration is still in flight. The Postgres contract construction below
# subtracts these per-table columns from the SQLite contract before swapping
# ``strategy_id`` → ``agent_id``.
#
# Empty by design as of T19 (VIB-4205): ``accounting_events.position_reference``
# (VIB-4196 / T10) is now required on hosted Postgres too. Re-adding an entry
# here would silently re-disable the fail-loud boot guard for that column.
_POSTGRES_DEFERRED_COLUMNS: dict[str, frozenset[str]] = {}


def _postgres_columns_for(table: str, sqlite_cols: frozenset[str]) -> frozenset[str]:
    """Derive the Postgres column set for ``table`` from its SQLite columns.

    Applies (1) per-table column deferrals from ``_POSTGRES_DEFERRED_COLUMNS``
    so columns the metrics-database migration has not landed yet are NOT
    required at hosted boot, and (2) the ``strategy_id`` → ``agent_id`` rename.
    """
    deferred = _POSTGRES_DEFERRED_COLUMNS.get(table, frozenset())
    return _swap_strategy_for_agent(sqlite_cols - deferred)


ACCOUNTING_SCHEMA_CONTRACT_POSTGRES: dict[str, frozenset[str]] = {
    table: _postgres_columns_for(table, cols)
    for table, cols in ACCOUNTING_SCHEMA_CONTRACT_SQLITE.items()
    if table not in _POSTGRES_DEFERRED_TABLES
}


# Backwards-compatibility alias. Existing call sites that import
# ``ACCOUNTING_SCHEMA_CONTRACT`` get the SQLite-shaped contract — the
# pre-split behaviour. The Postgres validator now imports the explicit
# ``_POSTGRES`` variant.
ACCOUNTING_SCHEMA_CONTRACT = ACCOUNTING_SCHEMA_CONTRACT_SQLITE


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
