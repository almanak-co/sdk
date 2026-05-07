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
}


# Postgres variant: same table set, ``strategy_id`` → ``agent_id`` per
# the deployed metrics-database schema. The transformation is
# table-by-table because some accounting tables (``accounting_events``,
# ``accounting_outbox``) historically carry both ``deployment_id`` AND
# the strategy/agent key.
def _swap_strategy_for_agent(cols: frozenset[str]) -> frozenset[str]:
    return frozenset({"agent_id" if c == "strategy_id" else c for c in cols})


ACCOUNTING_SCHEMA_CONTRACT_POSTGRES: dict[str, frozenset[str]] = {
    table: _swap_strategy_for_agent(cols) for table, cols in ACCOUNTING_SCHEMA_CONTRACT_SQLITE.items()
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
