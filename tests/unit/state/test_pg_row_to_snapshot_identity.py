"""Unit coverage for the hosted/Postgres snapshot identity mapping.

CodeRabbit (PR #2162) flagged that ``_pg_row_to_portfolio_snapshot`` is
the only place hosted-Postgres rows are converted into the SDK's
``PortfolioSnapshot`` shape, and that the QA scripts in this PR are
SQLite-only. A missed mapping for ``deployment_id`` / ``cycle_id`` /
``execution_mode`` would silently leave the hosted dashboard reading
empty identity even though the gateway persisted it.

The function is a pure free function over a row-shaped object that
supports both ``row["k"]`` and ``row.get("k")`` — a dict is the smallest
faithful test double.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from almanak.framework.portfolio.models import ValueConfidence
from almanak.framework.state.state_manager import _pg_row_to_portfolio_snapshot


def _row(**overrides):
    """Return a Postgres-shaped row dict with sensible defaults."""
    base = {
        "agent_id": "Strat:abc",
        "timestamp": datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC),
        "iteration_number": 7,
        "total_value_usd": "100.50",
        "available_cash_usd": "10.25",
        "deployed_capital_usd": "90.25",
        "wallet_total_value_usd": "10.25",
        "value_confidence": "HIGH",
        "positions_text": "[]",
        "token_prices_text": "{}",
        "wallet_balances_text": "[]",
        "chain": "arbitrum",
        "deployment_id": "Strat:abc",
        "cycle_id": "cycle-pg-001",
        "execution_mode": "live",
    }
    base.update(overrides)
    return base


def test_pg_row_round_trips_identity_fields() -> None:
    """All three identity fields land on the snapshot exactly as written."""
    row = _row()
    snap = _pg_row_to_portfolio_snapshot(row)
    assert snap.deployment_id == "Strat:abc"
    assert snap.cycle_id == "cycle-pg-001"
    assert snap.execution_mode == "live"


def test_pg_row_strategy_id_uses_agent_id_column() -> None:
    """``snapshot.strategy_id`` is sourced from the ``agent_id`` column
    (hosted convention per ``state_manager.py:702-710``)."""
    row = _row(agent_id="hosted-agent-xyz")
    snap = _pg_row_to_portfolio_snapshot(row)
    assert snap.strategy_id == "hosted-agent-xyz"


def test_pg_row_legacy_missing_identity_columns_default_to_empty_string() -> None:
    """Pre-VIB-4095 rows that don't have the identity columns still load.

    The reader uses ``row.get(...) or ""`` defensively so an existing
    legacy row does not blow up when the SDK is upgraded against an
    older Postgres schema (the validator catches the schema gap
    separately at boot — VIB-3763)."""

    class _RowMissingIdentity(dict):
        # Force ``row.get(missing)`` to return ``None`` (default), but
        # still allow ``row["agent_id"]`` etc. for required columns.
        pass

    row = _row()
    for k in ("deployment_id", "cycle_id", "execution_mode"):
        row.pop(k)
    snap = _pg_row_to_portfolio_snapshot(_RowMissingIdentity(row))
    assert snap.deployment_id == ""
    assert snap.cycle_id == ""
    assert snap.execution_mode == ""


def test_pg_row_empty_identity_columns_remain_empty_strings() -> None:
    """Columns present-but-empty must stay ``""`` (not get filled with a
    fallback like ``strategy_id``). The validator's intent is that an
    empty cell is observable."""
    row = _row(deployment_id="", cycle_id="", execution_mode="")
    snap = _pg_row_to_portfolio_snapshot(row)
    assert snap.deployment_id == ""
    assert snap.cycle_id == ""
    assert snap.execution_mode == ""


def test_pg_row_value_confidence_round_trips() -> None:
    """Sanity: confidence (the field Gemini erroneously claimed was missing
    from this mapper) round-trips correctly for every enum value."""
    for level in [
        ValueConfidence.HIGH,
        ValueConfidence.ESTIMATED,
        ValueConfidence.STALE,
        ValueConfidence.UNAVAILABLE,
    ]:
        row = _row(value_confidence=level.value)
        snap = _pg_row_to_portfolio_snapshot(row)
        assert snap.value_confidence == level


def test_pg_row_positions_envelope_unpacks_metadata() -> None:
    """Identity round-trip is independent of how positions_json is shaped
    (legacy bare list vs VIB-3923 envelope)."""
    envelope = json.dumps(
        {
            "schema_version": 1,
            "positions": [],
            "metadata": {"reconciliation_decomposition_present": True},
            "reconciliation": {},
        }
    )
    row = _row(positions_text=envelope)
    snap = _pg_row_to_portfolio_snapshot(row)
    assert snap.deployment_id == "Strat:abc"
    assert snap.cycle_id == "cycle-pg-001"
    assert snap.execution_mode == "live"
    assert snap.snapshot_metadata == {"reconciliation_decomposition_present": True}


@pytest.mark.parametrize(
    "broken_value",
    ["not-json{", "[", "{ truncated", ""],
)
def test_pg_row_broken_positions_text_does_not_drop_identity(broken_value) -> None:
    """A malformed ``positions_text`` falls back to an empty positions list,
    but identity must still be carried through — the dashboard / debugger
    must be able to identify an otherwise-broken row."""
    row = _row(positions_text=broken_value)
    snap = _pg_row_to_portfolio_snapshot(row)
    assert snap.positions == []
    assert snap.deployment_id == "Strat:abc"
    assert snap.cycle_id == "cycle-pg-001"
    assert snap.execution_mode == "live"
