"""VIB-5059 Phase 1 (SQL half) — the quant-input load's SQL shape is bounded.

Instruments the REAL SQLite connection with a statement trace during one full
``_load_quant_inputs`` and asserts the contract from the UAT card (D1.S3):

(a) NO executed statement reading ``transaction_ledger`` selects the
    ``extracted_data_json`` / ``post_state_json`` blob columns;
(b) every executed statement reading ``transaction_ledger`` is either an
    aggregate (COUNT/SUM — O(1) rows, the exact-decimal gas total included)
    or carries an explicit row LIMIT (the bounded first-action anchor lookup,
    the only statement allowed to touch ``pre_state_json`` /
    ``price_inputs_json``);
(c) the legacy bulk full-width ledger fetch (``get_ledger_entries``) is NEVER
    invoked by the quant-input load.

Plus the render-burst contract recount: three tile RPCs → ONE aggregate-stats
fetch (the PR #2731 cache contract, now counted on the new load's queries —
see test_dashboard_quant_inputs_cache.py for the full cache matrix).
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

_DEP = "deployment:vib5059shape"
_T0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)


@pytest_asyncio.fixture
async def store(tmp_path: Path) -> SQLiteStore:
    cfg = SQLiteConfig(db_path=str(tmp_path / "vib5059-shape.db"))
    s = SQLiteStore(cfg)
    await s.initialize()
    yield s
    await s.close()


def _seed(store: SQLiteStore) -> None:
    for i in range(6):
        store._conn.execute(  # type: ignore[union-attr]
            """
            INSERT INTO transaction_ledger (
                id, cycle_id, deployment_id, execution_mode, timestamp,
                intent_type, gas_usd, tx_hash, success,
                price_inputs_json, pre_state_json, post_state_json
            ) VALUES (?, 'cycle-1', ?, 'paper', ?, 'SWAP', '0.10', '0xabc', 1, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                _DEP,
                (_T0 + timedelta(minutes=i)).isoformat(),
                json.dumps({"USDC": {"price_usd": "1.0"}}),
                json.dumps({"wallet_balances": {"USDC": "100"}}),
                json.dumps({"wallet_balances": {"USDC": "90"}}),
            ),
        )
    store._conn.commit()  # type: ignore[union-attr]


def _state_manager_over(store: SQLiteStore) -> StateManager:
    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._warm = store
    sm._record_metrics = MagicMock()
    return sm


def _servicer_over(sm: Any) -> DashboardServiceServicer:
    svc = DashboardServiceServicer.__new__(DashboardServiceServicer)
    svc.settings = SimpleNamespace()
    svc._state_manager = sm
    svc._initialized = True
    svc._strategies_root = None
    svc._cached_positions = {}
    return svc


def _ledger_statements(statements: list[str]) -> list[str]:
    return [
        s
        for s in statements
        if "transaction_ledger" in s.lower() and s.lstrip().upper().startswith("SELECT")
    ]


def _is_aggregate(sql: str) -> bool:
    head = sql.lower().split("from", 1)[0]
    return "count(" in head or "almanak_decimal_sum(" in head or "sum(" in head


@pytest.mark.asyncio
async def test_quant_load_sql_shape_bounded_and_blob_free(store: SQLiteStore) -> None:
    _seed(store)
    sm = _state_manager_over(store)
    svc = _servicer_over(sm)

    statements: list[str] = []
    store._conn.set_trace_callback(statements.append)  # type: ignore[union-attr]
    try:
        inputs = await svc._load_quant_inputs(_DEP)
    finally:
        store._conn.set_trace_callback(None)  # type: ignore[union-attr]

    # Sanity: the load actually computed real stats from the seeded rows.
    ledger_stats = inputs[2]
    assert ledger_stats.total == 6
    assert ledger_stats.first_action_wallet_value_usd is not None

    ledger_selects = _ledger_statements(statements)
    assert ledger_selects, "expected the load to query transaction_ledger"

    for sql in ledger_selects:
        low = sql.lower()
        assert "select *" not in low, f"full-width select: {sql}"

        # (b) aggregate (O(1) scalar rows by construction — blob columns may
        # appear inside COUNT(CASE ...) PRESENCE predicates, but no column
        # VALUE is ever transferred) OR an explicit LIMIT bound.
        if _is_aggregate(sql):
            continue
        # (a) per-row statements: the blob columns the aggregation never
        # needs are NEVER selected.
        selected = low.split("from", 1)[0]
        assert "extracted_data_json" not in selected, f"blob column selected: {sql}"
        assert "post_state_json" not in selected, f"blob column selected: {sql}"
        assert re.search(r"\blimit\b", low), f"unbounded per-row ledger read: {sql}"
        # Only the anchor lookup may read the anchor JSON columns, and only
        # with that LIMIT in place.
        assert "pre_state_json" in selected and "price_inputs_json" in selected, (
            f"non-anchor per-row ledger read in the quant load: {sql}"
        )


@pytest.mark.asyncio
async def test_quant_load_never_invokes_bulk_ledger_fetch(store: SQLiteStore) -> None:
    _seed(store)
    sm = _state_manager_over(store)

    calls: list[str] = []
    original = sm.get_ledger_entries

    async def _spy(*a: Any, **k: Any) -> Any:
        calls.append("get_ledger_entries")
        return await original(*a, **k)

    sm.get_ledger_entries = _spy  # type: ignore[method-assign]
    svc = _servicer_over(sm)

    await svc._load_quant_inputs(_DEP)

    assert calls == [], "the quant-input load must not invoke the bulk full-width ledger fetch"


@pytest.mark.asyncio
async def test_render_burst_one_aggregate_fetch(store: SQLiteStore) -> None:
    _seed(store)
    sm = _state_manager_over(store)

    counts = {"stats": 0, "anchor_batches": 0}
    orig_stats = sm.get_ledger_quant_stats
    orig_anchor = sm.get_ledger_anchor_candidates

    async def _stats_spy(*a: Any, **k: Any) -> Any:
        counts["stats"] += 1
        return await orig_stats(*a, **k)

    async def _anchor_spy(*a: Any, **k: Any) -> Any:
        counts["anchor_batches"] += 1
        return await orig_anchor(*a, **k)

    sm.get_ledger_quant_stats = _stats_spy  # type: ignore[method-assign]
    sm.get_ledger_anchor_candidates = _anchor_spy  # type: ignore[method-assign]
    svc = _servicer_over(sm)

    ctx = MagicMock()
    await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEP), ctx)
    await svc.GetCostStack(gateway_pb2.GetCostStackRequest(deployment_id=_DEP), ctx)
    await svc.GetAuditPosture(gateway_pb2.GetAuditPostureRequest(deployment_id=_DEP), ctx)

    assert counts["stats"] == 1, "three tile RPCs in a render burst must share ONE aggregate fetch"
    # The anchor resolved on the FIRST batch (the normal case) — the walk is
    # bounded and does not page when the earliest candidate already values.
    assert counts["anchor_batches"] == 1
