"""VIB-5059 Phase 1 (SQL half) — equivalence of the targeted-SQL quant load.

The dashboard quant-input load used to fetch up to 100k FULL-WIDTH
``transaction_ledger`` rows (pre/post-state + price JSON blobs included) and
reduce them with per-row Python loops. The SQL half of Phase 1 pushes that
reduction into the stores (``LedgerQuantStats`` — COUNT/SUM aggregates + a
LIMIT-bounded first-action anchor walk). The tiles' VALUES must not move.

The reference implementation in this file reimplements the PREVIOUS per-row
Python aggregation verbatim (``_legacy_ledger_reduction`` — copied from the
pre-change ``compute_cost_stack`` / ``compute_audit_trail`` /
``evaluate_posture`` loops) and runs it over the same full-width row fetch
the old load performed. The suite then asserts the new SQL path produces
IDENTICAL stats and IDENTICAL tile values on the same seeded real-SQLite
store. UAT card: ``docs/internal/uat-cards/VIB-5059-p1sql.md`` (D1.S1,
D1.S2, D1.S4, D2.M1, D3.F1–F3, D3.F6).
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from almanak.framework.dashboard.quant_aggregations import (
    _detect_primitive,
    _wallet_value_at_first_action,
    compute_audit_trail,
    compute_cost_stack,
    compute_pnl_summary,
    compute_reconciliation,
    evaluate_posture,
    ledger_quant_stats_from_entries,
)
from almanak.framework.observability.ledger import LedgerQuantStats
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

_DEP = "deployment:vib5059sql01"

_T0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)


# =============================================================================
# Seeding helpers (raw SQL — full control over NULL vs '' vs value)
# =============================================================================


@dataclass
class _Row:
    """One transaction_ledger row spec; None columns stay SQL NULL."""

    ts: datetime
    tx_hash: str | None = "0xabc"
    cycle_id: str | None = "cycle-1"
    gas_usd: str | None = "0.10"
    price_inputs_json: str | None = None
    pre_state_json: str | None = None
    post_state_json: str | None = None
    success: int = 1
    intent_type: str = "SWAP"
    row_id: str | None = None


def _insert_ledger_row(store: SQLiteStore, row: _Row, deployment_id: str = _DEP) -> None:
    store._conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO transaction_ledger (
            id, cycle_id, deployment_id, execution_mode, timestamp,
            intent_type, gas_usd, tx_hash, success,
            price_inputs_json, pre_state_json, post_state_json
        ) VALUES (?, ?, ?, 'paper', ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row.row_id or str(uuid.uuid4()),
            # cycle_id column is NOT NULL in SQLite; None spec maps to ''
            # (the empty "parser didn't emit" sentinel the writer produces).
            row.cycle_id if row.cycle_id is not None else "",
            deployment_id,
            row.ts.isoformat(),
            row.intent_type,
            row.gas_usd,
            row.tx_hash,
            row.success,
            row.price_inputs_json,
            row.pre_state_json,
            row.post_state_json,
        ),
    )
    store._conn.commit()  # type: ignore[union-attr]


def _insert_snapshot(
    store: SQLiteStore,
    ts: datetime,
    total_value_usd: str,
    available_cash_usd: str,
    deployment_id: str = _DEP,
    positions_json: str = "[]",
    value_confidence: str = "HIGH",
) -> None:
    store._conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO portfolio_snapshots (
            deployment_id, timestamp, total_value_usd, available_cash_usd,
            value_confidence, positions_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (deployment_id, ts.isoformat(), total_value_usd, available_cash_usd, value_confidence, positions_json, ts.isoformat()),
    )
    store._conn.commit()  # type: ignore[union-attr]


def _insert_accounting_event(
    store: SQLiteStore,
    ts: datetime,
    event_type: str,
    payload: dict[str, Any],
    deployment_id: str = _DEP,
    confidence: str = "HIGH",
) -> None:
    store._conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO accounting_events (
            id, deployment_id, cycle_id, execution_mode, timestamp,
            chain, protocol, wallet_address, event_type, position_key,
            confidence, payload_json
        ) VALUES (?, ?, 'cycle-1', 'paper', ?, 'arbitrum', 'uniswap_v3', '0xwallet', ?, 'pos-1', ?, ?)
        """,
        (str(uuid.uuid4()), deployment_id, ts.isoformat(), event_type, confidence, json.dumps(payload)),
    )
    store._conn.commit()  # type: ignore[union-attr]


def _insert_portfolio_metrics(store: SQLiteStore, initial_value_usd: str, deployment_id: str = _DEP) -> None:
    store._conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO portfolio_metrics (
            deployment_id, initial_value_usd, initial_timestamp, updated_at
        ) VALUES (?, ?, ?, ?)
        """,
        (deployment_id, initial_value_usd, _T0.isoformat(), _T0.isoformat()),
    )
    store._conn.commit()  # type: ignore[union-attr]


def _anchor_pre_state(balances: dict[str, str]) -> str:
    return json.dumps({"wallet_balances": balances})


def _anchor_prices(prices: dict[str, str]) -> str:
    return json.dumps({tok: {"price_usd": p} for tok, p in prices.items()})


# =============================================================================
# Legacy reference — VERBATIM pre-change per-row reduction
# =============================================================================


def _legacy_to_decimal(value: Any, default: str = "0") -> Decimal:
    """Verbatim copy of quant_aggregations._to_decimal (legacy parse)."""
    if value is None or value == "":
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def _legacy_ledger_reduction(ledger_entries: list[Any]) -> LedgerQuantStats:
    """The pre-change per-row Python loops, verbatim.

    - gas sum: ``compute_cost_stack``'s ``stack.gas_usd += _to_decimal(gas)``
    - audit counts: ``compute_audit_trail``'s truthiness loop
    - all-rows checks: ``evaluate_posture``'s G1/G7 ``all(...)`` expressed as
      counts (``all(field)`` ⇔ ``count(field non-empty) == total``)
    - anchor: ``_wallet_value_at_first_action`` over the FULL row list (the
      production function, unchanged by this PR — it WAS the legacy walk)
    """
    gas_sum = Decimal("0")
    with_price_inputs = 0
    with_pre_post_state = 0
    with_gas = 0
    with_tx = 0
    with_cycle = 0
    for entry in ledger_entries:
        gas_usd = getattr(entry, "gas_usd", None)
        if gas_usd is None and isinstance(entry, dict):
            gas_usd = entry.get("gas_usd")
        gas_sum += _legacy_to_decimal(gas_usd)

        price_inputs = getattr(entry, "price_inputs_json", None)
        pre_state = getattr(entry, "pre_state_json", None)
        post_state = getattr(entry, "post_state_json", None)
        if price_inputs:
            with_price_inputs += 1
        if pre_state and post_state:
            with_pre_post_state += 1
        if gas_usd and _legacy_to_decimal(gas_usd) > Decimal("0"):
            with_gas += 1
        if getattr(entry, "tx_hash", "") or (isinstance(entry, dict) and entry.get("tx_hash")):
            with_tx += 1
        if getattr(entry, "cycle_id", "") or (isinstance(entry, dict) and entry.get("cycle_id")):
            with_cycle += 1

    return LedgerQuantStats(
        total=len(ledger_entries),
        with_tx_hash=with_tx,
        with_cycle_id=with_cycle,
        with_price_inputs=with_price_inputs,
        with_pre_post_state=with_pre_post_state,
        with_positive_gas_usd=with_gas,
        gas_usd_sum=gas_sum,
        first_action_wallet_value_usd=_wallet_value_at_first_action(ledger_entries),
    )


def _compute_all_tiles(
    *,
    portfolio_metrics: Any,
    snapshots: list[Any],
    ledger: Any,
    accounting_events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Run the full tile aggregation stack (PnL + cost + audit + posture)."""
    pnl = compute_pnl_summary(
        portfolio_metrics=portfolio_metrics,
        snapshots=snapshots,
        ledger_entries=ledger,
        accounting_events=accounting_events,
        position_summary=None,
    )
    cost = compute_cost_stack(ledger, accounting_events)
    audit = compute_audit_trail(ledger, accounting_events)
    recon = compute_reconciliation(
        initial_value_usd=pnl.deployed_usd,
        nav_usd=pnl.nav_usd,
        cost_stack=cost,
        accounting_events=accounting_events,
    )
    posture = evaluate_posture(
        primitive=_detect_primitive(accounting_events),
        ledger_entries=ledger,
        accounting_events=accounting_events,
        snapshots=snapshots,
        audit=audit,
        reconciliation=recon,
        portfolio_metrics=portfolio_metrics,
    )
    return {"pnl": pnl, "cost": cost, "audit": audit, "recon": recon, "posture": posture}


def _assert_tiles_equal(ref: dict[str, Any], new: dict[str, Any]) -> None:
    """Field-by-field exact equality across every tile dataclass."""
    for key in ("pnl", "cost", "audit", "recon", "posture"):
        r, n = ref[key], new[key]
        assert type(r) is type(n)
        for field_name in vars(r):
            rv, nv = getattr(r, field_name), getattr(n, field_name)
            assert rv == nv, f"{key}.{field_name}: legacy={rv!r} new={nv!r}"


# =============================================================================
# Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def store(tmp_path: Path) -> SQLiteStore:
    cfg = SQLiteConfig(db_path=str(tmp_path / "vib5059.db"))
    s = SQLiteStore(cfg)
    await s.initialize()
    yield s
    await s.close()


def _state_manager_over(store: SQLiteStore) -> StateManager:
    """Real StateManager delegating to the real SQLite warm store."""
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


def _seed_realistic_mix(store: SQLiteStore) -> None:
    """≥12 rows, ≥3 cycles, success+failed, NULL/''/value column mix.

    The FIRST chronological row deliberately carries an anchor that values
    to ZERO (all balances zero) so the anchor walk must SKIP it and resolve
    on the second anchor-bearing row — grab-first implementations fail.
    """
    rows = [
        # earliest: anchor-bearing but values to zero — must be skipped
        _Row(
            ts=_T0,
            cycle_id="cycle-1",
            gas_usd="0.10",
            pre_state_json=_anchor_pre_state({"USDC": "0"}),
            price_inputs_json=_anchor_prices({"USDC": "1.0"}),
            post_state_json='{"wallet_balances": {"USDC": "0"}}',
        ),
        # second: the REAL anchor (1000 USDC + 0.5 WETH @ $2000 = $2000 total)
        _Row(
            ts=_T0 + timedelta(minutes=1),
            cycle_id="cycle-1",
            gas_usd="0.20",
            pre_state_json=_anchor_pre_state({"USDC": "1000", "WETH": "0.5"}),
            price_inputs_json=_anchor_prices({"USDC": "1.0", "WETH": "2000"}),
            post_state_json='{"wallet_balances": {"USDC": "0", "WETH": "1.0"}}',
        ),
        # failed row, no tx_hash, NULL gas
        _Row(ts=_T0 + timedelta(minutes=2), tx_hash=None, gas_usd=None, success=0, cycle_id="cycle-2"),
        # empty-string gas (parser didn't emit), has tx
        _Row(ts=_T0 + timedelta(minutes=3), gas_usd="", cycle_id="cycle-2"),
        # zero gas (measured zero — truthy string, parses to 0)
        _Row(ts=_T0 + timedelta(minutes=4), gas_usd="0", cycle_id="cycle-2"),
        # plain successes across another cycle, exact-decimal gas values
        _Row(ts=_T0 + timedelta(minutes=5), gas_usd="0.1", cycle_id="cycle-3"),
        _Row(ts=_T0 + timedelta(minutes=6), gas_usd="0.2", cycle_id="cycle-3"),
        # missing cycle_id (empty)
        _Row(ts=_T0 + timedelta(minutes=7), cycle_id=None, gas_usd="0.05"),
        # price_inputs only (no pre/post) — counts price_inputs, not pre_post
        _Row(
            ts=_T0 + timedelta(minutes=8),
            price_inputs_json=_anchor_prices({"USDC": "1.0"}),
            gas_usd="0.01",
        ),
        # pre without post — must NOT count toward pre_post
        _Row(ts=_T0 + timedelta(minutes=9), pre_state_json='{"wallet_balances": {}}', gas_usd="0.02"),
        # unparsable gas text — legacy treats as 0, still counted in total
        _Row(ts=_T0 + timedelta(minutes=10), gas_usd="not-a-number"),
        # negative gas (refund-shaped) — sums, but not "positive gas" count
        _Row(ts=_T0 + timedelta(minutes=11), gas_usd="-0.03"),
        # LP-close style row
        _Row(ts=_T0 + timedelta(minutes=12), intent_type="LP_CLOSE", gas_usd="0.30"),
    ]
    for r in rows:
        _insert_ledger_row(store, r)

    _insert_snapshot(store, _T0 + timedelta(minutes=2), "2500", "400")
    _insert_snapshot(store, _T0 + timedelta(minutes=7), "2400", "450")
    _insert_snapshot(
        store,
        _T0 + timedelta(minutes=12),
        "2600",
        "500",
        positions_json=json.dumps([{"position_type": "LP", "in_range": True, "value_usd": "2600"}]),
    )
    _insert_portfolio_metrics(store, "3000")

    _insert_accounting_event(store, _T0 + timedelta(minutes=1), "SWAP", {
        "event_type": "SWAP",
        "slippage_usd": "0.50",
        "realized_pnl_usd": "1.25",
        "protocol_fee_usd": "0.10",
        "schema_version": "1",
        "formula_version": "1",
        "matching_policy_version": "1",
    })
    _insert_accounting_event(store, _T0 + timedelta(minutes=6), "LP_OPEN", {
        "event_type": "LP_OPEN",
        "cost_basis_usd": "2000",
        "position_key": "pos-1",
    })
    _insert_accounting_event(store, _T0 + timedelta(minutes=12), "LP_CLOSE", {
        "event_type": "LP_CLOSE",
        "fees_total_usd": "3.21",
        "realized_pnl_usd": "-0.75",
        "il_usd": "0.40",
        "position_key": "pos-1",
    })


async def _new_path_stats(store: SQLiteStore) -> LedgerQuantStats:
    """The NEW load path: SQL aggregates + the servicer's bounded anchor walk."""
    sm = _state_manager_over(store)
    svc = _servicer_over(sm)
    stats = await sm.get_ledger_quant_stats(_DEP)
    anchor = await svc._first_action_wallet_value(_DEP)
    if anchor is not None:
        import dataclasses

        stats = dataclasses.replace(stats, first_action_wallet_value_usd=anchor)
    return stats


async def _legacy_full_fetch(store: SQLiteStore, limit: int = 100_000) -> list[Any]:
    """The legacy load's row fetch (full-width, newest-first, capped)."""
    return await store.get_ledger_entries(_DEP, limit=limit)


# =============================================================================
# D1.S1 — stats + tile equivalence on the realistic mix
# =============================================================================


@pytest.mark.asyncio
async def test_equivalence_stats_match_legacy_reduction(store: SQLiteStore) -> None:
    _seed_realistic_mix(store)

    legacy = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    new = await _new_path_stats(store)

    assert new == legacy  # frozen dataclass — field-by-field equality
    # Pin the interesting fields explicitly so a regression names itself.
    assert new.total == 13
    assert new.gas_usd_sum == Decimal("0.95")  # exact decimal, no float artifacts
    assert new.with_positive_gas_usd == 8
    assert new.with_pre_post_state == 2
    assert new.with_price_inputs == 3
    assert new.with_tx_hash == 12
    assert new.with_cycle_id == 12
    assert new.first_action_wallet_value_usd == Decimal("2000.0")


@pytest.mark.asyncio
async def test_equivalence_every_tile_field_identical(store: SQLiteStore) -> None:
    _seed_realistic_mix(store)

    sm = _state_manager_over(store)
    snapshots = await sm.get_recent_snapshots(_DEP, limit=168)
    metrics = await sm.get_portfolio_metrics(_DEP)
    events = await sm.get_accounting_events_for_dashboard(deployment_id=_DEP)

    legacy_stats = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    new_stats = await _new_path_stats(store)

    ref = _compute_all_tiles(portfolio_metrics=metrics, snapshots=snapshots, ledger=legacy_stats, accounting_events=events)
    new = _compute_all_tiles(portfolio_metrics=metrics, snapshots=snapshots, ledger=new_stats, accounting_events=events)
    _assert_tiles_equal(ref, new)

    # The list path (legacy callers / regression suites) must agree too.
    full_rows = await _legacy_full_fetch(store)
    via_list = _compute_all_tiles(portfolio_metrics=metrics, snapshots=snapshots, ledger=full_rows, accounting_events=events)
    _assert_tiles_equal(ref, via_list)


# =============================================================================
# D1.S2 — the three tile RPCs serve the same values end-to-end
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_end_to_end_serves_reference_values(store: SQLiteStore) -> None:
    _seed_realistic_mix(store)

    sm = _state_manager_over(store)
    svc = _servicer_over(sm)

    snapshots = await sm.get_recent_snapshots(_DEP, limit=168)
    metrics = await sm.get_portfolio_metrics(_DEP)
    events = await sm.get_accounting_events_for_dashboard(deployment_id=_DEP)
    legacy_stats = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    ref = _compute_all_tiles(portfolio_metrics=metrics, snapshots=snapshots, ledger=legacy_stats, accounting_events=events)

    ctx = MagicMock()
    pnl = await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEP), ctx)
    cost = await svc.GetCostStack(gateway_pb2.GetCostStackRequest(deployment_id=_DEP), ctx)
    audit = await svc.GetAuditPosture(gateway_pb2.GetAuditPostureRequest(deployment_id=_DEP), ctx)

    assert pnl.deployed_usd == str(ref["pnl"].deployed_usd)
    assert pnl.nav_usd == str(ref["pnl"].nav_usd)
    assert pnl.lifetime_pnl_usd == str(ref["pnl"].lifetime_pnl_usd)
    assert pnl.value_confidence == ref["pnl"].value_confidence
    assert pnl.open_position_count == ref["pnl"].open_position_count

    assert cost.cost_gas_usd == str(ref["cost"].gas_usd)
    assert cost.cost_slippage_usd == str(ref["cost"].slippage_usd)
    assert cost.fees_earned_usd == str(ref["cost"].fees_earned_usd)
    assert cost.realized_pnl_usd == str(ref["cost"].realized_pnl_usd)

    assert audit.ledger_total == ref["audit"].ledger_total
    assert audit.ledger_with_price_inputs == ref["audit"].ledger_with_price_inputs
    assert audit.ledger_with_pre_post_state == ref["audit"].ledger_with_pre_post_state
    assert audit.ledger_with_gas_usd == ref["audit"].ledger_with_gas_usd
    assert audit.g6_sum_gas == str(ref["recon"].sum_gas)
    assert audit.cells_passed == ref["posture"].cells_passed
    assert audit.cells_failed == ref["posture"].cells_failed


# =============================================================================
# D1.S4 — beyond the legacy cap, the SQL path is the CORRECT one
# =============================================================================


@pytest.mark.asyncio
async def test_beyond_cap_sql_path_matches_ground_truth(store: SQLiteStore) -> None:
    """8 rows, stand-in cap of 5: the capped legacy reduction diverges from
    ground truth (totals AND first-action anchor); the SQL path equals it."""
    rows = [
        _Row(
            ts=_T0,
            gas_usd="1.00",
            pre_state_json=_anchor_pre_state({"USDC": "500"}),
            price_inputs_json=_anchor_prices({"USDC": "1.0"}),
        )
    ]
    rows += [_Row(ts=_T0 + timedelta(minutes=i), gas_usd="1.00") for i in range(1, 7)]
    rows.append(
        _Row(
            ts=_T0 + timedelta(minutes=7),
            gas_usd="1.00",
            pre_state_json=_anchor_pre_state({"USDC": "9999"}),
            price_inputs_json=_anchor_prices({"USDC": "1.0"}),
        )
    )
    for r in rows:
        _insert_ledger_row(store, r)

    ground_truth = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    capped_legacy = _legacy_ledger_reduction(await _legacy_full_fetch(store, limit=5))
    new = await _new_path_stats(store)

    # The legacy cap silently truncated (newest-first): wrong totals AND a
    # wrong anchor (it can only see the newest capped window, whose earliest
    # anchor-bearing row is the $9999 one, not the true first action's $500).
    assert capped_legacy.total == 5
    assert capped_legacy.gas_usd_sum == Decimal("5.00")
    assert capped_legacy.first_action_wallet_value_usd == Decimal("9999.0")
    assert capped_legacy != ground_truth

    assert ground_truth.total == 8
    assert ground_truth.gas_usd_sum == Decimal("8.00")
    assert ground_truth.first_action_wallet_value_usd == Decimal("500.0")
    assert new == ground_truth


# =============================================================================
# D2.M1 — data-shape sweep
# =============================================================================


@pytest.mark.asyncio
async def test_shape_all_null_optional_columns(store: SQLiteStore) -> None:
    for i in range(4):
        _insert_ledger_row(store, _Row(ts=_T0 + timedelta(minutes=i), tx_hash=None, cycle_id=None, gas_usd=None))
    legacy = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    new = await _new_path_stats(store)
    assert new == legacy
    assert new.total == 4
    assert new.with_tx_hash == 0
    assert new.with_cycle_id == 0
    assert new.gas_usd_sum == Decimal("0")
    assert new.first_action_wallet_value_usd is None


@pytest.mark.asyncio
async def test_shape_failed_rows_only(store: SQLiteStore) -> None:
    for i in range(3):
        _insert_ledger_row(store, _Row(ts=_T0 + timedelta(minutes=i), success=0, gas_usd="0.07"))
    legacy = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    new = await _new_path_stats(store)
    assert new == legacy
    assert new.gas_usd_sum == Decimal("0.21")
    assert new.with_positive_gas_usd == 3


@pytest.mark.asyncio
async def test_shape_anchor_on_first_row(store: SQLiteStore) -> None:
    _insert_ledger_row(
        store,
        _Row(
            ts=_T0,
            pre_state_json=_anchor_pre_state({"WETH": "2"}),
            price_inputs_json=_anchor_prices({"WETH": "1500"}),
        ),
    )
    _insert_ledger_row(store, _Row(ts=_T0 + timedelta(minutes=1)))
    legacy = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    new = await _new_path_stats(store)
    assert new == legacy
    assert new.first_action_wallet_value_usd == Decimal("3000")


@pytest.mark.asyncio
async def test_shape_anchor_only_on_later_row(store: SQLiteStore) -> None:
    _insert_ledger_row(store, _Row(ts=_T0))
    _insert_ledger_row(store, _Row(ts=_T0 + timedelta(minutes=1)))
    _insert_ledger_row(
        store,
        _Row(
            ts=_T0 + timedelta(minutes=2),
            pre_state_json=_anchor_pre_state({"USDC": "42"}),
            price_inputs_json=_anchor_prices({"USDC": "1.0"}),
        ),
    )
    legacy = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    new = await _new_path_stats(store)
    assert new == legacy
    assert new.first_action_wallet_value_usd == Decimal("42.0")


@pytest.mark.asyncio
async def test_shape_anchor_identical_timestamp_tiebreak_is_lower_id(store: SQLiteStore) -> None:
    """Two anchor rows at the SAME timestamp: the walk inspects lower-id first.

    Intentional divergence pin (pr-auditor item 3 on PR #2739): the SQL walk
    orders ``timestamp ASC, id ASC`` so the lower-id row (truly written first)
    anchors; the legacy in-memory stable sort over a DESC fetch inspected the
    higher-id row first. ``id ASC`` is the more correct semantics — this test
    pins it so the choice is deliberate, not accidental.
    """
    ts = _T0
    _insert_ledger_row(
        store,
        _Row(
            ts=ts,
            row_id="00000000-0000-0000-0000-00000000000a",
            pre_state_json=_anchor_pre_state({"USDC": "100"}),
            price_inputs_json=_anchor_prices({"USDC": "1.0"}),
        ),
    )
    _insert_ledger_row(
        store,
        _Row(
            ts=ts,
            row_id="ffffffff-ffff-ffff-ffff-ffffffffffff",
            pre_state_json=_anchor_pre_state({"USDC": "200"}),
            price_inputs_json=_anchor_prices({"USDC": "1.0"}),
        ),
    )
    new = await _new_path_stats(store)
    assert new.first_action_wallet_value_usd == Decimal("100.0")


@pytest.mark.asyncio
async def test_shape_anchor_on_no_row_falls_back_to_metrics(store: SQLiteStore) -> None:
    """No anchor anywhere → deployed falls back to portfolio_metrics, as before."""
    _insert_ledger_row(store, _Row(ts=_T0))
    _insert_portfolio_metrics(store, "1234")
    _insert_snapshot(store, _T0, "1000", "200")

    sm = _state_manager_over(store)
    svc = _servicer_over(sm)
    new = await _new_path_stats(store)
    assert new.first_action_wallet_value_usd is None

    pnl = await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEP), MagicMock())
    assert pnl.deployed_usd == "1234"


@pytest.mark.asyncio
async def test_shape_exact_decimal_addition(store: SQLiteStore) -> None:
    """0.1 + 0.2 must surface as 0.3 — never a float-artifact string."""
    _insert_ledger_row(store, _Row(ts=_T0, gas_usd="0.1"))
    _insert_ledger_row(store, _Row(ts=_T0 + timedelta(minutes=1), gas_usd="0.2"))
    new = await _new_path_stats(store)
    assert str(new.gas_usd_sum) == "0.3"
    legacy = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    assert new == legacy


# =============================================================================
# D3.F1 — backend unreachable: degrade, never fabricate; read-only proof
# =============================================================================


class _FailingSM:
    """Every read raises — the hosted 'database briefly unreachable' shape."""

    def __getattr__(self, name: str) -> Any:
        async def _fail(*a: Any, **k: Any) -> Any:
            raise RuntimeError("backend unavailable")

        return _fail


@pytest.mark.asyncio
async def test_degraded_backend_burst_resolves_unmeasured(
    store: SQLiteStore, caplog: pytest.LogCaptureFixture
) -> None:
    _seed_realistic_mix(store)

    def _row_counts() -> tuple[int, int]:
        c = store._conn  # type: ignore[union-attr]
        ledger = c.execute("SELECT COUNT(*) AS n FROM transaction_ledger").fetchone()["n"]
        snaps = c.execute("SELECT COUNT(*) AS n FROM portfolio_snapshots").fetchone()["n"]
        return ledger, snaps

    before = _row_counts()

    svc = _servicer_over(_FailingSM())
    with caplog.at_level("DEBUG", logger="almanak.gateway.services.dashboard_service"):
        pnl, cost, audit = await asyncio.gather(
            svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id="dep-degraded"), MagicMock()),
            svc.GetCostStack(gateway_pb2.GetCostStackRequest(deployment_id="dep-degraded"), MagicMock()),
            svc.GetAuditPosture(gateway_pb2.GetAuditPostureRequest(deployment_id="dep-degraded"), MagicMock()),
        )

    assert pnl.value_confidence == "UNAVAILABLE"
    assert cost.inventory_unrealized_usd == ""  # unmeasured sentinel, not "0"
    assert audit.g6_status == "NA"  # never a fabricated PASS
    # The documented degraded-load log lines fired (per-fetch failure logger).
    assert any("get_ledger_quant_stats failed" in r.message for r in caplog.records)

    assert _row_counts() == before  # read path performed no half-write


# =============================================================================
# D3.F2 — zero-row deployment renders today's exact empty-state tiles
# =============================================================================


@pytest.mark.asyncio
async def test_zero_row_deployment_empty_state_tiles(store: SQLiteStore) -> None:
    sm = _state_manager_over(store)
    svc = _servicer_over(sm)
    ctx = MagicMock()

    pnl = await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEP), ctx)
    cost = await svc.GetCostStack(gateway_pb2.GetCostStackRequest(deployment_id=_DEP), ctx)
    audit = await svc.GetAuditPosture(gateway_pb2.GetAuditPostureRequest(deployment_id=_DEP), ctx)

    # Pinned legacy empty-state values (not just "anything non-crashing").
    assert pnl.value_confidence == "UNAVAILABLE"
    assert pnl.deployed_usd == "0"
    assert pnl.nav_usd == "0"
    assert pnl.lifetime_pnl_usd == "0"
    assert pnl.open_position_count == 0
    assert pnl.primary_risk_label == "No active positions"

    assert cost.cost_gas_usd == "0"
    assert cost.inventory_unrealized_usd == ""

    assert audit.ledger_total == 0
    assert audit.events_total == 0
    assert audit.g6_status == "NA"


# =============================================================================
# D3.F3 — anchor walk failure: metrics fallback engages, loudly
# =============================================================================


class _AnchorFailingSM:
    """Real reads except the anchor candidates lookup, which raises."""

    def __init__(self, sm: StateManager) -> None:
        self._sm = sm

    def __getattr__(self, name: str) -> Any:
        if name == "get_ledger_anchor_candidates":

            async def _fail(*a: Any, **k: Any) -> Any:
                raise RuntimeError("anchor lookup unavailable")

            return _fail
        return getattr(self._sm, name)


@pytest.mark.asyncio
async def test_anchor_failure_falls_back_to_metrics(
    store: SQLiteStore, caplog: pytest.LogCaptureFixture
) -> None:
    _seed_realistic_mix(store)  # ledger HAS a $2000 wallet anchor on disk
    sm = _state_manager_over(store)
    svc = _servicer_over(_AnchorFailingSM(sm))

    # NOTE: StateManager.get_ledger_anchor_candidates itself degrades to []
    # on backend errors; this injects the failure ABOVE that layer to prove
    # the servicer's own guard also degrades (defense in depth) — and that
    # the degrade is LOUD, not silent.
    with caplog.at_level("DEBUG", logger="almanak.gateway.services.dashboard_service"):
        pnl = await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEP), MagicMock())

    # Documented fallback: portfolio_metrics initial_value_usd ($3000 seeded),
    # NOT a fabricated wallet-anchored number and NOT a zero.
    assert pnl.deployed_usd == "3000"
    assert any("first-action anchor walk failed" in r.message for r in caplog.records)


# =============================================================================
# D3.F6 — garbage / non-finite numeric text cannot crash or skew tiles
# =============================================================================


@pytest.mark.asyncio
async def test_garbage_gas_text_matches_legacy_zero_contribution(store: SQLiteStore) -> None:
    _insert_ledger_row(store, _Row(ts=_T0, gas_usd="0.50"))
    _insert_ledger_row(store, _Row(ts=_T0 + timedelta(minutes=1), gas_usd="garbage!!"))
    _insert_ledger_row(store, _Row(ts=_T0 + timedelta(minutes=2), gas_usd="0.25"))

    legacy = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    new = await _new_path_stats(store)

    assert new == legacy
    assert new.total == 3  # the garbage row is COUNTED, its gas contributes 0
    assert new.gas_usd_sum == Decimal("0.75")
    assert new.with_positive_gas_usd == 2


@pytest.mark.asyncio
async def test_garbage_gas_nan_stays_finite(store: SQLiteStore) -> None:
    """The one documented divergence: NaN gas no longer poisons the LTD sum.

    Legacy ``_to_decimal("NaN")`` returned ``Decimal("NaN")``, which turned
    the lifetime gas total into ``NaN`` (and crashed the audit count's
    ``> 0`` comparison). The new path treats non-finite text as contributing
    zero — pinned here on the real store; the Postgres twin guard is pinned
    in test_postgres_store_readers.py.
    """
    _insert_ledger_row(store, _Row(ts=_T0, gas_usd="0.40"))
    _insert_ledger_row(store, _Row(ts=_T0 + timedelta(minutes=1), gas_usd="NaN"))

    new = await _new_path_stats(store)
    assert new.gas_usd_sum.is_finite()
    assert new.gas_usd_sum == Decimal("0.40")
    assert new.with_positive_gas_usd == 1
    assert new.total == 2

    # And the tile pipeline stays evaluable end-to-end.
    sm = _state_manager_over(store)
    svc = _servicer_over(sm)
    cost = await svc.GetCostStack(gateway_pb2.GetCostStackRequest(deployment_id=_DEP), MagicMock())
    assert cost.cost_gas_usd == "0.40"


# =============================================================================
# Anchor walk pagination — bounded multi-batch behavior
# =============================================================================


@pytest.mark.asyncio
async def test_anchor_walk_pages_past_a_batch_of_valueless_candidates(store: SQLiteStore) -> None:
    """70 zero-valued anchor candidates precede the real one: the walk must
    page past the first LIMIT-bounded batch (64) and still resolve, exactly
    like the legacy full-list walk."""
    for i in range(70):
        _insert_ledger_row(
            store,
            _Row(
                ts=_T0 + timedelta(minutes=i),
                pre_state_json=_anchor_pre_state({"USDC": "0"}),
                price_inputs_json=_anchor_prices({"USDC": "1.0"}),
            ),
        )
    _insert_ledger_row(
        store,
        _Row(
            ts=_T0 + timedelta(minutes=70),
            pre_state_json=_anchor_pre_state({"USDC": "77"}),
            price_inputs_json=_anchor_prices({"USDC": "1.0"}),
        ),
    )

    legacy = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    new = await _new_path_stats(store)
    assert new == legacy
    assert new.first_action_wallet_value_usd == Decimal("77.0")


@pytest.mark.asyncio
async def test_anchor_walk_exhausts_valueless_candidates_to_none(store: SQLiteStore) -> None:
    """Every candidate values to zero → None (metrics fallback), as before."""
    for i in range(66):  # spans two batches, then exhausts
        _insert_ledger_row(
            store,
            _Row(
                ts=_T0 + timedelta(minutes=i),
                pre_state_json=_anchor_pre_state({"USDC": "0"}),
                price_inputs_json=_anchor_prices({"USDC": "1.0"}),
            ),
        )

    legacy = _legacy_ledger_reduction(await _legacy_full_fetch(store))
    new = await _new_path_stats(store)
    assert new == legacy
    assert new.first_action_wallet_value_usd is None
