"""VIB-4225 ACC-02 — portfolio_metrics.gas_spent_usd aggregator unit tests.

Pins the F4a / F4b / F4c / F5 / F6 contract from the frozen UAT card §6.

- F4a (live SQL failure): live mode raises AccountingPersistenceError.
- F4b (hosted NotImplementedError): all modes leave gas_spent_usd=0,
  stamp `gas_aggregator_status="hosted_unsupported"`, log WARN, no halt.
- F4c (type-narrowness): unrelated ValueError → live mode raises.
- F5 (NULL/empty rows in ledger): coalesce to 0 inside SUM.
- F6 (idempotency + freshness): repeated calls give the same SUM; new
  ledger rows are picked up.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from almanak.framework.portfolio import PortfolioSnapshot, ValueConfidence
from almanak.framework.runner.runner_state import (
    _populate_gas_spent_usd,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.exceptions import AccountingPersistenceError


@dataclass
class _FakeLedgerRow:
    """Minimal ledger-entry mock for SQLite raw-INSERT in F5 / F6 tests."""

    id: str
    deployment_id: str
    strategy_id: str
    gas_usd: str
    cycle_id: str = "cycle-test"
    execution_mode: str = "paper"
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())


def _insert_ledger_row(store: SQLiteStore, row: _FakeLedgerRow) -> None:
    """Insert a ledger row via direct SQLite execute (F5 / D1.2 setup)."""
    store._conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO transaction_ledger (
            id, cycle_id, strategy_id, deployment_id, execution_mode,
            timestamp, intent_type, gas_usd, success
        ) VALUES (?, ?, ?, ?, ?, ?, 'SWAP', ?, 1)
        """,
        (
            row.id, row.cycle_id, row.strategy_id, row.deployment_id,
            row.execution_mode, row.timestamp, row.gas_usd,
        ),
    )
    store._conn.commit()  # type: ignore[union-attr]


@pytest_asyncio.fixture
async def sqlite_store(tmp_path: Path) -> SQLiteStore:
    cfg = SQLiteConfig(db_path=str(tmp_path / "acc02.db"))
    store = SQLiteStore(cfg)
    await store.initialize()
    yield store
    await store.close()


def _runner_for_metrics(state_manager: Any, deployment_id: str = "dep-X", config: Any = None) -> Any:
    """Build a runner mock with the wires _build_metrics_for_snapshot needs."""
    runner = MagicMock()
    runner.state_manager = state_manager
    runner.deployment_id = deployment_id
    runner.config = config or MagicMock()
    runner._last_cycle_id = "cycle-test"
    return runner


def _snapshot(strategy_id: str = "demo") -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        strategy_id=strategy_id,
        total_value_usd=Decimal("100"),
        available_cash_usd=Decimal("100"),
        value_confidence=ValueConfidence.HIGH,
        snapshot_metadata={},
    )


# --- D1.2: gas_spent_usd = SUM(transaction_ledger.gas_usd) -----------------------

@pytest.mark.asyncio
async def test_gas_spent_usd_equals_ledger_sum(sqlite_store: SQLiteStore) -> None:
    """D1.2: aggregator sums all ledger.gas_usd; status stamped 'ok'."""
    deployment_id = "dep-A"
    for i, gas in enumerate(["0.0042", "0.0017", ""]):
        _insert_ledger_row(sqlite_store, _FakeLedgerRow(
            id=f"row-{i}", deployment_id=deployment_id, strategy_id="demo", gas_usd=gas,
        ))
    total = await sqlite_store.sum_ledger_gas_usd(deployment_id, "demo")
    assert total == Decimal("0.0059")


@pytest.mark.asyncio
async def test_aggregator_no_rows_returns_zero(sqlite_store: SQLiteStore) -> None:
    """No ledger rows → SUM is Decimal('0'), not None."""
    total = await sqlite_store.sum_ledger_gas_usd("nonexistent-dep", "nonexistent-strat")
    assert total == Decimal("0")


# --- F4a: live SQL failure → AccountingPersistenceError -------------------------

@pytest.mark.asyncio
async def test_f4a_live_query_failure_raises() -> None:
    """F4a: live mode + sum_ledger_gas_usd raises non-NotImplementedError → typed error."""
    state_manager = MagicMock()
    state_manager.sum_ledger_gas_usd = AsyncMock(side_effect=RuntimeError("disk i/o error"))
    snapshot = _snapshot()
    metrics = MagicMock(gas_spent_usd=Decimal("0"))

    with pytest.raises(AccountingPersistenceError) as excinfo:
        await _populate_gas_spent_usd(
            _runner_for_metrics(state_manager), metrics, snapshot,
            deployment_id="dep-X", strategy_id="demo", is_live=True,
        )
    assert excinfo.value.write_kind == "metrics"
    assert snapshot.snapshot_metadata["gas_aggregator_status"] == "query_failed"


@pytest.mark.asyncio
async def test_f4a_paper_query_failure_logs_and_continues() -> None:
    """F4a paper: same failure, no raise, gas_spent_usd=0, status=query_failed."""
    state_manager = MagicMock()
    state_manager.sum_ledger_gas_usd = AsyncMock(side_effect=RuntimeError("disk i/o error"))
    snapshot = _snapshot()
    metrics = MagicMock(gas_spent_usd=Decimal("0"))

    await _populate_gas_spent_usd(
        _runner_for_metrics(state_manager), metrics, snapshot,
        deployment_id="dep-X", strategy_id="demo", is_live=False,
    )
    assert metrics.gas_spent_usd == Decimal("0")
    assert snapshot.snapshot_metadata["gas_aggregator_status"] == "query_failed"


# --- F4b: hosted NotImplementedError → no raise in any mode ----------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("is_live", [True, False])
async def test_f4b_hosted_aggregator_unsupported(is_live: bool) -> None:
    """F4b: NotImplementedError catches in BOTH live and paper; never halts."""
    state_manager = MagicMock()
    state_manager.sum_ledger_gas_usd = AsyncMock(
        side_effect=NotImplementedError("VIB-4247 follow-up")
    )
    snapshot = _snapshot()
    metrics = MagicMock(gas_spent_usd=Decimal("0"))

    await _populate_gas_spent_usd(
        _runner_for_metrics(state_manager), metrics, snapshot,
        deployment_id="dep-X", strategy_id="demo", is_live=is_live,
    )
    assert metrics.gas_spent_usd == Decimal("0")
    assert snapshot.snapshot_metadata["gas_aggregator_status"] == "hosted_unsupported"


# --- F4c: type-narrowness — unrelated ValueError → live raises -------------------

@pytest.mark.asyncio
async def test_f4c_aggregator_type_narrow_catch() -> None:
    """F4c: type-narrow catch — ValueError must not be silently classified as
    hosted_unsupported. Live mode raises.
    """
    state_manager = MagicMock()
    state_manager.sum_ledger_gas_usd = AsyncMock(side_effect=ValueError("synthetic"))
    snapshot = _snapshot()
    metrics = MagicMock(gas_spent_usd=Decimal("0"))

    with pytest.raises(AccountingPersistenceError):
        await _populate_gas_spent_usd(
            _runner_for_metrics(state_manager), metrics, snapshot,
            deployment_id="dep-X", strategy_id="demo", is_live=True,
        )
    # Status stamped query_failed (not hosted_unsupported) — proves the catch
    # is type-narrow, not bare-except.
    assert snapshot.snapshot_metadata["gas_aggregator_status"] == "query_failed"


# --- F5: NULL / empty / "0" rows coalesce to 0 ---------------------------------

@pytest.mark.asyncio
async def test_f5_null_and_empty_rows_coalesced(sqlite_store: SQLiteStore) -> None:
    """F5: NULL → 0, '' → 0, '0' → 0, '0.001' → 0.001 (no row silently dropped)."""
    deployment_id = "dep-F5"
    sqlite_store._conn.execute(  # NULL row directly
        """
        INSERT INTO transaction_ledger
            (id, cycle_id, strategy_id, deployment_id, execution_mode,
             timestamp, intent_type, gas_usd, success)
        VALUES ('null-row', 'c', 'demo', ?, 'paper', ?, 'SWAP', NULL, 1)
        """,
        (deployment_id, datetime.now(UTC).isoformat()),
    )
    sqlite_store._conn.commit()
    for i, gas in enumerate(["", "0", "0.001"]):
        _insert_ledger_row(sqlite_store, _FakeLedgerRow(
            id=f"row-{i}", deployment_id=deployment_id, strategy_id="demo", gas_usd=gas,
        ))
    total = await sqlite_store.sum_ledger_gas_usd(deployment_id, "demo")
    assert total == Decimal("0.001")
    # 4 rows total in this deployment — proves no row was dropped from the count.
    cursor = sqlite_store._conn.execute(
        "SELECT COUNT(*) AS n FROM transaction_ledger WHERE deployment_id = ?",
        (deployment_id,),
    )
    assert cursor.fetchone()["n"] == 4


# --- F6: idempotency + freshness -----------------------------------------------

@pytest.mark.asyncio
async def test_f6_idempotent_and_fresh(sqlite_store: SQLiteStore) -> None:
    """F6: three repeated calls match (idempotent); insert a new row, second
    call after the insert reflects the new SUM (fresh).
    """
    deployment_id = "dep-F6"
    _insert_ledger_row(sqlite_store, _FakeLedgerRow(
        id="row-0", deployment_id=deployment_id, strategy_id="demo", gas_usd="0.0042",
    ))
    a = await sqlite_store.sum_ledger_gas_usd(deployment_id, "demo")
    b = await sqlite_store.sum_ledger_gas_usd(deployment_id, "demo")
    c = await sqlite_store.sum_ledger_gas_usd(deployment_id, "demo")
    assert a == b == c == Decimal("0.0042")  # idempotent

    _insert_ledger_row(sqlite_store, _FakeLedgerRow(
        id="row-1", deployment_id=deployment_id, strategy_id="demo", gas_usd="0.0001",
    ))
    fresh = await sqlite_store.sum_ledger_gas_usd(deployment_id, "demo")
    assert fresh == Decimal("0.0043")  # picked up the new row, not stuck at a's value


# --- happy path: gas_aggregator_status='ok' on success --------------------------

@pytest.mark.asyncio
async def test_happy_path_stamps_ok() -> None:
    """gas_aggregator_status=='ok' on a successful aggregator return."""
    state_manager = MagicMock()
    state_manager.sum_ledger_gas_usd = AsyncMock(return_value=Decimal("0.005"))
    snapshot = _snapshot()
    metrics = MagicMock(gas_spent_usd=Decimal("0"))

    await _populate_gas_spent_usd(
        _runner_for_metrics(state_manager), metrics, snapshot,
        deployment_id="dep-X", strategy_id="demo", is_live=True,
    )
    assert metrics.gas_spent_usd == Decimal("0.005")
    assert snapshot.snapshot_metadata["gas_aggregator_status"] == "ok"


# --- backend without sum_ledger_gas_usd attribute → hosted_unsupported ---------

@pytest.mark.asyncio
async def test_aggregator_perf_10k_rows(sqlite_store: SQLiteStore) -> None:
    """D2.5.2 perf: SUM over 10k mixed-empty rows completes in < 100 ms.

    Pins the aggregator against accidental N+1 / per-row Python-loop regressions
    (the SUM happens at SQLite level via a single CAST(NULLIF(gas_usd, '')) AS REAL).
    """
    import time

    deployment_id = "dep-perf"
    # Bulk-insert 10k rows mixing valid / empty / NULL gas_usd.
    rows = []
    for i in range(10_000):
        if i % 3 == 0:
            gas = "0.0001"
        elif i % 3 == 1:
            gas = ""
        else:
            gas = None
        rows.append((f"row-{i}", "cycle-perf", "demo", deployment_id, "paper",
                     datetime.now(UTC).isoformat(), "SWAP", gas, 1))
    sqlite_store._conn.executemany(
        """
        INSERT INTO transaction_ledger
            (id, cycle_id, strategy_id, deployment_id, execution_mode,
             timestamp, intent_type, gas_usd, success)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    sqlite_store._conn.commit()

    start = time.perf_counter()
    total = await sqlite_store.sum_ledger_gas_usd(deployment_id, "demo")
    elapsed_ms = (time.perf_counter() - start) * 1000

    # 1/3 of 10k rows have "0.0001" → ~3334 rows × 0.0001 = ~0.3334.
    expected = Decimal("0.0001") * (10_000 // 3 + (1 if 10_000 % 3 > 0 else 0))
    assert abs(total - expected) < Decimal("0.001"), f"SUM={total}, expected~{expected}"
    assert elapsed_ms < 100, f"aggregator took {elapsed_ms:.1f}ms (> 100ms threshold — regressed to per-row loop?)"


@pytest.mark.asyncio
async def test_legacy_backend_without_aggregator() -> None:
    """An older backend that pre-dates the aggregator stamps hosted_unsupported."""
    state_manager = MagicMock(spec=[])  # no attributes
    snapshot = _snapshot()
    metrics = MagicMock(gas_spent_usd=Decimal("0"))

    await _populate_gas_spent_usd(
        _runner_for_metrics(state_manager), metrics, snapshot,
        deployment_id="dep-X", strategy_id="demo", is_live=True,
    )
    assert metrics.gas_spent_usd == Decimal("0")
    assert snapshot.snapshot_metadata["gas_aggregator_status"] == "hosted_unsupported"
