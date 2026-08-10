"""Focused compatibility tests for persisted execution-mode stamps."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from almanak.framework.models.run_mode import RunMode, RunModeStamp
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.portfolio.models import (
    BaselineProvenance,
    BaselineProvenanceError,
    PortfolioMetrics,
    decode_baseline_provenance,
    encode_baseline_provenance,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


@pytest_asyncio.fixture
async def store() -> AsyncIterator[SQLiteStore]:
    sqlite_store = SQLiteStore(SQLiteConfig(db_path=":memory:"))
    await sqlite_store.initialize()
    yield sqlite_store
    await sqlite_store.close()


def _assert_mode(actual: RunModeStamp, expected: RunModeStamp) -> None:
    if isinstance(expected, RunMode):
        assert actual is expected
    else:
        assert actual == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        ("dry_run", RunMode.DRY_RUN),
        ("paper", RunMode.PAPER),
        ("live", RunMode.LIVE),
        ("", ""),
        (None, ""),
    ],
)
async def test_portfolio_metrics_reader_parses_persisted_modes(
    store: SQLiteStore, stored: str | None, expected: RunModeStamp
) -> None:
    metrics = PortfolioMetrics(
        deployment_id="deployment:metricsmode1",
        timestamp=datetime(2026, 7, 31, tzinfo=UTC),
        total_value_usd=Decimal("10"),
        initial_value_usd=Decimal("10"),
        positions_json=encode_baseline_provenance(
            BaselineProvenance(source="strategy_allocation_usd", initial_value_usd=Decimal("10"))
        ),
    )
    assert await store.save_portfolio_metrics(metrics) is True
    store._conn.execute(  # type: ignore[union-attr]
        "UPDATE portfolio_metrics SET execution_mode = ? WHERE deployment_id = ?",
        (stored, metrics.deployment_id),
    )
    store._conn.commit()  # type: ignore[union-attr]

    loaded = await store.get_portfolio_metrics(metrics.deployment_id)

    assert loaded is not None
    _assert_mode(loaded.execution_mode, expected)


@pytest.mark.asyncio
async def test_portfolio_metrics_provenance_round_trips_in_same_sqlite_row(store: SQLiteStore) -> None:
    payload = (
        '[{"initial_value_usd":"4","record_type":"accounting_baseline_provenance",'
        '"schema_version":1,"source":"strategy_allocation_usd"}]'
    )
    metrics = PortfolioMetrics(
        deployment_id="deployment:metricsprovenance1",
        timestamp=datetime(2026, 8, 9, tzinfo=UTC),
        total_value_usd=Decimal("4"),
        initial_value_usd=Decimal("4"),
        positions_json=payload,
    )

    assert await store.save_portfolio_metrics(metrics) is True
    loaded = await store.get_portfolio_metrics(metrics.deployment_id)

    assert loaded is not None
    assert loaded.initial_value_usd == Decimal("4")
    assert loaded.positions_json == payload


@pytest.mark.asyncio
async def test_portfolio_metrics_provenance_is_write_once_in_sqlite(store: SQLiteStore) -> None:
    payload = (
        '[{"initial_value_usd":"4","record_type":"accounting_baseline_provenance",'
        '"schema_version":1,"source":"strategy_allocation_usd"}]'
    )
    established = PortfolioMetrics(
        deployment_id="deployment:metricsimmutable1",
        timestamp=datetime(2026, 8, 9, tzinfo=UTC),
        total_value_usd=Decimal("4"),
        initial_value_usd=Decimal("4"),
        positions_json=payload,
    )
    assert await store.save_portfolio_metrics(established) is True

    with pytest.raises(BaselineProvenanceError, match="cannot be removed or replaced"):
        await store.save_portfolio_metrics(
            PortfolioMetrics(
                deployment_id=established.deployment_id,
                timestamp=established.timestamp,
                total_value_usd=Decimal("4"),
                initial_value_usd=Decimal("4"),
                positions_json="[]",
            )
        )
    with pytest.raises(BaselineProvenanceError, match="initial_value_usd"):
        await store.save_portfolio_metrics(
            PortfolioMetrics(
                deployment_id=established.deployment_id,
                timestamp=established.timestamp,
                total_value_usd=Decimal("5"),
                initial_value_usd=Decimal("5"),
                positions_json=payload,
            )
        )

    loaded = await store.get_portfolio_metrics(established.deployment_id)
    assert loaded is not None
    assert loaded.initial_value_usd == Decimal("4")
    assert loaded.positions_json == payload


@pytest.mark.asyncio
async def test_legacy_sqlite_baseline_remains_immutable_and_unproven(store: SQLiteStore) -> None:
    """A pre-existing legacy row remains updateable but cannot be backfilled."""
    deployment_id = "deployment:legacybaseline1"
    timestamp = datetime(2026, 8, 9, tzinfo=UTC)
    store._conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO portfolio_metrics (
            deployment_id, initial_value_usd, initial_timestamp,
            total_value_usd, positions_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (deployment_id, "4", timestamp.isoformat(), "4", "[]", timestamp.isoformat()),
    )
    store._conn.commit()  # type: ignore[union-attr]

    legacy_update = PortfolioMetrics(
        deployment_id=deployment_id,
        timestamp=timestamp,
        total_value_usd=Decimal("4"),
        initial_value_usd=Decimal("4"),
        positions_json="[]",
    )
    assert await store.save_portfolio_metrics(legacy_update) is True

    provenance = encode_baseline_provenance(
        BaselineProvenance(source="strategy_allocation_usd", initial_value_usd=Decimal("4"))
    )
    with pytest.raises(BaselineProvenanceError, match="cannot be backfilled"):
        await store.save_portfolio_metrics(
            PortfolioMetrics(
                deployment_id=deployment_id,
                timestamp=timestamp,
                total_value_usd=Decimal("4"),
                initial_value_usd=Decimal("4"),
                positions_json=provenance,
            )
        )

    loaded = await store.get_portfolio_metrics(deployment_id)
    assert loaded is not None
    assert loaded.initial_value_usd == Decimal("4")
    assert decode_baseline_provenance(loaded.positions_json) is None


@pytest.mark.asyncio
async def test_direct_sqlite_rejects_markerless_first_row_without_mutation(store: SQLiteStore) -> None:
    markerless = PortfolioMetrics(
        deployment_id="deployment:markerless-first",
        timestamp=datetime(2026, 8, 9, tzinfo=UTC),
        total_value_usd=Decimal("4"),
        initial_value_usd=Decimal("4"),
        positions_json="[]",
    )

    with pytest.raises(BaselineProvenanceError, match="required when establishing"):
        await store.save_portfolio_metrics(markerless)

    assert await store.get_portfolio_metrics(markerless.deployment_id) is None


@pytest.mark.asyncio
async def test_sqlite_refuses_contradictory_provenance_on_first_write(store: SQLiteStore) -> None:
    contradictory = PortfolioMetrics(
        deployment_id="deployment:contradictory-first-write",
        timestamp=datetime(2026, 8, 9, tzinfo=UTC),
        total_value_usd=Decimal("4"),
        initial_value_usd=Decimal("4"),
        positions_json=encode_baseline_provenance(
            BaselineProvenance(source="strategy_allocation_usd", initial_value_usd=Decimal("5"))
        ),
    )

    with pytest.raises(BaselineProvenanceError, match="must equal metrics initial_value_usd"):
        await store.save_portfolio_metrics(contradictory)
    assert await store.get_portfolio_metrics(contradictory.deployment_id) is None


@pytest.mark.asyncio
async def test_sqlite_refuses_lexically_different_provenance_rewrite(store: SQLiteStore) -> None:
    deployment_id = "deployment:exact-provenance-object"

    def metrics(value_text: str) -> PortfolioMetrics:
        return PortfolioMetrics(
            deployment_id=deployment_id,
            timestamp=datetime(2026, 8, 9, tzinfo=UTC),
            total_value_usd=Decimal("4"),
            initial_value_usd=Decimal("4"),
            positions_json=encode_baseline_provenance(
                BaselineProvenance(
                    source="strategy_allocation_usd",
                    initial_value_usd=Decimal(value_text),
                )
            ),
        )

    assert await store.save_portfolio_metrics(metrics("4.00")) is True
    with pytest.raises(BaselineProvenanceError, match="cannot be removed or replaced"):
        await store.save_portfolio_metrics(metrics("4"))

    loaded = await store.get_portfolio_metrics(deployment_id)
    assert loaded is not None
    assert '"initial_value_usd":"4.00"' in loaded.positions_json


@pytest.mark.asyncio
async def test_concurrent_sqlite_baseline_creation_has_exactly_one_winner(tmp_path, monkeypatch) -> None:
    """Validation and write are one transaction, including across store instances."""
    db_path = tmp_path / "baseline-race.sqlite"
    stores = [SQLiteStore(SQLiteConfig(db_path=str(db_path))) for _ in range(2)]
    for sqlite_store in stores:
        await sqlite_store.initialize()

    # Force both pre-fix SELECTs to finish before either INSERT. With the fixed
    # BEGIN IMMEDIATE boundary, the second store cannot reach validation until
    # the first commits; the timeout keeps that correct serialization live.
    from almanak.framework.state.backends import sqlite as sqlite_module

    original_validate = sqlite_module._validated_metrics_positions_json
    barrier = threading.Barrier(2)

    def synchronized_validate(conn, metrics):
        result = original_validate(conn, metrics)
        try:
            barrier.wait(timeout=0.25)
        except threading.BrokenBarrierError:
            pass
        return result

    monkeypatch.setattr(sqlite_module, "_validated_metrics_positions_json", synchronized_validate)

    def candidate(value: str) -> PortfolioMetrics:
        return PortfolioMetrics(
            deployment_id="deployment:baseline-race",
            timestamp=datetime(2026, 8, 9, tzinfo=UTC),
            total_value_usd=Decimal(value),
            initial_value_usd=Decimal(value),
            positions_json=encode_baseline_provenance(
                BaselineProvenance(source="strategy_allocation_usd", initial_value_usd=Decimal(value))
            ),
        )

    try:
        outcomes = await asyncio.gather(
            stores[0].save_portfolio_metrics(candidate("4")),
            stores[1].save_portfolio_metrics(candidate("5")),
            return_exceptions=True,
        )
        assert sum(outcome is True for outcome in outcomes) == 1
        errors = [outcome for outcome in outcomes if isinstance(outcome, BaselineProvenanceError)]
        assert len(errors) == 1

        loaded = await stores[0].get_portfolio_metrics("deployment:baseline-race")
        assert loaded is not None
        provenance = decode_baseline_provenance(loaded.positions_json)
        assert provenance is not None
        assert provenance.initial_value_usd == loaded.initial_value_usd
    finally:
        for sqlite_store in stores:
            await sqlite_store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        ("dry_run", RunMode.DRY_RUN),
        ("paper", RunMode.PAPER),
        ("live", RunMode.LIVE),
        ("", ""),
        (None, ""),
    ],
)
async def test_ledger_reader_parses_persisted_modes(
    store: SQLiteStore, stored: str | None, expected: RunModeStamp
) -> None:
    entry = LedgerEntry(id="ledger-mode-1", deployment_id="deployment:ledgermode1")
    await store.save_ledger_entry(entry)
    store._conn.execute(  # type: ignore[union-attr]
        "UPDATE transaction_ledger SET execution_mode = ? WHERE id = ?",
        (stored, entry.id),
    )
    store._conn.commit()  # type: ignore[union-attr]

    loaded = await store.get_ledger_entries(entry.deployment_id)

    assert len(loaded) == 1
    _assert_mode(loaded[0].execution_mode, expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["metrics", "ledger"])
async def test_persisted_invalid_mode_is_rejected(store: SQLiteStore, reader: str) -> None:
    if reader == "metrics":
        metrics = PortfolioMetrics(
            deployment_id="deployment:badmetrics01",
            timestamp=datetime(2026, 7, 31, tzinfo=UTC),
            total_value_usd=Decimal("10"),
            initial_value_usd=Decimal("10"),
            positions_json=encode_baseline_provenance(
                BaselineProvenance(source="strategy_allocation_usd", initial_value_usd=Decimal("10"))
            ),
        )
        assert await store.save_portfolio_metrics(metrics) is True
        store._conn.execute(  # type: ignore[union-attr]
            "UPDATE portfolio_metrics SET execution_mode = 'livve' WHERE deployment_id = ?",
            (metrics.deployment_id,),
        )
        store._conn.commit()  # type: ignore[union-attr]
        read = store.get_portfolio_metrics(metrics.deployment_id)
    else:
        entry = LedgerEntry(id="bad-ledger-mode", deployment_id="deployment:badledger001")
        await store.save_ledger_entry(entry)
        store._conn.execute(  # type: ignore[union-attr]
            "UPDATE transaction_ledger SET execution_mode = 'livve' WHERE id = ?",
            (entry.id,),
        )
        store._conn.commit()  # type: ignore[union-attr]
        read = store.get_ledger_entries(entry.deployment_id)

    with pytest.raises(ValueError, match="invalid run mode"):
        await read


def _mocked_store(row: dict[str, Any], *, many: bool) -> SQLiteStore:
    cursor = MagicMock()
    if many:
        cursor.fetchall.return_value = [row]
    else:
        cursor.fetchone.return_value = row
    connection = MagicMock()
    connection.execute.return_value = cursor
    sqlite_store = SQLiteStore(SQLiteConfig(db_path=":memory:"))
    sqlite_store._initialized = True
    sqlite_store._conn = connection
    return sqlite_store


@pytest.mark.asyncio
async def test_metrics_reader_preserves_unstamped_row_missing_execution_mode() -> None:
    now = datetime(2026, 7, 31, tzinfo=UTC).isoformat()
    row = {
        "initial_value_usd": "10",
        "initial_timestamp": now,
        "deposits_usd": "0",
        "withdrawals_usd": "0",
        "gas_spent_usd": "0",
        "total_value_usd": "10",
        "positions_json": "[]",
        "cycle_id": None,
        "deployment_id": "deployment:legacy000001",
        "is_complete": 1,
        "updated_at": now,
    }

    loaded = await _mocked_store(row, many=False).get_portfolio_metrics(row["deployment_id"])

    assert loaded is not None
    assert loaded.execution_mode == ""


@pytest.mark.asyncio
async def test_ledger_reader_preserves_unstamped_row_missing_execution_mode() -> None:
    row = {
        "id": "legacy-ledger",
        "cycle_id": "",
        "deployment_id": "deployment:legacy000002",
        "timestamp": datetime(2026, 7, 31, tzinfo=UTC).isoformat(),
        "intent_type": "HOLD",
        "token_in": "",
        "amount_in": "",
        "token_out": "",
        "amount_out": "",
        "effective_price": "",
        "slippage_bps": None,
        "gas_used": 0,
        "gas_usd": "",
        "tx_hash": "",
        "chain": "",
        "protocol": "",
        "success": 1,
        "error": "",
    }

    loaded = await _mocked_store(row, many=True).get_ledger_entries(row["deployment_id"])

    assert len(loaded) == 1
    assert loaded[0].execution_mode == ""
