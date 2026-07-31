"""Focused compatibility tests for persisted execution-mode stamps."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from almanak.framework.models.run_mode import RunMode, RunModeStamp
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.portfolio.models import PortfolioMetrics
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
