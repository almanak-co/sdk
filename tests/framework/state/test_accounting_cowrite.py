"""Tests for atomic co-write and portfolio_metrics extensions (VIB-2765).

Validates:
- total_value_usd is persisted and read back from portfolio_metrics
- save_snapshot_and_metrics atomically writes both or neither
"""

import asyncio
import tempfile
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.portfolio.models import PortfolioMetrics, PortfolioSnapshot, ValueConfidence
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


@pytest.fixture
def store(tmp_path):
    """Create a fresh SQLiteStore for each test."""
    db_path = str(tmp_path / "test.db")
    config = SQLiteConfig(db_path=db_path)
    s = SQLiteStore(config)
    asyncio.get_event_loop().run_until_complete(s.initialize())
    yield s
    asyncio.get_event_loop().run_until_complete(s.close())


def _make_snapshot(deployment_id: str = "strat:abc123", **kwargs) -> PortfolioSnapshot:
    defaults = dict(
        timestamp=datetime.now(UTC),
        deployment_id=deployment_id,
        total_value_usd=Decimal("10000"),
        available_cash_usd=Decimal("5000"),
        value_confidence=ValueConfidence.HIGH,
        chain="arbitrum",
        iteration_number=1,
    )
    defaults.update(kwargs)
    return PortfolioSnapshot(**defaults)


def _make_metrics(deployment_id: str = "strat:abc123", **kwargs) -> PortfolioMetrics:
    defaults = dict(
        deployment_id=deployment_id,
        timestamp=datetime.now(UTC),
        total_value_usd=Decimal("10000"),
        initial_value_usd=Decimal("9500"),
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
        gas_spent_usd=Decimal("15"),
    )
    defaults.update(kwargs)
    return PortfolioMetrics(**defaults)


class TestMetricsTotalValuePersistence:
    """total_value_usd is now persisted in portfolio_metrics (not hardcoded 0)."""

    def test_total_value_round_trip(self, store):
        metrics = _make_metrics(total_value_usd=Decimal("12345.67"))
        asyncio.get_event_loop().run_until_complete(store.save_portfolio_metrics(metrics))

        loaded = asyncio.get_event_loop().run_until_complete(
            store.get_portfolio_metrics("strat:abc123")
        )
        assert loaded is not None
        assert loaded.total_value_usd == Decimal("12345.67")

    def test_total_value_not_zero_after_save(self, store):
        """Regression: total_value_usd was previously hardcoded to 0 on read."""
        metrics = _make_metrics(total_value_usd=Decimal("500"))
        asyncio.get_event_loop().run_until_complete(store.save_portfolio_metrics(metrics))

        loaded = asyncio.get_event_loop().run_until_complete(
            store.get_portfolio_metrics("strat:abc123")
        )
        assert loaded is not None
        assert loaded.total_value_usd != Decimal("0")
        assert loaded.total_value_usd == Decimal("500")

    def test_pnl_computed_from_persisted_value(self, store):
        """PnL properties use the persisted total_value_usd."""
        metrics = _make_metrics(
            total_value_usd=Decimal("10500"),
            initial_value_usd=Decimal("10000"),
            gas_spent_usd=Decimal("50"),
        )
        asyncio.get_event_loop().run_until_complete(store.save_portfolio_metrics(metrics))

        loaded = asyncio.get_event_loop().run_until_complete(
            store.get_portfolio_metrics("strat:abc123")
        )
        assert loaded is not None
        assert loaded.pnl_before_gas == Decimal("500")
        assert loaded.pnl_after_gas == Decimal("450")


class TestAtomicCoWrite:
    """save_snapshot_and_metrics writes both or neither."""

    def test_both_written(self, store):
        snapshot = _make_snapshot()
        metrics = _make_metrics()

        sid = asyncio.get_event_loop().run_until_complete(
            store.save_snapshot_and_metrics(snapshot, metrics)
        )
        assert sid > 0

        loaded_snap = asyncio.get_event_loop().run_until_complete(
            store.get_latest_snapshot("strat:abc123")
        )
        loaded_metrics = asyncio.get_event_loop().run_until_complete(
            store.get_portfolio_metrics("strat:abc123")
        )
        assert loaded_snap is not None
        assert loaded_metrics is not None
        assert loaded_snap.total_value_usd == Decimal("10000")
        assert loaded_metrics.total_value_usd == Decimal("10000")

    def test_snapshot_exists_iff_metrics_exists(self, store):
        """Invariant: snapshot and metrics always appear together."""
        snapshot = _make_snapshot(deployment_id="test:inv")
        metrics = _make_metrics(deployment_id="test:inv")

        asyncio.get_event_loop().run_until_complete(
            store.save_snapshot_and_metrics(snapshot, metrics)
        )

        snap = asyncio.get_event_loop().run_until_complete(
            store.get_latest_snapshot("test:inv")
        )
        met = asyncio.get_event_loop().run_until_complete(
            store.get_portfolio_metrics("test:inv")
        )
        # Both exist
        assert snap is not None
        assert met is not None

        # Neither for unknown strategy
        snap2 = asyncio.get_event_loop().run_until_complete(
            store.get_latest_snapshot("unknown:xxx")
        )
        met2 = asyncio.get_event_loop().run_until_complete(
            store.get_portfolio_metrics("unknown:xxx")
        )
        assert snap2 is None
        assert met2 is None
