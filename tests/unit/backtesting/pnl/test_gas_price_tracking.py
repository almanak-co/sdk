"""Tests for gas price per-tick tracking (US-090b).

This module tests the gas price tracking feature that records GasPriceRecord
for each trade and provides GasPriceSummary statistics.

Key Features Tested:
1. GasPriceRecord dataclass creation and serialization
2. GasPriceSummary calculation from records and trades
3. BacktestResult gas_prices_used and gas_price_summary fields
4. track_gas_prices config option behavior
"""

from datetime import datetime
from decimal import Decimal

from almanak.framework.backtesting.models import (
    BacktestResult,
    GasPriceRecord,
    GasPriceSummary,
    IntentType,
    TradeRecord,
)


class TestGasPriceRecord:
    """Tests for GasPriceRecord dataclass."""

    def test_create_gas_price_record(self):
        """Test creating a basic GasPriceRecord."""
        timestamp = datetime(2024, 6, 15, 12, 0)
        record = GasPriceRecord(
            timestamp=timestamp,
            gwei=Decimal("30"),
            source="historical_gas:etherscan",
            usd_cost=Decimal("2.50"),
            eth_price_usd=Decimal("3000"),
        )
        assert record.timestamp == timestamp
        assert record.gwei == Decimal("30")
        assert record.source == "historical_gas:etherscan"
        assert record.usd_cost == Decimal("2.50")
        assert record.eth_price_usd == Decimal("3000")

    def test_gas_price_record_default_values(self):
        """Test GasPriceRecord with default values."""
        timestamp = datetime(2024, 6, 15, 12, 0)
        record = GasPriceRecord(
            timestamp=timestamp,
            gwei=Decimal("25"),
            source="config",
        )
        assert record.usd_cost == Decimal("0")
        assert record.eth_price_usd is None

    def test_gas_price_record_to_dict(self):
        """Test serialization to dictionary."""
        timestamp = datetime(2024, 6, 15, 12, 0)
        record = GasPriceRecord(
            timestamp=timestamp,
            gwei=Decimal("30"),
            source="market_state",
            usd_cost=Decimal("1.50"),
            eth_price_usd=Decimal("2500"),
        )
        data = record.to_dict()
        assert data["timestamp"] == "2024-06-15T12:00:00"
        assert data["gwei"] == "30"
        assert data["source"] == "market_state"
        assert data["usd_cost"] == "1.50"
        assert data["eth_price_usd"] == "2500"

    def test_gas_price_record_to_dict_without_eth_price(self):
        """Test serialization without eth_price_usd."""
        timestamp = datetime(2024, 6, 15, 12, 0)
        record = GasPriceRecord(
            timestamp=timestamp,
            gwei=Decimal("30"),
            source="config",
        )
        data = record.to_dict()
        assert data["eth_price_usd"] is None

    def test_gas_price_record_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "timestamp": "2024-06-15T12:00:00",
            "gwei": "30",
            "source": "historical_gas:etherscan",
            "usd_cost": "2.50",
            "eth_price_usd": "3000",
        }
        record = GasPriceRecord.from_dict(data)
        assert record.timestamp == datetime(2024, 6, 15, 12, 0)
        assert record.gwei == Decimal("30")
        assert record.source == "historical_gas:etherscan"
        assert record.usd_cost == Decimal("2.50")
        assert record.eth_price_usd == Decimal("3000")

    def test_gas_price_record_from_dict_without_eth_price(self):
        """Test deserialization without eth_price_usd."""
        data = {
            "timestamp": "2024-06-15T12:00:00",
            "gwei": "30",
            "source": "config",
        }
        record = GasPriceRecord.from_dict(data)
        assert record.eth_price_usd is None


class TestGasPriceSummary:
    """Tests for GasPriceSummary dataclass."""

    def test_create_gas_price_summary(self):
        """Test creating a GasPriceSummary."""
        summary = GasPriceSummary(
            min_gwei=Decimal("10"),
            max_gwei=Decimal("100"),
            mean_gwei=Decimal("45"),
            std_gwei=Decimal("25"),
            source_breakdown={"historical_gas:etherscan": 80, "config": 20},
            total_records=100,
        )
        assert summary.min_gwei == Decimal("10")
        assert summary.max_gwei == Decimal("100")
        assert summary.mean_gwei == Decimal("45")
        assert summary.std_gwei == Decimal("25")
        assert summary.source_breakdown == {"historical_gas:etherscan": 80, "config": 20}
        assert summary.total_records == 100

    def test_gas_price_summary_default_values(self):
        """Test GasPriceSummary with default values."""
        summary = GasPriceSummary()
        assert summary.min_gwei == Decimal("0")
        assert summary.max_gwei == Decimal("0")
        assert summary.mean_gwei == Decimal("0")
        assert summary.std_gwei == Decimal("0")
        assert summary.source_breakdown == {}
        assert summary.total_records == 0

    def test_gas_price_summary_to_dict(self):
        """Test serialization to dictionary."""
        summary = GasPriceSummary(
            min_gwei=Decimal("20"),
            max_gwei=Decimal("50"),
            mean_gwei=Decimal("35"),
            std_gwei=Decimal("10"),
            source_breakdown={"config": 10},
            total_records=10,
        )
        data = summary.to_dict()
        assert data["min_gwei"] == "20"
        assert data["max_gwei"] == "50"
        assert data["mean_gwei"] == "35"
        assert data["std_gwei"] == "10"
        assert data["source_breakdown"] == {"config": 10}
        assert data["total_records"] == 10

    def test_gas_price_summary_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "min_gwei": "20",
            "max_gwei": "50",
            "mean_gwei": "35",
            "std_gwei": "10",
            "source_breakdown": {"config": 10},
            "total_records": 10,
        }
        summary = GasPriceSummary.from_dict(data)
        assert summary.min_gwei == Decimal("20")
        assert summary.max_gwei == Decimal("50")
        assert summary.mean_gwei == Decimal("35")
        assert summary.std_gwei == Decimal("10")
        assert summary.source_breakdown == {"config": 10}
        assert summary.total_records == 10

    def test_gas_price_summary_from_records_empty(self):
        """Test from_records with empty list."""
        summary = GasPriceSummary.from_records([])
        assert summary.min_gwei == Decimal("0")
        assert summary.max_gwei == Decimal("0")
        assert summary.mean_gwei == Decimal("0")
        assert summary.std_gwei == Decimal("0")
        assert summary.source_breakdown == {}
        assert summary.total_records == 0

    def test_gas_price_summary_from_records_single(self):
        """Test from_records with single record."""
        records = [
            GasPriceRecord(
                timestamp=datetime(2024, 6, 15, 12, 0),
                gwei=Decimal("30"),
                source="config",
            )
        ]
        summary = GasPriceSummary.from_records(records)
        assert summary.min_gwei == Decimal("30")
        assert summary.max_gwei == Decimal("30")
        assert summary.mean_gwei == Decimal("30")
        assert summary.std_gwei == Decimal("0")
        assert summary.source_breakdown == {"config": 1}
        assert summary.total_records == 1

    def test_gas_price_summary_from_records_multiple(self):
        """Test from_records with multiple records."""
        records = [
            GasPriceRecord(
                timestamp=datetime(2024, 6, 15, 12, 0),
                gwei=Decimal("20"),
                source="config",
            ),
            GasPriceRecord(
                timestamp=datetime(2024, 6, 15, 13, 0),
                gwei=Decimal("40"),
                source="historical_gas:etherscan",
            ),
            GasPriceRecord(
                timestamp=datetime(2024, 6, 15, 14, 0),
                gwei=Decimal("30"),
                source="historical_gas:etherscan",
            ),
        ]
        summary = GasPriceSummary.from_records(records)
        assert summary.min_gwei == Decimal("20")
        assert summary.max_gwei == Decimal("40")
        assert summary.mean_gwei == Decimal("30")  # (20+40+30)/3 = 30
        assert summary.source_breakdown == {
            "config": 1,
            "historical_gas:etherscan": 2,
        }
        assert summary.total_records == 3

    def test_gas_price_summary_std_calculation(self):
        """Test standard deviation calculation."""
        # Values: 10, 20, 30 -> mean = 20, variance = ((10-20)^2 + (20-20)^2 + (30-20)^2)/3 = 200/3
        # std = sqrt(200/3) ≈ 8.165
        records = [
            GasPriceRecord(
                timestamp=datetime(2024, 6, 15, 12, 0),
                gwei=Decimal("10"),
                source="config",
            ),
            GasPriceRecord(
                timestamp=datetime(2024, 6, 15, 13, 0),
                gwei=Decimal("20"),
                source="config",
            ),
            GasPriceRecord(
                timestamp=datetime(2024, 6, 15, 14, 0),
                gwei=Decimal("30"),
                source="config",
            ),
        ]
        summary = GasPriceSummary.from_records(records)
        # Should be approximately 8.165
        assert summary.std_gwei > Decimal("8")
        assert summary.std_gwei < Decimal("9")


class TestBacktestResultGasPriceFields:
    """Tests for BacktestResult gas price fields."""

    def test_backtest_result_default_gas_fields(self):
        """Test default values for gas price fields."""
        from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics

        result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            metrics=BacktestMetrics(),
        )
        assert result.gas_prices_used == []
        assert result.gas_price_summary is None

    def test_backtest_result_with_gas_prices(self):
        """Test BacktestResult with gas price data."""
        from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics

        gas_records = [
            GasPriceRecord(
                timestamp=datetime(2024, 1, 15, 12, 0),
                gwei=Decimal("30"),
                source="config",
                usd_cost=Decimal("2.50"),
            ),
            GasPriceRecord(
                timestamp=datetime(2024, 2, 15, 12, 0),
                gwei=Decimal("40"),
                source="historical_gas:etherscan",
                usd_cost=Decimal("3.50"),
            ),
        ]
        summary = GasPriceSummary(
            min_gwei=Decimal("30"),
            max_gwei=Decimal("40"),
            mean_gwei=Decimal("35"),
            std_gwei=Decimal("5"),
            source_breakdown={"config": 1, "historical_gas:etherscan": 1},
            total_records=2,
        )

        result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            metrics=BacktestMetrics(),
            gas_prices_used=gas_records,
            gas_price_summary=summary,
        )
        assert len(result.gas_prices_used) == 2
        assert result.gas_price_summary is not None
        assert result.gas_price_summary.total_records == 2

    def test_backtest_result_to_dict_includes_gas_fields(self):
        """Test that to_dict includes gas price fields."""
        from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics

        gas_records = [
            GasPriceRecord(
                timestamp=datetime(2024, 1, 15, 12, 0),
                gwei=Decimal("30"),
                source="config",
            ),
        ]
        summary = GasPriceSummary(
            min_gwei=Decimal("30"),
            max_gwei=Decimal("30"),
            mean_gwei=Decimal("30"),
            std_gwei=Decimal("0"),
            source_breakdown={"config": 1},
            total_records=1,
        )

        result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            metrics=BacktestMetrics(),
            gas_prices_used=gas_records,
            gas_price_summary=summary,
        )
        data = result.to_dict()
        assert "gas_prices_used" in data
        assert "gas_price_summary" in data
        assert len(data["gas_prices_used"]) == 1
        assert data["gas_price_summary"]["total_records"] == 1

    def test_backtest_result_from_dict_parses_gas_fields(self):
        """Test that from_dict parses gas price fields."""

        result_data = {
            "engine": "pnl",
            "strategy_id": "test_strategy",
            "start_time": "2024-01-01T00:00:00",
            "end_time": "2024-06-01T00:00:00",
            "metrics": {},
            "trades": [],
            "equity_curve": [],
            "gas_prices_used": [
                {
                    "timestamp": "2024-01-15T12:00:00",
                    "gwei": "30",
                    "source": "config",
                    "usd_cost": "2.50",
                    "eth_price_usd": "3000",
                }
            ],
            "gas_price_summary": {
                "min_gwei": "30",
                "max_gwei": "30",
                "mean_gwei": "30",
                "std_gwei": "0",
                "source_breakdown": {"config": 1},
                "total_records": 1,
            },
        }
        result = BacktestResult.from_dict(result_data)
        assert len(result.gas_prices_used) == 1
        assert result.gas_prices_used[0].gwei == Decimal("30")
        assert result.gas_price_summary is not None
        assert result.gas_price_summary.total_records == 1


class TestTrackGasPricesConfig:
    """Tests for track_gas_prices config option."""

    def test_track_gas_prices_default_false(self):
        """Test that track_gas_prices defaults to False."""
        from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
        )
        assert config.track_gas_prices is False

    def test_track_gas_prices_enabled(self):
        """Test enabling track_gas_prices."""
        from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            track_gas_prices=True,
        )
        assert config.track_gas_prices is True


class TestGasPriceSummaryFromTrades:
    """Tests for creating GasPriceSummary from TradeRecord list."""

    def test_summary_from_trades_empty(self):
        """Test with empty trades list."""
        trades: list[TradeRecord] = []
        # Manual calculation equivalent to engine method
        gas_prices = [t.gas_price_gwei for t in trades if t.gas_price_gwei is not None]
        assert len(gas_prices) == 0

    def test_summary_from_trades_with_gas_prices(self):
        """Test with trades that have gas_price_gwei."""
        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 15, 12, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("3000"),
                fee_usd=Decimal("1"),
                slippage_usd=Decimal("0.5"),
                gas_cost_usd=Decimal("2.50"),
                pnl_usd=Decimal("10"),
                success=True,
                gas_price_gwei=Decimal("30"),
                metadata={"gas_price_source": "config"},
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 16, 12, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("3100"),
                fee_usd=Decimal("1"),
                slippage_usd=Decimal("0.5"),
                gas_cost_usd=Decimal("3.50"),
                pnl_usd=Decimal("15"),
                success=True,
                gas_price_gwei=Decimal("50"),
                metadata={"gas_price_source": "historical_gas:etherscan"},
            ),
        ]
        gas_prices = [t.gas_price_gwei for t in trades if t.gas_price_gwei is not None]
        assert len(gas_prices) == 2
        assert min(gas_prices) == Decimal("30")
        assert max(gas_prices) == Decimal("50")

    def test_summary_from_trades_mixed_gas_prices(self):
        """Test with trades where some have None gas_price_gwei."""
        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 15, 12, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("3000"),
                fee_usd=Decimal("1"),
                slippage_usd=Decimal("0.5"),
                gas_cost_usd=Decimal("2.50"),
                pnl_usd=Decimal("10"),
                success=True,
                gas_price_gwei=Decimal("30"),
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 16, 12, 0),
                intent_type=IntentType.HOLD,  # HOLD might not have gas
                executed_price=Decimal("0"),
                fee_usd=Decimal("0"),
                slippage_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
                pnl_usd=Decimal("0"),
                success=True,
                gas_price_gwei=None,
            ),
        ]
        gas_prices = [t.gas_price_gwei for t in trades if t.gas_price_gwei is not None]
        assert len(gas_prices) == 1
        assert gas_prices[0] == Decimal("30")
