"""Tests for crisis scenario warmup_days feature (VIB-176).

Verifies:
- CrisisScenario warmup_days field and warmup_start_date property
- CrisisBacktestConfig.to_pnl_config() extends start_time by warmup_days
- Serialization/deserialization preserves warmup_days
- Date validation uses warmup_start_date
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.backtesting.scenarios.crisis import (
    BLACK_THURSDAY,
    DEFAULT_WARMUP_DAYS,
    FTX_COLLAPSE,
    TERRA_COLLAPSE,
    CrisisScenario,
)
from almanak.framework.backtesting.scenarios.crisis_runner import (
    CrisisBacktestConfig,
    CrisisScenarioDateRangeError,
    _validate_scenario_date_range,
)


class TestCrisisScenarioWarmup:
    """Test CrisisScenario warmup_days field."""

    def test_default_warmup_days(self):
        """Default warmup_days should be DEFAULT_WARMUP_DAYS (30)."""
        scenario = CrisisScenario(
            name="test",
            start_date=datetime(2023, 6, 1, tzinfo=UTC),
            end_date=datetime(2023, 6, 8, tzinfo=UTC),
            description="Test scenario",
        )
        assert scenario.warmup_days == DEFAULT_WARMUP_DAYS
        assert scenario.warmup_days == 30

    def test_custom_warmup_days(self):
        """Custom warmup_days should override default."""
        scenario = CrisisScenario(
            name="test",
            start_date=datetime(2023, 6, 1, tzinfo=UTC),
            end_date=datetime(2023, 6, 8, tzinfo=UTC),
            description="Test scenario",
            warmup_days=60,
        )
        assert scenario.warmup_days == 60

    def test_zero_warmup_days(self):
        """warmup_days=0 disables warmup (original behavior)."""
        scenario = CrisisScenario(
            name="test",
            start_date=datetime(2023, 6, 1, tzinfo=UTC),
            end_date=datetime(2023, 6, 8, tzinfo=UTC),
            description="Test scenario",
            warmup_days=0,
        )
        assert scenario.warmup_days == 0
        assert scenario.warmup_start_date == scenario.start_date

    def test_warmup_start_date(self):
        """warmup_start_date should be start_date minus warmup_days."""
        start = datetime(2023, 6, 1, tzinfo=UTC)
        scenario = CrisisScenario(
            name="test",
            start_date=start,
            end_date=datetime(2023, 6, 8, tzinfo=UTC),
            description="Test scenario",
            warmup_days=30,
        )
        expected = start - timedelta(days=30)
        assert scenario.warmup_start_date == expected
        assert scenario.warmup_start_date == datetime(2023, 5, 2, tzinfo=UTC)

    def test_predefined_scenarios_have_default_warmup(self):
        """Predefined scenarios should have the default warmup_days."""
        for scenario in [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]:
            assert scenario.warmup_days == DEFAULT_WARMUP_DAYS

    def test_duration_days_unchanged(self):
        """duration_days should reflect only the crisis window, not warmup."""
        scenario = CrisisScenario(
            name="test",
            start_date=datetime(2023, 6, 1, tzinfo=UTC),
            end_date=datetime(2023, 6, 8, tzinfo=UTC),
            description="Test scenario",
            warmup_days=30,
        )
        assert scenario.duration_days == 7  # Not 37


class TestCrisisScenarioSerialization:
    """Test warmup_days survives serialization."""

    def test_to_dict_includes_warmup_days(self):
        """to_dict should include warmup_days."""
        scenario = CrisisScenario(
            name="test",
            start_date=datetime(2023, 6, 1, tzinfo=UTC),
            end_date=datetime(2023, 6, 8, tzinfo=UTC),
            description="Test",
            warmup_days=45,
        )
        d = scenario.to_dict()
        assert d["warmup_days"] == 45

    def test_from_dict_preserves_warmup_days(self):
        """from_dict should restore warmup_days."""
        d = {
            "name": "test",
            "start_date": "2023-06-01T00:00:00+00:00",
            "end_date": "2023-06-08T00:00:00+00:00",
            "description": "Test",
            "warmup_days": 45,
        }
        scenario = CrisisScenario.from_dict(d)
        assert scenario.warmup_days == 45

    def test_from_dict_defaults_warmup_days(self):
        """from_dict without warmup_days should use default."""
        d = {
            "name": "test",
            "start_date": "2023-06-01T00:00:00+00:00",
            "end_date": "2023-06-08T00:00:00+00:00",
            "description": "Test",
        }
        scenario = CrisisScenario.from_dict(d)
        assert scenario.warmup_days == DEFAULT_WARMUP_DAYS

    def test_roundtrip(self):
        """to_dict -> from_dict should preserve all fields."""
        original = CrisisScenario(
            name="test",
            start_date=datetime(2023, 6, 1, tzinfo=UTC),
            end_date=datetime(2023, 6, 8, tzinfo=UTC),
            description="Test",
            warmup_days=15,
        )
        restored = CrisisScenario.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.warmup_days == 15
        assert restored.warmup_start_date == original.warmup_start_date


class TestCrisisBacktestConfigWarmup:
    """Test that CrisisBacktestConfig uses warmup in PnL config."""

    def test_to_pnl_config_extends_start_by_warmup(self):
        """to_pnl_config should use warmup_start_date, not start_date."""
        scenario = CrisisScenario(
            name="test",
            start_date=datetime(2023, 6, 1, tzinfo=UTC),
            end_date=datetime(2023, 6, 8, tzinfo=UTC),
            description="Test",
            warmup_days=30,
        )
        config = CrisisBacktestConfig(scenario=scenario)
        pnl_config = config.to_pnl_config()

        expected_start = datetime(2023, 5, 2, tzinfo=UTC)
        assert pnl_config.start_time == expected_start
        assert pnl_config.end_time == scenario.end_date

    def test_to_pnl_config_zero_warmup_uses_start_date(self):
        """With warmup_days=0, start_time should equal scenario start_date."""
        scenario = CrisisScenario(
            name="test",
            start_date=datetime(2023, 6, 1, tzinfo=UTC),
            end_date=datetime(2023, 6, 8, tzinfo=UTC),
            description="Test",
            warmup_days=0,
        )
        config = CrisisBacktestConfig(scenario=scenario)
        pnl_config = config.to_pnl_config()
        assert pnl_config.start_time == scenario.start_date


class TestDateValidationWithWarmup:
    """Test that date validation accounts for warmup period."""

    def _make_backtester(self, provider_class_name="CoinGeckoDataProvider"):
        backtester = MagicMock()
        provider = MagicMock()
        type(provider).__name__ = provider_class_name
        backtester.data_provider = provider
        return backtester

    def test_validation_uses_warmup_start_date(self):
        """Validation should check warmup_start_date, not start_date."""
        now = datetime.now(UTC)
        # Scenario with start_date within 365 days but warmup_start_date outside
        scenario = CrisisScenario(
            name="edge_case",
            start_date=now - timedelta(days=340),
            end_date=now - timedelta(days=333),
            description="Test",
            warmup_days=30,  # warmup_start = 370 days ago > 365
        )
        backtester = self._make_backtester()
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(CrisisScenarioDateRangeError):
                _validate_scenario_date_range(scenario, backtester)

    def test_validation_passes_when_warmup_within_range(self):
        """Scenario with warmup within 365 days should pass."""
        now = datetime.now(UTC)
        scenario = CrisisScenario(
            name="recent",
            start_date=now - timedelta(days=30),
            end_date=now - timedelta(days=23),
            description="Test",
            warmup_days=30,  # warmup_start = 60 days ago < 365
        )
        backtester = self._make_backtester()
        with patch.dict("os.environ", {}, clear=True):
            _validate_scenario_date_range(scenario, backtester)  # Should not raise
