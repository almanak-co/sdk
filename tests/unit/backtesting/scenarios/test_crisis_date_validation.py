"""Tests for crisis scenario date range validation against CoinGecko free tier."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.backtesting.scenarios.crisis import (
    BLACK_THURSDAY,
    FTX_COLLAPSE,
    TERRA_COLLAPSE,
    CrisisScenario,
)
from almanak.framework.backtesting.scenarios.crisis_runner import (
    CrisisScenarioDateRangeError,
    _validate_scenario_date_range,
)


def _make_backtester(provider_class_name: str = "CoinGeckoDataProvider") -> MagicMock:
    """Create a mock backtester with a data provider of the given class name."""
    backtester = MagicMock()
    provider = MagicMock()
    type(provider).__name__ = provider_class_name
    backtester.data_provider = provider
    return backtester


class TestCrisisDateValidation:
    """Test date range validation for crisis scenarios."""

    def test_predefined_scenarios_fail_without_api_key(self):
        """All predefined scenarios are >365 days old and should fail without API key."""
        backtester = _make_backtester("CoinGeckoDataProvider")
        with patch.dict("os.environ", {}, clear=True):
            for scenario in [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]:
                with pytest.raises(CrisisScenarioDateRangeError) as exc_info:
                    _validate_scenario_date_range(scenario, backtester)
                assert "CoinGecko free tier" in str(exc_info.value)
                assert "COINGECKO_API_KEY" in str(exc_info.value)

    def test_predefined_scenarios_pass_with_api_key(self):
        """Predefined scenarios should pass when COINGECKO_API_KEY is set."""
        backtester = _make_backtester("CoinGeckoDataProvider")
        with patch.dict("os.environ", {"COINGECKO_API_KEY": "test-key"}):
            for scenario in [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]:
                _validate_scenario_date_range(scenario, backtester)  # Should not raise

    def test_recent_custom_scenario_passes(self):
        """A custom scenario within 365 days should pass without API key."""
        backtester = _make_backtester("CoinGeckoDataProvider")
        now = datetime.now(UTC)
        recent_scenario = CrisisScenario(
            name="recent_test",
            start_date=now - timedelta(days=30),
            end_date=now - timedelta(days=23),
            description="Test scenario within free tier range",
        )
        with patch.dict("os.environ", {}, clear=True):
            _validate_scenario_date_range(recent_scenario, backtester)  # Should not raise

    def test_error_message_includes_scenario_name(self):
        """Error message should include the scenario name for clarity."""
        backtester = _make_backtester("CoinGeckoDataProvider")
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(CrisisScenarioDateRangeError) as exc_info:
                _validate_scenario_date_range(BLACK_THURSDAY, backtester)
            assert "black_thursday" in str(exc_info.value)

    def test_error_message_includes_actionable_fixes(self):
        """Error message should tell users how to fix the problem."""
        backtester = _make_backtester("CoinGeckoDataProvider")
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(CrisisScenarioDateRangeError) as exc_info:
                _validate_scenario_date_range(BLACK_THURSDAY, backtester)
            error_msg = str(exc_info.value)
            assert "COINGECKO_API_KEY" in error_msg
            assert "custom scenario" in error_msg

    def test_non_coingecko_provider_skips_validation(self):
        """Non-CoinGecko providers should skip date validation entirely."""
        backtester = _make_backtester("CrisisDataProvider")
        with patch.dict("os.environ", {}, clear=True):
            for scenario in [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]:
                _validate_scenario_date_range(scenario, backtester)  # Should not raise

    def test_gateway_coingecko_provider_validates(self):
        """GatewayCoinGeckoDataProvider should also trigger validation."""
        backtester = _make_backtester("GatewayCoinGeckoDataProvider")
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(CrisisScenarioDateRangeError):
                _validate_scenario_date_range(BLACK_THURSDAY, backtester)


class TestCrisisBacktestResultDict:
    """Test that CrisisBacktestResult.to_dict includes success/error fields."""

    def test_to_dict_has_success_field(self):
        """CrisisBacktestResult.to_dict() should have top-level success field."""
        from almanak.framework.backtesting.scenarios.crisis_runner import CrisisBacktestResult

        mock_result = MagicMock()
        mock_result.to_dict.return_value = {"metrics": {}}
        mock_result.success = True
        mock_result.error = None

        crisis_result = CrisisBacktestResult(
            result=mock_result,
            scenario=BLACK_THURSDAY,
            crisis_metrics={},
        )

        result_dict = crisis_result.to_dict()
        assert "success" in result_dict
        assert result_dict["success"] is True
        assert "error" in result_dict
        assert result_dict["error"] is None
