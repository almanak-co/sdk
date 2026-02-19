"""Tests for receipt_utils module."""

import logging

import pytest

from almanak.framework.backtesting.pnl.receipt_utils import (
    DEFAULT_DISCREPANCY_THRESHOLD,
    DiscrepancyResult,
    calculate_discrepancy,
)


class TestCalculateDiscrepancy:
    """Tests for calculate_discrepancy function."""

    def test_no_discrepancy(self):
        """Test when expected equals actual."""
        result = calculate_discrepancy(expected=1000, actual=1000, log_warning=False)

        assert result.expected == 1000
        assert result.actual == 1000
        assert result.difference == 0
        assert result.percentage == 0.0
        assert result.exceeds_threshold is False
        assert result.threshold == DEFAULT_DISCREPANCY_THRESHOLD

    def test_discrepancy_below_threshold(self):
        """Test discrepancy below the 1% default threshold."""
        # 0.5% discrepancy (995 vs 1000)
        result = calculate_discrepancy(expected=1000, actual=995, log_warning=False)

        assert result.expected == 1000
        assert result.actual == 995
        assert result.difference == -5
        assert result.percentage == 0.005
        assert result.exceeds_threshold is False

    def test_discrepancy_above_threshold(self):
        """Test discrepancy above the 1% default threshold."""
        # 2% discrepancy (980 vs 1000)
        result = calculate_discrepancy(expected=1000, actual=980, log_warning=False)

        assert result.expected == 1000
        assert result.actual == 980
        assert result.difference == -20
        assert result.percentage == 0.02
        assert result.exceeds_threshold is True

    def test_discrepancy_exactly_at_threshold(self):
        """Test discrepancy exactly at threshold (should not exceed)."""
        # Exactly 1% discrepancy
        result = calculate_discrepancy(expected=1000, actual=990, log_warning=False)

        assert result.percentage == 0.01
        assert result.exceeds_threshold is False  # 0.01 is not > 0.01

    def test_positive_discrepancy(self):
        """Test when actual is greater than expected."""
        result = calculate_discrepancy(expected=1000, actual=1050, log_warning=False)

        assert result.difference == 50
        assert result.percentage == 0.05
        assert result.exceeds_threshold is True

    def test_custom_threshold(self):
        """Test with a custom threshold."""
        # 2% discrepancy with 5% threshold
        result = calculate_discrepancy(
            expected=1000, actual=980, threshold=0.05, log_warning=False
        )

        assert result.percentage == 0.02
        assert result.threshold == 0.05
        assert result.exceeds_threshold is False

    def test_zero_expected(self):
        """Test handling of zero expected value."""
        # When expected is 0 and actual is not, that's 100% discrepancy
        result = calculate_discrepancy(expected=0, actual=100, log_warning=False)

        assert result.percentage == 1.0
        assert result.exceeds_threshold is True

    def test_both_zero(self):
        """Test when both expected and actual are zero."""
        result = calculate_discrepancy(expected=0, actual=0, log_warning=False)

        assert result.percentage == 0.0
        assert result.exceeds_threshold is False

    def test_float_values(self):
        """Test with float values."""
        result = calculate_discrepancy(
            expected=100.5, actual=99.5, log_warning=False
        )

        assert result.expected == 100.5
        assert result.actual == 99.5
        assert result.difference == -1.0
        assert pytest.approx(result.percentage, abs=0.0001) == 0.00995

    def test_warning_logged_when_threshold_exceeded(self, caplog):
        """Test that a warning is logged when threshold is exceeded."""
        with caplog.at_level(logging.WARNING):
            calculate_discrepancy(
                expected=1000,
                actual=900,  # 10% discrepancy
                log_warning=True,
            )

        assert len(caplog.records) == 1
        assert "Execution discrepancy" in caplog.records[0].message
        assert "expected=1000" in caplog.records[0].message
        assert "actual=900" in caplog.records[0].message
        assert "10.00%" in caplog.records[0].message

    def test_no_warning_logged_below_threshold(self, caplog):
        """Test that no warning is logged when below threshold."""
        with caplog.at_level(logging.WARNING):
            calculate_discrepancy(
                expected=1000,
                actual=995,  # 0.5% discrepancy
                log_warning=True,
            )

        assert len(caplog.records) == 0

    def test_warning_with_context(self, caplog):
        """Test that context is included in warning message."""
        with caplog.at_level(logging.WARNING):
            calculate_discrepancy(
                expected=1000,
                actual=900,
                log_warning=True,
                context="USDC swap",
            )

        assert "[USDC swap]" in caplog.records[0].message

    def test_log_warning_disabled(self, caplog):
        """Test that log_warning=False suppresses warning."""
        with caplog.at_level(logging.WARNING):
            result = calculate_discrepancy(
                expected=1000,
                actual=900,  # 10% discrepancy
                log_warning=False,
            )

        assert result.exceeds_threshold is True
        assert len(caplog.records) == 0


class TestDiscrepancyResultToDict:
    """Tests for DiscrepancyResult.to_dict() method."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        result = DiscrepancyResult(
            expected=1000,
            actual=980,
            difference=-20,
            percentage=0.02,
            exceeds_threshold=True,
            threshold=0.01,
        )

        data = result.to_dict()

        assert data["expected"] == "1000"
        assert data["actual"] == "980"
        assert data["difference"] == "-20"
        assert data["percentage"] == 0.02
        assert data["exceeds_threshold"] is True
        assert data["threshold"] == 0.01

    def test_to_dict_with_floats(self):
        """Test serialization with float values."""
        result = DiscrepancyResult(
            expected=100.5,
            actual=99.5,
            difference=-1.0,
            percentage=0.00995,
            exceeds_threshold=False,
            threshold=0.01,
        )

        data = result.to_dict()

        assert data["expected"] == "100.5"
        assert data["actual"] == "99.5"
        assert data["difference"] == "-1.0"
