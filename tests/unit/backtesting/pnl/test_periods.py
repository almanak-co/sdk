"""Unit tests for multi-period preset support.

Tests cover:
- BacktestPeriod dataclass
- Quarterly presets (correct dates, correct number of periods)
- Monthly presets (correct dates, correct number of periods)
- Rolling 6M dynamic preset
- resolve_periods() with preset names
- resolve_periods() with JSON file paths
- resolve_periods() error handling (invalid spec, malformed JSON)
- list_presets() returns all available presets
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from almanak.framework.backtesting.pnl.periods import (
    PERIOD_PRESETS,
    BacktestPeriod,
    list_presets,
    resolve_periods,
)


# =============================================================================
# BacktestPeriod Tests
# =============================================================================


class TestBacktestPeriod:
    """Tests for BacktestPeriod dataclass."""

    def test_create_period(self) -> None:
        """Should create a period with name, start, and end."""
        period = BacktestPeriod(
            name="Q1 2024",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 3, 31),
        )
        assert period.name == "Q1 2024"
        assert period.start == datetime(2024, 1, 1)
        assert period.end == datetime(2024, 3, 31)

    def test_period_is_frozen(self) -> None:
        """BacktestPeriod should be immutable (frozen dataclass)."""
        period = BacktestPeriod(
            name="Q1",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 3, 31),
        )
        with pytest.raises(AttributeError):
            period.name = "modified"  # type: ignore[misc]


# =============================================================================
# Static Preset Tests
# =============================================================================


class TestQuarterlyPresets:
    """Tests for quarterly period presets."""

    @pytest.mark.parametrize("year", [2023, 2024, 2025])
    def test_quarterly_has_four_periods(self, year: int) -> None:
        """Each quarterly preset should have exactly 4 periods."""
        periods = PERIOD_PRESETS[f"{year}-quarterly"]
        assert len(periods) == 4

    def test_2024_quarterly_dates(self) -> None:
        """2024-quarterly should have correct quarter boundaries."""
        periods = PERIOD_PRESETS["2024-quarterly"]

        assert periods[0].name == "2024 Q1"
        assert periods[0].start == datetime(2024, 1, 1)
        assert periods[0].end == datetime(2024, 3, 31, 23, 59, 59)

        assert periods[1].name == "2024 Q2"
        assert periods[1].start == datetime(2024, 4, 1)
        assert periods[1].end == datetime(2024, 6, 30, 23, 59, 59)

        assert periods[2].name == "2024 Q3"
        assert periods[2].start == datetime(2024, 7, 1)
        assert periods[2].end == datetime(2024, 9, 30, 23, 59, 59)

        assert periods[3].name == "2024 Q4"
        assert periods[3].start == datetime(2024, 10, 1)
        assert periods[3].end == datetime(2024, 12, 31, 23, 59, 59)

    @pytest.mark.parametrize("year", [2023, 2024, 2025])
    def test_quarterly_periods_cover_full_year(self, year: int) -> None:
        """Quarterly periods should start Jan 1 and end Dec 31."""
        periods = PERIOD_PRESETS[f"{year}-quarterly"]
        assert periods[0].start.month == 1
        assert periods[0].start.day == 1
        assert periods[-1].end.month == 12
        assert periods[-1].end.day == 31


class TestMonthlyPresets:
    """Tests for monthly period presets."""

    @pytest.mark.parametrize("year", [2023, 2024, 2025])
    def test_monthly_has_twelve_periods(self, year: int) -> None:
        """Each monthly preset should have exactly 12 periods."""
        periods = PERIOD_PRESETS[f"{year}-monthly"]
        assert len(periods) == 12

    def test_2024_monthly_january(self) -> None:
        """January 2024 should start on Jan 1 and end on Jan 31."""
        periods = PERIOD_PRESETS["2024-monthly"]
        jan = periods[0]
        assert jan.name == "2024-01"
        assert jan.start == datetime(2024, 1, 1)
        assert jan.end == datetime(2024, 1, 31, 23, 59, 59)

    def test_2024_monthly_february_leap_year(self) -> None:
        """February 2024 should end on Feb 29 (leap year)."""
        periods = PERIOD_PRESETS["2024-monthly"]
        feb = periods[1]
        assert feb.name == "2024-02"
        assert feb.end == datetime(2024, 2, 29, 23, 59, 59)

    def test_2023_monthly_february_non_leap_year(self) -> None:
        """February 2023 should end on Feb 28 (non-leap year)."""
        periods = PERIOD_PRESETS["2023-monthly"]
        feb = periods[1]
        assert feb.end == datetime(2023, 2, 28, 23, 59, 59)


# =============================================================================
# Dynamic Preset Tests
# =============================================================================


class TestRolling6MPreset:
    """Tests for the rolling-6m dynamic preset."""

    def test_rolling_6m_returns_six_periods(self) -> None:
        """rolling-6m should return exactly 6 periods."""
        periods = resolve_periods("rolling-6m")
        assert len(periods) == 6

    def test_rolling_6m_periods_are_chronological(self) -> None:
        """Periods should be in chronological order (oldest first)."""
        periods = resolve_periods("rolling-6m")
        for i in range(1, len(periods)):
            assert periods[i].start > periods[i - 1].start

    def test_rolling_6m_periods_have_names(self) -> None:
        """Each period should have a descriptive name."""
        periods = resolve_periods("rolling-6m")
        for period in periods:
            assert period.name.startswith("6M-")
            assert "to" in period.name


# =============================================================================
# resolve_periods() Tests
# =============================================================================


class TestResolvePeriods:
    """Tests for resolve_periods() function."""

    def test_resolve_static_preset(self) -> None:
        """Should resolve a static preset name."""
        periods = resolve_periods("2024-quarterly")
        assert len(periods) == 4
        assert periods[0].name == "2024 Q1"

    def test_resolve_dynamic_preset(self) -> None:
        """Should resolve a dynamic preset name."""
        periods = resolve_periods("rolling-6m")
        assert len(periods) == 6

    def test_resolve_json_file(self) -> None:
        """Should load periods from a valid JSON file."""
        data = [
            {"name": "Bull Run", "start": "2024-01-01", "end": "2024-03-31"},
            {"name": "Bear Market", "start": "2024-04-01", "end": "2024-06-30"},
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()
            periods = resolve_periods(f.name)

        assert len(periods) == 2
        assert periods[0].name == "Bull Run"
        assert periods[0].start == datetime(2024, 1, 1)
        assert periods[1].name == "Bear Market"
        assert periods[1].end == datetime(2024, 6, 30, 23, 59, 59)

    def test_resolve_unknown_spec_raises(self) -> None:
        """Should raise ValueError for unknown spec."""
        with pytest.raises(ValueError, match="Unknown period spec"):
            resolve_periods("nonexistent-preset")

    def test_resolve_json_invalid_format_raises(self) -> None:
        """Should raise ValueError for JSON that isn't a list."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"not": "a list"}, f)
            f.flush()
            with pytest.raises(ValueError, match="must be a list"):
                resolve_periods(f.name)

    def test_resolve_json_missing_key_raises(self) -> None:
        """Should raise ValueError for JSON entries missing required keys."""
        data = [{"name": "Missing dates"}]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()
            with pytest.raises(ValueError, match="missing required key"):
                resolve_periods(f.name)

    def test_resolve_json_start_after_end_raises(self) -> None:
        """Should raise ValueError when start >= end."""
        data = [{"name": "Backwards", "start": "2024-06-01", "end": "2024-01-01"}]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()
            with pytest.raises(ValueError, match="must be before end"):
                resolve_periods(f.name)

    def test_resolve_json_empty_list_raises(self) -> None:
        """Should raise ValueError for empty period list."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([], f)
            f.flush()
            with pytest.raises(ValueError, match="empty"):
                resolve_periods(f.name)


# =============================================================================
# list_presets() Tests
# =============================================================================


class TestListPresets:
    """Tests for list_presets() function."""

    def test_returns_sorted_list(self) -> None:
        """Should return a sorted list of preset names."""
        presets = list_presets()
        assert presets == sorted(presets)

    def test_includes_static_presets(self) -> None:
        """Should include all static preset names."""
        presets = list_presets()
        assert "2024-quarterly" in presets
        assert "2024-monthly" in presets

    def test_includes_dynamic_presets(self) -> None:
        """Should include dynamic preset names."""
        presets = list_presets()
        assert "rolling-6m" in presets

    def test_has_expected_count(self) -> None:
        """Should include all presets (6 static + 1 dynamic)."""
        presets = list_presets()
        # 3 years * 2 types (quarterly, monthly) + rolling-6m = 7
        assert len(presets) == 7
