"""Unit tests for walk-forward optimization splitting.

Tests cover:
- Basic window generation with various configurations
- Overlapping vs non-overlapping (anchored) windows
- Edge cases: minimum data, exact fit, insufficient data
- Gap handling between train and test
- Serialization and deserialization
- Walk-forward optimization loop results
"""

from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.backtesting.pnl.walk_forward import (
    ParameterStability,
    WalkForwardConfig,
    WalkForwardResult,
    WalkForwardWindow,
    WalkForwardWindowResult,
    calculate_parameter_stability,
    split_walk_forward,
    split_walk_forward_tuples,
)


class TestWalkForwardWindow:
    """Tests for WalkForwardWindow dataclass."""

    def test_basic_creation(self) -> None:
        """Test creating a basic window."""
        window = WalkForwardWindow(
            window_index=0,
            train_start=datetime(2023, 1, 1),
            train_end=datetime(2023, 4, 1),
            test_start=datetime(2023, 4, 1),
            test_end=datetime(2023, 5, 1),
        )
        assert window.window_index == 0
        assert window.train_start == datetime(2023, 1, 1)
        assert window.train_end == datetime(2023, 4, 1)
        assert window.test_start == datetime(2023, 4, 1)
        assert window.test_end == datetime(2023, 5, 1)

    def test_train_duration(self) -> None:
        """Test train_duration property."""
        window = WalkForwardWindow(
            window_index=0,
            train_start=datetime(2023, 1, 1),
            train_end=datetime(2023, 4, 1),  # 90 days
            test_start=datetime(2023, 4, 1),
            test_end=datetime(2023, 5, 1),
        )
        assert window.train_duration == timedelta(days=90)

    def test_test_duration(self) -> None:
        """Test test_duration property."""
        window = WalkForwardWindow(
            window_index=0,
            train_start=datetime(2023, 1, 1),
            train_end=datetime(2023, 4, 1),
            test_start=datetime(2023, 4, 1),
            test_end=datetime(2023, 5, 1),  # 30 days
        )
        assert window.test_duration == timedelta(days=30)

    def test_gap_duration_no_gap(self) -> None:
        """Test gap_duration when train_end == test_start."""
        window = WalkForwardWindow(
            window_index=0,
            train_start=datetime(2023, 1, 1),
            train_end=datetime(2023, 4, 1),
            test_start=datetime(2023, 4, 1),
            test_end=datetime(2023, 5, 1),
        )
        assert window.gap_duration == timedelta(0)

    def test_gap_duration_with_gap(self) -> None:
        """Test gap_duration when there's a gap."""
        window = WalkForwardWindow(
            window_index=0,
            train_start=datetime(2023, 1, 1),
            train_end=datetime(2023, 4, 1),
            test_start=datetime(2023, 4, 3),  # 2-day gap
            test_end=datetime(2023, 5, 3),
        )
        assert window.gap_duration == timedelta(days=2)

    def test_validation_train_start_after_train_end(self) -> None:
        """Test validation rejects train_start >= train_end."""
        with pytest.raises(ValueError, match="train_start.*must be before.*train_end"):
            WalkForwardWindow(
                window_index=0,
                train_start=datetime(2023, 4, 1),
                train_end=datetime(2023, 1, 1),  # Before train_start
                test_start=datetime(2023, 5, 1),
                test_end=datetime(2023, 6, 1),
            )

    def test_validation_test_start_after_test_end(self) -> None:
        """Test validation rejects test_start >= test_end."""
        with pytest.raises(ValueError, match="test_start.*must be before.*test_end"):
            WalkForwardWindow(
                window_index=0,
                train_start=datetime(2023, 1, 1),
                train_end=datetime(2023, 4, 1),
                test_start=datetime(2023, 6, 1),
                test_end=datetime(2023, 5, 1),  # Before test_start
            )

    def test_validation_train_end_after_test_start(self) -> None:
        """Test validation rejects train_end > test_start (data leakage)."""
        with pytest.raises(ValueError, match="train_end.*must not be after.*test_start"):
            WalkForwardWindow(
                window_index=0,
                train_start=datetime(2023, 1, 1),
                train_end=datetime(2023, 5, 1),  # After test_start
                test_start=datetime(2023, 4, 1),
                test_end=datetime(2023, 6, 1),
            )

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        window = WalkForwardWindow(
            window_index=2,
            train_start=datetime(2023, 1, 1),
            train_end=datetime(2023, 4, 1),
            test_start=datetime(2023, 4, 1),
            test_end=datetime(2023, 5, 1),
        )
        d = window.to_dict()
        assert d["window_index"] == 2
        assert d["train_start"] == "2023-01-01T00:00:00"
        assert d["train_end"] == "2023-04-01T00:00:00"
        assert d["test_start"] == "2023-04-01T00:00:00"
        assert d["test_end"] == "2023-05-01T00:00:00"
        assert d["train_duration_days"] == 90
        assert d["test_duration_days"] == 30

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        d = {
            "window_index": 2,
            "train_start": "2023-01-01T00:00:00",
            "train_end": "2023-04-01T00:00:00",
            "test_start": "2023-04-01T00:00:00",
            "test_end": "2023-05-01T00:00:00",
        }
        window = WalkForwardWindow.from_dict(d)
        assert window.window_index == 2
        assert window.train_start == datetime(2023, 1, 1)
        assert window.train_end == datetime(2023, 4, 1)
        assert window.test_start == datetime(2023, 4, 1)
        assert window.test_end == datetime(2023, 5, 1)

    def test_to_tuple(self) -> None:
        """Test conversion to tuple."""
        window = WalkForwardWindow(
            window_index=0,
            train_start=datetime(2023, 1, 1),
            train_end=datetime(2023, 4, 1),
            test_start=datetime(2023, 4, 1),
            test_end=datetime(2023, 5, 1),
        )
        t = window.to_tuple()
        assert t == (
            datetime(2023, 1, 1),
            datetime(2023, 4, 1),
            datetime(2023, 4, 1),
            datetime(2023, 5, 1),
        )

    def test_roundtrip_serialization(self) -> None:
        """Test to_dict -> from_dict roundtrip."""
        original = WalkForwardWindow(
            window_index=5,
            train_start=datetime(2023, 7, 1),
            train_end=datetime(2023, 10, 1),
            test_start=datetime(2023, 10, 5),  # With gap
            test_end=datetime(2023, 11, 4),
        )
        restored = WalkForwardWindow.from_dict(original.to_dict())
        assert restored.window_index == original.window_index
        assert restored.train_start == original.train_start
        assert restored.train_end == original.train_end
        assert restored.test_start == original.test_start
        assert restored.test_end == original.test_end


class TestWalkForwardConfig:
    """Tests for WalkForwardConfig dataclass."""

    def test_basic_creation(self) -> None:
        """Test creating a basic config."""
        config = WalkForwardConfig(
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
        )
        assert config.train_size == timedelta(days=90)
        assert config.test_size == timedelta(days=30)
        assert config.step == timedelta(days=30)  # Default = test_size
        assert config.gap == timedelta(0)
        assert config.min_windows == 2

    def test_explicit_step(self) -> None:
        """Test config with explicit step."""
        config = WalkForwardConfig(
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            step=timedelta(days=7),
        )
        assert config.step == timedelta(days=7)

    def test_with_gap(self) -> None:
        """Test config with gap."""
        config = WalkForwardConfig(
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            gap=timedelta(days=2),
        )
        assert config.gap == timedelta(days=2)

    def test_window_size(self) -> None:
        """Test window_size property."""
        config = WalkForwardConfig(
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            gap=timedelta(days=5),
        )
        assert config.window_size == timedelta(days=125)  # 90 + 5 + 30

    def test_is_overlapping_true(self) -> None:
        """Test is_overlapping when step < test_size."""
        config = WalkForwardConfig(
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            step=timedelta(days=7),
        )
        assert config.is_overlapping is True
        assert config.is_anchored is False

    def test_is_anchored_true(self) -> None:
        """Test is_anchored when step == test_size."""
        config = WalkForwardConfig(
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            step=timedelta(days=30),  # Same as test_size
        )
        assert config.is_anchored is True
        assert config.is_overlapping is False

    def test_validation_train_size_positive(self) -> None:
        """Test validation rejects non-positive train_size."""
        with pytest.raises(ValueError, match="train_size must be positive"):
            WalkForwardConfig(
                train_size=timedelta(0),
                test_size=timedelta(days=30),
            )

    def test_validation_test_size_positive(self) -> None:
        """Test validation rejects non-positive test_size."""
        with pytest.raises(ValueError, match="test_size must be positive"):
            WalkForwardConfig(
                train_size=timedelta(days=90),
                test_size=timedelta(0),
            )

    def test_validation_step_positive(self) -> None:
        """Test validation rejects non-positive step."""
        with pytest.raises(ValueError, match="step must be positive"):
            WalkForwardConfig(
                train_size=timedelta(days=90),
                test_size=timedelta(days=30),
                step=timedelta(0),
            )

    def test_validation_gap_non_negative(self) -> None:
        """Test validation rejects negative gap."""
        with pytest.raises(ValueError, match="gap must be non-negative"):
            WalkForwardConfig(
                train_size=timedelta(days=90),
                test_size=timedelta(days=30),
                gap=timedelta(days=-1),
            )

    def test_validation_min_windows(self) -> None:
        """Test validation rejects min_windows < 1."""
        with pytest.raises(ValueError, match="min_windows must be at least 1"):
            WalkForwardConfig(
                train_size=timedelta(days=90),
                test_size=timedelta(days=30),
                min_windows=0,
            )

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        config = WalkForwardConfig(
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            step=timedelta(days=15),
            gap=timedelta(days=2),
            min_windows=3,
        )
        d = config.to_dict()
        assert d["train_size_seconds"] == 90 * 86400
        assert d["test_size_seconds"] == 30 * 86400
        assert d["step_seconds"] == 15 * 86400
        assert d["gap_seconds"] == 2 * 86400
        assert d["min_windows"] == 3

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        d = {
            "train_size_seconds": 90 * 86400,
            "test_size_seconds": 30 * 86400,
            "step_seconds": 15 * 86400,
            "gap_seconds": 2 * 86400,
            "min_windows": 3,
        }
        config = WalkForwardConfig.from_dict(d)
        assert config.train_size == timedelta(days=90)
        assert config.test_size == timedelta(days=30)
        assert config.step == timedelta(days=15)
        assert config.gap == timedelta(days=2)
        assert config.min_windows == 3

    def test_from_days(self) -> None:
        """Test factory method from_days."""
        config = WalkForwardConfig.from_days(
            train_days=90,
            test_days=30,
            step_days=7,
            gap_days=1,
            min_windows=5,
        )
        assert config.train_size == timedelta(days=90)
        assert config.test_size == timedelta(days=30)
        assert config.step == timedelta(days=7)
        assert config.gap == timedelta(days=1)
        assert config.min_windows == 5

    def test_from_days_defaults(self) -> None:
        """Test from_days with default values."""
        config = WalkForwardConfig.from_days(90, 30)
        assert config.step == timedelta(days=30)  # Default = test_size
        assert config.gap == timedelta(0)
        assert config.min_windows == 2

    def test_roundtrip_serialization(self) -> None:
        """Test to_dict -> from_dict roundtrip."""
        original = WalkForwardConfig(
            train_size=timedelta(days=120),
            test_size=timedelta(days=45),
            step=timedelta(days=20),
            gap=timedelta(hours=12),
            min_windows=4,
        )
        restored = WalkForwardConfig.from_dict(original.to_dict())
        assert restored.train_size == original.train_size
        assert restored.test_size == original.test_size
        assert restored.step == original.step
        # Note: timedelta hours may have floating point differences
        assert abs(restored.gap.total_seconds() - original.gap.total_seconds()) < 1
        assert restored.min_windows == original.min_windows


class TestSplitWalkForward:
    """Tests for split_walk_forward function."""

    def test_basic_split_non_overlapping(self) -> None:
        """Test basic non-overlapping (anchored) split."""
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            min_windows=1,
        )
        # 365 days with 90+30=120 day windows, step=30 -> multiple windows
        assert len(windows) >= 2

        # First window
        assert windows[0].window_index == 0
        assert windows[0].train_start == datetime(2023, 1, 1)
        assert windows[0].train_end == datetime(2023, 4, 1)
        assert windows[0].test_start == datetime(2023, 4, 1)
        assert windows[0].test_end == datetime(2023, 5, 1)

    def test_split_with_config_object(self) -> None:
        """Test split using config object."""
        config = WalkForwardConfig.from_days(90, 30)
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            config=config,
        )
        assert len(windows) >= 2
        assert all(w.train_duration == timedelta(days=90) for w in windows)
        assert all(w.test_duration == timedelta(days=30) for w in windows)

    def test_overlapping_windows(self) -> None:
        """Test overlapping windows with small step."""
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            step=timedelta(days=7),  # Weekly step -> many windows
            min_windows=1,
        )
        # With 7-day step, should have many more windows
        assert len(windows) > 10

        # Check overlap: second window starts only 7 days after first
        assert windows[1].train_start == windows[0].train_start + timedelta(days=7)

    def test_windows_with_gap(self) -> None:
        """Test windows with gap between train and test."""
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            gap=timedelta(days=5),
            min_windows=1,
        )
        # All windows should have 5-day gap
        for w in windows:
            assert w.gap_duration == timedelta(days=5)
            assert w.test_start == w.train_end + timedelta(days=5)

    def test_exact_fit_windows(self) -> None:
        """Test when data fits exactly for a certain number of windows."""
        # 120 days = exactly one window of 90+30
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2023, 5, 1),  # 120 days
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            min_windows=1,
        )
        assert len(windows) == 1

    def test_insufficient_data_for_min_windows(self) -> None:
        """Test error when data is insufficient for min_windows."""
        with pytest.raises(ValueError, match="Only 1 windows can be created.*min_windows=2"):
            split_walk_forward(
                start_date=datetime(2023, 1, 1),
                end_date=datetime(2023, 5, 1),  # 120 days = 1 window only
                train_size=timedelta(days=90),
                test_size=timedelta(days=30),
                min_windows=2,  # Requires at least 2
            )

    def test_insufficient_data_for_one_window(self) -> None:
        """Test error when data is too short for even one window."""
        with pytest.raises(ValueError, match="Date range.*is shorter than.*one window"):
            split_walk_forward(
                start_date=datetime(2023, 1, 1),
                end_date=datetime(2023, 2, 1),  # Only 31 days
                train_size=timedelta(days=90),  # Need 90+30=120 days
                test_size=timedelta(days=30),
                min_windows=1,
            )

    def test_validation_start_before_end(self) -> None:
        """Test error when start_date >= end_date."""
        with pytest.raises(ValueError, match="start_date.*must be before.*end_date"):
            split_walk_forward(
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2023, 1, 1),
                train_size=timedelta(days=90),
                test_size=timedelta(days=30),
            )

    def test_requires_train_and_test_size(self) -> None:
        """Test error when neither config nor both sizes provided."""
        with pytest.raises(ValueError, match="Either 'config' or both 'train_size' and 'test_size'"):
            split_walk_forward(
                start_date=datetime(2023, 1, 1),
                end_date=datetime(2024, 1, 1),
                train_size=timedelta(days=90),
                # Missing test_size
            )

    def test_window_indices_sequential(self) -> None:
        """Test that window indices are sequential."""
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            min_windows=1,
        )
        indices = [w.window_index for w in windows]
        assert indices == list(range(len(windows)))

    def test_no_data_leakage(self) -> None:
        """Test that train_end never exceeds test_start."""
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            step=timedelta(days=7),
            min_windows=1,
        )
        for w in windows:
            assert w.train_end <= w.test_start

    def test_test_windows_within_date_range(self) -> None:
        """Test that all test windows end within the date range."""
        end_date = datetime(2024, 1, 1)
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=end_date,
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            min_windows=1,
        )
        for w in windows:
            assert w.test_end <= end_date

    def test_large_step_fewer_windows(self) -> None:
        """Test that larger step produces fewer windows."""
        small_step = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=60),
            test_size=timedelta(days=30),
            step=timedelta(days=15),
            min_windows=1,
        )
        large_step = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=60),
            test_size=timedelta(days=30),
            step=timedelta(days=60),
            min_windows=1,
        )
        assert len(small_step) > len(large_step)


class TestSplitWalkForwardTuples:
    """Tests for split_walk_forward_tuples function."""

    def test_returns_tuples(self) -> None:
        """Test that function returns list of tuples."""
        tuples = split_walk_forward_tuples(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            min_windows=1,
        )
        assert isinstance(tuples, list)
        assert all(isinstance(t, tuple) for t in tuples)
        assert all(len(t) == 4 for t in tuples)

    def test_tuple_format(self) -> None:
        """Test that tuples have correct format."""
        tuples = split_walk_forward_tuples(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            min_windows=1,
        )
        train_start, train_end, test_start, test_end = tuples[0]
        assert train_start == datetime(2023, 1, 1)
        assert train_end == datetime(2023, 4, 1)
        assert test_start == datetime(2023, 4, 1)
        assert test_end == datetime(2023, 5, 1)

    def test_equivalent_to_window_to_tuple(self) -> None:
        """Test that results match window.to_tuple()."""
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            min_windows=1,
        )
        tuples = split_walk_forward_tuples(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            min_windows=1,
        )
        assert len(windows) == len(tuples)
        for window, tup in zip(windows, tuples, strict=False):
            assert window.to_tuple() == tup


class TestWalkForwardIntegration:
    """Integration tests for walk-forward splitting."""

    def test_yearly_backtest_quarterly_optimization(self) -> None:
        """Test typical yearly backtest with quarterly optimization."""
        # Common setup: 3 months train, 1 month test, rolling monthly
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),  # ~3 months
            test_size=timedelta(days=30),   # ~1 month
            step=timedelta(days=30),        # Monthly step
            min_windows=1,
        )
        # 365 days, first window needs 120 days, then 30-day steps
        # Should get roughly (365 - 120) / 30 + 1 = ~9 windows
        assert 8 <= len(windows) <= 10

    def test_multi_year_backtest(self) -> None:
        """Test multi-year backtest with many windows."""
        windows = split_walk_forward(
            start_date=datetime(2020, 1, 1),
            end_date=datetime(2024, 1, 1),  # 4 years
            train_size=timedelta(days=180),  # 6 months
            test_size=timedelta(days=90),    # 3 months
            step=timedelta(days=90),         # Quarterly step
            min_windows=1,
        )
        # 4 years = ~1460 days
        # First window: 180 + 90 = 270 days
        # Then 90-day steps: (1460 - 270) / 90 + 1 = ~14 windows
        assert len(windows) >= 10

    def test_weekly_rolling_windows(self) -> None:
        """Test weekly rolling walk-forward."""
        windows = split_walk_forward(
            start_date=datetime(2023, 6, 1),
            end_date=datetime(2023, 12, 31),  # ~7 months
            train_size=timedelta(days=30),    # 1 month train
            test_size=timedelta(days=7),      # 1 week test
            step=timedelta(days=7),           # Weekly roll
            min_windows=1,
        )
        # ~210 days, 37-day windows, weekly steps
        # Should get many windows with lots of overlap
        assert len(windows) > 20

    def test_all_windows_valid_for_backtest(self) -> None:
        """Test that all windows could be used for actual backtesting."""
        windows = split_walk_forward(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            min_windows=1,
        )

        for w in windows:
            # Training period is valid
            assert w.train_start < w.train_end
            assert w.train_duration.days >= 1

            # Test period is valid
            assert w.test_start < w.test_end
            assert w.test_duration.days >= 1

            # No data leakage
            assert w.train_end <= w.test_start

            # Window is within overall date range
            assert w.train_start >= datetime(2023, 1, 1)
            assert w.test_end <= datetime(2024, 1, 1)


# =============================================================================
# Tests for Walk-Forward Optimization Results
# =============================================================================


class TestWalkForwardWindowResult:
    """Tests for WalkForwardWindowResult dataclass."""

    @pytest.fixture
    def mock_window(self) -> WalkForwardWindow:
        """Create a mock window for testing."""
        return WalkForwardWindow(
            window_index=0,
            train_start=datetime(2023, 1, 1),
            train_end=datetime(2023, 4, 1),
            test_start=datetime(2023, 4, 1),
            test_end=datetime(2023, 5, 1),
        )

    @pytest.fixture
    def mock_optimization_result(self) -> MagicMock:
        """Create a mock optimization result."""
        result = MagicMock()
        result.best_params = {"initial_capital_usd": Decimal("50000")}
        result.best_value = 1.5
        result.best_trial_number = 42
        result.n_trials = 50
        result.study_name = "test_study"
        result.objective_metric = "sharpe_ratio"
        result.direction = "maximize"
        result.to_dict.return_value = {
            "best_params": {"initial_capital_usd": "50000"},
            "best_value": 1.5,
            "best_trial_number": 42,
            "n_trials": 50,
            "study_name": "test_study",
            "objective_metric": "sharpe_ratio",
            "direction": "maximize",
        }
        return result

    @pytest.fixture
    def mock_test_result(self) -> MagicMock:
        """Create a mock backtest result."""
        result = MagicMock()
        result.metrics = MagicMock()
        result.metrics.sharpe_ratio = Decimal("1.2")
        result.metrics.net_pnl_usd = Decimal("5000")
        result.metrics.total_return_pct = Decimal("0.10")
        result.to_dict.return_value = {
            "metrics": {
                "sharpe_ratio": "1.2",
                "net_pnl_usd": "5000",
                "total_return_pct": "0.10",
            }
        }
        return result

    def test_basic_creation(
        self,
        mock_window: WalkForwardWindow,
        mock_optimization_result: MagicMock,
        mock_test_result: MagicMock,
    ) -> None:
        """Test creating a window result."""
        result = WalkForwardWindowResult(
            window=mock_window,
            optimization_result=mock_optimization_result,
            test_result=mock_test_result,
            train_objective_value=1.5,
            test_objective_value=1.2,
            objective_metric="sharpe_ratio",
        )
        assert result.window == mock_window
        assert result.train_objective_value == 1.5
        assert result.test_objective_value == 1.2
        assert result.objective_metric == "sharpe_ratio"

    def test_overfitting_ratio_normal(
        self,
        mock_window: WalkForwardWindow,
        mock_optimization_result: MagicMock,
        mock_test_result: MagicMock,
    ) -> None:
        """Test overfitting ratio calculation."""
        result = WalkForwardWindowResult(
            window=mock_window,
            optimization_result=mock_optimization_result,
            test_result=mock_test_result,
            train_objective_value=1.5,
            test_objective_value=1.0,
            objective_metric="sharpe_ratio",
        )
        assert result.overfitting_ratio == 1.5

    def test_overfitting_ratio_zero_test(
        self,
        mock_window: WalkForwardWindow,
        mock_optimization_result: MagicMock,
        mock_test_result: MagicMock,
    ) -> None:
        """Test overfitting ratio when test is zero."""
        result = WalkForwardWindowResult(
            window=mock_window,
            optimization_result=mock_optimization_result,
            test_result=mock_test_result,
            train_objective_value=1.5,
            test_objective_value=0.0,
            objective_metric="sharpe_ratio",
        )
        assert result.overfitting_ratio == float("inf")

    def test_overfitting_ratio_both_zero(
        self,
        mock_window: WalkForwardWindow,
        mock_optimization_result: MagicMock,
        mock_test_result: MagicMock,
    ) -> None:
        """Test overfitting ratio when both are zero."""
        result = WalkForwardWindowResult(
            window=mock_window,
            optimization_result=mock_optimization_result,
            test_result=mock_test_result,
            train_objective_value=0.0,
            test_objective_value=0.0,
            objective_metric="sharpe_ratio",
        )
        assert result.overfitting_ratio == 0.0

    def test_generalization_score_perfect(
        self,
        mock_window: WalkForwardWindow,
        mock_optimization_result: MagicMock,
        mock_test_result: MagicMock,
    ) -> None:
        """Test generalization score when test >= train."""
        result = WalkForwardWindowResult(
            window=mock_window,
            optimization_result=mock_optimization_result,
            test_result=mock_test_result,
            train_objective_value=1.0,
            test_objective_value=1.2,
            objective_metric="sharpe_ratio",
        )
        assert result.generalization_score == 1.0  # Capped at 1.0

    def test_generalization_score_partial(
        self,
        mock_window: WalkForwardWindow,
        mock_optimization_result: MagicMock,
        mock_test_result: MagicMock,
    ) -> None:
        """Test generalization score with partial generalization."""
        result = WalkForwardWindowResult(
            window=mock_window,
            optimization_result=mock_optimization_result,
            test_result=mock_test_result,
            train_objective_value=2.0,
            test_objective_value=1.0,
            objective_metric="sharpe_ratio",
        )
        assert result.generalization_score == 0.5

    def test_to_dict(
        self,
        mock_window: WalkForwardWindow,
        mock_optimization_result: MagicMock,
        mock_test_result: MagicMock,
    ) -> None:
        """Test serialization to dictionary."""
        result = WalkForwardWindowResult(
            window=mock_window,
            optimization_result=mock_optimization_result,
            test_result=mock_test_result,
            train_objective_value=1.5,
            test_objective_value=1.2,
            objective_metric="sharpe_ratio",
        )
        data = result.to_dict()

        assert "window" in data
        assert "optimization_result" in data
        assert "test_result" in data
        assert data["train_objective_value"] == 1.5
        assert data["test_objective_value"] == 1.2
        assert data["objective_metric"] == "sharpe_ratio"
        assert "overfitting_ratio" in data
        assert "generalization_score" in data


class TestWalkForwardResult:
    """Tests for WalkForwardResult dataclass."""

    @pytest.fixture
    def mock_wf_config(self) -> WalkForwardConfig:
        """Create a mock walk-forward config."""
        return WalkForwardConfig.from_days(90, 30)

    @pytest.fixture
    def mock_window_results(self) -> list[MagicMock]:
        """Create mock window results."""
        results = []
        for i in range(3):
            wr = MagicMock()
            wr.train_objective_value = 1.5 - i * 0.1
            wr.test_objective_value = 1.2 - i * 0.1
            wr.overfitting_ratio = wr.train_objective_value / wr.test_objective_value
            wr.generalization_score = min(
                1.0, wr.test_objective_value / wr.train_objective_value
            )
            wr.test_result = MagicMock()
            wr.test_result.metrics = MagicMock()
            wr.test_result.metrics.net_pnl_usd = Decimal("5000")
            wr.test_result.metrics.total_return_pct = Decimal("0.10")
            wr.to_dict.return_value = {
                "train_objective_value": wr.train_objective_value,
                "test_objective_value": wr.test_objective_value,
            }
            results.append(wr)
        return results

    def test_basic_creation(
        self,
        mock_wf_config: WalkForwardConfig,
        mock_window_results: list[MagicMock],
    ) -> None:
        """Test creating a walk-forward result."""
        result = WalkForwardResult(
            windows=mock_window_results,
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
        )
        assert len(result.windows) == 3
        assert result.total_windows == 3
        assert result.successful_windows == 3
        assert result.combined_test_pnl_usd == Decimal("15000")

    def test_is_overfit_true(
        self,
        mock_wf_config: WalkForwardConfig,
        mock_window_results: list[MagicMock],
    ) -> None:
        """Test is_overfit when ratio > 1.5."""
        result = WalkForwardResult(
            windows=mock_window_results,
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.8,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.64,  # > 1.5
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
        )
        assert result.is_overfit is True

    def test_is_overfit_false(
        self,
        mock_wf_config: WalkForwardConfig,
        mock_window_results: list[MagicMock],
    ) -> None:
        """Test is_overfit when ratio <= 1.5."""
        result = WalkForwardResult(
            windows=mock_window_results,
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,  # <= 1.5
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
        )
        assert result.is_overfit is False

    def test_avg_generalization_score(
        self,
        mock_wf_config: WalkForwardConfig,
        mock_window_results: list[MagicMock],
    ) -> None:
        """Test average generalization score calculation."""
        result = WalkForwardResult(
            windows=mock_window_results,
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
        )
        # Should be average of generalization scores from mock results
        assert 0.0 <= result.avg_generalization_score <= 1.0

    def test_avg_generalization_score_empty(
        self,
        mock_wf_config: WalkForwardConfig,
    ) -> None:
        """Test average generalization score with empty windows."""
        result = WalkForwardResult(
            windows=[],
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=0,
            successful_windows=0,
            avg_train_objective=0.0,
            avg_test_objective=0.0,
            avg_overfitting_ratio=0.0,
            combined_test_pnl_usd=Decimal("0"),
            combined_test_return_pct=Decimal("0"),
        )
        assert result.avg_generalization_score == 0.0

    def test_to_dict(
        self,
        mock_wf_config: WalkForwardConfig,
        mock_window_results: list[MagicMock],
    ) -> None:
        """Test serialization to dictionary."""
        result = WalkForwardResult(
            windows=mock_window_results,
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
        )
        data = result.to_dict()

        assert "windows" in data
        assert len(data["windows"]) == 3
        assert "config" in data
        assert data["objective_metric"] == "sharpe_ratio"
        assert data["total_windows"] == 3
        assert data["successful_windows"] == 3
        assert "combined_test_pnl_usd" in data
        assert "combined_test_return_pct" in data
        assert "is_overfit" in data
        assert "avg_generalization_score" in data

    def test_summary(
        self,
        mock_wf_config: WalkForwardConfig,
        mock_window_results: list[MagicMock],
    ) -> None:
        """Test summary generation."""
        result = WalkForwardResult(
            windows=mock_window_results,
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
        )
        summary = result.summary()

        assert "WALK-FORWARD OPTIMIZATION RESULTS" in summary
        assert "3/3 successful" in summary
        assert "sharpe_ratio" in summary
        assert "Overfitting Ratio" in summary
        assert "Generalization" in summary


class TestWalkForwardOptimizationLoop:
    """Tests for walk-forward optimization loop functionality."""

    @pytest.fixture
    def mock_strategy_factory(self) -> MagicMock:
        """Create a mock strategy factory."""
        return MagicMock(return_value=MagicMock())

    @pytest.fixture
    def mock_data_provider_factory(self) -> MagicMock:
        """Create a mock data provider factory."""
        return MagicMock(return_value=MagicMock())

    @pytest.fixture
    def mock_backtester_factory(self) -> MagicMock:
        """Create a mock backtester factory."""
        mock_backtester = MagicMock()
        mock_result = MagicMock()
        mock_result.metrics = MagicMock()
        mock_result.metrics.sharpe_ratio = Decimal("1.2")
        mock_result.metrics.net_pnl_usd = Decimal("5000")
        mock_result.metrics.total_return_pct = Decimal("0.10")
        mock_backtester.backtest = AsyncMock(return_value=mock_result)
        return MagicMock(return_value=mock_backtester)

    def test_imports_available(self) -> None:
        """Test that optimization loop functions are importable."""
        from almanak.framework.backtesting.pnl.walk_forward import (
            run_walk_forward_optimization,
            run_walk_forward_optimization_sync,
        )

        assert callable(run_walk_forward_optimization)
        assert callable(run_walk_forward_optimization_sync)

    def test_window_result_metrics_consistency(self) -> None:
        """Test that window results maintain metric consistency."""
        window = WalkForwardWindow(
            window_index=0,
            train_start=datetime(2023, 1, 1),
            train_end=datetime(2023, 4, 1),
            test_start=datetime(2023, 4, 1),
            test_end=datetime(2023, 5, 1),
        )

        # Create mock results with consistent metrics
        opt_result = MagicMock()
        opt_result.best_value = 1.5
        opt_result.to_dict.return_value = {"best_value": 1.5}

        test_result = MagicMock()
        test_result.metrics.sharpe_ratio = Decimal("1.2")
        test_result.to_dict.return_value = {"metrics": {"sharpe_ratio": "1.2"}}

        wr = WalkForwardWindowResult(
            window=window,
            optimization_result=opt_result,
            test_result=test_result,
            train_objective_value=1.5,
            test_objective_value=1.2,
            objective_metric="sharpe_ratio",
        )

        # Check metric consistency
        assert wr.train_objective_value == opt_result.best_value
        assert wr.test_objective_value == float(test_result.metrics.sharpe_ratio)

    def test_aggregation_calculations(self) -> None:
        """Test that aggregation calculations are correct."""
        wf_config = WalkForwardConfig.from_days(90, 30)

        # Create window results with known values
        window_results = []
        for i, (train_val, test_val) in enumerate(
            [(1.5, 1.2), (1.6, 1.3), (1.4, 1.1)]
        ):
            wr = MagicMock()
            wr.window = WalkForwardWindow(
                window_index=i,
                train_start=datetime(2023, 1, 1) + timedelta(days=i * 30),
                train_end=datetime(2023, 4, 1) + timedelta(days=i * 30),
                test_start=datetime(2023, 4, 1) + timedelta(days=i * 30),
                test_end=datetime(2023, 5, 1) + timedelta(days=i * 30),
            )
            wr.train_objective_value = train_val
            wr.test_objective_value = test_val
            wr.overfitting_ratio = train_val / test_val
            wr.generalization_score = min(1.0, test_val / train_val)
            wr.test_result = MagicMock()
            wr.test_result.metrics.net_pnl_usd = Decimal("5000")
            wr.test_result.metrics.total_return_pct = Decimal("0.10")
            wr.to_dict.return_value = {}
            window_results.append(wr)

        # Expected values
        expected_avg_train = (1.5 + 1.6 + 1.4) / 3
        expected_avg_test = (1.2 + 1.3 + 1.1) / 3
        expected_combined_pnl = Decimal("15000")  # 3 * 5000

        result = WalkForwardResult(
            windows=window_results,
            config=wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=expected_avg_train,
            avg_test_objective=expected_avg_test,
            avg_overfitting_ratio=sum(w.overfitting_ratio for w in window_results) / 3,
            combined_test_pnl_usd=expected_combined_pnl,
            combined_test_return_pct=Decimal("0.30"),
        )

        assert result.avg_train_objective == pytest.approx(expected_avg_train)
        assert result.avg_test_objective == pytest.approx(expected_avg_test)
        assert result.combined_test_pnl_usd == expected_combined_pnl


# =============================================================================
# Tests for Parameter Stability Analysis
# =============================================================================


class TestParameterStability:
    """Tests for ParameterStability dataclass and calculate_parameter_stability."""

    def test_basic_creation(self) -> None:
        """Test creating a ParameterStability instance."""
        from almanak.framework.backtesting.pnl.walk_forward import ParameterStability

        stability = ParameterStability(
            param_name="initial_capital_usd",
            values=[Decimal("10000"), Decimal("12000"), Decimal("11000")],
            mean=11000.0,
            std=816.5,
            variance=666666.67,
            cv=0.074,
            min_value=Decimal("10000"),
            max_value=Decimal("12000"),
            is_stable=True,
            stability_threshold=0.3,
        )
        assert stability.param_name == "initial_capital_usd"
        assert stability.is_stable is True
        assert stability.cv == 0.074

    def test_to_dict_serialization(self) -> None:
        """Test ParameterStability serialization to dict."""
        from almanak.framework.backtesting.pnl.walk_forward import ParameterStability

        stability = ParameterStability(
            param_name="interval_seconds",
            values=[3600, 7200, 3600],
            mean=4800.0,
            std=1697.1,
            variance=2880000.0,
            cv=0.354,
            min_value=3600,
            max_value=7200,
            is_stable=False,
            stability_threshold=0.3,
        )
        data = stability.to_dict()

        assert data["param_name"] == "interval_seconds"
        assert data["mean"] == 4800.0
        assert data["is_stable"] is False
        assert data["cv"] == 0.354
        assert data["values"] == [3600, 7200, 3600]

    def test_from_dict_deserialization(self) -> None:
        """Test ParameterStability deserialization from dict."""
        from almanak.framework.backtesting.pnl.walk_forward import ParameterStability

        data = {
            "param_name": "test_param",
            "values": [100, 200, 150],
            "mean": 150.0,
            "std": 40.82,
            "variance": 1666.67,
            "cv": 0.272,
            "min_value": 100,
            "max_value": 200,
            "is_stable": True,
            "stability_threshold": 0.3,
        }
        stability = ParameterStability.from_dict(data)

        assert stability.param_name == "test_param"
        assert stability.mean == 150.0
        assert stability.is_stable is True

    def test_decimal_values_serialized_as_strings(self) -> None:
        """Test that Decimal values are serialized as strings."""
        from almanak.framework.backtesting.pnl.walk_forward import ParameterStability

        stability = ParameterStability(
            param_name="capital",
            values=[Decimal("10000.50"), Decimal("20000.75")],
            mean=15000.625,
            std=5000.125,
            variance=25001250.015625,
            cv=0.333,
            min_value=Decimal("10000.50"),
            max_value=Decimal("20000.75"),
            is_stable=False,
        )
        data = stability.to_dict()

        assert data["values"] == ["10000.50", "20000.75"]
        assert data["min_value"] == "10000.50"
        assert data["max_value"] == "20000.75"


class TestCalculateParameterStability:
    """Tests for calculate_parameter_stability function."""

    @pytest.fixture
    def mock_window_results_with_params(self) -> list[MagicMock]:
        """Create mock window results with varying parameters."""
        results = []
        # Simulate 3 windows with different optimal parameters
        params_per_window = [
            {"initial_capital_usd": Decimal("10000"), "interval_seconds": 3600},
            {"initial_capital_usd": Decimal("12000"), "interval_seconds": 7200},
            {"initial_capital_usd": Decimal("11000"), "interval_seconds": 3600},
        ]

        for params in params_per_window:
            wr = MagicMock()
            wr.optimization_result = MagicMock()
            wr.optimization_result.best_params = params
            results.append(wr)
        return results

    def test_calculates_stability_for_all_params(
        self,
        mock_window_results_with_params: list[MagicMock],
    ) -> None:
        """Test that stability is calculated for all parameters."""

        stability = calculate_parameter_stability(mock_window_results_with_params)

        assert "initial_capital_usd" in stability
        assert "interval_seconds" in stability

    def test_stable_parameter_detection(
        self,
        mock_window_results_with_params: list[MagicMock],
    ) -> None:
        """Test detection of stable parameters (low CV)."""

        stability = calculate_parameter_stability(mock_window_results_with_params)

        # initial_capital_usd: [10000, 12000, 11000] -> CV ~8%, should be stable
        capital_stability = stability["initial_capital_usd"]
        assert capital_stability.is_stable is True
        assert capital_stability.cv < 0.3

    def test_unstable_parameter_detection(self) -> None:
        """Test detection of unstable parameters (high CV)."""

        # Create window results with wildly varying parameter
        results = []
        for val in [1000, 5000, 100]:  # High variance
            wr = MagicMock()
            wr.optimization_result = MagicMock()
            wr.optimization_result.best_params = {"unstable_param": val}
            results.append(wr)

        stability = calculate_parameter_stability(results)

        # CV should be high (values vary significantly)
        assert stability["unstable_param"].is_stable is False
        assert stability["unstable_param"].cv > 0.3

    def test_single_value_is_stable(self) -> None:
        """Test that single value is considered stable."""

        results = []
        for _ in range(3):
            wr = MagicMock()
            wr.optimization_result = MagicMock()
            wr.optimization_result.best_params = {"constant_param": 42}
            results.append(wr)

        stability = calculate_parameter_stability(results)

        # All same value -> CV = 0 -> stable
        assert stability["constant_param"].is_stable is True
        assert stability["constant_param"].cv == 0.0
        assert stability["constant_param"].std == 0.0

    def test_empty_window_results(self) -> None:
        """Test handling of empty window results."""

        stability = calculate_parameter_stability([])
        assert stability == {}

    def test_custom_stability_threshold(self) -> None:
        """Test custom stability threshold."""

        # Create moderate variance (~20% CV)
        results = []
        for val in [100, 120, 110]:
            wr = MagicMock()
            wr.optimization_result = MagicMock()
            wr.optimization_result.best_params = {"param": val}
            results.append(wr)

        # With default threshold (0.3), should be stable
        stability_default = calculate_parameter_stability(results)
        assert stability_default["param"].is_stable is True

        # With strict threshold (0.05), should be unstable
        stability_strict = calculate_parameter_stability(results, stability_threshold=0.05)
        assert stability_strict["param"].is_stable is False

    def test_categorical_parameters_all_same(self) -> None:
        """Test categorical parameters that are all the same."""

        results = []
        for _ in range(3):
            wr = MagicMock()
            wr.optimization_result = MagicMock()
            wr.optimization_result.best_params = {"strategy_type": "aggressive"}
            results.append(wr)

        stability = calculate_parameter_stability(results)

        # All same categorical value -> stable
        assert stability["strategy_type"].is_stable is True

    def test_categorical_parameters_varying(self) -> None:
        """Test categorical parameters that vary."""

        results = []
        for strategy in ["aggressive", "conservative", "moderate"]:
            wr = MagicMock()
            wr.optimization_result = MagicMock()
            wr.optimization_result.best_params = {"strategy_type": strategy}
            results.append(wr)

        stability = calculate_parameter_stability(results)

        # Different categorical values -> unstable
        assert stability["strategy_type"].is_stable is False


class TestWalkForwardResultInstability:
    """Tests for parameter instability detection in WalkForwardResult."""

    @pytest.fixture
    def mock_wf_config(self) -> WalkForwardConfig:
        """Create a mock walk-forward config."""
        return WalkForwardConfig.from_days(90, 30)

    @pytest.fixture
    def stable_parameter_stability(self) -> "dict[str, ParameterStability]":
        """Create parameter stability dict with all stable params."""
        from almanak.framework.backtesting.pnl.walk_forward import ParameterStability

        return {
            "capital": ParameterStability(
                param_name="capital",
                values=[10000, 11000, 10500],
                mean=10500.0,
                std=408.25,
                variance=166666.67,
                cv=0.039,
                min_value=10000,
                max_value=11000,
                is_stable=True,
            ),
            "interval": ParameterStability(
                param_name="interval",
                values=[3600, 3600, 3600],
                mean=3600.0,
                std=0.0,
                variance=0.0,
                cv=0.0,
                min_value=3600,
                max_value=3600,
                is_stable=True,
            ),
        }

    @pytest.fixture
    def unstable_parameter_stability(self) -> "dict[str, ParameterStability]":
        """Create parameter stability dict with some unstable params."""
        from almanak.framework.backtesting.pnl.walk_forward import ParameterStability

        return {
            "capital": ParameterStability(
                param_name="capital",
                values=[10000, 50000, 25000],
                mean=28333.33,
                std=16498.9,
                variance=272222222.22,
                cv=0.582,
                min_value=10000,
                max_value=50000,
                is_stable=False,
            ),
            "interval": ParameterStability(
                param_name="interval",
                values=[3600, 3600, 3600],
                mean=3600.0,
                std=0.0,
                variance=0.0,
                cv=0.0,
                min_value=3600,
                max_value=3600,
                is_stable=True,
            ),
        }

    def test_unstable_parameters_property(
        self,
        mock_wf_config: WalkForwardConfig,
        unstable_parameter_stability: "dict[str, ParameterStability]",
    ) -> None:
        """Test unstable_parameters returns list of unstable param names."""
        result = WalkForwardResult(
            windows=[],
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
            parameter_stability=unstable_parameter_stability,
        )

        assert "capital" in result.unstable_parameters
        assert "interval" not in result.unstable_parameters

    def test_has_parameter_instability_true(
        self,
        mock_wf_config: WalkForwardConfig,
        unstable_parameter_stability: "dict[str, ParameterStability]",
    ) -> None:
        """Test has_parameter_instability when unstable params exist."""
        result = WalkForwardResult(
            windows=[],
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
            parameter_stability=unstable_parameter_stability,
        )

        assert result.has_parameter_instability is True

    def test_has_parameter_instability_false(
        self,
        mock_wf_config: WalkForwardConfig,
        stable_parameter_stability: "dict[str, ParameterStability]",
    ) -> None:
        """Test has_parameter_instability when all params stable."""
        result = WalkForwardResult(
            windows=[],
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
            parameter_stability=stable_parameter_stability,
        )

        assert result.has_parameter_instability is False

    def test_avg_parameter_cv(
        self,
        mock_wf_config: WalkForwardConfig,
        unstable_parameter_stability: "dict[str, ParameterStability]",
    ) -> None:
        """Test avg_parameter_cv calculation."""
        result = WalkForwardResult(
            windows=[],
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
            parameter_stability=unstable_parameter_stability,
        )

        # Average of 0.582 and 0.0
        expected_avg_cv = (0.582 + 0.0) / 2
        assert result.avg_parameter_cv == pytest.approx(expected_avg_cv, rel=0.01)

    def test_avg_parameter_cv_empty(
        self,
        mock_wf_config: WalkForwardConfig,
    ) -> None:
        """Test avg_parameter_cv with no parameters."""
        result = WalkForwardResult(
            windows=[],
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
            parameter_stability={},
        )

        assert result.avg_parameter_cv == 0.0

    def test_to_dict_includes_stability(
        self,
        mock_wf_config: WalkForwardConfig,
        unstable_parameter_stability: "dict[str, ParameterStability]",
    ) -> None:
        """Test that to_dict includes parameter stability fields."""
        result = WalkForwardResult(
            windows=[],
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
            parameter_stability=unstable_parameter_stability,
        )
        data = result.to_dict()

        assert "parameter_stability" in data
        assert "unstable_parameters" in data
        assert "has_parameter_instability" in data
        assert "avg_parameter_cv" in data
        assert data["has_parameter_instability"] is True
        assert "capital" in data["unstable_parameters"]

    def test_summary_includes_stability(
        self,
        mock_wf_config: WalkForwardConfig,
        unstable_parameter_stability: "dict[str, ParameterStability]",
    ) -> None:
        """Test that summary includes parameter stability info."""
        result = WalkForwardResult(
            windows=[],
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
            parameter_stability=unstable_parameter_stability,
        )
        summary = result.summary()

        assert "Parameter Stability" in summary
        assert "capital" in summary
        assert "UNSTABLE" in summary
        assert "Unstable parameters" in summary

    def test_from_dict_reconstructs_stability(
        self,
        mock_wf_config: WalkForwardConfig,
        unstable_parameter_stability: "dict[str, ParameterStability]",
    ) -> None:
        """Test that from_dict reconstructs parameter stability."""
        from almanak.framework.backtesting.pnl.walk_forward import WalkForwardResult

        original = WalkForwardResult(
            windows=[],
            config=mock_wf_config,
            objective_metric="sharpe_ratio",
            total_windows=3,
            successful_windows=3,
            avg_train_objective=1.4,
            avg_test_objective=1.1,
            avg_overfitting_ratio=1.27,
            combined_test_pnl_usd=Decimal("15000"),
            combined_test_return_pct=Decimal("0.30"),
            parameter_stability=unstable_parameter_stability,
        )

        data = original.to_dict()
        reconstructed = WalkForwardResult.from_dict(data)

        assert reconstructed.has_parameter_instability is True
        assert "capital" in reconstructed.parameter_stability
        assert reconstructed.parameter_stability["capital"].is_stable is False
