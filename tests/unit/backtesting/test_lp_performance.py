"""Tests for LPPerformanceTracker — IL estimation, fee tracking, gas, HODL benchmark."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.lp_performance import (
    LPPerformanceReport,
    LPPerformanceTracker,
    LPSnapshot,
)

ZERO = Decimal("0")
T0 = datetime(2025, 1, 1, tzinfo=UTC)
T1 = T0 + timedelta(hours=24)
T2 = T0 + timedelta(hours=48)
T3 = T0 + timedelta(hours=72)


def _make_tracker(benchmark: str = "hodl") -> LPPerformanceTracker:
    return LPPerformanceTracker(benchmark=benchmark)


class TestBasicTracking:
    """Basic snapshot recording and summary generation."""

    def test_needs_at_least_two_snapshots(self):
        tracker = _make_tracker()
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        with pytest.raises(ValueError, match="At least 2 snapshots"):
            tracker.summary()

    def test_empty_tracker_raises(self):
        tracker = _make_tracker()
        with pytest.raises(ValueError):
            tracker.summary()

    def test_two_snapshots_no_change(self):
        tracker = _make_tracker()
        for t in (T0, T1):
            tracker.record_snapshot(
                position_value_usd=Decimal("10000"),
                token0_amount=Decimal("5"),
                token1_amount=Decimal("5000"),
                token0_price=Decimal("1000"),
                token1_price=Decimal("1"),
                timestamp=t,
            )
        report = tracker.summary()
        assert report.total_pnl_usd == ZERO
        assert report.il_usd == ZERO
        assert report.fees_earned_usd == ZERO
        assert report.gas_spent_usd == ZERO
        assert report.num_snapshots == 2

    def test_reset_clears_snapshots(self):
        tracker = _make_tracker()
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        assert len(tracker.snapshots) == 1
        tracker.reset()
        assert len(tracker.snapshots) == 0

    def test_snapshots_returns_copy(self):
        tracker = _make_tracker()
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        snaps = tracker.snapshots
        snaps.clear()
        assert len(tracker.snapshots) == 1


class TestImpermanentLoss:
    """Known IL scenarios."""

    def test_price_doubles_il(self):
        """Classic IL: ETH doubles, position rebalances vs pure HODL."""
        tracker = _make_tracker()
        # Start: 5 ETH @ $1000 + 5000 USDC = $10,000
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        # ETH doubles to $2000. AMM rebalances to ~3.535 ETH + 7071 USDC = $14,142
        # HODL: 5*2000 + 5000*1 = $15,000
        tracker.record_snapshot(
            position_value_usd=Decimal("14142"),
            token0_amount=Decimal("3.535"),
            token1_amount=Decimal("7071"),
            token0_price=Decimal("2000"),
            token1_price=Decimal("1"),
            timestamp=T1,
        )
        report = tracker.summary()
        # HODL = 5*2000 + 5000 = 15000
        assert report.hodl_value_usd == Decimal("15000")
        # IL = hodl - position = 15000 - 14142 = 858
        assert report.il_usd == Decimal("858")
        # IL% = 858/10000 * 100 = 8.58%
        assert report.il_pct == Decimal("8.58")

    def test_no_price_change_no_il(self):
        tracker = _make_tracker()
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T1,
        )
        report = tracker.summary()
        assert report.il_usd == ZERO
        assert report.il_pct == ZERO


class TestFeeAndGasTracking:
    """Fee and gas cost accounting."""

    def test_fees_accumulated(self):
        tracker = _make_tracker()
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            fees_delta_usd=Decimal("10"),
            gas_delta_usd=Decimal("2"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            fees_delta_usd=Decimal("15"),
            gas_delta_usd=Decimal("3"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T1,
        )
        report = tracker.summary()
        assert report.fees_earned_usd == Decimal("25")
        assert report.gas_spent_usd == Decimal("5")
        assert report.total_pnl_usd == Decimal("25")  # no value change + 25 fees
        assert report.net_pnl_usd == Decimal("20")  # 25 - 5 gas

    def test_fees_offset_il(self):
        """Fees can make LP profitable despite IL."""
        tracker = _make_tracker()
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            fees_delta_usd=ZERO,
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        # Price change causes IL of 858 (same as above), but 1000 in fees
        tracker.record_snapshot(
            position_value_usd=Decimal("14142"),
            token0_amount=Decimal("3.535"),
            token1_amount=Decimal("7071"),
            fees_delta_usd=Decimal("1000"),
            token0_price=Decimal("2000"),
            token1_price=Decimal("1"),
            timestamp=T1,
        )
        report = tracker.summary()
        assert report.il_usd == Decimal("858")
        assert report.fees_earned_usd == Decimal("1000")
        # total pnl = (14142 - 10000) + 1000 = 5142
        assert report.total_pnl_usd == Decimal("5142")


class TestBenchmarks:
    """Different benchmark comparison modes."""

    def _add_standard_snapshots(self, tracker: LPPerformanceTracker):
        # Start: 5 ETH @ $1000 + 5000 USDC = $10,000
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        # End: ETH goes to $1500
        tracker.record_snapshot(
            position_value_usd=Decimal("12000"),
            token0_amount=Decimal("4"),
            token1_amount=Decimal("6000"),
            token0_price=Decimal("1500"),
            token1_price=Decimal("1"),
            timestamp=T1,
        )

    def test_hodl_benchmark(self):
        tracker = _make_tracker("hodl")
        self._add_standard_snapshots(tracker)
        report = tracker.summary()
        # HODL: 5 * 1500 + 5000 * 1 = 12500
        assert report.hodl_value_usd == Decimal("12500")
        assert report.benchmark == "hodl"

    def test_usd_benchmark(self):
        tracker = _make_tracker("usd")
        self._add_standard_snapshots(tracker)
        report = tracker.summary()
        # USD: just hold $10,000
        assert report.hodl_value_usd == Decimal("10000")

    def test_token0_benchmark(self):
        tracker = _make_tracker("token0")
        self._add_standard_snapshots(tracker)
        report = tracker.summary()
        # token0: convert all $10k to ETH at $1000 = 10 ETH, value at $1500 = $15,000
        assert report.hodl_value_usd == Decimal("15000")

    def test_token1_benchmark(self):
        tracker = _make_tracker("token1")
        self._add_standard_snapshots(tracker)
        report = tracker.summary()
        # token1: convert all to USDC at $1 = 10000 USDC, value at $1 = $10,000
        assert report.hodl_value_usd == Decimal("10000")


class TestValidation:
    """Input validation and error handling."""

    def test_zero_initial_value_raises(self):
        tracker = _make_tracker()
        tracker.record_snapshot(
            position_value_usd=Decimal("0"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T1,
        )
        with pytest.raises(ValueError, match="Initial position value must be positive"):
            tracker.summary()

    def test_hodl_benchmark_zero_price_raises(self):
        tracker = _make_tracker("hodl")
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        # Last snapshot has zero token0_price
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("0"),
            token1_price=Decimal("1"),
            timestamp=T1,
        )
        with pytest.raises(ValueError, match="Positive token prices required in the last snapshot"):
            tracker.summary()

    def test_token0_benchmark_zero_price_raises(self):
        tracker = _make_tracker("token0")
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("0"),  # zero price for token0 benchmark
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T1,
        )
        with pytest.raises(ValueError, match="token0 benchmark requires positive token0_price"):
            tracker.summary()

    def test_usd_benchmark_needs_prices_for_il(self):
        """USD benchmark still requires last-snapshot prices for IL calculation."""
        tracker = _make_tracker("usd")
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            timestamp=T0,
        )
        tracker.record_snapshot(
            position_value_usd=Decimal("11000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            timestamp=T1,
        )
        with pytest.raises(ValueError, match="Positive token prices required"):
            tracker.summary()

    def test_usd_benchmark_with_prices(self):
        """USD benchmark compares against holding initial USD value."""
        tracker = _make_tracker("usd")
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        tracker.record_snapshot(
            position_value_usd=Decimal("11000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T1,
        )
        report = tracker.summary()
        assert report.hodl_value_usd == Decimal("10000")
        # IL should use true HODL (initial tokens at current prices), not benchmark
        assert report.il_usd == Decimal("-1000")  # 5*1000 + 5000 - 11000 = -1000 (LP outperformed)


class TestSharpeRatio:
    """Sharpe ratio calculation."""

    def test_sharpe_none_with_two_snapshots(self):
        tracker = _make_tracker()
        for t in (T0, T1):
            tracker.record_snapshot(
                position_value_usd=Decimal("10000"),
                token0_amount=Decimal("5"),
                token1_amount=Decimal("5000"),
                token0_price=Decimal("1000"),
                token1_price=Decimal("1"),
                timestamp=t,
            )
        report = tracker.summary()
        assert report.sharpe_ratio is None

    def test_sharpe_none_with_zero_variance(self):
        tracker = _make_tracker()
        # All snapshots same value -> zero returns -> zero std -> None
        for t in [T0, T1, T2, T3]:
            tracker.record_snapshot(
                position_value_usd=Decimal("10000"),
                token0_amount=Decimal("5"),
                token1_amount=Decimal("5000"),
                token0_price=Decimal("1000"),
                token1_price=Decimal("1"),
                timestamp=t,
            )
        report = tracker.summary()
        assert report.sharpe_ratio is None

    def test_sharpe_computed_with_varying_returns(self):
        tracker = _make_tracker()
        values = [Decimal("10000"), Decimal("10100"), Decimal("9900"), Decimal("10200")]
        for val, t in zip(values, [T0, T1, T2, T3]):
            tracker.record_snapshot(
                position_value_usd=val,
                token0_amount=Decimal("5"),
                token1_amount=Decimal("5000"),
                token0_price=Decimal("1000"),
                token1_price=Decimal("1"),
                timestamp=t,
            )
        report = tracker.summary()
        assert report.sharpe_ratio is not None
        assert isinstance(report.sharpe_ratio, float)

    def test_sharpe_includes_fees_and_gas(self):
        """Sharpe should reflect fees earned and gas spent, not just position value."""
        tracker = _make_tracker()
        # Position value stays flat, but varying fees create non-zero returns
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            fees_delta_usd=Decimal("0"),
            gas_delta_usd=Decimal("0"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            fees_delta_usd=Decimal("100"),
            gas_delta_usd=Decimal("1"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T1,
        )
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            fees_delta_usd=Decimal("50"),
            gas_delta_usd=Decimal("2"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T2,
        )
        report = tracker.summary()
        # With fees, returns are positive even though position value is flat
        assert report.sharpe_ratio is not None
        assert report.sharpe_ratio > 0

    def test_sharpe_annualization_uses_actual_intervals(self):
        """Hourly snapshots should produce different annualization than daily."""
        tracker_daily = _make_tracker()
        tracker_hourly = _make_tracker()
        values = [Decimal("10000"), Decimal("10100"), Decimal("9900"), Decimal("10200")]

        # Daily snapshots
        for val, i in zip(values, range(4)):
            tracker_daily.record_snapshot(
                position_value_usd=val,
                token0_amount=Decimal("5"),
                token1_amount=Decimal("5000"),
                token0_price=Decimal("1000"),
                token1_price=Decimal("1"),
                timestamp=T0 + timedelta(days=i),
            )

        # Hourly snapshots (same values)
        for val, i in zip(values, range(4)):
            tracker_hourly.record_snapshot(
                position_value_usd=val,
                token0_amount=Decimal("5"),
                token1_amount=Decimal("5000"),
                token0_price=Decimal("1000"),
                token1_price=Decimal("1"),
                timestamp=T0 + timedelta(hours=i),
            )

        daily_sharpe = tracker_daily.summary().sharpe_ratio
        hourly_sharpe = tracker_hourly.summary().sharpe_ratio
        assert daily_sharpe is not None
        assert hourly_sharpe is not None
        # Hourly snapshots annualize more aggressively (sqrt(8760) vs sqrt(365))
        assert abs(hourly_sharpe) > abs(daily_sharpe)


class TestDuration:
    """Duration tracking."""

    def test_duration_hours(self):
        tracker = _make_tracker()
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0,
        )
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
            timestamp=T0 + timedelta(hours=48),
        )
        report = tracker.summary()
        assert report.duration_hours == Decimal("48")


class TestSerialization:
    """JSON serialization."""

    def test_to_dict_is_json_serializable(self):
        tracker = _make_tracker()
        for t in (T0, T1, T2):
            tracker.record_snapshot(
                position_value_usd=Decimal("10000"),
                token0_amount=Decimal("5"),
                token1_amount=Decimal("5000"),
                fees_delta_usd=Decimal("5"),
                gas_delta_usd=Decimal("1"),
                token0_price=Decimal("1000"),
                token1_price=Decimal("1"),
                timestamp=t,
            )
        report = tracker.summary()
        d = report.to_dict()
        # Must be JSON-serializable
        serialized = json.dumps(d)
        assert isinstance(serialized, str)
        parsed = json.loads(serialized)
        assert parsed["benchmark"] == "hodl"
        assert parsed["num_snapshots"] == 3

    def test_to_dict_keys(self):
        tracker = _make_tracker()
        for t in (T0, T1, T2):
            tracker.record_snapshot(
                position_value_usd=Decimal("10000"),
                token0_amount=Decimal("5"),
                token1_amount=Decimal("5000"),
                token0_price=Decimal("1000"),
                token1_price=Decimal("1"),
                timestamp=t,
            )
        report = tracker.summary()
        expected_keys = {
            "total_pnl_usd",
            "net_pnl_usd",
            "il_usd",
            "il_pct",
            "fees_earned_usd",
            "gas_spent_usd",
            "benchmark",
            "hodl_value_usd",
            "vs_hodl_usd",
            "vs_hodl_pct",
            "duration_hours",
            "num_snapshots",
            "sharpe_ratio",
        }
        assert set(report.to_dict().keys()) == expected_keys


class TestDefaultTimestamp:
    """Timestamp defaults to now(UTC) when not provided."""

    def test_auto_timestamp(self):
        tracker = _make_tracker()
        before = datetime.now(UTC)
        tracker.record_snapshot(
            position_value_usd=Decimal("10000"),
            token0_amount=Decimal("5"),
            token1_amount=Decimal("5000"),
            token0_price=Decimal("1000"),
            token1_price=Decimal("1"),
        )
        after = datetime.now(UTC)
        snap = tracker.snapshots[0]
        assert before <= snap.timestamp <= after
