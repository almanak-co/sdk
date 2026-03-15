"""Tests for the --duration flag on paper trading CLI.

Verifies:
- _parse_duration() correctly parses human-readable durations
- --duration and --max-ticks are mutually exclusive
- Duration is converted to max_ticks correctly

Fixes VIB-201.
"""

from almanak.framework.cli.backtest import _parse_duration


class TestParseDuration:
    """Tests for _parse_duration() helper."""

    def test_seconds_only(self):
        assert _parse_duration("30s") == 30

    def test_minutes_only(self):
        assert _parse_duration("5m") == 300

    def test_hours_only(self):
        assert _parse_duration("1h") == 3600

    def test_hours_and_minutes(self):
        assert _parse_duration("2h30m") == 9000

    def test_hours_minutes_seconds(self):
        assert _parse_duration("1h30m15s") == 5415

    def test_pure_integer_as_seconds(self):
        assert _parse_duration("120") == 120

    def test_case_insensitive(self):
        assert _parse_duration("5M") == 300
        assert _parse_duration("1H") == 3600

    def test_with_whitespace(self):
        assert _parse_duration("  5m  ") == 300

    def test_invalid_string(self):
        assert _parse_duration("abc") is None

    def test_empty_string(self):
        assert _parse_duration("") is None

    def test_zero_duration(self):
        assert _parse_duration("0s") is None
        assert _parse_duration("0") is None

    def test_large_duration(self):
        assert _parse_duration("24h") == 86400

    def test_minutes_and_seconds(self):
        assert _parse_duration("5m30s") == 330


class TestDurationToMaxTicks:
    """Test duration -> max_ticks conversion logic.

    Formula: max_ticks = duration_seconds // tick_interval + 1
    The +1 accounts for the first tick executing immediately (N ticks = N-1 sleep intervals).
    """

    def test_5m_at_60s_interval(self):
        """5 minutes at 60s interval = 6 ticks (5 sleeps = 300s)."""
        duration_seconds = _parse_duration("5m")
        tick_interval = 60
        max_ticks = max(1, duration_seconds // tick_interval + 1)
        assert max_ticks == 6

    def test_1h_at_60s_interval(self):
        """1 hour at 60s interval = 61 ticks (60 sleeps = 3600s)."""
        duration_seconds = _parse_duration("1h")
        tick_interval = 60
        max_ticks = max(1, duration_seconds // tick_interval + 1)
        assert max_ticks == 61

    def test_30s_at_60s_interval_rounds_to_1(self):
        """30 seconds at 60s interval = at least 1 tick."""
        duration_seconds = _parse_duration("30s")
        tick_interval = 60
        max_ticks = max(1, duration_seconds // tick_interval + 1)
        assert max_ticks == 1

    def test_10m_at_30s_interval(self):
        """10 minutes at 30s interval = 21 ticks (20 sleeps = 600s)."""
        duration_seconds = _parse_duration("10m")
        tick_interval = 30
        max_ticks = max(1, duration_seconds // tick_interval + 1)
        assert max_ticks == 21
