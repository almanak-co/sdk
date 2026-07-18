"""Tick interval derived from the strategy's declared timeframe (ALM-2943)."""

from almanak.services.backtest.services.backtest_runner import _derive_interval_seconds

DAY = 86_400


class TestDeriveIntervalSeconds:
    def test_declared_sub_hourly_served_on_recent_short_windows(self) -> None:
        assert _derive_interval_seconds("15m", DAY, quick=False, end_age_seconds=0) == 900
        assert _derive_interval_seconds("5m", DAY, quick=False, end_age_seconds=3600) == 300

    def test_sub_hourly_clamped_for_historical_windows(self) -> None:
        # The source only has 5-minutely data near real time; a 1-day window
        # ending last week comes back hourly, so the grid must not pretend.
        assert _derive_interval_seconds("15m", DAY, quick=False, end_age_seconds=7 * DAY) == 3600

    def test_sub_hourly_clamped_on_long_windows(self) -> None:
        # The source serves hourly beyond ~a day; refusing per tick over a
        # grid the user never chose is replaced by one clamp decision here.
        assert _derive_interval_seconds("15m", 90 * DAY, quick=False) == 3600

    def test_coarse_timeframes_keep_the_hourly_grid(self) -> None:
        assert _derive_interval_seconds("4h", 90 * DAY, quick=False) == 3600
        assert _derive_interval_seconds("1d", 90 * DAY, quick=False) == 3600

    def test_absent_or_bad_timeframe_defaults(self) -> None:
        assert _derive_interval_seconds(None, DAY, quick=False) == 3600
        assert _derive_interval_seconds("candles", DAY, quick=False) == 3600


class TestEndDateRecency:
    @staticmethod
    def _config(start: str, end: str):
        from decimal import Decimal

        from almanak.services.backtest.models import StrategySpec, TimeframeSpec
        from almanak.services.backtest.services.backtest_runner import build_backtest_config
        from tests.backtesting_funding import pnl_token_funding

        spec = StrategySpec(
            protocol="uniswap_v3",
            chain="arbitrum",
            action="swap",
            parameters={"timeframe": "15m", "tokens": ["WETH", "USDC"]},
        )
        return build_backtest_config(
            spec, TimeframeSpec(start=start, end=end), token_funding=pnl_token_funding(Decimal("5000"))
        )

    def test_end_date_today_means_up_to_now(self) -> None:
        # A date-only end of TODAY moves the WHOLE config to now — end_time,
        # window, and recency stay coherent, so 15m serves for an intraday
        # window instead of clamping against a midnight end.
        from datetime import UTC, datetime, time as dtime

        today = datetime.now(UTC).date()
        config = self._config(str(today), str(today))

        assert config.interval_seconds == 900
        assert config.end_time > datetime.combine(today, dtime(0), tzinfo=UTC)  # runs to now, not midnight

    def test_historical_end_dates_keep_midnight_and_clamp(self) -> None:
        from datetime import UTC, datetime, time as dtime, timedelta

        today = datetime.now(UTC).date()
        end_day = today - timedelta(days=7)
        config = self._config(str(end_day - timedelta(days=1)), str(end_day))

        assert config.interval_seconds == 3600  # stale window: sub-hourly clamps
        assert config.end_time == datetime.combine(end_day, dtime(0), tzinfo=UTC)  # midnight bound unchanged

    def test_quick_mode_wins(self) -> None:
        assert _derive_interval_seconds("15m", DAY, quick=True) == DAY

    def test_finer_than_source_minimum_clamps(self) -> None:
        assert _derive_interval_seconds("1m", 3600, quick=False) == 3600
