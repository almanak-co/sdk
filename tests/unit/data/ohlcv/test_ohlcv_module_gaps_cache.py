"""Branch-coverage tests for OHLCVModule internals.

Covers the three coverage-poor spans of
``almanak/framework/data/ohlcv/module.py``:

- ``OHLCVModule._fetch_with_cache`` — cache hit / incremental fetch /
  cold cache / stale-provider-data branches
- ``OHLCVModule._candles_to_dataframe`` — empty list schema and populated
  conversion including ``volume=None`` -> NaN
- ``OHLCVModule._handle_gaps`` — no-gap short circuits, nan / ffill / drop
  strategies, >24h warning escalation, duplicate timestamps

Provider and cache are MagicMock seams — no network, no SQLite.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.ohlcv.module import OHLCVModule

MODULE_LOGGER = "almanak.framework.data.ohlcv.module"

BASE_TS = datetime(2026, 1, 1, 0, 0, 0)


def _candle(
    ts: datetime,
    price: str = "100",
    volume: str | None = "5",
) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=ts,
        open=Decimal(price),
        high=Decimal(price) + Decimal("1"),
        low=Decimal(price) - Decimal("1"),
        close=Decimal(price),
        volume=Decimal(volume) if volume is not None else None,
    )


def _hourly_candles(hours: list[int], base: datetime = BASE_TS) -> list[OHLCVCandle]:
    return [_candle(base + timedelta(hours=h), price=str(100 + h)) for h in hours]


def _make_module() -> OHLCVModule:
    provider = MagicMock()
    provider.get_ohlcv = AsyncMock(return_value=[])
    provider.source_name = "mock-provider"
    cache = MagicMock()
    return OHLCVModule(provider=provider, cache=cache, chain="ethereum")


# =============================================================================
# _fetch_with_cache
# =============================================================================


class TestFetchWithCache:
    """Branch coverage for OHLCVModule._fetch_with_cache."""

    @pytest.mark.asyncio
    async def test_cache_hit_returns_tail_without_provider_call(self):
        module = _make_module()
        now = datetime.now(UTC)
        cached = [_candle(now - timedelta(hours=7 - i)) for i in range(7)]
        module.cache.get_latest_timestamp.return_value = cached[-1].timestamp
        module.cache.get_candles.return_value = cached

        result = await module._fetch_with_cache("ETH", "USD", "1h", 5)

        assert result == cached[-5:]
        module.provider.get_ohlcv.assert_not_awaited()
        module.cache.store_candles.assert_not_called()

    @pytest.mark.asyncio
    async def test_incremental_fetch_filters_stale_and_stores_new(self):
        module = _make_module()
        now = datetime.now(UTC)
        latest_cached = now - timedelta(hours=10)
        cached = [
            _candle(latest_cached - timedelta(hours=1)),
            _candle(latest_cached),
        ]
        fresh = [
            _candle(latest_cached + timedelta(hours=1), price="200"),
            _candle(latest_cached + timedelta(hours=2), price="201"),
        ]
        stale = _candle(latest_cached)  # not strictly newer -> filtered out
        final = [_candle(now - timedelta(hours=7 - i)) for i in range(7)]

        module.cache.get_latest_timestamp.return_value = latest_cached
        module.cache.get_candles.side_effect = [cached, final]
        module.provider.get_ohlcv = AsyncMock(return_value=[stale, *fresh])

        result = await module._fetch_with_cache("ETH", "USD", "1h", 5)

        # candles_needed = max(int(10h / 1h) + 1, 5 - 2) = 11
        module.provider.get_ohlcv.assert_awaited_once_with(
            token="ETH",
            quote="USD",
            timeframe="1h",
            limit=11,
        )
        module.cache.store_candles.assert_called_once_with(fresh, "ETH", "USD", "1h", "ethereum")
        # final cache read has more than limit -> returns the tail
        assert result == final[-5:]

    @pytest.mark.asyncio
    async def test_cold_cache_fetches_full_limit_and_returns_all(self):
        module = _make_module()
        now = datetime.now(UTC)
        fetched = [_candle(now - timedelta(hours=3 - i)) for i in range(3)]

        module.cache.get_latest_timestamp.return_value = None
        module.cache.get_candles.side_effect = [[], fetched]
        module.provider.get_ohlcv = AsyncMock(return_value=fetched)

        result = await module._fetch_with_cache("BTC", "USD", "1h", 5)

        module.provider.get_ohlcv.assert_awaited_once_with(
            token="BTC",
            quote="USD",
            timeframe="1h",
            limit=5,
        )
        module.cache.store_candles.assert_called_once_with(fetched, "BTC", "USD", "1h", "ethereum")
        # fewer candles than limit -> returned whole, not sliced
        assert result == fetched

    @pytest.mark.asyncio
    async def test_provider_returns_only_stale_candles_skips_store(self):
        module = _make_module()
        now = datetime.now(UTC)
        latest_cached = now - timedelta(hours=2)
        cached = [_candle(latest_cached)]

        module.cache.get_latest_timestamp.return_value = latest_cached
        module.cache.get_candles.side_effect = [cached, cached]
        module.provider.get_ohlcv = AsyncMock(return_value=[_candle(latest_cached - timedelta(hours=1))])

        result = await module._fetch_with_cache("ETH", "USD", "1h", 5)

        module.cache.store_candles.assert_not_called()
        assert result == cached


# =============================================================================
# _candles_to_dataframe
# =============================================================================


class TestCandlesToDataframe:
    """Branch coverage for OHLCVModule._candles_to_dataframe."""

    EXPECTED_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]

    def test_empty_list_returns_typed_empty_frame(self):
        module = _make_module()

        df = module._candles_to_dataframe([])

        assert df.empty
        assert list(df.columns) == self.EXPECTED_COLUMNS
        assert str(df["timestamp"].dtype) == "datetime64[ns]"
        for col in self.EXPECTED_COLUMNS[1:]:
            assert str(df[col].dtype) == "float64"

    def test_populated_conversion_and_none_volume_becomes_nan(self):
        module = _make_module()
        candles = [
            _candle(BASE_TS, price="100", volume="7"),
            _candle(BASE_TS + timedelta(hours=1), price="105", volume=None),
        ]

        df = module._candles_to_dataframe(candles)

        assert list(df.columns) == self.EXPECTED_COLUMNS
        assert len(df) == 2
        assert df["open"].tolist() == [100.0, 105.0]
        assert df["high"].tolist() == [101.0, 106.0]
        assert df["low"].tolist() == [99.0, 104.0]
        assert df["close"].tolist() == [100.0, 105.0]
        assert df["volume"].iloc[0] == 7.0
        assert pd.isna(df["volume"].iloc[1])
        assert df["timestamp"].iloc[0] == pd.Timestamp(BASE_TS)
        for col in self.EXPECTED_COLUMNS[1:]:
            assert str(df[col].dtype) == "float64"


# =============================================================================
# _handle_gaps
# =============================================================================


class TestHandleGaps:
    """Branch coverage for OHLCVModule._handle_gaps."""

    def test_single_row_returned_unchanged(self):
        module = _make_module()
        df = module._candles_to_dataframe(_hourly_candles([0]))

        result = module._handle_gaps(df, "ETH", "1h", "nan")

        assert result is df

    def test_continuous_data_has_no_gap_and_gets_sorted(self, caplog):
        module = _make_module()
        # Deliberately unsorted input — _handle_gaps sorts before diffing
        candles = _hourly_candles([2, 0, 1])
        df = module._candles_to_dataframe(candles)

        with caplog.at_level(logging.INFO, logger=MODULE_LOGGER):
            result = module._handle_gaps(df, "ETH", "1h", "nan")

        assert len(result) == 3
        assert result["timestamp"].is_monotonic_increasing
        assert "OHLCV gap detected" not in caplog.text

    def test_duplicate_timestamps_are_not_gaps(self, caplog):
        module = _make_module()
        candles = _hourly_candles([0, 1]) + _hourly_candles([1])
        df = module._candles_to_dataframe(candles)

        with caplog.at_level(logging.INFO, logger=MODULE_LOGGER):
            result = module._handle_gaps(df, "ETH", "1h", "nan")

        # zero diff is not > interval -> no gap detected, rows retained
        assert len(result) == 3
        assert "OHLCV gap detected" not in caplog.text

    def test_nan_strategy_inserts_nan_rows_and_logs_info(self, caplog):
        module = _make_module()
        df = module._candles_to_dataframe(_hourly_candles([0, 1, 2, 4, 5]))

        with caplog.at_level(logging.INFO, logger=MODULE_LOGGER):
            result = module._handle_gaps(df, "ETH", "1h", "nan")

        assert len(result) == 6
        missing_row = result[result["timestamp"] == pd.Timestamp(BASE_TS + timedelta(hours=3))]
        assert len(missing_row) == 1
        assert pd.isna(missing_row["open"].iloc[0])
        assert pd.isna(missing_row["close"].iloc[0])
        info_records = [r for r in caplog.records if "OHLCV gap detected" in r.message]
        assert info_records and info_records[0].levelno == logging.INFO
        assert "missing 1 candles" in info_records[0].message

    def test_ffill_strategy_forward_fills_missing_rows(self):
        module = _make_module()
        df = module._candles_to_dataframe(_hourly_candles([0, 1, 2, 4]))

        result = module._handle_gaps(df, "ETH", "1h", "ffill")

        assert len(result) == 5
        filled_row = result[result["timestamp"] == pd.Timestamp(BASE_TS + timedelta(hours=3))]
        # Forward-filled from the hour-2 candle (price 102)
        assert filled_row["close"].iloc[0] == 102.0
        assert not result[["open", "high", "low", "close"]].isna().any().any()

    def test_drop_strategy_keeps_longest_continuous_segment(self):
        module = _make_module()
        df = module._candles_to_dataframe(_hourly_candles([0, 1, 2]) + _hourly_candles([5, 6, 7, 8]))

        result = module._handle_gaps(df, "ETH", "1h", "drop")

        assert len(result) == 4
        assert result["timestamp"].iloc[0] == pd.Timestamp(BASE_TS + timedelta(hours=5))
        assert result["timestamp"].iloc[-1] == pd.Timestamp(BASE_TS + timedelta(hours=8))

    def test_gap_over_24_hours_logs_warning(self, caplog):
        module = _make_module()
        df = module._candles_to_dataframe(_hourly_candles([0, 1, 31, 32]))

        with caplog.at_level(logging.INFO, logger=MODULE_LOGGER):
            result = module._handle_gaps(df, "ETH", "1h", "drop")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings and "max gap: 30.0 hours" in warnings[0].message
        # drop keeps a longest segment of 2 rows
        assert len(result) == 2

    def test_gap_of_exactly_24_hours_stays_info(self, caplog):
        # Pins the escalation boundary as strictly exclusive (`> 24`):
        # a gap of exactly 24h logs the INFO gap notice, never the WARNING.
        module = _make_module()
        df = module._candles_to_dataframe(_hourly_candles([0, 1, 25, 26]))

        with caplog.at_level(logging.INFO, logger=MODULE_LOGGER):
            module._handle_gaps(df, "ETH", "1h", "drop")

        assert not [r for r in caplog.records if r.levelno == logging.WARNING]
        infos = [r for r in caplog.records if r.levelno == logging.INFO]
        assert infos and "missing 23 candles" in infos[0].message
