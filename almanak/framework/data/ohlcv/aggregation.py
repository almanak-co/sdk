"""Shared helpers for exact aggregation of provider-native OHLCV candles."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

from ..interfaces import OHLCVCandle


class _CandleLike(Protocol):
    """Structural price-only candle input accepted by the aggregator."""

    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


def open_time_from_close_time(close_time: datetime, native_stride_seconds: int) -> datetime:
    """Convert a provider close timestamp to the SDK candle-open convention."""
    if native_stride_seconds <= 0:
        raise ValueError(f"native_stride_seconds must be positive, got {native_stride_seconds}")
    return close_time - timedelta(seconds=native_stride_seconds)


def aggregate_complete_candles(
    candles: Sequence[_CandleLike],
    *,
    native_stride_seconds: int,
    target_stride_seconds: int,
) -> list[OHLCVCandle]:
    """Aggregate only complete, contiguous native-candle buckets.

    Inputs use the SDK convention that ``timestamp`` is the candle open time.
    A target bucket is emitted only when every expected native candle is
    present at its exact boundary. This prevents partial leading, trailing, or
    gap-containing buckets from carrying the target timeframe label.
    """
    if native_stride_seconds <= 0:
        raise ValueError(f"native_stride_seconds must be positive, got {native_stride_seconds}")
    if target_stride_seconds <= native_stride_seconds:
        raise ValueError(
            "target_stride_seconds must be greater than native_stride_seconds; "
            f"got target={target_stride_seconds}, native={native_stride_seconds}"
        )
    if target_stride_seconds % native_stride_seconds:
        raise ValueError(
            "target_stride_seconds must be an exact multiple of native_stride_seconds; "
            f"got target={target_stride_seconds}, native={native_stride_seconds}"
        )

    expected_count = target_stride_seconds // native_stride_seconds
    buckets: dict[int, list[_CandleLike]] = {}
    for candle in candles:
        timestamp_seconds = int(candle.timestamp.timestamp())
        bucket_start = timestamp_seconds // target_stride_seconds * target_stride_seconds
        buckets.setdefault(bucket_start, []).append(candle)

    aggregated: list[OHLCVCandle] = []
    for bucket_start in sorted(buckets):
        group = sorted(buckets[bucket_start], key=lambda candle: candle.timestamp)
        actual_timestamps = tuple(candle.timestamp for candle in group)
        expected_timestamps = tuple(
            datetime.fromtimestamp(bucket_start + index * native_stride_seconds, tz=UTC)
            for index in range(expected_count)
        )
        if actual_timestamps != expected_timestamps:
            continue

        aggregated.append(
            OHLCVCandle(
                timestamp=datetime.fromtimestamp(bucket_start, tz=UTC),
                open=group[0].open,
                high=max(candle.high for candle in group),
                low=min(candle.low for candle in group),
                close=group[-1].close,
                volume=None,
            )
        )
    return aggregated


__all__ = ["aggregate_complete_candles", "open_time_from_close_time"]
