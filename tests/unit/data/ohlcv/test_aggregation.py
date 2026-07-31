"""Direct contract tests for exact OHLCV candle aggregation."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.ohlcv.aggregation import aggregate_complete_candles, open_time_from_close_time


def _candle(timestamp: datetime, price: str) -> OHLCVCandle:
    value = Decimal(price)
    return OHLCVCandle(
        timestamp=timestamp,
        open=value,
        high=value + Decimal("2"),
        low=value - Decimal("2"),
        close=value + Decimal("1"),
        volume=Decimal("10"),
    )


def test_open_time_from_close_time_preserves_fractional_precision() -> None:
    close_time = datetime(2026, 1, 1, 1, 0, 0, 123456, tzinfo=UTC)

    assert open_time_from_close_time(close_time, 1800) == datetime(
        2026,
        1,
        1,
        0,
        30,
        0,
        123456,
        tzinfo=UTC,
    )


@pytest.mark.parametrize("native_stride_seconds", [0, -1])
def test_open_time_from_close_time_rejects_nonpositive_stride(native_stride_seconds: int) -> None:
    with pytest.raises(ValueError, match="native_stride_seconds must be positive"):
        open_time_from_close_time(datetime(2026, 1, 1, tzinfo=UTC), native_stride_seconds)


def test_aggregate_complete_candles_emits_aligned_bucket_with_correct_ohlc() -> None:
    bucket_start = datetime(2026, 1, 1, tzinfo=UTC)
    candles = [
        _candle(bucket_start + timedelta(minutes=30), "105"),
        _candle(bucket_start, "100"),
    ]

    result = aggregate_complete_candles(
        candles,
        native_stride_seconds=1800,
        target_stride_seconds=3600,
    )

    assert len(result) == 1
    assert result[0] == OHLCVCandle(
        timestamp=bucket_start,
        open=Decimal("100"),
        high=Decimal("107"),
        low=Decimal("98"),
        close=Decimal("106"),
        volume=None,
    )


@pytest.mark.parametrize(
    "candles",
    [
        pytest.param(
            [
                _candle(datetime(2026, 1, 1, tzinfo=UTC), "100"),
                _candle(datetime(2026, 1, 1, 1, 0, tzinfo=UTC), "110"),
            ],
            id="gap",
        ),
        pytest.param(
            [
                _candle(datetime(2026, 1, 1, tzinfo=UTC), "100"),
                _candle(datetime(2026, 1, 1, tzinfo=UTC), "101"),
            ],
            id="duplicate",
        ),
        pytest.param(
            [
                _candle(datetime(2026, 1, 1, 0, 0, 0, 500000, tzinfo=UTC), "100"),
                _candle(datetime(2026, 1, 1, 0, 30, 0, 500000, tzinfo=UTC), "105"),
            ],
            id="fractional",
        ),
    ],
)
def test_aggregate_complete_candles_excludes_nonexact_buckets(candles: list[OHLCVCandle]) -> None:
    assert (
        aggregate_complete_candles(
            candles,
            native_stride_seconds=1800,
            target_stride_seconds=3600,
        )
        == []
    )


@pytest.mark.parametrize(
    ("native_stride_seconds", "target_stride_seconds", "message"),
    [
        (0, 3600, "native_stride_seconds must be positive"),
        (-1, 3600, "native_stride_seconds must be positive"),
        (1800, 1800, "target_stride_seconds must be greater"),
        (1800, 900, "target_stride_seconds must be greater"),
        (1800, 4000, "target_stride_seconds must be an exact multiple"),
    ],
)
def test_aggregate_complete_candles_rejects_invalid_strides(
    native_stride_seconds: int,
    target_stride_seconds: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        aggregate_complete_candles(
            [],
            native_stride_seconds=native_stride_seconds,
            target_stride_seconds=target_stride_seconds,
        )
