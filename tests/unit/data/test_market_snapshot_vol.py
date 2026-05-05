"""Tests for MarketSnapshot volatility methods.

Tests cover:
- realized_vol() integration with RealizedVolatilityCalculator
- vol_cone() integration
- DataEnvelope wrapping and metadata
- Error handling and exception propagation
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

from almanak.framework.data.market_snapshot import (
    MarketSnapshot,
    VolatilityUnavailableError,
    VolConeUnavailableError,
)
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.volatility.realized import (
    RealizedVolatilityCalculator,
    VolatilityResult,
    VolConeResult,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ohlcv_df(n: int = 100) -> pd.DataFrame:
    """Create a DataFrame with OHLCV columns matching MarketSnapshot.ohlcv() output."""
    timestamps = [datetime(2024, 1, 1, tzinfo=UTC) + timedelta(hours=i) for i in range(n)]
    prices = [100.0 + i * 0.01 for i in range(n)]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": prices,
            "high": [p * 1.005 for p in prices],
            "low": [p * 0.995 for p in prices],
            "close": prices,
            "volume": [1000.0] * n,
        }
    )


def _mock_snapshot_with_vol(ohlcv_df: pd.DataFrame | None = None) -> MarketSnapshot:
    """Create a MarketSnapshot with a volatility calculator and mocked OHLCV."""
    ohlcv_module = MagicMock()
    if ohlcv_df is not None:
        ohlcv_module.get_ohlcv.return_value = ohlcv_df
    else:
        ohlcv_module.get_ohlcv.return_value = _make_ohlcv_df()

    calc = RealizedVolatilityCalculator()

    return MarketSnapshot(
        chain="arbitrum",
        wallet_address="0x123",
        ohlcv_module=ohlcv_module,
        volatility_calculator=calc,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRealizedVol:
    """Tests for MarketSnapshot.realized_vol()."""

    def test_basic_realized_vol(self):
        """Should return DataEnvelope[VolatilityResult]."""
        snapshot = _mock_snapshot_with_vol(_make_ohlcv_df(200))
        result = snapshot.realized_vol("WETH", window_days=2, timeframe="1h")

        assert isinstance(result, DataEnvelope)
        assert isinstance(result.value, VolatilityResult)
        assert result.value.annualized_vol > 0
        assert result.value.estimator == "close_to_close"
        assert result.meta.source == "computed"
        assert result.classification == DataClassification.INFORMATIONAL

    def test_parkinson_estimator(self):
        """Should support parkinson estimator."""
        snapshot = _mock_snapshot_with_vol(_make_ohlcv_df(200))
        result = snapshot.realized_vol("WETH", window_days=2, timeframe="1h", estimator="parkinson")
        assert result.value.estimator == "parkinson"
        assert result.value.annualized_vol > 0

    def test_transparent_delegation(self):
        """DataEnvelope should delegate to VolatilityResult attributes."""
        snapshot = _mock_snapshot_with_vol(_make_ohlcv_df(200))
        result = snapshot.realized_vol("WETH", window_days=2, timeframe="1h")

        # Transparent delegation.
        assert result.annualized_vol == result.value.annualized_vol
        assert result.daily_vol == result.value.daily_vol
        assert result.hourly_vol == result.value.hourly_vol

    def test_no_calculator_raises_value_error(self):
        """Should raise ValueError if no calculator configured."""
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x123",
        )
        with pytest.raises(ValueError, match="No volatility calculator"):
            snapshot.realized_vol("WETH")

    def test_empty_ohlcv_raises(self):
        """Should raise VolatilityUnavailableError if OHLCV data is empty."""
        snapshot = _mock_snapshot_with_vol(
            pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
        )
        with pytest.raises(VolatilityUnavailableError):
            snapshot.realized_vol("WETH")

    def test_insufficient_data_propagated(self):
        """InsufficientDataError from calculator should become VolatilityUnavailableError."""
        snapshot = _mock_snapshot_with_vol(_make_ohlcv_df(10))
        with pytest.raises(VolatilityUnavailableError):
            snapshot.realized_vol("WETH", window_days=1, timeframe="1h")

    def test_custom_ohlcv_limit(self):
        """ohlcv_limit kwarg should override auto-calculation."""
        ohlcv_module = MagicMock()
        ohlcv_module.get_ohlcv.return_value = _make_ohlcv_df(200)
        calc = RealizedVolatilityCalculator()

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x123",
            ohlcv_module=ohlcv_module,
            volatility_calculator=calc,
        )
        result = snapshot.realized_vol("WETH", window_days=2, timeframe="1h", ohlcv_limit=150)
        assert isinstance(result.value, VolatilityResult)
        # Verify ohlcv was called with the custom limit.
        ohlcv_module.get_ohlcv.assert_called_once()
        call_kwargs = ohlcv_module.get_ohlcv.call_args
        assert call_kwargs[1].get("limit") == 150 or call_kwargs.kwargs.get("limit") == 150


class TestVolCone:
    """Tests for MarketSnapshot.vol_cone()."""

    def test_basic_vol_cone(self):
        """Should return DataEnvelope[VolConeResult]."""
        snapshot = _mock_snapshot_with_vol(_make_ohlcv_df(3000))
        result = snapshot.vol_cone("WETH", windows=[7], timeframe="1h")

        assert isinstance(result, DataEnvelope)
        assert isinstance(result.value, VolConeResult)
        assert len(result.value.entries) == 1
        assert result.value.entries[0].window_days == 7
        assert result.value.token == "WETH"
        assert result.meta.source == "computed"
        assert result.classification == DataClassification.INFORMATIONAL

    def test_default_windows(self):
        """Default windows should be [7, 14, 30, 90]."""
        snapshot = _mock_snapshot_with_vol(_make_ohlcv_df(8000))
        result = snapshot.vol_cone("WETH", timeframe="1h")
        assert len(result.value.entries) == 4
        assert [e.window_days for e in result.value.entries] == [7, 14, 30, 90]

    def test_no_calculator_raises_value_error(self):
        """Should raise ValueError if no calculator configured."""
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x123",
        )
        with pytest.raises(ValueError, match="No volatility calculator"):
            snapshot.vol_cone("WETH")

    def test_insufficient_data_raises_vol_cone_error(self):
        """Should raise VolConeUnavailableError on insufficient data."""
        snapshot = _mock_snapshot_with_vol(_make_ohlcv_df(10))
        with pytest.raises(VolConeUnavailableError):
            snapshot.vol_cone("WETH", windows=[7], timeframe="1h")

    def test_vol_cone_transparent_delegation(self):
        """DataEnvelope should delegate to VolConeResult attributes."""
        snapshot = _mock_snapshot_with_vol(_make_ohlcv_df(3000))
        result = snapshot.vol_cone("WETH", windows=[7], timeframe="1h")
        assert result.entries == result.value.entries
        assert result.token == result.value.token


class TestExceptionClasses:
    """Tests for new exception classes."""

    def test_volatility_unavailable_error(self):
        """VolatilityUnavailableError should have token and reason."""
        err = VolatilityUnavailableError("WETH", "no data")
        assert err.token == "WETH"
        assert err.reason == "no data"
        assert "WETH" in str(err)
        assert "no data" in str(err)

    def test_vol_cone_unavailable_error(self):
        """VolConeUnavailableError should have token and reason."""
        err = VolConeUnavailableError("WETH", "insufficient history")
        assert err.token == "WETH"
        assert err.reason == "insufficient history"
        assert "WETH" in str(err)
