"""Tests for Quant Data Layer exception classes."""

from __future__ import annotations

import pytest

from almanak.framework.data.exceptions import (
    DataUnavailableError,
    LowConfidenceError,
    StaleDataError,
)
from almanak.framework.data.interfaces import DataSourceError


class TestDataUnavailableError:
    def test_inherits_from_data_source_error(self):
        assert issubclass(DataUnavailableError, DataSourceError)

    def test_construction(self):
        err = DataUnavailableError(
            data_type="pool_price",
            instrument="WETH/USDC",
            reason="all providers failed",
        )
        assert err.data_type == "pool_price"
        assert err.instrument == "WETH/USDC"
        assert err.reason == "all providers failed"

    def test_message_format(self):
        err = DataUnavailableError("ohlcv", "ETH", "timeout")
        assert "ohlcv" in str(err)
        assert "ETH" in str(err)
        assert "timeout" in str(err)

    def test_catchable_as_base(self):
        with pytest.raises(DataSourceError):
            raise DataUnavailableError("price", "BTC", "down")


class TestStaleDataError:
    def test_inherits_from_data_source_error(self):
        assert issubclass(StaleDataError, DataSourceError)

    def test_construction(self):
        err = StaleDataError(source="alchemy_rpc", staleness_ms=5000, threshold_ms=2000)
        assert err.source == "alchemy_rpc"
        assert err.staleness_ms == 5000
        assert err.threshold_ms == 2000

    def test_message_format(self):
        err = StaleDataError("binance", 10000, 3000)
        assert "binance" in str(err)
        assert "10000" in str(err)
        assert "3000" in str(err)


class TestLowConfidenceError:
    def test_inherits_from_data_source_error(self):
        assert issubclass(LowConfidenceError, DataSourceError)

    def test_construction(self):
        err = LowConfidenceError(source="defillama", confidence=0.4, threshold=0.7)
        assert err.source == "defillama"
        assert err.confidence == 0.4
        assert err.threshold == 0.7

    def test_message_format(self):
        err = LowConfidenceError("coingecko", 0.35, 0.5)
        assert "coingecko" in str(err)
        assert "0.35" in str(err)
        assert "0.50" in str(err)
