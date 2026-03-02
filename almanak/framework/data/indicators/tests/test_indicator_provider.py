"""Tests for IndicatorProvider wiring and sync wrapper factories.

Tests the generalized provider pattern for calculator-backed TA indicators
(MACD, Bollinger Bands, Stochastic, ATR, SMA, EMA) in MarketSnapshot,
and the sync wrapper factory functions.
"""

import asyncio
from collections.abc import Coroutine
from decimal import Decimal
from typing import Any, TypeVar
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.indicators.sync_wrappers import (
    create_sync_adx_func,
    create_sync_atr_func,
    create_sync_bollinger_func,
    create_sync_cci_func,
    create_sync_ema_func,
    create_sync_ichimoku_func,
    create_sync_macd_func,
    create_sync_obv_func,
    create_sync_rsi_func,
    create_sync_sma_func,
    create_sync_stochastic_func,
)
from almanak.framework.strategies.intent_strategy import (
    ADXData,
    ATRData,
    BollingerBandsData,
    CCIData,
    IchimokuData,
    IndicatorProvider,
    MACDData,
    MAData,
    MarketSnapshot,
    OBVData,
    RSIData,
    StochasticData,
)

T = TypeVar("T")


def run_async[T](coro: Coroutine[Any, Any, T]) -> T:
    """Helper to run async functions in sync tests."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# =============================================================================
# Test Data
# =============================================================================

SAMPLE_MACD = MACDData(
    macd_line=Decimal("1.5"),
    signal_line=Decimal("1.0"),
    histogram=Decimal("0.5"),
    fast_period=12,
    slow_period=26,
    signal_period=9,
)

SAMPLE_BOLLINGER = BollingerBandsData(
    upper_band=Decimal("3100"),
    middle_band=Decimal("3000"),
    lower_band=Decimal("2900"),
    bandwidth=Decimal("0.067"),
    percent_b=Decimal("0.5"),
    period=20,
    std_dev=2.0,
)

SAMPLE_STOCHASTIC = StochasticData(
    k_value=Decimal("25"),
    d_value=Decimal("30"),
    k_period=14,
    d_period=3,
)

SAMPLE_ATR = ATRData(
    value=Decimal("50"),
    value_percent=Decimal("2.0"),
    period=14,
)

SAMPLE_SMA = MAData(
    value=Decimal("3000"),
    ma_type="SMA",
    period=20,
    current_price=Decimal("3100"),
)

SAMPLE_EMA = MAData(
    value=Decimal("3050"),
    ma_type="EMA",
    period=12,
    current_price=Decimal("3100"),
)

SAMPLE_ADX = ADXData(
    adx=Decimal("30"),
    plus_di=Decimal("25"),
    minus_di=Decimal("15"),
    period=14,
)

SAMPLE_OBV = OBVData(
    obv=Decimal("1000000"),
    signal_line=Decimal("950000"),
    signal_period=21,
)

SAMPLE_CCI = CCIData(
    value=Decimal("-110"),
    period=20,
)

SAMPLE_ICHIMOKU = IchimokuData(
    tenkan_sen=Decimal("3050"),
    kijun_sen=Decimal("3000"),
    senkou_span_a=Decimal("3025"),
    senkou_span_b=Decimal("2950"),
    chikou_span=Decimal("3100"),
    current_price=Decimal("3100"),
    tenkan_period=9,
    kijun_period=26,
    senkou_b_period=52,
)


def _make_snapshot(indicator_provider=None, rsi_provider=None):
    """Create a MarketSnapshot with optional providers."""
    return MarketSnapshot(
        chain="arbitrum",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        indicator_provider=indicator_provider,
        rsi_provider=rsi_provider,
    )


# =============================================================================
# MACD Provider Tests
# =============================================================================


class TestMACDProvider:
    def test_macd_with_provider(self):
        mock_macd = MagicMock(return_value=SAMPLE_MACD)
        provider = IndicatorProvider(macd=mock_macd)
        snap = _make_snapshot(indicator_provider=provider)

        result = snap.macd("WETH")
        assert result == SAMPLE_MACD
        mock_macd.assert_called_once_with("WETH", 12, 26, 9, timeframe="4h")

    def test_macd_provider_caches_result(self):
        mock_macd = MagicMock(return_value=SAMPLE_MACD)
        provider = IndicatorProvider(macd=mock_macd)
        snap = _make_snapshot(indicator_provider=provider)

        snap.macd("WETH")
        snap.macd("WETH")
        mock_macd.assert_called_once()

    def test_macd_provider_different_params_different_cache(self):
        mock_macd = MagicMock(return_value=SAMPLE_MACD)
        provider = IndicatorProvider(macd=mock_macd)
        snap = _make_snapshot(indicator_provider=provider)

        snap.macd("WETH", fast_period=12)
        snap.macd("WETH", fast_period=8)
        assert mock_macd.call_count == 2

    def test_macd_provider_different_timeframe_different_cache(self):
        mock_macd = MagicMock(return_value=SAMPLE_MACD)
        provider = IndicatorProvider(macd=mock_macd)
        snap = _make_snapshot(indicator_provider=provider)

        snap.macd("WETH", timeframe="4h")
        snap.macd("WETH", timeframe="1h")
        assert mock_macd.call_count == 2

    def test_macd_prepopulated_takes_priority(self):
        mock_macd = MagicMock(return_value=SAMPLE_MACD)
        provider = IndicatorProvider(macd=mock_macd)
        snap = _make_snapshot(indicator_provider=provider)
        snap.set_macd("WETH", SAMPLE_MACD)

        result = snap.macd("WETH")
        assert result == SAMPLE_MACD
        mock_macd.assert_not_called()

    def test_macd_no_provider_raises(self):
        snap = _make_snapshot()
        with pytest.raises(ValueError, match="MACD data not available"):
            snap.macd("WETH")

    def test_macd_provider_failure_raises(self):
        mock_macd = MagicMock(side_effect=RuntimeError("calc failed"))
        provider = IndicatorProvider(macd=mock_macd)
        snap = _make_snapshot(indicator_provider=provider)

        with pytest.raises(ValueError, match="MACD data not available"):
            snap.macd("WETH")


# =============================================================================
# Bollinger Bands Provider Tests
# =============================================================================


class TestBollingerProvider:
    def test_bollinger_with_provider(self):
        mock_bb = MagicMock(return_value=SAMPLE_BOLLINGER)
        provider = IndicatorProvider(bollinger=mock_bb)
        snap = _make_snapshot(indicator_provider=provider)

        result = snap.bollinger_bands("WETH")
        assert result == SAMPLE_BOLLINGER
        mock_bb.assert_called_once_with("WETH", 20, 2.0, timeframe="4h")

    def test_bollinger_provider_caches_result(self):
        mock_bb = MagicMock(return_value=SAMPLE_BOLLINGER)
        provider = IndicatorProvider(bollinger=mock_bb)
        snap = _make_snapshot(indicator_provider=provider)

        snap.bollinger_bands("WETH")
        snap.bollinger_bands("WETH")
        mock_bb.assert_called_once()

    def test_bollinger_provider_different_params_different_cache(self):
        mock_bb = MagicMock(return_value=SAMPLE_BOLLINGER)
        provider = IndicatorProvider(bollinger=mock_bb)
        snap = _make_snapshot(indicator_provider=provider)

        snap.bollinger_bands("WETH", period=20)
        snap.bollinger_bands("WETH", period=10)
        assert mock_bb.call_count == 2

    def test_bollinger_provider_different_timeframe_different_cache(self):
        mock_bb = MagicMock(return_value=SAMPLE_BOLLINGER)
        provider = IndicatorProvider(bollinger=mock_bb)
        snap = _make_snapshot(indicator_provider=provider)

        snap.bollinger_bands("WETH", timeframe="4h")
        snap.bollinger_bands("WETH", timeframe="1h")
        assert mock_bb.call_count == 2

    def test_bollinger_prepopulated_takes_priority(self):
        mock_bb = MagicMock(return_value=SAMPLE_BOLLINGER)
        provider = IndicatorProvider(bollinger=mock_bb)
        snap = _make_snapshot(indicator_provider=provider)
        snap.set_bollinger_bands("WETH", SAMPLE_BOLLINGER)

        result = snap.bollinger_bands("WETH")
        assert result == SAMPLE_BOLLINGER
        mock_bb.assert_not_called()

    def test_bollinger_no_provider_raises(self):
        snap = _make_snapshot()
        with pytest.raises(ValueError, match="Bollinger Bands data not available"):
            snap.bollinger_bands("WETH")

    def test_bollinger_provider_failure_raises(self):
        mock_bb = MagicMock(side_effect=RuntimeError("calc failed"))
        provider = IndicatorProvider(bollinger=mock_bb)
        snap = _make_snapshot(indicator_provider=provider)

        with pytest.raises(ValueError, match="Bollinger Bands data not available"):
            snap.bollinger_bands("WETH")


# =============================================================================
# Stochastic Provider Tests
# =============================================================================


class TestStochasticProvider:
    def test_stochastic_with_provider(self):
        mock_stoch = MagicMock(return_value=SAMPLE_STOCHASTIC)
        provider = IndicatorProvider(stochastic=mock_stoch)
        snap = _make_snapshot(indicator_provider=provider)

        result = snap.stochastic("WETH")
        assert result == SAMPLE_STOCHASTIC
        mock_stoch.assert_called_once_with("WETH", 14, 3, timeframe="4h")

    def test_stochastic_provider_caches_result(self):
        mock_stoch = MagicMock(return_value=SAMPLE_STOCHASTIC)
        provider = IndicatorProvider(stochastic=mock_stoch)
        snap = _make_snapshot(indicator_provider=provider)

        snap.stochastic("WETH")
        snap.stochastic("WETH")
        mock_stoch.assert_called_once()

    def test_stochastic_provider_different_timeframe_different_cache(self):
        mock_stoch = MagicMock(return_value=SAMPLE_STOCHASTIC)
        provider = IndicatorProvider(stochastic=mock_stoch)
        snap = _make_snapshot(indicator_provider=provider)

        snap.stochastic("WETH", timeframe="4h")
        snap.stochastic("WETH", timeframe="1h")
        assert mock_stoch.call_count == 2

    def test_stochastic_prepopulated_takes_priority(self):
        mock_stoch = MagicMock(return_value=SAMPLE_STOCHASTIC)
        provider = IndicatorProvider(stochastic=mock_stoch)
        snap = _make_snapshot(indicator_provider=provider)
        snap.set_stochastic("WETH", SAMPLE_STOCHASTIC)

        result = snap.stochastic("WETH")
        assert result == SAMPLE_STOCHASTIC
        mock_stoch.assert_not_called()

    def test_stochastic_no_provider_raises(self):
        snap = _make_snapshot()
        with pytest.raises(ValueError, match="Stochastic data not available"):
            snap.stochastic("WETH")

    def test_stochastic_provider_failure_raises(self):
        mock_stoch = MagicMock(side_effect=RuntimeError("calc failed"))
        provider = IndicatorProvider(stochastic=mock_stoch)
        snap = _make_snapshot(indicator_provider=provider)

        with pytest.raises(ValueError, match="Stochastic data not available"):
            snap.stochastic("WETH")


# =============================================================================
# ATR Provider Tests
# =============================================================================


class TestATRProvider:
    def test_atr_with_provider(self):
        mock_atr = MagicMock(return_value=SAMPLE_ATR)
        provider = IndicatorProvider(atr=mock_atr)
        snap = _make_snapshot(indicator_provider=provider)

        result = snap.atr("WETH")
        assert result == SAMPLE_ATR
        mock_atr.assert_called_once_with("WETH", 14, timeframe="4h")

    def test_atr_provider_caches_result(self):
        mock_atr = MagicMock(return_value=SAMPLE_ATR)
        provider = IndicatorProvider(atr=mock_atr)
        snap = _make_snapshot(indicator_provider=provider)

        snap.atr("WETH")
        snap.atr("WETH")
        mock_atr.assert_called_once()

    def test_atr_provider_different_timeframe_different_cache(self):
        mock_atr = MagicMock(return_value=SAMPLE_ATR)
        provider = IndicatorProvider(atr=mock_atr)
        snap = _make_snapshot(indicator_provider=provider)

        snap.atr("WETH", timeframe="4h")
        snap.atr("WETH", timeframe="1h")
        assert mock_atr.call_count == 2

    def test_atr_prepopulated_takes_priority(self):
        mock_atr = MagicMock(return_value=SAMPLE_ATR)
        provider = IndicatorProvider(atr=mock_atr)
        snap = _make_snapshot(indicator_provider=provider)
        snap.set_atr("WETH", SAMPLE_ATR)

        result = snap.atr("WETH")
        assert result == SAMPLE_ATR
        mock_atr.assert_not_called()

    def test_atr_no_provider_raises(self):
        snap = _make_snapshot()
        with pytest.raises(ValueError, match="ATR data not available"):
            snap.atr("WETH")

    def test_atr_provider_failure_raises(self):
        mock_atr = MagicMock(side_effect=RuntimeError("calc failed"))
        provider = IndicatorProvider(atr=mock_atr)
        snap = _make_snapshot(indicator_provider=provider)

        with pytest.raises(ValueError, match="ATR data not available"):
            snap.atr("WETH")


# =============================================================================
# SMA Provider Tests
# =============================================================================


class TestSMAProvider:
    def test_sma_with_provider(self):
        mock_sma = MagicMock(return_value=SAMPLE_SMA)
        provider = IndicatorProvider(sma=mock_sma)
        snap = _make_snapshot(indicator_provider=provider)

        result = snap.sma("WETH")
        assert result == SAMPLE_SMA
        mock_sma.assert_called_once_with("WETH", 20, timeframe="4h")

    def test_sma_provider_caches_result(self):
        mock_sma = MagicMock(return_value=SAMPLE_SMA)
        provider = IndicatorProvider(sma=mock_sma)
        snap = _make_snapshot(indicator_provider=provider)

        snap.sma("WETH")
        snap.sma("WETH")
        mock_sma.assert_called_once()

    def test_sma_provider_different_params_different_cache(self):
        mock_sma = MagicMock(return_value=SAMPLE_SMA)
        provider = IndicatorProvider(sma=mock_sma)
        snap = _make_snapshot(indicator_provider=provider)

        snap.sma("WETH", period=20)
        snap.sma("WETH", period=50)
        assert mock_sma.call_count == 2

    def test_sma_provider_different_timeframe_different_cache(self):
        mock_sma = MagicMock(return_value=SAMPLE_SMA)
        provider = IndicatorProvider(sma=mock_sma)
        snap = _make_snapshot(indicator_provider=provider)

        snap.sma("WETH", timeframe="4h")
        snap.sma("WETH", timeframe="1h")
        assert mock_sma.call_count == 2

    def test_sma_prepopulated_takes_priority(self):
        mock_sma = MagicMock(return_value=SAMPLE_SMA)
        provider = IndicatorProvider(sma=mock_sma)
        snap = _make_snapshot(indicator_provider=provider)
        snap.set_ma("WETH", SAMPLE_SMA, ma_type="SMA", period=20)

        result = snap.sma("WETH")
        assert result == SAMPLE_SMA
        mock_sma.assert_not_called()

    def test_sma_no_provider_raises(self):
        snap = _make_snapshot()
        with pytest.raises(ValueError, match="SMA data not available"):
            snap.sma("WETH")

    def test_sma_provider_failure_raises(self):
        mock_sma = MagicMock(side_effect=RuntimeError("calc failed"))
        provider = IndicatorProvider(sma=mock_sma)
        snap = _make_snapshot(indicator_provider=provider)

        with pytest.raises(ValueError, match="SMA data not available"):
            snap.sma("WETH")


# =============================================================================
# EMA Provider Tests
# =============================================================================


class TestEMAProvider:
    def test_ema_with_provider(self):
        mock_ema = MagicMock(return_value=SAMPLE_EMA)
        provider = IndicatorProvider(ema=mock_ema)
        snap = _make_snapshot(indicator_provider=provider)

        result = snap.ema("WETH")
        assert result == SAMPLE_EMA
        mock_ema.assert_called_once_with("WETH", 12, timeframe="4h")

    def test_ema_provider_caches_result(self):
        mock_ema = MagicMock(return_value=SAMPLE_EMA)
        provider = IndicatorProvider(ema=mock_ema)
        snap = _make_snapshot(indicator_provider=provider)

        snap.ema("WETH")
        snap.ema("WETH")
        mock_ema.assert_called_once()

    def test_ema_provider_different_timeframe_different_cache(self):
        mock_ema = MagicMock(return_value=SAMPLE_EMA)
        provider = IndicatorProvider(ema=mock_ema)
        snap = _make_snapshot(indicator_provider=provider)

        snap.ema("WETH", timeframe="4h")
        snap.ema("WETH", timeframe="1h")
        assert mock_ema.call_count == 2

    def test_ema_prepopulated_takes_priority(self):
        mock_ema = MagicMock(return_value=SAMPLE_EMA)
        provider = IndicatorProvider(ema=mock_ema)
        snap = _make_snapshot(indicator_provider=provider)
        snap.set_ma("WETH", SAMPLE_EMA, ma_type="EMA", period=12)

        result = snap.ema("WETH")
        assert result == SAMPLE_EMA
        mock_ema.assert_not_called()

    def test_ema_no_provider_raises(self):
        snap = _make_snapshot()
        with pytest.raises(ValueError, match="EMA data not available"):
            snap.ema("WETH")

    def test_ema_provider_failure_raises(self):
        mock_ema = MagicMock(side_effect=RuntimeError("calc failed"))
        provider = IndicatorProvider(ema=mock_ema)
        snap = _make_snapshot(indicator_provider=provider)

        with pytest.raises(ValueError, match="EMA data not available"):
            snap.ema("WETH")


# =============================================================================
# ADX Provider Tests
# =============================================================================


class TestADXProvider:
    def test_adx_with_provider(self):
        mock_adx = MagicMock(return_value=SAMPLE_ADX)
        provider = IndicatorProvider(adx=mock_adx)
        snap = _make_snapshot(indicator_provider=provider)

        result = snap.adx("WETH")
        assert result == SAMPLE_ADX
        mock_adx.assert_called_once_with("WETH", period=14, timeframe="4h")

    def test_adx_provider_caches_result(self):
        mock_adx = MagicMock(return_value=SAMPLE_ADX)
        provider = IndicatorProvider(adx=mock_adx)
        snap = _make_snapshot(indicator_provider=provider)

        snap.adx("WETH")
        snap.adx("WETH")
        mock_adx.assert_called_once()

    def test_adx_prepopulated_takes_priority(self):
        mock_adx = MagicMock(return_value=SAMPLE_ADX)
        provider = IndicatorProvider(adx=mock_adx)
        snap = _make_snapshot(indicator_provider=provider)
        snap.set_adx("WETH", SAMPLE_ADX)

        result = snap.adx("WETH")
        assert result == SAMPLE_ADX
        mock_adx.assert_not_called()

    def test_adx_no_provider_raises(self):
        snap = _make_snapshot()
        with pytest.raises(ValueError, match="ADX data not available"):
            snap.adx("WETH")

    def test_adx_provider_failure_raises(self):
        mock_adx = MagicMock(side_effect=RuntimeError("calc failed"))
        provider = IndicatorProvider(adx=mock_adx)
        snap = _make_snapshot(indicator_provider=provider)

        with pytest.raises(ValueError, match="ADX data not available"):
            snap.adx("WETH")


# =============================================================================
# OBV Provider Tests
# =============================================================================


class TestOBVProvider:
    def test_obv_with_provider(self):
        mock_obv = MagicMock(return_value=SAMPLE_OBV)
        provider = IndicatorProvider(obv=mock_obv)
        snap = _make_snapshot(indicator_provider=provider)

        result = snap.obv("WETH")
        assert result == SAMPLE_OBV
        mock_obv.assert_called_once_with("WETH", signal_period=21, timeframe="4h")

    def test_obv_provider_caches_result(self):
        mock_obv = MagicMock(return_value=SAMPLE_OBV)
        provider = IndicatorProvider(obv=mock_obv)
        snap = _make_snapshot(indicator_provider=provider)

        snap.obv("WETH")
        snap.obv("WETH")
        mock_obv.assert_called_once()

    def test_obv_prepopulated_takes_priority(self):
        mock_obv = MagicMock(return_value=SAMPLE_OBV)
        provider = IndicatorProvider(obv=mock_obv)
        snap = _make_snapshot(indicator_provider=provider)
        snap.set_obv("WETH", SAMPLE_OBV)

        result = snap.obv("WETH")
        assert result == SAMPLE_OBV
        mock_obv.assert_not_called()

    def test_obv_no_provider_raises(self):
        snap = _make_snapshot()
        with pytest.raises(ValueError, match="OBV data not available"):
            snap.obv("WETH")

    def test_obv_provider_failure_raises(self):
        mock_obv = MagicMock(side_effect=RuntimeError("calc failed"))
        provider = IndicatorProvider(obv=mock_obv)
        snap = _make_snapshot(indicator_provider=provider)

        with pytest.raises(ValueError, match="OBV data not available"):
            snap.obv("WETH")


# =============================================================================
# CCI Provider Tests
# =============================================================================


class TestCCIProvider:
    def test_cci_with_provider(self):
        mock_cci = MagicMock(return_value=SAMPLE_CCI)
        provider = IndicatorProvider(cci=mock_cci)
        snap = _make_snapshot(indicator_provider=provider)

        result = snap.cci("WETH")
        assert result == SAMPLE_CCI
        mock_cci.assert_called_once_with("WETH", period=20, timeframe="4h")

    def test_cci_provider_caches_result(self):
        mock_cci = MagicMock(return_value=SAMPLE_CCI)
        provider = IndicatorProvider(cci=mock_cci)
        snap = _make_snapshot(indicator_provider=provider)

        snap.cci("WETH")
        snap.cci("WETH")
        mock_cci.assert_called_once()

    def test_cci_prepopulated_takes_priority(self):
        mock_cci = MagicMock(return_value=SAMPLE_CCI)
        provider = IndicatorProvider(cci=mock_cci)
        snap = _make_snapshot(indicator_provider=provider)
        snap.set_cci("WETH", SAMPLE_CCI)

        result = snap.cci("WETH")
        assert result == SAMPLE_CCI
        mock_cci.assert_not_called()

    def test_cci_no_provider_raises(self):
        snap = _make_snapshot()
        with pytest.raises(ValueError, match="CCI data not available"):
            snap.cci("WETH")

    def test_cci_provider_failure_raises(self):
        mock_cci = MagicMock(side_effect=RuntimeError("calc failed"))
        provider = IndicatorProvider(cci=mock_cci)
        snap = _make_snapshot(indicator_provider=provider)

        with pytest.raises(ValueError, match="CCI data not available"):
            snap.cci("WETH")


# =============================================================================
# Ichimoku Provider Tests
# =============================================================================


class TestIchimokuProvider:
    def test_ichimoku_with_provider(self):
        mock_ichimoku = MagicMock(return_value=SAMPLE_ICHIMOKU)
        provider = IndicatorProvider(ichimoku=mock_ichimoku)
        snap = _make_snapshot(indicator_provider=provider)

        result = snap.ichimoku("WETH")
        assert result == SAMPLE_ICHIMOKU
        mock_ichimoku.assert_called_once_with(
            "WETH",
            tenkan_period=9,
            kijun_period=26,
            senkou_b_period=52,
            timeframe="4h",
        )

    def test_ichimoku_provider_caches_result(self):
        mock_ichimoku = MagicMock(return_value=SAMPLE_ICHIMOKU)
        provider = IndicatorProvider(ichimoku=mock_ichimoku)
        snap = _make_snapshot(indicator_provider=provider)

        snap.ichimoku("WETH")
        snap.ichimoku("WETH")
        mock_ichimoku.assert_called_once()

    def test_ichimoku_prepopulated_takes_priority(self):
        mock_ichimoku = MagicMock(return_value=SAMPLE_ICHIMOKU)
        provider = IndicatorProvider(ichimoku=mock_ichimoku)
        snap = _make_snapshot(indicator_provider=provider)
        snap.set_ichimoku("WETH", SAMPLE_ICHIMOKU)

        result = snap.ichimoku("WETH")
        assert result == SAMPLE_ICHIMOKU
        mock_ichimoku.assert_not_called()

    def test_ichimoku_no_provider_raises(self):
        snap = _make_snapshot()
        with pytest.raises(ValueError, match="Ichimoku data not available"):
            snap.ichimoku("WETH")

    def test_ichimoku_provider_failure_raises(self):
        mock_ichimoku = MagicMock(side_effect=RuntimeError("calc failed"))
        provider = IndicatorProvider(ichimoku=mock_ichimoku)
        snap = _make_snapshot(indicator_provider=provider)

        with pytest.raises(ValueError, match="Ichimoku data not available"):
            snap.ichimoku("WETH")


# =============================================================================
# RSI Provider Tests (with timeframe)
# =============================================================================


class TestRSIProviderTimeframe:
    def test_rsi_with_provider_and_timeframe(self):
        sample_rsi = RSIData(value=Decimal("45"), period=14)
        mock_rsi = MagicMock(return_value=sample_rsi)
        snap = _make_snapshot(rsi_provider=mock_rsi)

        result = snap.rsi("WETH", timeframe="1h")
        assert result == sample_rsi
        mock_rsi.assert_called_once_with("WETH", 14, timeframe="1h")

    def test_rsi_provider_caches_by_timeframe(self):
        sample_rsi = RSIData(value=Decimal("45"), period=14)
        mock_rsi = MagicMock(return_value=sample_rsi)
        snap = _make_snapshot(rsi_provider=mock_rsi)

        snap.rsi("WETH", timeframe="4h")
        snap.rsi("WETH", timeframe="1h")
        assert mock_rsi.call_count == 2

    def test_rsi_backward_compat_two_arg_provider(self):
        """Old-style RSI providers that only accept (token, period) still work."""
        sample_rsi = RSIData(value=Decimal("45"), period=14)

        def legacy_rsi(_token: str, _period: int) -> RSIData:
            return sample_rsi

        snap = _make_snapshot(rsi_provider=legacy_rsi)
        result = snap.rsi("WETH", timeframe="1h")
        assert result == sample_rsi

    def test_rsi_prepopulated_takes_priority(self):
        sample_rsi = RSIData(value=Decimal("45"), period=14)
        mock_rsi = MagicMock(return_value=sample_rsi)
        snap = _make_snapshot(rsi_provider=mock_rsi)
        snap.set_rsi("WETH", sample_rsi)

        result = snap.rsi("WETH")
        assert result == sample_rsi
        mock_rsi.assert_not_called()


# =============================================================================
# Sync Wrapper Factory Tests
# =============================================================================


class TestCreateSyncMACDFunc:
    def test_sync_macd_returns_macd_data(self):
        from almanak.framework.data.indicators.base import MACDResult

        mock_calc = MagicMock()
        mock_calc.calculate_macd = AsyncMock(return_value=MACDResult(macd_line=1.5, signal_line=1.0, histogram=0.5))

        sync_fn = create_sync_macd_func(mock_calc)
        result = sync_fn("WETH")

        assert isinstance(result, MACDData)
        assert result.macd_line == Decimal("1.5")
        assert result.signal_line == Decimal("1.0")
        assert result.histogram == Decimal("0.5")
        assert result.fast_period == 12
        assert result.slow_period == 26
        assert result.signal_period == 9

    def test_sync_macd_passes_params(self):
        from almanak.framework.data.indicators.base import MACDResult

        mock_calc = MagicMock()
        mock_calc.calculate_macd = AsyncMock(return_value=MACDResult(macd_line=0.0, signal_line=0.0, histogram=0.0))

        sync_fn = create_sync_macd_func(mock_calc)
        sync_fn("WETH", fast_period=8, slow_period=17, signal_period=5, timeframe="1h")

        mock_calc.calculate_macd.assert_awaited_once_with("WETH", 8, 17, 5, timeframe="1h")

    def test_sync_macd_passes_timeframe(self):
        from almanak.framework.data.indicators.base import MACDResult

        mock_calc = MagicMock()
        mock_calc.calculate_macd = AsyncMock(return_value=MACDResult(macd_line=0.0, signal_line=0.0, histogram=0.0))

        sync_fn = create_sync_macd_func(mock_calc)
        sync_fn("WETH", timeframe="1d")

        mock_calc.calculate_macd.assert_awaited_once_with("WETH", 12, 26, 9, timeframe="1d")


class TestCreateSyncBollingerFunc:
    def test_sync_bollinger_returns_bollinger_data(self):
        from almanak.framework.data.indicators.base import BollingerBandsResult

        mock_calc = MagicMock()
        mock_calc.calculate_bollinger_bands = AsyncMock(
            return_value=BollingerBandsResult(
                upper_band=3100.0,
                middle_band=3000.0,
                lower_band=2900.0,
                bandwidth=0.067,
                percent_b=0.5,
            )
        )

        sync_fn = create_sync_bollinger_func(mock_calc)
        result = sync_fn("WETH")

        assert isinstance(result, BollingerBandsData)
        assert result.upper_band == Decimal("3100.0")
        assert result.middle_band == Decimal("3000.0")
        assert result.lower_band == Decimal("2900.0")
        assert result.period == 20
        assert result.std_dev == 2.0


class TestCreateSyncStochasticFunc:
    def test_sync_stochastic_returns_stochastic_data(self):
        from almanak.framework.data.indicators.base import StochasticResult

        mock_calc = MagicMock()
        mock_calc.calculate_stochastic = AsyncMock(return_value=StochasticResult(k_value=25.0, d_value=30.0))

        sync_fn = create_sync_stochastic_func(mock_calc)
        result = sync_fn("WETH")

        assert isinstance(result, StochasticData)
        assert result.k_value == Decimal("25.0")
        assert result.d_value == Decimal("30.0")
        assert result.k_period == 14
        assert result.d_period == 3


class TestCreateSyncATRFunc:
    def test_sync_atr_returns_atr_data(self):
        mock_calc = MagicMock()
        mock_calc.calculate_atr = AsyncMock(return_value=50.0)
        mock_price_oracle = MagicMock(return_value=Decimal("2500"))

        sync_fn = create_sync_atr_func(mock_calc, mock_price_oracle)
        result = sync_fn("WETH")

        assert isinstance(result, ATRData)
        assert result.value == Decimal("50.0")
        assert result.period == 14

    def test_sync_atr_sets_value_percent_from_price_oracle(self):
        mock_calc = MagicMock()
        mock_calc.calculate_atr = AsyncMock(return_value=50.0)
        mock_price_oracle = MagicMock(return_value=Decimal("2500"))

        sync_fn = create_sync_atr_func(mock_calc, mock_price_oracle)
        result = sync_fn("WETH")

        # value_percent = (50 / 2500) * 100 = 2.0
        assert result.value_percent == Decimal("2.0")
        mock_price_oracle.assert_called_with("WETH", "USD")

    def test_sync_atr_raises_when_price_oracle_fails(self):
        mock_calc = MagicMock()
        mock_calc.calculate_atr = AsyncMock(return_value=50.0)
        mock_price_oracle = MagicMock(side_effect=RuntimeError("price oracle down"))

        sync_fn = create_sync_atr_func(mock_calc, mock_price_oracle)
        with pytest.raises(RuntimeError, match="price oracle down"):
            sync_fn("WETH")

    def test_sync_atr_passes_timeframe(self):
        mock_calc = MagicMock()
        mock_calc.calculate_atr = AsyncMock(return_value=50.0)
        mock_price_oracle = MagicMock(return_value=Decimal("2500"))

        sync_fn = create_sync_atr_func(mock_calc, mock_price_oracle)
        sync_fn("WETH", timeframe="1d")

        mock_calc.calculate_atr.assert_awaited_once_with("WETH", 14, timeframe="1d")


class TestCreateSyncSMAFunc:
    def test_sync_sma_returns_ma_data(self):
        mock_calc = MagicMock()
        mock_calc.sma = AsyncMock(return_value=3000.0)
        mock_price_oracle = MagicMock(return_value=Decimal("3100"))

        sync_fn = create_sync_sma_func(mock_calc, mock_price_oracle)
        result = sync_fn("WETH")

        assert isinstance(result, MAData)
        assert result.value == Decimal("3000.0")
        assert result.ma_type == "SMA"
        assert result.period == 20

    def test_sync_sma_sets_current_price_from_price_oracle(self):
        mock_calc = MagicMock()
        mock_calc.sma = AsyncMock(return_value=3000.0)
        mock_price_oracle = MagicMock(return_value=Decimal("3100"))

        sync_fn = create_sync_sma_func(mock_calc, mock_price_oracle)
        result = sync_fn("WETH")

        assert result.current_price == Decimal("3100")
        mock_price_oracle.assert_called_with("WETH", "USD")

    def test_sync_sma_raises_when_price_oracle_fails(self):
        mock_calc = MagicMock()
        mock_calc.sma = AsyncMock(return_value=3000.0)
        mock_price_oracle = MagicMock(side_effect=RuntimeError("price oracle down"))

        sync_fn = create_sync_sma_func(mock_calc, mock_price_oracle)
        with pytest.raises(RuntimeError, match="price oracle down"):
            sync_fn("WETH")


class TestCreateSyncEMAFunc:
    def test_sync_ema_returns_ma_data(self):
        mock_calc = MagicMock()
        mock_calc.ema = AsyncMock(return_value=3050.0)
        mock_price_oracle = MagicMock(return_value=Decimal("3100"))

        sync_fn = create_sync_ema_func(mock_calc, mock_price_oracle)
        result = sync_fn("WETH")

        assert isinstance(result, MAData)
        assert result.value == Decimal("3050.0")
        assert result.ma_type == "EMA"
        assert result.period == 12

    def test_sync_ema_sets_current_price_from_price_oracle(self):
        mock_calc = MagicMock()
        mock_calc.ema = AsyncMock(return_value=3050.0)
        mock_price_oracle = MagicMock(return_value=Decimal("3100"))

        sync_fn = create_sync_ema_func(mock_calc, mock_price_oracle)
        result = sync_fn("WETH")

        assert result.current_price == Decimal("3100")

    def test_sync_ema_raises_when_price_oracle_fails(self):
        mock_calc = MagicMock()
        mock_calc.ema = AsyncMock(return_value=3050.0)
        mock_price_oracle = MagicMock(side_effect=RuntimeError("price oracle down"))

        sync_fn = create_sync_ema_func(mock_calc, mock_price_oracle)
        with pytest.raises(RuntimeError, match="price oracle down"):
            sync_fn("WETH")

    def test_sync_ema_passes_timeframe(self):
        mock_calc = MagicMock()
        mock_calc.ema = AsyncMock(return_value=3050.0)
        mock_price_oracle = MagicMock(return_value=Decimal("3100"))

        sync_fn = create_sync_ema_func(mock_calc, mock_price_oracle)
        sync_fn("WETH", timeframe="1d")

        mock_calc.ema.assert_awaited_once_with("WETH", 12, timeframe="1d")


class TestCreateSyncRSIFunc:
    def test_sync_rsi_returns_rsi_data(self):
        mock_calc = MagicMock()
        mock_calc.calculate_rsi = AsyncMock(return_value=45.0)

        sync_fn = create_sync_rsi_func(mock_calc)
        result = sync_fn("WETH")

        assert isinstance(result, RSIData)
        assert result.value == Decimal("45.0")
        assert result.period == 14

    def test_sync_rsi_passes_timeframe(self):
        mock_calc = MagicMock()
        mock_calc.calculate_rsi = AsyncMock(return_value=45.0)

        sync_fn = create_sync_rsi_func(mock_calc)
        sync_fn("WETH", timeframe="1d")

        mock_calc.calculate_rsi.assert_awaited_once_with("WETH", 14, timeframe="1d")


class TestCreateSyncADXFunc:
    def test_sync_adx_returns_adx_data(self):
        from almanak.framework.data.indicators.base import ADXResult

        mock_calc = MagicMock()
        mock_calc.calculate_adx = AsyncMock(return_value=ADXResult(adx=30.0, plus_di=25.0, minus_di=15.0))

        sync_fn = create_sync_adx_func(mock_calc)
        result = sync_fn("WETH")

        assert isinstance(result, ADXData)
        assert result.adx == Decimal("30.0")
        assert result.plus_di == Decimal("25.0")
        assert result.minus_di == Decimal("15.0")
        assert result.period == 14

    def test_sync_adx_passes_timeframe(self):
        from almanak.framework.data.indicators.base import ADXResult

        mock_calc = MagicMock()
        mock_calc.calculate_adx = AsyncMock(return_value=ADXResult(adx=0.0, plus_di=0.0, minus_di=0.0))

        sync_fn = create_sync_adx_func(mock_calc)
        sync_fn("WETH", timeframe="1d")

        mock_calc.calculate_adx.assert_awaited_once_with("WETH", period=14, timeframe="1d")


class TestCreateSyncOBVFunc:
    def test_sync_obv_returns_obv_data(self):
        from almanak.framework.data.indicators.base import OBVResult

        mock_calc = MagicMock()
        mock_calc.calculate_obv = AsyncMock(return_value=OBVResult(obv=1000000.0, signal_line=950000.0))

        sync_fn = create_sync_obv_func(mock_calc)
        result = sync_fn("WETH")

        assert isinstance(result, OBVData)
        assert result.obv == Decimal("1000000.0")
        assert result.signal_line == Decimal("950000.0")
        assert result.signal_period == 21

    def test_sync_obv_passes_params(self):
        from almanak.framework.data.indicators.base import OBVResult

        mock_calc = MagicMock()
        mock_calc.calculate_obv = AsyncMock(return_value=OBVResult(obv=0.0, signal_line=0.0))

        sync_fn = create_sync_obv_func(mock_calc)
        sync_fn("WETH", signal_period=10, timeframe="1d")

        mock_calc.calculate_obv.assert_awaited_once_with("WETH", signal_period=10, timeframe="1d")


class TestCreateSyncCCIFunc:
    def test_sync_cci_returns_cci_data(self):
        mock_calc = MagicMock()
        mock_calc.calculate_cci = AsyncMock(return_value=-110.5)

        sync_fn = create_sync_cci_func(mock_calc)
        result = sync_fn("WETH")

        assert isinstance(result, CCIData)
        assert result.value == Decimal("-110.5")
        assert result.period == 20

    def test_sync_cci_passes_timeframe(self):
        mock_calc = MagicMock()
        mock_calc.calculate_cci = AsyncMock(return_value=0.0)

        sync_fn = create_sync_cci_func(mock_calc)
        sync_fn("WETH", timeframe="1d")

        mock_calc.calculate_cci.assert_awaited_once_with("WETH", period=20, timeframe="1d")


class TestCreateSyncIchimokuFunc:
    def test_sync_ichimoku_returns_data(self):
        from almanak.framework.data.indicators.base import IchimokuResult

        mock_calc = MagicMock()
        mock_calc.calculate_ichimoku = AsyncMock(
            return_value=IchimokuResult(
                tenkan_sen=3050.0,
                kijun_sen=3000.0,
                senkou_span_a=3025.0,
                senkou_span_b=2950.0,
                chikou_span=3100.0,
                current_price=3100.0,
            )
        )

        sync_fn = create_sync_ichimoku_func(mock_calc)
        result = sync_fn("WETH")

        assert isinstance(result, IchimokuData)
        assert result.tenkan_sen == Decimal("3050.0")
        assert result.kijun_sen == Decimal("3000.0")
        assert result.senkou_span_a == Decimal("3025.0")
        assert result.senkou_span_b == Decimal("2950.0")
        assert result.chikou_span == Decimal("3100.0")
        assert result.current_price == Decimal("3100.0")

    def test_sync_ichimoku_passes_params(self):
        from almanak.framework.data.indicators.base import IchimokuResult

        mock_calc = MagicMock()
        mock_calc.calculate_ichimoku = AsyncMock(
            return_value=IchimokuResult(
                tenkan_sen=0.0,
                kijun_sen=0.0,
                senkou_span_a=0.0,
                senkou_span_b=0.0,
                chikou_span=0.0,
                current_price=0.0,
            )
        )

        sync_fn = create_sync_ichimoku_func(mock_calc)
        sync_fn("WETH", tenkan_period=7, kijun_period=22, senkou_b_period=44, timeframe="1d")

        mock_calc.calculate_ichimoku.assert_awaited_once_with(
            "WETH",
            tenkan_period=7,
            kijun_period=22,
            senkou_b_period=44,
            timeframe="1d",
        )
