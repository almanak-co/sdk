"""Unit tests for the TA dashboard's OHLC-range indicator wiring (VIB-4884).

Before VIB-4884 only RSI and MACD rendered: the other five shipped factory
configs (Bollinger, CCI, Stochastic, ATR, ADX) produced a ``TADashboardConfig``
but no client-side computation, so the dashboard silently fell back to a
price-only chart. These tests pin three things:

1. Each rolling series helper agrees with the framework's canonical scalar
   calculator at the latest point (so the chart matches the strategy's signals).
2. ``prepare_ta_session_state`` populates the chart key each renderer reads.
3. A factory sweep asserts EVERY shipped ``get_*_config()`` actually computes
   indicator data — the regression guard against the silent price-only fallback.
"""

from __future__ import annotations

import math
from decimal import Decimal
from typing import Any

import pandas as pd
import pytest

from almanak.framework.dashboard.templates import (
    get_adx_config,
    get_atr_config,
    get_bollinger_config,
    get_cci_config,
    get_macd_config,
    get_rsi_config,
    get_stochastic_config,
    prepare_ta_session_state,
)
from almanak.framework.dashboard.templates.ta_dashboard import (
    _adx_series,
    _atr_series,
    _bollinger_bands_from_closes,
    _cci_series,
    _render_charts_section,
    _stochastic_series,
)
from almanak.framework.data.indicators.adx import ADXCalculator
from almanak.framework.data.indicators.atr import ATRCalculator
from almanak.framework.data.indicators.bollinger_bands import BollingerBandsCalculator
from almanak.framework.data.indicators.cci import CCICalculator
from almanak.framework.data.indicators.stochastic import StochasticCalculator
from almanak.framework.data.interfaces import OHLCVCandle

# ----------------------------------------------------------------------
# Synthetic OHLC data shared by the parity tests (deterministic, no RNG).
# ----------------------------------------------------------------------

_N = 80


def _series() -> tuple[list[float], list[float], list[float]]:
    """Return (high, low, close) lists with a real intrabar range and trend."""
    closes = [1000.0 + 50.0 * math.sin(i / 5.0) + i * 1.5 for i in range(_N)]
    highs = [c + 5.0 + 3.0 * abs(math.sin(i / 3.0)) for i, c in enumerate(closes)]
    lows = [c - 5.0 - 3.0 * abs(math.cos(i / 4.0)) for i, c in enumerate(closes)]
    return highs, lows, closes


def _price_df() -> pd.DataFrame:
    highs, lows, closes = _series()
    times = pd.date_range("2026-05-12", periods=_N, freq="1h", tz="UTC")
    return pd.DataFrame({"time": times, "price": closes, "high": highs, "low": lows})


def _candles() -> list[OHLCVCandle]:
    highs, lows, closes = _series()
    times = pd.date_range("2026-05-12", periods=_N, freq="1h", tz="UTC")
    return [
        OHLCVCandle(
            timestamp=t.to_pydatetime(),
            open=Decimal(str(c)),
            high=Decimal(str(h)),
            low=Decimal(str(low)),
            close=Decimal(str(c)),
            volume=Decimal("1"),
        )
        for t, h, low, c in zip(times, highs, lows, closes, strict=True)
    ]


def _ohlcv_payload() -> list[dict[str, Any]]:
    return [c.to_dict() for c in _candles()]


# ----------------------------------------------------------------------
# Rolling-series parity with the canonical scalar calculators.
# ----------------------------------------------------------------------


def test_bollinger_series_matches_canonical_calculator():
    _, _, closes = _series()
    expected = BollingerBandsCalculator.calculate_bollinger_from_prices(
        [Decimal(str(c)) for c in closes], period=20, std_dev_multiplier=2.0
    )
    bands = _bollinger_bands_from_closes(pd.Series(closes), period=20, std_dev=2.0).dropna()
    last = bands.iloc[-1]
    assert last["upper"] == pytest.approx(expected.upper_band)
    assert last["middle"] == pytest.approx(expected.middle_band)
    assert last["lower"] == pytest.approx(expected.lower_band)
    # The reported bug: bands must be three distinct ordered lines, not one.
    assert last["upper"] > last["middle"] > last["lower"]


def test_cci_series_matches_canonical_calculator():
    expected = CCICalculator.calculate_cci_from_candles(_candles(), period=20)
    cci = _cci_series(_price_df(), period=20).dropna()
    assert cci.iloc[-1] == pytest.approx(expected)


def test_stochastic_series_matches_canonical_calculator():
    expected = StochasticCalculator.calculate_stochastic_from_candles(_candles(), k_period=14, d_period=3)
    stoch = _stochastic_series(_price_df(), k_period=14, d_period=3).dropna()
    assert stoch["k"].iloc[-1] == pytest.approx(expected.k_value)
    assert stoch["d"].iloc[-1] == pytest.approx(expected.d_value)


def test_atr_series_matches_canonical_calculator():
    expected = ATRCalculator.calculate_atr_from_candles(_candles(), period=14)
    atr = _atr_series(_price_df(), period=14).dropna()
    assert atr.iloc[-1] == pytest.approx(expected)


def test_adx_series_matches_canonical_calculator():
    expected = ADXCalculator.calculate_adx_from_candles(_candles(), period=14)
    adx = _adx_series(_price_df(), period=14).dropna(subset=["adx"])
    assert adx["adx"].iloc[-1] == pytest.approx(expected.adx)
    assert adx["plus_di"].iloc[-1] == pytest.approx(expected.plus_di)
    assert adx["minus_di"].iloc[-1] == pytest.approx(expected.minus_di)


# ----------------------------------------------------------------------
# Degradation: OHLC-range indicators must not crash on a close-only frame.
# ----------------------------------------------------------------------


@pytest.mark.parametrize("series_fn", [_cci_series, _atr_series])
def test_range_series_degrade_on_close_only_frame(series_fn):
    _, _, closes = _series()
    close_only = pd.DataFrame({"time": pd.date_range("2026-05-12", periods=_N, freq="1h", tz="UTC"), "price": closes})
    out = series_fn(close_only, 14)  # no high/low columns → falls back to close
    # Fallback must produce a full-length, positionally-aligned, numeric series
    # with real values after warmup — not just "didn't crash".
    assert isinstance(out, pd.Series)
    assert len(out) == len(close_only)
    assert pd.api.types.is_numeric_dtype(out)
    assert out.notna().any()


# ----------------------------------------------------------------------
# Factory sweep: every shipped config must compute indicator data.
# ----------------------------------------------------------------------

_FACTORIES = {
    "RSI": get_rsi_config(),
    "MACD": get_macd_config(),
    "Bollinger": get_bollinger_config(),
    "CCI": get_cci_config(),
    "Stochastic": get_stochastic_config(),
    "ATR": get_atr_config(),
    "ADX": get_adx_config(),
}


class _Client:
    def __init__(self, ohlcv: list[dict[str, Any]]) -> None:
        self._ohlcv = ohlcv

    def get_ohlcv(self, **_: Any) -> list[dict[str, Any]]:
        return self._ohlcv

    def get_trade_tape(self) -> dict[str, Any]:
        return {"rows": [], "has_more": False}

    def get_timeline(self, **_: Any) -> list[dict[str, Any]]:
        return []


@pytest.mark.parametrize("name", list(_FACTORIES))
def test_every_factory_config_computes_indicator_data(name):
    """Regression guard for VIB-4884: no shipped config silently renders nothing."""
    config = _FACTORIES[name]
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)

    key = config.indicator_name.lower()
    history, data = out.get(f"{key}_history"), out.get(f"{key}_data")
    has_history = isinstance(history, pd.Series) and not history.empty
    has_data = isinstance(data, pd.DataFrame) and not data.empty
    assert has_history or has_data, f"{name}: no indicator series computed — dashboard would fall back to price-only"
    # Latest scalar feeds the signal-status / metric row.
    assert f"{key}_value" in out, f"{name}: missing {key}_value scalar"


@pytest.mark.parametrize("name", list(_FACTORIES))
def test_every_factory_config_renders_without_raising(name):
    """The dedicated render path must execute for every shipped config.

    Streamlit calls are no-ops without a script context, so this asserts the
    dispatch + plot-helper wiring doesn't raise (e.g. a 2-D frame hitting the
    generic single-line ``Series.values`` path, or a missing column).
    """
    config = _FACTORIES[name]
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    _render_charts_section(out, {}, config, period=config.indicator_period)
