"""Unit tests for multi-indicator (multi-signal) TA dashboard support (VIB-4897).

Covers the additive multi-signal path on top of the single-indicator template:

1. ``multi_ta_config`` attaches extras without mutating the primary.
2. ``prepare_ta_session_state`` computes EVERY configured indicator's series
   (primary + extras) from one shared OHLCV pull — and does NOT compute extras
   when none are configured (single-indicator path unchanged).
3. The stacked render path executes for a 7-indicator config without raising,
   and degrades (announces, doesn't crash) when an indicator has no series.
4. Every shipped ``get_*_config`` indicator has a dedicated panel.
"""

from __future__ import annotations

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
    multi_ta_config,
    prepare_ta_session_state,
)
from almanak.framework.dashboard.templates.ta_dashboard import (
    _INDICATOR_PANELS,
    TADashboardConfig,
    _render_charts_section,
)

_N = 80


def _ohlcv_payload() -> list[dict[str, Any]]:
    """80 hourly OHLCV candles with a real intrabar range (enough for ADX's 2*period)."""
    import math

    times = pd.date_range("2026-05-12", periods=_N, freq="1h", tz="UTC")
    rows = []
    for i, t in enumerate(times):
        close = 2000.0 + 50.0 * math.sin(i / 5.0) + i * 1.5
        rows.append(
            {
                "timestamp": t.isoformat(),
                "open": str(close - 1),
                "high": str(close + 5.0 + 3.0 * abs(math.sin(i / 3.0))),
                "low": str(close - 5.0 - 3.0 * abs(math.cos(i / 4.0))),
                "close": str(close),
                "volume": "1",
            }
        )
    return rows


class _Client:
    def __init__(self, ohlcv: list[dict[str, Any]]) -> None:
        self._ohlcv = ohlcv

    def get_ohlcv(self, **_: Any) -> list[dict[str, Any]]:
        return self._ohlcv

    def get_trade_tape(self) -> dict[str, Any]:
        return {"rows": [], "has_more": False}

    def get_timeline(self, **_: Any) -> list[dict[str, Any]]:
        return []


def _all_indicators_config() -> TADashboardConfig:
    """RSI primary + the other six as extras."""
    return multi_ta_config(
        get_rsi_config(),
        get_macd_config(),
        get_bollinger_config(),
        get_cci_config(),
        get_stochastic_config(),
        get_atr_config(),
        get_adx_config(),
    )


# Indicator name (lowercase) -> the session_state key its series lands under.
_EXPECTED_KEYS = {
    "rsi": "rsi_history",
    "macd": "macd_data",
    "bollinger": "bollinger_data",
    "cci": "cci_history",
    "stochastic": "stochastic_data",
    "atr": "atr_history",
    "adx": "adx_data",
}


# ----------------------------------------------------------------------
# multi_ta_config
# ----------------------------------------------------------------------


def test_multi_ta_config_attaches_extras_without_mutating_primary():
    primary = get_rsi_config()
    extras = (get_macd_config(), get_bollinger_config())
    config = multi_ta_config(primary, *extras)

    assert config.indicator_name == "RSI"
    assert [c.indicator_name for c in config.extra_indicators] == ["MACD", "Bollinger"]
    # Primary must not be mutated (replace returns a new config).
    assert primary.extra_indicators == []


def test_single_config_has_no_extras_by_default():
    assert get_rsi_config().extra_indicators == []


# ----------------------------------------------------------------------
# prepare_ta_session_state — multi vs single
# ----------------------------------------------------------------------


def test_prepare_populates_every_configured_indicator_series():
    config = _all_indicators_config()
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)

    for key in _EXPECTED_KEYS.values():
        series = out.get(key)
        present = isinstance(series, pd.Series | pd.DataFrame) and not series.empty
        assert present, f"{key} not populated for multi-indicator config"


def test_prepare_single_config_does_not_compute_extras():
    # The single-indicator path must be unchanged: only RSI's series appears.
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=get_rsi_config())
    assert isinstance(out.get("rsi_history"), pd.Series) and not out["rsi_history"].empty
    assert "macd_data" not in out
    assert "bollinger_data" not in out


def test_prepare_preserves_caller_supplied_extra_series():
    config = multi_ta_config(get_rsi_config(), get_macd_config())
    caller_macd = pd.DataFrame({"macd": [1.0], "signal": [0.5], "histogram": [0.5]})
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={"macd_data": caller_macd}, config=config)
    assert out["macd_data"].equals(caller_macd)  # not recomputed


# ----------------------------------------------------------------------
# Rendering
# ----------------------------------------------------------------------


def _capture_streamlit(monkeypatch) -> dict[str, int]:
    """Count the chart / caption side effects the renderer emits (no Streamlit ctx)."""
    import almanak.framework.dashboard.templates.ta_dashboard as tad

    calls = {"plotly_chart": 0, "caption": 0}
    monkeypatch.setattr(
        tad.st, "plotly_chart", lambda *a, **k: calls.__setitem__("plotly_chart", calls["plotly_chart"] + 1)
    )
    monkeypatch.setattr(tad.st, "caption", lambda *a, **k: calls.__setitem__("caption", calls["caption"] + 1))
    return calls


def test_render_multi_indicator_draws_one_panel_per_indicator(monkeypatch):
    """Price chart once + one panel per indicator; nothing announced as missing."""
    calls = _capture_streamlit(monkeypatch)
    config = _all_indicators_config()  # primary + 6 extras = 7 indicators
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    _render_charts_section(out, {}, config, period=14)
    assert calls["plotly_chart"] == 8  # 1 price + 7 indicator panels
    assert calls["caption"] == 0


def test_render_multi_indicator_tolerates_missing_series(monkeypatch):
    """A missing series is announced once (st.caption); the other panels still draw."""
    calls = _capture_streamlit(monkeypatch)
    config = _all_indicators_config()
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    out.pop("macd_data", None)  # simulate MACD's series failing to compute
    _render_charts_section(out, {}, config, period=14)
    assert calls["plotly_chart"] == 7  # 1 price + 6 remaining panels (MACD skipped)
    assert calls["caption"] == 1  # the missing MACD is announced, not silently dropped
    # Other indicators are unaffected.
    assert isinstance(out.get("rsi_history"), pd.Series) and not out["rsi_history"].empty


# ----------------------------------------------------------------------
# Same-type indicators (e.g. dual RSI) must not collide (VIB-4897 review)
# ----------------------------------------------------------------------


def test_prepare_supports_dual_same_type_indicators():
    config = multi_ta_config(get_rsi_config(period=14), get_rsi_config(period=21))
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    # First RSI keeps the bare key; the second is disambiguated to rsi_2.
    assert isinstance(out.get("rsi_history"), pd.Series) and not out["rsi_history"].empty
    assert isinstance(out.get("rsi_2_history"), pd.Series) and not out["rsi_2_history"].empty
    # Different periods → genuinely different series (no collision / duplicate panel).
    assert not out["rsi_history"].equals(out["rsi_2_history"])


def test_render_dual_same_type_indicators_draws_two_panels(monkeypatch):
    calls = _capture_streamlit(monkeypatch)
    config = multi_ta_config(get_rsi_config(period=14), get_rsi_config(period=21))
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    _render_charts_section(out, {}, config, period=14)
    assert calls["plotly_chart"] == 3  # 1 price + 2 distinct RSI panels
    assert calls["caption"] == 0


# ----------------------------------------------------------------------
# Panel coverage
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "factory",
    [
        get_rsi_config,
        get_macd_config,
        get_bollinger_config,
        get_cci_config,
        get_stochastic_config,
        get_atr_config,
        get_adx_config,
    ],
)
def test_every_shipped_indicator_has_a_panel(factory):
    name = factory().indicator_name.upper()
    assert name in _INDICATOR_PANELS, f"{name} has no dedicated multi-signal panel"
