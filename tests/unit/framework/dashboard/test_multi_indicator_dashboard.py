"""Unit tests for multi-indicator (multi-signal) TA dashboard support (VIB-4897).

Covers the additive multi-signal path on top of the single-indicator template:

1. ``multi_ta_config`` attaches extras without mutating the primary.
2. ``prepare_ta_session_state`` computes EVERY configured indicator's series
   (primary + extras) from one shared OHLCV pull — and does NOT compute extras
   when none are configured (single-indicator path unchanged).
3. The stacked render path composes ONE shared-axis ``make_subplots`` figure
   with ``1 + N`` rows (price + one row per indicator) for a 7-indicator config
   (VIB-4982 — NOT ``N + 1`` separate figures), and degrades (announces in-row,
   doesn't crash) when an indicator has no series.
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
    _render_multi_indicator_charts,
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


def _capture_streamlit(monkeypatch) -> dict[str, Any]:
    """Capture the chart / caption side effects the renderer emits (no Streamlit ctx).

    Records the *figures* passed to ``st.plotly_chart`` (not just a count) so the
    VIB-4982 contract — ONE shared-axis ``make_subplots`` figure with ``1 + N``
    rows, NOT ``N + 1`` separate figures — can be asserted on the figure shape.
    """
    import almanak.framework.dashboard.templates.ta_dashboard as tad

    calls: dict[str, Any] = {"plotly_chart": 0, "caption": 0, "figs": []}
    monkeypatch.setattr(
        tad.st,
        "plotly_chart",
        lambda fig=None, *a, **k: (
            calls.__setitem__("plotly_chart", calls["plotly_chart"] + 1),
            calls["figs"].append(fig),
        ),
    )
    monkeypatch.setattr(tad.st, "caption", lambda *a, **k: calls.__setitem__("caption", calls["caption"] + 1))
    return calls


def _subplot_row_count(fig: Any) -> int:
    """Number of stacked subplot rows in a Plotly figure (one y-axis per row)."""
    import re

    return sum(1 for key in fig.layout if re.fullmatch(r"yaxis\d*", key))


def test_render_multi_indicator_is_one_figure_with_price_plus_one_row_per_indicator(monkeypatch):
    """VIB-4982: the multi-indicator render is ONE figure with ``1 + N`` rows.

    Previously this path drew ``N + 1`` SEPARATE ``st.plotly_chart`` figures
    (price + one per indicator). The fix composes a single shared-axis
    ``make_subplots`` figure: price + signals on row 1, then one indicator per
    row. So we assert exactly one ``plotly_chart`` call whose figure has ``1 + N``
    subplot rows.
    """
    calls = _capture_streamlit(monkeypatch)
    config = _all_indicators_config()  # primary + 6 extras = 7 indicators
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    _render_charts_section(out, {}, config, period=14)

    assert calls["plotly_chart"] == 1, "multi-indicator must render exactly ONE figure (not N+1)"
    assert calls["caption"] == 0
    fig = calls["figs"][0]
    assert _subplot_row_count(fig) == 8  # 1 price row + 7 indicator rows


def test_render_multi_indicator_tolerates_missing_series(monkeypatch):
    """A missing series keeps its row (announced in-place), not silently dropped.

    The single composite figure still has the full ``1 + N`` rows even when one
    indicator's series failed to compute; the empty row carries an in-row
    "no indicator data available" annotation instead of being removed (which would
    shift every later indicator's row and break the shared-axis layout).
    """
    calls = _capture_streamlit(monkeypatch)
    config = _all_indicators_config()
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    out.pop("macd_data", None)  # simulate MACD's series failing to compute
    _render_charts_section(out, {}, config, period=14)

    assert calls["plotly_chart"] == 1  # still ONE figure
    fig = calls["figs"][0]
    assert _subplot_row_count(fig) == 8  # 1 price + 7 indicator rows (MACD row kept)
    # The missing MACD is announced (not silently dropped) via an in-row annotation.
    announcements = [a.text for a in fig.layout.annotations if a.text and "no indicator data available" in a.text]
    assert announcements == ["MACD: no indicator data available"]
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
    assert calls["plotly_chart"] == 1  # ONE composite figure
    assert calls["caption"] == 0
    fig = calls["figs"][0]
    assert _subplot_row_count(fig) == 3  # 1 price + 2 distinct RSI rows


# ----------------------------------------------------------------------
# VIB-4982 regression: one shared/linked-time-axis figure, not N+1 figures
# ----------------------------------------------------------------------


# NOTE: every case here passes at least one extra so ``config.extra_indicators``
# is non-empty and ``_render_charts_section`` actually dispatches to
# ``_render_multi_indicator_charts`` (the composite path under test). With zero
# extras the section falls through to the legacy single-indicator RSI branch,
# which independently builds its own 2-row subplot — that path is covered by the
# single-indicator tests, not this VIB-4982 regression.
@pytest.mark.parametrize(
    ("extras", "expected_rows"),
    [
        ([get_macd_config], 3),  # RSI + MACD                        → price + 2
        ([get_macd_config, get_bollinger_config], 4),  # + Bollinger → price + 3
        ([get_macd_config, get_bollinger_config, get_cci_config], 5),  # → price + 4
    ],
)
def test_multi_indicator_renders_single_figure_with_one_plus_n_rows(monkeypatch, extras, expected_rows):
    """The multi-indicator dashboard is ONE figure with ``1 + N`` rows (VIB-4982).

    Root bug: N indicators were drawn as ``N + 1`` independent Plotly figures
    (a price chart + one figure per indicator), so users saw "N graphs with 1
    indicator each" with unlinked time axes. The fix composes a single
    ``make_subplots`` figure — price on row 1, each indicator on its own row —
    so a single ``st.plotly_chart`` call yields ``1 + N`` rows whose x axes are
    shared (zoom/pan/hover move together).
    """
    calls = _capture_streamlit(monkeypatch)
    config = multi_ta_config(get_rsi_config(), *[factory() for factory in extras])
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    _render_charts_section(out, {}, config, period=14)

    # Exactly ONE figure (never N+1 separate plotly_chart calls).
    assert calls["plotly_chart"] == 1
    fig = calls["figs"][0]

    # ``1 + N`` stacked rows: price row + one per configured indicator.
    assert _subplot_row_count(fig) == expected_rows

    # The time axis is shared/linked across every row. With Plotly
    # ``shared_xaxes=True`` every non-anchor x axis is wired to a single common
    # anchor axis (and the anchor itself has ``matches is None``). For >1 row
    # exactly one axis is the anchor and all the rest point at the same target —
    # this linkage is the property that was missing when every indicator owned its
    # own standalone figure.
    x_axes = [key for key in fig.layout if key.startswith("xaxis")]
    assert len(x_axes) == expected_rows
    if expected_rows > 1:
        match_targets = {fig.layout[ax].matches for ax in x_axes}
        anchors = {ax for ax in x_axes if fig.layout[ax].matches is None}
        assert len(anchors) == 1, "expected exactly one shared-x-axis anchor row"
        # Every non-anchor axis links to the same single target.
        assert match_targets - {None} and len({t for t in match_targets if t is not None}) == 1


def test_multi_indicator_composite_is_themed(monkeypatch):
    """The composite figure carries the dashboard theme (not Plotly's default).

    Gemini review: without ``apply_theme`` the single composite figure renders
    with Plotly's default light theme instead of the dashboard theme used by every
    standalone plot. ``apply_theme`` stamps the dark template AND the config
    background onto the layout (a raw ``make_subplots`` figure leaves
    ``paper_bgcolor``/``plot_bgcolor`` as ``None``), so assert those are set to
    the theme background.
    """
    from almanak.framework.dashboard.plots.base import get_default_config

    calls = _capture_streamlit(monkeypatch)
    config = multi_ta_config(get_rsi_config(), get_macd_config())
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    _render_charts_section(out, {}, config, period=14)

    fig = calls["figs"][0]
    theme_bg = get_default_config().colors.background
    assert fig.layout.paper_bgcolor == theme_bg  # None on an unthemed figure
    assert fig.layout.plot_bgcolor == theme_bg
    assert fig.layout.template is not None and fig.layout.template.layout is not None
    # Dynamic per-row height survives apply_theme. Two indicators (RSI primary +
    # MACD extra) → 300 + 250 * 2 (apply_theme must NOT clobber this with its
    # single-figure default height).
    assert fig.layout.height == 300 + 250 * 2


def test_multi_indicator_empty_price_keeps_row_and_announces(monkeypatch):
    """Empty price data keeps row 1 and announces it, rather than rendering blank.

    Gemini review: in composite mode ``plot_price_with_signals`` adds no traces
    when the price frame is empty, which would leave the price subplot blank with
    no user-facing message. The composite renderer now annotates that row instead.

    ``_render_charts_section`` itself short-circuits with an ``st.info`` banner
    before reaching the composite path when price history is empty, so this
    exercises the composite renderer (``_render_multi_indicator_charts``) directly
    to cover its own defensive empty-price branch.
    """
    calls = _capture_streamlit(monkeypatch)
    config = multi_ta_config(get_rsi_config(), get_macd_config())
    out = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    empty_price = pd.DataFrame(columns=["time", "price"])
    _render_multi_indicator_charts(out, config, empty_price, None, None)

    assert calls["plotly_chart"] == 1
    fig = calls["figs"][0]
    assert _subplot_row_count(fig) == 3  # price + 2 indicators (row kept)
    announcements = [a.text for a in fig.layout.annotations if a.text and "no price data available" in a.text]
    assert announcements == ["Price: no price data available"]


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
