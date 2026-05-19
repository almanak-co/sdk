"""
Technical Analysis (TA) Dashboard Template.

Reusable template for creating dashboards for indicator-based strategies.
Supports any TA indicator with configurable signal logic and visualization.

Scope (single-signal / single-position): the template is designed for one
signal driving one position on one ``base_token``/``quote_token`` pair.
The 3 accounting sections (PnL, Cost Stack, Trade Tape) are baked in so
every TA dashboard ships with full accounting by default. For multi-
position or multi-signal layouts, do **not** stretch this template — write
a custom dashboard composed from the section helpers
(``render_pnl_section``, ``render_cost_stack_section``,
``render_trade_tape_section``) plus primitive plot helpers from
``almanak.framework.dashboard.plots``. See the dashboard blueprints for
the recommended composition.

Usage:
    from almanak.framework.dashboard.templates import TADashboardConfig, render_ta_dashboard

    config = TADashboardConfig(
        indicator_name="RSI",
        indicator_period=14,
        upper_threshold=70,
        lower_threshold=30,
        signal_type="reversion",  # or "momentum"
    )

    def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
        render_ta_dashboard(strategy_id, strategy_config, session_state, config)
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from almanak.framework.dashboard.plots import plot_price_with_signals
from almanak.framework.dashboard.plots.base import get_default_config
from almanak.framework.dashboard.sections import (
    render_cost_stack_section,
    render_pnl_section,
    render_trade_tape_section,
)

logger = logging.getLogger(__name__)


@dataclass
class TADashboardConfig:
    """Configuration for a TA dashboard.

    Attributes:
        indicator_name: Name of the indicator (e.g., "RSI", "MACD", "CCI")
        indicator_period: Primary period for the indicator
        secondary_periods: Additional periods (e.g., signal line for MACD)
        upper_threshold: Upper threshold for signals (overbought/bullish)
        lower_threshold: Lower threshold for signals (oversold/bearish)
        signal_type: Type of signal logic - "reversion" or "momentum"
        value_format: Format string for displaying indicator value (e.g., "{:.1f}", "{:+.2f}")
        value_suffix: Suffix for indicator value (e.g., "%", " bps")
        show_progress_bar: Whether to show a progress bar for the indicator
        progress_range: (min, max) range for progress bar normalization
        custom_signal_fn: Optional custom function for signal determination
        chain: Default chain name
        protocol: Default protocol name
        base_token: Default base token
        quote_token: Default quote token
    """

    indicator_name: str
    indicator_period: int = 14
    secondary_periods: list[int] = field(default_factory=list)
    upper_threshold: float | None = None
    lower_threshold: float | None = None
    signal_type: Literal["reversion", "momentum"] = "reversion"
    value_format: str = "{:.1f}"
    value_suffix: str = ""
    show_progress_bar: bool = False
    progress_range: tuple[float, float] = (0, 100)
    custom_signal_fn: Callable[[dict[str, Any]], str] | None = None
    chain: str = "Arbitrum"
    protocol: str = "Uniswap V3"
    base_token: str = "WETH"
    quote_token: str = "USDC"


def _rsi_series_from_closes(closes: pd.Series, period: int) -> pd.Series:
    """Compute the rolling RSI series using Wilder's smoothing.

    Matches the scalar implementation in
    ``almanak.framework.data.indicators.rsi.RSIIndicatorService.calculate_rsi_from_prices``
    but returns the rolling series (NaN for the first ``period`` rows).
    """
    if len(closes) < period + 1:
        return pd.Series([float("nan")] * len(closes), index=closes.index, name="rsi")
    delta = closes.astype(float).diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    # Wilder's smoothing == EMA with alpha=1/period, adjust=False, min_periods=period
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    # avg_loss == 0 (no losses in window) collapses to RSI=100
    rsi = rsi.where(avg_loss != 0, other=100.0)
    return rsi.rename("rsi")


def _ohlcv_to_price_history(ohlcv: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalise an api_client.get_ohlcv() payload into ``{time, price}`` rows."""
    if not ohlcv:
        return pd.DataFrame(columns=["time", "price"])
    df = pd.DataFrame(ohlcv)
    if "timestamp" not in df.columns or "close" not in df.columns:
        return pd.DataFrame(columns=["time", "price"])
    df = df.rename(columns={"timestamp": "time", "close": "price"})
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["time", "price"]).sort_values("time").reset_index(drop=True)
    return df[["time", "price"]]


def _trade_rows_to_signals(
    rows: list[dict[str, Any]],
    base_token: str,
    quote_token: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split SWAP rows into buy/sell DataFrames.

    A row counts as a BUY of ``base_token`` when ``token_in == quote_token``
    AND ``token_out == base_token`` (we paid quote, received base). A SELL
    is the reverse. Other rows (non-SWAP, or for a different pair) are
    ignored so dashboards in mixed-strategy folders don't pick up unrelated
    markers.
    """
    empty = pd.DataFrame(columns=["time", "price"])
    if not rows:
        return empty, empty
    buys: list[dict[str, Any]] = []
    sells: list[dict[str, Any]] = []
    bu = base_token.upper()
    qu = quote_token.upper()
    for r in rows:
        if str(r.get("intent_type", "")).upper() != "SWAP":
            continue
        ti = str(r.get("token_in", "")).upper()
        to = str(r.get("token_out", "")).upper()
        ts = r.get("timestamp")
        price = r.get("effective_price")
        if ts is None or price in (None, ""):
            continue
        marker = {"time": ts, "price": price}
        if ti == qu and to == bu:
            buys.append(marker)
        elif ti == bu and to == qu:
            sells.append(marker)

    def _frame(items: list[dict[str, Any]]) -> pd.DataFrame:
        if not items:
            return empty.copy()
        df = pd.DataFrame(items)
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        return df.dropna(subset=["time", "price"]).reset_index(drop=True)

    return _frame(buys), _frame(sells)


def _trade_tape_rows(api_client: Any) -> list[dict[str, Any]]:
    """Extract trade-tape rows from whatever shape the api_client exposes."""
    if api_client is None or not hasattr(api_client, "get_trade_tape"):
        return []
    try:
        tape = api_client.get_trade_tape()
    except Exception:  # noqa: BLE001
        logger.debug("api_client.get_trade_tape() failed", exc_info=True)
        return []
    if tape is None:
        return []
    # Typed dataclass with .rows attribute, OR a dict with "rows" key.
    rows = getattr(tape, "rows", None)
    if rows is None and isinstance(tape, dict):
        rows = tape.get("rows", [])
    if rows is None:
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        if isinstance(r, dict):
            out.append(r)
            continue
        # Dataclass-like: harvest the few fields we need by attribute.
        try:
            ts: Any = getattr(r, "timestamp", None)
            if ts is not None and hasattr(ts, "isoformat"):
                ts = ts.isoformat()
            out.append(
                {
                    "timestamp": ts,
                    "intent_type": getattr(r, "intent_type", ""),
                    "token_in": getattr(r, "token_in", ""),
                    "token_out": getattr(r, "token_out", ""),
                    "effective_price": getattr(r, "effective_price", ""),
                }
            )
        except Exception:  # noqa: BLE001
            continue
    return out


def prepare_ta_session_state(
    api_client: Any,
    session_state: dict[str, Any] | None = None,
    config: TADashboardConfig | None = None,
) -> dict[str, Any]:
    """Enrich session state for ``render_ta_dashboard`` (chart subplot).

    Mirrors :func:`prepare_lp_session_state`: fetches OHLCV via the
    api_client, computes the indicator series client-side, and reads the
    trade tape for buy/sell markers — strategy authors don't write any of
    that plumbing. Without this helper the chart section silently degrades
    to ``Price history data not available`` because nothing populates
    ``price_history`` / ``rsi_history`` / ``buy_signals`` / ``sell_signals``.

    Args:
        api_client: ``DashboardAPIClient`` (or a duck-typed mock).
        session_state: Optional pre-existing state. Caller-supplied keys
            are preserved — never overwritten — so custom dashboards that
            already populate ``price_history`` keep working.
        config: ``TADashboardConfig`` describing the indicator, pair, and
            chain. Required to know which token to fetch OHLCV for.

    Returns:
        The enriched session_state dict. Always returns; degrades to the
        unenriched state on any API failure rather than raising.
    """
    result: dict[str, Any] = dict(session_state) if session_state else {}
    if config is None:
        return result

    base_token = result.get("base_token") or config.base_token
    quote_token = result.get("quote_token") or config.quote_token
    chain = result.get("chain") or config.chain

    # OHLCV + price history (skip if caller already supplied).
    price_df: pd.DataFrame | None = None
    if "price_history" not in result and api_client is not None:
        try:
            ohlcv = api_client.get_ohlcv(
                token=base_token,
                quote=quote_token,
                timeframe="1h",
                limit=168,
                chain=chain,
            )
            price_df = _ohlcv_to_price_history(ohlcv or [])
            if not price_df.empty:
                result["price_history"] = price_df
        except Exception:  # noqa: BLE001
            logger.debug("api_client.get_ohlcv() failed", exc_info=True)
    elif "price_history" in result:
        existing = result["price_history"]
        if isinstance(existing, pd.DataFrame):
            price_df = existing

    # Indicator series — only RSI today; other indicators fall through.
    indicator_key = config.indicator_name.lower()
    history_key = f"{indicator_key}_history"
    data_key = f"{indicator_key}_data"
    if (
        history_key not in result
        and data_key not in result
        and price_df is not None
        and not price_df.empty
        and indicator_key == "rsi"
    ):
        try:
            rsi = _rsi_series_from_closes(price_df["price"], config.indicator_period)
            # The renderer's RSI subplot consumes `rsi_history` as a
            # pandas Series indexed by time (or a list of (time, value)
            # tuples). Emit a Series so renderer's `rsi_series.values`
            # access works without further coercion.
            history_series = pd.Series(
                rsi.values,
                index=pd.DatetimeIndex(price_df["time"]),
                name="rsi",
            ).dropna()
            if not history_series.empty:
                result[history_key] = history_series
                # Surface the latest value to the metric row too.
                result.setdefault(f"{indicator_key}_value", float(history_series.iloc[-1]))
                result.setdefault(indicator_key, result[f"{indicator_key}_value"])
        except Exception:  # noqa: BLE001
            logger.debug("RSI series computation failed", exc_info=True)

    # Buy / sell signals from the trade tape (SWAP rows for this pair).
    if "buy_signals" not in result or "sell_signals" not in result:
        rows = _trade_tape_rows(api_client)
        buys, sells = _trade_rows_to_signals(rows, base_token, quote_token)
        result.setdefault("buy_signals", buys)
        result.setdefault("sell_signals", sells)

    return result


def render_ta_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    session_state: dict[str, Any],
    config: TADashboardConfig,
) -> None:
    """Render a technical analysis dashboard using the provided configuration.

    Single-signal / single-position template — one indicator driving one
    position on one configured pair. Bakes in the 3 accounting sections
    (PnL → primitive content → Cost Stack → Trade Tape). For multi-
    position or multi-signal layouts, compose a custom dashboard from
    the section helpers directly rather than parameterizing this template.

    Args:
        strategy_id: The strategy identifier
        strategy_config: Strategy configuration dictionary
        session_state: Current session state with indicator values
        config: TADashboardConfig for this dashboard
    """
    st.title(f"{config.indicator_name} Strategy Dashboard")

    # Extract config overrides
    base_token = strategy_config.get("base_token", config.base_token)
    quote_token = strategy_config.get("quote_token", config.quote_token)
    chain = strategy_config.get("chain", config.chain)
    protocol = strategy_config.get("protocol", config.protocol)
    period = strategy_config.get(f"{config.indicator_name.lower()}_period", config.indicator_period)

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown(f"**Chain:** {chain} | **Protocol:** {protocol}")

    # Eyeball — am I making or losing money?
    render_pnl_section(strategy_id)

    # Indicator section
    _render_indicator_section(session_state, strategy_config, config, period)

    st.divider()

    # Charts section - Price with signals and indicator
    _render_charts_section(session_state, strategy_config, config, period)

    st.divider()

    # Signal status
    _render_signal_status(session_state, strategy_config, config)

    st.divider()

    # Position section
    st.subheader("Current Position")
    _render_position(session_state, base_token, quote_token)

    st.divider()

    # Performance section
    st.subheader("Performance")
    _render_performance(session_state)

    # Audit — life-to-date costs + per-intent trade tape
    render_cost_stack_section(strategy_id)
    render_trade_tape_section(strategy_id)


def _render_indicator_section(
    session_state: dict[str, Any],
    strategy_config: dict[str, Any],
    config: TADashboardConfig,
    period: int,
) -> None:
    """Render the indicator display section."""
    st.subheader(f"{config.indicator_name}({period})")

    # Get primary indicator value
    indicator_key = config.indicator_name.lower()
    indicator_value = float(session_state.get(f"{indicator_key}_value", session_state.get(indicator_key, 50)))

    # Create columns based on whether we have thresholds
    if config.upper_threshold is not None and config.lower_threshold is not None:
        col1, col2, col3 = st.columns(3)
        with col1:
            formatted_value = config.value_format.format(indicator_value) + config.value_suffix
            st.metric(config.indicator_name, formatted_value)
        with col2:
            st.metric("Upper", f"{config.upper_threshold}{config.value_suffix}")
        with col3:
            st.metric("Lower", f"{config.lower_threshold}{config.value_suffix}")
    else:
        col1, col2 = st.columns(2)
        with col1:
            formatted_value = config.value_format.format(indicator_value) + config.value_suffix
            st.metric(config.indicator_name, formatted_value)
        with col2:
            st.metric("Period", str(period))

    # Secondary indicator values (e.g., signal line for MACD)
    if config.secondary_periods:
        secondary_cols = st.columns(len(config.secondary_periods) + 1)
        for i, sec_period in enumerate(config.secondary_periods):
            with secondary_cols[i]:
                key = f"{indicator_key}_signal_{sec_period}"
                alt_key = f"{indicator_key}_{sec_period}"
                value = float(session_state.get(key, session_state.get(alt_key, 0)))
                formatted = config.value_format.format(value) + config.value_suffix
                st.metric(f"Signal({sec_period})", formatted)

    # Progress bar visualization
    if config.show_progress_bar:
        min_val, max_val = config.progress_range
        normalized = (indicator_value - min_val) / (max_val - min_val)
        normalized = max(0, min(1, normalized))
        st.progress(normalized, text=f"{config.indicator_name}: {config.value_format.format(indicator_value)}")


def _render_charts_section(  # noqa: C901
    session_state: dict[str, Any],
    strategy_config: dict[str, Any],
    config: TADashboardConfig,
    period: int,
) -> None:
    """Render price and indicator charts with buy/sell signals."""
    st.subheader("Price & Indicator Charts")

    # Get price history
    price_history = session_state.get("price_history")
    if price_history is None or (isinstance(price_history, pd.DataFrame) and price_history.empty):
        st.info("Price history data not available")
        return

    # Convert to DataFrame if it's a list
    if isinstance(price_history, list):
        price_df = pd.DataFrame(price_history, columns=["time", "price"])
    else:
        price_df = price_history.copy()

    # Ensure time column is datetime
    if "time" in price_df.columns:
        if not pd.api.types.is_datetime64_any_dtype(price_df["time"]):
            price_df["time"] = pd.to_datetime(price_df["time"])
    elif "timestamp" in price_df.columns:
        price_df = price_df.rename(columns={"timestamp": "time"})
        if not pd.api.types.is_datetime64_any_dtype(price_df["time"]):
            price_df["time"] = pd.to_datetime(price_df["time"])
    else:
        st.warning("Price data missing time column")
        return

    # Get buy/sell signals
    buy_signals = session_state.get("buy_signals")
    sell_signals = session_state.get("sell_signals")

    # Convert signals to DataFrame if they're lists. Use ``is not None`` +
    # explicit emptiness checks rather than ``if signals:`` — pandas raises
    # ``ValueError: The truth value of a DataFrame is ambiguous`` on the
    # truthiness gate.
    def _coerce_signals(signals: Any) -> pd.DataFrame | None:
        if signals is None:
            return None
        if isinstance(signals, list):
            if not signals:
                return None
            df = pd.DataFrame(signals, columns=["time", "price"])
        elif isinstance(signals, pd.DataFrame):
            if signals.empty:
                return None
            df = signals.copy()
        else:
            return None
        if "time" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        return df

    buy_df = _coerce_signals(buy_signals)
    sell_df = _coerce_signals(sell_signals)

    # Get indicator data
    indicator_key = config.indicator_name.lower()
    # Avoid ``a or b`` and ``and data`` truthiness on pandas objects — both
    # raise ``ValueError: The truth value of a Series/DataFrame is ambiguous``.
    indicator_data = session_state.get(f"{indicator_key}_data")
    if indicator_data is None:
        indicator_data = session_state.get(f"{indicator_key}_history")

    def _has_indicator_data(data: Any) -> bool:
        if data is None:
            return False
        if isinstance(data, pd.Series | pd.DataFrame):
            return not data.empty
        if isinstance(data, list):
            return len(data) > 0
        return bool(data)

    # For RSI specifically, create combined subplot
    if config.indicator_name.upper() == "RSI" and _has_indicator_data(indicator_data):
        # Create subplot: price on top, RSI on bottom
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.7, 0.3],
            subplot_titles=("Price with Buy/Sell Signals", f"{config.indicator_name} Indicator"),
        )

        config_plot = get_default_config()
        colors = config_plot.colors

        # Add price line
        fig.add_trace(
            go.Scatter(
                x=price_df["time"],
                y=price_df["price"],
                mode="lines",
                name="Price",
                line={"color": colors.primary, "width": 2},
            ),
            row=1,
            col=1,
        )

        # Add buy signals (green triangles up)
        if buy_df is not None and not buy_df.empty:
            for _, signal in buy_df.iterrows():
                signal_time = signal["time"]
                signal_price = signal.get(
                    "price",
                    price_df.loc[price_df["time"] == signal_time, "price"].values[0]
                    if len(price_df.loc[price_df["time"] == signal_time]) > 0
                    else price_df["price"].iloc[-1],
                )
                fig.add_trace(
                    go.Scatter(
                        x=[signal_time],
                        y=[signal_price],
                        mode="markers",
                        name="Buy",
                        marker={
                            "symbol": "triangle-up",
                            "size": 15,
                            "color": colors.success,
                            "line": {"color": "white", "width": 1},
                        },
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

        # Add sell signals (red triangles down)
        if sell_df is not None and not sell_df.empty:
            for _, signal in sell_df.iterrows():
                signal_time = signal["time"]
                signal_price = signal.get(
                    "price",
                    price_df.loc[price_df["time"] == signal_time, "price"].values[0]
                    if len(price_df.loc[price_df["time"] == signal_time]) > 0
                    else price_df["price"].iloc[-1],
                )
                fig.add_trace(
                    go.Scatter(
                        x=[signal_time],
                        y=[signal_price],
                        mode="markers",
                        name="Sell",
                        marker={
                            "symbol": "triangle-down",
                            "size": 15,
                            "color": colors.danger,
                            "line": {"color": "white", "width": 1},
                        },
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

        # Add RSI indicator
        if isinstance(indicator_data, list):
            # Convert list of tuples to Series
            rsi_times = [item[0] for item in indicator_data]
            rsi_values = [item[1] for item in indicator_data]
            rsi_series = pd.Series(rsi_values, index=pd.to_datetime(rsi_times))
        else:
            rsi_series = indicator_data

        overbought = config.upper_threshold or 70
        oversold = config.lower_threshold or 30

        fig.add_trace(
            go.Scatter(
                x=rsi_series.index,
                y=rsi_series.values,
                mode="lines",
                name="RSI",
                line={"color": colors.secondary, "width": 2},
            ),
            row=2,
            col=1,
        )

        # Add RSI zones
        fig.add_hrect(
            y0=0,
            y1=oversold,
            fillcolor=colors.success,
            opacity=0.1,
            layer="below",
            line_width=0,
            row=2,
            col=1,
        )
        fig.add_hrect(
            y0=overbought,
            y1=100,
            fillcolor=colors.danger,
            opacity=0.1,
            layer="below",
            line_width=0,
            row=2,
            col=1,
        )

        # Add reference lines
        fig.add_hline(y=oversold, line_dash="dash", line_color=colors.success, row=2, col=1)
        fig.add_hline(y=50, line_dash="dash", line_color=colors.neutral, row=2, col=1)
        fig.add_hline(y=overbought, line_dash="dash", line_color=colors.danger, row=2, col=1)

        # Update layout
        fig.update_xaxes(title_text="Time", row=2, col=1)
        fig.update_yaxes(title_text="Price", row=1, col=1)
        fig.update_yaxes(title_text="RSI", range=[0, 100], row=2, col=1)
        fig.update_layout(
            height=800,
            hovermode="x unified",
            showlegend=True,
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        # For other indicators, use separate charts
        # Price chart with signals
        fig_price = plot_price_with_signals(
            price_data=price_df,
            buy_signals=buy_df,
            sell_signals=sell_df,
            title="Price with Buy/Sell Signals",
        )
        st.plotly_chart(fig_price, use_container_width=True)

        # Indicator chart if available (for non-RSI indicators)
        # Note: RSI with indicator_data is handled in the if branch above (line 244),
        # so this else branch only handles non-RSI indicators
        if _has_indicator_data(indicator_data):
            if isinstance(indicator_data, list):
                indicator_times = [item[0] for item in indicator_data]
                indicator_values = [item[1] for item in indicator_data]
                indicator_series = pd.Series(indicator_values, index=pd.to_datetime(indicator_times))
            else:
                indicator_series = indicator_data

            # Generic indicator line chart for non-RSI indicators
            fig_indicator = go.Figure()
            fig_indicator.add_trace(
                go.Scatter(
                    x=indicator_series.index,
                    y=indicator_series.values,
                    mode="lines",
                    name=config.indicator_name,
                    line={"color": "#1f77b4", "width": 2},
                )
            )
            fig_indicator.update_layout(
                title=f"{config.indicator_name} Indicator",
                xaxis_title="Time",
                yaxis_title=config.indicator_name,
                height=300,
            )
            st.plotly_chart(fig_indicator, use_container_width=True)


def _render_signal_status(
    session_state: dict[str, Any],
    strategy_config: dict[str, Any],
    config: TADashboardConfig,
) -> None:
    """Render the signal status section."""
    st.subheader("Signal Status")

    # Get indicator value
    indicator_key = config.indicator_name.lower()
    indicator_value = float(session_state.get(f"{indicator_key}_value", session_state.get(indicator_key, 50)))

    # Use custom signal function if provided
    if config.custom_signal_fn is not None:
        signal = config.custom_signal_fn(session_state)
        if "buy" in signal.lower() or "bullish" in signal.lower():
            st.success(signal)
        elif "sell" in signal.lower() or "bearish" in signal.lower():
            st.error(signal)
        else:
            st.info(signal)
        return

    # Default signal logic
    if config.upper_threshold is not None and config.lower_threshold is not None:
        if config.signal_type == "reversion":
            # Reversion: buy on low values, sell on high values
            if indicator_value < config.lower_threshold:
                st.success(
                    f"BUY SIGNAL: {config.indicator_name} ({indicator_value:.1f}) < {config.lower_threshold} (Oversold)"
                )
            elif indicator_value > config.upper_threshold:
                st.error(
                    f"SELL SIGNAL: {config.indicator_name} ({indicator_value:.1f}) > {config.upper_threshold} (Overbought)"
                )
            else:
                st.info(f"NEUTRAL: {config.indicator_name} ({indicator_value:.1f}) in normal range")
        else:
            # Momentum: buy on high values, sell on low values
            if indicator_value > config.upper_threshold:
                st.success(
                    f"BUY SIGNAL: {config.indicator_name} ({indicator_value:.1f}) > {config.upper_threshold} (Strong momentum)"
                )
            elif indicator_value < config.lower_threshold:
                st.error(
                    f"SELL SIGNAL: {config.indicator_name} ({indicator_value:.1f}) < {config.lower_threshold} (Weak momentum)"
                )
            else:
                st.info(f"NEUTRAL: {config.indicator_name} ({indicator_value:.1f}) in normal range")
    else:
        # No thresholds - just display value
        st.info(f"Current {config.indicator_name}: {indicator_value:.1f}")


def _render_position(
    session_state: dict[str, Any],
    base_token: str,
    quote_token: str,
) -> None:
    """Render the current position section."""
    base_balance = Decimal(str(session_state.get("base_balance", "0")))
    quote_balance = Decimal(str(session_state.get("quote_balance", "0")))

    # Get price from session state or use default
    base_price = Decimal(str(session_state.get("base_price", "1")))
    total_value = base_balance * base_price + quote_balance

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(f"{base_token}", f"{float(base_balance):.4f}")
    with col2:
        st.metric(f"{quote_token}", f"${float(quote_balance):,.2f}")
    with col3:
        st.metric("Total", f"${float(total_value):,.2f}")


def _render_performance(session_state: dict[str, Any]) -> None:
    """Render the performance metrics section."""
    pnl = Decimal(str(session_state.get("total_pnl", "0")))
    trades = session_state.get("total_trades", 0)
    win_rate = Decimal(str(session_state.get("win_rate", "50")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("PnL", f"${float(pnl):+,.2f}")
    with col2:
        st.metric("Trades", str(trades))
    with col3:
        st.metric("Win Rate", f"{float(win_rate):.0f}%")


# Pre-configured templates for common indicators


def get_rsi_config(period: int = 14, overbought: float = 70, oversold: float = 30) -> TADashboardConfig:
    """Get pre-configured RSI dashboard config."""
    return TADashboardConfig(
        indicator_name="RSI",
        indicator_period=period,
        upper_threshold=overbought,
        lower_threshold=oversold,
        signal_type="reversion",
        value_suffix="",
        show_progress_bar=True,
        progress_range=(0, 100),
    )


def get_macd_config(fast: int = 12, slow: int = 26, signal: int = 9) -> TADashboardConfig:
    """Get pre-configured MACD dashboard config."""
    return TADashboardConfig(
        indicator_name="MACD",
        indicator_period=fast,
        secondary_periods=[slow, signal],
        signal_type="momentum",
        value_format="{:+.2f}",
        show_progress_bar=False,
    )


def get_cci_config(period: int = 20, overbought: float = 100, oversold: float = -100) -> TADashboardConfig:
    """Get pre-configured CCI dashboard config."""
    return TADashboardConfig(
        indicator_name="CCI",
        indicator_period=period,
        upper_threshold=overbought,
        lower_threshold=oversold,
        signal_type="reversion",
        value_format="{:+.1f}",
        show_progress_bar=True,
        progress_range=(-200, 200),
    )


def get_stochastic_config(
    fast_k: int = 14, slow_k: int = 3, slow_d: int = 3, overbought: float = 80, oversold: float = 20
) -> TADashboardConfig:
    """Get pre-configured Stochastic dashboard config."""
    return TADashboardConfig(
        indicator_name="Stochastic",
        indicator_period=fast_k,
        secondary_periods=[slow_k, slow_d],
        upper_threshold=overbought,
        lower_threshold=oversold,
        signal_type="reversion",
        value_suffix="%",
        show_progress_bar=True,
        progress_range=(0, 100),
    )


def get_atr_config(period: int = 14) -> TADashboardConfig:
    """Get pre-configured ATR dashboard config."""
    return TADashboardConfig(
        indicator_name="ATR",
        indicator_period=period,
        signal_type="momentum",
        value_format="${:.2f}",
        show_progress_bar=False,
    )


def get_adx_config(period: int = 14, trend_threshold: float = 25) -> TADashboardConfig:
    """Get pre-configured ADX dashboard config."""
    return TADashboardConfig(
        indicator_name="ADX",
        indicator_period=period,
        lower_threshold=trend_threshold,
        signal_type="momentum",
        show_progress_bar=True,
        progress_range=(0, 100),
    )


def get_bollinger_config(period: int = 20, std_dev: float = 2.0) -> TADashboardConfig:
    """Get pre-configured Bollinger Bands dashboard config."""
    return TADashboardConfig(
        indicator_name="Bollinger",
        indicator_period=period,
        secondary_periods=[int(std_dev * 10)],  # Encode std_dev
        signal_type="reversion",
        value_format="${:.2f}",
        show_progress_bar=False,
    )
