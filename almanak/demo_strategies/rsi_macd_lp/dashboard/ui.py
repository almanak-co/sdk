"""RSI + MACD Confluence LP Strategy Dashboard.

Two primitives in one strategy: enters an LP position only when **both**
RSI and MACD confirm (conjunction entry), exits when **either** turns
bearish (disjunction exit). The dashboard renders both indicator series
using the same ``OHLCVRouter`` factory (VIB-4347) the strategy used in
its live ``decide()`` — same source, same timeframe, same provider
priority — so the chart faithfully reproduces the market context the
strategy saw when it made each decision.
"""

from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd
import streamlit as st

from almanak.framework.dashboard import (
    render_cost_stack_section,
    render_pnl_section,
    render_trade_tape_section,
)
from almanak.framework.dashboard.plots.ta_plots import (
    plot_macd_indicator,
    plot_price_with_signals,
    plot_rsi_indicator,
)
from almanak.framework.data.indicators.macd import MACDCalculator
from almanak.framework.data.indicators.rsi import RSICalculator


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the RSI + MACD Confluence LP custom dashboard."""
    st.title("RSI + MACD Confluence LP Dashboard")
    render_pnl_section(strategy_id)

    pool = strategy_config.get("pool", "WETH/USDC/500")
    chain = strategy_config.get("chain", "arbitrum")
    base_token, _quote_token = _parse_pool_tokens(pool)
    rsi_period = int(strategy_config.get("rsi_period", 14))
    rsi_oversold = float(strategy_config.get("rsi_oversold", 30))
    rsi_overbought = float(strategy_config.get("rsi_overbought", 70))
    macd_fast = int(strategy_config.get("macd_fast", 12))
    macd_slow = int(strategy_config.get("macd_slow", 26))
    macd_signal_period = int(strategy_config.get("macd_signal", 9))

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {pool} | **Chain:** {chain} | **DEX:** Uniswap V3")
    st.markdown(
        f"**RSI({rsi_period}):** ≤ {rsi_oversold:.0f} / ≥ {rsi_overbought:.0f}  "
        f"| **MACD:** {macd_fast}/{macd_slow}/{macd_signal_period}"
    )
    st.caption(
        "Entry requires BOTH signals bullish; exit on EITHER signal flip. "
        "Plots below show the same OHLCV the strategy itself fetched."
    )

    st.divider()
    # Strategy's market.rsi() / market.macd() resolve to RSICalculator /
    # MACDCalculator which fetch quote="USD" (rsi.py:629). Match that here so
    # the chart reflects the same series the strategy traded on rather than a
    # parallel quote_token-denominated chart.
    candles, fetch_error = _safe_get_ohlcv(api_client, token=base_token, chain=chain)
    price_df = _candles_to_price_df(candles)

    _render_price_panel(api_client, price_df, base_token, fetch_error)
    _render_rsi_panel(price_df, rsi_period, rsi_oversold, rsi_overbought, base_token)
    _render_macd_panel(price_df, macd_fast, macd_slow, macd_signal_period, base_token)

    st.divider()
    st.markdown("## Audit")
    render_cost_stack_section(strategy_id, heading="")
    render_trade_tape_section(strategy_id)


def _parse_pool_tokens(pool: str) -> tuple[str, str]:
    parts = pool.split("/")
    if len(parts) >= 2:
        return parts[0], parts[1]
    return "WETH", "USDC"


def _render_price_panel(
    api_client: Any,
    price_df: pd.DataFrame,
    base_token: str,
    fetch_error: str | None,
) -> None:
    """Render the price-with-signals chart for the LP's base token."""
    st.subheader("Price (router-fetched OHLCV)")
    if fetch_error is not None:
        st.error(f"OHLCV fetch failed via shared router: {fetch_error}")
        return
    if price_df.empty:
        st.info(
            "No OHLCV candles returned by the shared router yet — the "
            "strategy needs at least one successful indicator fetch."
        )
        return
    st.plotly_chart(
        plot_price_with_signals(
            price_df,
            buy_signals=_signals_df(api_client, price_df, side="buy"),
            sell_signals=_signals_df(api_client, price_df, side="sell"),
            title=f"{base_token}/USD ({len(price_df)} candles)",
        ),
        use_container_width=True,
    )


def _render_rsi_panel(
    price_df: pd.DataFrame,
    rsi_period: int,
    rsi_oversold: float,
    rsi_overbought: float,
    base_token: str,
) -> None:
    if price_df.empty:
        return
    rsi_values, rsi_index = _rolling_rsi(price_df, rsi_period)
    st.subheader(f"RSI({rsi_period})")
    if not rsi_values:
        st.info(
            f"Not enough candles to compute RSI({rsi_period}). Need at least {rsi_period + 1}, got {len(price_df)}."
        )
        return
    st.plotly_chart(
        plot_rsi_indicator(
            rsi_data=rsi_values,
            time_index=rsi_index,
            overbought=rsi_overbought,
            oversold=rsi_oversold,
            current_value=rsi_values[-1],
            title=f"RSI({rsi_period}) — {base_token}",
        ),
        use_container_width=True,
    )


def _render_macd_panel(
    price_df: pd.DataFrame,
    fast: int,
    slow: int,
    signal_period: int,
    base_token: str,
) -> None:
    if price_df.empty:
        return
    macd_line, signal_line, hist, time_index = _rolling_macd(
        price_df, fast=fast, slow=slow, signal_period=signal_period
    )
    st.subheader(f"MACD ({fast}/{slow}/{signal_period})")
    if not macd_line:
        st.info(f"Not enough candles to compute MACD. Need at least {slow + signal_period}, got {len(price_df)}.")
        return
    st.plotly_chart(
        plot_macd_indicator(
            macd=macd_line,
            macd_signal=signal_line,
            macd_hist=hist,
            time_index=time_index,
            title=f"MACD — {base_token}",
        ),
        use_container_width=True,
    )


def _safe_get_ohlcv(
    api_client: Any,
    *,
    token: str,
    chain: str,
) -> tuple[list[dict[str, Any]], str | None]:
    """Pull OHLCV via the gateway-backed router and never raise.

    Returns ``(candles, error)`` so the dashboard can distinguish a legitimately
    empty fetch from a broken router / gateway / auth path. Quote is hardcoded
    to ``"USD"`` to match the upstream ``RSICalculator`` / ``MACDCalculator``
    fetch (rsi.py:629).
    """
    try:
        candles = api_client.get_ohlcv(
            token=token,
            quote="USD",
            timeframe="4h",
            limit=168,
            chain=chain,
        )
        return candles or [], None
    except Exception as exc:  # noqa: BLE001
        return [], f"{type(exc).__name__}: {exc}"


def _candles_to_price_df(candles: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for c in candles:
        try:
            rows.append({"time": c["timestamp"], "price": float(Decimal(str(c["close"])))})
        except (KeyError, ValueError, TypeError, InvalidOperation):
            continue
    if not rows:
        return pd.DataFrame(columns=["time", "price"])
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    return df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)


def _rolling_rsi(price_df: pd.DataFrame, period: int) -> tuple[list[float], list]:
    closes = [Decimal(str(p)) for p in price_df["price"].tolist()]
    needed = period + 1
    if len(closes) < needed:
        return [], []
    values: list[float] = []
    index: list = []
    for end in range(needed, len(closes) + 1):
        try:
            rsi = RSICalculator.calculate_rsi_from_prices(closes[:end], period=period)
        except Exception:
            continue
        values.append(rsi)
        index.append(price_df["time"].iloc[end - 1])
    return values, index


def _rolling_macd(
    price_df: pd.DataFrame,
    *,
    fast: int,
    slow: int,
    signal_period: int,
) -> tuple[list[float], list[float], list[float], list]:
    """Roll the same MACD calculator the strategy uses across the window.

    ``MACDCalculator.calculate_macd_from_prices`` returns the latest
    triple per call; sliding the window over the close series yields the
    series the strategy effectively observed.
    """
    closes = [Decimal(str(p)) for p in price_df["price"].tolist()]
    needed = slow + signal_period
    if len(closes) < needed:
        return [], [], [], []
    macd_vals: list[float] = []
    signal_vals: list[float] = []
    hist_vals: list[float] = []
    index: list = []
    for end in range(needed, len(closes) + 1):
        try:
            result = MACDCalculator.calculate_macd_from_prices(
                closes[:end],
                fast_period=fast,
                slow_period=slow,
                signal_period=signal_period,
            )
        except Exception:
            continue
        macd_vals.append(float(result.macd_line))
        signal_vals.append(float(result.signal_line))
        hist_vals.append(float(result.histogram))
        index.append(price_df["time"].iloc[end - 1])
    return macd_vals, signal_vals, hist_vals, index


def _signals_df(api_client: Any, price_df: pd.DataFrame, *, side: str) -> pd.DataFrame | None:
    """LP entries/exits map onto OPEN / CLOSE PositionEvent rows.

    PositionEventType values are bare ``OPEN`` / ``CLOSE`` (see
    ``almanak.framework.observability.position_events.PositionEventType``);
    the intent-type strings ``LP_OPEN`` / ``LP_CLOSE`` map *to* those event
    types but never appear in the event row itself. PositionEvent rows do not
    carry an execution price, so the marker price is interpolated from the
    chart's nearest candle by timestamp.
    """
    try:
        events = api_client.get_position_events(position_types=["LP"]) or []
    except Exception:
        return None
    if not events or price_df.empty:
        return None
    want = "OPEN" if side == "buy" else "CLOSE"
    chart_times = pd.to_datetime(price_df["time"], utc=True, errors="coerce")
    rows = []
    for e in events:
        kind = (e.get("event_type") or "").upper()
        if kind != want:
            continue
        ts = e.get("timestamp")
        if ts is None:
            continue
        ts_parsed = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(ts_parsed):
            continue
        idx = (chart_times - ts_parsed).abs().idxmin()
        rows.append({"time": ts_parsed, "price": float(price_df["price"].iloc[idx])})
    if not rows:
        return None
    return pd.DataFrame(rows).sort_values("time").reset_index(drop=True)
