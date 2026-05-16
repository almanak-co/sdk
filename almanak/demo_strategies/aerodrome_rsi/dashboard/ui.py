"""Aerodrome RSI Strategy Dashboard.

Renders the same OHLCV and RSI series the strategy itself computes during
``decide()`` — no re-derivation against a different data source. The
``api_client.get_ohlcv(...)`` call routes through the shared
``OHLCVRouter`` factory (VIB-4347) that ``MarketSnapshot.ohlcv()`` uses
in-process, so the chart faithfully reproduces the market context the
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
    plot_price_with_signals,
    plot_rsi_indicator,
)
from almanak.framework.data.indicators.rsi import RSICalculator


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the Aerodrome RSI custom dashboard."""
    st.title("Aerodrome RSI Strategy Dashboard")
    render_pnl_section(strategy_id)

    base_token = strategy_config.get("base_token", "WETH")
    quote_token = strategy_config.get("quote_token", "USDC")
    chain = strategy_config.get("chain", "base")
    rsi_period = int(strategy_config.get("rsi_period", 14))
    rsi_oversold = float(strategy_config.get("rsi_oversold", 30))
    rsi_overbought = float(strategy_config.get("rsi_overbought", 70))

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token} | **Chain:** {chain} | **DEX:** Aerodrome")
    st.markdown(f"**RSI({rsi_period}):** oversold ≤ {rsi_oversold:.0f}, overbought ≥ {rsi_overbought:.0f}")

    st.divider()
    _render_indicator_section(
        api_client=api_client,
        base_token=base_token,
        chain=chain,
        rsi_period=rsi_period,
        rsi_oversold=rsi_oversold,
        rsi_overbought=rsi_overbought,
    )

    st.divider()
    st.markdown("## Audit")
    render_cost_stack_section(strategy_id, heading="")
    render_trade_tape_section(strategy_id)


def _render_indicator_section(
    *,
    api_client: Any,
    base_token: str,
    chain: str,
    rsi_period: int,
    rsi_oversold: float,
    rsi_overbought: float,
) -> None:
    """Fetch OHLCV through the shared router and plot price + RSI series.

    The dashboard re-fetches via the same composition path the strategy
    used live (``OHLCVRouter`` → gateway providers). It does not read a
    saved snapshot of what the strategy saw — re-fetching is sufficient
    for now (see VIB-3975 scope note).
    """
    st.subheader("Indicator")

    # Strategy's market.rsi() resolves to RSICalculator which fetches quote="USD"
    # (rsi.py:629). Match that here so the chart reflects the same series the
    # strategy traded on rather than a parallel quote_token-denominated chart.
    candles, fetch_error = _safe_get_ohlcv(api_client, token=base_token, chain=chain)
    if fetch_error is not None:
        st.error(f"OHLCV fetch failed via shared router: {fetch_error}")
        return
    if not candles:
        st.info(
            "No OHLCV candles returned by the shared router yet — "
            "the strategy needs at least one successful indicator fetch "
            "(see VIB-4347)."
        )
        return

    price_df = _candles_to_price_df(candles)
    if price_df.empty:
        st.info("OHLCV envelope returned no close-price rows.")
        return

    rsi_values, rsi_index = _rolling_rsi(price_df, rsi_period)

    # Buy/sell markers are intentionally omitted — swap intents do not produce
    # PositionEvent rows (only LP / PERP / lending do), so the gateway's
    # GetPositionEventsFiltered returns nothing for this strategy. Wiring
    # markers from the transaction ledger is tracked as a follow-up.
    st.plotly_chart(
        plot_price_with_signals(
            price_df,
            title=f"{base_token}/USD (router-fetched, {len(price_df)} candles)",
        ),
        use_container_width=True,
    )

    if rsi_values:
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
    else:
        st.info(
            f"Not enough candles to compute RSI({rsi_period}). Need at least {rsi_period + 1}, got {len(price_df)}."
        )


def _safe_get_ohlcv(
    api_client: Any,
    *,
    token: str,
    chain: str,
) -> tuple[list[dict[str, Any]], str | None]:
    """Pull OHLCV via the gateway-backed router and never raise.

    Returns a ``(candles, error)`` tuple so the dashboard can distinguish a
    legitimately empty fetch (``([], None)``) from a broken router / gateway /
    auth path (``([], "<reason>")``). Quote is hardcoded to ``"USD"`` to match
    the upstream ``RSICalculator`` / ``MACDCalculator`` fetch — see
    ``almanak.framework.data.indicators.rsi.RSICalculator.calculate_rsi``.
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
    """Convert ``DashboardAPIClient.get_ohlcv()`` output to plot input."""
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
    """Compute RSI series using the same calculator the strategy uses.

    Uses ``RSICalculator.calculate_rsi_from_prices`` (Wilder's smoothing)
    over a rolling window so the dashboard's RSI line matches what
    ``market.rsi()`` would have returned at each historical step.
    """
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
