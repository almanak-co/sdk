"""RSI + MACD Confluence LP Strategy Dashboard.

Two primitives in one strategy: enters an LP position only when **both** RSI and
MACD confirm (conjunction entry), exits when **either** turns bearish
(disjunction exit).

Multi-indicator dashboard (VIB-4897): the RSI and MACD panels are composed with
``multi_ta_config`` and rendered by the shared ``render_ta_dashboard`` template,
which fetches the OHLCV once (via the same gateway-backed router the strategy's
``market.rsi()`` / ``market.macd()`` use), computes both indicator series, and
stacks one panel per indicator under a shared price chart — replacing the
hand-rolled rolling/plot plumbing this dashboard used to carry.

LP nuance: an LP strategy's "trades" are ``LP_OPEN`` / ``LP_CLOSE`` *position
events*, not SWAP rows, so the template's swap-derived markers are empty here.
We overwrite the buy/sell markers with the LP open/close events
(:func:`_lp_markers`) so entries/exits still show on the price chart.
"""

from typing import Any

import pandas as pd

from almanak.framework.dashboard.templates import (
    get_macd_config,
    get_rsi_config,
    multi_ta_config,
    prepare_ta_session_state,
    render_ta_dashboard,
)


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the RSI + MACD Confluence LP custom dashboard."""
    base_token, quote_token = _parse_pool_tokens(strategy_config.get("pool", "WETH/USDC/500"))

    # RSI + MACD as stacked panels on one shared price chart.
    config = multi_ta_config(
        get_rsi_config(
            period=int(strategy_config.get("rsi_period", 14)),
            overbought=float(strategy_config.get("rsi_overbought", 70)),
            oversold=float(strategy_config.get("rsi_oversold", 30)),
        ),
        get_macd_config(
            fast=int(strategy_config.get("macd_fast", 12)),
            slow=int(strategy_config.get("macd_slow", 26)),
            signal=int(strategy_config.get("macd_signal", 9)),
        ),
    )
    config.base_token = base_token
    config.quote_token = quote_token
    config.chain = str(strategy_config.get("chain", config.chain))
    config.protocol = "uniswap_v3"

    # Framework fetches OHLCV once and computes the RSI + MACD series.
    session_state = prepare_ta_session_state(api_client, session_state=session_state, config=config)

    # This is an LP strategy: surface LP_OPEN / LP_CLOSE position events as the
    # price-chart markers (prepare_ta_session_state's swap-tape markers are empty
    # for an LP). Overwrite only once we have a price series to anchor them to.
    price_df = session_state.get("price_history")
    if isinstance(price_df, pd.DataFrame) and not price_df.empty:
        session_state["buy_signals"] = _lp_markers(api_client, price_df, side="open")
        session_state["sell_signals"] = _lp_markers(api_client, price_df, side="close")

    render_ta_dashboard(deployment_id, strategy_config, session_state, config)


def _parse_pool_tokens(pool: str) -> tuple[str, str]:
    parts = pool.split("/")
    if len(parts) >= 2:
        return parts[0], parts[1]
    return "WETH", "USDC"


def _lp_markers(api_client: Any, price_df: pd.DataFrame, *, side: str) -> pd.DataFrame | None:
    """LP entries/exits as price-chart markers from OPEN / CLOSE position events.

    ``PositionEventType`` values are bare ``OPEN`` / ``CLOSE`` (the intent-type
    strings ``LP_OPEN`` / ``LP_CLOSE`` map *to* those event types but never
    appear in the row). Position events carry no execution price, so the marker
    price is interpolated from the chart's nearest candle by timestamp.
    """
    try:
        events = api_client.get_position_events(position_types=["LP"]) or []
    except Exception:  # noqa: BLE001
        return None
    if not events or price_df.empty:
        return None
    want = "OPEN" if side == "open" else "CLOSE"
    chart_times = pd.to_datetime(price_df["time"], utc=True, errors="coerce")
    rows = []
    for e in events:
        if (e.get("event_type") or "").upper() != want:
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
