"""
Technical Analysis (TA) Dashboard Template.

Reusable template for creating dashboards for indicator-based strategies.
Supports any TA indicator with configurable signal logic and visualization.

Scope (single-position): the template drives one position on one
``base_token``/``quote_token`` pair. The 3 accounting sections (PnL, Cost
Stack, Trade Tape) are baked in so every TA dashboard ships with full
accounting by default.

- **Single indicator** — pass one ``TADashboardConfig``.
- **Multiple indicators (multi-signal, VIB-4897)** — compose with
  :func:`multi_ta_config`. The template stacks one panel per indicator under a
  shared price chart (all on one time axis). Useful for confluence strategies
  (e.g. RSI + MACD + Bollinger).

For genuinely multi-*position* layouts the template is still the wrong tool —
hand-roll from the section helpers (``render_pnl_section``,
``render_cost_stack_section``, ``render_trade_tape_section``) plus the
primitive plot helpers in ``almanak.framework.dashboard.plots``. See the
dashboard blueprints.

Usage (single indicator):
    from almanak.framework.dashboard.templates import TADashboardConfig, render_ta_dashboard

    config = TADashboardConfig(
        indicator_name="RSI", indicator_period=14,
        upper_threshold=70, lower_threshold=30, signal_type="reversion",
    )

    def render_custom_dashboard(deployment_id, strategy_config, api_client, session_state):
        session_state = prepare_ta_session_state(api_client, session_state, config)
        render_ta_dashboard(deployment_id, strategy_config, session_state, config)

Usage (multi-signal):
    from almanak.framework.dashboard.templates import (
        get_rsi_config, get_macd_config, get_bollinger_config,
        multi_ta_config, prepare_ta_session_state, render_ta_dashboard,
    )

    config = multi_ta_config(get_rsi_config(), get_macd_config(), get_bollinger_config())

    def render_custom_dashboard(deployment_id, strategy_config, api_client, session_state):
        session_state = prepare_ta_session_state(api_client, session_state, config)
        render_ta_dashboard(deployment_id, strategy_config, session_state, config)
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from almanak.framework.dashboard.plots import (
    plot_adx_indicator,
    plot_atr_indicator,
    plot_bollinger_bands,
    plot_cci_indicator,
    plot_macd_indicator,
    plot_price_with_signals,
    plot_rsi_indicator,
    plot_stochastic_indicator,
)
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
        custom_signal_fn: Optional custom function for signal determination
        chain: Default chain name
        protocol: Default protocol name
        base_token: Default base token
        quote_token: Default quote token
        extra_indicators: Additional indicator configs to render as stacked
            panels (multi-signal layout, VIB-4897). Empty by default — the
            single-indicator path is unchanged. Compose via
            :func:`multi_ta_config`. Each extra contributes one more panel
            beneath the price chart; the extras' pair/chain are ignored (the
            primary config drives the shared OHLCV fetch).
    """

    indicator_name: str
    indicator_period: int = 14
    secondary_periods: list[int] = field(default_factory=list)
    upper_threshold: float | None = None
    lower_threshold: float | None = None
    signal_type: Literal["reversion", "momentum"] = "reversion"
    value_format: str = "{:.1f}"
    value_suffix: str = ""
    custom_signal_fn: Callable[[dict[str, Any]], str] | None = None
    chain: str = "Arbitrum"
    protocol: str = "Uniswap V3"
    base_token: str = "WETH"
    quote_token: str = "USDC"
    extra_indicators: list["TADashboardConfig"] = field(default_factory=list)


def multi_ta_config(primary: TADashboardConfig, *extras: TADashboardConfig) -> TADashboardConfig:
    """Compose a multi-indicator (multi-signal) TA dashboard config.

    The first config is the **primary** — it drives the dashboard title, the
    configured pair/chain, the shared OHLCV fetch, and the signal-status
    section. Each additional config renders as one more stacked indicator panel
    beneath the price chart. Indicator params (periods / thresholds) on the
    extras are honoured; their ``base_token`` / ``quote_token`` / ``chain`` are
    not — the primary's pair is authoritative so every panel shares one time
    axis.

    Example::

        config = multi_ta_config(
            get_rsi_config(period=14),
            get_macd_config(),
            get_bollinger_config(period=20, std_dev=2.0),
        )
        session_state = prepare_ta_session_state(api_client, session_state, config)
        render_ta_dashboard(deployment_id, strategy_config, session_state, config)

    Returns a new config (does not mutate ``primary``).
    """
    return replace(primary, extra_indicators=list(extras))


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


def _ema_sma_seeded(values: pd.Series, period: int) -> pd.Series:
    """Compute an EMA series seeded with the SMA of the first ``period`` values.

    Mirrors the scalar implementation in
    ``almanak.framework.data.indicators.macd.MACDCalculator._calculate_ema``
    (k = 2 / (period + 1), SMA seed) but returns the rolling series. It is
    reimplemented here rather than imported to keep the dashboard import lean —
    see ``tests/framework/dashboard/test_imports_lean.py``. The first
    ``period - 1`` entries are NaN; index ``period - 1`` holds the SMA seed.
    """
    floats = values.astype(float).to_numpy()
    n = len(floats)
    ema: list[float] = [float("nan")] * n
    if n < period:
        return pd.Series(ema, index=values.index)
    k = 2.0 / (period + 1)
    prev = float(floats[:period].mean())
    ema[period - 1] = prev
    for i in range(period, n):
        prev = floats[i] * k + prev * (1 - k)
        ema[i] = prev
    return pd.Series(ema, index=values.index)


def _macd_series_from_closes(closes: pd.Series, fast: int, slow: int, signal: int) -> pd.DataFrame:
    """Compute the MACD line, signal line, and histogram series from closes.

    Matches the framework's ``MACDCalculator`` (SMA-seeded EMAs; signal line is
    the EMA of the MACD line; histogram = MACD - signal) so the dashboard chart
    agrees with the strategy's own MACD signals. Returns an empty frame
    (columns ``macd`` / ``signal`` / ``histogram``) when there are too few rows.
    """
    empty = pd.DataFrame(columns=["macd", "signal", "histogram"])
    if len(closes) < slow + signal:
        return empty
    macd_line = (_ema_sma_seeded(closes, fast) - _ema_sma_seeded(closes, slow)).rename("macd")
    valid = macd_line.dropna()
    if len(valid) < signal:
        return empty
    # Signal line is the EMA of the (contiguous) valid MACD values, realigned.
    signal_valid = _ema_sma_seeded(valid.reset_index(drop=True), signal)
    signal_line = pd.Series(float("nan"), index=macd_line.index, name="signal")
    signal_line.loc[valid.index] = signal_valid.to_numpy()
    histogram = (macd_line - signal_line).rename("histogram")
    return pd.DataFrame({"macd": macd_line, "signal": signal_line, "histogram": histogram})


def _high_low_close(price_df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Return float ``(high, low, close)`` series from a price history frame.

    ``price`` is the close. When the frame carries no ``high`` / ``low``
    (caller supplied a close-only history) both fall back to the close —
    the same close==high==low degradation the framework's own
    ``ATRCalculator.calculate_atr_from_prices`` documents — so the range
    indicators still produce a (flattened) series instead of crashing.
    """
    close = price_df["price"].astype(float)
    # A row can carry a valid close but a NaN high/low (bad/missing OHLC field
    # coerced by ``pd.to_numeric(errors="coerce")``); fill those from the close
    # so the NaN doesn't poison the rolling window and blank the chart.
    high = price_df["high"].astype(float).fillna(close) if "high" in price_df.columns else close
    low = price_df["low"].astype(float).fillna(close) if "low" in price_df.columns else close
    return high, low, close


def _bollinger_bands_from_closes(closes: pd.Series, period: int, std_dev: float) -> pd.DataFrame:
    """Compute rolling Bollinger Bands (upper / middle / lower) from closes.

    Mirrors ``BollingerBandsCalculator.calculate_bollinger_from_prices``:
    middle = SMA(period), bands = middle ± std_dev · population-σ (``ddof=0``,
    dividing by ``period`` to match the calculator's variance). Reimplemented
    locally rather than imported to keep the dashboard import lean (see
    ``tests/framework/dashboard/test_imports_lean.py``). Returns an empty
    ``upper/middle/lower`` frame when there are too few rows.
    """
    empty = pd.DataFrame(columns=["upper", "middle", "lower"])
    if len(closes) < period:
        return empty
    floats = closes.astype(float)
    middle = floats.rolling(period).mean()
    sigma = floats.rolling(period).std(ddof=0)
    upper = middle + std_dev * sigma
    lower = middle - std_dev * sigma
    return pd.DataFrame({"upper": upper, "middle": middle, "lower": lower})


def _cci_series(price_df: pd.DataFrame, period: int) -> pd.Series:
    """Compute the rolling CCI series from a price history frame.

    Mirrors ``CCICalculator.calculate_cci_from_candles``: typical price
    ``(H+L+C)/3``, then ``(TP - SMA(TP)) / (0.015 · meanDeviation(TP))`` over
    each window (mean deviation divides by ``period``). CCI is 0 where the
    mean deviation is 0.
    """
    high, low, close = _high_low_close(price_df)
    if len(close) < period:
        return pd.Series(dtype=float, name="cci")
    typical = (high + low + close) / 3.0
    sma = typical.rolling(period).mean()
    mean_dev = typical.rolling(period).apply(lambda w: float(abs(w - w.mean()).mean()), raw=True)
    cci = (typical - sma) / (0.015 * mean_dev)
    cci = cci.where(mean_dev != 0, other=0.0)
    return cci.rename("cci")


def _stochastic_series(price_df: pd.DataFrame, k_period: int, d_period: int) -> pd.DataFrame:
    """Compute the rolling Stochastic %K / %D series from a price history frame.

    Mirrors ``StochasticCalculator.calculate_stochastic_from_candles``:
    ``%K = 100 · (close - LL) / (HH - LL)`` over ``k_period`` (50 when the
    range is flat), ``%D = SMA(%K, d_period)``. Returns an empty ``k/d`` frame
    when there are fewer than ``k_period + d_period - 1`` rows — %D needs that
    many to yield a single non-NaN value, matching the calculator's
    ``required`` threshold.
    """
    empty = pd.DataFrame(columns=["k", "d"])
    high, low, close = _high_low_close(price_df)
    if len(close) < k_period + d_period - 1:
        return empty
    lowest_low = low.rolling(k_period).min()
    highest_high = high.rolling(k_period).max()
    price_range = highest_high - lowest_low
    k = 100.0 * (close - lowest_low) / price_range
    k = k.where(price_range > 0, other=50.0)
    d = k.rolling(d_period).mean()
    return pd.DataFrame({"k": k, "d": d})


def _atr_series(price_df: pd.DataFrame, period: int) -> pd.Series:
    """Compute the rolling ATR series via Wilder's smoothing.

    Mirrors ``ATRCalculator.calculate_atr_from_candles``: True Range =
    ``max(H-L, |H-prevC|, |L-prevC|)``; the seed ATR at index ``period`` is the
    simple mean of the first ``period`` True Ranges, then Wilder smoothing
    ``ATR = (priorATR·(period-1) + TR) / period``. NaN before the seed.
    """
    high, low, close = _high_low_close(price_df)
    n = len(close)
    out = pd.Series([float("nan")] * n, index=price_df.index, name="atr")
    if n < period + 1:
        return out
    prev_close = close.shift(1)
    true_range = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    tr = true_range.to_numpy()  # tr[0] is NaN (no prior close)
    values = [float("nan")] * n
    atr = float(tr[1 : period + 1].mean())
    values[period] = atr
    for i in range(period + 1, n):
        atr = (atr * (period - 1) + tr[i]) / period
        values[i] = atr
    return pd.Series(values, index=price_df.index, name="atr")


def _adx_series(price_df: pd.DataFrame, period: int) -> pd.DataFrame:
    """Compute the rolling ADX / +DI / -DI series via Wilder's smoothing.

    Mirrors ``ADXCalculator.calculate_adx_from_candles``: directional movement
    and True Range are Wilder-smoothed into +DI / -DI; ``DX = 100·|+DI - -DI| /
    (+DI + -DI)``; ADX seeds at the mean of the first ``period`` DX values then
    Wilder-smooths. The first ADX lands at index ``2·period - 1``. Returns an
    empty ``adx/plus_di/minus_di`` frame when there are too few rows.
    """
    empty = pd.DataFrame(columns=["adx", "plus_di", "minus_di"])
    high, low, close = _high_low_close(price_df)
    n = len(close)
    if n < period * 2:
        return empty

    h = high.to_numpy()
    low_arr = low.to_numpy()
    c = close.to_numpy()
    tr_values, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        high_diff = h[i] - h[i - 1]
        low_diff = low_arr[i - 1] - low_arr[i]
        plus_dm.append(high_diff if high_diff > low_diff and high_diff > 0 else 0.0)
        minus_dm.append(low_diff if low_diff > high_diff and low_diff > 0 else 0.0)
        tr_values.append(max(h[i] - low_arr[i], abs(h[i] - c[i - 1]), abs(low_arr[i] - c[i - 1])))

    tr_smooth = sum(tr_values[:period])
    plus_smooth = sum(plus_dm[:period])
    minus_smooth = sum(minus_dm[:period])

    plus_di_series = [float("nan")] * n
    minus_di_series = [float("nan")] * n
    adx_series = [float("nan")] * n

    def _di(smooth: float) -> float:
        return 100.0 * (smooth / tr_smooth) if tr_smooth > 0 else 0.0

    def _dx(plus: float, minus: float) -> float:
        denom = plus + minus
        return 100.0 * abs(plus - minus) / denom if denom > 0 else 0.0

    # First smoothed DI/DX correspond to candle index ``period`` (tr_values[k]
    # maps to candle k+1, so the first ``period`` TRs cover candles 1..period).
    plus_di = _di(plus_smooth)
    minus_di = _di(minus_smooth)
    plus_di_series[period] = plus_di
    minus_di_series[period] = minus_di
    dx_values = [_dx(plus_di, minus_di)]

    for idx in range(period, len(tr_values)):
        tr_smooth = tr_smooth - (tr_smooth / period) + tr_values[idx]
        plus_smooth = plus_smooth - (plus_smooth / period) + plus_dm[idx]
        minus_smooth = minus_smooth - (minus_smooth / period) + minus_dm[idx]
        plus_di = _di(plus_smooth)
        minus_di = _di(minus_smooth)
        candle_idx = idx + 1
        plus_di_series[candle_idx] = plus_di
        minus_di_series[candle_idx] = minus_di
        dx_values.append(_dx(plus_di, minus_di))

    # ADX seeds at the mean of the first ``period`` DX values (dx_values[j]
    # maps to candle index ``period + j``), then Wilder-smooths.
    adx = sum(dx_values[:period]) / period
    adx_series[period * 2 - 1] = adx
    for j in range(period, len(dx_values)):
        adx = (adx * (period - 1) + dx_values[j]) / period
        adx_series[period + j] = adx

    return pd.DataFrame(
        {"adx": adx_series, "plus_di": plus_di_series, "minus_di": minus_di_series},
        index=price_df.index,
    )


def _ohlcv_to_price_history(ohlcv: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalise an api_client.get_ohlcv() payload into ``{time, price}`` rows.

    ``high`` / ``low`` are carried through as extra columns when the payload
    provides them — the OHLC-range indicators (CCI, Stochastic, ATR, ADX)
    need them, while close-only indicators (RSI, MACD, Bollinger) just read
    ``price``. They are dropped silently when absent so callers that supply a
    close-only price history keep working.
    """
    base_cols = ["time", "price"]
    if not ohlcv:
        return pd.DataFrame(columns=base_cols)
    df = pd.DataFrame(ohlcv)
    if "timestamp" not in df.columns or "close" not in df.columns:
        return pd.DataFrame(columns=base_cols)
    df = df.rename(columns={"timestamp": "time", "close": "price"})
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    keep = list(base_cols)
    for col in ("high", "low"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            keep.append(col)

    df = df.dropna(subset=base_cols).sort_values("time").reset_index(drop=True)
    return df[keep]


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
        if ts is None:
            continue
        marker = {"time": ts, "price": _trade_row_marker_price(r, ti, to, qu, bu)}
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


def _trade_row_marker_price(
    row: dict[str, Any],
    token_in: str,
    token_out: str,
    quote_token: str,
    base_token: str,
) -> str | None:
    """Return marker y-value in chart units: quote token per base token.

    Ledger ``effective_price`` is protocol/receipt-shaped and may be stored
    as output-per-input. For a BUY like ``USDC -> WETH`` that is WETH per
    USDC (~0.00047), which plots off-screen on a WETH/USD chart. Prefer the
    explicit amounts and derive quote/base:

    * BUY  quote -> base: amount_in / amount_out
    * SELL base -> quote: amount_out / amount_in

    Fall back to ``effective_price`` only when amount fields are unavailable.
    """

    amount_in = _decimal_or_none(row.get("amount_in"))
    amount_out = _decimal_or_none(row.get("amount_out"))
    if amount_in is not None and amount_out is not None and amount_in != Decimal("0") and amount_out != Decimal("0"):
        if token_in == quote_token and token_out == base_token:
            return str(amount_in / amount_out)
        if token_in == base_token and token_out == quote_token:
            return str(amount_out / amount_in)

    price = row.get("effective_price")
    if price in (None, ""):
        return None
    return str(price)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


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
                    "amount_in": getattr(r, "amount_in", ""),
                    "token_out": getattr(r, "token_out", ""),
                    "amount_out": getattr(r, "amount_out", ""),
                    "effective_price": getattr(r, "effective_price", ""),
                }
            )
        except Exception:  # noqa: BLE001
            continue
    return out


def _event_field(event: Any, field: str) -> Any:
    if isinstance(event, dict):
        return event.get(field)
    return getattr(event, field, None)


def _strategy_start_time(api_client: Any) -> datetime | pd.Timestamp | None:
    """Return the strategy start timestamp from gateway timeline events."""
    if api_client is None or not hasattr(api_client, "get_timeline"):
        return None
    try:
        events = api_client.get_timeline(limit=200)
    except Exception:  # noqa: BLE001
        logger.debug("api_client.get_timeline() failed", exc_info=True)
        return None
    if not events:
        return None

    candidates: list[Any] = []
    fallback: list[Any] = []
    for event in events:
        ts = _event_field(event, "timestamp")
        if not ts:
            continue
        fallback.append(ts)
        if str(_event_field(event, "event_type")).upper() == "STRATEGY_STARTED":
            candidates.append(ts)

    source = candidates or fallback
    if not source:
        return None
    parsed = pd.to_datetime(source, utc=True, errors="coerce")
    parsed = parsed[~pd.isna(parsed)]
    if len(parsed) == 0:
        return None
    return parsed.min()


def _time_indexed(series_or_frame: pd.Series | pd.DataFrame, price_df: pd.DataFrame) -> pd.Series | pd.DataFrame:
    """Re-index a positionally-aligned series/frame onto the price-history time axis.

    The compute helpers return values aligned to ``price_df`` row order; the
    renderers consume them with a ``DatetimeIndex`` x-axis, so swap the index
    here in one place.
    """
    out = series_or_frame.copy()
    out.index = pd.DatetimeIndex(price_df["time"])
    return out


def _apply_rsi(result: dict[str, Any], price_df: pd.DataFrame, config: TADashboardConfig) -> None:
    """Emit ``rsi_history`` (Series) + latest scalar — close-only."""
    rsi = _time_indexed(_rsi_series_from_closes(price_df["price"], config.indicator_period), price_df).dropna()
    if rsi.empty:
        return
    result["rsi_history"] = rsi.rename("rsi")
    result.setdefault("rsi_value", float(rsi.iloc[-1]))
    result.setdefault("rsi", result["rsi_value"])


def _apply_macd(result: dict[str, Any], price_df: pd.DataFrame, config: TADashboardConfig) -> None:
    """Emit ``macd_data`` (macd/signal/histogram frame) + latest scalars — close-only."""
    periods = list(config.secondary_periods or [])
    slow = int(periods[0]) if len(periods) > 0 else 26
    signal_period = int(periods[1]) if len(periods) > 1 else 9
    macd_df = _macd_series_from_closes(price_df["price"], int(config.indicator_period), slow, signal_period)
    if macd_df.empty:
        return
    macd_df = _time_indexed(macd_df, price_df).dropna(subset=["macd"])
    if macd_df.empty:
        return
    result["macd_data"] = macd_df
    latest = macd_df.iloc[-1]
    result.setdefault("macd_value", float(latest["macd"]))
    result.setdefault("signal_line", float(latest["signal"]))
    result.setdefault("histogram", float(latest["histogram"]))
    result.setdefault("macd", result["macd_value"])


def _apply_bollinger(result: dict[str, Any], price_df: pd.DataFrame, config: TADashboardConfig) -> None:
    """Emit ``bollinger_data`` (upper/middle/lower frame) — close-only.

    ``get_bollinger_config`` encodes ``std_dev`` as ``secondary_periods=[std_dev*10]``;
    decode it back here.
    """
    periods = list(config.secondary_periods or [])
    std_dev = (periods[0] / 10.0) if periods else 2.0
    bands = _bollinger_bands_from_closes(price_df["price"], int(config.indicator_period), std_dev)
    if bands.empty:
        return
    bands = _time_indexed(bands, price_df).dropna()
    if bands.empty:
        return
    result["bollinger_data"] = bands
    result.setdefault("bollinger_value", float(bands["middle"].iloc[-1]))
    result.setdefault("bollinger", result["bollinger_value"])


def _apply_cci(result: dict[str, Any], price_df: pd.DataFrame, config: TADashboardConfig) -> None:
    """Emit ``cci_history`` (Series) — needs OHLC high/low (degrades to close-only)."""
    cci = _cci_series(price_df, int(config.indicator_period))
    if cci.empty:
        return
    cci = _time_indexed(cci, price_df).dropna()
    if cci.empty:
        return
    result["cci_history"] = cci.rename("cci")
    result.setdefault("cci_value", float(cci.iloc[-1]))
    result.setdefault("cci", result["cci_value"])


def _apply_stochastic(result: dict[str, Any], price_df: pd.DataFrame, config: TADashboardConfig) -> None:
    """Emit ``stochastic_data`` (k/d frame) — needs OHLC high/low (degrades to close-only).

    ``get_stochastic_config`` encodes ``secondary_periods=[slow_k, slow_d]``. The
    framework's ``StochasticCalculator`` is a *fast* stochastic — raw %K over
    ``k_period`` then ``%D = SMA(%K, slow_d)``, with no ``slow_k`` "slowing" of
    %K. We mirror that exactly (``k_period = indicator_period``, ``d_period =
    slow_d``) so the chart matches the strategy's own signals; ``slow_k`` is
    intentionally not applied — wiring it would diverge the dashboard from the
    calculator the strategy actually runs.
    """
    periods = list(config.secondary_periods or [])
    d_period = int(periods[1]) if len(periods) > 1 else 3
    stoch = _stochastic_series(price_df, int(config.indicator_period), d_period)
    if stoch.empty:
        return
    stoch = _time_indexed(stoch, price_df).dropna()
    if stoch.empty:
        return
    result["stochastic_data"] = stoch
    result.setdefault("stochastic_value", float(stoch["k"].iloc[-1]))
    result.setdefault("stochastic", result["stochastic_value"])


def _apply_atr(result: dict[str, Any], price_df: pd.DataFrame, config: TADashboardConfig) -> None:
    """Emit ``atr_history`` (Series) — needs OHLC high/low (degrades to close-only)."""
    atr = _atr_series(price_df, int(config.indicator_period))
    if atr.empty:
        return
    atr = _time_indexed(atr, price_df).dropna()
    if atr.empty:
        return
    result["atr_history"] = atr.rename("atr")
    result.setdefault("atr_value", float(atr.iloc[-1]))
    result.setdefault("atr", result["atr_value"])


def _apply_adx(result: dict[str, Any], price_df: pd.DataFrame, config: TADashboardConfig) -> None:
    """Emit ``adx_data`` (adx/plus_di/minus_di frame) — needs OHLC high/low (degrades to close-only)."""
    adx = _adx_series(price_df, int(config.indicator_period))
    if adx.empty:
        return
    adx = _time_indexed(adx, price_df).dropna(subset=["adx"])
    if adx.empty:
        return
    result["adx_data"] = adx
    result.setdefault("adx_value", float(adx["adx"].iloc[-1]))
    result.setdefault("adx", result["adx_value"])


# Indicator key (``config.indicator_name.lower()``) → series-compute handler.
# Each handler populates ``result`` in place with a time-indexed series/frame
# plus a latest scalar; the dispatcher wraps them in a single soft-fail guard.
_INDICATOR_HANDLERS: dict[str, Callable[[dict[str, Any], pd.DataFrame, TADashboardConfig], None]] = {
    "rsi": _apply_rsi,
    "macd": _apply_macd,
    "bollinger": _apply_bollinger,
    "cci": _apply_cci,
    "stochastic": _apply_stochastic,
    "atr": _apply_atr,
    "adx": _apply_adx,
}


def _apply_indicator_series(result: dict[str, Any], price_df: pd.DataFrame, config: TADashboardConfig) -> None:
    """Compute the configured indicator's chart series into ``result`` in place.

    No-op when the caller already supplied the series, when there's no price
    data, or when the indicator isn't one we compute client-side. Dispatches to
    a per-indicator handler (see :data:`_INDICATOR_HANDLERS`) which emits a
    time-indexed ``<key>_history`` Series or ``<key>_data`` frame plus a latest
    scalar. Degrades silently on error so the dashboard still renders price +
    signals.
    """
    indicator_key = config.indicator_name.lower()
    if f"{indicator_key}_history" in result or f"{indicator_key}_data" in result or price_df.empty:
        return
    handler = _INDICATOR_HANDLERS.get(indicator_key)
    if handler is None:
        return
    try:
        handler(result, price_df, config)
    except Exception:  # noqa: BLE001
        logger.debug("%s indicator series computation failed", indicator_key, exc_info=True)


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

    # Indicator series — computed client-side from price history. The primary
    # indicator plus any extras (multi-signal layout, VIB-4897) all share the
    # same price_df; each handler is a no-op if its series is already present.
    if price_df is not None:
        _apply_indicator_series(result, price_df, config)
        # Extras may repeat an indicator type (e.g. dual RSI with different
        # periods). A bare-key apply would collide — _apply_indicator_series
        # early-returns when ``<key>_*`` already exists — so same-type repeats
        # are computed in isolation and stored under a disambiguated slot key
        # (rsi_2, rsi_3, …). VIB-4897.
        for slot, cfg in _multi_indicator_slots(config)[1:]:
            base = cfg.indicator_name.lower()
            if slot == base:
                _apply_indicator_series(result, price_df, cfg)
            elif not any(k == slot or k.startswith(f"{slot}_") for k in result):
                tmp: dict[str, Any] = {}
                _apply_indicator_series(tmp, price_df, cfg)
                for k, v in tmp.items():
                    result[k.replace(base, slot, 1)] = v

    # Buy / sell signals from the trade tape (SWAP rows for this pair).
    if "buy_signals" not in result or "sell_signals" not in result:
        rows = _trade_tape_rows(api_client)
        buys, sells = _trade_rows_to_signals(rows, base_token, quote_token)
        result.setdefault("buy_signals", buys)
        result.setdefault("sell_signals", sells)

    if "strategy_start_time" not in result:
        start_time = _strategy_start_time(api_client)
        if start_time is not None:
            result["strategy_start_time"] = start_time

    return result


def render_ta_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    session_state: dict[str, Any],
    config: TADashboardConfig,
) -> None:
    """Render a technical analysis dashboard using the provided configuration.

    Single-position template. Renders one indicator panel by default; pass a
    :func:`multi_ta_config` (``config.extra_indicators`` set) to stack one panel
    per indicator under a shared price chart (multi-signal, VIB-4897). Bakes in
    the 3 accounting sections (PnL → chart content → Cost Stack → Trade Tape).
    For multi-*position* layouts, compose a custom dashboard from the section
    helpers directly rather than parameterizing this template.

    Args:
        deployment_id: The deployment identifier
        strategy_config: Strategy configuration dictionary
        session_state: Current session state with indicator values
        config: TADashboardConfig for this dashboard
    """
    if config.extra_indicators:
        names = " + ".join([config.indicator_name, *(c.indicator_name for c in config.extra_indicators)])
        st.title(f"{names} Strategy Dashboard")
    else:
        st.title(f"{config.indicator_name} Strategy Dashboard")

    # Extract config overrides
    base_token = strategy_config.get("base_token", config.base_token)
    quote_token = strategy_config.get("quote_token", config.quote_token)
    chain = strategy_config.get("chain", config.chain)
    protocol = strategy_config.get("protocol", config.protocol)
    period = strategy_config.get(f"{config.indicator_name.lower()}_period", config.indicator_period)

    st.markdown(f"**Deployment ID:** `{deployment_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown(f"**Chain:** {chain} | **Protocol:** {protocol}")

    # Eyeball — am I making or losing money?
    render_pnl_section(deployment_id)

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
    render_cost_stack_section(deployment_id)
    render_trade_tape_section(deployment_id)


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
    strategy_start_time = session_state.get("strategy_start_time")

    # Multi-signal layout (VIB-4897): render price once, then one dedicated
    # panel per configured indicator. The single-indicator path below is
    # unchanged.
    if config.extra_indicators:
        _render_multi_indicator_charts(session_state, config, price_df, buy_df, sell_df)
        return

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

        if strategy_start_time is not None:
            start_time = pd.to_datetime(strategy_start_time, utc=True, errors="coerce")
            if not pd.isna(start_time):
                fig.add_trace(
                    go.Scatter(
                        x=[start_time, start_time],
                        y=[price_df["price"].min(), price_df["price"].max()],
                        mode="lines",
                        name="Start",
                        line={"color": colors.neutral, "width": 1, "dash": "dot"},
                    ),
                    row=1,
                    col=1,
                )
                fig.add_trace(
                    go.Scatter(
                        x=[start_time, start_time],
                        y=[0, 100],
                        mode="lines",
                        name="Start",
                        line={"color": colors.neutral, "width": 1, "dash": "dot"},
                        showlegend=False,
                    ),
                    row=2,
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

    elif _has_indicator_data(indicator_data) and config.indicator_name.upper() in _DEDICATED_RENDERERS:
        # MACD/Bollinger/CCI/Stochastic/ATR/ADX each need a purpose-built chart
        # (multi-line bands, oscillator zones, …), not the generic single line.
        _DEDICATED_RENDERERS[config.indicator_name.upper()](price_df, buy_df, sell_df, indicator_data, config)

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


def _render_price_signals(
    price_df: pd.DataFrame,
    buy_df: pd.DataFrame | None,
    sell_df: pd.DataFrame | None,
) -> None:
    """Render the shared price-with-buy/sell-signals chart."""
    fig_price = plot_price_with_signals(
        price_data=price_df,
        buy_signals=buy_df,
        sell_signals=sell_df,
        title="Price with Buy/Sell Signals",
    )
    st.plotly_chart(fig_price, use_container_width=True)


def _as_indicator_series(data: Any) -> pd.Series | None:
    """Coerce a single-line indicator payload (Series or list of pairs) to a Series."""
    if isinstance(data, pd.Series):
        return data if not data.empty else None
    if isinstance(data, list) and data:
        times = [item[0] for item in data]
        values = [item[1] for item in data]
        return pd.Series(values, index=pd.to_datetime(times))
    return None


def _indicator_present(data: Any) -> bool:
    """Module-level twin of the ``_has_indicator_data`` closure, for the panel path."""
    if data is None:
        return False
    if isinstance(data, pd.Series | pd.DataFrame):
        return not data.empty
    if isinstance(data, list):
        return len(data) > 0
    return bool(data)


# --- Standalone indicator panels (the indicator chart only, no price line) ---
# Shared by the single-indicator dedicated renderers (price + panel) and the
# multi-signal layout (price once, then one panel per indicator).


def _panel_rsi(price_df: pd.DataFrame, data: Any, config: TADashboardConfig) -> None:
    series = _as_indicator_series(data)
    if series is None:
        return
    fig = plot_rsi_indicator(
        rsi_data=series,
        time_index=series.index,
        overbought=config.upper_threshold if config.upper_threshold is not None else 70,
        oversold=config.lower_threshold if config.lower_threshold is not None else 30,
    )
    st.plotly_chart(fig, use_container_width=True)


def _panel_macd(price_df: pd.DataFrame, data: Any, config: TADashboardConfig) -> None:
    if not (isinstance(data, pd.DataFrame) and not data.empty):
        return
    fig = plot_macd_indicator(
        macd=data["macd"], macd_signal=data["signal"], macd_hist=data["histogram"], time_index=data.index
    )
    st.plotly_chart(fig, use_container_width=True)


def _panel_bollinger(price_df: pd.DataFrame, data: Any, config: TADashboardConfig) -> None:
    if not (isinstance(data, pd.DataFrame) and not data.empty):
        return
    price_aligned = price_df.set_index("time")["price"].reindex(data.index)
    fig = plot_bollinger_bands(
        price_data=price_aligned,
        upper_band=data["upper"],
        middle_band=data["middle"],
        lower_band=data["lower"],
        time_index=data.index,
    )
    st.plotly_chart(fig, use_container_width=True)


def _panel_cci(price_df: pd.DataFrame, data: Any, config: TADashboardConfig) -> None:
    series = _as_indicator_series(data)
    if series is None:
        return
    fig = plot_cci_indicator(
        cci_data=series,
        time_index=series.index,
        overbought=config.upper_threshold if config.upper_threshold is not None else 100,
        oversold=config.lower_threshold if config.lower_threshold is not None else -100,
    )
    st.plotly_chart(fig, use_container_width=True)


def _panel_stochastic(price_df: pd.DataFrame, data: Any, config: TADashboardConfig) -> None:
    if not (isinstance(data, pd.DataFrame) and not data.empty):
        return
    fig = plot_stochastic_indicator(
        stoch_k=data["k"],
        stoch_d=data["d"],
        time_index=data.index,
        overbought=config.upper_threshold if config.upper_threshold is not None else 80,
        oversold=config.lower_threshold if config.lower_threshold is not None else 20,
    )
    st.plotly_chart(fig, use_container_width=True)


def _panel_atr(price_df: pd.DataFrame, data: Any, config: TADashboardConfig) -> None:
    series = _as_indicator_series(data)
    if series is None:
        return
    fig = plot_atr_indicator(atr_data=series, time_index=series.index)
    st.plotly_chart(fig, use_container_width=True)


def _panel_adx(price_df: pd.DataFrame, data: Any, config: TADashboardConfig) -> None:
    if not (isinstance(data, pd.DataFrame) and not data.empty):
        return
    fig = plot_adx_indicator(
        adx_data=data["adx"],
        plus_di=data["plus_di"],
        minus_di=data["minus_di"],
        time_index=data.index,
        trend_threshold=config.lower_threshold if config.lower_threshold is not None else 25,
    )
    st.plotly_chart(fig, use_container_width=True)


def _panel_generic(price_df: pd.DataFrame, data: Any, config: TADashboardConfig) -> None:
    """Fallback single-line panel for an indicator with no dedicated chart."""
    series = _as_indicator_series(data)
    if series is None:
        return
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=series.index,
            y=series.values,
            mode="lines",
            name=config.indicator_name,
            line={"color": "#1f77b4", "width": 2},
        )
    )
    fig.update_layout(
        title=f"{config.indicator_name} Indicator",
        xaxis_title="Time",
        yaxis_title=config.indicator_name,
        height=300,
    )
    st.plotly_chart(fig, use_container_width=True)


# Uppercased ``config.indicator_name`` → standalone panel builder.
_INDICATOR_PANELS: dict[str, Callable[[pd.DataFrame, Any, TADashboardConfig], None]] = {
    "RSI": _panel_rsi,
    "MACD": _panel_macd,
    "BOLLINGER": _panel_bollinger,
    "CCI": _panel_cci,
    "STOCHASTIC": _panel_stochastic,
    "ATR": _panel_atr,
    "ADX": _panel_adx,
}


def _multi_indicator_slots(config: TADashboardConfig) -> list[tuple[str, TADashboardConfig]]:
    """Map the primary + extra indicators to unique session-state slot keys.

    The first occurrence of an indicator type keeps its bare key (``rsi``) so the
    single-indicator path and existing session-state keys are unchanged; a second
    indicator of the same type becomes ``rsi_2``, a third ``rsi_3``, … so dual
    same-type configs (e.g. two RSIs with different periods) never collide.
    """
    counts: dict[str, int] = {}
    slots: list[tuple[str, TADashboardConfig]] = []
    for cfg in [config, *config.extra_indicators]:
        base = cfg.indicator_name.lower()
        counts[base] = counts.get(base, 0) + 1
        slots.append((base if counts[base] == 1 else f"{base}_{counts[base]}", cfg))
    return slots


def _render_multi_indicator_charts(
    session_state: dict[str, Any],
    config: TADashboardConfig,
    price_df: pd.DataFrame,
    buy_df: pd.DataFrame | None,
    sell_df: pd.DataFrame | None,
) -> None:
    """Stacked multi-signal layout: price+signals once, then one panel per indicator.

    Iterates the primary config plus ``config.extra_indicators``; each panel
    reuses the same ``almanak.framework.dashboard.plots`` primitive as the
    single-indicator path, so all panels share one time axis. An indicator with
    no computed series is announced (not silently dropped).
    """
    _render_price_signals(price_df, buy_df, sell_df)
    for slot, cfg in _multi_indicator_slots(config):
        # ``slot`` matches the disambiguated key prepare_ta_session_state wrote,
        # so a second same-type indicator (rsi_2) reads its own series.
        data = session_state.get(f"{slot}_data")
        if data is None:
            data = session_state.get(f"{slot}_history")
        st.markdown(f"**{cfg.indicator_name}**")
        if not _indicator_present(data):
            st.caption(f"{cfg.indicator_name}: no indicator data available")
            continue
        panel = _INDICATOR_PANELS.get(cfg.indicator_name.upper(), _panel_generic)
        panel(price_df, data, cfg)


def _render_indicator_with_price(
    price_df: pd.DataFrame,
    buy_df: pd.DataFrame | None,
    sell_df: pd.DataFrame | None,
    data: Any,
    config: TADashboardConfig,
) -> None:
    """Single-indicator dedicated render: price+signals chart, then the indicator panel.

    Shares the per-indicator panel builders with the multi-signal layout so the
    two paths can never drift in how a given indicator is drawn.
    """
    _render_price_signals(price_df, buy_df, sell_df)
    panel = _INDICATOR_PANELS.get(config.indicator_name.upper(), _panel_generic)
    panel(price_df, data, config)


# Uppercased ``config.indicator_name`` → dedicated chart renderer. RSI is handled
# inline in ``_render_charts_section`` (combined price+RSI subplot); every entry
# here renders a price+signals chart plus its dedicated indicator panel.
_DEDICATED_RENDERERS: dict[
    str, Callable[[pd.DataFrame, pd.DataFrame | None, pd.DataFrame | None, Any, TADashboardConfig], None]
] = dict.fromkeys(("MACD", "BOLLINGER", "CCI", "STOCHASTIC", "ATR", "ADX"), _render_indicator_with_price)


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
    )


def _macd_signal_fn(session_state: dict[str, Any]) -> str:
    """Bullish/bearish/neutral text from the MACD histogram (MACD vs Signal)."""
    macd = float(session_state.get("macd_value", session_state.get("macd", 0)) or 0)
    signal = float(session_state.get("signal_line", 0) or 0)
    hist = float(session_state.get("histogram", macd - signal) or 0)
    if hist > 0:
        return f"BULLISH: MACD ({macd:+.4f}) above Signal ({signal:+.4f})"
    if hist < 0:
        return f"BEARISH: MACD ({macd:+.4f}) below Signal ({signal:+.4f})"
    return "NEUTRAL: MACD at Signal line"


def get_macd_config(fast: int = 12, slow: int = 26, signal: int = 9) -> TADashboardConfig:
    """Get pre-configured MACD dashboard config."""
    return TADashboardConfig(
        indicator_name="MACD",
        indicator_period=fast,
        secondary_periods=[slow, signal],
        custom_signal_fn=_macd_signal_fn,
        signal_type="momentum",
        value_format="{:+.2f}",
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
    )


def get_atr_config(period: int = 14) -> TADashboardConfig:
    """Get pre-configured ATR dashboard config."""
    return TADashboardConfig(
        indicator_name="ATR",
        indicator_period=period,
        signal_type="momentum",
        value_format="${:.2f}",
    )


def get_adx_config(period: int = 14, trend_threshold: float = 25) -> TADashboardConfig:
    """Get pre-configured ADX dashboard config."""
    return TADashboardConfig(
        indicator_name="ADX",
        indicator_period=period,
        lower_threshold=trend_threshold,
        signal_type="momentum",
    )


def get_bollinger_config(period: int = 20, std_dev: float = 2.0) -> TADashboardConfig:
    """Get pre-configured Bollinger Bands dashboard config."""
    return TADashboardConfig(
        indicator_name="Bollinger",
        indicator_period=period,
        secondary_periods=[int(std_dev * 10)],  # Encode std_dev
        signal_type="reversion",
        value_format="${:.2f}",
    )
