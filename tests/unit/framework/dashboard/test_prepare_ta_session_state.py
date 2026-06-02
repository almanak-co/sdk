"""Unit tests for prepare_ta_session_state and its helpers.

Covers the contract the chart subplot in ``render_ta_dashboard`` depends
on: caller-provided keys are preserved, OHLCV is fetched once and shaped
correctly, RSI is computed as a pandas Series with a DatetimeIndex, and
trade-tape rows are split into buy/sell markers by pair direction.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from almanak.framework.dashboard.templates import (
    get_macd_config,
    get_rsi_config,
    prepare_ta_session_state,
)
from almanak.framework.dashboard.templates._ohlcv_window import (
    DEFAULT_CANDLE_LIMIT,
    normalize_timeframe,
    ohlcv_limit_for_timeframe,
)
from almanak.framework.dashboard.templates.ta_dashboard import (
    _RSI_DECISION_BUFFER,
    _ema_sma_seeded,
    _macd_series_from_closes,
    _macd_signal_fn,
    _ohlcv_to_price_history,
    _rsi_series_from_closes,
    _trade_rows_to_signals,
    _wilder_rsi_window,
)


def _ohlcv_payload(prices: list[float]) -> list[dict[str, Any]]:
    """Build a minimally-shaped OHLCV payload — only `timestamp` + `close` matter."""
    return [
        {
            "timestamp": f"2026-05-12T{h:02d}:00:00Z",
            "open": str(p - 1),
            "high": str(p + 2),
            "low": str(p - 2),
            "close": str(p),
            "volume": "1",
        }
        for h, p in enumerate(prices)
    ]


class _FakeClient:
    """Duck-typed DashboardAPIClient stand-in."""

    def __init__(
        self,
        ohlcv: list[dict[str, Any]] | None = None,
        tape_rows: list[dict[str, Any]] | None = None,
        timeline: list[dict[str, Any]] | None = None,
        token_balances: list[dict[str, Any]] | None = None,
        price: float | None = None,
    ) -> None:
        self._ohlcv = ohlcv or []
        self._tape_rows = tape_rows or []
        self._timeline = timeline or []
        self._token_balances = token_balances or []
        self._price = price
        self.ohlcv_call_count = 0
        self.get_price_call_count = 0

    def get_ohlcv(self, **_: Any) -> list[dict[str, Any]]:
        self.ohlcv_call_count += 1
        return self._ohlcv

    def get_trade_tape(self) -> dict[str, Any]:
        return {"rows": self._tape_rows, "has_more": False}

    def get_timeline(self, **_: Any) -> list[dict[str, Any]]:
        return self._timeline

    def get_position(self) -> dict[str, Any]:
        return {"token_balances": self._token_balances}

    def get_price(self, token: str, quote: str = "USD", chain: str | None = None) -> float | None:
        self.get_price_call_count += 1
        return self._price


# ----------------------------------------------------------------------
# _rsi_series_from_closes
# ----------------------------------------------------------------------


def test_rsi_series_returns_nans_until_warmup():
    closes = pd.Series([100.0, 101.0, 100.5, 101.5])
    out = _rsi_series_from_closes(closes, period=14)
    # Below the required points → all NaN, same length as input.
    assert len(out) == len(closes)
    assert out.isna().all()


def test_rsi_series_no_losses_clamps_to_100():
    # Monotonically rising prices → avg_loss = 0 → RSI = 100.
    closes = pd.Series([100.0 + i for i in range(40)])
    out = _rsi_series_from_closes(closes, period=14)
    # After warmup, every defined value should be 100.
    defined = out.dropna()
    assert not defined.empty
    assert (defined == 100.0).all()


def test_rsi_series_with_oscillation_stays_in_0_to_100():
    closes = pd.Series([100.0 + (10 if i % 4 < 2 else -10) for i in range(60)])
    out = _rsi_series_from_closes(closes, period=14).dropna()
    assert not out.empty
    assert out.min() >= 0.0
    assert out.max() <= 100.0


# ----------------------------------------------------------------------
# _ohlcv_to_price_history
# ----------------------------------------------------------------------


def test_ohlcv_to_price_history_normalises_to_time_price():
    payload = _ohlcv_payload([2300.0, 2305.0, 2310.0])
    df = _ohlcv_to_price_history(payload)
    # close → price, plus high/low carried through for the OHLC-range indicators.
    assert list(df.columns) == ["time", "price", "high", "low"]
    assert len(df) == 3
    assert pd.api.types.is_datetime64_any_dtype(df["time"])
    assert df["price"].tolist() == [2300.0, 2305.0, 2310.0]
    # _ohlcv_payload sets high = close + 2, low = close - 2.
    assert df["high"].tolist() == [2302.0, 2307.0, 2312.0]
    assert df["low"].tolist() == [2298.0, 2303.0, 2308.0]


def test_ohlcv_to_price_history_close_only_payload_drops_high_low():
    # A payload without high/low (e.g. a caller-supplied close series) keeps
    # working — only time/price are produced.
    payload = [{"timestamp": "2026-05-12T00:00:00Z", "close": "100"}]
    df = _ohlcv_to_price_history(payload)
    assert list(df.columns) == ["time", "price"]
    assert df["price"].tolist() == [100.0]


def test_ohlcv_to_price_history_handles_empty():
    assert _ohlcv_to_price_history([]).empty


def test_ohlcv_to_price_history_drops_unparseable_rows():
    payload = _ohlcv_payload([2300.0, 2305.0])
    payload.append({"timestamp": "not-a-time", "close": "garbage"})
    df = _ohlcv_to_price_history(payload)
    assert len(df) == 2
    assert df["price"].tolist() == [2300.0, 2305.0]


# ----------------------------------------------------------------------
# _trade_rows_to_signals
# ----------------------------------------------------------------------


def test_trade_rows_split_by_direction():
    rows = [
        {
            "timestamp": "2026-05-12T01:00:00Z",
            "intent_type": "SWAP",
            "token_in": "USDC",
            "amount_in": "3",
            "token_out": "WETH",
            "amount_out": "0.0015",
            "effective_price": "0.0005",
        },
        {
            "timestamp": "2026-05-12T05:00:00Z",
            "intent_type": "SWAP",
            "token_in": "WETH",
            "amount_in": "0.00125",
            "token_out": "USDC",
            "amount_out": "3",
            "effective_price": "2400",
        },
    ]
    buys, sells = _trade_rows_to_signals(rows, "WETH", "USDC")
    assert len(buys) == 1 and buys["price"].iloc[0] == 2000.0
    assert len(sells) == 1 and sells["price"].iloc[0] == 2400.0


def test_trade_rows_buy_marker_converts_output_per_input_price_to_chart_price():
    rows = [
        {
            "timestamp": "2026-05-20T16:23:23Z",
            "intent_type": "SWAP",
            "token_in": "USDC",
            "amount_in": "3",
            "token_out": "WETH",
            "amount_out": "0.001407624092196586",
            "effective_price": "0.0004692080307321953333333333333",
        }
    ]

    buys, sells = _trade_rows_to_signals(rows, "WETH", "USDC")

    assert sells.empty
    assert len(buys) == 1
    assert buys["price"].iloc[0] == pytest.approx(2131.251, rel=1e-6)


def test_trade_rows_ignore_non_swap_and_off_pair():
    rows = [
        {
            "timestamp": "2026-05-12T01:00:00Z",
            "intent_type": "LP_OPEN",
            "token_in": "USDC",
            "token_out": "WETH",
            "effective_price": "0",
        },
        {
            "timestamp": "2026-05-12T02:00:00Z",
            "intent_type": "SWAP",
            "token_in": "USDC",
            "token_out": "ARB",
            "effective_price": "1",
        },
    ]
    buys, sells = _trade_rows_to_signals(rows, "WETH", "USDC")
    assert buys.empty and sells.empty


def test_trade_rows_empty_input():
    buys, sells = _trade_rows_to_signals([], "WETH", "USDC")
    assert buys.empty and sells.empty


# ----------------------------------------------------------------------
# prepare_ta_session_state
# ----------------------------------------------------------------------


def _config():
    config = get_rsi_config(period=14, overbought=70, oversold=30)
    config.base_token = "WETH"
    config.quote_token = "USDC"
    config.chain = "arbitrum"
    return config


def test_prepare_populates_chart_keys_from_api_client():
    prices = [2300.0 + (15 if i % 6 < 3 else -12) for i in range(60)]
    client = _FakeClient(
        ohlcv=_ohlcv_payload(prices),
        tape_rows=[
            {
                "timestamp": "2026-05-12T05:00:00Z",
                "intent_type": "SWAP",
                "token_in": "USDC",
                "token_out": "WETH",
                "effective_price": "2200",
            }
        ],
        timeline=[
            {
                "timestamp": "2026-05-12T04:00:00Z",
                "event_type": "STRATEGY_STARTED",
                "description": "Strategy started",
            }
        ],
    )
    out = prepare_ta_session_state(client, session_state={}, config=_config())
    # Chart inputs the renderer's _render_charts_section depends on.
    assert isinstance(out["price_history"], pd.DataFrame) and not out["price_history"].empty
    assert isinstance(out["rsi_history"], pd.Series)
    assert isinstance(out["rsi_history"].index, pd.DatetimeIndex)
    assert not out["rsi_history"].empty
    # Metric row needs rsi_value too.
    assert "rsi_value" in out
    assert 0.0 <= out["rsi_value"] <= 100.0
    # Buy/sell parsing
    assert len(out["buy_signals"]) == 1
    assert out["sell_signals"].empty
    assert out["strategy_start_time"] == pd.Timestamp("2026-05-12T04:00:00Z")


def test_prepare_populates_position_balances_from_snapshot():
    # Regression: the Current Position section read base_balance/quote_balance
    # that nothing populated, so every TA dashboard showed 0.0000 / $0.00 /
    # $0.00 even with funds in the wallet. prepare_ta_session_state must load
    # them from the gateway position snapshot, mirroring the LP template.
    client = _FakeClient(
        token_balances=[
            {"symbol": "WETH", "balance": "0.0017", "value_usd": "4.33"},
            {"symbol": "USDC", "balance": "0.77", "value_usd": "0.77"},
        ],
    )
    out = prepare_ta_session_state(client, session_state={}, config=_config())
    assert out["base_balance"] == "0.0017"
    assert out["quote_balance"] == "0.77"
    # base_price is derived from the snapshot's own valuation (value_usd / balance),
    # so the rendered Total matches the strategy's view — no extra price lookup.
    assert float(out["base_price"]) == pytest.approx(4.33 / 0.0017)
    assert client.get_price_call_count == 0


def test_prepare_falls_back_to_live_price_when_snapshot_unvalued():
    # Balance present but value_usd missing/zero → derive price via get_price.
    client = _FakeClient(
        token_balances=[{"symbol": "WETH", "balance": "0.0017", "value_usd": "0"}],
        price=2500.0,
    )
    out = prepare_ta_session_state(client, session_state={}, config=_config())
    assert out["base_balance"] == "0.0017"
    assert float(out["base_price"]) == pytest.approx(2500.0)
    assert client.get_price_call_count == 1


def test_prepare_position_balances_coerce_none_to_zero_string():
    # A snapshot may carry an explicit None for an unmeasured field. Storing
    # None would make _render_position do Decimal(str(None)) -> InvalidOperation
    # and trip the dashboard's error boundary. Coerce to "0" instead so the
    # section degrades to zeros, as intended.
    client = _FakeClient(
        token_balances=[
            {"symbol": "WETH", "balance": None, "value_usd": None},
            {"symbol": "USDC", "balance": None, "value_usd": None},
        ],
        price=None,
    )
    out = prepare_ta_session_state(client, session_state={}, config=_config())
    assert out["base_balance"] == "0"
    assert out["quote_balance"] == "0"
    # No usable price anywhere → key stays absent, _render_position uses its default.
    assert "base_price" not in out
    # And the stored values are safe to feed straight into Decimal(str(...)).
    from decimal import Decimal

    assert Decimal(str(out["base_balance"])) == Decimal("0")


def test_prepare_position_balances_tolerate_non_dict_entries():
    # Defensive: a malformed snapshot entry that isn't a dict must not crash
    # the comprehension — it's skipped, and the well-formed entry still loads.
    client = _FakeClient(
        token_balances=[
            "garbage",
            {"symbol": "WETH", "balance": "2.0", "value_usd": "5000"},
        ],
    )
    out = prepare_ta_session_state(client, session_state={}, config=_config())
    assert out["base_balance"] == "2.0"


def test_prepare_preserves_caller_supplied_balances():
    client = _FakeClient(
        token_balances=[{"symbol": "WETH", "balance": "9.9", "value_usd": "99"}],
        price=1234.0,
    )
    out = prepare_ta_session_state(
        client,
        session_state={"base_balance": "1.5", "quote_balance": "10", "base_price": "2000"},
        config=_config(),
    )
    assert out["base_balance"] == "1.5"
    assert out["quote_balance"] == "10"
    assert out["base_price"] == "2000"
    # All three supplied → no snapshot price lookup needed.
    assert client.get_price_call_count == 0


def test_prepare_position_balances_absent_when_snapshot_empty():
    # Empty snapshot (or a client whose get_position raises) must not crash;
    # the section falls back to its zero defaults (keys simply absent, so
    # _render_position uses its "0" fallbacks).
    client = _FakeClient(ohlcv=_ohlcv_payload([100.0] * 60))  # no token_balances
    out = prepare_ta_session_state(client, session_state={}, config=_config())
    assert "base_balance" not in out
    assert "quote_balance" not in out


def test_prepare_prefers_strategy_started_for_start_marker():
    client = _FakeClient(
        timeline=[
            {"timestamp": "2026-05-12T05:00:00Z", "event_type": "STATE_CHANGE"},
            {"timestamp": "2026-05-12T04:30:00Z", "event_type": "STRATEGY_STARTED"},
            {"timestamp": "2026-05-12T04:00:00Z", "event_type": "STATE_CHANGE"},
        ],
    )

    out = prepare_ta_session_state(client, session_state={}, config=_config())

    assert out["strategy_start_time"] == pd.Timestamp("2026-05-12T04:30:00Z")


def test_prepare_preserves_caller_keys():
    caller_price_history = pd.DataFrame([{"time": pd.Timestamp("2020-01-01", tz="UTC"), "price": 99.0}])
    client = _FakeClient(ohlcv=_ohlcv_payload([100.0] * 60))
    state = {
        "price_history": caller_price_history,
        "rsi_value": 42.0,
        "buy_signals": pd.DataFrame([{"time": pd.Timestamp("2021-01-01", tz="UTC"), "price": 1.0}]),
    }
    out = prepare_ta_session_state(client, session_state=state, config=_config())
    # Caller's data wasn't overwritten.
    assert out["price_history"].iloc[0]["price"] == 99.0
    assert out["rsi_value"] == 42.0
    assert out["buy_signals"].iloc[0]["price"] == 1.0
    # And get_ohlcv was NOT called (we already had price_history).
    assert client.ohlcv_call_count == 0


def test_prepare_degrades_gracefully_when_api_fails():
    class ExplodingClient:
        def get_ohlcv(self, **_: Any) -> list[dict[str, Any]]:
            raise RuntimeError("gateway is down")

        def get_trade_tape(self) -> Any:
            raise RuntimeError("still down")

    out = prepare_ta_session_state(ExplodingClient(), session_state={}, config=_config())
    # No crash, no chart keys populated — caller renders the "data unavailable" branch.
    assert "price_history" not in out
    assert "rsi_history" not in out
    # buy/sell are set to empty frames so downstream isinstance checks pass.
    assert out["buy_signals"].empty
    assert out["sell_signals"].empty


def test_prepare_handles_empty_ohlcv():
    client = _FakeClient(ohlcv=[], tape_rows=[])
    out = prepare_ta_session_state(client, session_state={}, config=_config())
    assert "price_history" not in out  # never populated for empty payload
    assert out["buy_signals"].empty
    assert out["sell_signals"].empty


def test_prepare_with_none_config_returns_passthrough():
    client = _FakeClient(ohlcv=_ohlcv_payload([100.0] * 30))
    out = prepare_ta_session_state(client, session_state={"foo": "bar"}, config=None)
    assert out == {"foo": "bar"}


@pytest.mark.parametrize(
    "tape",
    [None, {}, {"rows": None}, {"rows": []}],
)
def test_prepare_tolerates_tape_shapes(tape):
    class TapeClient:
        def get_ohlcv(self, **_: Any) -> list[dict[str, Any]]:
            return _ohlcv_payload([100.0 + i for i in range(30)])

        def get_trade_tape(self) -> Any:
            return tape

    out = prepare_ta_session_state(TapeClient(), session_state={}, config=_config())
    assert out["buy_signals"].empty
    assert out["sell_signals"].empty


def test_render_charts_section_tolerates_dataframe_signals():
    """Regression: ``if buy_signals:`` on a DataFrame raises ValueError.

    Before this guard, passing buy/sell DataFrames into
    ``_render_charts_section`` blew up with
    ``ValueError: The truth value of a DataFrame is ambiguous`` — which
    is the failure path that surfaced when ``prepare_ta_session_state``
    started populating those keys as frames instead of lists.
    """
    from almanak.framework.dashboard.templates.ta_dashboard import (
        _render_charts_section,
    )

    closes = [100.0 + (5 if i % 4 < 2 else -5) for i in range(40)]
    times = pd.date_range("2026-05-12", periods=40, freq="1h", tz="UTC")
    price_df = pd.DataFrame({"time": times, "price": closes})
    buy_df = pd.DataFrame([{"time": times[10], "price": closes[10]}])
    sell_df = pd.DataFrame([{"time": times[25], "price": closes[25]}])
    rsi_history = pd.Series(
        _rsi_series_from_closes(price_df["price"], 14).values,
        index=times,
        name="rsi",
    ).dropna()

    session_state = {
        "price_history": price_df,
        "rsi_history": rsi_history,
        "buy_signals": buy_df,
        "sell_signals": sell_df,
        "strategy_start_time": times[0],
    }
    # Must not raise — Streamlit calls become no-ops without a script
    # context. The point of the test is the type-coercion path.
    _render_charts_section(session_state, {}, _config(), period=14)


# ----------------------------------------------------------------------
# MACD: _ema_sma_seeded / _macd_series_from_closes / _macd_signal_fn
# ----------------------------------------------------------------------


def test_ema_sma_seeded_matches_canonical_formula():
    # Mirror MACDCalculator._calculate_ema: NaN warmup, SMA seed, then EMA.
    closes = pd.Series([float(i) for i in range(1, 11)])  # 1..10
    out = _ema_sma_seeded(closes, period=3)
    assert out.iloc[:2].isna().all()
    # Seed at index 2 == SMA of first 3 (1,2,3) == 2.0
    assert out.iloc[2] == pytest.approx(2.0)
    # Next: 4 * (2/4) + 2 * (1 - 2/4) = 2 + 1 = 3.0
    assert out.iloc[3] == pytest.approx(3.0)


def test_ema_sma_seeded_all_nan_below_warmup():
    out = _ema_sma_seeded(pd.Series([1.0, 2.0]), period=5)
    assert len(out) == 2
    assert out.isna().all()


def test_macd_series_empty_below_warmup():
    # Needs slow + signal (26 + 9 = 35) rows; fewer → empty frame.
    out = _macd_series_from_closes(pd.Series([100.0] * 20), fast=12, slow=26, signal=9)
    assert list(out.columns) == ["macd", "signal", "histogram"]
    assert out.empty


def test_macd_series_histogram_is_macd_minus_signal():
    closes = pd.Series([100.0 + i * 0.5 for i in range(80)])
    out = _macd_series_from_closes(closes, fast=12, slow=26, signal=9)
    defined = out.dropna()
    assert not defined.empty
    assert (defined["histogram"] - (defined["macd"] - defined["signal"])).abs().max() < 1e-9


def test_macd_series_rising_prices_are_bullish():
    # An *accelerating* uptrend keeps the MACD line rising, so it stays above
    # its own (lagging) signal EMA → MACD > 0 and a positive tail histogram.
    # (A perfectly linear ramp would flatten MACD and drive the histogram to 0.)
    closes = pd.Series([100.0 + i + 0.1 * i * i for i in range(80)])
    out = _macd_series_from_closes(closes, fast=12, slow=26, signal=9).dropna()
    assert out["macd"].iloc[-1] > 0
    assert out["histogram"].iloc[-1] > 0


def _macd_config():
    config = get_macd_config(fast=12, slow=26, signal=9)
    config.base_token = "WETH"
    config.quote_token = "USDC"
    config.chain = "base"
    return config


def _ohlcv_payload_hourly(prices: list[float]) -> list[dict[str, Any]]:
    """OHLCV payload with valid hourly timestamps (rolls past 24h).

    ``_ohlcv_payload`` formats the hour as ``T{h:02d}`` so any series longer
    than 24 rows produces invalid hours that get dropped — too few rows for
    MACD's 26+9 warmup. This rolls a real hourly index instead.
    """
    times = pd.date_range("2026-05-12", periods=len(prices), freq="1h", tz="UTC")
    return [
        {"timestamp": t.isoformat(), "open": str(p), "high": str(p), "low": str(p), "close": str(p), "volume": "1"}
        for t, p in zip(times, prices, strict=True)
    ]


def test_prepare_populates_macd_chart_keys():
    prices = [2300.0 + i * 1.5 for i in range(80)]
    client = _FakeClient(ohlcv=_ohlcv_payload_hourly(prices))
    out = prepare_ta_session_state(client, session_state={}, config=_macd_config())
    # The renderer's MACD branch consumes `macd_data` as a time-indexed frame.
    assert isinstance(out["macd_data"], pd.DataFrame)
    assert list(out["macd_data"].columns) == ["macd", "signal", "histogram"]
    assert isinstance(out["macd_data"].index, pd.DatetimeIndex)
    assert not out["macd_data"].empty
    # Latest scalars feed the signal-status section.
    assert "macd_value" in out and "signal_line" in out and "histogram" in out
    assert out["histogram"] == pytest.approx(out["macd_value"] - out["signal_line"])


def test_prepare_macd_preserves_caller_supplied_data():
    caller_macd = pd.DataFrame({"macd": [1.0], "signal": [0.5], "histogram": [0.5]})
    client = _FakeClient(ohlcv=_ohlcv_payload([2300.0 + i for i in range(80)]))
    out = prepare_ta_session_state(client, session_state={"macd_data": caller_macd}, config=_macd_config())
    # Caller's frame is preserved, not recomputed.
    assert out["macd_data"].equals(caller_macd)


def test_macd_signal_fn_classifies_by_histogram():
    assert "BULLISH" in _macd_signal_fn({"macd_value": 1.2, "signal_line": 0.8, "histogram": 0.4})
    assert "BEARISH" in _macd_signal_fn({"macd_value": -0.5, "signal_line": 0.1, "histogram": -0.6})
    assert "NEUTRAL" in _macd_signal_fn({"macd_value": 0.0, "signal_line": 0.0, "histogram": 0.0})


def test_render_charts_section_macd_does_not_raise():
    """MACD config + macd_data frame must hit the MACD branch without error."""
    from almanak.framework.dashboard.templates.ta_dashboard import _render_charts_section

    times = pd.date_range("2026-05-12", periods=80, freq="1h", tz="UTC")
    closes = pd.Series([2300.0 + i for i in range(80)])
    macd_df = _macd_series_from_closes(closes, 12, 26, 9)
    macd_df.index = times
    macd_df = macd_df.dropna(subset=["macd"])
    session_state = {
        "price_history": pd.DataFrame({"time": times, "price": closes}),
        "macd_data": macd_df,
        "buy_signals": pd.DataFrame([{"time": times[40], "price": closes.iloc[40]}]),
        "sell_signals": pd.DataFrame(columns=["time", "price"]),
    }
    # No Streamlit script context → calls are no-ops; the point is the
    # DataFrame-valued indicator path must not raise (the generic else
    # branch would explode on `Series.values` from a 2-D frame).
    _render_charts_section(session_state, {}, _macd_config(), period=12)


def test_prepare_accepts_dataclass_like_tape_rows():
    class _Row:
        def __init__(self, ts: str, intent_type: str, token_in: str, token_out: str, price: str) -> None:
            self.timestamp = pd.Timestamp(ts)
            self.intent_type = intent_type
            self.token_in = token_in
            self.token_out = token_out
            self.effective_price = price

    class _Tape:
        rows = [_Row("2026-05-12T03:00:00Z", "SWAP", "USDC", "WETH", "2200")]

    class TapeClient:
        def get_ohlcv(self, **_: Any) -> list[dict[str, Any]]:
            return _ohlcv_payload([100.0 + i for i in range(30)])

        def get_trade_tape(self) -> Any:
            return _Tape()

    out = prepare_ta_session_state(TapeClient(), session_state={}, config=_config())
    assert len(out["buy_signals"]) == 1


# ----------------------------------------------------------------------
# VIB-4969: dashboard timeframe must match the strategy candle granularity.
# ----------------------------------------------------------------------


class _RecordingClient:
    """api_client stand-in that records the get_ohlcv() call kwargs."""

    def __init__(self, ohlcv: list[dict[str, Any]] | None = None) -> None:
        self._ohlcv = ohlcv or []
        self.ohlcv_kwargs: dict[str, Any] | None = None

    def get_ohlcv(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.ohlcv_kwargs = kwargs
        return self._ohlcv

    def get_trade_tape(self) -> dict[str, Any]:
        return {"rows": [], "has_more": False}

    def get_timeline(self, **_: Any) -> list[dict[str, Any]]:
        return []


def test_ohlcv_limit_policy_scales_per_timeframe():
    # 1h preserves the legacy default exactly (back-compat anchor).
    assert ohlcv_limit_for_timeframe("1h") == DEFAULT_CANDLE_LIMIT == 168
    # Fine granularities are capped (NOT a fixed 7-day window = 2016 @ 5m).
    assert ohlcv_limit_for_timeframe("5m") == 720
    assert ohlcv_limit_for_timeframe("1m") == 720
    assert ohlcv_limit_for_timeframe("15m") == 720
    # Coarse granularities get a longer recent span.
    assert ohlcv_limit_for_timeframe("4h") == 180
    assert ohlcv_limit_for_timeframe("1d") == 120
    # Case / whitespace tolerant.
    assert ohlcv_limit_for_timeframe(" 5M ") == 720
    # Unknown timeframe → fail-safe to the legacy default, never unbounded.
    assert ohlcv_limit_for_timeframe("3h") == DEFAULT_CANDLE_LIMIT
    assert ohlcv_limit_for_timeframe("") == DEFAULT_CANDLE_LIMIT


def test_prepare_requests_ohlcv_with_configured_timeframe_and_scaled_limit():
    """VIB-4969: a 5m strategy must fetch 5m candles, not the hardcoded 1h.

    This is the core bug: the dashboard used to fetch ``timeframe="1h"`` no
    matter what granularity the strategy decided on, so the RSI line was a
    different series from the one the strategy traded.
    """
    client = _RecordingClient(ohlcv=_ohlcv_payload_hourly([2300.0 + i for i in range(60)]))
    config = get_rsi_config(period=14, overbought=70, oversold=30, timeframe="5m")
    config.base_token = "WETH"
    config.quote_token = "USDC"
    config.chain = "arbitrum"

    prepare_ta_session_state(client, session_state={}, config=config)

    assert client.ohlcv_kwargs is not None
    assert client.ohlcv_kwargs["timeframe"] == "5m"
    assert client.ohlcv_kwargs["limit"] == ohlcv_limit_for_timeframe("5m") == 720
    assert client.ohlcv_kwargs["token"] == "WETH"
    assert client.ohlcv_kwargs["quote"] == "USDC"


def test_prepare_defaults_to_1h_when_timeframe_unset():
    """Back-compat: callers that never set a timeframe still request 1h/168."""
    client = _RecordingClient(ohlcv=_ohlcv_payload_hourly([2300.0 + i for i in range(60)]))
    config = get_rsi_config(period=14)  # no timeframe kwarg
    config.base_token = "WETH"
    config.quote_token = "USDC"
    config.chain = "arbitrum"

    prepare_ta_session_state(client, session_state={}, config=config)

    assert client.ohlcv_kwargs is not None
    assert client.ohlcv_kwargs["timeframe"] == "1h"
    assert client.ohlcv_kwargs["limit"] == 168


def test_normalize_timeframe_coerces_falsy_to_default():
    """VIB-4969 (Gemini): None / empty / whitespace must not reach get_ohlcv.

    A strategy may carry ``data_granularity: null``; passing that straight
    through would error at the data layer. Non-empty values pass unchanged.
    """
    assert normalize_timeframe(None) == "1h"
    assert normalize_timeframe("") == "1h"
    assert normalize_timeframe("   ") == "1h"
    assert normalize_timeframe("5m") == "5m"
    assert normalize_timeframe(" 1h ") == "1h"


def test_prepare_requests_1h_when_config_timeframe_is_none():
    """A falsy config.timeframe must be normalized to 1h, not passed as None."""
    client = _RecordingClient(ohlcv=_ohlcv_payload_hourly([2300.0 + i for i in range(60)]))
    config = get_rsi_config(period=14)
    config.base_token = "WETH"
    config.quote_token = "USDC"
    config.chain = "arbitrum"
    config.timeframe = None  # e.g. data_granularity: null reached the config

    prepare_ta_session_state(client, session_state={}, config=config)

    assert client.ohlcv_kwargs is not None
    assert client.ohlcv_kwargs["timeframe"] == "1h"
    assert client.ohlcv_kwargs["limit"] == 168


def test_rsi_series_is_computed_from_the_returned_frame():
    """The RSI series and the chart price share one frame/timeframe.

    Guards the bug's other half: the displayed RSI must be derived from the
    SAME candles the dashboard fetched (so markers and the RSI band align).
    Recompute RSI independently from the returned closes and assert the
    enriched ``rsi_history`` matches it point-for-point on the same index.
    """
    prices = [2300.0 + (15 if i % 6 < 3 else -12) for i in range(60)]
    payload = _ohlcv_payload_hourly(prices)
    client = _RecordingClient(ohlcv=payload)
    config = get_rsi_config(period=14, timeframe="5m")
    config.base_token = "WETH"
    config.quote_token = "USDC"
    config.chain = "arbitrum"

    out = prepare_ta_session_state(client, session_state={}, config=config)

    price_df = out["price_history"]
    rsi_history = out["rsi_history"]
    assert isinstance(rsi_history, pd.Series)
    # Independent recompute from the same returned frame.
    expected = _rsi_series_from_closes(price_df["price"], 14)
    expected.index = pd.DatetimeIndex(price_df["time"])
    expected = expected.dropna()
    assert not rsi_history.empty
    assert rsi_history.index.equals(expected.index)
    pd.testing.assert_series_equal(
        rsi_history.astype(float), expected.astype(float), check_names=False
    )


def test_multi_signal_extras_inherit_primary_timeframe():
    """Multi-signal: the primary's timeframe drives the single shared fetch.

    The extras' own timeframes are ignored — only one OHLCV request is made,
    at the primary timeframe.
    """
    from almanak.framework.dashboard.templates import multi_ta_config

    client = _RecordingClient(ohlcv=_ohlcv_payload_hourly([2300.0 + i for i in range(80)]))
    primary = get_rsi_config(period=14, timeframe="15m")
    extra = get_macd_config()  # default 1h timeframe — must be ignored
    config = multi_ta_config(primary, extra)
    config.base_token = "WETH"
    config.quote_token = "USDC"
    config.chain = "arbitrum"

    prepare_ta_session_state(client, session_state={}, config=config)

    assert client.ohlcv_kwargs is not None
    assert client.ohlcv_kwargs["timeframe"] == "15m"
    assert client.ohlcv_kwargs["limit"] == ohlcv_limit_for_timeframe("15m")


# ----------------------------------------------------------------------
# VIB-4969 finding #3: displayed RSI must equal the strategy's DECISION RSI.
#
# The strategy computes RSI from a sliding ``period + RSI_DECISION_BUFFER``
# window per iteration (rsi.RSICalculator.calculate_rsi). A single continuous
# Wilder EMA over the whole dashboard pull drifts from that. The dashboard now
# reconstructs the decision-faithful series so markers sit on the crossings the
# strategy actually saw.
# ----------------------------------------------------------------------


def test_dashboard_decision_buffer_matches_strategy_constant():
    """Drift guard: the dashboard's local buffer must equal the canonical one.

    The dashboard re-declares the value (it can't import the indicator stack —
    that would break the lean-import contract), so this test ties the two
    together: if someone changes ``rsi.RSI_DECISION_BUFFER`` the dashboard
    reconstruction would silently diverge unless this fails first.
    """
    from almanak.framework.data.indicators.rsi import RSI_DECISION_BUFFER

    assert _RSI_DECISION_BUFFER == RSI_DECISION_BUFFER


def test_wilder_rsi_window_matches_strategy_scalar():
    """``_wilder_rsi_window`` is byte-for-byte the strategy's scalar RSI."""
    from decimal import Decimal

    from almanak.framework.data.indicators.rsi import RSICalculator

    closes = [2300.0 + (12 if i % 5 < 2 else -9) for i in range(34)]
    dash = _wilder_rsi_window(closes, 14)
    strat = RSICalculator.calculate_rsi_from_prices([Decimal(str(c)) for c in closes], 14)
    assert dash == pytest.approx(strat, abs=1e-9)


def test_displayed_rsi_at_trade_timestamp_equals_decision_rsi():
    """The reconstructed RSI at a trade candle == the strategy's decision RSI.

    Build a 5m price series; pick a candle as the "trade" timestamp; compute the
    strategy's decision RSI the way the runner does (sliding ``period + buffer``
    window ending at that candle) and assert the dashboard's ``rsi_history`` at
    that timestamp matches — NOT a continuous-EMA recompute.
    """
    from decimal import Decimal

    from almanak.framework.data.indicators.rsi import RSICalculator

    period = 14
    window = period + _RSI_DECISION_BUFFER
    prices = [2300.0 + (15 if i % 6 < 3 else -12) for i in range(80)]
    payload = _ohlcv_payload_hourly(prices)
    client = _RecordingClient(ohlcv=payload)
    config = get_rsi_config(period=period, timeframe="5m")
    config.base_token, config.quote_token, config.chain = "WETH", "USDC", "arbitrum"

    out = prepare_ta_session_state(client, session_state={}, config=config)
    rsi_history = out["rsi_history"]
    price_df = out["price_history"]

    # Pick a trade candle well past warm-up.
    trade_idx = 70
    trade_time = price_df["time"].iloc[trade_idx]

    # Strategy's decision RSI: sliding window of `window` closes ending at trade.
    start = max(0, trade_idx - window + 1)
    decision_window = [Decimal(str(p)) for p in price_df["price"].iloc[start : trade_idx + 1]]
    decision_rsi = RSICalculator.calculate_rsi_from_prices(decision_window, period)

    displayed = float(rsi_history.loc[trade_time])
    assert displayed == pytest.approx(decision_rsi, abs=1e-9)


def test_decision_faithful_series_differs_from_continuous_ema():
    """Regression: the fix must NOT collapse back to a continuous Wilder EMA.

    A continuous EMA over the full pull (the old behaviour) and the sliding
    decision-window reconstruction give *different* mid-series values once the
    smoothing has diverged. Assert they differ on a realistic series, so a
    future refactor that silently reverts to ``ewm`` over the whole frame fails
    here instead of in production.
    """
    prices = pd.Series([2300.0 + (18 if i % 7 < 3 else -14) for i in range(120)])
    decision = _rsi_series_from_closes(prices, 14).dropna()

    # Old continuous-EMA reconstruction (what the dashboard used to do).
    delta = prices.astype(float).diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    continuous = (100 - (100 / (1 + rs))).where(avg_loss != 0, other=100.0).dropna()

    common = decision.index.intersection(continuous.index)
    assert len(common) > 10
    # At least somewhere in the middle the two series disagree by > 0.5 RSI pt.
    max_gap = (decision.loc[common].astype(float) - continuous.loc[common].astype(float)).abs().max()
    assert max_gap > 0.5
