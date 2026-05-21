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
    get_rsi_config,
    prepare_ta_session_state,
)
from almanak.framework.dashboard.templates.ta_dashboard import (
    _ohlcv_to_price_history,
    _rsi_series_from_closes,
    _trade_rows_to_signals,
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
    ) -> None:
        self._ohlcv = ohlcv or []
        self._tape_rows = tape_rows or []
        self._timeline = timeline or []
        self.ohlcv_call_count = 0

    def get_ohlcv(self, **_: Any) -> list[dict[str, Any]]:
        self.ohlcv_call_count += 1
        return self._ohlcv

    def get_trade_tape(self) -> dict[str, Any]:
        return {"rows": self._tape_rows, "has_more": False}

    def get_timeline(self, **_: Any) -> list[dict[str, Any]]:
        return self._timeline


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
    assert list(df.columns) == ["time", "price"]
    assert len(df) == 3
    assert pd.api.types.is_datetime64_any_dtype(df["time"])
    assert df["price"].tolist() == [2300.0, 2305.0, 2310.0]


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
    caller_price_history = pd.DataFrame(
        [{"time": pd.Timestamp("2020-01-01", tz="UTC"), "price": 99.0}]
    )
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


def test_prepare_accepts_dataclass_like_tape_rows():
    class _Row:
        def __init__(
            self, ts: str, intent_type: str, token_in: str, token_out: str, price: str
        ) -> None:
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
