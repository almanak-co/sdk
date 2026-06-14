"""TA/LP price charts follow the operator-selected NAV range (VIB-5114).

When the operator picks a bounded NAV range (24h/7d/30d) on the shared NAV chart,
the TA and LP price-chart candle fetch follows that range's granularity, and the
TA marker fetch follows the same window (``from_ts = now - range_seconds``). The
default path (no ``deployment_id``, no selection, "All", or an unknown value) is
byte-for-byte the legacy recent-window fetch. These tests record the kwargs the
templates would send to ``api_client.get_ohlcv`` / ``get_trade_tape`` — no
network, no gateway, no store writes.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import pytest

from almanak.framework.dashboard.chart_window import candles_for_range, granularity_for_range
from almanak.framework.dashboard.sections import nav_range_session_key
from almanak.framework.dashboard.templates import (
    get_rsi_config,
    get_uniswap_v3_config,
    prepare_lp_session_state,
    prepare_ta_session_state,
)
from almanak.framework.dashboard.templates._ohlcv_window import (
    build_chart_window,
    ohlcv_limit_for_timeframe,
)
from almanak.framework.dashboard.templates.ta_dashboard import _resolve_chart_window

_DID = "deployment:wf01"


def _ohlcv(prices: list[float]) -> list[dict[str, Any]]:
    times = pd.date_range("2026-05-12", periods=len(prices), freq="1h", tz="UTC")
    return [
        {"timestamp": t.isoformat(), "open": str(p), "high": str(p), "low": str(p), "close": str(p), "volume": "1"}
        for t, p in zip(times, prices, strict=True)
    ]


class _RecordingTAClient:
    """Records get_ohlcv / get_trade_tape kwargs for the TA template."""

    def __init__(self) -> None:
        self.ohlcv_kwargs: dict[str, Any] | None = None
        self.tape_from_ts: Any = "UNSET"
        self._ohlcv = _ohlcv([2300.0 + i for i in range(60)])

    def get_ohlcv(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.ohlcv_kwargs = kwargs
        return self._ohlcv

    def get_trade_tape(self, from_ts: Any = None, **_: Any) -> dict[str, Any]:
        self.tape_from_ts = from_ts
        return {"rows": [], "has_more": False}

    def get_timeline(self, **_: Any) -> list[dict[str, Any]]:
        return []


class _LegacyTapeClient(_RecordingTAClient):
    """A duck-typed client whose get_trade_tape predates the from_ts kwarg."""

    def get_trade_tape(self, **kwargs: Any) -> dict[str, Any]:  # type: ignore[override]
        if "from_ts" in kwargs:
            raise TypeError("get_trade_tape() got an unexpected keyword argument 'from_ts'")
        self.tape_from_ts = "LEGACY_CALLED"
        return {"rows": [], "has_more": False}


def _ta_config(timeframe: str = "1h"):
    cfg = get_rsi_config(period=14, timeframe=timeframe)
    cfg.base_token, cfg.quote_token, cfg.chain = "WETH", "USDC", "arbitrum"
    return cfg


# ---------------------------------------------------------------------------
# D1.S3 — TA follows a selected bounded range
# ---------------------------------------------------------------------------


def test_ta_follows_selected_range() -> None:
    client = _RecordingTAClient()
    state = {nav_range_session_key(_DID): "7d"}
    prepare_ta_session_state(client, session_state=state, config=_ta_config(), deployment_id=_DID)

    assert client.ohlcv_kwargs is not None
    expected_tf = granularity_for_range(604_800)
    assert client.ohlcv_kwargs["timeframe"] == expected_tf
    assert client.ohlcv_kwargs["limit"] == candles_for_range(604_800, expected_tf)
    # Markers fetched from the window start (~now - 7d).
    assert isinstance(client.tape_from_ts, datetime)
    delta = (datetime.now(UTC) - client.tape_from_ts).total_seconds()
    assert abs(delta - 604_800) < 60


# ---------------------------------------------------------------------------
# D3.F2 — "All" / unset / unknown all fall through to legacy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("state", [{}, {nav_range_session_key(_DID): "All"}, {nav_range_session_key(_DID): "bogus"}])
def test_ta_all_and_unset_are_legacy(state: dict[str, Any]) -> None:
    client = _RecordingTAClient()
    prepare_ta_session_state(client, session_state=state, config=_ta_config("5m"), deployment_id=_DID)
    assert client.ohlcv_kwargs is not None
    # Legacy: configured timeframe + its recent-window cap; markers unbounded (None).
    assert client.ohlcv_kwargs["timeframe"] == "5m"
    assert client.ohlcv_kwargs["limit"] == ohlcv_limit_for_timeframe("5m") == 720
    assert client.tape_from_ts is None


def test_ta_no_deployment_id_is_legacy() -> None:
    client = _RecordingTAClient()
    # Even with a range in state, NO deployment_id => legacy (back-compat call site).
    state = {nav_range_session_key(_DID): "7d"}
    prepare_ta_session_state(client, session_state=state, config=_ta_config("5m"))
    assert client.ohlcv_kwargs is not None
    assert client.ohlcv_kwargs["timeframe"] == "5m"
    assert client.ohlcv_kwargs["limit"] == 720
    assert client.tape_from_ts is None


# ---------------------------------------------------------------------------
# D3.F3 — duck-typed client without the windowed kwarg still works
# ---------------------------------------------------------------------------


def test_ta_legacy_trade_tape_client_falls_back() -> None:
    client = _LegacyTapeClient()
    state = {nav_range_session_key(_DID): "7d"}
    out = prepare_ta_session_state(client, session_state=state, config=_ta_config(), deployment_id=_DID)
    # No exception escaped; the unbounded fallback was taken; markers populated.
    assert client.tape_from_ts == "LEGACY_CALLED"
    assert "buy_signals" in out and "sell_signals" in out


# ---------------------------------------------------------------------------
# D1.S4 — LP follows a selected bounded range (candles only, no markers)
# ---------------------------------------------------------------------------


class _RecordingLPClient:
    """Records get_ohlcv kwargs for the LP pool-candle fetch."""

    def __init__(self) -> None:
        self.ohlcv_kwargs: dict[str, Any] | None = None
        self.tape_called = False

    def get_state(self) -> dict[str, Any]:
        return {}

    def get_summary(self) -> dict[str, Any]:
        return {}

    def get_price(self, *_a: Any, **_k: Any) -> float | None:
        return 2500.0

    def get_position_events(self, **_: Any) -> list[dict[str, Any]]:
        return []

    def get_ohlcv(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.ohlcv_kwargs = kwargs
        return _ohlcv([2300.0 + i for i in range(40)])

    def get_trade_tape(self, **_: Any) -> dict[str, Any]:
        self.tape_called = True
        return {"rows": [], "has_more": False}


def _lp_state_with_pool() -> dict[str, Any]:
    return {"positions": [{"pool_address": "0xpool", "chain": "base"}]}


def test_lp_follows_selected_range() -> None:
    client = _RecordingLPClient()
    config = get_uniswap_v3_config(token0="WETH", token1="USDC")
    config.chain = "base"
    state = _lp_state_with_pool()
    state[nav_range_session_key(_DID)] = "30d"
    prepare_lp_session_state(client, session_state=state, config=config, deployment_id=_DID)

    assert client.ohlcv_kwargs is not None
    expected_tf = granularity_for_range(2_592_000)
    assert client.ohlcv_kwargs["timeframe"] == expected_tf
    assert client.ohlcv_kwargs["limit"] == candles_for_range(2_592_000, expected_tf)


def test_lp_unset_range_is_legacy() -> None:
    client = _RecordingLPClient()
    config = get_uniswap_v3_config(token0="WETH", token1="USDC")
    config.chain = "base"
    prepare_lp_session_state(client, session_state=_lp_state_with_pool(), config=config, deployment_id=_DID)
    assert client.ohlcv_kwargs is not None
    legacy_tf = config.timeframe or "1h"
    assert client.ohlcv_kwargs["timeframe"] == legacy_tf
    assert client.ohlcv_kwargs["limit"] == ohlcv_limit_for_timeframe(legacy_tf)


# ---------------------------------------------------------------------------
# D2.M2 — both templates resolve through one shared pure builder
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("range_seconds", [None, 0, 86_400, 604_800, 2_592_000])
def test_shared_builder_parity(range_seconds: int | None) -> None:
    # The TA resolver and the pure LP-equivalent builder agree on (timeframe, limit)
    # for the same inputs — no divergence between templates.
    state = {}
    if range_seconds == 0:
        state = {nav_range_session_key(_DID): "All"}
    elif range_seconds == 86_400:
        state = {nav_range_session_key(_DID): "24h"}
    elif range_seconds == 604_800:
        state = {nav_range_session_key(_DID): "7d"}
    elif range_seconds == 2_592_000:
        state = {nav_range_session_key(_DID): "30d"}

    ta_window = _resolve_chart_window(_DID, state, "1h")
    lp_window = build_chart_window("1h", range_seconds if range_seconds else None)
    assert ta_window.timeframe == lp_window.timeframe
    assert ta_window.limit == lp_window.limit


# ---------------------------------------------------------------------------
# D3.F4 — clip backstop is a no-op when candles + markers share the window
# ---------------------------------------------------------------------------


def test_clip_is_noop_on_shared_window() -> None:
    from almanak.framework.dashboard.templates.ta_dashboard import _clip_signals_to_price_window

    now = datetime.now(UTC)
    from_ts = now - timedelta(days=7)
    # Candles span the full [from_ts, now] window (hourly).
    candle_times = pd.date_range(from_ts, now, freq="1h", tz="UTC")
    price_df = pd.DataFrame({"time": candle_times, "price": [2300.0] * len(candle_times)})
    # Markers all lie INSIDE the window — exactly what a windowed marker fetch yields.
    marker_times = [from_ts + timedelta(days=1), from_ts + timedelta(days=3), now - timedelta(hours=2)]
    markers = pd.DataFrame({"time": marker_times, "price": [2310.0, 2290.0, 2305.0]})

    clipped = _clip_signals_to_price_window(markers, price_df)
    # Nothing clipped: same count, same rows.
    assert clipped is not None
    assert len(clipped) == len(markers)
    assert list(clipped["time"]) == list(markers["time"])
