"""VIB-4347: ``prepare_lp_session_state`` populates OHLCV via the new API.

Verifies:
- Grouping by ``(chain, pool_address)`` tuple (multi-chain same-address case).
- Caller-provided ``price_history`` is preserved as override.
- ``position_history`` is populated from ``api_client.get_position_events(...)``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.framework.dashboard.templates.lp_dashboard import (
    LPDashboardConfig,
    prepare_lp_session_state,
)


@pytest.fixture
def api_client_mock() -> MagicMock:
    """Stub api_client with all read methods preconfigured."""
    api = MagicMock(name="api_client")
    api.get_state.return_value = {}
    api.get_price.return_value = 1900.0
    api.get_position.return_value = {"token_balances": []}
    api.get_position_events.return_value = []  # tests override
    api.get_ohlcv.return_value = []  # tests override
    return api


def _candle(close: str = "1900") -> dict:
    return {
        "timestamp": "2026-05-13T12:00:00+00:00",
        "open": "1890",
        "high": "1910",
        "low": "1880",
        "close": close,
        "volume": "1",
    }


# =============================================================================
# D1.5 — Groups by (chain, pool_address) tuple
# =============================================================================


def test_groups_by_chain_and_pool_address(api_client_mock: MagicMock) -> None:
    """Same pool address on two different chains -> two separate fetches.

    Without the tuple key, the second chain would silently overwrite the
    first — exactly the multi-chain same-address bug the design doc calls
    out in §3 / §9 adaptation 4.
    """
    api_client_mock.get_position_events.return_value = []
    api_client_mock.get_ohlcv.side_effect = lambda **kw: [
        _candle(close=f"{kw['chain']}:{kw['pool_address']}")
    ]

    caller_state = {
        "positions": [
            {"pool_address": "0xshared", "chain": "base"},
            {"pool_address": "0xshared", "chain": "optimism"},
        ],
    }
    config = LPDashboardConfig(token0="WETH")
    result = prepare_lp_session_state(api_client_mock, caller_state, config)

    by_pool = result.get("price_history_by_pool")
    assert isinstance(by_pool, dict)
    assert ("base", "0xshared") in by_pool
    assert ("optimism", "0xshared") in by_pool
    assert by_pool[("base", "0xshared")][0]["close"] == "base:0xshared"
    assert by_pool[("optimism", "0xshared")][0]["close"] == "optimism:0xshared"


# =============================================================================
# D1.5 / F6 — Caller-provided price_history is preserved as override
# =============================================================================


def test_preserves_caller_provided_price_history(api_client_mock: MagicMock) -> None:
    """Custom dashboard already filled price_history -> never overwrite."""
    api_client_mock.get_position_events.return_value = []
    api_client_mock.get_ohlcv.return_value = [_candle(close="9999")]
    caller_supplied = [_candle(close="custom-data-do-not-touch")]
    state = {
        "price_history": caller_supplied,
        "positions": [{"pool_address": "0xpool", "chain": "arbitrum"}],
    }
    config = LPDashboardConfig(token0="WETH")

    result = prepare_lp_session_state(api_client_mock, state, config)
    assert result["price_history"] is caller_supplied
    # The gateway-fetched value is parked under price_history_by_pool — caller
    # can still see what came from gateway if they want.
    if "price_history_by_pool" in result:
        for candles in result["price_history_by_pool"].values():
            for row in candles:
                assert row["close"] != "custom-data-do-not-touch"


def test_caller_price_history_not_silently_overwritten(api_client_mock: MagicMock) -> None:
    """F6 silent-error guard: caller-provided ``price_history`` is identical
    after the call (same object, same content). If a future refactor reaches
    in to mutate it, this test fails loudly."""
    api_client_mock.get_position_events.return_value = []
    api_client_mock.get_ohlcv.return_value = [_candle()]
    caller_list = [_candle(close="DO-NOT-OVERWRITE")]
    state = {"price_history": caller_list, "positions": []}
    config = LPDashboardConfig(token0="WETH")

    result = prepare_lp_session_state(api_client_mock, state, config)
    assert result["price_history"] is caller_list
    assert result["price_history"][0]["close"] == "DO-NOT-OVERWRITE"


# =============================================================================
# D1.5 — position_history populated from get_position_events
# =============================================================================


def test_position_history_populated_from_position_events(api_client_mock: MagicMock) -> None:
    api_client_mock.get_position_events.return_value = [
        {
            "position_id": "p1",
            "event_type": "OPEN",
            "timestamp": "2026-05-13T00:00:00+00:00",
            "tick_lower": 100,
            "tick_upper": 200,
            "position_type": "LP",
            "protocol": "uniswap_v3",
            "chain": "arbitrum",
        },
    ]
    state = {}
    config = LPDashboardConfig(token0="WETH")
    result = prepare_lp_session_state(api_client_mock, state, config)
    assert "position_history" in result
    positions = result["position_history"]
    assert len(positions) == 1
    assert positions[0]["position_id"] == "p1"
    assert positions[0]["is_active"] is True

    api_client_mock.get_position_events.assert_called_once_with(
        position_types=["LP"],
    )


def test_active_lp_event_hydrates_status_and_plot_bounds(api_client_mock: MagicMock) -> None:
    api_client_mock.get_state.return_value = {"current_position_id": "5497836"}
    api_client_mock.get_price.return_value = 2136.8
    api_client_mock.get_position.return_value = {"token_balances": []}
    api_client_mock.get_position_events.return_value = [
        {
            "position_id": "5497836",
            "event_type": "OPEN",
            "timestamp": "2026-05-20T17:07:47+00:00",
            "tick_lower": -200730,
            "tick_upper": -198720,
            "position_type": "LP",
            "protocol": "uniswap_v3",
            "chain": "arbitrum",
            "amount0": "995360453058942",
            "amount1": "2354621",
            "value_usd": "4.47",
        },
    ]
    api_client_mock.get_ohlcv.return_value = [
        {
            "timestamp": "2026-05-20T17:00:00+00:00",
            "close": "2136.8",
            "open": "2136.8",
            "high": "2136.8",
            "low": "2136.8",
            "volume": "0",
        }
    ]

    result = prepare_lp_session_state(
        api_client_mock,
        {},
        LPDashboardConfig(token0="WETH", token1="USDC", chain="arbitrum"),
    )

    assert result["position_id"] == "5497836"
    assert result["is_active"] is True
    assert result["in_range"] is True
    assert result["token0_amount"] == pytest.approx(0.000995360453058942)
    assert result["token1_amount"] == pytest.approx(2.354621)
    assert result["range_lower"] == pytest.approx(1917.9, rel=0.01)
    assert result["range_upper"] == pytest.approx(2344.9, rel=0.01)
    assert result["position_history"][0]["bound_price_lower"] == pytest.approx(1917.9, rel=0.01)
    assert result["position_history"][0]["bound_price_upper"] == pytest.approx(2344.9, rel=0.01)
    assert result["price_history"]


def test_position_history_preserves_caller_provided(api_client_mock: MagicMock) -> None:
    """If the caller already populated ``position_history``, do not refetch."""
    api_client_mock.get_position_events.return_value = [{"position_id": "fresh"}]
    caller_supplied = [{"position_id": "from-caller"}]
    state = {"position_history": caller_supplied}
    config = LPDashboardConfig(token0="WETH")
    result = prepare_lp_session_state(api_client_mock, state, config)
    assert result["position_history"] is caller_supplied
    api_client_mock.get_position_events.assert_not_called()


# =============================================================================
# D1.5 — fetch happens only when positions exist; not when there are none
# =============================================================================


def test_no_positions_means_no_ohlcv_fetch(api_client_mock: MagicMock) -> None:
    api_client_mock.get_position_events.return_value = []
    state = {"positions": []}
    config = LPDashboardConfig(token0="WETH")
    prepare_lp_session_state(api_client_mock, state, config)
    api_client_mock.get_ohlcv.assert_not_called()


# =============================================================================
# VIB-4969 — LP price-history chart honours the configured timeframe
# =============================================================================


def test_lp_ohlcv_fetch_uses_configured_timeframe(api_client_mock: MagicMock) -> None:
    """A 5m LP config must fetch 5m candles with a scaled limit, not 1h/168."""
    from almanak.framework.dashboard.templates._ohlcv_window import ohlcv_limit_for_timeframe

    api_client_mock.get_position_events.return_value = []
    api_client_mock.get_ohlcv.return_value = [_candle()]
    state = {"positions": [{"pool_address": "0xpool", "chain": "arbitrum"}]}
    config = LPDashboardConfig(token0="WETH", timeframe="5m")

    prepare_lp_session_state(api_client_mock, state, config)

    api_client_mock.get_ohlcv.assert_called_once()
    kwargs = api_client_mock.get_ohlcv.call_args.kwargs
    assert kwargs["timeframe"] == "5m"
    assert kwargs["limit"] == ohlcv_limit_for_timeframe("5m") == 720


def test_lp_ohlcv_fetch_defaults_to_1h(api_client_mock: MagicMock) -> None:
    """Back-compat: an LP config without an explicit timeframe still uses 1h/168."""
    api_client_mock.get_position_events.return_value = []
    api_client_mock.get_ohlcv.return_value = [_candle()]
    state = {"positions": [{"pool_address": "0xpool", "chain": "arbitrum"}]}
    config = LPDashboardConfig(token0="WETH")  # no timeframe

    prepare_lp_session_state(api_client_mock, state, config)

    api_client_mock.get_ohlcv.assert_called_once()
    kwargs = api_client_mock.get_ohlcv.call_args.kwargs
    assert kwargs["timeframe"] == "1h"
    assert kwargs["limit"] == 168
