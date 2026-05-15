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
