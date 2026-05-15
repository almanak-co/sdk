"""VIB-4347 (Gemini audit): mock API client must grow with the real API.

A custom dashboard calling new methods (``get_ohlcv``, ``get_position_events``,
``get_position_history``) must NOT crash with the mock client returned by
:func:`almanak.framework.dashboard.custom.renderer.create_mock_api_client`.
"""

from __future__ import annotations

from almanak.framework.dashboard.custom.renderer import create_mock_api_client


# =============================================================================
# D2.3 — mock client implements the full new API surface
# =============================================================================


def test_mock_implements_full_api() -> None:
    """Custom dashboard calling the new methods does not raise."""
    mock = create_mock_api_client()
    # All three methods exist and are callable.
    assert callable(getattr(mock, "get_ohlcv", None))
    assert callable(getattr(mock, "get_position_events", None))
    assert callable(getattr(mock, "get_position_history", None))

    # Calling them with realistic args must not raise.
    candles = mock.get_ohlcv("WETH", chain="arbitrum", pool_address="0xabc")
    events = mock.get_position_events(position_types=["LP_OPEN", "LP_CLOSE"])
    history = mock.get_position_history(position_id="pid-1")

    assert isinstance(candles, list)
    assert isinstance(events, list)
    assert isinstance(history, list)


# =============================================================================
# F5 — mock must return empty lists, NOT synthetic fixture data
# =============================================================================


def test_mock_returns_empty_not_synthetic() -> None:
    """Mock methods must return ``[]`` — never invented fixture data.

    Synthesizing dollar-shaped data from a mock client would silently fool
    custom dashboards in fallback/demo mode into rendering meaningful-looking
    charts off thin air. If a test needs OHLCV/position fixtures it must
    inject them explicitly.
    """
    mock = create_mock_api_client()
    assert mock.get_ohlcv("WETH", chain="arbitrum") == []
    assert mock.get_position_events() == []
    assert mock.get_position_history(position_id="pid-1") == []


# =============================================================================
# Backward-compat: existing mock methods continue to work
# =============================================================================


def test_mock_preserves_existing_methods() -> None:
    """The mock client's existing surface (get_timeline, get_strategy_state,
    pause/resume) must still be present after the VIB-4347 additions."""
    mock = create_mock_api_client()
    assert callable(getattr(mock, "get_strategy_state", None))
    assert callable(getattr(mock, "get_timeline", None))
    assert callable(getattr(mock, "pause_strategy", None))
    assert callable(getattr(mock, "resume_strategy", None))

    # Their return shapes are unchanged.
    state = mock.get_strategy_state("my-strategy")
    assert isinstance(state, dict)
    assert state["status"] == "RUNNING"
    timeline = mock.get_timeline("my-strategy", limit=10)
    assert isinstance(timeline, list)
