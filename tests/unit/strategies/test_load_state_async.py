"""Tests for IntentStrategy.load_state_async()."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.strategies.intent_strategy import IntentStrategy


# ---------------------------------------------------------------------------
# Minimal concrete subclass (bypasses full IntentStrategy.__init__)
# ---------------------------------------------------------------------------


class _MinimalStrategy(IntentStrategy):
    """Bare-minimum concrete strategy for unit testing load_state_async."""

    STRATEGY_NAME = "test_load_state_async"

    def __init__(self) -> None:  # type: ignore[override]
        # Bypass IntentStrategy.__init__ — we only test state-loading logic.
        self._state_manager = None
        self._strategy_id = ""
        self._state_version = 0

    def decide(self, market):  # type: ignore[override]
        return None

    def get_open_positions(self):  # type: ignore[override]
        return None

    def generate_teardown_intents(self, mode, market=None):  # type: ignore[override]
        return []

    def load_persistent_state(self, state: dict) -> None:
        """Record which state dict was loaded."""
        self._loaded_state = state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_state_data(state: dict | None, version: int = 1) -> SimpleNamespace:
    """Build a fake StateData object."""
    return SimpleNamespace(state=state, version=version)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLoadStateAsync:
    """load_state_async() covers the same cases as the sync load_state()."""

    def setup_method(self) -> None:
        self.strategy = _MinimalStrategy()

    # ------------------------------------------------------------------
    # Guard: no state manager / no strategy_id
    # ------------------------------------------------------------------

    def test_returns_false_when_no_state_manager(self) -> None:
        self.strategy._state_manager = None
        self.strategy._strategy_id = "my-strategy"
        result = asyncio.run(self.strategy.load_state_async())
        assert result is False
        # Guard must fire before any I/O — no state manager means nothing is awaited.

    def test_returns_false_when_no_strategy_id(self) -> None:
        mock_manager = MagicMock()
        mock_manager.load_state = AsyncMock()
        self.strategy._state_manager = mock_manager
        self.strategy._strategy_id = ""
        result = asyncio.run(self.strategy.load_state_async())
        assert result is False
        mock_manager.load_state.assert_not_called()

    # ------------------------------------------------------------------
    # Success path
    # ------------------------------------------------------------------

    def test_success_loads_state_and_returns_true(self) -> None:
        state = {"position_id": "123", "amount": "500"}
        state_data = _make_state_data(state, version=7)

        mock_manager = MagicMock()
        mock_manager.load_state = AsyncMock(return_value=state_data)

        self.strategy._state_manager = mock_manager
        self.strategy._strategy_id = "my-strategy"

        result = asyncio.run(self.strategy.load_state_async())

        assert result is True
        assert self.strategy._state_version == 7
        assert self.strategy._loaded_state == state
        mock_manager.load_state.assert_awaited_once_with("my-strategy")

    # ------------------------------------------------------------------
    # Not-found paths
    # ------------------------------------------------------------------

    def test_returns_false_when_state_data_is_none(self) -> None:
        mock_manager = MagicMock()
        mock_manager.load_state = AsyncMock(return_value=None)

        self.strategy._state_manager = mock_manager
        self.strategy._strategy_id = "my-strategy"

        result = asyncio.run(self.strategy.load_state_async())
        assert result is False

    def test_returns_false_when_state_dict_is_empty(self) -> None:
        state_data = _make_state_data(state={})
        mock_manager = MagicMock()
        mock_manager.load_state = AsyncMock(return_value=state_data)

        self.strategy._state_manager = mock_manager
        self.strategy._strategy_id = "my-strategy"

        result = asyncio.run(self.strategy.load_state_async())
        assert result is False

    def test_not_found_exception_returns_false_and_logs_debug(self) -> None:
        """StateNotFoundError (or any 'not found' message) → False, debug log."""
        mock_manager = MagicMock()
        mock_manager.load_state = AsyncMock(side_effect=Exception("State not found"))

        self.strategy._state_manager = mock_manager
        self.strategy._strategy_id = "my-strategy"

        with patch("almanak.framework.strategies.intent_strategy.logger") as mock_log:
            result = asyncio.run(self.strategy.load_state_async())

        assert result is False
        mock_log.debug.assert_called_once()
        debug_msg = mock_log.debug.call_args[0][0]
        assert "No existing state" in debug_msg

    def test_generic_exception_returns_false_and_logs_warning(self) -> None:
        """Unexpected errors → False, warning log."""
        mock_manager = MagicMock()
        mock_manager.load_state = AsyncMock(side_effect=RuntimeError("network timeout"))

        self.strategy._state_manager = mock_manager
        self.strategy._strategy_id = "my-strategy"

        with patch("almanak.framework.strategies.intent_strategy.logger") as mock_log:
            result = asyncio.run(self.strategy.load_state_async())

        assert result is False
        mock_log.warning.assert_called_once()
        warning_msg = mock_log.warning.call_args[0][0]
        assert "Failed to load state" in warning_msg
