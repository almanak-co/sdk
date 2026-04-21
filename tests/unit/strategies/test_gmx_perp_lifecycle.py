"""Unit tests for GMX V2 perp lifecycle demo strategy (VIB-3298).

Two regression areas:

- Issue 1: ``on_intent_executed()`` must transition ``_loop_state`` to
  ``"closed"`` on any successful PERP_CLOSE, not only when the strategy is
  already in ``"closing"``. Teardown emits PERP_CLOSE while the strategy is
  still in ``"open"``, and a state-driven condition would leave the synthetic
  position reported as open after teardown succeeds.
- Issue 2: the strategy must not hand the literal string ``"USD"`` to the
  token resolver / runner price pre-warm. The default config-derived tracker
  splits ``market="ETH/USD"`` and adds ``"USD"`` to the tracked set, which the
  resolver then fails to resolve on every tick.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.demo_strategies.gmx_perp_lifecycle.strategy import GMXPerpLifecycleStrategy


@pytest.fixture()
def strategy():
    """Construct the strategy without touching the IntentStrategy machinery."""
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        s = GMXPerpLifecycleStrategy.__new__(GMXPerpLifecycleStrategy)
        # Attributes normally populated by the framework / the strategy __init__
        s._strategy_id = "test_gmx_perp_lifecycle"
        s._chain = "arbitrum"
        s.market = "ETH/USD"
        s.collateral_token = "USDC"
        s.collateral_amount = Decimal("10")
        s.leverage = Decimal("2.0")
        s.is_long = True
        s.max_slippage_pct = Decimal("2.0")
        s.force_action = None
        s._loop_state = "idle"
        s._previous_stable_state = "idle"
        s._position_size_usd = Decimal("0")
        return s


# ---------------------------------------------------------------------------
# Issue 1 — teardown transitions _loop_state to "closed" directly from "open"
# ---------------------------------------------------------------------------


class TestPerpCloseStateTransition:
    """PERP_CLOSE must transition ``_loop_state`` to ``"closed"`` regardless of
    the starting state — otherwise teardown verification sees a stale position.
    """

    def _make_perp_close_intent(self):
        intent = MagicMock()
        intent.intent_type = MagicMock()
        intent.intent_type.value = "PERP_CLOSE"
        return intent

    def _make_perp_open_intent(self):
        intent = MagicMock()
        intent.intent_type = MagicMock()
        intent.intent_type.value = "PERP_OPEN"
        return intent

    def test_perp_close_from_open_transitions_to_closed(self, strategy):
        """Teardown path: strategy is in ``"open"`` when PERP_CLOSE succeeds."""
        strategy._loop_state = "open"
        strategy._position_size_usd = Decimal("20")

        strategy.on_intent_executed(self._make_perp_close_intent(), success=True, result=MagicMock())

        assert strategy._loop_state == "closed"
        assert strategy._position_size_usd == Decimal("0")

    def test_perp_close_from_closing_transitions_to_closed(self, strategy):
        """Normal lifecycle path still works (``"closing" -> "closed"``)."""
        strategy._loop_state = "closing"
        strategy._position_size_usd = Decimal("20")

        strategy.on_intent_executed(self._make_perp_close_intent(), success=True, result=MagicMock())

        assert strategy._loop_state == "closed"
        assert strategy._position_size_usd == Decimal("0")

    def test_get_open_positions_empty_after_teardown_close(self, strategy):
        """Teardown verification must see no open positions after PERP_CLOSE."""
        strategy._loop_state = "open"
        strategy._position_size_usd = Decimal("20")

        # Sanity check: before the close the strategy reports the position.
        summary_before = strategy.get_open_positions()
        assert len(summary_before.positions) == 1

        strategy.on_intent_executed(self._make_perp_close_intent(), success=True, result=MagicMock())

        summary_after = strategy.get_open_positions()
        assert summary_after.positions == []

    def test_perp_close_idempotent(self, strategy):
        """Calling the callback twice on PERP_CLOSE must not flip state back."""
        strategy._loop_state = "open"
        strategy._position_size_usd = Decimal("20")

        strategy.on_intent_executed(self._make_perp_close_intent(), success=True, result=MagicMock())
        strategy.on_intent_executed(self._make_perp_close_intent(), success=True, result=MagicMock())

        assert strategy._loop_state == "closed"
        assert strategy._position_size_usd == Decimal("0")

    def test_perp_open_transitions_to_open(self, strategy):
        strategy._loop_state = "opening"

        strategy.on_intent_executed(self._make_perp_open_intent(), success=True, result=MagicMock())

        assert strategy._loop_state == "open"
        # Stable-state marker must advance so a later failed close reverts to
        # "open", not the pre-open "idle".
        assert strategy._previous_stable_state == "open"

    def test_perp_close_promotes_previous_stable_state(self, strategy):
        strategy._loop_state = "closing"
        strategy._previous_stable_state = "open"
        strategy._position_size_usd = Decimal("20")

        strategy.on_intent_executed(self._make_perp_close_intent(), success=True, result=MagicMock())

        assert strategy._loop_state == "closed"
        assert strategy._previous_stable_state == "closed"

    def test_perp_close_failure_reverts_to_previous_state(self, strategy):
        strategy._loop_state = "closing"
        strategy._previous_stable_state = "open"
        strategy._position_size_usd = Decimal("20")

        strategy.on_intent_executed(self._make_perp_close_intent(), success=False, result=MagicMock())

        assert strategy._loop_state == "open"
        # On failure we must NOT clear the synthetic size — the position still exists.
        assert strategy._position_size_usd == Decimal("20")

    def test_open_then_failed_teardown_close_still_reports_position(self, strategy):
        """Regression: PERP_OPEN must promote ``_previous_stable_state`` so a
        later failed PERP_CLOSE reverts to ``"open"`` and the live position is
        still reported by ``get_open_positions()``.

        Without promotion, ``_previous_stable_state`` would still be ``"idle"``
        (from the constructor or the open-path bookkeeping in ``decide()``),
        and a failed close would revert the strategy to ``"idle"`` — silently
        hiding a live on-chain position from teardown verification.
        """
        # Start fresh — strategy is "idle", no synthetic bookkeeping yet.
        assert strategy._loop_state == "idle"
        assert strategy._previous_stable_state == "idle"

        # Real open: state machine records its own last-stable marker.
        strategy._position_size_usd = Decimal("20")  # would be set by _create_open_intent()
        strategy.on_intent_executed(self._make_perp_open_intent(), success=True, result=MagicMock())
        assert strategy._loop_state == "open"
        assert strategy._previous_stable_state == "open"

        # Teardown fires PERP_CLOSE. Simulate a failed close.
        strategy.on_intent_executed(self._make_perp_close_intent(), success=False, result=MagicMock())

        # The live position must still be visible to teardown verification.
        assert strategy._loop_state == "open"
        assert strategy._position_size_usd == Decimal("20")

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].value_usd == Decimal("20")


# ---------------------------------------------------------------------------
# Issue 2 — USD is never handed to the token resolver / price pre-warm
# ---------------------------------------------------------------------------


class TestTrackedTokensExcludeUsd:
    """Regression for the ``token_resolution_error token=USD`` warnings."""

    def test_tracked_tokens_excludes_usd(self, strategy):
        tokens = strategy._get_tracked_tokens()
        assert "USD" not in tokens
        assert "usd" not in [t.lower() for t in tokens]

    def test_tracked_tokens_includes_index_and_collateral(self, strategy):
        tokens = strategy._get_tracked_tokens()
        # ETH is the index token (base of ETH/USD); USDC is the collateral.
        assert "ETH" in tokens
        assert "USDC" in tokens

    def test_tracked_tokens_dedups_when_index_equals_collateral(self, strategy):
        strategy.market = "USDC/USD"
        strategy.collateral_token = "USDC"
        tokens = strategy._get_tracked_tokens()
        assert tokens == ["USDC"]

    def test_tracked_tokens_no_duplicates(self, strategy):
        tokens = strategy._get_tracked_tokens()
        assert len(tokens) == len(set(tokens))

    def test_tracked_tokens_handles_btc_usd_market(self, strategy):
        strategy.market = "BTC/USD"
        strategy.collateral_token = "USDC"
        tokens = strategy._get_tracked_tokens()
        assert "USD" not in tokens
        assert "BTC" in tokens
        assert "USDC" in tokens
