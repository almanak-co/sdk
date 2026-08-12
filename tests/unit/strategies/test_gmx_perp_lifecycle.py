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

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
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
        s._deployment_id = "test_gmx_perp_lifecycle"
        s._chain = "arbitrum"
        s.market = "ETH/USD"
        # Address-first contract: the author-declared market-token address that
        # __init__ now requires and every intent carries.
        s.market_address = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
        s.index_token_address = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        s.collateral_token = "USDC"
        s.collateral_token_address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        s.collateral_amount = Decimal("10")
        s.leverage = Decimal("2.0")
        s.is_long = True
        s.max_slippage_pct = Decimal("2.0")
        s.force_action = None
        s.cancel_min_age_seconds = 315
        s.pending_trigger_distance_pct = Decimal("50")
        s._loop_state = "idle"
        s._previous_stable_state = "idle"
        s._position_size_usd = Decimal("0")
        s._pending_order_key = None
        s._pending_order_created_at = None
        s._replacement_order_key = None
        s._close_order_key = None
        s._position_observed = False
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


class TestTrackedTokensUseAddresses:
    """Regression for ambiguous symbol lookups and the literal USD quote."""

    def test_tracked_tokens_excludes_usd(self, strategy):
        tokens = strategy._get_tracked_tokens()
        assert "USD" not in tokens
        assert "usd" not in [t.lower() for t in tokens]

    def test_tracked_tokens_include_exact_index_and_collateral_addresses(self, strategy):
        assert strategy._get_tracked_tokens() == [
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        ]


class TestStrategyAuthoredCancelLifecycle:
    ORDER_A = "0x" + "11" * 32
    ORDER_B = "0x" + "22" * 32
    CLOSE_B = "0x" + "33" * 32

    @staticmethod
    def _intent(intent_type: str):
        return SimpleNamespace(intent_type=SimpleNamespace(value=intent_type))

    @staticmethod
    def _result(order_key: str):
        return SimpleNamespace(async_orders=[SimpleNamespace(order_id=order_key)])

    def test_first_open_is_a_non_marketable_resting_order(self, strategy):
        # The cancel/replace choreography is the explicit opt-in mode; the
        # default (force_action None) is the observation-driven lifecycle.
        strategy.force_action = "lifecycle"
        market = MagicMock()
        market.price.return_value = Decimal("3000")
        market.collateral_value_usd.return_value = Decimal("10")

        intent = strategy.decide(market)

        assert intent.intent_type.value == "PERP_OPEN"
        assert intent.settlement_mode == "submission"
        assert intent.trigger_price == Decimal("1500")
        assert strategy._loop_state == "opening_a"

    def test_open_a_callback_persists_authoritative_key(self, strategy):
        strategy._loop_state = "opening_a"

        strategy.on_intent_executed(self._intent("PERP_OPEN"), True, self._result(self.ORDER_A))

        assert strategy._loop_state == "pending_a"
        assert strategy._pending_order_key == self.ORDER_A
        assert strategy._pending_order_created_at is None
        state = strategy.get_persistent_state()
        assert state["pending_order_key"] == self.ORDER_A
        assert state["pending_order_created_at"] is None

    def test_cancel_is_not_emitted_before_account_age_gate(self, strategy):
        strategy._loop_state = "pending_a"
        strategy._pending_order_key = self.ORDER_A
        strategy._pending_order_created_at = datetime.now(UTC)
        market = MagicMock()
        market.block_timestamp.return_value = strategy._pending_order_created_at

        intent = strategy.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert "cancellation gate" in intent.reason
        assert strategy._loop_state == "pending_a"

    def test_cancel_uses_exact_persisted_key_after_age_gate(self, strategy):
        strategy._loop_state = "pending_a"
        strategy._pending_order_key = self.ORDER_A
        strategy._pending_order_created_at = datetime.now(UTC) - timedelta(seconds=316)
        market = MagicMock()
        market.block_timestamp.return_value = datetime.now(UTC)

        intent = strategy.decide(market)

        assert intent.intent_type.value == "PERP_CANCEL_ORDER"
        assert intent.order_key == self.ORDER_A
        assert strategy._loop_state == "cancelling_a"

    def test_cancel_callback_unlocks_replacement_and_clears_old_key(self, strategy):
        strategy._loop_state = "cancelling_a"
        strategy._pending_order_key = self.ORDER_A
        strategy._pending_order_created_at = datetime.now(UTC) - timedelta(seconds=316)

        strategy.on_intent_executed(self._intent("PERP_CANCEL_ORDER"), True, SimpleNamespace())

        assert strategy._loop_state == "cancelled_a"
        assert strategy._pending_order_key is None
        assert strategy._pending_order_created_at is None

    def test_replacement_callback_stays_pending_until_position_read(self, strategy):
        strategy._loop_state = "opening_b"

        strategy.on_intent_executed(self._intent("PERP_OPEN"), True, self._result(self.ORDER_B))

        assert strategy._loop_state == "order_b_pending"
        assert strategy._replacement_order_key == self.ORDER_B
        assert strategy._position_observed is False

    def test_close_callback_waits_for_measured_absence(self, strategy):
        strategy._loop_state = "closing_b"
        strategy._position_observed = True
        strategy._position_size_usd = Decimal("20")

        strategy.on_intent_executed(self._intent("PERP_CLOSE"), True, self._result(self.CLOSE_B))

        assert strategy._loop_state == "close_submitted"
        assert strategy._close_order_key == self.CLOSE_B
        assert strategy._position_size_usd == Decimal("20")

    def test_malformed_order_key_requires_recovery_without_raising(self, strategy):
        strategy._loop_state = "opening_a"

        strategy.on_intent_executed(
            self._intent("PERP_OPEN"),
            True,
            self._result("adapter-placeholder"),
        )

        assert strategy._loop_state == "recovery_required"
        assert strategy._previous_stable_state == "recovery_required"

    @pytest.mark.parametrize("in_flight", ["opening", "opening_a", "closing", "cancelling_a", "opening_b", "closing_b"])
    def test_restart_never_replays_in_flight_money_action(self, strategy, in_flight: str):
        strategy.load_persistent_state(
            {
                "loop_state": in_flight,
                "previous_stable_state": "idle",
                "position_size_usd": "20",
            }
        )

        assert strategy._loop_state == "recovery_required"
        assert strategy.decide(MagicMock()).intent_type.value == "HOLD"

    def test_chain_timestamp_is_required_for_cancel_age(self, strategy):
        strategy._loop_state = "pending_a"
        strategy._pending_order_key = self.ORDER_A
        market = MagicMock()
        market.block_timestamp.return_value = None

        intent = strategy.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert "timestamp is unmeasured" in intent.reason

    def test_teardown_does_not_duplicate_submitted_close(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._loop_state = "close_submitted"
        strategy._position_observed = True

        assert strategy.generate_teardown_intents(TeardownMode.HARD) == []


class TestObservationDrivenDefaultLifecycle:
    """Default (no force_action) lifecycle: open -> observe -> close.

    The recommended perp pattern, portable across live, Anvil, and the PnL
    backtest plane: no submission mode, no trigger price, no cancellation, and
    settlement is verified only through the measured ``perp_positions`` read.
    """

    ORDER_B = "0x" + "22" * 32

    @staticmethod
    def _intent(intent_type: str):
        return SimpleNamespace(intent_type=SimpleNamespace(value=intent_type))

    def test_default_open_is_a_market_open(self, strategy):
        market = MagicMock()
        market.price.return_value = Decimal("3000")
        market.collateral_value_usd.return_value = Decimal("10")

        intent = strategy.decide(market)

        assert intent.intent_type.value == "PERP_OPEN"
        assert intent.settlement_mode == "auto"
        assert intent.trigger_price is None
        assert strategy._loop_state == "opening_b"

    def test_open_callback_with_order_key_persists_it(self, strategy):
        strategy._loop_state = "opening_b"

        strategy.on_intent_executed(
            self._intent("PERP_OPEN"),
            True,
            SimpleNamespace(async_orders=[SimpleNamespace(order_id=self.ORDER_B)]),
        )

        assert strategy._loop_state == "order_b_pending"
        assert strategy._replacement_order_key == self.ORDER_B

    def test_open_callback_without_async_orders_falls_back_to_observation(self, strategy):
        """Synchronous settlement (the PnL backtest plane) carries no order key:
        the strategy proceeds to the observation gate instead of parking in
        ``recovery_required`` — the measured venue read is the authority."""
        strategy._loop_state = "opening_b"

        strategy.on_intent_executed(self._intent("PERP_OPEN"), True, SimpleNamespace())

        assert strategy._loop_state == "order_b_pending"
        assert strategy._replacement_order_key is None

    def test_close_callback_without_async_orders_falls_back_to_observation(self, strategy):
        strategy._loop_state = "closing_b"
        strategy._position_observed = True

        strategy.on_intent_executed(self._intent("PERP_CLOSE"), True, SimpleNamespace())

        assert strategy._loop_state == "close_submitted"
        assert strategy._close_order_key is None

    def test_malformed_key_with_orders_present_still_requires_recovery(self, strategy):
        """A result that DOES carry async orders but with an unusable key is a
        broken live enrichment — the loud recovery path is preserved."""
        strategy._loop_state = "opening_b"

        strategy.on_intent_executed(
            self._intent("PERP_OPEN"),
            True,
            SimpleNamespace(async_orders=[SimpleNamespace(order_id="adapter-placeholder")]),
        )

        assert strategy._loop_state == "recovery_required"

    def test_observation_gate_closes_only_after_position_measured(self, strategy):
        strategy._loop_state = "order_b_pending"
        market = MagicMock()

        with patch.object(strategy, "_target_position_is_open", return_value=None):
            assert strategy.decide(market).intent_type.value == "HOLD"
        assert strategy._loop_state == "order_b_pending"

        with patch.object(strategy, "_target_position_is_open", return_value=False):
            assert strategy.decide(market).intent_type.value == "HOLD"
        assert strategy._loop_state == "order_b_pending"

        with patch.object(strategy, "_target_position_is_open", return_value=True):
            intent = strategy.decide(market)
        assert intent.intent_type.value == "PERP_CLOSE"
        assert intent.size_usd is None
        assert strategy._loop_state == "closing_b"
        assert strategy._position_observed is True

    def test_close_submitted_completes_only_on_measured_flat(self, strategy):
        strategy._loop_state = "close_submitted"
        strategy._position_observed = True
        market = MagicMock()

        with patch.object(strategy, "_target_position_is_open", return_value=True):
            assert strategy.decide(market).intent_type.value == "HOLD"
        assert strategy._loop_state == "close_submitted"

        with patch.object(strategy, "_target_position_is_open", return_value=False):
            assert strategy.decide(market).intent_type.value == "HOLD"
        assert strategy._loop_state == "closed"
        assert strategy._position_observed is False


class TestTrackedTokensAdditional:
    def test_tracked_tokens_dedups_when_index_equals_collateral(self, strategy):
        strategy.index_token_address = strategy.collateral_token_address
        tokens = strategy._get_tracked_tokens()
        assert tokens == [strategy.collateral_token_address]

    def test_tracked_tokens_no_duplicates(self, strategy):
        tokens = strategy._get_tracked_tokens()
        assert len(tokens) == len(set(tokens))

    def test_tracked_tokens_handles_btc_usd_market(self, strategy):
        btc_address = "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f"
        strategy.index_token_address = btc_address
        tokens = strategy._get_tracked_tokens()
        assert tokens == [btc_address, strategy.collateral_token_address]
