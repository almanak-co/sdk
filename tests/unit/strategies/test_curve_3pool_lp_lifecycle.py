"""Tests for the Curve 3pool LP Lifecycle strategy.

Validates LP_OPEN and LP_CLOSE decisions, state transitions,
teardown, and persistent state for Curve StableSwap 3pool on Ethereum.

Kitchen Loop iteration 143, VIB-2169.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

LP_TOKEN_ADDRESS = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"


@pytest.fixture
def strategy():
    from strategies.incubating.curve_3pool_lp_lifecycle.strategy import (
        Curve3poolLPLifecycleStrategy,
    )

    strat = Curve3poolLPLifecycleStrategy.__new__(Curve3poolLPLifecycleStrategy)
    strat._chain = "ethereum"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-curve-3pool-lp-lifecycle"
    strat.deposit_amount = Decimal("100")
    strat.force_action = "open"
    strat._phase = "IDLE"
    strat._lp_position_id = None
    return strat


def _mock_market(
    dai_balance: float = 10000.0,
    usdc_balance: float = 10000.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token in ("DAI", "USDC", "USDT"):
            return Decimal("1.0")
        raise ValueError(f"Unexpected token: {token}")

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "DAI":
            bal.balance = Decimal(str(dai_balance))
            bal.balance_usd = Decimal(str(dai_balance))
        elif token in ("USDC", "USDT"):
            bal.balance = Decimal(str(usdc_balance))
            bal.balance_usd = Decimal(str(usdc_balance))
        else:
            raise ValueError(f"Unexpected token: {token}")
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


# =============================================================================
# LP_OPEN Phase Tests
# =============================================================================


class TestOpenPhase:
    def test_idle_emits_lp_open_intent(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

    def test_open_uses_curve_protocol(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "curve"

    def test_open_pool_is_3pool(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.pool == "3pool"

    def test_open_amounts_match_config(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.amount0 == Decimal("100")  # DAI
        assert intent.amount1 == Decimal("100")  # USDC

    def test_open_chain_is_ethereum(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.chain == "ethereum"

    def test_open_range_is_dummy_for_curve(self, strategy):
        """Curve uses pool-based LP, range_lower/upper are dummy values."""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.range_lower == Decimal("1")
        assert intent.range_upper == Decimal("1000000")

    def test_forced_open_from_idle(self, strategy):
        strategy.force_action = "open"
        strategy._phase = "IDLE"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

    def test_forced_open_ignored_when_already_open(self, strategy):
        """force_action=open only triggers from IDLE phase."""
        strategy.force_action = "open"
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS
        market = _mock_market()
        intent = strategy.decide(market)
        # Should close since phase is OPEN with position_id
        assert intent.intent_type.value == "LP_CLOSE"


# =============================================================================
# LP_CLOSE Phase Tests
# =============================================================================


class TestClosePhase:
    def test_open_phase_with_position_emits_lp_close(self, strategy):
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS
        strategy.force_action = ""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_close_intent_pool_is_3pool(self, strategy):
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS
        strategy.force_action = ""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.pool == "3pool"

    def test_close_intent_position_id_is_lp_token_address(self, strategy):
        """For Curve, position_id is the LP token address (compiler queries on-chain balance)."""
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS
        strategy.force_action = ""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.position_id == LP_TOKEN_ADDRESS

    def test_close_intent_uses_curve_protocol(self, strategy):
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS
        strategy.force_action = ""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "curve"

    def test_forced_close_from_open_phase(self, strategy):
        strategy.force_action = "close"
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_open_phase_no_position_uses_fallback(self, strategy):
        """If no position_id tracked, fallback to known LP token address."""
        strategy._phase = "OPEN"
        strategy._lp_position_id = None
        strategy.force_action = ""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert intent.position_id == LP_TOKEN_ADDRESS


# =============================================================================
# CLOSED Phase Tests
# =============================================================================


class TestClosedPhase:
    def test_closed_phase_holds(self, strategy):
        strategy._phase = "CLOSED"
        strategy.force_action = ""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_closed_phase_hold_reason(self, strategy):
        strategy._phase = "CLOSED"
        strategy.force_action = ""
        market = _mock_market()
        intent = strategy.decide(market)
        assert "complete" in intent.reason.lower()


# =============================================================================
# State Transition Tests (on_intent_executed)
# =============================================================================


class TestStateTransitions:
    def test_lp_open_success_transitions_to_open(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"
        mock_result = MagicMock()
        mock_result.position_id = LP_TOKEN_ADDRESS

        strategy.on_intent_executed(mock_intent, success=True, result=mock_result)
        assert strategy._phase == "OPEN"
        assert strategy._lp_position_id == LP_TOKEN_ADDRESS

    def test_lp_open_failure_stays_in_phase(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"

        strategy.on_intent_executed(mock_intent, success=False, result=None)
        assert strategy._phase == "IDLE"

    def test_lp_close_success_transitions_to_closed(self, strategy):
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_CLOSE"
        mock_result = MagicMock()

        strategy.on_intent_executed(mock_intent, success=True, result=mock_result)
        assert strategy._phase == "CLOSED"
        assert strategy._lp_position_id is None

    def test_lp_close_failure_stays_open(self, strategy):
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_CLOSE"

        strategy.on_intent_executed(mock_intent, success=False, result=None)
        assert strategy._phase == "OPEN"
        assert strategy._lp_position_id == LP_TOKEN_ADDRESS

    def test_lp_open_no_position_id_uses_fallback(self, strategy):
        """If ResultEnricher doesn't set position_id, use known LP token address."""
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"
        mock_result = MagicMock()
        mock_result.position_id = None

        strategy.on_intent_executed(mock_intent, success=True, result=mock_result)
        assert strategy._phase == "OPEN"
        assert strategy._lp_position_id == LP_TOKEN_ADDRESS

    def test_full_lifecycle_idle_to_closed(self, strategy):
        """Simulate full lifecycle: IDLE -> OPEN -> CLOSED."""
        # Phase 1: LP_OPEN
        open_intent = MagicMock()
        open_intent.intent_type.value = "LP_OPEN"
        open_result = MagicMock()
        open_result.position_id = LP_TOKEN_ADDRESS
        strategy.on_intent_executed(open_intent, success=True, result=open_result)
        assert strategy._phase == "OPEN"

        # Phase 2: LP_CLOSE
        close_intent = MagicMock()
        close_intent.intent_type.value = "LP_CLOSE"
        close_result = MagicMock()
        strategy.on_intent_executed(close_intent, success=True, result=close_result)
        assert strategy._phase == "CLOSED"
        assert strategy._lp_position_id is None


# =============================================================================
# State Persistence Tests
# =============================================================================


class TestStatePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS
        state = strategy.get_persistent_state()
        assert state["phase"] == "OPEN"
        assert state["lp_position_id"] == LP_TOKEN_ADDRESS

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({
            "phase": "OPEN",
            "lp_position_id": LP_TOKEN_ADDRESS,
        })
        assert strategy._phase == "OPEN"
        assert strategy._lp_position_id == LP_TOKEN_ADDRESS

    def test_load_empty_state(self, strategy):
        """Loading empty state should not crash."""
        strategy.load_persistent_state({})
        assert strategy._phase == "IDLE"
        assert strategy._lp_position_id is None

    def test_roundtrip_persistence(self, strategy):
        strategy._phase = "CLOSED"
        strategy._lp_position_id = None
        state = strategy.get_persistent_state()

        strategy2 = strategy.__class__.__new__(strategy.__class__)
        strategy2._phase = "IDLE"
        strategy2._lp_position_id = None
        strategy2.load_persistent_state(state)
        assert strategy2._phase == "CLOSED"


# =============================================================================
# Teardown Tests
# =============================================================================


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_teardown_no_position_when_idle(self, strategy):
        strategy._phase = "IDLE"
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_teardown_no_position_when_closed(self, strategy):
        strategy._phase = "CLOSED"
        strategy._lp_position_id = None
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_teardown_has_position_when_open(self, strategy):
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.protocol == "curve"
        assert pos.details["pool"] == "3pool"

    def test_generate_teardown_intents_when_open(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"
        assert intents[0].position_id == LP_TOKEN_ADDRESS
        assert intents[0].pool == "3pool"

    def test_generate_teardown_intents_empty_when_closed(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._phase = "CLOSED"
        strategy._lp_position_id = None
        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert len(intents) == 0


# =============================================================================
# Status Tests
# =============================================================================


class TestStatus:
    def test_get_status_fields(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "curve_3pool_lp_lifecycle"
        assert status["chain"] == "ethereum"
        assert status["phase"] == "IDLE"
        assert status["pool"] == "3pool"

    def test_status_reflects_phase(self, strategy):
        strategy._phase = "OPEN"
        strategy._lp_position_id = LP_TOKEN_ADDRESS
        status = strategy.get_status()
        assert status["phase"] == "OPEN"
        assert status["lp_position_id"] == LP_TOKEN_ADDRESS
