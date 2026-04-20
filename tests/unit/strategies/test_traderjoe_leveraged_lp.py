"""Unit tests for TraderJoe V2 Leveraged LP with Auto-Compound strategy (VIB-111).

Tests the state machine, auto-compound cycle, health monitoring, and teardown
without requiring a gateway or Anvil fork.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import IntentType


# =============================================================================
# Helpers
# =============================================================================


def _create_strategy(config_overrides: dict | None = None):
    """Create a TraderJoeLeveragedLPStrategy with mocked framework dependencies."""
    # Import inside function to avoid import errors when framework is not fully configured
    import sys
    sys.path.insert(0, "almanak/demo_strategies/traderjoe_leveraged_lp")
    from almanak.demo_strategies.traderjoe_leveraged_lp.strategy import TraderJoeLeveragedLPStrategy

    config = {
        "strategy_id": "test-leveraged-lp",
        "strategy_name": "test-leveraged-lp",
        "chain": "avalanche",
        "collateral_token": "WAVAX",
        "collateral_amount": "5",
        "borrow_token": "USDC",
        "ltv_target": "0.3",
        "lp_pool": "WAVAX/USDC/20",
        "lp_range_width_pct": "0.1",
        "compound_min_usd": "5",
        "health_factor_floor": "1.5",
    }
    if config_overrides:
        config.update(config_overrides)

    with patch.object(TraderJoeLeveragedLPStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = TraderJoeLeveragedLPStrategy.__new__(TraderJoeLeveragedLPStrategy)

    # Manually initialize what __init__ would do (use private attrs for properties)
    strategy._strategy_id = config["strategy_id"]
    strategy._chain = config["chain"]
    strategy.collateral_token = config["collateral_token"]
    strategy.collateral_amount = Decimal(config["collateral_amount"])
    strategy.borrow_token = config["borrow_token"]
    strategy.ltv_target = Decimal(config["ltv_target"])
    strategy.interest_rate_mode = "variable"
    strategy.lp_pool = config["lp_pool"]
    strategy.lp_range_width_pct = Decimal(config["lp_range_width_pct"])
    strategy.compound_min_usd = Decimal(config["compound_min_usd"])
    strategy.health_factor_floor = Decimal(config["health_factor_floor"])
    strategy.liquidation_threshold = Decimal(config.get("liquidation_threshold", "0.65"))
    strategy._loop_state = "idle"
    strategy._previous_stable_state = "idle"
    strategy._supplied_amount = Decimal("0")
    strategy._borrowed_amount = Decimal("0")
    strategy._lp_bin_ids = []
    strategy._lp_wavax = Decimal("0")
    strategy._lp_usdc = Decimal("0")
    strategy._compound_count = 0
    strategy._collected_fee_wavax = Decimal("0")
    strategy._collected_fee_usdc = Decimal("0")
    strategy._deleveraging = False

    return strategy


def _mock_market(wavax_price=Decimal("25"), usdc_price=Decimal("1"), wavax_balance=Decimal("10")):
    """Create a mock MarketSnapshot."""
    market = MagicMock()

    def price_fn(token):
        prices = {"WAVAX": wavax_price, "USDC": usdc_price}
        if token not in prices:
            raise ValueError(f"No price for {token}")
        return prices[token]

    def balance_fn(token):
        bal = MagicMock()
        bal.balance = wavax_balance if token == "WAVAX" else Decimal("0")
        return bal

    market.price = price_fn
    market.balance = balance_fn
    return market


# =============================================================================
# Phase 1: Setup State Machine
# =============================================================================


class TestSetupPhase:
    """Test the initial setup flow: idle -> supply -> borrow -> LP_OPEN."""

    def test_idle_emits_supply_intent(self):
        strategy = _create_strategy()
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.SUPPLY
        assert strategy._loop_state == "supplying"

    def test_idle_holds_if_insufficient_balance(self):
        strategy = _create_strategy()
        market = _mock_market(wavax_balance=Decimal("2"))  # < 5 + 1

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.HOLD

    def test_supply_success_transitions_to_supplied(self):
        strategy = _create_strategy()
        strategy._loop_state = "supplying"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._loop_state == "supplied"
        assert strategy._supplied_amount == Decimal("5")

    def test_supplied_emits_borrow_intent(self):
        strategy = _create_strategy()
        strategy._loop_state = "supplied"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.BORROW
        assert strategy._loop_state == "borrowing"

    def test_borrow_success_transitions_to_borrowed(self):
        strategy = _create_strategy()
        strategy._loop_state = "borrowing"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "BORROW"
        mock_intent.borrow_amount = Decimal("37.50")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._loop_state == "borrowed"
        assert strategy._borrowed_amount == Decimal("37.50")

    def test_borrowed_emits_lp_open_intent(self):
        strategy = _create_strategy()
        strategy._loop_state = "borrowed"
        strategy._borrowed_amount = Decimal("37.50")
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.LP_OPEN
        assert strategy._loop_state == "lp_opening"

    def test_lp_open_success_transitions_to_active(self):
        strategy = _create_strategy()
        strategy._loop_state = "lp_opening"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._loop_state == "active"
        assert strategy._lp_bin_ids == [-1]


# =============================================================================
# Phase 2: Auto-Compound Cycle
# =============================================================================


class TestAutoCompoundCycle:
    """Test the auto-compound flow: active -> collect fees -> close LP -> reopen LP."""

    def test_active_emits_collect_fees_intent(self):
        strategy = _create_strategy()
        strategy._loop_state = "active"
        strategy._supplied_amount = Decimal("5")
        strategy._borrowed_amount = Decimal("37.50")
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.LP_COLLECT_FEES
        assert strategy._loop_state == "collecting_fees"

    def test_collect_fees_success_transitions_to_fees_collected(self):
        strategy = _create_strategy()
        strategy._loop_state = "collecting_fees"
        strategy._previous_stable_state = "active"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_COLLECT_FEES"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._loop_state == "fees_collected"

    def test_fees_collected_emits_lp_close(self):
        strategy = _create_strategy()
        strategy._loop_state = "fees_collected"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.LP_CLOSE
        assert strategy._loop_state == "closing_lp"

    def test_lp_close_for_compound_transitions_correctly(self):
        strategy = _create_strategy()
        strategy._loop_state = "closing_lp"
        strategy._previous_stable_state = "active"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_CLOSE"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._loop_state == "lp_closed_for_compound"

    def test_lp_closed_for_compound_emits_lp_open_with_fees(self):
        strategy = _create_strategy()
        strategy._loop_state = "lp_closed_for_compound"
        strategy._lp_wavax = Decimal("1")
        strategy._lp_usdc = Decimal("37.50")
        strategy._collected_fee_wavax = Decimal("0.05")
        strategy._collected_fee_usdc = Decimal("1.25")
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.LP_OPEN
        # Amounts should include collected fees
        assert intent.amount0 == Decimal("1.05")  # 1 + 0.05
        assert intent.amount1 == Decimal("38.75")  # 37.50 + 1.25

    def test_compound_lp_open_success_commits_accounting(self):
        """LP accounting is only committed after LP_OPEN succeeds."""
        strategy = _create_strategy()
        strategy._loop_state = "reopening_lp"
        strategy._previous_stable_state = "active"
        strategy._lp_wavax = Decimal("1")
        strategy._lp_usdc = Decimal("37.50")
        strategy._collected_fee_wavax = Decimal("0.05")
        strategy._collected_fee_usdc = Decimal("1.25")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._loop_state == "active"
        assert strategy._compound_count == 1
        # Accounting committed: original + fees
        assert strategy._lp_wavax == Decimal("1.05")
        assert strategy._lp_usdc == Decimal("38.75")
        # Fees cleared after commit
        assert strategy._collected_fee_wavax == Decimal("0")
        assert strategy._collected_fee_usdc == Decimal("0")

    def test_fee_ingestion_from_result(self):
        """on_intent_executed for LP_COLLECT_FEES extracts and normalizes fees from result."""
        strategy = _create_strategy()
        strategy._loop_state = "collecting_fees"
        strategy._previous_stable_state = "active"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_COLLECT_FEES"

        # Raw wei: 0.05 WAVAX (18 decimals), 1.25 USDC (6 decimals)
        mock_result = MagicMock()
        mock_result.extracted_data = {"fee_amount_x": "50000000000000000", "fee_amount_y": "1250000"}

        # Mock token decimals for normalization
        def mock_decimals(token):
            return {"WAVAX": 18, "USDC": 6}[token]

        with patch.object(strategy, "_get_token_decimals", side_effect=mock_decimals):
            strategy.on_intent_executed(mock_intent, success=True, result=mock_result)

        assert strategy._loop_state == "fees_collected"
        # Normalized from raw wei to human-readable amounts
        assert strategy._collected_fee_wavax == Decimal("0.05")
        assert strategy._collected_fee_usdc == Decimal("1.25")


# =============================================================================
# Health Monitoring
# =============================================================================


class TestHealthMonitoring:
    """Test health factor monitoring and deleverage trigger."""

    def test_healthy_position_compounds(self):
        """With HF > floor, strategy should compound normally."""
        strategy = _create_strategy()
        strategy._loop_state = "active"
        strategy._supplied_amount = Decimal("5")
        strategy._borrowed_amount = Decimal("37.50")
        # HF = (5 * 25 * 0.65) / (37.50 * 1) = 81.25 / 37.50 = 2.17 > 1.5
        market = _mock_market(wavax_price=Decimal("25"))

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.LP_COLLECT_FEES

    def test_unhealthy_position_deleverages(self):
        """With HF < floor, strategy should close LP to deleverage."""
        strategy = _create_strategy()
        strategy._loop_state = "active"
        strategy._supplied_amount = Decimal("5")
        strategy._borrowed_amount = Decimal("37.50")
        strategy._lp_bin_ids = [-1]
        # HF = (5 * 10 * 0.65) / (37.50 * 1) = 32.5 / 37.50 = 0.87 < 1.5
        market = _mock_market(wavax_price=Decimal("10"))

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.LP_CLOSE
        assert strategy._loop_state == "closing_lp"
        assert strategy._deleveraging is True

    def test_deleverage_lp_close_transitions_to_repay(self):
        """After deleverage LP_CLOSE, strategy should proceed to repay (not compound)."""
        strategy = _create_strategy()
        strategy._loop_state = "closing_lp"
        strategy._previous_stable_state = "active"
        strategy._deleveraging = True
        strategy._lp_bin_ids = [-1]

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_CLOSE"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        # Should go to deleverage_repaying (not lp_closed_for_compound or active)
        assert strategy._loop_state == "deleverage_repaying"
        assert strategy._deleveraging is False
        assert strategy._lp_bin_ids == []

    def test_deleverage_repay_emits_repay_intent(self):
        """In deleverage_repaying state, strategy should emit REPAY intent."""
        strategy = _create_strategy()
        strategy._loop_state = "deleverage_repaying"
        strategy._borrowed_amount = Decimal("37.50")
        strategy._supplied_amount = Decimal("5")
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.REPAY

    def test_deleverage_repay_success_transitions_to_deleveraged(self):
        """After successful REPAY in deleverage_withdrawing state, strategy transitions to deleveraged."""
        strategy = _create_strategy()
        # decide() transitions deleverage_repaying -> deleverage_withdrawing before emitting REPAY,
        # so on_intent_executed fires while in deleverage_withdrawing
        strategy._loop_state = "deleverage_withdrawing"
        strategy._borrowed_amount = Decimal("37.50")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "REPAY"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._loop_state == "deleveraged"
        assert strategy._borrowed_amount == Decimal("0")


# =============================================================================
# Failure Recovery
# =============================================================================


class TestFailureRecovery:
    """Test state machine recovery from failed intents."""

    def test_failed_intent_reverts_state(self):
        strategy = _create_strategy()
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "supplied"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "BORROW"

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._loop_state == "supplied"

    def test_stuck_transitional_state_reverts(self):
        strategy = _create_strategy()
        strategy._loop_state = "some_invalid_state"
        strategy._previous_stable_state = "active"
        market = _mock_market()

        intent = strategy.decide(market)

        # Should revert to active and hold
        assert strategy._loop_state == "active"


# =============================================================================
# State Persistence
# =============================================================================


class TestStatePersistence:
    """Test get_persistent_state / load_persistent_state round-trip."""

    def test_state_round_trip(self):
        strategy = _create_strategy()
        strategy._loop_state = "active"
        strategy._supplied_amount = Decimal("5")
        strategy._borrowed_amount = Decimal("37.50")
        strategy._lp_wavax = Decimal("1")
        strategy._lp_usdc = Decimal("37.50")
        strategy._compound_count = 3
        strategy._collected_fee_wavax = Decimal("0.1")

        state = strategy.get_persistent_state()

        strategy2 = _create_strategy()
        strategy2.load_persistent_state(state)

        assert strategy2._loop_state == "active"
        assert strategy2._supplied_amount == Decimal("5")
        assert strategy2._borrowed_amount == Decimal("37.50")
        assert strategy2._lp_wavax == Decimal("1")
        assert strategy2._lp_usdc == Decimal("37.50")
        assert strategy2._compound_count == 3
        assert strategy2._collected_fee_wavax == Decimal("0.1")


# =============================================================================
# Teardown
# =============================================================================


class TestTeardown:
    """Test teardown intent generation."""

    def test_teardown_with_all_positions(self):
        strategy = _create_strategy()
        strategy._loop_state = "active"
        strategy._supplied_amount = Decimal("5")
        strategy._borrowed_amount = Decimal("37.50")
        strategy._lp_bin_ids = [-1]

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 3
        assert intents[0].intent_type == IntentType.LP_CLOSE
        assert intents[1].intent_type == IntentType.REPAY
        assert intents[2].intent_type == IntentType.WITHDRAW

    def test_teardown_with_no_positions(self):
        strategy = _create_strategy()
        strategy._loop_state = "idle"

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 0

    def test_get_open_positions(self):
        strategy = _create_strategy()
        strategy._loop_state = "active"
        strategy._supplied_amount = Decimal("5")
        strategy._borrowed_amount = Decimal("37.50")
        strategy._lp_bin_ids = [-1]
        strategy._lp_usdc = Decimal("37.50")

        # Mock create_market_snapshot
        mock_market = _mock_market()
        strategy.create_market_snapshot = MagicMock(return_value=mock_market)

        summary = strategy.get_open_positions()

        assert len(summary.positions) == 3
        types = [p.position_type.value for p in summary.positions]
        assert "LP" in types
        assert "SUPPLY" in types
        assert "BORROW" in types
