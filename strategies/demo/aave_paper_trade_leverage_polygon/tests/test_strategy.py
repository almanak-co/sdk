"""Unit tests for Aave V3 Paper Trade Leverage Loop on Polygon.

Tests the three-phase leverage loop (supply -> borrow -> swap) and
state machine transitions including failure recovery.

To run:
    uv run pytest strategies/demo/aave_paper_trade_leverage_polygon/tests/ -v
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from strategies.demo.aave_paper_trade_leverage_polygon import AavePaperTradeLeveragePolygonStrategy


def create_strategy(config: dict | None = None) -> AavePaperTradeLeveragePolygonStrategy:
    """Create strategy with mocked __init__."""
    with patch.object(AavePaperTradeLeveragePolygonStrategy, "__init__", lambda self, *a, **kw: None):
        s = AavePaperTradeLeveragePolygonStrategy.__new__(AavePaperTradeLeveragePolygonStrategy)

    defaults = {
        "collateral_token": "USDC",
        "collateral_amount": "500",
        "borrow_token": "WETH",
        "swap_to_token": "WMATIC",
        "ltv_target": "0.3",
        "swap_protocol": "uniswap_v3",
    }
    if config:
        defaults.update(config)

    s.config = defaults
    s._chain = "polygon"
    s._wallet_address = "0x" + "11" * 20
    s._strategy_id = "test-paper-leverage-polygon"

    s.collateral_token = defaults["collateral_token"]
    s.collateral_amount = Decimal(str(defaults["collateral_amount"]))
    s.borrow_token = defaults["borrow_token"]
    s.swap_to_token = defaults["swap_to_token"]
    s.ltv_target = Decimal(str(defaults["ltv_target"]))
    s.swap_protocol = defaults["swap_protocol"]

    s._state = "idle"
    s._previous_stable_state = "idle"
    s._supplied_amount = Decimal("0")
    s._borrowed_amount = Decimal("0")
    s._swapped_amount = Decimal("0")

    return s


def make_market(usdc_price: Decimal = Decimal("1"), weth_price: Decimal = Decimal("2000")) -> MagicMock:
    """Create a mock market with given prices."""
    market = MagicMock()

    def mock_price(token: str) -> Decimal:
        prices = {"USDC": usdc_price, "WETH": weth_price, "WMATIC": Decimal("0.5")}
        return prices.get(token, Decimal("1"))

    market.price = mock_price
    return market


@pytest.fixture
def strategy() -> AavePaperTradeLeveragePolygonStrategy:
    return create_strategy()


class TestPhase1Supply:
    """Phase 1: Supply USDC collateral."""

    def test_idle_supplies(self, strategy):
        intent = strategy.decide(make_market())
        assert intent.intent_type.value == "SUPPLY"
        assert intent.protocol == "aave_v3"
        assert strategy._state == "supplying"

    def test_supply_callback_advances(self, strategy):
        strategy._state = "supplying"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("500")


class TestPhase2Borrow:
    """Phase 2: Borrow WETH against collateral."""

    def test_supplied_borrows(self, strategy):
        strategy._state = "supplied"
        intent = strategy.decide(make_market())
        assert intent.intent_type.value == "BORROW"
        assert strategy._state == "borrowing"

    def test_borrow_amount_calculation(self, strategy):
        """Borrow amount = collateral_value * ltv_target / borrow_price."""
        strategy._state = "supplied"
        # 500 USDC * 1 = $500, * 0.3 LTV = $150, / $2000 WETH = 0.075
        intent = strategy.decide(make_market())
        assert intent.borrow_amount == Decimal("0.0750")
        # Collateral already supplied in Phase 1, borrow must not re-supply
        assert intent.collateral_amount == Decimal("0")

    def test_borrow_callback_advances(self, strategy):
        strategy._state = "borrowing"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("0.075")
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "borrowed"
        assert strategy._borrowed_amount == Decimal("0.075")


class TestPhase3Swap:
    """Phase 3: Swap WETH -> WMATIC."""

    def test_borrowed_swaps(self, strategy):
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("0.075")
        intent = strategy.decide(make_market())
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "WMATIC"
        assert strategy._state == "swapping"

    def test_borrowed_zero_amount_holds(self, strategy):
        """Cannot swap if borrow amount is zero."""
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("0")
        intent = strategy.decide(make_market())
        assert intent.intent_type.value == "HOLD"

    def test_swap_callback_completes(self, strategy):
        strategy._state = "swapping"
        strategy._borrowed_amount = Decimal("0.075")
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        result = MagicMock()
        result.swap_amounts = MagicMock()
        result.swap_amounts.amount_out_decimal = Decimal("300")
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._state == "complete"
        assert strategy._swapped_amount == Decimal("300")


class TestStateMachine:
    """State machine edge cases."""

    def test_complete_holds(self, strategy):
        strategy._state = "complete"
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.075")
        strategy._swapped_amount = Decimal("300")
        intent = strategy.decide(make_market())
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()

    def test_transitional_holds_waiting(self, strategy):
        """In-flight transitional state holds instead of replaying."""
        strategy._state = "borrowing"
        strategy._previous_stable_state = "supplied"
        intent = strategy.decide(make_market())
        assert intent.intent_type.value == "HOLD"
        assert "borrowing" in intent.reason.lower()

    def test_failure_reverts_state(self, strategy):
        strategy._state = "swapping"
        strategy._previous_stable_state = "borrowed"
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "borrowed"

    def test_persistent_state_roundtrip(self, strategy):
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.075")
        state = strategy.get_persistent_state()

        strategy2 = create_strategy()
        strategy2.load_persistent_state(state)
        assert strategy2._state == "borrowed"
        assert strategy2._supplied_amount == Decimal("500")
        assert strategy2._borrowed_amount == Decimal("0.075")


class TestTeardown:
    """Teardown generates correct unwind intents."""

    def test_full_teardown(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.075")
        strategy._swapped_amount = Decimal("300")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 3
        # Order: swap back, repay, withdraw
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "WMATIC"
        assert intents[1].intent_type.value == "REPAY"
        assert intents[2].intent_type.value == "WITHDRAW"

    def test_teardown_no_positions(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_open_positions_reports(self, strategy):
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.075")
        strategy._swapped_amount = Decimal("300")
        strategy.STRATEGY_NAME = "demo_aave_paper_trade_leverage_polygon"
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 3
        types = {p.position_type.value for p in summary.positions}
        assert "SUPPLY" in types
        assert "BORROW" in types
        assert "TOKEN" in types

    def test_open_positions_borrow_has_value(self, strategy):
        """Borrow position reports non-zero value for accurate exposure tracking."""
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.075")
        strategy.STRATEGY_NAME = "demo_aave_paper_trade_leverage_polygon"
        summary = strategy.get_open_positions()
        borrow_pos = [p for p in summary.positions if p.position_type.value == "BORROW"][0]
        assert borrow_pos.value_usd == Decimal("0.075")

    def test_teardown_callbacks_clear_amounts(self, strategy):
        """Teardown execution clears position tracking amounts."""
        strategy._state = "complete"
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.075")
        strategy._swapped_amount = Decimal("300")

        # Teardown reverse swap clears swapped amount
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._swapped_amount == Decimal("0")

        # Repay clears borrowed amount
        intent.intent_type.value = "REPAY"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._borrowed_amount == Decimal("0")

        # Withdraw clears supplied amount
        intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._supplied_amount == Decimal("0")
