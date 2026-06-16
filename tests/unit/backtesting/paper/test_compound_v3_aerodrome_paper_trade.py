"""Paper trading tests for Compound V3 + Aerodrome composed yield farm on Base.

First multi-protocol composed strategy tested through the paper trading pipeline.
Previous paper trade tests only covered single-protocol strategies (Aerodrome LP,
Compound V3 lending, Aave V3 lending, Curve swap).

Tests validate:
1. PaperTraderConfig creation for the composed strategy on Base
2. Multi-tick lifecycle simulation (BORROW -> LP_OPEN -> HOLD across ticks)
3. Cross-protocol state persistence between paper trading ticks
4. Teardown ordering in paper trading context
5. Position tracking across both protocols

Kitchen Loop iteration 145, VIB-2212.
"""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_strategy(
    collateral_amount: str = "0.05",
    ltv_target: str = "0.3",
    chain: str = "base",
) -> Any:
    """Create the composed strategy with mocked framework wiring."""
    from strategies.incubating.compound_v3_aerodrome_yield_farm_base.strategy import (
        CompoundV3AerodromeYieldFarmBaseStrategy,
    )

    with patch.object(CompoundV3AerodromeYieldFarmBaseStrategy, "__init__", lambda self, *a, **kw: None):
        strat = CompoundV3AerodromeYieldFarmBaseStrategy.__new__(CompoundV3AerodromeYieldFarmBaseStrategy)

    strat._deployment_id = "test-compound-aero-paper"
    strat._chain = chain
    strat._wallet_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    strat.config = {}
    strat.collateral_token = "WETH"
    strat.collateral_amount = Decimal(collateral_amount)
    strat.borrow_token = "USDC"
    strat.compound_market = "usdc"
    strat.ltv_target = Decimal(ltv_target)
    strat.lp_pool = "WETH/USDC"
    strat.lp_amount0_weth = Decimal("0.005")
    strat.lp_amount1_usdc = Decimal("10")
    strat._state = "idle"
    strat._previous_stable_state = "idle"
    strat._supplied_amount = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._lp_position_active = False
    strat._lp_position_id = None
    return strat


def _mock_market(
    eth_price: Decimal = Decimal("3000"),
) -> MagicMock:
    """Create a mock MarketSnapshot for paper trading ticks."""
    market = MagicMock()

    def price_fn(token: str) -> Decimal:
        prices = {"ETH": eth_price, "WETH": eth_price, "USDC": Decimal("1")}
        if token in prices:
            return prices[token]
        raise ValueError(f"No price for {token}")

    market.price = MagicMock(side_effect=price_fn)
    return market


# ---------------------------------------------------------------------------
# PaperTraderConfig tests
# ---------------------------------------------------------------------------


class TestPaperTraderConfig:
    """Test that PaperTraderConfig works for the composed strategy on Base."""

    def test_base_chain_config_creation(self):
        from almanak.framework.backtesting.paper.config import PaperTraderConfig

        config = PaperTraderConfig(
            chain="base",
            rpc_url="https://mainnet.base.org",
            deployment_id="compound_v3_aerodrome_yield_farm_base",
            initial_eth=Decimal("100"),
            initial_tokens={"USDC": Decimal("10000"), "WETH": Decimal("1")},
            max_ticks=10,
            tick_interval_seconds=60,
        )
        assert config.chain == "base"
        assert config.chain_id == 8453
        assert config.max_ticks == 10

    def test_config_initial_tokens_for_composed_strategy(self):
        from almanak.framework.backtesting.paper.config import PaperTraderConfig

        config = PaperTraderConfig(
            chain="base",
            rpc_url="https://mainnet.base.org",
            deployment_id="compound_v3_aerodrome_yield_farm_base",
            initial_eth=Decimal("100"),
            initial_tokens={
                "USDC": Decimal("10000"),
                "WETH": Decimal("1"),
            },
        )
        assert config.initial_tokens["USDC"] == Decimal("10000")
        assert config.initial_tokens["WETH"] == Decimal("1")

    def test_config_max_duration(self):
        from almanak.framework.backtesting.paper.config import PaperTraderConfig

        config = PaperTraderConfig(
            chain="base",
            rpc_url="https://mainnet.base.org",
            deployment_id="compound_v3_aerodrome_yield_farm_base",
            max_ticks=10,
            tick_interval_seconds=60,
        )
        assert config.max_duration_seconds == 600

    def test_config_fork_rpc_url(self):
        from almanak.framework.backtesting.paper.config import PaperTraderConfig

        config = PaperTraderConfig(
            chain="base",
            rpc_url="https://mainnet.base.org",
            deployment_id="compound_v3_aerodrome_yield_farm_base",
            anvil_port=8546,
        )
        assert config.fork_rpc_url == "http://localhost:8546"


# ---------------------------------------------------------------------------
# Multi-tick lifecycle simulation
# ---------------------------------------------------------------------------


class TestMultiTickLifecycle:
    """Simulate paper trading ticks: each tick calls decide() once.

    Tick 1: idle -> SUPPLY intent (transitional: supplying)
    On success: state -> supplied
    Tick 2: supplied -> BORROW intent (transitional: borrowing)
    On success: state -> borrowed
    Tick 3: borrowed -> LP_OPEN intent (transitional: opening_lp)
    On success: state -> complete
    Tick 4+: complete -> HOLD
    """

    def test_tick1_emits_supply(self):
        strat = _make_strategy()
        market = _mock_market()
        intent = strat.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert intent.protocol == "compound_v3"
        assert intent.use_as_collateral is True

    def test_tick1_supply_success_advances_state(self):
        strat = _make_strategy()
        strat._state = "supplying"

        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        intent.amount = Decimal("0.05")

        strat.on_intent_executed(intent, True, None)
        assert strat._state == "supplied"
        assert strat._supplied_amount == Decimal("0.05")

    def test_tick2_emits_borrow(self):
        strat = _make_strategy()
        strat._state = "supplied"
        market = _mock_market()
        intent = strat.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert intent.protocol == "compound_v3"
        # Collateral supplied by the standalone SUPPLY intent (VIB-3586)
        assert intent.collateral_amount == Decimal("0")

    def test_tick2_borrow_success_advances_state(self):
        strat = _make_strategy()
        strat._state = "borrowing"
        strat._supplied_amount = Decimal("0.05")  # Set by prior SUPPLY

        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("45.00")

        strat.on_intent_executed(intent, True, None)
        assert strat._state == "borrowed"
        assert strat._borrowed_amount == Decimal("45.00")
        assert strat._supplied_amount == Decimal("0.05")

    def test_tick3_emits_lp_open(self):
        strat = _make_strategy()
        strat._state = "borrowed"
        strat._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        intent = strat.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"
        assert intent.protocol == "aerodrome"

    def test_tick2_lp_open_success_completes(self):
        strat = _make_strategy()
        strat._state = "opening_lp"

        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"

        result = MagicMock()
        result.position_id = "98765"

        strat.on_intent_executed(intent, True, result)
        assert strat._state == "complete"
        assert strat._lp_position_active is True
        assert strat._lp_position_id == "98765"

    def test_tick3_holds_when_complete(self):
        strat = _make_strategy()
        strat._state = "complete"
        strat._supplied_amount = Decimal("0.05")
        strat._borrowed_amount = Decimal("45.00")
        strat._lp_position_active = True
        market = _mock_market()
        intent = strat.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_full_lifecycle_across_ticks(self):
        """Simulate the complete 4-tick lifecycle with state persistence."""
        strat = _make_strategy()
        market = _mock_market()

        # Tick 1: SUPPLY (standalone collateral leg, VIB-3586)
        intent0 = strat.decide(market)
        assert intent0.intent_type.value == "SUPPLY"

        # Simulate successful SUPPLY execution
        supply_intent = MagicMock()
        supply_intent.intent_type.value = "SUPPLY"
        supply_intent.amount = Decimal("0.05")
        strat.on_intent_executed(supply_intent, True, None)

        # Persist + restore between SUPPLY and BORROW ticks
        supplied_state = strat.get_persistent_state()
        strat_b = _make_strategy()
        strat_b.load_persistent_state(supplied_state)
        assert strat_b._state == "supplied"
        assert strat_b._supplied_amount == Decimal("0.05")

        # Tick 2: BORROW
        intent1 = strat_b.decide(market)
        assert intent1.intent_type.value == "BORROW"
        assert intent1.collateral_amount == Decimal("0")

        # Simulate successful BORROW execution
        borrow_intent = MagicMock()
        borrow_intent.intent_type.value = "BORROW"
        borrow_intent.borrow_amount = Decimal("45.00")
        strat_b.on_intent_executed(borrow_intent, True, None)

        # Save and restore state (simulates between-tick persistence)
        saved_state = strat_b.get_persistent_state()
        assert "previous_stable_state" in saved_state

        strat2 = _make_strategy()
        strat2.load_persistent_state(saved_state)
        assert strat2._state == "borrowed"
        assert strat2._borrowed_amount == Decimal("45.00")
        assert strat2._supplied_amount == Decimal("0.05")
        assert strat2._previous_stable_state == saved_state["previous_stable_state"]

        # Tick 3: LP_OPEN
        intent2 = strat2.decide(market)
        assert intent2.intent_type.value == "LP_OPEN"

        # Simulate successful LP_OPEN execution
        lp_intent = MagicMock()
        lp_intent.intent_type.value = "LP_OPEN"
        lp_result = MagicMock()
        lp_result.position_id = "42"
        strat2.on_intent_executed(lp_intent, True, lp_result)

        # Save and restore again
        saved_state2 = strat2.get_persistent_state()
        strat3 = _make_strategy()
        strat3.load_persistent_state(saved_state2)
        assert strat3._state == "complete"
        assert strat3._lp_position_active is True

        # Tick 4: HOLD
        intent3 = strat3.decide(market)
        assert intent3.intent_type.value == "HOLD"


# ---------------------------------------------------------------------------
# Cross-protocol state tracking
# ---------------------------------------------------------------------------


class TestCrossProtocolStateTracking:
    """Test that the strategy tracks positions across both protocols."""

    def test_supply_updates_compound_collateral(self):
        strat = _make_strategy()
        strat._state = "supplying"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        intent.amount = Decimal("0.05")
        strat.on_intent_executed(intent, True, None)

        assert strat._state == "supplied"
        assert strat._supplied_amount == strat.collateral_amount

    def test_borrow_updates_compound_state(self):
        strat = _make_strategy()
        strat._state = "borrowing"
        strat._supplied_amount = strat.collateral_amount  # Set by prior SUPPLY
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("45.00")
        strat.on_intent_executed(intent, True, None)

        assert strat._supplied_amount == strat.collateral_amount
        assert strat._borrowed_amount == Decimal("45.00")

    def test_lp_open_updates_aerodrome_state(self):
        strat = _make_strategy()
        strat._state = "opening_lp"
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        result = MagicMock()
        result.position_id = "12345"
        strat.on_intent_executed(intent, True, result)

        assert strat._lp_position_id == "12345"
        assert strat._lp_position_active is True
        assert strat._state == "complete"

    def test_persistent_state_captures_both_protocols(self):
        strat = _make_strategy()
        strat._state = "complete"
        strat._previous_stable_state = "borrowed"
        strat._supplied_amount = Decimal("0.05")
        strat._borrowed_amount = Decimal("45.00")
        strat._lp_position_active = True
        strat._lp_position_id = "12345"

        state = strat.get_persistent_state()

        assert state["state"] == "complete"
        assert state["previous_stable_state"] == "borrowed"
        assert Decimal(str(state["supplied_amount"])) == Decimal("0.05")
        assert Decimal(str(state["borrowed_amount"])) == Decimal("45.00")
        assert state["lp_position_active"] is True
        assert state["lp_position_id"] == "12345"


# ---------------------------------------------------------------------------
# Teardown in paper trading context
# ---------------------------------------------------------------------------


class TestTeardownInPaperTrading:
    """Test teardown ordering for paper trading mode.

    Paper trading must correctly teardown composed positions:
    1. LP_CLOSE (Aerodrome)
    2. REPAY (Compound V3)
    3. WITHDRAW (Compound V3)
    """

    def test_full_teardown_ordering(self):
        from almanak.framework.teardown import TeardownMode

        strat = _make_strategy()
        strat._supplied_amount = Decimal("0.05")
        strat._borrowed_amount = Decimal("45.00")
        strat._lp_position_active = True
        strat._lp_position_id = "12345"

        intents = strat.generate_teardown_intents(TeardownMode.SOFT)

        types = [i.intent_type.value for i in intents]
        assert types == ["LP_CLOSE", "REPAY", "WITHDRAW"]

    def test_teardown_protocols_are_correct(self):
        from almanak.framework.teardown import TeardownMode

        strat = _make_strategy()
        strat._supplied_amount = Decimal("0.05")
        strat._borrowed_amount = Decimal("45.00")
        strat._lp_position_active = True
        strat._lp_position_id = "12345"

        intents = strat.generate_teardown_intents(TeardownMode.SOFT)

        assert intents[0].protocol == "aerodrome"
        assert intents[1].protocol == "compound_v3"
        assert intents[2].protocol == "compound_v3"

    def test_teardown_without_lp(self):
        from almanak.framework.teardown import TeardownMode

        strat = _make_strategy()
        strat._supplied_amount = Decimal("0.05")
        strat._borrowed_amount = Decimal("45.00")
        strat._lp_position_active = False

        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        types = [i.intent_type.value for i in intents]
        assert "LP_CLOSE" not in types
        assert "REPAY" in types
        assert "WITHDRAW" in types

    def test_teardown_repay_full_for_interest(self):
        from almanak.framework.teardown import TeardownMode

        strat = _make_strategy()
        strat._borrowed_amount = Decimal("45.00")
        strat._supplied_amount = Decimal("0.05")

        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        repay_intents = [i for i in intents if i.intent_type.value == "REPAY"]
        assert len(repay_intents) == 1
        assert repay_intents[0].repay_full is True


# ---------------------------------------------------------------------------
# Open positions reporting
# ---------------------------------------------------------------------------


class TestOpenPositionsForPaperTrading:
    """Test position reporting -- paper trader uses this for valuation."""

    def test_reports_compound_positions(self):
        strat = _make_strategy()
        strat._supplied_amount = Decimal("0.05")
        strat._borrowed_amount = Decimal("45.00")
        strat._lp_position_active = False

        mock_market = MagicMock()
        mock_market.price.return_value = Decimal("3000")
        strat.create_market_snapshot = MagicMock(return_value=mock_market)

        summary = strat.get_open_positions()
        types = [p.position_type.value for p in summary.positions]
        assert "SUPPLY" in types
        assert "BORROW" in types
        protocols = [p.protocol for p in summary.positions]
        assert "compound_v3" in protocols

    def test_reports_aerodrome_lp_position(self):
        strat = _make_strategy()
        strat._supplied_amount = Decimal("0.05")
        strat._borrowed_amount = Decimal("45.00")
        strat._lp_position_active = True
        strat._lp_position_id = "12345"

        mock_market = MagicMock()
        mock_market.price.return_value = Decimal("3000")
        strat.create_market_snapshot = MagicMock(return_value=mock_market)

        summary = strat.get_open_positions()
        types = [p.position_type.value for p in summary.positions]
        assert "LP" in types
        protocols = [p.protocol for p in summary.positions]
        assert "aerodrome" in protocols
        assert "compound_v3" in protocols


# ---------------------------------------------------------------------------
# Failure recovery in paper trading
# ---------------------------------------------------------------------------


class TestFailureRecoveryInPaperTrading:
    """Test that execution failures during paper trading revert correctly."""

    def test_borrow_failure_reverts_to_idle(self):
        strat = _make_strategy()
        strat._state = "borrowing"
        strat._previous_stable_state = "idle"

        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        strat.on_intent_executed(intent, False, None)

        assert strat._state == "idle"

    def test_lp_open_failure_reverts_to_borrowed(self):
        strat = _make_strategy()
        strat._state = "opening_lp"
        strat._previous_stable_state = "borrowed"

        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strat.on_intent_executed(intent, False, None)

        assert strat._state == "borrowed"

    def test_failure_preserves_prior_positions(self):
        """If LP_OPEN fails, the BORROW position must still be tracked."""
        strat = _make_strategy()
        strat._state = "opening_lp"
        strat._previous_stable_state = "borrowed"
        strat._supplied_amount = Decimal("0.05")
        strat._borrowed_amount = Decimal("45.00")

        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strat.on_intent_executed(intent, False, None)

        assert strat._supplied_amount == Decimal("0.05")
        assert strat._borrowed_amount == Decimal("45.00")
        assert strat._lp_position_active is False
        assert strat._lp_position_id is None
