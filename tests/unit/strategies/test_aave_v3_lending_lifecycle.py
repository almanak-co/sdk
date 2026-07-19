"""Clean-path unit tests for the consolidated chain-generic Aave V3 lending strategy.

Covers the lifecycle state machine, intent generation, on-execution advancement,
teardown, and price handling for ``AaveV3LendingStrategy``
(``strategies/incubating/aave_v3_lending``) with ``check_frozen_reserve`` OFF — the
default clean lifecycle path. The frozen-reserve guard (ON) is covered by
``test_aave_v3_lending_linea.py`` / ``test_aave_v3_lending_mantle.py``.

Promoted from the former ``aave_v3_lending_sonic_weth`` co-located test (which was
outside CI testpaths) when that per-chain clone folded into the consolidated
strategy. Kitchen Loop iteration 153 (VIB-2366) lineage; sonic chain retained so the
chain-registry assertions stay meaningful.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch  # noqa: F401 - patch used in some tests

import pytest

_AAVE_MODULE = "strategies.incubating.aave_v3_lending.strategy"


@pytest.fixture
def strategy_config():
    """Clean (frozen-guard-off) config: USDC collateral -> WETH borrow on Sonic."""
    return {
        "deployment_id": "aave_v3_lending_sonic",
        "strategy_name": "aave_v3_lending",
        "collateral_token": "USDC",
        "collateral_amount": "500",
        "borrow_token": "WETH",
        "ltv_target": 0.3,
        "force_action": "lifecycle",
        "chain": "sonic",
        "check_frozen_reserve": False,
    }


@pytest.fixture
def mock_market():
    market = MagicMock()

    def price_side_effect(token):
        return {"WETH": Decimal("2500"), "USDC": Decimal("1")}.get(token, Decimal("0"))

    market.price = MagicMock(side_effect=price_side_effect)
    return market


@pytest.fixture
def strategy(strategy_config):
    """Consolidated strategy instance with mocked framework dependencies."""
    from strategies.incubating.aave_v3_lending.strategy import AaveV3LendingStrategy

    instance = AaveV3LendingStrategy.__new__(AaveV3LendingStrategy)
    instance.config = strategy_config
    instance._config = strategy_config
    instance._state_manager = MagicMock()
    instance._deployment_id = "aave_v3_lending_sonic"
    instance._chain = "sonic"
    instance._wallet_address = "0x1234000000000000000000000000000000000001"

    instance.collateral_token = "USDC"
    instance.collateral_amount = Decimal("500")
    instance.borrow_token = "WETH"
    instance.ltv_target = Decimal("0.3")
    instance.force_action = "lifecycle"
    instance._check_frozen_reserve = False
    instance._lifecycle_stop_after = None
    instance._frozen_detected = False
    instance._state = "idle"
    instance._previous_stable_state = "idle"
    instance._supplied_amount = Decimal("0")
    instance._borrowed_amount = Decimal("0")
    instance._failure_details = []

    return instance


class TestStateTransitions:
    def test_idle_to_supplying(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_supplied_to_borrowing(self, strategy, mock_market):
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("500")
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "BORROW"
        assert strategy._state == "borrowing"

    def test_borrowed_to_repaying(self, strategy, mock_market):
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("0.05")
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "REPAY"
        assert strategy._state == "repaying"

    def test_repaid_to_withdrawing(self, strategy, mock_market):
        strategy._state = "repaid"
        strategy._supplied_amount = Decimal("500")
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._state == "withdrawing"

    def test_complete_holds(self, strategy, mock_market):
        strategy._state = "complete"
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "HOLD"

    def test_stuck_transitional_holds(self, strategy, mock_market):
        # Canonical (base/mantle-majority) behavior: a transitional state HOLDs
        # and waits for the in-flight intent to resolve (it does NOT auto-revert
        # to the previous stable state — that was the sonic variant's outlier
        # behavior, dropped on consolidation in favor of the dominant shape).
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "supplying"


class TestIntentGeneration:
    def test_supply_intent_uses_collateral(self, strategy):
        intent = strategy._create_supply_intent()
        assert intent.intent_type.value == "SUPPLY"
        assert intent.token == "USDC"
        assert intent.amount == Decimal("500")
        assert intent.protocol == "aave_v3"

    def test_borrow_intent_uses_ltv_target(self, strategy):
        # collateral_value = 500 USDC * $1 = $500; borrow_value = $500 * 0.3 = $150;
        # borrow_amount = $150 / $2500 = 0.06 WETH
        intent = strategy._create_borrow_intent(Decimal("1"), Decimal("2500"))
        assert intent.intent_type.value == "BORROW"
        assert intent.borrow_amount == Decimal("0.06")
        assert intent.borrow_token == "WETH"
        assert intent.collateral_token == "USDC"

    def test_repay_intent_uses_full_repay(self, strategy):
        strategy._borrowed_amount = Decimal("0.05")
        intent = strategy._create_repay_intent()
        assert intent.intent_type.value == "REPAY"
        assert intent.repay_full is True
        assert intent.token == "WETH"

    def test_withdraw_intent_uses_withdraw_all(self, strategy):
        strategy._supplied_amount = Decimal("500")
        intent = strategy._create_withdraw_intent()
        assert intent.intent_type.value == "WITHDRAW"
        assert intent.withdraw_all is True
        assert intent.token == "USDC"

    def test_all_intents_target_configured_chain(self, strategy):
        supply = strategy._create_supply_intent()
        borrow = strategy._create_borrow_intent(Decimal("1"), Decimal("2500"))
        repay = strategy._create_repay_intent()
        withdraw = strategy._create_withdraw_intent()
        for intent in [supply, borrow, repay, withdraw]:
            assert intent.chain == "sonic"


class TestOnIntentExecuted:
    def test_supply_success_advances_state(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy._state = "supplying"
        with patch(f"{_AAVE_MODULE}.add_event"):
            strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("500")

    def test_borrow_success_advances_state(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("0.05")
        strategy._state = "borrowing"
        with patch(f"{_AAVE_MODULE}.add_event"):
            strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "borrowed"
        assert strategy._borrowed_amount == Decimal("0.05")

    def test_repay_success_clears_debt(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "REPAY"
        strategy._state = "repaying"
        strategy._borrowed_amount = Decimal("0.05")
        with patch(f"{_AAVE_MODULE}.add_event"):
            strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_success_completes_lifecycle(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "WITHDRAW"
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("500")
        with patch(f"{_AAVE_MODULE}.add_event"):
            strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "complete"
        assert strategy._supplied_amount == Decimal("0")

    def test_failure_reverts_to_previous_state(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "idle"


class TestTeardown:
    def test_teardown_from_borrowed_state(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("0.05")
        strategy._supplied_amount = Decimal("500")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_from_supplied_state(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_teardown_from_idle_no_intents(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "idle"
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_get_open_positions_from_borrowed(self, strategy):
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.05")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 2
        position_types = {p.position_type.value for p in summary.positions}
        assert "SUPPLY" in position_types

    def test_get_open_positions_from_idle(self, strategy):
        strategy._state = "idle"
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0


class TestPriceHandling:
    def test_zero_price_returns_hold(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("No price"))
        # IDLE needs no prices -> SUPPLY proceeds even during a price outage.
        assert strategy.decide(market).intent_type.value == "SUPPLY"
        # Only the BORROW step needs prices -> HOLD on outage in the SUPPLIED state.
        strategy._state = "supplied"
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_force_supply_skips_price_check(self, strategy):
        strategy.force_action = "supply"
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("No price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"


class TestSonicChainRegistry:
    """Chain-coverage assertions (independent of the strategy class)."""

    def test_weth_address_in_token_registry(self):
        from almanak.framework.data.tokens import get_token_resolver

        token = get_token_resolver().resolve("WETH", "sonic")
        assert token is not None
        assert token.address.lower() == "0x50c42deacd8fc9773493ed674b675be577f2634b"

    def test_usdc_address_in_token_registry(self):
        from almanak.framework.data.tokens import get_token_resolver

        token = get_token_resolver().resolve("USDC", "sonic")
        assert token is not None
        assert token.address.lower() == "0x29219dd400f2bf60e5a23d13be72b486d4038894"

    def test_aave_v3_pool_configured_for_sonic(self):
        from almanak.connectors.aave_v3.addresses import AAVE_V3

        assert "sonic" in AAVE_V3
        assert "pool" in AAVE_V3["sonic"]
        assert AAVE_V3["sonic"]["pool"].startswith("0x")

    def test_aave_v3_oracle_configured_for_sonic(self):
        from almanak.connectors.aave_v3.addresses import AAVE_V3

        assert "oracle" in AAVE_V3["sonic"]
        assert AAVE_V3["sonic"]["oracle"].startswith("0x")
