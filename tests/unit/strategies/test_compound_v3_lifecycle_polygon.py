"""USDC.e-market coverage for the consolidated Compound V3 lifecycle strategy.

The Polygon Compound V3 base asset is bridged USDC.e (not native USDC), selected
via ``market="usdc_e"`` / ``borrow_token="USDC.e"``. This locks in that the SAME
consolidated ``CompoundV3LifecycleStrategy`` drives the USDC.e Comet correctly from
config alone — the consolidation's key generalization (also proven on-chain, e2e
PASS on the Polygon USDC.e Comet).

Promoted (focused) from the former ``compound_v3_lifecycle_polygon`` co-located
test when that per-chain clone folded into the consolidated strategy. The clone's
richer outlier behaviors (force_action branching, stuck-state revert, a teardown
consolidation SWAP) were intentionally dropped in favor of the clean base canonical
(the 4-of-5 majority shape); the lifecycle + USDC.e routing — the load-bearing
coverage — is preserved here.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

_POLYGON_CONFIG = {
    "chain": "polygon",
    "collateral_token": "WETH",
    "collateral_amount": "0.5",
    "borrow_token": "USDC.e",
    "ltv_target": 0.3,
    "market": "usdc_e",
}


@pytest.fixture
def mock_market():
    market = MagicMock()
    market.price = MagicMock(side_effect=lambda t: {"WETH": Decimal("2500"), "USDC.e": Decimal("1")}.get(t, Decimal("0")))
    return market


@pytest.fixture
def strategy():
    from strategies.incubating.compound_v3_lifecycle.strategy import CompoundV3LifecycleStrategy

    s = CompoundV3LifecycleStrategy.__new__(CompoundV3LifecycleStrategy)
    s.config = _POLYGON_CONFIG
    s._config = _POLYGON_CONFIG
    s._chain = "polygon"
    s._wallet_address = "0x" + "1" * 40
    s._deployment_id = "compound_v3_lifecycle_polygon"
    s.collateral_token = "WETH"
    s.collateral_amount = Decimal("0.5")
    s.borrow_token = "USDC.e"
    s.ltv_target = Decimal("0.3")
    s.market = "usdc_e"
    s._loop_state = "idle"
    s._previous_stable_state = "idle"
    s._collateral_supplied = Decimal("0")
    s._borrowed_amount = Decimal("0")
    return s


class TestUsdcEMarketRouting:
    def test_supply_routes_to_usdc_e_comet(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "SUPPLY"
        assert intent.token == "WETH"
        assert intent.use_as_collateral is True
        assert intent.market_id == "usdc_e"
        assert intent.chain == "polygon"

    def test_borrow_is_usdc_e_and_standalone(self, strategy):
        strategy._loop_state = "supplied"
        intent = strategy._create_borrow_intent(Decimal("2500"), Decimal("1"))
        assert intent.intent_type.value == "BORROW"
        assert intent.borrow_token == "USDC.e"
        assert intent.collateral_amount == Decimal("0")  # VIB-3586: supplied separately
        # 0.5 WETH * $2500 * 0.3 / $1 = $375
        assert intent.borrow_amount == Decimal("375.00")
        assert intent.market_id == "usdc_e"

    def test_repay_targets_usdc_e_market(self, strategy):
        strategy._borrowed_amount = Decimal("375")
        intent = strategy._create_repay_intent()
        assert intent.intent_type.value == "REPAY"
        assert intent.token == "USDC.e"
        assert intent.repay_full is True
        assert intent.market_id == "usdc_e"

    def test_withdraw_returns_collateral_from_usdc_e_market(self, strategy):
        strategy._collateral_supplied = Decimal("0.5")
        intent = strategy._create_withdraw_intent()
        assert intent.intent_type.value == "WITHDRAW"
        assert intent.token == "WETH"
        assert intent.withdraw_all is True
        assert intent.market_id == "usdc_e"


class TestUsdcELifecycle:
    def test_full_lifecycle_on_usdc_e(self, strategy, mock_market):
        def step(it):
            ev = MagicMock()
            ev.intent_type.value = it.intent_type.value
            ev.amount = getattr(it, "amount", None)
            ev.borrow_amount = getattr(it, "borrow_amount", None)
            strategy.on_intent_executed(ev, True, None)

        i = strategy.decide(mock_market)
        assert i.intent_type.value == "SUPPLY"
        step(i)
        assert strategy._loop_state == "supplied"

        i = strategy.decide(mock_market)
        assert i.intent_type.value == "BORROW"
        step(i)
        assert strategy._loop_state == "borrowed"

        i = strategy.decide(mock_market)
        assert i.intent_type.value == "REPAY"
        step(i)
        assert strategy._loop_state == "repaid"

        i = strategy.decide(mock_market)
        assert i.intent_type.value == "WITHDRAW"
        step(i)
        assert strategy._loop_state == "complete"

        assert strategy.decide(mock_market).intent_type.value == "HOLD"

    def test_teardown_repay_then_withdraw(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._collateral_supplied = Decimal("0.5")
        strategy._borrowed_amount = Decimal("375")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert [i.intent_type.value for i in intents] == ["REPAY", "WITHDRAW"]
        assert intents[0].market_id == "usdc_e"


class TestStatus:
    def test_status_reports_usdc_e_market(self, strategy):
        s = strategy.get_status()
        assert s["strategy"] == "compound_v3_lifecycle"
        assert s["chain"] == "polygon"
        assert s["market"] == "usdc_e"
