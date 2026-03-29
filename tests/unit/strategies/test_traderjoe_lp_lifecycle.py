"""Tests for the TraderJoe V2 LP Lifecycle demo strategy.

Validates LP_OPEN and LP_CLOSE force_action decisions, balance checks,
teardown, and compilation on TraderJoe V2 Liquidity Book (Avalanche).

Kitchen Loop iteration 134, VIB-195.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.demo.traderjoe_lp_lifecycle.strategy import (
        LPLifecycleConfig,
        TraderJoeLPLifecycleStrategy,
    )

    strat = TraderJoeLPLifecycleStrategy.__new__(TraderJoeLPLifecycleStrategy)
    strat.config = LPLifecycleConfig()
    strat._chain = "avalanche"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-traderjoe-lp-lifecycle"
    strat.pool = "WAVAX/USDC/20"
    strat.token_x = "WAVAX"
    strat.token_y = "USDC"
    strat.bin_step = 20
    strat.range_width_pct = Decimal("0.10")
    strat.amount_x = Decimal("0.001")
    strat.amount_y = Decimal("3")
    strat.force_action = "open"
    strat._position_bin_ids = []
    strat._last_x_price = None
    return strat


def _mock_market(
    x_balance: float = 100.0,
    y_balance: float = 10000.0,
    x_price: float = 25.0,
    y_price: float = 1.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "WAVAX":
            return Decimal(str(x_price))
        if token == "USDC":
            return Decimal(str(y_price))
        raise ValueError(f"Unexpected token: {token}")

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "WAVAX":
            bal.balance = Decimal(str(x_balance))
            bal.balance_usd = Decimal(str(x_balance)) * Decimal(str(x_price))
        elif token == "USDC":
            bal.balance = Decimal(str(y_balance))
            bal.balance_usd = Decimal(str(y_balance)) * Decimal(str(y_price))
        else:
            raise ValueError(f"Unexpected token: {token}")
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestOpenPhase:
    def test_open_emits_lp_open_intent(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

    def test_open_uses_traderjoe_v2_protocol(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "traderjoe_v2"

    def test_open_pool_matches_config(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.pool == "WAVAX/USDC/20"

    def test_open_amounts_match_config(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.amount0 == Decimal("0.001")
        assert intent.amount1 == Decimal("3")

    def test_open_range_calculated_from_price(self, strategy):
        market = _mock_market(x_price=25.0, y_price=1.0)
        intent = strategy.decide(market)
        # Current price = 25/1 = 25, range_width = 10%, so ±5% => [23.75, 26.25]
        assert intent.range_lower == Decimal("23.75")
        assert intent.range_upper == Decimal("26.25")

    def test_open_hold_when_insufficient_token_x(self, strategy):
        market = _mock_market(x_balance=0.0001)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient" in intent.reason

    def test_open_hold_when_insufficient_token_y(self, strategy):
        market = _mock_market(y_balance=1.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient" in intent.reason

    def test_open_hold_when_no_price(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("no price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "price" in intent.reason.lower()


class TestClosePhase:
    def test_close_emits_lp_close_intent(self, strategy):
        strategy.force_action = "close"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_close_uses_traderjoe_v2_protocol(self, strategy):
        strategy.force_action = "close"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "traderjoe_v2"

    def test_close_pool_matches_config(self, strategy):
        strategy.force_action = "close"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.pool == "WAVAX/USDC/20"

    def test_close_collects_fees(self, strategy):
        strategy.force_action = "close"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.collect_fees is True

    def test_close_position_id_is_pool(self, strategy):
        strategy.force_action = "close"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.position_id == "WAVAX/USDC/20"


class TestForceAction:
    def test_unknown_force_action_holds(self, strategy):
        strategy.force_action = "invalid"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Unknown" in intent.reason


class TestOnIntentExecuted:
    def test_open_success_stores_bin_ids(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        result = MagicMock()
        result.bin_ids = [8388600, 8388601, 8388602]
        strategy.on_intent_executed(intent, True, result)
        assert strategy._position_bin_ids == [8388600, 8388601, 8388602]

    def test_close_success_clears_bin_ids(self, strategy):
        strategy._position_bin_ids = [8388600, 8388601]
        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"
        strategy.on_intent_executed(intent, True, MagicMock())
        assert strategy._position_bin_ids == []

    def test_failure_does_not_modify_state(self, strategy):
        strategy._position_bin_ids = [8388600]
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(intent, False, MagicMock())
        assert strategy._position_bin_ids == [8388600]


class TestTeardown:
    def test_teardown_empty_when_no_position(self, strategy):
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0

    def test_teardown_has_position_when_bin_ids_exist(self, strategy):
        strategy._position_bin_ids = [8388600, 8388601]
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].protocol == "traderjoe_v2"

    def test_teardown_intents_empty_when_no_position(self, strategy):
        intents = strategy.generate_teardown_intents()
        assert intents == []

    def test_teardown_intents_close_when_position_exists(self, strategy):
        strategy._position_bin_ids = [8388600, 8388601]
        intents = strategy.generate_teardown_intents()
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"


class TestMetadata:
    def test_strategy_name(self):
        from strategies.demo.traderjoe_lp_lifecycle.strategy import TraderJoeLPLifecycleStrategy

        assert TraderJoeLPLifecycleStrategy.STRATEGY_NAME == "demo_traderjoe_lp_lifecycle"

    def test_supported_chains(self):
        from strategies.demo.traderjoe_lp_lifecycle.strategy import TraderJoeLPLifecycleStrategy

        assert "avalanche" in TraderJoeLPLifecycleStrategy.STRATEGY_METADATA.supported_chains

    def test_supported_protocols(self):
        from strategies.demo.traderjoe_lp_lifecycle.strategy import TraderJoeLPLifecycleStrategy

        assert "traderjoe_v2" in TraderJoeLPLifecycleStrategy.STRATEGY_METADATA.supported_protocols

    def test_intent_types(self):
        from strategies.demo.traderjoe_lp_lifecycle.strategy import TraderJoeLPLifecycleStrategy

        types = TraderJoeLPLifecycleStrategy.STRATEGY_METADATA.intent_types
        assert "LP_OPEN" in types
        assert "LP_CLOSE" in types
        assert "HOLD" in types


class TestCompilation:
    """Test that the TraderJoe V2 compiler accepts LP_OPEN and LP_CLOSE intents."""

    def test_lp_open_compiles_for_avalanche(self):
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
        from almanak.framework.intents.vocabulary import LPOpenIntent

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="avalanche", config=config)
        intent = LPOpenIntent(
            pool="WAVAX/USDC/20",
            amount0=Decimal("0.001"),
            amount1=Decimal("3"),
            range_lower=Decimal("23.75"),
            range_upper=Decimal("26.25"),
            protocol="traderjoe_v2",
        )
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"LP_OPEN compilation failed: {result.error}"
        assert result.action_bundle is not None

    def test_lp_close_compiles_for_avalanche(self):
        """Verify LP_CLOSE dispatches to TraderJoe V2 compiler path.

        Mocks the internal compilation method to avoid on-chain RPC dependency.
        """
        from unittest.mock import patch

        from almanak.framework.intents.compiler import (
            CompilationResult,
            CompilationStatus,
            IntentCompiler,
            IntentCompilerConfig,
        )
        from almanak.framework.intents.vocabulary import LPCloseIntent

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="avalanche", config=config)
        intent = LPCloseIntent(
            position_id="WAVAX/USDC/20",
            pool="WAVAX/USDC/20",
            collect_fees=True,
            protocol="traderjoe_v2",
        )

        mock_result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        with patch.object(compiler, "_compile_lp_close_traderjoe_v2", return_value=mock_result) as mock_method:
            result = compiler.compile(intent)
            mock_method.assert_called_once_with(intent)
        assert result.status.value == "SUCCESS"
