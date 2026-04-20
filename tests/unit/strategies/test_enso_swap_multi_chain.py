"""Tests for Enso swap strategies on BSC, Avalanche, and Ethereum.

Parametrized tests validating the Enso aggregator swap lifecycle (BUY + SELL)
across 3 untested chains. All strategies share the same decision logic pattern.

Kitchen Loop iteration 120, VIB-1682.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

# Chain-specific strategy configurations
CHAIN_CONFIGS = [
    {
        "chain": "bnb",
        "module": "strategies.incubating.enso_swap_bsc.strategy",
        "class_name": "EnsoSwapBscStrategy",
        "base_token": "WBNB",
        "quote_token": "USDC",
        "strategy_name": "enso_swap_bsc",
        "position_id": "enso_bsc_wbnb",
    },
    {
        "chain": "avalanche",
        "module": "strategies.incubating.enso_swap_avalanche.strategy",
        "class_name": "EnsoSwapAvalancheStrategy",
        "base_token": "WAVAX",
        "quote_token": "USDC",
        "strategy_name": "enso_swap_avalanche",
        "position_id": "enso_avalanche_wavax",
    },
    {
        "chain": "ethereum",
        "module": "strategies.incubating.enso_swap_ethereum.strategy",
        "class_name": "EnsoSwapEthereumStrategy",
        "base_token": "WETH",
        "quote_token": "USDC",
        "strategy_name": "enso_swap_ethereum",
        "position_id": "enso_ethereum_weth",
    },
]


def _create_strategy(config):
    """Create a strategy instance without calling __init__."""
    import importlib

    mod = importlib.import_module(config["module"])
    cls = getattr(mod, config["class_name"])
    strat = cls.__new__(cls)
    strat.config = {}
    strat._chain = config["chain"]
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = f"test-{config['strategy_name']}"
    strat.trade_size_usd = Decimal("100")
    strat.max_slippage_pct = 1.0
    strat.base_token = config["base_token"]
    strat.quote_token = config["quote_token"]
    strat.force_action = None
    strat._iteration = 0
    strat._buy_executed = False
    strat._sell_executed = False
    return strat


@pytest.fixture(params=CHAIN_CONFIGS, ids=[c["chain"] for c in CHAIN_CONFIGS])
def strategy_config(request):
    return request.param


@pytest.fixture
def strategy(strategy_config):
    return _create_strategy(strategy_config)


class TestDecision:
    def test_first_iteration_buys(self, strategy, strategy_config):
        """First iteration creates a BUY swap intent."""
        market = MagicMock()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == strategy_config["quote_token"]
        assert intent.to_token == strategy_config["base_token"]
        assert intent.amount_usd == Decimal("100")
        assert intent.protocol == "enso"

    def test_second_iteration_sells(self, strategy, strategy_config):
        """Second iteration creates a SELL swap intent."""
        strategy._buy_executed = True
        market = MagicMock()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == strategy_config["base_token"]
        assert intent.to_token == strategy_config["quote_token"]

    def test_third_iteration_holds(self, strategy):
        """Third iteration holds after lifecycle complete."""
        strategy._buy_executed = True
        strategy._sell_executed = True
        market = MagicMock()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Lifecycle complete" in intent.reason

    def test_slippage_value(self, strategy):
        """Max slippage is 1% (100 bps)."""
        market = MagicMock()
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.01")

    def test_iteration_counter_increments(self, strategy):
        """Iteration counter increments each call."""
        market = MagicMock()
        strategy.decide(market)
        assert strategy._iteration == 1
        strategy.decide(market)
        assert strategy._iteration == 2


class TestOnIntentExecuted:
    def test_logs_swap_amounts(self, strategy):
        """Logs swap amounts from enriched result."""
        mock_intent = MagicMock()
        mock_result = MagicMock()
        mock_result.swap_amounts.amount_in = 100
        mock_result.swap_amounts.amount_out = 50000
        mock_result.swap_amounts.effective_price = 3000

        # Should not raise
        strategy.on_intent_executed(mock_intent, True, mock_result)

    def test_handles_failure(self, strategy):
        """Handles failed execution gracefully."""
        mock_intent = MagicMock()
        mock_result = MagicMock()
        mock_result.error = "test error"
        strategy.on_intent_executed(mock_intent, False, mock_result)


class TestStatePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._iteration = 2
        strategy._buy_executed = True
        strategy._sell_executed = True
        state = strategy.get_persistent_state()
        assert state == {
            "iteration": 2,
            "buy_executed": True,
            "sell_executed": True,
        }

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({
            "iteration": 5,
            "buy_executed": True,
            "sell_executed": False,
        })
        assert strategy._iteration == 5
        assert strategy._buy_executed is True
        assert strategy._sell_executed is False

    def test_load_empty_state(self, strategy):
        strategy.load_persistent_state({})
        assert strategy._iteration == 0
        assert strategy._buy_executed is False


class TestTeardown:
    def test_position_when_bought_not_sold(self, strategy, strategy_config):
        """Has open position after BUY but before SELL."""
        strategy._buy_executed = True
        strategy._sell_executed = False
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_id == strategy_config["position_id"]
        assert summary.positions[0].protocol == "enso"

    def test_no_position_before_buy(self, strategy):
        """No position before any trade."""
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_no_position_after_sell(self, strategy):
        """No position after lifecycle complete."""
        strategy._buy_executed = True
        strategy._sell_executed = True
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_teardown_intent_sells_all(self, strategy, strategy_config):
        """Teardown sells all base tokens."""
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].from_token == strategy_config["base_token"]
        assert intents[0].to_token == strategy_config["quote_token"]
        assert intents[0].protocol == "enso"

    def test_hard_teardown_wider_slippage(self, strategy):
        """Hard teardown uses 3% slippage."""
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents[0].max_slippage == Decimal("0.03")


class TestStatus:
    def test_get_status(self, strategy, strategy_config):
        status = strategy.get_status()
        assert status["strategy"] == strategy_config["strategy_name"]
        assert status["chain"] == strategy_config["chain"]
        assert status["buy_executed"] is False
        assert status["sell_executed"] is False
