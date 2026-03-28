"""Tests for the Velodrome V2 Swap Optimism demo strategy.

Validates BUY/SELL force_action decisions, balance checks, and teardown
with Velodrome V2 (Aerodrome connector) on Optimism.

Kitchen Loop iteration 134, VIB-1847.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.demo.velodrome_swap_optimism.strategy import VelodromeSwapOptimismStrategy

    strat = VelodromeSwapOptimismStrategy.__new__(VelodromeSwapOptimismStrategy)
    strat.config = {}
    strat._chain = "optimism"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-velodrome-swap-optimism"
    strat.swap_amount = Decimal("50")
    strat.max_slippage_pct = Decimal("1.0")
    strat.base_token = "WETH"
    strat.quote_token = "USDC"
    strat.force_action = "buy"
    return strat


def _mock_market(
    quote_usd: float = 10000.0,
    base_balance: float = 1.0,
    base_price: float = 3000.0,
    quote_price: float = 1.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "WETH":
            return Decimal(str(base_price))
        if token == "USDC":
            return Decimal(str(quote_price))
        raise ValueError(f"Unexpected token: {token}")

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "USDC":
            bal.balance_usd = Decimal(str(quote_usd))
            bal.balance = Decimal(str(quote_usd))
        elif token == "WETH":
            bal.balance_usd = Decimal(str(base_balance)) * Decimal(str(base_price))
            bal.balance = Decimal(str(base_balance))
        else:
            raise ValueError(f"Unexpected token: {token}")
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestBuyPhase:
    def test_buy_emits_swap_intent(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"

    def test_buy_uses_aerodrome_protocol(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "aerodrome"

    def test_buy_amount_usd_matches_config(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.amount_usd == Decimal("50")

    def test_buy_slippage_matches_config(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.01")

    def test_buy_hold_when_insufficient_quote(self, strategy):
        market = _mock_market(quote_usd=10.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient" in intent.reason

    def test_buy_proceeds_when_balance_unavailable(self, strategy):
        market = _mock_market()
        market.balance = MagicMock(side_effect=ValueError("balance unavailable"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"


class TestSellPhase:
    def test_sell_emits_swap_intent(self, strategy):
        strategy.force_action = "sell"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"

    def test_sell_uses_aerodrome_protocol(self, strategy):
        strategy.force_action = "sell"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "aerodrome"

    def test_sell_hold_when_insufficient_base(self, strategy):
        strategy.force_action = "sell"
        market = _mock_market(base_balance=0.001, base_price=3000.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient" in intent.reason

    def test_sell_proceeds_when_balance_unavailable(self, strategy):
        strategy.force_action = "sell"
        market = _mock_market()
        market.balance = MagicMock(side_effect=ValueError("balance unavailable"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"


class TestForceAction:
    def test_unknown_force_action_holds(self, strategy):
        strategy.force_action = "invalid"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Unknown" in intent.reason


class TestTeardown:
    def test_teardown_empty(self, strategy):
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0

    def test_teardown_intents_empty(self, strategy):
        intents = strategy.generate_teardown_intents()
        assert intents == []


class TestMetadata:
    def test_strategy_name(self):
        from strategies.demo.velodrome_swap_optimism.strategy import VelodromeSwapOptimismStrategy

        assert VelodromeSwapOptimismStrategy.STRATEGY_NAME == "demo_velodrome_swap_optimism"

    def test_supported_chains(self):
        from strategies.demo.velodrome_swap_optimism.strategy import VelodromeSwapOptimismStrategy

        assert "optimism" in VelodromeSwapOptimismStrategy.STRATEGY_METADATA.supported_chains

    def test_supported_protocols(self):
        from strategies.demo.velodrome_swap_optimism.strategy import VelodromeSwapOptimismStrategy

        assert "aerodrome" in VelodromeSwapOptimismStrategy.STRATEGY_METADATA.supported_protocols

    def test_default_chain(self):
        from strategies.demo.velodrome_swap_optimism.strategy import VelodromeSwapOptimismStrategy

        assert VelodromeSwapOptimismStrategy.STRATEGY_METADATA.default_chain == "optimism"


class TestCompilation:
    """Test that the Aerodrome compiler accepts Velodrome swap intents for Optimism."""

    def test_velodrome_swap_compiles_for_optimism(self):
        """Verify compiler auto-selects classic routing for Optimism (no CL contracts)."""
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
        from almanak.framework.intents.vocabulary import SwapIntent

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="optimism", config=config)
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("50"),
            max_slippage=Decimal("0.01"),
            protocol="aerodrome",
        )
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None

    def test_velodrome_sell_compiles_for_optimism(self):
        """Verify SELL direction also compiles on Optimism."""
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
        from almanak.framework.intents.vocabulary import SwapIntent

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="optimism", config=config)
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount_usd=Decimal("50"),
            max_slippage=Decimal("0.01"),
            protocol="aerodrome",
        )
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None

    def test_velodrome_protocol_alias_resolves(self):
        """Verify 'velodrome' protocol alias resolves to 'aerodrome' for Optimism."""
        from almanak.framework.connectors.protocol_aliases import normalize_protocol

        resolved = normalize_protocol("optimism", "velodrome")
        assert resolved == "aerodrome"
