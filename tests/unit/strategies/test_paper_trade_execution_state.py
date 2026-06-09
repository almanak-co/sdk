"""Execution-state regression tests for the RSI paper-trade demos.

Both ``demo_uniswap_paper_trade_optimism`` and ``demo_pancakeswap_paper_trade_bsc``
persist ``holding_base`` and the buy/sell counters across restarts. Those fields
are execution-derived and MUST only be set on a successful swap -- never
optimistically in ``decide()``. Setting them in ``decide()`` (the prior
behaviour) meant a failed or held-back swap left a phantom ``holding_base``
that, once persisted and restored, drove teardown to emit an ``amount="all"``
sell of a position the strategy never actually acquired -- or, in the inverse
direction, cleared ``holding_base`` on a failed sell and stranded a real
position teardown should have unwound.

These tests pin the corrected contract: state moves only in
``on_intent_executed`` on ``success=True``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


def _build(cls, base_token: str, quote_token: str):
    strat = cls.__new__(cls)
    strat.trade_size_usd = Decimal("3")
    strat.rsi_period = 14
    strat.rsi_oversold = Decimal("40")
    strat.rsi_overbought = Decimal("70")
    strat.max_slippage_bps = 100
    strat.base_token = base_token
    strat.quote_token = quote_token
    strat._consecutive_holds = 0
    strat._total_buys = 0
    strat._total_sells = 0
    strat._holding_base = False
    strat._last_signal = "neutral"
    return strat


def _mock_market(base_token, quote_token, *, rsi_value, base_balance=1.0, base_price=3000.0):
    market = MagicMock()

    rsi_obj = MagicMock()
    rsi_obj.value = Decimal(str(rsi_value))
    market.rsi = MagicMock(return_value=rsi_obj)
    market.price = MagicMock(return_value=Decimal(str(base_price)))

    def balance_fn(token, *args, **kwargs):
        bal = MagicMock()
        if token == quote_token:
            bal.balance_usd = Decimal("10000")
            bal.balance = Decimal("10000")
        elif token == base_token:
            bal.balance = Decimal(str(base_balance))
            bal.balance_usd = Decimal(str(base_balance)) * Decimal(str(base_price))
        else:
            raise ValueError(f"Unexpected token: {token}")
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


_DEMOS = [
    (
        "almanak.demo_strategies.uniswap_paper_trade_optimism.strategy",
        "UniswapPaperTradeOptimismStrategy",
        "WETH",
        "USDC",
    ),
    (
        "almanak.demo_strategies.pancakeswap_paper_trade_bsc.strategy",
        "PancakeSwapPaperTradeBscStrategy",
        "WBNB",
        "USDT",
    ),
]


@pytest.fixture(params=_DEMOS, ids=lambda d: d[1])
def demo(request):
    import importlib

    module_path, cls_name, base, quote = request.param
    cls = getattr(importlib.import_module(module_path), cls_name)
    return _build(cls, base, quote), base, quote


class TestDecideDoesNotMutateExecutionState:
    def test_buy_decision_leaves_counters_and_holding_untouched(self, demo):
        strat, base, quote = demo
        intent = strat.decide(_mock_market(base, quote, rsi_value=25.0))
        assert intent.intent_type.value == "SWAP"
        assert intent.to_token == base
        # decide() must NOT have mutated execution-derived state.
        assert strat._holding_base is False
        assert strat._total_buys == 0
        assert strat._last_signal == "neutral"

    def test_sell_decision_leaves_counters_and_holding_untouched(self, demo):
        strat, base, quote = demo
        strat._holding_base = True  # pretend we hold base from a prior buy
        intent = strat.decide(_mock_market(base, quote, rsi_value=80.0))
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == base
        assert strat._holding_base is True  # unchanged until the sell executes
        assert strat._total_sells == 0


class TestLatchOnlyOnSuccessfulSwap:
    def test_failed_buy_does_not_latch_or_set_holding(self, demo):
        strat, base, quote = demo
        intent = strat.decide(_mock_market(base, quote, rsi_value=25.0))
        strat.on_intent_executed(intent, success=False, result=None)
        assert strat._holding_base is False
        assert strat._total_buys == 0
        assert strat._last_signal == "neutral"

    def test_successful_buy_sets_holding_and_counter_and_latch(self, demo):
        strat, base, quote = demo
        intent = strat.decide(_mock_market(base, quote, rsi_value=25.0))
        strat.on_intent_executed(intent, success=True, result=None)
        assert strat._holding_base is True
        assert strat._total_buys == 1
        assert strat._last_signal == "buy"

    def test_successful_sell_clears_holding_and_counter_and_latch(self, demo):
        strat, base, quote = demo
        strat._holding_base = True
        intent = strat.decide(_mock_market(base, quote, rsi_value=80.0))
        strat.on_intent_executed(intent, success=True, result=None)
        assert strat._holding_base is False
        assert strat._total_sells == 1
        assert strat._last_signal == "sell"


class TestTeardownReflectsRealHoldings:
    def test_failed_buy_yields_no_phantom_teardown_sell(self, demo):
        from almanak.framework.teardown import TeardownMode

        strat, base, quote = demo
        intent = strat.decide(_mock_market(base, quote, rsi_value=25.0))
        strat.on_intent_executed(intent, success=False, result=None)
        # No base acquired -> teardown must NOT emit an amount="all" sell.
        assert strat.generate_teardown_intents(TeardownMode.SOFT) == []

    def test_failed_sell_does_not_strand_real_position(self, demo):
        from almanak.framework.teardown import TeardownMode

        strat, base, quote = demo
        # Acquire base for real.
        buy = strat.decide(_mock_market(base, quote, rsi_value=25.0))
        strat.on_intent_executed(buy, success=True, result=None)
        assert strat._holding_base is True
        # Sell decision fires but the swap fails: holding_base must stay True so
        # teardown still unwinds the position we actually hold.
        sell = strat.decide(_mock_market(base, quote, rsi_value=80.0))
        strat.on_intent_executed(sell, success=False, result=None)
        assert strat._holding_base is True
        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == base
        assert intents[0].to_token == quote
