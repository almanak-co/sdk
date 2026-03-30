"""Tests for the Pendle YT Yield demo strategy.

Validates YT purchase decisions, teardown, and intent generation
for the leveraged floating yield strategy on Arbitrum.

Kitchen Loop iteration 94, VIB-315.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from almanak.demo_strategies.pendle_yt_yield.strategy import PendleYTYieldStrategy

    strat = PendleYTYieldStrategy.__new__(PendleYTYieldStrategy)
    strat.config = {}
    strat._chain = "arbitrum"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-pendle-yt"
    strat.market = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b"
    strat.market_name = "wstETH-25JUN2026"
    strat.trade_size_token = Decimal("0.01")
    strat.trade_size_usd = Decimal("10")
    strat.max_slippage_bps = 200
    strat.base_token = "WSTETH"
    strat.base_token_symbol = "WSTETH"
    strat.yt_token = "YT-wstETH-25JUN2026"
    strat.yt_token_symbol = "YT-wstETH"
    strat.stop_loss_pct = 50
    strat.teardown_hard_slippage_bps = 1500
    strat.teardown_soft_slippage_bps = 500
    strat._has_entered_position = False
    strat._consecutive_holds = 0
    strat._entry_value_usd = Decimal("0")
    return strat


def _mock_market(
    base_balance: float = 1.0,
    base_price: float = 3400.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "WSTETH":
            return Decimal(str(base_price))
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "WSTETH":
            bal.balance_usd = Decimal(str(base_balance)) * Decimal(str(base_price))
            bal.balance = Decimal(str(base_balance))
        else:
            raise ValueError(f"Unknown token: {token}")
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestDecision:
    def test_buy_yt_when_sufficient_balance(self, strategy):
        """Should buy YT when wstETH balance >= trade_size_token."""
        market = _mock_market(base_balance=1.0)
        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WSTETH"
        assert intent.to_token == "YT-wstETH-25JUN2026"
        assert intent.amount == Decimal("0.01")
        assert intent.protocol == "pendle"
        assert strategy._has_entered_position is True

    def test_hold_when_insufficient_balance(self, strategy):
        """Should hold when wstETH balance < trade_size_token."""
        market = _mock_market(base_balance=0.001)
        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert strategy._has_entered_position is False

    def test_hold_after_entering_position(self, strategy):
        """Should hold after position is entered."""
        market = _mock_market(base_balance=1.0)

        # First call: buy YT
        intent1 = strategy.decide(market)
        assert intent1.intent_type.value == "SWAP"

        # Second call: hold
        intent2 = strategy.decide(market)
        assert intent2.intent_type.value == "HOLD"
        assert strategy._consecutive_holds == 1

    def test_hold_when_price_unavailable(self, strategy):
        """Should hold when price data is unavailable."""
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("No price"))
        intent = strategy.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_slippage_set_correctly(self, strategy):
        """Slippage should be max_slippage_bps / 10000."""
        market = _mock_market(base_balance=1.0)
        intent = strategy.decide(market)

        expected_slippage = Decimal("200") / Decimal("10000")  # 2%
        assert intent.max_slippage == expected_slippage

    def test_entry_value_tracked(self, strategy):
        """Entry value in USD should be tracked for monitoring."""
        market = _mock_market(base_balance=1.0, base_price=3400.0)
        strategy.decide(market)

        # 0.01 wstETH * $3400 = $34
        assert strategy._entry_value_usd == Decimal("0.01") * Decimal("3400.0")


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_no_positions_when_idle(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_position_reported_after_entry(self, strategy):
        strategy._has_entered_position = True
        strategy._entry_value_usd = Decimal("34")

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.protocol == "pendle"
        assert pos.details["yt_token"] == "YT-wstETH-25JUN2026"

    def test_teardown_generates_swap_intent(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_entered_position = True
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 1
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "YT-wstETH-25JUN2026"
        assert intents[0].to_token == "WSTETH"
        assert intents[0].protocol == "pendle"

    def test_teardown_hard_mode_wider_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_entered_position = True
        intents = strategy.generate_teardown_intents(TeardownMode.HARD)

        assert intents[0].max_slippage == Decimal("0.15")

    def test_teardown_empty_when_no_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0


class TestStatus:
    def test_status_contains_yt_info(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "pendle_yt_yield"
        assert status["config"]["yt_token"] == "YT-wstETH-25JUN2026"
        assert status["state"]["has_position"] is False


class TestTrackedTokens:
    def test_tracked_tokens(self, strategy):
        tokens = strategy._get_tracked_tokens()
        assert "WSTETH" in tokens
        assert "YT-wstETH" in tokens


class TestYTTokenConfig:
    """Tests verifying the Pendle SDK YT token config for Arbitrum."""

    def test_yt_token_info_has_arbitrum(self):
        from almanak.framework.connectors.pendle.sdk import YT_TOKEN_INFO

        assert "arbitrum" in YT_TOKEN_INFO
        arb_yt = YT_TOKEN_INFO["arbitrum"]
        assert "YT-wstETH-25JUN2026" in arb_yt
        address, decimals = arb_yt["YT-wstETH-25JUN2026"]
        assert address == "0x25bda1edd6af17c61399aa0eb84b93daa3069764"
        assert decimals == 18

    def test_market_by_yt_token_has_arbitrum(self):
        from almanak.framework.connectors.pendle.sdk import MARKET_BY_YT_TOKEN

        assert "arbitrum" in MARKET_BY_YT_TOKEN
        arb_markets = MARKET_BY_YT_TOKEN["arbitrum"]
        assert "YT-wstETH-25JUN2026" in arb_markets
        assert arb_markets["YT-wstETH-25JUN2026"] == "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b"
