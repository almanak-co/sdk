"""Tests for Solana LST Depeg Recovery Arbitrage Strategy."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.solana_lst_depeg_arb.strategy import SolanaLstDepegArbStrategy


@pytest.fixture
def strategy():
    """Create a strategy instance with default config."""
    s = SolanaLstDepegArbStrategy.__new__(SolanaLstDepegArbStrategy)
    s.config = {
        "lst_token": "mSOL",
        "base_token": "SOL",
        "depeg_entry_threshold_pct": 0.8,
        "depeg_exit_threshold_pct": 0.15,
        "max_hold_iterations": 96,
        "swap_amount": "10.0",
        "max_slippage_pct": 1.0,
        "stop_loss_depeg_pct": 3.0,
    }
    s.state = {}
    s._strategy_id = "test-lst-depeg"
    return s


@pytest.fixture
def market():
    """Create a mock MarketSnapshot."""
    m = MagicMock()
    return m


def _set_prices(market, sol_price, msol_price):
    """Configure market mock with given prices."""

    def price_fn(token):
        prices = {"SOL": sol_price, "mSOL": msol_price, "USDC": Decimal("1")}
        return prices.get(token)

    market.price = price_fn


class TestNoPosition:
    """Tests when strategy has no open position."""

    def test_hold_when_no_depeg(self, strategy, market):
        """Should hold when LST trades near fair value."""
        _set_prices(market, Decimal("200"), Decimal("199.5"))  # 0.25% depeg, below 0.8%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_buy_on_depeg(self, strategy, market):
        """Should buy LST when depeg exceeds entry threshold."""
        _set_prices(market, Decimal("200"), Decimal("198"))  # 1% depeg > 0.8%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "SOL"
        assert intent.to_token == "mSOL"
        assert intent.amount == Decimal("10.0")
        # Verify USD value stored for teardown (10 SOL * $200 = $2000)
        assert strategy.state["entry_value_usd"] == "2000.0"

    def test_buy_at_exact_threshold(self, strategy, market):
        """Should buy when depeg is exactly at entry threshold (>= comparison)."""
        sol_price = Decimal("200")
        msol_price = sol_price * (Decimal("1") - Decimal("0.008"))
        _set_prices(market, sol_price, msol_price)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"

    def test_hold_when_missing_prices(self, strategy, market):
        """Should hold when price data is missing."""
        market.price = lambda token: None
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Missing price" in intent.reason

    def test_hold_when_base_price_zero(self, strategy, market):
        """Should hold when base token price is zero."""
        _set_prices(market, Decimal("0"), Decimal("100"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestWithPosition:
    """Tests when strategy has an open position."""

    def test_sell_on_repeg(self, strategy, market):
        """Should sell LST when price recovers."""
        strategy.state["has_position"] = True
        strategy.state["hold_iterations"] = 5
        _set_prices(market, Decimal("200"), Decimal("199.8"))  # 0.1% depeg < 0.15% exit
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "mSOL"
        assert intent.to_token == "SOL"
        assert intent.amount == "all"

    def test_stop_loss(self, strategy, market):
        """Should exit on stop-loss when depeg deepens."""
        strategy.state["has_position"] = True
        strategy.state["hold_iterations"] = 2
        _set_prices(market, Decimal("200"), Decimal("192"))  # 4% depeg > 3% stop-loss
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "mSOL"
        assert intent.to_token == "SOL"
        assert intent.max_slippage == Decimal("0.03")  # wider slippage for emergency

    def test_max_hold_exit(self, strategy, market):
        """Should exit after max hold iterations."""
        strategy.state["has_position"] = True
        strategy.state["hold_iterations"] = 96  # at max
        _set_prices(market, Decimal("200"), Decimal("198"))  # still depegged
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "mSOL"

    def test_continue_hold(self, strategy, market):
        """Should hold when depeg is between entry/exit thresholds."""
        strategy.state["has_position"] = True
        strategy.state["hold_iterations"] = 10
        _set_prices(market, Decimal("200"), Decimal("199"))  # 0.5% depeg
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy.state["hold_iterations"] == 11


class TestOnIntentExecuted:
    """Tests for position state tracking."""

    def test_tracks_entry(self, strategy):
        """Should track position when LST is bought."""
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.to_token = "mSOL"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy.state["has_position"] is True
        assert strategy.state["hold_iterations"] == 0
        assert strategy.state["entry_swap_amount"] == "10.0"

    def test_tracks_exit(self, strategy):
        """Should clear position when LST is sold."""
        strategy.state["has_position"] = True
        strategy.state["hold_iterations"] = 10
        strategy.state["exit_reason"] = "repeg"
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.to_token = "SOL"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy.state["has_position"] is False
        assert strategy.state["hold_iterations"] == 0

    def test_no_update_on_failure(self, strategy):
        """Should not update state on failed execution."""
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.to_token = "mSOL"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy.state.get("has_position") is None


class TestTeardown:
    """Tests for teardown methods."""

    def test_no_positions_when_empty(self, strategy):
        """Should return empty positions when no open position."""
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_reports_open_position(self, strategy):
        """Should report position with USD value (not SOL amount)."""
        strategy.state["has_position"] = True
        strategy.state["hold_iterations"] = 5
        strategy.state["entry_value_usd"] = "2000.0"  # 10 SOL * $200
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_id == "lst_depeg_msol"
        assert summary.positions[0].protocol == "jupiter"
        assert summary.positions[0].value_usd == Decimal("2000.0")

    def test_teardown_intents_empty_when_no_position(self, strategy):
        """Should return no intents when no position."""
        intents = strategy.generate_teardown_intents(mode="SOFT")
        assert intents == []

    def test_teardown_intents_sell_lst(self, strategy):
        """Should generate sell intent for open position."""
        from almanak.framework.teardown import TeardownMode

        strategy.state["has_position"] = True
        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].from_token == "mSOL"
        assert intents[0].to_token == "SOL"
        assert intents[0].amount == "all"

    def test_teardown_hard_mode_wider_slippage(self, strategy):
        """Should use wider slippage in hard teardown mode."""
        from almanak.framework.teardown import TeardownMode

        strategy.state["has_position"] = True
        intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD)
        assert intents[0].max_slippage == Decimal("0.03")

    def test_teardown_soft_mode_tight_slippage(self, strategy):
        """Should use tighter slippage in soft teardown mode."""
        from almanak.framework.teardown import TeardownMode

        strategy.state["has_position"] = True
        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert intents[0].max_slippage == Decimal("0.01")


    def test_supports_teardown(self, strategy):
        """Should support teardown."""
        assert strategy.supports_teardown() is True


class TestJitoSOLConfig:
    """Tests with JitoSOL configuration."""

    def test_jitosol_buy(self, market):
        """Should work with JitoSOL config."""
        s = SolanaLstDepegArbStrategy.__new__(SolanaLstDepegArbStrategy)
        s.config = {
            "lst_token": "JitoSOL",
            "base_token": "SOL",
                "depeg_entry_threshold_pct": 0.8,
            "depeg_exit_threshold_pct": 0.15,
            "max_hold_iterations": 96,
            "swap_amount": "5.0",
            "max_slippage_pct": 1.0,
            "stop_loss_depeg_pct": 3.0,
        }
        s.state = {}
        s._strategy_id = "test-jitosol-depeg"

        def price_fn(token):
            prices = {"SOL": Decimal("200"), "JitoSOL": Decimal("196")}  # 2% depeg
            return prices.get(token)

        market.price = price_fn
        intent = s.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.to_token == "JitoSOL"
        assert intent.amount == Decimal("5.0")
