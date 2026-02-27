"""Tests for create_market_snapshot_from_state balance exposure."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state


def _make_market_state(tokens: dict[str, Decimal], timestamp: datetime | None = None):
    """Create a minimal MarketState mock with given token prices."""
    state = MagicMock()
    state.timestamp = timestamp or datetime(2025, 1, 15, 12, 0, tzinfo=UTC)
    state.available_tokens = list(tokens.keys())

    def get_price(token):
        if token in tokens:
            return tokens[token]
        raise KeyError(f"No price for {token}")

    state.get_price = get_price
    return state


def _make_portfolio(cash_usd: Decimal, tokens: dict[str, Decimal] | None = None):
    """Create a minimal SimulatedPortfolio mock."""
    portfolio = MagicMock()
    portfolio.cash_usd = cash_usd
    portfolio.tokens = tokens or {}
    return portfolio


class TestStablecoinAliases:
    """Verify that cash_usd is exposed under stablecoin symbols."""

    def test_usdc_returns_cash_balance(self):
        state = _make_market_state({"WETH": Decimal("3000"), "USDC": Decimal("1")})
        portfolio = _make_portfolio(cash_usd=Decimal("10000"))
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("USDC")
        assert bal.balance == Decimal("10000")
        assert bal.balance_usd == Decimal("10000")

    def test_usdt_returns_cash_balance(self):
        state = _make_market_state({"WETH": Decimal("3000")})
        portfolio = _make_portfolio(cash_usd=Decimal("5000"))
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("USDT")
        assert bal.balance == Decimal("5000")

    def test_dai_returns_cash_balance(self):
        state = _make_market_state({"WETH": Decimal("3000")})
        portfolio = _make_portfolio(cash_usd=Decimal("2500"))
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("DAI")
        assert bal.balance == Decimal("2500")

    def test_stablecoin_alias_does_not_override_real_holding(self):
        """If portfolio holds actual USDC tokens, the alias should not override."""
        state = _make_market_state({"USDC": Decimal("1")})
        portfolio = _make_portfolio(
            cash_usd=Decimal("10000"),
            tokens={"USDC": Decimal("500")},
        )
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("USDC")
        # Should use the actual token holding (500 USDC), not the cash alias
        assert bal.balance == Decimal("500")


class TestZeroBalanceForTrackedTokens:
    """Verify that tracked tokens not in portfolio get zero balances."""

    def test_tracked_token_returns_zero_balance(self):
        state = _make_market_state({"WETH": Decimal("3000"), "WBTC": Decimal("60000")})
        portfolio = _make_portfolio(cash_usd=Decimal("10000"))
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("WETH")
        assert bal.balance == Decimal("0")
        assert bal.balance_usd == Decimal("0")

    def test_held_token_not_overridden_by_zero(self):
        """Tokens actually held in portfolio should keep their real balance."""
        state = _make_market_state({"WETH": Decimal("3000")})
        portfolio = _make_portfolio(
            cash_usd=Decimal("5000"),
            tokens={"WETH": Decimal("2.5")},
        )
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        bal = snapshot.balance("WETH")
        assert bal.balance == Decimal("2.5")

    def test_multiple_tracked_tokens_all_zero(self):
        state = _make_market_state({
            "WETH": Decimal("3000"),
            "WBTC": Decimal("60000"),
            "ARB": Decimal("1.5"),
        })
        portfolio = _make_portfolio(cash_usd=Decimal("10000"))
        snapshot = create_market_snapshot_from_state(state, portfolio=portfolio)

        for token in ["WETH", "WBTC", "ARB"]:
            bal = snapshot.balance(token)
            assert bal.balance == Decimal("0"), f"{token} should have zero balance"


class TestNoPortfolio:
    """Verify behavior when no portfolio is provided."""

    def test_no_portfolio_no_balance_error(self):
        state = _make_market_state({"WETH": Decimal("3000")})
        snapshot = create_market_snapshot_from_state(state)

        # Without a portfolio, no balances are set; balance() should raise
        with pytest.raises(ValueError, match="Cannot determine balance"):
            snapshot.balance("WETH")

    def test_prices_still_available_without_portfolio(self):
        state = _make_market_state({"WETH": Decimal("3000")})
        snapshot = create_market_snapshot_from_state(state)

        price = snapshot.price("WETH")
        assert price == Decimal("3000")
