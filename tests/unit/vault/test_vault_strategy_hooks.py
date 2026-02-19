"""Tests for vault strategy hooks: valuate(), on_vault_settled(), total_portfolio_usd()."""

from decimal import Decimal

from almanak.framework.strategies.intent_strategy import (
    IntentStrategy,
    MarketSnapshot,
    TokenBalance,
)
from almanak.framework.vault.config import SettlementResult


# --- MarketSnapshot.total_portfolio_usd() tests ---


class TestMarketSnapshotTotalPortfolioUsd:
    """Tests for the no-arg total_portfolio_usd() on single-chain MarketSnapshot."""

    def _make_snapshot(self) -> MarketSnapshot:
        return MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x1234",
        )

    def test_empty_returns_zero(self):
        snapshot = self._make_snapshot()
        assert snapshot.total_portfolio_usd() == Decimal("0")

    def test_sums_prepopulated_balances(self):
        snapshot = self._make_snapshot()
        snapshot.set_balance("WETH", TokenBalance(symbol="WETH", balance=Decimal("2"), balance_usd=Decimal("5000")))
        snapshot.set_balance("USDC", TokenBalance(symbol="USDC", balance=Decimal("1000"), balance_usd=Decimal("1000")))

        assert snapshot.total_portfolio_usd() == Decimal("6000")

    def test_sums_cached_balances(self):
        snapshot = self._make_snapshot()
        snapshot._balance_cache["WETH"] = TokenBalance(
            symbol="WETH", balance=Decimal("1"), balance_usd=Decimal("2500")
        )
        snapshot._balance_cache["USDC"] = TokenBalance(
            symbol="USDC", balance=Decimal("500"), balance_usd=Decimal("500")
        )

        assert snapshot.total_portfolio_usd() == Decimal("3000")

    def test_no_double_counting(self):
        """Tokens in both _balances and _balance_cache should not be double counted."""
        snapshot = self._make_snapshot()
        snapshot.set_balance("WETH", TokenBalance(symbol="WETH", balance=Decimal("2"), balance_usd=Decimal("5000")))
        snapshot._balance_cache["WETH"] = TokenBalance(
            symbol="WETH", balance=Decimal("2"), balance_usd=Decimal("5000")
        )

        assert snapshot.total_portfolio_usd() == Decimal("5000")

    def test_mixed_prepopulated_and_cached(self):
        snapshot = self._make_snapshot()
        snapshot.set_balance("WETH", TokenBalance(symbol="WETH", balance=Decimal("1"), balance_usd=Decimal("2500")))
        snapshot._balance_cache["USDC"] = TokenBalance(
            symbol="USDC", balance=Decimal("1000"), balance_usd=Decimal("1000")
        )

        assert snapshot.total_portfolio_usd() == Decimal("3500")

    def test_zero_balance_usd_included(self):
        snapshot = self._make_snapshot()
        snapshot.set_balance("WETH", TokenBalance(symbol="WETH", balance=Decimal("0"), balance_usd=Decimal("0")))
        snapshot.set_balance("USDC", TokenBalance(symbol="USDC", balance=Decimal("100"), balance_usd=Decimal("100")))

        assert snapshot.total_portfolio_usd() == Decimal("100")


# --- Strategy helper using object.__new__() to bypass __init__ ---


def _make_strategy(cls):
    """Create a strategy instance bypassing __init__ to avoid config validation."""
    strategy = object.__new__(cls)
    strategy._chain = "arbitrum"
    strategy._wallet_address = "0xabcd"
    return strategy


# --- IntentStrategy.valuate() tests ---


class ConcreteStrategy(IntentStrategy):
    """Minimal concrete strategy for testing."""

    def decide(self, market):
        return None


class CustomValuateStrategy(IntentStrategy):
    """Strategy with custom valuate() override."""

    def decide(self, market):
        return None

    def valuate(self, market: MarketSnapshot) -> Decimal:
        try:
            return market.balance("USDC").balance_usd
        except ValueError:
            return Decimal("0")


class TestIntentStrategyValuate:
    """Tests for IntentStrategy.valuate() default behavior."""

    def _make_snapshot(self) -> MarketSnapshot:
        return MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xabcd",
        )

    def test_default_valuate_returns_total_portfolio_usd(self):
        strategy = _make_strategy(ConcreteStrategy)
        market = self._make_snapshot()
        market.set_balance("WETH", TokenBalance(symbol="WETH", balance=Decimal("2"), balance_usd=Decimal("5000")))
        market.set_balance("USDC", TokenBalance(symbol="USDC", balance=Decimal("1000"), balance_usd=Decimal("1000")))

        result = strategy.valuate(market)
        assert result == Decimal("6000")

    def test_default_valuate_empty_portfolio(self):
        strategy = _make_strategy(ConcreteStrategy)
        market = self._make_snapshot()

        result = strategy.valuate(market)
        assert result == Decimal("0")

    def test_custom_valuate_override(self):
        strategy = _make_strategy(CustomValuateStrategy)
        market = self._make_snapshot()
        market.set_balance("WETH", TokenBalance(symbol="WETH", balance=Decimal("2"), balance_usd=Decimal("5000")))
        market.set_balance("USDC", TokenBalance(symbol="USDC", balance=Decimal("1000"), balance_usd=Decimal("1000")))

        result = strategy.valuate(market)
        assert result == Decimal("1000")  # Only USDC


# --- IntentStrategy.on_vault_settled() tests ---


class TestIntentStrategyOnVaultSettled:
    """Tests for IntentStrategy.on_vault_settled() callback."""

    def test_default_on_vault_settled_is_noop(self):
        strategy = _make_strategy(ConcreteStrategy)
        settlement = SettlementResult(
            success=True,
            deposits_received=1000,
            redemptions_processed=0,
            new_total_assets=10000,
            shares_minted=100,
            shares_burned=0,
            fee_shares_minted=5,
            epoch_id=1,
        )
        # Should not raise
        strategy.on_vault_settled(settlement)

    def test_on_vault_settled_can_be_overridden(self):
        callback_data = {}

        class TrackingStrategy(IntentStrategy):
            def decide(self, market):
                return None

            def on_vault_settled(self, settlement):
                callback_data["epoch"] = settlement.epoch_id
                callback_data["success"] = settlement.success

        strategy = _make_strategy(TrackingStrategy)
        settlement = SettlementResult(
            success=True,
            deposits_received=500,
            redemptions_processed=200,
            new_total_assets=8000,
            shares_minted=50,
            shares_burned=20,
            fee_shares_minted=3,
            epoch_id=42,
        )
        strategy.on_vault_settled(settlement)

        assert callback_data["epoch"] == 42
        assert callback_data["success"] is True
