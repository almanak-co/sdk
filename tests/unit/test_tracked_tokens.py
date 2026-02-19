"""Tests for _get_tracked_tokens and _derive_tokens_from_config."""

from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak import IntentStrategy


# ---------------------------------------------------------------------------
# Minimal config fixtures
# ---------------------------------------------------------------------------

@dataclass
class PoolConfig:
    """Config with pool field (LP strategies)."""
    pool: str = "WETH/USDC/500"
    range_width_pct: Decimal = Decimal("0.20")
    amount0: Decimal = Decimal("0.001")
    amount1: Decimal = Decimal("3")
    strategy_id: str = "test"
    strategy_name: str = "test"
    chain: str = "arbitrum"

    def to_dict(self):
        return {
            "pool": self.pool,
            "range_width_pct": str(self.range_width_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
        }

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


@dataclass
class SwapConfig:
    """Config with base_token/quote_token fields (swap strategies)."""
    base_token: str = "WETH"
    quote_token: str = "USDC"
    trade_amount: str = "100"
    strategy_id: str = "test"
    strategy_name: str = "test"
    chain: str = "arbitrum"

    def to_dict(self):
        return {
            "base_token": self.base_token,
            "quote_token": self.quote_token,
            "trade_amount": self.trade_amount,
        }

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


@dataclass
class LendingConfig:
    """Config with collateral_token/borrow_token fields (lending strategies)."""
    collateral_token: str = "wstETH"
    borrow_token: str = "USDC"
    strategy_id: str = "test"
    strategy_name: str = "test"
    chain: str = "arbitrum"

    def to_dict(self):
        return {
            "collateral_token": self.collateral_token,
            "borrow_token": self.borrow_token,
        }

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


@dataclass
class FromToConfig:
    """Config with from_token/to_token fields (simple swap strategies)."""
    from_token: str = "WETH"
    to_token: str = "USDC"
    strategy_id: str = "test"
    strategy_name: str = "test"
    chain: str = "arbitrum"

    def to_dict(self):
        return {
            "from_token": self.from_token,
            "to_token": self.to_token,
        }

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


@dataclass
class EmptyConfig:
    """Config with no token-related fields."""
    interval: int = 60
    strategy_id: str = "test"
    strategy_name: str = "test"
    chain: str = "arbitrum"

    def to_dict(self):
        return {"interval": self.interval}

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


# ---------------------------------------------------------------------------
# Helper to create a strategy instance without the full framework
# ---------------------------------------------------------------------------

class _ConcreteStrategy(IntentStrategy):
    """Minimal concrete subclass for testing."""

    def decide(self, market):
        return None


def _make_strategy(config):
    """Create a strategy instance with minimal mocking."""
    strategy = object.__new__(_ConcreteStrategy)
    strategy.config = config
    strategy._chain = getattr(config, "chain", "arbitrum")
    strategy._strategy_id = getattr(config, "strategy_id", "test")
    return strategy


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDeriveTokensFromConfig:
    """Test _derive_tokens_from_config extracts tokens correctly."""

    def test_pool_field_extracts_tokens(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/500"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH", "USDC"]

    def test_pool_field_two_token_format(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH", "USDC"]

    def test_pool_field_skips_fee_tier(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/3000"))
        tokens = strategy._derive_tokens_from_config()
        assert "3000" not in tokens
        assert tokens == ["WETH", "USDC"]

    def test_base_quote_tokens(self):
        strategy = _make_strategy(SwapConfig(base_token="WETH", quote_token="USDC"))
        tokens = strategy._derive_tokens_from_config()
        assert "WETH" in tokens
        assert "USDC" in tokens
        assert len(tokens) == 2

    def test_collateral_borrow_tokens(self):
        strategy = _make_strategy(LendingConfig(collateral_token="wstETH", borrow_token="USDC"))
        tokens = strategy._derive_tokens_from_config()
        assert "wstETH" in tokens
        assert "USDC" in tokens
        assert len(tokens) == 2

    def test_from_to_tokens(self):
        strategy = _make_strategy(FromToConfig(from_token="WETH", to_token="USDC"))
        tokens = strategy._derive_tokens_from_config()
        assert "WETH" in tokens
        assert "USDC" in tokens
        assert len(tokens) == 2

    def test_empty_config_returns_empty(self):
        strategy = _make_strategy(EmptyConfig())
        tokens = strategy._derive_tokens_from_config()
        assert tokens == []

    def test_no_duplicates(self):
        """If same token appears in multiple fields, it should appear once."""
        @dataclass
        class DupConfig:
            base_token: str = "USDC"
            quote_token: str = "USDC"
            strategy_id: str = "test"
            chain: str = "arbitrum"
            def to_dict(self):
                return {"base_token": self.base_token, "quote_token": self.quote_token}
            def update(self, **kwargs):
                pass

        strategy = _make_strategy(DupConfig())
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["USDC"]

    def test_pool_with_bridged_tokens(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC.e/500"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WETH", "USDC.e"]

    def test_traderjoe_pool_format(self):
        strategy = _make_strategy(PoolConfig(pool="WAVAX/USDC/20"))
        tokens = strategy._derive_tokens_from_config()
        assert tokens == ["WAVAX", "USDC"]


class TestGetTrackedTokens:
    """Test _get_tracked_tokens returns derived tokens or fallback."""

    def test_returns_derived_tokens_when_available(self):
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/500"))
        tokens = strategy._get_tracked_tokens()
        assert tokens == ["WETH", "USDC"]

    def test_fallback_when_no_tokens_in_config(self):
        strategy = _make_strategy(EmptyConfig())
        tokens = strategy._get_tracked_tokens()
        assert tokens == ["USDC", "WETH"]

    def test_does_not_include_unrelated_tokens(self):
        """The key bug fix: LP strategy should NOT fetch USDT, DAI, ETH."""
        strategy = _make_strategy(PoolConfig(pool="WETH/USDC/500"))
        tokens = strategy._get_tracked_tokens()
        assert "USDT" not in tokens
        assert "DAI" not in tokens
        assert "ETH" not in tokens

    def test_config_with_none_value(self):
        """Config fields with None values should be skipped."""
        @dataclass
        class NullConfig:
            base_token: str = "WETH"
            quote_token: str | None = None
            strategy_id: str = "test"
            chain: str = "arbitrum"
            def to_dict(self):
                return {"base_token": self.base_token, "quote_token": self.quote_token}
            def update(self, **kwargs):
                pass

        strategy = _make_strategy(NullConfig())
        tokens = strategy._get_tracked_tokens()
        assert tokens == ["WETH"]
