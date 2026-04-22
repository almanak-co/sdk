"""Tests for fiat-quote exclusion in IntentStrategy._derive_tokens_from_config.

Covers the strategy-path entry point for the fiat-quote filter introduced to
fix BSC staging errors (2026-04-22) where a strategy with market='BTC/USD'
caused the tracked-tokens loop to call balance("USD") and price("USD") —
both of which fail because USD is a quote denomination, not an ERC-20 token,
and no Chainlink USD/USD feed exists.
"""

from dataclasses import dataclass

import pytest

from almanak import IntentStrategy


@dataclass
class _Config:
    strategy_id: str = "test"
    strategy_name: str = "test"
    chain: str = "bsc"

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if k not in {"strategy_id", "strategy_name", "chain"}}

    def update(self, **kwargs) -> None:
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


class _Strat(IntentStrategy):
    def decide(self, market):
        return None

    def get_open_positions(self):
        from almanak.framework.teardown.models import TeardownPositionSummary
        return TeardownPositionSummary.empty("test")

    def generate_teardown_intents(self, mode=None, market=None):
        return []


def _make(config):
    s = object.__new__(_Strat)
    s.config = config
    s._chain = getattr(config, "chain", "bsc")
    s._strategy_id = "test"
    return s


class TestFiatQuoteExclusionInDeriveTokens:
    """IntentStrategy._derive_tokens_from_config excludes fiat quote symbols."""

    def test_market_btc_usd_excludes_usd(self):
        """Regression: market='BTC/USD' on BSC must not yield USD as a tracked token."""
        @dataclass
        class C(_Config):
            market: str = "BTC/USD"
            def to_dict(self):
                return {"market": self.market}

        tokens = _make(C())._derive_tokens_from_config()
        assert tokens == ["BTC"]
        assert "USD" not in tokens

    def test_quote_token_usd_excluded(self):
        @dataclass
        class C(_Config):
            base_token: str = "WETH"
            quote_token: str = "USD"
            def to_dict(self):
                return {"base_token": self.base_token, "quote_token": self.quote_token}

        tokens = _make(C())._derive_tokens_from_config()
        assert tokens == ["WETH"]
        assert "USD" not in tokens

    @pytest.mark.parametrize("fiat", ["EUR", "GBP", "JPY"])
    def test_other_fiat_symbols_excluded(self, fiat):
        @dataclass
        class C(_Config):
            base_token: str = "WETH"
            quote_token: str = "USD"  # overridden below
            def to_dict(self):
                return {"base_token": self.base_token, "quote_token": self.quote_token}

        cfg = C()
        cfg.quote_token = fiat
        tokens = _make(cfg)._derive_tokens_from_config()
        assert tokens == ["WETH"]

    def test_usdc_quote_token_not_excluded(self):
        """USDC is a real ERC-20 token and must never be filtered as fiat."""
        @dataclass
        class C(_Config):
            base_token: str = "WETH"
            quote_token: str = "USDC"
            def to_dict(self):
                return {"base_token": self.base_token, "quote_token": self.quote_token}

        tokens = _make(C())._derive_tokens_from_config()
        assert set(tokens) == {"WETH", "USDC"}
        assert len(tokens) == 2
