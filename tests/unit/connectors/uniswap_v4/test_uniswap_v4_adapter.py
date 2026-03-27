"""Tests for Uniswap V4 Adapter."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.uniswap_v4.adapter import (
    UniswapV4Adapter,
    UniswapV4Config,
)
from almanak.framework.data.tokens import TokenNotFoundError

# Known tokens for mock resolver
_KNOWN_TOKENS = {
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": ("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", 6),
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": ("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", 18),
    "0x0000000000000000000000000000000000000000": ("0x0000000000000000000000000000000000000000", 18),
}


def _make_resolver():
    """Create a mock token resolver that knows common Arbitrum tokens."""
    resolver = MagicMock()

    def _resolve(token, chain):
        key = token.lower()
        if key in _KNOWN_TOKENS:
            m = MagicMock()
            m.address, m.decimals = _KNOWN_TOKENS[key]
            return m
        raise ValueError(f"Unknown token {token}")

    def _resolve_for_swap(token, chain):
        return _resolve(token, chain)

    resolver.resolve.side_effect = _resolve
    resolver.resolve_for_swap.side_effect = _resolve_for_swap
    return resolver


class TestAdapterInit:
    def test_init_with_chain(self):
        adapter = UniswapV4Adapter(chain="arbitrum")
        assert adapter.chain == "arbitrum"

    def test_init_with_config(self):
        config = UniswapV4Config(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_fee_tier=500,
        )
        adapter = UniswapV4Adapter(config=config)
        assert adapter.chain == "ethereum"
        assert adapter.default_fee_tier == 500

    def test_init_unsupported_chain(self):
        with pytest.raises(ValueError, match="not supported"):
            UniswapV4Adapter(chain="fantom")

    def test_init_no_args(self):
        with pytest.raises(ValueError, match="Either chain or config"):
            UniswapV4Adapter()


_TEST_WALLET = "0x1234567890123456789012345678901234567890"


class TestSwapExactInput:
    def test_basic_swap(self):
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        result = adapter.swap_exact_input(
            token_in="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            amount_in=Decimal("1000"),
            slippage_bps=50,
        )
        assert result.success is True
        assert len(result.transactions) == 3  # approve Permit2 + Permit2 approve router + swap
        assert result.amount_in > 0
        assert result.amount_out_minimum > 0

    def test_no_wallet_address_raises(self):
        adapter = UniswapV4Adapter(chain="arbitrum", token_resolver=_make_resolver())
        with pytest.raises(ValueError, match="wallet_address must be set"):
            adapter.swap_exact_input(
                token_in="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                amount_in=Decimal("1000"),
            )

    def test_native_eth_no_approve(self):
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        result = adapter.swap_exact_input(
            token_in="0x0000000000000000000000000000000000000000",
            token_out="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            amount_in=Decimal("1"),
        )
        assert result.success is True
        # No approve needed for native ETH - just swap
        assert len(result.transactions) == 1

    def test_slippage_applied(self):
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        result = adapter.swap_exact_input(
            token_in="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            amount_in=Decimal("1000"),
            slippage_bps=100,  # 1%
        )
        assert result.success is True
        # amount_out_minimum should be ~99% of quote output
        assert result.amount_out_minimum > 0


class TestTokenResolution:
    def test_resolve_by_address(self):
        adapter = UniswapV4Adapter(chain="arbitrum", token_resolver=_make_resolver())
        addr, dec = adapter._resolve_token("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        assert addr == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert dec == 6

    def test_resolve_by_address_without_resolver_raises(self):
        """Raw address without resolver must raise, not fallback to 18 decimals."""
        adapter = UniswapV4Adapter(chain="arbitrum")
        with pytest.raises(TokenNotFoundError):
            adapter._resolve_token("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")

    def test_resolve_by_symbol_fallback(self):
        adapter = UniswapV4Adapter(chain="arbitrum")
        addr, dec = adapter._resolve_token("USDC")
        assert addr.lower() == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        assert dec == 6

    def test_resolve_with_token_resolver(self):
        resolver = MagicMock()
        resolved = MagicMock()
        resolved.address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        resolved.decimals = 6
        resolver.resolve_for_swap.return_value = resolved

        adapter = UniswapV4Adapter(chain="arbitrum", token_resolver=resolver)
        addr, dec = adapter._resolve_token("USDC")
        assert addr == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert dec == 6

    def test_resolve_unknown_token_raises_token_not_found(self):
        adapter = UniswapV4Adapter(chain="arbitrum")
        with pytest.raises(TokenNotFoundError):
            adapter._resolve_token("UNKNOWN_TOKEN_XYZ")

    def test_resolve_symbol_with_unknown_decimals_raises(self):
        """Symbol found in UNISWAP_V3_TOKENS but not in decimals_map must raise."""
        from unittest.mock import patch

        fake_tokens = {"arbitrum": {"FAKECOIN": "0x1234567890123456789012345678901234567890"}}
        adapter = UniswapV4Adapter(chain="arbitrum")
        with patch("almanak.core.contracts.UNISWAP_V3_TOKENS", fake_tokens):
            with pytest.raises(TokenNotFoundError):
                adapter._resolve_token("FAKECOIN")

    def test_raw_address_without_resolver_raises(self):
        """Raw addresses without a token_resolver must fail, not assume 18 decimals."""
        adapter = UniswapV4Adapter(chain="arbitrum")
        with pytest.raises(TokenNotFoundError):
            adapter.swap_exact_input(
                token_in="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                amount_in=Decimal("1000"),
            )


class TestCompileSwapIntent:
    def test_compile_with_amount(self):
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config)

        # Create a mock SwapIntent
        intent = MagicMock()
        intent.from_token = "USDC"
        intent.to_token = "WETH"
        intent.amount = Decimal("1000")
        intent.amount_usd = None
        intent.max_slippage = Decimal("0.005")
        intent.intent_id = "test-intent-1"

        bundle = adapter.compile_swap_intent(intent)
        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) > 0
        assert bundle.metadata["protocol_version"] == "v4"
        assert bundle.metadata["from_token"]["symbol"] == "USDC"
        assert bundle.metadata["from_token"]["address"] is not None
        assert bundle.metadata["to_token"]["symbol"] == "WETH"
        assert bundle.metadata["to_token"]["address"] is not None

    def test_compile_with_amount_usd(self):
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config)

        intent = MagicMock()
        intent.from_token = "USDC"
        intent.to_token = "WETH"
        intent.amount = None
        intent.amount_usd = Decimal("1000")
        intent.max_slippage = Decimal("0.005")
        intent.intent_id = "test-intent-2"

        price_oracle = {"USDC": Decimal("1.0")}
        bundle = adapter.compile_swap_intent(intent, price_oracle)
        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) > 0

    def test_compile_amount_all_raises(self):
        adapter = UniswapV4Adapter(chain="arbitrum")

        intent = MagicMock()
        intent.from_token = "USDC"
        intent.to_token = "WETH"
        intent.amount = "all"
        intent.amount_usd = None
        intent.max_slippage = Decimal("0.005")
        intent.intent_id = "test-intent-3"

        with pytest.raises(ValueError, match="must be resolved"):
            adapter.compile_swap_intent(intent)

    def test_compile_no_amount_raises(self):
        adapter = UniswapV4Adapter(chain="arbitrum")

        intent = MagicMock()
        intent.from_token = "USDC"
        intent.to_token = "WETH"
        intent.amount = None
        intent.amount_usd = None
        intent.max_slippage = Decimal("0.005")
        intent.intent_id = "test-intent-4"

        with pytest.raises(ValueError, match="amount or amount_usd"):
            adapter.compile_swap_intent(intent)


class TestIntentCompilerV4Routing:
    """Test that IntentCompiler routes protocol='uniswap_v4' to V4 adapter."""

    def test_compiler_v4_quarantined(self):
        """Verify V4 compilation is blocked with clear quarantine error (VIB-1462)."""
        from almanak.framework.intents import SwapIntent
        from almanak.framework.intents.compiler import IntentCompiler

        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address=_TEST_WALLET,
            price_oracle={"USDC": Decimal("1.0"), "WETH": Decimal("2500.0")},
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.20"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status.value == "FAILED"
        assert "blocked" in result.error.lower()
        assert "VIB-1965" in result.error
        assert "uniswap_v3" in result.error  # Should suggest V3 alternative
