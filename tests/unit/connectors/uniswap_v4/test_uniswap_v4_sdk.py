"""Tests for Uniswap V4 SDK."""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.connectors.uniswap_v4.sdk import (
    FEE_TIERS,
    NATIVE_CURRENCY,
    POOL_MANAGER_ADDRESSES,
    QUOTER_ADDRESSES,
    ROUTER_ADDRESSES,
    TICK_SPACING,
    UNISWAP_V4_GAS_ESTIMATES,
    PoolKey,
    SwapQuote,
    UniswapV4SDK,
)


# =============================================================================
# Constants tests
# =============================================================================


class TestConstants:
    def test_fee_tiers(self):
        assert FEE_TIERS == [100, 500, 3000, 10000]

    def test_tick_spacing(self):
        assert TICK_SPACING[100] == 1
        assert TICK_SPACING[3000] == 60

    def test_gas_estimates(self):
        assert UNISWAP_V4_GAS_ESTIMATES["approve"] == 50_000
        assert UNISWAP_V4_GAS_ESTIMATES["swap"] == 200_000

    def test_pool_manager_addresses(self):
        # All chains should have the same pool manager (CREATE2 deployment)
        for chain, addr in POOL_MANAGER_ADDRESSES.items():
            assert addr.lower() == "0x000000000004444c5dc75cb358380d2e3de08a90"

    def test_router_addresses(self):
        assert "arbitrum" in ROUTER_ADDRESSES
        assert "ethereum" in ROUTER_ADDRESSES

    def test_quoter_addresses(self):
        assert "arbitrum" in QUOTER_ADDRESSES


# =============================================================================
# PoolKey tests
# =============================================================================


class TestPoolKey:
    def test_sorted_order(self):
        """Pool key should sort currency0 < currency1."""
        key = PoolKey(
            currency0="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            currency1="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            fee=3000,
            tick_spacing=60,
        )
        # Should swap since 0xbb > 0xaa
        assert int(key.currency0, 16) < int(key.currency1, 16)

    def test_already_sorted(self):
        key = PoolKey(
            currency0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            currency1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=3000,
            tick_spacing=60,
        )
        assert key.currency0 == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        assert key.currency1 == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    def test_native_currency(self):
        """Native ETH (zero address) should always be currency0."""
        key = PoolKey(
            currency0="0xaf88d065e77c8cc2239327c5edb3a432268e5831",  # USDC
            currency1=NATIVE_CURRENCY,
            fee=3000,
            tick_spacing=60,
        )
        assert key.currency0 == NATIVE_CURRENCY

    def test_hooks_default(self):
        key = PoolKey(
            currency0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            currency1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=3000,
            tick_spacing=60,
        )
        assert key.hooks == NATIVE_CURRENCY

    def test_custom_hooks(self):
        key = PoolKey(
            currency0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            currency1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=3000,
            tick_spacing=60,
            hooks="0x1234567890123456789012345678901234567890",
        )
        assert key.hooks == "0x1234567890123456789012345678901234567890"


# =============================================================================
# SDK initialization tests
# =============================================================================


class TestUniswapV4SDKInit:
    def test_init_supported_chain(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        assert sdk.chain == "arbitrum"
        assert sdk.pool_manager.lower() == "0x000000000004444c5dc75cb358380d2e3de08a90"

    def test_init_unsupported_chain(self):
        with pytest.raises(ValueError, match="not supported"):
            UniswapV4SDK(chain="fantom")

    def test_init_case_insensitive(self):
        sdk = UniswapV4SDK(chain="Arbitrum")
        assert sdk.chain == "arbitrum"


# =============================================================================
# Pool key computation tests
# =============================================================================


class TestComputePoolKey:
    def test_default_tick_spacing(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        key = sdk.compute_pool_key(
            token0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=3000,
        )
        assert key.tick_spacing == 60

    def test_custom_tick_spacing(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        key = sdk.compute_pool_key(
            token0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=3000,
            tick_spacing=10,
        )
        assert key.tick_spacing == 10

    def test_fee_100_tick_spacing(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        key = sdk.compute_pool_key(
            token0="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token1="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            fee=100,
        )
        assert key.tick_spacing == 1


# =============================================================================
# Local quote tests
# =============================================================================


class TestGetQuoteLocal:
    def test_basic_quote(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = sdk.get_quote_local(
            token_in="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_out="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            amount_in=10**18,
            fee_tier=3000,
        )
        assert quote.amount_in == 10**18
        assert quote.amount_out > 0
        assert quote.amount_out < 10**18  # Less due to fees

    def test_quote_with_price_ratio(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = sdk.get_quote_local(
            token_in="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_out="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            amount_in=1000 * 10**6,  # 1000 USDC
            fee_tier=500,
            token_in_decimals=6,
            token_out_decimals=18,
            price_ratio=Decimal("0.0005"),  # 1 USDC = 0.0005 ETH
        )
        assert quote.amount_out > 0
        assert quote.effective_price is not None

    def test_quote_fee_deduction(self):
        """Quote should deduct fees from output."""
        sdk = UniswapV4SDK(chain="arbitrum")
        amount_in = 10**18

        # 0.3% fee tier
        quote = sdk.get_quote_local(
            token_in="0xaaaa",
            token_out="0xbbbb",
            amount_in=amount_in,
            fee_tier=3000,
        )
        # Should be approximately 99.7% of input
        expected = int(Decimal(amount_in) * Decimal("0.997"))
        assert abs(quote.amount_out - expected) < 2  # Allow rounding


# =============================================================================
# Transaction building tests
# =============================================================================


class TestBuildSwapTx:
    def test_build_swap_tx(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = SwapQuote(
            amount_in=10**18,
            amount_out=997 * 10**15,
            fee_tier=3000,
            token_in="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_out="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        tx = sdk.build_swap_tx(quote, recipient="0x1234567890123456789012345678901234567890")
        assert tx.to == sdk.router
        assert tx.data.startswith("0xf3cd914c")  # SWAP_SELECTOR
        assert tx.gas_estimate == 200_000
        assert tx.value == 0  # Not native ETH

    def test_build_native_swap_tx(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        quote = SwapQuote(
            amount_in=10**18,
            amount_out=997 * 10**15,
            fee_tier=3000,
            token_in=NATIVE_CURRENCY,
            token_out="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        tx = sdk.build_swap_tx(quote, recipient="0x1234567890123456789012345678901234567890")
        assert tx.value == 10**18  # ETH value set


class TestBuildApproveTx:
    def test_build_approve(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        tx = sdk.build_approve_tx(
            token_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            spender="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            amount=10**18,
        )
        assert tx.data.startswith("0x095ea7b3")
        assert tx.gas_estimate == 50_000
        assert tx.value == 0


# =============================================================================
# Tick math tests
# =============================================================================


class TestTickMath:
    def test_tick_to_price_zero(self):
        price = UniswapV4SDK.tick_to_price(0)
        assert abs(price - Decimal("1")) < Decimal("0.001")

    def test_price_to_tick_roundtrip(self):
        price = Decimal("2000")
        tick = UniswapV4SDK.price_to_tick(price)
        recovered = UniswapV4SDK.tick_to_price(tick)
        # Should be within 0.1% due to tick discretization
        assert abs(float(recovered - price) / float(price)) < 0.001

    def test_price_to_tick_negative(self):
        with pytest.raises(ValueError, match="positive"):
            UniswapV4SDK.price_to_tick(Decimal("-1"))
