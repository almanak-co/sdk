"""The Uniswap V3 exact-LP helper must follow pool token0/token1 order.

A bare pool address on LPOpenIntent uses the pool contract's canonical
orientation. Arbitrum/Base WETH/USDC have WETH as token0; Ethereum has USDC.
Hard-coding amount0=WETH would compile inverted amounts and a nonsense range
on Ethereum. Restoring the WETH-is-always-token0 mapping fails the Ethereum
case below.
"""

from __future__ import annotations

from decimal import Decimal

from tests.intents._uniswap_v3_lp_exact_proofs import (
    RANGE_LOWER,
    RANGE_UPPER,
    USDC_AMOUNT,
    WETH_AMOUNT,
    _canonical_weth_usdc_pair,
)

# Arbitrum WETH < USDC by address. Ethereum USDC < WETH.
ARBITRUM_WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
ARBITRUM_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
ETHEREUM_WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
ETHEREUM_USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


def test_arbitrum_keeps_weth_as_token0() -> None:
    token0, token1, amount0, amount1, range_lower, range_upper = _canonical_weth_usdc_pair(
        ARBITRUM_WETH, ARBITRUM_USDC
    )
    assert token0.lower() == ARBITRUM_WETH.lower()
    assert token1.lower() == ARBITRUM_USDC.lower()
    assert amount0 == WETH_AMOUNT
    assert amount1 == USDC_AMOUNT
    assert range_lower == RANGE_LOWER
    assert range_upper == RANGE_UPPER


def test_ethereum_puts_usdc_as_token0_and_inverts_the_price_band() -> None:
    token0, token1, amount0, amount1, range_lower, range_upper = _canonical_weth_usdc_pair(
        ETHEREUM_WETH, ETHEREUM_USDC
    )
    assert token0.lower() == ETHEREUM_USDC.lower()
    assert token1.lower() == ETHEREUM_WETH.lower()
    assert amount0 == USDC_AMOUNT
    assert amount1 == WETH_AMOUNT
    assert range_lower == Decimal(1) / RANGE_UPPER
    assert range_upper == Decimal(1) / RANGE_LOWER
