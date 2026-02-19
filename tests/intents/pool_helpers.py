"""Pool existence helpers for intent tests.

These helpers validate that a liquidity pool exists before running an intent test.
If the pool doesn't exist, the test fails with a clear message instead of
failing with an unhelpful on-chain revert.

Usage:
    from tests.intents.pool_helpers import fail_if_v3_pool_missing

    class TestUniswapV3SwapIntent:
        async def test_swap_usdc_to_weth(self, web3, ...):
            fail_if_v3_pool_missing(web3, "base", "uniswap_v3", USDC, WETH, 500)
            # ... rest of test
"""

from __future__ import annotations

import pytest

from almanak.framework.intents.pool_validation import (
    validate_aerodrome_pool,
    validate_traderjoe_pool,
    validate_v3_pool,
)


def _get_rpc_url_from_web3(web3) -> str | None:
    """Extract RPC URL from a Web3 instance."""
    try:
        provider = web3.provider
        if hasattr(provider, "endpoint_uri"):
            return str(provider.endpoint_uri)
    except Exception:
        pass
    return None


def fail_if_v3_pool_missing(
    web3,
    chain: str,
    protocol: str,
    token_a: str,
    token_b: str,
    fee_tier: int,
) -> None:
    """Fail the test if a V3 pool doesn't exist for the given pair.

    Args:
        web3: Web3 instance (used to extract RPC URL).
        chain: Chain name.
        protocol: Protocol name ("uniswap_v3", "sushiswap_v3", "pancakeswap_v3").
        token_a: Token A address.
        token_b: Token B address.
        fee_tier: Fee tier in basis points.
    """
    rpc_url = _get_rpc_url_from_web3(web3)
    result = validate_v3_pool(chain, protocol, token_a, token_b, fee_tier, rpc_url)

    if result.exists is False:
        pytest.fail(f"Pool missing: {result.error}")
    if result.exists is None:
        pytest.fail(f"Could not validate pool existence: {result.warning}")


def fail_if_aerodrome_pool_missing(
    web3,
    chain: str,
    token_a: str,
    token_b: str,
    stable: bool,
) -> None:
    """Fail the test if an Aerodrome Classic pool doesn't exist.

    Args:
        web3: Web3 instance (used to extract RPC URL).
        chain: Chain name (should be "base").
        token_a: Token A address.
        token_b: Token B address.
        stable: True for stable pool, False for volatile.
    """
    rpc_url = _get_rpc_url_from_web3(web3)
    result = validate_aerodrome_pool(chain, token_a, token_b, stable, rpc_url)

    if result.exists is False:
        pytest.fail(f"Pool missing: {result.error}")
    if result.exists is None:
        pytest.fail(f"Could not validate pool existence: {result.warning}")


def fail_if_traderjoe_pool_missing(
    web3,
    chain: str,
    token_x: str,
    token_y: str,
    bin_step: int,
) -> None:
    """Fail the test if a TraderJoe V2 pool doesn't exist.

    Args:
        web3: Web3 instance (used to extract RPC URL).
        chain: Chain name.
        token_x: Token X address.
        token_y: Token Y address.
        bin_step: Bin step of the pair.
    """
    rpc_url = _get_rpc_url_from_web3(web3)
    result = validate_traderjoe_pool(chain, token_x, token_y, bin_step, rpc_url)

    if result.exists is False:
        pytest.fail(f"Pool missing: {result.error}")
    if result.exists is None:
        pytest.fail(f"Could not validate pool existence: {result.warning}")
