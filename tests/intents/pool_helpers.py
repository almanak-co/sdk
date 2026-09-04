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

from almanak.connectors.aerodrome.pool_validation import (
    validate_aerodrome_cl_pool,
    validate_aerodrome_pool,
)
from almanak.connectors.traderjoe_v2.pool_validation import validate_traderjoe_pool
from almanak.connectors.uniswap_v3.pool_validation import validate_v3_pool


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


def fail_if_v4_pool_missing(
    web3,
    chain: str,
    token_a: str,
    token_b: str,
    fee_tier: int,
) -> None:
    """Fail the test if the Uniswap V4 pool for the pair and fee tier is uninitialised.

    V4 pools live inside the PoolManager singleton and are keyed by the
    PoolKey hash, so existence is read through ``StateView.getSlot0`` at the
    fork block: an uninitialised pool answers ``sqrtPriceX96 == 0``. When the
    requested tier is missing, the failure names every canonical tier that IS
    initialised so the test's fee constant can be corrected in one edit.

    An unreadable StateView (revert or RPC error) is reported but never fails
    the test: the LP compiler tolerates the same read failing by falling back
    to an estimated price, and the execution and balance layers still fail
    closed on a pool that does not exist.
    """
    from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
    from almanak.connectors.uniswap_v4.hooks import build_get_slot0_calldata, decode_slot0_response
    from almanak.connectors.uniswap_v4.sdk import FEE_TIERS, TICK_SPACING, PoolKey

    state_view = UNISWAP_V4[chain]["state_view"]

    def _initialised(tier: int) -> bool | None:
        key = PoolKey(currency0=token_a, currency1=token_b, fee=tier, tick_spacing=TICK_SPACING[tier])
        try:
            raw = web3.eth.call({"to": state_view, "data": build_get_slot0_calldata(key)})
        except Exception as exc:  # noqa: BLE001 - any revert means "not readable here"
            print(f"Warning: StateView.getSlot0 on {chain} ({state_view}) unreadable for fee={tier}: {exc}")
            return None
        return decode_slot0_response(web3.to_hex(raw)).exists

    requested = _initialised(fee_tier)
    if requested is None:
        print(f"Warning: skipping V4 pool preflight for {token_a}/{token_b} on {chain}; later layers fail closed")
        return
    if requested:
        return
    others = [tier for tier in FEE_TIERS if tier != fee_tier and _initialised(tier)]
    pytest.fail(
        f"Uniswap V4 pool {token_a}/{token_b} fee={fee_tier} is not initialised on {chain} at the fork block "
        f"(StateView {state_view}); initialised canonical tiers for this pair: {others or 'none'}"
    )


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


def fail_if_aerodrome_cl_pool_missing(
    web3,
    chain: str,
    token_a: str,
    token_b: str,
    tick_spacing: int,
) -> None:
    """Fail the test if an Aerodrome Slipstream CL pool doesn't exist.

    Args:
        web3: Web3 instance (used to extract RPC URL).
        chain: Chain name (should be "base").
        token_a: Token A address.
        token_b: Token B address.
        tick_spacing: CL pool tick spacing (e.g. 100).
    """
    rpc_url = _get_rpc_url_from_web3(web3)
    result = validate_aerodrome_cl_pool(chain, token_a, token_b, tick_spacing, rpc_url)

    if result.exists is False:
        pytest.fail(f"CL pool missing: {result.error}")
    if result.exists is None:
        pytest.fail(f"Could not validate CL pool existence: {result.warning}")


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
