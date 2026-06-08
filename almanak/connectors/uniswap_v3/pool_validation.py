"""Uniswap V3-family pool-existence validation (connector-owned).

The Uniswap V3 connector owns the single ``getPool(address,address,uint24)``
validator for the whole :class:`~almanak.connectors._strategy_base.address_registry.AbiFamily.V3_FACTORY`
family — ``uniswap_v3`` and its forks (``sushiswap_v3``, ``pancakeswap_v3``,
``agni_finance``) all expose the same factory interface and are grouped under
``AbiFamily.V3_FACTORY`` in the :class:`AddressRegistry`. The per-fork factory
address is resolved through ``AddressRegistry.resolve_contract_address`` rather
than hardcoded here, so the forks share this one validator instead of copying it.

This module also owns ``slot0()`` fetching (:func:`fetch_v3_pool_sqrt_price_x96`),
used by the compiler to recompute LP deposit amounts and avoid price-slippage
reverts, since it speaks the same V3 pool ABI.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.address_registry import AbiFamily, AddressRegistry
from almanak.connectors._strategy_base.pool_validation_base import (
    ZERO_ADDRESS,
    PoolValidationReason,
    PoolValidationResult,
    decode_address,
    eth_call,
)
from almanak.connectors._strategy_base.v3_pool_abi import (
    V3_GET_POOL_SELECTOR,
    V3_SLOT0_SELECTOR,
    encode_v3_get_pool,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

__all__ = [
    "V3_GET_POOL_SELECTOR",
    "fetch_v3_pool_sqrt_price_x96",
    "validate_v3_pool",
]


def _encode_get_pool_v3(token_a: str, token_b: str, fee: int) -> str:
    """Encode getPool(address,address,uint24) calldata for V3 factories."""
    return encode_v3_get_pool(token_a, token_b, fee)


def validate_v3_pool(
    chain: str,
    protocol: str,
    token_a: str,
    token_b: str,
    fee_tier: int,
    rpc_url: str | None,
    gateway_client: GatewayClient | None = None,
) -> PoolValidationResult:
    """Validate that a V3-style pool exists on-chain.

    Works for Uniswap V3 and its forks (SushiSwap V3, PancakeSwap V3, Agni).

    Args:
        chain: Chain name (e.g. "arbitrum", "base").
        protocol: Protocol name ("uniswap_v3", "sushiswap_v3", "pancakeswap_v3", "agni_finance").
        token_a: Token A address (checksummed or lowercase).
        token_b: Token B address (checksummed or lowercase).
        fee_tier: Fee tier in basis points (e.g. 500, 3000).
        rpc_url: RPC URL for on-chain query. If None, returns unknown unless gateway_client is available.
        gateway_client: Optional connected gateway client for gateway-routed eth_call.

    Returns:
        PoolValidationResult with exists=True/False/None.
    """
    if rpc_url is None and gateway_client is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning=f"No RPC URL available — cannot verify {protocol} pool existence on {chain}",
        )

    if not AddressRegistry.has_abi(protocol, AbiFamily.V3_FACTORY):
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.PROTOCOL_UNKNOWN,
            warning=f"Unknown protocol '{protocol}' — cannot verify pool existence",
        )

    factory = AddressRegistry.resolve_contract_address(protocol, chain, "factory")
    if not factory:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning=f"No {protocol} factory address for chain '{chain}' — cannot verify pool existence",
        )

    calldata = _encode_get_pool_v3(token_a, token_b, fee_tier)
    raw = eth_call(rpc_url or "", factory, calldata, chain=chain, gateway_client=gateway_client)

    if raw is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning=f"RPC call to {protocol} factory failed on {chain} — cannot verify pool existence",
        )

    pool_address = decode_address(raw)

    if pool_address == ZERO_ADDRESS:
        return PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error=(
                f"No {protocol} pool found for "
                f"{token_a[:10]}.../{token_b[:10]}... with fee tier {fee_tier} on {chain}. "
                f"The pool may not exist or may use a different fee tier."
            ),
        )

    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool_address)


def fetch_v3_pool_sqrt_price_x96(
    pool_address: str,
    rpc_url: str | None,
    *,
    chain: str | None = None,
    gateway_client: GatewayClient | None = None,
) -> tuple[int, int] | None:
    """Fetch sqrtPriceX96 and current tick from a Uniswap V3-compatible pool's slot0().

    Calls slot0() on the pool contract and returns the first two return values:
    sqrtPriceX96 (uint160) and tick (int24). Both are used by the compiler to
    recompute LP deposit amounts and avoid "Price slippage check" reverts.

    The tick is used for exact integer branch selection (below/in/above range),
    avoiding float precision issues at tick boundaries.

    Args:
        pool_address: Pool contract address.
        rpc_url: RPC URL for on-chain query.
        chain: Chain name for gateway-routed eth_call.
        gateway_client: Optional connected gateway client for gateway-routed eth_call.

    Returns:
        (sqrtPriceX96, current_tick) as (int, int), or None on any failure.
    """
    raw = eth_call(rpc_url or "", pool_address, V3_SLOT0_SELECTOR, chain=chain, gateway_client=gateway_client)
    if raw is None or len(raw) < 64:
        return None
    sqrt_price_x96 = int.from_bytes(raw[:32], "big")
    # Sanity check: sqrtPriceX96 must be within Uniswap V3 valid range
    if sqrt_price_x96 < 4295128739 or sqrt_price_x96 > 1461446703485210103287273052203988822378723970342:
        return None
    # ABI-decode int24 tick (sign-extended to int256 in ABI encoding)
    tick_raw = int.from_bytes(raw[32:64], "big")
    if tick_raw >= 2**255:
        tick_raw -= 2**256
    # Validate tick is within Uniswap V3 bounds
    if tick_raw < -887272 or tick_raw > 887272:
        return None
    return sqrt_price_x96, tick_raw
