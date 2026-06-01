"""Aerodrome pool-existence validation (connector-owned).

The Aerodrome connector owns both its Classic factory validator
(``getPool(address,address,bool)`` — selector ``0x79bc57d5``) and its
Slipstream / concentrated-liquidity factory validator
(``getPool(address,address,int24)`` — selector ``0x28af8d0b``, resolved against
the ``cl_factory`` contract kind). Factory addresses are resolved through
:class:`AddressRegistry` rather than hardcoded here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.pool_validation_base import (
    ZERO_ADDRESS,
    PoolValidationReason,
    PoolValidationResult,
    decode_address,
    eth_call,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

__all__ = [
    "validate_aerodrome_cl_pool",
    "validate_aerodrome_pool",
]

# Aerodrome Classic getPool(address,address,bool) selector
# See `almanak/connectors/aerodrome/abis/pool_factory.json`
_AERODROME_GET_POOL_SELECTOR = "0x79bc57d5"

# Aerodrome Slipstream CL getPool(address,address,int24) selector
_AERODROME_CL_GET_POOL_SELECTOR = "0x28af8d0b"


def _encode_get_pool_aerodrome(token_a: str, token_b: str, stable: bool) -> str:
    """Encode getPool(address,address,bool) calldata for Aerodrome factory."""
    a = token_a.lower().replace("0x", "").zfill(64)
    b = token_b.lower().replace("0x", "").zfill(64)
    s = "1".zfill(64) if stable else "0".zfill(64)
    return _AERODROME_GET_POOL_SELECTOR + a + b + s


def _encode_get_pool_aerodrome_cl(token_a: str, token_b: str, tick_spacing: int) -> str:
    """Encode getPool(address,address,int24) calldata for Aerodrome CL factory."""
    a = token_a.lower().replace("0x", "").zfill(64)
    b = token_b.lower().replace("0x", "").zfill(64)
    # tick_spacing is always positive, safe to encode as uint
    ts = hex(tick_spacing)[2:].zfill(64)
    return _AERODROME_CL_GET_POOL_SELECTOR + a + b + ts


def validate_aerodrome_pool(
    chain: str,
    token_a: str,
    token_b: str,
    stable: bool,
    rpc_url: str | None,
    gateway_client: GatewayClient | None = None,
) -> PoolValidationResult:
    """Validate that an Aerodrome Classic pool exists on-chain.

    Args:
        chain: Chain name (should be "base").
        token_a: Token A address.
        token_b: Token B address.
        stable: True for stable pool, False for volatile.
        rpc_url: RPC URL for on-chain query. If None, returns unknown unless gateway_client is available.
        gateway_client: Optional connected gateway client for gateway-routed eth_call.

    Returns:
        PoolValidationResult with exists=True/False/None.
    """
    if rpc_url is None and gateway_client is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning=f"No RPC URL available — cannot verify Aerodrome pool existence on {chain}",
        )

    factory = AddressRegistry.resolve_contract_address("aerodrome", chain, "factory")
    if not factory:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning=f"No Aerodrome factory address for chain '{chain}' — cannot verify pool existence",
        )

    calldata = _encode_get_pool_aerodrome(token_a, token_b, stable)
    raw = eth_call(rpc_url or "", factory, calldata, chain=chain, gateway_client=gateway_client)

    if raw is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning=f"RPC call to Aerodrome factory failed on {chain} — cannot verify pool existence",
        )

    pool_address = decode_address(raw)
    pool_type = "stable" if stable else "volatile"

    if pool_address == ZERO_ADDRESS:
        return PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error=(
                f"No Aerodrome {pool_type} pool found for "
                f"{token_a[:10]}.../{token_b[:10]}... on {chain}. "
                f"The pool may not exist or may use a different pool type "
                f"(try {'volatile' if stable else 'stable'})."
            ),
        )

    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool_address)


def validate_aerodrome_cl_pool(
    chain: str,
    token_a: str,
    token_b: str,
    tick_spacing: int,
    rpc_url: str | None,
    gateway_client: GatewayClient | None = None,
) -> PoolValidationResult:
    """Validate that an Aerodrome Slipstream (CL) pool exists on-chain.

    Args:
        chain: Chain name (should be "base").
        token_a: Token A address.
        token_b: Token B address.
        tick_spacing: CL pool tick spacing (e.g. 100).
        rpc_url: RPC URL for on-chain query. If None, returns unknown unless gateway_client is available.
        gateway_client: Optional connected gateway client for gateway-routed eth_call.

    Returns:
        PoolValidationResult with exists=True/False/None.
    """
    if rpc_url is None and gateway_client is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning=f"No RPC URL available — cannot verify Aerodrome CL pool existence on {chain}",
        )

    cl_factory = AddressRegistry.resolve_contract_address("aerodrome", chain, "cl_factory")
    if not cl_factory:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning=f"No Aerodrome CL factory address for chain '{chain}' — cannot verify pool existence",
        )

    calldata = _encode_get_pool_aerodrome_cl(token_a, token_b, tick_spacing)
    raw = eth_call(rpc_url or "", cl_factory, calldata, chain=chain, gateway_client=gateway_client)

    if raw is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning=f"RPC call to Aerodrome CL factory failed on {chain} — cannot verify pool existence",
        )

    pool_address = decode_address(raw)

    if pool_address == ZERO_ADDRESS:
        return PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error=(
                f"No Aerodrome CL pool found for "
                f"{token_a[:10]}.../{token_b[:10]}... with tick spacing {tick_spacing} on {chain}. "
                f"The pool may not exist or may use a different tick spacing."
            ),
        )

    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool_address)
