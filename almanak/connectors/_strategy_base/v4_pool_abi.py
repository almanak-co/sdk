"""Uniswap V4 pool-read ABI shared by the framework pool reader.

V4 has NO per-pool contracts: all pool state lives in the PoolManager
singleton and is read through the StateView periphery contract, keyed by a
``bytes32 PoolId = keccak256(abi.encode(PoolKey))`` where the PoolKey is
``(currency0, currency1, fee, tickSpacing, hooks)`` with currencies in
ascending numeric order and native ETH as the zero address.

This module is the CANONICAL PoolId computation: the connector's
``uniswap_v4.hooks.compute_pool_id`` delegates here (single implementation,
relocated to the ``_strategy_base`` foundation layer so framework code never
imports a concrete connector module). The delegation is pinned by
``tests/unit/data/test_uniswap_v4_pool_reader.py``.

Selectors are DERIVED from signatures (never hand-typed hex). The deployed
``StateView.getSlot0`` takes the ``bytes32 PoolId`` — selector ``0xc815641c``
— NOT the PoolKey tuple (the tuple-arg form does not exist on-chain and
reverts; VIB-5038 / VIB-5024).
"""

from __future__ import annotations

from eth_utils import function_signature_to_4byte_selector, keccak

# Native ETH / "no hooks" sentinel — V4 encodes both as the zero address.
V4_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Canonical default tick spacing per fee tier for vanilla (hookless) pools.
# V4 allows arbitrary spacings at initialization, but these are the standard
# pairings the official frontends deploy with. The connector's
# ``uniswap_v4.sdk.TICK_SPACING`` re-exports this map (single source).
V4_DEFAULT_TICK_SPACING: dict[int, int] = {
    100: 1,  # 0.01%
    500: 10,  # 0.05%
    3000: 60,  # 0.3%
    10000: 200,  # 1%
}


def _selector(signature: str) -> str:
    """0x-prefixed 4-byte selector derived from a function signature."""
    return "0x" + function_signature_to_4byte_selector(signature).hex()


# StateView read selectors (bytes32 PoolId argument).
V4_GET_SLOT0_SELECTOR = _selector("getSlot0(bytes32)")  # == 0xc815641c
V4_GET_LIQUIDITY_SELECTOR = _selector("getLiquidity(bytes32)")


def _pad_address_word(addr: str) -> str:
    """Left-pad a 20-byte address to a 32-byte ABI word (hex, no 0x)."""
    clean = addr.lower().removeprefix("0x")
    if len(clean) != 40:
        raise ValueError(f"address must be 20 bytes, got {len(clean) // 2}")
    int(clean, 16)
    return clean.zfill(64)


def _pad_uint24_word(value: int) -> str:
    """Left-pad a uint24 to a 32-byte ABI word (hex, no 0x)."""
    if not 0 <= value < (1 << 24):
        raise ValueError(f"uint24 out of range: {value}")
    return hex(value)[2:].zfill(64)


def _pad_int24_word(value: int) -> str:
    """Left-pad a signed int24 to a 32-byte ABI word (two's complement)."""
    if not -(1 << 23) <= value < (1 << 23):
        raise ValueError(f"int24 out of range: {value}")
    if value < 0:
        value = (1 << 256) + value
    return hex(value)[2:].zfill(64)


def compute_v4_pool_id(
    currency0: str,
    currency1: str,
    fee: int,
    tick_spacing: int,
    hooks: str = V4_ZERO_ADDRESS,
) -> str:
    """Compute the V4 PoolId: ``keccak256(abi.encode(PoolKey))``.

    Currencies are sorted into ascending numeric order first (mirroring the
    on-chain PoolKey invariant and the connector's ``PoolKey.__post_init__``),
    so callers may pass them in either order.

    Returns:
        0x-prefixed 32-byte hex string (66 chars).
    """
    c0, c1 = currency0.lower(), currency1.lower()
    if int(c0, 16) > int(c1, 16):
        c0, c1 = c1, c0
    encoded = (
        _pad_address_word(c0)
        + _pad_address_word(c1)
        + _pad_uint24_word(fee)
        + _pad_int24_word(tick_spacing)
        + _pad_address_word(hooks)
    )
    return "0x" + keccak(bytes.fromhex(encoded)).hex()


def encode_get_slot0(pool_id: str) -> str:
    """Calldata for ``StateView.getSlot0(bytes32 poolId)``."""
    return V4_GET_SLOT0_SELECTOR + _pool_id_word(pool_id)


def encode_get_liquidity(pool_id: str) -> str:
    """Calldata for ``StateView.getLiquidity(bytes32 poolId)``."""
    return V4_GET_LIQUIDITY_SELECTOR + _pool_id_word(pool_id)


def _pool_id_word(pool_id: str) -> str:
    """Validate and normalize a PoolId to its 32-byte hex word (no 0x)."""
    clean = pool_id.lower().removeprefix("0x")
    if len(clean) != 64:
        raise ValueError(f"PoolId must be 32 bytes, got {len(clean) // 2}")
    int(clean, 16)
    return clean


__all__ = [
    "V4_DEFAULT_TICK_SPACING",
    "V4_GET_LIQUIDITY_SELECTOR",
    "V4_GET_SLOT0_SELECTOR",
    "V4_ZERO_ADDRESS",
    "compute_v4_pool_id",
    "encode_get_liquidity",
    "encode_get_slot0",
]
