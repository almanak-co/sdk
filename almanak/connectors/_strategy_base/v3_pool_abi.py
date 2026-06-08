"""Protocol-clean ABI helpers for Uniswap V3-style pool reads."""

from __future__ import annotations

V3_GET_POOL_SELECTOR = "0x1698ee82"
V3_SLOT0_SELECTOR = "0x3850c7bd"
V3_LIQUIDITY_SELECTOR = "0x1a686502"
V3_TOKEN0_SELECTOR = "0x0dfe1681"
V3_TOKEN1_SELECTOR = "0xd21220a7"
V3_FEE_SELECTOR = "0xddca3f43"


def encode_get_pool(selector: str, token_a: str, token_b: str, fee_or_spacing: int) -> str:
    """Encode ``getPool(address,address,uint24/int24)`` calldata.

    ``fee_or_spacing`` is right-aligned into a 32-byte ABI word. Negative
    ``int24`` tick spacings are sign-extended via two's complement — plain
    ``hex()`` slicing would emit malformed calldata for negative inputs.
    """
    if not isinstance(fee_or_spacing, int):
        raise TypeError(f"fee_or_spacing must be int, got {type(fee_or_spacing).__name__}")
    a = token_a.lower().replace("0x", "").zfill(64)
    b = token_b.lower().replace("0x", "").zfill(64)
    fee = (fee_or_spacing & ((1 << 256) - 1)).to_bytes(32, "big").hex()
    return selector + a + b + fee


def encode_v3_get_pool(token_a: str, token_b: str, fee_tier: int) -> str:
    """Encode canonical V3 factory ``getPool(address,address,uint24)`` calldata."""
    return encode_get_pool(V3_GET_POOL_SELECTOR, token_a, token_b, fee_tier)


__all__ = [
    "V3_FEE_SELECTOR",
    "V3_GET_POOL_SELECTOR",
    "V3_LIQUIDITY_SELECTOR",
    "V3_SLOT0_SELECTOR",
    "V3_TOKEN0_SELECTOR",
    "V3_TOKEN1_SELECTOR",
    "encode_get_pool",
    "encode_v3_get_pool",
]
