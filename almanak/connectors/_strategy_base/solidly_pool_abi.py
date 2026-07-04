"""Protocol-clean ABI helpers for getReserves-family (Solidly / V2) pool reads."""

from __future__ import annotations

# keccak4 of the canonical Solidly pool / factory signatures:
#   getReserves()        -> (uint256 reserve0, uint256 reserve1, uint256 blockTimestampLast)
#                           (selector shared with Uniswap V2 pairs)
#   stable()             -> bool  (Solidly-only — the positive Solidly classification signal)
#   factory()            -> address
#   getFee(address,bool) -> uint256  (PoolFactory; fee in basis points, e.g. 30 = 0.30%)
SOLIDLY_GET_RESERVES_SELECTOR = "0x0902f1ac"
SOLIDLY_STABLE_SELECTOR = "0x22be3de1"
SOLIDLY_FACTORY_SELECTOR = "0xc45a0155"
SOLIDLY_GET_FEE_SELECTOR = "0xcc56b2c5"

# keccak4 of ``price0CumulativeLast()`` — the canonical Uniswap-V2 oracle
# getter. Answered by V2-family pairs, absent on Solidly pools (they expose
# ``reserve0CumulativeLast`` instead), so it serves as the positive V2
# classification signal: an empty ``stable()`` response alone can also mean a
# transient transport failure and must never downgrade a pool to V2.
V2_PRICE0_CUMULATIVE_LAST_SELECTOR = "0x5909c0d5"


def encode_solidly_get_fee(pool_address: str, stable: bool) -> str:
    """Encode Solidly ``PoolFactory.getFee(address pool, bool _stable)`` calldata.

    Raises:
        ValueError: If ``pool_address`` is not a 20-byte hex address — silent
            zero-padding of a malformed address would produce calldata that
            queries the fee of a different (or nonexistent) pool.
    """
    raw = pool_address.lower().removeprefix("0x")
    if len(raw) != 40 or any(c not in "0123456789abcdef" for c in raw):
        raise ValueError(f"pool_address must be a 20-byte hex address, got {pool_address!r}")
    stable_word = int(bool(stable)).to_bytes(32, "big").hex()
    return SOLIDLY_GET_FEE_SELECTOR + raw.zfill(64) + stable_word


__all__ = [
    "SOLIDLY_FACTORY_SELECTOR",
    "SOLIDLY_GET_FEE_SELECTOR",
    "SOLIDLY_GET_RESERVES_SELECTOR",
    "SOLIDLY_STABLE_SELECTOR",
    "V2_PRICE0_CUMULATIVE_LAST_SELECTOR",
    "encode_solidly_get_fee",
]
