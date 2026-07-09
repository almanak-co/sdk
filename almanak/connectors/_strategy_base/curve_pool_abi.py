"""Curve pool read ABI shared by the framework pool reader.

Curve pools do not speak the Uniswap-V3 slot0() ABI: state is read via
``coins(i)`` / ``balances(i)`` / ``get_dy(i, j, dx)`` / ``fee()``. Two ABI
families exist for the index-taking getters:

- Modern pools (factory / NG / crypto) index with ``uint256``.
- Legacy Vyper pools (e.g. mainnet 3pool, steth) index with ``int128``.

Both encode a non-negative index identically (one 32-byte big-endian word),
so only the 4-byte selector differs — readers probe the ``uint256`` form
first and fall back to ``int128``. The same split applies to ``get_dy``:
StableSwap pools expose ``get_dy(int128,int128,uint256)`` while the
CryptoSwap / Tricrypto family exposes ``get_dy(uint256,uint256,uint256)``.

Selectors are DERIVED from their signatures (never hand-typed hex) so the
calldata cannot drift from the ABI; the derivation is pinned against the
Curve adapter's on-chain-verified literals in
``tests/unit/data/test_curve_pool_reader.py``.
"""

from __future__ import annotations

from eth_utils import function_signature_to_4byte_selector


def _selector(signature: str) -> str:
    """0x-prefixed 4-byte selector derived from a function signature."""
    return "0x" + function_signature_to_4byte_selector(signature).hex()


# get_dy quote — StableSwap (int128 indices) vs CryptoSwap/Tricrypto (uint256).
CURVE_GET_DY_INT128_SELECTOR = _selector("get_dy(int128,int128,uint256)")
CURVE_GET_DY_UINT256_SELECTOR = _selector("get_dy(uint256,uint256,uint256)")

# coins(i) — modern uint256 form vs legacy Vyper int128 form.
CURVE_COINS_UINT256_SELECTOR = _selector("coins(uint256)")
CURVE_COINS_INT128_SELECTOR = _selector("coins(int128)")

# balances(i) — same family split as coins(i).
CURVE_BALANCES_UINT256_SELECTOR = _selector("balances(uint256)")
CURVE_BALANCES_INT128_SELECTOR = _selector("balances(int128)")

# fee() -> uint256, 1e10-scaled (4000000 == 0.04%). Signature-identical to the
# v3 family's fee() so the selector coincides with V3_FEE_SELECTOR; the RETURN
# SCALE differs (v3: 1e-6 units), so readers must convert, never pass through.
CURVE_FEE_SELECTOR = _selector("fee()")

# fee() return scale: value / 1e10 = fraction. Dividing the raw value by 1e4
# converts it to the v3 fee-tier unit (1e-6 fraction units, e.g. 500 = 0.05%).
CURVE_FEE_SCALE_TO_V3_UNITS = 10**4

# Discriminator slot value for Curve known-pool keys. Curve pools carry no
# fee-tier / tick-spacing discriminator: the connector spec keys every curated
# pair under this fixed slot and the reader ignores the caller's fee_tier.
CURVE_POOL_KEY = 0


def _pad_uint256(value: int) -> str:
    """Left-pad a non-negative int to a 32-byte ABI word (hex, no 0x)."""
    if value < 0:
        raise ValueError(f"cannot ABI-encode negative value {value} as a padded word")
    if value >= 1 << 256:
        raise ValueError(f"value {value} exceeds uint256 — would overflow a 32-byte ABI word")
    return hex(value)[2:].zfill(64)


def encode_index_call(selector: str, index: int) -> str:
    """Calldata for a single-index getter (``coins(i)`` / ``balances(i)``).

    Valid for BOTH the uint256 and int128 ABI families: a non-negative index
    encodes identically in either, only the selector differs.
    """
    return selector + _pad_uint256(index)


def encode_get_dy(selector: str, i: int, j: int, dx: int) -> str:
    """Calldata for ``get_dy(i, j, dx)`` in either ABI family."""
    return selector + _pad_uint256(i) + _pad_uint256(j) + _pad_uint256(dx)


__all__ = [
    "CURVE_BALANCES_INT128_SELECTOR",
    "CURVE_BALANCES_UINT256_SELECTOR",
    "CURVE_COINS_INT128_SELECTOR",
    "CURVE_COINS_UINT256_SELECTOR",
    "CURVE_FEE_SCALE_TO_V3_UNITS",
    "CURVE_FEE_SELECTOR",
    "CURVE_POOL_KEY",
    "CURVE_GET_DY_INT128_SELECTOR",
    "CURVE_GET_DY_UINT256_SELECTOR",
    "encode_get_dy",
    "encode_index_call",
]
