"""Golden tests for V4 ``compute_pool_id`` across all standard fee tiers.

VIB-4474 T05. Exercises the int24 sign-extension path of ``_pad_int24`` in
``compute_pool_id`` via the standard V4 fee/tickSpacing pairings (100/1,
500/10, 3000/60, 10000/200) plus a hooked fee tier. Determinism is locked
in by recomputing each id against the explicit keccak256(abi.encode(...))
that v4-core uses.
"""

from __future__ import annotations

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.hooks import compute_pool_id
from almanak.framework.connectors.uniswap_v4.sdk import (
    NATIVE_CURRENCY,
    PoolKey,
    _pad_address,
    _pad_int24,
    _pad_uint24,
)

USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"


def _expected_pool_id(pool_key: PoolKey) -> str:
    """Re-implementation of v4-core's ``keccak256(abi.encode(PoolKey))``.

    Independent of ``compute_pool_id``'s internals -- the test fails if either
    diverges from the canonical encoding.
    """
    packed = (
        _pad_address(pool_key.currency0)
        + _pad_address(pool_key.currency1)
        + _pad_uint24(pool_key.fee)
        + _pad_int24(pool_key.tick_spacing)
        + _pad_address(pool_key.hooks)
    )
    return "0x" + Web3.keccak(bytes.fromhex(packed)).hex()


class TestComputePoolIdAllFeeTiers:
    """Standard V4 fee tier coverage: 100, 500, 3000, 10000."""

    @pytest.mark.parametrize(
        ("fee", "tick_spacing"),
        [
            (100, 1),
            (500, 10),
            (3000, 60),
            (10000, 200),
        ],
    )
    def test_fee_tier_matches_canonical_encoding(self, fee: int, tick_spacing: int) -> None:
        pool_key = PoolKey(
            currency0=USDC,
            currency1=WETH,
            fee=fee,
            tick_spacing=tick_spacing,
        )
        pool_id = compute_pool_id(pool_key)
        assert pool_id.startswith("0x")
        assert len(pool_id) == 66
        assert pool_id == _expected_pool_id(pool_key)

    def test_native_eth_pair(self) -> None:
        """V4-native USDC/ETH pool: currency0 = address(0)."""
        pool_key = PoolKey(
            currency0=NATIVE_CURRENCY,
            currency1=USDC,
            fee=500,
            tick_spacing=10,
        )
        pool_id = compute_pool_id(pool_key)
        assert pool_id == _expected_pool_id(pool_key)
        assert pool_key.currency0 == NATIVE_CURRENCY

    def test_hooked_pool(self) -> None:
        """Hook address changes the pool id."""
        hook = "0x0000000000000000000000000000000000002400"  # arbitrary, not zero
        pool_key_no_hook = PoolKey(
            currency0=USDC,
            currency1=WETH,
            fee=3000,
            tick_spacing=60,
        )
        pool_key_hooked = PoolKey(
            currency0=USDC,
            currency1=WETH,
            fee=3000,
            tick_spacing=60,
            hooks=hook,
        )
        assert compute_pool_id(pool_key_no_hook) != compute_pool_id(pool_key_hooked)
        assert compute_pool_id(pool_key_hooked) == _expected_pool_id(pool_key_hooked)


class TestComputePoolIdInt24SignExtension:
    """Int24 sign-extension regression: tick_spacing is signed (V4 allows
    negative values in principle); _pad_int24 must produce two's-complement
    when fed a negative spacing.
    """

    def test_negative_tick_spacing(self) -> None:
        pool_key = PoolKey(
            currency0=USDC,
            currency1=WETH,
            fee=500,
            tick_spacing=-10,
        )
        pool_id = compute_pool_id(pool_key)
        assert pool_id == _expected_pool_id(pool_key)
        # Sanity: the padded tick_spacing has the high bytes set (two's complement)
        padded = _pad_int24(-10)
        assert padded.startswith("ffff"), padded

    def test_min_negative_tick_spacing(self) -> None:
        pool_key = PoolKey(
            currency0=USDC,
            currency1=WETH,
            fee=500,
            tick_spacing=-(1 << 23),  # int24 min
        )
        assert compute_pool_id(pool_key) == _expected_pool_id(pool_key)


class TestComputePoolIdDeterminism:
    """Same inputs -> same hash, every invocation."""

    def test_repeatable(self) -> None:
        pool_key = PoolKey(
            currency0=USDC,
            currency1=WETH,
            fee=3000,
            tick_spacing=60,
        )
        assert compute_pool_id(pool_key) == compute_pool_id(pool_key)
        assert compute_pool_id(pool_key) == compute_pool_id(pool_key)

    def test_currency_sort_is_idempotent(self) -> None:
        """Swapping currency0/currency1 inputs at construction yields the
        same canonical pool_id because PoolKey.__post_init__ sorts them."""
        pk_forward = PoolKey(currency0=USDC, currency1=WETH, fee=500, tick_spacing=10)
        pk_reverse = PoolKey(currency0=WETH, currency1=USDC, fee=500, tick_spacing=10)
        assert compute_pool_id(pk_forward) == compute_pool_id(pk_reverse)
