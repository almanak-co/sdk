"""Unit tests for UniswapV4PoolReader (reader_kind ``uniswap_v4_stateview``).

Scripted-RPC tests against an impersonated StateView periphery: V4 pool state
lives in the PoolManager singleton keyed by ``bytes32 PoolId``, so the reader
resolves synthetic pool identifiers offline (PoolKey hashing REUSED from the
connector via ``_strategy_base.v4_pool_abi`` — pinned here against both the
connector's own ``compute_pool_id`` and the published mainnet ETH/USDC pool
id) and reads ``getSlot0`` / ``getLiquidity`` through the injected rpc_call.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from eth_utils import keccak

from almanak.connectors._strategy_base.v4_pool_abi import (
    V4_GET_LIQUIDITY_SELECTOR,
    V4_GET_SLOT0_SELECTOR,
    V4_ZERO_ADDRESS,
    compute_v4_pool_id,
    encode_get_slot0,
)
from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.models import DataClassification
from almanak.framework.data.pools.reader import PoolReaderRegistry, UniswapV4PoolReader

# Base-chain fixtures.
WETH_BASE = "0x4200000000000000000000000000000000000006"
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
# Mainnet fixtures for the published pool-id vector.
USDC_ETH = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
NATIVE_PLACEHOLDER = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

STATE_VIEW_BASE = "0xA3c0c9b65baD0b08107Aa264b0f3dB444b867A71"  # addresses.py["base"]["state_view"]


def _uint256_bytes(value: int) -> bytes:
    return value.to_bytes(32, byteorder="big")


def _int256_bytes(value: int) -> bytes:
    return value.to_bytes(32, byteorder="big", signed=True)


class ScriptedStateView:
    """A scripted rpc_call impersonating StateView + coin ERC-20s.

    ``pools`` maps pool_id (lowercase 0x-hex) -> (sqrt_price_x96, tick,
    protocol_fee, lp_fee, liquidity). Unknown ids read back all-zero words —
    exactly how the real StateView answers an uninitialized PoolId.
    """

    def __init__(
        self,
        pools: dict[str, tuple[int, int, int, int, int]],
        decimals: dict[str, int],
    ) -> None:
        self.pools = pools
        self.decimals = {addr.lower(): dec for addr, dec in decimals.items()}
        self.calls: list[tuple[str, str, str]] = []

    def __call__(self, chain: str, to: str, calldata: str) -> bytes:
        self.calls.append((chain, to, calldata))
        selector = calldata[:10]
        if to.lower() == STATE_VIEW_BASE.lower():
            pool_id = "0x" + calldata[10:74].lower()
            state = self.pools.get(pool_id, (0, 0, 0, 0, 0))
            sqrt_price_x96, tick, protocol_fee, lp_fee, liquidity = state
            if selector == V4_GET_SLOT0_SELECTOR:
                return (
                    _uint256_bytes(sqrt_price_x96)
                    + _int256_bytes(tick)
                    + _uint256_bytes(protocol_fee)
                    + _uint256_bytes(lp_fee)
                )
            if selector == V4_GET_LIQUIDITY_SELECTOR:
                return _uint256_bytes(liquidity)
        if selector == "0x313ce567" and to.lower() in self.decimals:
            return _uint256_bytes(self.decimals[to.lower()])
        raise ValueError(f"revert: unknown call {to} {selector}")


def _v4_reader(rpc_call) -> UniswapV4PoolReader:
    registry = PoolReaderRegistry(rpc_call=rpc_call)
    reader = registry.get_reader("base", "uniswap_v4")
    assert type(reader) is UniswapV4PoolReader
    return reader


def _pool_id(token_a: str, token_b: str, fee: int, tick_spacing: int) -> str:
    return compute_v4_pool_id(token_a, token_b, fee, tick_spacing)


# ---------------------------------------------------------------------------
# PoolId derivation — REUSED from the connector, cross-checked, and pinned
# ---------------------------------------------------------------------------


def test_pool_id_matches_connector_compute_pool_id() -> None:
    """The shared derivation and the connector's own function must agree.

    ``hooks.compute_pool_id`` delegates to ``compute_v4_pool_id``; this pins
    the delegation (including the currency-sorting invariant — PoolKey sorts
    in __post_init__, the shared function sorts internally).
    """
    from almanak.connectors.uniswap_v4.hooks import build_get_slot0_calldata, compute_pool_id
    from almanak.connectors.uniswap_v4.sdk import PoolKey

    # Deliberately unsorted input: WETH > USDC numerically.
    pool_key = PoolKey(currency0=WETH_BASE, currency1=USDC_BASE, fee=500, tick_spacing=10)
    assert compute_pool_id(pool_key) == compute_v4_pool_id(WETH_BASE, USDC_BASE, 500, 10)
    assert build_get_slot0_calldata(pool_key) == encode_get_slot0(compute_pool_id(pool_key))


def test_pool_id_known_mainnet_vector_and_independent_keccak() -> None:
    """Pin the encoding against the published mainnet ETH/USDC 0.05% pool id.

    Also recompute the keccak independently word-by-word so the word order /
    widths (address, address, uint24, int24, address) can never drift.
    """
    pool_id = compute_v4_pool_id(V4_ZERO_ADDRESS, USDC_ETH, 500, 10)
    assert pool_id == "0x21c67e77068de97969ba93d4aab21826d33ca12bb9f565d8496e8fda8a82ca27"

    words = (
        "00" * 32  # currency0 = native zero address
        + USDC_ETH.lower().removeprefix("0x").zfill(64)
        + hex(500)[2:].zfill(64)
        + hex(10)[2:].zfill(64)
        + "00" * 32  # hooks = zero address
    )
    assert pool_id == "0x" + keccak(bytes.fromhex(words)).hex()


def test_get_slot0_selector_is_bytes32_form() -> None:
    """The deployed StateView takes bytes32 PoolId (0xc815641c), not the tuple."""
    assert V4_GET_SLOT0_SELECTOR == "0xc815641c"


def test_encode_get_liquidity_calldata_shape() -> None:
    """Selector + the 32-byte PoolId word, exactly as getLiquidity(bytes32) expects."""
    from almanak.connectors._strategy_base.v4_pool_abi import encode_get_liquidity

    pool_id = "0x" + "ab" * 32
    calldata = encode_get_liquidity(pool_id)
    assert calldata == V4_GET_LIQUIDITY_SELECTOR + "ab" * 32
    # 0x-less input encodes identically (normalized, not double-prefixed).
    assert encode_get_liquidity("ab" * 32) == calldata


def test_abi_word_encoders_fail_closed_on_invalid_input() -> None:
    """The padding/validation helpers must raise, never emit malformed calldata."""
    from almanak.connectors._strategy_base.v4_pool_abi import (
        _pad_address_word,
        _pad_int24_word,
        _pad_uint24_word,
        _pool_id_word,
    )

    # uint24: full range accepted, out-of-range rejected on both sides.
    assert _pad_uint24_word((1 << 24) - 1).endswith("ffffff")
    with pytest.raises(ValueError, match="uint24 out of range"):
        _pad_uint24_word(1 << 24)
    with pytest.raises(ValueError, match="uint24 out of range"):
        _pad_uint24_word(-1)

    # int24: negative values encode two's complement; out-of-range rejected.
    assert _pad_int24_word(-1) == "f" * 64
    with pytest.raises(ValueError, match="int24 out of range"):
        _pad_int24_word(1 << 23)
    with pytest.raises(ValueError, match="int24 out of range"):
        _pad_int24_word(-(1 << 23) - 1)

    # PoolId: exactly 32 bytes of hex, or refuse.
    with pytest.raises(ValueError, match="PoolId must be 32 bytes"):
        _pool_id_word("0x" + "ab" * 31)
    with pytest.raises(ValueError):
        _pool_id_word("0x" + "zz" * 32)  # non-hex

    # Address word: exactly 20 bytes of hex, or refuse.
    with pytest.raises(ValueError, match="address must be 20 bytes"):
        _pad_address_word("0x" + "ab" * 19)


# ---------------------------------------------------------------------------
# resolve_pool_address (synthetic PoolId, existence-verified)
# ---------------------------------------------------------------------------


def _default_pools() -> dict[str, tuple[int, int, int, int, int]]:
    # ETH/USDC 0.05% on Base, price raw 4 (sqrt 2 in Q64.96), tick 100.
    # Keyed by the NATIVE PoolKey: WETH_BASE is the chain's wrapped native, so
    # the reader normalizes it to currency zero (flagship V4 pools are
    # native-currency; mirrors the connector SDK's wrapped->native quoting).
    return {
        _pool_id(V4_ZERO_ADDRESS, USDC_BASE, 500, 10): (2 << 96, 100, 0, 500, 7 * 10**18),
    }


def test_resolve_returns_synthetic_pool_id_and_verifies_existence() -> None:
    state_view = ScriptedStateView(_default_pools(), {WETH_BASE: 18, USDC_BASE: 6})
    reader = _v4_reader(state_view)

    pool_id = reader.resolve_pool_address(WETH_BASE, USDC_BASE, "base", 500)

    assert pool_id == _pool_id(V4_ZERO_ADDRESS, USDC_BASE, 500, 10)
    assert len(pool_id.removeprefix("0x")) == 64  # opaque 32-byte id, not an address
    # Existence was verified via getSlot0 against the StateView.
    assert any(
        to.lower() == STATE_VIEW_BASE.lower() and calldata.startswith(V4_GET_SLOT0_SELECTOR)
        for _, to, calldata in state_view.calls
    )


def test_resolve_uninitialized_pool_returns_none() -> None:
    state_view = ScriptedStateView(_default_pools(), {WETH_BASE: 18, USDC_BASE: 6})
    reader = _v4_reader(state_view)
    # 0.3% tier not scripted -> sqrtPriceX96 reads back 0 -> not a pool.
    assert reader.resolve_pool_address(WETH_BASE, USDC_BASE, "base", 3000) is None


def test_resolve_nonstandard_fee_tier_returns_none() -> None:
    reader = _v4_reader(ScriptedStateView(_default_pools(), {}))
    # No canonical tick spacing derivable -> honest miss, no guessed PoolKey.
    assert reader.resolve_pool_address(WETH_BASE, USDC_BASE, "base", 1234) is None


def test_resolve_chain_without_stateview_returns_none() -> None:
    reader = _v4_reader(ScriptedStateView(_default_pools(), {}))
    assert reader.resolve_pool_address(WETH_BASE, USDC_BASE, "solana", 500) is None


def test_resolve_wrapped_native_normalizes_to_native_pool() -> None:
    """MarketSnapshot canonicalizes native symbols to their wrapped form
    (ETH -> WETH) before the reader sees them, but the flagship V4 pools are
    NATIVE-currency pools. WETH input must therefore hash the native PoolKey
    (mirroring the connector SDK's wrapped->native quoting) — otherwise every
    symbol-API caller reports those pools missing."""
    pools = {_pool_id(V4_ZERO_ADDRESS, USDC_BASE, 500, 10): (2 << 96, 100, 0, 500, 10**18)}
    state_view = ScriptedStateView(pools, {USDC_BASE: 6})
    reader = _v4_reader(state_view)

    pool_id = reader.resolve_pool_address(WETH_BASE, USDC_BASE, "base", 500)

    assert pool_id == _pool_id(V4_ZERO_ADDRESS, USDC_BASE, 500, 10)
    # And the memoized key prices native decimals without an ERC-20 call on WETH.
    envelope = reader.read_pool_price(pool_id, "base")
    assert envelope.value.token0_decimals == 18


def test_resolve_native_placeholder_normalizes_to_zero_currency() -> None:
    pools = {_pool_id(V4_ZERO_ADDRESS, USDC_BASE, 500, 10): (2 << 96, 100, 0, 500, 10**18)}
    state_view = ScriptedStateView(pools, {USDC_BASE: 6})
    reader = _v4_reader(state_view)

    pool_id = reader.resolve_pool_address(NATIVE_PLACEHOLDER, USDC_BASE, "base", 500)

    assert pool_id == _pool_id(V4_ZERO_ADDRESS, USDC_BASE, 500, 10)


# ---------------------------------------------------------------------------
# read_pool_price
# ---------------------------------------------------------------------------


def test_read_pool_price_roundtrip() -> None:
    state_view = ScriptedStateView(_default_pools(), {WETH_BASE: 18, USDC_BASE: 6})
    reader = _v4_reader(state_view)

    pool_id = reader.resolve_pool_address(WETH_BASE, USDC_BASE, "base", 500)
    envelope = reader.read_pool_price(pool_id, "base")

    pp = envelope.value
    # sqrt=2 -> raw price 4; currency0=native (zero address) 18 dec,
    # currency1=USDC 6 dec: shared v3 decoder gives 4 * 10^(18-6).
    assert pp.price == Decimal(4) * Decimal(10) ** 12
    assert pp.tick == 100
    assert pp.liquidity == 7 * 10**18
    assert pp.fee_tier == 500  # lpFee from getSlot0, already v3 1e-6 units
    assert pp.token0_decimals == 18
    assert pp.token1_decimals == 6
    assert pp.pool_address == pool_id
    assert envelope.classification == DataClassification.EXECUTION_GRADE


def test_read_pool_price_native_currency_decimals_without_erc20_call() -> None:
    pools = {_pool_id(V4_ZERO_ADDRESS, USDC_BASE, 500, 10): (2 << 96, -50, 0, 500, 10**18)}
    state_view = ScriptedStateView(pools, {USDC_BASE: 6})
    reader = _v4_reader(state_view)

    pool_id = reader.resolve_pool_address(V4_ZERO_ADDRESS, USDC_BASE, "base", 500)
    envelope = reader.read_pool_price(pool_id, "base")

    assert envelope.value.token0_decimals == 18  # native, chain constant
    assert envelope.value.tick == -50  # negative int24 round-trips
    # decimals() must never be called on the zero address.
    assert not any(to.lower() == V4_ZERO_ADDRESS for _, to, _ in state_view.calls)


def test_read_pool_price_unknown_pool_id_fails_closed() -> None:
    reader = _v4_reader(ScriptedStateView(_default_pools(), {WETH_BASE: 18, USDC_BASE: 6}))
    unknown_id = "0x" + "ab" * 32
    with pytest.raises(DataUnavailableError, match="one-way hashes"):
        reader.read_pool_price(unknown_id, "base")


def test_read_pool_price_uninitialized_after_resolve_fails_closed() -> None:
    pools = _default_pools()
    state_view = ScriptedStateView(pools, {WETH_BASE: 18, USDC_BASE: 6})
    reader = _v4_reader(state_view)
    pool_id = reader.resolve_pool_address(WETH_BASE, USDC_BASE, "base", 500)
    pools.clear()  # pool state vanishes (e.g. re-org / wrong fork)
    with pytest.raises(DataUnavailableError, match="uninitialized"):
        reader.read_pool_price(pool_id, "base")


def test_read_pool_price_caches_within_ttl() -> None:
    state_view = ScriptedStateView(_default_pools(), {WETH_BASE: 18, USDC_BASE: 6})
    reader = _v4_reader(state_view)
    pool_id = reader.resolve_pool_address(WETH_BASE, USDC_BASE, "base", 500)

    first = reader.read_pool_price(pool_id, "base")
    calls_after_first = len(state_view.calls)
    second = reader.read_pool_price(pool_id, "base")

    assert len(state_view.calls) == calls_after_first
    assert second.meta.cache_hit is True
    assert second.value.price == first.value.price


# ---------------------------------------------------------------------------
# _get_pool_metadata (TWAP decimals duck-type) + best-pool sweep
# ---------------------------------------------------------------------------


def test_get_pool_metadata_is_v4_shaped() -> None:
    state_view = ScriptedStateView(_default_pools(), {WETH_BASE: 18, USDC_BASE: 6})
    reader = _v4_reader(state_view)
    pool_id = reader.resolve_pool_address(WETH_BASE, USDC_BASE, "base", 500)

    assert reader._get_pool_metadata(pool_id, "base") == (18, 6, 500)


def test_resolve_best_pool_address_ranks_by_liquidity() -> None:
    pools = {
        _pool_id(V4_ZERO_ADDRESS, USDC_BASE, 500, 10): (2 << 96, 100, 0, 500, 5 * 10**18),
        _pool_id(V4_ZERO_ADDRESS, USDC_BASE, 3000, 60): (2 << 96, 100, 0, 3000, 9 * 10**18),
    }
    state_view = ScriptedStateView(pools, {WETH_BASE: 18, USDC_BASE: 6})
    reader = _v4_reader(state_view)

    best = reader.resolve_best_pool_address(WETH_BASE, USDC_BASE, "base")

    assert best == _pool_id(V4_ZERO_ADDRESS, USDC_BASE, 3000, 60)  # deepest wins
