"""Unit tests for CurvePoolReader (reader_kind ``curve_pool``).

Scripted-RPC tests: every on-chain byte the reader consumes is encoded here
exactly as a Curve pool returns it (32-byte ABI words), covering both index
ABI families (modern ``uint256`` vs legacy Vyper ``int128``), both ``get_dy``
families (StableSwap ``int128`` vs CryptoSwap ``uint256``), the native-asset
placeholder coin, fee-scale conversion, fail-closed paths, curated-pair
resolution, and the LWAP multi-count guard for tier-insensitive resolution.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.curve_pool_abi import (
    CURVE_BALANCES_INT128_SELECTOR,
    CURVE_BALANCES_UINT256_SELECTOR,
    CURVE_COINS_INT128_SELECTOR,
    CURVE_COINS_UINT256_SELECTOR,
    CURVE_FEE_SELECTOR,
    CURVE_GET_DY_INT128_SELECTOR,
    CURVE_GET_DY_UINT256_SELECTOR,
)
from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.models import DataClassification
from almanak.framework.data.pools.aggregation import PriceAggregator
from almanak.framework.data.pools.reader import CurvePoolReader, PoolReaderRegistry

# ---------------------------------------------------------------------------
# ABI encoding helpers
# ---------------------------------------------------------------------------


def _uint256_bytes(value: int) -> bytes:
    return value.to_bytes(32, byteorder="big")


def _address_bytes(address: str) -> bytes:
    return b"\x00" * 12 + bytes.fromhex(address.removeprefix("0x"))


# Mainnet 3pool fixtures (curated in CURVE_POOLS["ethereum"]["3pool"]).
POOL_3POOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
DAI = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
NATIVE_ETH = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
STETH = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"


class ScriptedCurvePool:
    """A scripted rpc_call impersonating one Curve pool + its coin ERC-20s."""

    def __init__(
        self,
        pool_address: str = POOL_3POOL,
        coin0: str = DAI,
        coin1: str = USDC,
        coin0_decimals: int = 18,
        coin1_decimals: int = 6,
        get_dy_out: int = 999_500,  # 0.9995 USDC for 1 DAI
        fee_raw: int = 1_000_000,  # 0.01% in Curve's 1e10 scale
        balance0: int = 150_000_000 * 10**18,
        index_abi_uint256: bool = True,
        get_dy_uint256: bool = False,
    ) -> None:
        self.pool = pool_address.lower()
        self.coin0 = coin0
        self.coin1 = coin1
        self.coin0_decimals = coin0_decimals
        self.coin1_decimals = coin1_decimals
        self.get_dy_out = get_dy_out
        self.fee_raw = fee_raw
        self.balance0 = balance0
        self.index_abi_uint256 = index_abi_uint256
        self.get_dy_uint256 = get_dy_uint256
        self.calls: list[tuple[str, str, str]] = []

    def __call__(self, chain: str, to: str, calldata: str) -> bytes:
        self.calls.append((chain, to, calldata))
        selector = calldata[:10]
        if to.lower() == self.pool:
            return self._pool_call(selector, calldata)
        # Coin ERC-20s: decimals() only.
        if selector == "0x313ce567":
            if to.lower() == self.coin0.lower():
                return _uint256_bytes(self.coin0_decimals)
            if to.lower() == self.coin1.lower():
                return _uint256_bytes(self.coin1_decimals)
        raise ValueError(f"revert: unknown call {to} {selector}")

    def _pool_call(self, selector: str, calldata: str) -> bytes:
        coins_sel = CURVE_COINS_UINT256_SELECTOR if self.index_abi_uint256 else CURVE_COINS_INT128_SELECTOR
        balances_sel = CURVE_BALANCES_UINT256_SELECTOR if self.index_abi_uint256 else CURVE_BALANCES_INT128_SELECTOR
        get_dy_sel = CURVE_GET_DY_UINT256_SELECTOR if self.get_dy_uint256 else CURVE_GET_DY_INT128_SELECTOR
        if selector == coins_sel:
            index = int(calldata[10:], 16)
            coin = {0: self.coin0, 1: self.coin1}.get(index)
            if coin is None:
                raise ValueError("revert: coin index out of range")
            return _address_bytes(coin)
        if selector == balances_sel:
            index = int(calldata[10:], 16)
            if index == 0:
                return _uint256_bytes(self.balance0)
            raise ValueError("revert: balance index out of range")
        if selector == get_dy_sel:
            return _uint256_bytes(self.get_dy_out)
        if selector == CURVE_FEE_SELECTOR:
            return _uint256_bytes(self.fee_raw)
        raise ValueError(f"revert: pool does not answer {selector}")


def _curve_reader(rpc_call) -> CurvePoolReader:
    registry = PoolReaderRegistry(rpc_call=rpc_call)
    reader = registry.get_reader("ethereum", "curve")
    assert type(reader) is CurvePoolReader
    return reader


# ---------------------------------------------------------------------------
# Selector derivation — pinned against the adapter's on-chain-verified hex
# ---------------------------------------------------------------------------


def test_derived_selectors_match_adapter_literals() -> None:
    from almanak.connectors.curve import adapter

    assert CURVE_GET_DY_INT128_SELECTOR == adapter.GET_DY_SELECTOR
    assert CURVE_GET_DY_UINT256_SELECTOR == adapter.GET_DY_UINT256_SELECTOR
    assert CURVE_COINS_UINT256_SELECTOR == adapter.COINS_UINT256_SELECTOR
    assert CURVE_COINS_INT128_SELECTOR == adapter.COINS_INT128_SELECTOR
    assert CURVE_BALANCES_UINT256_SELECTOR == adapter.BALANCES_UINT256_SELECTOR
    assert CURVE_BALANCES_INT128_SELECTOR == adapter.BALANCES_INT128_SELECTOR
    assert CURVE_FEE_SELECTOR == "0xddca3f43"  # fee() — signature-identical to v3


# ---------------------------------------------------------------------------
# read_pool_price
# ---------------------------------------------------------------------------


def test_read_pool_price_stableswap() -> None:
    pool = ScriptedCurvePool()
    reader = _curve_reader(pool)

    envelope = reader.read_pool_price(POOL_3POOL, "ethereum")

    pp = envelope.value
    # 999500 raw USDC (6 dec) for 1 whole DAI -> 0.9995 (execution quote incl. fee)
    assert pp.price == Decimal("999500") / Decimal(10**6)
    assert pp.tick is None  # Curve has no ticks — never fabricated as 0
    assert pp.liquidity == pool.balance0
    assert pp.fee_tier == 100  # 1_000_000 / 1e10 = 0.01% -> 100 in v3 1e-6 units
    assert pp.token0_decimals == 18
    assert pp.token1_decimals == 6
    assert pp.pool_address == POOL_3POOL
    assert envelope.classification == DataClassification.EXECUTION_GRADE
    assert envelope.meta.cache_hit is False


def test_read_pool_price_legacy_int128_pool_uses_matching_balances_abi() -> None:
    pool = ScriptedCurvePool(index_abi_uint256=False)
    reader = _curve_reader(pool)

    envelope = reader.read_pool_price(POOL_3POOL, "ethereum")

    assert envelope.value.price == Decimal("999500") / Decimal(10**6)
    selectors = [calldata[:10] for _, to, calldata in pool.calls if to.lower() == pool.pool]
    # The uint256 probe fails first, then everything runs on the int128 family.
    assert CURVE_COINS_INT128_SELECTOR in selectors
    assert CURVE_BALANCES_INT128_SELECTOR in selectors
    assert CURVE_BALANCES_UINT256_SELECTOR not in selectors


def test_read_pool_price_crypto_pool_uses_uint256_get_dy() -> None:
    # WETH/WBTC-style crypto pool: 18 -> 8 decimals, get_dy answers uint256 form.
    pool = ScriptedCurvePool(
        coin0_decimals=18,
        coin1_decimals=8,
        get_dy_out=5_400_000,  # 0.054 WBTC for 1 WETH
        get_dy_uint256=True,
        fee_raw=40_000_000,  # 0.4%
    )
    reader = _curve_reader(pool)

    envelope = reader.read_pool_price(POOL_3POOL, "ethereum")

    assert envelope.value.price == Decimal("5400000") / Decimal(10**8)
    assert envelope.value.fee_tier == 4000


def test_read_pool_price_native_placeholder_coin_decimals() -> None:
    # steth-style pool: coins(0) is the native-ETH placeholder (not an ERC-20).
    pool = ScriptedCurvePool(
        coin0=NATIVE_ETH,
        coin1=STETH,
        coin0_decimals=18,
        coin1_decimals=18,
        get_dy_out=1_001 * 10**15,  # 1.001 stETH per ETH
        index_abi_uint256=False,
    )
    reader = _curve_reader(pool)

    envelope = reader.read_pool_price(POOL_3POOL, "ethereum")

    assert envelope.value.price == Decimal("1.001")
    # decimals() must never be called on the placeholder address.
    assert not any(to.lower() == NATIVE_ETH.lower() for _, to, _ in pool.calls)


def test_read_pool_price_fails_closed_on_non_curve_pool() -> None:
    def rpc_call(chain: str, to: str, calldata: str) -> bytes:
        raise ValueError("revert")  # a v3 pool answers none of the Curve ABI

    reader = _curve_reader(rpc_call)
    with pytest.raises(DataUnavailableError):
        reader.read_pool_price("0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640", "ethereum")


def test_read_pool_price_fails_closed_on_zero_quote() -> None:
    pool = ScriptedCurvePool(get_dy_out=0)
    reader = _curve_reader(pool)
    with pytest.raises(DataUnavailableError, match="no positive quote"):
        reader.read_pool_price(POOL_3POOL, "ethereum")


def test_read_pool_price_fails_closed_on_zero_coin_address() -> None:
    pool = ScriptedCurvePool(coin1="0x" + "0" * 40)
    reader = _curve_reader(pool)
    with pytest.raises(DataUnavailableError, match="not a Curve pool"):
        reader.read_pool_price(POOL_3POOL, "ethereum")


def test_read_pool_price_caches_within_ttl() -> None:
    pool = ScriptedCurvePool()
    reader = _curve_reader(pool)

    first = reader.read_pool_price(POOL_3POOL, "ethereum")
    calls_after_first = len(pool.calls)
    second = reader.read_pool_price(POOL_3POOL, "ethereum")

    assert len(pool.calls) == calls_after_first  # no extra RPC
    assert second.meta.cache_hit is True
    assert second.value.price == first.value.price


# ---------------------------------------------------------------------------
# _get_pool_metadata (TWAP decimals path duck-type)
# ---------------------------------------------------------------------------


def test_get_pool_metadata_is_curve_shaped() -> None:
    pool = ScriptedCurvePool()
    reader = _curve_reader(pool)

    dec0, dec1, fee_tier = reader._get_pool_metadata(POOL_3POOL, "ethereum")

    assert (dec0, dec1, fee_tier) == (18, 6, 100)


# ---------------------------------------------------------------------------
# Pool resolution (curated pairs, tier-insensitive, total)
# ---------------------------------------------------------------------------


def test_resolve_pool_address_ignores_fee_tier_and_order() -> None:
    reader = _curve_reader(ScriptedCurvePool())
    for fee_tier in (100, 500, 3000, 10000, 0):
        assert reader.resolve_pool_address(DAI, USDC, "ethereum", fee_tier) == POOL_3POOL
    assert reader.resolve_pool_address(USDC, DAI, "ethereum") == POOL_3POOL


def test_resolve_pool_address_unknown_pair_or_chain_is_none() -> None:
    reader = _curve_reader(ScriptedCurvePool())
    # DAI/USDT sit at coin indices (0, 2) of 3pool — NOT a leading pair, so it
    # must miss honestly rather than resolve a pool whose read would price
    # the wrong pair.
    assert reader.resolve_pool_address(DAI, USDT, "ethereum") is None
    assert reader.resolve_pool_address(DAI, USDC, "solana") is None


def test_resolve_best_pool_address_single_total_sweep() -> None:
    pool = ScriptedCurvePool()
    reader = _curve_reader(pool)

    best = reader.resolve_best_pool_address(DAI, USDC, "ethereum")

    assert best == POOL_3POOL


# ---------------------------------------------------------------------------
# LWAP multi-count guard
# ---------------------------------------------------------------------------


def test_lwap_does_not_multi_count_tier_insensitive_pool() -> None:
    """Curve resolves the same pool for every swept tier — LWAP counts it once."""
    pool = ScriptedCurvePool()
    registry = PoolReaderRegistry(rpc_call=pool)
    aggregator = PriceAggregator(pool_registry=registry, rpc_call=pool)

    envelope = aggregator.lwap(DAI, USDC, "ethereum", protocols=["curve"])

    aggregated = envelope.value
    assert aggregated.pool_count == 1
    assert len(aggregated.sources) == 1
    assert aggregated.sources[0].pool_address == POOL_3POOL
    assert aggregated.price == Decimal("999500") / Decimal(10**6)
