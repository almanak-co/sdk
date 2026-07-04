"""PoolReserves dataclass behaviour for the Solidly (Aerodrome/Velodrome) family.

Pins the ``solidly_v2`` additions: dex-type validation, the ``stable`` flag's
serialization round-trip, and the price properties — a volatile pool prices as
a constant-product ratio while a stable pool prices as the marginal price on
the x^3*y + x*y^3 curve (the reserve ratio is NOT the spot price there).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.data.defi.pools import PoolReserves
from almanak.framework.data.tokens.models import ChainToken, Token

_POOL = "0x3333333333333333333333333333333333333333"


def _chain_token(symbol: str, decimals: int, address: str) -> ChainToken:
    return ChainToken(
        token=Token(symbol=symbol, name=symbol, decimals=decimals, addresses={"base": address}),
        chain="base",
        address=address,
        decimals=decimals,
    )


def _solidly_pool(*, stable: bool, reserve0: str, reserve1: str) -> PoolReserves:
    return PoolReserves(
        pool_address=_POOL,
        dex="solidly_v2",
        token0=_chain_token("USDC", 6, "0x1111111111111111111111111111111111111111"),
        token1=_chain_token("DAI", 18, "0x2222222222222222222222222222222222222222"),
        reserve0=Decimal(reserve0),
        reserve1=Decimal(reserve1),
        fee_tier=500,
        stable=stable,
        tvl_usd=Decimal("0"),
        last_updated=datetime.now(UTC),
    )


def test_solidly_v2_is_a_valid_dex_type():
    pool = _solidly_pool(stable=False, reserve0="1000", reserve1="1000")
    assert pool.is_solidly
    assert not pool.is_v3


def test_volatile_pool_prices_as_reserve_ratio():
    pool = _solidly_pool(stable=False, reserve0="2000", reserve1="1000")
    assert pool.price_token0_in_token1 == Decimal("0.5")


def test_stable_pool_prices_on_curve_not_reserve_ratio():
    # Balanced stable pool: marginal price is exactly 1 regardless of curve.
    balanced = _solidly_pool(stable=True, reserve0="1000", reserve1="1000")
    assert balanced.price_token0_in_token1 == Decimal("1")

    # Imbalanced stable pool: the x^3*y + x*y^3 marginal price stays far
    # closer to peg than the raw reserve ratio.
    imbalanced = _solidly_pool(stable=True, reserve0="2000", reserve1="1000")
    price = imbalanced.price_token0_in_token1
    assert price is not None
    ratio = Decimal("0.5")  # what a constant-product read would claim
    assert ratio < price < Decimal("1")
    # dy/dx = (3x^2*y + y^3)/(x^3 + 3x*y^2) with x=2000, y=1000.
    expected = (3 * Decimal(2000) ** 2 * 1000 + Decimal(1000) ** 3) / (
        Decimal(2000) ** 3 + 3 * 2000 * Decimal(1000) ** 2
    )
    assert price == expected


def test_stable_flag_survives_serialization_round_trip():
    pool = _solidly_pool(stable=True, reserve0="1000", reserve1="1000")
    restored = PoolReserves.from_dict(pool.to_dict())
    assert restored.dex == "solidly_v2"
    assert restored.stable is True

    volatile = _solidly_pool(stable=False, reserve0="1000", reserve1="1000")
    assert PoolReserves.from_dict(volatile.to_dict()).stable is False


def test_v3_price_decimal_adjustment_sign():
    # Regression: the property multiplied by 10^(token1 - token0) decimals,
    # inverting the adjustment (a WETH/USDC pool priced ~1.7e-21 instead of
    # ~1726). Values observed live from the Base Slipstream WETH/USDC pool
    # 0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59; the correct formula matches
    # decode_sqrt_price_x96 in data/pools/reader.py.
    pool = PoolReserves(
        pool_address=_POOL,
        dex="uniswap_v3",
        token0=_chain_token("WETH", 18, "0x1111111111111111111111111111111111111111"),
        token1=_chain_token("USDC", 6, "0x2222222222222222222222222222222222222222"),
        reserve0=Decimal("3140"),
        reserve1=Decimal("3891113"),
        fee_tier=894,
        sqrt_price_x96=3291845559452554362106893,
        tick=-201783,
        liquidity=1713932465492818950,
        tvl_usd=Decimal("0"),
        last_updated=datetime.now(UTC),
    )
    price = pool.price_token0_in_token1
    assert price is not None
    assert Decimal("1700") < price < Decimal("1760")  # ~1726 USDC per WETH
    inverse = pool.price_token1_in_token0
    assert inverse is not None
    assert Decimal("0.0005") < inverse < Decimal("0.0006")


def test_unmeasured_v2_fee_is_none_not_guessed():
    pool = PoolReserves(
        pool_address=_POOL,
        dex="uniswap_v2",
        token0=_chain_token("USDC", 6, "0x1111111111111111111111111111111111111111"),
        token1=_chain_token("DAI", 18, "0x2222222222222222222222222222222222222222"),
        reserve0=Decimal("1000"),
        reserve1=Decimal("1000"),
        fee_tier=None,  # V2 fees are not on-chain readable — unmeasured
        tvl_usd=Decimal("0"),
        last_updated=datetime.now(UTC),
    )
    assert pool.fee_tier is None
    assert pool.fee_percent is None
    restored = PoolReserves.from_dict(pool.to_dict())
    assert restored.fee_tier is None


def test_solidly_stable_contract_enforced():
    # solidly_v2 without a stable flag would silently price on the wrong curve.
    with pytest.raises(ValueError, match="stable flag is required"):
        _solidly_pool(stable=None, reserve0="1", reserve1="1")  # type: ignore[arg-type]
    # A deserialized string like "false" is truthy — must be a real bool.
    with pytest.raises(ValueError, match="must be a bool"):
        _solidly_pool(stable="false", reserve0="1", reserve1="1")  # type: ignore[arg-type]
    # Non-Solidly pools must not carry the flag.
    with pytest.raises(ValueError, match="must be None"):
        PoolReserves(
            pool_address=_POOL,
            dex="uniswap_v2",
            token0=_chain_token("USDC", 6, "0x1111111111111111111111111111111111111111"),
            token1=_chain_token("DAI", 18, "0x2222222222222222222222222222222222222222"),
            reserve0=Decimal("1"),
            reserve1=Decimal("1"),
            fee_tier=None,
            stable=False,
            tvl_usd=Decimal("0"),
            last_updated=datetime.now(UTC),
        )


def test_invalid_dex_type_still_rejected():
    with pytest.raises(ValueError, match="Invalid dex type"):
        _solidly_pool(stable=False, reserve0="1", reserve1="1").__class__(
            pool_address=_POOL,
            dex="not_a_dex",  # type: ignore[arg-type]
            token0=_chain_token("USDC", 6, "0x1111111111111111111111111111111111111111"),
            token1=_chain_token("DAI", 18, "0x2222222222222222222222222222222222222222"),
            reserve0=Decimal("1"),
            reserve1=Decimal("1"),
            fee_tier=500,
            tvl_usd=Decimal("0"),
            last_updated=datetime.now(UTC),
        )
