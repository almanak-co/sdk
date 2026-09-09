"""Protocol QA: Uniswap V4 fee and tick-spacing contract.

V3 has four fee tiers, each welded to one tick spacing, so "WETH/USDC/3000"
names exactly one pool. V4 dropped both restrictions: ``fee`` is any uint24 up
to ``MAX_LP_FEE`` (or the dynamic-fee sentinel, where the hook prices each
swap), and ``tickSpacing`` is an independent PoolKey field the pool creator
picks. Under V4 rules "WETH/USDC/4000" therefore names a *family* of pools, one
per tick spacing, each with its own PoolId and its own liquidity.

The declared invariant set this module owns:

F1  A fee with no canonical tick spacing is an under-specified pool: the compile
    must refuse rather than substitute a spacing and address a different pool.
F2  An explicitly supplied tickSpacing is honoured exactly, and changes the
    PoolId -- the same pair and fee at two spacings are two pools.
F3  The canonical fee->spacing pairings are one shared map, and every V4 surface
    that consults it refuses an unpaired fee rather than defaulting.
F4  The dynamic-fee sentinel is not a rate; no quote may price against it.
F5  A static fee above ``MAX_LP_FEE`` is not a V4 fee and is refused, never
    encoded into a pool key that is then traded.

Sources: v4-core ``LPFeeLibrary.sol`` (``DYNAMIC_FEE_FLAG``, ``MAX_LP_FEE``),
``PoolKey.sol``, ``Pool.initialize`` tick-spacing bounds.
"""

from __future__ import annotations

import inspect
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.v4_pool_abi import (
    V4_DEFAULT_TICK_SPACING,
    V4_DYNAMIC_FEE_FLAG,
    V4_MAX_LP_FEE,
    V4_MAX_TICK_SPACING,
    V4_MIN_TICK_SPACING,
    V4PoolKeyError,
    resolve_v4_tick_spacing,
    validate_v4_static_fee,
)
from almanak.connectors.uniswap_v4.adapter import (
    UniswapV4Adapter,
    UniswapV4Config,
    UniswapV4UnsupportedPoolError,
)
from almanak.connectors.uniswap_v4.hooks import compute_pool_id, discover_pool
from almanak.connectors.uniswap_v4.sdk import UniswapV4SDK

_WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_WALLET = "0x1234567890abcdef1234567890abcdef12345678"

# Fees that are legal V4 static fees but have no canonical spacing pairing.
_UNPAIRED_FEES = (0, 250, 4000, 7500, V4_MAX_LP_FEE)


def _resolver() -> MagicMock:
    resolver = MagicMock()
    tokens = {
        "WETH": MagicMock(address=_WETH, decimals=18, is_native=False),
        "USDC": MagicMock(address=_USDC, decimals=6, is_native=False),
    }
    resolver.resolve_for_swap = lambda symbol, chain: tokens[symbol.upper()]
    resolver.resolve = lambda symbol, chain: tokens[symbol.upper()]
    return resolver


@pytest.fixture()
def adapter() -> UniswapV4Adapter:
    return UniswapV4Adapter(
        config=UniswapV4Config(chain="arbitrum", wallet_address=_WALLET),
        token_resolver=_resolver(),
    )


def _lp_open_intent(fee: int, **protocol_params):
    from almanak.framework.intents.vocabulary import LPOpenIntent

    return LPOpenIntent(
        pool=f"WETH/USDC/{fee}",
        amount0=Decimal("0.1"),
        amount1=Decimal("200"),
        range_lower=Decimal("1500"),
        range_upper=Decimal("2500"),
        protocol="uniswap_v4",
        protocol_params={"allow_estimated_price": True, **protocol_params},
    )


_ORACLE = {"WETH": Decimal("2000"), "USDC": Decimal("1")}


# F1 -- an unpaired fee is under-specified, not defaulted


@pytest.mark.parametrize("fee", _UNPAIRED_FEES)
def test_f1_lp_open_refuses_a_fee_with_no_canonical_tick_spacing(adapter: UniswapV4Adapter, fee: int) -> None:
    """A "T0/T1/FEE" pool string cannot express tickSpacing.

    Substituting one would mint into a pool the strategy never named -- the
    failure is silent on-chain, since the guessed PoolId may be a real pool.
    """
    with pytest.raises(UniswapV4UnsupportedPoolError) as raised:
        adapter.compile_lp_open_intent(_lp_open_intent(fee), _ORACLE)

    assert "tick" in str(raised.value).lower()


@pytest.mark.parametrize("fee", sorted(V4_DEFAULT_TICK_SPACING))
def test_f1_canonical_fees_still_compile_without_an_explicit_spacing(adapter: UniswapV4Adapter, fee: int) -> None:
    """Control: the four V3-inherited pairings are unambiguous and must not regress."""
    bundle = adapter.compile_lp_open_intent(_lp_open_intent(fee), _ORACLE)

    assert bundle.transactions, bundle.metadata
    assert bundle.metadata["fee"] == fee


# F2 -- an explicit tick spacing is honoured and is part of pool identity


def test_f2_explicit_tick_spacing_selects_a_different_pool(adapter: UniswapV4Adapter) -> None:
    canonical = adapter.compile_lp_open_intent(_lp_open_intent(3000), _ORACLE)
    narrow = adapter.compile_lp_open_intent(_lp_open_intent(3000, tick_spacing=10), _ORACLE)

    assert canonical.metadata["pool_id"] != narrow.metadata["pool_id"], (
        "same pair and fee at two tick spacings must be two distinct V4 pools"
    )


@pytest.mark.parametrize("fee", _UNPAIRED_FEES)
def test_f2_an_unpaired_fee_compiles_once_its_spacing_is_named(adapter: UniswapV4Adapter, fee: int) -> None:
    """The refusal in F1 is about the missing field, not about the fee value."""
    bundle = adapter.compile_lp_open_intent(_lp_open_intent(fee, tick_spacing=30), _ORACLE)

    assert bundle.transactions, bundle.metadata
    assert bundle.metadata["fee"] == fee


def test_f2_tick_spacing_outside_the_v4_range_is_refused(adapter: UniswapV4Adapter) -> None:
    for spacing in (0, -1, V4_MAX_TICK_SPACING + 1):
        with pytest.raises(UniswapV4UnsupportedPoolError):
            adapter.compile_lp_open_intent(_lp_open_intent(3000, tick_spacing=spacing), _ORACLE)

    assert resolve_v4_tick_spacing(3000, V4_MIN_TICK_SPACING) == V4_MIN_TICK_SPACING
    assert resolve_v4_tick_spacing(3000, V4_MAX_TICK_SPACING) == V4_MAX_TICK_SPACING


# F3 -- one shared map, and no surface defaults past it


def test_f3_the_connector_reexports_the_shared_canonical_map() -> None:
    from almanak.connectors.uniswap_v4 import sdk as v4_sdk

    assert v4_sdk.TICK_SPACING is V4_DEFAULT_TICK_SPACING


@pytest.mark.parametrize("fee", _UNPAIRED_FEES)
def test_f3_no_v4_surface_substitutes_a_spacing_for_an_unpaired_fee(fee: int) -> None:
    """Every surface that builds a PoolKey from a bare fee must refuse the same way.

    A surface that quietly defaults produces a PoolId the other surfaces do not
    agree with -- discovery, compile and permissions would then describe
    different pools while all reporting success.
    """
    sdk = UniswapV4SDK(chain="arbitrum")

    with pytest.raises(V4PoolKeyError):
        sdk.compute_pool_key(_WETH, _USDC, fee)
    with pytest.raises(V4PoolKeyError):
        discover_pool(_WETH, _USDC, fee=fee)
    with pytest.raises(V4PoolKeyError):
        resolve_v4_tick_spacing(fee)


def test_f3_canonical_surfaces_agree_on_the_same_pool_id() -> None:
    sdk = UniswapV4SDK(chain="arbitrum")

    for fee, spacing in V4_DEFAULT_TICK_SPACING.items():
        from_sdk = compute_pool_id(sdk.compute_pool_key(_WETH, _USDC, fee))
        from_discovery = discover_pool(_WETH, _USDC, fee=fee).pool_id
        from_explicit = compute_pool_id(sdk.compute_pool_key(_WETH, _USDC, fee, spacing))

        assert from_sdk == from_discovery == from_explicit, fee


# F4 -- the dynamic-fee sentinel is not a rate


def test_f4_the_sentinel_is_the_v4_constant_not_a_percentage() -> None:
    assert V4_DYNAMIC_FEE_FLAG == 0x800000
    assert V4_DYNAMIC_FEE_FLAG > V4_MAX_LP_FEE, "the sentinel sits above every legal static fee"


def test_f4_offline_quotes_refuse_the_dynamic_fee_sentinel() -> None:
    """Priced as hundredths of a bip the sentinel is an 8.39x fee, i.e. a negative output.

    A negative amount_out becomes a negative minOut, which is either a revert or
    a swap with no downside protection at all.
    """
    sdk = UniswapV4SDK(chain="arbitrum")

    with pytest.raises(V4PoolKeyError):
        sdk.get_quote_local(_WETH, _USDC, 10**18, fee_tier=V4_DYNAMIC_FEE_FLAG, price_ratio=Decimal("2000"))


def test_f4_lp_open_refuses_a_dynamic_fee_pool(adapter: UniswapV4Adapter) -> None:
    """A dynamic-fee pool needs a hook to set its fee, and hooked pools are refused."""
    with pytest.raises(UniswapV4UnsupportedPoolError):
        adapter.compile_lp_open_intent(_lp_open_intent(V4_DYNAMIC_FEE_FLAG, tick_spacing=60), _ORACLE)


# F5 -- static fees are bounded by MAX_LP_FEE


@pytest.mark.parametrize("fee", [V4_MAX_LP_FEE + 1, 0xFFFFFF, -1])
def test_f5_a_fee_outside_the_static_range_is_refused(fee: int) -> None:
    with pytest.raises(V4PoolKeyError):
        validate_v4_static_fee(fee)


@pytest.mark.parametrize("fee", [0, 1, 100, 3000, V4_MAX_LP_FEE])
def test_f5_the_whole_static_range_is_accepted(fee: int) -> None:
    """V4 fees are a range, not V3's four-value enumeration."""
    assert validate_v4_static_fee(fee) == fee


def test_f5_lp_open_refuses_an_over_range_fee_even_with_an_explicit_spacing(
    adapter: UniswapV4Adapter,
) -> None:
    with pytest.raises(UniswapV4UnsupportedPoolError):
        adapter.compile_lp_open_intent(_lp_open_intent(V4_MAX_LP_FEE + 1, tick_spacing=60), _ORACLE)


def test_f5_offline_quotes_refuse_an_over_range_fee() -> None:
    sdk = UniswapV4SDK(chain="arbitrum")

    with pytest.raises(V4PoolKeyError):
        sdk.get_quote_local(_WETH, _USDC, 10**18, fee_tier=V4_MAX_LP_FEE + 1, price_ratio=Decimal("2000"))


def test_f5_the_executable_quote_declares_which_fees_it_can_reach() -> None:
    """The on-chain quote path is narrower than V4: it accepts only the four
    canonical tiers. That is a capability limit, not a protocol rule, so it must
    be a loud refusal naming V4 -- never a silent retarget to a nearby tier.
    """
    from almanak.connectors.uniswap_v4.sdk import FEE_TIERS

    assert set(FEE_TIERS) == set(V4_DEFAULT_TICK_SPACING), (
        "the executable quote's reachable fees must be exactly the canonically paired ones"
    )
    assert "fee_tier" in inspect.signature(UniswapV4SDK.get_quote).parameters


def test_dynamic_identity_does_not_embed_a_static_rate():
    from almanak.connectors._strategy_base.v4_pool_abi import is_v4_dynamic_fee, validate_v4_fee_field

    assert validate_v4_fee_field(0x800000) == 0x800000
    assert not is_v4_dynamic_fee(0x800000 | 3000)
    with pytest.raises(V4PoolKeyError):
        validate_v4_fee_field(0x800000 | 3000)
