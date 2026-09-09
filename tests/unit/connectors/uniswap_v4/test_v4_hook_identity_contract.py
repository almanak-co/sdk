"""Protocol QA: Uniswap V4 hook identity contract.

Hooks are the V4-only axis of pool identity. A V3 pool is fully named by
(token0, token1, fee); a V4 pool is named by the 5-tuple PoolKey
``(currency0, currency1, fee, tickSpacing, hooks)``, and its PoolId is
``keccak256(abi.encode(PoolKey))``. Two pools over the same pair and fee that
differ only in their hook address are *different pools* with different
liquidity, different prices, and different settlement rules.

The declared invariant set this module owns. Green here means these six were
satisfied. Hook execution additionally requires a reviewed operation profile.

H1  Hook capabilities are the low 14 bits of the hook address and nothing else.
H2  The hook address is part of pool identity: PoolKeys differing only in
    ``hooks`` hash to different PoolIds, matching an independent abi.encode.
H3  PoolKey normalization sorts currencies without moving or dropping ``hooks``.
H4  LP_OPEN refuses unverified hooked pools loudly.
H5  The default swap stays hookless; full-key quoting accepts explicit hook data.
H6  A hook address never becomes a wallet permission target -- in V4 the
    PoolManager calls the hook, the wallet never does.

Sources: v4-core ``Hooks.sol`` (permission bit layout), ``PoolId.sol``
(``toId``), ``PoolKey.sol``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from eth_abi import encode as abi_encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.adapter import (
    UniswapV4Adapter,
    UniswapV4Config,
    UniswapV4UnsupportedPoolError,
)
from almanak.connectors.uniswap_v4.hooks import HookFlags, compute_pool_id
from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY, PoolKey, UniswapV4SDK

_WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_WALLET = "0x1234567890abcdef1234567890abcdef12345678"

# The 14 permission bits v4-core mines into a hook address, bit position to the
# HookFlags attribute it must set. Transcribed from Hooks.sol, not from the
# connector's own constants -- a shared constant could not detect a wrong bit.
_HOOK_PERMISSION_BITS: dict[int, str] = {
    13: "before_initialize",
    12: "after_initialize",
    11: "before_add_liquidity",
    10: "after_add_liquidity",
    9: "before_remove_liquidity",
    8: "after_remove_liquidity",
    7: "before_swap",
    6: "after_swap",
    5: "before_donate",
    4: "after_donate",
    3: "before_swap_returns_delta",
    2: "after_swap_returns_delta",
    1: "after_add_liquidity_returns_delta",
    0: "after_remove_liquidity_returns_delta",
}


def _address(value: int) -> str:
    return "0x" + format(value, "040x")


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


# H1 -- hook capabilities are the low 14 bits and nothing else


@pytest.mark.parametrize(("bit", "attribute"), sorted(_HOOK_PERMISSION_BITS.items()))
def test_h1_each_permission_bit_decodes_to_exactly_one_capability(bit: int, attribute: str) -> None:
    flags = HookFlags.from_address(_address(1 << bit))

    set_attributes = {name for name in _HOOK_PERMISSION_BITS.values() if getattr(flags, name)}
    assert set_attributes == {attribute}


def test_h1_bits_above_the_permission_window_grant_no_capability() -> None:
    """Address entropy above bit 13 is not a permission; only the low 14 bits are."""
    high_entropy = _address(((1 << 160) - 1) ^ ((1 << 14) - 1))

    flags = HookFlags.from_address(high_entropy)

    assert not any(getattr(flags, name) for name in _HOOK_PERMISSION_BITS.values())
    assert flags.is_empty


def test_h1_the_zero_address_is_the_no_hook_sentinel() -> None:
    assert HookFlags.from_address(NATIVE_CURRENCY).is_empty


# H2 -- the hook address is part of pool identity


def _independent_pool_id(key: PoolKey) -> str:
    """PoolId re-derived with eth_abi, independent of the connector's encoder."""
    encoded = abi_encode(
        ["address", "address", "uint24", "int24", "address"],
        [key.currency0, key.currency1, key.fee, key.tick_spacing, key.hooks],
    )
    return "0x" + keccak(encoded).hex()


def test_h2_pool_id_matches_an_independently_encoded_keccak() -> None:
    key = PoolKey(currency0=_WETH, currency1=_USDC, fee=3000, tick_spacing=60, hooks=_address(0x2400))

    assert compute_pool_id(key) == _independent_pool_id(key)


def test_h2_changing_only_the_hook_changes_the_pool_id() -> None:
    hookless = PoolKey(currency0=_WETH, currency1=_USDC, fee=3000, tick_spacing=60)
    hooked = PoolKey(currency0=_WETH, currency1=_USDC, fee=3000, tick_spacing=60, hooks=_address(0x80))

    assert compute_pool_id(hookless) != compute_pool_id(hooked)
    assert compute_pool_id(hooked) == _independent_pool_id(hooked)


# H3 -- normalization must not move or drop the hook


def test_h3_currency_sorting_preserves_the_hook_address() -> None:
    hook = _address(0x1000)

    forward = PoolKey(currency0=_WETH, currency1=_USDC, fee=3000, tick_spacing=60, hooks=hook)
    reversed_pair = PoolKey(currency0=_USDC, currency1=_WETH, fee=3000, tick_spacing=60, hooks=hook)

    assert forward.hooks == hook
    assert reversed_pair.hooks == hook
    assert compute_pool_id(forward) == compute_pool_id(reversed_pair)


def test_h3_hook_case_does_not_change_pool_identity() -> None:
    lower = PoolKey(currency0=_WETH, currency1=_USDC, fee=3000, tick_spacing=60, hooks=_address(0xABCD))
    upper = PoolKey(
        currency0=_WETH,
        currency1=_USDC,
        fee=3000,
        tick_spacing=60,
        hooks=_address(0xABCD).upper().replace("0X", "0x"),
    )

    assert compute_pool_id(lower) == compute_pool_id(upper)


# H4 -- LP_OPEN refuses a hooked pool loudly


def _lp_open_intent(**protocol_params):
    from almanak.framework.intents.vocabulary import LPOpenIntent

    return LPOpenIntent(
        pool="WETH/USDC/3000",
        amount0=Decimal("0.1"),
        amount1=Decimal("200"),
        range_lower=Decimal("1500"),
        range_upper=Decimal("2500"),
        protocol="uniswap_v4",
        protocol_params={"allow_estimated_price": True, **protocol_params},
    )


@pytest.mark.parametrize(
    "hook",
    [
        _address(0x80),  # beforeSwap only
        _address((1 << 14) - 1),  # every permission bit
        _address(0x101),  # afterRemoveLiquidity with its paired returns-delta bit
    ],
)
def test_h4_lp_open_requires_verified_hook_evidence(adapter: UniswapV4Adapter, hook: str) -> None:
    with pytest.raises(UniswapV4UnsupportedPoolError, match="gateway-verified operation evidence"):
        adapter.compile_lp_open_intent(
            _lp_open_intent(hooks=hook, hook_data="0x"), {"WETH": Decimal("2000"), "USDC": Decimal("1")}
        )


def test_h4_refusal_is_raised_never_a_soft_error_bundle(adapter: UniswapV4Adapter) -> None:
    """A hooked pool must never come back as an empty bundle a runner can skip past."""
    try:
        bundle = adapter.compile_lp_open_intent(
            _lp_open_intent(hooks=_address(0x80)),
            {"WETH": Decimal("2000"), "USDC": Decimal("1")},
        )
    except UniswapV4UnsupportedPoolError:
        return
    pytest.fail(f"hooked LP_OPEN degraded to a bundle instead of raising: {bundle.metadata}")


# H5 -- the swap path cannot address a hooked pool


def _decode_encoded_pool_key(calldata: str, currency1: str) -> tuple[str, int, int, str]:
    """Read the V4 PoolKey out of swap calldata, anchored on ``currency1``.

    ``ExactInputSingleParams`` leads with the five PoolKey words in order, so the
    word holding currency1 fixes the position of fee, tickSpacing and hooks. The
    hook word cannot be located by its zero value alone -- a wrapped-native leg
    encodes currency0 as address(0) too.
    """
    body = calldata.lower().removeprefix("0x")[8:]  # drop the 4-byte selector
    words = [body[index : index + 64] for index in range(0, len(body) - len(body) % 64, 64)]
    target = "0" * 24 + currency1.lower().removeprefix("0x")

    position = next((index for index, word in enumerate(words) if word == target and index >= 1), None)
    assert position is not None, "encoded swap calldata contains no currency1 pool-key word"
    assert words[position - 1][:24] == "0" * 24, "word before currency1 is not an address"

    return (
        "0x" + words[position - 1][24:],
        int(words[position + 1], 16),
        int(words[position + 2], 16),
        "0x" + words[position + 3][24:],
    )


def test_h5_swap_calldata_always_encodes_the_hookless_pool_key() -> None:
    """The hook word of the encoded ExactInputSingleParams must be address(0).

    The alternative failure is silent, not loud: a swap built against a hook
    address the caller never authorised would settle under that hook's rules.
    """
    adapter = UniswapV4Adapter(
        config=UniswapV4Config(chain="arbitrum", wallet_address=_WALLET),
        token_resolver=_resolver(),
    )

    result = adapter.swap_exact_input("WETH", "USDC", Decimal("1"), price_ratio=Decimal("2000"), offline_mode=True)
    assert result.success, "offline swap build must succeed to measure its calldata"

    from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

    router = UNISWAP_V4["arbitrum"]["universal_router"].lower()
    router_calls = [tx for tx in result.transactions if tx.to.lower() == router]
    assert len(router_calls) == 1, "expected exactly one UniversalRouter call to decode"

    currency0, fee, tick_spacing, hooks = _decode_encoded_pool_key(router_calls[0].data, _USDC)

    assert hooks == NATIVE_CURRENCY, f"encoded V4 pool key must carry hooks == address(0), got {hooks}"
    # WETH is a distinct ERC20 pool currency.
    assert currency0 == _WETH, currency0
    assert (fee, tick_spacing) == (3000, 60), (fee, tick_spacing)


def test_h5_full_key_and_hook_data_are_explicit_codec_inputs() -> None:
    import inspect

    assert "swap_params" in inspect.signature(UniswapV4Adapter.swap_exact_input).parameters
    assert {"pool_key", "hook_data"} <= set(inspect.signature(UniswapV4SDK.get_quote).parameters)
    assert "hook_data" not in inspect.signature(UniswapV4SDK.get_quote_local).parameters


# H6 -- a hook address is never a wallet permission target


def test_h6_generated_permissions_target_only_canonical_v4_contracts_and_tokens() -> None:
    """In V4 the PoolManager calls the hook; the wallet never does.

    Stated as an allowlist rather than a hook-shaped denylist: hook addresses are
    CREATE2-mined for their low 14 bits, but so are vanity deployments (Permit2's
    own address decodes as six "capabilities"), so only an enumerated target set
    fails loudly on an unexpected grant.
    """
    from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
    from almanak.connectors.uniswap_v4.sdk import PERMIT2_ADDRESS
    from almanak.core.chains import ChainRegistry
    from almanak.framework.execution.signer.safe.constants import MULTISEND_ADDRESSES
    from almanak.framework.permissions.generator import generate_manifest

    chain = "arbitrum"
    manifest = generate_manifest(
        strategy_name="uniswap-v4-hook-permission-closure",
        chain=chain,
        supported_protocols=["uniswap_v4"],
        intent_types=["SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"],
    )

    targets = {permission.target.lower() for permission in manifest.permissions}
    assert targets, "V4 permission discovery produced no targets to check"

    allowed = {
        UNISWAP_V4[chain]["universal_router"].lower(),
        UNISWAP_V4[chain]["position_manager"].lower(),
        UNISWAP_V4[chain]["pool_manager"].lower(),
        PERMIT2_ADDRESS.lower(),
        MULTISEND_ADDRESSES[chain].lower(),
    }
    allowed |= {str(address).lower() for address in ChainRegistry.resolve(chain).tokens.values()}

    assert targets <= allowed, f"unexpected V4 permission targets: {sorted(targets - allowed)}"
