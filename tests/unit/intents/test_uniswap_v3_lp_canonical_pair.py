"""The Uniswap V3 exact-LP helper must follow pool token0/token1 order.

A bare pool address on LPOpenIntent uses the pool contract's canonical
orientation. Arbitrum/Base WETH/USDC have WETH as token0; Ethereum has USDC.
Hard-coding amount0=WETH would compile inverted amounts and a nonsense range
on Ethereum. Restoring the WETH-is-always-token0 mapping fails the Ethereum
case below.
"""

from __future__ import annotations

from decimal import Decimal

from tests.intents._uniswap_v3_lp_exact_proofs import (
    FEE_TIER,
    RANGE_LOWER,
    RANGE_UPPER,
    USDC_AMOUNT,
    WETH_AMOUNT,
    _canonical_volatile_stable_pair,
    run_uniswap_v3_lp_open_exact_proof,
)


def _default_weth_usdc_pair(weth: str, usdc: str):
    """Call the mapping the way LP_OPEN calls it, with LP_OPEN's own defaults.

    Binding these arguments here rather than through a WETH/USDC wrapper is the
    point: a wrapper re-declares the defaults, so transposing the SIGNATURE
    defaults would leave this file green. A transposition inside LP_OPEN's own
    call is a different mutation and this file cannot see it -- that one is
    excluded by the mapping being keyword-only, not by any assertion here.
    """
    return _canonical_volatile_stable_pair(
        volatile=weth,
        stable=usdc,
        volatile_amount=WETH_AMOUNT,
        stable_amount=USDC_AMOUNT,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
    )


# Arbitrum WETH < USDC by address. Ethereum USDC < WETH.
ARBITRUM_WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
ARBITRUM_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
ETHEREUM_WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
ETHEREUM_USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


def test_arbitrum_keeps_weth_as_token0() -> None:
    pair = _default_weth_usdc_pair(ARBITRUM_WETH, ARBITRUM_USDC)
    token0, token1 = pair.token0, pair.token1
    amount0, amount1 = pair.amount0, pair.amount1
    range_lower, range_upper = pair.range_lower, pair.range_upper
    assert token0.lower() == ARBITRUM_WETH.lower()
    assert token1.lower() == ARBITRUM_USDC.lower()
    assert amount0 == WETH_AMOUNT
    assert amount1 == USDC_AMOUNT
    assert range_lower == RANGE_LOWER
    assert range_upper == RANGE_UPPER


def test_ethereum_puts_usdc_as_token0_and_inverts_the_price_band() -> None:
    pair = _default_weth_usdc_pair(ETHEREUM_WETH, ETHEREUM_USDC)
    token0, token1 = pair.token0, pair.token1
    amount0, amount1 = pair.amount0, pair.amount1
    range_lower, range_upper = pair.range_lower, pair.range_upper
    assert token0.lower() == ETHEREUM_USDC.lower()
    assert token1.lower() == ETHEREUM_WETH.lower()
    assert amount0 == USDC_AMOUNT
    assert amount1 == WETH_AMOUNT
    assert range_lower == Decimal(1) / RANGE_UPPER
    assert range_upper == Decimal(1) / RANGE_LOWER


def test_bsc_usdt_wbnb_inverts_the_usdt_per_wbnb_band() -> None:
    # USDT < WBNB by address on BSC, so token0 is the stable and the
    # quote-per-base band must invert to WBNB-per-USDT.
    usdt = "0x55d398326f99059fF775485246999027B3197955"
    wbnb = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
    volatile_amount = Decimal("0.01")
    stable_amount = Decimal("6")
    quote_lower = Decimal("100")
    quote_upper = Decimal("10000")
    pair = _canonical_volatile_stable_pair(
        volatile=wbnb,
        stable=usdt,
        volatile_amount=volatile_amount,
        stable_amount=stable_amount,
        range_lower=quote_lower,
        range_upper=quote_upper,
    )
    token0, token1 = pair.token0, pair.token1
    amount0, amount1 = pair.amount0, pair.amount1
    range_lower, range_upper = pair.range_lower, pair.range_upper
    assert token0.lower() == usdt.lower()
    assert token1.lower() == wbnb.lower()
    assert amount0 == stable_amount
    assert amount1 == volatile_amount
    assert range_lower == Decimal(1) / quote_upper
    assert range_upper == Decimal(1) / quote_lower


def test_lp_open_defaults_still_bind_the_pair_the_way_this_file_asserts_it() -> None:
    """The two cases above only prove the mapping if LP_OPEN feeds it these
    arguments. Nothing else re-reads the live signature, so a transposition of
    the SIGNATURE DEFAULTS or an inverted default band would otherwise pass.
    A transposition inside LP_OPEN's own call or unpack is a different mutation
    that this assertion cannot see; those raise at runtime instead -- see the
    keyword-only and CanonicalPair checks below.
    """
    import inspect

    defaults = {
        name: parameter.default
        for name, parameter in inspect.signature(run_uniswap_v3_lp_open_exact_proof).parameters.items()
    }
    assert defaults["volatile_symbol"] == "WETH"
    assert defaults["stable_symbol"] == "USDC"
    assert defaults["volatile_amount"] == WETH_AMOUNT
    assert defaults["stable_amount"] == USDC_AMOUNT
    assert defaults["range_lower"] == RANGE_LOWER
    assert defaults["range_upper"] == RANGE_UPPER
    assert defaults["fee_tier"] == FEE_TIER
    assert RANGE_LOWER < RANGE_UPPER, "a quote-per-base band must be ordered low to high"


def test_the_pair_mapping_can_only_be_called_by_keyword() -> None:
    """The signature assertion above reads defaults, never LP_OPEN's own call.

    A reviewer demonstrated the gap twice: first swapping the third and fourth
    positional ARGUMENTS inside LP_OPEN, then -- after keyword-only closed that --
    swapping the two amounts in the positional UNPACK of the return value. Both
    left every test here green while minting 1 WETH where 0.001 was meant. The
    two Decimal pairs are adjacent and interchangeable by type, so nothing about
    the values catches either one. Keyword-only parameters make the positional
    call raise, and a non-iterable frozen dataclass makes the positional unpack
    raise -- this test pins both, and pins that the return type is not a tuple.
    A NamedTuple would satisfy the first check and fail the second: it subclasses
    tuple, so the unpack stays legal and the surface is only unused.

    What remains possible is a crossing written in NAMES -- `pair.amount1` where
    `amount0` is meant, or crossed values in a caller's `_PAIR` dict. Nothing
    here catches those; they read as deliberate edits at review time, which is
    the whole of what naming buys.
    """
    import ast
    import dataclasses
    import inspect
    import pathlib

    import pytest

    from tests.intents import _uniswap_v3_lp_exact_proofs as helper

    parameters = inspect.signature(_canonical_volatile_stable_pair).parameters.values()
    assert parameters, "mapping lost its parameters"
    assert all(p.kind is inspect.Parameter.KEYWORD_ONLY for p in parameters), (
        "every parameter must be keyword-only; a positional pair is transposable"
    )

    with pytest.raises(TypeError):
        _canonical_volatile_stable_pair(  # type: ignore[misc]
            ARBITRUM_WETH, ARBITRUM_USDC, WETH_AMOUNT, USDC_AMOUNT, RANGE_LOWER, RANGE_UPPER
        )

    # A bare tuple -- or a NamedTuple, which subclasses one -- would leave the
    # consumer-side unpack legal, which is the half keyword-only did not close.
    result = _default_weth_usdc_pair(ARBITRUM_WETH, ARBITRUM_USDC)
    assert isinstance(result, helper.CanonicalPair), "mapping must return a named pair"
    assert tuple(f.name for f in dataclasses.fields(result)) == (
        "token0",
        "token1",
        "amount0",
        "amount1",
        "range_lower",
        "range_upper",
    )
    with pytest.raises(TypeError):
        _token0, _token1, _a0, _a1, _lo, _hi = result  # type: ignore[misc]

    # Every call site IN THE HELPER MODULE must bind by keyword: a positional one
    # there would otherwise fail only when a fork test runs the helper. Call sites
    # elsewhere need no sweep -- keyword-only makes those a runtime TypeError, and
    # this file deliberately keeps one positional call inside pytest.raises above.
    source = pathlib.Path(inspect.getsourcefile(helper)).read_text(encoding="utf-8")
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "_canonical_volatile_stable_pair":
            assert not node.args, f"call at line {node.lineno} passes positional arguments"
