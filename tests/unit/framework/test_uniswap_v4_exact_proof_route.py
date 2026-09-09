"""Controls for the pool a declared V4 swap contract requires.

The expectation must follow the contract the cell declares, not the route the
connector happens to compile: an expectation copied from connector internals
agrees with the connector by construction and can prove nothing about it.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from almanak.connectors._strategy_base.v4_pool_abi import compute_v4_pool_id
from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY, UniswapV4SDK
from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE
from tests.intents._uniswap_v4_exact_proofs import (
    V4_SWAP_PROFILE,
    V4_SWAP_ROUTE_PROFILE,
    expected_pool_key,
)

CHAIN = "robinhood"
USDG = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"
USDE = "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34"
FEE = 3000

# Pool ids read from the Robinhood fork pinned at block 57,900,000.
ERC20_POOL_ID = "0x77c25b9386d47de62e0155c393696e9f43f7e6d036c6ca52f66735ccbb8808a7"
NATIVE_POOL_ID = "0xd313d79d9d6a714e7bdf02fc42a2c27ede7e51928ffd605126fe9e1192630cf8"


@pytest.fixture
def sdk() -> UniswapV4SDK:
    return UniswapV4SDK(chain=CHAIN)


def _pool_id(key) -> str:
    return compute_v4_pool_id(key.currency0, key.currency1, key.fee, key.tick_spacing, key.hooks).lower()


def test_erc20_contract_names_the_pool_of_the_requested_assets(sdk: UniswapV4SDK) -> None:
    weth = WRAPPED_NATIVE[CHAIN]
    key = expected_pool_key(sdk, chain=CHAIN, token_in=USDG, token_out=weth, fee_tier=FEE, profile=V4_SWAP_PROFILE)
    assert {key.currency0, key.currency1} == {USDG.lower(), weth.lower()}
    assert _pool_id(key) == ERC20_POOL_ID


def test_route_contract_substitutes_the_native_currency_for_the_wrapper(sdk: UniswapV4SDK) -> None:
    key = expected_pool_key(
        sdk,
        chain=CHAIN,
        token_in=USDG,
        token_out=WRAPPED_NATIVE[CHAIN],
        fee_tier=FEE,
        profile=V4_SWAP_ROUTE_PROFILE,
    )
    assert key.currency0 == NATIVE_CURRENCY
    assert _pool_id(key) == NATIVE_POOL_ID


def test_route_contract_refuses_a_pair_that_holds_no_wrapper(sdk: UniswapV4SDK) -> None:
    with pytest.raises(ValueError, match="converts nothing"):
        expected_pool_key(sdk, chain=CHAIN, token_in=USDG, token_out=USDE, fee_tier=FEE, profile=V4_SWAP_ROUTE_PROFILE)


def test_an_undeclared_contract_is_refused(sdk: UniswapV4SDK) -> None:
    with pytest.raises(ValueError, match="Unsupported V4 swap contract profile"):
        expected_pool_key(sdk, chain=CHAIN, token_in=USDG, token_out=USDE, fee_tier=FEE, profile="swap.v1")


def _declared_and_requested(path: Path) -> list[tuple[str, str | None, str | None]]:
    """Return (function, qa_proof contract, profile passed to the helper) per node."""
    tree = ast.parse(path.read_text())
    found: list[tuple[str, str | None, str | None]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            continue
        declared: str | None = None
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                continue
            if decorator.func.attr != "qa_proof":
                continue
            for keyword in decorator.keywords:
                if keyword.arg == "contract" and isinstance(keyword.value, ast.Constant):
                    declared = keyword.value.value
        if declared is None:
            continue
        requested: str | None = None
        for call in ast.walk(node):
            if not isinstance(call, ast.Call):
                continue
            name = call.func.id if isinstance(call.func, ast.Name) else None
            if name != "run_uniswap_v4_swap_exact_proof":
                continue
            for keyword in call.keywords:
                if keyword.arg == "profile" and isinstance(keyword.value, ast.Constant):
                    requested = keyword.value.value
        found.append((node.name, declared, requested))
    return found


def test_every_v4_exact_swap_cell_proves_the_contract_it_declares() -> None:
    """The catalogue reads the marker; the run reads the argument.

    The marker must stay a literal for the static catalogue scan, so nothing in
    the language stops the two from drifting -- and a cell that declared the
    conversion contract while proving the plain one would seal green on evidence
    its declaration never asked for.
    """
    root = Path(__file__).resolve().parents[3] / "tests" / "intents"
    sources = [path for path in root.rglob("test_*.py") if "run_uniswap_v4_swap_exact_proof" in path.read_text()]
    assert sources, "no V4 exact-swap proof cells found; this guard would pass vacuously"
    for path in sources:
        for name, declared, requested in _declared_and_requested(path):
            assert requested == declared, (
                f"{path.relative_to(root)}::{name} declares {declared!r} but proves {requested!r}"
            )
