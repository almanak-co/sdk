"""VIB-4580: lock the V4 LP_OPEN max-spend invariant.

The on-chain ``PositionManager.MINT_POSITION`` action carries ``amount0Max`` /
``amount1Max`` caps and reverts with ``MaximumAmountExceeded`` if the settled
amount for the minted ``liquidity`` exceeds them. Therefore the **encoded caps
ARE the hard spend ceiling**: actual spend can never exceed them on a successful
mint.

VIB-4580 reported a small WETH overspend (``spent > requested max``) on Polygon.
The original defect was that the slippage buffer was applied to the **max**
(raising ``amount*Max`` above the user-requested wei), instead of to the
**liquidity budget**. VIB-2180 (PR #2508) fixed the design so that:

- ``liquidity`` is sized from a *discounted* budget (``amount*_wei /
  (1 + slippage)``), and
- ``amount0Max`` / ``amount1Max`` are pinned to the **exact requested wei** —
  never widened.

This means the requested amount is the on-chain cap, so a successful LP_OPEN
provably cannot spend more than the user asked for; an estimate that would
require more reverts rather than overspends.

These tests pin that invariant at the adapter->SDK boundary so a regression that
re-introduces a buffered max (the VIB-4580 bug class) fails fast and
fork-independently — the intent test
``tests/intents/polygon/test_uniswap_v4_lp_open.py`` is the on-chain backstop,
but it only trips when the live fork price diverges enough, so it cannot be the
sole guard.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

# Real mainnet token addresses so the PoolKey sort order matches production.
# Crucially the two chains sort the SAME pair OPPOSITELY, which exercises the
# adapter's amount0/amount1 swap on both branches:
#   - Arbitrum: WETH (0x82af…) < USDC (0xaf88…) → WETH is currency0, USDC currency1.
#   - Polygon:  USDC (0x3c49…) < WETH (0x7ceb…) → USDC is currency0, WETH currency1.
# Assertions below resolve the cap per token symbol (not slot) so they hold on
# either ordering.
_TOKENS = {
    "arbitrum": {
        "WETH": ("0x82af49447d8a07e3bd95bd0d56f35241523fbab1", 18),
        "USDC": ("0xaf88d065e77c8cc2239327c5edb3a432268e5831", 6),
    },
    "polygon": {
        "WETH": ("0x7ceb23fd6bc0add59e62ac25578270cff1b9f619", 18),
        "USDC": ("0x3c499c542cef5e3811e1192ce70d8cc03d5c3359", 6),
    },
}


def _make_resolver(chain: str):
    resolver = MagicMock()
    table = _TOKENS[chain]

    def resolve_for_swap(symbol, _chain):
        addr, dec = table[symbol.upper()]
        return MagicMock(address=addr, decimals=dec, is_native=False)

    resolver.resolve_for_swap = resolve_for_swap
    resolver.resolve = resolve_for_swap
    return resolver


def _make_adapter(chain: str, rpc_url: str | None = None):
    config = UniswapV4Config(
        chain=chain,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        rpc_url=rpc_url,
    )
    return UniswapV4Adapter(config=config, token_resolver=_make_resolver(chain))


def _make_intent(amount_weth: Decimal, amount_usdc: Decimal, *, allow_estimated: bool):
    from almanak.framework.intents.vocabulary import LPOpenIntent

    return LPOpenIntent(
        pool="WETH/USDC/3000",
        amount0=amount_weth,  # intent.amount0 is the WETH leg by symbol order
        amount1=amount_usdc,
        range_lower=Decimal("1000"),
        range_upper=Decimal("10000"),
        protocol="uniswap_v4",
        chain="ignored",
        protocol_params={"allow_estimated_price": True} if allow_estimated else None,
    )


def _capture_mint_params(adapter: UniswapV4Adapter):
    """Patch the SDK mint builder to capture the LPMintParams it receives.

    Returns (spy, captured) where ``captured["params"]`` holds the LPMintParams
    after a successful compile.
    """
    captured: dict = {}
    orig = adapter._sdk.build_mint_position_tx

    def spy(params, *args, **kwargs):
        captured["params"] = params
        return orig(params, *args, **kwargs)

    return spy, captured


_PRICE_ORACLE = {"WETH": Decimal("2260"), "USDC": Decimal("1")}


def _caps_by_symbol(chain: str, params) -> dict[str, int]:
    """Map the encoded amount0Max/amount1Max back to token symbol.

    The adapter sorts the PoolKey by address, which differs per chain, so the
    invariant ("encoded cap == requested wei") must be asserted per symbol, not
    per slot.
    """
    weth_addr = _TOKENS[chain]["WETH"][0].lower()
    usdc_addr = _TOKENS[chain]["USDC"][0].lower()
    c0 = params.pool_key.currency0.lower()
    caps = {c0: params.amount0_max, params.pool_key.currency1.lower(): params.amount1_max}
    return {"WETH": caps[weth_addr], "USDC": caps[usdc_addr]}


def _budgets_by_symbol(chain: str, params, metadata) -> dict[str, int]:
    """Map the discounted liquidity budgets back to token symbol (same slot order)."""
    weth_addr = _TOKENS[chain]["WETH"][0].lower()
    usdc_addr = _TOKENS[chain]["USDC"][0].lower()
    c0 = params.pool_key.currency0.lower()
    budgets = {
        c0: int(metadata["amount0_liquidity_budget"]),
        params.pool_key.currency1.lower(): int(metadata["amount1_liquidity_budget"]),
    }
    return {"WETH": budgets[weth_addr], "USDC": budgets[usdc_addr]}


@pytest.mark.parametrize("chain", ["arbitrum", "polygon"])
def test_encoded_max_equals_requested_wei_estimated_price(chain):
    """Estimated-price path: encoded amount*Max == EXACT requested wei (no buffer).

    The slippage buffer must reduce the liquidity budget, never raise the caps.
    """
    amount_weth = Decimal("0.01")
    amount_usdc = Decimal("25")
    weth_wei = int(amount_weth * Decimal(10**18))
    usdc_wei = int(amount_usdc * Decimal(10**6))

    adapter = _make_adapter(chain)  # no rpc_url → estimated price path
    intent = _make_intent(amount_weth, amount_usdc, allow_estimated=True)

    spy, captured = _capture_mint_params(adapter)
    with patch.object(adapter._sdk, "build_mint_position_tx", side_effect=spy):
        bundle = adapter.compile_lp_open_intent(intent, _PRICE_ORACLE)

    assert bundle.metadata["price_source"] != "on_chain"
    params = captured["params"]
    caps = _caps_by_symbol(chain, params)
    budgets = _budgets_by_symbol(chain, params, bundle.metadata)
    assert caps["USDC"] == usdc_wei, (
        f"USDC cap must equal requested wei exactly; got {caps['USDC']}, requested {usdc_wei}"
    )
    assert caps["WETH"] == weth_wei, (
        f"WETH cap must equal requested wei exactly — a value > {weth_wei} "
        f"re-introduces the VIB-4580 overspend; got {caps['WETH']}"
    )
    # The discounted liquidity budget (surfaced in metadata) must be STRICTLY
    # below the cap — that headroom is what absorbs estimate drift on-chain.
    assert budgets["WETH"] < caps["WETH"]
    assert budgets["USDC"] < caps["USDC"]


@pytest.mark.parametrize("chain", ["arbitrum", "polygon"])
def test_encoded_max_equals_requested_wei_onchain_price(chain):
    """On-chain-price path: encoded amount*Max still == EXACT requested wei."""
    amount_weth = Decimal("0.01")
    amount_usdc = Decimal("25")
    weth_wei = int(amount_weth * Decimal(10**18))
    usdc_wei = int(amount_usdc * Decimal(10**6))

    adapter = _make_adapter(chain, rpc_url="http://localhost:8545")
    intent = _make_intent(amount_weth, amount_usdc, allow_estimated=True)

    spy, captured = _capture_mint_params(adapter)
    # sqrtPriceX96 for ~2260 USDC/WETH; mid-range so liquidity is in-range and
    # both legs are non-zero. The exact value is irrelevant to the cap invariant.
    with (
        patch.object(adapter._sdk, "get_pool_sqrt_price", return_value=1930404233069694178259720106207302),
        patch.object(adapter._sdk, "build_mint_position_tx", side_effect=spy),
    ):
        bundle = adapter.compile_lp_open_intent(intent, _PRICE_ORACLE)

    assert bundle.metadata["price_source"] == "on_chain"
    params = captured["params"]
    caps = _caps_by_symbol(chain, params)
    budgets = _budgets_by_symbol(chain, params, bundle.metadata)
    assert caps["USDC"] == usdc_wei
    assert caps["WETH"] == weth_wei
    # On-chain price keeps the tighter 5% floor but the cap is still the exact
    # requested wei, and the budget is still strictly below it.
    assert bundle.metadata["effective_slippage_bps"] == 500
    assert budgets["WETH"] < caps["WETH"]


def test_higher_slippage_does_not_raise_the_cap():
    """A wider slippage tolerance lowers the budget further but NEVER lifts the cap.

    Directly guards the VIB-4580 bug class: if a future change wired the slippage
    buffer back into amount*Max, a larger slippage would raise the encoded cap.
    """
    amount_weth = Decimal("0.01")
    amount_usdc = Decimal("25")
    weth_wei = int(amount_weth * Decimal(10**18))
    usdc_wei = int(amount_usdc * Decimal(10**6))

    adapter = _make_adapter("polygon")
    # LPOpenIntent (frozen, extra="forbid") has no max_slippage field; the adapter
    # reads it defensively via getattr(intent, "max_slippage", None). Mirror the
    # production access with a SimpleNamespace carrying a wide tolerance — exactly
    # the pattern test_adapter_lp_open_estimated_price.py uses.
    intent = SimpleNamespace(
        pool="WETH/USDC/3000",
        amount0=amount_weth,
        amount1=amount_usdc,
        range_lower=Decimal("1000"),
        range_upper=Decimal("10000"),
        protocol="uniswap_v4",
        protocol_params={"allow_estimated_price": True},
        max_slippage=Decimal("0.50"),
        intent_id="vib4580-wide-slippage",
    )

    spy, captured = _capture_mint_params(adapter)
    with patch.object(adapter._sdk, "build_mint_position_tx", side_effect=spy):
        bundle = adapter.compile_lp_open_intent(intent, _PRICE_ORACLE)

    params = captured["params"]
    caps = _caps_by_symbol("polygon", params)
    budgets = _budgets_by_symbol("polygon", params, bundle.metadata)
    # Cap is pinned to requested wei regardless of slippage magnitude.
    assert caps["USDC"] == usdc_wei
    assert caps["WETH"] == weth_wei
    # Budget shrank well below the cap because slippage is large.
    assert budgets["WETH"] < weth_wei
