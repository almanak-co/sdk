"""Regression tests for the Uniswap V3 permission manifest on Robinhood Chain.

Robinhood Chain (4663, an Arbitrum Orbit L2) has NO USDC/USDT — its canonical
stablecoin is ``USDG`` (6 dec) and the only liquid uniswap_v3 pools are
``WETH/USDG`` (primary fee tier 500). The framework's synthetic stable-pair
resolution (``_candidate_stable_symbols``) sorts ``USDE`` (Ethena USDe, also
registered as a robinhood stablecoin) AHEAD of ``USDG``, so without a
per-connector override the synthetic SWAP/LP discovery seeds ``(USDe, WETH)``
and the Zodiac manifest authorises ERC-20 ``approve`` on USDe — a token the
real WETH/USDG strategy never touches. Every value transfer then reverts at
``execTransactionWithRole`` (the same failure class as the sushiswap_v3 bsc
trap in #1902).

The fix pins ``synthetic_swap_pair`` / ``synthetic_lp_pair`` (and the liquid
``synthetic_fee_tier`` 500) to ``(USDG, WETH)`` on the uniswap_v3 connector's
``PermissionHints``. These tests pin the corrected approve coverage plus a
negative assertion on USDe so the trap cannot reappear.
"""

from __future__ import annotations

from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3, UNISWAP_V3_TOKENS
from almanak.connectors.uniswap_v3.adapter import EXACT_INPUT_SINGLE_SELECTOR
from almanak.framework.intents.compiler import (
    NFT_POSITION_BURN_SELECTOR,
    NFT_POSITION_COLLECT_SELECTOR,
    NFT_POSITION_DECREASE_SELECTOR,
    NFT_POSITION_MINT_SELECTOR,
)
from almanak.framework.intents.compiler_constants import (
    CHAIN_TOKENS,
    ERC20_APPROVE_SELECTOR,
)
from almanak.framework.permissions.generator import generate_manifest

_CHAIN = "robinhood"
_USDG = UNISWAP_V3_TOKENS[_CHAIN]["USDG"].lower()
_WETH = UNISWAP_V3_TOKENS[_CHAIN]["WETH"].lower()
# Ethena USDe on robinhood — the stale stable the resolver picks first; it must
# NOT appear as an approve target once the override pins the canonical pair.
_USDE = CHAIN_TOKENS[_CHAIN]["usde"].lower()
_SWAP_ROUTER_02 = UNISWAP_V3[_CHAIN]["swap_router_02"].lower()
_NPM = UNISWAP_V3[_CHAIN]["position_manager"].lower()


def _manifest_pairs(intent_types: list[str]) -> set[tuple[str, str]]:
    """Return the (target, selector) set produced by the manifest generator."""
    manifest = generate_manifest(
        strategy_name="uniswap-v3-robinhood-manifest-regression",
        chain=_CHAIN,
        supported_protocols=["uniswap_v3"],
        intent_types=intent_types,
    )
    return {
        (perm.target.lower(), sel.selector.lower())
        for perm in manifest.permissions
        for sel in perm.function_selectors
    }


def _approve_targets(intent_types: list[str]) -> set[str]:
    pairs = _manifest_pairs(intent_types)
    return {target for target, sel in pairs if sel == ERC20_APPROVE_SELECTOR}


class TestUniswapV3RobinhoodManifest:
    def test_manifest_non_empty(self) -> None:
        assert _manifest_pairs(["SWAP", "LP_OPEN", "LP_CLOSE"]), (
            "uniswap_v3 robinhood manifest must be non-empty"
        )

    def test_swap_authorises_router_and_usdg_approve(self) -> None:
        pairs = _manifest_pairs(["SWAP"])
        assert (_SWAP_ROUTER_02, EXACT_INPUT_SINGLE_SELECTOR) in pairs, (
            f"SWAP manifest missing SwapRouter02 exactInputSingle "
            f"({EXACT_INPUT_SINGLE_SELECTOR}) on {_SWAP_ROUTER_02}"
        )
        approves = _approve_targets(["SWAP"])
        assert approves == {_USDG}, (
            f"SWAP manifest must authorise approve on exactly USDG ({_USDG}) — "
            f"any extra target is silent over-authorisation; got {sorted(approves)}"
        )
        assert _USDE not in approves, (
            f"SWAP manifest must NOT authorise approve on USDe ({_USDE}) — "
            "robinhood trades WETH/USDG, not USDe."
        )

    def test_lp_open_authorises_npm_mint_and_pair_approve(self) -> None:
        pairs = _manifest_pairs(["LP_OPEN"])
        assert (_NPM, NFT_POSITION_MINT_SELECTOR) in pairs, (
            f"LP_OPEN manifest missing NPM mint ({NFT_POSITION_MINT_SELECTOR}) on {_NPM}"
        )
        approves = _approve_targets(["LP_OPEN"])
        assert approves == {_USDG, _WETH}, (
            f"LP_OPEN manifest must authorise approve on exactly USDG and WETH — "
            f"any extra target is silent over-authorisation; got {sorted(approves)}"
        )
        assert _USDE not in approves, (
            f"LP_OPEN manifest must NOT authorise approve on USDe ({_USDE}) — "
            "the liquid robinhood pool is WETH/USDG."
        )

    def test_lp_close_includes_npm_teardown_selectors(self) -> None:
        pairs = _manifest_pairs(["LP_CLOSE"])
        for selector, label in (
            (NFT_POSITION_DECREASE_SELECTOR, "decreaseLiquidity"),
            (NFT_POSITION_COLLECT_SELECTOR, "collect"),
            (NFT_POSITION_BURN_SELECTOR, "burn"),
        ):
            assert (_NPM, selector) in pairs, (
                f"LP_CLOSE manifest missing {label} ({selector}) on NPM {_NPM}"
            )
