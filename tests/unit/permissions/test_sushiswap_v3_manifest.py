"""Regression tests for the SushiSwap V3 permission manifest (issue #1902).

Synthetic LP discovery seeds the LP_OPEN intent's pool string from
``_get_token_pair(chain)`` — for bsc that resolves to ``(USDC, ETH-bridged)``
because bsc declares ``weth`` (Binance-pegged ETH) ahead of ``wbnb`` in
``CHAIN_TOKENS``. SushiSwap V3 on bsc has no liquid USDC/ETH pool; the real
test on bnb LPs into ``(USDT, WBNB)``, so the manifest's approve targets are
wrong and the test's ``approve(USDT)`` is denied by the Zodiac role.

The fix introduces a per-protocol ``synthetic_lp_pair`` override on
``PermissionHints`` and the sushiswap_v3 connector pins ``(USDT, WBNB)`` for
bsc. These tests pin the expected approve coverage on every supported chain
plus a negative assertion specifically for bsc so the trap cannot reappear.
"""

from __future__ import annotations

import pytest

from almanak.framework.intents.compiler import (
    LP_POSITION_MANAGERS,
    NFT_POSITION_BURN_SELECTOR,
    NFT_POSITION_COLLECT_SELECTOR,
    NFT_POSITION_DECREASE_SELECTOR,
)
from almanak.framework.intents.compiler_constants import (
    CHAIN_TOKENS,
    ERC20_APPROVE_SELECTOR,
)
from almanak.framework.permissions.generator import generate_manifest

# Uniswap V3-style mint(MintParams) selector emitted by the
# NonfungiblePositionManager when the LP_OPEN compile succeeds.
_NPM_MINT_SELECTOR = "0x88316456"

# Per-chain canonical sushiswap_v3 LP pair as (token0_key, token1_key).
# Resolved against ``CHAIN_TOKENS`` so the test stays in sync with the
# compiler's address registry rather than hard-coding addresses. The
# regression on bsc is the override to ``(usdt, wbnb)`` — every other chain
# uses the framework chain-default ``(usdc, weth-equivalent)``.
_LP_OPEN_PAIRS_BY_CHAIN: dict[str, tuple[str, str]] = {
    "ethereum": ("usdc", "weth"),
    "arbitrum": ("usdc", "weth"),
    "optimism": ("usdc", "weth"),
    "polygon": ("usdc", "weth"),
    "base": ("usdc", "weth"),
    # The regression: prior to the fix, bsc resolved to ``(usdc, weth)``
    # where ``weth`` is the Binance-pegged ETH. The canonical pair on bsc
    # is ``(USDT, WBNB)``.
    "bsc": ("usdt", "wbnb"),
}

# Binance-pegged ETH on bsc — must NOT appear as an approve target on the
# bsc sushiswap_v3 LP_OPEN manifest. Pins #1902 specifically.
_BSC_BRIDGED_ETH = "0x2170Ed0880ac9A755fd29B2688956BD959F933F8".lower()
_BSC_USDC = "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d".lower()


def _addr(chain: str, key: str) -> str:
    """Look up a token address from ``CHAIN_TOKENS`` (lowercased for set ops)."""
    return CHAIN_TOKENS[chain][key].lower()


def _manifest_pairs(intent_types: list[str], chain: str) -> set[tuple[str, str]]:
    """Return the (target, selector) set produced by the manifest generator."""
    manifest = generate_manifest(
        strategy_name="sushiswap-v3-manifest-regression",
        chain=chain,
        supported_protocols=["sushiswap_v3"],
        intent_types=intent_types,
    )
    return {
        (perm.target.lower(), sel.selector.lower())
        for perm in manifest.permissions
        for sel in perm.function_selectors
    }


def _approve_targets(intent_types: list[str], chain: str) -> set[str]:
    """Return the set of addresses authorised for ERC-20 ``approve``."""
    pairs = _manifest_pairs(intent_types, chain)
    return {target for target, sel in pairs if sel == ERC20_APPROVE_SELECTOR}


class TestSushiSwapV3LPOpenManifest:
    """LP_OPEN must authorise approves on the chain-specific liquid pair."""

    @pytest.mark.parametrize("chain", sorted(_LP_OPEN_PAIRS_BY_CHAIN))
    def test_lp_open_manifest_authorises_chain_specific_token_approve(
        self, chain: str
    ) -> None:
        """At least one token from the canonical (token0, token1) pair the
        protocol uses on each chain must get an ERC-20 approve permission so
        the Safe can fund the LP. Synthetic LP_OPEN compiles a single approve
        (only one token actually consumed under placeholder prices), but it
        MUST be on a token from the chain-specific pair — never on a stale
        chain-default token like ETH-bsc.
        """
        token0_key, token1_key = _LP_OPEN_PAIRS_BY_CHAIN[chain]
        expected_token0 = _addr(chain, token0_key)
        expected_token1 = _addr(chain, token1_key)
        expected_pair = {expected_token0, expected_token1}

        approves = _approve_targets(["LP_OPEN"], chain)
        on_pair = approves & expected_pair

        assert on_pair, (
            f"{chain}: LP_OPEN manifest must authorise approve on at least "
            f"one of the canonical pair tokens ({token0_key.upper()}={expected_token0}, "
            f"{token1_key.upper()}={expected_token1}). Got approve targets: "
            f"{sorted(approves)}"
        )

    def test_bsc_lp_open_manifest_authorises_canonical_bsc_pair(self) -> None:
        """Positive: bsc LP_OPEN's approve must be on USDT or WBNB."""
        approves = _approve_targets(["LP_OPEN"], "bsc")
        bsc_canonical = {_addr("bsc", "usdt"), _addr("bsc", "wbnb")}
        assert approves & bsc_canonical, (
            "bsc LP_OPEN manifest must authorise approve on USDT or WBNB; "
            f"got approve targets: {sorted(approves)}"
        )

    def test_bsc_lp_open_manifest_does_not_authorise_bridged_eth(self) -> None:
        """Negative regression for #1902. Before the fix, bsc's synthetic
        LP_OPEN seeded ``(USDC, ETH-bridged)`` and the manifest contained
        an approve permission on the bridged-ETH token. After the fix, the
        override resolves to ``(USDT, WBNB)`` and bridged ETH must NOT
        appear as an approve target.
        """
        approves = _approve_targets(["LP_OPEN"], "bsc")
        assert _BSC_BRIDGED_ETH not in approves, (
            "bsc LP_OPEN manifest must NOT authorise approve on Binance-pegged "
            f"ETH ({_BSC_BRIDGED_ETH}) — sushiswap_v3 on bsc LPs into USDT/WBNB. "
            "Regression of #1902."
        )
        assert _BSC_USDC not in approves, (
            "bsc LP_OPEN manifest must NOT authorise approve on USDC "
            f"({_BSC_USDC}) — sushiswap_v3 on bsc LPs into USDT/WBNB. "
            "Regression of #1902."
        )

    @pytest.mark.parametrize("chain", sorted(_LP_OPEN_PAIRS_BY_CHAIN))
    def test_lp_open_manifest_includes_npm_mint_selector(self, chain: str) -> None:
        """The synthetic LP_OPEN compile must emit the NonfungiblePositionManager
        ``mint`` selector. Without the chain-specific pair fix, the compile
        on bsc fails silently (no liquid USDC/ETH-bsc pool resolvable) and
        the manifest omits ``mint`` entirely — which alone breaks the LP_OPEN
        test even before the wrong-token approve denial fires.
        """
        npm_address = LP_POSITION_MANAGERS[chain]["sushiswap_v3"].lower()
        manifest_pairs = _manifest_pairs(["LP_OPEN"], chain)
        assert (npm_address, _NPM_MINT_SELECTOR) in manifest_pairs, (
            f"{chain}: LP_OPEN manifest missing NPM ``mint`` selector "
            f"({_NPM_MINT_SELECTOR}) on {npm_address}. "
            "Compile likely failed for the chain-default pair — see #1902."
        )


class TestSushiSwapV3LPCloseManifest:
    """LP_CLOSE must surface decreaseLiquidity / collect / burn on the NPM.

    Pinned by PR #1854; re-asserted here so the contract stays solid as
    new chains are added.
    """

    @pytest.mark.parametrize("chain", sorted(_LP_OPEN_PAIRS_BY_CHAIN))
    def test_lp_close_manifest_includes_npm_teardown_selectors(
        self, chain: str
    ) -> None:
        npm_address = LP_POSITION_MANAGERS[chain]["sushiswap_v3"].lower()
        manifest_pairs = _manifest_pairs(["LP_CLOSE"], chain)

        for selector, label in (
            (NFT_POSITION_DECREASE_SELECTOR, "decreaseLiquidity"),
            (NFT_POSITION_COLLECT_SELECTOR, "collect"),
            (NFT_POSITION_BURN_SELECTOR, "burn"),
        ):
            assert (npm_address, selector) in manifest_pairs, (
                f"{chain}: LP_CLOSE manifest missing {label} ({selector}) on "
                f"NonfungiblePositionManager {npm_address}."
            )
