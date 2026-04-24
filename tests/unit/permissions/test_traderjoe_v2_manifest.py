"""Regression tests for the TraderJoe V2 permission manifest (issue #1841).

TJv2 uses a dedicated compile path (``_compile_swap_traderjoe_v2`` / VIB-1928)
with LBRouter2 ``swapExactTokensForTokens`` on a ``Path`` struct — fundamentally
different from Uniswap V3. Its LBRouter address lives in ``LP_POSITION_MANAGERS``
rather than ``PROTOCOL_ROUTERS``, which caused ``_build_swap_intents`` to skip
TJv2 entirely and emit a manifest that omitted the LBRouter swap selector
(``0x2a443fae``). On-chain Zodiac authorisation therefore reverted at the
LBRouter2 target for both SWAP and LP_OPEN (same target; LP_OPEN only worked in
isolation because its selectors were already surfaced via static permission
hints).

These tests pin the expected coverage so the regression does not reappear:
the (target, selector) pairs the compiler emits for SWAP + LP_OPEN must be a
subset of the generated manifest.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.intents.compiler import (
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import LPOpenIntent, SwapIntent
from almanak.framework.permissions.generator import generate_manifest
from almanak.framework.permissions.synthetic_intents import build_synthetic_intents

# Avalanche LBRouter2 / tokens pinned in the case file
# (tests/intents/permission_cases/traderjoe_v2.py).
_LBROUTER2 = "0xb4315e873dbcf96ffd0acd8ea43f689d8c20fb30"
_USDC = "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"
_WAVAX = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"

# swapExactTokensForTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
_SWAP_EXACT_SELECTOR = "0x2a443fae"
# addLiquidity(LiquidityParameters)
_ADD_LIQUIDITY_SELECTOR = "0xa3c7271a"
# removeLiquidity(address,address,uint16,uint256,uint256,uint256[],uint256[],address,uint256)
_REMOVE_LIQUIDITY_SELECTOR = "0xc22159b6"


def _manifest_pairs_for(intent_types: list[str]) -> set[tuple[str, str]]:
    """Return the (target, selector) set produced by the manifest generator.

    Uses the same config shape the on-chain harness applies
    (``base_token``/``quote_token`` aliases so ERC-20 approvals surface).
    """
    manifest = generate_manifest(
        strategy_name="tjv2-manifest-regression",
        chain="avalanche",
        supported_protocols=["traderjoe_v2"],
        intent_types=intent_types,
        config={"base_token": "USDC", "quote_token": "WAVAX"},
    )
    return {
        (perm.target.lower(), sel.selector.lower())
        for perm in manifest.permissions
        for sel in perm.function_selectors
    }


def _compile(intent):
    """Compile ``intent`` against the same offline settings ``discover_permissions`` uses."""
    compiler = IntentCompiler(
        chain="avalanche",
        config=IntentCompilerConfig(
            allow_placeholder_prices=True,
            swap_pool_selection_mode="auto",
            fixed_swap_fee_tier=3000,
        ),
    )
    return compiler.compile(intent)


def _compiled_pairs(result) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for tx in result.transactions or []:
        if tx.data and len(tx.data) >= 10:
            pairs.add((tx.to.lower(), tx.data[:10].lower()))
    return pairs


class TestTraderJoeV2SwapCoverage:
    """SWAP on avalanche must emit + cover the LBRouter2 swap selector."""

    def test_swap_synthetic_intent_is_built(self) -> None:
        intents = build_synthetic_intents("traderjoe_v2", "SWAP", "avalanche")
        assert len(intents) == 1
        assert isinstance(intents[0], SwapIntent)

    def test_swap_compiled_selector_is_in_manifest(self) -> None:
        """The ``swapExactTokensForTokens`` selector must land on LBRouter2."""
        manifest = _manifest_pairs_for(["SWAP"])
        assert (_LBROUTER2, _SWAP_EXACT_SELECTOR) in manifest, (
            "SWAP manifest missing LBRouter2 swapExactTokensForTokens selector"
        )

    def test_swap_manifest_covers_compiled_bundle(self) -> None:
        """Every (target, selector) the compiler produces must be in the manifest."""
        intent = SwapIntent(
            from_token=_USDC,
            to_token=_WAVAX,
            amount=Decimal("1"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )
        result = _compile(intent)
        assert result.status.value == "SUCCESS", f"Compile failed: {result.error}"
        compiled = _compiled_pairs(result)
        assert compiled, "Expected at least one compiled transaction"

        manifest = _manifest_pairs_for(["SWAP"])
        missing = compiled - manifest
        assert not missing, (
            f"SWAP manifest missing {len(missing)} (target, selector) pair(s): {missing}"
        )


class TestTraderJoeV2LPOpenCoverage:
    """LP_OPEN on avalanche must cover the LBRouter2 addLiquidity selector."""

    def test_lp_open_manifest_includes_add_liquidity(self) -> None:
        manifest = _manifest_pairs_for(["LP_OPEN"])
        assert (_LBROUTER2, _ADD_LIQUIDITY_SELECTOR) in manifest, (
            "LP_OPEN manifest missing LBRouter2 addLiquidity selector"
        )

    def test_lp_only_manifest_does_not_include_swap_selector(self) -> None:
        """Regression for codex review on #1851: LP-only manifests must NOT
        carry the swap selector. ``static_permissions`` is merged for every
        manifest regardless of intent_types — leaking ``swapExactTokensForTokens``
        into an LP-only manifest would over-permission the Safe.
        """
        manifest = _manifest_pairs_for(["LP_OPEN"])
        assert (_LBROUTER2, _SWAP_EXACT_SELECTOR) not in manifest, (
            "LP-only manifest must not include swapExactTokensForTokens — "
            "static_permissions must scope swap selectors out of LP strategies."
        )

    def test_lp_open_manifest_covers_compiled_bundle(self) -> None:
        """Mirrors the case declaration in permission_cases/traderjoe_v2.py."""
        intent = LPOpenIntent(
            protocol="traderjoe_v2",
            chain="avalanche",
            pool=f"{_USDC}/{_WAVAX}",
            amount0=Decimal("100"),
            amount1=Decimal("2"),
            range_lower=Decimal("20"),
            range_upper=Decimal("60"),
        )
        result = _compile(intent)
        assert result.status.value == "SUCCESS", f"Compile failed: {result.error}"
        compiled = _compiled_pairs(result)
        assert compiled, "Expected at least one compiled transaction"

        # LP_OPEN expansion auto-adds LP_CLOSE for teardown, so static hints
        # will also add removeLiquidity — that's fine, we assert subset, not equality.
        manifest = _manifest_pairs_for(["LP_OPEN"])
        missing = compiled - manifest
        assert not missing, (
            f"LP_OPEN manifest missing {len(missing)} (target, selector) pair(s): {missing}"
        )


class TestTraderJoeV2CombinedCoverage:
    """SWAP + LP_OPEN together — the exact combination that fails on-chain."""

    @pytest.mark.parametrize(
        "selector,label",
        [
            (_SWAP_EXACT_SELECTOR, "swapExactTokensForTokens"),
            (_ADD_LIQUIDITY_SELECTOR, "addLiquidity"),
            (_REMOVE_LIQUIDITY_SELECTOR, "removeLiquidity"),
        ],
    )
    def test_lbrouter2_selector_present(self, selector: str, label: str) -> None:
        """Each LBRouter2 selector the connector emits must survive into the manifest."""
        manifest = _manifest_pairs_for(["SWAP", "LP_OPEN"])
        assert (_LBROUTER2, selector) in manifest, (
            f"Manifest missing LBRouter2 {label} selector ({selector})"
        )
