"""Regression tests for the Morpho Blue permission manifest (PR #1850).

Morpho Blue is flag-routed: ``SupplyIntent.use_as_collateral`` selects between
``supplyCollateral`` (True) and ``supply`` (False); ``WithdrawIntent.is_collateral``
selects between ``withdrawCollateral`` (True) and ``withdraw`` (False). The
prior implementation worked around the single-flag-sample problem by dumping
ALL six LEND selectors (supply, supplyCollateral, withdraw, withdrawCollateral,
borrow, repay) into ``static_permissions`` — but ``discover_permissions()``
merges ``static_permissions`` regardless of the requested ``intent_types``,
which over-authorised ``borrow`` / ``repay`` on the Safe for SUPPLY-only
strategies (codex review 3135601928).

The fix: ``_build_supply_intents`` / ``_build_withdraw_intents`` emit BOTH
flag variants for morpho_blue during synthetic discovery; ``static_permissions``
on the hint is removed. These tests pin the expected surface so the
over-permissioning regression cannot reappear.
"""

from __future__ import annotations

import pytest

from almanak.framework.permissions.generator import generate_manifest

# Morpho Blue singleton selectors (see connector adapter + permission_hints.py).
_SUPPLY_SEL = "0xa99aad89"  # supply(...)
_SUPPLY_COLLATERAL_SEL = "0x238d6579"  # supplyCollateral(...)
_WITHDRAW_SEL = "0x5c2bea49"  # withdraw(...)
_WITHDRAW_COLLATERAL_SEL = "0x8720316d"  # withdrawCollateral(...)
_BORROW_SEL = "0x50d8cd4b"  # borrow(...)
_REPAY_SEL = "0x20b76e81"  # repay(...)

# Morpho Blue singleton address on ethereum (vanity: 0xBBBB...FFCb).
_MORPHO_BLUE_ETHEREUM = "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"


def _manifest_selectors(intent_types: list[str], chain: str = "ethereum") -> set[str]:
    """Return the set of selectors authorised on the Morpho Blue singleton."""
    manifest = generate_manifest(
        strategy_name="morpho-blue-manifest-regression",
        chain=chain,
        supported_protocols=["morpho_blue"],
        intent_types=intent_types,
    )
    return {
        sel.selector.lower()
        for perm in manifest.permissions
        if perm.target.lower() == _MORPHO_BLUE_ETHEREUM
        for sel in perm.function_selectors
    }


class TestMorphoBlueManifestCoverage:
    """Regression: manifest must scope selectors to the requested intent types."""

    def test_supply_only_manifest_includes_both_flag_variants(self) -> None:
        """SUPPLY-only strategies need ``supply`` AND ``supplyCollateral`` on
        the manifest — the prior single-intent synthetic sweep only exercised
        the default ``use_as_collateral=True`` and missed the loan-token path.
        """
        selectors = _manifest_selectors(["SUPPLY"])
        assert _SUPPLY_SEL in selectors, (
            "SUPPLY manifest missing loan-token supply selector "
            "(use_as_collateral=False path)"
        )
        assert _SUPPLY_COLLATERAL_SEL in selectors, (
            "SUPPLY manifest missing supplyCollateral selector "
            "(use_as_collateral=True path)"
        )

    def test_supply_only_manifest_excludes_borrow_and_repay(self) -> None:
        """Codex P1 regression: a SUPPLY-only strategy must NOT be authorised
        to ``borrow`` or ``repay`` on the Safe. ``static_permissions`` merged
        regardless of intent_types was the bug; this assertion pins the fix.
        """
        selectors = _manifest_selectors(["SUPPLY"])
        assert _BORROW_SEL not in selectors, (
            "SUPPLY-only manifest must not include borrow — "
            "static_permissions must scope selectors to intent types."
        )
        assert _REPAY_SEL not in selectors, (
            "SUPPLY-only manifest must not include repay — "
            "static_permissions must scope selectors to intent types."
        )

    def test_supply_manifest_includes_teardown_withdraw_paths(self) -> None:
        """SUPPLY auto-expands to SUPPLY+WITHDRAW for teardown. Both withdraw
        flag variants must land on the manifest so teardown can reclaim either
        loan-token liquidity or collateral.
        """
        selectors = _manifest_selectors(["SUPPLY"])
        assert _WITHDRAW_SEL in selectors
        assert _WITHDRAW_COLLATERAL_SEL in selectors

    def test_borrow_only_manifest_excludes_supply_and_withdraw_selectors(
        self,
    ) -> None:
        """A BORROW-only strategy should not be authorised to withdraw. The
        compiler still supplies collateral as part of BORROW, so ``supplyCollateral``
        is expected; but ``supply`` (loan-token) and both withdraw selectors must
        NOT leak in.
        """
        selectors = _manifest_selectors(["BORROW"])
        assert _BORROW_SEL in selectors
        assert _REPAY_SEL in selectors, (
            "BORROW auto-expands to BORROW+REPAY for teardown"
        )
        assert _SUPPLY_COLLATERAL_SEL in selectors, (
            "BORROW-only manifest must include supplyCollateral — the "
            "compiler posts collateral as part of BORROW; losing this "
            "selector silently breaks the BORROW flow."
        )
        assert _WITHDRAW_SEL not in selectors, (
            "BORROW-only manifest must not include loan-token withdraw"
        )
        assert _WITHDRAW_COLLATERAL_SEL not in selectors, (
            "BORROW-only manifest must not include withdrawCollateral"
        )
        assert _SUPPLY_SEL not in selectors, (
            "BORROW-only manifest must not include loan-token supply — "
            "only the collateral-posting step (supplyCollateral) is a legitimate "
            "side-effect of BORROW compilation."
        )

    def test_full_lend_surface_includes_all_six_selectors(self) -> None:
        """Explicit full-surface strategies get the full LEND surface."""
        selectors = _manifest_selectors(["SUPPLY", "WITHDRAW", "BORROW", "REPAY"])
        for sel in (
            _SUPPLY_SEL,
            _SUPPLY_COLLATERAL_SEL,
            _WITHDRAW_SEL,
            _WITHDRAW_COLLATERAL_SEL,
            _BORROW_SEL,
            _REPAY_SEL,
        ):
            assert sel in selectors, f"Full LEND manifest missing selector {sel}"

    @pytest.mark.parametrize(
        "chain", ["ethereum", "arbitrum", "base", "polygon", "monad"]
    )
    def test_supply_manifest_covers_both_flag_variants_on_every_chain(
        self, chain: str
    ) -> None:
        """The flag-sweep must work on every chain that has a morpho_blue
        deployment — not just ethereum. The ``_morpho_blue_synthetic_market_id``
        helper sources a valid market ID per chain from ``MORPHO_MARKETS``.
        """
        manifest = generate_manifest(
            strategy_name="morpho-blue-per-chain",
            chain=chain,
            supported_protocols=["morpho_blue"],
            intent_types=["SUPPLY"],
        )
        morpho_selectors: set[str] = set()
        for perm in manifest.permissions:
            if any(
                s.selector.lower() in {_SUPPLY_SEL, _SUPPLY_COLLATERAL_SEL}
                for s in perm.function_selectors
            ):
                morpho_selectors.update(s.selector.lower() for s in perm.function_selectors)
        assert _SUPPLY_SEL in morpho_selectors, (
            f"{chain}: SUPPLY manifest missing loan-token supply selector"
        )
        assert _SUPPLY_COLLATERAL_SEL in morpho_selectors, (
            f"{chain}: SUPPLY manifest missing supplyCollateral selector"
        )

    @pytest.mark.parametrize(
        "chain", ["ethereum", "arbitrum", "base", "polygon"]
    )
    def test_borrow_manifest_authorises_chain_specific_collateral_approve(
        self, chain: str
    ) -> None:
        """The BORROW manifest must authorise an ERC-20 ``approve`` on the
        chain's actual market collateral, not on the chain-default WETH.

        Regression for #1904: ``_build_borrow_intents`` previously hardcoded
        ``collateral_token=weth``, which discovered approve(WETH) on chains
        where the market collateral is wstETH (arbitrum/base) or WBTC
        (polygon). The Zodiac role then rejected the test's actual approve
        call and every BORROW/REPAY/WITHDRAW-collateral test on those chains
        failed with AuthorizationFailed.
        """
        from almanak.connectors.morpho_blue.adapter import MORPHO_MARKETS

        chain_markets = MORPHO_MARKETS.get(chain, {})
        if not chain_markets:
            pytest.skip(f"{chain}: no morpho_blue markets registered")
        first_market = next(iter(chain_markets.values()))
        expected_collateral = first_market["collateral_token_address"].lower()

        manifest = generate_manifest(
            strategy_name="morpho-blue-borrow-collateral-regression",
            chain=chain,
            supported_protocols=["morpho_blue"],
            intent_types=["BORROW"],
        )
        approve_targets = {
            perm.target.lower()
            for perm in manifest.permissions
            for sel in perm.function_selectors
            if sel.selector.lower() == "0x095ea7b3"  # ERC-20 approve(address,uint256)
        }
        assert expected_collateral in approve_targets, (
            f"{chain}: BORROW manifest missing approve permission for the market "
            f"collateral token {expected_collateral}. Got approve targets: "
            f"{sorted(approve_targets)}"
        )
