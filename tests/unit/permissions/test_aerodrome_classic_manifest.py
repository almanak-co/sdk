"""Offline permission-manifest regression for classic Aerodrome LP.

Classic mint/burn fail closed without a router quote, so
``discover_permissions(..., rpc_url=None)`` no longer compiles
``addLiquidity`` / ``removeLiquidity`` calldata. Static permissions on
the router must still grant those selectors or the Zodiac manifest
silently drops the mint and burn.

The LP-token approve cannot be listed statically: its target is the pool
contract (pair-specific) and is discovered when compile succeeds with RPC.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from almanak.connectors.aerodrome.adapter import ADD_LIQUIDITY_SELECTOR, REMOVE_LIQUIDITY_SELECTOR
from almanak.connectors.aerodrome.addresses import AERODROME
from almanak.framework.permissions.discovery import discover_permissions
from almanak.framework.permissions.hints import get_permission_hints

_CLASSIC_CHAINS = ("base", "optimism")


def _router_selectors(intent_types: list[str], chain: str) -> set[str]:
    permissions, _warnings = discover_permissions(
        chain=chain,
        protocols=["aerodrome"],
        intent_types=intent_types,
        rpc_url=None,
    )
    router = AERODROME[chain]["router"].lower()
    return {
        sel.selector.lower() for perm in permissions if perm.target.lower() == router for sel in perm.function_selectors
    }


class TestClassicStaticPermissions:
    @pytest.mark.parametrize("chain", _CLASSIC_CHAINS)
    def test_hints_declare_add_and_remove_on_the_router(self, chain: str) -> None:
        hints = get_permission_hints("aerodrome")
        entries = hints.static_permissions.get(chain, [])
        router = AERODROME[chain]["router"].lower()
        selectors = {sel.lower() for entry in entries if entry.target.lower() == router for sel in entry.selectors}
        assert ADD_LIQUIDITY_SELECTOR in selectors
        assert REMOVE_LIQUIDITY_SELECTOR in selectors


class TestClassicOfflineDiscovery:
    @pytest.mark.parametrize("chain", _CLASSIC_CHAINS)
    def test_lp_open_manifest_grants_add_liquidity_without_rpc(self, chain: str) -> None:
        selectors = _router_selectors(["LP_OPEN"], chain)
        assert ADD_LIQUIDITY_SELECTOR in selectors, (
            f"offline LP_OPEN on {chain} must still authorise addLiquidity "
            f"({ADD_LIQUIDITY_SELECTOR}); got {sorted(selectors)}"
        )

    @pytest.mark.parametrize("chain", _CLASSIC_CHAINS)
    def test_lp_close_manifest_grants_remove_liquidity_without_rpc(self, chain: str) -> None:
        selectors = _router_selectors(["LP_CLOSE"], chain)
        assert REMOVE_LIQUIDITY_SELECTOR in selectors, (
            f"offline LP_CLOSE on {chain} must still authorise removeLiquidity "
            f"({REMOVE_LIQUIDITY_SELECTOR}); got {sorted(selectors)}"
        )

    @pytest.mark.parametrize("chain", _CLASSIC_CHAINS)
    def test_swap_only_does_not_inherit_lp_router_selectors(self, chain: str) -> None:
        selectors = _router_selectors(["SWAP"], chain)
        assert ADD_LIQUIDITY_SELECTOR not in selectors
        assert REMOVE_LIQUIDITY_SELECTOR not in selectors

    def test_lp_open_keeps_add_liquidity_when_the_quote_is_unavailable(self) -> None:
        with patch(
            "almanak.connectors.aerodrome.adapter.AerodromeAdapter._quote_add_liquidity",
            return_value=None,
        ):
            selectors = _router_selectors(["LP_OPEN"], "base")
        assert ADD_LIQUIDITY_SELECTOR in selectors, (
            "a missing live quote must not drop addLiquidity from the offline manifest"
        )

    def test_lp_close_keeps_remove_liquidity_when_the_quote_is_unavailable(self) -> None:
        with patch(
            "almanak.connectors.aerodrome.adapter.AerodromeAdapter._quote_remove_liquidity",
            return_value=None,
        ):
            selectors = _router_selectors(["LP_CLOSE"], "base")
        assert REMOVE_LIQUIDITY_SELECTOR in selectors, (
            "a missing live quote must not drop removeLiquidity from the offline manifest"
        )
