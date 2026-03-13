"""Tests for compilation-based permission discovery."""

import pytest

from almanak.framework.intents.compiler import (
    AAVE_BORROW_SELECTOR,
    AAVE_SUPPLY_SELECTOR,
    AAVE_WITHDRAW_SELECTOR,
    ERC20_APPROVE_SELECTOR,
    LENDING_POOL_ADDRESSES,
    NFT_POSITION_MINT_SELECTOR,
    PROTOCOL_ROUTERS,
)
from almanak.framework.permissions.discovery import discover_permissions


class TestDiscoverSwapPermissions:
    """Test permission discovery for SWAP intents."""

    def test_uniswap_v3_swap_arbitrum(self):
        """Uniswap V3 swap on arbitrum should produce router permissions."""
        permissions, warnings = discover_permissions(
            chain="arbitrum",
            protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        # Should have at least the swap router + token approve
        assert len(permissions) >= 1
        # Find the swap router permission
        router_addr = PROTOCOL_ROUTERS["arbitrum"]["uniswap_v3"].lower()
        router_perms = [p for p in permissions if p.target == router_addr]
        assert len(router_perms) == 1, f"Expected router permission for {router_addr}"
        router = router_perms[0]
        # Should have at least one function selector
        assert len(router.function_selectors) >= 1
        selectors = {s.selector for s in router.function_selectors}
        # swap selector should be present (exactInputSingle or similar)
        assert len(selectors) >= 1

    def test_uniswap_v3_swap_base(self):
        """Uniswap V3 swap on base uses a different router address."""
        permissions, _ = discover_permissions(
            chain="base",
            protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        router_addr = PROTOCOL_ROUTERS["base"]["uniswap_v3"].lower()
        router_perms = [p for p in permissions if p.target == router_addr]
        assert len(router_perms) == 1

    def test_swap_produces_approve_permission(self):
        """Swap compilation should include an ERC-20 approve transaction."""
        permissions, _ = discover_permissions(
            chain="arbitrum",
            protocols=["uniswap_v3"],
            intent_types=["SWAP"],
        )
        # Find permissions with approve selector
        approve_perms = [
            p for p in permissions
            if any(s.selector == ERC20_APPROVE_SELECTOR for s in p.function_selectors)
        ]
        assert len(approve_perms) >= 1, "Should have approve permission for input token"

    def test_unsupported_protocol_chain_combo(self):
        """Protocol not deployed on chain should produce empty permissions."""
        # Aerodrome is only on base, not arbitrum
        permissions, _ = discover_permissions(
            chain="arbitrum",
            protocols=["aerodrome"],
            intent_types=["SWAP"],
        )
        # Should produce no permissions (aerodrome has no router on arbitrum)
        assert permissions == []


class TestDiscoverLPPermissions:
    """Test permission discovery for LP intents."""

    def test_lp_open_produces_mint_selector(self):
        """LP_OPEN should produce position manager with mint selector."""
        permissions, _ = discover_permissions(
            chain="arbitrum",
            protocols=["uniswap_v3"],
            intent_types=["LP_OPEN"],
        )
        # Find permission with mint selector
        mint_perms = [
            p for p in permissions
            if any(s.selector == NFT_POSITION_MINT_SELECTOR for s in p.function_selectors)
        ]
        assert len(mint_perms) >= 1, "Should have mint permission for LP_OPEN"


class TestDiscoverLendingPermissions:
    """Test permission discovery for lending intents."""

    def test_aave_v3_supply_arbitrum(self):
        """Aave V3 supply on arbitrum should produce pool permissions."""
        permissions, _ = discover_permissions(
            chain="arbitrum",
            protocols=["aave_v3"],
            intent_types=["SUPPLY"],
        )
        pool_addr = LENDING_POOL_ADDRESSES["arbitrum"]["aave_v3"].lower()
        pool_perms = [p for p in permissions if p.target == pool_addr]
        assert len(pool_perms) == 1, f"Expected pool permission for {pool_addr}"
        selectors = {s.selector for s in pool_perms[0].function_selectors}
        assert AAVE_SUPPLY_SELECTOR in selectors

    def test_aave_v3_borrow_includes_supply_and_borrow(self):
        """Borrow intent compiles both supply collateral and borrow."""
        permissions, _ = discover_permissions(
            chain="arbitrum",
            protocols=["aave_v3"],
            intent_types=["BORROW"],
        )
        pool_addr = LENDING_POOL_ADDRESSES["arbitrum"]["aave_v3"].lower()
        pool_perms = [p for p in permissions if p.target == pool_addr]
        assert len(pool_perms) == 1
        selectors = {s.selector for s in pool_perms[0].function_selectors}
        assert AAVE_SUPPLY_SELECTOR in selectors, "Borrow intent supplies collateral first"
        assert AAVE_BORROW_SELECTOR in selectors

    def test_aave_v3_withdraw(self):
        """Withdraw intent produces withdraw selector."""
        permissions, _ = discover_permissions(
            chain="arbitrum",
            protocols=["aave_v3"],
            intent_types=["WITHDRAW"],
        )
        pool_addr = LENDING_POOL_ADDRESSES["arbitrum"]["aave_v3"].lower()
        pool_perms = [p for p in permissions if p.target == pool_addr]
        assert len(pool_perms) == 1
        selectors = {s.selector for s in pool_perms[0].function_selectors}
        assert AAVE_WITHDRAW_SELECTOR in selectors


class TestPermissionMerging:
    """Test that permissions from multiple intent types merge correctly."""

    def test_merge_same_target_different_selectors(self):
        """Multiple intent types hitting same contract should merge selectors."""
        permissions, _ = discover_permissions(
            chain="arbitrum",
            protocols=["aave_v3"],
            intent_types=["SUPPLY", "WITHDRAW", "BORROW"],
        )
        pool_addr = LENDING_POOL_ADDRESSES["arbitrum"]["aave_v3"].lower()
        pool_perms = [p for p in permissions if p.target == pool_addr]
        assert len(pool_perms) == 1, "Same pool address should merge into one permission"
        selectors = {s.selector for s in pool_perms[0].function_selectors}
        assert AAVE_SUPPLY_SELECTOR in selectors
        assert AAVE_WITHDRAW_SELECTOR in selectors
        assert AAVE_BORROW_SELECTOR in selectors


class TestWarnings:
    """Test warning generation for unsupported combinations."""

    def test_unknown_protocol_no_crash(self):
        """Unknown protocol should not crash, just produce no permissions."""
        permissions, warnings = discover_permissions(
            chain="arbitrum",
            protocols=["nonexistent_protocol"],
            intent_types=["SWAP"],
        )
        # Should not crash, may or may not have warnings
        assert isinstance(permissions, list)
        assert isinstance(warnings, list)

    def test_unknown_intent_type_no_crash(self):
        """Unknown intent type should not crash."""
        permissions, warnings = discover_permissions(
            chain="arbitrum",
            protocols=["uniswap_v3"],
            intent_types=["NONEXISTENT_TYPE"],
        )
        assert isinstance(permissions, list)
        assert isinstance(warnings, list)
