"""Contract-role declarations for the Fluid connector (VIB-4928 PR-3a).

Phase 1 (VIB-5029): Fluid is SWAP-only and **routerless** — swaps execute
directly on per-pair pool contracts resolved on-chain at compile time, so
no framework role table (ROUTER / LP_POSITION_MANAGER / QUOTER / …)
applies. The previous ``LP_POSITION_MANAGER: dex_factory`` mapping was
scaffolding for an LP model Phase 0 disproved (VIB-5028 §V4) and was
removed with the LP intents.

The connector's address table (``addresses.py``) stays registered for the
registry completeness guard and later phases; it simply maps to no
framework role slots today.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRoleSpec,
)

# Vault NFT-CDP surface (Phase 3, VIB-5031, ADR §7.1). The Zodiac permission
# universe per pinned type-1 vault is exactly three rows, all produced by
# compilation-based synthetic discovery (``fluid/vault_permission_hints.py``):
#
#   1. ``operate(uint256,int256,int256,address)`` on the VAULT address
#      (``send_value`` flips on for native-collateral vaults — vault id 1
#      takes raw ETH as msg.value);
#   2. ``approve(address,uint256)`` on the collateral token, spender == vault
#      (ERC-20 legs only; amount = the supply amount, bounded);
#   3. ``approve(address,uint256)`` on the debt token, spender == vault
#      (repay pull; amount = debt_now x (1 + headroom), bounded — never
#      MAX_UINT256).
#
# NO ERC-721 surface: ``operate()`` acts on caller-owned positions; the
# factory NFT is never transferred or approved and the VaultFactory is not a
# call target. The role TABLE entry maps no framework slots — per-market
# vault targets are not router/lending-pool roles.
#
# Both protocol slugs are published from this ONE module-level tuple (the
# aerodrome precedent — the registry completeness guard requires every
# registered slug to appear in a discovered ``CONTRACT_ROLES`` attribute).
CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="fluid",
        roles={},
    ),
    ContractRoleSpec(
        protocol="fluid_vault",
        roles={},
    ),
)
