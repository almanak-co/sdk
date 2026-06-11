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

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="fluid",
        roles={},
    ),
)
