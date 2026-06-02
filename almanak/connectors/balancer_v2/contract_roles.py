"""Contract-role declarations for the Balancer V2 connector (VIB-4928 PR-3a).

Balancer V2 contributes the flash-loan ``Vault`` address (a CREATE2
deterministic deployment shared across chains). Backs the flat
``BALANCER_VAULT_ADDRESSES`` table. See
:mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="balancer_v2",
        roles={ContractRole.FLASH_LOAN_VAULT: ("vault",)},
    ),
)
