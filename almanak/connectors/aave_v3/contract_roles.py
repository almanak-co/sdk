"""Contract-role declarations for the Aave V3 connector (VIB-4928 PR-3a).

See :mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="aave_v3",
        roles={
            ContractRole.LENDING_POOL: ("pool",),
            ContractRole.LENDING_DATA_PROVIDER: ("pool_data_provider",),
        },
    ),
)
