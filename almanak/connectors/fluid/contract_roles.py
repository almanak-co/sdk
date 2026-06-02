"""Contract-role declarations for the Fluid connector (VIB-4928 PR-3a).

Fluid surfaces its LP position-manager slot from the ``dex_factory`` kind. See
:mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="fluid",
        roles={ContractRole.LP_POSITION_MANAGER: ("dex_factory",)},
    ),
)
