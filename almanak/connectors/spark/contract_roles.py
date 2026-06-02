"""Contract-role declarations for the Spark connector (VIB-4928 PR-3a).

Spark declares ``LENDING_POOL`` only — **deliberately no
``LENDING_DATA_PROVIDER``**. The legacy central ``LENDING_POOL_DATA_PROVIDERS``
dict only ever carried ``aave_v3`` entries for the lending pre-flight; PR-3a
preserves that exact surface (Spark owns its own ``pool_data_provider`` address
in ``addresses.SPARK``, but widening the pre-flight to Spark is a separate
decision, not part of this pure-inversion refactor). See
``compiler_constants._build_lending_pool_data_providers`` and the pin in
``tests/unit/intents/test_contract_role_registry_equivalence.py``
(``test_spark_lending_data_provider_omitted``). See
:mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="spark",
        roles={ContractRole.LENDING_POOL: ("pool",)},
    ),
)
