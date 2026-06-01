"""Strategy-side gas-estimate connector registration site (VIB-4858 / W6).

Sibling of :mod:`almanak.connectors._strategy_receipt_registry`, scoped
to the gas-estimate concern.

Lives one level up from ``_strategy_base/`` because it imports every
connector's ``gas_estimate_provider`` module — and ``_strategy_base/``
must stay protocol-clean (no concrete connector imports). Adding a new
strategy-side connector with gas estimates means one import + one
``STRATEGY_GAS_ESTIMATE_REGISTRY.register`` line below.

Each provider is imported from
``almanak.connectors.<protocol>.gas_estimate_provider``::

    from almanak.connectors.<protocol>.gas_estimate_provider import (
        <Protocol>GasEstimateConnector,
    )
    STRATEGY_GAS_ESTIMATE_REGISTRY.register(<Protocol>GasEstimateConnector())

The completeness invariant — every connector whose actions appeared in
the central ``DEFAULT_GAS_ESTIMATES`` table MUST register here — is
enforced statically by the byte-equivalence pin in
``tests/unit/intents/test_w6_gas_estimate_byte_equivalence.py``.

Why a strategy-side registry (vs. reading from ``GATEWAY_REGISTRY``)
====================================================================

Intent compilation runs inside the strategy container — the framework's
``Compiler`` builds ``TransactionData`` entries with their gas estimates
before the gateway ever sees them. Strategy-side modules are forbidden
from importing the gateway-side registry
(``almanak.connectors._gateway_registry``) per
``tests/static/test_strategy_import_boundary.py``, so the gas-estimate
dispatch cannot consume ``GATEWAY_REGISTRY``. This file is the
strategy-side mirror.

This file is allow-listed in the strategy-side import boundary scan
(``_STRATEGY_SCAN_SKIP_PARTS`` in
``tests/static/test_strategy_import_boundary.py``) the same way
``_strategy_receipt_registry.py`` is allow-listed: it is the boot-time
discovery entry point that legitimately knows every connector by name.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.gas_estimate_registry import (
    STRATEGY_GAS_ESTIMATE_REGISTRY,
)

__all__ = ["STRATEGY_GAS_ESTIMATE_REGISTRY"]


def _register_all() -> None:
    """Register every strategy-side gas-estimate connector.

    Imports are local to the function so loading this module does not
    transitively import every provider module's class until the
    registry resolves an action key. Provider modules are lightweight
    (just the integer table) but keeping the local-import shape matches
    the W2 ``_strategy_receipt_registry`` convention and isolates any
    future heavyweight provider.
    """
    # Lending — Aave V3 owns the lending_* and flash_loan* action keys.
    from almanak.connectors.aave_v3.gas_estimate_provider import (
        AaveV3GasEstimateConnector,
    )

    # Bridges — Across owns bridge_deposit.
    from almanak.connectors.across.gas_estimate_provider import (
        AcrossGasEstimateConnector,
    )

    # Flash loans — Balancer V2 owns balancer_flash_loan*.
    from almanak.connectors.balancer_v2.gas_estimate_provider import (
        BalancerV2GasEstimateConnector,
    )

    # Vaults — MetaMorpho owns vault_deposit / vault_redeem.
    from almanak.connectors.morpho_vault.gas_estimate_provider import (
        MetaMorphoGasEstimateConnector,
    )

    # DEX / AMM — Uniswap V3 owns the lp_* family (canonical CL DEX
    # estimates; UniV3 forks reuse the same numbers).
    from almanak.connectors.uniswap_v3.gas_estimate_provider import (
        UniswapV3GasEstimateConnector,
    )

    STRATEGY_GAS_ESTIMATE_REGISTRY.register(UniswapV3GasEstimateConnector())
    STRATEGY_GAS_ESTIMATE_REGISTRY.register(AaveV3GasEstimateConnector())
    STRATEGY_GAS_ESTIMATE_REGISTRY.register(BalancerV2GasEstimateConnector())
    STRATEGY_GAS_ESTIMATE_REGISTRY.register(AcrossGasEstimateConnector())
    STRATEGY_GAS_ESTIMATE_REGISTRY.register(MetaMorphoGasEstimateConnector())


_register_all()
