"""Outer-folder gateway-side connector registry.

Lives one level up from ``_base/`` because it imports every concrete
gateway-side connector class — and ``_base/`` must stay protocol-clean
(no concrete connector imports). Adding a new gateway-side connector
means one import + one ``GATEWAY_REGISTRY.register`` line in
``_register_all`` below.

Currently registered (Phase 2 — VIB-4810): ``uniswap_v4``, ``aave_v3``,
``compound_v3``, ``fluid``, ``morpho_vault``, ``pendle``, ``jupiter``,
``beefy``, ``yearn``, ``enso``, ``polymarket``. Each is imported from
``almanak.connectors.<protocol>.gateway.provider``::

    from almanak.connectors.<protocol>.gateway.provider import (
        <Protocol>GatewayConnector,
    )
    GATEWAY_REGISTRY.register(<Protocol>GatewayConnector())

Strategy-side code MUST NOT import this module — enforced by
``tests/static/test_strategy_import_boundary.py``.
"""

from __future__ import annotations

from ._base.gateway_registry import GatewayConnectorRegistry

__all__ = ["GATEWAY_REGISTRY"]


GATEWAY_REGISTRY: GatewayConnectorRegistry = GatewayConnectorRegistry()


def _register_all() -> None:
    """Register every gateway-side connector with ``GATEWAY_REGISTRY``.

    Phase 2 (VIB-4810) — one ``GATEWAY_REGISTRY.register(...)`` line per
    migrated connector. Imports are local to the function so that loading
    the registry module does not transitively import the gateway-only
    machinery (gRPC servicers, web3 RPC providers, …) until the gateway
    boot path actually needs them.
    """
    from almanak.connectors.aave_v3.gateway.provider import (
        AaveV3GatewayConnector,
    )
    from almanak.connectors.beefy.gateway.provider import (
        BeefyGatewayConnector,
    )
    from almanak.connectors.compound_v3.gateway.provider import (
        CompoundV3GatewayConnector,
    )
    from almanak.connectors.enso.gateway.provider import (
        EnsoGatewayConnector,
    )
    from almanak.connectors.fluid.gateway.provider import (
        FluidGatewayConnector,
    )
    from almanak.connectors.jupiter.gateway.provider import (
        JupiterGatewayConnector,
    )
    from almanak.connectors.morpho_vault.gateway.provider import (
        MorphoVaultGatewayConnector,
    )
    from almanak.connectors.pendle.gateway.provider import (
        PendleGatewayConnector,
    )
    from almanak.connectors.polymarket.gateway.provider import (
        PolymarketGatewayConnector,
    )
    from almanak.connectors.uniswap_v4.gateway.provider import (
        UniswapV4GatewayConnector,
    )
    from almanak.connectors.yearn.gateway.provider import (
        YearnGatewayConnector,
    )

    GATEWAY_REGISTRY.register(UniswapV4GatewayConnector())
    GATEWAY_REGISTRY.register(AaveV3GatewayConnector())
    GATEWAY_REGISTRY.register(CompoundV3GatewayConnector())
    GATEWAY_REGISTRY.register(FluidGatewayConnector())
    GATEWAY_REGISTRY.register(MorphoVaultGatewayConnector())
    GATEWAY_REGISTRY.register(PendleGatewayConnector())
    GATEWAY_REGISTRY.register(JupiterGatewayConnector())
    GATEWAY_REGISTRY.register(BeefyGatewayConnector())
    GATEWAY_REGISTRY.register(YearnGatewayConnector())
    GATEWAY_REGISTRY.register(EnsoGatewayConnector())
    GATEWAY_REGISTRY.register(PolymarketGatewayConnector())


_register_all()
