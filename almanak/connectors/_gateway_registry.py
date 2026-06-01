"""Outer-folder gateway-side connector registry.

Lives one level up from ``_base/`` because it imports every concrete
gateway-side connector class — and ``_base/`` must stay protocol-clean
(no concrete connector imports). Adding a new gateway-side connector
means one import + one ``GATEWAY_REGISTRY.register`` line in
``_register_all`` below.

Currently registered (Phase 2 — VIB-4810): ``uniswap_v4``, ``aave_v3``,
``compound_v3``, ``fluid``, ``morpho_vault``, ``pendle``, ``jupiter``,
``beefy``, ``yearn``, ``enso``, ``polymarket``.

Phase 3 (VIB-4811) registers ``uniswap_v3`` and ``aerodrome`` so the
pool-history allowlist can be derived from
``GatewayPoolHistoryCapability``. Subsequent Phase 3 commits add more
protocols as additional capabilities (DefiLlama slug, subgraph URLs,
CoinGecko IDs, DEX quotes, funding rates) come online.

Each is imported from ``almanak.connectors.<protocol>.gateway.provider``::

    from almanak.connectors.<protocol>.gateway.provider import (
        <Protocol>GatewayConnector,
    )
    GATEWAY_REGISTRY.register(<Protocol>GatewayConnector())

Strategy-side code MUST NOT import this module — enforced by
``tests/static/test_strategy_import_boundary.py``.

Future work — entry-point-based auto-discovery (deferred from VIB-4817)
======================================================================

The validation report for VIB-4808 (2026-05-26) flagged that this file
hand-wires 25 imports + 24 ``register`` calls. The spec's eventual goal
is PEP 621 ``[project.entry-points."almanak.connectors.gateway"]``
discovery, where each connector's pyproject declares::

    [project.entry-points."almanak.connectors.gateway"]
    aave_v3 = "almanak.connectors.aave_v3.gateway.provider:AaveV3GatewayConnector"

and ``_register_all`` becomes::

    for ep in importlib.metadata.entry_points(group="almanak.connectors.gateway"):
        cls = ep.load()
        GATEWAY_REGISTRY.register(cls())

Why this is deferred:

* This repository ships a single distribution (``almanak``), so 24
  entry-point declarations would land in *one* ``pyproject.toml`` —
  the UX gain over the current "one ``register`` line per connector"
  is small, and the cost (a new abstraction layer, new test surface
  for missing/duplicate entries, new failure modes for ``uv sync``-vs-
  ``pip install`` parity) is real.
* Registration order matters in places (e.g. ``GatewayPriceIdCapability``
  providers feed CoinGecko / DexScreener identifier merges in
  registration order). Python's ``entry_points()`` order is stable
  but unspecified; introducing a sort key would re-introduce the
  hand-curation the switch was meant to remove.
* The current file is already outside ``almanak/gateway/`` — the
  "ZERO gateway edits to add a connector" goal of VIB-4808 is met.

The right time to switch is when the SDK starts shipping third-party
connector packages (out-of-tree distributions that need to register
without editing this file). Open a follow-up Linear ticket if/when
that requirement materialises; the implementation is mechanical given
the above sketch.
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
    from almanak.connectors.aerodrome.gateway.provider import (
        AerodromeGatewayConnector,
    )
    from almanak.connectors.balancer_v2.gateway.provider import (
        BalancerV2GatewayConnector,
    )
    from almanak.connectors.beefy.gateway.provider import (
        BeefyGatewayConnector,
    )
    from almanak.connectors.benqi.gateway.provider import (
        BenqiGatewayConnector,
    )
    from almanak.connectors.compound_v3.gateway.provider import (
        CompoundV3GatewayConnector,
    )
    from almanak.connectors.curve.gateway.provider import (
        CurveGatewayConnector,
    )
    from almanak.connectors.enso.gateway.provider import (
        EnsoGatewayConnector,
    )
    from almanak.connectors.ethena.gateway.provider import (
        EthenaGatewayConnector,
    )
    from almanak.connectors.fluid.gateway.provider import (
        FluidGatewayConnector,
    )
    from almanak.connectors.gmx_v2.gateway.provider import (
        GmxV2GatewayConnector,
    )
    from almanak.connectors.hyperliquid.gateway.provider import (
        HyperliquidGatewayConnector,
    )
    from almanak.connectors.jupiter.gateway.provider import (
        JupiterGatewayConnector,
    )
    from almanak.connectors.lido.gateway.provider import (
        LidoGatewayConnector,
    )
    from almanak.connectors.morpho_vault.gateway.provider import (
        MorphoVaultGatewayConnector,
    )
    from almanak.connectors.orca.gateway.provider import (
        OrcaGatewayConnector,
    )
    from almanak.connectors.pancakeswap_v3.gateway.provider import (
        PancakeSwapV3GatewayConnector,
    )
    from almanak.connectors.pendle.gateway.provider import (
        PendleGatewayConnector,
    )
    from almanak.connectors.polymarket.gateway.provider import (
        PolymarketGatewayConnector,
    )
    from almanak.connectors.raydium.gateway.provider import (
        RaydiumGatewayConnector,
    )
    from almanak.connectors.sushiswap_v3.gateway.provider import (
        SushiSwapV3GatewayConnector,
    )
    from almanak.connectors.traderjoe_v2.gateway.provider import (
        TraderJoeV2GatewayConnector,
    )
    from almanak.connectors.uniswap_v3.gateway.provider import (
        UniswapV3GatewayConnector,
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
    # Phase 3 (VIB-4811) — pool history capability providers.
    GATEWAY_REGISTRY.register(UniswapV3GatewayConnector())
    GATEWAY_REGISTRY.register(AerodromeGatewayConnector())
    # Phase 3 (VIB-4811) — funding-rate capability providers.
    GATEWAY_REGISTRY.register(GmxV2GatewayConnector())
    GATEWAY_REGISTRY.register(HyperliquidGatewayConnector())
    # Phase 3 (VIB-4811) — subgraph capability providers (additional).
    GATEWAY_REGISTRY.register(BalancerV2GatewayConnector())
    # Phase 3 (VIB-4811) — price-id capability providers (CoinGecko +
    # DexScreener identifiers). These connectors are minimal Phase-3
    # scaffolds — they own only the gateway-side identifiers their
    # protocol token contributes, not the full strategy-side connector.
    GATEWAY_REGISTRY.register(LidoGatewayConnector())
    GATEWAY_REGISTRY.register(EthenaGatewayConnector())
    GATEWAY_REGISTRY.register(TraderJoeV2GatewayConnector())
    GATEWAY_REGISTRY.register(PancakeSwapV3GatewayConnector())
    GATEWAY_REGISTRY.register(RaydiumGatewayConnector())
    GATEWAY_REGISTRY.register(OrcaGatewayConnector())
    GATEWAY_REGISTRY.register(BenqiGatewayConnector())
    # Phase 3 (VIB-4811) — DEX-quote capability provider (Curve).
    # Uniswap V3 + Enso already registered above; Curve is added here
    # since it has no other capability surface yet.
    GATEWAY_REGISTRY.register(CurveGatewayConnector())
    # W1 (VIB-4853) — GatewayAddressCapability scaffolds for connectors
    # that didn't have a gateway-side provider yet. SushiSwap V3 only
    # publishes per-chain addresses; the strategy-side intent code still
    # lives in the connector folder.
    GATEWAY_REGISTRY.register(SushiSwapV3GatewayConnector())
    # Agni Finance is a Uniswap V3 fork on Mantle that reuses the V3
    # parser; the addresses live under ``connectors/uniswap_v3/addresses.py``.
    # Registering it as a distinct gateway-side connector lets non-
    # connector callers (e.g. teardown discovery, pool validation,
    # ContractRegistry) resolve Agni's addresses through the capability
    # interface instead of importing the dict by name.
    from almanak.connectors.uniswap_v3.gateway.agni_provider import (
        AgniFinanceGatewayConnector,
    )

    GATEWAY_REGISTRY.register(AgniFinanceGatewayConnector())

    # W1 (VIB-4853) — Morpho Blue + Aster Perps scaffolds. Both already
    # have strategy-side connector code under their respective folders;
    # the gateway-side scaffold exists solely to publish addresses
    # through :class:`GatewayAddressCapability`.
    from almanak.connectors.aster_perps.gateway.provider import (
        AsterPerpsGatewayConnector,
    )
    from almanak.connectors.morpho_blue.gateway.provider import (
        MorphoBlueGatewayConnector,
    )

    GATEWAY_REGISTRY.register(MorphoBlueGatewayConnector())
    GATEWAY_REGISTRY.register(AsterPerpsGatewayConnector())


_register_all()
