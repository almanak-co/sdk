"""Uniswap V3 subgraph URL registry views.

The canonical home for this endpoint set is
``Connector.dex_volume.volume_subgraph_urls`` on the uniswap_v3 manifest.
The module-level name is preserved for back-compat.

The historical ``SubgraphVolumeProvider`` and its rate-limit/exception
scaffolding were removed (ALM-2943 ph3); the live volume path is
``SubgraphClient`` in ``subgraph_client``.
"""

from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

# Derived compat views (plan 024 / VIB-4851 B1). Canonical homes are
# ``Connector.dex_volume.volume_subgraph_urls`` on the uniswap_v3 manifest.
# This module-level name is preserved for back-compat.
UNISWAP_V3_SUBGRAPHS: dict[str, str] = DexVolumeRegistry.volume_subgraph_urls_for("uniswap_v3") or {}
