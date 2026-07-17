"""Uniswap V3 subgraph URL registry views.

Canonical homes for these endpoint sets are
``Connector.dex_volume.volume_subgraph_urls`` /
``Connector.dex_volume.hosted_volume_subgraph_urls`` on the uniswap_v3
manifest. These module-level names are preserved for back-compat: the
uniswap_v3 subgraph-URL parity test imports them by name from this module.

The historical ``SubgraphVolumeProvider`` and its rate-limit/exception
scaffolding were removed (ALM-2943 ph3); the live volume path is
``SubgraphClient`` in ``subgraph_client``.
"""

from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

# Derived compat views (plan 024 / VIB-4851 B1). Canonical homes are
# ``Connector.dex_volume.volume_subgraph_urls`` /
# ``Connector.dex_volume.hosted_volume_subgraph_urls`` on the uniswap_v3
# manifest. These module-level names are preserved for back-compat (the test
# suite imports them by name from this module).
UNISWAP_V3_SUBGRAPHS: dict[str, str] = DexVolumeRegistry.volume_subgraph_urls_for("uniswap_v3") or {}
UNISWAP_V3_HOSTED_SUBGRAPHS: dict[str, str] = DexVolumeRegistry.hosted_volume_subgraph_urls_for("uniswap_v3") or {}
