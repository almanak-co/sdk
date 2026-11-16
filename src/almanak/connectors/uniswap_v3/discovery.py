"""How `almanak ax` discovers Uniswap V3 pools."""

from almanak.connectors.discovery import PoolDiscovery

POOL = PoolDiscovery(
    "concentrated", vendor_labels=("uniswap_v3", "uniswap-v3"), answers_factory=True
)
