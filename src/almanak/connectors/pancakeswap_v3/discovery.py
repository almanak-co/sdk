"""How `almanak ax` discovers PancakeSwap V3 pools."""

from almanak.connectors.discovery import PoolDiscovery

POOL = PoolDiscovery("concentrated", vendor_labels=("pancakeswap-v3",), answers_factory=True)
