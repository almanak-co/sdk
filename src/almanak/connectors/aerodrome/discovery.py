"""How `almanak ax` discovers Aerodrome classic pools."""

from almanak.connectors.discovery import PoolDiscovery

POOL = PoolDiscovery(
    "resolved",
    vendor_labels=("aerodrome-base",),
    answers_factory=True,
    prices="almanak.connectors.resolved_pool_prices:aerodrome_prices",
)
