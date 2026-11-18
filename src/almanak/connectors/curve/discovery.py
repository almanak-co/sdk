"""How `almanak ax` discovers Curve pools."""

from almanak.connectors.discovery import PoolDiscovery

POOL = PoolDiscovery(
    "resolved",
    vendor_labels=("curve",),
    currencies="coins",
    prices="almanak.connectors.resolved_pool_prices:curve_report",
    funding="almanak.connectors.curve.pool_report:unfunded_report",
)
