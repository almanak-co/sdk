"""How `almanak ax` discovers Aerodrome Slipstream pools."""

from almanak.connectors.discovery import PoolDiscovery

POOL = PoolDiscovery("concentrated", vendor_labels=("aerodrome-slipstream",), answers_factory=True)
