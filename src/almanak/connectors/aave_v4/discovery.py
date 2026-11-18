"""How `almanak ax lending-markets` lists Aave V4 markets."""

from almanak.connectors.discovery import LendingDiscovery

LENDING = LendingDiscovery(listing="hub_spokes")
