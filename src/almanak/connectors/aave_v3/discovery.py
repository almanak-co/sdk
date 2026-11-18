"""How `almanak ax lending-markets` lists Aave V3 markets."""

from almanak.connectors.discovery import LendingDiscovery

LENDING = LendingDiscovery(listing="pool_reserves")
