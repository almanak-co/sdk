"""How `almanak ax lending-markets` lists SparkLend markets."""

from almanak.connectors.discovery import LendingDiscovery

LENDING = LendingDiscovery(listing="pool_reserves")
