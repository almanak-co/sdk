"""How `almanak ax lending-markets` lists Morpho Blue markets."""

from almanak.connectors.discovery import LendingDiscovery

LENDING = LendingDiscovery(listing="morpho_markets")
