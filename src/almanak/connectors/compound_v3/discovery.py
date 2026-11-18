"""How `almanak ax lending-markets` lists Compound V3 markets."""

from almanak.connectors.discovery import LendingDiscovery

LENDING = LendingDiscovery(listing="comet_deployments")
