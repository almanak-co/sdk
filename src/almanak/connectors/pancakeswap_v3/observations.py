"""Concrete sources and readers bind shared V3 fund observations."""

from almanak.connectors.pancakeswap_v3 import reader as reads
from almanak.connectors.pancakeswap_v3.deployments import verify_periphery
from almanak.connectors.pancakeswap_v3.domain import PoolObservation
from almanak.connectors.pancakeswap_v3.models import Target
from almanak.connectors.pancakeswap_v3.positions import Ownership, Quote
from almanak.connectors.v3_pool_observations import Observations
from almanak.connectors.v3_pool_observations import Snapshot as FamilySnapshot
from almanak.connectors.v3_pool_observations import quotes_for as quotes_for


class Snapshot(FamilySnapshot[Target, PoolObservation, Ownership, Quote]):
    pass


_observations = Observations(
    snapshot_type=Snapshot,
    ownership_type=Ownership,
    quote_type=Quote,
    reads=reads,
    verify_periphery=verify_periphery,
)
initial_owned = _observations.initial_owned
ownership = _observations.ownership
allowances = _observations.allowances
decision_quotes = _observations.decision_quotes
measured_quote = _observations.measured_quote
snapshot = _observations.snapshot
wallet_balances = _observations.wallet_balances
