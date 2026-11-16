"""Concrete sources and readers bind shared V3 fund observations."""

from almanak.connectors.aerodrome_slipstream import reader as reads
from almanak.connectors.aerodrome_slipstream.deployments import (
    require_no_discount,
    verify_periphery,
    verify_pool,
)
from almanak.connectors.aerodrome_slipstream.domain import PoolObservation
from almanak.connectors.aerodrome_slipstream.models import Target
from almanak.connectors.aerodrome_slipstream.positions import Ownership, Quote
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


def snapshot(backend, name, block, reference=None, *, quotes=True, residual=False):
    target = backend.config.target(name)
    profile = backend.config.accounts.binding(target.account).profile
    # Quoter calls use zero origin. Execution must use the same undiscounted fee context.
    verify_pool(backend.reader, target.pool, block)
    require_no_discount(backend.reader, target.pool, block, profile.submitter.address)
    return _observations.snapshot(backend, name, block, reference, quotes=quotes, residual=residual)


wallet_balances = _observations.wallet_balances
