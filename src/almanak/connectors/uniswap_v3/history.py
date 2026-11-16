"""Concrete V3 revisions bound to canonical liquidity boundary evidence."""

from almanak.connectors import v3_pool_history as family
from almanak.connectors.uniswap_v3.deployments import verify_periphery, verify_pool
from almanak.connectors.uniswap_v3.models import Target
from almanak.connectors.v3_pool_history import continuous_gross as continuous_gross
from almanak.connectors.v3_pool_history import liquidity_event as liquidity_event
from almanak.connectors.v3_pool_history import liquidity_logs as liquidity_logs
from almanak.connectors.v3_pool_history import tick_gross as tick_gross


class BoundaryEvidence(family.BoundaryEvidence[Target]):
    pass


def contract_identities(reader, target, block):
    return (
        *verify_pool(reader, target.pool, block),
        *verify_periphery(reader, target.pool, target.contracts, block),
    )


def preflight(history, target, schedule, policy):
    return family.preflight(
        history,
        target,
        schedule,
        policy,
        contract_identities=contract_identities,
        evidence_type=BoundaryEvidence,
    )
