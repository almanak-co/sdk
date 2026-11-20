"""Aerodrome uses the shared authenticated gateway and durable transaction journal."""

from almanak.connectors.aerodrome import abi
from almanak.connectors.aerodrome.deployments import verify_router
from almanak.connectors.aerodrome.observations import snapshot
from almanak.connectors.aerodrome.ownership import validate_initial
from almanak.connectors.aerodrome.planning import operation
from almanak.connectors.aerodrome.reader import read_pool
from almanak.connectors.aerodrome.reconciliation import reconcile
from almanak.connectors.aerodrome.simulation import validate_simulation
from almanak.connectors.aerodrome.validation import (
    expired,
    increases_exposure,
    validate_current,
    validate_operation,
)
from almanak.connectors.funding import (
    capital_requirements,
    exchange_entry_transactions,
    funding_rows,
)
from almanak.connectors.gas_lifecycle import AERODROME_LIFECYCLE
from almanak.gateway.ethereum import DataError
from almanak.transaction_family import is_signed


class AerodromeExecution:
    snapshot = staticmethod(snapshot)
    operation = staticmethod(operation)
    validate_operation = staticmethod(validate_operation)
    validate_current = staticmethod(validate_current)
    expired = staticmethod(expired)
    validate_simulation = staticmethod(validate_simulation)
    reconcile = staticmethod(reconcile)
    increases_exposure = staticmethod(increases_exposure)

    @staticmethod
    def call(config, operation):
        if operation.target != config.target(operation.allocation):
            raise ValueError("call target differs from authoritative configuration")
        return abi.call(operation)

    @staticmethod
    def validate_replacement(config, operation):
        raise DataError(
            "replacement_unsupported", "Aerodrome replacement requires separate execution proofs"
        )

    @staticmethod
    def validate_config(config):
        for allocation in config.allocations:
            target = config.target(allocation.name)
            contracts = target.contracts.router, target.pool.pool, target.pool.factory
            if len(set(contracts)) != len(contracts) or any(
                a.reference in contracts for a in target.assets
            ):
                raise ValueError("Aerodrome contract roles and pool currencies must be distinct")

    @staticmethod
    def plan_evidence(backend, operation, call, before):
        read_pool(backend.reader, operation.target.pool, before.block)
        verify_router(
            backend.reader, operation.target.pool, operation.target.contracts, before.block
        )
        return {}

    @staticmethod
    def current_snapshot(backend, plan, block):
        """The head observation a plan is validated against before it is signed or sent.

        Signed bytes are observed as such (`observations.position`): shares sent to a
        trading fund after the signature do not hold them back.
        """
        return snapshot(
            backend, plan.operation.allocation, block, plan.before, signed=is_signed(plan)
        )

    @staticmethod
    def current_checks(backend, plan, block=None):
        block = block or backend.reader.pin_block("latest")
        current = snapshot(
            backend,
            plan.operation.allocation,
            block,
            plan.before.owned,
            quotes=False,
            signed=is_signed(plan),
        )
        validate_current(backend, plan, current)

    requirements = staticmethod(capital_requirements)
    # An entry slice approves the router for the funding currency and swaps it. A
    # liquidity allocation, after its slices, approves the router for both currencies
    # (the swap spent the funding one) and mints: 5 with a single slice.
    gas_lifecycle = AERODROME_LIFECYCLE
    entry_transactions = staticmethod(exchange_entry_transactions(gas_lifecycle))

    @staticmethod
    def preflight(config, account, observations):
        _, before = preflight_observation(config, account, observations)
        validate_initial(before)
        balances = {a.asset: a.amount for a in (before.wallet0, before.wallet1)}
        return funding_rows(capital_requirements(config, account), balances), (
            not before.needs_cleanup
        )


def preflight_observation(config, account, observations):
    allocations = [a for a in config.allocations if a.account == account]
    observed = [o for o in observations if o.target.account == account]
    if (len(allocations), len(observed)) != (1, 1):
        raise ValueError("Aerodrome preflight requires exactly its configured account allocation")
    allocation, before = allocations[0], observed[0]
    if before.target != config.target(allocation.name):
        raise ValueError("preflight observation differs from configured target")
    return allocation, before
