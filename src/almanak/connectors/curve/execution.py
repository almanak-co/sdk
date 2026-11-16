"""Curve uses the shared authenticated gateway and durable transaction journal."""

from almanak.connectors.curve import abi
from almanak.connectors.curve.observations import snapshot
from almanak.connectors.curve.ownership import validate_initial
from almanak.connectors.curve.planning import operation
from almanak.connectors.curve.reader import read_pool
from almanak.connectors.curve.reconciliation import reconcile
from almanak.connectors.curve.simulation import validate_simulation
from almanak.connectors.curve.validation import (
    increases_exposure,
    validate_current,
    validate_operation,
)
from almanak.connectors.funding import exchange_entry_transactions, funding_rows, summed
from almanak.connectors.gas_lifecycle import CURVE_LIFECYCLE
from almanak.gateway.ethereum import DataError


class CurveExecution:
    snapshot = staticmethod(snapshot)
    operation = staticmethod(operation)
    validate_operation = staticmethod(validate_operation)
    validate_current = staticmethod(validate_current)
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
            "replacement_unsupported", "Curve replacement requires separate execution proofs"
        )

    @staticmethod
    def validate_config(config):
        for allocation in config.allocations:
            target = config.target(allocation.name)
            contracts = target.pool.pool, target.pool.factory
            if len(set(contracts)) != len(contracts) or any(
                a.reference in contracts for a in target.assets
            ):
                raise ValueError("Curve contract roles and pool currencies must be distinct")

    @staticmethod
    def plan_evidence(backend, operation, call, before):
        read_pool(backend.reader, operation.target.pool, before.block)
        return {}

    @staticmethod
    def current_checks(backend, plan, block=None):
        block = block or backend.reader.pin_block("latest")
        current = snapshot(
            backend, plan.operation.allocation, block, plan.before.owned, quotes=False
        )
        validate_current(backend, plan, current)

    # An entry slice approves the pool for the funding asset and swaps it. A liquidity
    # allocation, after its slices, approves the pool for every coin (the swap spent
    # the funding one) and adds liquidity: 3 plus the coins with a single slice.
    gas_lifecycle = CURVE_LIFECYCLE
    entry_transactions = staticmethod(
        exchange_entry_transactions(
            gas_lifecycle,
            items=lambda allocation: {"coins": len(allocation.pool.coins)},
            capital=lambda allocation: next(
                (
                    value.amount
                    for value in allocation.capital.root
                    if value.asset == allocation.funding_asset
                ),
                0,
            ),
        )
    )

    @staticmethod
    def requirements(config, account):
        """Every coin amount the account's pool allocations deposit on entry."""
        return summed(
            (
                value
                for allocation in config.allocations
                if allocation.account == account
                for value in allocation.capital.root
            ),
            "configured pool deposit capital",
        )

    @staticmethod
    def preflight(config, account, observations):
        _, before = preflight_observation(config, account, observations)
        validate_initial(before)
        balances = {a.asset: a.amount for a in before.wallets.root}
        tokens = funding_rows(CurveExecution.requirements(config, account), balances)
        return tokens, not before.needs_cleanup


def preflight_observation(config, account, observations):
    allocations = [a for a in config.allocations if a.account == account]
    observed = [o for o in observations if o.target.account == account]
    if (len(allocations), len(observed)) != (1, 1):
        raise ValueError("Curve preflight requires exactly its configured account allocation")
    allocation, before = allocations[0], observed[0]
    if before.target != config.target(allocation.name):
        raise ValueError("preflight observation differs from configured target")
    return allocation, before
