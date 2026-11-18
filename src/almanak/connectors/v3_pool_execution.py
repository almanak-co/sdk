"""V3 protocol operations use the shared gateway transaction and recovery path."""

from collections.abc import Callable
from typing import ClassVar

from almanak.connectors.funding import (
    capital_requirements,
    exchange_entry_transactions,
    funding_rows,
)
from almanak.connectors.gas_lifecycle import V3_POOL_LIFECYCLE
from almanak.connectors.v3_pool_validation import expired
from almanak.gateway.ethereum import DataError


class Execution:
    snapshot: ClassVar[Callable]
    validate_current: ClassVar[Callable]
    encode_call: ClassVar[Callable]
    verify_pool: ClassVar[Callable]
    verify_periphery: ClassVar[Callable]
    expired = staticmethod(expired)

    @classmethod
    def call(cls, config, operation):
        if operation.target != config.target(operation.allocation):
            raise ValueError("call target differs from authoritative configuration")
        return cls.encode_call(operation)

    @staticmethod
    def validate_replacement(config, operation):
        raise DataError(
            "replacement_unsupported", "V3 fee replacement requires separate execution proofs"
        )

    @staticmethod
    def validate_config(config):
        for allocation in config.allocations:
            target = config.target(allocation.name)
            contracts = (
                *target.contracts.model_dump().values(),
                target.pool.pool,
                target.pool.factory,
            )
            if len(set(contracts)) != len(contracts) or any(
                a.reference in contracts for a in target.assets
            ):
                raise ValueError("V3 contract roles and pool currencies must be distinct")

    @classmethod
    def plan_evidence(cls, backend, operation, call, before):
        target = operation.target
        cls.verify_pool(backend.reader, target.pool, before.block)
        cls.verify_periphery(backend.reader, target.pool, target.contracts, before.block)
        return {}

    @classmethod
    def current_checks(cls, backend, plan, block=None):
        block = block or backend.reader.pin_block("latest")
        current = cls.snapshot(
            backend, plan.operation.allocation, block, plan.before.owned, quotes=False
        )
        cls.validate_current(backend, plan, current)

    requirements = staticmethod(capital_requirements)
    # An entry slice approves the router for the funding currency and swaps it. A
    # liquidity allocation, after its slices, approves the position manager for both
    # currencies and mints: 5 with a single slice.
    gas_lifecycle = V3_POOL_LIFECYCLE
    entry_transactions = staticmethod(exchange_entry_transactions(V3_POOL_LIFECYCLE))

    @staticmethod
    def preflight(config, account, observations):
        _, before = preflight_position(config, account, observations)
        balances = {a.asset: a.amount for a in (before.wallet0, before.wallet1)}
        tokens = funding_rows(capital_requirements(config, account), balances)
        return tokens, not before.needs_cleanup and before.position is None


def preflight_position(config, account, observations):
    allocations = [a for a in config.allocations if a.account == account]
    observed = [o for o in observations if o.target.account == account]
    return validate_preflight_position(config, allocations, observed)


def validate_preflight_position(config, allocations, observed):
    if (len(allocations), len(observed)) != (1, 1):
        raise ValueError("V3 preflight requires its exact configured account allocation")
    allocation, before = allocations[0], observed[0]
    if before.target != config.target(allocation.name):
        raise ValueError("preflight observation differs from configured target")
    return allocation, before
