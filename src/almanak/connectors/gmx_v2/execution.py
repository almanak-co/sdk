"""GMX protocol preparation uses the shared account, simulation, signing and receipt path."""

from almanak.connectors.funding import funding_rows, lifecycle_entries, summed
from almanak.connectors.gas_lifecycle import MINIMUM_EXIT, GasLifecycle, KindCounts
from almanak.connectors.gmx_v2 import abi, planning
from almanak.connectors.gmx_v2.deployments import records, verify_deployments
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.exchange_models import ExchangeAllocation, input_assets
from almanak.connectors.gmx_v2.exchange_operations import (
    CancelExchangeOrder,
    CreateSwap,
    ExchangeApproval,
)
from almanak.connectors.gmx_v2.lifecycle import Lifecycle, validate_position
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation
from almanak.connectors.gmx_v2.liquidity_operations import CancelLiquidityRequest, CreateDeposit
from almanak.connectors.gmx_v2.observations import snapshot
from almanak.connectors.gmx_v2.operations import Approval, CancelOrder, ClaimFunding, CreateOrder
from almanak.connectors.gmx_v2.reconciliation import reconcile
from almanak.gateway.ethereum import DataError


def validate_operation(backend, state, operation, before):
    if state.decision is None or before.target != operation.target:
        raise ValueError(
            "GMX operation requires its retained decision and exact observation target"
        )
    owned = next(o for o in state.owned if o.target == operation.target)
    if owned != before.owned:
        raise DataError("ownership", "GMX preparation differs from retained aggregate ownership")
    validate_position(owned, before)
    if planning.operation(state.config, state, state.decision.intent, before) != operation:
        raise ValueError("GMX operation differs from its independently planned economic request")
    if Lifecycle.increases_exposure(state.decision.intent) and not isinstance(
        operation, (CancelOrder, CancelExchangeOrder, CancelLiquidityRequest)
    ):
        validate_entry(state, before, operation)


def validate_entry(state, before, operation):
    if isinstance(operation, (Approval, ExchangeApproval)) and operation.amount.amount == 0:
        return
    if state.mode != "running" or any(o.accounting_issues for o in state.outcomes):
        raise DataError("accounting", "new GMX exposure requires a running reconciled deployment")
    minimum = state.config.allocation(operation.allocation).minimum_native_balance
    if before.native_balance.amount < minimum:
        raise DataError("gas_budget", "GMX submitter lacks the configured entry reserve")


def validate_current(backend, plan, current):
    if isinstance(plan.operation.target, ExchangeTarget):
        from almanak.connectors.gmx_v2.exchange_validation import (
            validate_current as validate_exchange_current,
        )

        validate_exchange_current(plan, current)
        return
    validate_current_cancellation(plan, current)
    validate_current_funding(plan, current)
    validate_current_position(plan, current)
    validate_current_inventory(current)


def validate_current_cancellation(plan, current):
    if isinstance(plan.operation, CancelOrder) and (
        current.pending_order is None or not current.pending_order.cancellable
    ):
        raise DataError("stale_plan", "GMX cancellation request settled or became ineligible")


def validate_current_funding(plan, current):
    if (
        isinstance(plan.operation, ClaimFunding)
        and current.claimable_funding != plan.before.claimable_funding
    ):
        raise DataError(
            "precondition_changed", "GMX funding entitlements changed after preparation"
        )


def validate_current_position(plan, current):
    if current.position != plan.before.owned.position:
        raise DataError("precondition_changed", "GMX aggregate position changed after preparation")
    if current.allowance != plan.before.allowance:
        raise DataError("precondition_changed", "GMX input allowance changed after preparation")


def validate_current_inventory(current):
    balances = {a.asset: a.amount for a in current.wallets}
    if any(balances[a.asset] < a.amount for a in current.owned.inventory):
        raise DataError("inventory", "GMX fund wallet no longer covers retained inventory")


def requirements(config, account):
    """Owned entry inventory; validation keeps the wrapped-native request fee inside it."""
    allocations = account_allocations(config, account)
    positions = (a.capital for a in allocations if not isinstance(a, ExchangeAllocation))
    exchanges = (
        value
        for a in allocations
        if isinstance(a, ExchangeAllocation)
        for value in a.initial_inventory.root
    )
    return (
        *summed(positions, "configured position capital including the request fee"),
        *summed(exchanges, "initial exchange inventory including the request fee"),
    )


# Each escrowed input asset's approval, then the request. A position approves its
# collateral, which carries the request fee, and creates the increase order. A swap
# approves the swapped currency and the wrapped-native fee and creates the request;
# a deposit approves each distinct input asset (both market currencies and the fee).
# A position exits by approving the decrease order's wrapped-native fee, creating the
# close order and, once closed, claiming its accrued funding (planning.cleanup); the
# keeper's execution is not the payer's transaction (a fork replays it as the keeper).
GAS_LIFECYCLE = GasLifecycle(
    kinds={
        "position": KindCounts(entry=2, exit=3),
        "swap": KindCounts(entry=3, exit=MINIMUM_EXIT),
        "liquidity": KindCounts(entry=1, exit=MINIMUM_EXIT, per_item={"input_assets": 1}),
    }
)


def entry_kind(allocation):
    if isinstance(allocation, LiquidityAllocation):
        return "liquidity", {"items": {"input_assets": len(input_assets(allocation.market))}}
    return ("swap" if isinstance(allocation, ExchangeAllocation) else "position"), {}


entry_transactions = lifecycle_entries(GAS_LIFECYCLE, entry_kind)


def preflight(config, account, observations):
    _, before = preflight_inputs(config, account, observations)
    balances = {value.asset: value.amount for value in before.wallets}
    tokens = funding_rows(requirements(config, account), balances)
    if isinstance(before.target, ExchangeTarget):
        return tokens, before.market_tokens.amount == 0 and not before.needs_cleanup
    return tokens, before.position is None and not before.needs_cleanup


def preflight_inputs(config, account, observations):
    allocations = account_allocations(config, account)
    observed = account_observations(observations, account)
    if len(allocations) != 1 or len(observed) != 1:
        raise ValueError("GMX preflight requires its exact configured fund allocation")
    allocation, before = allocations[0], observed[0]
    if before.target != config.target(allocation.name):
        raise ValueError("GMX preflight target differs from authoritative configuration")
    return allocation, before


def account_allocations(config, account):
    return tuple(a for a in config.allocations if a.account == account)


def account_observations(observations, account):
    return tuple(o for o in observations if o.target.account == account)


class Execution:
    snapshot = staticmethod(snapshot)
    operation = staticmethod(planning.operation)
    validate_operation = staticmethod(validate_operation)
    validate_current = staticmethod(validate_current)
    validate_simulation = staticmethod(abi.validate_simulation)
    reconcile = staticmethod(reconcile)
    preflight = staticmethod(preflight)
    requirements = staticmethod(requirements)
    gas_lifecycle = GAS_LIFECYCLE
    entry_transactions = staticmethod(entry_transactions)

    @staticmethod
    def call(config, operation):
        if operation.target != config.target(operation.allocation):
            raise ValueError("GMX protocol call differs from authoritative configuration")
        return abi.call(operation)

    @staticmethod
    def validate_replacement(config, operation):
        raise DataError(
            "replacement_unsupported",
            "GMX fee replacement requires separate request recovery proofs",
        )

    @staticmethod
    def validate_config(config):
        for allocation in config.allocations:
            records(allocation.market)

    @staticmethod
    def plan_evidence(backend, operation, call, before):
        verify_deployments(backend.reader, operation.target.market, before.block)
        return {}

    @staticmethod
    def current_checks(backend, plan, block=None):
        block = block or backend.reader.pin_block("latest")
        current = snapshot(backend, plan.operation.allocation, block, plan.before)
        validate_current(backend, plan, current)

    @staticmethod
    def increases_exposure(config, operation):
        if isinstance(operation, CreateDeposit):
            return True
        if isinstance(operation, CreateSwap):
            return operation.minimum_output.asset != operation.target.funding_asset
        if isinstance(operation, ExchangeApproval):
            return operation.amount.amount > 0
        if isinstance(operation, CreateOrder):
            return operation.order_type == 2
        return isinstance(operation, Approval) and operation.amount.amount > 0
