"""Typed protocol dispatch within the same gateway, signer and submission lifecycle."""

from copy import copy

from almanak.connectors.aave_v3.operations import RepayOperation, SupplyOperation
from almanak.connectors.aave_v4.operations import RepayOperation as AaveV4Repay
from almanak.connectors.aave_v4.operations import SupplyOperation as AaveV4Supply
from almanak.connectors.aerodrome.operations import Addition as AerodromeAddition
from almanak.connectors.aerodrome.operations import Swap as AerodromeSwap
from almanak.connectors.aerodrome_slipstream.operations import Addition as SlipstreamAddition
from almanak.connectors.aerodrome_slipstream.operations import Swap as SlipstreamSwap
from almanak.connectors.composed.inventory import cash_evidence
from almanak.connectors.composed.lifecycle import component_state, local
from almanak.connectors.composed.models import (
    component_for_allocation,
    components_for_account,
    wallet_key,
)
from almanak.connectors.composition import connector_for
from almanak.connectors.compound_v3.operations import (
    RepayOperation as CompoundRepay,
)
from almanak.connectors.compound_v3.operations import (
    SupplyOperation as CompoundSupply,
)
from almanak.connectors.curve.operations import Addition as CurveAddition
from almanak.connectors.curve.operations import Swap as CurveSwap
from almanak.connectors.funding import funding_rows, summed
from almanak.connectors.gas_lifecycle import GasLifecycle
from almanak.connectors.gmx_v2.abi import input_amount as gmx_order_input
from almanak.connectors.gmx_v2.exchange_operations import CreateSwap as GmxSwap
from almanak.connectors.gmx_v2.liquidity_operations import CreateLiquidity as GmxLiquidity
from almanak.connectors.gmx_v2.operations import CreateOrder as GmxOrder
from almanak.connectors.morpho_blue.capacity import funding_requirement as morpho_funding
from almanak.connectors.morpho_blue.operations import (
    RepayOperation as MorphoRepay,
)
from almanak.connectors.morpho_blue.operations import (
    SupplyOperation as MorphoSupply,
)
from almanak.connectors.pancakeswap_v3.operations import Addition as PancakeAddition
from almanak.connectors.pancakeswap_v3.operations import Swap as PancakeSwap
from almanak.connectors.repayment import require_repayment_funding
from almanak.connectors.sparklend.operations import RepayOperation as SparkRepay
from almanak.connectors.sparklend.operations import SupplyOperation as SparkSupply
from almanak.connectors.target_routing import connector_for_target, target_connector
from almanak.connectors.uniswap_v3.operations import Addition, Swap
from almanak.connectors.uniswap_v4.state import Mint as V4Mint
from almanak.connectors.uniswap_v4.state import Swap as V4Swap
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount


def backend_view(backend, target):
    view = copy(backend)
    view.config = backend.config.component(target_connector(target))
    return view


def spending(operation):
    if isinstance(operation, (GmxSwap, GmxLiquidity)):
        return operation.inputs
    if isinstance(operation, GmxOrder):
        return (operation.execution_fee.model_copy(update={"amount": gmx_order_input(operation)}),)
    if isinstance(
        operation,
        (
            SupplyOperation,
            AaveV4Supply,
            AaveV4Repay,
            RepayOperation,
            SparkRepay,
            SparkSupply,
            MorphoSupply,
            MorphoRepay,
            CompoundSupply,
            CompoundRepay,
            Swap,
            PancakeSwap,
            AerodromeSwap,
            SlipstreamSwap,
            V4Swap,
            CurveSwap,
        ),
    ):
        return (operation.amount,)
    if isinstance(
        operation, (Addition, V4Mint, PancakeAddition, AerodromeAddition, SlipstreamAddition)
    ):
        return operation.maximum0, operation.maximum1
    if isinstance(operation, CurveAddition):
        return operation.amounts.root
    return ()


def full_repayment(operation) -> bool:
    if isinstance(operation, (RepayOperation, SparkRepay, CompoundRepay, MorphoRepay)):
        return operation.full
    # Aave V4 has only full repayment.
    return isinstance(operation, AaveV4Repay)


def projected_debt(operation, before) -> int:
    """The debt a full repayment spends; its approval is an upper bound, not spending."""
    if isinstance(operation, MorphoRepay):
        return morpho_funding(before, operation).amount
    if isinstance(operation, (RepayOperation, SparkRepay)):
        return before.position.debt_assets
    if isinstance(operation, CompoundRepay):
        return before.account.borrowed_base
    return before.position.total_debt


def validate_inventory(state, operation, before=None):
    amounts = {balance.key: balance.amount for balance in state.inventory}
    if full_repayment(operation):
        if before is None:
            raise ValueError("full repayment inventory validation requires its exact snapshot")
        # A full repay may also spend loan asset sent to the fund beyond the retained
        # inventory, so a top-up clears a shortfall without a restart. That surplus is
        # no component's inventory, and the repay's ownership absorbs what it spent.
        target = operation.target
        debt = projected_debt(operation, before)
        require_repayment_funding(target.allocation, target.asset, debt, before.wallet_balance)
        return
    for value in spending(operation):
        key = wallet_key(operation.target, value.asset)
        if key not in amounts or value.amount > amounts[key]:
            raise DataError("inventory_unavailable", "operation exceeds retained fund inventory")


def requirements(config, account):
    """Only the explicit shared inventory; components spend from it instead of adding to it."""
    return summed(
        (
            AssetAmount(asset=balance.key.asset, amount=balance.amount)
            for balance in config.inventory
            if balance.key.account == account
        ),
        "explicit initial strategy-owned fund inventory",
    )


def entry_transactions(config, account):
    """Every component's entry transactions for the account; they share its payer."""
    return sum(
        connector_for(view).execution.entry_transactions(view, account)
        for _, view in components_for_account(config, account)
    )


def preflight(config, account, observations):
    # One account binds one chain, so its measured wallet keys are unique by asset.
    measured = {
        key.asset: value[1]
        for key, value in cash_evidence(observations).items()
        if key.account == account
    }
    tokens = funding_rows(requirements(config, account), measured)
    pristine = tuple(
        connector_for(view).execution.preflight(view, account, local(observations, identity))[1]
        for identity, view in components_for_account(config, account)
    )
    return tokens, bool(pristine) and all(pristine)


class ComposedExecution:
    # Components count their own kinds; the composition submits nothing of its own.
    gas_lifecycle = GasLifecycle(kinds={})
    preflight = staticmethod(preflight)
    requirements = staticmethod(requirements)
    entry_transactions = staticmethod(entry_transactions)

    @staticmethod
    def validate_config(config):
        for identity in config.parameter_components:
            view = config.component(identity)
            connector_for(view).execution.validate_config(view)

    @staticmethod
    def snapshot(backend, name, block, reference=None):
        target = backend.config.target(name)
        if reference is None:
            from almanak.connectors.composed.inventory import initial_owned

            reference = next(o for o in initial_owned(backend.config) if o.target == target)
        return connector_for_target(target).execution.snapshot(
            backend_view(backend, target), name, block, reference
        )

    @staticmethod
    def operation(config, state, intent, before):
        identity = target_connector(intent.target)
        return connector_for_target(intent.target).execution.operation(
            config.component(identity), component_state(state, identity), intent, before
        )

    @staticmethod
    def call(config, operation):
        return connector_for_target(operation.target).execution.call(
            component_for_allocation(config, operation.allocation), operation
        )

    @staticmethod
    def validate_operation(backend, state, operation, before):
        execution = connector_for_target(operation.target).execution
        view = backend_view(backend, operation.target)
        if execution.increases_exposure(view.config, operation) and any(
            o.accounting_issues for o in state.outcomes
        ):
            raise DataError("entry_unavailable", "new exposure requires reconciled fund accounting")
        validate_inventory(state, operation, before)
        execution.validate_operation(
            view, component_state(state, target_connector(operation.target)), operation, before
        )

    @staticmethod
    def validate_simulation(operation, result, before):
        connector_for_target(operation.target).execution.validate_simulation(
            operation, result, before
        )

    @staticmethod
    def validate_current(backend, plan, current):
        connector_for_target(plan.operation.target).execution.validate_current(
            backend_view(backend, plan.operation.target), plan, current
        )

    @staticmethod
    def expired(operation, block) -> bool:
        # Only a component whose calldata enforces the deadline declares expiry; lending
        # and GMX operations never become provably dead and keep the rebroadcast rule.
        expired = getattr(connector_for_target(operation.target).execution, "expired", None)
        return expired is not None and expired(operation, block)

    @staticmethod
    def plan_evidence(backend, operation, call, before):
        return connector_for_target(operation.target).execution.plan_evidence(
            backend_view(backend, operation.target), operation, call, before
        )

    @staticmethod
    def current_checks(backend, plan, block=None):
        connector_for_target(plan.operation.target).execution.current_checks(
            backend_view(backend, plan.operation.target), plan, block
        )

    @staticmethod
    def reconcile(backend, pending, receipt, block):
        return connector_for_target(pending.operation.target).execution.reconcile(
            backend_view(backend, pending.operation.target), pending, receipt, block
        )

    @staticmethod
    def increases_exposure(config, operation):
        return connector_for_target(operation.target).execution.increases_exposure(
            component_for_allocation(config, operation.allocation), operation
        )

    @staticmethod
    def validate_replacement(config, operation):
        connector_for_target(operation.target).execution.validate_replacement(
            component_for_allocation(config, operation.allocation), operation
        )
