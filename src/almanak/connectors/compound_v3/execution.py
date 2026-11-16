"""Comet semantics use the shared guarded EVM execution, account and fee path."""

from almanak.connectors.compound_v3 import capacity, planning
from almanak.connectors.compound_v3.lifecycle import validate_market_ownership
from almanak.connectors.compound_v3.observations import snapshot
from almanak.connectors.compound_v3.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    plan_call,
)
from almanak.connectors.compound_v3.receipts import accounting_issues, movement, validate_effects
from almanak.connectors.erc20_approval import approval_succeeded
from almanak.connectors.funding import (
    funding_rows,
    lending_entries,
    lending_requirements,
)
from almanak.connectors.gas_lifecycle import lending_lifecycle
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees
from almanak.lending_intents import RepayIntent


def increases_exposure(config, operation):
    if isinstance(operation, ApproveOperation):
        return operation.purpose == "supply"
    return isinstance(operation, (SupplyOperation, BorrowOperation))


def economic_operation(operation, intent):
    if not isinstance(operation, ApproveOperation):
        return operation
    if operation.purpose == "supply":
        return SupplyOperation(target=operation.target, amount=operation.amount)
    return RepayOperation(
        target=operation.target, amount=operation.amount, full=isinstance(intent, RepayIntent)
    )


def validate_entry(state, operation, before):
    if not increases_exposure(state.config, operation):
        return
    if state.mode != "running" or any(o.accounting_issues for o in state.outcomes):
        raise DataError(
            "entry_unavailable",
            "new Comet exposure requires running mode and reconciled accounting",
        )
    allocation = state.config.allocation(operation.allocation)
    if before.native_balance.amount < allocation.minimum_native_balance:
        raise DataError("payer_unavailable", "Comet gas payer is below its configured minimum")


def current_capacity(config, operation, before, intent):
    if isinstance(operation, RevokeOperation):
        if not before.allowance:
            raise DataError("allowance_mismatch", "Comet revocation requires residual allowance")
        return
    core = economic_operation(operation, intent)
    if isinstance(operation, ApproveOperation):
        if before.allowance:
            raise DataError("allowance_mismatch", "Comet approval requires prior allowance cleanup")
        # Check the proposed economic step with the exact approval being admitted.
        # This projection is not persisted as an observed allowance.
        before = before.model_copy(update={"allowance": operation.amount.amount})
    capacity.validate(config, before, core)


def retained_allowance(state, before):
    for outcome in reversed(state.outcomes):
        if outcome.after.target == before.target:
            return outcome.after.allowance
    for baseline in state.baseline:
        if baseline.target == before.target:
            return baseline.allowance
    return 0


def validate_operation(backend, state, operation, before):
    backend.guard()
    if state.mode == "closed":
        raise ValueError("closed deployments cannot execute Comet operations")
    if state.decision is None or operation != planning.operation(
        backend.config, state, state.decision.intent, before
    ):
        raise ValueError("Comet operation differs from its retained economic decision")
    retained = next(o for o in state.owned if o.target == operation.target)
    planning.validate_ownership(retained, before)
    validate_market_ownership(state, before)
    if retained_allowance(state, before) != before.allowance:
        raise DataError("allowance_mismatch", "Comet allowance changed outside retained execution")
    validate_entry(state, operation, before)
    current_capacity(backend.config, operation, before, state.decision.intent)


def validate_current(backend, plan, current):
    before = plan.before
    if (
        before.target != current.target
        or before.allowance != current.allowance
        or before.account.principal != current.account.principal
        or before.account.collaterals != current.account.collaterals
        or (before.account.assets_in, before.account.extended_assets_in)
        != (current.account.assets_in, current.account.extended_assets_in)
    ):
        raise DataError(
            "precondition_changed", "Comet fund ownership or allowance changed after preparation"
        )
    if current.wallet_balance < before.wallet_balance:
        raise DataError("precondition_changed", "Comet fund inventory decreased after preparation")
    current_capacity(backend.config, plan.operation, current, plan.decision.intent)


def validate_simulation(operation, result, before):
    raw = bytes.fromhex(result[2:])
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        if not approval_succeeded(raw):
            raise DataError("simulation", "Compound underlying token rejected approval")
    elif raw:
        raise DataError("simulation", "Comet economic operation returned unexpected data")


def reconcile(backend, pending, receipt, block):
    from almanak.connectors.compound_v3.state import Outcome

    after = snapshot(backend, pending.operation.allocation, block)
    success = receipt["status"] == "0x1"
    assets = movement(receipt, pending.operation) if success else 0
    principal, collateral = validate_effects(pending, after, assets, success)
    fees, fee_evidence = charged_fees(backend, pending, receipt, block)
    return Outcome(
        decision=pending.decision,
        operation=pending.operation,
        tx_hash=pending.tx_hash,
        nonce=pending.nonce,
        before=pending.before,
        after=after,
        success=success,
        assets=assets,
        principal_delta=principal,
        collateral_delta=collateral,
        gas_paid=fees,
        op_fee_evidence=fee_evidence,
        receipt=receipt,
        call=pending.call,
        execution=pending.execution,
        accounting_issues=accounting_issues(
            pending.before, after, pending.operation, assets, fees.amount
        ),
    )


class CompoundExecution:
    validate_operation = staticmethod(validate_operation)
    operation = staticmethod(planning.operation)
    validate_current = staticmethod(validate_current)
    validate_simulation = staticmethod(validate_simulation)
    reconcile = staticmethod(reconcile)
    increases_exposure = staticmethod(increases_exposure)

    @staticmethod
    def snapshot(backend, name, block, reference=None):
        return snapshot(backend, name, block)

    @staticmethod
    def call(config, operation):
        if operation.target != config.target(operation.allocation):
            raise ValueError("Comet call target differs from authoritative configuration")
        return plan_call(operation)

    @staticmethod
    def validate_config(config):
        for allocation in config.positions:
            if allocation.reserve.market.comet == allocation.reserve.asset.reference:
                raise ValueError("Comet and underlying token must be distinct contracts")

    @staticmethod
    def validate_replacement(config, operation):
        raise DataError(
            "replacement_unsupported", "Compound replacement requires separate execution proofs"
        )

    @staticmethod
    def plan_evidence(backend, operation, call, before):
        return {}

    @staticmethod
    def current_checks(backend, plan, block=None):
        block = block or backend.reader.pin_block("latest")
        current = snapshot(backend, plan.operation.allocation, block)
        validate_current(backend, plan, current)

    requirements = staticmethod(lending_requirements)
    gas_lifecycle = lending_lifecycle("borrow")
    entry_transactions = staticmethod(lending_entries(gas_lifecycle))

    @staticmethod
    def preflight(config, account, observations):
        observed = tuple(o for o in observations if o.target.account == account)
        targets = {config.target(a.name) for a in config.positions if a.account == account}
        balances = {o.target.asset: o.wallet_balance for o in observed}
        tokens = funding_rows(lending_requirements(config, account), balances)
        pristine = (
            bool(targets)
            and {o.target for o in observed} == targets
            and all(
                not (
                    o.allowance
                    or o.account.principal
                    or any(c.balance for c in o.account.collaterals)
                )
                for o in observed
            )
        )
        return tokens, pristine
