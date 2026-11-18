"""V3 protocol semantics shared by identified forks and authorized EOA/Safe execution."""

from eth_abi import decode

from almanak.connectors.erc20_approval import approval_succeeded
from almanak.connectors.funding import (
    LENDING_LIFECYCLE,
    funding_rows,
    lending_entry_transactions,
    lending_requirements,
)
from almanak.connectors.sparklend import planning
from almanak.connectors.sparklend.observations import snapshot
from almanak.connectors.sparklend.operations import (
    ApproveOperation,
    BorrowOperation,
    CollateralOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
    plan_call,
)
from almanak.connectors.sparklend.projection import requires_risk, validate_projection
from almanak.connectors.sparklend.receipts import accounting_issues, movement, validate_effects
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees
from almanak.lending_intents import RepayIntent


def increases_exposure(config, operation):
    if isinstance(operation, ApproveOperation):
        return operation.purpose == "supply"
    if isinstance(operation, CollateralOperation):
        return operation.enabled
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
            "entry_unavailable", "new exposure requires running mode and reconciled accounting"
        )
    allocation = state.config.allocation(operation.allocation)
    if before.native_balance.amount < allocation.minimum_native_balance:
        raise DataError("payer_unavailable", "gas payer is below the configured native minimum")


def validate_pool_ownership(state, before):
    if before.pool_risk is None:
        return
    for position in before.pool_risk.positions:
        matches = [
            o
            for o in state.owned
            if (o.target.fund, o.target.reserve) == (position.fund, position.reserve.source)
        ]
        if len(matches) != 1:
            raise DataError(
                "ownership_mismatch", "Pool risk contains a position outside owned scope"
            )
        owned = matches[0]
        if (owned.supply_scaled, owned.debt_scaled, owned.collateral_enabled) != (
            position.supplied_scaled,
            position.debt_scaled,
            position.collateral_enabled,
        ):
            raise DataError(
                "ownership_mismatch", "Pool risk differs from retained collateral or debt"
            )


def validate_collateral(config, operation, before):
    planning.reserve_available(before.position.reserve)
    if (
        not before.position.supplied_scaled
        or operation.enabled == before.position.collateral_enabled
    ):
        raise DataError(
            "collateral_unavailable", "collateral control requires a nonempty changing position"
        )
    if operation.enabled and (
        not config.allocation(operation.allocation).collateral
        or not before.position.reserve.configuration.ltv_bps
    ):
        raise DataError(
            "collateral_unavailable", "collateral enablement is unavailable or unauthorized"
        )


def current_capacity(config, operation, before):
    if increases_exposure(config, operation):
        validate_ordinary_reserves(before)
    if isinstance(operation, SupplyOperation):
        planning.supply_capacity(before, operation.amount.amount)
        if (
            not before.position.supplied_scaled
            and before.position.reserve.configuration.first_supply_enables_collateral
            and not config.allocation(operation.allocation).collateral
        ):
            raise DataError(
                "collateral_authority",
                "automatic Pool collateral enablement is not authorized: the reserve's LTV is "
                "above zero, so a first supply becomes collateral; set collateral: true on "
                f"allocation {operation.allocation}",
            )
    elif isinstance(operation, BorrowOperation):
        planning.borrow_capacity(before, operation.amount.amount)
    elif isinstance(operation, WithdrawOperation):
        planning.withdraw_capacity(before, operation)
    elif isinstance(operation, RepayOperation):
        planning.repayment_capacity(before, operation)
    elif isinstance(operation, CollateralOperation):
        validate_collateral(config, operation, before)
    elif isinstance(operation, RevokeOperation) and not before.allowance:
        raise DataError("allowance_mismatch", "revocation requires residual allowance")
    validate_projection(config, before, operation)


def validate_ordinary_reserves(before):
    if before.pool_risk is None:
        raise DataError("risk_unavailable", "new exposure requires complete Pool risk")
    for position in before.pool_risk.positions:
        config = position.reserve.configuration
        if config.bits(212, 40) or config.bits(62, 1):
            raise DataError(
                "unsupported_mode", "SparkLend currently excludes isolated and siloed reserves"
            )


def validate_operation(backend, state, operation, before):
    backend.guard()
    if state.mode == "closed":
        raise ValueError("closed deployments cannot execute protocol operations")
    if state.decision is None or operation != planning.operation(
        backend.config, state, state.decision.intent, before
    ):
        raise ValueError("operation differs from its retained economic decision")
    owned = next(o for o in state.owned if o.target == operation.target)
    planning.validate_ownership(owned, before)
    core = economic_operation(operation, state.decision.intent)
    if requires_risk(before, core):
        validate_pool_ownership(state, before)
    validate_entry(state, operation, before)
    current_capacity(backend.config, core, before)


def validate_current(backend, plan, current):
    before = plan.before
    if (
        before.target != current.target
        or before.allowance != current.allowance
        or (
            before.position.supplied_scaled,
            before.position.debt_scaled,
            before.position.collateral_enabled,
            before.position.user_configuration,
        )
        != (
            current.position.supplied_scaled,
            current.position.debt_scaled,
            current.position.collateral_enabled,
            current.position.user_configuration,
        )
    ):
        raise DataError(
            "precondition_changed", "owned position or allowance changed after preparation"
        )
    if current.wallet_balance < before.wallet_balance:
        raise DataError("precondition_changed", "fund inventory decreased after preparation")
    current_capacity(
        backend.config, economic_operation(plan.operation, plan.decision.intent), current
    )


def validate_simulation(operation, result, before):
    raw = bytes.fromhex(result[2:])
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        if not approval_succeeded(raw):
            raise DataError("simulation", "underlying token rejected approval")
        return
    if isinstance(operation, (SupplyOperation, BorrowOperation, CollateralOperation)):
        if raw:
            raise DataError("simulation", "Pool operation returned unexpected data")
        return
    amount = decode(("uint256",), raw)[0]
    expected = (
        min(operation.amount.amount, before.position.debt_assets)
        if isinstance(operation, RepayOperation)
        else before.position.supplied_assets
        if operation.full
        else operation.amount.amount
    )
    if not amount or amount != expected:
        raise DataError("simulation", "simulated repayment or withdrawal differs from owned assets")


def reconcile(backend, pending, receipt, block):
    from almanak.connectors.sparklend.state import Outcome

    after = snapshot(backend, pending.operation.allocation, block)
    success = receipt["status"] == "0x1"
    assets = movement(receipt, pending.operation) if success else 0
    supply, debt = validate_effects(pending, after, assets, success)
    fees, fee_evidence = charged_fees(backend, pending, receipt, block)
    gas = fees.amount
    return Outcome(
        decision=pending.decision,
        operation=pending.operation,
        tx_hash=pending.tx_hash,
        nonce=pending.nonce,
        before=pending.before,
        after=after,
        success=success,
        assets=assets,
        supply_delta=supply,
        debt_delta=debt,
        gas_paid=fees,
        op_fee_evidence=fee_evidence,
        receipt=receipt,
        call=pending.call,
        execution=pending.execution,
        accounting_issues=accounting_issues(pending.before, after, pending.operation, assets, gas),
    )


class SparkExecution:
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
            raise ValueError("call target differs from authoritative configuration")
        return plan_call(operation)

    @staticmethod
    def validate_config(config):
        for allocation in config.positions:
            if allocation.reserve.pool == allocation.reserve.asset.reference:
                raise ValueError("Pool and underlying token must be distinct contracts")

    @staticmethod
    def validate_replacement(config, operation):
        raise DataError(
            "replacement_unsupported", "SparkLend replacement requires separate execution proofs"
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
    gas_lifecycle = LENDING_LIFECYCLE
    entry_transactions = staticmethod(lending_entry_transactions)

    @staticmethod
    def preflight(config, account, observations):
        observed = tuple(o for o in observations if o.target.account == account)
        balances = {o.target.asset: o.wallet_balance for o in observed}
        tokens = funding_rows(lending_requirements(config, account), balances)
        targets = {config.target(a.name) for a in config.positions if a.account == account}
        pristine = (
            bool(targets)
            and {o.target for o in observed} == targets
            and all(
                not (
                    o.allowance
                    or o.position.supplied_scaled
                    or o.position.debt_scaled
                    or o.position.user_configuration
                )
                and o.pool_risk is not None
                and o.pool_risk.emode_category == 0
                for o in observed
            )
        )
        return tokens, pristine
