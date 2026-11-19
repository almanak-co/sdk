"""Shared Aave transaction lifecycle; environment profiles enforce execution authority."""

from typing import assert_never

from eth_abi import decode

from almanak.connectors.aave_v4.authority import admit_intent
from almanak.connectors.aave_v4.debt import project_borrow, snapshot_evidence
from almanak.connectors.aave_v4.deployments import validate_markets
from almanak.connectors.aave_v4.models import (
    DebtAllocation,
    LendingSnapshot,
)
from almanak.connectors.aave_v4.operations import (
    AaveOperation,
    ApproveOperation,
    BorrowOperation,
    CollateralOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.aave_v4.preflight import GAS_LIFECYCLE
from almanak.connectors.aave_v4.preflight import entry_transactions as _entry_transactions
from almanak.connectors.aave_v4.preflight import preflight as _preflight
from almanak.connectors.aave_v4.preflight import requirements as _requirements
from almanak.connectors.aave_v4.reader import (
    plan_call,
    plan_operation,
    read_position,
    receipt_movement,
)
from almanak.connectors.aave_v4.state import Checkpoint, Outcome, Pending
from almanak.connectors.aave_v4.validation import (
    require_zero_debt,
    validate_account_release,
    validate_debt_movement,
    validate_debt_operation,
    validate_owned,
    validate_repayment_funding,
)
from almanak.connectors.erc20_approval import approval_succeeded
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import erc20_address
from almanak.gateway.evm_fees import charged_fees
from almanak.models import AssetAmount, Block, Eoa, SafeRoles
from almanak.operator_deployment import is_operator


def snapshot(backend, name: str, block: Block) -> LendingSnapshot:
    allocation = backend.config.allocation(name)
    if backend.config.production is not None:
        from almanak.connectors.aave_v4.deployments import verify_deployments

        verify_deployments(backend.reader, allocation.reserve, block)
    profile = backend.config.accounts.binding(allocation.account).profile
    fund = profile.fund
    position = read_position(backend.reader, allocation.reserve, fund, block)
    from almanak.configuration.metadata import verify

    verify(backend.config, position.reserve.asset)
    (balance,) = backend.reader.call(
        erc20_address(allocation.reserve.asset, block.chain.chain_id),
        "balanceOf(address)",
        block,
        inputs=("address",),
        args=(fund.address,),
    )
    (allowance,) = backend.reader.call(
        erc20_address(allocation.reserve.asset, block.chain.chain_id),
        "allowance(address,address)",
        block,
        inputs=("address", "address"),
        args=(fund.address, allocation.reserve.spoke),
    )
    native = int(
        backend.reader._rpc(
            "eth_getBalance",
            [profile.submitter.address, {"blockHash": block.hash, "requireCanonical": True}],
        ),
        16,
    )
    evidence = {}
    if backend.config.debts or isinstance(profile, SafeRoles):
        evidence = snapshot_evidence(backend.reader, backend.config, allocation, block)
    return LendingSnapshot(
        allocation=name,
        account=allocation.account,
        position=position,
        wallet_balance=balance,
        allowance=allowance,
        native_balance=AssetAmount(asset=allocation.native_asset, amount=native),
        payer=profile.submitter,
        **evidence,
    )


def validate_operation(
    backend, state: Checkpoint, operation: AaveOperation, before: LendingSnapshot
):
    backend.guard()
    allocation = backend.config.allocation(operation.allocation)
    fund = backend.config.accounts.binding(operation.account).profile.fund
    owned = next(item.shares for item in state.owned if item.target == operation.target)
    position = before.position
    if (
        before.target != operation.target
        or operation.target != backend.config.target(allocation.name)
        or position.account != fund
    ):
        raise ValueError("operation observation does not match the authorized allocation")
    if state.decision is None or operation != plan_operation(
        state.decision.intent, before, allocation
    ):
        raise ValueError("operation differs from its retained economic intent")
    if state.capabilities is None:
        raise ValueError("gateway requires retained economic capabilities")
    admit_intent(state.capabilities, state.decision.intent)
    if position.supplied_shares != owned or (
        position.total_debt and not isinstance(allocation, DebtAllocation)
    ):
        raise DataError("ownership_mismatch", "observed exposure differs from owned supply")
    ownership = next(o for o in state.owned if o.target == operation.target)
    validate_owned(ownership, before)
    if isinstance(allocation, DebtAllocation):
        validate_debt_operation(state, operation, before, allocation)
    elif isinstance(operation, (ApproveOperation, SupplyOperation)):
        validate_supply(state, operation, before, allocation, owned)
    else:
        if backend.config.debts or before.collateral_enabled:
            validate_collateral_release(backend, allocation, before)
        validate_teardown(state, operation, before, owned)
    if is_operator(state):
        validate_account_release(backend.reader, operation, before.block)


def validate_collateral_release(backend, allocation, before):
    if before.collateral_enabled:
        raise DataError(
            "residual_collateral",
            "disable collateral after verified repayment before withdrawal",
        )
    for debt in backend.config.debts:
        if allocation.name in debt.collateral:
            require_zero_debt(backend.snapshot(debt.name, before.position.reserve.block))


def borrow_projection(backend, operation, allocation, call, block):
    if not isinstance(operation, BorrowOperation):
        return None
    backend.reader.pin_block(block.number)
    return project_borrow(backend.reader, backend.config, allocation, call, block)


def current_debt_checks(backend, plan, block=None):
    allocation = backend.config.allocation(plan.operation.allocation)
    if not isinstance(allocation, DebtAllocation):
        return
    block = block or backend.reader.pin_block("latest")
    if isinstance(plan.operation, BorrowOperation):
        project_borrow(backend.reader, backend.config, allocation, plan.call, block)
    if isinstance(plan.operation, RepayOperation):
        validate_repayment_funding(backend.snapshot(allocation.name, block), allocation)


def current_release_checks(backend, plan, block=None):
    """Hold an operator's planned release of collateral to the account's debt now.

    The plan was validated at its own pinned block. A debt opened since then is seen
    here, at the head the step is signed against, so the plan is refused unsigned.
    """
    # Only an operator deployment admits an operator trigger, and a plan retains it.
    if plan.decision.trigger.kind != "operator":
        return
    block = block or backend.reader.pin_block("latest")
    validate_account_release(backend.reader, plan.operation, block)


def current_checks(backend, plan, block=None):
    from almanak.gateway.replacement_assets import verify_asset
    from almanak.transaction_family import replacement_policy

    if replacement_policy(backend.config, plan) is not None:
        block = block or backend.reader.pin_block("latest")
        verify_asset(backend.reader, plan.operation.target.reserve.asset, block)
    current_debt_checks(backend, plan, block)
    current_release_checks(backend, plan, block)


def reconcile(backend, pending, receipt, block):
    after = backend.snapshot(pending.operation.allocation, block)
    success = receipt["status"] == "0x1"
    assets, shares = (0, 0)
    if success:
        assets, shares = receipt_movement(
            receipt,
            backend.config.allocation(pending.operation.allocation),
            pending.call.fund,
            pending.operation,
        )
    before = pending.before
    fees, fee_evidence = charged_fees(backend, pending, receipt, block)
    gas_paid = fees.amount
    accounting_issues = validate_balances(pending, after, assets, shares, gas_paid, success)
    amount = pending.operation.amount.amount if pending.operation.amount else 0
    validate_effects(pending, after, success, assets, shares, amount)
    return Outcome(
        decision=pending.decision,
        operation=pending.operation,
        tx_hash=pending.tx_hash,
        nonce=pending.nonce,
        before=before,
        after=after,
        success=success,
        assets=assets,
        shares=shares,
        gas_paid=fees,
        op_fee_evidence=fee_evidence,
        receipt=receipt,
        call=pending.call,
        execution=pending.execution,
        borrow_projection=pending.borrow_projection,
        accounting_issues=accounting_issues,
    )


def validate_supply(
    state, operation: ApproveOperation | SupplyOperation, before, allocation, owned
):
    if any(item.accounting_issues for item in state.outcomes):
        raise DataError("accounting", "unexplained wallet movements block new supply")
    if (
        before.native_balance.asset != allocation.native_asset
        or before.native_balance.amount < allocation.minimum_native_balance
    ):
        raise DataError("unavailable", "native balance is below the configured minimum")
    amount = operation.amount.amount
    if state.mode != "running":
        raise ValueError("new supply requires running mode")
    validate_supply_availability(before, amount)
    expected = 0 if isinstance(operation, ApproveOperation) else amount
    if before.allowance != expected:
        raise DataError("inconsistent", "allowance does not match the planned supply step")


def validate_supply_availability(before, amount):
    if (
        before.position.reserve.paused
        or before.position.reserve.frozen
        or before.wallet_balance < amount
    ):
        raise DataError("unavailable", "reserve or allocated funds are unavailable for supply")


def validate_teardown(state, operation, before, owned):
    if state.mode == "closed":
        raise ValueError("a closed deployment cannot dispatch withdrawals")
    if isinstance(operation, WithdrawOperation):
        if not owned or before.position.reserve.paused:
            raise DataError("unavailable", "owned supply cannot be withdrawn")
    else:
        validate_revoke(before, owned)


def validate_revoke(before, owned):
    if owned or not before.allowance:
        raise ValueError("allowance cleanup requires zero owned shares and residual allowance")


def validate_simulation(operation: AaveOperation, result, amount, owned):
    raw = bytes.fromhex(result[2:])
    if isinstance(operation, CollateralOperation):
        if raw:
            raise DataError("inconsistent", "collateral simulation returned unexpected data")
        return
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        if not approval_succeeded(raw):
            raise DataError("inconsistent", "token rejected approval simulation")
        return
    shares, assets = decode(("uint256", "uint256"), raw)
    validate_simulated_movement(operation, shares, assets, amount, owned)


def validate_simulated_movement(
    operation: SupplyOperation | WithdrawOperation | BorrowOperation | RepayOperation,
    shares,
    assets,
    amount,
    owned,
):
    if isinstance(operation, RepayOperation):
        validate_simulated_repayment(shares, assets, amount, owned)
        return
    if min(shares, assets) <= 0:
        raise DataError("inconsistent", "simulation does not match the owned supply operation")
    measured, expected = (
        (assets, amount)
        if isinstance(operation, (SupplyOperation, BorrowOperation))
        else (shares, owned)
    )
    if measured != expected:
        raise DataError("inconsistent", "simulation does not match the owned supply operation")


def validate_simulated_repayment(shares, assets, approved, owned):
    if shares != owned or not 0 < assets <= approved:
        raise DataError(
            "residual_debt", "repayment simulation does not repay the full debt within approval"
        )


def validate_current_position(before, current):
    # Accrued assets may change while the owned debt shares and collateral stay fixed.
    fields = (
        "allowance",
        "drawn_shares",
        "premium_shares",
        "premium_offset_ray",
        "collateral_enabled",
        "collateral",
    )
    if before.position.supplied_shares != current.position.supplied_shares or any(
        getattr(before, f) != getattr(current, f) for f in fields
    ):
        raise DataError("precondition_changed", "position or allowance changed after preparation")


def validate_balances(pending: Pending, after, assets, shares, gas_paid, success=True):
    before = pending.before
    direction = movement_direction(pending.operation)
    supply_direction = (
        direction if isinstance(pending.operation, (SupplyOperation, WithdrawOperation)) else 0
    )
    if (after.position.supplied_shares, after.native_balance.asset, after.payer) != (
        before.position.supplied_shares + supply_direction * shares,
        before.native_balance.asset,
        before.payer,
    ):
        raise DataError(
            "ownership_mismatch", "receipt position effects do not match owned shares or gas payer"
        )
    validate_debt_movement(pending, after, assets, shares, success)
    issues = []
    if after.wallet_balance != before.wallet_balance - direction * assets:
        issues.append("wallet token delta includes unexplained external movements")
    if after.native_balance.amount != before.native_balance.amount - gas_paid:
        issues.append("gas payer balance includes unexplained external movements")
    return tuple(issues)


def movement_direction(operation: AaveOperation) -> int:
    if isinstance(operation, (SupplyOperation, RepayOperation)):
        return 1
    if isinstance(operation, (WithdrawOperation, BorrowOperation)):
        return -1
    if isinstance(operation, (ApproveOperation, RevokeOperation, CollateralOperation)):
        return 0
    assert_never(operation)


def successful_allowance(operation: AaveOperation, previous: int, assets: int = 0) -> int:
    if isinstance(operation, ApproveOperation):
        return operation.amount.amount
    if isinstance(operation, (SupplyOperation, RevokeOperation)):
        return 0
    if isinstance(operation, RepayOperation):
        return previous - assets
    if isinstance(operation, (WithdrawOperation, BorrowOperation, CollateralOperation)):
        return previous
    assert_never(operation)


def validate_effects(pending: Pending, after, success, assets, shares, amount):
    expected_allowance = pending.before.allowance
    if success:
        expected_allowance = successful_allowance(
            pending.operation, pending.before.allowance, assets
        )
        validate_successful_movement(pending.operation, after, assets, shares, amount)
    if after.allowance != expected_allowance:
        raise DataError("inconsistent", "post-transaction allowance does not reconcile")


def validate_successful_movement(operation, after, assets, shares, amount):
    if isinstance(operation, SupplyOperation):
        validate_supply_event(assets, shares, amount)
    if isinstance(operation, WithdrawOperation) and after.position.supplied_shares:
        raise DataError("inconsistent", "full withdrawal left residual supply shares")


def validate_supply_event(assets, shares, amount):
    if assets != amount or shares == 0:
        raise DataError("inconsistent", "supply event does not match its allocation")


class AaveExecution:
    @staticmethod
    def validate_replacement(config, operation):
        # Verified deployments and direct EOA / Roles CALL paths have no gas-price-based
        # economic parameters. Current simulation still enforces the original operation.
        profile = config.accounts.binding(operation.account).profile
        if not isinstance(profile, (Eoa, SafeRoles)) or not isinstance(
            operation,
            (
                ApproveOperation,
                SupplyOperation,
                WithdrawOperation,
                BorrowOperation,
                RepayOperation,
                RevokeOperation,
                CollateralOperation,
            ),
        ):
            raise DataError(
                "replacement_unsupported", "operation/account does not support fee-only replacement"
            )
        from almanak.gateway.replacement_assets import require_network

        require_network(operation.target.reserve.asset.chain)

    preflight = staticmethod(_preflight)
    requirements = staticmethod(_requirements)
    gas_lifecycle = GAS_LIFECYCLE
    entry_transactions = staticmethod(_entry_transactions)
    validate_config = staticmethod(validate_markets)
    validate_operation = staticmethod(validate_operation)
    current_checks = staticmethod(current_checks)
    reconcile = staticmethod(reconcile)

    @staticmethod
    def snapshot(backend, name, block, reference=None):
        return snapshot(backend, name, block)

    @staticmethod
    def operation(config, state, intent, before):
        return plan_operation(intent, before, config.allocation(intent.target.allocation))

    @staticmethod
    def call(config, operation):
        allocation = config.allocation(operation.allocation)
        fund = config.accounts.binding(operation.account).profile.fund
        return plan_call(allocation, fund, operation)

    @staticmethod
    def validate_simulation(operation, result, before):
        owned = (
            before.drawn_shares
            if isinstance(operation, RepayOperation)
            else before.position.supplied_shares
        )
        amount = operation.amount.amount if operation.amount else 0
        validate_simulation(operation, result, amount, owned)

    @staticmethod
    def plan_evidence(backend, operation, call, before):
        allocation = backend.config.allocation(operation.allocation)
        return {
            "borrow_projection": borrow_projection(
                backend, operation, allocation, call, before.block
            )
        }

    @staticmethod
    def validate_current(backend, plan, current):
        validate_current_position(plan.before, current)
        if current.wallet_balance < plan.before.wallet_balance:
            raise DataError(
                "precondition_changed", "fund no longer covers the prepared transaction"
            )

    @staticmethod
    def increases_exposure(config, operation):
        if isinstance(operation, CollateralOperation):
            return operation.enabled
        if isinstance(operation, ApproveOperation):
            return not isinstance(config.allocation(operation.allocation), DebtAllocation)
        return isinstance(operation, (SupplyOperation, BorrowOperation))
