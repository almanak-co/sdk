"""Reconcile cash and fungible LP ownership without treating fees as separate cash."""

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.curve import operations as ops
from almanak.connectors.curve.observations import snapshot
from almanak.connectors.curve.positions import Position
from almanak.connectors.curve.receipts import movement, zero
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees
from almanak.models import AssetAmount


def owned_after(before, operation, effects, success):
    old = before.owned
    effects.validate_target(old.target)
    if not success:
        return old
    inventory = AssetAmounts(
        tuple(
            AssetAmount(asset=value.asset, amount=value.amount + delta.amount)
            for value, delta in zip(old.inventory.root, effects.asset_changes, strict=True)
        )
    )
    shares = (old.position.shares.amount if old.position else 0) + effects.share_change.amount
    if shares < 0:
        raise DataError("ownership", "Curve burn exceeds retained LP shares")
    position = (
        Position(shares=AssetAmount(asset=old.target.pool.shares_asset, amount=shares))
        if shares
        else None
    )
    return type(old)(
        target=old.target,
        inventory=inventory,
        position=position,
        entry_complete=old.entry_complete
        or isinstance(operation, ops.Addition)
        or (
            isinstance(operation, ops.Swap)
            and operation.amount.asset == operation.target.funding_asset
        ),
    )


def allowances_after(before, operation, effects, success):
    if not success:
        return before.allowances
    return tuple(settled_allowance(value, operation, effects) for value in before.allowances)


def settled_allowance(value, operation, effects):
    amount = value.amount
    if isinstance(operation, ops.Approval) and value.asset == operation.asset:
        amount = operation.amount
    elif amount != 2**256 - 1:
        amount += min(
            0,
            next(
                change.amount for change in effects.asset_changes if change.key.asset == value.asset
            ),
        )
    if amount < 0:
        raise DataError("receipt", "Curve settlement exceeds its retained allowance")
    return type(value)(asset=value.asset, spender=value.spender, amount=amount)


def validate_balances(pending, after, effects, gas, success):
    before = pending.before
    if after.position != after.owned.position:
        raise DataError("ownership", "observed Curve LP shares differ from reconciled ownership")
    if after.allowances != allowances_after(before, pending.operation, effects, success):
        raise DataError("receipt", "ERC-20 allowance differs from executed Curve settlement")
    if before.native_balance is None or after.native_balance is None:
        raise DataError("receipt", "Curve settlement is missing native payer balances")
    if before.payer != after.payer or before.native_balance.asset != after.native_balance.asset:
        raise DataError("receipt", "native gas payer scope changed across Curve execution")
    return (*wallet_movement_issues(before, after, effects, gas), *unowned_share_issues(after))


def unowned_share_issues(after):
    """LP shares the fund holds beside a trading-only target, which no step of it minted.

    The step is reconciled from its own receipt all the same: the shares are another
    account's transfer, and refusing the outcome over them would leave a mined step
    unresolved for as long as the receipt block is read.
    """
    held = after.unowned_shares
    if held is None:
        return ()
    return (
        f"{held.asset}: wallet holds {held.amount} base units of the pool's LP token "
        "that this allocation does not own",
    )


def wallet_movement_issues(before, after, effects, gas):
    issues = []
    for old, new, change in zip(
        before.wallets.root, after.wallets.root, effects.asset_changes, strict=True
    ):
        if new.amount != old.amount + change.amount:
            issues.append(f"{old.asset}: wallet includes unexplained external movements")
    if after.native_balance.amount != before.native_balance.amount - gas:
        issues.append("gas payer balance includes unexplained external movements")
    return tuple(issues)


def reconcile(backend, pending, receipt, block):
    from almanak.connectors.curve.state import Outcome, TransactionPlan

    op = pending.operation
    success = receipt["status"] == "0x1"
    effects = movement(receipt, op, pending.before) if success else zero(op.target)
    owned = owned_after(pending.before, op, effects, success)
    # An exit leg measures what non-funding inventory remains so the exit can complete
    # with residual dust; other operations record no quotes.
    exiting = pending.decision.intent.kind == "exit_exchange"
    after = snapshot(backend, op.allocation, block, owned, quotes=False, residual=exiting)
    gas_paid, fee_evidence = charged_fees(backend, pending, receipt, block)
    issues = validate_balances(pending, after, effects, gas_paid.amount, success)
    return Outcome(
        transaction_plan=TransactionPlan.model_validate(
            pending.model_dump(exclude={"family", "raw_transaction", "tx_hash", "unsent"})
        ),
        decision=pending.decision,
        operation=op,
        tx_hash=pending.tx_hash,
        nonce=pending.nonce,
        before=pending.before,
        after=after,
        success=success,
        asset_changes=effects.asset_changes,
        share_change=effects.share_change,
        gas_paid=gas_paid,
        receipt=receipt,
        op_fee_evidence=fee_evidence,
        call=pending.call,
        execution=pending.execution,
        accounting_issues=issues,
    )
