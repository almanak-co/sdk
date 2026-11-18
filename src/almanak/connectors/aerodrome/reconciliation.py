"""Reconcile fungible shares and stored fee checkpoints independently of cash."""

from almanak.connectors.aerodrome import operations as ops
from almanak.connectors.aerodrome.math import accrued
from almanak.connectors.aerodrome.observations import snapshot, stored_position
from almanak.connectors.aerodrome.reader import read_pool
from almanak.connectors.aerodrome.receipts import Effects, movement
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees
from almanak.models import AssetAmount


def retained_position(before, operation, effects, current, pool, success):
    old = before.owned.position
    observed = None if current.empty else current
    if not success or isinstance(operation, (ops.Approval, ops.Swap)):
        if observed != old:
            raise DataError("ownership", "unrelated operation changed the fund LP checkpoint")
        return old
    expected = (old.shares.amount if old else 0) + effects.shares_delta
    if current.shares.amount != expected:
        raise DataError("ownership", "observed LP shares differ from retained receipt effects")
    validate_claims(before, operation, effects, current, pool)
    return observed


def validate_claims(before, operation, effects, current, pool):
    old = before.owned.position
    for index in (0, 1):
        previous_global = getattr(before.pool, f"index{index}")
        global_index = getattr(pool, f"index{index}")
        checkpoint = getattr(current, f"index{index}")
        if not previous_global <= checkpoint <= global_index:
            raise DataError("ownership", "fund fee checkpoint falls outside observed pool indices")
        previous_index = getattr(old, f"index{index}") if old else 0
        previous_claim = getattr(old, f"fees{index}").amount if old else 0
        expected = accrued(
            old.shares.amount if old else 0, checkpoint, previous_index, previous_claim
        )
        paid = getattr(effects, f"fees{index}")
        retained = getattr(current, f"fees{index}").amount
        if isinstance(operation, ops.Collect):
            matches = paid == expected and retained == 0
        else:
            matches = paid == 0 and retained == expected
        if not matches:
            raise DataError("ownership", "stored fee claims differ from accrued and paid amounts")


def owned_after(before, operation, effects, position, success):
    old = before.owned
    if not success:
        return old
    inventory = tuple(
        AssetAmount(asset=value.asset, amount=value.amount + delta)
        for value, delta in zip(
            (old.inventory0, old.inventory1), (effects.delta0, effects.delta1), strict=True
        )
    )
    entered = (
        old.entry_complete
        or isinstance(operation, ops.Addition)
        or (
            isinstance(operation, ops.Swap)
            and operation.amount.asset == operation.target.funding_currency
        )
    )
    return type(old)(
        target=old.target,
        inventory0=inventory[0],
        inventory1=inventory[1],
        position=position,
        entry_complete=entered,
    )


def allowance_spent(operation, effects, asset):
    if isinstance(operation, ops.Removal):
        return operation.shares.amount if asset == operation.shares.asset else 0
    if isinstance(operation, (ops.Swap, ops.Addition)) and asset in operation.target.assets:
        return -min(0, (effects.delta0, effects.delta1)[operation.target.assets.index(asset)])
    return 0


def allowances_after(before, operation, effects, success):
    if not success:
        return before.allowances
    return tuple(settled_allowance(value, operation, effects) for value in before.allowances)


def settled_allowance(value, operation, effects):
    amount = value.amount
    if isinstance(operation, ops.Approval) and value.asset == operation.asset:
        amount = operation.amount
    elif amount != 2**256 - 1:
        amount -= allowance_spent(operation, effects, value.asset)
    if amount < 0:
        raise DataError("receipt", "settlement exceeds its retained allowance")
    return type(value)(asset=value.asset, spender=value.spender, amount=amount)


def validate_balances(pending, after, effects, gas, success):
    before = pending.before
    if after.position != after.owned.position:
        raise DataError("ownership", "observed LP checkpoint differs from reconciled ownership")
    if after.allowances != allowances_after(before, pending.operation, effects, success):
        raise DataError("receipt", "ERC-20 allowance differs from executed settlement")
    if before.payer != after.payer or before.native_balance.asset != after.native_balance.asset:
        raise DataError("receipt", "native gas payer scope changed across execution")
    return wallet_movement_issues(before, after, effects, gas)


def wallet_movement_issues(before, after, effects, gas):
    issues = []
    for old, new, delta in zip(
        (before.wallet0, before.wallet1),
        (after.wallet0, after.wallet1),
        (effects.delta0, effects.delta1),
        strict=True,
    ):
        if new.amount != old.amount + delta:
            issues.append(f"{old.asset}: wallet includes unexplained external movements")
    if after.native_balance.amount != before.native_balance.amount - gas:
        issues.append("gas payer balance includes unexplained external movements")
    return tuple(issues)


def reconcile(backend, pending, receipt, block):
    from almanak.connectors.aerodrome.state import Outcome

    op = pending.operation
    success = receipt["status"] == "0x1"
    pool = read_pool(backend.reader, op.target.pool, block)
    effects = movement(receipt, op, pool.pool_fees) if success else Effects(delta0=0, delta1=0)
    current = stored_position(backend.reader, op.target, pool)
    position = retained_position(pending.before, op, effects, current, pool, success)
    owned = owned_after(pending.before, op, effects, position, success)
    # An exit leg measures what counter inventory remains so the exit can complete
    # with residual dust; other operations record no quotes.
    exiting = pending.decision.intent.kind == "exit_exchange"
    after = snapshot(backend, op.allocation, block, owned, quotes=False, residual=exiting)
    gas_paid, fee_evidence = charged_fees(backend, pending, receipt, block)
    issues = validate_balances(pending, after, effects, gas_paid.amount, success)
    if success and isinstance(op, ops.Addition) and effects.shares_delta < op.minimum_shares.amount:
        issues += ("mined LP shares fall below the retained simulation minimum",)
    return Outcome(
        decision=pending.decision,
        operation=op,
        tx_hash=pending.tx_hash,
        nonce=pending.nonce,
        before=pending.before,
        after=after,
        success=success,
        delta0=effects.delta0,
        delta1=effects.delta1,
        shares_delta=effects.shares_delta,
        fees0=effects.fees0,
        fees1=effects.fees1,
        gas_paid=gas_paid,
        receipt=receipt,
        op_fee_evidence=fee_evidence,
        call=pending.call,
        execution=pending.execution,
        accounting_issues=issues,
    )
