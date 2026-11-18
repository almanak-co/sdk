"""Retain actual fund effects and explicit settlement shortfalls across recovery."""

from almanak.connectors.v3_pool_operations import (
    Approval,
    Close,
    Collect,
    Decrease,
    Increase,
    Mint,
    Swap,
)
from almanak.connectors.v3_pool_reader import nft_position
from almanak.connectors.v3_pool_receipts import Effects
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees
from almanak.models import AssetAmount


def retained_position(backend, pending, effects, block, success, *, position_reader=nft_position):
    old, operation = pending.before.owned.position, pending.operation
    if not success or isinstance(operation, (Approval, Swap)):
        return old
    if isinstance(operation, Close):
        return None
    return reconciled_liquidity(
        backend, old, operation, effects, block, position_reader=position_reader
    )


def reconciled_liquidity(backend, old, operation, effects, block, *, position_reader=nft_position):
    nft = effects.nft if isinstance(operation, Mint) else old.nft
    current = position_reader(
        backend.reader, operation.target, nft, block, expected=operation.range
    )
    expected = (old.liquidity if old else 0) + effects.liquidity_delta
    if current.liquidity != expected:
        raise DataError("ownership", "observed NFT liquidity differs from retained receipt effects")
    return current


def owned_after(pending, effects, position, success):
    before = pending.before.owned
    if not success:
        return before
    inventory = tuple(
        AssetAmount(asset=old.asset, amount=old.amount + delta)
        for old, delta in zip(
            (before.inventory0, before.inventory1), (effects.delta0, effects.delta1), strict=True
        )
    )
    operation = pending.operation
    entered = (
        before.entry_complete
        or isinstance(operation, (Mint, Increase))
        or (
            isinstance(operation, Swap)
            and operation.amount.asset == operation.target.funding_currency
        )
    )
    return type(before)(
        target=before.target,
        inventory0=inventory[0],
        inventory1=inventory[1],
        position=position,
        entry_complete=entered,
    )


def allowances_after(before, operation, effects, success):
    if not success:
        return before.allowances
    result = []
    for value in before.allowances:
        amount = value.amount
        if isinstance(operation, Approval):
            if (value.asset, value.spender) == (operation.asset, operation.spender):
                amount = operation.amount
        elif isinstance(operation, (Swap, Mint, Increase)):
            spender = (
                operation.target.contracts.router
                if isinstance(operation, Swap)
                else operation.target.contracts.position_manager
            )
            if value.spender == spender:
                amount += min(
                    0, (effects.delta0, effects.delta1)[operation.target.assets.index(value.asset)]
                )
        if amount < 0:
            raise DataError("receipt", "settlement exceeds its retained allowance")
        result.append(type(value)(asset=value.asset, spender=value.spender, amount=amount))
    return tuple(result)


def validate_balances(pending, after, effects, gas, success):
    before = pending.before
    if after.position != after.owned.position:
        raise DataError("ownership", "observed NFT differs from reconciled ownership")
    if after.allowances != allowances_after(before, pending.operation, effects, success):
        raise DataError("receipt", "ERC-20 allowance differs from the executed settlement")
    issues = []
    for old, new, delta in zip(
        (before.wallet0, before.wallet1),
        (after.wallet0, after.wallet1),
        (effects.delta0, effects.delta1),
        strict=True,
    ):
        if new.amount != old.amount + delta:
            issues.append(f"{old.asset}: wallet includes unexplained external movements")
    if before.payer != after.payer or before.native_balance.asset != after.native_balance.asset:
        raise DataError("receipt", "native gas payer scope changed across execution")
    if after.native_balance.amount != before.native_balance.amount - gas:
        issues.append("gas payer balance includes unexplained external movements")
    return tuple(issues)


def settlement(operation, effects, success):
    if not success:
        return (0, 0), ()
    if isinstance(operation, Collect):
        return (effects.delta0, effects.delta1), ()
    if isinstance(operation, (Close, Decrease)):
        return removal_settlement(operation, effects)
    if isinstance(operation, (Mint, Increase)) and (
        effects.liquidity_delta < operation.minimum_liquidity
        or -effects.delta0 < operation.minimum0.amount
        or -effects.delta1 < operation.minimum1.amount
    ):
        return (0, 0), ("mined liquidity addition violates its retained minimum quantities",)
    return (0, 0), ()


def removal_settlement(operation, effects):
    earned, issues = [], []
    for cash, principal, minimum in zip(
        (effects.delta0, effects.delta1),
        (effects.principal0, effects.principal1),
        (operation.minimum0, operation.minimum1),
        strict=True,
    ):
        if cash < minimum.amount or principal < minimum.amount:
            issues.append(f"{minimum.asset}: mined removal falls below its retained minimum")
        # NFPM's aggregate pool position may deliver less than nominal principal.
        # Never invent cash or a negative fee to force reconciliation.
        earned.append(cash - principal if cash >= principal else None)
        if cash < principal:
            issues.append(
                f"{minimum.asset}: principal/fee split is unmeasured after collection rounding"
            )
    return tuple(earned), tuple(issues)


def reconcile(
    backend,
    pending,
    receipt,
    block,
    *,
    snapshot,
    movement,
    outcome_type,
    position_reader=nft_position,
):
    success = receipt["status"] == "0x1"
    effects = movement(receipt, pending.operation) if success else Effects(delta0=0, delta1=0)
    position = retained_position(
        backend, pending, effects, block, success, position_reader=position_reader
    )
    owned = owned_after(pending, effects, position, success)
    # An exit leg measures what counter inventory remains so the exit can complete
    # with residual dust; other operations record no quotes.
    exiting = pending.decision.intent.kind == "exit_exchange"
    after = snapshot(
        backend, pending.operation.allocation, block, owned, quotes=False, residual=exiting
    )
    gas_paid, fee_evidence = charged_fees(backend, pending, receipt, block)
    issues = validate_balances(pending, after, effects, gas_paid.amount, success)
    earned, settlement_issues = settlement(pending.operation, effects, success)
    return outcome_type(
        decision=pending.decision,
        operation=pending.operation,
        tx_hash=pending.tx_hash,
        nonce=pending.nonce,
        before=pending.before,
        after=after,
        success=success,
        delta0=effects.delta0,
        delta1=effects.delta1,
        liquidity_delta=effects.liquidity_delta,
        nft=effects.nft,
        fees0=earned[0],
        fees1=earned[1],
        gas_paid=gas_paid,
        receipt=receipt,
        op_fee_evidence=fee_evidence,
        call=pending.call,
        execution=pending.execution,
        accounting_issues=(*issues, *settlement_issues),
    )
