"""Receipt-driven ownership survives missing fee measurements and process replacement."""

from almanak.connectors.uniswap_v4.hooks import require_entry_code
from almanak.connectors.uniswap_v4.math import accrued
from almanak.connectors.uniswap_v4.observations import snapshot
from almanak.connectors.uniswap_v4.reader import position_growth
from almanak.connectors.uniswap_v4.receipts import (
    fund_movement,
    fund_spend,
    movement,
    nft_transition,
)
from almanak.connectors.uniswap_v4.state import (
    Approval,
    Burn,
    Collect,
    LiquidityPosition,
    Mint,
    Outcome,
    Swap,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import ZERO_ADDRESS, AssetAmount


def retained_growth(backend, operation, block, nft, success):
    if not success or not isinstance(operation, (Mint, Collect, Burn)):
        return None, ()
    identity = nft if isinstance(operation, Mint) else operation.nft
    try:
        liquidity, growth = position_growth(
            backend.reader, operation.target, int(identity.token_id), block, operation.range
        )
    except DataError as exc:
        return None, (f"position fee checkpoint unavailable: {exc.kind}",)
    expected = (
        0
        if isinstance(operation, Burn)
        else operation.liquidity
        if isinstance(operation, Mint)
        else None
    )
    if expected is not None and liquidity != expected:
        raise DataError("ownership", "PoolManager position liquidity differs from the receipt")
    return growth, ()


def owned_after(pending, deltas, nft, growth, success):
    before = pending.before.owned
    if not success:
        return before
    inventory = (before.inventory0, before.inventory1)
    amounts = tuple(
        AssetAmount(asset=a.asset, amount=a.amount + delta)
        for a, delta in zip(inventory, deltas, strict=True)
    )
    operation = pending.operation
    position = before.position
    entered = before.entry_complete
    if isinstance(operation, Mint):
        position = LiquidityPosition(
            nft=nft, liquidity=operation.liquidity, fee_growth=growth, range=operation.range
        )
        entered = True
    if isinstance(operation, Collect):
        position = position.model_copy(update={"fee_growth": growth})
    if isinstance(operation, Burn):
        position = None
    if isinstance(operation, Swap) and operation.amount.asset == operation.target.funding_currency:
        entered = True
    return before.model_copy(
        update={
            "inventory0": amounts[0],
            "inventory1": amounts[1],
            "position": position,
            "entry_complete": entered,
        }
    )


def allowance_after(before, operation, deltas, timestamp):
    result = []
    for previous in before.allowances:
        current = previous
        if isinstance(operation, Approval):
            if previous.asset == operation.asset and previous.spender == operation.spender:
                update = {"amount": operation.amount}
                if operation.expiration is not None:
                    # Permit2 encodes zero expiry as the execution block timestamp.
                    update["expiration"] = operation.expiration or timestamp
                current = previous.model_copy(update=update)
        else:
            index = operation.target.assets.index(previous.asset)
            spender = (
                operation.target.contracts.router
                if isinstance(operation, Swap)
                else operation.target.contracts.position_manager
            )
            if deltas[index] < 0 and (previous.expiration is None or previous.spender == spender):
                current = previous.model_copy(update={"amount": previous.amount + deltas[index]})
        result.append(current)
    return tuple(result)


def validate_balances(pending, after, deltas, gas, success, spent=None):
    before = pending.before
    expected = (
        allowance_after(
            before, pending.operation, deltas if spent is None else spent, after.block.timestamp
        )
        if success
        else before.allowances
    )
    if after.allowances != expected:
        raise DataError("receipt", "ERC-20 or Permit2 allowance does not reconcile")
    issues = []
    for old, new, delta in zip(
        (before.wallet0, before.wallet1), (after.wallet0, after.wallet1), deltas, strict=True
    ):
        if new.amount != old.amount + delta:
            issues.append(f"{old.asset}: wallet includes unexplained external movements")
    if after.native_balance.asset != before.native_balance.asset or after.payer != before.payer:
        raise DataError("receipt", "gas payer identity changed across execution")
    if after.native_balance.amount != before.native_balance.amount - gas:
        issues.append("gas payer balance includes unexplained external movements")
    return tuple(issues)


def fees(pending, deltas, growth, success):
    if not success or not isinstance(pending.operation, (Collect, Burn)):
        return (0, 0), ()
    position = pending.before.owned.position
    # A native leg's sweep also returns whatever already sat in the PositionManager, so its
    # proceeds may exceed the position's fees: the excess stays in the fund's deltas and out
    # of the earned fees, without an issue, so a donation cannot hold the collect back.
    native = pending.operation.target.native_index
    if growth is None or position.fee_growth is None:
        if isinstance(pending.operation, Collect):
            # A zero-liquidity modification transfers only fees. Token events
            # remain independent evidence even when the historical growth read fails.
            if native is None:
                return deltas, ()
            return tuple(None if i == native else d for i, d in enumerate(deltas)), ()
        return (None, None), ("close proceeds are known; principal/fee split is unmeasured",)
    measured = tuple(
        accrued(position.liquidity, old, new)
        for old, new in zip(position.fee_growth, growth, strict=True)
    )
    if any(fee > total for fee, total in zip(measured, deltas, strict=True)):
        return (None, None), ("fee growth exceeds observed exit proceeds",)
    if (
        isinstance(pending.operation, Collect)
        and measured != deltas
        and (native is None or measured[1 - native] != deltas[1 - native])
    ):
        return deltas, ("collected token fees differ from measured fee growth",)
    return measured, ()


def receipt_movement(backend, pending, receipt, block):
    """Fund deltas, NFT, measured haircut and issues of a mined receipt.

    Hook code that differs from its pin at the receipt block was admitted for an exit
    only, or changed after an entry was sent. The operation is always recorded, from the
    fund's own transfers when the receipt no longer fits the reviewed model, with the
    NFT the PositionManager moved, and the changed code stays visible.
    """
    if receipt["status"] != "0x1":
        return (0, 0), None, None, ()
    operation = pending.operation
    pool = operation.target.pool
    # Approvals and revokes never call the hook, so its code says nothing about them.
    if (
        pool.hooks == ZERO_ADDRESS
        or isinstance(operation, Approval)
        or require_entry_code(AuthorityReader(backend.reader, block), pool, entering=False)
    ):
        return (*movement(receipt, operation), ())
    changed = f"executed through Uniswap V4 hook {pool.hooks} code that differs from its pin"
    try:
        return (*movement(receipt, operation), (changed,))
    except DataError as error:
        issue = f"{changed}; receipt differs from the reviewed model: {error}"
        # The PositionManager, not the hook, mints and burns the NFT.
        return (
            fund_movement(receipt, operation.target),
            nft_transition(receipt, operation),
            None,
            (issue,),
        )


def hook_issues(pending, measured):
    """An output-haircut take must equal the reviewed per-pool rates at the swap.

    A differing or unmeasured take stays visible and stops new exposure; it never
    blocks recording the swap, so an exit through a changed hook still completes.
    """
    if measured is None:
        return ()
    take, gross = measured
    terms = pending.before.hook_terms
    if terms is None:
        return (f"hook take {take} is unverified: hook terms were not measured",)
    expected = terms.take(gross)
    if take != expected:
        return (f"hook take {take} differs from the reviewed haircut {expected}",)
    return ()


def reconcile(backend, pending, receipt, block):
    success = receipt["status"] == "0x1"
    # Permit2 allowances lose what was pulled from the fund: a swap's whole input even when a
    # hook refunds part of it, and a native mint's whole native maximum even though the
    # PositionManager sweeps back what the position did not take.
    gross = isinstance(pending.operation, Swap) or (
        isinstance(pending.operation, Mint) and pending.operation.target.native_index is not None
    )
    deltas, nft, measured, hook_notes = receipt_movement(backend, pending, receipt, block)
    growth, growth_issues = retained_growth(backend, pending.operation, block, nft, success)
    owned = owned_after(pending, deltas, nft, growth, success)
    # An exit leg measures what counter inventory remains so the exit can complete
    # with residual dust; other operations record no quotes.
    exiting = pending.decision.intent.kind == "exit_exchange"
    after = snapshot(
        backend, pending.operation.allocation, block, owned, quotes=False, residual=exiting
    )
    if after.position_liquidity != (owned.position.liquidity if owned.position else 0):
        raise DataError("ownership", "receipt NFT does not have its retained liquidity")
    gas_paid, fee_evidence = charged_fees(backend, pending, receipt, block)
    gas = gas_paid.amount
    spent = fund_spend(receipt, pending.operation.target) if success and gross else None
    issues = validate_balances(pending, after, deltas, gas, success, spent)
    earned, fee_issues = fees(pending, deltas, growth, success)
    return Outcome(
        decision=pending.decision,
        operation=pending.operation,
        tx_hash=pending.tx_hash,
        nonce=pending.nonce,
        before=pending.before,
        after=after,
        success=success,
        delta0=deltas[0],
        delta1=deltas[1],
        fees0=earned[0],
        fees1=earned[1],
        gas_paid=gas_paid,
        op_fee_evidence=fee_evidence,
        receipt=receipt,
        call=pending.call,
        execution=pending.execution,
        accounting_issues=(
            *issues,
            *growth_issues,
            *fee_issues,
            *hook_notes,
            *hook_issues(pending, measured),
        ),
    )
