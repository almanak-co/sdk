"""Only canonical keeper events convert accepted request escrow into economic position effects."""

from almanak.connectors.gmx_v2 import funding
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.observations import snapshot
from almanak.connectors.gmx_v2.operations import Approval, CancelOrder, ClaimFunding, CreateOrder
from almanak.connectors.gmx_v2.position_reader import position_key
from almanak.connectors.gmx_v2.receipts import (
    accepted_request,
    check_values,
    event_evidence,
    execution_fee,
    one_protocol_event,
    remaining_fee,
    token_movement,
)
from almanak.connectors.gmx_v2.state import RequestOutcome, TransactionOutcome
from almanak.effects import AssetDelta, WalletAsset
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees
from almanak.models import AssetAmount


def position_effect(request, before, after, receipt):
    if request.status != "executed":
        if after.position != before.owned.position:
            raise DataError("ownership", "unexecuted GMX request cannot change position ownership")
        return before.owned.position
    target = before.target
    kind = request.events[0].event().value("uint256", "orderType")
    name, sender = (
        ("PositionIncrease", target.market.contracts.increase_order_executor)
        if kind == 2
        else ("PositionDecrease", target.market.contracts.decrease_order_executor)
    )
    event = one_protocol_event(receipt, target, name, sender)
    check_values(
        event,
        "address",
        {
            "account": target.fund.address,
            "market": target.market.share_asset.reference,
            "collateralToken": target.collateral_asset.reference,
        },
    )
    check_values(
        event,
        "bytes32",
        {"orderKey": request.key, "positionKey": "0x" + position_key(target).hex()},
    )
    check_values(event, "bool", {"isLong": target.side == "long"})
    position = after.position
    expected = (
        (0, 0, 0)
        if position is None
        else (position.size_usd_e30, position.index_amount.amount, position.collateral.amount)
    )
    check_values(
        event,
        "uint256",
        dict(zip(("sizeInUsd", "sizeInTokens", "collateralAmount"), expected, strict=True)),
    )
    return position


def cash_effects(receipt, target, inventory, native):
    return tuple(
        AssetDelta(
            key=WalletAsset(account=target.account, chain=target.fund.chain, asset=value.asset),
            amount=native
            if value.asset.namespace == "slip44"
            else token_movement(receipt, target, value.asset),
        )
        for value in inventory
    )


def changed_ownership(before, assets, position):
    inventory = tuple(
        AssetAmount(asset=value.asset, amount=value.amount + delta.amount)
        for value, delta in zip(before.owned.inventory, assets, strict=True)
    )
    return before.owned.model_copy(update={"inventory": inventory, "position": position})


def validate_wallets(before, after, assets, gas):
    issues = []
    for old, new, delta in zip(before.wallets, after.wallets, assets, strict=True):
        payer_charge = (
            gas if old.asset.namespace == "slip44" and before.payer == before.target.fund else 0
        )
        if new.amount != old.amount + delta.amount - payer_charge:
            issues.append(f"{old.asset}: fund wallet includes unexplained external movements")
    native_received = sum(d.amount for d in assets if d.key.asset.namespace == "slip44")
    payer_credit = native_received if before.payer == before.target.fund else 0
    if after.native_balance.amount != before.native_balance.amount + payer_credit - gas:
        issues.append("GMX submitter balance includes unexplained external movements")
    return tuple(issues)


def completed_request(pending, receipt, block):
    operation = pending.operation
    if isinstance(operation, CreateOrder):
        return accepted_request(pending, receipt, block)
    if not isinstance(operation, CancelOrder):
        return None
    previous = pending.before.request
    if previous is None or previous.key != operation.request_key:
        raise ValueError("GMX cancellation receipt lacks its admitted request identity")
    event = one_protocol_event(
        receipt, operation.target, "OrderCancelled", operation.target.market.contracts.order_handler
    )
    return previous.append(event_evidence(event, block, receipt, "cancelled"))


def terminal_fee(receipt, target, request, before_request):
    if request is None or request.status == "accepted":
        return 0
    if before_request is None:
        raise ValueError("GMX terminal fee accounting requires retained prior request escrow")
    return execution_fee(
        receipt,
        target,
        remaining_fee(before_request, target),
        handler=request.events[0].identity.handler,
    ).fund_received


def transaction_effects(pending, receipt, after, request, success):
    before, target = pending.before, pending.operation.target
    if not success:
        return (), before.owned
    native = terminal_fee(receipt, target, request, before.request)
    assets = cash_effects(receipt, target, before.owned.inventory, native)
    position = (
        before.owned.position
        if request is None
        else position_effect(request, before, after, receipt)
    )
    owned = changed_ownership(before, assets, position)
    validate_transaction_effects(pending.operation, before, after, assets)
    if isinstance(pending.operation, ClaimFunding):
        funding.validate_claim(pending.operation, before, after, receipt, assets)
        owned = owned.model_copy(update={"claimable_funding": ()})
    return assets, owned


def validate_transaction_effects(operation, before, after, assets):
    if isinstance(operation, Approval):
        if after.allowance != operation.amount.amount or any(a.amount for a in assets):
            raise DataError("receipt", "GMX approval does not match its admitted allowance")
        return
    if isinstance(operation, CreateOrder):
        amount = operation.execution_fee.amount + (
            operation.collateral_delta.amount if operation.order_type == 2 else 0
        )
        if assets[0].amount != -amount or after.allowance != before.allowance - amount:
            raise DataError("receipt", "GMX request did not transfer its exact admitted escrow")


def reconcile(backend, pending, receipt, block):
    if isinstance(pending.operation.target, ExchangeTarget):
        from almanak.connectors.gmx_v2.exchange_reconciliation import (
            reconcile as reconcile_exchange,
        )

        return reconcile_exchange(backend, pending, receipt, block)
    return _reconcile_position(backend, pending, receipt, block)


def _reconcile_position(backend, pending, receipt, block):
    success = receipt["status"] == "0x1"
    request = completed_request(pending, receipt, block) if success else pending.before.request
    after = snapshot(backend, pending.operation.allocation, block, pending.before.owned)
    assets, owned = transaction_effects(pending, receipt, after, request, success)
    after = after.model_copy(update={"owned": owned})
    gas_paid, fee_evidence = charged_fees(backend, pending, receipt, block)
    measured = assets or cash_effects(receipt, pending.operation.target, owned.inventory, 0)
    issues = validate_wallets(pending.before, after, measured, gas_paid.amount)
    expected_claims = _expected_funding_claims(pending, success)
    issues += funding.observed_issues(expected_claims, after)
    return TransactionOutcome(
        decision=pending.decision,
        operation=pending.operation,
        nonce=pending.nonce,
        before=pending.before,
        after=after,
        success=success,
        request=request,
        assets=assets,
        tx_hash=pending.tx_hash,
        receipt=receipt,
        call=pending.call,
        execution=pending.execution,
        accounting_issues=issues,
        gas_paid=gas_paid,
        op_fee_evidence=fee_evidence,
    )


def _expected_funding_claims(pending, success):
    cleared = success and isinstance(pending.operation, ClaimFunding)
    return {
        value.asset: 0 if cleared else value.amount for value in pending.before.claimable_funding
    }


def _retained_request_outcome(state, request, message):
    prior = next(
        (
            outcome
            for outcome in reversed(state.outcomes)
            if outcome.tx_hash == request.events[-1].identity.transaction_hash
        ),
        None,
    )
    if prior is None or prior.request != request:
        raise ValueError(message)
    return prior


def reconcile_request(backend, state, request, evidence):
    if isinstance(state.config.target(request.allocation), ExchangeTarget):
        from almanak.connectors.gmx_v2.exchange_reconciliation import (
            reconcile_request as reconcile_exchange_request,
        )

        return reconcile_exchange_request(backend, state, request, evidence)
    return _reconcile_position_request(backend, state, request, evidence)


def _reconcile_position_request(backend, state, request, evidence):
    updated = request.append(evidence)
    prior = _retained_request_outcome(
        state, request, "GMX keeper reconciliation lacks the retained preceding request outcome"
    )
    from almanak.connectors.gmx_v2.liquidation_reader import financial_before

    before = financial_before(state, prior.after.target)
    if before is None or (
        before != prior.after and before.block.number >= evidence.identity.block.number
    ):
        raise DataError(
            "ordering", "GMX request event cannot precede its latest financial reference"
        )
    after = snapshot(backend, request.allocation, evidence.identity.block, before.owned)
    native = terminal_fee(evidence.receipt, before.target, updated, request)
    assets = cash_effects(evidence.receipt, before.target, before.owned.inventory, native)
    position = position_effect(updated, before, after, evidence.receipt)
    claims, funding_issues = funding.accrue(before, after, evidence.receipt, updated)
    owned = changed_ownership(before, assets, position).model_copy(
        update={"claimable_funding": claims}
    )
    after = after.model_copy(update={"owned": owned})
    return RequestOutcome(
        decision=state.decision,
        operation=prior.operation,
        before=before,
        after=after,
        tx_hash=evidence.identity.transaction_hash,
        receipt=evidence.receipt,
        success=True,
        request=updated,
        assets=assets,
        accounting_issues=validate_wallets(before, after, assets, 0) + funding_issues,
    )
