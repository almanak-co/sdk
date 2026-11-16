"""Swap acceptance spends escrow once; canonical keeper events commit output or refunds."""

from almanak.connectors.gmx_v2.exchange_observations import snapshot
from almanak.connectors.gmx_v2.exchange_operations import (
    CancelExchangeOrder,
    CreateSwap,
    ExchangeApproval,
)
from almanak.connectors.gmx_v2.exchange_receipts import accepted_request, validate_cash
from almanak.connectors.gmx_v2.liquidity_operations import (
    CancelLiquidityRequest,
    CreateLiquidity,
)
from almanak.connectors.gmx_v2.receipts import event_evidence, one_protocol_event
from almanak.connectors.gmx_v2.reconciliation import (
    _retained_request_outcome,
    cash_effects,
    terminal_fee,
    validate_wallets,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees
from almanak.models import AssetAmount


def completed_request(pending, receipt, block):
    operation = pending.operation
    if isinstance(operation, (CreateLiquidity, CancelLiquidityRequest)):
        from almanak.connectors.gmx_v2.liquidity_receipts import (
            completed_request as completed_liquidity,
        )

        return completed_liquidity(pending, receipt, block)
    if isinstance(operation, CreateSwap):
        return accepted_request(pending, receipt, block)
    if isinstance(operation, CancelExchangeOrder):
        return _cancelled_request(pending, receipt, block)
    return None


def _cancelled_request(pending, receipt, block):
    operation = pending.operation
    previous = pending.before.request
    if previous is None or previous.key != operation.request_key:
        raise ValueError("GMX swap cancellation lacks its retained request identity")
    event = one_protocol_event(
        receipt, operation.target, "OrderCancelled", operation.target.market.contracts.order_handler
    )
    return previous.append(event_evidence(event, block, receipt, "cancelled"))


def economic_operation(decision, request):
    intent = decision.intent
    return CreateSwap(
        target=intent.target,
        amount=intent.amount,
        minimum_output=intent.minimum_output,
        execution_fee=request.execution_fee,
        admission_deadline=intent.deadline,
    )


def changed_ownership(before, assets):
    inventory = tuple(
        AssetAmount(asset=value.asset, amount=value.amount + delta.amount)
        for value, delta in zip(before.owned.inventory, assets, strict=True)
    )
    return before.owned.model_copy(update={"inventory": inventory})


def validate_shares(before, after):
    if after.market_tokens != before.owned.market_tokens:
        raise DataError("ownership", "GMX swap cannot adopt or consume unrelated GM shares")


def validate_allowances(operation, before, after):
    expected = {value.asset: value.amount for value in before.allowances}
    if isinstance(operation, ExchangeApproval):
        expected[operation.amount.asset] = operation.amount.amount
    elif isinstance(operation, (CreateSwap, CreateLiquidity)):
        for value in operation.inputs:
            expected[value.asset] -= value.amount
    if {value.asset: value.amount for value in after.allowances} != expected:
        raise DataError(
            "receipt", "GMX exchange receipt changed allowance beyond its admitted operation"
        )


def transaction_effects(pending, receipt, after, request, success):
    before = pending.before
    if not success:
        return (), before.owned
    return _successful_transaction_effects(pending, receipt, after, request)


def _successful_transaction_effects(pending, receipt, after, request):
    before = pending.before
    if isinstance(pending.operation, (CreateLiquidity, CancelLiquidityRequest)):
        from almanak.connectors.gmx_v2.liquidity_reconciliation import (
            transaction_effects as liquidity_effects,
        )

        return liquidity_effects(pending, receipt, after, request)
    validate_shares(before, after)
    validate_allowances(pending.operation, before, after)
    native = terminal_fee(receipt, before.target, request, before.request)
    assets = cash_effects(receipt, before.target, before.owned.inventory, native)
    _validate_exchange_cash(pending, request, receipt, assets)
    return assets, changed_ownership(before, assets)


def _validate_exchange_cash(pending, request, receipt, assets):
    if isinstance(pending.operation, ExchangeApproval):
        if any(value.amount for value in assets):
            raise DataError("receipt", "GMX exchange approval cannot change owned inventory")
        return
    if request is not None:
        validate_cash(economic_operation(pending.decision, request), request, receipt, assets)


def reconcile(backend, pending, receipt, block):
    from almanak.connectors.gmx_v2.state import TransactionOutcome

    success = receipt["status"] == "0x1"
    request = completed_request(pending, receipt, block) if success else pending.before.request
    after = snapshot(backend, pending.operation.allocation, block, pending.before.owned)
    assets, owned = transaction_effects(pending, receipt, after, request, success)
    after = after.model_copy(update={"owned": owned})
    gas_paid, fee_evidence = charged_fees(backend, pending, receipt, block)
    measured = assets or cash_effects(receipt, pending.operation.target, owned.inventory, 0)
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
        accounting_issues=validate_wallets(
            pending.before, after, measured[: len(pending.before.wallets)], gas_paid.amount
        ),
        gas_paid=gas_paid,
        op_fee_evidence=fee_evidence,
    )


def reconcile_request(backend, state, request, evidence):
    if request.kind != "order":
        from almanak.connectors.gmx_v2.liquidity_reconciliation import (
            reconcile_request as reconcile_liquidity,
        )

        return reconcile_liquidity(backend, state, request, evidence)
    return _reconcile_order_request(backend, state, request, evidence)


def _reconcile_order_request(backend, state, request, evidence):
    from almanak.connectors.gmx_v2.state import RequestOutcome

    updated = request.append(evidence)
    prior = _retained_request_outcome(
        state,
        request,
        "GMX swap keeper reconciliation lacks its retained preceding request outcome",
    )
    before = prior.after
    after = snapshot(backend, request.allocation, evidence.identity.block, before.owned)
    validate_shares(before, after)
    _validate_keeper_allowances(before, after)
    native = terminal_fee(evidence.receipt, before.target, updated, request)
    assets = cash_effects(evidence.receipt, before.target, before.owned.inventory, native)
    validate_cash(economic_operation(state.decision, request), updated, evidence.receipt, assets)
    after = after.model_copy(update={"owned": changed_ownership(before, assets)})
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
        accounting_issues=validate_wallets(before, after, assets, 0),
    )


def _validate_keeper_allowances(before, after):
    if after.allowances != before.allowances:
        raise DataError(
            "ownership", "GMX swap keeper execution cannot change fund token allowances"
        )
