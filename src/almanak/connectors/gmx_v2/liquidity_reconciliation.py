"""GM settlement commits fund currency effects and independently measured share ownership."""

from almanak.connectors.gmx_v2.exchange_observations import snapshot
from almanak.connectors.gmx_v2.liquidity_receipts import (
    economic_operation,
    share_effect,
    validate_cash,
)
from almanak.connectors.gmx_v2.reconciliation import (
    _retained_request_outcome,
    cash_effects,
    terminal_fee,
    validate_wallets,
)
from almanak.effects import AssetDelta, WalletAsset
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount


def effects(before, after, decision, request, receipt):
    from almanak.connectors.gmx_v2.exchange_reconciliation import changed_ownership

    operation = economic_operation(decision, request)
    native = terminal_fee(receipt, before.target, request, before.request)
    cash = cash_effects(receipt, before.target, before.owned.inventory, native)
    validate_cash(operation, request, receipt, cash)
    change = share_effect(operation, request, receipt)
    shares = AssetAmount(
        asset=before.target.market.share_asset, amount=before.owned.market_tokens.amount + change
    )
    if after.market_tokens != shares:
        raise DataError(
            "ownership", "observed GM shares differ from retained request mint, escrow or refund"
        )
    owned = changed_ownership(before, cash).model_copy(update={"market_tokens": shares})
    delta = AssetDelta(
        key=WalletAsset(
            account=before.target.account, chain=before.target.fund.chain, asset=shares.asset
        ),
        amount=change,
    )
    return (*cash, delta), owned


def transaction_effects(pending, receipt, after, request):
    from almanak.connectors.gmx_v2.exchange_reconciliation import validate_allowances

    validate_allowances(pending.operation, pending.before, after)
    return effects(pending.before, after, pending.decision, request, receipt)


def reconcile_request(backend, state, request, evidence):
    from almanak.connectors.gmx_v2.state import RequestOutcome

    prior = _retained_request_outcome(
        state, request, "GM keeper reconciliation lacks its retained preceding request outcome"
    )
    before = prior.after.model_copy(update={"request": request})
    after = snapshot(backend, request.allocation, evidence.identity.block, before.owned)
    if after.allowances != before.allowances:
        raise DataError("ownership", "GM keeper settlement cannot change fund token allowances")
    updated = request.append(evidence)
    assets, owned = effects(before, after, state.decision, updated, evidence.receipt)
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
        accounting_issues=validate_wallets(before, after, assets[: len(before.wallets)], 0),
    )
