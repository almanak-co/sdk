"""Funding entitlements come from owned request events, not unexplained fund-level balances."""

from almanak.connectors.gmx_v2.receipts import check_values, protocol_events
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount


def asset_for(target, address):
    assets = (target.market.long_asset, target.market.short_asset)
    return next((asset for asset in assets if asset.reference == address), None)


def owned_amounts(before):
    retained = {value.asset: value.amount for value in before.owned.claimable_funding}
    return tuple(
        AssetAmount(asset=value.asset, amount=retained.get(value.asset, 0))
        for value in before.claimable_funding
    )


def account_events(receipt, target, name):
    return tuple(
        event
        for event in protocol_events(receipt, target)
        if event.name == name and event.value("address", "account") == target.fund.address
    )


def validate_target(event, target, sender):
    asset = asset_for(target, event.value("address", "token"))
    if event.sender != sender or asset is None:
        raise DataError("receipt", "GMX funding event differs from its verified contract or asset")
    check_values(
        event,
        "address",
        {"market": target.market.share_asset.reference, "account": target.fund.address},
    )
    return asset


def accrue(before, after, receipt, request):
    target = before.target
    sender = (
        target.market.contracts.increase_order_executor
        if request.events[0].event().value("uint256", "orderType") == 2
        else target.market.contracts.decrease_order_executor
    )
    events = account_events(receipt, target, "ClaimableFundingUpdated")
    if request.status != "executed" and events:
        raise DataError("receipt", "unexecuted request cannot create owned funding entitlements")
    owned = {value.asset: value.amount for value in owned_amounts(before)}
    observed = {value.asset: value.amount for value in before.claimable_funding}
    for event in events:
        asset = validate_target(event, target, sender)
        delta = event.value("uint256", "delta")
        owned[asset] += delta
        observed[asset] += delta
        if observed[asset] != event.value("uint256", "nextValue"):
            raise DataError("receipt", "GMX funding update does not reconcile with prior evidence")
    return retained_claims(owned), observed_issues(observed, after)


def retained_claims(amounts):
    return tuple(
        AssetAmount(asset=asset, amount=amount) for asset, amount in amounts.items() if amount
    )


def observed_issues(expected, after):
    actual = {value.asset: value.amount for value in after.claimable_funding}
    return (
        ("GMX claimable funding includes unexplained external changes",)
        if actual != expected
        else ()
    )


def claimable(before):
    if before.position is not None:
        raise ValueError("GMX funding cleanup requires full position closure first")
    if before.claimable_funding != owned_amounts(before):
        raise DataError("ownership", "GMX claim would include unowned funding entitlements")
    return before.claimable_funding


def validate_claim(operation, before, after, receipt, assets):
    events = account_events(receipt, operation.target, "FundingFeesClaimed")
    if len(events) != len(operation.amounts):
        raise DataError("receipt", "GMX funding claim requires one event per configured asset")
    for amount in operation.amounts:
        validate_claim_asset(operation.target, amount, events, assets)
    if after.allowance != before.allowance or after.position != before.position:
        raise DataError("receipt", "GMX funding claim changed unrelated position or allowance")
    if any(value.amount for value in after.claimable_funding):
        raise DataError("ownership", "GMX funding claim left an unexplained remaining entitlement")


def validate_claim_asset(target, amount, events, assets):
    selected = [
        event for event in events if event.value("address", "token") == amount.asset.reference
    ]
    if len(selected) != 1:
        raise DataError("receipt", "GMX funding claim duplicated or omitted its asset")
    event = selected[0]
    validate_target(event, target, target.market.contracts.exchange_router)
    check_values(event, "address", {"receiver": target.fund.address})
    check_values(event, "uint256", {"amount": amount.amount})
    delta = next(value.amount for value in assets if value.key.asset == amount.asset)
    if delta != amount.amount:
        raise DataError("receipt", "GMX funding event differs from measured fund token receipts")
