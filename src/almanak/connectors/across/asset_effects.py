"""Token changes of a bridge: the deposit leaves the source wallet, and the arrival or
the refund enters a wallet. The claim in between is no balance."""

from almanak.connectors.across.settlement_types import ArrivalOutcome, RefundOutcome
from almanak.effects import AssetDelta, WalletAsset


def source(target) -> WalletAsset:
    return WalletAsset(account=target.account, chain=target.fund.chain, asset=target.asset)


def destination(target) -> WalletAsset:
    return WalletAsset(
        account=target.destination_account,
        chain=target.destination_fund.chain,
        asset=target.destination_asset,
    )


def effects(outcome):
    if isinstance(outcome, ArrivalOutcome):
        return (AssetDelta(key=destination(outcome.target), amount=outcome.received.amount),)
    if isinstance(outcome, RefundOutcome):
        return (AssetDelta(key=source(outcome.target), amount=outcome.returned.amount),)
    if not outcome.success or not outcome.assets:
        return ()
    return (AssetDelta(key=source(outcome.operation.target), amount=-outcome.assets),)
