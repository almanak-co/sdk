"""A mined deposit removes the sent amount from the source wallet; the claim is no balance."""

from almanak.effects import AssetDelta, WalletAsset


def effects(outcome):
    if not outcome.success or not outcome.assets:
        return ()
    target = outcome.operation.target
    return (
        AssetDelta(
            key=WalletAsset(account=target.account, chain=target.fund.chain, asset=target.asset),
            amount=-outcome.assets,
        ),
    )
