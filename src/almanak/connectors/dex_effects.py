"""Receipt-derived exchange pool cash effects belong to the fund's shared inventory."""

from almanak.effects import AssetDelta, WalletAsset


def effects(outcome):
    if not outcome.success:
        return ()
    target = outcome.operation.target
    return tuple(
        AssetDelta(
            key=WalletAsset(account=target.account, chain=target.fund.chain, asset=asset),
            amount=delta,
        )
        for asset, delta in zip(target.assets, (outcome.delta0, outcome.delta1), strict=True)
    )
