"""Receipt-derived Pool cash effects, excluding the submitter's separately retained gas."""

from almanak.effects import AssetDelta, WalletAsset


def effects(outcome):
    if not outcome.success:
        return ()
    target = outcome.operation.target
    direction = {
        "supply": -1,
        "withdraw": 1,
        "borrow": 1,
        "repay": -1,
        "approve": 0,
        "revoke": 0,
        "collateral": 0,
    }[outcome.operation.kind]
    return (
        AssetDelta(
            key=WalletAsset(account=target.account, chain=target.fund.chain, asset=target.asset),
            amount=direction * outcome.assets,
        ),
    )
