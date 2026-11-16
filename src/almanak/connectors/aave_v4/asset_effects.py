"""Receipt-derived Aave V4 token effects, separate from submitter gas and external transfers."""

from almanak.connectors.aave_v4.execution import movement_direction
from almanak.effects import AssetDelta, WalletAsset


def effects(outcome):
    if not outcome.success:
        return ()
    target = outcome.operation.target
    return (
        AssetDelta(
            key=WalletAsset(account=target.account, chain=target.fund.chain, asset=target.asset),
            amount=-movement_direction(outcome.operation) * outcome.assets,
        ),
    )
