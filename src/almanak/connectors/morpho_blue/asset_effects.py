"""Morpho receipt cash effects retain fund inventory separately from payer gas."""

from almanak.connectors.morpho_blue.receipts import wallet_direction
from almanak.effects import AssetDelta, WalletAsset


def effects(outcome):
    if not outcome.success or not outcome.assets:
        return ()
    target = outcome.operation.target
    return (
        AssetDelta(
            key=WalletAsset(account=target.account, chain=target.fund.chain, asset=target.asset),
            amount=wallet_direction(outcome.operation) * outcome.assets,
        ),
    )
