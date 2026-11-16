"""GMX composition keeps protocol allowances separate from shared fund cash views."""

from almanak.connectors.gmx_v2.economic_perpetual_state import (
    EconomicPerpetualPosition,
    PerpetualOwnership,
)
from almanak.connectors.gmx_v2.economic_spending import spending_assets
from almanak.connectors.gmx_v2.economic_state import EconomicPosition
from almanak.connectors.gmx_v2.exchange_observations import allowance_assets, initial_owned
from almanak.connectors.gmx_v2.models import Allocation
from almanak.models import AssetAmount


def initial_position(config, allocation, owned=None):
    if isinstance(allocation, Allocation):
        from almanak.connectors.gmx_v2.economic_perpetual_component import (
            initial_position as initial_perpetual,
        )

        return initial_perpetual(config, allocation, owned)
    target = config.target(allocation.name)
    ownership = initial_owned(config, allocation.name) if owned is None else owned
    if ownership.target != target:
        raise ValueError("GMX initial economic ownership differs from its configured target")
    return EconomicPosition(
        target=target,
        owned=ownership,
        spent=tuple(AssetAmount(asset=asset, amount=0) for asset in spending_assets(allocation)),
        allowances=tuple(AssetAmount(asset=asset, amount=0) for asset in allowance_assets(target)),
        escrow=None,
    )


def protocol_closed(payload):
    if isinstance(payload, EconomicPerpetualPosition):
        from almanak.connectors.gmx_v2.economic_perpetual_component import (
            protocol_closed as perpetual_closed,
        )

        return perpetual_closed(payload)
    return (
        payload.escrow is None
        and payload.owned.market_tokens.amount == 0
        and not any(value.amount for value in payload.allowances)
    )


def owned_cash(owned):
    return owned.inventory


def observed_cash(observed):
    if isinstance(observed.owned, PerpetualOwnership):
        return observed.wallets
    return (*observed.wallets, observed.market_tokens)
