"""Initialize hypothetical perpetual ownership without importing native position claims."""

from almanak.connectors.gmx_v2.economic_perpetual_state import (
    EconomicPerpetualPosition,
    PerpetualOwnership,
    cleanup_outstanding,
)
from almanak.connectors.gmx_v2.observations import initial_owned
from almanak.connectors.gmx_v2.positions import Ownership
from almanak.models import AssetAmount


def initial_position(config, allocation, owned=None):
    target = config.target(allocation.name)
    ownership = initial_owned(config, allocation.name) if owned is None else owned
    if not isinstance(ownership, Ownership) or ownership.target != target:
        raise ValueError("GMX perpetual initialization requires its configured native ownership")
    if ownership.position is not None or any(value.amount for value in ownership.claimable_funding):
        raise ValueError(
            "GMX perpetual initialization cannot import unmodeled native positions or claims"
        )
    zero = tuple(
        AssetAmount(asset=asset, amount=0)
        for asset in (target.market.long_asset, target.market.short_asset)
    )
    return EconomicPerpetualPosition(
        target=target,
        owned=PerpetualOwnership(
            target=target,
            inventory=ownership.inventory,
            position=None,
            claimable_funding=zero,
            deferred_collateral=zero,
        ),
        spent=(AssetAmount(asset=target.market.wrapped_native, amount=0),),
        spent_notional_usd_e30=0,
        allowances=(AssetAmount(asset=target.market.wrapped_native, amount=0),),
        escrow=None,
    )


def protocol_closed(payload):
    return (
        payload.escrow is None
        and payload.owned.position is None
        and not cleanup_outstanding(payload)
    )
