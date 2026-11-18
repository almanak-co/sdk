"""Unresolved Curve economic choices, with exact named market/account references.

The connector's `authoring` module holds the strategy-facing intent helpers, so
these public author inputs live beside them under their own name. Amounts stay
author quantities until the resolved coin decimals convert them to base units.
"""

from typing import Annotated, Literal

from pydantic import Field

from almanak.caip import AssetType
from almanak.configuration.input_values import AmountInput, PositiveDuration, Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.curve.models import Allocation
from almanak.connectors.curve.resolution import ResolvedPool
from almanak.connectors.entry_swap import ENTRY_SWAP_BPS_DESCRIPTION, EntrySwapBps
from almanak.gateway.evm import native_metadata
from almanak.models import UInt
from almanak.triggers import HOLD_SECONDS_DESCRIPTION

Amounts = Annotated[tuple[AmountInput, ...], Field(min_length=2, max_length=8)]


class AllocationInput(PositionInput):
    connector: Literal["curve"] = Field(default="curve", exclude=True)
    funding_asset: AssetType
    trading_asset: AssetType
    liquidity_enabled: bool
    # An absent exit coin withdraws proportionally across the verified coins.
    single_coin_exit: AssetType | None = None
    capital: Amounts
    entry_swap_bps: Annotated[EntrySwapBps, Field(description=ENTRY_SWAP_BPS_DESCRIPTION)]
    hold_seconds: PositiveDuration | None = Field(
        default=None, description=HOLD_SECONDS_DESCRIPTION
    )
    slippage_bps: Annotated[UInt, Field(lt=10000)]
    deadline_seconds: PositiveDuration
    minimum_native_balance: Quantity

    def resolve(self, name: str, market: ResolvedPool) -> Allocation:
        identity, native = native_metadata(market.selection.chain)
        result = self.model_dump(exclude={"market"})
        result.update(
            name=name,
            pool=market.pool,
            native_asset=identity,
            capital=in_coin_order(self.capital, market, "capital"),
            minimum_native_balance=converted(
                self.minimum_native_balance, native.decimals, "minimum_native_balance"
            ),
        )
        return Allocation.model_validate(result)


def in_coin_order(values, market: ResolvedPool, field: str) -> tuple[dict, ...]:
    if tuple(value.asset for value in values) != tuple(a.identifier for a in market.coins):
        raise ConfigurationError(
            f"{field} must name every resolved pool coin in its verified order",
            field=f"{field}.asset",
        )
    return tuple(
        {
            "asset": value.asset,
            "amount": converted(value.quantity, asset.decimals, f"{field}.quantity"),
        }
        for value, asset in zip(values, market.coins, strict=True)
    )
