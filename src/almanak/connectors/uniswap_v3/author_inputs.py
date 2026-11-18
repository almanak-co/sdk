"""Unresolved V3 economic choices, with exact named market/account references.

The connector's `authoring` module holds the strategy-facing intent helpers, so
these public author inputs live beside them under their own name.
"""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.configuration.input_values import AmountInput, PositiveDuration, Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.entry_swap import CONCENTRATED_ENTRY_SWAP_BPS_DESCRIPTION, EntrySwapBps
from almanak.connectors.uniswap_v3.models import Allocation
from almanak.connectors.uniswap_v3.resolution import ResolvedPool
from almanak.connectors.v3_pool_author_inputs import BandInput, validate_shape_inputs
from almanak.connectors.v3_pool_domain import Tick
from almanak.gateway.evm import native_metadata
from almanak.models import Asset, Model, UInt
from almanak.triggers import HOLD_SECONDS_DESCRIPTION


class RangeInput(Model):
    lower: Tick
    upper: Tick


class AllocationInput(PositionInput):
    connector: Literal["uniswap_v3"] = Field(default="uniswap_v3", exclude=True)
    # Neither range nor band is a trading allocation; liquidity requires exactly one.
    range: RangeInput | None = None
    band: BandInput | None = None
    capital: AmountInput
    entry_swap_bps: Annotated[
        EntrySwapBps, Field(description=CONCENTRATED_ENTRY_SWAP_BPS_DESCRIPTION)
    ]
    hold_seconds: PositiveDuration | None = Field(
        default=None, description=HOLD_SECONDS_DESCRIPTION
    )
    slippage_bps: Annotated[UInt, Field(lt=10000)]
    deadline_seconds: PositiveDuration
    minimum_native_balance: Quantity

    @model_validator(mode="after")
    def one_shape(self):
        validate_shape_inputs(self.range, self.band)
        return self

    def resolve(self, name: str, market: ResolvedPool) -> Allocation:
        identity, native = native_metadata(market.selection.chain)
        result = self.model_dump(exclude={"market"})
        result.update(
            name=name,
            pool=market.pool,
            contracts=market.contracts,
            band=self.band.resolve(market.pool.tick_spacing) if self.band else None,
            native_asset=identity,
            capital=funding(self.capital, market),
            minimum_native_balance=converted(
                self.minimum_native_balance, native.decimals, "minimum_native_balance"
            ),
        )
        return Allocation.model_validate(result)


def funding(value: AmountInput, market: ResolvedPool) -> dict:
    for asset in market.assets:
        if value.asset == asset.identifier:
            return amount(value, asset, "capital")
    raise ConfigurationError(
        "capital must name one of the resolved pool currencies", field="capital.asset"
    )


def amount(value: AmountInput, asset: Asset, field: str) -> dict:
    return {
        "asset": value.asset,
        "amount": converted(value.quantity, asset.decimals, f"{field}.quantity"),
    }
