"""Unresolved V4 economic choices, with exact named market/account references.

The connector's `authoring` module holds the strategy-facing intent helpers, so
these public author inputs live beside them under their own name.
"""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.configuration.input_values import AmountInput, PositiveDuration, Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.entry_swap import NO_ENTRY_SWAP, EntrySwapBps
from almanak.connectors.uniswap_v4.models import COLLECT_SECONDS_DESCRIPTION, Allocation, Range
from almanak.connectors.uniswap_v4.resolution import ResolvedPool
from almanak.connectors.v3_pool_author_inputs import BandInput, validate_shape_inputs
from almanak.gateway.evm import native_metadata
from almanak.models import Asset, UInt
from almanak.triggers import HOLD_SECONDS_DESCRIPTION

ENTRY_SWAP_BPS = (
    "Share of capital that sizes each entry quote, capped by the funding inventory held. "
    "Trading: the quote swaps that share, and entries repeat while funding remains. "
    "Liquidity: the quote swaps the part of that share the mint pairs with, and is offered "
    "only while no position is held; open_liquidity mints with all inventory held, so with "
    "the usual single entry swap a value below 10000 leaves about the rest of capital idle "
    "in the funding asset. A banded allocation that recentres through rebalance_quote, or a "
    "strategy that swaps repeatedly before minting, is not limited this way. " + NO_ENTRY_SWAP
)


class AllocationInput(PositionInput):
    connector: Literal["uniswap_v4"] = Field(default="uniswap_v4", exclude=True)
    kind: Literal["trading", "liquidity"]
    # A liquidity allocation declares exactly one of range or band; trading declares neither.
    range: Range | None = None
    band: BandInput | None = None
    capital: AmountInput
    entry_swap_bps: Annotated[EntrySwapBps, Field(description=ENTRY_SWAP_BPS)] = 10000
    hold_seconds: PositiveDuration | None = Field(
        default=None, description=HOLD_SECONDS_DESCRIPTION
    )
    collect_seconds: PositiveDuration | None = Field(
        default=None, description=COLLECT_SECONDS_DESCRIPTION
    )
    slippage_bps: Annotated[UInt, Field(lt=10000)]
    deadline_seconds: PositiveDuration
    minimum_native_balance: Quantity

    @model_validator(mode="after")
    def shape(self):
        liquidity = self.kind == "liquidity"
        validate_shape_inputs(self.range, self.band)
        if liquidity != (self.range is not None or self.band is not None):
            raise ValueError("only a liquidity allocation carries tick bounds or a band")
        if not liquidity and self.collect_seconds is not None:
            raise ValueError("only a liquidity allocation collects fees on a schedule")
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
