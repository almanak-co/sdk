"""Unresolved Aerodrome economic choices, with exact named market/account references.

The connector's `authoring` module holds the strategy-facing intent helpers, so
these public author inputs live beside them under their own name. The pool
invariant comes from the chain, never from the author, so a position states only
its economics.
"""

from typing import Annotated, Literal

from pydantic import Field

from almanak.configuration.input_values import AmountInput, PositiveDuration, Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.aerodrome.models import Allocation
from almanak.connectors.aerodrome.resolution import ResolvedPool
from almanak.connectors.entry_swap import ENTRY_SWAP_BPS_DESCRIPTION, EntrySwapBps
from almanak.gateway.evm import native_metadata
from almanak.models import Asset, UInt
from almanak.triggers import HOLD_SECONDS_DESCRIPTION


class AllocationInput(PositionInput):
    connector: Literal["aerodrome"] = Field(default="aerodrome", exclude=True)
    liquidity_enabled: bool
    capital: AmountInput
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
            contracts=market.contracts,
            native_asset=identity,
            capital=funding(self.capital, market),
            minimum_native_balance=converted(
                self.minimum_native_balance, native.decimals, "minimum_native_balance"
            ),
        )
        return Allocation.model_validate(result)


def funding(value: AmountInput, market: ResolvedPool) -> dict:
    for asset in market.currencies:
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
