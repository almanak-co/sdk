"""Unresolved GMX V2 economic choices, with exact named market/account references.

The connector's `authoring` module holds the harness quantity normalizer, so
these public author inputs live beside it under their own name. One GM market
carries three position kinds: a perpetual position, a market swap and a GM
liquidity allocation. The kind selects the runtime allocation and is the only
discriminator the author writes; the connector tag comes from the named market.
"""

import json
from typing import Annotated, Literal

from pydantic import Field

from almanak.caip import AssetType
from almanak.configuration.input_values import AmountInput, PositiveDuration, Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.gmx_v2.domain import Contracts, Market
from almanak.connectors.gmx_v2.exchange_domain import ExchangeMarket
from almanak.connectors.gmx_v2.exchange_models import SwapAllocation
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation
from almanak.connectors.gmx_v2.models import Allocation, Bound
from almanak.connectors.gmx_v2.resolution import ResolvedMarket
from almanak.gateway.evm import native_metadata
from almanak.models import Asset, UInt
from almanak.triggers import HOLD_SECONDS_DESCRIPTION

Amounts = Annotated[tuple[AmountInput, ...], Field(min_length=2, max_length=3)]
BlockSpan = Annotated[UInt, Field(gt=0)]


class GmxPosition(PositionInput):
    connector: Literal["gmx_v2"] = Field(default="gmx_v2", exclude=True)


class PositionAllocationInput(GmxPosition):
    kind: Literal["gmx_position"]
    side: Literal["long", "short"]
    capital: AmountInput
    entry_collateral: AmountInput
    entry_size_usd_e30: Bound
    execution_fee: AmountInput
    increase_acceptable_price: UInt
    decrease_acceptable_price: UInt
    hold_seconds: PositiveDuration | None = Field(
        default=None, description=HOLD_SECONDS_DESCRIPTION
    )
    maximum_event_block_span: BlockSpan
    minimum_native_balance: Quantity

    def resolve(self, name: str, market: ResolvedMarket) -> Allocation:
        identity, native = native_metadata(market.selection.chain)
        result = self.model_dump(exclude={"market", "kind"})
        result.update(
            name=name,
            market=position_market(market.market),
            native_asset=identity,
            capital=amount(self.capital, market, "capital"),
            entry_collateral=amount(self.entry_collateral, market, "entry_collateral"),
            execution_fee=amount(self.execution_fee, market, "execution_fee"),
            minimum_native_balance=converted(
                self.minimum_native_balance, native.decimals, "minimum_native_balance"
            ),
        )
        return Allocation.model_validate(result)


class ExchangeAllocationInput(GmxPosition):
    """The inventory, request fee and timing every GM exchange allocation states."""

    initial_inventory: Amounts
    execution_fee: AmountInput
    hold_seconds: PositiveDuration | None = Field(
        default=None, description=HOLD_SECONDS_DESCRIPTION
    )
    deadline_seconds: PositiveDuration
    maximum_event_block_span: BlockSpan
    minimum_native_balance: Quantity

    def exchange_values(self, name: str, market: ResolvedMarket) -> dict:
        identity, native = native_metadata(market.selection.chain)
        result = self.model_dump(exclude={"market"})
        result.update(
            name=name,
            market=market.market,
            native_asset=identity,
            initial_inventory=ordered(self.initial_inventory, market, "initial_inventory"),
            execution_fee=amount(self.execution_fee, market, "execution_fee"),
            minimum_native_balance=converted(
                self.minimum_native_balance, native.decimals, "minimum_native_balance"
            ),
        )
        return result


class SwapAllocationInput(ExchangeAllocationInput):
    kind: Literal["gmx_swap"]
    entry_amount: AmountInput
    entry_minimum_output: AmountInput
    exit_minimum_output: AmountInput

    def resolve(self, name: str, market: ResolvedMarket) -> SwapAllocation:
        result = self.exchange_values(name, market)
        result.update(
            entry_amount=amount(self.entry_amount, market, "entry_amount"),
            entry_minimum_output=amount(self.entry_minimum_output, market, "entry_minimum_output"),
            exit_minimum_output=amount(self.exit_minimum_output, market, "exit_minimum_output"),
        )
        return SwapAllocation.model_validate(result)


class LiquidityAllocationInput(ExchangeAllocationInput):
    kind: Literal["gmx_liquidity"]
    funding_asset: AssetType
    entry_amounts: Amounts
    additional_amounts: Amounts
    minimum_market_tokens: AmountInput
    partial_withdrawal: AmountInput
    withdrawal_minimums: Amounts

    def resolve(self, name: str, market: ResolvedMarket) -> LiquidityAllocation:
        result = self.exchange_values(name, market)
        result.update(
            entry_amounts=ordered(self.entry_amounts, market, "entry_amounts"),
            additional_amounts=ordered(self.additional_amounts, market, "additional_amounts"),
            withdrawal_minimums=ordered(self.withdrawal_minimums, market, "withdrawal_minimums"),
            minimum_market_tokens=amount(
                self.minimum_market_tokens, market, "minimum_market_tokens"
            ),
            partial_withdrawal=amount(self.partial_withdrawal, market, "partial_withdrawal"),
        )
        return LiquidityAllocation.model_validate(result)


AllocationInput = Annotated[
    PositionAllocationInput | SwapAllocationInput | LiquidityAllocationInput,
    Field(discriminator="kind"),
]


def position_market(market: ExchangeMarket) -> Market:
    """A perpetual position keeps the ten roles its request templates use."""
    raw = market.model_dump(mode="json")
    raw["contracts"] = {field: raw["contracts"][field] for field in Contracts.model_fields}
    return Market.model_validate_json(json.dumps(raw))


def resolved_asset(market: ResolvedMarket, identity: AssetType, field: str) -> Asset:
    for asset in market.assets:
        if identity == asset.identifier:
            return asset
    raise ConfigurationError("amounts must name a resolved market asset", field=f"{field}.asset")


def amount(value: AmountInput, market: ResolvedMarket, field: str) -> dict:
    asset = resolved_asset(market, value.asset, field)
    return {
        "asset": value.asset,
        "amount": converted(value.quantity, asset.decimals, f"{field}.quantity"),
    }


def ordered(values, market: ResolvedMarket, field: str) -> tuple[dict, ...]:
    """Ordered inputs keep the author's order; the runtime checks the market's order."""
    return tuple(amount(value, market, f"{field}.{index}") for index, value in enumerate(values))
