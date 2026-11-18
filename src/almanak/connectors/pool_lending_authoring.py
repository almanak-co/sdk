"""Unresolved reserve economics shared by the Aave V3 and SparkLend author inputs.

Each connector's own input carries the excluded connector tag and converts these
values at the precision its resolved reserve verified.
"""

from pydantic import Field

from almanak.configuration.input_values import Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.lending_health import (
    MINIMUM_HEALTH_FACTOR_DESCRIPTION,
    health_factor_input,
)
from almanak.connectors.pool_lending_resolution import require_anchor
from almanak.gateway.evm import native_metadata
from almanak.models import Address


class ReserveAllocationInput(PositionInput):
    # The reserve asset the allocation starts with; omitted means none.
    capital: Quantity | None = Field(default=None, exclude_if=lambda value: value is None)
    # On the supported ordinary-mode reserves the Pool enables a first supply as collateral
    # whenever the reserve's LTV is above zero, so supplying such a reserve requires true;
    # false fits zero-LTV reserves and allocations that only borrow and repay.
    collateral: bool
    borrow: bool = False
    # Required where the allocation, or another in its fund/Pool, may borrow.
    minimum_health_factor: Quantity | None = Field(
        default=None, description=MINIMUM_HEALTH_FACTOR_DESCRIPTION
    )
    # These remain deliberately pinned source choices, not protocol-relative policies.
    oracle: Address
    price_source: Address
    minimum_native_balance: Quantity


def allocation_values(inputs: ReserveAllocationInput, name: str, market) -> dict:
    identity, native = native_metadata(market.selection.chain)
    decimals = market.asset.decimals
    values = inputs.model_dump(exclude={"market"})
    values.update(
        name=name,
        reserve=market.reserve,
        native_asset=identity,
        capital=0 if inputs.capital is None else converted(inputs.capital, decimals, "capital"),
        minimum_health_factor=health_factor_input(inputs.minimum_health_factor),
        minimum_native_balance=converted(
            inputs.minimum_native_balance, native.decimals, "minimum_native_balance"
        ),
    )
    return values


def require_supply_authority(inputs: ReserveAllocationInput, protocol, reader, market):
    """Refuse a supply-only allocation the Pool's automatic collateral would block.

    Without `collateral` or `borrow` the allocation can only supply and withdraw, and
    execution refuses every first supply on a reserve whose LTV enables it as collateral.
    """
    if inputs.collateral or inputs.borrow:
        return
    require_anchor(reader, market.block, "reserve")
    observed = protocol.read_reserve(reader, market.reserve, market.block)
    if observed.configuration.first_supply_enables_collateral:
        raise ConfigurationError(
            "collateral: false cannot supply this reserve: its LTV is "
            f"{observed.configuration.ltv_bps} bps, so the Pool enables the first supply "
            "as collateral automatically; set collateral: true (false only fits a zero-LTV "
            "reserve or an allocation that borrows)",
            field="collateral",
        )
